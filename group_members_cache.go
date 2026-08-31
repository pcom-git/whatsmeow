// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/singleflight"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

const (
	groupMemberSnapshotTTL    = 60 * time.Second
	groupMemberRefreshTimeout = 10 * time.Second
)

var groupMemberRefreshes singleflight.Group

type groupMemberRefreshResult struct {
	Refreshed   bool
	RemoteTotal int
	LocalTotal  int
}

func usableGroupMemberSnapshot(group *store.GroupListPageEntry, localTotal int) bool {
	if group == nil || group.LastSyncAt.IsZero() {
		return false
	}
	return group.ParticipantCount == 0 || localTotal > 0
}

func shouldRefreshGroupMemberSnapshot(page int, group *store.GroupListPageEntry, localTotal int, now time.Time) bool {
	if page != 1 {
		return false
	}
	if !usableGroupMemberSnapshot(group, localTotal) {
		return true
	}
	return now.Sub(group.LastSyncAt) > groupMemberSnapshotTTL
}

func groupMemberRefreshKey(cli *Client, groupJID types.JID) string {
	return fmt.Sprintf("%p:%s", cli, groupJID.ToNonAD().String())
}

func runGroupMemberRefresh(
	ctx context.Context,
	key string,
	timeout time.Duration,
	refresh func(context.Context) (groupMemberRefreshResult, error),
) (groupMemberRefreshResult, error) {
	resultCh := groupMemberRefreshes.DoChan(key, func() (any, error) {
		refreshCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return refresh(refreshCtx)
	})

	select {
	case <-ctx.Done():
		return groupMemberRefreshResult{}, ctx.Err()
	case result := <-resultCh:
		if result.Err != nil {
			return groupMemberRefreshResult{}, result.Err
		}
		refreshResult, ok := result.Val.(groupMemberRefreshResult)
		if !ok {
			return groupMemberRefreshResult{}, fmt.Errorf("unexpected group member refresh result %T", result.Val)
		}
		return refreshResult, nil
	}
}

func (cli *Client) getStoredActiveGroupMemberTotal(ctx context.Context, groupJID types.JID) (int, error) {
	page, err := cli.Store.Groups.GetGroupMemberListPage(ctx, store.GroupMemberListPageOptions{
		GroupJID: groupJID,
		Page:     1,
		PageSize: 1,
	})
	return page.Total, err
}

func (cli *Client) refreshGroupMemberSnapshot(ctx context.Context, groupJID types.JID) (groupMemberRefreshResult, error) {
	storedGroup, err := cli.Store.Groups.GetGroup(ctx, groupJID)
	if err != nil {
		return groupMemberRefreshResult{}, fmt.Errorf("failed to read stored group before refresh: %w", err)
	}
	storedTotal, err := cli.getStoredActiveGroupMemberTotal(ctx, groupJID)
	if err != nil {
		return groupMemberRefreshResult{}, fmt.Errorf("failed to read stored group members before refresh: %w", err)
	}
	if !shouldRefreshGroupMemberSnapshot(1, storedGroup, storedTotal, time.Now()) {
		return groupMemberRefreshResult{LocalTotal: storedTotal}, nil
	}

	info, err := cli.getGroupInfoWithSnapshotRequirement(ctx, groupJID, true, true)
	if err != nil {
		return groupMemberRefreshResult{}, err
	}
	freshTotal, err := cli.getStoredActiveGroupMemberTotal(ctx, groupJID)
	if err != nil {
		return groupMemberRefreshResult{}, fmt.Errorf("failed to read refreshed group members: %w", err)
	}
	remoteTotal := info.ParticipantCount
	if remoteTotal == 0 {
		remoteTotal = len(info.Participants)
	}
	if freshTotal != remoteTotal {
		return groupMemberRefreshResult{}, fmt.Errorf(
			"refreshed group member total mismatch: remote=%d local=%d",
			remoteTotal,
			freshTotal,
		)
	}
	return groupMemberRefreshResult{
		Refreshed:   true,
		RemoteTotal: remoteTotal,
		LocalTotal:  freshTotal,
	}, nil
}

func (cli *Client) getGroupMemberListPageWithRefresh(ctx context.Context, opts store.GroupMemberListPageOptions) (store.GroupMemberListPage, error) {
	localPage, err := cli.Store.Groups.GetGroupMemberListPage(ctx, opts)
	if err != nil || localPage.Page != 1 {
		return localPage, err
	}
	storedGroup, err := cli.Store.Groups.GetGroup(ctx, opts.GroupJID)
	if err != nil {
		return localPage, err
	}
	localTotal, err := cli.getStoredActiveGroupMemberTotal(ctx, opts.GroupJID)
	if err != nil {
		return localPage, err
	}
	localSnapshotUsable := usableGroupMemberSnapshot(storedGroup, localTotal)
	if !shouldRefreshGroupMemberSnapshot(localPage.Page, storedGroup, localTotal, time.Now()) {
		return localPage, nil
	}

	startedAt := time.Now()
	refreshResult, refreshErr := runGroupMemberRefresh(
		ctx,
		groupMemberRefreshKey(cli, opts.GroupJID),
		groupMemberRefreshTimeout,
		func(refreshCtx context.Context) (groupMemberRefreshResult, error) {
			return cli.refreshGroupMemberSnapshot(refreshCtx, opts.GroupJID)
		},
	)
	if refreshErr != nil {
		if localSnapshotUsable {
			kind := "failed"
			if errors.Is(refreshErr, context.DeadlineExceeded) {
				kind = "timed out"
			}
			cli.Log.Warnf(
				"Group member refresh for %s %s after %s, returning %d cached members: %v",
				opts.GroupJID,
				kind,
				time.Since(startedAt),
				localTotal,
				refreshErr,
			)
			return localPage, nil
		}
		return localPage, refreshErr
	}

	refreshedPage, err := cli.Store.Groups.GetGroupMemberListPage(ctx, opts)
	if err != nil {
		if localSnapshotUsable {
			cli.Log.Warnf("Failed to read refreshed group members for %s, returning cached page: %v", opts.GroupJID, err)
			return localPage, nil
		}
		return localPage, err
	}
	if refreshResult.Refreshed {
		cli.Log.Infof(
			"Refreshed group members for %s in %s: remote=%d local=%d",
			opts.GroupJID,
			time.Since(startedAt),
			refreshResult.RemoteTotal,
			refreshResult.LocalTotal,
		)
	}
	return refreshedPage, nil
}
