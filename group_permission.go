// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"errors"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// ErrGroupPermissionStoreUnavailable means the configured group store does
// not provide the optional local group permission projection.
var ErrGroupPermissionStoreUnavailable = errors.New("group permission store is unavailable")

// GetStoredGroupSendPermission reads group posting permission exclusively from
// the local store. It never sends an IQ or otherwise accesses the network.
func (cli *Client) GetStoredGroupSendPermission(ctx context.Context, groupJID types.JID) (*store.GroupSendPermission, error) {
	unknown := &store.GroupSendPermission{}
	if cli == nil || cli.Store == nil || cli.Store.Groups == nil {
		return unknown, ErrGroupPermissionStoreUnavailable
	}
	groupJID = groupJID.ToNonAD()
	if cli.isGroupPermissionDirty(groupJID) {
		return unknown, nil
	}
	permissionStore, ok := cli.Store.Groups.(store.GroupPermissionStore)
	if !ok {
		return unknown, ErrGroupPermissionStoreUnavailable
	}
	return permissionStore.GetGroupSendPermission(ctx, groupJID, cli.getOwnID(), cli.getOwnLID())
}

func groupInfoEventAffectsSendPermission(evt *events.GroupInfo) bool {
	return evt != nil && (evt.Announce != nil || evt.Delete != nil || evt.Suspended || evt.Unsuspended ||
		len(evt.Join) > 0 || len(evt.Leave) > 0 || len(evt.Promote) > 0 || len(evt.Demote) > 0)
}

func (cli *Client) beginGroupPermissionUpdate(groupJID types.JID) (wasDirty bool) {
	if cli == nil || groupJID.IsEmpty() {
		return false
	}
	groupJID = groupJID.ToNonAD()
	cli.groupPermissionLock.Lock()
	defer cli.groupPermissionLock.Unlock()
	if cli.groupPermissionDirty == nil {
		cli.groupPermissionDirty = make(map[types.JID]struct{})
	}
	_, wasDirty = cli.groupPermissionDirty[groupJID]
	cli.groupPermissionDirty[groupJID] = struct{}{}
	return wasDirty
}

func (cli *Client) finishGroupPermissionUpdate(groupJID types.JID, wasDirty, succeeded bool) {
	if cli == nil || groupJID.IsEmpty() || wasDirty || !succeeded {
		return
	}
	groupJID = groupJID.ToNonAD()
	cli.groupPermissionLock.Lock()
	delete(cli.groupPermissionDirty, groupJID)
	cli.groupPermissionLock.Unlock()
}

func (cli *Client) markGroupPermissionDirty(groupJID types.JID) {
	if cli == nil || groupJID.IsEmpty() {
		return
	}
	groupJID = groupJID.ToNonAD()
	cli.groupPermissionLock.Lock()
	if cli.groupPermissionDirty == nil {
		cli.groupPermissionDirty = make(map[types.JID]struct{})
	}
	cli.groupPermissionDirty[groupJID] = struct{}{}
	cli.groupPermissionLock.Unlock()
}

func (cli *Client) isGroupPermissionDirty(groupJID types.JID) bool {
	if cli == nil || groupJID.IsEmpty() {
		return false
	}
	cli.groupPermissionLock.RLock()
	_, dirty := cli.groupPermissionDirty[groupJID.ToNonAD()]
	cli.groupPermissionLock.RUnlock()
	return dirty
}

func (cli *Client) clearGroupPermissionDirty(groupJID types.JID) {
	if cli == nil || groupJID.IsEmpty() {
		return
	}
	cli.groupPermissionLock.Lock()
	delete(cli.groupPermissionDirty, groupJID.ToNonAD())
	cli.groupPermissionLock.Unlock()
}
