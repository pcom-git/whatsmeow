// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type permissionTestStore struct {
	*store.NoopStore
	permission *store.GroupSendPermission
	calls      int
}

func (s *permissionTestStore) GetGroupSendPermission(context.Context, types.JID, types.JID, types.JID) (*store.GroupSendPermission, error) {
	s.calls++
	return s.permission, nil
}

func TestGetStoredGroupSendPermissionUsesLocalStoreAndDirtyGuard(t *testing.T) {
	ownPN := types.NewJID("1234", types.DefaultUserServer)
	ownLID := types.NewJID("5678", types.HiddenUserServer)
	groupJID := types.NewJID("9999", types.GroupServer)
	permissionStore := &permissionTestStore{
		NoopStore: &store.NoopStore{},
		permission: &store.GroupSendPermission{
			StateKnown: true,
			IsJoined:   true,
			IsAnnounce: true,
			IsMember:   true,
		},
	}
	client := &Client{
		Store: &store.Device{
			ID:     &ownPN,
			LID:    ownLID,
			Groups: permissionStore,
		},
		groupPermissionDirty: make(map[types.JID]struct{}),
	}

	permission, err := client.GetStoredGroupSendPermission(context.Background(), groupJID)
	if err != nil {
		t.Fatalf("failed to get stored permission: %v", err)
	}
	if !permission.StateKnown || permissionStore.calls != 1 {
		t.Fatalf("unexpected stored permission=%+v calls=%d", permission, permissionStore.calls)
	}

	client.markGroupPermissionDirty(groupJID)
	permission, err = client.GetStoredGroupSendPermission(context.Background(), groupJID)
	if err != nil {
		t.Fatalf("dirty permission read failed: %v", err)
	}
	if permission.StateKnown || permissionStore.calls != 1 {
		t.Fatalf("dirty group must bypass store, permission=%+v calls=%d", permission, permissionStore.calls)
	}

	client.clearGroupPermissionDirty(groupJID)
	permission, err = client.GetStoredGroupSendPermission(context.Background(), groupJID)
	if err != nil || !permission.StateKnown || permissionStore.calls != 2 {
		t.Fatalf("cleared group must use store, permission=%+v calls=%d err=%v", permission, permissionStore.calls, err)
	}
}

func TestFailedPermissionUpdateStaysDirtyUntilSnapshotClear(t *testing.T) {
	client := &Client{groupPermissionDirty: make(map[types.JID]struct{})}
	groupJID := types.NewJID("9999", types.GroupServer)

	wasDirty := client.beginGroupPermissionUpdate(groupJID)
	client.finishGroupPermissionUpdate(groupJID, wasDirty, false)
	if !client.isGroupPermissionDirty(groupJID) {
		t.Fatal("failed permission update must leave group dirty")
	}

	wasDirty = client.beginGroupPermissionUpdate(groupJID)
	client.finishGroupPermissionUpdate(groupJID, wasDirty, true)
	if !client.isGroupPermissionDirty(groupJID) {
		t.Fatal("later incremental update must not clear an earlier dirty state")
	}

	client.clearGroupPermissionDirty(groupJID)
	if client.isGroupPermissionDirty(groupJID) {
		t.Fatal("complete snapshot clear must restore readable state")
	}
}

func TestGroupInfoEventAffectsSendPermission(t *testing.T) {
	if groupInfoEventAffectsSendPermission(nil) {
		t.Fatal("nil event must not affect permission")
	}
	if groupInfoEventAffectsSendPermission(&events.GroupInfo{Timestamp: time.Now()}) {
		t.Fatal("metadata-only event must not affect send permission")
	}
	if !groupInfoEventAffectsSendPermission(&events.GroupInfo{
		Announce: &types.GroupAnnounce{IsAnnounce: true},
	}) {
		t.Fatal("announcement event must affect send permission")
	}
}
