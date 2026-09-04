// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlstore

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

func putPermissionTestGroup(t *testing.T, sqlStore *SQLStore, groupJID, ownPN, ownLID types.JID, announce, admin bool) {
	t.Helper()
	info := &types.GroupInfo{
		JID:              groupJID,
		OwnerJID:         types.NewJID("9999", types.DefaultUserServer),
		GroupAnnounce:    types.GroupAnnounce{IsAnnounce: announce},
		ParticipantCount: 1,
		Participants: []types.GroupParticipant{{
			JID:          ownLID,
			PhoneNumber:  ownPN,
			LID:          ownLID,
			IsAdmin:      admin,
			IsSuperAdmin: false,
		}},
		ParticipantVersionID: "members-v1",
	}
	if err := sqlStore.PutGroupInfoSnapshot(context.Background(), info, time.Now()); err != nil {
		t.Fatalf("failed to store group snapshot: %v", err)
	}
}

func TestGetGroupSendPermissionFromStoredProjection(t *testing.T) {
	ctx := context.Background()
	_, sqlStore := newTestSQLStore(t)
	ownPN := types.NewJID("1234", types.DefaultUserServer)
	ownLID := types.NewJID("5678", types.HiddenUserServer)

	tests := []struct {
		name     string
		announce bool
		admin    bool
	}{
		{name: "regular group member", announce: false, admin: false},
		{name: "announcement group admin", announce: true, admin: true},
		{name: "announcement group member", announce: true, admin: false},
	}
	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			groupJID := types.NewJID(string(rune('1'+index)), types.GroupServer)
			putPermissionTestGroup(t, sqlStore, groupJID, ownPN, ownLID, tc.announce, tc.admin)
			permission, err := sqlStore.GetGroupSendPermission(ctx, groupJID, ownPN, ownLID)
			if err != nil {
				t.Fatalf("failed to get permission: %v", err)
			}
			if !permission.StateKnown || !permission.IsJoined || !permission.IsMember {
				t.Fatalf("expected a complete joined member projection, got %+v", permission)
			}
			if permission.IsAnnounce != tc.announce || permission.IsAdmin != tc.admin {
				t.Fatalf("unexpected permission: got %+v", permission)
			}
		})
	}

	missing, err := sqlStore.GetGroupSendPermission(ctx, types.NewJID("missing", types.GroupServer), ownPN, ownLID)
	if err != nil {
		t.Fatalf("failed to query missing group: %v", err)
	}
	if missing.StateKnown {
		t.Fatalf("missing group must be unknown, got %+v", missing)
	}
}

func TestGroupPermissionEventsResolvePNAndLIDAliases(t *testing.T) {
	ctx := context.Background()
	_, sqlStore := newTestSQLStore(t)
	groupJID := types.NewJID("12345", types.GroupServer)
	ownPN := types.NewJID("1234", types.DefaultUserServer)
	ownLID := types.NewJID("5678", types.HiddenUserServer)
	if err := sqlStore.LIDMap.PutLIDMapping(ctx, ownLID, ownPN); err != nil {
		t.Fatalf("failed to put LID mapping: %v", err)
	}
	putPermissionTestGroup(t, sqlStore, groupJID, ownPN, ownLID, true, false)

	if err := sqlStore.PutGroupInfoEvent(ctx, &store.GroupInfoEvent{
		JID:                  groupJID,
		Timestamp:            time.Now(),
		ParticipantVersionID: "members-v2",
		Promote:              []types.JID{ownPN},
	}); err != nil {
		t.Fatalf("failed to promote PN alias: %v", err)
	}
	permission, err := sqlStore.GetGroupSendPermission(ctx, groupJID, ownPN, ownLID)
	if err != nil || !permission.IsAdmin {
		t.Fatalf("expected PN promotion to update LID member, permission=%+v err=%v", permission, err)
	}

	if err = sqlStore.PutGroupInfoEvent(ctx, &store.GroupInfoEvent{
		JID:                  groupJID,
		Timestamp:            time.Now().Add(time.Second),
		ParticipantVersionID: "members-v3",
		Demote:               []types.JID{ownLID},
	}); err != nil {
		t.Fatalf("failed to demote LID alias: %v", err)
	}
	permission, err = sqlStore.GetGroupSendPermission(ctx, groupJID, ownPN, ownLID)
	if err != nil || permission.IsAdmin || permission.IsSuperAdmin {
		t.Fatalf("expected LID demotion to clear admin, permission=%+v err=%v", permission, err)
	}

	if err = sqlStore.PutGroupInfoEvent(ctx, &store.GroupInfoEvent{
		JID:                  groupJID,
		Timestamp:            time.Now().Add(2 * time.Second),
		ParticipantVersionID: "members-v4",
		Leave:                []types.JID{ownPN},
	}); err != nil {
		t.Fatalf("failed to remove PN alias: %v", err)
	}
	permission, err = sqlStore.GetGroupSendPermission(ctx, groupJID, ownPN, ownLID)
	if err != nil || permission.StateKnown || permission.IsMember {
		t.Fatalf("announcement group without current member must be unknown, permission=%+v err=%v", permission, err)
	}

	if err = sqlStore.PutGroupInfoEvent(ctx, &store.GroupInfoEvent{
		JID:                  groupJID,
		Timestamp:            time.Now().Add(3 * time.Second),
		ParticipantVersionID: "members-v5",
		Join:                 []types.JID{ownLID},
	}); err != nil {
		t.Fatalf("failed to rejoin LID alias: %v", err)
	}
	permission, err = sqlStore.GetGroupSendPermission(ctx, groupJID, ownPN, ownLID)
	if err != nil || !permission.StateKnown || !permission.IsMember || permission.IsAdmin {
		t.Fatalf("expected rejoined alias to be a regular active member, permission=%+v err=%v", permission, err)
	}

	var activeRows int
	if err = sqlStore.db.QueryRow(ctx, `
		SELECT COUNT(*) FROM whatsmeow_group_members
		WHERE our_jid=$1 AND group_jid=$2 AND status='active'
	`, sqlStore.JID, jidString(groupJID)).Scan(&activeRows); err != nil {
		t.Fatalf("failed to count active members: %v", err)
	}
	if activeRows != 1 {
		t.Fatalf("expected one canonical active member, got %d", activeRows)
	}
}

func TestAnnouncementEventDoesNotOverwriteParticipantVersion(t *testing.T) {
	ctx := context.Background()
	_, sqlStore := newTestSQLStore(t)
	groupJID := types.NewJID("54321", types.GroupServer)
	ownPN := types.NewJID("1234", types.DefaultUserServer)
	ownLID := types.NewJID("5678", types.HiddenUserServer)
	putPermissionTestGroup(t, sqlStore, groupJID, ownPN, ownLID, false, false)

	if err := sqlStore.PutGroupInfoEvent(ctx, &store.GroupInfoEvent{
		JID:       groupJID,
		Timestamp: time.Now(),
		Announce: &types.GroupAnnounce{
			IsAnnounce:        true,
			AnnounceVersionID: "announce-v2",
		},
	}); err != nil {
		t.Fatalf("failed to store announcement event: %v", err)
	}

	var announce bool
	var participantVersion string
	if err := sqlStore.db.QueryRow(ctx, `
		SELECT is_announce, participant_version_id
		FROM whatsmeow_groups WHERE our_jid=$1 AND group_jid=$2
	`, sqlStore.JID, jidString(groupJID)).Scan(&announce, &participantVersion); err != nil {
		t.Fatalf("failed to query group state: %v", err)
	}
	if !announce || participantVersion != "members-v1" {
		t.Fatalf("unexpected group state: announce=%t participant_version=%q", announce, participantVersion)
	}
}

func TestAnnouncementPermissionWithoutCompleteSnapshotIsUnknown(t *testing.T) {
	ctx := context.Background()
	_, sqlStore := newTestSQLStore(t)
	groupJID := types.NewJID("77777", types.GroupServer)
	ownPN := types.NewJID("1234", types.DefaultUserServer)
	ownLID := types.NewJID("5678", types.HiddenUserServer)
	if err := sqlStore.PutGroupInfoEvent(ctx, &store.GroupInfoEvent{
		JID:       groupJID,
		Timestamp: time.Now(),
		Announce:  &types.GroupAnnounce{IsAnnounce: true},
	}); err != nil {
		t.Fatalf("failed to store partial event: %v", err)
	}
	permission, err := sqlStore.GetGroupSendPermission(ctx, groupJID, ownPN, ownLID)
	if err != nil {
		t.Fatalf("failed to get permission: %v", err)
	}
	if permission.StateKnown {
		t.Fatalf("partial group projection must be unknown, got %+v", permission)
	}
}

func TestGroupOwnerIsTreatedAsSuperAdminWithoutMemberRow(t *testing.T) {
	ctx := context.Background()
	_, sqlStore := newTestSQLStore(t)
	groupJID := types.NewJID("88888", types.GroupServer)
	ownPN := types.NewJID("1234", types.DefaultUserServer)
	ownLID := types.NewJID("5678", types.HiddenUserServer)
	if err := sqlStore.PutGroupInfoSnapshot(ctx, &types.GroupInfo{
		JID:           groupJID,
		OwnerPN:       ownPN,
		GroupAnnounce: types.GroupAnnounce{IsAnnounce: true},
	}, time.Now()); err != nil {
		t.Fatalf("failed to store owner snapshot: %v", err)
	}
	permission, err := sqlStore.GetGroupSendPermission(ctx, groupJID, ownPN, ownLID)
	if err != nil {
		t.Fatalf("failed to get owner permission: %v", err)
	}
	if !permission.StateKnown || !permission.IsMember || !permission.IsAdmin || !permission.IsSuperAdmin {
		t.Fatalf("owner must be treated as a super admin, got %+v", permission)
	}
}
