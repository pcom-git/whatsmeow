// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mau.fi/util/dbutil"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

const (
	groupMemberStatusActive  = "active"
	groupMemberStatusLeft    = "left"
	groupMemberStatusRemoved = "removed"
	groupMemberStatusAll     = "all"

	groupMemberRoleAll    = "all"
	groupMemberRoleAdmin  = "admin"
	groupMemberRoleMember = "member"
)

var _ store.GroupStore = (*SQLStore)(nil)

const (
	upsertGroupQuery = `
		INSERT INTO whatsmeow_groups (
			our_jid, group_jid, owner_jid, owner_pn, name, topic,
			is_locked, is_announce, is_ephemeral, disappearing_timer,
			is_incognito, is_parent, default_membership_approval_mode,
			linked_parent_jid, is_default_sub_group, is_join_approval_required,
			participant_count, participant_version_id, member_add_mode, suspended,
			is_joined, is_deleted, last_sync_at, updated_at, raw_info
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10,
			$11, $12, $13,
			$14, $15, $16,
			$17, $18, $19, $20,
			$21, $22, $23, $24, $25
		)
		ON CONFLICT (our_jid, group_jid) DO UPDATE SET
			owner_jid=excluded.owner_jid,
			owner_pn=excluded.owner_pn,
			name=excluded.name,
			topic=excluded.topic,
			is_locked=excluded.is_locked,
			is_announce=excluded.is_announce,
			is_ephemeral=excluded.is_ephemeral,
			disappearing_timer=excluded.disappearing_timer,
			is_incognito=excluded.is_incognito,
			is_parent=excluded.is_parent,
			default_membership_approval_mode=excluded.default_membership_approval_mode,
			linked_parent_jid=excluded.linked_parent_jid,
			is_default_sub_group=excluded.is_default_sub_group,
			is_join_approval_required=excluded.is_join_approval_required,
			participant_count=excluded.participant_count,
			participant_version_id=excluded.participant_version_id,
			member_add_mode=excluded.member_add_mode,
			suspended=excluded.suspended,
			is_joined=true,
			is_deleted=false,
			last_sync_at=excluded.last_sync_at,
			updated_at=excluded.updated_at,
			raw_info=excluded.raw_info
	`
	ensureGroupQuery = `
		INSERT INTO whatsmeow_groups (our_jid, group_jid, last_sync_at, updated_at)
		VALUES ($1, $2, $3, $3)
		ON CONFLICT (our_jid, group_jid) DO UPDATE SET updated_at=excluded.updated_at
	`
	upsertActiveGroupMemberQuery = `
		INSERT INTO whatsmeow_group_members (
			our_jid, group_jid, member_jid, phone_number, lid, display_name,
			is_admin, is_super_admin, status, joined_at, left_at,
			participant_version_id, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'active', $9, 0, $10, $11)
		ON CONFLICT (our_jid, group_jid, member_jid) DO UPDATE SET
			phone_number=COALESCE(excluded.phone_number, whatsmeow_group_members.phone_number),
			lid=COALESCE(excluded.lid, whatsmeow_group_members.lid),
			display_name=CASE
				WHEN excluded.display_name <> '' THEN excluded.display_name
				ELSE whatsmeow_group_members.display_name
			END,
			is_admin=excluded.is_admin,
			is_super_admin=excluded.is_super_admin,
			status='active',
			joined_at=CASE
				WHEN whatsmeow_group_members.joined_at = 0 OR whatsmeow_group_members.status <> 'active'
					THEN excluded.joined_at
				ELSE whatsmeow_group_members.joined_at
			END,
			left_at=0,
			participant_version_id=excluded.participant_version_id,
			updated_at=excluded.updated_at
	`
	upsertInactiveGroupMemberQuery = `
		INSERT INTO whatsmeow_group_members (
			our_jid, group_jid, member_jid, phone_number, lid, display_name,
			is_admin, is_super_admin, status, joined_at, left_at,
			participant_version_id, updated_at
		) VALUES ($1, $2, $3, $4, $5, '', false, false, $6, 0, $7, $8, $7)
		ON CONFLICT (our_jid, group_jid, member_jid) DO UPDATE SET
			status=excluded.status,
			left_at=excluded.left_at,
			participant_version_id=excluded.participant_version_id,
			updated_at=excluded.updated_at
	`
	recalculateGroupParticipantCountQuery = `
		UPDATE whatsmeow_groups
		SET participant_count=(
			SELECT COUNT(*)
			FROM whatsmeow_group_members
			WHERE our_jid=$1 AND group_jid=$2 AND status='active'
		), updated_at=$3
		WHERE our_jid=$1 AND group_jid=$2
	`
	setGroupMemberAdminQuery = `
		UPDATE whatsmeow_group_members
		SET is_admin=$4,
			is_super_admin=CASE WHEN $4 THEN is_super_admin ELSE false END,
			participant_version_id=$5,
			updated_at=$6
		WHERE our_jid=$1 AND group_jid=$2 AND member_jid=$3
	`
	updateGroupNameQuery = `
		UPDATE whatsmeow_groups SET name=$3, updated_at=$4 WHERE our_jid=$1 AND group_jid=$2
	`
	updateGroupTopicQuery = `
		UPDATE whatsmeow_groups SET topic=$3, updated_at=$4 WHERE our_jid=$1 AND group_jid=$2
	`
	updateGroupLockedQuery = `
		UPDATE whatsmeow_groups SET is_locked=$3, updated_at=$4 WHERE our_jid=$1 AND group_jid=$2
	`
	updateGroupAnnounceQuery = `
		UPDATE whatsmeow_groups
		SET is_announce=$3, participant_version_id=$4, updated_at=$5
		WHERE our_jid=$1 AND group_jid=$2
	`
	updateGroupEphemeralQuery = `
		UPDATE whatsmeow_groups
		SET is_ephemeral=$3, disappearing_timer=$4, updated_at=$5
		WHERE our_jid=$1 AND group_jid=$2
	`
	updateGroupMembershipApprovalQuery = `
		UPDATE whatsmeow_groups
		SET is_join_approval_required=$3, updated_at=$4
		WHERE our_jid=$1 AND group_jid=$2
	`
	updateGroupSuspendedQuery = `
		UPDATE whatsmeow_groups SET suspended=$3, updated_at=$4 WHERE our_jid=$1 AND group_jid=$2
	`
	markGroupDeletedQuery = `
		UPDATE whatsmeow_groups
		SET is_deleted=true, is_joined=false, updated_at=$3
		WHERE our_jid=$1 AND group_jid=$2
	`
	groupListSelect = `
		SELECT
			g.group_jid,
			COALESCE(NULLIF(g.owner_pn, ''), g.owner_jid, ''),
			g.name,
			g.topic,
			g.is_locked,
			g.is_announce,
			g.is_ephemeral,
			g.disappearing_timer,
			g.is_incognito,
			g.is_parent,
			g.default_membership_approval_mode,
			COALESCE(g.linked_parent_jid, ''),
			g.is_default_sub_group,
			g.is_join_approval_required,
			COALESCE(active_members.active_count, g.participant_count),
			g.member_add_mode,
			g.suspended
		FROM whatsmeow_groups g
		LEFT JOIN (
			SELECT our_jid, group_jid, COUNT(*) AS active_count
			FROM whatsmeow_group_members
			WHERE our_jid=$1 AND status='active'
			GROUP BY our_jid, group_jid
		) active_members
		  ON active_members.our_jid=g.our_jid AND active_members.group_jid=g.group_jid
	`
	getGroupQuery = groupListSelect + `
		WHERE g.our_jid=$1 AND g.group_jid=$2 AND g.is_deleted=false
	`
)

func jidDBValue(jid types.JID) any {
	if jid.IsEmpty() {
		return nil
	}
	return jid.ToNonAD().String()
}

func jidString(jid types.JID) string {
	if jid.IsEmpty() {
		return ""
	}
	return jid.ToNonAD().String()
}

func groupEventUnix(t time.Time) int64 {
	if t.IsZero() {
		return time.Now().Unix()
	}
	return t.Unix()
}

func normalizeGroupListPageOptions(options store.GroupListPageOptions) store.GroupListPageOptions {
	if options.Page <= 0 {
		options.Page = 1
	}
	if options.PageSize <= 0 {
		options.PageSize = 50
	} else if options.PageSize > 500 {
		options.PageSize = 500
	}
	options.Keyword = strings.TrimSpace(options.Keyword)
	return options
}

func normalizeGroupMemberListPageOptions(options store.GroupMemberListPageOptions) (store.GroupMemberListPageOptions, error) {
	if options.Page <= 0 {
		options.Page = 1
	}
	if options.PageSize <= 0 {
		options.PageSize = 50
	} else if options.PageSize > 500 {
		options.PageSize = 500
	}
	options.Keyword = strings.TrimSpace(options.Keyword)
	options.Status = strings.ToLower(strings.TrimSpace(options.Status))
	if options.Status == "" {
		options.Status = groupMemberStatusActive
	}
	switch options.Status {
	case groupMemberStatusActive, groupMemberStatusLeft, groupMemberStatusRemoved, groupMemberStatusAll:
	default:
		return options, fmt.Errorf("invalid group member status %q", options.Status)
	}
	options.Role = strings.ToLower(strings.TrimSpace(options.Role))
	if options.Role == "" {
		options.Role = groupMemberRoleAll
	}
	switch options.Role {
	case groupMemberRoleAll, groupMemberRoleAdmin, groupMemberRoleMember:
	default:
		return options, fmt.Errorf("invalid group member role %q", options.Role)
	}
	return options, nil
}

func appendSQLArg(args *[]any, val any) string {
	*args = append(*args, val)
	return fmt.Sprintf("$%d", len(*args))
}

func sqlPlaceholders(start, count int) []string {
	placeholders := make([]string, count)
	for i := range count {
		placeholders[i] = fmt.Sprintf("$%d", start+i)
	}
	return placeholders
}

func groupParticipantCount(group *types.GroupInfo) int {
	if group.ParticipantCount > 0 || len(group.Participants) == 0 {
		return group.ParticipantCount
	}
	return len(group.Participants)
}

func (s *SQLStore) upsertGroup(ctx context.Context, group *types.GroupInfo, syncedAt time.Time) error {
	if group == nil || group.JID.IsEmpty() {
		return nil
	}
	ts := groupEventUnix(syncedAt)
	_, err := s.db.Exec(ctx, upsertGroupQuery,
		s.JID,
		jidString(group.JID),
		jidDBValue(group.OwnerJID),
		jidDBValue(group.OwnerPN),
		group.Name,
		group.Topic,
		group.IsLocked,
		group.IsAnnounce,
		group.IsEphemeral,
		int64(group.DisappearingTimer),
		group.IsIncognito,
		group.IsParent,
		group.DefaultMembershipApprovalMode,
		jidDBValue(group.LinkedParentJID),
		group.IsDefaultSubGroup,
		group.IsJoinApprovalRequired,
		groupParticipantCount(group),
		group.ParticipantVersionID,
		string(group.MemberAddMode),
		group.Suspended,
		true,
		false,
		ts,
		ts,
		nil,
	)
	return err
}

func (s *SQLStore) ensureGroup(ctx context.Context, groupJID types.JID, eventTime time.Time) error {
	if groupJID.IsEmpty() {
		return nil
	}
	_, err := s.db.Exec(ctx, ensureGroupQuery, s.JID, jidString(groupJID), groupEventUnix(eventTime))
	return err
}

func participantMemberJID(participant types.GroupParticipant) types.JID {
	if !participant.JID.IsEmpty() {
		return participant.JID.ToNonAD()
	}
	if !participant.PhoneNumber.IsEmpty() {
		return participant.PhoneNumber.ToNonAD()
	}
	return participant.LID.ToNonAD()
}

func normalizeParticipant(participant types.GroupParticipant) types.GroupParticipant {
	participant.JID = participantMemberJID(participant)
	if participant.PhoneNumber.IsEmpty() && participant.JID.Server == types.DefaultUserServer {
		participant.PhoneNumber = participant.JID
	}
	if participant.LID.IsEmpty() && participant.JID.Server == types.HiddenUserServer {
		participant.LID = participant.JID
	}
	return participant
}

func groupParticipantFromJID(jid types.JID) types.GroupParticipant {
	participant := types.GroupParticipant{JID: jid.ToNonAD()}
	if jid.Server == types.DefaultUserServer {
		participant.PhoneNumber = participant.JID
	} else if jid.Server == types.HiddenUserServer {
		participant.LID = participant.JID
	}
	return participant
}

func (s *SQLStore) upsertActiveGroupMember(ctx context.Context, groupJID types.JID, participant types.GroupParticipant, versionID string, eventTime time.Time) error {
	if groupJID.IsEmpty() {
		return nil
	}
	participant = normalizeParticipant(participant)
	if participant.JID.IsEmpty() {
		return nil
	}
	ts := groupEventUnix(eventTime)
	_, err := s.db.Exec(ctx, upsertActiveGroupMemberQuery,
		s.JID,
		jidString(groupJID),
		jidString(participant.JID),
		jidDBValue(participant.PhoneNumber),
		jidDBValue(participant.LID),
		participant.DisplayName,
		participant.IsAdmin || participant.IsSuperAdmin,
		participant.IsSuperAdmin,
		ts,
		versionID,
		ts,
	)
	return err
}

func (s *SQLStore) upsertInactiveGroupMember(ctx context.Context, groupJID, member types.JID, status, versionID string, eventTime time.Time) error {
	if groupJID.IsEmpty() || member.IsEmpty() {
		return nil
	}
	participant := groupParticipantFromJID(member)
	ts := groupEventUnix(eventTime)
	_, err := s.db.Exec(ctx, upsertInactiveGroupMemberQuery,
		s.JID,
		jidString(groupJID),
		jidString(participant.JID),
		jidDBValue(participant.PhoneNumber),
		jidDBValue(participant.LID),
		status,
		ts,
		versionID,
	)
	return err
}

func (s *SQLStore) recalculateGroupParticipantCount(ctx context.Context, groupJID types.JID, eventTime time.Time) error {
	if groupJID.IsEmpty() {
		return nil
	}
	_, err := s.db.Exec(ctx, recalculateGroupParticipantCountQuery, s.JID, jidString(groupJID), groupEventUnix(eventTime))
	return err
}

func (s *SQLStore) replaceGroupMembers(ctx context.Context, groupJID types.JID, members []types.GroupParticipant, versionID string, syncedAt time.Time) error {
	if groupJID.IsEmpty() {
		return nil
	}
	memberIDs := make([]string, 0, len(members))
	seenMembers := make(map[string]struct{}, len(members))
	for _, member := range members {
		member = normalizeParticipant(member)
		if member.JID.IsEmpty() {
			continue
		}
		memberID := jidString(member.JID)
		if _, ok := seenMembers[memberID]; ok {
			continue
		}
		seenMembers[memberID] = struct{}{}
		memberIDs = append(memberIDs, memberID)
		if err := s.upsertActiveGroupMember(ctx, groupJID, member, versionID, syncedAt); err != nil {
			return err
		}
	}

	ts := groupEventUnix(syncedAt)
	if len(memberIDs) == 0 {
		_, err := s.db.Exec(ctx, `
			UPDATE whatsmeow_group_members
			SET status='left', left_at=$3, participant_version_id=$4, updated_at=$3
			WHERE our_jid=$1 AND group_jid=$2 AND status='active'
		`, s.JID, jidString(groupJID), ts, versionID)
		if err != nil {
			return err
		}
	} else {
		args := make([]any, 4, 4+len(memberIDs))
		args[0] = s.JID
		args[1] = jidString(groupJID)
		args[2] = ts
		args[3] = versionID
		for _, memberID := range memberIDs {
			args = append(args, memberID)
		}
		query := fmt.Sprintf(`
			UPDATE whatsmeow_group_members
			SET status='left', left_at=$3, participant_version_id=$4, updated_at=$3
			WHERE our_jid=$1 AND group_jid=$2 AND status='active' AND member_jid NOT IN (%s)
		`, strings.Join(sqlPlaceholders(5, len(memberIDs)), ","))
		_, err := s.db.Exec(ctx, query, args...)
		if err != nil {
			return err
		}
	}
	return s.recalculateGroupParticipantCount(ctx, groupJID, syncedAt)
}

func shouldReplaceGroupMembers(group *types.GroupInfo) bool {
	return group != nil && (len(group.Participants) > 0 || group.ParticipantCount == 0)
}

func (s *SQLStore) PutJoinedGroupsSnapshot(ctx context.Context, groups []*types.GroupInfo, syncedAt time.Time) error {
	return s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		groupIDs := make([]string, 0, len(groups))
		seenGroups := make(map[string]struct{}, len(groups))
		for _, group := range groups {
			if group == nil || group.JID.IsEmpty() {
				continue
			}
			groupID := jidString(group.JID)
			if _, ok := seenGroups[groupID]; ok {
				continue
			}
			seenGroups[groupID] = struct{}{}
			groupIDs = append(groupIDs, groupID)
			if err := s.upsertGroup(ctx, group, syncedAt); err != nil {
				return err
			}
			if shouldReplaceGroupMembers(group) {
				if err := s.replaceGroupMembers(ctx, group.JID, group.Participants, group.ParticipantVersionID, syncedAt); err != nil {
					return err
				}
			}
		}

		ts := groupEventUnix(syncedAt)
		if len(groupIDs) == 0 {
			_, err := s.db.Exec(ctx, `
				UPDATE whatsmeow_groups SET is_joined=false, updated_at=$2 WHERE our_jid=$1 AND is_joined=true
			`, s.JID, ts)
			return err
		}
		args := make([]any, 2, 2+len(groupIDs))
		args[0] = s.JID
		args[1] = ts
		for _, groupID := range groupIDs {
			args = append(args, groupID)
		}
		query := fmt.Sprintf(`
			UPDATE whatsmeow_groups
			SET is_joined=false, updated_at=$2
			WHERE our_jid=$1 AND is_joined=true AND group_jid NOT IN (%s)
		`, strings.Join(sqlPlaceholders(3, len(groupIDs)), ","))
		_, err := s.db.Exec(ctx, query, args...)
		return err
	})
}

func (s *SQLStore) PutGroupInfoSnapshot(ctx context.Context, group *types.GroupInfo, syncedAt time.Time) error {
	if group == nil || group.JID.IsEmpty() {
		return nil
	}
	return s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		if err := s.upsertGroup(ctx, group, syncedAt); err != nil {
			return err
		}
		if shouldReplaceGroupMembers(group) {
			return s.replaceGroupMembers(ctx, group.JID, group.Participants, group.ParticipantVersionID, syncedAt)
		}
		return nil
	})
}

func sameParticipantJID(member types.JID, sender, senderPN *types.JID) bool {
	if sender != nil && member.ToNonAD() == sender.ToNonAD() {
		return true
	}
	return senderPN != nil && member.ToNonAD() == senderPN.ToNonAD()
}

func (s *SQLStore) PutGroupInfoEvent(ctx context.Context, evt *store.GroupInfoEvent) error {
	if evt == nil || evt.JID.IsEmpty() {
		return nil
	}
	return s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		if err := s.ensureGroup(ctx, evt.JID, evt.Timestamp); err != nil {
			return err
		}
		groupID := jidString(evt.JID)
		ts := groupEventUnix(evt.Timestamp)

		if evt.Name != nil {
			if _, err := s.db.Exec(ctx, updateGroupNameQuery, s.JID, groupID, evt.Name.Name, ts); err != nil {
				return err
			}
		}
		if evt.Topic != nil {
			if _, err := s.db.Exec(ctx, updateGroupTopicQuery, s.JID, groupID, evt.Topic.Topic, ts); err != nil {
				return err
			}
		}
		if evt.Locked != nil {
			if _, err := s.db.Exec(ctx, updateGroupLockedQuery, s.JID, groupID, evt.Locked.IsLocked, ts); err != nil {
				return err
			}
		}
		if evt.Announce != nil {
			if _, err := s.db.Exec(ctx, updateGroupAnnounceQuery, s.JID, groupID, evt.Announce.IsAnnounce, evt.Announce.AnnounceVersionID, ts); err != nil {
				return err
			}
		}
		if evt.Ephemeral != nil {
			if _, err := s.db.Exec(ctx, updateGroupEphemeralQuery, s.JID, groupID, evt.Ephemeral.IsEphemeral, int64(evt.Ephemeral.DisappearingTimer), ts); err != nil {
				return err
			}
		}
		if evt.MembershipApprovalMode != nil {
			if _, err := s.db.Exec(ctx, updateGroupMembershipApprovalQuery, s.JID, groupID, evt.MembershipApprovalMode.IsJoinApprovalRequired, ts); err != nil {
				return err
			}
		}
		if evt.Suspended || evt.Unsuspended {
			if _, err := s.db.Exec(ctx, updateGroupSuspendedQuery, s.JID, groupID, evt.Suspended, ts); err != nil {
				return err
			}
		}
		if evt.Delete != nil && evt.Delete.Deleted {
			if _, err := s.db.Exec(ctx, markGroupDeletedQuery, s.JID, groupID, ts); err != nil {
				return err
			}
		}

		participantChanged := len(evt.Join) > 0 || len(evt.Leave) > 0
		for _, member := range evt.Join {
			if err := s.upsertActiveGroupMember(ctx, evt.JID, groupParticipantFromJID(member), evt.ParticipantVersionID, evt.Timestamp); err != nil {
				return err
			}
		}
		for _, member := range evt.Leave {
			status := groupMemberStatusRemoved
			if sameParticipantJID(member, evt.Sender, evt.SenderPN) {
				status = groupMemberStatusLeft
			}
			if err := s.upsertInactiveGroupMember(ctx, evt.JID, member, status, evt.ParticipantVersionID, evt.Timestamp); err != nil {
				return err
			}
		}
		for _, member := range evt.Promote {
			if err := s.upsertActiveGroupMember(ctx, evt.JID, groupParticipantFromJID(member), evt.ParticipantVersionID, evt.Timestamp); err != nil {
				return err
			}
			if _, err := s.db.Exec(ctx, setGroupMemberAdminQuery, s.JID, groupID, jidString(member), true, evt.ParticipantVersionID, ts); err != nil {
				return err
			}
		}
		for _, member := range evt.Demote {
			if err := s.upsertActiveGroupMember(ctx, evt.JID, groupParticipantFromJID(member), evt.ParticipantVersionID, evt.Timestamp); err != nil {
				return err
			}
			if _, err := s.db.Exec(ctx, setGroupMemberAdminQuery, s.JID, groupID, jidString(member), false, evt.ParticipantVersionID, ts); err != nil {
				return err
			}
		}
		if participantChanged {
			return s.recalculateGroupParticipantCount(ctx, evt.JID, evt.Timestamp)
		}
		return nil
	})
}

func buildGroupListWhere(options store.GroupListPageOptions) (string, []any) {
	args := []any{nil}
	conditions := []string{"g.our_jid=$1", "g.is_deleted=false"}
	if !options.IncludeLeft {
		conditions = append(conditions, "g.is_joined=true")
	}
	if options.Keyword != "" {
		placeholder := appendSQLArg(&args, "%"+strings.ToLower(options.Keyword)+"%")
		conditions = append(conditions, fmt.Sprintf(`(
			LOWER(COALESCE(g.name, '')) LIKE %[1]s OR
			LOWER(COALESCE(g.topic, '')) LIKE %[1]s OR
			LOWER(g.group_jid) LIKE %[1]s OR
			LOWER(COALESCE(g.owner_jid, '')) LIKE %[1]s OR
			LOWER(COALESCE(g.owner_pn, '')) LIKE %[1]s
		)`, placeholder))
	}
	return " WHERE " + strings.Join(conditions, " AND "), args
}

func scanGroupListPageEntry(row dbutil.Scannable) (store.GroupListPageEntry, error) {
	var entry store.GroupListPageEntry
	var disappearingTimer int64
	var memberAddMode string
	err := row.Scan(
		&entry.GroupID,
		&entry.OwnerJID,
		&entry.Name,
		&entry.Topic,
		&entry.IsLocked,
		&entry.IsAnnounce,
		&entry.IsEphemeral,
		&disappearingTimer,
		&entry.IsIncognito,
		&entry.IsParent,
		&entry.DefaultMembershipApprovalMode,
		&entry.LinkedParentID,
		&entry.IsDefaultSubGroup,
		&entry.IsJoinApprovalRequired,
		&entry.ParticipantCount,
		&memberAddMode,
		&entry.Suspended,
	)
	entry.DisappearingTimer = uint32(disappearingTimer)
	entry.MemberAddMode = types.GroupMemberAddMode(memberAddMode)
	return entry, err
}

func (s *SQLStore) GetGroupListPage(ctx context.Context, options store.GroupListPageOptions) (store.GroupListPage, error) {
	options = normalizeGroupListPageOptions(options)
	page := store.GroupListPage{
		List:     []store.GroupListPageEntry{},
		Page:     options.Page,
		PageSize: options.PageSize,
	}
	where, args := buildGroupListWhere(options)
	args[0] = s.JID

	countQuery := "SELECT COUNT(*) FROM whatsmeow_groups g" + where
	if err := s.db.QueryRow(ctx, countQuery, args...).Scan(&page.Total); err != nil {
		return page, err
	}
	if page.Total > 0 {
		page.TotalPages = (page.Total + options.PageSize - 1) / options.PageSize
		page.HasMore = options.Page < page.TotalPages
	}

	offset := (options.Page - 1) * options.PageSize
	listArgs := append(args, options.PageSize, offset)
	listQuery := groupListSelect + where + `
		ORDER BY LOWER(COALESCE(g.name, '')) ASC, g.group_jid ASC
		LIMIT $` + fmt.Sprint(len(listArgs)-1) + ` OFFSET $` + fmt.Sprint(len(listArgs))
	rows, err := s.db.Query(ctx, listQuery, listArgs...)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		entry, err := scanGroupListPageEntry(rows)
		if err != nil {
			return page, fmt.Errorf("error scanning row: %w", err)
		}
		page.List = append(page.List, entry)
	}
	return page, rows.Err()
}

func (s *SQLStore) GetGroup(ctx context.Context, groupJID types.JID) (*store.GroupListPageEntry, error) {
	if groupJID.IsEmpty() {
		return nil, nil
	}
	entry, err := scanGroupListPageEntry(s.db.QueryRow(ctx, getGroupQuery, s.JID, jidString(groupJID)))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

func buildGroupMemberListWhere(options store.GroupMemberListPageOptions) (string, []any) {
	args := []any{nil, jidString(options.GroupJID)}
	conditions := []string{"our_jid=$1", "group_jid=$2"}
	if options.Status != groupMemberStatusAll {
		conditions = append(conditions, "status="+appendSQLArg(&args, options.Status))
	}
	switch options.Role {
	case groupMemberRoleAdmin:
		conditions = append(conditions, "(is_admin=true OR is_super_admin=true)")
	case groupMemberRoleMember:
		conditions = append(conditions, "is_admin=false AND is_super_admin=false")
	}
	if options.Keyword != "" {
		placeholder := appendSQLArg(&args, "%"+strings.ToLower(options.Keyword)+"%")
		conditions = append(conditions, fmt.Sprintf(`(
			LOWER(member_jid) LIKE %[1]s OR
			LOWER(COALESCE(phone_number, '')) LIKE %[1]s OR
			LOWER(COALESCE(lid, '')) LIKE %[1]s OR
			LOWER(COALESCE(display_name, '')) LIKE %[1]s
		)`, placeholder))
	}
	return " WHERE " + strings.Join(conditions, " AND "), args
}

func (s *SQLStore) GetGroupMemberListPage(ctx context.Context, options store.GroupMemberListPageOptions) (store.GroupMemberListPage, error) {
	options, err := normalizeGroupMemberListPageOptions(options)
	page := store.GroupMemberListPage{
		List:     []store.GroupMemberListPageEntry{},
		Page:     options.Page,
		PageSize: options.PageSize,
	}
	if err != nil {
		return page, err
	}
	if options.GroupJID.IsEmpty() {
		return page, fmt.Errorf("group JID is required")
	}

	where, args := buildGroupMemberListWhere(options)
	args[0] = s.JID

	countQuery := "SELECT COUNT(*) FROM whatsmeow_group_members" + where
	if err := s.db.QueryRow(ctx, countQuery, args...).Scan(&page.Total); err != nil {
		return page, err
	}
	if page.Total > 0 {
		page.TotalPages = (page.Total + options.PageSize - 1) / options.PageSize
		page.HasMore = options.Page < page.TotalPages
	}

	offset := (options.Page - 1) * options.PageSize
	listArgs := append(args, options.PageSize, offset)
	listQuery := `
		SELECT member_jid, COALESCE(phone_number, ''), COALESCE(lid, ''), display_name,
		       is_admin, is_super_admin, status
		FROM whatsmeow_group_members
	` + where + `
		ORDER BY is_super_admin DESC, is_admin DESC, LOWER(COALESCE(display_name, '')) ASC, member_jid ASC
		LIMIT $` + fmt.Sprint(len(listArgs)-1) + ` OFFSET $` + fmt.Sprint(len(listArgs))
	rows, err := s.db.Query(ctx, listQuery, listArgs...)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry store.GroupMemberListPageEntry
		err = rows.Scan(&entry.JID, &entry.PhoneNumber, &entry.LID, &entry.DisplayName, &entry.IsAdmin, &entry.IsSuperAdmin, &entry.Status)
		if err != nil {
			return page, fmt.Errorf("error scanning row: %w", err)
		}
		page.List = append(page.List, entry)
	}
	return page, rows.Err()
}
