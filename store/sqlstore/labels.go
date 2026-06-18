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

const pendingFavoritesLabelID = "__wm_pending_favorites__"

var _ store.LabelStore = (*SQLStore)(nil)

const (
	upsertLabelQuery = `
		INSERT INTO whatsmeow_labels (
			our_jid, label_id, name, type, color, predefined_id, deleted,
			is_active, order_index, is_immutable, mute_end_time_ms, is_pending,
			last_event_time, from_full_sync, updated_at, raw_action
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7,
			$8, $9, $10, $11, $12,
			$13, $14, $15, $16
		)
		ON CONFLICT (our_jid, label_id) DO UPDATE SET
			name=excluded.name,
			type=excluded.type,
			color=excluded.color,
			predefined_id=excluded.predefined_id,
			deleted=excluded.deleted,
			is_active=excluded.is_active,
			order_index=excluded.order_index,
			is_immutable=excluded.is_immutable,
			mute_end_time_ms=excluded.mute_end_time_ms,
			is_pending=excluded.is_pending,
			last_event_time=excluded.last_event_time,
			from_full_sync=excluded.from_full_sync,
			updated_at=excluded.updated_at,
			raw_action=excluded.raw_action
		WHERE excluded.last_event_time >= whatsmeow_labels.last_event_time
	`
	ensureLabelQuery = `
		INSERT INTO whatsmeow_labels (our_jid, label_id, is_pending, updated_at)
		VALUES ($1, $2, true, $3)
		ON CONFLICT (our_jid, label_id) DO NOTHING
	`
	ensurePendingFavoritesLabelQuery = `
		INSERT INTO whatsmeow_labels (
			our_jid, label_id, name, type, is_active, is_pending, updated_at
		) VALUES (
			$1, $2, 'Favorites', $3, true, true, $4
		)
		ON CONFLICT (our_jid, label_id) DO UPDATE SET
			type=excluded.type,
			is_active=true,
			is_pending=true,
			updated_at=excluded.updated_at
	`
	upsertLabelMemberQuery = `
		INSERT INTO whatsmeow_label_members (
			our_jid, label_id, chat_jid, chat_type, labeled, source,
			last_event_time, from_full_sync, updated_at, raw_action
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10
		)
		ON CONFLICT (our_jid, label_id, chat_jid) DO UPDATE SET
			chat_type=excluded.chat_type,
			labeled=excluded.labeled,
			source=excluded.source,
			last_event_time=excluded.last_event_time,
			from_full_sync=excluded.from_full_sync,
			updated_at=excluded.updated_at,
			raw_action=excluded.raw_action
		WHERE excluded.last_event_time >= whatsmeow_label_members.last_event_time
	`
	getActiveFavoritesLabelIDQuery = `
		SELECT label_id
		FROM whatsmeow_labels
		WHERE our_jid=$1 AND type=$2 AND is_pending=false AND deleted=false AND is_active=true
		ORDER BY order_index ASC, label_id ASC
		LIMIT 1
	`
	getPendingFavoritesLabelIDQuery = `
		SELECT label_id
		FROM whatsmeow_labels
		WHERE our_jid=$1 AND label_id=$2 AND is_pending=true
		LIMIT 1
	`
	markOldFavoriteMembersQuery = `
		UPDATE whatsmeow_label_members
		SET labeled=false, last_event_time=$4, from_full_sync=$5, updated_at=$6
		WHERE our_jid=$1 AND label_id=$2 AND source=$3 AND last_event_time <= $4
	`
	deletePendingFavoritesLabelQuery = `
		DELETE FROM whatsmeow_labels WHERE our_jid=$1 AND label_id=$2
	`
)

func labelEventUnixMilli(t time.Time) int64 {
	if t.IsZero() {
		return time.Now().UnixMilli()
	}
	return t.UnixMilli()
}

func nullableString(val string) any {
	if val == "" {
		return nil
	}
	return val
}

func normalizeLabelListPageOptions(options store.LabelListPageOptions) store.LabelListPageOptions {
	if options.Page <= 0 {
		options.Page = 1
	}
	if options.PageSize <= 0 {
		options.PageSize = 50
	} else if options.PageSize > 500 {
		options.PageSize = 500
	}
	return options
}

func normalizeLabelMemberListPageOptions(options store.LabelMemberListPageOptions) (store.LabelMemberListPageOptions, error) {
	if options.Page <= 0 {
		options.Page = 1
	}
	if options.PageSize <= 0 {
		options.PageSize = 50
	} else if options.PageSize > 500 {
		options.PageSize = 500
	}
	options.ChatType = strings.ToLower(strings.TrimSpace(options.ChatType))
	switch options.ChatType {
	case "", store.LabelChatTypeContact, store.LabelChatTypeGroup, store.LabelChatTypeUnknown:
	default:
		return options, fmt.Errorf("invalid label member chat type %q", options.ChatType)
	}
	return options, nil
}

func (s *SQLStore) ensureLabel(ctx context.Context, labelID string) error {
	if labelID == "" {
		return nil
	}
	_, err := s.db.Exec(ctx, ensureLabelQuery, s.JID, labelID, time.Now().UnixMilli())
	return err
}

func (s *SQLStore) ensurePendingFavoritesLabel(ctx context.Context) error {
	_, err := s.db.Exec(ctx, ensurePendingFavoritesLabelQuery, s.JID, pendingFavoritesLabelID, store.LabelTypeFavorites, time.Now().UnixMilli())
	return err
}

func (s *SQLStore) PutLabel(ctx context.Context, label store.LabelInfo) error {
	if label.LabelID == "" {
		return nil
	}
	eventTS := labelEventUnixMilli(label.LastEventTime)
	updatedAt := time.Now().UnixMilli()
	return s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		_, err := s.db.Exec(ctx, upsertLabelQuery,
			s.JID,
			label.LabelID,
			label.Name,
			label.Type,
			label.Color,
			label.PredefinedID,
			label.Deleted,
			label.IsActive,
			label.OrderIndex,
			label.IsImmutable,
			label.MuteEndTimeMS,
			label.IsPending,
			eventTS,
			label.FromFullSync,
			updatedAt,
			nullableString(label.RawAction),
		)
		if err != nil {
			return err
		}
		if label.Type == store.LabelTypeFavorites && !label.IsPending && label.IsActive && !label.Deleted {
			return s.migratePendingFavoriteMembers(ctx, label.LabelID)
		}
		return nil
	})
}

func (s *SQLStore) PutLabelMember(ctx context.Context, member store.LabelMemberInfo) error {
	return s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		return s.putLabelMember(ctx, member)
	})
}

func (s *SQLStore) putLabelMember(ctx context.Context, member store.LabelMemberInfo) error {
	if member.LabelID == "" || member.ChatJID.IsEmpty() {
		return nil
	}
	chatJID := member.ChatJID.ToNonAD()
	chatType := member.ChatType
	if chatType == "" {
		chatType = store.LabelChatTypeForJID(chatJID)
	}
	source := member.Source
	if source == "" {
		source = store.LabelSourceAssociation
	}
	if err := s.ensureLabel(ctx, member.LabelID); err != nil {
		return err
	}
	eventTS := labelEventUnixMilli(member.LastEventTime)
	_, err := s.db.Exec(ctx, upsertLabelMemberQuery,
		s.JID,
		member.LabelID,
		jidString(chatJID),
		chatType,
		member.Labeled,
		source,
		eventTS,
		member.FromFullSync,
		time.Now().UnixMilli(),
		nullableString(member.RawAction),
	)
	return err
}

func (s *SQLStore) ReplaceFavoriteMembers(ctx context.Context, members []types.JID, ts time.Time, fromFullSync bool) error {
	eventTS := labelEventUnixMilli(ts)
	return s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		labelID, err := s.getActiveFavoritesLabelID(ctx)
		if err != nil {
			return err
		}
		if labelID == "" {
			labelID = pendingFavoritesLabelID
			if err = s.ensurePendingFavoritesLabel(ctx); err != nil {
				return err
			}
		}

		if _, err = s.db.Exec(ctx, markOldFavoriteMembersQuery,
			s.JID,
			labelID,
			store.LabelSourceFavorites,
			eventTS,
			fromFullSync,
			time.Now().UnixMilli(),
		); err != nil {
			return err
		}

		seen := make(map[types.JID]struct{}, len(members))
		for _, jid := range members {
			jid = jid.ToNonAD()
			if jid.IsEmpty() {
				continue
			}
			if _, ok := seen[jid]; ok {
				continue
			}
			seen[jid] = struct{}{}
			err = s.putLabelMember(ctx, store.LabelMemberInfo{
				LabelID:       labelID,
				ChatJID:       jid,
				ChatType:      store.LabelChatTypeForJID(jid),
				Labeled:       true,
				Source:        store.LabelSourceFavorites,
				LastEventTime: ts,
				FromFullSync:  fromFullSync,
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *SQLStore) getActiveFavoritesLabelID(ctx context.Context) (string, error) {
	var labelID string
	err := s.db.QueryRow(ctx, getActiveFavoritesLabelIDQuery, s.JID, store.LabelTypeFavorites).Scan(&labelID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return labelID, err
}

func (s *SQLStore) getPendingFavoritesLabelID(ctx context.Context) (string, error) {
	var labelID string
	err := s.db.QueryRow(ctx, getPendingFavoritesLabelIDQuery, s.JID, pendingFavoritesLabelID).Scan(&labelID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return labelID, err
}

func (s *SQLStore) migratePendingFavoriteMembers(ctx context.Context, realLabelID string) error {
	if realLabelID == "" || realLabelID == pendingFavoritesLabelID {
		return nil
	}
	rows, err := s.db.Query(ctx, `
		SELECT chat_jid, chat_type, labeled, source, last_event_time, from_full_sync, raw_action
		FROM whatsmeow_label_members
		WHERE our_jid=$1 AND label_id=$2
	`, s.JID, pendingFavoritesLabelID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var member store.LabelMemberInfo
		var lastEventMS int64
		var rawAction sql.NullString
		member.LabelID = realLabelID
		err = rows.Scan(&member.ChatJID, &member.ChatType, &member.Labeled, &member.Source, &lastEventMS, &member.FromFullSync, &rawAction)
		if err != nil {
			return fmt.Errorf("error scanning pending favorite member: %w", err)
		}
		if lastEventMS > 0 {
			member.LastEventTime = time.UnixMilli(lastEventMS)
		}
		if rawAction.Valid {
			member.RawAction = rawAction.String
		}
		if err = s.putLabelMember(ctx, member); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	_, err = s.db.Exec(ctx, deletePendingFavoritesLabelQuery, s.JID, pendingFavoritesLabelID)
	return err
}

func buildLabelListWhere(options store.LabelListPageOptions) (string, []any) {
	args := []any{nil}
	conditions := []string{"l.our_jid=$1", "l.is_pending=false"}
	if !options.IncludeDeleted {
		conditions = append(conditions, "l.deleted=false")
	}
	if !options.IncludeInactive {
		conditions = append(conditions, "l.is_active=true")
	}
	if options.Type != nil {
		conditions = append(conditions, "l.type="+appendSQLArg(&args, *options.Type))
	}
	return " WHERE " + strings.Join(conditions, " AND "), args
}

func scanLabelInfo(row dbutil.Scannable, includeCount bool) (store.LabelInfo, error) {
	var label store.LabelInfo
	var lastEventMS int64
	var rawAction sql.NullString
	var memberCount sql.NullInt64
	scanArgs := []any{
		&label.LabelID,
		&label.Name,
		&label.Type,
		&label.Color,
		&label.PredefinedID,
		&label.Deleted,
		&label.IsActive,
		&label.OrderIndex,
		&label.IsImmutable,
		&label.MuteEndTimeMS,
		&label.IsPending,
		&lastEventMS,
		&label.FromFullSync,
		&rawAction,
	}
	if includeCount {
		scanArgs = append(scanArgs, &memberCount)
	}
	err := row.Scan(scanArgs...)
	if err != nil {
		return label, err
	}
	if lastEventMS > 0 {
		label.LastEventTime = time.UnixMilli(lastEventMS)
	}
	if rawAction.Valid {
		label.RawAction = rawAction.String
	}
	if memberCount.Valid {
		count := int(memberCount.Int64)
		label.MemberCount = &count
	}
	return label, nil
}

func (s *SQLStore) GetLabels(ctx context.Context, options store.LabelListPageOptions) (store.LabelListPage, error) {
	options = normalizeLabelListPageOptions(options)
	page := store.LabelListPage{
		List:     []store.LabelInfo{},
		Page:     options.Page,
		PageSize: options.PageSize,
	}
	where, args := buildLabelListWhere(options)
	args[0] = s.JID

	countQuery := "SELECT COUNT(*) FROM whatsmeow_labels l" + where
	if err := s.db.QueryRow(ctx, countQuery, args...).Scan(&page.Total); err != nil {
		return page, err
	}
	if page.Total > 0 {
		page.TotalPages = (page.Total + options.PageSize - 1) / options.PageSize
		page.HasMore = options.Page < page.TotalPages
	}

	offset := (options.Page - 1) * options.PageSize
	listArgs := append(args, options.PageSize, offset)
	selectColumns := `
		SELECT
			l.label_id, l.name, l.type, l.color, l.predefined_id, l.deleted,
			l.is_active, l.order_index, l.is_immutable, l.mute_end_time_ms,
			l.is_pending, l.last_event_time, l.from_full_sync, l.raw_action
	`
	countJoin := ""
	if options.IncludeCounts {
		selectColumns += ", COALESCE(member_counts.member_count, 0)"
		countJoin = `
			LEFT JOIN (
				SELECT our_jid, label_id, COUNT(*) AS member_count
				FROM whatsmeow_label_members
				WHERE our_jid=$1 AND labeled=true
				GROUP BY our_jid, label_id
			) member_counts
			  ON member_counts.our_jid=l.our_jid AND member_counts.label_id=l.label_id
		`
	}
	listQuery := selectColumns + `
		FROM whatsmeow_labels l
	` + countJoin + where + `
		ORDER BY l.order_index ASC, l.label_id ASC
		LIMIT $` + fmt.Sprint(len(listArgs)-1) + ` OFFSET $` + fmt.Sprint(len(listArgs))
	rows, err := s.db.Query(ctx, listQuery, listArgs...)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		label, err := scanLabelInfo(rows, options.IncludeCounts)
		if err != nil {
			return page, fmt.Errorf("error scanning row: %w", err)
		}
		page.List = append(page.List, label)
	}
	return page, rows.Err()
}

func (s *SQLStore) resolveDefaultLabelMemberID(ctx context.Context) (string, bool, error) {
	labelID, err := s.getActiveFavoritesLabelID(ctx)
	if err != nil || labelID != "" {
		return labelID, false, err
	}
	labelID, err = s.getPendingFavoritesLabelID(ctx)
	if err != nil || labelID == "" {
		return labelID, false, err
	}
	return labelID, true, nil
}

func (s *SQLStore) isQueryableLabel(ctx context.Context, labelID string, allowPending bool) (bool, error) {
	query := `
		SELECT true
		FROM whatsmeow_labels
		WHERE our_jid=$1 AND label_id=$2
	`
	if allowPending {
		query += " AND is_pending=true"
	} else {
		query += " AND is_pending=false AND deleted=false AND is_active=true"
	}
	var ok bool
	err := s.db.QueryRow(ctx, query, s.JID, labelID).Scan(&ok)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return ok, err
}

func buildLabelMemberListWhere(labelID string, options store.LabelMemberListPageOptions) (string, []any) {
	args := []any{nil, labelID}
	conditions := []string{"lm.our_jid=$1", "lm.label_id=$2"}
	if !options.IncludeUnlabeled {
		conditions = append(conditions, "lm.labeled=true")
	}
	if options.ChatType != "" {
		conditions = append(conditions, "lm.chat_type="+appendSQLArg(&args, options.ChatType))
	}
	return " WHERE " + strings.Join(conditions, " AND "), args
}

func displayNameForLabelMember(contact types.ContactInfo, jid types.JID, chatType, groupName string) string {
	switch chatType {
	case store.LabelChatTypeContact:
		switch {
		case contact.FullName != "":
			return contact.FullName
		case contact.FirstName != "":
			return contact.FirstName
		case contact.BusinessName != "":
			return contact.BusinessName
		case contact.PushName != "":
			return contact.PushName
		default:
			return "+" + jid.User
		}
	case store.LabelChatTypeGroup:
		return groupName
	default:
		return ""
	}
}

func (s *SQLStore) GetLabelMembers(ctx context.Context, labelID string, options store.LabelMemberListPageOptions) (store.LabelMemberListPage, error) {
	options, err := normalizeLabelMemberListPageOptions(options)
	page := store.LabelMemberListPage{
		List:     []store.LabelMemberInfo{},
		Page:     options.Page,
		PageSize: options.PageSize,
	}
	if err != nil {
		return page, err
	}

	allowPending := false
	if labelID == "" {
		labelID, allowPending, err = s.resolveDefaultLabelMemberID(ctx)
		if err != nil {
			return page, err
		}
		if labelID == "" {
			return page, nil
		}
	}
	ok, err := s.isQueryableLabel(ctx, labelID, allowPending)
	if err != nil || !ok {
		return page, err
	}

	where, args := buildLabelMemberListWhere(labelID, options)
	args[0] = s.JID

	countQuery := "SELECT COUNT(*) FROM whatsmeow_label_members lm" + where
	if err = s.db.QueryRow(ctx, countQuery, args...).Scan(&page.Total); err != nil {
		return page, err
	}
	if page.Total > 0 {
		page.TotalPages = (page.Total + options.PageSize - 1) / options.PageSize
		page.HasMore = options.Page < page.TotalPages
	}

	offset := (options.Page - 1) * options.PageSize
	listArgs := append(args, options.PageSize, offset)
	listQuery := `
		SELECT
			lm.label_id, lm.chat_jid, lm.chat_type, lm.labeled, lm.source,
			lm.last_event_time, lm.from_full_sync, lm.raw_action,
			COALESCE(c.first_name, ''), COALESCE(c.full_name, ''),
			COALESCE(c.push_name, ''), COALESCE(c.business_name, ''),
			COALESCE(g.name, '')
		FROM whatsmeow_label_members lm
		LEFT JOIN whatsmeow_contacts c
		  ON c.our_jid=lm.our_jid
		 AND c.their_jid=lm.chat_jid
		 AND lm.chat_type='contact'
		LEFT JOIN whatsmeow_groups g
		  ON g.our_jid=lm.our_jid
		 AND g.group_jid=lm.chat_jid
		 AND lm.chat_type='group'
	` + where + `
		ORDER BY lm.chat_type ASC, lm.chat_jid ASC
		LIMIT $` + fmt.Sprint(len(listArgs)-1) + ` OFFSET $` + fmt.Sprint(len(listArgs))
	rows, err := s.db.Query(ctx, listQuery, listArgs...)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var member store.LabelMemberInfo
		var lastEventMS int64
		var rawAction sql.NullString
		var contact types.ContactInfo
		var groupName string
		err = rows.Scan(
			&member.LabelID,
			&member.ChatJID,
			&member.ChatType,
			&member.Labeled,
			&member.Source,
			&lastEventMS,
			&member.FromFullSync,
			&rawAction,
			&contact.FirstName,
			&contact.FullName,
			&contact.PushName,
			&contact.BusinessName,
			&groupName,
		)
		if err != nil {
			return page, fmt.Errorf("error scanning row: %w", err)
		}
		member.DisplayName = displayNameForLabelMember(contact, member.ChatJID, member.ChatType, groupName)
		if lastEventMS > 0 {
			member.LastEventTime = time.UnixMilli(lastEventMS)
		}
		if rawAction.Valid {
			member.RawAction = rawAction.String
		}
		page.List = append(page.List, member)
	}
	return page, rows.Err()
}
