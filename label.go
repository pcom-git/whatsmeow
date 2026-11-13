// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// labelStorePageSize is the page size used when reading the local label store to allocate IDs or
// look up existing labels. WhatsApp accounts have at most a few dozen labels, so a single page
// is more than enough.
const labelStorePageSize = 500

// GetLabels returns a page of locally stored labels without making network requests.
func (cli *Client) GetLabels(ctx context.Context, opts store.LabelListPageOptions) (store.LabelListPage, error) {
	if cli == nil {
		return store.LabelListPage{}, ErrClientIsNil
	}
	if cli.Store == nil || cli.Store.Labels == nil {
		return store.LabelListPage{}, fmt.Errorf("label store is nil")
	}
	return cli.Store.Labels.GetLabels(ctx, opts)
}

// GetLabelMembers returns a page of locally stored contacts and groups in the given label.
//
// If labelID is empty, the active Favorites label (type=3) is used. If that label hasn't arrived yet
// but a favorites snapshot has, the pending favorites members are returned.
func (cli *Client) GetLabelMembers(ctx context.Context, labelID string, opts store.LabelMemberListPageOptions) (store.LabelMemberListPage, error) {
	if cli == nil {
		return store.LabelMemberListPage{}, ErrClientIsNil
	}
	if cli.Store == nil || cli.Store.Labels == nil {
		return store.LabelMemberListPage{}, fmt.Errorf("label store is nil")
	}
	return cli.Store.Labels.GetLabelMembers(ctx, labelID, opts)
}


// allocateLabelID picks the next free numeric label ID based on the locally stored labels.
// It returns max(existing numeric IDs) + 1 (at least "1").
func (cli *Client) allocateLabelID(ctx context.Context) (string, error) {
	page, err := cli.Store.Labels.GetLabels(ctx, store.LabelListPageOptions{
		Page:            1,
		PageSize:        labelStorePageSize,
		IncludeDeleted:  true,
		IncludeInactive: true,
	})
	if err != nil {
		return "", err
	}
	maxID := 0
	for _, label := range page.List {
		if n, convErr := strconv.Atoi(label.LabelID); convErr == nil && n > maxID {
			maxID = n
		}
	}
	return strconv.Itoa(maxID + 1), nil
}

// getLabelInfo returns the locally stored label with the given ID, or nil if it isn't known yet.
func (cli *Client) getLabelInfo(ctx context.Context, labelID string) (*store.LabelInfo, error) {
	page, err := cli.Store.Labels.GetLabels(ctx, store.LabelListPageOptions{
		Page:            1,
		PageSize:        labelStorePageSize,
		IncludeDeleted:  true,
		IncludeInactive: true,
	})
	if err != nil {
		return nil, err
	}
	for i := range page.List {
		if page.List[i].LabelID == labelID {
			info := page.List[i]
			return &info, nil
		}
	}
	return nil, nil
}

// CreateLabel creates a new custom label with the given name and color and returns the new label ID.
//
// The ID is allocated automatically from the local label store, and the label is marked as a custom,
// active label. The new label is also written to the local store optimistically so it shows up in
// GetLabels immediately, before the change echoes back from the server.
func (cli *Client) CreateLabel(ctx context.Context, name string, color int32) (string, error) {
	if cli == nil {
		return "", ErrClientIsNil
	}
	if cli.Store == nil || cli.Store.Labels == nil {
		return "", fmt.Errorf("label store is nil")
	}
	labelID, err := cli.allocateLabelID(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to allocate label ID: %w", err)
	}
	action := &waSyncAction.LabelEditAction{
		Name:     proto.String(name),
		Color:    proto.Int32(color),
		Deleted:  proto.Bool(false),
		Type:     waSyncAction.LabelEditAction_CUSTOM.Enum(),
		IsActive: proto.Bool(true),
	}
	if err = cli.SendAppState(ctx, appstate.BuildLabelEditFull(labelID, action)); err != nil {
		return "", err
	}
	if putErr := cli.Store.Labels.PutLabel(ctx, store.LabelInfo{
		LabelID:       labelID,
		Name:          name,
		Type:          int32(waSyncAction.LabelEditAction_CUSTOM),
		Color:         color,
		IsActive:      true,
		LastEventTime: time.Now(),
	}); putErr != nil {
		cli.Log.Warnf("Failed to optimistically store created label %s: %v", labelID, putErr)
	}
	return labelID, nil
}

// EditLabel updates the name and color of an existing label.
//
// The label's type and other metadata are preserved from the local store so they aren't reset.
// Immutable labels (e.g. predefined system labels) cannot be edited.
func (cli *Client) EditLabel(ctx context.Context, labelID, name string, color int32) error {
	if cli == nil {
		return ErrClientIsNil
	}
	if cli.Store == nil || cli.Store.Labels == nil {
		return fmt.Errorf("label store is nil")
	}
	if labelID == "" {
		return fmt.Errorf("label ID is empty")
	}
	existing, err := cli.getLabelInfo(ctx, labelID)
	if err != nil {
		return err
	}
	if existing != nil && existing.IsImmutable {
		return fmt.Errorf("label %s is immutable and cannot be edited", labelID)
	}
	action := &waSyncAction.LabelEditAction{
		Name:    proto.String(name),
		Color:   proto.Int32(color),
		Deleted: proto.Bool(false),
	}
	if existing != nil {
		action.Type = waSyncAction.LabelEditAction_ListType(existing.Type).Enum()
		action.IsActive = proto.Bool(existing.IsActive)
		if existing.PredefinedID != 0 {
			action.PredefinedID = proto.Int32(existing.PredefinedID)
		}
		if existing.OrderIndex != 0 {
			action.OrderIndex = proto.Int32(existing.OrderIndex)
		}
	} else {
		action.Type = waSyncAction.LabelEditAction_CUSTOM.Enum()
		action.IsActive = proto.Bool(true)
	}
	if err = cli.SendAppState(ctx, appstate.BuildLabelEditFull(labelID, action)); err != nil {
		return err
	}
	if existing != nil {
		existing.Name = name
		existing.Color = color
		existing.LastEventTime = time.Now()
		if putErr := cli.Store.Labels.PutLabel(ctx, *existing); putErr != nil {
			cli.Log.Warnf("Failed to optimistically store edited label %s: %v", labelID, putErr)
		}
	}
	return nil
}

// DeleteLabel deletes an existing label. Immutable labels cannot be deleted.
func (cli *Client) DeleteLabel(ctx context.Context, labelID string) error {
	if cli == nil {
		return ErrClientIsNil
	}
	if cli.Store == nil || cli.Store.Labels == nil {
		return fmt.Errorf("label store is nil")
	}
	if labelID == "" {
		return fmt.Errorf("label ID is empty")
	}
	existing, err := cli.getLabelInfo(ctx, labelID)
	if err != nil {
		return err
	}
	if existing != nil && existing.IsImmutable {
		return fmt.Errorf("label %s is immutable and cannot be deleted", labelID)
	}
	action := &waSyncAction.LabelEditAction{
		Deleted: proto.Bool(true),
	}
	if existing != nil {
		action.Name = proto.String(existing.Name)
		action.Color = proto.Int32(existing.Color)
		action.Type = waSyncAction.LabelEditAction_ListType(existing.Type).Enum()
	}
	if err = cli.SendAppState(ctx, appstate.BuildLabelEditFull(labelID, action)); err != nil {
		return err
	}
	if existing != nil {
		existing.Deleted = true
		existing.LastEventTime = time.Now()
		if putErr := cli.Store.Labels.PutLabel(ctx, *existing); putErr != nil {
			cli.Log.Warnf("Failed to optimistically store deleted label %s: %v", labelID, putErr)
		}
	}
	return nil
}

// SetChatLabel adds (labeled=true) or removes (labeled=false) a single chat (contact or group)
// to/from the given label.
func (cli *Client) SetChatLabel(ctx context.Context, labelID string, chat types.JID, labeled bool) error {
	return cli.SetChatLabels(ctx, labelID, []types.JID{chat}, labeled)
}

// SetChatLabels adds or removes multiple chats (contacts or groups) to/from the given label in a
// single app state patch. Membership changes are also written to the local store optimistically.
func (cli *Client) SetChatLabels(ctx context.Context, labelID string, chats []types.JID, labeled bool) error {
	if cli == nil {
		return ErrClientIsNil
	}
	if cli.Store == nil || cli.Store.Labels == nil {
		return fmt.Errorf("label store is nil")
	}
	if labelID == "" {
		return fmt.Errorf("label ID is empty")
	}
	targets := make([]types.JID, 0, len(chats))
	seen := make(map[types.JID]struct{}, len(chats))
	for _, chat := range chats {
		chat = chat.ToNonAD()
		if chat.IsEmpty() {
			continue
		}
		if _, ok := seen[chat]; ok {
			continue
		}
		seen[chat] = struct{}{}
		targets = append(targets, chat)
	}
	if len(targets) == 0 {
		return fmt.Errorf("no valid chats provided")
	}
	if err := cli.SendAppState(ctx, appstate.BuildLabelChatBatch(targets, labelID, labeled)); err != nil {
		return err
	}
	now := time.Now()
	for _, chat := range targets {
		if putErr := cli.Store.Labels.PutLabelMember(ctx, store.LabelMemberInfo{
			LabelID:       labelID,
			ChatJID:       chat,
			ChatType:      store.LabelChatTypeForJID(chat),
			Labeled:       labeled,
			Source:        store.LabelSourceAssociation,
			LastEventTime: now,
		}); putErr != nil {
			cli.Log.Warnf("Failed to optimistically store label member %s/%s: %v", labelID, chat, putErr)
		}
	}
	return nil
}
