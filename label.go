// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"fmt"

	"go.mau.fi/whatsmeow/store"
)

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
