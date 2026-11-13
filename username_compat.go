// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

// localContactUsernameStore is an optional, temporary capability. Keeping it
// separate from ContactStore lets other storage implementations remain usable.
type localContactUsernameStore interface {
	PutLocalContactUsername(ctx context.Context, user types.JID, username string) error
}

// storeMessageUsernames saves raw message attributes without extending the
// upstream MessageInfo or event types. It runs before message dispatch so a
// message handler can immediately query the updated contact.
func (cli *Client) storeMessageUsernames(ctx context.Context, node *waBinary.Node, info *types.MessageInfo) {
	contacts, ok := cli.Store.Contacts.(localContactUsernameStore)
	if !ok {
		return
	}
	ag := node.AttrGetter()
	cli.storeContactUsername(ctx, contacts, info.Sender, info.SenderAlt, ag.OptionalString("username"))
	if info.IsFromMe && !info.IsGroup {
		cli.storeContactUsername(ctx, contacts, info.Chat, info.RecipientAlt, ag.OptionalString("peer_recipient_username"))
	}
}

func (cli *Client) storeContactUsername(ctx context.Context, contacts localContactUsernameStore, user, alt types.JID, username string) {
	user = localUsernameJID(user)
	if username == "" || !isLocalUsernameUser(user) {
		return
	}
	if err := contacts.PutLocalContactUsername(ctx, user, username); err != nil {
		cli.Log.Warnf("Failed to save local username of %s: %v", user, err)
	}
	alt = localUsernameJID(alt)
	if alt.IsEmpty() && cli.Store.LIDs != nil {
		var err error
		alt, err = cli.Store.GetAltJID(ctx, user)
		if err != nil {
			cli.Log.Warnf("Failed to resolve alternate JID for local username of %s: %v", user, err)
		}
		alt = localUsernameJID(alt)
	}
	// Check the alternate independently, even if the primary value was unchanged:
	// a PN/LID mapping may have become available since the last username update.
	if alt != user && isLocalUsernameUser(alt) {
		if err := contacts.PutLocalContactUsername(ctx, alt, username); err != nil {
			cli.Log.Warnf("Failed to save local username of %s: %v", alt, err)
		}
	}
}

func isLocalUsernameUser(jid types.JID) bool {
	return jid.User != "" && (jid.Server == types.DefaultUserServer || jid.Server == types.HiddenUserServer)
}

func localUsernameJID(jid types.JID) types.JID {
	jid = jid.ToNonAD()
	switch jid.Server {
	case types.HostedServer:
		jid.Server = types.DefaultUserServer
	case types.HostedLIDServer:
		jid.Server = types.HiddenUserServer
	}
	return jid
}
