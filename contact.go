// Copyright (c) 2021 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

// ErrAddContactMissingIdentifier is returned when AddContact is called without a phone number or username.
var ErrAddContactMissingIdentifier = errors.New("phone number or username is required")

// AddContactParams contains the inputs for adding a WhatsApp contact.
type AddContactParams struct {
	PhoneNumber              string
	Username                 string
	FirstName                string
	FullName                 string
	SaveOnPrimaryAddressbook bool
}

// AddContactResult contains the resolved contact identifiers that were written to app state.
type AddContactResult struct {
	PhoneJID                 types.JID
	LIDJID                   types.JID
	Username                 string
	FirstName                string
	FullName                 string
	SaveOnPrimaryAddressbook bool
}

type addContactIdentity struct {
	PhoneJID types.JID
	LIDJID   types.JID
	Username string
}

// AddContact adds or updates a contact using the same usync preflight and app state patch flow used by WhatsApp Web.
func (cli *Client) AddContact(ctx context.Context, params AddContactParams) (*AddContactResult, error) {
	if cli == nil {
		return nil, ErrClientIsNil
	}
	phoneNumber := normalizeAddContactPhone(params.PhoneNumber)
	username := normalizeAddContactUsername(params.Username)
	if phoneNumber == "" && username == "" {
		return nil, ErrAddContactMissingIdentifier
	}
	firstName := strings.TrimSpace(params.FirstName)
	fullName := strings.TrimSpace(params.FullName)
	if firstName == "" {
		firstName = fullName
	}
	if fullName == "" {
		fullName = firstName
	}

	cli.Log.Infof("Adding contact: phone_present=%t username_present=%t save_on_primary_addressbook=%t", phoneNumber != "", username != "", params.SaveOnPrimaryAddressbook)

	var identity addContactIdentity
	if phoneNumber != "" {
		cli.Log.Debugf("Resolving contact phone number with usync query/delta: phone=%s", phoneNumber)
		phoneIdentity, err := cli.resolveAddContactPhone(ctx, phoneNumber)
		if err != nil {
			return nil, err
		}
		identity = phoneIdentity
		cli.Log.Debugf("Resolved contact phone number: phone_jid=%s lid_jid=%s username=%s", phoneIdentity.PhoneJID, phoneIdentity.LIDJID, phoneIdentity.Username)
	}
	if username != "" {
		cli.Log.Debugf("Resolving contact username with usync query/delta: username=%s", username)
		usernameIdentity, err := cli.resolveAddContactUsername(ctx, username)
		if err != nil {
			return nil, err
		}
		if identity.LIDJID.IsEmpty() {
			identity.LIDJID = usernameIdentity.LIDJID
		} else if identity.LIDJID != usernameIdentity.LIDJID {
			return nil, fmt.Errorf("phone and username resolved to different LIDs: phone=%s username=%s", identity.LIDJID, usernameIdentity.LIDJID)
		}
		if identity.PhoneJID.IsEmpty() {
			identity.PhoneJID = usernameIdentity.PhoneJID
		}
		identity.Username = usernameIdentity.Username
		cli.Log.Debugf("Resolved contact username: lid_jid=%s username=%s", usernameIdentity.LIDJID, usernameIdentity.Username)
	}
	if identity.LIDJID.IsEmpty() {
		return nil, errors.New("failed to resolve contact LID")
	}
	if identity.Username == "" {
		identity.Username = username
	}

	patch := appstate.BuildContact(appstate.ContactInfo{
		PhoneJID:                 identity.PhoneJID,
		LIDJID:                   identity.LIDJID,
		Username:                 identity.Username,
		FirstName:                firstName,
		FullName:                 fullName,
		SaveOnPrimaryAddressbook: params.SaveOnPrimaryAddressbook,
	})
	actionType := "contact"
	if identity.PhoneJID.IsEmpty() {
		actionType = "lid_contact"
	}
	cli.Log.Debugf("Sending add contact app state patch: collection=%s action=%s index_jid=%s lid_jid=%s save_on_primary_addressbook=%t", patch.Type, actionType, patch.Mutations[0].Index[1], identity.LIDJID, params.SaveOnPrimaryAddressbook)
	if err := cli.SendAppState(ctx, patch); err != nil {
		return nil, fmt.Errorf("failed to send add contact app state patch: %w", err)
	}
	if err := cli.cacheAddContactLIDMapping(ctx, identity); err != nil {
		return nil, err
	}
	cli.Log.Debugf("Subscribing to added contact presence: lid_jid=%s", identity.LIDJID)
	if err := cli.SubscribePresence(ctx, identity.LIDJID); err != nil {
		cli.Log.Warnf("Failed to subscribe to added contact presence %s: %v", identity.LIDJID, err)
	}
	cli.Log.Infof("Added contact: phone_jid=%s lid_jid=%s username=%s save_on_primary_addressbook=%t", identity.PhoneJID, identity.LIDJID, identity.Username, params.SaveOnPrimaryAddressbook)
	return &AddContactResult{
		PhoneJID:                 identity.PhoneJID,
		LIDJID:                   identity.LIDJID,
		Username:                 identity.Username,
		FirstName:                firstName,
		FullName:                 fullName,
		SaveOnPrimaryAddressbook: params.SaveOnPrimaryAddressbook,
	}, nil
}

func (cli *Client) cacheAddContactLIDMapping(ctx context.Context, identity addContactIdentity) error {
	if identity.PhoneJID.IsEmpty() || identity.LIDJID.IsEmpty() || cli.Store == nil || cli.Store.LIDs == nil {
		return nil
	}
	if cli.Log != nil {
		cli.Log.Debugf("Caching added contact LID mapping: lid_jid=%s phone_jid=%s", identity.LIDJID, identity.PhoneJID)
	}
	if err := cli.Store.LIDs.PutLIDMapping(ctx, identity.LIDJID, identity.PhoneJID); err != nil {
		return fmt.Errorf("failed to cache added contact LID mapping %s -> %s: %w", identity.LIDJID, identity.PhoneJID, err)
	}
	return nil
}

func normalizeAddContactPhone(phone string) string {
	phone = strings.TrimSpace(phone)
	replacer := strings.NewReplacer(" ", "", "-", "", "(", "", ")", "")
	phone = replacer.Replace(phone)
	return strings.TrimPrefix(phone, "+")
}

func addContactPhoneJIDUser(phone string) string {
	return phone
}

func isAddContactPhoneDigits(phone string) bool {
	if phone == "" {
		return false
	}
	for i := range phone {
		if phone[i] < '0' || phone[i] > '9' {
			return false
		}
	}
	return true
}

// matchesAddContactPhone accepts only an exact match or the Brazilian ninth-digit
// difference in a server-resolved PN. It must not be used to rewrite requests.
func matchesAddContactPhone(phone, resolved string) bool {
	if !isAddContactPhoneDigits(phone) || !isAddContactPhoneDigits(resolved) {
		return false
	}
	if phone == resolved {
		return true
	}
	long, short := phone, resolved
	if len(long) < len(short) {
		long, short = short, long
	}
	return len(long) == 13 && len(short) == 12 &&
		strings.HasPrefix(long, "55") &&
		long[4] == '9' && long[:4] == short[:4] && long[5:] == short[4:]
}

func addContactPhoneQueryValue(phone string) string {
	if phone == "" || strings.HasPrefix(phone, "+") {
		return phone
	}
	return "+" + phone
}

func normalizeAddContactUsername(username string) string {
	return strings.TrimPrefix(strings.TrimSpace(username), "@")
}

func (cli *Client) resolveAddContactPhone(ctx context.Context, phone string) (addContactIdentity, error) {
	list, _, err := cli.sendAddContactPhoneUSync(ctx, phone, "query")
	if err != nil {
		return addContactIdentity{}, fmt.Errorf("failed to query contact phone %s: %w", phone, err)
	}
	identity, err := parsePhoneContactQueryResponse(list, phone)
	if err != nil {
		return addContactIdentity{}, err
	}
	list, result, err := cli.sendAddContactPhoneUSync(ctx, phone, "delta")
	if err != nil {
		return addContactIdentity{}, fmt.Errorf("failed to delta contact phone %s: %w", phone, err)
	}
	if err = parsePhoneContactDeltaResponse(list, result, phone, identity); err != nil {
		return addContactIdentity{}, err
	}
	return identity, nil
}

func (cli *Client) resolveAddContactUsername(ctx context.Context, username string) (addContactIdentity, error) {
	list, _, err := cli.sendAddContactUsernameQueryUSync(ctx, username)
	if err != nil {
		return addContactIdentity{}, fmt.Errorf("failed to query contact username %s: %w", username, err)
	}
	identity, err := parseUsernameContactQueryResponse(list, username)
	if err != nil {
		return addContactIdentity{}, err
	}
	list, result, err := cli.sendAddContactUsernameDeltaUSync(ctx, username, identity.LIDJID)
	if err != nil {
		return addContactIdentity{}, fmt.Errorf("failed to delta contact username %s: %w", username, err)
	}
	if err = parseUsernameContactDeltaResponse(list, result, username, identity.LIDJID); err != nil {
		return addContactIdentity{}, err
	}
	return identity, nil
}

func (cli *Client) sendAddContactPhoneUSync(ctx context.Context, phone, mode string) (*waBinary.Node, *waBinary.Node, error) {
	resp, err := cli.sendIQ(ctx, infoQuery{
		Namespace: "usync",
		Type:      iqGet,
		To:        types.ServerJID,
		Timeout:   32 * time.Second,
		Content: []waBinary.Node{
			buildPhoneAddContactUSyncNode(cli.generateRequestID(), phone, mode),
		},
	})
	if err != nil {
		return nil, nil, err
	}
	return parseAddContactUSyncResponse(resp)
}

func (cli *Client) sendAddContactUsernameQueryUSync(ctx context.Context, username string) (*waBinary.Node, *waBinary.Node, error) {
	resp, err := cli.sendIQ(ctx, infoQuery{
		Namespace: "usync",
		Type:      iqGet,
		To:        types.ServerJID,
		Timeout:   32 * time.Second,
		Content: []waBinary.Node{
			buildUsernameAddContactQueryUSyncNode(cli.generateRequestID(), username),
		},
	})
	if err != nil {
		return nil, nil, err
	}
	return parseAddContactUSyncResponse(resp)
}

func (cli *Client) sendAddContactUsernameDeltaUSync(ctx context.Context, username string, lid types.JID) (*waBinary.Node, *waBinary.Node, error) {
	resp, err := cli.sendIQ(ctx, infoQuery{
		Namespace: "usync",
		Type:      iqGet,
		To:        types.ServerJID,
		Timeout:   32 * time.Second,
		Content: []waBinary.Node{
			buildUsernameAddContactDeltaUSyncNode(cli.generateRequestID(), username, lid),
		},
	})
	if err != nil {
		return nil, nil, err
	}
	return parseAddContactUSyncResponse(resp)
}

func buildPhoneAddContactUSyncNode(requestID, phone, mode string) waBinary.Node {
	query := []waBinary.Node{
		{Tag: "contact", Attrs: waBinary.Attrs{"addressing_mode": string(types.AddressingModeLID)}},
	}
	if mode == "query" {
		query = append(query,
			waBinary.Node{Tag: "business", Content: []waBinary.Node{{Tag: "verified_name"}}},
			waBinary.Node{Tag: "disappearing_mode"},
		)
	}
	query = append(query, waBinary.Node{Tag: "username"})
	return waBinary.Node{
		Tag: "usync",
		Attrs: waBinary.Attrs{
			"sid":     requestID,
			"mode":    mode,
			"last":    "true",
			"index":   "0",
			"context": "interactive",
		},
		Content: []waBinary.Node{
			{Tag: "query", Content: query},
			{Tag: "list", Content: []waBinary.Node{{
				Tag:     "user",
				Content: []waBinary.Node{{Tag: "contact", Content: addContactPhoneQueryValue(phone)}},
			}}},
		},
	}
}

func buildUsernameAddContactQueryUSyncNode(requestID, username string) waBinary.Node {
	return waBinary.Node{
		Tag: "usync",
		Attrs: waBinary.Attrs{
			"sid":     requestID,
			"mode":    "query",
			"last":    "true",
			"index":   "0",
			"context": "interactive",
		},
		Content: []waBinary.Node{
			{Tag: "query", Content: []waBinary.Node{
				{Tag: "contact", Attrs: waBinary.Attrs{"addressing_mode": string(types.AddressingModeLID)}},
				{Tag: "business", Content: []waBinary.Node{{Tag: "verified_name"}}},
			}},
			{Tag: "list", Content: []waBinary.Node{{
				Tag:     "user",
				Content: []waBinary.Node{{Tag: "contact", Attrs: waBinary.Attrs{"username": username}}},
			}}},
		},
	}
}

func buildUsernameAddContactDeltaUSyncNode(requestID, username string, lid types.JID) waBinary.Node {
	return waBinary.Node{
		Tag: "usync",
		Attrs: waBinary.Attrs{
			"sid":     requestID,
			"mode":    "delta",
			"last":    "true",
			"index":   "0",
			"context": "interactive",
		},
		Content: []waBinary.Node{
			{Tag: "query", Content: []waBinary.Node{
				{Tag: "contact", Attrs: waBinary.Attrs{"addressing_mode": string(types.AddressingModeLID)}},
				{Tag: "username"},
			}},
			{Tag: "list", Content: []waBinary.Node{{
				Tag:   "user",
				Attrs: waBinary.Attrs{"jid": lid},
				Content: []waBinary.Node{
					{Tag: "contact", Attrs: waBinary.Attrs{"username": username}},
					{Tag: "username", Attrs: waBinary.Attrs{"username": username}},
				},
			}}},
		},
	}
}

func parseAddContactUSyncResponse(resp *waBinary.Node) (*waBinary.Node, *waBinary.Node, error) {
	usync, ok := resp.GetOptionalChildByTag("usync")
	if !ok {
		return nil, nil, &ElementMissingError{Tag: "usync", In: "response to add contact usync"}
	}
	list, ok := usync.GetOptionalChildByTag("list")
	if !ok {
		return nil, nil, &ElementMissingError{Tag: "list", In: "response to add contact usync"}
	}
	result, _ := usync.GetOptionalChildByTag("result")
	return &list, &result, nil
}

func parsePhoneContactQueryResponse(list *waBinary.Node, phone string) (addContactIdentity, error) {
	if !isAddContactPhoneDigits(phone) {
		return addContactIdentity{}, fmt.Errorf("invalid phone %q in contact query", phone)
	}
	if list == nil {
		return addContactIdentity{}, fmt.Errorf("missing user in phone contact query response for %s", phone)
	}
	users := list.GetChildrenByTag("user")
	if len(users) == 0 {
		return addContactIdentity{}, fmt.Errorf("missing user in phone contact query response for %s", phone)
	} else if len(users) != 1 {
		return addContactIdentity{}, fmt.Errorf("expected one user in phone contact query response for %s, got %d", phone, len(users))
	}
	user := users[0]
	contact, ok := user.GetOptionalChildByTag("contact")
	if !ok {
		return addContactIdentity{}, &ElementMissingError{Tag: "contact", In: "phone contact query response"}
	}
	contactType := contact.AttrGetter().OptionalString("type")
	if contactType != "in" {
		return addContactIdentity{}, fmt.Errorf("phone %s is not an active WhatsApp contact: type=%s", phone, contactType)
	}
	lid, err := parsePhoneContactJID(&user, "jid", types.HiddenUserServer)
	if err != nil {
		return addContactIdentity{}, err
	}
	pn := types.NewJID(addContactPhoneJIDUser(phone), types.DefaultUserServer)
	if _, present := user.Attrs["pn_jid"]; present {
		pn, err = parsePhoneContactJID(&user, "pn_jid", types.DefaultUserServer)
		if err != nil {
			return addContactIdentity{}, err
		}
		if !matchesAddContactPhone(phone, pn.User) {
			return addContactIdentity{}, fmt.Errorf("phone %s resolved to unexpected pn_jid %s", phone, pn)
		}
	}
	username := contact.AttrGetter().OptionalString("username")
	if username == "" {
		if usernameNode, present := user.GetOptionalChildByTag("username"); present {
			username = usernameNode.AttrGetter().OptionalString("username")
		}
	}
	return addContactIdentity{PhoneJID: pn, LIDJID: lid, Username: username}, nil
}

func parsePhoneContactJID(user *waBinary.Node, key, server string) (types.JID, error) {
	// ParseJID accepts some non-user forms, so validate the response attribute as
	// well as the parsed identity. An invalid PN must never become a fallback PN.
	if user == nil {
		return types.EmptyJID, fmt.Errorf("missing %s in phone contact query response", key)
	}
	if value, ok := user.Attrs[key].(string); ok {
		local, actualServer, valid := strings.Cut(value, "@")
		if !valid || actualServer != server {
			return types.EmptyJID, fmt.Errorf("invalid %s in phone contact query response", key)
		}
		// Validate before ToNonAD drops the suffixes: ParseJID casts signed
		// integers to uint8/uint16 without rejecting negative or overflowing values.
		userPart, device, hasDevice := strings.Cut(local, ":")
		if hasDevice {
			_, err := strconv.ParseUint(device, 10, 16)
			if !isAddContactPhoneDigits(device) || err != nil {
				return types.EmptyJID, fmt.Errorf("invalid %s device in phone contact query response", key)
			}
		}
		_, agent, hasAgent := strings.Cut(userPart, ".")
		if hasAgent {
			_, err := strconv.ParseUint(agent, 10, 8)
			if !isAddContactPhoneDigits(agent) || err != nil {
				return types.EmptyJID, fmt.Errorf("invalid %s agent in phone contact query response", key)
			}
		}
	}
	jid := optionalAddContactJID(user, key)
	if jid.Server != server || !isAddContactPhoneDigits(jid.User) {
		return types.EmptyJID, fmt.Errorf("missing or invalid %s in phone contact query response", key)
	}
	return jid, nil
}

func parsePhoneContactDeltaResponse(list, result *waBinary.Node, phone string, expected addContactIdentity) error {
	if err := requireAddContactIntegrity(result); err != nil {
		return err
	}
	identity, err := parsePhoneContactQueryResponse(list, phone)
	if err != nil {
		return err
	}
	if identity.LIDJID != expected.LIDJID {
		return fmt.Errorf("phone delta LID mismatch for %s: expected %s, got %s", phone, expected.LIDJID, identity.LIDJID)
	}
	// Compare the actual PN, not just its Brazilian alias. If query resolved an
	// alias, a missing delta PN falls back to the input and must fail this check.
	if identity.PhoneJID != expected.PhoneJID {
		return fmt.Errorf("phone delta PN mismatch for %s: expected %s, got %s", phone, expected.PhoneJID, identity.PhoneJID)
	}
	return nil
}

func parseUsernameContactQueryResponse(list *waBinary.Node, username string) (addContactIdentity, error) {
	for _, user := range list.GetChildrenByTag("user") {
		contact := user.GetChildByTag("contact")
		contactUsername := contact.AttrGetter().OptionalString("username")
		if contactUsername != "" && !strings.EqualFold(contactUsername, username) {
			continue
		}
		contactType := contact.AttrGetter().OptionalString("type")
		if contactType != "in" {
			return addContactIdentity{}, fmt.Errorf("username %s is not an active WhatsApp contact: type=%s", username, contactType)
		}
		lid := optionalAddContactJID(&user, "jid")
		if lid.IsEmpty() {
			return addContactIdentity{}, fmt.Errorf("missing LID for username %s in contact query response", username)
		}
		return addContactIdentity{LIDJID: lid.ToNonAD(), Username: username}, nil
	}
	return addContactIdentity{}, fmt.Errorf("missing user in username contact query response for %s", username)
}

func parseUsernameContactDeltaResponse(list, result *waBinary.Node, username string, expectedLID types.JID) error {
	if err := requireAddContactIntegrity(result); err != nil {
		return err
	}
	identity, err := parseUsernameContactQueryResponse(list, username)
	if err != nil {
		return err
	}
	if !expectedLID.IsEmpty() && identity.LIDJID != expectedLID {
		return fmt.Errorf("username delta LID mismatch for %s: expected %s, got %s", username, expectedLID, identity.LIDJID)
	}
	for _, user := range list.GetChildrenByTag("user") {
		usernameNode := user.GetChildByTag("username")
		if usernameNode.Tag != "username" {
			continue
		}
		state := usernameNode.AttrGetter().OptionalString("state")
		if state != "" && state != "active" {
			return fmt.Errorf("username %s is not active: state=%s", username, state)
		}
		return nil
	}
	return nil
}

func optionalAddContactJID(node *waBinary.Node, key string) types.JID {
	if node == nil || node.Attrs == nil {
		return types.EmptyJID
	}
	switch jid := node.Attrs[key].(type) {
	case types.JID:
		return jid.ToNonAD()
	case string:
		parsed, err := types.ParseJID(jid)
		if err == nil {
			return parsed.ToNonAD()
		}
	}
	return types.EmptyJID
}

func requireAddContactIntegrity(result *waBinary.Node) error {
	if result == nil {
		return &ElementMissingError{Tag: "contact", In: "add contact usync result"}
	}
	contact, ok := result.GetOptionalChildByTag("contact")
	if !ok {
		return &ElementMissingError{Tag: "contact", In: "add contact usync result"}
	}
	if integrity := contact.AttrGetter().OptionalString("integrity"); integrity != "pass" {
		return fmt.Errorf("add contact usync integrity check failed: integrity=%s", integrity)
	}
	return nil
}
