// Copyright (c) 2021 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"go.mau.fi/libsignal/ecc"
	"go.mau.fi/libsignal/groups"
	"go.mau.fi/libsignal/keys/prekey"
	"go.mau.fi/libsignal/protocol"
	"google.golang.org/protobuf/proto"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waMsgApplication"
	"go.mau.fi/whatsmeow/proto/waMsgTransport"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waKeys "go.mau.fi/whatsmeow/util/keys"
)

// Number of sent messages to cache in memory for handling retry receipts.
const recentMessagesSize = 256

type recentMessageKey struct {
	To types.JID
	ID types.MessageID
}

type RecentMessage struct {
	wa *waE2E.Message
	fb *waMsgApplication.MessageApplication
}

func (rm RecentMessage) IsEmpty() bool {
	return rm.wa == nil && rm.fb == nil
}

func (cli *Client) addRecentMessage(ctx context.Context, to types.JID, id types.MessageID, wa *waE2E.Message, fb *waMsgApplication.MessageApplication) error {
	if cli.UseRetryMessageStore {
		var buf []byte
		var format string
		var err error
		if wa != nil {
			buf, err = proto.Marshal(wa)
			format = "wa"
		} else if fb != nil {
			buf, err = proto.Marshal(fb)
			format = "fb"
		}
		if err != nil {
			return fmt.Errorf("failed to marshal message for retry store: %w", err)
		}
		if buf != nil {
			err = cli.Store.EventBuffer.AddOutgoingEvent(ctx, to, id, format, buf)
			if err != nil {
				return fmt.Errorf("failed to add message to retry store: %w", err)
			}
			if time.Since(cli.lastRetryStoreClear) > 12*time.Hour {
				err = cli.Store.EventBuffer.DeleteOldOutgoingEvents(ctx)
				if err != nil {
					return fmt.Errorf("failed to clear old messages from retry store: %w", err)
				}
			}
		}
	}
	cli.recentMessagesLock.Lock()
	key := recentMessageKey{to, id}
	if cli.recentMessagesList[cli.recentMessagesPtr].ID != "" {
		delete(cli.recentMessagesMap, cli.recentMessagesList[cli.recentMessagesPtr])
	}
	cli.recentMessagesMap[key] = RecentMessage{wa: wa, fb: fb}
	cli.recentMessagesList[cli.recentMessagesPtr] = key
	cli.recentMessagesPtr++
	if cli.recentMessagesPtr >= len(cli.recentMessagesList) {
		cli.recentMessagesPtr = 0
	}
	cli.recentMessagesLock.Unlock()
	return nil
}

func (cli *Client) getRecentMessage(to types.JID, id types.MessageID) RecentMessage {
	cli.recentMessagesLock.RLock()
	defer cli.recentMessagesLock.RUnlock()
	return cli.recentMessagesMap[recentMessageKey{to, id}]
}

func (cli *Client) getMessageForRetry(ctx context.Context, receipt *events.Receipt, messageID types.MessageID) (*RecentMessage, error) {
	msg := cli.getRecentMessage(receipt.Chat, messageID)
	if !msg.IsEmpty() {
		cli.Log.Debugf("Found message in local cache to accept retry receipt for %s/%s from %s", receipt.Chat, messageID, receipt.Sender)
		return &msg, nil
	}
	var altChat types.JID
	var err error
	switch receipt.Chat.Server {
	case types.DefaultUserServer:
		altChat, err = cli.Store.LIDs.GetLIDForPN(ctx, receipt.Chat)
	case types.HiddenUserServer:
		altChat, err = cli.Store.LIDs.GetPNForLID(ctx, receipt.Chat)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get alternate JID for %s: %w", receipt.Chat, err)
	} else if !altChat.IsEmpty() {
		msg = cli.getRecentMessage(altChat, messageID)
		if !msg.IsEmpty() {
			cli.Log.Debugf("Found message in local cache with alternate chat JID %s to accept retry receipt for %s/%s from %s", altChat, receipt.Chat, messageID, receipt.Sender)
			return &msg, nil
		}
	}
	if cli.UseRetryMessageStore {
		format, buf, err := cli.Store.EventBuffer.GetOutgoingEvent(ctx, receipt.Chat, altChat, messageID)
		if err != nil {
			return nil, fmt.Errorf("failed to get message from retry store: %w", err)
		}
		return parseRecentMessage(format, buf)
	}
	waMsg := cli.GetMessageForRetry(receipt.Sender, receipt.Chat, messageID)
	if waMsg != nil {
		cli.Log.Debugf("Found message in GetMessageForRetry to accept retry receipt for %s/%s from %s", receipt.Chat, messageID, receipt.Sender)
		return &RecentMessage{wa: waMsg}, nil
	}
	return nil, nil
}

func parseRecentMessage(format string, buf []byte) (*RecentMessage, error) {
	var rm RecentMessage
	var err error
	switch format {
	case "wa":
		rm.wa = &waE2E.Message{}
		err = proto.Unmarshal(buf, rm.wa)
	case "fb":
		rm.fb = &waMsgApplication.MessageApplication{}
		err = proto.Unmarshal(buf, rm.fb)
	default:
		err = fmt.Errorf("unknown format in retry store: %s", format)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload in retry store: %w", err)
	}
	return &rm, nil
}

const recreateSessionTimeout = 1 * time.Hour

func (cli *Client) shouldRecreateSession(ctx context.Context, retryCount int, jid types.JID) (reason string, recreate bool) {
	cli.sessionRecreateHistoryLock.Lock()
	defer cli.sessionRecreateHistoryLock.Unlock()
	if contains, err := cli.Store.ContainsSession(ctx, jid.SignalAddress()); err != nil {
		return "", false
	} else if !contains {
		cli.sessionRecreateHistory[jid] = time.Now()
		return "we don't have a Signal session with them", true
	} else if retryCount < 2 {
		return "", false
	}
	prevTime, ok := cli.sessionRecreateHistory[jid]
	if !ok || prevTime.Add(recreateSessionTimeout).Before(time.Now()) {
		cli.sessionRecreateHistory[jid] = time.Now()
		return "retry count > 1 and over an hour since last recreation", true
	}
	return "", false
}

type incomingRetryKey struct {
	chat      types.JID
	jid       types.JID
	messageID types.MessageID
}

type messageRetryKey struct {
	chat      types.JID
	sender    types.JID
	messageID types.MessageID
}

type phoneRerequestState struct {
	retryCount int
	cancel     context.CancelFunc
	done       <-chan struct{}
}

const retryLockCount = 64

func messageRetryKeyFromInfo(info *types.MessageInfo) messageRetryKey {
	return messageRetryKey{chat: info.Chat, sender: info.Sender, messageID: info.ID}
}

func retryLockIndex(chat, sender types.JID, messageID types.MessageID) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	hash := uint64(offset)
	for _, value := range [...]string{chat.String(), sender.String(), string(messageID)} {
		for index := 0; index < len(value); index++ {
			hash ^= uint64(value[index])
			hash *= prime
		}
		hash ^= 0
		hash *= prime
	}
	return hash % retryLockCount
}

func (cli *Client) retryLock(chat, sender types.JID, messageID types.MessageID) *sync.Mutex {
	return &cli.retryLocks[retryLockIndex(chat, sender, messageID)]
}

func (cli *Client) tryHandleRetryReceipt(ctx context.Context, receipt *events.Receipt, node *waBinary.Node) {
	defer func() {
		err := recover()
		if err != nil {
			cli.Log.Errorf("Retry receipt handler panicked: %v\n%s", err, debug.Stack())
		}
	}()
	if cli.retrySema != nil {
		err := cli.retrySema.Acquire(ctx, 1)
		if err != nil {
			return
		}
		defer cli.retrySema.Release(1)
	}
	err := cli.handleRetryReceipt(ctx, receipt, node)
	if err != nil {
		var messageID types.MessageID
		if len(receipt.MessageIDs) > 0 {
			messageID = receipt.MessageIDs[0]
		}
		cli.Log.Errorf("Failed to handle retry receipt for %s/%s from %s: %v", receipt.Chat, messageID, receipt.Sender, err)
	}
}

const (
	maxIncomingRetryCount = 4
	maxRetryMessageAge    = 60 * 24 * time.Hour
)

func retryRegistrationID(node *waBinary.Node) (uint32, error) {
	registrationBytes, ok := node.GetChildByTag("registration").Content.([]byte)
	if !ok || len(registrationBytes) != 4 {
		return 0, fmt.Errorf("invalid registration ID in retry receipt")
	}
	return binary.BigEndian.Uint32(registrationBytes), nil
}

func cloneRecentMessage(msg *RecentMessage) *RecentMessage {
	cloned := &RecentMessage{}
	if msg.wa != nil {
		cloned.wa = proto.Clone(msg.wa).(*waE2E.Message)
	}
	if msg.fb != nil {
		cloned.fb = proto.Clone(msg.fb).(*waMsgApplication.MessageApplication)
	}
	return cloned
}

func (cli *Client) reconcileRetryRegistration(ctx context.Context, jid types.JID, registrationID uint32) (hasSession, changed bool, err error) {
	signalAddress := jid.SignalAddress()
	hasSession, err = cli.Store.ContainsSession(ctx, signalAddress)
	if err != nil || !hasSession {
		return hasSession, false, err
	}
	sessionRecord, err := cli.Store.LoadSession(ctx, signalAddress)
	if err != nil {
		return false, false, err
	}
	if sessionRecord.SessionState().RemoteRegistrationID() == registrationID {
		return true, false, nil
	}
	if err = cli.Store.Sessions.DeleteSession(ctx, signalAddress.String()); err != nil {
		return false, false, err
	}
	return false, true, nil
}

func sendRetryFrameWithReconnect(
	ctx context.Context,
	node waBinary.Node,
	send func([]byte) error,
	waitForConnection func(time.Duration) bool,
) error {
	payload, err := waBinary.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal retry frame: %w", err)
	}
	firstErr := send(payload)
	if firstErr == nil {
		return nil
	}
	if ctx.Err() != nil {
		return fmt.Errorf("failed to send retry frame: %w", firstErr)
	}
	if !waitForConnection(5 * time.Second) {
		return fmt.Errorf("failed to send retry frame and connection was not restored: %w", firstErr)
	}
	if err = send(payload); err != nil {
		return fmt.Errorf("failed to resend retry frame after initial error %v: %w", firstErr, err)
	}
	return nil
}

func (cli *Client) sendRetryFrame(ctx context.Context, node waBinary.Node) error {
	send := func(payload []byte) error {
		cli.socketLock.RLock()
		sock := cli.socket
		cli.socketLock.RUnlock()
		if sock == nil {
			return ErrNotConnected
		}
		return sock.SendFrame(ctx, payload)
	}
	return sendRetryFrameWithReconnect(ctx, node, send, cli.WaitForConnection)
}

// handleRetryReceipt handles an incoming retry receipt for an outgoing message.
func (cli *Client) handleRetryReceipt(ctx context.Context, receipt *events.Receipt, node *waBinary.Node) error {
	retryChild, ok := node.GetOptionalChildByTag("retry")
	if !ok {
		return &ElementMissingError{Tag: "retry", In: "retry receipt"}
	}
	ag := retryChild.AttrGetter()
	messageID := types.MessageID(ag.String("id"))
	timestamp := ag.UnixTime("t")
	retryCount := ag.Int("count")
	if !ag.OK() {
		return ag.Error()
	}
	if retryCount < 1 || retryCount > maxIncomingRetryCount {
		cli.Log.Warnf("Ignoring retry request for %s/%s from %s with invalid count %d", receipt.Chat, messageID, receipt.Sender, retryCount)
		return nil
	}
	if len(receipt.MessageIDs) == 0 || receipt.MessageIDs[0] != messageID {
		return fmt.Errorf("retry message ID %s doesn't match receipt message ID", messageID)
	}
	if timestamp.Before(time.Now().Add(-maxRetryMessageAge)) {
		cli.Log.Warnf("Ignoring retry request for expired message %s/%s from %s with count %d", receipt.Chat, messageID, receipt.Sender, retryCount)
		return nil
	}
	registrationID, err := retryRegistrationID(node)
	if err != nil {
		return err
	}

	retryKey := incomingRetryKey{chat: receipt.Chat, jid: receipt.Sender, messageID: messageID}
	retryLock := cli.retryLock(retryKey.chat, retryKey.jid, retryKey.messageID)
	retryLock.Lock()
	defer retryLock.Unlock()

	cli.incomingRetryRequestCounterLock.Lock()
	lastRetryCount := cli.incomingRetryRequestCounter[retryKey]
	cli.incomingRetryRequestCounterLock.Unlock()
	if retryCount <= lastRetryCount {
		cli.Log.Debugf("Ignoring duplicate retry request for %s/%s from %s with count %d", receipt.Chat, messageID, receipt.Sender, retryCount)
		return nil
	}

	msg, err := cli.getMessageForRetry(ctx, receipt, messageID)
	if err != nil {
		return err
	} else if msg == nil {
		return fmt.Errorf("couldn't find message %s", messageID)
	}
	msg = cloneRecentMessage(msg)

	var fbConsumerMsg *waConsumerApplication.ConsumerApplication
	if msg.fb != nil {
		subProto, ok := msg.fb.GetPayload().GetSubProtocol().GetSubProtocol().(*waMsgApplication.MessageApplication_SubProtocolPayload_ConsumerMessage)
		if ok {
			fbConsumerMsg, err = subProto.Decode()
			if err != nil {
				return fmt.Errorf("failed to decode consumer message for retry: %w", err)
			}
		}
	}

	// TODO pre-retry callback for fb
	if cli.PreRetryCallback != nil && !cli.PreRetryCallback(receipt, messageID, retryCount, msg.wa) {
		cli.incomingRetryRequestCounterLock.Lock()
		if cli.incomingRetryRequestCounter[retryKey] < retryCount {
			cli.incomingRetryRequestCounter[retryKey] = retryCount
		}
		cli.incomingRetryRequestCounterLock.Unlock()
		cli.Log.Debugf("Cancelled retry request for %s/%s from %s with count %d in PreRetryCallback", receipt.Chat, messageID, receipt.Sender, retryCount)
		return nil
	}

	cli.messageSendLock.Lock()
	defer cli.messageSendLock.Unlock()

	encryptionIdentity := receipt.Sender
	if msg.wa != nil && receipt.Sender.Server == types.DefaultUserServer {
		lidForPN, lidErr := cli.Store.LIDs.GetLIDForPN(ctx, receipt.Sender)
		if lidErr != nil {
			cli.Log.Warnf("Failed to resolve retry requester %s to LID for %s/%s: %v", receipt.Sender, receipt.Chat, messageID, lidErr)
		} else if !lidForPN.IsEmpty() {
			cli.migrateSessionStore(ctx, receipt.Sender, lidForPN)
			encryptionIdentity = lidForPN
		}
	}

	hasSession, registrationChanged, err := cli.reconcileRetryRegistration(ctx, encryptionIdentity, registrationID)
	if err != nil {
		return fmt.Errorf("failed to reconcile session registration with retry requester %s: %w", encryptionIdentity, err)
	}

	_, hasKeys := node.GetOptionalChildByTag("keys")
	var bundle *prekey.Bundle
	if retryCount > 1 && hasKeys {
		bundle, err = nodeToPreKeyBundle(uint32(receipt.Sender.Device), *node)
		if err != nil {
			return fmt.Errorf("failed to read prekey bundle in retry receipt: %w", err)
		}
	} else {
		reason := ""
		recreate := !hasSession
		if recreate {
			if registrationChanged {
				reason = "the registration ID changed"
			} else {
				reason = "we don't have a Signal session with them"
			}
		} else {
			reason, recreate = cli.shouldRecreateSession(ctx, retryCount, encryptionIdentity)
		}
		if recreate {
			cli.Log.Debugf("Fetching prekeys for retry of %s/%s to %s at count %d because %s", receipt.Chat, messageID, encryptionIdentity, retryCount, reason)
			var fetched map[types.JID]preKeyResp
			fetched, err = cli.fetchPreKeys(ctx, []types.JID{encryptionIdentity})
			if err != nil {
				return err
			}
			bundle, err = fetched[encryptionIdentity].bundle, fetched[encryptionIdentity].err
			if err != nil {
				return fmt.Errorf("failed to fetch prekeys: %w", err)
			} else if bundle == nil {
				return fmt.Errorf("didn't get prekey bundle for %s (response size: %d)", encryptionIdentity, len(fetched))
			}
		}
	}

	var fbSKDM *waMsgTransport.MessageTransport_Protocol_Ancillary_SenderKeyDistributionMessage
	var fbDSM *waMsgTransport.MessageTransport_Protocol_Integral_DeviceSentMessage
	if receipt.IsGroup {
		builder := groups.NewGroupSessionBuilder(cli.Store, pbSerializer)
		senderKeyName := protocol.NewSenderKeyName(receipt.Chat.String(), cli.getOwnLID().SignalAddress())
		signalSKDMessage, err := builder.Create(ctx, senderKeyName)
		if err != nil {
			cli.Log.Warnf("Failed to create sender key distribution message to include in retry of %s in %s to %s: %v", messageID, receipt.Chat, receipt.Sender, err)
		} else if msg.wa != nil {
			msg.wa.SenderKeyDistributionMessage = &waE2E.SenderKeyDistributionMessage{
				GroupID:                             proto.String(receipt.Chat.String()),
				AxolotlSenderKeyDistributionMessage: signalSKDMessage.Serialize(),
			}
		} else {
			fbSKDM = &waMsgTransport.MessageTransport_Protocol_Ancillary_SenderKeyDistributionMessage{
				GroupID:                             proto.String(receipt.Chat.String()),
				AxolotlSenderKeyDistributionMessage: signalSKDMessage.Serialize(),
			}
		}
	} else if receipt.IsFromMe {
		if msg.wa != nil {
			msg.wa = &waE2E.Message{
				DeviceSentMessage: &waE2E.DeviceSentMessage{
					DestinationJID: proto.String(receipt.Chat.String()),
					Message:        msg.wa,
				},
			}
		} else {
			fbDSM = &waMsgTransport.MessageTransport_Protocol_Integral_DeviceSentMessage{
				DestinationJID: proto.String(receipt.Chat.String()),
			}
		}
	}

	var plaintext, frankingTag []byte
	if msg.wa != nil {
		plaintext, err = proto.Marshal(msg.wa)
		if err != nil {
			return fmt.Errorf("failed to marshal message: %w", err)
		}
	} else {
		plaintext, err = proto.Marshal(msg.fb)
		if err != nil {
			return fmt.Errorf("failed to marshal consumer message: %w", err)
		}
		frankingHash := hmac.New(sha256.New, msg.fb.GetMetadata().GetFrankingKey())
		frankingHash.Write(plaintext)
		frankingTag = frankingHash.Sum(nil)
	}
	encAttrs := waBinary.Attrs{}
	var msgAttrs messageAttrs
	if msg.wa != nil {
		msgAttrs.MediaType = getMediaTypeFromMessage(msg.wa)
		msgAttrs.Type = getTypeFromMessage(msg.wa)
	} else if fbConsumerMsg != nil {
		msgAttrs = getAttrsFromFBMessage(fbConsumerMsg)
	} else {
		msgAttrs.Type = "text"
	}
	if msgAttrs.MediaType != "" {
		encAttrs["mediatype"] = msgAttrs.MediaType
	}
	var encrypted *waBinary.Node
	var includeDeviceIdentity bool
	if msg.wa != nil {
		encrypted, includeDeviceIdentity, err = cli.encryptMessageForDevice(ctx, plaintext, encryptionIdentity, bundle, encAttrs, nil)
	} else {
		encrypted, err = cli.encryptMessageForDeviceV3(ctx, &waMsgTransport.MessageTransport_Payload{
			ApplicationPayload: &waCommon.SubProtocol{
				Payload: plaintext,
				Version: proto.Int32(FBMessageApplicationVersion),
			},
			FutureProof: waCommon.FutureProofBehavior_PLACEHOLDER.Enum(),
		}, fbSKDM, fbDSM, receipt.Sender, bundle, encAttrs)
	}
	if err != nil {
		return fmt.Errorf("failed to encrypt message for retry: %w", err)
	}
	encrypted.Attrs["count"] = retryCount

	attrs := waBinary.Attrs{
		"to":   node.Attrs["from"],
		"type": msgAttrs.Type,
		"id":   messageID,
		"t":    timestamp.Unix(),
	}
	if !receipt.IsGroup {
		attrs["device_fanout"] = false
	}
	if participant, ok := node.Attrs["participant"]; ok {
		attrs["participant"] = participant
	}
	if recipient, ok := node.Attrs["recipient"]; ok {
		attrs["recipient"] = recipient
	}
	if edit, ok := node.Attrs["edit"]; ok {
		attrs["edit"] = edit
	}
	var content []waBinary.Node
	if msg.wa != nil {
		content = cli.getMessageContent(
			*encrypted, msg.wa, attrs, includeDeviceIdentity, nodeExtraParams{},
		)
	} else {
		content = []waBinary.Node{
			*encrypted,
			{Tag: "franking", Content: []waBinary.Node{{Tag: "franking_tag", Content: frankingTag}}},
		}
	}
	err = cli.sendRetryFrame(ctx, waBinary.Node{
		Tag:     "message",
		Attrs:   attrs,
		Content: content,
	})
	if err != nil {
		return fmt.Errorf("failed to send retry message: %w", err)
	}
	cli.incomingRetryRequestCounterLock.Lock()
	if cli.incomingRetryRequestCounter[retryKey] < retryCount {
		cli.incomingRetryRequestCounter[retryKey] = retryCount
	}
	cli.incomingRetryRequestCounterLock.Unlock()
	cli.Log.Debugf("Sent retry for %s/%s to %s with count %d (registration changed: %t)", receipt.Chat, messageID, receipt.Sender, retryCount, registrationChanged)
	return nil
}

func (cli *Client) cancelDelayedRequestFromPhoneForMessage(info *types.MessageInfo) {
	if !cli.AutomaticMessageRerequestFromPhone || cli.MessengerConfig != nil {
		return
	}
	key := messageRetryKeyFromInfo(info)
	cli.pendingPhoneRerequestsLock.Lock()
	state, found := cli.pendingPhoneRerequests[key]
	delete(cli.pendingPhoneRerequests, key)
	cli.pendingPhoneRerequestsLock.Unlock()
	if found && state.cancel != nil {
		state.cancel()
	}
}

func (cli *Client) cancelDelayedRequestFromPhone(msgID types.MessageID) {
	if !cli.AutomaticMessageRerequestFromPhone || cli.MessengerConfig != nil {
		return
	}
	var cancellations []context.CancelFunc
	cli.pendingPhoneRerequestsLock.Lock()
	for key, state := range cli.pendingPhoneRerequests {
		if key.messageID == msgID {
			delete(cli.pendingPhoneRerequests, key)
			if state.cancel != nil {
				cancellations = append(cancellations, state.cancel)
			}
		}
	}
	cli.pendingPhoneRerequestsLock.Unlock()
	for _, cancel := range cancellations {
		cancel()
	}
}

// RequestFromPhoneDelay specifies how long to wait for the sender to resend the message before requesting from your phone.
// This is only used if Client.AutomaticMessageRerequestFromPhone is true.
var RequestFromPhoneDelay = 5 * time.Second

func (cli *Client) delayedRequestMessageFromPhone(info *types.MessageInfo) {
	ctx, cancel, reserved := cli.preparePhoneRerequest(info, 0)
	if reserved {
		cli.waitForPhoneRerequest(ctx, cancel, info, 0)
	}
}

func (cli *Client) reservePhoneRerequest(info *types.MessageInfo, retryCount int, cancel context.CancelFunc, done <-chan struct{}) (context.CancelFunc, bool) {
	key := messageRetryKeyFromInfo(info)
	cli.pendingPhoneRerequestsLock.Lock()
	defer cli.pendingPhoneRerequestsLock.Unlock()
	current, found := cli.pendingPhoneRerequests[key]
	if found && retryCount <= current.retryCount {
		return nil, false
	}
	cli.pendingPhoneRerequests[key] = phoneRerequestState{retryCount: retryCount, cancel: cancel, done: done}
	return current.cancel, true
}

func (cli *Client) finishPhoneRerequest(info *types.MessageInfo, retryCount int, done <-chan struct{}) {
	key := messageRetryKeyFromInfo(info)
	cli.pendingPhoneRerequestsLock.Lock()
	defer cli.pendingPhoneRerequestsLock.Unlock()
	state, found := cli.pendingPhoneRerequests[key]
	if !found || state.retryCount != retryCount || state.done != done {
		return
	}
	if retryCount == 0 {
		delete(cli.pendingPhoneRerequests, key)
	} else {
		state.cancel = nil
		cli.pendingPhoneRerequests[key] = state
	}
}

func (cli *Client) preparePhoneRerequest(info *types.MessageInfo, retryCount int) (context.Context, context.CancelFunc, bool) {
	if !cli.AutomaticMessageRerequestFromPhone || cli.MessengerConfig != nil {
		return nil, nil, false
	}
	ctx, cancel := context.WithCancel(cli.BackgroundEventCtx)
	previousCancel, reserved := cli.reservePhoneRerequest(info, retryCount, cancel, ctx.Done())
	if !reserved {
		cancel()
		return nil, nil, false
	}
	if previousCancel != nil {
		previousCancel()
	}
	return ctx, cancel, true
}

func (cli *Client) waitForPhoneRerequest(ctx context.Context, cancel context.CancelFunc, info *types.MessageInfo, retryCount int) {
	defer cancel()
	defer cli.finishPhoneRerequest(info, retryCount, ctx.Done())
	timer := time.NewTimer(RequestFromPhoneDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		cli.Log.Debugf("Cancelled delayed request for message %s from phone", info.ID)
		return
	}
	cli.immediateRequestMessageFromPhone(ctx, info)
}

func (cli *Client) schedulePhoneRerequest(ctx context.Context, info *types.MessageInfo, retryCount int) {
	if !cli.AutomaticMessageRerequestFromPhone || cli.MessengerConfig != nil {
		return
	}
	if cli.SynchronousAck {
		previousCancel, reserved := cli.reservePhoneRerequest(info, retryCount, nil, nil)
		if !reserved {
			return
		}
		if previousCancel != nil {
			previousCancel()
		}
		cli.immediateRequestMessageFromPhone(ctx, info)
	} else {
		phoneCtx, cancel, reserved := cli.preparePhoneRerequest(info, retryCount)
		if reserved {
			go cli.waitForPhoneRerequest(phoneCtx, cancel, info, retryCount)
		}
	}
}

func (cli *Client) immediateRequestMessageFromPhone(ctx context.Context, info *types.MessageInfo) {
	_, err := cli.SendPeerMessage(ctx, cli.BuildUnavailableMessageRequest(info.Chat, info.Sender, info.ID))
	if err != nil {
		cli.Log.Warnf("Failed to send request for unavailable message %s to phone: %v", info.ID, err)
	} else {
		cli.Log.Debugf("Requested message %s from phone", info.ID)
	}
	return
}

func (cli *Client) clearDelayedMessageRequests() {
	var cancellations []context.CancelFunc
	cli.pendingPhoneRerequestsLock.Lock()
	for key, state := range cli.pendingPhoneRerequests {
		delete(cli.pendingPhoneRerequests, key)
		if state.cancel != nil {
			cancellations = append(cancellations, state.cancel)
		}
	}
	cli.pendingPhoneRerequestsLock.Unlock()
	for _, cancel := range cancellations {
		cancel()
	}
}

type retryPreKeyGenerator interface {
	GenOneRetryPreKey(context.Context) (*waKeys.PreKey, error)
}

func (cli *Client) generateRetryPreKey(ctx context.Context) (*waKeys.PreKey, error) {
	if generator, ok := cli.Store.PreKeys.(retryPreKeyGenerator); ok {
		return generator.GenOneRetryPreKey(ctx)
	}
	return cli.Store.PreKeys.GenOnePreKey(ctx)
}

// sendRetryReceipt sends a retry receipt for an incoming message.
func (cli *Client) sendRetryReceipt(ctx context.Context, node *waBinary.Node, info *types.MessageInfo, forceIncludeIdentity bool) {
	failedEncCount := 0
	for _, child := range node.GetChildrenByTag("enc") {
		if count := child.AttrGetter().OptionalInt("count"); count > failedEncCount {
			failedEncCount = count
		}
	}
	if err := cli.sendRetryReceiptWithCount(ctx, node, info, forceIncludeIdentity, failedEncCount); err != nil {
		cli.Log.Errorf("Failed to send retry receipt for %s/%s from %s: %v", info.Chat, info.ID, info.Sender, err)
	}
}

func retryReceiptCount(failedEncCount int) (int, error) {
	if failedEncCount < 0 || failedEncCount > maxIncomingRetryCount {
		return 0, fmt.Errorf("invalid failed encryption count %d", failedEncCount)
	}
	return failedEncCount + 1, nil
}

func (cli *Client) sendRetryReceiptWithCount(ctx context.Context, node *waBinary.Node, info *types.MessageInfo, forceIncludeIdentity bool, failedEncCount int) error {
	retryCount, err := retryReceiptCount(failedEncCount)
	if err != nil {
		return err
	}
	key := messageRetryKeyFromInfo(info)
	retryLock := cli.retryLock(key.chat, key.sender, key.messageID)
	retryLock.Lock()
	defer retryLock.Unlock()

	cli.messageRetriesLock.Lock()
	previousRetryCount := cli.messageRetries[key]
	if retryCount <= previousRetryCount {
		cli.messageRetriesLock.Unlock()
		return nil
	}
	cli.messageRetries[key] = retryCount
	cli.messageRetriesLock.Unlock()
	retryCountCommitted := false
	defer func() {
		if !retryCountCommitted {
			cli.messageRetriesLock.Lock()
			if previousRetryCount == 0 {
				delete(cli.messageRetries, key)
			} else {
				cli.messageRetries[key] = previousRetryCount
			}
			cli.messageRetriesLock.Unlock()
		}
	}()

	cli.schedulePhoneRerequest(ctx, info, retryCount)

	var registrationIDBytes [4]byte
	binary.BigEndian.PutUint32(registrationIDBytes[:], cli.Store.RegistrationID)
	attrs := buildBaseReceipt(info.ID, node)
	attrs["type"] = "retry"
	if info.Type == "peer_msg" && info.IsFromMe {
		attrs["category"] = "peer"
	}
	payload := waBinary.Node{
		Tag:   "receipt",
		Attrs: attrs,
		Content: []waBinary.Node{
			{Tag: "retry", Attrs: waBinary.Attrs{
				"count": retryCount,
				"id":    info.ID,
				"t":     node.Attrs["t"],
				"v":     1,
			}},
			{Tag: "registration", Content: registrationIDBytes[:]},
		},
	}
	if retryCount > 1 || forceIncludeIdentity {
		preKey, err := cli.generateRetryPreKey(ctx)
		if err != nil {
			return fmt.Errorf("failed to get prekey for retry receipt: %w", err)
		}
		deviceIdentity, err := proto.Marshal(cli.Store.Account)
		if err != nil {
			return fmt.Errorf("failed to marshal account info for retry receipt: %w", err)
		}
		payload.Content = append(payload.GetChildren(), waBinary.Node{
			Tag: "keys",
			Content: []waBinary.Node{
				{Tag: "type", Content: []byte{ecc.DjbType}},
				{Tag: "identity", Content: cli.Store.IdentityKey.Pub[:]},
				preKeyToNode(preKey),
				preKeyToNode(cli.Store.SignedPreKey),
				{Tag: "device-identity", Content: deviceIdentity},
			},
		})
	}
	if err := cli.sendRetryFrame(ctx, payload); err != nil {
		return fmt.Errorf("failed to send retry receipt: %w", err)
	}
	retryCountCommitted = true
	cli.Log.Debugf("Sent retry receipt for %s/%s from %s with count %d", info.Chat, info.ID, info.Sender, retryCount)
	return nil
}
