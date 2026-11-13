// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"sync"
	"time"

	"go.mau.fi/libsignal/ecc"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
)

// preKeyMutex keeps the health-check deadline effective while another upload
// is waiting for the server. Its zero value is ready for use.
type preKeyMutex struct {
	once  sync.Once
	token chan struct{}
}

func (m *preKeyMutex) Lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.once.Do(func() { m.token = make(chan struct{}, 1) })
	select {
	case m.token <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *preKeyMutex) Unlock() {
	<-m.token
}

const (
	preKeyHealthInterval      = time.Minute
	preKeyHealthRetryInterval = 10 * time.Second
	preKeyHealthTimeout       = 15 * time.Second
)

// ensureServerPreKeys is also called on the first retry of an incoming message.
// That first failure gets a fresh check even immediately after a login check.
func (cli *Client) ensureServerPreKeys(ctx context.Context) error {
	return cli.ensureServerPreKeysFor(ctx, true)
}

func (cli *Client) ensureServerPreKeysFor(ctx context.Context, onFailure bool) error {
	return cli.ensureServerPreKeysWith(ctx, onFailure, cli.sendIQ)
}

func (cli *Client) ensureServerPreKeysWith(ctx context.Context, onFailure bool, query preKeyQuery) error {
	checkCtx, cancel := context.WithTimeout(ctx, preKeyHealthTimeout)
	defer cancel()
	if err := cli.preKeyHealthLock.Lock(checkCtx); err != nil {
		return err
	}
	defer cli.preKeyHealthLock.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	interval := preKeyHealthInterval
	if cli.preKeyCheckErr != nil {
		interval = preKeyHealthRetryInterval
	}
	pending := cli.preKeyRepairPending || cli.preKeyReplenishPending
	if time.Since(cli.lastPreKeyCheck) < interval && (cli.preKeyCheckOnFailure || !onFailure) && !(pending && !onFailure) {
		return cli.preKeyCheckErr
	}
	err := cli.checkAndSyncPreKeys(checkCtx, query)
	cli.lastPreKeyCheck = time.Now()
	cli.preKeyCheckErr = err
	cli.preKeyCheckOnFailure = onFailure
	if err == nil {
		cli.preKeyRepairPending = false
		cli.preKeyReplenishPending = false
	} else if (cli.preKeyRepairPending || cli.preKeyReplenishPending) && (!cli.preKeyRecoveryActive || !onFailure) {
		cli.preKeyRecoveryActive = true
		cli.preKeyRecoveryID++
		go cli.retryPreKeyRepair(cli.preKeyRecoveryID)
	}
	return err
}

// A reset may have succeeded even when its IQ response or replacement upload
// failed. Do not rely on another incoming message to refill an empty server pool.
func (cli *Client) retryPreKeyRepair(generation uint64) {
	defer cli.finishPreKeyRecovery(generation)
	ctx := cli.BackgroundEventCtx
	if ctx == nil {
		ctx = context.Background()
	}
	for attempt := 1; attempt <= 3; attempt++ {
		timer := time.NewTimer(preKeyHealthRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		_ = cli.preKeyHealthLock.Lock(context.Background())
		superseded := cli.preKeyRecoveryID != generation
		cli.preKeyHealthLock.Unlock()
		if superseded {
			return
		}
		if !cli.IsConnected() || !cli.IsLoggedIn() {
			return // The post-connect check will resume a pending repair.
		}
		if err := cli.ensureServerPreKeys(ctx); err != nil {
			cli.Log.Warnf("Prekey repair recovery attempt %d failed: %v", attempt, err)
			_ = cli.preKeyHealthLock.Lock(context.Background())
			pending := cli.preKeyRepairPending || cli.preKeyReplenishPending
			cli.preKeyHealthLock.Unlock()
			if !pending {
				return
			}
		} else {
			return
		}
	}
}

func (cli *Client) finishPreKeyRecovery(generation uint64) {
	_ = cli.preKeyHealthLock.Lock(context.Background())
	defer cli.preKeyHealthLock.Unlock()
	if cli.preKeyRecoveryID == generation {
		cli.preKeyRecoveryActive = false
	}
}

func validateLocalPreKeys(device *store.Device) error {
	if device == nil || device.PreKeys == nil {
		return fmt.Errorf("local prekey store is missing")
	}
	if device.IdentityKey == nil || device.IdentityKey.Priv == nil || device.IdentityKey.Pub == nil {
		return fmt.Errorf("local identity key is incomplete")
	}
	if device.SignedPreKey == nil || device.SignedPreKey.Priv == nil || device.SignedPreKey.Pub == nil || device.SignedPreKey.Signature == nil {
		return fmt.Errorf("local signed prekey is incomplete")
	}
	if device.SignedPreKey.KeyID < store.PreKeyIDMin || device.SignedPreKey.KeyID > store.PreKeyIDMax {
		return fmt.Errorf("local signed prekey ID is outside the valid range")
	}
	if *keys.NewKeyPairFromPrivateKey(*device.IdentityKey.Priv).Pub != *device.IdentityKey.Pub ||
		*keys.NewKeyPairFromPrivateKey(*device.SignedPreKey.Priv).Pub != *device.SignedPreKey.Pub {
		return fmt.Errorf("local identity or signed prekey public/private key mismatch")
	}
	signedPayload := append([]byte{ecc.DjbType}, device.SignedPreKey.Pub[:]...)
	if !ecc.VerifySignature(ecc.NewDjbECPublicKey(*device.IdentityKey.Pub), signedPayload, *device.SignedPreKey.Signature) {
		return fmt.Errorf("local signed prekey signature is invalid")
	}
	return nil
}

type serverPreKeyDigest struct {
	RegistrationID        uint32
	KeyType               byte
	IdentityKey           [32]byte
	SignedPreKeyID        uint32
	SignedPreKeyValue     [32]byte
	SignedPreKeySignature [64]byte
	PreKeyIDs             []uint32
}

func getServerPreKeyDigest(ctx context.Context, query preKeyQuery) (*serverPreKeyDigest, error) {
	resp, err := query(ctx, infoQuery{
		Namespace: "encrypt", Type: iqGet, To: types.ServerJID, NoRetry: true,
		Content: []waBinary.Node{{Tag: "digest"}},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query server prekey digest: %w", err)
	}
	return parseServerPreKeyDigest(resp)
}

func preKeyDigestBytes(node waBinary.Node, tag string, length int) ([]byte, error) {
	children := node.GetChildrenByTag(tag)
	if len(children) != 1 {
		return nil, fmt.Errorf("prekey digest expected one %s element, got %d", tag, len(children))
	}
	data, ok := children[0].Content.([]byte)
	if !ok || len(data) != length {
		return nil, fmt.Errorf("prekey digest %s must contain %d bytes", tag, length)
	}
	return data, nil
}

func parseServerPreKeyDigest(resp *waBinary.Node) (*serverPreKeyDigest, error) {
	if resp == nil || len(resp.GetChildrenByTag("digest")) != 1 {
		return nil, fmt.Errorf("prekey response must contain one digest")
	}
	node := resp.GetChildByTag("digest")
	registration, err := preKeyDigestBytes(node, "registration", 4)
	if err != nil {
		return nil, err
	}
	keyType, err := preKeyDigestBytes(node, "type", 1)
	if err != nil {
		return nil, err
	}
	identity, err := preKeyDigestBytes(node, "identity", 32)
	if err != nil {
		return nil, err
	}
	if len(node.GetChildrenByTag("skey")) != 1 || len(node.GetChildrenByTag("list")) != 1 {
		return nil, fmt.Errorf("prekey digest must contain one skey and one list")
	}
	skey := node.GetChildByTag("skey")
	signedID, err := preKeyDigestBytes(skey, "id", 3)
	if err != nil {
		return nil, err
	}
	signedValue, err := preKeyDigestBytes(skey, "value", 32)
	if err != nil {
		return nil, err
	}
	signedSignature, err := preKeyDigestBytes(skey, "signature", 64)
	if err != nil {
		return nil, err
	}
	digest := &serverPreKeyDigest{
		RegistrationID: binary.BigEndian.Uint32(registration), KeyType: keyType[0],
		SignedPreKeyID: uint32(signedID[0])<<16 | uint32(signedID[1])<<8 | uint32(signedID[2]),
	}
	if digest.SignedPreKeyID < store.PreKeyIDMin || digest.SignedPreKeyID > store.PreKeyIDMax {
		return nil, fmt.Errorf("invalid signed prekey ID %d", digest.SignedPreKeyID)
	}
	copy(digest.IdentityKey[:], identity)
	copy(digest.SignedPreKeyValue[:], signedValue)
	copy(digest.SignedPreKeySignature[:], signedSignature)
	seen := make(map[uint32]bool)
	list := node.GetChildByTag("list")
	for _, child := range list.GetChildren() {
		data, ok := child.Content.([]byte)
		if child.Tag != "id" || !ok || len(data) != 3 {
			return nil, fmt.Errorf("invalid prekey ID element in digest")
		}
		id := uint32(data[0])<<16 | uint32(data[1])<<8 | uint32(data[2])
		if id < store.PreKeyIDMin || id > store.PreKeyIDMax || seen[id] {
			return nil, fmt.Errorf("invalid or duplicate prekey ID %d in digest", id)
		}
		seen[id] = true
		digest.PreKeyIDs = append(digest.PreKeyIDs, id)
	}
	slices.Sort(digest.PreKeyIDs)
	return digest, nil
}

// Check the actual key material before comparing inventories. Matching IDs alone
// cannot supply a missing local private key; such a mismatch needs a rebuild.
func (cli *Client) preKeyDigestProblem(ctx context.Context, digest *serverPreKeyDigest) (string, error) {
	switch {
	case digest.RegistrationID != cli.Store.RegistrationID:
		return "registration ID mismatch", nil
	case digest.KeyType != ecc.DjbType:
		return "key type mismatch", nil
	case digest.IdentityKey != *cli.Store.IdentityKey.Pub:
		return "identity key mismatch", nil
	case digest.SignedPreKeyID != cli.Store.SignedPreKey.KeyID || digest.SignedPreKeyValue != *cli.Store.SignedPreKey.Pub || digest.SignedPreKeySignature != *cli.Store.SignedPreKey.Signature:
		return "signed prekey mismatch", nil
	}
	for _, id := range digest.PreKeyIDs {
		if id > store.PreKeyServerIDMax {
			return fmt.Sprintf("retry-only prekey %d published on server", id), nil
		}
		key, err := cli.Store.PreKeys.GetPreKey(ctx, id)
		if err != nil {
			return "", fmt.Errorf("failed to check local prekey %d: %w", id, err)
		}
		if key == nil || key.KeyID != id || key.Priv == nil || key.Pub == nil ||
			*keys.NewKeyPairFromPrivateKey(*key.Priv).Pub != *key.Pub {
			return fmt.Sprintf("server prekey %d has no usable local private key", id), nil
		}
	}
	return "", nil
}

func (cli *Client) preKeyInventoryStatus(ctx context.Context, digest *serverPreKeyDigest) (problem string, prune bool, err error) {
	problem, err = cli.preKeyDigestProblem(ctx, digest)
	if err != nil || problem != "" {
		return problem, false, err
	}
	syncStore, ok := cli.Store.PreKeys.(store.PreKeySyncStore)
	if !ok {
		return "", false, nil
	}
	localIDs, err := syncStore.PreKeyIDs(ctx)
	if err != nil {
		return "", false, fmt.Errorf("failed to read local prekey inventory: %w", err)
	}
	serverIDs := make(map[uint32]bool, len(digest.PreKeyIDs))
	for _, id := range digest.PreKeyIDs {
		serverIDs[id] = true
	}
	intersection := 0
	for _, id := range localIDs {
		if id < store.PreKeyServerIDMin || id > store.PreKeyServerIDMax {
			return "", false, fmt.Errorf("invalid local server prekey ID %d", id)
		}
		if serverIDs[id] {
			intersection++
		} else {
			prune = true
		}
	}
	if intersection == 0 && (len(localIDs) > 0 || len(digest.PreKeyIDs) > 0) {
		return "local and server prekey inventories have no intersection", false, nil
	}
	return "", prune, nil
}

func (cli *Client) checkAndSyncPreKeys(ctx context.Context, query preKeyQuery) error {
	if err := validateLocalPreKeys(cli.Store); err != nil {
		return err
	}
	if err := cli.uploadPreKeysLock.Lock(ctx); err != nil {
		return err
	}
	defer cli.uploadPreKeysLock.Unlock()
	digest, err := getServerPreKeyDigest(ctx, query)
	if err != nil {
		// Unsupported or malformed digests never authorize a remote reset.
		// Keep the count-based replenishment path available as a fallback.
		cli.Log.Warnf("Full prekey check unavailable, checking server count: %v", err)
		count, countErr := getServerPreKeyCount(ctx, query)
		if countErr != nil {
			return fmt.Errorf("%w (count fallback also failed: %v)", err, countErr)
		}
		localCount, localErr := cli.Store.PreKeys.UploadedPreKeyCount(ctx)
		if localErr != nil {
			return fmt.Errorf("%w (local count fallback failed: %v)", err, localErr)
		}
		if count < MinPreKeyCount || localCount < MinPreKeyCount {
			cli.preKeyReplenishPending = true
			if uploadErr := cli.uploadPreKeysLocked(ctx, false, true, query); uploadErr != nil {
				return fmt.Errorf("%w (fallback upload failed: %v)", err, uploadErr)
			}
			count, countErr = getServerPreKeyCount(ctx, query)
			if countErr != nil || count < MinPreKeyCount {
				return fmt.Errorf("%w (fallback replenishment not confirmed: count=%d, error=%v)", err, count, countErr)
			}
		}
		cli.preKeyReplenishPending = false
		return err
	}
	problem, prune, err := cli.preKeyInventoryStatus(ctx, digest)
	if err != nil {
		return err
	}
	if problem != "" || prune {
		// A concurrent decryption may just have consumed a key in the first
		// snapshot. Require a fresh, valid snapshot before resetting anything.
		digest, err = getServerPreKeyDigest(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to confirm prekey inconsistency: %w", err)
		}
		problem, prune, err = cli.preKeyInventoryStatus(ctx, digest)
		if err != nil {
			return err
		}
		if problem != "" {
			return cli.repairServerPreKeys(ctx, query, problem)
		}
	}
	if syncStore, ok := cli.Store.PreKeys.(store.PreKeySyncStore); ok {
		if prune {
			err = syncStore.SyncUploadedPreKeyIDs(ctx, digest.PreKeyIDs)
		} else {
			err = syncStore.MarkPreKeysAsUploadedIDs(ctx, digest.PreKeyIDs)
		}
		if err != nil {
			return fmt.Errorf("failed to reconcile prekey upload status: %w", err)
		}
		if prune {
			cli.Log.Infof("Removed local prekeys absent from the confirmed server inventory")
		}
	}
	if len(digest.PreKeyIDs) < MinPreKeyCount {
		cli.preKeyReplenishPending = true
		if err = cli.uploadPreKeysLocked(ctx, false, true, query); err != nil {
			return err
		}
		if err = cli.verifyServerPreKeys(ctx, query); err != nil {
			return err
		}
		cli.preKeyRepairPending = false
		cli.preKeyReplenishPending = false
		cli.Log.Infof("Prekey inventory replenished and verified")
		return nil
	}
	cli.preKeyRepairPending = false
	cli.preKeyReplenishPending = false
	cli.Log.Debugf("Prekey health check passed with %d available server keys", len(digest.PreKeyIDs))
	return nil
}

func (cli *Client) repairServerPreKeys(ctx context.Context, query preKeyQuery, reason string) error {
	syncStore, ok := cli.Store.PreKeys.(store.PreKeySyncStore)
	if !ok {
		return fmt.Errorf("cannot repair %s: prekey store does not implement store.PreKeySyncStore", reason)
	}
	cli.preKeyRepairPending = true
	cli.Log.Warnf("Resetting server and local prekeys after confirmed %s", reason)
	_, err := query(ctx, infoQuery{
		Namespace: "encrypt", Type: iqSet, To: types.ServerJID, NoRetry: true,
		Content: []waBinary.Node{
			{Tag: "op", Attrs: waBinary.Attrs{"mode": "delete"}},
			{Tag: "list"}, {Tag: "pq_list"},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to reset server prekeys: %w", err)
	}
	// Do not delete the local keys until the server has acknowledged the reset.
	// Clear the entire one-time-key pool, including old retry keys, before
	// generating replacements. Account identity, signed keys and sessions stay
	// in their own stores and are not part of this reset.
	if err = syncStore.ClearPreKeys(ctx); err != nil {
		return fmt.Errorf("failed to clear local prekeys after server reset: %w", err)
	}
	if err = cli.uploadPreKeysLocked(ctx, false, true, query); err != nil {
		return fmt.Errorf("failed to upload replacement prekeys: %w", err)
	}
	if err = cli.verifyServerPreKeys(ctx, query); err != nil {
		return err
	}
	cli.preKeyRepairPending = false
	cli.preKeyReplenishPending = false
	cli.Log.Infof("Server and local prekey pools rebuilt and verified")
	return nil
}

func (cli *Client) verifyServerPreKeys(ctx context.Context, query preKeyQuery) error {
	digest, err := getServerPreKeyDigest(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to verify repaired prekeys: %w", err)
	}
	problem, err := cli.preKeyDigestProblem(ctx, digest)
	if err != nil {
		return err
	} else if problem != "" {
		return fmt.Errorf("prekey repair verification failed: %s", problem)
	} else if len(digest.PreKeyIDs) < MinPreKeyCount {
		return fmt.Errorf("prekey repair verification found insufficient server keys: %d", len(digest.PreKeyIDs))
	}
	if syncStore, ok := cli.Store.PreKeys.(store.PreKeySyncStore); ok {
		// Verification is a single snapshot, and keys may have been distributed
		// since this upload. Only confirm flags here; destructive pruning belongs
		// to the separately confirmed health-check reconciliation above.
		return syncStore.MarkPreKeysAsUploadedIDs(ctx, digest.PreKeyIDs)
	}
	return nil
}
