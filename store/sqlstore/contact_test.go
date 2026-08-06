// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlstore

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow/types"
)

func newTestSQLStore(t *testing.T) (*Container, *SQLStore) {
	t.Helper()
	ctx := context.Background()
	container, err := New(ctx, "sqlite3", "file::memory:?cache=shared&_foreign_keys=on", nil)
	if err != nil {
		t.Fatalf("failed to create sqlstore: %v", err)
	}
	t.Cleanup(func() {
		_ = container.Close()
	})
	ourJID := types.NewJID("1234", types.DefaultUserServer)
	_, err = container.db.Exec(ctx, `
		INSERT INTO whatsmeow_device (
			jid, registration_id, noise_key, identity_key,
			signed_pre_key, signed_pre_key_id, signed_pre_key_sig,
			adv_key, adv_details, adv_account_sig, adv_account_sig_key, adv_device_sig
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, ourJID, 1, make([]byte, 32), make([]byte, 32), make([]byte, 32), 1, make([]byte, 64),
		make([]byte, 32), []byte{1}, make([]byte, 64), make([]byte, 32), make([]byte, 64))
	if err != nil {
		t.Fatalf("failed to insert test device: %v", err)
	}
	return container, NewSQLStore(container, ourJID)
}

func TestContactNameStoresAddressBookFlag(t *testing.T) {
	ctx := context.Background()
	_, store := newTestSQLStore(t)
	contactJID := types.NewJID("15551234567", types.DefaultUserServer)

	if err := store.PutContactName(ctx, contactJID, "Alice", "Alice Example", true); err != nil {
		t.Fatalf("failed to put contact name: %v", err)
	}
	info, err := store.GetContact(ctx, contactJID)
	if err != nil {
		t.Fatalf("failed to get contact: %v", err)
	}
	if !info.IsAddContact {
		t.Fatal("expected contact action writes to mark address book contact")
	}

	pushJID := types.NewJID("15559876543", types.DefaultUserServer)
	if _, _, err = store.PutPushName(ctx, pushJID, "Push Name"); err != nil {
		t.Fatalf("failed to put push name: %v", err)
	}
	info, err = store.GetContact(ctx, pushJID)
	if err != nil {
		t.Fatalf("failed to get push contact: %v", err)
	}
	if info.IsAddContact {
		t.Fatal("expected non-contact rows to default to not address book contacts")
	}
}

func TestLatestSchemaHasIsAddContactColumn(t *testing.T) {
	ctx := context.Background()
	container, _ := newTestSQLStore(t)

	var defaultValue sql.NullString
	err := container.db.QueryRow(ctx, `
		SELECT dflt_value
		FROM pragma_table_info('whatsmeow_contacts')
		WHERE name='is_add_contact'
	`).Scan(&defaultValue)
	if err != nil {
		t.Fatalf("expected is_add_contact column in latest schema: %v", err)
	}
	if !defaultValue.Valid || defaultValue.String != "false" {
		t.Fatalf("expected is_add_contact default false, got %q valid=%t", defaultValue.String, defaultValue.Valid)
	}
}

func TestUpgradeAddsIsAddContactColumn(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", "file:contact-upgrade?mode=memory&cache=shared&_foreign_keys=on")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	_, err = db.ExecContext(ctx, `
		CREATE TABLE whatsmeow_version (version INTEGER, compat INTEGER);
		INSERT INTO whatsmeow_version (version, compat) VALUES (16, 8);
		CREATE TABLE whatsmeow_contacts (
			our_jid        TEXT,
			their_jid      TEXT,
			first_name     TEXT,
			full_name      TEXT,
			push_name      TEXT,
			business_name  TEXT,
			redacted_phone TEXT,
			PRIMARY KEY (our_jid, their_jid)
		);
	`)
	if err != nil {
		t.Fatalf("failed to create old schema: %v", err)
	}

	container := NewWithDB(db, "sqlite3", nil)
	if err = container.Upgrade(ctx); err != nil {
		t.Fatalf("failed to upgrade old schema: %v", err)
	}

	var defaultValue sql.NullString
	err = db.QueryRowContext(ctx, `
		SELECT dflt_value
		FROM pragma_table_info('whatsmeow_contacts')
		WHERE name='is_add_contact'
	`).Scan(&defaultValue)
	if err != nil {
		t.Fatalf("expected is_add_contact column after upgrade: %v", err)
	}
	if !defaultValue.Valid || defaultValue.String != "false" {
		t.Fatalf("expected upgraded is_add_contact default false, got %q valid=%t", defaultValue.String, defaultValue.Valid)
	}
}
