// Copyright (c) 2025 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package upgrades

import (
	"context"
	"embed"

	"go.mau.fi/util/dbutil"
)

//go:embed *.sql
var upgrades embed.FS

var Table = dbutil.BuildUpgradeTable().
	WithFS(upgrades).
	WithRaw(17, 18, 8, "Add companion meta nonce column to device table", dbutil.TxnModeOn, addCompanionMetaNonce).
	WithRaw(18, 19, 8, "Remember the active WASA root secret per bot", dbutil.TxnModeOn, addWASARootSecret).
	WithRaw(19, 20, 8, "Remember prekey creation times for retry retention", dbutil.TxnModeOn, addPreKeyCreationTime).
	Finish()

func addWASARootSecret(ctx context.Context, db *dbutil.Database) error {
	tableExists, err := visibleTableExists(ctx, db, "whatsmeow_chat_settings")
	if err != nil || !tableExists {
		return err
	}
	columnExists, err := visibleColumnExists(ctx, db, "whatsmeow_chat_settings", "wasa_root_secret_id")
	if err != nil || columnExists {
		return err
	}
	_, err = db.Exec(ctx, "ALTER TABLE whatsmeow_chat_settings ADD COLUMN wasa_root_secret_id TEXT NOT NULL DEFAULT ''")
	return err
}

func addCompanionMetaNonce(ctx context.Context, db *dbutil.Database) error {
	tableExists, err := db.TableExists(ctx, "whatsmeow_device")
	if err != nil || !tableExists {
		return err
	}
	columnExists, err := db.ColumnExists(ctx, "whatsmeow_device", "companion_meta_nonce")
	if err != nil || columnExists {
		return err
	}
	_, err = db.Exec(ctx, "ALTER TABLE whatsmeow_device ADD COLUMN companion_meta_nonce TEXT NOT NULL DEFAULT ''")
	return err
}

func addPreKeyCreationTime(ctx context.Context, db *dbutil.Database) error {
	tableExists, err := visibleTableExists(ctx, db, "whatsmeow_pre_keys")
	if err != nil || !tableExists {
		return err
	}
	columnExists, err := visibleColumnExists(ctx, db, "whatsmeow_pre_keys", "created_at")
	if err != nil {
		return err
	}
	if !columnExists {
		if _, err = db.Exec(ctx, "ALTER TABLE whatsmeow_pre_keys ADD COLUMN created_at BIGINT NOT NULL DEFAULT 0"); err != nil {
			return err
		}
	}
	// Older clients only marked uploads after an IQ reply. A pending key may
	// already have been distributed if that reply was lost. Retain its private
	// key, but never publish it again after upgrading to reserve-before-send.
	if _, err = db.Exec(ctx, "UPDATE whatsmeow_pre_keys SET uploaded=true WHERE key_id BETWEEN 1 AND 16000000 AND uploaded=false"); err != nil {
		return err
	}
	_, err = db.Exec(ctx, "CREATE INDEX IF NOT EXISTS whatsmeow_pre_keys_created_at ON whatsmeow_pre_keys (jid, created_at, key_id)")
	return err
}

// PostgreSQL may contain an unrelated copy of the table in another schema.
// Resolve the same visible relation as the subsequent unqualified ALTER TABLE.
func visibleTableExists(ctx context.Context, db *dbutil.Database, table string) (exists bool, err error) {
	if db.Dialect != dbutil.Postgres {
		return db.TableExists(ctx, table)
	}
	err = db.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists)
	return
}

func visibleColumnExists(ctx context.Context, db *dbutil.Database, table, column string) (exists bool, err error) {
	if db.Dialect != dbutil.Postgres {
		return db.ColumnExists(ctx, table, column)
	}
	err = db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_attribute
			WHERE attrelid=to_regclass($1) AND attname=$2 AND attnum>0 AND NOT attisdropped
		)
	`, table, column).Scan(&exists)
	return
}
