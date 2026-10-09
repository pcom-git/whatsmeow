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
	WithRaw(18, 19, 19, "Remember the active WASA root secret per bot", dbutil.TxnModeOn, addWASARootSecretID).
	Finish()

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

func addWASARootSecretID(ctx context.Context, db *dbutil.Database) error {
	if db.Dialect == dbutil.Postgres {
		var tableExists bool
		err := db.QueryRow(ctx, "SELECT to_regclass('whatsmeow_chat_settings') IS NOT NULL").Scan(&tableExists)
		if err != nil || !tableExists {
			return err
		}
		_, err = db.Exec(ctx, "ALTER TABLE whatsmeow_chat_settings ADD COLUMN IF NOT EXISTS wasa_root_secret_id TEXT NOT NULL DEFAULT ''")
		return err
	}
	tableExists, err := db.TableExists(ctx, "whatsmeow_chat_settings")
	if err != nil || !tableExists {
		return err
	}
	columnExists, err := db.ColumnExists(ctx, "whatsmeow_chat_settings", "wasa_root_secret_id")
	if err != nil || columnExists {
		return err
	}
	_, err = db.Exec(ctx, "ALTER TABLE whatsmeow_chat_settings ADD COLUMN wasa_root_secret_id TEXT NOT NULL DEFAULT ''")
	return err
}
