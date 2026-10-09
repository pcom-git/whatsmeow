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
