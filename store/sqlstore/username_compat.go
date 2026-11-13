// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlstore

import (
	"context"
	"errors"
	"fmt"

	"go.mau.fi/util/dbutil"

	"go.mau.fi/whatsmeow/types"
)

// ensureLocalUsernameColumn keeps the temporary username extension outside the
// upstream schema version sequence. The distinct column name also leaves room
// for an upstream username column without a duplicate-column migration failure.
func (c *Container) ensureLocalUsernameColumn(ctx context.Context) error {
	exists, err := c.localUsernameColumnExists(ctx)
	if err != nil {
		return fmt.Errorf("failed to check local username column: %w", err)
	} else if exists {
		return nil
	}
	_, err = c.db.Exec(ctx, "ALTER TABLE whatsmeow_contacts ADD COLUMN local_username TEXT NOT NULL DEFAULT ''")
	if err == nil {
		return nil
	}
	// Another initializer may have added the column after the first check. Only
	// suppress that race if a fresh check confirms the column now exists.
	if ctx.Err() == nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		if exists, checkErr := c.localUsernameColumnExists(ctx); checkErr == nil && exists {
			return nil
		}
	}
	return fmt.Errorf("failed to add local username column: %w", err)
}

func (c *Container) localUsernameColumnExists(ctx context.Context) (exists bool, err error) {
	if c.db.Dialect != dbutil.Postgres {
		return c.db.ColumnExists(ctx, "whatsmeow_contacts", "local_username")
	}
	// Resolve the same table that the unqualified ALTER and contact queries use.
	// dbutil.ColumnExists also matches columns in unrelated PostgreSQL schemas.
	err = c.db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_attribute
			WHERE attrelid=to_regclass('whatsmeow_contacts')
			  AND attname='local_username' AND NOT attisdropped
		)
	`).Scan(&exists)
	return
}

const putLocalContactUsernameQuery = `
	INSERT INTO whatsmeow_contacts (our_jid, their_jid, local_username) VALUES ($1, $2, $3)
	ON CONFLICT (our_jid, their_jid) DO UPDATE SET local_username=excluded.local_username
`

// PutLocalContactUsername saves a username only when it differs from the cached
// contact value. Missing usernames do not clear previously observed values.
func (s *SQLStore) PutLocalContactUsername(ctx context.Context, user types.JID, username string) error {
	if username == "" {
		return nil
	}
	s.contactCacheLock.Lock()
	defer s.contactCacheLock.Unlock()

	cached, err := s.getContact(ctx, user)
	if err != nil {
		return err
	} else if cached.Username == username {
		return nil
	}
	_, err = s.db.Exec(ctx, putLocalContactUsernameQuery, s.JID, user, username)
	if err != nil {
		return err
	}
	cached.Username = username
	cached.Found = true
	return nil
}
