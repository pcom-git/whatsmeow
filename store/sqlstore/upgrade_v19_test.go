package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

func TestFreshDatabaseIncludesWASARootSecretID(t *testing.T) {
	ctx := context.Background()
	address := fmt.Sprintf("file:%s?mode=memory&cache=shared&_foreign_keys=on", strings.ReplaceAll(t.Name(), "/", "-"))
	container, err := New(ctx, "sqlite3", address, nil)
	if err != nil {
		t.Fatalf("failed to create fresh sqlstore: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })
	assertWASARootSecretSchema(t, ctx, container, 19, 8)
}

func TestUpgradeV18AddsWASARootSecretID(t *testing.T) {
	container := newV18ChatSettingsStore(t, false)
	ctx := context.Background()
	assertWASARootSecretSchema(t, ctx, container, 19, 19)
	var id string
	if err := container.db.QueryRow(ctx, "SELECT wasa_root_secret_id FROM whatsmeow_chat_settings WHERE chat_jid='chat'").Scan(&id); err != nil {
		t.Fatalf("failed to read upgraded chat setting: %v", err)
	}
	if id != "" {
		t.Fatalf("expected empty default root secret ID, got %q", id)
	}
}

func TestUpgradeV18PreservesExistingWASARootSecretID(t *testing.T) {
	container := newV18ChatSettingsStore(t, true)
	ctx := context.Background()
	assertWASARootSecretSchema(t, ctx, container, 19, 19)
	var id string
	if err := container.db.QueryRow(ctx, "SELECT wasa_root_secret_id FROM whatsmeow_chat_settings WHERE chat_jid='chat'").Scan(&id); err != nil {
		t.Fatalf("failed to read existing chat setting: %v", err)
	}
	if id != "existing-root" {
		t.Fatalf("expected existing root secret ID to be retained, got %q", id)
	}
}

func TestUpgradeV18PostgresUsesVisibleSchema(t *testing.T) {
	dsn := os.Getenv("WHATSMEOW_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WHATSMEOW_TEST_POSTGRES_DSN to test PostgreSQL migrations")
	}
	ctx := context.Background()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("failed to open PostgreSQL database: %v", err)
	}
	db.SetMaxOpenConns(1)
	schemaPrefix := fmt.Sprintf("wm_upgrade_%d", time.Now().UnixNano())
	visibleSchema, otherSchema := schemaPrefix+"_visible", schemaPrefix+"_other"
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "SET search_path TO public")
		_, _ = db.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+otherSchema+" CASCADE")
		_, _ = db.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+visibleSchema+" CASCADE")
		_ = db.Close()
	})
	for _, query := range []string{
		"CREATE SCHEMA " + visibleSchema,
		"CREATE SCHEMA " + otherSchema,
		"CREATE TABLE " + visibleSchema + ".whatsmeow_version (version INTEGER NOT NULL, compat INTEGER NOT NULL)",
		"INSERT INTO " + visibleSchema + ".whatsmeow_version VALUES (18, 8)",
		"CREATE TABLE " + visibleSchema + ".whatsmeow_chat_settings (our_jid TEXT, chat_jid TEXT)",
		"INSERT INTO " + visibleSchema + ".whatsmeow_chat_settings VALUES ('self', 'chat')",
		"CREATE TABLE " + otherSchema + ".whatsmeow_chat_settings (our_jid TEXT, chat_jid TEXT, wasa_root_secret_id TEXT NOT NULL DEFAULT '')",
		"SET search_path TO " + visibleSchema + ", " + otherSchema,
	} {
		if _, err = db.ExecContext(ctx, query); err != nil {
			t.Fatalf("failed to prepare PostgreSQL schemas: %v", err)
		}
	}
	container := NewWithDB(db, "postgres", nil)
	if err = container.Upgrade(ctx); err != nil {
		t.Fatalf("failed to upgrade visible PostgreSQL schema: %v", err)
	}
	var version, count int
	if err = db.QueryRowContext(ctx, "SELECT version FROM "+visibleSchema+".whatsmeow_version").Scan(&version); err != nil {
		t.Fatalf("failed to read visible schema version: %v", err)
	}
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=$1 AND table_name='whatsmeow_chat_settings' AND column_name='wasa_root_secret_id'", visibleSchema).Scan(&count); err != nil {
		t.Fatalf("failed to inspect visible chat settings: %v", err)
	}
	if version != 19 || count != 1 {
		t.Fatalf("visible schema not migrated: version=%d column_count=%d", version, count)
	}
}

func newV18ChatSettingsStore(t *testing.T, columnExists bool) *Container {
	t.Helper()
	ctx := context.Background()
	address := fmt.Sprintf("file:%s?mode=memory&cache=shared&_foreign_keys=on", strings.ReplaceAll(t.Name(), "/", "-"))
	db, err := sql.Open("sqlite3", address)
	if err != nil {
		t.Fatalf("failed to open v18 database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	schema := `
		CREATE TABLE whatsmeow_version (version INTEGER, compat INTEGER);
		INSERT INTO whatsmeow_version (version, compat) VALUES (18, 8);
		CREATE TABLE whatsmeow_chat_settings (our_jid TEXT, chat_jid TEXT`
	if columnExists {
		schema += ", wasa_root_secret_id TEXT NOT NULL DEFAULT ''"
	}
	schema += ");"
	if columnExists {
		schema += "INSERT INTO whatsmeow_chat_settings (our_jid, chat_jid, wasa_root_secret_id) VALUES ('self', 'chat', 'existing-root');"
	} else {
		schema += "INSERT INTO whatsmeow_chat_settings (our_jid, chat_jid) VALUES ('self', 'chat');"
	}
	if _, err = db.ExecContext(ctx, schema); err != nil {
		t.Fatalf("failed to create v18 schema: %v", err)
	}
	container := NewWithDB(db, "sqlite3", nil)
	if err = container.Upgrade(ctx); err != nil {
		t.Fatalf("failed to upgrade v18 database: %v", err)
	}
	return container
}

func assertWASARootSecretSchema(t *testing.T, ctx context.Context, container *Container, wantVersion, wantCompat int) {
	t.Helper()
	var version, compat int
	if err := container.db.QueryRow(ctx, "SELECT version, compat FROM whatsmeow_version").Scan(&version, &compat); err != nil {
		t.Fatalf("failed to read schema version: %v", err)
	}
	if version != wantVersion || compat != wantCompat {
		t.Fatalf("unexpected schema version: got %d/%d, want %d/%d", version, compat, wantVersion, wantCompat)
	}
	var count int
	if err := container.db.QueryRow(ctx, "SELECT COUNT(*) FROM pragma_table_info('whatsmeow_chat_settings') WHERE name='wasa_root_secret_id'").Scan(&count); err != nil {
		t.Fatalf("failed to inspect chat settings: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one WASA root secret column, got %d", count)
	}
}
