-- v16 (compatible with v8+): Add local label metadata and member pagination tables
CREATE TABLE whatsmeow_labels (
	our_jid          TEXT    NOT NULL,
	label_id         TEXT    NOT NULL,
	name             TEXT    NOT NULL DEFAULT '',
	type             INTEGER NOT NULL DEFAULT 0,
	color            INTEGER NOT NULL DEFAULT 0,
	predefined_id    INTEGER NOT NULL DEFAULT 0,
	deleted          BOOLEAN NOT NULL DEFAULT false,
	is_active        BOOLEAN NOT NULL DEFAULT true,
	order_index      INTEGER NOT NULL DEFAULT 0,
	is_immutable     BOOLEAN NOT NULL DEFAULT false,
	mute_end_time_ms BIGINT  NOT NULL DEFAULT 0,
	is_pending       BOOLEAN NOT NULL DEFAULT false,
	last_event_time  BIGINT  NOT NULL DEFAULT 0,
	from_full_sync   BOOLEAN NOT NULL DEFAULT false,
	updated_at       BIGINT  NOT NULL,
	raw_action       TEXT,

	PRIMARY KEY (our_jid, label_id),
	FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE whatsmeow_label_members (
	our_jid         TEXT    NOT NULL,
	label_id        TEXT    NOT NULL,
	chat_jid        TEXT    NOT NULL,
	chat_type       TEXT    NOT NULL DEFAULT 'unknown',
	labeled         BOOLEAN NOT NULL DEFAULT true,
	source          TEXT    NOT NULL,
	last_event_time BIGINT  NOT NULL DEFAULT 0,
	from_full_sync  BOOLEAN NOT NULL DEFAULT false,
	updated_at      BIGINT  NOT NULL,
	raw_action      TEXT,

	PRIMARY KEY (our_jid, label_id, chat_jid),
	FOREIGN KEY (our_jid, label_id) REFERENCES whatsmeow_labels(our_jid, label_id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX idx_whatsmeow_labels_list
ON whatsmeow_labels (our_jid, is_pending, deleted, is_active, order_index, label_id);

CREATE INDEX idx_whatsmeow_labels_type
ON whatsmeow_labels (our_jid, type, deleted, is_active);

CREATE INDEX idx_whatsmeow_label_members_by_label
ON whatsmeow_label_members (our_jid, label_id, labeled, chat_type, chat_jid);

CREATE INDEX idx_whatsmeow_label_members_by_chat
ON whatsmeow_label_members (our_jid, chat_jid, labeled);
