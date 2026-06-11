-- v15 (compatible with v8+): Add local group metadata and member pagination tables
CREATE TABLE whatsmeow_groups (
	our_jid                          TEXT    NOT NULL,
	group_jid                        TEXT    NOT NULL,
	owner_jid                        TEXT,
	owner_pn                         TEXT,
	name                             TEXT    NOT NULL DEFAULT '',
	topic                            TEXT    NOT NULL DEFAULT '',
	is_locked                        BOOLEAN NOT NULL DEFAULT false,
	is_announce                      BOOLEAN NOT NULL DEFAULT false,
	is_ephemeral                     BOOLEAN NOT NULL DEFAULT false,
	disappearing_timer               BIGINT  NOT NULL DEFAULT 0,
	is_incognito                     BOOLEAN NOT NULL DEFAULT false,
	is_parent                        BOOLEAN NOT NULL DEFAULT false,
	default_membership_approval_mode TEXT    NOT NULL DEFAULT '',
	linked_parent_jid                TEXT,
	is_default_sub_group             BOOLEAN NOT NULL DEFAULT false,
	is_join_approval_required        BOOLEAN NOT NULL DEFAULT false,
	participant_count                BIGINT  NOT NULL DEFAULT 0,
	participant_version_id           TEXT    NOT NULL DEFAULT '',
	member_add_mode                  TEXT    NOT NULL DEFAULT '',
	suspended                        BOOLEAN NOT NULL DEFAULT false,
	is_joined                        BOOLEAN NOT NULL DEFAULT true,
	is_deleted                       BOOLEAN NOT NULL DEFAULT false,
	last_sync_at                     BIGINT  NOT NULL DEFAULT 0,
	updated_at                       BIGINT  NOT NULL DEFAULT 0,
	raw_info                         TEXT,

	PRIMARY KEY (our_jid, group_jid),
	FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE whatsmeow_group_members (
	our_jid                TEXT    NOT NULL,
	group_jid              TEXT    NOT NULL,
	member_jid             TEXT    NOT NULL,
	phone_number           TEXT,
	lid                    TEXT,
	display_name           TEXT    NOT NULL DEFAULT '',
	is_admin               BOOLEAN NOT NULL DEFAULT false,
	is_super_admin         BOOLEAN NOT NULL DEFAULT false,
	status                 TEXT    NOT NULL DEFAULT 'active',
	joined_at              BIGINT  NOT NULL DEFAULT 0,
	left_at                BIGINT  NOT NULL DEFAULT 0,
	participant_version_id TEXT    NOT NULL DEFAULT '',
	updated_at             BIGINT  NOT NULL DEFAULT 0,

	PRIMARY KEY (our_jid, group_jid, member_jid),
	FOREIGN KEY (our_jid, group_jid) REFERENCES whatsmeow_groups(our_jid, group_jid) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX idx_whatsmeow_groups_list
ON whatsmeow_groups (our_jid, is_joined, is_deleted, name, group_jid);

CREATE INDEX idx_whatsmeow_group_members_list
ON whatsmeow_group_members (our_jid, group_jid, status, is_super_admin, is_admin, member_jid);

CREATE INDEX idx_whatsmeow_group_members_member
ON whatsmeow_group_members (our_jid, member_jid, status);
