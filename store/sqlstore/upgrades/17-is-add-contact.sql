-- v16 -> v17: Add explicit address book contact marker.
ALTER TABLE whatsmeow_contacts ADD COLUMN is_add_contact BOOLEAN NOT NULL DEFAULT false;
