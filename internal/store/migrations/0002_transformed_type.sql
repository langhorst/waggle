-- The channel translator chain may convert formats, so the transformed
-- payload's data type can differ from the inbound one and must be stored
-- for parsing (tree explorer, structural diff).
ALTER TABLE messages ADD COLUMN transformed_data_type TEXT NOT NULL DEFAULT '';
