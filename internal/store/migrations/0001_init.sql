CREATE TABLE messages (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  channel_id     TEXT    NOT NULL,
  correlation_id TEXT    NOT NULL,
  replay_of      INTEGER,
  state          TEXT    NOT NULL,
  data_type      TEXT    NOT NULL,
  raw            BLOB    NOT NULL,
  transformed    BLOB,
  error_text     TEXT    NOT NULL DEFAULT '',
  meta_json      TEXT    NOT NULL DEFAULT '{}',
  received_at    INTEGER NOT NULL,
  updated_at     INTEGER NOT NULL
);
CREATE INDEX idx_messages_channel       ON messages(channel_id, id DESC);
CREATE INDEX idx_messages_channel_state ON messages(channel_id, state, id DESC);

-- Per-destination outcome of the Recipient List fan-out. state=ERROR is
-- synonymous with dead-lettered: retrying failures stay QUEUED (attempts and
-- last_error track progress) until they exhaust retries or hit a permanent
-- rejection.
CREATE TABLE message_destinations (
  message_id     INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  destination_id TEXT    NOT NULL,
  state          TEXT    NOT NULL,
  payload        BLOB,
  attempts       INTEGER NOT NULL DEFAULT 0,
  last_error     TEXT    NOT NULL DEFAULT '',
  dead_letter    INTEGER NOT NULL DEFAULT 0,
  queued_at      INTEGER,
  sent_at        INTEGER,
  updated_at     INTEGER NOT NULL,
  PRIMARY KEY (message_id, destination_id)
);
CREATE INDEX idx_dest_dlq ON message_destinations(dead_letter) WHERE dead_letter = 1;

-- Guaranteed Delivery work queue: one row per pending delivery, consumed in
-- id order (FIFO per destination) by exactly one worker per destination.
CREATE TABLE destination_queue (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  channel_id     TEXT    NOT NULL,
  destination_id TEXT    NOT NULL,
  message_id     INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  not_before     INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_queue_dest ON destination_queue(channel_id, destination_id, id);
