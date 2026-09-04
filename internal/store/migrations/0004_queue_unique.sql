-- One pending delivery per (destination, message): the pipeline enqueues a
-- message once per destination and Requeue refuses while one is pending,
-- so the index makes that invariant the database's rather than the
-- caller's.
CREATE UNIQUE INDEX idx_queue_unique ON destination_queue(destination_id, message_id);
