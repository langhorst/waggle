-- Transformer scripts can write meta entries that outbound adapters read
-- (e.g. http.path routing the http-sender per message). Queued deliveries
-- happen later — and DLQ requeues later still — so the delivery's meta is
-- persisted alongside its payload. NULL keeps any previously stored value,
-- mirroring the payload column's semantics.
ALTER TABLE message_destinations ADD COLUMN meta TEXT;
