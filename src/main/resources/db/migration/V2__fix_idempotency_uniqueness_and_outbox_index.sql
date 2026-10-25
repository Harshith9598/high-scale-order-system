-- V2: idempotency is per (event, consumer), not per event globally.
-- V1 declared event_id UNIQUE on its own, which broke fan-out: the same event
-- (e.g. a payment result) is legitimately consumed by more than one consumer
-- group (order-status AND notification). The second consumer's insert violated
-- the global unique constraint and was misread as "duplicate".
ALTER TABLE idempotency_records DROP CONSTRAINT IF EXISTS idempotency_records_event_id_key;

-- The unique index on the pair already exists from V1
-- (idx_idempotency_event_consumer); keep the plain event_id index for lookups.

-- Outbox polling scans by (status, next_retry_at, created_at); give it a
-- matching composite index instead of separate single-column indexes.
CREATE INDEX IF NOT EXISTS idx_outbox_status_retry_created
    ON outbox_events (status, next_retry_at, created_at);
