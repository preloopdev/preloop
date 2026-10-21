-- Persist which registered App's webhook secret verified each delivery, so
-- durable processing pins credential selection to that App. Existing rows
-- remain NULL (legacy shared secret or pre-column rows).
ALTER TABLE webhook_deliveries ADD COLUMN signer_app_id TEXT;
