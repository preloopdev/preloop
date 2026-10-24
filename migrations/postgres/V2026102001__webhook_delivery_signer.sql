-- Persist which registered App's webhook secret verified each delivery, so
-- durable processing pins credential selection to that App. Existing rows
-- remain NULL (legacy shared secret or pre-column rows).
SET search_path = control;

ALTER TABLE webhook_deliveries ADD COLUMN signer_app_id text;
