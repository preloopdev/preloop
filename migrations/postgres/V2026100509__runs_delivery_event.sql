-- A webhook delivery can legitimately produce one run per event for a single
-- workflow file: a `pull_request` delivery is both the `pull_request_target`
-- and the `pull_request` event, and a workflow may list both triggers. The
-- replay key is therefore (delivery, workflow path, event). Widening a unique
-- index cannot reject a row the narrower one accepted, so existing data stays.
SET search_path = control;

DROP INDEX runs_delivery;
CREATE UNIQUE INDEX runs_delivery ON runs(webhook_delivery_id, workflow_path, event)
    WHERE webhook_delivery_id IS NOT NULL;
