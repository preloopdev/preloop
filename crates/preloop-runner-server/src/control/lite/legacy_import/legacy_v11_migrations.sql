-- Legacy v11 durable-state store schema: the released migration chain,
-- copied verbatim from the pre-control-backend store.
--
-- Source: crates/preloop-runner-server/src/store.rs, `const MIGRATIONS`, at
-- commit 5e7ec6ed^ (the last commit of the old-store lineage, parent of the
-- control-backend cutover 5e7ec6ed). The const also carries migration 12
-- ("message-payload-migration-marker"), excluded here because v11 is the
-- released version this importer accepts. Bodies were extracted by matching
-- the tuple form `(N, "name", r#"..."#)` / `(N, "name", "...")` and copied
-- byte-for-byte; `LEGACY_MIGRATION_SHA256` in fixture.rs pins each body and a
-- unit test re-hashes the parsed file.
--
-- Each entry: a marker line `--@@migration <version> <name>@@` followed by
-- the verbatim SQL body.

--@@migration 1 initial-control-plane-schema@@

        CREATE TABLE IF NOT EXISTS workflow_run_counters (
          repository_key TEXT NOT NULL,
          workflow_path TEXT NOT NULL,
          next_run_number INTEGER NOT NULL CHECK (next_run_number >= 1),
          PRIMARY KEY (repository_key, workflow_path)
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS runs (
          run_id TEXT PRIMARY KEY,
          repository TEXT NOT NULL,
          workflow_path TEXT NOT NULL,
          status TEXT NOT NULL,
          run_number INTEGER NOT NULL,
          run_attempt INTEGER NOT NULL,
          created_at_us INTEGER NOT NULL,
          completed_at_us INTEGER,
          record_blob BLOB NOT NULL
        ) STRICT;

        CREATE TABLE IF NOT EXISTS run_secrets (
          run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
          crypto_version INTEGER NOT NULL DEFAULT 1,
          secret_blob BLOB NOT NULL
        ) STRICT;

        CREATE TABLE IF NOT EXISTS runners (
          runner_id INTEGER PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          ephemeral INTEGER NOT NULL CHECK (ephemeral IN (0,1)),
          runner_group_id INTEGER,
          runner_group_name TEXT,
          public_key TEXT,
          rsa_public_key TEXT,
          created_at_us INTEGER NOT NULL,
          updated_at_us INTEGER NOT NULL,
          deleted_at_us INTEGER
        ) STRICT;

        CREATE TABLE IF NOT EXISTS runner_labels (
          runner_id INTEGER NOT NULL REFERENCES runners(runner_id) ON DELETE CASCADE,
          label TEXT NOT NULL,
          ordinal INTEGER NOT NULL,
          PRIMARY KEY (runner_id, label)
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS runner_sessions (
          session_id TEXT PRIMARY KEY,
          runner_id INTEGER NOT NULL,
          protocol TEXT NOT NULL,
          client_id TEXT,
          session_key_blob BLOB,
          session_iv BLOB,
          session_tag BLOB,
          created_at_us INTEGER NOT NULL,
          last_seen_at_us INTEGER NOT NULL,
          closed_at_us INTEGER
        ) STRICT;

        CREATE TABLE IF NOT EXISTS jobs (
          run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
          job_id TEXT NOT NULL,
          status TEXT NOT NULL,
          queue_kind TEXT NOT NULL CHECK (
            queue_kind IN ('ready','pending','blocked','held')
          ),
          queue_position INTEGER NOT NULL,
          payload_blob BLOB NOT NULL,
          PRIMARY KEY (run_id, job_id)
        ) STRICT, WITHOUT ROWID;

        CREATE INDEX IF NOT EXISTS jobs_claim_idx
          ON jobs (queue_kind, queue_position)
          WHERE status IN ('queued','pending');

        CREATE TABLE IF NOT EXISTS job_dependencies (
          run_id TEXT NOT NULL,
          job_id TEXT NOT NULL,
          depends_on_job_id TEXT NOT NULL,
          PRIMARY KEY (run_id, job_id, depends_on_job_id),
          FOREIGN KEY (run_id, job_id)
            REFERENCES jobs(run_id, job_id) ON DELETE CASCADE
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS job_requests (
          request_id INTEGER PRIMARY KEY,
          run_id TEXT NOT NULL,
          job_id TEXT NOT NULL,
          agent_job_id TEXT NOT NULL UNIQUE,
          plan_id TEXT NOT NULL UNIQUE,
          timeline_id TEXT NOT NULL UNIQUE,
          state TEXT NOT NULL,
          request_blob BLOB NOT NULL,
          FOREIGN KEY (run_id)
            REFERENCES runs(run_id) ON DELETE CASCADE
        ) STRICT;

        CREATE TABLE IF NOT EXISTS runner_commands (
          message_id INTEGER PRIMARY KEY,
          command_type TEXT NOT NULL,
          run_id TEXT NOT NULL,
          job_id TEXT NOT NULL,
          payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
          state TEXT NOT NULL,
          created_at_us INTEGER NOT NULL
        ) STRICT;

        CREATE TABLE IF NOT EXISTS control_events (
          event_id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id TEXT NOT NULL,
          job_id TEXT,
          event_type TEXT NOT NULL,
          payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
          created_at_us INTEGER NOT NULL
        ) STRICT;

        CREATE INDEX IF NOT EXISTS control_events_cursor_idx
          ON control_events (run_id, event_id);

        CREATE TABLE IF NOT EXISTS session_active_requests (
          session_id TEXT PRIMARY KEY,
          active_request_id INTEGER NOT NULL
            REFERENCES job_requests(request_id) ON DELETE CASCADE
        ) STRICT;

        CREATE TABLE IF NOT EXISTS broker_messages (
          session_id TEXT NOT NULL,
          message_id INTEGER NOT NULL,
          payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
          written_at_us INTEGER NOT NULL,
          PRIMARY KEY (session_id, message_id)
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS log_files (
          log_key TEXT PRIMARY KEY,
          byte_count INTEGER NOT NULL DEFAULT 0 CHECK (byte_count >= 0),
          line_count INTEGER NOT NULL DEFAULT 0 CHECK (line_count >= 0),
          updated_at_us INTEGER NOT NULL
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS log_chunks (
          log_key TEXT NOT NULL REFERENCES log_files(log_key) ON DELETE CASCADE,
          chunk_index INTEGER NOT NULL,
          payload BLOB NOT NULL,
          written_at_us INTEGER NOT NULL,
          PRIMARY KEY (log_key, chunk_index)
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS runtime_snapshots (
          snapshot_id INTEGER PRIMARY KEY CHECK (snapshot_id = 1),
          format_version INTEGER NOT NULL,
          meta_blob BLOB NOT NULL,
          written_at_us INTEGER NOT NULL
        ) STRICT;
        
--@@migration 2 drop-redundant-run-secrets@@

        DROP TABLE IF EXISTS run_secrets;
        
--@@migration 3 job-request-messages-table@@

        CREATE TABLE IF NOT EXISTS job_request_messages (
          request_id INTEGER PRIMARY KEY,
          payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
          written_at_us INTEGER NOT NULL
        ) STRICT;
        
--@@migration 4 job-steps-table@@

        CREATE TABLE IF NOT EXISTS job_steps (
          run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
          agent_job_id TEXT NOT NULL,
          step_id TEXT NOT NULL,
          kind TEXT NOT NULL CHECK (kind IN ('workflow','synthetic')),
          workflow_index INTEGER,
          runner_number INTEGER,
          context_name TEXT,
          name_blob BLOB NOT NULL,
          conclusion TEXT NOT NULL,
          started_at_us INTEGER,
          finished_at_us INTEGER,
          -- Monotonic per attempt, bumped on every in-memory mutation of the
          -- manifest. Reconciliation snapshots under the state lock and writes
          -- after releasing it, so two reports for one attempt can commit out
          -- of order; the upsert compares this and refuses to move a row
          -- backwards rather than letting the older snapshot win.
          revision INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY (agent_job_id, step_id)
        ) STRICT, WITHOUT ROWID;

        CREATE INDEX IF NOT EXISTS job_steps_order_idx
          ON job_steps (agent_job_id, kind, workflow_index);
        
--@@migration 5 runtime-snapshot-revision@@
ALTER TABLE runtime_snapshots ADD COLUMN revision INTEGER NOT NULL DEFAULT 0;
--@@migration 6 webhook-deliveries-table@@

        CREATE TABLE IF NOT EXISTS webhook_deliveries (
          delivery_id TEXT PRIMARY KEY,
          event TEXT NOT NULL,
          payload_blob BLOB NOT NULL,
          received_at_us INTEGER NOT NULL,
          state TEXT NOT NULL CHECK (state IN ('received', 'processing', 'done', 'failed')),
          attempts INTEGER NOT NULL DEFAULT 0,
          lease_until_us INTEGER,
          last_error TEXT
        ) STRICT;

        CREATE INDEX IF NOT EXISTS webhook_deliveries_claim_idx
          ON webhook_deliveries (state, received_at_us);
        
--@@migration 7 webhook-delivery-lease-fencing@@

        ALTER TABLE webhook_deliveries ADD COLUMN lease_token TEXT;
        
--@@migration 8 webhook-run-reservation@@

        ALTER TABLE runs ADD COLUMN webhook_delivery_id TEXT;
        CREATE UNIQUE INDEX IF NOT EXISTS runs_webhook_delivery_workflow_idx
          ON runs (webhook_delivery_id, workflow_path)
          WHERE webhook_delivery_id IS NOT NULL;
        
--@@migration 9 webhook-delivery-repair-state@@

        CREATE TABLE IF NOT EXISTS webhook_watchdog (
          scope TEXT PRIMARY KEY,
          cursor_delivered_at_us INTEGER,
          last_poll_at_us INTEGER,
          last_success_at_us INTEGER
        ) STRICT;

        CREATE TABLE IF NOT EXISTS webhook_redeliveries (
          delivery_guid TEXT PRIMARY KEY,
          github_delivery_id INTEGER NOT NULL,
          app_id TEXT NOT NULL,
          reason TEXT NOT NULL CHECK (reason IN ('remote_failure', 'phantom_ack')),
          attempts INTEGER NOT NULL DEFAULT 0,
          first_seen_at_us INTEGER NOT NULL,
          last_attempt_at_us INTEGER,
          resolved_at_us INTEGER,
          last_error TEXT
        ) STRICT;

        CREATE INDEX IF NOT EXISTS webhook_redeliveries_open_idx
          ON webhook_redeliveries (resolved_at_us, first_seen_at_us);

        
--@@migration 10 drop-source-state-reconciler@@
DROP TABLE IF EXISTS webhook_synthetic_events;
--@@migration 11 webhook-watchdog-safe-pagination@@

        ALTER TABLE webhook_watchdog ADD COLUMN cursor_delivered_at_guid TEXT;
        ALTER TABLE webhook_watchdog ADD COLUMN scan_cursor TEXT;
        