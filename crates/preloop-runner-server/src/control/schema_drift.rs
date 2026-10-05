//! Schema-drift guard between the two copies of the control schema, plus the
//! reserved/unused inventory the maintainers must keep honest.
//!
//! `lite/schema.sql` is the SQLite translation of `pg/schema.sql` (the
//! source of truth, mirrored into `docs/control-schema.sql`). Both must
//! expose the same tables, the same column names, the same foreign keys and
//! the same indexes — only storage types differ (uuid/text -> TEXT,
//! timestamptz -> INTEGER microseconds, jsonb -> TEXT, bytea -> BLOB,
//! identity -> INTEGER PRIMARY KEY AUTOINCREMENT).
//!
//! [`DOCUMENTED_DIFFERENCES`] is the complete list of deliberate exceptions;
//! every entry carries the reason it exists. A difference that is not listed
//! fails [`control_schema_differences_are_documented`], and a listed
//! difference that no longer exists fails too — stale entries must be
//! deleted, so the guard shrinks itself whenever the backends are aligned.
//!
//! This guard is parse-level only. It cannot see storage semantics (a column
//! UNIQUE here is a constraint there), so it does not bless any difference it
//! cannot express — see the `UNIQUE`/foreign-key checks, which are the parts
//! of the drift that matter for correctness.
//!
//! This is not a migration tool: a database created at any other stamped
//! `schema_version` is still refused at open, and changing that is a separate
//! release blocker (`lite/mod.rs` / `pg/mod.rs`). Until versioned migrations
//! exist, this module's job is to keep the *fresh-database* schemas honest
//! and to name every place the two backends disagree.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// The SQLite translation (see the file header for the type mapping).
const LITE_SCHEMA: &str = include_str!("lite/schema.sql");
/// The Postgres source of truth.
const PG_SCHEMA: &str = include_str!("pg/schema.sql");

/// Deliberate structural differences between the two schemas, keyed exactly
/// as [`differences`] reports them. Keep the keys verbatim: a typo makes the
/// guard fail twice (undocumented difference + stale entry).
const DOCUMENTED_DIFFERENCES: &[(&str, &str)] = &[
    // ── Partitioning: Postgres-only ──────────────────────────────────
    (
        "table attempt_history_default: pg only",
        "partition child: pg partitions the *_history tables by run_created_at and drops whole \
         partitions; SQLite has no partitioning and keeps one table. The logical table and its \
         columns are identical.",
    ),
    (
        "table job_history_default: pg only",
        "partition child of job_history (see attempt_history_default).",
    ),
    (
        "table outbox_events_default: pg only",
        "partition child of outbox_events (see attempt_history_default).",
    ),
    (
        "table run_history_default: pg only",
        "partition child of run_history (see attempt_history_default).",
    ),
    (
        "table step_history_default: pg only",
        "partition child of step_history (see attempt_history_default).",
    ),
    // ── Outbox ordering: single writer vs xid8 safe point ────────────
    (
        "table outbox_events column txid: pg only",
        "pg orders the outbox by (txid, event_id) below pg_snapshot_xmin to read only finished \
         transactions. SQLite has exactly one writer (BEGIN IMMEDIATE), so commit order is \
         event_id order and no safe-point column exists. Owned by the durable-outbox PR: do not \
         drop or rename on either side.",
    ),
    (
        "table outbox_events column origin: pg only",
        "pg stamps the writing node (`preloop.origin`) so a node skips its own rows; the single \
         SQLite writer has no other node to skip.",
    ),
    (
        "table consumer_offsets column last_txid: pg only",
        "pg bookmarks the outbox by xid8 safe point; SQLite bookmarks by last_event_id. Owned by \
         the durable-outbox PR: do not drop or rename on either side.",
    ),
    (
        "index outbox_events_read: pg only",
        "the (txid, event_id) reader index; there is no txid column on SQLite (see above).",
    ),
    // ── Indexes that need an engine feature SQLite has no equivalent of ─
    (
        "index runners_labels: pg only",
        "GIN index over the `labels` jsonb (label -> runner search). SQLite has no GIN; lite \
         matches labels in Rust (`runner_label_sets`) over the same JSON payload.",
    ),
    // ── Columns and constraints that encode a deliberate behavioral split ─
    (
        "table job_messages column job_timeout_s: pg only",
        "pg extracts `timeout-minutes` once at write because the reaper reads it for every \
         unfinished attempt on every tick and detoasting the template cost ~15x the rest of the \
         query. Lite re-extracts with json_extract from a non-toasted TEXT column. Aligning the \
         two needs a schema-version bump with a tested migration (existing databases have no \
         column and the reaper must not fail on them).",
    ),
    (
        "table job_requests column timeline_id inline UNIQUE: pg only",
        "intentional: a timeline is shared across the attempts of one job. Pg makes the column \
         UNIQUE so `timelines` can FK to it; SQLite keeps several job_requests rows on one \
         timeline_id and deletes the timeline through the `job_requests_timeline_cascade` trigger \
         when the last request goes.",
    ),
    (
        "table timelines foreign key timeline_id -> job_requests(timeline_id): pg only",
        "intentional: the reverse-direction FK for the pg UNIQUE above cannot be expressed on a \
         shared, non-unique SQLite column; `job_requests_timeline_cascade` owns the lifecycle.",
    ),
    (
        "table runner_sessions foreign key runner_id -> runners(runner_id): pg only",
        "intentional: the legacy AgentSession/distributedtask create may name an agent id whose \
         registration arrives later (`require_live_runner: false` in runner_lifecycle.rs). SQLite \
         runs with `PRAGMA foreign_keys = ON`, so an FK would reject that row; lite instead \
         checks liveness inside the insert transaction when the caller requires it, reads through \
         a LEFT JOIN, and deletes the sessions before the runner row (lifecycle.rs). Note: the pg \
         FK means the legacy create cannot work against Postgres — tracked as a parity finding.",
    ),
];

/// Schema objects kept although *no* code reads them yet — placeholders for
/// features the maintainers intend to wire up. The test below greps the Rust
/// sources so the day one of them starts being used, this list (and the
/// matching `-- RESERVED:` comment in both schemas) must be revisited.
const RESERVED_UNUSED: &[Kept] = &[
    Kept {
        what: "table namespace_policies",
        why: "planned: hosted-tenant admission policy (fork-PR policy, token permission ceiling, \
              OIDC audiences, local-execution policy)",
        needles: &[("namespace_policies", None)],
        allowed_mentions: &[],
    },
    Kept {
        what: "column namespace_limits.max_job_timeout_minutes",
        why: "planned: per-tenant job timeout cap, enforced at claim/acquire",
        needles: &[("namespace_limits", Some("max_job_timeout_minutes"))],
        allowed_mentions: &[],
    },
    Kept {
        what: "column namespace_limits.priority_tier",
        why: "planned: input to the tenant ordering policy (dispatch orders by priority, \
              run_order, job_order today)",
        needles: &[("namespace_limits", Some("priority_tier"))],
        allowed_mentions: &[],
    },
    Kept {
        what: "column namespace_limits.run_history_retention_days",
        why: "planned: per-tenant history retention window",
        needles: &[("namespace_limits", Some("run_history_retention_days"))],
        allowed_mentions: &[],
    },
    Kept {
        what: "table artifacts",
        why: "planned: shared artifact index (bytes live in the file-backed ArtifactStore). \
              Today the only production statements are the run-archive `DELETE FROM artifacts`; \
              the two test-only statements in retention.rs are listed as allowed mentions.",
        needles: &[("artifacts", None)],
        allowed_mentions: &[
            "SELECT count(*) FROM artifacts WHERE run_id = ?1",
            "INSERT INTO artifacts (namespace_id, run_id, job_backend_id, name",
        ],
    },
];

/// Schema objects with no reader and no writer anywhere in this crate. They
/// are *not* dropped here: this branch has no migration runner, and a
/// database created from this file refuses any other stamped version (see
/// the module docs), so removing a column would strand existing databases.
/// Drop them only in a migration-backed change; until then this list (and the
/// `-- UNUSED:` comment at each site) is the inventory.
const UNUSED: &[Kept] = &[
    Kept {
        what: "column namespaces.cell_generation",
        why: "pg planned it to fence stale writers after a cell move; no code reads or writes it",
        needles: &[("namespaces", Some("cell_generation"))],
        allowed_mentions: &[],
    },
    Kept {
        what: "column namespaces.config_version",
        why: "pg planned it for platform config pushes; no code reads or writes it",
        needles: &[("namespaces", Some("config_version"))],
        allowed_mentions: &[],
    },
    Kept {
        what: "column run_submissions.secret_refs",
        why: "secret values are resolved at acquire and never stored; no code reads or writes the \
              column (the secret-name list travels in `submission`)",
        needles: &[("run_submissions", Some("secret_refs"))],
        allowed_mentions: &[],
    },
    Kept {
        what: "column jobs.not_before",
        why: "retry-backoff / delayed-start leftover from the old scheduler; the queue orders by \
              priority, run_order, job_order",
        needles: &[("jobs", Some("not_before"))],
        allowed_mentions: &[],
    },
    Kept {
        what: "column provision_requests.lease_owner",
        why: "the provisioner queue has no lease protocol; no code reads or writes it",
        needles: &[("provision_requests", Some("lease_owner"))],
        allowed_mentions: &[],
    },
    Kept {
        what: "column provision_requests.leased_until",
        why: "the provisioner queue has no lease protocol; no code reads or writes it",
        needles: &[("provision_requests", Some("leased_until"))],
        allowed_mentions: &[],
    },
    Kept {
        what: "column runner_sessions.engine_node_id",
        why: "long-poll wake routing is in-process (`control/wake.rs`), not persisted",
        needles: &[("runner_sessions", Some("engine_node_id"))],
        allowed_mentions: &[],
    },
    Kept {
        what: "column log_files.byte_count",
        why: "log sizes live in the log-segment store; the control table only allocates \
              (plan_id, log_id)",
        needles: &[("log_files", Some("byte_count"))],
        allowed_mentions: &[],
    },
    Kept {
        what: "column log_files.line_count",
        why: "log sizes live in the log-segment store; the control table only allocates \
              (plan_id, log_id)",
        needles: &[("log_files", Some("line_count"))],
        allowed_mentions: &[],
    },
    Kept {
        what: "column artifacts.upload_token_hash",
        why: "the file-backed ArtifactStore owns upload state; the control table is a delete-only \
              index today",
        needles: &[("artifacts", Some("upload_token_hash"))],
        allowed_mentions: &[],
    },
    Kept {
        what: "column artifacts.finalized_at",
        why: "the file-backed ArtifactStore owns upload state; the control table is a delete-only \
              index today",
        needles: &[("artifacts", Some("finalized_at"))],
        allowed_mentions: &[],
    },
];

/// One kept-but-unused schema object and the evidence it is still unused.
struct Kept {
    /// Human description used in the failure message.
    what: &'static str,
    /// Why it is kept. Quoted in the failure message.
    why: &'static str,
    /// `(table, column)`: a column when set, a whole table otherwise.
    needles: &'static [(&'static str, Option<&'static str>)],
    /// Statement fragments that are known to mention the object and are
    /// accepted (test-only helpers); every other match fails the test.
    allowed_mentions: &'static [&'static str],
}

#[test]
fn control_schema_differences_are_documented() {
    let differences = differences();
    let documented: BTreeMap<&str, &str> = DOCUMENTED_DIFFERENCES.iter().copied().collect();
    let mut problems = Vec::new();
    for difference in &differences {
        if !documented.contains_key(difference.as_str()) {
            problems.push(format!("undocumented difference: {difference}"));
        }
    }
    for (difference, _reason) in DOCUMENTED_DIFFERENCES {
        if !differences.contains(*difference) {
            problems.push(format!(
                "stale entry, delete it from DOCUMENTED_DIFFERENCES: {difference}"
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "the SQLite and Postgres control schemas drifted ({} difference(s)):\n{}",
        differences.len(),
        problems.join("\n")
    );
}

#[test]
fn reserved_and_unused_objects_stay_unreferenced() {
    let sources = rust_sources();
    let mut findings = Vec::new();
    for kept in RESERVED_UNUSED.iter().chain(UNUSED) {
        for (table, column) in kept.needles {
            for (path, text) in &sources {
                if path.ends_with("schema_drift.rs") {
                    continue;
                }
                for statement in sql_mentions(text, table, *column) {
                    if kept
                        .allowed_mentions
                        .iter()
                        .any(|allowed| statement.contains(allowed))
                    {
                        continue;
                    }
                    findings.push(format!(
                        "{} ({}): {statement}\n    in {}",
                        kept.what,
                        kept.why,
                        path.display()
                    ));
                }
            }
        }
    }
    assert!(
        findings.is_empty(),
        "these schema objects are declared RESERVED/UNUSED but the code references them now:\n{}\n\
         If this is a real use, remove the object from RESERVED_UNUSED/UNUSED in \
         control/schema_drift.rs, delete its `-- RESERVED:`/`-- UNUSED:` comment in both schemas, \
         and (for a wired-up feature) drop the `unused` claim from the PR.",
        findings.join("\n")
    );
}

// ── The parsed schemas ──────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
struct Table {
    columns: BTreeSet<String>,
    /// Columns declared with an inline `UNIQUE`.
    unique_columns: BTreeSet<String>,
    /// `column -> table(columns)` for inline `REFERENCES`, `a,b -> table(x,y)`
    /// for table-level `FOREIGN KEY` clauses. The `ON DELETE` action is not
    /// part of the key (it is behavioral, not structural, and both schemas
    /// pick the action to match their own cascade rules).
    foreign_keys: BTreeSet<String>,
}

#[derive(Debug, Default)]
struct Schema {
    tables: BTreeMap<String, Table>,
    /// Index name -> ordered column list (ASC/DESC and the access method are
    /// not part of the comparison; the guard still catches column-order and
    /// column-set drift because the list is ordered).
    indexes: BTreeMap<String, Vec<String>>,
}

fn differences() -> BTreeSet<String> {
    let lite = parse(LITE_SCHEMA);
    let pg = parse(PG_SCHEMA);
    let mut out = BTreeSet::new();
    for name in lite.tables.keys() {
        if !pg.tables.contains_key(name) {
            out.insert(format!("table {name}: lite only"));
        }
    }
    for name in pg.tables.keys() {
        if !lite.tables.contains_key(name) {
            out.insert(format!("table {name}: pg only"));
        }
    }
    for (name, lite_table) in &lite.tables {
        let Some(pg_table) = pg.tables.get(name) else {
            continue;
        };
        diff_members(
            &lite_table.columns,
            &pg_table.columns,
            |item, side| format!("table {name} column {item}: {side} only"),
            &mut out,
        );
        diff_members(
            &lite_table.unique_columns,
            &pg_table.unique_columns,
            |item, side| format!("table {name} column {item} inline UNIQUE: {side} only"),
            &mut out,
        );
        diff_members(
            &lite_table.foreign_keys,
            &pg_table.foreign_keys,
            |item, side| format!("table {name} foreign key {item}: {side} only"),
            &mut out,
        );
    }
    for name in lite.indexes.keys() {
        match pg.indexes.get(name) {
            None => {
                out.insert(format!("index {name}: lite only"));
            }
            Some(pg_columns) => {
                let lite_columns = &lite.indexes[name];
                if lite_columns != pg_columns {
                    out.insert(format!(
                        "index {name} columns: lite {lite_columns:?} vs pg {pg_columns:?}"
                    ));
                }
            }
        }
    }
    for name in pg.indexes.keys() {
        if !lite.indexes.contains_key(name) {
            out.insert(format!("index {name}: pg only"));
        }
    }
    out
}

fn diff_members(
    lite: &BTreeSet<String>,
    pg: &BTreeSet<String>,
    key: impl Fn(&str, &str) -> String,
    out: &mut BTreeSet<String>,
) {
    for item in lite.difference(pg) {
        out.insert(key(item, "lite"));
    }
    for item in pg.difference(lite) {
        out.insert(key(item, "pg"));
    }
}

/// Extract the printable schema from one DDL file: tables (with columns,
/// inline UNIQUE flags and foreign keys) and indexes. Comments and string
/// literals never span a line in these files; `--` always starts a comment.
fn parse(sql: &str) -> Schema {
    let mut schema = Schema::default();
    let mut current: Option<(String, Table)> = None;
    let mut depth: i32 = 0;
    for raw in sql.lines() {
        let trimmed = strip_comment(raw).trim();
        if trimmed.is_empty() {
            continue;
        }
        if current.is_some() {
            if depth == 1 && !trimmed.starts_with(')') {
                let table = &mut current.as_mut().expect("checked above").1;
                parse_table_body_line(trimmed, table);
            }
            depth += paren_delta(trimmed);
            if depth <= 0 {
                let (name, table) = current.take().expect("checked above");
                schema.tables.insert(name, table);
            }
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("CREATE TABLE ") {
            let (name, tail) = split_ident(rest);
            let depth_after_header = paren_delta(&tail);
            let table = Table::default();
            if depth_after_header > 0 {
                depth = depth_after_header;
                current = Some((name, table));
            } else {
                // `CREATE TABLE x PARTITION OF y ...` (no body) or a
                // single-line table.
                schema.tables.insert(name, table);
            }
            continue;
        }
        let index = trimmed
            .strip_prefix("CREATE UNIQUE INDEX ")
            .or_else(|| trimmed.strip_prefix("CREATE INDEX "));
        if let Some(rest) = index
            && let Some((name, columns)) = parse_index(rest)
        {
            schema.indexes.insert(name, columns);
        }
    }
    schema
}

/// One line inside a `CREATE TABLE (...)` body: a column definition, a
/// table-level constraint, or a continuation line (`ON DELETE ...`).
fn parse_table_body_line(line: &str, table: &mut Table) {
    let (word, rest) = split_ident(line);
    let keyword = word.to_ascii_uppercase();
    if matches!(
        keyword.as_str(),
        "PRIMARY"
            | "FOREIGN"
            | "UNIQUE"
            | "CHECK"
            | "CONSTRAINT"
            | "ON"
            | "DEFERRABLE"
            | "INITIALLY"
    ) {
        if keyword == "FOREIGN"
            && let Some(foreign_key) = table_foreign_key(&rest)
        {
            table.foreign_keys.insert(foreign_key);
        }
        return;
    }
    if !table.columns.insert(word.clone()) {
        return;
    }
    if find_word(&rest, "UNIQUE").is_some() {
        table.unique_columns.insert(word.clone());
    }
    if let Some(target) = reference_target(&rest) {
        table.foreign_keys.insert(format!("{word} -> {target}"));
    }
}

fn table_foreign_key(rest: &str) -> Option<String> {
    let open = rest.find('(')?;
    let close = rest[open..].find(')')? + open;
    let columns = normalize_columns(&rest[open + 1..close]);
    let target = reference_target(&rest[close + 1..])?;
    Some(format!("{columns} -> {target}"))
}

fn reference_target(text: &str) -> Option<String> {
    let at = find_word(text, "REFERENCES")?;
    let after = text[at + "REFERENCES".len()..].trim_start();
    let open = after.find('(')?;
    let close = after[open..].find(')')? + open;
    let table = after[..open].trim();
    let columns = normalize_columns(&after[open + 1..close]);
    Some(format!("{table}({columns})"))
}

fn parse_index(rest: &str) -> Option<(String, Vec<String>)> {
    let (name, tail) = split_ident(rest);
    let open = tail.find('(')?;
    let close = tail[open..].find(')')? + open;
    let columns = tail[open + 1..close]
        .split(',')
        .map(|column| {
            let column = column.trim();
            let column = column
                .strip_suffix("DESC")
                .or_else(|| column.strip_suffix("ASC"))
                .unwrap_or(column);
            column.trim().to_ascii_lowercase()
        })
        .filter(|column| !column.is_empty())
        .collect();
    Some((name, columns))
}

// ── Text helpers ────────────────────────────────────────────────────────

fn strip_comment(line: &str) -> &str {
    match line.find("--") {
        Some(at) => &line[..at],
        None => line,
    }
}

fn paren_delta(line: &str) -> i32 {
    line.matches('(').count() as i32 - line.matches(')').count() as i32
}

/// `("CREATE TABLE", "namespaces (")`-style split on the first run of
/// whitespace.
fn split_ident(text: &str) -> (String, String) {
    let text = text.trim();
    match text.find(char::is_whitespace) {
        Some(at) => (text[..at].to_owned(), text[at..].trim().to_owned()),
        None => (text.to_owned(), String::new()),
    }
}

fn normalize_columns(columns: &str) -> String {
    columns
        .split(',')
        .map(str::trim)
        .collect::<Vec<_>>()
        .join(",")
}

/// Byte offset of `needle` in `haystack` when bounded by non-identifier
/// characters on both sides.
fn find_word(haystack: &str, needle: &str) -> Option<usize> {
    for (at, _) in haystack.match_indices(needle) {
        let before = haystack[..at].chars().next_back();
        let after = haystack[at + needle.len()..].chars().next();
        let is_boundary = |ch: Option<char>| !ch.is_some_and(|c| c.is_alphanumeric() || c == '_');
        if is_boundary(before) && is_boundary(after) {
            return Some(at);
        }
    }
    None
}

// ── Source grep for the reserved/unused inventory ───────────────────────

fn rust_sources() -> Vec<(PathBuf, String)> {
    fn walk(dir: &Path, out: &mut Vec<(PathBuf, String)>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs")
                && let Ok(text) = std::fs::read_to_string(&path)
            {
                out.push((path, text));
            }
        }
    }
    let mut out = Vec::new();
    walk(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src"), &mut out);
    out
}

/// Every mention of `table` (or `table.column` when `column` is set) inside a
/// SQL read/write statement. SQL is written across several Rust string lines,
/// so any statement verb within 240 bytes before the mention counts as the
/// same statement. `DELETE`/`DROP` are deliberately not verbs: the
/// `artifacts` table is delete-only by design, and deletes are not reads.
///
/// The window only looks backwards, so a mention that puts the table name
/// after the column (some `SELECT` shapes) can be missed — this guard is a
/// prompt, not a proof.
fn sql_mentions(text: &str, table: &str, column: Option<&str>) -> Vec<String> {
    const VERBS: &[&str] = &["SELECT", "INSERT", "UPDATE", "JOIN"];
    let needle = column.unwrap_or(table);
    let mut mentions = Vec::new();
    let mut from = 0;
    while let Some(at) = find_word(&text[from..], needle) {
        let at = from + at;
        from = at + 1;
        let start = char_window_start(text, at, 240);
        let window = &text[start..at];
        if !VERBS.iter().any(|verb| find_word(window, verb).is_some()) {
            continue;
        }
        if column.is_some() && find_word(window, table).is_none() {
            continue;
        }
        let line_start = text[..at].rfind('\n').map_or(0, |newline| newline + 1);
        let line_end = text[at..].find('\n').map_or(text.len(), |end| at + end);
        mentions.push(text[line_start..line_end].trim().to_owned());
    }
    mentions
}

/// Start of the `max_chars`-long window that ends at `at`, on a char
/// boundary.
fn char_window_start(text: &str, at: usize, max_chars: usize) -> usize {
    text[..at]
        .char_indices()
        .rev()
        .nth(max_chars)
        .map_or(0, |(index, _)| index)
}
