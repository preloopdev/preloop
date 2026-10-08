//! Schema-drift guard between the two copies of the control schema.
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
    // SQLite's own DDL dump double-quotes identifiers after `ALTER TABLE ..
    // RENAME`; none of these files use double quotes for anything else.
    let sql = sql.replace('"', "");
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
                for part in split_top_level(trimmed) {
                    parse_table_body_line(part, table);
                }
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

/// Split a table-body line at its top-level commas, so
/// `a INTEGER, b TEXT,` yields `a INTEGER` and `b TEXT` while a comma inside
/// parentheses (`CHECK (x IN ('a','b'))`, `FOREIGN KEY (a, b)`) stays put.
/// SQLite's `ALTER TABLE .. ADD COLUMN` lists appended columns on one line.
fn split_top_level(line: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    for (at, ch) in line.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&line[start..at]);
                start = at + 1;
            }
            _ => {}
        }
    }
    parts.push(&line[start..]);
    parts
        .into_iter()
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect()
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
