//! Backend-neutral command helpers: runner-protocol file commands and
//! shared parse/format utilities used by callers outside `control/`. The
//! per-command transaction bodies moved into `control/lite` and
//! `control/pg` (one short SQL transaction each).

fn system_to_us(t: std::time::SystemTime) -> i64 {
    t.duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

fn parse_uuid(s: &str) -> uuid::Uuid {
    super::logic::session_uuid(s)
}
