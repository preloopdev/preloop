//! Render snapshots.
//!
//! Each test drives the real update path with fixture data, renders the screen
//! into a 120x32 buffer (the approved mockup size), and compares the text with
//! a golden file under `tests/snapshots/`. Each screen also gets a second golden
//! that maps every cell to its colour class, so colour regressions show up in
//! review as well.
//!
//! `UPDATE_SNAPSHOTS=1 cargo test -p preloop-tui` rewrites the goldens.

use std::path::PathBuf;

use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::buffer::Buffer;
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::style::Color;

use crate::api::{
    DebugSessions, LiveLogChunk, OperationalSnapshot, PoolBindings, RunRecord, WebhookDeliveries,
    WebhookHealth,
};
use crate::app::{App, ContextInfo, Fetched, Msg};
use crate::client::sandbox::{ProviderRow, SandboxSummary};
use crate::options::StartView;
use crate::runtime::render;
use crate::theme::{ACCENT, BORDER, ERROR, FG, MUTED, SELECTION, SUCCESS, Theme, WARNING};
use crate::views::ViewKind;

const WIDTH: u16 = 120;
const HEIGHT: u16 = 32;

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 22, 12, 20, 0)
        .single()
        .expect("valid time")
}

fn fixture(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("reading {}: {error}", path.display()))
}

fn parse<T: serde::de::DeserializeOwned>(name: &str) -> T {
    serde_json::from_str(&fixture(name)).unwrap_or_else(|error| panic!("parsing {name}: {error}"))
}

fn key(code: KeyCode) -> Msg {
    Msg::Key(KeyEvent::new(code, KeyModifiers::NONE))
}

/// An app with every slot the overview reads, loaded from fixtures.
fn loaded_app() -> App {
    let mut app = App::new(
        vec![ContextInfo {
            name: "cpane".into(),
            engine_url: "http://127.0.0.1:9090".into(),
            sandbox: true,
        }],
        0,
        StartView::Overview,
        false,
        Theme::color(),
        now(),
    );
    app.startup();

    // Two snapshots five minutes apart, so the leaked-binding window reads 4m12s.
    let earlier: OperationalSnapshot = parse("status_earlier.json");
    let status: OperationalSnapshot = parse("status.json");
    app.update(Msg::Fetched(Fetched::Status(Ok(earlier))));
    app.update(Msg::Fetched(Fetched::Status(Ok(status))));

    // Ready-queue history for the sparkline, as the mockup shows it.
    for (index, value) in [0, 0, 1, 2, 4, 2, 1, 0, 0, 1, 4, 1].into_iter().enumerate() {
        let at = now() - TimeDelta::seconds(5 * (12 - index as i64));
        app.store.push_ready(at, value);
    }

    let deliveries: WebhookDeliveries = parse("webhook_deliveries.json");
    app.update(Msg::Fetched(Fetched::Webhooks(Ok(deliveries))));
    let health: WebhookHealth = parse("webhook_health.json");
    app.update(Msg::Fetched(Fetched::WebhookHealth(Ok(health))));
    let sessions: DebugSessions = parse("debug_sessions.json");
    app.update(Msg::Fetched(Fetched::Debug(Ok(sessions.sessions))));
    let bindings: PoolBindings = parse("pool_bindings.json");
    app.update(Msg::Fetched(Fetched::Bindings(Ok(bindings))));
    app.update(Msg::Fetched(Fetched::Sandbox(Ok(SandboxSummary {
        providers: vec![
            ProviderRow {
                name: "cpane".into(),
                cordoned: false,
            },
            ProviderRow {
                name: "macstudio".into(),
                cordoned: true,
            },
        ],
        sandboxes: 2,
        host_mem: None,
    }))));
    let recent_runs: Vec<RunRecord> = parse("runs.json");
    app.update(Msg::Fetched(Fetched::Runs(Ok(recent_runs))));
    app
}

/// The runs screen with the mockup's filter, showing failed run #412.
fn runs_app() -> App {
    let mut app = loaded_app();
    app.update(key(KeyCode::Char('2')));
    app.runs.filter = crate::widgets::text_input::TextInput::new("status:failure,running  workflow:ci");
    let runs: Vec<RunRecord> = parse("runs.json");
    let failed = runs
        .iter()
        .find(|run| run.run_number == 412)
        .expect("fixture has #412")
        .clone();
    app.update(Msg::Fetched(Fetched::Runs(Ok(runs))));
    app.update(Msg::Fetched(Fetched::RunDetail {
        run_id: failed.run_id.clone(),
        result: Box::new(Ok(failed.clone())),
    }));
    let chunks: Vec<LiveLogChunk> = parse("live_8f3a.json");
    for chunk in chunks {
        app.update(Msg::LiveLines {
            run_id: failed.run_id.clone(),
            job_id: "test (sandbox)".into(),
            chunk,
        });
    }
    app
}

/// The runs screen with in-progress run #411 selected: a running step whose
/// clock ticks and whose lines arrive live.
fn runs_live_app() -> App {
    let mut app = runs_app();
    let runs: Vec<RunRecord> = parse("runs.json");
    let live = runs
        .iter()
        .find(|run| run.run_number == 411)
        .expect("fixture has #411")
        .clone();
    app.update(Msg::Fetched(Fetched::RunDetail {
        run_id: live.run_id.clone(),
        result: Box::new(Ok(live.clone())),
    }));
    app.update(key(KeyCode::Down));
    let chunks: Vec<LiveLogChunk> = parse("live_c01d.json");
    for chunk in chunks {
        app.update(Msg::LiveLines {
            run_id: live.run_id.clone(),
            job_id: "test (runner)".into(),
            chunk,
        });
    }
    app
}

fn plain_text(buffer: &Buffer) -> String {
    let mut out = String::new();
    for y in 0..buffer.area.height {
        let row: String = (0..buffer.area.width)
            .map(|x| buffer[(x, y)].symbol().to_owned())
            .collect();
        out.push_str(row.trim_end());
        out.push('\n');
    }
    out
}

/// One letter per cell: `f` body text, `m` muted, `a` accent, `s` success,
/// `w` warning, `e` error, `b` border, `k` selection background, `.` no colour.
fn class_of(cell_fg: Color, cell_bg: Color) -> char {
    if cell_bg == SELECTION {
        return 'k';
    }
    match cell_fg {
        Color::Reset => '.',
        color if color == FG => 'f',
        color if color == MUTED => 'm',
        color if color == ACCENT => 'a',
        color if color == SUCCESS => 's',
        color if color == WARNING => 'w',
        color if color == ERROR => 'e',
        color if color == BORDER => 'b',
        _ => '?',
    }
}

fn style_text(buffer: &Buffer) -> String {
    let mut out = String::new();
    for y in 0..buffer.area.height {
        for x in 0..buffer.area.width {
            let cell = &buffer[(x, y)];
            out.push(class_of(cell.fg, cell.bg));
        }
        out.push('\n');
    }
    out
}

fn render_screen(app: &App) -> (String, String) {
    let mut terminal = Terminal::new(TestBackend::new(WIDTH, HEIGHT)).expect("test terminal");
    terminal
        .draw(|frame| render(app, frame))
        .expect("draw succeeds");
    let buffer = terminal.backend().buffer().clone();
    (plain_text(&buffer), style_text(&buffer))
}

fn golden_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/snapshots")
        .join(format!("{name}.txt"))
}

fn check(name: &str, actual: &str) {
    let path = golden_path(name);
    if std::env::var_os("UPDATE_SNAPSHOTS").is_some() {
        std::fs::create_dir_all(path.parent().expect("has parent")).expect("create snapshots dir");
        std::fs::write(&path, actual).expect("write golden");
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!("missing golden {} — run with UPDATE_SNAPSHOTS=1", path.display())
    });
    if expected != actual {
        let mut diff = String::new();
        for (index, (want, got)) in expected.lines().zip(actual.lines()).enumerate() {
            if want != got {
                diff.push_str(&format!(
                    "line {}:\n  want {want:?}\n  got  {got:?}\n",
                    index + 1
                ));
            }
        }
        panic!("snapshot {name} differs from {}:\n{diff}", path.display());
    }
}

#[test]
fn overview_matches_golden() {
    let app = loaded_app();
    assert_eq!(app.view, ViewKind::Overview);
    let (plain, styles) = render_screen(&app);
    check("overview", &plain);
    check("overview.styles", &styles);
}

#[test]
fn runs_matches_golden() {
    let app = runs_app();
    let (plain, styles) = render_screen(&app);
    check("runs", &plain);
    check("runs.styles", &styles);
}

#[test]
fn runs_live_matches_golden() {
    let app = runs_live_app();
    let (plain, styles) = render_screen(&app);
    check("runs_live", &plain);
    check("runs_live.styles", &styles);
}

#[test]
fn engine_down_banner_names_the_setup_keys() {
    let mut app = loaded_app();
    app.update(Msg::Fetched(Fetched::Status(Err(
        "connection refused".into(),
    ))));
    let (plain, _) = render_screen(&app);
    assert!(
        plain.contains("no engine at http://127.0.0.1:9090 — s: setup wizard, S: start engine"),
        "{plain}"
    );
}
