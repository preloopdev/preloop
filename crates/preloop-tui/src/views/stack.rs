//! Stack screen: configured repositories and their pull-request topology.
//!
//! GitHub reads happen in the runtime. This module is deliberately a pure
//! renderer over the loaded [`StackSnapshot`](crate::api::StackSnapshot).

use std::collections::HashSet;

use ratatui::Frame;
use ratatui::crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use super::View;
use crate::actions::Action;
use crate::api::{RunRecord, StackPullRequest, StackRepository, StackSnapshot};
use crate::app::{App, Effect};
use crate::config::StackConfig;
use crate::theme::{Theme, Tone};

/// Stack selection state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct State {
    /// Index into the flattened PR rows.
    pub cursor: usize,
}

impl State {
    /// Keep the selection valid after a refresh.
    pub fn clamp(&mut self, snapshot: Option<&StackSnapshot>, config: &StackConfig) {
        let count = snapshot
            .map(|snapshot| {
                snapshot
                    .repositories
                    .iter()
                    .map(|repo| repo.pull_requests.len())
                    .sum()
            })
            .unwrap_or_else(|| config.repos.len());
        self.cursor = self.cursor.min(count.saturating_sub(1));
    }
}

#[derive(Debug, Clone)]
struct TreeRow {
    repo: String,
    branch: Option<String>,
    pull_request: Option<StackPullRequest>,
    depth: usize,
    mirror: bool,
}

/// The Stack tab.
pub struct Stack;

impl View for Stack {
    fn render(&self, app: &App, frame: &mut Frame, area: Rect) {
        let theme = app.theme;
        let columns = Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(area);
        render_tree(app, frame, columns[0], theme);
        render_detail(app, frame, columns[1], theme);
    }

    fn on_key(&self, app: &mut App, key: KeyEvent) -> Vec<Effect> {
        let rows = tree_rows(app);
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                app.stack.cursor = app.stack.cursor.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let count = rows.iter().filter(|row| row.pull_request.is_some()).count();
                if count > 0 {
                    app.stack.cursor = (app.stack.cursor + 1).min(count - 1);
                }
            }
            KeyCode::Enter => {
                if let Some(run_id) = selected_run(app) {
                    app.view = super::ViewKind::Runs;
                    app.runs.run = Some(run_id);
                    return app.after_selection_change();
                }
                app.notify("selected pull request has no linked preloop run");
            }
            KeyCode::Char('o') => {
                if let Some(pull_request) = selected_pr(app) {
                    if pull_request.url.starts_with("https://") {
                        return app.run_action(Action::OpenUrl {
                            url: pull_request.url.clone(),
                        });
                    }
                    app.notify("PR URL not loaded");
                } else {
                    app.notify("no pull request selected");
                }
            }
            KeyCode::Char('w') => {
                app.notify("worktree checkout disabled: no checkout hook is configured")
            }
            KeyCode::Char('R') => {
                if let Some(run_id) = selected_run(app) {
                    return app.run_action(Action::RerunRun { run_id });
                }
                app.notify("no linked preloop run to rerun");
            }
            KeyCode::Char('m') => {
                app.notify("restack hint: rebase this PR onto its displayed base branch")
            }
            _ => {}
        }
        Vec::new()
    }

    fn hints(&self, _app: &App) -> Vec<String> {
        [
            "↑/↓ select",
            "enter checks",
            "o open PR",
            "w worktree (disabled)",
            "R rerun linked run",
            "m restack hint",
            "? help",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }
}

fn render_tree(app: &App, frame: &mut Frame, area: Rect, theme: Theme) {
    let title = if app.stack_config.repos.is_empty() {
        "Sandbox stack · not configured"
    } else {
        "Sandbox stack"
    };
    let block = Block::default()
        .title(format!(" {title} "))
        .borders(Borders::ALL)
        .border_style(theme.border(true));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if app.stack_config.repos.is_empty() {
        frame.render_widget(
            Paragraph::new("no repositories configured").style(theme.muted()),
            inner,
        );
        return;
    }
    let rows = tree_rows(app);
    if rows.is_empty() {
        let text = if let Some(error) = app.store.stack.error.as_deref() {
            format!("stack data not loaded: {error}")
        } else if app.store.stack.loading {
            "stack data not loaded (loading GitHub)".to_owned()
        } else {
            "configured repositories: no pull requests loaded".to_owned()
        };
        frame.render_widget(
            Paragraph::new(text)
                .style(theme.muted())
                .wrap(Wrap { trim: true }),
            inner,
        );
        return;
    }
    let mut pr_index = 0usize;
    let lines = rows
        .iter()
        .map(|row| {
            let selected = row.pull_request.as_ref().is_some_and(|_| {
                let current = pr_index;
                pr_index += 1;
                current == app.stack.cursor
            });
            let mut spans = Vec::new();
            if let Some(pr) = &row.pull_request {
                let prefix = if row.mirror { "⎇ " } else { "┼ " };
                spans.push(Span::styled(
                    format!("{}{}#{} ", "  ".repeat(row.depth), prefix, pr.number),
                    theme.muted(),
                ));
                spans.push(Span::styled(pr.title.clone(), theme.text()));
                spans.push(Span::raw(" "));
                spans.push(Span::styled(
                    pr.checks.glyph(),
                    check_tone(&pr.checks, theme),
                ));
                if pr.draft {
                    spans.push(Span::styled(" draft", theme.tone(Tone::Warning)));
                }
            } else if let Some(branch) = &row.branch {
                spans.push(Span::styled(
                    format!("{}{}", "  ".repeat(row.depth), branch),
                    theme.accent(),
                ));
            } else {
                spans.push(Span::styled(row.repo.clone(), theme.accent()));
            }
            let line = Line::from(spans);
            if selected {
                line.patch_style(theme.selected())
            } else {
                line
            }
        })
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(lines), inner);
}

fn render_detail(app: &App, frame: &mut Frame, area: Rect, theme: Theme) {
    let block = Block::default()
        .title(" Pull request ")
        .borders(Borders::ALL)
        .border_style(theme.border(false));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let Some(pr) = selected_pr(app) else {
        let text = if app.stack_config.repos.is_empty() {
            "stack is not configured"
        } else if app.store.stack.loading {
            "pull request data not loaded"
        } else {
            "select a loaded pull request"
        };
        frame.render_widget(Paragraph::new(text).style(theme.muted()), inner);
        return;
    };
    let state = if pr.state.is_empty() {
        "not loaded".to_owned()
    } else {
        pr.state.to_ascii_lowercase()
    };
    let mergeable = pr.mergeable.as_deref().unwrap_or("not loaded");
    let checks = format!("{} {}", pr.checks.glyph(), pr.checks.label());
    let run = selected_run(app).unwrap_or_else(|| "not loaded".to_owned());
    let lines = vec![
        Line::from(Span::styled(
            format!("#{} {}", pr.number, pr.title),
            theme.accent(),
        )),
        Line::from(format!("repo       {}", pr.repository)),
        Line::from(format!(
            "state      {}{}",
            state,
            if pr.draft { " · draft" } else { "" }
        )),
        Line::from(format!("mergeable  {}", mergeable.to_ascii_lowercase())),
        Line::from(format!("base ← head {} ← {}", pr.base, pr.head)),
        Line::from(format!(
            "base sha   {}",
            pr.base_sha.as_deref().unwrap_or("not loaded")
        )),
        Line::from(format!(
            "head sha   {}",
            pr.head_sha.as_deref().unwrap_or("not loaded")
        )),
        Line::from(format!("checks     {checks}")),
        Line::from(format!("preloop    {run}")),
        Line::from(format!(
            "url        {}",
            if pr.url.is_empty() {
                "not loaded"
            } else {
                &pr.url
            }
        )),
    ];
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: true }), inner);
}

fn check_tone(checks: &crate::api::CheckSummary, theme: Theme) -> ratatui::style::Style {
    if checks.failing > 0 {
        theme.tone(Tone::Error)
    } else if checks.pending > 0 {
        theme.tone(Tone::Warning)
    } else if checks.total > 0 && checks.passing == checks.total {
        theme.tone(Tone::Success)
    } else {
        theme.muted()
    }
}

fn tree_rows(app: &App) -> Vec<TreeRow> {
    let Some(snapshot) = app.store.stack.get() else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for repo in &snapshot.repositories {
        rows.push(TreeRow {
            repo: repo.name.clone(),
            branch: None,
            pull_request: None,
            depth: 0,
            mirror: false,
        });
        if let Some(error) = &repo.error {
            rows.push(TreeRow {
                repo: format!("  not loaded: {error}"),
                branch: None,
                pull_request: None,
                depth: 1,
                mirror: false,
            });
            continue;
        }
        let branch = repo.default_branch.as_deref().unwrap_or("not loaded");
        rows.push(TreeRow {
            repo: repo.name.clone(),
            branch: Some(branch.to_owned()),
            pull_request: None,
            depth: 1,
            mirror: false,
        });
        let mut visited = HashSet::new();
        for pr in &repo.pull_requests {
            if !repo
                .pull_requests
                .iter()
                .any(|parent| parent.head == pr.base)
            {
                append_pr(&mut rows, repo, pr, 2, false, &mut visited, app);
            }
        }
        for pr in &repo.pull_requests {
            append_pr(&mut rows, repo, pr, 2, false, &mut visited, app);
        }
    }
    rows
}

fn append_pr(
    rows: &mut Vec<TreeRow>,
    repo: &StackRepository,
    pr: &StackPullRequest,
    depth: usize,
    mirror: bool,
    visited: &mut HashSet<String>,
    app: &App,
) {
    let key = pr.reference();
    if !visited.insert(key) {
        return;
    }
    rows.push(TreeRow {
        repo: repo.name.clone(),
        branch: None,
        pull_request: Some(pr.clone()),
        depth,
        mirror,
    });
    for child in repo
        .pull_requests
        .iter()
        .filter(|child| child.base == pr.head)
    {
        append_pr(rows, repo, child, depth + 1, false, visited, app);
    }
    for (parent, child) in &app.stack_config.extra_edges {
        if pr.matches_reference(parent) {
            for other_repo in app
                .store
                .stack
                .get()
                .into_iter()
                .flat_map(|s| s.repositories.iter())
            {
                for child_pr in &other_repo.pull_requests {
                    if child_pr.matches_reference(child) {
                        append_pr(rows, other_repo, child_pr, depth + 1, true, visited, app);
                    }
                }
            }
        }
    }
}

fn selected_pr(app: &App) -> Option<StackPullRequest> {
    tree_rows(app)
        .into_iter()
        .filter_map(|row| row.pull_request)
        .nth(app.stack.cursor)
}

fn selected_run(app: &App) -> Option<String> {
    let pr = selected_pr(app)?;
    app.store
        .runs
        .get()
        .into_iter()
        .flatten()
        .chain(app.store.details.values().filter_map(|slot| slot.get()))
        .find(|run| run_matches_pr(run, &pr))
        .map(|run| run.run_id.clone())
}

fn run_matches_pr(run: &RunRecord, pr: &StackPullRequest) -> bool {
    if !run
        .submission
        .repository
        .eq_ignore_ascii_case(&pr.repository)
    {
        return false;
    }
    run.submission.git_ref == format!("refs/pull/{}/merge", pr.number)
        || run.submission.git_ref == format!("refs/pull/{}/head", pr.number)
        || run
            .submission
            .payload
            .get("number")
            .and_then(serde_json::Value::as_u64)
            == Some(pr.number)
        || run
            .submission
            .payload
            .get("pull_request")
            .and_then(|value| value.get("number"))
            .and_then(serde_json::Value::as_u64)
            == Some(pr.number)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::StackSnapshot;
    use crate::app::{App, ContextInfo};
    use crate::config::StackConfig;
    use crate::options::StartView;
    use chrono::Utc;
    use ratatui::{Terminal, backend::TestBackend, buffer::Buffer};

    fn app() -> App {
        let mut app = App::new(
            vec![ContextInfo {
                name: "test".into(),
                engine_url: "http://localhost".into(),
                sandbox: false,
            }],
            0,
            StartView::Overview,
            false,
            Theme::monochrome(),
            Utc::now(),
        );
        app.set_stack_config(StackConfig {
            repos: vec!["acme/widgets".into()],
            author: None,
            extra_edges: vec![],
        });
        let value: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/stack_github.json")).unwrap();
        let repo = crate::client::github::parse_repository(&value, "acme/widgets").unwrap();
        app.store.stack.succeed(
            StackSnapshot {
                repositories: vec![repo],
            },
            app.now,
        );
        app
    }

    #[test]
    fn render_contains_loaded_pr_and_check_summary() {
        let app = app();
        let mut terminal = Terminal::new(TestBackend::new(100, 24)).unwrap();
        terminal
            .draw(|frame| Stack.render(&app, frame, frame.area()))
            .unwrap();
        let buffer: &Buffer = terminal.backend().buffer();
        let text = buffer
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(text.contains("#42"));
        assert!(text.contains("checks"));
        assert!(text.contains("not loaded"));
    }
}
