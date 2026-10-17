//! `PRELOOP_STORE_URL` selection tests. The variable is exercised through a
//! child process (`Command::env`/`env_remove`), never by mutating this
//! process's environment: the child shares a temp directory with the parent
//! through [`DIR_ENV`], and the parent asserts on the database files it left
//! behind. They live in their own test binary so a sibling test opening a
//! store cannot observe the child's temporary database.

mod common;

use common::*;

/// Marks the child process spawned by [`rerun`].
const CHILD_ENV: &str = "PRELOOP_TEST_CHILD";
/// The temp directory shared between the parent and its child.
const DIR_ENV: &str = "PRELOOP_TEST_SHARED_DIR";

/// In the spawned child: the shared directory the parent created.
fn child_dir() -> Option<std::path::PathBuf> {
    std::env::var_os(CHILD_ENV)?;
    Some(std::path::PathBuf::from(std::env::var_os(DIR_ENV)?))
}

/// Re-run this test binary's `test_name` in a child process with the given
/// environment applied (`None` removes the variable). The child shares `dir`
/// through [`DIR_ENV`] and takes the `child_dir()` branch of the same test.
fn rerun(test_name: &str, dir: &std::path::Path, vars: &[(&str, Option<String>)]) {
    let mut cmd = std::process::Command::new(std::env::current_exe().expect("current test binary"));
    cmd.args([test_name, "--exact"]);
    cmd.env(CHILD_ENV, "1");
    cmd.env(DIR_ENV, dir);
    for (name, value) in vars {
        match value {
            Some(value) => {
                cmd.env(name, value);
            }
            None => {
                cmd.env_remove(name);
            }
        }
    }
    let status = cmd.status().expect("spawn child test");
    assert!(status.success(), "child run of {test_name} failed");
}

/// `PRELOOP_STORE_URL` selects the control backend when no explicit URL is
/// given — the documented `preloop engine`/systemd configuration, which has no
/// `--store` flag at all. Regression: the cutover dropped the env fallback, so
/// the engine silently ran on local SQLite while `/api/v1/status` still
/// reported `postgres` (the label path kept reading the env).
#[tokio::test]
async fn store_url_env_selects_the_control_backend() {
    if let Some(dir) = child_dir() {
        let state_dir = dir.join("state");
        let backend = crate::state::test_open_backend(None, &state_dir)
            .await
            .expect("env-selected backend must open");
        assert_eq!(backend, "sqlite");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let env_db = dir.path().join("from-env.db");
    let state_dir = dir.path().join("state");
    fs::create_dir_all(&state_dir).unwrap();
    rerun(
        "store_url_env_selects_the_control_backend",
        dir.path(),
        &[(
            crate::store::STORE_URL_ENV,
            Some(format!("sqlite://{}", env_db.display())),
        )],
    );
    assert!(
        env_db.exists(),
        "PRELOOP_STORE_URL must select the database"
    );
    assert!(
        !state_dir.join("preloop.db").exists(),
        "the state-dir default must not be opened when the env selects a database"
    );
}

/// Regression: test builds never let the ambient `PRELOOP_STORE_URL` reach
/// [`AppState`]. `AppState::new_for_tests` pins `None` to the state-dir
/// default in test builds; production keeps the env fallback, which
/// `test_open_backend` still exercises above.
#[tokio::test]
async fn app_state_ignores_ambient_store_url_in_test_builds() {
    if let Some(dir) = child_dir() {
        let state_dir = dir.join("state");
        AppState::new_for_tests(state_dir).await.unwrap();
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let env_db = dir.path().join("from-env.db");
    let state_dir = dir.path().join("state");
    fs::create_dir_all(&state_dir).unwrap();
    rerun(
        "app_state_ignores_ambient_store_url_in_test_builds",
        dir.path(),
        &[(
            crate::store::STORE_URL_ENV,
            Some(format!("sqlite://{}", env_db.display())),
        )],
    );
    assert!(
        state_dir.join("preloop.db").exists(),
        "a test-build AppState must use <state_dir>/preloop.db"
    );
    assert!(
        !env_db.exists(),
        "a test-build AppState must not consult the ambient PRELOOP_STORE_URL"
    );
}

/// Explicit URL wins over the environment — the precedence the merge base had.
#[tokio::test]
async fn explicit_store_url_wins_over_env() {
    if let Some(dir) = child_dir() {
        let state_dir = dir.join("state");
        let backend = crate::state::test_open_backend(
            Some(&format!("sqlite://{}", dir.join("explicit.db").display())),
            &state_dir,
        )
        .await
        .unwrap();
        assert_eq!(backend, "sqlite");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let env_db = dir.path().join("from-env.db");
    let explicit_db = dir.path().join("explicit.db");
    let state_dir = dir.path().join("state");
    fs::create_dir_all(&state_dir).unwrap();
    rerun(
        "explicit_store_url_wins_over_env",
        dir.path(),
        &[(
            crate::store::STORE_URL_ENV,
            Some(format!("sqlite://{}", env_db.display())),
        )],
    );
    assert!(explicit_db.exists());
    assert!(!env_db.exists());
    assert!(!state_dir.join("preloop.db").exists());
}

/// `Backend::open` must parse store URLs with the same grammar as the status
/// label: `sqlite:` (single slash) is valid, a whitespace-only value means
/// "unset", and an unknown scheme is rejected outright rather than becoming a
/// bogus relative path.
#[tokio::test]
async fn store_url_parsing_matches_the_label_path() {
    if let Some(dir) = child_dir() {
        // The whitespace case falls back to `PRELOOP_STORE_URL`, which the
        // child has removed (an explicitly unset variable).
        let ws_state = dir.join("ws-state");
        let backend = crate::state::test_open_backend(Some("   "), &ws_state)
            .await
            .unwrap();
        assert_eq!(backend, "sqlite");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    fs::create_dir_all(&state_dir).unwrap();

    // `sqlite:` single-slash form (the merge base accepted it).
    let single = dir.path().join("single.db");
    let backend =
        crate::state::test_open_backend(Some(&format!("sqlite:{}", single.display())), &state_dir)
            .await
            .expect("sqlite: single-slash form must be accepted");
    assert_eq!(backend, "sqlite");
    assert!(single.exists());

    // A whitespace-only value is "unset": the state-dir default is opened, and
    // no database file literally named " " appears in the working directory.
    std::fs::create_dir_all(dir.path().join("ws-state")).unwrap();
    rerun(
        "store_url_parsing_matches_the_label_path",
        dir.path(),
        &[(crate::store::STORE_URL_ENV, None)],
    );
    assert!(
        dir.path().join("ws-state/preloop.db").exists(),
        "a whitespace-only URL must fall back to <state_dir>/preloop.db"
    );
    assert!(
        !std::path::Path::new(" ").exists(),
        "a whitespace-only URL must not create a file named ' '"
    );

    // Unknown scheme: explicit rejection, not an I/O error on a relative path.
    let error = match crate::state::test_open_backend(Some("mysql://host/db"), &state_dir).await {
        Ok(_) => panic!("unknown scheme must be rejected"),
        Err(error) => error,
    };
    assert!(
        error.contains("unsupported store URL"),
        "unexpected error: {error}"
    );
}
