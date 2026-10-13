// SAFETY: edition-2024 env mutation; each test serializes via GITHUB_ENV_LOCK.
#![allow(unsafe_code)]

//! `PRELOOP_STORE_URL` selection tests. They mutate the process environment,
//! and every `AppState::new` falls back to that variable, so they live in
//! their own test binary: a sibling test opening a store mid-mutation would
//! pick up this test's temporary database and fail once it is deleted.

mod common;

use common::*;

/// `PRELOOP_STORE_URL` selects the control backend when no explicit URL is
/// given — the documented `preloop engine`/systemd configuration, which has no
/// `--store` flag at all. Regression: the cutover dropped the env fallback, so
/// the engine silently ran on local SQLite while `/api/v1/status` still
/// reported `postgres` (the label path kept reading the env).
#[tokio::test]
async fn store_url_env_selects_the_control_backend() {
    let _guard = crate::state::GITHUB_ENV_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let env_db = dir.path().join("from-env.db");
    let _env = crate::state::TestEnvVar::set(
        crate::store::STORE_URL_ENV,
        format!("sqlite://{}", env_db.display()),
    );
    let state_dir = dir.path().join("state");
    fs::create_dir_all(&state_dir).unwrap();
    let backend = crate::state::test_open_backend(None, &state_dir)
        .await
        .expect("env-selected backend must open");
    assert_eq!(backend, "sqlite");
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
/// [`AppState`]. The env-mutating tests above serialize on
/// `GITHUB_ENV_LOCK`, but every other test in this binary opens an `AppState`
/// while that variable may point at a sibling's temp database — deleted with
/// the tempdir or still live as a foreign store. `AppState::new_with_store`
/// pins `None` to the state-dir default in test builds; production keeps the
/// env fallback, which `test_open_backend` still exercises.
#[tokio::test]
async fn app_state_ignores_ambient_store_url_in_test_builds() {
    let _guard = crate::state::GITHUB_ENV_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let env_db = dir.path().join("from-env.db");
    let _env = crate::state::TestEnvVar::set(
        crate::store::STORE_URL_ENV,
        format!("sqlite://{}", env_db.display()),
    );
    let state_dir = dir.path().join("state");
    fs::create_dir_all(&state_dir).unwrap();
    AppState::new(state_dir.clone()).await.unwrap();
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
    let _guard = crate::state::GITHUB_ENV_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let env_db = dir.path().join("from-env.db");
    let explicit_db = dir.path().join("explicit.db");
    let _env = crate::state::TestEnvVar::set(
        crate::store::STORE_URL_ENV,
        format!("sqlite://{}", env_db.display()),
    );
    let state_dir = dir.path().join("state");
    fs::create_dir_all(&state_dir).unwrap();
    let backend = crate::state::test_open_backend(
        Some(&format!("sqlite://{}", explicit_db.display())),
        &state_dir,
    )
    .await
    .unwrap();
    assert_eq!(backend, "sqlite");
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
    // The whitespace case falls back to `PRELOOP_STORE_URL`, so this test has
    // to serialize with the other env-mutating tests and pin the variable
    // unset — otherwise a sibling's temp database leaks in and is gone.
    let _guard = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _env = crate::state::TestEnvVar::unset(crate::store::STORE_URL_ENV);
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
    let ws_dir = tempfile::tempdir().unwrap();
    let ws_state = ws_dir.path().join("state");
    fs::create_dir_all(&ws_state).unwrap();
    let backend = crate::state::test_open_backend(Some("   "), &ws_state)
        .await
        .unwrap();
    assert_eq!(backend, "sqlite");
    assert!(
        ws_state.join("preloop.db").exists(),
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
