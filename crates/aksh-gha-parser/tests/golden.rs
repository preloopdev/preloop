//! Golden expansion fixtures.
//!
//! `fixtures/golden/<name>.yml` is a workflow; `<name>.json` is the expected
//! serialized `Vec<JobPlan>` from `expand_jobs`.
//!
//! This lived only in the `aksh-conformance` `golden` subcommand, which is a
//! CLI entry point and therefore never ran in CI — the fixtures had silently
//! drifted by three fields before anyone noticed. Keeping the comparison here
//! as an ordinary test means a wire-shape change to `JobPlan` fails the gate.
//!
//! Regenerate after an intentional shape change:
//!
//! ```sh
//! UPDATE_GOLDEN=1 cargo test -p aksh-gha-parser --test golden
//! ```
//!
//! and review the resulting diff, exactly as `REVIEW.md` §7 requires.

use aksh_gha_parser::{expand_jobs, parse_workflow};
use std::path::{Path, PathBuf};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/golden")
}

/// Fixture stems (`<name>.yml` with a matching `<name>.json`), sorted so the
/// failure output is stable.
fn fixture_stems(dir: &Path) -> Vec<String> {
    let mut stems: Vec<String> = std::fs::read_dir(dir)
        .expect("fixtures/golden is readable")
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            let ext = path.extension()?.to_str()?;
            if ext != "yml" && ext != "yaml" {
                return None;
            }
            path.with_extension("json")
                .exists()
                .then(|| path.file_stem()?.to_str().map(str::to_owned))?
        })
        .collect();
    stems.sort();
    stems
}

/// Expansion output as a `Value`, matching what the conformance CLI compares.
fn expanded(yaml: &str) -> serde_json::Value {
    let workflow = parse_workflow(yaml).expect("fixture workflow parses");
    let plans = expand_jobs(&workflow).expect("fixture workflow expands");
    serde_json::to_value(&plans).expect("plans serialize")
}

#[test]
fn golden_expansions_match_fixtures() {
    let dir = fixtures_dir();
    let stems = fixture_stems(&dir);
    assert!(!stems.is_empty(), "no golden fixtures found in {dir:?}");

    let update = std::env::var_os("UPDATE_GOLDEN").is_some();
    let mut mismatched = Vec::new();

    for stem in &stems {
        let yaml = std::fs::read_to_string(dir.join(format!("{stem}.yml")))
            .expect("fixture yaml is readable");
        let json_path = dir.join(format!("{stem}.json"));
        let actual = expanded(&yaml);

        if update {
            let mut rendered =
                serde_json::to_string_pretty(&actual).expect("expansion re-serializes");
            rendered.push('\n');
            std::fs::write(&json_path, rendered).expect("fixture is writable");
            continue;
        }

        let expected: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&json_path).expect("fixture readable"))
                .expect("fixture json parses");
        if expected != actual {
            mismatched.push(stem.clone());
        }
    }

    assert!(
        mismatched.is_empty(),
        "golden expansion drift in {mismatched:?}; \
         if the new shape is intended, regenerate with \
         `UPDATE_GOLDEN=1 cargo test -p aksh-gha-parser --test golden` and review the diff"
    );
}
