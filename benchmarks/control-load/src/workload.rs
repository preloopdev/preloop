//! Realistic submissions: a weighted workflow mix, many repositories, sent
//! through the REST API and as signed GitHub push webhooks.

use hmac::{Hmac, Mac};
use rand::distributions::{Distribution, WeightedIndex};
use rand::Rng;
use sha2::Sha256;

/// A workflow shape and its weight in the mix. The mix follows what CI on
/// GitHub looks like in aggregate: most runs are one or a few jobs, a
/// minority fan out through matrices, and a long tail is large.
pub struct Shape {
    pub name: &'static str,
    pub weight: u32,
    pub jobs: u32,
    pub yaml: String,
}

fn steps(n: usize) -> String {
    (0..n)
        .map(|i| format!("      - run: echo step {i}\n"))
        .collect()
}

/// The workflow mix. `jobs` is the number of runnable jobs a run produces.
pub fn shapes() -> Vec<Shape> {
    let single = format!(
        "name: lint\non: push\njobs:\n  lint:\n    runs-on: self-hosted\n    steps:\n{}",
        steps(6)
    );
    let fanout = format!(
        "name: ci\non: push\njobs:\n  lint:\n    runs-on: self-hosted\n    steps:\n{s}  test:\n    runs-on: self-hosted\n    steps:\n{s}  build:\n    runs-on: self-hosted\n    steps:\n{s}",
        s = steps(8)
    );
    let chain = format!(
        "name: release\non: push\njobs:\n  build:\n    runs-on: self-hosted\n    steps:\n{s}  test:\n    needs: build\n    runs-on: self-hosted\n    steps:\n{s}  package:\n    needs: test\n    runs-on: self-hosted\n    steps:\n{s}  deploy:\n    needs: [test, package]\n    runs-on: self-hosted\n    steps:\n{s}",
        s = steps(8)
    );
    let matrix = format!(
        "name: matrix\non: push\njobs:\n  test:\n    runs-on: self-hosted\n    strategy:\n      matrix:\n        os: [a, b, c]\n        v: [1, 2, 3]\n    steps:\n{s}  report:\n    needs: test\n    runs-on: self-hosted\n    steps:\n{s}",
        s = steps(10)
    );
    let big_matrix = format!(
        "name: monorepo\non: push\nconcurrency:\n  group: mono-${{{{ github.ref }}}}\n  cancel-in-progress: true\njobs:\n  shard:\n    runs-on: self-hosted\n    strategy:\n      fail-fast: false\n      matrix:\n        shard: [{shards}]\n    steps:\n{s}  merge:\n    needs: shard\n    runs-on: self-hosted\n    steps:\n{s}",
        shards = (1..=50).map(|i| i.to_string()).collect::<Vec<_>>().join(", "),
        s = steps(12)
    );
    vec![
        Shape { name: "single", weight: 40, jobs: 1, yaml: single },
        Shape { name: "fanout", weight: 30, jobs: 3, yaml: fanout },
        Shape { name: "chain", weight: 15, jobs: 4, yaml: chain },
        Shape { name: "matrix", weight: 12, jobs: 10, yaml: matrix },
        Shape { name: "big_matrix", weight: 3, jobs: 51, yaml: big_matrix },
    ]
}

/// Weighted picker over the mix.
pub struct Mix {
    pub shapes: Vec<Shape>,
    index: WeightedIndex<u32>,
}

impl Mix {
    pub fn new() -> Self {
        let shapes = shapes();
        let index = WeightedIndex::new(shapes.iter().map(|s| s.weight)).unwrap();
        Self { shapes, index }
    }

    pub fn pick_index(&self, rng: &mut impl Rng) -> usize {
        self.index.sample(rng)
    }

    /// Mean runnable jobs per workflow run.
    pub fn mean_jobs(&self) -> f64 {
        let total: u32 = self.shapes.iter().map(|s| s.weight).sum();
        self.shapes
            .iter()
            .map(|s| s.weight as f64 * s.jobs as f64)
            .sum::<f64>()
            / total as f64
    }
}

/// REST submission body for `shape` in `repository`.
pub fn api_submission(shape: &Shape, repository: &str, sequence: u64) -> serde_json::Value {
    let now = chrono::Utc::now();
    serde_json::json!({
        "workflow_yaml": shape.yaml,
        "event": "push",
        "repository": repository,
        "git_ref": "refs/heads/main",
        "workflow_path": format!(".github/workflows/{}.yml", shape.name),
        "payload": {
            "ref": "refs/heads/main",
            "after": format!("{:040x}", sequence),
            "repository": {
                "full_name": repository,
                "default_branch": "main",
                "pushed_at": now.timestamp(),
            },
            "commits": [],
        },
    })
}

/// A signed GitHub `push` delivery for the load workspace's HEAD commit.
pub fn webhook_push(
    repository: &str,
    head_sha: &str,
    secret: &str,
) -> (Vec<u8>, String, String) {
    let body = serde_json::to_vec(&serde_json::json!({
        "ref": "refs/heads/main",
        "before": "0000000000000000000000000000000000000000",
        "after": head_sha,
        "repository": {
            "full_name": repository,
            "default_branch": "main",
            "pushed_at": chrono::Utc::now().timestamp(),
        },
        "commits": [{"id": head_sha, "added": [], "modified": ["src/lib.rs"], "removed": []}],
        "head_commit": {"id": head_sha, "message": "load"},
        "sender": {"login": "loadgen"},
    }))
    .unwrap();
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(&body);
    let signature = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
    (body, signature, uuid::Uuid::new_v4().to_string())
}

/// Write the webhook workspace into `dir` as a git repository; return HEAD.
pub fn prepare_workspace(dir: &std::path::Path, mix: &Mix) -> anyhow::Result<String> {
    let workflows = dir.join(".github/workflows");
    std::fs::create_dir_all(&workflows)?;
    // A push to a typical repository triggers a couple of workflows, not the
    // whole mix: lint + ci (four jobs per push).
    for shape in mix.shapes.iter().filter(|s| matches!(s.name, "single" | "fanout")) {
        std::fs::write(workflows.join(format!("{}.yml", shape.name)), &shape.yaml)?;
    }
    let git = |args: &[&str]| -> anyhow::Result<String> {
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .output()?;
        anyhow::ensure!(out.status.success(), "git {args:?} failed");
        Ok(String::from_utf8(out.stdout)?.trim().to_owned())
    };
    git(&["init", "-q", "-b", "main"])?;
    git(&["config", "user.email", "load@example.invalid"])?;
    git(&["config", "user.name", "load"])?;
    git(&["add", "-A"])?;
    git(&["commit", "-qm", "load workflows", "--allow-empty"])?;
    git(&["rev-parse", "HEAD"])
}
