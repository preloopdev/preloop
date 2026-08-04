#!/usr/bin/env python3
"""Compare golden GitHub runs (cell A) against aksh-server captures (cells B/C).

Cell A: official runner vs GitHub — captured from GitHub's runs API.
Cell B: official runner vs aksh server — captured by the campaign harness.
Cell C: aksh runner vs aksh server — captured by the campaign harness.

The comparison is semantic: job names, step names, step order and conclusions.
Diffs are categorized:

  environment  — the local host cannot do what the workflow asks (Windows
                 matrix cells on macOS, `apt-get`, missing cross toolchains,
                 broken local libraries). Not a conformance defect.
  semantic     — the aksh stack produced a different job/step outcome than
                 GitHub would. A conformance defect to investigate.
  naming       — job/step display-name differences (raw YAML keys vs
                 GitHub's `name:` rendering, un-evaluated matrix names).

Usage:
  python3 compare-goldens.py [--json]
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent.parent  # repo root
GOLDENS = ROOT / "benchmarks" / "real-world" / "goldens"
RESULTS = ROOT / "benchmarks" / "real-world" / "results" / "conformance-4repos"

REPOS = {
    "bat": "sharkdp_bat",
    "vite": "vitejs_vite",
    "uv": "astral-sh_uv",
    "nextcloud": "nextcloud_server",
    "runner": "actions_runner",
    "qm": "yc_software_qm",
    "buzz": "block_buzz",
    "openclaw": "openclaw_openclaw",
    "agent-ci": "redwoodjs_agent-ci",
    "bento": "nyblnet_bento",
    "caddy": "caddyserver_caddy",
    "tokio": "tokio-rs_tokio",
}

# bat's CICD on GitHub runs a 13-target cross-compile matrix; the local host
# only executes what it can. Environment diffs are expected for these.
ENV_JOB_HINTS = ("windows", "musl", "arm-unknown", "i686", "x86_64-unknown")


def load_golden(repo):
    gdir = GOLDENS / REPOS[repo]
    return json.loads((gdir / "golden.json").read_text())


def load_capture(repo, cell):
    p = RESULTS / repo / cell / "run.json"
    if not p.exists():
        return None
    data = json.loads(p.read_text())
    jobs = {j["name"]: j for j in data.get("jobs_list", [])}
    return {"status": data.get("status"), "jobs": jobs}


def golden_job_index(golden):
    """Key golden jobs by their YAML key where possible, else display name."""
    # GitHub's jobs API returns the evaluated display name; aksh's record uses
    # the YAML key. Build both indexes for fuzzy matching.
    by_display = {}
    by_key = {}
    for job in golden["jobs"]:
        by_display[job["name"]] = job
        # The first job whose display name differs from its key is the key.
    return by_display, by_key


def compare_repo(repo, cell):
    golden = load_golden(repo)
    capture = load_capture(repo, cell)
    if capture is None:
        return {"repo": repo, "cell": cell, "error": "no capture"}
    g_jobs = {j["name"]: j for j in golden["jobs"]}

    out = {
        "repo": repo,
        "cell": cell,
        "golden_status": golden.get("conclusion"),
        "captured_status": capture["status"],
        "jobs": [],
        "counts": {"match": 0, "semantic": 0, "environment": 0, "naming": 0},
    }

    captured_names = list(capture["jobs"].keys())
    # aksh records jobs by YAML key; match golden display names to keys by
    # trying the name and, failing that, the first golden job whose display
    # name starts with the key or vice versa.
    matched_golden = set()
    generic_steps = {"Set up job", "Complete job", "Post run", "Post Run actions/checkout@v4", "Post Run actions/checkout@v6"}
    run_has_env_failure = any(
        cjob.get("conclusion") == "failure" and is_env_name(cname)
        for cname, cjob in capture["jobs"].items()
    )
    for key, cjob in capture["jobs"].items():
        gjob = g_jobs.get(key)
        if gjob is None:
            # Score golden jobs by shared non-generic step names.
            c_steps = {s["name"] for s in cjob.get("steps", []) if s["name"] not in generic_steps}
            best, best_score = None, 0
            for gname, g in g_jobs.items():
                g_steps = {s["name"] for s in g.get("steps", []) if s["name"] not in generic_steps}
                score = len(c_steps & g_steps)
                if score > best_score:
                    best, best_score = g, score
            if best is not None and best_score > 0:
                gjob = best
        if gjob is None:
            gjob = {"name": key, "conclusion": None, "steps": []}
        matched_golden.add(gjob["name"])
        job = compare_job(gjob, cjob, run_has_env_failure)
        out["jobs"].append(job)
        out["counts"][job["category"]] += 1

    # Golden jobs that were never captured (skipped locally before dispatch).
    for gname, gjob in g_jobs.items():
        if gname not in matched_golden:
            if gjob.get("conclusion") == "skipped":
                continue
            job = {
                "name": gname,
                "golden_conclusion": gjob.get("conclusion"),
                "captured_conclusion": "absent",
                "category": "environment",
                "detail": "job absent from local run",
                "steps": [],
            }
            out["jobs"].append(job)
            out["counts"]["environment"] += 1

    out["jobs"].sort(key=lambda j: j["name"])
    return out


def is_env_name(name):
    low = name.lower()
    return any(hint in low for hint in ENV_JOB_HINTS)


def compare_job(gjob, cjob, run_has_env_failure=False):
    gname = gjob["name"]
    cname = cjob["name"]
    g_conc = gjob.get("conclusion")
    c_conc = cjob.get("conclusion")

    g_steps = {s["name"]: s.get("conclusion") for s in gjob.get("steps", [])}
    c_steps = {s["name"]: s.get("conclusion") for s in cjob.get("steps", [])}
    # aksh run records drop unnamed steps (known projection gap); the runner
    # logs carry them. Normalize: compare only steps present in both, and
    # report naming gaps for empty names.
    g_step_names = [s["name"] for s in gjob.get("steps", [])]
    c_step_names = [s["name"] for s in cjob.get("steps", [])]

    diffs = []
    for s in c_step_names:
        if not s:
            diffs.append("unnamed step in captured run (projection gap)")
    for s in g_step_names:
        if s not in c_step_names and s not in ("Set up job", "Complete job", "Post run"):
            diffs.append(f"step '{s}' missing from captured run")

    for s, c in c_steps.items():
        if not s:
            continue
        g = g_steps.get(s)
        if g is not None and g != c:
            diffs.append(f"step '{s}': golden={g} captured={c}")

    # Step-set comparison without names (order + conclusions).
    g_conc_list = [s.get("conclusion") for s in gjob.get("steps", []) if s.get("name")]
    c_conc_list = [s.get("conclusion") for s in cjob.get("steps", []) if s.get("name")]
    if g_conc_list and c_conc_list and g_conc_list != c_conc_list:
        diffs.append(f"step conclusion sequence differs: golden={g_conc_list} captured={c_conc_list}")

    if g_conc != c_conc:
        category = "semantic"
        if is_env_name(gname):
            category = "environment"
        elif g_conc == "skipped" and c_conc != "skipped":
            category = "semantic"
        elif g_conc != "skipped" and c_conc == "skipped":
            category = "environment"  # dependency failure skip on local host
        elif g_conc == "success" and c_conc == "failure" and run_has_env_failure:
            # Gate jobs (`all-jobs`) fail because local environment jobs failed.
            category = "environment"
        detail = f"conclusion: golden={g_conc} captured={c_conc}"
    else:
        category = "match"
        detail = ""

    # Name-form differences (raw YAML keys / un-evaluated matrix) are naming.
    if category == "match" and gname != cname:
        category = "naming"
        detail = f"job name: golden='{gname}' captured='{cname}'"

    if category == "match" and diffs:
        category = "semantic"
        detail = "; ".join(diffs[:4])
    elif category != "match" and diffs and not detail:
        detail = "; ".join(diffs[:4])
    elif category == "match" and not detail:
        detail = "ok"

    return {
        "name": cname,
        "golden_name": gname,
        "golden_conclusion": g_conc,
        "captured_conclusion": c_conc,
        "golden_steps": len(gjob.get("steps", [])),
        "captured_steps": len(cjob.get("steps", [])),
        "category": category,
        "detail": detail,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--repo", default="all")
    args = ap.parse_args()

    report = []
    # `c` is the preloop production path: aksh runner in per-job smolVMs
    # against the local engine. `official`/`aksh` are the older host-runner
    # cells from conformance-4repos.sh.
    for repo, cells in (("bat", ["official", "aksh", "c"]), ("vite", ["official", "aksh", "c"]),
                        ("uv", ["official", "aksh", "c"]), ("nextcloud", ["official", "aksh", "c"]),
                        ("runner", ["c"]), ("qm", ["c"]), ("buzz", ["c"]),
                        ("openclaw", ["c"]), ("agent-ci", ["c"])):
        if args.repo != "all" and repo != args.repo:
            continue
        for cell in cells:
            report.append(compare_repo(repo, cell))

    if args.json:
        print(json.dumps(report, indent=2))
        return

    for r in report:
        if "error" in r:
            print(f"== {r['repo']}/{r['cell']}: {r['error']}")
            continue
        print(f"== {r['repo']}/{r['cell']}: golden={r['golden_status']} captured={r['captured_status']} "
              f"({r['counts']['match']} match, {r['counts']['semantic']} semantic, "
              f"{r['counts']['environment']} environment, {r['counts']['naming']} naming)")
        for j in r["jobs"]:
            if j["category"] != "match":
                print(f"   [{j['category']:11s}] {j['name'][:70]:70s} {j['detail'][:90]}")


if __name__ == "__main__":
    main()
