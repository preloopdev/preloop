#!/usr/bin/env python3
"""Build a comprehensive capture inventory document for all conformance workflows."""
import json, re
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone

REPO = "preloopdev/aksh-conformance-sample"
HOME = Path.home()
WORKTREES = [
    HOME / "macos-runners",
    HOME / "cachingv4",
    HOME / "mitm-proxy",
    HOME / "runner-watcher",
    HOME / "workflow-support",
]
OUTPUT = Path("benchmarks/real-world/results/CAPTURE-INVENTORY.md")

def find_flows(dirs: list[Path]) -> list[Path]:
    results = []
    for d in dirs:
        if not d.exists(): continue
        for f in d.rglob("flows.jsonl"):
            if "node_modules" in str(f): continue
            if f.stat().st_size > 0:
                results.append(f)
    return sorted(results)

def extract_pair(path: Path) -> tuple[str, str] | None:
    """Return (number, runner) or None."""
    s = str(path)
    # Pattern: runner-flow/93-empty-null-values/official/latest/flows.jsonl
    m = re.search(r'runner-flow/(\d{2,3})-[^/]+?/(official|aksh)/', s)
    if m: return (m.group(1), m.group(2))
    # Pattern: captures/official/07-step-failure/.../flows.jsonl
    m = re.search(r'/captures/(official|aksh|aksh-runner-aksh|runner-server)/(\d{2,3})-[^/]+?/', s)
    if m: return (m.group(2), m.group(1))
    # Pattern: golden/v2.335.1/06-multi-step/flows.jsonl
    m = re.search(r'/golden/[^/]+/(\d{2,3})-[^/]+?/flows\.jsonl', s)
    if m: return (m.group(1), "official")
    return None

def count(path: Path) -> int:
    try: return sum(1 for _ in open(path))
    except: return 0

# ── Workflow inventory ──────────────────────────────────────────────

MANUAL = {
    "01":"register-and-idle","06":"multi-step","07":"step-failure",
    "08":"job-outputs-needs","09":"matrix-fan-out","10":"uses-checkout",
    "11":"cache-roundtrip","12":"artifact","13":"composite-action",
    "14":"annotations","15":"oidc-id-token",
    "19":"step-summary","20":"reusable-workflow","21":"job-timeout",
    "22":"cancel-semantics","23":"context-fields","24":"problem-matcher",
    "30":"container-job-basic","31":"container-with-services",
    "32":"services-no-container","33":"container-env-options",
    "34":"container-with-checkout","35":"container-lifecycle",
    "36":"docker-action","50":"signal-sequence","51":"action-contexts",
    "52":"expression-features","53":"secret-masking","54":"job-annotations",
    "55":"proxy-injection","56":"problem-matcher-frompath",
    "57":"runner-settings","58":"auth-and-diag","60":"hashfiles-and-fips",
    "61":"cache-stress","62":"artifact-stress","63":"mega-runner-stress",
    "70":"defaults-run","71":"composite-advanced","72":"label-matching",
    "73":"path-env","74":"broker-poll-timing","75":"workflow-call-stress",
    "80":"custom-shells","81":"step-timeout","82":"reusable-workflow",
    "83":"local-node-action","84":"concurrency-groups",
    "85":"permissions-scoping","86":"environment-deployments",
    "87":"multiline-output","88":"state-and-post","89":"workflow-inputs",
    "90":"shell-exit-behavior","91":"large-output",
    "92":"unicode-special-chars","93":"empty-null-values",
    "94":"action-pinning","95":"nested-composite-outputs",
    "96":"env-inheritance","97":"artifact-cross-job",
    "98":"outcome-vs-conclusion","99":"workspace-defaults","100":"tool-cache",
}
workflows = {n: {"name": name, "file": f"{n}-{name}.yml"} for n, name in MANUAL.items()}

# ── Collect flows ───────────────────────────────────────────────────

official = {}; aksh = {}
for f in find_flows(WORKTREES):
    pair = extract_pair(f)
    if not pair: continue
    num, runner = pair
    c = count(f)
    if runner == "official":
        if num not in official or c > official[num]: official[num] = c
    else:
        if num not in aksh or c > aksh[num]: aksh[num] = c

# ── Outcome data ────────────────────────────────────────────────────

OFF_MAP = {"Multiline Output via Heredoc":"87","State and Post Step Behavior":"88",
    "Workflow Dispatch with Typed Inputs":"89","Shell Exit Behavior and Pipefail":"90",
    "Large Output Handling":"91","Unicode and Special Characters":"92",
    "Empty and Null Values":"93","Custom Shells":"80","Step Timeout":"81",
    "Reusable Workflow Caller":"82","Local Node Action":"83",
    "Permissions Scoping":"85","Environment Deployments":"86"}

off_out = {}; aksh_out = {}

off_j = Path("benchmarks/real-world/results/conformance/conformance-official.jsonl")
if off_j.exists():
    for line in open(off_j):
        d = json.loads(line)
        wf = d.get("workflow") or d.get("result",{}).get("workflow","")
        n = OFF_MAP.get(wf) or (m.group(1) if (m:=re.match(r"(\d+)", wf)) else None)
        if n: off_out[n] = d.get("conclusion","") or "(empty)"

aksh_j = Path("benchmarks/real-world/results/conformance/conformance-aksh.jsonl")
if aksh_j.exists():
    for line in open(aksh_j):
        d = json.loads(line)
        if m := re.match(r"(\d+)", d.get("workflow","")):
            c = d.get("conclusion","") or "(empty)"
            if c != "(empty)" or m.group(1) not in aksh_out:
                aksh_out[m.group(1)] = c

# ── Flow diffs ──────────────────────────────────────────────────────

diffs = {}
diff_root = Path("benchmarks/real-world/results/runner-flow")
for df in diff_root.rglob("diff.md"):
    scenario = df.parent.name
    text = df.read_text()
    if m := re.search(r'FAIL: (\d+) contract', text):
        diffs[scenario] = ("FAIL", int(m.group(1)))
    elif "PASS" in text:
        diffs[scenario] = ("PASS", 0)

def find_diff_for(name: str) -> str:
    for d in diff_root.iterdir():
        if d.is_dir() and d.name.startswith(f"{name}-") or d.name == name:
            diff_file = d / "diff.md"
            if diff_file.exists():
                text = diff_file.read_text()
                if m := re.search(r'FAIL: (\d+) contract', text):
                    return f"⚠️ [{m.group(1)} diffs](runner-flow/{d.name}/diff.md)"
                if "PASS" in text:
                    return f"✅ [PASS](runner-flow/{d.name}/diff.md)"
            return f"— [no verdict](runner-flow/{d.name}/diff.md)"
    return ""

# ── Generate report ─────────────────────────────────────────────────

lines = []
lines.append("# Capture Inventory & Conformance Status")
lines.append("")
lines.append(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
lines.append("")
lines.append("## Legend")
lines.append("")
lines.append("| Icon | Meaning |")
lines.append("|---|---|")
lines.append("| ✅ | Both sides captured, flow diff PASS |")
lines.append("| ⚠️ | Both captured, flow diff FAIL (see diff.md) |")
lines.append("| 🔵 | Official MITM flows only (no aksh) |")
lines.append("| 🟡 | Aksh MITM flows only (no official) |")
lines.append("| ⬜ | Neither MITM captured |")
lines.append("| 🟢 | Outcome match (both same conclusion) |")
lines.append("| 🔴 | Outcome mismatch |")
lines.append("| ⏳ | One side incomplete or missing |")
lines.append("")

total = len(workflows)
off_n = sum(1 for n in workflows if n in official)
aksh_n = sum(1 for n in workflows if n in aksh)
both_n = sum(1 for n in workflows if n in official and n in aksh)
diff_n = len(diffs)
outcome_matches = sum(1 for n in off_out if n in aksh_out and off_out[n]==aksh_out[n] and off_out[n] not in ("(empty)",""))

lines.append("## Summary")
lines.append(f"| Metric | Count |")
lines.append(f"|---|---:|")
lines.append(f"| Workflows | {total} |")
lines.append(f"| Official MITM flows | {off_n} |")
lines.append(f"| Aksh MITM flows | {aksh_n} |")
lines.append(f"| Both captured | {both_n} |")
lines.append(f"| Flow diffs | {diff_n} |")
lines.append(f"| Outcome matches | {outcome_matches} |")
lines.append("")

lines.append("## Per-Workflow Inventory")
lines.append("| # | Workflow | Official | Aksh | Flow Diff | Outcome |")
lines.append("|---|---:|---:|---|---|")

for num in sorted(workflows, key=lambda x: int(x)):
    info = workflows[num]
    name = info["name"]
    of = official.get(num, 0)
    af = aksh.get(num, 0)
    
    # Icon
    if of and af and num in diffs: icon = "✅" if diffs[num][0]=="PASS" else "⚠️"
    elif of and af: icon = "⚠️"
    elif of: icon = "🔵"
    elif af: icon = "🟡"
    else: icon = "⬜"
    
    # Flow diff link
    df = find_diff_for(num)
    if not df and of and af: df = "— (no diff)"
    
    # Outcome
    oo = off_out.get(num, "—")
    ao = aksh_out.get(num, "—")
    outcome = ""
    if oo != "—" and ao != "—":
        if oo == ao and oo not in ("(empty)",""): outcome = f"🟢 {oo}"
        elif oo == "(empty)" or ao == "(empty)": outcome = f"⏳ {oo} | {ao}"
        else: outcome = f"🔴 {oo} vs {ao}"
    elif oo != "—": outcome = f"{oo} (off only)"
    elif ao != "—": outcome = f"{ao} (aksh only)"
    
    lines.append(f"| {num} | {icon} {name} | {of} | {af} | {df} | {outcome} |")

lines.append("")
lines.append("## Gaps")
lines.append("")
lines.append("### Official MITM flows only — need aksh recapture")
g1 = [n for n in sorted(workflows, key=int) if n in official and n not in aksh]
for n in g1: lines.append(f"- **{n}** — {workflows[n]['name']} ({official[n]} flows)")
if not g1: lines.append("_none_")
lines.append(f"_({len(g1)} scenarios)_")
lines.append("")
lines.append("### Aksh MITM flows only — need official recapture")
g2 = [n for n in sorted(workflows, key=int) if n not in official and n in aksh]
for n in g2: lines.append(f"- **{n}** — {workflows[n]['name']} ({aksh[n]} flows)")
if not g2: lines.append("_none_")
lines.append(f"_({len(g2)} scenarios)_")
lines.append("")
lines.append("### Neither captured")
g3 = [n for n in sorted(workflows, key=int) if n not in official and n not in aksh]
for n in g3: lines.append(f"- **{n}** — {workflows[n]['name']}")
if not g3: lines.append("_none_")
lines.append(f"_({len(g3)} scenarios)_")

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
OUTPUT.write_text("\n".join(lines) + "\n")
print(f"Written: {OUTPUT}")
print(f"  {total} workflows | {off_n} official | {aksh_n} aksh | {both_n} both | {diff_n} diffs")
print(f"  Official-only: {len(g1)} | Aksh-only: {len(g2)} | Neither: {len(g3)}")
