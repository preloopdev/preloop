#!/usr/bin/env python3
"""Build a comprehensive capture inventory document for all conformance workflows."""
import json, re
from pathlib import Path
from datetime import datetime, timezone

HOME = Path.home()
WORKTREES = [HOME / "macos-runners", HOME / "cachingv4", HOME / "mitm-proxy", HOME / "runner-watcher", HOME / "workflow-support"]
OUTPUT = Path("benchmarks/real-world/results/CAPTURE-INVENTORY.md")

def find_flows(dirs):
    results = []
    for d in dirs:
        if not d.exists(): continue
        for f in d.rglob("flows.jsonl"):
            if "node_modules" in str(f): continue
            if f.stat().st_size > 0: results.append(f)
    return sorted(results)

def extract_pair(path):
    s = str(path)
    for pat in [r'runner-flow/(\d{2,3})-[^/]+?/(official|aksh)/',
                r'/captures/(official|aksh|aksh-runner-aksh|runner-server)/(\d{2,3})-[^/]+?/',
                r'/golden/[^/]+/(\d{2,3})-[^/]+?/flows\.jsonl']:
        m = re.search(pat, s)
        if m:
            if m.lastindex == 2 and m.group(1).isdigit(): return (m.group(1), m.group(2))
            if m.lastindex == 2 and not m.group(1).isdigit(): return (m.group(2), m.group(1))
            if m.lastindex == 1: return (m.group(1), "official")
    return None

def cnt(path):
    try: return sum(1 for _ in open(path))
    except: return 0

MANUAL = {
    "01":"register-and-idle","06":"multi-step","07":"step-failure",
    "08":"job-outputs-needs","09":"matrix-fan-out","10":"uses-checkout",
    "11":"cache-roundtrip","12":"artifact","13":"composite-action",
    "14":"annotations","15":"oidc-id-token","19":"step-summary",
    "20":"reusable-workflow","21":"job-timeout","22":"cancel-semantics",
    "23":"context-fields","24":"problem-matcher",
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
workflows = {n: {"name": name} for n, name in MANUAL.items()}

official = {}; aksh = {}
for f in find_flows(WORKTREES):
    pair = extract_pair(f)
    if not pair: continue
    num, runner = pair
    c = cnt(f)
    if runner == "official":
        if num not in official or c > official[num]: official[num] = c
    elif num not in aksh or c > aksh[num]: aksh[num] = c

OFF_MAP = {"Multiline Output via Heredoc":"87","State and Post Step Behavior":"88",
    "Workflow Dispatch with Typed Inputs":"89","Shell Exit Behavior and Pipefail":"90",
    "Large Output Handling":"91","Unicode and Special Characters":"92",
    "Empty and Null Values":"93","Custom Shells":"80","Step Timeout":"81",
    "Reusable Workflow Caller":"82","Local Node Action":"83",
    "Permissions Scoping":"85","Environment Deployments":"86"}

off_out = {}; aksh_out = {}
for fn, store in [("conformance-official.jsonl", off_out), ("conformance-aksh.jsonl", aksh_out)]:
    p = Path(f"benchmarks/real-world/results/conformance/{fn}")
    if not p.exists(): continue
    for line in open(p):
        d = json.loads(line)
        wf = d.get("workflow") or d.get("result",{}).get("workflow","")
        n = OFF_MAP.get(wf) or (m.group(1) if (m:=re.match(r"(\d+)", wf)) else None)
        if n:
            c = d.get("conclusion","") or "(empty)"
            if store is aksh_out and c == "(empty)" and n in aksh_out: continue
            store[n] = c

diff_root = Path("benchmarks/real-world/results/runner-flow")
def find_diff(num, name):
    for df in [diff_root / f"{num}-{name}" / "diff.md", diff_root / num / "diff.md"]:
        if not df.exists(): continue
        t = df.read_text()
        if m := re.search(r'FAIL: (\d+) contract', t): return f"[{m.group(1)} diffs](runner-flow/{df.parent.name}/diff.md)"
        if "PASS" in t: return f"[PASS](runner-flow/{df.parent.name}/diff.md)"
        return f"[no verdict](runner-flow/{df.parent.name}/diff.md)"
    return "—"

lines = []
lines.append("# Capture Inventory & Conformance Status")
lines.append(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
lines.append("## Legend")
lines.append("| Icon | Meaning |"); lines.append("|---|---|")
lines.append("| ⚠️ | Both MITM captured, flow diff available |")
lines.append("| 🔵 | Official MITM only (no aksh) |")
lines.append("| 🟡 | Aksh MITM only (no official) |")
lines.append("| ⬜ | Neither MITM captured |")
lines.append("| 🟢 | Outcome match |"); lines.append("| 🔴 | Outcome mismatch |")
lines.append("| ⏳ | One side incomplete |")
lines.append("")

off_n = sum(1 for n in workflows if n in official)
aksh_n = sum(1 for n in workflows if n in aksh)
both_n = sum(1 for n in workflows if n in official and n in aksh)
match_n = sum(1 for n in off_out if n in aksh_out and off_out[n]==aksh_out[n] and off_out[n] not in ("(empty)",""))

lines.append("## Summary")
lines.append(f"64 workflows — {off_n} official MITM captures — {aksh_n} aksh MITM captures — {both_n} both — {match_n} outcome matches")
lines.append("")

lines.append("| # | Workflow | Official | Aksh | Flow Diff | Outcome |")
lines.append("|---|---:|---:|---|---|")

for num in sorted(workflows, key=lambda x: int(x)):
    name = workflows[num]["name"]
    of = official.get(num, 0); af = aksh.get(num, 0)
    icon = "⚠️" if of and af else ("🔵" if of else ("🟡" if af else "⬜"))
    df = find_diff(num, name)
    oo = off_out.get(num, "—"); ao = aksh_out.get(num, "—")
    if oo != "—" and ao != "—":
        outcome = f"🟢 {oo}" if (oo==ao and oo not in ("(empty)","")) else (f"⏳ {oo}/{ao}" if (oo=="(empty)" or ao=="(empty)") else f"🔴 {oo}/{ao}")
    else: outcome = oo if oo != "—" else (ao if ao != "—" else "—")
    of_s = str(of) if of else "—"; af_s = str(af) if af else "—"
    lines.append(f"| {num} | {icon} {name} | {of_s} | {af_s} | {df} | {outcome} |")

lines.append("")
lines.append("## Gaps")
lines.append("### Official MITM only — need aksh recapture")
g1 = [n for n in sorted(workflows, key=int) if n in official and n not in aksh]
for n in g1: lines.append(f"- **{n}** {workflows[n]['name']} — {official[n]} official flows")
lines.append(f"_{len(g1)} scenarios_")
lines.append("### Aksh MITM only — need official recapture")
g2 = [n for n in sorted(workflows, key=int) if n not in official and n in aksh]
for n in g2: lines.append(f"- **{n}** {workflows[n]['name']} — {aksh[n]} aksh flows")
lines.append(f"_{len(g2)} scenarios_")
lines.append("### Neither MITM captured")
g3 = [n for n in sorted(workflows, key=int) if n not in official and n not in aksh]
for n in g3: lines.append(f"- **{n}** {workflows[n]['name']}")
lines.append(f"_{len(g3)} scenarios_")

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
OUTPUT.write_text("\n".join(lines) + "\n")
print(f"Written: {OUTPUT}")
print(f"  {off_n} official | {aksh_n} aksh | {both_n} both | {match_n} outcome matches")
print(f"  Official-only: {len(g1)} | Aksh-only: {len(g2)} | Neither: {len(g3)}")
