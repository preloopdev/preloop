#!/usr/bin/env python3
"""Build capture inventory with separate MITM and Conformance tables."""
import json, re
from pathlib import Path
from datetime import datetime, timezone

HOME = Path.home()
WT = [HOME / "macos-runners", HOME / "cachingv4", HOME / "mitm-proxy", HOME / "runner-watcher", HOME / "workflow-support"]
OUT = Path("benchmarks/real-world/results/CAPTURE-INVENTORY.md")

def find_flows(dirs):
    results = []
    for d in dirs:
        if not d.exists(): continue
        for f in d.rglob("flows.jsonl"):
            if "node_modules" in str(f): continue
            if f.stat().st_size > 0: results.append(f)
    return sorted(results)

def extract(fpath):
    s = str(fpath)
    for pat in [r'runner-flow/(\d{2,3})-[^/]+?/(official|aksh)/',
                r'/captures/(official|aksh|aksh-runner-aksh|runner-server)/(\d{2,3})-[^/]+?/',
                r'/golden/[^/]+/(\d{2,3})-[^/]+?/flows\.jsonl']:
        m = re.search(pat, s)
        if m:
            g = m.groups()
            if len(g) == 2 and g[0].isdigit(): return (g[0], g[1])
            if len(g) == 2 and not g[0].isdigit(): return (g[1], g[0])
            if len(g) == 1: return (g[0], "official")
    return None

def cnt(path):
    try: return sum(1 for _ in open(path))
    except: return 0

# ── MITM flow inventory ─────────────────────────────────────────────

MITM_SCENARIOS = {f"{n:02d}": name for n, name in [
    (1,"register-and-idle"),(6,"multi-step"),(7,"step-failure"),
    (8,"job-outputs-needs"),(9,"matrix-fan-out"),(10,"uses-checkout"),
    (11,"cache-roundtrip"),(12,"artifact"),(13,"composite-action"),
    (14,"annotations"),(15,"oidc-id-token"),(19,"step-summary"),
    (20,"reusable-workflow"),(21,"job-timeout"),(22,"cancel-semantics"),
    (23,"context-fields"),(24,"problem-matcher"),
    (30,"container-job-basic"),(31,"container-with-services"),
    (32,"services-no-container"),(33,"container-env-options"),
    (34,"container-with-checkout"),(35,"container-lifecycle"),
    (36,"docker-action"),(50,"signal-sequence"),(51,"action-contexts"),
    (52,"expression-features"),(53,"secret-masking"),(54,"job-annotations"),
    (55,"proxy-injection"),(56,"problem-matcher-frompath"),
    (57,"runner-settings"),(58,"auth-and-diag"),(60,"hashfiles-and-fips"),
    (61,"cache-stress"),(62,"artifact-stress"),(63,"mega-runner-stress"),
    (70,"defaults-run"),(71,"composite-advanced"),(72,"label-matching"),
    (73,"path-env"),(74,"broker-poll-timing"),(75,"workflow-call-stress"),
]}

official = {}; aksh = {}
for f in find_flows(WT):
    pair = extract(f)
    if not pair: continue
    num, runner = pair
    c = cnt(f)
    if runner == "official":
        if num not in official or c > official[num]: official[num] = c
    elif num not in aksh or c > aksh[num]: aksh[num] = c

diff_root = Path("benchmarks/real-world/results/runner-flow")
def find_diff(num, name):
    for df in [diff_root / f"{num}-{name}" / "diff.md", diff_root / num / "diff.md"]:
        if not df.exists(): continue
        t = df.read_text()
        if m := re.search(r'FAIL: (\d+) contract', t): return f"[{m.group(1)} diffs](runner-flow/{df.parent.name}/diff.md)"
        if "PASS" in t: return f"[PASS](runner-flow/{df.parent.name}/diff.md)"
        return f"[empty](runner-flow/{df.parent.name}/diff.md)"
    return "—"

# ── Conformance outcome data ────────────────────────────────────────

CONFORMANCE_NUMS = [str(n) for n in range(80, 101)]

OFF_MAP = {"Multiline Output via Heredoc":"87","State and Post Step Behavior":"88",
    "Workflow Dispatch with Typed Inputs":"89","Shell Exit Behavior and Pipefail":"90",
    "Large Output Handling":"91","Unicode and Special Characters":"92",
    "Empty and Null Values":"93","Custom Shells":"80","Step Timeout":"81",
    "Reusable Workflow Caller":"82","Local Node Action":"83",
    "Permissions Scoping":"85","Environment Deployments":"86"}

CONFORMANCE_NAMES = {
    "80":"custom-shells","81":"step-timeout","82":"reusable-workflow",
    "83":"local-node-action","84":"concurrency-groups","85":"permissions-scoping",
    "86":"environment-deployments","87":"multiline-output","88":"state-and-post",
    "89":"workflow-inputs","90":"shell-exit-behavior","91":"large-output",
    "92":"unicode-special-chars","93":"empty-null-values","94":"action-pinning",
    "95":"nested-composite-outputs","96":"env-inheritance","97":"artifact-cross-job",
    "98":"outcome-vs-conclusion","99":"workspace-defaults","100":"tool-cache",
}

off_out = {}; aksh_out = {}
for fn, store in [("conformance-official.jsonl", off_out), ("conformance-aksh.jsonl", aksh_out)]:
    p = Path(f"benchmarks/real-world/results/conformance/{fn}")
    if not p.exists(): continue
    for line in open(p):
        d = json.loads(line)
        wf = d.get("workflow") or d.get("result",{}).get("workflow","")
        n = OFF_MAP.get(wf) or (m.group(1) if (m:=re.match(r"(\d+)", wf)) else None)
        if n and n in CONFORMANCE_NUMS:
            c = d.get("conclusion","") or "(empty)"
            if store is aksh_out and c == "(empty)" and n in aksh_out: continue
            store[n] = c

# ── Generate report ─────────────────────────────────────────────────

L = []
L.append("# Capture Inventory & Conformance Status")
L.append(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
L.append("")
L.append("Two separate data sources:")
L.append("- **MITM flows** — raw HTTP traffic captures (flows.jsonl) from mitmproxy, recording every request/response")
L.append("- **Conformance outcomes** — job conclusion + step data from GitHub's API after live workflow dispatch")
L.append("")

# ── Table 1: MITM ──────────────────────────────────────────────────

off_n = sum(1 for n in MITM_SCENARIOS if n in official)
aksh_n = sum(1 for n in MITM_SCENARIOS if n in aksh)
both_n = sum(1 for n in MITM_SCENARIOS if n in official and n in aksh)

L.append("## MITM Flow Captures")
L.append(f"{len(MITM_SCENARIOS)} scenarios — {off_n} official — {aksh_n} aksh — {both_n} both — [diffs](runner-flow/) linked where available")
L.append("")
L.append("| # | Scenario | Official | Aksh | Diff |")
L.append("|---|---:|---:|---|")

for num in sorted(MITM_SCENARIOS, key=lambda x: int(x)):
    name = MITM_SCENARIOS[num]
    of = official.get(num, 0); af = aksh.get(num, 0)
    icon = "⚠️" if of and af else ("🔵" if of else ("🟡" if af else "⬜"))
    df = find_diff(num, name)
    of_s = str(of) if of else "—"; af_s = str(af) if af else "—"
    L.append(f"| {num} | {icon} {name} | {of_s} | {af_s} | {df} |")

L.append("")
L.append("### Gaps")
g1 = [n for n in sorted(MITM_SCENARIOS, key=int) if n in official and n not in aksh]
g2 = [n for n in sorted(MITM_SCENARIOS, key=int) if n not in official and n in aksh]
g3 = [n for n in sorted(MITM_SCENARIOS, key=int) if n not in official and n not in aksh]
L.append(f"**Official only ({len(g1)}):** " + ", ".join(f"{n} {MITM_SCENARIOS[n]}" for n in g1) if g1 else "**Official only:** _none_")
L.append(f"**Aksh only ({len(g2)}):** " + ", ".join(f"{n} {MITM_SCENARIOS[n]}" for n in g2) if g2 else "**Aksh only:** _none_")
L.append(f"**Neither ({len(g3)}):** " + ", ".join(f"{n}" for n in g3) if g3 else "**Neither:** _none_")
L.append("")

# ── Table 2: Conformance ───────────────────────────────────────────

match_n = sum(1 for n in CONFORMANCE_NUMS if n in off_out and n in aksh_out and off_out[n]==aksh_out[n] and off_out[n] not in ("(empty)",""))
fail_n = sum(1 for n in CONFORMANCE_NUMS if n in off_out and n in aksh_out and off_out[n]!=aksh_out[n] and off_out[n] not in ("(empty)","") and aksh_out[n] not in ("(empty)",""))
inc_n  = sum(1 for n in CONFORMANCE_NUMS if n in off_out and n in aksh_out and (off_out[n]=="(empty)" or aksh_out[n]=="(empty)"))

for num in CONFORMANCE_NUMS:
    name = CONFORMANCE_NAMES.get(num, num)
    oo = off_out.get(num, "—"); ao = aksh_out.get(num, "—")
    if oo != "—" and ao != "—":
        if oo == ao and oo not in ("(empty)",""):
            icon, display = "🟢", f"{oo}"
        elif oo == "(empty)" or ao == "(empty)":
            icon, display = "⏳", f"{oo} / {ao}"
        else:
            icon, display = "🔴", f"{oo} / {ao}"
    else:
        icon, display = "—", (oo if oo != "—" else ao)
    L.append(f"| {num} | {name} | {oo} | {ao} | {icon} {display} |")

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text("\n".join(L) + "\n")
print(f"Written: {OUT}")
print(f"  MITM: {off_n} official, {aksh_n} aksh, {both_n} both | Official-only: {len(g1)}, Aksh-only: {len(g2)}, Neither: {len(g3)}")
print(f"  Conformance: {match_n} match, {fail_n} mismatch, {inc_n} incomplete")
