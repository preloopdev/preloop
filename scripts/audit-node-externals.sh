#!/usr/bin/env bash
set -euo pipefail

# audit-node-externals.sh — Node externals supply-chain gate
#
# Reads node20_externals_version / node24_externals_version from versions.toml,
# downloads the pinned linux-x64 tarballs from nodejs.org, verifies SHA-256
# against SHASUMS256.txt, extracts lib/node_modules, enumerates every
# package.json, queries OSV (https://api.osv.dev/v1/querybatch), fails on
# HIGH/CRITICAL, and emits a CycloneDX 1.5 JSON SBOM.
#
# Usage:
#   scripts/audit-node-externals.sh            # fails on HIGH/CRITICAL
#   scripts/audit-node-externals.sh --report-only  # never fails on vulns, just reports
#   scripts/audit-node-externals.sh --record-baseline # records current vulns to baseline file
#   scripts/audit-node-externals.sh --test     # runs self-test unit tests
#   SBOM_OUTPUT=path.json scripts/audit-node-externals.sh ...

REPORT_ONLY=0
RECORD_BASELINE=0
SBOM_OUTPUT="${SBOM_OUTPUT:-sbom-node-externals.cdx.json}"
BASELINE_FILE="${BASELINE_FILE:-supply-chain/node-baseline.json}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --report-only) REPORT_ONLY=1; shift ;;
    --record-baseline) RECORD_BASELINE=1; shift ;;
    --baseline) BASELINE_FILE="$2"; shift 2 ;;
    --sbom-output) SBOM_OUTPUT="$2"; shift 2 ;;
    --test)
      python3 - <<'PY'
import math, sys

def parse_cvss3_base_score(vector_str):
    if not isinstance(vector_str, str) or not vector_str.startswith("CVSS:3."):
        return None
    parts = vector_str.split("/")
    metrics = {}
    for part in parts[1:]:
        if ":" in part:
            k, v = part.split(":", 1)
            metrics[k] = v
    required = ["AV", "AC", "PR", "UI", "S", "C", "I", "A"]
    if not all(k in metrics for k in required):
        return None
    av_map = {"N": 0.85, "A": 0.62, "L": 0.55, "P": 0.2}
    ac_map = {"L": 0.77, "H": 0.44}
    ui_map = {"N": 0.85, "R": 0.62}
    s_val = metrics["S"]
    if s_val == "U":
        pr_map = {"N": 0.85, "L": 0.62, "H": 0.27}
    elif s_val == "C":
        pr_map = {"N": 0.85, "L": 0.68, "H": 0.50}
    else:
        return None
    cia_map = {"N": 0.0, "L": 0.22, "H": 0.56}
    av = av_map.get(metrics["AV"])
    ac = ac_map.get(metrics["AC"])
    pr = pr_map.get(metrics["PR"])
    ui = ui_map.get(metrics["UI"])
    c = cia_map.get(metrics["C"])
    i = cia_map.get(metrics["I"])
    a = cia_map.get(metrics["A"])
    if any(x is None for x in [av, ac, pr, ui, c, i, a]):
        return None
    iss = 1.0 - ((1.0 - c) * (1.0 - i) * (1.0 - a))
    if s_val == "U":
        impact = 6.42 * iss
    else:
        impact = 7.52 * (iss - 0.029) - 3.25 * ((iss - 0.02) ** 15)
    exploitability = 8.22 * av * ac * pr * ui
    if impact <= 0:
        return 0.0
    int_val = round((impact + exploitability if s_val == "U" else 1.08 * (impact + exploitability)) * 100000)
    if int_val > 1000000:
        return 10.0
    if int_val % 10000 == 0:
        return int_val / 100000.0
    else:
        return (math.floor(int_val / 10000) + 1) / 10.0

def parse_cvss_score(score_entry):
    if not score_entry:
        return None
    if isinstance(score_entry, (int, float)):
        return float(score_entry)
    if isinstance(score_entry, dict):
        return parse_cvss_score(score_entry.get("score"))
    if isinstance(score_entry, str):
        try:
            return float(score_entry)
        except ValueError:
            pass
        if score_entry.startswith("CVSS:3."):
            return parse_cvss3_base_score(score_entry)
    return None

def is_high_or_critical(db_sev, cvss_scores):
    db_sev = (db_sev or "").upper()
    if db_sev in ("HIGH", "CRITICAL"):
        return True
    if db_sev in ("LOW", "MODERATE", "MEDIUM"):
        return False
    for entry in cvss_scores:
        score = parse_cvss_score(entry)
        if score is not None:
            if score >= 7.0:
                return True
        else:
            if isinstance(entry, str) and (entry.startswith("CVSS:") or entry.startswith("AV:")):
                return True
            if isinstance(entry, dict) and isinstance(entry.get("score"), str):
                s = entry.get("score", "")
                if s.startswith("CVSS:") or s.startswith("AV:"):
                    return True
    return False

# 1. High-scoring vector without H metrics (base score 7.3)
assert is_high_or_critical("", ["CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:L/I:L/A:L"]) is True, "High without H failed"
# 2. Low-scoring vector with H metric (base score 3.8)
assert is_high_or_critical("", ["CVSS:3.1/AV:P/AC:H/PR:H/UI:R/S:U/C:H/I:N/A:N"]) is False, "Low with H failed"
# 3. Medium-scoring vector with H metric (base score 4.2)
assert is_high_or_critical("", ["CVSS:3.1/AV:L/AC:H/PR:H/UI:R/S:U/C:H/I:N/A:N"]) is False, "Medium with H failed"
# 4. Medium-scoring vector with A:H (base score 4.4)
assert is_high_or_critical("", ["CVSS:3.1/AV:N/AC:H/PR:H/UI:N/S:U/C:N/I:N/A:H"]) is False, "Medium with A:H failed"
# 5. Direct numeric scores
assert is_high_or_critical("", [8.5]) is True, "Direct score >= 7.0 failed"
assert is_high_or_critical("", [4.5]) is False, "Direct score < 7.0 failed"
# 6. Explicit database_specific overrides
assert is_high_or_critical("HIGH", ["CVSS:3.1/AV:P/AC:H/PR:H/UI:R/S:U/C:N/I:N/A:N"]) is True, "db_sev HIGH override failed"
assert is_high_or_critical("LOW", ["CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H"]) is False, "db_sev LOW override failed"
# 7. Fail closed on unparseable CVSS strings
assert is_high_or_critical("", ["CVSS:unknown_future_format"]) is True, "Fail closed failed"
print("audit-node-externals.sh self-test: ALL CVSS TESTS PASSED")
PY
      exit 0
      ;;
    --help|-h)
      echo "Usage: $0 [--report-only] [--record-baseline] [--baseline PATH] [--sbom-output PATH] [--test]"
      exit 0
      ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VERSIONS_TOML="$ROOT_DIR/versions.toml"

if [[ ! -f "$VERSIONS_TOML" ]]; then
  echo "versions.toml not found at $VERSIONS_TOML" >&2
  exit 1
fi

# --- parse versions.toml ----------------------------------------------------
# Expected lines: node20_externals_version = "20.19.0"
#                 node24_externals_version = "24.3.0"
parse_version() {
  local key="$1"
  local val
  val="$(grep -E "^${key}[[:space:]]*=" "$VERSIONS_TOML" | head -n1 | sed -E 's/.*"[[:space:]]*([^"]+)[[:space:]]*".*/\1/')"
  if [[ -z "$val" ]]; then
    echo "failed to parse $key from $VERSIONS_TOML" >&2
    exit 1
  fi
  echo "$val"
}

NODE20_VERSION="$(parse_version node20_externals_version)"
NODE24_VERSION="$(parse_version node24_externals_version)"
echo "Node externals pins: node20=$NODE20_VERSION node24=$NODE24_VERSION"

# --- temp workspace ---------------------------------------------------------
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

# Collect all package.json name@version entries here
PACKAGES_JSONL="$TMPDIR/packages.jsonl"
: > "$PACKAGES_JSONL"

# Collect unique components for SBOM (dedup by name@version)
SBOM_COMPONENTS_TMP="$TMPDIR/sbom_components.jsonl"
: > "$SBOM_COMPONENTS_TMP"

# --- helpers ----------------------------------------------------------------
fetch_shasums() {
  local version="$1"
  local url="https://nodejs.org/dist/v${version}/SHASUMS256.txt"
  echo "Fetching $url" >&2
  curl -fsSL --retry 3 --retry-all-errors "$url" -o "$TMPDIR/SHASUMS256-${version}.txt"
}

download_and_verify() {
  local version="$1"
  local archive="node-v${version}-linux-x64.tar.gz"
  local url="https://nodejs.org/dist/v${version}/${archive}"
  local dest="$TMPDIR/${archive}"

  echo "Downloading $url" >&2
  curl -fsSL --retry 3 --retry-all-errors "$url" -o "$dest"

  local shasums="$TMPDIR/SHASUMS256-${version}.txt"
  local expected
  expected="$(grep -F "  ${archive}" "$shasums" | awk '{print $1}' || true)"
  if [[ -z "$expected" ]]; then
    echo "SHA256 line for $archive not found in SHASUMS256.txt" >&2
    exit 1
  fi
  local actual
  actual="$(sha256sum "$dest" | awk '{print $1}')"
  if [[ "$expected" != "$actual" ]]; then
    echo "SHA-256 mismatch for $archive" >&2
    echo "  expected: $expected" >&2
    echo "  actual:   $actual" >&2
    exit 1
  fi
  echo "SHA-256 verified for $archive: $actual" >&2
  echo "$dest"
}

enumerate_packages() {
  local version="$1"

  # Record Node runtime itself for query & SBOM
  printf 'node\t%s\truntime\n' "$version" >> "$PACKAGES_JSONL"

  local archive_path="$2"
  local extract_root="$TMPDIR/extract-${version}"
  mkdir -p "$extract_root"
  echo "Extracting $archive_path -> $extract_root" >&2
  tar -xzf "$archive_path" -C "$extract_root" --strip-components=1

  local lib_root="$extract_root/lib/node_modules"
  if [[ ! -d "$lib_root" ]]; then
    echo "lib/node_modules not found in $archive_path" >&2
    exit 1
  fi

  # Enumerate every package.json under lib/node_modules (covers npm/node_modules nested trees)
  local count=0
  while IFS= read -r -d '' pkg_json; do
    # Extract name and version via python3 (jq-free; python is everywhere)
    local nv
    nv="$(python3 -c '
import json, sys
try:
    d=json.load(open(sys.argv[1]))
    name=d.get("name")
    ver=d.get("version")
    if name and ver:
        print(f"{name}\t{ver}")
except Exception as e:
    pass
' "$pkg_json" 2>/dev/null || true)"
    if [[ -z "$nv" ]]; then
      continue
    fi
    local name ver
    name="$(echo "$nv" | cut -f1)"
    ver="$(echo "$nv" | cut -f2)"
    if [[ -z "$name" || -z "$ver" ]]; then
      continue
    fi
    # record for OSV
    printf '%s\t%s\t%s\n' "$name" "$ver" "$pkg_json" >> "$PACKAGES_JSONL"
    count=$((count + 1))
  done < <(find "$lib_root" -name "package.json" -print0)

  echo "Enumerated $count package.json files for node v${version}" >&2
  echo "$count"
}

# --- main download + enumeration -------------------------------------------
for ver in "$NODE20_VERSION" "$NODE24_VERSION"; do
  fetch_shasums "$ver"
done

TOTAL_PKGS=0
for ver in "$NODE20_VERSION" "$NODE24_VERSION"; do
  archive_path="$(download_and_verify "$ver")"
  cnt="$(enumerate_packages "$ver" "$archive_path")"
  TOTAL_PKGS=$((TOTAL_PKGS + cnt))
done

echo "Total package.json occurrences: $TOTAL_PKGS"

# Deduplicate to unique name@version for OSV + SBOM
# Build queries for OSV and SBOM components in one python pass
DEDUP_JSON="$TMPDIR/dedup.json"
python3 - "$PACKAGES_JSONL" "$DEDUP_JSON" "$SBOM_COMPONENTS_TMP" <<'PY'
import sys
from pathlib import Path

packages_path = Path(sys.argv[1])
dedup_path = Path(sys.argv[2])
sbom_tmp = Path(sys.argv[3])

seen = {}
# sbom dedup
sbom_seen = set()

for line in packages_path.read_text().splitlines():
    if not line.strip():
        continue
    parts = line.split("\t")
    if len(parts) < 2:
        continue
    name, ver = parts[0], parts[1]
    key = f"{name}@{ver}"
    if key not in seen:
        seen[key] = (name, ver)
    if key not in sbom_seen:
        sbom_seen.add(key)
        sbom_tmp.write_text(sbom_tmp.read_text() + f"{name}\t{ver}\n" if sbom_tmp.exists() else f"{name}\t{ver}\n") if False else None

# write dedup json for OSV batching
import json
out = [{"name": n, "version": v} for (n, v) in seen.values()]
dedup_path.write_text(json.dumps(out))

# write sbom components temp (overwrite with correct content)
lines = []
for key in sorted(sbom_seen):
    name, ver = key.rsplit("@", 1)
    lines.append(f"{name}\t{ver}")
# Use second tmp path for sbom list
sbom_list = Path(str(sbom_tmp) + ".list")
sbom_list.write_text("\n".join(lines) + "\n" if lines else "")
# Move to expected path
import shutil
if sbom_list.exists():
    shutil.move(str(sbom_list), str(sbom_tmp))
PY

# Count unique
UNIQUE_COUNT="$(python3 -c "import json; print(len(json.load(open('$DEDUP_JSON'))))")"
echo "Unique npm packages: $UNIQUE_COUNT"

# --- OSV querybatch ---------------------------------------------------------
# Build querybatch payloads (chunk 500 to stay under 1000 limit and avoid huge request)
OSV_RESULTS="$TMPDIR/osv_results.jsonl"
: > "$OSV_RESULTS"

python3 - "$DEDUP_JSON" "$TMPDIR" <<'PY'
import json
from pathlib import Path
import subprocess
import sys

dedup = json.loads(Path(sys.argv[1]).read_text())
tmpdir = Path(sys.argv[2])

# Chunk size 500
chunk_size = 500
chunks = [dedup[i:i+chunk_size] for i in range(0, len(dedup), chunk_size)]

all_results = []  # list of (name, version, result)
for idx, chunk in enumerate(chunks):
    queries = [{"package": {"name": p["name"], "ecosystem": "npm"}, "version": p["version"]} for p in chunk]
    payload = json.dumps({"queries": queries})
    payload_path = tmpdir / f"osv_payload_{idx}.json"
    payload_path.write_text(payload)
    # curl
    result_path = tmpdir / f"osv_result_{idx}.json"
    curl_cmd = [
        "curl", "-fsSL", "--retry", "3", "--retry-all-errors",
        "-H", "Content-Type: application/json",
        "-X", "POST", "https://api.osv.dev/v1/querybatch",
        "-d", f"@{payload_path}"
    ]
    try:
        subprocess.check_call(curl_cmd, stdout=open(result_path, "wb"))
    except subprocess.CalledProcessError as e:
        print(f"OSV querybatch failed for chunk {idx}: {e}", file=sys.stderr)
        sys.exit(1)
    data = json.loads(result_path.read_text())
    results = data.get("results", [])
    # sanity
    if len(results) != len(chunk):
        print(f"OSV result length mismatch chunk {idx}: {len(results)} vs {len(chunk)}", file=sys.stderr)
        sys.exit(1)
    for pkg, res in zip(chunk, results):
        all_results.append((pkg, res))

# Write aggregated results as jsonl: each line {name, version, vulns: [ids]}
out_path = tmpdir / "osv_results.jsonl"
with open(out_path, "w") as f:
    for pkg, res in all_results:
        vulns = res.get("vulns") or []
        # vulns entries have id, modified
        f.write(json.dumps({"name": pkg["name"], "version": pkg["version"], "vulns": vulns}) + "\n")

# Also write distinct vuln ids
distinct_ids = set()
for _, res in all_results:
    for v in (res.get("vulns") or []):
        distinct_ids.add(v.get("id"))
(tmpdir / "osv_distinct_ids.txt").write_text("\n".join(sorted(distinct_ids)) + ("\n" if distinct_ids else ""))

print(f"OSV querybatch complete: {len(all_results)} queries, {len(distinct_ids)} distinct vulns", file=sys.stderr)
PY

DISTINCT_IDS_FILE="$TMPDIR/osv_distinct_ids.txt"
DISTINCT_COUNT="$(wc -l < "$DISTINCT_IDS_FILE" | tr -d ' ')"
echo "OSV: $DISTINCT_COUNT distinct vulnerability IDs found"

# --- Fetch vuln details and classify severity -------------------------------
# For each distinct vuln ID, fetch https://api.osv.dev/v1/vulns/<id>
# We do this with python to parse severity reliably.

VULN_DETAILS_DIR="$TMPDIR/vuln_details"
mkdir -p "$VULN_DETAILS_DIR"

if [[ "$DISTINCT_COUNT" -gt 0 ]]; then
  python3 - "$DISTINCT_IDS_FILE" "$VULN_DETAILS_DIR" <<'PY'
import json
from pathlib import Path
import subprocess
import sys

ids = [l.strip() for l in Path(sys.argv[1]).read_text().splitlines() if l.strip()]
outdir = Path(sys.argv[2])

for vid in ids:
    out = outdir / f"{vid}.json"
    # Try curl with retries
    try:
        subprocess.check_call([
            "curl", "-fsSL", "--retry", "3", "--retry-all-errors",
            f"https://api.osv.dev/v1/vulns/{vid}"
        ], stdout=open(out, "wb"))
    except subprocess.CalledProcessError as e:
        print(f"failed to fetch vuln {vid}: {e}", file=sys.stderr)
        sys.exit(1)

print(f"Fetched {len(ids)} vuln details", file=sys.stderr)
PY
fi

# Now classify and print findings
FINDINGS_TXT="$TMPDIR/findings.txt"
python3 - "$TMPDIR/osv_results.jsonl" "$VULN_DETAILS_DIR" "$FINDINGS_TXT" "$PACKAGES_JSONL" "$ROOT_DIR/$BASELINE_FILE" "$RECORD_BASELINE" "$NODE20_VERSION" "$NODE24_VERSION" <<'PY'
import json
from pathlib import Path
import datetime
import math
import sys

results_path = Path(sys.argv[1])
details_dir = Path(sys.argv[2])
findings_out = Path(sys.argv[3])
packages_jsonl = Path(sys.argv[4])
baseline_path = Path(sys.argv[5])
record_baseline = sys.argv[6] == "1"
node20_ver = sys.argv[7]
node24_ver = sys.argv[8]

# Load vuln severity map
severity_map = {}  # id -> (severity_str, summary, aliases, cvss_scores)
for p in details_dir.glob("*.json"):
    try:
        data = json.loads(p.read_text())
        vid = data.get("id", p.stem)
        db_sev = (data.get("database_specific", {}) or {}).get("severity", "") or ""
        db_sev = db_sev.upper()
        # severity array scores
        sev_arr = data.get("severity") or []
        cvss_scores = [s.get("score","") for s in sev_arr]
        summary = data.get("summary") or data.get("details","")[:120] or ""
        aliases = data.get("aliases") or []
        severity_map[vid] = (db_sev, summary, aliases, cvss_scores, data)
    except Exception as e:
        print(f"failed to parse {p}: {e}", file=sys.stderr)
        sys.exit(1)

def parse_cvss3_base_score(vector_str):
    """Computes CVSS v3.0 / v3.1 base score from vector string (returns float 0.0 to 10.0)."""
    if not isinstance(vector_str, str) or not vector_str.startswith("CVSS:3."):
        return None
    parts = vector_str.split("/")
    metrics = {}
    for part in parts[1:]:
        if ":" in part:
            k, v = part.split(":", 1)
            metrics[k] = v
    required = ["AV", "AC", "PR", "UI", "S", "C", "I", "A"]
    if not all(k in metrics for k in required):
        return None
    av_map = {"N": 0.85, "A": 0.62, "L": 0.55, "P": 0.2}
    ac_map = {"L": 0.77, "H": 0.44}
    ui_map = {"N": 0.85, "R": 0.62}
    s_val = metrics["S"]
    if s_val == "U":
        pr_map = {"N": 0.85, "L": 0.62, "H": 0.27}
    elif s_val == "C":
        pr_map = {"N": 0.85, "L": 0.68, "H": 0.50}
    else:
        return None
    cia_map = {"N": 0.0, "L": 0.22, "H": 0.56}
    av = av_map.get(metrics["AV"])
    ac = ac_map.get(metrics["AC"])
    pr = pr_map.get(metrics["PR"])
    ui = ui_map.get(metrics["UI"])
    c = cia_map.get(metrics["C"])
    i = cia_map.get(metrics["I"])
    a = cia_map.get(metrics["A"])
    if any(x is None for x in [av, ac, pr, ui, c, i, a]):
        return None
    iss = 1.0 - ((1.0 - c) * (1.0 - i) * (1.0 - a))
    if s_val == "U":
        impact = 6.42 * iss
    else:
        impact = 7.52 * (iss - 0.029) - 3.25 * ((iss - 0.02) ** 15)
    exploitability = 8.22 * av * ac * pr * ui
    if impact <= 0:
        return 0.0
    int_val = round((impact + exploitability if s_val == "U" else 1.08 * (impact + exploitability)) * 100000)
    if int_val > 1000000:
        return 10.0
    if int_val % 10000 == 0:
        return int_val / 100000.0
    else:
        return (math.floor(int_val / 10000) + 1) / 10.0

def parse_cvss_score(score_entry):
    """Extracts or calculates numeric CVSS score from an OSV entry (returns float or None)."""
    if not score_entry:
        return None
    if isinstance(score_entry, (int, float)):
        return float(score_entry)
    if isinstance(score_entry, dict):
        return parse_cvss_score(score_entry.get("score"))
    if isinstance(score_entry, str):
        try:
            return float(score_entry)
        except ValueError:
            pass
        if score_entry.startswith("CVSS:3."):
            return parse_cvss3_base_score(score_entry)
    return None

def is_high_or_critical(db_sev, cvss_scores):
    db_sev = (db_sev or "").upper()
    if db_sev in ("HIGH", "CRITICAL"):
        return True
    if db_sev in ("LOW", "MODERATE", "MEDIUM"):
        return False
    # When database_specific.severity is absent, inspect and compute CVSS scores
    for entry in cvss_scores:
        score = parse_cvss_score(entry)
        if score is not None:
            if score >= 7.0:
                return True
        else:
            # Fail closed on unrecognized/unparseable CVSS string vectors
            if isinstance(entry, str) and (entry.startswith("CVSS:") or entry.startswith("AV:")):
                return True
            if isinstance(entry, dict) and isinstance(entry.get("score"), str):
                s = entry.get("score", "")
                if s.startswith("CVSS:") or s.startswith("AV:"):
                    return True
    return False

# Load existing baseline if available
known_baseline_ids = set()
if baseline_path.exists() and not record_baseline:
    try:
        bdata = json.loads(baseline_path.read_text())
        known_baseline_ids = set(bdata.get("known_advisories", []))
    except Exception as e:
        print(f"warning: failed to read baseline from {baseline_path}: {e}", file=sys.stderr)

# Map package -> vulns
high_critical = []
new_high_critical = []
all_findings = []
all_distinct_ids = set()

with open(results_path) as f:
    for line in f:
        rec = json.loads(line)
        name = rec["name"]
        version = rec["version"]
        vulns = rec.get("vulns") or []
        for v in vulns:
            vid = v.get("id")
            if vid:
                all_distinct_ids.add(vid)
            sev, summary, aliases, cvss, data = severity_map.get(vid, ("", "", [], [], {}))
            for a in aliases:
                all_distinct_ids.add(a)

            is_hc = is_high_or_critical(sev, cvss)
            # Check if this vuln or any alias is in baseline
            in_baseline = (vid in known_baseline_ids) or any(a in known_baseline_ids for a in aliases)
            all_findings.append((name, version, vid, sev or "UNKNOWN", summary, is_hc, in_baseline))
            if is_hc:
                high_critical.append((name, version, vid, sev))
                if not in_baseline:
                    new_high_critical.append((name, version, vid, sev))

# If recording baseline, dump current distinct IDs
if record_baseline:
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
        "node20_version": node20_ver,
        "node24_version": node24_ver,
        "description": "Baseline snapshot of known upstream OSV advisories for pinned Node distributions.",
        "total_advisories": len(all_distinct_ids),
        "known_advisories": sorted(all_distinct_ids)
    }
    baseline_path.write_text(json.dumps(baseline_payload, indent=2) + "\n")
    print(f"Recorded baseline with {len(all_distinct_ids)} advisory IDs to {baseline_path}", file=sys.stderr)

# Sort for stable output
all_findings.sort(key=lambda x: (x[0], x[1], x[2], x[6]))

# Write findings
with open(findings_out, "w") as out:
    out.write(f"OSV findings: {len(all_findings)} total advisories across packages\n")
    if known_baseline_ids:
        out.write(f"Active baseline: {len(known_baseline_ids)} known advisories from {baseline_path.name}\n")
    if not all_findings:
        out.write("No vulnerabilities found.\n")
    else:
        out.write(f"{'Package':<30} {'Version':<12} {'Vuln ID':<28} {'Severity':<10} {'Status':<12} Summary\n")
        out.write("-"*130 + "\n")
        for name, ver, vid, sev, summary, is_hc, in_baseline in all_findings:
            if is_hc and not in_baseline:
                flag = "[NEW HIGH]"
            elif is_hc and in_baseline:
                flag = "[BASELINE]"
            elif in_baseline:
                flag = "[BASELINE]"
            else:
                flag = ""
            # trim summary to 60 chars
            summ = summary.replace("\n"," ")[:60]
            out.write(f"{name:<30} {ver:<12} {vid:<28} {sev:<10} {flag:<12} {summ}\n")
        out.write("\n")
        out.write(f"High/Critical count: {len(high_critical)} ({len(high_critical) - len(new_high_critical)} in baseline, {len(new_high_critical)} new)\n")
        if new_high_critical:
            out.write("NEW High/Critical advisories (not in baseline):\n")
            for name, ver, vid, sev in new_high_critical:
                out.write(f"  - {name}@{ver}: {vid} ({sev})\n")

# Also emit counts for shell
Path(str(findings_out) + ".counts").write_text(json.dumps({
    "total": len(all_findings),
    "high_critical": len(high_critical),
    "new_high_critical": len(new_high_critical)
}))

PY

cat "$FINDINGS_TXT"
COUNTS_JSON="$FINDINGS_TXT.counts"
TOTAL_FINDINGS="$(python3 -c "import json; print(json.load(open('$COUNTS_JSON'))['total'])")"
HIGH_CRITICAL_COUNT="$(python3 -c "import json; print(json.load(open('$COUNTS_JSON'))['high_critical'])")"
NEW_HIGH_CRITICAL_COUNT="$(python3 -c "import json; print(json.load(open('$COUNTS_JSON'))['new_high_critical'])")"

echo ""
echo "Summary: $TOTAL_FINDINGS advisories ($HIGH_CRITICAL_COUNT high/critical total, $NEW_HIGH_CRITICAL_COUNT new)"

# --- Generate CycloneDX 1.5 SBOM -------------------------------------------
# SBOM includes all unique npm packages from both node distributions.
python3 - "$SBOM_COMPONENTS_TMP" "$SBOM_OUTPUT" "$NODE20_VERSION" "$NODE24_VERSION" <<'PY'
import json
from pathlib import Path
import datetime
import hashlib
import uuid
import sys

components_tmp = Path(sys.argv[1])
sbom_out = Path(sys.argv[2])
node20 = sys.argv[3]
node24 = sys.argv[4]

# Read unique packages
packages = []
if components_tmp.exists():
    for line in components_tmp.read_text().splitlines():
        if not line.strip():
            continue
        name, ver = line.split("\t", 1)
        packages.append((name, ver))

# Deduplicate again just in case, sort for stability
uniq = {}
for n, v in packages:
    uniq[f"{n}@{v}"] = (n, v)
packages = sorted(uniq.values(), key=lambda x: (x[0].lower(), x[1]))

# Build CycloneDX 1.5
now = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
bom_serial = f"urn:uuid:{uuid.uuid4()}"

components = []

# Add top-level Node.js runtimes as platform components
for node_ver in [node20, node24]:
    purl = f"pkg:generic/node@{node_ver}"
    components.append({
        "type": "platform",
        "name": "node",
        "version": node_ver,
        "purl": purl,
        "bom-ref": purl,
        "scope": "required",
        "description": f"Node.js runtime distribution v{node_ver}"
    })

for name, ver in packages:
    if name == "node":
        continue
    # purl for npm: pkg:npm/<name>@<version>
    # Handle scoped packages @scope/name -> pkg:npm/%40scope/name
    purl_name = name.replace("@", "%40")
    purl = f"pkg:npm/{purl_name}@{ver}"
    # bom-ref should be unique; use purl as ref
    components.append({
        "type": "library",
        "name": name,
        "version": ver,
        "purl": purl,
        "bom-ref": purl,
        "scope": "required"
    })

sbom = {
    "bomFormat": "CycloneDX",
    "specVersion": "1.5",
    "serialNumber": bom_serial,
    "version": 1,
    "metadata": {
        "timestamp": now,
        "tools": {
            "components": [
                {
                    "type": "application",
                    "name": "audit-node-externals.sh",
                    "version": "1.0.0"
                }
            ]
        },
        "component": {
            "type": "application",
            "name": "node-externals",
            "version": f"{node20}+{node24}",
            "description": f"Node.js externals bundled with node {node20} and {node24} (lib/node_modules)"
        },
        "lifecycles": [{"phase": "build"}]
    },
    "components": components
}

# Write with pretty print for readability
sbom_out.write_text(json.dumps(sbom, indent=2) + "\n")
print(f"SBOM written to {sbom_out} with {len(components)} components", file=sys.stderr)

# Validate JSON
json.loads(sbom_out.read_text())
PY

# Resolve SBOM_OUTPUT to absolute path for workflow artifact logging
if [[ "$SBOM_OUTPUT" != /* ]]; then
  SBOM_ABS="$ROOT_DIR/$SBOM_OUTPUT"
else
  SBOM_ABS="$SBOM_OUTPUT"
fi
echo "SBOM artifact: $SBOM_ABS"

# --- Gate decision ----------------------------------------------------------
if [[ "$REPORT_ONLY" -eq 1 ]]; then
  echo "Report-only mode: not failing on high/critical (found $HIGH_CRITICAL_COUNT total, $NEW_HIGH_CRITICAL_COUNT new)"
  exit 0
fi

if [[ "$RECORD_BASELINE" -eq 1 ]]; then
  echo "Recorded baseline snapshot to $BASELINE_FILE: PASS"
  exit 0
fi

if [[ "$NEW_HIGH_CRITICAL_COUNT" -gt 0 ]]; then
  echo "FAIL: $NEW_HIGH_CRITICAL_COUNT new high/critical vulnerabilities found (not in $BASELINE_FILE) — see above" >&2
  exit 1
fi

echo "PASS: no new high/critical vulnerabilities (all known issues documented in $BASELINE_FILE)"
exit 0
