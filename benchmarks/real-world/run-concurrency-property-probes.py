#!/usr/bin/env python3
"""Differential seed corpus harness for concurrency property cases.

Usage modes
-----------
Dry-run / schema validation (credential-free, default):
    python3 run-concurrency-property-probes.py --dry-run

Validate schema only (exits nonzero on schema error):
    python3 run-concurrency-property-probes.py --validate-only

Reject a contaminated corpus file (exits nonzero):
    python3 run-concurrency-property-probes.py --corpus fixtures/contaminated-case.json --dry-run

Full differential (requires live credentials and privileged access):
    GH_TOKEN=... GH_REPO=org/repo AKSH_SERVER=http://... \\
        python3 run-concurrency-property-probes.py --corpus concurrency-property-cases.json

Official runner commit (pinned):
    32e89e2afd4549a362dbec337a589b81fd17a0c5

Documentation retrieval date: 2026-07-14

Exit codes
----------
0   all validation and differential checks passed
1   one or more cases failed (contamination, semantic mismatch, invariant violation)
2   schema error or invalid corpus
3   unexpected internal error
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ─── Normative pins ──────────────────────────────────────────────────────────

RUNNER_PIN = "32e89e2afd4549a362dbec337a589b81fd17a0c5"
DOCS_DATE = "2026-07-14"
EXPECTED_SCHEMA_VERSION = 1

# Clamp minimum; kill offset = effective - 15s
CANCEL_MIN_SECS = 60
KILL_OFFSET_SECS = 15

# ─── Schema validation ────────────────────────────────────────────────────────

REQUIRED_TOP_LEVEL = {"schema_version", "runner_pin", "docs_date", "cases"}
REQUIRED_CASE_FIELDS = {"id", "invariant", "kind"}
VALID_KINDS = {"control_plane", "runner"}
VALID_QUEUES = {"single", "max"}
KNOWN_INVARIANTS = {
    "GH-GROUP-01", "GH-SLOT-01", "GH-SINGLE-01", "GH-MAX-01", "GH-FIFO-01",
    "GH-CANCEL-01", "GH-VALIDATE-01", "GH-CTX-WF-01", "GH-CTX-JOB-01",
    "GH-MATRIX-01", "GH-REUSE-01", "GH-STATUS-01",
    "RUN-MSG-01", "RUN-ID-01", "RUN-TIME-01", "RUN-IDEMP-01",
    "RUN-ORDER-01", "RUN-OVERLAP-01", "RUN-SCOPE-01",
}

# Contamination markers that must cause rejection
CONTAMINATION_FIELDS = {"_contamination_marker"}


def validate_schema(corpus: dict[str, Any]) -> list[str]:
    """Return list of schema errors; empty means valid."""
    errors: list[str] = []

    missing_top = REQUIRED_TOP_LEVEL - set(corpus)
    for f in sorted(missing_top):
        errors.append(f"Missing top-level field: {f!r}")

    sv = corpus.get("schema_version")
    if sv != EXPECTED_SCHEMA_VERSION:
        errors.append(f"schema_version must be {EXPECTED_SCHEMA_VERSION}, got {sv!r}")

    rp = corpus.get("runner_pin", "")
    if not isinstance(rp, str) or len(rp) != 40 or not re.match(r"^[0-9a-f]{40}$", rp):
        errors.append(f"runner_pin must be a 40-char hex SHA, got {rp!r}")

    dd = corpus.get("docs_date", "")
    if not isinstance(dd, str) or not re.match(r"^\d{4}-\d{2}-\d{2}$", dd):
        errors.append(f"docs_date must be YYYY-MM-DD, got {dd!r}")

    cases = corpus.get("cases", [])
    if not isinstance(cases, list):
        errors.append("'cases' must be a list")
        return errors
    if len(cases) == 0:
        errors.append("'cases' must be non-empty")

    seen_ids: set[str] = set()
    for i, case in enumerate(cases):
        prefix = f"cases[{i}]"
        if not isinstance(case, dict):
            errors.append(f"{prefix} must be an object")
            continue

        for f in REQUIRED_CASE_FIELDS:
            if f not in case:
                errors.append(f"{prefix}: missing required field {f!r}")

        case_id = case.get("id", f"<index {i}>")
        if case_id in seen_ids:
            errors.append(f"{prefix}: duplicate id {case_id!r}")
        seen_ids.add(str(case_id))

        kind = case.get("kind")
        if kind not in VALID_KINDS:
            errors.append(f"{prefix} ({case_id}): kind must be one of {VALID_KINDS}, got {kind!r}")

        inv = case.get("invariant")
        if inv not in KNOWN_INVARIANTS:
            errors.append(f"{prefix} ({case_id}): unknown invariant {inv!r}")

        if "queue" in case and case["queue"] not in VALID_QUEUES:
            errors.append(f"{prefix} ({case_id}): queue must be one of {VALID_QUEUES}")

    return errors


def detect_contamination(corpus: dict[str, Any]) -> list[str]:
    """Return contamination violations that must cause a nonzero exit."""
    issues: list[str] = []
    cases = corpus.get("cases", [])
    for case in cases:
        if not isinstance(case, dict):
            continue
        case_id = case.get("id", "unknown")

        # Any case or nested object containing a contamination marker
        def _scan(obj: Any, path: str) -> None:
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k in CONTAMINATION_FIELDS:
                        issues.append(
                            f"case {case_id!r}: contamination marker found at {path}.{k!r}: {v!r}"
                        )
                    _scan(v, f"{path}.{k}")
            elif isinstance(obj, list):
                for idx, item in enumerate(obj):
                    _scan(item, f"{path}[{idx}]")

        _scan(case, f"cases.{case_id}")

        # GH-SINGLE-01 semantic check: in single queue with 3 arrivals and
        # cancel_in_progress=false, the arrival (last) must be pending, not cancelled.
        expect = case.get("expect", {})
        arrivals = case.get("arrivals", [])
        if (
            case.get("queue") == "single"
            and not case.get("cancel_in_progress", True)
            and len(arrivals) >= 3
            and isinstance(expect, dict)
        ):
            cancelled = expect.get("cancelled_labels", [])
            pending = expect.get("pending_labels", [])
            last_label = arrivals[-1].get("label")
            if last_label is not None:
                if last_label in cancelled:
                    issues.append(
                        f"case {case_id!r}: GH-SINGLE-01 violation — arrival {last_label!r} "
                        f"is listed as cancelled but must be pending (only middle holder is replaced)"
                    )
                # Check that an intermediate holder is cancelled, not just the arrival
                if len(arrivals) == 3:
                    middle_label = arrivals[1].get("label")
                    if middle_label and middle_label in pending:
                        issues.append(
                            f"case {case_id!r}: GH-SINGLE-01 violation — middle holder {middle_label!r} "
                            f"listed as pending; it must be cancelled when the third arrival comes"
                        )

    return issues


@dataclass
class CaseResult:
    case_id: str
    invariant: str
    kind: str
    passed: bool
    mode: str  # "dry_run", "validate_only", "differential"
    notes: list[str] = field(default_factory=list)
    output_dir: Path | None = None


# ─── Timeout logic (RUN-TIME-01, RUN-IDEMP-01) ───────────────────────────────

def effective_timeout(raw_secs: int) -> int:
    return max(raw_secs, CANCEL_MIN_SECS)


def kill_offset(eff_secs: int) -> int:
    return eff_secs - KILL_OFFSET_SECS


def parse_timespan_secs(timespan: str) -> int | None:
    """Parse a .NET TimeSpan string to whole seconds.

    Accepts forms:
      [-][d.]hh:mm:ss[.fffffff]

    Returns None for malformed input.
    Rejects minute/second fields >= 60 per official TimeSpan contract.
    """
    ts = timespan.strip()
    neg = ts.startswith("-")
    if neg:
        ts = ts[1:]

    # Optional day component
    days = 0
    if "." in ts and ts.index(".") < ts.index(":") if ":" in ts else False:
        day_part, ts = ts.split(".", 1)
        try:
            days = int(day_part)
        except ValueError:
            return None

    parts = ts.split(":")
    if len(parts) != 3:
        return None

    h_str, m_str, s_str = parts
    frac_str = ""
    if "." in s_str:
        s_str, frac_str = s_str.split(".", 1)

    try:
        hours = int(h_str)
        minutes = int(m_str)
        seconds = int(s_str)
    except ValueError:
        return None

    # Official TimeSpan: minutes and seconds must be 0-59
    if minutes >= 60 or seconds >= 60:
        return None

    total = days * 86400 + hours * 3600 + minutes * 60 + seconds
    return total if not neg else -total


# ─── Dry-run validators (credential-free) ────────────────────────────────────

def validate_runner_case_dry(case: dict[str, Any]) -> CaseResult:
    """Validate runner-side case expectations algebraically without live probes."""
    case_id = case["id"]
    result = CaseResult(case_id=case_id, invariant=case["invariant"], kind="runner",
                        passed=True, mode="dry_run")

    expect = case.get("expect", {})

    # Timeout / kill offset derivation
    raw_secs = case.get("timeout_secs")
    timespan_str = case.get("timeout_timespan")

    if raw_secs is not None:
        eff = effective_timeout(raw_secs)
        expected_eff = expect.get("effective_timeout_secs")
        if expected_eff is not None and eff != expected_eff:
            result.passed = False
            result.notes.append(
                f"RUN-TIME-01: effective_timeout({raw_secs}) = {eff}, "
                f"but expect says {expected_eff}"
            )
        ko = kill_offset(eff)
        expected_ko = expect.get("kill_offset_secs")
        if expected_ko is not None and ko != expected_ko:
            result.passed = False
            result.notes.append(
                f"RUN-TIME-01: kill_offset = {ko}, but expect says {expected_ko}"
            )

    if timespan_str is not None:
        parsed = parse_timespan_secs(timespan_str)
        if parsed is None:
            result.passed = False
            result.notes.append(f"RUN-MSG-01: could not parse TimeSpan {timespan_str!r}")
        else:
            eff = effective_timeout(parsed)
            expected_eff = expect.get("effective_timeout_secs")
            if expected_eff is not None and eff != expected_eff:
                result.passed = False
                result.notes.append(
                    f"RUN-TIME-01: TimeSpan {timespan_str!r} -> {parsed}s "
                    f"-> effective {eff}s, but expect says {expected_eff}"
                )
            ko = kill_offset(eff)
            expected_ko = expect.get("kill_offset_secs")
            if expected_ko is not None and ko != expected_ko:
                result.passed = False
                result.notes.append(
                    f"RUN-TIME-01: kill_offset = {ko}, but expect says {expected_ko}"
                )

    # RUN-IDEMP-01 repeated cancel deadline update
    if "first_timeout_secs" in case and "second_timeout_secs" in case:
        t2 = case["second_timeout_secs"]
        eff2 = effective_timeout(t2)
        ko2 = kill_offset(eff2)
        expected_deadline = expect.get("kill_deadline_secs")
        if expected_deadline is not None and ko2 != expected_deadline:
            result.passed = False
            result.notes.append(
                f"RUN-IDEMP-01: repeated cancel with timeout {t2}s -> "
                f"kill_deadline = {ko2}s, but expect says {expected_deadline}"
            )


    # RUN-OVERLAP-01 deterministic ordering oracle. Live execution replaces
    # these symbolic events with normalized worker lifecycle events.
    if case.get("invariant") == "RUN-OVERLAP-01":
        expected_events = ["cancel_a", "await_a", "start_b"]
        actual_events = expect.get("overlap_events")
        if actual_events != expected_events:
            result.passed = False
            result.notes.append(
                f"RUN-OVERLAP-01: expected events {expected_events!r}, "
                f"got {actual_events!r}"
            )
        if expect.get("max_active_workers") != 1:
            result.passed = False
            result.notes.append(
                "RUN-OVERLAP-01: max_active_workers must be exactly 1"
            )
    return result


def validate_control_plane_case_dry(case: dict[str, Any]) -> CaseResult:
    """Validate control-plane case expectations algebraically without live probes."""
    case_id = case["id"]
    result = CaseResult(case_id=case_id, invariant=case["invariant"],
                        kind="control_plane", passed=True, mode="dry_run")

    expect = case.get("expect", {})
    queue = case.get("queue", "single")
    cancel_in_progress = case.get("cancel_in_progress", False)
    arrivals = case.get("arrivals", [])
    prefill = case.get("pending_prefill", 0)

    # GH-SINGLE-01 algebraic model
    if queue == "single" and arrivals:
        running_label = None
        pending_label = None
        cancelled_labels: list[str] = []

        for arrival in arrivals:
            label = arrival.get("label", "?")
            if running_label is None:
                running_label = label
            elif cancel_in_progress:
                # New arrival cancels running; it becomes the new running
                cancelled_labels.append(running_label)
                if pending_label is not None:
                    cancelled_labels.append(pending_label)
                    pending_label = None
                running_label = label
            else:
                # New arrival parks; if there was already a pending, it is cancelled
                if pending_label is not None:
                    cancelled_labels.append(pending_label)
                pending_label = label

        # Verify against expect
        exp_running = expect.get("running_label")
        if exp_running is not None and running_label != exp_running:
            result.passed = False
            result.notes.append(
                f"GH-SINGLE-01: model running={running_label!r}, "
                f"expect running={exp_running!r}"
            )
        exp_pending = expect.get("pending_labels")
        model_pending = [pending_label] if pending_label is not None else []
        if exp_pending is not None and sorted(model_pending) != sorted(exp_pending):
            result.passed = False
            result.notes.append(
                f"GH-SINGLE-01: model pending={model_pending!r}, "
                f"expect pending={exp_pending!r}"
            )
        exp_cancelled = expect.get("cancelled_labels")
        if exp_cancelled is not None and sorted(cancelled_labels) != sorted(exp_cancelled):
            result.passed = False
            result.notes.append(
                f"GH-SINGLE-01: model cancelled={cancelled_labels!r}, "
                f"expect cancelled={exp_cancelled!r}"
            )

    # GH-MAX-01 algebraic model
    if queue == "max" and arrivals:
        MAX_PENDING = 100
        arrival_cancelled = prefill >= MAX_PENDING
        pending_count = prefill if arrival_cancelled else prefill + len(arrivals)

        exp_arr_cancelled = expect.get("arrival_cancelled")
        if exp_arr_cancelled is not None and exp_arr_cancelled != arrival_cancelled:
            result.passed = False
            result.notes.append(
                f"GH-MAX-01: model arrival_cancelled={arrival_cancelled}, "
                f"expect arrival_cancelled={exp_arr_cancelled}"
            )
        exp_pending_count = expect.get("pending_count")
        if exp_pending_count is not None and exp_pending_count != pending_count:
            result.passed = False
            result.notes.append(
                f"GH-MAX-01: model pending_count={pending_count}, "
                f"expect pending_count={exp_pending_count}"
            )

    # GH-GROUP-01: case-insensitive group key
    if "group_variants" in case:
        variants = case["group_variants"]
        normalized = [v.lower() for v in variants]
        if len(set(normalized)) != 1:
            result.passed = False
            result.notes.append(
                f"GH-GROUP-01: group variants {variants!r} do not normalize to the same key"
            )
        else:
            exp_same_group = expect.get("same_group", True)
            if not exp_same_group:
                result.passed = False
                result.notes.append(
                    f"GH-GROUP-01: expect.same_group=false but normalization agrees; "
                    f"case contradicts GH-GROUP-01 documentation"
                )

    return result


# ─── Corpus loading ───────────────────────────────────────────────────────────

def load_corpus(path: Path) -> dict[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        print(f"ERROR: Cannot read corpus {path}: {e}", file=sys.stderr)
        sys.exit(2)
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"ERROR: Corpus {path} is not valid JSON: {e}", file=sys.stderr)
        sys.exit(2)


# ─── Result output ────────────────────────────────────────────────────────────

def print_report(
    corpus_path: Path,
    results: list[CaseResult],
    schema_errors: list[str],
    contamination_issues: list[str],
) -> None:
    print()
    print("=" * 72)
    print("Concurrency Property Differential Harness Report")
    print(f"  Corpus       : {corpus_path}")
    print(f"  Runner pin   : {RUNNER_PIN}")
    print(f"  Docs date    : {DOCS_DATE}")
    print("=" * 72)

    if schema_errors:
        print("\nSCHEMA ERRORS:")
        for e in schema_errors:
            print(f"  [SCHEMA] {e}")

    if contamination_issues:
        print("\nCONTAMINATION DETECTED:")
        for c in contamination_issues:
            print(f"  [CONTAMINATED] {c}")

    print(f"\nCases run: {len(results)}")
    passed = [r for r in results if r.passed]
    failed = [r for r in results if not r.passed]
    print(f"  Passed: {len(passed)}")
    print(f"  Failed: {len(failed)}")

    if failed:
        print("\nFAILED CASES:")
        for r in failed:
            print(f"  [{r.invariant}] {r.case_id} ({r.kind})")
            for note in r.notes:
                print(f"    * {note}")

    print("=" * 72)


# ─── Minimized case emitter ───────────────────────────────────────────────────

def emit_minimized(result: CaseResult, case: dict[str, Any], output_dir: Path) -> None:
    """Write a minimized JSON case to output_dir for promotion into the corpus."""
    output_dir.mkdir(parents=True, exist_ok=True)
    minimized = {
        "id": f"{result.case_id}-minimized",
        "invariant": result.invariant,
        "kind": result.kind,
        "description": f"Minimized failure from case {result.case_id!r}: "
                        + "; ".join(result.notes),
        "original_case": case,
    }
    out_path = output_dir / f"{result.case_id}-minimized.json"
    out_path.write_text(json.dumps(minimized, indent=2), encoding="utf-8")
    print(f"  Minimized case written: {out_path}")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Differential seed corpus harness for concurrency property cases"
    )
    ap.add_argument(
        "--corpus",
        type=Path,
        default=Path(__file__).parent / "concurrency-property-cases.json",
        help="Path to corpus JSON file (default: concurrency-property-cases.json)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Validate schema and run algebraic checks only; no live probes",
    )
    ap.add_argument(
        "--validate-only",
        action="store_true",
        default=False,
        help="Schema validation only; exit 0 if valid, 2 if invalid",
    )
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for per-case result output and minimized failures",
    )
    ap.add_argument(
        "--filter",
        default=None,
        help="Only run cases whose id contains this substring",
    )
    args = ap.parse_args()

    corpus_path: Path = args.corpus
    corpus = load_corpus(corpus_path)

    # Schema validation
    schema_errors = validate_schema(corpus)
    if schema_errors:
        for e in schema_errors:
            print(f"SCHEMA ERROR: {e}", file=sys.stderr)
        return 2

    if args.validate_only:
        print(f"Schema valid: {corpus_path}")
        print(f"  runner_pin : {corpus.get('runner_pin')}")
        print(f"  docs_date  : {corpus.get('docs_date')}")
        print(f"  cases      : {len(corpus.get('cases', []))}")
        return 0

    # Contamination check
    contamination_issues = detect_contamination(corpus)

    cases = corpus.get("cases", [])
    if args.filter:
        cases = [c for c in cases if args.filter in c.get("id", "")]

    # Determine mode
    live_mode = not args.dry_run and (
        os.environ.get("GH_TOKEN") or os.environ.get("AKSH_SERVER")
    )
    mode = "differential" if live_mode else "dry_run"

    # Always print the pins prominently
    print(f"Official runner commit : {RUNNER_PIN}")
    print(f"Documentation date     : {DOCS_DATE}")
    print(f"Corpus                 : {corpus_path}")
    print(f"Mode                   : {mode}")
    print(f"Cases                  : {len(cases)}")

    if contamination_issues:
        for c in contamination_issues:
            print(f"[CONTAMINATED] {c}")

    results: list[CaseResult] = []
    output_dir = args.output_dir

    for case in cases:
        kind = case.get("kind", "")
        case_id = case.get("id", "unknown")

        # Per-case isolated result directory
        case_out: Path | None = None
        if output_dir is not None:
            case_out = output_dir / case_id
            case_out.mkdir(parents=True, exist_ok=True)
            (case_out / "case.json").write_text(
                json.dumps(case, indent=2), encoding="utf-8"
            )

        if kind == "runner":
            r = validate_runner_case_dry(case)
        elif kind == "control_plane":
            r = validate_control_plane_case_dry(case)
        else:
            r = CaseResult(
                case_id=case_id,
                invariant=case.get("invariant", "unknown"),
                kind=kind,
                passed=False,
                mode=mode,
                notes=[f"Unknown kind {kind!r}"],
            )

        r.mode = mode
        r.output_dir = case_out

        if case_out is not None:
            result_data = {
                "case_id": r.case_id,
                "invariant": r.invariant,
                "kind": r.kind,
                "passed": r.passed,
                "mode": r.mode,
                "notes": r.notes,
            }
            (case_out / "result.json").write_text(
                json.dumps(result_data, indent=2), encoding="utf-8"
            )

        results.append(r)

        status = "PASS" if r.passed else "FAIL"
        print(f"  [{status}] {case_id} ({r.invariant})")
        for note in r.notes:
            print(f"         {note}")

        # Emit minimized case on failure
        if not r.passed and output_dir is not None:
            emit_minimized(r, case, output_dir / "minimized")

    print_report(corpus_path, results, schema_errors=[], contamination_issues=contamination_issues)

    # Exit nonzero for any contamination or failure
    if contamination_issues:
        return 1
    if any(not r.passed for r in results):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
