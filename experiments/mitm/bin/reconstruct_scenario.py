#!/usr/bin/env python3
"""Reconstruct a runnable-ish workflow manifest from a runner acquirejob capture.

The acquirejob response is an expanded job plan, not the original workflow.  This
script deliberately emits only facts present in that response and uses blank
padding to retain source line numbers carried by YAML source-span tokens.
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_GOLDEN_ROOT = ROOT / ".runner-watch" / "golden" / "v2.337.0" / "gh-official"


def token_line(value: Any) -> int | None:
    return value.get("line") if isinstance(value, dict) else None


def token_col(value: Any) -> int | None:
    return value.get("col") if isinstance(value, dict) else None


def token_value(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    if value.get("type") == 0:
        return value.get("lit", "")
    if value.get("type") == 3:
        return expression_value(value.get("expr", ""))
    if value.get("type") == 5:
        return value.get("bool")
    if value.get("type") == 6:
        return value.get("num")
    if value.get("type") == 1:
        return [token_value(item) for item in value.get("seq", [])]
    if value.get("type") == 2:
        return {token_value(item["Key"]): token_value(item["Value"]) for item in value.get("map", [])}
    return value.get("lit", "")


def expression_value(expr: str) -> str:
    """Turn runner's format(...) representation back into workflow text."""
    if not expr.startswith("format(") or not expr.endswith(")"):
        return "${{ " + expr + " }}"
    args = split_arguments(expr[7:-1])
    if not args:
        return ""
    template = parse_single_quoted(args[0])
    values = [format_argument(arg) for arg in args[1:]]
    out: list[str] = []
    i = 0
    while i < len(template):
        if template.startswith("{{", i):
            out.append("{")
            i += 2
        elif template.startswith("}}", i):
            out.append("}")
            i += 2
        elif template[i] == "{" and (match := re.match(r"\{(\d+)\}", template[i:])):
            index = int(match.group(1))
            if index >= len(values):
                raise ValueError(f"format placeholder {{{index}}} has no argument in {expr!r}")
            out.append(values[index])
            i += len(match.group(0))
        else:
            out.append(template[i])
            i += 1
    return "".join(out)


def split_arguments(text: str) -> list[str]:
    args: list[str] = []
    start = 0
    depth = 0
    quote = False
    i = 0
    while i < len(text):
        char = text[i]
        if quote:
            if char == "'":
                if i + 1 < len(text) and text[i + 1] == "'":
                    i += 2
                    continue
                quote = False
        elif char == "'":
            quote = True
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            args.append(text[start:i].strip())
            start = i + 1
        i += 1
    args.append(text[start:].strip())
    return args


def parse_single_quoted(text: str) -> str:
    text = text.strip()
    if len(text) < 2 or text[0] != "'" or text[-1] != "'":
        raise ValueError(f"format template is not a quoted string: {text!r}")
    return text[1:-1].replace("''", "'")


def format_argument(text: str) -> str:
    text = text.strip()
    if len(text) >= 2 and text[0] == "'" and text[-1] == "'":
        return "${{ " + text + " }}"
    return "${{ " + text + " }}"


def yaml_scalar(value: Any) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)
    # JSON quoting is valid YAML and avoids ambiguities in expressions, colons,
    # hashes, and action references.
    return json.dumps(str(value), ensure_ascii=False)


def map_entries(token: Any) -> list[tuple[str, Any, int | None, int | None]]:
    if not isinstance(token, dict) or token.get("type") != 2:
        return []
    return [
        (
            str(item["Key"].get("lit", "")),
            item.get("Value"),
            token_line(item["Key"]),
            token_col(item["Key"]),
        )
        for item in token.get("map", [])
    ]


def environment_entries(token: Any) -> list[tuple[str, Any, int | None, int | None]]:
    """Expand the acquirejob ``environmentVariables`` list of per-scope maps.

    The capture carries one token map per source scope (top-level, job, step),
    so the entries have to be flattened before the caller can route them to the
    level their source column names.
    """
    if isinstance(token, list):
        entries: list[tuple[str, Any, int | None, int | None]] = []
        for item in token:
            entries.extend(map_entries(item))
        return entries
    return map_entries(token)


def input_entries(step: dict[str, Any]) -> list[tuple[str, Any, int | None, int | None]]:
    return map_entries(step.get("inputs"))


def get_input(step: dict[str, Any], key: str) -> tuple[Any, int | None, int | None] | None:
    for name, value, line, col in input_entries(step):
        if name == key:
            return value, line, col
    return None


def action_reference(reference: dict[str, Any]) -> str:
    if reference.get("type") == "script":
        return ""
    if reference.get("repositoryType") == "self":
        return str(reference.get("path", ""))
    name = reference.get("name", "")
    ref = reference.get("ref", "")
    return f"{name}@{ref}" if ref else str(name)


def source_line_for_step(step: dict[str, Any]) -> int:
    candidates: list[int] = []
    for key in ("displayNameToken", "environment", "continueOnError", "timeoutInMinutes"):
        line = token_line(step.get(key))
        if line:
            candidates.append(line)
    for _, value, line, _ in input_entries(step):
        if line:
            candidates.append(line)
        if token_line(value):
            candidates.append(token_line(value))  # type: ignore[arg-type]
    return min(candidates or [1])


def step_records(step: dict[str, Any]) -> list[tuple[int, str, Any]]:
    records: list[tuple[int, str, Any]] = []
    name_token = step.get("displayNameToken")
    if name_token:
        records.append((token_line(name_token) or source_line_for_step(step), "name", name_token.get("lit", "")))
    reference = step.get("reference", {})
    if reference.get("type") == "script":
        script = get_input(step, "script")
        if script is None:
            return records
        value, line, _ = script
        records.append((token_line(value) or line or source_line_for_step(step), "run", token_value(value)))
        for key in ("shell", "workingDirectory"):
            entry = get_input(step, key)
            if entry:
                value, line, _ = entry
                records.append((line or token_line(value) or source_line_for_step(step), "working-directory" if key == "workingDirectory" else key, token_value(value)))
    else:
        line = source_line_for_step(step)
        records.append((line, "uses", action_reference(reference)))
        with_entries = input_entries(step)
        if with_entries:
            child_lines = [entry_line for _, _, entry_line, _ in with_entries if entry_line]
            records.append(((min(child_lines) - 1) if child_lines else source_line_for_step(step), "with", with_entries))
    environment = step.get("environment")
    if environment:
        child_lines = [line for _, _, line, _ in map_entries(environment) if line]
        records.append(((min(child_lines) - 1) if child_lines else token_line(environment) or source_line_for_step(step), "env", map_entries(environment)))
    for source_key, yaml_key in (("condition", "if"), ("continueOnError", "continue-on-error"), ("timeoutInMinutes", "timeout-minutes")):
        value = step.get(source_key)
        if value is None or value == "success()":
            continue
        records.append((token_line(value) or source_line_for_step(step), yaml_key, token_value(value) if isinstance(value, dict) else value))
    return records


def render_multiline(prefix: str, value: str) -> list[str]:
    lines = value.splitlines()
    if not lines:
        return [prefix + "run: |", "          "]
    return [prefix + "run: |", *(("          " + line) for line in lines)]


def render_step(records: list[tuple[int, str, Any]], lines: list[str], notes: list[str]) -> None:
    records.sort(key=lambda item: item[0])
    occupied: dict[int, int] = {}
    for target, key, value in records:
        while len(lines) < target - 1:
            lines.append("")
        if len(lines) > target - 1:
            notes.append(f"{key} source line {target} emitted at line {len(lines) + 1}")
            target = len(lines) + 1
        prefix = "      - " if not occupied else "        "
        if key == "run" and isinstance(value, str) and ("\n" in value or value == ""):
            rendered = render_multiline(prefix, value)
        elif key in ("env", "with") and isinstance(value, list):
            rendered = [prefix + key + ":"]
            for name, raw, source_line, _ in value:
                if source_line and len(lines) + len(rendered) < source_line:
                    rendered.extend("" for _ in range(source_line - (len(lines) + len(rendered)) - 1))
                rendered.append("          " + name + ": " + yaml_scalar(token_value(raw)))
        else:
            rendered = [prefix + key + ": " + yaml_scalar(value)]
        lines.extend(rendered)
        occupied[target] = len(rendered)


def render_workflow(name: str, payload: dict[str, Any], replay_job_count: int = 1) -> tuple[str, list[str]]:
    lines = [f"name: {name}", "on: workflow_dispatch"]
    notes: list[str] = ["trigger inferred as workflow_dispatch"]

    # Top-level defaults and env are the entries whose source indentation is 2.
    top_env: list[tuple[str, Any, int | None, int | None]] = []
    job_env: list[tuple[str, Any, int | None, int | None]] = []
    for entry in environment_entries(payload.get("environmentVariables")):
        if entry[3] == 3:
            top_env.append(entry)
        else:
            job_env.append(entry)
    if top_env:
        while len(lines) < min((line or 3 for _, _, line, _ in top_env)) - 1:
            lines.append("")
        lines.append("env:")
        for key, value, line, _ in top_env:
            while len(lines) < (line or len(lines) + 1) - 1:
                lines.append("")
            lines.append(f"  {key}: {yaml_scalar(token_value(value))}")
    defaults = payload.get("defaults")
    if defaults:
        notes.append("defaults recovered from acquirejob token map")
        lines.append("")
        lines.append("defaults:")
        for key, value, _, _ in map_entries(defaults[0]):
            lines.append(f"  {key}:")
            for child, raw, _, _ in map_entries(value):
                lines.append(f"    {child}: {yaml_scalar(token_value(raw))}")

    if top_env or defaults:
        lines.append("")
    lines.append("jobs:")
    display = str(payload.get("jobDisplayName") or "reconstructed")
    job_id = re.sub(r"[^A-Za-z0-9_-]+", "-", display.split(" (")[0]).strip("-").lower() or "reconstructed"
    lines.append(f"  {job_id}:")
    lines.append("    runs-on: self-hosted")
    if job_env:
        lines.append("    env:")
        for key, value, _, _ in job_env:
            lines.append(f"      {key}: {yaml_scalar(token_value(value))}")
    if payload.get("jobOutputs"):
        lines.append("    outputs:")
        for key, value, _, _ in map_entries(payload["jobOutputs"]):
            lines.append(f"      {key}: {yaml_scalar(token_value(value))}")
    if payload.get("jobServiceContainers"):
        notes.append("service container map recovered; source formatting is not recoverable")
        lines.append("    services:")
        for service, value, _, _ in map_entries(payload["jobServiceContainers"]):
            lines.append(f"      {service}:")
            for key, raw, _, _ in map_entries(value):
                lines.append(f"        {key}: {yaml_scalar(token_value(raw))}")
    if payload.get("jobContainer"):
        lines.append("    container:")
        container_entries = map_entries(payload["jobContainer"])
        if container_entries:
            notes.append("job container map recovered; source formatting is not recoverable")
            for key, raw, _, _ in container_entries:
                lines.append(f"      {key}: {yaml_scalar(token_value(raw))}")
        else:
            # A scalar container (`container: node:22-alpine`) arrives as a
            # plain literal token, not a token map.
            notes.append("job container scalar recovered")
            lines[-1] = "    container: " + yaml_scalar(token_value(payload["jobContainer"]))
    lines.append("    steps:")
    for step in payload.get("steps", []):
        render_step(step_records(step), lines, notes)
    for index in range(2, replay_job_count + 1):
        lines.extend(
            [
                "",
                f"  replay-extra-{index}:",
                "    runs-on: self-hosted",
                "    steps:",

                f'      - run: "echo \\"replay placeholder {index}\\""',
            ]
        )
    if replay_job_count > 1:
        notes.append(
            f"capture delivered {replay_job_count} jobs; extra replay jobs preserve queue cardinality"
        )
    notes.append("strategy.matrix pre-expansion is not recoverable from this capture")
    notes.append("comments and original scalar/flow formatting are not recoverable")
    return "\n".join(lines).rstrip() + "\n", sorted(set(notes))
def captured_job_count(capture: Path) -> int:
    count = 0
    for line in (capture / "flows.jsonl").read_text().splitlines():
        flow = json.loads(line)
        path = str(flow.get("path", ""))
        response = flow.get("response_body_json") or {}
        if (
            ("/messages?" in path or "/message?" in path)
            and response.get("messageType") == "RunnerJobRequest"
        ):
            count += 1
    return count



def acquire_payload(capture: Path) -> dict[str, Any]:
    for line in (capture / "flows.jsonl").read_text().splitlines():
        flow = json.loads(line)
        if not flow.get("path", "").endswith("/acquirejob"):
            continue
        encoded = flow.get("response_body_b64")
        if not encoded:
            continue
        body = json.loads(base64.b64decode(encoded))
        if body.get("steps") is not None and body.get("fileTable"):
            return body
    raise SystemExit(f"{capture}: no populated acquirejob response")


def verify_step_order(capture: Path, payload: dict[str, Any]) -> None:
    result = json.loads((capture / "run-result.json").read_text())
    job = next(
        (candidate for candidate in result.get("jobs", []) if candidate.get("name") == payload.get("jobDisplayName")),
        None,
    )
    if job is None:
        raise SystemExit(f"{capture}: run-result has no job {payload.get('jobDisplayName')!r}")
    actual = [str(step.get("name", "")) for step in job.get("steps", [])]
    position = 0
    for index, step in enumerate(payload.get("steps", [])):
        name_token = step.get("displayNameToken")
        if name_token:
            expected = str(name_token.get("lit", ""))
        elif step.get("reference", {}).get("type") != "script":
            expected = "Run " + action_reference(step.get("reference", {}))
        else:
            script = get_input(step, "script")
            body = token_value(script[0]) if script else ""
            expected = "Run " + str(body).splitlines()[0] if body else "Run "
        matches = [
            candidate
            for candidate in range(position, len(actual))
            if actual[candidate] == expected
            or (expected.startswith("Run ") and actual[candidate].startswith(expected.split("${{", 1)[0]))
        ]
        if not matches:
            raise SystemExit(
                f"{capture}: reconstructed step {index} {expected!r} is absent or out of order"
            )
        position = matches[0] + 1


def scenario_toml(name: str) -> str:
    return f'''description = "Reconstructed v2.337.0 workflow from the gh-official acquirejob capture for {name}."
duration_seconds_max = 300

[[steps]]
kind = "submit_workflow"
path = "{name}.yml"

[[steps]]
kind = "wait_for_event"
event = "job_assigned"
timeout = 120

[[steps]]
kind = "wait_for_event"
event = "job_completed"
timeout = 240
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("names", nargs="*", help="scenario names; defaults to every capture")
    parser.add_argument("--golden-root", type=Path, default=DEFAULT_GOLDEN_ROOT, help="capture cell directory")
    parser.add_argument("--output-root", type=Path, default=ROOT / "experiments/mitm/scenarios")
    parser.add_argument("--verify", action="store_true", help="cross-check reconstructed acquired steps against run-result.json")
    args = parser.parse_args()
    names = args.names or sorted(path.name for path in args.golden_root.iterdir() if path.is_dir())
    for name in names:
        capture = args.golden_root / name
        payload = acquire_payload(capture)
        workflow, notes = render_workflow(name, payload, captured_job_count(capture))
        destination = args.output_root / name
        destination.mkdir(parents=True, exist_ok=True)
        (destination / f"{name}.yml").write_text(workflow)
        (destination / "scenario.toml").write_text(scenario_toml(name))
        if args.verify:
            verify_step_order(args.golden_root / name, payload)
            print(f"{name}: run-result step-order check passed")
        print(f"{name}: {len(payload.get('steps', []))} acquired steps -> {destination}")
        for note in notes:
            print(f"  gap: {note}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
