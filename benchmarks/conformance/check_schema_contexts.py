#!/usr/bin/env python3
"""Diff the vendored GitHub Actions workflow schema against the parser.

The schema is a small DSL rather than JSON Schema: named definitions compose
through ``mapping``, ``sequence``, and ``one-of`` nodes.  This checker walks
that DSL from ``workflow-root`` and compares every reachable context-bearing
field definition with the CTX_* constants in eval.rs.

The schema-to-constant table is deliberately explicit.  The schema names a
field's contract, while eval.rs chooses which validator is called for that
field; there is no reliable spelling-only mapping between those two APIs.  The
constants themselves are parsed from Rust source, including aliases, so a
changed or malformed constant cannot silently make this check pass.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = ROOT / "benchmarks" / "conformance" / "schema"
SCHEMA_PATH = SCHEMA_DIR / "workflow-v1.0.json"
SOURCE_PATH = SCHEMA_DIR / "SOURCE.json"
EVAL_PATH = ROOT / "crates" / "preloop-gha-parser" / "src" / "eval.rs"
MODELS_PATH = ROOT / "crates" / "preloop-gha-parser" / "src" / "models.rs"

# Definition names reachable from the normal (non-strict) workflow root are
# discovered from the schema.  This table only describes how the parser
# validates the resulting definitions.  Entries absent here must be written
# into CONTEXT_ALLOWLIST with a reason before the checker can pass.
CONTEXT_BINDINGS: dict[str, tuple[str, ...]] = {
    "run-name": ("CTX_RUN_NAME",),
    "workflow-env": ("CTX_WORKFLOW_ENV",),
    "workflow-concurrency": ("CTX_WORKFLOW_CONCURRENCY",),
    "job-if": ("CTX_JOB_IF",),
    "strategy": ("CTX_STRATEGY",),
    "runs-on": ("CTX_JOB_RUNS_ON",),
    "job-env": ("CTX_JOB_ENV",),
    "job-concurrency": ("CTX_JOB_CONCURRENCY",),
    "job-defaults-run": ("CTX_JOB_DEFAULTS_RUN",),
    "container": ("CTX_JOB_CONTAINER",),
    "services": ("CTX_JOB_CONTAINER",),
    "services-container": ("CTX_JOB_CONTAINER",),
    "container-registry-credentials": ("CTX_CONTAINER_CREDENTIALS",),
    "boolean-strategy-context": ("CTX_JOB_RUNS_ON",),
    "string-strategy-context": ("CTX_JOB_RUNS_ON",),
    "string-runner-context": ("CTX_RUNNER",),
    "step-continue-on-error": ("CTX_STEP_CONTINUE_ON_ERROR",),
    "step-if": ("CTX_STEP_IF",),
    "step-env": ("CTX_STEP_ENV",),
    "step-name": ("CTX_STEP_NAME",),
    "step-timeout-minutes": ("CTX_STEP_TIMEOUT",),
    "step-with": ("CTX_STEP_WITH",),
    "string-steps-context": ("CTX_STEP_RUN", "CTX_STEP_WORKING_DIR"),
}

# These are real parser gaps, not expected schema differences.  They remain
# visible in the report and are required to carry a written reason.  A missing
# validator is reported as a wrongly-accepts gap because arbitrary contexts
# are not rejected at that field.
CONTEXT_ALLOWLIST: dict[str, str] = {
    "workflow-call-input-default": (
        "InputDefinition.default is Value (models.rs:360-361) and "
        "validate_workflow_expressions does not inspect workflow_call definitions "
        "(eval.rs:358-548)."
    ),
    "workflow-output-context": (
        "OutputDefinition.value is parsed as String (models.rs:420-427), but "
        "workflow_call output definitions are not visited by the workflow "
        "expression validator (eval.rs:358-548)."
    ),
    "job-environment": (
        "Job.environment is retained as raw Value (models.rs:665-667) and no "
        "job environment validation call exists (eval.rs:424-477)."
    ),
    "job-environment-name": (
        "The nested environment name is inside Job.environment Value "
        "(models.rs:665-667); no validator visits it (eval.rs:424-477)."
    ),
    "number-strategy-context": (
        "The schema's job timeout and cancel-timeout fields have no Job model "
        "fields (models.rs:637-695), so serde drops them and no context check "
        "can run."
    ),
    "scalar-needs-context": (
        "Reusable-job with values are stored as Job.with Value "
        "(models.rs:677-679) and are not visited by eval.rs:378-477."
    ),
    "scalar-needs-context-with-secrets": (
        "Reusable-job secrets are stored as Job.secrets Value "
        "(models.rs:680-682) and are not visited by eval.rs:378-477."
    ),
    "snapshot-if": (
        "Job has no snapshot field (models.rs:637-695), so snapshot.if is an "
        "unknown serde key and no context check runs."
    ),
    "string-runner-context-no-secrets": (
        "The environment URL is nested in raw Job.environment Value "
        "(models.rs:665-667) and is not visited by eval.rs:424-477."
    ),
}

# Type checks cover every scalar field represented by the workflow models and
# every scalar schema definition used by those fields.  The source declaration
# is located at runtime, so line numbers in the report remain useful after
# unrelated edits.  Broad Value representations and absent fields are kept in
# TYPE_ALLOWLIST until a behavior-preserving model cutover is designed.
TYPE_CHECKS: tuple[dict[str, str], ...] = (
    {"target": "Workflow.name", "schema": "workflow-name"},
    {"target": "Workflow.run_name", "schema": "run-name"},
    {"target": "Workflow.env", "schema": "workflow-env"},
    {
        "target": "Workflow.env values",
        "source_target": "Workflow.env",
        "model_type": "EnvValue",
        "schema": "string",
    },
    {"target": "Workflow.permissions", "schema": "permissions"},
    {"target": "Workflow.concurrency", "schema": "workflow-concurrency"},
    {"target": "Workflow.defaults", "schema": "workflow-defaults"},
    {"target": "InputDefinition.input_type", "schema": "workflow-call-input-type"},
    {"target": "InputDefinition.required", "schema": "boolean"},
    {"target": "InputDefinition.default", "schema": "workflow-call-input-default"},
    {"target": "InputDefinition.description", "schema": "string"},
    {"target": "SecretDefinition.required", "schema": "boolean"},
    {"target": "SecretDefinition.description", "schema": "string"},
    {"target": "OutputDefinition.value", "schema": "workflow-output-context"},
    {"target": "OutputDefinition.description", "schema": "string"},
    {"target": "EnvValue", "schema": "string"},
    {"target": "Job.name", "schema": "string-strategy-context"},
    {"target": "Job.if_condition", "schema": "job-if"},
    {"target": "Job.continue_on_error", "schema": "boolean-strategy-context"},
    {"target": "Job.timeout_minutes", "schema": "number-strategy-context"},
    {"target": "Job.cancel_timeout_minutes", "schema": "number-strategy-context"},
    {"target": "Job.environment", "schema": "job-environment"},
    {"target": "Job.with", "schema": "workflow-job-with"},
    {
        "target": "Job.with values",
        "source_target": "Job.with",
        "model_type": "Value",
        "schema": "scalar-needs-context",
    },
    {"target": "Job.secrets", "schema": "workflow-job-secrets"},
    {
        "target": "Job.secrets values",
        "source_target": "Job.secrets",
        "model_type": "Value",
        "schema": "scalar-needs-context-with-secrets",
    },
    {"target": "Job.outputs", "schema": "job-outputs"},
    {
        "target": "Job.outputs values",
        "source_target": "Job.outputs",
        "model_type": "Value",
        "schema": "string-runner-context",
    },
    {"target": "Job.uses", "schema": "non-empty-string"},
    {"target": "Job.permissions", "schema": "permissions"},
    {"target": "Job.container", "schema": "container"},
    {
        "target": "Job.container values",
        "source_target": "Job.container",
        "model_type": "Value",
        "schema": "container",
    },
    {"target": "Job.services", "schema": "services"},
    {
        "target": "Job.services values",
        "source_target": "Job.services",
        "model_type": "Value",
        "schema": "services",
    },
    {"target": "Job.env", "schema": "job-env"},
    {
        "target": "Job.env values",
        "source_target": "Job.env",
        "model_type": "EnvValue",
        "schema": "string",
    },
    {"target": "Job.defaults", "schema": "job-defaults"},
    {"target": "DefaultsRun.shell", "schema": "shell"},
    {"target": "DefaultsRun.working_directory", "schema": "working-directory"},
    {"target": "RunsOn", "schema": "runs-on"},
    {"target": "Needs", "schema": "needs"},
    {"target": "Strategy.fail_fast", "schema": "boolean"},
    {"target": "Strategy.max_parallel", "schema": "number"},
    {"target": "DeferredBool", "schema": "boolean"},
    {"target": "DeferredNumber", "schema": "number"},
    {"target": "Concurrency.group", "schema": "non-empty-string"},
    {"target": "Concurrency.cancel_in_progress", "schema": "boolean"},
    {"target": "Step.id", "schema": "step-id"},
    {"target": "Step.name", "schema": "step-name"},
    {"target": "Step.run", "schema": "string-steps-context"},
    {"target": "Step.uses", "schema": "step-uses"},
    {"target": "Step.if_condition", "schema": "step-if"},
    {"target": "Step.working_directory", "schema": "string-steps-context"},
    {"target": "Step.shell", "schema": "shell"},
    {"target": "Step.continue_on_error", "schema": "step-continue-on-error"},
    {"target": "Step.timeout_minutes", "schema": "step-timeout-minutes"},
    {"target": "Step.env", "schema": "step-env"},
    {
        "target": "Step.env values",
        "source_target": "Step.env",
        "model_type": "EnvValue",
        "schema": "string",
    },
    {"target": "Step.with", "schema": "step-with"},
    {
        "target": "Step.with values",
        "source_target": "Step.with",
        "model_type": "Value",
        "schema": "string",
    },
)

# Values accepted by the parser but intentionally broader than the schema are
# reported with a reason until a typed model cutover can preserve behavior.
TYPE_ALLOWLIST: dict[str, str] = {
    "InputDefinition.default": (
        "Value is intentionally retained until the declared workflow_call input "
        "type is known; apply_workflow_dispatch_inputs/coerce_value enforce the "
        "runtime type (models.rs:351-365; expand.rs:682-756)."
    ),
    "EnvValue": (
        "The schema declares environment values as strings, while EnvValue accepts "
        "YAML scalar bool/number/null and normalizes them to strings "
        "(models.rs:584-595,609-617); changing this is a coercion-policy decision."
    ),
    "Workflow.env values": (
        "Workflow.env stores scalar values as EnvValue, which accepts YAML "
        "bool/number/null and normalizes them to strings "
        "(models.rs:181,587-595,609-617)."
    ),
    "Job.env values": (
        "Job.env stores scalar values as EnvValue, which accepts YAML "
        "bool/number/null and normalizes them to strings "
        "(models.rs:664,587-595,609-617)."
    ),
    "Step.env values": (
        "Step.env stores scalar values as EnvValue, which accepts YAML "
        "bool/number/null and normalizes them to strings "
        "(models.rs:1018,587-595,609-617)."
    ),
    "Step.with values": (
        "Step.with values remain raw Value because actions commonly accept "
        "boolean and numeric inputs; narrowing to schema strings is not mechanical "
        "(models.rs:1021; action input serialization in job_builder.rs)."
    ),
    "Job.timeout_minutes": (
        "The schema declares a numeric job field, but Job has no corresponding field "
        "(models.rs:637-695); unknown serde keys are dropped."
    ),
    "Job.cancel_timeout_minutes": (
        "The schema declares a numeric job field, but Job has no corresponding field "
        "(models.rs:637-695); unknown serde keys are dropped."
    ),
    "Job.environment": (
        "Job.environment is raw Value for scalar-or-mapping support "
        "(models.rs:665-667); narrowing it requires a model and expansion cutover."
    ),
    "Job.secrets": (
        "Reusable-workflow secret values are raw Value (models.rs:680-682); the "
        "schema-compatible typed cutover is not mechanical."
    ),
    "Job.outputs": (
        "Job output values are raw Value (models.rs:691-694) so expressions and "
        "wire serialization are preserved; schema says string."
    ),
    "RunsOn": (
        "RunsOn is a deliberate one-of representation of string/list/object input "
        "(models.rs:716-726), not a scalar type mismatch."
    ),
    "Needs": (
        "Needs is a deliberate one-of representation of string/list input "
        "(models.rs:768-779), not a scalar type mismatch."
    ),
    "Concurrency.cancel_in_progress": (
        "The schema's boolean may be a deferred expression; the custom deserializer "
        "accepts bool and string and stores the expression source "
        "(models.rs:922-984)."
    ),
    "Workflow.permissions": (
        "Permissions are retained as raw Value because the schema accepts several "
        "mapping and shorthand forms (models.rs:182-184); no scalar-only cutover "
        "is mechanical."
    ),
    "Job.permissions": (
        "Job permissions are retained as raw Value because the schema accepts "
        "several mapping and shorthand forms (models.rs:668-670)."
    ),
    "Job.container": (
        "Container accepts string or mapping forms and is evaluated runner-side as "
        "raw Value (models.rs:683-686); a typed cutover is not mechanical."
    ),
    "Job.services": (
        "Services accept a mapping of raw container values and are evaluated "
        "runner-side (models.rs:686-687); a typed cutover is not mechanical."
    ),
    "Job.with values": (
        "Reusable-job input values remain raw Value (models.rs:677-679); schema "
        "scalar enforcement needs a separate typed input contract."
    ),
    "Job.secrets values": (
        "Reusable-job secret values remain raw Value (models.rs:680-682); schema "
        "scalar enforcement needs a separate typed secret contract."
    ),
    "Job.outputs values": (
        "Job output values remain raw Value to preserve expression/wire handling "
        "(models.rs:691-694); schema declares strings."
    ),
    "Job.container values": (
        "Container values are raw Value and can be scalar or mapping "
        "(models.rs:683-686); narrowing nested values is not mechanical."
    ),
    "Job.services values": (
        "Service container values are raw Value (models.rs:686-687); narrowing "
        "nested values is not mechanical."
    ),
}

PRIMITIVE_KINDS = {"string", "number", "boolean", "null", "any", "sequence", "mapping"}


class AuditError(Exception):
    """Malformed input makes the audit fail closed."""


def load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AuditError(f"cannot read {label} {path}: {error}") from error
    if not isinstance(value, dict):
        raise AuditError(f"{label} must contain a JSON object: {path}")
    return value


def verify_schema_source(schema: dict[str, Any], source: dict[str, Any], schema_path: Path) -> None:
    required = {"repo", "path", "commit", "raw_url", "sha256"}
    missing = sorted(required - source.keys())
    if missing:
        raise AuditError(f"schema source metadata missing keys: {', '.join(missing)}")
    commit = source["commit"]
    if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise AuditError("schema source commit must be a 40-character lowercase SHA")
    raw_url = source["raw_url"]
    expected_fragment = f"{source['repo']}/{commit}/{source['path']}"
    if not isinstance(raw_url, str) or expected_fragment not in raw_url:
        raise AuditError("schema source raw_url does not pin the recorded repository/path/SHA")
    actual_hash = hashlib.sha256(schema_path.read_bytes()).hexdigest()
    if actual_hash != source["sha256"]:
        raise AuditError(
            f"vendored schema sha256 mismatch: metadata={source['sha256']} actual={actual_hash}"
        )
    if schema.get("version") != "workflow-v1.0":
        raise AuditError(f"unexpected schema version: {schema.get('version')!r}")


def parse_context_constants(source: str) -> dict[str, dict[str, Any]]:
    declared = set(re.findall(r"(?m)^\s*const\s+(CTX_[A-Z0-9_]+)\s*:", source))
    declaration_re = re.compile(
        r"(?ms)^\s*const\s+(CTX_[A-Z0-9_]+)\s*:\s*&\[&str\]\s*=\s*(.*?);"
    )
    parsed: dict[str, dict[str, Any]] = {}
    for match in declaration_re.finditer(source):
        name, rhs = match.group(1), match.group(2).strip()
        line = source.count("\n", 0, match.start()) + 1
        if rhs.startswith("&[") and rhs.endswith("]"):
            body = rhs[2:-1]
            values = re.findall(r'"([^"\\]*(?:\\.[^"\\]*)*)"', body)
            remainder = re.sub(r'"([^"\\]*(?:\\.[^"\\]*)*)"', "", body)
            remainder = re.sub(r"[\s,]", "", remainder)
            if remainder:
                raise AuditError(f"cannot parse {name} at eval.rs:{line}: {rhs}")
            parsed[name] = {"values": values, "line": line, "rhs": rhs}
            continue
        alias = re.fullmatch(r"(CTX_[A-Z0-9_]+)", rhs)
        if alias:
            parsed[name] = {"alias": alias.group(1), "line": line, "rhs": rhs}
            continue
        raise AuditError(f"cannot parse {name} at eval.rs:{line}: {rhs}")
    if declared != set(parsed):
        missing = sorted(declared - set(parsed))
        extra = sorted(set(parsed) - declared)
        raise AuditError(f"CTX declaration parse mismatch: missing={missing} extra={extra}")

    resolving: set[str] = set()

    def resolve(name: str) -> list[str]:
        if name not in parsed:
            raise AuditError(f"CTX alias references unknown constant {name}")
        if name in resolving:
            raise AuditError(f"CTX alias cycle includes {name}")
        item = parsed[name]
        if "values" in item:
            return list(item["values"])
        resolving.add(name)
        values = resolve(item["alias"])
        resolving.remove(name)
        return values

    for name in parsed:
        parsed[name]["values"] = resolve(name)
    return parsed


def named_spec(spec: Any) -> str | None:
    if isinstance(spec, str):
        return spec
    if isinstance(spec, dict) and isinstance(spec.get("type"), str):
        return spec["type"]
    return None


def schema_kinds(definitions: dict[str, Any], spec: Any, stack: tuple[str, ...] = ()) -> set[str]:
    name = named_spec(spec)
    if name is None:
        raise AuditError(f"schema spec is not a named type: {spec!r}")
    if name in PRIMITIVE_KINDS:
        return {name}
    if name not in definitions:
        raise AuditError(f"schema references unknown definition {name!r}")
    if name in stack:
        return {"recursive"}
    node = definitions[name]
    result: set[str] = set()
    for kind in PRIMITIVE_KINDS:
        if kind in node:
            result.add(kind)
    if "mapping" in node:
        result.add("mapping")
    if "sequence" in node:
        result.add("sequence")
    if "one-of" in node:
        for alternative in node["one-of"]:
            result.update(schema_kinds(definitions, alternative, stack + (name,)))
    if not result:
        raise AuditError(f"schema definition {name!r} has no recognized type")
    return result


def walk_schema(definitions: dict[str, Any]) -> dict[str, set[str]]:
    """Return reachable context definition -> workflow paths."""

    context_paths: dict[str, set[str]] = {}

    def walk(spec: Any, path: str, inherited_context: tuple[str, ...], stack: tuple[str, ...]) -> None:
        name = named_spec(spec)
        if name is None:
            raise AuditError(f"schema spec at {path or '<root>'} is malformed: {spec!r}")
        if name in PRIMITIVE_KINDS:
            return
        if name not in definitions:
            raise AuditError(f"schema references unknown definition {name!r}")
        node = definitions[name]
        context = tuple(node.get("context", inherited_context))
        if "context" in node:
            if not isinstance(node["context"], list) or not all(
                isinstance(value, str) for value in node["context"]
            ):
                raise AuditError(f"schema context for {name!r} is not a string list")
            context_paths.setdefault(name, set()).add(path or "<root>")
        if name in stack:
            return
        next_stack = stack + (name,)

        if "one-of" in node:
            alternatives = node["one-of"]
            if not isinstance(alternatives, list) or not alternatives:
                raise AuditError(f"schema one-of for {name!r} is empty or malformed")
            for alternative in alternatives:
                walk(alternative, path, context, next_stack)
        if "mapping" in node:
            mapping = node["mapping"]
            if not isinstance(mapping, dict):
                raise AuditError(f"schema mapping for {name!r} is malformed")
            properties = mapping.get("properties", {})
            if not isinstance(properties, dict):
                raise AuditError(f"schema properties for {name!r} is malformed")
            for key, child in properties.items():
                if not isinstance(key, str):
                    raise AuditError(f"schema property name for {name!r} is not a string")
                walk(child, f"{path}.{key}" if path else key, context, next_stack)
            if "loose-value-type" in mapping:
                child_path = f"{path}.<*>" if path else "<*>"
                walk(mapping["loose-value-type"], child_path, context, next_stack)
        if "sequence" in node:
            sequence = node["sequence"]
            if not isinstance(sequence, dict) or "item-type" not in sequence:
                raise AuditError(f"schema sequence for {name!r} is malformed")
            walk(sequence["item-type"], f"{path}[*]", context, next_stack)

        # Direct scalar definitions are covered by TYPE_CHECKS below.

    walk("workflow-root", "", (), ())
    return context_paths


def normalize_context(value: str) -> str:
    return value.split("(", 1)[0].strip().lower()


def context_diff(schema_values: list[str], ours: list[str]) -> tuple[list[str], list[str]]:
    schema_by_name = {normalize_context(value): value for value in schema_values}
    ours_by_name = {normalize_context(value): value for value in ours}
    missing = [schema_by_name[key] for key in sorted(schema_by_name.keys() - ours_by_name.keys())]
    extra = [ours_by_name[key] for key in sorted(ours_by_name.keys() - schema_by_name.keys())]
    return missing, extra


def struct_body(source: str, name: str) -> tuple[str, int] | None:
    match = re.search(rf"(?m)^pub struct {re.escape(name)}\s*\{{", source)
    if not match:
        return None
    end = re.search(r"(?m)^}\s*$", source[match.end() :])
    if not end:
        raise AuditError(f"unterminated struct {name}")
    body_start = match.end()
    body_end = match.end() + end.start()
    return source[body_start:body_end], source.count("\n", 0, body_start) + 1


def enum_body(source: str, name: str) -> tuple[str, int] | None:
    match = re.search(rf"(?m)^pub enum {re.escape(name)}\s*\{{", source)
    if not match:
        return None
    end = re.search(r"(?m)^}\s*$", source[match.end() :])
    if not end:
        raise AuditError(f"unterminated enum {name}")
    body_start = match.end()
    body_end = match.end() + end.start()
    return source[body_start:body_end], source.count("\n", 0, body_start) + 1


def rust_target_type(source: str, target: str) -> tuple[str | None, int | None]:
    if "." not in target:
        enum = enum_body(source, target)
        if enum is None:
            return None, None
        body, start_line = enum
        if target == "EnvValue":
            # EnvValue is a deliberate scalar-normalization enum rather than a
            # single Rust scalar.  The caller classifies it explicitly.
            return "EnvValue", start_line
        return target, start_line
    struct_name, field_name = target.split(".", 1)
    struct = struct_body(source, struct_name)
    if struct is None:
        return None, None
    body, start_line = struct
    match = re.search(rf"(?m)^\s*pub {re.escape(field_name)}\s*:\s*", body)
    if not match:
        return None, None
    type_start = match.end()
    depth = 0
    for index, character in enumerate(body[type_start:], type_start):
        if character in "<([{":
            depth += 1
        elif character in ">)]}":
            depth -= 1
        elif character == "," and depth == 0:
            line = start_line + body.count("\n", 0, match.start())
            return body[type_start:index].strip(), line
        elif character == "\n" and depth == 0:
            line = start_line + body.count("\n", 0, match.start())
            return body[type_start:index].strip(), line
    raise AuditError(f"unterminated field declaration {target}")


def rust_kinds(type_name: str | None, target: str) -> set[str]:
    if type_name is None:
        return set()
    if target == "EnvValue" or type_name == "EnvValue":
        return {"string", "boolean", "number", "null"}
    type_name = re.sub(r"\s+", "", type_name)
    while type_name.startswith("Option<") and type_name.endswith(">"):
        type_name = type_name[7:-1]
    if type_name.startswith("Vec<"):
        return {"sequence"}
    if type_name.startswith("BTreeMap<") or type_name.startswith("IndexMap<"):
        return {"mapping"}
    if type_name in {"String", "str"}:
        return {"string"}
    if type_name in {"bool"}:
        return {"boolean"}
    if type_name in {
        "u8",
        "u16",
        "u32",
        "u64",
        "u128",
        "usize",
        "i8",
        "i16",
        "i32",
        "i64",
        "i128",
        "isize",
        "f32",
        "f64",
        "serde_json::Number",
    }:
        return {"number"}
    if type_name == "Value":
        return {"any"}
    custom = {
        "Env": {"mapping"},
        "InputType": {"string"},
        "JobContinueOnError": {"boolean"},
        "DeferredBool": {"boolean"},
        "DeferredNumber": {"number"},
        "RunsOn": {"one-of"},
        "Needs": {"one-of"},
        "Concurrency": {"mapping"},
        "JobDefaults": {"mapping"},
        "Strategy": {"mapping"},
        "Value": {"any"},
    }
    return custom.get(type_name, {"opaque"})


def schema_type_text(kinds: set[str]) -> str:
    return "{" + ", ".join(sorted(kinds)) + "}"


def type_compatible(model: set[str], schema: set[str]) -> bool:
    if "any" in model:
        return "any" in schema
    if "one-of" in model:
        return "one-of" in schema or "mapping" in schema or len(schema) > 1
    if "opaque" in model or "recursive" in model:
        return False
    return model <= schema


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", type=Path, default=SCHEMA_PATH)
    parser.add_argument("--source", type=Path, default=SOURCE_PATH)
    parser.add_argument("--eval", dest="eval_path", type=Path, default=EVAL_PATH)
    parser.add_argument("--models", type=Path, default=MODELS_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        schema = load_json(args.schema, "schema")
        source_metadata = load_json(args.source, "schema source metadata")
        verify_schema_source(schema, source_metadata, args.schema)
        definitions = schema.get("definitions")
        if not isinstance(definitions, dict):
            raise AuditError("schema definitions must be an object")
        eval_source = args.eval_path.read_text()
        models_source = args.models.read_text()
        constants = parse_context_constants(eval_source)
        context_paths = walk_schema(definitions)
    except (AuditError, OSError, UnicodeDecodeError) as error:
        print(f"schema audit: ERROR: {error}", file=sys.stderr)
        return 1

    errors: list[str] = []
    bound_constants: set[str] = set()
    print(
        f"schema {schema['version']} commit {source_metadata['commit']} "
        f"sha256 {source_metadata['sha256']}"
    )
    print(f"eval.rs CTX constants: {len(constants)}")
    print("\nContext fields:")
    for definition in sorted(context_paths):
        paths = sorted(context_paths[definition])
        if definition in CONTEXT_BINDINGS:
            binding = CONTEXT_BINDINGS[definition]
            bound_constants.update(binding)
            schema_values = definitions[definition].get("context")
            if not isinstance(schema_values, list) or not all(
                isinstance(value, str) for value in schema_values
            ):
                errors.append(f"{definition}: malformed schema context")
                continue
            # Every constant in a binding is checked.  Shared bindings are
            # aliases only by convention; comparing each one prevents a stale
            # secondary validator from being hidden by the primary constant.
            missing_constants = [name for name in binding if name not in constants]
            if missing_constants:
                errors.append(
                    f"{definition}: binding references missing "
                    f"{', '.join(missing_constants)}"
                )
            mismatches: list[tuple[str, list[str], list[str]]] = []
            for constant in binding:
                if constant not in constants:
                    continue
                missing, extra = context_diff(schema_values, constants[constant]["values"])
                if missing or extra:
                    mismatches.append((constant, missing, extra))
            location = ", ".join(
                f"eval.rs:{constants[name]['line']}" for name in binding if name in constants
            )
            if missing_constants or mismatches:
                details = [
                    f"missing constants={missing_constants}"
                ] if missing_constants else []
                classification: set[str] = set()
                if missing_constants:
                    classification.add("missing-constant")
                for constant, missing, extra in mismatches:
                    details.append(
                        f"{constant}: missing={missing or '-'} extra={extra or '-'}"
                    )
                    if missing:
                        classification.add("wrongly-rejects")
                    if extra:
                        classification.add("wrongly-accepts")
                detail = "; ".join(details)
                errors.append(f"{definition}: {detail}")
                print(
                    f"DIVERGENCE {definition} paths={paths} {detail} "
                    f"class={'+'.join(sorted(classification))} source={location}"
                )
            else:
                print(
                    f"OK {definition} paths={paths} contexts={schema_values} "
                    f"constant={', '.join(binding)} source={location}"
                )
        elif definition in CONTEXT_ALLOWLIST:
            reason = CONTEXT_ALLOWLIST[definition].strip()
            if not reason:
                errors.append(f"{definition}: empty context allowlist reason")
            schema_values = definitions[definition].get("context", [])
            print(
                f"ALLOWLISTED {definition} paths={paths} contexts={schema_values} "
                f"class=wrongly-accepts reason={reason}"
            )
        else:
            errors.append(f"{definition}: no binding or written allowlist reason")
            print(f"DIVERGENCE {definition} paths={paths} class=unclassified")

    undeclared = sorted(set(constants) - bound_constants)
    if undeclared:
        errors.append("unbound CTX constants: " + ", ".join(undeclared))
        print("DIVERGENCE unbound constants=" + ", ".join(undeclared))

    print("\nModel scalar types:")
    for check in TYPE_CHECKS:
        target, schema_name = check["target"], check["schema"]
        source_target = check.get("source_target", target)
        declared, line = rust_target_type(models_source, source_target)
        if "model_type" in check:
            declared = check["model_type"]
        schema_spec: Any = schema_name
        try:
            expected = schema_kinds(definitions, schema_spec)
        except AuditError as error:
            errors.append(f"{target}: {error}")
            print(f"TYPE ERROR {target}: {error}")
            continue
        observed = rust_kinds(declared, target)
        source_location = f"models.rs:{line}" if line is not None else "models.rs:<missing>"
        if not observed:
            reason = TYPE_ALLOWLIST.get(target)
            if reason:
                print(
                    f"TYPE ALLOWLISTED {target} schema={schema_type_text(expected)} "
                    f"model=<missing> source={source_location} reason={reason}"
                )
            else:
                errors.append(f"{target}: model declaration missing")
                print(
                    f"TYPE DIVERGENCE {target} schema={schema_type_text(expected)} "
                    f"model=<missing> source={source_location} class=type-mismatch"
                )
            continue
        if type_compatible(observed, expected):
            print(
                f"TYPE OK {target} schema={schema_type_text(expected)} "
                f"model={declared} source={source_location}"
            )
        else:
            reason = TYPE_ALLOWLIST.get(target)
            if reason:
                print(
                    f"TYPE ALLOWLISTED {target} schema={schema_type_text(expected)} "
                    f"model={declared} source={source_location} reason={reason}"
                )
            else:
                errors.append(
                    f"{target}: schema={schema_type_text(expected)} model={declared}"
                )
                print(
                    f"TYPE DIVERGENCE {target} schema={schema_type_text(expected)} "
                    f"model={declared} source={source_location} class=type-mismatch"
                )

    if errors:
        print(f"\nFAIL: {len(errors)} unallowlisted divergence(s)", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1
    print(f"\nPASS: {len(context_paths)} context definitions; {len(TYPE_CHECKS)} model type checks")
    return 0


if __name__ == "__main__":
    sys.exit(main())
