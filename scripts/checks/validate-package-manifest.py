#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
import copy
import hashlib
import ipaddress
import json
import re
import sys
import tomllib
from pathlib import Path


ROOT = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(__file__).resolve().parents[2]
MANIFEST_PATH = ROOT / "undercurrent-package.json"
PACKAGE_SCHEMA_PATH = ROOT / "schemas/undercurrent-package.schema.json"
CONFIG_SCHEMA_PATH = ROOT / "schemas/config.schema.json"
FIXTURE_SCHEMA_PATH = ROOT / "schemas/package-negative-fixture.schema.json"
FIXTURE_ROOT = ROOT / "tests/conformance/package"
PROTOCOL_FIXTURE_ROOT = ROOT / "crates/undercurrent-protocol/tests/fixtures"
CURRENT_PRODUCT_REQUEST_FIXTURE = PROTOCOL_FIXTURE_ROOT / "product_request.json"
CURRENT_EXECUTE_REQUEST_FIXTURE = PROTOCOL_FIXTURE_ROOT / "execute_in_session.json"
UNSUPPORTED_PRODUCT_REQUEST_FIXTURE = PROTOCOL_FIXTURE_ROOT / "unsupported_product_request.json"
UNSUPPORTED_EXECUTE_REQUEST_FIXTURE = PROTOCOL_FIXTURE_ROOT / "unsupported_execute_in_session.json"
REPAIR_REQUEST_FIXTURE = PROTOCOL_FIXTURE_ROOT / "repair_operation_attempt.json"
RUNTIME_ATTEMPT_STATUS_FIXTURE = PROTOCOL_FIXTURE_ROOT / "runtime_attempt_status.json"
RUNTIME_CANCELLATION_FIXTURE = PROTOCOL_FIXTURE_ROOT / "runtime_cancellation.json"


class SchemaViolation(Exception):
    def __init__(self, path, message):
        self.code = "SCHEMA_INVALID"
        self.path = path
        super().__init__(f"{self.code} at {path}: {message}")


def load_json(path):
    try:
        with path.open("r", encoding="utf-8") as source:
            return json.load(source)
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"error: cannot parse {path.relative_to(ROOT)}: {error}") from error


def resolve_ref(root_schema, reference):
    if not reference.startswith("#/"):
        raise SchemaViolation("$", f"unsupported non-local schema reference {reference!r}")
    value = root_schema
    for raw_part in reference[2:].split("/"):
        part = raw_part.replace("~1", "/").replace("~0", "~")
        if not isinstance(value, dict) or part not in value:
            raise SchemaViolation("$", f"unresolved schema reference {reference!r}")
        value = value[part]
    return value


def type_matches(instance, expected):
    if expected == "object":
        return isinstance(instance, dict)
    if expected == "array":
        return isinstance(instance, list)
    if expected == "string":
        return isinstance(instance, str)
    if expected == "integer":
        return isinstance(instance, int) and not isinstance(instance, bool)
    if expected == "boolean":
        return isinstance(instance, bool)
    if expected == "null":
        return instance is None
    raise SchemaViolation("$", f"validator does not support schema type {expected!r}")


def validate(instance, schema, root_schema, path="$"):
    if "$ref" in schema:
        validate(instance, resolve_ref(root_schema, schema["$ref"]), root_schema, path)
        return

    if "oneOf" in schema:
        matches = 0
        for candidate in schema["oneOf"]:
            try:
                validate(instance, candidate, root_schema, path)
            except SchemaViolation:
                continue
            matches += 1
        if matches != 1:
            raise SchemaViolation(path, f"expected exactly one schema alternative, matched {matches}")
        return

    if "const" in schema and instance != schema["const"]:
        raise SchemaViolation(path, f"expected constant {schema['const']!r}")
    if "enum" in schema and instance not in schema["enum"]:
        raise SchemaViolation(path, f"value {instance!r} is outside the allowed set")

    expected_type = schema.get("type")
    if expected_type is not None and not type_matches(instance, expected_type):
        raise SchemaViolation(path, f"expected {expected_type}")

    if isinstance(instance, dict) and expected_type == "object":
        properties = schema.get("properties", {})
        for name in schema.get("required", []):
            if name not in instance:
                raise SchemaViolation(path, f"missing required property {name!r}")
        if schema.get("additionalProperties") is False:
            extras = sorted(set(instance) - set(properties))
            if extras:
                raise SchemaViolation(path, f"unknown properties: {', '.join(extras)}")
        for name, value in instance.items():
            if name in properties:
                validate(value, properties[name], root_schema, f"{path}.{name}")

    if isinstance(instance, list) and expected_type == "array":
        if len(instance) < schema.get("minItems", 0):
            raise SchemaViolation(path, f"requires at least {schema['minItems']} items")
        if "maxItems" in schema and len(instance) > schema["maxItems"]:
            raise SchemaViolation(path, f"allows at most {schema['maxItems']} items")
        if schema.get("uniqueItems"):
            encoded = [json.dumps(item, sort_keys=True, separators=(",", ":")) for item in instance]
            if len(encoded) != len(set(encoded)):
                raise SchemaViolation(path, "items must be unique")
        if "items" in schema:
            for index, value in enumerate(instance):
                validate(value, schema["items"], root_schema, f"{path}[{index}]")

    if isinstance(instance, str):
        if len(instance) < schema.get("minLength", 0):
            raise SchemaViolation(path, f"requires at least {schema['minLength']} characters")
        if "pattern" in schema and re.search(schema["pattern"], instance) is None:
            raise SchemaViolation(path, f"does not match {schema['pattern']!r}")

    if isinstance(instance, int) and not isinstance(instance, bool):
        if "minimum" in schema and instance < schema["minimum"]:
            raise SchemaViolation(path, f"must be at least {schema['minimum']}")
        if "maximum" in schema and instance > schema["maximum"]:
            raise SchemaViolation(path, f"must be at most {schema['maximum']}")


def ensure_strict_objects(schema, path="$"):
    if isinstance(schema, dict):
        if schema.get("type") == "object" and schema.get("additionalProperties") is not False:
            raise SystemExit(f"error: object schema {path} is not strict")
        for name, value in schema.items():
            ensure_strict_objects(value, f"{path}.{name}")
    elif isinstance(schema, list):
        for index, value in enumerate(schema):
            ensure_strict_objects(value, f"{path}[{index}]")


def require(condition, path, message):
    if not condition:
        raise SchemaViolation(path, message)


EXPECTED_OPERATIONS = (
    "start_session",
    "stop_session",
    "restart_session",
    "rename_session",
    "update_session_labels",
    "remove_session",
    "resize_terminal",
    "send_terminal_input",
    "close_standard_input",
    "list_sessions",
    "get_session_detail",
    "get_execution_context",
    "execute_in_session",
    "read_terminal_output",
    "read_terminal_screen",
    "read_terminal_screen_diff",
    "read_session_events",
    "upsert_work_item",
    "remove_work_item",
    "list_profiles",
    "get_profile",
    "match_profile",
    "lint_profiles",
    "test_profile",
    "get_diagnostics",
    "create_diagnostic_bundle",
    "get_daemon_status",
    "shutdown_daemon",
    "repair_operation_attempt",
    "get_runtime_attempt_status",
    "cancel_runtime_attempt",
    "get_runtime_cancellation_status",
    "get_agent_event",
    "reply_agent_event",
)

EXPECTED_CAPABILITIES = (
    "authenticated_request_context",
    "authorized_operation_attempt_repair",
    "authorized_runtime_attempt_cancellation",
    "authorized_runtime_attempt_status_read",
    "authorized_runtime_cancellation_status_read",
    "bounded_launch_preparation",
    "bounded_agent_event_channel",
    "bounded_streams",
    "effect_certainty",
    "environment_bound_working_directory",
    "fail_closed_attempt_readiness",
    "idempotent_mutations",
    "immutable_content_references",
    "operation_attempt_fencing",
    "object_handle_working_directory",
    "output_integrity",
    "preacceptance_executable_resolution",
    "reconciliation",
    "resume",
    "runtime_dispatch_policy_retention_without_os_limit_enforcement",
    "schema_negotiation",
    "sealed_executable_launch",
    "secret_reference_resolution",
    "terminal_pty",
    "typed_standard_input_close",
)

EXPECTED_CLI_COMMANDS = (
    "start",
    "stop",
    "restart",
    "ls",
    "inspect",
    "attach",
    "tui",
    "group",
    "send",
    "exec",
    "wait",
    "watch",
    "clean",
    "top",
    "logs",
    "peek",
    "explain",
    "context",
    "init",
    "config",
    "settings",
    "profile",
    "daemon",
    "reset",
    "debug",
    "completion",
    "events",
)

EXPECTED_STARTUP_STAGES = (
    "generate_identity",
    "prepare_instance_lock_path",
    "acquire_instance_lock",
    "open_persistence",
    "compose_telemetry",
    "compose_projections",
    "start_process_runtime",
    "compose_application_adapter",
    "compose_detection",
    "compose_foreground",
    "start_health_checks",
    "start_background_runtime",
    "start_stream_pump",
    "compose_product_reads",
    "start_dispatcher",
    "bind_listener",
    "attach_status",
)


def validate_manifest_semantics(manifest):
    release = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    require(manifest["version"] == release, "$.version", "package must match workspace release")
    for path in sorted((ROOT / "crates").glob("*/Cargo.toml")):
        package = tomllib.loads(path.read_text())["package"]
        require(package.get("version") == {"workspace": True}, str(path.relative_to(ROOT)), "crate must inherit workspace version")

    require(manifest["version"] == "0.1.0-beta", "$.version", "package/artifact version must be exactly 0.1.0-beta")
    require(
        set(manifest["runtime"]["allowed_placements"]) == {"local_process"},
        "$.runtime.allowed_placements",
        "the only allowed placement is local_process",
    )
    require(manifest["runtime"]["implicit_daemon_launch"] is True, "$.runtime.implicit_daemon_launch", "CLI daemon activation must be declared")
    artifact_pairs = {(item["role"], item["name"]) for item in manifest["artifacts"]}
    require(
        artifact_pairs == {("cli", "uc"), ("daemon", "uc-daemon"), ("session_host", "uc-session-host")},
        "$.artifacts",
        "the canonical three executable roles must be present exactly once",
    )
    require(
        tuple(manifest["contracts"]["operations"]) == EXPECTED_OPERATIONS,
        "$.contracts.operations",
        "operation registry must preserve canonical order and membership",
    )
    require(manifest["state"]["schema"]["current_version"] == "0.1.0-beta", "$.state.schema.current_version", "storage identity must match the release")
    require(manifest["state"]["schema"]["storage_format"] == 1, "$.state.schema.storage_format", "SQLite storage format must be 1")
    canonical_store = next(
        store for store in manifest["state"]["stores"] if store["id"] == "canonical_state"
    )
    require(
        canonical_store["data_classes"]
        == [
            "sessions",
            "operation_attempts",
            "runtime_cancellation_controls",
            "runtime_dispatch_policies",
            "caller_request_digests",
            "transactional_outbox",
            "scheduled_restarts",
            "scheduled_observations",
        ],
        "$.state.stores[canonical_state].data_classes",
        "canonical SQLite data classes must include separately retained caller request digests",
    )
    require(manifest["state"]["schema"]["existing_database_policy"] == "exact_current_or_reject_without_mutation", "$.state.schema.existing_database_policy", "only current state is supported")
    require(
        manifest["state"]["schema"]["current_attached_policy"]
        == "required_and_fail_closed_on_loss",
        "$.state.schema.current_attached_policy",
        "current attached attempts must retain policy and fail closed on loss",
    )
    require(
        manifest["state"]["schema"]["current_attached_request_digest"]
        == "required_canonical_and_fail_closed_on_loss",
        "$.state.schema.current_attached_request_digest",
        "current attached attempts must retain a canonical caller request digest and fail closed on loss",
    )
    require(manifest["contracts"]["contract_version"] == "0.1.0-beta", "$.contracts.contract_version", "contract version must be exact 0.1.0-beta")
    require(manifest["contracts"]["schema_version"] == "0.1.0-beta", "$.contracts.schema_version", "contract schema must be exact 0.1.0-beta")
    require(manifest["contracts"]["version_negotiation"] == "exact_current_or_reject", "$.contracts.version_negotiation", "future contract versions must be rejected")
    require(
        manifest["configuration"]["product_path_keys"] == [],
        "$.configuration.product_path_keys",
        "standalone path keys must match runtime configuration",
    )
    require(manifest["configuration"]["future_schema_versions"] == "reject", "$.configuration.future_schema_versions", "future config schema versions must be rejected")
    require(
        tuple(manifest["contracts"]["capabilities"]) == EXPECTED_CAPABILITIES,
        "$.contracts.capabilities",
        "capability registry must preserve canonical order and membership",
    )
    registry = manifest["contracts"]["registries"]
    require(
        manifest["state"]["retention"]["runtime_cancellation_controls"]
        == "retain_until_explicit_product_purge",
        "$.state.retention.runtime_cancellation_controls",
        "runtime cancellation controls must remain status-queryable until explicit product purge",
    )
    expected_counts = {"operations": 34, "streams": 4, "events": 16, "errors": 19}
    for name, count in expected_counts.items():
        require(registry[name]["count"] == count, f"$.contracts.registries.{name}.count", "incorrect registry count")
        fixture_path = ROOT / registry[name]["fixture"]
        require(fixture_path.is_file(), f"$.contracts.registries.{name}.fixture", "registry fixture does not exist")
        require(len(load_json(fixture_path)) == count, f"$.contracts.registries.{name}.count", "registry fixture count differs from manifest")
    protocols = manifest["protocols"]
    require(protocols["ucmp"] == {"magic": "UCMP", "version": "0.1.0-beta", "connection_version": "0.1.0-beta", "visibility": "public_local_ipc", "version_policy": "exact_current_or_reject"}, "$.protocols.ucmp", "UCMP framing and Undercurrent connection protocol must both be exactly 0.1.0-beta")
    require(protocols["uchs"] == {"magic": "UCHS", "version": "0.1.0-beta", "visibility": "implementation_private", "configurable_endpoint": False, "version_policy": "exact_current_or_reject"}, "$.protocols.uchs", "UCHS must remain exact-0.1.0-beta and implementation-private")
    implementation_registries = manifest["implementation_registries"]
    expected_implementation = {
        "cli_commands": EXPECTED_CLI_COMMANDS,
        "daemon_startup_stages": EXPECTED_STARTUP_STAGES,
    }
    for name, expected in expected_implementation.items():
        descriptor = implementation_registries[name]
        require(descriptor["count"] == len(expected), f"$.implementation_registries.{name}.count", "incorrect implementation registry count")
        require(tuple(load_json(ROOT / descriptor["fixture"])) == expected, f"$.implementation_registries.{name}.fixture", "implementation registry order or membership differs")
    require((ROOT / manifest["configuration"]["schema"]).is_file(), "$.configuration.schema", "configuration schema does not exist")
    require((ROOT / manifest["verification"]["package_validator"]).is_file(), "$.verification.package_validator", "validator does not exist")
    require(manifest["dependencies"]["required"] == [], "$.dependencies.required", "standalone bundle must not expand packages")
    require(manifest["dependencies"]["optional"] == [], "$.dependencies.optional", "optional packages must not be implicit")
    require(manifest["dependencies"]["peers"] == [], "$.dependencies.peers", "direct Undercurrent has no Hub attachment peer")
    require(not manifest["release"]["publishing_performed"], "$.release.publishing_performed", "local manifest must not claim publishing")
    require(not manifest["release"]["deployment_performed"], "$.release.deployment_performed", "local manifest must not claim deployment")


def request_contract(fixture, path):
    try:
        return fixture["input"]["mutation"]["contract"]
    except (KeyError, TypeError) as error:
        raise SchemaViolation(path, "fixture must contain input.mutation.contract") from error


def validate_product_contract_fixtures():
    expected_current_contracts = (
        (CURRENT_PRODUCT_REQUEST_FIXTURE, "restart_session"),
        (CURRENT_EXECUTE_REQUEST_FIXTURE, "execute_in_session"),
    )
    expected_unsupported_contracts = (
        (UNSUPPORTED_PRODUCT_REQUEST_FIXTURE, "restart_session"),
        (UNSUPPORTED_EXECUTE_REQUEST_FIXTURE, "execute_in_session"),
    )

    for fixture_path, capability in expected_current_contracts:
        relative = fixture_path.relative_to(ROOT).as_posix()
        fixture = load_json(fixture_path)
        require(fixture.get("operation") == capability, relative, "fixture operation must match its capability")
        require(
            request_contract(fixture, relative)
            == {
                "capability": capability,
                "schema": "undercurrent.request",
                "schema_version": "0.1.0-beta",
                "contract_version": "0.1.0-beta",
            },
            relative,
            "authoritative product request fixture must carry exact contract-0.1.0-beta/schema-0.1.0-beta metadata",
        )

    for fixture_path, capability in expected_unsupported_contracts:
        relative = fixture_path.relative_to(ROOT).as_posix()
        fixture = load_json(fixture_path)
        require(fixture.get("operation") == capability, relative, "fixture operation must match its capability")
        require(
            request_contract(fixture, relative)
            == {
                "capability": capability,
                "schema": "undercurrent.request",
                "schema_version": "0.1.0-beta",
                "contract_version": "99.0.0",
            },
            relative,
            "retired product request fixture must remain exact contract-0.1.0-beta rejection evidence",
        )

    repair_relative = REPAIR_REQUEST_FIXTURE.relative_to(ROOT).as_posix()
    repair_fixture = load_json(REPAIR_REQUEST_FIXTURE)
    repair_request = repair_fixture.get("request")
    require(isinstance(repair_request, dict), repair_relative, "repair fixture must contain a request object")
    require(
        set(repair_request) == {"attempt_id", "request_digest_sha256", "contract"},
        repair_relative,
        "repair request must require exactly attempt_id, request_digest_sha256, and contract",
    )
    require(
        isinstance(repair_request["request_digest_sha256"], str)
        and re.fullmatch(r"[0-9a-f]{64}", repair_request["request_digest_sha256"]) is not None,
        repair_relative,
        "attached repair fixture must carry the exact canonical retained caller request digest",
    )
    require(
        repair_request["contract"]
        == {
            "capability": "repair_operation_attempt",
            "schema": "undercurrent.request",
            "schema_version": "0.1.0-beta",
            "contract_version": "0.1.0-beta",
        },
        repair_relative,
        "repair fixture must carry exact contract-0.1.0-beta/schema-0.1.0-beta metadata",
    )

    status_relative = RUNTIME_ATTEMPT_STATUS_FIXTURE.relative_to(ROOT).as_posix()
    status_fixture = load_json(RUNTIME_ATTEMPT_STATUS_FIXTURE)
    require(
        set(status_fixture) == {"request", "result"},
        status_relative,
        "runtime-attempt status fixture must contain exactly request and result",
    )
    status_request = status_fixture["request"]
    require(
        isinstance(status_request, dict)
        and set(status_request) == {"attempt_id", "request_digest_sha256", "contract"},
        status_relative,
        "status request must require exact attached attempt, digest, and contract",
    )
    require(
        isinstance(status_request["request_digest_sha256"], str)
        and re.fullmatch(r"[0-9a-f]{64}", status_request["request_digest_sha256"]) is not None,
        status_relative,
        "status request must carry one exact canonical caller request digest",
    )
    require(
        status_request["contract"]
        == {
            "capability": "get_runtime_attempt_status",
            "schema": "undercurrent.request",
            "schema_version": "0.1.0-beta",
            "contract_version": "0.1.0-beta",
        },
        status_relative,
        "status fixture must carry exact contract-0.1.0-beta/schema-0.1.0-beta metadata",
    )
    status_result = status_fixture["result"]
    require(
        isinstance(status_result, dict)
        and set(status_result) == {"attempt_id", "fencing_token", "binding", "snapshot"},
        status_relative,
        "status result must expose only identity, fence, safe binding, and snapshot",
    )
    require(
        set(status_result["binding"])
        == {
            "caller_cell_identity",
            "request_digest_sha256",
            "launch_profile_digest_sha256",
            "environment_binding_digest_sha256",
            "input_digest_sha256",
        },
        status_relative,
        "status binding must expose only safe verification identity and digests",
    )
    require(
        status_result["snapshot"].get("status") == "known_applied"
        and isinstance(status_result["snapshot"].get("session"), dict),
        status_relative,
        "known-applied status fixture must carry a required coherent session snapshot",
    )

    cancellation_relative = RUNTIME_CANCELLATION_FIXTURE.relative_to(ROOT).as_posix()
    cancellation = load_json(RUNTIME_CANCELLATION_FIXTURE)
    require(
        set(cancellation) == {"request", "status_request", "cancel_result", "status_result"},
        cancellation_relative,
        "runtime-cancellation fixture must contain exact request, status, and result records",
    )
    cancel_request = cancellation["request"]
    cancel_request_fields = {
        "mutation", "control_id", "command_digest_sha256", "target_attempt_id",
        "target_request_digest_sha256", "target_fencing_generation",
        "caller_cell_identity", "session_id", "expected_session_revision",
        "expected_lifecycle_revision", "expected_provider_id",
        "expected_provider_generation", "mode", "reason", "propagation_policy",
        "absolute_deadline_millis", "source_product", "tenant_id",
        "actor_context_ref", "authority_ref",
    }
    require(
        isinstance(cancel_request, dict) and set(cancel_request) == cancel_request_fields,
        cancellation_relative,
        "cancel request must carry every exact target, control, envelope, and mutation binding",
    )
    require(
        set(cancel_request["mutation"]) == {"idempotency_key", "attempt", "contract"},
        cancellation_relative,
        "cancel mutation context must be exact and contain no compatible unknowns",
    )
    require(
        cancel_request["mutation"]["contract"] == {
            "capability": "cancel_runtime_attempt",
            "schema": "undercurrent.request",
            "schema_version": "0.1.0-beta",
            "contract_version": "0.1.0-beta",
        },
        cancellation_relative,
        "cancel request must carry the exact current product contract",
    )
    semantic = {
        "digest_domain": "undercurrent.runtime.cancel.command/v0.1.0-beta",
        **{
            key: cancel_request[key]
            for key in (
                "control_id", "target_attempt_id", "target_request_digest_sha256",
                "target_fencing_generation", "caller_cell_identity", "session_id",
                "expected_session_revision", "expected_lifecycle_revision",
                "expected_provider_id", "expected_provider_generation", "mode", "reason",
                "propagation_policy",
            )
        },
    }
    for key in ("target_fencing_generation", "expected_session_revision",
                "expected_lifecycle_revision", "expected_provider_generation"):
        semantic[key] = str(semantic[key])
    semantic_digest = hashlib.sha256(
        json.dumps(semantic, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    require(
        semantic_digest == cancel_request["command_digest_sha256"]
        == "d696e602e1194d973a1fddbfe01f8a8f241f0fb0b9c1a0a0bd60525d83aa780e",
        cancellation_relative,
        "caller cancellation command digest must match the canonical Undercurrent cancellation vector",
    )
    require(
        cancel_request["mutation"]["attempt"] == {
            "attempt_id": cancel_request["control_id"],
            "fencing_token": cancel_request["target_fencing_generation"],
            "payload_digest_sha256": "1e92a02d2efdc29741e85a0f2c2110a3b296626c753468f8c2df47af2e14215a",
        },
        cancellation_relative,
        "internal product-request fingerprint must remain distinct and exactly attempt-bound",
    )
    status_request = cancellation["status_request"]
    require(
        isinstance(status_request, dict)
        and set(status_request) == {
            "control_id", "command_digest_sha256", "target_attempt_id",
            "target_request_digest_sha256", "session_id", "expected_provider_id",
            "expected_provider_generation", "source_product", "tenant_id",
            "actor_context_ref", "authority_ref", "contract",
        },
        cancellation_relative,
        "cancellation status request must contain only exact lookup pins and call context",
    )
    require(
        status_request["contract"] == {
            "capability": "get_runtime_cancellation_status",
            "schema": "undercurrent.request",
            "schema_version": "0.1.0-beta",
            "contract_version": "0.1.0-beta",
        },
        cancellation_relative,
        "cancellation status request must carry the exact current product contract",
    )
    target_fields = {
        "target_attempt_id", "target_request_digest_sha256", "target_fencing_generation",
        "caller_cell_identity", "session_id", "expected_session_revision",
        "expected_lifecycle_revision", "expected_provider_id", "expected_provider_generation",
    }
    control_fields = {
        "control_id", "command_digest_sha256", "idempotency_key", "mode", "reason",
        "propagation_policy", "deadline_millis", "source_product", "tenant_id",
        "actor_context_ref", "authority_ref",
    }
    for result_name in ("cancel_result", "status_result"):
        result = cancellation[result_name]
        require(
            isinstance(result, dict) and set(result) == {"target", "control", "state"},
            cancellation_relative,
            f"{result_name} must echo exact target, control, and discriminated state",
        )
        require(set(result["target"]) == target_fields, cancellation_relative, "result target binding is incomplete")
        require(set(result["control"]) == control_fields, cancellation_relative, "result control binding is incomplete")
        require(
            result["target"] == {key: cancel_request[key] for key in target_fields},
            cancellation_relative,
            "result target must exactly echo every admitted target pin",
        )
        expected_control = {
            "control_id": cancel_request["control_id"],
            "command_digest_sha256": cancel_request["command_digest_sha256"],
            "idempotency_key": cancel_request["mutation"]["idempotency_key"],
            "mode": cancel_request["mode"],
            "reason": cancel_request["reason"],
            "propagation_policy": cancel_request["propagation_policy"],
            "deadline_millis": cancel_request["absolute_deadline_millis"],
            "source_product": cancel_request["source_product"],
            "tenant_id": cancel_request["tenant_id"],
            "actor_context_ref": cancel_request["actor_context_ref"],
            "authority_ref": cancel_request["authority_ref"],
        }
        require(result["control"] == expected_control, cancellation_relative, "result control must exactly echo admission context")
        require(
            result["state"].get("status")
            in {"accepted_stopping", "applied_terminated", "already_terminal", "unknown"}
            and set(result["state"]) == {"status", "session"}
            and result["state"]["session"].get("session_id") == cancel_request["session_id"],
            cancellation_relative,
            "result must carry one supported durable state and exact session identity",
        )


SAFE_REFERENCE_KEYS = {
    "bearer_token_file",
    "certificate_chain",
    "private_key",
    "client_authority",
}
FORBIDDEN_INLINE_KEY_PARTS = ("password", "bearer_token", "api_key", "credential", "private_key_pem")


def reject_inline_secrets(value, path="$"):
    if isinstance(value, dict):
        for name, child in value.items():
            lowered = name.lower()
            if name not in SAFE_REFERENCE_KEYS and any(part in lowered for part in FORBIDDEN_INLINE_KEY_PARTS):
                raise SchemaViolation(path, f"inline credential property {name!r} is forbidden")
            reject_inline_secrets(child, f"{path}.{name}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            reject_inline_secrets(child, f"{path}[{index}]")


def validate_config_semantics(config):
    reject_inline_secrets(config)
    require(not config.get("paths", {}), "$.paths", "storage paths derive exclusively from UNDERCURRENT_HOME")
    require("endpoint" not in config.get("local", {}), "$.local", "endpoint derives exclusively from UNDERCURRENT_HOME")


def decode_pointer(pointer):
    if not pointer.startswith("/"):
        raise SystemExit(f"error: invalid fixture mutation path {pointer!r}")
    return [part.replace("~1", "/").replace("~0", "~") for part in pointer[1:].split("/")]


def apply_mutation(document, mutation):
    parts = decode_pointer(mutation["path"])
    parent = document
    for part in parts[:-1]:
        parent = parent[int(part)] if isinstance(parent, list) else parent[part]
    leaf = parts[-1]
    operation = mutation["operation"]
    if isinstance(parent, list):
        index = int(leaf)
        if operation == "add":
            parent.insert(index, mutation["value"])
        else:
            parent[index] = mutation["value"]
    else:
        if operation == "replace" and leaf not in parent:
            raise SystemExit(f"error: fixture replace target {mutation['path']!r} does not exist")
        parent[leaf] = mutation["value"]


schemas = {
    PACKAGE_SCHEMA_PATH: load_json(PACKAGE_SCHEMA_PATH),
    CONFIG_SCHEMA_PATH: load_json(CONFIG_SCHEMA_PATH),
    FIXTURE_SCHEMA_PATH: load_json(FIXTURE_SCHEMA_PATH),
}
for schema_path, schema in schemas.items():
    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        raise SystemExit(f"error: {schema_path.relative_to(ROOT)} is not JSON Schema 2020-12")
    ensure_strict_objects(schema)

public_surfaces = [
    MANIFEST_PATH,
    CONFIG_SCHEMA_PATH,
    ROOT / "README.md",
    ROOT / "docs/getting-started/bundles.md",
    ROOT / "docs/reference/compatibility.md",
]
# Future gateway discussion is allowed; the retired artifact and configuration
# remain forbidden by this scanner and strict config negative fixtures.
forbidden_public_patterns = {
    r"UNDERCURRENT_SOCKET_PATH": "retired public socket override",
    r"contract_compatibility": "prerelease contract compatibility field",
    r"schema_compatibility": "prerelease schema compatibility field",
    r"--no-auto-daemon": "implicit daemon launch compatibility flag",
    r"uc-gateway": "retired gateway artifact",
    r"schema downgrade": "unsupported downgrade claim",
}
for surface_path in public_surfaces:
    try:
        body = surface_path.read_text(encoding="utf-8")
    except OSError as error:
        raise SystemExit(f"error: cannot read public surface {surface_path.relative_to(ROOT)}: {error}") from error
    for pattern, description in forbidden_public_patterns.items():
        if re.search(pattern, body, flags=re.IGNORECASE):
            raise SystemExit(f"error: {surface_path.relative_to(ROOT)} contains {description}")

manifest = load_json(MANIFEST_PATH)
validate(manifest, schemas[PACKAGE_SCHEMA_PATH], schemas[PACKAGE_SCHEMA_PATH])
validate_manifest_semantics(manifest)
validate_product_contract_fixtures()

valid_config_paths = sorted(FIXTURE_ROOT.glob("*.valid.json"))
if len(valid_config_paths) < 2:
    raise SystemExit("error: explicit and default local configuration fixtures are required")
for config_path in valid_config_paths:
    config = load_json(config_path)
    validate(config, schemas[CONFIG_SCHEMA_PATH], schemas[CONFIG_SCHEMA_PATH])
    validate_config_semantics(config)

negative_paths = sorted(FIXTURE_ROOT.glob("*.invalid.json"))
if len(negative_paths) < 12:
    raise SystemExit("error: at least twelve package conformance negative fixtures are required")
for fixture_path in negative_paths:
    fixture = load_json(fixture_path)
    validate(fixture, schemas[FIXTURE_SCHEMA_PATH], schemas[FIXTURE_SCHEMA_PATH])
    base_path = ROOT / fixture["base"]
    candidate = copy.deepcopy(load_json(base_path))
    apply_mutation(candidate, fixture["mutation"])
    target_schema = schemas[PACKAGE_SCHEMA_PATH] if fixture["target"] == "package" else schemas[CONFIG_SCHEMA_PATH]
    try:
        validate(candidate, target_schema, target_schema)
        if fixture["target"] == "package":
            validate_manifest_semantics(candidate)
        else:
            validate_config_semantics(candidate)
    except SchemaViolation as error:
        if error.code != fixture["expected_code"]:
            raise SystemExit(f"error: {fixture_path.name} returned {error.code}, expected {fixture['expected_code']}") from error
        if error.path != fixture["expected_path"]:
            raise SystemExit(f"error: {fixture_path.name} failed at {error.path}, expected {fixture['expected_path']}") from error
    else:
        raise SystemExit(f"error: negative fixture {fixture_path.name} unexpectedly passed")

print(
    "package conformance passed: "
    f"1 manifest, {len(schemas)} schemas, {len(valid_config_paths)} valid configs, "
    f"{len(negative_paths)} negative fixtures"
)
