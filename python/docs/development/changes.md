# Extend and verify the Python SDK

Use this guide for Python SDK 0.1.0-beta changes. Start in the repository root with
Python 3.11+ available. The focused Python tests below need no running daemon.

## Extend at the owning boundary

1. Verify the owning product DTO, registry, authorization, mutation effect
   contract, and capability negotiation. Use the
   [source map](../README.md#ownership-and-source-map) to find their owners.
2. Add the supported method and conversion in `client.py`. Export a new value
   through `__init__.py` only when needed; keep schema details in the protocol.
3. Check whether the bridge needs mutation preparation or a new stream kind.
   Its current subscribe whitelist permits only session watch and session events.
   Preserve the public SDK boundary; daemon storage and private session-host IPC
   are not Python extension endpoints.
4. Update the affected consumer guide and boundary diagrams, then select the
   checks below for the behavior changed.

Preserve correlation, payload bounds, finite worker limits, per-stream ACK
flow control, explicit gap handling, and cleanup on exceptions/cancellation.

For state helper changes, test behavior against transient events and gaps,
not just matching snapshots. For input, distinguish before-effect rejection,
after-write settlement failure, and application acknowledgment.

Changing an error taxonomy belongs in its protocol registry and must preserve
absent metadata across the bridge.

## Verify a change

Run focused checks from the repository root. The Python tests need no daemon,
network credentials, or third-party test package:

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=python/src \
  python3 -m unittest discover -s python/tests -v
```

### Python input and wait tests

[Input tests](../../tests/test_input.py) cover text/paste/auto encoding, Enter
combinations at multiple input sizes, and bad paste types. They mock `_request`;
they do not prove PTY delivery, Rust validation, or deduplication.

[Wait tests](../../tests/test_wait.py) cover predicate composition, inclusive
revision floors, transient/coalesced contradictions, evidence refresh, resync,
journal gaps, disconnect, terminal mismatch, timeout/cancellation cleanup, and
argument rejection. They simulate the watch and journal; they do not exercise
real subprocess transport.

### Rust command and client tests

[Command tests](../../../crates/undercurrent-application/src/commands/command_tests.rs)
cover durable/effect ordering, start/stop recovery, duplicate/ambiguous input,
and revision guards using controlled ports.

[Rust client tests](../../../crates/undercurrent-sdk/tests/product_client.rs)
cover error metadata, request cancellation/correlation, capability preflight
for guarded input/stop, and bounded stream recovery.

### Native integration checks

The [input limits script](../../../tests/e2e/cli/input-limits.py) checks configured
finite-input boundaries through the real CLI and Python bridge. Build the matching
native binaries first, then run `python3 tests/e2e/run.py --test input-limits
--bin-dir target/debug` from the repository root. It creates isolated daemon homes.

The [input round trip](../../examples/input_roundtrip.py) is a runnable Linux
consumer smoke example with an isolated daemon. It checks application
acknowledgment and cleanup; its syntax alone does not prove live behavior.

### Select the checks for a Rust change

For bridge or Rust changes, select package-level checks such as
`cargo test --locked -p undercurrent-sdk --test product_client` and
`cargo clippy --locked -p undercurrent-python-bridge --no-deps -- -D warnings`.
Use `scripts/check-architecture.py --positive` for dependency/surface policy.
The repository's broader runner is
[`scripts/validate.py`](../../../scripts/validate.py), including
the warning-denied Rustdoc stage; that is a wider integration gate, not a reason
to build the entire workspace for a Python prose edit.

For documentation changes, use the [shared verification guide](../../../docs/development/verification.md)
for link checks and rendered Mermaid inspection.

## Verification scope

The [packaging workflow](../../../.github/workflows/python-sdk.yml) installs the
built wheel and runs unit tests on Python 3.11–3.14 on Linux. These checks do not
establish live-daemon or cross-platform integration qualification. Verify the
examples against matching binaries in an isolated home before declaring live
qualification. Rust native platform support alone does not establish Python
integration qualification. The unit tests simulate requests and observations;
they cannot validate bridge startup or native process IO.

The [package manifest](../../pyproject.toml) declares Python 3.11+, no third-party
runtime dependencies, and setuptools 77.0.3+ for wheel builds. The wheel contains
Python code and typing metadata; the bridge and daemon must be supplied
separately. The [workspace manifest](../../../Cargo.toml) requires Rust 1.95+
and the [toolchain file](../../../rust-toolchain.toml) pins 1.95.0 for source builds.

[Python maintainer documentation](../README.md)
