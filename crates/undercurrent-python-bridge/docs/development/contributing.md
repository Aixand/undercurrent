# Develop and check the Python bridge

Build the adapter and verify a read-only connection before changing its
subprocess boundary. Then use the extension and verification sections to choose
the owning code and checks. Commands run from the
**repository root**, not this crate directory. The read-only example requires an
already running daemon; it never starts or stops managed sessions.

[Crate entry point](../../README.md) · [Implementation map](../internals/operation-flow.md#code-map) ·
[Protocol reference](../reference/stdio-protocol.md) · [Failure recovery](../internals/recovery.md)

## Prerequisites and version scope

- Use Rust/Cargo from the root [`rust-toolchain.toml`](../../../../rust-toolchain.toml):
  `1.95.0`, with the normal native compiler/linker for your target. The workspace
  declares Rust `1.95` and edition 2024; do not build from a detached copy of this
  manifest because it inherits workspace settings and path dependencies.
- Use Python 3.11 or newer, as declared in [`pyproject.toml`](../../../../python/pyproject.toml).
  Importing this source package needs no third-party runtime Python dependency.
  `PYTHONPATH=python/src` below avoids installing the package into an environment.
- Have a matching external daemon already running on the local machine, under the
  intended OS identity and runtime home. For daemon ownership and installation
  context, start at the [repository README](../../../../README.md). This crate does
  not implicitly launch it, install Rust executables, or create an embedded runtime.
- The shell commands here target Linux and a POSIX shell. See the
  [Python setup guide](../../../../docs/sdk/python/quickstart.md) for consumer setup.
  No Python bridge live qualification across platforms is established by this
  guide. The [local transport code](../../../undercurrent-transport-local/src/lib.rs)
  includes Linux/macOS Unix sockets and Windows named pipes; those adapters alone
  do not prove Python end-to-end qualification.

Build the Python package and bridge from the same revision. A wheel contains
Python code, so the Rust executable must be built separately. See
[version identifiers](../reference/stdio-protocol.md#version-identifiers) when
checking compatibility; matching package numbers alone are insufficient.

## Prepare the executable and Python import

1. Build the bridge from the repository root:

   ```sh
   cargo build --locked -p undercurrent-python-bridge
   ```

   Expected: Cargo produces `target/debug/uc-python-bridge`. If you configured
   `CARGO_TARGET_DIR`, use that directory in the next step.

2. Select the executable and check the example's imports:

   ```sh
   export UNDERCURRENT_PYTHON_BRIDGE="$PWD/target/debug/uc-python-bridge"
   PYTHONPATH=python/src python3 crates/undercurrent-python-bridge/examples/inspect_runtime.py --help
   ```

   Expected: argument help, without contacting a daemon. A missing module means
   the command ran outside the root or without `PYTHONPATH`. A missing bridge
   on the connection step means the executable path or build selection is wrong.

The executable itself has no `--help` mode; use the example's help, or read the
[stdio configuration reference](../reference/stdio-protocol.md#framing-and-startup).

## Check a connection without changing sessions

1. Run the maintained example against the intended runtime home:

   ```sh
   PYTHONPATH=python/src python3 crates/undercurrent-python-bridge/examples/inspect_runtime.py
   ```

   The default home is the Python client's resolved `~/.aixand/undercurrent`, unless
   `UNDERCURRENT_HOME` is set. To target another **already running** daemon, replace
   `/absolute/path/to/runtime-home` with its actual home:

   ```sh
   PYTHONPATH=python/src python3 crates/undercurrent-python-bridge/examples/inspect_runtime.py \
     --home /absolute/path/to/runtime-home
   ```

2. Verify the session list. An illustrative result for an empty runtime is:

   ```text
   Connected; 0 session(s)
   ```

   For a nonempty runtime it prints IDs, lifecycle states, and quoted names.
   Success proves that list requests completed through the bridge. Session
   readiness or health requires separate inspection.

3. Let the example exit. Its async context manager closes the bridge on success
   or error. It leaves the daemon and existing sessions running and creates no
   persistent state to remove.

If connection fails, inspect the printed category and follow
[startup recovery](../internals/recovery.md#fix-startup-and-framing-failures). This example
prints available product disposition and certainty on `UndercurrentError` and
does not automatically retry.

## Read an existing session's output

1. Copy an exact ID from the list into `SESSION_ID` below. This is a placeholder
   for a session ID. Supply the same `--home` if using a nondefault home.

   ```sh
   PYTHONPATH=python/src python3 \
     crates/undercurrent-python-bridge/examples/inspect_runtime.py \
     --session SESSION_ID --max-pages 4
   ```

2. Check the current revision/lifecycle and up to four 4,096-byte output pages.
   Each page prints offsets and Python `repr` bytes, so terminal controls are
   displayed rather than executed. The page limit bounds this check even if
   output keeps arriving.

3. Choose the next action from the result:

   | Result | Next action |
   | --- | --- |
   | Page limit reached | Re-run with the printed `--from-offset` to continue |
   | Caught up | Exit, or read again later from that offset; new output can still arrive and this is not an application completion signal |
   | Retention gap, exit 2 | Record the incomplete history. Resume at the printed earliest retained offset only if accepting that loss is appropriate |
   | Argument error, exit 2 with usage help | Fix the selection; offsets must be nonnegative and page limits positive |
   | Missing session, SDK error, or Python deadline, exit 1 | Fix the reported cause before repeating the read |

The example closes its bridge on success or error and leaves the session running.
It never silently jumps a gap or retries a mutation. Its source in
[`examples/inspect_runtime.py`](../../examples/inspect_runtime.py) is the single
maintained copy; it follows the [output continuation rules](../../../../docs/sdk/python/streams.md#read-retained-output).

## Inspect one raw envelope when debugging

Use this only to investigate the private adapter against an already running
daemon.

1. Set the intended home explicitly (the earlier example
   `--home` option does not change your shell environment), then launch:

   ```sh
   UNDERCURRENT_HOME=/absolute/path/to/runtime-home "$UNDERCURRENT_PYTHON_BRIDGE"
   ```

   Replace the path with the home of the already running daemon.

2. Wait for `{"type":"ready","protocol":"0.1.0-beta"}`, then paste this one-line read request:

   ```json
   {"id":"probe-1","op":"request","body":{"operation":"get_daemon_status"},"timeout":5}
   ```

   Expected: a `result` with `id: "probe-1"` and a `get_daemon_status` product result,
   or an ID-correlated error. Daemon-specific values are intentionally not sample
   constants. On a terminal, input echo is separate from the bridge's output.

3. Wait for the response, then close stdin (Ctrl-D on an empty line in a POSIX
   terminal) to exit. Do not pipe the request followed immediately by EOF: the
   [EOF cleanup sequence](../internals/recovery.md#close-and-cancellation-are-different-actions)
   can close the client before the request finishes.

Only stdout is protocol; keep diagnostic capture private because successful
responses can contain session or terminal content.

## Extend the correct owner

| Intended change | Owning code and required review |
| --- | --- |
| New Python convenience operation | [Python `Client`](../../../../python/src/undercurrent/client.py), models/errors, and the existing product DTO/registry; verify complete request and response shapes before exposing it |
| Different mutation preparation | [`prepare`](../../src/main.rs), protocol compatibility validation, daemon boundary and idempotency scope; explicitly assess attempt/fence preservation and exact retry payloads |
| New stdio command or envelope field | Both `Command`/`run` and Python `Bridge`; decide compatibility of ready protocol `1`, defaults, validation, errors, and teardown before shipping both halves |
| New stream kind | Admission match, worker projection, Python iterator/queue, gap behavior, cancellation, and stream-open metadata; terminal and agent streams are currently excluded deliberately by admission |
| Larger limits or different scheduling | Atomic finite count, active-entry capacity, Python semaphore/queue, and SDK profile bounds; account for encoded byte growth, slow stdout, stalled ack, synchronous open, and worker cleanup |
| Authentication, launch, durable state, or retry authority | Public SDK/protocol and owning daemon/application code; do not add direct runtime/store/private-wire dependencies here |
| More diagnostics | Preserve stdout framing and privacy; Python currently discards stderr, and the bridge exposes no SDK pressure/telemetry command |

Preserve the distinction between the supplied product request and
`MutationContext::current`: simply adding a Python method for a mutation outside
the five normalized cases does not give it current contract metadata. For stream
changes, retain activation-before-first-item ordering and release both SDK receive
and acknowledgment waits on teardown. A new public method also needs a consumer
example and tests at the language boundary it changes, not just a DTO test.

## Verify a change

Run these focused checks from the repository root with the prerequisites above.
They require no live daemon and leave no managed sessions to clean up:

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=python/src python3 -m unittest discover -s python/tests -v
cargo clippy --locked -p undercurrent-python-bridge --no-deps -- -D warnings
cargo test --locked -p aixand-undercurrent --test product_client --test client_contract
RUSTDOCFLAGS=-Dwarnings cargo doc --locked -p undercurrent-python-bridge --no-deps
```

Expected: the Python suite and SDK tests pass, Clippy reports no warnings, and
Rustdoc generates documentation without warnings. A failure should be investigated
at the owning boundary in the table below.

There is no bridge library target or bridge-local Rust test module. Rustdoc checks
comments and links; an empty crate test run cannot establish framing or subprocess
behavior. Python tests below replace the transport with fakes; they do not launch
a bridge or daemon.

| Evidence | What it checks; what it does not establish |
| --- | --- |
| [Python input tests](../../../../python/tests/test_input.py) | `test_modes_and_submission` checks auto-text, paste, literal text, optional Enter, and argument rejection; bypasses `_request` transport |
| [Python wait tests](../../../../python/tests/test_wait.py) | Transient state, coalesced observations, gaps, revision floors, disconnected inspection, terminal state, timeout and cancellation cleanup; fake observation source |
| [SDK product tests](../../../undercurrent-sdk/tests/product_client.rs) | Typed request/stream sharing, cancellation correlation, capability checks, category/disposition preservation, and retained recovery profile behavior; no bridge stdio |
| [SDK connection tests](../../../undercurrent-sdk/tests/client_contract.rs) | Pending-request teardown, stream resume over replacement connections, recovery exhaustion and bounded backoff; no Python subprocess |
| [Application command tests](../../../undercurrent-application/src/commands/command_tests.rs) | Start commit/compensation ordering; delivery replay, payload conflicts, indeterminate writes and settlement failure |
| [Output tests](../../../undercurrent-application/tests/terminal_output.rs) | Retention gaps, returned range versus available end, digest integrity and bounded output |
| [Daemon dispatch tests](../../../undercurrent-daemon/tests/product_dispatch.rs) | Exact request-idempotency response replay and error disposition behavior |
| [Architecture policy/gate](../../../../tests/architecture/README.md) | Supported external dependencies and negative boundary fixtures; run `python3 scripts/check-architecture.py --positive` when available |

For wider changes, the existing [project validation runner](../../../../scripts/validate.py)
owns `clippy`, `tests`, and warning-denied `rustdoc` stages. Use its normal project
validation environment; the `tests` stage builds workspace binaries and runs
workspace tests and doctests, and `rustdoc` builds workspace docs. Select those
wider stages when a change affects workspace integration:

```sh
scripts/validate.py --stage tests
scripts/validate.py --stage rustdoc
```

No checked-in bridge-specific harness currently covers malformed/oversized stdio,
worker limits, late responses, stalled acknowledgment, slow subscription open,
or EOF/broken-output handling end to end. Before changing those behaviors, add
focused boundary tests with fake SDK/caller peers and, where necessary, an
isolated temporary daemon home. Do not infer end-to-end bridge coverage from
Python mocks or tests outside this repository.

After Markdown changes, check all relative links/anchors and ensure diagrams
retain `accTitle`/`accDescr`. Render the Mermaid sources beside the explanations
and visually inspect ordering, labels, clipping, and readability in the intended
display themes. A syntax/accessibility-marker check alone is insufficient.

[Crate documentation](../README.md)
