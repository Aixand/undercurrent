# Verify and extend the local transport

Use this guide to reproduce framing and endpoint behavior and select checks
for a proposed change. Commands run from the **repository root**, the directory
containing the workspace [Cargo.toml](../../../../Cargo.toml), not from `crates/`.
Use the pinned Rust **1.95.0** toolchain in
[rust-toolchain.toml](../../../../rust-toolchain.toml) with its rustfmt/clippy
components and the dependencies from the existing lockfile. Standard native
compiler/linker prerequisites apply to the selected OS. No installed daemon,
credentials, token file, network listener, or real managed process is needed
for the focused crate tests or example.

The commands below are verification instructions, not a claim that checks
have run on every platform. Native Windows checks require Windows; macOS
endpoint behavior requires macOS. Cross-compiling alone cannot verify peer
identity, filesystem behavior, or pipe handle lifetimes.

## Exercise framing without a daemon

The maintained [framing round-trip example](../../examples/framing_roundtrip.rs)
uses an in-memory stream. It tests internal framing without authentication or
a filesystem endpoint.

1. From the repository root, run:

   ```sh
   cargo run --locked -p aixand-undercurrent-transport-local --example framing_roundtrip
   ```

2. Check for exit status zero and this expected application output:

   ```text
   verified one complete UCMP Ping and clean EOF
   ```

   This is expected output, not a recorded qualification result. The example
   encodes a UCMP Ping, checks exact byte equality, then checks clean EOF after
   sink closure. A one-byte-capacity Tokio duplex stream forces concurrent
   send/receive because it cannot hold a complete frame.

3. On failure, use the failing check to choose the next investigation:

   | Failure | Next check |
   | --- | --- |
   | Cargo cannot build | Confirm the pinned toolchain, native compiler/linker, and locked dependencies. Keep `--locked`; fix the environment before testing transport behavior. |
   | Five-second timeout or write failure | Check that send and receive still run concurrently and that the sink closes. See [framing write behavior](../reference/framing.md#buffer-and-cancellation-behavior). |
   | Unexpected EOF or byte mismatch | Compare the wire-encoded frame and sink closure ordering. Run the [framing tests](../../tests/framing.rs) for truncation, oversize, and cancelled receives. |

The example launches no daemon and leaves no endpoint to remove. Its in-memory
resources disappear at exit; Cargo retains ordinary target-directory build
artifacts. Continue with the focused tests for native endpoint behavior.

## Run focused checks

The basic crate tests exercise in-memory framing and the native adapter for the
host target. Unix endpoint tests use canonical temporary roots and clean up
their temporary directories after releasing handles.

```sh
cargo test --locked -p aixand-undercurrent-transport-local
cargo test --locked -p aixand-undercurrent --test local_boundary
```

Expected result: Cargo reports passed tests with exit status zero. On Linux,
the three ignored cross-user entries (including their child helper) are not
qualification evidence until the namespace runner executes the two parent
cases. `windows_endpoint` has no runnable cases on Unix; `endpoint` has no
runnable cases on Windows. Confirm test names and counts instead of treating
a zero-test result as platform validation.

If `browser_frontend_absence` fails, inspect the paths named in its assertion.
Its marker scan includes JavaScript and HTML files anywhere outside its excluded
directories, so documentation preview tooling can also trigger it. Establish
whether a reported file is a product frontend before changing the applicability
claim; do not treat that failure as a native transport regression or silently
remove the check.

### Qualify Linux cross-user identity

On Linux with user/mount namespaces enabled, `unshare` available, and subordinate
UID/GID mapping configured for `--map-auto`, use the existing runner:

```sh
python3 scripts/validate.py --stage local_identity
```

The [security script](../../../../scripts/checks/check-local-ipc-security.py) builds the
test as the invoking user, then runs the two parent tests in mapped namespaces
with a private tmpfs `/tmp` and a 30-second subprocess timeout. Host root is
not required. An unavailable mapping or namespace is an environment failure,
not a passed identity test. Do not run its ignored UID-changing helper directly
against host credentials. The script removes its temporary build inventory;
namespace processes, mounts, and test directories are temporary. Validation
logs remain under `target/standalone-validation/`.

### Check documentation comments and examples

For documentation comments and examples, use the package checks, then the
existing repository validation stages when qualifying the integrated change:

```sh
RUSTDOCFLAGS=-Dwarnings cargo doc --locked -p aixand-undercurrent-transport-local --no-deps
cargo test --locked -p aixand-undercurrent-transport-local --doc
cargo check --locked -p aixand-undercurrent-transport-local --examples
python3 scripts/validate.py --stage architecture
python3 scripts/validate.py --stage rustdoc
```

The environment-prefix command shown is POSIX shell syntax. On PowerShell,
set `$env:RUSTDOCFLAGS = '-Dwarnings'` before `cargo doc` instead. The repository
runner requires Bash/Python, sets Cargo offline mode, uses the pinned toolchain,
and its Rustdoc stage covers the whole workspace with warnings denied. Missing
cached dependencies must be resolved by the development environment; do not
silently remove `--locked` or alter a manifest to make a documentation check
pass. Generated docs are
under `target/doc/undercurrent_transport_local/` unless `CARGO_TARGET_DIR` is set.

Mermaid blocks live beside their explanations. Validate and render all changed
diagrams with the project's configured renderer, then inspect labels,
relationships, clipping, and readability in supported themes. Parser success
alone is insufficient. Check relative links and anchors from each owning page.

## Find the relevant test suite

### Adapter checks

- [framing.rs](../../tests/framing.rs): Fragmented/coalesced frames survive cancelled receives; oversized header rejected before body allocation; truncated header is not clean EOF; tiny-buffer writes preserve bytes.

- [endpoint.rs](../../tests/endpoint.rs): Owner connects; active listener cannot be replaced; stale recovery; unsafe modes, symlinks, and regular files rejected/preserved; relative/traversing/long paths rejected.

- [windows_endpoint.rs](../../tests/windows_endpoint.rs): Both ends authenticate; repeated accept maintains ownership; cancelled accept remains usable; eventual name release; missing endpoint has no fallback.

- [cross_user.rs](../../tests/cross_user.rs): Another mapped UID denied; kernel check works independently of metadata; client rejects a foreign server even after metadata is made trusted.

- [browser_frontend_absence.rs](../../tests/browser_frontend_absence.rs): Records conditional FR-MUX-002 applicability by checking absence of browser source markers. This is not browser transport support or a browser security test.

- [error.rs unit test](../../src/error.rs): Concrete source retained, but source text absent from `Debug` and `Display`.

- [server.rs maintenance test](../../src/server.rs): Idle timer expires every due journal without pressure or traffic.

- [cleanup tests](../../src/server/cleanup_tests.rs): Abort, runtime destruction, injected accept failure, full writer channel, cancellation during retirement, callback panics, and client-ID reservation/reuse.

### Integration checks

[SDK local_boundary.rs](../../../undercurrent-sdk/tests/local_boundary.rs): Binary payloads and 300 streams on one connection; large owner-backed writes; typed admission overload; pre-cancelled setup; drain with active request; same-epoch resume; epoch-change gap; setup cancellation/deadline; silent-handshake shutdown; component-wise queue negotiation; slow-reader isolation; bind rejects undersized adapter bound.

[Daemon product_dispatch.rs](../../../undercurrent-daemon/tests/product_dispatch.rs): Typed product errors, exact idempotent retry and response loss, cooperative cancellation, dispatcher queue saturation, stream activation, and publication backpressure.

## Place a change in its owning layer

| Change | Owner and required evidence |
| --- | --- |
| Endpoint path validation, Unix locking, or stale cleanup | `endpoint.rs` / `unix.rs`; preserve fail-closed checks and run native endpoint plus Linux identity tests. Add a focused regression for a new failure path rather than loosening validation. |
| Windows name/ACL/identity primitive | Windows platform crate; local adaptation in `windows.rs`. Require native pipe, cancellation, exclusivity, ACL, and peer checks. |
| Byte framing, buffer bound, cancellation, UCLC | `framed.rs`; exercise fragmented input, truncated/oversized input, partial output, and matching SDK interpretation. Update the [frame reference](../reference/framing.md) for a changed contract. |
| Listener or worker limits, timers, shutdown | `server.rs`; check idle maintenance and relevant SDK admission, slow-reader, cancellation, and draining cases. Keep dispatcher calls bounded and avoid blocking socket I/O on writer bridge threads. |
| UCMP/UCHS format or compatibility | Wire owns codecs and registries; review both client and server, wire fixtures, and negotiation tests. Do not define a second semantic transport here. |
| Request correlation, stream admission, replay semantics | Transport core and multiplex; preserve one logical client registration, frame-write confirmation ordering, byte/message bounds, and explicit gap behavior. |
| Product operation, durable commit, terminal retention, mutation retry | Protocol, daemon dispatcher, application, and storage owners; update their contracts/tests. No new runtime or database dependency belongs in this adapter. |
| New public Rust API | Follow supported external SDK/protocol policy; internal `pub` visibility does not grant consumer support. |

Maintain the [architecture graph](../../../../tests/architecture/allowed-graph.json)
policy, not just successful compilation. The CLI must not import this crate
directly. Read [architecture tests](../../../../tests/architecture/README.md)
before changing crate edges or platform surfaces.

## Limits of existing evidence

Source review establishes intended code paths, while focused tests establish
only the cases they execute. The existing framing suite tests receive
cancellation and partial writes, but has no dedicated cancelled-send regression
or complete `set_maximum` transition matrix. The idle maintenance unit test uses
a shorter cadence; it does not measure production scheduling latency. The
cross-user qualification is Linux-only. Windows native tests do not by
themselves establish cross-account qualification on every Windows deployment.
The cleanup tests exercise selected cancellation points and inject an accept
failure; they do not exhaust every schedule or establish progress for callbacks
that block permanently. Check actual writer/session retirement as well as
counters when extending these cases.

[Crate entry point](../../README.md) · [Recovery guide](troubleshooting.md)

[Crate documentation](../README.md)
