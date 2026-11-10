# Work on the macOS process adapter

Use this guide to verify a native-query change or add a bounded query. Run
commands from the repository root, the directory containing the workspace `Cargo.toml`.
These tasks target maintainers on **macOS**; no daemon, account, SDK client, or
privileged process inspection is needed for the crate's tests.

Read the [query contracts](../reference/query-contracts.md) before changing acceptance checks,
and the [runtime integration](../internals/runtime-integration.md) before changing how callers
interpret results. The crate has no feature flags. The entire library and its
tests are absent on non-macOS targets; an empty test run elsewhere cannot verify
Darwin behavior.

## Run the focused checks

Prerequisites are the Rust `1.95.0` toolchain pinned in
[rust-toolchain.toml](../../../../rust-toolchain.toml), its Clippy and rustfmt
components, locked Cargo dependencies, and a selected macOS SDK with a working
C compiler. The [workspace manifest](../../../../Cargo.toml) sets Rust 1.95 as the
minimum; [build.rs](../../build.rs) uses `cc` to compile SDK socket structures.
An existing Xcode Command Line Tools or Xcode setup supplies that native build
environment. This crate does not declare a minimum macOS release or qualify
every SDK/architecture combination merely by using `target_os = "macos"`.

### 1. Check the native build environment

Check the environment before testing:

```sh
uname -s
rustc --version
cargo --version
xcrun --find clang
xcrun --sdk macosx --show-sdk-path
cc --version
```

Expected: `Darwin`, the pinned Rust/Cargo toolchain, and available compiler/SDK
paths. If the selected developer directory or SDK is missing, repair that local
build prerequisite before diagnosing the adapter. If locked dependencies are
unavailable, obtain them through the project's dependency setup; do not remove
`--locked` or edit the lockfile to bypass dependency setup.

### 2. Run the self-query test

This is the shortest check of the real boundary:

```sh
cargo test --locked -p undercurrent-platform-macos --lib tests::reads_current_identity_arguments_and_directory -- --exact
```

Expected: one passing test covering its own identity, group membership, argument
buffer, working directory, and executable path. It prints no argument contents
and creates no persistent runtime state. Zero selected tests means this did not
exercise the macOS boundary; check the host and target before proceeding.

### 3. Run the crate checks

Run native tests serially:

```sh
cargo test --locked -p undercurrent-platform-macos --lib -- --test-threads=1
cargo clippy --locked -p undercurrent-platform-macos --all-targets -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --locked -p undercurrent-platform-macos --no-deps
cargo test --locked -p undercurrent-platform-macos --doc
```

Expected: five native unit tests pass, Clippy and Rustdoc have no warnings, and
the doctest phase succeeds (currently there are no runnable Rustdoc examples).
The listener tests inspect the test process itself, so serial execution avoids
one test's temporary listener changing another's expected `Absent` result.

Open `target/doc/undercurrent_platform_macos/index.html` after Rustdoc, or the
corresponding path under `CARGO_TARGET_DIR` if set. A generated empty page on a
non-macOS host is not API coverage. The self-query test above is the maintained executable
example.
If a test fails, use the [diagnosis guide](diagnosing-observations.md); if the C
fixture panics, check [temporary-file cleanup](#resources-and-cleanup) before
rerunning it.

### 4. Check affected runtime callers

For changes to identity or sampling interpretation, also run the relevant
runtime tests on macOS:

```sh
cargo test --locked -p undercurrent-runtime --lib process::macos_observation::tests:: -- --test-threads=1
cargo test --locked -p undercurrent-runtime --lib process::hosted::agent_ancestry_tests:: -- --test-threads=1
```

Expected: the macOS observer's seven tests and the ancestry test run, rather than
zero selected tests. These checks require the runtime's additional dependencies.
The ancestry test creates and reaps its own `/bin/sleep` child. It is separate
from the read-only self-query example and never requires attaching to an
existing user's process.

### 5. Include the integration gate when required

For integration validation, the current runner owns the workspace Rustdoc gate:

```sh
scripts/validate.py --stage rustdoc
```

The [runner](../../../../scripts/validate.py) selects the pinned toolchain,
sets Cargo offline mode, and runs warning-denied workspace Rustdoc with all
features. Its separate `tests` stage builds binaries and runs native tests and
doctests; a Rustdoc pass alone does not exercise OS behavior. Use this gate as part of workspace
integration after the focused crate and affected runtime checks.

## Resources and cleanup

The self-query and invalid-ID tests create no runtime state. Listener tests bind
ephemeral `127.0.0.1` ports and close them on drop. The high-descriptor test runs
an isolated child test process, raises that child's soft descriptor limit to
2048, opens 700 `/dev/null` handles, then drops its listener and files. A runner
must permit that soft limit and local socket binding.

The C contract test compiles a temporary executable with
`cc -Wall -Wextra -Werror` in a mode-0700 `uc-listener-contract-<test-pid>`
directory under the OS temporary directory. On its normal execution path it
removes the directory before asserting the fixture result. A panic during
compilation can leave the directory behind. After the test process has exited,
remove only the leftover directory for that test run; do not clean unrelated
temporary state. Cargo artifacts remain under the target directory and can be
reused. No Undercurrent runtime home or daemon shutdown is required.

## What the tests prove

### Native queries

Tests live in [lib.rs](../../src/lib.rs). Use the test name to locate its assertions.

**Self-query** — `reads_current_identity_arguments_and_directory`

Own PID, positive creation time, own group membership, nonempty arguments, cwd and executable agreement. Covers a native happy path, without short-structure fault injection.

**Invalid IDs** — `invalid_and_absent_processes_fail`

Zero and `u32::MAX` fail for identity, arguments, cwd, and group; an absent high PID fails identity. Does not explicitly test invalid executable-path inputs or every path branch.

**Listener lifetime** — `observes_real_listener_and_its_removal`

Binding a listener yields `Present`, dropping it yields `Absent`, and PID zero fails. Requires no competing listener in this test process.

**High descriptor** — `observes_listener_above_old_descriptor_limit`

Finds a listener behind 700 open files. Exercises a real high descriptor number below the production cap.

**Native failures** — `bounded_listener_failure_contracts`

Compiles the deterministic [C fixture](../../tests/listeners_contract.c) described below.

### Deterministic listener failures

The C fixture includes the production helper with substituted OS calls. It
checks descriptor counts 256, 257, 700, 4096, and 4097; bounded buffer sizes; at
most five growth snapshots plus final validation; and no socket certification
on overflow.

Its seven failure modes cover a permission-like failure, malformed size,
oversized reply, socket loss, descriptor reuse, changed final descriptor list,
and changed TCP state. All must return unavailable. This is deterministic query
coverage, not kernel stress testing: allocation failure, every socket kind/family,
and an errno taxonomy are not covered.

### Runtime interpretation

The [macOS observer tests](../../../undercurrent-runtime/src/process/macos_observation.rs)
use a fake process source. They cover empty argv positions, environment-name
projection, malformed/oversized commands, and incomplete names. Readiness cases
include children, truncation, refusal, PID reuse, group changes, and exec. They
also check listener ownership, cache age, Ready bypass, and changing PTY groups.
They do not exercise Darwin FFI.

The [hosted ancestry test](../../../undercurrent-runtime/src/process/hosted.rs),
`real_child_requires_exact_executable_and_root_incarnation`, checks a real child,
exact executable comparison, wrong executable rejection, and root-incarnation
mismatch. It verifies caller decisions without making process-table reads atomic.

There are no tests proving optimizer-resistant argument erasure or atomicity of
multiple kernel queries, and neither is promised. The cap and error branches for
identity, group, argument length, and path decoding have less deterministic fault
coverage than the C listener helper. Add targeted malformed-reply coverage when
changing those boundaries; a self-query happy path is insufficient for such a
change.

Broader foreground behavior is exercised by the shared
[platform detection fixture](../../../../tests/e2e/platform/test-platform-detection.py).
It needs built product binaries, zsh, bash, fish, and `cc`; it creates temporary
shell configuration and an isolated product home. It is an integration test that
starts processes, not a prerequisite for reading these docs or running the
adapter's self-query test. The [native smoke fixture](../../../../tests/e2e/platform/test-platform-contract.py)
also exercises daemon/session behavior. Keep their qualification separate from
the crate unit tests and inspect their current binary-path setup before use.

## Diagnose incomplete or unavailable evidence

Use [Diagnose process evidence](diagnosing-observations.md) to distinguish build
failures, native query errors, incomplete samples, and runtime rejection.

## Extend the boundary

1. Decide ownership first. A new bounded Darwin primitive belongs here; parsing,
   cache policy, profile selection, process signalling, persistence, and transport
   semantics remain in their existing runtime/application owners. Public API or
   dependency changes also require review against the
   [architecture policy](../../../../tests/architecture/allowed-graph.json).
2. Define a safe owned return type and explicit incomplete/unavailable semantics.
   Validate identifiers before FFI and decide fixed storage/work bounds before
   allocating. Explain why a negative result is authoritative enough for its
   intended use, including observed races and truncation.
3. Keep unsafe operations local with adjacent initialization, capacity, lifetime,
   and pointer-retention arguments. Use SDK layouts where needed; preserve the
   runtime's unsafe-code prohibition. If a new native source file is needed,
   update build inputs and rerun triggers as part of that behavior change.
4. Add meaningful fault cases beside the owning code. Listener work needs C
   malformed/changed-reply fixtures and native lifecycle coverage. Changes to
   identity, parsing, membership, or caching also need the runtime's fake-source
   tests and relevant peer-verification tests.
5. Review every caller's downgrade path. Preserve the difference between absent,
   truncated, and unavailable. Recheck privacy, allocation cleanup, synchronous
   blocking, and whether callers need additional before/after identity checks.
6. Run the focused native, Clippy, Rustdoc, and relevant runtime checks above;
   include workspace architecture and validation stages when affected.
   Update symbol comments, the owning contract explanation, and any affected
   Mermaid sequence or ownership diagrams. Render and visually inspect diagrams,
   including labels and connections in supported themes; parsing alone is not a
   visual check.

[Crate documentation](../README.md) · [Query contracts](../reference/query-contracts.md)
