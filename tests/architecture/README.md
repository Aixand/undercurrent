# Undercurrent architecture gate

The gate requires `cargo`, `python3`, and GNU-compatible `nm` on `PATH`. It reads the checked-in lockfiles and runs without network access after locked dependencies are available locally.

Run the complete graph, negative-fixture, consumer-tree, consumer-build, and symbol checks from the Undercurrent project root:

```sh
scripts/check-architecture.py --all
```

Run one component when diagnosing a failure:

```sh
scripts/check-architecture.py --positive
scripts/check-architecture.py --consumer
scripts/check-architecture.py --negative tests/architecture/negative/domain-outward-edge.json
```

`allowed-graph.json` defines the current workspace and permitted dependencies.
Negative fixtures mutate an in-memory copy of that graph and must fail with
their declared rule. Unknown workspace members fail `ARCH_WORKSPACE_DISPOSITION`;
unapproved local path dependencies fail `ARCH_FOREIGN_PRODUCT_IMPLEMENTATION`.
The fixtures exercise these general boundaries without naming former products
or maintaining a list of deleted crates. Layer import guards check the current
domain, application, and CLI boundaries; normal Rust compilation rejects
undeclared crate imports.

The consumer crate is wiring/client-only and uses the public protocol and SDK
without implementing server routing or runtime behavior.

No external contract path package is allowlisted. Product definitions are owned
by `aixand-undercurrent-protocol`, and framing definitions by `aixand-undercurrent-wire`. The
Python package's private `undercurrent-python-bridge` is a client adapter with
exactly two permitted product dependencies: the public protocol and Rust SDK.
It does not access the runtime, store, or private session-host protocol.
The
vendored terminal-parser exceptions are exact: `undercurrent-runtime` and
`undercurrent-cli` may consume `vt100` as a normal dependency at `vendor/vt100`.
The CLI group view renderer also consumes `vendor/vte` as a normal dependency for bounded
terminal-query decoding. These frontend parsers do not add a dependency on any
private runtime implementation. Changing the consumer, package name, path, or dependency kind
fails `ARCH_VENDORED_PATH_MISMATCH`. The registry reference dependencies are the
`vt100-reference` and `vte-reference` dev aliases in `undercurrent-runtime`, pinned
to crates.io `vt100 = 0.15.2` and `vte = 0.11.1` for differential tests.
Changes to the consumer, alias, version, source, or kind fail
`ARCH_VT100_REFERENCE_MISMATCH` or `ARCH_VTE_IDENTITY_MISMATCH`, respectively.

The positive gate also restricts persisted dispatch compatibility constructors
to the SQLite durable decoder in `sqlite/database.rs`.
Production request admission cannot construct a policy-less dispatch binding;
test-only compatibility fixtures remain outside this source guard.

Local CLI reset has one declared private module, `src/reset.rs`. It checks that
the product-owned session-host socket is inactive before deleting state; it sends
no UCHS frames or semantic commands. The module's native test owns a listener and
verifies the connection closes without bytes while state remains intact. The
architecture gate admits only that module's standard-library stream and listener
symbols. Other CLI modules, SDK access, asynchronous socket APIs and datagrams
remain forbidden; the CLI is not added to the private Unix package allowlist.
`test_private_reset_boundary.py` checks those distinctions in the `--all` gate.

The public native adapter is `aixand-undercurrent-transport-local`. The
`aixand-undercurrent-transport-core` owns socket-independent connection behavior and is
not counted as a second transport. SDK native I/O is delegated to the local
adapter; CLI still has only its narrow private reset exception. Additional
semantic transports and direct CLI imports of either transport crate remain forbidden.

The macOS-only `undercurrent-platform-macos` adapter is daemon-private. Only the
runtime may depend on it, and public consumer dependencies/symbols are rejected.
Its narrow native-call safety policy is documented in the adapter's README; the
runtime and other workspace crates retain their inherited unsafe-code prohibition.

The runtime also consumes the approved `vendor/portable-pty` 0.9.0 patch for
owned working-directory handles. Its source, MIT license, patch and provenance
are retained in that directory. It is excluded from workspace membership and
is not a first-party unsafe-code exception.
