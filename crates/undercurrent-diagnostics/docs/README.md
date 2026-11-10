# Maintain executable failure evidence

The executable adapter resolves its home without loading configuration, then
installs a `Recorder`. Construction does not create files. Only actual failure,
panic, and process-start capture writes evidence. Supported external Rust clients
remain `aixand-undercurrent` and `aixand-undercurrent-protocol`.

`Recorder::save` records machine categories and typed OS error kinds/codes.
It never serializes arbitrary `Error::Display`, `Error::Debug`, or panic payloads.
Request adapters use a synchronous `IncidentScope` to associate failures with
the caller's support reference. A reference is not an idempotency key, authority,
or proof of effect absence.

| Artifact under the selected home | Bound and meaning |
| --- | --- |
| `logs/incidents/*.json` | Up to 128 retained failure records, each at most 16 KiB. Concurrent writers can briefly exceed the count before pruning. |
| `logs/incidents/*.backtrace` | Up to eight panic backtraces, each at most 64 KiB. Excluded from ordinary support export because they can contain build paths. |
| `logs/build-daemon.json`, `logs/build-session-host.json` | Last started executable's version, optional source revision, platform, PID, and capture time. These are retained evidence, not a probe of the current process. |

Independent files avoid the daemon's lossy logging queue. Unix publication uses
an owner-only temporary file and atomic rename/link; the Windows adapter owns
private access. A full disk, inaccessible home, process kill, or failure in the
capture path can still lose evidence. Synchronizing file contents does not make
the containing directory a durable commit journal. Capture errors never replace
the original operation result.

The panic hook is installed only by executables. It omits the panic payload and
attempts a separate backtrace and failure record; it does not promise crash dumps
for native faults, forced termination, or allocation failure.

Run `cargo test -p undercurrent-diagnostics` from the repository root with the
pinned toolchain. The subprocess panic test verifies evidence without a log
worker; other tests check private modes, bounds, source redaction, and refusing
to replace existing output files. The CLI's
[support tests](../../undercurrent-cli/tests/support_collection.rs) check export
with invalid configuration and an absent daemon.

[Crate overview](../README.md)
