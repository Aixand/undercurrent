# Prepare the real runtime workflows

Run complete local SDK programs with real processes and checked results. These
Linux examples need no model, credentials or external service. They target the
current development checkout, not an arbitrary older installation.

## Prepare matching tools

Follow [source setup](../../development/setup.md). Build the native tools from
the repository root with the pinned Rust toolchain:

```sh
cargo build --locked -p undercurrent-cli -p undercurrent-daemon \
  -p undercurrent-python-bridge -p undercurrent-mcp
export RUNTIME_BIN="$PWD/target/debug"
python3 -m venv /tmp/uc-doc-sdk
. /tmp/uc-doc-sdk/bin/activate
python -m pip install ./python
```

Keep `uc-daemon`, `uc-session-host`, `uc-python-bridge`, and (for the observer)
`uc-mcp` from matching builds. A source tree alone does not install Python imports.
Use the [consumer SDK guide](../../sdk/python/README.md) for ordinary installation.

## Choose a fresh work directory

Each command requires `--work` with a new, short path. Its private runtime home,
workers and evidence stay inside that directory. It refuses existing directories
and never uses the normal daemon home.

```sh
python docs/examples/runtime/workflows.py parallel-report \
  --work /tmp/uc-report-demo --bin-dir "$RUNTIME_BIN"
```

`workflows.py` owns daemon readiness, selected sessions and bounded fixture
lifetimes. It stops its sessions, observes their terminal states, then waits for
the daemon to exit. Closing an SDK connection alone does not stop processes.

A successful run retains this `cleanup.json`:

```json
{"owned_sessions_stopped": true, "daemon_exited": true, "failures": []}
```

If setup or cleanup fails, keep the printed work path and inspect daemon logs,
session references and stopped-state evidence. Remove only your disposable work
directory after all of its processes are known to have stopped.

## Scope the recovery claim

The examples exercise specific interruption points. They are not a general
scheduler or an atomic filesystem checkpoint system. The output example checks
saved file size against its checkpoint before resuming; arbitrary crashes between
those writes still require reconciliation. The daemon example distinguishes
process survival, retained terminal history and application-owned archival bytes.

The [crate exercises](../crates/README.md) expose policy, codecs, replay and
transactions without running a daemon. macOS and Windows primitives have their own
native verification requirements; these Linux runs do not qualify those systems.
