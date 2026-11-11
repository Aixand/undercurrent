# Install the client and verify an input round trip

Start a local terminal process, send it input, and verify its acknowledgment
through Python SDK **0.1.0b0** from PyPI. Then clean up the session and isolated runtime.
This guide assumes `uc` is already installed and available in your terminal.

For a complete workflow built on these controls, see the
[coordinator example](../../assets/demos/fixtures/orchestrate.py) and its
[recording walkthrough](../../assets/demos/README.md#coordinate-jobs).
Verify this round trip first, then follow the workflow setup below.

You need:

- **Linux**, a POSIX shell, and **Python 3.11+** with `venv` and `pip`.
- This repository checkout for the maintained example source.
- The matching `uc-python-bridge` on `PATH`. If you need to build it, use the
  pinned **Rust 1.95.0** toolchain and [native build prerequisites](../../development/setup.md).

Use matching **0.1.0-beta** native executables with Python package **0.1.0b0**.
Install them together through the [hosted installer](../../getting-started/installation.md)
or build them from matching source. Python packaging tests cover Python 3.11–3.14;
they do not establish live integration qualification on macOS or Windows.
See the [verification scope](../../../python/docs/development/changes.md#verification-scope).

Run all commands from the **repository root**, containing `Cargo.toml` and
`python/`, and keep the same shell open through cleanup.

<a id="prepare-matching-executables"></a>

## 1. Install the Python package

```sh
python3 -m venv .venv-python-sdk
. .venv-python-sdk/bin/activate
python -m pip install aixand-undercurrent-sdk==0.1.0b0
python -c 'from importlib.metadata import version; from undercurrent import Client; print(version("aixand-undercurrent-sdk"))'
```

The version check prints `0.1.0b0` and verifies that `Client` imports.
The [PyPI distribution](https://pypi.org/project/aixand-undercurrent-sdk/0.1.0b0/)
contains Python code and typing metadata; it does not install native executables.

For SDK development, replace the PyPI install command with
`python -m pip install ./python` and use native executables from that same checkout.

The SDK finds `uc-python-bridge` on `PATH`. For a source installation, build it
from the checkout matching your other native executables:

```sh
cargo install --locked --path crates/undercurrent-python-bridge
```

The [portable product installer](../../getting-started/product-installer.md)
includes the matching bridge and SDK wheel. Its shared command directory can
provide the bridge on `PATH`; applications install the PyPI package or supplied
wheel into their own Python environment. Applications with private installations select their own
bridge and explicit runtime home.

For a bridge outside `PATH`, set `UNDERCURRENT_PYTHON_BRIDGE` to its absolute path.

<a id="start-an-isolated-runtime"></a>

## 2. Start an isolated runtime

Create a dedicated home for this tutorial:

```sh
export UNDERCURRENT_HOME="$(mktemp -d /tmp/uc-python-sdk.XXXXXX)"
printf 'Example runtime home: %s\n' "$UNDERCURRENT_HOME"
uc daemon start
uc daemon status
```

Successful status should identify a running daemon in that home. The SDK itself
never starts one. The bridge connects to `<home>/run/control.sock` through the
public local transport, including its authentication and handshake; no token
argument, database path, or remote URL belongs in `Client`.

<a id="run-and-verify"></a>

## 3. Run and verify

The executable source is [input_roundtrip.py](../../../python/examples/input_roundtrip.py).
It launches the selected Python interpreter in a managed terminal, sends a
unique value once, and requires the child to emit `UC_ACK:<value>`. Terminal echo
alone cannot produce that acknowledgment. It then verifies the process's exit
state and prints a success message.

```sh
python python/examples/input_roundtrip.py
```

Expected output includes a generated session name and ID, an input receipt,
and `Verified application acknowledgment and zero exit status.` This is expected
output, not a claim that this tutorial was run in your environment. The example
prints its name before starting so that a lost start response can be reconciled.

The example waits for `lifecycle="running"` to deliver input, not for a detected
prompt: the fixture accepts a line on standard input and needs no profile.
It uses a separate acknowledgment to establish application behavior. A generic
program's idle screen or zero exit code would not establish your application's
business result.

To observe an existing session or copy its output, continue with
[streams and retained output](streams.md) after this tutorial's cleanup.

<a id="handle-failures-and-clean-up"></a>

## 4. Handle failures and clean up

If the bridge is missing, check `UNDERCURRENT_PYTHON_BRIDGE` and executable
permissions.

If connection setup fails, check `uc daemon status` using the same
home and user. Match the CLI, daemon, session host, bridge, and Python package
versions before diagnosing protocol errors.

A timeout after sending a mutation is an uncertain outcome;
use the [recovery guide](recovery.md), preserving the printed name and session ID.

The round-trip example stops its known session if it is still live in its
`finally` block. If the connection failed, it reports that cleanup could not be
confirmed. Inspect that exact session through a fresh client before proceeding.
If starting lost its response, use the printed unique name to discover its ID
with [start reconciliation](recovery.md#start). Keep the temporary home while the
outcome or cleanup remains unconfirmed.

Once all example sessions are terminal, stop **only this tutorial's daemon**:

```sh
uc daemon stop
uc daemon status
```

Check that it is stopped before deleting the temporary home. Daemon shutdown
alone is not a substitute for stopping managed sessions. The following removes the isolated records:

```sh
case "$UNDERCURRENT_HOME" in
  /tmp/uc-python-sdk.*) rm -rf -- "$UNDERCURRENT_HOME" ;;
  *) printf 'Not an example home; leaving it intact.\n' ;;
esac
unset UNDERCURRENT_HOME UNDERCURRENT_PYTHON_BRIDGE
deactivate
```

The virtual environment can be retained for future examples. The SDK has no
session-removal method; stopping leaves the session's durable record in its
runtime home. Continue with [session and input contracts](operations.md).

## Next: coordinate jobs

Use the [coordinator example](../../assets/demos/fixtures/orchestrate.py) to run
independent jobs, verify their exits, and start a dependent report. Follow the
[recording walkthrough](../../assets/demos/README.md#coordinate-jobs) for its
setup and expected result.

Reactivate your SDK environment and restore the matching bridge path if needed;
the cleanup above stopped the tutorial runtime and cleared its environment
variables. Start a daemon in the home selected for your next workflow.

For an agent-specific workflow, use the [standalone orchestration skill](../../agent/README.md).
