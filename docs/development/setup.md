# Prepare a development checkout

Build from the repository root containing `Cargo.toml`, `Cargo.lock`, `dev.sh`,
and `rust-toolchain.toml`. The [workspace manifest](../../Cargo.toml) and
[architecture policy](../../tests/architecture/allowed-graph.json) identify the
first-party crates and supported consumer surfaces. Vendored dependencies and
excluded contract-test consumers stay in their declared locations. Use the
[dependency map](../architecture/dependencies.md) to find each crate's owner.

Use Rust **1.95.0** with Rustfmt and Clippy, Python **3.11 or newer**, and the
native compiler/linker for the target. macOS needs Xcode Command Line Tools.
Shell-based checks additionally need Bash and their declared fixture shells.
See [platform scope](../reference/platforms.md) before selecting native tests.
The checked-in lockfile and path dependencies must be available. The validation
runner forces offline Cargo resolution, so populate the dependency cache in
your normal development setup before using that runner.

## Build the boundary you changed

From the repository root, an SDK-only check is:

```sh
cargo check --locked -p aixand-undercurrent
cargo test --locked -p aixand-undercurrent --test product_client
```

Successful commands exit zero. A dependency-cache or toolchain failure is a setup
failure; it does not establish a failing product assertion. Use the owning
[crate guide](../architecture/dependencies.md) to choose tests for another boundary. Test fixtures
can launch subprocesses: inspect their requirements before running them.

For the three standalone executables:

```sh
cargo build --locked -p undercurrent-cli -p undercurrent-daemon
./target/debug/uc --version
./target/debug/uc-daemon --version
./target/debug/uc-session-host --version
```

These paths assume `CARGO_TARGET_DIR` is unset. Keep the three matching artifacts
together. They are source-build outputs, not evidence that a published bundle
exists.

## Install the local checkout

From the repository root on Linux/macOS:

```sh
./dev.sh install --prod
```

For later local changes, use `./dev.sh reinstall --prod`. On Windows, use
`./dev.ps1 install --prod` and `./dev.ps1 reinstall --prod` in native PowerShell.
These helpers build the checkout and install into Cargo's `bin` directory by
default. Reinstalling stops the selected daemon and refuses to replace binaries
still in use; plan for that interruption.

The [source installation guide](../getting-started/installation.md#install-for-source-development)
covers custom paths and process handling. For a maintainer-confirmed published
release, follow [download installation](../getting-started/installation.md#install-a-published-release).
Use the development helpers to run changes from your working tree.

## Use isolated runtime state

Keep development runs separate from your everyday sessions. From the repository
root in a POSIX shell, open a subshell and select the build and a temporary home:

```sh
sh
export PATH="${CARGO_TARGET_DIR:-$PWD/target}/debug:$PATH"
UNDERCURRENT_HOME=$(mktemp -d /tmp/uc-dev.XXXXXX) || exit
export UNDERCURRENT_HOME
printf 'Development home: %s\n' "$UNDERCURRENT_HOME"
```

Use an absolute `CARGO_TARGET_DIR` if you set it. In that subshell, the commands
in [your first session](../getting-started/first-session.md) use the checkout
build and temporary state. After cleaning up your test sessions, stop this
daemon and remove its home:

```sh
uc daemon stop && rm -rf -- "$UNDERCURRENT_HOME"
exit
```

If shutdown fails, keep the printed home and exact session IDs for investigation.
Exiting the subshell restores your original environment. The
[Python tutorial](../sdk/python/quickstart.md) covers the extra bridge and
Python environment.

Python package tests use the standard library and mocked requests:

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=python/src python3 -m unittest discover -s python/tests -v
```

Expected result: tests pass without a running daemon. This does not verify the
bridge subprocess or native PTY delivery. A build alone leaves Cargo artifacts;
the mocked Python command needs no runtime cleanup.

## Before review

Read [ownership](../architecture/README.md) and [representative flows](../architecture/flows.md), select an
[extension boundary](extending.md), then apply the [verification matrix](verification.md).
Update affected guides and Mermaid when behavior changes. Do not turn a passing
unit fixture into a platform, crash-durability, or release claim.

[Documentation index](../README.md)
