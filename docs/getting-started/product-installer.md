# Portable product installer

One native artifact contains
`uc`, `uc-daemon`, `uc-session-host`, `uc-python-bridge`, `uc-mcp`, the Python SDK wheel,
reviewed notices, and pinned CPython 3.12.14 for the installer and private SDK
environment. Undercurrent runs as native executables and an external daemon;
the SDK connects through local IPC.

The configured targets are Linux GNU x64, macOS Intel/ARM64, and Windows x64.
Actual execution is recorded in [installer validation](../development/installer-validation.md).
Build each target natively and qualify it before publication.

## Build and qualify

Builders need Python 3.11+, uv 0.12.7, Rust 1.95.0, and a native compiler/linker.
Windows packaging also needs Rust 1.98.0 for the native command shim.
CPython 3.12.14 is the selected SDK/build runtime.

```sh
./dev.sh develop
./dev.sh check
./dev.sh test
./dev.sh package --output dist/release
./dev.sh verify --artifact dist/release --install --report dist/verification.json
python3 scripts/prepare_product_runtime.py --bootstrap-output dist/bootstrap
python3 scripts/qualify_product_installation.py dist/release \
  --exercise-upgrade --bootstrap dist/bootstrap --report dist/lifecycle.json
./dev.sh export-release --artifact dist/release \
  --report dist/verification.json --output dist/handoff
```

PowerShell uses `.\dev.ps1` with the same arguments. Use `python` or `py -3`
for Python commands. `AIXAND_DEV_PYTHON` or `UNDERCURRENT_DEV_PYTHON` selects
the developer helper. The SDK keeps its existing setuptools backend, constrained
to 80.9.0 through `scripts/product-build-requirements.txt`. Export it independently:

```sh
./dev.sh package --component sdk --output dist/sdk
./dev.sh verify --artifact dist/sdk --install --python /absolute/path/to/python3.12
```

That component creates no global commands. Other applications install the SDK
wheel into their own environments; the CLI's private environment is not shared.

## Install without source or language tools

Distribute the matching artifact and generated bootstrap together. The first stage
downloads its checksum-pinned helper interpreter. Shell bootstrap needs curl,
tar/gzip, and a SHA-256 utility; Windows uses PowerShell 5.1+ and system tar.
Actual product installation consumes bundled bytes offline.

```sh
sh /path/to/bootstrap/bootstrap.sh plan \
  --artifact /path/to/release --prefix /absolute/path/shared-tools
```

The plan lists the required license SHA-256. Read
`/path/to/release/objects/<license-sha256>`, then pass the exact digest:

```sh
sh /path/to/bootstrap/bootstrap.sh install \
  --artifact /path/to/release --prefix /absolute/path/shared-tools \
  --accept-license <license-sha256> --request-id undercurrent-setup --events jsonl
/absolute/path/shared-tools/bin/uc --version
```

Replace the angle-bracketed placeholder before running it. Windows uses the
supplied `bootstrap.ps1` and `uc.exe`. Missing acceptance fails before creating
the store. Owned products retain acceptance of unchanged text; changed text needs
new acceptance. `verify --install` accepts declared texts only for disposable
technical qualification.

Default shared stores are `~/.local/share/aixand/products` on Linux (respecting
`XDG_DATA_HOME`), `~/Library/Application Support/Aixand/products` on macOS, and
`%LOCALAPPDATA%\Aixand\products` on Windows. Commands live in `bin`. PATH and shell
initialization remain explicit. Installation does not start a persistent daemon
or change the existing runtime home. Verification runs a temporary isolated daemon
and PTY session.

The payload also includes complete agent skill folders under
`share/undercurrent/skills/` and an MCP setup guide. Registration with an assistant
is explicit; installation does not edit assistant settings.

## Shared and application-private ownership

A private installation requires an application owner and explicit store:

```sh
./dev.sh install --artifact dist/release --product undercurrent \
  --prefix /absolute/path/example-app/runtime/products \
  --scope private --owner example-app --accept-license <license-sha256>
```

Scope controls software ownership and command location. Runtime namespaces are
selected separately through `UNDERCURRENT_HOME`. Independent use retains
`~/.aixand/undercurrent` by default. An application must set a dedicated absolute
home outside versioned payloads when launching its private CLI, daemon, or bridge.
Select its private bridge through `Client(bridge=...)` or
`UNDERCURRENT_PYTHON_BRIDGE` as well.

Each home derives its own configuration, state directory, and IPC endpoint.
The integration must supply this home consistently: a private prefix alone does
not change runtime configuration. Do not point shared and private daemons at the
same home.

## Upgrade, repair, and remove

Explicitly stop the selected home's daemon and managed processes before replacement.
Back up its state directory, configuration, and profiles while quiesced; preserve
private permissions and exclude transient `run/` sockets/locks. Review native
protocol and database compatibility before restarting. The current product rejects
an incompatible database instead of migrating it.

```sh
./dev.sh upgrade --artifact /path/to/new-release --prefix /absolute/path/shared-tools
./dev.sh reinstall --artifact dist/release --prefix /absolute/path/shared-tools
./dev.sh inspect --prefix /absolute/path/shared-tools
./dev.sh uninstall --product undercurrent \
  --prefix /absolute/path/shared-tools --stopped
```

Changed bytes require a new release version. Upgrades select an explicit artifact.
Generic rollback is rejected because Undercurrent is stateful. No automatic data
migration, backup, service replacement, or restart is implied by generic artifact
commands. The application/operator owns the explicit stop, backup, compatibility
check, replacement, and restart sequence.

Uninstall removes owned software and commands while preserving runtime homes,
databases, profiles, configuration, and retained installer/runtime caches.
It requires `--stopped`. Removing an application's complete private software tree
leaves an independent shared installation usable.

## Source installs and publication

`install --prod` and bare `reinstall`/`uninstall` select the Cargo-bin source workflow
and its process checks. They install the three core executables plus `uc-mcp` into Cargo's
binary directory; they do not manage portable product stores. To install source
into a product store, use explicit `--product` or `--prefix` without `--prod`.
Source and supplied-artifact ownership stay distinct; installing into a store
does not silently adopt an existing Cargo-bin installation.

Release-bundle validation lives in `scripts/release/`. The portable assembly
retains the reviewed dependency notices, licenses, foundation attribution,
vendored provenance/audits, schemas, and package metadata. Windows verifies and
carries pinned ConPTY, OpenConsole, license, and provenance.
`product-installation.json` records all five executables and the SDK wheel.
Product metadata also retains the three-core-executable bundle contract used
by the download installer.

`dist/handoff` contains the exact artifact, descriptor, and bound installation
report. Keep `dist/bootstrap` and `dist/lifecycle.json` alongside it. Export does
not publish or manufacture an attestation. The
[release workflow](../development/releases.md#publish-a-version-tag) validates
`v<version>` tags, runs native qualification, and attaches versioned archives to
a GitHub Release after crates.io publication succeeds. Export itself does not
deploy hosted installer endpoints or make a release available for download.
