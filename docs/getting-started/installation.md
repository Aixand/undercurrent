# Install Undercurrent

Use [installer.aixand.org/undercurrent](https://installer.aixand.org/undercurrent)
for a prebuilt installation. After installation, verify the
components, initialize your shell, and create your first session.

## Choose an installation route

| Route | Prerequisites and scope |
| --- | --- |
| Hosted installer | Linux x86_64 GNU or macOS arm64/x86_64: curl, tar/gzip, awk, mktemp, and sha256sum or shasum. Windows x64: PowerShell 5.1+ and system tar. No system Python or Rust. |
| Source development | This complete repository, pinned Rust 1.95.0, Cargo, native compiler/linker, and dependency access. |
| Portable product artifact | A supplied artifact and matching bootstrap for your native platform. Follow the [product installer](product-installer.md). |
| Verified artifact | A separately obtained bundle with checksums, provenance, and license metadata. See [bundle qualification](../development/releases.md). |

The hosted installer includes `uc`, `uc-daemon`, `uc-session-host`,
`uc-python-bridge`, `uc-mcp`, and the Python SDK wheel. It downloads a pinned,
checksum-verified GitHub Release bundle and bootstraps its helper interpreter.
Python applications install `aixand-undercurrent-sdk==0.1.0b0` from PyPI into
their own virtual environment. Follow the [Python SDK quickstart](../sdk/python/quickstart.md)
to use the matching bridge and runtime.

The required executables are `uc`, `uc-daemon`, and `uc-session-host` (`.exe` on
Windows). Windows also requires the matching `uc-conpty` directory containing
Microsoft's console runtime and its license. Install or replace these together
from the same build. `uc` finds a
sibling daemon before searching `PATH`; the daemon needs its private session
host. An external agent bridge is a separate integration dependency, not a
fourth executable required for ordinary sessions.

The manifest declares Linux x86_64 GNU, macOS arm64 and x86_64, and Windows
x86_64 MSVC. For source builds, Linux needs a native C toolchain; macOS needs
Xcode Command Line Tools; Windows needs native MSVC build tools and a supported
ConPTY host. Native Windows uses the public PowerShell entry point. Linux ARM64
and musl/Alpine Linux are not supported by the public installer.
Use [platform support](../reference/platforms.md) for limitations and
[platform coverage](../development/platform-testing.md) for what checked-in workflows
exercise. A target declaration or cross-compilation is not a recorded native
qualification result.

## Install for source development

From the repository root on Linux/macOS:

```sh
./dev.sh install --prod
```

On native Windows run `./dev.ps1 install --prod` in PowerShell. These explicit
source-build workflows install the three core release-mode binaries plus
`uc-mcp` and leave the daemon stopped. Build the Python bridge separately for
the SDK using its [quickstart](../sdk/python/quickstart.md).
The default binary directory is `CARGO_HOME/bin`, or the native
user's `.cargo/bin` when unset. An absolute `UNDERCURRENT_DEV_INSTALL_DIR`
selects another binary directory. Add it to your shell's `PATH` using your
normal shell configuration.

The helpers reject existing destinations under `install`. `reinstall` stops
the selected daemon before replacement and refuses replacement when installed
executables remain in use. Back up important state and plan for interruption
before using it. The POSIX helper uses a separate build-cache root unless
`CARGO_TARGET_DIR` is supplied; [dev.sh](../../dev.sh) and [dev.ps1](../../dev.ps1)
own host-specific cache and replacement behavior.

Verify the selected executable with `command -v uc` on POSIX or `Get-Command uc`
on PowerShell, then run the three version probes and `uc config validate`.
Validation is offline and does not start a daemon. An absent main configuration
file is valid and uses defaults. Next, [initialize your shell](#initialize-your-shell)
before opening managed interactive sessions.

## Install a published release

From any directory on a supported Linux/macOS host, run:

```sh
curl -fsSL https://installer.aixand.org/undercurrent | sh
```


The default shared store is `~/.local/share/aixand/products` on Linux
(respecting `XDG_DATA_HOME`) or `~/Library/Application Support/Aixand/products`
on macOS. Use `--prefix /absolute/path/to/products` to select another store.
Commands live in the store's `bin`; shared installs also create links in
`~/.local/bin`, preserving existing commands. Add that directory to PATH:

```sh
export PATH="$HOME/.local/bin:$PATH"
command -v uc
uc --version
uc-daemon --version
uc-session-host --version
uc config validate
```

All three probes should report the same release version, and validation should
succeed. These commands do not start a persistent daemon. If an existing command
takes precedence, inspect `command -v uc` and the installer's command report.
Next, [initialize your shell](#initialize-your-shell).

### Native Windows PowerShell

From any directory on Windows x64, run:

```powershell
& ([scriptblock]::Create((irm https://installer.aixand.org/undercurrent/install.ps1))) -Mode Install -AcceptLicense c94788b242f3ee3ee9141c1fd61120b60d42d22dc511d687e5a6975ed9f0a45c
```


The default store is `%LOCALAPPDATA%\Aixand\products`. Use
`-Prefix 'C:\Tools\AixandProducts'` for another absolute store path.
Shared command registration updates your user PATH; open a new terminal,
then verify:

```powershell
Get-Command uc
uc --version
uc-daemon --version
uc-session-host --version
uc config validate
```

Check the command report if another installation takes precedence. See the
[product installer](product-installer.md) for private ownership and lifecycle operations.

## Build without installing

From the **repository root**, with the repository toolchain available:

```sh
cargo build --locked -p undercurrent-cli -p undercurrent-daemon --bins
./target/debug/uc --version
./target/debug/uc-daemon --version
./target/debug/uc-session-host --version
```

In native PowerShell use the corresponding executables under `target/debug`
with their `.exe` suffixes. Each probe should report its name and `0.1.0-beta`.
These are source-derived expectations, not an authoring-time execution
transcript. If `CARGO_TARGET_DIR` is set, use that directory instead of `target`.

Keep the workspace and lockfile intact. Building only the CLI leaves required
helpers unavailable. Use [development setup](../development/setup.md#use-isolated-runtime-state)
to try checkout builds with isolated state. The usage guides assume `uc` is
installed on `PATH`; follow [source installation](#install-for-source-development) to install your build.

## Initialize your shell

After installation, run `uc init SHELL` as your normal user for
each shell you will launch in Undercurrent. Run the corresponding command from
any directory; `uc` must already be on `PATH`.

| Shell | Command | Startup file |
| --- | --- | --- |
| Bash | `uc init bash` | `~/.bashrc` |
| Zsh | `uc init zsh` | `~/.zshrc` |
| Fish | `uc init fish` | `~/.config/fish/config.fish` |
| PowerShell | `uc init powershell --profile "$PROFILE"` | The current PowerShell host's `$PROFILE`. Run this command in PowerShell. |

The command adds or updates one marked hook block and preserves the rest of
the startup file. It reports `installed`, `updated`, or `already installed`;
rerunning it with the same options does not duplicate the block. Run it again
after upgrading `uc` to refresh the embedded hooks.

These hooks report when a managed shell is ready for input or running a
command. They also capture its exported environment for separate commands
started with `uc exec`. Hooks activate inside managed sessions; `uc init`
itself does not start a daemon or session. The CLI starts the daemon when
needed by `uc start`.

Use the startup file your shell actually loads. For custom `ZDOTDIR`, Fish
configuration locations, or other layouts, pass its absolute path with
`--profile /absolute/path/to/startup-file`. For managed or symlinked dotfiles,
use `--print` and incorporate the complete marked block through your dotfile
setup; see [shell integration](../guides/cli/shell-integration.md).

### Preview hooks and choose a prompt

For example, with Zsh:

```sh
uc init zsh --print
uc init zsh --prompt session
```

The first command prints the hooks without changing files. The second installs
them with a prompt label such as `[uc:demo]`. Prompt decoration defaults to
`off`; `--prompt simple` adds `[uc]`, and `--prompt session` includes the session
name when available. Omit `--prompt` or use `--prompt off` to keep your prompt
unchanged. `--print` and `--profile` cannot be combined.

Open a **new managed shell** so the updated startup file runs in its session.
Existing shells retain their loaded hooks. Follow the
[shell-hook verification walkthrough](../guides/cli/shell-integration.md#3-verify-in-a-new-managed-shell)
to try a specific shell, or continue with [your first session](first-session.md)
and [interactive terminals](../guides/cli/interactive.md).

### Optional shell completions

`uc completion SHELL` prints completion definitions for the same four supported
shells. For example, `uc completion zsh` prints the Zsh completion script.
Load the output through your shell's completion setup; `uc init` installs
detection hooks separately. See the
[local command reference](../reference/cli-commands.md#interactive-and-local-utilities).

## Upgrade, uninstall, and recover failed setup

For hosted and portable installations, follow the
[product lifecycle guide](product-installer.md#upgrade-repair-and-remove).
Review the installation plan before changing an existing store.

[Stop the selected daemon](../guides/daemon/README.md) and verify ownership release before
replacement. Preserve configuration, the database, and private host evidence
as a consistent set; [storage recovery](../guides/daemon/storage.md) explains why copying an
active SQLite file or deleting receipts is insufficient.

For a development installation, `./dev.sh uninstall` or `./dev.ps1 uninstall`
removes installed executables and preserves product state. The helpers require
the installed processes to be stopped. They do not purge the home. Use
[session cleanup](../guides/cli/sessions.md) for ordinary work; `uc reset` is a separate,
deliberately destructive operation.

| Failure | Recovery |
| --- | --- |
| Public release metadata or platform bundle is unavailable | Confirm the requested version has been published for your platform, or use the source-development route. |
| A required installer tool is missing | Install the prerequisites for your platform before rerunning the public installer. |
| Toolchain or linker missing | Prepare the pinned toolchain and native build prerequisites, then rerun the build. |
| Missing helper or mismatched version | Restore the complete three-binary set and recheck sibling paths and `PATH`. |
| Configuration rejects old inputs | Follow [configuration](../guides/configuration/README.md) and the linked home-layout diagnosis before startup. |
| Unsafe or busy installation destination | Keep the installation intact, inspect the reported object or executable, and stop its owner normally. |
| Startup fails after a build | Use [troubleshooting](../guides/troubleshooting.md); compilation does not prove compatible state or authenticated readiness. |

No source example here claims to publish, sign, or qualify a release. For
integration rather than CLI use, continue with the [SDK paths](../sdk/README.md).

[Documentation index](../README.md)
