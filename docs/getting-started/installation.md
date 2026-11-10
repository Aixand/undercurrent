# Install Undercurrent

For this checkout, use [source installation](#install-for-source-development)
with `dev.sh` or `dev.ps1`. A supplied artifact uses the
[portable product installer](product-installer.md).

Hosted installer availability is not established for this snapshot. Use hosted
commands only after a maintainer confirms working endpoints and a published
bundle for your platform. After installation, verify the components, initialize
your shell, and create your first session.

## Choose an installation route

| Route | Prerequisites and scope |
| --- | --- |
| Source development | This complete repository, pinned Rust 1.95.0, Cargo, native compiler/linker, and dependency access. Builds locally and can install into your chosen prefix. |
| Portable product artifact | A supplied artifact and matching bootstrap for your native platform; no system Python or Rust. Includes the five native executables and SDK wheel. Follow the [product installer](product-installer.md). |
| Public installer | curl and Python 3.11+; Linux x86_64 with GNU libc or macOS arm64/x86_64, with a published bundle for your platform. Downloads and verifies prebuilt binaries. |
| Public Windows installer | Native Windows x86_64, PowerShell 5.1 or 7, 64-bit Python 3.11+, curl.exe, and a published Windows bundle. Uses `install.aixand.org/install.ps1`. |
| Verified artifact | A separately obtained bundle with checksums, provenance, and license metadata for the exact native platform. The package contract requires verification before execution; see [bundle qualification](../development/releases.md). |

The public installers select releases with bundles available for your platform
at `downloads.aixand.org`. To install the current source checkout, follow
[source development](#install-for-source-development).

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

This route requires maintainer-confirmed working hosted endpoints and a matching
published bundle. Source publication alone does not make this route available.

From any directory on a supported Linux/macOS host, with curl and Python 3.11+
available:

```sh
curl -fsSL https://install.aixand.org | sh -s -- undercurrent
```

The installer selects the latest published version for your platform, downloads
it from `downloads.aixand.org`, verifies the checksums and bundle contents, and
installs the three matching executables. It prompts for Apache-2.0 acceptance
through your terminal. For unattended installation, explicitly accept the license:

```sh
curl -fsSL https://install.aixand.org |
  sh -s -- undercurrent --accept-license Apache-2.0
```

Commands are linked into `~/.local/bin` by default. The installer prints PATH
instructions without editing your shell configuration. Add that directory to
your shell's PATH configuration; for the current shell, run:

```sh
export PATH="$HOME/.local/bin:$PATH"
command -v uc
uc --version
uc-daemon --version
uc-session-host --version
uc config validate
```

All three executables should report the same release version, and configuration
validation should succeed. Installation and these checks do not start a daemon.
A source checkout, Rust, and a native compiler are not required for this
route. After these checks, [initialize your shell](#initialize-your-shell).

To choose another prefix, use `--prefix "$HOME/tools/undercurrent"`; commands
then go into that prefix's `bin` directory. Use `--version VERSION` to select a
specific published version, replacing `VERSION` with its release number. To
check release selection without installing:

```sh
curl -fsSL https://install.aixand.org | sh -s -- undercurrent --dry-run
```

Existing unrelated commands in the destination are preserved. If you also use
a development installation, select a separate prefix and check `command -v uc`
to confirm which installation your shell selects.

### Native Windows PowerShell

For a published Windows x86_64 release, run:

```powershell
& ([scriptblock]::Create((irm https://install.aixand.org/install.ps1))) -Product undercurrent -AddToPath
uc --version
uc-daemon --version
uc-session-host --version
uc config validate
```

The installer prompts for Apache-2.0 acceptance. Add `-AcceptLicense Apache-2.0`
in automation. Use `-Version VERSION` with a published release number, or
`-DryRun` to inspect release selection without installing.

The default installation directory is `%LOCALAPPDATA%\Aixand\undercurrent`.
The three native executables and their matching `uc-conpty` directory stay
together under `payload\bin`. The installer adds that command directory to the
current PowerShell session's PATH; `-AddToPath` also saves it to your user PATH.
Reopen other terminals to pick up that change. It does not require administrator
access, symlinks, or a persistent execution-policy change.

Use `-Prefix 'C:\Tools\Undercurrent'` to choose a new absolute local directory.
Existing directories are preserved. Repeating an installation checks the
installed files; a version change requires the stop/backup/compatibility-check
upgrade procedure and a separate prefix. Use `Get-Command uc` to check which
installation your shell selects. For local source work, continue using
`./dev.ps1 install --prod`.

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

After either installation route, run `uc init SHELL` as your normal user for
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

The public installer can be rerun for the same version. It refuses to replace
an existing managed Undercurrent installation with a different version; stage
the new release with a separate `--prefix` and follow the compatibility and
shutdown guidance below before switching. It does not provide an automated
upgrade or uninstall command.

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
| curl or Python 3.11+ is missing | Install the required tools before rerunning the public installer. |
| Toolchain or linker missing | Prepare the pinned toolchain and native build prerequisites, then rerun the build. |
| Missing helper or mismatched version | Restore the complete three-binary set and recheck sibling paths and `PATH`. |
| Configuration rejects old inputs | Follow [configuration](../guides/configuration/README.md) and the linked home-layout diagnosis before startup. |
| Unsafe or busy installation destination | Keep the installation intact, inspect the reported object or executable, and stop its owner normally. |
| Startup fails after a build | Use [troubleshooting](../guides/troubleshooting.md); compilation does not prove compatible state or authenticated readiness. |

No source example here claims to publish, sign, or qualify a release. For
integration rather than CLI use, continue with the [SDK paths](../sdk/README.md).

[Documentation index](../README.md)
