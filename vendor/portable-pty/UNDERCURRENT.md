# Undercurrent portable-pty patch

Upstream 0.9.0, MIT licensed. See PROVENANCE.json and cwd-handle.patch.

Adds an owned Unix directory handle to CommandBuilder, usable only with an
absolute executable path. The child calls fchdir before inherited descriptors
are closed. The parent's cwd never changes. A failed fchdir aborts spawning;
there is no fallback to HOME. Cloning retains the descriptor; live handles
cannot be serialized. Runtime macOS preparation supplies the verified handle.

The only added native call is fchdir, inside the existing Unix pre_exec boundary.
It uses a retained valid descriptor and performs no allocation or locking after
fork. Existing upstream unsafe code remains unchanged.

Regression: cargo test -p undercurrent-runtime --lib pinned_pty_cwd

Windows also retains a kill-on-close Job Object before resuming each ConPTY
root. Batch launches use an explicitly selected system interpreter and a separate
argument encoder adapted from Rust 1.95.0 `library/std/src/sys/args/windows.rs`.
The adapted encoder is available under the MIT license in LICENSE-RUST-MIT;
its attribution is retained in src/win/batch.rs. It disables AutoRun and delayed
expansion, escapes script paths as well as arguments, and rejects CR/LF/NUL and
commands that exceed the command interpreter's length limit.

Regression: cargo test -p undercurrent-runtime --test windows_native_runtime

Windows builds also ship Microsoft.Windows.Console.ConPTY 1.25.260930003,
with its original MIT license and package/file hashes in
[conpty/PROVENANCE.json](conpty/PROVENANCE.json). The system ConPTY API on
Server 2022 retains a terminated console-host process handle after each
`ClosePseudoConsole`; a native API-only probe reproduces the leak. The pinned
Microsoft runtime releases that handle, so the lifecycle test keeps its original
handle-growth bound. [windows-conpty.patch](windows-conpty.patch) records this
change against the repository revision identified in PROVENANCE.json.

The build script stages `uc-conpty/{conpty.dll,OpenConsole.exe,LICENSE}` beside
Cargo binaries, tests and examples. The adapter loads the DLL by its absolute
path beside the current executable. Missing or incompatible assets fail terminal
creation instead of falling back to the leaking system backend. Windows
installers and release bundles must carry this directory with the executables;
bundle verification pins the exact runtime bytes and attribution.
