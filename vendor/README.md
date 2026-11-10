# Vendored dependencies

This directory contains local copies of third-party Rust libraries for terminal
parsing, screen state, and pseudo-terminal process management. These copies are
kept in the repository because they include patches on top of published
crates.io releases.

| Library | Upstream version | Purpose | Local changes |
| --- | --- | --- | --- |
| [`vt100`](vt100/README.md) | `0.15.2` | Tracks terminal screen contents and scrollback. | Faster text processing, scrollback fixes, terminal-query replies, and a dependency on the local `vte` copy. |
| [`vte`](vte/README.md) | `0.11.1` | Parses terminal text and escape sequences. | Batches printable text and supported control sequences while preserving parser behavior. |
| [`portable-pty`](portable-pty/UNDERCURRENT.md) | `0.9.0` | Creates pseudo-terminals and launches processes across platforms. | Launches from retained directory handles on Unix; adds Windows process-tree control and batch-command argument handling. |

Each library has a provenance record describing its upstream source, checksums,
and local patches:

- [vt100 provenance](vt100/PROVENANCE.json) and [patches](vt100/patches/).
- [vte provenance](vte/PROVENANCE.json), [patches](vte/patches/), and [unsafe-code audit](vte/UNSAFE-AUDIT.json).
- [portable-pty provenance](portable-pty/PROVENANCE.json) and [patch details](portable-pty/UNDERCURRENT.md).
- [Microsoft ConPTY runtime provenance](portable-pty/conpty/PROVENANCE.json) and
  [MIT license](portable-pty/conpty/LICENSE). Windows builds ship the pinned
  runtime to avoid the Server 2022 system console-host handle leak.

Upstream license texts are retained with each copy: [MIT for vt100](vt100/LICENSE),
[Apache-2.0](vte/LICENSE-APACHE) or [MIT](vte/LICENSE-MIT) for vte, and
[MIT for portable-pty](portable-pty/LICENSE.md). The Windows argument encoder
also retains its [Rust project MIT license](portable-pty/LICENSE-RUST-MIT).
