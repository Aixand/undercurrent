# Review dependency documentation and notices

Find original dependency documentation, local patch provenance and notices from
the same source revision as these guides. The original files stay at their
packaging paths. Published text downloads preserve their bytes.

## Understand the owners

| Need | Owning document |
| --- | --- |
| Reviewed inventory, collection and regeneration | [Dependency licenses and notices](../../licenses/README.md) |
| Why libraries are vendored | [Vendored dependencies](../../vendor/README.md) |
| Native PTY patch scope | [Portable PTY patch](../../vendor/portable-pty/UNDERCURRENT.md) |
| Distribution checks | [Release guide](../development/distribution/releases.md) |

The inventory and source provenance identify their reviewed inputs. A historical
validation report applies to that revision and artifact; it does not establish
that a later dependency change passed those checks.

## Read original dependency documents

These downloads are dependency records, not additional Undercurrent API guides.
Upstream names and historical changelogs retain their own version scope.

| Dependency | Original documentation |
| --- | --- |
| vt100 | [README](../../vendor/vt100/README.md) · [Changelog](../../vendor/vt100/CHANGELOG.md) · [MIT license](../../vendor/vt100/LICENSE) · [Provenance](../../vendor/vt100/PROVENANCE.json) |
| vte | [README](../../vendor/vte/README.md) · [Changelog](../../vendor/vte/CHANGELOG.md) · [Apache license](../../vendor/vte/LICENSE-APACHE) · [MIT license](../../vendor/vte/LICENSE-MIT) · [Provenance](../../vendor/vte/PROVENANCE.json) · [Unsafe-code audit](../../vendor/vte/UNSAFE-AUDIT.json) |
| portable-pty | [MIT license](../../vendor/portable-pty/LICENSE.md) · [Rust project notice](../../vendor/portable-pty/LICENSE-RUST-MIT) · [Provenance](../../vendor/portable-pty/PROVENANCE.json) |
| Windows terminal runtime | [Original notices](../../licenses/inputs/terminal-NOTICE.md) · [ConPTY provenance](../../vendor/portable-pty/conpty/PROVENANCE.json) · [License](../../vendor/portable-pty/conpty/LICENSE) |

## Read distributed notices

- [Undercurrent license](../../LICENSE).
- [Foundation attribution](../../crates/undercurrent-foundation/NOTICE).
- [Collected third-party notices](../../licenses/THIRD-PARTY-NOTICES.txt).
- [Rust standard-library notice HTML source, served as text](../../licenses/RUST-STDLIB.html).

Packaging tools read the retained source files. Keep their originals and recorded
provenance when reorganizing explanatory documentation. The [collection guide](../../licenses/README.md#verify-and-update)
owns freshness checks and regeneration.
