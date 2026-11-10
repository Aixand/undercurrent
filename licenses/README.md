# Dependency licenses and notices

The current `Cargo.lock` review covers 212 third-party Rust packages in the
normal/build dependency graphs for the CLI, daemon/session host, Python bridge, MCP adapter,
and public Rust SDK/protocol packages
across the four supported targets. This is a conservative graph inventory,
including build dependencies and workspace feature unification; it is not an
exact inventory of machine code linked into each executable.

The selected license alternatives are permissive. No package in this reviewed
graph requires selecting a GPL/LGPL license or relicensing Undercurrent. These
findings concern the checked versions and features; new dependencies and
changed license expressions need review.

Security advisories are checked separately by the
[dependency advisory workflow](../.github/workflows/dependency-advisories.yml).
The license inventory records the current versions, original license texts,
and archive hashes; a license freshness check does not audit security.

| Selected terms | Packages |
| --- | ---: |
| MIT | 194 |
| Apache-2.0 | 3 |
| ISC | 2 |
| MIT AND BSD-3-Clause | 1 |
| MIT AND Unicode-3.0 | 1 |
| Apache-2.0 AND ISC | 1 |
| BSD-3-Clause | 1 |
| Zlib | 1 |
| CDLA-Permissive-2.0 | 1 |

## Included evidence

- [inventory.json](inventory.json) records exact package versions, original
  license declarations, selected alternatives, supported-target membership,
  crate archive checksums, and hashes of license/notice source files. It also
  binds the lockfile, manifests, vendored sources, and collector inputs.
- [THIRD-PARTY-NOTICES.txt](THIRD-PARTY-NOTICES.txt) preserves upstream license
  and copyright text, with duplicate text grouped by source. All applicable
  parts of `AND` expressions are retained. Additional source-file notices
  identified by cargo-about are included, particularly ring's embedded code.
- [RUST-STDLIB.html](RUST-STDLIB.html) is the full upstream notice collection
  for the pinned Rust 1.95.0 standard library. Its root MIT/Apache license
  texts also appear in the text collection.
- [inputs/provenance.json](inputs/provenance.json) records the pinned sources
  and hashes for Microsoft Terminal's notice file and the Rust documentation
  component. The original inputs are retained unchanged under `inputs/`.

Registry license files were compared with the cached `.crate` archives, whose
SHA-256 digests were checked against `Cargo.lock`. Every selected package had
original local license files; scanner-generated fallback license text was not
used as a substitute for missing upstream attribution. Packages that only
occur in development or unsupported-target graphs are outside this collection.

## Native and embedded components

| Component | Review and treatment |
| --- | --- |
| SQLite 3.54.1 in libsqlite3-sys 0.39.1 | The current build enables `bundled`, not SQLCipher. Retain the wrapper's MIT notice and the embedded SQLite public-domain dedication. The source hash and version are recorded. |
| ring 0.17.14 | Retain Apache-2.0 and ISC notices, plus the shipped fiat/once_cell and source-header notices. The package's SPDX field alone does not enumerate each embedded notice. |
| encoding_rs 0.8.41 | Select MIT for the implementation and also retain the WHATWG BSD-3-Clause data notice. |
| unicode-ident 1.0.24 | Select MIT and also retain the Unicode-3.0 notice. |
| webpki-roots 1.0.10 | Retain CDLA-Permissive-2.0 with the embedded trust-anchor data. |
| Microsoft.Windows.Console.ConPTY 1.25.260930003 | The pinned NuGet archive digest and MIT declaration were verified. Existing binary hashes and MIT text remain enforced. The collection includes the full [Terminal notice](inputs/terminal-NOTICE.md) from the same upstream revision used for the vendored license. This conservatively includes notices for more than ConPTY; the NuGet package does not identify its exact build-to-source revision. |
| Rust standard library | Include the upstream runtime notice collection for 1.95.0, including its own dependencies outside the application's Cargo.lock. |

Missing standalone license files found in the broader development cache belong
to packages outside the 212-package supported build closure. They must be reviewed if future feature,
target, or packaging changes bring them into a distributed artifact.

## Packaging fixes

Binary bundles now require the inventory, text notices, and Rust notice HTML
under `share/undercurrent/third-party/`. They are covered by bundle checksums,
the exact expected-file set, and comparison with the reviewed checkout's
collection. Missing or modified notices fail verification, even if a bundle's
checksums are rewritten. Assembly and verification reject stale inventory
inputs without accessing the network or invoking Cargo.

All first-party Rust packages declare the SPDX `license` and retain a package-local
copy of the root `LICENSE`, so Cargo source packages include the Apache license
text without redundant `license-file` metadata. The inventory freshness check
requires those copies to match the canonical root license. The foundation package retains its
existing `NOTICE`. The pure Python SDK now includes `python/LICENSE` in both
its wheel and source distribution; its Rust bridge is distributed separately
and is included in the Rust dependency review above.

Repository source archives retain the original vendored licenses/provenance,
the foundation notice, and this collection. Preserve those files when creating
a source distribution. Standard OS libraries and separately installed external
applications are outside the application's bundled payload; adding a native
library, runtime DLL, font, image, or other third-party asset needs review too.

## Verify and update

Python 3.11+ is sufficient for the normal, offline freshness check:

```sh
python3 scripts/release/third_party_licenses.py check
python3 tests/tooling/checks/dependency_licenses_test.py
```

The [license workflow](../.github/workflows/licenses.yml) runs these checks on
pull requests and main. They verify the reviewed records and packaging behavior;
they do not automatically approve a new license or prove upstream ownership.

To regenerate after reviewing dependency changes, install
[cargo-about 0.9.2](https://github.com/EmbarkStudios/cargo-about/releases/tag/0.9.2),
make the locked dependencies available in Cargo's cache, and install the Rust
toolchain pinned by `rust-toolchain.toml`. Then run:

```sh
cargo +1.95.0 fetch --locked
python3 scripts/release/third_party_licenses.py generate --cargo-about cargo-about
```

Generation itself is offline. It fails on unreviewed license expressions,
missing original license files, or source/archive checksum mismatches. Review
the resulting versions, license choices, original texts, native inputs, and
diff before accepting the updated collection. The original legal text remains
the authority; `selected_license` documents the chosen top-level alternative,
while additional embedded notices remain applicable to their components.

Build releases with the recorded toolchain, targets, and features. Changing the
compiler requires refreshing its standard-library notices; the local audit
host's default compiler was 1.98.0, so the collector explicitly used 1.95.0.
Existing binaries without matching build evidence are not certified by this
source and packaging review. Record the inventory hash alongside the actual
release build's provenance; bundle assembly now includes this hash as a
`dependency_license_inventory` material.

## Validation performed

The nine collector/packaging tests passed, including four platform layouts,
missing/tampered notices, stale manifests/lockfiles, and notice preservation
through archive, unpack, and installation. These use non-executed synthetic
binary headers. The existing fourteen adversarial bundle cases and package
conformance checks also passed. Two successive collections were byte-identical.

`cargo +1.95.0 package --list --allow-dirty --offline --locked` confirmed
`LICENSE` in all eighteen first-party Rust source packages and `NOTICE` in
the foundation package. The Python wheel and source archive were built and
their embedded license files matched the root Apache license byte for byte.
These checks establish packaging behavior; native runtime qualification remains
part of the separate release procedure.

[Release qualification](../docs/development/releases.md) · [Apache-2.0](../LICENSE)
