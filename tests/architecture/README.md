# Undercurrent architecture gate

The gate reads the workspace and consumer lockfiles. Make the locked dependencies
available locally and put Cargo, Python 3, and GNU-compatible `nm` on `PATH`.
From the repository root, run `scripts/check-architecture.py --all`.

The [architecture testing guide](../../docs/development/testing/architecture.md)
explains individual checks, fixtures, and permitted dependencies.
