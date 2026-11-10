# Diagnose storage and preserve recoverable state

Use this guide when startup rejects durable state or before restoring a backup.
Preserve configuration, matching binaries, the database and SQLite sidecars,
private host records, and execution-output files as a consistent set.

## Identify the failed boundary

| Failure | Next step |
| --- | --- |
| Home layout or retired configuration | Follow [configuration diagnosis](../configuration/diagnose.md#diagnose-a-rejection) and check the [current home layout](../configuration/README.md#home-directory-contents). |
| Storage format or corruption | Inspect the exact error against the [storage format](../../../crates/undercurrent-store-sqlite/docs/reference/storage-format.md) and its owning implementation. |
| Unknown process or mutation outcome | Reconcile the exact attempt using [request recovery](../../sdk/recovery.md) and [daemon diagnostics](diagnostics.md). |

SQLite has numeric storage marker 1 and release identity `0.1.0-beta`. The exact
complete layout fingerprint, row markers, and digests also govern admission.
The [database implementation](../../../crates/undercurrent-store-sqlite/src/sqlite/database.rs)
owns these checks. Editing `user_version` or the schema identity cannot establish
compatibility. Repository open, maintenance verification, and per-record decoding
inspect different things; passing one is not a complete payload audit.

## Back up and restore with all owners accounted for

Follow [store maintenance](../../../crates/undercurrent-store-sqlite/docs/development/maintenance.md)
for the maintained isolated backup/restore example and its exact guarantees.
SQLite-aware backup includes committed WAL state; an ordinary copy of a live
database does not provide that contract. A database backup does not capture the
whole live session host, output history, or private credentials.

Restore requires closing every target owner. The helper does not enforce that
exclusion for callers, and it is an internal maintainer API rather than a
general `uc restore` command. It stages and verifies candidates, with rollback
attempts for returned errors. It does not promise crash-atomic replacement or
power-loss parity across platforms. Keep the original backup and any retained
candidate/rollback sidecars until the reopened system has been reconciled.

For an unsupported layout, preserve the exact files, matching binaries, and
error classification for the storage owner. Do not reset, delete sidecars,
merge databases, or restore while processes can still use the target. A new
empty database beside retained host effects cannot establish a recovered state.

[Daemon guides](README.md) · [Troubleshooting](../troubleshooting.md) · [Documentation index](../../README.md)
