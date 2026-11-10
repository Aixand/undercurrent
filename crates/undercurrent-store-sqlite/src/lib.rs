// SPDX-License-Identifier: Apache-2.0

//! Daemon-private SQLite implementations of application persistence ports.
//!
//! [`SqliteSessionRepository`] serializes requests on one bounded worker and
//! commits session aggregates with their outbox events and snapshots. The same
//! handle retains fenced operation attempts, repair audits, runtime cancellation
//! controls, and agent event/reply evidence. Domain and application crates own
//! the contracts; this adapter does not execute process effects or serve clients.
//!
//! Opening and final handle drop are synchronous. Port calls return futures,
//! but dropping an admitted call does not cancel its database work. A missing
//! reply can therefore require operation-specific outcome reconciliation.
//!
//! Maintenance functions use independent synchronous connections. Backups include
//! committed WAL contents; restore requires an externally closed target.
//! These public Rust items support internal composition and tests, not a
//! supported external consumer API.

#![forbid(unsafe_code)]

mod sqlite;

pub use sqlite::*;
