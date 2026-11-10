// SPDX-License-Identifier: Apache-2.0
//! Shared configuration documents, loading, validation, and home paths for the
//! standalone CLI and daemon. This is an internal workspace surface; supported
//! external Rust clients use `undercurrent-sdk` and `undercurrent-protocol`.
//!
//! Start with [`load_config`] and [`ConfigDiscovery`] for a validated snapshot.
//! [`UndercurrentPaths::resolve`] only derives paths; it creates no directories.
//! Loading performs synchronous bounded file reads, with no daemon connection,
//! runtime construction, file writes, or live-reload watcher.
//!
//! [`UndercurrentProductConfig`] owns product defaults and field meanings.
//! Direct Serde decoding and subsequent mutation of public fields do not run
//! shared validation. CLI and daemon adapters retain their component-specific
//! validation after loading. [`CURRENT_CONFIG_SCHEMA_VERSION`] identifies the
//! supported configuration release, currently `0.1.0-beta`.
mod document;
mod load;
mod paths;
mod validate;
pub use document::*;
pub use load::*;
pub use paths::*;
