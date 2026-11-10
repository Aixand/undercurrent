// SPDX-License-Identifier: Apache-2.0
//! Protected local IPC for the public Undercurrent client boundary.
//!
//! This is an internal adapter, not a supported external Rust SDK surface.
//! Linux and macOS use authenticated Unix sockets; Windows uses authenticated
//! local named pipes. The daemon supplies product dispatch through transport
//! core, whose internal composition types are re-exported here.
//!
//! [`LocalEndpoint`] performs lexical address validation. Native bind/connect
//! performs OS-specific checks; [`LocalFrameStream`] supplies bounded framing
//! without interpreting product payloads or owning durable application state.
//! Async I/O requires a caller-owned Tokio runtime with I/O and time enabled.
mod endpoint;
pub use endpoint::LocalEndpoint;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod unix;
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub use unix::*;
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::*;
mod framed;
pub use framed::{CAPACITY_CLOSE_FRAME, LocalFrameStream};
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
mod server;
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
pub use server::*;
mod error;
pub use error::*;
pub use undercurrent_transport_core::*;
