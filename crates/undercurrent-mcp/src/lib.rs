// SPDX-License-Identifier: Apache-2.0
//! Local MCP adapter over the public Undercurrent SDK. No embedded runtime.
mod client;
mod operations;
mod params;
mod server;

pub use client::Connection;
pub use server::{MAX_MESSAGE_BYTES, Server};
