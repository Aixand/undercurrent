// SPDX-License-Identifier: Apache-2.0

//! Daemon composition and product adapters for Undercurrent.
//!
//! This outer crate joins the product protocol, application-facing execution
//! ports, and transport server without allowing those dependencies to leak
//! into the domain or application core.
//!
//! This is an internal composition library, not a supported external Rust
//! client surface. Clients use `undercurrent-sdk` and `undercurrent-protocol`.
//! The package's binaries are `uc-daemon` and its private `uc-session-host`
//! helper; the helper's UCHS channel is distinct from public local UCMP.
//!
//! [`DaemonProcess`] owns startup recovery, listener admission, and ordered
//! teardown. Session recovery precedes listener binding; unresolved operation
//! attempts can still keep process-mutation readiness closed after the listener
//! becomes reachable. [`DaemonProcessControl::begin_shutdown`] uses durable
//! host shutdown before final connection drain. Dropping client connections or
//! daemon-side runtime handles is not proof that supervised work has stopped.

mod agent_bridge_gateway;
mod agent_event_notifications;
pub(crate) use agent_event_notifications::AgentEventNotificationHub;
mod application_adapter;
mod attempt_tracking;
mod authorization;
mod background;
mod blocking;
#[allow(dead_code)]
mod bootstrap_config;
mod bootstrap_paths;
mod boundary;
mod cancellation;
mod composition;
mod config;
mod config_client;
mod config_services;
mod config_transport;
mod cursor;
mod dispatch;
mod dispatch_telemetry;
mod error;
mod event_kind;
mod execution_output_store;
mod failure_evidence;
pub use failure_evidence::{initialize_failure_evidence, record_process_failure};
mod logging;
mod multiplex_telemetry;
mod operations;
mod presentation;
#[cfg_attr(windows, path = "process_instance_windows.rs")]
mod process_instance;
mod profile_builtins;
mod profile_files;
mod profile_yaml;
mod registry;
mod runtime_diagnostics;
mod runtime_status;
mod screen_checkpoint;
mod stream;
mod structured_logging;
mod workers;

pub use agent_bridge_gateway::*;
pub use application_adapter::*;
pub use authorization::*;
pub use background::*;
pub use bootstrap_config::{
    ConfigDiscovery, ConfigError, ConfigSource, ResolvedRuntimePath, RuntimePathCandidates,
    RuntimePathSource, TelemetryConfig, load_config,
};
pub use bootstrap_paths::{PathEnvironment, PathsError, UndercurrentPaths, resolve_path_from_base};
pub use boundary::*;
pub use cancellation::*;
pub use composition::*;
pub use config::*;
pub use config_client::*;
pub use config_services::*;
pub use config_transport::*;
pub use cursor::*;
pub use dispatch::*;
pub use dispatch_telemetry::*;
pub use error::*;
pub use logging::*;
pub use multiplex_telemetry::*;
pub use operations::*;
pub use presentation::*;
pub use process_instance::{prepare_instance_scope_parent, prepare_private_directory};
pub use registry::*;
pub use screen_checkpoint::*;
pub use stream::*;
pub use workers::*;
