// SPDX-License-Identifier: Apache-2.0

//! Native CLI presentation foundations for Undercurrent.
//!
//! This crate owns CLI-only command names, presentation context, process
//! startup, and typed error translation. Domain and application behavior remain
//! behind the typed product SDK.
//!
//! The native entry point is [`run_native_cli_process`]. It routes local commands
//! without a product connection and shares one blocking SDK client across a
//! daemon-backed invocation. Eligible session, group, and profile commands can
//! start an absent local daemon; connection errors do not generally imply absence.
//!
//! Public items expose binary orchestration and test seams, not a supported
//! external Rust SDK. Consumers should use `undercurrent-sdk` and
//! `undercurrent-protocol`. The crate README links command, lifecycle, output,
//! interactive-workspace, and contributor guides.

#![deny(missing_docs)]

mod arguments;
mod attach;
#[allow(dead_code)]
mod bootstrap_config;
#[allow(dead_code)]
mod bootstrap_paths;
mod cli_support;
mod client;
mod context;
mod daemon_lifecycle;
mod deadline;
mod detach;
mod diagnostics;
mod dispatch;
mod duration;
mod error;
mod execution_limits;
mod executor;
mod group;
pub use group::api::{GroupClient, GroupMember, GroupRecord};
mod help;
mod incidents;
mod interactive_terminal;
mod local;
mod native;
mod preparation;
mod presentation;
mod registry;
mod reset;
mod runtime_config;
mod session_context;
mod session_picker;
mod settings;
mod shell_integration;
mod shortcuts;
mod signals;
mod stop;
mod stream_dispatch;
mod streams;
mod support;
mod top;
mod tui;

pub use arguments::*;
pub use attach::*;
pub use bootstrap_config::{ConfigDiscovery, RuntimePathSource, TelemetryConfig};
pub use bootstrap_paths::PathEnvironment;
pub use cli_support::*;
pub use client::*;
pub use context::*;
pub use daemon_lifecycle::*;
pub use detach::*;
pub use diagnostics::*;
pub use dispatch::*;
pub use duration::*;
pub use error::*;
pub use execution_limits::*;
pub use executor::*;
pub use interactive_terminal::*;
pub use local::*;
pub use native::*;
pub use preparation::*;
pub use presentation::*;
pub use registry::*;
pub use reset::*;
pub use runtime_config::*;
pub use settings::{
    AutoStartPolicy, CliSettingsError, ConnectionProfileAction, SettingsAction, SettingsArguments,
    SettingsSet,
};
pub use shell_integration::*;
pub use signals::*;
pub use stream_dispatch::*;
pub use streams::*;

#[cfg(test)]
pub(crate) fn test_foreground(
    execution: undercurrent_protocol::ExecutionStateDto,
    interaction: undercurrent_protocol::InteractionStateDto,
) -> undercurrent_protocol::ForegroundObservationDto {
    use undercurrent_protocol::{
        EvidenceDispositionDto, ExecutionObservationDto, ForegroundObservationDto,
        InteractionModalityDto, InteractionObservationDto, InteractionStateDto,
        ObservationBasisDto, RuntimeConstraintObservationDto, RuntimeConstraintStateDto,
    };

    let basis = ObservationBasisDto {
        confidence_basis_points: 9_000,
        source: Some("test".to_owned()),
        disposition: EvidenceDispositionDto::Affirmative,
        committed_at_millis: 1,
        evidence_observed_at_millis: Some(1),
    };
    ForegroundObservationDto {
        execution: ExecutionObservationDto {
            state: execution,
            basis: basis.clone(),
        },
        interaction: InteractionObservationDto {
            state: interaction,
            modality: (interaction != InteractionStateDto::None)
                .then_some(InteractionModalityDto::Options),
            semantic: (interaction == InteractionStateDto::Requested)
                .then(|| "test.request".to_owned()),
            basis: basis.clone(),
        },
        constraint: RuntimeConstraintObservationDto {
            state: RuntimeConstraintStateDto::Clear,
            semantic: None,
            basis,
        },
    }
}
