// SPDX-License-Identifier: Apache-2.0

//! Retained output, profile, diagnostic, and daemon finite operations.

use undercurrent_protocol::{
    DaemonStatusDto, DiagnosticBundleDto, DiagnosticPageDto, GetProfileRequest, ProductOperation,
    ProductRequest, ProductSuccess, ProfileDetailDto, ProfileLintDto, ProfileMatchDto,
    ProfilePageDto, ProfileTestDto, ReadTerminalOutputRequest, RepairOperationAttemptRequest,
    RepairOperationAttemptResultDto, ShutdownAcceptedDto, TerminalOutputPageDto,
};

use super::{FiniteCliExecutor, operation_mismatch};
use crate::{
    CliCommand, CliError, CliInputError, DebugAction, DebugArguments, ProfileListArguments,
    ProfileMatchArguments, ProfileTestArguments, prepare_diagnostic_bundle_request,
    prepare_diagnostics_request, prepare_profile_lint_request, prepare_profile_list_request,
    prepare_profile_match_request, prepare_profile_test_request, prepare_shutdown_request,
};

impl FiniteCliExecutor<'_> {
    /// Reads one bounded retained-output page for one exact named session.
    ///
    /// # Errors
    ///
    /// Returns lookup, SDK, server, or correlation failure.
    pub fn terminal_output(
        &self,
        name: &str,
        from_offset: u64,
        max_bytes: u32,
    ) -> Result<TerminalOutputPageDto, CliError> {
        let session_id = self.resolve_name(name)?;
        self.terminal_output_id(session_id, from_offset, max_bytes)
    }

    /// Reads one bounded retained-output page for an already resolved session ID.
    ///
    /// This avoids a second mutable-name lookup when a caller transitions from
    /// finite replay to a live stream. The stable ID must have come from the same
    /// connected daemon. This method performs one product request and does not
    /// panic unless the injected request port panics.
    ///
    /// # Errors
    ///
    /// Returns SDK, server, or operation-correlation failure.
    pub fn terminal_output_id(
        &self,
        session_id: impl Into<String>,
        from_offset: u64,
        max_bytes: u32,
    ) -> Result<TerminalOutputPageDto, CliError> {
        let success = self.execute(ProductRequest::ReadTerminalOutput(
            ReadTerminalOutputRequest {
                session_id: session_id.into(),
                from_offset,
                max_bytes,
            },
        ))?;
        match success {
            ProductSuccess::ReadTerminalOutput(result) => Ok(result),
            other => Err(operation_mismatch(
                ProductOperation::ReadTerminalOutput,
                other,
            )),
        }
    }

    /// Lists one bounded profile-summary page.
    ///
    /// # Errors
    ///
    /// Returns local page-size, SDK, server, or correlation failure.
    pub fn profile_list(
        &self,
        arguments: &ProfileListArguments,
    ) -> Result<ProfilePageDto, CliError> {
        let request = prepare_profile_list_request(arguments)?;
        let success = self.execute(ProductRequest::ListProfiles(request))?;
        match success {
            ProductSuccess::ListProfiles(result) => Ok(result),
            other => Err(operation_mismatch(ProductOperation::ListProfiles, other)),
        }
    }

    /// Inspects one exact detection profile.
    ///
    /// # Errors
    ///
    /// Returns SDK, server, or correlation failure.
    pub fn profile(&self, name: &str) -> Result<ProfileDetailDto, CliError> {
        let success = self.execute(ProductRequest::GetProfile(GetProfileRequest {
            name: name.to_owned(),
        }))?;
        match success {
            ProductSuccess::GetProfile(result) => Ok(result),
            other => Err(operation_mismatch(ProductOperation::GetProfile, other)),
        }
    }

    /// Matches safe process characteristics to configured profiles.
    ///
    /// # Errors
    ///
    /// Returns SDK, server, or correlation failure.
    pub fn profile_match(
        &self,
        arguments: &ProfileMatchArguments,
    ) -> Result<ProfileMatchDto, CliError> {
        let success = self.execute(ProductRequest::MatchProfile(prepare_profile_match_request(
            arguments,
        )))?;
        match success {
            ProductSuccess::MatchProfile(result) => Ok(result),
            other => Err(operation_mismatch(ProductOperation::MatchProfile, other)),
        }
    }

    /// Validates all profiles or one exact profile.
    ///
    /// # Errors
    ///
    /// Returns SDK, server, or correlation failure.
    pub fn profile_lint(&self, name: Option<&str>) -> Result<ProfileLintDto, CliError> {
        let success = self.execute(ProductRequest::LintProfiles(prepare_profile_lint_request(
            name,
        )))?;
        match success {
            ProductSuccess::LintProfiles(result) => Ok(result),
            other => Err(operation_mismatch(ProductOperation::LintProfiles, other)),
        }
    }

    /// Evaluates one profile against one bounded private sample.
    ///
    /// # Errors
    ///
    /// Returns SDK, server, or correlation failure.
    pub fn profile_test(
        &self,
        arguments: &ProfileTestArguments,
    ) -> Result<ProfileTestDto, CliError> {
        let success = self.execute(ProductRequest::TestProfile(prepare_profile_test_request(
            arguments,
        )))?;
        match success {
            ProductSuccess::TestProfile(result) => Ok(result),
            other => Err(operation_mismatch(ProductOperation::TestProfile, other)),
        }
    }

    /// Reads one bounded typed operational diagnostic page.
    ///
    /// # Errors
    ///
    /// Returns local page-size, SDK, server, or correlation failure.
    pub fn diagnostics(&self, arguments: &DebugArguments) -> Result<DiagnosticPageDto, CliError> {
        let request = prepare_diagnostics_request(arguments)?;
        let success = self.execute(ProductRequest::GetDiagnostics(request))?;
        match success {
            ProductSuccess::GetDiagnostics(result) => Ok(result),
            other => Err(operation_mismatch(ProductOperation::GetDiagnostics, other)),
        }
    }

    /// Creates one bounded privacy-safe standalone diagnostic bundle.
    ///
    /// # Errors
    ///
    /// Returns local bounds, SDK, authorization, server, or response-correlation
    /// failure.
    pub fn diagnostic_bundle(&self, action: &DebugAction) -> Result<DiagnosticBundleDto, CliError> {
        let request = prepare_diagnostic_bundle_request(action)?;
        let success = self.execute(ProductRequest::CreateDiagnosticBundle(request))?;
        match success {
            ProductSuccess::CreateDiagnosticBundle(result) => Ok(result),
            other => Err(operation_mismatch(
                ProductOperation::CreateDiagnosticBundle,
                other,
            )),
        }
    }

    /// Repairs one durable Unknown attempt from daemon-retained evidence.
    ///
    /// The request carries the stable attempt identity and the required explicit
    /// `null` caller request digest for standalone/operator repair. Effect
    /// certainty, result digest, fence, and evidence are established by the
    /// daemon. This command always crosses the configured product SDK transport.
    ///
    /// # Errors
    ///
    /// Returns local identifier validation, SDK, authorization, evidence,
    /// durable-repair, or response-correlation failure.
    pub fn repair_operation_attempt(
        &self,
        attempt_id: String,
    ) -> Result<RepairOperationAttemptResultDto, CliError> {
        let request = RepairOperationAttemptRequest::new(attempt_id)
            .map_err(|_| CliInputError::InvalidArguments)?;
        let success = self.execute(ProductRequest::RepairOperationAttempt(request))?;
        match success {
            ProductSuccess::RepairOperationAttempt(result) => Ok(result),
            other => Err(operation_mismatch(
                ProductOperation::RepairOperationAttempt,
                other,
            )),
        }
    }

    /// Reads current daemon identity and aggregate pressure.
    ///
    /// # Errors
    ///
    /// Returns SDK, server, or correlation failure.
    pub fn daemon_status(&self) -> Result<DaemonStatusDto, CliError> {
        let success = self.execute(ProductRequest::GetDaemonStatus)?;
        match success {
            ProductSuccess::GetDaemonStatus(result) => Ok(result),
            other => Err(operation_mismatch(ProductOperation::GetDaemonStatus, other)),
        }
    }

    /// Begins one bounded idempotent graceful daemon shutdown.
    ///
    /// # Errors
    ///
    /// Returns local duration, SDK, server, or correlation failure.
    pub fn shutdown_daemon(&self, drain_timeout: &str) -> Result<ShutdownAcceptedDto, CliError> {
        self.shutdown_daemon_with_mode(
            drain_timeout,
            undercurrent_protocol::DaemonShutdownModeDto::Graceful,
        )
    }

    pub(crate) fn shutdown_daemon_with_mode(
        &self,
        drain_timeout: &str,
        mode: undercurrent_protocol::DaemonShutdownModeDto,
    ) -> Result<ShutdownAcceptedDto, CliError> {
        let mut request = prepare_shutdown_request(
            drain_timeout,
            self.mutations
                .next(CliCommand::Daemon, ProductOperation::ShutdownDaemon),
        )?;
        request.mode = mode;
        let success = self.execute(ProductRequest::ShutdownDaemon(request))?;
        match success {
            ProductSuccess::ShutdownDaemon(result) => Ok(result),
            other => Err(operation_mismatch(ProductOperation::ShutdownDaemon, other)),
        }
    }
}
