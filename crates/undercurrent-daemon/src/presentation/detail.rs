// SPDX-License-Identifier: Apache-2.0

use undercurrent_application::{ResolvedExecutionContext, SessionDetail, SessionPersistenceStatus};
use undercurrent_domain::{
    HealthRule, HealthRuleKind, OutputCaptureMode, ProfileBindingMode, ProfileSelectionOrigin,
    RestartPolicy, StopSignal, WorkItem, WorkRelationKind, WorkTarget,
};
use undercurrent_protocol::{
    CommandSpecDto, ExecutableResolutionDto, ExecutionContextDto, ExecutionEnvironmentBindingDto,
    ExecutionPolicyDto, ForegroundEvidenceDto, HealthResultDto, HealthRuleDto, HealthRulesDto,
    OutputCaptureModeDto, ProcessDetailDto, ProfileBindingModeDto, ProfileSelectionOriginDto,
    RestartPolicyDto, SessionConfigurationDetailDto, SessionDetailDto, StopPolicyDto,
    StopSignalDto, TerminalSizeDto, WorkContextDetailDto, WorkItemDto, WorkRelationshipDto,
    WorkTargetDto,
};

use super::{
    ProductPresentation, ProductPresentationError, evidence_disposition, foreground_axis,
    foreground_claim, labels, work_status,
};

impl ProductPresentation {
    /// Copies private resolved context into its purpose-built authorized DTO.
    ///
    /// The returned values must remain inside the finite `exec` response and
    /// must never be logged, traced, or added to a general detail projection.
    pub fn execution_context(context: &ResolvedExecutionContext) -> ExecutionContextDto {
        ExecutionContextDto {
            environment: context.environment().clone(),
            working_directory: context.working_directory().map(str::to_owned),
        }
    }

    /// Formats one already authorized, explicitly projected session detail.
    pub fn session_detail(
        detail: &SessionDetail,
    ) -> Result<SessionDetailDto, ProductPresentationError> {
        Ok(SessionDetailDto {
            shell_environment: None,
            restoration_directory: detail
                .restoration_directory()
                .map(|p| p.as_str().to_owned()),
            summary: Self::session_summary(detail.summary())?,
            configuration: detail.configuration().map(configuration).transpose()?,
            work_context: detail
                .work_context()
                .map(|work| {
                    Ok(WorkContextDetailDto {
                        revision: work.revision().value(),
                        items: work
                            .items()
                            .iter()
                            .map(work_item)
                            .collect::<Result<Vec<_>, ProductPresentationError>>()?,
                    })
                })
                .transpose()?,
            process: detail.process().map(|process| ProcessDetailDto {
                instance_id: process.id.as_str().to_owned(),
                started_at_millis: process.started_at.value(),
            }),
            foreground_evidence: detail.foreground_decision().map(|decision| {
                decision
                    .evidence()
                    .iter()
                    .map(|assessed| {
                        let evidence = assessed.evidence();
                        ForegroundEvidenceDto {
                            source: evidence.source().as_str().to_owned(),
                            priority: evidence.priority().as_str().to_owned(),
                            disposition: evidence_disposition(evidence.disposition()),
                            axis: foreground_axis(evidence.axis()),
                            claim: evidence.claim().map(foreground_claim),
                            confidence_basis_points: evidence.confidence().basis_points(),
                            observed_at_millis: evidence.observed_at().value(),
                            assessment: assessed.assessment().as_str().to_owned(),
                        }
                    })
                    .collect()
            }),
            health_results: detail
                .health_observation()
                .map(|observation| {
                    observation
                        .results()
                        .iter()
                        .map(|result| {
                            Ok(HealthResultDto {
                                rule_index: result.rule_index(),
                                passed: result.passed(),
                                latency_millis: u64::try_from(result.latency().as_millis())
                                    .map_err(|_| ProductPresentationError {
                                        field: "health_latency_millis",
                                    })?,
                                safe_detail: result.detail().map(str::to_owned),
                            })
                        })
                        .collect::<Result<Vec<_>, ProductPresentationError>>()
                })
                .transpose()?,
            persisted: matches!(detail.persistence(), SessionPersistenceStatus::Persisted),
        })
    }
}

fn configuration(
    value: &undercurrent_application::SessionConfigurationDetail,
) -> Result<SessionConfigurationDetailDto, ProductPresentationError> {
    let execution = value.execution();
    let restart = execution.restart();
    let restart_dto = match restart.policy() {
        RestartPolicy::Never => RestartPolicyDto::Never,
        RestartPolicy::OnFailure => {
            let initial_delay_millis =
                duration_millis(restart.initial_delay(), "restart_initial_delay_millis")?;
            let max_delay_millis =
                duration_millis(restart.max_delay(), "restart_max_delay_millis")?;
            let min_uptime_millis =
                duration_millis(restart.min_uptime(), "restart_min_uptime_millis")?;
            RestartPolicyDto::OnFailure {
                max_attempts: restart.max_restarts(),
                initial_delay_millis,
                max_delay_millis,
                backoff_basis_points: restart.backoff_basis_points(),
                min_uptime_millis,
            }
        }
        RestartPolicy::Always => {
            let initial_delay_millis =
                duration_millis(restart.initial_delay(), "restart_initial_delay_millis")?;
            let max_delay_millis =
                duration_millis(restart.max_delay(), "restart_max_delay_millis")?;
            let min_uptime_millis =
                duration_millis(restart.min_uptime(), "restart_min_uptime_millis")?;
            RestartPolicyDto::Always {
                max_attempts: restart.max_restarts(),
                initial_delay_millis,
                max_delay_millis,
                backoff_basis_points: restart.backoff_basis_points(),
                min_uptime_millis,
            }
        }
    };
    let stop = execution.stop();
    Ok(SessionConfigurationDetailDto {
        command: CommandSpecDto {
            program: value.command().program().to_owned(),
            arguments: value.command().arguments().to_vec(),
        },
        executable_resolution: value.command().executable_resolution().map(|resolution| {
            ExecutableResolutionDto {
                launch_mode: match value.command().launch_mode() {
                    undercurrent_domain::LaunchMode::Native => {
                        undercurrent_protocol::LaunchModeDto::Native
                    }
                    undercurrent_domain::LaunchMode::Sealed => {
                        undercurrent_protocol::LaunchModeDto::Sealed
                    }
                },
                requested_reference: resolution.requested_reference().to_owned(),
                resolved_program: value.command().program().to_owned(),
                content_digest_sha256: resolution.content_digest().to_hex(),
                receipt_digest_sha256: resolution.receipt_digest().to_hex(),
            }
        }),
        environment_variable_names: value.environment().variable_names().to_vec(),
        environment_file: value
            .environment()
            .environment_file()
            .map(|path| path.as_str().to_owned()),
        working_directory: value
            .working_directory()
            .map(|path| path.as_str().to_owned()),
        environment_binding: value.environment().binding().map(|binding| {
            ExecutionEnvironmentBindingDto {
                binding_digest_sha256: binding.digest().to_hex(),
                working_directory_root: binding.working_directory_root().as_str().to_owned(),
            }
        }),
        terminal: TerminalSizeDto {
            columns: value.terminal_size().columns(),
            rows: value.terminal_size().rows(),
        },
        output_capture_mode: match value.output_capture_mode() {
            OutputCaptureMode::Terminal => OutputCaptureModeDto::Terminal,
            OutputCaptureMode::Opaque => OutputCaptureModeDto::Opaque,
        },
        execution: ExecutionPolicyDto {
            restart: restart_dto,
            stop: StopPolicyDto {
                signal: stop_signal(stop.signal()),
                timeout_millis: duration_millis(stop.force_after(), "stop_timeout_millis")?,
            },
            detection_enabled: execution.detection_enabled(),
            auto_remove: execution.auto_remove(),
        },
        health: HealthRulesDto {
            rules: value
                .health_rules()
                .iter()
                .map(health_rule)
                .collect::<Result<Vec<_>, _>>()?,
        },
        profile: value.profile().map(|profile| profile.as_str().to_owned()),
        profile_origin: value.profile_origin().map(|origin| match origin {
            ProfileSelectionOrigin::Explicit => ProfileSelectionOriginDto::Explicit,
            ProfileSelectionOrigin::LaunchCommand => ProfileSelectionOriginDto::LaunchCommand,
        }),
        profile_binding: match value.profile_binding() {
            ProfileBindingMode::Fixed => ProfileBindingModeDto::Fixed,
            ProfileBindingMode::ForegroundAdaptive => ProfileBindingModeDto::ForegroundAdaptive,
        },
        labels: labels(value.labels()),
        metadata: value
            .metadata()
            .iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect(),
    })
}

fn health_rule(value: &HealthRule) -> Result<HealthRuleDto, ProductPresentationError> {
    Ok(match value.kind() {
        HealthRuleKind::TcpPort => HealthRuleDto::TcpPort {
            port: value.port().ok_or(ProductPresentationError {
                field: "health_tcp_port",
            })?,
        },
        HealthRuleKind::HttpEndpoint => HealthRuleDto::HttpEndpoint {
            endpoint: health_text(value)?.to_owned(),
        },
        HealthRuleKind::OutputContains => HealthRuleDto::OutputContains {
            pattern: health_text(value)?.to_owned(),
        },
        HealthRuleKind::OutputRejects => HealthRuleDto::OutputRejects {
            pattern: health_text(value)?.to_owned(),
        },
        HealthRuleKind::RedCellThreshold => HealthRuleDto::RedCellThreshold {
            minimum_cells: value.minimum_cells().ok_or(ProductPresentationError {
                field: "health_red_cell_threshold",
            })?,
        },
        HealthRuleKind::Command => {
            let command = value.command_spec().ok_or(ProductPresentationError {
                field: "health_command",
            })?;
            HealthRuleDto::Command {
                command: CommandSpecDto {
                    program: command.program().to_owned(),
                    arguments: command.arguments().to_vec(),
                },
            }
        }
    })
}

fn health_text(value: &HealthRule) -> Result<&str, ProductPresentationError> {
    value.text_value().ok_or(ProductPresentationError {
        field: "health_rule_text",
    })
}

fn work_item(value: &WorkItem) -> Result<WorkItemDto, ProductPresentationError> {
    Ok(WorkItemDto {
        owner: value.key().owner().as_str().to_owned(),
        item_id: value.key().id().as_str().to_owned(),
        relationship: match value.relation() {
            WorkRelationKind::DependsOn => WorkRelationshipDto::DependsOn,
            WorkRelationKind::Delegated => WorkRelationshipDto::Delegated,
            WorkRelationKind::Spawned => WorkRelationshipDto::Spawned,
            WorkRelationKind::Related => WorkRelationshipDto::Related,
        },
        title: value.title().to_owned(),
        description: value.description().map(str::to_owned),
        status: work_status(value.status()),
        targets: value.targets().map(work_target).collect(),
        provenance: value.provenance().as_str().to_owned(),
        created_at_millis: value.created_at().value(),
        updated_at_millis: value.updated_at().value(),
        expires_at_millis: value.expires_at().map(|time| time.value()),
        metadata: value
            .metadata()
            .iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect(),
    })
}

fn work_target(value: &WorkTarget) -> WorkTargetDto {
    match value {
        WorkTarget::Session(session_id) => WorkTargetDto::SessionId {
            session_id: session_id.as_str().to_owned(),
        },
        WorkTarget::Labels(value) => WorkTargetDto::Labels {
            labels: labels(value),
        },
        WorkTarget::External { system, id } => WorkTargetDto::External {
            namespace: system.as_str().to_owned(),
            selector: id.as_str().to_owned(),
        },
    }
}

const fn stop_signal(value: StopSignal) -> StopSignalDto {
    match value {
        StopSignal::Terminate => StopSignalDto::Terminate,
        StopSignal::Interrupt => StopSignalDto::Interrupt,
        StopSignal::Hangup => StopSignalDto::Hangup,
        StopSignal::Quit => StopSignalDto::Quit,
        StopSignal::User1 => StopSignalDto::User1,
        StopSignal::User2 => StopSignalDto::User2,
    }
}

fn duration_millis(
    value: std::time::Duration,
    field: &'static str,
) -> Result<u64, ProductPresentationError> {
    u64::try_from(value.as_millis()).map_err(|_| ProductPresentationError { field })
}
