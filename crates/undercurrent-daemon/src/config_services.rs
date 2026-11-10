// SPDX-License-Identifier: Apache-2.0

//! Validated domain boundary, application registry, and observation settings.

pub use undercurrent_config::DaemonServiceConfig;

use std::fmt::{Debug, Formatter};
use std::time::Duration;

use undercurrent_application::{OutboxDispatchLimits, ProfileServiceLimits, TerminalOutputLimits};
use undercurrent_domain::{
    Confidence, DetectionProfileLimits, ExecutionLimits, ForegroundDecisionPolicy,
};
use undercurrent_runtime::{
    NativeProcessRuntimeLimits, RegexPatternEngineLimits, RuntimeEvidenceSource,
    TerminalForegroundEvidenceLimits,
};

use crate::{
    DaemonConfigError, DaemonTransportSettings, ProductBoundary, ProductBoundaryLimits,
    ProductEventBoundaryLimits, ProductProfileBoundaryLimits,
};

/// Non-zero capacities for concrete in-memory application adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DaemonResourceLimits {
    session_subscriptions: usize,
    restart_jobs: usize,
    observation_jobs: usize,
    input_delivery_records: usize,
    session_event_records: usize,
    session_event_subscriptions: usize,
    telemetry_diagnostic_records: usize,
    telemetry_metric_records: usize,
    detection_profiles: usize,
}

impl DaemonResourceLimits {
    /// Returns the active prepared session query subscription cap.
    pub const fn session_subscriptions(self) -> usize {
        self.session_subscriptions
    }

    /// Returns the pending scheduled restart cap.
    pub const fn restart_jobs(self) -> usize {
        self.restart_jobs
    }

    /// Returns the pending scheduled observation and plan cap.
    pub const fn observation_jobs(self) -> usize {
        self.observation_jobs
    }

    /// Returns the semantic input delivery record cap.
    pub const fn input_delivery_records(self) -> usize {
        self.input_delivery_records
    }

    /// Returns retained published session event capacity.
    pub const fn session_event_records(self) -> usize {
        self.session_event_records
    }

    /// Returns active selective session event subscription capacity.
    pub const fn session_event_subscriptions(self) -> usize {
        self.session_event_subscriptions
    }

    /// Returns retained diagnostic record capacity.
    pub const fn telemetry_diagnostic_records(self) -> usize {
        self.telemetry_diagnostic_records
    }

    /// Returns retained metric observation capacity.
    pub const fn telemetry_metric_records(self) -> usize {
        self.telemetry_metric_records
    }

    /// Returns active detection profile capacity.
    pub const fn detection_profiles(self) -> usize {
        self.detection_profiles
    }
}

/// Fully validated service, boundary, registry, and observation settings.
#[derive(Clone)]
pub struct DaemonServiceSettings {
    boundary: ProductBoundary,
    resources: DaemonResourceLimits,
    profile_service: ProfileServiceLimits,
    profile_definition: DetectionProfileLimits,
    regex_pattern: RegexPatternEngineLimits,
    foreground: ForegroundDecisionPolicy,
    foreground_evidence: TerminalForegroundEvidenceLimits,
}

impl DaemonServiceSettings {
    /// Validates the domain boundary and every in-memory application capacity.
    pub(crate) fn validate(
        config: DaemonServiceConfig,
        terminal: TerminalOutputLimits,
        process: NativeProcessRuntimeLimits,
        outbox: OutboxDispatchLimits,
        transport: &DaemonTransportSettings,
    ) -> Result<Self, DaemonConfigError> {
        let execution = ExecutionLimits {
            max_arguments: config.max_command_arguments,
            max_command_bytes: config.max_command_bytes,
            max_environment_entries: config.max_environment_entries,
            max_environment_bytes: config.max_environment_bytes,
            max_path_bytes: config.max_path_bytes,
            max_health_rules: config.max_health_rules,
            max_health_rule_bytes: config.max_health_rule_bytes,
        };
        let boundary = ProductBoundary::new(ProductBoundaryLimits {
            execution,
            max_terminal_columns: config.max_terminal_columns,
            max_terminal_rows: config.max_terminal_rows,
            max_input_items: config.max_input_items,
            max_input_bytes: config.max_input_bytes,
            max_query_page_size: config.max_query_page_size,
            max_work_items: config.max_work_items,
            events: ProductEventBoundaryLimits {
                max_filter_sessions: config.max_event_filter_sessions,
                max_filter_kinds: config.max_event_filter_kinds,
                max_page_size: config.max_event_page_size,
            },
            profiles: ProductProfileBoundaryLimits {
                max_page_size: config.max_profile_page_size,
                max_environment_name_bytes: config.max_profile_environment_name_bytes,
                max_sample_bytes: config.max_profile_sample_bytes,
            },
        })
        .map_err(|source| DaemonConfigError::component("services.boundary", source))?;
        let profile_service = ProfileServiceLimits {
            max_profiles: config.detection_profiles,
            max_test_samples: config.profile_test_samples,
        }
        .validate()
        .map_err(|source| DaemonConfigError::component("services.profile_service", source))?;
        let profile_definition = DetectionProfileLimits {
            max_description_bytes: config.profile_description_bytes,
            max_binary_patterns: config.profile_binary_patterns,
            max_argument_patterns: config.profile_argument_patterns,
            max_environment_names: config.profile_environment_names,
            max_environment_name_bytes: config.profile_environment_name_bytes,
            max_foreground_rules: config.profile_foreground_rules,
            max_background_rules: config.profile_background_rules,
            max_patterns_per_rule: config.profile_patterns_per_rule,
            max_pattern_bytes: config.profile_pattern_bytes,
            max_trait_needles: config.profile_trait_needles,
            max_trait_text_bytes: config.profile_trait_text_bytes,
            max_input_submission_delay_millis: config.profile_input_submission_delay_millis,
        }
        .validate()
        .map_err(|source| DaemonConfigError::component("services.profile_definition", source))?;
        let regex_pattern = RegexPatternEngineLimits {
            cached_patterns: config.regex_cached_patterns,
            compiled_size_bytes: config.regex_compiled_size_bytes,
            determinize_size_bytes: config.regex_determinize_size_bytes,
            match_text_bytes: config.regex_match_text_bytes,
        }
        .validate()
        .map_err(|source| DaemonConfigError::component("services.regex_pattern", source))?;
        let confidence =
            Confidence::from_basis_points(config.foreground_minimum_confidence_basis_points)
                .map_err(|source| DaemonConfigError::component("services.foreground", source))?;
        let foreground = ForegroundDecisionPolicy::new(
            config.foreground_evidence_items,
            Duration::from_millis(config.foreground_evidence_age_millis),
            confidence,
        )
        .map_err(|source| DaemonConfigError::component("services.foreground", source))?;
        let foreground_evidence = TerminalForegroundEvidenceLimits {
            tracked_sessions: process.max_processes,
            profile_samples: config.profile_foreground_rules,
            profile_sample_bytes: config.max_profile_sample_bytes,
            motion_rows: usize::from(config.max_terminal_rows),
            motion_confidence: Confidence::from_basis_points(
                config.foreground_motion_confidence_basis_points,
            )
            .map_err(|source| {
                DaemonConfigError::component("services.foreground_evidence", source)
            })?,
            server_idle_confidence: Confidence::from_basis_points(
                config.foreground_server_idle_confidence_basis_points,
            )
            .map_err(|source| {
                DaemonConfigError::component("services.foreground_evidence", source)
            })?,
        }
        .validate()
        .map_err(|source| DaemonConfigError::component("services.foreground_evidence", source))?;
        let resources = validate_resources(&config)?;
        validate_relationships(&config, terminal, process, outbox, transport)?;
        Ok(Self {
            boundary,
            resources,
            profile_service,
            profile_definition,
            regex_pattern,
            foreground,
            foreground_evidence,
        })
    }

    /// Returns the validated external-to-domain request translator.
    pub const fn boundary(&self) -> ProductBoundary {
        self.boundary
    }

    /// Returns the product-protocol maximum for one compact query page.
    pub const fn max_query_page_size(&self) -> usize {
        self.boundary.max_query_page_size()
    }

    /// Returns the product-protocol maximum terminal columns.
    pub const fn max_terminal_columns(&self) -> u16 {
        self.boundary.max_terminal_columns()
    }

    /// Returns the product-protocol maximum terminal rows.
    pub const fn max_terminal_rows(&self) -> u16 {
        self.boundary.max_terminal_rows()
    }

    /// Returns the product-protocol maximum items in one terminal input request.
    pub const fn max_input_items(&self) -> usize {
        self.boundary.max_input_items()
    }

    /// Returns the product-protocol maximum estimated terminal input bytes.
    pub const fn max_input_bytes(&self) -> usize {
        self.boundary.max_input_bytes()
    }

    /// Returns concrete in-memory adapter capacities.
    pub const fn resources(&self) -> DaemonResourceLimits {
        self.resources
    }

    /// Returns standalone profile query and test bounds.
    pub const fn profile_service(&self) -> ProfileServiceLimits {
        self.profile_service
    }

    /// Returns complete detection profile definition bounds.
    pub const fn profile_definition(&self) -> DetectionProfileLimits {
        self.profile_definition
    }

    /// Returns compiled-pattern cache and execution bounds.
    pub const fn regex_pattern(&self) -> RegexPatternEngineLimits {
        self.regex_pattern
    }

    /// Returns best-effort foreground evidence decision policy.
    pub const fn foreground(&self) -> ForegroundDecisionPolicy {
        self.foreground
    }

    /// Returns bounded parser-backed foreground-evidence adapter settings.
    pub const fn foreground_evidence(&self) -> TerminalForegroundEvidenceLimits {
        self.foreground_evidence
    }
}

impl Debug for DaemonServiceSettings {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonServiceSettings")
            .field("boundary", &self.boundary)
            .field("resources", &self.resources)
            .field("profile_service", &self.profile_service)
            .field("profile_definition", &self.profile_definition)
            .field("regex_pattern", &self.regex_pattern)
            .field("foreground", &self.foreground)
            .field("foreground_evidence", &self.foreground_evidence)
            .finish()
    }
}

fn validate_resources(
    config: &DaemonServiceConfig,
) -> Result<DaemonResourceLimits, DaemonConfigError> {
    for (component, value) in [
        ("session_subscriptions", config.session_subscriptions),
        ("restart_jobs", config.restart_jobs),
        ("observation_jobs", config.observation_jobs),
        ("input_delivery_records", config.input_delivery_records),
        ("session_event_records", config.session_event_records),
        (
            "session_event_subscriptions",
            config.session_event_subscriptions,
        ),
        (
            "telemetry_diagnostic_records",
            config.telemetry_diagnostic_records,
        ),
        ("telemetry_metric_records", config.telemetry_metric_records),
        ("detection_profiles", config.detection_profiles),
    ] {
        if value == 0 {
            return Err(DaemonConfigError::InvalidRelationship {
                rule: match component {
                    "session_subscriptions" => "session subscription capacity must be non-zero",
                    "restart_jobs" => "restart job capacity must be non-zero",
                    "observation_jobs" => "observation job capacity must be non-zero",
                    "input_delivery_records" => "input delivery record capacity must be non-zero",
                    "session_event_records" => "session event record capacity must be non-zero",
                    "session_event_subscriptions" => {
                        "session event subscription capacity must be non-zero"
                    }
                    "telemetry_diagnostic_records" => {
                        "telemetry diagnostic capacity must be non-zero"
                    }
                    "telemetry_metric_records" => "telemetry metric capacity must be non-zero",
                    _ => "detection profile capacity must be non-zero",
                },
            });
        }
    }
    Ok(DaemonResourceLimits {
        session_subscriptions: config.session_subscriptions,
        restart_jobs: config.restart_jobs,
        observation_jobs: config.observation_jobs,
        input_delivery_records: config.input_delivery_records,
        session_event_records: config.session_event_records,
        session_event_subscriptions: config.session_event_subscriptions,
        telemetry_diagnostic_records: config.telemetry_diagnostic_records,
        telemetry_metric_records: config.telemetry_metric_records,
        detection_profiles: config.detection_profiles,
    })
}

fn validate_relationships(
    config: &DaemonServiceConfig,
    terminal: TerminalOutputLimits,
    process: NativeProcessRuntimeLimits,
    outbox: OutboxDispatchLimits,
    transport: &DaemonTransportSettings,
) -> Result<(), DaemonConfigError> {
    let connection_streams = transport.policy().limits().max_logical_streams() as usize;
    for (capacity, rule) in [
        (
            config.session_subscriptions,
            "session subscription capacity must cover one connection's logical streams",
        ),
        (
            config.session_event_subscriptions,
            "event subscription capacity must cover one connection's logical streams",
        ),
        (
            terminal.subscriptions,
            "terminal subscription capacity must cover one connection's logical streams",
        ),
    ] {
        if capacity < connection_streams {
            return Err(DaemonConfigError::InvalidRelationship { rule });
        }
    }
    let all_observations = process.max_processes.saturating_mul(2);
    if config.observation_jobs < all_observations {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "observation job capacity must cover foreground and health for every process",
        });
    }
    if config.session_event_records < outbox.batch_size() {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "session event retention must contain one complete outbox dispatch batch",
        });
    }
    if config.detection_profiles < config.max_profile_page_size {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "profile catalog capacity must cover one maximum profile page",
        });
    }
    if config.regex_match_text_bytes < config.max_profile_sample_bytes {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "regex match input must contain one maximum profile test sample",
        });
    }
    if config.regex_match_text_bytes < process.screen_snapshot_bytes {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "regex match input must contain one maximum live screen snapshot",
        });
    }
    if config.foreground_evidence_items < RuntimeEvidenceSource::ALL.len() {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "foreground decision capacity must retain every runtime evidence source",
        });
    }
    if config.foreground_motion_confidence_basis_points
        < config.foreground_minimum_confidence_basis_points
        || config.foreground_server_idle_confidence_basis_points
            < config.foreground_minimum_confidence_basis_points
    {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "runtime foreground confidence must meet the foreground decision minimum",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use undercurrent_application::OutboxDispatchLimits;
    use undercurrent_runtime::NativeProcessRuntimeLimits;

    use super::*;
    use crate::{DaemonRequestConfig, DaemonTransportConfig};

    fn terminal() -> TerminalOutputLimits {
        TerminalOutputLimits {
            per_session_bytes: 2_097_152,
            global_bytes: 268_435_456,
            chunk_bytes: 16_384,
            append_bytes: 65_536,
            replay_bytes: 1_048_576,
            sessions: 2_048,
            subscriptions: 4_096,
        }
    }

    fn process() -> NativeProcessRuntimeLimits {
        NativeProcessRuntimeLimits {
            max_processes: 2_048,
            spawn_workers: 8,
            spawn_queue_capacity: 512,
            input_queue_sequences: 64,
            input_queue_bytes_per_process: 1_048_576,
            output_read_bytes: 16_384,
            screen_snapshot_bytes: 131_072,
            environment_file_bytes: 1_048_576,
            environment_entries: 1_024,
            environment_bytes: 2_097_152,
            worker_thread_stack_bytes: 524_288,
        }
    }

    fn transport() -> DaemonTransportSettings {
        DaemonTransportSettings::validate(
            DaemonTransportConfig::default(),
            DaemonRequestConfig::default(),
            terminal(),
            process(),
        )
        .expect("transport")
    }

    fn outbox() -> OutboxDispatchLimits {
        OutboxDispatchLimits::new(256, Duration::from_millis(100), Duration::from_millis(100))
            .expect("outbox")
    }

    #[test]
    fn defaults_validate_complete_service_boundaries() {
        let settings = DaemonServiceSettings::validate(
            DaemonServiceConfig::default(),
            terminal(),
            process(),
            outbox(),
            &transport(),
        )
        .expect("service settings");

        assert!(settings.resources().session_subscriptions() >= 300);
        assert!(settings.resources().observation_jobs() >= 4_096);
        assert_eq!(
            settings.foreground().minimum_confidence().basis_points(),
            5_000
        );
        assert_eq!(settings.regex_pattern().cached_patterns, 4_096);
        assert_eq!(settings.regex_pattern().match_text_bytes, 131_072);
        assert_eq!(settings.foreground_evidence().tracked_sessions, 2_048);
        assert_eq!(settings.foreground_evidence().profile_samples, 64);
        assert_eq!(settings.foreground_evidence().motion_rows, 500);
        assert_eq!(settings.max_terminal_columns(), 500);
        assert_eq!(settings.max_terminal_rows(), 500);
        assert_eq!(settings.max_input_items(), 128);
        assert_eq!(settings.max_input_bytes(), 5_242_880);
    }

    #[test]
    fn observation_capacity_covers_both_independent_kinds() {
        let config = DaemonServiceConfig {
            observation_jobs: 4_095,
            ..DaemonServiceConfig::default()
        };

        let error =
            DaemonServiceSettings::validate(config, terminal(), process(), outbox(), &transport())
                .expect_err("undersized observation queue must fail");

        assert_eq!(
            error.to_string(),
            "observation job capacity must cover foreground and health for every process"
        );
    }

    #[test]
    fn specialized_subscription_sources_cover_one_connection() {
        let config = DaemonServiceConfig {
            session_event_subscriptions: 1_023,
            ..DaemonServiceConfig::default()
        };

        let error =
            DaemonServiceSettings::validate(config, terminal(), process(), outbox(), &transport())
                .expect_err("undersized event subscriptions must fail");

        assert_eq!(
            error.to_string(),
            "event subscription capacity must cover one connection's logical streams"
        );
    }

    #[test]
    fn regex_input_limit_covers_profile_tests_and_live_screens() {
        let config = DaemonServiceConfig {
            regex_match_text_bytes: 131_071,
            ..DaemonServiceConfig::default()
        };

        let error =
            DaemonServiceSettings::validate(config, terminal(), process(), outbox(), &transport())
                .expect_err("undersized regex input must fail");

        assert_eq!(
            error.to_string(),
            "regex match input must contain one maximum live screen snapshot"
        );
    }

    #[test]
    fn observation_policy_contains_every_concrete_evidence_source() {
        let config = DaemonServiceConfig {
            foreground_evidence_items: RuntimeEvidenceSource::ALL.len() - 1,
            ..DaemonServiceConfig::default()
        };

        let error =
            DaemonServiceSettings::validate(config, terminal(), process(), outbox(), &transport())
                .expect_err("undersized evidence rationale must fail");

        assert_eq!(
            error.to_string(),
            "foreground decision capacity must retain every runtime evidence source"
        );
    }

    #[test]
    fn concrete_foreground_claims_must_clear_the_decision_threshold() {
        let config = DaemonServiceConfig {
            foreground_server_idle_confidence_basis_points: 4_999,
            ..DaemonServiceConfig::default()
        };

        let error =
            DaemonServiceSettings::validate(config, terminal(), process(), outbox(), &transport())
                .expect_err("unselectable configured evidence must fail");

        assert_eq!(
            error.to_string(),
            "runtime foreground confidence must meet the foreground decision minimum"
        );
    }
}
