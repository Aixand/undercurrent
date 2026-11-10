// SPDX-License-Identifier: Apache-2.0

//! Session command human presentation.

use std::fmt::Write as _;

use crate::cli_support::Verbosity;
use undercurrent_protocol::{
    CloseStandardInputResultDto, ExecutionStateDto, ForegroundClaimDto, HealthStatusDto,
    InteractionModalityDto, InteractionStateDto, LifecycleStateDto, OutputCaptureModeDto,
    RemoveSessionResultDto, RestartStatusDto, RuntimeConstraintStateDto,
    SendTerminalInputResultDto, SessionDetailDto, SessionMutationResultDto, SessionPageDto,
    SessionSummaryDto, StopSessionResultDto,
};

use crate::SessionBatchResult;

use super::safe_text;

/// Renders one successful session mutation acknowledgement.
///
/// This function performs no IO and does not panic.
pub fn session_mutation_text(result: &SessionMutationResultDto, action: &str) -> String {
    format!(
        "{} {} ({})",
        safe_text(action),
        safe_text(&result.session.name),
        safe_text(&result.session.id)
    )
}

/// Renders a deterministic bulk stop report.
///
/// This function performs no IO and does not panic.
pub fn stop_batch_text(result: &SessionBatchResult<StopSessionResultDto>) -> String {
    let mut output = String::new();
    for stopped in &result.succeeded {
        let disposition = if stopped.already_terminal {
            "already terminal"
        } else {
            "stopped"
        };
        let _ = writeln!(output, "{disposition} {}", safe_text(&stopped.session.name));
    }
    append_batch_failures(&mut output, result);
    let _ = write!(
        output,
        "selected {}, stopped {}, failed {}",
        result.selected,
        result.succeeded.len(),
        result.failures.len()
    );
    output
}

pub(crate) fn stop_picker_label(session: &SessionSummaryDto) -> String {
    let restart = match session.restart_status {
        RestartStatusDto::Scheduled { attempt, .. } => {
            format!("restart scheduled (attempt {attempt})")
        }
        RestartStatusDto::Exhausted { .. } => "restart exhausted".to_owned(),
        RestartStatusDto::Idle | RestartStatusDto::NotScheduled { .. } => {
            "no restart scheduled".to_owned()
        }
    };
    format!(
        "{} · {} · {restart}",
        safe_text(&session.name),
        lifecycle_text(session.lifecycle)
    )
}

/// Renders a deterministic bulk removal report without reconstructing names.
///
/// This function performs no IO and does not panic.
pub fn clean_batch_text(result: &SessionBatchResult<RemoveSessionResultDto>) -> String {
    let mut output = String::new();
    for removed in &result.succeeded {
        let _ = writeln!(output, "removed {}", safe_text(&removed.session_id));
    }
    append_batch_failures(&mut output, result);
    let _ = write!(
        output,
        "selected {}, removed {}, failed {}",
        result.selected,
        result.succeeded.len(),
        result.failures.len()
    );
    output
}

fn append_batch_failures<T>(output: &mut String, result: &SessionBatchResult<T>) {
    for failure in &result.failures {
        let _ = writeln!(
            output,
            "failed {} [{}]: {}",
            safe_text(&failure.session_id),
            failure.result_category.as_str(),
            safe_text(&failure.message)
        );
    }
}

/// Renders one compact session-summary page as a stable scan-friendly table.
///
/// This function performs no IO and does not panic.
pub fn session_page_text(page: &SessionPageDto, verbosity: Verbosity) -> String {
    if page.items.is_empty() {
        return "no sessions".to_owned();
    }
    let rows = page
        .items
        .iter()
        .map(|session| {
            (
                session,
                lifecycle_text(session.lifecycle),
                interaction_text(session),
                constraint_text(session),
            )
        })
        .collect::<Vec<_>>();
    let name_width = rows
        .iter()
        .map(|(session, _, _, _)| safe_text(&session.name).len())
        .max()
        .unwrap_or(4)
        .max(4);
    let lifecycle_width = rows
        .iter()
        .map(|(_, lifecycle, _, _)| lifecycle.len())
        .max()
        .unwrap_or(9)
        .max(9);
    let interaction_width = rows
        .iter()
        .map(|(_, _, interaction, _)| interaction.len())
        .max()
        .unwrap_or(11)
        .max(11);
    let constraint_width = rows
        .iter()
        .map(|(_, _, _, constraint)| constraint.len())
        .max()
        .unwrap_or(10)
        .max(10);
    let mut output = format!(
        "{:<name_width$}  {:<lifecycle_width$}  {:<9}  {:<interaction_width$}  {:<constraint_width$}  {:>7}  {:<10}  {:<6}  PROFILE\n",
        "NAME", "LIFECYCLE", "EXECUTION", "INTERACTION", "CONSTRAINT", "LINKED", "HEALTH", "ROLE"
    );
    for (session, lifecycle, interaction, constraint) in rows {
        let _ = writeln!(
            output,
            "{:<name_width$}  {:<lifecycle_width$}  {:<9}  {:<interaction_width$}  {:<constraint_width$}  {:>7}  {:<10}  {:<6}  {}",
            safe_text(&session.name),
            lifecycle,
            execution_text(session.foreground.execution.state),
            interaction,
            constraint,
            format!(
                "{}/{}",
                session.work.active_targets, session.work.total_targets
            ),
            health_text(session.health.status),
            session
                .profile_context
                .runtime_role
                .map(|_| "server")
                .unwrap_or("-"),
            effective_profile_text(session)
        );
        if verbosity >= Verbosity::Verbose {
            let _ = writeln!(
                output,
                "  id={} revision={} restarts={} labels={} base_profile={} profile_mode={} base_origin={} effective_origin={} profile_epoch={} cwd={} server_origin={}",
                safe_text(&session.id),
                session.revision,
                session.restart_count,
                labels_text(session),
                session
                    .profile_context
                    .base_profile
                    .as_deref()
                    .map(safe_text)
                    .unwrap_or_else(|| "-".to_owned()),
                profile_binding_text(session.profile_context.binding),
                session
                    .profile_context
                    .base_origin
                    .map(base_profile_origin_text)
                    .unwrap_or("-"),
                effective_profile_origin_text(session.profile_context.effective_origin),
                session.profile_context.epoch,
                session
                    .profile_context
                    .current_directory
                    .as_deref()
                    .map(safe_text)
                    .unwrap_or_else(|| "-".to_owned()),
                session
                    .profile_context
                    .server_role_origin
                    .map(server_role_origin_text)
                    .unwrap_or("-"),
            );
        }
    }
    if let Some(cursor) = &page.next_cursor {
        let _ = write!(output, "next cursor: {}", safe_text(cursor));
    } else if output.ends_with('\n') {
        output.pop();
    }
    output
}

const fn server_role_origin_text(
    value: undercurrent_protocol::ServerRoleOriginDto,
) -> &'static str {
    match value {
        undercurrent_protocol::ServerRoleOriginDto::Profile => "profile",
        undercurrent_protocol::ServerRoleOriginDto::TcpListener => "listener",
        undercurrent_protocol::ServerRoleOriginDto::ProfileAndTcpListener => "profile+listener",
        undercurrent_protocol::ServerRoleOriginDto::ManagedProcessTcpListener => "process listener",
        undercurrent_protocol::ServerRoleOriginDto::ProfileAndManagedProcessTcpListener => {
            "profile+process listener"
        }
        undercurrent_protocol::ServerRoleOriginDto::ManagedTreeTcpListener => "tree listener",
        undercurrent_protocol::ServerRoleOriginDto::ProfileAndManagedTreeTcpListener => {
            "profile+tree listener"
        }
    }
}

const fn base_profile_origin_text(
    value: undercurrent_protocol::ProfileSelectionOriginDto,
) -> &'static str {
    match value {
        undercurrent_protocol::ProfileSelectionOriginDto::Explicit => "explicit",
        undercurrent_protocol::ProfileSelectionOriginDto::LaunchCommand => "launch",
    }
}

fn effective_profile_text(session: &SessionSummaryDto) -> String {
    if let Some(profile) = session.profile.as_deref() {
        return safe_text(profile);
    }
    if session.profile_context.binding
        == undercurrent_protocol::ProfileBindingModeDto::ForegroundAdaptive
    {
        let reason = session
            .profile_context
            .unclassified_reason
            .map(unclassified_profile_reason_text)
            .unwrap_or("unknown");
        return format!("unclassified:{reason}");
    }
    "-".to_owned()
}

const fn profile_binding_text(value: undercurrent_protocol::ProfileBindingModeDto) -> &'static str {
    match value {
        undercurrent_protocol::ProfileBindingModeDto::Fixed => "fixed",
        undercurrent_protocol::ProfileBindingModeDto::ForegroundAdaptive => "foreground",
    }
}

const fn effective_profile_origin_text(
    value: undercurrent_protocol::EffectiveProfileOriginDto,
) -> &'static str {
    match value {
        undercurrent_protocol::EffectiveProfileOriginDto::BaseProfile => "base",
        undercurrent_protocol::EffectiveProfileOriginDto::ForegroundProcess => "process",
        undercurrent_protocol::EffectiveProfileOriginDto::CooperativeShell => "shell",
    }
}

const fn unclassified_profile_reason_text(
    value: undercurrent_protocol::UnclassifiedProfileReasonDto,
) -> &'static str {
    match value {
        undercurrent_protocol::UnclassifiedProfileReasonDto::AwaitingObservation => "pending",
        undercurrent_protocol::UnclassifiedProfileReasonDto::NoMatch => "no-match",
        undercurrent_protocol::UnclassifiedProfileReasonDto::Ambiguous => "ambiguous",
        undercurrent_protocol::UnclassifiedProfileReasonDto::IncompleteEvidence => "incomplete",
        undercurrent_protocol::UnclassifiedProfileReasonDto::Unavailable => "unavailable",
    }
}

/// Renders one compact session summary.
///
/// This function performs no IO and does not panic.
pub fn session_summary_text(summary: &SessionSummaryDto, verbosity: Verbosity) -> String {
    let page = SessionPageDto {
        items: vec![summary.clone()],
        next_cursor: None,
        snapshot_revision: summary.revision,
        total: Some(1),
    };
    session_page_text(&page, verbosity)
}

/// Renders projected session detail without exposing absent sections.
///
/// This function performs no IO and does not panic.
pub fn session_detail_text(detail: &SessionDetailDto, verbosity: Verbosity) -> String {
    let mut output = session_summary_text(&detail.summary, verbosity);
    let context = &detail.summary.profile_context;
    if !context.matching_evidence.is_empty() {
        let _ = write!(
            output,
            "\nprofile selection: {}",
            effective_profile_text(&detail.summary)
        );
        for candidate in &context.matching_evidence {
            let status = if candidate.suppressed {
                "fallback suppressed by application match"
            } else if candidate.matching_fallback {
                "fallback candidate"
            } else {
                "application candidate"
            };
            let _ = write!(
                output,
                "\n  {} [{}]; processes: {}",
                safe_text(&candidate.profile),
                status,
                display_list(&candidate.process_incarnations)
            );
        }
    }
    let _ = write!(output, "\npersisted: {}", detail.persisted);
    if let Some(process) = &detail.process {
        let _ = write!(
            output,
            "\nprocess: {} (started {})",
            safe_text(&process.instance_id),
            process.started_at_millis
        );
    }
    match &detail.shell_environment {
        Some(snapshot) => {
            let _ = write!(
                output,
                "\nlive environment: available (root shell {}, captured at {} ms)",
                snapshot.process_id, snapshot.observed_at_millis
            );
        }
        None => output.push_str("\nlive environment: unavailable (use --env-launch)"),
    }
    let _ = write!(
        output,
        "\nworking directory: {}",
        detail
            .summary
            .profile_context
            .current_directory
            .as_deref()
            .map(safe_text)
            .unwrap_or_else(|| "-".to_owned())
    );
    if let Some(directory) = &detail.restoration_directory {
        let _ = write!(
            output,
            "\nlast observed directory: {}",
            safe_text(directory)
        );
    }
    if let Some(configuration) = &detail.configuration {
        let mut command = safe_text(&configuration.command.program);
        for argument in &configuration.command.arguments {
            command.push(' ');
            command.push_str(&safe_text(argument));
        }
        let _ = write!(output, "\ncommand: {command}");
        if let Some(receipt) = &configuration.executable_resolution {
            let mode = match receipt.launch_mode {
                undercurrent_protocol::LaunchModeDto::Native => "native",
                undercurrent_protocol::LaunchModeDto::Sealed => "sealed",
            };
            let _ = write!(output, "\nlaunch mode: {mode}");
        }
        let _ = write!(
            output,
            "\nlaunch directory: {}",
            configuration
                .working_directory
                .as_deref()
                .map(safe_text)
                .unwrap_or_else(|| "-".to_owned())
        );
        let _ = write!(
            output,
            "\nenvironment names: {}",
            display_list(&configuration.environment_variable_names)
        );
        let _ = write!(output, "\nlabels: {}", display_map(&configuration.labels));
        let _ = write!(
            output,
            "\nmetadata: {}",
            display_map(&configuration.metadata)
        );
        let _ = write!(
            output,
            "\noutput capture: {}",
            output_capture_mode_text(configuration.output_capture_mode)
        );
    }
    if let Some(work) = &detail.work_context {
        let _ = write!(
            output,
            "\nwork context: {} items at revision {}",
            work.items.len(),
            work.revision
        );
    }
    if let Some(evidence) = &detail.foreground_evidence {
        let _ = write!(output, "\nforeground evidence: {} records", evidence.len());
        for item in evidence {
            let _ = write!(
                output,
                "\n  {} [{}]: {} ({} bp, {})",
                safe_text(&item.source),
                foreground_axis_text(item.axis),
                item.claim
                    .as_ref()
                    .map(foreground_claim_text)
                    .unwrap_or_else(|| "unavailable".to_owned()),
                item.confidence_basis_points,
                safe_text(&item.assessment)
            );
        }
    }
    if let Some(results) = &detail.health_results {
        let passed = results.iter().filter(|result| result.passed).count();
        let _ = write!(
            output,
            "\nhealth results: {passed}/{} passed",
            results.len()
        );
    }
    output
}

/// Renders one terminal-input delivery acknowledgement without echoing input.
///
/// This function performs no IO and does not panic.
pub fn terminal_input_text(result: &SendTerminalInputResultDto) -> String {
    let disposition = if result.duplicate {
        "already delivered"
    } else {
        "delivered"
    };
    format!("input {disposition} to {}", safe_text(&result.session_id))
}

/// Renders standard-input closure without implying that EOF was a byte write.
pub fn standard_input_closed_text(result: &CloseStandardInputResultDto) -> String {
    let disposition = if result.already_closed {
        "was already closed for"
    } else {
        "closed for"
    };
    format!(
        "standard input {disposition} {}",
        safe_text(&result.session_id)
    )
}

pub(crate) fn lifecycle_text(value: LifecycleStateDto) -> String {
    match value {
        LifecycleStateDto::Starting => "starting".to_owned(),
        LifecycleStateDto::Running => "running".to_owned(),
        LifecycleStateDto::Stopping => "stopping".to_owned(),
        LifecycleStateDto::Stopped => "stopped".to_owned(),
        LifecycleStateDto::Exited { code: Some(code) } => format!("exited({code})"),
        LifecycleStateDto::Exited { code: None } => "exited".to_owned(),
        LifecycleStateDto::Failed => "failed".to_owned(),
    }
}

pub(crate) const fn execution_text(value: ExecutionStateDto) -> &'static str {
    match value {
        ExecutionStateDto::Active => "active",
        ExecutionStateDto::Quiescent => "quiescent",
        ExecutionStateDto::Unknown => "unknown",
    }
}

pub(crate) fn interaction_text(session: &SessionSummaryDto) -> String {
    let interaction = &session.foreground.interaction;
    let state = match interaction.state {
        InteractionStateDto::None => "none",
        InteractionStateDto::Available => "available",
        InteractionStateDto::Requested => "requested",
    };
    let detail = interaction.semantic.as_deref().map(safe_text).or_else(|| {
        interaction
            .modality
            .map(interaction_modality_text)
            .map(str::to_owned)
    });
    detail.map_or_else(|| state.to_owned(), |detail| format!("{state}:{detail}"))
}

pub(crate) fn constraint_text(session: &SessionSummaryDto) -> String {
    match session.foreground.constraint.state {
        RuntimeConstraintStateDto::Clear => "clear".to_owned(),
        RuntimeConstraintStateDto::Present => session
            .foreground
            .constraint
            .semantic
            .as_deref()
            .map(safe_text)
            .unwrap_or_else(|| "present".to_owned()),
    }
}

const fn interaction_modality_text(value: InteractionModalityDto) -> &'static str {
    match value {
        InteractionModalityDto::Text => "text",
        InteractionModalityDto::Options => "options",
        InteractionModalityDto::Secret => "secret",
        InteractionModalityDto::Navigation => "navigation",
    }
}

const fn foreground_axis_text(value: undercurrent_protocol::ForegroundAxisDto) -> &'static str {
    match value {
        undercurrent_protocol::ForegroundAxisDto::Execution => "execution",
        undercurrent_protocol::ForegroundAxisDto::Interaction => "interaction",
        undercurrent_protocol::ForegroundAxisDto::Constraint => "constraint",
    }
}

fn foreground_claim_text(value: &ForegroundClaimDto) -> String {
    match value {
        ForegroundClaimDto::Execution { state } => execution_text(*state).to_owned(),
        ForegroundClaimDto::Interaction {
            state,
            modality,
            semantic,
        } => {
            let state = match state {
                InteractionStateDto::None => "none",
                InteractionStateDto::Available => "available",
                InteractionStateDto::Requested => "requested",
            };
            semantic
                .as_deref()
                .map(safe_text)
                .or_else(|| modality.map(interaction_modality_text).map(str::to_owned))
                .map_or_else(|| state.to_owned(), |detail| format!("{state}:{detail}"))
        }
        ForegroundClaimDto::Constraint { state, semantic } => match state {
            RuntimeConstraintStateDto::Clear => "clear".to_owned(),
            RuntimeConstraintStateDto::Present => semantic
                .as_deref()
                .map(safe_text)
                .unwrap_or_else(|| "present".to_owned()),
        },
    }
}

pub(crate) const fn health_text(value: HealthStatusDto) -> &'static str {
    match value {
        HealthStatusDto::Healthy => "healthy",
        HealthStatusDto::Unhealthy => "unhealthy",
        HealthStatusDto::Unknown => "unknown",
        HealthStatusDto::NoCheck => "no-check",
    }
}

const fn output_capture_mode_text(value: OutputCaptureModeDto) -> &'static str {
    match value {
        OutputCaptureModeDto::Terminal => "terminal",
        OutputCaptureModeDto::Opaque => "opaque",
    }
}

fn labels_text(session: &SessionSummaryDto) -> String {
    session
        .labels
        .as_ref()
        .map(display_map)
        .unwrap_or_else(|| "not projected".to_owned())
}

fn display_map(values: &std::collections::BTreeMap<String, String>) -> String {
    if values.is_empty() {
        return "-".to_owned();
    }
    values
        .iter()
        .map(|(key, value)| format!("{}={}", safe_text(key), safe_text(value)))
        .collect::<Vec<_>>()
        .join(",")
}

fn display_list(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_owned()
    } else {
        values
            .iter()
            .map(|value| safe_text(value))
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use undercurrent_protocol::{HealthSummaryDto, WorkSummaryDto};

    use super::*;

    #[test]
    fn session_pages_render_stable_operational_columns_and_exact_exit_codes() {
        let mut session = summary();
        session.lifecycle = LifecycleStateDto::Exited { code: Some(137) };
        let page = SessionPageDto {
            items: vec![session],
            next_cursor: Some("opaque".to_owned()),
            snapshot_revision: 1,
            total: None,
        };

        let text = session_page_text(&page, Verbosity::Verbose);

        assert!(text.contains("NAME"));
        assert!(text.contains("worker"));
        assert!(text.contains("exited(137)"));
        assert!(text.contains("id=session-1"));
        assert!(text.contains("next cursor: opaque"));
    }

    #[test]
    fn presentation_escapes_control_characters_in_query_visible_values() {
        let mut session = summary();
        session.name = "worker\u{1b}[2J".to_owned();
        session.labels = Some(BTreeMap::from([(
            "environment".to_owned(),
            "prod\nforged".to_owned(),
        )]));

        let text = session_summary_text(&session, Verbosity::Verbose);

        assert!(!text.contains('\u{1b}'));
        assert!(!text.contains("prod\nforged"));
        assert!(text.contains("\\u{1b}"));
        assert!(text.contains("prod\\nforged"));
    }

    #[test]
    fn session_columns_preserve_execution_independently_of_liveness_and_interaction() {
        let mut session = summary();
        for (execution, interaction, expected) in [
            (
                ExecutionStateDto::Unknown,
                InteractionStateDto::None,
                "unknown",
            ),
            (
                ExecutionStateDto::Unknown,
                InteractionStateDto::Available,
                "unknown",
            ),
            (
                ExecutionStateDto::Unknown,
                InteractionStateDto::Requested,
                "unknown",
            ),
            (
                ExecutionStateDto::Active,
                InteractionStateDto::None,
                "active",
            ),
            (
                ExecutionStateDto::Quiescent,
                InteractionStateDto::Available,
                "quiescent",
            ),
            (
                ExecutionStateDto::Active,
                InteractionStateDto::Requested,
                "active",
            ),
        ] {
            session.foreground = crate::test_foreground(execution, interaction);
            let text = session_summary_text(&session, Verbosity::Normal);
            let mut lines = text.lines();
            let columns = lines.next().unwrap().split_whitespace().collect::<Vec<_>>();
            assert_eq!(
                &columns[..4],
                &["NAME", "LIFECYCLE", "EXECUTION", "INTERACTION"]
            );
            let values = lines.next().unwrap().split_whitespace().collect::<Vec<_>>();
            assert_eq!(values[1], "running", "{text}");
            assert_eq!(values[2], expected, "{text}");
            if interaction == InteractionStateDto::Requested {
                assert!(text.contains("requested:"), "{text}");
            }
        }
    }

    #[test]
    fn nonrunning_lifecycle_and_retained_execution_remain_visible_separately() {
        let mut session = summary();
        session.foreground =
            crate::test_foreground(ExecutionStateDto::Active, InteractionStateDto::Requested);
        for (lifecycle, expected) in [
            (LifecycleStateDto::Starting, "starting"),
            (LifecycleStateDto::Stopping, "stopping"),
            (LifecycleStateDto::Stopped, "stopped"),
            (LifecycleStateDto::Exited { code: Some(137) }, "exited(137)"),
            (LifecycleStateDto::Exited { code: None }, "exited"),
            (LifecycleStateDto::Failed, "failed"),
        ] {
            session.lifecycle = lifecycle;
            let text = session_summary_text(&session, Verbosity::Normal);
            let values = text
                .lines()
                .nth(1)
                .unwrap()
                .split_whitespace()
                .collect::<Vec<_>>();
            assert_eq!(values[1], expected, "{text}");
            assert_eq!(values[2], "active", "{text}");
        }
    }

    #[test]
    fn separate_lifecycle_and_execution_keep_table_columns_aligned() {
        let mut unknown = summary();
        unknown.foreground =
            crate::test_foreground(ExecutionStateDto::Unknown, InteractionStateDto::None);
        let mut active = unknown.clone();
        active.lifecycle = LifecycleStateDto::Exited { code: Some(137) };
        active.foreground.execution.state = ExecutionStateDto::Active;
        let text = session_page_text(
            &SessionPageDto {
                items: vec![unknown, active],
                next_cursor: None,
                snapshot_revision: 1,
                total: Some(2),
            },
            Verbosity::Normal,
        );
        let mut lines = text.lines();
        let header = lines.next().unwrap();
        let interaction_column = header.find("INTERACTION").unwrap();
        for line in lines {
            let prefix = &line[..line.find("none").unwrap()];
            assert_eq!(prefix.chars().count(), interaction_column, "{text}");
        }
    }

    #[test]
    fn configured_output_capture_modes_have_stable_text() {
        assert_eq!(
            output_capture_mode_text(OutputCaptureModeDto::Terminal),
            "terminal"
        );
        assert_eq!(
            output_capture_mode_text(OutputCaptureModeDto::Opaque),
            "opaque"
        );
    }

    #[test]
    fn detail_renders_live_directory_without_configuration() {
        let mut summary = summary();
        summary.profile_context.current_directory = Some("/live/project".to_owned());
        let mut detail = SessionDetailDto {
            shell_environment: None,
            restoration_directory: None,
            summary,
            configuration: None,
            work_context: None,
            process: None,
            foreground_evidence: None,
            health_results: None,
            persisted: true,
        };
        let text = session_detail_text(&detail, Verbosity::Normal);
        assert!(text.contains("\nworking directory: /live/project"));
        detail.summary.profile_context.current_directory = None;
        let text = session_detail_text(&detail, Verbosity::Normal);
        assert!(text.contains("\nworking directory: -"));
    }

    fn summary() -> SessionSummaryDto {
        SessionSummaryDto {
            id: "session-1".to_owned(),
            name: "worker".to_owned(),
            lifecycle: LifecycleStateDto::Running,
            foreground: crate::test_foreground(
                undercurrent_protocol::ExecutionStateDto::Quiescent,
                undercurrent_protocol::InteractionStateDto::None,
            ),
            health: HealthSummaryDto {
                status: HealthStatusDto::NoCheck,
                observed_at_millis: None,
                configured_checks: 0,
            },
            work: WorkSummaryDto {
                revision: 0,
                total_items: 0,
                active_items: 0,
                total_targets: 0,
                active_targets: 0,
                aggregate_status: None,
            },
            profile: None,
            profile_context: Default::default(),
            labels: Some(BTreeMap::new()),
            started_at_millis: Some(1),
            updated_at_millis: 1,
            restart_count: 0,
            restart_status: undercurrent_protocol::RestartStatusDto::Idle,
            revision: 1,
            lifecycle_revision: 1,
        }
    }
}
