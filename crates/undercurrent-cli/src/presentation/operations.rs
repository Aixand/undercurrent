// SPDX-License-Identifier: Apache-2.0

//! Profile, diagnostic, and daemon command human presentation.

use std::fmt::Write as _;

use undercurrent_protocol::{
    DaemonStatusDto, DiagnosticBundleDto, DiagnosticPageDto, DiagnosticSectionDto,
    DiagnosticValueDto, ProfileDetailDto, ProfileInputDeliveryDto, ProfileLintDto, ProfileMatchDto,
    ProfilePageDto, ProfileTestDto, ShutdownAcceptedDto, TerminalOutputPageDto,
};

use super::safe_text;
use crate::{
    DaemonRestartOutcome, DaemonStartOutcome, DaemonStartState, DaemonStopOutcome, DaemonStopState,
    ResetOutcome,
};

/// Renders one path-redacted local-state reset outcome.
///
/// This function performs no IO and does not panic.
pub fn reset_text(outcome: &ResetOutcome) -> String {
    format!(
        "local state reset: {} removed, {} already absent",
        outcome.state.removed.total(),
        outcome.state.already_absent.total()
    )
}

/// Renders one bounded profile page.
///
/// This function performs no IO and does not panic.
pub fn profile_page_text(page: &ProfilePageDto) -> String {
    if page.items.is_empty() {
        return "no profiles".to_owned();
    }
    let width = page
        .items
        .iter()
        .map(|profile| safe_text(&profile.name).len())
        .max()
        .unwrap_or(4)
        .max(4);
    let mut output = format!("{:<width$}  {:<10}  SOURCE\n", "NAME", "STATE");
    for profile in &page.items {
        let state = if profile.disabled {
            "disabled"
        } else {
            "enabled"
        };
        let _ = writeln!(
            output,
            "{:<width$}  {:<10}  {}",
            safe_text(&profile.name),
            state,
            safe_text(&profile.source)
        );
    }
    if let Some(cursor) = &page.next_cursor {
        let _ = write!(output, "next cursor: {}", safe_text(cursor));
    } else {
        output.pop();
    }
    output
}

/// Renders inspectable profile metadata.
///
/// This function performs no IO and does not panic.
pub fn profile_detail_text(profile: &ProfileDetailDto) -> String {
    format!(
        "profile: {}\nsource: {}\nstate: {}\nschema: {}\narchetype: {}\nmatching fallback: {}\ninput delivery: {}\nprompt rules: {}\nbackground rules: {}",
        safe_text(&profile.summary.name),
        safe_text(&profile.summary.source),
        if profile.summary.disabled {
            "disabled"
        } else {
            "enabled"
        },
        profile.schema_version,
        safe_text(&profile.archetype),
        profile.matching_fallback,
        input_delivery(&profile.input_delivery),
        profile.foreground_rule_count,
        profile.background_rule_count
    )
}

/// Renders one inspectable profile delivery policy without performing IO.
fn input_delivery(value: &ProfileInputDeliveryDto) -> String {
    match value {
        ProfileInputDeliveryDto::Immediate { text_mode } => {
            format!("immediate (text: {text_mode:?})")
        }
        ProfileInputDeliveryDto::PacedTrailingSubmit {
            delay_millis,
            text_mode,
        } => {
            format!("paced trailing submit ({delay_millis} ms, text: {text_mode:?})")
        }
    }
}

/// Renders deterministic profile matching candidates.
///
/// This function performs no IO and does not panic.
pub fn profile_match_text(result: &ProfileMatchDto) -> String {
    format!(
        "selected: {}\ncandidates: {}",
        result
            .selected
            .as_deref()
            .map(safe_text)
            .unwrap_or_else(|| "none".to_owned()),
        display_list(&result.candidates)
    )
}

/// Renders profile validation diagnostics.
///
/// This function performs no IO and does not panic.
pub fn profile_lint_text(result: &ProfileLintDto) -> String {
    if result.diagnostics.is_empty() {
        return if result.valid {
            "profiles are valid".to_owned()
        } else {
            "profiles are invalid".to_owned()
        };
    }
    let mut output = format!("valid: {}", result.valid);
    for diagnostic in &result.diagnostics {
        let field = diagnostic
            .field
            .as_deref()
            .map(|field| format!(" ({})", safe_text(field)))
            .unwrap_or_default();
        let _ = write!(
            output,
            "\n{} [{}] {}{}",
            safe_text(&diagnostic.severity),
            diagnostic
                .profile
                .as_deref()
                .map(safe_text)
                .unwrap_or_else(|| "global".to_owned()),
            safe_text(&diagnostic.message),
            field
        );
    }
    output
}

/// Renders a bounded profile sample evaluation without sample contents.
///
/// This function performs no IO and does not panic.
pub fn profile_test_text(result: &ProfileTestDto) -> String {
    let selected = result
        .selected_foreground
        .iter()
        .map(|selection| selection.rule.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "profile: {}\nevaluated rules: {}\nselected rule: {}",
        safe_text(&result.profile),
        result.evaluations.len(),
        if selected.is_empty() {
            "none"
        } else {
            &selected
        }
    )
}

/// Renders one bounded operational diagnostic page.
///
/// This function performs no IO and does not panic.
pub fn diagnostic_page_text(page: &DiagnosticPageDto) -> String {
    if page.items.is_empty() {
        return "no diagnostic rows".to_owned();
    }
    let mut output = String::new();
    for row in &page.items {
        let _ = writeln!(output, "{}", diagnostic_row_text(row));
    }
    if let Some(cursor) = &page.next_cursor {
        let _ = write!(output, "next cursor: {}", safe_text(cursor));
    } else {
        output.pop();
    }
    output
}

/// Renders one bounded diagnostic bundle without exposing terminal content
/// unless it was explicitly authorized and returned by the daemon.
///
/// This function performs no IO and does not panic.
pub fn diagnostic_bundle_text(bundle: &DiagnosticBundleDto) -> String {
    let mut output = diagnostic_page_text(&bundle.diagnostics);
    output.push('\n');
    if bundle.terminal_content_redacted {
        output.push_str("terminal content: redacted");
        return output;
    }
    let Some(terminal) = &bundle.terminal_content else {
        output.push_str("terminal content: not included");
        return output;
    };
    match &terminal.page {
        TerminalOutputPageDto::Data {
            from_offset,
            to_offset,
            next_offset,
            has_more,
            bytes,
            ..
        } => {
            let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
            for byte in bytes {
                let _ = write!(encoded, "{byte:02x}");
            }
            let _ = write!(
                output,
                "terminal content: session={} from={} to={} next={} has_more={} bytes_hex={}",
                safe_text(&terminal.session_id),
                from_offset,
                to_offset,
                next_offset,
                has_more,
                encoded
            );
        }
        TerminalOutputPageDto::Gap { .. } => {
            let _ = write!(
                output,
                "terminal content: session={} gap",
                safe_text(&terminal.session_id)
            );
        }
    }
    output
}

/// Renders one payload-safe operational diagnostic row.
///
/// This function performs no IO and does not panic.
pub fn diagnostic_row_text(row: &undercurrent_protocol::DiagnosticRowDto) -> String {
    let fields = row
        .fields
        .iter()
        .map(|(key, value)| format!("{}={}", safe_text(key), diagnostic_value_text(value)))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "{} {} {fields}",
        diagnostic_section_text(row.section),
        safe_text(&row.identity)
    )
}

/// Renders aggregate daemon identity and pressure.
///
/// This function performs no IO and does not panic.
pub fn daemon_status_text(status: &DaemonStatusDto) -> String {
    format!(
        "epoch: {}\naccepting connections: {}\nphysical connections: {}\nlogical streams: {}\nmanaged sessions: {}",
        safe_text(&status.server_epoch),
        status.accepting_connections,
        status.physical_connections,
        status.logical_streams,
        status.managed_sessions
    )
}

/// Renders one idempotent daemon-start result with bounded current status.
///
/// This function performs no IO and does not panic.
pub fn daemon_start_text(outcome: &DaemonStartOutcome) -> String {
    let state = match outcome.state {
        DaemonStartState::AlreadyRunning => "daemon already running",
        DaemonStartState::Started => "daemon started",
    };
    format!("{state}\n{}", daemon_status_text(&outcome.status))
}

/// Renders one completed or already-complete daemon stop.
///
/// This function performs no IO and does not panic.
pub fn daemon_stop_text(outcome: &DaemonStopOutcome) -> String {
    match outcome.state {
        DaemonStopState::AlreadyStopped => "daemon already stopped".to_owned(),
        DaemonStopState::Stopped => "daemon stopped".to_owned(),
    }
}

/// Renders one verified daemon replacement with bounded current status.
///
/// This function performs no IO and does not panic.
pub fn daemon_restart_text(outcome: &DaemonRestartOutcome) -> String {
    format!("daemon restarted\n{}", daemon_status_text(&outcome.status))
}

/// Renders one accepted graceful shutdown request.
///
/// This function performs no IO and does not panic.
pub fn shutdown_text(result: &ShutdownAcceptedDto) -> String {
    format!(
        "daemon shutdown accepted (deadline {})",
        result.drain_deadline_millis
    )
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

fn diagnostic_value_text(value: &DiagnosticValueDto) -> String {
    match value {
        DiagnosticValueDto::Boolean(value) => value.to_string(),
        DiagnosticValueDto::Unsigned(value) => value.to_string(),
        DiagnosticValueDto::Signed(value) => value.to_string(),
        DiagnosticValueDto::Text(value) => safe_text(value),
    }
}

const fn diagnostic_section_text(value: DiagnosticSectionDto) -> &'static str {
    match value {
        DiagnosticSectionDto::Connections => "connections",
        DiagnosticSectionDto::Streams => "streams",
        DiagnosticSectionDto::SlowConsumers => "slow_consumers",
        DiagnosticSectionDto::OutputProducers => "output_producers",
        DiagnosticSectionDto::SupervisorQueues => "supervisor_queues",
        DiagnosticSectionDto::DetectionEvidence => "detection_evidence",
        DiagnosticSectionDto::Persistence => "persistence",
        DiagnosticSectionDto::Reconciliation => "reconciliation",
        DiagnosticSectionDto::Configuration => "configuration",
    }
}
