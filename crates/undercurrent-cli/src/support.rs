// SPDX-License-Identifier: Apache-2.0

//! Bounded local support export with independent, optional live reads.

use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Cursor, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::{Value, json};
use undercurrent_diagnostics::{FailureRecord, MAX_RECORD_BYTES, read_bounded};
use undercurrent_protocol::{
    DiagnosticPageDto, DiagnosticSectionDto, ProductRequest, ProductSuccess,
};
use undercurrent_sdk::ProductClient;

use crate::{
    CliError, CliInputError, CliInvocationContext, NativeProductClientFactory,
    SupportCollectArguments,
};

const ARCHIVE_BYTES: usize = 16 * 1024 * 1024;
const LOG_BYTES: usize = 1024 * 1024;
const MAX_PAGES: usize = 64;

#[derive(Serialize)]
pub(crate) struct CollectionReport {
    pub partial: bool,
    pub entries: usize,
    pub terminal_content_included: bool,
}

#[derive(Serialize)]
struct SourceReport {
    source: String,
    status: &'static str,
    reason: Option<String>,
}

struct Collection {
    files: Vec<(String, Vec<u8>)>,
    sources: Vec<SourceReport>,
    bytes: usize,
    partial: bool,
}

impl Collection {
    fn new() -> Self {
        Self {
            files: Vec::new(),
            sources: Vec::new(),
            bytes: 0,
            partial: false,
        }
    }

    fn note(&mut self, source: &str, status: &'static str, reason: impl Into<Option<String>>) {
        self.partial |= matches!(status, "unavailable" | "truncated");
        self.sources.push(SourceReport {
            source: source.into(),
            status,
            reason: reason.into(),
        });
    }

    fn add(&mut self, name: &str, bytes: Vec<u8>) -> bool {
        // Leave room for the manifest and ZIP directory. All entry names are
        // generated here, never copied from profile names or filesystem paths.
        if self.bytes.saturating_add(bytes.len()) > ARCHIVE_BYTES - 128 * 1024 {
            self.note(name, "truncated", Some("archive_byte_limit".into()));
            return false;
        }
        self.bytes += bytes.len();
        self.files.push((name.into(), bytes));
        true
    }

    fn json(&mut self, name: &str, value: &impl Serialize) -> bool {
        match serde_json::to_vec_pretty(value) {
            Ok(bytes) => self.add(name, bytes),
            Err(_) => {
                self.note(name, "unavailable", Some("serialization".into()));
                false
            }
        }
    }

    fn local(&mut self, home: &Path, arguments: &SupportCollectArguments) {
        for component in ["daemon", "session-host"] {
            let name = format!("build-{component}.json");
            match read_bounded(&home.join("logs").join(&name), MAX_RECORD_BYTES)
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).map_err(io::Error::other))
            {
                Ok(value) => {
                    self.json(&name, &sanitize_log(&value));
                    self.note(&name, "collected", None);
                }
                Err(error) => self.note(&name, "unavailable", Some(format!("{:?}", error.kind()))),
            }
        }
        let directory = home.join("logs/incidents");
        let mut backtraces = 0;
        match fs::read_dir(&directory) {
            Ok(entries) => {
                let mut paths = entries
                    .take(1025)
                    .filter_map(Result::ok)
                    .map(|e| e.path())
                    .collect::<Vec<_>>();
                if paths.len() > 1024 {
                    self.note(
                        "incidents",
                        "truncated",
                        Some("directory_entry_limit".into()),
                    );
                }
                paths.sort();
                let mut records = Vec::new();
                for path in paths.into_iter().rev().take(1024) {
                    if path.extension().is_some_and(|s| s == "json") {
                        let record = read_bounded(&path, MAX_RECORD_BYTES)
                            .ok()
                            .and_then(|bytes| serde_json::from_slice::<FailureRecord>(&bytes).ok())
                            .filter(FailureRecord::is_safe);
                        match record {
                            Some(record)
                                if arguments
                                    .incident
                                    .as_ref()
                                    .is_none_or(|id| id == &record.incident_id) =>
                            {
                                if records.len() == undercurrent_diagnostics::RETAINED_RECORDS {
                                    self.note(
                                        "incidents",
                                        "truncated",
                                        Some("record_limit".into()),
                                    );
                                    break;
                                }
                                records.push(record);
                            }
                            Some(_) => {}
                            None => self.note(
                                "incidents",
                                "unavailable",
                                Some("invalid_or_unreadable_record".into()),
                            ),
                        }
                    } else if arguments.include_backtraces
                        && backtraces < undercurrent_diagnostics::RETAINED_BACKTRACES
                        && path.extension().is_some_and(|s| s == "backtrace")
                        && arguments.incident.as_ref().is_none_or(|id| {
                            path.file_name()
                                .is_some_and(|n| n.to_string_lossy().contains(id))
                        })
                    {
                        match read_bounded(&path, undercurrent_diagnostics::MAX_BACKTRACE_BYTES) {
                            Ok(bytes) => {
                                if self.add(&format!("backtraces/{backtraces}.txt"), bytes) {
                                    backtraces += 1;
                                }
                            }
                            Err(error) => self.note(
                                "backtraces",
                                "unavailable",
                                Some(format!("{:?}", error.kind())),
                            ),
                        }
                    }
                }
                if arguments.incident.is_some() && records.is_empty() {
                    self.note(
                        "incidents",
                        "unavailable",
                        Some("reference_not_retained".into()),
                    );
                } else {
                    self.note("incidents", "collected", None);
                }
                self.json("incidents.json", &records);
            }
            Err(error) => self.note(
                "incidents",
                "unavailable",
                Some(format!("{:?}", error.kind())),
            ),
        }
        if !arguments.include_backtraces {
            self.note("backtraces", "redacted", None);
        } else if backtraces > 0 {
            self.note("backtraces", "collected", None);
        } else {
            self.note(
                "backtraces",
                "unavailable",
                Some("no_retained_backtraces".into()),
            );
        }
        match read_bounded(&home.join("logs/profile-reload.json"), MAX_RECORD_BYTES)
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).map_err(io::Error::other))
        {
            Ok(mut value) => {
                // Only stable status fields are exported; private parser text
                // and filesystem names are never part of this projection.
                if let Some(object) = value.as_object_mut() {
                    object.retain(|key, _| {
                        matches!(
                            key.as_str(),
                            "observed_at_millis"
                                | "active_generation"
                                | "previous_definition_active"
                                | "stage"
                                | "status"
                                | "last_rejection_at_millis"
                        )
                    });
                }
                self.json("profile-reload.json", &sanitize_log(&value));
                self.note("profile_reload", "collected", None);
            }
            Err(error) => self.note(
                "profile_reload",
                "unavailable",
                Some(format!("{:?}", error.kind())),
            ),
        }
        self.logs(home, arguments.incident.as_deref());
    }

    fn logs(&mut self, home: &Path, incident: Option<&str>) {
        let mut rows = Vec::new();
        let mut found = false;
        // Rotation retention is configuration-dependent; inspect a fixed upper
        // bound even if the configuration cannot be decoded.
        for index in 0..8 {
            let name = if index == 0 {
                "daemon.jsonl".into()
            } else {
                format!("daemon.jsonl.{index}")
            };
            let path = home.join("logs").join(&name);
            match read_log_tail(&path) {
                Ok((bytes, truncated)) => {
                    found = true;
                    let mut invalid = false;
                    for line in bytes.split(|b| *b == b'\n').filter(|line| !line.is_empty()) {
                        match serde_json::from_slice::<Value>(line) {
                            Ok(value) => {
                                let correlation = value
                                    .pointer("/span/correlation_id")
                                    .and_then(Value::as_str);
                                if incident.is_some_and(|id| correlation.is_some_and(|c| c != id)) {
                                    continue;
                                }
                                if rows.len() == 8192 {
                                    self.note(
                                        "daemon_logs",
                                        "truncated",
                                        Some("log_row_limit".into()),
                                    );
                                    self.json("daemon-log.json", &rows);
                                    return;
                                }
                                rows.push(sanitize_log(&value));
                            }
                            Err(_) => invalid = true,
                        }
                    }
                    self.note(
                        &name,
                        if truncated || invalid {
                            "truncated"
                        } else {
                            "collected"
                        },
                        (truncated || invalid).then(|| "bounded_tail_or_incomplete_lines".into()),
                    );
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => self.note(&name, "unavailable", Some(format!("{:?}", error.kind()))),
            }
        }
        if !found {
            self.note(
                "daemon_logs",
                "unavailable",
                Some("no_readable_logs".into()),
            );
        }
        if home.join("logs/daemon.jsonl.8").symlink_metadata().is_ok() {
            self.note("daemon_logs", "truncated", Some("log_file_limit".into()));
        }
        self.json("daemon-log.json", &rows);
    }

    fn live(
        &mut self,
        client: &ProductClient,
        arguments: &SupportCollectArguments,
        deadline: Instant,
        page_size: u32,
    ) -> bool {
        match request(client, ProductRequest::GetDaemonStatus, deadline) {
            Ok(ProductSuccess::GetDaemonStatus(status)) => {
                self.json("daemon-status.json", &status);
                self.note("daemon_status", "collected", None);
            }
            result => self.note(
                "daemon_status",
                "unavailable",
                Some(result_category(result)),
            ),
        }
        for section in DiagnosticSectionDto::ALL {
            let source = format!("diagnostics/{section:?}.json");
            let pages = collect_pages(|cursor| {
                match request(
                    client,
                    ProductRequest::GetDiagnostics(undercurrent_protocol::GetDiagnosticsRequest {
                        sections: vec![section],
                        page_size,
                        cursor,
                    }),
                    deadline,
                )? {
                    ProductSuccess::GetDiagnostics(page) => Ok(page),
                    _ => Err("response_mismatch".into()),
                }
            });
            self.json(&source, &pages.items);
            self.note(
                &source,
                if pages.complete {
                    "collected"
                } else {
                    "truncated"
                },
                pages.reason,
            );
        }
        if let Some(session) = &arguments.session {
            let query = undercurrent_protocol::GetSessionDetailRequest {
                session_id: session.clone(),
                projection: undercurrent_protocol::SessionDetailProjectionDto {
                    process: true,
                    foreground_evidence: true,
                    health_results: true,
                    ..Default::default()
                },
            };
            match request(client, ProductRequest::GetSessionDetail(query), deadline) {
                Ok(ProductSuccess::GetSessionDetail(detail)) => {
                    // Project known typed observations. Raw detail can contain
                    // restoration paths, user label keys and profile semantics.
                    let summary = &detail.summary;
                    let value = json!({ "session_id": summary.id,
                        "lifecycle": summary.lifecycle, "execution": summary.foreground.execution.state,
                        "interaction": summary.foreground.interaction.state, "constraint": summary.foreground.constraint.state,
                        "health": summary.health, "restart_status": summary.restart_status,
                        "revision": summary.revision, "lifecycle_revision": summary.lifecycle_revision,
                        "restart_count": summary.restart_count, "updated_at_millis": summary.updated_at_millis,
                        "profile_binding": summary.profile_context.binding, "profile_origin": summary.profile_context.effective_origin,
                        "profile_epoch": summary.profile_context.epoch, "unclassified_reason": summary.profile_context.unclassified_reason,
                        "process": detail.process, "persisted": detail.persisted });
                    self.json("session.json", &value);
                    self.note("session", "collected", None);
                }
                result => self.note("session", "unavailable", Some(result_category(result))),
            }
        }
        if let Some(profile) = &arguments.profile {
            match request(
                client,
                ProductRequest::GetProfile(undercurrent_protocol::GetProfileRequest {
                    name: profile.clone(),
                }),
                deadline,
            ) {
                Ok(ProductSuccess::GetProfile(detail)) => {
                    let value = json!({ "disabled": detail.summary.disabled, "schema_version": detail.schema_version,
                        "matching_fallback": detail.matching_fallback, "input_delivery": detail.input_delivery,
                        "foreground_rule_count": detail.foreground_rule_count, "background_rule_count": detail.background_rule_count,
                        "binary_pattern_count": detail.binary_patterns.len(), "argument_pattern_count": detail.argument_patterns.len(),
                        "required_environment_count": detail.required_environment.len() });
                    self.json("profile.json", &value);
                    self.note("profile", "collected", None);
                }
                result => self.note("profile", "unavailable", Some(result_category(result))),
            }
        }
        if let Some(session) = &arguments.include_terminal_content {
            let query = undercurrent_protocol::CreateDiagnosticBundleRequest {
                sections: vec![DiagnosticSectionDto::Configuration],
                page_size: 1,
                cursor: None,
                terminal_content: Some(
                    undercurrent_protocol::DiagnosticBundleTerminalContentRequestDto {
                        session_id: session.clone(),
                        from_offset: 0,
                        max_bytes: 4096,
                    },
                ),
            };
            match request(
                client,
                ProductRequest::CreateDiagnosticBundle(query),
                deadline,
            ) {
                Ok(ProductSuccess::CreateDiagnosticBundle(bundle))
                    if !bundle.terminal_content_redacted && bundle.terminal_content.is_some() =>
                {
                    let included = self.json("terminal-content.json", &bundle.terminal_content);
                    self.note(
                        "terminal_content",
                        if included { "collected" } else { "truncated" },
                        None,
                    );
                    return included;
                }
                result => self.note(
                    "terminal_content",
                    "unavailable",
                    Some(result_category(result)),
                ),
            }
        }
        false
    }
}

pub(crate) fn collect(
    arguments: &SupportCollectArguments,
    context: &CliInvocationContext,
) -> Result<CollectionReport, CliError> {
    if arguments
        .incident
        .as_ref()
        .is_some_and(|id| !undercurrent_diagnostics::valid_incident(id))
    {
        return Err(CliInputError::InvalidArguments.into());
    }
    let mut collection = Collection::new();
    let deadline = Instant::now() + context.common().timeout.get().min(Duration::from_secs(60));
    let mut terminal_content_included = false;
    let mut live_attempted = false;
    collection.json("build.json", &json!({
        "cli_version": env!("CARGO_PKG_VERSION"), "build_revision": undercurrent_diagnostics::REVISION,
        "os": std::env::consts::OS, "arch": std::env::consts::ARCH,
    }));
    let mut path_environment = undercurrent_config::PathEnvironment::from_process_env();
    if let Some(selection) = &context.selection {
        selection.apply_paths(&mut path_environment);
    }
    if context.selection_failed {
        collection.note("home", "unavailable", Some("invalid_cli_selection".into()));
    } else {
        match undercurrent_config::UndercurrentPaths::resolve(&path_environment) {
            Ok(paths) => collection.local(paths.home(), arguments),
            Err(_) => collection.note("home", "unavailable", Some("invalid_home".into())),
        }
    }
    let factory = NativeProductClientFactory::from_context(context);
    match (!context.selection_failed).then(|| factory.resolve()) {
        Some(Ok(config)) => {
            let mut value = serde_json::to_value(config.redacted_report()).unwrap_or(Value::Null);
            redact_strings(&mut value);
            collection.json("configuration.json", &value);
            collection.note("configuration", "collected", None);
            if !arguments.offline {
                match factory.connect_existing_resolved_with_timeout(
                    &config,
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_secs(3)),
                ) {
                    Ok(Some(client)) => {
                        live_attempted = true;
                        terminal_content_included = collection.live(
                            &client,
                            arguments,
                            deadline,
                            config.document().requests.diagnostic_page_size.min(100) as u32,
                        );
                        let events = client.telemetry_diagnostics().map(|events| {
                            events
                                .into_iter()
                                .map(|event| event.as_str())
                                .collect::<Vec<_>>()
                        });
                        let pressure = client.telemetry_pressure().map(|p| json!({ "retained_diagnostics": p.retained_diagnostics, "diagnostic_limit": p.diagnostic_limit, "dropped_diagnostics": p.dropped_diagnostics, "metric_series": p.metric_series, "dropped_metric_updates": p.dropped_metric_updates }));
                        collection.json("collection-client-telemetry.json", &json!({ "scope": "collection_client_only", "events": events, "pressure": pressure }));
                    }
                    Ok(None) => {
                        collection.note("daemon", "unavailable", Some("not_running".into()))
                    }
                    Err(error) => collection.note(
                        "daemon",
                        "unavailable",
                        Some(error.exit_code().as_str().into()),
                    ),
                }
            } else {
                collection.note("daemon", "skipped", Some("offline_requested".into()));
            }
        }
        Some(Err(error)) => {
            collection.json("configuration-error.json", &json!({ "category": error.exit_code().as_str(), "io_causes": undercurrent_diagnostics::io_causes(&error) }));
            collection.note(
                "configuration",
                "unavailable",
                Some("invalid_configuration".into()),
            );
            collection.note(
                "daemon",
                "skipped",
                Some("configuration_unavailable".into()),
            );
        }
        None => {
            collection.note(
                "configuration",
                "unavailable",
                Some("invalid_cli_selection".into()),
            );
            collection.note(
                "daemon",
                "skipped",
                Some("configuration_unavailable".into()),
            );
        }
    }
    if !live_attempted {
        for (source, requested) in [
            ("session", arguments.session.is_some()),
            ("profile", arguments.profile.is_some()),
        ] {
            if requested {
                collection.note(
                    source,
                    "unavailable",
                    Some("live_source_unavailable".into()),
                );
            }
        }
    }
    if arguments.include_terminal_content.is_none() {
        collection.note("terminal_content", "redacted", None);
    } else if !terminal_content_included {
        collection.note(
            "terminal_content",
            "unavailable",
            Some("not_collected".into()),
        );
    }
    let manifest = json!({
        "schema": "undercurrent.support/v0.1.0-beta", "captured_at_millis": undercurrent_diagnostics::now_millis(),
        "incident_id": arguments.incident, "partial": collection.partial, "sources": collection.sources,
        "terminal_content_included": terminal_content_included, "backtraces_requested": arguments.include_backtraces,
        "identifiers_included": true, "maximum_archive_bytes": ARCHIVE_BYTES, "sources_are_bounded": true,
        "consistency": "independent_sources; diagnostic_continuations_share_one_client",
    });
    let report = CollectionReport {
        partial: collection.partial,
        entries: collection.files.len() + 1,
        terminal_content_included,
    };
    let mut archive = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .unix_permissions(0o600);
    for (name, bytes) in collection.files.into_iter().chain(std::iter::once((
        "manifest.json".into(),
        serde_json::to_vec_pretty(&manifest)
            .map_err(|e| CliError::SupportReport(io::Error::other(e)))?,
    ))) {
        archive
            .start_file(name, options)
            .map_err(|e| CliError::SupportReport(io::Error::other(e)))?;
        archive.write_all(&bytes).map_err(CliError::SupportReport)?;
    }
    let bytes = archive
        .finish()
        .map_err(|e| CliError::SupportReport(io::Error::other(e)))?
        .into_inner();
    if bytes.len() > ARCHIVE_BYTES {
        return Err(CliError::SupportReport(io::ErrorKind::FileTooLarge.into()));
    }
    let output = if arguments.output.is_absolute() {
        arguments.output.clone()
    } else {
        std::env::current_dir()
            .map_err(CliError::SupportReport)?
            .join(&arguments.output)
    };
    undercurrent_diagnostics::write_private_new(&output, &bytes)
        .map_err(CliError::SupportReport)?;
    Ok(report)
}

fn request(
    client: &ProductClient,
    request: ProductRequest,
    deadline: Instant,
) -> Result<ProductSuccess, String> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err("collection_deadline".into());
    }
    client
        .request(request, remaining.min(Duration::from_secs(3)))
        .map_err(|e| e.category().as_str().into())
}

fn result_category(result: Result<ProductSuccess, String>) -> String {
    result.err().unwrap_or_else(|| "response_mismatch".into())
}

struct Pages {
    items: Vec<DiagnosticPageDto>,
    complete: bool,
    reason: Option<String>,
}

fn collect_pages(
    mut read: impl FnMut(Option<String>) -> Result<DiagnosticPageDto, String>,
) -> Pages {
    let mut result = Pages {
        items: Vec::new(),
        complete: false,
        reason: None,
    };
    let mut cursor = None;
    let mut seen = BTreeSet::new();
    let mut captured = None;
    let mut bytes = 0;
    for _ in 0..MAX_PAGES {
        let page = match read(cursor) {
            Ok(page) => page,
            Err(error) => {
                result.reason = Some(error);
                return result;
            }
        };
        if captured.is_some_and(|time| time != page.captured_at_millis) {
            result.reason = Some("snapshot_changed".into());
            return result;
        }
        captured = Some(page.captured_at_millis);
        bytes += serde_json::to_vec(&page).map_or(ARCHIVE_BYTES, |b| b.len());
        if bytes > LOG_BYTES {
            result.reason = Some("section_byte_limit".into());
            return result;
        }
        cursor = page.next_cursor.clone();
        result.items.push(page);
        match &cursor {
            None => {
                result.complete = true;
                return result;
            }
            Some(token) if !seen.insert(token.clone()) => {
                result.reason = Some("repeated_cursor".into());
                return result;
            }
            _ => {}
        }
    }
    result.reason = Some("page_limit".into());
    result
}

fn redact_strings(value: &mut Value) {
    match value {
        Value::String(text) => *text = "[REDACTED]".into(),
        Value::Array(values) => values.iter_mut().for_each(redact_strings),
        Value::Object(values) => values.values_mut().for_each(redact_strings),
        _ => {}
    }
}

fn sanitize_log(value: &Value) -> Value {
    let Some(object) = value.as_object() else {
        return Value::Null;
    };
    let mut safe = serde_json::Map::new();
    for (key, value) in object {
        if matches!(key.as_str(), "fields" | "span") {
            safe.insert(key.clone(), sanitize_log(value));
            continue;
        }
        if !matches!(
            key.as_str(),
            "timestamp"
                | "level"
                | "target"
                | "event_name"
                | "operation"
                | "category"
                | "error_kind"
                | "result"
                | "duration_micros"
                | "response_bytes"
                | "connection_id"
                | "server_epoch"
                | "client_id"
                | "request_id"
                | "trace_id"
                | "parent_span_id"
                | "correlation_id"
                | "session_id"
                | "generation"
                | "profiles"
                | "worker_kind"
                | "observed_at_millis"
                | "active_generation"
                | "previous_definition_active"
                | "stage"
                | "status"
                | "last_rejection_at_millis"
                | "component"
                | "version"
                | "build_revision"
                | "os"
                | "arch"
                | "process_id"
                | "recorded_at_millis"
        ) {
            continue;
        }
        match value {
            Value::Number(_) | Value::Bool(_) | Value::Null => {
                safe.insert(key.clone(), value.clone());
            }
            Value::String(s)
                if s.len() <= 128
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_-.:+TZ".contains(&b)) =>
            {
                safe.insert(key.clone(), value.clone());
            }
            _ => {}
        }
    }
    Value::Object(safe)
}

fn read_log_tail(path: &Path) -> io::Result<(Vec<u8>, bool)> {
    use std::io::{Read, Seek, SeekFrom};
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let mut file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let offset = file.metadata()?.len().saturating_sub(LOG_BYTES as u64);
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = Vec::new();
    file.take(LOG_BYTES as u64).read_to_end(&mut bytes)?;
    if offset > 0 {
        if let Some(end) = bytes.iter().position(|b| *b == b'\n') {
            bytes.drain(..=end);
        } else {
            bytes.clear();
        }
    }
    Ok((bytes, offset > 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pagination_retains_partial_evidence_and_rejects_snapshot_changes() {
        let mut calls = 0;
        let pages = collect_pages(|cursor| {
            calls += 1;
            assert_eq!(
                cursor,
                if calls == 1 {
                    None
                } else {
                    Some("next".into())
                }
            );
            Ok(DiagnosticPageDto {
                items: vec![],
                next_cursor: Some("next".into()),
                captured_at_millis: calls,
            })
        });
        assert!(!pages.complete);
        assert_eq!(pages.items.len(), 1);
        assert_eq!(pages.reason.as_deref(), Some("snapshot_changed"));
        let pages = collect_pages(|_| {
            Ok(DiagnosticPageDto {
                items: vec![],
                next_cursor: Some("same".into()),
                captured_at_millis: 1,
            })
        });
        assert_eq!(pages.reason.as_deref(), Some("repeated_cursor"));
    }

    #[test]
    fn shareable_logs_exclude_private_error_bodies_and_unknown_fields() {
        let value = sanitize_log(
            &json!({ "timestamp":"2026-10-05T00:00:00Z", "fields": { "message":"SECRET", "error":"/private/SECRET", "operation":"start_session", "environment":"SECRET" }, "span": { "correlation_id":"UC-0123456789abcdef0123456789abcdef", "payload":"SECRET" } }),
        );
        let text = value.to_string();
        assert!(!text.contains("SECRET"));
        assert!(text.contains("start_session"));
        assert!(text.contains("UC-"));
    }
}
