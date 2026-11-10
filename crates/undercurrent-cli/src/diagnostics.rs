// SPDX-License-Identifier: Apache-2.0

//! Payload-free native CLI diagnostic records and outer sinks.

use std::io::Write;
use std::time::Duration;

use crate::cli_support::CommonExitCode;
use serde::ser::{Serialize, SerializeMap, Serializer};
use undercurrent_protocol::ProductOperation;

use crate::{CliCommand, CliDiagnosticEvent, CliLogField};

/// One fixed-cardinality, payload-free CLI diagnostic record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CliDiagnosticRecord {
    /// Stable event registry value.
    pub event: CliDiagnosticEvent,
    /// Stable command registry value when a command owns the event.
    pub command: Option<CliCommand>,
    /// Stable result category for terminal command events.
    pub result_category: Option<CommonExitCode>,
    /// Stable finite product operation for request-boundary events.
    pub operation: Option<ProductOperation>,
    /// Saturating elapsed operation duration in microseconds.
    pub duration_micros: Option<u64>,
}

impl CliDiagnosticRecord {
    /// Creates a payload-free event for one command.
    ///
    /// This function performs no IO and does not panic.
    pub const fn command(event: CliDiagnosticEvent, command: CliCommand) -> Self {
        Self {
            event,
            command: Some(command),
            result_category: None,
            operation: None,
            duration_micros: None,
        }
    }

    /// Creates one terminal command event with a stable category and duration.
    ///
    /// Durations that exceed `u64` microseconds saturate. This function performs
    /// no IO and does not panic.
    pub fn terminal(
        event: CliDiagnosticEvent,
        command: CliCommand,
        result_category: CommonExitCode,
        duration: Duration,
    ) -> Self {
        Self {
            event,
            command: Some(command),
            result_category: Some(result_category),
            operation: None,
            duration_micros: Some(u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)),
        }
    }

    /// Creates one terminal finite-request event without retaining its payload.
    ///
    /// This function performs no IO and does not panic.
    pub fn request(
        command: CliCommand,
        operation: ProductOperation,
        result_category: CommonExitCode,
        duration: Duration,
    ) -> Self {
        Self {
            event: CliDiagnosticEvent::RequestCompleted,
            command: Some(command),
            result_category: Some(result_category),
            operation: Some(operation),
            duration_micros: Some(u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)),
        }
    }
}

impl Serialize for CliDiagnosticRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut count = 1;
        count += usize::from(self.command.is_some());
        count += usize::from(self.result_category.is_some());
        count += usize::from(self.operation.is_some());
        count += usize::from(self.duration_micros.is_some());
        let mut map = serializer.serialize_map(Some(count))?;
        map.serialize_entry(CliLogField::Event.as_str(), self.event.as_str())?;
        if let Some(command) = self.command {
            map.serialize_entry(CliLogField::Command.as_str(), command.as_str())?;
        }
        if let Some(result_category) = self.result_category {
            map.serialize_entry(
                CliLogField::ResultCategory.as_str(),
                result_category.as_str(),
            )?;
        }
        if let Some(operation) = self.operation {
            map.serialize_entry(CliLogField::Operation.as_str(), operation.as_str())?;
        }
        if let Some(duration_micros) = self.duration_micros {
            map.serialize_entry(CliLogField::DurationMicros.as_str(), &duration_micros)?;
        }
        map.end()
    }
}

/// Outer diagnostic boundary consumed by native CLI orchestration.
pub trait CliDiagnosticSink: Send + Sync {
    /// Emits one payload-free record without affecting command success.
    ///
    /// Implementations must never add request payloads, arguments, filesystem
    /// paths, environment values, or nested error strings.
    fn emit(&self, record: CliDiagnosticRecord);
}

/// Diagnostic sink that intentionally discards all records.
#[derive(Debug, Default, Clone, Copy)]
pub struct DisabledCliDiagnosticSink;

impl CliDiagnosticSink for DisabledCliDiagnosticSink {
    fn emit(&self, _record: CliDiagnosticRecord) {}
}

/// Best-effort JSON Lines diagnostics written to the process error channel.
#[derive(Debug, Clone, Copy)]
pub struct NativeCliDiagnosticSink {
    enabled: bool,
}

impl NativeCliDiagnosticSink {
    /// Creates a process diagnostic sink controlled by explicit CLI verbosity.
    ///
    /// This function performs no IO and does not panic.
    pub const fn new(enabled: bool) -> Self {
        Self { enabled }
    }
}

impl CliDiagnosticSink for NativeCliDiagnosticSink {
    fn emit(&self, record: CliDiagnosticRecord) {
        if !self.enabled {
            return;
        }
        let stderr = std::io::stderr();
        let mut writer = stderr.lock();
        let mut value = serde_json::to_value(record).unwrap_or_default();
        if let Some(object) = value.as_object_mut() {
            object.insert(
                "timestamp_millis".into(),
                undercurrent_diagnostics::now_millis().into(),
            );
            if let Some(recorder) = undercurrent_diagnostics::process() {
                object.insert("incident_id".into(), recorder.incident_id().into());
            }
        }
        if serde_json::to_writer(&mut writer, &value).is_ok() {
            let _ = writer.write_all(b"\n");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_record_uses_only_central_safe_fields() {
        let record = CliDiagnosticRecord::terminal(
            CliDiagnosticEvent::CommandCompleted,
            CliCommand::Ls,
            CommonExitCode::Success,
            Duration::from_micros(42),
        );

        let value = serde_json::to_value(record).expect("diagnostic serialization");

        assert_eq!(value[CliLogField::Event.as_str()], "cli.command.completed");
        assert_eq!(value[CliLogField::Command.as_str()], "ls");
        assert_eq!(value[CliLogField::ResultCategory.as_str()], "success");
        assert_eq!(value[CliLogField::DurationMicros.as_str()], 42);
        assert_eq!(value.as_object().expect("object").len(), 4);
    }

    #[test]
    fn request_record_names_the_operation_without_a_payload() {
        let record = CliDiagnosticRecord::request(
            CliCommand::Ls,
            ProductOperation::ListSessions,
            CommonExitCode::Success,
            Duration::from_micros(7),
        );

        let value = serde_json::to_value(record).expect("diagnostic serialization");

        assert_eq!(value[CliLogField::Operation.as_str()], "list_sessions");
        assert_eq!(value.as_object().expect("object").len(), 5);
    }
}
