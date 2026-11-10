// SPDX-License-Identifier: Apache-2.0

//! Local utility-command human presentation.

use std::fmt::Write as _;

use crate::cli_support::{CliOutputError, OutputFormat, OutputShape, Verbosity};
use crate::{CliError, CliPresenter, DiagnosticEventCatalog, RedactedCliConfigReport};

use super::safe_text;

/// Renders the complete linked diagnostic-event catalog as a scan-friendly table.
///
/// This function performs no IO and does not panic.
pub fn diagnostic_event_catalog_text(catalog: &DiagnosticEventCatalog) -> String {
    let owner_width = catalog
        .events
        .iter()
        .map(|event| event.owner.as_str().len())
        .max()
        .unwrap_or(5)
        .max(5);
    let mut output = format!("{:<owner_width$}  EVENT\n", "OWNER");
    for event in &catalog.events {
        let _ = writeln!(
            output,
            "{:<owner_width$}  {}",
            event.owner.as_str(),
            safe_text(&event.name)
        );
    }
    let _ = write!(output, "{} diagnostic events", catalog.count);
    output
}

impl CliPresenter {
    /// Writes complete effective path-redacted configuration.
    ///
    /// Human text uses deterministic pretty JSON so every effective setting is
    /// visible without adding a second configuration serialization dependency.
    /// Machine JSON uses the standard finite-output adapter. Quiet text is
    /// suppressed; explicitly requested machine output is retained. This
    /// method performs writer IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns output-shape, serialization, or writer failure.
    pub fn write_config_report<W>(
        self,
        writer: &mut W,
        report: &RedactedCliConfigReport,
    ) -> Result<(), CliError>
    where
        W: std::io::Write,
    {
        self.context.validate_output_shape(OutputShape::Finite)?;
        if self.context.output != OutputFormat::Text {
            return self.write_finite(writer, report, |_| String::new());
        }
        if self.context.verbosity == Verbosity::Quiet {
            return Ok(());
        }
        serde_json::to_writer_pretty(&mut *writer, report).map_err(CliOutputError::from)?;
        writer.write_all(b"\n").map_err(CliOutputError::from)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::cli_support::{CliContext, CommandTimeout};
    use crate::{
        CliConfigSourceKind, CliConnectionPlacement, DiagnosticEventCatalogEntry,
        DiagnosticEventOwner,
    };

    use super::*;

    #[test]
    fn catalog_text_escapes_event_names_and_reports_the_exact_count() {
        let catalog = DiagnosticEventCatalog {
            count: 1,
            events: vec![DiagnosticEventCatalogEntry {
                owner: DiagnosticEventOwner::Cli,
                name: "command\nforged".to_owned(),
            }],
        };

        let rendered = diagnostic_event_catalog_text(&catalog);

        assert!(rendered.contains("command\\nforged"));
        assert!(!rendered.contains("command\nforged"));
        assert!(rendered.ends_with("1 diagnostic events"));
    }

    #[test]
    fn configuration_text_is_complete_json_without_any_path_value() {
        let report = RedactedCliConfigReport {
            effective: serde_json::json!({}),
            source: CliConfigSourceKind::Explicit,
            placement: CliConnectionPlacement::Local,
            max_sessions: 300,
            bulk_parallel_requests: 8,
            request_bytes: 65_536,
        };
        let mut output = Vec::new();

        CliPresenter::new(CliContext::new(
            OutputFormat::Text,
            Verbosity::Normal,
            CommandTimeout::new(Duration::from_secs(1), Duration::from_secs(1)).expect("timeout"),
        ))
        .write_config_report(&mut output, &report)
        .expect("config report");

        let value: serde_json::Value = serde_json::from_slice(&output).expect("config JSON");
        assert_eq!(value["source"], "explicit");
        assert_eq!(value["placement"], "local");
        assert!(value.get("loopback_port").is_none());
        assert_eq!(value["max_sessions"], 300);
        let rendered = String::from_utf8(output).expect("UTF-8");
        assert!(!rendered.contains("product_home"));
        assert!(!rendered.contains("database.sqlite"));
        assert!(!rendered.contains("websocket.token"));
    }
}
