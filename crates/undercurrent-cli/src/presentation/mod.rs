// SPDX-License-Identifier: Apache-2.0

//! Native CLI output-shape enforcement and payload-safe human presentation.

mod operations;
mod session;
mod stream;
mod terminal;
mod utility;

use std::io::Write;

use crate::cli_support::{
    CliContext, CliOutputError, CommonExitCode, OutputFormat, OutputShape, Verbosity,
};
use serde::Serialize;

use crate::{CliCommand, CliError};

pub use operations::*;
pub use session::*;
pub use stream::*;
pub use terminal::*;
pub use utility::*;

/// Shared writer adapter for one validated CLI invocation.
#[derive(Debug, Clone, Copy)]
pub struct CliPresenter {
    context: CliContext,
}

impl CliPresenter {
    /// Creates a presenter from the shared format, verbosity, and timeout context.
    ///
    /// This function performs no IO and does not panic.
    pub const fn new(context: CliContext) -> Self {
        Self { context }
    }

    /// Writes one finite result in text or pretty JSON.
    ///
    /// Text rendering is suppressed by quiet mode. Explicit machine output is
    /// never suppressed. The text closure is evaluated only when text will be
    /// written.
    ///
    /// # Errors
    ///
    /// Returns a typed shape, serialization, or writer failure. This method
    /// performs writer IO and does not panic unless the supplied closure panics.
    pub fn write_finite<T, F, W>(self, writer: &mut W, value: &T, text: F) -> Result<(), CliError>
    where
        T: Serialize,
        F: FnOnce(Verbosity) -> String,
        W: Write,
    {
        self.context.validate_output_shape(OutputShape::Finite)?;
        if self.context.output == OutputFormat::Text && self.context.verbosity == Verbosity::Quiet {
            return Ok(());
        }
        self.context
            .output
            .write(writer, value, || text(self.context.verbosity))?;
        Ok(())
    }

    /// Writes one independently consumable stream item in text or JSON Lines.
    ///
    /// Text rendering is suppressed by quiet mode. Explicit JSON Lines output
    /// is never suppressed.
    ///
    /// # Errors
    ///
    /// Returns a typed shape, serialization, or writer failure. This method
    /// performs writer IO and does not panic unless the supplied closure panics.
    pub fn write_stream_item<T, F, W>(
        self,
        writer: &mut W,
        value: &T,
        text: F,
    ) -> Result<(), CliError>
    where
        T: Serialize,
        F: FnOnce(Verbosity) -> String,
        W: Write,
    {
        self.context.validate_output_shape(OutputShape::Stream)?;
        if self.context.output == OutputFormat::Text && self.context.verbosity == Verbosity::Quiet {
            return Ok(());
        }
        self.context
            .output
            .write(writer, value, || text(self.context.verbosity))?;
        Ok(())
    }

    /// Writes raw finite bytes only in text mode.
    ///
    /// This is used for terminal screen and output commands whose text contract
    /// is the exact byte stream. Machine modes must serialize their typed DTOs
    /// through [`Self::write_finite`] instead.
    ///
    /// # Errors
    ///
    /// Returns a typed output-shape or writer failure. This method performs
    /// writer IO and does not panic.
    pub fn write_raw_finite<W>(self, writer: &mut W, bytes: &[u8]) -> Result<(), CliError>
    where
        W: Write,
    {
        self.context.validate_output_shape(OutputShape::Finite)?;
        if self.context.output != OutputFormat::Text {
            return Err(CliError::RawOutputRequiresText);
        }
        if self.context.verbosity != Verbosity::Quiet {
            writer.write_all(bytes).map_err(CliOutputError::from)?;
        }
        Ok(())
    }

    /// Writes an essential text document produced by one utility command.
    ///
    /// Unlike ordinary human presentation, explicit document output is not
    /// hidden by quiet mode because the bytes are the command result. This is
    /// used for shell completions and print-only integration scripts.
    ///
    /// # Errors
    ///
    /// Returns a typed output-shape or writer failure. This method performs
    /// writer IO and does not panic.
    pub fn write_text_document<W>(
        self,
        writer: &mut W,
        command: CliCommand,
        bytes: &[u8],
    ) -> Result<(), CliError>
    where
        W: Write,
    {
        self.context.validate_output_shape(OutputShape::Finite)?;
        if self.context.output != OutputFormat::Text {
            return Err(CliError::TextDocumentRequiresText { command });
        }
        writer.write_all(bytes).map_err(CliOutputError::from)?;
        Ok(())
    }

    /// Writes one failure report to the caller's error channel.
    ///
    /// Failures are never hidden by quiet mode. Only the payload-safe public
    /// display text and stable category are emitted; nested sources are not
    /// serialized or printed.
    ///
    /// # Errors
    ///
    /// Returns serialization or writer IO failure. This method performs writer
    /// IO and does not panic.
    pub fn write_failure<W>(
        self,
        writer: &mut W,
        command: Option<CliCommand>,
        error: &CliError,
    ) -> Result<(), CliOutputError>
    where
        W: Write,
    {
        self.write_failure_with_incident(writer, command, error, None)
    }

    pub(crate) fn write_failure_with_incident<W: Write>(
        self,
        writer: &mut W,
        command: Option<CliCommand>,
        error: &CliError,
        incident_id: Option<&str>,
    ) -> Result<(), CliOutputError> {
        let report = CliFailureReport {
            incident_id: incident_id.map(str::to_owned),
            command,
            result_category: error.exit_code(),
            product_error_category: error.product_error_category(),
            error_disposition: error.error_disposition(),
            effect_certainty: error.error_effect_certainty(),
            message: error.to_string(),
        };
        match self.context.output {
            OutputFormat::Text => {
                writeln!(writer, "error: {}", report.message)?;
                if let Some(id) = incident_id {
                    writeln!(
                        writer,
                        "error reference: {id}\nCollect a support report: uc debug collect --incident {id} --output uc-support.zip"
                    )?;
                }
                Ok(())
            }
            OutputFormat::Json => OutputFormat::Json.write(writer, &report, String::new),
            OutputFormat::Jsonl => OutputFormat::Jsonl.write(writer, &report, String::new),
        }
    }
}

/// Stable structured failure emitted only on the CLI error channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CliFailureReport {
    /// Support reference linking this invocation to retained failure evidence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incident_id: Option<String>,
    /// Parsed command when argument decoding reached the subcommand boundary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<CliCommand>,
    /// Stable shared process result category.
    pub result_category: CommonExitCode,
    /// Exact server product category when one was supplied.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub product_error_category: Option<undercurrent_protocol::ProductErrorCategory>,
    /// Exact cross-boundary disposition when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_disposition: Option<undercurrent_protocol::ProductErrorDisposition>,
    /// Explicit owner evidence, absent for legacy or unproven failures.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effect_certainty: Option<undercurrent_protocol::EffectCertaintyDto>,
    /// Payload-safe actionable diagnostic.
    pub message: String,
}

pub(super) fn safe_text(value: &str) -> String {
    value.escape_debug().to_string()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::cli_support::{CommandTimeout, OutputFormat};
    use serde_json::json;

    use super::*;

    #[test]
    fn finite_and_stream_machine_shapes_are_enforced() {
        let finite = presenter(OutputFormat::Json, Verbosity::Normal);
        let mut bytes = Vec::new();
        finite
            .write_finite(&mut bytes, &json!({"name": "worker"}), |_| {
                "human".to_owned()
            })
            .expect("finite JSON");
        assert!(serde_json::from_slice::<serde_json::Value>(&bytes).is_ok());
        assert!(
            finite
                .write_stream_item(&mut Vec::new(), &json!({"name": "worker"}), |_| {
                    "human".to_owned()
                })
                .is_err()
        );

        let stream = presenter(OutputFormat::Jsonl, Verbosity::Normal);
        let mut bytes = Vec::new();
        stream
            .write_stream_item(&mut bytes, &json!({"name": "worker"}), |_| {
                "human".to_owned()
            })
            .expect("stream JSON Lines");
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);
        assert!(
            stream
                .write_finite(&mut Vec::new(), &json!({"name": "worker"}), |_| {
                    "human".to_owned()
                })
                .is_err()
        );
    }

    #[test]
    fn quiet_suppresses_text_but_not_explicit_machine_results() {
        let value = json!({"name": "worker"});
        let quiet_text = presenter(OutputFormat::Text, Verbosity::Quiet);
        let mut text = Vec::new();
        quiet_text
            .write_finite(&mut text, &value, |_| "human".to_owned())
            .expect("quiet text");
        assert!(text.is_empty());

        let quiet_json = presenter(OutputFormat::Json, Verbosity::Quiet);
        let mut json = Vec::new();
        quiet_json
            .write_finite(&mut json, &value, |_| "human".to_owned())
            .expect("quiet JSON");
        assert!(!json.is_empty());
    }

    #[test]
    fn failures_never_include_nested_private_source_text() {
        let error = CliError::Process {
            operation: crate::CliProcessOperation::SpawnDaemon,
            source: std::io::Error::other("/private/credential/path"),
        };
        let mut bytes = Vec::new();

        presenter(OutputFormat::Json, Verbosity::Diagnostic)
            .write_failure(&mut bytes, Some(CliCommand::Daemon), &error)
            .expect("failure report");

        let rendered = String::from_utf8(bytes).expect("UTF-8");
        assert!(!rendered.contains("/private/credential/path"));
        let report: serde_json::Value = serde_json::from_str(&rendered).expect("JSON");
        assert_eq!(report["result_category"], "runtime");
        assert_eq!(report["command"], "daemon");
    }

    #[test]
    fn essential_text_documents_ignore_quiet_but_reject_machine_wrapping() {
        let mut bytes = Vec::new();
        presenter(OutputFormat::Text, Verbosity::Quiet)
            .write_text_document(&mut bytes, CliCommand::Completion, b"complete uc\n")
            .expect("quiet completion document");
        assert_eq!(bytes, b"complete uc\n");

        let error = presenter(OutputFormat::Json, Verbosity::Normal)
            .write_text_document(&mut Vec::new(), CliCommand::Completion, b"complete uc\n")
            .expect_err("completion cannot be JSON wrapped");
        assert!(matches!(
            error,
            CliError::TextDocumentRequiresText {
                command: CliCommand::Completion
            }
        ));
    }

    fn presenter(output: OutputFormat, verbosity: Verbosity) -> CliPresenter {
        CliPresenter::new(CliContext::new(
            output,
            verbosity,
            CommandTimeout::new(Duration::from_secs(1), Duration::from_secs(1)).expect("timeout"),
        ))
    }
}
