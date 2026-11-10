// SPDX-License-Identifier: Apache-2.0

//! Shared CLI output and exit-code helpers.
//!
//! This module is independent of `clap`. The native argument adapter maps
//! parser values into these types before command routing and presentation.

#![deny(missing_docs)]

use crate::duration::{DurationParseErrorReason, DurationParser, DurationUnits};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::{self, Write};
use std::process::ExitCode;
use std::str::FromStr;
use std::time::Duration;

/// Default universal upper bound used by [`CommandTimeout::from_str`].
pub const MAX_COMMON_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

/// Whether a command produces one finite result or a continuing item stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputShape {
    /// One finite result object.
    Finite,
    /// Zero or more independently consumable stream items.
    Stream,
}

/// Presentation verbosity for the Undercurrent CLI.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Verbosity {
    /// Suppress non-essential presentation while preserving failures and
    /// explicitly requested machine output.
    Quiet,
    /// Default operator-facing context.
    #[default]
    Normal,
    /// Additional safe operational context.
    Verbose,
    /// Maximum payload-redacted diagnostics intended for troubleshooting.
    Diagnostic,
}

/// Reusable CLI presentation and timeout context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliContext {
    /// Selected output encoding.
    pub output: OutputFormat,
    /// Selected safe presentation verbosity.
    pub verbosity: Verbosity,
    /// Explicit command timeout.
    pub timeout: CommandTimeout,
}

impl CliContext {
    /// Constructs a common CLI context from independently validated values.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub const fn new(output: OutputFormat, verbosity: Verbosity, timeout: CommandTimeout) -> Self {
        Self {
            output,
            verbosity,
            timeout,
        }
    }

    /// Validates that the selected machine format matches the command's output
    /// shape.
    ///
    /// # Errors
    ///
    /// Returns [`OutputShapeError`] when JSON is selected for a stream or JSON
    /// Lines is selected for a finite result. Text is valid for both.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub const fn validate_output_shape(self, shape: OutputShape) -> Result<(), OutputShapeError> {
        self.output.validate_shape(shape)
    }
}

/// Machine output formats used by the Undercurrent CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputFormat {
    /// Human-readable text output.
    Text,
    /// Pretty JSON output.
    Json,
    /// One compact JSON object per line.
    Jsonl,
}

impl OutputFormat {
    /// Returns true when the format is machine-readable.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub const fn is_structured(self) -> bool {
        matches!(self, OutputFormat::Json | OutputFormat::Jsonl)
    }

    /// Validates finite JSON versus streaming JSON Lines semantics.
    ///
    /// # Errors
    ///
    /// Returns [`OutputShapeError`] for a structured format that cannot be
    /// incrementally or atomically consumed as requested.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub const fn validate_shape(self, shape: OutputShape) -> Result<(), OutputShapeError> {
        match (self, shape) {
            (Self::Json, OutputShape::Stream) | (Self::Jsonl, OutputShape::Finite) => {
                Err(OutputShapeError {
                    format: self,
                    shape,
                })
            }
            (Self::Text, _)
            | (Self::Json, OutputShape::Finite)
            | (Self::Jsonl, OutputShape::Stream) => Ok(()),
        }
    }

    /// Renders a value using this output format.
    ///
    /// The `text` closure is evaluated only for [`OutputFormat::Text`].
    ///
    /// # Errors
    ///
    /// Returns [`serde_json::Error`] when JSON serialization fails.
    ///
    /// # Panics
    ///
    /// Does not panic unless the supplied text closure panics.
    pub fn render<T, F>(self, value: &T, text: F) -> Result<String, serde_json::Error>
    where
        T: Serialize,
        F: FnOnce() -> String,
    {
        match self {
            OutputFormat::Text => Ok(text()),
            OutputFormat::Json => serde_json::to_string_pretty(value),
            OutputFormat::Jsonl => serde_json::to_string(value).map(|line| format!("{line}\n")),
        }
    }

    /// Writes a value to a writer using this output format.
    ///
    /// # Errors
    ///
    /// Returns [`CliOutputError`] when serialization or writing fails.
    ///
    /// # Panics
    ///
    /// Does not panic unless the supplied text closure panics.
    pub fn write<T, F, W>(self, writer: &mut W, value: &T, text: F) -> Result<(), CliOutputError>
    where
        T: Serialize,
        F: FnOnce() -> String,
        W: Write,
    {
        let rendered = self.render(value, text)?;
        writer.write_all(rendered.as_bytes())?;
        if !rendered.ends_with('\n') {
            writer.write_all(b"\n")?;
        }
        Ok(())
    }
}

/// Structured output format does not match finite or streaming command shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputShapeError {
    format: OutputFormat,
    shape: OutputShape,
}

impl OutputShapeError {
    /// Returns rejected output format.
    pub const fn format(self) -> OutputFormat {
        self.format
    }

    /// Returns command output shape.
    pub const fn shape(self) -> OutputShape {
        self.shape
    }
}

impl Display for OutputShapeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "output format {} is incompatible with {:?} command output",
            self.format, self.shape
        )
    }
}

impl Error for OutputShapeError {}

/// Non-zero command timeout bounded by a caller-supplied maximum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct CommandTimeout(Duration);

impl CommandTimeout {
    /// Constructs a non-zero timeout within `maximum`.
    ///
    /// # Errors
    ///
    /// Returns [`TimeoutParseError`] for zero, zero maximum, or a duration above
    /// the caller-supplied maximum.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub const fn new(duration: Duration, maximum: Duration) -> Result<Self, TimeoutParseError> {
        if duration.is_zero() || maximum.is_zero() {
            return Err(TimeoutParseError::Zero);
        }
        if duration.as_millis() > maximum.as_millis() {
            return Err(TimeoutParseError::ExceedsMaximum { maximum });
        }
        Ok(Self(duration))
    }

    /// Returns the validated timeout duration.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub const fn get(self) -> Duration {
        self.0
    }

    /// Parses an integer timeout using `ms`, `s`, `m`, or `h`, then applies a
    /// caller-supplied upper bound.
    ///
    /// # Errors
    ///
    /// Returns [`TimeoutParseError`] for malformed, zero, overflowing, unknown,
    /// or excessive values.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn parse_with_maximum(value: &str, maximum: Duration) -> Result<Self, TimeoutParseError> {
        let duration = DurationParser::new(DurationUnits::SUB_DAY)
            .with_maximum(maximum)
            .parse(value)
            .map_err(TimeoutParseError::from_duration_error)?;
        Self::new(duration, maximum)
    }
}

impl FromStr for CommandTimeout {
    type Err = TimeoutParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse_with_maximum(value, MAX_COMMON_TIMEOUT)
    }
}

impl<'de> Deserialize<'de> for CommandTimeout {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let duration = Duration::deserialize(deserializer)?;
        Self::new(duration, MAX_COMMON_TIMEOUT).map_err(serde::de::Error::custom)
    }
}

impl Display for CommandTimeout {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}ms", self.0.as_millis())
    }
}

/// Invalid command timeout text or bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutParseError {
    /// Timeout or configured maximum was zero.
    Zero,
    /// Numeric component was empty or not an unsigned integer.
    InvalidNumber,
    /// Unit was absent or not one of `ms`, `s`, `m`, or `h`.
    MissingOrUnknownUnit,
    /// Millisecond conversion overflowed.
    Overflow,
    /// Timeout exceeded caller-supplied maximum.
    ExceedsMaximum {
        /// Inclusive maximum.
        maximum: Duration,
    },
}

impl Display for TimeoutParseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Zero => formatter.write_str("timeout and maximum must be non-zero"),
            Self::InvalidNumber => {
                formatter.write_str("timeout must start with an unsigned integer")
            }
            Self::MissingOrUnknownUnit => formatter.write_str("timeout must use ms, s, m, or h"),
            Self::Overflow => formatter.write_str("timeout is too large to represent"),
            Self::ExceedsMaximum { maximum } => write!(
                formatter,
                "timeout exceeds maximum of {}ms",
                maximum.as_millis()
            ),
        }
    }
}

impl Error for TimeoutParseError {}

impl TimeoutParseError {
    fn from_duration_error(error: crate::duration::DurationParseError) -> Self {
        match error.reason() {
            DurationParseErrorReason::Zero => Self::Zero,
            DurationParseErrorReason::MissingUnit { .. }
            | DurationParseErrorReason::UnsupportedUnit { .. }
            | DurationParseErrorReason::UnitNotAllowed { .. } => Self::MissingOrUnknownUnit,
            DurationParseErrorReason::Overflow => Self::Overflow,
            DurationParseErrorReason::ExceedsMaximum { maximum } => {
                Self::ExceedsMaximum { maximum: *maximum }
            }
            DurationParseErrorReason::Empty
            | DurationParseErrorReason::MissingNumber { .. }
            | DurationParseErrorReason::MissingFraction { .. }
            | DurationParseErrorReason::MultipleSegments
            | DurationParseErrorReason::DecimalNotAllowed
            | DurationParseErrorReason::PrecisionLoss => Self::InvalidNumber,
        }
    }
}

impl Display for OutputFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Text => f.write_str("text"),
            OutputFormat::Json => f.write_str("json"),
            OutputFormat::Jsonl => f.write_str("jsonl"),
        }
    }
}

impl FromStr for OutputFormat {
    type Err = OutputFormatParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "text" => Ok(OutputFormat::Text),
            "json" => Ok(OutputFormat::Json),
            "jsonl" => Ok(OutputFormat::Jsonl),
            other => Err(OutputFormatParseError {
                value: other.to_string(),
            }),
        }
    }
}

/// Error returned when parsing a CLI output format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputFormatParseError {
    value: String,
}

impl OutputFormatParseError {
    /// Returns the rejected output format value.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl Display for OutputFormatParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "unsupported output format {:?}", self.value)
    }
}

impl Error for OutputFormatParseError {}

/// Error returned while rendering or writing CLI output.
#[derive(Debug)]
pub enum CliOutputError {
    /// JSON serialization failed.
    Serialize(serde_json::Error),
    /// Writer IO failed.
    Io(io::Error),
}

impl Display for CliOutputError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CliOutputError::Serialize(error) => write!(f, "failed to serialize output: {error}"),
            CliOutputError::Io(error) => write!(f, "failed to write output: {error}"),
        }
    }
}

impl Error for CliOutputError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CliOutputError::Serialize(error) => Some(error),
            CliOutputError::Io(error) => Some(error),
        }
    }
}

impl From<serde_json::Error> for CliOutputError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialize(error)
    }
}

impl From<io::Error> for CliOutputError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Common exit codes that products may reuse at their CLI boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommonExitCode {
    /// Command completed successfully.
    Success = 0,
    /// Generic command failure.
    Failure = 1,
    /// Usage, validation, parsing, or malformed input failure.
    InvalidInput = 2,
    /// Completed with incomplete or indeterminate output.
    Incomplete = 3,
    /// Denied by policy or authority.
    Denied = 10,
    /// Approval is required before continuing.
    ApprovalRequired = 11,
    /// Local software, storage, process, or unexpected runtime failure.
    Runtime = 70,
    /// Command was interrupted.
    Interrupted = 130,
}

impl CommonExitCode {
    /// Returns the stable snake_case name used in structured CLI diagnostics.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::InvalidInput => "invalid_input",
            Self::Incomplete => "incomplete",
            Self::Denied => "denied",
            Self::ApprovalRequired => "approval_required",
            Self::Runtime => "runtime",
            Self::Interrupted => "interrupted",
        }
    }

    /// Returns the numeric process exit code.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub const fn code(self) -> u8 {
        self as u8
    }

    /// Converts the common code into [`std::process::ExitCode`].
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn exit_code(self) -> ExitCode {
        ExitCode::from(self.code())
    }
}

impl From<CommonExitCode> for ExitCode {
    fn from(value: CommonExitCode) -> Self {
        value.exit_code()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn output_format_parses_stable_values() {
        assert_eq!("text".parse::<OutputFormat>(), Ok(OutputFormat::Text));
        assert_eq!("json".parse::<OutputFormat>(), Ok(OutputFormat::Json));
        assert_eq!("jsonl".parse::<OutputFormat>(), Ok(OutputFormat::Jsonl));
        assert!("yaml".parse::<OutputFormat>().is_err());
    }

    #[test]
    fn json_output_is_pretty_and_newline_terminated_when_written() {
        let mut bytes = Vec::new();

        OutputFormat::Json
            .write(&mut bytes, &json!({"ok": true}), || "ok".to_string())
            .expect("write succeeds");

        let output = String::from_utf8(bytes).expect("utf8 output");
        assert!(output.contains("\"ok\": true"));
        assert!(output.ends_with('\n'));
    }

    #[test]
    fn jsonl_output_is_single_line() {
        let rendered = OutputFormat::Jsonl
            .render(&json!({"ok": true}), || "ok".to_string())
            .expect("render succeeds");

        assert_eq!(rendered, "{\"ok\":true}\n");
    }

    #[test]
    fn common_exit_code_values_are_stable() {
        assert_eq!(CommonExitCode::Success.code(), 0);
        assert_eq!(CommonExitCode::InvalidInput.code(), 2);
        assert_eq!(CommonExitCode::Denied.code(), 10);
        assert_eq!(CommonExitCode::Interrupted.code(), 130);
    }

    #[test]
    fn common_exit_code_names_are_stable() {
        assert_eq!(CommonExitCode::Success.as_str(), "success");
        assert_eq!(CommonExitCode::InvalidInput.as_str(), "invalid_input");
        assert_eq!(
            CommonExitCode::ApprovalRequired.as_str(),
            "approval_required"
        );
        assert_eq!(CommonExitCode::Runtime.as_str(), "runtime");
        assert_eq!(
            serde_json::to_value(CommonExitCode::ApprovalRequired).expect("serialize exit code"),
            json!("approval_required")
        );
    }

    #[test]
    fn output_shape_enforces_json_for_finite_and_jsonl_for_streams() {
        assert!(
            OutputFormat::Text
                .validate_shape(OutputShape::Finite)
                .is_ok()
        );
        assert!(
            OutputFormat::Text
                .validate_shape(OutputShape::Stream)
                .is_ok()
        );
        assert!(
            OutputFormat::Json
                .validate_shape(OutputShape::Finite)
                .is_ok()
        );
        assert!(
            OutputFormat::Json
                .validate_shape(OutputShape::Stream)
                .is_err()
        );
        assert!(
            OutputFormat::Jsonl
                .validate_shape(OutputShape::Stream)
                .is_ok()
        );
        assert!(
            OutputFormat::Jsonl
                .validate_shape(OutputShape::Finite)
                .is_err()
        );
    }

    #[test]
    fn command_timeout_parses_stable_integer_units_and_applies_bounds() {
        assert_eq!(
            "500ms"
                .parse::<CommandTimeout>()
                .expect("milliseconds")
                .get(),
            Duration::from_millis(500)
        );
        assert_eq!(
            "30s".parse::<CommandTimeout>().expect("seconds").get(),
            Duration::from_secs(30)
        );
        assert_eq!(
            "2m".parse::<CommandTimeout>().expect("minutes").get(),
            Duration::from_secs(120)
        );
        assert!("0s".parse::<CommandTimeout>().is_err());
        assert!("1.5s".parse::<CommandTimeout>().is_err());
        assert!("10".parse::<CommandTimeout>().is_err());
        assert!(CommandTimeout::parse_with_maximum("2m", Duration::from_secs(60)).is_err());
    }

    #[test]
    fn command_timeout_deserialization_enforces_constructor_invariants() {
        let zero = json!({"secs": 0, "nanos": 0});
        let excessive = json!({"secs": MAX_COMMON_TIMEOUT.as_secs() + 1, "nanos": 0});
        let valid = json!({"secs": 30, "nanos": 0});

        assert!(serde_json::from_value::<CommandTimeout>(zero).is_err());
        assert!(serde_json::from_value::<CommandTimeout>(excessive).is_err());
        assert_eq!(
            serde_json::from_value::<CommandTimeout>(valid)
                .expect("valid timeout")
                .get(),
            Duration::from_secs(30)
        );
    }
}
