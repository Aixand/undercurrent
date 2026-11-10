// SPDX-License-Identifier: Apache-2.0

//! Typed constructors for stable diagnostic rows and values.

use crate::{
    DaemonDiagnosticField, DaemonDiagnosticRow, DaemonDiagnosticSection, DaemonDiagnosticValue,
};

/// Builds a diagnostic row from already validated identities and fields.
/// Callers must bound values and exclude private payloads; this constructor
/// performs no content sanitization or byte-limit check. See
/// [source authoring](../../docs/internals/diagnostics.md#add-or-change-a-source).
pub(super) fn row(
    section: DaemonDiagnosticSection,
    identity: impl Into<String>,
    fields: impl IntoIterator<Item = (DaemonDiagnosticField, DaemonDiagnosticValue)>,
) -> DaemonDiagnosticRow {
    DaemonDiagnosticRow {
        section,
        identity: identity.into(),
        fields: fields.into_iter().collect(),
    }
}

/// Wraps already validated diagnostic text.
/// Callers must bound the text and exclude private payloads; this constructor
/// performs no content sanitization or byte-limit check. See
/// [source authoring](../../docs/internals/diagnostics.md#add-or-change-a-source).
pub(super) fn text(
    field: DaemonDiagnosticField,
    value: impl Into<String>,
) -> (DaemonDiagnosticField, DaemonDiagnosticValue) {
    (field, DaemonDiagnosticValue::Text(value.into()))
}

/// Converts an architecture-sized aggregate into its wire-safe representation.
pub(super) fn count(
    field: DaemonDiagnosticField,
    value: usize,
) -> (DaemonDiagnosticField, DaemonDiagnosticValue) {
    (
        field,
        DaemonDiagnosticValue::Unsigned(u64::try_from(value).unwrap_or(u64::MAX)),
    )
}

/// Builds an unsigned diagnostic field without conversion.
pub(super) const fn number(
    field: DaemonDiagnosticField,
    value: u64,
) -> (DaemonDiagnosticField, DaemonDiagnosticValue) {
    (field, DaemonDiagnosticValue::Unsigned(value))
}

/// Builds a signed diagnostic value such as an epoch timestamp.
pub(super) const fn signed(
    field: DaemonDiagnosticField,
    value: i64,
) -> (DaemonDiagnosticField, DaemonDiagnosticValue) {
    (field, DaemonDiagnosticValue::Signed(value))
}

/// Builds a Boolean diagnostic field.
pub(super) const fn boolean(
    field: DaemonDiagnosticField,
    value: bool,
) -> (DaemonDiagnosticField, DaemonDiagnosticValue) {
    (field, DaemonDiagnosticValue::Boolean(value))
}
