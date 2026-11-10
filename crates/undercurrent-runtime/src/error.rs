// SPDX-License-Identifier: Apache-2.0

//! Shared typed bounds and payload-safe adapter sources.

use std::error::Error;
use std::fmt::{Display, Formatter};

/// Invalid standard-library runtime adapter bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeLimitError {
    field: &'static str,
}

impl RuntimeLimitError {
    pub(crate) const fn zero(field: &'static str) -> Self {
        Self { field }
    }

    /// Returns the stable invalid limit name.
    pub const fn field(self) -> &'static str {
        self.field
    }
}

impl Display for RuntimeLimitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "runtime limit {} must be non-zero", self.field)
    }
}

impl Error for RuntimeLimitError {}

#[derive(Debug)]
pub(crate) struct RuntimeAdapterFailure(pub(crate) &'static str);

impl Display for RuntimeAdapterFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for RuntimeAdapterFailure {}

#[derive(Debug)]
pub(crate) struct RuntimeBoxedFailure {
    source: Box<dyn Error + Send + Sync>,
}

impl RuntimeBoxedFailure {
    pub(crate) fn new(source: Box<dyn Error + Send + Sync>) -> Self {
        Self { source }
    }
}

impl Display for RuntimeBoxedFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("runtime adapter dependency failed")
    }
}

impl Error for RuntimeBoxedFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}
