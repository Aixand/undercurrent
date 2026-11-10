// SPDX-License-Identifier: Apache-2.0

//! Distributed tracing context frozen by UCMP 0.1.0-beta.
//!
//! This is transport metadata only: it generates no IDs or spans and exports no
//! telemetry. Trace and span IDs use generic identifier validation, not W3C
//! hexadecimal width constraints. Optional fields are omitted from JSON and
//! use explicit presence bytes in UCMP binary request/activation frames.

use serde::{Deserialize, Serialize};

use crate::{CorrelationId, SpanId, TraceId};

/// Distributed trace context carried by requests and stream activation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceContext {
    /// Distributed trace identifier.
    pub trace_id: TraceId,
    /// Current span identifier.
    pub span_id: SpanId,
    /// Optional parent span identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_span_id: Option<SpanId>,
    /// Optional correlation identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<CorrelationId>,
}

impl TraceContext {
    /// Creates context with a trace and current span.
    pub fn new(trace_id: TraceId, span_id: SpanId) -> Self {
        Self {
            trace_id,
            span_id,
            parent_span_id: None,
            correlation_id: None,
        }
    }

    /// Adds a parent span identifier.
    pub fn with_parent_span_id(mut self, parent_span_id: SpanId) -> Self {
        self.parent_span_id = Some(parent_span_id);
        self
    }

    /// Adds a correlation identifier.
    pub fn with_correlation_id(mut self, correlation_id: CorrelationId) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }
}
