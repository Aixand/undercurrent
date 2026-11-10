// SPDX-License-Identifier: Apache-2.0

//! Outward mapping from application-owned stable strings to product DTOs.

use serde::{Deserialize, Serialize};

/// Product boundary status independent of an inward application crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductBoundaryStatusDto {
    /// Invalid input.
    InvalidInput,
    /// Semantic rejection.
    Rejected,
    /// Authorization denial.
    Denied,
    /// Resource absent.
    NotFound,
    /// Concurrent conflict.
    Conflict,
    /// Capacity rejection.
    CapacityExceeded,
    /// Rate rejection.
    RateLimited,
    /// Cancellation.
    Cancelled,
    /// Deadline elapsed.
    TimedOut,
    /// Interrupted effect.
    Interrupted,
    /// Dependency unavailable.
    Unavailable,
    /// Replay unavailable.
    ReplayUnavailable,
    /// Stale cursor or revision.
    Stale,
    /// Protocol violation.
    ProtocolError,
    /// Unsupported behavior.
    Unsupported,
    /// Internal or invariant failure.
    Failed,
}

/// Maps an application error's stable string at the outward boundary.
pub fn map_application_error_status(value: &str) -> ProductBoundaryStatusDto {
    match value {
        "invalid_argument" => ProductBoundaryStatusDto::InvalidInput,
        "unauthenticated" | "failed_precondition" => ProductBoundaryStatusDto::Rejected,
        "permission_denied" => ProductBoundaryStatusDto::Denied,
        "not_found" => ProductBoundaryStatusDto::NotFound,
        "conflict" => ProductBoundaryStatusDto::Conflict,
        "capacity_exceeded" => ProductBoundaryStatusDto::CapacityExceeded,
        "rate_limited" => ProductBoundaryStatusDto::RateLimited,
        "cancelled" => ProductBoundaryStatusDto::Cancelled,
        "timeout" => ProductBoundaryStatusDto::TimedOut,
        "interrupted" => ProductBoundaryStatusDto::Interrupted,
        "unavailable" => ProductBoundaryStatusDto::Unavailable,
        "replay_unavailable" => ProductBoundaryStatusDto::ReplayUnavailable,
        "cursor_expired" => ProductBoundaryStatusDto::Stale,
        "protocol_violation" => ProductBoundaryStatusDto::ProtocolError,
        "unsupported" => ProductBoundaryStatusDto::Unsupported,
        _ => ProductBoundaryStatusDto::Failed,
    }
}

/// Builds a qualified product observability name from stable registry strings.
pub fn qualified_observability_name(kind: &str, local_name: &str) -> Option<String> {
    let valid = |value: &str| {
        !value.is_empty()
            && value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    };
    (valid(kind) && valid(local_name)).then(|| format!("undercurrent.{kind}.{local_name}"))
}
