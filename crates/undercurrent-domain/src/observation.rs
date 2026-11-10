// SPDX-License-Identifier: Apache-2.0

//! Shared evidence vocabulary and detailed health observations.

use std::collections::BTreeSet;

use crate::{
    DomainError, DomainResult, HealthCheckResult, HealthStatus, HealthSummary, InvalidValueReason,
    TimestampMillis,
};

/// Semantic precedence assigned after an adapter translates concrete evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EvidencePriority {
    /// Cooperative state emitted directly by the supervised program.
    Cooperative,
    /// Operating-system foreground process ownership.
    ForegroundProcess,
    /// Direct terminal output or screen evidence.
    Terminal,
    /// Terminal motion observed between two scheduled screen checkpoints.
    TerminalHistory,
    /// Process resource, descendant, or lifecycle evidence.
    Process,
    /// Weak absence-based or fallback inference.
    Inference,
}

impl EvidencePriority {
    /// Complete priority registry in strongest-to-weakest order.
    pub const ALL: [Self; 6] = [
        Self::Cooperative,
        Self::ForegroundProcess,
        Self::Terminal,
        Self::TerminalHistory,
        Self::Process,
        Self::Inference,
    ];

    /// Returns the stable snake-case diagnostic and presentation name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cooperative => "cooperative",
            Self::ForegroundProcess => "foreground_process",
            Self::Terminal => "terminal",
            Self::TerminalHistory => "terminal_history",
            Self::Process => "process",
            Self::Inference => "inference",
        }
    }
}

/// Whether a foreground claim is affirmative evidence or an inference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EvidenceDisposition {
    /// The source affirmatively observed the claimed state.
    Affirmative,
    /// The source inferred the claim from incomplete or absent signals.
    Inference,
    /// The source could not produce a claim during this evaluation.
    Unavailable,
}

impl EvidenceDisposition {
    /// Returns the stable persisted and diagnostic name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Affirmative => "affirmative",
            Self::Inference => "inference",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Why one evidence item did not determine its foreground axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IgnoredEvidenceReason {
    /// The adapter explicitly reported its source unavailable.
    Unavailable,
    /// Evidence predates the configured freshness window.
    Stale,
    /// Evidence claims a future observation time.
    Future,
    /// Confidence is below the configured decision floor.
    BelowConfidence,
    /// Stronger or more confident evidence was selected.
    LowerPrecedence,
}

impl IgnoredEvidenceReason {
    /// Returns the stable diagnostic reason name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::Stale => "stale",
            Self::Future => "future",
            Self::BelowConfidence => "below_confidence",
            Self::LowerPrecedence => "lower_precedence",
        }
    }
}

/// Decision disposition retained for one evidence item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EvidenceAssessment {
    /// This evidence determined the committed snapshot.
    Selected,
    /// This evidence was retained but did not determine the snapshot.
    Ignored(IgnoredEvidenceReason),
}

impl EvidenceAssessment {
    /// Returns the stable selected or ignored diagnostic name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Selected => "selected",
            Self::Ignored(IgnoredEvidenceReason::Unavailable) => "ignored_unavailable",
            Self::Ignored(IgnoredEvidenceReason::Stale) => "ignored_stale",
            Self::Ignored(IgnoredEvidenceReason::Future) => "ignored_future",
            Self::Ignored(IgnoredEvidenceReason::BelowConfidence) => "ignored_below_confidence",
            Self::Ignored(IgnoredEvidenceReason::LowerPrecedence) => "ignored_lower_precedence",
        }
    }
}

/// Detailed results and compact summary from one health evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthObservation {
    summary: HealthSummary,
    results: Vec<HealthCheckResult>,
}

impl HealthObservation {
    /// Returns the unconfigured health observation.
    pub const fn unconfigured() -> Self {
        Self {
            summary: HealthSummary::unconfigured(),
            results: Vec::new(),
        }
    }

    /// Wraps a compact summary without detailed check results.
    ///
    /// This supports explicit summary updates while ensuring prior detail does
    /// not falsely appear current. It performs no IO and does not panic.
    pub const fn from_summary(summary: HealthSummary) -> Self {
        Self {
            summary,
            results: Vec::new(),
        }
    }

    /// Rehydrates one persisted health observation and canonicalizes result order.
    ///
    /// Summary-only observations remain valid with an empty result set. When
    /// detail is present, the configured count, indexes, outcome, and observed
    /// timestamp are recomputed and must exactly match the persisted summary.
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns validation failure for malformed results or corrupt-state failure
    /// when the recomputed summary differs.
    pub fn rehydrate(
        summary: HealthSummary,
        results: Vec<HealthCheckResult>,
    ) -> DomainResult<Self> {
        if results.is_empty() {
            return Ok(Self::from_summary(summary));
        }
        let observed_at = summary.observed_at().ok_or(DomainError::CorruptState {
            scope: "health_observation",
            rule: "detailed health results require an observation timestamp",
        })?;
        let observation = Self::evaluate(summary.configured_checks(), results, observed_at)?;
        if observation.summary != summary {
            return Err(DomainError::CorruptState {
                scope: "health_observation",
                rule: "health results must exactly support the compact summary",
            });
        }
        Ok(observation)
    }

    /// Validates and aggregates one independently scheduled health evaluation.
    ///
    /// A partial result set is `unknown`, a complete all-pass set is `healthy`,
    /// and any explicit failure is `unhealthy`. Rule indexes must be unique and
    /// within the configured rule count. This function performs no IO and does
    /// not panic.
    ///
    /// # Errors
    ///
    /// Returns validation errors for inconsistent rule counts or indexes.
    pub fn evaluate(
        configured_checks: u16,
        mut results: Vec<HealthCheckResult>,
        observed_at: TimestampMillis,
    ) -> DomainResult<Self> {
        if configured_checks == 0 {
            if results.is_empty() {
                return Ok(Self::unconfigured());
            }
            return Err(DomainError::InvalidValue {
                field: "health_results",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "unconfigured health cannot contain check results",
                },
            });
        }
        if results.len() > usize::from(configured_checks) {
            return Err(DomainError::CapacityExceeded {
                resource: "health_results",
                limit: usize::from(configured_checks),
            });
        }
        let mut indexes = BTreeSet::new();
        for result in &results {
            if result.rule_index() >= configured_checks || !indexes.insert(result.rule_index()) {
                return Err(DomainError::InvalidValue {
                    field: "health_rule_index",
                    reason: InvalidValueReason::InvalidCombination {
                        rule: "health result indexes must be unique and configured",
                    },
                });
            }
        }
        results.sort_unstable_by_key(HealthCheckResult::rule_index);
        let status = if results.iter().any(|result| !result.passed()) {
            HealthStatus::Unhealthy
        } else if results.len() == usize::from(configured_checks) {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unknown
        };
        Ok(Self {
            summary: HealthSummary::new(status, configured_checks, Some(observed_at))?,
            results,
        })
    }

    /// Returns compact health state used by prepared session summaries.
    pub const fn summary(&self) -> &HealthSummary {
        &self.summary
    }

    /// Returns bounded detailed rule results for an authorized diagnostic view.
    pub fn results(&self) -> &[HealthCheckResult] {
        &self.results
    }
}

impl Default for HealthObservation {
    fn default() -> Self {
        Self::unconfigured()
    }
}
