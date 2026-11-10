// SPDX-License-Identifier: Apache-2.0

//! Health status kept independent from lifecycle and foreground observation.

use std::collections::BTreeSet;
use std::fmt::{Debug, Display, Formatter};
use std::time::Duration;

use crate::{
    CommandSpec, DomainError, DomainResult, ExecutionLimits, InvalidValueReason, TimestampMillis,
};

/// Stable kind registry for configured health checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HealthRuleKind {
    /// Connect to a local TCP port.
    TcpPort,
    /// Perform an HTTP request through an adapter.
    HttpEndpoint,
    /// Require a pattern in recent terminal output.
    OutputContains,
    /// Fail while a pattern is present in recent terminal output.
    OutputRejects,
    /// Fail while red foreground cells meet a threshold.
    RedCellThreshold,
    /// Execute a bounded external check command.
    Command,
}

impl HealthRuleKind {
    /// Complete health-rule registry in stable declaration order.
    pub const ALL: [Self; 6] = [
        Self::TcpPort,
        Self::HttpEndpoint,
        Self::OutputContains,
        Self::OutputRejects,
        Self::RedCellThreshold,
        Self::Command,
    ];

    /// Returns the stable snake-case configuration and diagnostic name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TcpPort => "tcp_port",
            Self::HttpEndpoint => "http_endpoint",
            Self::OutputContains => "output_contains",
            Self::OutputRejects => "output_rejects",
            Self::RedCellThreshold => "red_cell_threshold",
            Self::Command => "command",
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum HealthRuleSpec {
    TcpPort(u16),
    Text(String),
    RedCellThreshold(u32),
    Command(CommandSpec),
}

/// One validated health-check declaration.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HealthRule {
    kind: HealthRuleKind,
    spec: HealthRuleSpec,
}

impl HealthRule {
    /// Creates a non-zero local TCP-port check.
    ///
    /// # Errors
    ///
    /// Returns a validation error for port zero. This function performs no IO
    /// and does not panic.
    pub fn tcp_port(port: u16) -> DomainResult<Self> {
        if port == 0 {
            return Err(DomainError::InvalidValue {
                field: "health_tcp_port",
                reason: InvalidValueReason::OutOfRange {
                    min: 1,
                    max: u64::from(u16::MAX),
                },
            });
        }
        Ok(Self {
            kind: HealthRuleKind::TcpPort,
            spec: HealthRuleSpec::TcpPort(port),
        })
    }

    /// Creates a bounded HTTP-endpoint check.
    ///
    /// Endpoint syntax and authorization are validated by the HTTP adapter;
    /// this domain constructor enforces only non-empty, NUL-free bounded text.
    ///
    /// # Errors
    ///
    /// Returns a validation or capacity error for invalid input.
    pub fn http_endpoint(value: impl Into<String>, max_bytes: usize) -> DomainResult<Self> {
        Self::text(HealthRuleKind::HttpEndpoint, value.into(), max_bytes)
    }

    /// Creates a bounded output-presence check.
    ///
    /// Pattern syntax is compiled by the detection adapter. This function
    /// performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a validation or capacity error for invalid input.
    pub fn output_contains(value: impl Into<String>, max_bytes: usize) -> DomainResult<Self> {
        Self::text(HealthRuleKind::OutputContains, value.into(), max_bytes)
    }

    /// Creates a bounded output-rejection check.
    ///
    /// Pattern syntax is compiled by the detection adapter. This function
    /// performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a validation or capacity error for invalid input.
    pub fn output_rejects(value: impl Into<String>, max_bytes: usize) -> DomainResult<Self> {
        Self::text(HealthRuleKind::OutputRejects, value.into(), max_bytes)
    }

    /// Creates a non-zero red-cell threshold check.
    ///
    /// # Errors
    ///
    /// Returns a validation error for zero. This function performs no IO and
    /// does not panic.
    pub fn red_cell_threshold(minimum_cells: u32) -> DomainResult<Self> {
        if minimum_cells == 0 {
            return Err(DomainError::InvalidValue {
                field: "health_red_cell_threshold",
                reason: InvalidValueReason::OutOfRange {
                    min: 1,
                    max: u64::from(u32::MAX),
                },
            });
        }
        Ok(Self {
            kind: HealthRuleKind::RedCellThreshold,
            spec: HealthRuleSpec::RedCellThreshold(minimum_cells),
        })
    }

    /// Creates a command-based health check from a validated command.
    ///
    /// This function performs no IO, cannot fail, and does not panic.
    pub const fn command(command: CommandSpec) -> Self {
        Self {
            kind: HealthRuleKind::Command,
            spec: HealthRuleSpec::Command(command),
        }
    }

    /// Returns the stable rule kind.
    pub const fn kind(&self) -> HealthRuleKind {
        self.kind
    }

    /// Returns a TCP port for port rules.
    pub const fn port(&self) -> Option<u16> {
        match self.spec {
            HealthRuleSpec::TcpPort(port) => Some(port),
            _ => None,
        }
    }

    /// Returns bounded textual content for endpoint and output rules.
    ///
    /// Callers must keep endpoint credentials and pattern contents out of
    /// diagnostics.
    pub fn text_value(&self) -> Option<&str> {
        match &self.spec {
            HealthRuleSpec::Text(value) => Some(value),
            _ => None,
        }
    }

    /// Returns a threshold for red-cell rules.
    pub const fn minimum_cells(&self) -> Option<u32> {
        match self.spec {
            HealthRuleSpec::RedCellThreshold(value) => Some(value),
            _ => None,
        }
    }

    /// Returns command specification for command rules.
    pub const fn command_spec(&self) -> Option<&CommandSpec> {
        match &self.spec {
            HealthRuleSpec::Command(command) => Some(command),
            _ => None,
        }
    }

    fn text(kind: HealthRuleKind, value: String, max_bytes: usize) -> DomainResult<Self> {
        if value.is_empty() {
            return Err(DomainError::InvalidValue {
                field: "health_rule_text",
                reason: InvalidValueReason::Empty,
            });
        }
        if max_bytes == 0 || value.len() > max_bytes {
            return Err(DomainError::CapacityExceeded {
                resource: "health_rule_bytes",
                limit: max_bytes,
            });
        }
        if let Some(index) = value.find('\0') {
            return Err(DomainError::InvalidValue {
                field: "health_rule_text",
                reason: InvalidValueReason::InvalidCharacter {
                    index,
                    character: '\0',
                },
            });
        }
        Ok(Self {
            kind,
            spec: HealthRuleSpec::Text(value),
        })
    }
}

impl Debug for HealthRule {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug = formatter.debug_struct("HealthRule");
        debug.field("kind", &self.kind);
        match &self.spec {
            HealthRuleSpec::TcpPort(port) => debug.field("port", port),
            HealthRuleSpec::Text(value) => debug.field("value_bytes", &value.len()),
            HealthRuleSpec::RedCellThreshold(value) => debug.field("minimum_cells", value),
            HealthRuleSpec::Command(command) => debug.field("command", command),
        };
        debug.finish_non_exhaustive()
    }
}

/// Bounded set of checks whose results are combined with logical AND.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HealthRules(Vec<HealthRule>);

impl HealthRules {
    /// Constructs a deduplicated rule set within execution limits.
    ///
    /// Original order is retained because diagnostics refer to stable rule
    /// positions. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a capacity error when supplied rules exceed the configured limit.
    /// Counting before deduplication prevents duplicate-heavy input from
    /// bypassing admission limits.
    pub fn new(
        rules: impl IntoIterator<Item = HealthRule>,
        limits: ExecutionLimits,
    ) -> DomainResult<Self> {
        let limits = limits.validate()?;
        let mut unique = Vec::new();
        let mut seen = BTreeSet::new();
        let mut supplied = 0usize;
        for rule in rules {
            supplied = supplied.saturating_add(1);
            if supplied > limits.max_health_rules || supplied > usize::from(u16::MAX) {
                return Err(DomainError::CapacityExceeded {
                    resource: "health_rules",
                    limit: limits.max_health_rules.min(usize::from(u16::MAX)),
                });
            }
            if seen.insert(rule.clone()) {
                unique.push(rule);
            }
        }
        Ok(Self(unique))
    }

    /// Returns number of configured checks.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether no checks are configured.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates over rules in stable declaration order.
    pub fn iter(&self) -> impl Iterator<Item = &HealthRule> {
        self.0.iter()
    }
}

/// Result of one configured health rule evaluation.
#[derive(Clone, PartialEq, Eq)]
pub struct HealthCheckResult {
    rule_index: u16,
    passed: bool,
    latency: Duration,
    detail: Option<String>,
}

impl HealthCheckResult {
    /// Constructs a result for a stable zero-based rule index.
    ///
    /// Detail is retained for an authorized diagnostic view and redacted from
    /// `Debug`. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns invalid-value failure for a zero detail limit or a capacity error
    /// when detail exceeds that limit. Rule-index validity is checked when the
    /// result joins a [`crate::HealthObservation`].
    pub fn new(
        rule_index: u16,
        passed: bool,
        latency: Duration,
        detail: Option<String>,
        max_detail_bytes: usize,
    ) -> DomainResult<Self> {
        if max_detail_bytes == 0 {
            return Err(DomainError::InvalidValue {
                field: "health_detail_limit",
                reason: InvalidValueReason::OutOfRange { min: 1, max: 1 },
            });
        }
        if detail
            .as_ref()
            .is_some_and(|value| value.len() > max_detail_bytes)
        {
            return Err(DomainError::CapacityExceeded {
                resource: "health_detail_bytes",
                limit: max_detail_bytes,
            });
        }
        Ok(Self {
            rule_index,
            passed,
            latency,
            detail,
        })
    }

    /// Returns zero-based configured-rule index.
    pub const fn rule_index(&self) -> u16 {
        self.rule_index
    }

    /// Returns whether this check passed.
    pub const fn passed(&self) -> bool {
        self.passed
    }

    /// Returns adapter-observed check latency.
    pub const fn latency(&self) -> Duration {
        self.latency
    }

    /// Returns optional diagnostic detail to an authorized caller.
    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }
}

impl Debug for HealthCheckResult {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HealthCheckResult")
            .field("rule_index", &self.rule_index)
            .field("passed", &self.passed)
            .field("latency", &self.latency)
            .field("has_detail", &self.detail.is_some())
            .finish()
    }
}

/// Compact aggregate health status.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HealthStatus {
    /// Every required configured check currently passes.
    Healthy,
    /// At least one required configured check currently fails.
    Unhealthy,
    /// Checks are configured but no current conclusion is available.
    Unknown,
    /// The session has no configured health checks.
    #[default]
    NoCheck,
}

impl Display for HealthStatus {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Healthy => "healthy",
            Self::Unhealthy => "unhealthy",
            Self::Unknown => "unknown",
            Self::NoCheck => "no_check",
        })
    }
}

/// Compact health information suitable for session summaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthSummary {
    status: HealthStatus,
    configured_checks: u16,
    observed_at: Option<TimestampMillis>,
}

impl HealthSummary {
    /// Constructs a health summary while enforcing configuration invariants.
    ///
    /// `no_check` requires zero checks and no observation timestamp. Every other
    /// status requires at least one configured check. The function performs no
    /// IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidValue`] when status and check count do not
    /// agree.
    pub fn new(
        status: HealthStatus,
        configured_checks: u16,
        observed_at: Option<TimestampMillis>,
    ) -> DomainResult<Self> {
        let invalid = match status {
            HealthStatus::NoCheck => configured_checks != 0 || observed_at.is_some(),
            _ => configured_checks == 0,
        };
        if invalid {
            return Err(DomainError::InvalidValue {
                field: "health_summary",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "health status must agree with configured check count",
                },
            });
        }
        Ok(Self {
            status,
            configured_checks,
            observed_at,
        })
    }

    /// Returns the unconfigured health summary for a new session.
    ///
    /// This function performs no IO and does not panic.
    pub const fn unconfigured() -> Self {
        Self {
            status: HealthStatus::NoCheck,
            configured_checks: 0,
            observed_at: None,
        }
    }

    /// Returns aggregate health status.
    pub const fn status(&self) -> HealthStatus {
        self.status
    }

    /// Returns the number of configured checks represented by the summary.
    pub const fn configured_checks(&self) -> u16 {
        self.configured_checks
    }

    /// Returns when checks were most recently observed, if applicable.
    pub const fn observed_at(&self) -> Option<TimestampMillis> {
        self.observed_at
    }
}

impl Default for HealthSummary {
    fn default() -> Self {
        Self::unconfigured()
    }
}
