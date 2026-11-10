// SPDX-License-Identifier: Apache-2.0

//! Bounded process-launch and stop configuration owned by a session.

use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::time::Duration;

use crate::{
    DomainError, DomainResult, HealthRules, InvalidValueReason, Labels, Metadata, PayloadDigest,
    ProfileName, RestartConfig, SecretReference, TerminalSize,
};

/// How a running session binds its effective detection profile.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProfileBindingMode {
    /// The launch-selected base profile remains effective for the process lifetime.
    #[default]
    Fixed,
    /// Foreground process-group evidence may temporarily replace the base profile.
    ForegroundAdaptive,
}

impl ProfileBindingMode {
    /// Complete stable profile-binding mode registry.
    pub const ALL: [Self; 2] = [Self::Fixed, Self::ForegroundAdaptive];

    /// Returns the stable protocol, persistence, and diagnostic name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fixed => "fixed",
            Self::ForegroundAdaptive => "foreground_adaptive",
        }
    }
}

/// Durable provenance of the base detection profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProfileSelectionOrigin {
    /// The caller selected the profile explicitly.
    Explicit,
    /// The launch command matched exactly one enabled profile.
    LaunchCommand,
}

impl ProfileSelectionOrigin {
    /// Complete stable base-profile origin registry.
    pub const ALL: [Self; 2] = [Self::Explicit, Self::LaunchCommand];

    /// Returns the stable protocol, persistence, and diagnostic name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::LaunchCommand => "launch_command",
        }
    }
}

/// Caller-supplied hard limits for launch configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionLimits {
    /// Maximum command arguments excluding the program.
    pub max_arguments: usize,
    /// Maximum total bytes across program and arguments.
    pub max_command_bytes: usize,
    /// Maximum explicit environment entries.
    pub max_environment_entries: usize,
    /// Maximum total bytes across environment keys and values.
    pub max_environment_bytes: usize,
    /// Maximum bytes in a working-directory or environment-file reference.
    pub max_path_bytes: usize,
    /// Maximum configured health rules.
    pub max_health_rules: usize,
    /// Maximum bytes in one textual health-rule specification.
    pub max_health_rule_bytes: usize,
}

impl ExecutionLimits {
    /// Validates that every hard limit is non-zero.
    ///
    /// # Errors
    ///
    /// Returns a validation error if any limit is zero. This function performs
    /// no IO and does not panic.
    pub fn validate(self) -> DomainResult<Self> {
        if self.max_arguments == 0
            || self.max_command_bytes == 0
            || self.max_environment_entries == 0
            || self.max_environment_bytes == 0
            || self.max_path_bytes == 0
            || self.max_health_rules == 0
            || self.max_health_rule_bytes == 0
        {
            return Err(DomainError::InvalidValue {
                field: "execution_limits",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "all execution limits must be non-zero",
                },
            });
        }
        Ok(self)
    }
}

/// How a session executable is launched. Legacy launches remain sealed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LaunchMode {
    /// Execute the saved installation path, revalidating it on each launch.
    Native,
    /// Execute only the immutable bytes accepted for this session.
    #[default]
    Sealed,
}

/// Validated program and argument vector.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommandSpec {
    program: String,
    arguments: Vec<String>,
    total_bytes: usize,
    launch_mode: LaunchMode,
    expected_executable_digest: Option<PayloadDigest>,
    executable_resolution: Option<ExecutableResolution>,
}

impl CommandSpec {
    /// Constructs a command within the supplied execution limits.
    ///
    /// NUL bytes are rejected because operating-system process APIs cannot pass
    /// them. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns validation or capacity errors for invalid or oversized input.
    pub fn new(
        program: impl Into<String>,
        arguments: Vec<String>,
        limits: ExecutionLimits,
    ) -> DomainResult<Self> {
        let limits = limits.validate()?;
        let program = program.into();
        validate_process_text("program", &program, false)?;
        if arguments.len() > limits.max_arguments {
            return Err(DomainError::CapacityExceeded {
                resource: "command_arguments",
                limit: limits.max_arguments,
            });
        }
        let total_bytes = arguments
            .iter()
            .try_fold(program.len(), |total, argument| {
                validate_process_text("command_argument", argument, true)?;
                total
                    .checked_add(argument.len())
                    .ok_or(DomainError::CapacityExceeded {
                        resource: "command_bytes",
                        limit: limits.max_command_bytes,
                    })
            })?;
        if total_bytes > limits.max_command_bytes {
            return Err(DomainError::CapacityExceeded {
                resource: "command_bytes",
                limit: limits.max_command_bytes,
            });
        }
        Ok(Self {
            program,
            arguments,
            total_bytes,
            launch_mode: LaunchMode::Sealed,
            expected_executable_digest: None,
            executable_resolution: None,
        })
    }

    /// Attaches the caller-supplied executable content pin that launch
    /// preparation must verify before durable acceptance.
    ///
    /// This function performs no IO, cannot fail, and does not panic.
    pub const fn with_expected_executable_digest(
        mut self,
        expected_executable_digest: Option<PayloadDigest>,
    ) -> Self {
        self.expected_executable_digest = expected_executable_digest;
        self
    }

    /// Replaces a floating executable reference with one adapter-resolved path
    /// and its immutable resolution receipt.
    ///
    /// The caller must resolve and verify the executable through an outer
    /// filesystem adapter. The domain revalidates all text bounds and rejects
    /// a content digest that conflicts with a caller-supplied pin.
    ///
    /// # Errors
    ///
    /// Returns a validation or capacity error for an invalid resolved path, or
    /// when the resolved content does not match the expected executable digest.
    pub fn pin_executable(
        self,
        resolved_program: impl Into<String>,
        content_digest: PayloadDigest,
        receipt_digest: PayloadDigest,
        limits: ExecutionLimits,
    ) -> DomainResult<Self> {
        self.pin_executable_inner(
            resolved_program,
            content_digest,
            None,
            receipt_digest,
            limits,
        )
    }

    /// Replaces a floating executable reference with a resolved path, content
    /// digest, and opaque filesystem-object identity.
    ///
    /// The identity is produced and interpreted only by the filesystem
    /// adapter. Persisting it lets a later spawn fail closed when it cannot
    /// prove that it reopened the same object accepted during preparation.
    ///
    /// # Errors
    ///
    /// Returns the same validation and capacity errors as
    /// [`Self::pin_executable`].
    pub fn pin_executable_with_identity(
        self,
        resolved_program: impl Into<String>,
        content_digest: PayloadDigest,
        object_identity: PayloadDigest,
        receipt_digest: PayloadDigest,
        limits: ExecutionLimits,
    ) -> DomainResult<Self> {
        self.pin_executable_inner(
            resolved_program,
            content_digest,
            Some(object_identity),
            receipt_digest,
            limits,
        )
    }

    fn pin_executable_inner(
        self,
        resolved_program: impl Into<String>,
        content_digest: PayloadDigest,
        object_identity: Option<PayloadDigest>,
        receipt_digest: PayloadDigest,
        limits: ExecutionLimits,
    ) -> DomainResult<Self> {
        if self
            .expected_executable_digest
            .is_some_and(|expected| expected != content_digest)
        {
            return Err(DomainError::InvalidValue {
                field: "executable_digest",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "resolved executable content must match the supplied digest",
                },
            });
        }
        let requested_reference = self.program;
        let expected_executable_digest = self.expected_executable_digest;
        let mut resolved = Self::new(resolved_program, self.arguments, limits)?;
        resolved.launch_mode = self.launch_mode;
        resolved.expected_executable_digest = expected_executable_digest;
        resolved.executable_resolution = Some(ExecutableResolution {
            requested_reference,
            content_digest,
            object_identity,
            receipt_digest,
        });
        Ok(resolved)
    }

    /// Selects the persisted executable launch policy.
    pub fn with_launch_mode(mut self, mode: LaunchMode) -> Self {
        self.launch_mode = mode;
        self
    }

    /// Returns the persisted executable launch policy.
    pub const fn launch_mode(&self) -> LaunchMode {
        self.launch_mode
    }

    /// Returns the program for an authorized process adapter.
    pub fn program(&self) -> &str {
        &self.program
    }

    /// Returns arguments for an authorized process adapter.
    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }

    /// Returns the caller's executable content pin when one was supplied.
    pub const fn expected_executable_digest(&self) -> Option<PayloadDigest> {
        self.expected_executable_digest
    }

    /// Returns the durable pre-acceptance executable resolution receipt.
    pub const fn executable_resolution(&self) -> Option<&ExecutableResolution> {
        self.executable_resolution.as_ref()
    }

    /// Returns whether executable resolution was recorded before durable acceptance.
    /// Native receipts record acceptance without pinning future executable bytes.
    pub const fn executable_is_pinned(&self) -> bool {
        self.executable_resolution.is_some()
    }

    /// Returns total program and argument bytes used for admission control.
    pub const fn total_bytes(&self) -> usize {
        self.total_bytes
    }
}

impl Debug for CommandSpec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommandSpec")
            .field("launch_mode", &self.launch_mode)
            .field("argument_count", &self.arguments.len())
            .field("total_bytes", &self.total_bytes)
            .field(
                "expected_executable_digest_set",
                &self.expected_executable_digest.is_some(),
            )
            .field("executable_is_pinned", &self.executable_is_pinned())
            .finish_non_exhaustive()
    }
}

/// Durable proof that an outer adapter resolved one executable before session
/// acceptance.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExecutableResolution {
    requested_reference: String,
    content_digest: PayloadDigest,
    object_identity: Option<PayloadDigest>,
    receipt_digest: PayloadDigest,
}

impl ExecutableResolution {
    /// Returns the exact caller-supplied path, program name, or executable URI.
    pub fn requested_reference(&self) -> &str {
        &self.requested_reference
    }

    /// Returns the SHA-256 digest of the executable bytes read during launch
    /// preparation.
    pub const fn content_digest(&self) -> PayloadDigest {
        self.content_digest
    }

    /// Returns the adapter-owned identity of the filesystem object opened
    /// during pre-acceptance verification.
    ///
    /// `None` represents a legacy durable record and must be treated as
    /// insufficient proof by a spawning adapter.
    pub const fn object_identity(&self) -> Option<PayloadDigest> {
        self.object_identity
    }

    /// Returns the canonical receipt digest binding the requested reference,
    /// resolved path, content, and supplied environment binding.
    pub const fn receipt_digest(&self) -> PayloadDigest {
        self.receipt_digest
    }
}

impl Debug for ExecutableResolution {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutableResolution")
            .field("requested_reference_bytes", &self.requested_reference.len())
            .field("content_digest", &self.content_digest)
            .field("object_identity_set", &self.object_identity.is_some())
            .field("receipt_digest", &self.receipt_digest)
            .finish_non_exhaustive()
    }
}

/// Explicit environment variables with redacted diagnostics.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct EnvironmentVariables {
    entries: BTreeMap<String, String>,
    total_bytes: usize,
}

impl EnvironmentVariables {
    /// Validates and constructs explicit environment variables.
    ///
    /// Duplicate keys use the last value. Keys cannot be empty or contain `=`,
    /// and neither keys nor values may contain NUL. This function performs no
    /// IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns validation or capacity errors for invalid or oversized input.
    pub fn new(
        entries: impl IntoIterator<Item = (String, String)>,
        limits: ExecutionLimits,
    ) -> DomainResult<Self> {
        let limits = limits.validate()?;
        let mut values = BTreeMap::new();
        for (key, value) in entries {
            validate_environment_key(&key)?;
            validate_process_text("environment_value", &value, true)?;
            if crate::classify_raw_environment_entry(&key, &value)
                == crate::RawEnvironmentClassification::CredentialClassified
            {
                return Err(DomainError::InvalidValue {
                    field: "environment_value",
                    reason: crate::InvalidValueReason::InvalidCombination {
                        rule: "credential-classified values require an opaque secret reference",
                    },
                });
            }
            values.insert(key, value);
            if values.len() > limits.max_environment_entries {
                return Err(DomainError::CapacityExceeded {
                    resource: "environment_entries",
                    limit: limits.max_environment_entries,
                });
            }
        }
        let total_bytes = values.iter().try_fold(0usize, |total, (key, value)| {
            total
                .checked_add(key.len())
                .and_then(|with_key| with_key.checked_add(value.len()))
                .ok_or(DomainError::CapacityExceeded {
                    resource: "environment_bytes",
                    limit: limits.max_environment_bytes,
                })
        })?;
        if total_bytes > limits.max_environment_bytes {
            return Err(DomainError::CapacityExceeded {
                resource: "environment_bytes",
                limit: limits.max_environment_bytes,
            });
        }
        Ok(Self {
            entries: values,
            total_bytes,
        })
    }

    /// Returns the number of explicit variables without exposing values.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no explicit variables are present.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns total key and value bytes used for admission control.
    pub const fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Iterates over variable names without exposing their values.
    ///
    /// This is the only environment projection suitable for session detail
    /// responses. The function performs no IO and does not panic.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }

    /// Iterates over values for an explicitly authorized process adapter.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.entries
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }

    /// Confirms every retained value is explicitly eligible for non-secret persistence.
    pub fn validate_non_secret(&self) -> DomainResult<()> {
        if self.entries.iter().any(|(key, value)| {
            crate::classify_raw_environment_entry(key, value)
                == crate::RawEnvironmentClassification::CredentialClassified
        }) {
            return Err(DomainError::CorruptState {
                scope: "environment_variables",
                rule: "credential-classified raw value crossed the persistence boundary",
            });
        }
        Ok(())
    }
}

impl Debug for EnvironmentVariables {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EnvironmentVariables")
            .field("entry_count", &self.entries.len())
            .field("total_bytes", &self.total_bytes)
            .finish_non_exhaustive()
    }
}

/// Durable environment-variable names paired only with opaque secret handles.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SecretEnvironmentVariables {
    entries: BTreeMap<String, SecretReference>,
    total_reference_bytes: usize,
}

impl SecretEnvironmentVariables {
    /// Validates opaque secret-variable bindings alongside ordinary environment state.
    ///
    /// Names obey the process environment contract, cannot overlap a persisted
    /// ordinary value, and share the configured entry and metadata-byte bounds.
    /// Secret material is never accepted by this constructor.
    ///
    /// # Errors
    ///
    /// Returns a validation or capacity error for an invalid name, overlap, or
    /// combined environment declaration exceeding its durable bounds.
    pub fn new(
        entries: impl IntoIterator<Item = (String, SecretReference)>,
        ordinary: &EnvironmentVariables,
        limits: ExecutionLimits,
    ) -> DomainResult<Self> {
        let limits = limits.validate()?;
        // Compatibility builders can supply ordinary entries validated under a
        // looser budget. An empty secret iterator must still recheck their count.
        if ordinary.len() > limits.max_environment_entries {
            return Err(DomainError::CapacityExceeded {
                resource: "environment_entries",
                limit: limits.max_environment_entries,
            });
        }
        let mut values = BTreeMap::new();
        for (key, reference) in entries {
            validate_environment_key(&key)?;
            if ordinary.entries.contains_key(&key) {
                return Err(DomainError::InvalidValue {
                    field: "secret_environment_name",
                    reason: InvalidValueReason::InvalidCombination {
                        rule: "a variable cannot have both an ordinary value and a secret reference",
                    },
                });
            }
            values.insert(key, reference);
            if ordinary
                .len()
                .checked_add(values.len())
                .is_none_or(|total| total > limits.max_environment_entries)
            {
                return Err(DomainError::CapacityExceeded {
                    resource: "environment_entries",
                    limit: limits.max_environment_entries,
                });
            }
        }
        let total_reference_bytes = values.iter().try_fold(0usize, |total, (key, reference)| {
            total
                .checked_add(key.len())
                .and_then(|with_key| with_key.checked_add(reference.as_str().len()))
                .ok_or(DomainError::CapacityExceeded {
                    resource: "environment_bytes",
                    limit: limits.max_environment_bytes,
                })
        })?;
        if ordinary
            .total_bytes()
            .checked_add(total_reference_bytes)
            .is_none_or(|total| total > limits.max_environment_bytes)
        {
            return Err(DomainError::CapacityExceeded {
                resource: "environment_bytes",
                limit: limits.max_environment_bytes,
            });
        }
        Ok(Self {
            entries: values,
            total_reference_bytes,
        })
    }

    /// Returns the number of durable opaque bindings.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no opaque bindings are present.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns durable metadata bytes, never resolved secret bytes.
    pub const fn total_reference_bytes(&self) -> usize {
        self.total_reference_bytes
    }

    /// Iterates variable names without exposing opaque handles.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }

    /// Iterates opaque bindings for an explicitly authorized resolver adapter.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &SecretReference)> {
        self.entries
            .iter()
            .map(|(key, reference)| (key.as_str(), reference))
    }

    fn validate_compatibility(
        &self,
        ordinary: &EnvironmentVariables,
        limits: ExecutionLimits,
    ) -> DomainResult<()> {
        Self::new(
            self.entries
                .iter()
                .map(|(key, reference)| (key.clone(), reference.clone())),
            ordinary,
            limits,
        )
        .map(|_| ())
    }
}

impl Debug for SecretEnvironmentVariables {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecretEnvironmentVariables")
            .field("entry_count", &self.entries.len())
            .field("total_reference_bytes", &self.total_reference_bytes)
            .finish_non_exhaustive()
    }
}

/// Opaque filesystem reference interpreted only by an outer adapter.
#[derive(Clone, PartialEq, Eq)]
pub struct PathReference(String);

impl PathReference {
    /// Constructs a non-empty path reference within a caller-supplied limit.
    ///
    /// The domain does not resolve, normalize, read, or otherwise interpret the
    /// path. NUL is rejected for operating-system compatibility.
    ///
    /// # Errors
    ///
    /// Returns a validation or capacity error for invalid input.
    pub fn new(value: impl Into<String>, max_path_bytes: usize) -> DomainResult<Self> {
        let value = value.into();
        validate_process_text("path_reference", &value, false)?;
        if max_path_bytes == 0 || value.len() > max_path_bytes {
            return Err(DomainError::CapacityExceeded {
                resource: "path_reference_bytes",
                limit: max_path_bytes,
            });
        }
        Ok(Self(value))
    }

    /// Returns the opaque value to an authorized filesystem adapter.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Supplied environment boundary used to constrain process working-directory
/// resolution.
#[derive(Clone, PartialEq, Eq)]
pub struct ExecutionEnvironmentBinding {
    digest: PayloadDigest,
    working_directory_root: PathReference,
    root_identity: Option<PayloadDigest>,
}

impl ExecutionEnvironmentBinding {
    /// Constructs an already validated opaque binding digest and filesystem
    /// root. Filesystem normalization remains an outer-adapter responsibility.
    pub const fn new(digest: PayloadDigest, working_directory_root: PathReference) -> Self {
        Self {
            digest,
            working_directory_root,
            root_identity: None,
        }
    }

    /// Attaches the opaque identity of the opened binding-root object.
    pub const fn with_root_identity(mut self, root_identity: Option<PayloadDigest>) -> Self {
        self.root_identity = root_identity;
        self
    }

    /// Returns the immutable supplied environment-binding digest.
    pub const fn digest(&self) -> PayloadDigest {
        self.digest
    }

    /// Returns the root within which a configured working directory must
    /// resolve.
    pub const fn working_directory_root(&self) -> &PathReference {
        &self.working_directory_root
    }

    /// Returns the adapter-owned identity of the opened binding-root object.
    ///
    /// `None` represents a legacy durable record and must fail closed when the
    /// binding is used for process creation.
    pub const fn root_identity(&self) -> Option<PayloadDigest> {
        self.root_identity
    }
}

impl Debug for ExecutionEnvironmentBinding {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionEnvironmentBinding")
            .field("digest", &self.digest)
            .field(
                "working_directory_root_bytes",
                &self.working_directory_root.as_str().len(),
            )
            .field("root_identity_set", &self.root_identity.is_some())
            .finish_non_exhaustive()
    }
}

impl Debug for PathReference {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PathReference")
            .field("byte_len", &self.0.len())
            .finish_non_exhaustive()
    }
}

/// Portable graceful signal intent resolved by an operating-system adapter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum StopSignal {
    /// Interrupt signal, typically `SIGINT`.
    Interrupt,
    /// Termination signal, typically `SIGTERM`.
    #[default]
    Terminate,
    /// Hangup signal, typically `SIGHUP`.
    Hangup,
    /// Quit signal, typically `SIGQUIT`.
    Quit,
    /// First user-defined signal, typically `SIGUSR1`.
    User1,
    /// Second user-defined signal, typically `SIGUSR2`.
    User2,
}

/// Graceful stop behavior before forced termination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopPolicy {
    signal: StopSignal,
    force_after: Duration,
    no_escalation: bool,
}

impl StopPolicy {
    /// Constructs a stop policy with a non-zero forced-termination timeout.
    ///
    /// # Errors
    ///
    /// Returns a validation error for zero timeout. This function performs no
    /// IO and does not panic.
    pub fn new(signal: StopSignal, force_after: Duration) -> DomainResult<Self> {
        if force_after.is_zero() {
            return Err(DomainError::InvalidValue {
                field: "stop_timeout",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "forced-termination timeout must be non-zero",
                },
            });
        }
        Ok(Self {
            signal,
            force_after,
            no_escalation: false,
        })
    }

    /// Controls whether a graceful deadline may escalate to forced termination.
    pub const fn with_no_escalation(mut self, value: bool) -> Self {
        self.no_escalation = value;
        self
    }

    /// Returns whether forced escalation is forbidden.
    pub const fn no_escalation(self) -> bool {
        self.no_escalation
    }

    /// Returns graceful signal intent.
    pub const fn signal(self) -> StopSignal {
        self.signal
    }

    /// Returns delay before forced termination.
    pub const fn force_after(self) -> Duration {
        self.force_after
    }
}

/// Output interpretation selected for one managed process launch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OutputCaptureMode {
    /// Interpret output as terminal data and maintain terminal screen state.
    #[default]
    Terminal,
    /// Preserve output as opaque bytes without terminal interpretation.
    Opaque,
}

/// Immutable configuration required to launch and reproduce a PTY session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchSpec {
    command: CommandSpec,
    environment: EnvironmentVariables,
    secret_environment: SecretEnvironmentVariables,
    environment_file: Option<PathReference>,
    working_directory: Option<PathReference>,
    working_directory_identity: Option<PayloadDigest>,
    environment_binding: Option<ExecutionEnvironmentBinding>,
    terminal_size: TerminalSize,
    output_capture_mode: OutputCaptureMode,
}

impl LaunchSpec {
    /// Constructs a launch specification from independently validated values.
    ///
    /// This function performs no IO, cannot fail, and does not panic.
    pub fn new(
        command: CommandSpec,
        environment: EnvironmentVariables,
        environment_file: Option<PathReference>,
        working_directory: Option<PathReference>,
        terminal_size: TerminalSize,
    ) -> Self {
        Self {
            command,
            environment,
            secret_environment: SecretEnvironmentVariables::default(),
            environment_file,
            working_directory,
            working_directory_identity: None,
            environment_binding: None,
            terminal_size,
            output_capture_mode: OutputCaptureMode::Terminal,
        }
    }

    /// Attaches already-validated durable opaque secret-variable bindings.
    ///
    /// Only handles enter this immutable launch state. Resolution remains an
    /// application/runtime edge responsibility.
    pub fn with_secret_environment(
        mut self,
        secret_environment: SecretEnvironmentVariables,
        limits: ExecutionLimits,
    ) -> DomainResult<Self> {
        secret_environment.validate_compatibility(&self.environment, limits)?;
        self.secret_environment = secret_environment;
        Ok(self)
    }

    /// Attaches the supplied environment boundary used by launch preparation
    /// to constrain the working directory.
    pub fn with_environment_binding(
        mut self,
        environment_binding: Option<ExecutionEnvironmentBinding>,
    ) -> Self {
        self.environment_binding = environment_binding;
        // Directory identity belongs to the previous preparation. Clear it so
        // replacing the boundary also requires preparation to supply directory
        // proof again. See [path identity](../docs/reference/values-and-io.md#paths-and-environment-identity).
        self.working_directory_identity = None;
        self
    }

    /// Replaces the command after an outer launch-preparation adapter has
    /// resolved and pinned its executable.
    pub fn with_command(mut self, command: CommandSpec) -> Self {
        self.command = command;
        self
    }

    /// Replaces filesystem references with adapter-canonicalized values after
    /// all environment-boundary checks succeed.
    pub fn with_resolved_paths(
        mut self,
        working_directory: Option<PathReference>,
        environment_binding: Option<ExecutionEnvironmentBinding>,
    ) -> Self {
        self.working_directory = working_directory;
        // Attach the new identity after replacing paths, as launch preparation
        // does; the old directory's proof cannot validate the replacement.
        self.working_directory_identity = None;
        self.environment_binding = environment_binding;
        self
    }

    /// Attaches the opaque identity of the opened working-directory object.
    pub const fn with_working_directory_identity(
        mut self,
        working_directory_identity: Option<PayloadDigest>,
    ) -> Self {
        self.working_directory_identity = working_directory_identity;
        self
    }

    /// Replaces explicit and file-backed environment inputs with the exact
    /// bounded, non-secret values resolved before durable acceptance.
    pub fn with_resolved_environment(
        mut self,
        environment: EnvironmentVariables,
        limits: ExecutionLimits,
    ) -> DomainResult<Self> {
        self.secret_environment
            .validate_compatibility(&environment, limits)?;
        self.environment = environment;
        self.environment_file = None;
        Ok(self)
    }

    /// Selects how process output is interpreted and retained.
    ///
    /// This function performs no IO, cannot fail, and does not panic.
    pub const fn with_output_capture_mode(
        mut self,
        output_capture_mode: OutputCaptureMode,
    ) -> Self {
        self.output_capture_mode = output_capture_mode;
        self
    }

    /// Returns command specification.
    pub const fn command(&self) -> &CommandSpec {
        &self.command
    }

    /// Returns explicit environment variables.
    pub const fn environment(&self) -> &EnvironmentVariables {
        &self.environment
    }

    /// Returns durable opaque secret-variable bindings.
    pub const fn secret_environment(&self) -> &SecretEnvironmentVariables {
        &self.secret_environment
    }

    /// Returns optional environment-file reference.
    pub const fn environment_file(&self) -> Option<&PathReference> {
        self.environment_file.as_ref()
    }

    /// Returns optional working-directory reference.
    pub const fn working_directory(&self) -> Option<&PathReference> {
        self.working_directory.as_ref()
    }

    /// Returns the adapter-owned identity of the opened working-directory
    /// object. A missing identity on a configured directory denotes a legacy
    /// record and is insufficient for spawn.
    pub const fn working_directory_identity(&self) -> Option<PayloadDigest> {
        self.working_directory_identity
    }

    /// Returns the supplied environment boundary for working-directory
    /// resolution.
    pub const fn environment_binding(&self) -> Option<&ExecutionEnvironmentBinding> {
        self.environment_binding.as_ref()
    }

    /// Returns initial terminal dimensions.
    pub const fn terminal_size(&self) -> TerminalSize {
        self.terminal_size
    }

    /// Returns how process output is interpreted and retained.
    pub const fn output_capture_mode(&self) -> OutputCaptureMode {
        self.output_capture_mode
    }

    /// Replaces the persisted terminal dimensions after the aggregate has
    /// validated and revisioned a resize.
    ///
    /// This crate-private mutation keeps future process restarts aligned with
    /// the current PTY size. It performs no IO and does not panic.
    pub(crate) fn set_terminal_size(&mut self, terminal_size: TerminalSize) {
        self.terminal_size = terminal_size;
    }
}

/// Complete durable process policy associated with one session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionPolicy {
    restart: RestartConfig,
    stop: StopPolicy,
    detection_enabled: bool,
    auto_remove: bool,
}

impl ExecutionPolicy {
    /// Constructs execution policy from validated values.
    ///
    /// This function performs no IO, cannot fail, and does not panic.
    pub const fn new(
        restart: RestartConfig,
        stop: StopPolicy,
        detection_enabled: bool,
        auto_remove: bool,
    ) -> Self {
        Self {
            restart,
            stop,
            detection_enabled,
            auto_remove,
        }
    }

    /// Returns automatic restart configuration.
    pub const fn restart(&self) -> &RestartConfig {
        &self.restart
    }

    /// Returns graceful stop policy.
    pub const fn stop(&self) -> StopPolicy {
        self.stop
    }

    /// Returns whether best-effort foreground observation is enabled.
    pub const fn detection_enabled(&self) -> bool {
        self.detection_enabled
    }

    /// Returns whether terminal sessions are removed automatically after exit.
    pub const fn auto_remove(&self) -> bool {
        self.auto_remove
    }
}

/// Complete durable configuration required to reproduce a managed session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConfiguration {
    launch: LaunchSpec,
    execution: ExecutionPolicy,
    health_rules: HealthRules,
    profile: Option<ProfileName>,
    profile_origin: Option<ProfileSelectionOrigin>,
    profile_binding: ProfileBindingMode,
    labels: Labels,
    metadata: Metadata,
}

impl SessionConfiguration {
    /// Constructs configuration from independently validated domain values.
    ///
    /// This function performs no IO, cannot fail, and does not panic.
    pub const fn new(
        launch: LaunchSpec,
        execution: ExecutionPolicy,
        health_rules: HealthRules,
        profile: Option<ProfileName>,
        labels: Labels,
        metadata: Metadata,
    ) -> Self {
        Self {
            launch,
            execution,
            health_rules,
            profile_origin: match &profile {
                Some(_) => Some(ProfileSelectionOrigin::Explicit),
                None => None,
            },
            profile,
            profile_binding: ProfileBindingMode::Fixed,
            labels,
            metadata,
        }
    }

    /// Persists the explicit graceful-only stop constraint for crash recovery.
    pub fn with_stop_no_escalation(mut self, value: bool) -> Self {
        self.execution.stop = self.execution.stop.with_no_escalation(value);
        self
    }

    /// Returns immutable process launch specification.
    pub const fn launch(&self) -> &LaunchSpec {
        &self.launch
    }

    /// Replaces the launch specification with an adapter-prepared, executable-
    /// pinned form before the session aggregate is created.
    pub fn with_prepared_launch(mut self, launch: LaunchSpec) -> Self {
        self.launch = launch;
        self
    }

    /// Returns restart, stop, detection, and removal policy.
    pub const fn execution(&self) -> &ExecutionPolicy {
        &self.execution
    }

    /// Returns configured health rules.
    pub const fn health_rules(&self) -> &HealthRules {
        &self.health_rules
    }

    /// Returns selected detection profile, if present.
    pub const fn profile(&self) -> Option<&ProfileName> {
        self.profile.as_ref()
    }

    /// Returns how the durable base profile was selected, when one exists.
    pub const fn profile_origin(&self) -> Option<ProfileSelectionOrigin> {
        self.profile_origin
    }

    /// Returns whether effective profile selection is fixed or foreground-adaptive.
    pub const fn profile_binding(&self) -> ProfileBindingMode {
        self.profile_binding
    }

    /// Sets the validated profile-binding policy during request preparation.
    ///
    /// This function performs no IO and does not panic.
    pub fn set_profile_binding(&mut self, mode: ProfileBindingMode) {
        self.profile_binding = mode;
    }

    /// Applies an automatically selected profile only when none was explicit.
    ///
    /// This mutation is intended for request preparation before a session
    /// aggregate is created. It never replaces an operator-selected profile,
    /// performs no IO, and does not panic. The return value reports whether the
    /// profile was applied.
    pub fn select_profile_if_absent(&mut self, profile: ProfileName) -> bool {
        if self.profile.is_some() {
            return false;
        }
        self.profile = Some(profile);
        self.profile_origin = Some(ProfileSelectionOrigin::LaunchCommand);
        true
    }

    /// Restores base-profile provenance and binding from a trusted durable record.
    ///
    /// Profile and provenance must either both be present or both be absent.
    /// Adapters should call this only while reconstructing a validated durable
    /// record. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a validation error when profile presence and origin disagree.
    pub fn restore_profile_binding(
        &mut self,
        origin: Option<ProfileSelectionOrigin>,
        mode: ProfileBindingMode,
    ) -> DomainResult<()> {
        if self.profile.is_some() != origin.is_some() {
            return Err(DomainError::InvalidValue {
                field: "profile_origin",
                reason: InvalidValueReason::InvalidCombination {
                    rule: "profile origin must be present exactly when a base profile is present",
                },
            });
        }
        self.profile_origin = origin;
        self.profile_binding = mode;
        Ok(())
    }

    /// Returns labels used by query selection and display.
    pub const fn labels(&self) -> &Labels {
        &self.labels
    }

    /// Replaces validated labels while preserving every other launch setting.
    /// This function performs no IO and does not panic.
    pub fn with_labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    /// Returns private metadata to an explicitly authorized detail caller.
    pub const fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Replaces current launch dimensions after the owning session aggregate
    /// has accepted a revisioned resize.
    ///
    /// This crate-private function performs no IO and does not panic.
    pub(crate) fn set_terminal_size(&mut self, terminal_size: TerminalSize) {
        self.launch.set_terminal_size(terminal_size);
    }
}

fn validate_process_text(field: &'static str, value: &str, allow_empty: bool) -> DomainResult<()> {
    if !allow_empty && value.is_empty() {
        return Err(DomainError::InvalidValue {
            field,
            reason: InvalidValueReason::Empty,
        });
    }
    if let Some(index) = value.find('\0') {
        return Err(DomainError::InvalidValue {
            field,
            reason: InvalidValueReason::InvalidCharacter {
                index,
                character: '\0',
            },
        });
    }
    Ok(())
}

fn validate_environment_key(value: &str) -> DomainResult<()> {
    validate_process_text("environment_key", value, false)?;
    if let Some(index) = value.find('=') {
        return Err(DomainError::InvalidValue {
            field: "environment_key",
            reason: InvalidValueReason::InvalidCharacter {
                index,
                character: '=',
            },
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launch_spec_defaults_to_terminal_and_accepts_explicit_opaque_capture() {
        let limits = ExecutionLimits {
            max_arguments: 1,
            max_command_bytes: 16,
            max_environment_entries: 1,
            max_environment_bytes: 16,
            max_path_bytes: 16,
            max_health_rules: 1,
            max_health_rule_bytes: 16,
        };
        let launch = LaunchSpec::new(
            CommandSpec::new("true", Vec::new(), limits).expect("command"),
            EnvironmentVariables::new(Vec::new(), limits).expect("environment"),
            None,
            None,
            TerminalSize::new(80, 24, 400, 200).expect("terminal"),
        );

        assert_eq!(launch.output_capture_mode(), OutputCaptureMode::Terminal);
        assert_eq!(
            launch
                .with_output_capture_mode(OutputCaptureMode::Opaque)
                .output_capture_mode(),
            OutputCaptureMode::Opaque
        );
    }

    #[test]
    fn executable_pin_requires_expected_content_and_redacts_paths() {
        let limits = ExecutionLimits {
            max_arguments: 2,
            max_command_bytes: 256,
            max_environment_entries: 1,
            max_environment_bytes: 16,
            max_path_bytes: 256,
            max_health_rules: 1,
            max_health_rule_bytes: 16,
        };
        let expected = PayloadDigest::of_bytes(b"expected executable");
        let command = CommandSpec::new("tool://example-tool/0.1.0-beta", Vec::new(), limits)
            .expect("command")
            .with_expected_executable_digest(Some(expected));
        assert!(
            command
                .clone()
                .pin_executable(
                    "/private/runtime/example-tool",
                    PayloadDigest::of_bytes(b"different executable"),
                    PayloadDigest::of_bytes(b"receipt"),
                    limits,
                )
                .is_err()
        );

        let pinned = command
            .pin_executable_with_identity(
                "/private/runtime/example-tool",
                expected,
                PayloadDigest::of_bytes(b"filesystem object"),
                PayloadDigest::of_bytes(b"receipt"),
                limits,
            )
            .expect("matching executable pins");

        assert!(pinned.executable_is_pinned());
        assert_eq!(
            pinned
                .executable_resolution()
                .expect("resolution")
                .requested_reference(),
            "tool://example-tool/0.1.0-beta"
        );
        assert!(
            pinned
                .executable_resolution()
                .expect("resolution")
                .object_identity()
                .is_some()
        );
        let debug = format!("{pinned:?}");
        assert!(!debug.contains("private"));
        assert!(!debug.contains("example-tool"));
    }
}
