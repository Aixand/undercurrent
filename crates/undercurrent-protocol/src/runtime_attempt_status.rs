// SPDX-License-Identifier: Apache-2.0

//! Read-only, digest-bound runtime-attempt status DTOs.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::{
    ContractPayloadKind, LifecycleStateDto, ProductContractDto, ProductOperation,
    validate_product_contract,
};

const MAX_ATTEMPT_ID_BYTES: usize = 128;
const MAX_CELL_ID_BYTES: usize = 128;
const MAX_SESSION_ID_BYTES: usize = 256;
const SHA256_HEX_CHARS: usize = 64;

/// Exact attached lookup for one caller-bound runtime start attempt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GetRuntimeAttemptStatusRequest {
    attempt_id: String,
    request_digest_sha256: String,
    contract: ProductContractDto,
}

impl GetRuntimeAttemptStatusRequest {
    /// Constructs the sole supported attached status lookup.
    pub fn attached(
        attempt_id: String,
        request_digest_sha256: String,
    ) -> Result<Self, RuntimeAttemptStatusDtoError> {
        validate_attempt_id(&attempt_id)?;
        validate_digest(
            &request_digest_sha256,
            RuntimeAttemptStatusDtoError::InvalidRequestDigest,
        )?;
        Ok(Self {
            attempt_id,
            request_digest_sha256,
            contract: ProductContractDto::current(
                ProductOperation::GetRuntimeAttemptStatus,
                ContractPayloadKind::Request,
            ),
        })
    }

    /// Returns the exact durable attempt identity.
    pub fn attempt_id(&self) -> &str {
        &self.attempt_id
    }

    /// Returns the caller's canonical immutable runtime-start request digest.
    pub fn request_digest_sha256(&self) -> &str {
        &self.request_digest_sha256
    }

    /// Returns the exact current product-contract descriptor.
    pub const fn contract(&self) -> &ProductContractDto {
        &self.contract
    }

    /// Requires the sole current status-request contract without state access.
    pub fn validate_contract(&self) -> Result<(), crate::ProductContractError> {
        validate_product_contract(
            &self.contract,
            ProductOperation::GetRuntimeAttemptStatus,
            ContractPayloadKind::Request,
        )
    }

    /// Validates a request decoded through a generic transport codec.
    pub fn validate(&self) -> Result<(), RuntimeAttemptStatusDtoError> {
        self.validate_contract()
            .map_err(|_| RuntimeAttemptStatusDtoError::InvalidProductContract)?;
        validate_attempt_id(&self.attempt_id)?;
        validate_digest(
            &self.request_digest_sha256,
            RuntimeAttemptStatusDtoError::InvalidRequestDigest,
        )
    }
}

/// Safe immutable dispatch identity returned for caller-side verification.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeAttemptBindingDto {
    caller_cell_identity: String,
    request_digest_sha256: String,
    launch_profile_digest_sha256: String,
    environment_binding_digest_sha256: String,
    input_digest_sha256: String,
}

impl RuntimeAttemptBindingDto {
    /// Constructs a payload-free projection of a current retained dispatch binding.
    pub fn new(
        caller_cell_identity: String,
        request_digest_sha256: String,
        launch_profile_digest_sha256: String,
        environment_binding_digest_sha256: String,
        input_digest_sha256: String,
    ) -> Result<Self, RuntimeAttemptStatusDtoError> {
        validate_safe_identity(
            &caller_cell_identity,
            MAX_CELL_ID_BYTES,
            RuntimeAttemptStatusDtoError::InvalidCallerCellIdentity,
        )?;
        for digest in [
            &request_digest_sha256,
            &launch_profile_digest_sha256,
            &environment_binding_digest_sha256,
            &input_digest_sha256,
        ] {
            validate_digest(digest, RuntimeAttemptStatusDtoError::InvalidBindingDigest)?;
        }
        Ok(Self {
            caller_cell_identity,
            request_digest_sha256,
            launch_profile_digest_sha256,
            environment_binding_digest_sha256,
            input_digest_sha256,
        })
    }

    /// Returns the caller-owned cell identity guarded by the attempt fence.
    pub fn caller_cell_identity(&self) -> &str {
        &self.caller_cell_identity
    }

    /// Returns the exact caller runtime-start request digest.
    pub fn request_digest_sha256(&self) -> &str {
        &self.request_digest_sha256
    }

    /// Returns the exact pinned launch-profile digest.
    pub fn launch_profile_digest_sha256(&self) -> &str {
        &self.launch_profile_digest_sha256
    }

    /// Returns the exact opaque environment-binding digest.
    pub fn environment_binding_digest_sha256(&self) -> &str {
        &self.environment_binding_digest_sha256
    }

    /// Returns the exact pinned input-set digest.
    pub fn input_digest_sha256(&self) -> &str {
        &self.input_digest_sha256
    }

    /// Validates a binding decoded through a generic transport codec.
    pub fn validate(&self) -> Result<(), RuntimeAttemptStatusDtoError> {
        Self::new(
            self.caller_cell_identity.clone(),
            self.request_digest_sha256.clone(),
            self.launch_profile_digest_sha256.clone(),
            self.environment_binding_digest_sha256.clone(),
            self.input_digest_sha256.clone(),
        )
        .map(|_| ())
    }
}

/// Minimal coherent session state exposed only for a known-applied start.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeAttemptSessionSnapshotDto {
    session_id: String,
    lifecycle: LifecycleStateDto,
    revision: u64,
    lifecycle_revision: u64,
    observed_at_millis: i64,
}

impl RuntimeAttemptSessionSnapshotDto {
    /// Constructs a validated, payload-free durable session snapshot.
    pub fn new(
        session_id: String,
        lifecycle: LifecycleStateDto,
        revision: u64,
        lifecycle_revision: u64,
        observed_at_millis: i64,
    ) -> Result<Self, RuntimeAttemptStatusDtoError> {
        validate_session_id(&session_id)?;
        if matches!(lifecycle, LifecycleStateDto::Exited { code: None }) {
            return Err(RuntimeAttemptStatusDtoError::MissingExitCode);
        }
        if revision == 0 || lifecycle_revision == 0 || lifecycle_revision > revision {
            return Err(RuntimeAttemptStatusDtoError::InvalidSessionRevision);
        }
        Ok(Self {
            session_id,
            lifecycle,
            revision,
            lifecycle_revision,
            observed_at_millis,
        })
    }

    /// Returns the exact Undercurrent session identity.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Returns the authoritative durable session lifecycle.
    pub const fn lifecycle(&self) -> LifecycleStateDto {
        self.lifecycle
    }

    /// Returns the current durable aggregate revision.
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the latest authoritative lifecycle revision.
    pub const fn lifecycle_revision(&self) -> u64 {
        self.lifecycle_revision
    }

    /// Returns the durable session update time represented by this snapshot.
    pub const fn observed_at_millis(&self) -> i64 {
        self.observed_at_millis
    }

    /// Validates a snapshot decoded through a generic transport codec.
    pub fn validate(&self) -> Result<(), RuntimeAttemptStatusDtoError> {
        Self::new(
            self.session_id.clone(),
            self.lifecycle,
            self.revision,
            self.lifecycle_revision,
            self.observed_at_millis,
        )
        .map(|_| ())
    }
}

/// Exact durable effect snapshot for one runtime-start attempt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum RuntimeAttemptStatusSnapshotDto {
    /// Retained durable state cannot yet establish whether the effect occurred.
    Unknown { observed_at_millis: i64 },
    /// Retained durable state proves the start effect did not occur.
    KnownAbsent { observed_at_millis: i64 },
    /// Retained durable state proves the start and binds its exact session result.
    KnownApplied {
        result_digest_sha256: String,
        observed_at_millis: i64,
        session: RuntimeAttemptSessionSnapshotDto,
    },
    /// A superseded start was cleaned up under an exact newer fence.
    Compensated { observed_at_millis: i64 },
}

impl RuntimeAttemptStatusSnapshotDto {
    /// Validates a discriminated snapshot decoded through a generic codec.
    pub fn validate(&self) -> Result<(), RuntimeAttemptStatusDtoError> {
        match self {
            Self::Unknown { .. } | Self::KnownAbsent { .. } | Self::Compensated { .. } => Ok(()),
            Self::KnownApplied {
                result_digest_sha256,
                session,
                ..
            } => {
                validate_digest(
                    result_digest_sha256,
                    RuntimeAttemptStatusDtoError::InvalidResultDigest,
                )?;
                session.validate()
            }
        }
    }

    /// Returns the attempt-state observation time.
    pub const fn observed_at_millis(&self) -> i64 {
        match self {
            Self::Unknown { observed_at_millis }
            | Self::KnownAbsent { observed_at_millis }
            | Self::KnownApplied {
                observed_at_millis, ..
            }
            | Self::Compensated { observed_at_millis } => *observed_at_millis,
        }
    }

    /// Returns the exact start-result digest only for KnownApplied.
    pub fn result_digest_sha256(&self) -> Option<&str> {
        match self {
            Self::KnownApplied {
                result_digest_sha256,
                ..
            } => Some(result_digest_sha256),
            _ => None,
        }
    }

    /// Returns the exact session snapshot only for KnownApplied.
    pub const fn session(&self) -> Option<&RuntimeAttemptSessionSnapshotDto> {
        match self {
            Self::KnownApplied { session, .. } => Some(session),
            _ => None,
        }
    }
}

/// Successful, authorization-gated status result for one exact attached attempt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeAttemptStatusResultDto {
    attempt_id: String,
    fencing_token: u64,
    binding: RuntimeAttemptBindingDto,
    snapshot: RuntimeAttemptStatusSnapshotDto,
}

impl RuntimeAttemptStatusResultDto {
    /// Constructs a complete internally consistent public result.
    pub fn new(
        attempt_id: String,
        fencing_token: u64,
        binding: RuntimeAttemptBindingDto,
        snapshot: RuntimeAttemptStatusSnapshotDto,
    ) -> Result<Self, RuntimeAttemptStatusDtoError> {
        validate_attempt_id(&attempt_id)?;
        if fencing_token == 0 {
            return Err(RuntimeAttemptStatusDtoError::InvalidFencingToken);
        }
        binding.validate()?;
        snapshot.validate()?;
        Ok(Self {
            attempt_id,
            fencing_token,
            binding,
            snapshot,
        })
    }

    /// Returns the exact durable attempt identity.
    pub fn attempt_id(&self) -> &str {
        &self.attempt_id
    }

    /// Returns the exact non-zero caller fence.
    pub const fn fencing_token(&self) -> u64 {
        self.fencing_token
    }

    /// Returns the safe immutable caller binding projection.
    pub const fn binding(&self) -> &RuntimeAttemptBindingDto {
        &self.binding
    }

    /// Returns the strict durable effect snapshot.
    pub const fn snapshot(&self) -> &RuntimeAttemptStatusSnapshotDto {
        &self.snapshot
    }

    /// Validates a result decoded through a generic transport codec.
    pub fn validate(&self) -> Result<(), RuntimeAttemptStatusDtoError> {
        Self::new(
            self.attempt_id.clone(),
            self.fencing_token,
            self.binding.clone(),
            self.snapshot.clone(),
        )
        .map(|_| ())
    }
}

/// Stable validation failures for the runtime-attempt status contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeAttemptStatusDtoError {
    InvalidProductContract,
    InvalidAttemptId,
    InvalidRequestDigest,
    InvalidCallerCellIdentity,
    InvalidBindingDigest,
    InvalidResultDigest,
    InvalidFencingToken,
    InvalidSessionId,
    InvalidSessionRevision,
    MissingExitCode,
}

impl Display for RuntimeAttemptStatusDtoError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidProductContract => "invalid runtime-attempt-status product contract",
            Self::InvalidAttemptId => "invalid operation attempt identifier",
            Self::InvalidRequestDigest => "invalid canonical request digest",
            Self::InvalidCallerCellIdentity => "invalid caller cell identity",
            Self::InvalidBindingDigest => "invalid canonical dispatch-binding digest",
            Self::InvalidResultDigest => "invalid canonical start-result digest",
            Self::InvalidFencingToken => "runtime-attempt fencing token must be non-zero",
            Self::InvalidSessionId => "invalid runtime session identifier",
            Self::InvalidSessionRevision => "invalid runtime session revisions",
            Self::MissingExitCode => "exited runtime session requires an exact exit code",
        })
    }
}

impl Error for RuntimeAttemptStatusDtoError {}

fn validate_attempt_id(value: &str) -> Result<(), RuntimeAttemptStatusDtoError> {
    validate_safe_identity(
        value,
        MAX_ATTEMPT_ID_BYTES,
        RuntimeAttemptStatusDtoError::InvalidAttemptId,
    )
}

fn validate_session_id(value: &str) -> Result<(), RuntimeAttemptStatusDtoError> {
    if value.is_empty()
        || value.len() > MAX_SESSION_ID_BYTES
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(RuntimeAttemptStatusDtoError::InvalidSessionId);
    }
    Ok(())
}

fn validate_safe_identity(
    value: &str,
    maximum: usize,
    error: RuntimeAttemptStatusDtoError,
) -> Result<(), RuntimeAttemptStatusDtoError> {
    if value.is_empty()
        || value.len() > maximum
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
    {
        return Err(error);
    }
    Ok(())
}

fn validate_digest(
    value: &str,
    error: RuntimeAttemptStatusDtoError,
) -> Result<(), RuntimeAttemptStatusDtoError> {
    if value.len() != SHA256_HEX_CHARS
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    #[test]
    fn attached_request_and_known_applied_result_are_file_backed_and_strict() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/runtime_attempt_status.json"
        ))
        .expect("runtime-attempt status fixture JSON");
        let request: GetRuntimeAttemptStatusRequest =
            serde_json::from_value(fixture["request"].clone()).expect("status request");
        request.validate().expect("valid attached request");
        assert_eq!(request.attempt_id(), "attempt-0001");
        assert_eq!(
            request.contract(),
            &ProductContractDto::current(
                ProductOperation::GetRuntimeAttemptStatus,
                ContractPayloadKind::Request,
            )
        );

        let result: RuntimeAttemptStatusResultDto =
            serde_json::from_value(fixture["result"].clone()).expect("status result");
        result.validate().expect("valid status result");
        assert_eq!(
            result
                .snapshot()
                .session()
                .expect("known-applied session")
                .session_id(),
            "session-0001"
        );
        let encoded = serde_json::to_value(&result).expect("status result JSON");
        assert_no_sensitive_keys(&encoded);
    }

    #[test]
    fn request_requires_digest_and_contract_and_result_rejects_inconsistent_session() {
        let valid = serde_json::json!({
            "attempt_id": "attempt-0001",
            "request_digest_sha256": "1".repeat(64),
            "contract": {
                "capability": "get_runtime_attempt_status",
                "schema": "undercurrent.request",
                "schema_version": "0.1.0-beta",
                "contract_version": "0.1.0-beta"
            }
        });
        for field in ["request_digest_sha256", "contract"] {
            let mut missing = valid.clone();
            missing
                .as_object_mut()
                .expect("request object")
                .remove(field);
            assert!(serde_json::from_value::<GetRuntimeAttemptStatusRequest>(missing).is_err());
        }
        let mut digestless = valid;
        digestless["request_digest_sha256"] = Value::Null;
        assert!(serde_json::from_value::<GetRuntimeAttemptStatusRequest>(digestless).is_err());

        assert!(
            RuntimeAttemptSessionSnapshotDto::new(
                "session-0001".to_owned(),
                LifecycleStateDto::Running,
                4,
                5,
                42,
            )
            .is_err()
        );
        assert!(
            RuntimeAttemptSessionSnapshotDto::new(
                "session-0001".to_owned(),
                LifecycleStateDto::Exited { code: None },
                5,
                5,
                42,
            )
            .is_err()
        );
    }

    #[test]
    fn every_status_discriminator_round_trips_without_nullable_session_identity() {
        let session = RuntimeAttemptSessionSnapshotDto::new(
            "session-0001".to_owned(),
            LifecycleStateDto::Running,
            5,
            4,
            50,
        )
        .expect("session snapshot");
        let cases = [
            RuntimeAttemptStatusSnapshotDto::Unknown {
                observed_at_millis: 10,
            },
            RuntimeAttemptStatusSnapshotDto::KnownAbsent {
                observed_at_millis: 20,
            },
            RuntimeAttemptStatusSnapshotDto::KnownApplied {
                result_digest_sha256: "2".repeat(64),
                observed_at_millis: 30,
                session,
            },
            RuntimeAttemptStatusSnapshotDto::Compensated {
                observed_at_millis: 40,
            },
        ];
        for case in cases {
            case.validate().expect("valid status");
            let encoded = serde_json::to_value(&case).expect("status JSON");
            assert_eq!(
                serde_json::from_value::<RuntimeAttemptStatusSnapshotDto>(encoded.clone())
                    .expect("status decodes"),
                case
            );
            assert_eq!(
                encoded.get("session").is_some(),
                matches!(case, RuntimeAttemptStatusSnapshotDto::KnownApplied { .. })
            );
        }
    }

    fn assert_no_sensitive_keys(value: &Value) {
        match value {
            Value::Object(fields) => {
                for (field, child) in fields {
                    assert!(
                        !matches!(
                            field.as_str(),
                            "command"
                                | "environment"
                                | "environment_variables"
                                | "variables"
                                | "credential_handles"
                                | "runtime_policy"
                                | "callbacks"
                                | "telemetry"
                                | "raw_output"
                                | "payload_digest"
                                | "payload_digest_sha256"
                        ),
                        "forbidden sensitive field {field}",
                    );
                    assert_no_sensitive_keys(child);
                }
            }
            Value::Array(values) => values.iter().for_each(assert_no_sensitive_keys),
            _ => {}
        }
    }
}
