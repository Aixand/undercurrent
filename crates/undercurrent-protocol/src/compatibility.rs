// SPDX-License-Identifier: Apache-2.0

//! Validation for the sole supported 0.1.0-beta product contract.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};

use serde_json::Value;

use crate::{
    ContractPayloadKind, MutationContext, ProductContractDto, ProductOperation, ProductRequest,
};

/// A current-0.1.0-beta product contract validation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProductContractError {
    /// Mutation metadata did not include a product contract.
    MissingMetadata,
    /// The capability does not match the requested operation.
    CapabilityMismatch,
    /// The schema does not match the requested payload kind.
    SchemaMismatch,
    /// The schema version is not the sole current schema generation.
    SchemaVersionMismatch,
    /// The contract version is not the sole current 0.1.0-beta contract version.
    ContractVersionMismatch,
    /// An additive field attempted to control a security-sensitive concern.
    SecurityControllingUnknownField(UnknownSecurityControllingField),
}

impl Display for ProductContractError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingMetadata => formatter.write_str("product contract metadata is required"),
            Self::CapabilityMismatch => {
                formatter.write_str("product contract capability does not match the operation")
            }
            Self::SchemaMismatch => {
                formatter.write_str("product contract schema does not match the payload kind")
            }
            Self::SchemaVersionMismatch => {
                formatter.write_str("product contract schema generation is not current")
            }
            Self::ContractVersionMismatch => {
                formatter.write_str("product contract version is not current 0.1.0-beta")
            }
            Self::SecurityControllingUnknownField(source) => Display::fmt(source, formatter),
        }
    }
}

impl Error for ProductContractError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SecurityControllingUnknownField(source) => Some(source),
            _ => None,
        }
    }
}

/// An additive field whose name could alter a security-sensitive concern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownSecurityControllingField {
    field: String,
}

impl UnknownSecurityControllingField {
    /// Returns the rejected field name.
    pub fn field(&self) -> &str {
        &self.field
    }
}

impl Display for UnknownSecurityControllingField {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "compatible unknown field `{}` cannot control security",
            self.field
        )
    }
}

impl Error for UnknownSecurityControllingField {}

/// Requires metadata to match the sole current 0.1.0-beta contract for an operation and payload kind.
pub fn validate_product_contract(
    contract: &ProductContractDto,
    operation: ProductOperation,
    payload: ContractPayloadKind,
) -> Result<(), ProductContractError> {
    let expected = ProductContractDto::current(operation, payload);

    if contract.capability != expected.capability {
        return Err(ProductContractError::CapabilityMismatch);
    }
    if contract.schema != expected.schema {
        return Err(ProductContractError::SchemaMismatch);
    }
    if contract.schema_version != expected.schema_version {
        return Err(ProductContractError::SchemaVersionMismatch);
    }
    if contract.contract_version != expected.contract_version {
        return Err(ProductContractError::ContractVersionMismatch);
    }

    Ok(())
}

/// Validates the required current-0.1.0-beta request contract and additive fields for a mutation.
pub fn validate_mutation_contract(
    mutation: &MutationContext,
    operation: ProductOperation,
) -> Result<(), ProductContractError> {
    let contract = mutation
        .contract
        .as_ref()
        .ok_or(ProductContractError::MissingMetadata)?;
    validate_product_contract(contract, operation, ContractPayloadKind::Request)?;
    validate_compatible_unknown_fields(&mutation.compatible_unknown)
        .map_err(ProductContractError::SecurityControllingUnknownField)
}

/// Validates the contract carried by every semantic mutation request.
///
/// Repair remains intentionally outside [`MutationContext`]: it does not accept
/// peer-supplied attempt fences, effect certainty, or result claims. Its direct
/// descriptor is nevertheless required to match the same exact current product
/// contract before the daemon performs authorization or retained-evidence IO.
pub fn validate_product_request_contract(
    request: &ProductRequest,
) -> Result<(), ProductContractError> {
    let operation = request.operation();
    match request {
        ProductRequest::RepairOperationAttempt(repair) => repair.validate_contract(),
        ProductRequest::GetRuntimeAttemptStatus(status) => status.validate_contract(),
        ProductRequest::GetRuntimeCancellationStatus(status) => status.validate_contract(),
        _ if !operation.is_mutating() => Ok(()),
        _ => validate_mutation_contract(
            request
                .mutation()
                .ok_or(ProductContractError::MissingMetadata)?,
            operation,
        ),
    }
}

/// Accepts additive fields only when their names cannot control security-sensitive behavior.
pub fn validate_compatible_unknown_fields(
    fields: &BTreeMap<String, Value>,
) -> Result<(), UnknownSecurityControllingField> {
    const SECURITY_PREFIXES: [&str; 7] = [
        "auth",
        "authority",
        "credential",
        "permission",
        "secret",
        "security",
        "tenant",
    ];

    // This reserves top-level extension names only; it neither inspects nested
    // values nor grants authority. Consumers must not interpret retained unknown
    // fields as security policy. See [extension rules](../../../docs/reference/protocol/encoding.md#unknown-fields-and-mutation-metadata).
    for field in fields.keys() {
        let normalized = field.to_ascii_lowercase();
        let security_controlling = SECURITY_PREFIXES.iter().any(|prefix| {
            normalized == *prefix
                || normalized.strip_prefix(prefix).is_some_and(|suffix| {
                    suffix.starts_with('.') || suffix.starts_with('_') || suffix.starts_with('-')
                })
        });
        if security_controlling {
            return Err(UnknownSecurityControllingField {
                field: field.clone(),
            });
        }
    }

    Ok(())
}
