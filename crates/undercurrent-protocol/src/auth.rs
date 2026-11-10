// SPDX-License-Identifier: Apache-2.0

//! Credential-free authenticated identity propagated after transport authentication.

use std::fmt::{Debug, Display, Formatter};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

const MAX_AUTHENTICATED_REFERENCE_BYTES: usize = 256;

/// Invalid bounded authenticated identity reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidAuthenticatedReference;

impl Display for InvalidAuthenticatedReference {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(
            "authenticated reference must be non-empty, bounded, and contain no control characters",
        )
    }
}

impl std::error::Error for InvalidAuthenticatedReference {}

macro_rules! authenticated_reference {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            /// Validates an identity reference, never credential material.
            pub fn new(value: impl Into<String>) -> Result<Self, InvalidAuthenticatedReference> {
                let value = value.into();
                if value.is_empty()
                    || value.len() > MAX_AUTHENTICATED_REFERENCE_BYTES
                    || value.chars().any(char::is_control)
                {
                    return Err(InvalidAuthenticatedReference);
                }
                Ok(Self(value))
            }

            /// Returns the authenticated reference.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Debug for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("byte_len", &self.0.len())
                    .finish_non_exhaustive()
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
            }
        }
    };
}

authenticated_reference!(
    AuthenticatedPrincipalRef,
    "Authenticated principal reference established by transport policy."
);
authenticated_reference!(TenantRefDto, "Optional authenticated tenant reference.");
authenticated_reference!(
    AuthorityRefDto,
    "Authority decision or grant reference, never a bearer credential."
);

/// Identity context constructed only after transport authentication succeeds.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthenticatedRequestContext {
    principal: AuthenticatedPrincipalRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tenant: Option<TenantRefDto>,
    authority: AuthorityRefDto,
}

impl AuthenticatedRequestContext {
    /// Constructs credential-free context for daemon dispatch and authorization.
    pub const fn new(
        principal: AuthenticatedPrincipalRef,
        tenant: Option<TenantRefDto>,
        authority: AuthorityRefDto,
    ) -> Self {
        Self {
            principal,
            tenant,
            authority,
        }
    }

    /// Returns authenticated principal reference.
    pub const fn principal(&self) -> &AuthenticatedPrincipalRef {
        &self.principal
    }
    /// Returns optional authenticated tenant reference.
    pub const fn tenant(&self) -> Option<&TenantRefDto> {
        self.tenant.as_ref()
    }
    /// Returns authority decision reference.
    pub const fn authority(&self) -> &AuthorityRefDto {
        &self.authority
    }
}

impl Debug for AuthenticatedRequestContext {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthenticatedRequestContext")
            .field("principal", &self.principal)
            .field("tenant_present", &self.tenant.is_some())
            .field("authority", &self.authority)
            .finish()
    }
}
