// SPDX-License-Identifier: Apache-2.0

//! Bounded JSON codec selected by the negotiated connection protocol.
//!
//! Decode checks byte size before parsing; encode serializes before checking
//! the resulting size. Neither path runs product-contract or domain validation.
//! Call [`crate::validate_product_request_contract`] and any dedicated DTO
//! validation required by the boundary separately.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{
    ProductCodecError, ProductCodecErrorKind, ProductCodecStage, ProductRequest, ProductResponse,
    ProductStreamItem, ProductStreamOpenResponse, ProductStreamSpec,
};

/// Hard limits for every JSON product payload class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductCodecLimits {
    request_bytes: usize,
    response_bytes: usize,
    stream_spec_bytes: usize,
    stream_item_bytes: usize,
}

impl ProductCodecLimits {
    /// Constructs non-zero limits that must fit negotiated frame payloads.
    ///
    /// Only nonzero values are checked here. The enclosing SDK or transport
    /// adapter must ensure that these limits fit its negotiated payload budget.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns the first stable field whose limit is zero.
    pub const fn new(
        request_bytes: usize,
        response_bytes: usize,
        stream_spec_bytes: usize,
        stream_item_bytes: usize,
    ) -> Result<Self, InvalidProductCodecLimits> {
        if request_bytes == 0 {
            return Err(InvalidProductCodecLimits {
                field: "request_bytes",
            });
        }
        if response_bytes == 0 {
            return Err(InvalidProductCodecLimits {
                field: "response_bytes",
            });
        }
        if stream_spec_bytes == 0 {
            return Err(InvalidProductCodecLimits {
                field: "stream_spec_bytes",
            });
        }
        if stream_item_bytes == 0 {
            return Err(InvalidProductCodecLimits {
                field: "stream_item_bytes",
            });
        }
        Ok(Self {
            request_bytes,
            response_bytes,
            stream_spec_bytes,
            stream_item_bytes,
        })
    }

    /// Returns the finite request byte limit.
    pub const fn request_bytes(self) -> usize {
        self.request_bytes
    }

    /// Returns the finite response byte limit.
    pub const fn response_bytes(self) -> usize {
        self.response_bytes
    }

    /// Returns the logical-stream specification byte limit.
    pub const fn stream_spec_bytes(self) -> usize {
        self.stream_spec_bytes
    }

    /// Returns the non-terminal JSON stream-item byte limit.
    pub const fn stream_item_bytes(self) -> usize {
        self.stream_item_bytes
    }
}

/// Invalid product codec limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidProductCodecLimits {
    field: &'static str,
}

impl InvalidProductCodecLimits {
    /// Returns the stable invalid configuration field.
    pub const fn field(self) -> &'static str {
        self.field
    }
}

impl std::fmt::Display for InvalidProductCodecLimits {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "product codec limit {} must be non-zero",
            self.field
        )
    }
}

impl std::error::Error for InvalidProductCodecLimits {}

/// Stateless bounded codec for negotiated product JSON messages.
#[derive(Debug, Clone, Copy)]
pub struct ProductCodec {
    limits: ProductCodecLimits,
}

impl ProductCodec {
    /// Constructs a product codec from validated hard limits.
    ///
    /// Construction performs no IO, cannot fail, and does not panic.
    pub const fn new(limits: ProductCodecLimits) -> Self {
        Self { limits }
    }

    /// Returns the active hard limits.
    pub const fn limits(self) -> ProductCodecLimits {
        self.limits
    }

    /// Encodes one finite request without retaining its private content.
    ///
    /// # Errors
    ///
    /// Returns a redacted serialization or capacity failure.
    pub fn encode_request(&self, request: &ProductRequest) -> Result<Vec<u8>, ProductCodecError> {
        encode_json(
            ProductCodecStage::Request,
            request,
            self.limits.request_bytes,
        )
    }

    /// Decodes one finite request within its hard byte limit.
    ///
    /// # Errors
    ///
    /// Returns a redacted empty, malformed, or capacity failure.
    pub fn decode_request(&self, bytes: &[u8]) -> Result<ProductRequest, ProductCodecError> {
        decode_json(ProductCodecStage::Request, bytes, self.limits.request_bytes)
    }

    /// Encodes one typed finite response.
    ///
    /// # Errors
    ///
    /// Returns a redacted serialization or capacity failure.
    pub fn encode_response(
        &self,
        response: &ProductResponse,
    ) -> Result<Vec<u8>, ProductCodecError> {
        encode_json(
            ProductCodecStage::Response,
            response,
            self.limits.response_bytes,
        )
    }

    /// Decodes one typed finite response.
    ///
    /// # Errors
    ///
    /// Returns a redacted empty, malformed, or capacity failure.
    pub fn decode_response(&self, bytes: &[u8]) -> Result<ProductResponse, ProductCodecError> {
        decode_json(
            ProductCodecStage::Response,
            bytes,
            self.limits.response_bytes,
        )
    }

    /// Encodes one logical-stream specification.
    ///
    /// # Errors
    ///
    /// Returns a redacted serialization or capacity failure.
    pub fn encode_stream_spec(
        &self,
        spec: &ProductStreamSpec,
    ) -> Result<Vec<u8>, ProductCodecError> {
        encode_json(
            ProductCodecStage::StreamSpec,
            spec,
            self.limits.stream_spec_bytes,
        )
    }

    /// Decodes one logical-stream specification.
    ///
    /// # Errors
    ///
    /// Returns a redacted empty, malformed, or capacity failure.
    pub fn decode_stream_spec(&self, bytes: &[u8]) -> Result<ProductStreamSpec, ProductCodecError> {
        decode_json(
            ProductCodecStage::StreamSpec,
            bytes,
            self.limits.stream_spec_bytes,
        )
    }

    /// Encodes one logical-stream activation response.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a redacted serialization or capacity failure.
    pub fn encode_stream_open_response(
        &self,
        response: &ProductStreamOpenResponse,
    ) -> Result<Vec<u8>, ProductCodecError> {
        encode_json(
            ProductCodecStage::Response,
            response,
            self.limits.response_bytes,
        )
    }

    /// Decodes one logical-stream activation response.
    ///
    /// # Errors
    ///
    /// Returns a redacted empty, malformed, or capacity failure.
    pub fn decode_stream_open_response(
        &self,
        bytes: &[u8],
    ) -> Result<ProductStreamOpenResponse, ProductCodecError> {
        decode_json(
            ProductCodecStage::Response,
            bytes,
            self.limits.response_bytes,
        )
    }

    /// Encodes one non-terminal JSON logical-stream item.
    ///
    /// Terminal bytes use [`crate::TerminalOutputCodec`] and are never JSON or
    /// base64 encoded. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a redacted serialization or capacity failure.
    pub fn encode_stream_item(
        &self,
        item: &ProductStreamItem,
    ) -> Result<Vec<u8>, ProductCodecError> {
        encode_json(
            ProductCodecStage::StreamItem,
            item,
            self.limits.stream_item_bytes,
        )
    }

    /// Decodes one non-terminal JSON logical-stream item.
    ///
    /// # Errors
    ///
    /// Returns a redacted empty, malformed, or capacity failure.
    pub fn decode_stream_item(&self, bytes: &[u8]) -> Result<ProductStreamItem, ProductCodecError> {
        decode_json(
            ProductCodecStage::StreamItem,
            bytes,
            self.limits.stream_item_bytes,
        )
    }
}

fn encode_json<T: Serialize>(
    stage: ProductCodecStage,
    value: &T,
    limit: usize,
) -> Result<Vec<u8>, ProductCodecError> {
    let bytes = serde_json::to_vec(value).map_err(|source| {
        ProductCodecError::new(stage, ProductCodecErrorKind::Malformed(source))
    })?;
    ensure_size(stage, bytes.len(), limit)?;
    Ok(bytes)
}

fn decode_json<T: DeserializeOwned>(
    stage: ProductCodecStage,
    bytes: &[u8],
    limit: usize,
) -> Result<T, ProductCodecError> {
    if bytes.is_empty() {
        return Err(ProductCodecError::new(stage, ProductCodecErrorKind::Empty));
    }
    ensure_size(stage, bytes.len(), limit)?;
    serde_json::from_slice(bytes)
        .map_err(|source| ProductCodecError::new(stage, ProductCodecErrorKind::Malformed(source)))
}

fn ensure_size(
    stage: ProductCodecStage,
    observed: usize,
    limit: usize,
) -> Result<(), ProductCodecError> {
    if observed > limit {
        return Err(ProductCodecError::new(
            stage,
            ProductCodecErrorKind::capacity(observed, limit),
        ));
    }
    Ok(())
}
