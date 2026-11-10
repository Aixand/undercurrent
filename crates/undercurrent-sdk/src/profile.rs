// SPDX-License-Identifier: Apache-2.0

//! Standalone client defaults independent of daemon implementation crates.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::time::Duration;

use undercurrent_protocol::{ProductCodec, ProductCodecLimits, TerminalOutputCodec};
use undercurrent_wire::MAX_CLIENT_FRAME_ENVELOPE_BYTES;

use crate::{
    ClientConnectionHello, ClientConnectionLimits, ClientProtocolVersion, ClientRecoveryPolicy,
    ClientRuntimeLimits, LocalClientLimits, PRODUCT_CLIENT_OPTIONAL_CAPABILITIES,
    PRODUCT_CLIENT_REQUIRED_CAPABILITIES, ProductClientRuntime,
};

/// Safe component label for an invalid compiled standalone profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandaloneClientProfileError {
    component: &'static str,
}

impl StandaloneClientProfileError {
    const fn at(component: &'static str) -> Self {
        Self { component }
    }

    /// Returns the stable invalid component label.
    pub const fn component(self) -> &'static str {
        self.component
    }
}

impl Display for StandaloneClientProfileError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "standalone client profile {} is invalid",
            self.component
        )
    }
}

impl Error for StandaloneClientProfileError {}

/// Validated UCHS offer, Local limits, and product runtime for standalone clients.
#[derive(Debug, Clone)]
pub struct StandaloneClientProfile {
    hello: ClientConnectionHello,
    local: LocalClientLimits,
    product: ProductClientRuntime,
}

const STANDARD_STREAM_QUEUE_BYTES: u64 = 524_288;
/// Default complete JSON request bound, reserving a wire envelope in a 32 MiB frame.
pub const DEFAULT_REQUEST_BYTES: u32 = 33_554_432 - MAX_CLIENT_FRAME_ENVELOPE_BYTES;
// Match the standalone daemon's finite JSON response budget. A 1 MiB raw
// output page can expand to more than 4 MiB as a JSON integer array.
const DEFAULT_RESPONSE_BYTES: u32 = 5 * 1_024 * 1_024;
const RETAINED_RECOVERY_QUALIFICATION_STREAM_QUEUE_BYTES: u64 = 8_388_608;
const STANDARD_RECOVERY_MAXIMUM_ATTEMPTS: u32 = 8;
const EXTERNALLY_DEADLINED_MAXIMUM_ATTEMPTS: u32 = 1;

/// Physical reconnect-attempt bound used by planned daemon-restart qualification.
pub const PLANNED_RESTART_QUALIFICATION_MAXIMUM_ATTEMPTS: u32 = 20;

impl StandaloneClientProfile {
    /// Builds the bounded default profile shipped with the standalone daemon and CLI.
    ///
    /// The profile offers at least 1,024 logical streams, so a single
    /// connection can exercise the contracted 300-stream qualification.
    pub fn standard(client_id: impl Into<String>) -> Result<Self, StandaloneClientProfileError> {
        Self::with_qualification_limits(
            client_id,
            STANDARD_STREAM_QUEUE_BYTES,
            STANDARD_RECOVERY_MAXIMUM_ATTEMPTS,
            DEFAULT_REQUEST_BYTES,
        )
    }

    /// Builds a standard profile with a caller-selected complete JSON request bound.
    ///
    /// The frame and transport queues grow to contain this bound plus framing.
    /// The daemon's negotiated transport and service limits still apply. This
    /// does not configure the daemon's terminal-input allowance or retry budget.
    ///
    /// # Errors
    ///
    /// Returns an invalid profile for a zero bound or overflowing frame size.
    pub fn with_request_limit(
        client_id: impl Into<String>,
        request_bytes: u32,
    ) -> Result<Self, StandaloneClientProfileError> {
        Self::with_qualification_limits(
            client_id,
            STANDARD_STREAM_QUEUE_BYTES,
            STANDARD_RECOVERY_MAXIMUM_ATTEMPTS,
            request_bytes,
        )
    }

    /// Builds the standard standalone profile for one externally deadline-governed probe.
    ///
    /// Protocol offers, codecs, and queues match [`Self::standard`], but the
    /// returned recovery policy allows only one physical attempt. Passing this
    /// entire runtime to ordinary `connect` also limits later recovery to one
    /// attempt. To bound only startup, pass its policy as the initial policy to
    /// [`crate::ProductClient::connect_with_initial_policy`] with a standard runtime.
    /// Dial and handshake deadlines remain separate adapter settings.
    pub fn single_connection_attempt(
        client_id: impl Into<String>,
    ) -> Result<Self, StandaloneClientProfileError> {
        Self::with_qualification_limits(
            client_id,
            STANDARD_STREAM_QUEUE_BYTES,
            EXTERNALLY_DEADLINED_MAXIMUM_ATTEMPTS,
            DEFAULT_REQUEST_BYTES,
        )
    }

    /// Builds the bounded standalone profile used only by retained-recovery qualification.
    ///
    /// This preserves the standard protocol, transport, connection-wide queue, and runtime
    /// settings while reserving at least an 8 MiB queue for its recovery window.
    /// Larger request frames can require a larger queue in both profiles.
    pub fn retained_recovery_qualification(
        client_id: impl Into<String>,
    ) -> Result<Self, StandaloneClientProfileError> {
        Self::with_qualification_limits(
            client_id,
            RETAINED_RECOVERY_QUALIFICATION_STREAM_QUEUE_BYTES,
            STANDARD_RECOVERY_MAXIMUM_ATTEMPTS,
            DEFAULT_REQUEST_BYTES,
        )
    }

    /// Builds the opt-in profile for a qualification-controlled daemon restart.
    ///
    /// Every transport, codec, queue, timeout, backoff, jitter, and stream-resume setting is
    /// identical to [`Self::standard`]. Only the bounded physical reconnect-attempt count is
    /// raised so the same client can span a deliberate daemon stop and start.
    pub fn planned_restart_qualification(
        client_id: impl Into<String>,
    ) -> Result<Self, StandaloneClientProfileError> {
        Self::with_qualification_limits(
            client_id,
            STANDARD_STREAM_QUEUE_BYTES,
            PLANNED_RESTART_QUALIFICATION_MAXIMUM_ATTEMPTS,
            DEFAULT_REQUEST_BYTES,
        )
    }

    fn with_qualification_limits(
        client_id: impl Into<String>,
        stream_queue_bytes: u64,
        recovery_maximum_attempts: u32,
        request_bytes: u32,
    ) -> Result<Self, StandaloneClientProfileError> {
        let frame_bytes = request_bytes
            .checked_add(MAX_CLIENT_FRAME_ENVELOPE_BYTES)
            .ok_or_else(|| StandaloneClientProfileError::at("request_bytes"))?
            .max(DEFAULT_RESPONSE_BYTES + MAX_CLIENT_FRAME_ENVELOPE_BYTES);
        let limits = ClientConnectionLimits::new(
            frame_bytes,
            1_024,
            256,
            // Server admission reserves one complete frame per traffic class.
            16_777_216_u64.max(u64::from(frame_bytes) * 4),
            stream_queue_bytes.max(u64::from(frame_bytes)),
            262_144,
        )
        .map_err(|_| StandaloneClientProfileError::at("connection_limits"))?;
        let hello = ClientConnectionHello::new(
            client_id,
            ClientProtocolVersion::CURRENT,
            PRODUCT_CLIENT_REQUIRED_CAPABILITIES.map(str::to_owned),
            PRODUCT_CLIENT_OPTIONAL_CAPABILITIES.map(str::to_owned),
            limits,
        )
        .map_err(|_| StandaloneClientProfileError::at("hello"))?;
        let local = LocalClientLimits::new(
            65_536,
            Duration::from_secs(5),
            Duration::from_secs(5),
            frame_bytes as usize,
            1_024,
            64,
        )
        .map_err(|_| StandaloneClientProfileError::at("local"))?;
        // Reserve the largest client envelope within the offered frame bound
        // so resume IDs and optional trace fields at their allowed lengths do
        // not crowd out payload. ClientRuntimeLimits::validate checks payload alone.
        // See [profile bounds](../../../docs/sdk/rust/limits-and-diagnostics.md#standard-profile).
        let client_limits = ClientRuntimeLimits::new(
            frame_bytes - MAX_CLIENT_FRAME_ENVELOPE_BYTES,
            1_024,
            // Credit is byte-based: even the smallest valid frame must fit
            // before credit runs out. Sixteen is a conservative lower bound
            // on a UCMP frame, with additional slots reserved for gaps.
            (262_144 / 16) + 64,
            262_144,
        )
        .map_err(|_| StandaloneClientProfileError::at("client_limits"))?;
        let recovery = ClientRecoveryPolicy::new(
            Duration::from_millis(50),
            Duration::from_secs(2),
            20_000,
            2_000,
            recovery_maximum_attempts,
            8,
        )
        .map_err(|_| StandaloneClientProfileError::at("recovery"))?;
        let product_limits = ProductCodecLimits::new(
            request_bytes as usize,
            DEFAULT_RESPONSE_BYTES as usize,
            65_536,
            131_072,
        )
        .map_err(|_| StandaloneClientProfileError::at("product_codec"))?;
        let terminal = TerminalOutputCodec::new(131_072)
            .map_err(|_| StandaloneClientProfileError::at("terminal_codec"))?;
        let product = ProductClientRuntime::new(
            client_limits,
            recovery,
            Duration::from_secs(5),
            ProductCodec::new(product_limits),
            terminal,
        )
        .map_err(|_| StandaloneClientProfileError::at("product_runtime"))?;
        Ok(Self {
            hello,
            local,
            product,
        })
    }

    /// Returns the validated client handshake offer.
    pub const fn hello(&self) -> &ClientConnectionHello {
        &self.hello
    }

    /// Returns bounded Local transport limits.
    pub const fn local(&self) -> LocalClientLimits {
        self.local
    }

    /// Returns bounded product codecs and recovery policy.
    pub const fn product(&self) -> ProductClientRuntime {
        self.product
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finite_screen_with_scrollback_fits_even_with_small_request_limit() {
        use undercurrent_protocol::{ProductResponse, ProductSuccess, TerminalScreenSnapshotDto};
        for request_bytes in [131_072, DEFAULT_REQUEST_BYTES] {
            let profile =
                StandaloneClientProfile::with_request_limit("screen", request_bytes).unwrap();
            let codec = profile.product().product_codec();
            let response = ProductResponse::Success(Box::new(ProductSuccess::ReadTerminalScreen(
                TerminalScreenSnapshotDto {
                    checkpoint: "screen-checkpoint".into(),
                    output_offset: 131_072,
                    columns: 80,
                    rows: 24,
                    cursor_row: 23,
                    cursor_column: 79,
                    cursor_visible: false,
                    alternate_screen: false,
                    contents: vec![255; 131_072],
                },
            )));
            let encoded = codec.encode_response(&response).unwrap();
            assert!(encoded.len() > 196_608);
            assert_eq!(codec.decode_response(&encoded).unwrap(), response);
        }
    }

    #[test]
    fn request_limit_grows_framing_and_rejects_zero_and_overflow() {
        for bytes in [131_072, DEFAULT_REQUEST_BYTES, 64 * 1024 * 1024] {
            let profile = StandaloneClientProfile::with_request_limit("sizing", bytes).unwrap();
            assert_eq!(
                profile.product().product_codec().limits().request_bytes(),
                bytes as usize
            );
            assert!(
                profile.hello().limits().max_frame_bytes()
                    >= bytes + MAX_CLIENT_FRAME_ENVELOPE_BYTES
            );
            assert!(
                profile.local().maximum_message_bytes()
                    >= profile.hello().limits().max_frame_bytes() as usize
            );
        }
        assert!(StandaloneClientProfile::with_request_limit("sizing", 0).is_err());
        assert!(StandaloneClientProfile::with_request_limit("sizing", u32::MAX).is_err());
    }

    #[test]
    fn default_request_codec_accepts_worst_case_escaped_five_mib_text() {
        use undercurrent_protocol::{
            InputObservationPolicyDto, MutationContext, ProductOperation, ProductRequest,
            SendTerminalInputRequest, TerminalInputDto,
        };
        let profile = StandaloneClientProfile::standard("escaped-input").unwrap();
        let codec = profile.product().product_codec();
        let request = ProductRequest::SendTerminalInput(SendTerminalInputRequest {
            mutation: MutationContext::current(ProductOperation::SendTerminalInput),
            session_id: "session".into(),
            expected_revision: None,
            delivery_id: None,
            items: vec![TerminalInputDto::Text {
                text: "\0".repeat(5 * 1024 * 1024),
            }],
            observation_policy: InputObservationPolicyDto::ForceActive,
        });
        let encoded = codec.encode_request(&request).unwrap();
        assert!(encoded.len() > 30 * 1024 * 1024);
        assert!(codec.decode_request(&encoded).unwrap() == request);
    }

    #[test]
    fn planned_restart_changes_only_the_bounded_reconnect_attempt_count() {
        let standard =
            StandaloneClientProfile::standard("profile-comparison").expect("standard profile");
        let retained =
            StandaloneClientProfile::retained_recovery_qualification("profile-comparison")
                .expect("retained-recovery profile");
        let planned = StandaloneClientProfile::planned_restart_qualification("profile-comparison")
            .expect("planned-restart profile");

        let standard_recovery = standard.product().recovery_policy();
        let planned_recovery = planned.product().recovery_policy();
        assert_eq!(standard_recovery.maximum_attempts(), 8);
        assert_eq!(retained.product().recovery_policy().maximum_attempts(), 8);
        assert_eq!(
            planned_recovery.maximum_attempts(),
            PLANNED_RESTART_QUALIFICATION_MAXIMUM_ATTEMPTS
        );
        assert_eq!(planned.hello().limits(), standard.hello().limits());
        assert_eq!(planned.local(), standard.local());
        assert_eq!(
            planned.product().client_limits(),
            standard.product().client_limits()
        );
        assert_eq!(
            planned.product().resume_timeout(),
            standard.product().resume_timeout()
        );
        assert_eq!(
            planned_recovery.initial_delay(),
            standard_recovery.initial_delay()
        );
        assert_eq!(
            planned_recovery.maximum_delay(),
            standard_recovery.maximum_delay()
        );
        assert_eq!(
            planned_recovery.backoff_basis_points(),
            standard_recovery.backoff_basis_points()
        );
        assert_eq!(
            planned_recovery.jitter_basis_points(),
            standard_recovery.jitter_basis_points()
        );
        assert_eq!(
            planned_recovery.maximum_parallel_resumes(),
            standard_recovery.maximum_parallel_resumes()
        );
    }

    #[test]
    fn black_holed_externally_deadlined_probe_has_one_physical_attempt() {
        let standard =
            StandaloneClientProfile::standard("profile-comparison").expect("standard profile");
        let probe = StandaloneClientProfile::single_connection_attempt("profile-comparison")
            .expect("single-attempt profile");

        assert_eq!(standard.product().recovery_policy().maximum_attempts(), 8);
        assert_eq!(probe.product().recovery_policy().maximum_attempts(), 1);
        assert_eq!(probe.hello().limits(), standard.hello().limits());
        assert_eq!(probe.local(), standard.local());
        assert_eq!(
            probe.product().client_limits(),
            standard.product().client_limits()
        );
        assert_eq!(
            probe.product().resume_timeout(),
            standard.product().resume_timeout()
        );
    }
}
