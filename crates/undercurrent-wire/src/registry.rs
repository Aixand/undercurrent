// SPDX-License-Identifier: Apache-2.0

//! Central binary frame-kind registry.

/// Direction of one semantic connection frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WireDirection {
    /// Client to external daemon.
    ClientToServer,
    /// External daemon to client.
    ServerToClient,
}

/// Stable binary frame kinds and discriminator codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WireMessageKind {
    /// Finite client request.
    Request,
    /// Best-effort request cancellation.
    CancelRequest,
    /// Correlated logical-stream open.
    OpenStream,
    /// Correlated logical-stream resume after reconnect.
    ResumeStream,
    /// Independent logical-stream close.
    CloseStream,
    /// Connection and stream credit update.
    WindowUpdate,
    /// Client heartbeat probe.
    Ping,
    /// Client graceful close.
    ClientClose,
    /// Finite server response.
    Response,
    /// Ordered binary logical-stream data.
    StreamData,
    /// Explicit typed logical-stream gap.
    StreamGap,
    /// Server heartbeat response.
    Pong,
    /// Server terminal close.
    ServerClose,
}

impl WireMessageKind {
    /// Complete frame-kind registry in semantic order, not numeric code order.
    /// Resolve stable bytes through [`Self::code`], not an array index.
    pub const ALL: [Self; 13] = [
        Self::Request,
        Self::CancelRequest,
        Self::OpenStream,
        Self::ResumeStream,
        Self::CloseStream,
        Self::WindowUpdate,
        Self::Ping,
        Self::ClientClose,
        Self::Response,
        Self::StreamData,
        Self::StreamGap,
        Self::Pong,
        Self::ServerClose,
    ];

    /// Returns stable one-byte binary discriminator.
    pub const fn code(self) -> u8 {
        match self {
            Self::Request => 1,
            Self::CancelRequest => 2,
            Self::OpenStream => 3,
            Self::ResumeStream => 8,
            Self::CloseStream => 4,
            Self::WindowUpdate => 5,
            Self::Ping => 6,
            Self::ClientClose => 7,
            Self::Response => 128,
            Self::StreamData => 129,
            Self::StreamGap => 130,
            Self::Pong => 131,
            Self::ServerClose => 132,
        }
    }

    /// Returns stable snake-case protocol name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::CancelRequest => "cancel_request",
            Self::OpenStream => "open_stream",
            Self::ResumeStream => "resume_stream",
            Self::CloseStream => "close_stream",
            Self::WindowUpdate => "window_update",
            Self::Ping => "ping",
            Self::ClientClose => "client_close",
            Self::Response => "response",
            Self::StreamData => "stream_data",
            Self::StreamGap => "stream_gap",
            Self::Pong => "pong",
            Self::ServerClose => "server_close",
        }
    }

    /// Resolves one binary code only for the expected connection direction.
    pub const fn from_code(code: u8, direction: WireDirection) -> Option<Self> {
        let kind = match code {
            1 => Self::Request,
            2 => Self::CancelRequest,
            3 => Self::OpenStream,
            8 => Self::ResumeStream,
            4 => Self::CloseStream,
            5 => Self::WindowUpdate,
            6 => Self::Ping,
            7 => Self::ClientClose,
            128 => Self::Response,
            129 => Self::StreamData,
            130 => Self::StreamGap,
            131 => Self::Pong,
            132 => Self::ServerClose,
            _ => return None,
        };
        match (direction, kind) {
            (
                WireDirection::ClientToServer,
                Self::Request
                | Self::CancelRequest
                | Self::OpenStream
                | Self::ResumeStream
                | Self::CloseStream
                | Self::WindowUpdate
                | Self::Ping
                | Self::ClientClose,
            )
            | (
                WireDirection::ServerToClient,
                Self::Response
                | Self::StreamData
                | Self::StreamGap
                | Self::Pong
                | Self::ServerClose,
            ) => Some(kind),
            _ => None,
        }
    }
}
