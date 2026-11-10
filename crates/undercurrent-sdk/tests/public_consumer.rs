// SPDX-License-Identifier: Apache-2.0

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_sdk::{
    ClientConnectionHello, ClientConnectionLimits, ClientProtocolVersion, ClientServerEpoch,
    ClientStreamId, ClientStreamSequence, ClientTraceContext, ClientTransport,
    ClientTransportError, ClientTransportInbound, ClientTransportOperation,
    ClientTransportOutbound, LocalClientConfig, StandaloneClientProfile,
};

struct ConsumerTransport;

impl ClientTransport for ConsumerTransport {
    fn send(&self, message: ClientTransportOutbound) -> Result<(), ClientTransportError> {
        let _ = message.command();
        Ok(())
    }

    fn receive(&self) -> Result<ClientTransportInbound, ClientTransportError> {
        Err(ClientTransportError::new(
            ClientTransportOperation::Receive,
            Closed,
        ))
    }

    fn close(&self) {}
}

#[derive(Debug)]
struct Closed;

impl Display for Closed {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("closed")
    }
}

impl Error for Closed {}

#[test]
fn external_consumer_compiles_using_only_sdk_owned_boundary_types() {
    let profile = StandaloneClientProfile::standard("public-consumer").expect("profile");
    let config = LocalClientConfig::new(
        std::env::temp_dir().join("undercurrent/run/control.sock"),
        profile.hello().clone(),
        profile.local(),
    )
    .expect("Local config");

    assert_eq!(config.hello().client_id(), "public-consumer");
    assert_eq!(ClientProtocolVersion::CURRENT.major, 0);
    let connection_limits =
        ClientConnectionLimits::new(1_024, 8, 8, 16_384, 4_096, 1_024).expect("connection limits");
    assert_eq!(connection_limits.max_logical_streams(), 8);
    assert!(
        ClientConnectionHello::new(
            "future-client",
            ClientProtocolVersion::new(2, 1),
            Vec::new(),
            Vec::new(),
            connection_limits,
        )
        .is_err()
    );
    assert_eq!(
        ClientStreamId::new("stream-1").unwrap().as_str(),
        "stream-1"
    );
    assert_eq!(
        ClientServerEpoch::new("epoch-1").unwrap().as_str(),
        "epoch-1"
    );
    assert_eq!(ClientStreamSequence::new(7).value(), 7);
    assert_eq!(
        ClientTraceContext::new("trace-1", "span-1")
            .expect("trace context")
            .trace_id(),
        "trace-1"
    );
    assert!(
        !ClientTransportInbound::response("request-1", b"response".as_slice().into())
            .expect("inbound response")
            .closes_connection()
    );
    let transport: &dyn ClientTransport = &ConsumerTransport;
    transport.close();
}

#[test]
fn consumer_fixture_has_no_private_protocol_imports() {
    let source = include_str!("public_consumer.rs");
    assert!(!source.contains(concat!("undercurrent_", "wire")));
    assert!(!source.contains(concat!("undercurrent_", "multiplex")));
}
