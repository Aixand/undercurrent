// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::sync::mpsc;

use undercurrent_wire::core::NamespacedName;
use undercurrent_wire::{
    CURRENT_PROTOCOL_VERSION, ClientId, ConnectionHello, ConnectionId, ConnectionLimits,
    ConnectionPolicy, HeartbeatPolicy, ServerEpoch, connection_capabilities, negotiate_connection,
};

use super::*;
use crate::{
    ClientConnectionAgreement, ClientTransport, ClientTransportError, ClientTransportInbound,
    ClientTransportOperation, ClientTransportOutbound, DeterministicClientRecoveryJitter,
    NegotiatedClientTransport,
};

const DEADLINE: Duration = Duration::from_secs(3);

#[test]
fn close_wait_race_does_not_strand_automatic_recovery() {
    let connector = Arc::new(TestConnector);
    let client = connect(connector);
    let (physical, _) = client.inner.current_client().expect("physical client");
    let gate = pause_before_parking(&physical);
    let closing = physical.clone();
    let (closed_tx, closed_rx) = mpsc::channel();
    let closer = thread::spawn(move || {
        closing.close();
        closed_tx.send(()).expect("close completion");
    });

    // With the old code, close completes and its notification is discarded
    // while the predicate still holds its earlier 'open' result. With the fix,
    // closure waits for the same mutex until this gate allows waiter enrollment.
    let closed_before_enrollment = closed_rx.recv_timeout(Duration::from_millis(100)).is_ok();
    gate.send(()).expect("allow waiter enrollment");
    closer.join().expect("closer thread");

    let started = Instant::now();
    while client.pressure().generation == 1 && started.elapsed() < DEADLINE {
        thread::yield_now();
    }
    let recovered = client.pressure().generation == 2
        && client.pressure().phase == ResilientClientPhase::Connected;

    // Rescue a regressed implementation before asserting, so the test reports
    // failure instead of hanging in the resilient owner's destructor.
    if !recovered {
        physical.before_close_wait(|| {});
    }
    client.close();
    drop(client);
    assert!(
        recovered,
        "one close notification must start recovery (closed before enrollment: {closed_before_enrollment})"
    );
}

#[test]
fn close_wait_race_does_not_strand_final_owner_drop() {
    let client = connect(Arc::new(TestConnector));
    let (physical, _) = client.inner.current_client().expect("physical client");
    let gate = pause_before_parking(&physical);
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let dropping = thread::spawn(move || {
        drop(client);
        dropped_tx.send(()).expect("owner drop completion");
    });

    let dropped_before_enrollment = dropped_rx.recv_timeout(Duration::from_millis(100)).is_ok();
    let closed_before_enrollment = physical.is_closed();
    gate.send(()).expect("allow waiter enrollment");
    let completed = dropped_before_enrollment || dropped_rx.recv_timeout(DEADLINE).is_ok();
    if !completed {
        physical.before_close_wait(|| {});
        dropped_rx
            .recv_timeout(DEADLINE)
            .expect("rescue owner drop");
    }
    dropping.join().expect("owner drop thread");
    assert!(
        completed,
        "final owner must join the recovery supervisor (closed before enrollment: {closed_before_enrollment})"
    );
}

fn pause_before_parking(client: &Client) -> mpsc::Sender<()> {
    let (checked_tx, checked_rx) = mpsc::channel();
    let (continue_tx, continue_rx) = mpsc::channel();
    client.before_close_wait(move || {
        checked_tx.send(()).expect("predicate evaluated");
        continue_rx
            .recv_timeout(DEADLINE)
            .expect("release predicate");
    });
    checked_rx
        .recv_timeout(DEADLINE)
        .expect("supervisor reached predicate");
    continue_tx
}

fn connect(connector: Arc<TestConnector>) -> ResilientClient {
    ResilientClient::connect(
        connector,
        ClientRuntimeLimits::new(900, 8, 8, 4_096).expect("client limits"),
        ClientRecoveryPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(2),
            10_000,
            1,
            1,
            1,
        )
        .expect("recovery policy"),
        Arc::new(DeterministicClientRecoveryJitter::new(1)),
        DEADLINE,
    )
    .expect("resilient client")
}

struct TestConnector;

impl ClientConnector for TestConnector {
    fn connect(
        &self,
        _cancellation: &ClientRecoveryCancellation,
    ) -> Result<NegotiatedClientTransport, ClientTransportError> {
        let capabilities = [
            connection_capabilities::MULTIPLEX,
            connection_capabilities::FLOW_CONTROL,
            connection_capabilities::STREAM_RESUME,
            connection_capabilities::TRACE_CONTEXT,
        ]
        .into_iter()
        .map(|name| NamespacedName::new(name).expect("capability"))
        .collect::<BTreeSet<_>>();
        let limits =
            ConnectionLimits::new(1_024, 8, 8, 16_384, 4_096, 1_024).expect("connection limits");
        let hello = ConnectionHello::new(
            ClientId::new("close-race-client").expect("client ID"),
            CURRENT_PROTOCOL_VERSION,
            capabilities.clone(),
            BTreeSet::new(),
            limits,
        )
        .expect("hello");
        let policy = ConnectionPolicy::new(
            CURRENT_PROTOCOL_VERSION,
            capabilities,
            limits,
            HeartbeatPolicy::new(10, 60_000).expect("heartbeat"),
            Some(60_000),
        )
        .expect("policy");
        let agreement = negotiate_connection(
            &hello,
            &policy,
            ConnectionId::new("close-race-connection").expect("connection ID"),
            ServerEpoch::new("close-race-epoch").expect("server epoch"),
        )
        .expect("agreement");
        Ok(NegotiatedClientTransport::new(
            ClientConnectionAgreement::from_raw(agreement),
            Arc::new(TestTransport::default()),
        ))
    }
}

#[derive(Default)]
struct TestTransport {
    closed: Mutex<bool>,
    changed: Condvar,
}

impl ClientTransport for TestTransport {
    fn send(&self, _message: ClientTransportOutbound) -> Result<(), ClientTransportError> {
        Ok(())
    }

    fn receive(&self) -> Result<ClientTransportInbound, ClientTransportError> {
        let guard = self.closed.lock().expect("transport lock");
        drop(
            self.changed
                .wait_while(guard, |closed| !*closed)
                .expect("transport close"),
        );
        Err(ClientTransportError::new(
            ClientTransportOperation::Receive,
            std::io::Error::from(std::io::ErrorKind::ConnectionAborted),
        ))
    }

    fn close(&self) {
        *self.closed.lock().expect("transport lock") = true;
        self.changed.notify_all();
    }
}
