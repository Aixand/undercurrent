// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::path::Path;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use undercurrent_application::{
    AgentChannelRepository, AgentChannelTarget, AgentChannelTargetResolution, AgentEventAdmission,
    AgentEventAdmissionOutcome, AgentEventPollOutcome, AgentEventReplyAdmission,
    AgentEventReplyOutcome, MAX_UNSETTLED_AGENT_EVENTS_PER_ATTEMPT, OperationAttemptAcceptance,
    OperationAttemptStore,
};
use undercurrent_domain::{
    AgentRuntimeBinding, DispatchCellIdentity, FencingToken, OperationAttempt, OperationAttemptId,
    PayloadDigest, ProcessInstanceId, ProcessMutationKind, RuntimeCallbackRef,
    RuntimeDispatchBinding, RuntimeDispatchLimits, RuntimeDispatchPolicy, RuntimeOutputContract,
    RuntimeRetentionPolicyRef, RuntimeTelemetryPolicyRef, SessionId, TimestampMillis,
};
use undercurrent_foundation::Classification;
use undercurrent_store_sqlite::{
    SqliteRepositoryConfig, SqliteRepositoryLimits, SqliteSessionRepository,
};

#[test]
fn agent_events_are_fenced_idempotent_and_restart_durable() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("agent-events.sqlite3");
    let attempt = agent_attempt("agent-attempt", 7, TimestampMillis::new(10_000));
    let target = target(&attempt, true);
    let call = br#"{"action":"report_progress","progress":{"completed":1}}"#.to_vec();
    let admission = AgentEventAdmission {
        target: target.clone(),
        request_id: "mcp-request-1".to_owned(),
        call_digest: PayloadDigest::of_bytes(&call),
        call_canonical_json: call.clone(),
        received_at: TimestampMillis::new(20),
    };

    let repository = open(&path);
    accept(&repository, &attempt);
    assert_eq!(
        block_on(repository.poll_agent_event(&target, 0, TimestampMillis::new(15)))
            .expect("poll the empty queue"),
        AgentEventPollOutcome::None { next_sequence: 1 }
    );
    assert!(matches!(
        block_on(repository.resolve_agent_channel_target(
            attempt.target_session().expect("target session"),
            target
                .process_instance_id
                .as_ref()
                .expect("process identity"),
            TimestampMillis::new(15),
        ))
        .expect("resolve protected peer target"),
        AgentChannelTargetResolution::Current(ref resolved) if resolved.target == target
            && resolved.expected_result_schema_canonical_json
                == br#"{"additionalProperties":false,"type":"object"}"#
    ));
    let event = match block_on(repository.enqueue_agent_event(&admission)).expect("enqueue event") {
        AgentEventAdmissionOutcome::Queued(event) => event,
        outcome => panic!("unexpected admission outcome: {outcome:?}"),
    };
    assert!(matches!(
        block_on(repository.enqueue_agent_event(&admission)).expect("replay pending event"),
        AgentEventAdmissionOutcome::DuplicatePending(ref duplicate) if duplicate == &event
    ));
    let mut conflict = admission.clone();
    conflict.call_canonical_json =
        br#"{"action":"report_progress","progress":{"completed":2}}"#.to_vec();
    conflict.call_digest = PayloadDigest::of_bytes(&conflict.call_canonical_json);
    assert_eq!(
        block_on(repository.enqueue_agent_event(&conflict)).expect("classify request conflict"),
        AgentEventAdmissionOutcome::RequestConflict,
    );
    drop(repository);

    let repository = open(&path);
    let mut poll_target = target.clone();
    poll_target.process_instance_id = None;
    assert!(matches!(
        block_on(repository.poll_agent_event(&poll_target, 0, TimestampMillis::new(30)))
            .expect("poll restarted queue"),
        AgentEventPollOutcome::Event(ref restarted) if restarted.as_ref() == &event
    ));
    let reply = br#"{"accepted":true}"#.to_vec();
    let reply_admission = AgentEventReplyAdmission {
        target: event.target.clone(),
        event_id: event.event_id.clone(),
        sequence: event.sequence,
        call_digest: event.call_digest,
        reply_digest: PayloadDigest::of_bytes(&reply),
        outcome_canonical_json: reply,
        replied_at: TimestampMillis::new(40),
    };
    let retained_reply =
        match block_on(repository.reply_agent_event(&reply_admission)).expect("reply to event") {
            AgentEventReplyOutcome::Accepted(reply) => reply,
            outcome => panic!("unexpected reply outcome: {outcome:?}"),
        };
    assert!(matches!(
        block_on(repository.reply_agent_event(&reply_admission)).expect("replay reply"),
        AgentEventReplyOutcome::Duplicate(ref duplicate) if duplicate == &retained_reply
    ));
    assert_eq!(
        block_on(repository.get_agent_event_reply(
            &target,
            &event.event_id,
            TimestampMillis::new(50),
        ))
        .expect("read durable reply"),
        Some(retained_reply.clone()),
    );
    assert_eq!(
        block_on(repository.poll_agent_event(
            &poll_target,
            event.sequence,
            TimestampMillis::new(50),
        ))
        .expect("poll after the settled event"),
        AgentEventPollOutcome::None {
            next_sequence: event.sequence.saturating_add(1),
        }
    );
    assert!(matches!(
        block_on(repository.enqueue_agent_event(&admission)).expect("replay replied event"),
        AgentEventAdmissionOutcome::DuplicateReplied(ref duplicate) if duplicate == &retained_reply
    ));

    let mut stale = target.clone();
    stale.fence = FencingToken::new(8).expect("stale fence");
    let mut stale_admission = admission.clone();
    stale_admission.target = stale;
    stale_admission.request_id = "stale-request".to_owned();
    assert_eq!(
        block_on(repository.enqueue_agent_event(&stale_admission)).expect("classify stale target"),
        AgentEventAdmissionOutcome::StaleTarget,
    );
    assert_eq!(
        block_on(repository.poll_agent_event(
            &poll_target,
            event.sequence,
            TimestampMillis::new(10_000)
        ))
        .expect("classify expired target"),
        AgentEventPollOutcome::BindingExpired,
    );
    assert_eq!(
        block_on(
            repository.resolve_agent_channel_target(
                attempt.target_session().expect("target session"),
                target
                    .process_instance_id
                    .as_ref()
                    .expect("process identity"),
                TimestampMillis::new(10_000),
            )
        )
        .expect("classify expired protected peer target"),
        AgentChannelTargetResolution::BindingExpired,
    );
}

#[test]
fn agent_event_queue_enforces_digest_and_unsettled_capacity() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("agent-capacity.sqlite3");
    let attempt = agent_attempt("capacity-attempt", 1, TimestampMillis::new(20_000));
    let target = target(&attempt, true);
    let repository = open(&path);
    accept(&repository, &attempt);

    let invalid = AgentEventAdmission {
        target: target.clone(),
        request_id: "invalid-digest".to_owned(),
        call_digest: PayloadDigest::of_bytes(b"different"),
        call_canonical_json: br#"{"action":"need_input"}"#.to_vec(),
        received_at: TimestampMillis::new(20),
    };
    assert!(block_on(repository.enqueue_agent_event(&invalid)).is_err());

    for index in 0..MAX_UNSETTLED_AGENT_EVENTS_PER_ATTEMPT {
        let call = format!(r#"{{"action":"report_progress","index":{index}}}"#).into_bytes();
        let admission = AgentEventAdmission {
            target: target.clone(),
            request_id: format!("request-{index}"),
            call_digest: PayloadDigest::of_bytes(&call),
            call_canonical_json: call,
            received_at: TimestampMillis::new(30 + index as i64),
        };
        assert!(matches!(
            block_on(repository.enqueue_agent_event(&admission)).expect("enqueue bounded event"),
            AgentEventAdmissionOutcome::Queued(_)
        ));
    }
    let overflow_call = br#"{"action":"report_progress","overflow":true}"#.to_vec();
    let overflow = AgentEventAdmission {
        target,
        request_id: "request-overflow".to_owned(),
        call_digest: PayloadDigest::of_bytes(&overflow_call),
        call_canonical_json: overflow_call,
        received_at: TimestampMillis::new(100),
    };
    assert_eq!(
        block_on(repository.enqueue_agent_event(&overflow)).expect("classify capacity"),
        AgentEventAdmissionOutcome::CapacityExceeded,
    );
}

#[test]
fn resolution_filters_current_cell_bindings_before_limiting_history() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("binding-history.sqlite3");
    let repository = open(&path);
    let expiry = TimestampMillis::new(10_000);
    let low = scoped_agent_attempt("low-1", 1, expiry, "cell:low", "agent-session");
    accept(&repository, &low);
    for fence in 7..=11 {
        accept(
            &repository,
            &scoped_agent_attempt(
                &format!("high-{fence}"),
                fence,
                expiry,
                "cell:high",
                "agent-session",
            ),
        );
    }
    let session = SessionId::new("agent-session").unwrap();
    let process = ProcessInstanceId::new("agent-process").unwrap();
    let resolve = |repository: &SqliteSessionRepository| {
        block_on(repository.resolve_agent_channel_target(
            &session,
            &process,
            TimestampMillis::new(20),
        ))
        .unwrap()
    };
    assert_eq!(
        resolve(&repository),
        AgentChannelTargetResolution::Ambiguous,
        "retained high-fence history must not hide the low-fence current cell"
    );
    accept(
        &repository,
        &scoped_agent_attempt("high-12", 12, expiry, "cell:high", "other-session"),
    );
    assert!(
        matches!(resolve(&repository), AgentChannelTargetResolution::Current(ref binding)
        if binding.target.attempt_id == *low.id()),
        "superseded high-fence history must not hide the only current binding"
    );
    drop(repository);
    let repository = open(&path);
    assert!(
        matches!(resolve(&repository), AgentChannelTargetResolution::Current(ref binding)
        if binding.target.attempt_id == *low.id())
    );
    accept(
        &repository,
        &scoped_agent_attempt("low-2", 2, expiry, "cell:low", "other-session"),
    );
    assert_eq!(resolve(&repository), AgentChannelTargetResolution::NotFound);
}

fn agent_attempt(id: &str, fence: u64, expires_at: TimestampMillis) -> OperationAttempt {
    scoped_agent_attempt(id, fence, expires_at, "cell:agent-test", "agent-session")
}

fn scoped_agent_attempt(
    id: &str,
    fence: u64,
    expires_at: TimestampMillis,
    cell: &str,
    session: &str,
) -> OperationAttempt {
    let attempt_id = OperationAttemptId::new(id).expect("attempt identity");
    let fence = FencingToken::new(fence).expect("fencing token");
    let request_digest = PayloadDigest::of_bytes(b"agent caller request");
    let agent = AgentRuntimeBinding::new(
        "tenant:agent-test".to_owned(),
        "workflow".to_owned(),
        "actor:test".to_owned(),
        "authority:test".to_owned(),
        "undercurrent:test-provider".to_owned(),
        3,
        attempt_id.clone(),
        4,
        2,
        fence,
        request_digest,
        br#"{"additionalProperties":false,"type":"object"}"#.to_vec(),
        br#"{"attempt_id":"agent-attempt","protocol_version":1}"#.to_vec(),
        expires_at,
    )
    .expect("agent binding");
    let dispatch = RuntimeDispatchBinding::new(
        DispatchCellIdentity::new(cell).expect("cell identity"),
        request_digest,
        PayloadDigest::of_bytes(b"launch profile"),
        PayloadDigest::of_bytes(b"environment binding"),
        PayloadDigest::of_bytes(b"agent input"),
        runtime_policy(expires_at),
    )
    .with_agent(agent);
    OperationAttempt::accepted_with_dispatch(
        attempt_id,
        Some(SessionId::new(session).expect("session identity")),
        ProcessMutationKind::Spawn,
        fence,
        PayloadDigest::of_bytes(b"agent spawn"),
        Some(dispatch),
        TimestampMillis::new(10),
    )
}

fn runtime_policy(expires_at: TimestampMillis) -> RuntimeDispatchPolicy {
    RuntimeDispatchPolicy::new(
        RuntimeDispatchLimits::new(30_000, 1_024, 2_048, None, None).expect("limits"),
        RuntimeOutputContract::new(
            "agent-channel-test/0.1.0-beta",
            PayloadDigest::of_bytes(b"agent output schema"),
            Classification::Internal,
        )
        .expect("output contract"),
        RuntimeTelemetryPolicyRef::new("trust://telemetry/policies/agent-channel-test")
            .expect("telemetry policy"),
        RuntimeRetentionPolicyRef::new("trust://retention/policies/agent-channel-test")
            .expect("retention policy"),
        vec![
            RuntimeCallbackRef::new(
                "runtime-callback://example/workflow/v0.1.0-beta/agent-channel-test",
            )
            .expect("callback"),
        ],
        TimestampMillis::new(5_000),
        expires_at,
    )
    .expect("runtime policy")
}

fn target(attempt: &OperationAttempt, include_process: bool) -> AgentChannelTarget {
    let dispatch = attempt.dispatch_binding().expect("dispatch binding");
    let agent = dispatch.agent().expect("agent binding");
    AgentChannelTarget {
        tenant_id: agent.tenant_id().to_owned(),
        source_product: agent.source_product().to_owned(),
        actor_context_ref: agent.actor_context_ref().to_owned(),
        authority_ref: agent.authority_ref().to_owned(),
        undercurrent_provider_id: agent.undercurrent_provider_id().to_owned(),
        undercurrent_provider_generation: agent.undercurrent_provider_generation(),
        session_id: attempt.target_session().expect("target session").clone(),
        process_instance_id: include_process
            .then(|| ProcessInstanceId::new("agent-process").expect("process identity")),
        attempt_id: attempt.id().clone(),
        attempt_revision: agent.attempt_revision(),
        recovery_epoch: agent.recovery_epoch(),
        fence: attempt.fencing_token(),
        request_digest: agent.request_digest(),
    }
}

fn accept(repository: &SqliteSessionRepository, attempt: &OperationAttempt) {
    assert!(matches!(
        block_on(OperationAttemptStore::accept(repository, attempt)).expect("accept attempt"),
        OperationAttemptAcceptance::Accepted(accepted) if accepted.id() == attempt.id()
    ));
}

fn open(path: &Path) -> SqliteSessionRepository {
    SqliteSessionRepository::open(
        SqliteRepositoryConfig::new(
            path,
            SqliteRepositoryLimits::new(100, 100, 1_048_576, 100, 64).expect("limits"),
            Duration::from_secs(1),
        )
        .expect("configuration"),
    )
    .expect("open repository")
}

struct ThreadWake(thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::park(),
        }
    }
}
