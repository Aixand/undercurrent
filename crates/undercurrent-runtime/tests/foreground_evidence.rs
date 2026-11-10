// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use undercurrent_application::{
    Clock, ForegroundEvidenceCollector, ForegroundProcessGroupObservation,
    ForegroundProcessObserver, OutputOffset, PortError, PortErrorKind, PortFuture, PortOperation,
    ProcessGroupCompleteness, ProfilePatternEngine, TcpListenerObservation, TerminalOutputLimits,
    TerminalOutputStore,
};
use undercurrent_domain::{
    Confidence, DetectionArchetype, DetectionProfile, DetectionProfileLimits, EvidencePriority,
    ExecutionState, ForegroundClaim, ForegroundDecisionPolicy, ForegroundEvidence,
    ForegroundObservation, ForegroundRule, InteractionClaim, InteractionModality, InteractionState,
    ObservationSemanticId, OutputExclusions, PatternExpression, ProcessInstanceId,
    ProfileMatchRules, ProfileName, PromptTraits, SessionId, TerminalSize, TimestampMillis,
};
use undercurrent_runtime::{
    CooperativeForegroundState, CooperativeSignalStore, InMemoryDetectionProfileCatalog,
    RegexPatternEngine, RegexPatternEngineLimits, RuntimeEvidenceSource,
    TerminalForegroundEvidenceCollector, TerminalForegroundEvidenceLimits, TerminalScreenLimits,
    VtTerminalScreenStore,
};

#[test]
fn current_profile_prompt_precedes_motion_from_the_prior_checkpoint() {
    let fixture = Fixture::new(4);
    let profile = profile(
        "agent",
        DetectionArchetype::AnimatedAgent,
        vec![prompt("(?m)^Allow\\?$", "agent.permission", 8_500)],
        OutputExclusions::default(),
    );
    fixture.activate(profile);
    let session = session("prompt-and-motion");
    let process = process("process-1");
    fixture.begin(&session, OutputOffset::ZERO);
    fixture.output(&session, b"working");
    let collector = fixture.collector(4);
    let initial = collect(&collector, &session, &process, Some("agent"));
    assert_eq!(
        decision(initial).execution().state(),
        ExecutionState::Unknown
    );

    fixture.output(&session, b"\x1b[2K\rAllow?");
    let evidence = collect(&collector, &session, &process, Some("agent"));

    assert_eq!(evidence.len(), 5);
    assert!(matches!(
        evidence[2].claim(),
        Some(ForegroundClaim::Interaction(claim))
            if claim.state() == InteractionState::Requested
    ));
    assert_eq!(evidence[2].priority(), EvidencePriority::Terminal);
    assert!(matches!(
        evidence[3].claim(),
        Some(ForegroundClaim::Execution(ExecutionState::Active))
    ));
    assert_eq!(evidence[3].priority(), EvidencePriority::TerminalHistory);
    let selected = decision(evidence);
    assert_eq!(selected.execution().state(), ExecutionState::Active);
    assert_eq!(selected.interaction().state(), InteractionState::Requested);
    assert!(selected.interaction().is_actionable());
    let pressure = collector.pressure();
    assert_eq!(pressure.prompt_matches, 1);
    assert_eq!(pressure.motion_matches, 1);
}

#[test]
fn first_checkpoint_is_unknown_and_later_visible_motion_is_busy() {
    let fixture = Fixture::new(2);
    let session = session("motion-baseline");
    let process = process("process-1");
    fixture.begin(&session, OutputOffset::ZERO);
    fixture.output(&session, b"private-initial-output");
    let collector = fixture.collector(2);

    let initial = collect(&collector, &session, &process, None);
    assert_eq!(
        decision(initial).execution().state(),
        ExecutionState::Unknown
    );

    fixture.output(&session, b"\x1b[2K\rprivate-next-output");
    let changed = collect(&collector, &session, &process, None);
    assert_eq!(
        decision(changed).execution().state(),
        ExecutionState::Active
    );
    assert_eq!(collector.pressure().cycles, 2);
    assert!(!format!("{collector:?}").contains("private-next-output"));
}

#[test]
fn exact_process_cooperative_signal_precedes_terminal_evidence() {
    let fixture = Fixture::new(2);
    let session = session("cooperative");
    let active_process = process("process-1");
    fixture.begin(&session, OutputOffset::ZERO);
    fixture.output(&session, b"moving output");
    let collector = fixture.collector(2);
    let _ = collect(&collector, &session, &active_process, None);
    fixture.output(&session, b"more motion");
    fixture.cooperative.record(
        session.clone(),
        active_process.clone(),
        CooperativeForegroundState::Ready,
        TimestampMillis::new(99),
    );

    let evidence = collect(&collector, &session, &active_process, None);
    let selected = decision(evidence);

    assert_eq!(selected.execution().state(), ExecutionState::Quiescent);
    assert_eq!(selected.interaction().state(), InteractionState::Available);
    assert_eq!(collector.pressure().cooperative_matches, 1);
    assert_eq!(
        decision(collect(&collector, &session, &process("process-2"), None))
            .execution()
            .state(),
        ExecutionState::Unknown
    );
}

#[test]
fn idle_shell_readiness_is_reconfirmed_after_an_hour_without_output() {
    let mut fixture = Fixture::new(2);
    let now = TimestampMillis::new(3_600_000);
    fixture.clock = Arc::new(FixedClock(now));
    let session = session("idle-shell");
    let process = process("shell-1");
    fixture.begin(&session, OutputOffset::ZERO);
    fixture.cooperative.record(
        session.clone(),
        process.clone(),
        CooperativeForegroundState::Ready,
        TimestampMillis::new(1),
    );
    // Reproduces the original failure without live confirmation.
    assert_eq!(
        decision_at(
            collect(&fixture.collector(2), &session, &process, None),
            now
        )
        .interaction()
        .state(),
        InteractionState::None
    );
    let observer = Arc::new(ShellObserver(
        Mutex::new(Some(CooperativeForegroundState::Ready)),
        now,
    ));
    let collector = fixture.collector(2).with_foreground_processes(observer);
    for bytes in [b"".as_slice(), b"background job completed".as_slice()] {
        if !bytes.is_empty() {
            fixture.output(&session, bytes);
        }
        let evidence = collect(&collector, &session, &process, None);
        assert_eq!(evidence[0].observed_at(), now);
        let selected = decision_at(evidence, now);
        assert_eq!(selected.execution().state(), ExecutionState::Quiescent);
        assert_eq!(selected.interaction().state(), InteractionState::Available);
    }
    // Confirmation must not rewrite when the shell actually emitted its event.
    assert_eq!(
        fixture
            .cooperative
            .latest_exact(&session, &process)
            .unwrap()
            .observed_at(),
        TimestampMillis::new(1)
    );
    assert_eq!(
        decision_at(
            collect(&collector, &session, &self::process("replacement"), None),
            now
        )
        .interaction()
        .state(),
        InteractionState::None
    );
    fixture.cooperative.remove_exact(&session, &process);
    assert_eq!(
        decision_at(collect(&collector, &session, &process, None), now)
            .interaction()
            .state(),
        InteractionState::None
    );
}

#[test]
fn shell_ready_is_withdrawn_on_takeover_or_monitoring_failure_and_replaced_on_command_start() {
    let fixture = Fixture::new(1);
    let now = fixture.clock.now();
    let session = session("shell-transitions");
    let process = process("shell-1");
    fixture.begin(&session, OutputOffset::ZERO);
    fixture.cooperative.record(
        session.clone(),
        process.clone(),
        CooperativeForegroundState::Ready,
        TimestampMillis::new(99),
    );
    let observer = Arc::new(ShellObserver(
        Mutex::new(Some(CooperativeForegroundState::Ready)),
        now,
    ));
    let collector = fixture
        .collector(1)
        .with_foreground_processes(observer.clone());
    assert_eq!(
        decision(collect(&collector, &session, &process, None))
            .interaction()
            .state(),
        InteractionState::Available
    );
    // Reject even a recent ready event when the live observation contradicts it
    // or cannot be obtained; no wait for the freshness timeout is required.
    for state in [Some(CooperativeForegroundState::Running), None] {
        *observer.0.lock().unwrap() = state;
        assert_eq!(
            decision(collect(&collector, &session, &process, None))
                .interaction()
                .state(),
            InteractionState::None
        );
    }
    fixture.cooperative.record(
        session.clone(),
        process.clone(),
        CooperativeForegroundState::Running,
        now,
    );
    let running = decision(collect(&collector, &session, &process, None));
    assert_eq!(running.execution().state(), ExecutionState::Active);
    assert_eq!(running.interaction().state(), InteractionState::None);
}

struct ShellObserver(Mutex<Option<CooperativeForegroundState>>, TimestampMillis);

impl ForegroundProcessObserver for ShellObserver {
    fn observe_foreground<'a>(
        &'a self,
        session: &'a SessionId,
        process: &'a ProcessInstanceId,
    ) -> PortFuture<'a, Result<ForegroundProcessGroupObservation, PortError>> {
        Box::pin(async move {
            let state = *self.0.lock().unwrap();
            if state.is_none() {
                return Err(PortError::new(
                    PortErrorKind::Unavailable,
                    PortOperation::ObserveForegroundProcessGroup,
                    Some(session.clone()),
                    std::io::Error::other("monitor disconnected"),
                ));
            }
            Ok(ForegroundProcessGroupObservation::new(
                process.clone(),
                self.1,
                Vec::new(),
                ProcessGroupCompleteness::Complete,
                None,
                TcpListenerObservation::Absent,
                state,
            ))
        })
    }
}

#[test]
fn quiet_server_requires_a_baseline_and_output_from_the_current_process() {
    let fixture = Fixture::new(2);
    fixture.activate(profile(
        "server",
        DetectionArchetype::Server,
        Vec::new(),
        OutputExclusions::default(),
    ));
    let session = session("quiet-server");
    let first_process = process("process-1");
    fixture.begin(&session, OutputOffset::ZERO);
    let appended = fixture.output(&session, b"listening");
    let collector = fixture.collector(2);

    let initial = collect(&collector, &session, &first_process, Some("server"));
    assert_eq!(
        decision(initial).execution().state(),
        ExecutionState::Unknown
    );
    let quiet = collect(&collector, &session, &first_process, Some("server"));
    let selected = decision(quiet);
    assert_eq!(selected.execution().state(), ExecutionState::Quiescent);
    assert_eq!(
        selected.execution().basis().disposition(),
        undercurrent_domain::EvidenceDisposition::Inference
    );

    fixture.begin(&session, appended);
    let restarted = collect(&collector, &session, &process("process-2"), Some("server"));
    assert_eq!(
        decision(restarted).execution().state(),
        ExecutionState::Unknown
    );
    assert_eq!(collector.pressure().server_idle_matches, 1);
}

#[test]
fn profile_row_exclusions_prevent_ambient_motion_claims() {
    let fixture = Fixture::new(2);
    fixture.activate(profile(
        "filtered",
        DetectionArchetype::AnimatedAgent,
        Vec::new(),
        OutputExclusions {
            top_rows: 0,
            bottom_rows: 1,
        },
    ));
    let session = session("filtered-motion");
    let process = process("process-1");
    fixture.begin(&session, OutputOffset::ZERO);
    fixture.output(&session, b"\x1b[1;1Hwork\x1b[4;1Hclock-a");
    let collector = fixture.collector(2);
    let _ = collect(&collector, &session, &process, Some("filtered"));

    fixture.output(&session, b"\x1b[4;1Hclock-b");
    let ambient = collect(&collector, &session, &process, Some("filtered"));

    assert_eq!(
        decision(ambient).execution().state(),
        ExecutionState::Unknown
    );
    assert_eq!(collector.pressure().motion_matches, 0);
}

#[test]
fn motion_cache_is_bounded_lru_and_process_changes_reset_the_baseline() {
    let fixture = Fixture::new(3);
    let first = session("cache-first");
    let second = session("cache-second");
    fixture.begin(&first, OutputOffset::ZERO);
    fixture.begin(&second, OutputOffset::ZERO);
    fixture.output(&first, b"private-first");
    fixture.output(&second, b"private-second");
    let collector = fixture.collector(1);

    let _ = collect(&collector, &first, &process("process-1"), None);
    let _ = collect(&collector, &second, &process("process-2"), None);
    let readmitted = collect(&collector, &first, &process("process-1"), None);

    assert_eq!(
        decision(readmitted).execution().state(),
        ExecutionState::Unknown
    );
    assert_eq!(collector.pressure().tracked_sessions, 1);
    assert_eq!(collector.pressure().evictions, 2);
    assert!(!format!("{collector:?}").contains("private-first"));
    assert!(!format!("{collector:?}").contains("private-second"));
}

#[test]
fn effective_profile_changes_reset_the_motion_baseline() {
    let fixture = Fixture::new(1);
    fixture.activate(profile(
        "first-agent",
        DetectionArchetype::AnimatedAgent,
        Vec::new(),
        OutputExclusions::default(),
    ));
    let session = session("profile-transition");
    let process = process("process-1");
    fixture.begin(&session, OutputOffset::ZERO);
    fixture.output(&session, b"first frame");
    let collector = fixture.collector(1);
    let _ = collect(&collector, &session, &process, Some("first-agent"));

    fixture.activate(profile(
        "second-agent",
        DetectionArchetype::AnimatedAgent,
        Vec::new(),
        OutputExclusions::default(),
    ));
    fixture.output(&session, b"\rsecond frame");
    let switched = collect(&collector, &session, &process, Some("second-agent"));

    assert_eq!(
        decision(switched).execution().state(),
        ExecutionState::Unknown
    );
    assert_eq!(collector.pressure().motion_matches, 0);

    fixture.output(&session, b"\rthird frame");
    let continued = collect(&collector, &session, &process, Some("second-agent"));
    assert_eq!(
        decision(continued).execution().state(),
        ExecutionState::Active
    );
    assert_eq!(collector.pressure().motion_matches, 1);
}

#[test]
fn missing_profiles_and_zero_limits_are_typed_and_payload_free() {
    let fixture = Fixture::new(1);
    let session = session("missing-profile");
    fixture.begin(&session, OutputOffset::ZERO);
    fixture.output(&session, b"private-screen-payload");
    let collector = fixture.collector(1);

    let error = block_on(collector.collect(
        &session,
        &process("process-1"),
        Some(&profile_name("absent")),
        &ForegroundObservation::unknown(TimestampMillis::new(1)),
    ))
    .expect_err("referenced profile must exist");

    assert_eq!(error.kind(), PortErrorKind::InvalidData);
    assert_eq!(error.operation(), PortOperation::CollectForegroundEvidence);
    assert!(!format!("{error:?}").contains("private-screen-payload"));

    let mut invalid = limits(1);
    invalid.tracked_sessions = 0;
    assert_eq!(
        invalid
            .validate()
            .expect_err("zero capacity must fail")
            .field(),
        "foreground_tracked_sessions"
    );
    let names = RuntimeEvidenceSource::ALL
        .into_iter()
        .map(RuntimeEvidenceSource::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(names.len(), RuntimeEvidenceSource::ALL.len());
}

struct Fixture {
    catalog: Arc<InMemoryDetectionProfileCatalog>,
    patterns: Arc<RegexPatternEngine>,
    screens: Arc<VtTerminalScreenStore>,
    output: TerminalOutputStore,
    clock: Arc<FixedClock>,
    cooperative: Arc<CooperativeSignalStore>,
}

impl Fixture {
    fn new(screen_sessions: usize) -> Self {
        Self {
            catalog: Arc::new(
                InMemoryDetectionProfileCatalog::new(8).expect("profile catalog limit"),
            ),
            patterns: Arc::new(
                RegexPatternEngine::new(RegexPatternEngineLimits {
                    cached_patterns: 32,
                    compiled_size_bytes: 1_048_576,
                    determinize_size_bytes: 1_048_576,
                    match_text_bytes: 8_192,
                })
                .expect("regex limits"),
            ),
            screens: Arc::new(
                VtTerminalScreenStore::new(TerminalScreenLimits {
                    scrollback_lines: 0,
                    sessions: screen_sessions,
                    snapshot_bytes: 8_192,
                })
                .expect("screen limits"),
            ),
            output: TerminalOutputStore::new(TerminalOutputLimits {
                per_session_bytes: 8_192,
                global_bytes: 32_768,
                chunk_bytes: 1_024,
                append_bytes: 4_096,
                replay_bytes: 8_192,
                sessions: screen_sessions,
                subscriptions: 4,
            })
            .expect("output limits"),
            clock: Arc::new(FixedClock(TimestampMillis::new(100))),
            cooperative: Arc::new(
                CooperativeSignalStore::new(screen_sessions).expect("cooperative signal limit"),
            ),
        }
    }

    fn activate(&self, profile: DetectionProfile) {
        self.catalog
            .replace(vec![profile], Vec::new(), self.patterns.as_ref())
            .expect("profile activation");
    }

    fn begin(&self, session_id: &SessionId, offset: OutputOffset) {
        self.screens
            .begin_session(session_id.clone(), terminal_size(), offset)
            .expect("screen begins");
    }

    fn output(&self, session_id: &SessionId, bytes: &[u8]) -> OutputOffset {
        self.screens
            .observe_output(&self.output, session_id, bytes)
            .expect("output is retained")
            .end
    }

    fn collector(&self, tracked_sessions: usize) -> TerminalForegroundEvidenceCollector {
        let clock: Arc<dyn Clock> = self.clock.clone();
        let catalog: Arc<dyn undercurrent_application::DetectionProfileCatalog> =
            self.catalog.clone();
        let patterns: Arc<dyn ProfilePatternEngine> = self.patterns.clone();
        TerminalForegroundEvidenceCollector::new(
            clock,
            catalog,
            patterns,
            self.screens.clone(),
            self.cooperative.clone(),
            limits(tracked_sessions),
        )
        .expect("collector")
    }
}

struct FixedClock(TimestampMillis);

impl Clock for FixedClock {
    fn now(&self) -> TimestampMillis {
        self.0
    }
}

fn collect(
    collector: &TerminalForegroundEvidenceCollector,
    session_id: &SessionId,
    process: &ProcessInstanceId,
    profile: Option<&str>,
) -> Vec<ForegroundEvidence> {
    let profile = profile.map(profile_name);
    block_on(collector.collect(
        session_id,
        process,
        profile.as_ref(),
        &ForegroundObservation::unknown(TimestampMillis::new(1)),
    ))
    .expect("evidence collection")
}

fn decision(evidence: Vec<ForegroundEvidence>) -> ForegroundObservation {
    decision_at(evidence, TimestampMillis::new(100))
}

fn decision_at(evidence: Vec<ForegroundEvidence>, now: TimestampMillis) -> ForegroundObservation {
    ForegroundDecisionPolicy::new(16, Duration::from_secs(1), confidence(1))
        .expect("decision policy")
        .decide(evidence, now)
        .expect("foreground decision")
        .observation()
        .clone()
}

fn profile(
    name: &str,
    archetype: DetectionArchetype,
    prompts: Vec<ForegroundRule>,
    exclusions: OutputExclusions,
) -> DetectionProfile {
    DetectionProfile::new(
        profile_name(name),
        None,
        false,
        ProfileMatchRules::default(),
        archetype,
        undercurrent_domain::InputDeliveryPolicy::immediate(),
        (archetype == DetectionArchetype::AnimatedAgent)
            .then(|| available_interaction("agent.composer")),
        prompts,
        Vec::new(),
        false,
        false,
        exclusions,
        profile_limits(),
    )
    .expect("profile")
}

fn prompt(pattern: &str, semantic: &str, confidence_value: u16) -> ForegroundRule {
    ForegroundRule::new(
        ForegroundClaim::Interaction(
            InteractionClaim::new(
                InteractionState::Requested,
                Some(InteractionModality::Options),
                Some(ObservationSemanticId::new(semantic).expect("semantic")),
            )
            .expect("interaction claim"),
        ),
        vec![PatternExpression::new(pattern, 1_024).expect("pattern")],
        Vec::new(),
        PromptTraits::default(),
        confidence(confidence_value),
        None,
        profile_limits(),
    )
    .expect("prompt rule")
}

fn available_interaction(semantic: &str) -> InteractionClaim {
    InteractionClaim::new(
        InteractionState::Available,
        Some(InteractionModality::Text),
        Some(ObservationSemanticId::new(semantic).expect("semantic")),
    )
    .expect("interaction claim")
}

fn limits(tracked_sessions: usize) -> TerminalForegroundEvidenceLimits {
    TerminalForegroundEvidenceLimits {
        tracked_sessions,
        profile_samples: 8,
        profile_sample_bytes: 8_192,
        motion_rows: 4,
        motion_confidence: confidence(8_500),
        server_idle_confidence: confidence(7_000),
    }
}

fn profile_limits() -> DetectionProfileLimits {
    DetectionProfileLimits {
        max_description_bytes: 1_024,
        max_binary_patterns: 8,
        max_argument_patterns: 8,
        max_environment_names: 8,
        max_environment_name_bytes: 128,
        max_foreground_rules: 8,
        max_background_rules: 8,
        max_patterns_per_rule: 8,
        max_pattern_bytes: 1_024,
        max_trait_needles: 8,
        max_trait_text_bytes: 256,
        max_input_submission_delay_millis: 2_000,
    }
}

fn terminal_size() -> TerminalSize {
    TerminalSize::new(20, 4, 100, 100).expect("terminal size")
}

fn session(value: &str) -> SessionId {
    SessionId::new(value).expect("session ID")
}

fn process(value: &str) -> ProcessInstanceId {
    ProcessInstanceId::new(value).expect("process ID")
}

fn profile_name(value: &str) -> ProfileName {
    ProfileName::new(value).expect("profile name")
}

fn confidence(value: u16) -> Confidence {
    Confidence::from_basis_points(value).expect("confidence")
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}
