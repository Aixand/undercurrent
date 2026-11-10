// SPDX-License-Identifier: Apache-2.0

//! Run pure domain transitions with synthetic identities, timestamps, and evidence.
//! No command is executed and no daemon, database, PTY, or secret is accessed.

use std::time::Duration;

use undercurrent_domain::*;

fn main() -> DomainResult<()> {
    let limits = ExecutionLimits {
        max_arguments: 8,
        max_command_bytes: 1_024,
        max_environment_entries: 8,
        max_environment_bytes: 1_024,
        max_path_bytes: 1_024,
        max_health_rules: 8,
        max_health_rule_bytes: 1_024,
    };
    let launch = LaunchSpec::new(
        CommandSpec::new("example-program", Vec::new(), limits)?,
        EnvironmentVariables::new(Vec::new(), limits)?,
        None,
        None,
        TerminalSize::new(80, 24, 200, 100)?,
    );
    let configuration = SessionConfiguration::new(
        launch,
        ExecutionPolicy::new(
            RestartConfig::never(),
            StopPolicy::new(StopSignal::Terminate, Duration::from_secs(5))?,
            true,
            false,
        ),
        HealthRules::default(),
        None,
        Labels::default(),
        Metadata::default(),
    );
    let (mut session, created) = Session::create(
        SessionId::new("example-session")?,
        SessionName::new("example")?,
        configuration,
        4,
        TimestampMillis::new(1_000),
    )?;
    assert_eq!(created.revision(), Revision::new(1));

    // This is a simulated adapter fact, not evidence of a real OS process.
    session.record_process_ownership(
        ProcessIdentity::new(
            ProcessInstanceId::new("example-incarnation-1")?,
            TimestampMillis::new(1_001),
        ),
        TimestampMillis::new(1_001),
    )?;
    session.transition_lifecycle(LifecycleState::Running, TimestampMillis::new(1_002))?;

    let decision = ForegroundDecisionPolicy::new(8, Duration::from_secs(1), Confidence::NONE)?
        .decide(
            vec![ForegroundEvidence::new(
                EvidenceSource::new("example:cooperative")?,
                EvidencePriority::Cooperative,
                EvidenceDisposition::Affirmative,
                ForegroundClaim::Interaction(InteractionClaim::new(
                    InteractionState::Available,
                    Some(InteractionModality::Text),
                    None,
                )?),
                Confidence::CERTAIN,
                TimestampMillis::new(1_002),
            )?],
            TimestampMillis::new(1_003),
        )?;
    session.update_foreground_observation(decision)?;
    assert!(!session.foreground().interaction().is_actionable());
    assert_eq!(session.lifecycle_revision(), Revision::new(3));
    assert_eq!(Session::rehydrate(session.state())?, session);
    println!(
        "session={} lifecycle={} revision={}",
        session.id(),
        session.lifecycle(),
        session.revision().value()
    );

    // A separate synthetic input attempt models response loss. In production,
    // acceptance and settlement are store commits, and reconciliation needs
    // retained adapter evidence. No input is delivered by this example.
    let fence = FencingToken::new(1)?;
    let mut attempt = OperationAttempt::accepted(
        OperationAttemptId::new("example:input-1")?,
        Some(session.id().clone()),
        ProcessMutationKind::WriteInput,
        fence,
        PayloadDigest::of_bytes(b"synthetic canonical input"),
        TimestampMillis::new(1_004),
    );
    attempt.settle(
        fence,
        EffectCertainty::Unknown,
        None,
        TimestampMillis::new(1_005),
    )?;
    assert!(attempt.state().requires_reconciliation());
    attempt.reconcile(
        EffectCertainty::KnownApplied,
        Some(PayloadDigest::of_bytes(b"synthetic retained receipt")),
        TimestampMillis::new(1_006),
    )?;
    println!(
        "attempt_reconciliation_required={}",
        attempt.state().requires_reconciliation()
    );

    let integrity = OutputChunkIntegrity::for_payload(b"abc", 9)?;
    let verified = integrity.verify(b"abc") == OutputIntegrityStatus::Verified { truncated: true };
    assert!(verified);
    println!(
        "output_verified={verified} truncated={}",
        integrity.truncated()
    );
    Ok(())
}
