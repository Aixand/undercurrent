// SPDX-License-Identifier: Apache-2.0

//! Shared durable process-activation transaction.

use undercurrent_domain::{LifecycleState, ProcessIdentity, ProcessMutationKind, Session};

use super::{MutationCore, SessionMutationError, not_before};
use crate::{
    DiagnosticEventName, DiagnosticLevel, OperationAttemptAdmission, PortError, PortOperation,
    ProcessInstance, ProcessRuntime, ProcessUnbindOutcome, SessionProcessDirectory,
    process_mutation_admission,
};

/// Exact activation stage that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessActivationStage {
    Spawn,
    RecordProcessOwnership,
    CommitProcessOwnership,
    BindProcess,
    ArmProcessExit,
    EnterRunning,
    CommitRunning,
}

/// Typed primary failure from process activation.
#[derive(Debug)]
pub(crate) enum ProcessActivationFailureSource {
    Mutation(SessionMutationError),
    Port(PortError),
}

/// Whether the spawned process reached the supervisor directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessBindingState {
    Unbound,
    Bound,
}

/// Failed activation with the last known durable aggregate and process truth.
#[derive(Debug)]
pub(crate) struct ProcessActivationFailure {
    pub(crate) stage: ProcessActivationStage,
    pub(crate) source: ProcessActivationFailureSource,
    pub(crate) session: Session,
    pub(crate) process: Option<ProcessInstance>,
    pub(crate) binding: ProcessBindingState,
}

/// Successfully activated durable session and exact process incarnation.
#[derive(Debug)]
pub(crate) struct ActivatedProcess {
    pub(crate) session: Session,
    pub(crate) process: ProcessInstance,
}

/// Best available result of terminating one partially activated process.
#[derive(Debug, Default)]
pub(crate) struct ProcessCompensationReport {
    pub(crate) force_stop_failure: Option<PortError>,
    pub(crate) unbind_failure: Option<PortError>,
    pub(crate) unbind_conflict: bool,
}

impl ProcessCompensationReport {
    /// Returns whether durable lifecycle settlement may safely discard process
    /// ownership after compensation. This function performs no IO and does not
    /// panic.
    pub(crate) const fn safe_to_settle(&self) -> bool {
        self.force_stop_failure.is_none() && self.unbind_failure.is_none() && !self.unbind_conflict
    }
}

/// Spawns, owns, binds, and durably activates one `starting` session.
///
/// The caller must retain session-local mutation ownership for the complete
/// operation. Failure returns the last state known to be durable, the spawned
/// process when one exists, and whether exact directory cleanup is required.
/// This function performs process, directory, persistence, projection, and
/// telemetry IO. It does not compensate or select caller-specific failure
/// policy, and it does not panic.
pub(crate) async fn activate_process(
    core: &MutationCore<'_>,
    runtime: &dyn ProcessRuntime,
    processes: &dyn SessionProcessDirectory,
    mut session: Session,
    external_spawn_admission: Option<&OperationAttemptAdmission>,
) -> Result<ActivatedProcess, ProcessActivationFailure> {
    let spawn_started_at = not_before(core.clock.now(), session.updated_at());
    core.emit(
        DiagnosticEventName::ProcessSpawnStarted,
        DiagnosticLevel::Debug,
        spawn_started_at,
        Some(session.id().clone()),
        Some(PortOperation::SpawnProcess),
        None,
    );
    let spawn_admission = external_spawn_admission.map_or_else(
        || {
            process_mutation_admission(
                None,
                session.id().clone(),
                ProcessMutationKind::Spawn,
                b"activate_process.spawn.v0.1.0-beta",
                spawn_started_at,
            )
        },
        |admission| admission.for_target_session(session.id().clone()),
    );
    let process = match runtime
        .spawn_admitted(
            &spawn_admission,
            session.id(),
            session.configuration().launch(),
        )
        .await
    {
        Ok(process) => process,
        Err(source) => {
            let failed_at = core.clock.now();
            core.emit_port_failure(failed_at, Some(session.id().clone()), &source);
            core.emit(
                DiagnosticEventName::ProcessSpawnFailed,
                DiagnosticLevel::for_port_error(source.kind()),
                failed_at,
                Some(session.id().clone()),
                Some(PortOperation::SpawnProcess),
                Some(source.kind()),
            );
            return Err(ProcessActivationFailure {
                stage: ProcessActivationStage::Spawn,
                source: ProcessActivationFailureSource::Port(source),
                session,
                process: None,
                binding: ProcessBindingState::Unbound,
            });
        }
    };
    core.emit(
        DiagnosticEventName::ProcessSpawnSucceeded,
        DiagnosticLevel::Info,
        process.started_at,
        Some(session.id().clone()),
        Some(PortOperation::SpawnProcess),
        None,
    );

    let durable_starting = session.clone();
    let expected_revision = session.revision();
    let ownership_committed_at = not_before(
        process.started_at,
        not_before(core.clock.now(), session.updated_at()),
    );
    let ownership_change = match session
        .record_process_ownership(
            ProcessIdentity::new(process.id.clone(), process.started_at),
            ownership_committed_at,
        )
        .map_err(SessionMutationError::Domain)
    {
        Ok(Some(change)) => change,
        Ok(None) => {
            return Err(post_spawn_failure(
                ProcessActivationStage::RecordProcessOwnership,
                ProcessActivationFailureSource::Mutation(
                    SessionMutationError::InvariantViolation {
                        rule: "new process must produce a durable ownership change",
                    },
                ),
                durable_starting,
                process,
                ProcessBindingState::Unbound,
            ));
        }
        Err(source) => {
            return Err(post_spawn_failure(
                ProcessActivationStage::RecordProcessOwnership,
                ProcessActivationFailureSource::Mutation(source),
                durable_starting,
                process,
                ProcessBindingState::Unbound,
            ));
        }
    };
    if let Err(source) = core
        .commit(
            &session,
            expected_revision,
            ownership_change,
            ownership_committed_at,
        )
        .await
    {
        return Err(post_spawn_failure(
            ProcessActivationStage::CommitProcessOwnership,
            ProcessActivationFailureSource::Mutation(source),
            durable_starting,
            process,
            ProcessBindingState::Unbound,
        ));
    }

    if let Err(source) = processes.bind(session.id(), process.clone()) {
        core.emit_port_failure(core.clock.now(), Some(session.id().clone()), &source);
        return Err(post_spawn_failure(
            ProcessActivationStage::BindProcess,
            ProcessActivationFailureSource::Port(source),
            session,
            process,
            ProcessBindingState::Unbound,
        ));
    }

    let arm_payload = format!("{:?}", process.id);
    let arm_admission = process_mutation_admission(
        None,
        session.id().clone(),
        ProcessMutationKind::ArmExit,
        arm_payload.as_bytes(),
        core.clock.now(),
    );
    if let Err(source) = runtime
        .arm_exit_admitted(&arm_admission, session.id(), &process.id)
        .await
    {
        core.emit_port_failure(core.clock.now(), Some(session.id().clone()), &source);
        return Err(post_spawn_failure(
            ProcessActivationStage::ArmProcessExit,
            ProcessActivationFailureSource::Port(source),
            session,
            process,
            ProcessBindingState::Bound,
        ));
    }

    let durable_starting = session.clone();
    let expected_revision = session.revision();
    let running_committed_at = not_before(core.clock.now(), session.updated_at());
    let running_change = match session
        .transition_lifecycle(LifecycleState::Running, running_committed_at)
        .map_err(SessionMutationError::Domain)
    {
        Ok(Some(change)) => change,
        Ok(None) => {
            return Err(post_spawn_failure(
                ProcessActivationStage::EnterRunning,
                ProcessActivationFailureSource::Mutation(
                    SessionMutationError::InvariantViolation {
                        rule: "starting process must produce a running lifecycle change",
                    },
                ),
                durable_starting,
                process,
                ProcessBindingState::Bound,
            ));
        }
        Err(source) => {
            return Err(post_spawn_failure(
                ProcessActivationStage::EnterRunning,
                ProcessActivationFailureSource::Mutation(source),
                durable_starting,
                process,
                ProcessBindingState::Bound,
            ));
        }
    };
    if let Err(source) = core
        .commit(
            &session,
            expected_revision,
            running_change,
            running_committed_at,
        )
        .await
    {
        return Err(post_spawn_failure(
            ProcessActivationStage::CommitRunning,
            ProcessActivationFailureSource::Mutation(source),
            durable_starting,
            process,
            ProcessBindingState::Bound,
        ));
    }
    core.emit(
        DiagnosticEventName::LifecycleCommitted,
        DiagnosticLevel::Info,
        running_committed_at,
        Some(session.id().clone()),
        Some(PortOperation::CommitSession),
        None,
    );
    Ok(ActivatedProcess { session, process })
}

/// Force-terminates one spawned process and removes only its exact binding.
///
/// A failed or uncertain force operation leaves the binding untouched. A
/// replacement binding is preserved and reported as a conflict. Diagnostics
/// never include command or process payloads. This function performs process,
/// directory, and telemetry IO and does not panic.
pub(crate) async fn compensate_process(
    core: &MutationCore<'_>,
    runtime: &dyn ProcessRuntime,
    processes: &dyn SessionProcessDirectory,
    session: &Session,
    process: &ProcessInstance,
    binding: ProcessBindingState,
) -> ProcessCompensationReport {
    core.emit(
        DiagnosticEventName::CompensationStarted,
        DiagnosticLevel::Warn,
        not_before(core.clock.now(), session.updated_at()),
        Some(session.id().clone()),
        Some(PortOperation::ForceProcess),
        None,
    );
    let mut report = ProcessCompensationReport::default();
    let force_payload = format!("{:?}", process.id);
    let force_admission = process_mutation_admission(
        None,
        session.id().clone(),
        ProcessMutationKind::ForceStop,
        force_payload.as_bytes(),
        core.clock.now(),
    );
    if let Err(error) = runtime
        .force_stop_admitted(&force_admission, session.id(), &process.id)
        .await
    {
        core.emit_port_failure(core.clock.now(), Some(session.id().clone()), &error);
        report.force_stop_failure = Some(error);
        emit_compensation_result(core, session, &report);
        return report;
    }
    if binding == ProcessBindingState::Bound {
        match processes.unbind_if(session.id(), &process.id) {
            Ok(ProcessUnbindOutcome::Unbound(_) | ProcessUnbindOutcome::Missing) => {}
            Ok(ProcessUnbindOutcome::ProcessMismatch) => report.unbind_conflict = true,
            Err(error) => {
                core.emit_port_failure(core.clock.now(), Some(session.id().clone()), &error);
                report.unbind_failure = Some(error);
            }
        }
    }
    emit_compensation_result(core, session, &report);
    report
}

fn emit_compensation_result(
    core: &MutationCore<'_>,
    session: &Session,
    report: &ProcessCompensationReport,
) {
    let completed = report.safe_to_settle();
    core.emit(
        if completed {
            DiagnosticEventName::CompensationSucceeded
        } else {
            DiagnosticEventName::CompensationFailed
        },
        if completed {
            DiagnosticLevel::Warn
        } else {
            DiagnosticLevel::Error
        },
        core.clock.now(),
        Some(session.id().clone()),
        Some(PortOperation::ForceProcess),
        None,
    );
}

fn post_spawn_failure(
    stage: ProcessActivationStage,
    source: ProcessActivationFailureSource,
    session: Session,
    process: ProcessInstance,
    binding: ProcessBindingState,
) -> ProcessActivationFailure {
    ProcessActivationFailure {
        stage,
        source,
        session,
        process: Some(process),
        binding,
    }
}
