// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use serde::Serialize;
use undercurrent_cli::{
    CliCommand, CliContext, CliError, CliPresenter, CliSignalError, CommandTimeout, CommonExitCode,
    OutputFormat, Verbosity,
};
use undercurrent_protocol::{
    ProductError, ProductErrorCategory, ProductErrorDisposition, ProductOperation,
    ProductStreamError,
};
use undercurrent_sdk::ProductClientError;

#[derive(Serialize)]
struct SessionRow<'a> {
    id: &'a str,
    revision: u64,
    lifecycle: &'a str,
}

#[derive(Serialize)]
struct FiniteOutput<'a> {
    command: CliCommand,
    snapshot_revision: u64,
    sessions: [SessionRow<'a>; 1],
}

#[derive(Serialize)]
struct StreamOutput<'a> {
    record: &'a str,
    sequence: u64,
    session_id: &'a str,
    process_id: &'a str,
    lifecycle: &'a str,
}

#[derive(Serialize)]
struct ExitClass {
    name: &'static str,
    code: u8,
}

#[test]
fn public_cli_machine_contract_matches_v1_goldens_without_migration() {
    let commands: Vec<String> = CliCommand::ALL
        .into_iter()
        .map(|command| command.as_str().to_owned())
        .collect();
    let expected_commands: Vec<String> = serde_json::from_slice(include_bytes!(
        "../../../tests/conformance/package/cli-commands.json"
    ))
    .expect("command golden");
    assert_eq!(commands, expected_commands);

    let timeout = CommandTimeout::new(Duration::from_secs(1), Duration::from_secs(1))
        .expect("one second is within the public timeout bound");

    let mut finite = Vec::new();
    CliPresenter::new(CliContext::new(
        OutputFormat::Json,
        Verbosity::Normal,
        timeout,
    ))
    .write_finite(
        &mut finite,
        &FiniteOutput {
            command: CliCommand::Ls,
            snapshot_revision: 7,
            sessions: [SessionRow {
                id: "session-1",
                revision: 3,
                lifecycle: "running",
            }],
        },
        |_| "unused machine-output text".to_owned(),
    )
    .expect("finite machine output");
    assert_eq!(
        finite.as_slice(),
        include_bytes!("fixtures/uref-012-finite.json")
    );

    let mut stream = Vec::new();
    CliPresenter::new(CliContext::new(
        OutputFormat::Jsonl,
        Verbosity::Normal,
        timeout,
    ))
    .write_stream_item(
        &mut stream,
        &StreamOutput {
            record: "session_lifecycle",
            sequence: 9,
            session_id: "session-1",
            process_id: "process-7",
            lifecycle: "exited",
        },
        |_| "unused machine-output text".to_owned(),
    )
    .expect("stream machine output");
    assert_eq!(
        stream.as_slice(),
        include_bytes!("fixtures/uref-012-stream.jsonl")
    );

    let codes = [
        CommonExitCode::Success,
        CommonExitCode::Failure,
        CommonExitCode::InvalidInput,
        CommonExitCode::Incomplete,
        CommonExitCode::Denied,
        CommonExitCode::ApprovalRequired,
        CommonExitCode::Runtime,
        CommonExitCode::Interrupted,
    ];
    let exits = codes.map(|code| ExitClass {
        name: code.as_str(),
        code: code.code(),
    });
    let mut exit_bytes = serde_json::to_vec_pretty(&exits).expect("serialize exit classes");
    exit_bytes.push(b'\n');
    assert_eq!(
        exit_bytes.as_slice(),
        include_bytes!("fixtures/uref-012-exit-classes.json")
    );
}

#[test]
fn uref_034_failure_machine_contract_preserves_exact_taxonomy() {
    let cases = [
        (
            ProductErrorCategory::InvalidRequest,
            ProductErrorDisposition::SemanticRejection,
            "semantic",
        ),
        (
            ProductErrorCategory::Persistence,
            ProductErrorDisposition::InfrastructureFailure,
            "infrastructure",
        ),
        (
            ProductErrorCategory::Process,
            ProductErrorDisposition::RuntimeFailure,
            "runtime",
        ),
        (
            ProductErrorCategory::Transport,
            ProductErrorDisposition::TransportFailure,
            "transport",
        ),
        (
            ProductErrorCategory::ProtocolMismatch,
            ProductErrorDisposition::ProtocolFailure,
            "protocol",
        ),
        (
            ProductErrorCategory::TimedOut,
            ProductErrorDisposition::Timeout,
            "timeout",
        ),
        (
            ProductErrorCategory::Cancelled,
            ProductErrorDisposition::Cancellation,
            "cancellation",
        ),
        (
            ProductErrorCategory::FailedPrecondition,
            ProductErrorDisposition::UnknownInconclusive,
            "unknown",
        ),
    ];
    let timeout = CommandTimeout::new(Duration::from_secs(1), Duration::from_secs(1))
        .expect("one second is within the public timeout bound");
    let mut output = Vec::new();
    for (category, disposition, message) in cases {
        let error = CliError::Product(ProductClientError::Server(ProductError {
            effect_certainty: None,
            operation: Some(ProductOperation::ShutdownDaemon),
            category,
            disposition: Some(disposition),
            message: message.to_owned(),
            session_id: None,
            retryable: false,
            retry_after_millis: None,
            limit: None,
            resynchronization: None,
        }));
        CliPresenter::new(CliContext::new(
            OutputFormat::Jsonl,
            Verbosity::Normal,
            timeout,
        ))
        .write_failure(&mut output, Some(CliCommand::Daemon), &error)
        .expect("failure JSON Lines");
    }
    CliPresenter::new(CliContext::new(
        OutputFormat::Jsonl,
        Verbosity::Normal,
        timeout,
    ))
    .write_failure(
        &mut output,
        Some(CliCommand::Exec),
        &CliError::ExecOutputUnavailable { channel: "stdout" },
    )
    .expect("partial failure JSON Lines");

    assert_eq!(
        output.as_slice(),
        include_bytes!("fixtures/uref-034-failure-taxonomy.jsonl")
    );
}

#[test]
fn uref_034_legacy_omission_and_local_failure_classification_remain_exact() {
    let timeout = CommandTimeout::new(Duration::from_secs(1), Duration::from_secs(1))
        .expect("one second is within the public timeout bound");
    let legacy = CliError::Product(ProductClientError::Server(ProductError {
        effect_certainty: None,
        operation: Some(ProductOperation::ShutdownDaemon),
        category: ProductErrorCategory::FailedPrecondition,
        disposition: None,
        message: "legacy rejection".to_owned(),
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    }));
    let current = CliError::Product(ProductClientError::Server(ProductError {
        effect_certainty: None,
        operation: Some(ProductOperation::ShutdownDaemon),
        category: ProductErrorCategory::FailedPrecondition,
        disposition: Some(ProductErrorDisposition::UnknownInconclusive),
        message: "legacy rejection".to_owned(),
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    }));

    let mut machine = Vec::new();
    CliPresenter::new(CliContext::new(
        OutputFormat::Jsonl,
        Verbosity::Normal,
        timeout,
    ))
    .write_failure(&mut machine, Some(CliCommand::Daemon), &legacy)
    .expect("legacy machine failure");
    let report: serde_json::Value = serde_json::from_slice(&machine).expect("failure report");
    assert_eq!(report["product_error_category"], "failed_precondition");
    assert!(report.get("error_disposition").is_none());
    assert!(report.get("effect_certainty").is_none());

    let legacy_stream = CliError::Product(ProductClientError::StreamRejected(ProductStreamError {
        kind: None,
        category: ProductErrorCategory::FailedPrecondition,
        disposition: None,
        message: "legacy stream rejection".to_owned(),
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    }));
    let mut stream_machine = Vec::new();
    CliPresenter::new(CliContext::new(
        OutputFormat::Jsonl,
        Verbosity::Normal,
        timeout,
    ))
    .write_failure(
        &mut stream_machine,
        Some(CliCommand::Attach),
        &legacy_stream,
    )
    .expect("legacy stream machine failure");
    let stream_report: serde_json::Value =
        serde_json::from_slice(&stream_machine).expect("stream failure report");
    assert_eq!(
        stream_report["product_error_category"],
        "failed_precondition"
    );
    assert!(stream_report.get("error_disposition").is_none());

    let mut legacy_human = Vec::new();
    let mut current_human = Vec::new();
    CliPresenter::new(CliContext::new(
        OutputFormat::Text,
        Verbosity::Normal,
        timeout,
    ))
    .write_failure(&mut legacy_human, Some(CliCommand::Daemon), &legacy)
    .expect("legacy human failure");
    CliPresenter::new(CliContext::new(
        OutputFormat::Text,
        Verbosity::Normal,
        timeout,
    ))
    .write_failure(&mut current_human, Some(CliCommand::Daemon), &current)
    .expect("current human failure");
    assert_eq!(legacy_human, current_human);
    assert_eq!(legacy.exit_code(), current.exit_code());

    assert_eq!(
        CliError::ExecOutputIntegrity { channel: "stdout" }.error_disposition(),
        Some(ProductErrorDisposition::ProtocolFailure)
    );
    assert_eq!(
        CliError::PaginationInvariant {
            command: CliCommand::Ls,
        }
        .error_disposition(),
        Some(ProductErrorDisposition::ProtocolFailure)
    );
    assert_eq!(
        CliError::Signals(CliSignalError::ZeroStackSize).error_disposition(),
        Some(ProductErrorDisposition::InfrastructureFailure)
    );
}

#[test]
fn explicit_owner_effect_evidence_is_preserved_in_machine_failures() {
    use undercurrent_protocol::EffectCertaintyDto;
    let timeout = CommandTimeout::new(Duration::from_secs(1), Duration::from_secs(1)).unwrap();
    for (certainty, expected) in [
        (Some(EffectCertaintyDto::KnownAbsent), Some("known_absent")),
        (
            Some(EffectCertaintyDto::KnownApplied),
            Some("known_applied"),
        ),
        (Some(EffectCertaintyDto::Unknown), Some("unknown")),
        (None, None),
    ] {
        let error = CliError::Product(ProductClientError::Server(ProductError {
            operation: Some(ProductOperation::StartSession),
            category: ProductErrorCategory::CapacityExceeded,
            disposition: Some(ProductErrorDisposition::CapacityRejected),
            effect_certainty: certainty,
            message: "bounded owner refusal".into(),
            session_id: None,
            retryable: false,
            retry_after_millis: None,
            limit: Some(2),
            resynchronization: None,
        }));
        let mut output = Vec::new();
        CliPresenter::new(CliContext::new(
            OutputFormat::Json,
            Verbosity::Normal,
            timeout,
        ))
        .write_failure(&mut output, Some(CliCommand::Start), &error)
        .unwrap();
        let report: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(
            report
                .get("effect_certainty")
                .and_then(serde_json::Value::as_str),
            expected
        );
        assert_eq!(report["product_error_category"], "capacity_exceeded");
    }
}
