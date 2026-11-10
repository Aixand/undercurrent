// SPDX-License-Identifier: Apache-2.0

//! Process-entry failure capture; ordinary library presentation stays pure.

use crate::{CliCommand, CliError};
use undercurrent_diagnostics::{Component, Recorder, install_process, process};

pub(crate) fn initialize() {
    initialize_at(None);
}

pub(crate) fn initialize_at(home: Option<&std::path::Path>) {
    let paths = undercurrent_config::UndercurrentPaths::resolve(
        &undercurrent_config::PathEnvironment::from_process_env(),
    )
    .ok();
    install_process(Recorder::new(
        home.or_else(|| paths.as_ref().map(|p| p.home())),
        Component::Cli,
        env!("CARGO_PKG_VERSION"),
        None,
    ));
}

pub(crate) fn failure(command: Option<CliCommand>, error: &CliError) -> Option<&'static str> {
    if process().is_none() {
        initialize();
    }
    let recorder = process()?;
    let mut record = recorder.record("command", error.exit_code().as_str(), Some(error));
    record.command = command.map(|c| c.as_str().into());
    record.exit_code = Some(error.exit_code().code());
    record.effect_certainty = error.error_effect_certainty().map(|value| {
        match value {
            undercurrent_protocol::EffectCertaintyDto::KnownAbsent => "known_absent",
            undercurrent_protocol::EffectCertaintyDto::KnownApplied => "known_applied",
            undercurrent_protocol::EffectCertaintyDto::Unknown => "unknown",
        }
        .into()
    });
    let _ = recorder.save(&record);
    Some(recorder.incident_id())
}

pub(crate) fn unsuccessful_exit(
    command: CliCommand,
    exit: crate::CliProcessExit,
    context: crate::cli_support::CliContext,
) {
    if let Some(recorder) = process() {
        let mut record = recorder.record("command_exit", exit.category().as_str(), None);
        record.command = Some(command.as_str().into());
        record.exit_code = Some(exit.code());
        let _ = recorder.save(&record);
        if context.output.is_structured() {
            use std::io::Write;
            let mut stderr = std::io::stderr().lock();
            let _ = serde_json::to_writer(
                &mut stderr,
                &serde_json::json!({ "incident_id": recorder.incident_id(), "result_category": exit.category().as_str(), "exit_code": exit.code() }),
            );
            let _ = stderr.write_all(b"\n");
        } else {
            eprintln!("error reference: {}", recorder.incident_id());
        }
    }
}
