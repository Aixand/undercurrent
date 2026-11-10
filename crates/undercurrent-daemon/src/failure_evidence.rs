// SPDX-License-Identifier: Apache-2.0

//! Failure capture usable before daemon configuration and logging are available.

use undercurrent_diagnostics::{Component, Recorder};

pub fn initialize_failure_evidence(component: Component) {
    let paths = undercurrent_config::UndercurrentPaths::resolve(
        &undercurrent_config::PathEnvironment::from_process_env(),
    )
    .ok();
    let incident = std::env::var(undercurrent_diagnostics::INCIDENT_ENV).ok();
    undercurrent_diagnostics::install_process(Recorder::new(
        paths.as_ref().map(|p| p.home()),
        component,
        env!("CARGO_PKG_VERSION"),
        incident,
    ));
}

pub fn record_process_failure(
    stage: &str,
    category: &str,
    error: &(dyn std::error::Error + 'static),
) {
    if let Some(recorder) = undercurrent_diagnostics::process() {
        let record = recorder.record(stage, category, Some(error));
        let _ = recorder.save(&record);
        eprintln!("error reference: {}", record.incident_id);
    }
}
