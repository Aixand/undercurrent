// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;

use undercurrent_application::PortOperation;

#[test]
fn port_operation_registry_is_unique_and_exposes_shutdown_host_telemetry_label() {
    let labels = PortOperation::ALL
        .iter()
        .map(|operation| operation.as_str())
        .collect::<BTreeSet<_>>();

    assert_eq!(labels.len(), PortOperation::ALL.len());
    assert_eq!(
        PortOperation::ShutdownProcessHost.as_str(),
        "shutdown_process_host"
    );
}
