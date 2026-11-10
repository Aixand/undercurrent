// SPDX-License-Identifier: Apache-2.0

use undercurrent_protocol::{DaemonShutdownModeDto, ShutdownDaemonRequest};

#[test]
fn omitted_shutdown_mode_preserves_graceful_wire_shape() {
    let legacy = serde_json::json!({"drain_timeout_millis": 5000});
    let mut request: ShutdownDaemonRequest = serde_json::from_value(legacy).unwrap();
    assert_eq!(request.mode, DaemonShutdownModeDto::Graceful);
    let graceful = serde_json::to_value(&request).unwrap();
    assert!(graceful.get("mode").is_none());
    request.mode = DaemonShutdownModeDto::Reset;
    let reset = serde_json::to_value(&request).unwrap();
    assert_eq!(reset["mode"], "reset");
    assert_eq!(
        serde_json::from_value::<ShutdownDaemonRequest>(reset)
            .unwrap()
            .mode,
        DaemonShutdownModeDto::Reset
    );
}

#[test]
fn unknown_shutdown_mode_is_rejected() {
    assert!(
        serde_json::from_value::<ShutdownDaemonRequest>(serde_json::json!({
            "drain_timeout_millis": 5000, "mode": "unknown"
        }))
        .is_err()
    );
}

#[test]
fn restart_force_is_opt_in_on_the_wire_and_legacy_requests_round_trip() {
    let legacy = serde_json::json!({"session_id": "worker"});
    let mut request: undercurrent_protocol::RestartSessionRequest =
        serde_json::from_value(legacy).unwrap();
    assert!(!request.force);
    assert!(
        serde_json::to_value(&request)
            .unwrap()
            .get("force")
            .is_none()
    );
    request.force = true;
    assert_eq!(serde_json::to_value(&request).unwrap()["force"], true);
}
