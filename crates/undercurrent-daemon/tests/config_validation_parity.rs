// SPDX-License-Identifier: Apache-2.0
//! Sample component bounds and wire envelopes to detect shared-validation drift.
use undercurrent_config::{ConfigDiscovery, PathEnvironment, load_config};
use undercurrent_daemon::{DaemonConfigOverrides, load_daemon_config};

#[test]
fn default_input_transport_budgets_match_the_public_sdk_profile() {
    let config = undercurrent_config::UndercurrentProductConfig::default();
    let profile = undercurrent_sdk::StandaloneClientProfile::standard("config-parity").unwrap();
    assert_eq!(
        config.requests.request_bytes,
        undercurrent_sdk::DEFAULT_REQUEST_BYTES
    );
    assert_eq!(
        config.transport.max_frame_bytes,
        profile.hello().limits().max_frame_bytes()
    );
    assert_eq!(
        config.transport.connection_queue_bytes,
        profile.hello().limits().connection_queue_bytes()
    );
    assert_eq!(
        config.transport.stream_queue_bytes,
        profile.hello().limits().stream_queue_bytes()
    );
}

#[test]
fn shared_validation_covers_component_bounds() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("config.yaml");
    let discovery = ConfigDiscovery::from_path_env(
        PathEnvironment::default().with_undercurrent_home(home.path()),
    );
    let original =
        serde_json::to_value(undercurrent_config::UndercurrentProductConfig::default()).unwrap();
    let mut pointers = Vec::new();
    fn collect(value: &serde_json::Value, pointer: String, result: &mut Vec<String>) {
        if let Some(fields) = value.as_object() {
            for (key, value) in fields {
                collect(value, format!("{pointer}/{key}"), result);
            }
        } else if value.is_number() {
            result.push(pointer);
        }
    }
    collect(&original, String::new(), &mut pointers);
    let mut gaps = Vec::new();
    for pointer in pointers {
        for number in [0u64, 1, 2, 65536, u32::MAX as u64, u64::MAX] {
            let mut value = original.clone();
            *value.pointer_mut(&pointer).unwrap() = number.into();
            std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
            if load_config(&discovery).is_ok()
                && let Err(error) = load_daemon_config(&discovery, DaemonConfigOverrides::new())
            {
                gaps.push(format!("{pointer}={number}: {error:?}"));
            }
        }
    }
    assert!(
        gaps.is_empty(),
        "shared validator missed:\n{}",
        gaps.join("\n")
    );
}

#[test]
fn shared_frame_bounds_match_wire_codec_envelopes() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("config.yaml");
    let discovery = ConfigDiscovery::from_path_env(
        PathEnvironment::default().with_undercurrent_home(home.path()),
    );
    let defaults = undercurrent_config::UndercurrentProductConfig::default();
    for extra in [0, 1] {
        let payload = defaults.transport.max_frame_bytes
            - undercurrent_wire::MAX_CLIENT_FRAME_ENVELOPE_BYTES
            + extra;
        std::fs::write(&path, format!("requests:\n  request_bytes: {payload}\n")).unwrap();
        assert_eq!(load_config(&discovery).is_ok(), extra == 0);
        if extra == 0 {
            load_daemon_config(&discovery, DaemonConfigOverrides::new()).unwrap();
        }
    }
    for size in [12, 21, 22] {
        std::fs::write(
            &path,
            format!("transport:\n  handshake_frame_bytes: {size}\n"),
        )
        .unwrap();
        assert_eq!(
            load_config(&discovery).is_ok(),
            undercurrent_wire::HandshakeCodec::new(size).is_ok()
        );
    }
}
