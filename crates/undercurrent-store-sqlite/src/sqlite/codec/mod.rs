// SPDX-License-Identifier: Apache-2.0

//! Sole-version durable aggregate codec and its unversioned record helpers.
//!
//! Row version and JSON envelope version must both be supported. The codec
//! reconstructs domain values rather than deserializing domain internals. Its
//! explicit legacy defaults do not authorize arbitrary old layouts with the
//! same version number. Runtime dispatch policy JSON has a separate codec in
//! the database module.

mod aggregate;
mod current;
mod state;

pub(super) use aggregate::SessionCodecError;
pub(super) use current::SESSION_RECORD_VERSION;

/// Encodes the current durable aggregate record.
pub(super) fn encode_session(
    session: &undercurrent_domain::Session,
    max_record_bytes: usize,
) -> Result<Vec<u8>, SessionCodecError> {
    current::encode_session(session, max_record_bytes)
}

/// Decodes the durable aggregate version declared by its SQLite row.
pub(super) fn decode_session(
    bytes: &[u8],
    max_record_bytes: usize,
    record_version: u32,
) -> Result<undercurrent_domain::Session, SessionCodecError> {
    if record_version == SESSION_RECORD_VERSION {
        current::decode_session(bytes, max_record_bytes)
    } else {
        Err(SessionCodecError::UnsupportedVersion(record_version))
    }
}

/// Reports whether a SQLite row version can be decoded by this binary.
pub(super) const fn is_supported_session_record_version(version: u32) -> bool {
    version == SESSION_RECORD_VERSION
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use undercurrent_domain::{
        CommandSpec, EnvironmentVariables, ExecutionEnvironmentBinding, ExecutionLimits,
        ExecutionPolicy, HealthRules, Labels, LaunchSpec, Metadata, OutputCaptureMode,
        PathReference, PayloadDigest, ProfileBindingMode, ProfileName, ProfileSelectionOrigin,
        RestartConfig, Revision, Session, SessionConfiguration, SessionId, SessionName, StopPolicy,
        StopSignal, TerminalSize, TimestampMillis,
    };

    use super::*;

    #[test]
    fn unsupported_row_and_envelope_versions_are_rejected() {
        assert!(matches!(
            decode_session(b"not-json", 1_024, 2),
            Err(SessionCodecError::UnsupportedVersion(2))
        ));
        assert!(matches!(
            decode_session(
                br#"{"schema_version":99,"profile":{},"state":{}}"#,
                1_024,
                1
            ),
            Err(SessionCodecError::Json(_))
        ));
    }

    #[test]
    fn oversized_record_is_rejected_before_json_parsing() {
        assert!(matches!(
            decode_session(b"not-json", 3, SESSION_RECORD_VERSION),
            Err(SessionCodecError::RecordTooLarge {
                actual: 8,
                limit: 3
            })
        ));
    }

    #[test]
    fn launch_modes_survive_storage_and_missing_mode_remains_sealed() {
        let original = session();
        for mode in [
            undercurrent_domain::LaunchMode::Native,
            undercurrent_domain::LaunchMode::Sealed,
        ] {
            let launch = original.configuration().launch().clone();
            let command = launch.command().clone().with_launch_mode(mode);
            let selected = session_with_launch(launch.with_command(command));
            let bytes = encode_session(&selected, 64 * 1024).expect("encode");
            let decoded =
                decode_session(&bytes, 64 * 1024, SESSION_RECORD_VERSION).expect("decode");
            assert_eq!(
                decoded.configuration().launch().command().launch_mode(),
                mode
            );
            let text = String::from_utf8(bytes).expect("JSON");
            let without = text
                .replace("\"launch_mode\":\"native\",", "")
                .replace("\"launch_mode\":\"sealed\",", "");
            assert_ne!(text, without);
            assert!(decode_session(without.as_bytes(), 64 * 1024, SESSION_RECORD_VERSION).is_err());
        }
    }

    #[test]
    fn current_records_preserve_exact_lifecycle_revision() {
        let mut session = session();
        session
            .rename(
                SessionName::new("renamed").expect("name"),
                TimestampMillis::new(2),
            )
            .expect("rename")
            .expect("change");

        let bytes = encode_session(&session, 64 * 1024).expect("encode current record");
        let decoded = decode_session(&bytes, 64 * 1024, SESSION_RECORD_VERSION)
            .expect("decode current record");

        assert_eq!(decoded.revision(), Revision::new(2));
        assert_eq!(decoded.lifecycle_revision(), Revision::new(1));
    }

    #[test]
    fn current_records_preserve_adaptive_launch_profile_binding() {
        let mut session = session();
        let mut state = session.state();
        state
            .configuration
            .select_profile_if_absent(ProfileName::new("codex").expect("profile"));
        state
            .configuration
            .set_profile_binding(ProfileBindingMode::ForegroundAdaptive);
        session = Session::rehydrate(state).expect("binding state");

        let bytes = encode_session(&session, 64 * 1024).expect("encode current record");
        let decoded = decode_session(&bytes, 64 * 1024, SESSION_RECORD_VERSION)
            .expect("decode current record");

        assert_eq!(
            decoded.configuration().profile_origin(),
            Some(ProfileSelectionOrigin::LaunchCommand)
        );
        assert_eq!(
            decoded.configuration().profile_binding(),
            ProfileBindingMode::ForegroundAdaptive
        );
    }

    #[test]
    fn current_records_preserve_opaque_output_capture() {
        let session = session_with_output_capture(OutputCaptureMode::Opaque);

        let bytes = encode_session(&session, 64 * 1024).expect("encode opaque record");
        let decoded = decode_session(&bytes, 64 * 1024, SESSION_RECORD_VERSION)
            .expect("decode opaque record");

        assert_eq!(
            decoded.configuration().launch().output_capture_mode(),
            OutputCaptureMode::Opaque
        );
    }

    #[test]
    fn current_records_preserve_executable_receipt_and_environment_binding() {
        let limits = execution_limits();
        let expected = PayloadDigest::of_bytes(b"pinned executable");
        let receipt = PayloadDigest::of_bytes(b"resolution receipt");
        let executable_identity = PayloadDigest::of_bytes(b"executable object");
        let binding = PayloadDigest::of_bytes(b"environment binding");
        let root_identity = PayloadDigest::of_bytes(b"binding root object");
        let working_directory_identity = PayloadDigest::of_bytes(b"working directory object");
        let command = CommandSpec::new("tool://example-tool/1.2.3", Vec::new(), limits)
            .expect("command")
            .with_expected_executable_digest(Some(expected))
            .pin_executable_with_identity(
                "/home/runtime/bin/example-tool",
                expected,
                executable_identity,
                receipt,
                limits,
            )
            .expect("pinned command");
        let launch = LaunchSpec::new(
            command,
            EnvironmentVariables::new(Vec::new(), limits).expect("environment"),
            None,
            Some(PathReference::new("/home/workspace", 128).expect("working directory")),
            TerminalSize::new(80, 24, 400, 200).expect("terminal"),
        )
        .with_environment_binding(Some(
            ExecutionEnvironmentBinding::new(
                binding,
                PathReference::new("/home", 128).expect("environment root"),
            )
            .with_root_identity(Some(root_identity)),
        ))
        .with_working_directory_identity(Some(working_directory_identity));
        let session = session_with_launch(launch);

        let bytes = encode_session(&session, 64 * 1024).expect("encode prepared record");
        let decoded = decode_session(&bytes, 64 * 1024, SESSION_RECORD_VERSION)
            .expect("decode prepared record");
        let launch = decoded.configuration().launch();
        let resolution = launch
            .command()
            .executable_resolution()
            .expect("resolution receipt");
        assert_eq!(launch.command().program(), "/home/runtime/bin/example-tool");
        assert_eq!(
            resolution.requested_reference(),
            "tool://example-tool/1.2.3"
        );
        assert_eq!(resolution.content_digest(), expected);
        assert_eq!(resolution.object_identity(), Some(executable_identity));
        assert_eq!(resolution.receipt_digest(), receipt);
        assert_eq!(
            launch
                .environment_binding()
                .expect("environment binding")
                .digest(),
            binding
        );
        assert_eq!(
            launch
                .environment_binding()
                .expect("environment binding")
                .root_identity(),
            Some(root_identity)
        );
        assert_eq!(
            launch.working_directory_identity(),
            Some(working_directory_identity)
        );
    }

    #[test]
    fn optional_object_identities_do_not_supply_launch_provenance() {
        let limits = execution_limits();
        let digest = PayloadDigest::of_bytes(b"executable");
        let command = CommandSpec::new("example-tool", Vec::new(), limits)
            .expect("command")
            .pin_executable_with_identity(
                "/home/bin/example-tool",
                digest,
                PayloadDigest::of_bytes(b"executable object"),
                PayloadDigest::of_bytes(b"receipt"),
                limits,
            )
            .expect("prepared command");
        let launch = LaunchSpec::new(
            command,
            EnvironmentVariables::new(Vec::new(), limits).expect("environment"),
            None,
            Some(PathReference::new("/home/work", 128).expect("working directory")),
            TerminalSize::new(80, 24, 400, 200).expect("terminal"),
        )
        .with_environment_binding(Some(
            ExecutionEnvironmentBinding::new(
                PayloadDigest::of_bytes(b"binding"),
                PathReference::new("/home", 128).expect("environment root"),
            )
            .with_root_identity(Some(PayloadDigest::of_bytes(b"root object"))),
        ))
        .with_working_directory_identity(Some(PayloadDigest::of_bytes(b"cwd object")));
        let bytes = encode_session(&session_with_launch(launch), 64 * 1024)
            .expect("encode prepared record");
        let mut record: serde_json::Value =
            serde_json::from_slice(&bytes).expect("decode record JSON");
        let launch = record
            .pointer_mut("/state/aggregate/configuration/launch")
            .and_then(serde_json::Value::as_object_mut)
            .expect("launch record");
        assert!(launch.remove("working_directory_identity_sha256").is_some());
        let binding = launch
            .get_mut("environment_binding")
            .and_then(serde_json::Value::as_object_mut)
            .expect("binding record");
        assert!(binding.remove("root_identity_sha256").is_some());
        let resolution = launch
            .get_mut("command")
            .and_then(serde_json::Value::as_object_mut)
            .and_then(|command| command.get_mut("executable_resolution"))
            .and_then(serde_json::Value::as_object_mut)
            .expect("executable resolution record");
        assert!(resolution.remove("object_identity_sha256").is_some());

        let record_bytes = serde_json::to_vec(&record).expect("encode record");
        let decoded = decode_session(&record_bytes, 64 * 1024, SESSION_RECORD_VERSION)
            .expect("optional object identities remain absent");
        let launch = decoded.configuration().launch();
        assert_eq!(launch.working_directory_identity(), None);
        assert_eq!(
            launch
                .environment_binding()
                .expect("binding")
                .root_identity(),
            None
        );
        assert_eq!(
            launch
                .command()
                .executable_resolution()
                .expect("resolution")
                .object_identity(),
            None
        );
    }

    #[test]
    fn records_without_required_output_capture_mode_are_rejected() {
        let session = session();
        let bytes = encode_session(&session, 64 * 1024).expect("encode current record");
        let mut record: serde_json::Value =
            serde_json::from_slice(&bytes).expect("decode record JSON");
        let launch = record
            .pointer_mut("/state/aggregate/configuration/launch")
            .and_then(serde_json::Value::as_object_mut)
            .expect("launch record");
        assert!(launch.remove("output_capture_mode").is_some());
        let record_bytes = serde_json::to_vec(&record).expect("encode record");

        assert!(decode_session(&record_bytes, 64 * 1024, SESSION_RECORD_VERSION).is_err());
    }

    #[test]
    fn absent_optional_launch_receipts_decode_as_unprepared() {
        let bytes = encode_session(&session(), 64 * 1024).expect("encode current record");
        let mut record: serde_json::Value =
            serde_json::from_slice(&bytes).expect("decode record JSON");
        let launch = record
            .pointer_mut("/state/aggregate/configuration/launch")
            .and_then(serde_json::Value::as_object_mut)
            .expect("launch record");
        assert!(launch.remove("environment_binding").is_some());
        let command = launch
            .get_mut("command")
            .and_then(serde_json::Value::as_object_mut)
            .expect("command record");
        assert!(
            command
                .remove("expected_executable_digest_sha256")
                .is_some()
        );
        assert!(command.remove("executable_resolution").is_some());
        let record_bytes = serde_json::to_vec(&record).expect("encode record");

        let decoded = decode_session(&record_bytes, 64 * 1024, SESSION_RECORD_VERSION)
            .expect("decode record");
        assert!(
            decoded
                .configuration()
                .launch()
                .command()
                .executable_resolution()
                .is_none()
        );
        assert!(
            decoded
                .configuration()
                .launch()
                .environment_binding()
                .is_none()
        );
    }

    fn session() -> Session {
        session_with_output_capture(OutputCaptureMode::Terminal)
    }

    fn session_with_output_capture(output_capture_mode: OutputCaptureMode) -> Session {
        let limits = execution_limits();
        session_with_launch(
            LaunchSpec::new(
                CommandSpec::new("true", Vec::new(), limits).expect("command"),
                EnvironmentVariables::new(Vec::new(), limits).expect("environment"),
                None,
                None,
                TerminalSize::new(80, 24, 400, 200).expect("terminal"),
            )
            .with_output_capture_mode(output_capture_mode),
        )
    }

    fn session_with_launch(launch: LaunchSpec) -> Session {
        let limits = execution_limits();
        let configuration = SessionConfiguration::new(
            launch,
            ExecutionPolicy::new(
                RestartConfig::never(),
                StopPolicy::new(StopSignal::Terminate, Duration::from_secs(1)).expect("stop"),
                true,
                false,
            ),
            HealthRules::new(Vec::new(), limits).expect("health rules"),
            None,
            Labels::default(),
            Metadata::default(),
        );
        Session::create(
            SessionId::new("session-1").expect("session ID"),
            SessionName::new("worker").expect("session name"),
            configuration,
            4,
            TimestampMillis::new(1),
        )
        .expect("session")
        .0
    }

    const fn execution_limits() -> ExecutionLimits {
        ExecutionLimits {
            max_arguments: 4,
            max_command_bytes: 128,
            max_environment_entries: 4,
            max_environment_bytes: 128,
            max_path_bytes: 128,
            max_health_rules: 4,
            max_health_rule_bytes: 128,
        }
    }
}
