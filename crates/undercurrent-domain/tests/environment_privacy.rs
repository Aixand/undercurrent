// SPDX-License-Identifier: Apache-2.0

use undercurrent_domain::{
    EnvironmentVariables, ExecutionLimits, RawEnvironmentClassification,
    SecretEnvironmentVariables, SecretReference, classify_raw_environment_entry,
};

const TEST_LIMITS: ExecutionLimits = ExecutionLimits {
    max_arguments: 16,
    max_command_bytes: 4096,
    max_environment_entries: 16,
    max_environment_bytes: 4096,
    max_path_bytes: 4096,
    max_health_rules: 16,
    max_health_rule_bytes: 4096,
};

#[test]
fn credential_classified_raw_environment_cannot_enter_durable_launch_state() {
    assert_eq!(
        classify_raw_environment_entry("API_TOKEN", "private-value"),
        RawEnvironmentClassification::CredentialClassified
    );
    assert!(
        EnvironmentVariables::new(
            [("API_TOKEN".to_owned(), "private-value".to_owned())],
            TEST_LIMITS,
        )
        .is_err()
    );

    let safe = EnvironmentVariables::new([("RUST_LOG".to_owned(), "info".to_owned())], TEST_LIMITS)
        .expect("explicit non-secret environment");
    safe.validate_non_secret().expect("persistence admission");
    assert!(!format!("{safe:?}").contains("info"));
}

#[test]
fn opaque_secret_environment_is_bounded_disjoint_and_redacted() {
    let ordinary =
        EnvironmentVariables::new([("RUST_LOG".to_owned(), "info".to_owned())], TEST_LIMITS)
            .expect("ordinary environment");
    let reference = "passport-handle-secret-canary";
    let secrets = SecretEnvironmentVariables::new(
        [(
            "SERVICE_CREDENTIAL".to_owned(),
            SecretReference::new(reference).expect("opaque reference"),
        )],
        &ordinary,
        TEST_LIMITS,
    )
    .expect("secret environment");

    assert_eq!(secrets.names().collect::<Vec<_>>(), ["SERVICE_CREDENTIAL"]);
    let debug = format!("{secrets:?}");
    assert!(!debug.contains(reference));
    assert!(debug.contains("entry_count"));

    let overlap = SecretEnvironmentVariables::new(
        [(
            "RUST_LOG".to_owned(),
            SecretReference::new("opaque").expect("opaque reference"),
        )],
        &ordinary,
        TEST_LIMITS,
    );
    assert!(overlap.is_err());
}

#[test]
fn empty_secret_bindings_recheck_tighter_entry_limits_in_all_launch_builders() {
    use undercurrent_domain::{CommandSpec, DomainError, LaunchSpec, TerminalSize};
    let ordinary = EnvironmentVariables::new(
        [
            ("LANG".to_owned(), "C".to_owned()),
            ("RUST_LOG".to_owned(), "info".to_owned()),
        ],
        TEST_LIMITS,
    )
    .unwrap();
    let tighter = ExecutionLimits {
        max_environment_entries: 1,
        ..TEST_LIMITS
    };
    let launch = LaunchSpec::new(
        CommandSpec::new("true", Vec::new(), TEST_LIMITS).unwrap(),
        ordinary.clone(),
        None,
        None,
        TerminalSize::new(80, 24, 400, 200).unwrap(),
    );
    let errors = [
        SecretEnvironmentVariables::new([], &ordinary, tighter).err(),
        launch
            .clone()
            .with_secret_environment(SecretEnvironmentVariables::default(), tighter)
            .err(),
        launch
            .with_resolved_environment(ordinary.clone(), tighter)
            .err(),
    ];
    for error in errors {
        assert!(matches!(
            error,
            Some(DomainError::CapacityExceeded {
                resource: "environment_entries",
                limit: 1
            })
        ));
    }
    for count in [2, 3] {
        let limits = ExecutionLimits {
            max_environment_entries: count,
            ..TEST_LIMITS
        };
        assert!(SecretEnvironmentVariables::new([], &ordinary, limits).is_ok());
    }
    let reference = SecretReference::new("opaque-handle").unwrap();
    assert!(
        SecretEnvironmentVariables::new(
            [("CREDENTIAL".to_owned(), reference)],
            &ordinary,
            ExecutionLimits {
                max_environment_entries: 2,
                ..TEST_LIMITS
            }
        )
        .is_err()
    );
    assert!(matches!(
        SecretEnvironmentVariables::new(
            [],
            &ordinary,
            ExecutionLimits {
                max_environment_bytes: 1,
                ..TEST_LIMITS
            }
        ),
        Err(DomainError::CapacityExceeded {
            resource: "environment_bytes",
            limit: 1
        })
    ));
}
