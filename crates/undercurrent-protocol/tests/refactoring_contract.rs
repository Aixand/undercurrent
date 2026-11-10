// SPDX-License-Identifier: Apache-2.0

use undercurrent_protocol::*;

#[test]
fn execution_environment_selection_round_trips_and_binds_retry_identity() {
    let mut request: ProductRequest =
        serde_json::from_str(include_str!("fixtures/execute_in_session.json")).unwrap();
    let original = canonical_product_request_digest(&request).unwrap();
    let ProductRequest::ExecuteInSession(ref mut execute) = request else {
        panic!("exec")
    };
    assert!(!execute.env_launch);
    execute.env_launch = true;
    assert_ne!(
        original,
        canonical_product_request_digest(&request).unwrap()
    );
    let decoded: ProductRequest =
        serde_json::from_str(&serde_json::to_string(&request).unwrap()).unwrap();
    let ProductRequest::ExecuteInSession(execute) = decoded else {
        panic!("exec")
    };
    assert!(execute.env_launch);
}

#[test]
fn execution_directory_selection_round_trips_and_changes_request_digest() {
    let mut request: ProductRequest =
        serde_json::from_str(include_str!("fixtures/execute_in_session.json")).unwrap();
    let live_digest = canonical_product_request_digest(&request).unwrap();
    let ProductRequest::ExecuteInSession(ref mut execute) = request else {
        panic!("expected execute")
    };
    assert!(!execute.dir_launch);
    execute.dir_launch = true;
    assert_ne!(
        live_digest,
        canonical_product_request_digest(&request).unwrap()
    );
    let encoded = serde_json::to_string(&request).unwrap();
    let decoded: ProductRequest = serde_json::from_str(&encoded).unwrap();
    let ProductRequest::ExecuteInSession(execute) = decoded else {
        panic!("expected execute")
    };
    assert!(execute.dir_launch);
}

#[test]
fn current_mutation_payload_requires_contract_and_preserves_additive_fields() {
    let request: ProductRequest =
        serde_json::from_str(include_str!("fixtures/product_request.json"))
            .expect("current 0.1.0-beta fixture");
    let mutation = request.mutation().expect("mutation");
    assert_eq!(mutation.attempt.as_ref().expect("attempt").fencing_token, 7);
    assert!(mutation.compatible_unknown.contains_key("future_hint"));
    validate_mutation_contract(mutation, request.operation()).expect("current 0.1.0-beta contract");

    let encoded = serde_json::to_value(&request).expect("re-encode");
    assert_eq!(
        encoded["input"]["mutation"]["future_hint"]["mode"],
        "compatible"
    );
}

#[test]
fn daemon_supervised_execution_fixture_preserves_attempt_and_output_bound() {
    let request: ProductRequest =
        serde_json::from_str(include_str!("fixtures/execute_in_session.json"))
            .expect("execute fixture");
    assert_eq!(request.operation(), ProductOperation::ExecuteInSession);
    let ProductRequest::ExecuteInSession(execute) = request else {
        panic!("expected execute request");
    };
    assert_eq!(execute.session_id, "session-1");
    assert_eq!(execute.output_inline_limit_bytes, 4096);
    assert_eq!(
        execute
            .mutation
            .attempt
            .expect("attempt metadata")
            .fencing_token,
        8
    );
}

#[test]
fn authenticated_context_serializes_references_but_never_credentials() {
    let context = AuthenticatedRequestContext::new(
        AuthenticatedPrincipalRef::new("principal:operator").unwrap(),
        Some(TenantRefDto::new("tenant:one").unwrap()),
        AuthorityRefDto::new("authority:grant-1").unwrap(),
    );
    let json = serde_json::to_string(&context).expect("serialize context");
    assert!(json.contains("principal:operator"));
    assert!(!json.contains("token"));
    assert!(!json.contains("credential"));
}

#[test]
fn secret_reference_serialization_excludes_resolved_material() {
    let reference = SecretReferenceDto::new("secret://runtime/api-key").unwrap();
    let json = serde_json::to_string(&reference).expect("serialize reference");
    assert!(json.contains("secret://runtime/api-key"));
    assert!(!json.contains("private-token-value"));
}

#[test]
fn output_integrity_metadata_is_additive_and_explicit() {
    let page = TerminalOutputPageDto::Data {
        from_offset: 0,
        to_offset: 3,
        next_offset: 9,
        has_more: true,
        bytes: b"abc".to_vec(),
        payload_digest_sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            .into(),
        truncation: Some(OutputTruncationDto {
            source_bytes: 9,
            delivered_bytes: 3,
        }),
    };
    let json = serde_json::to_value(page).expect("serialize output");
    assert_eq!(json["payload_digest_sha256"].as_str().unwrap().len(), 64);
    assert_eq!(json["truncation"]["source_bytes"], 9);
}

#[test]
fn product_registries_match_fixed_fixtures_and_serialized_names() {
    assert_registry_fixture(
        ProductOperation::ALL,
        ProductOperation::as_str,
        include_str!("fixtures/registry_operations.json"),
    );
    assert_registry_fixture(
        ProductStreamKind::ALL,
        ProductStreamKind::as_str,
        include_str!("fixtures/registry_streams.json"),
    );
    assert_registry_fixture(
        SessionEventKindDto::ALL,
        SessionEventKindDto::as_str,
        include_str!("fixtures/registry_events.json"),
    );
    assert_registry_fixture(
        ProductErrorCategory::ALL,
        ProductErrorCategory::as_str,
        include_str!("fixtures/registry_errors.json"),
    );
}

fn assert_registry_fixture<T, const N: usize>(
    values: [T; N],
    name: fn(T) -> &'static str,
    fixture: &str,
) where
    T: serde::Serialize + serde::de::DeserializeOwned + Copy + PartialEq + std::fmt::Debug,
{
    let expected: Vec<String> = serde_json::from_str(fixture).expect("registry fixture");
    assert_eq!(values.map(name).as_slice(), expected.as_slice());
    for (value, expected_name) in values.into_iter().zip(expected) {
        let serialized = serde_json::Value::String(expected_name);
        assert_eq!(
            serde_json::to_value(value).expect("serialize registry value"),
            serialized
        );
        assert_eq!(
            serde_json::from_value::<T>(serialized).expect("decode registry value"),
            value
        );
    }
    assert!(serde_json::from_str::<T>(r#""unknown_registry_value""#).is_err());
}

#[test]
fn current_contract_descriptors_preserve_request_and_response_metadata() {
    for (payload, schema) in [
        (ContractPayloadKind::Request, "undercurrent.request"),
        (ContractPayloadKind::Response, "undercurrent.response"),
    ] {
        let descriptor = ProductContractDto::current(ProductOperation::StartSession, payload);
        assert_eq!(
            serde_json::to_value(descriptor).expect("serialize current descriptor"),
            serde_json::json!({
                "capability": "start_session",
                "schema": schema,
                "schema_version": "0.1.0-beta",
                "contract_version": "0.1.0-beta",
            })
        );
    }
}

#[test]
fn product_contract_validation_accepts_only_exact_current_metadata() {
    let request: ProductRequest =
        serde_json::from_str(include_str!("fixtures/product_request.json"))
            .expect("current 0.1.0-beta fixture");
    let operation = request.operation();
    let mutation = request.mutation().expect("mutation");
    validate_mutation_contract(mutation, operation).expect("current 0.1.0-beta contract");

    let mut missing = mutation.clone();
    missing.contract = None;
    assert_eq!(
        validate_mutation_contract(&missing, operation),
        Err(ProductContractError::MissingMetadata)
    );

    let current = mutation.contract.as_ref().expect("contract");
    for version in ["0.1.0", "0.1.1-beta", "99.0.0"] {
        let mut invalid = current.clone();
        invalid.contract_version = version.to_owned();
        assert_eq!(
            validate_product_contract(&invalid, operation, ContractPayloadKind::Request),
            Err(ProductContractError::ContractVersionMismatch)
        );
    }
    for version in ["0", "2"] {
        let mut invalid = current.clone();
        invalid.schema_version = version.to_owned();
        assert_eq!(
            validate_product_contract(&invalid, operation, ContractPayloadKind::Request),
            Err(ProductContractError::SchemaVersionMismatch)
        );
    }

    let mut wrong_capability = current.clone();
    wrong_capability.capability = "unknown_operation".to_owned();
    assert_eq!(
        validate_product_contract(&wrong_capability, operation, ContractPayloadKind::Request,),
        Err(ProductContractError::CapabilityMismatch)
    );

    assert_eq!(
        validate_product_contract(current, operation, ContractPayloadKind::Response),
        Err(ProductContractError::SchemaMismatch)
    );

    let mut security_controlling = mutation.clone();
    security_controlling
        .compatible_unknown
        .insert("security.override".to_owned(), serde_json::json!(true));
    let error = validate_mutation_contract(&security_controlling, operation)
        .expect_err("security-controlling additive field");
    let ProductContractError::SecurityControllingUnknownField(field) = error else {
        panic!("unexpected contract error: {error}");
    };
    assert_eq!(field.field(), "security.override");
}

#[test]
fn unsupported_product_contract_fixtures_are_rejected_explicitly() {
    for fixture in [
        include_str!("fixtures/unsupported_product_request.json"),
        include_str!("fixtures/unsupported_execute_in_session.json"),
    ] {
        let request: ProductRequest =
            serde_json::from_str(fixture).expect("retired 0.1.0-beta fixture");
        assert_eq!(
            validate_mutation_contract(request.mutation().expect("mutation"), request.operation()),
            Err(ProductContractError::ContractVersionMismatch)
        );
    }
}

#[test]
fn repair_requires_exact_current_contract_without_becoming_an_attempt_mutation() {
    let standalone = ProductRequest::RepairOperationAttempt(
        RepairOperationAttemptRequest::new("repair-standalone".to_owned())
            .expect("standalone repair request"),
    );
    assert!(standalone.operation().is_mutating());
    assert!(
        standalone.mutation().is_none(),
        "repair must not accept peer attempt, fence, or effect metadata"
    );
    validate_product_request_contract(&standalone).expect("exact current repair contract");
    assert_eq!(
        serde_json::to_value(&standalone).expect("standalone JSON")["input"]["request_digest_sha256"],
        serde_json::Value::Null
    );

    let attached = ProductRequest::RepairOperationAttempt(
        RepairOperationAttemptRequest::attached("repair-attached".to_owned(), "7".repeat(64))
            .expect("attached repair request"),
    );
    validate_product_request_contract(&attached).expect("attached current repair contract");

    let current = serde_json::to_value(&attached).expect("current repair JSON");

    let mut missing = current.clone();
    missing["input"]
        .as_object_mut()
        .expect("repair input")
        .remove("contract");
    assert!(
        serde_json::from_value::<ProductRequest>(missing).is_err(),
        "repair contract metadata is required-present"
    );

    for (version, expected) in [
        ("99.0.0", ProductContractError::ContractVersionMismatch),
        ("99.0.0", ProductContractError::ContractVersionMismatch),
        ("4.0.0", ProductContractError::ContractVersionMismatch),
    ] {
        let mut value = current.clone();
        value["input"]["contract"]["contract_version"] = serde_json::json!(version);
        let request: ProductRequest =
            serde_json::from_value(value).expect("structurally valid versioned repair");
        assert_eq!(validate_product_request_contract(&request), Err(expected));
    }

    for (field, replacement, expected) in [
        (
            "capability",
            serde_json::json!("start_session"),
            ProductContractError::CapabilityMismatch,
        ),
        (
            "schema",
            serde_json::json!("undercurrent.response"),
            ProductContractError::SchemaMismatch,
        ),
        (
            "schema_version",
            serde_json::json!("2"),
            ProductContractError::SchemaVersionMismatch,
        ),
    ] {
        let mut value = current.clone();
        value["input"]["contract"][field] = replacement;
        let request: ProductRequest =
            serde_json::from_value(value).expect("structurally valid mismatched repair");
        assert_eq!(validate_product_request_contract(&request), Err(expected));
    }

    let mut malformed = current.clone();
    malformed["input"]["contract"]["contract_version"] = serde_json::json!(2);
    assert!(serde_json::from_value::<ProductRequest>(malformed).is_err());

    for forbidden_claim in [
        "mutation",
        "effect_certainty",
        "fencing_token",
        "result_digest_sha256",
    ] {
        let mut value = current.clone();
        value["input"]
            .as_object_mut()
            .expect("repair input")
            .insert(forbidden_claim.to_owned(), serde_json::json!("peer-claim"));
        assert!(
            serde_json::from_value::<ProductRequest>(value).is_err(),
            "repair accepted forbidden peer claim {forbidden_claim}"
        );
    }
}

#[test]
fn canonical_payload_digest_excludes_retry_attempt_metadata() {
    let original_value: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/product_request.json"))
            .expect("current 0.1.0-beta fixture");
    let mut retry_value = original_value.clone();
    retry_value["input"]["mutation"]["attempt"]["attempt_id"] = serde_json::json!("attempt-2");
    retry_value["input"]["mutation"]["attempt"]["fencing_token"] = serde_json::json!(8);
    let original: ProductRequest =
        serde_json::from_value(original_value).expect("original request");
    let retry: ProductRequest = serde_json::from_value(retry_value).expect("retry request");
    assert_eq!(
        canonical_product_request_digest(&original).expect("original digest"),
        canonical_product_request_digest(&retry).expect("retry digest")
    );
}

#[test]
fn private_environment_accepts_unicode_and_multiline_without_relaxing_credentials() {
    for value in ["/home/用户/é", "ordinary\nmultiline\ttext", "€ summary"] {
        let environment = EnvironmentSpecDto {
            variables: [("MESSAGE".to_owned(), value.to_owned())].into(),
            ..Default::default()
        };
        environment.validate_for_persistence().unwrap();
        let encoded = serde_json::to_string(&environment).unwrap();
        let decoded: EnvironmentSpecDto = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.variables["MESSAGE"], value);
        assert!(undercurrent_foundation::credential_like_text(value));
    }
    for (key, value) in [
        ("API_KEY", "ordinary"),
        ("MESSAGE", "ordinary\nBearer credential"),
        ("MESSAGE", "€ sk-example"),
        ("MESSAGE", "ordinary\npassword=credential"),
        ("MESSAGE", "nul\0byte"),
        ("MESSAGE", "-----BEGIN PRIVATE KEY-----\nprivate"),
    ] {
        let environment = EnvironmentSpecDto {
            variables: [(key.to_owned(), value.to_owned())].into(),
            ..Default::default()
        };
        assert!(environment.validate_for_persistence().is_err());
        assert!(serde_json::to_string(&environment).is_err());
    }
    let environment = EnvironmentSpecDto {
        secret_variables: [(
            "API_KEY".to_owned(),
            SecretReferenceDto::new("secret://runtime/api-key").unwrap(),
        )]
        .into(),
        ..Default::default()
    };
    environment.validate_for_persistence().unwrap();
    assert!(serde_json::to_string(&environment).is_ok());
    assert!(
        undercurrent_foundation::SensitiveMaterialDetector::new()
            .contains_sensitive_material(
                &serde_json::json!("ordinary"),
                undercurrent_foundation::Classification::Secret
            )
            .unwrap()
    );
}
