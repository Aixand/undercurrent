// SPDX-License-Identifier: Apache-2.0

const SOURCES: &[(&str, &str)] = &[
    ("boundary.rs", include_str!("../src/boundary.rs")),
    ("client.rs", include_str!("../src/client.rs")),
    ("error.rs", include_str!("../src/error.rs")),
    ("lib.rs", include_str!("../src/lib.rs")),
    ("product.rs", include_str!("../src/product.rs")),
    ("product/error.rs", include_str!("../src/product/error.rs")),
    (
        "product/subscription.rs",
        include_str!("../src/product/subscription.rs"),
    ),
    ("profile.rs", include_str!("../src/profile.rs")),
    ("reconnect.rs", include_str!("../src/reconnect.rs")),
    ("registry.rs", include_str!("../src/registry.rs")),
    ("resilient.rs", include_str!("../src/resilient.rs")),
    ("telemetry.rs", include_str!("../src/telemetry.rs")),
    ("trace.rs", include_str!("../src/trace.rs")),
    ("transport.rs", include_str!("../src/transport.rs")),
    ("local.rs", include_str!("../src/local.rs")),
];

const FORBIDDEN_CRATES: [&str; 2] = ["undercurrent_wire", "undercurrent_multiplex"];
const FORBIDDEN_TYPES: &[&str] = &[
    "ClientId",
    "ClientMessage",
    "ConnectionAgreement",
    "ConnectionHello",
    "ConnectionLimits",
    "ConnectionRouter",
    "DecodeStatus",
    "HandshakeCodec",
    "HandshakeDecodeStatus",
    "HandshakeResponse",
    "HeartbeatPolicy",
    "InboundStream",
    "InboundStreamError",
    "InboundStreamItem",
    "MultiplexError",
    "NamespacedName",
    "PendingResponseError",
    "ProtocolVersion",
    "RequestId",
    "ServerEpoch",
    "ServerMessage",
    "StreamGap",
    "StreamGapReason",
    "StreamId",
    "StreamRecoveryAction",
    "StreamSequence",
    "TraceContext",
    "VersionRange",
    "WireCodec",
];
const CURRENT_API_BASELINE: &str = include_str!("../PUBLIC_API.md");

#[test]
fn public_declarations_do_not_expose_private_wire_or_router_types() {
    let mut violations = Vec::new();
    for (name, source) in SOURCES {
        scan_public_declarations(name, source, &mut violations);
    }
    assert!(
        violations.is_empty(),
        "private protocol types escaped the SDK boundary:\n{}",
        violations.join("\n")
    );
}

#[test]
fn boundary_scanner_detects_unqualified_private_types() {
    let mut violations = Vec::new();
    scan_public_declarations(
        "fixture.rs",
        "pub fn leak(value: ConnectionAgreement) -> ClientMessage { todo!() }",
        &mut violations,
    );
    assert_eq!(violations.len(), 2);
}

#[test]
fn current_boundary_documents_the_connection_contract_and_retains_opaque_types() {
    let normalized = CURRENT_API_BASELINE
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    assert!(normalized.contains("local IPC public SDK boundary"));
    assert!(normalized.contains("private wire or multiplex types"));
    assert!(normalized.contains("LocalClientConnector"));
    assert!(normalized.contains("0.1.0-beta"));
}

fn scan_public_declarations(name: &str, source: &str, violations: &mut Vec<String>) {
    let lines = source.lines().collect::<Vec<_>>();
    let mut index = 0;
    while index < lines.len() {
        let trimmed = lines[index].trim_start();
        if trimmed.starts_with("pub use ") || trimmed.starts_with("pub type ") {
            let (declaration, end) = declaration_through(&lines, index, ';');
            reject(name, index + 1, &declaration, violations);
            index = end + 1;
            continue;
        }
        if is_public_function(trimmed) {
            let (declaration, end) = signature_through_body(&lines, index);
            reject(name, index + 1, &declaration, violations);
            index = end + 1;
            continue;
        }
        if trimmed.starts_with("pub enum ") || trimmed.starts_with("pub trait ") {
            let (body, end) = braced_item(&lines, index);
            reject(name, index + 1, &body, violations);
            index = end + 1;
            continue;
        }
        if trimmed.starts_with("pub struct ") && trimmed.contains('{') {
            let (body, end) = braced_item(&lines, index);
            for (offset, line) in body.lines().enumerate() {
                if line.trim_start().starts_with("pub ") {
                    reject(name, index + offset + 1, line, violations);
                }
            }
            index = end + 1;
            continue;
        }
        index += 1;
    }
}

fn is_public_function(line: &str) -> bool {
    line.starts_with("pub fn ")
        || line.starts_with("pub const fn ")
        || line.starts_with("pub async fn ")
        || line.starts_with("pub unsafe fn ")
}

fn declaration_through(lines: &[&str], start: usize, terminator: char) -> (String, usize) {
    let mut declaration = String::new();
    for (index, line) in lines.iter().enumerate().skip(start) {
        declaration.push_str(line);
        declaration.push('\n');
        if line.contains(terminator) {
            return (declaration, index);
        }
    }
    (declaration, lines.len().saturating_sub(1))
}

fn signature_through_body(lines: &[&str], start: usize) -> (String, usize) {
    let mut declaration = String::new();
    for (index, line) in lines.iter().enumerate().skip(start) {
        let ending = line.find(['{', ';']);
        declaration.push_str(ending.map_or(*line, |position| &line[..position]));
        declaration.push('\n');
        if ending.is_some() {
            return (declaration, index);
        }
    }
    (declaration, lines.len().saturating_sub(1))
}

fn braced_item(lines: &[&str], start: usize) -> (String, usize) {
    let mut item = String::new();
    let mut depth = 0_i64;
    let mut opened = false;
    for (index, line) in lines.iter().enumerate().skip(start) {
        item.push_str(line);
        item.push('\n');
        for character in line.chars() {
            match character {
                '{' => {
                    depth += 1;
                    opened = true;
                }
                '}' => depth -= 1,
                _ => {}
            }
        }
        if opened && depth == 0 {
            return (item, index);
        }
    }
    (item, lines.len().saturating_sub(1))
}

fn reject(name: &str, line: usize, declaration: &str, violations: &mut Vec<String>) {
    let declaration = declaration
        .replace(
            "TraceContext(TraceContextGenerationError)",
            "GeneratedTraceContextError",
        )
        .replace(
            "TraceContext(ClientGeneratedIdentifierError)",
            "GeneratedTraceContextError",
        );
    for forbidden in FORBIDDEN_CRATES {
        if declaration.contains(forbidden) {
            violations.push(format!("{name}:{line}: contains {forbidden}"));
        }
    }
    for forbidden in FORBIDDEN_TYPES {
        if declaration
            .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
            .any(|identifier| identifier == *forbidden)
        {
            violations.push(format!("{name}:{line}: exposes {forbidden}"));
        }
    }
}
