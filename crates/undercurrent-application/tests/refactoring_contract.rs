// SPDX-License-Identifier: Apache-2.0

use undercurrent_application::*;

#[test]
fn attempt_and_secret_ports_remain_object_safe() {
    fn accepts_attempt_store(_: &dyn OperationAttemptStore) {}
    fn accepts_secret_resolver(_: &dyn SecretResolver) {}
    fn accepts_command_executor(_: &dyn SessionCommandExecutor) {}
    let _ = accepts_attempt_store;
    let _ = accepts_secret_resolver;
    let _ = accepts_command_executor;
}

#[test]
fn ephemeral_secrets_are_excluded_from_debug_output() {
    let secret = EphemeralSecret::new(b"private-token-value".to_vec());
    assert_eq!(secret.expose(|bytes| bytes.len()), 19);
    let debug = format!("{secret:?}");
    assert!(debug.contains("redacted"));
    assert!(!debug.contains("private-token-value"));
}
