// SPDX-License-Identifier: Apache-2.0

//! External-daemon fixture for the Undercurrent live integration gate.
//!
//! This executable is built and launched only by the local integration script.
//! It composes the production daemon with a narrow mock of the semantic authorization
//! decision port so cancellation can be exercised before a live authorization service is
//! available. Ordinary `uc-daemon` composition remains fail closed.

use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[cfg(unix)]
use signal_hook::{
    consts::signal::{SIGINT, SIGTERM},
    iterator::Signals,
};
use undercurrent_application::{
    EphemeralSecret, PortFuture, RuntimeCancellationActor, RuntimeCancellationAuthorization,
    RuntimeCancellationAuthorizer, RuntimeCancellationMode, SecretResolver,
    UnavailableSecretResolver,
};
use undercurrent_daemon::{
    ConfigDiscovery, DaemonConfigOverrides, DaemonProcess, load_daemon_config,
    prepare_instance_scope_parent, prepare_private_directory,
};
use undercurrent_domain::{OperationAttemptId, SecretReference};
#[cfg(windows)]
use undercurrent_platform_windows::ConsoleSignals as Signals;

const TEST_TENANT: &str = "tenant_0198a5bd-7607-7ae9-9a0c-42532d56e7f7";
const TEST_ACTOR: &str = "trust://actors/live-runtime-operator";
const TEST_AUTHORITY_PREFIX: &str = "trust://decisions/live-runtime-";
const TEST_CODEX_CREDENTIAL_HANDLE: &str = "secret://example/codex-home";

fn main() -> Result<(), Box<dyn Error>> {
    let config_path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("expected one explicit config path")?;
    if std::env::args_os().nth(2).is_some() {
        return Err("expected exactly one explicit config path".into());
    }
    let discovery = ConfigDiscovery::from_process_env(Some(config_path));
    let config = load_daemon_config(&discovery, DaemonConfigOverrides::new())?;
    prepare_private_directory(config.paths().product_home())?;
    prepare_instance_scope_parent(config.paths().instance_lock())?;

    let process = DaemonProcess::compose_with_external_adapters(
        config,
        Arc::new(IntegrationSecretResolver),
        Arc::new(IntegrationAuthorizer),
    )?;
    let control = process.control();
    #[cfg(unix)]
    let mut signals = Signals::new([SIGINT, SIGTERM])?;
    #[cfg(windows)]
    let mut signals = Signals::new()?;
    let signal_handle = signals.handle();
    let signal_worker = thread::spawn(move || {
        if signals.forever().next().is_some() {
            let _ = control.begin_shutdown(Duration::from_secs(2));
        }
    });
    process.run()?;
    signal_handle.close();
    signal_worker
        .join()
        .map_err(|_| "integration daemon signal worker panicked")?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct IntegrationSecretResolver;

impl SecretResolver for IntegrationSecretResolver {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> PortFuture<'a, Result<EphemeralSecret, undercurrent_application::PortError>> {
        if reference.as_str() == TEST_CODEX_CREDENTIAL_HANDLE {
            if std::env::var("UNDERCURRENT_LIVE_CODEX_QUALIFICATION_MODE").as_deref()
                == Ok("live-codex")
            {
                let Ok(codex_home) = std::env::var("CODEX_HOME") else {
                    return UnavailableSecretResolver.resolve(reference);
                };
                if !Path::new(&codex_home).is_absolute()
                    || !Path::new(&codex_home).is_dir()
                    || codex_home.len() > 4_096
                    || codex_home.chars().any(char::is_control)
                {
                    return UnavailableSecretResolver.resolve(reference);
                }
                return Box::pin(async move { Ok(EphemeralSecret::new(codex_home.into_bytes())) });
            }
            Box::pin(async {
                Ok(EphemeralSecret::new(
                    b"deterministic-runner-does-not-consume-this-value".to_vec(),
                ))
            })
        } else {
            UnavailableSecretResolver.resolve(reference)
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct IntegrationAuthorizer;

impl RuntimeCancellationAuthorizer for IntegrationAuthorizer {
    fn authorize_cancel<'a>(
        &'a self,
        actor: &'a RuntimeCancellationActor,
        _control_id: &'a OperationAttemptId,
        _target_attempt_id: &'a OperationAttemptId,
        mode: RuntimeCancellationMode,
    ) -> PortFuture<'a, Result<RuntimeCancellationAuthorization, undercurrent_application::PortError>>
    {
        let authorized = exact_cancel_scope(actor, mode);
        Box::pin(async move {
            Ok(if authorized {
                RuntimeCancellationAuthorization::Authorized
            } else {
                RuntimeCancellationAuthorization::Denied
            })
        })
    }

    fn authorize_status<'a>(
        &'a self,
        actor: &'a RuntimeCancellationActor,
        _control_id: &'a OperationAttemptId,
        _target_attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<RuntimeCancellationAuthorization, undercurrent_application::PortError>>
    {
        let authorized = exact_mock_scope(actor);
        Box::pin(async move {
            Ok(if authorized {
                RuntimeCancellationAuthorization::Authorized
            } else {
                RuntimeCancellationAuthorization::Denied
            })
        })
    }
}

fn exact_mock_scope(actor: &RuntimeCancellationActor) -> bool {
    actor.source_product() == "example/workflow"
        && actor.tenant_id() == TEST_TENANT
        && actor.actor_context_ref() == TEST_ACTOR
        && actor.authority_ref().contains(TEST_AUTHORITY_PREFIX)
}

fn exact_cancel_scope(actor: &RuntimeCancellationActor, mode: RuntimeCancellationMode) -> bool {
    exact_mock_scope(actor) && mode == RuntimeCancellationMode::Graceful
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor(source_product: &str, authority_ref: &str) -> RuntimeCancellationActor {
        RuntimeCancellationActor::new(
            "loopback-test-principal".to_owned(),
            None,
            "operation-integration-mock".to_owned(),
            source_product.to_owned(),
            TEST_TENANT.to_owned(),
            TEST_ACTOR.to_owned(),
            authority_ref.to_owned(),
        )
        .expect("test actor")
    }

    #[test]
    fn mock_trust_scope_is_exact_and_does_not_authorize_force() {
        let exact = actor(
            "example/workflow",
            r#"{"decision_ref":"trust://decisions/live-runtime-cancel"}"#,
        );
        assert!(exact_mock_scope(&exact));
        assert!(!exact_mock_scope(&actor(
            "example/other-client",
            r#"{"decision_ref":"trust://decisions/live-runtime-cancel"}"#,
        )));

        assert!(exact_cancel_scope(
            &exact,
            RuntimeCancellationMode::Graceful
        ));
        assert!(!exact_cancel_scope(&exact, RuntimeCancellationMode::Force));
    }
}
