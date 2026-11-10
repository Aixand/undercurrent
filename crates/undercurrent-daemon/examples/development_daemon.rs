// SPDX-License-Identifier: Apache-2.0

//! External Undercurrent daemon with an explicitly selected development secret resolver.
//!
//! The production `uc-daemon` remains fail closed for secret resolution. This
//! process adds only an exact local Codex credential-handle adapter. Transport
//! authentication and semantic cancellation authorization retain their ordinary
//! composition; selecting development credentials grants no cancellation authority.

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
    EphemeralSecret, PortFuture, SecretResolver, UnavailableSecretResolver,
};
use undercurrent_daemon::{
    ConfigDiscovery, DaemonConfigOverrides, DaemonProcess, load_daemon_config,
    prepare_instance_scope_parent, prepare_private_directory,
};
use undercurrent_domain::SecretReference;
#[cfg(windows)]
use undercurrent_platform_windows::ConsoleSignals as Signals;

const DEVELOPMENT_CODEX_HOME_ENVIRONMENT: &str = "UNDERCURRENT_DEVELOPMENT_CODEX_HOME";
const DEVELOPMENT_CODEX_CREDENTIAL_HANDLE: &str = "secret://example/codex-home";
const MAXIMUM_CODEX_HOME_BYTES: usize = 4_096;

fn main() -> Result<(), Box<dyn Error>> {
    let config_path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("expected one explicit config path")?;
    if std::env::args_os().nth(2).is_some() {
        return Err("expected exactly one explicit config path".into());
    }
    let secret_resolver = DevelopmentSecretResolver::from_process_environment()?;
    let discovery = ConfigDiscovery::from_process_env(Some(config_path));
    let config = load_daemon_config(&discovery, DaemonConfigOverrides::new())?;
    prepare_private_directory(config.paths().product_home())?;
    prepare_instance_scope_parent(config.paths().instance_lock())?;

    let process = DaemonProcess::compose_with_secret_resolver(config, Arc::new(secret_resolver))?;
    let control = process.control();
    #[cfg(unix)]
    let mut signals = Signals::new([SIGINT, SIGTERM])?;
    #[cfg(windows)]
    let mut signals = Signals::new()?;
    let signal_handle = signals.handle();
    let signal_worker = thread::spawn(move || {
        if signals.forever().next().is_some() {
            let _ = control.begin_shutdown(Duration::from_secs(5));
        }
    });
    process.run()?;
    signal_handle.close();
    signal_worker
        .join()
        .map_err(|_| "development daemon signal worker panicked")?;
    Ok(())
}

#[derive(Debug, Clone)]
struct DevelopmentSecretResolver {
    codex_home: Vec<u8>,
}

impl DevelopmentSecretResolver {
    fn from_process_environment() -> Result<Self, Box<dyn Error>> {
        let codex_home = std::env::var(DEVELOPMENT_CODEX_HOME_ENVIRONMENT)
            .map_err(|_| "development Codex home is not configured")?;
        Self::new(codex_home)
    }

    fn new(codex_home: String) -> Result<Self, Box<dyn Error>> {
        if !valid_codex_home(&codex_home) {
            return Err("development Codex home is invalid".into());
        }
        Ok(Self {
            codex_home: codex_home.into_bytes(),
        })
    }
}

impl SecretResolver for DevelopmentSecretResolver {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> PortFuture<'a, Result<EphemeralSecret, undercurrent_application::PortError>> {
        if reference.as_str() != DEVELOPMENT_CODEX_CREDENTIAL_HANDLE {
            return UnavailableSecretResolver.resolve(reference);
        }
        let codex_home = self.codex_home.clone();
        Box::pin(async move { Ok(EphemeralSecret::new(codex_home)) })
    }
}

fn valid_codex_home(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAXIMUM_CODEX_HOME_BYTES
        && !value.chars().any(char::is_control)
        && Path::new(value).is_absolute()
        && Path::new(value).is_dir()
}

#[cfg(test)]
mod tests {
    use super::*;
    use undercurrent_application::{
        PortErrorKind, RuntimeCancellationActor, RuntimeCancellationAuthorization,
        RuntimeCancellationAuthorizer, RuntimeCancellationMode,
    };
    use undercurrent_daemon::AuthenticatedTransportAuthorization;
    use undercurrent_domain::OperationAttemptId;

    #[tokio::test(flavor = "current_thread")]
    async fn exact_operation_codex_handle_resolves_only_the_validated_home() {
        let temporary = tempfile::tempdir().expect("temporary Codex home");
        let codex_home = temporary.path().to_string_lossy().into_owned();
        let resolver =
            DevelopmentSecretResolver::new(codex_home.clone()).expect("development resolver");
        let exact =
            SecretReference::new(DEVELOPMENT_CODEX_CREDENTIAL_HANDLE).expect("exact handle");
        let resolved = resolver.resolve(&exact).await.expect("resolved home");
        assert_eq!(
            resolved.expose(|value| value.to_vec()),
            codex_home.into_bytes()
        );

        let other = SecretReference::new("trust://passport/credential-handles/other")
            .expect("other handle");
        let failure = resolver.resolve(&other).await.expect_err("fail closed");
        assert_eq!(failure.kind(), PortErrorKind::Unsupported);
    }

    #[test]
    fn development_codex_home_must_be_an_existing_bounded_absolute_directory() {
        assert!(DevelopmentSecretResolver::new(String::new()).is_err());
        assert!(DevelopmentSecretResolver::new("relative/path".into()).is_err());
        assert!(
            DevelopmentSecretResolver::new("/definitely/not/an/undercurrent/codex-home".into())
                .is_err()
        );
        assert!(DevelopmentSecretResolver::new(format!("/{}", "x".repeat(4_096))).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn development_daemon_requires_semantic_authority_for_cancellation_and_status() {
        let authorizer = AuthenticatedTransportAuthorization::new();
        let actor = RuntimeCancellationActor::new(
            "example-development-client".to_owned(),
            Some("tenant-development".to_owned()),
            "undercurrent-development-loopback".to_owned(),
            "example/workflow".to_owned(),
            "tenant-development".to_owned(),
            "example-dev://actors/client".to_owned(),
            "example-dev://authority/client".to_owned(),
        )
        .expect("bounded development actor");
        let control = OperationAttemptId::new("control-development").expect("control ID");
        let target = OperationAttemptId::new("attempt-development").expect("attempt ID");

        for mode in [
            RuntimeCancellationMode::Graceful,
            RuntimeCancellationMode::Force,
        ] {
            assert_eq!(
                authorizer
                    .authorize_cancel(&actor, &control, &target, mode)
                    .await
                    .expect("development authorization cannot fail"),
                RuntimeCancellationAuthorization::Denied
            );
        }
        assert_eq!(
            authorizer
                .authorize_status(&actor, &control, &target)
                .await
                .expect("development authorization cannot fail"),
            RuntimeCancellationAuthorization::Denied
        );
    }
}
