// SPDX-License-Identifier: Apache-2.0

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

use undercurrent_application::{
    EphemeralSecret, PortError, PortErrorKind, PortFuture, PortOperation, SecretResolver,
    resolve_secret_environment,
};
use undercurrent_domain::{
    EnvironmentVariables, ExecutionLimits, SecretEnvironmentVariables, SecretReference, SessionId,
};

#[test]
fn resolver_material_is_bounded_and_excluded_from_diagnostics() {
    const CANARY: &str = "application-secret-canary-02c4";
    let references = references("opaque-handle");
    let session = SessionId::new("secret-resolution").expect("session ID");
    let resolved = block_on(resolve_secret_environment(
        &StaticResolver(CANARY.as_bytes().to_vec()),
        &references,
        &session,
        2,
        128,
    ))
    .expect("secret resolves");

    assert_eq!(resolved.len(), 1);
    assert_eq!(
        resolved
            .get("SERVICE_CREDENTIAL")
            .expect("resolved variable")
            .expose(|bytes| bytes.to_vec()),
        CANARY.as_bytes()
    );
    assert!(!format!("{resolved:?}").contains(CANARY));

    let nul_error = block_on(resolve_secret_environment(
        &StaticResolver(b"unsafe\0secret".to_vec()),
        &references,
        &session,
        2,
        128,
    ))
    .expect_err("NUL-bearing material fails closed");
    assert_eq!(nul_error.kind(), PortErrorKind::InvalidData);
    assert!(!format!("{nul_error:?}").contains("unsafe"));

    let capacity_error = block_on(resolve_secret_environment(
        &StaticResolver(vec![b'x'; 129]),
        &references,
        &session,
        2,
        128,
    ))
    .expect_err("oversized material fails closed");
    assert_eq!(capacity_error.kind(), PortErrorKind::CapacityExceeded);

    let adapter_error = block_on(resolve_secret_environment(
        &LeakyErrorResolver,
        &references,
        &session,
        2,
        128,
    ))
    .expect_err("adapter failure remains closed");
    let mut current: Option<&(dyn Error + 'static)> = Some(&adapter_error);
    while let Some(error) = current {
        assert!(!error.to_string().contains(CANARY));
        current = error.source();
    }
}

struct StaticResolver(Vec<u8>);

impl SecretResolver for StaticResolver {
    fn resolve<'a>(
        &'a self,
        _reference: &'a SecretReference,
    ) -> PortFuture<'a, Result<EphemeralSecret, PortError>> {
        let bytes = self.0.clone();
        Box::pin(async move { Ok(EphemeralSecret::new(bytes)) })
    }
}

struct LeakyErrorResolver;

impl SecretResolver for LeakyErrorResolver {
    fn resolve<'a>(
        &'a self,
        _reference: &'a SecretReference,
    ) -> PortFuture<'a, Result<EphemeralSecret, PortError>> {
        Box::pin(async {
            Err(PortError::new(
                PortErrorKind::Unavailable,
                PortOperation::ResolveSecretEnvironment,
                None,
                LeakyAdapterError,
            ))
        })
    }
}

#[derive(Debug)]
struct LeakyAdapterError;

impl Display for LeakyAdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("application-secret-canary-02c4")
    }
}

impl Error for LeakyAdapterError {}

fn references(reference: &str) -> SecretEnvironmentVariables {
    let limits = ExecutionLimits {
        max_arguments: 1,
        max_command_bytes: 1,
        max_environment_entries: 2,
        max_environment_bytes: 1_024,
        max_path_bytes: 1,
        max_health_rules: 1,
        max_health_rule_bytes: 1,
    };
    let ordinary = EnvironmentVariables::new(Vec::new(), limits).expect("environment");
    SecretEnvironmentVariables::new(
        [(
            "SERVICE_CREDENTIAL".to_owned(),
            SecretReference::new(reference).expect("opaque reference"),
        )],
        &ordinary,
        limits,
    )
    .expect("secret environment")
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWake(thread::Thread);

    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let mut future = Box::pin(future);
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::park(),
        }
    }
}
