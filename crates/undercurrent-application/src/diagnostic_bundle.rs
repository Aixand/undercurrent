// SPDX-License-Identifier: Apache-2.0

//! Explicit authorization for safe diagnostic-bundle detail.

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_domain::SessionId;

use crate::{
    AccessAction, AccessDecision, AccessDenialReason, AccessResource, ApplicationErrorCategory,
    AuthorizationPort, PortError,
};

/// Requested diagnostic-bundle detail.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DiagnosticBundleDetail {
    /// Payload-free operational diagnostics only.
    #[default]
    Safe,
    /// Include explicitly supplied, bounded terminal content.
    TerminalContent,
}

/// Application request for one diagnostic bundle.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiagnosticBundleRequest {
    terminal_session: Option<SessionId>,
}

impl DiagnosticBundleRequest {
    /// Constructs the safe payload-free default.
    pub const fn safe() -> Self {
        Self {
            terminal_session: None,
        }
    }

    /// Explicitly requests terminal content in addition to safe diagnostics.
    #[must_use]
    pub fn explicitly_include_terminal_content(mut self, session_id: SessionId) -> Self {
        self.terminal_session = Some(session_id);
        self
    }

    /// Returns the requested detail without exposing any diagnostic payload.
    pub const fn detail(&self) -> DiagnosticBundleDetail {
        if self.terminal_session.is_some() {
            DiagnosticBundleDetail::TerminalContent
        } else {
            DiagnosticBundleDetail::Safe
        }
    }

    /// Returns the explicit terminal session, absent for the safe default.
    pub const fn terminal_session(&self) -> Option<&SessionId> {
        self.terminal_session.as_ref()
    }
}

/// Authorization proof consumed by the daemon diagnostic-bundle adapter.
///
/// The private field prevents adapters from manufacturing sensitive-detail
/// permission without first invoking [`AuthorizeDiagnosticBundleService`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedDiagnosticBundleRequest {
    terminal_session: Option<SessionId>,
}

impl AuthorizedDiagnosticBundleRequest {
    /// Returns whether the caller explicitly requested and was authorized to
    /// include terminal content.
    pub const fn includes_terminal_content(&self) -> bool {
        self.terminal_session.is_some()
    }

    /// Returns the separately authorized terminal session.
    pub const fn terminal_session(&self) -> Option<&SessionId> {
        self.terminal_session.as_ref()
    }
}

/// Safe authorization failure for diagnostic-bundle creation.
#[derive(Debug)]
pub enum DiagnosticBundleAuthorizationError {
    /// The configured policy denied one required action.
    Denied {
        /// Action that was denied.
        action: AccessAction,
        /// Stable denial reason.
        reason: AccessDenialReason,
    },
    /// The authorization adapter could not make a decision.
    Authorization {
        /// Action whose authorization failed.
        action: AccessAction,
        /// Typed adapter source retained without private bundle content.
        source: PortError,
    },
}

impl DiagnosticBundleAuthorizationError {
    /// Returns the action that did not receive permission.
    pub const fn action(&self) -> AccessAction {
        match self {
            Self::Denied { action, .. } | Self::Authorization { action, .. } => *action,
        }
    }

    /// Returns the stable application category without exposing policy detail.
    pub fn category(&self) -> ApplicationErrorCategory {
        match self {
            Self::Denied {
                reason: AccessDenialReason::Unauthenticated,
                ..
            } => ApplicationErrorCategory::Unauthenticated,
            Self::Denied {
                reason: AccessDenialReason::Forbidden,
                ..
            } => ApplicationErrorCategory::PermissionDenied,
            Self::Authorization { source, .. } => source.category(),
        }
    }
}

impl Display for DiagnosticBundleAuthorizationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Denied { action, .. } => {
                write!(
                    formatter,
                    "diagnostic bundle action {} was denied",
                    action.as_str()
                )
            }
            Self::Authorization { action, .. } => write!(
                formatter,
                "diagnostic bundle action {} could not be authorized",
                action.as_str()
            ),
        }
    }
}

impl Error for DiagnosticBundleAuthorizationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Authorization { source, .. } => Some(source),
            Self::Denied { .. } => None,
        }
    }
}

/// Authorizes safe diagnostics and sensitive detail as independent actions.
pub struct AuthorizeDiagnosticBundleService<'a> {
    authorizer: &'a dyn AuthorizationPort,
}

impl<'a> AuthorizeDiagnosticBundleService<'a> {
    /// Constructs the application service over its authorization port.
    pub const fn new(authorizer: &'a dyn AuthorizationPort) -> Self {
        Self { authorizer }
    }

    /// Returns an unforgeable adapter proof after every required decision permits.
    pub async fn authorize(
        &self,
        request: DiagnosticBundleRequest,
    ) -> Result<AuthorizedDiagnosticBundleRequest, DiagnosticBundleAuthorizationError> {
        self.require(AccessAction::ReadDiagnostics).await?;
        if let Some(session_id) = request.terminal_session() {
            self.require(AccessAction::ExportDiagnosticContent).await?;
            self.require_resource(
                AccessAction::SubscribeTerminal,
                &AccessResource::Session(session_id.clone()),
            )
            .await?;
        }
        Ok(AuthorizedDiagnosticBundleRequest {
            terminal_session: request.terminal_session,
        })
    }

    async fn require(
        &self,
        action: AccessAction,
    ) -> Result<(), DiagnosticBundleAuthorizationError> {
        self.require_resource(action, &AccessResource::Diagnostics)
            .await
    }

    async fn require_resource(
        &self,
        action: AccessAction,
        resource: &AccessResource,
    ) -> Result<(), DiagnosticBundleAuthorizationError> {
        match self.authorizer.authorize(action, resource).await {
            Ok(AccessDecision::Permit) => Ok(()),
            Ok(AccessDecision::Deny(reason)) => {
                Err(DiagnosticBundleAuthorizationError::Denied { action, reason })
            }
            Err(source) => {
                Err(DiagnosticBundleAuthorizationError::Authorization { action, source })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::{Context, Poll, Wake, Waker};

    use crate::PortFuture;

    use super::*;

    struct RecordingAuthorizer {
        deny: Option<AccessAction>,
        actions: Mutex<Vec<AccessAction>>,
        resources: Mutex<Vec<AccessResource>>,
    }

    impl AuthorizationPort for RecordingAuthorizer {
        fn authorize<'a>(
            &'a self,
            action: AccessAction,
            resource: &'a AccessResource,
        ) -> PortFuture<'a, Result<AccessDecision, PortError>> {
            self.actions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(action);
            self.resources
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(resource.clone());
            let denied = self.deny == Some(action);
            Box::pin(async move {
                if denied {
                    Ok(AccessDecision::Deny(AccessDenialReason::Forbidden))
                } else {
                    Ok(AccessDecision::Permit)
                }
            })
        }
    }

    #[test]
    fn safe_default_never_requests_sensitive_detail_permission() {
        let authorizer = RecordingAuthorizer {
            deny: Some(AccessAction::ExportDiagnosticContent),
            actions: Mutex::new(Vec::new()),
            resources: Mutex::new(Vec::new()),
        };
        let authorized = block_on(
            AuthorizeDiagnosticBundleService::new(&authorizer)
                .authorize(DiagnosticBundleRequest::default()),
        )
        .expect("safe diagnostics are authorized");

        assert!(!authorized.includes_terminal_content());
        assert_eq!(
            *authorizer
                .actions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            [AccessAction::ReadDiagnostics]
        );
        assert_eq!(
            *authorizer
                .resources
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            [AccessResource::Diagnostics]
        );
    }

    #[test]
    fn terminal_content_requires_a_distinct_explicit_authorization_action() {
        let authorizer = RecordingAuthorizer {
            deny: Some(AccessAction::SubscribeTerminal),
            actions: Mutex::new(Vec::new()),
            resources: Mutex::new(Vec::new()),
        };
        let error = block_on(
            AuthorizeDiagnosticBundleService::new(&authorizer).authorize(
                DiagnosticBundleRequest::safe().explicitly_include_terminal_content(
                    SessionId::new("diagnostic-session").expect("session ID"),
                ),
            ),
        )
        .expect_err("sensitive detail denial must fail closed");

        assert_eq!(error.action(), AccessAction::SubscribeTerminal);
        assert_eq!(
            *authorizer
                .actions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            [
                AccessAction::ReadDiagnostics,
                AccessAction::ExportDiagnosticContent,
                AccessAction::SubscribeTerminal,
            ]
        );
        assert_eq!(
            *authorizer
                .resources
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            [
                AccessResource::Diagnostics,
                AccessResource::Diagnostics,
                AccessResource::Session(SessionId::new("diagnostic-session").expect("session ID")),
            ]
        );
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        struct NoopWake;
        impl Wake for NoopWake {
            fn wake(self: Arc<Self>) {}
        }

        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }
}
