// SPDX-License-Identifier: Apache-2.0

//! Scoped native signal interruption for one active logical stream.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

#[cfg(unix)]
use signal_hook::consts::signal::{SIGINT, SIGTERM};
#[cfg(unix)]
use signal_hook::iterator::{Handle, Signals};

use crate::{CliProductSubscription, CliStreamCancellation};
#[cfg(windows)]
use undercurrent_platform_windows::{ConsoleSignalHandle as Handle, ConsoleSignals as Signals};

/// Native signal boundary operation used in payload-safe diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliSignalOperation {
    /// Register process signal listeners.
    Register,
    /// Spawn the scoped blocking signal worker.
    SpawnWorker,
}

impl CliSignalOperation {
    /// Returns the stable operation name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Register => "register",
            Self::SpawnWorker => "spawn_worker",
        }
    }
}

/// Failure installing or retiring one scoped signal worker.
#[derive(Debug)]
pub enum CliSignalError {
    /// Configured signal worker stack size was zero.
    ZeroStackSize,
    /// Signal registration or worker creation failed.
    Io {
        /// Stable failure stage.
        operation: CliSignalOperation,
        /// Concrete OS error retained outside public presentation.
        source: std::io::Error,
    },
    /// Signal worker terminated through a panic.
    WorkerPanicked,
}

impl Display for CliSignalError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroStackSize => formatter.write_str("CLI signal worker stack must be non-zero"),
            Self::Io { operation, .. } => {
                write!(formatter, "CLI signal {} failed", operation.as_str())
            }
            Self::WorkerPanicked => formatter.write_str("CLI signal worker panicked"),
        }
    }
}

impl Error for CliSignalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::ZeroStackSize | Self::WorkerPanicked => None,
        }
    }
}

/// Scoped worker that turns native termination signals into logical cancellation.
pub struct NativeStreamSignalGuard {
    handle: Handle,
    worker: Option<JoinHandle<()>>,
}

impl NativeStreamSignalGuard {
    /// Registers native interruptions for one active logical subscription:
    /// SIGINT/SIGTERM on Unix and Ctrl-C/Ctrl-Break on Windows.
    ///
    /// The first received signal marks local cancellation and wakes the
    /// subscription's blocking receiver without stopping managed runtime processes.
    /// Peer isolation follows [`CliProductSubscription::cancel`], including its
    /// close-pressure caveat. This method performs signal and thread IO and does
    /// not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed stack-size, signal-registration, or worker-spawn failure.
    pub fn install(
        subscription: Arc<dyn CliProductSubscription>,
        cancellation: CliStreamCancellation,
        worker_stack_bytes: usize,
    ) -> Result<Self, CliSignalError> {
        if worker_stack_bytes == 0 {
            return Err(CliSignalError::ZeroStackSize);
        }
        #[cfg(unix)]
        let signals = Signals::new([SIGINT, SIGTERM]);
        #[cfg(windows)]
        let signals = Signals::new();
        let mut signals = signals.map_err(|source| CliSignalError::Io {
            operation: CliSignalOperation::Register,
            source,
        })?;
        let handle = signals.handle();
        let worker = thread::Builder::new()
            .name("undercurrent-cli-signals".to_owned())
            .stack_size(worker_stack_bytes)
            .spawn(move || {
                if signals.forever().next().is_some() {
                    cancellation.cancel(subscription.as_ref());
                }
            })
            .map_err(|source| CliSignalError::Io {
                operation: CliSignalOperation::SpawnWorker,
                source,
            })?;
        Ok(Self {
            handle,
            worker: Some(worker),
        })
    }

    /// Stops and joins the scoped signal worker.
    ///
    /// Closing the iterator wakes an idle worker immediately. This method
    /// performs thread synchronization and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed failure if the signal worker panicked.
    pub fn finish(mut self) -> Result<(), CliSignalError> {
        self.close_and_join()
    }

    fn close_and_join(&mut self) -> Result<(), CliSignalError> {
        self.handle.close();
        self.worker.take().map_or(Ok(()), |worker| {
            worker.join().map_err(|_| CliSignalError::WorkerPanicked)
        })
    }
}

impl Drop for NativeStreamSignalGuard {
    fn drop(&mut self) {
        let _ = self.close_and_join();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use undercurrent_sdk::{ProductClientError, ProductSubscriptionItem};

    use super::*;

    struct IdleSubscription;

    impl CliProductSubscription for IdleSubscription {
        fn recv(&self) -> Result<ProductSubscriptionItem, ProductClientError> {
            unreachable!("signal guard test does not consume")
        }

        fn recv_timeout(
            &self,
            _timeout: Duration,
        ) -> Result<ProductSubscriptionItem, ProductClientError> {
            unreachable!("signal guard test does not consume")
        }

        fn cancel(&self) {}

        fn close(self) {}
    }

    #[test]
    fn zero_stack_fails_before_signal_registration() {
        let error = NativeStreamSignalGuard::install(
            Arc::new(IdleSubscription),
            CliStreamCancellation::default(),
            0,
        )
        .err()
        .expect("zero stack");

        assert!(matches!(error, CliSignalError::ZeroStackSize));
    }

    #[test]
    fn finishing_an_idle_guard_closes_and_joins_its_worker() {
        NativeStreamSignalGuard::install(
            Arc::new(IdleSubscription),
            CliStreamCancellation::default(),
            64 * 1024,
        )
        .expect("signal guard")
        .finish()
        .expect("signal worker join");
    }

    #[test]
    fn signal_error_display_never_exposes_os_source_text() {
        let error = CliSignalError::Io {
            operation: CliSignalOperation::Register,
            source: std::io::Error::other("/private/path"),
        };

        assert!(!error.to_string().contains("/private/path"));
    }
}
