// SPDX-License-Identifier: Apache-2.0

//! Cheap cooperative cancellation for accepted product work.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use undercurrent_application::{OperationCancellation, PortFuture};

/// Clonable cancellation state scoped to one connection-epoch request.
#[derive(Debug, Clone, Default)]
pub struct ProductCancellation {
    cancelled: Arc<AtomicBool>,
}

impl ProductCancellation {
    /// Constructs a non-cancelled token without performing IO.
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests cooperative cancellation idempotently.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Returns whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

impl OperationCancellation for ProductCancellation {
    fn is_cancelled(&self) -> bool {
        ProductCancellation::is_cancelled(self)
    }

    fn cancelled(&self) -> PortFuture<'_, ()> {
        Box::pin(async move {
            while !ProductCancellation::is_cancelled(self) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    }
}
