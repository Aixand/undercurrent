// SPDX-License-Identifier: Apache-2.0

//! Thread-parking bridge for synchronous daemon composition and worker paths.
//!
//! This helper drives application-port futures on its caller; it creates
//! neither an executor thread nor an async IO/timer runtime. Port implementations
//! must arrange to wake the supplied waker when progress becomes possible.

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

struct ThreadWake(Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// Runs one application future to completion on the calling daemon thread.
///
/// The waker parks the worker instead of polling or spawning another executor
/// thread. Futures supplied by application ports remain responsible for waking
/// after external completion. This function performs no IO itself and does not
/// panic unless the future panics while polled.
pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

// Reset may abandon a suspended mutation because the entire runtime is being
// retired. Its durable attempt remains available if reset subsequently fails;
// cancellation does not assert that the mutation had no external effect.
pub(crate) fn block_on_until<F: Future>(
    future: F,
    cancelled: impl Fn() -> bool,
) -> Option<F::Output> {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if cancelled() {
            return None;
        }
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return Some(output),
            Poll::Pending => thread::park_timeout(std::time::Duration::from_millis(5)),
        }
    }
}
