// SPDX-License-Identifier: Apache-2.0

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::mpsc as native_channel;
use std::thread::JoinHandle;

use tokio::sync::{OwnedSemaphorePermit, oneshot};

use super::*;
use crate::ActiveConnectionLease;

/// Retires connection resources independently of the caller's Tokio runtime.
pub(super) struct CleanupSupervisor {
    sender: native_channel::Sender<CleanupJob>,
    worker: JoinHandle<()>,
    finished: oneshot::Receiver<()>,
}

impl CleanupSupervisor {
    pub(super) fn start() -> Result<Self, LocalServerError> {
        let (sender, receiver) = native_channel::channel::<CleanupJob>();
        let (finished, completion) = oneshot::channel();
        let worker = thread::Builder::new()
            .name("undercurrent-local-cleanup".to_owned())
            .spawn(move || {
                while let Ok(mut job) = receiver.recv() {
                    let completion = job.completion.take();
                    // A product callback must not destroy the cleanup owner for
                    // other connections. Individual close steps also isolate
                    // callbacks so the writer join still runs after a panic.
                    let result = catch_unwind(AssertUnwindSafe(|| job.finish()))
                        .unwrap_or_else(|_| Err(cleanup_error()));
                    if let Some(completion) = completion {
                        let _ = completion.send(result);
                    }
                }
                let _ = finished.send(());
            })
            .map_err(|source| server_error(LocalServerStage::SpawnWorker).with_source(source))?;
        Ok(Self {
            sender,
            worker,
            finished: completion,
        })
    }

    pub(super) fn own(
        &self,
        shared: Arc<ServerShared>,
        permit: OwnedSemaphorePermit,
    ) -> ConnectionCleanup {
        ConnectionCleanup {
            sender: self.sender.clone(),
            job: Some(CleanupJob {
                session: None,
                authentication: None,
                registration: None,
                writer: None,
                receiver: None,
                active: false,
                close_dispatched: false,
                completion: None,
                worker: ActiveWorker::new(shared),
                _permit: permit,
            }),
        }
    }

    pub(super) async fn finish(self) -> Result<(), LocalServerError> {
        drop(self.sender);
        // Do not occupy a Tokio blocking worker while callbacks/jobs drain.
        // Cancellation here leaves the native supervisor owning those jobs.
        self.finished.await.map_err(|_| cleanup_error())?;
        tokio::task::spawn_blocking(move || self.worker.join())
            .await
            .map_err(|_| cleanup_error())?
            .map_err(|_| cleanup_error())
    }
}

pub(super) struct ConnectionCleanup {
    sender: native_channel::Sender<CleanupJob>,
    job: Option<CleanupJob>,
}

impl ConnectionCleanup {
    pub(super) fn attach(
        &mut self,
        session: Arc<ServerConnectionSession>,
        authentication: Arc<AuthenticatedRequestContext>,
        registration: ActiveConnectionLease,
    ) {
        let job = self.job.as_mut().expect("connection cleanup is owned");
        job.session = Some(session);
        job.authentication = Some(authentication);
        job.registration = Some(registration);
    }

    pub(super) fn start_writer(&mut self, ordinal: u64) -> Result<(), LocalServerError> {
        let job = self.job.as_mut().expect("connection cleanup is owned");
        let session = job.session.as_ref().expect("registered session").clone();
        let (sender, receiver) = mpsc::channel(job.worker.0.limits.writer_queue_messages);
        job.receiver = Some(receiver);
        job.writer = Some(
            thread::Builder::new()
                .name(format!("undercurrent-local-writer-{ordinal}"))
                .spawn(move || {
                    while let Ok(Some(write)) = session.wait_for_write() {
                        if sender.blocking_send(write).is_err() {
                            break;
                        }
                    }
                })
                .map_err(|source| {
                    server_error(LocalServerStage::SpawnWorker).with_source(source)
                })?,
        );
        job.active = true;
        Ok(())
    }

    pub(super) fn receiver(&mut self) -> &mut mpsc::Receiver<PendingServerWrite> {
        self.job
            .as_mut()
            .and_then(|job| job.receiver.as_mut())
            .expect("started writer")
    }

    pub(super) fn mark_close_dispatched(&mut self) {
        // An error or panic does not establish that a callback had no effects.
        // Record the attempt before calling it so fallback cleanup cannot retry.
        self.job
            .as_mut()
            .expect("connection cleanup is owned")
            .close_dispatched = true;
    }

    pub(super) fn begin_close(&mut self) -> Result<(), LocalServerError> {
        self.job
            .as_mut()
            .expect("connection cleanup is owned")
            .close()
    }

    pub(super) async fn finish(mut self) -> Result<(), LocalServerError> {
        let (completion, completed) = oneshot::channel();
        self.job
            .as_mut()
            .expect("connection cleanup is owned")
            .completion = Some(completion);
        self.submit();
        completed.await.map_err(|_| cleanup_error())?
    }

    fn submit(&mut self) {
        if let Some(job) = self.job.take() {
            // Each job retains its admission permit until retirement, so this
            // queue is bounded by maximum_connections even though send itself
            // never waits for capacity. The receiver outlives all senders and
            // isolates job panics; it cannot normally disconnect here.
            self.sender.send(job).unwrap_or_else(|_| {
                panic!("connection cleanup supervisor disconnected while owned")
            });
        }
    }
}

impl Drop for ConnectionCleanup {
    fn drop(&mut self) {
        // No callbacks, thread creation, or join on an async task's drop path.
        // The native supervisor drains even after the Tokio runtime is gone.
        self.submit();
    }
}

struct CleanupJob {
    session: Option<Arc<ServerConnectionSession>>,
    authentication: Option<Arc<AuthenticatedRequestContext>>,
    registration: Option<ActiveConnectionLease>,
    writer: Option<JoinHandle<()>>,
    receiver: Option<mpsc::Receiver<PendingServerWrite>>,
    active: bool,
    close_dispatched: bool,
    completion: Option<oneshot::Sender<Result<(), LocalServerError>>>,
    worker: ActiveWorker,
    _permit: OwnedSemaphorePermit,
}

impl CleanupJob {
    fn close(&mut self) -> Result<(), LocalServerError> {
        let Some(session) = &self.session else {
            return Ok(());
        };
        let closed = catch_unwind(AssertUnwindSafe(|| session.close()))
            .map(|_| ())
            .map_err(|_| cleanup_error());
        // Close wakes the core wait; receiver drop releases blocking_send.
        drop(self.receiver.take());
        let dispatched = if self.active && !self.close_dispatched {
            self.close_dispatched = true;
            catch_unwind(AssertUnwindSafe(|| {
                // Preserve best-effort close errors, but do not let a panic
                // skip channel release or writer retirement.
                let _ = self.worker.0.dispatcher.dispatch_authenticated(
                    self.authentication
                        .as_ref()
                        .expect("authenticated session")
                        .clone(),
                    session.clone(),
                    ServerAction::CloseConnection,
                );
            }))
            .map_err(|_| cleanup_error())
        } else {
            Ok(())
        };
        closed.and(dispatched)
    }

    fn finish(mut self) -> Result<(), LocalServerError> {
        let closed = self.close();
        let joined = self
            .writer
            .take()
            .map_or(Ok(()), |writer| writer.join().map_err(|_| cleanup_error()));
        drop(self.session.take());
        // Product cleanup uses logical client IDs. Keep the lease until both
        // that cleanup and the writer join finish, before allowing ID reuse.
        drop(self.registration.take());
        closed.and(joined)
    }
}

fn cleanup_error() -> LocalServerError {
    server_error(LocalServerStage::SpawnWorker)
}
