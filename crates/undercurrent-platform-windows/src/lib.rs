// SPDX-License-Identifier: Apache-2.0
//! Internal Windows OS primitives shared by Undercurrent workspace adapters.
//!
//! This Windows-only crate owns checked native handle operations, not product
//! protocols or session policy. Public exports serve sibling crates; supported
//! external clients use `undercurrent-sdk` and `undercurrent-protocol`.
//!
//! - `PinnedPath` and private-file helpers retain and validate filesystem objects.
//! - `PipeListenerHandle` supplies account-restricted asynchronous pipe handles;
//!   `BlockingPipeStream` adapts the same primitive for private host workers.
//! - `ProcessHandle`, `JobChild`, and `JobProcessObserver` distinguish exact
//!   process identity, tree control, and bounded membership observations.
//! - `ConsoleSignals` supports cancellable blocking notification workers.
//!
//! OS success does not establish a durable application result. Callers own
//! framing, request deadlines, mutation reconciliation, and storage transactions.
//! The crate README and linked `docs/` pages explain those ownership boundaries.
#![cfg(windows)]

mod console;
mod listeners;
mod process;
pub use console::{ConsoleSignalHandle, ConsoleSignals, prevent_standard_handle_inheritance};
pub use listeners::TcpListenerObservation;
pub use process::{ProcessHandle, ProcessIdentity, command_interpreter, windows_directory};
mod job;
mod job_observation;
pub use job::{JobChild, JobControl};
pub use job_observation::{JobProcessObserver, JobProcessSnapshot};
mod security;
pub use security::current_user_sid;
mod file_owner;
pub use file_owner::with_current_user_file_owner;
mod pipe;
pub use pipe::{PipeListenerHandle, pipe_name, verify_pipe_client, verify_pipe_server};
mod blocking_pipe;
pub use blocking_pipe::{BlockingPipeListener, BlockingPipeStream};
mod filesystem;
pub use filesystem::PrivateRemovalFile;
pub use filesystem::{
    FileObjectIdentity, PinnedPath, create_private_file, file_identity, open_private_file,
    open_private_lock_file, prepare_private_directory, replace_private_file,
};
