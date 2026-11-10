// SPDX-License-Identifier: Apache-2.0

//! State-changing application use cases.

mod activation;
mod management;
mod mutation;
mod process_exit;
mod restart;
mod scheduled_restart;
mod start;
mod startup_reconciliation;
mod stop;
mod terminal;

pub use mutation::*;
pub use process_exit::*;
pub use restart::*;
pub use scheduled_restart::*;
pub use start::*;
pub use startup_reconciliation::*;
pub use stop::*;
pub use terminal::*;

#[cfg(test)]
#[path = "command_tests.rs"]
mod command_tests;
pub use management::*;
