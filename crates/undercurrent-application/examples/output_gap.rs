// SPDX-License-Identifier: Apache-2.0

//! Demonstrates explicit recovery from a bounded, in-memory output retention gap.
//! Run from the repository root with:
//! `cargo run --locked -p undercurrent-application --example output_gap`.
//! No daemon, process, credentials, or persistent state is needed.

use undercurrent_application::{
    OutputGapRecovery, OutputOffset, OutputReplay, TerminalOutputLimits, TerminalOutputStore,
};
use undercurrent_domain::{OutputCaptureMode, SessionId};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = TerminalOutputStore::new(TerminalOutputLimits {
        per_session_bytes: 8,
        global_bytes: 8,
        chunk_bytes: 4,
        append_bytes: 12,
        replay_bytes: 4,
        sessions: 1,
        subscriptions: 1,
    })?;
    let session = SessionId::new("output-gap-example")?;
    store.begin_session(
        session.clone(),
        OutputCaptureMode::Opaque,
        OutputOffset::ZERO,
    )?;
    store.append(&session, b"abcdefghijkl")?;

    let mut checkpoint = OutputOffset::ZERO;
    loop {
        match store.replay(
            &session,
            checkpoint,
            4,
            OutputGapRecovery::RestartAtAvailableOffset,
        )? {
            OutputReplay::Gap {
                requested,
                available_from,
                next_offset,
                ..
            } => {
                println!(
                    "gap: requested={}, available={}, end={}",
                    requested.value(),
                    available_from.value(),
                    next_offset.value()
                );
                // This example explicitly accepts the lost prefix. An audit
                // consumer could instead stop and report incomplete history.
                checkpoint = available_from;
            }
            OutputReplay::Data {
                slices,
                from,
                to,
                has_more,
                ..
            } => {
                let bytes: Vec<u8> = slices
                    .iter()
                    .flat_map(|slice| slice.bytes().iter().copied())
                    .collect();
                println!(
                    "data: {}..{} = {}",
                    from.value(),
                    to.value(),
                    std::str::from_utf8(&bytes)?
                );
                checkpoint = to;
                if !has_more {
                    break;
                }
            }
        }
    }
    store.remove_session(&session)?;
    Ok(())
}
