// SPDX-License-Identifier: Apache-2.0
//! Create, immediately stop, and remove one new disposable session.
//! Run only in an isolated daemon home; see ../../docs/sdk/rust/operations.md (from this crate).
//! An error is not retried: inspect the printed name/ID to reconcile cleanup.

mod common;

use std::path::PathBuf;
use std::time::Duration;

use undercurrent_protocol::{
    CommandSpecDto, MutationContext, ProductOperation, ProductRequest, ProductSuccess,
    RemovalConfirmationDto, RemoveSessionRequest, StartSessionRequest, StopSessionRequest,
    TerminalSizeDto,
};

fn main() -> common::ExampleResult<()> {
    let mut args = std::env::args().skip(1);
    let endpoint = PathBuf::from(args.next().ok_or("missing endpoint")?);
    let name = args
        .next()
        .ok_or("missing unique disposable session name")?;
    let program = args
        .next()
        .ok_or("usage: create_and_stop ABSOLUTE_ENDPOINT UNIQUE_NAME PROGRAM [ARG ...]")?;
    let arguments = args.collect();
    common::with_client(endpoint, |client| {
        let timeout = Duration::from_secs(30);
        println!("creating disposable session named {name}");
        let started = client.request(
            ProductRequest::StartSession(StartSessionRequest {
                launch_mode: undercurrent_protocol::LaunchModeDto::Native,
                mutation: MutationContext::current(ProductOperation::StartSession),
                dispatch: None,
                name,
                command: CommandSpecDto { program, arguments },
                working_directory: None,
                environment: Default::default(),
                terminal: TerminalSizeDto {
                    columns: 80,
                    rows: 24,
                },
                output_capture_mode: Default::default(),
                execution: Default::default(),
                health: Default::default(),
                profile: None,
                profile_binding: Default::default(),
                labels: Default::default(),
                metadata: Default::default(),
            }),
            timeout,
        )?;
        let session_id = match started {
            ProductSuccess::StartSession(result) => result.session.id,
            _ => return Err("unexpected start result; inspect the session name".into()),
        };
        println!("created {session_id}; stopping it now");
        match client.request(
            ProductRequest::StopSession(StopSessionRequest {
                graceful_only: false,
                mutation: MutationContext::current(ProductOperation::StopSession),
                session_id: session_id.clone(),
                expected_revision: None,
                force: false,
            }),
            timeout,
        )? {
            ProductSuccess::StopSession(_) => {}
            _ => return Err("unexpected stop result; inspect the printed session ID".into()),
        }
        match client.request(
            ProductRequest::RemoveSession(RemoveSessionRequest {
                mutation: MutationContext::current(ProductOperation::RemoveSession),
                session_id: session_id.clone(),
                confirmation: RemovalConfirmationDto::Forced,
            }),
            timeout,
        )? {
            ProductSuccess::RemoveSession(_) => println!("removed {session_id}"),
            _ => return Err("unexpected remove result; inspect the printed session ID".into()),
        }
        Ok(())
    })
}
