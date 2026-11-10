// SPDX-License-Identifier: Apache-2.0
//! Read bounded output and observe terminal chunks for up to five seconds.
//! Requires an existing session; prints offsets/counts, never terminal bytes.

mod common;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use undercurrent_protocol::{
    ProductRequest, ProductStreamSpec, ProductSuccess, ReadTerminalOutputRequest,
    TerminalInitialModeDto, TerminalOutputPageDto, TerminalOutputSpec, TerminalStreamItem,
};
use undercurrent_sdk::{ProductClientErrorCategory, ProductSubscriptionItem};

fn main() -> common::ExampleResult<()> {
    let mut args = std::env::args_os().skip(1);
    let endpoint = args.next().map(PathBuf::from).ok_or("missing endpoint")?;
    let session_id = args
        .next()
        .ok_or("usage: terminal_output ABSOLUTE_ENDPOINT SESSION_ID")?
        .into_string()
        .map_err(|_| "session ID must be UTF-8")?;
    common::with_client(endpoint, |client| {
        let timeout = Duration::from_secs(5);
        match client.request(
            ProductRequest::ReadTerminalOutput(ReadTerminalOutputRequest {
                session_id: session_id.clone(),
                from_offset: 0,
                max_bytes: 16_384,
            }),
            timeout,
        )? {
            ProductSuccess::ReadTerminalOutput(TerminalOutputPageDto::Data {
                from_offset,
                to_offset,
                bytes,
                has_more,
                ..
            }) => println!(
                "retained={from_offset}..{to_offset} bytes={} more={has_more}",
                bytes.len()
            ),
            ProductSuccess::ReadTerminalOutput(TerminalOutputPageDto::Gap {
                available_from,
                next_offset,
                ..
            }) => println!("retained gap: available={available_from}..{next_offset}"),
            _ => return Err("unexpected output response".into()),
        }
        // This independent tail view may overlap the finite page above.
        let opened = client.open_subscription(
            ProductStreamSpec::TerminalOutput(TerminalOutputSpec {
                session_id,
                initial: TerminalInitialModeDto::Tail { max_bytes: 16_384 },
            }),
            timeout,
        )?;
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match opened
                .subscription
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            {
                Ok(ProductSubscriptionItem::Terminal { sequence, item }) => match item {
                    TerminalStreamItem::Output(chunk) => println!(
                        "sequence={sequence} offset={} bytes={}",
                        chunk.start_offset(),
                        chunk.bytes().len()
                    ),
                    TerminalStreamItem::Screen(screen) => {
                        println!(
                            "sequence={sequence} screen_offset={}",
                            screen.output_offset()
                        );
                    }
                },
                Ok(ProductSubscriptionItem::Gap(gap)) => {
                    println!(
                        "stream gap: {} / {}",
                        gap.reason.as_str(),
                        gap.recovery.as_str()
                    );
                    break;
                }
                Ok(ProductSubscriptionItem::Json { .. }) => {
                    return Err("unexpected JSON terminal item".into());
                }
                Err(error) if error.category() == ProductClientErrorCategory::Timeout => break,
                Err(error) => return Err(error.into()),
            }
        }
        opened.subscription.close();
        Ok(())
    })
}
