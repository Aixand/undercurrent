// SPDX-License-Identifier: Apache-2.0
//! Read daemon status, list a bounded page, and consume one watch snapshot.
//! Run against an already running daemon; see ../../docs/sdk/rust/connect-and-query.md (from this crate).

mod common;

use std::path::PathBuf;
use std::time::Duration;

use undercurrent_protocol::{
    ListSessionsRequest, ProductRequest, ProductStreamItem, ProductStreamSpec, ProductSuccess,
    SessionWatchSpec,
};
use undercurrent_sdk::ProductSubscriptionItem;

fn main() -> common::ExampleResult<()> {
    let endpoint = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: query_watch ABSOLUTE_ENDPOINT")?;
    common::with_client(endpoint, |client| {
        let timeout = Duration::from_secs(5);
        match client.request(ProductRequest::GetDaemonStatus, timeout)? {
            ProductSuccess::GetDaemonStatus(status) => println!(
                "accepting={} managed_sessions={}",
                status.accepting_connections, status.managed_sessions
            ),
            _ => return Err("unexpected daemon status response".into()),
        }
        let query = ListSessionsRequest {
            filter: Default::default(),
            sort: Default::default(),
            projection: Default::default(),
            page_size: 20,
            cursor: None,
            include_total: true,
        };
        match client.request(ProductRequest::ListSessions(query.clone()), timeout)? {
            ProductSuccess::ListSessions(page) => println!(
                "list_items={} revision={} more={}",
                page.items.len(),
                page.snapshot_revision,
                page.next_cursor.is_some()
            ),
            _ => return Err("unexpected list response".into()),
        }
        let opened = client.open_subscription(
            ProductStreamSpec::SessionWatch(SessionWatchSpec { query }),
            timeout,
        )?;
        let item = opened.subscription.recv_timeout(timeout);
        opened.subscription.cancel();
        match item? {
            ProductSubscriptionItem::Json {
                item: ProductStreamItem::SessionSnapshot(page),
                ..
            } => println!("watch_snapshot_items={}", page.items.len()),
            ProductSubscriptionItem::Gap(gap) => {
                eprintln!(
                    "watch gap: {} / {}",
                    gap.reason.as_str(),
                    gap.recovery.as_str()
                );
                return Err("watch needs a fresh snapshot; see streams-and-recovery.md".into());
            }
            _ => return Err("expected initial watch snapshot".into()),
        }
        println!("phase={:?}", client.pressure().phase);
        Ok(())
    })
}
