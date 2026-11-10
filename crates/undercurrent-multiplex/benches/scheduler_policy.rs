// SPDX-License-Identifier: Apache-2.0

//! Reproducible scheduler-only workload probe; excludes socket and peer latency.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;
use undercurrent_multiplex::{
    DeliverySemantics, FrameRoute, OutboundFrame, OutboundScheduler, ScheduledFrame,
    SchedulerLimits, SchedulerMessageLimits, StreamKind, StreamRecoveryAction, TrafficClass,
};
use undercurrent_wire::{ConnectionLimits, StreamId, StreamSequence};

fn main() {
    for (name, sizes, stalled) in [
        ("uniform", [512usize; 4], false),
        ("mixed", [64, 256, 1024, 4096], false),
        ("mixed_credit_stall", [64, 256, 1024, 4096], true),
    ] {
        run(name, sizes, stalled);
    }
}

fn run(name: &str, sizes: [usize; 4], stalled: bool) {
    let frame = 4096u64;
    let queue = 512 * 1024u64;
    let remainder = (queue - 4 * frame) / 8;
    let budgets = [
        frame + 2 * remainder,
        frame + 2 * remainder,
        frame + remainder,
        frame + 3 * remainder,
    ];
    let limits = ConnectionLimits::new(frame as u32, 8, 8, queue, 64 * 1024, 8192).unwrap();
    let scheduler = OutboundScheduler::new(
        SchedulerLimits::new(
            limits,
            budgets,
            queue,
            4 * frame,
            SchedulerMessageLimits::new(256, 32, [64; 4]).unwrap(),
        )
        .unwrap(),
    );
    let streams: Vec<_> = (0..8)
        .map(|n| StreamId::new(format!("peer-{n}")).unwrap())
        .collect();
    let payloads: Vec<Arc<[u8]>> = sizes
        .into_iter()
        .map(|size| Arc::from(vec![0; size]))
        .collect();
    let mut sequence = [0u64; 8];
    let mut queued: Vec<VecDeque<Instant>> = (0..8).map(|_| VecDeque::new()).collect();
    let enqueue = |peer: usize, sequence: &mut [u64; 8], queued: &mut Vec<VecDeque<Instant>>| {
        sequence[peer] += 1;
        scheduler
            .enqueue(
                OutboundFrame::stream(
                    TrafficClass::ALL[peer / 2],
                    streams[peer].clone(),
                    StreamSequence::new(sequence[peer]),
                    DeliverySemantics::Reliable,
                    payloads[peer / 2].clone(),
                    frame as u32,
                )
                .unwrap(),
            )
            .unwrap();
        queued[peer].push_back(Instant::now());
    };
    for (peer, stream) in streams.iter().enumerate() {
        scheduler
            .open_stream(
                stream.clone(),
                StreamKind::TerminalOutput,
                StreamRecoveryAction::RestartQuery,
            )
            .unwrap();
        for _ in 0..8 {
            enqueue(peer, &mut sequence, &mut queued);
        }
    }
    let mut latencies: Vec<Vec<u128>> = (0..4).map(|_| Vec::new()).collect();
    let mut counts = [0u64; 8];
    let mut bytes = [0u64; 4];
    let mut previous = [0usize; 8];
    let mut max_gaps = [0usize; 8];
    let mut withheld = 0u64;
    let started = Instant::now();
    for turn in 1..=150_000 {
        if turn % 1000 == 0 && withheld != 0 {
            scheduler
                .grant_consumed_credit(&streams[7], withheld)
                .unwrap();
            withheld = 0;
        }
        let ScheduledFrame::Data(selected) = scheduler.dequeue().unwrap().unwrap() else {
            panic!("unexpected gap")
        };
        let FrameRoute::Stream(id) = selected.route() else {
            panic!("stream route")
        };
        let peer = streams.iter().position(|stream| stream == id).unwrap();
        counts[peer] += 1;
        bytes[peer / 2] += selected.encoded_len();
        latencies[peer / 2].push(queued[peer].pop_front().unwrap().elapsed().as_nanos());
        max_gaps[peer] = max_gaps[peer].max(turn - previous[peer]);
        previous[peer] = turn;
        let consumed = selected.flow_controlled_bytes();
        if peer == 7 && stalled {
            withheld += consumed;
        } else if consumed != 0 {
            scheduler.grant_consumed_credit(id, consumed).unwrap();
        }
        enqueue(peer, &mut sequence, &mut queued);
    }
    let seconds = started.elapsed().as_secs_f64();
    println!(
        "workload={name} elapsed_ms={:.3} frames_per_second={:.0} peers={counts:?} max_selection_gaps={max_gaps:?}",
        seconds * 1000.0,
        150000.0 / seconds
    );
    for class in 0..4 {
        latencies[class].sort_unstable();
        let p99 = latencies[class][latencies[class].len() * 99 / 100];
        println!(
            "  class={} frames={} bytes={} queue_p99_us={:.3}",
            TrafficClass::ALL[class].as_str(),
            latencies[class].len(),
            bytes[class],
            p99 as f64 / 1000.0
        );
    }
    assert!(counts.into_iter().all(|count| count > 0));
}
