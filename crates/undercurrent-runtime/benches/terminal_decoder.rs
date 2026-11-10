// SPDX-License-Identifier: Apache-2.0

use std::hint::black_box;
use std::time::Instant;

const SCHEMA: &str = "undercurrent.terminal-decoder-throughput/v0.1.0-beta";
const INPUT_BYTES: usize = 64 * 1_024;
const WARMUP_BYTES: usize = 16 * 1_024 * 1_024;
const MEASUREMENT_BYTES: usize = 256 * 1_024 * 1_024;
const MEBIBYTE: f64 = 1_048_576.0;
const FRAGMENT_BYTES: [usize; 8] = [1, 3, 7, 31, 127, 511, 2_048, 4_096];

#[derive(Clone, Copy)]
enum Feed {
    Contiguous,
    Fragmented,
}

struct Workload {
    scenario: &'static str,
    payload: Vec<u8>,
    feed: Feed,
}

struct Measurement {
    bytes: usize,
    elapsed_nanos: u128,
    mib_per_second_per_core: f64,
}

fn main() {
    let workloads = [
        Workload {
            scenario: "plain",
            payload: repeated_payload(
                b"2026-08-20T08:15:30Z INFO worker=17 request=req-0042 completed in 8ms\r\n",
            ),
            feed: Feed::Contiguous,
        },
        Workload {
            scenario: "ansi_heavy",
            payload: repeated_payload(
                b"\x1b[2K\r\x1b[1;38;5;39mINFO\x1b[0m \x1b[48;2;24;28;36m build \x1b[0m 72%\x1b[1A\x1b[18C\x1b[4mactive\x1b[0m\r\n",
            ),
            feed: Feed::Contiguous,
        },
        Workload {
            scenario: "utf8",
            payload: repeated_payload(
                "Καλημέρα κόσμε · こんにちは世界 · नमस्ते दुनिया · cafe\u{301} · 🚀\r\n".as_bytes(),
            ),
            feed: Feed::Contiguous,
        },
        Workload {
            scenario: "fragmented",
            payload: repeated_payload(
                "\x1b[38;2;90;180;255m進捗\x1b[0m [██████░░] 75% · compiling 🚀\r\n"
                    .as_bytes(),
            ),
            feed: Feed::Fragmented,
        },
    ];

    println!(
        "schema\tscenario\tworker_threads\tmeasured_bytes\telapsed_nanos\tmib_per_second_per_core"
    );
    for workload in workloads {
        let measurement = measure(&workload);
        println!(
            "{}\t{}\t1\t{}\t{}\t{:.3}",
            SCHEMA,
            workload.scenario,
            measurement.bytes,
            measurement.elapsed_nanos,
            measurement.mib_per_second_per_core,
        );
    }
}

fn repeated_payload(unit: &[u8]) -> Vec<u8> {
    assert!(
        !unit.is_empty(),
        "terminal decoder workload unit must not be empty"
    );
    assert!(
        unit.len() <= INPUT_BYTES,
        "terminal decoder workload unit must fit the input buffer"
    );
    let mut payload = Vec::with_capacity(INPUT_BYTES);
    while payload.len() + unit.len() <= INPUT_BYTES {
        payload.extend_from_slice(unit);
    }
    payload.resize(INPUT_BYTES, b' ');
    payload
}

fn measure(workload: &Workload) -> Measurement {
    assert_eq!(workload.payload.len(), INPUT_BYTES);
    let mut parser = vt100::Parser::new(40, 120, 0);
    feed_bytes(&mut parser, &workload.payload, workload.feed, WARMUP_BYTES);

    let started = Instant::now();
    let bytes = feed_bytes(
        &mut parser,
        &workload.payload,
        workload.feed,
        MEASUREMENT_BYTES,
    );
    let elapsed = started.elapsed();
    black_box(parser.screen().cursor_position());

    let elapsed_seconds = elapsed.as_secs_f64();
    assert!(
        elapsed_seconds > 0.0,
        "terminal decoder measurement clock did not advance"
    );
    Measurement {
        bytes,
        elapsed_nanos: elapsed.as_nanos(),
        mib_per_second_per_core: (bytes as f64 / MEBIBYTE) / elapsed_seconds,
    }
}

fn feed_bytes(
    parser: &mut vt100::Parser,
    payload: &[u8],
    feed: Feed,
    target_bytes: usize,
) -> usize {
    assert_eq!(target_bytes % payload.len(), 0);
    let iterations = target_bytes / payload.len();
    for _ in 0..iterations {
        match feed {
            Feed::Contiguous => parser.process(black_box(payload)),
            Feed::Fragmented => feed_fragmented(parser, payload),
        }
    }
    iterations * payload.len()
}

fn feed_fragmented(parser: &mut vt100::Parser, payload: &[u8]) {
    let mut offset = 0;
    let mut fragment = 0;
    while offset < payload.len() {
        let end = offset
            .saturating_add(FRAGMENT_BYTES[fragment % FRAGMENT_BYTES.len()])
            .min(payload.len());
        parser.process(black_box(&payload[offset..end]));
        offset = end;
        fragment += 1;
    }
}
