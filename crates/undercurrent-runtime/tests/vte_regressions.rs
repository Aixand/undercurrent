// SPDX-License-Identifier: Apache-2.0

use vte as local_vte;
use vte_reference as reference_vte;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Event {
    Print(char),
    Execute(u8),
    Hook {
        params: Vec<Vec<u16>>,
        intermediates: Vec<u8>,
        ignore: bool,
        action: char,
    },
    Put(u8),
    Unhook,
    Osc {
        params: Vec<Vec<u8>>,
        bell_terminated: bool,
    },
    Csi {
        params: Vec<Vec<u16>>,
        intermediates: Vec<u8>,
        ignore: bool,
        action: char,
    },
    Esc {
        intermediates: Vec<u8>,
        ignore: bool,
        byte: u8,
    },
}

#[derive(Default)]
struct Recorder {
    events: Vec<Event>,
}

macro_rules! impl_recorder {
    ($module:ident) => {
        impl $module::Perform for Recorder {
            fn print(&mut self, character: char) {
                self.events.push(Event::Print(character));
            }

            fn execute(&mut self, byte: u8) {
                self.events.push(Event::Execute(byte));
            }

            fn hook(
                &mut self,
                params: &$module::Params,
                intermediates: &[u8],
                ignore: bool,
                action: char,
            ) {
                self.events.push(Event::Hook {
                    params: params.iter().map(<[u16]>::to_vec).collect(),
                    intermediates: intermediates.to_vec(),
                    ignore,
                    action,
                });
            }

            fn put(&mut self, byte: u8) {
                self.events.push(Event::Put(byte));
            }

            fn unhook(&mut self) {
                self.events.push(Event::Unhook);
            }

            fn osc_dispatch(&mut self, params: &[&[u8]], bell_terminated: bool) {
                self.events.push(Event::Osc {
                    params: params.iter().map(|param| param.to_vec()).collect(),
                    bell_terminated,
                });
            }

            fn csi_dispatch(
                &mut self,
                params: &$module::Params,
                intermediates: &[u8],
                ignore: bool,
                action: char,
            ) {
                self.events.push(Event::Csi {
                    params: params.iter().map(<[u16]>::to_vec).collect(),
                    intermediates: intermediates.to_vec(),
                    ignore,
                    action,
                });
            }

            fn esc_dispatch(&mut self, intermediates: &[u8], ignore: bool, byte: u8) {
                self.events.push(Event::Esc {
                    intermediates: intermediates.to_vec(),
                    ignore,
                    byte,
                });
            }
        }
    };
}

impl_recorder!(local_vte);
impl_recorder!(reference_vte);

fn is_optimized_ground_noop(byte: u8) -> bool {
    matches!(byte, 0x90 | 0x9b | 0x9d..=0xc1 | 0xf5..=0xff)
}

fn utf8_probe_sequence(lead: u8) -> Vec<u8> {
    match lead {
        0xc2..=0xdf => vec![lead, 0x80],
        0xe0 => vec![lead, 0xa0, 0x80],
        0xe1..=0xec | 0xee..=0xef => vec![lead, 0x80, 0x80],
        0xed => vec![lead, 0x80, 0x80],
        0xf0 => vec![lead, 0x90, 0x80, 0x80],
        0xf1..=0xf4 => vec![lead, 0x80, 0x80, 0x80],
        _ => unreachable!("not a pristine Ground UTF-8 lead: {lead:#04x}"),
    }
}

#[test]
fn every_fresh_ground_byte_matches_the_registry_oracle_and_exact_fast_paths() {
    for byte in 0_u8..=u8::MAX {
        let mut local_parser = local_vte::Parser::new();
        let mut reference_parser = reference_vte::Parser::new();
        let mut local = Recorder::default();
        let mut reference = Recorder::default();

        local_parser.advance(&mut local, byte);
        reference_parser.advance(&mut reference, byte);
        let reference_initial = reference.events.clone();
        assert_eq!(
            local.events, reference_initial,
            "fresh Ground callback mismatch for byte {byte:#04x}"
        );

        for probe in b"Q\x1b[0mR" {
            local_parser.advance(&mut local, *probe);
            reference_parser.advance(&mut reference, *probe);
        }
        assert_eq!(
            local.events, reference.events,
            "state-revealing suffix diverged for fresh Ground byte {byte:#04x}"
        );
        assert_eq!(
            reference.events.last(),
            Some(&Event::Print('R')),
            "registry oracle did not return to Ground for byte {byte:#04x}"
        );

        let registry_direct_print = reference_initial == vec![Event::Print(char::from(byte))];
        assert_eq!(
            registry_direct_print,
            (0x20..=0x7f).contains(&byte),
            "optimized direct-print range differs from registry vte: {byte:#04x}"
        );
        let registry_ground_noop =
            reference_initial.is_empty() && reference.events.first() == Some(&Event::Print('Q'));
        assert_eq!(
            registry_ground_noop,
            is_optimized_ground_noop(byte),
            "optimized no-op set differs from registry vte: {byte:#04x}"
        );

        if (0xc2..=0xf4).contains(&byte) {
            let sequence = utf8_probe_sequence(byte);
            let character = std::str::from_utf8(&sequence)
                .expect("probe sequence must be valid UTF-8")
                .chars()
                .next()
                .expect("probe sequence must contain one character");
            let mut local_parser = local_vte::Parser::new();
            let mut reference_parser = reference_vte::Parser::new();
            let mut local = Recorder::default();
            let mut reference = Recorder::default();
            for input in sequence {
                local_parser.advance(&mut local, input);
                reference_parser.advance(&mut reference, input);
            }
            local_parser.advance(&mut local, b'Q');
            reference_parser.advance(&mut reference, b'Q');
            assert_eq!(
                local.events, reference.events,
                "valid UTF-8 completion diverged for lead {byte:#04x}"
            );
            assert_eq!(
                reference.events,
                vec![Event::Print(character), Event::Print('Q')],
                "registry UTF-8 state probe was not exact for lead {byte:#04x}"
            );
        }
    }
}

macro_rules! trace_function {
    ($name:ident, $module:ident) => {
        fn $name(corpus: &[u8], chunk_sizes: &[usize]) -> Vec<Event> {
            assert!(
                !chunk_sizes.is_empty() && chunk_sizes.iter().all(|size| *size > 0),
                "chunk partitions must be nonempty and positive"
            );
            let mut parser = $module::Parser::new();
            let mut recorder = Recorder::default();
            let mut offset = 0;
            let mut chunk = 0;
            while offset < corpus.len() {
                let size = chunk_sizes[chunk % chunk_sizes.len()];
                let end = offset.saturating_add(size).min(corpus.len());
                for byte in &corpus[offset..end] {
                    parser.advance(&mut recorder, *byte);
                }
                offset = end;
                chunk += 1;
            }
            recorder.events
        }
    };
}

trace_function!(local_trace, local_vte);
trace_function!(reference_trace, reference_vte);

fn assert_oracle_equivalent(label: &str, corpus: &[u8], partitions: &[Vec<usize>]) {
    let baseline = reference_trace(corpus, &partitions[0]);
    assert_eq!(
        baseline.last(),
        Some(&Event::Print('Q')),
        "{label} registry corpus did not finish in Ground"
    );
    for chunks in partitions {
        let reference = reference_trace(corpus, chunks);
        let local = local_trace(corpus, chunks);
        assert_eq!(
            reference, baseline,
            "{label} registry trace changed for chunk pattern {chunks:?}"
        );
        assert_eq!(
            local, reference,
            "{label} local trace differs from registry for chunk pattern {chunks:?}"
        );
    }
}

fn deterministic_random_corpus() -> Vec<u8> {
    let mut state = 0x6a09_e667_f3bc_c909_u64;
    let mut corpus = Vec::with_capacity(4_102);
    for _ in 0..4_096 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        corpus.push((state >> 24) as u8);
    }
    corpus.extend_from_slice(b"\x18\x1b[0mQ");
    corpus
}

#[test]
fn fragmented_structured_and_random_corpora_match_the_registry_oracle() {
    let mut structured = b"A\x00\x1b[31;1mB\xc3\xa9\x1b]2;title\x07\x1bP1;2|xy\x9c\x1b(BZ".to_vec();
    structured.extend_from_slice(b"\x1b]3;string-terminated\x1b\\");
    structured.extend_from_slice(b"\xf0\x9f\x92\xa9\xf0(\x8c\xbc");
    structured.extend_from_slice(b"\x1bP$qpayload\x9c");
    structured.extend_from_slice(b"\x18\x1b[0mQ");

    let partitions = [
        vec![structured.len()],
        vec![1],
        vec![1, 2, 3, 5, 8, 13],
        vec![16, 1, 31, 2, 7],
    ];
    assert_oracle_equivalent("structured", &structured, &partitions);

    let random = deterministic_random_corpus();
    let random_partitions = [
        vec![random.len()],
        vec![1],
        vec![2, 7, 31, 127],
        vec![257, 3, 61, 5, 1_024],
    ];
    assert_oracle_equivalent("deterministic-random", &random, &random_partitions);
}
