// SPDX-License-Identifier: Apache-2.0

//! Whole-envelope serialization shared by ordinary workers and terminal failure.
use std::io::{self, Write};
use std::sync::Mutex;

use serde_json::Value;

pub(super) struct EnvelopeOutput<W> {
    state: Mutex<State<W>>,
}

struct State<W> {
    writer: W,
    terminal: bool,
}

impl<W: Write> EnvelopeOutput<W> {
    pub(super) fn new(writer: W) -> Self {
        Self {
            state: Mutex::new(State {
                writer,
                terminal: false,
            }),
        }
    }

    pub(super) fn emit(&self, value: &Value, terminal: bool) -> io::Result<()> {
        let mut state = self.state.lock().expect("stdout lock");
        if state.terminal {
            return Ok(());
        }
        // Fatal is the final complete frame. Suppress late worker output while
        // holding the same lock, before main can exit and truncate another frame.
        state.terminal = terminal;
        serde_json::to_writer(&mut state.writer, value)?;
        state.writer.write_all(b"\n")?;
        state.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, mpsc};
    use std::thread;

    struct FragmentedWriter {
        bytes: Arc<Mutex<Vec<u8>>>,
        first_write: Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>,
    }
    impl Write for FragmentedWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.is_empty() {
                return Ok(0);
            }
            self.bytes.lock().unwrap().push(bytes[0]);
            if let Some((entered, resume)) = self.first_write.take() {
                entered.send(()).unwrap();
                resume.recv().unwrap();
            }
            thread::yield_now();
            Ok(1)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn fatal_waits_for_a_partial_worker_frame_and_suppresses_later_frames() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let (entered, first_byte) = mpsc::channel();
        let (resume, continued) = mpsc::channel();
        let output = Arc::new(EnvelopeOutput::new(FragmentedWriter {
            bytes: bytes.clone(),
            first_write: Some((entered, continued)),
        }));
        let worker_output = output.clone();
        let worker = thread::spawn(move || {
            worker_output
                .emit(
                    &serde_json::json!({"type":"result", "id":"request-1"}),
                    false,
                )
                .unwrap();
        });
        first_byte.recv().unwrap();
        let fatal_output = output.clone();
        let fatal = thread::spawn(move || {
            fatal_output
                .emit(
                    &serde_json::json!({"type":"fatal", "error":"reader failed"}),
                    true,
                )
                .unwrap();
        });
        resume.send(()).unwrap();
        worker.join().unwrap();
        fatal.join().unwrap();
        let late = (0..16)
            .map(|id| {
                let output = output.clone();
                thread::spawn(move || {
                    output
                        .emit(&serde_json::json!({"type":"result", "id":id}), false)
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();
        for worker in late {
            worker.join().unwrap();
        }
        let output = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(output.ends_with('\n'));
        let lines = output
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            lines,
            vec![
                serde_json::json!({"type":"result", "id":"request-1"}),
                serde_json::json!({"type":"fatal", "error":"reader failed"})
            ]
        );
    }
}
