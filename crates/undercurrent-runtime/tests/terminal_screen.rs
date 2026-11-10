// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use undercurrent_application::{
    OutputGapRecovery, OutputOffset, OutputReplay, PortErrorKind, TerminalOutputLimits,
    TerminalOutputStore, TerminalScreenDiffer, TerminalScreenReader, TerminalScreenSnapshot,
    TerminalScreenUpdate,
};
use undercurrent_domain::{
    OutputCaptureMode, OutputExclusions, ProfileRegion, ScreenAnchor, SessionId, TerminalSize,
};
use undercurrent_runtime::{TerminalScreenLimits, VtTerminalScreenStore};

#[test]
fn full_width_output_keeps_a_valid_snapshot_and_pending_wrap() {
    let output = output_store();
    let screens = screen_store(1, 64 * 1_024);
    let session = session("pending-wrap");
    screens
        .begin_session(session.clone(), size(8, 3), OutputOffset::ZERO)
        .unwrap();
    screens
        .observe_output(&output, &session, b"12345678")
        .unwrap();
    let full = snapshot(&screens, &session).unwrap();
    assert_eq!((full.cursor_row(), full.cursor_column()), (0, 7));

    // The protocol exposes a visible cursor, but the renderer must retain the
    // parser's pending wrap so the next printable byte starts the next row.
    let mut client = vt100::Parser::new(3, 8, 0);
    client.process(full.contents());
    client.process(b"x");
    screens.observe_output(&output, &session, b"x").unwrap();
    let next = snapshot(&screens, &session).unwrap();
    assert_eq!(client.screen().contents(), parser_screen(&next).contents());
    assert_eq!((next.cursor_row(), next.cursor_column()), (1, 1));
}

#[test]
fn output_retention_and_vt_screen_advance_at_one_checkpoint() {
    let output = output_store();
    let screens = screen_store(8, 64 * 1_024);
    let session = session("screen-one");
    screens
        .begin_session(session.clone(), size(80, 24), OutputOffset::ZERO)
        .unwrap();

    let bytes = b"hello\x1b[2;4Hworld\x1b[?25l";
    let appended = screens.observe_output(&output, &session, bytes).unwrap();
    let snapshot = snapshot(&screens, &session).expect("screen exists");

    assert_eq!(snapshot.output_offset(), appended.end);
    assert_eq!(snapshot.output_offset().value(), bytes.len() as u64);
    assert_eq!((snapshot.columns(), snapshot.rows()), (80, 24));
    assert_eq!((snapshot.cursor_row(), snapshot.cursor_column()), (1, 8));
    assert!(!snapshot.cursor_visible());
    assert!(
        snapshot
            .contents()
            .windows(5)
            .any(|window| window == b"hello")
    );
    assert!(
        snapshot
            .contents()
            .windows(5)
            .any(|window| window == b"world")
    );

    let OutputReplay::Data { slices, .. } = output
        .replay(
            &session,
            OutputOffset::ZERO,
            64 * 1_024,
            OutputGapRecovery::RestartAtAvailableOffset,
        )
        .unwrap()
    else {
        panic!("raw output unexpectedly gapped")
    };
    let retained = slices
        .iter()
        .flat_map(|slice| slice.bytes().iter().copied())
        .collect::<Vec<_>>();
    assert_eq!(retained, bytes);
}

#[test]
fn opaque_capture_retains_exact_ordered_bytes_without_screen_state() {
    let output = output_store();
    let screens = screen_store(1, 64 * 1_024);
    let session = session("opaque-capture");
    screens
        .begin_session_with_capture_mode(
            session.clone(),
            size(80, 24),
            OutputOffset::ZERO,
            OutputCaptureMode::Opaque,
        )
        .unwrap();

    let first = b"binary\0prefix\x1b]9091;uc=";
    let second = b"ready\x07suffix\xff";
    let first_append = screens.observe_output(&output, &session, first).unwrap();
    let second_append = screens.observe_output(&output, &session, second).unwrap();

    assert_eq!(first_append.end.value(), first.len() as u64);
    assert_eq!(
        second_append.end.value(),
        (first.len() + second.len()) as u64
    );
    assert_eq!(
        screens.capture_mode(&session),
        Some(OutputCaptureMode::Opaque)
    );
    assert_eq!(
        block_on(screens.snapshot(&session)).unwrap_err().kind(),
        PortErrorKind::Unsupported
    );
    assert_eq!(
        screens
            .health_sample(&session, 1_024, 80)
            .unwrap_err()
            .kind(),
        PortErrorKind::Unsupported
    );
    screens.resize(&session, size(132, 43)).unwrap();

    let OutputReplay::Data { slices, .. } = output
        .replay(
            &session,
            OutputOffset::ZERO,
            64 * 1_024,
            OutputGapRecovery::RestartAtAvailableOffset,
        )
        .unwrap()
    else {
        panic!("opaque output unexpectedly gapped")
    };
    let retained = slices
        .iter()
        .flat_map(|slice| slice.bytes().iter().copied())
        .collect::<Vec<_>>();
    assert_eq!(retained, [first.as_slice(), second.as_slice()].concat());
}

#[test]
fn failed_screen_install_restores_prior_parser_state() {
    let output = output_store();
    let screens = screen_store(1, 64 * 1_024);
    let session = session("screen-two");
    screens
        .begin_session(session.clone(), size(80, 24), OutputOffset::ZERO)
        .unwrap();
    screens.observe_output(&output, &session, b"prior").unwrap();

    {
        let _update = screens
            .prepare_session(session.clone(), size(120, 40), OutputOffset::new(5))
            .unwrap();
        let replacement = snapshot(&screens, &session).unwrap();
        assert_eq!((replacement.columns(), replacement.rows()), (120, 40));
        assert!(
            !replacement
                .contents()
                .windows(5)
                .any(|window| window == b"prior")
        );
    }

    let restored = snapshot(&screens, &session).unwrap();
    assert_eq!((restored.columns(), restored.rows()), (80, 24));
    assert!(
        restored
            .contents()
            .windows(5)
            .any(|window| window == b"prior")
    );
}

#[test]
fn resize_updates_the_parser_and_manual_output_divergence_is_rejected() {
    let output = output_store();
    let screens = screen_store(1, 64 * 1_024);
    let session = session("screen-three");
    screens
        .begin_session(session.clone(), size(80, 24), OutputOffset::ZERO)
        .unwrap();
    screens.resize(&session, size(132, 43)).unwrap();
    let resized = snapshot(&screens, &session).unwrap();
    assert_eq!((resized.columns(), resized.rows()), (132, 43));

    output.append(&session, b"outside-coordinator").unwrap();
    let error = screens
        .observe_output(&output, &session, b"must-not-apply")
        .unwrap_err();
    assert_eq!(error.kind(), PortErrorKind::InvalidData);
    assert_eq!(
        snapshot(&screens, &session).unwrap().output_offset(),
        OutputOffset::ZERO
    );
}

#[test]
fn screen_session_and_rendered_byte_limits_are_hard_bounds() {
    let output = output_store();
    let screens = screen_store(1, 1);
    let first = session("screen-four");
    screens
        .begin_session(first.clone(), size(80, 24), OutputOffset::ZERO)
        .unwrap();
    assert!(
        screens
            .begin_session(session("screen-five"), size(80, 24), OutputOffset::ZERO)
            .is_err()
    );

    screens.observe_output(&output, &first, b"visible").unwrap();
    let error = block_on(screens.snapshot(&first)).unwrap_err();
    assert_eq!(error.kind(), PortErrorKind::CapacityExceeded);
    assert!(!format!("{error:?}").contains("visible"));
}

#[test]
fn bounded_diff_reproduces_the_current_visible_screen_without_log_replay() {
    let output = output_store();
    let screens = screen_store(1, 64 * 1_024);
    let session = session("screen-diff");
    screens
        .begin_session(session.clone(), size(80, 24), OutputOffset::ZERO)
        .unwrap();
    screens
        .observe_output(&output, &session, b"prompt> ")
        .unwrap();
    let previous = snapshot(&screens, &session).unwrap();
    screens
        .observe_output(&output, &session, b"working\x1b[2;5Hstatus")
        .unwrap();
    let current = snapshot(&screens, &session).unwrap();

    let update = block_on(screens.diff(&session, &previous, &current)).unwrap();

    let TerminalScreenUpdate::Diff(diff) = update else {
        panic!("compatible screens unexpectedly required a snapshot")
    };
    assert_eq!(diff.from_output_offset(), previous.output_offset());
    assert_eq!(diff.current(), &current);
    let mut client = vt100::Parser::new(previous.rows(), previous.columns(), 0);
    client.process(previous.contents());
    client.process(diff.contents());
    assert_eq!(
        client.screen().contents(),
        parser_screen(&current).contents()
    );
    assert_eq!(
        client.screen().cursor_position(),
        parser_screen(&current).cursor_position()
    );
    assert_eq!(
        client.screen().hide_cursor(),
        parser_screen(&current).hide_cursor()
    );
}

#[test]
fn snapshots_and_diffs_restore_child_input_modes_without_replaying_startup() {
    let output = output_store();
    let screens = screen_store(1, 64 * 1024);
    let id = session("input-modes");
    screens
        .begin_session(id.clone(), size(80, 24), OutputOffset::ZERO)
        .unwrap();
    screens
        .observe_output(
            &output,
            &id,
            b"\x1b[?1049h\x1b[?1003h\x1b[?1006h\x1b[?2004h\x1b[?1hready",
        )
        .unwrap();
    let before = snapshot(&screens, &id).unwrap();
    let mut client = vt100::Parser::new(24, 80, 0);
    client.process(b"\x1b[?1049h");
    client.process(before.contents());
    assert_eq!(
        client.screen().mouse_protocol_mode(),
        vt100::MouseProtocolMode::AnyMotion
    );
    assert_eq!(
        client.screen().mouse_protocol_encoding(),
        vt100::MouseProtocolEncoding::Sgr
    );
    assert!(client.screen().bracketed_paste());
    assert!(client.screen().application_cursor());
    screens
        .observe_output(&output, &id, b"\x1b[?1003l\x1b[?1006l\x1b[?2004l\x1b[?1l")
        .unwrap();
    let after = snapshot(&screens, &id).unwrap();
    let TerminalScreenUpdate::Diff(diff) = block_on(screens.diff(&id, &before, &after)).unwrap()
    else {
        panic!("input-only change should be a diff");
    };
    client.process(diff.contents());
    assert_eq!(
        client.screen().mouse_protocol_mode(),
        vt100::MouseProtocolMode::None
    );
    assert_eq!(
        client.screen().mouse_protocol_encoding(),
        vt100::MouseProtocolEncoding::Default
    );
    assert!(!client.screen().bracketed_paste());
    assert!(!client.screen().application_cursor());
}

#[test]
fn snapshot_carries_bounded_normal_history_with_wraps_attributes_and_live_cursor() {
    let output = output_store();
    let screens = VtTerminalScreenStore::new(TerminalScreenLimits {
        sessions: 2,
        snapshot_bytes: 64 * 1024,
        scrollback_lines: 10,
    })
    .unwrap();
    let id = session("history");
    screens
        .begin_session(id.clone(), size(8, 3), OutputOffset::ZERO)
        .unwrap();
    let bytes =
        "\x1b[31mabcdef界wrapped\r\nsecond\r\nthird\r\nfourth\r\nfifth\r\nsixth\r\nlast".as_bytes();
    screens.observe_output(&output, &id, bytes).unwrap();
    let seed = snapshot(&screens, &id).unwrap();
    let mut expected = vt100::Parser::new(3, 8, 10);
    expected.process(bytes);
    let mut client = vt100::Parser::new(3, 8, 10);
    client.process(seed.contents());
    assert_eq!(
        client.screen().cursor_position(),
        expected.screen().cursor_position()
    );
    expected.set_scrollback(usize::MAX);
    client.set_scrollback(usize::MAX);
    assert_eq!(client.screen().scrollback(), expected.screen().scrollback());
    for offset in 0..=expected.screen().scrollback() {
        expected.set_scrollback(offset);
        client.set_scrollback(offset);
        for row in 0..3 {
            assert_eq!(
                client.screen().row_wrapped(row),
                expected.screen().row_wrapped(row),
                "offset {offset}, row {row}"
            );
            for column in 0..8 {
                assert_eq!(
                    client.screen().cell(row, column),
                    expected.screen().cell(row, column),
                    "offset {offset}, row {row}, column {column}"
                );
            }
        }
    }
    let restored_id = session("restored-history");
    screens
        .restore_snapshot_at(restored_id.clone(), &seed, OutputOffset::new(999))
        .unwrap();
    assert_eq!(
        snapshot(&screens, &restored_id).unwrap().contents(),
        seed.contents()
    );
    // Visiting history to build a snapshot must not move the live parser.
    assert_eq!(snapshot(&screens, &id).unwrap(), seed);
}

#[test]
fn history_pressure_drops_oldest_pages_without_truncating_the_viewport() {
    let output = output_store();
    let screens = VtTerminalScreenStore::new(TerminalScreenLimits {
        sessions: 1,
        snapshot_bytes: 240,
        scrollback_lines: 40,
    })
    .unwrap();
    let id = session("history-budget");
    screens
        .begin_session(id.clone(), size(8, 3), OutputOffset::ZERO)
        .unwrap();
    for line in 0..35 {
        screens
            .observe_output(&output, &id, format!("line {line}\r\n").as_bytes())
            .unwrap();
    }
    let seed = snapshot(&screens, &id).unwrap();
    assert!(seed.contents().len() <= 240);
    let mut client = vt100::Parser::new(3, 8, 40);
    client.process(seed.contents());
    assert!(client.screen().contents().contains("line 34"));
    client.set_scrollback(usize::MAX);
    assert!(client.screen().scrollback() > 0);
    assert!(client.screen().scrollback() < 33);
    assert!(!client.screen().contents().contains("line 0"));
    screens
        .observe_output(&output, &id, b"\x1b[?1049happ")
        .unwrap();
    let alternate = snapshot(&screens, &id).unwrap();
    assert!(alternate.alternate_screen());
    assert!(!alternate.contents().windows(5).any(|w| w == b"line "));
}

#[test]
fn soft_wraps_survive_history_page_boundaries() {
    for rows in [2, 3, 5] {
        let output = output_store();
        let screens = VtTerminalScreenStore::new(TerminalScreenLimits {
            sessions: 1,
            snapshot_bytes: 64 * 1024,
            scrollback_lines: 12,
        })
        .unwrap();
        let id = session("wrapped-pages");
        screens
            .begin_session(id.clone(), size(6, rows), OutputOffset::ZERO)
            .unwrap();
        let text = "ab界cd".repeat(35) + "x";
        screens
            .observe_output(&output, &id, text.as_bytes())
            .unwrap();
        let seed = snapshot(&screens, &id).unwrap();
        let mut expected = vt100::Parser::new(rows, 6, 12);
        expected.process(text.as_bytes());
        let mut actual = vt100::Parser::new(rows, 6, 12);
        actual.process(seed.contents());
        for offset in 0..=12 {
            expected.set_scrollback(offset);
            actual.set_scrollback(offset);
            assert_eq!(actual.screen().scrollback(), expected.screen().scrollback());
            for row in 0..rows {
                assert_eq!(
                    actual.screen().row_wrapped(row),
                    expected.screen().row_wrapped(row),
                    "height {rows}, offset {offset}, row {row}"
                );
                assert_eq!(
                    actual.screen().rows(0, 6).nth(usize::from(row)),
                    expected.screen().rows(0, 6).nth(usize::from(row))
                );
            }
        }
    }
}

#[test]
fn incompatible_geometry_or_buffer_returns_an_authoritative_snapshot() {
    let output = output_store();
    let screens = screen_store(1, 64 * 1_024);
    let session = session("screen-diff-fallback");
    screens
        .begin_session(session.clone(), size(80, 24), OutputOffset::ZERO)
        .unwrap();
    screens
        .observe_output(&output, &session, b"before")
        .unwrap();
    let before_resize = snapshot(&screens, &session).unwrap();
    screens.resize(&session, size(100, 30)).unwrap();
    let resized = snapshot(&screens, &session).unwrap();

    assert!(matches!(
        block_on(screens.diff(&session, &before_resize, &resized)).unwrap(),
        TerminalScreenUpdate::Snapshot(snapshot) if snapshot == resized
    ));

    screens
        .observe_output(&output, &session, b"\x1b[?1049hfull-screen")
        .unwrap();
    let alternate = snapshot(&screens, &session).unwrap();
    assert!(alternate.alternate_screen());
    assert!(matches!(
        block_on(screens.diff(&session, &resized, &alternate)).unwrap(),
        TerminalScreenUpdate::Snapshot(snapshot) if snapshot == alternate
    ));
}

#[test]
fn future_or_oversized_diff_checkpoints_are_rejected_without_payload_diagnostics() {
    let screens = screen_store(1, 16);
    let session = session("screen-diff-invalid");
    let current = TerminalScreenSnapshot::new(
        OutputOffset::new(4),
        80,
        24,
        0,
        0,
        true,
        false,
        Arc::from(b"now".as_slice()),
    )
    .unwrap();
    let future = TerminalScreenSnapshot::new(
        OutputOffset::new(5),
        80,
        24,
        0,
        0,
        true,
        false,
        Arc::from(b"private-future".as_slice()),
    )
    .unwrap();
    let error = block_on(screens.diff(&session, &future, &current)).unwrap_err();
    assert_eq!(error.kind(), PortErrorKind::InvalidData);
    assert!(!format!("{error:?}").contains("private-future"));

    let oversized = TerminalScreenSnapshot::new(
        OutputOffset::new(3),
        80,
        24,
        0,
        0,
        true,
        false,
        Arc::from(vec![b'x'; 17]),
    )
    .unwrap();
    let error = block_on(screens.diff(&session, &oversized, &current)).unwrap_err();
    assert_eq!(error.kind(), PortErrorKind::CapacityExceeded);
}

#[test]
fn profile_regions_share_one_checkpoint_and_preserve_cursor_anchor_semantics() {
    let output = output_store();
    let screens = screen_store(1, 64 * 1_024);
    let session = session("profile-samples");
    screens
        .begin_session(session.clone(), size(20, 6), OutputOffset::ZERO)
        .unwrap();
    screens
        .observe_output(
            &output,
            &session,
            b"\x1b[1;1Htop\x1b[3;1Hprompt> right-status\x1b[5;1Hfooter\x1b[3;9H",
        )
        .unwrap();
    let cursor_region = ProfileRegion::BottomRows {
        rows: NonZeroU16::new(1).unwrap(),
        anchor: ScreenAnchor::Cursor,
        columns: None,
    };
    let bottom_region = ProfileRegion::BottomRows {
        rows: NonZeroU16::new(2).unwrap(),
        anchor: ScreenAnchor::Any,
        columns: Some(NonZeroU16::new(4).unwrap()),
    };
    let below_last = ProfileRegion::BelowCursor {
        rows: NonZeroU16::new(2).unwrap(),
        columns: None,
    };
    let above_first = ProfileRegion::AboveCursor {
        rows: NonZeroU16::new(2).unwrap(),
        columns: None,
    };

    let samples = screens
        .profile_samples(
            &session,
            [
                None,
                Some(cursor_region.clone()),
                Some(bottom_region.clone()),
            ],
            3,
            1_024,
        )
        .unwrap();

    assert_eq!(
        samples.output_offset(),
        snapshot(&screens, &session).unwrap().output_offset()
    );
    assert_eq!(samples.samples().len(), 3);
    assert_eq!(
        samples.get(Some(&cursor_region)).unwrap().text(),
        "prompt> r"
    );
    assert_eq!(samples.get(Some(&bottom_region)).unwrap().text(), "foot\n");
    assert!(samples.get(None).unwrap().text().contains("right-status"));
    assert!(!format!("{samples:?}").contains("right-status"));

    screens
        .observe_output(&output, &session, b"\x1b[6;1H")
        .unwrap();
    let edge = screens
        .profile_samples(&session, [Some(below_last.clone())], 1, 1_024)
        .unwrap();
    assert_eq!(edge.get(Some(&below_last)).unwrap().text(), "");
    screens
        .observe_output(&output, &session, b"\x1b[1;1H")
        .unwrap();
    let edge = screens
        .profile_samples(&session, [Some(above_first.clone())], 1, 1_024)
        .unwrap();
    assert_eq!(edge.get(Some(&above_first)).unwrap().text(), "");
}

#[test]
fn profile_sample_limits_fail_without_exposing_screen_text() {
    let output = output_store();
    let screens = screen_store(1, 64 * 1_024);
    let session = session("profile-sample-limits");
    screens
        .begin_session(session.clone(), size(80, 24), OutputOffset::ZERO)
        .unwrap();
    screens
        .observe_output(&output, &session, b"private-screen-text")
        .unwrap();

    let error = screens
        .profile_samples(&session, [None], 1, 4)
        .expect_err("oversized sample must fail");

    assert_eq!(error.kind(), PortErrorKind::CapacityExceeded);
    assert!(!format!("{error:?}").contains("private-screen-text"));
}

#[test]
fn foreground_samples_apply_profile_motion_exclusions_at_one_checkpoint() {
    let output = output_store();
    let screens = screen_store(1, 64 * 1_024);
    let session = session("foreground-samples");
    screens
        .begin_session(session.clone(), size(20, 4), OutputOffset::ZERO)
        .unwrap();
    screens
        .observe_output(
            &output,
            &session,
            b"\x1b[1;1Hprivate-work-a\x1b[4;1Hclock-a",
        )
        .unwrap();
    let exclusions = OutputExclusions {
        top_rows: 0,
        bottom_rows: 1,
    };
    let initial = screens
        .foreground_samples(&session, [None], exclusions, 1, 1_024, 4)
        .unwrap();

    assert_eq!(initial.output_offset(), initial.motion().output_offset());
    assert!(!format!("{initial:?}").contains("private-work-a"));

    screens
        .observe_output(&output, &session, b"\x1b[4;1Hclock-b")
        .unwrap();
    let excluded_only = screens
        .foreground_samples(&session, [None], exclusions, 1, 1_024, 4)
        .unwrap();
    assert!(!excluded_only.motion().changed_since(initial.motion()));

    screens
        .observe_output(&output, &session, b"\x1b[1;1Hprivate-work-b")
        .unwrap();
    let meaningful = screens
        .foreground_samples(&session, [None], exclusions, 1, 1_024, 4)
        .unwrap();
    assert!(meaningful.motion().changed_since(excluded_only.motion()));
}

#[test]
fn foreground_motion_row_limit_is_typed_and_payload_free() {
    let output = output_store();
    let screens = screen_store(1, 64 * 1_024);
    let session = session("foreground-row-limit");
    screens
        .begin_session(session.clone(), size(20, 4), OutputOffset::ZERO)
        .unwrap();
    screens
        .observe_output(&output, &session, b"private-motion-content")
        .unwrap();

    let error = screens
        .foreground_samples(
            &session,
            std::iter::empty(),
            OutputExclusions::default(),
            1,
            1_024,
            3,
        )
        .expect_err("oversized motion screen must fail");

    assert_eq!(error.kind(), PortErrorKind::CapacityExceeded);
    assert!(!format!("{error:?}").contains("private-motion-content"));
}

#[test]
fn health_sample_keeps_text_and_red_cells_on_one_redacted_checkpoint() {
    let output = output_store();
    let screens = screen_store(1, 64 * 1_024);
    let session = session("health-screen");
    screens
        .begin_session(session.clone(), size(20, 4), OutputOffset::ZERO)
        .unwrap();
    screens
        .observe_output(
            &output,
            &session,
            b"\x1b[31mERR\x1b[0m normal \x1b[91mXX\x1b[0m",
        )
        .unwrap();

    let sample = screens.health_sample(&session, 1_024, 80).unwrap();

    assert_eq!(
        sample.output_offset(),
        snapshot(&screens, &session).unwrap().output_offset()
    );
    assert!(sample.text().contains("ERR normal XX"));
    assert_eq!(sample.red_cells(), 5);
    assert!(!format!("{sample:?}").contains("ERR normal XX"));
}

#[test]
fn health_sample_limits_fail_without_exposing_visible_text() {
    let output = output_store();
    let screens = screen_store(1, 64 * 1_024);
    let session = session("health-screen-limits");
    screens
        .begin_session(session.clone(), size(20, 4), OutputOffset::ZERO)
        .unwrap();
    screens
        .observe_output(&output, &session, b"private-health-screen")
        .unwrap();

    let cells = screens
        .health_sample(&session, 1_024, 79)
        .expect_err("cell capacity must fail");
    let bytes = screens
        .health_sample(&session, 4, 80)
        .expect_err("text capacity must fail");

    assert_eq!(cells.kind(), PortErrorKind::CapacityExceeded);
    assert_eq!(bytes.kind(), PortErrorKind::CapacityExceeded);
    assert!(!format!("{cells:?}").contains("private-health-screen"));
    assert!(!format!("{bytes:?}").contains("private-health-screen"));
}

fn output_store() -> TerminalOutputStore {
    TerminalOutputStore::new(TerminalOutputLimits {
        per_session_bytes: 64 * 1_024,
        global_bytes: 256 * 1_024,
        chunk_bytes: 1_024,
        append_bytes: 8 * 1_024,
        replay_bytes: 64 * 1_024,
        sessions: 8,
        subscriptions: 8,
    })
    .unwrap()
}

fn screen_store(sessions: usize, snapshot_bytes: usize) -> VtTerminalScreenStore {
    VtTerminalScreenStore::new(TerminalScreenLimits {
        scrollback_lines: 0,
        sessions,
        snapshot_bytes,
    })
    .unwrap()
}

fn snapshot(
    screens: &VtTerminalScreenStore,
    session: &SessionId,
) -> Option<undercurrent_application::TerminalScreenSnapshot> {
    block_on(screens.snapshot(session)).unwrap()
}

fn parser_screen(snapshot: &TerminalScreenSnapshot) -> vt100::Screen {
    let mut parser = vt100::Parser::new(snapshot.rows(), snapshot.columns(), 0);
    if snapshot.alternate_screen() {
        parser.process(b"\x1b[?1049h");
    }
    parser.process(snapshot.contents());
    parser.screen().clone()
}

fn session(value: &str) -> SessionId {
    SessionId::new(value).unwrap()
}

fn size(columns: u16, rows: u16) -> TerminalSize {
    TerminalSize::new(columns, rows, 400, 200).unwrap()
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn terminal_queries_reply_at_parse_position_and_remain_bounded() {
    let mut parser = vt100::Parser::new(24, 80, 0);
    for byte in b"\x1b[0c\x1b[5n\x1b[4;7H\x1b[6n" {
        parser.process(&[*byte]);
    }
    assert_eq!(parser.take_replies(), b"\x1b[?1;2c\x1b[0n\x1b[4;7R");
    assert!(parser.take_replies().is_empty());
    parser.process(b"\x1b[>0c\x1b[?1;2c\x1b[1c\x1b[5;6n");
    assert!(parser.take_replies().is_empty());
    parser.process(&b"\x1b[0c".repeat(10_000));
    let replies = parser.take_replies();
    assert!(!replies.is_empty());
    assert!(replies.len() <= 1024);
    assert!(replies.chunks(7).all(|reply| reply == b"\x1b[?1;2c"));
}
