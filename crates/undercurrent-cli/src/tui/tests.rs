// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::collections::BTreeMap;
use undercurrent_protocol::*;
use unicode_width::UnicodeWidthStr;

fn summary(id: &str, name: &str) -> SessionSummaryDto {
    SessionSummaryDto {
        id: id.into(),
        name: name.into(),
        lifecycle: LifecycleStateDto::Running,
        foreground: crate::test_foreground(ExecutionStateDto::Quiescent, InteractionStateDto::None),
        health: HealthSummaryDto {
            status: HealthStatusDto::NoCheck,
            observed_at_millis: None,
            configured_checks: 0,
        },
        work: WorkSummaryDto {
            revision: 0,
            total_items: 0,
            active_items: 0,
            total_targets: 0,
            active_targets: 0,
            aggregate_status: None,
        },
        profile: None,
        profile_context: Default::default(),
        labels: Some(BTreeMap::new()),
        started_at_millis: Some(1),
        updated_at_millis: 1,
        restart_count: 0,
        restart_status: RestartStatusDto::Idle,
        revision: 1,
        lifecycle_revision: 1,
    }
}

#[test]
fn refresh_reorders_without_retargeting_selection_or_marked_actions() {
    let mut app = App::default();
    app.apply_catalog(vec![summary("a", "api"), summary("b", "agent")], vec![]);
    app.move_selection(1);
    app.marked.insert("b".into());
    let targets = app.targets();
    app.apply_catalog(vec![summary("b", "renamed"), summary("c", "api")], vec![]);
    assert_eq!(app.selected.as_deref(), Some("b"));
    assert_eq!(app.targets()[0].id, "b");
    assert_eq!(targets[0].name, "agent");
    app.apply_catalog(vec![summary("c", "api")], vec![]);
    assert!(app.marked.is_empty());
    assert_eq!(app.selected.as_deref(), Some("c"));
    assert_eq!(
        targets[0].id, "b",
        "an open dialog retains its captured identity"
    );
}

#[test]
fn filtered_out_marks_remain_explicit_targets_and_labels_are_searchable() {
    let mut agent = summary("a", "agent");
    agent.labels = Some(BTreeMap::from([("task".into(), "UI".into())]));
    agent.foreground.interaction.state = InteractionStateDto::Requested;
    let mut app = App::default();
    app.apply_catalog(vec![agent, summary("b", "server")], vec![]);
    app.marked.insert("b".into());
    app.search = "agent task=ui".into();
    app.filter = Filter::NeedsInput;
    app.normalize_selection();
    assert_eq!(app.visible().len(), 1);
    assert_eq!(app.targets()[0].id, "b");
    assert_eq!(app.selected.as_deref(), Some("a"));
}

#[test]
fn frames_fit_tiny_narrow_and_unicode_terminals_without_child_escapes() {
    let mut app = App::default();
    let mut session = summary("a", "工程\u{1b}[31mworker");
    session.profile_context.current_directory = Some("/项目\n\r\u{1b}]52;c;secret\u{7}".into());
    app.apply_catalog(vec![session], vec![]);
    for (rows, columns) in [
        (1, 1),
        (8, 25),
        (12, 40),
        (14, 72),
        (15, 80),
        (24, 100),
        (50, 160),
    ] {
        let size = TerminalSizeDto { rows, columns };
        for modal in [false, true] {
            app.dialog = modal.then(|| Dialog::new_session("/项目", ""));
            let lines = view::lines(&app, size, "local");
            assert!(lines.len() <= usize::from(rows));
            assert!(
                lines.iter().all(|s| s.width() < usize::from(columns)),
                "{rows}x{columns}: {lines:?}"
            );
            assert!(lines.iter().all(|s| !s.chars().any(char::is_control)));
        }
    }
}

#[test]
fn quoted_arguments_preserve_windows_paths_empty_arguments_and_literal_shell_syntax() {
    assert_eq!(
        forms::words(r#"--name "hello world" '' C:\work\app '$HOME' $(id)"#).unwrap(),
        [
            "--name",
            "hello world",
            "",
            r"C:\work\app",
            "$HOME",
            "$(id)"
        ]
    );
    assert_eq!(
        forms::words(r#""C:\Program Files\app.exe" a\ b"#).unwrap(),
        [r"C:\Program Files\app.exe", "a b"]
    );
    assert!(forms::words("'unclosed").is_err());
}

#[test]
fn fields_edit_unicode_at_character_boundaries_and_confine_paste() {
    let mut field = Field::new("Name", "a界b");
    for key in [KeyDto::Left, KeyDto::Backspace] {
        field.input(&TerminalInputDto::Key { key });
    }
    assert_eq!(field.text, "ab");
    field.input(&TerminalInputDto::Paste {
        text: "\u{1b}\n\r界".into(),
    });
    assert_eq!(field.text, "a界b");
    field.input(&TerminalInputDto::Key {
        key: KeyDto::Delete,
    });
    assert_eq!(field.text, "a界");
    assert_eq!(field.display(true, 3), "界|");
    field.input(&TerminalInputDto::Key { key: KeyDto::Home });
    assert_eq!(field.display(true, 3), "|a");
}

#[test]
fn destructive_actions_require_explicit_confirmation_with_frozen_targets() {
    let mut app = App::default();
    app.apply_catalog(vec![summary("a", "api")], vec![]);
    let mut dialog = Dialog::form(Kind::Actions, "Actions", vec![], app.targets());
    dialog.choices = vec!["Stop".into()];
    assert!(
        finish_dialog(
            &mut app,
            &dialog,
            std::path::Path::new("/tmp"),
            TerminalSizeDto {
                rows: 24,
                columns: 80
            }
        )
        .unwrap()
        .is_none()
    );
    let mut confirm = app.dialog.take().unwrap();
    app.apply_catalog(vec![summary("b", "api")], vec![]);
    assert!(
        finish_dialog(
            &mut app,
            &confirm,
            std::path::Path::new("/tmp"),
            TerminalSizeDto {
                rows: 24,
                columns: 80
            }
        )
        .unwrap()
        .is_none()
    );
    confirm.active = 1;
    let Some(Action::Lifecycle { targets, .. }) = finish_dialog(
        &mut app,
        &confirm,
        std::path::Path::new("/tmp"),
        TerminalSizeDto {
            rows: 24,
            columns: 80,
        },
    )
    .unwrap() else {
        panic!("confirmed stop")
    };
    assert_eq!(targets[0].id, "a");
}

#[test]
fn label_editor_protects_recovery_keys_and_rejects_ambiguous_patches() {
    let mut app = App::default();
    let size = TerminalSizeDto {
        rows: 24,
        columns: 80,
    };
    for (set, remove) in [
        ("uc.group.slot=bad", ""),
        ("", "uc.tui.creation"),
        ("task=ui", "task"),
    ] {
        let dialog = Dialog::form(
            Kind::Labels,
            "Labels",
            vec![Field::new("Set", set), Field::new("Remove", remove)],
            vec![],
        );
        assert!(finish_dialog(&mut app, &dialog, std::path::Path::new("/tmp"), size).is_err());
    }
}

#[test]
fn workspace_alias_and_tty_validation_reject_before_connecting() {
    for name in ["tui", "workspace"] {
        let parsed = NativeCliArguments::try_parse_from(["uc", name]).unwrap();
        assert!(matches!(parsed.command, NativeCliCommand::Tui));
        for (stdin, stdout) in [(false, true), (true, false)] {
            let context =
                crate::CliInvocationContext::from_options(parsed.global_options()).unwrap();
            let result = crate::execute_native_cli(
                &parsed,
                &context,
                crate::CliTerminalAvailability::new(stdin, stdout),
                &mut std::io::Cursor::new(vec![]),
                &mut vec![],
                &mut vec![],
                &crate::DisabledCliDiagnosticSink,
            );
            assert!(matches!(
                result,
                Err(CliError::Input(
                    crate::CliInputError::InteractiveTerminalRequired { .. }
                ))
            ));
        }
        for mode in ["--json", "--jsonl"] {
            let parsed = NativeCliArguments::try_parse_from(["uc", mode, name]).unwrap();
            let context =
                crate::CliInvocationContext::from_options(parsed.global_options()).unwrap();
            let result = crate::execute_native_cli(
                &parsed,
                &context,
                crate::CliTerminalAvailability::new(true, true),
                &mut std::io::Cursor::new(vec![]),
                &mut vec![],
                &mut vec![],
                &crate::DisabledCliDiagnosticSink,
            );
            assert!(matches!(result, Err(CliError::RawOutputRequiresText)));
        }
    }
}

struct MockClient {
    replies: std::sync::Mutex<
        std::collections::VecDeque<Result<ProductSuccess, undercurrent_sdk::ProductClientError>>,
    >,
    requests: std::sync::Mutex<Vec<ProductRequest>>,
}

impl MockClient {
    fn new(replies: Vec<ProductSuccess>) -> Self {
        Self {
            replies: std::sync::Mutex::new(replies.into_iter().map(Ok).collect()),
            requests: std::sync::Mutex::new(vec![]),
        }
    }
}

impl crate::CliProductRequestPort for MockClient {
    fn request(
        &self,
        request: ProductRequest,
        _: Duration,
    ) -> Result<ProductSuccess, undercurrent_sdk::ProductClientError> {
        self.requests.lock().unwrap().push(request);
        self.replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("only expected requests are sent")
    }
}

struct NoSubscription;
impl crate::CliProductSubscription for NoSubscription {
    fn recv(
        &self,
    ) -> Result<undercurrent_sdk::ProductSubscriptionItem, undercurrent_sdk::ProductClientError>
    {
        panic!("read-only catalog and preview")
    }
    fn recv_timeout(
        &self,
        _: Duration,
    ) -> Result<undercurrent_sdk::ProductSubscriptionItem, undercurrent_sdk::ProductClientError>
    {
        panic!("read-only catalog and preview")
    }
    fn cancel(&self) {}
    fn close(self) {}
}
impl crate::CliProductStreamPort for MockClient {
    type Subscription = NoSubscription;
    fn open_product_subscription(
        &self,
        _: ProductStreamSpec,
        _: Duration,
    ) -> Result<crate::OpenedCliSubscription<NoSubscription>, undercurrent_sdk::ProductClientError>
    {
        panic!("read-only catalog and preview")
    }
}

fn config(root: &std::path::Path) -> ResolvedCliConfig {
    crate::load_cli_config(&crate::ConfigDiscovery::from_path_env(
        crate::PathEnvironment::default().with_undercurrent_home(root),
    ))
    .unwrap()
}

fn worker<'a>(client: &'a MockClient, config: &'a ResolvedCliConfig) -> Worker<'a, MockClient> {
    Worker {
        client,
        config,
        limits: CliExecutionLimits::from_daemon(config).unwrap(),
        timeout: Duration::from_secs(1),
        cwd: config.product_home().to_owned(),
        cancellation: CliStreamCancellation::default(),
    }
}

fn page(id: &str, cursor: Option<&str>) -> ProductSuccess {
    ProductSuccess::ListSessions(SessionPageDto {
        items: vec![summary(id, id)],
        next_cursor: cursor.map(str::to_owned),
        snapshot_revision: 1,
        total: None,
    })
}

#[test]
fn catalog_collects_all_pages_and_rejects_repeated_cursors_without_publishing_partial_rows() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let client = MockClient::new(vec![page("a", Some("next")), page("b", None)]);
    let catalog = worker(&client, &config).catalog().unwrap();
    assert_eq!(
        catalog
            .sessions
            .iter()
            .map(|s| s.id.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    let requests = client.requests.lock().unwrap();
    let ProductRequest::ListSessions(first) = &requests[0] else {
        panic!("list")
    };
    let ProductRequest::ListSessions(second) = &requests[1] else {
        panic!("list")
    };
    assert!(first.cursor.is_none());
    assert_eq!(first.projection, SessionProjectionDto::WithLabels);
    assert_eq!(second.cursor.as_deref(), Some("next"));
    drop(requests);
    let repeated = MockClient::new(vec![page("a", Some("next")), page("b", Some("next"))]);
    assert!(worker(&repeated, &config).catalog().is_err());
    assert_eq!(repeated.requests.lock().unwrap().len(), 2);
    let duplicate = MockClient::new(vec![page("a", Some("next")), page("a", None)]);
    assert!(worker(&duplicate, &config).catalog().is_err());
}

#[test]
fn preview_only_reads_and_confines_terminal_control_sequences() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let client = MockClient::new(vec![ProductSuccess::ReadTerminalScreen(
        TerminalScreenSnapshotDto {
            checkpoint: "checkpoint".into(),
            output_offset: 10,
            columns: 80,
            rows: 24,
            cursor_row: 0,
            cursor_column: 5,
            cursor_visible: true,
            alternate_screen: false,
            contents: b"\x1b]52;c;secret\x07\x1b[31mready".to_vec(),
        },
    )]);
    let Reply::Preview(preview) = worker(&client, &config).execute(Job::Preview {
        id: "a".into(),
        tab: Tab::Screen,
    }) else {
        panic!("preview")
    };
    assert_eq!(preview.lines, ["ready"]);
    let requests = client.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(
        matches!(&requests[0], ProductRequest::ReadTerminalScreen(request) if request.session_id == "a")
    );
}

#[test]
fn interrupted_worker_does_not_submit_more_requests() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let client = MockClient::new(vec![]);
    let worker = worker(&client, &config);
    worker.cancellation.cancel_local();
    assert!(worker.catalog().is_err());
    assert!(client.requests.lock().unwrap().is_empty());
}

#[test]
fn catalog_restarts_read_only_snapshot_after_connection_loss_and_discards_old_pages() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let client = MockClient {
        replies: std::sync::Mutex::new(
            [
                Ok(page("old", Some("old-cursor"))),
                Err(undercurrent_sdk::ProductClientError::Client(
                    undercurrent_sdk::ResilientClientError::Recovering,
                )),
                Ok(page("new", Some("new-cursor"))),
                Ok(page("last", None)),
            ]
            .into(),
        ),
        requests: Default::default(),
    };
    let catalog = worker(&client, &config).catalog().unwrap();
    assert_eq!(
        catalog
            .sessions
            .iter()
            .map(|s| s.id.as_str())
            .collect::<Vec<_>>(),
        ["new", "last"]
    );
    let requests = client.requests.lock().unwrap();
    let ProductRequest::ListSessions(reopened) = &requests[2] else {
        panic!("read-only recovery")
    };
    assert!(reopened.cursor.is_none());
    assert!(
        requests
            .iter()
            .all(|r| matches!(r, ProductRequest::ListSessions(_)))
    );
}
