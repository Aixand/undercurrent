// SPDX-License-Identifier: Apache-2.0

//! Pure local translation from parsed arguments to typed product requests.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::duration::{DurationParser, DurationUnits};
use undercurrent_protocol::{
    CommandSpecDto, CreateDiagnosticBundleRequest, DiagnosticBundleTerminalContentRequestDto,
    DiagnosticSectionDto, EnvironmentSpecDto, ExecutionEnvironmentBindingDto, ExecutionPolicyDto,
    ExecutionStateDto, GetDiagnosticsRequest, HealthRuleDto, HealthRulesDto, HealthStatusDto,
    InputObservationPolicyDto, InteractionStateDto, KeyDto, KeyModifiersDto, LifecycleKindDto,
    LintProfilesRequest, ListProfilesRequest, ListSessionsRequest,
    MAX_DIAGNOSTIC_BUNDLE_TERMINAL_CONTENT_BYTES, MatchProfileRequest, ModifiableKeyDto,
    MutationContext, OutputCaptureModeDto, ProductStreamSpec, ProfileBindingModeDto,
    RestartPolicyDto, SendTerminalInputRequest, SessionFilterDto, SessionProjectionDto,
    SessionSortDto, SessionSortFieldDto, SessionWatchSpec, ShutdownDaemonRequest, SortDirectionDto,
    StartSessionRequest, StopPolicyDto, StopSignalDto, TerminalInitialModeDto, TerminalInputDto,
    TerminalOutputSpec, TerminalSizeDto, TestProfileRequest,
    standalone_environment_binding_digest_sha256,
};

use crate::{
    CleanArguments, CliInputError, CliInputField, DebugAction, DebugArguments,
    DiagnosticSectionArgument, ExecutionArgument, HealthArgument, InteractionArgument,
    LifecycleArgument, ListArguments, ProfileListArguments, ProfileMatchArguments,
    ProfileModeArgument, ProfileTestArguments, RestartModeArgument, SendArguments,
    SessionProjectionArgument, SessionSortArgument, StartArguments, StopArguments,
    StopSignalArgument, TopArguments, WatchArguments,
};

const MAX_POLICY_DURATION: Duration = Duration::from_secs(24 * 60 * 60);

#[cfg(windows)]
const FALLBACK_SHELL: &str = "powershell.exe";
#[cfg(not(windows))]
const FALLBACK_SHELL: &str = "/bin/sh";

#[cfg(not(windows))]
pub(crate) fn native_default_shell() -> String {
    let variable = "SHELL";
    std::env::var(variable)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| FALLBACK_SHELL.to_owned())
}

#[cfg(windows)]
pub(crate) fn native_default_shell() -> String {
    let path = std::env::var_os("PATH").unwrap_or_default();
    let windows = undercurrent_platform_windows::windows_directory().ok();
    windows_default_shell(&path, windows.as_deref())
}

#[cfg(windows)]
fn windows_default_shell(path: &std::ffi::OsStr, windows: Option<&Path>) -> String {
    // Prefer a real PowerShell 7 executable on PATH. Discovery follows the same
    // path restrictions as native launch, so Store aliases and junctions cannot
    // hide a usable later entry. Runtime preflight pins the selected file again.
    for directory in std::env::split_paths(path).filter(|entry| entry.is_absolute()) {
        let candidate = directory.join("pwsh.exe");
        if undercurrent_platform_windows::PinnedPath::launch_file(&candidate).is_ok() {
            return candidate.to_string_lossy().into_owned();
        }
    }
    windows
        .map(|root| root.join("System32/WindowsPowerShell/v1.0/powershell.exe"))
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|| FALLBACK_SHELL.to_owned())
}

/// Prepared exact-name or paged-query session selection for a CLI command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreparedSessionSelection {
    /// Resolve and mutate one exact human-facing name.
    ExactName(String),
    /// Collect stable IDs from one server-side filtered query snapshot.
    Query(SessionFilterDto),
}

/// Outer process values needed to prepare a start request without performing IO.
pub struct StartPreparationContext {
    current_directory: PathBuf,
    default_shell: Option<String>,
}

impl StartPreparationContext {
    /// Captures the caller's absolute current directory and optional shell.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed input failure when `current_directory` is relative.
    pub fn new(
        current_directory: impl Into<PathBuf>,
        default_shell: Option<String>,
    ) -> Result<Self, CliInputError> {
        let current_directory = current_directory.into();
        if !current_directory.is_absolute() {
            return Err(CliInputError::RelativeCurrentDirectory);
        }
        Ok(Self {
            current_directory,
            default_shell,
        })
    }
}

/// Prepares one complete typed start request before the start mutation.
///
/// Explicit environment values are retained only in the returned protocol DTO
/// and are never copied into diagnostics. Relative filesystem references are
/// anchored to the caller's current directory because the daemon may have a
/// different process working directory. This function performs no IO and does
/// not panic. Native routing calls it after connecting; only the interactive
/// command conflict is checked before configuration and daemon IO.
///
/// # Errors
///
/// Returns a typed local validation failure for command, key/value, duration,
/// path, restart-policy, terminal-size, or health-rule input.
pub fn prepare_start_request(
    arguments: &StartArguments,
    context: &StartPreparationContext,
    mutation: MutationContext,
) -> Result<StartSessionRequest, CliInputError> {
    validate_start_arguments(arguments)?;
    let command = command(arguments, context)?;
    let variables = key_values(&arguments.environment, CliInputField::Environment)?;
    let labels = key_values(&arguments.label, CliInputField::Label)?;
    let metadata = key_values(&arguments.metadata, CliInputField::Metadata)?;
    let working_directory = match arguments.cwd.as_deref() {
        Some(path) => absolute_path(
            path,
            &context.current_directory,
            CliInputField::WorkingDirectory,
        )?,
        None => utf8_path(&context.current_directory, CliInputField::CurrentDirectory)?,
    };
    let environment_file = arguments
        .env_file
        .as_deref()
        .map(|path| {
            absolute_path(
                Path::new(path),
                &context.current_directory,
                CliInputField::EnvironmentFile,
            )
        })
        .transpose()?;
    if arguments.columns == 0 || arguments.rows == 0 {
        return Err(CliInputError::InvalidTerminalSize);
    }
    let restart = restart_policy(arguments)?;
    let stop_timeout_millis =
        duration_millis(&arguments.stop_timeout, CliInputField::StopTimeout, false)?;
    let health = arguments
        .health
        .iter()
        .enumerate()
        .map(|(index, rule)| health_rule(rule, index))
        .collect::<Result<Vec<_>, _>>()?;
    // Auto follows foreground ownership so a wrapper's child can select the
    // application profile. An explicit profile keeps profile selection fixed unless
    // foreground mode is requested; disabled detection always stays fixed.
    // See [profile defaults](../../../docs/architecture/decisions/0002-cli-foreground-default.md#decision).
    let profile_binding = if arguments.no_detect || arguments.profile.is_some() {
        match arguments.profile_mode {
            ProfileModeArgument::Foreground if !arguments.no_detect => {
                ProfileBindingModeDto::ForegroundAdaptive
            }
            _ => ProfileBindingModeDto::Fixed,
        }
    } else {
        match arguments.profile_mode {
            ProfileModeArgument::Auto | ProfileModeArgument::Foreground => {
                ProfileBindingModeDto::ForegroundAdaptive
            }
            ProfileModeArgument::Fixed => ProfileBindingModeDto::Fixed,
        }
    };

    Ok(StartSessionRequest {
        launch_mode: match arguments.launch_mode {
            crate::LaunchModeArgument::Native => undercurrent_protocol::LaunchModeDto::Native,
            crate::LaunchModeArgument::Sealed => undercurrent_protocol::LaunchModeDto::Sealed,
        },
        mutation,
        dispatch: None,
        name: arguments.name.clone(),
        command,
        working_directory: Some(working_directory.clone()),
        environment: EnvironmentSpecDto {
            schema_version: undercurrent_protocol::EnvironmentSchemaVersionDto::Current,
            variables,
            secret_variables: BTreeMap::new(),
            file: environment_file,
            binding: Some(ExecutionEnvironmentBindingDto {
                binding_digest_sha256: standalone_environment_binding_digest_sha256(
                    &working_directory,
                ),
                working_directory_root: working_directory,
            }),
        },
        terminal: TerminalSizeDto {
            columns: arguments.columns,
            rows: arguments.rows,
        },
        execution: ExecutionPolicyDto {
            restart,
            stop: StopPolicyDto {
                signal: stop_signal(arguments.stop_signal.unwrap_or(if arguments.interactive {
                    StopSignalArgument::Hangup
                } else {
                    StopSignalArgument::Terminate
                })),
                timeout_millis: stop_timeout_millis,
            },
            detection_enabled: !arguments.no_detect,
            auto_remove: arguments.auto_remove,
        },
        health: HealthRulesDto { rules: health },
        profile: arguments.profile.clone(),
        profile_binding,
        labels,
        metadata,
        output_capture_mode: match arguments.output_capture {
            crate::OutputCaptureModeArgument::Terminal => OutputCaptureModeDto::Terminal,
            crate::OutputCaptureModeArgument::Opaque => OutputCaptureModeDto::Opaque,
        },
    })
}

/// Validates start argument relationships before any terminal or daemon IO.
///
/// The rejected command payload is neither retained nor included in the
/// returned error. This function performs no IO and does not panic.
///
/// # Errors
///
/// Returns a typed conflict when persistent interactive-shell mode is combined
/// with a direct command.
pub fn validate_start_arguments(arguments: &StartArguments) -> Result<(), CliInputError> {
    if arguments.interactive && !arguments.command.is_empty() {
        return Err(CliInputError::InteractiveCommandConflict);
    }
    Ok(())
}

/// Converts every `ls` selection into a bounded server-side prepared query.
///
/// This function performs no IO, does not locally filter results, and does not
/// panic.
///
/// # Errors
///
/// Returns a typed key/value failure for malformed label selectors. Remaining
/// domain bounds are authoritative at the daemon boundary.
pub fn prepare_list_request(
    arguments: &ListArguments,
) -> Result<ListSessionsRequest, CliInputError> {
    if arguments.page_size.unwrap_or(100) == 0 {
        return Err(CliInputError::InvalidPageSize);
    }
    Ok(ListSessionsRequest {
        filter: SessionFilterDto {
            ids: arguments.ids.clone(),
            name: arguments.name.clone(),
            name_contains: arguments.name_contains.clone(),
            lifecycles: arguments.lifecycle.iter().copied().map(lifecycle).collect(),
            executions: arguments.execution.iter().copied().map(execution).collect(),
            interactions: arguments
                .interaction
                .iter()
                .copied()
                .map(interaction)
                .collect(),
            health: arguments.health.iter().copied().map(health).collect(),
            profile: arguments.profile.clone(),
            labels: key_values(&arguments.label, CliInputField::Label)?,
            has_active_work: arguments.has_active_work,
        },
        sort: SessionSortDto {
            field: session_sort(arguments.sort),
            direction: if arguments.descending {
                SortDirectionDto::Descending
            } else {
                SortDirectionDto::Ascending
            },
        },
        projection: projection(arguments.projection),
        page_size: arguments.page_size.unwrap_or(100),
        cursor: arguments.cursor.clone(),
        include_total: arguments.total,
    })
}

/// Builds the one-row indexed lookup used to resolve a human-facing name.
///
/// This function performs no IO and does not panic. The daemon validates the
/// name through the same typed boundary as every prepared query.
pub fn exact_name_lookup(name: impl Into<String>) -> ListSessionsRequest {
    ListSessionsRequest {
        filter: SessionFilterDto {
            name: Some(name.into()),
            ..SessionFilterDto::default()
        },
        sort: SessionSortDto::default(),
        projection: SessionProjectionDto::Compact,
        page_size: 1,
        cursor: None,
        include_total: false,
    }
}

/// Builds the one-row indexed lookup used to resynchronize a stable session ID.
///
/// This function performs no IO and does not panic.
pub fn exact_id_lookup(session_id: impl Into<String>) -> ListSessionsRequest {
    ListSessionsRequest {
        filter: SessionFilterDto {
            ids: vec![session_id.into()],
            ..SessionFilterDto::default()
        },
        sort: SessionSortDto::default(),
        projection: SessionProjectionDto::Compact,
        page_size: 1,
        cursor: None,
        include_total: false,
    }
}

/// Prepares the explicit selection accepted by `stop`.
///
/// Label selection stays server-side and values are never included in errors.
/// This function performs no IO and does not panic.
///
/// # Errors
///
/// Returns a typed selection or label-shape failure.
pub fn prepare_stop_selection(
    arguments: &StopArguments,
) -> Result<PreparedSessionSelection, CliInputError> {
    if let Some(name) = &arguments.name {
        return Ok(PreparedSessionSelection::ExactName(name.clone()));
    }
    if arguments.all {
        return Ok(PreparedSessionSelection::Query(SessionFilterDto::default()));
    }
    if let Some(label) = &arguments.label {
        return Ok(PreparedSessionSelection::Query(SessionFilterDto {
            labels: key_values(std::slice::from_ref(label), CliInputField::Label)?,
            ..SessionFilterDto::default()
        }));
    }
    Err(CliInputError::MissingSessionSelection)
}

/// Prepares exact-name or terminal-session selection accepted by `clean`.
///
/// Omitting the name (with or without `--all`) requests only terminal lifecycle
/// summaries so active sessions are not sent to destructive mutation endpoints.
/// This function performs no IO and does not panic.
///
/// # Errors
///
/// This selection currently has no failure cases.
pub fn prepare_clean_selection(
    arguments: &CleanArguments,
) -> Result<PreparedSessionSelection, CliInputError> {
    if let Some(name) = &arguments.name {
        return Ok(PreparedSessionSelection::ExactName(name.clone()));
    }
    Ok(PreparedSessionSelection::Query(SessionFilterDto {
        lifecycles: vec![
            LifecycleKindDto::Stopped,
            LifecycleKindDto::Exited,
            LifecycleKindDto::Failed,
        ],
        ..SessionFilterDto::default()
    }))
}

/// Prepares the one bounded query stream used by the selective dashboard.
///
/// This function performs no IO and does not panic.
///
/// # Errors
///
/// Returns a typed local failure for a zero page size.
pub fn prepare_top_stream(arguments: &TopArguments) -> Result<ProductStreamSpec, CliInputError> {
    require_page_size(arguments.page_size.unwrap_or(100))?;
    Ok(ProductStreamSpec::SessionWatch(SessionWatchSpec {
        query: ListSessionsRequest {
            filter: SessionFilterDto::default(),
            sort: SessionSortDto::default(),
            projection: projection(arguments.projection),
            page_size: arguments.page_size.unwrap_or(100),
            cursor: None,
            include_total: true,
        },
    }))
}

/// Prepares one bounded watched page after exact names have been resolved.
///
/// Empty `session_ids` selects the first unfiltered page. Non-empty IDs must
/// all fit within the requested page so an explicit named selection is never
/// silently truncated. This function performs no IO and does not panic.
///
/// # Errors
///
/// Returns a typed page-size or explicit-selection failure.
pub fn prepare_watch_stream(
    arguments: &WatchArguments,
    session_ids: Vec<String>,
) -> Result<ProductStreamSpec, CliInputError> {
    require_page_size(arguments.page_size.unwrap_or(100))?;
    if session_ids.len() > arguments.page_size.unwrap_or(100) as usize {
        return Err(CliInputError::SelectionExceedsPage {
            selected: session_ids.len(),
            page_size: arguments.page_size.unwrap_or(100),
        });
    }
    Ok(ProductStreamSpec::SessionWatch(SessionWatchSpec {
        query: ListSessionsRequest {
            filter: SessionFilterDto {
                ids: session_ids,
                ..SessionFilterDto::default()
            },
            sort: SessionSortDto::default(),
            projection: projection(arguments.projection),
            page_size: arguments.page_size.unwrap_or(100),
            cursor: None,
            include_total: false,
        },
    }))
}

/// Prepares an exact one-row watch used by the wait use case.
///
/// This function performs no IO and does not panic.
pub fn prepare_wait_stream(session_id: impl Into<String>) -> ProductStreamSpec {
    ProductStreamSpec::SessionWatch(SessionWatchSpec {
        query: ListSessionsRequest {
            filter: SessionFilterDto {
                ids: vec![session_id.into()],
                ..SessionFilterDto::default()
            },
            sort: SessionSortDto::default(),
            projection: SessionProjectionDto::Compact,
            page_size: 1,
            cursor: None,
            include_total: false,
        },
    })
}

/// Prepares one selective binary terminal-output logical stream.
///
/// This function performs no IO and does not panic.
pub fn prepare_terminal_stream(
    session_id: impl Into<String>,
    initial: TerminalInitialModeDto,
) -> ProductStreamSpec {
    ProductStreamSpec::TerminalOutput(TerminalOutputSpec {
        session_id: session_id.into(),
        initial,
    })
}

/// Prepares one atomic typed terminal input request for a resolved session.
///
/// `stdin_text` is used only when no positional text was supplied. Input
/// content is moved into the request and never included in errors or
/// diagnostics. This function performs no IO and does not panic.
///
/// # Errors
///
/// Returns a payload-safe error when no items were supplied or one named key
/// is unsupported.
pub fn prepare_terminal_input_request(
    arguments: &SendArguments,
    session_id: impl Into<String>,
    stdin_text: Option<String>,
    mutation: MutationContext,
) -> Result<SendTerminalInputRequest, CliInputError> {
    let mut items = Vec::new();
    let mut append_newline = false;
    if let Some(text) = arguments.input.clone().or(stdin_text)
        && !text.is_empty()
    {
        items.push(if arguments.paste {
            TerminalInputDto::Paste { text }
        } else if arguments.raw {
            TerminalInputDto::Text { text }
        } else {
            TerminalInputDto::AutoText { text }
        });
        append_newline = arguments.newline && !arguments.no_newline;
    }
    items.extend(
        arguments
            .key
            .iter()
            .enumerate()
            .map(|(index, key)| parse_key(key, index).map(|key| TerminalInputDto::Key { key }))
            .collect::<Result<Vec<_>, _>>()?,
    );
    if append_newline {
        items.push(TerminalInputDto::Key { key: KeyDto::Enter });
    }
    if items.is_empty() {
        return Err(CliInputError::MissingTerminalInput);
    }
    Ok(SendTerminalInputRequest {
        expected_revision: None,
        mutation,
        session_id: session_id.into(),
        delivery_id: arguments.delivery_id.clone(),
        items,
        observation_policy: if arguments.preserve_observation {
            InputObservationPolicyDto::Preserve
        } else {
            InputObservationPolicyDto::ForceActive
        },
    })
}

/// Prepares one bounded profile-summary page request.
///
/// # Errors
///
/// Returns a typed local failure for a zero page size. This function performs
/// no IO and does not panic.
pub fn prepare_profile_list_request(
    arguments: &ProfileListArguments,
) -> Result<ListProfilesRequest, CliInputError> {
    require_page_size(arguments.page_size)?;
    Ok(ListProfilesRequest {
        page_size: arguments.page_size,
        cursor: arguments.cursor.clone(),
    })
}

/// Prepares safe process facts for deterministic profile selection.
///
/// This function performs no IO and does not panic. Environment values are not
/// accepted by the parser or this request.
pub fn prepare_profile_match_request(arguments: &ProfileMatchArguments) -> MatchProfileRequest {
    MatchProfileRequest {
        executable: arguments.executable.clone(),
        arguments: arguments.arguments.clone(),
        environment_names: arguments.environment_names.clone(),
    }
}

/// Prepares one bounded standalone profile sample evaluation.
///
/// Sample text is moved only into the request and the returned type has no
/// payload-bearing `Debug` implementation. This function performs no IO and
/// does not panic.
pub fn prepare_profile_test_request(arguments: &ProfileTestArguments) -> TestProfileRequest {
    TestProfileRequest {
        name: arguments.name.clone(),
        screen_text: arguments.screen_text.clone(),
        last_line: arguments.last_line.clone(),
        cursor_at_end: !arguments.cursor_not_at_end,
    }
}

/// Prepares one bounded typed diagnostic page selection.
///
/// # Errors
///
/// Returns a typed local failure for a zero page size. This function performs
/// no IO and does not panic.
pub fn prepare_diagnostics_request(
    arguments: &DebugArguments,
) -> Result<GetDiagnosticsRequest, CliInputError> {
    if arguments.action.is_some() {
        return Err(CliInputError::InvalidArguments);
    }
    require_page_size(arguments.page_size)?;
    Ok(GetDiagnosticsRequest {
        sections: vec![diagnostic_section(
            arguments.section.ok_or(CliInputError::InvalidArguments)?,
        )],
        page_size: arguments.page_size,
        cursor: arguments.cursor.clone(),
    })
}

/// Prepares one bounded privacy-safe standalone diagnostic bundle request.
///
/// # Errors
///
/// Returns a typed local failure for invalid diagnostic or terminal-content
/// bounds. This function performs no IO and does not panic.
pub fn prepare_diagnostic_bundle_request(
    action: &DebugAction,
) -> Result<CreateDiagnosticBundleRequest, CliInputError> {
    let DebugAction::Bundle {
        include_terminal_content,
        from_offset,
        max_bytes,
        page_size,
        cursor,
    } = action
    else {
        return Err(CliInputError::InvalidArguments);
    };
    require_page_size(*page_size)?;
    let terminal_content = include_terminal_content.as_ref().map(|session_id| {
        DiagnosticBundleTerminalContentRequestDto {
            session_id: session_id.clone(),
            from_offset: from_offset.unwrap_or(0),
            max_bytes: max_bytes.unwrap_or(MAX_DIAGNOSTIC_BUNDLE_TERMINAL_CONTENT_BYTES),
        }
    });
    let request = CreateDiagnosticBundleRequest {
        sections: DiagnosticSectionDto::ALL.to_vec(),
        page_size: *page_size,
        cursor: cursor.clone(),
        terminal_content,
    };
    request
        .validate()
        .map_err(|_| CliInputError::InvalidArguments)?;
    Ok(request)
}

/// Prepares one idempotent bounded graceful daemon-shutdown request.
///
/// # Errors
///
/// Returns a typed shared-duration failure for malformed or zero drain time.
/// This function performs no IO and does not panic.
pub fn prepare_shutdown_request(
    drain_timeout: &str,
    mutation: MutationContext,
) -> Result<ShutdownDaemonRequest, CliInputError> {
    Ok(ShutdownDaemonRequest {
        mode: Default::default(),
        mutation,
        drain_timeout_millis: duration_millis(drain_timeout, CliInputField::DrainTimeout, false)?,
    })
}

/// Prepares an optional exact profile lint scope.
///
/// This function performs no IO and does not panic.
pub fn prepare_profile_lint_request(name: Option<&str>) -> LintProfilesRequest {
    LintProfilesRequest {
        name: name.map(str::to_owned),
    }
}

fn command(
    arguments: &StartArguments,
    context: &StartPreparationContext,
) -> Result<CommandSpecDto, CliInputError> {
    let selected = if arguments.command.is_empty() {
        if !arguments.interactive {
            return Err(CliInputError::MissingCommand);
        }
        vec![
            arguments
                .shell
                .clone()
                .or_else(|| context.default_shell.clone())
                .unwrap_or_else(|| FALLBACK_SHELL.to_owned()),
        ]
    } else {
        arguments.command.clone()
    };
    let mut selected = selected.into_iter();
    let program = selected.next().ok_or(CliInputError::MissingCommand)?;
    Ok(CommandSpecDto {
        program,
        arguments: selected.collect(),
    })
}

fn restart_policy(arguments: &StartArguments) -> Result<RestartPolicyDto, CliInputError> {
    if arguments.restart == RestartModeArgument::Never {
        return Ok(RestartPolicyDto::Never);
    }
    if arguments.backoff_basis_points < 10_000 {
        return Err(CliInputError::InvalidRestartBackoff);
    }
    let initial_delay_millis =
        duration_millis(&arguments.restart_delay, CliInputField::RestartDelay, true)?;
    let max_delay_millis = duration_millis(
        &arguments.max_restart_delay,
        CliInputField::MaximumRestartDelay,
        true,
    )?;
    if max_delay_millis < initial_delay_millis {
        return Err(CliInputError::InvalidRestartDelayOrder);
    }
    let min_uptime_millis =
        duration_millis(&arguments.min_uptime, CliInputField::MinimumUptime, true)?;
    let values = (
        arguments.max_restarts,
        initial_delay_millis,
        max_delay_millis,
        arguments.backoff_basis_points,
        min_uptime_millis,
    );
    Ok(match arguments.restart {
        RestartModeArgument::Never => RestartPolicyDto::Never,
        RestartModeArgument::OnFailure => RestartPolicyDto::OnFailure {
            max_attempts: values.0,
            initial_delay_millis: values.1,
            max_delay_millis: values.2,
            backoff_basis_points: values.3,
            min_uptime_millis: values.4,
        },
        RestartModeArgument::Always => RestartPolicyDto::Always {
            max_attempts: values.0,
            initial_delay_millis: values.1,
            max_delay_millis: values.2,
            backoff_basis_points: values.3,
            min_uptime_millis: values.4,
        },
    })
}

fn duration_millis(
    value: &str,
    field: CliInputField,
    allow_zero: bool,
) -> Result<u64, CliInputError> {
    let parser = DurationParser::new(DurationUnits::SUB_DAY)
        .allow_compound()
        .with_maximum(MAX_POLICY_DURATION);
    let parser = if allow_zero {
        parser.allow_zero()
    } else {
        parser
    };
    let duration = parser
        .parse(value)
        .map_err(|source| CliInputError::InvalidDuration { field, source })?;
    u64::try_from(duration.as_millis()).map_err(|_| CliInputError::DurationOutOfRange { field })
}

fn require_page_size(page_size: u32) -> Result<(), CliInputError> {
    if page_size == 0 {
        Err(CliInputError::InvalidPageSize)
    } else {
        Ok(())
    }
}

fn key_values(
    values: &[String],
    field: CliInputField,
) -> Result<BTreeMap<String, String>, CliInputError> {
    values
        .iter()
        .enumerate()
        .try_fold(BTreeMap::new(), |mut parsed, (index, value)| {
            let (key, value) = value
                .split_once('=')
                .filter(|(key, _)| !key.is_empty())
                .ok_or(CliInputError::InvalidKeyValue { field, index })?;
            parsed.insert(key.to_owned(), value.to_owned());
            Ok(parsed)
        })
}

fn absolute_path(
    path: &Path,
    current_directory: &Path,
    field: CliInputField,
) -> Result<String, CliInputError> {
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        current_directory.join(path)
    };
    utf8_path(&resolved, field)
}

fn utf8_path(path: &Path, field: CliInputField) -> Result<String, CliInputError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or(CliInputError::NonUtf8Path { field })
}

fn health_rule(value: &str, index: usize) -> Result<HealthRuleDto, CliInputError> {
    let invalid = || CliInputError::InvalidHealthRule { index };
    if let Some(port) = value
        .strip_prefix("port:")
        .or_else(|| value.strip_prefix("tcp:"))
    {
        return port
            .parse::<u16>()
            .ok()
            .filter(|port| *port != 0)
            .map(|port| HealthRuleDto::TcpPort { port })
            .ok_or_else(invalid);
    }
    if let Some(endpoint) = value.strip_prefix("http:") {
        return non_empty(endpoint, invalid)
            .map(|endpoint| HealthRuleDto::HttpEndpoint { endpoint });
    }
    if let Some(pattern) = value.strip_prefix("log-match:") {
        return non_empty(pattern, invalid)
            .map(|pattern| HealthRuleDto::OutputContains { pattern });
    }
    if let Some(pattern) = value.strip_prefix("log-error:") {
        return non_empty(pattern, invalid).map(|pattern| HealthRuleDto::OutputRejects { pattern });
    }
    if value == "error-color" {
        return Ok(HealthRuleDto::RedCellThreshold { minimum_cells: 12 });
    }
    if let Some(minimum) = value.strip_prefix("error-color:") {
        return minimum
            .parse::<u32>()
            .ok()
            .filter(|minimum| *minimum != 0)
            .map(|minimum_cells| HealthRuleDto::RedCellThreshold { minimum_cells })
            .ok_or_else(invalid);
    }
    if let Some(command) = value
        .strip_prefix("exit-command:")
        .or_else(|| value.strip_prefix("exit:"))
    {
        let mut components = command.split_whitespace();
        let program = components.next().ok_or_else(invalid)?.to_owned();
        return Ok(HealthRuleDto::Command {
            command: CommandSpecDto {
                program,
                arguments: components.map(str::to_owned).collect(),
            },
        });
    }
    Err(invalid())
}

fn non_empty(value: &str, error: impl FnOnce() -> CliInputError) -> Result<String, CliInputError> {
    if value.is_empty() {
        Err(error())
    } else {
        Ok(value.to_owned())
    }
}

fn parse_key(value: &str, index: usize) -> Result<KeyDto, CliInputError> {
    let normalized = value.to_ascii_lowercase();
    let key = match normalized.as_str() {
        "enter" | "return" => KeyDto::Enter,
        "tab" => KeyDto::Tab,
        "backtab" | "back-tab" | "shift-tab" => KeyDto::BackTab,
        "escape" | "esc" => KeyDto::Escape,
        "backspace" => KeyDto::Backspace,
        "delete" | "del" => KeyDto::Delete,
        "space" => KeyDto::Space,
        "up" => KeyDto::Up,
        "down" => KeyDto::Down,
        "left" => KeyDto::Left,
        "right" => KeyDto::Right,
        "home" => KeyDto::Home,
        "end" => KeyDto::End,
        "pageup" | "page-up" => KeyDto::PageUp,
        "pagedown" | "page-down" => KeyDto::PageDown,
        _ => return parse_complex_key(&normalized, index),
    };
    Ok(key)
}

fn parse_complex_key(value: &str, index: usize) -> Result<KeyDto, CliInputError> {
    let invalid = || CliInputError::InvalidTerminalKey { index };
    if let Some(number) = value.strip_prefix('f')
        && let Ok(number) = number.parse::<u8>()
        && (1..=24).contains(&number)
    {
        return Ok(KeyDto::Function { number });
    }
    if let Some(character) = value.strip_prefix("ctrl-")
        && character.chars().count() == 1
    {
        return Ok(KeyDto::Control {
            character: character.chars().next().ok_or_else(invalid)?,
        });
    }

    let mut shift = false;
    let mut control = false;
    let mut alt = false;
    let mut remainder = value;
    loop {
        if let Some(next) = remainder.strip_prefix("shift-")
            && !shift
        {
            shift = true;
            remainder = next;
        } else if let Some(next) = remainder.strip_prefix("ctrl-")
            && !control
        {
            control = true;
            remainder = next;
        } else if let Some(next) = remainder.strip_prefix("control-")
            && !control
        {
            control = true;
            remainder = next;
        } else if let Some(next) = remainder.strip_prefix("alt-")
            && !alt
        {
            alt = true;
            remainder = next;
        } else {
            break;
        }
    }
    let base = parse_modifiable_key(remainder).ok_or_else(invalid)?;
    if !shift && !control && !alt {
        return Err(invalid());
    }
    Ok(KeyDto::Modified {
        base,
        modifiers: KeyModifiersDto {
            shift,
            control,
            alt,
        },
    })
}

fn parse_modifiable_key(value: &str) -> Option<ModifiableKeyDto> {
    Some(match value {
        "up" => ModifiableKeyDto::Up,
        "down" => ModifiableKeyDto::Down,
        "left" => ModifiableKeyDto::Left,
        "right" => ModifiableKeyDto::Right,
        "home" => ModifiableKeyDto::Home,
        "end" => ModifiableKeyDto::End,
        "pageup" | "page-up" => ModifiableKeyDto::PageUp,
        "pagedown" | "page-down" => ModifiableKeyDto::PageDown,
        value if value.starts_with('f') => ModifiableKeyDto::Function {
            number: value
                .strip_prefix('f')?
                .parse::<u8>()
                .ok()
                .filter(|number| (1..=24).contains(number))?,
        },
        _ => return None,
    })
}

const fn stop_signal(value: StopSignalArgument) -> StopSignalDto {
    match value {
        StopSignalArgument::Terminate => StopSignalDto::Terminate,
        StopSignalArgument::Interrupt => StopSignalDto::Interrupt,
        StopSignalArgument::Hangup => StopSignalDto::Hangup,
        StopSignalArgument::Quit => StopSignalDto::Quit,
        StopSignalArgument::User1 => StopSignalDto::User1,
        StopSignalArgument::User2 => StopSignalDto::User2,
    }
}

pub(crate) const fn lifecycle(value: LifecycleArgument) -> LifecycleKindDto {
    match value {
        LifecycleArgument::Starting => LifecycleKindDto::Starting,
        LifecycleArgument::Running => LifecycleKindDto::Running,
        LifecycleArgument::Stopping => LifecycleKindDto::Stopping,
        LifecycleArgument::Stopped => LifecycleKindDto::Stopped,
        LifecycleArgument::Exited => LifecycleKindDto::Exited,
        LifecycleArgument::Failed => LifecycleKindDto::Failed,
    }
}

pub(crate) const fn execution(value: ExecutionArgument) -> ExecutionStateDto {
    match value {
        ExecutionArgument::Active => ExecutionStateDto::Active,
        ExecutionArgument::Quiescent => ExecutionStateDto::Quiescent,
        ExecutionArgument::Unknown => ExecutionStateDto::Unknown,
    }
}

pub(crate) const fn interaction(value: InteractionArgument) -> InteractionStateDto {
    match value {
        InteractionArgument::Absent => InteractionStateDto::None,
        InteractionArgument::Available => InteractionStateDto::Available,
        InteractionArgument::Requested => InteractionStateDto::Requested,
    }
}

pub(crate) const fn health(value: HealthArgument) -> HealthStatusDto {
    match value {
        HealthArgument::Healthy => HealthStatusDto::Healthy,
        HealthArgument::Unhealthy => HealthStatusDto::Unhealthy,
        HealthArgument::Unknown => HealthStatusDto::Unknown,
        HealthArgument::NoCheck => HealthStatusDto::NoCheck,
    }
}

const fn session_sort(value: SessionSortArgument) -> SessionSortFieldDto {
    match value {
        SessionSortArgument::Name => SessionSortFieldDto::Name,
        SessionSortArgument::Lifecycle => SessionSortFieldDto::Lifecycle,
        SessionSortArgument::Execution => SessionSortFieldDto::Execution,
        SessionSortArgument::Interaction => SessionSortFieldDto::Interaction,
        SessionSortArgument::Health => SessionSortFieldDto::Health,
        SessionSortArgument::UpdatedAt => SessionSortFieldDto::UpdatedAt,
        SessionSortArgument::RestartCount => SessionSortFieldDto::RestartCount,
    }
}

const fn projection(value: SessionProjectionArgument) -> SessionProjectionDto {
    match value {
        SessionProjectionArgument::Compact => SessionProjectionDto::Compact,
        SessionProjectionArgument::WithLabels => SessionProjectionDto::WithLabels,
        SessionProjectionArgument::Operational => SessionProjectionDto::Operational,
    }
}

const fn diagnostic_section(value: DiagnosticSectionArgument) -> DiagnosticSectionDto {
    match value {
        DiagnosticSectionArgument::Connections => DiagnosticSectionDto::Connections,
        DiagnosticSectionArgument::Streams => DiagnosticSectionDto::Streams,
        DiagnosticSectionArgument::SlowConsumers => DiagnosticSectionDto::SlowConsumers,
        DiagnosticSectionArgument::OutputProducers => DiagnosticSectionDto::OutputProducers,
        DiagnosticSectionArgument::Supervisors => DiagnosticSectionDto::SupervisorQueues,
        DiagnosticSectionArgument::Detection => DiagnosticSectionDto::DetectionEvidence,
        DiagnosticSectionArgument::Persistence => DiagnosticSectionDto::Persistence,
        DiagnosticSectionArgument::Reconciliation => DiagnosticSectionDto::Reconciliation,
        DiagnosticSectionArgument::Configuration => DiagnosticSectionDto::Configuration,
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::{NativeCliArguments, NativeCliCommand};

    fn parse_start(arguments: &[&str]) -> Box<StartArguments> {
        let parsed = NativeCliArguments::try_parse_from(arguments).expect("valid CLI arguments");
        let NativeCliCommand::Start(start) = parsed.command else {
            panic!("expected start")
        };
        start
    }

    fn context() -> StartPreparationContext {
        StartPreparationContext::new(workspace(), Some("/bin/zsh".to_owned()))
            .expect("absolute context")
    }

    fn workspace() -> PathBuf {
        std::env::temp_dir().join("workspace")
    }

    #[test]
    fn start_preparation_resolves_paths_and_retains_private_values_only_in_the_dto() {
        let arguments = parse_start(&[
            "uc",
            "start",
            "worker",
            "--env",
            "TOKEN=private",
            "--env-file",
            ".env",
            "--cwd",
            "service",
            "--label",
            "env=prod",
            "--metadata",
            "owner=agent",
            "--healthy-when",
            "port:8080",
            "--",
            "sh",
            "-c",
            "echo ok",
        ]);

        let request = prepare_start_request(&arguments, &context(), MutationContext::default())
            .expect("prepared start");

        assert_eq!(request.command.program, "sh");
        assert_eq!(request.command.arguments, ["-c", "echo ok"]);
        assert_eq!(
            request.working_directory.as_deref().map(Path::new),
            Some(workspace().join("service").as_path())
        );
        assert_eq!(
            request.environment.file.as_deref().map(Path::new),
            Some(workspace().join(".env").as_path())
        );
        assert_eq!(
            request
                .environment
                .variables
                .get("TOKEN")
                .map(String::as_str),
            Some("private")
        );
        assert_eq!(request.labels.get("env").map(String::as_str), Some("prod"));
        assert_eq!(
            request.metadata.get("owner").map(String::as_str),
            Some("agent")
        );
        assert_eq!(
            request.health.rules,
            [HealthRuleDto::TcpPort { port: 8080 }]
        );
        assert_eq!(request.output_capture_mode, OutputCaptureModeDto::Terminal);
    }

    #[test]
    fn opaque_output_capture_maps_to_the_protocol_request() {
        let arguments = parse_start(&[
            "uc",
            "start",
            "binary-worker",
            "--output-capture",
            "opaque",
            "--",
            "cat",
        ]);

        let request = prepare_start_request(&arguments, &context(), MutationContext::default())
            .expect("prepared opaque start");

        assert_eq!(request.output_capture_mode, OutputCaptureModeDto::Opaque);
    }

    #[test]
    fn native_is_default_and_sealed_is_explicit_for_direct_and_interactive_starts() {
        for tail in [vec!["--", "/bin/echo"], vec!["-i"]] {
            for (flag, mode) in [
                (None, undercurrent_protocol::LaunchModeDto::Native),
                (Some("native"), undercurrent_protocol::LaunchModeDto::Native),
                (Some("sealed"), undercurrent_protocol::LaunchModeDto::Sealed),
            ] {
                let mut argv = vec!["uc", "start", "mode-test"];
                if let Some(flag) = flag {
                    argv.extend(["--launch-mode", flag]);
                }
                argv.extend(tail.clone());
                let args = parse_start(&argv);
                let request = prepare_start_request(&args, &context(), MutationContext::default())
                    .expect("prepare start");
                assert_eq!(request.launch_mode, mode);
            }
        }
    }

    #[test]
    fn interactive_start_uses_the_captured_shell_without_reading_process_state() {
        let arguments = parse_start(&["uc", "start", "shell", "--interactive"]);

        let request = prepare_start_request(&arguments, &context(), MutationContext::default())
            .expect("prepared interactive start");

        assert_eq!(request.command.program, "/bin/zsh");
        assert!(request.command.arguments.is_empty());
        assert_eq!(
            request.profile_binding,
            ProfileBindingModeDto::ForegroundAdaptive
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_default_shell_prefers_path_powershell_and_falls_back_to_system_powershell() {
        let root = tempfile::tempdir().expect("fixture directory");
        let missing = root.path().join("missing");
        let invalid = root.path().join("invalid");
        let valid = root.path().join("PowerShell with spaces");
        std::fs::create_dir_all(invalid.join("pwsh.exe")).unwrap();
        std::fs::create_dir_all(&valid).unwrap();
        std::fs::write(valid.join("pwsh.exe"), b"discovery fixture").unwrap();
        let windows = root.path().join("Windows");
        let path =
            std::env::join_paths([missing.as_path(), invalid.as_path(), valid.as_path()]).unwrap();
        assert_eq!(
            windows_default_shell(&path, Some(&windows)),
            valid.join("pwsh.exe").to_string_lossy()
        );
        std::fs::remove_file(valid.join("pwsh.exe")).unwrap();
        assert_eq!(
            windows_default_shell(&path, Some(&windows)),
            windows
                .join("System32/WindowsPowerShell/v1.0/powershell.exe")
                .to_string_lossy()
        );
    }

    #[test]
    fn explicit_interactive_shell_overrides_the_default() {
        let arguments = parse_start(&["uc", "start", "shell", "-i", "--shell", "chosen-shell"]);
        let request =
            prepare_start_request(&arguments, &context(), MutationContext::default()).unwrap();
        assert_eq!(request.command.program, "chosen-shell");
    }

    #[test]
    fn stop_policy_defaults_follow_launch_mode_and_preserve_explicit_signals() {
        for interactive in [false, true] {
            for explicit in [None, Some("terminate"), Some("hangup"), Some("interrupt")] {
                let mut argv = vec!["uc", "start", "worker"];
                if interactive {
                    argv.push("--interactive");
                }
                if let Some(signal) = explicit {
                    argv.extend(["--stop-signal", signal]);
                }
                if !interactive {
                    argv.extend(["--", "sleep", "60"]);
                }
                let arguments = parse_start(&argv);
                let request =
                    prepare_start_request(&arguments, &context(), MutationContext::default())
                        .expect("prepared start");
                let expected = match explicit {
                    Some("terminate") => StopSignalDto::Terminate,
                    Some("hangup") => StopSignalDto::Hangup,
                    Some("interrupt") => StopSignalDto::Interrupt,
                    None if interactive => StopSignalDto::Hangup,
                    None => StopSignalDto::Terminate,
                    _ => unreachable!(),
                };
                assert_eq!(request.execution.stop.signal, expected, "{argv:?}");
                assert_eq!(request.execution.stop.timeout_millis, 5000);
            }
        }
    }

    #[test]
    fn interactive_start_rejects_a_direct_command_with_actionable_payload_safe_help() {
        let arguments = parse_start(&[
            "uc",
            "start",
            "shell",
            "--interactive",
            "--",
            "private-command",
            "private-argument",
        ]);

        let error = match prepare_start_request(&arguments, &context(), MutationContext::default())
        {
            Ok(_) => panic!("interactive command must be rejected"),
            Err(error) => error,
        };

        assert!(matches!(error, CliInputError::InteractiveCommandConflict));
        let rendered = error.to_string();
        assert!(rendered.contains("cannot be combined"));
        assert!(rendered.contains("uc start NAME --interactive --attach"));
        assert!(rendered.contains("remove `--interactive`"));
        assert!(!rendered.contains("private-command"));
        assert!(!rendered.contains("private-argument"));
    }

    #[test]
    fn profile_binding_follows_direct_launches_unless_explicitly_pinned() {
        let direct = prepare_start_request(
            &parse_start(&["uc", "start", "worker", "--", "codex"]),
            &context(),
            MutationContext::default(),
        )
        .expect("direct start");
        assert_eq!(
            direct.profile_binding,
            ProfileBindingModeDto::ForegroundAdaptive
        );

        let wrapped = prepare_start_request(
            &parse_start(&["uc", "start", "worker", "--", "agent", "codex", "start"]),
            &context(),
            MutationContext::default(),
        )
        .expect("wrapped start");
        assert_eq!(
            wrapped.profile_binding,
            ProfileBindingModeDto::ForegroundAdaptive
        );

        let fixed = prepare_start_request(
            &parse_start(&[
                "uc",
                "start",
                "worker",
                "--profile-mode",
                "fixed",
                "--",
                "codex",
            ]),
            &context(),
            MutationContext::default(),
        )
        .expect("fixed start");
        assert_eq!(fixed.profile_binding, ProfileBindingModeDto::Fixed);

        let explicit = prepare_start_request(
            &parse_start(&["uc", "start", "worker", "--profile", "codex", "--", "sh"]),
            &context(),
            MutationContext::default(),
        )
        .expect("explicit profile");
        assert_eq!(explicit.profile_binding, ProfileBindingModeDto::Fixed);

        let adaptive_explicit = prepare_start_request(
            &parse_start(&[
                "uc",
                "start",
                "worker",
                "--profile",
                "bash",
                "--profile-mode",
                "foreground",
                "--",
                "sh",
            ]),
            &context(),
            MutationContext::default(),
        )
        .expect("adaptive explicit profile");
        assert_eq!(
            adaptive_explicit.profile_binding,
            ProfileBindingModeDto::ForegroundAdaptive
        );

        let disabled = prepare_start_request(
            &parse_start(&[
                "uc",
                "start",
                "worker",
                "--no-detect",
                "--profile-mode",
                "foreground",
                "--",
                "sh",
            ]),
            &context(),
            MutationContext::default(),
        )
        .expect("disabled detection");
        assert_eq!(disabled.profile_binding, ProfileBindingModeDto::Fixed);
    }

    #[test]
    fn invalid_environment_diagnostic_does_not_retain_the_private_value() {
        let arguments = parse_start(&[
            "uc",
            "start",
            "worker",
            "--env",
            "private-value-without-key",
            "--",
            "true",
        ]);

        let error = match prepare_start_request(&arguments, &context(), MutationContext::default())
        {
            Ok(_) => panic!("expected invalid environment"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            CliInputError::InvalidKeyValue {
                field: CliInputField::Environment,
                index: 0
            }
        ));
        assert!(!error.to_string().contains("private-value"));
    }

    #[test]
    fn list_preparation_preserves_every_server_side_query_control() {
        let parsed = NativeCliArguments::try_parse_from([
            "uc",
            "ls",
            "--name",
            "worker",
            "--lifecycle",
            "running",
            "--execution",
            "active",
            "--interaction",
            "requested",
            "--health",
            "healthy",
            "--profile",
            "agent",
            "--label",
            "env=prod",
            "--has-active-work",
            "true",
            "--sort",
            "restart-count",
            "--descending",
            "--projection",
            "operational",
            "--page-size",
            "23",
            "--cursor",
            "opaque",
            "--total",
        ])
        .expect("valid list");
        let NativeCliCommand::Ls(arguments) = parsed.command else {
            panic!("expected list")
        };

        let request = prepare_list_request(&arguments).expect("prepared list");

        assert_eq!(request.filter.name.as_deref(), Some("worker"));
        assert_eq!(request.filter.lifecycles, [LifecycleKindDto::Running]);
        assert_eq!(request.filter.executions, [ExecutionStateDto::Active]);
        assert_eq!(
            request.filter.interactions,
            [InteractionStateDto::Requested]
        );
        assert_eq!(request.filter.health, [HealthStatusDto::Healthy]);
        assert_eq!(
            request.filter.labels.get("env").map(String::as_str),
            Some("prod")
        );
        assert_eq!(request.filter.has_active_work, Some(true));
        assert_eq!(request.sort.field, SessionSortFieldDto::RestartCount);
        assert_eq!(request.sort.direction, SortDirectionDto::Descending);
        assert_eq!(request.projection, SessionProjectionDto::Operational);
        assert_eq!(request.page_size, 23);
        assert_eq!(request.cursor.as_deref(), Some("opaque"));
        assert!(request.include_total);
    }

    #[test]
    fn exact_name_lookup_is_one_compact_row_without_total_count() {
        let request = exact_name_lookup("worker");

        assert_eq!(request.filter.name.as_deref(), Some("worker"));
        assert_eq!(request.page_size, 1);
        assert_eq!(request.projection, SessionProjectionDto::Compact);
        assert!(!request.include_total);
        assert!(request.cursor.is_none());

        let by_id = exact_id_lookup("session-1");
        assert_eq!(by_id.filter.ids, ["session-1"]);
        assert!(by_id.filter.name.is_none());
        assert_eq!(by_id.page_size, 1);
    }

    #[test]
    fn bulk_selections_are_explicit_and_server_side() {
        let label = prepare_stop_selection(&StopArguments {
            name: None,
            all: false,
            label: Some("environment=production".to_owned()),
            graceful: true,
            force: false,
        })
        .expect("label selection");
        let PreparedSessionSelection::Query(label_filter) = label else {
            panic!("expected query selection")
        };
        assert_eq!(
            label_filter.labels.get("environment").map(String::as_str),
            Some("production")
        );

        let terminal = prepare_clean_selection(&CleanArguments {
            name: None,
            all: true,
            yes: true,
        })
        .expect("terminal selection");
        let PreparedSessionSelection::Query(terminal_filter) = terminal else {
            panic!("expected query selection")
        };
        assert_eq!(
            terminal_filter.lifecycles,
            [
                LifecycleKindDto::Stopped,
                LifecycleKindDto::Exited,
                LifecycleKindDto::Failed,
            ]
        );
    }

    #[test]
    fn stop_requires_selection_and_clean_defaults_to_terminal_sessions() {
        assert!(matches!(
            prepare_stop_selection(&StopArguments {
                name: None,
                all: false,
                label: None,
                graceful: true,
                force: false,
            }),
            Err(CliInputError::MissingSessionSelection)
        ));
        let implicit = prepare_clean_selection(&CleanArguments {
            name: None,
            all: false,
            yes: false,
        })
        .expect("default clean selection");
        let explicit = prepare_clean_selection(&CleanArguments {
            name: None,
            all: true,
            yes: false,
        })
        .expect("explicit clean selection");
        assert_eq!(implicit, explicit);
        assert_eq!(
            prepare_clean_selection(&CleanArguments {
                name: Some("worker".to_owned()),
                all: false,
                yes: false,
            })
            .expect("named clean selection"),
            PreparedSessionSelection::ExactName("worker".to_owned())
        );
    }

    #[test]
    fn stream_preparation_is_selective_bounded_and_binary_for_terminals() {
        let parsed = NativeCliArguments::try_parse_from([
            "uc",
            "watch",
            "worker-a",
            "worker-b",
            "--page-size",
            "2",
            "--projection",
            "operational",
        ])
        .expect("valid watch");
        let NativeCliCommand::Watch(arguments) = parsed.command else {
            panic!("expected watch")
        };
        let specification = prepare_watch_stream(
            &arguments,
            vec!["session-a".to_owned(), "session-b".to_owned()],
        )
        .expect("bounded named watch");
        let ProductStreamSpec::SessionWatch(watch) = specification else {
            panic!("expected session watch")
        };
        assert_eq!(watch.query.filter.ids, ["session-a", "session-b"]);
        assert_eq!(watch.query.page_size, 2);
        assert_eq!(watch.query.projection, SessionProjectionDto::Operational);

        let terminal = prepare_terminal_stream("session-a", TerminalInitialModeDto::Screen);
        assert!(matches!(
            terminal,
            ProductStreamSpec::TerminalOutput(TerminalOutputSpec {
                session_id,
                initial: TerminalInitialModeDto::Screen,
            }) if session_id == "session-a"
        ));
    }

    #[test]
    fn explicit_watch_selection_cannot_be_silently_truncated() {
        let parsed = NativeCliArguments::try_parse_from([
            "uc",
            "watch",
            "worker-a",
            "worker-b",
            "--page-size",
            "1",
        ])
        .expect("valid syntax");
        let NativeCliCommand::Watch(arguments) = parsed.command else {
            panic!("expected watch")
        };

        assert!(matches!(
            prepare_watch_stream(
                &arguments,
                vec!["session-a".to_owned(), "session-b".to_owned()]
            ),
            Err(CliInputError::SelectionExceedsPage {
                selected: 2,
                page_size: 1
            })
        ));
    }

    #[test]
    fn terminal_input_is_one_atomic_typed_sequence_without_payload_diagnostics() {
        let parsed = NativeCliArguments::try_parse_from([
            "uc",
            "send",
            "worker",
            "private input",
            "--key",
            "ctrl-c",
            "--key",
            "alt-page-up",
            "--delivery-id",
            "delivery-1",
        ])
        .expect("valid send");
        let NativeCliCommand::Send(arguments) = parsed.command else {
            panic!("expected send")
        };

        let request = prepare_terminal_input_request(
            &arguments,
            "session-1",
            None,
            MutationContext::default(),
        )
        .expect("prepared input");

        assert_eq!(request.session_id, "session-1");
        assert_eq!(request.delivery_id.as_deref(), Some("delivery-1"));
        assert_eq!(request.items.len(), 4);
        assert!(matches!(
            request.items[1],
            TerminalInputDto::Key {
                key: KeyDto::Control { character: 'c' }
            }
        ));
        assert!(matches!(
            request.items[2],
            TerminalInputDto::Key {
                key: KeyDto::Modified {
                    base: ModifiableKeyDto::PageUp,
                    modifiers: KeyModifiersDto { alt: true, .. }
                }
            }
        ));
        assert!(matches!(
            request.items[3],
            TerminalInputDto::Key { key: KeyDto::Enter }
        ));
    }

    #[test]
    fn prompt_modes_and_no_submit_are_explicit() {
        for flag in [None, Some("--paste"), Some("--raw")] {
            let mut args = vec!["uc", "send", "worker", "large\nprompt", "--no-newline"];
            if let Some(flag) = flag {
                args.push(flag);
            }
            let NativeCliCommand::Send(arguments) =
                NativeCliArguments::try_parse_from(args).unwrap().command
            else {
                panic!("send");
            };
            let request = prepare_terminal_input_request(
                &arguments,
                "session-1",
                None,
                MutationContext::default(),
            )
            .unwrap();
            assert_eq!(request.items.len(), 1);
            match (&request.items[0], flag) {
                (TerminalInputDto::AutoText { text }, None)
                | (TerminalInputDto::Paste { text }, Some("--paste"))
                | (TerminalInputDto::Text { text }, Some("--raw")) => {
                    assert_eq!(text, "large\nprompt")
                }
                _ => panic!("wrong input mode"),
            }
        }
        assert!(
            NativeCliArguments::try_parse_from(["uc", "send", "worker", "x", "--paste", "--raw"])
                .is_err()
        );
    }

    #[test]
    fn key_only_input_does_not_gain_an_implicit_enter() {
        let parsed =
            NativeCliArguments::try_parse_from(["uc", "send", "worker", "--key", "ctrl-c"])
                .expect("valid key input");
        let NativeCliCommand::Send(arguments) = parsed.command else {
            panic!("expected send")
        };

        let request = prepare_terminal_input_request(
            &arguments,
            "session-1",
            None,
            MutationContext::default(),
        )
        .expect("prepared key input");

        assert_eq!(request.items.len(), 1);
        assert!(matches!(
            request.items[0],
            TerminalInputDto::Key {
                key: KeyDto::Control { character: 'c' }
            }
        ));
    }

    #[test]
    fn reconciliation_debug_selection_maps_to_its_distinct_protocol_section() {
        let parsed = NativeCliArguments::try_parse_from([
            "uc",
            "debug",
            "reconciliation",
            "--page-size",
            "25",
            "--cursor",
            "opaque",
        ])
        .expect("valid diagnostics");
        let NativeCliCommand::Debug(arguments) = parsed.command else {
            panic!("expected debug")
        };

        let request = prepare_diagnostics_request(&arguments).expect("prepared diagnostics");

        assert_eq!(request.sections, [DiagnosticSectionDto::Reconciliation]);
        assert_eq!(request.page_size, 25);
        assert_eq!(request.cursor.as_deref(), Some("opaque"));
    }

    #[test]
    fn diagnostic_bundle_defaults_to_redacted_and_bounds_explicit_terminal_content() {
        let safe = NativeCliArguments::try_parse_from(["uc", "debug", "bundle"])
            .expect("safe diagnostic bundle");
        let NativeCliCommand::Debug(safe) = safe.command else {
            panic!("expected debug bundle")
        };
        let safe = prepare_diagnostic_bundle_request(safe.action.as_ref().expect("bundle action"))
            .expect("safe bundle request");
        assert_eq!(safe.sections, DiagnosticSectionDto::ALL);
        assert_eq!(safe.page_size, 100);
        assert!(safe.cursor.is_none());
        assert!(safe.terminal_content.is_none());

        let explicit = NativeCliArguments::try_parse_from([
            "uc",
            "debug",
            "bundle",
            "--include-terminal-content",
            "worker",
            "--from-offset",
            "9",
            "--max-bytes",
            "1024",
            "--cursor",
            "bundle-page-2",
        ])
        .expect("explicit diagnostic bundle");
        let NativeCliCommand::Debug(explicit) = explicit.command else {
            panic!("expected debug bundle")
        };
        let explicit =
            prepare_diagnostic_bundle_request(explicit.action.as_ref().expect("bundle action"))
                .expect("explicit bundle request");
        let terminal = explicit.terminal_content.expect("terminal selection");
        assert_eq!(terminal.session_id, "worker");
        assert_eq!(terminal.from_offset, 9);
        assert_eq!(terminal.max_bytes, 1024);
        assert_eq!(explicit.cursor.as_deref(), Some("bundle-page-2"));
    }

    #[test]
    fn profile_sample_and_shutdown_duration_are_prepared_without_ambient_io() {
        let parsed = NativeCliArguments::try_parse_from([
            "uc",
            "profile",
            "test",
            "agent",
            "--screen-text",
            "private screen",
            "--last-line",
            "private line",
            "--cursor-not-at-end",
        ])
        .expect("valid profile test");
        let NativeCliCommand::Profile(profile) = parsed.command else {
            panic!("expected profile")
        };
        let crate::ProfileAction::Test(arguments) = profile.action else {
            panic!("expected profile test")
        };

        let request = prepare_profile_test_request(&arguments);
        let shutdown = prepare_shutdown_request("1m30s", MutationContext::default())
            .expect("prepared shutdown");

        assert_eq!(request.name, "agent");
        assert_eq!(request.screen_text, "private screen");
        assert_eq!(request.last_line, "private line");
        assert!(!request.cursor_at_end);
        assert_eq!(shutdown.drain_timeout_millis, 90_000);
    }
}
