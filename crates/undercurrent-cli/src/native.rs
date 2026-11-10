// SPDX-License-Identifier: Apache-2.0

//! Native `uc` process composition and complete command routing.

use std::ffi::{OsStr, OsString};
use std::io::{BufRead, IsTerminal, Read, Write};
use std::time::{Duration, Instant};

use crate::cli_support::{
    CliContext, CliOutputError, CommandTimeout, CommonExitCode, OutputFormat, OutputShape,
    Verbosity,
};
use clap::Parser;
use clap::error::ErrorKind as ClapErrorKind;
use undercurrent_protocol::{
    ExecutionOutputDto, ProductRequest, ProductStreamSpec, ProductSuccess, RemovalConfirmationDto,
    TerminalInitialModeDto, TerminalSizeDto,
};
use undercurrent_sdk::{ProductClient, ProductClientError, ProductSubscription};

use crate::{
    CliCommand, CliDiagnosticEvent, CliDiagnosticRecord, CliDiagnosticSink, CliError,
    CliExecutionLimits, CliInputError, CliInvocationContext, CliPresenter, CliProcessOperation,
    CliProductRequestPort, CliProductStreamPort, CliStandardInputOperation, CliStreamCancellation,
    CliTerminalStream, ConnectedFiniteCommand, ConnectedFiniteDispatcher, ConnectedStreamCommand,
    ConnectedStreamDispatcher, DaemonAction, DaemonLifecycleDispatcher, DaemonLifecycleService,
    DetachMatcher, FiniteCliExecutor, InteractiveAttachExecutor, InteractiveTerminalPort,
    NativeCliArguments, NativeCliCommand, NativeCliDiagnosticSink, NativeDaemonLifecyclePort,
    NativeInteractiveTerminal, NativeProductClientFactory, NativeResetLifecyclePort,
    NativeResetStatePort, NativeStreamSignalGuard, OpenedCliSubscription, OpenedInteractiveAttach,
    OutputCaptureModeArgument, ResetService, ResolvedCliConfig, StartPreparationContext,
    execute_completion, execute_config, execute_events, execute_init, reset_text,
    session_mutation_text, terminal_output_next_offset, validate_start_arguments,
};

fn apply_cli_preferences(
    arguments: &mut NativeCliArguments,
    preferences: undercurrent_config::CliPreferences,
) {
    match &mut arguments.command {
        NativeCliCommand::Ls(a) => {
            a.page_size = Some(a.page_size.unwrap_or(preferences.list_page_size))
        }
        NativeCliCommand::Top(a) => {
            a.page_size = Some(a.page_size.unwrap_or(preferences.list_page_size))
        }
        NativeCliCommand::Watch(a) => {
            a.page_size = Some(a.page_size.unwrap_or(preferences.list_page_size))
        }
        NativeCliCommand::Logs(a) => {
            a.max_bytes = Some(a.max_bytes.unwrap_or(preferences.logs_max_bytes))
        }
        _ => {}
    }
}

fn apply_default_profile(arguments: &mut NativeCliArguments, default_profile: Option<&str>) {
    if let NativeCliCommand::Start(start) = &mut arguments.command
        && start.profile.is_none()
    {
        start.profile = default_profile.map(str::to_owned);
    }
}

const CONFIRMATION_BYTES: u64 = 64;

struct ConnectedCommandContext<'a> {
    client: &'a DiagnosticProductClient<'a>,
    configuration: &'a ResolvedCliConfig,
    limits: CliExecutionLimits,
    timeout: Duration,
    presenter: CliPresenter,
    diagnostics: &'a dyn CliDiagnosticSink,
}

struct DiagnosticProductClient<'a> {
    client: ProductClient,
    command: CliCommand,
    diagnostics: &'a dyn CliDiagnosticSink,
}

struct PreparedInteractiveAttach {
    initial: TerminalInitialModeDto,
    read_only: bool,
    detach: Option<DetachMatcher>,
}

struct NativeAttachContext {
    preparation: PreparedInteractiveAttach,
    terminal: NativeInteractiveTerminal,
    size: TerminalSizeDto,
}

/// Native standard-stream terminal availability captured before CLI execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CliTerminalAvailability {
    standard_input: bool,
    standard_output: bool,
}

impl CliTerminalAvailability {
    /// Creates one immutable standard-stream terminal observation.
    ///
    /// This function performs no IO and does not panic.
    pub const fn new(standard_input: bool, standard_output: bool) -> Self {
        Self {
            standard_input,
            standard_output,
        }
    }
}

/// Native process exit preserving both a stable diagnostic category and code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CliProcessExit {
    category: CommonExitCode,
    code: u8,
}

impl CliProcessExit {
    /// Returns the stable category used by diagnostics and structured errors.
    pub const fn category(self) -> CommonExitCode {
        self.category
    }

    /// Returns the exact portable code returned to the operating system.
    pub const fn code(self) -> u8 {
        self.code
    }

    fn executed(exit_code: Option<i32>) -> Self {
        let category = if exit_code == Some(0) {
            CommonExitCode::Success
        } else {
            CommonExitCode::Failure
        };
        let code = exit_code
            .and_then(|code| u8::try_from(code).ok())
            .unwrap_or(CommonExitCode::Failure.code());
        Self { category, code }
    }
}

impl From<CommonExitCode> for CliProcessExit {
    fn from(category: CommonExitCode) -> Self {
        Self {
            category,
            code: category.code(),
        }
    }
}

impl PartialEq<CommonExitCode> for CliProcessExit {
    fn eq(&self, other: &CommonExitCode) -> bool {
        self.category == *other && self.code == other.code()
    }
}

impl CliProductRequestPort for DiagnosticProductClient<'_> {
    fn server_epoch(&self) -> Option<String> {
        self.client.server_epoch()
    }

    fn request(
        &self,
        request: ProductRequest,
        timeout: Duration,
    ) -> Result<ProductSuccess, ProductClientError> {
        let operation = request.operation();
        let started = Instant::now();
        let result = self.client.request(request, timeout);
        if let Err(error) = &result
            && let Some(recorder) = undercurrent_diagnostics::process()
        {
            let mut record = recorder.record("request", error.category().as_str(), Some(error));
            record.command = Some(self.command.as_str().into());
            record.operation = Some(operation.as_str().into());
            record.duration_micros =
                Some(started.elapsed().as_micros().try_into().unwrap_or(u64::MAX));
            record.client_events = self
                .client
                .telemetry_diagnostics()
                .unwrap_or_default()
                .into_iter()
                .rev()
                .take(128)
                .map(|event| event.as_str().into())
                .collect();
            record.client_events.reverse();
            record.dropped_client_events = self
                .client
                .telemetry_pressure()
                .map(|p| p.dropped_diagnostics);
            let _ = recorder.save(&record);
        }
        let result_category = result
            .as_ref()
            .map_or_else(crate::error::product_error_exit_code, |_| {
                CommonExitCode::Success
            });
        self.diagnostics.emit(CliDiagnosticRecord::request(
            self.command,
            operation,
            result_category,
            started.elapsed(),
        ));
        result
    }
}

impl CliProductStreamPort for DiagnosticProductClient<'_> {
    type Subscription = ProductSubscription;

    fn open_product_subscription(
        &self,
        specification: ProductStreamSpec,
        timeout: Duration,
    ) -> Result<OpenedCliSubscription<Self::Subscription>, ProductClientError> {
        let opened = self.client.open_subscription(specification, timeout)?;
        Ok(OpenedCliSubscription {
            metadata: opened.metadata,
            subscription: opened.subscription,
        })
    }
}

/// Executes one already parsed native command through its owning adapters.
///
/// Configuration is resolved at most once. Every daemon-backed ordinary
/// command creates one product client, and every finite request or logical
/// stream for that command shares the client's single physical connection.
/// Piped input and interactive confirmations are read through the supplied
/// bounded outer adapter. This function performs filesystem, network,
/// standard-input, and writer IO as required by the selected command. Process
/// creation is confined to the daemon lifecycle adapter. Commands selected by
/// the central registry may start an absent daemon automatically; diagnostics
/// and local utilities do not.
///
/// # Errors
///
/// Returns typed context, input, configuration, product, lifecycle, stream,
/// presentation, or process-boundary failure.
pub fn execute_native_cli<R, W, E>(
    arguments: &NativeCliArguments,
    context: &CliInvocationContext,
    terminals: CliTerminalAvailability,
    input: &mut R,
    output: &mut W,
    error_output: &mut E,
    diagnostics: &dyn CliDiagnosticSink,
) -> Result<CliProcessExit, CliError>
where
    R: BufRead,
    W: Write,
    E: Write,
{
    let selected_context = crate::settings::prepare_context(&arguments.command, context.clone())?;
    let context = &selected_context;
    if let NativeCliCommand::Settings(settings) = &arguments.command {
        context
            .common()
            .validate_output_shape(OutputShape::Finite)?;
        crate::settings::execute(
            settings,
            context,
            CliPresenter::new(context.common()),
            output,
        )?;
        return Ok(CommonExitCode::Success.into());
    }
    if let NativeCliCommand::Debug(crate::DebugArguments {
        action: Some(crate::DebugAction::Collect(arguments)),
        ..
    }) = &arguments.command
    {
        context
            .common()
            .validate_output_shape(OutputShape::Finite)?;
        // Collection must survive invalid configuration and must not enter the
        // ordinary command path that can auto-start an absent daemon.
        let report = crate::support::collect(arguments, context)?;
        CliPresenter::new(context.common()).write_finite(output, &report, |_| {
            format!(
                "Support report written to {}{}",
                arguments.output.display(),
                if report.partial {
                    " (partial; see manifest.json)"
                } else {
                    ""
                }
            )
        })?;
        return Ok(CommonExitCode::Success.into());
    }
    if let NativeCliCommand::Group(crate::group::GroupArguments {
        action: Some(crate::group::GroupAction::Report { .. }),
    }) = &arguments.command
    {
        // Compatibility for hooks already loaded by older shells: reports no
        // longer write metadata, and must never connect to or start a daemon.
        return Ok(CommonExitCode::Success.into());
    }
    if let NativeCliCommand::Start(arguments) = &arguments.command {
        validate_start_arguments(arguments)?;
    }
    if matches!(&arguments.command, NativeCliCommand::Tui)
        || matches!(&arguments.command, NativeCliCommand::Group(group) if group.interactive())
    {
        if context.common().output != OutputFormat::Text {
            return Err(CliError::RawOutputRequiresText);
        }
        for (available, stream) in [
            (terminals.standard_input, CliTerminalStream::StandardInput),
            (terminals.standard_output, CliTerminalStream::StandardOutput),
        ] {
            if !available {
                return Err(CliInputError::InteractiveTerminalRequired { stream }.into());
            }
        }
    }
    validate_stop_picker(&arguments.command, context.common(), terminals)?;
    validate_output_shape(&arguments.command, context.common())?;
    let mut effective_arguments = arguments.clone();
    let mut effective_context = context.clone();
    if !matches!(
        arguments.command,
        NativeCliCommand::Completion(_)
            | NativeCliCommand::Events
            | NativeCliCommand::Init(_)
            | NativeCliCommand::Context(_)
    ) {
        let config = NativeProductClientFactory::from_context(context).resolve()?;
        apply_cli_preferences(&mut effective_arguments, config.document().cli);
        apply_default_profile(&mut effective_arguments, config.default_profile());
        effective_context = context
            .with_configuration(config)
            .map_err(CliError::Context)?;
    }
    let arguments = &effective_arguments;
    let context = &effective_context;
    let mut interactive = prepare_interactive_attach(
        &arguments.command,
        context.common(),
        terminals.standard_input,
        terminals.standard_output,
    )?;

    if let Some(prepared) = &mut interactive {
        let default_keys = match &arguments.command {
            NativeCliCommand::Attach(a) => a.detach_keys == "default",
            NativeCliCommand::Start(a) => a.detach_keys == "default",
            _ => false,
        };
        if default_keys && let Some(config) = &context.configuration {
            prepared.detach = Some(crate::shortcuts::matcher(&config.document().shortcuts)?);
        }
    }
    let presenter = CliPresenter::new(context.common());
    match &arguments.command {
        NativeCliCommand::Completion(command) => {
            execute_completion(command, presenter, output)?;
            Ok(CommonExitCode::Success.into())
        }
        NativeCliCommand::Context(command) => {
            crate::session_context::execute_context(command, context, output).map(Into::into)
        }
        NativeCliCommand::Events => {
            execute_events(presenter, output)?;
            Ok(CommonExitCode::Success.into())
        }
        NativeCliCommand::Init(command) => {
            execute_init(command, presenter, output)?;
            Ok(CommonExitCode::Success.into())
        }
        NativeCliCommand::Config(command) => {
            let factory = NativeProductClientFactory::from_context(context);
            let configuration = resolve_configuration(&factory, diagnostics, CliCommand::Config)?;
            execute_config(command, &configuration, presenter, output)?;
            Ok(CommonExitCode::Success.into())
        }
        NativeCliCommand::Daemon(command) => {
            execute_daemon_command(&command.action, context, presenter, output, diagnostics)
                .map(Into::into)
        }
        NativeCliCommand::Reset(command) => {
            require_destructive_confirmation(
                CliCommand::Reset,
                command.yes,
                "reset all local Undercurrent state? [y/N] ",
                context.common(),
                terminals.standard_input,
                input,
                error_output,
            )?;
            execute_reset_command(context, presenter, output, diagnostics).map(Into::into)
        }
        _ => execute_connected_command(
            &arguments.command,
            context,
            terminals,
            interactive,
            input,
            output,
            error_output,
            diagnostics,
        ),
    }
}

/// Parses process arguments, executes one command, presents safe failures, and
/// returns a process exit preserving the stable diagnostic category and, for
/// `exec`, the one-shot child's portable exit code.
///
/// Invalid Clap input is deliberately collapsed to a generic diagnostic so a
/// rejected raw argument cannot be reflected into logs or error output. Help
/// documents receive section dividers; version output remains exact Clap output.
/// This function accesses the process environment, standard streams, filesystem,
/// and network as required
/// by the command. Explicit daemon lifecycle commands and registry-selected
/// ordinary commands may additionally launch the local daemon through the
/// lifecycle adapter.
pub fn run_native_cli_process() -> CliProcessExit {
    let raw_arguments = std::env::args_os().collect::<Vec<_>>();
    let fallback = match fallback_context(&raw_arguments) {
        Ok(context) => context,
        Err(error) => {
            if let Some(id) = crate::incidents::failure(None, &CliError::Context(error)) {
                eprintln!("invalid global options; error reference: {id}");
            }
            return CommonExitCode::Runtime.into();
        }
    };
    let arguments = match NativeCliArguments::try_parse_from(&raw_arguments) {
        Ok(arguments) => arguments,
        Err(error)
            if matches!(
                error.kind(),
                ClapErrorKind::DisplayHelp | ClapErrorKind::DisplayVersion
            ) =>
        {
            let printed = if error.kind() == ClapErrorKind::DisplayHelp {
                crate::help::print_help(&error.render())
            } else {
                error.print()
            };
            return (if printed.is_ok() {
                CommonExitCode::Success
            } else {
                CommonExitCode::Runtime
            })
            .into();
        }
        Err(_) => {
            let error = CliError::Input(CliInputError::InvalidArguments);
            let mut stderr = std::io::stderr();
            return (if CliPresenter::new(fallback)
                .write_failure_with_incident(
                    &mut stderr,
                    None,
                    &error,
                    crate::incidents::failure(None, &error),
                )
                .is_ok()
            {
                error.exit_code()
            } else {
                CommonExitCode::Runtime
            })
            .into();
        }
    };
    let command = arguments.command.command();
    let diagnostics = NativeCliDiagnosticSink::new(arguments.diagnostic);
    let started = Instant::now();
    diagnostics.emit(CliDiagnosticRecord::command(
        CliDiagnosticEvent::CommandStarted,
        command,
    ));
    let context = match CliInvocationContext::from_options(arguments.global_options())
        .map_err(CliError::Context)
        .and_then(|context| crate::settings::prepare_context(&arguments.command, context))
    {
        Ok(context) => context,
        Err(error) => {
            diagnostics.emit(CliDiagnosticRecord::terminal(
                CliDiagnosticEvent::CommandFailed,
                command,
                error.exit_code(),
                started.elapsed(),
            ));
            let mut stderr = std::io::stderr();
            return (if CliPresenter::new(fallback)
                .write_failure_with_incident(
                    &mut stderr,
                    Some(command),
                    &error,
                    crate::incidents::failure(Some(command), &error),
                )
                .is_ok()
            {
                error.exit_code()
            } else {
                CommonExitCode::Runtime
            })
            .into();
        }
    };

    crate::incidents::initialize_at(
        context
            .selection
            .as_ref()
            .map(|selection| selection.home.as_path()),
    );
    let stdin = std::io::stdin();
    let stdin_is_terminal = stdin.is_terminal();
    let mut stdout = std::io::stdout();
    let stdout_is_terminal = stdout.is_terminal();
    let mut stderr = std::io::stderr();
    let interactive = match &arguments.command {
        NativeCliCommand::Attach(_) | NativeCliCommand::Tui => true,
        NativeCliCommand::Stop(arguments) => arguments.interactive(),
        NativeCliCommand::Top(_) => {
            crate::top::interactive(stdin_is_terminal, stdout_is_terminal, context.common())
        }
        NativeCliCommand::Group(args) => args.interactive(),
        NativeCliCommand::Start(start) => start.attach,
        _ => false,
    };
    let result = if interactive {
        execute_native_cli(
            &arguments,
            &context,
            CliTerminalAvailability::new(stdin_is_terminal, stdout_is_terminal),
            &mut std::io::Cursor::new(Vec::<u8>::new()),
            &mut stdout,
            &mut stderr,
            &diagnostics,
        )
    } else {
        let mut input = stdin.lock();
        execute_native_cli(
            &arguments,
            &context,
            CliTerminalAvailability::new(stdin_is_terminal, stdout_is_terminal),
            &mut input,
            &mut stdout,
            &mut stderr,
            &diagnostics,
        )
    };
    match result {
        Ok(exit_code) => {
            // Some local queries use nonzero for an ordinary negative answer
            // (for example `context --name` outside a session). Only executed
            // workload exits represent a failure here; Err handles CLI errors.
            if exit_code.code() != 0 && command == CliCommand::Exec {
                crate::incidents::unsuccessful_exit(command, exit_code, context.common());
            }
            diagnostics.emit(CliDiagnosticRecord::terminal(
                CliDiagnosticEvent::CommandCompleted,
                command,
                exit_code.category(),
                started.elapsed(),
            ));
            exit_code
        }
        Err(error) => {
            let exit_code = error.exit_code();
            diagnostics.emit(CliDiagnosticRecord::terminal(
                CliDiagnosticEvent::CommandFailed,
                command,
                exit_code,
                started.elapsed(),
            ));
            if CliPresenter::new(context.common())
                .write_failure_with_incident(
                    &mut stderr,
                    Some(command),
                    &error,
                    crate::incidents::failure(Some(command), &error),
                )
                .is_err()
            {
                CommonExitCode::Runtime.into()
            } else {
                exit_code.into()
            }
        }
    }
}

fn execute_daemon_command<W>(
    action: &DaemonAction,
    context: &CliInvocationContext,
    presenter: CliPresenter,
    output: &mut W,
    diagnostics: &dyn CliDiagnosticSink,
) -> Result<CommonExitCode, CliError>
where
    W: Write,
{
    let factory = NativeProductClientFactory::from_context(context);
    let configuration = resolve_configuration(&factory, diagnostics, CliCommand::Daemon)?;
    let limits = CliExecutionLimits::from_daemon(&configuration)?;
    let request_timeout = context.common().timeout.get();
    let shutdown_wait_timeout = configuration
        .maximum_drain_timeout()
        .saturating_add(request_timeout);
    let _startup_guard = if matches!(action, DaemonAction::Start | DaemonAction::Restart { .. }) {
        Some(crate::daemon_lifecycle::acquire_startup_guard(
            &configuration,
            Instant::now() + shutdown_wait_timeout,
        )?)
    } else {
        None
    };
    let port = NativeDaemonLifecyclePort::new(&factory, &configuration);
    let service =
        DaemonLifecycleService::new(&port, request_timeout, shutdown_wait_timeout, limits)
            .with_startup_timeout(configuration.startup_timeout());
    DaemonLifecycleDispatcher::new(service, presenter)
        .execute(action, output)
        .map(|outcome| outcome.exit_code)
}

fn execute_reset_command<W>(
    context: &CliInvocationContext,
    presenter: CliPresenter,
    output: &mut W,
    diagnostics: &dyn CliDiagnosticSink,
) -> Result<CommonExitCode, CliError>
where
    W: Write,
{
    let factory = NativeProductClientFactory::from_context(context);
    let configuration = resolve_configuration(&factory, diagnostics, CliCommand::Reset)?;
    require_local_reset_placement(configuration.connection().placement())?;
    let limits = CliExecutionLimits::from_daemon(&configuration)?;
    let request_timeout = context.common().timeout.get();
    // Keep lock acquisition, the request, and ownership release within one wait.
    let daemon_drain_timeout = configuration.maximum_drain_timeout().min(request_timeout);
    let deadline = Instant::now() + request_timeout;
    let _startup_guard = crate::daemon_lifecycle::acquire_startup_guard(&configuration, deadline)?;
    let request_timeout = deadline.saturating_duration_since(Instant::now());
    let shutdown_wait_timeout = request_timeout;
    let daemon_port = NativeDaemonLifecyclePort::new(&factory, &configuration);
    let daemon =
        DaemonLifecycleService::new(&daemon_port, request_timeout, shutdown_wait_timeout, limits);
    let lifecycle =
        NativeResetLifecyclePort::new(daemon, format!("{}ms", daemon_drain_timeout.as_millis()));
    let state = NativeResetStatePort::new(&configuration)?;
    let outcome = ResetService::new(&lifecycle, &state).execute()?;
    presenter.write_finite(output, &outcome, |_| reset_text(&outcome))?;
    Ok(CommonExitCode::Success)
}

fn require_local_reset_placement(
    _placement: crate::CliConnectionPlacement,
) -> Result<(), crate::CliConfigurationError> {
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn execute_connected_command<R, W, E>(
    command: &NativeCliCommand,
    context: &CliInvocationContext,
    terminals: CliTerminalAvailability,
    interactive: Option<PreparedInteractiveAttach>,
    input: &mut R,
    output: &mut W,
    error_output: &mut E,
    diagnostics: &dyn CliDiagnosticSink,
) -> Result<CliProcessExit, CliError>
where
    R: BufRead,
    W: Write,
    E: Write,
{
    let mut interactive = interactive
        .map(|preparation| -> Result<NativeAttachContext, CliError> {
            let mut terminal = NativeInteractiveTerminal::new();
            let size = terminal.size()?;
            Ok(NativeAttachContext {
                preparation,
                terminal,
                size,
            })
        })
        .transpose()?;
    let factory = NativeProductClientFactory::from_context(context);
    let configuration = resolve_configuration(&factory, diagnostics, command.command())?;
    let limits = CliExecutionLimits::from_daemon(&configuration)?;
    let start_context = prepare_start_context(command)?;
    let stdin_text = prepare_send_stdin(
        command,
        terminals.standard_input,
        input,
        configuration.request_bytes(),
    )?;
    let confirmation = prepare_clean_confirmation(
        command,
        context.common(),
        terminals.standard_input,
        input,
        error_output,
    )?;
    let client = if context
        .selection
        .as_ref()
        .is_some_and(|selection| selection.auto_start == crate::AutoStartPolicy::Never)
    {
        factory
            .connect_existing_resolved(&configuration)?
            .ok_or_else(crate::settings::unavailable)?
    } else if command.command().automatically_starts_daemon() {
        crate::daemon_lifecycle::connect_or_start(&factory, &configuration)?
    } else {
        factory.connect_resolved(&configuration)?
    };
    diagnostics.emit(CliDiagnosticRecord::command(
        CliDiagnosticEvent::ConnectionEstablished,
        command.command(),
    ));
    let diagnostic_client = DiagnosticProductClient {
        client: client.clone(),
        command: command.command(),
        diagnostics,
    };
    let result = if matches!(command, NativeCliCommand::Tui) {
        crate::tui::run(
            &diagnostic_client,
            &configuration,
            context.common(),
            context
                .selection
                .as_ref()
                .and_then(|selection| selection.profile.as_deref())
                .unwrap_or("local"),
            output,
            diagnostics,
        )
        .map(Into::into)
    } else if let NativeCliCommand::Top(arguments) = command
        && crate::top::interactive(
            terminals.standard_input,
            terminals.standard_output,
            context.common(),
        )
    {
        ConnectedStreamDispatcher::new(
            &diagnostic_client,
            context.common().timeout.get(),
            CliPresenter::new(context.common()),
        )
        .open(ConnectedStreamCommand::Top(arguments))
        .and_then(|opened| {
            diagnostics.emit(CliDiagnosticRecord::command(
                CliDiagnosticEvent::StreamOpened,
                CliCommand::Top,
            ));
            crate::top::run(
                opened,
                context.common().verbosity,
                configuration.worker_thread_stack_bytes(),
                output,
            )
            .map(Into::into)
        })
    } else {
        execute_on_client(
            command,
            &diagnostic_client,
            &configuration,
            limits,
            context.common(),
            start_context.as_ref(),
            stdin_text,
            confirmation,
            interactive.as_mut(),
            output,
            diagnostics,
        )
    };
    client.close();
    result
}

#[allow(clippy::too_many_arguments)]
fn execute_on_client<W>(
    command: &NativeCliCommand,
    client: &DiagnosticProductClient<'_>,
    configuration: &ResolvedCliConfig,
    limits: CliExecutionLimits,
    context: CliContext,
    start_context: Option<&StartPreparationContext>,
    stdin_text: Option<String>,
    confirmation: Option<RemovalConfirmationDto>,
    interactive: Option<&mut NativeAttachContext>,
    output: &mut W,
    diagnostics: &dyn CliDiagnosticSink,
) -> Result<CliProcessExit, CliError>
where
    W: Write,
{
    let presenter = CliPresenter::new(context);
    let timeout = context.timeout.get();
    let connected = ConnectedCommandContext {
        client,
        configuration,
        limits,
        timeout,
        presenter,
        diagnostics,
    };
    let finite = ConnectedFiniteDispatcher::new(client, timeout, limits, presenter);
    let finite_command = match command {
        NativeCliCommand::Start(arguments) if !arguments.attach => {
            Some(ConnectedFiniteCommand::Start(
                arguments,
                start_context.ok_or(CliError::CommandRoutingInvariant {
                    command: CliCommand::Start,
                })?,
            ))
        }
        NativeCliCommand::Stop(arguments) if !arguments.interactive() => {
            Some(ConnectedFiniteCommand::Stop(arguments))
        }
        NativeCliCommand::Restart(arguments) => Some(ConnectedFiniteCommand::Restart(arguments)),
        NativeCliCommand::Ls(arguments) => Some(ConnectedFiniteCommand::List(arguments)),
        NativeCliCommand::Inspect(arguments) => Some(ConnectedFiniteCommand::Inspect(arguments)),
        NativeCliCommand::Send(arguments) => {
            Some(ConnectedFiniteCommand::Send(arguments, stdin_text))
        }
        NativeCliCommand::Clean(arguments) => Some(ConnectedFiniteCommand::Clean(
            arguments,
            confirmation.ok_or(CliError::ApprovalRequired {
                command: CliCommand::Clean,
            })?,
        )),
        NativeCliCommand::Peek(arguments) => Some(ConnectedFiniteCommand::Peek(arguments)),
        NativeCliCommand::Explain(arguments) => {
            Some(ConnectedFiniteCommand::Explain(&arguments.name))
        }
        NativeCliCommand::Profile(arguments) => Some(ConnectedFiniteCommand::Profile(arguments)),
        NativeCliCommand::Debug(arguments) => Some(ConnectedFiniteCommand::Debug(arguments)),
        _ => None,
    };
    if let Some(command) = finite_command {
        return finite
            .execute(command, output)
            .map(|outcome| outcome.exit_code.into());
    }

    match command {
        NativeCliCommand::Stop(arguments) => crate::stop::run(
            client,
            context,
            limits,
            !arguments.graceful,
            configuration.worker_thread_stack_bytes(),
            output,
        )
        .map(Into::into),
        NativeCliCommand::Group(arguments) => crate::group::execute(
            arguments,
            client,
            configuration,
            limits,
            context,
            output,
            diagnostics,
        )
        .map(Into::into),
        NativeCliCommand::Start(arguments) if arguments.attach => {
            let context = start_context.ok_or(CliError::CommandRoutingInvariant {
                command: CliCommand::Start,
            })?;
            let result =
                FiniteCliExecutor::new(client, timeout, limits).start(arguments, context)?;
            presenter.write_finite(output, &result, |_| {
                session_mutation_text(&result, "started")
            })?;
            let attach = interactive.ok_or(CliError::CommandRoutingInvariant {
                command: CliCommand::Attach,
            })?;
            let opened = InteractiveAttachExecutor::new(client, timeout, limits)
                .with_scrollback(configuration.document().terminal.scrollback)
                .open_session(
                    result.session.id,
                    attach.preparation.initial.clone(),
                    attach.size,
                )?;
            execute_interactive_attach(
                opened,
                attach,
                configuration.worker_thread_stack_bytes(),
                output,
                diagnostics,
            )
            .map(Into::into)
        }
        NativeCliCommand::Attach(arguments) => {
            let attach = interactive.ok_or(CliError::CommandRoutingInvariant {
                command: CliCommand::Attach,
            })?;
            let executor = InteractiveAttachExecutor::new(client, timeout, limits)
                .with_scrollback(configuration.document().terminal.scrollback);
            let opened = if let Some(name) = &arguments.name {
                executor.open_name(name, attach.preparation.initial.clone(), attach.size)?
            } else {
                match crate::attach::picker::run(
                    &executor,
                    attach.preparation.initial.clone(),
                    &mut attach.terminal,
                    output,
                    configuration.worker_thread_stack_bytes(),
                )? {
                    crate::attach::picker::Outcome::Attached(opened) => *opened,
                    crate::attach::picker::Outcome::Exit(code) => return Ok(code.into()),
                }
            };
            execute_interactive_attach(
                opened,
                attach,
                configuration.worker_thread_stack_bytes(),
                output,
                diagnostics,
            )
            .map(Into::into)
        }
        NativeCliCommand::Exec(arguments) => {
            execute_exec(arguments, client, timeout, limits, output)
        }
        NativeCliCommand::Wait(arguments) => {
            ConnectedStreamDispatcher::new(client, timeout, presenter)
                .wait(arguments, output)
                .map(Into::into)
        }
        NativeCliCommand::Watch(arguments) => consume_opened_stream(
            ConnectedStreamDispatcher::new(client, timeout, presenter)
                .open(ConnectedStreamCommand::Watch(arguments))?,
            CliCommand::Watch,
            configuration.worker_thread_stack_bytes(),
            presenter,
            output,
            diagnostics,
        )
        .map(Into::into),
        NativeCliCommand::Top(arguments) => consume_opened_stream(
            ConnectedStreamDispatcher::new(client, timeout, presenter)
                .open(ConnectedStreamCommand::Top(arguments))?,
            CliCommand::Top,
            configuration.worker_thread_stack_bytes(),
            presenter,
            output,
            diagnostics,
        )
        .map(Into::into),
        NativeCliCommand::Logs(arguments) => {
            execute_logs(arguments, &connected, output).map(Into::into)
        }
        _ => Err(CliError::CommandRoutingInvariant {
            command: command.command(),
        }),
    }
}

fn execute_exec<W: Write>(
    arguments: &crate::ExecArguments,
    client: &DiagnosticProductClient<'_>,
    timeout: Duration,
    limits: CliExecutionLimits,
    output: &mut W,
) -> Result<CliProcessExit, CliError> {
    let result = FiniteCliExecutor::new(client, timeout, limits).execute_in_session(arguments)?;
    let stdout = complete_exec_output(&result.stdout, "stdout")?;
    let stderr = complete_exec_output(&result.stderr, "stderr")?;
    output.write_all(stdout).map_err(CliOutputError::from)?;
    std::io::stderr()
        .lock()
        .write_all(stderr)
        .map_err(CliOutputError::from)?;
    Ok(CliProcessExit::executed(result.exit_code))
}

fn complete_exec_output<'a>(
    output: &'a ExecutionOutputDto,
    channel: &'static str,
) -> Result<&'a [u8], CliError> {
    let ExecutionOutputDto::Inline {
        bytes,
        payload_digest_sha256,
        truncation,
    } = output
    else {
        return Err(CliError::ExecOutputUnavailable { channel });
    };
    if truncation.is_some() {
        return Err(CliError::ExecOutputUnavailable { channel });
    }
    if !undercurrent_protocol::verify_execution_output_payload_digest(bytes, payload_digest_sha256)
    {
        return Err(CliError::ExecOutputIntegrity { channel });
    }
    Ok(bytes)
}

fn execute_interactive_attach<W>(
    opened: OpenedInteractiveAttach<'_>,
    context: &mut NativeAttachContext,
    worker_stack_bytes: usize,
    output: &mut W,
    diagnostics: &dyn CliDiagnosticSink,
) -> Result<CommonExitCode, CliError>
where
    W: Write,
{
    diagnostics.emit(CliDiagnosticRecord::command(
        CliDiagnosticEvent::StreamOpened,
        CliCommand::Attach,
    ));
    let subscription = opened.terminal_subscription();
    let cancellation = CliStreamCancellation::default();
    let guard = match NativeStreamSignalGuard::install(
        subscription.clone(),
        cancellation.clone(),
        worker_stack_bytes,
    ) {
        Ok(guard) => guard,
        Err(error) => {
            subscription.cancel();
            return Err(error.into());
        }
    };
    let detach = context
        .preparation
        .detach
        .take()
        .ok_or(CliError::CommandRoutingInvariant {
            command: CliCommand::Attach,
        })?;
    let result = opened.consume(
        &mut context.terminal,
        output,
        &cancellation,
        diagnostics,
        context.preparation.read_only,
        detach,
    );
    let finish = guard.finish().map_err(CliError::from);
    match result {
        Err(error) => Err(error),
        Ok(exit_code) => {
            finish?;
            Ok(exit_code)
        }
    }
}

fn execute_logs<W>(
    arguments: &crate::LogsArguments,
    context: &ConnectedCommandContext<'_>,
    output: &mut W,
) -> Result<CommonExitCode, CliError>
where
    W: Write,
{
    let executor = FiniteCliExecutor::new(context.client, context.timeout, context.limits);
    let session_id = executor.resolve_name(&arguments.name)?;
    let page = executor.terminal_output_id(
        session_id.clone(),
        arguments.from_offset,
        arguments.max_bytes.unwrap_or(65_536),
    )?;
    if !arguments.follow {
        context
            .presenter
            .write_terminal_output_page(output, &page)?;
        return Ok(CommonExitCode::Success);
    }
    context
        .presenter
        .write_terminal_output_stream_page(output, &page)?;
    let after_offset = terminal_output_next_offset(&page);
    let opened =
        ConnectedStreamDispatcher::new(context.client, context.timeout, context.presenter).open(
            ConnectedStreamCommand::LogsFollowSession(&session_id, after_offset),
        )?;
    consume_opened_stream(
        opened,
        CliCommand::Logs,
        context.configuration.worker_thread_stack_bytes(),
        context.presenter,
        output,
        context.diagnostics,
    )
}

fn consume_opened_stream<W>(
    opened: crate::OpenedConnectedStream,
    command: CliCommand,
    worker_stack_bytes: usize,
    presenter: CliPresenter,
    output: &mut W,
    diagnostics: &dyn CliDiagnosticSink,
) -> Result<CommonExitCode, CliError>
where
    W: Write,
{
    diagnostics.emit(CliDiagnosticRecord::command(
        CliDiagnosticEvent::StreamOpened,
        command,
    ));
    let subscription = opened.subscription();
    let cancellation = CliStreamCancellation::default();
    let guard = match NativeStreamSignalGuard::install(
        subscription.clone(),
        cancellation.clone(),
        worker_stack_bytes,
    ) {
        Ok(guard) => guard,
        Err(error) => {
            subscription.cancel();
            return Err(error.into());
        }
    };
    let result = opened.consume(&cancellation, presenter, output);
    let finish = guard.finish().map_err(CliError::from);
    match result {
        Err(error) => Err(error),
        Ok(exit_code) => {
            finish?;
            Ok(exit_code)
        }
    }
}

fn resolve_configuration(
    factory: &NativeProductClientFactory,
    diagnostics: &dyn CliDiagnosticSink,
    command: CliCommand,
) -> Result<ResolvedCliConfig, CliError> {
    let configuration = factory.resolve()?;
    diagnostics.emit(CliDiagnosticRecord::command(
        CliDiagnosticEvent::ConfigurationResolved,
        command,
    ));
    Ok(configuration)
}

fn prepare_start_context(
    command: &NativeCliCommand,
) -> Result<Option<StartPreparationContext>, CliError> {
    if !matches!(command, NativeCliCommand::Start(_)) {
        return Ok(None);
    }
    let current_directory = std::env::current_dir().map_err(|source| CliError::Process {
        operation: CliProcessOperation::ResolveCurrentDirectory,
        source,
    })?;
    let default_shell = Some(crate::preparation::native_default_shell());
    StartPreparationContext::new(current_directory, default_shell)
        .map(Some)
        .map_err(Into::into)
}

fn prepare_send_stdin<R>(
    command: &NativeCliCommand,
    stdin_is_terminal: bool,
    input: &mut R,
    maximum: usize,
) -> Result<Option<String>, CliError>
where
    R: BufRead,
{
    let NativeCliCommand::Send(arguments) = command else {
        return Ok(None);
    };
    if arguments.eof {
        return Ok(None);
    }
    if let Some(text) = &arguments.input {
        if text.is_empty() && arguments.key.is_empty() {
            return Err(CliInputError::MissingTerminalInput.into());
        }
        return Ok(None);
    }
    if !arguments.key.is_empty() {
        return Ok(None);
    }
    if stdin_is_terminal {
        return Err(CliInputError::MissingTerminalInput.into());
    }
    let limit = u64::try_from(maximum).unwrap_or(u64::MAX);
    let mut bytes = Vec::new();
    input
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|source| CliError::StandardInput {
            operation: CliStandardInputOperation::ReadTerminalInput,
            source,
        })?;
    if bytes.len() > maximum {
        return Err(CliInputError::StdinTooLarge { maximum }.into());
    }
    let text = String::from_utf8(bytes).map_err(|_| CliInputError::StdinNotUtf8)?;
    if text.is_empty() {
        return Err(CliInputError::MissingTerminalInput.into());
    }
    Ok(Some(text))
}

fn prepare_clean_confirmation<R, E>(
    command: &NativeCliCommand,
    context: CliContext,
    stdin_is_terminal: bool,
    input: &mut R,
    error_output: &mut E,
) -> Result<Option<RemovalConfirmationDto>, CliError>
where
    R: BufRead,
    E: Write,
{
    let NativeCliCommand::Clean(arguments) = command else {
        return Ok(None);
    };
    require_destructive_confirmation(
        CliCommand::Clean,
        arguments.yes,
        "remove selected terminal sessions? [y/N] ",
        context,
        stdin_is_terminal,
        input,
        error_output,
    )?;
    Ok(Some(if arguments.yes {
        RemovalConfirmationDto::Forced
    } else {
        RemovalConfirmationDto::Confirmed
    }))
}

#[allow(clippy::too_many_arguments)]
fn require_destructive_confirmation<R, E>(
    command: CliCommand,
    forced: bool,
    prompt: &str,
    context: CliContext,
    stdin_is_terminal: bool,
    input: &mut R,
    error_output: &mut E,
) -> Result<(), CliError>
where
    R: BufRead,
    E: Write,
{
    if forced {
        return Ok(());
    }
    if !stdin_is_terminal || context.output != OutputFormat::Text {
        return Err(CliError::ApprovalRequired { command });
    }
    write!(error_output, "{prompt}")
        .and_then(|()| error_output.flush())
        .map_err(CliOutputError::from)?;
    let mut answer = String::new();
    input
        .take(CONFIRMATION_BYTES)
        .read_line(&mut answer)
        .map_err(|source| CliError::StandardInput {
            operation: CliStandardInputOperation::ReadConfirmation,
            source,
        })?;
    if matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
        Ok(())
    } else {
        Err(CliError::ApprovalRequired { command })
    }
}

fn validate_output_shape(command: &NativeCliCommand, context: CliContext) -> Result<(), CliError> {
    if (matches!(command, NativeCliCommand::Exec(_))
        || matches!(command, NativeCliCommand::Init(arguments) if arguments.print))
        && context.output != OutputFormat::Text
    {
        return Err(CliError::TextDocumentRequiresText {
            command: command.command(),
        });
    }
    let shape = match command {
        NativeCliCommand::Start(arguments) if arguments.attach => OutputShape::Stream,
        NativeCliCommand::Group(arguments) if arguments.interactive() => OutputShape::Stream,
        NativeCliCommand::Attach(_)
        | NativeCliCommand::Tui
        | NativeCliCommand::Top(_)
        | NativeCliCommand::Watch(_)
        | NativeCliCommand::Logs(crate::LogsArguments { follow: true, .. }) => OutputShape::Stream,
        _ => OutputShape::Finite,
    };
    context.validate_output_shape(shape).map_err(Into::into)
}

fn prepare_interactive_attach(
    command: &NativeCliCommand,
    context: CliContext,
    stdin_is_terminal: bool,
    stdout_is_terminal: bool,
) -> Result<Option<PreparedInteractiveAttach>, CliError> {
    let (initial, read_only, detach_keys) = match command {
        NativeCliCommand::Attach(arguments) => (
            arguments
                .from_offset
                .map_or(TerminalInitialModeDto::Screen, |after_offset| {
                    TerminalInitialModeDto::Resume { after_offset }
                }),
            arguments.read_only,
            arguments.detach_keys.as_str(),
        ),
        NativeCliCommand::Start(arguments) if arguments.attach => (
            match arguments.output_capture {
                OutputCaptureModeArgument::Terminal => TerminalInitialModeDto::Screen,
                OutputCaptureModeArgument::Opaque => {
                    TerminalInitialModeDto::Resume { after_offset: 0 }
                }
            },
            false,
            arguments.detach_keys.as_str(),
        ),
        _ => return Ok(None),
    };
    if context.output != OutputFormat::Text {
        return Err(CliError::RawOutputRequiresText);
    }
    if !stdin_is_terminal {
        return Err(CliInputError::InteractiveTerminalRequired {
            stream: CliTerminalStream::StandardInput,
        }
        .into());
    }
    if !stdout_is_terminal {
        return Err(CliInputError::InteractiveTerminalRequired {
            stream: CliTerminalStream::StandardOutput,
        }
        .into());
    }
    let detach = if detach_keys == "default" {
        crate::shortcuts::matcher(&undercurrent_config::ShortcutPreferences::default())?
    } else {
        DetachMatcher::parse(detach_keys)
            .map_err(|source| CliInputError::InvalidDetachKeys { source })?
    };
    Ok(Some(PreparedInteractiveAttach {
        initial,
        read_only,
        detach: Some(detach),
    }))
}

fn validate_stop_picker(
    command: &NativeCliCommand,
    context: CliContext,
    terminals: CliTerminalAvailability,
) -> Result<(), CliError> {
    if matches!(command, NativeCliCommand::Stop(arguments) if arguments.interactive()) {
        // Automation must provide a target rather than entering an interactive picker.
        if context.output != OutputFormat::Text
            || !terminals.standard_input
            || !terminals.standard_output
        {
            return Err(CliInputError::StopPickerRequiresSelection.into());
        }
    }
    Ok(())
}

fn fallback_context(arguments: &[OsString]) -> Result<CliContext, crate::CliContextError> {
    let has = |flag: &str| arguments.iter().any(|value| value == OsStr::new(flag));
    let output = match (has("--json"), has("--jsonl")) {
        (true, false) => OutputFormat::Json,
        (false, true) => OutputFormat::Jsonl,
        _ => OutputFormat::Text,
    };
    let verbosity = if has("--quiet") || has("-q") {
        Verbosity::Quiet
    } else if has("--diagnostic") {
        Verbosity::Diagnostic
    } else if has("--verbose") || has("-v") {
        Verbosity::Verbose
    } else {
        Verbosity::Normal
    };
    let timeout = CommandTimeout::new(crate::DEFAULT_CLI_TIMEOUT, crate::DEFAULT_CLI_TIMEOUT)
        .map_err(crate::CliContextError::Timeout)?;
    Ok(CliContext::new(output, verbosity, timeout))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::DisabledCliDiagnosticSink;

    #[test]
    fn default_profile_applies_only_when_start_has_no_explicit_profile() {
        for (args, expected) in [
            (
                vec!["uc", "start", "worker", "--", "sleep", "1"],
                Some("codex"),
            ),
            (
                vec![
                    "uc",
                    "start",
                    "worker",
                    "--profile",
                    "opencode",
                    "--",
                    "sleep",
                    "1",
                ],
                Some("opencode"),
            ),
        ] {
            let mut parsed = NativeCliArguments::try_parse_from(args).unwrap();
            apply_default_profile(&mut parsed, Some("codex"));
            let NativeCliCommand::Start(start) = parsed.command else {
                panic!("start");
            };
            assert_eq!(start.profile.as_deref(), expected);
        }
    }

    #[test]
    fn local_reset_uses_the_only_supported_placement() {
        require_local_reset_placement(crate::CliConnectionPlacement::Local).expect("local reset");
    }

    #[test]
    fn interactive_command_conflict_is_rejected_before_native_io() {
        let arguments = NativeCliArguments::try_parse_from([
            "uc",
            "start",
            "worker",
            "--interactive",
            "--",
            "private-command",
            "private-argument",
        ])
        .expect("start arguments");
        let context = CliInvocationContext::from_options(arguments.global_options())
            .expect("invocation context");

        let error = execute_native_cli(
            &arguments,
            &context,
            CliTerminalAvailability::new(false, false),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
            &mut Vec::new(),
            &DisabledCliDiagnosticSink,
        )
        .expect_err("interactive command must be rejected");

        assert!(matches!(
            error,
            CliError::Input(CliInputError::InteractiveCommandConflict)
        ));
    }

    #[test]
    fn local_events_command_executes_without_configuration_or_connection() {
        let arguments =
            NativeCliArguments::try_parse_from(["uc", "events"]).expect("events arguments");
        let context = CliInvocationContext::from_options(arguments.global_options())
            .expect("invocation context");
        let mut output = Vec::new();

        let exit = execute_native_cli(
            &arguments,
            &context,
            CliTerminalAvailability::new(false, false),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut output,
            &mut Vec::new(),
            &DisabledCliDiagnosticSink,
        )
        .expect("events execution");

        assert_eq!(exit, CommonExitCode::Success);
        assert!(
            String::from_utf8(output)
                .expect("UTF-8")
                .contains("cli.command.started")
        );
    }

    #[test]
    fn local_init_prints_exact_hook_without_configuration_or_connection() {
        let arguments = NativeCliArguments::try_parse_from(["uc", "init", "fish", "--print"])
            .expect("init arguments");
        let context = CliInvocationContext::from_options(arguments.global_options())
            .expect("invocation context");
        let mut output = Vec::new();

        let exit = execute_native_cli(
            &arguments,
            &context,
            CliTerminalAvailability::new(false, false),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut output,
            &mut Vec::new(),
            &DisabledCliDiagnosticSink,
        )
        .expect("init print execution");

        assert_eq!(exit, CommonExitCode::Success);
        assert_eq!(
            output,
            crate::shell_hook_document(crate::ShellArgument::Fish)
        );

        let structured =
            NativeCliArguments::try_parse_from(["uc", "--json", "init", "bash", "--print"])
                .expect("structured init arguments");
        let structured_context =
            CliInvocationContext::from_options(structured.global_options()).expect("context");
        assert!(matches!(
            execute_native_cli(
                &structured,
                &structured_context,
                CliTerminalAvailability::new(false, false),
                &mut Cursor::new(Vec::<u8>::new()),
                &mut Vec::new(),
                &mut Vec::new(),
                &DisabledCliDiagnosticSink,
            ),
            Err(CliError::TextDocumentRequiresText {
                command: CliCommand::Init
            })
        ));
    }

    #[test]
    fn stop_picker_requires_text_and_both_terminals_before_connection() {
        let text = fallback_context(&[]).unwrap();
        for args in [vec!["uc", "stop"], vec!["uc", "stop", "--force"]] {
            let parsed = NativeCliArguments::try_parse_from(args).unwrap();
            for output in [OutputFormat::Text, OutputFormat::Json, OutputFormat::Jsonl] {
                for (stdin, stdout) in [(false, false), (false, true), (true, false), (true, true)]
                {
                    let context = CliContext::new(output, text.verbosity, text.timeout);
                    let result = validate_stop_picker(
                        &parsed.command,
                        context,
                        CliTerminalAvailability::new(stdin, stdout),
                    );
                    if output == OutputFormat::Text && stdin && stdout {
                        assert!(result.is_ok());
                    } else {
                        assert!(matches!(
                            result,
                            Err(CliError::Input(CliInputError::StopPickerRequiresSelection))
                        ));
                    }
                }
            }
        }
        for args in [
            vec!["uc", "stop", "worker"],
            vec!["uc", "stop", "--all"],
            vec!["uc", "stop", "--label", "project=demo"],
        ] {
            let parsed = NativeCliArguments::try_parse_from(args).unwrap();
            assert!(
                validate_stop_picker(
                    &parsed.command,
                    CliContext::new(OutputFormat::Json, text.verbosity, text.timeout),
                    CliTerminalAvailability::new(false, false)
                )
                .is_ok()
            );
        }
    }

    #[test]
    fn unnamed_attach_accepts_options_and_requires_text_and_both_terminals() {
        let parsed = NativeCliArguments::try_parse_from([
            "uc",
            "attach",
            "--read-only",
            "--from-offset",
            "42",
            "--detach-keys",
            "ctrl-b,d",
        ])
        .unwrap();
        assert!(matches!(&parsed.command, NativeCliCommand::Attach(a) if a.name.is_none()));
        let text = fallback_context(&[]).unwrap();
        let prepared = prepare_interactive_attach(&parsed.command, text, true, true)
            .unwrap()
            .unwrap();
        assert!(prepared.read_only);
        assert_eq!(
            prepared.initial,
            TerminalInitialModeDto::Resume { after_offset: 42 }
        );
        assert!(prepared.detach.is_some());
        for (stdin, stdout) in [(false, true), (true, false)] {
            assert!(matches!(
                prepare_interactive_attach(&parsed.command, text, stdin, stdout),
                Err(CliError::Input(
                    CliInputError::InteractiveTerminalRequired { .. }
                ))
            ));
        }
        for output in [OutputFormat::Json, OutputFormat::Jsonl] {
            let context = CliContext::new(output, text.verbosity, text.timeout);
            assert!(matches!(
                prepare_interactive_attach(&parsed.command, context, true, true),
                Err(CliError::RawOutputRequiresText)
            ));
        }
        let bare = NativeCliArguments::try_parse_from(["uc", "attach"]).unwrap();
        assert!(matches!(bare.command, NativeCliCommand::Attach(a) if a.name.is_none()));
    }

    #[test]
    fn attach_preflight_validates_output_ttys_and_detach_keys_before_connection() {
        let parsed = NativeCliArguments::try_parse_from([
            "uc",
            "attach",
            "worker",
            "--read-only",
            "--from-offset",
            "42",
            "--detach-keys",
            "ctrl-a,x",
        ])
        .expect("attach arguments");
        let text = fallback_context(&[]).expect("text context");
        let prepared = prepare_interactive_attach(&parsed.command, text, true, true)
            .expect("valid preflight")
            .expect("interactive command");
        assert!(prepared.read_only);
        assert_eq!(
            prepared.initial,
            TerminalInitialModeDto::Resume { after_offset: 42 }
        );

        assert!(matches!(
            prepare_interactive_attach(&parsed.command, text, false, true),
            Err(CliError::Input(
                CliInputError::InteractiveTerminalRequired {
                    stream: CliTerminalStream::StandardInput
                }
            ))
        ));
        assert!(matches!(
            prepare_interactive_attach(&parsed.command, text, true, false),
            Err(CliError::Input(
                CliInputError::InteractiveTerminalRequired {
                    stream: CliTerminalStream::StandardOutput
                }
            ))
        ));

        let jsonl = CliContext::new(OutputFormat::Jsonl, text.verbosity, text.timeout);
        assert!(matches!(
            prepare_interactive_attach(&parsed.command, jsonl, true, true),
            Err(CliError::RawOutputRequiresText)
        ));

        let invalid = NativeCliArguments::try_parse_from([
            "uc",
            "attach",
            "worker",
            "--detach-keys",
            "private-invalid-value",
        ])
        .expect("structurally parsed arguments");
        let error = prepare_interactive_attach(&invalid.command, text, true, true)
            .err()
            .expect("invalid detach keys");
        assert!(matches!(
            error,
            CliError::Input(CliInputError::InvalidDetachKeys { .. })
        ));
        assert!(!error.to_string().contains("private-invalid-value"));
    }

    #[test]
    fn attached_start_is_a_text_stream_with_default_shortcuts() {
        let parsed =
            NativeCliArguments::try_parse_from(["uc", "start", "worker", "--attach", "--", "sh"])
                .expect("attached start arguments");
        let text = fallback_context(&[]).expect("text context");

        validate_output_shape(&parsed.command, text).expect("stream output shape");
        let prepared = prepare_interactive_attach(&parsed.command, text, true, true)
            .expect("attached start preflight")
            .expect("interactive command");
        assert_eq!(prepared.initial, TerminalInitialModeDto::Screen);
        assert!(!prepared.read_only);
        assert!(prepared.detach.is_some());
        assert_eq!(prepared.detach.as_ref().unwrap().prefix_label(), "Alt+U");

        let opaque = NativeCliArguments::try_parse_from([
            "uc",
            "start",
            "binary-worker",
            "--attach",
            "--output-capture",
            "opaque",
            "--",
            "cat",
        ])
        .expect("attached opaque start arguments");
        let opaque = prepare_interactive_attach(&opaque.command, text, true, true)
            .expect("opaque attached start preflight")
            .expect("interactive command");
        assert_eq!(
            opaque.initial,
            TerminalInitialModeDto::Resume { after_offset: 0 }
        );
    }

    #[test]
    fn piped_send_input_is_bounded_and_requires_utf8() {
        let command = NativeCliArguments::try_parse_from(["uc", "send", "worker"])
            .expect("send arguments")
            .command;

        assert!(matches!(
            prepare_send_stdin(&command, false, &mut Cursor::new(b"12345"), 4),
            Err(CliError::Input(CliInputError::StdinTooLarge { maximum: 4 }))
        ));
        assert!(matches!(
            prepare_send_stdin(&command, false, &mut Cursor::new([0xff]), 4),
            Err(CliError::Input(CliInputError::StdinNotUtf8))
        ));
        assert_eq!(
            prepare_send_stdin(&command, false, &mut Cursor::new(b"ok"), 4)
                .expect("bounded stdin")
                .as_deref(),
            Some("ok")
        );
        assert!(matches!(
            prepare_send_stdin(&command, false, &mut Cursor::new([]), 4),
            Err(CliError::Input(CliInputError::MissingTerminalInput))
        ));
        assert!(matches!(
            prepare_send_stdin(&command, true, &mut Cursor::new(b"ignored"), 4),
            Err(CliError::Input(CliInputError::MissingTerminalInput))
        ));
    }

    #[test]
    fn typed_standard_input_close_never_reads_process_stdin() {
        let command = NativeCliArguments::try_parse_from(["uc", "send", "worker", "--eof"])
            .expect("typed standard-input close arguments")
            .command;
        let mut input = Cursor::new(b"must-not-be-consumed");

        assert_eq!(
            prepare_send_stdin(&command, false, &mut input, 4).expect("no stdin preparation"),
            None
        );
        assert_eq!(input.position(), 0);
    }

    #[test]
    fn default_clean_prompts_and_yes_flags_skip_input() {
        let context = fallback_context(&[]).expect("fallback context");
        for args in [
            vec!["uc", "clean"],
            vec!["uc", "clean", "--yes"],
            vec!["uc", "clean", "-y"],
        ] {
            let command = NativeCliArguments::try_parse_from(&args)
                .expect("clean arguments")
                .command;
            let forced = args.len() > 2;
            let mut input = Cursor::new(b"yes\n");
            let mut prompt = Vec::new();
            assert_eq!(
                prepare_clean_confirmation(&command, context, !forced, &mut input, &mut prompt)
                    .expect("clean confirmation"),
                Some(if forced {
                    RemovalConfirmationDto::Forced
                } else {
                    RemovalConfirmationDto::Confirmed
                })
            );
            if forced {
                assert!(prompt.is_empty());
                assert_eq!(input.position(), 0);
            } else {
                assert_eq!(prompt, b"remove selected terminal sessions? [y/N] ");
                assert!(input.position() > 0);
            }
        }
    }

    #[test]
    fn clean_requires_machine_confirmation_and_accepts_bounded_tty_yes() {
        let command = NativeCliArguments::try_parse_from(["uc", "clean", "worker"])
            .expect("clean arguments")
            .command;
        let text = fallback_context(&[]).expect("fallback context");
        assert_eq!(
            prepare_clean_confirmation(
                &command,
                text,
                true,
                &mut Cursor::new(b"yes\n"),
                &mut Vec::new(),
            )
            .expect("confirmation"),
            Some(RemovalConfirmationDto::Confirmed)
        );

        let json = CliContext::new(OutputFormat::Json, text.verbosity, text.timeout);
        assert!(matches!(
            prepare_clean_confirmation(
                &command,
                json,
                true,
                &mut Cursor::new(b"yes\n"),
                &mut Vec::new(),
            ),
            Err(CliError::ApprovalRequired {
                command: CliCommand::Clean
            })
        ));

        let mut prompt = Vec::new();
        require_destructive_confirmation(
            CliCommand::Reset,
            false,
            "reset? ",
            text,
            true,
            &mut Cursor::new(b"y\n"),
            &mut prompt,
        )
        .expect("interactive reset confirmation");
        assert_eq!(prompt, b"reset? ");
        require_destructive_confirmation(
            CliCommand::Reset,
            true,
            "reset? ",
            json,
            false,
            &mut Cursor::new([]),
            &mut Vec::new(),
        )
        .expect("forced machine reset confirmation");
        assert!(matches!(
            require_destructive_confirmation(
                CliCommand::Reset,
                false,
                "reset? ",
                json,
                false,
                &mut Cursor::new(b"yes\n"),
                &mut Vec::new(),
            ),
            Err(CliError::ApprovalRequired {
                command: CliCommand::Reset
            })
        ));
    }
}

#[cfg(test)]
mod configuration_preferences_tests {
    use super::*;

    #[test]
    fn saved_defaults_preserve_explicit_flags_even_when_equal_to_old_defaults() {
        let preferences = undercurrent_config::CliPreferences {
            list_page_size: 17,
            logs_max_bytes: 4096,
            ..Default::default()
        };
        for explicit in [false, true] {
            let mut args = vec!["uc", "logs", "example"];
            if explicit {
                args.extend(["--max-bytes", "65536"]);
            }
            let mut parsed = NativeCliArguments::try_parse_from(args).unwrap();
            apply_cli_preferences(&mut parsed, preferences);
            let NativeCliCommand::Logs(logs) = parsed.command else {
                unreachable!()
            };
            assert_eq!(logs.max_bytes, Some(if explicit { 65536 } else { 4096 }));
        }
        for command in ["ls", "top", "watch"] {
            for explicit in [false, true] {
                let mut args = vec!["uc", command];
                if explicit {
                    args.extend(["--page-size", "100"]);
                }
                let mut parsed = NativeCliArguments::try_parse_from(args).unwrap();
                apply_cli_preferences(&mut parsed, preferences);
                let actual = match parsed.command {
                    NativeCliCommand::Ls(a) => a.page_size,
                    NativeCliCommand::Top(a) => a.page_size,
                    NativeCliCommand::Watch(a) => a.page_size,
                    _ => unreachable!(),
                };
                assert_eq!(actual, Some(if explicit { 100 } else { 17 }));
            }
        }
    }
}
