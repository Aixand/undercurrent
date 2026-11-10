// SPDX-License-Identifier: Apache-2.0

//! `uc-daemon` process entry point.

use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt::{Display, Formatter};
use std::process::ExitCode;
use std::thread;
use std::time::Duration;

#[cfg(unix)]
use signal_hook::consts::signal::{SIGINT, SIGTERM};
#[cfg(unix)]
use signal_hook::iterator::Signals;
use undercurrent_daemon::ConfigDiscovery;
use undercurrent_daemon::{
    DaemonConfigError, DaemonConfigOverrides, DaemonLogEventName, DaemonLoggingError,
    DaemonProcess, DaemonProcessRunError, DaemonProcessStartError, emit_daemon_log_event,
    install_daemon_logging, load_daemon_config,
};

const SIGNAL_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const HELP: &str = "Usage: uc-daemon [OPTIONS]

Options:
  -h, --help             Print help
  -V, --version          Print version
";

fn main() -> ExitCode {
    undercurrent_daemon::initialize_failure_evidence(undercurrent_diagnostics::Component::Daemon);
    match run(std::env::args_os().skip(1)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            undercurrent_daemon::record_process_failure("daemon_process", error.category(), &error);
            eprintln!("uc-daemon: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(arguments: impl IntoIterator<Item = OsString>) -> Result<(), DaemonMainError> {
    let arguments = DaemonArguments::parse(arguments)?;
    match arguments.action {
        DaemonArgumentAction::Help => {
            print!("{HELP}");
            return Ok(());
        }
        DaemonArgumentAction::Version => {
            println!("uc-daemon {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        DaemonArgumentAction::Run => {}
    }
    if let Some(recorder) = undercurrent_diagnostics::process() {
        let _ = recorder.save_build();
    }

    let discovery = ConfigDiscovery::from_process_env(None);
    let overrides = DaemonConfigOverrides::new();
    let config = load_daemon_config(&discovery, overrides)?;
    undercurrent_daemon::prepare_private_directory(config.paths().product_home())
        .map_err(DaemonMainError::PrepareRuntime)
        .inspect_err(log_main_failure)?;
    undercurrent_daemon::prepare_instance_scope_parent(config.paths().instance_lock())
        .map_err(DaemonMainError::PrepareRuntime)
        .inspect_err(log_main_failure)?;
    let logging = install_daemon_logging(
        config.paths().log(),
        config.logging(),
        config.telemetry().debug_enabled,
    )?;
    emit_daemon_log_event(DaemonLogEventName::ProcessStarting, None);
    let drain_timeout =
        SIGNAL_DRAIN_TIMEOUT.min(config.transport().operations().maximum_drain_timeout());
    let signal_stack_bytes = config.worker_thread_stack_bytes();
    #[cfg(unix)]
    let signals = Signals::new([SIGINT, SIGTERM]);
    #[cfg(windows)]
    let signals = undercurrent_platform_windows::ConsoleSignals::new();
    let mut signals = signals
        .map_err(DaemonMainError::Signals)
        .inspect_err(log_main_failure)?;
    let signal_handle = signals.handle();
    let process = DaemonProcess::compose_with_log_pressure(config, logging.pressure())
        .map_err(DaemonMainError::Start)
        .inspect_err(log_main_failure)?;
    let control = process.control();
    emit_daemon_log_event(DaemonLogEventName::ProcessReady, None);
    let signal_worker = thread::Builder::new()
        .name("undercurrent-signals".to_owned())
        .stack_size(signal_stack_bytes)
        .spawn(move || {
            let mut graceful_requested = false;
            for _signal in signals.forever() {
                if graceful_requested {
                    emit_daemon_log_event(DaemonLogEventName::ShutdownForced, None);
                    control.force_shutdown();
                    break;
                }
                emit_daemon_log_event(DaemonLogEventName::ShutdownRequested, None);
                if control.begin_shutdown(drain_timeout).is_err() {
                    emit_daemon_log_event(
                        DaemonLogEventName::ShutdownForced,
                        Some("shutdown_control"),
                    );
                    control.force_shutdown();
                    break;
                }
                graceful_requested = true;
            }
        })
        .map_err(DaemonMainError::SignalWorkerStart)
        .inspect_err(log_main_failure)?;

    let process_result = process.run();
    signal_handle.close();
    let signal_result = signal_worker.join();
    process_result
        .map_err(DaemonMainError::Run)
        .inspect_err(log_main_failure)?;
    signal_result
        .map_err(|_| DaemonMainError::SignalWorkerPanicked)
        .inspect_err(log_main_failure)?;
    emit_daemon_log_event(DaemonLogEventName::ProcessStopped, None);
    Ok(())
}

fn log_main_failure(error: &DaemonMainError) {
    emit_daemon_log_event(DaemonLogEventName::ProcessFailed, Some(error.category()));
}

#[derive(Debug, Default, PartialEq, Eq)]
struct DaemonArguments {
    action: DaemonArgumentAction,
}

impl DaemonArguments {
    fn parse(arguments: impl IntoIterator<Item = OsString>) -> Result<Self, DaemonArgumentError> {
        let mut parsed = Self::default();
        for argument in arguments {
            match argument.as_os_str() {
                value if value == OsStr::new("-h") || value == OsStr::new("--help") => {
                    parsed.set_action(DaemonArgumentAction::Help)?;
                }
                value if value == OsStr::new("-V") || value == OsStr::new("--version") => {
                    parsed.set_action(DaemonArgumentAction::Version)?;
                }
                value if value == OsStr::new("--config") || value == OsStr::new("--database") => {
                    return Err(DaemonArgumentError::RetiredPath);
                }
                _ => return Err(DaemonArgumentError::UnknownOption),
            }
        }
        Ok(parsed)
    }

    fn set_action(&mut self, action: DaemonArgumentAction) -> Result<(), DaemonArgumentError> {
        if self.action != DaemonArgumentAction::Run && self.action != action {
            return Err(DaemonArgumentError::ConflictingAction);
        }
        self.action = action;
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum DaemonArgumentAction {
    #[default]
    Run,
    Help,
    Version,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DaemonArgumentError {
    UnknownOption,
    RetiredPath,
    ConflictingAction,
}

impl Display for DaemonArgumentError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownOption => formatter.write_str("unknown option; use --help"),
            Self::RetiredPath => {
                formatter.write_str("independent path options are retired; use UNDERCURRENT_HOME")
            }
            Self::ConflictingAction => {
                formatter.write_str("--help and --version cannot be combined")
            }
        }
    }
}

impl Error for DaemonArgumentError {}

#[derive(Debug)]
enum DaemonMainError {
    Arguments(DaemonArgumentError),
    Config(DaemonConfigError),
    PrepareRuntime(std::io::Error),
    Logging(DaemonLoggingError),
    Signals(std::io::Error),
    Start(DaemonProcessStartError),
    SignalWorkerStart(std::io::Error),
    Run(DaemonProcessRunError),
    SignalWorkerPanicked,
}

impl Display for DaemonMainError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Arguments(error) => Display::fmt(error, formatter),
            Self::Config(error) => Display::fmt(error, formatter),
            Self::PrepareRuntime(_) => {
                formatter.write_str("could not secure daemon runtime directory")
            }
            Self::Logging(error) => Display::fmt(error, formatter),
            Self::Signals(_) => formatter.write_str("could not register process signals"),
            Self::Start(error) => Display::fmt(error, formatter),
            Self::SignalWorkerStart(_) => formatter.write_str("could not start signal worker"),
            Self::Run(error) => Display::fmt(error, formatter),
            Self::SignalWorkerPanicked => formatter.write_str("signal worker panicked"),
        }
    }
}

impl Error for DaemonMainError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Arguments(error) => Some(error),
            Self::Config(error) => Some(error),
            Self::PrepareRuntime(error) => Some(error),
            Self::Logging(error) => Some(error),
            Self::Signals(error) | Self::SignalWorkerStart(error) => Some(error),
            Self::Start(error) => Some(error),
            Self::Run(error) => Some(error),
            Self::SignalWorkerPanicked => None,
        }
    }
}

impl DaemonMainError {
    const fn category(&self) -> &'static str {
        match self {
            Self::Arguments(_) => "arguments",
            Self::Config(_) => "configuration",
            Self::PrepareRuntime(_) => "prepare_runtime",
            Self::Logging(error) => error.stage().as_str(),
            Self::Signals(_) => "signals",
            Self::Start(error) => error.stage().as_str(),
            Self::SignalWorkerStart(_) => "signal_worker_start",
            Self::Run(error) => error.category().as_str(),
            Self::SignalWorkerPanicked => "signal_worker_panicked",
        }
    }
}

impl From<DaemonArgumentError> for DaemonMainError {
    fn from(error: DaemonArgumentError) -> Self {
        Self::Arguments(error)
    }
}

impl From<DaemonConfigError> for DaemonMainError {
    fn from(error: DaemonConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<DaemonLoggingError> for DaemonMainError {
    fn from(error: DaemonLoggingError) -> Self {
        Self::Logging(error)
    }
}

impl From<DaemonProcessStartError> for DaemonMainError {
    fn from(error: DaemonProcessStartError) -> Self {
        Self::Start(error)
    }
}

impl From<DaemonProcessRunError> for DaemonMainError {
    fn from(error: DaemonProcessRunError) -> Self {
        Self::Run(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_arguments_select_environment_backed_run() {
        assert_eq!(
            DaemonArguments::parse(Vec::new()).expect("arguments"),
            DaemonArguments::default()
        );
    }

    #[test]
    fn retired_path_options_are_rejected() {
        for option in ["--config", "--database"] {
            assert_eq!(
                DaemonArguments::parse([OsString::from(option)]),
                Err(DaemonArgumentError::RetiredPath)
            );
        }
    }

    #[test]
    fn invalid_options_are_payload_redacted() {
        let unknown =
            DaemonArguments::parse([OsString::from("--secret-value")]).expect_err("unknown option");
        assert_eq!(unknown.to_string(), "unknown option; use --help");
        let duplicate = DaemonArguments::parse([
            OsString::from("--database"),
            OsString::from("/tmp/one"),
            OsString::from("--database"),
            OsString::from("/tmp/two"),
        ])
        .expect_err("duplicate option");
        assert_eq!(
            duplicate.to_string(),
            "independent path options are retired; use UNDERCURRENT_HOME"
        );
    }

    #[test]
    fn help_and_version_are_mutually_exclusive() {
        assert_eq!(
            DaemonArguments::parse([OsString::from("--help"), OsString::from("--version")]),
            Err(DaemonArgumentError::ConflictingAction)
        );
    }
}
