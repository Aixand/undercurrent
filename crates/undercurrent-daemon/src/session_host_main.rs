// SPDX-License-Identifier: Apache-2.0

//! Private daemon-independent native session-host entry point.

use std::ffi::{OsStr, OsString};
use std::io::Write;
use std::process::ExitCode;

use undercurrent_runtime::{
    EXECUTION_SUPERVISOR_ARGUMENT, ProcessHostBootstrap, run_execution_supervisor, run_process_host,
};

const HELP: &str = "Usage: uc-session-host [OPTIONS]\n\n\
uc-session-host is an implementation-private Undercurrent helper.\n\n\
Options:\n\
  -h, --help     Print help\n\
  -V, --version  Print version\n";

fn main() -> ExitCode {
    undercurrent_daemon::initialize_failure_evidence(
        undercurrent_diagnostics::Component::SessionHost,
    );
    if std::env::args_os()
        .skip(1)
        .eq([OsString::from("--capture-shell-environment")])
    {
        return match undercurrent_runtime::capture_shell_environment() {
            Ok(_) => ExitCode::SUCCESS,
            Err(error) => {
                undercurrent_daemon::record_process_failure("shell_environment", "runtime", &error);
                ExitCode::FAILURE
            }
        };
    }
    let mut stdout = std::io::stdout();
    match dispatch(
        std::env::args_os().skip(1),
        &mut stdout,
        || {
            if let Some(recorder) = undercurrent_diagnostics::process() {
                let _ = recorder.save_build();
            }
            let stdin = std::io::stdin();
            match run_execution_supervisor(stdin.lock()) {
                Ok(outcome) => outcome.terminate(),
                Err(error) => {
                    undercurrent_daemon::record_process_failure(
                        "execution_supervisor",
                        "runtime",
                        &error,
                    );
                    eprintln!("uc-session-host: execution supervisor failed");
                    ExitCode::FAILURE
                }
            }
        },
        || {
            if let Some(recorder) = undercurrent_diagnostics::process() {
                let _ = recorder.save_build();
            }
            let stdin = std::io::stdin();
            match ProcessHostBootstrap::read_from(stdin.lock()).and_then(run_process_host) {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    undercurrent_daemon::record_process_failure("process_host", "runtime", &error);
                    eprintln!("uc-session-host: {error}");
                    ExitCode::FAILURE
                }
            }
        },
    ) {
        Ok(code) => code,
        Err(error) => {
            if let Some(recorder) = undercurrent_diagnostics::process() {
                let _ = recorder.save(&recorder.record("arguments", "invalid_argument", None));
            }
            eprintln!("uc-session-host: {error}");
            ExitCode::FAILURE
        }
    }
}

fn dispatch(
    arguments: impl IntoIterator<Item = OsString>,
    output: &mut impl Write,
    run_supervisor: impl FnOnce() -> ExitCode,
    run_host: impl FnOnce() -> ExitCode,
) -> Result<ExitCode, SessionHostArgumentError> {
    match SessionHostAction::parse(arguments)? {
        SessionHostAction::Help => {
            output
                .write_all(HELP.as_bytes())
                .map_err(|_| SessionHostArgumentError::Output)?;
            Ok(ExitCode::SUCCESS)
        }
        SessionHostAction::Version => {
            writeln!(output, "uc-session-host {}", env!("CARGO_PKG_VERSION"))
                .map_err(|_| SessionHostArgumentError::Output)?;
            Ok(ExitCode::SUCCESS)
        }
        SessionHostAction::ExecutionSupervisor => Ok(run_supervisor()),
        SessionHostAction::ProcessHost => Ok(run_host()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionHostAction {
    Help,
    Version,
    ExecutionSupervisor,
    ProcessHost,
}

impl SessionHostAction {
    fn parse(
        arguments: impl IntoIterator<Item = OsString>,
    ) -> Result<Self, SessionHostArgumentError> {
        let mut arguments = arguments.into_iter();
        let first = arguments.next();
        if arguments.next().is_some() {
            return Err(SessionHostArgumentError::UnknownOption);
        }
        match first.as_deref() {
            None => Ok(Self::ProcessHost),
            Some(value) if value == OsStr::new("-h") || value == OsStr::new("--help") => {
                Ok(Self::Help)
            }
            Some(value) if value == OsStr::new("-V") || value == OsStr::new("--version") => {
                Ok(Self::Version)
            }
            Some(value) if value == OsStr::new(EXECUTION_SUPERVISOR_ARGUMENT) => {
                Ok(Self::ExecutionSupervisor)
            }
            Some(_) => Err(SessionHostArgumentError::UnknownOption),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionHostArgumentError {
    UnknownOption,
    Output,
}

impl std::fmt::Display for SessionHostArgumentError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownOption => formatter.write_str("unknown option; use --help"),
            Self::Output => formatter.write_str("could not write command output"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn help_and_version_do_not_enter_private_runtime() {
        for (argument, expected) in [
            ("--help", HELP.to_owned()),
            (
                "--version",
                format!("uc-session-host {}\n", env!("CARGO_PKG_VERSION")),
            ),
        ] {
            let mut output = Vec::new();
            let code = dispatch(
                [OsString::from(argument)],
                &mut output,
                || panic!("probe must not read supervisor bootstrap input"),
                || panic!("probe must not read process-host bootstrap input"),
            )
            .expect("side-effect-free probe");
            assert_eq!(code, ExitCode::SUCCESS);
            assert_eq!(output, expected.as_bytes());
        }
    }

    #[test]
    fn unknown_or_extra_arguments_fail_before_private_runtime() {
        for arguments in [
            vec![OsString::from("--socket")],
            vec![
                OsString::from(EXECUTION_SUPERVISOR_ARGUMENT),
                OsString::from("extra"),
            ],
        ] {
            let error = dispatch(
                arguments,
                &mut Vec::new(),
                || panic!("invalid arguments must not start the supervisor"),
                || panic!("invalid arguments must not start the process host"),
            )
            .expect_err("invalid arguments");
            assert_eq!(error, SessionHostArgumentError::UnknownOption);
        }
    }

    #[test]
    fn private_actions_preserve_exact_dispatch() {
        let host = dispatch(
            Vec::new(),
            &mut Vec::new(),
            || panic!("host bootstrap must not select supervisor"),
            || ExitCode::from(7),
        )
        .expect("host dispatch");
        assert_eq!(host, ExitCode::from(7));

        let supervisor = dispatch(
            [OsString::from(EXECUTION_SUPERVISOR_ARGUMENT)],
            &mut Vec::new(),
            || ExitCode::from(9),
            || panic!("supervisor bootstrap must not select host"),
        )
        .expect("supervisor dispatch");
        assert_eq!(supervisor, ExitCode::from(9));
    }
}
