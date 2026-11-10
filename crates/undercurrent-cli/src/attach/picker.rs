// SPDX-License-Identifier: Apache-2.0

//! Attachment activation from the shared session picker.

use super::*;
use crate::session_picker::{self, PickerView, SessionPicker};

pub(crate) enum Outcome<'a> {
    Attached(Box<OpenedInteractiveAttach<'a>>),
    Exit(CommonExitCode),
}

pub(crate) fn run<'a, P: CliProductStreamPort, W: Write>(
    executor: &InteractiveAttachExecutor<'a, P>,
    initial: TerminalInitialModeDto,
    terminal: &mut dyn InteractiveTerminalPort,
    writer: &mut W,
    worker_stack_bytes: usize,
) -> Result<Outcome<'a>, CliError>
where
    P::Subscription: 'static,
{
    let cancellation = CliStreamCancellation::default();
    let signal = session_picker::install_signals(cancellation.clone(), worker_stack_bytes)?;
    let result = choose(executor, initial, terminal, writer, &cancellation);
    finish(result, signal.finish().map_err(CliError::from))
}

pub(super) fn choose<'a, P: CliProductStreamPort, W: Write>(
    executor: &InteractiveAttachExecutor<'a, P>,
    initial: TerminalInitialModeDto,
    terminal: &mut dyn InteractiveTerminalPort,
    writer: &mut W,
    cancellation: &CliStreamCancellation,
) -> Result<Outcome<'a>, CliError>
where
    P::Subscription: 'static,
{
    let picker = SessionPicker::new(executor.client, executor.timeout, executor.limits)?;
    let mut opened = None;
    let result = session_picker::choose(
        picker,
        terminal,
        writer,
        cancellation,
        PickerView {
            title: "uc attach · Enter: attach · q/Esc: cancel",
            selection_error: "Could not attach; select another session or press R to refresh.",
            label: |s| s.name.clone(),
        },
        |session, size| {
            opened = Some(Box::new(executor.open_session(
                &session.id,
                initial.clone(),
                size,
            )?));
            Ok(())
        },
    );
    match result {
        Ok(session_picker::Outcome::Selected(())) => {
            Ok(Outcome::Attached(opened.expect("selected attachment")))
        }
        Ok(session_picker::Outcome::Exit(code)) => Ok(Outcome::Exit(code)),
        Err(error) => {
            // Selection can open streams before leaving the alternate screen fails.
            if let Some(opened) = opened {
                opened.cancel_streams();
            }
            Err(error)
        }
    }
}

fn finish<'a>(
    result: Result<Outcome<'a>, CliError>,
    cleanup: Result<(), CliError>,
) -> Result<Outcome<'a>, CliError> {
    let outcome = result?;
    if let Err(error) = cleanup {
        if let Outcome::Attached(opened) = &outcome {
            opened.cancel_streams();
        }
        return Err(error);
    }
    Ok(outcome)
}
