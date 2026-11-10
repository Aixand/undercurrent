// SPDX-License-Identifier: Apache-2.0

//! Daemon-independent native CLI utility use cases.

use clap::CommandFactory;
use clap_complete::{Shell, generate};
use serde::Serialize;
use undercurrent_sdk::ClientDiagnosticName;

use crate::{
    CliCommand, CliConfigSourceKind, CliDiagnosticEvent, CliError, CliPresenter,
    CompletionArguments, ConfigAction, ConfigArguments, InitArguments, NativeCliArguments,
    ResolvedCliConfig, ShellArgument, diagnostic_event_catalog_text,
    install_shell_integration_with_prompt, native_home_directory, shell_hook_document_with_prompt,
};

/// Component that owns one stable diagnostic-event registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticEventOwner {
    /// Native CLI adapter diagnostics.
    Cli,
    /// Resilient native product-client diagnostics.
    Sdk,
}

impl DiagnosticEventOwner {
    /// Returns the stable owner name used in human presentation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Sdk => "sdk",
        }
    }
}

/// One registered payload-free diagnostic event name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DiagnosticEventCatalogEntry {
    /// Component that declares the event.
    pub owner: DiagnosticEventOwner,
    /// Stable event name emitted by that component.
    pub name: String,
}

/// Complete deterministic diagnostic-event catalog linked into `uc`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DiagnosticEventCatalog {
    /// Total registered event names.
    pub count: usize,
    /// Entries ordered by owner and then stable event name.
    pub events: Vec<DiagnosticEventCatalogEntry>,
}

/// Successful complete daemon configuration validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ConfigurationValidationReport {
    /// Whether every common, product, component, and relationship check passed.
    pub valid: bool,
    /// Safe category describing the selected common configuration source.
    pub source: CliConfigSourceKind,
}

/// Builds the complete diagnostic-event vocabulary linked into the native CLI.
///
/// This operation performs no IO and does not panic. It enumerates central
/// registries directly, so the command cannot drift from emitters as new event
/// variants are added.
pub fn diagnostic_event_catalog() -> DiagnosticEventCatalog {
    let mut events = Vec::new();
    extend_events(
        &mut events,
        DiagnosticEventOwner::Cli,
        CliDiagnosticEvent::ALL,
        CliDiagnosticEvent::as_str,
    );
    extend_events(
        &mut events,
        DiagnosticEventOwner::Sdk,
        ClientDiagnosticName::ALL,
        ClientDiagnosticName::as_str,
    );
    events.sort_unstable_by(|left, right| {
        (left.owner, left.name.as_str()).cmp(&(right.owner, right.name.as_str()))
    });
    DiagnosticEventCatalog {
        count: events.len(),
        events,
    }
}

/// Executes the daemon-independent `events` utility through typed presentation.
///
/// This operation performs only caller-supplied writer IO and does not panic.
///
/// # Errors
///
/// Returns output-shape, serialization, or writer failure.
pub fn execute_events<W>(presenter: CliPresenter, writer: &mut W) -> Result<(), CliError>
where
    W: std::io::Write,
{
    let catalog = diagnostic_event_catalog();
    presenter.write_finite(writer, &catalog, |_| {
        diagnostic_event_catalog_text(&catalog)
    })
}

/// Executes daemon-independent native shell-completion generation.
///
/// The completion document is always exact text, including in quiet mode. This
/// operation performs only caller-supplied writer IO and does not panic.
///
/// # Errors
///
/// Returns a typed structured-output rejection or writer failure.
pub fn execute_completion<W>(
    arguments: &CompletionArguments,
    presenter: CliPresenter,
    writer: &mut W,
) -> Result<(), CliError>
where
    W: std::io::Write,
{
    let script = generate_completion_script(arguments.shell);
    presenter.write_text_document(writer, CliCommand::Completion, &script)
}

/// Executes daemon-independent shell-hook printing or installation.
///
/// Print mode emits the exact embedded document and performs no filesystem IO.
/// Install mode resolves the native home or explicit profile, applies one bounded atomic update, and emits a
/// payload-safe typed report. This function does not panic.
///
/// # Errors
///
/// Returns typed output-shape, home-contract, filesystem, or writer failures.
pub fn execute_init<W>(
    arguments: &InitArguments,
    presenter: CliPresenter,
    writer: &mut W,
) -> Result<(), CliError>
where
    W: std::io::Write,
{
    if arguments.print {
        return presenter.write_text_document(
            writer,
            CliCommand::Init,
            &shell_hook_document_with_prompt(arguments.shell, arguments.prompt),
        );
    }
    let report = if let Some(profile) = &arguments.profile {
        crate::shell_integration::install_shell_integration_at_path(
            arguments.shell,
            profile,
            arguments.prompt,
        )?
    } else {
        let home = native_home_directory()?;
        install_shell_integration_with_prompt(arguments.shell, &home, arguments.prompt)?
    };
    presenter.write_finite(writer, &report, |_| {
        format!(
            "{} shell integration {}",
            report.shell.as_str(),
            report.disposition.as_str()
        )
    })
}

/// Executes daemon-independent effective configuration inspection or validation.
///
/// The configuration must already have passed complete resolution. Show output
/// excludes product-home, database, and socket values; validation reports only
/// a safe source category. This operation performs only caller-supplied writer
/// IO and does not panic.
///
/// # Errors
///
/// Returns output-shape, serialization, or writer failure.
pub fn execute_config<W>(
    arguments: &ConfigArguments,
    configuration: &ResolvedCliConfig,
    presenter: CliPresenter,
    writer: &mut W,
) -> Result<(), CliError>
where
    W: std::io::Write,
{
    match &arguments.action {
        ConfigAction::Show => {
            presenter.write_config_report(writer, &configuration.redacted_report())
        }
        ConfigAction::Validate => {
            let report = ConfigurationValidationReport {
                valid: true,
                source: configuration.source(),
            };
            presenter.write_finite(writer, &report, |_| {
                format!(
                    "configuration is valid (source: {})",
                    config_source_text(report.source)
                )
            })
        }
    }
}

/// Generates a complete native shell-completion document in memory.
///
/// The returned bytes are intended for exact text output or redirection. This
/// operation performs no filesystem or process IO and does not panic.
pub fn generate_completion_script(shell: ShellArgument) -> Vec<u8> {
    let mut command = NativeCliArguments::command();
    let mut output = Vec::new();
    generate(completion_shell(shell), &mut command, "uc", &mut output);
    output
}

fn extend_events<T, const N: usize>(
    destination: &mut Vec<DiagnosticEventCatalogEntry>,
    owner: DiagnosticEventOwner,
    values: [T; N],
    name: impl Fn(T) -> &'static str,
) where
    T: Copy,
{
    destination.extend(values.into_iter().map(|value| DiagnosticEventCatalogEntry {
        owner,
        name: name(value).to_owned(),
    }));
}

const fn completion_shell(shell: ShellArgument) -> Shell {
    match shell {
        ShellArgument::Bash => Shell::Bash,
        ShellArgument::Zsh => Shell::Zsh,
        ShellArgument::Fish => Shell::Fish,
        ShellArgument::PowerShell => Shell::PowerShell,
    }
}

const fn config_source_text(source: CliConfigSourceKind) -> &'static str {
    match source {
        CliConfigSourceKind::Defaults => "defaults",
        CliConfigSourceKind::Explicit => "explicit",
        CliConfigSourceKind::Environment => "environment",
        CliConfigSourceKind::UndercurrentHome => "undercurrent_home",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use super::*;
    use crate::cli_support::{CliContext, CommandTimeout, OutputFormat, Verbosity};

    #[test]
    fn catalog_contains_every_linked_registry_once_and_is_deterministic() {
        let catalog = diagnostic_event_catalog();
        let expected_count = CliDiagnosticEvent::ALL.len() + ClientDiagnosticName::ALL.len();
        let unique = catalog
            .events
            .iter()
            .map(|event| (event.owner, event.name.as_str()))
            .collect::<BTreeSet<_>>();

        assert_eq!(catalog.count, expected_count);
        assert_eq!(unique.len(), expected_count);
        assert!(catalog.events.windows(2).all(|pair| {
            (pair[0].owner, pair[0].name.as_str()) <= (pair[1].owner, pair[1].name.as_str())
        }));
    }

    #[test]
    fn completion_documents_are_shell_specific_and_cover_retained_commands() {
        let bash = String::from_utf8(generate_completion_script(ShellArgument::Bash))
            .expect("bash completion UTF-8");
        let zsh = String::from_utf8(generate_completion_script(ShellArgument::Zsh))
            .expect("zsh completion UTF-8");
        let fish = String::from_utf8(generate_completion_script(ShellArgument::Fish))
            .expect("fish completion UTF-8");

        assert!(bash.contains("_uc"));
        assert!(zsh.contains("#compdef uc"));
        assert!(fish.contains("complete -c uc"));
        for command in crate::CliCommand::ALL {
            assert!(bash.contains(command.as_str()), "missing {command:?}");
        }
        assert!(!bash.contains("stack"));
    }

    #[test]
    fn utility_execution_supports_typed_json_events_and_exact_completion_text() {
        let timeout =
            CommandTimeout::new(Duration::from_secs(1), Duration::from_secs(1)).expect("timeout");
        let mut events = Vec::new();
        execute_events(
            CliPresenter::new(CliContext::new(
                OutputFormat::Json,
                Verbosity::Normal,
                timeout,
            )),
            &mut events,
        )
        .expect("JSON events");
        let catalog: serde_json::Value =
            serde_json::from_slice(&events).expect("event catalog JSON");
        assert_eq!(
            catalog["count"],
            serde_json::json!(diagnostic_event_catalog().count)
        );

        let mut completion = Vec::new();
        execute_completion(
            &CompletionArguments {
                shell: ShellArgument::Fish,
            },
            CliPresenter::new(CliContext::new(
                OutputFormat::Text,
                Verbosity::Quiet,
                timeout,
            )),
            &mut completion,
        )
        .expect("quiet completion text");
        assert!(!completion.is_empty());
    }
}
