// SPDX-License-Identifier: Apache-2.0

//! Typed native `uc` argument adapter.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;

use crate::{CliCommand, GlobalCliOptions};

/// Complete native `uc` command-line parser.
#[derive(Clone, Parser, PartialEq, Eq)]
#[command(
    name = "uc",
    version,
    about = "Manage and observe local Undercurrent sessions",
    disable_help_subcommand = false,
    mut_subcommands = alphabetical_help,
    help_template = grouped_help_template()
)]
pub struct NativeCliArguments {
    /// Selected product command.
    #[command(subcommand)]
    pub command: NativeCliCommand,

    /// Print command results as JSON.
    #[arg(long, global = true, conflicts_with = "jsonl")]
    #[arg(help_heading = "Global options")]
    pub json: bool,

    /// Print streamed results as JSON Lines.
    #[arg(long, global = true, conflicts_with = "json")]
    #[arg(help_heading = "Global options")]
    pub jsonl: bool,

    /// Suppress non-essential presentation.
    #[arg(long, short, global = true, conflicts_with_all = ["verbose", "diagnostic"])]
    #[arg(help_heading = "Global options")]
    pub quiet: bool,

    /// Include additional safe operational context.
    #[arg(long, short, global = true, conflicts_with_all = ["quiet", "diagnostic"])]
    #[arg(help_heading = "Global options")]
    pub verbose: bool,

    /// Include maximum payload-redacted diagnostic context.
    #[arg(long, global = true, conflicts_with_all = ["quiet", "verbose"])]
    #[arg(help_heading = "Global options")]
    pub diagnostic: bool,

    /// Bound finite requests and stream activation.
    #[arg(long, global = true, value_name = "DURATION")]
    #[arg(help_heading = "Global options")]
    pub timeout: Option<String>,
    /// Use one saved connection without changing the saved default.
    #[arg(long, global = true, value_name = "NAME")]
    #[arg(help_heading = "Global options")]
    pub connection: Option<String>,
}

/// Workflow sections for the top-level help; descriptions come from the parser.
const HELP_SECTIONS: &[(&str, &[&str])] = &[
    (
        "Setup & configuration",
        &["init", "completion", "config", "settings"],
    ),
    ("Session lifecycle", &["start", "stop", "restart", "clean"]),
    (
        "Interactive workspaces",
        &["tui", "attach", "group", "context"],
    ),
    ("Automation & orchestration", &["send", "exec", "wait"]),
    (
        "Monitoring & output",
        &["ls", "top", "logs", "peek", "watch", "inspect"],
    ),
    (
        "Detection & troubleshooting",
        &["profile", "explain", "debug", "events"],
    ),
    ("System administration", &["daemon", "reset"]),
];

fn grouped_help_template() -> String {
    let bold = clap::builder::styling::Style::new().bold();
    let commands = NativeCliCommand::augment_subcommands(clap::Command::new("uc"));
    let mut template = String::from("{about-with-newline}\n{usage-heading} {usage}\n");
    for (heading, names) in HELP_SECTIONS {
        template.push_str(&format!("\n{heading}:\n"));
        for name in *names {
            let command = commands
                .find_subcommand(name)
                .expect("registered help command");
            let about = command.get_about().expect("command description");
            template.push_str(&format!("  {bold}{name:12}{bold:#}{about}\n"));
        }
    }
    template.push_str(&format!(
        "\nHelp:\n  {bold}help{bold:#}        Show help for a command\n\nGlobal options:\n{{options}}\n\nExamples:\n  uc start dev --interactive      Start an interactive shell\n  uc attach dev                  Open its terminal\n  uc group new project           Create and open a workspace\n  uc logs dev --follow            Follow session output\n\nRun uc <command> --help for options and examples.\n",
    ));
    template
}

fn alphabetical_help(command: clap::Command) -> clap::Command {
    // Equal display orders sort by name; 999 also includes Clap's generated help command.
    command
        .display_order(999)
        .mut_subcommands(alphabetical_help)
}

impl NativeCliArguments {
    /// Converts raw global flags into the parser-independent context input.
    ///
    /// This function performs no IO and does not panic.
    pub fn global_options(&self) -> GlobalCliOptions {
        GlobalCliOptions {
            json: self.json,
            jsonl: self.jsonl,
            quiet: self.quiet,
            verbose: self.verbose,
            diagnostic: self.diagnostic,
            timeout: self.timeout.clone(),
            config_path: None,
            connection: self.connection.clone(),
        }
    }
}

/// Complete retained command surface. Stack orchestration is intentionally absent.
#[derive(Clone, Subcommand, PartialEq, Eq)]
pub enum NativeCliCommand {
    /// Start a managed session.
    Start(Box<StartArguments>),
    /// Stop one or more sessions.
    Stop(StopArguments),
    /// Restart a session.
    Restart(RestartArguments),
    /// List sessions.
    #[command(alias = "list")]
    Ls(ListArguments),
    /// Show detailed session information.
    Inspect(InspectArguments),
    /// Open a session terminal.
    Attach(AttachArguments),
    /// Browse sessions, attach terminals, and manage groups and labels.
    #[command(
        visible_alias = "workspace",
        after_help = "Enter attaches; the configured detach shortcut returns to the workspace.\nSpace selects sessions, n creates, g groups, l edits labels, and q quits.\nRequires terminal stdin/stdout and text output. Processes survive leaving the workspace."
    )]
    Tui,
    /// Organize sessions into interactive workspaces.
    Group(crate::group::GroupArguments),
    /// Send text or keys to a session.
    Send(SendArguments),
    /// Run a command using a session’s live directory and shell exports.
    Exec(ExecArguments),
    /// Wait for a session to reach a selected state.
    Wait(WaitArguments),
    /// Watch session state changes.
    Watch(WatchArguments),
    /// Remove finished sessions after confirmation.
    Clean(CleanArguments),
    /// Show a live session dashboard.
    Top(TopArguments),
    /// Read saved session output or follow it live.
    Logs(LogsArguments),
    /// Show a session terminal screen or changes.
    Peek(PeekArguments),
    /// Explain the detected foreground state of a session.
    Explain(SessionTargetArguments),
    /// Show which session this shell belongs to.
    Context(ContextArguments),
    /// Install or inspect shell integration.
    Init(InitArguments),
    /// Show or validate configuration.
    Config(ConfigArguments),
    /// Manage saved CLI connection profiles.
    Settings(crate::SettingsArguments),
    /// Inspect and test detection profiles.
    Profile(ProfileArguments),
    /// Control the local daemon process.
    Daemon(DaemonArguments),
    /// Delete local state after confirmation.
    Reset(ResetArguments),
    /// View diagnostics and troubleshoot problems.
    Debug(DebugArguments),
    /// Generate shell completion definitions.
    Completion(CompletionArguments),
    /// List diagnostic event names.
    Events,
}

impl NativeCliCommand {
    /// Returns the corresponding stable central command registry value.
    pub const fn command(&self) -> CliCommand {
        match self {
            Self::Start(_) => CliCommand::Start,
            Self::Stop(_) => CliCommand::Stop,
            Self::Restart(_) => CliCommand::Restart,
            Self::Ls(_) => CliCommand::Ls,
            Self::Inspect(_) => CliCommand::Inspect,
            Self::Attach(_) => CliCommand::Attach,
            Self::Tui => CliCommand::Tui,
            Self::Group(_) => CliCommand::Group,
            Self::Send(_) => CliCommand::Send,
            Self::Exec(_) => CliCommand::Exec,
            Self::Wait(_) => CliCommand::Wait,
            Self::Watch(_) => CliCommand::Watch,
            Self::Clean(_) => CliCommand::Clean,
            Self::Top(_) => CliCommand::Top,
            Self::Logs(_) => CliCommand::Logs,
            Self::Peek(_) => CliCommand::Peek,
            Self::Explain(_) => CliCommand::Explain,
            Self::Context(_) => CliCommand::Context,
            Self::Init(_) => CliCommand::Init,
            Self::Config(_) => CliCommand::Config,
            Self::Settings(_) => CliCommand::Settings,
            Self::Profile(_) => CliCommand::Profile,
            Self::Daemon(_) => CliCommand::Daemon,
            Self::Reset(_) => CliCommand::Reset,
            Self::Debug(_) => CliCommand::Debug,
            Self::Completion(_) => CliCommand::Completion,
            Self::Events => CliCommand::Events,
        }
    }
}

/// Arguments that create one managed session.
#[derive(Clone, Args, PartialEq, Eq)]
#[command(
    after_help = "Examples:\n  uc start dev --interactive\n  uc start server -- python -m http.server 8000\n  uc start worker --restart on-failure -- ./worker"
)]
pub struct StartArguments {
    /// Executable policy: native installation path or verified sealed copy.
    #[arg(long, value_enum, default_value = "native", help_heading = "Launch")]
    pub launch_mode: LaunchModeArgument,
    /// Unique human-facing session name.
    pub name: String,
    /// Executable followed by its ordered arguments.
    #[arg(last = true)]
    pub command: Vec<String>,
    /// Start a persistent interactive shell; cannot be combined with COMMAND.
    #[arg(long, short = 'i')]
    #[arg(help_heading = "Launch")]
    pub interactive: bool,
    /// Shell used for interactive mode.
    #[arg(long, value_name = "PATH")]
    #[arg(help_heading = "Launch")]
    pub shell: Option<String>,
    /// Explicit environment value, repeatable as `KEY=VALUE`.
    #[arg(long = "env", short = 'e', value_name = "KEY=VALUE")]
    #[arg(help_heading = "Environment & metadata")]
    pub environment: Vec<String>,
    /// Environment-file reference resolved by the daemon runtime.
    #[arg(long, value_name = "PATH")]
    #[arg(help_heading = "Environment & metadata")]
    pub env_file: Option<String>,
    /// Working directory, resolved relative to the caller's current directory.
    #[arg(long, value_name = "PATH")]
    #[arg(help_heading = "Launch")]
    pub cwd: Option<PathBuf>,
    /// Automatic restart behavior.
    #[arg(long, value_enum, default_value_t)]
    #[arg(help_heading = "Restart policy")]
    pub restart: RestartModeArgument,
    /// Maximum consecutive automatic restart attempts.
    #[arg(long, default_value_t = 3)]
    #[arg(help_heading = "Restart policy")]
    pub max_restarts: u32,
    /// Initial automatic restart delay.
    #[arg(long, default_value = "1s", value_name = "DURATION")]
    #[arg(help_heading = "Restart policy")]
    pub restart_delay: String,
    /// Maximum automatic restart delay.
    #[arg(long, default_value = "30s", value_name = "DURATION")]
    #[arg(help_heading = "Restart policy")]
    pub max_restart_delay: String,
    /// Exponential backoff multiplier in basis points.
    #[arg(long, default_value_t = 20_000)]
    #[arg(help_heading = "Restart policy")]
    pub backoff_basis_points: u32,
    /// Uptime required to reset consecutive restart attempts.
    #[arg(long, default_value = "10s", value_name = "DURATION")]
    #[arg(help_heading = "Restart policy")]
    pub min_uptime: String,
    /// Explicit detection profile.
    #[arg(long)]
    #[arg(help_heading = "Detection & health")]
    pub profile: Option<String>,
    /// Effective-profile binding policy; auto follows foreground processes unless a profile is explicit.
    #[arg(long, value_enum, default_value_t)]
    #[arg(help_heading = "Detection & health")]
    pub profile_mode: ProfileModeArgument,
    /// Disable foreground observation for this session.
    #[arg(long)]
    #[arg(help_heading = "Detection & health")]
    pub no_detect: bool,
    /// Query-visible label, repeatable as `KEY=VALUE`.
    #[arg(long, short = 'l', value_name = "KEY=VALUE")]
    #[arg(help_heading = "Environment & metadata")]
    pub label: Vec<String>,
    /// Private metadata, repeatable as `KEY=VALUE`.
    #[arg(long, short = 'M', value_name = "KEY=VALUE")]
    #[arg(help_heading = "Environment & metadata")]
    pub metadata: Vec<String>,
    /// Configured health rule, repeatable as `KIND:VALUE`.
    #[arg(long = "healthy-when", value_name = "RULE")]
    #[arg(help_heading = "Detection & health")]
    pub health: Vec<String>,
    /// Graceful stop signal (default: hangup for interactive shells, terminate otherwise).
    #[arg(long, value_enum)]
    #[arg(help_heading = "Shutdown & cleanup")]
    pub stop_signal: Option<StopSignalArgument>,
    /// Time before forced termination is permitted.
    #[arg(long, default_value = "5s", value_name = "DURATION")]
    #[arg(help_heading = "Shutdown & cleanup")]
    pub stop_timeout: String,
    /// Terminal output interpretation and screen-projection policy.
    #[arg(long, value_enum, default_value_t)]
    #[arg(help_heading = "Terminal & attachment")]
    pub output_capture: OutputCaptureModeArgument,
    /// Initial terminal columns.
    #[arg(long, default_value_t = 80)]
    #[arg(help_heading = "Terminal & attachment")]
    pub columns: u16,
    /// Initial terminal rows.
    #[arg(long, default_value_t = 24)]
    #[arg(help_heading = "Terminal & attachment")]
    pub rows: u16,
    /// Remove the session automatically when it finishes.
    #[arg(long = "rm")]
    #[arg(help_heading = "Shutdown & cleanup")]
    pub auto_remove: bool,
    /// Open the terminal after starting the session.
    #[arg(long)]
    #[arg(help_heading = "Terminal & attachment")]
    pub attach: bool,
    /// Override detach keys; default uses the saved prefix (Alt+U, then d).
    #[arg(long, default_value = "default", value_name = "KEYS")]
    #[arg(help_heading = "Terminal & attachment")]
    pub detach_keys: String,
}

/// Requested effective-profile binding behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub enum ProfileModeArgument {
    /// Follow foreground processes unless an explicit profile pins the session.
    #[default]
    Auto,
    /// Keep the launch-selected base profile for the process lifetime.
    Fixed,
    /// Follow exact foreground process-group profile evidence.
    Foreground,
}

/// Automatic restart selection accepted by `start`.
#[derive(Debug, Clone, Copy, Default, ValueEnum, PartialEq, Eq)]
pub enum RestartModeArgument {
    /// Never restart automatically.
    #[default]
    Never,
    /// Restart only after non-successful exit.
    OnFailure,
    /// Restart after every exit.
    Always,
}

/// Portable graceful stop signal accepted by `start`.
#[derive(Debug, Clone, Copy, Default, ValueEnum, PartialEq, Eq)]
pub enum StopSignalArgument {
    /// Conventional termination request.
    #[default]
    Terminate,
    /// Interactive interrupt.
    Interrupt,
    /// Hangup request.
    Hangup,
    /// Quit request.
    Quit,
    /// First user-defined signal.
    User1,
    /// Second user-defined signal.
    User2,
}

/// Terminal output capture policy accepted by `start`.
#[derive(Debug, Clone, Copy, Default, ValueEnum, PartialEq, Eq)]
pub enum OutputCaptureModeArgument {
    /// Interpret output as a terminal and maintain VT screen projection.
    #[default]
    Terminal,
    /// Retain exact PTY-master bytes without terminal interpretation.
    Opaque,
}

/// One human-facing session target.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct SessionTargetArguments {
    /// Exact unique session name.
    pub name: String,
}

/// Selection and mode for stopping sessions.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
#[command(
    after_help = "Examples:\n  uc stop                  Pick a session to stop\n  uc stop --force          Pick a session to stop immediately\n  uc stop dev\n  uc stop --all\n\nWithout a selection, requires terminal stdin/stdout and text output. Up/Down or k/j selects, h/l pages, Enter stops, q/Esc cancels."
)]
#[command(group(
    clap::ArgGroup::new("selection")
        .multiple(false)
        .args(["name", "all", "label"])
))]
pub struct StopArguments {
    /// Exact unique session name; omit all selectors to choose interactively.
    pub name: Option<String>,
    /// Select every session.
    #[arg(long)]
    pub all: bool,
    /// Select sessions with one exact `KEY=VALUE` label.
    #[arg(long, short = 'l', value_name = "KEY=VALUE")]
    pub label: Option<String>,
    /// Stop immediately (the default).
    #[arg(long, short, conflicts_with = "graceful")]
    pub force: bool,
    /// Send the configured stop signal and allow its grace period before forcing.
    #[arg(long)]
    pub graceful: bool,
}

/// Restart one session with explicit graceful opt-in.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct RestartArguments {
    /// Exact unique session name.
    pub name: String,
    /// Allow the configured stop signal and grace period before replacement.
    #[arg(long, conflicts_with = "force")]
    pub graceful: bool,
    /// Replace immediately (the default).
    #[arg(long, short)]
    pub force: bool,
}

impl StopArguments {
    pub(crate) fn interactive(&self) -> bool {
        self.name.is_none() && !self.all && self.label.is_none()
    }
}

/// Prepared bounded session list query.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
#[command(
    after_help = "Examples:\n  uc ls\n  uc ls --lifecycle running\n  uc ls --label project=demo"
)]
pub struct ListArguments {
    /// Restrict to a stable session ID; repeatable.
    #[arg(long = "id")]
    #[arg(help_heading = "Filters")]
    pub ids: Vec<String>,
    /// Restrict to one exact unique session name.
    #[arg(long)]
    #[arg(help_heading = "Filters")]
    pub name: Option<String>,
    /// Require a case-sensitive display-name substring.
    #[arg(long)]
    #[arg(help_heading = "Filters")]
    pub name_contains: Option<String>,
    /// Accepted lifecycle kind; repeatable.
    #[arg(long, value_enum)]
    #[arg(help_heading = "Filters")]
    pub lifecycle: Vec<LifecycleArgument>,
    /// Accepted execution state; repeatable.
    #[arg(long, value_enum)]
    #[arg(help_heading = "Filters")]
    pub execution: Vec<ExecutionArgument>,
    /// Accepted interaction state; repeatable.
    #[arg(long, value_enum)]
    #[arg(help_heading = "Filters")]
    pub interaction: Vec<InteractionArgument>,
    /// Accepted health status; repeatable.
    #[arg(long, value_enum)]
    #[arg(help_heading = "Filters")]
    pub health: Vec<HealthArgument>,
    /// Restrict to an exact detection profile.
    #[arg(long)]
    #[arg(help_heading = "Filters")]
    pub profile: Option<String>,
    /// Require an exact query-visible label; repeatable as `KEY=VALUE`.
    #[arg(long, short = 'l', value_name = "KEY=VALUE")]
    #[arg(help_heading = "Filters")]
    pub label: Vec<String>,
    /// Restrict by presence of active WorkContext.
    #[arg(long)]
    #[arg(help_heading = "Filters")]
    pub has_active_work: Option<bool>,
    /// Stable server-side sort field.
    #[arg(long, value_enum, default_value_t)]
    #[arg(help_heading = "Sorting & display")]
    pub sort: SessionSortArgument,
    /// Reverse the stable server-side sort.
    #[arg(long)]
    #[arg(help_heading = "Sorting & display")]
    pub descending: bool,
    /// Typed server-side summary projection.
    #[arg(long, value_enum, default_value_t)]
    #[arg(help_heading = "Sorting & display")]
    pub projection: SessionProjectionArgument,
    /// Maximum summaries returned in this page.
    #[arg(long)]
    #[arg(help_heading = "Pagination")]
    pub page_size: Option<u32>,
    /// Opaque continuation cursor returned by a preceding page.
    #[arg(long)]
    #[arg(help_heading = "Pagination")]
    pub cursor: Option<String>,
    /// Request an exact total count when the server can calculate it.
    #[arg(long)]
    #[arg(help_heading = "Pagination")]
    pub total: bool,
}

/// Stable lifecycle filter values.
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum LifecycleArgument {
    /// Process launch is in progress.
    Starting,
    /// Process is running.
    Running,
    /// Graceful or forced stop is in progress.
    Stopping,
    /// Session stopped without a process exit code.
    Stopped,
    /// Process exited.
    Exited,
    /// Session failed.
    Failed,
}

/// Stable foreground execution filter values.
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum ExecutionArgument {
    /// Observable foreground work or motion is occurring.
    Active,
    /// The process is alive without current work evidence.
    Quiescent,
    /// Available evidence cannot establish execution state.
    Unknown,
}

/// Stable foreground interaction filter values.
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum InteractionArgument {
    /// No current interaction surface has been observed.
    #[value(name = "none")]
    Absent,
    /// Input is mechanically available but not affirmatively requested.
    Available,
    /// Fresh affirmative evidence says input is requested now.
    Requested,
}

/// Stable health filter values.
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum HealthArgument {
    /// Every evaluated rule passed.
    Healthy,
    /// At least one evaluated rule failed.
    Unhealthy,
    /// Configured rules have no current result.
    Unknown,
    /// No health rule is configured.
    NoCheck,
}

/// Stable server-side session sort fields.
#[derive(Debug, Clone, Copy, Default, ValueEnum, PartialEq, Eq)]
pub enum SessionSortArgument {
    /// Human-facing name.
    #[default]
    Name,
    /// Lifecycle state.
    Lifecycle,
    /// Foreground execution state.
    Execution,
    /// Foreground interaction state.
    Interaction,
    /// Health aggregate.
    Health,
    /// Last summary update time.
    UpdatedAt,
    /// Automatic restart count.
    RestartCount,
}

/// Typed summary projection selection.
#[derive(Debug, Clone, Copy, Default, ValueEnum, PartialEq, Eq)]
pub enum SessionProjectionArgument {
    /// Small default summary.
    #[default]
    Compact,
    /// Summary with query-visible labels.
    WithLabels,
    /// Summary with operational fields.
    Operational,
}

/// Full detail inspection for one session; legacy projection flags are accepted.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct InspectArguments {
    /// Exact unique session name.
    pub name: String,
    /// Accepted for compatibility; all detail sections are included by default.
    #[arg(long, hide = true)]
    pub configuration: bool,
    /// Accepted for compatibility; all detail sections are included by default.
    #[arg(long, hide = true)]
    pub work_context: bool,
    /// Accepted for compatibility; all detail sections are included by default.
    #[arg(long, hide = true)]
    pub process: bool,
    /// Accepted for compatibility; all detail sections are included by default.
    #[arg(long, hide = true)]
    pub foreground_evidence: bool,
    /// Accepted for compatibility; all detail sections are included by default.
    #[arg(long, hide = true)]
    pub health_results: bool,
    /// Accepted for compatibility; all detail sections are included by default.
    #[arg(long, hide = true)]
    pub all: bool,
}

/// Selective interactive terminal attachment.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
#[command(
    after_help = "Examples:\n  uc attach                  Pick a session interactively\n  uc attach dev\n  uc attach --read-only\n  uc attach dev --read-only\n\nPicker: Up/Down or k/j selects, h/l pages, Enter attaches, q/Esc cancels.\nAlt+U, then: d detach, [ copy mode, ] paste, % split left/right, \" split top/bottom, o next pane, z zoom, x close pane, ? help.\nSplits can attach an existing session or create a shell. Uses your configured prefix when changed."
)]
pub struct AttachArguments {
    /// Exact unique session name; omit to choose from an interactive picker.
    pub name: Option<String>,
    /// Resume retained output at this byte offset.
    #[arg(long)]
    pub from_offset: Option<u64>,
    /// Observe output and detach keys without forwarding other input.
    #[arg(long)]
    pub read_only: bool,
    /// Override detach keys; default uses the saved prefix (Alt+U, then d).
    #[arg(long, default_value = "default", value_name = "KEYS")]
    pub detach_keys: String,
}

/// One ordered terminal input sequence.
#[derive(Clone, Args, PartialEq, Eq)]
#[command(
    after_help = "Examples:\n  uc send dev \"echo hello\"\n  uc send dev --key enter\n  uc send dev \"draft text\" --no-newline"
)]
pub struct SendArguments {
    /// Exact unique session name.
    pub name: String,
    /// Text to send using the profile default. Omit to read from standard input.
    pub input: Option<String>,
    /// Close standard input after all previously accepted writes.
    #[arg(long, conflicts_with_all = ["input", "paste", "raw", "key", "delivery_id", "preserve_observation", "no_newline"])]
    #[arg(help_heading = "Input")]
    pub eof: bool,
    /// Treat the text body as a paste item.
    #[arg(long)]
    #[arg(help_heading = "Input")]
    pub paste: bool,
    /// Force literal text, overriding the profile default.
    #[arg(long, conflicts_with = "paste", help_heading = "Input")]
    pub raw: bool,
    /// Append an Enter key after the body.
    #[arg(long, default_value_t = true, conflicts_with = "no_newline")]
    #[arg(help_heading = "Input")]
    pub newline: bool,
    /// Do not append an Enter key after the body.
    #[arg(long)]
    #[arg(help_heading = "Input")]
    pub no_newline: bool,
    /// Named terminal key or chord; repeatable.
    #[arg(long)]
    #[arg(help_heading = "Input")]
    pub key: Vec<String>,
    /// Stable semantic delivery identity.
    #[arg(long)]
    #[arg(help_heading = "Delivery control")]
    pub delivery_id: Option<String>,
    /// Preserve the current foreground observation after delivery.
    #[arg(long)]
    #[arg(help_heading = "Delivery control")]
    pub preserve_observation: bool,
}

/// One-shot execution in one session context.
#[derive(Clone, Args, PartialEq, Eq)]
#[command(
    after_help = "Examples:\n  uc exec dev -- pwd\n  uc exec dev --dir-launch -- pwd\n\nUses the observed live directory and latest captured shell exports by default.\nUse --dir-launch and/or --env-launch to select launch settings.\nLive exports require current uc init shell integration."
)]
pub struct ExecArguments {
    /// Exact unique session name.
    pub name: String,
    /// Use the saved launch directory instead of the observed live directory.
    #[arg(long)]
    pub dir_launch: bool,
    /// Use launch environment settings instead of the latest shell exports.
    #[arg(long)]
    pub env_launch: bool,
    /// Executable followed by its ordered arguments.
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
}

/// One bounded wait condition.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
#[command(after_help = "Example:\n  uc wait worker --lifecycle exited --timeout 30s")]
pub struct WaitArguments {
    /// Exact unique session name.
    pub name: String,
    /// Target execution state; repeatable.
    #[arg(long, value_enum)]
    pub execution: Vec<ExecutionArgument>,
    /// Target interaction state; repeatable.
    #[arg(long, value_enum)]
    pub interaction: Vec<InteractionArgument>,
    /// Target lifecycle; repeatable.
    #[arg(long, value_enum)]
    pub lifecycle: Vec<LifecycleArgument>,
    /// Target health status; repeatable.
    #[arg(long, value_enum)]
    pub health: Vec<HealthArgument>,
}

/// Selective revisioned session-state watch.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct WatchArguments {
    /// Exact unique session names; empty means a paged query selection.
    pub names: Vec<String>,
    /// Include compact WorkContext changes.
    #[arg(long)]
    pub work_context: bool,
    /// Include health changes.
    #[arg(long)]
    pub health: bool,
    /// Maximum summaries retained by the watched query.
    #[arg(long)]
    pub page_size: Option<u32>,
    /// Typed summary projection delivered by the watched query.
    #[arg(long, value_enum, default_value_t)]
    pub projection: SessionProjectionArgument,
}

/// Confirmed removal selection.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
#[command(group(
    clap::ArgGroup::new("selection")
        .required(false)
        .multiple(false)
        .args(["name", "all"])
))]
pub struct CleanArguments {
    /// Exact unique session name; omit to select every terminal session.
    pub name: Option<String>,
    /// Select every terminal session (the default when no name is supplied).
    #[arg(long)]
    pub all: bool,
    /// Confirm destructive removal without an interactive prompt.
    #[arg(long, short = 'y')]
    pub yes: bool,
}

/// Selective bounded dashboard arguments.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct TopArguments {
    /// Maximum summaries retained by the dashboard.
    #[arg(long)]
    pub page_size: Option<u32>,
    /// Typed summary projection.
    #[arg(long, value_enum, default_value_t)]
    pub projection: SessionProjectionArgument,
}

/// Retained or live terminal output selection.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
#[command(after_help = "Examples:\n  uc logs dev\n  uc logs dev --follow")]
pub struct LogsArguments {
    /// Exact unique session name.
    pub name: String,
    /// First retained byte offset.
    #[arg(long, default_value_t = 0)]
    pub from_offset: u64,
    /// Maximum bytes in one finite page.
    #[arg(long)]
    pub max_bytes: Option<u32>,
    /// Continue with a selective terminal-output stream.
    #[arg(long, short)]
    pub follow: bool,
}

/// Current terminal screen selection, printed as plain text by default.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct PeekArguments {
    /// Exact unique session name.
    pub name: String,
    /// Opaque prior screen checkpoint for a bounded diff.
    #[arg(long)]
    pub checkpoint: Option<String>,
    /// Write ANSI screen bytes without text presentation.
    #[arg(long)]
    pub raw: bool,
}

/// Current managed-session selection.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct ContextArguments {
    /// Print only the session name, for custom prompt integrations.
    #[arg(long, conflicts_with_all = ["json", "jsonl"])]
    pub name: bool,
}

/// Optional shell prompt appearance.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub enum PromptArgument {
    /// Preserve the existing prompt.
    #[default]
    Off,
    /// Prefix the prompt with `[uc]`.
    Simple,
    /// Prefix with `[uc:session-name]`, falling back to `[uc]` if unavailable.
    Session,
}

impl PromptArgument {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Simple => "simple",
            Self::Session => "session",
        }
    }
}

/// Shell integration selection.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
#[command(
    after_help = "Examples:\n  uc init zsh\n  uc init bash --print\n  uc init fish --prompt session\n  uc init powershell --profile \"$PROFILE\""
)]
pub struct InitArguments {
    /// Target shell.
    #[arg(value_enum)]
    pub shell: ShellArgument,
    /// Opt-in prompt indicator; takes effect in new shells.
    #[arg(long, value_enum, default_value = "off")]
    pub prompt: PromptArgument,
    /// Print integration text without modifying shell files.
    #[arg(long)]
    pub print: bool,
    /// Explicit absolute startup file, such as PowerShell's $PROFILE.
    #[arg(long, conflicts_with = "print")]
    pub profile: Option<PathBuf>,
}

/// Supported shell adapters.
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShellArgument {
    /// Bourne Again Shell.
    Bash,
    /// Z shell.
    Zsh,
    /// Fish shell.
    Fish,
    /// PowerShell (use --profile "$PROFILE" for the current host).
    #[value(name = "powershell", alias = "pwsh")]
    #[serde(rename = "powershell")]
    PowerShell,
}

impl ShellArgument {
    /// Complete supported shell registry.
    pub const ALL: [Self; 4] = [Self::Bash, Self::Zsh, Self::Fish, Self::PowerShell];

    /// Returns the stable command-line and presentation name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bash => "bash",
            Self::Zsh => "zsh",
            Self::Fish => "fish",
            Self::PowerShell => "powershell",
        }
    }
}

/// Configuration operations.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct ConfigArguments {
    /// Configuration operation.
    #[command(subcommand)]
    pub action: ConfigAction,
}

/// Finite configuration operations.
#[derive(Clone, Debug, Subcommand, PartialEq, Eq)]
pub enum ConfigAction {
    /// Display effective secret-redacted configuration.
    Show,
    /// Validate effective configuration.
    Validate,
}

/// Detection profile operations.
#[derive(Clone, Args, PartialEq, Eq)]
pub struct ProfileArguments {
    /// Profile operation.
    #[command(subcommand)]
    pub action: ProfileAction,
}

/// Finite detection profile operations.
#[derive(Clone, Subcommand, PartialEq, Eq)]
pub enum ProfileAction {
    /// List one bounded page of profile summaries.
    List(ProfileListArguments),
    /// Inspect one complete profile contract.
    Inspect(ProfileNameArguments),
    /// Match process characteristics to configured profiles.
    Match(ProfileMatchArguments),
    /// Validate configured profiles.
    Lint(ProfileOptionalNameArguments),
    /// Evaluate one profile against bounded sample text.
    Test(ProfileTestArguments),
}

/// One named profile.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct ProfileNameArguments {
    /// Exact profile name.
    pub name: String,
}

/// Optional profile selection.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct ProfileOptionalNameArguments {
    /// Exact profile name; omit to validate all profiles.
    pub name: Option<String>,
}

/// Bounded profile page.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct ProfileListArguments {
    /// Maximum profile summaries in one page.
    #[arg(long, default_value_t = 100)]
    pub page_size: u32,
    /// Opaque continuation cursor.
    #[arg(long)]
    pub cursor: Option<String>,
}

/// Safe process characteristics used for deterministic profile matching.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct ProfileMatchArguments {
    /// Executable or binary name.
    #[arg(long)]
    pub executable: String,
    /// Ordered process argument; repeatable.
    #[arg(long = "arg")]
    pub arguments: Vec<String>,
    /// Environment variable name only; repeatable.
    #[arg(long = "env-name")]
    pub environment_names: Vec<String>,
}

/// Bounded standalone profile sample evaluation.
#[derive(Clone, Args, PartialEq, Eq)]
pub struct ProfileTestArguments {
    /// Exact profile name.
    pub name: String,
    /// Bounded current screen sample.
    #[arg(long, default_value = "")]
    pub screen_text: String,
    /// Bounded final logical line sample.
    #[arg(long, default_value = "")]
    pub last_line: String,
    /// Indicate that the sample cursor is not at the logical end.
    #[arg(long)]
    pub cursor_not_at_end: bool,
}

/// Local daemon process operations.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct DaemonArguments {
    /// Daemon operation.
    #[command(subcommand)]
    pub action: DaemonAction,
}

/// Daemon lifecycle operations.
#[derive(Clone, Debug, Subcommand, PartialEq, Eq)]
pub enum DaemonAction {
    /// Start the configured local daemon.
    Start,
    /// Stop the daemon and its processes immediately; preserve saved state.
    Stop {
        /// Allow accepted work and connected clients to drain before exit.
        #[arg(long)]
        graceful: bool,
        /// Maximum graceful drain interval (requires --graceful).
        #[arg(
            long,
            default_value = "5s",
            value_name = "DURATION",
            requires = "graceful"
        )]
        drain_timeout: String,
    },
    /// Replace the configured local daemon immediately.
    Restart {
        /// Allow accepted work and connected clients to drain before exit.
        #[arg(long)]
        graceful: bool,
        /// Maximum graceful drain interval (requires --graceful).
        #[arg(
            long,
            default_value = "5s",
            value_name = "DURATION",
            requires = "graceful"
        )]
        drain_timeout: String,
    },
    /// Display daemon identity and aggregate pressure.
    Status,
}

/// Confirmed local-state reset.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct ResetArguments {
    /// Confirm destructive reset without an interactive prompt.
    #[arg(long, short = 'y')]
    pub yes: bool,
}

/// Bounded operational diagnostic page.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct DebugArguments {
    /// Administrative operation, when diagnostics are not requested.
    #[command(subcommand)]
    pub action: Option<DebugAction>,
    /// Diagnostic section.
    #[arg(value_enum)]
    pub section: Option<DiagnosticSectionArgument>,
    /// Maximum diagnostic rows returned in one page.
    #[arg(long, default_value_t = 100)]
    pub page_size: u32,
    /// Opaque continuation cursor.
    #[arg(long)]
    pub cursor: Option<String>,
}

/// Diagnostic queries, local support collection, and authenticated administration.
#[derive(Clone, Debug, Subcommand, PartialEq, Eq)]
pub enum DebugAction {
    /// Collect a bounded support ZIP, including evidence from a stopped daemon.
    Collect(SupportCollectArguments),
    /// Create one bounded privacy-safe standalone diagnostic bundle.
    Bundle {
        /// Explicitly include one bounded terminal-output page for this session.
        #[arg(long, value_name = "SESSION")]
        include_terminal_content: Option<String>,
        /// First retained terminal byte to include.
        #[arg(long, requires = "include_terminal_content")]
        from_offset: Option<u64>,
        /// Maximum terminal bytes to include, bounded by the product protocol.
        #[arg(long, requires = "include_terminal_content")]
        max_bytes: Option<u32>,
        /// Maximum diagnostic rows returned in the bundle.
        #[arg(long, default_value_t = 100)]
        page_size: u32,
        /// Opaque continuation cursor returned by a prior bundle page.
        #[arg(long)]
        cursor: Option<String>,
    },
    /// Resolve one durable Unknown attempt from daemon-retained evidence.
    RepairAttempt {
        /// Stable operation-attempt identifier; no effect claim is accepted.
        attempt_id: String,
    },
}

/// Local support collection with optional authenticated live evidence.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct SupportCollectArguments {
    /// New archive path; existing files are never overwritten.
    #[arg(long, default_value = "uc-support.zip")]
    pub output: PathBuf,
    /// Error reference printed by the failing invocation.
    #[arg(long)]
    pub incident: Option<String>,
    /// Stable session ID to inspect without starting an absent daemon.
    #[arg(long, value_name = "SESSION_ID")]
    pub session: Option<String>,
    /// Active profile name to inspect.
    #[arg(long)]
    pub profile: Option<String>,
    /// Include private panic backtraces, which may contain build paths.
    #[arg(long)]
    pub include_backtraces: bool,
    /// Explicitly include bounded terminal content for this stable session ID.
    #[arg(long, value_name = "SESSION_ID")]
    pub include_terminal_content: Option<String>,
    /// Collect only local files; do not connect to the daemon.
    #[arg(long, conflicts_with = "include_terminal_content")]
    pub offline: bool,
}

/// Bounded diagnostic view selection.
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum DiagnosticSectionArgument {
    /// Negotiated physical connections.
    Connections,
    /// Logical streams and subscriptions.
    Streams,
    /// Slow consumers.
    SlowConsumers,
    /// Largest terminal output producers.
    OutputProducers,
    /// Session supervisor queues.
    Supervisors,
    /// Detection decision evidence.
    Detection,
    /// Persistence state.
    Persistence,
    /// Startup reconciliation outcomes.
    Reconciliation,
    /// Effective secret-redacted configuration.
    Configuration,
}

/// Shell completion generation.
#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct CompletionArguments {
    /// Target shell.
    #[arg(value_enum)]
    pub shell: ShellArgument,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use clap::{CommandFactory, Parser};

    use super::*;

    #[test]
    fn exec_selects_live_context_by_default_and_independent_launch_overrides() {
        for dir_launch in [false, true] {
            for env_launch in [false, true] {
                let mut argv = vec!["uc", "exec", "dev"];
                if dir_launch {
                    argv.push("--dir-launch");
                }
                if env_launch {
                    argv.push("--env-launch");
                }
                argv.extend(["--", "pwd"]);
                let parsed = NativeCliArguments::try_parse_from(argv).unwrap();
                let NativeCliCommand::Exec(exec) = parsed.command else {
                    panic!("expected exec")
                };
                assert_eq!(exec.dir_launch, dir_launch);
                assert_eq!(exec.env_launch, env_launch);
                assert_eq!(exec.command, ["pwd"]);
            }
        }
    }

    #[test]
    fn clap_surface_matches_the_central_retained_registry() {
        let parsed = NativeCliArguments::command()
            .get_subcommands()
            .map(|command| command.get_name().to_owned())
            .collect::<BTreeSet<_>>();
        let registered = CliCommand::ALL
            .into_iter()
            .map(|command| command.as_str().to_owned())
            .collect::<BTreeSet<_>>();

        assert_eq!(parsed, registered);
        assert!(!parsed.contains("stack"));

        // A newly registered command must appear exactly once in the grouped help.
        let grouped = HELP_SECTIONS
            .iter()
            .flat_map(|(_, names)| names.iter().copied())
            .collect::<Vec<_>>();
        let unique = grouped
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<BTreeSet<_>>();
        assert_eq!(grouped.len(), unique.len(), "duplicate help command");
        assert_eq!(unique, registered, "help sections must cover every command");
    }

    #[test]
    fn debug_preserves_diagnostics_and_adds_bundle_and_identity_only_attempt_repair() {
        let diagnostics = NativeCliArguments::try_parse_from(["uc", "debug", "reconciliation"])
            .expect("diagnostics syntax");
        let NativeCliCommand::Debug(diagnostics) = diagnostics.command else {
            panic!("expected debug diagnostics")
        };
        assert_eq!(
            diagnostics.section,
            Some(DiagnosticSectionArgument::Reconciliation)
        );
        assert!(diagnostics.action.is_none());

        let bundle = NativeCliArguments::try_parse_from([
            "uc",
            "debug",
            "bundle",
            "--include-terminal-content",
            "worker",
            "--from-offset",
            "7",
            "--max-bytes",
            "1024",
            "--page-size",
            "25",
        ])
        .expect("diagnostic bundle syntax");
        let NativeCliCommand::Debug(bundle) = bundle.command else {
            panic!("expected debug bundle")
        };
        assert_eq!(
            bundle.action,
            Some(DebugAction::Bundle {
                include_terminal_content: Some("worker".to_owned()),
                from_offset: Some(7),
                max_bytes: Some(1024),
                page_size: 25,
                cursor: None,
            })
        );
        assert!(bundle.section.is_none());

        let repair =
            NativeCliArguments::try_parse_from(["uc", "debug", "repair-attempt", "attempt-17"])
                .expect("attempt repair syntax");
        let NativeCliCommand::Debug(repair) = repair.command else {
            panic!("expected debug repair")
        };
        assert_eq!(
            repair.action,
            Some(DebugAction::RepairAttempt {
                attempt_id: "attempt-17".to_owned(),
            })
        );
        assert!(repair.section.is_none());

        assert!(
            NativeCliArguments::try_parse_from(["uc", "debug", "bundle", "--max-bytes", "1024",])
                .is_err()
        );
    }

    #[test]
    fn global_context_and_list_bounds_are_parsed_without_local_filtering() {
        let parsed = NativeCliArguments::try_parse_from([
            "uc",
            "--json",
            "--timeout",
            "250ms",
            "ls",
            "--execution",
            "quiescent",
            "--interaction",
            "requested",
            "--label",
            "env=prod",
            "--sort",
            "updated-at",
            "--descending",
            "--projection",
            "with-labels",
            "--page-size",
            "25",
            "--cursor",
            "opaque",
        ])
        .expect("valid bounded list");

        assert!(parsed.global_options().json);
        let NativeCliCommand::Ls(list) = parsed.command else {
            panic!("expected list command")
        };
        assert_eq!(list.execution, [ExecutionArgument::Quiescent]);
        assert_eq!(list.interaction, [InteractionArgument::Requested]);
        assert_eq!(list.label, ["env=prod"]);
        assert_eq!(list.sort, SessionSortArgument::UpdatedAt);
        assert_eq!(list.projection, SessionProjectionArgument::WithLabels);
        assert_eq!(list.page_size, Some(25));
        assert_eq!(list.cursor.as_deref(), Some("opaque"));
    }

    #[test]
    fn lifecycle_is_immediate_by_default_with_explicit_graceful_opt_in() {
        let parsed = NativeCliArguments::try_parse_from(["uc", "stop", "worker"]).unwrap();
        assert!(matches!(parsed.command, NativeCliCommand::Stop(a) if !a.graceful));
        let parsed = NativeCliArguments::try_parse_from(["uc", "restart", "worker"]).unwrap();
        assert!(matches!(parsed.command, NativeCliCommand::Restart(a) if !a.graceful));
        for command in ["stop", "restart"] {
            let parsed = NativeCliArguments::try_parse_from(["uc", "daemon", command]).unwrap();
            assert!(matches!(
                parsed.command,
                NativeCliCommand::Daemon(DaemonArguments {
                    action: DaemonAction::Stop {
                        graceful: false,
                        ..
                    } | DaemonAction::Restart {
                        graceful: false,
                        ..
                    }
                })
            ));
            assert!(
                NativeCliArguments::try_parse_from([
                    "uc",
                    command,
                    "worker",
                    "--force",
                    "--graceful"
                ])
                .is_err()
            );
            assert!(
                NativeCliArguments::try_parse_from([
                    "uc",
                    "daemon",
                    command,
                    "--drain-timeout",
                    "1s"
                ])
                .is_err()
            );
            assert!(
                NativeCliArguments::try_parse_from([
                    "uc",
                    "daemon",
                    command,
                    "--graceful",
                    "--drain-timeout",
                    "1s"
                ])
                .is_ok()
            );
        }
        assert!(
            NativeCliArguments::try_parse_from([
                "uc",
                "group",
                "delete",
                "project",
                "--stop",
                "--graceful"
            ])
            .is_ok()
        );
        assert!(
            NativeCliArguments::try_parse_from(["uc", "group", "delete", "project", "--graceful"])
                .is_err()
        );
    }

    #[test]
    fn stop_accepts_picker_or_one_explicit_selection() {
        for args in [vec!["uc", "stop"], vec!["uc", "stop", "--force"]] {
            let parsed = NativeCliArguments::try_parse_from(args).unwrap();
            assert!(matches!(parsed.command, NativeCliCommand::Stop(a) if a.interactive()));
        }
        assert!(NativeCliArguments::try_parse_from(["uc", "stop", "worker", "--all"]).is_err());
        assert!(
            NativeCliArguments::try_parse_from(["uc", "stop", "worker", "--label", "x=y"]).is_err()
        );
        assert!(
            NativeCliArguments::try_parse_from(["uc", "stop", "--all", "--label", "x=y"]).is_err()
        );
        assert!(NativeCliArguments::try_parse_from(["uc", "stop", "--all"]).is_ok());
    }

    #[test]
    fn clean_defaults_to_all_and_accepts_confirmation_flags() {
        for args in [
            vec!["uc", "clean"],
            vec!["uc", "clean", "--yes"],
            vec!["uc", "clean", "-y"],
            vec!["uc", "clean", "worker"],
            vec!["uc", "clean", "worker", "-y"],
        ] {
            let parsed = NativeCliArguments::try_parse_from(&args).expect("valid clean syntax");
            let NativeCliCommand::Clean(clean) = parsed.command else {
                panic!("expected clean command")
            };
            assert_eq!(
                clean.name.as_deref(),
                args.contains(&"worker").then_some("worker")
            );
            assert_eq!(clean.yes, args.contains(&"--yes") || args.contains(&"-y"));
        }
        assert!(NativeCliArguments::try_parse_from(["uc", "clean", "worker", "--all"]).is_err());
        assert!(NativeCliArguments::try_parse_from(["uc", "clean", "--all", "--yes"]).is_ok());
    }

    #[test]
    fn send_eof_is_an_explicit_exclusive_typed_operation() {
        let parsed = NativeCliArguments::try_parse_from(["uc", "send", "worker", "--eof"])
            .expect("typed standard-input close syntax");
        let NativeCliCommand::Send(send) = parsed.command else {
            panic!("expected send command")
        };

        assert_eq!(send.name, "worker");
        assert!(send.eof);
        assert!(send.input.is_none());
        assert!(
            NativeCliArguments::try_parse_from(["uc", "send", "worker", "text", "--eof"]).is_err()
        );
        assert!(
            NativeCliArguments::try_parse_from(["uc", "send", "worker", "--key", "enter", "--eof"])
                .is_err()
        );
    }

    #[test]
    fn removed_connection_and_gateway_flags_are_unknown() {
        assert!(
            NativeCliArguments::try_parse_from(["uc", "--socket", "/tmp/uc.sock", "ls"]).is_err()
        );
        assert!(NativeCliArguments::try_parse_from(["uc", "--no-auto-daemon", "ls"]).is_err());
        assert!(NativeCliArguments::try_parse_from(["uc", "gateway", "status"]).is_err());
    }

    #[test]
    fn start_keeps_command_arguments_after_the_separator() {
        let parsed = NativeCliArguments::try_parse_from([
            "uc",
            "start",
            "worker",
            "--env",
            "TOKEN=private",
            "--",
            "sh",
            "-c",
            "echo ok",
        ])
        .expect("valid start");
        let NativeCliCommand::Start(start) = parsed.command else {
            panic!("expected start command")
        };

        assert_eq!(start.name, "worker");
        assert_eq!(start.environment, ["TOKEN=private"]);
        assert_eq!(start.command, ["sh", "-c", "echo ok"]);
        assert_eq!(start.output_capture, OutputCaptureModeArgument::Terminal);
    }

    #[test]
    fn start_accepts_explicit_opaque_output_capture() {
        let parsed = NativeCliArguments::try_parse_from([
            "uc",
            "start",
            "worker",
            "--output-capture",
            "opaque",
            "--",
            "cat",
        ])
        .expect("valid opaque capture start");
        let NativeCliCommand::Start(start) = parsed.command else {
            panic!("expected start command")
        };

        assert_eq!(start.output_capture, OutputCaptureModeArgument::Opaque);
    }
}

/// Selects executable compatibility or immutable executable verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum LaunchModeArgument {
    /// Run the original installation path.
    Native,
    /// Run an immutable verified copy.
    Sealed,
}
