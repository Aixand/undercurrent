// SPDX-License-Identifier: Apache-2.0

//! Distribution-owned YAML definitions, embedded for standalone installations.
use crate::profile_yaml::{ProfileResult, decode};
use undercurrent_domain::{DetectionProfile, DetectionProfileLimits};

pub(crate) const EMBEDDED: &[(&str, &str)] = &[
    ("bash.yaml", include_str!("../profiles/bash.yaml")),
    ("zsh.yaml", include_str!("../profiles/zsh.yaml")),
    ("fish.yaml", include_str!("../profiles/fish.yaml")),
    ("cmd.yaml", include_str!("../profiles/cmd.yaml")),
    (
        "powershell.yaml",
        include_str!("../profiles/powershell.yaml"),
    ),
    ("python.yaml", include_str!("../profiles/python.yaml")),
    ("node.yaml", include_str!("../profiles/node.yaml")),
    ("server.yaml", include_str!("../profiles/server.yaml")),
    (
        "dev-server.yaml",
        include_str!("../profiles/dev-server.yaml"),
    ),
    ("codex.yaml", include_str!("../profiles/codex.yaml")),
    (
        "antigravity.yaml",
        include_str!("../profiles/antigravity.yaml"),
    ),
    (
        "claude-code.yaml",
        include_str!("../profiles/claude-code.yaml"),
    ),
    ("opencode.yaml", include_str!("../profiles/opencode.yaml")),
    ("aider.yaml", include_str!("../profiles/aider.yaml")),
    (
        "copilot-cli.yaml",
        include_str!("../profiles/copilot-cli.yaml"),
    ),
];

pub(crate) fn built_in_detection_profiles(
    limits: DetectionProfileLimits,
) -> ProfileResult<Vec<DetectionProfile>> {
    EMBEDDED
        .iter()
        .map(|(name, source)| {
            decode(source, limits).map_err(|error| format!("{name}: {error}").into())
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use std::future::Future;
    use std::num::NonZeroU16;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};
    use std::{collections::BTreeSet, time::Duration};
    use undercurrent_domain::*;

    use undercurrent_application::{
        Clock, DetectionProfileCatalog, DetectionProfileSelector, ForegroundEvidenceCollector,
        OutputOffset, ProfileMatchOutcome, ProfileRuleEvaluator, TerminalOutputLimits,
        TerminalOutputStore,
    };
    use undercurrent_domain::{
        ForegroundDecisionPolicy, ForegroundObservation, ProcessInstanceId, ProfileMatchTarget,
        ProfileSampleView, SessionId, TerminalSize, TimestampMillis,
    };
    use undercurrent_runtime::{
        CooperativeSignalStore, InMemoryDetectionProfileCatalog, RegexPatternEngine,
        RegexPatternEngineLimits, TerminalForegroundEvidenceCollector,
        TerminalForegroundEvidenceLimits, TerminalScreenLimits, VtTerminalScreenStore,
    };

    use super::*;

    const AGENT_TRAILING_SUBMIT_DELAY: Duration = Duration::from_millis(120);
    const CODEX_FOLDER_ACCESS: &str =
        include_str!("profile_builtins/fixtures/codex-folder-access.txt");
    const CODEX_PERMISSION_SETTINGS: &str = "Update Model Permissions\n\n  1. Ask for approval\n  2. Approve for me\n  3. Full Access\n› 4. Read Only (current)\n\n  enter select · esc back";
    // Captured from real 120x40 PTYs: Claude 2.1.299 and Antigravity 1.3.5.
    const CLAUDE_FOLDER_TRUST: &str =
        include_str!("profile_builtins/fixtures/claude-folder-trust.txt");
    const ANTIGRAVITY_FOLDER_TRUST: &str =
        include_str!("profile_builtins/fixtures/antigravity-folder-trust.txt");
    const CLAUDE_THEME_SELECTION: &str =
        include_str!("profile_builtins/fixtures/claude-theme-selection.txt");

    #[test]
    fn focused_builtins_are_valid_unique_and_operational() {
        let limits = limits();
        let patterns = RegexPatternEngine::new(RegexPatternEngineLimits {
            cached_patterns: 256,
            compiled_size_bytes: 1_048_576,
            determinize_size_bytes: 2_097_152,
            match_text_bytes: 131_072,
        })
        .expect("pattern engine");
        let catalog = InMemoryDetectionProfileCatalog::new(32).expect("catalog");

        let activation = catalog
            .replace(
                built_in_detection_profiles(limits).expect("builtins"),
                Vec::new(),
                &patterns,
            )
            .expect("valid focused catalog");

        assert_eq!(activation.profiles(), 15);
        assert_eq!(activation.warnings().len(), 1);
        assert!(activation.warnings().iter().all(|report| {
            report.profile().as_str() == "server"
                && report.issues().iter().all(|issue| {
                    issue.code()
                        == undercurrent_application::ProfileIssueCode::NotAutomaticallySelectable
                })
        }));
        assert_eq!(
            catalog
                .list(32)
                .expect("profile summaries")
                .into_iter()
                .map(|summary| summary.name().as_str().to_owned())
                .collect::<Vec<_>>(),
            vec![
                "aider",
                "antigravity",
                "bash",
                "claude-code",
                "cmd",
                "codex",
                "copilot-cli",
                "dev-server",
                "fish",
                "node",
                "opencode",
                "powershell",
                "python",
                "server",
                "zsh",
            ]
        );
    }

    #[test]
    fn windows_match_convention_select_builtins_without_weakening_signatures() {
        let patterns = RegexPatternEngine::new(RegexPatternEngineLimits {
            cached_patterns: 256,
            compiled_size_bytes: 1_048_576,
            determinize_size_bytes: 2_097_152,
            match_text_bytes: 131_072,
        })
        .unwrap();
        let catalog = InMemoryDetectionProfileCatalog::new(32).unwrap();
        catalog
            .replace(
                built_in_detection_profiles(limits()).unwrap(),
                Vec::new(),
                &patterns,
            )
            .unwrap();
        let selector = DetectionProfileSelector::new(&catalog, &patterns, 32).unwrap();
        for (program, arguments, expected) in [
            (r"C:\tools\CODEX.CMD", vec![], Some("codex")),
            ("NODE.EXE", vec![], Some("node")),
            (r"C:\Windows\System32\CMD.EXE", vec![], Some("cmd")),
            ("cmd", vec![], Some("cmd")),
            ("PwSh.EXE", vec![], Some("powershell")),
            ("PowerShell.EXE", vec![], Some("powershell")),
            ("cmd.txt", vec![], None),
            ("CLAUDE.exe", vec![], Some("claude-code")),
            ("AIDER.bat", vec![], Some("aider")),
            ("OpenCode.EXE", vec![], Some("opencode")),
            ("COPILOT.cmd", vec![], Some("copilot-cli")),
            ("AGY.EXE", vec![], Some("antigravity")),
            ("agy.txt", vec![], None),
            ("NPM.CMD", vec!["run", "dev"], Some("dev-server")),
            ("NPM.CMD", vec!["start"], None),
            ("NPM.CMD", vec!["RUN", "DEV"], None),
            ("CODEX.txt", vec![], None),
        ] {
            let target = ProfileMatchTarget::new(
                program,
                arguments.into_iter().map(str::to_owned).collect(),
                BTreeSet::new(),
                4096,
                8,
                8,
            )
            .unwrap()
            .with_match_convention(ProfileMatchConvention::Windows);
            let selected = match selector.select(&target).unwrap() {
                ProfileMatchOutcome::Matched { profile, .. } => Some(profile.as_str().to_owned()),
                ProfileMatchOutcome::NoMatch => None,
                other => panic!("unexpected selection for {program}: {other:?}"),
            };
            assert_eq!(selected.as_deref(), expected, "{program}");
        }
    }

    #[test]
    fn windows_shell_profiles_recognize_default_prompts() {
        let profiles = built_in_detection_profiles(limits()).unwrap();
        let patterns = RegexPatternEngine::new(RegexPatternEngineLimits {
            cached_patterns: 256,
            compiled_size_bytes: 1_048_576,
            determinize_size_bytes: 2_097_152,
            match_text_bytes: 131_072,
        })
        .unwrap();
        let evaluator = ProfileRuleEvaluator::new(&patterns);
        for (name, text, expected) in [
            ("cmd", r"C:\Users\example>", true),
            ("cmd", r"D:\A folder>", true),
            ("cmd", r"C:\>", true),
            ("cmd", r"C:\Users\example>echo hello", false),
            ("cmd", "Working...", false),
            ("cmd", "output >", false),
            ("cmd", r"PS C:\Users\example>", false),
            ("powershell", r"PS C:\Users\example> ", true),
            (
                "powershell",
                r"PS C:\Users\example> Write-Output hello",
                false,
            ),
            ("powershell", r"C:\Users\example>", false),
        ] {
            let profile = find_profile(&profiles, name);
            assert!(profile.matching_fallback());
            let sample =
                ProfileSampleView::new(Some(bottom_rows(2)), text, text, true, 4_096).unwrap();
            let selected = evaluator.select_foreground(profile, &[sample]).unwrap();
            let available = selected.iter().any(|selection| {
                matches!(&selection.claim, ForegroundClaim::Interaction(claim)
                    if claim.state() == InteractionState::Available)
            });
            assert_eq!(available, expected, "{name}: {text}");
        }
    }

    #[test]
    fn antigravity_matches_its_cli_and_reports_authentication_constraints() {
        let profiles = built_in_detection_profiles(limits()).unwrap();
        let patterns = RegexPatternEngine::new(RegexPatternEngineLimits {
            cached_patterns: 256,
            compiled_size_bytes: 1_048_576,
            determinize_size_bytes: 2_097_152,
            match_text_bytes: 131_072,
        })
        .unwrap();
        let catalog = InMemoryDetectionProfileCatalog::new(32).unwrap();
        catalog
            .replace(profiles.clone(), Vec::new(), &patterns)
            .unwrap();
        let selector = DetectionProfileSelector::new(&catalog, &patterns, 32).unwrap();
        for (binary, expected) in [("agy", true), ("agy-helper", false), ("antigravity", false)] {
            let target =
                ProfileMatchTarget::new(binary, Vec::new(), BTreeSet::new(), 4096, 8, 8).unwrap();
            let matched = matches!(selector.select(&target).unwrap(),
                ProfileMatchOutcome::Matched { profile, .. } if profile.as_str() == "antigravity");
            assert_eq!(matched, expected, "{binary}");
        }
        let evaluator = ProfileRuleEvaluator::new(&patterns);
        for text in [
            "Authentication required",
            "Error: authentication required. Run 'agy' to log in.",
        ] {
            let sample =
                ProfileSampleView::new(Some(bottom_rows(16)), text, text, true, 4096).unwrap();
            let selected = evaluator
                .select_foreground(find_profile(&profiles, "antigravity"), &[sample])
                .unwrap();
            assert!(selected.iter().any(|selection| matches!(&selection.claim,
                ForegroundClaim::Constraint(claim)
                    if claim.state() == RuntimeConstraintState::Present
                        && claim.semantic().is_some_and(|value| value.as_str() == "antigravity.authentication"))));
        }
    }

    #[test]
    fn development_server_signatures_are_exact_and_do_not_match_generic_npm_start() {
        let limits = limits();
        let patterns = RegexPatternEngine::new(RegexPatternEngineLimits {
            cached_patterns: 256,
            compiled_size_bytes: 1_048_576,
            determinize_size_bytes: 2_097_152,
            match_text_bytes: 131_072,
        })
        .expect("pattern engine");
        let catalog = InMemoryDetectionProfileCatalog::new(32).expect("catalog");
        catalog
            .replace(
                built_in_detection_profiles(limits).expect("builtins"),
                Vec::new(),
                &patterns,
            )
            .expect("valid focused catalog");
        let selector = DetectionProfileSelector::new(&catalog, &patterns, 32).expect("selector");

        for (binary, arguments) in [
            ("pnpm", vec!["run", "dev"]),
            ("npm", vec!["run", "start:dev"]),
            ("ng", vec!["serve"]),
            ("next", vec!["dev"]),
        ] {
            let target = ProfileMatchTarget::new(
                binary,
                arguments.into_iter().map(str::to_owned).collect(),
                BTreeSet::new(),
                4_096,
                8,
                8,
            )
            .expect("target");
            assert!(matches!(
                selector.select(&target).expect("selection"),
                ProfileMatchOutcome::Matched { profile, .. }
                    if profile.as_str() == "dev-server"
            ));
        }

        let generic = ProfileMatchTarget::new(
            "npm",
            vec!["start".to_owned()],
            BTreeSet::new(),
            4_096,
            8,
            8,
        )
        .expect("target");
        assert_eq!(
            selector.select(&generic).expect("selection"),
            ProfileMatchOutcome::NoMatch
        );
    }

    #[test]
    fn agent_profiles_own_paced_submission_while_other_profiles_remain_immediate() {
        let profiles = built_in_detection_profiles(limits()).expect("builtins");
        let paced = [
            "aider",
            "antigravity",
            "claude-code",
            "codex",
            "copilot-cli",
            "opencode",
        ];

        for profile in &profiles {
            if paced.contains(&profile.name().as_str()) {
                assert_eq!(
                    profile.input_delivery().trailing_submit_delay(),
                    Some(AGENT_TRAILING_SUBMIT_DELAY),
                    "{} must pace a trailing submit",
                    profile.name()
                );
            } else {
                assert_eq!(
                    profile.input_delivery(),
                    InputDeliveryPolicy::immediate(),
                    "{} must preserve immediate input",
                    profile.name()
                );
            }
        }
    }

    #[test]
    fn codex_composer_requires_idle_footer_and_rejects_startup_or_queue_snapshots() {
        let limits = limits();
        let profiles = built_in_detection_profiles(limits).expect("builtins");
        let patterns = RegexPatternEngine::new(RegexPatternEngineLimits {
            cached_patterns: 256,
            compiled_size_bytes: 1_048_576,
            determinize_size_bytes: 2_097_152,
            match_text_bytes: 131_072,
        })
        .expect("pattern engine");
        let evaluator = ProfileRuleEvaluator::new(&patterns);
        let codex = find_profile(&profiles, "codex");
        let cases = [
            (
                "composer and context without settled footer",
                "  › Ask Codex to do anything\n\n  100% context left",
                false,
            ),
            (
                "settled footer and context without composer",
                "  gpt-5.6-sol xhigh · /workspace\n  100% context left",
                false,
            ),
            (
                "bordered model loading",
                "╭────────────────────────╮\n│ model:     loading     │\n│ directory: /workspace  │\n╰────────────────────────╯\n\n  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace",
                false,
            ),
            (
                "bordered directory loading",
                "╭────────────────────────────────╮\n│ model:     gpt-5.6-sol xhigh   │\n│ directory: loading             │\n╰────────────────────────────────╯\n\n  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace",
                false,
            ),
            (
                "MCP starting",
                "  Starting MCP servers\n\n  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace",
                false,
            ),
            (
                "message queue active",
                "  › queued prompt\n\n  tab to queue message\n  100% context left\n  gpt-5.6-sol xhigh · /workspace",
                false,
            ),
            (
                "interruptible work active",
                "  › Ask Codex to do anything\n\n  esc to interrupt\n  gpt-5.6-sol xhigh · /workspace",
                false,
            ),
            (
                "codex 0.166 default effort footer",
                "  › Ask Codex to do anything\n\n  GPT-6.1-Sol default · /workspace\n  ← for agents · ? for shortcuts",
                true,
            ),
            (
                "Windows home-relative idle footer",
                "  › Ask Codex to do anything\n\n  GPT-6.1-Sol default · ~\\workspace",
                true,
            ),
            (
                "Windows absolute idle footer",
                "  › Ask Codex to do anything\n\n  GPT-6.1-Sol default · C:\\Users\\tester\\workspace",
                true,
            ),
            (
                "Windows home-relative working footer",
                "  › Ask Codex to do anything\n\n  esc to interrupt\n  GPT-6.1-Sol default · ~\\workspace",
                false,
            ),
            (
                "default effort while working",
                "  › Ask Codex to do anything\n\n  esc to interrupt\n  GPT-6.1-Sol default · /workspace",
                false,
            ),
            (
                "default effort during startup",
                "  Starting MCP servers\n\n  › Ask Codex to do anything\n\n  GPT-6.1-Sol default · /workspace",
                false,
            ),
            (
                "idle composer with direct separator",
                "  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace",
                true,
            ),
            (
                "idle composer with context status",
                "  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · 100% context left · /workspace",
                true,
            ),
            (
                "idle composer with whitespace separator",
                "  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh   /workspace",
                true,
            ),
            (
                "workspace trust veto",
                "Press enter to confirm\n1. Yes\n\n  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace",
                false,
            ),
            (
                "current workspace trust veto",
                "Press enter to continue\n\n  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace",
                false,
            ),
            (
                "tool permission veto",
                "Codex needs your approval\nMCP server to run tool\n\n  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace",
                false,
            ),
        ];

        for (name, text, expected) in cases {
            let sample =
                ProfileSampleView::new(None, text, text, true, 4_096).expect("Codex snapshot");
            let selected = evaluator
                .select_foreground(codex, &[sample])
                .expect("Codex profile evaluation");
            let composer_selected = selected.iter().any(|selection| {
                matches!(
                    &selection.claim,
                    ForegroundClaim::Interaction(claim)
                        if claim.state() == InteractionState::Available
                            && claim.semantic().is_some_and(|value| {
                                value.as_str() == "codex.composer"
                            })
                )
            });

            assert_eq!(composer_selected, expected, "unexpected result for {name}");
        }
    }

    #[test]
    fn codex_folder_access_requires_the_complete_current_dialog() {
        let profiles = built_in_detection_profiles(limits()).unwrap();
        let patterns = RegexPatternEngine::new(RegexPatternEngineLimits {
            cached_patterns: 256,
            compiled_size_bytes: 1_048_576,
            determinize_size_bytes: 2_097_152,
            match_text_bytes: 131_072,
        })
        .unwrap();
        let evaluator = ProfileRuleEvaluator::new(&patterns);
        let codex = find_profile(&profiles, "codex");
        let cases = [
            ("observed dialog", CODEX_FOLDER_ACCESS.to_owned(), true),
            (
                "CRLF dialog",
                CODEX_FOLDER_ACCESS.replace('\n', "\r\n"),
                true,
            ),
            (
                "quit selected",
                CODEX_FOLDER_ACCESS
                    .replace("› 1.", "  1.")
                    .replace("  2.", "❯ 2."),
                true,
            ),
            ("heading only", "  Folder access\n".to_owned(), false),
            ("question only", "Trust this folder?".to_owned(), false),
            (
                "unrelated menu",
                "› 1. Trust and continue\n  2. Quit\n  enter continue · esc quit".to_owned(),
                false,
            ),
            (
                "incomplete rendering",
                CODEX_FOLDER_ACCESS.replace("  enter continue · esc quit", ""),
                false,
            ),
            (
                "dialog followed by composer",
                format!(
                    "{CODEX_FOLDER_ACCESS}\n  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace"
                ),
                false,
            ),
        ];
        for (name, text, expected) in cases {
            let sample = ProfileSampleView::new(None, &text, "", true, 4_096).unwrap();
            let selected = evaluator.select_foreground(codex, &[sample]).unwrap();
            let requested = selected.iter().any(|selection| {
                matches!(&selection.claim, ForegroundClaim::Interaction(claim)
                    if claim.state() == InteractionState::Requested
                        && claim.modality() == Some(InteractionModality::Options)
                        && claim.semantic().is_some_and(|value| value.as_str() == "codex.workspace_trust"))
            });
            assert_eq!(requested, expected, "{name}");
        }
    }

    #[test]
    fn codex_runtime_foreground_resolves_working_evidence_without_assuming_readiness() {
        let limits = limits();
        let profiles = built_in_detection_profiles(limits).expect("builtins");
        let codex = find_profile(&profiles, "codex");
        assert!(codex.baseline_interaction().is_none());
        let codex_name = codex.name().clone();

        let cases = [
            (
                "composer and context without settled footer",
                "  › Ask Codex to do anything\n\n  100% context left",
                InteractionState::None,
                ExecutionState::Unknown,
                true,
            ),
            (
                "permission settings menu",
                CODEX_PERMISSION_SETTINGS,
                InteractionState::Requested,
                ExecutionState::Unknown,
                false,
            ),
            (
                "incomplete permission settings menu",
                "Update Model Permissions\n  1. Ask for approval\n  enter select · esc back",
                InteractionState::None,
                ExecutionState::Unknown,
                false,
            ),
            (
                "settled footer and context without composer",
                "  gpt-5.6-sol xhigh · /workspace\n  100% context left",
                InteractionState::None,
                ExecutionState::Unknown,
                true,
            ),
            (
                "bordered model loading",
                "╭────────────────────────╮\n│ model:     loading     │\n│ directory: /workspace  │\n╰────────────────────────╯\n\n  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace",
                InteractionState::None,
                ExecutionState::Unknown,
                true,
            ),
            (
                "bordered directory loading",
                "╭────────────────────────────────╮\n│ model:     gpt-5.6-sol xhigh   │\n│ directory: loading             │\n╰────────────────────────────────╯\n\n  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace",
                InteractionState::None,
                ExecutionState::Unknown,
                true,
            ),
            (
                "MCP starting",
                "  Starting MCP servers\n\n  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace",
                InteractionState::None,
                ExecutionState::Unknown,
                true,
            ),
            (
                "message queue active",
                "  › queued prompt\n\n  tab to queue message\n  100% context left\n  gpt-5.6-sol xhigh · /workspace",
                InteractionState::None,
                ExecutionState::Active,
                true,
            ),
            (
                "working timer above composer",
                "• Working (12s • esc to interrupt)\n\n  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace",
                InteractionState::None,
                ExecutionState::Active,
                true,
            ),
            (
                "working timer with details and queued draft",
                "• Reading sources (1h 02m 03s • esc to interrupt)\n  └ Inspecting the project\n\n  › queued prompt\n\n  tab to queue message\n  gpt-5.6-sol xhigh · /workspace",
                InteractionState::None,
                ExecutionState::Active,
                false,
            ),
            (
                "working timer without animation",
                "Working (1m 02s • esc to interrupt)\n\n  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · ~/workspace",
                InteractionState::None,
                ExecutionState::Active,
                true,
            ),
            (
                "Windows home-relative working timer",
                "Working (12s • esc to interrupt)\n\n  › Ask Codex to do anything\n\n  GPT-6.1-Sol default · ~\\workspace",
                InteractionState::None,
                ExecutionState::Active,
                true,
            ),
            (
                "quoted interrupt hint above idle composer",
                "The previous task showed esc to interrupt.\n\n  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace",
                InteractionState::None,
                ExecutionState::Unknown,
                true,
            ),
            (
                "old working indicator followed by completed output",
                "• Working (12s • esc to interrupt)\nTask completed.\n\n  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace",
                InteractionState::None,
                ExecutionState::Unknown,
                true,
            ),
            (
                "working hint without composer",
                "esc to interrupt\n  gpt-5.6-sol xhigh · /workspace",
                InteractionState::None,
                ExecutionState::Unknown,
                true,
            ),
            (
                "working hint without settled footer",
                "  › Ask Codex to do anything\n\n  esc to interrupt\n  100% context left",
                InteractionState::None,
                ExecutionState::Unknown,
                true,
            ),
            (
                "draft quotes a working hint",
                "  › explain esc to interrupt\n\n  gpt-5.6-sol xhigh · /workspace",
                InteractionState::None,
                ExecutionState::Unknown,
                true,
            ),
            (
                "quoted queue hint in output",
                "Documentation: tab to queue message\n\n  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace",
                InteractionState::None,
                ExecutionState::Unknown,
                true,
            ),
            (
                "startup still visible with working hint",
                "Starting MCP servers\n\n  › Ask Codex to do anything\n\n  esc to interrupt\n  gpt-5.6-sol xhigh · /workspace",
                InteractionState::None,
                ExecutionState::Unknown,
                true,
            ),
            (
                "approval still visible with working hint",
                "Codex needs your approval\nMCP server to run tool\n\n  › queued prompt\n\n  tab to queue message\n  gpt-5.6-sol xhigh · /workspace",
                InteractionState::Requested,
                ExecutionState::Unknown,
                false,
            ),
            (
                "interruptible work active",
                "  › Ask Codex to do anything\n\n  esc to interrupt\n  gpt-5.6-sol xhigh · /workspace",
                InteractionState::None,
                ExecutionState::Active,
                true,
            ),
            (
                "idle composer with direct separator",
                "  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace",
                InteractionState::Available,
                ExecutionState::Unknown,
                true,
            ),
            (
                "idle composer with context status",
                "  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · 100% context left · /workspace",
                InteractionState::Available,
                ExecutionState::Unknown,
                true,
            ),
            (
                "idle composer with whitespace separator",
                "  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh   /workspace",
                InteractionState::Available,
                ExecutionState::Unknown,
                true,
            ),
            (
                "workspace trust veto at top",
                "Press enter to confirm\n1. Yes\n\n  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace",
                InteractionState::None,
                ExecutionState::Unknown,
                true,
            ),
            (
                "tool permission veto at top",
                "Codex needs your approval\nMCP server to run tool\n\n  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace",
                InteractionState::None,
                ExecutionState::Unknown,
                true,
            ),
            (
                "workspace trust request",
                "Press enter to confirm\n1. Yes",
                InteractionState::Requested,
                ExecutionState::Unknown,
                false,
            ),
            (
                "tool permission request",
                "Codex needs your approval\nMCP server to run tool",
                InteractionState::Requested,
                ExecutionState::Unknown,
                false,
            ),
        ];
        let patterns = Arc::new(
            RegexPatternEngine::new(RegexPatternEngineLimits {
                cached_patterns: 256,
                compiled_size_bytes: 1_048_576,
                determinize_size_bytes: 2_097_152,
                match_text_bytes: 131_072,
            })
            .expect("pattern engine"),
        );
        let catalog =
            Arc::new(InMemoryDetectionProfileCatalog::new(32).expect("profile catalog limit"));
        catalog
            .replace(profiles, Vec::new(), patterns.as_ref())
            .expect("profile activation");
        let screens = Arc::new(
            VtTerminalScreenStore::new(TerminalScreenLimits {
                scrollback_lines: 0,
                sessions: cases.len() + 3,
                snapshot_bytes: 32_768,
            })
            .expect("screen limits"),
        );
        let output = TerminalOutputStore::new(TerminalOutputLimits {
            per_session_bytes: 32_768,
            global_bytes: 262_144,
            chunk_bytes: 4_096,
            append_bytes: 32_768,
            replay_bytes: 32_768,
            sessions: cases.len() + 3,
            subscriptions: 1,
        })
        .expect("output limits");
        let observed_at = TimestampMillis::new(100);
        let collector = TerminalForegroundEvidenceCollector::new(
            Arc::new(ProfileTestClock(observed_at)),
            catalog,
            patterns,
            screens.clone(),
            Arc::new(
                CooperativeSignalStore::new(cases.len() + 3).expect("cooperative signal limit"),
            ),
            TerminalForegroundEvidenceLimits {
                tracked_sessions: cases.len() + 3,
                profile_samples: 16,
                profile_sample_bytes: 32_768,
                motion_rows: 98,
                motion_confidence: Confidence::from_basis_points(8_500).expect("motion confidence"),
                server_idle_confidence: Confidence::from_basis_points(7_000)
                    .expect("idle confidence"),
            },
        )
        .expect("foreground collector");
        let decision = ForegroundDecisionPolicy::new(
            16,
            Duration::from_secs(1),
            Confidence::from_basis_points(1).expect("decision confidence"),
        )
        .expect("decision policy");
        let terminal = TerminalSize::new(120, 40, 120, 40).expect("terminal size");
        let top_aligned_prefix = concat!(
            "╭────────────────────────────────╮\n",
            "│ >_ OpenAI Codex (v0.149.0)     │\n",
            "│                                │\n",
            "│ model: gpt-5.6-sol xhigh       │\n",
            "│ directory: /workspace          │\n",
            "╰────────────────────────────────╯\n",
            "\n",
            "  Tip: Use /feedback for feedback\n",
            "\n",
            "\n",
        );
        let settled_layout = format!(
            "{top_aligned_prefix}{}",
            "  › Ask Codex to do anything\n\n  gpt-5.6-sol xhigh · /workspace"
        );
        let composer_row = settled_layout
            .lines()
            .position(|line| line.trim_start().starts_with('›'))
            .expect("top-aligned composer")
            + 1;
        let footer_row = settled_layout
            .lines()
            .position(|line| line.contains("gpt-5.6-sol xhigh · /workspace"))
            .expect("top-aligned settled footer")
            + 1;
        assert_eq!(composer_row, 11);
        assert_eq!(footer_row, 13);
        assert!(
            footer_row < 25,
            "settled fixture must remain above the legacy bottom-16 sample"
        );

        for (index, (name, text, expected, expected_execution, top_aligned)) in
            cases.into_iter().enumerate()
        {
            let session =
                SessionId::new(format!("codex-profile-{index}")).expect("session identity");
            let process =
                ProcessInstanceId::new(format!("process-{index}")).expect("process identity");
            screens
                .begin_session(session.clone(), terminal, OutputOffset::ZERO)
                .expect("screen begins");
            let rendered = if top_aligned {
                format!(
                    "\x1b[2J\x1b[1;1H{}",
                    format!("{top_aligned_prefix}{text}").replace('\n', "\r\n")
                )
            } else {
                format!("\x1b[2J\x1b[30;1H{}", text.replace('\n', "\r\n"))
            };
            screens
                .observe_output(&output, &session, rendered.as_bytes())
                .expect("screen output");
            let evidence = block_on_profile_test(collector.collect(
                &session,
                &process,
                Some(&codex_name),
                &ForegroundObservation::unknown(observed_at),
            ))
            .expect("foreground evidence");
            let observation = decision
                .decide(evidence, observed_at)
                .expect("foreground decision")
                .observation()
                .clone();

            assert_eq!(
                observation.interaction().state(),
                expected,
                "unexpected interaction for {name}"
            );
            assert_eq!(
                observation.execution().state(),
                expected_execution,
                "unexpected execution for {name}"
            );

            // A second collection has no terminal motion: the visible working UI
            // must support Active on its own, without marking the composer ready.
            let evidence = block_on_profile_test(collector.collect(
                &session,
                &process,
                Some(&codex_name),
                &observation,
            ))
            .expect("unchanged screen evidence");
            let repeated = decision
                .decide(evidence, observed_at)
                .expect("repeated decision");
            assert_eq!(
                repeated.observation().execution().state(),
                expected_execution,
                "{name}"
            );
            assert_eq!(
                repeated.observation().interaction().state(),
                expected,
                "{name}"
            );
            if expected_execution == ExecutionState::Active {
                screens
                    .observe_output(&output, &session, b"\x1b[2J\x1b[1;1HUnrecognized screen")
                    .expect("replace working indicator");
                // The redraw itself is motion. Once it settles, a vanished
                // indicator must not leave a sticky Active observation.
                for expected in [ExecutionState::Active, ExecutionState::Unknown] {
                    let evidence = block_on_profile_test(collector.collect(
                        &session,
                        &process,
                        Some(&codex_name),
                        repeated.observation(),
                    ))
                    .expect("replacement evidence");
                    let next = decision
                        .decide(evidence, observed_at)
                        .expect("replacement decision");
                    assert_eq!(
                        next.observation().execution().state(),
                        expected,
                        "vanished indicator for {name}"
                    );
                    assert_eq!(
                        next.observation().interaction().state(),
                        InteractionState::None
                    );
                }
            }
        }

        let session = SessionId::new("codex-profile-current-trust").expect("session identity");
        let process = ProcessInstanceId::new("process-current-trust").expect("process identity");
        let terminal = TerminalSize::new(80, 24, 80, 24).expect("current Codex terminal size");
        screens
            .begin_session(session.clone(), terminal, OutputOffset::ZERO)
            .expect("screen begins");
        let screen = concat!(
            "> You are in /workspace/example\n",
            "\n",
            "  Do you trust the contents of this directory? Working with untrusted contents\n",
            "  comes with higher risk of prompt injection. Trusting the directory allows\n",
            "  project-local config, hooks, and exec policies to load.\n",
            "\n",
            "› 1. Yes, continue\n",
            "  2. No, quit\n",
            "\n",
            "  Press enter to continue",
        );
        let rendered = format!("\x1b[2J\x1b[1;1H{}", screen.replace('\n', "\r\n"));
        screens
            .observe_output(&output, &session, rendered.as_bytes())
            .expect("screen output");
        let evidence = block_on_profile_test(collector.collect(
            &session,
            &process,
            Some(&codex_name),
            &ForegroundObservation::unknown(observed_at),
        ))
        .expect("foreground evidence");
        let observation = decision
            .decide(evidence, observed_at)
            .expect("foreground decision")
            .observation()
            .clone();

        assert_eq!(
            observation.interaction().state(),
            InteractionState::Requested
        );
        assert_eq!(
            observation.interaction().modality(),
            Some(InteractionModality::Options)
        );
        assert_eq!(
            observation
                .interaction()
                .semantic()
                .map(ObservationSemanticId::as_str),
            Some("codex.workspace_trust")
        );

        for (columns, rows) in [(80, 24), (236, 98)] {
            let session = SessionId::new(format!("codex-folder-access-{columns}-{rows}")).unwrap();
            let process =
                ProcessInstanceId::new(format!("folder-access-{columns}-{rows}")).unwrap();
            screens
                .begin_session(
                    session.clone(),
                    TerminalSize::new(columns, rows, 236, 98).unwrap(),
                    OutputOffset::ZERO,
                )
                .unwrap();
            let rendered = format!(
                "\x1b[2J\x1b[1;1H{}",
                CODEX_FOLDER_ACCESS.replace('\n', "\r\n")
            );
            screens
                .observe_output(&output, &session, rendered.as_bytes())
                .unwrap();
            let evidence = block_on_profile_test(collector.collect(
                &session,
                &process,
                Some(&codex_name),
                &ForegroundObservation::unknown(observed_at),
            ))
            .unwrap();
            let decision = decision.decide(evidence, observed_at).unwrap();
            let interaction = decision.observation().interaction();
            assert_eq!(
                interaction.state(),
                InteractionState::Requested,
                "{columns}x{rows}"
            );
            assert_eq!(interaction.modality(), Some(InteractionModality::Options));
            assert_eq!(
                interaction.semantic().map(ObservationSemanticId::as_str),
                Some("codex.workspace_trust")
            );
        }
    }

    #[test]
    fn agent_profiles_classify_representative_current_screens_without_canonical_turns() {
        let limits = limits();
        let profiles = built_in_detection_profiles(limits).expect("builtins");
        let patterns = RegexPatternEngine::new(RegexPatternEngineLimits {
            cached_patterns: 256,
            compiled_size_bytes: 1_048_576,
            determinize_size_bytes: 2_097_152,
            match_text_bytes: 131_072,
        })
        .expect("pattern engine");
        let evaluator = ProfileRuleEvaluator::new(&patterns);
        let cases = [
            (
                "antigravity",
                ANTIGRAVITY_FOLDER_TRUST,
                None,
                "antigravity.workspace_trust",
                InteractionState::Requested,
            ),
            (
                "claude-code",
                "Do you want to run this command?",
                Some(bottom_rows(16)),
                "claude-code.permission",
                InteractionState::Requested,
            ),
            (
                "codex",
                "This command needs your approval",
                Some(bottom_rows(16)),
                "codex.permission",
                InteractionState::Requested,
            ),
            (
                "opencode",
                "Permission required\nAllow once",
                Some(bottom_rows(16)),
                "opencode.permission",
                InteractionState::Requested,
            ),
            (
                "copilot-cli",
                "Do you trust the files in this folder?",
                Some(bottom_rows(16)),
                "copilot-cli.workspace_trust",
                InteractionState::Requested,
            ),
            (
                "copilot-cli",
                "Path confirmation (1 remaining)\nAllow directory access\nDo you want to allow this?\n1. Yes\n2. Yes, and add these directories to the allowed list\n3. No (Esc)",
                Some(bottom_rows(16)),
                "copilot-cli.permission",
                InteractionState::Requested,
            ),
            (
                "aider",
                ">",
                Some(cursor_rows(1)),
                "aider.composer",
                InteractionState::Available,
            ),
        ];

        for (name, text, region, semantic, state) in cases {
            let profile = find_profile(&profiles, name);
            let sample = ProfileSampleView::new(region, text, text, true, 4_096)
                .expect("representative sample");
            let selected = evaluator
                .select_foreground(profile, &[sample])
                .expect("profile evaluation");
            assert!(
                selected.iter().any(|selection| {
                    matches!(
                        &selection.claim,
                        ForegroundClaim::Interaction(claim)
                            if claim.state() == state
                                && claim.semantic().is_some_and(|value| value.as_str() == semantic)
                    )
                }),
                "{name} did not select {semantic}"
            );
            if name == "codex" {
                assert!(profile.baseline_interaction().is_none());
            } else {
                assert_eq!(
                    profile
                        .baseline_interaction()
                        .expect("agent baseline")
                        .state(),
                    InteractionState::Available
                );
            }
        }

        let claude = find_profile(&profiles, "claude-code");
        let expired_login = ProfileSampleView::new(
            Some(bottom_rows(16)),
            "Login expired · Please run /login",
            "Login expired · Please run /login",
            true,
            4_096,
        )
        .expect("expired login sample");
        assert!(
            evaluator
                .select_foreground(claude, &[expired_login])
                .expect("claude constraint evaluation")
                .iter()
                .any(|selection| matches!(
                    &selection.claim,
                    ForegroundClaim::Constraint(claim)
                        if claim.state() == RuntimeConstraintState::Present
                            && claim.semantic().is_some_and(|value| {
                                value.as_str() == "claude-code.error"
                            })
                ))
        );
    }

    #[test]
    fn native_agent_startup_decisions_require_a_complete_whole_screen_dialog() {
        let profiles = built_in_detection_profiles(limits()).unwrap();
        let patterns = RegexPatternEngine::new(RegexPatternEngineLimits {
            cached_patterns: 256,
            compiled_size_bytes: 1_048_576,
            determinize_size_bytes: 2_097_152,
            match_text_bytes: 131_072,
        })
        .unwrap();
        let evaluator = ProfileRuleEvaluator::new(&patterns);
        for (name, text, semantic, required_choice) in [
            (
                "claude-code",
                CLAUDE_FOLDER_TRUST,
                "claude-code.workspace_trust",
                "Yes, I trust this folder",
            ),
            (
                "antigravity",
                ANTIGRAVITY_FOLDER_TRUST,
                "antigravity.workspace_trust",
                "Yes, I trust this folder",
            ),
            (
                "claude-code",
                CLAUDE_THEME_SELECTION,
                "claude-code.theme_selection",
                "Light mode",
            ),
        ] {
            let profile = find_profile(&profiles, name);
            for (screen, expected) in [
                (text.to_owned(), true),
                (text.replace(required_choice, ""), false),
                (
                    "Do you trust the contents of this project?".to_owned(),
                    false,
                ),
            ] {
                let sample = ProfileSampleView::new(None, &screen, "", true, 16_384).unwrap();
                let selected = evaluator.select_foreground(profile, &[sample]).unwrap();
                let requested = selected.iter().any(|selection| matches!(&selection.claim,
                    ForegroundClaim::Interaction(claim) if claim.state() == InteractionState::Requested
                        && claim.modality() == Some(InteractionModality::Options)
                        && claim.semantic().is_some_and(|value| value.as_str() == semantic)));
                assert_eq!(requested, expected, "{name}");
            }
        }
    }

    fn bottom_rows(rows: u16) -> ProfileRegion {
        ProfileRegion::BottomRows {
            rows: NonZeroU16::new(rows).unwrap_or(NonZeroU16::MIN),
            anchor: ScreenAnchor::Any,
            columns: None,
        }
    }

    fn cursor_rows(rows: u16) -> ProfileRegion {
        ProfileRegion::BottomRows {
            rows: NonZeroU16::new(rows).unwrap_or(NonZeroU16::MIN),
            anchor: ScreenAnchor::Cursor,
            columns: None,
        }
    }

    fn find_profile<'a>(profiles: &'a [DetectionProfile], name: &str) -> &'a DetectionProfile {
        profiles
            .iter()
            .find(|profile| profile.name().as_str() == name)
            .expect("profile exists")
    }

    struct ProfileTestClock(TimestampMillis);

    impl Clock for ProfileTestClock {
        fn now(&self) -> TimestampMillis {
            self.0
        }
    }

    fn block_on_profile_test<F: Future>(future: F) -> F::Output {
        let waker = Waker::from(Arc::new(ProfileTestWake));
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    struct ProfileTestWake;

    impl Wake for ProfileTestWake {
        fn wake(self: Arc<Self>) {}
    }

    pub(crate) fn limits() -> DetectionProfileLimits {
        DetectionProfileLimits {
            max_description_bytes: 4_096,
            max_binary_patterns: 32,
            max_argument_patterns: 32,
            max_environment_names: 32,
            max_environment_name_bytes: 256,
            max_foreground_rules: 64,
            max_background_rules: 64,
            max_patterns_per_rule: 32,
            max_pattern_bytes: 4_096,
            max_trait_needles: 64,
            max_trait_text_bytes: 1_024,
            max_input_submission_delay_millis: 2_000,
        }
    }
}
