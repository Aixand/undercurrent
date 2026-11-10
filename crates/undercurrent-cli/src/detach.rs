// SPDX-License-Identifier: Apache-2.0

//! Bounded detach-sequence parsing and overlap-safe interactive input matching.

use std::error::Error;
use std::fmt::{Display, Formatter};

use undercurrent_protocol::TerminalInputDto;

/// Maximum keys accepted in one detach sequence.
pub const MAXIMUM_DETACH_KEYS: usize = 16;

/// One key that can participate in an interactive detach sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetachKey {
    /// One ASCII control chord from Ctrl-A through Ctrl-Z.
    Control(char),
    /// One Alt-modified ASCII character.
    Alt(char),
    /// One literal ASCII character.
    Literal(char),
}

/// One translated input item and its optional detach-matching identity.
#[derive(Clone, PartialEq, Eq)]
pub struct InteractiveInput {
    /// Protocol input forwarded to the managed terminal when not consumed.
    pub item: TerminalInputDto,
    /// Exact logical key used only by the local detach matcher.
    pub detach_key: Option<DetachKey>,
}

/// Payload-safe detach specification failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetachSequenceError {
    /// A comma-delimited component was empty.
    EmptyComponent {
        /// Zero-based component index without retaining its value.
        index: usize,
    },
    /// A component was neither Ctrl-A..Ctrl-Z nor one ASCII literal.
    InvalidComponent {
        /// Zero-based component index without retaining its value.
        index: usize,
    },
    /// A non-empty sequence contained fewer than two keys.
    TooShort,
    /// A sequence exceeded the bounded matcher capacity.
    TooLong {
        /// Maximum accepted key count.
        maximum: usize,
    },
}

impl Display for DetachSequenceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyComponent { index } => write!(
                formatter,
                "detach key component {} cannot be empty",
                index.saturating_add(1)
            ),
            Self::InvalidComponent { index } => write!(
                formatter,
                "detach key component {} must be ctrl-a through ctrl-z, alt-a through alt-z, or one ASCII character",
                index.saturating_add(1)
            ),
            Self::TooShort => formatter.write_str("detach sequence must contain at least two keys"),
            Self::TooLong { maximum } => {
                write!(formatter, "detach sequence cannot exceed {maximum} keys")
            }
        }
    }
}

impl Error for DetachSequenceError {}

/// Result of feeding one translated terminal event into a detach matcher.
pub struct DetachMatch {
    /// Whether the complete configured sequence was consumed.
    pub detached: bool,
    /// Input proven not to be part of a detach sequence, in original order.
    pub passthrough: Vec<TerminalInputDto>,
}

/// Bounded overlap-safe detach matcher.
pub struct DetachMatcher {
    sequence: Vec<DetachKey>,
    pending: Vec<InteractiveInput>,
    prefix: Option<DetachKey>,
    action: Option<ShortcutAction>,
    workspace: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShortcutAction {
    Help,
    Sessions,
    Create,
    Next,
    Previous,
    Copy,
    Paste,
    SplitColumns,
    SplitRows,
    Focus,
    Zoom,
    ClosePane,
}

impl DetachMatcher {
    pub(crate) fn with_prefix(specification: &str) -> Result<Self, DetachSequenceError> {
        let prefix = parse_key(specification, 0)?;
        Ok(Self {
            sequence: Vec::new(),
            pending: Vec::new(),
            prefix: Some(prefix),
            action: None,
            workspace: false,
        })
    }

    pub(crate) fn for_workspace(mut self) -> Self {
        self.workspace = true;
        self
    }

    pub(crate) fn prefix_pending(&self) -> bool {
        self.prefix.is_some() && !self.pending.is_empty()
    }

    pub(crate) fn take_action(&mut self) -> Option<ShortcutAction> {
        self.action.take()
    }

    pub(crate) fn has_action(&self) -> bool {
        self.action.is_some()
    }

    pub(crate) fn prefix_label(&self) -> String {
        match self.prefix {
            Some(DetachKey::Alt(c)) => format!("Alt+{}", c.to_ascii_uppercase()),
            Some(DetachKey::Control(c)) => format!("Ctrl+{}", c.to_ascii_uppercase()),
            Some(DetachKey::Literal(c)) => c.to_string(),
            None => String::new(),
        }
    }

    pub(crate) fn detach_label(&self) -> String {
        if self.prefix.is_some() {
            return format!("{}, d: detach", self.prefix_label());
        }
        if self.sequence.is_empty() {
            return "detach keys disabled".to_owned();
        }
        let keys: Vec<_> = self
            .sequence
            .iter()
            .map(|key| match key {
                DetachKey::Alt(c) => format!("Alt+{}", c.to_ascii_uppercase()),
                DetachKey::Control(c) => format!("Ctrl+{}", c.to_ascii_uppercase()),
                DetachKey::Literal(c) => c.to_string(),
            })
            .collect();
        format!("{}: detach", keys.join(", "))
    }

    /// Parses a comma-delimited sequence such as `ctrl-b,d`.
    ///
    /// An empty specification disables detaching. Non-empty sequences require
    /// between two and [`MAXIMUM_DETACH_KEYS`] entries. The original string is
    /// never retained. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a payload-safe structural error for an invalid component or bound.
    pub fn parse(specification: &str) -> Result<Self, DetachSequenceError> {
        if specification.is_empty() {
            return Ok(Self {
                sequence: Vec::new(),
                pending: Vec::new(),
                prefix: None,
                action: None,
                workspace: false,
            });
        }
        let mut sequence = Vec::new();
        for (index, component) in specification.split(',').enumerate() {
            if sequence.len() == MAXIMUM_DETACH_KEYS {
                return Err(DetachSequenceError::TooLong {
                    maximum: MAXIMUM_DETACH_KEYS,
                });
            }
            let component = component.trim();
            if component.is_empty() {
                return Err(DetachSequenceError::EmptyComponent { index });
            }
            sequence.push(parse_key(component, index)?);
        }
        if sequence.len() < 2 {
            return Err(DetachSequenceError::TooShort);
        }
        Ok(Self {
            pending: Vec::with_capacity(sequence.len()),
            sequence,
            prefix: None,
            action: None,
            workspace: false,
        })
    }

    /// Consumes one translated input item without losing mismatched prefixes.
    ///
    /// Partial matches remain buffered until they either complete or are
    /// disproved. On mismatch the earliest item is released until the remaining
    /// suffix is a valid prefix, which preserves overlapping sequences. This
    /// function performs no IO and does not panic.
    pub fn feed(&mut self, input: InteractiveInput) -> DetachMatch {
        if let Some(prefix) = self.prefix {
            if let Some(pending) = self.pending.pop() {
                let mut result = DetachMatch {
                    detached: false,
                    passthrough: Vec::new(),
                };
                if input.detach_key == Some(prefix) {
                    result.passthrough.push(pending.item);
                } else if matches!(
                    input.item,
                    TerminalInputDto::Key {
                        key: undercurrent_protocol::KeyDto::Escape
                    }
                ) {
                    // Escape cancels the prefix locally.
                } else {
                    match input.detach_key {
                        Some(DetachKey::Literal('c')) if self.workspace => {
                            self.action = Some(ShortcutAction::Create)
                        }
                        Some(DetachKey::Literal('n')) if self.workspace => {
                            self.action = Some(ShortcutAction::Next)
                        }
                        Some(DetachKey::Literal('p')) if self.workspace => {
                            self.action = Some(ShortcutAction::Previous)
                        }
                        Some(DetachKey::Literal('[')) => self.action = Some(ShortcutAction::Copy),
                        Some(DetachKey::Literal(']')) => self.action = Some(ShortcutAction::Paste),
                        Some(DetachKey::Literal('%')) if !self.workspace => {
                            self.action = Some(ShortcutAction::SplitColumns)
                        }
                        Some(DetachKey::Literal('"')) if !self.workspace => {
                            self.action = Some(ShortcutAction::SplitRows)
                        }
                        Some(DetachKey::Literal('o')) if !self.workspace => {
                            self.action = Some(ShortcutAction::Focus)
                        }
                        Some(DetachKey::Literal('z')) if !self.workspace => {
                            self.action = Some(ShortcutAction::Zoom)
                        }
                        Some(DetachKey::Literal('x')) if !self.workspace => {
                            self.action = Some(ShortcutAction::ClosePane)
                        }
                        Some(DetachKey::Literal('d')) => result.detached = true,
                        Some(DetachKey::Literal('s')) => {
                            self.action = Some(ShortcutAction::Sessions)
                        }
                        Some(DetachKey::Literal('?')) => self.action = Some(ShortcutAction::Help),
                        _ => result.passthrough.extend([pending.item, input.item]),
                    }
                }
                return result;
            }
            if input.detach_key == Some(prefix) {
                self.pending.push(input);
                return DetachMatch {
                    detached: false,
                    passthrough: Vec::new(),
                };
            }
        }
        if self.sequence.is_empty() {
            return DetachMatch {
                detached: false,
                passthrough: vec![input.item],
            };
        }
        self.pending.push(input);
        if self.pending_matches_prefix() && self.pending.len() == self.sequence.len() {
            self.pending.clear();
            return DetachMatch {
                detached: true,
                passthrough: Vec::new(),
            };
        }

        let mut passthrough = Vec::new();
        while !self.pending_matches_prefix() {
            passthrough.push(self.pending.remove(0).item);
        }
        DetachMatch {
            detached: false,
            passthrough,
        }
    }

    /// Releases any incomplete prefix when attachment ends for another reason.
    ///
    /// The returned items preserve their original order. This function performs
    /// no IO and does not panic.
    pub fn drain_pending(&mut self) -> Vec<TerminalInputDto> {
        self.pending.drain(..).map(|input| input.item).collect()
    }

    fn pending_matches_prefix(&self) -> bool {
        self.pending
            .iter()
            .zip(&self.sequence)
            .all(|(input, expected)| input.detach_key.as_ref() == Some(expected))
    }
}

fn parse_key(component: &str, index: usize) -> Result<DetachKey, DetachSequenceError> {
    let lower = component.to_ascii_lowercase();
    if let Some(alt) = lower.strip_prefix("alt-") {
        let mut characters = alt.chars();
        if let Some(character) = characters.next()
            && character.is_ascii_alphabetic()
            && characters.next().is_none()
        {
            return Ok(DetachKey::Alt(character));
        }
        return Err(DetachSequenceError::InvalidComponent { index });
    }
    if let Some(control) = lower.strip_prefix("ctrl-") {
        let mut characters = control.chars();
        let Some(character) = characters.next() else {
            return Err(DetachSequenceError::InvalidComponent { index });
        };
        if characters.next().is_none() && character.is_ascii_lowercase() {
            return Ok(DetachKey::Control(character));
        }
        return Err(DetachSequenceError::InvalidComponent { index });
    }
    let mut characters = component.chars();
    let Some(character) = characters.next() else {
        return Err(DetachSequenceError::EmptyComponent { index });
    };
    if characters.next().is_none() && character.is_ascii() {
        Ok(DetachKey::Literal(character))
    } else {
        Err(DetachSequenceError::InvalidComponent { index })
    }
}

#[cfg(test)]
mod tests {
    use undercurrent_protocol::KeyDto;

    use super::*;

    fn literal(character: char) -> InteractiveInput {
        InteractiveInput {
            item: TerminalInputDto::Text {
                text: character.to_string(),
            },
            detach_key: Some(DetachKey::Literal(character)),
        }
    }

    fn control(character: char) -> InteractiveInput {
        InteractiveInput {
            item: TerminalInputDto::Key {
                key: KeyDto::Control { character },
            },
            detach_key: Some(DetachKey::Control(character)),
        }
    }

    fn alt_u() -> InteractiveInput {
        InteractiveInput {
            item: TerminalInputDto::Raw {
                bytes: b"\x1bu".to_vec(),
            },
            detach_key: Some(DetachKey::Alt('u')),
        }
    }

    #[test]
    fn workspace_actions_are_local_to_workspace_mode() {
        for (key, action) in [
            ('c', ShortcutAction::Create),
            ('n', ShortcutAction::Next),
            ('p', ShortcutAction::Previous),
        ] {
            let mut direct = DetachMatcher::with_prefix("alt-u").unwrap();
            direct.feed(alt_u());
            assert!(direct.feed(literal(key)).passthrough == vec![alt_u().item, literal(key).item]);
            assert!(!direct.has_action());
            let mut workspace = DetachMatcher::with_prefix("alt-u").unwrap().for_workspace();
            workspace.feed(alt_u());
            assert!(workspace.prefix_pending());
            assert!(workspace.feed(literal(key)).passthrough.is_empty());
            assert!(!workspace.prefix_pending());
            assert_eq!(workspace.take_action(), Some(action));
        }
    }

    #[test]
    fn prefix_actions_escape_and_literal_passthrough() {
        let mut matcher =
            crate::shortcuts::matcher(&undercurrent_config::ShortcutPreferences::default())
                .unwrap();
        for key in ['u', 'b'] {
            assert!(matcher.feed(control(key)).passthrough == vec![control(key).item]);
            assert!(!matcher.prefix_pending());
        }
        assert!(matcher.feed(alt_u()).passthrough.is_empty());
        assert!(matcher.feed(literal('d')).detached);
        matcher.feed(alt_u());
        matcher.feed(literal('s'));
        assert_eq!(matcher.take_action(), Some(ShortcutAction::Sessions));
        matcher.feed(alt_u());
        matcher.feed(literal('?'));
        assert_eq!(matcher.take_action(), Some(ShortcutAction::Help));
        matcher.feed(alt_u());
        assert!(matcher.feed(alt_u()).passthrough == vec![alt_u().item]);
        matcher.feed(alt_u());
        assert!(
            matcher
                .feed(InteractiveInput {
                    item: TerminalInputDto::Key {
                        key: KeyDto::Escape
                    },
                    detach_key: None
                })
                .passthrough
                .is_empty()
        );
        assert!(matcher.feed(literal('d')).passthrough == vec![literal('d').item]);
    }

    #[test]
    fn unknown_sequences_and_paste_preserve_exact_input() {
        let mut matcher = DetachMatcher::with_prefix("alt-u").unwrap();
        matcher.feed(alt_u());
        assert!(matcher.feed(literal('w')).passthrough == vec![alt_u().item, literal('w').item]);
        let paste = TerminalInputDto::Paste {
            text: "\x1bud".to_owned(),
        };
        assert!(
            matcher
                .feed(InteractiveInput {
                    item: paste.clone(),
                    detach_key: None
                })
                .passthrough
                == vec![paste.clone()]
        );
        matcher.feed(alt_u());
        assert!(
            matcher
                .feed(InteractiveInput {
                    item: paste.clone(),
                    detach_key: None
                })
                .passthrough
                == vec![alt_u().item, paste]
        );
        assert!(!matcher.has_action());
    }

    #[test]
    fn explicit_alt_detach_sequence_remains_supported() {
        let mut matcher = DetachMatcher::parse("alt-u,d").unwrap();
        matcher.feed(alt_u());
        assert!(matcher.feed(literal('d')).detached);
    }

    #[test]
    fn default_sequence_matches_across_feeds() {
        let mut matcher = DetachMatcher::parse("ctrl-b,d").expect("valid sequence");
        assert!(matcher.feed(control('b')).passthrough.is_empty());
        let result = matcher.feed(literal('d'));
        assert!(result.detached);
        assert!(result.passthrough.is_empty());
    }

    #[test]
    fn mismatch_releases_original_items() {
        let mut matcher = DetachMatcher::parse("ctrl-b,d").expect("valid sequence");
        matcher.feed(control('b'));
        let result = matcher.feed(literal('x'));
        assert!(!result.detached);
        assert_eq!(result.passthrough.len(), 2);
    }

    #[test]
    fn overlapping_prefix_is_retained() {
        let mut matcher = DetachMatcher::parse("a,a,b").expect("valid sequence");
        matcher.feed(literal('a'));
        matcher.feed(literal('a'));
        let mismatch = matcher.feed(literal('a'));
        assert_eq!(mismatch.passthrough.len(), 1);
        assert!(matcher.feed(literal('b')).detached);
    }

    #[test]
    fn empty_sequence_disables_matching() {
        let mut matcher = DetachMatcher::parse("").expect("disabled sequence");
        let result = matcher.feed(literal('d'));
        assert!(!result.detached);
        assert_eq!(result.passthrough.len(), 1);
    }

    #[test]
    fn invalid_and_unbounded_sequences_are_rejected_without_payloads() {
        assert_eq!(
            DetachMatcher::parse("ctrl-b").err(),
            Some(DetachSequenceError::TooShort)
        );
        assert_eq!(
            DetachMatcher::parse("ctrl-b,not-a-key").err(),
            Some(DetachSequenceError::InvalidComponent { index: 1 })
        );
        let oversized = std::iter::repeat_n("a", MAXIMUM_DETACH_KEYS + 1)
            .collect::<Vec<_>>()
            .join(",");
        assert!(matches!(
            DetachMatcher::parse(&oversized),
            Err(DetachSequenceError::TooLong { .. })
        ));
    }
}
