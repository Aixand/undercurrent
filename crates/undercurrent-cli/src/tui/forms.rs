// SPDX-License-Identifier: Apache-2.0

//! Bounded editable fields and modal commands; form input never reaches a child.

use super::model::Target;
use undercurrent_protocol::{KeyDto, TerminalInputDto};

const MAX_FIELD_BYTES: usize = 8192;

#[derive(Clone)]
pub(super) struct Field {
    pub label: &'static str,
    pub text: String,
    pub cursor: usize,
}

impl Field {
    pub fn new(label: &'static str, text: impl Into<String>) -> Self {
        let text = text.into();
        Self {
            label,
            cursor: text.len(),
            text,
        }
    }

    pub fn input(&mut self, input: &TerminalInputDto) {
        match input {
            TerminalInputDto::Text { text } | TerminalInputDto::Paste { text } => {
                let text = text.chars().filter(|c| !c.is_control()).collect::<String>();
                if self.text.len().saturating_add(text.len()) <= MAX_FIELD_BYTES {
                    self.text.insert_str(self.cursor, &text);
                    self.cursor += text.len();
                }
            }
            TerminalInputDto::Key { key } => match key {
                KeyDto::Space => self.input(&TerminalInputDto::Text { text: " ".into() }),
                KeyDto::Home => self.cursor = 0,
                KeyDto::End => self.cursor = self.text.len(),
                KeyDto::Left => {
                    self.cursor = self.text[..self.cursor]
                        .char_indices()
                        .last()
                        .map_or(0, |(i, _)| i)
                }
                KeyDto::Right => {
                    self.cursor += self.text[self.cursor..]
                        .chars()
                        .next()
                        .map_or(0, char::len_utf8)
                }
                KeyDto::Backspace if self.cursor > 0 => {
                    let prior = self.text[..self.cursor]
                        .char_indices()
                        .last()
                        .map_or(0, |(i, _)| i);
                    self.text.drain(prior..self.cursor);
                    self.cursor = prior;
                }
                KeyDto::Delete if self.cursor < self.text.len() => {
                    self.text.remove(self.cursor);
                }
                KeyDto::Control { character: 'u' } => {
                    self.text.clear();
                    self.cursor = 0;
                }
                _ => {}
            },
            _ => {}
        }
    }

    pub fn display(&self, active: bool, width: usize) -> String {
        if !active {
            return crate::group::renderer::fit(&self.text, width);
        }
        let mut start = self.cursor;
        let mut columns = 1;
        for (index, character) in self.text[..self.cursor].char_indices().rev() {
            columns += unicode_width::UnicodeWidthChar::width(character).unwrap_or(0);
            if columns > width {
                break;
            }
            start = index;
        }
        crate::group::renderer::fit(
            &format!(
                "{}|{}",
                &self.text[start..self.cursor],
                &self.text[self.cursor..]
            ),
            width,
        )
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum LifecycleAction {
    Stop,
    ForceStop,
    Restart,
}

impl LifecycleAction {
    pub fn name(self) -> &'static str {
        match self {
            Self::Stop => "Stop gracefully",
            Self::ForceStop => "Force stop",
            Self::Restart => "Restart",
        }
    }
}

pub(super) enum Kind {
    Search,
    New,
    Groups,
    GroupName,
    Labels,
    Actions,
    Confirm(LifecycleAction),
    Help,
    Message,
}

pub(super) struct Dialog {
    pub kind: Kind,
    pub title: String,
    pub fields: Vec<Field>,
    pub choices: Vec<String>,
    pub active: usize,
    pub targets: Vec<Target>,
    pub error: String,
}

impl Dialog {
    pub fn form(
        kind: Kind,
        title: impl Into<String>,
        fields: Vec<Field>,
        targets: Vec<Target>,
    ) -> Self {
        Self {
            kind,
            title: title.into(),
            fields,
            choices: vec!["Apply".into()],
            active: 0,
            targets,
            error: String::new(),
        }
    }

    pub fn new_session(cwd: &str, group: &str) -> Self {
        let mut form = Self::form(
            Kind::New,
            "New session",
            vec![
                Field::new("Name", ""),
                Field::new("Type (Space)", "shell"),
                Field::new(
                    "Executable / shell",
                    crate::preparation::native_default_shell(),
                ),
                Field::new("Arguments", ""),
                Field::new("Directory", cwd),
                Field::new("Group (optional)", group),
                Field::new("Labels: key=value", ""),
                Field::new("Environment: key=value", ""),
                Field::new("Profile (optional)", ""),
                Field::new("Restart (Space)", "never"),
            ],
            vec![],
        );
        form.choices = vec!["Start".into(), "Start and attach".into()];
        form
    }

    pub fn navigate(&mut self, input: &TerminalInputDto) -> bool {
        let count = if matches!(self.kind, Kind::Help) {
            13
        } else {
            self.fields.len() + self.choices.len()
        };
        match input {
            TerminalInputDto::Key {
                key: KeyDto::Tab | KeyDto::Down,
            } => self.active = (self.active + 1) % count.max(1),
            TerminalInputDto::Key {
                key: KeyDto::BackTab | KeyDto::Up,
            } => self.active = (self.active + count.saturating_sub(1)) % count.max(1),
            TerminalInputDto::Key { key: KeyDto::Enter } => {
                if matches!(self.kind, Kind::Search | Kind::GroupName) {
                    return true;
                }
                if self.active >= self.fields.len() {
                    return true;
                }
                self.active += 1;
            }
            _ => {
                let space = matches!(input, TerminalInputDto::Key { key: KeyDto::Space })
                    || matches!(input, TerminalInputDto::Text { text } if text == " ");
                if matches!(self.kind, Kind::New) && (self.active == 1 || self.active == 9) {
                    if space {
                        let text = if self.active == 1 {
                            if self.fields[1].text == "shell" {
                                "command"
                            } else {
                                "shell"
                            }
                        } else {
                            match self.fields[9].text.as_str() {
                                "never" => "on-failure",
                                "on-failure" => "always",
                                _ => "never",
                            }
                        };
                        if self.active == 1 {
                            self.fields[2] = Field::new(
                                "Executable / shell",
                                if text == "shell" {
                                    crate::preparation::native_default_shell()
                                } else {
                                    String::new()
                                },
                            );
                        }
                        let label = self.fields[self.active].label;
                        self.fields[self.active] = Field::new(label, text);
                    }
                } else if let Some(field) = self.fields.get_mut(self.active) {
                    field.input(input);
                } else if let TerminalInputDto::Text { text } = input {
                    match text.as_str() {
                        "j" => self.active = (self.active + 1) % count.max(1),
                        "k" => self.active = (self.active + count.saturating_sub(1)) % count.max(1),
                        _ => {}
                    }
                }
            }
        }
        false
    }
}

/// Tokenize quoted arguments without expanding variables or invoking a shell.
/// Backslashes in native paths are literal except before whitespace/quotes outside quotes.
pub(super) fn words(text: &str) -> Result<Vec<String>, String> {
    let mut result = vec![];
    let mut word = String::new();
    let mut quote = None;
    let mut started = false;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if Some(c) == quote {
            quote = None;
        } else if quote.is_none() && (c == '\'' || c == '"') {
            quote = Some(c);
            started = true;
        } else if c == '\\'
            && quote != Some('\'')
            && chars.peek().is_some_and(|next| {
                Some(*next) == quote
                    || (quote.is_none() && (next.is_whitespace() || *next == '\'' || *next == '"'))
            })
        {
            word.push(chars.next().expect("peeked"));
            started = true;
        } else if c.is_whitespace() && quote.is_none() {
            if started {
                result.push(std::mem::take(&mut word));
                started = false;
            }
        } else {
            word.push(c);
            started = true;
        }
    }
    if quote.is_some() {
        return Err("Close the quote before continuing.".into());
    }
    if started {
        result.push(word);
    }
    Ok(result)
}

pub(super) fn attributes(text: &str) -> Result<std::collections::BTreeMap<String, String>, String> {
    words(text)?
        .into_iter()
        .map(|word| {
            let (key, value) = word
                .split_once('=')
                .ok_or("Use key=value for each entry.")?;
            if key.is_empty() {
                return Err("An entry needs a key.".into());
            }
            Ok((key.to_owned(), value.to_owned()))
        })
        .collect()
}
