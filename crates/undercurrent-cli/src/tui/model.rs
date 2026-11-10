// SPDX-License-Identifier: Apache-2.0

//! Workspace selection is keyed by session identity, never by a changing row.

use std::collections::BTreeSet;

use undercurrent_protocol::{InteractionStateDto, LifecycleStateDto, SessionSummaryDto};

use crate::GroupRecord;

use super::forms::Dialog;

#[derive(Clone, PartialEq, Eq)]
pub(super) enum Filter {
    All,
    Running,
    NeedsInput,
    Ungrouped,
    Group(String),
}

impl Filter {
    pub fn name(&self) -> &str {
        match self {
            Self::All => "All sessions",
            Self::Running => "Running",
            Self::NeedsInput => "Needs input",
            Self::Ungrouped => "Ungrouped",
            Self::Group(name) => name,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Tab {
    Details,
    Screen,
    Logs,
}

#[derive(Clone)]
pub(super) struct Target {
    pub id: String,
    pub name: String,
    pub group: Option<String>,
    pub revision: u64,
}

pub(super) struct Preview {
    pub id: String,
    pub tab: Tab,
    pub lines: Vec<String>,
}

pub(super) struct App {
    pub sessions: Vec<SessionSummaryDto>,
    pub groups: Vec<GroupRecord>,
    pub selected: Option<String>,
    pub marked: BTreeSet<String>,
    pub filter: Filter,
    pub sidebar: bool,
    pub search: String,
    pub tab: Tab,
    pub preview: Option<Preview>,
    pub preview_scroll: usize,
    pub dialog: Option<Dialog>,
    pub notice: String,
    pub fresh: bool,
    pub pending: usize,
    pub operations: usize,
}

impl Default for App {
    fn default() -> Self {
        Self {
            sessions: Vec::new(),
            groups: Vec::new(),
            selected: None,
            marked: BTreeSet::new(),
            filter: Filter::All,
            sidebar: false,
            search: String::new(),
            tab: Tab::Details,
            preview: None,
            preview_scroll: 0,
            dialog: None,
            notice: "Loading sessions...".into(),
            fresh: false,
            pending: 0,
            operations: 0,
        }
    }
}

impl App {
    pub fn owner(&self, id: &str) -> Option<&str> {
        self.groups
            .iter()
            .find(|g| g.members.iter().any(|m| m.session_id == id))
            .map(|g| g.name.as_str())
    }

    pub fn filters(&self) -> Vec<Filter> {
        [
            Filter::All,
            Filter::Running,
            Filter::NeedsInput,
            Filter::Ungrouped,
        ]
        .into_iter()
        .chain(self.groups.iter().map(|g| Filter::Group(g.name.clone())))
        .collect()
    }

    pub fn matches(&self, session: &SessionSummaryDto, filter: &Filter) -> bool {
        match filter {
            Filter::All => true,
            Filter::Running => matches!(
                session.lifecycle,
                LifecycleStateDto::Running | LifecycleStateDto::Starting
            ),
            Filter::NeedsInput => {
                session.foreground.interaction.state == InteractionStateDto::Requested
            }
            Filter::Ungrouped => self.owner(&session.id).is_none(),
            Filter::Group(name) => self.owner(&session.id) == Some(name),
        }
    }

    pub fn visible(&self) -> Vec<&SessionSummaryDto> {
        let words = self
            .search
            .split_whitespace()
            .map(str::to_lowercase)
            .collect::<Vec<_>>();
        self.sessions
            .iter()
            .filter(|s| {
                self.matches(s, &self.filter)
                    && words.iter().all(|word| {
                        if let Some((key, value)) = word.split_once('=') {
                            s.labels.as_ref().is_some_and(|labels| {
                                labels.iter().any(|(k, v)| {
                                    k.to_lowercase() == key && v.to_lowercase() == value
                                })
                            })
                        } else {
                            s.name.to_lowercase().contains(word)
                                || s.profile
                                    .as_ref()
                                    .is_some_and(|p| p.to_lowercase().contains(word))
                                || s.id.contains(word)
                        }
                    })
            })
            .collect()
    }

    pub fn current(&self) -> Option<&SessionSummaryDto> {
        self.sessions
            .iter()
            .find(|s| Some(&s.id) == self.selected.as_ref())
    }

    pub fn normalize_selection(&mut self) {
        let visible = self.visible();
        if !visible
            .iter()
            .any(|s| Some(&s.id) == self.selected.as_ref())
        {
            self.selected = visible.first().map(|s| s.id.clone());
            self.preview_scroll = 0;
        }
    }

    pub fn move_selection(&mut self, step: isize) {
        let visible = self.visible();
        let index = visible
            .iter()
            .position(|s| Some(&s.id) == self.selected.as_ref())
            .unwrap_or(0);
        let next = index
            .saturating_add_signed(step)
            .min(visible.len().saturating_sub(1));
        self.selected = visible.get(next).map(|s| s.id.clone());
        self.preview_scroll = 0;
    }

    pub fn move_filter(&mut self, step: isize) {
        let filters = self.filters();
        let index = filters.iter().position(|f| f == &self.filter).unwrap_or(0);
        self.filter = filters[index.saturating_add_signed(step).min(filters.len() - 1)].clone();
        self.normalize_selection();
    }

    pub fn apply_catalog(&mut self, sessions: Vec<SessionSummaryDto>, groups: Vec<GroupRecord>) {
        self.sessions = sessions;
        self.groups = groups;
        self.marked
            .retain(|id| self.sessions.iter().any(|s| &s.id == id));
        if !self.filters().contains(&self.filter) {
            self.filter = Filter::All;
        }
        self.normalize_selection();
        if !self.fresh {
            self.notice = "Enter attaches. Detach returns here; processes keep running.".into();
        }
        self.fresh = true;
    }

    /// Freeze the full explicit selection, including marks outside the current filter.
    pub fn targets(&self) -> Vec<Target> {
        self.sessions
            .iter()
            .filter(|s| {
                if self.marked.is_empty() {
                    Some(&s.id) == self.selected.as_ref()
                } else {
                    self.marked.contains(&s.id)
                }
            })
            .map(|s| Target {
                id: s.id.clone(),
                name: s.name.clone(),
                group: self.owner(&s.id).map(str::to_owned),
                revision: s.revision,
            })
            .collect()
    }
}
