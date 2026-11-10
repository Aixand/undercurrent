// SPDX-License-Identifier: Apache-2.0
//! Construct local shortcut matching from an already-resolved preference snapshot.
use crate::{CliError, CliInputError, DetachMatcher};

pub(crate) fn matcher(
    preferences: &undercurrent_config::ShortcutPreferences,
) -> Result<DetachMatcher, CliError> {
    DetachMatcher::with_prefix(&preferences.prefix)
        .map_err(|source| CliInputError::InvalidDetachKeys { source }.into())
}
