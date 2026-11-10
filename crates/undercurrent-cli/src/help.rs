// SPDX-License-Identifier: Apache-2.0

//! Shared visual treatment for all CLI help pages.

use std::io::Write;

use clap::builder::{StyledStr, styling::Style};

/// Preserve Clap's styled command syntax and add visible section boundaries.
fn sectioned_help(help: &StyledStr) -> StyledStr {
    let plain = help.to_string();
    let ansi = help.ansi().to_string();
    let bold = Style::new().bold();
    let mut output = StyledStr::new();
    for (plain_line, styled_line) in plain.lines().zip(ansi.lines()) {
        // Help sections are unindented labels ending in a colon. Usage and
        // indented value descriptions are deliberately left alone.
        if !plain_line.starts_with(char::is_whitespace)
            && let Some(heading) = plain_line.strip_suffix(':')
            && !heading.is_empty()
        {
            let divider = "─".repeat(unicode_width::UnicodeWidthStr::width(heading));
            output.push_str(&format!("{bold}{heading}{bold:#}\n{divider}\n"));
        } else {
            output.push_str(styled_line);
            output.push_str("\n");
        }
    }
    output
}

pub(crate) fn print_help(help: &StyledStr) -> std::io::Result<()> {
    let help = sectioned_help(help);
    // Use the same automatic terminal styling backend as Clap. Redirected
    // output and NO_COLOR retain the dividers without ANSI escape sequences.
    let mut output = anstream::AutoStream::new(std::io::stdout(), anstream::ColorChoice::Auto);
    write!(output, "{}", help.ansi())?;
    output.flush()
}
