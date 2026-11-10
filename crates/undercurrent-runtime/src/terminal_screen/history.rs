// SPDX-License-Identifier: Apache-2.0

//! Seed normal-screen scrollback within the existing ANSI snapshot byte budget.

pub(super) fn with_history(parser: &mut vt100::Parser, live: Vec<u8>, limit: usize) -> Vec<u8> {
    if parser.screen().alternate_screen() {
        return live;
    }
    let original = parser.screen().scrollback();
    parser.set_scrollback(usize::MAX);
    let history = parser.screen().scrollback();
    let rows = parser.screen().size().0;
    let mut pages = Vec::new();
    let mut used = live.len();
    let mut included = 0;
    while included < history {
        // A wrapped row needs the following visible row to encode its wrap.
        // Keep that continuation on screen instead of exporting the last row.
        let count = usize::from(rows.saturating_sub(1).max(1)).min(history - included);
        parser.set_scrollback(included + count);
        let mut page = parser.screen().contents_formatted();
        // Scrolling a rendered page puts its leading rows in the receiver's
        // history, including cell attributes and soft wraps. The final live
        // render restores the viewport and cursor without forwarding raw OSCs.
        page.extend(format!("\x1b[{rows};1H").bytes());
        for _ in 0..count {
            page.extend_from_slice(b"\r\n");
        }
        if page.len() > limit.saturating_sub(used) {
            break;
        }
        used += page.len();
        pages.push(page);
        included += count;
    }
    parser.set_scrollback(original);
    let mut contents = Vec::with_capacity(used);
    // Admit newest pages first so pressure drops only older history. Visible
    // cells and input modes always fit in full or the caller reports capacity.
    for page in pages.into_iter().rev() {
        contents.extend(page);
    }
    contents.extend(live);
    contents
}
