use crate::term::BufWrite as _;
use unicode_width::UnicodeWidthChar as _;

const TEXT_CACHE_ENTRIES: usize = 4;
const TEXT_CACHE_MAX_BYTES: usize = 256;

#[derive(Clone, Copy, Debug)]
enum TextOp {
    Ascii { start: u16, end: u16 },
    Glyph { c: char, width: u16 },
}

#[derive(Debug)]
struct CachedText {
    text: String,
    ops: Vec<TextOp>,
    errors: usize,
    columns: u16,
    renderable: bool,
    rendered_attrs: Option<crate::attrs::Attrs>,
    rendered_cells: Vec<crate::cell::Cell>,
}

#[derive(Debug, Default)]
struct TextCache {
    entries: Vec<CachedText>,
    next: usize,
}

impl Clone for TextCache {
    fn clone(&self) -> Self {
        Self::default()
    }
}

#[derive(Clone, Debug)]
pub struct Grid {
    size: Size,
    pos: Pos,
    saved_pos: Pos,
    rows: Vec<crate::row::Row>,
    scroll_top: u16,
    scroll_bottom: u16,
    origin_mode: bool,
    saved_origin_mode: bool,
    scrollback: std::collections::VecDeque<crate::row::Row>,
    scrollback_len: usize,
    scrollback_offset: usize,
    width_cache: [Option<(char, Option<u16>)>; 128],
    text_cache: TextCache,
}

impl Grid {
    pub fn new(size: Size, scrollback_len: usize) -> Self {
        Self {
            size,
            pos: Pos::default(),
            saved_pos: Pos::default(),
            rows: vec![],
            scroll_top: 0,
            scroll_bottom: size.rows - 1,
            origin_mode: false,
            saved_origin_mode: false,
            scrollback: std::collections::VecDeque::new(),
            scrollback_len,
            scrollback_offset: 0,
            width_cache: [None; 128],
            text_cache: TextCache::default(),
        }
    }

    pub fn allocate_rows(&mut self) {
        if self.rows.is_empty() {
            self.rows.extend(
                std::iter::repeat_with(|| crate::row::Row::new(self.size.cols))
                    .take(usize::from(self.size.rows)),
            );
        }
    }

    fn new_row(&self) -> crate::row::Row {
        crate::row::Row::new(self.size.cols)
    }

    pub fn clear(&mut self) {
        self.pos = Pos::default();
        self.saved_pos = Pos::default();
        for row in self.drawing_rows_mut() {
            row.clear(crate::attrs::Attrs::default());
        }
        self.scroll_top = 0;
        self.scroll_bottom = self.size.rows - 1;
        self.origin_mode = false;
        self.saved_origin_mode = false;
    }

    pub fn size(&self) -> Size {
        self.size
    }

    pub fn set_size(&mut self, size: Size) {
        if size.cols != self.size.cols {
            for row in &mut self.rows {
                row.wrap(false);
            }
        }

        if self.scroll_bottom == self.size.rows - 1 {
            self.scroll_bottom = size.rows - 1;
        }

        self.size = size;
        for row in self.rows.iter_mut().chain(self.scrollback.iter_mut()) {
            row.resize(size.cols, crate::cell::Cell::default());
        }
        self.rows.resize(usize::from(size.rows), self.new_row());

        if self.scroll_bottom >= size.rows {
            self.scroll_bottom = size.rows - 1;
        }
        if self.scroll_bottom < self.scroll_top {
            self.scroll_top = 0;
        }

        self.row_clamp_top(false);
        self.row_clamp_bottom(false);
        self.col_clamp();
    }

    pub fn pos(&self) -> Pos {
        self.pos
    }

    pub fn set_pos(&mut self, mut pos: Pos) {
        if self.origin_mode {
            pos.row = pos.row.saturating_add(self.scroll_top);
        }
        self.pos = pos;
        self.row_clamp_top(self.origin_mode);
        self.row_clamp_bottom(self.origin_mode);
        self.col_clamp();
    }

    pub fn save_cursor(&mut self) {
        self.saved_pos = self.pos;
        self.saved_origin_mode = self.origin_mode;
    }

    pub fn restore_cursor(&mut self) {
        self.pos = self.saved_pos;
        self.origin_mode = self.saved_origin_mode;
    }

    pub fn visible_rows(&self) -> impl Iterator<Item = &crate::row::Row> {
        let scrollback_len = self.scrollback.len();
        let rows_len = self.rows.len();
        self.scrollback
            .iter()
            .skip(scrollback_len - self.scrollback_offset)
            .chain(self.rows.iter())
            .take(rows_len)
    }

    pub fn drawing_rows(&self) -> impl Iterator<Item = &crate::row::Row> {
        self.rows.iter()
    }

    pub fn drawing_rows_mut(&mut self) -> impl Iterator<Item = &mut crate::row::Row> {
        self.rows.iter_mut()
    }

    pub fn visible_row(&self, row: u16) -> Option<&crate::row::Row> {
        self.visible_rows().nth(usize::from(row))
    }

    pub fn drawing_row(&self, row: u16) -> Option<&crate::row::Row> {
        self.drawing_rows().nth(usize::from(row))
    }

    pub fn drawing_row_mut(&mut self, row: u16) -> Option<&mut crate::row::Row> {
        self.drawing_rows_mut().nth(usize::from(row))
    }

    #[inline]
    pub fn current_row_mut(&mut self) -> &mut crate::row::Row {
        self.drawing_row_mut(self.pos.row)
            // we assume self.pos.row is always valid
            .unwrap()
    }

    #[inline]
    pub fn write_ascii(&mut self, mut bytes: &[u8], attrs: crate::attrs::Attrs) {
        debug_assert!(bytes.iter().all(|byte| matches!(byte, 0x20..=0x7e)));
        while !bytes.is_empty() {
            if self.pos.col >= self.size.cols {
                let last_cell = self
                    .drawing_cell(Pos {
                        row: self.pos.row,
                        col: self.size.cols - 1,
                    })
                    .expect("the cursor row and final column must be valid");
                let wrap = last_cell.has_contents() || last_cell.is_wide_continuation();
                self.col_wrap(1, wrap);
            }

            let available = usize::from(self.size.cols - self.pos.col);
            let count = available.min(bytes.len());
            let start = self.pos.col;
            self.current_row_mut()
                .write_ascii(start, &bytes[..count], attrs);
            self.pos.col += u16::try_from(count).expect("a terminal row cannot exceed u16 columns");
            bytes = &bytes[count..];
        }
    }

    #[inline]
    pub fn write_text(&mut self, text: &str, attrs: crate::attrs::Attrs) -> usize {
        if text.len() <= TEXT_CACHE_MAX_BYTES {
            let cache_index = self
                .text_cache
                .entries
                .iter()
                .position(|entry| entry.text == text)
                .unwrap_or_else(|| {
                    let entry = Self::cache_text(text);
                    if self.text_cache.entries.len() < TEXT_CACHE_ENTRIES {
                        self.text_cache.entries.push(entry);
                        self.text_cache.entries.len() - 1
                    } else {
                        let index = self.text_cache.next;
                        self.text_cache.entries[index] = entry;
                        self.text_cache.next = (index + 1) % TEXT_CACHE_ENTRIES;
                        index
                    }
                });
            let errors = self.text_cache.entries[cache_index].errors;
            if self.write_cached_text(cache_index, text, attrs) {
                return errors;
            }
            let op_count = self.text_cache.entries[cache_index].ops.len();
            for op_index in 0..op_count {
                let op = self.text_cache.entries[cache_index].ops[op_index];
                self.apply_text_op(text, op, attrs);
            }
            return errors;
        }

        let bytes = text.as_bytes();
        let mut errors = 0usize;
        let mut offset = 0usize;
        while offset < bytes.len() {
            let ascii_start = offset;
            while offset < bytes.len() && matches!(bytes[offset], 0x20..=0x7e) {
                offset += 1;
            }
            if offset > ascii_start {
                self.write_ascii(&bytes[ascii_start..offset], attrs);
                continue;
            }

            let c = text[offset..]
                .chars()
                .next()
                .expect("offset must remain on a UTF-8 character boundary");
            if c == '\u{fffd}' || ('\u{80}'..'\u{a0}').contains(&c) {
                errors = errors.saturating_add(1);
            }
            let width = self.char_width(c);
            if width.is_none() && (u32::from(c)) < 256 {
                offset += c.len_utf8();
                continue;
            }
            let width = width.unwrap_or(1);
            if width == 0 {
                self.write_combining(c);
            } else {
                self.write_char(c, width, attrs);
            }
            offset += c.len_utf8();
        }
        errors
    }

    fn cache_text(text: &str) -> CachedText {
        let bytes = text.as_bytes();
        let mut ops = Vec::new();
        let mut errors = 0usize;
        let mut columns = 0u16;
        let mut renderable = true;
        let mut offset = 0usize;
        while offset < bytes.len() {
            let ascii_start = offset;
            while offset < bytes.len() && matches!(bytes[offset], 0x20..=0x7e) {
                offset += 1;
            }
            if offset > ascii_start {
                ops.push(TextOp::Ascii {
                    start: u16::try_from(ascii_start)
                        .expect("cacheable text offset must fit in u16"),
                    end: u16::try_from(offset).expect("cacheable text offset must fit in u16"),
                });
                columns = columns.saturating_add(
                    u16::try_from(offset - ascii_start)
                        .expect("cacheable text length must fit in u16"),
                );
                continue;
            }

            let c = text[offset..]
                .chars()
                .next()
                .expect("offset must remain on a UTF-8 character boundary");
            if c == '\u{fffd}' || ('\u{80}'..'\u{a0}').contains(&c) {
                errors = errors.saturating_add(1);
            }
            let width = c
                .width()
                .map(|value| u16::try_from(value).expect("character width must fit in u16"));
            if width.is_some() || (u32::from(c)) >= 256 {
                let width = width.unwrap_or(1);
                // The scalar renderer uses widths above two for wrapping but
                // represents the glyph as one wide-cell pair. A flat cached
                // cell slice cannot reproduce that wrapping rule exactly.
                if (width == 0 && columns == 0) || width > 2 {
                    renderable = false;
                }
                ops.push(TextOp::Glyph { c, width });
                columns = columns.saturating_add(width);
            }
            offset += c.len_utf8();
        }
        CachedText {
            text: text.to_owned(),
            ops,
            errors,
            columns,
            renderable,
            rendered_attrs: None,
            rendered_cells: Vec::new(),
        }
    }

    fn write_cached_text(
        &mut self,
        cache_index: usize,
        text: &str,
        attrs: crate::attrs::Attrs,
    ) -> bool {
        let start = self.pos.col;
        let columns = self.text_cache.entries[cache_index].columns;
        if !self.text_cache.entries[cache_index].renderable
            || columns == 0
            || start > self.size.cols.saturating_sub(columns)
            || self
                .drawing_row(self.pos.row)
                .expect("the cursor row must be valid")
                .dirty_end()
                > start
        {
            return false;
        }

        if self.text_cache.entries[cache_index].rendered_attrs != Some(attrs) {
            let cells = Self::render_cached_text(
                text,
                &self.text_cache.entries[cache_index].ops,
                columns,
                attrs,
            );
            let entry = &mut self.text_cache.entries[cache_index];
            entry.rendered_attrs = Some(attrs);
            entry.rendered_cells = cells;
        }

        let row_index = usize::from(self.pos.row);
        let cells = &self.text_cache.entries[cache_index].rendered_cells;
        self.rows[row_index].write_cached(start, cells);
        self.pos.col = self.pos.col.saturating_add(columns);
        true
    }

    fn render_cached_text(
        text: &str,
        ops: &[TextOp],
        columns: u16,
        attrs: crate::attrs::Attrs,
    ) -> Vec<crate::cell::Cell> {
        let mut cells = vec![crate::cell::Cell::default(); usize::from(columns)];
        let mut col = 0usize;
        for op in ops {
            match *op {
                TextOp::Ascii { start, end } => {
                    for byte in &text.as_bytes()[usize::from(start)..usize::from(end)] {
                        cells[col].set(char::from(*byte), attrs, false);
                        col += 1;
                    }
                }
                TextOp::Glyph { c, width: 0 } => {
                    let mut target = col - 1;
                    if cells[target].is_wide_continuation() {
                        target -= 1;
                    }
                    cells[target].append(c);
                }
                TextOp::Glyph { c, width: 1 } => {
                    cells[col].set(c, attrs, false);
                    col += 1;
                }
                TextOp::Glyph { c, width: 2 } => {
                    cells[col].set(c, attrs, true);
                    cells[col + 1].set_default_wide_continuation();
                    col += 2;
                }
                TextOp::Glyph { .. } => {
                    unreachable!("Unicode width must be zero, one, or two")
                }
            }
        }
        cells
    }

    #[inline]
    fn apply_text_op(&mut self, text: &str, op: TextOp, attrs: crate::attrs::Attrs) {
        match op {
            TextOp::Ascii { start, end } => {
                self.write_ascii(
                    &text.as_bytes()[usize::from(start)..usize::from(end)],
                    attrs,
                );
            }
            TextOp::Glyph { c, width: 0 } => self.write_combining(c),
            TextOp::Glyph { c, width } => self.write_char(c, width, attrs),
        }
    }

    #[inline]
    pub fn char_width(&mut self, c: char) -> Option<u16> {
        let code = c as usize;
        let index = (code ^ (code >> 6) ^ (code >> 12)) & (self.width_cache.len() - 1);
        if let Some((cached_char, width)) = self.width_cache[index] {
            if cached_char == c {
                return width;
            }
        }

        let width = c
            .width()
            .map(|value| u16::try_from(value).expect("character width must fit in u16"));
        self.width_cache[index] = Some((c, width));
        width
    }

    #[inline]
    fn write_char(&mut self, c: char, width: u16, attrs: crate::attrs::Attrs) {
        debug_assert!(width > 0);
        let pos = self.pos;
        let mut wrap = false;
        if pos.col > self.size.cols - width {
            let last_cell = self
                .drawing_cell(Pos {
                    row: pos.row,
                    col: self.size.cols - 1,
                })
                .expect("the cursor row and final column must be valid");
            wrap = last_cell.has_contents() || last_cell.is_wide_continuation();
        }
        self.col_wrap(width, wrap);
        let col = self.pos.col;
        let occupied_columns = if width > 1 { 2 } else { 1 };
        self.current_row_mut()
            .write_char(col, c, occupied_columns, attrs);
        self.pos.col = self.pos.col.saturating_add(occupied_columns);
    }

    #[inline]
    fn write_combining(&mut self, c: char) {
        let pos = self.pos;
        if pos.col > 0 {
            self.current_row_mut().append_combining(pos.col - 1, c);
        } else if pos.row > 0
            && self
                .drawing_row(pos.row - 1)
                .expect("the previous cursor row must be valid")
                .wrapped()
        {
            let col = self.size.cols - 1;
            self.drawing_row_mut(pos.row - 1)
                .expect("the previous cursor row must be valid")
                .append_combining(col, c);
        }
    }

    pub fn visible_cell(&self, pos: Pos) -> Option<&crate::cell::Cell> {
        self.visible_row(pos.row).and_then(|r| r.get(pos.col))
    }

    pub fn drawing_cell(&self, pos: Pos) -> Option<&crate::cell::Cell> {
        self.drawing_row(pos.row).and_then(|r| r.get(pos.col))
    }

    pub fn drawing_cell_mut(&mut self, pos: Pos) -> Option<&mut crate::cell::Cell> {
        self.drawing_row_mut(pos.row)
            .and_then(|r| r.get_mut(pos.col))
    }

    pub fn scrollback_len(&self) -> usize {
        self.scrollback_len
    }

    pub fn scrollback(&self) -> usize {
        self.scrollback_offset
    }

    pub fn set_scrollback(&mut self, rows: usize) {
        self.scrollback_offset = rows.min(self.scrollback.len());
    }

    pub fn write_contents(&self, contents: &mut String) {
        let mut wrapping = false;
        for row in self.visible_rows() {
            row.write_contents(contents, 0, self.size.cols, wrapping);
            if !row.wrapped() {
                contents.push('\n');
            }
            wrapping = row.wrapped();
        }

        while contents.ends_with('\n') {
            contents.truncate(contents.len() - 1);
        }
    }

    pub fn write_contents_formatted(&self, contents: &mut Vec<u8>) -> crate::attrs::Attrs {
        crate::term::ClearAttrs::default().write_buf(contents);
        crate::term::ClearScreen::default().write_buf(contents);

        let mut prev_attrs = crate::attrs::Attrs::default();
        let mut prev_pos = Pos::default();
        let mut wrapping = false;
        for (i, row) in self.visible_rows().enumerate() {
            // we limit the number of cols to a u16 (see Size), so
            // visible_rows() can never return more rows than will fit
            let i = i.try_into().unwrap();
            let (new_pos, new_attrs) = row.write_contents_formatted(
                contents,
                0,
                self.size.cols,
                i,
                wrapping,
                Some(prev_pos),
                Some(prev_attrs),
            );
            prev_pos = new_pos;
            prev_attrs = new_attrs;
            wrapping = row.wrapped();
        }

        self.write_cursor_position_formatted(contents, Some(prev_pos), Some(prev_attrs));

        prev_attrs
    }

    pub fn write_contents_diff(
        &self,
        contents: &mut Vec<u8>,
        prev: &Self,
        mut prev_attrs: crate::attrs::Attrs,
    ) -> crate::attrs::Attrs {
        let mut prev_pos = prev.pos;
        let mut wrapping = false;
        let mut prev_wrapping = false;
        for (i, (row, prev_row)) in self.visible_rows().zip(prev.visible_rows()).enumerate() {
            // we limit the number of cols to a u16 (see Size), so
            // visible_rows() can never return more rows than will fit
            let i = i.try_into().unwrap();
            let (new_pos, new_attrs) = row.write_contents_diff(
                contents,
                prev_row,
                0,
                self.size.cols,
                i,
                wrapping,
                prev_wrapping,
                prev_pos,
                prev_attrs,
            );
            prev_pos = new_pos;
            prev_attrs = new_attrs;
            wrapping = row.wrapped();
            prev_wrapping = prev_row.wrapped();
        }

        self.write_cursor_position_formatted(contents, Some(prev_pos), Some(prev_attrs));

        prev_attrs
    }

    pub fn write_cursor_position_formatted(
        &self,
        contents: &mut Vec<u8>,
        prev_pos: Option<Pos>,
        prev_attrs: Option<crate::attrs::Attrs>,
    ) {
        let prev_attrs = prev_attrs.unwrap_or_default();
        // writing a character to the last column of a row doesn't wrap the
        // cursor immediately - it waits until the next character is actually
        // drawn. it is only possible for the cursor to have this kind of
        // position after drawing a character though, so if we end in this
        // position, we need to redraw the character at the end of the row.
        if prev_pos != Some(self.pos) && self.pos.col >= self.size.cols {
            let mut pos = Pos {
                row: self.pos.row,
                col: self.size.cols - 1,
            };
            if self
                .drawing_cell(pos)
                // we assume self.pos.row is always valid, and self.size.cols
                // - 1 is always a valid column
                .unwrap()
                .is_wide_continuation()
            {
                pos.col = self.size.cols - 2;
            }
            let cell =
                // we assume self.pos.row is always valid, and self.size.cols
                // - 2 must be a valid column because self.size.cols - 1 is
                // always valid and we just checked that the cell at
                // self.size.cols - 1 is a wide continuation character, which
                // means that the first half of the wide character must be
                // before it
                self.drawing_cell(pos).unwrap();
            if cell.has_contents() {
                if let Some(prev_pos) = prev_pos {
                    crate::term::MoveFromTo::new(prev_pos, pos).write_buf(contents);
                } else {
                    crate::term::MoveTo::new(pos).write_buf(contents);
                }
                cell.attrs().write_escape_code_diff(contents, &prev_attrs);
                contents.extend(cell.contents().as_bytes());
                prev_attrs.write_escape_code_diff(contents, cell.attrs());
            } else {
                // if the cell doesn't have contents, we can't have gotten
                // here by drawing a character in the last column. this means
                // that as far as i'm aware, we have to have reached here from
                // a newline when we were already after the end of an earlier
                // row. in the case where we are already after the end of an
                // earlier row, we can just write a few newlines, otherwise we
                // also need to do the same as above to get ourselves to after
                // the end of a row.
                let mut found = false;
                for i in (0..self.pos.row).rev() {
                    pos.row = i;
                    pos.col = self.size.cols - 1;
                    if self
                        .drawing_cell(pos)
                        // i is always less than self.pos.row, which we assume
                        // to be always valid, so it must also be valid.
                        // self.size.cols - 1 is always a valid col.
                        .unwrap()
                        .is_wide_continuation()
                    {
                        pos.col = self.size.cols - 2;
                    }
                    let cell = self
                        .drawing_cell(pos)
                        // i is always less than self.pos.row, which we assume
                        // to be always valid, so it must also be valid.
                        // self.size.cols - 2 is valid because self.size.cols
                        // - 1 is always valid, and col gets set to
                        // self.size.cols - 2 when the cell at self.size.cols
                        // - 1 is a wide continuation character, meaning that
                        // the first half of the wide character must be before
                        // it
                        .unwrap();
                    if cell.has_contents() {
                        if let Some(prev_pos) = prev_pos {
                            if prev_pos.row != i || prev_pos.col < self.size.cols {
                                crate::term::MoveFromTo::new(prev_pos, pos).write_buf(contents);
                                cell.attrs().write_escape_code_diff(contents, &prev_attrs);
                                contents.extend(cell.contents().as_bytes());
                                prev_attrs.write_escape_code_diff(contents, cell.attrs());
                            }
                        } else {
                            crate::term::MoveTo::new(pos).write_buf(contents);
                            cell.attrs().write_escape_code_diff(contents, &prev_attrs);
                            contents.extend(cell.contents().as_bytes());
                            prev_attrs.write_escape_code_diff(contents, cell.attrs());
                        }
                        contents.extend("\n".repeat(usize::from(self.pos.row - i)).as_bytes());
                        found = true;
                        break;
                    }
                }

                // this can happen if you get the cursor off the end of a row,
                // and then do something to clear the end of the current row
                // without moving the cursor (IL, DL, ED, EL, etc). we know
                // there can't be something in the last column because we
                // would have caught that above, so it should be safe to
                // overwrite it.
                if !found {
                    pos = Pos {
                        row: self.pos.row,
                        col: self.size.cols - 1,
                    };
                    if let Some(prev_pos) = prev_pos {
                        crate::term::MoveFromTo::new(prev_pos, pos).write_buf(contents);
                    } else {
                        crate::term::MoveTo::new(pos).write_buf(contents);
                    }
                    contents.push(b' ');
                    // we know that the cell has no contents, but it still may
                    // have drawing attributes (background color, etc)
                    let end_cell = self
                        .drawing_cell(pos)
                        // we assume self.pos.row is always valid, and
                        // self.size.cols - 1 is always a valid column
                        .unwrap();
                    end_cell
                        .attrs()
                        .write_escape_code_diff(contents, &prev_attrs);
                    crate::term::SaveCursor::default().write_buf(contents);
                    crate::term::Backspace::default().write_buf(contents);
                    crate::term::EraseChar::new(1).write_buf(contents);
                    crate::term::RestoreCursor::default().write_buf(contents);
                    prev_attrs.write_escape_code_diff(contents, end_cell.attrs());
                }
            }
        } else if let Some(prev_pos) = prev_pos {
            crate::term::MoveFromTo::new(prev_pos, self.pos).write_buf(contents);
        } else {
            crate::term::MoveTo::new(self.pos).write_buf(contents);
        }
    }

    pub fn erase_all(&mut self, attrs: crate::attrs::Attrs) {
        for row in self.drawing_rows_mut() {
            row.clear(attrs);
        }
    }

    pub fn erase_all_forward(&mut self, attrs: crate::attrs::Attrs) {
        let pos = self.pos;
        for row in self.drawing_rows_mut().skip(usize::from(pos.row) + 1) {
            row.clear(attrs);
        }

        self.erase_row_forward(attrs);
    }

    pub fn erase_all_backward(&mut self, attrs: crate::attrs::Attrs) {
        let pos = self.pos;
        for row in self.drawing_rows_mut().take(usize::from(pos.row)) {
            row.clear(attrs);
        }

        self.erase_row_backward(attrs);
    }

    pub fn erase_row(&mut self, attrs: crate::attrs::Attrs) {
        self.current_row_mut().clear(attrs);
    }

    pub fn erase_row_forward(&mut self, attrs: crate::attrs::Attrs) {
        let size = self.size;
        let pos = self.pos;
        let row = self.current_row_mut();
        for col in pos.col..size.cols {
            row.erase(col, attrs);
        }
    }

    pub fn erase_row_backward(&mut self, attrs: crate::attrs::Attrs) {
        let size = self.size;
        let pos = self.pos;
        let row = self.current_row_mut();
        for col in 0..=pos.col.min(size.cols - 1) {
            row.erase(col, attrs);
        }
    }

    pub fn insert_cells(&mut self, count: u16) {
        let size = self.size;
        let pos = self.pos;
        let wide = pos.col < size.cols
            && self
                .drawing_cell(pos)
                // we assume self.pos.row is always valid, and we know we are
                // not off the end of a row because we just checked pos.col <
                // size.cols
                .unwrap()
                .is_wide_continuation();
        let row = self.current_row_mut();
        for _ in 0..count {
            if wide {
                row.get_mut(pos.col).unwrap().set_wide_continuation(false);
            }
            row.insert(pos.col, crate::cell::Cell::default());
            if wide {
                row.get_mut(pos.col).unwrap().set_wide_continuation(true);
            }
        }
        row.truncate(size.cols);
    }

    pub fn delete_cells(&mut self, count: u16) {
        let size = self.size;
        let pos = self.pos;
        let row = self.current_row_mut();
        for _ in 0..(count.min(size.cols - pos.col)) {
            row.remove(pos.col);
        }
        row.resize(size.cols, crate::cell::Cell::default());
    }

    pub fn erase_cells(&mut self, count: u16, attrs: crate::attrs::Attrs) {
        let size = self.size;
        let pos = self.pos;
        let row = self.current_row_mut();
        for col in pos.col..((pos.col.saturating_add(count)).min(size.cols)) {
            row.erase(col, attrs);
        }
    }

    pub fn insert_lines(&mut self, count: u16) {
        for _ in 0..count {
            self.rows.remove(usize::from(self.scroll_bottom));
            self.rows.insert(usize::from(self.pos.row), self.new_row());
            // self.scroll_bottom is maintained to always be a valid row
            self.rows[usize::from(self.scroll_bottom)].wrap(false);
        }
    }

    pub fn delete_lines(&mut self, count: u16) {
        for _ in 0..(count.min(self.size.rows - self.pos.row)) {
            self.rows
                .insert(usize::from(self.scroll_bottom) + 1, self.new_row());
            self.rows.remove(usize::from(self.pos.row));
        }
    }

    pub fn scroll_up(&mut self, count: u16) {
        for _ in 0..(count.min(self.size.rows - self.scroll_top)) {
            let mut removed = self.rows.remove(usize::from(self.scroll_top));
            // A top-anchored region can leave a fixed composer/status area below
            // it. Rows leaving the top still belong to primary-screen history.
            // Interior regions and the zero-history alternate grid stay excluded.
            if self.scrollback_len > 0 && self.scroll_top == 0 {
                self.rows
                    .insert(usize::from(self.scroll_bottom), self.new_row());
                self.scrollback.push_back(removed);
                while self.scrollback.len() > self.scrollback_len {
                    self.scrollback.pop_front();
                }
                if self.scrollback_offset > 0 {
                    self.scrollback_offset = self.scrollback.len().min(self.scrollback_offset + 1);
                }
            } else {
                removed.reset();
                self.rows.insert(usize::from(self.scroll_bottom), removed);
            }
        }
    }

    pub fn scroll_down(&mut self, count: u16) {
        for _ in 0..count {
            self.rows.remove(usize::from(self.scroll_bottom));
            self.rows
                .insert(usize::from(self.scroll_top), self.new_row());
            // self.scroll_bottom is maintained to always be a valid row
            self.rows[usize::from(self.scroll_bottom)].wrap(false);
        }
    }

    pub fn set_scroll_region(&mut self, top: u16, bottom: u16) {
        let bottom = bottom.min(self.size().rows - 1);
        if top < bottom {
            self.scroll_top = top;
            self.scroll_bottom = bottom;
        } else {
            self.scroll_top = 0;
            self.scroll_bottom = self.size().rows - 1;
        }
        self.pos.row = self.scroll_top;
        self.pos.col = 0;
    }

    fn in_scroll_region(&self) -> bool {
        self.pos.row >= self.scroll_top && self.pos.row <= self.scroll_bottom
    }

    pub fn set_origin_mode(&mut self, mode: bool) {
        self.origin_mode = mode;
        self.set_pos(Pos { row: 0, col: 0 });
    }

    pub fn row_inc_clamp(&mut self, count: u16) {
        let in_scroll_region = self.in_scroll_region();
        self.pos.row = self.pos.row.saturating_add(count);
        self.row_clamp_bottom(in_scroll_region);
    }

    pub fn row_inc_scroll(&mut self, count: u16) -> u16 {
        let in_scroll_region = self.in_scroll_region();
        self.pos.row = self.pos.row.saturating_add(count);
        let lines = self.row_clamp_bottom(in_scroll_region);
        if in_scroll_region {
            self.scroll_up(lines);
            lines
        } else {
            0
        }
    }

    pub fn row_dec_clamp(&mut self, count: u16) {
        let in_scroll_region = self.in_scroll_region();
        self.pos.row = self.pos.row.saturating_sub(count);
        self.row_clamp_top(in_scroll_region);
    }

    pub fn row_dec_scroll(&mut self, count: u16) {
        let in_scroll_region = self.in_scroll_region();
        // need to account for clamping by both row_clamp_top and by
        // saturating_sub
        let extra_lines = if count > self.pos.row {
            count - self.pos.row
        } else {
            0
        };
        self.pos.row = self.pos.row.saturating_sub(count);
        let lines = self.row_clamp_top(in_scroll_region);
        self.scroll_down(lines + extra_lines);
    }

    pub fn row_set(&mut self, i: u16) {
        self.pos.row = i;
        self.row_clamp();
    }

    #[inline]
    pub fn col_inc(&mut self, count: u16) {
        self.pos.col = self.pos.col.saturating_add(count);
    }

    pub fn col_inc_clamp(&mut self, count: u16) {
        self.pos.col = self.pos.col.saturating_add(count);
        self.col_clamp();
    }

    pub fn col_dec(&mut self, count: u16) {
        self.pos.col = self.pos.col.saturating_sub(count);
    }

    pub fn col_tab(&mut self) {
        self.pos.col -= self.pos.col % 8;
        self.pos.col += 8;
        self.col_clamp();
    }

    pub fn col_set(&mut self, i: u16) {
        self.pos.col = i;
        self.col_clamp();
    }

    #[inline]
    pub fn col_wrap(&mut self, width: u16, wrap: bool) {
        if self.pos.col > self.size.cols - width {
            let mut prev_pos = self.pos;
            self.pos.col = 0;
            let scrolled = self.row_inc_scroll(1);
            prev_pos.row -= scrolled;
            let new_pos = self.pos;
            self.drawing_row_mut(prev_pos.row)
                // we assume self.pos.row is always valid, and so prev_pos.row
                // must be valid because it is always less than or equal to
                // self.pos.row
                .unwrap()
                .wrap(wrap && prev_pos.row + 1 == new_pos.row);
        }
    }

    fn row_clamp_top(&mut self, limit_to_scroll_region: bool) -> u16 {
        if limit_to_scroll_region && self.pos.row < self.scroll_top {
            let rows = self.scroll_top - self.pos.row;
            self.pos.row = self.scroll_top;
            rows
        } else {
            0
        }
    }

    fn row_clamp_bottom(&mut self, limit_to_scroll_region: bool) -> u16 {
        let bottom = if limit_to_scroll_region {
            self.scroll_bottom
        } else {
            self.size.rows - 1
        };
        if self.pos.row > bottom {
            let rows = self.pos.row - bottom;
            self.pos.row = bottom;
            rows
        } else {
            0
        }
    }

    fn row_clamp(&mut self) {
        if self.pos.row > self.size.rows - 1 {
            self.pos.row = self.size.rows - 1;
        }
    }

    fn col_clamp(&mut self) {
        if self.pos.col > self.size.cols - 1 {
            self.pos.col = self.size.cols - 1;
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct Size {
    pub rows: u16,
    pub cols: u16,
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct Pos {
    pub row: u16,
    pub col: u16,
}
