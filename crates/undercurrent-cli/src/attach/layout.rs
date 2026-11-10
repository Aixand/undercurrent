// SPDX-License-Identifier: Apache-2.0

use undercurrent_protocol::TerminalSizeDto;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Axis {
    Columns,
    Rows,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Rect {
    pub x: u16,
    pub y: u16,
    pub size: TerminalSizeDto,
}

impl Rect {
    pub fn contains(self, column: u16, row: u16) -> bool {
        column > self.x
            && column <= self.x + self.size.columns
            && row > self.y
            && row <= self.y + self.size.rows
    }
}

#[derive(Clone)]
pub(super) enum Layout {
    Pane(usize),
    Split(Axis, Box<Self>, Box<Self>),
}

impl Layout {
    pub fn split(&mut self, target: usize, next: usize, axis: Axis) {
        match self {
            Self::Pane(id) if *id == target => {
                *self = Self::Split(
                    axis,
                    Box::new(Self::Pane(target)),
                    Box::new(Self::Pane(next)),
                );
            }
            Self::Split(_, first, second) => {
                first.split(target, next, axis);
                second.split(target, next, axis);
            }
            _ => {}
        }
    }

    pub fn remove(self, target: usize) -> Option<Self> {
        match self {
            Self::Pane(id) if id == target => None,
            Self::Pane(id) => Some(Self::Pane(if id > target { id - 1 } else { id })),
            Self::Split(axis, first, second) => match (first.remove(target), second.remove(target))
            {
                (Some(first), Some(second)) => {
                    Some(Self::Split(axis, Box::new(first), Box::new(second)))
                }
                (first, second) => first.or(second),
            },
        }
    }

    pub fn rectangles(&self, rect: Rect, result: &mut Vec<(usize, Rect)>) -> bool {
        match self {
            Self::Pane(id) => {
                if rect.size.columns < 4 || rect.size.rows < 2 {
                    return false;
                }
                result.push((*id, rect));
                true
            }
            Self::Split(axis, first, second) => {
                let mut a = rect;
                let mut b = rect;
                match axis {
                    Axis::Columns => {
                        a.size.columns = rect.size.columns.saturating_sub(1) / 2;
                        b.x += a.size.columns + 1;
                        b.size.columns = rect.size.columns.saturating_sub(a.size.columns + 1);
                    }
                    Axis::Rows => {
                        a.size.rows = rect.size.rows / 2;
                        b.y += a.size.rows;
                        b.size.rows -= a.size.rows;
                    }
                }
                first.rectangles(a, result) && second.rectangles(b, result)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_tiles_are_disjoint_and_collapse_after_removal() {
        let rect = Rect {
            x: 0,
            y: 0,
            size: TerminalSizeDto {
                columns: 81,
                rows: 24,
            },
        };
        let mut layout = Layout::Pane(0);
        layout.split(0, 1, Axis::Columns);
        layout.split(1, 2, Axis::Rows);
        let mut rectangles = vec![];
        assert!(layout.rectangles(rect, &mut rectangles));
        assert_eq!(rectangles.len(), 3);
        for column in 1..=81 {
            for row in 1..=24 {
                assert!(
                    rectangles
                        .iter()
                        .filter(|(_, r)| r.contains(column, row))
                        .count()
                        <= 1
                );
            }
        }
        let layout = layout.remove(0).unwrap();
        rectangles.clear();
        assert!(layout.rectangles(rect, &mut rectangles));
        assert_eq!(
            rectangles[0].1.size,
            TerminalSizeDto {
                columns: 81,
                rows: 12
            }
        );
        assert_eq!(rectangles[1].0, 1);
    }
}
