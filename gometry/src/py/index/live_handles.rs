//! Ordered live-handle ranges. Dense indexes need no directory allocation;
//! deletion splits only the range containing the removed stable handle.

use std::collections::BTreeMap;
use std::ops::Range;

use crate::HeapSize;

#[derive(Clone, Debug)]
pub(super) enum LiveHandles {
    Contiguous(Range<usize>),
    Sparse(BTreeMap<usize, usize>),
}

impl LiveHandles {
    pub(super) fn from_live(live: &[u8], live_count: usize) -> Self {
        if live_count == live.len() {
            return Self::Contiguous(0..live.len());
        }
        let mut handles = Self::Contiguous(0..0);
        for (handle, &present) in live.iter().enumerate() {
            if present != 0 {
                handles.insert(handle);
            }
        }
        handles
    }

    pub(super) fn insert(&mut self, handle: usize) {
        let end = handle.wrapping_add(1);
        match self {
            Self::Contiguous(range) if range.start == range.end => *range = handle..end,
            Self::Contiguous(range) if range.end == handle => range.end = end,
            Self::Contiguous(range) => {
                debug_assert!(range.end < handle);
                *self = Self::Sparse(BTreeMap::from([(range.start, range.end), (handle, end)]));
            },
            Self::Sparse(ranges) => {
                if let Some(mut last) = ranges.last_entry()
                    && *last.get() == handle
                {
                    *last.get_mut() = end;
                } else {
                    ranges.insert(handle, end);
                }
            },
        }
    }

    pub(super) fn remove(&mut self, handle: usize) {
        let after = handle.wrapping_add(1);
        match self {
            Self::Contiguous(range) => {
                debug_assert!(range.contains(&handle));
                if handle == range.start {
                    range.start = after;
                } else if after == range.end {
                    range.end = handle;
                } else {
                    *self =
                        Self::Sparse(BTreeMap::from([(range.start, handle), (after, range.end)]));
                }
            },
            Self::Sparse(ranges) => {
                let (&start, &end) = ranges
                    .range(..=handle)
                    .next_back()
                    .expect("removed handle belongs to a live range");
                debug_assert!(handle < end);
                if start == handle {
                    ranges.remove(&start);
                } else {
                    *ranges.get_mut(&start).expect("live range was admitted") = handle;
                }
                if after < end {
                    ranges.insert(after, end);
                }
            },
        }
    }

    pub(super) fn next_at_or_after(&self, cursor: usize) -> Option<usize> {
        match self {
            Self::Contiguous(range) => {
                let next = cursor.max(range.start);
                (next < range.end).then_some(next)
            },
            Self::Sparse(ranges) => {
                if let Some((_, &end)) = ranges.range(..=cursor).next_back()
                    && cursor < end
                {
                    return Some(cursor);
                }
                ranges.range(cursor..).next().map(|(&start, _)| start)
            },
        }
    }
}

impl Default for LiveHandles {
    fn default() -> Self {
        Self::Contiguous(0..0)
    }
}

impl HeapSize for LiveHandles {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Contiguous(_) => 0,
            // B-tree node capacity is private; estimate the entries and links.
            Self::Sparse(ranges) => ranges.len().saturating_mul(
                size_of::<(usize, usize)>().saturating_add(size_of::<[usize; 2]>()),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(handles: &LiveHandles) -> Vec<usize> {
        let mut cursor = 0;
        let mut found = Vec::new();
        while let Some(handle) = handles.next_at_or_after(cursor) {
            found.push(handle);
            cursor = handle.wrapping_add(1);
        }
        found
    }

    #[test]
    fn ranges_follow_stable_handles_across_splits_and_appends() {
        let mut handles = LiveHandles::from_live(&[1; 8], 8);
        for removed in [3, 0, 7, 4, 2] {
            handles.remove(removed);
        }
        assert_eq!(collect(&handles), [1, 5, 6]);
        handles.insert(8);
        handles.insert(9);
        assert_eq!(collect(&handles), [1, 5, 6, 8, 9]);
        for removed in [5, 6, 1, 9, 8] {
            handles.remove(removed);
        }
        assert!(collect(&handles).is_empty());
        handles.insert(10);
        assert_eq!(collect(&handles), [10]);
    }

    #[test]
    fn original_missing_rows_do_not_become_live() {
        let mut handles = LiveHandles::from_live(&[0, 1, 1, 0, 0, 1, 0], 3);
        assert_eq!(collect(&handles), [1, 2, 5]);
        handles.remove(2);
        handles.insert(7);
        assert_eq!(collect(&handles), [1, 5, 7]);
    }
}
