//! Ordered live-handle ranges. Dense indexes need no directory allocation;
//! deletion splits only the range containing the removed stable handle.

use std::collections::BTreeMap;
use std::ops::Range;

use crate::HeapSize;

#[derive(Clone, Debug)]
pub(super) struct LiveHandles {
    ranges: LiveRanges,
    len: usize,
}

#[derive(Clone, Debug)]
enum LiveRanges {
    Contiguous(Range<usize>),
    Sparse(BTreeMap<usize, usize>),
}

enum LiveRangeIter<'a> {
    Contiguous(Option<&'a Range<usize>>),
    Sparse(std::collections::btree_map::Iter<'a, usize, usize>),
}

impl Iterator for LiveRangeIter<'_> {
    type Item = Range<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Contiguous(range) => range.take().cloned(),
            Self::Sparse(ranges) => ranges.next().map(|(&start, &end)| start..end),
        }
    }
}

pub(super) struct Iter<'a> {
    ranges: LiveRangeIter<'a>,
    current: Option<Range<usize>>,
}

impl Iterator for Iter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current) = &mut self.current
                && let Some(handle) = current.next()
            {
                return Some(handle);
            }
            self.current = self.ranges.next();
            self.current.as_ref()?;
        }
    }
}

impl LiveHandles {
    #[cfg(test)]
    pub(super) fn from_sorted(handles: impl IntoIterator<Item = usize>) -> Self {
        let mut live = Self::default();
        let mut previous = None;
        for handle in handles {
            debug_assert!(previous.is_none_or(|previous| previous < handle));
            live.insert(handle);
            previous = Some(handle);
        }
        live
    }

    pub(super) const fn len(&self) -> usize {
        self.len
    }

    pub(super) const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(super) fn contains(&self, handle: usize) -> bool {
        match &self.ranges {
            LiveRanges::Contiguous(range) => range.contains(&handle),
            LiveRanges::Sparse(ranges) => ranges
                .range(..=handle)
                .next_back()
                .is_some_and(|(_, &end)| handle < end),
        }
    }

    pub(super) fn iter(&self) -> Iter<'_> {
        let ranges = match &self.ranges {
            LiveRanges::Contiguous(range) => LiveRangeIter::Contiguous(Some(range)),
            LiveRanges::Sparse(ranges) => LiveRangeIter::Sparse(ranges.iter()),
        };
        Iter {
            ranges,
            current: None,
        }
    }

    pub(super) fn append_ordered(&self, out: &mut Vec<usize>) {
        if self.is_empty() {
            return;
        }
        for range in self.range_iter() {
            out.extend(range);
        }
    }

    fn range_iter(&self) -> LiveRangeIter<'_> {
        match &self.ranges {
            LiveRanges::Contiguous(range) => LiveRangeIter::Contiguous(Some(range)),
            LiveRanges::Sparse(ranges) => LiveRangeIter::Sparse(ranges.iter()),
        }
    }

    pub(super) fn insert(&mut self, handle: usize) {
        assert!(!self.contains(handle), "live handle inserted twice");
        let end = handle.wrapping_add(1);
        match &mut self.ranges {
            LiveRanges::Contiguous(range) if range.start == range.end => *range = handle..end,
            LiveRanges::Contiguous(range) if range.end == handle => range.end = end,
            LiveRanges::Contiguous(range) => {
                debug_assert!(range.end < handle);
                self.ranges =
                    LiveRanges::Sparse(BTreeMap::from([(range.start, range.end), (handle, end)]));
            },
            LiveRanges::Sparse(ranges) => {
                if let Some(mut last) = ranges.last_entry()
                    && *last.get() == handle
                {
                    *last.get_mut() = end;
                } else {
                    ranges.insert(handle, end);
                }
            },
        }
        self.len = self.len.wrapping_add(1);
    }

    pub(super) fn remove(&mut self, handle: usize) -> bool {
        if !self.contains(handle) {
            return false;
        }
        let after = handle.wrapping_add(1);
        match &mut self.ranges {
            LiveRanges::Contiguous(range) => {
                if handle == range.start {
                    range.start = after;
                } else if after == range.end {
                    range.end = handle;
                } else {
                    self.ranges = LiveRanges::Sparse(BTreeMap::from([
                        (range.start, handle),
                        (after, range.end),
                    ]));
                }
            },
            LiveRanges::Sparse(ranges) => {
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
        self.len = self.len.wrapping_sub(1);
        if let LiveRanges::Sparse(ranges) = &self.ranges
            && ranges.len() <= 1
        {
            let range = ranges
                .first_key_value()
                .map_or(0..0, |(&start, &end)| start..end);
            self.ranges = LiveRanges::Contiguous(range);
        }
        true
    }

    pub(super) fn next_at_or_after(&self, cursor: usize) -> Option<usize> {
        match &self.ranges {
            LiveRanges::Contiguous(range) => {
                let next = cursor.max(range.start);
                (next < range.end).then_some(next)
            },
            LiveRanges::Sparse(ranges) => {
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
        Self {
            ranges: LiveRanges::Contiguous(0..0),
            len: 0,
        }
    }
}

impl HeapSize for LiveHandles {
    fn heap_bytes(&self) -> usize {
        match &self.ranges {
            LiveRanges::Contiguous(_) => 0,
            // B-tree node capacity is private; estimate the entries and links.
            LiveRanges::Sparse(ranges) => ranges.len().saturating_mul(
                size_of::<(usize, usize)>().saturating_add(size_of::<[usize; 2]>()),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(handles: &LiveHandles) -> Vec<usize> {
        handles.iter().collect()
    }

    #[test]
    fn ranges_follow_stable_handles_across_splits_and_appends() {
        let mut handles = LiveHandles::from_sorted(0..8);
        for removed in [3, 0, 7, 4, 2] {
            assert!(handles.remove(removed));
        }
        assert_eq!(collect(&handles), [1, 5, 6]);
        handles.insert(8);
        handles.insert(9);
        assert_eq!(collect(&handles), [1, 5, 6, 8, 9]);
        for removed in [5, 6, 1, 9, 8] {
            assert!(handles.remove(removed));
        }
        assert!(collect(&handles).is_empty());
        assert_eq!(handles.len(), 0);
        assert!(handles.is_empty());
        assert!(!handles.remove(8));
        handles.insert(10);
        assert_eq!(collect(&handles), [10]);
    }

    #[test]
    fn original_missing_rows_do_not_become_live() {
        let mut handles = LiveHandles::from_sorted([1, 2, 5]);
        assert_eq!(collect(&handles), [1, 2, 5]);
        assert!(handles.remove(2));
        handles.insert(7);
        assert_eq!(collect(&handles), [1, 5, 7]);
        assert!(handles.contains(5));
        assert!(!handles.contains(4));
        let mut appended = Vec::new();
        handles.append_ordered(&mut appended);
        assert_eq!(appended, [1, 5, 7]);
    }
}
