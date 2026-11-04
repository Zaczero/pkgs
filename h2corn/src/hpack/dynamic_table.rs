//! Shared dynamic-table bookkeeping for the HPACK encoder and decoder.
//!
//! Both sides maintain a queue indexed from the back, where the most
//! recently inserted entry has dynamic offset 0. Eviction drops the oldest
//! owner immediately.
//! Entry sizes are derived from the retained name and value lengths.

use std::collections::VecDeque;

use super::HpackField;

#[derive(Debug)]
pub(super) struct DynamicBuffer {
    entries: VecDeque<HpackField>,
    size: usize,
    max_size: usize,
}

impl DynamicBuffer {
    pub(super) const fn new(max_size: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            size: 0,
            max_size,
        }
    }

    pub(super) fn live(&self) -> impl DoubleEndedIterator<Item = &HpackField> + ExactSizeIterator {
        self.entries.iter()
    }

    /// Returns the entry at `offset` slots back from the most recent insertion,
    /// where `offset == 0` is the newest live entry.
    pub(super) fn entry_from_end(&self, offset: usize) -> Option<&HpackField> {
        self.live().rev().nth(offset)
    }

    pub(super) fn set_max_size(&mut self, max_size: usize) {
        self.max_size = max_size;
        self.evict_to_fit(0);
    }

    pub(super) fn insert<F>(&mut self, size: usize, entry: F)
    where
        F: FnOnce() -> HpackField,
    {
        if size > self.max_size {
            self.clear();
            return;
        }
        self.evict_to_fit(size);
        let entry = entry();
        debug_assert_eq!(entry.table_size(), size);
        self.size = self.size.wrapping_add(size);
        self.entries.push_back(entry);
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.size = 0;
    }

    fn evict_to_fit(&mut self, incoming: usize) {
        debug_assert!(incoming <= self.max_size);
        let available = self.max_size.wrapping_sub(incoming);
        while self.size > available {
            let entry = self
                .entries
                .pop_front()
                .expect("retained size belongs to live entries");
            self.size = self.size.wrapping_sub(entry.table_size());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::DynamicBuffer;
    use crate::hpack::HpackField;

    #[test]
    fn partial_eviction_releases_the_oldest_owner() {
        let first: Arc<[u8]> = Arc::from(&b"x"[..]);
        let weak = Arc::downgrade(&first);
        let latest: Arc<[u8]> = Arc::from(&b"y"[..]);
        let mut table = DynamicBuffer::new(66);
        table.insert(33, || {
            HpackField::from_parts(Bytes::from_owner(first), Bytes::new())
        });
        table.insert(33, || {
            HpackField::from_parts(Bytes::from_owner(Arc::clone(&latest)), Bytes::new())
        });
        table.set_max_size(33);
        assert!(weak.upgrade().is_none());
        assert_eq!(
            table.entry_from_end(0).unwrap().name().as_ptr(),
            latest.as_ptr()
        );
        assert!(table.entry_from_end(1).is_none());
    }

    #[test]
    fn oversized_entries_clear_without_constructing_a_retained_copy() {
        let mut table = DynamicBuffer::new(33);
        table.insert(33, || HpackField::from_static(b"x", b""));
        table.insert(34, || panic!("an oversized entry cannot be retained"));
        assert!(table.entry_from_end(0).is_none());
    }
}
