//! Shared dynamic-table bookkeeping for the HPACK encoder and decoder.
//!
//! Both sides maintain a queue indexed from the back, where the most
//! recently inserted entry has dynamic offset 0. Eviction drops the oldest
//! owner immediately.
//! Each slot pairs the entry with its precomputed HPACK size so the buffer
//! does not need to know the entry shape.

use std::collections::VecDeque;

#[derive(Debug)]
pub(super) struct DynamicBuffer<E> {
    entries: VecDeque<(E, usize)>,
    size: usize,
    max_size: usize,
}

impl<E> DynamicBuffer<E> {
    pub(super) const fn new(max_size: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            size: 0,
            max_size,
        }
    }

    pub(super) fn live(&self) -> impl DoubleEndedIterator<Item = &(E, usize)> + ExactSizeIterator {
        self.entries.iter()
    }

    /// Returns the entry at `offset` slots back from the most recent insertion,
    /// where `offset == 0` is the newest live entry.
    pub(super) fn entry_from_end(&self, offset: usize) -> Option<&E> {
        self.live().rev().nth(offset).map(|(entry, _)| entry)
    }

    pub(super) fn set_max_size(&mut self, max_size: usize) {
        self.max_size = max_size;
        self.evict_to_fit(0);
    }

    pub(super) fn insert<F>(&mut self, size: usize, entry: F)
    where
        F: FnOnce() -> E,
    {
        if size > self.max_size {
            self.clear();
            return;
        }
        self.evict_to_fit(size);
        let entry = entry();
        self.size = self.size.wrapping_add(size);
        self.entries.push_back((entry, size));
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.size = 0;
    }

    fn evict_to_fit(&mut self, incoming: usize) {
        debug_assert!(incoming <= self.max_size);
        let available = self.max_size.wrapping_sub(incoming);
        while self.size > available {
            let (_, size) = self
                .entries
                .pop_front()
                .expect("retained size belongs to live entries");
            self.size = self.size.wrapping_sub(size);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::DynamicBuffer;

    #[test]
    fn partial_eviction_releases_the_oldest_owner() {
        let first = Arc::new(());
        let weak = Arc::downgrade(&first);
        let latest = Arc::new(());
        let mut table = DynamicBuffer::new(2);
        table.insert(1, || first);
        table.insert(1, || Arc::clone(&latest));
        table.set_max_size(1);
        assert!(weak.upgrade().is_none());
        assert!(Arc::ptr_eq(table.entry_from_end(0).unwrap(), &latest));
        assert!(table.entry_from_end(1).is_none());
    }

    #[test]
    fn oversized_entries_clear_without_constructing_a_retained_copy() {
        let mut table = DynamicBuffer::new(1);
        table.insert(1, || 7);
        table.insert(2, || panic!("an oversized entry cannot be retained"));
        assert!(table.entry_from_end(0).is_none());
    }
}
