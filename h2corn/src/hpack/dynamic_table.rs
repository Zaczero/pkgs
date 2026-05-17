//! Shared dynamic-table bookkeeping for the HPACK encoder and decoder.
//!
//! Both sides maintain a growing buffer indexed from the back, where the
//! most recently inserted entry has dynamic offset 0. `start` advances on
//! eviction; `compact` reclaims that slack once it dominates the buffer.
//! Each slot pairs the entry with its precomputed HPACK size so the buffer
//! does not need to know the entry shape.

#[derive(Debug)]
pub(super) struct DynamicBuffer<E> {
    entries: Vec<(E, usize)>,
    start: usize,
    size: usize,
    max_size: usize,
}

impl<E> DynamicBuffer<E> {
    pub(super) const fn new(max_size: usize) -> Self {
        Self {
            entries: Vec::new(),
            start: 0,
            size: 0,
            max_size,
        }
    }

    pub(super) fn live(&self) -> &[(E, usize)] {
        &self.entries[self.start..]
    }

    /// Returns the entry at `offset` slots back from the most recent insertion,
    /// where `offset == 0` is the newest live entry.
    pub(super) fn entry_from_end(&self, offset: usize) -> Option<&E> {
        let slot = self.entries.len().checked_sub(1)?.checked_sub(offset)?;
        if slot < self.start {
            return None;
        }
        self.entries.get(slot).map(|(entry, _)| entry)
    }

    pub(super) fn set_max_size(&mut self, max_size: usize) {
        self.max_size = max_size;
        self.evict_to_fit(0);
    }

    pub(super) fn insert(&mut self, entry: E, size: usize) {
        if size > self.max_size {
            self.clear();
            return;
        }
        self.evict_to_fit(size);
        self.size += size;
        self.entries.push((entry, size));
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.start = 0;
        self.size = 0;
    }

    fn evict_to_fit(&mut self, incoming: usize) {
        while self.size + incoming > self.max_size {
            let Some((_, size)) = self.entries.get(self.start) else {
                self.clear();
                return;
            };
            self.size -= size;
            self.start += 1;
        }
        self.compact();
    }

    fn compact(&mut self) {
        if self.start == 0 {
            return;
        }
        if self.start == self.entries.len() {
            self.entries.clear();
            self.start = 0;
            return;
        }
        if self.start >= 32 && self.start * 2 >= self.entries.len() {
            self.entries.drain(..self.start);
            self.start = 0;
        }
    }
}
