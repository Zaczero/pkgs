//! Priority queue for bounded-search patches.

use super::span_bounds::nonnegative_upper_bound;
use super::{BinaryHeap, Ordering, PatchRecord, Rational, Result};

#[derive(Debug)]
pub(super) struct QueueEntry {
    key: f64,
    order: usize,
    record: PatchRecord,
}

impl PartialEq for QueueEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key.to_bits() == other.key.to_bits() && self.order == other.order
    }
}

impl Eq for QueueEntry {}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .total_cmp(&other.key)
            // Earlier extraction order wins an exact queue-key tie. This is
            // part of the directed result's deterministic witness contract.
            .then_with(|| other.order.cmp(&self.order))
    }
}

#[derive(Debug)]
pub(super) struct PatchQueue {
    heap: BinaryHeap<QueueEntry>,
    stable_ties: bool,
    next_order: usize,
}

impl PatchQueue {
    pub(super) const fn new(stable_ties: bool) -> Self {
        Self {
            heap: BinaryHeap::new(),
            stable_ties,
            next_order: 0,
        }
    }

    pub(super) fn push(&mut self, record: PatchRecord) -> Result<()> {
        let key = match &record.upper {
            Some(value) => nonnegative_upper_bound(value)?,
            None => f64::INFINITY,
        };
        let order = if self.stable_ties {
            let order = self.next_order;
            self.next_order = self
                .next_order
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            order
        } else {
            0
        };
        self.heap.push(QueueEntry { key, order, record });

        Ok(())
    }

    pub(super) fn pop(&mut self) -> Option<PatchRecord> {
        self.heap.pop().map(|entry| entry.record)
    }

    pub(super) fn max_upper(&self) -> Option<Rational> {
        let entry = self.heap.peek()?;
        (!entry.key.is_infinite()).then(|| Rational::from_f64(entry.key))
    }

    pub(super) fn max_overflow_upper(&self) -> Result<Option<Rational>> {
        let mut maximum: Option<Rational> = None;
        for entry in &self.heap {
            if !entry.key.is_infinite() {
                continue;
            }
            let Some(upper) = &entry.record.upper else {
                return Ok(None);
            };
            let replaces = match &maximum {
                None => true,
                Some(current) => upper.try_cmp(current)? == Ordering::Greater,
            };
            if replaces {
                maximum = Some(upper.clone());
            }
        }
        Ok(maximum)
    }

    #[expect(
        clippy::missing_const_for_fn,
        reason = "Kani 0.68's standard library lacks const BinaryHeap queries"
    )]
    pub(super) fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}
