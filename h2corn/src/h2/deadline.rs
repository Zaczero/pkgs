use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::hash::Hash;

use nohash_hasher::{BuildNoHashHasher, IsEnabled};
use tokio::time::Instant;

/// A lazily allocated, generation-stamped deadline index.
///
/// `current` is authoritative. Heap entries are immutable scheduling hints;
/// update and cancel leave stale hints behind and `peek` discards them. The
/// occasional rebuild bounds memory under adversarial update/cancel churn.
pub(super) struct DeadlineQueue<K> {
    heap: BinaryHeap<DeadlineEntry<K>>,
    current: HashMap<K, CurrentDeadline, BuildNoHashHasher<K>>,
    next_generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CurrentDeadline {
    at: Instant,
    generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DeadlineEntry<K> {
    key: K,
    at: Instant,
    generation: u64,
}

impl<K> Ord for DeadlineEntry<K>
where
    K: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; reverse the time/generation ordering so
        // the earliest deadline is at the top.
        other
            .at
            .cmp(&self.at)
            .then_with(|| other.generation.cmp(&self.generation))
            .then_with(|| other.key.cmp(&self.key))
    }
}

impl<K> PartialOrd for DeadlineEntry<K>
where
    K: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K> Default for DeadlineQueue<K> {
    fn default() -> Self {
        Self {
            heap: BinaryHeap::new(),
            current: HashMap::with_hasher(BuildNoHashHasher::default()),
            next_generation: 0,
        }
    }
}

impl<K> DeadlineQueue<K>
where
    K: Copy + Eq + Hash + IsEnabled + Ord,
{
    pub(super) fn schedule(&mut self, key: K, at: Instant) {
        self.next_generation = self.next_generation.wrapping_add(1);
        // Generation zero is reserved for the wrap boundary. Rebuilding
        // makes all live entries unambiguous before numbering restarts.
        if self.next_generation == 0 {
            self.rebuild_with_fresh_generations();
            self.next_generation = self.current.len() as u64 + 1;
        }
        let generation = self.next_generation;
        self.current.insert(key, CurrentDeadline { at, generation });
        self.heap.push(DeadlineEntry {
            key,
            at,
            generation,
        });
        self.compact_if_needed();
    }

    pub(super) fn cancel(&mut self, key: K) {
        self.current.remove(&key);
        self.compact_if_needed();
    }

    pub(super) fn next(&mut self) -> Option<(K, Instant)> {
        self.discard_stale();
        self.heap.peek().map(|entry| (entry.key, entry.at))
    }

    pub(super) fn pop_expired(&mut self, now: Instant) -> Option<(K, Instant)> {
        self.discard_stale();
        let entry = self.heap.peek().copied()?;
        if entry.at > now {
            return None;
        }
        self.heap.pop();
        self.current.remove(&entry.key);
        Some((entry.key, entry.at))
    }

    fn discard_stale(&mut self) {
        while self.heap.peek().is_some_and(|entry| {
            self.current.get(&entry.key).is_none_or(|current| {
                current.generation != entry.generation || current.at != entry.at
            })
        }) {
            self.heap.pop();
        }
    }

    fn compact_if_needed(&mut self) {
        // Avoid rebuilding for normal short bursts, but cap stale storage at
        // roughly 2x the live set plus a small fixed allowance.
        if self.heap.len() > self.current.len().saturating_mul(2).saturating_add(32) {
            self.rebuild();
        }
    }

    fn rebuild(&mut self) {
        self.heap = self
            .current
            .iter()
            .map(|(&key, current)| DeadlineEntry {
                key,
                at: current.at,
                generation: current.generation,
            })
            .collect();
    }

    fn rebuild_with_fresh_generations(&mut self) {
        let mut generation = 0_u64;
        for current in self.current.values_mut() {
            generation += 1;
            current.generation = generation;
        }
        self.next_generation = generation;
        self.rebuild();
    }

    #[cfg(test)]
    pub(super) fn storage_len(&self) -> usize {
        self.heap.len()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::DeadlineQueue;

    #[test]
    fn update_cancel_and_expiry_never_return_stale_entries() {
        let now = Instant::now();
        let mut deadlines = DeadlineQueue::default();
        deadlines.schedule(1_u32, now + Duration::from_secs(3));
        deadlines.schedule(2, now + Duration::from_secs(2));
        deadlines.schedule(1, now + Duration::from_secs(1));
        deadlines.cancel(2);

        assert_eq!(deadlines.next(), Some((1, now + Duration::from_secs(1))));
        assert_eq!(deadlines.pop_expired(now), None);
        assert_eq!(
            deadlines.pop_expired(now + Duration::from_secs(1)),
            Some((1, now + Duration::from_secs(1))),
        );
        assert_eq!(deadlines.next(), None);
    }

    #[test]
    fn churn_is_compacted_independently_of_stream_id_sparsity() {
        let now = Instant::now();
        let mut deadlines = DeadlineQueue::default();
        for generation in 0..10_000 {
            deadlines.schedule(0x7FFF_FFFD_u32, now + Duration::from_nanos(generation + 1));
        }
        assert!(deadlines.storage_len() <= 33);
        assert_eq!(
            deadlines.next(),
            Some((0x7FFF_FFFD, now + Duration::from_micros(10))),
        );
    }
}
