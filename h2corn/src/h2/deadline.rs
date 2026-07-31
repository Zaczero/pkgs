
use tokio::time::Instant;

/// A lazily allocated, generation-stamped deadline index.
///
/// HTTP/2 has at most `max_concurrent_streams` live deadlines (256 by
/// default), so one compact vector is both smaller and simpler than a heap
/// plus a second authoritative map. Updates replace their unique entry and
/// cancellation removes it, leaving no stale state to reconcile.
pub(super) struct DeadlineQueue<K> {
    entries: Vec<DeadlineEntry<K>>,
    next_generation: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DeadlineEntry<K> {
    key: K,
    at: Instant,
    generation: u32,
}

impl<K> Default for DeadlineQueue<K> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            next_generation: 0,
        }
    }
}

impl<K> DeadlineQueue<K>
where
    K: Copy + Eq + Ord,
{
    pub(super) fn schedule(&mut self, key: K, at: Instant) {
        self.next_generation = self.next_generation.wrapping_add(1);
        if self.next_generation == 0 {
            let mut generation = 0;
            for entry in &mut self.entries {
                generation += 1;
                entry.generation = generation;
            }
            self.next_generation = generation + 1;
        }
        let generation = self.next_generation;
        if let Some(entry) = self.entries.iter_mut().find(|entry| entry.key == key) {
            *entry = DeadlineEntry {
                key,
                at,
                generation,
            };
        } else {
            self.entries.push(DeadlineEntry {
                key,
                at,
                generation,
            });
        }
    }

    pub(super) fn cancel(&mut self, key: K) {
        if let Some(index) = self.entries.iter().position(|entry| entry.key == key) {
            self.entries.swap_remove(index);
        }
    }

    pub(super) fn next(&self) -> Option<(K, Instant)> {
        self.entries
            .iter()
            .min_by_key(|entry| (entry.at, entry.generation, entry.key))
            .map(|entry| (entry.key, entry.at))
    }

    pub(super) fn pop_expired(&mut self, now: Instant) -> Option<(K, Instant)> {
        let index = self
            .entries
            .iter()
            .enumerate()
            .min_by_key(|(_, entry)| (entry.at, entry.generation, entry.key))
            .map(|(index, _)| index)?;
        let entry = self.entries[index];
        if entry.at > now {
            return None;
        }
        self.entries.swap_remove(index);
        Some((entry.key, entry.at))
    }

    #[cfg(test)]
    pub(super) const fn storage_len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(super) const fn set_next_generation(&mut self, generation: u32) {
        self.next_generation = generation;
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

    #[test]
    fn generation_wrap_keeps_live_deadline_order_distinct() {
        let now = Instant::now();
        let mut deadlines = DeadlineQueue::default();
        deadlines.set_next_generation(u32::MAX - 1);
        deadlines.schedule(2_u32, now);
        deadlines.schedule(1, now);

        assert_eq!(deadlines.pop_expired(now), Some((2, now)));
        assert_eq!(deadlines.pop_expired(now), Some((1, now)));
    }
}
