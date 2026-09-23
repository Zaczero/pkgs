use std::time::Duration;

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
            self.entries.sort_unstable_by_key(|entry| entry.generation);
            let mut generation = 0_u32;
            for entry in &mut self.entries {
                generation = generation
                    .checked_add(1)
                    .expect("live deadlines fit generation space");
                entry.generation = generation;
            }
            self.next_generation = generation
                .checked_add(1)
                .expect("new deadline fits generation space");
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

    /// Replace `key` when its relative deadline is representable, or remove
    /// its previous deadline when the monotonic clock cannot represent it.
    pub(super) fn reschedule_after(&mut self, key: K, now: Instant, timeout: Duration) {
        if let Some(at) = now.checked_add(timeout) {
            self.schedule(key, at);
        } else {
            self.cancel(key);
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
        let (index, &entry) = self
            .entries
            .iter()
            .enumerate()
            .min_by_key(|(_, entry)| (entry.at, entry.generation, entry.key))?;
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

    #[test]
    fn generation_wrap_preserves_ties_after_cancellation_and_rescheduling() {
        let now = Instant::now();
        let mut deadlines = DeadlineQueue::default();
        for key in [10, 20, 30, 40] {
            deadlines.schedule(key, now);
        }
        deadlines.cancel(10);
        deadlines.schedule(20, now);
        deadlines.set_next_generation(u32::MAX);
        deadlines.schedule(50, now);

        for key in [30, 40, 20, 50] {
            assert_eq!(deadlines.next(), Some((key, now)));
            assert_eq!(deadlines.pop_expired(now), Some((key, now)));
        }
        assert_eq!(deadlines.next(), None);
    }

    fn instant_without_one_second_of_headroom(base: Instant) -> Instant {
        let mut fits = 0_u64;
        let mut overflows = u64::MAX;
        while fits.abs_diff(overflows) > 1 {
            let candidate = fits.midpoint(overflows);
            if base.checked_add(Duration::from_secs(candidate)).is_some() {
                fits = candidate;
            } else {
                overflows = candidate;
            }
        }
        base.checked_add(Duration::from_secs(fits))
            .expect("last whole-second instant fits")
    }

    #[test]
    fn unrepresentable_reschedule_cancels_the_previous_deadline() {
        let now = Instant::now();
        let timeout = Duration::from_secs(1);
        let later = instant_without_one_second_of_headroom(now);
        assert!(now.checked_add(timeout).is_some());
        assert!(later.checked_add(timeout).is_none());
        let old_panic = std::panic::catch_unwind(|| {
            later
                .checked_add(timeout)
                .expect("old at-use deadline path")
        })
        .expect_err("old at-use deadline path must panic");
        drop(old_panic);

        let mut deadlines = DeadlineQueue::default();
        let ordinary_timeout = Duration::from_secs(3);
        let expected = now
            .checked_add(ordinary_timeout)
            .expect("ordinary test deadline fits");
        deadlines.reschedule_after(1_u32, now, ordinary_timeout);
        assert_eq!(deadlines.next(), Some((1, expected)));

        deadlines.reschedule_after(1, later, timeout);
        assert_eq!(deadlines.next(), None);
    }
}
