use std::fmt::{self, Debug, Formatter};
use std::mem::MaybeUninit;
use std::ptr;

use smallvec::SmallVec;

pub(crate) struct InlineFifo<T, const N: usize> {
    // SAFETY INVARIANT: slots before `front` are uninitialized;
    // `front..items.len()` is one contiguous initialized range of owned `T`s.
    // An empty queue is normalized to `front == items.len() == 0`. Slots at or
    // beyond the SmallVec length never own a value, including stale bytes left
    // by compaction. Every mutation preserves this partition before it can
    // panic, and Drop destroys exactly the live range.
    //
    // MaybeUninit prevents SmallVec from generating per-slot discriminant and
    // destruction paths; this type alone owns initialization and destruction.
    items: SmallVec<[MaybeUninit<T>; N]>,
    front: usize,
}

impl<T: Debug, const N: usize> Debug for InlineFifo<T, N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        struct LiveItems<'a, T, const N: usize>(&'a InlineFifo<T, N>);

        impl<T: Debug, const N: usize> Debug for LiveItems<'_, T, N> {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                f.debug_list().entries(self.0.iter()).finish()
            }
        }

        f.debug_struct("InlineFifo")
            .field("items", &LiveItems(self))
            .field("front", &self.front)
            .finish()
    }
}

impl<T, const N: usize> Default for InlineFifo<T, N> {
    fn default() -> Self {
        Self {
            items: SmallVec::new(),
            front: 0,
        }
    }
}

impl<T, const N: usize> InlineFifo<T, N> {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn len(&self) -> usize {
        debug_assert!(self.front <= self.items.len());
        self.items.len() - self.front
    }

    pub(crate) fn front(&self) -> Option<&T> {
        if self.is_empty() {
            return None;
        }
        // SAFETY: a non-empty queue has an initialized item at `front`.
        Some(unsafe { self.items.get_unchecked(self.front).assume_init_ref() })
    }

    pub(crate) fn front_mut(&mut self) -> Option<&mut T> {
        if self.is_empty() {
            return None;
        }

        // SAFETY: a non-empty queue has an initialized item at `front`, and
        // `&mut self` makes the returned reference exclusive.
        Some(unsafe { self.items.get_unchecked_mut(self.front).assume_init_mut() })
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        self.items[self.front..].iter().map(|item| {
            // SAFETY: the sliced live range is exactly the initialized range.
            unsafe { item.assume_init_ref() }
        })
    }

    pub(crate) fn push_back(&mut self, item: T) {
        if self.front >= N && self.front >= self.len() {
            self.compact();
        }
        self.items.push(MaybeUninit::new(item));
    }

    pub(crate) fn pop_front(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }

        // SAFETY: a non-empty queue has an initialized item at `front`; the
        // front advance immediately removes that slot from the live range.
        let item = unsafe { self.items.get_unchecked(self.front).assume_init_read() };
        self.advance_front();
        Some(item)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.front == self.items.len()
    }

    pub(crate) fn clear(&mut self) {
        let live = self.len();
        // SAFETY: the live range contains contiguous initialized `T`s with
        // identical layout to `MaybeUninit<T>`. Marking the SmallVec empty
        // first transfers destruction responsibility to the temporary slice
        // and keeps `clear` sound even if an element destructor panics.
        let live = unsafe {
            let live = ptr::slice_from_raw_parts_mut(
                self.items.as_mut_ptr().add(self.front).cast::<T>(),
                live,
            );
            self.items.set_len(0);
            self.front = 0;
            live
        };
        // SAFETY: ownership of every formerly live item was transferred to
        // this slice exactly once above.
        unsafe { ptr::drop_in_place(live) };
    }

    fn advance_front(&mut self) {
        debug_assert!(!self.is_empty());
        self.front += 1;
        if self.is_empty() {
            // Every slot has either been moved out or was already consumed.
            // Clearing MaybeUninit resets storage without running T's drop
            // glue or entering the general live-range destruction path.
            self.items.clear();
            self.front = 0;
        }
    }

    fn compact(&mut self) {
        if self.front == 0 {
            return;
        }
        let live = self.len();
        // SAFETY: source and destination are within the allocation and may
        // overlap, hence `copy`. The source live range is moved byte-for-byte
        // to the start; shortening the MaybeUninit vector makes only the
        // destination range authoritative and drops no duplicate values.
        unsafe {
            ptr::copy(
                self.items.as_ptr().add(self.front),
                self.items.as_mut_ptr(),
                live,
            );
            self.items.set_len(live);
        }
        self.front = 0;
    }
}

impl<T, const N: usize> Drop for InlineFifo<T, N> {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::InlineFifo;

    struct DropCount(Arc<AtomicUsize>);

    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn inline_spill_pop_compact_clear_and_drop_destroy_exactly_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut queue = InlineFifo::<_, 2>::new();
        for _ in 0..6 {
            queue.push_back(DropCount(Arc::clone(&drops)));
        }
        drop(queue.pop_front());
        drop(queue.pop_front());
        drop(queue.pop_front());
        assert_eq!(drops.load(Ordering::Relaxed), 3);

        // With three consumed spilled slots, the next push compacts the live
        // range before appending and must not duplicate ownership.
        queue.push_back(DropCount(Arc::clone(&drops)));
        queue.clear();
        assert_eq!(drops.load(Ordering::Relaxed), 7);
        queue.push_back(DropCount(Arc::clone(&drops)));
        drop(queue);
        assert_eq!(drops.load(Ordering::Relaxed), 8);
    }

    #[test]
    fn queue_order_and_mutable_front_survive_compaction() {
        let mut queue = InlineFifo::<_, 2>::new();
        queue.push_back(1);
        queue.push_back(2);
        queue.push_back(3);
        assert_eq!(queue.pop_front(), Some(1));
        assert_eq!(queue.pop_front(), Some(2));
        queue.push_back(4);
        *queue.front_mut().unwrap() = 30;
        assert_eq!(queue.iter().copied().collect::<Vec<_>>(), [30, 4]);
    }

    fn assert_exhaustive_state_model<const N: usize>() {
        const OPERATIONS: usize = 4;
        const DEPTH: u32 = 8;

        for mut program in 0..OPERATIONS.pow(DEPTH) {
            let mut queue = InlineFifo::<u32, N>::new();
            let mut model = VecDeque::new();
            let mut next = 0_u32;

            for _ in 0..DEPTH {
                match program % OPERATIONS {
                    0 => {
                        queue.push_back(next);
                        model.push_back(next);
                        next += 1;
                    },
                    1 => assert_eq!(queue.pop_front(), model.pop_front()),
                    2 => {
                        queue.clear();
                        model.clear();
                    },
                    3 => {
                        if let Some(value) = queue.front_mut() {
                            *value = value.wrapping_add(17);
                        }
                        if let Some(value) = model.front_mut() {
                            *value = value.wrapping_add(17);
                        }
                    },
                    _ => unreachable!(),
                }
                program /= OPERATIONS;

                assert_eq!(queue.len(), model.len());
                assert_eq!(queue.is_empty(), model.is_empty());
                assert_eq!(queue.front(), model.front());
                assert!(queue.iter().copied().eq(model.iter().copied()));
                if model.is_empty() {
                    assert_eq!(queue.front, 0);
                    assert_eq!(queue.items.len(), 0);
                }
            }
        }
    }

    #[test]
    fn exhaustive_small_state_machine_matches_vecdeque_and_normalizes_empty() {
        assert_exhaustive_state_model::<0>();
        assert_exhaustive_state_model::<1>();
        assert_exhaustive_state_model::<2>();
        assert_exhaustive_state_model::<4>();
    }

    #[test]
    fn zero_inline_capacity_zst_and_repeated_clear_reuse_are_supported() {
        let mut queue = InlineFifo::<(), 0>::new();
        for _ in 0..4 {
            queue.push_back(());
        }
        assert_eq!(queue.len(), 4);
        queue.clear();
        queue.clear();
        queue.push_back(());
        assert_eq!(queue.pop_front(), Some(()));
        assert!(queue.is_empty());
    }

    #[test]
    fn panicking_destructor_cannot_leave_live_ownership_in_queue() {
        struct PanicDrop {
            id: usize,
            drops: Arc<AtomicUsize>,
        }

        impl Drop for PanicDrop {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::Relaxed);
                assert!(
                    self.id != 1 || std::thread::panicking(),
                    "intentional destructor panic"
                );
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let mut queue = InlineFifo::<_, 2>::new();
        for id in 0..3 {
            queue.push_back(PanicDrop {
                id,
                drops: Arc::clone(&drops),
            });
        }
        assert!(catch_unwind(AssertUnwindSafe(|| queue.clear())).is_err());
        assert!(queue.is_empty());
        assert_eq!(drops.load(Ordering::Relaxed), 3);

        queue.push_back(PanicDrop {
            id: 4,
            drops: Arc::clone(&drops),
        });
        drop(queue);
        assert_eq!(drops.load(Ordering::Relaxed), 4);
    }
}
