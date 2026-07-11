use std::fmt::{self, Debug, Formatter};
use std::mem::MaybeUninit;
use std::ptr;

use smallvec::SmallVec;

pub(crate) struct InlineFifo<T, const N: usize> {
    /// Slots before `front` are uninitialized; `front..items.len()` is one
    /// contiguous initialized range. `MaybeUninit` prevents SmallVec from
    /// generating per-slot discriminant and destruction paths.
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
        self.items.len().saturating_sub(self.front)
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
        if self.front >= N && self.front * 2 >= self.items.len() {
            self.compact();
        } else if self.is_empty() {
            self.clear();
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
            self.clear();
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
}
