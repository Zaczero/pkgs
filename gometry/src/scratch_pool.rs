use std::cell::Cell;
use std::collections::HashMap;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use std::thread::LocalKey;

pub(crate) const SCRATCH_RETAIN_BYTES: usize = 8 * 1024 * 1024;

pub(crate) trait RetainScratch: Default {
    fn clear_for_reuse(&mut self);

    fn retained_bytes(&self) -> usize;
}

impl<T> RetainScratch for Vec<T> {
    fn clear_for_reuse(&mut self) {
        self.clear();
    }

    fn retained_bytes(&self) -> usize {
        flat_vec_retained_bytes(self)
    }
}

const fn allocation_bytes(capacity: usize, element_size: usize) -> usize {
    capacity.saturating_mul(element_size)
}

pub(crate) const fn flat_vec_retained_bytes<T>(value: &Vec<T>) -> usize {
    allocation_bytes(value.capacity(), size_of::<T>())
}

pub(crate) fn nested_vec_retained_bytes<T>(value: &Vec<Vec<T>>) -> usize {
    value.iter().fold(
        allocation_bytes(value.capacity(), size_of::<Vec<T>>()),
        |bytes, inner| bytes.saturating_add(flat_vec_retained_bytes(inner)),
    )
}

/// Conservatively charges one machine word of hash-table metadata per entry.
/// This deliberately avoids depending on hashbrown's private bucket layout.
pub(crate) fn hash_map_retained_bytes<K, V, S>(value: &HashMap<K, V, S>) -> usize {
    allocation_bytes(
        value.capacity(),
        size_of::<K>()
            .saturating_add(size_of::<V>())
            .saturating_add(size_of::<usize>()),
    )
}

pub(crate) struct ScratchPool<T> {
    value: Cell<T>,
    retain_bytes: usize,
}

impl<T> ScratchPool<T> {
    pub(crate) const fn new(value: T) -> Self {
        Self::with_retain_bytes(value, SCRATCH_RETAIN_BYTES)
    }

    pub(crate) const fn with_retain_bytes(value: T, retain_bytes: usize) -> Self {
        Self {
            value: Cell::new(value),
            retain_bytes,
        }
    }
}

impl<T: RetainScratch + 'static> ScratchPool<T> {
    pub(crate) fn checkout(key: &'static LocalKey<Self>) -> ScratchLease<T> {
        let value = key.with(|pool| pool.value.take());
        ScratchLease {
            key,
            value: Some(value),
        }
    }
}

pub(crate) struct ScratchLease<T: RetainScratch + 'static> {
    key: &'static LocalKey<ScratchPool<T>>,
    value: Option<T>,
}

impl<T: RetainScratch> Deref for ScratchLease<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
            .as_ref()
            .expect("scratch lease always owns a value")
    }
}

impl<T: RetainScratch> DerefMut for ScratchLease<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value
            .as_mut()
            .expect("scratch lease always owns a value")
    }
}

impl<T: RetainScratch> Drop for ScratchLease<T> {
    fn drop(&mut self) {
        let Some(mut value) = self.value.take() else {
            return;
        };
        let return_value = || {
            value.clear_for_reuse();
            let _ = self.key.try_with(|pool| {
                if value.retained_bytes() <= pool.retain_bytes {
                    pool.value.set(value);
                }
            });
        };
        if std::thread::panicking() {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(return_value));
        } else {
            return_value();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Default)]
    struct ObservedScratch {
        values: Vec<Rc<()>>,
        retained: usize,
        clears: Rc<Cell<usize>>,
    }

    impl RetainScratch for ObservedScratch {
        fn clear_for_reuse(&mut self) {
            self.values.clear();
            self.clears.set(self.clears.get() + 1);
        }

        fn retained_bytes(&self) -> usize {
            assert!(self.values.is_empty(), "live values must be cleared first");
            self.retained
        }
    }

    thread_local! {
        static SMALL_POOL: ScratchPool<ObservedScratch> = ScratchPool::with_retain_bytes(
            ObservedScratch::default(), 8
        );
    }

    #[test]
    fn retains_under_and_at_budget_but_drops_over_budget_after_clearing() {
        let clears = Rc::new(Cell::new(0));
        let live = Rc::new(());
        for retained in [7, 8, 9] {
            let mut lease = ScratchPool::checkout(&SMALL_POOL);
            lease.clears = Rc::clone(&clears);
            lease.retained = retained;
            lease.values.push(Rc::clone(&live));
            drop(lease);
            assert_eq!(Rc::strong_count(&live), 1);
        }
        assert_eq!(clears.get(), 3);
        let lease = ScratchPool::checkout(&SMALL_POOL);
        assert_eq!(lease.retained, 0, "the over-budget value was dropped");
    }

    #[test]
    fn nested_checkout_keeps_the_last_returned_outer_value() {
        let mut outer = ScratchPool::checkout(&SMALL_POOL);
        outer.retained = 3;
        {
            let mut inner = ScratchPool::checkout(&SMALL_POOL);
            inner.retained = 5;
        }
        drop(outer);
        assert_eq!(ScratchPool::checkout(&SMALL_POOL).retained, 3);
    }

    #[test]
    fn operation_panic_returns_cleared_scratch() {
        let result = std::panic::catch_unwind(|| {
            let mut lease = ScratchPool::checkout(&SMALL_POOL);
            lease.retained = 4;
            lease.values.push(Rc::new(()));
            panic!("operation failed");
        });
        assert!(result.is_err());
        let lease = ScratchPool::checkout(&SMALL_POOL);
        assert!(lease.values.is_empty());
        assert_eq!(lease.retained, 4);
    }

    #[derive(Default)]
    struct PanickingClear;

    impl RetainScratch for PanickingClear {
        fn clear_for_reuse(&mut self) {
            panic!("clear failed");
        }

        fn retained_bytes(&self) -> usize {
            0
        }
    }

    thread_local! {
        static PANICKING_POOL: ScratchPool<PanickingClear> = const { ScratchPool::new(PanickingClear) };
    }

    #[test]
    fn failing_cleanup_does_not_replace_an_operation_panic() {
        let result = std::panic::catch_unwind(|| {
            let _lease = ScratchPool::checkout(&PANICKING_POOL);
            panic!("operation failed");
        });
        assert_eq!(
            result.unwrap_err().downcast_ref::<&str>(),
            Some(&"operation failed")
        );
    }

    #[test]
    fn accounting_is_saturating_and_includes_nested_allocations() {
        let nested = vec![Vec::<u64>::with_capacity(3), Vec::<u64>::with_capacity(5)];
        assert_eq!(
            nested_vec_retained_bytes(&nested),
            nested.capacity() * size_of::<Vec<u64>>() + u64::BITS as usize
        );
        let impossible = usize::MAX / size_of::<u64>() + 1;
        assert_eq!(allocation_bytes(impossible, size_of::<u64>()), usize::MAX);

        let map = HashMap::<u32, u64>::with_capacity(4);
        assert_eq!(
            hash_map_retained_bytes(&map),
            map.capacity() * (size_of::<u32>() + size_of::<u64>() + size_of::<usize>())
        );
    }

    #[derive(Default)]
    struct CapacityScratch(Vec<u8>);

    impl RetainScratch for CapacityScratch {
        fn clear_for_reuse(&mut self) {
            self.0.clear();
        }

        fn retained_bytes(&self) -> usize {
            flat_vec_retained_bytes(&self.0)
        }
    }

    thread_local! {
        static CAPACITY_POOL: ScratchPool<CapacityScratch> = ScratchPool::with_retain_bytes(
            CapacityScratch::default(), 128
        );
    }

    #[test]
    fn medium_repeats_reuse_and_huge_then_medium_warms_once() {
        let medium_capacity = {
            let mut lease = ScratchPool::checkout(&CAPACITY_POOL);
            lease.0.reserve_exact(64);
            lease.0.capacity()
        };
        let (reused_capacity, repeat_allocations) = crate::test_support::allocation_count(|| {
            let mut medium = ScratchPool::checkout(&CAPACITY_POOL);
            medium.0.reserve_exact(64);
            medium.0.capacity()
        });
        assert_eq!(reused_capacity, medium_capacity);
        assert_eq!(repeat_allocations, 0);

        {
            let mut huge = ScratchPool::checkout(&CAPACITY_POOL);
            huge.0.reserve_exact(256);
        }
        assert_eq!(ScratchPool::checkout(&CAPACITY_POOL).0.capacity(), 0);

        let (warmed, warm_allocations) = crate::test_support::allocation_count(|| {
            let mut medium = ScratchPool::checkout(&CAPACITY_POOL);
            medium.0.reserve_exact(64);
            medium.0.capacity()
        });
        assert_eq!(warm_allocations, 1);
        let ((), repeat_allocations) = crate::test_support::allocation_count(|| {
            let mut medium = ScratchPool::checkout(&CAPACITY_POOL);
            medium.0.reserve_exact(64);
            assert_eq!(medium.0.capacity(), warmed);
        });
        assert_eq!(repeat_allocations, 0);
    }

    static THREAD_DROPS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Default)]
    struct ThreadScratch(Vec<u8>);

    impl Drop for ThreadScratch {
        fn drop(&mut self) {
            THREAD_DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl RetainScratch for ThreadScratch {
        fn clear_for_reuse(&mut self) {
            self.0.clear();
        }

        fn retained_bytes(&self) -> usize {
            flat_vec_retained_bytes(&self.0)
        }
    }

    thread_local! {
        static THREAD_POOL: ScratchPool<ThreadScratch> = const { ScratchPool::new(ThreadScratch(Vec::new())) };
    }

    #[test]
    fn worker_threads_have_independent_pools_and_teardown_drops_values() {
        THREAD_DROPS.store(0, Ordering::Relaxed);
        
        assert_eq!(
            (1..=2)
            .map(|capacity| {
                std::thread::spawn(move || {
                    let mut lease = ScratchPool::checkout(&THREAD_POOL);
                    lease.0.reserve_exact(capacity * 16);
                    lease.0.capacity()
                })
            })
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>(),
            [16, 32]
        );
        assert_eq!(THREAD_DROPS.load(Ordering::Relaxed), 4);
    }

    #[derive(Default)]
    struct TeardownScratch;

    impl RetainScratch for TeardownScratch {
        fn clear_for_reuse(&mut self) {}

        fn retained_bytes(&self) -> usize {
            0
        }
    }

    thread_local! {
        static TEARDOWN_POOL: ScratchPool<TeardownScratch> = const { ScratchPool::new(TeardownScratch) };
        static TEARDOWN_LEASE: std::cell::RefCell<Option<ScratchLease<TeardownScratch>>> = const {
            std::cell::RefCell::new(None)
        };
    }

    #[test]
    fn lease_drop_during_tls_teardown_does_not_panic() {
        let handle = std::thread::spawn(|| {
            // Initialize the lease holder first so the pool is destroyed before
            // the holder tries to return its lease during reverse-order teardown.
            TEARDOWN_LEASE.with(|holder| assert!(holder.borrow().is_none()));
            let lease = ScratchPool::checkout(&TEARDOWN_POOL);
            TEARDOWN_LEASE.with(|holder| *holder.borrow_mut() = Some(lease));
        });
        handle.join().unwrap();
    }
}
