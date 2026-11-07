//! Support for [free allocation lists][1].
//!
//! This can improve performance for types that are often created and deleted in quick succession.
//!
//! Rather than implementing this manually,
//! implement it by annotating a struct with `#[pyclass(freelist = N)]`,
//! where `N` is the size of the freelist.
//!
//! [1]: https://en.wikipedia.org/wiki/Free_list

use crate::ffi;
use crate::marker::Python;
use core::ptr::NonNull;

#[cfg(all(not(Py_GIL_DISABLED), any(PyPy, GraalPy, RustPython)))]
use crate::platform::sync::nonpoison::Mutex as FreeListCell;
#[cfg(not(any(Py_GIL_DISABLED, PyPy, GraalPy, RustPython)))]
use core::cell::UnsafeCell as FreeListCell;

/// Whether this ABI benefits from a class-wide cache.
///
/// Free-threaded CPython already recycles through its thread-local allocator.
/// Avoid a second, shared synchronization point, including in abi3t builds.
pub const CACHE_ENABLED: bool = !cfg!(Py_GIL_DISABLED);

/// Bounded storage for destroyed allocations of one concrete Python class.
///
/// The initialized prefix owns the allocations; entries outside it are inert
/// pointer bits. No Python calls, destructors or detachment occur during access.
/// Like PyO3's static type objects, this cache belongs to its single supported
/// interpreter.
pub struct FreeList<const N: usize> {
    #[cfg(not(Py_GIL_DISABLED))]
    list: FreeListCell<PyObjectFreeList<N>>,
}

// SAFETY: on GIL-enabled CPython, every access requires an attached thread
// holding the GIL, and the private leaf operations cannot release it or reenter.
#[cfg(not(any(Py_GIL_DISABLED, PyPy, GraalPy, RustPython)))]
unsafe impl<const N: usize> Sync for FreeList<N> {}

impl<const N: usize> FreeList<N> {
    #[expect(clippy::new_without_default, reason = "always called in const context")]
    pub const fn new() -> Self {
        Self {
            #[cfg(not(Py_GIL_DISABLED))]
            list: FreeListCell::new(PyObjectFreeList::new()),
        }
    }

    #[cfg(not(Py_GIL_DISABLED))]
    fn with<R>(&self, _py: Python<'_>, f: impl FnOnce(&mut PyObjectFreeList<N>) -> R) -> Option<R> {
        cfg_select! {
            any(PyPy, GraalPy, RustPython) => {
                // A busy optimization must not make allocation or destruction wait.
                self.list.try_lock().map(|mut list| f(&mut list))
            }
            _ => {
                // SAFETY: the GIL excludes other accesses. The private callers
                // only transfer pointer ownership; no reference can escape.
                Some(f(unsafe { &mut *self.list.get() }))
            }
        }
    }
}

/// Operations available to the generated per-class allocation hooks.
pub trait FreeListStorage: crate::sealed::Sealed {
    /// Transfer one destroyed allocation out of the cache, if available.
    fn pop(&self, py: Python<'_>) -> Option<NonNull<ffi::PyObject>>;

    /// Return the allocation if the cache is full or busy.
    ///
    /// # Safety
    /// `ptr` must be an exclusively owned, destroyed, untracked allocation for
    /// the class owning this cache. Success transfers ownership to the cache;
    /// the caller must not access it until a later `pop` returns it.
    unsafe fn try_insert(
        &self,
        py: Python<'_>,
        ptr: NonNull<ffi::PyObject>,
    ) -> Option<NonNull<ffi::PyObject>>;
}

impl<const N: usize> crate::sealed::Sealed for FreeList<N> {}

impl<const N: usize> FreeListStorage for FreeList<N> {
    fn pop(&self, py: Python<'_>) -> Option<NonNull<ffi::PyObject>> {
        cfg_select! {
            Py_GIL_DISABLED => {
                let _ = py;
                None
            }
            _ => {
                if N == 0 {
                    None
                } else {
                    self.with(py, |list| list.pop()).flatten()
                }
            }
        }
    }

    unsafe fn try_insert(
        &self,
        py: Python<'_>,
        ptr: NonNull<ffi::PyObject>,
    ) -> Option<NonNull<ffi::PyObject>> {
        cfg_select! {
            Py_GIL_DISABLED => {
                let _ = py;
                Some(ptr)
            }
            _ => {
                if N == 0 {
                    Some(ptr)
                } else {
                    self.with(py, |list| list.insert(ptr)).unwrap_or(Some(ptr))
                }
            }
        }
    }
}

#[cfg(any(not(Py_GIL_DISABLED), test))]
struct PyObjectFreeList<const N: usize> {
    entries: [Option<NonNull<ffi::PyObject>>; N],
    split: usize,
}

// SAFETY: entries are never dereferenced here. Only the initialized prefix owns
// allocations, and insertion/pop transfer that ownership under exclusive access.
#[cfg(any(not(Py_GIL_DISABLED), test))]
unsafe impl<const N: usize> Send for PyObjectFreeList<N> {}

#[cfg(any(not(Py_GIL_DISABLED), test))]
impl<const N: usize> PyObjectFreeList<N> {
    const fn new() -> Self {
        Self {
            entries: [None; N],
            split: 0,
        }
    }

    fn pop(&mut self) -> Option<NonNull<ffi::PyObject>> {
        let idx = self.split;
        if idx == 0 {
            None
        } else {
            self.split = idx - 1;
            // The prefix length is the ownership authority. Clearing the old
            // slot would only duplicate that state and add a hot-path store.
            self.entries[idx - 1]
        }
    }

    fn insert(&mut self, val: NonNull<ffi::PyObject>) -> Option<NonNull<ffi::PyObject>> {
        if self.split < N {
            self.entries[self.split] = Some(val);
            self.split += 1;
            None
        } else {
            Some(val)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_capacity<const N: usize>() {
        let mut allocations = [0_u8; 18];
        let pointers = core::array::from_fn::<_, 18, _>(|i| {
            NonNull::from(&mut allocations[i]).cast::<ffi::PyObject>()
        });
        let mut list = PyObjectFreeList::<N>::new();
        for _ in 0..2 {
            assert_eq!(list.pop(), None);
            for &ptr in &pointers[..N] {
                assert_eq!(list.insert(ptr), None, "capacity {N}");
            }
            assert_eq!(list.insert(pointers[N]), Some(pointers[N]));
            for &ptr in pointers[..N].iter().rev() {
                assert_eq!(list.pop(), Some(ptr));
            }
            assert_eq!(list.pop(), None);
        }
    }

    #[test]
    fn exact_capacity_and_lifo() {
        check_capacity::<0>();
        check_capacity::<1>();
        check_capacity::<2>();
        check_capacity::<17>();
    }

    #[test]
    #[cfg(feature = "macros")]
    #[cfg(all(not(Py_GIL_DISABLED), any(PyPy, GraalPy, RustPython)))]
    #[cfg_attr(miri, ignore = "requires the CPython allocator")]
    fn busy_cache_falls_back() {
        use crate::impl_::pyclass::PyClassWithFreeList;

        #[crate::pyclass(crate = "crate", freelist = 1)]
        struct Cached;

        Python::initialize();
        Python::attach(|py| {
            drop(crate::Bound::new(py, Cached).unwrap());
            let cache = Cached::get_free_list();
            let ptr = cache.pop(py).unwrap();
            let guard = cache.list.lock();
            assert_eq!(cache.pop(py), None);
            // SAFETY: `ptr` was removed from this cache and remains owned here.
            assert_eq!(unsafe { cache.try_insert(py, ptr) }, Some(ptr));
            drop(guard);
            assert_eq!(cache.pop(py), None);
            // SAFETY: the unsuccessful insertion returned ownership of `ptr`.
            assert_eq!(unsafe { cache.try_insert(py, ptr) }, None);
        });
    }
}
