//! `LRUCache` pyclass: a dict-like LRU cache backed by [`crate::store::Store`].
//!
//! ## Concurrency model
//!
//! The class is `frozen`, so `PyO3` methods take `&self` and synchronisation
//! lives in a single [`parking_lot::Mutex`] over the inner store. On the
//! GIL-enabled build the GIL serialises Python code, so contention is rare. On
//! the free-threaded build, multiple Rust threads can call methods concurrently
//! and the mutex is the source of truth.
//!
//! [`MutexExt::lock_py_attached`] is used so that, when waiting on a contended
//! mutex, the calling thread temporarily detaches from the Python interpreter;
//! this avoids the classic GIL/mutex interleaving deadlock when another thread
//! holds the mutex while waiting on the GC.
//!
//! ## Reentrancy contract
//!
//! User-supplied `__hash__` runs *before* the lock is acquired, so it is free
//! to do anything. A user-supplied `__eq__` runs from inside the lookup closure
//! while the lock is held; it must not recurse into this same cache instance,
//! which would self-deadlock the thread. This matches the contract of `dict` in
//! `CPython` and is documented in the public API.
//!
//! ## Hash protocol
//!
//! `__hash__` is invoked exactly once per call (via `PyObject_Hash`). The
//! resulting `Py_hash_t` is stored in the entry alongside the key, so
//! subsequent lookups for the same stored key never re-hash. Collision handling
//! uses `PyObject_RichCompareBool(_, _, Py_EQ)`: it has `CPython`'s
//! pointer-equality fast path built in, so equal Python object pointers
//! short-circuit before the Python-level `__eq__` is ever called.

use std::num::NonZeroUsize;

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::{PyAny, PyTuple, PyType};
use pyo3::{Py, ffi};

use crate::errors::{CacheError, missing_key};
use crate::store::Store;

#[pyclass(frozen, module = "lrucache_rs._lib")]
pub struct LRUCache {
    inner: Mutex<Store>,
    /// Stored alongside `inner` so it can be read without taking the lock.
    maxsize: NonZeroUsize,
}

#[pymethods]
impl LRUCache {
    /// Subscriptable type hints: `LRUCache[K, V]` is just `LRUCache` at
    /// runtime.
    #[classmethod]
    fn __class_getitem__(cls: &Bound<'_, PyType>, _item: &Bound<'_, PyAny>) -> Py<PyType> {
        cls.clone().unbind()
    }

    #[new]
    fn new(maxsize: usize) -> PyResult<Self> {
        let maxsize = NonZeroUsize::new(maxsize)
            .ok_or_else(|| CacheError::MaxsizeMustBePositive.into_pyerr())?;
        if maxsize.get() > crate::store::MAX_CAPACITY {
            return Err(CacheError::MaxsizeTooLarge.into_pyerr());
        }
        Ok(Self {
            inner: Mutex::new(Store::new(maxsize)),
            maxsize,
        })
    }

    fn __len__(&self, py: Python<'_>) -> usize {
        self.inner.lock_py_attached(py).len()
    }

    fn __contains__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<bool> {
        let hash = key.hash()?;
        let guard = self.inner.lock_py_attached(py);
        guard.contains(hash, |stored| eq_keys(stored, key))
    }

    fn __getitem__<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let hash = key.hash()?;
        let mut guard = self.inner.lock_py_attached(py);
        let value = guard.touch_get(py, hash, |stored| eq_keys(stored, key))?;
        drop(guard);
        let Some(value) = value else {
            return Err(missing_key(key));
        };
        Ok(value.into_bound(py))
    }

    fn __setitem__(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        value: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let hash = key.hash()?;
        let mut guard = self.inner.lock_py_attached(py);
        let displaced = guard.put(hash, key.clone().unbind(), value.unbind(), |stored| {
            eq_keys(stored, key)
        })?;
        // Release the lock before the displaced Py<...> values drop, since their
        // finalizers can run arbitrary Python code.
        drop(guard);
        drop(displaced);
        Ok(())
    }

    fn __delitem__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<()> {
        let hash = key.hash()?;
        let mut guard = self.inner.lock_py_attached(py);
        let removed = guard.remove(hash, |stored| eq_keys(stored, key))?;
        drop(guard);
        if removed.is_some() {
            Ok(())
        } else {
            Err(missing_key(key))
        }
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Snapshot keys under the lock, then build a tuple iterator after releasing it.
        // The tuple takes independent strong references, so subsequent cache
        // mutations cannot invalidate the returned iterator.
        let guard = self.inner.lock_py_attached(py);
        let keys: Vec<Py<PyAny>> = guard.iter_keys().map(|k| k.clone_ref(py)).collect();
        drop(guard);
        let tuple = PyTuple::new(py, keys)?;
        Ok(tuple.try_iter()?.into_any())
    }

    #[pyo3(signature = (key, /, default=None))]
    fn get(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let hash = key.hash()?;
        let mut guard = self.inner.lock_py_attached(py);
        Ok(guard
            .touch_get(py, hash, |stored| eq_keys(stored, key))?
            .unwrap_or_else(|| default.unwrap_or_else(|| py.None())))
    }

    /// Read without bumping recency. Useful for instrumentation that should not
    /// perturb the LRU order; the convention is borrowed from `cachetools`.
    #[pyo3(signature = (key, /, default=None))]
    fn peek(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let hash = key.hash()?;
        let guard = self.inner.lock_py_attached(py);
        Ok(guard
            .peek_get(py, hash, |stored| eq_keys(stored, key))?
            .unwrap_or_else(|| default.unwrap_or_else(|| py.None())))
    }

    fn clear(&self, py: Python<'_>) {
        self.inner.lock_py_attached(py).clear(py);
    }

    /// Remove and return the least-recently-used `(key, value)` pair, raising
    /// `KeyError` if empty.
    fn popitem<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let mut guard = self.inner.lock_py_attached(py);
        let popped = guard.pop_lru();
        drop(guard);
        let Some(pair) = popped else {
            return Err(CacheError::PopitemEmpty.into_pyerr());
        };
        pair.into_pyobject(py)
    }

    #[getter]
    const fn maxsize(&self) -> usize {
        self.maxsize.get()
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        let len = self.inner.lock_py_attached(py).len();
        format!("LRUCache(maxsize={}, currsize={len})", self.maxsize.get())
    }
}

/// Compare a stored key against the lookup key.
///
/// We short-circuit on pointer equality before crossing the FFI boundary; this
/// is the typical case for interned integers, interned strings, and any caller
/// that reuses the same Python object as a key. `PyObject_RichCompareBool` does
/// the same identity check internally, but avoiding the call saves the
/// function-call overhead and lets the linker keep the cold path
/// (`__eq__` invocation) out of the icache for the common case.
///
/// The remaining FFI is `PyObject_RichCompareBool`, called directly because
/// pyo3's safe `eq()` would route through `PyObject_RichCompare` plus a
/// separate `PyObject_IsTrue` (two FFI calls and an intermediate `PyObject`
/// allocation) instead of one. See pyo3 issue #985 for why `Bound::eq`
/// deliberately avoids the `RichCompareBool` shape.
fn eq_keys(stored: &Py<PyAny>, lookup: &Bound<'_, PyAny>) -> PyResult<bool> {
    if lookup.is(stored) {
        return Ok(true);
    }
    // SAFETY: both pointers refer to live Python objects under a held GIL/attached
    // state; `lookup` is a `Bound<...>`, and `stored` is owned by the store
    // which only mutates while attached. PyObject_RichCompareBool returns -1
    // with the error indicator set on failure.
    let cmp =
        unsafe { ffi::PyObject_RichCompareBool(stored.as_ptr(), lookup.as_ptr(), ffi::Py_EQ) };
    if cmp < 0 {
        Err(PyErr::fetch(lookup.py()))
    } else {
        Ok(cmp == 1)
    }
}
