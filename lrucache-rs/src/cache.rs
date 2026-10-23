//! Atomic metadata publication with one immutable, GC-visible reference edge.

use std::num::NonZeroUsize;

use parking_lot::Mutex;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pyclass::{PyTraverseError, PyVisit};
use pyo3::sync::MutexExt as _;
use pyo3::types::{PyInt, PyList, PyTuple};

use crate::errors::CacheError;
use crate::store::Store;

impl From<CacheError> for PyErr {
    fn from(error: CacheError) -> Self {
        PyValueError::new_err(error.to_string())
    }
}

/// Native storage for the public LRU mapping.
#[pyclass(frozen, subclass, mapping, module = "lrucache_rs._lib", name = "Cache")]
pub(crate) struct LRUCache {
    inner: Mutex<Store>,
    references: Py<PyList>,
    maxsize: NonZeroUsize,
}

struct Entry<'py> {
    slot: u32,
    pair: Bound<'py, PyTuple>,
}

#[derive(Clone, Copy)]
enum Action<'a, 'py> {
    Read { touch: bool },
    Set(&'a Bound<'py, PyAny>),
    SetDefault(&'a Bound<'py, PyAny>),
    Remove,
}

impl LRUCache {
    fn entry<'py>(&self, py: Python<'py>, slot: u32) -> Entry<'py> {
        Entry {
            slot,
            pair: self
                .references
                .bind(py)
                .get_item(slot as usize)
                .expect("fixed reference slot")
                .cast_into()
                .expect("occupied reference slot"),
        }
    }

    fn write(&self, py: Python<'_>, slot: u32, pair: &Bound<'_, PyAny>) {
        // The outgoing pair is pinned by the caller until after unlocking.
        self.references
            .bind(py)
            .set_item(slot as usize, pair)
            .expect("fixed reference slot");
    }

    fn access<'py>(
        &self,
        key: &Bound<'py, PyAny>,
        action: Action<'_, 'py>,
    ) -> PyResult<Option<Py<PyTuple>>> {
        let py = key.py();
        let hash = key.hash()?;
        let none = py.None();
        loop {
            let (revision, entries) = {
                let guard = self.inner.lock_py_attached(py);
                let entries = guard
                    .candidates(hash)
                    .map(|slot| self.entry(py, slot))
                    .collect::<Vec<_>>();
                (guard.revision(), entries)
            };
            let mut found = None;
            for entry in entries {
                let stored = entry.pair.get_item(0)?;
                if stored.is(key) || stored.eq(key)? {
                    found = Some(entry);
                    break;
                }
            }
            let updated = match action {
                Action::Set(value) | Action::SetDefault(value)
                    if matches!(action, Action::Set(_)) || found.is_none() =>
                {
                    if let Some(entry) = &found {
                        if entry.pair.get_item(1)?.is(value) {
                            Some(entry.pair.clone())
                        } else {
                            Some(PyTuple::new(py, [entry.pair.get_item(0)?, value.clone()])?)
                        }
                    } else {
                        Some(PyTuple::new(py, [key, value])?)
                    }
                },
                Action::Read { .. } | Action::Set(_) | Action::SetDefault(_) | Action::Remove => {
                    None
                },
            };
            let mut retired = None;
            let mut guard = self.inner.lock_py_attached(py);
            if guard.revision() != revision {
                drop(guard);
                continue;
            }
            let result = if let Some(entry) = &found {
                let slot = entry.slot;
                match action {
                    Action::Read { touch } => {
                        if touch {
                            guard.touch(slot);
                        }
                        Some(entry.pair.clone().unbind())
                    },
                    Action::Set(_) => {
                        let replacement = updated.as_ref().expect("built pair");
                        if replacement.is(&entry.pair) {
                            guard.touch(slot);
                        } else {
                            self.write(py, slot, replacement.as_any());
                            guard.changed_value(slot);
                        }
                        None
                    },
                    Action::SetDefault(_) => {
                        guard.touch(slot);
                        Some(entry.pair.clone().unbind())
                    },
                    Action::Remove => {
                        guard.remove(slot);
                        self.write(py, slot, none.bind(py));
                        Some(entry.pair.clone().unbind())
                    },
                }
            } else {
                match action {
                    Action::Read { .. } | Action::Remove => None,
                    Action::Set(_) | Action::SetDefault(_) => {
                        let insertion = guard.insertion_slot();
                        if guard.is_full() {
                            let slot = guard.lru().expect("full store has tail");
                            retired = Some(self.entry(py, slot));
                            guard.remove(slot);
                        }
                        let slot = guard.insert(hash);
                        debug_assert_eq!(slot, insertion);
                        let pair = updated.as_ref().expect("built pair");
                        self.write(py, slot, pair.as_any());
                        matches!(action, Action::SetDefault(_)).then(|| pair.clone().unbind())
                    },
                }
            };
            drop(guard);
            drop(retired);
            return Ok(result);
        }
    }
}

#[pymethods]
impl LRUCache {
    #[new]
    /// Create native storage for the given positive cache capacity.
    ///
    /// Parameters
    /// ----------
    /// maxsize : int
    ///     Positive maximum entry count within the native index range.
    ///
    /// Returns
    /// -------
    /// Cache
    ///     Initialized native storage for the public Python mapping.
    fn new(py: Python<'_>, maxsize: &Bound<'_, PyInt>) -> PyResult<Self> {
        if maxsize.le(0)? {
            return Err(CacheError::MaxsizeMustBePositive.into());
        }
        if maxsize.gt(crate::store::MAX_CAPACITY)? {
            return Err(CacheError::MaxsizeTooLarge.into());
        }
        let maxsize = NonZeroUsize::new(maxsize.extract()?).expect("positive capacity");
        Ok(Self {
            inner: Mutex::new(py.detach(|| Store::new(maxsize))),
            references: PyList::new(py, std::iter::repeat_n(py.None().bind(py), maxsize.get()))?
                .unbind(),
            maxsize,
        })
    }

    #[expect(clippy::same_name_method, reason = "PyO3 requires the GC slot name")]
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        // Like an immutable tuple, this edge never needs clearing: the mutable
        // list breaks unreachable cycles. Traversal never waits on the mutex,
        // including when another thread holds it while reattaching to Python.
        visit.call(&self.references)
    }

    fn __len__(&self, py: Python<'_>) -> usize {
        self.inner.lock_py_attached(py).len()
    }

    #[pyo3(name = "_get")]
    fn get_entry(&self, key: &Bound<'_, PyAny>) -> PyResult<Option<Py<PyTuple>>> {
        self.access(key, Action::Read { touch: true })
    }

    #[pyo3(name = "_peek")]
    fn peek_entry(&self, key: &Bound<'_, PyAny>) -> PyResult<Option<Py<PyTuple>>> {
        self.access(key, Action::Read { touch: false })
    }

    #[pyo3(name = "_set")]
    fn set_entry<'py>(&self, key: &Bound<'py, PyAny>, value: &Bound<'py, PyAny>) -> PyResult<()> {
        self.access(key, Action::Set(value))?;
        Ok(())
    }

    #[pyo3(name = "_setdefault")]
    fn setdefault_entry<'py>(
        &self,
        key: &Bound<'py, PyAny>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<Py<PyTuple>> {
        Ok(self
            .access(key, Action::SetDefault(value))?
            .expect("setdefault inserts or finds a pair"))
    }

    #[pyo3(name = "_remove")]
    fn remove_entry(&self, key: &Bound<'_, PyAny>) -> PyResult<Option<Py<PyTuple>>> {
        self.access(key, Action::Remove)
    }

    /// Remove all entries before running any retired key or value finalizers.
    ///
    /// Returns
    /// -------
    /// None
    ///     Finalizers may insert new entries after the clear commits.
    fn clear(&self, py: Python<'_>) {
        let mut guard = self.inner.lock_py_attached(py);
        let retired = guard
            .indices()
            .into_iter()
            .map(|slot| self.entry(py, slot))
            .collect::<Vec<_>>();
        guard.clear();
        let none = py.None();
        for entry in &retired {
            self.write(py, entry.slot, none.bind(py));
        }
        drop(guard);
        drop(retired);
    }

    #[pyo3(name = "_popitem")]
    fn pop_entry(&self, py: Python<'_>) -> Option<Py<PyTuple>> {
        let mut guard = self.inner.lock_py_attached(py);
        let slot = guard.lru()?;
        let entry = self.entry(py, slot);
        guard.remove(slot);
        self.write(py, slot, py.None().bind(py));
        drop(guard);
        Some(entry.pair.unbind())
    }

    #[pyo3(name = "_snapshot_entries")]
    fn snapshot_entries(&self, py: Python<'_>) -> Vec<Py<PyTuple>> {
        let guard = self.inner.lock_py_attached(py);
        guard
            .indices()
            .into_iter()
            .map(|slot| self.entry(py, slot).pair.unbind())
            .collect()
    }

    #[getter]
    /// The configured maximum number of entries.
    ///
    /// Returns
    /// -------
    /// int
    ///     The positive entry capacity.
    const fn maxsize(&self) -> usize {
        self.maxsize.get()
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        let len = self.inner.lock_py_attached(py).len();
        format!("LRUCache(maxsize={}, currsize={len})", self.maxsize.get())
    }
}
