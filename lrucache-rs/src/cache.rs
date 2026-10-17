//! Native metadata and atomic publication of Python-owned entry references.

use std::num::NonZeroUsize;

use parking_lot::Mutex;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::MutexExt as _;
use pyo3::types::{PyInt, PyList};

use crate::errors::CacheError;
use crate::store::Store;

/// Native storage for the public Python-owned LRU mapping.
#[pyclass(frozen, subclass, mapping, module = "lrucache_rs._lib", name = "Cache")]
pub(crate) struct LRUCache {
    inner: Mutex<Store>,
    maxsize: NonZeroUsize,
}

struct Position<'py> {
    slot: u32,
    index: Bound<'py, PyInt>,
}

impl<'py> Position<'py> {
    fn new(py: Python<'py>, slot: u32) -> Self {
        Self {
            slot,
            index: slot.into_pyobject(py).expect("integer conversion"),
        }
    }

    fn read(self, references: &Bound<'py, PyList>) -> PyResult<Entry<'py>> {
        // PyList_GetItem would switch PyPy to opaque cpyext raw-reference storage.
        let owner = references.as_any().get_item(&self.index)?.unbind();
        Ok(Entry {
            position: self,
            owner,
        })
    }

    fn write(&self, references: &Bound<'_, PyList>, owner: &Bound<'_, PyAny>) {
        // The exact list, exact in-range int and Entry/None already have Python
        // objects and C wrappers. Generic assignment preserves ObjectListStrategy
        // on PyPy: its built-in shortcut unwraps the int and replaces one pointer,
        // without allocation, resize or Python dispatch. Do not use PyList_SetItem
        // or move object/index conversion under the mutex. Every outgoing owner
        // has a retirement pin, preventing last-reference finalizers on CPython.
        references
            .as_any()
            .set_item(&self.index, owner)
            .expect("fixed entry slot");
    }
}

struct Entry<'py> {
    position: Position<'py>,
    owner: Py<PyAny>,
}

#[derive(Clone, Copy)]
enum Action {
    Read { touch: bool },
    Set,
    SetDefault,
    Remove,
}

fn failed(request: &Bound<'_, PyAny>) -> PyResult<bool> {
    // Fetching the exception or payload itself would expose recursive tuples
    // to cpyext. Only the controlled request's Boolean crosses this boundary.
    request.getattr("failed")?.is_truthy()
}

impl LRUCache {
    fn references<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyList>> {
        slf.getattr("_references")?.cast_into().map_err(Into::into)
    }

    fn access<'py>(
        &self,
        references: &Bound<'py, PyList>,
        request: &Bound<'py, PyAny>,
        action: Action,
    ) -> PyResult<Option<Py<PyAny>>> {
        let py = request.py();
        let hash = request.hash()?;
        if failed(request)? {
            return Ok(None);
        }
        let none = py.None();
        loop {
            let inserting = matches!(action, Action::Set | Action::SetDefault);
            let (revision, candidates, insertion, victim) = {
                let guard = self.inner.lock_py_attached(py);
                (
                    guard.revision(),
                    guard.candidates(hash),
                    guard.insertion_slot(),
                    (inserting && guard.is_full())
                        .then(|| guard.lru().expect("full store has tail")),
                )
            };
            let insertion = Position::new(py, insertion);
            let victim = victim
                .map(|idx| Position::new(py, idx).read(references))
                .transpose()?;
            let entries = candidates
                .into_iter()
                .map(|slot| Position::new(py, slot).read(references))
                .collect::<PyResult<Vec<_>>>()?;
            let guard = self.inner.lock_py_attached(py);
            let valid = guard.revision() == revision;
            drop(guard);
            if !valid {
                continue;
            }
            let mut found = None;
            for entry in entries {
                let matched = eq_entries(&entry.owner, request)?;
                if failed(request)? {
                    return Ok(None);
                }
                if matched {
                    found = Some(entry);
                    break;
                }
            }
            let updated = if matches!(action, Action::Set)
                || (matches!(action, Action::SetDefault) && found.is_none())
            {
                let canonical = found
                    .as_ref()
                    .map_or_else(|| none.bind(py), |entry| entry.owner.bind(py));
                let owner = request.call_method1("build_entry", (canonical,))?.unbind();
                if failed(request)? {
                    return Ok(None);
                }
                Some(owner)
            } else {
                None
            };
            let mut guard = self.inner.lock_py_attached(py);
            if guard.revision() != revision {
                drop(guard);
                continue;
            }
            let result = if let Some(entry) = &found {
                let slot = entry.position.slot;
                match action {
                    Action::Read { touch } => {
                        if touch {
                            guard.touch(slot);
                        }
                        Some(entry.owner.clone_ref(py))
                    },
                    Action::Set => {
                        let replacement = updated.as_ref().expect("built entry").bind(py);
                        // Idempotent equality-callback writes must not invalidate
                        // their own enclosing lookup's content revision forever.
                        if replacement.is(&entry.owner) {
                            guard.touch(slot);
                        } else {
                            entry.position.write(references, replacement);
                            guard.changed_value(slot);
                        }
                        None
                    },
                    Action::SetDefault => {
                        guard.touch(slot);
                        Some(entry.owner.clone_ref(py))
                    },
                    Action::Remove => {
                        guard.remove(slot);
                        entry.position.write(references, none.bind(py));
                        Some(entry.owner.clone_ref(py))
                    },
                }
            } else {
                match action {
                    Action::Read { .. } | Action::Remove => None,
                    Action::Set | Action::SetDefault => {
                        // Reads can change the tail without changing membership.
                        if guard.insertion_slot() != insertion.slot {
                            drop(guard);
                            continue;
                        }
                        if let Some(entry) = &victim {
                            guard.remove(entry.position.slot);
                        }
                        let slot = guard.insert(hash);
                        debug_assert_eq!(slot, insertion.slot);
                        let owner = updated.as_ref().expect("built entry");
                        insertion.write(references, owner.bind(py));
                        matches!(action, Action::SetDefault).then(|| owner.clone_ref(py))
                    },
                }
            };
            drop(guard);
            return Ok(result);
        }
    }
}

#[pymethods]
impl LRUCache {
    #[new]
    /// Create native metadata for the given positive cache capacity.
    ///
    /// Parameters
    /// ----------
    /// maxsize : int
    ///     Positive maximum entry count within the native index range.
    ///
    /// Returns
    /// -------
    /// Cache
    ///     Initialized native metadata for the public Python mapping.
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

    #[pyo3(name = "_get")]
    fn get_entry(slf: &Bound<'_, Self>, request: &Bound<'_, PyAny>) -> PyResult<Option<Py<PyAny>>> {
        slf.get()
            .access(&Self::references(slf)?, request, Action::Read {
                touch: true,
            })
    }

    #[pyo3(name = "_peek")]
    fn peek_entry(
        slf: &Bound<'_, Self>,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Py<PyAny>>> {
        slf.get()
            .access(&Self::references(slf)?, request, Action::Read {
                touch: false,
            })
    }

    #[pyo3(name = "_set")]
    fn set_entry(slf: &Bound<'_, Self>, request: &Bound<'_, PyAny>) -> PyResult<()> {
        slf.get()
            .access(&Self::references(slf)?, request, Action::Set)?;
        Ok(())
    }

    #[pyo3(name = "_setdefault")]
    fn setdefault_entry(
        slf: &Bound<'_, Self>,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Py<PyAny>>> {
        slf.get()
            .access(&Self::references(slf)?, request, Action::SetDefault)
    }

    #[pyo3(name = "_remove")]
    fn remove_entry(
        slf: &Bound<'_, Self>,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Py<PyAny>>> {
        slf.get()
            .access(&Self::references(slf)?, request, Action::Remove)
    }

    /// Remove all entries before running any retired key or value finalizers.
    ///
    /// Returns
    /// -------
    /// None
    ///     Finalizers may insert new entries after the clear commits.
    fn clear(slf: &Bound<'_, Self>) -> PyResult<()> {
        let references = Self::references(slf)?;
        let py = slf.py();
        let none = py.None();
        loop {
            let (revision, indices) = {
                let guard = slf.get().inner.lock_py_attached(py);
                (guard.revision(), guard.indices())
            };
            let retired = indices
                .into_iter()
                .map(|slot| Position::new(py, slot).read(&references))
                .collect::<PyResult<Vec<_>>>()?;
            let mut guard = slf.get().inner.lock_py_attached(py);
            if guard.revision() != revision {
                drop(guard);
                continue;
            }
            guard.clear();
            for entry in &retired {
                entry.position.write(&references, none.bind(py));
            }
            drop(guard);
            return Ok(());
        }
    }

    #[pyo3(name = "_popitem")]
    fn pop_entry(slf: &Bound<'_, Self>) -> PyResult<Option<Py<PyAny>>> {
        let references = Self::references(slf)?;
        let py = slf.py();
        let none = py.None();
        loop {
            let (revision, tail) = {
                let guard = slf.get().inner.lock_py_attached(py);
                (guard.revision(), guard.lru())
            };
            let Some(tail) = tail else { return Ok(None) };
            let entry = Position::new(py, tail).read(&references)?;
            let mut guard = slf.get().inner.lock_py_attached(py);
            if guard.revision() != revision || guard.lru() != Some(tail) {
                drop(guard);
                continue;
            }
            guard.remove(tail);
            entry.position.write(&references, none.bind(py));
            drop(guard);
            return Ok(Some(entry.owner));
        }
    }

    #[pyo3(name = "_snapshot_entries")]
    fn snapshot_entries(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        loop {
            let (revision, indices) = {
                let guard = slf.get().inner.lock_py_attached(py);
                (guard.revision(), guard.indices())
            };
            // Only integer indices enter a native-created container. The
            // recursive entry graph stays in a Python-created snapshot list.
            let entries = slf.call_method1("_capture", (indices,))?.unbind();
            let guard = slf.get().inner.lock_py_attached(py);
            let valid = guard.revision() == revision;
            drop(guard);
            if valid {
                return Ok(entries);
            }
        }
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

fn eq_entries(stored: &Py<PyAny>, request: &Bound<'_, PyAny>) -> PyResult<bool> {
    // SAFETY: both ordinary Python envelope objects have independent strong
    // references and this thread is attached. Their comparison transports
    // provider errors in Python slots and returns only a bool, outside locking.
    let cmp =
        unsafe { ffi::PyObject_RichCompareBool(stored.as_ptr(), request.as_ptr(), ffi::Py_EQ) };
    if cmp < 0 {
        Err(PyErr::fetch(request.py()))
    } else {
        Ok(cmp == 1)
    }
}
