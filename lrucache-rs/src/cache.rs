//! Native operations over Python-owned references and a Python-free slab.
//!
//! All allocating reference reads and arbitrary key equality run outside the
//! mutex. A revision validates their snapshots before commitment. Under the
//! lock, exact-list assignments only replace already-realized objects at
//! existing integer positions; every outgoing object has a retirement pin.

use std::num::NonZeroUsize;

use parking_lot::Mutex;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::MutexExt as _;
use pyo3::types::{PyInt, PyList, PyTuple, PyType};

use crate::errors::{CacheError, missing_key};
use crate::store::Store;

/// Native storage for the public Python-owned LRU mapping.
#[pyclass(frozen, subclass, mapping, module = "lrucache_rs._lib", name = "Cache")]
pub struct LRUCache {
    inner: Mutex<Store>,
    maxsize: NonZeroUsize,
}

struct Position<'py> {
    slot: u32,
    key: Bound<'py, PyInt>,
    value: Bound<'py, PyInt>,
}

impl<'py> Position<'py> {
    fn new(py: Python<'py>, slot: u32) -> Self {
        let key = (slot as usize).wrapping_mul(2);
        Self {
            slot,
            key: key.into_pyobject(py).expect("integer conversion"),
            value: key
                .wrapping_add(1)
                .into_pyobject(py)
                .expect("integer conversion"),
        }
    }

    fn read(self, references: &Bound<'py, PyList>) -> PyResult<Entry<'py>> {
        // Generic indexing preserves PyPy's Python-managed list strategy.
        // PyList_GetItem would convert it to opaque cpyext raw-reference storage.
        let key = references.as_any().get_item(&self.key)?.unbind();
        let value = references.as_any().get_item(&self.value)?.unbind();
        Ok(Entry {
            position: self,
            key,
            value,
        })
    }

    fn write(
        &self,
        references: &Bound<'_, PyList>,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) {
        // Exact list + exact, in-range int indices do not dispatch user code.
        // Both incoming wrappers exist already; outgoing wrappers are pinned
        // by Entry until after unlocking. No resize or last-reference drop.
        references
            .as_any()
            .set_item(&self.key, key)
            .expect("fixed key slot");
        references
            .as_any()
            .set_item(&self.value, value)
            .expect("fixed value slot");
    }
}

struct Entry<'py> {
    position: Position<'py>,
    key: Py<PyAny>,
    value: Py<PyAny>,
}

#[derive(Clone, Copy)]
enum Action<'a, 'py> {
    Read { touch: bool },
    Contains,
    Set(&'a Bound<'py, PyAny>),
    SetDefault(&'a Bound<'py, PyAny>),
    Remove,
}

impl LRUCache {
    fn references<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyList>> {
        slf.getattr("_references")?.cast_into().map_err(Into::into)
    }

    fn access<'py>(
        &self,
        references: &Bound<'py, PyList>,
        key: &Bound<'py, PyAny>,
        action: Action<'_, 'py>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let py = key.py();
        let hash = key.hash()?;
        let none = py.None();
        loop {
            let inserting = matches!(action, Action::Set(_) | Action::SetDefault(_));
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
                if eq_keys(&entry.key, key)? {
                    found = Some(entry);
                    break;
                }
            }
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
                        Some(entry.value.clone_ref(py))
                    },
                    Action::Contains => Some(entry.value.clone_ref(py)),
                    Action::Set(value) => {
                        entry.position.write(references, entry.key.bind(py), value);
                        guard.changed_value(slot);
                        None
                    },
                    Action::SetDefault(_) => {
                        guard.touch(slot);
                        Some(entry.value.clone_ref(py))
                    },
                    Action::Remove => {
                        guard.remove(slot);
                        entry
                            .position
                            .write(references, none.bind(py), none.bind(py));
                        Some(entry.value.clone_ref(py))
                    },
                }
            } else {
                match action {
                    Action::Read { .. } | Action::Contains | Action::Remove => None,
                    Action::Set(value) | Action::SetDefault(value) => {
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
                        insertion.write(references, key, value);
                        matches!(action, Action::SetDefault(_)).then(|| value.clone().unbind())
                    },
                }
            };
            drop(guard);
            return Ok(result);
        }
    }

    fn snapshot<'py>(&self, references: &Bound<'py, PyList>) -> PyResult<Vec<Entry<'py>>> {
        let py = references.py();
        loop {
            let (revision, indices) = {
                let guard = self.inner.lock_py_attached(py);
                (guard.revision(), guard.indices())
            };
            let entries = indices
                .into_iter()
                .map(|slot| Position::new(py, slot).read(references))
                .collect::<PyResult<Vec<_>>>()?;
            let guard = self.inner.lock_py_attached(py);
            let valid = guard.revision() == revision;
            drop(guard);
            if valid {
                return Ok(entries);
            }
        }
    }
}

#[pymethods]
impl LRUCache {
    #[classmethod]
    fn __class_getitem__(cls: &Bound<'_, PyType>, _item: &Bound<'_, PyAny>) -> Py<PyType> {
        cls.clone().unbind()
    }

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

    fn __contains__(slf: &Bound<'_, Self>, key: &Bound<'_, PyAny>) -> PyResult<bool> {
        Ok(slf
            .get()
            .access(&Self::references(slf)?, key, Action::Contains)?
            .is_some())
    }

    fn __getitem__<'py>(slf: &Bound<'py, Self>, key: &Bound<'py, PyAny>) -> PyResult<Py<PyAny>> {
        slf.get()
            .access(&Self::references(slf)?, key, Action::Read { touch: true })?
            .ok_or_else(|| missing_key(key))
    }

    fn __setitem__(
        slf: &Bound<'_, Self>,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        slf.get()
            .access(&Self::references(slf)?, key, Action::Set(value))?;
        Ok(())
    }

    fn __delitem__(slf: &Bound<'_, Self>, key: &Bound<'_, PyAny>) -> PyResult<()> {
        slf.get()
            .access(&Self::references(slf)?, key, Action::Remove)?
            .map(|_| ())
            .ok_or_else(|| missing_key(key))
    }

    fn __iter__<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let entries = slf.get().snapshot(&Self::references(slf)?)?;
        let tuple = PyTuple::new(slf.py(), entries.into_iter().map(|entry| entry.key))?;
        Ok(tuple.try_iter()?.into_any())
    }

    /// Retrieve a value and mark a present key most recently used.
    ///
    /// Parameters
    /// ----------
    /// key : Hashable
    ///     Key to look up.
    /// default : object, optional
    ///     Value returned when the key is absent.
    ///
    /// Returns
    /// -------
    /// object
    ///     The identical stored value, or the default when absent.
    #[pyo3(signature = (key, /, default=None))]
    fn get(
        slf: &Bound<'_, Self>,
        key: &Bound<'_, PyAny>,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        Ok(slf
            .get()
            .access(&Self::references(slf)?, key, Action::Read { touch: true })?
            .unwrap_or_else(|| default.unwrap_or_else(|| slf.py().None())))
    }

    /// Retrieve a value without changing recency.
    ///
    /// Parameters
    /// ----------
    /// key : Hashable
    ///     Key to look up.
    /// default : object, optional
    ///     Value returned when the key is absent.
    ///
    /// Returns
    /// -------
    /// object
    ///     The identical stored value, or the default when absent.
    #[pyo3(signature = (key, /, default=None))]
    fn peek(
        slf: &Bound<'_, Self>,
        key: &Bound<'_, PyAny>,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        Ok(slf
            .get()
            .access(&Self::references(slf)?, key, Action::Read { touch: false })?
            .unwrap_or_else(|| default.unwrap_or_else(|| slf.py().None())))
    }

    /// Remove a key atomically, returning its value or the supplied default.
    ///
    /// Parameters
    /// ----------
    /// key : Hashable
    ///     Key to remove.
    /// default : object, optional
    ///     Value returned when absent. If omitted, absence raises KeyError.
    ///
    /// Returns
    /// -------
    /// object
    ///     The removed value, or the supplied default when absent.
    #[pyo3(signature = (key, /, *default))]
    fn pop(
        slf: &Bound<'_, Self>,
        key: &Bound<'_, PyAny>,
        default: &Bound<'_, PyTuple>,
    ) -> PyResult<Py<PyAny>> {
        if default.len() > 1 {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "pop accepts at most one default",
            ));
        }
        let result = slf
            .get()
            .access(&Self::references(slf)?, key, Action::Remove)?;
        if let Some(value) = result {
            return Ok(value);
        }
        if default.is_empty() {
            Err(missing_key(key))
        } else {
            Ok(default.get_item(0)?.unbind())
        }
    }

    /// Return and touch an existing value, or atomically insert the default.
    ///
    /// Parameters
    /// ----------
    /// key : Hashable
    ///     Key to look up or insert.
    /// default : object, optional
    ///     Value inserted when absent; None when omitted.
    ///
    /// Returns
    /// -------
    /// object
    ///     The identical existing value or inserted default.
    #[pyo3(signature = (key, /, default=None))]
    fn setdefault(
        slf: &Bound<'_, Self>,
        key: &Bound<'_, PyAny>,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let default = default.unwrap_or_else(|| slf.py().None());
        Ok(slf
            .get()
            .access(
                &Self::references(slf)?,
                key,
                Action::SetDefault(default.bind(slf.py())),
            )?
            .expect("setdefault returns an existing or inserted value"))
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
                entry
                    .position
                    .write(&references, none.bind(py), none.bind(py));
            }
            drop(guard);
            return Ok(());
        }
    }

    /// Remove and return the least-recently-used pair, or raise KeyError.
    ///
    /// Returns
    /// -------
    /// tuple
    ///     The removed key and value with their identities preserved.
    fn popitem(slf: &Bound<'_, Self>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let references = Self::references(slf)?;
        let py = slf.py();
        let none = py.None();
        loop {
            let (revision, tail) = {
                let guard = slf.get().inner.lock_py_attached(py);
                (guard.revision(), guard.lru())
            };
            let tail = tail.ok_or_else(|| CacheError::PopitemEmpty.into_pyerr())?;
            let entry = Position::new(py, tail).read(&references)?;
            let mut guard = slf.get().inner.lock_py_attached(py);
            if guard.revision() != revision || guard.lru() != Some(tail) {
                drop(guard);
                continue;
            }
            guard.remove(tail);
            entry
                .position
                .write(&references, none.bind(py), none.bind(py));
            drop(guard);
            return Ok((entry.key, entry.value));
        }
    }

    #[pyo3(name = "_snapshot_items")]
    fn snapshot_items(slf: &Bound<'_, Self>) -> PyResult<Vec<(Py<PyAny>, Py<PyAny>)>> {
        Ok(slf
            .get()
            .snapshot(&Self::references(slf)?)?
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect())
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

fn eq_keys(stored: &Py<PyAny>, lookup: &Bound<'_, PyAny>) -> PyResult<bool> {
    if lookup.is(stored) {
        return Ok(true);
    }
    // SAFETY: both objects have independent strong references and this thread
    // is attached. The comparison and result destruction run without a lock.
    let cmp =
        unsafe { ffi::PyObject_RichCompareBool(stored.as_ptr(), lookup.as_ptr(), ffi::Py_EQ) };
    if cmp < 0 {
        Err(PyErr::fetch(lookup.py()))
    } else {
        Ok(cmp == 1)
    }
}
