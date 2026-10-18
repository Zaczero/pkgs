//! Thread-local caches of already-built Python containers for static PROJ
//! catalog reads (`crs_units`, `crs_celestial_bodies`) and rich CRS dict
//! materializations (`crs_operation`, `crs_operations`).
//!
//! Rust-side LRUs still own the PROJ DB work; this layer stores the
//! IntoPyObject materialization so warm hits return an isolation-safe copy
//! instead of re-building nested dicts. Entries are stamped with
//! [`crate::crs::RuntimeState::generation`] and dropped on generation bump
//! (`crs_clear_cache` / runtime config changes) — same lifetime as the CRS
//! thread-local LRUs.
//!
//! Isolation contract: nested dicts are frozen as `MappingProxyType` and nested
//! lists as tuples inside those proxies, so in-place leaf mutation cannot poison
//! the cache. Top-level lists stay real lists. Authority lists are shallow-copied
//! on return because their elements are immutable strings; record lists copy each
//! top-level dict. Top-level dicts are shallow-copied.

use std::cell::RefCell;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::crs;

const UNITS_PY_CAPACITY: usize = 16;
const BODIES_PY_CAPACITY: usize = 8;
const AUTHORITIES_PY_CAPACITY: usize = 4;
const OPERATION_PY_CAPACITY: usize = 32;
const OPERATIONS_PY_CAPACITY: usize = 32;

struct UnitsPyEntry {
    generation: u64,
    authority: String,
    category: Option<String>,
    allow_deprecated: bool,
    list: Py<PyList>,
}

struct BodiesPyEntry {
    generation: u64,
    authority: Option<String>,
    list: Py<PyList>,
}

struct AuthoritiesPyEntry {
    generation: u64,
    list: Py<PyList>,
}

struct OperationPyEntry {
    generation: u64,
    source: String,
    target: String,
    options: crs::TransformOptions,
    dict: Py<PyDict>,
}

struct OperationsPyEntry {
    generation: u64,
    source: String,
    target: String,
    options: crs::TransformOptions,
    list: Py<PyList>,
}

thread_local! {
    static UNITS_PY_CACHE: RefCell<Vec<UnitsPyEntry>> = const { RefCell::new(Vec::new()) };
    static BODIES_PY_CACHE: RefCell<Vec<BodiesPyEntry>> = const { RefCell::new(Vec::new()) };
    static AUTHORITIES_PY_CACHE: RefCell<Vec<AuthoritiesPyEntry>> =
        const { RefCell::new(Vec::new()) };
    static OPERATION_PY_CACHE: RefCell<Vec<OperationPyEntry>> = const { RefCell::new(Vec::new()) };
    static OPERATIONS_PY_CACHE: RefCell<Vec<OperationsPyEntry>> = const { RefCell::new(Vec::new()) };
}

/// Return a shallow copy of a cached authority list.
fn shallow_copy_list<'py>(py: Python<'py>, list: &Py<PyList>) -> PyResult<Bound<'py, PyList>> {
    // list.copy() is a shallow copy: new list, same element objects — O(n)
    // pointer writes, not re-IntoPyObject. Matches the public "fresh list"
    // contract without letting callers mutate the cached list in place.
    list.bind(py)
        .call_method0("copy")?
        .extract::<Bound<'py, PyList>>()
        .map_err(PyErr::from)
}

/// Generation-stamped, error-aware list/dict resolve shaped like
/// [`crs::lru_resolve`]: drop stale entries, move-to-back on hit, build on miss.
fn py_lru_resolve<E, R>(
    cache: &RefCell<Vec<E>>,
    capacity: usize,
    generation: u64,
    entry_generation: impl Fn(&E) -> u64,
    matches: impl Fn(&E) -> bool,
    copy_out: impl FnOnce(&E) -> PyResult<R>,
    make: impl FnOnce() -> PyResult<E>,
) -> PyResult<R> {
    let (entry, stale) = {
        let mut cache = cache.borrow_mut();
        let stale = cache
            .extract_if(.., |entry| entry_generation(entry) != generation)
            .collect::<Vec<_>>();
        let entry = cache
            .iter()
            .position(&matches)
            .map(|index| cache.remove(index));
        (entry, stale)
    };
    drop(stale);
    // Python construction and copying can reenter this thread-local cache.
    let entry = match entry {
        Some(entry) => entry,
        None => make()?,
    };
    let out = copy_out(&entry)?;
    let retired = {
        let mut cache = cache.borrow_mut();
        let mut retired = cache
            .extract_if(.., |entry| matches(entry))
            .collect::<Vec<_>>();
        if cache.len() >= capacity {
            retired.push(cache.remove(0));
        }
        cache.push(entry);
        retired
    };
    drop(retired);
    Ok(out)
}

/// Freeze a nested value: dicts → MappingProxyType, lists → tuples, leaves shared.
fn freeze_nested<'py>(py: Python<'py>, value: &Bound<'py, PyAny>) -> PyResult<Py<PyAny>> {
    if let Ok(dict) = value.cast::<PyDict>() {
        let inner = PyDict::new(py);
        for (key, item) in dict.iter() {
            inner.set_item(key, freeze_nested(py, &item)?)?;
        }
        let proxy = py
            .import("types")?
            .getattr("MappingProxyType")?
            .call1((inner,))?;
        return Ok(proxy.unbind());
    }
    if let Ok(list) = value.cast::<PyList>() {
        let mut items = Vec::with_capacity(list.len());
        for item in list.iter() {
            items.push(freeze_nested(py, &item)?);
        }
        return Ok(PyTuple::new(py, items)?.into_any().unbind());
    }
    Ok(value.clone().unbind())
}

/// Freeze a top-level dict for caching: nested containers frozen, top-level
/// lists kept as real lists of frozen elements (so `== []` and list.copy work).
pub(crate) fn freeze_top_dict<'py>(
    py: Python<'py>,
    dict: &Bound<'py, PyDict>,
) -> PyResult<Py<PyDict>> {
    let out = PyDict::new(py);
    for (key, value) in dict.iter() {
        if let Ok(list) = value.cast::<PyList>() {
            let frozen_list = PyList::empty(py);
            for item in list.iter() {
                frozen_list.append(freeze_nested(py, &item)?)?;
            }
            out.set_item(key, frozen_list)?;
        } else if value.cast::<PyDict>().is_ok() {
            out.set_item(key, freeze_nested(py, &value)?)?;
        } else {
            out.set_item(key, &value)?;
        }
    }
    Ok(out.unbind())
}

/// Isolation-safe return copy of a cached top-level dict.
pub(crate) fn isolation_copy_dict<'py>(
    py: Python<'py>,
    cached: &Py<PyDict>,
) -> PyResult<Bound<'py, PyDict>> {
    let src = cached.bind(py);
    let out = PyDict::new(py);
    // One pass: copy scalars/proxies by shared ref; shallow-copy lists so the
    // caller cannot append/replace through a shared list object.
    for (key, value) in src.iter() {
        if let Ok(list) = value.cast::<PyList>() {
            let copied = PyList::empty(py);
            for item in list.iter() {
                copied.append(item)?;
            }
            out.set_item(key, copied)?;
        } else {
            out.set_item(key, value)?;
        }
    }
    Ok(out)
}

pub(crate) fn units_py_list<'py>(
    py: Python<'py>,
    authority: &str,
    category: Option<&str>,
    allow_deprecated: bool,
) -> PyResult<Bound<'py, PyList>> {
    let state = crs::runtime_state_snapshot()?;
    let generation = state.generation;
    UNITS_PY_CACHE.with(|cache| {
        py_lru_resolve(
            cache,
            UNITS_PY_CAPACITY,
            generation,
            |e| e.generation,
            |e| {
                e.authority == authority
                    && e.category.as_deref() == category
                    && e.allow_deprecated == allow_deprecated
            },
            |e| isolation_copy_record_list(py, &e.list),
            || {
                let items = py.detach(|| {
                    crs::units_with_state(authority, category, allow_deprecated, &state)
                })?;
                let list = PyList::empty(py);
                for item in items {
                    list.append(item)?;
                }
                Ok(UnitsPyEntry {
                    generation,
                    authority: authority.to_owned(),
                    category: category.map(str::to_owned),
                    allow_deprecated,
                    list: list.unbind(),
                })
            },
        )
    })
}

pub(crate) fn celestial_bodies_py_list<'py>(
    py: Python<'py>,
    authority: Option<&str>,
) -> PyResult<Bound<'py, PyList>> {
    let state = crs::runtime_state_snapshot()?;
    let generation = state.generation;
    BODIES_PY_CACHE.with(|cache| {
        py_lru_resolve(
            cache,
            BODIES_PY_CAPACITY,
            generation,
            |e| e.generation,
            |e| e.authority.as_deref() == authority,
            |e| isolation_copy_record_list(py, &e.list),
            || {
                let items = py.detach(|| crs::celestial_bodies_with_state(authority, &state))?;
                let list = PyList::empty(py);
                for item in items {
                    list.append(item)?;
                }
                Ok(BodiesPyEntry {
                    generation,
                    authority: authority.map(str::to_owned),
                    list: list.unbind(),
                })
            },
        )
    })
}

/// Cached `crs_authorities` list (generation-stamped, isolation-copied).
pub(crate) fn authorities_py_list(py: Python<'_>) -> PyResult<Bound<'_, PyList>> {
    let state = crs::runtime_state_snapshot()?;
    let generation = state.generation;
    AUTHORITIES_PY_CACHE.with(|cache| {
        py_lru_resolve(
            cache,
            AUTHORITIES_PY_CAPACITY,
            generation,
            |e| e.generation,
            |_| true,
            |e| shallow_copy_list(py, &e.list),
            || {
                let items = py.detach(|| crs::authorities_with_state(&state))?;
                let list = PyList::empty(py);
                for item in items {
                    list.append(item)?;
                }
                Ok(AuthoritiesPyEntry {
                    generation,
                    list: list.unbind(),
                })
            },
        )
    })
}

/// Cached single-operation dict for `(source, target, options)`.
pub(crate) fn crs_operation_py_dict<'py>(
    py: Python<'py>,
    source: &str,
    target: &str,
    options: &crs::TransformOptions,
) -> PyResult<Bound<'py, PyDict>> {
    let state = crs::runtime_state_snapshot()?;
    let generation = state.generation;
    OPERATION_PY_CACHE.with(|cache| {
        py_lru_resolve(
            cache,
            OPERATION_PY_CAPACITY,
            generation,
            |e| e.generation,
            |e| e.source == source && e.target == target && e.options == *options,
            |e| isolation_copy_dict(py, &e.dict),
            || {
                let info =
                    py.detach(|| crs::operation_info_with_state(source, target, options, &state))?;
                let built = info.into_pyobject(py)?;
                let owned = freeze_top_dict(py, &built)?;
                Ok(OperationPyEntry {
                    generation,
                    source: source.to_owned(),
                    target: target.to_owned(),
                    options: options.clone(),
                    dict: owned,
                })
            },
        )
    })
}

/// Cached list of operation dicts for `(source, target, options)`.
pub(crate) fn crs_operations_py_list<'py>(
    py: Python<'py>,
    source: &str,
    target: &str,
    options: &crs::TransformOptions,
) -> PyResult<Bound<'py, PyList>> {
    let state = crs::runtime_state_snapshot()?;
    let generation = state.generation;
    OPERATIONS_PY_CACHE.with(|cache| {
        py_lru_resolve(
            cache,
            OPERATIONS_PY_CAPACITY,
            generation,
            |e| e.generation,
            |e| e.source == source && e.target == target && e.options == *options,
            |e| isolation_copy_record_list(py, &e.list),
            || {
                let items =
                    py.detach(|| crs::operations_info_with_state(source, target, options, &state))?;
                let list = PyList::empty(py);
                for item in items {
                    let built = item.into_pyobject(py)?;
                    list.append(freeze_top_dict(py, &built)?)?;
                }
                Ok(OperationsPyEntry {
                    generation,
                    source: source.to_owned(),
                    target: target.to_owned(),
                    options: options.clone(),
                    list: list.unbind(),
                })
            },
        )
    })
}

/// New list of isolation-copied record dicts.
fn isolation_copy_record_list<'py>(
    py: Python<'py>,
    cached: &Py<PyList>,
) -> PyResult<Bound<'py, PyList>> {
    let src = cached.bind(py);
    let out = PyList::empty(py);
    for item in src.iter() {
        let dict = item.cast::<PyDict>().map_err(PyErr::from)?;
        let owned: Py<PyDict> = dict.clone().unbind();
        out.append(isolation_copy_dict(py, &owned)?)?;
    }
    Ok(out)
}

/// Drop Python-side catalog lists and dict materializations immediately
/// (generation already bumped).
pub(crate) fn clear_py_list_caches() {
    UNITS_PY_CACHE.with(|cache| drop(cache.take()));
    BODIES_PY_CACHE.with(|cache| drop(cache.take()));
    AUTHORITIES_PY_CACHE.with(|cache| drop(cache.take()));
    OPERATION_PY_CACHE.with(|cache| drop(cache.take()));
    OPERATIONS_PY_CACHE.with(|cache| drop(cache.take()));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::miss(false)]
    #[case::hit(true)]
    fn copying_can_reenter_the_catalog_cache(#[case] warm: bool) {
        let cache = RefCell::new(if warm { vec![7] } else { Vec::new() });
        let value = py_lru_resolve(
            &cache,
            2,
            1,
            |_| 1,
            |entry| *entry == 7,
            |entry| {
                let nested = py_lru_resolve(
                    &cache,
                    2,
                    1,
                    |_| 1,
                    |entry| *entry == 11,
                    |entry| Ok(*entry),
                    || Ok(11),
                )?;
                Ok((*entry, nested))
            },
            || Ok(7),
        )
        .expect("copy can resolve another catalog entry");
        assert_eq!(value, (7, 11));
        let mut entries = cache.into_inner();
        entries.sort_unstable();
        assert_eq!(entries, [7, 11]);
    }
}
