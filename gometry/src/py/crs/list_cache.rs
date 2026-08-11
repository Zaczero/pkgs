//! Thread-local caches of already-built Python containers for static PROJ
//! catalog reads (`crs_units`, `crs_celestial_bodies`) and rich CRS dict
//! materializations (`crs_info`, `crs_operation`, `crs_operations`).
//!
//! Rust-side LRUs still own the PROJ DB work; this layer stores the
//! IntoPyObject materialization so warm hits return an isolation-safe copy
//! instead of re-building nested dicts. Entries are stamped with
//! [`crate::crs::runtime_config_generation`] and dropped on generation bump
//! (`crs_clear_cache` / runtime config changes) — same lifetime as the CRS
//! thread-local LRUs.
//!
//! Isolation contract: nested dicts are frozen as `MappingProxyType` and nested
//! lists as tuples inside those proxies, so in-place leaf mutation cannot poison
//! the cache. Top-level lists stay real lists and are shallow-copied on return
//! (new list, shared frozen elements). Top-level dicts are shallow-copied.

use std::cell::RefCell;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::crs;

const UNITS_PY_CAPACITY: usize = 16;
const BODIES_PY_CAPACITY: usize = 8;
const AUTHORITIES_PY_CAPACITY: usize = 4;
const INFO_PY_CAPACITY: usize = 64;
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

struct InfoPyEntry {
    generation: u64,
    crs: String,
    dict: Py<PyDict>,
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
    static INFO_PY_CACHE: RefCell<Vec<InfoPyEntry>> = const { RefCell::new(Vec::new()) };
    static OPERATION_PY_CACHE: RefCell<Vec<OperationPyEntry>> = const { RefCell::new(Vec::new()) };
    static OPERATIONS_PY_CACHE: RefCell<Vec<OperationsPyEntry>> = const { RefCell::new(Vec::new()) };
}

/// Drop stale entries and return a shallow copy of a cached list, or build one.
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
    cache: &mut Vec<E>,
    capacity: usize,
    generation: u64,
    entry_generation: impl Fn(&E) -> u64,
    matches: impl Fn(&E) -> bool,
    copy_out: impl FnOnce(&E) -> PyResult<R>,
    make: impl FnOnce() -> PyResult<E>,
) -> PyResult<R> {
    cache.retain(|entry| entry_generation(entry) == generation);
    if let Some(index) = cache.iter().position(&matches) {
        let entry = cache.remove(index);
        let out = copy_out(&entry)?;
        cache.push(entry);
        return Ok(out);
    }
    if cache.len() >= capacity {
        cache.remove(0);
    }
    let entry = make()?;
    let out = copy_out(&entry)?;
    cache.push(entry);
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
    dict: Bound<'py, PyDict>,
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
    let generation = crs::runtime_config_generation();
    UNITS_PY_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        py_lru_resolve(
            &mut cache,
            UNITS_PY_CAPACITY,
            generation,
            |e| e.generation,
            |e| {
                e.authority == authority
                    && e.category.as_deref() == category
                    && e.allow_deprecated == allow_deprecated
            },
            |e| shallow_copy_list(py, &e.list),
            || {
                let items = crs::units(authority, category, allow_deprecated)?;
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
    let generation = crs::runtime_config_generation();
    BODIES_PY_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        py_lru_resolve(
            &mut cache,
            BODIES_PY_CAPACITY,
            generation,
            |e| e.generation,
            |e| e.authority.as_deref() == authority,
            |e| shallow_copy_list(py, &e.list),
            || {
                let items = crs::celestial_bodies(authority)?;
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
    let generation = crs::runtime_config_generation();
    AUTHORITIES_PY_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        py_lru_resolve(
            &mut cache,
            AUTHORITIES_PY_CAPACITY,
            generation,
            |e| e.generation,
            |_| true,
            |e| shallow_copy_list(py, &e.list),
            || {
                let items = crs::authorities()?;
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

/// Cached `crs_info` Python dict for a normalized CRS identifier.
pub(crate) fn crs_info_py_dict<'py>(
    py: Python<'py>,
    normalized_crs: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let generation = crs::runtime_config_generation();
    INFO_PY_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        py_lru_resolve(
            &mut cache,
            INFO_PY_CAPACITY,
            generation,
            |e| e.generation,
            |e| e.crs == normalized_crs,
            |e| isolation_copy_dict(py, &e.dict),
            || {
                let info = (*crs::info(normalized_crs)?).clone();
                let built = info.into_pyobject(py)?;
                let owned = freeze_top_dict(py, built)?;
                Ok(InfoPyEntry {
                    generation,
                    crs: normalized_crs.to_owned(),
                    dict: owned,
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
    let generation = crs::runtime_config_generation();
    OPERATION_PY_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        py_lru_resolve(
            &mut cache,
            OPERATION_PY_CAPACITY,
            generation,
            |e| e.generation,
            |e| e.source == source && e.target == target && e.options == *options,
            |e| isolation_copy_dict(py, &e.dict),
            || {
                let info = crs::operation_info(source, target, options)?;
                let built = info.into_pyobject(py)?;
                let owned = freeze_top_dict(py, built)?;
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
    let generation = crs::runtime_config_generation();
    OPERATIONS_PY_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        py_lru_resolve(
            &mut cache,
            OPERATIONS_PY_CAPACITY,
            generation,
            |e| e.generation,
            |e| e.source == source && e.target == target && e.options == *options,
            |e| isolation_copy_operations_list(py, &e.list),
            || {
                let items = crs::operations_info(source, target, options)?;
                let list = PyList::empty(py);
                for item in items {
                    let built = item.into_pyobject(py)?;
                    list.append(freeze_top_dict(py, built)?)?;
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

/// New list of isolation-copied operation dicts (each top-level list key copied).
fn isolation_copy_operations_list<'py>(
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
    UNITS_PY_CACHE.with(|cache| cache.borrow_mut().clear());
    BODIES_PY_CACHE.with(|cache| cache.borrow_mut().clear());
    AUTHORITIES_PY_CACHE.with(|cache| cache.borrow_mut().clear());
    INFO_PY_CACHE.with(|cache| cache.borrow_mut().clear());
    OPERATION_PY_CACHE.with(|cache| cache.borrow_mut().clear());
    OPERATIONS_PY_CACHE.with(|cache| cache.borrow_mut().clear());
}
