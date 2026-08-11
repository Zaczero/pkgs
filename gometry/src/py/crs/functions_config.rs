use crate::py::crs::{
    Bound, PyAny, PyAnyMethods as _, PyErr, PyResult, PyStringMethods as _, PyTypeError, crs,
    pyfunction,
};

#[pyfunction(signature = (
    *,
    search_paths = None,
    user_writable_directory = None
))]
/// Configure local CRS engine paths.
///
/// Parameters
/// ----------
/// search_paths : str, path, or sequence of these, optional
///     Directories PROJ searches for its database and grids.
/// user_writable_directory : str or path, optional
///     Directory PROJ includes in local grid lookup.
///
/// Returns
/// -------
/// dict
///     The effective configuration after applying the changes.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> isinstance(gm.crs_configure(), dict)
/// True
pub(crate) fn crs_configure(
    search_paths: Option<&Bound<'_, PyAny>>,
    user_writable_directory: Option<&Bound<'_, PyAny>>,
) -> PyResult<crs::RuntimeConfig> {
    Ok(crs::configure_runtime(
        parse_search_paths(search_paths)?,
        parse_optional_path("user_writable_directory", user_writable_directory)?,
    )?)
}

fn parse_optional_path(
    name: &'static str,
    value: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_none() {
        return Ok(None);
    }
    parse_path(value, name).map(Some)
}

fn parse_search_paths(value: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Vec<String>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_none() {
        return Ok(None);
    }
    if let Ok(path) = parse_path(value, "search_paths") {
        return Ok(Some(vec![path]));
    }
    // Specialized fallible loop (not bare collect_py_iter + extract::<String>):
    // each path is an owned heap `String`. Outer `Vec` growth *and* per-item
    // string reservation must be fallible, and retained output must be dropped
    // *before* boxing `PyMemoryError` — otherwise RLIMIT_AS aborts while the
    // exception is constructed (`memory allocation of N bytes failed`).
    let mut paths = Vec::new();
    if let Ok(hint) = value.len()
        && crate::try_reserve_hint(&mut paths, hint).is_err()
    {
        return Err(crate::grow_sequence_error());
    }
    let Ok(mut iter) = value.try_iter() else {
        return Err(PyTypeError::new_err(
            "search_paths must be a string/path-like path or an iterable of string/path-like paths",
        ));
    };
    loop {
        let item = match iter.next() {
            None => break,
            Some(Ok(item)) => item,
            Some(Err(err)) => {
                drop(paths);
                return Err(err);
            },
        };
        if paths.len() == paths.capacity() {
            let additional = paths.capacity().max(8);
            if paths.try_reserve(additional).is_err() {
                drop(paths);
                return Err(crate::grow_sequence_error());
            }
        }
        match take_path_string(&item, "search_paths entries") {
            Ok(owned) => paths.push(owned),
            Err(PathTakeError::Oom) => {
                drop(paths);
                return Err(crate::string_alloc_error());
            },
            Err(PathTakeError::Err(err)) => {
                drop(paths);
                if err.is_instance_of::<pyo3::exceptions::PyMemoryError>(value.py()) {
                    return Err(err);
                }
                return Err(PyTypeError::new_err(
                    "search_paths must be a string/path-like path or an iterable of string/path-like paths",
                ));
            },
        }
    }
    Ok(Some(paths))
}

enum PathTakeError {
    /// Allocator refused the owned-string reservation (no `PyErr` yet).
    Oom,
    Err(PyErr),
}

/// Copy a string/path-like Python value into an owned Rust path string.
///
/// OOM is `PathTakeError::Oom` so the caller can drop retained buffers before
/// boxing a `PyMemoryError`.
fn take_path_string(value: &Bound<'_, PyAny>, name: &'static str) -> Result<String, PathTakeError> {
    if let Ok(py_str) = value.cast::<pyo3::types::PyString>() {
        let s = py_str.to_str().map_err(PathTakeError::Err)?;
        return crate::try_string_from_str(s).map_err(|()| PathTakeError::Oom);
    }
    let os = value.py().import("os").map_err(PathTakeError::Err)?;
    let path = os
        .getattr("fspath")
        .map_err(PathTakeError::Err)?
        .call1((value,))
        .map_err(|_| {
            PathTakeError::Err(PyTypeError::new_err(format!(
                "{name} must be a string or path-like path"
            )))
        })?;
    if let Ok(py_str) = path.cast::<pyo3::types::PyString>() {
        let s = py_str.to_str().map_err(PathTakeError::Err)?;
        return crate::try_string_from_str(s).map_err(|()| PathTakeError::Oom);
    }
    Err(PathTakeError::Err(PyTypeError::new_err(format!(
        "{name} must resolve to a string path"
    ))))
}

fn parse_path(value: &Bound<'_, PyAny>, name: &'static str) -> PyResult<String> {
    // Fallible owned copy: `extract::<String>()` allocates infallibly and can
    // abort under RLIMIT_AS on unbounded `search_paths` streams.
    match take_path_string(value, name) {
        Ok(s) => Ok(s),
        Err(PathTakeError::Oom) => Err(crate::string_alloc_error()),
        Err(PathTakeError::Err(err)) => Err(err),
    }
}

/// Return the current CRS engine configuration.
///
/// Returns
/// -------
/// dict
///     The current CRS engine configuration.
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> isinstance(gm.crs_config(), dict)
/// True
pub(crate) fn crs_config() -> PyResult<crs::RuntimeConfig> {
    Ok(crs::runtime_config()?)
}

/// Reset CRS engine configuration to defaults.
///
/// Returns
/// -------
/// dict
///     The configuration after the reset.
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> isinstance(gm.crs_reset(), dict)
/// True
pub(crate) fn crs_reset() -> PyResult<crs::RuntimeConfig> {
    Ok(crs::reset_runtime_config()?)
}

/// Clear the CRS object cache.
///
/// Returns
/// -------
/// None
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_clear_cache()
/// >>> True
/// True
pub(crate) fn crs_clear_cache() {
    crs::clear_cache();
    // Drop Python-side catalog list materializations on this thread immediately
    // (generation bump already invalidates on next access for other threads).
    super::list_cache::clear_py_list_caches();
}

/// Statistics about the CRS caches.
///
/// Returns
/// -------
/// dict
///     CRS cache statistics (generation, entry counts, per-bucket info).
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_cache_info()['total_capacity'] > 0
/// True
pub(crate) fn crs_cache_info() -> crs::CacheInfo {
    crs::cache_info()
}
