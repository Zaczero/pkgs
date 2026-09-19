use crate::py::crs::{
    Bound, PyAny, PyAnyMethods as _, PyErr, PyResult, PyStringMethods as _, PyTypeError, crs,
    pyfunction,
};
use crate::py::errors::native_memory_error;

#[pyfunction(signature = (
    *,
    search_paths = None,
    user_writable_directory = None
))]
/// Configure local CRS engine paths.
///
/// Omitted arguments keep their current value, so a call without arguments
/// returns the current configuration unchanged. Use `crs_reset` to restore
/// the defaults.
///
/// Parameters
/// ----------
/// search_paths : str, path, or sequence of these, optional
///     Directories PROJ searches for its database and grids. When a named grid
///     is absent from these directories, PROJ may also try the bare grid
///     filename in the current working directory.
/// user_writable_directory : str or path, optional
///     Directory PROJ includes in local grid lookup.
///
/// Notes
/// -----
/// The configured directories are not an exclusive grid-file whitelist. A
/// grid in the current working directory can therefore change grid
/// availability, the selected transformation, and whether ``AccuracyWarning``
/// is emitted after the configured directories miss.
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
    if value.cast::<pyo3::types::PyString>().is_ok()
        || value
            .getattr_opt(pyo3::intern!(value.py(), "__fspath__"))?
            .is_some()
    {
        return Ok(Some(vec![parse_path(value, "search_paths")?]));
    }
    // Specialized fallible loop (not bare collect_py_iter + extract::<String>):
    // each path is an owned heap `String`. Outer `Vec` growth *and* per-item
    // string reservation must be fallible, and retained output must be dropped
    // *before* raising `MemoryError` — otherwise RLIMIT_AS can abort while the
    // exception is constructed (`memory allocation of N bytes failed`).
    let mut paths = Vec::new();
    if let Some(hint) = crate::advisory_len(value)? {
        crate::try_reserve_hint(&mut paths, hint)?;
    }
    let mut iter = crate::py_iter_or_error(value, || {
        PyTypeError::new_err(
            "search_paths must be a string/path-like path or an iterable of string/path-like paths",
        )
    })?;
    loop {
        let item = match iter.next() {
            None => break,
            Some(Ok(item)) => item,
            Some(Err(err)) => {
                drop(paths);
                return Err(err);
            },
        };
        crate::check_python_signals(value.py(), paths.len())?;
        if paths.len() == paths.capacity() {
            let additional = paths.capacity().max(8);
            if paths.try_reserve(additional).is_err() {
                drop(paths);
                return Err(native_memory_error());
            }
        }
        match take_path_string(&item, "search_paths entries") {
            Ok(owned) => paths.push(owned),
            Err(PathTakeError::Oom) => {
                drop(paths);
                return Err(native_memory_error());
            },
            Err(PathTakeError::Provider(err)) => {
                drop(paths);
                return Err(err);
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
    Provider(PyErr),
    Err(PyErr),
}

/// Copy a string/path-like Python value into an owned Rust path string.
///
/// OOM is `PathTakeError::Oom` so the caller can drop retained buffers before
/// raising the native `MemoryError`.
fn take_path_string(value: &Bound<'_, PyAny>, name: &'static str) -> Result<String, PathTakeError> {
    if let Ok(py_str) = value.cast::<pyo3::types::PyString>() {
        let s = py_str.to_str().map_err(PathTakeError::Err)?;
        return crate::try_string_from_str(s).map_err(|()| PathTakeError::Oom);
    }
    let fspath = value
        .getattr_opt(pyo3::intern!(value.py(), "__fspath__"))
        .map_err(PathTakeError::Provider)?
        .ok_or_else(|| {
            PathTakeError::Err(PyTypeError::new_err(format!(
                "{name} must be a string or path-like path"
            )))
        })?;
    if !fspath.is_callable() {
        return Err(PathTakeError::Err(PyTypeError::new_err(format!(
            "{name} must be a string or path-like path"
        ))));
    }
    let path = fspath.call0().map_err(PathTakeError::Provider)?;
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
        Err(PathTakeError::Oom) => Err(native_memory_error()),
        Err(PathTakeError::Provider(err) | PathTakeError::Err(err)) => Err(err),
    }
}

/// Reset CRS engine configuration to defaults.
///
/// Returns
/// -------
/// dict
///     The configuration after the reset.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> isinstance(gm.crs_reset(), dict)
/// True
#[pyfunction]
pub(crate) fn crs_reset() -> PyResult<crs::RuntimeConfig> {
    Ok(crs::reset_runtime_config()?)
}

/// Clear the CRS object cache.
///
/// Returns
/// -------
/// None
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_clear_cache()
/// >>> True
/// True
#[pyfunction]
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
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_cache_info()['total_capacity'] > 0
/// True
#[pyfunction]
pub(crate) fn crs_cache_info() -> crs::CacheInfo {
    crs::cache_info()
}
