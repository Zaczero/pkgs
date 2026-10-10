use pyo3::{Borrowed, FromPyObject, Py};

use crate::crs::ConfigUpdate;
use crate::py::crs::{
    Bound, PyAny, PyAnyMethods as _, PyErr, PyResult, PyStringMethods as _, PyTypeError, Python,
    crs, pyfunction,
};

pub(crate) enum ConfigureArgument {
    Omitted,
    Explicit(Py<PyAny>),
}

impl<'a, 'py> FromPyObject<'a, 'py> for ConfigureArgument {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        Ok(Self::Explicit(value.to_owned().unbind()))
    }
}

#[pyfunction(
    signature = (
        search_paths = ConfigureArgument::Omitted,
        user_writable_directory = ConfigureArgument::Omitted
    ),
    text_signature = "($module, /, *, search_paths=..., user_writable_directory=...)"
)]
/// Configure local CRS engine paths.
///
/// Omitted arguments keep their current value, while explicit ``None`` restores
/// that field's default. A call without arguments returns the current
/// configuration unchanged; use `crs_reset` to restore both defaults.
///
/// Parameters
/// ----------
/// search_paths : str, path, or sequence of these, optional
///     Directories PROJ searches for its database and grids. ``None`` restores
///     the default search paths. When a named grid is absent from these
///     directories, PROJ may also try the bare grid filename in the current
///     working directory.
/// user_writable_directory : str or path, optional
///     Directory PROJ includes in local grid lookup. ``None`` restores the
///     default writable directory.
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
    py: Python<'_>,
    search_paths: ConfigureArgument,
    user_writable_directory: ConfigureArgument,
) -> PyResult<crs::RuntimeConfig> {
    Ok(crs::configure_runtime(
        parse_search_paths(py, search_paths)?,
        parse_optional_path(py, "user_writable_directory", user_writable_directory)?,
    )?)
}

fn parse_optional_path(
    py: Python<'_>,
    name: &'static str,
    value: ConfigureArgument,
) -> PyResult<ConfigUpdate<Option<String>>> {
    match value {
        ConfigureArgument::Omitted => Ok(ConfigUpdate::Keep),
        ConfigureArgument::Explicit(value) => {
            let value = value.bind(py);
            if value.is_none() {
                Ok(ConfigUpdate::Set(None))
            } else {
                parse_path(value, name).map(|path| ConfigUpdate::Set(Some(path)))
            }
        },
    }
}

fn parse_search_paths(
    py: Python<'_>,
    value: ConfigureArgument,
) -> PyResult<ConfigUpdate<Option<Vec<String>>>> {
    let owner = match value {
        ConfigureArgument::Omitted => return Ok(ConfigUpdate::Keep),
        ConfigureArgument::Explicit(value) => value,
    };
    let value = owner.bind(py);
    if value.is_none() {
        return Ok(ConfigUpdate::Set(None));
    }
    if value.cast::<pyo3::types::PyString>().is_ok()
        || value
            .getattr_opt(pyo3::intern!(value.py(), "__fspath__"))?
            .is_some()
    {
        return Ok(ConfigUpdate::Set(Some(vec![parse_path(
            value,
            "search_paths",
        )?])));
    }
    // Keep the specialized loop so provider exceptions remain distinguishable
    // from values that simply are not string/path-like entries.
    let mut iter = crate::py_iter_or_error(value, || {
        PyTypeError::new_err(
            "search_paths must be a string/path-like path or an iterable of string/path-like paths",
        )
    })?;
    let mut paths = Vec::new();
    crate::reserve_from_py_len(value, &mut paths)?;
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
        match take_path_string(&item, "search_paths entries") {
            Ok(owned) => paths.push(owned),
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
    Ok(ConfigUpdate::Set(Some(paths)))
}

enum PathTakeError {
    Provider(PyErr),
    Err(PyErr),
}

/// Copy a string/path-like Python value into an owned Rust path string.
fn take_path_string(value: &Bound<'_, PyAny>, name: &'static str) -> Result<String, PathTakeError> {
    if let Ok(py_str) = value.cast::<pyo3::types::PyString>() {
        let s = py_str.to_str().map_err(PathTakeError::Err)?;
        return Ok(s.to_owned());
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
        return Ok(s.to_owned());
    }
    Err(PathTakeError::Err(PyTypeError::new_err(format!(
        "{name} must resolve to a string path"
    ))))
}

fn parse_path(value: &Bound<'_, PyAny>, name: &'static str) -> PyResult<String> {
    match take_path_string(value, name) {
        Ok(s) => Ok(s),
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
