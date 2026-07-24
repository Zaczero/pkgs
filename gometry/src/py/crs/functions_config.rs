use super::*;

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
    // Fallible growth (D10): infinite search_paths streams → MemoryError.
    let paths =
        crate::collect_py_iter(value, |item| parse_path(&item, "search_paths entries")).map_err(
            |err| {
                if err.is_instance_of::<pyo3::exceptions::PyMemoryError>(value.py())
                    || (err.is_instance_of::<PyTypeError>(value.py())
                        && !err.to_string().contains("not iterable"))
                {
                    err
                } else {
                    PyTypeError::new_err(
                        "search_paths must be a string/path-like path or an iterable of string/path-like paths",
                    )
                }
            },
        )?;
    Ok(Some(paths))
}

fn parse_path(value: &Bound<'_, PyAny>, name: &'static str) -> PyResult<String> {
    if let Ok(path) = value.extract::<String>() {
        return Ok(path);
    }
    let os = value.py().import("os")?;
    let path = os.getattr("fspath")?.call1((value,)).and_then(|path| {
        path.extract::<String>()
            .map_err(|_| PyTypeError::new_err(format!("{name} must resolve to a string path")))
    })?;
    Ok(path)
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
