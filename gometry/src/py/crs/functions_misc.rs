use crate::py::crs::{Bound, CRSError, Crs, PyAny, PyResult, crs, parse_crs_inner, pyfunction};
/// Name/version of the underlying CRS engine (PROJ).
///
/// Returns
/// -------
/// dict
///     Engine metadata. ``paths`` is the effective per-context grid search
///     path configured through :func:`crs_configure`.
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_engine()['version']
/// '9.8.1'
pub(crate) fn crs_engine() -> PyResult<crs::EngineInfo> {
    let mut info = crs::engine_info()?;
    info.paths = crs::runtime_config()?.search_paths.unwrap_or_default();
    Ok(info)
}

/// One required-CRS parser: borrowed `Crs`/text through the shared
/// [`parse_crs_inner`] path (geometry `to_crs`, transforms, and normalize).
pub(crate) fn crs_parse_required(value: &Bound<'_, PyAny>) -> PyResult<Crs> {
    parse_crs_inner(value, 0)?.ok_or_else(|| CRSError::new_err("CRS is required"))
}

// Canonical CRS identifier: the same parser as `crs_parse_required` / geometry
// `to_crs`, so equivalent spellings ("Epsg:4326", "EPSG:4326") share one cached
// pipeline and the in-core fast path.
//
// Crate-internal only.  This carried `#[pyfunction]` and a full user-facing
// numpydoc block, but was registered nowhere — absent from `register()`, from
// `__init__.py`, from `_lib.pyi`, and from the runtime.  It was the crate's only
// unregistered `#[pyfunction]`, and the family-inventory gate is a hard-coded
// allowlist so it could not catch that.  Registering it would also duplicate
// `str(gm.CRS(v))`, which already returns the canonical string.
//
// A byte-identical twin named `crs_canonical` used to sit beside this and was
// called interchangeably within a single file; it has been folded in here.
pub(crate) fn crs_normalize(value: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(crs_parse_required(value)?.to_string())
}
