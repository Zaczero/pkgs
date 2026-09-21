use crate::py::crs::{Bound, CRSError, Crs, PyAny, PyResult, crs, parse_crs_inner, pyfunction};
/// Name/version of the underlying CRS engine (PROJ).
///
/// Returns
/// -------
/// dict
///     Engine metadata. ``paths`` is the effective per-context grid search
///     path configured through :func:`crs_configure`.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_engine()['version']
/// '9.6.2'
#[pyfunction]
pub(crate) fn crs_engine() -> PyResult<crs::EngineInfo> {
    let state = crs::runtime_state_snapshot()?;
    Ok(crs::engine_info_with_state(&state)?)
}

/// One required-CRS parser: borrowed `Crs`/text through the shared
/// [`parse_crs_inner`] path (geometry `to_crs`, transforms, and normalize).
pub(crate) fn crs_parse_required(value: &Bound<'_, PyAny>) -> PyResult<Crs> {
    parse_crs_inner(value, 0)?.ok_or_else(|| CRSError::new_err("CRS is required"))
}

// Normalized CRS definition: the same parser as `crs_parse_required` /
// geometry `to_crs`, so syntax-only spellings ("Epsg:4326", "EPSG:4326") share
// one stored authority spelling without rewriting authored definitions.
pub(crate) fn crs_normalize(value: &Bound<'_, PyAny>) -> PyResult<String> {
    let crs = crs_parse_required(value)?;
    Ok(crs.as_str().to_owned())
}
