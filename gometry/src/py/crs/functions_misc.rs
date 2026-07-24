use crate::py::crs::*;
/// Name/version of the underlying CRS engine (PROJ).
///
/// Returns
/// -------
/// dict
///     Engine metadata (backend, version, PROJ paths, local grid directory).
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_engine()['version']
/// '9.8.1'
pub(crate) fn crs_engine() -> PyResult<crs::EngineInfo> {
    Ok(crs::engine_info()?)
}

/// One required-CRS parser: borrowed `Crs`/text through the shared
/// [`parse_crs_inner`] path (geometry `to_crs`, transforms, and normalize).
pub(crate) fn crs_parse_required(value: &Bound<'_, PyAny>) -> PyResult<Crs> {
    parse_crs_inner(value, 0)?.ok_or_else(|| CRSError::new_err("CRS is required"))
}

/// Normalize a CRS specification to a canonical form.
///
/// Parameters
/// ----------
/// value : CRS-like
///     CRS as an EPSG code or authority/WKT string. Authority prefixes are
///     case-insensitive (``Epsg:4326`` is accepted and canonicalizes to
///     ``EPSG:4326``).
///
/// Returns
/// -------
/// str
///     The canonical CRS string.
///
/// Raises
/// ------
/// CRSError
///     If the value is not a recognized CRS.
#[pyfunction]
pub(crate) fn crs_normalize(value: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(crs_parse_required(value)?.to_string())
}

/// Canonical CRS identifier for the raw-transform cache key — same parser as
/// [`crs_parse_required`] / geometry `to_crs`, so equivalent spellings share
/// one cached pipeline and the in-core fast path.
pub(crate) fn crs_canonical(value: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(crs_parse_required(value)?.to_string())
}
