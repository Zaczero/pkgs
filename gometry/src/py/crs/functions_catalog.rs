use std::num::NonZeroUsize;

use super::*;

/// Names of all registry authorities known to PROJ.
///
/// Returns
/// -------
/// list of str
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_authorities()[:3]
/// ['EPSG', 'ESRI', 'IAU_2015']
#[pyfunction]
pub(crate) fn crs_authorities() -> PyResult<Vec<String>> {
    Ok(crs::authorities()?)
}

/// Object codes within a registry authority.
///
/// Parameters
/// ----------
/// authority : str
///     Registry authority, e.g. ``"EPSG"``.
/// kind : str, optional
///     Restrict to a CRS kind.
/// allow_deprecated : bool, optional
///     Include deprecated codes (default ``False``).
///
/// Returns
/// -------
/// list of str
#[pyfunction]
#[pyo3(signature = (authority, *, kind = None, allow_deprecated = false))]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_codes('EPSG')[:3]
/// ['10150', '10151', '10156']
pub(crate) fn crs_codes(
    authority: &str,
    kind: Option<&str>,
    allow_deprecated: bool,
) -> PyResult<Vec<String>> {
    Ok(crs::codes(authority, kind, allow_deprecated)?)
}

/// Non-deprecated replacements for a deprecated CRS.
///
/// Parameters
/// ----------
/// crs : str or int
///     CRS as an EPSG code or authority/WKT string.
///
/// Returns
/// -------
/// list
pub(crate) fn crs_non_deprecated(
    value: &Bound<'_, PyAny>,
) -> PyResult<Vec<crs::AuthorityObjectInfo>> {
    Ok(crs::non_deprecated(&crs_normalize(value)?)?)
}

/// Search the PROJ catalog for CRS by name.
///
/// Parameters
/// ----------
/// name : str
///     Substring to match against CRS names.
/// authority : str, optional
///     Restrict to this authority.
/// kind : str, optional
///     Restrict to a CRS kind (e.g. ``"projected"``).
/// approximate : bool, optional
///     Allow approximate name matching (default ``False``).
/// limit : int, optional
///     Maximum number of results.
///
/// Returns
/// -------
/// list of CrsCatalogInfo
#[pyfunction]
#[pyo3(signature = (name, *, authority = None, kind = None, approximate = false, limit = 20))]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_search('WGS 84')[0]['code']
/// '4326'
pub(crate) fn crs_search(
    name: &str,
    authority: Option<String>,
    kind: Option<String>,
    approximate: bool,
    limit: i64,
) -> PyResult<Vec<crs::AuthorityObjectInfo>> {
    let limit = crs_search_limit(limit)?;
    Ok(crs::search(name, &crs::CrsSearchOptions {
        authority,
        kind: kind
            .as_deref()
            .map(|kind| crs::CrsObjectKind::parse_crs(Some(kind)))
            .transpose()?,
        approximate,
        limit,
    })?)
}

pub(crate) fn crs_search_limit(limit: i64) -> PyResult<NonZeroUsize> {
    Ok(crs::validate_search_limit(limit)?)
}

/// Catalog of CRS in the database matching the given filters.
///
/// Parameters
/// ----------
/// authority : str, optional
///     Registry authority to search (default ``"EPSG"``).
/// kind : str, optional
///     Restrict to a CRS kind.
/// area : sequence of float, optional
///     ``(minx, miny, maxx, maxy)`` filter area.
/// contains_area : bool, optional
///     Require the CRS area to contain ``area`` (default ``False``).
/// allow_deprecated : bool, optional
///     Include deprecated CRS (default ``False``).
/// celestial_body : str, optional
///     Restrict to this celestial body.
///
/// Returns
/// -------
/// list of CrsCatalogInfo
#[pyfunction]
#[pyo3(signature = (*, authority = None, kind = None, area = None, contains_area = false, allow_deprecated = false, celestial_body = None))]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_catalog(authority='EPSG')[0]['code']
/// '2000'
pub(crate) fn crs_catalog(
    authority: Option<&str>,
    kind: Option<String>,
    area: Option<&Bound<'_, PyAny>>,
    contains_area: bool,
    allow_deprecated: bool,
    celestial_body: Option<String>,
) -> PyResult<Vec<crs::CrsCatalogInfo>> {
    if matches!(authority, Some("")) {
        return Err(CRSError::new_err(
            "CRS catalog authority must be a non-empty string",
        ));
    }
    Ok(crs::catalog(authority, &crs::CrsCatalogOptions {
        kind: kind
            .as_deref()
            .map(|kind| crs::CrsObjectKind::parse_crs(Some(kind)))
            .transpose()?,
        area: parse_area(area, "area")?,
        contains_area,
        allow_deprecated,
        celestial_body,
    })?)
}

/// UTM-zone CRS from the catalog.
///
/// Parameters
/// ----------
/// datum_name : str, optional
///     Restrict to this datum.
/// area : sequence of float, optional
///     ``(minx, miny, maxx, maxy)`` filter area.
/// contains_area : bool, optional
///     Require the zone area to contain ``area`` (default ``False``).
/// allow_deprecated : bool, optional
///     Include deprecated zones (default ``False``).
///
/// Returns
/// -------
/// list of CrsCatalogInfo
#[pyfunction]
#[pyo3(signature = (*, datum_name = None, area = None, contains_area = false, allow_deprecated = false))]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_utm_zones()[0]['code']
/// '10286'
pub(crate) fn crs_utm_zones(
    datum_name: Option<String>,
    area: Option<&Bound<'_, PyAny>>,
    contains_area: bool,
    allow_deprecated: bool,
) -> PyResult<Vec<crs::CrsCatalogInfo>> {
    Ok(crs::utm_zones(&crs::UtmCatalogOptions {
        datum_name,
        area: parse_area(area, "area")?,
        contains_area,
        allow_deprecated,
    })?)
}

/// Celestial bodies known to the CRS database.
///
/// Parameters
/// ----------
/// authority : str, optional
///     Restrict to this authority.
///
/// Returns
/// -------
/// list of CrsCelestialBodyInfo
#[pyfunction]
#[pyo3(signature = (*, authority = None))]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_celestial_bodies()[0]['name']
/// '1_Ceres'
pub(crate) fn crs_celestial_bodies(
    authority: Option<&str>,
) -> PyResult<Vec<crs::CelestialBodyInfo>> {
    Ok(crs::celestial_bodies(authority)?)
}

/// List geoid models available for a CRS.
///
/// Parameters
/// ----------
/// value : CRS-like
///     CRS as an EPSG code or authority/WKT string.
///
/// Returns
/// -------
/// list
pub(crate) fn crs_geoid_models(value: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    let crs = crs_normalize(value)?;
    Ok(crs::geoid_models(&crs)?)
}
