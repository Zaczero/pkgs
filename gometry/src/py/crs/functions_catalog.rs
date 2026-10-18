use std::collections::HashSet;

use crate::py::crs::{
    Bound, CRSError, PyAny, PyList, PyResult, Python, crs, crs_normalize, parse_area, pyfunction,
};

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
pub(crate) fn crs_authorities(py: Python<'_>) -> PyResult<Bound<'_, pyo3::types::PyList>> {
    super::list_cache::authorities_py_list(py)
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
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_codes('EPSG')[:3]
/// ['10150', '10151', '10156']
#[pyfunction]
#[pyo3(signature = (authority, *, kind = None, allow_deprecated = false))]
pub(crate) fn crs_codes(
    py: Python<'_>,
    authority: &str,
    kind: Option<&str>,
    allow_deprecated: bool,
) -> PyResult<Vec<String>> {
    Ok(py.detach(|| crs::codes(authority, kind, allow_deprecated))?)
}

/// Non-deprecated replacements for a deprecated CRS.
///
/// Parameters
/// ----------
/// crs : CRS-like
///     Any supported CRS-like input.
///
/// Returns
/// -------
/// list
pub(crate) fn crs_non_deprecated(
    value: &Bound<'_, PyAny>,
) -> PyResult<Vec<crs::AuthorityObjectInfo>> {
    let definition = crs_normalize(value)?;
    Ok(value.py().detach(|| crs::non_deprecated(&definition))?)
}

/// Catalog of CRS in the database matching the given filters.
///
/// Parameters
/// ----------
/// name : str, optional
///     Match CRS names using PROJ's database name matcher.
/// authority : str, optional
///     Registry authority to search (default ``"EPSG"``).
/// kind : str, optional
///     Restrict to a CRS kind.
/// area_of_interest : sequence of float, dict, or object, optional
///     Filter area as ``(west, south, east, north)`` in DEGREES, an area
///     mapping, or an AreaOfInterest-like object.
/// contains_area_of_interest : bool, optional
///     Require the CRS area to contain ``area_of_interest``
///     (default ``False``).
/// allow_deprecated : bool, optional
///     Include deprecated CRS (default ``False``).
/// celestial_body : str, optional
///     Restrict to this celestial body.
/// approximate : bool, default False
///     Allow approximate matching when ``name`` is supplied.
/// limit : int, optional
///     Maximum number of returned records; omitted means no result limit.
///
/// Returns
/// -------
/// list of CrsCatalogInfo
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> any(item['crs'] == 'EPSG:4326' for item in gm.crs_catalog(name='WGS 84'))
/// True
#[pyfunction]
#[pyo3(signature = (name = None, *, authority = None, kind = None, area_of_interest = None, contains_area_of_interest = false, allow_deprecated = false, celestial_body = None, approximate = false, limit = None))]
pub(crate) fn crs_catalog(
    py: Python<'_>,
    name: Option<&str>,
    authority: Option<&str>,
    kind: Option<&str>,
    area_of_interest: Option<&Bound<'_, PyAny>>,
    contains_area_of_interest: bool,
    allow_deprecated: bool,
    celestial_body: Option<String>,
    approximate: bool,
    limit: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<crs::CrsCatalogInfo>> {
    if matches!(authority, Some("")) {
        return Err(CRSError::new_err(
            "CRS catalog authority must be a non-empty string",
        ));
    }
    let limit = limit
        .map(crate::strict_i64)
        .transpose()?
        .map(|limit| {
            usize::try_from(limit)
                .ok()
                .filter(|limit| *limit > 0)
                .ok_or_else(|| {
                    CRSError::new_err(format!(
                        "CRS catalog limit must be a positive integer, got {limit}"
                    ))
                })
        })
        .transpose()?;
    let kind = kind
        .map(|kind| crs::CrsObjectKind::parse_crs(Some(kind)))
        .transpose()?;
    let options = crs::CrsCatalogOptions {
        kind,
        area: parse_area(area_of_interest, "area_of_interest")?,
        contains_area: contains_area_of_interest,
        allow_deprecated,
        celestial_body,
    };
    let state = crs::runtime_state_snapshot()?;
    Ok(py.detach(|| -> crate::error::Result<_> {
        let mut items = crs::catalog_with_state(authority, &options, &state)?;
        if let Some(name) = name {
            if name.trim().is_empty() {
                return Err(crs::CrsError::message(
                    "CRS catalog name must be a non-empty string",
                ));
            }
            let name_options = crs::CrsNameMatchOptions {
                authority: Some(authority.unwrap_or("EPSG").to_owned()),
                kind,
                approximate,
            };
            let matches = crs::catalog_name_matches_with_state(name, &name_options, &state)?;
            let matches: HashSet<_> = matches.into_iter().collect();
            items.retain(|item| matches.contains(&item.crs));
        }
        if let Some(limit) = limit {
            items.truncate(limit);
        }
        Ok(items)
    })?)
}

/// UTM-zone CRS from the catalog.
///
/// Parameters
/// ----------
/// datum_name : str, optional
///     Restrict to projected CRSs whose displayed name starts with this datum
///     prefix followed by ``" / UTM zone "``. Matching is ASCII
///     case-insensitive, but spaces and punctuation are significant. For
///     example, use ``"WGS 84"``, ``"NAD83(CSRS)"``, or
///     ``"ETRS89/DREF91/2016"``. Only CRSs named ``"UTM zone"`` are
///     included; arbitrary substrings and long datum descriptions do not
///     match.
/// area_of_interest : sequence of float, dict, or object, optional
///     Filter area as ``(west, south, east, north)`` in DEGREES, an area
///     mapping, or an AreaOfInterest-like object.
/// contains_area_of_interest : bool, optional
///     Require the zone area to contain ``area_of_interest``
///     (default ``False``).
/// allow_deprecated : bool, optional
///     Include deprecated zones (default ``False``).
///
/// Returns
/// -------
/// list of CrsCatalogInfo
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_utm_zones()[0]['code']
/// '10286'
#[pyfunction]
#[pyo3(signature = (*, datum_name = None, area_of_interest = None, contains_area_of_interest = false, allow_deprecated = false))]
pub(crate) fn crs_utm_zones(
    py: Python<'_>,
    datum_name: Option<String>,
    area_of_interest: Option<&Bound<'_, PyAny>>,
    contains_area_of_interest: bool,
    allow_deprecated: bool,
) -> PyResult<Vec<crs::CrsCatalogInfo>> {
    let options = crs::UtmCatalogOptions {
        datum_name,
        area: parse_area(area_of_interest, "area_of_interest")?,
        contains_area: contains_area_of_interest,
        allow_deprecated,
    };
    Ok(py.detach(|| crs::utm_zones(&options))?)
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
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.crs_celestial_bodies()[0]['name']
/// '1_Ceres'
#[pyfunction]
#[pyo3(signature = (*, authority = None))]
pub(crate) fn crs_celestial_bodies<'py>(
    py: Python<'py>,
    authority: Option<&str>,
) -> PyResult<Bound<'py, PyList>> {
    // Warm path: isolation-copy the generation-stamped cached record list.
    super::list_cache::celestial_bodies_py_list(py, authority)
}

/// List geoid models available for a CRS.
///
/// Parameters
/// ----------
/// value : CRS-like
///     Any supported CRS-like input.
///
/// Returns
/// -------
/// list
pub(crate) fn crs_geoid_models(value: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    let crs = crs_normalize(value)?;
    Ok(value.py().detach(|| crs::geoid_models(&crs))?)
}
