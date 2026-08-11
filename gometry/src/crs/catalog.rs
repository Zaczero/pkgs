//! CRS database queries (identify, search, codes, lookup, catalog, units,
//! ellipsoids, ...) and serialization/comparison (WKT, PROJ string, PROJJSON,
//! CF, equality). Thin PROJ-database glue over the FFI wrappers in the parent.

use std::num::NonZeroUsize;

use crate::crs::CrsError;
use crate::error::Result;

/// Maximum CRS search result limit accepted by the search API.
pub(crate) const CRS_SEARCH_MAX_LIMIT: usize = 1_000;

pub(crate) fn validate_search_limit(limit: i64) -> Result<NonZeroUsize> {
    if !(1..=CRS_SEARCH_MAX_LIMIT as i64).contains(&limit) {
        return Err(CrsError::invalid(format!(
            "CRS search limit must be between 1 and {CRS_SEARCH_MAX_LIMIT}, got {limit}"
        )));
    }
    Ok(NonZeroUsize::new(limit as usize).expect("range check excludes zero"))
}

mod cf;
mod export;
mod identify;
mod lookup;
mod units;

pub(crate) use cf::{
    CF_TRANSVERSE_MERCATOR_PARAMETERS, add_cf_lambert_azimuthal_equal_area_parameters,
    add_cf_lambert_conformal_conic_parameters, add_cf_lambert_cylindrical_equal_area_parameters,
    add_cf_mercator_parameters, add_cf_polar_stereographic_parameters,
    add_cf_projection_parameters, is_operation_method, is_transverse_mercator,
};
pub(crate) use export::{
    ProjStringVersion, cached_export, crs_to_2d_export, crs_to_3d_export, ellipsoids,
    prime_meridians, proj_operations, same, to_proj, to_projjson, to_projjson_with_options,
    to_wkt_with_options,
};
#[cfg(test)]
pub(crate) use identify::authority_matches;
pub(crate) use identify::{
    identify, non_deprecated, search, to_2d, to_3d, to_authority, to_cf, to_epsg,
};
pub(crate) use lookup::{authorities, catalog, celestial_bodies, codes, geoid_models};
pub(crate) use units::{unit_info, units, utm_zones};
