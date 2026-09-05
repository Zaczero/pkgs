//! CRS database queries (search, codes, lookup, catalog, units,
//! ellipsoids, ...) and serialization/comparison (WKT, PROJ string, PROJJSON,
//! CF, equality). Thin PROJ-database glue over the FFI wrappers in the parent.

use std::num::NonZeroUsize;

use crate::crs::CrsError;
use crate::error::Result;

/// Maximum CRS search result limit accepted by the search API.
pub(crate) const CRS_SEARCH_MAX_LIMIT: usize = 1_000;

/// Metres per CF projected linear-unit token `m` (EPSG:9001).
pub(crate) const CF_LINEAR_METRE: f64 = 1.0;
/// Metres per CF projected linear-unit token `ft` (EPSG:9002, international foot).
pub(crate) const CF_LINEAR_INTERNATIONAL_FOOT: f64 = 0.3048;
/// Metres per CF projected linear-unit token `us-ft`.
///
/// EPSG:9003 / PROJ `+units=us-ft` is the ratio 1200/3937. Shared by CF export
/// admission and CF parse false-easting scale.
pub(crate) const CF_LINEAR_US_SURVEY_FOOT: f64 = 1200.0 / 3937.0;

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
    add_cf_projection_parameters, cf_prime_meridian_longitude_degrees, cf_projected_axis_to_metre,
    is_operation_method, is_transverse_mercator,
};
pub(crate) use export::{
    ProjStringVersion, cached_export, ellipsoids, prime_meridians, proj_operations, same, to_proj,
    to_projjson, to_projjson_with_options, to_projjson_with_options_at, to_wkt_with_options,
};
#[cfg(test)]
pub(crate) use identify::authority_matches;
pub(crate) use identify::{
    identify_with_state, non_deprecated, search, to_2d, to_3d, to_authority,
    to_authority_with_state, to_cf, to_epsg,
};
pub(crate) use lookup::{
    authorities_with_state, catalog, catalog_with_state, celestial_bodies_with_state, codes,
    geoid_models,
};
pub(crate) use units::{unit_info, units_with_state, utm_zones, utm_zones_at};
