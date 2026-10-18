//! CRS database queries (search, codes, lookup, catalog, units,
//! ellipsoids, ...) and serialization/comparison (WKT, PROJ string, PROJJSON,
//! CF, equality). Thin PROJ-database glue over the FFI wrappers in the parent.

/// Metres per CF projected linear-unit token `m` (EPSG:9001).
pub(crate) const CF_LINEAR_METRE: f64 = 1.0;
/// Metres per CF projected linear-unit token `ft` (EPSG:9002, international foot).
pub(crate) const CF_LINEAR_INTERNATIONAL_FOOT: f64 = 0.3048;
/// Metres per CF projected linear-unit token `us-ft`.
///
/// EPSG:9003 / PROJ `+units=us-ft` is the ratio 1200/3937. Shared by CF export
/// admission and CF parse false-easting scale.
pub(crate) const CF_LINEAR_US_SURVEY_FOOT: f64 = 1200.0 / 3937.0;

mod cf;
mod export;
mod identify;
mod lookup;
mod units;

pub(crate) use cf::{
    CF_TRANSVERSE_MERCATOR_PARAMETERS, add_cf_lambert_azimuthal_equal_area_parameters,
    add_cf_lambert_conformal_conic_1sp_parameters, add_cf_lambert_conformal_conic_parameters,
    add_cf_lambert_cylindrical_equal_area_parameters, add_cf_mercator_variant_a_parameters,
    add_cf_mercator_variant_b_parameters, add_cf_polar_stereographic_parameters,
    add_cf_projection_parameters, cf_lcc_1sp_admits_mapping, cf_mercator_b_has_scale,
    cf_prime_meridian_longitude_degrees, cf_projected_axis_to_metre, is_operation_method,
    is_transverse_mercator,
};
#[cfg(test)]
pub(crate) use export::to_wkt_with_options;
pub(crate) use export::{
    ProjStringVersion, ellipsoids, prime_meridians, proj_operations, same, same_if_cached,
    same_with_state, to_projjson, to_projjson_with_options_at,
};
#[cfg(test)]
pub(crate) use identify::authority_matches;
pub(crate) use identify::{
    AuthorityLookup, authority_if_cached, catalog_name_matches_with_state, identify_with_state,
    non_deprecated, to_authority_with_state, to_cf,
};
pub(crate) use lookup::{
    authorities_with_state, catalog_with_state, celestial_bodies_with_state, codes, geoid_models,
};
pub(crate) use units::{unit_info, units_with_state, utm_zones};
