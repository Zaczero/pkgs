//! CRS engine — libPROJ-backed transforms, introspection, catalog queries,
//! geodesy, and in-core fast paths.
//!
//! This module is the seam only: submodule declarations, the re-exported
//! public API, and the handful of shared constants. The substrate lives in
//! the children — `proj` (FFI handles + string/error utilities), `introspect`
//! (raw-pointer metadata readers), `cache` (thread-local LRU machinery),
//! `runtime` (global PROJ runtime config), `pipeline`/`transformer`/
//! `transform` (the transform engine), `in_core` (closed-form Web
//! Mercator/UTM), `catalog`/`info`/`operations`/`grids` (database queries),
//! `geodesic` (ellipsoidal math), and `types`/`options_impls`/`error` (DTOs,
//! their impls, and the error model). Children reach shared items via
//! `use super::*`.

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::{OnceLock, RwLock};

use geographiclib_rs::{Geodesic, GeodesicLine};

use crate::geometry::{CoordSeq, GeometryErrorKind, Point, Shape};

// `proj_env` + `context` before `proj`: typed ProjContext owner is path-
// includable by compile-fail fixtures; proj attaches errno and sibling handles.
mod context;
mod proj_env;
// `proj` so `#[macro_use]` makes `proj_c_string!` available to siblings.
#[macro_use]
mod proj;
mod cache;
mod catalog;
mod error;
mod geodesic;
mod grids;
mod in_core;
mod info;
mod introspect;
mod local;
mod operations;
mod options_impls;
mod pipeline;
mod proj_object;
mod rhumb;
mod runtime;
mod transform;
mod transformer;
mod types;

#[cfg(test)]
pub(crate) static RUNTIME_STATE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

use cache::{
    CRS_AUTHORITY_MATCH_CACHE, CRS_AUTHORITY_MATCH_CACHE_CAPACITY, CRS_CATALOG_CACHE,
    CRS_CATALOG_CACHE_CAPACITY, CRS_CELESTIAL_BODIES_CACHE, CRS_CELESTIAL_BODIES_CACHE_CAPACITY,
    CRS_COMPARISON_CACHE, CRS_COMPARISON_CACHE_CAPACITY, CRS_EXPORT_CACHE,
    CRS_EXPORT_CACHE_CAPACITY, CRS_FACTOR_CACHE, CRS_FACTOR_CACHE_CAPACITY, CRS_GEODESIC_CACHE,
    CRS_GEODESIC_CACHE_CAPACITY, CRS_IN_CORE_DESCRIPTOR_CACHE,
    CRS_IN_CORE_DESCRIPTOR_CACHE_CAPACITY, CRS_INFO_CACHE, CRS_INFO_CACHE_CAPACITY,
    CRS_OPERATIONS_CACHE, CRS_OPERATIONS_CACHE_CAPACITY, CRS_SEARCH_CACHE,
    CRS_SEARCH_CACHE_CAPACITY, CRS_UNITS_CACHE, CRS_UNITS_CACHE_CAPACITY, CachedAuthorityObjects,
    CachedCelestialBodies, CachedCrsAuthorityMatches, CachedCrsCatalog, CachedCrsComparison,
    CachedCrsExport, CachedCrsInfo, CachedCrsOperations, CachedCrsSearch, CachedGeodesicObject,
    CachedProjectionFactorsObject, CachedUnits, CrsComparisonKey, CrsExportKey, CrsExportKind,
    GEODESIC_LINE_CACHE, NON_DEPRECATED_CACHE, NON_DEPRECATED_CACHE_CAPACITY,
    begin_transform_observation, ensure_thread_caches_current, record_transform_engine,
    with_proj_diagnostic_pipeline_at, with_proj_operation_at, with_proj_pipeline_at,
    with_proj_pipeline_indexed_at,
};
pub(crate) use cache::{cache_info, lru_insert, lru_promote, lru_resolve};
pub(crate) use catalog::{
    CF_LINEAR_INTERNATIONAL_FOOT, CF_LINEAR_METRE, CF_LINEAR_US_SURVEY_FOOT, ProjStringVersion,
    authorities_with_state, catalog, celestial_bodies_with_state, codes, ellipsoids, geoid_models,
    identify_with_state, non_deprecated, prime_meridians, proj_operations, same, search, to_2d,
    to_3d, to_authority, to_cf, to_epsg, to_proj, to_projjson, to_projjson_with_options,
    to_projjson_with_options_at, to_wkt_with_options, unit_info, units_with_state, utm_zones,
    validate_search_limit,
};
use context::{ProjContext, with_proj_context, with_proj_context_at};
pub(crate) use error::{CrsError, preserve_resource_error};
#[cfg(test)]
pub(crate) use geodesic::geodesic_counters;
#[cfg(test)]
pub(crate) use geodesic::with_ellipsoid_metric;
pub(crate) use geodesic::{
    DistanceMode, EllipsoidMetric, EllipsoidShape, GeodesicCalculation, GeodesicLineKey,
    GeodesicMetric, MetricModel, ResolvedMetric, chart_line_length_columns, chart_shape_area,
    chart_shape_length, ellipsoid_shape_at, ensure_3d_metric, ensure_geographic_columns_in_frame,
    ensure_geographic_domain_in_frame, ensure_geographic_lonlat, ensure_geographic_lonlat_in_frame,
    finite, geodesic_bearing, geodesic_cross_track_crs_at, geodesic_cross_track_radius_crs_at,
    geodesic_cross_track_with_radius, geodesic_destination, geodesic_destination_crs_at,
    geodesic_for_crs_at, geodesic_frechet_at_with_scratch, geodesic_hausdorff_at,
    geodesic_interpolate_on_line, geodesic_line_interpolate, geodesic_line_interpolate_coordseq,
    geodesic_line_locate, geodesic_line_locate_coordseq, geodesic_line_solution,
    geodesic_line_substring, geodesic_line_substring_coordseq, geodesic_point_between,
    geodesic_point_distances, geodesic_point_pair_distance, geodesic_point_pair_distances,
    geodesic_point_pair_dwithin, geodesic_point_pair_dwithin_one, geodesic_witness_distance_at,
    geographic_lonlat_result, metric_model, metric_model_meters, sincos_degrees,
    with_ellipsoid_metric_at, with_geodesic_cache_at, with_geodesic_coordseq_collect_rows_at,
    with_geodesic_coordseq_metric_at, with_resolved_ellipsoid_metric_at,
};
pub(crate) use grids::grid_info;
use in_core::{
    InCoreTransform, transform_in_core_xy_batch, transform_in_core_xy_batch_indexed,
    try_in_core_transform_with_state, utm_crs,
};
pub(crate) use info::{
    AngularUnit, engine_info_with_state, factor_columns, factors, factors_at, info,
    info_with_state, is_dynamic, is_dynamic_with_state, operation_info_at,
    operation_info_with_state,
};
pub(crate) use introspect::Confidence;
use introspect::{
    CompoundAxisMetadata, area_of_use, authority_object_info, axes, axis_role,
    compound_axis_metadata, coordinate_system_type, create_crs_transform_object, crs_type_name,
    datum_admits_geometry_epoch, datum_info, domain_infos, ellipsoid_info, exported_owned_crs,
    geographic_frame, id_authority, live_crs_admits_geometry_epoch, operation_info_from_pj,
    owned_authority_object_info, owned_crs_coordinate_operation_info, prime_meridian_info,
    split_authority, sub_crs_infos, validate_min_confidence,
};
pub(crate) use local::{
    LOCAL_SCALE_ERROR_LIMIT, LocalCrsDefinition, estimate_local_crs, estimate_local_crs_at,
    local_crs_fits_at,
};
pub(crate) use operations::{operations_info_with_state, roundtrip_errors};
pub(crate) use options_impls::c_option_ptrs;
use proj::{
    OwnedCelestialBodyList, OwnedCrsInfoList, OwnedPj, OwnedProjStringList, OwnedUnitList,
    ProjArea, ProjCrsListParameters, ProjIntList, ProjObjList, ProjObject,
    ProjOperationFactoryContext, ProjPipeline, ProjTransformOptions, copy_proj_c_string,
    first_static_string_from_ptr, optional_c_string, proj_context_error_message,
    proj_error_message, take_proj_string_list,
};
#[cfg(test)]
pub(crate) use rhumb::rhumb_distance_crs;
pub(crate) use rhumb::{rhumb_bearing_crs_at, rhumb_destination_crs_at, rhumb_distance_crs_at};
pub(crate) use runtime::{
    clear_cache, configure_runtime, coordinate_identity_crs, cstring, definition, normalize,
    normalize_pair, reset_runtime_config, runtime_state_snapshot,
};
pub(crate) use transform::{
    apply_operation, coordinates_are_finite, is_wgs84_lonlat, parse_epsg, proj_transform_error,
    transform, transform_at, transform_bounds, transform_bounds_3d, transform_bounds_3d_many,
    transform_bounds_many, transform_coordinates, validate_coordinate_lanes,
};
pub(crate) use transformer::TransformPlan;
pub(crate) use types::{
    AreaOfInterest, AreaOfUse, AuthorityObjectInfo, AxisInfo, CacheBucketInfo, CacheInfo,
    CelestialBodyInfo, CfValue, CrsCatalogInfo, CrsCatalogOptions, CrsComparison,
    CrsCoordinateOperationInfo, CrsInfo, CrsObjectKind, CrsProjJsonOptions, CrsProjOptions,
    CrsSearchOptions, CrsWktOptions, DatumInfo, DomainInfo, EllipsoidCatalogInfo, EllipsoidInfo,
    EngineInfo, GeodesicDirectInfo, GridDatabaseInfo, GridInfo, IdentifyCandidate, MethodInfo,
    OperationInfo, OperationParameterInfo, PrimeMeridianCatalogInfo, PrimeMeridianInfo,
    ProjDirection, ProjOperationCatalogInfo, ProjectionFactorColumns, ProjectionFactors,
    RuntimeConfig, RuntimeState, TransformExecution, TransformFailure, TransformOptions, UnitInfo,
    UtmCatalogOptions, WktAxisRule, WktVersion,
};

const WEB_MERCATOR_RADIUS: f64 = 6_378_137.0;
const WGS84_A: f64 = 6_378_137.0;
const WGS84_F: f64 = 1.0 / 298.257_223_563;
const UTM_K0: f64 = 0.9996;
const TRANSFORM_BOUNDS_MAX_DENSIFY: u32 = 10_000;
pub(crate) const DEGREE_TO_RADIAN: f64 = std::f64::consts::PI / 180.0;
