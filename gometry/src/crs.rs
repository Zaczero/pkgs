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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{OnceLock, RwLock};

use geographiclib_rs::Geodesic;

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

use cache::{
    CRS_AUTHORITY_MATCH_CACHE, CRS_AUTHORITY_MATCH_CACHE_CAPACITY, CRS_CANONICAL_CACHE,
    CRS_CANONICAL_CACHE_CAPACITY, CRS_CATALOG_CACHE, CRS_CATALOG_CACHE_CAPACITY,
    CRS_CELESTIAL_BODIES_CACHE, CRS_CELESTIAL_BODIES_CACHE_CAPACITY, CRS_COMPARISON_CACHE,
    CRS_COMPARISON_CACHE_CAPACITY, CRS_EXPORT_CACHE, CRS_EXPORT_CACHE_CAPACITY, CRS_FACTOR_CACHE,
    CRS_FACTOR_CACHE_CAPACITY, CRS_GEODESIC_CACHE, CRS_GEODESIC_CACHE_CAPACITY,
    CRS_IN_CORE_DESCRIPTOR_CACHE, CRS_IN_CORE_DESCRIPTOR_CACHE_CAPACITY, CRS_INFO_CACHE,
    CRS_INFO_CACHE_CAPACITY, CRS_OPERATIONS_CACHE, CRS_OPERATIONS_CACHE_CAPACITY, CRS_SEARCH_CACHE,
    CRS_SEARCH_CACHE_CAPACITY, CRS_UNITS_CACHE, CRS_UNITS_CACHE_CAPACITY, CachedAuthorityObjects,
    CachedCelestialBodies, CachedCrsAuthorityMatches, CachedCrsCatalog, CachedCrsComparison,
    CachedCrsExport, CachedCrsInfo, CachedCrsOperations, CachedCrsSearch, CachedGeodesicObject,
    CachedProjectionFactorsObject, CachedUnits, CrsComparisonKey, CrsExportKey, CrsExportKind,
    NON_DEPRECATED_CACHE, NON_DEPRECATED_CACHE_CAPACITY, begin_transform_observation,
    ensure_thread_caches_current, record_transform_engine, with_proj_diagnostic_pipeline,
    with_proj_operation, with_proj_pipeline,
};
pub(crate) use cache::{cache_info, lru_resolve};
pub(crate) use catalog::{
    ProjStringVersion, authorities, catalog, celestial_bodies, codes, ellipsoids, geoid_models,
    identify, non_deprecated, prime_meridians, proj_operations, same, search, to_2d, to_3d,
    to_authority, to_cf, to_epsg, to_proj, to_projjson, to_projjson_with_options,
    to_wkt_with_options, unit_info, units, utm_zones, validate_search_limit,
};
use context::{ProjContext, with_proj_context};
pub(crate) use error::CrsError;
pub(crate) use geodesic::{
    AngleUnit, DistanceMode, EllipsoidMetric, EllipsoidShape, MetricModel, ResolvedMetric,
    ellipsoid_shape, ensure_3d_metric, ensure_geographic_columns, ensure_geographic_domain,
    ensure_geographic_lonlat, geodesic_bearing, geodesic_bearing_crs, geodesic_cross_track_crs,
    geodesic_cross_track_radius_crs, geodesic_cross_track_with_radius, geodesic_destination,
    geodesic_destination_crs, geodesic_direct, geodesic_direct_columns, geodesic_for_crs,
    geodesic_frechet, geodesic_hausdorff, geodesic_interpolate_on_line_const,
    geodesic_interpolates, geodesic_inverse, geodesic_inverses, geodesic_length,
    geodesic_line_interpolate, geodesic_line_interpolate_coordseq, geodesic_line_length_columns,
    geodesic_line_locate, geodesic_line_locate_coordseq, geodesic_line_solution_const,
    geodesic_line_substring, geodesic_line_substring_coordseq, geodesic_point_between,
    geodesic_point_distances, geodesic_point_pair_distances, geodesic_point_pair_dwithin,
    geodesic_ring_measure_columns, geodesic_shape_area, geodesic_shape_length,
    interpolate_optional_ordinate, metric_model, metric_model_meters, with_ellipsoid_metric,
    with_geodesic_coordseq_collect_rows, with_geodesic_coordseq_metric,
    with_resolved_ellipsoid_metric,
};
pub(crate) use grids::grid_info;
use in_core::{
    InCoreTransform, in_core_xy_op, lonlat_to_web_mercator_xy, transform_in_core_bounds,
    transform_in_core_bounds_3d, transform_in_core_xy_batch, try_in_core_transform, utm_crs,
    web_mercator_to_lonlat_xy,
};
pub(crate) use info::{
    AngularUnit, engine_info, ensure_same_geodesic_len, factor_columns, factors, info, is_dynamic,
    operation_info, operation_info_at,
};
pub(crate) use introspect::Confidence;
use introspect::{
    area_of_use, authority_object_info, axes, axis_role, compound_axis_metadata,
    coordinate_system_type, create_crs_transform_object, crs_type_name, datum_info, domain_infos,
    ellipsoid_info, exported_owned_crs, id_authority, operation_info_from_pj,
    owned_authority_object_info, owned_crs_coordinate_operation_info, prime_meridian_info,
    split_authority, sub_crs_infos, validate_min_confidence,
};
pub(crate) use local::{LOCAL_SCALE_ERROR_LIMIT, estimate_local_crs, local_crs_fits};
pub(crate) use operations::{operations_info, roundtrip_errors};
pub(crate) use options_impls::c_option_ptrs;
use proj::{
    OwnedCelestialBodyList, OwnedCrsInfoList, OwnedPj, OwnedProjStringList, OwnedUnitList,
    ProjArea, ProjCrsListParameters, ProjIntList, ProjObjList, ProjObject,
    ProjOperationFactoryContext, ProjPipeline, ProjTransformOptions, copy_proj_c_string,
    first_static_string_from_ptr, optional_c_string, proj_context_error_message,
    proj_error_message, take_proj_string_list,
};
pub(crate) use rhumb::{rhumb_bearing_crs, rhumb_destination_crs, rhumb_distance_crs};
pub(crate) use runtime::{
    begin_accuracy_diagnostics, canonicalize, clear_cache, configure_runtime,
    coordinate_identity_crs, cstring, normalize, normalize_pair, reset_runtime_config,
    runtime_config, runtime_config_generation, take_accuracy_diagnostic,
};
pub(crate) use transform::{
    apply_operation, coordinates_are_finite, is_geographic_crs, is_wgs84_lonlat, parse_epsg,
    proj_transform_error, transform, transform_bounds, transform_bounds_3d,
    transform_bounds_3d_many, transform_bounds_many, transform_coordinates, transform_proj_shapes,
    validate_coordinate_lanes,
};
pub(crate) use transformer::Transformer;
pub(crate) use types::{
    AreaOfInterest, AreaOfUse, AuthorityObjectInfo, AxisInfo, CacheBucketInfo, CacheInfo,
    CelestialBodyInfo, CfValue, CrsCatalogInfo, CrsCatalogOptions, CrsComparison,
    CrsCoordinateOperationInfo, CrsInfo, CrsObjectKind, CrsProjJsonOptions, CrsProjOptions,
    CrsSearchOptions, CrsWktOptions, DatumInfo, DomainInfo, EllipsoidCatalogInfo, EllipsoidInfo,
    EngineInfo, GeodesicDirectColumns, GeodesicDirectInfo, GeodesicInterpolateInfo,
    GeodesicInverseInfo, GridDatabaseInfo, GridInfo, IdentifyCandidate, MethodInfo, OperationInfo,
    OperationParameterInfo, PrimeMeridianCatalogInfo, PrimeMeridianInfo, ProjDirection,
    ProjOperationCatalogInfo, ProjectionFactorColumns, ProjectionFactors, RuntimeConfig,
    TransformOptions, UnitInfo, UtmCatalogOptions, WktAxisRule, WktVersion,
};

const GOLDEN_RATIO: f64 = 0.618_033_988_749_894_9;
const GOLDEN_SECTION_TOLERANCE_METRES: f64 = 1e-3;
const WEB_MERCATOR_RADIUS: f64 = 6_378_137.0;
const WGS84_A: f64 = 6_378_137.0;
const WGS84_F: f64 = 1.0 / 298.257_223_563;
const UTM_K0: f64 = 0.9996;
const TRANSFORM_BOUNDS_MAX_DENSIFY: u32 = 10_000;
const DEGREE_TO_RADIAN: f64 = std::f64::consts::PI / 180.0;
