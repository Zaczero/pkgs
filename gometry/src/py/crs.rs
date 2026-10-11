//! CRS (coordinate reference system) `PyO3` surface — the `CRS` class plus the
//! `crs_*` introspection / transform / catalog functions. Generic
//! value/coordinate parsing helpers stay in the crate root and are reached via
//! ancestor access.

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyDict, PyList, PyTuple};
use serde_json::Value;

use crate::py::errors::{CRSError, GeometryError};
use crate::{
    Crs, CrsCoordinateArgs, broadcast_coordinate_group, crs, crs_arc, finite_coordinate_required,
    finite_f64_required, json_to_py, parse_area,
};

mod functions;
mod functions_catalog;
mod functions_config;
mod functions_factors;
mod functions_misc;
mod functions_transform;
mod list_cache;
mod parsing;
mod py_crs;
mod pymethods_constructors;
mod pymethods_export;
mod pymethods_introspect;
mod pymethods_operations;
pub(crate) use functions::{
    MinConfidence, crs_ellipsoids, crs_prime_meridians, crs_proj_operations, crs_projjson_mapping,
    crs_same, crs_to_authority, crs_to_cf, crs_to_epsg, crs_to_proj, crs_to_projjson, crs_to_wkt,
    crs_unit, crs_units,
};
pub(crate) use functions_catalog::{
    crs_authorities, crs_catalog, crs_celestial_bodies, crs_codes, crs_geoid_models,
    crs_non_deprecated, crs_utm_zones,
};
pub(crate) use functions_config::{crs_cache_info, crs_clear_cache, crs_configure, crs_reset};
pub(crate) use functions_factors::crs_factors;
pub(crate) use functions_misc::{crs_engine, crs_normalize, crs_parse_required};
pub(crate) use functions_transform::{
    crs_apply, crs_grid, crs_operation, crs_operation_at, crs_operations, crs_roundtrip,
    crs_transform, crs_transform_bounds, drain_accuracy_warning,
};
pub(crate) use parsing::parse_crs_inner;
pub(crate) use py_crs::PyCrs;

pub(crate) use crate::boundary::TransformOptionArgs;

#[expect(
    clippy::used_underscore_items,
    reason = "the versioned pickle entry point is a persistent wire contract"
)]
#[pyfunction]
fn _unpickle_crs_v1(definition: String) -> PyCrs {
    PyCrs::from_definition(Crs::from(definition))
}

/// Register the CRS class and flat `crs_*` functions on the module.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::add_classes!(m; PyCrs);
    crate::add_functions!(m;
        crs_apply, crs_cache_info, crs_clear_cache, crs_configure, crs_ellipsoids,
        crs_engine, crs_grid, _unpickle_crs_v1,
        crs_prime_meridians, crs_proj_operations, crs_reset,
        crs_roundtrip, crs_transform, crs_transform_bounds, crs_unit, crs_units,
        crs_catalog, crs_authorities, crs_codes, crs_utm_zones, crs_celestial_bodies,
    );
    Ok(())
}
