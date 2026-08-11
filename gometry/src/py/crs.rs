//! CRS (coordinate reference system) `PyO3` surface — the `CRS` class plus the
//! `crs_*` introspection / transform / catalog functions. Generic
//! value/coordinate parsing helpers stay in the crate root and are reached via
//! ancestor access.

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyDict, PyInt, PyList, PyModule, PyTuple};
use serde_json::Value;

use crate::py::errors::{AccuracyWarning, CRSError, GeometryError};
use crate::{
    CoordinateInput, Crs, CrsCoordinateArgs, accuracy_option, broadcast_coordinate_group,
    coordinate_epoch_option, coordinate_input, coordinate_input_with_error, coordinate_values, crs,
    crs_arc, finite_coordinate_required, finite_f64_required, json_to_py, parse_area, parse_crs,
};

mod functions;
mod functions_catalog;
mod functions_config;
mod functions_geodesic;
mod functions_misc;
mod functions_transform;
mod list_cache;
mod parsing;
mod py_crs;
mod pymethods_constructors;
mod pymethods_export;
mod pymethods_geodesic;
mod pymethods_introspect;
mod pymethods_operations;
pub(crate) use functions::{
    MinConfidence, crs_ellipsoids, crs_identify, crs_info, crs_prime_meridians,
    crs_proj_operations, crs_same, crs_to_authority, crs_to_epsg, crs_to_proj, crs_to_projjson,
    crs_to_projjson_dict, crs_to_wkt, crs_unit, crs_units,
};
pub(crate) use functions_catalog::{
    crs_authorities, crs_catalog, crs_celestial_bodies, crs_codes, crs_geoid_models,
    crs_non_deprecated, crs_search, crs_utm_zones,
};
pub(crate) use functions_config::{
    crs_cache_info, crs_clear_cache, crs_config, crs_configure, crs_reset,
};
pub(crate) use functions_geodesic::{
    crs_factors, crs_geodesic, crs_geodesic_direct, crs_geodesic_interpolate, crs_to_cf,
};
pub(crate) use functions_misc::{crs_engine, crs_normalize, crs_parse_required};
pub(crate) use functions_transform::{
    TransformOptionArgs, crs_apply, crs_grid, crs_operation, crs_operation_at, crs_operations,
    crs_roundtrip, crs_transform, crs_transform_bounds, drain_accuracy_warning,
};
pub(crate) use parsing::parse_crs_inner;
pub(crate) use py_crs::PyCrs;

/// Register the CRS class and flat `crs_*` functions on the module.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::add_classes!(m; PyCrs);
    crate::add_functions!(m;
        crs_apply, crs_cache_info, crs_clear_cache, crs_config, crs_configure, crs_ellipsoids,
        crs_engine, crs_grid,
        crs_info, crs_prime_meridians, crs_proj_operations, crs_reset,
        crs_roundtrip, crs_transform, crs_transform_bounds, crs_unit, crs_units,
        crs_search, crs_catalog, crs_authorities, crs_codes, crs_utm_zones, crs_celestial_bodies,
    );
    Ok(())
}

pub(crate) fn crs_html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn warn_about_accuracy_degradation(py: Python<'_>) -> PyResult<()> {
    if let Some(message) = crs::take_accuracy_diagnostic() {
        let message = std::ffi::CString::new(message)
            .map_err(|_| GeometryError::new_err("PROJ diagnostic contained a NUL byte"))?;
        PyErr::warn(py, &py.get_type::<AccuracyWarning>(), &message, 1)?;
    }
    Ok(())
}
