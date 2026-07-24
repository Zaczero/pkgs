#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! CRS (coordinate reference system) `PyO3` surface — the `CRS` class plus the
//! `crs_*` introspection / transform / catalog functions. Generic
//! value/coordinate parsing helpers stay in the crate root and are reached via
//! ancestor access.

use std::sync::OnceLock;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyDict, PyInt, PyList, PyModule, PyTuple};
use serde_json::Value;

use crate::py::errors::{CRSError, GeometryError};
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
mod parsing;
mod py_crs;
mod pymethods_constructors;
mod pymethods_export;
mod pymethods_geodesic;
mod pymethods_introspect;
mod pymethods_operations;
pub(crate) use functions::*;
pub(crate) use functions_catalog::*;
pub(crate) use functions_config::*;
pub(crate) use functions_geodesic::*;
pub(crate) use functions_misc::*;
pub(crate) use functions_transform::*;
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
