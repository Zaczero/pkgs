#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::num::NonZeroU32;

use smol_str::SmolStr;

use crate::*;

pub(crate) type Crs = SmolStr;

pub(crate) const QUADRANT_SEGMENTS_DEFAULT: NonZeroU32 = NonZeroU32::new(8).unwrap();
pub(crate) const QUADRANT_SEGMENTS_DEFAULT_I64: i64 = 8;
const _: () = assert!(QUADRANT_SEGMENTS_DEFAULT.get() == QUADRANT_SEGMENTS_DEFAULT_I64 as u32);

/// Cached `gometry._types.Extremes` constructor — `extremes()` runs in
/// user loops, so the per-call module import + attribute lookup is hoisted
/// into a process-wide cell.
pub(crate) fn extreme_points_type(py: Python<'_>) -> PyResult<&Bound<'_, pyo3::types::PyType>> {
    static CELL: pyo3::sync::PyOnceLock<Py<pyo3::types::PyType>> = pyo3::sync::PyOnceLock::new();
    CELL.import(py, "gometry._types", "Extremes")
}

/// Cached `gometry._types.PolygonizeResult` constructor (see
/// [`extreme_points_type`]).
pub(crate) fn polygonize_result_type(py: Python<'_>) -> PyResult<&Bound<'_, pyo3::types::PyType>> {
    static CELL: pyo3::sync::PyOnceLock<Py<pyo3::types::PyType>> = pyo3::sync::PyOnceLock::new();
    CELL.import(py, "gometry._types", "PolygonizeResult")
}

/// Cached `gometry._types.Features` constructor returned by the native
/// `from_features` boundary.
pub(crate) fn features_type(py: Python<'_>) -> PyResult<&Bound<'_, pyo3::types::PyType>> {
    static CELL: pyo3::sync::PyOnceLock<Py<pyo3::types::PyType>> = pyo3::sync::PyOnceLock::new();
    CELL.import(py, "gometry._types", "Features")
}

pub(crate) fn gometry_lib_module(py: Python<'_>) -> PyResult<&Bound<'_, pyo3::types::PyModule>> {
    static CELL: pyo3::sync::PyOnceLock<Py<pyo3::types::PyModule>> = pyo3::sync::PyOnceLock::new();
    CELL.get_or_try_init(py, || py.import("gometry._lib").map(Bound::unbind))
        .map(|module| module.bind(py))
}

/// A 3D bounding box: ``(minx, miny, minz, maxx, maxy, maxz)``.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Bounds3D {
    pub minx: f64,
    pub miny: f64,
    pub minz: f64,
    pub maxx: f64,
    pub maxy: f64,
    pub maxz: f64,
}

impl Bounds3D {
    pub(crate) const fn into_tuple(self) -> (f64, f64, f64, f64, f64, f64) {
        (
            self.minx, self.miny, self.minz, self.maxx, self.maxy, self.maxz,
        )
    }
}

impl From<(f64, f64, f64, f64, f64, f64)> for Bounds3D {
    fn from((minx, miny, minz, maxx, maxy, maxz): (f64, f64, f64, f64, f64, f64)) -> Self {
        Self {
            minx,
            miny,
            minz,
            maxx,
            maxy,
            maxz,
        }
    }
}

/// Maximum number of geometries rendered in a `GeometryArray` HTML preview.
pub(crate) const SVG_ARRAY_PREVIEW: usize = 12;

/// Finest H3 resolution (cells span ``0..=H3_MAX_RESOLUTION``).
pub(crate) const H3_MAX_RESOLUTION: u8 = 15;

mod bounds_index;
mod constructive;
mod extract;
mod frame_args;
mod geojson;
mod io_encoders;
mod linref;
mod nearest;
mod tokens;
mod validate;

pub(crate) use bounds_index::*;
pub(crate) use constructive::*;
pub(crate) use extract::*;
pub(crate) use frame_args::*;
pub(crate) use geojson::*;
pub(crate) use io_encoders::*;
pub(crate) use linref::*;
pub(crate) use nearest::*;
pub(crate) use tokens::*;
pub(crate) use validate::*;

/// Register a list of `#[pyclass]` types on the module in one statement.
macro_rules! add_classes {
    ($m:ident; $($class:ty),+ $(,)?) => {
        $($m.add_class::<$class>()?;)+
    };
}
pub(crate) use add_classes;

/// Register a list of `#[pyfunction]`s on the module in one statement.
macro_rules! add_functions {
    ($m:ident; $($function:path),+ $(,)?) => {
        $($m.add_function(pyo3::wrap_pyfunction!($function, $m)?)?;)+
    };
}
pub(crate) use add_functions;
