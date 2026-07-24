#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![feature(once_cell_try, portable_simd)]

mod boundary;
pub(crate) use boundary::*;
mod collections;
mod crs;
mod curves;
mod error;
mod geometry;
#[macro_use]
mod heap_size;
pub(crate) use heap_size::HeapSize;
#[macro_use]
mod pymethod_macros;
mod io;
mod measures;
mod numeric;
pub(crate) use numeric::*;
mod text;
mod tokens;
pub(crate) use measures::*;
mod array;
pub(crate) use array::{
    GeometryArrayStorage, PackedColumnError, PointRows, PyGeometryArray, ShapeRow,
};
mod broadcast;
pub(crate) use broadcast::*;
mod dispatch;
pub(crate) use py::classes::{
    CoordinateReplacement, PyCoordinates, PyCoordinatesIter, PyGeometry, PyGeometryCollection,
    PyGeometryParts, PyGeometryPartsIter, PyLineString, PyMultiLineString, PyMultiPoint,
    PyMultiPolygon, PyPoint, PyPolygon, Typed, get_coordinates, map_coordinates_callback,
    parse_coordinate_replacement, replace_shape_coordinates, slice_replacement_for_shape,
};
use py::functions::polyline::from_polyline;
pub(crate) use py::functions::*;
mod grid;
mod predicates;
pub(crate) use predicates::{PyPreparedGeometry, *};
mod py;
mod render;
use std::sync::Arc;

use boundary::coordinates::CoordinateAxis;
use error::Result;
use geometry::{
    Bounds, BufferCapStyle, BufferJoinStyle, BufferSide, CoordSeq, CoordinateAxes, Coordinates,
    EmptyKind, GeometryErrorKind, GeometryKind, LineIndex, PlanarMetric, Point, PointBatchTester,
    Polygon, PolygonizeFull, RepairMethod, Ring, Shape, ShapeData, SimplifyMethod, SmoothMethod,
    ValidationIssue, VoronoiBoundary, point_distance, row_sample_seed,
};
use py::arrow::{
    ArrowEncoding, geometries_to_arrow, geometries_to_wkb_arrow, packed_points_to_arrow,
};
use py::crs::PyCrs;
use py::errors::{CRSError, GeometryError, InvalidGeometryError, geometry_type_err};
pub(crate) use py::support::*;
use pyo3::IntoPyObjectExt;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAny, PyBool, PyBytes, PyInt, PyList, PyModule, PyTuple};
use render::{geometry_array_svg_grid_html_masked, render_shape_svg};
use rstar::AABB;
use serde_json::Value;

use crate::py::numpy::float64_array;

// `gil_used = false`: free-threaded CPython is supported — every `#[pyclass]`
// is Send+Sync (lazy caches use OnceLock/Mutex/atomic; scratch pools are
// thread_local). PreparedGeometry no longer opts out of cross-thread sharing.
#[pymodule(gil_used = false)]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Cargo is the package version authority; exporting its compile-time
    // value keeps Python import off the importlib.metadata filesystem path.
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    add_classes!(m;
        PyGeometry,
        // Typed leaf subclasses of Geometry (one per Shape variant).
        PyPoint,
        PyMultiPoint,
        PyLineString,
        PyMultiLineString,
        PyPolygon,
        PyMultiPolygon,
        PyGeometryCollection,
        PyGeometryParts,
        PyGeometryPartsIter,
        PyGeometryArray,
        array::PyGeometryArrayIter,
        PyPreparedGeometry,
        PyCoordinates,
        PyCoordinatesIter,
        PyValidationReport,
    );
    py::functions::constructors::register(m)?;
    add_functions!(m; area, length, bounds);
    add_functions!(m;
        snap, shared_paths,
        // Spatial predicates and pairwise measures.
        contains, contains_properly, within, covers, covered_by, contains_xy,
        intersects_xy, intersects, disjoint, touches, crosses, overlaps,
        relate, relate_pattern, equals, equals_exact, equals_identical,
        distance, distance_3d, hausdorff_distance,
        frechet_distance, nearest_points, shortest_line, split, dwithin,
        bearing, cross_track_distance, destination, point_between, rhumb_distance,
    );
    py::index::register(m)?;
    add_functions!(m;
        intersection, union, difference, symmetric_difference, union_all,
        intersection_all, symmetric_difference_all,
        coverage_is_valid, coverage_invalid_edges, coverage_simplify, coverage_clean,
        coverage_union, polygonize, polygonize_full,
        // IO, validation, and coordinate access.
        parts, rings, get_coordinates,
        from_wkt, from_wkb, from_geojson, from_features, to_feature, to_feature_collection,
        from_polyline, require,
        // Pickle payload rebuilders (private; see the `__reduce__` impls).
        _unpickle_geometry, _unpickle_geometry_array, _unpickle_point_array, _unpickle_line_array,
        _unpickle_polygon_array,
        _unpickle_prepared, _unpickle_validation_report,
        // Token vocabularies for the stub-parity gate (private).
        _token_vocabulary,
    );
    py::arrow::register(m)?;
    py::crs::register(m)?;
    py::vectors::register(m)?;
    py::errors::register(m)?;
    py::cells::register(m)?;
    add_functions!(m;
        pluscode_encode, pluscode_polygon, pluscode_shorten, pluscode_recover,
        osm_shortlink_encode, osm_shortlink_location,
    );
    // PyO3 auto-collects every registered name into `__all__`; drop the
    // private plumbing (pickle rebuilders, gate registries) so
    // `from gometry._lib import *` and the stub's mirrored `__all__` stay
    // public-only.
    let public: Vec<String> = m
        .getattr("__all__")?
        .extract::<Vec<String>>()?
        .into_iter()
        .filter(|name| !name.starts_with('_'))
        .collect();
    m.setattr("__all__", public)?;
    Ok(())
}
