use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyModule};

use crate::geometry::{CoordSeq, LineSeq, Point, Polygon, Ring, Shape};
use crate::py::errors::{CRSError, GeometryError, InvalidGeometryError};
use crate::{
    CoordinateAxes, Crs, EmptyKind, Frame, FrameAdoption, PyGeometry, PyGeometryArray, ShapeData,
    Typed, box_polygon, broadcast_coordinate_group, broadcast_crs_coordinate_inputs,
    coordinate_arc_values, coordinate_epoch_option, coordinate_input, coordinate_inputs_are_scalar,
    crs, ensure_homogeneous_axes, exact_geometry, extract_coordinate, extract_lines,
    extract_points, extract_polygons, finite_coordinate_required, optional_coordinate_arc_values,
    optional_coordinates, parse_crs, parse_crs_epoch, wrapped_box,
};

mod builders;
mod pyfuncs;

use builders::{
    build_box_shape, build_geometry_array, line_string_from_data_item,
    multi_line_string_from_data_item, multi_point_from_data_item, multi_polygon_from_data_item,
    polygon_from_data_item,
};
pub(crate) use builders::{
    build_geometry_collection, build_line_string, build_multi_line_string, build_multi_point,
    build_multi_polygon, build_point, build_polygon,
};
use pyfuncs::{
    box_, boxes, line_strings, multi_line_strings, multi_points, multi_polygons, points, polygons,
};

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::add_functions!(
        m;
        box_,
        boxes,
        points,
        line_strings,
        polygons,
        multi_points,
        multi_line_strings,
        multi_polygons
    );
    Ok(())
}
