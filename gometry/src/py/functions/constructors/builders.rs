#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use crate::boundary::{FloatInput, metadata};
use crate::geometry::{
    Bounds, CoordSeqBuilder, HasM, HasZ, MOrdinate, MultiLineString, MultiMember, MultiPoint,
    MultiPolygon, ZOrdinate, geographic_bounds_to_shape,
};
use crate::py::errors::{geometry_type_err, parameter_error};
use crate::py::functions::constructors::{
    Bound, CRSError, CoordSeq, CoordinateAxes, Crs, Frame, FrameAdoption, GeometryKind,
    InvalidGeometryError, LineSeq, Point, Polygon, PyAny, PyAnyMethods as _, PyErr, PyGeometry,
    PyGeometryArray, PyResult, PyTypeError, Ring, Shape, box_polygon, coordinate_column_values,
    coordinate_column_values_exact, coordinate_epoch_option, coordinate_sequence_len_hint, crs,
    ensure_homogeneous_axes, exact_geometry, extract_coordinate, extract_coordinate_or_empty,
    extract_points, finite_coordinate_required, optional_coordinate_column_values,
    optional_coordinates, parse_crs, parse_crs_epoch,
};
use crate::py::numpy;

mod frame;

mod scalar;

mod multipart;

mod data;

mod array;

#[cfg(test)]
mod tests;

pub(super) use array::build_geometry_array;
pub(crate) use array::build_geometry_collection;
use data::{GeometryArrayItem, polygon_from_coordinates_item};
pub(super) use data::{
    line_string_from_data_item, multi_line_string_from_data_item, multi_point_from_data_item,
    multi_polygon_from_data_item, polygon_from_data_item,
};
use frame::{geometry_with_crs_epoch, geometry_with_frame};
pub(crate) use multipart::{build_multi_line_string, build_multi_point, build_multi_polygon};
use multipart::{
    build_multi_line_string_parts, build_multi_point_parts, build_multi_polygon_parts,
};
pub(super) use scalar::build_box_shape;
pub(crate) use scalar::{build_line_string, build_point, build_polygon};
use scalar::{
    coordinate_vertices, empty_geometry_collection, empty_multi_line_string, empty_multi_point,
    empty_multi_polygon,
};
