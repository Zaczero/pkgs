//! Point navigation kernels — bearing, destination, interpolate, rhumb.
use pyo3::types::PyAny;

use crate::dispatch::MetricOp;
use crate::geographic::{GeographicFrame, lonlat as geographic};
use crate::geometry::storage::validity::{LogicalRowIndex, PresentRowIndex, union_many};
use crate::geometry::storage::{
    ArrayRows, GeometryArrayStorage, PackedStorage, ValidityMask, ValidityRank,
};
use crate::geometry::{
    CoordSeq, CoordSeqBuilder, CoordinateAxes, HasM, HasZ, MOrdinate, ZOrdinate,
    same_topological_coordinate, storage,
};
use crate::py::errors::{CRSError, GeometryError, InvalidGeometryError};
use crate::py::numpy::float64_array;
use crate::{
    Bound, CoordinateInput, Crs, DistanceUnit, Frame, GeometryInput, NavigationPath, Point,
    PointRows, Py, PyErr, PyGeometry, PyGeometryArray, PyResult, Python, Shape,
    broadcast_coordinate_group, classify_input, classify_required, coordinate_sequence_len_hint,
    crs, ensure_same_len, expected_geometry_or_array, parameter_input_with_expected, pyfunction,
    resolve_metric,
};

mod adapters;
mod cross_track;
mod destination;
mod kernels;
mod pair;
mod rows;

pub(crate) use adapters::reject_rhumb_unit;
pub(crate) use cross_track::cross_track_distance;
pub(crate) use destination::{
    destination, destination_point_receiver, rhumb_destination_point_receiver,
};
pub(crate) use pair::{bearing, distance_rhumb, point_between};
