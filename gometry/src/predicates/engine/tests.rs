use super::*;
use crate::crs::frame::Frame;
#[cfg(debug_assertions)]
use crate::geometry::storage::GeometryArrayStorage;
use crate::geometry::{CoordSeq, LineSeq, Point, Polygon, Ring};

include!("tests/support.rs");
include!("tests/gates.rs");
include!("tests/topology.rs");
include!("tests/points.rs");
include!("tests/matrix.rs");
