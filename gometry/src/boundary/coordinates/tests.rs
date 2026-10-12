use std::sync::Arc;

use super::{
    CoordSeq, CoordinateAxis, CoordinateColumnRef, CoordinateView, CsrOffsetColumn, Point, Polygon,
    Shape, ShapeData,
};
use crate::array::RingLevel;
use crate::geometry::storage::{RowSelection, ValidityMask};
use crate::geometry::{LineSeq, PolygonLevel, Ring, SharedSlice, storage};
use crate::{Frame, PyGeometryArray};

mod support;

use support::{closed_box, concat3, line, packed_polygons, pt};

include!("tests/traversal.rs");
include!("tests/equality.rs");
