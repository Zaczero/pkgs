use std::num::NonZeroU32;

use crate::Positive;
use crate::geometry::constructive::offset::offset_curve_points;
use crate::geometry::constructive::{
    WalkJoinRule, convex_buffer, winding_buffer, winding_erosion, winding_stroke,
};
use crate::geometry::overlay::{partition_calls, reset_partition_calls};
use crate::geometry::{
    BufferCapStyle, BufferJoinStyle, CoordSeq, ExpansionBudget, GeometryErrorKind, LineSeq,
    MultiPolygon, Point, Polygon, Ring, Shape,
};

mod buffer;
mod expansion_budget;
mod subdivision;
