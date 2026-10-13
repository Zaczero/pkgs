//! Hierarchical prepared point-in-polygon membership.
//!
//! Ownership mirrors geometry structure:
//! - [`PointBatchTester`] — multipolygon parts via [`YStabbingIndex`]
//! - [`PolygonPointIndex`] — shell ring + holes via [`YStabbingIndex`]
//! - [`RingPointIndex`] — ring edges via the same index type
//!
//! Every descent applies a full X/Y bounds check before exact classification.
//! Semantics are exact: polygon = shell minus its holes; multipolygon = any
//! Interior wins, else Boundary, else Exterior (never global XOR across parts).
//! Crossing arithmetic reuses [`ray_crossing_is_right`] / [`orientation_xy`].

mod arrangement;
mod prepared;

use std::ops::ControlFlow;
#[cfg(test)]
use std::sync::Arc;

pub(in crate::geometry) use arrangement::{
    AreaLocationLabels, ExactLocationCells, ExactLocationDart, ExactLocationEdge,
    ExactLocationIndex, ExactLocationPoints, SourceLocationLabels,
};
#[cfg(all(test, debug_assertions))]
pub(in crate::geometry) use arrangement::{
    query_comparisons, query_edge_tests, reset_query_comparisons,
};
pub(in crate::geometry) use prepared::PointQueryPlan;
pub(crate) use prepared::PreparedPointQueries;

use crate::HeapSize;
use crate::error::Result;
use crate::geometry::segment_index::{EdgeId, EdgeYIndex, YStabbingIndex, checked_index_count};
use crate::geometry::source_support::{
    PointMembership, PointQuery, RingEdgeCandidates, SourcePointLocation, StreamingLocation,
    cartesian_point_location_and_area, exact_edge_winding, exact_offset_edge_winding,
    exact_offset_point_in_bounds, exact_point_in_bounds, exact_query_xy, exact_query_y,
};
use crate::geometry::tessellation::exact::ExactDyadic;
use crate::geometry::{
    Bounds, CoordSeq, Loc, Point, Polygon, Ring, RingClass, RingView, Shape, SharedSlice, XY,
    edge_winding, ring_kind_columns, xy_bounds_columns,
};

mod batch;
mod polygon;
mod ring;
#[cfg(test)]
mod tests;

pub(crate) use batch::PointBatchTester;
pub(crate) use polygon::{ExactPointTester, PolygonPointIndex};
use polygon::{indexed_parts_occupied_exact, location_from_indexed_parts};
pub(crate) use ring::RingPointIndex;
