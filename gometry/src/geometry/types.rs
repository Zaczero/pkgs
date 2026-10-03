//! Geometry data model: every type and its inherent/core-trait impls.
//!
//! The `Shape` enum, coordinate storage (`CoordSeq`, `Point`, `Ring`,
//! `Polygon`, `Bounds`, `Segment`), the `Coordinates` and `GeodesicMetric`
//! traits, the option/kind enums, and `GeometryErrorKind`. Behavior lives in the
//! sibling concern modules; the shared geometry kernel stays in the module
//! root.

mod coordseq;
mod csr;
mod error;
mod point;
mod primitives;
mod ring;
mod shape;
mod shape_data;
mod shared_slice;

pub(crate) use coordseq::{
    CoordIter, CoordSeq, CoordSeqBuilder, CoordWindow, MColumn, VisiblePointRunKey, VisibleXYKey,
    coerce_to_common_axes, coordinate_columns_eq,
};
pub(crate) use csr::{
    CsrOffsetBuilder, CsrOffsetColumn, GeometryKind, PolygonLevel, RingLevel, checked_csr_window,
    ensure_coordseq_vertex_capacity,
};
pub(crate) use error::GeometryErrorKind;
pub(in crate::geometry) use primitives::GeographicMetricKey;
pub(crate) use primitives::{
    Bounds, CoordinateAxes, DistanceParts, HasM, HasZ, MOrdinate, Point, Polygon, Segment, XY,
    ZOrdinate,
};
pub(crate) use ring::{Coordinates, Ring};
pub(crate) use shape::{
    BufferCapStyle, BufferJoinStyle, BufferSide, LineSeq, MAX_NESTING, MultiLineString,
    MultiMember, MultiPoint, MultiPolygon, RepairMethod, Shape, ShapeCountQuery, ShapeHeapMemo,
    ShapeOwnerKey, SimplifyMethod, SmoothMethod, VoronoiBoundary,
};
pub(crate) use shape_data::{
    ChartBox, ChartEdgeDifferential, ChartEdgeDifferentialBounds, ChartMetric,
    ChartPointDistanceSample, FrameDependentCaches, GeodesicMetric, GeodesicNumericalPolicy,
    IndexedSegment, IntoSharedShape, PointEdgeMinimum, PointEdgeQuery, PointEdgeQueryResult,
    PointKey, PolygonizeFull, PreparedPlan, ShapeData, ValidationIssue, canonical_f64_bits,
};
pub(crate) use shared_slice::{SharedSlice, SharedSliceIter};

#[cfg(test)]
mod tests;
