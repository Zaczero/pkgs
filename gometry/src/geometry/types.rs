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

pub(crate) use coordseq::{
    CoordIter, CoordSeq, CoordSeqBuilder, CoordWindow, coerce_to_common_axes, concat_coord_columns,
    reverse_column_arc, witness_pair,
};
pub(crate) use csr::{
    CsrOffsetBuilder, CsrOffsetColumn, GeometryKind, PolygonLevel, RingLevel,
    ensure_coordseq_vertex_capacity,
};
pub(crate) use error::GeometryErrorKind;
pub(in crate::geometry) use primitives::GeodesicPartsKey;
pub(crate) use primitives::{
    Bounds, Bounds3D, CoordinateAxes, DistanceParts, GeodesicParts, GeodesicSegment, HasM, HasZ,
    MOrdinate, Point, Polygon, Segment, XY, ZOrdinate, segment_midpoint, segment_midpoint_xy,
};
pub(crate) use ring::{Coordinates, Ring};
pub(crate) use shape::{
    BufferCapStyle, BufferJoinStyle, BufferSide, EmptyKind, LineSeq, RepairMethod, Shape,
    SimplifyMethod, SmoothMethod, VoronoiBoundary,
};
pub(crate) use shape_data::{
    FrameDependentCaches, GeodesicMetric, GeodesicSegmentWitness, IndexedSegment, PointKey,
    PolygonizeFull, PolylabelCell, ShapeData, ValidationIssue, canonical_f64_bits, ordered_edge,
};

#[cfg(test)]
mod tests;
