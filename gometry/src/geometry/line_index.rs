//! Prefix-length linear-referencing engine: one build walks the linework
//! and caches per-segment lengths + cumulative offsets, then interpolate /
//! substring / locate run off the prefix table instead of re-measuring every
//! segment per call. Parameterized by a [`SegmentMetric`] so planar and
//! geographic chart-edge referencing share one lifecycle — each build measures
//! every source edge once and caches cumulative offsets.
//!
//! Zero-length segments stay in the route sequence at their unchanged prefix
//! and contribute their stored vertices to interpolation and substrings.
//! Locate skips them, matching the former projection walk; exact prefix
//! boundaries resolve to the first vertex at that prefix, except the end,
//! which resolves to the last stored vertex.

use crate::HeapSize;
use crate::crs::{ChartEdge, PointEdgeScratch};
use crate::error::Result;
use crate::geometry::distance::{
    ChartEdgePart, ChartParts, coordinate_squared_safe, point_chart_locate, squared_space_safe,
};
use crate::geometry::facet_bvh::update_nearest_candidate;
use crate::geometry::segments::projection::{
    point_segment_distance_and_projection, segment_projection,
};
use crate::geometry::{
    ChartMetric, CoordSeq, Coordinates as _, DistanceParts, GeodesicMetric, GeometryErrorKind,
    LineSeq, MeasureRange, NearestCandidate, Ordering, Point, Segment, Shape, ShapeCountQuery, XY,
    lerp_point, linework_first_point, normalize_line_distance, point_distance,
    point_segment_selection, push_distinct_active_position,
};
#[cfg(test)]
use crate::py::support::prepare_line_locate;

mod build;
mod locate;
mod metric;
mod route;

use build::{LineSegments, OrdinateSegment};
use metric::InterpolationProvenance;
pub(crate) use metric::{PlanarMetric, SegmentMetric, locate_planar_route};

/// Cache slot for a built (or unbuildable) [`LineIndex`].
///
/// `OnceLock` needs a stored value, and `GeometryErrorKind` is not `Clone`, so
/// the two build failures are remembered as variants and re-raised by the
/// accessor.
pub(crate) enum LineIndexSlot {
    Index(LineIndex),
    NotLineal,
    Empty,
    /// Fallible construction (non-finite segment/total length, etc.).
    Invalid(String),
}

impl LineIndexSlot {
    pub(crate) fn build(shape: &Shape, metric: &impl SegmentMetric) -> Result<Self> {
        use crate::error::ErrorKind;
        match LineIndex::build(shape, metric) {
            Ok(index) => Ok(Self::Index(index)),
            Err(error) => match error.kind() {
                ErrorKind::Geometry(GeometryErrorKind::EmptyLinework) => Ok(Self::Empty),
                ErrorKind::Geometry(GeometryErrorKind::LineStringRequired) => Ok(Self::NotLineal),
                ErrorKind::Resource(_) => Err(error),
                ErrorKind::Geometry(_)
                | ErrorKind::Crs(_)
                | ErrorKind::Io(_)
                | ErrorKind::Frame(_) => Ok(Self::Invalid(error.to_string())),
            },
        }
    }

    pub(crate) fn get(&self) -> Result<&LineIndex> {
        match self {
            Self::Index(index) => Ok(index),
            Self::NotLineal => Err(GeometryErrorKind::LineStringRequired.into()),
            Self::Empty => Err(GeometryErrorKind::EmptyLinework.into()),
            Self::Invalid(msg) => Err(GeometryErrorKind::message(Box::<str>::from(msg.as_str()))),
        }
    }
}

pub(crate) type ChartLineIndex = LineIndex<std::sync::Arc<ChartParts>>;

pub(crate) struct LineIndex<S = Box<[OrdinateSegment]>> {
    /// All stored segments, in linework order. Zero-length segments share a
    /// prefix with the surrounding route vertices.
    segments: S,
    /// `prefix[i]` = cumulative length before segment `i`; the last entry is
    /// the cached total length. Lengths are cached here — locate's
    /// `mul_add(length, fraction, offset)` and every interpolation walk read
    /// them back instead of re-measuring.
    prefix: Box<[f64]>,
    /// Stored separately: deriving it added 0.17–2.08% instructions in measured paths.
    total: f64,
    first: Point,
    last: Point,
    has_zero_segments: bool,
}

impl LineIndex {
    pub(crate) const fn has_zero_length_segments(&self) -> bool {
        self.has_zero_segments
    }
}

impl HeapSize for LineIndexSlot {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Index(index) => index.heap_bytes(),
            Self::NotLineal | Self::Empty => 0,
            Self::Invalid(msg) => msg.capacity(),
        }
    }
}

impl<S: LineSegments> HeapSize for LineIndex<S> {
    fn heap_bytes(&self) -> usize {
        self.segments
            .heap_bytes()
            .saturating_add(self.prefix.heap_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod build;
    mod locate;
    mod route;
    mod support;

    use support::*;
}
