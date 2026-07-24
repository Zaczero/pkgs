#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Geometry kernel — the shared math/conversion substrate plus the core
//! `Shape`/`Polygon`/`Bounds` utility impls.
//!
//! The operation families live in the children — `access`
//! (inspection/traversal/reducers), `predicates` (topological queries),
//! `constructive` (editing transforms), `overlay` (set operations),
//! `tessellation` (triangulation/voronoi/polygonize), `linear`/`distance`
//! (linear referencing and proximity), `derived` (polylabel and friends),
//! `segments` (segment/orientation primitives), `ordinates` (Z/M carry), and
//! `types` (the data model). Children reach the shared kernel via `use
//! super::*`.

mod access;
mod area;
pub(crate) use access::{
    REDUCE_LANES, REDUCE_SIMD_MIN, ReduceSimd, ShapePart, canonicalize_zero, centroid_ring_sums,
    column_all_finite, column_mean2, column_minmax, line_length_3d_columns, line_length_columns,
    pair_map4_guarded_f64, pair_select_mask, shape_axes_all_z, simd_argmin_f64,
    simd_indexed_map_f64, simd_map_f64, simd_mask_all, simd_mask_any, simd_select_indices,
    simd_xy_map_f64, try_simd_map_f64, xy_bounds_columns,
};
pub(crate) use area::{
    AreaSign, RingDecisionArea, RingWinding, closed_columns_winding, open_point_cycle_decision,
    open_point_cycle_winding, open_xy_cycle_decision, open_xy_cycle_winding, ring_area_measure,
    ring_area_measure_columns, ring_decision_area, ring_winding,
};
pub(crate) use segments::{Orientation, orientation};
pub(crate) mod types;
pub(crate) use types::*;
mod antimeridian;
pub(crate) use antimeridian::{
    geographic_crossing, geographic_crossing_bounds, geographic_crossing_bounds_for_shapes,
    is_geographic_frame, is_ring_data_in_frame, is_simple_data_in_frame, repair_data_in_frame,
    repair_shape_in_frame, self_intersections_in_frame, topology_split, unary_antimeridian_derived,
    validate_data_in_frame, validate_shape_in_frame,
};
mod arrangement;
mod bvh_code;
mod clean_union;
mod constructive;
mod convex_contact;
mod coverage;
#[doc(hidden)]
pub(crate) use coverage::vw_area_tolerance;
mod dimension;
pub(crate) use dimension::{DimMode, Dimension, dimension};
mod derived;
pub(crate) use derived::{
    centroid_line_row_columns, centroid_polygon_row_columns, monotone_chain_hull,
    native_concave_hull, point_on_surface_line_columns, point_on_surface_polygon_row_columns,
    shape_from_open_hull, smallest_enclosing_circle,
};
mod distance;
mod expansion;
mod facet_bvh;
mod geodesic_bvh;
mod line_index;
mod linear;
mod ordinates;
mod overlay;
pub(crate) mod packed_line_metrics;
mod predicates;
mod provenance;
pub(crate) use provenance::{VertexProvenance, emit_from_original};
mod relate;
mod relate_ng;
pub(crate) use relate_ng::CompiledPattern;
mod segment_index;
mod segments;
mod tessellation;
mod topology;
mod util;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;
use std::simd::cmp::{SimdPartialEq, SimdPartialOrd};
use std::simd::num::SimdFloat;
use std::simd::{Select, StdFloat};

use access::*;
pub(crate) use arrangement::*;
pub(crate) use bvh_code::{BvhCode, BvhNode};
#[doc(hidden)]
pub(crate) use constructive::vw_keep;
use constructive::*;
pub(crate) use constructive::{
    DEFAULT_MITER_LIMIT, affine_about, densify_points, rdp_keep, segmentize_points,
};
pub(crate) use coverage::*;
use distance::*;
pub(crate) use distance::{
    Distance3dParts, bounds_distance_squared, distance_3d_with_parts,
    frechet_distance_line_columns_batch, hausdorff_distance_line_columns_batch,
};
#[doc(hidden)]
pub(crate) use distance::{frechet_distance_line_columns, hausdorff_distance_line_columns};
pub(crate) use expansion::{ExpansionBudget, GENERATED_ITEM_LIMIT};
pub(crate) use facet_bvh::*;
use geodesic_bvh::*;
pub(crate) use line_index::*;
pub(crate) use linear::MeasureRange;
use linear::*;
use ordinates::*;
use overlay::*;
pub(crate) use overlay::{
    OverlayOp, Strictness, disjoint_overlay_combine, empty_shape_for_dimension,
};
#[doc(hidden)]
pub(crate) use packed_line_metrics::PackedLineColumnView;
pub(crate) use packed_line_metrics::scale_metric_values;
use predicates::*;
pub(crate) use predicates::{
    PolePosition, convex_box_strictly_inside, convex_halfplanes_cover, line_crosses_antimeridian,
    line_is_ccw, line_is_closed, line_is_simple, line_is_valid, point_is_geographic_pole,
    pole_position, polygon_is_valid, shape_encloses_pole, shape_has_polar_ring,
    shape_spans_full_longitude,
};
pub(crate) use util::*;

/// Re-export seam: `pub(super)` kernel in the private `predicates` child.
pub(crate) fn polygon_row_point_membership<const BOUNDARY: bool>(
    coords: &CoordSeq,
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
    point: Point,
) -> bool {
    predicates::polygon_row_point_membership::<BOUNDARY>(coords, ring_offsets, rings, point)
}
use relate::*;
pub(crate) use segment_index::*;
use segments::*;
/// The guarded-sqrt distance is the one segment-kernel helper the broadcast
/// lanes call directly (packed point columns).
pub(crate) use segments::{line_segments, point_distance, point_distance_squared};
use tessellation::*;
pub(crate) use tessellation::{CdtRefinement, row_sample_seed};

use crate::collections::{HashMap, HashMapExt, HashSet, HashSetExt};
use crate::error::Result;

pub(crate) struct BulkParams;

impl rstar::RTreeParams for BulkParams {
    const MIN_SIZE: usize = 8;
    const MAX_SIZE: usize = 16;
    const REINSERTION_COUNT: usize = 4;
    type DefaultInsertionStrategy = rstar::RStarInsertionStrategy;
}

pub(crate) type BulkRTree<T> = rstar::RTree<T, BulkParams>;

/// The ordinate axes BOTH operands carry (the intersection). The
/// common-dimensionality of a nearest/shortest result: a witness on each side
/// carries that operand's ordinates, so the pair's shared axes are the operands'
/// shared axes.
pub(crate) fn common_axes(a: &Shape, b: &Shape) -> CoordinateAxes {
    let (a, b) = (a.axes(), b.axes());
    CoordinateAxes::new(HasZ(a.has_z() && b.has_z()), HasM(a.has_m() && b.has_m()))
}

/// The `shortest_line` Shape for a nearest pair: the witness `LineString`, or
/// `LINESTRING EMPTY` (in `common` axes) when an operand is empty. The single
/// place the empty→sentinel and common-axes rules meet for the line surface.
pub(crate) fn nearest_line(pair: Option<(Point, Point)>, common: CoordinateAxes) -> Shape {
    match pair {
        Some((start, end)) => Shape::LineString(
            LineSeq::try_new(witness_pair(start, end))
                .expect("nearest-line witness has two vertices"),
        ),
        None => Shape::LineString(LineSeq::empty(common)),
    }
}

/// The `(left, right)` witness Shapes for `nearest_points`: the points dropped to
/// their common axes, or `(POINT EMPTY, POINT EMPTY)` when an operand is empty.
/// Mirrors [`nearest_line`] so the tuple and the line stay consistent.
pub(crate) fn nearest_point_pair(
    pair: Option<(Point, Point)>,
    common: CoordinateAxes,
) -> (Shape, Shape) {
    match pair {
        Some((left, right)) => {
            let (left, right) = coerce_to_common_axes(left, right);
            (Shape::Point(left), Shape::Point(right))
        },
        None => (
            Shape::typed_empty(EmptyKind::Point, common),
            Shape::typed_empty(EmptyKind::Point, common),
        ),
    }
}

impl Shape {
    /// The canonical `POINT EMPTY` (XY).
    pub const fn empty_point() -> Self {
        Self::Empty(EmptyKind::Point, CoordinateAxes::XY)
    }

    /// The canonical `POLYGON EMPTY` (XY).
    pub const fn empty_polygon() -> Self {
        Self::Empty(EmptyKind::Polygon, CoordinateAxes::XY)
    }

    /// The canonical typed empty of `kind` carrying `axes` — THE constructor
    /// for axes-tagged empties. The container kinds normalize their XY empty
    /// to the empty-`Vec` data form (`MULTIPOLYGON EMPTY` stays
    /// `Shape::MultiPolygon(vec![])`), so an XY container empty has exactly
    /// one representation and value equality cannot split on it.
    pub(crate) fn typed_empty(kind: EmptyKind, axes: CoordinateAxes) -> Self {
        if axes == CoordinateAxes::XY {
            match kind {
                EmptyKind::MultiLineString => return Self::MultiLineString(Vec::new()),
                EmptyKind::MultiPolygon => return Self::MultiPolygon(Vec::new()),
                EmptyKind::GeometryCollection => return Self::GeometryCollection(Vec::new()),
                EmptyKind::Point | EmptyKind::Polygon => {},
            }
        }
        Self::Empty(kind, axes)
    }

    /// `true` for a typed empty ([`Shape::Empty`]).
    pub const fn is_typed_empty(&self) -> bool {
        matches!(self, Self::Empty(..))
    }

    /// Visit every line segment — linework edges of (multi)linestrings, ring
    /// edges of (multi)polygons, recursively through collections — without
    /// materializing a `Vec`. Point-only shapes have no segments.
    pub(crate) fn for_each_segment(&self, mut visit: impl FnMut(Segment)) {
        self.for_each_segment_inner(&mut visit);
    }

    /// [`for_each_segment`](Self::for_each_segment) with FULL ordinate
    /// carry: each edge as its two `Point`s (Z/M intact) — the ordinate
    /// lanes' traversal (the planar `Segment` is XY-only by design).
    pub(crate) fn for_each_vertex_pair(&self, mut visit: impl FnMut(Point, Point)) {
        self.for_each_vertex_pair_inner(&mut visit);
    }

    fn for_each_vertex_pair_inner<F: FnMut(Point, Point)>(&self, visit: &mut F) {
        fn lines<F: FnMut(Point, Point)>(seq: &CoordSeq, visit: &mut F) {
            for [start, end] in seq.segment_pairs() {
                visit(start, end);
            }
        }
        match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => {},
            Self::LineString(points) => lines(points, visit),
            Self::MultiLineString(parts) => {
                for line in parts {
                    lines(line, visit);
                }
            },
            Self::Polygon(polygon) => {
                for ring in polygon.rings() {
                    lines(ring, visit);
                }
            },
            Self::MultiPolygon(polygons) => {
                for polygon in polygons {
                    for ring in polygon.rings() {
                        lines(ring, visit);
                    }
                }
            },
            Self::GeometryCollection(geometries) => {
                for geometry in geometries {
                    geometry.for_each_vertex_pair_inner(visit);
                }
            },
        }
    }

    fn for_each_segment_inner<F: FnMut(Segment)>(&self, visit: &mut F) {
        match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => {},
            Self::LineString(points) => line_segments(points).for_each(&mut *visit),
            Self::MultiLineString(lines) => {
                lines.iter().flat_map(line_segments).for_each(&mut *visit);
            },
            Self::Polygon(polygon) => polygon.segments().for_each(&mut *visit),
            Self::MultiPolygon(polygons) => polygons
                .iter()
                .flat_map(Polygon::segments)
                .for_each(&mut *visit),
            Self::GeometryCollection(geometries) => {
                for geometry in geometries {
                    geometry.for_each_segment_inner(visit);
                }
            },
        }
    }

    /// Raw coordinate payload in bytes — the stored `f64` ordinate columns.
    /// This is exact slice payload (`len == retained`) and is shared by
    /// logical `nbytes` and retained heap accounting.
    pub fn coordinate_bytes(&self) -> usize {
        match self {
            Self::Empty(..) => 0,
            Self::Point(point) => {
                CoordinateAxes::new(HasZ(point.z().is_some()), HasM(point.m().is_some()))
                    .byte_width(1)
            },
            Self::MultiPoint(seq) => seq.coordinate_bytes(),
            Self::LineString(seq) => seq.coordinate_bytes(),
            Self::MultiLineString(lines) => lines.iter().map(|line| line.coordinate_bytes()).sum(),
            Self::Polygon(polygon) => polygon.rings().map(CoordSeq::coordinate_bytes).sum(),
            Self::MultiPolygon(polygons) => polygons
                .iter()
                .flat_map(Polygon::rings)
                .map(CoordSeq::coordinate_bytes)
                .sum(),
            Self::GeometryCollection(geometries) => {
                geometries.iter().map(Self::coordinate_bytes).sum()
            },
        }
    }

    /// Total segment count — `O(parts)`, for pre-sizing collectors.
    pub(crate) fn segment_count(&self) -> usize {
        match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => 0,
            Self::LineString(points) => points.len().saturating_sub(1),
            Self::MultiLineString(lines) => {
                lines.iter().map(|line| line.len().saturating_sub(1)).sum()
            },
            Self::Polygon(polygon) => polygon.segment_count(),
            Self::MultiPolygon(polygons) => polygons.iter().map(Polygon::segment_count).sum(),
            Self::GeometryCollection(geometries) => {
                geometries.iter().map(Self::segment_count).sum()
            },
        }
    }

    pub(crate) fn segments(&self) -> Vec<Segment> {
        let mut segments = Vec::with_capacity(self.segment_count());
        self.for_each_segment(|segment| segments.push(segment));
        segments
    }

    fn point_only_points(&self) -> Vec<Point> {
        match self {
            Self::Point(point) => vec![*point],
            Self::MultiPoint(points) => points.to_vec(),
            Self::GeometryCollection(geometries) => geometries
                .iter()
                .flat_map(Self::point_only_points)
                .collect(),
            _ => Vec::new(),
        }
    }

    fn linework(&self) -> Result<Vec<&CoordSeq>> {
        match self {
            Self::LineString(points) => Ok(vec![points]),
            Self::MultiLineString(lines) => Ok(lines.iter().map(LineSeq::as_coords).collect()),
            _ => Err(GeometryErrorKind::LineStringRequired.into()),
        }
    }

    pub(crate) fn single_linework(&self) -> Result<&CoordSeq> {
        match self {
            Self::LineString(points) if !points.is_empty() => Ok(points),
            Self::MultiLineString(lines) if lines.len() == 1 && !lines[0].is_empty() => {
                Ok(&lines[0])
            },
            // The right kind with no content is a structural error, not a
            // kind error.
            Self::LineString(_) => Err(GeometryErrorKind::EmptyLinework.into()),
            Self::MultiLineString(lines) if lines.len() == 1 => {
                Err(GeometryErrorKind::EmptyLinework.into())
            },
            _ => Err(GeometryErrorKind::FrechetLineStringRequired.into()),
        }
    }

    fn splitter_points(&self) -> Result<Vec<Point>> {
        match self {
            Self::Point(point) => Ok(vec![*point]),
            Self::MultiPoint(points) => Ok(points.to_vec()),
            // `POINT EMPTY` is point-like but contributes no split locations.
            Self::Empty(EmptyKind::Point, _) => Ok(Vec::new()),
            _ => Err(GeometryErrorKind::PointSplitterRequired.into()),
        }
    }
}

impl Polygon {
    pub fn new(shell: Ring, holes: Vec<Ring>) -> Self {
        Self {
            shell,
            holes: holes.into(),
        }
    }

    pub fn points(&self) -> impl Iterator<Item = Point> + '_ {
        self.shell.iter().chain(self.holes.iter().flatten())
    }

    /// Total vertex count across the shell and every hole — `O(rings)`, for
    /// pre-sizing collectors.
    pub fn coord_count(&self) -> usize {
        self.shell.coord_count() + self.holes.iter().map(Ring::coord_count).sum::<usize>()
    }

    /// The shell followed by every hole, each as a borrowed coordinate column.
    pub fn rings(&self) -> impl Iterator<Item = &CoordSeq> + '_ {
        std::iter::once(self.shell.coords()).chain(self.holes.iter().map(Ring::coords))
    }

    /// Apply a per-ring coordinate transform to the shell and every hole. The
    /// transform preserves ring validity by construction (it operates on an
    /// already-valid ring), so results are wrapped with [`Ring::from_trusted_closed`].
    fn map_rings(&self, transform: impl Fn(&CoordSeq) -> Vec<Point>) -> Self {
        Self {
            shell: Ring::from_trusted_closed(transform(self.shell.coords())),
            holes: self
                .holes
                .iter()
                .map(|hole| Ring::from_trusted_closed(transform(hole.coords())))
                .collect(),
        }
    }

    /// Fallible variant of [`map_rings`].
    fn try_map_rings<E>(
        &self,
        transform: impl Fn(&CoordSeq) -> Result<Vec<Point>, E>,
    ) -> Result<Self, E> {
        Ok(Self {
            shell: Ring::from_trusted_closed(transform(self.shell.coords())?),
            holes: self
                .holes
                .iter()
                .map(|hole| transform(hole.coords()).map(Ring::from_trusted_closed))
                .collect::<Result<_, _>>()?,
        })
    }

    fn area(&self) -> f64 {
        let mut area = ring_area_measure(self.shell.coords()).get();
        for hole in self.holes.iter() {
            area -= ring_area_measure(hole.coords()).get();
        }
        area
    }

    fn bounds(&self) -> Option<Bounds> {
        // Holes lie inside the shell for valid polygons, so the shell envelope
        // IS the polygon envelope (matches the packed bounds path and OGC
        // envelope semantics) — no hole fold needed.
        Bounds::from_coords(self.shell.coords())
    }

    fn length(&self) -> f64 {
        line_length(self.shell.coords())
            + self
                .holes
                .iter()
                .map(|hole| line_length(hole.coords()))
                .sum::<f64>()
    }

    pub(crate) fn validate(&self, name: &str, path: &str) -> Option<ValidationIssue> {
        validate_ring(self.shell.coords(), || {
            (format!("{name} shell"), format!("{path}.shell"))
        })
        .or_else(|| {
            self.holes.iter().enumerate().find_map(|(idx, hole)| {
                validate_ring(hole.coords(), || {
                    (format!("{name} hole"), format!("{path}.holes[{idx}]"))
                })
            })
        })
        .or_else(|| polygon_rings_issue(self, path))
    }

    fn contains_point(&self, point: Point) -> bool {
        ring_classify_point(self.shell.coords(), point) == RingClass::Interior
            && !self
                .holes
                .iter()
                .any(|hole| ring_classify_point(hole.coords(), point) != RingClass::Exterior)
    }

    fn covers_point(&self, point: Point) -> bool {
        ring_classify_point(self.shell.coords(), point) != RingClass::Exterior
            && !self
                .holes
                .iter()
                .any(|hole| ring_classify_point(hole.coords(), point) == RingClass::Interior)
    }

    fn quantize(&self, precision: i32) -> Self {
        self.map_ring_seqs(|ring| ring.quantize(precision))
    }

    /// Fallible columnar XY transform over the shell and every hole, generic
    /// over the error (the in-core CRS projections' per-polygon engine).
    fn map_rings_xy_with<E>(
        &self,
        op: impl Fn(f64, f64) -> Result<(f64, f64), E> + Copy,
    ) -> Result<Self, E> {
        Ok(Self {
            shell: Ring::from_trusted_closed(self.shell.coords().try_map_xy_with(op)?),
            holes: self
                .holes
                .iter()
                .map(|hole| {
                    Ok(Ring::from_trusted_closed(
                        hole.coords().try_map_xy_with(op)?,
                    ))
                })
                .collect::<Result<_, E>>()?,
        })
    }

    /// Fallible per-ring `CoordSeq` transform over the shell and every hole,
    /// generic over the error. The columnar counterpart to
    /// [`map_ring_seqs`](Self::map_ring_seqs): preserves ring validity
    /// (closure and length) with no `AoS` `Vec<Point>` round-trip, and is
    /// the ring engine behind the SIMD affine / snap-to-grid column kernels
    /// (and any other fallible column map).
    fn try_map_ring_seqs<E>(
        &self,
        transform: impl Fn(&CoordSeq) -> Result<CoordSeq, E> + Copy,
    ) -> Result<Self, E> {
        Ok(Self {
            shell: Ring::from_trusted_closed(transform(self.shell.coords())?),
            holes: self
                .holes
                .iter()
                .map(|hole| Ok(Ring::from_trusted_closed(transform(hole.coords())?)))
                .collect::<Result<_, E>>()?,
        })
    }

    fn remove_repeated_points(&self, tolerance: f64) -> Self {
        self.map_ring_seqs(|ring| remove_repeated_points(ring, tolerance))
    }

    fn segmentize(&self, max_segment_length: f64) -> Result<Self> {
        self.try_map_ring_seqs(|ring| segmentize_points(ring, max_segment_length))
    }

    fn densified(&self, fraction: f64) -> Result<Self> {
        self.try_map_ring_seqs(|ring| densify_points(ring, fraction))
    }

    fn reverse(&self) -> Self {
        self.map_ring_seqs(CoordSeq::reversed)
    }

    /// Rebuild every ring through a columnar transform that preserves ring
    /// validity (closure and length) — no `AoS` `Vec<Point>` round-trip.
    fn map_ring_seqs(&self, transform: impl Fn(&CoordSeq) -> CoordSeq) -> Self {
        Self {
            shell: Ring::from_trusted_closed(transform(self.shell.coords())),
            holes: self
                .holes
                .iter()
                .map(|hole| Ring::from_trusted_closed(transform(hole.coords())))
                .collect(),
        }
    }

    fn orient(&self, exterior_cw: bool) -> Self {
        Self {
            shell: Ring::from_trusted_closed(orient_ring(self.shell.coords(), exterior_cw)),
            holes: self
                .holes
                .iter()
                .map(|hole| Ring::from_trusted_closed(orient_ring(hole.coords(), !exterior_cw)))
                .collect(),
        }
    }

    fn normalize(&self) -> Self {
        // Canonical winding follows RFC 7946 (GeoJSON): exterior CCW,
        // holes CW — the modern convention, deliberately not GEOS's
        // CW-shell legacy. Rings rotate min-vertex-first; holes sort
        // ascending like every other canonical sequence.
        let mut holes = self
            .holes
            .iter()
            .map(|hole| Ring::from_trusted_closed(canonical_ring_seq(hole.coords(), true)))
            .collect::<Vec<_>>();
        holes.sort_by(compare_point_slices);
        Self {
            shell: Ring::from_trusted_closed(canonical_ring_seq(self.shell.coords(), false)),
            holes: holes.into(),
        }
    }

    fn equals_exact_impl<const Z: bool, const M: bool>(
        &self,
        other: &Self,
        tolerance: f64,
    ) -> bool {
        points_equal_exact::<Z, M, _, _>(&self.shell, &other.shell, tolerance)
            && self.holes.len() == other.holes.len()
            && self
                .holes
                .iter()
                .zip(other.holes.iter())
                .all(|(left, right)| points_equal_exact::<Z, M, _, _>(left, right, tolerance))
    }

    fn force_2d(&self) -> Self {
        self.map_ring_seqs(CoordSeq::force_2d)
    }

    fn map_points<F>(&self, transform: &F) -> Result<Self>
    where
        F: Fn(Point) -> Result<Point> + ?Sized,
    {
        self.try_map_rings(|ring| ring.iter().map(transform).collect())
    }

    fn snap_to_reference(&self, reference: &SnapReference) -> Self {
        self.map_rings(|ring| snap_ring_to_reference(ring, reference))
    }

    fn segments(&self) -> impl Iterator<Item = Segment> + '_ {
        line_segments(&self.shell).chain(self.holes.iter().flat_map(line_segments))
    }

    /// Total ring-edge count across the shell and every hole — `O(rings)`,
    /// for pre-sizing collectors.
    fn segment_count(&self) -> usize {
        self.rings().map(|ring| ring.len().saturating_sub(1)).sum()
    }

    fn normalized_degenerate(self) -> Shape {
        let unique = unique_xy_points(&self.shell);
        match unique.as_slice() {
            // Areal op (minimum-rotated-rectangle) → empty result is `POLYGON
            // EMPTY`, the op's natural output type, not an untyped collection.
            [] => Shape::empty_polygon(),
            [point] => Shape::Point(*point),
            [first, second] => Shape::LineString(
                LineSeq::try_new(CoordSeq::from(vec![*first, *second]))
                    .expect("two unique polygon vertices form a line"),
            ),
            _ => Shape::Polygon(self),
        }
    }
}

impl Bounds {
    pub fn new(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Result<Self> {
        if minx.is_finite()
            && miny.is_finite()
            && maxx.is_finite()
            && maxy.is_finite()
            && minx <= maxx
            && miny <= maxy
        {
            Ok(Self::new_unchecked(minx, miny, maxx, maxy))
        } else {
            Err(GeometryErrorKind::InvalidRectangle.into())
        }
    }

    /// Geographic clip rectangle: finite latitudes with ``south <= north``;
    /// longitudes may use west>east to signal an antimeridian-crossing window.
    pub fn new_geographic(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Result<Self> {
        if minx.is_finite()
            && miny.is_finite()
            && maxx.is_finite()
            && maxy.is_finite()
            && miny <= maxy
        {
            Ok(Self::new_unchecked(minx, miny, maxx, maxy))
        } else {
            Err(GeometryErrorKind::InvalidRectangle.into())
        }
    }

    /// Whether these bounds cross the antimeridian (west longitude > east).
    pub fn crosses_antimeridian(self) -> bool {
        self.minx() > self.maxx()
    }

    pub fn intersects(self, other: Self) -> bool {
        self.minx() <= other.maxx()
            && self.maxx() >= other.minx()
            && self.miny() <= other.maxy()
            && self.maxy() >= other.miny()
    }

    /// Whether `other` lies entirely within these bounds (boundary-inclusive).
    pub fn contains(self, other: Self) -> bool {
        self.minx() <= other.minx()
            && self.maxx() >= other.maxx()
            && self.miny() <= other.miny()
            && self.maxy() >= other.maxy()
    }

    pub const fn into_tuple(self) -> (f64, f64, f64, f64) {
        (self.minx(), self.miny(), self.maxx(), self.maxy())
    }

    pub fn expand(self, distance: f64) -> Self {
        Self::new_unchecked(
            self.minx() - distance,
            self.miny() - distance,
            self.maxx() + distance,
            self.maxy() + distance,
        )
    }

    pub const fn from_point(point: Point) -> Self {
        Self::new_unchecked(point.x, point.y, point.x, point.y)
    }

    pub const fn from_xy(point: XY) -> Self {
        Self::new_unchecked(point.x, point.y, point.x, point.y)
    }

    pub fn from_xy_iter<I: IntoIterator<Item = XY>>(points: I) -> Self {
        let mut minx = f64::INFINITY;
        let mut miny = f64::INFINITY;
        let mut maxx = f64::NEG_INFINITY;
        let mut maxy = f64::NEG_INFINITY;
        for point in points {
            minx = minx.min(point.x);
            miny = miny.min(point.y);
            maxx = maxx.max(point.x);
            maxy = maxy.max(point.y);
        }
        Self::new_unchecked(minx, miny, maxx, maxy)
    }

    pub fn from_points<I: IntoIterator<Item = Point>>(points: I) -> Self {
        Self::from_xy_iter(points.into_iter().map(XY::from))
    }

    pub const fn into_array(self) -> [f64; 4] {
        [self.minx(), self.miny(), self.maxx(), self.maxy()]
    }

    pub fn pad_by_span(self, factor: f64) -> Self {
        let pad = (self.maxx() - self.minx()).max(self.maxy() - self.miny()) * factor;
        Self::new_unchecked(
            self.minx() - pad,
            self.miny() - pad,
            self.maxx() + pad,
            self.maxy() + pad,
        )
    }

    pub const fn corners(self) -> [[f64; 2]; 4] {
        [
            [self.minx(), self.miny()],
            [self.maxx(), self.miny()],
            [self.maxx(), self.maxy()],
            [self.minx(), self.maxy()],
        ]
    }

    pub(crate) fn contains_point(self, point: Point) -> bool {
        point.x >= self.minx()
            && point.x <= self.maxx()
            && point.y >= self.miny()
            && point.y <= self.maxy()
    }

    pub(crate) fn contains_xy(self, point: XY) -> bool {
        point.x >= self.minx()
            && point.x <= self.maxx()
            && point.y >= self.miny()
            && point.y <= self.maxy()
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.z(), self.m()) {
            (None, None) => write!(f, "{} {}", self.x, self.y),
            (Some(z), None) => write!(f, "{} {} {}", self.x, self.y, z),
            (None, Some(m)) => write!(f, "{} {} {}", self.x, self.y, m),
            (Some(z), Some(m)) => write!(f, "{} {} {} {}", self.x, self.y, z, m),
        }
    }
}
