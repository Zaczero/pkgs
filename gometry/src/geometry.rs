#![allow(
    clippy::similar_names,
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
mod frame;
pub(crate) use frame::{
    AxisFrame, axis_extent, axis_pow2_scale, power_of_two_exponent,
    scale_by_power_of_two, scaled_residual, unscale_area,
};
mod area;
pub(crate) mod exact_arith;
pub(crate) mod expansion;
pub(crate) use access::{
    REDUCE_LANES, REDUCE_SIMD_MIN, ReduceSimd, canonicalize_zero, centroid_ring_sums,
    column_all_finite, column_mean2, column_minmax,
    line_length_3d_columns, line_length_columns, pair_map4_guarded_f64, pair_select_mask,
    shape_axes_all_z, simd_argmin_f64, simd_indexed_map_f64, simd_map_f64, simd_mask_all,
    simd_mask_any, simd_select_indices, simd_xy_map_f64, squared_norm_untrusted_mask,
    try_simd_map_f64, xy_bounds_columns,
};
pub(crate) use area::{
    AreaSign, RingWinding, closed_columns_winding,
    open_point_cycle_decision, open_point_cycle_winding,
    open_xy_cycle_decision, open_xy_cycle_magnitude, open_xy_cycle_sign, open_xy_cycle_winding,
    cmp_area_keys_then_closed, cmp_area_keys_then_open_columns, polygon_area_measure_columns,
    polygon_area_measure_with, ring_area_measure, ring_decision_area, ring_winding,
};
pub(crate) use segments::{Orientation, held_axis_interpolate, orientation};
pub(crate) mod types;
pub(crate) use types::{
    Bounds, Bounds3D, BufferCapStyle, BufferJoinStyle, BufferSide, CoordSeq, CoordSeqBuilder,
    CoordWindow, CoordinateAxes, Coordinates, CsrOffsetBuilder, CsrOffsetColumn, DistanceParts,
    FrameDependentCaches, GeodesicMetric, GeodesicParts, GeodesicSegment, GeodesicSegmentWitness,
    GeometryErrorKind, GeometryKind, HasM, HasZ, IndexedSegment, LineSeq, MAX_NESTING, MOrdinate,
    MultiLineString, MultiMember, MultiPoint, MultiPolygon, Point, PointKey, Polygon, PolygonLevel,
    PolygonizeFull, PolylabelCell, PreparedPlan, RepairMethod, Ring, RingLevel, Segment, Shape,
    ShapeData, SimplifyMethod, SmoothMethod, ValidationIssue, VoronoiBoundary, XY, ZOrdinate,
    canonical_f64_bits, coerce_to_common_axes, concat_coord_columns, ordered_edge,
    reverse_column_arc, segment_midpoint,
};
mod antimeridian;
pub(crate) use antimeridian::{
    DerivedPointStrategy, crosses_antimeridian_coords_in_frame, crosses_antimeridian_in_frame,
    geographic_crossing, geographic_crossing_bounds,
    geographic_crossing_bounds_for_shapes_in_frame, geographic_crossing_bounds_in_frame,
    is_ring_data_in_frame, is_simple_data_in_frame, repair_data_in_frame, repair_shape_in_frame,
    self_intersections_in_frame, topology_domain, unary_antimeridian_derived,
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
pub(crate) use dimension::{Dimension, dimension, ring_is_collinear};
mod derived;
pub(crate) use derived::{
    centroid_line_row_columns, centroid_polygon_row_columns, geometric_xy_cmp, native_concave_hull,
    point_on_surface_line_columns, point_on_surface_polygon_row_columns, shape_from_selected_hull,
    smallest_enclosing_circle,
};
mod distance;
mod facet_bvh;
mod generated_budget;
mod geodesic_bvh;
mod line_index;
mod linear;
mod linear_3d;
mod ordinates;
mod overlay;
pub(crate) use overlay::overlay_all;
pub(crate) mod packed_line_metrics;
mod point_location;
mod predicates;
mod provenance;
pub(crate) use provenance::{VertexProvenance, emit_from_original};
mod relate;
mod relate_pattern;
pub(crate) use relate_pattern::CompiledPattern;
mod segment_index;
pub(crate) use segment_index::{EdgeId, EdgeYIndex, uses_linear_plan_for_len};
mod segments;
mod tessellation;
mod topology;
pub(crate) use topology::{TopologyDomain, TopologyGraph};
mod util;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;

use access::{
    compare_f64, compare_point_slices, compare_points, compare_polygons, compare_shapes,
    line_length, linework_first_point, simd_reduce_f64, simd_xy_map_one_f64,
};
pub(crate) use arrangement::Arrangement;
pub(crate) use bvh_code::{BvhCode, BvhNode};
#[doc(hidden)]
pub(crate) use constructive::vw_keep;
use constructive::{
    Arc, SnapReference, assemble_region_polygons, canonical_ring, canonical_ring_seq,
    decimal_scale, orient_ring, quantize_to_scale, snap_ring_to_reference, winding_region,
};
pub(crate) use constructive::{
    DEFAULT_MITER_LIMIT, SegmentPlacement, affine_about_origin_ordinate,
    densify_points_budgeted_with_placement, rdp_keep, segmentize_points_budgeted,
    stable_affine_xy, stable_snap_ordinate,
};
pub(crate) use coverage::{
    CoverageOverlapRule, HashMapExt, RTreeObject, coverage_clean, coverage_invalid_edges,
    coverage_is_valid, coverage_simplify, coverage_union_topo,
};
pub(in crate::geometry) use coverage::{TopologyGuard, simplify_chain_guarded, triangle_area};
pub(crate) use distance::{
    Distance3dParts, FrechetDpScratch, bounds_distance_squared, distance_3d_with_parts,
    frechet_distance_line_columns_batch, hausdorff_distance_line_columns_batch,
};
use distance::{GeodesicSweepCaps, line_length_3d};
#[doc(hidden)]
pub(crate) use distance::{frechet_distance_line_columns, hausdorff_distance_line_columns};
pub(crate) use facet_bvh::{
    BVH_MIN_INDEXED_SEGMENTS, FacetBvh, NearestCandidate, PreparedLinework,
};
pub(crate) use generated_budget::{ExpansionBudget, GENERATED_ITEM_LIMIT};
use geodesic_bvh::{
    GeodesicFacetBvh, GeodesicWitnessCandidate, geodesic_witness_is_better, witness_refine_limit,
};
pub(crate) use line_index::{LineIndex, LineIndexSlot, PlanarMetric, SegmentMetric};
pub(crate) use linear::{MeasureRange, MeasuredLineIndex, MeasuredLineIndexSlot};
use linear::{normalize_line_distance, push_distinct_point};
pub(crate) use ordinates::{
    carry_direct_point_ordinates, carry_direct_points_ordinates, carry_overlay_ordinates,
    carry_direct_overlay_ordinates, carry_overlay_ordinates_from_graph,
};
use ordinates::{carry_each, carry_ordinates};
pub(crate) use overlay::{OverlayOp, Strictness, empty_shape_for_dimension_with_axes};
use overlay::{
    binary_areal_overlay, clip_multipoint_columns, clip_polygonal, clipped_line_parts,
    clipped_linestring, line_parts_to_shape, polygon_parts_to_shape, rect_polygon,
    self_node_segments,
};
#[doc(hidden)]
pub(crate) use packed_line_metrics::PackedLineColumnView;
pub(crate) use packed_line_metrics::scale_metric_values;
pub(crate) use point_location::{PointBatchTester, PolygonPointIndex};
pub(in crate::geometry) use segments::{DdInterval, dd_strict_cmp};
use predicates::{
    LineworkChains, TouchDirections, classify_ring_pair, face_interior_point, polygon_rings_issue,
    ring_classify_point, ring_contains_interior, ring_edge_membership, ring_encloses_pole,
    ring_label, ring_probe_point, segment_intersection_is_simple, segments_are_adjacent,
    settle_touches, shell_is_convex, simplified_polygon_delta_is_simple, validate_ring,
    visit_interacting_pairs,
};
pub(crate) use predicates::{
    RingClass, RingEdgeMembership, line_crosses_antimeridian, line_is_ccw, line_is_closed,
    line_is_simple, line_is_valid, polygon_is_valid, shape_encloses_pole_in_frame,
    shape_has_polar_ring_in_frame, shape_reaches_geographic_pole_in_frame,
};
pub(crate) use util::{
    bounds_to_shape, columns_within, dedup_consecutive_points, dedup_consecutive_xy,
    empty_geometry, geographic_bounds_to_shape, point_equals_exact, points_equal_exact,
    same_active_position, same_point, same_topological_coordinate, topology_coordinate_bits_simd,
    unique_xy_points, wrap_index,
};

/// Which optional ordinate an edit replaces or clears.
#[derive(Clone, Copy)]
pub(crate) enum OrdinateAxis {
    Z,
    M,
}

/// Replace one optional ordinate on a point without rebuilding a coordinate sequence.
pub(crate) fn set_point_ordinate(
    point: Point,
    axis: OrdinateAxis,
    value: Option<f64>,
    overwrite: bool,
) -> Point {
    let current = match axis {
        OrdinateAxis::Z => point.z(),
        OrdinateAxis::M => point.m(),
    };
    let value = match value {
        None => None,
        Some(value) if overwrite || current.is_none() => Some(value),
        Some(_) => current,
    };
    match axis {
        OrdinateAxis::Z => Point::new_raw_axes(
            point.x,
            point.y,
            ZOrdinate(value),
            MOrdinate(point.m()),
        ),
        OrdinateAxis::M => Point::new_raw_axes(
            point.x,
            point.y,
            ZOrdinate(point.z()),
            MOrdinate(value),
        ),
    }
}

/// Whether raw coordinate bounds can represent an operand in a geographic frame.
///
/// Longitude spellings cannot be compared as Cartesian coordinates across the
/// seam, at a pole, or when the frame would canonicalize either bound edge.
/// A rectangle supplies no shape, so it only needs the latter check.
pub(crate) fn raw_coordinate_bounds_are_sound_in_frame(
    shape: Option<&Shape>,
    bounds: Bounds,
    frame: crate::geographic::GeographicFrame,
) -> bool {
    shape.is_none_or(|shape| {
        !crosses_antimeridian_in_frame(shape, frame)
            && !shape_reaches_geographic_pole_in_frame(shape, frame)
    }) && {
        let unmoved = |x: f64| {
            frame
                .longitude()
                .canonicalize(x)
                .is_ok_and(|canonical| canonical.get().to_bits() == x.to_bits())
        };
        unmoved(bounds.minx()) && unmoved(bounds.maxx())
    }
}

/// Re-export seam: `pub(super)` kernel in the private `predicates` child.
pub(crate) fn polygon_row_point_membership<const BOUNDARY: bool>(
    coords: &CoordSeq,
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
    point: Point,
) -> bool {
    predicates::polygon_row_point_membership::<BOUNDARY>(coords, ring_offsets, rings, point)
}
pub(crate) use relate::{
    De9im, GeographicPrepared, graph_intersects_bounds, graph_relate_data,
    graph_relate_data_geographic, graph_relate_data_geographic_with_left_graph,
    graph_relate_data_geographic_with_right_graph, graph_relate_data_with_left_graph,
    graph_relate_data_with_right_graph, graph_relate_shapes, is_axis_aligned_rectangle,
    line_scalar_prefers_direct_polygon_relate,
};
pub(crate) use relate::{De9imInterval, Loc, OperandShape};
pub(crate) use relate::{
    StagedLinework, StagedRings, collect_polygons, direct_areal_linework_predicate,
    direct_areal_linework_pattern, direct_areal_pattern, direct_areal_predicate,
    direct_linework_pattern, direct_linework_predicate, ring_has_area, stage_linework, stage_rings,
};
pub(in crate::geometry) use segment_index::{
    MonotoneRun, candidate_pairs_between, candidate_pairs_over_staged_runs, for_each_candidate_pair,
    for_each_indexed_overlapping_bounds_pair, for_each_overlapping_bounds_pair, sign_of,
    single_chain, try_for_each_candidate_pair,
};
pub(crate) use segment_index::{PointSetIndex, SEGMENT_INDEX_MIN_PAIRS, SegmentIndex};
pub(crate) use segments::clip_segment;
/// Convex lerp shared with the broadcast fallback lanes (see
/// `broadcast::metrics::points`), so packed and fallback agree at the extremes.
pub(crate) use segments::{interpolate_f64, interpolate_fraction};
use segments::{
    CCW_ERRBOUND_A, Contact, ON_SEGMENT_EPSILON, SegmentProjection, SharedSpan, SquaredDistanceKey,
    interpolate_optional, interpolate_segment_point, nearest_points, orientation_xy,
    point_pair_distance_key, point_segment_distance, point_segment_distance_key,
    point_segment_distance_squared, point_segment_selection, ray_crossing_is_right,
    segment_contact_exact, segment_contact_point, segment_cross_point, segment_envelopes_disjoint,
    segment_projection, segments_intersect, shared_segment_part, try_point_distance_squared,
    try_point_segment_distance_squared, undirected_edge_key, undirected_segment_edge_key,
};
/// The guarded-sqrt distance is the one segment-kernel helper the broadcast
/// lanes call directly (packed point columns).
pub(crate) use segments::{
    RectContact, finish_planar_squared_min, lerp_point, line_segments, point_distance,
    point_distance_squared, point_on_segment, points_dwithin, segment_vs_rect,
    squared_norm_is_trustworthy,
};
pub(crate) use tessellation::{CdtRefinement, row_sample_seed};
use tessellation::{delaunay_triangulation, point_in_ccw_triangle};
pub(in crate::geometry) use types::GeodesicPartsKey;

use crate::collections::{HashMap, HashSet};
use crate::error::Result;

#[derive(Clone, Debug)]
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
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
pub(crate) fn nearest_line(pair: Option<(Point, Point)>, common: CoordinateAxes) -> Shape {
    match pair {
        Some((start, end)) => Shape::LineString(LineSeq::segment(start, end)),
        None => Shape::typed_empty(GeometryKind::LineString, common),
    }
}

/// The `(left, right)` witness Shapes for `nearest_points`: the points dropped to
/// their common axes, or `(POINT EMPTY, POINT EMPTY)` when an operand is empty.
/// Mirrors [`nearest_line`] so the tuple and the line stay consistent.
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
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
            Shape::typed_empty(GeometryKind::Point, common),
            Shape::typed_empty(GeometryKind::Point, common),
        ),
    }
}

impl Shape {
    /// The canonical `POINT EMPTY` (XY).
    pub const fn empty_point() -> Self {
        Self::Empty(GeometryKind::Point, CoordinateAxes::XY)
    }

    /// The canonical `POLYGON EMPTY` (XY).
    pub const fn empty_polygon() -> Self {
        Self::Empty(GeometryKind::Polygon, CoordinateAxes::XY)
    }

    /// The canonical typed empty of `kind` carrying `axes`. Multipart members
    /// are stored inside their owning wrapper; an empty top-level multipart is
    /// always this representation.
    pub(crate) const fn typed_empty(kind: GeometryKind, axes: CoordinateAxes) -> Self {
        Self::Empty(kind, axes)
    }

    /// A dense `MULTIPOINT`, or `MULTIPOINT EMPTY` carrying `empty_axes` when
    /// the sequence has no coordinates. An empty multipart is a distinct
    /// variant, so admitting the sequence here is what stops every caller from
    /// re-deriving that rule — and from constructing the unrepresentable one.
    pub(crate) fn multi_point(empty_axes: CoordinateAxes, coords: CoordSeq) -> Self {
        MultiPoint::from_coordseq(coords).map_or_else(
            |_| Self::Empty(GeometryKind::MultiPoint, empty_axes),
            Self::MultiPoint,
        )
    }

    /// A dense `MULTILINESTRING`, or `MULTILINESTRING EMPTY` carrying
    /// `empty_axes` when there are no members.
    pub(crate) fn multi_line_string(empty_axes: CoordinateAxes, values: Vec<LineSeq>) -> Self {
        MultiLineString::from_dense(values).map_or_else(
            || Self::Empty(GeometryKind::MultiLineString, empty_axes),
            Self::MultiLineString,
        )
    }

    /// A dense `MULTIPOLYGON`, or `MULTIPOLYGON EMPTY` carrying `empty_axes`
    /// when there are no members.
    pub(crate) fn multi_polygon(empty_axes: CoordinateAxes, values: Vec<Polygon>) -> Self {
        MultiPolygon::from_dense(values).map_or_else(
            || Self::Empty(GeometryKind::MultiPolygon, empty_axes),
            Self::MultiPolygon,
        )
    }

    /// The narrowest shape for a point decomposition: `POINT EMPTY` for none,
    /// the bare point for one, a dense multipoint otherwise.
    pub(crate) fn narrow_points(empty_axes: CoordinateAxes, values: Vec<Point>) -> Self {
        match <[Point; 1]>::try_from(values) {
            Ok([point]) => Self::Point(point),
            Err(values) if values.is_empty() => Self::Empty(GeometryKind::Point, empty_axes),
            Err(values) => Self::multi_point(empty_axes, CoordSeq::from_points(&values)),
        }
    }

    /// The narrowest shape for a lineal decomposition: `LINESTRING EMPTY` for
    /// none, the bare linestring for one, a dense multilinestring otherwise.
    pub(crate) fn narrow_lines(empty_axes: CoordinateAxes, values: Vec<LineSeq>) -> Self {
        match <[LineSeq; 1]>::try_from(values) {
            Ok([line]) => Self::LineString(line),
            Err(values) if values.is_empty() => Self::Empty(GeometryKind::LineString, empty_axes),
            Err(values) => Self::multi_line_string(empty_axes, values),
        }
    }

    /// The narrowest shape for a polygonal decomposition: `POLYGON EMPTY` for
    /// none, the bare polygon for one, a dense multipolygon otherwise.
    pub(crate) fn narrow_polygons(empty_axes: CoordinateAxes, values: Vec<Polygon>) -> Self {
        match <[Polygon; 1]>::try_from(values) {
            Ok([polygon]) => Self::Polygon(polygon),
            Err(values) if values.is_empty() => Self::Empty(GeometryKind::Polygon, empty_axes),
            Err(values) => Self::multi_polygon(empty_axes, values),
        }
    }

    /// Visit every line segment — linework edges of (multi)linestrings, ring
    /// edges of (multi)polygons, recursively through collections — without
    /// materializing a `Vec`. Point-only shapes have no segments.
    pub(crate) fn for_each_segment(&self, mut visit: impl FnMut(Segment)) {
        self.for_each_segment_chain(|chain| {
            line_segments(chain).for_each(&mut visit);
        });
    }

    /// [`for_each_segment`](Self::for_each_segment) with FULL ordinate
    /// carry: each edge as its two `Point`s (Z/M intact) — the ordinate
    /// lanes' traversal (the planar `Segment` is XY-only by design).
    pub(crate) fn for_each_vertex_pair(&self, mut visit: impl FnMut(Point, Point)) {
        self.for_each_segment_chain(|chain| {
            for [start, end] in chain.segment_pairs() {
                visit(start, end);
            }
        });
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
            Self::Polygon(polygon) => polygon.rings().map(|ring| ring.coordinate_bytes()).sum(),
            Self::MultiPolygon(polygons) => polygons
                .iter()
                .flat_map(Polygon::rings)
                .map(|ring| ring.coordinate_bytes())
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
            Self::Empty(GeometryKind::LineString | GeometryKind::MultiLineString, _) => {
                Ok(Vec::new())
            },
            _ => Err(GeometryErrorKind::LineStringRequired.into()),
        }
    }

    pub(crate) fn single_linework(&self) -> Result<&CoordSeq> {
        match self {
            Self::LineString(points) if !points.is_empty() => Ok(points),
            Self::MultiLineString(lines)
                if lines.len() == 1 && !lines.first().is_some_and(|line| line.is_empty()) =>
            {
                Ok(lines.first().expect("one line"))
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
            Self::Empty(GeometryKind::Point, _) => Ok(Vec::new()),
            _ => Err(GeometryErrorKind::PointSplitterRequired.into()),
        }
    }
}

impl Polygon {
    /// The one `Arc<[Ring]>` every hole-free polygon shares.
    ///
    /// `Vec::new().into()` allocates an `ArcInner` even for zero elements, so
    /// the common case — a polygon with no holes — paid one heap allocation
    /// each. Sharing a single empty makes that a refcount bump. The field type
    /// stays `Arc<[Ring]>` deliberately: `Option<Arc<[Ring]>>` is the same 16
    /// bytes via the null niche, but it would rewrite every `.holes` access
    /// site for the same win.
    fn empty_holes() -> Arc<[Ring]> {
        static EMPTY: std::sync::LazyLock<Arc<[Ring]>> = std::sync::LazyLock::new(|| Arc::from([]));
        Arc::clone(&EMPTY)
    }

    pub fn new(shell: Ring, holes: Vec<Ring>) -> Self {
        Self {
            shell,
            holes: if holes.is_empty() {
                Self::empty_holes()
            } else {
                holes.into()
            },
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
    pub fn rings(&self) -> impl Iterator<Item = &Ring> + '_ {
        std::iter::once(&self.shell).chain(self.holes.iter())
    }

    fn area(&self) -> f64 {
        polygon_area_measure_with(|visit| {
            visit(self.shell.coords().xs(), self.shell.coords().ys(), false);
            for hole in self.holes.iter() {
                visit(hole.coords().xs(), hole.coords().ys(), true);
            }
        })
    }

    fn bounds(&self) -> Option<Bounds> {
        self.rings()
            .filter_map(|ring| Bounds::from_coords(ring.coords()))
            .reduce(|left, right| {
                Bounds::new_raw(
                    left.minx().min(right.minx()),
                    left.miny().min(right.miny()),
                    left.maxx().max(right.maxx()),
                    left.maxy().max(right.maxy()),
                )
            })
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
        self.try_map_ring_seqs(|ring| ring.try_map_xy_with(op))
    }

    /// Fallible per-ring `CoordSeq` transform over the shell and every hole,
    /// generic over the error. The columnar counterpart to
    /// [`map_ring_seqs`](Self::map_ring_seqs): preserves ring validity
    /// (closure and length) with no `AoS` `Vec<Point>` round-trip, and is
    /// the ring engine behind the SIMD affine / snap-to-grid column kernels
    /// (and any other fallible column map).
    fn try_map_ring_seqs<E>(
        &self,
        mut transform: impl FnMut(&CoordSeq) -> Result<CoordSeq, E>,
    ) -> Result<Self, E> {
        let shell = Ring::from_closed(transform(self.shell.coords())?);
        let holes = if self.holes.is_empty() {
            Self::empty_holes()
        } else {
            self.holes
                .iter()
                .map(|hole| Ok(Ring::from_closed(transform(hole.coords())?)))
                .collect::<Result<Arc<[Ring]>, E>>()?
        };
        Ok(Self { shell, holes })
    }

    fn segmentize_budgeted(
        &self,
        max_segment_length: f64,
        placement: SegmentPlacement<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        self.try_map_ring_seqs(|ring| {
            segmentize_points_budgeted(ring, max_segment_length, placement, budget)
        })
    }

    fn densified_budgeted_with_placement(
        &self,
        fraction: f64,
        placement: SegmentPlacement<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        self.try_map_ring_seqs(|ring| {
            densify_points_budgeted_with_placement(ring, fraction, placement, budget)
        })
    }

    fn reverse(&self) -> Self {
        self.map_ring_seqs(CoordSeq::reversed)
    }

    /// Rebuild every ring through a columnar transform that preserves ring
    /// validity (closure and length) — no `AoS` `Vec<Point>` round-trip.
    pub(crate) fn map_ring_seqs(&self, mut transform: impl FnMut(&CoordSeq) -> CoordSeq) -> Self {
        let shell = Ring::from_closed(transform(self.shell.coords()));
        let holes = if self.holes.is_empty() {
            Self::empty_holes()
        } else {
            self.holes
                .iter()
                .map(|hole| Ring::from_closed(transform(hole.coords())))
                .collect::<Arc<[Ring]>>()
        };
        Self { shell, holes }
    }

    fn orient(&self, exterior_cw: bool) -> Self {
        let shell = constructive::oriented_ring(&self.shell, exterior_cw);
        let holes = if self.holes.is_empty() {
            Self::empty_holes()
        } else {
            self.holes
                .iter()
                .map(|hole| constructive::oriented_ring(hole, !exterior_cw))
                .collect::<Arc<[Ring]>>()
        };
        Self { shell, holes }
    }

    fn normalize(&self) -> Self {
        // Canonical winding follows RFC 7946 (GeoJSON): exterior CCW,
        // holes CW — the modern convention, deliberately not GEOS's
        // CW-shell legacy. Rings rotate min-vertex-first; holes sort
        // ascending like every other canonical sequence.
        let mut holes = self
            .holes
            .iter()
            .map(|hole| Ring::from_closed(canonical_ring_seq(hole.coords(), true)))
            .collect::<Vec<_>>();
        holes.sort_by(compare_point_slices);
        Self::new(
            Ring::from_closed(canonical_ring_seq(self.shell.coords(), false)),
            holes,
        )
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
        self.try_map_ring_seqs(|ring| {
            ring.iter().map(transform).collect::<Result<Vec<_>>>().map(CoordSeq::from)
        })
    }

    fn snap_to_reference(&self, reference: &SnapReference) -> Self {
        self.map_ring_seqs(|ring| CoordSeq::from(snap_ring_to_reference(ring, reference)))
    }

    /// Total ring-edge count across the shell and every hole — `O(rings)`,
    /// for pre-sizing collectors.
    fn segment_count(&self) -> usize {
        self.rings().map(|ring| ring.len().saturating_sub(1)).sum()
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
            Ok(Self::new_raw(minx, miny, maxx, maxy))
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
            Ok(Self::new_raw(minx, miny, maxx, maxy))
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
        Self::new_raw(
            self.minx() - distance,
            self.miny() - distance,
            self.maxx() + distance,
            self.maxy() + distance,
        )
    }

    pub const fn from_point(point: Point) -> Self {
        Self::new_raw(point.x, point.y, point.x, point.y)
    }

    pub const fn from_xy(point: XY) -> Self {
        Self::new_raw(point.x, point.y, point.x, point.y)
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
        Self::new_raw(minx, miny, maxx, maxy)
    }

    pub fn from_points<I: IntoIterator<Item = Point>>(points: I) -> Self {
        Self::from_xy_iter(points.into_iter().map(XY::from))
    }

    pub const fn into_array(self) -> [f64; 4] {
        [self.minx(), self.miny(), self.maxx(), self.maxy()]
    }

    pub fn pad_by_span(self, factor: f64) -> Self {
        let pad = (self.maxx() - self.minx()).max(self.maxy() - self.miny()) * factor;
        Self::new_raw(
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
