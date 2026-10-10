#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
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

/// Runtime axis flags select const-generic loops at their callers, avoiding
/// per-coordinate axis checks. The expanded block remains in the caller, so
/// its `?`, returns, and error order do too.
macro_rules! dispatch_axes {
    (($has_z:expr, $has_m:expr), $z:ident, $m:ident, $body:block) => {
        match ($has_z, $has_m) {
            (false, false) => {
                const $z: bool = false;
                const $m: bool = false;
                $body
            },
            (true, false) => {
                const $z: bool = true;
                const $m: bool = false;
                $body
            },
            (false, true) => {
                const $z: bool = false;
                const $m: bool = true;
                $body
            },
            (true, true) => {
                const $z: bool = true;
                const $m: bool = true;
                $body
            },
        }
    };
}
pub(crate) use dispatch_axes;

mod access;
mod frame;
pub(crate) use frame::{
    AxisFrame, axis_extent, axis_pow2_scale, power_of_two_exponent, scale_by_power_of_two,
    scaled_residual, unscale_area,
};
mod area;
pub(crate) mod exact_arith;
pub(crate) mod expansion;
mod geographic_area;
pub(crate) use access::{
    Component, CoordRun, Lineal, MetricSupportRun, PointSet, Polygonal, REDUCE_LANES,
    REDUCE_SIMD_MIN, ReduceSimd, RunIdentity, SharedShapeOwners, SupportRunKind, canonicalize_zero,
    centroid_ring_sums, column_all_finite, column_mean2, column_minmax, line_length_3d_columns,
    line_length_columns, pair_map4_guarded_f64, pair_select_mask, shape_axes_all_z,
    simd_argmin_f64, simd_indexed_map_f64, simd_map_f64, simd_xy_map_f64,
    squared_norm_untrusted_mask, try_simd_map_f64, xy_bounds_columns, xy_finite_bounds_columns,
};
#[cfg(test)]
pub(crate) use area::open_point_cycle_magnitude;
pub(crate) use area::{
    ExactFactorialArea, ExactRationalArea, RingWinding, closed_columns_winding,
    open_point_cycle_winding, open_xy_cycle_magnitude, open_xy_cycle_winding,
    polygon_area_measure_columns, polygon_area_measure_with, ring_winding,
};
pub(crate) use geographic_area::{
    GeographicAreaDensity, MAX_AUTHALIC_HARMONICS, polygon_area as geographic_polygon_area,
};
pub(crate) use segments::{Orientation, held_axis_interpolate, orientation};
pub(crate) mod storage;
pub(crate) mod types;
pub(crate) use types::{
    Bounds, BufferCapStyle, BufferJoinStyle, BufferSide, ChartBox, ChartEdgeDifferential,
    ChartEdgeDifferentialBounds, ChartMetric, ChartPointDistanceSample, CoordSeq, CoordSeqBuilder,
    CoordWindow, CoordinateAxes, Coordinates, CsrOffsetBuilder, CsrOffsetColumn, DistanceParts,
    FrameDependentCaches, GeodesicMetric, GeodesicNumericalPolicy, GeometryErrorKind, GeometryKind,
    HasM, HasZ, IndexedSegment, IntoSharedShape, LineSeq, MAX_NESTING, MColumn, MOrdinate,
    MultiLineString, MultiMember, MultiPoint, MultiPolygon, Point, PointEdgeMinimum,
    PointEdgeQuery, PointEdgeQueryResult, PointKey, Polygon, PolygonLevel, PolygonizeFull,
    PreparedPlan, RepairMethod, Ring, RingLevel, Segment, Shape, ShapeCountQuery, ShapeData,
    ShapeOwnerKey, SharedSlice, SimplifyMethod, SmoothMethod, ValidationIssue, VisiblePointRunKey,
    VisibleXYKey, VoronoiBoundary, XY, ZOrdinate, canonical_f64_bits, coerce_to_common_axes,
};
mod antimeridian;
mod boundary;
#[cfg(test)]
pub(crate) use antimeridian::self_intersections_in_frame;
pub(crate) use antimeridian::{
    DerivedPointStrategy, GeographicDerivedPointScan, GeographicTopologyRoute, UnwrapChartError,
    antimeridian_frame, chain_crosses_antimeridian, classify_geographic_chart,
    classify_geographic_validity_chart, crosses_antimeridian_in_frame, faithful_geographic_chain,
    geographic_crossing_bounds, geographic_topology_route, is_ring_data_in_frame,
    is_simple_data_in_frame, repair_data_in_frame, repair_shape_in_frame,
    self_intersections_data_in_frame, topology_domain, topology_domain_at,
    unary_antimeridian_centroid, unary_antimeridian_derived, unwrap_contact_chart,
    unwrap_coordinates, validate_data_in_frame, validate_shape_in_frame,
};
pub(crate) use boundary::boundary_with_metric;
mod computed_shape;
pub(crate) use computed_shape::{
    PolygonPartEdgeRoles, PublishedTriangleVertices, map_computed_shape_chains,
    publish_changed_raw_computed_shape, publish_computed_line_column_with_budget,
    publish_computed_point_or_raw_shape, publish_polygon_parts_with_edge_roles,
    publish_polygon_with_edge_roles_and_context, publish_raw_computed_shape,
    publish_raw_computed_shape_with_budget, publish_raw_computed_shape_with_budget_and_context,
    publish_raw_computed_shape_with_context, publish_raw_computed_shapes_with_budget_and_context,
    publish_raw_computed_shapes_with_context, publish_stored_computed_shape,
    publish_transformed_chain, publish_transformed_shape, publish_triangle_vertex_stream_budgeted,
    publish_voronoi_polygon_outputs,
};
mod arrangement;
mod bvh_code;
mod chart_bvh;
mod constructive;
mod coverage;
pub(crate) use coverage::vw_area_tolerance;
mod dimension;
pub(crate) use dimension::{Dimension, dimension, ring_is_collinear};
mod derived;
pub(crate) use derived::{
    PointSetCenter, centroid_line_row_columns, centroid_polygon_row_columns,
    centroid_polygon_row_columns_with_bounds, exact_point_chart_centroid, geometric_xy_cmp,
    minimum_area_rectangle_chart, monotone_chain_chart_hull, native_concave_hull,
    point_on_surface_line_columns, point_on_surface_polygon_row_columns, point_set_center_summary,
    point_set_surface_point_at, shape_from_selected_hull, smallest_enclosing_circle,
};
mod distance;
pub(in crate::geometry) use distance::{ExactPoint, point_segment_distance_squared_exact};
pub(crate) use distance::{RowMetricPair, discrete_frechet_distance_with_scratch};
pub(crate) use types::coordinate_columns_eq;
mod embedding_search;
pub(crate) use embedding_search::{Unmetered, Work};
mod facet_bvh;
mod generated_budget;
mod line_index;
mod linear;
mod linear_3d;
mod ordinates;
pub(crate) use ordinates::{
    ParallelSourceArcMetricAccess, RootMetricState, SourceArcContext, SourceArcMetricAccess,
    computed_native_sample_ordinates, source_ordinate_values_at_fraction,
};
mod perimeter;
pub(crate) use perimeter::{geographic_packed_polygonal_perimeter, geographic_polygonal_perimeter};
mod overlay;
pub(crate) use overlay::{
    occupied_cartesian_area, occupied_support_shape, overlay_all, overlay_all_geographic_chart,
    overlay_all_with_metric,
};
pub(crate) mod packed_line_metrics;
mod point_location;
mod predicates;
mod provenance;
pub(crate) use provenance::{RefinedChainProvenance, VertexProvenance, emit_from_original};
mod relate;
mod relate_pattern;
pub(crate) use relate_pattern::CompiledPattern;
mod segment_index;
pub(crate) use segment_index::{EdgeId, EdgeYIndex, uses_linear_plan_for_len};
pub(in crate::geometry) use segment_index::{OrderTree, PersistentOrder};
mod segments;
pub(crate) mod source_support;
pub(in crate::geometry) use source_support::compare_rays;
pub(crate) use source_support::{
    RingKind, RingView, cartesian_area_covers_point, cartesian_point_covers,
    cartesian_point_location, edge_winding, ring_kind_columns,
};
mod tessellation;
mod topology;
pub(crate) use topology::{
    ExactGeographicRectangleLocation, ExactGeographicTopology, TopologyDomain, VertexKey,
};
mod util;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;

use access::{
    compare_f64, compare_point_slices, compare_points, compare_polygons, compare_shapes,
    line_length, linework_first_point, simd_reduce_f64, simd_xy_map_one_f64,
};
pub(crate) use bvh_code::{BvhCode, BvhNode};
pub(crate) use chart_bvh::ChartEdgeBvh;
pub(crate) use constructive::{
    AffineLinear, DEFAULT_MITER_LIMIT, GeographicRdpGuard, OriginAffine, OriginSpec,
    PlanarRdpGuard, RdpEdgeSpanGuard, SegmentPlacement, SegmentPlacementOwned,
    densify_points_budgeted_with_placement, rdp_keep_with_edge_guard, segmentize_points_budgeted,
    segmentize_points_with_piece_counts_budgeted, stable_affine_xy, stable_snap_ordinate, vw_keep,
    vw_keep_geographic,
};
use constructive::{
    SnapReference, decimal_scale, orient_ring, quantize_to_scale, snap_ring_to_reference,
};
pub(crate) use coverage::{
    CoverageOverlapRule, HashMapExt, coverage_clean, coverage_invalid_edges, coverage_is_valid,
    coverage_simplify_with_context, coverage_union_topo,
};
pub(in crate::geometry) use coverage::{TopologyGuard, simplify_chain_guarded, triangle_area};
use distance::line_length_3d;
pub(crate) use distance::{
    DirectedOptimizationResult, Distance3dParts, FrechetDpScratch, PreparedSupport,
    bounds_distance_squared, directed_inradius_scaled, distance_3d_with_parts,
    ensure_geographic_hausdorff_linework, frechet_distance_line_columns,
    frechet_distance_line_columns_batch, hausdorff_distance, hausdorff_distance_line_columns,
    hausdorff_distance_line_columns_batch,
};
pub(crate) use facet_bvh::{
    BVH_MIN_INDEXED_SEGMENTS, ClearanceFacetBvh, FacetBvh, NearestCandidate, NearestTarget,
    PreparedLinework,
};
pub(crate) use generated_budget::{ExpansionBudget, GENERATED_ITEM_LIMIT};
pub(crate) use line_index::{
    ChartLineIndex, LineIndex, LineIndexSlot, PlanarMetric, SegmentMetric, locate_planar_route,
};
use linear::normalize_line_distance;
pub(crate) use linear::{MeasureRange, MeasuredLineIndex, MeasuredLineIndexSlot};
use ordinates::carry_ordinates;
pub(crate) use ordinates::carry_overlay_ordinates;
#[cfg(test)]
pub(crate) use overlay::cartesian_support_nonempty;
use overlay::polygon_parts_to_shape;
pub(crate) use overlay::{OverlayOp, Strictness, exact_intersects_bounds, rect_polygon};
pub(crate) use packed_line_metrics::{PackedLineColumnView, scale_metric_values};
pub(in crate::geometry) use point_location::{
    AreaLocationLabels, ExactLocationIndex, ExactLocationPoints,
};
pub(crate) use point_location::{PointBatchTester, PolygonPointIndex, PreparedPointQueries};
#[cfg(test)]
pub(crate) use predicates::ring_bounded_pole;
use predicates::{
    LineworkChains, TouchDirections, classify_ring_pair, classify_ring_pair_with_contacts,
    face_interior_point, polygon_rings_issue_with_contacts, ring_classify_point,
    ring_contains_interior, ring_encloses_pole, ring_label, ring_probe_point,
    segment_intersection_is_simple, segments_are_adjacent, settle_touches, shell_is_convex,
    simplified_polygon_delta_is_simple, visit_interacting_pairs,
};
pub(in crate::geometry) use predicates::{PolygonContact, remove_antenna_vertices};
pub(crate) use predicates::{
    RingClass, fan_alternations, line_is_ccw, line_is_closed_in_domain, line_is_simple,
    line_is_valid, polygon_is_valid, ring_classify_point_columns, shape_encloses_pole_in_frame,
    shape_has_polar_ring_in_frame, shape_is_closed_in_domain,
    shape_reaches_geographic_pole_in_frame, validate_line, validate_point, validate_points,
    validate_ring,
};
pub(in crate::geometry) use segments::{
    DdInterval, SegmentParameter, clip_segment_parameter, dd_strict_cmp, exact_crossing_parameter,
};
#[cfg(test)]
pub(crate) use util::dedup_consecutive_xy;
pub(crate) use util::{
    bounds_to_shape, columns_within, dedup_consecutive_points, empty_geometry,
    geographic_bounds_chart, geographic_bounds_to_shape, point_equals_exact, points_equal_exact,
    push_distinct_active_position, push_distinct_xy_position, same_active_position, same_point,
    same_topological_coordinate, topology_coordinate_bits_simd, unique_xy_points, wrap_index,
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
        OrdinateAxis::Z => {
            Point::new_raw_axes(point.x, point.y, ZOrdinate(value), MOrdinate(point.m()))
        },
        OrdinateAxis::M => {
            Point::new_raw_axes(point.x, point.y, ZOrdinate(point.z()), MOrdinate(value))
        },
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

pub(crate) use relate::{
    De9im, De9imInterval, GeographicPrepared, Loc, OperandShape, evaluate_cartesian_pattern,
    evaluate_cartesian_predicate, evaluate_cartesian_prepared_row,
    graph_intersects_data_geographic, graph_relate_data, graph_relate_data_geographic,
    graph_relate_shapes, is_axis_aligned_rectangle, is_axis_aligned_rectangle_row, predicate_rows,
};
#[cfg(test)]
pub(crate) use segment_index::{
    CLEARANCE_POINT_BUILD_SITES, CLEARANCE_POINT_LEAVES, CLEARANCE_POINT_NODES,
    CLEARANCE_POINT_RECHECKS,
};
pub(crate) use segment_index::{
    ClearancePointQuery, PointSetIndex, SEGMENT_INDEX_MIN_PAIRS, SegmentIndex,
};
pub(in crate::geometry) use segment_index::{
    MonotoneRun, candidate_pairs_over_staged_runs, for_each_candidate_pair,
    for_each_indexed_overlapping_bounds_pair, for_each_overlapping_bounds_cross_pair,
    for_each_overlapping_bounds_cross_pair_with_work, for_each_overlapping_bounds_pair, sign_of,
    single_chain,
};
#[cfg(test)]
use segments::point_segment_distance_key;
use segments::{
    CCW_ERRBOUND_A, Contact, ON_SEGMENT_EPSILON, SquaredDistanceKey, clearance_aabb_filter,
    orientation_xy, point_pair_distance_key, point_segment_distance_and_projection,
    point_segment_distance_squared, point_segment_exact_distance_key, ray_crossing_is_right,
    segment_contact_exact, segment_contact_point, segment_cross_point, segment_envelopes_disjoint,
    segments_intersect, shared_segment_part, undirected_segment_edge_key,
};
#[cfg(test)]
pub(crate) use segments::{
    CLEARANCE_AABB_COMPARE_DECISIONS, CLEARANCE_AABB_COMPARE_DECLINES,
    CLEARANCE_AABB_FILTER_DECLINES, CLEARANCE_AABB_FILTERS, CLEARANCE_DOT_BINARY64_DECISIONS,
    CLEARANCE_DOT_BINARY64_DECLINES, CLEARANCE_STRIP_BINARY64_DECISIONS,
    CLEARANCE_STRIP_BINARY64_DECLINES, CLEARANCE_STRIP_EXACT_FALLBACKS,
    CLEARANCE_STRIP_SUPPORT_FALLBACKS, EXACT_SELECTION_FALLBACKS, POINT_SEGMENT_BINARY64_DECISIONS,
    POINT_SEGMENT_BINARY64_DECLINES,
};
/// The guarded-sqrt distance is the one segment-kernel helper the broadcast
/// lanes call directly (packed point columns).
pub(crate) use segments::{
    RectContact, WitnessRole, clearance_strip_exceeds, finish_planar_squared_min, lerp_point,
    line_segments, point_distance, point_distance_squared, point_on_segment, points_dwithin,
    segment_vs_rect, source_dot_cmp, squared_norm_is_trustworthy,
};
pub(crate) use segments::{
    SegmentProjection, point_segment_distance, point_segment_selection, segment_projection,
};
/// Convex lerp shared with the broadcast fallback lanes (see
/// `broadcast::metrics::points`), so packed and fallback agree at the extremes.
pub(crate) use segments::{
    interpolate_f64, interpolate_fraction, interpolate_optional_ordinate,
    interpolate_subdivision_ordinate,
};
pub(in crate::geometry) use segments::{try_dedup_by, try_sort_by, try_sort_by_with_work};
pub(crate) use tessellation::exact::ExactRational;
pub(crate) use tessellation::{CdtRefinement, VoronoiPolygonOutput, row_sample_seed};
use tessellation::{delaunay_triangulation, point_in_ccw_triangle};
pub(in crate::geometry) use types::GeographicMetricKey;

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
pub(crate) fn nearest_line(pair: Option<&(Point, Point)>, common: CoordinateAxes) -> Result<Shape> {
    match pair {
        Some((start, end)) => Ok(Shape::LineString(LineSeq::segment(*start, *end)?)),
        None => Ok(Shape::typed_empty(GeometryKind::LineString, common)),
    }
}

/// Publish a computed nearest-pair line with the displacement retained by its
/// metric producer.
pub(crate) fn nearest_line_with_intent(
    pair: Option<&(Point, Point)>,
    common: CoordinateAxes,
    frame: crate::geographic::GeographicFrame,
    intent: crate::geographic::ComputedEdgeIntent<'_>,
) -> Result<Shape> {
    let Some(pair) = pair else {
        return nearest_line(None, common);
    };
    let points: [Point; 2] = coerce_to_common_axes(pair.0, pair.1).into();
    let mut budget = ExpansionBudget::new("shortest_line", "computed edges");
    let published = crate::geographic::publish_computed_chain(
        frame,
        points.as_slice(),
        false,
        &mut budget,
        |_, _, _| Ok(intent),
    )?;
    match published {
        Some(points) => Ok(Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_points(&points)?,
        ))),
        None => Ok(Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_points(&points)?,
        ))),
    }
}

/// The `(left, right)` witness Shapes for `nearest_points`: the points dropped to
/// their common axes, or `(POINT EMPTY, POINT EMPTY)` when an operand is empty.
/// Mirrors [`nearest_line`] so the tuple and the line stay consistent.
pub(crate) fn nearest_point_pair(
    pair: Option<&(Point, Point)>,
    common: CoordinateAxes,
) -> (Shape, Shape) {
    match pair {
        Some((left, right)) => {
            let (left, right) = coerce_to_common_axes(*left, *right);
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
    pub(crate) fn narrow_points(empty_axes: CoordinateAxes, values: Vec<Point>) -> Result<Self> {
        match <[Point; 1]>::try_from(values) {
            Ok([point]) => Ok(Self::Point(point)),
            Err(values) if values.is_empty() => Ok(Self::Empty(GeometryKind::Point, empty_axes)),
            Err(values) => Ok(Self::multi_point(
                empty_axes,
                CoordSeq::try_from_points(&values)?,
            )),
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
    #[cfg(test)]
    pub(crate) fn for_each_segment(&self, mut visit: impl FnMut(Segment)) {
        self.for_each_segment_chain(|chain| {
            line_segments(chain).for_each(&mut visit);
        });
    }

    /// Visit every line segment as its two `Point`s, preserving Z/M.
    pub(crate) fn for_each_vertex_pair(&self, mut visit: impl FnMut(Point, Point)) {
        self.for_each_segment_chain(|chain| {
            for [start, end] in chain.segment_pairs() {
                visit(start, end);
            }
        });
    }

    /// Fallible counterpart to [`for_each_vertex_pair`]: stops at the first
    /// `Err`.
    pub(crate) fn try_for_each_vertex_pair<E>(
        &self,
        mut visit: impl FnMut(Point, Point) -> Result<(), E>,
    ) -> Result<(), E> {
        self.try_for_each_segment_chain(|chain| {
            for [start, end] in chain.segment_pairs() {
                visit(start, end)?;
            }
            Ok(())
        })
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

    fn point_only_points(&self) -> Vec<Point> {
        let mut points = Vec::new();
        self.append_first_point_only(RunIdentity::Full, &mut points);
        points
    }

    #[cfg(test)]
    fn append_point_only(&self, out: &mut Vec<Point>) {
        match self {
            Self::Point(point) => out.push(*point),
            Self::MultiPoint(points) => {
                out.reserve(points.coord_count());
                out.extend(points);
            },
            Self::GeometryCollection(geometries) => {
                for geometry in geometries {
                    geometry.append_point_only(out);
                }
            },
            Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Polygon(_)
            | Self::MultiPolygon(_)
            | Self::Empty(..) => {},
        }
    }

    fn linework(&self) -> Result<Vec<&CoordSeq>> {
        Ok(
            match self.lineal().ok_or(GeometryErrorKind::LineStringRequired)? {
                Lineal::LineString(points) => vec![points.as_coords()],
                Lineal::MultiLineString(lines) => lines.iter().map(LineSeq::as_coords).collect(),
                Lineal::Empty => Vec::new(),
            },
        )
    }

    pub(crate) fn single_linework(&self) -> Result<&CoordSeq> {
        match self
            .lineal()
            .ok_or(GeometryErrorKind::FrechetLineStringRequired)?
        {
            Lineal::LineString(points) => Ok(points),
            Lineal::MultiLineString(lines) => match &**lines {
                [line] => Ok(line),
                // The right kind with no content is a structural error, not a
                // kind error.
                [] => Err(GeometryErrorKind::EmptyLinework.into()),
                _ => Err(GeometryErrorKind::FrechetLineStringRequired.into()),
            },
            Lineal::Empty => Err(GeometryErrorKind::EmptyLinework.into()),
        }
    }

    pub(crate) fn splitter_points(&self) -> Result<Vec<Point>> {
        match self
            .point_set()
            .ok_or(GeometryErrorKind::PointSplitterRequired)?
        {
            PointSet::Point(point) => Ok(vec![point]),
            PointSet::MultiPoint(points) => Ok(points.to_vec()),
            // An empty point set contributes no split locations.
            PointSet::Empty => Ok(Vec::new()),
        }
    }
}

impl Polygon {
    pub fn new(shell: Ring, holes: Vec<Ring>) -> Self {
        Self {
            shell,
            holes: SharedSlice::from_vec(holes),
        }
    }

    pub fn points(&self) -> impl Iterator<Item = Point> + '_ {
        self.shell.iter().chain(self.holes.iter().flatten())
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

    pub(crate) fn validate(
        &self,
        name: &str,
        path: &str,
    ) -> crate::error::Result<Option<ValidationIssue>> {
        self.validate_inner(name, path, None)
    }

    pub(in crate::geometry) fn validate_with_contacts(
        &self,
        name: &str,
        path: &str,
        contacts: &mut Vec<PolygonContact>,
    ) -> crate::error::Result<Option<ValidationIssue>> {
        self.validate_inner(name, path, Some(contacts))
    }

    fn validate_inner(
        &self,
        name: &str,
        path: &str,
        contacts: Option<&mut Vec<PolygonContact>>,
    ) -> crate::error::Result<Option<ValidationIssue>> {
        if let Some(issue) = validate_ring(self.shell.coords(), || {
            (format!("{name} shell"), format!("{path}.shell"))
        }) {
            return Ok(Some(issue));
        }
        for (idx, hole) in self.holes.iter().enumerate() {
            if let Some(issue) = validate_ring(hole.coords(), || {
                (format!("{name} hole"), format!("{path}.holes[{idx}]"))
            }) {
                return Ok(Some(issue));
            }
        }
        polygon_rings_issue_with_contacts(self, path, contacts)
    }

    fn contains_point(&self, point: Point) -> bool {
        ring_classify_point(self.shell.coords(), point) == RingClass::Interior
            && !self
                .holes
                .iter()
                .any(|hole| ring_classify_point(hole.coords(), point) != RingClass::Exterior)
    }

    fn quantize(&self, precision: i32) -> Result<Self> {
        self.map_ring_seqs(|ring| ring.quantize(precision))
    }

    /// Fallible columnar XY transform over the shell and every hole, generic
    /// over the error (the in-core CRS projections' per-polygon engine).
    fn map_rings_xy_with<E>(
        &self,
        op: impl Fn(f64, f64) -> Result<(f64, f64), E> + Copy,
    ) -> Result<Self, E>
    where
        E: From<crate::error::Error>,
    {
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
    ) -> Result<Self, E>
    where
        E: From<crate::error::Error>,
    {
        let shell = Ring::try_from_closed(transform(self.shell.coords())?)?;
        let mut holes = Vec::with_capacity(self.holes.len());
        for hole in self.holes.iter() {
            holes.push(Ring::try_from_closed(transform(hole.coords())?)?);
        }
        let holes = SharedSlice::from_vec(holes);
        Ok(Self { shell, holes })
    }

    fn segmentize_budgeted(
        &self,
        max_segment_length: f64,
        placement: &SegmentPlacement<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        self.try_map_ring_seqs(|ring| {
            segmentize_points_budgeted(ring, max_segment_length, placement, budget)
        })
    }

    fn densified_budgeted_with_placement(
        &self,
        fraction: f64,
        placement: &SegmentPlacement<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        self.try_map_ring_seqs(|ring| {
            densify_points_budgeted_with_placement(ring, fraction, placement, budget)
        })
    }

    fn reverse(&self) -> Result<Self> {
        self.map_ring_seqs(CoordSeq::reversed)
    }

    /// Rebuild every ring through a transform that may collapse it: `None`
    /// from the transform, or a closed ring shorter than four coordinates,
    /// drops a hole and collapses the whole polygon (`None`) for the shell.
    pub(crate) fn map_rings_collapsing(
        &self,
        mut transform: impl FnMut(&CoordSeq) -> Result<Option<CoordSeq>>,
    ) -> Result<Option<Self>> {
        let closed = |ring: Option<CoordSeq>| ring.and_then(Ring::from_closed);
        let Some(shell) = closed(transform(self.shell.coords())?) else {
            return Ok(None);
        };
        let mut holes = Vec::with_capacity(self.holes.len());
        for hole in self.holes.iter() {
            if let Some(hole) = closed(transform(hole.coords())?) {
                holes.push(hole);
            }
        }
        Ok(Some(Self {
            shell,
            holes: SharedSlice::from_vec(holes),
        }))
    }

    /// Rebuild every ring through a columnar transform that preserves ring
    /// validity (closure and length) — no `AoS` `Vec<Point>` round-trip.
    pub(crate) fn map_ring_seqs(
        &self,
        mut transform: impl FnMut(&CoordSeq) -> Result<CoordSeq>,
    ) -> Result<Self> {
        self.try_map_ring_seqs(|ring| transform(ring))
    }

    fn orient(&self, exterior_cw: bool) -> Result<Self> {
        let shell = constructive::oriented_ring(&self.shell, exterior_cw)?;
        let mut holes = Vec::with_capacity(self.holes.len());
        for hole in self.holes.iter() {
            holes.push(constructive::oriented_ring(hole, !exterior_cw)?);
        }
        Ok(Self::new(shell, holes))
    }

    fn normalize(&self) -> Result<Self> {
        // Canonical winding follows RFC 7946 (GeoJSON): exterior CCW, holes
        // CW. Rings rotate min-vertex-first; holes sort ascending like every
        // other canonical sequence.
        let mut holes = Vec::with_capacity(self.holes.len());
        for hole in self.holes.iter() {
            holes.push(Ring::try_from_closed(constructive::canonical_ring_seq(
                hole.coords(),
                true,
            )?)?);
        }
        holes.sort_unstable_by(compare_point_slices);
        Ok(Self::new(
            Ring::try_from_closed(constructive::canonical_ring_seq(
                self.shell.coords(),
                false,
            )?)?,
            holes,
        ))
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

    fn force_2d(&self) -> Result<Self> {
        self.map_ring_seqs(CoordSeq::force_2d)
    }

    fn map_points<F>(&self, transform: &F) -> Result<Self>
    where
        F: Fn(Point) -> Result<Point> + ?Sized,
    {
        self.try_map_ring_seqs(|ring| ring.try_map_points(transform))
    }

    /// Snap every ring onto the reference; `None` when the shell collapses.
    fn snap_to_reference(&self, reference: &SnapReference) -> Result<Option<Self>> {
        self.map_rings_collapsing(|ring| {
            CoordSeq::try_from_points(&snap_ring_to_reference(ring, reference)?).map(Some)
        })
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

    /// Geographic rectangle with frame-native longitude and latitude bounds.
    /// Finite west/east endpoints use west>east to signal a seam-crossing
    /// window; latitudes must satisfy ``south <= north``.
    pub(crate) fn new_geographic(
        frame: crate::geographic::GeographicFrame,
        west: f64,
        south: f64,
        east: f64,
        north: f64,
    ) -> Result<Self> {
        let half_period = frame.longitude().half_period();
        let pole = frame.pole();
        if west.is_finite()
            && south.is_finite()
            && east.is_finite()
            && north.is_finite()
            && (-half_period..=half_period).contains(&west)
            && (-half_period..=half_period).contains(&east)
            && (-pole..=pole).contains(&south)
            && (-pole..=pole).contains(&north)
            && south <= north
        {
            Ok(Self::new_raw(west, south, east, north))
        } else {
            Err(GeometryErrorKind::InvalidGeographicRectangle.into())
        }
    }

    /// The full longitude band between two frame-valid latitudes.
    pub(crate) fn full_geographic_longitude(
        frame: crate::geographic::GeographicFrame,
        south: f64,
        north: f64,
    ) -> Result<Self> {
        let half_period = frame.longitude().half_period();
        Self::new_geographic(frame, -half_period, south, half_period, north)
    }

    /// Whether these bounds cross the antimeridian (west longitude > east).
    pub fn crosses_antimeridian(self) -> bool {
        self.minx() > self.maxx()
    }

    /// Split a validated geographic rectangle at its frame's periodic seam.
    pub(crate) fn split_geographic(
        self,
        frame: crate::geographic::GeographicFrame,
    ) -> Option<[Self; 2]> {
        self.crosses_antimeridian().then(|| {
            let half_period = frame.longitude().half_period();
            [
                Self::new_raw(self.minx(), self.miny(), half_period, self.maxy()),
                Self::new_raw(-half_period, self.miny(), self.maxx(), self.maxy()),
            ]
        })
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

    pub const fn into_array(self) -> [f64; 4] {
        [self.minx(), self.miny(), self.maxx(), self.maxy()]
    }

    pub const fn corners(self) -> [[f64; 2]; 4] {
        [
            [self.minx(), self.miny()],
            [self.maxx(), self.miny()],
            [self.maxx(), self.maxy()],
            [self.minx(), self.maxy()],
        ]
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

#[cfg(test)]
mod point_only_collection_tests {
    use super::*;

    #[test]
    fn multipoint_append_uses_existing_additional_capacity() {
        let points = MultiPoint::from_coordseq(
            CoordSeq::try_from_points(&[Point::new_raw_xy(1.0, 0.0), Point::new_raw_xy(2.0, 0.0)])
                .unwrap(),
        )
        .unwrap();
        let shape = Shape::MultiPoint(points);
        let mut output = Vec::with_capacity(3);
        output.push(Point::new_raw_xy(0.0, 0.0));

        let ((), allocations) =
            crate::test_support::allocation_count(|| shape.append_point_only(&mut output));

        assert_eq!(allocations, 0);
        assert_eq!(output.len(), 3);
    }
}
