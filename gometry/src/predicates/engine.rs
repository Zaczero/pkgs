//! The one source of truth for boolean spatial predicates.
//!
//! Every surface that evaluates a named predicate — free functions, `Geometry`
//! and `GeometryArray` methods, `PreparedGeometry`, the spatial index refine
//! loop, and `join` — reads its facts from [`Predicate::spec`] and runs batches
//! through [`scalar_vs_shapes`]. One table, one batch policy: the surfaces
//! cannot drift apart in which tokens they accept, which point fast paths they
//! take, or when they amortize a prepared relation.

use crate::geometry::{
    Bounds, Dimension, Point, PointBatchTester, RingClass, Shape, ShapeData,
    convex_halfplanes_cover, point_is_geographic_pole, pole_position, same_point, topology_split,
};
use crate::{GeometryKind, PointRows, PyGeometryArray, ShapeRow};

/// Historical amortize threshold for prepared/array batch entry points.
///
/// Answer-producing algorithm selection must NOT depend on this constant:
/// `scalar_vs_shapes` uses one pipeline for every batch length. Call sites may
/// still use it only as a layout/scheduling hint (e.g. detach vs in-GIL), never
/// to pick a different predicate kernel.
pub(crate) const PREPARED_PREDICATE_MIN: usize = 16;

/// A named planar topological predicate with one canonical public token.
///
/// `dwithin` is not listed: it is a metric test that needs a distance model,
/// not a DE-9IM relation (the spatial index handles it separately).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Predicate {
    Intersects,
    Disjoint,
    Contains,
    ContainsProperly,
    Within,
    Covers,
    CoveredBy,
    Touches,
    Crosses,
    Overlaps,
    Equals,
}

/// How the R-tree narrows candidates for `query OP item`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IndexEnvelope {
    /// Candidates whose envelope intersects the query envelope.
    Intersecting,
    /// Candidates whose envelope lies inside the query envelope — the tighter
    /// lookup containment predicates allow.
    ContainedInQuery,
}

/// All dispatch facts for one predicate, read from [`Predicate::spec`].
#[derive(Clone, Copy)]
pub(crate) struct PredicateSpec {
    /// The predicate these facts describe.
    pub predicate: Predicate,
    /// The public token (`'intersects'`, `'covers'`, …).
    pub token: &'static str,
    /// Whether `p(a, b) == p(b, a)`.
    pub symmetric: bool,
    /// Fast kernel when the **right** operand is a point, if one exists.
    pub right_point: Option<fn(&Shape, Point) -> bool>,
    /// Fast kernel when the **left** operand is a point, if one exists.
    pub left_point: Option<fn(Point, &Shape) -> bool>,
    /// R-tree candidate strategy, or `None` when the predicate cannot be
    /// index-accelerated (`disjoint` would enumerate the complement).
    pub index_envelope: Option<IndexEnvelope>,
}

pub(crate) fn point_within_shape(point: Point, shape: &Shape) -> bool {
    shape.contains_point(point)
}

pub(crate) fn point_covered_by_shape(point: Point, shape: &Shape) -> bool {
    shape.covers_point(point)
}

pub(crate) fn shape_disjoint_point(shape: &Shape, point: Point) -> bool {
    !shape.covers_point(point)
}

pub(crate) fn point_disjoint_shape(point: Point, shape: &Shape) -> bool {
    !shape.covers_point(point)
}

/// Point on the right: touches when the probe lies on an areal boundary
/// (OGC: interiors disjoint, boundaries meet). Pure (multi)polygons answer
/// via hierarchical classification; mixed/non-areal fall through to the
/// full DE-9IM contact lane (`classify_area_point` is `None` there).
pub(crate) fn shape_touches_point(shape: &Shape, point: Point) -> bool {
    match PointBatchTester::new(shape).classify_area_point(point) {
        Some(RingClass::Boundary) => true,
        Some(_) => false,
        None => shape.touches(&Shape::Point(point)),
    }
}

/// Point on the left: touches is symmetric for point×geometry.
pub(crate) fn point_touches_shape(point: Point, shape: &Shape) -> bool {
    shape_touches_point(shape, point)
}

/// Single-Point never crosses (OGC); MultiPoint is a different declared kind.
pub(crate) const fn shape_crosses_point(_shape: &Shape, _point: Point) -> bool {
    false
}

pub(crate) const fn point_crosses_shape(_point: Point, _shape: &Shape) -> bool {
    false
}

/// Single-Point never overlaps (OGC); MultiPoint is a different declared kind.
pub(crate) const fn shape_overlaps_point(_shape: &Shape, _point: Point) -> bool {
    false
}

pub(crate) const fn point_overlaps_shape(_point: Point, _shape: &Shape) -> bool {
    false
}

/// Point on the right: topological equals against a shape (empty/multi handled).
pub(crate) fn shape_equals_point(shape: &Shape, point: Point) -> bool {
    match shape {
        Shape::Point(other) => same_point(*other, point),
        _ => shape.equals(&Shape::Point(point)),
    }
}

/// Point on the left: equals is symmetric for point×geometry.
pub(crate) fn point_equals_shape(point: Point, shape: &Shape) -> bool {
    shape_equals_point(shape, point)
}

/// Declared-kind class for the kind×kind constant gate (not topological dim).
#[derive(Clone, Copy, PartialEq, Eq)]
enum KindClass {
    /// `Shape::Point` / empty point — never MultiPoint.
    Point,
    MultiPoint,
    Curve,
    Surface,
    Collection,
}

const fn kind_class(kind: GeometryKind) -> KindClass {
    match kind {
        GeometryKind::Point => KindClass::Point,
        GeometryKind::MultiPoint => KindClass::MultiPoint,
        GeometryKind::LineString | GeometryKind::MultiLineString => KindClass::Curve,
        GeometryKind::Polygon | GeometryKind::MultiPolygon => KindClass::Surface,
        GeometryKind::GeometryCollection => KindClass::Collection,
    }
}

const fn kind_class_dim(class: KindClass) -> Option<u8> {
    match class {
        KindClass::Point | KindClass::MultiPoint => Some(0),
        KindClass::Curve => Some(1),
        KindClass::Surface => Some(2),
        KindClass::Collection => None,
    }
}

impl Predicate {
    /// Every predicate, in public-documentation order.
    pub(crate) const ALL: [Self; 11] = [
        Self::Intersects,
        Self::Disjoint,
        Self::Contains,
        Self::ContainsProperly,
        Self::Within,
        Self::Covers,
        Self::CoveredBy,
        Self::Touches,
        Self::Crosses,
        Self::Overlaps,
        Self::Equals,
    ];

    /// Parse a public token; `None` for unknown values (callers shape the
    /// error).
    pub(crate) fn parse(token: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|predicate| predicate.spec().token == token)
    }

    /// The canonical token (inverse of [`parse`](Self::parse)).
    pub(crate) fn token(self) -> &'static str {
        self.spec().token
    }

    /// Truth value of this predicate between two POINTS, given whether they are
    /// the same point (XY, via `same_topological_coordinate`). Points have an
    /// empty boundary, so `touches`/`crosses`/`overlaps` are always false; the
    /// intersection/containment family reduces to point equality and `disjoint`
    /// is its negation. Identical to the general kernel on point×point (the
    /// point×point parity test pins this), but needs no `ShapeData`.
    pub(crate) const fn point_pair(self, same: bool) -> bool {
        match self {
            Self::Intersects
            | Self::Contains
            | Self::ContainsProperly
            | Self::Within
            | Self::Covers
            | Self::CoveredBy
            | Self::Equals => same,
            Self::Disjoint => !same,
            Self::Touches | Self::Crosses | Self::Overlaps => false,
        }
    }

    /// The dispatch facts for this predicate — the only place they live.
    pub(crate) fn spec(self) -> PredicateSpec {
        match self {
            Self::Intersects => self.symmetric_spec(
                "intersects",
                Some(Shape::covers_point),
                Some(point_covered_by_shape),
                Some(IndexEnvelope::Intersecting),
            ),
            Self::Disjoint => self.symmetric_spec(
                "disjoint",
                Some(shape_disjoint_point),
                Some(point_disjoint_shape),
                None,
            ),
            Self::Contains => self.directed_spec("contains", Some(Shape::contains_point), None),
            Self::ContainsProperly => {
                self.directed_spec("contains_properly", Some(Shape::contains_point), None)
            },
            Self::Within => self.directed_spec("within", None, Some(point_within_shape)),
            Self::Covers => self.directed_spec("covers", Some(Shape::covers_point), None),
            Self::CoveredBy => self.directed_spec("covered_by", None, Some(point_covered_by_shape)),
            Self::Touches => self.symmetric_spec(
                "touches",
                Some(shape_touches_point),
                Some(point_touches_shape),
                Some(IndexEnvelope::Intersecting),
            ),
            Self::Crosses => self.symmetric_spec(
                "crosses",
                Some(shape_crosses_point),
                Some(point_crosses_shape),
                Some(IndexEnvelope::Intersecting),
            ),
            Self::Overlaps => self.symmetric_spec(
                "overlaps",
                Some(shape_overlaps_point),
                Some(point_overlaps_shape),
                Some(IndexEnvelope::Intersecting),
            ),
            Self::Equals => self.symmetric_spec(
                "equals",
                Some(shape_equals_point),
                Some(point_equals_shape),
                Some(IndexEnvelope::Intersecting),
            ),
        }
    }

    /// Compile-time kind×kind constant answers for Crosses / Overlaps / Equals.
    ///
    /// Keys off **declared** [`GeometryKind`] (`Point` vs `MultiPoint`), never
    /// topological dimension alone — a MultiPoint can cross an area while a
    /// single Point never can. `GeometryCollection` on either side returns
    /// `None` (fall through). For `Equals`, callers must resolve emptiness
    /// first (`both empty → true`, `exactly one empty → false`); this table
    /// assumes non-empty operands when it returns `Some(false)` for unequal
    /// dimensions.
    pub(crate) const fn kind_gate(
        self,
        left_kind: GeometryKind,
        right_kind: GeometryKind,
    ) -> Option<bool> {
        let left = kind_class(left_kind);
        let right = kind_class(right_kind);
        if matches!(left, KindClass::Collection) || matches!(right, KindClass::Collection) {
            return None;
        }
        match self {
            Self::Crosses => {
                // Point × anything = F (both orders). MultiPoint is NOT Point.
                if matches!(left, KindClass::Point) || matches!(right, KindClass::Point) {
                    return Some(false);
                }
                // Area × Area = F. MultiPoint × MultiPoint = F.
                if (matches!(left, KindClass::Surface) && matches!(right, KindClass::Surface))
                    || (matches!(left, KindClass::MultiPoint)
                        && matches!(right, KindClass::MultiPoint))
                {
                    return Some(false);
                }
                // MultiPoint×{Line,Area}, Line×Line, Line×Area = REAL.
                None
            },
            Self::Overlaps => {
                // Point × anything = F (both orders).
                if matches!(left, KindClass::Point) || matches!(right, KindClass::Point) {
                    return Some(false);
                }
                // Unequal dimension = F; same-dim MultiPoint/Line/Area = REAL.
                match (kind_class_dim(left), kind_class_dim(right)) {
                    (Some(a), Some(b)) if a != b => Some(false),
                    _ => None,
                }
            },
            // Equals (and every other predicate): no kind×kind constant here.
            // Degenerate non-empty operands can be topologically equal across
            // kinds (`LINESTRING (0 0, 0 0)` equals `POINT (0 0)` — GEOS/Shapely
            // and pre-gate gometry), so Equals must not constant-false unequal
            // declared kinds. Empty handling lives in `shape_kind_gate`;
            // Point×Point rides the cheap equals point kernels.
            Self::Equals
            | Self::Intersects
            | Self::Disjoint
            | Self::Contains
            | Self::ContainsProperly
            | Self::Within
            | Self::Covers
            | Self::CoveredBy
            | Self::Touches => None,
        }
    }

    /// Shape-aware constant gate: empty handling for Equals, then [`kind_gate`].
    pub(crate) fn shape_kind_gate(self, left: &Shape, right: &Shape) -> Option<bool> {
        if self == Self::Equals {
            // Empty×empty is equal across kinds; exactly one empty is not.
            // Unequal-kind non-empty pairs fall through (degenerate cross-kind
            // equals are real — see kind_gate Equals note).
            match (left.is_empty(), right.is_empty()) {
                (true, true) => return Some(true),
                (true, false) | (false, true) => return Some(false),
                (false, false) => {},
            }
        }
        self.kind_gate(GeometryKind::of(left), GeometryKind::of(right))
    }

    /// Homogeneous packed column × scalar kind×kind constant, if any.
    ///
    /// Shared by the batch engine ([`scalar_vs_shapes`]) and the prepared /
    /// broadcast scalar-vs-array altitude so both paths share one empty-Equals
    /// + [`kind_gate`] rule without re-deriving it at each call site.
    pub(crate) fn homogeneous_scalar_packed_verdict(
        self,
        scalar_kind: GeometryKind,
        scalar_is_empty: bool,
        row_kind: GeometryKind,
        scalar_is_left: bool,
    ) -> Option<bool> {
        let (left_kind, right_kind) = if scalar_is_left {
            (scalar_kind, row_kind)
        } else {
            (row_kind, scalar_kind)
        };
        // Empty scalar vs packed points is always false (packed points are
        // never empty). Empty line/poly windows may be empty, so Equals with
        // an empty scalar cannot whole-column fill those storages.
        if self == Self::Equals && scalar_is_empty {
            return match row_kind {
                GeometryKind::Point => Some(false),
                _ => None,
            };
        }
        self.kind_gate(left_kind, right_kind)
    }

    fn symmetric_spec(
        self,
        token: &'static str,
        right_point: Option<fn(&Shape, Point) -> bool>,
        left_point: Option<fn(Point, &Shape) -> bool>,
        index_envelope: Option<IndexEnvelope>,
    ) -> PredicateSpec {
        PredicateSpec {
            predicate: self,
            token,
            symmetric: true,
            right_point,
            left_point,
            index_envelope,
        }
    }

    fn directed_spec(
        self,
        token: &'static str,
        right_point: Option<fn(&Shape, Point) -> bool>,
        left_point: Option<fn(Point, &Shape) -> bool>,
    ) -> PredicateSpec {
        // Directed containment predicates share one envelope rule: the
        // container's envelope must contain the candidate's, so the index can
        // use the tighter `locate_in_envelope` lookup when the query is the
        // container (`contains`/`contains_properly`/`covers`), and the generic
        // intersecting lookup otherwise.
        let index_envelope = Some(match self {
            Self::Contains | Self::ContainsProperly | Self::Covers => {
                IndexEnvelope::ContainedInQuery
            },
            _ => IndexEnvelope::Intersecting,
        });
        PredicateSpec {
            predicate: self,
            token,
            symmetric: false,
            right_point,
            left_point,
            index_envelope,
        }
    }
}

/// Axis-aligned bounds gates are unsound for seam-crossing operands in a
/// geographic frame — skip them and let the split-normalized kernel decide.
pub(crate) fn skip_antimeridian_bounds_gate_row(geographic: bool, row: ShapeRow<'_>) -> bool {
    geographic && row.with_shape(Shape::crosses_antimeridian)
}

impl PredicateSpec {
    /// A definite answer from operand bounds alone, before any exact topology.
    ///
    /// Strictly disjoint bounds settle every predicate (`disjoint` → `true`,
    /// everything else → `false`); containment and equality predicates also
    /// reject when the required bounds nesting fails. `None` means the exact
    /// kernel must decide.
    pub(crate) fn bounds_gate(&self, left: Bounds, right: Bounds) -> Option<bool> {
        if !left.intersects(right) {
            return Some(self.predicate == Predicate::Disjoint);
        }
        match self.predicate {
            Predicate::Contains | Predicate::ContainsProperly | Predicate::Covers
                if !left.contains(right) =>
            {
                Some(false)
            },
            Predicate::Within | Predicate::CoveredBy if !right.contains(left) => Some(false),
            // Topologically equal point sets have identical extremes.
            Predicate::Equals if left != right => Some(false),
            _ => None,
        }
    }
}

/// Evaluate `spec` between one fixed `scalar` operand and every row of
/// `elements`, building the scalar's cached/prepared state at most once.
///
/// `scalar_is_left` names the operand order: `true` evaluates
/// `scalar OP element`, `false` evaluates `element OP scalar`. This is the
/// single batch engine behind the free functions, the array methods,
/// `PreparedGeometry`, and the spatial-index refine loop. Rows come straight
/// off the storage: `Mixed` rows keep their persistent handles (prepared
/// state accumulates), packed point rows never synthesize one.
fn dispatch_row_data<R>(
    array: Option<&PyGeometryArray>,
    index: usize,
    row: ShapeRow<'_>,
    f: impl FnOnce(&ShapeData) -> R,
) -> R {
    match array {
        Some(array) => array.with_row_data(index, row, f),
        None => row.with_data(f),
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "cohesive kernel; splitting obscures the algorithm"
)]
pub(crate) fn scalar_vs_shapes<'a>(
    spec: &PredicateSpec,
    scalar: &ShapeData,
    elements: impl ExactSizeIterator<Item = (usize, ShapeRow<'a>)>,
    scalar_is_left: bool,
    array: Option<&PyGeometryArray>,
    geographic: bool,
) -> Vec<bool> {
    let scalar_orig = scalar;
    let n = elements.len();
    // Homogeneous packed column vs scalar: constant kind gate fills the whole
    // output with zero per-row topology (the poly×poly crosses fill generalized).
    if let Some(array) = array
        && let Some(row_kind) = array.storage().homogeneous_kind()
    {
        let constant = spec.predicate.homogeneous_scalar_packed_verdict(
            GeometryKind::of(scalar_orig.shape()),
            scalar_orig.shape().is_empty(),
            row_kind,
            scalar_is_left,
        );
        if let Some(verdict) = constant {
            return vec![verdict; n];
        }
    }
    let scalar_split = (geographic && scalar.shape().crosses_antimeridian())
        .then(|| ShapeData::from(topology_split(scalar.shape())));
    let scalar = scalar_split.as_ref().unwrap_or(scalar_orig);
    let scalar_kind = GeometryKind::of(scalar_orig.shape());
    let scalar_empty = scalar_orig.shape().is_empty();
    // Point elements answer through the matching point kernel in every
    // strategy: it is both exact (mod-2 boundary rules) and cheaper than any
    // relate.
    let element_point = |row: ShapeRow<'_>| -> Option<bool> {
        let point = match row {
            ShapeRow::Point(point) => point,
            ShapeRow::Handle(handle) => match handle.shape() {
                Shape::Point(point) => *point,
                _ => return None,
            },
            ShapeRow::Shape(shape) => match shape {
                Shape::Point(point) => *point,
                _ => return None,
            },
            ShapeRow::Line(..) | ShapeRow::Rings(..) => return None,
        };
        if geographic {
            let (left, right) = if scalar_is_left {
                (scalar_orig.shape(), &Shape::Point(point))
            } else {
                (&Shape::Point(point), scalar_orig.shape())
            };
            if let Some(verdict) = try_geographic_point_pair(spec, left, right) {
                return Some(verdict);
            }
        }
        // The scalar's cached hierarchical `PointBatchTester` answers point
        // elements (built once per handle; `point_batch_eval` mirrors the
        // kernel table exactly), so prepared/array batches never pay a full
        // per-probe ring scan.
        if let Some(tester) = scalar.point_tester()
            && let Some(verdict) = point_batch_eval(spec, tester, point, scalar_is_left)
        {
            return Some(verdict);
        }
        if scalar_is_left {
            spec.right_point.map(|kernel| kernel(scalar, point))
        } else {
            spec.left_point.map(|kernel| kernel(point, scalar))
        }
    };
    // Reject from operand bounds before building any handle, for EVERY row
    // type: strictly-disjoint boxes settle every predicate (and containment /
    // equality also reject on a failed nesting), so a refuted row never pays
    // `with_data` + the prepared kernel. The box is one cheap read per row —
    // packed line/polygon rows scan their columns (no `Arc` bumps, no transient
    // `ShapeData`), `Mixed` rows read their cached bounds. This is the dominant
    // saving on the spatial-filter hot shape (one query vs many mostly-disjoint
    // candidates). Point rows are already settled by `element_point` above in
    // the prepared lanes, and fall here harmlessly in the per-pair lane.
    let scalar_bounds = scalar.bounds();
    let element_bounds = array.and_then(PyGeometryArray::cached_element_bounds);
    let row_is_missing = |index: usize| array.is_some_and(|array| array.is_row_missing(index));
    // Per-row declared-kind constant before bounds / materialize / contact.
    let kind_reject = |row: ShapeRow<'_>| -> Option<bool> {
        let (row_kind, row_empty) = row_kind_empty(row);
        if spec.predicate == Predicate::Equals {
            match (scalar_empty, row_empty) {
                (true, true) => return Some(true),
                (true, false) | (false, true) => return Some(false),
                (false, false) => {},
            }
        }
        let (left_kind, right_kind) = if scalar_is_left {
            (scalar_kind, row_kind)
        } else {
            (row_kind, scalar_kind)
        };
        spec.predicate.kind_gate(left_kind, right_kind)
    };
    // Axis-aligned bounds are unsound when either operand crosses the
    // antimeridian (including a fixed scalar against simple point rows).  A
    // physical pole has the same property: longitude is merely a spelling,
    // so its planar X cannot certify a negative against a geographic row.
    let scalar_crosses = geographic && scalar_orig.shape().crosses_antimeridian();
    let scalar_is_pole = geographic
        && matches!(
            scalar_orig.shape(),
            Shape::Point(point) if point_is_geographic_pole(*point).is_some()
        );
    let bounds_reject = |index: usize, row: ShapeRow<'_>| -> Option<bool> {
        if scalar_crosses || scalar_is_pole || skip_antimeridian_bounds_gate_row(geographic, row) {
            return None;
        }
        let row_bounds = element_bounds
            .as_ref()
            .and_then(|bounds| bounds.get(index).copied().flatten())
            .or_else(|| row.quick_bounds())?;
        let scalar_bounds = scalar_bounds?;
        if scalar_is_left {
            spec.bounds_gate(scalar_bounds, row_bounds)
        } else {
            spec.bounds_gate(row_bounds, scalar_bounds)
        }
    };
    // Hoisted convex fast path: a small convex hole-free scalar settles
    // the containment family through pure halfplane signs, with the
    // shell, orientation, and predicate flavor resolved ONCE for the
    // whole batch (the index-refine hot shape: one query, many small
    // candidates).
    let convex_side = matches!(
        (spec.predicate, scalar_is_left),
        (
            Predicate::Contains | Predicate::Covers | Predicate::ContainsProperly,
            true
        ) | (Predicate::Within | Predicate::CoveredBy, false)
    );
    if convex_side
        && scalar.shape().coord_count() < PointBatchTester::MIN_PROBES
        && scalar.convex_shell().is_some()
    {
        return elements
            .map(|(index, row)| {
                if row_is_missing(index) {
                    false
                } else {
                    kind_reject(row)
                        .or_else(|| element_point(row))
                        .unwrap_or_else(|| {
                            dispatch_row_data(array, index, row, |element| {
                                convex_scalar_pair(spec, scalar, element, scalar_is_left)
                            })
                        })
                }
            })
            .collect();
    }
    // One pipeline for every batch length: kind → point kernel (incl. geo
    // seam/pole) → bounds → topology pair. A prior dual path omitted the
    // point kernel below PREPARED_PREDICATE_MIN and answered geographic
    // seam points from planar bounds alone — flipping answers at the threshold.
    elements
        .map(|(index, row)| {
            if row_is_missing(index) {
                false
            } else {
                kind_reject(row)
                    .or_else(|| element_point(row))
                    .or_else(|| bounds_reject(index, row))
                    .unwrap_or_else(|| {
                        dispatch_row_data(array, index, row, |element| {
                            let order = if scalar_is_left {
                                ScalarOperand::Left
                            } else {
                                ScalarOperand::Right
                            };
                            topology_scalar_pair_unchecked(spec, scalar, element, order, geographic)
                        })
                    })
            }
        })
        .collect()
}

/// Declared kind + emptiness for one storage row (no materialize for packed).
fn row_kind_empty(row: ShapeRow<'_>) -> (GeometryKind, bool) {
    match row {
        ShapeRow::Point(_) => (GeometryKind::Point, false),
        ShapeRow::Line(_, start, end) => (GeometryKind::LineString, start == end),
        ShapeRow::Rings(_, _, start, end) => (GeometryKind::Polygon, start == end),
        ShapeRow::Handle(handle) => (GeometryKind::of(handle.shape()), handle.shape().is_empty()),
        ShapeRow::Shape(shape) => (GeometryKind::of(shape), shape.is_empty()),
    }
}

#[derive(Clone, Copy)]
enum ScalarOperand {
    Left,
    Right,
}

fn topology_scalar_pair_unchecked(
    spec: &PredicateSpec,
    left: &ShapeData,
    right: &ShapeData,
    scalar: ScalarOperand,
    geographic: bool,
) -> bool {
    let (left, right) = match scalar {
        ScalarOperand::Left => (left, right),
        ScalarOperand::Right => (right, left),
    };
    topology_scalar_pair(spec, left, right, geographic)
}

/// Geographic pole-probe fast path: evaluate on unsplit operands before
/// antimeridian normalization (the split world-rect form misclassifies poles).
pub(crate) fn try_geographic_point_pair(
    spec: &PredicateSpec,
    left: &Shape,
    right: &Shape,
) -> Option<bool> {
    let (container, point, container_is_left) = match (left, right) {
        (container, Shape::Point(point)) => (container, *point, true),
        (Shape::Point(point), container) => (container, *point, false),
        _ => return None,
    };
    try_geographic_point_membership(spec, container, point, container_is_left)
}

/// Geographic point membership that must be decided on the original
/// container rather than its seam-split representation.
pub(crate) fn try_geographic_point_membership(
    spec: &PredicateSpec,
    container: &Shape,
    point: Point,
    container_is_left: bool,
) -> Option<bool> {
    try_geographic_pole_membership(spec, container, point, container_is_left)
        .or_else(|| try_geographic_seam_membership(spec, container, point, container_is_left))
}

/// Geographic pole-probe membership against one container.
///
/// This is the allocation-free core shared by geometry-pair predicates and
/// the ``contains_xy``/``intersects_xy`` coordinate lanes.  It must run on the
/// original unsplit container: antimeridian normalization represents a polar
/// cap with artificial seam edges, which cannot distinguish a pole in the
/// interior from one on that fabricated boundary.
pub(crate) fn try_geographic_pole_membership(
    spec: &PredicateSpec,
    container: &Shape,
    point: Point,
    container_is_left: bool,
) -> Option<bool> {
    use crate::geometry::PolePosition::{Boundary, Exterior, Interior};

    // Pole enclosure is an areal-ring concept. Let the ordinary point/curve
    // kernels decide non-areal operands; intercepting them here would turn
    // even an identical POINT at a pole into `intersects = false`.
    if !container.has_area_parts() {
        return None;
    }
    let north_pole = point_is_geographic_pole(point)?;
    // Tri-state: a pole strictly inside is contained; a pole ON the boundary
    // (a ring vertex at ±90) only touches. The old strict-enclosure shortcut
    // wrongly reported `intersects=false`/`disjoint=true` for a polygon whose
    // boundary passes through the pole (e.g. an S2 pole-corner cell), while
    // `touches`/`relate` — which were never short-circuited — answered
    // correctly, producing a self-contradiction.
    let pos = pole_position(container, north_pole);
    Some(match spec.predicate {
        Predicate::Intersects => pos != Exterior,
        Predicate::Disjoint => pos == Exterior,
        Predicate::Touches => pos == Boundary,
        Predicate::Contains | Predicate::ContainsProperly if container_is_left => pos == Interior,
        Predicate::Covers if container_is_left => pos != Exterior,
        Predicate::Within if !container_is_left => pos == Interior,
        Predicate::CoveredBy if !container_is_left => pos != Exterior,
        _ => return None,
    })
}

/// Classify a probe on ±180 against a crossing geographic container without
/// mistaking the split representation's fabricated seam for real boundary.
///
/// A point strictly inside an areal region has interior immediately on both
/// sides of the identified meridian.  A line crossing has matching split
/// endpoints on both seam aliases; they are one physical interior point unless
/// the original line already classifies an actual seam vertex as interior.
/// One-sided contact remains a genuine boundary.  The slow split is paid only
/// for a literal seam probe, never on the ordinary point-predicate path.
#[expect(
    clippy::float_cmp,
    reason = "the seam aliases are the exact public longitude literals ±180"
)]
fn try_geographic_seam_membership(
    spec: &PredicateSpec,
    container: &Shape,
    point: Point,
    container_is_left: bool,
) -> Option<bool> {
    use crate::geometry::PolePosition::{Boundary, Exterior, Interior};

    if point.x != -180.0 && point.x != 180.0 || !container.crosses_antimeridian() {
        return None;
    }
    let split = topology_split(container);
    let west = point.with_xy((-180.0_f64).next_up(), point.y).ok()?;
    let east = point.with_xy(180.0_f64.next_down(), point.y).ok()?;
    let west_seam = point.with_xy(-180.0, point.y).ok()?;
    let east_seam = point.with_xy(180.0, point.y).ok()?;
    let seam_covered = split.covers_point(west_seam) || split.covers_point(east_seam);
    let area_interior =
        container.has_area_parts() && split.contains_point(west) && split.contains_point(east);
    let curve_interior = container.topological_dimension() == Dimension::Curve
        && (container.contains_point(west_seam)
            || container.contains_point(east_seam)
            || (split.covers_point(west_seam) && split.covers_point(east_seam)));
    let position = if !seam_covered {
        Exterior
    } else if area_interior || curve_interior {
        Interior
    } else {
        Boundary
    };
    Some(match spec.predicate {
        Predicate::Intersects => position != Exterior,
        Predicate::Disjoint => position == Exterior,
        Predicate::Touches => position == Boundary,
        Predicate::Contains | Predicate::ContainsProperly if container_is_left => {
            position == Interior
        },
        Predicate::Covers if container_is_left => position != Exterior,
        Predicate::Within if !container_is_left => position == Interior,
        Predicate::CoveredBy if !container_is_left => position != Exterior,
        _ => return None,
    })
}

/// THE keystone for binary geographic ops. Returns the seam-split forms of a
/// pair when (and only when) they need normalizing — geographic frame AND at
/// least one operand crosses ±180 — else `None` (use the originals). Every
/// binary topology/metric op routes its planar/geodesic kernel through this (or
/// [`geo_binary`]) so a crossing pair can never silently reach a kernel on its
/// false-middle planar box. Splitting at the keystone — not at each op — is what
/// stops new entry points from re-introducing the seam bug.
pub(crate) fn geo_split_pair(
    geographic: bool,
    left: &Shape,
    right: &Shape,
) -> Option<(Shape, Shape)> {
    (geographic && (left.crosses_antimeridian() || right.crosses_antimeridian()))
        .then(|| (topology_split(left), topology_split(right)))
}

/// Run a `&Shape`-kernel over a pair, split-normalizing geographic antimeridian
/// crossings first (see [`geo_split_pair`]). The one-liner every binary topology
/// op (relate, relate_pattern, …) wraps its kernel in.
pub(crate) fn geo_binary<R>(
    geographic: bool,
    left: &Shape,
    right: &Shape,
    kernel: impl FnOnce(&Shape, &Shape) -> R,
) -> R {
    match geo_split_pair(geographic, left, right) {
        Some((left, right)) => kernel(&left, &right),
        None => kernel(left, right),
    }
}

/// Evaluate ``spec`` for one pair, split-normalizing geographic antimeridian
/// crossings on either operand before the planar predicate kernel runs. The
/// single gated per-pair entry — every frame-aware surface (broadcast, index,
/// prepared) routes through here, so the bare [`scalar_pair`] kernel is only
/// ever reached post-normalization. Infallible (see [`topology_split`]).
pub(crate) fn topology_scalar_pair(
    spec: &PredicateSpec,
    left: &ShapeData,
    right: &ShapeData,
    geographic: bool,
) -> bool {
    if geographic
        && let Some(verdict) = try_geographic_point_pair(spec, left.shape(), right.shape())
    {
        return verdict;
    }
    match geo_split_pair(geographic, left.shape(), right.shape()) {
        Some((left, right)) => scalar_pair(spec, &ShapeData::from(left), &ShapeData::from(right)),
        None => scalar_pair(spec, left, right),
    }
}

/// The batched point-membership reading of `spec` against a fixed container
/// (`scalar OP point` when `scalar_is_left`, else `point OP scalar`), through
/// a prebuilt [`PointBatchTester`]. Mirrors the spec's point-kernel table
/// exactly: strict `contains_point` where the kernel is strict,
/// boundary-inclusive `covers_point` (or its negation) elsewhere. `None` when
/// the predicate has no point kernel for that side.
pub(crate) fn point_batch_eval(
    spec: &PredicateSpec,
    tester: &PointBatchTester,
    point: Point,
    scalar_is_left: bool,
) -> Option<bool> {
    Some(match (spec.predicate, scalar_is_left) {
        (Predicate::Contains | Predicate::ContainsProperly, true) | (Predicate::Within, false) => {
            tester.contains_point(point)
        },
        (Predicate::Covers, true) | (Predicate::CoveredBy, false) | (Predicate::Intersects, _) => {
            tester.covers_point(point)
        },
        (Predicate::Disjoint, _) => !tester.covers_point(point),
        // Areal point×geometry: OGC touch ≡ on boundary (Interior of either
        // side meets ⇒ not touches). Non-areal testers return None.
        (Predicate::Touches, _) => match tester.classify_area_point(point) {
            Some(RingClass::Boundary) => true,
            Some(_) => false,
            None => return None,
        },
        _ => return None,
    })
}

/// One containment-family pair against a SMALL CONVEX hole-free scalar
/// — pure halfplane signs (see `convex_halfplanes_cover`). Strict
/// (`contains_properly`) is decisive for any candidate; inclusive
/// confirmation needs area under `contains`/`within` (a covered line
/// can ride the boundary), where covered non-areal rows fall back to
/// the per-pair lane.
fn convex_scalar_pair(
    spec: &PredicateSpec,
    scalar: &ShapeData,
    element: &ShapeData,
    scalar_is_left: bool,
) -> bool {
    let (Some(ccw), Shape::Polygon(container)) = (scalar.convex_shell(), scalar.shape()) else {
        unreachable!("gated by the caller");
    };
    if element.bounds().is_none() {
        return false;
    }
    let shell = container.shell.coords();
    let strict = spec.predicate == Predicate::ContainsProperly;
    let covered = if strict {
        convex_halfplanes_cover::<true, _>(shell, ccw, element.shape())
    } else {
        convex_halfplanes_cover::<false, _>(shell, ccw, element.shape())
    };
    if !covered {
        return false;
    }
    let needs_area = matches!(spec.predicate, Predicate::Contains | Predicate::Within);
    if strict || !needs_area || element.shape().has_area_parts() {
        return true;
    }
    if scalar_is_left {
        scalar_pair(spec, scalar, element)
    } else {
        scalar_pair(spec, element, scalar)
    }
}

/// Evaluate a packed point batch against one fixed handle: the cached
/// hierarchical `PointBatchTester` when the predicate maps onto it (built
/// once per handle), else the table's point kernel per probe. `None` when
/// the spec has no point kernel for that side — the caller falls through
/// to its shape lanes.
pub(crate) fn point_batch(
    spec: &PredicateSpec,
    scalar: &ShapeData,
    points: &PointRows<'_>,
    scalar_is_left: bool,
) -> Option<Vec<bool>> {
    let kernel_for_side = if scalar_is_left {
        spec.right_point.is_some()
    } else {
        spec.left_point.is_some()
    };
    if !kernel_for_side {
        return None;
    }
    if points.len() >= PointBatchTester::MIN_PROBES
        && let Some(tester) = scalar.point_tester()
        && let Some(first) = points.first()
        && point_batch_eval(spec, tester, first, scalar_is_left).is_some()
    {
        return Some(
            points
                .iter()
                .map(|point| {
                    point_batch_eval(spec, tester, point, scalar_is_left).expect("probed above")
                })
                .collect(),
        );
    }
    Some(if scalar_is_left {
        let kernel = spec.right_point.expect("checked above");
        points.iter().map(|point| kernel(scalar, point)).collect()
    } else {
        let kernel = spec.left_point.expect("checked above");
        points.iter().map(|point| kernel(point, scalar)).collect()
    })
}

/// Evaluate `spec` for one pair, taking the point fast kernels when an operand
/// is a point.
pub(crate) fn scalar_pair(spec: &PredicateSpec, left: &ShapeData, right: &ShapeData) -> bool {
    // Kind×kind constant before point kernels / bounds / contact / relate.
    if let Some(verdict) = spec.predicate.shape_kind_gate(left.shape(), right.shape()) {
        return verdict;
    }
    if let (Some(right_point), Shape::Point(point)) = (spec.right_point, right.shape()) {
        if let Some(tester) = left.point_tester()
            && let Some(verdict) = point_batch_eval(spec, tester, *point, true)
        {
            return verdict;
        }
        return right_point(left, *point);
    }
    if let (Some(left_point), Shape::Point(point)) = (spec.left_point, left.shape()) {
        if let Some(tester) = right.point_tester()
            && let Some(verdict) = point_batch_eval(spec, tester, *point, false)
        {
            return verdict;
        }
        return left_point(*point, right);
    }
    // The intersects family runs on the handles' cached prepared state (the
    // native oracle), and the relate-class predicates run their gates over
    // the handles' CACHED bounds — no per-pair coordinate re-scan anywhere.
    match spec.predicate {
        Predicate::Intersects => left.intersects(right),
        Predicate::Disjoint => !left.intersects(right),
        Predicate::Contains => left.contains_cached(right),
        Predicate::Within => right.contains_cached(left),
        Predicate::Covers => left.covers_cached(right),
        Predicate::CoveredBy => right.covers_cached(left),
        Predicate::Equals => left.equals_cached(right),
        Predicate::Touches => left.touches_cached(right),
        Predicate::Crosses => left.crosses_cached(right),
        Predicate::Overlaps => left.overlaps_cached(right),
        Predicate::ContainsProperly => left.contains_properly_cached(right),
    }
}
