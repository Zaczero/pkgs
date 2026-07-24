#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! The one source of truth for boolean spatial predicates.
//!
//! Every surface that evaluates a named predicate — free functions, `Geometry`
//! and `GeometryArray` methods, `PreparedGeometry`, the spatial index refine
//! loop, and `join` — reads its facts from [`Predicate::spec`] and runs batches
//! through [`scalar_vs_shapes`]. One table, one batch policy: the surfaces
//! cannot drift apart in which tokens they accept, which point fast paths they
//! take, or when they amortize a prepared relation.

use crate::geometry::{
    Bounds, Dimension, Point, PointBatchTester, Shape, ShapeData, convex_halfplanes_cover,
    point_is_geographic_pole, pole_position, topology_split,
};
use crate::{PointRows, PyGeometryArray, ShapeRow};

/// Minimum batch size at which building one prepared relation (or one cached
/// geo tree) from the fixed scalar operand amortizes its setup cost.
///
/// Below this the per-pair kernels win.
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
            Self::Touches => {
                self.symmetric_spec("touches", None, None, Some(IndexEnvelope::Intersecting))
            },
            Self::Crosses => {
                self.symmetric_spec("crosses", None, None, Some(IndexEnvelope::Intersecting))
            },
            Self::Overlaps => {
                self.symmetric_spec("overlaps", None, None, Some(IndexEnvelope::Intersecting))
            },
            Self::Equals => {
                self.symmetric_spec("equals", None, None, Some(IndexEnvelope::Intersecting))
            },
        }
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

/// How a scalar-vs-batch evaluation runs, chosen once per batch by
/// [`batch_strategy`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BatchStrategy {
    /// Per-pair kernel: small batches, or an empty scalar (whose semantics
    /// live in the kernels, not the relate graph).
    PerPair,
    /// Build the scalar's prepared/cached state once, then gate each element
    /// by the point kernel / bounds reject / topology pair — one loop for
    /// both the intersects-family oracle and the relate-class accessors.
    Batched,
}

fn batch_strategy(scalar: &Shape, batch_len: usize) -> BatchStrategy {
    if batch_len < PREPARED_PREDICATE_MIN || scalar.bounds().is_none() {
        BatchStrategy::PerPair
    } else {
        // Above the amortize threshold the scalar's facet trees / point
        // tester build once and serve every row (intersects family and
        // relate-class predicates share the same batch loop body).
        BatchStrategy::Batched
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
    let scalar_split = (geographic && scalar.shape().crosses_antimeridian())
        .then(|| ShapeData::from(topology_split(scalar.shape())));
    let scalar = scalar_split.as_ref().unwrap_or(scalar_orig);
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
        // The scalar's cached band-indexed tester answers point elements
        // (built once per handle; `point_batch_eval` mirrors the kernel
        // table exactly), so prepared/array batches never pay a full
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
    let bounds_reject = |index: usize, row: ShapeRow<'_>| -> Option<bool> {
        if skip_antimeridian_bounds_gate_row(geographic, row) {
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
                element_point(row).unwrap_or_else(|| {
                    dispatch_row_data(array, index, row, |element| {
                        convex_scalar_pair(spec, scalar, element, scalar_is_left)
                    })
                })
            })
            .collect();
    }
    match batch_strategy(scalar, elements.len()) {
        // `scalar_pair` carries the point kernels AND the native
        // intersects-family routing, on both operands' cached handles.
        // Bounds gate only — the point kernel is not applied here so empty-
        // scalar / below-threshold batches keep their historical order of
        // work (point fast path is reserved for the amortized Batched lane).
        BatchStrategy::PerPair => elements
            .map(|(index, row)| {
                bounds_reject(index, row).unwrap_or_else(|| {
                    dispatch_row_data(array, index, row, |element| {
                        topology_scalar_pair_unchecked(
                            spec,
                            scalar,
                            element,
                            scalar_is_left,
                            geographic,
                        )
                    })
                })
            })
            .collect(),
        // Intersects-family and relate-class predicates share one loop: the
        // scalar's facet trees / point tester build once, each Mixed element's
        // caches persist on its handle, and `topology_scalar_pair` both
        // answers the named predicate and split-normalizes geographic
        // antimeridian crossings (no post-hoc negation).
        BatchStrategy::Batched => elements
            .map(|(index, row)| {
                element_point(row)
                    .or_else(|| bounds_reject(index, row))
                    .unwrap_or_else(|| {
                        dispatch_row_data(array, index, row, |element| {
                            topology_scalar_pair_unchecked(
                                spec,
                                scalar,
                                element,
                                scalar_is_left,
                                geographic,
                            )
                        })
                    })
            })
            .collect(),
    }
}

fn topology_scalar_pair_unchecked(
    spec: &PredicateSpec,
    left: &ShapeData,
    right: &ShapeData,
    left_is_scalar: bool,
    geographic: bool,
) -> bool {
    let (left, right) = if left_is_scalar {
        (left, right)
    } else {
        (right, left)
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
/// band-indexed tester when the predicate maps onto it (built once per
/// handle), else the table's point kernel per probe. `None` when the
/// spec has no point kernel for that side — the caller falls through to
/// its shape lanes.
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
