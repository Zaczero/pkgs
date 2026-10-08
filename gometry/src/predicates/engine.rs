//! The one source of truth for boolean spatial predicates.
//!
//! Every surface that evaluates a named predicate — free functions, `Geometry`
//! and `GeometryArray` methods, the spatial index refine
//! loop, and `join` — reads dispatch facts from [`Predicate::spec`] and asks
//! the domain's topology owner for an exact answer. Cartesian predicates use
//! the witness evaluator, with `intersects` and `disjoint` retaining their
//! prepared kernel; geographic predicates use their chart relation.

use std::cell::OnceCell;

use crate::geometry::source_support::{PointMembership, cartesian_row_point_location};
use crate::geometry::storage::PackedRow;
use crate::geometry::{
    Bounds, CompiledPattern, GeographicPrepared, GeographicTopologyRoute, Loc, PreparedPlan, Shape,
    SharedShapeOwners, TopologyDomain, graph_relate_data, ring_winding,
};
#[cfg(all(test, debug_assertions))]
use crate::test_support::binary_dispatch_counters::point_predicate_dispatch;

#[derive(Clone, Copy)]
pub(crate) enum PreparedTopologyPair<'a> {
    Cartesian {
        left: &'a PreparedPlan,
        right: &'a PreparedPlan,
    },
    Geographic {
        left: GeographicPrepared<'a>,
        right: GeographicPrepared<'a>,
    },
}

impl<'a> PreparedTopologyPair<'a> {
    pub(crate) fn try_for_domain(
        left: &'a PreparedPlan,
        left_cache: &'a crate::geometry::FrameDependentCaches,
        right: &'a PreparedPlan,
        right_cache: &'a crate::geometry::FrameDependentCaches,
        domain: TopologyDomain,
    ) -> crate::error::Result<Self> {
        match domain {
            TopologyDomain::Cartesian => Ok(Self::Cartesian { left, right }),
            TopologyDomain::Geographic(frame) => {
                Self::try_geographic(left, left_cache, right, right_cache, frame)
            },
        }
    }

    /// The planar pair. Cartesian topology reads no frame products, so this
    /// constructor takes none — resolving them would allocate a row-product
    /// chunk per operand for this arm to ignore.
    pub(crate) const fn cartesian(left: &'a PreparedPlan, right: &'a PreparedPlan) -> Self {
        Self::Cartesian { left, right }
    }

    pub(crate) fn try_geographic(
        left: &'a PreparedPlan,
        left_cache: &'a crate::geometry::FrameDependentCaches,
        right: &'a PreparedPlan,
        right_cache: &'a crate::geometry::FrameDependentCaches,
        frame: crate::geographic::GeographicFrame,
    ) -> crate::error::Result<Self> {
        Ok(Self::from_geographic(
            GeographicPrepared::try_new(left, left_cache, frame)?,
            GeographicPrepared::try_new(right, right_cache, frame)?,
        ))
    }

    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the geographic variant stores both prepared views by value"
    )]
    pub(crate) const fn from_geographic(
        left: GeographicPrepared<'a>,
        right: GeographicPrepared<'a>,
    ) -> Self {
        Self::Geographic { left, right }
    }

    pub(crate) fn relate(self) -> crate::error::Result<crate::geometry::De9im> {
        match self {
            Self::Cartesian { left, right } => graph_relate_data(left, right),
            Self::Geographic { left, right } => {
                crate::geometry::graph_relate_data_geographic(left, right)
            },
        }
    }
}
use crate::{GeometryKind, PyGeometryArray, ShapeRow};

/// Historical amortize threshold for prepared/array batch entry points.
///
/// Answer-producing algorithm selection must NOT depend on this constant:
/// `scalar_vs_shapes` uses one pipeline for every batch length. Call sites may
/// still use it only as a layout/scheduling hint (e.g. detach vs in-GIL), never
/// to pick a different predicate kernel.
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
    /// Use the query's whole outer envelope for containment predicates. The
    /// candidate walk is still intersecting and exact refinement owns
    /// containment; a raw item envelope is not a support certificate.
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
    /// R-tree candidate strategy, or `None` when the predicate cannot be
    /// index-accelerated (`disjoint` would enumerate the complement).
    pub index_envelope: Option<IndexEnvelope>,
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

impl Predicate {
    pub(crate) const fn point_membership(self, point_is_left: bool) -> Option<PointMembership> {
        match (self, point_is_left) {
            (Self::Contains | Self::ContainsProperly, false) | (Self::Within, true) => {
                Some(PointMembership::Strict)
            },
            (Self::Covers, false) | (Self::CoveredBy, true) => {
                Some(PointMembership::BoundaryInclusive)
            },
            _ => None,
        }
    }

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
    pub(crate) const fn token(self) -> &'static str {
        self.spec().token
    }

    /// The dispatch facts for this predicate — the only place they live.
    pub(crate) const fn spec(self) -> PredicateSpec {
        match self {
            Self::Intersects => {
                self.symmetric_spec("intersects", Some(IndexEnvelope::Intersecting))
            },
            Self::Disjoint => self.symmetric_spec("disjoint", None),
            Self::Contains => self.directed_spec("contains"),
            Self::ContainsProperly => self.directed_spec("contains_properly"),
            Self::Within => self.directed_spec("within"),
            Self::Covers => self.directed_spec("covers"),
            Self::CoveredBy => self.directed_spec("covered_by"),
            Self::Touches => self.symmetric_spec("touches", Some(IndexEnvelope::Intersecting)),
            Self::Crosses => self.symmetric_spec("crosses", Some(IndexEnvelope::Intersecting)),
            Self::Overlaps => self.symmetric_spec("overlaps", Some(IndexEnvelope::Intersecting)),
            Self::Equals => self.symmetric_spec("equals", Some(IndexEnvelope::Intersecting)),
        }
    }

    /// Structural refutations that do not depend on a carrier's effective
    /// support. A single Point cannot cross or overlap another carrier;
    /// declared polygon and line kinds are not support-dimension proofs.
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
                // A single Point has no crossing locus in either order.
                if matches!(left, KindClass::Point) || matches!(right, KindClass::Point) {
                    return Some(false);
                }
                None
            },
            Self::Overlaps => {
                // A single Point never overlaps another carrier. Other
                // declarations do not prove their effective support
                // dimension: polygons can collapse and lines can be point-only.
                if matches!(left, KindClass::Point) || matches!(right, KindClass::Point) {
                    return Some(false);
                }
                None
            },
            // Every other predicate uses the complete relation matrix.
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

    const fn symmetric_spec(
        self,
        token: &'static str,
        index_envelope: Option<IndexEnvelope>,
    ) -> PredicateSpec {
        PredicateSpec {
            predicate: self,
            token,
            symmetric: true,
            index_envelope,
        }
    }

    const fn directed_spec(self, token: &'static str) -> PredicateSpec {
        // Directed containment predicates use the query's whole outer window;
        // candidate traversal remains intersecting and exact refinement decides
        // whether the effective support is contained.
        let index_envelope = Some(match self {
            Self::Contains | Self::ContainsProperly | Self::Covers => {
                IndexEnvelope::ContainedInQuery
            },
            Self::Within
            | Self::CoveredBy
            | Self::Intersects
            | Self::Disjoint
            | Self::Touches
            | Self::Crosses
            | Self::Overlaps
            | Self::Equals => IndexEnvelope::Intersecting,
        });
        PredicateSpec {
            predicate: self,
            token,
            symmetric: false,
            index_envelope,
        }
    }
}

impl PredicateSpec {
    /// A definite answer from pre-topology facts, before the pair is related.
    ///
    /// Disjoint boxes refute every predicate whose matrix requires an
    /// intersection. Equality also refutes when both operands have a
    /// certificate of non-empty support; raw bounds alone cannot distinguish
    /// two empty supports, which are equal.
    pub(crate) fn bounds_gate(
        &self,
        left: &GateOperand<'_>,
        right: &GateOperand<'_>,
    ) -> Option<bool> {
        let (left_bounds, right_bounds) = (left.bounds?, right.bounds?);
        if !left_bounds.intersects(right_bounds) {
            return match self.predicate {
                Predicate::Disjoint => Some(true),
                Predicate::Intersects
                | Predicate::Contains
                | Predicate::ContainsProperly
                | Predicate::Within
                | Predicate::Covers
                | Predicate::CoveredBy
                | Predicate::Touches
                | Predicate::Crosses
                | Predicate::Overlaps => Some(false),
                Predicate::Equals
                    if left.has_known_nonempty_support() && right.has_known_nonempty_support() =>
                {
                    Some(false)
                },
                Predicate::Equals => None,
            };
        }
        None
    }

    /// Every answer available before either operand is prepared.
    ///
    /// Ordered by what each stage has to touch: declared kinds refute without
    /// reading coordinates, disjoint bounds refute shared-support predicates,
    /// and two rectangles that meet settle `intersects` outright. `None` hands
    /// the pair to the exact kernel.
    pub(crate) fn pre_topology(
        &self,
        left: &GateOperand<'_>,
        right: &GateOperand<'_>,
    ) -> crate::error::Result<Option<bool>> {
        #[cfg(all(test, debug_assertions))]
        PRE_TOPOLOGY_CALLS.with(|calls| calls.set(calls.get().wrapping_add(1)));
        if let Some(verdict) = self.predicate.kind_gate(left.kind, right.kind) {
            return Ok(Some(verdict));
        }
        if !left.bounds_sound || !right.bounds_sound {
            return Ok(None);
        }
        if let Some(verdict) = self.bounds_gate(left, right) {
            return Ok(Some(verdict));
        }
        let (Some(left_bounds), Some(right_bounds)) = (left.bounds, right.bounds) else {
            return Ok(None);
        };
        self.rectangles_meet(left, left_bounds, right, right_bounds)
    }

    /// Two axis-aligned rectangles whose bounds overlap share ground, because
    /// each covers its own bounding box exactly. Reaching here means the
    /// bounds gate already found the boxes meet, so no graph decides this.
    ///
    /// Geographic operands included, because this library charts a geographic
    /// edge as a straight segment between canonical endpoints rather than a
    /// great-circle arc, so a rectangle really is its box here. Only the seam,
    /// the poles and a non-canonical longitude break that, and reaching this
    /// point means both operands already passed all three.
    fn rectangles_meet(
        &self,
        left: &GateOperand<'_>,
        left_bounds: Bounds,
        right: &GateOperand<'_>,
        right_bounds: Bounds,
    ) -> crate::error::Result<Option<bool>> {
        let meeting = match self.predicate {
            Predicate::Intersects => true,
            Predicate::Disjoint => false,
            Predicate::Contains
            | Predicate::ContainsProperly
            | Predicate::Within
            | Predicate::Covers
            | Predicate::CoveredBy
            | Predicate::Touches
            | Predicate::Crosses
            | Predicate::Overlaps
            | Predicate::Equals => return Ok(None),
        };
        if left.rectangle == Some(false) || right.rectangle == Some(false) {
            return Ok(None);
        }
        Ok(
            (left.is_rectangle(left_bounds)? && right.is_rectangle(right_bounds)?)
                .then_some(meeting),
        )
    }
}

/// One operand as the pre-topology gates see it: a storage row, its declared
/// kind, its coordinate bounds, and whether those bounds mean what they say.
///
/// Deliberately not a [`PreparedPlan`] — a packed row would have to be
/// materialized to build one, and the point of these gates is to answer
/// before anything is materialized at all. An operand fixed for a whole call
/// is built once; operands that vary per row are built per row.
#[derive(Clone, Copy)]
pub(crate) struct GateOperand<'a> {
    row: ShapeRow<'a>,
    kind: GeometryKind,
    bounds: Option<Bounds>,
    bounds_sound: bool,
    /// Whether the operand is an axis-aligned rectangle, once settled for an
    /// operand that stays fixed across a call.
    rectangle: Option<bool>,
}

impl<'a> GateOperand<'a> {
    pub(crate) const fn bounds(&self) -> Option<Bounds> {
        self.bounds
    }
    /// Read Cartesian gate facts off `row`, preferring `bounds` when the caller
    /// already holds them cached. Packed rows stay unmaterialized.
    pub(crate) fn cartesian(row: ShapeRow<'a>, bounds: Option<Bounds>) -> Self {
        let (kind, _) = row_kind_empty(row);
        let bounds = bounds.or_else(|| row.quick_bounds());
        Self {
            row,
            kind,
            bounds,
            bounds_sound: true,
            rectangle: None,
        }
    }

    /// Geographic gate facts derived from one admitted operand. There is no
    /// geographic operand except from a live [`GeographicPrepared`].
    pub(crate) fn from_prepared(prepared: &GeographicPrepared<'a>) -> Self {
        let plan = prepared.plan();
        let row = ShapeRow::Shape(plan.shape());
        let (kind, _) = row_kind_empty(row);
        Self {
            row,
            kind,
            bounds: plan.bounds(),
            bounds_sound: prepared.bounds_sound(),
            rectangle: None,
        }
    }

    /// Gate facts of an operand every pair of a call shares, with its
    /// rectangle fact settled once.
    pub(crate) fn fixed(shape: &'a Shape, bounds: Option<Bounds>) -> Self {
        let mut operand = Self::cartesian(ShapeRow::Shape(shape), bounds);
        operand.rectangle = Some(
            operand
                .bounds
                .is_some_and(|bounds| crate::geometry::is_axis_aligned_rectangle(shape, bounds)),
        );
        operand
    }

    fn is_rectangle(&self, bounds: Bounds) -> crate::error::Result<bool> {
        if let Some(known) = self.rectangle {
            return Ok(known);
        }
        if self.kind != GeometryKind::Polygon {
            return Ok(false);
        }
        self.row
            .try_with_shape(|shape| Ok(crate::geometry::is_axis_aligned_rectangle(shape, bounds)))
    }

    /// Positive-only proof for the empty-support equality case. A hole-free
    /// ring with nonzero exact area owns a filled face; all other areal
    /// support remains unknown rather than guessed from declared kind.
    fn has_known_nonempty_support(&self) -> bool {
        match self.row {
            ShapeRow::Handle(plan) => shape_has_known_nonempty_support(
                plan.shape(),
                &mut SharedShapeOwners::for_collections(),
            ),
            ShapeRow::Shape(shape) => {
                shape_has_known_nonempty_support(shape, &mut SharedShapeOwners::for_collections())
            },
            ShapeRow::Point(_) => true,
            ShapeRow::Packed(row @ PackedRow::Polygons { .. }) => {
                let Some(mut rings) = ShapeRow::Packed(row).polygon_rings() else {
                    return false;
                };
                let Some(shell) = rings.next() else {
                    return false;
                };
                rings.next().is_none() && !ring_winding(&shell).is_degenerate()
            },
            ShapeRow::Packed(row) => !row.is_empty(),
        }
    }

    /// Whether this operand's coordinate box means what it says, so a caller
    /// may compare it as a plain rectangle. False across the seam, at a pole,
    /// and at a longitude the frame would move.
    pub(crate) const fn bounds_are_sound(&self) -> bool {
        self.bounds_sound
    }
}

fn shape_has_known_nonempty_support(shape: &Shape, owners: &mut SharedShapeOwners) -> bool {
    match shape {
        Shape::Point(_) => true,
        Shape::MultiPoint(points) => !points.is_empty(),
        Shape::LineString(line) => !line.is_empty(),
        Shape::MultiLineString(lines) => lines.iter().any(|line| !line.is_empty()),
        Shape::Polygon(polygon) => {
            polygon.holes.is_empty() && !ring_winding(&polygon.shell).is_degenerate()
        },
        Shape::MultiPolygon(polygons) => polygons.iter().any(|polygon| {
            polygon.holes.is_empty() && !ring_winding(&polygon.shell).is_degenerate()
        }),
        Shape::GeometryCollection(parts) => {
            owners.first_collection(parts.as_slice(), parts.members_are_shared())
                && parts
                    .iter()
                    .any(|part| shape_has_known_nonempty_support(part, owners))
        },
        Shape::Empty(..) => false,
    }
}

/// Evaluate `spec` between one fixed `scalar` operand and every row of
/// `elements`, preparing the scalar's frame view at most once.
///
/// `scalar_is_left` names the operand order: `true` evaluates
/// `scalar OP element`, `false` evaluates `element OP scalar`. This is the
/// single batch engine behind the free functions, the array methods,
/// and the spatial-index refine loop. Rows come straight
/// off the storage: `Mixed` rows keep their persistent handles (prepared
/// state accumulates), packed point rows never synthesize one.
pub(crate) fn scalar_vs_shapes<'a>(
    spec: &PredicateSpec,
    scalar: &PreparedPlan,
    elements: impl ExactSizeIterator<Item = (usize, Option<ShapeRow<'a>>)>,
    scalar_is_left: bool,
    array: Option<&PyGeometryArray>,
    domain: TopologyDomain,
    scalar_cache: &crate::geometry::FrameDependentCaches,
) -> Result<Vec<bool>, (usize, crate::error::Error)> {
    let scalar_bounds = scalar.bounds();
    let element_bounds = array.and_then(PyGeometryArray::existing_element_bounds);
    let row_is_missing = |index: usize| array.is_some_and(|array| array.is_row_missing(index));
    let mut row_operation = array.map(PyGeometryArray::row_operation);
    match domain {
        TopologyDomain::Cartesian => {
            let scalar_operand = GateOperand::fixed(scalar.shape(), scalar_bounds);
            let gate = |index: usize, row: ShapeRow<'_>| -> crate::error::Result<Option<bool>> {
                let row_operand = GateOperand::cartesian(
                    row,
                    element_bounds
                        .as_ref()
                        .and_then(|bounds| bounds.get(index).copied().flatten()),
                );
                let (left, right) = if scalar_is_left {
                    (scalar_operand, row_operand)
                } else {
                    (row_operand, scalar_operand)
                };
                if let Some(verdict) = spec.pre_topology(&left, &right)? {
                    return Ok(Some(verdict));
                }
                if spec.predicate != Predicate::Intersects
                    || !left.bounds_are_sound()
                    || !right.bounds_are_sound()
                {
                    return Ok(None);
                }
                row.try_with_shape(|shape| {
                    let Some(bounds) = shape.bounds() else {
                        return Ok(None);
                    };
                    if !crate::geometry::is_axis_aligned_rectangle(shape, bounds) {
                        return Ok(None);
                    }
                    Ok(Some(crate::geometry::exact_intersects_bounds(
                        scalar.shape(),
                        bounds,
                    )?))
                })
            };
            // Point rows probe the scalar once each: index it for a batch.
            let element_count = elements.len();
            let mut point_queries: Option<crate::geometry::PreparedPointQueries<'_>> = None;
            let point_membership = spec
                .predicate
                .point_membership(!scalar_is_left)
                .or_else(|| {
                    matches!(spec.predicate, Predicate::Intersects | Predicate::Disjoint)
                        .then_some(PointMembership::BoundaryInclusive)
                });
            let mut results = Vec::with_capacity(element_count);
            for (index, row) in elements {
                let mut evaluate = || -> crate::error::Result<bool> {
                    if row_is_missing(index) {
                        return Ok(false);
                    }
                    let Some(row) = row else { return Ok(false) };
                    if let Some(point) = row.point()
                        && let Some(membership) = point_membership
                    {
                        if scalar_bounds.is_none_or(|bounds| !bounds.contains_xy(point.xy())) {
                            return Ok(spec.predicate == Predicate::Disjoint);
                        }
                        let queries = if let Some(queries) = point_queries {
                            queries
                        } else {
                            let queries = scalar.prepare_point_queries(element_count)?;
                            point_queries = Some(queries);
                            queries
                        };
                        let matches = queries.membership(point, membership)?;
                        return Ok(matches != (spec.predicate == Predicate::Disjoint));
                    }
                    if let Some(verdict) = gate(index, row)? {
                        return Ok(verdict);
                    }
                    if matches!(spec.predicate, Predicate::Intersects | Predicate::Disjoint) {
                        if let Some(verdict) =
                            row_point_intersection(spec, ShapeRow::Shape(scalar.shape()), row)?
                        {
                            return Ok(verdict);
                        }
                    } else {
                        return crate::geometry::evaluate_cartesian_prepared_row(
                            scalar,
                            row,
                            scalar_is_left,
                            spec.predicate,
                        );
                    }
                    let element = row_operation.as_mut().map_or_else(
                        || crate::array::PreparedRow::transient(row),
                        |operation| operation.prepared_at(index, row),
                    )?;
                    let operands = if scalar_is_left {
                        PreparedTopologyPair::Cartesian {
                            left: scalar,
                            right: &element,
                        }
                    } else {
                        PreparedTopologyPair::Cartesian {
                            left: &element,
                            right: scalar,
                        }
                    };
                    topology_pair_after_gates(spec, &operands)
                };
                let verdict = evaluate().map_err(|error| (index, error))?;
                results.push(verdict);
            }
            Ok(results)
        },
        TopologyDomain::Geographic(frame) => {
            let (scalar_kind, _) = row_kind_empty(ShapeRow::Shape(scalar.shape()));
            let scalar_prepared = OnceCell::new();
            let scalar_exact_topology = OnceCell::new();
            let mut results = Vec::with_capacity(elements.len());
            for (index, row) in elements {
                if row_is_missing(index) {
                    results.push(false);
                    continue;
                }
                let Some(row) = row else {
                    results.push(false);
                    continue;
                };
                let (row_kind, _) = row_kind_empty(row);
                let (left_kind, right_kind) = if scalar_is_left {
                    (scalar_kind, row_kind)
                } else {
                    (row_kind, scalar_kind)
                };
                if let Some(verdict) = spec.predicate.kind_gate(left_kind, right_kind) {
                    results.push(verdict);
                    continue;
                }
                let element = row_operation
                    .as_mut()
                    .map_or_else(
                        || crate::array::PreparedRow::transient(row),
                        |operation| operation.prepared_at(index, row),
                    )
                    .map_err(|error| (index, error))?;
                let row_cache = row_operation
                    .as_mut()
                    .map(|operation| operation.frame_cache(index));
                // An array-backed row already owns its frame products; only a
                // row without them pays for a transient set, which has to
                // outlive the branch that builds it.
                let transient_cache;
                #[expect(
                    clippy::option_if_let_else,
                    reason = "map_or_else cannot initialize the binding the borrow outlives"
                )]
                let element_cache = if let Some(cache) = row_cache.as_deref() {
                    cache
                } else {
                    transient_cache = crate::geometry::FrameDependentCaches::default();
                    &transient_cache
                };
                let element_prepared = GeographicPrepared::try_new(&element, element_cache, frame)
                    .map_err(|error| (index, error))?;
                let prepared = *scalar_prepared
                    .get_or_try_init(|| GeographicPrepared::try_new(scalar, scalar_cache, frame))
                    .map_err(|error| (index, error))?;
                let scalar_operand = GateOperand::from_prepared(&prepared);
                let row_operand = GateOperand::from_prepared(&element_prepared);
                let (left, right) = if scalar_is_left {
                    (scalar_operand, row_operand)
                } else {
                    (row_operand, scalar_operand)
                };
                if let Some(verdict) = spec
                    .pre_topology(&left, &right)
                    .map_err(|error| (index, error))?
                {
                    results.push(verdict);
                    continue;
                }
                if spec.predicate == Predicate::Intersects
                    && left.bounds_are_sound()
                    && right.bounds_are_sound()
                {
                    let rectangle = element_prepared.plan().shape();
                    if let Some(bounds) = rectangle.bounds()
                        && crate::geometry::is_axis_aligned_rectangle(rectangle, bounds)
                    {
                        if prepared
                            .chart_route()
                            .combine(element_prepared.chart_route())
                            == GeographicTopologyRoute::ExactPartition
                        {
                            results.push(
                                crate::geometry::exact_intersects_bounds(
                                    prepared.plan().shape(),
                                    bounds,
                                )
                                .map_err(|error| (index, error))?,
                            );
                            continue;
                        }
                        if let Some(location) = scalar_exact_topology
                            .get_or_try_init(|| prepared.exact_topology())
                            .map_err(|error| (index, error))?
                            .classify_rectangle(bounds)
                            .map_err(|error| (index, error))?
                        {
                            results.push(matches!(
                                location,
                                crate::geometry::ExactGeographicRectangleLocation::Interior
                                    | crate::geometry::ExactGeographicRectangleLocation::Boundary
                            ));
                            continue;
                        }
                    }
                }
                let operands = if scalar_is_left {
                    PreparedTopologyPair::from_geographic(prepared, element_prepared)
                } else {
                    PreparedTopologyPair::from_geographic(element_prepared, prepared)
                };
                let verdict =
                    topology_scalar_pair(spec, &operands).map_err(|error| (index, error))?;
                results.push(verdict);
            }
            Ok(results)
        },
    }
}

/// Declared kind + emptiness for one storage row (no materialize for packed).
pub(crate) fn row_kind_empty(row: ShapeRow<'_>) -> (GeometryKind, bool) {
    (row.kind(), row.is_empty())
}

pub(crate) fn topology_scalar_pair(
    spec: &PredicateSpec,
    operands: &PreparedTopologyPair<'_>,
) -> crate::error::Result<bool> {
    let (left, right) = match *operands {
        PreparedTopologyPair::Cartesian { left, right } => {
            let (left_shape, right_shape) = (left.shape(), right.shape());
            if (matches!(left_shape, Shape::Point(_)) || matches!(right_shape, Shape::Point(_)))
                && let Some(answer) = cartesian_point_predicate(spec, left, right)?
            {
                return Ok(answer);
            }
            (
                GateOperand::cartesian(ShapeRow::Shape(left_shape), left.bounds()),
                GateOperand::cartesian(ShapeRow::Shape(right_shape), right.bounds()),
            )
        },
        PreparedTopologyPair::Geographic { left, right } => (
            GateOperand::from_prepared(&left),
            GateOperand::from_prepared(&right),
        ),
    };
    // The array, index and coverer paths all run the pre-topology gates before
    // admitting a pair to the relation; the scalar pair was the one lane that
    // did not, so `gm.intersects(a, b)` on two operands a hundred units apart
    // still built the exact pair topology to discover they share no point.
    if let Some(verdict) = spec.pre_topology(&left, &right)? {
        return Ok(verdict);
    }
    topology_pair_after_gates(spec, operands)
}

pub(crate) fn topology_shapes_predicate(
    predicate: Predicate,
    left: &Shape,
    right: &Shape,
) -> crate::error::Result<bool> {
    let left = PreparedPlan::transient(left);
    let right = PreparedPlan::transient(right);
    topology_scalar_pair(
        &predicate.spec(),
        &PreparedTopologyPair::cartesian(&left, &right),
    )
}

/// Decide a pair after its caller's pre-topology gates have declined.
pub(crate) fn topology_pair_after_gates(
    spec: &PredicateSpec,
    operands: &PreparedTopologyPair<'_>,
) -> crate::error::Result<bool> {
    if let PreparedTopologyPair::Cartesian { left, right } = *operands {
        if let Some(verdict) = cartesian_point_predicate(spec, left, right)? {
            return Ok(verdict);
        }
        if matches!(spec.predicate, Predicate::Intersects | Predicate::Disjoint) {
            return Ok(left.intersects(right)? == (spec.predicate == Predicate::Intersects));
        }
        return crate::geometry::evaluate_cartesian_predicate(left, right, spec.predicate);
    }
    Ok(predicate_matrix_value(spec, operands.relate()?))
}

#[cfg(all(test, debug_assertions))]
thread_local! {
    static PRE_TOPOLOGY_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn cartesian_point_predicate(
    spec: &PredicateSpec,
    left: &PreparedPlan,
    right: &PreparedPlan,
) -> crate::error::Result<Option<bool>> {
    #[cfg(all(test, debug_assertions))]
    point_predicate_dispatch();
    if matches!(spec.predicate, Predicate::Intersects | Predicate::Disjoint) {
        return point_intersection_value(
            spec,
            ShapeRow::Shape(left.shape()).point(),
            ShapeRow::Shape(right.shape()).point(),
            |left_side, point| {
                (if left_side { left } else { right })
                    .cartesian_point_membership(point, PointMembership::BoundaryInclusive)
            },
        );
    }
    for (target, point_operand, point_is_left) in [(left, right, false), (right, left, true)] {
        if let Some(membership) = spec.predicate.point_membership(point_is_left)
            && let Some(point) = ShapeRow::Shape(point_operand.shape()).point()
        {
            return target
                .cartesian_point_membership(point, membership)
                .map(Some);
        }
    }
    Ok(None)
}

pub(crate) fn row_point_intersection(
    spec: &PredicateSpec,
    left: ShapeRow<'_>,
    right: ShapeRow<'_>,
) -> crate::error::Result<Option<bool>> {
    if !matches!(spec.predicate, Predicate::Intersects | Predicate::Disjoint) {
        return Ok(None);
    }
    let target = if right.point().is_some() { left } else { right };
    if crate::array::packed_row_persistent!(target) {
        return Ok(None);
    }
    point_intersection_value(spec, left.point(), right.point(), |left_side, point| {
        Ok(
            cartesian_row_point_location(if left_side { left } else { right }, point, true)
                != Loc::Exterior,
        )
    })
}

fn point_intersection_value(
    spec: &PredicateSpec,
    left_point: Option<crate::geometry::Point>,
    right_point: Option<crate::geometry::Point>,
    mut covers: impl FnMut(bool, crate::geometry::Point) -> crate::error::Result<bool>,
) -> crate::error::Result<Option<bool>> {
    let intersects = match (left_point, right_point) {
        (Some(left), Some(right)) => crate::geometry::same_point(left, right),
        (Some(point), None) => covers(false, point)?,
        (None, Some(point)) => covers(true, point)?,
        (None, None) => return Ok(None),
    };
    Ok(Some(if spec.predicate == Predicate::Disjoint {
        !intersects
    } else {
        intersects
    }))
}

/// A DE-9IM pattern over a scalar pair, decided as early as the evidence
/// allows: sound disjoint bounds settle every cell the bounds prove (a
/// curve's boundary cells stay open, since whether a closed line has a
/// boundary is a staging fact), and only a pair they leave open builds the
/// matrix.
pub(crate) fn topology_scalar_pattern(
    pattern: CompiledPattern,
    operands: &PreparedTopologyPair<'_>,
) -> crate::error::Result<bool> {
    match *operands {
        PreparedTopologyPair::Cartesian { left, right } => {
            crate::geometry::evaluate_cartesian_pattern(left, right, pattern)
        },
        PreparedTopologyPair::Geographic { left, right } => {
            let (left, right) = (
                GateOperand::from_prepared(&left),
                GateOperand::from_prepared(&right),
            );
            if let Some(verdict) = pattern_bounds_gate(pattern, &left, &right)? {
                return Ok(verdict);
            }
            Ok(operands.relate()?.matches(pattern))
        },
    }
}

/// What sound, disjoint bounds prove about a pattern: the four inner cells
/// are empty and each operand lies wholly in the other's exterior. Surface
/// dimensions are admitted only for a filled rectangle certificate; all other
/// areal support remains undecided here rather than inferred from kind.
fn pattern_bounds_gate(
    pattern: CompiledPattern,
    left: &GateOperand<'_>,
    right: &GateOperand<'_>,
) -> crate::error::Result<Option<bool>> {
    use crate::geometry::{De9imInterval, Dimension, OperandShape, dimension};
    if !left.bounds_sound || !right.bounds_sound {
        return Ok(None);
    }
    let (Some(left_bounds), Some(right_bounds)) = (left.bounds, right.bounds) else {
        return Ok(None);
    };
    if left_bounds.intersects(right_bounds) {
        return Ok(None);
    }
    let shape_of = |operand: GateOperand<'_>| -> crate::error::Result<Option<OperandShape>> {
        let Some(interior) = operand.row.with_shape(dimension)? else {
            return Ok(None);
        };
        if interior == Dimension::Surface {
            let Some(bounds) = operand.bounds else {
                return Ok(None);
            };
            let is_filled_rectangle = operand.row.try_with_shape(|shape| {
                Ok::<_, crate::error::Error>(crate::geometry::is_axis_aligned_rectangle(
                    shape, bounds,
                ))
            })?;
            if !is_filled_rectangle {
                return Ok(None);
            }
        }
        Ok(Some(OperandShape {
            interior,
            boundary: match interior {
                Dimension::Surface => Some(Dimension::Curve),
                // Open or closed is a staging fact; leave the cell's bound.
                Dimension::Curve => Some(Dimension::Point),
                Dimension::Point => None,
            },
        }))
    };
    let (Some(a), Some(b)) = (shape_of(*left)?, shape_of(*right)?) else {
        return Ok(None);
    };
    let mut interval = De9imInterval::open(a, b);
    for (row, col) in [
        (Loc::Interior, Loc::Interior),
        (Loc::Interior, Loc::Boundary),
        (Loc::Boundary, Loc::Interior),
        (Loc::Boundary, Loc::Boundary),
    ] {
        interval.settle(row, col);
    }
    interval.witness(Loc::Interior, Loc::Exterior, a.interior);
    interval.settle(Loc::Interior, Loc::Exterior);
    interval.witness(Loc::Exterior, Loc::Interior, b.interior);
    interval.settle(Loc::Exterior, Loc::Interior);
    if a.interior == Dimension::Surface {
        interval.witness(Loc::Boundary, Loc::Exterior, Dimension::Curve);
        interval.settle(Loc::Boundary, Loc::Exterior);
    }
    if b.interior == Dimension::Surface {
        interval.witness(Loc::Exterior, Loc::Boundary, Dimension::Curve);
        interval.settle(Loc::Exterior, Loc::Boundary);
    }
    Ok(interval.pattern(pattern))
}

/// A named predicate read off a fully decided matrix.
pub(crate) fn predicate_matrix_value(spec: &PredicateSpec, matrix: crate::geometry::De9im) -> bool {
    match spec.predicate {
        Predicate::Intersects => matrix.is_intersects(),
        Predicate::Disjoint => matrix.is_disjoint(),
        Predicate::Contains => matrix.is_contains(),
        Predicate::Within => matrix.transpose().is_contains(),
        Predicate::Covers => matrix.is_covers(),
        Predicate::CoveredBy => matrix.transpose().is_covers(),
        Predicate::Equals => matrix.is_equal_topo(),
        Predicate::Touches => matrix.is_touches(),
        Predicate::Crosses => matrix.is_crosses_by_dimension(),
        Predicate::Overlaps => matrix.is_overlaps_by_dimension(),
        Predicate::ContainsProperly => matrix.is_contains_properly(),
    }
}

#[cfg(test)]
mod tests {
    /// Sound disjoint bounds prove the four inner cells empty and each
    /// operand inside the other's exterior, and nothing else: whether an
    /// open line's boundary meets the other's exterior is a staging fact the
    /// bounds cannot see, so a pattern reading that cell must come back
    /// undecided rather than guessed.
    #[test]
    fn disjoint_bounds_decide_a_pattern_only_where_they_prove_it() {
        let gate =
            |left: &crate::geometry::Shape, right: &crate::geometry::Shape, pattern: &str| {
                let (lp, rp) = (
                    PreparedPlan::transient(left),
                    PreparedPlan::transient(right),
                );
                let lo = GateOperand::cartesian(ShapeRow::Shape(lp.shape()), lp.bounds());
                let ro = GateOperand::cartesian(ShapeRow::Shape(rp.shape()), rp.bounds());
                super::pattern_bounds_gate(
                    crate::geometry::CompiledPattern::new(pattern).unwrap(),
                    &lo,
                    &ro,
                )
                .unwrap()
            };
        let (a, b) = (rectangle(0.0, 0.0, 1.0, 1.0), rectangle(3.0, 3.0, 4.0, 4.0));
        assert_eq!(
            gate(&a, &b, "FF2FF1212"),
            Some(true),
            "the full disjoint areal matrix"
        );
        assert_eq!(gate(&a, &b, "T********"), Some(false));
        let (l, m) = (line((0.0, 0.0), (1.0, 1.0)), line((5.0, 5.0), (6.0, 6.0)));
        assert_eq!(
            gate(&l, &m, "FF*FF****"),
            Some(true),
            "disjoint, wildcards on the open cells"
        );
        assert_eq!(
            gate(&l, &m, "FF1FF*1*2"),
            Some(true),
            "consistent on every settled cell, wildcards on the open ones"
        );
        assert_eq!(
            gate(&l, &m, "FF1FF0102"),
            None,
            "BE reads a boundary cell the bounds cannot settle"
        );
        assert_eq!(gate(&l, &m, "FF1FF*102"), None, "EB likewise");
        assert_eq!(
            gate(&a, &l, "FF2FF11F2"),
            None,
            "the line's EB cell is open even against an areal operand"
        );
        // Polygon against a line disjoint from it: the line's EB cell is the
        // one the bounds cannot settle, and every other cell is decided.
        {
            let cells: [&str; 9] = [
                "T********",
                "*T*******",
                "**2******",
                "***T*****",
                "****T****",
                "*****1***",
                "******1**",
                "*******T*",
                "********2",
            ];
            let open: Vec<usize> = cells
                .iter()
                .enumerate()
                .filter(|(_, c)| gate(&a, &m, c).is_none())
                .map(|(i, _)| i)
                .collect();
            assert_eq!(open, vec![7], "only EB may be open for polygon-vs-line");
        }
        assert_eq!(
            gate(&a, &m, "FF2FF11F2"),
            None,
            "the line's EB cell is open even against an areal operand"
        );
        assert_eq!(
            gate(&a, &m, "FF2FF11*2"),
            Some(true),
            "and settled everywhere else"
        );
        assert_eq!(
            gate(&a, &l, "FF2FF11*2"),
            None,
            "a line inside the square's box: bounds meet, the gate declines"
        );
        let touching = rectangle(1.0, 0.0, 2.0, 1.0);
        assert_eq!(
            gate(&a, &touching, "FF2FF1212"),
            None,
            "bounds meet: the gate declines"
        );
        let cancelled =
            crate::io::parse_wkt("POLYGON ((0 0,2 0,2 2,0 2,0 0,0 2,2 2,2 0,0 0))").unwrap();
        let remote = rectangle(4.0, 4.0, 5.0, 5.0);
        assert_eq!(
            gate(&cancelled, &remote, "**2******"),
            None,
            "declared polygon kind cannot prove an effective interior"
        );
        assert!(!cancelled.relate_pattern(&remote, "**2******").unwrap());
    }

    use super::*;
    use crate::crs::frame::Frame;
    use crate::geometry::storage::GeometryArrayStorage;
    use crate::geometry::{CoordSeq, LineSeq, Point, Polygon, Ring};

    #[test]
    #[cfg(debug_assertions)]
    fn disjoint_nonpoint_pairs_skip_point_membership_dispatch() {
        use crate::test_support::binary_dispatch_counters;

        let left = PreparedPlan::new(rectangle(0.0, 0.0, 4.0, 4.0));
        let right = PreparedPlan::new(rectangle(10.0, 10.0, 14.0, 14.0));
        for predicate in [
            Predicate::Intersects,
            Predicate::Contains,
            Predicate::Within,
            Predicate::Touches,
        ] {
            binary_dispatch_counters::reset();
            assert!(
                !topology_scalar_pair(
                    &predicate.spec(),
                    &PreparedTopologyPair::cartesian(&left, &right),
                )
                .unwrap()
            );
            assert_eq!(
                binary_dispatch_counters::snapshot().point_predicate_dispatches,
                0
            );
        }
        let point = PreparedPlan::new(Shape::Point(Point::new_raw_xy(2.0, 2.0)));
        assert!(
            topology_scalar_pair(
                &Predicate::Contains.spec(),
                &PreparedTopologyPair::cartesian(&left, &point),
            )
            .unwrap()
        );
        assert!(binary_dispatch_counters::snapshot().point_predicate_dispatches > 0);
    }

    #[test]
    #[cfg(debug_assertions)]
    fn scalar_point_membership_precedes_generic_topology_gates() {
        let polygon =
            crate::io::parse_wkt("POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))").unwrap();
        let target = PreparedPlan::transient(&polygon);
        for (x, y, strict, inclusive) in [
            (3.0, 3.0, true, true),
            (0.0, 3.0, false, true),
            (1.5, 1.5, false, false),
            (9.0, 8.0, false, false),
        ] {
            let point = Shape::Point(Point::new_raw_xy(x, y));
            let point = PreparedPlan::transient(&point);
            for (predicate, swapped, expected) in [
                (Predicate::Contains, false, strict),
                (Predicate::ContainsProperly, false, strict),
                (Predicate::Within, true, strict),
                (Predicate::Covers, false, inclusive),
                (Predicate::CoveredBy, true, inclusive),
                (Predicate::Intersects, false, inclusive),
                (Predicate::Intersects, true, inclusive),
                (Predicate::Disjoint, false, !inclusive),
                (Predicate::Disjoint, true, !inclusive),
            ] {
                let pair = if swapped {
                    PreparedTopologyPair::cartesian(&point, &target)
                } else {
                    PreparedTopologyPair::cartesian(&target, &point)
                };
                PRE_TOPOLOGY_CALLS.with(|calls| calls.set(0));
                assert_eq!(
                    topology_scalar_pair(&predicate.spec(), &pair).unwrap(),
                    expected
                );
                assert_eq!(PRE_TOPOLOGY_CALLS.with(std::cell::Cell::get), 0);
            }
        }
        PRE_TOPOLOGY_CALLS.with(|calls| calls.set(0));
        topology_scalar_pair(
            &Predicate::Intersects.spec(),
            &PreparedTopologyPair::cartesian(&target, &target),
        )
        .unwrap();
        assert!(PRE_TOPOLOGY_CALLS.with(std::cell::Cell::get) > 0);
    }

    #[test]
    #[cfg(debug_assertions)]
    fn packed_point_batches_reuse_membership_without_generic_gates_or_source_scans() {
        let polygon = crate::io::parse_wkt(
            "POLYGON ((0 0,1 0,2 0,3 0,4 0,4 1,4 2,4 3,4 4,\
             3 4,2 4,1 4,0 4,0 3,0 2,0 1,0 0),\
             (1 1,2 1,2 2,1 2,1 1))",
        )
        .unwrap();
        let array = PyGeometryArray::from_shapes(
            [(9.0, 8.0), (3.0, 3.0), (0.0, 3.0), (1.5, 1.5), (2.0, 1.5)]
                .into_iter()
                .map(|(x, y)| Shape::Point(Point::new_raw_xy(x, y)))
                .collect(),
            Frame::None,
        )
        .unwrap();
        assert!(matches!(array.storage(), GeometryArrayStorage::Packed(_)));
        for (predicate, scalar_is_left, expected) in [
            (Predicate::Intersects, true, [
                false, true, true, false, true,
            ]),
            (Predicate::Disjoint, false, [
                true, false, false, true, false,
            ]),
            (Predicate::Contains, true, [
                false, true, false, false, false,
            ]),
            (Predicate::ContainsProperly, true, [
                false, true, false, false, false,
            ]),
            (Predicate::Within, false, [false, true, false, false, false]),
            (Predicate::Covers, true, [false, true, true, false, true]),
            (Predicate::CoveredBy, false, [
                false, true, true, false, true,
            ]),
        ] {
            let scalar = PreparedPlan::transient(&polygon);
            for _ in 0..2 {
                PRE_TOPOLOGY_CALLS.with(|calls| calls.set(0));
                let actual = scalar_vs_shapes(
                    &predicate.spec(),
                    &scalar,
                    array
                        .row_operation()
                        .rows_with_missing(|| ())
                        .map(|row| match row {
                            crate::array::ArrayRow::Missing { logical, .. } => (logical, None),
                            crate::array::ArrayRow::Present(row) => {
                                (row.logical(), Some(row.row()))
                            },
                        }),
                    scalar_is_left,
                    Some(&array),
                    TopologyDomain::Cartesian,
                    &crate::geometry::FrameDependentCaches::default(),
                )
                .unwrap();
                assert_eq!(actual, expected, "{predicate:?}");
                assert_eq!(
                    PRE_TOPOLOGY_CALLS.with(std::cell::Cell::get),
                    0,
                    "{predicate:?}"
                );
                assert_eq!(scalar.point_location_scans(), 0, "{predicate:?}");
            }
        }
    }

    #[test]
    fn exterior_scalar_points_do_not_warm_a_membership_index() {
        let mut coordinates = (0..512)
            .map(|i| {
                let angle = std::f64::consts::TAU * f64::from(i) / 512.0;
                Point::new_raw_xy(5.0 * angle.cos(), 5.0 * angle.sin())
            })
            .collect::<Vec<_>>();
        coordinates.push(coordinates[0]);
        let polygon = Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&coordinates).unwrap()).unwrap(),
            vec![],
        ));
        let target = PreparedPlan::transient(&polygon);
        target.bounds().unwrap();
        let outside_shape = Shape::Point(Point::new_raw_xy(9.0, 8.0));
        let outside = PreparedPlan::transient(&outside_shape);
        let spec = Predicate::Intersects.spec();
        let ((), allocations) = crate::test_support::allocation_count(|| {
            for _ in 0..8 {
                assert!(
                    !topology_scalar_pair(
                        &spec,
                        &PreparedTopologyPair::cartesian(&target, &outside)
                    )
                    .unwrap()
                );
            }
        });
        assert_eq!(allocations, 0);
        let inside_shape = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        let inside = PreparedPlan::transient(&inside_shape);
        let ((), control_allocations) = crate::test_support::allocation_count(|| {
            for _ in 0..8 {
                assert!(
                    topology_scalar_pair(&spec, &PreparedTopologyPair::cartesian(&target, &inside))
                        .unwrap()
                );
            }
        });
        assert!(control_allocations > 0);
    }

    fn degrees() -> crate::geographic::GeographicFrame {
        crate::crs::info("EPSG:4326")
            .unwrap()
            .geographic_frame()
            .unwrap()
    }

    #[expect(
        clippy::similar_names,
        reason = "bounds are named by axis and extreme, as in the public API"
    )]
    fn rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Shape {
        Shape::Polygon(Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(minx, miny),
                    Point::new_raw_xy(maxx, miny),
                    Point::new_raw_xy(maxx, maxy),
                    Point::new_raw_xy(minx, maxy),
                    Point::new_raw_xy(minx, miny),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        ))
    }

    fn line(start: (f64, f64), end: (f64, f64)) -> Shape {
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(vec![start.0, end.0], vec![start.1, end.1], None, None)
                    .unwrap(),
            )
            .expect("fixture line has two vertices"),
        )
    }

    #[derive(Clone, Copy)]
    struct FarApartSupportCase {
        name: &'static str,
        left: &'static str,
        right: &'static str,
        both_supports_certified_nonempty: bool,
        matrix: Option<&'static str>,
    }

    const REMOTE_RECTANGLE: &str = "POLYGON ((1000 1000,1004 1000,1004 1004,1000 1004,1000 1000))";

    const FAR_APART_SUPPORT_CASES: [FarApartSupportCase; 14] = [
        FarApartSupportCase {
            name: "spike",
            left: "POLYGON ((0 0,4 0,4 4,2 4,2 6,2 4,0 4,0 0))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: true,
            matrix: None,
        },
        FarApartSupportCase {
            name: "collapsed polygon",
            left: "POLYGON ((0 0,2 2,4 4,0 0))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: false,
            matrix: None,
        },
        FarApartSupportCase {
            name: "shell equals hole",
            left: "POLYGON ((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: false,
            matrix: None,
        },
        FarApartSupportCase {
            name: "zero-net bow-tie",
            left: "POLYGON ((0 0,4 4,0 4,4 0,0 0))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: false,
            matrix: None,
        },
        FarApartSupportCase {
            name: "twice-wound shell",
            left: "POLYGON ((0 0,4 0,4 4,0 4,0 0,4 0,4 4,0 4,0 0))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: true,
            matrix: None,
        },
        FarApartSupportCase {
            name: "external hole",
            left: "POLYGON ((0 0,4 0,4 4,0 4,0 0),(8 8,9 8,9 9,8 9,8 8))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: false,
            matrix: None,
        },
        FarApartSupportCase {
            name: "empty polygon",
            left: "POLYGON EMPTY",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: false,
            matrix: None,
        },
        FarApartSupportCase {
            name: "collapsed-only multipolygon",
            left: "MULTIPOLYGON (((0 0,2 2,4 4,0 0)))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: false,
            matrix: None,
        },
        FarApartSupportCase {
            name: "multipolygon with collapsed member",
            left: "MULTIPOLYGON (((0 0,2 2,4 4,0 0)),((6 6,9 6,9 9,6 9,6 6)))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: true,
            matrix: None,
        },
        FarApartSupportCase {
            name: "collection with collapsed polygon member",
            left: "GEOMETRYCOLLECTION (POLYGON ((0 0,2 2,4 4,0 0)),LINESTRING (6 6,7 6))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: true,
            matrix: None,
        },
        FarApartSupportCase {
            name: "two empty supports with raw bounds",
            left: "POLYGON ((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0))",
            right: "POLYGON ((1000 1000,1004 1000,1004 1004,1000 1004,1000 1000),(1000 1000,1004 1000,1004 1004,1000 1004,1000 1000))",
            both_supports_certified_nonempty: false,
            matrix: Some("FFFFFFFF2"),
        },
        FarApartSupportCase {
            name: "two typed empty polygons",
            left: "POLYGON EMPTY",
            right: "POLYGON EMPTY",
            both_supports_certified_nonempty: false,
            matrix: Some("FFFFFFFF2"),
        },
        FarApartSupportCase {
            name: "disjoint points",
            left: "POINT (0 0)",
            right: "POINT (1000 1000)",
            both_supports_certified_nonempty: true,
            matrix: None,
        },
        FarApartSupportCase {
            name: "disjoint lines",
            left: "LINESTRING (0 0,4 4)",
            right: "LINESTRING (1000 1000,1004 1004)",
            both_supports_certified_nonempty: true,
            matrix: None,
        },
    ];

    fn parse_wkt_shape(wkt: &str) -> Shape {
        crate::io::parse_wkt(wkt).expect("bounds-gate fixture WKT parses")
    }

    fn prepared_pair(left: &Shape, right: &Shape) -> (PreparedPlan, PreparedPlan) {
        (
            PreparedPlan::new(left.clone()),
            PreparedPlan::new(right.clone()),
        )
    }

    #[test]
    fn scalar_rows_relate_from_fixed_graphs_in_both_cartesian_orders() {
        let scalar = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0));
        let rows = [line((-1.0, 1.0), (3.0, 1.0)), line((3.0, 3.0), (4.0, 4.0))];
        let cache = crate::geometry::FrameDependentCaches::default();
        for scalar_is_left in [true, false] {
            let actual = scalar_vs_shapes(
                &Predicate::Intersects.spec(),
                &scalar,
                rows.iter()
                    .enumerate()
                    .map(|(index, shape)| (index, Some(ShapeRow::Shape(shape)))),
                scalar_is_left,
                None,
                TopologyDomain::Cartesian,
                &cache,
            )
            .expect("fixture admits");
            assert_eq!(actual, [true, false]);
        }
    }

    #[test]
    fn refuted_scalar_rows_do_not_admit_topology() {
        let rows = [line((10.0, 10.0), (11.0, 11.0))];
        let cartesian = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0));
        let cartesian_cache = crate::geometry::FrameDependentCaches::default();
        assert_eq!(
            scalar_vs_shapes(
                &Predicate::Intersects.spec(),
                &cartesian,
                rows.iter()
                    .enumerate()
                    .map(|(index, shape)| (index, Some(ShapeRow::Shape(shape)))),
                true,
                None,
                TopologyDomain::Cartesian,
                &cartesian_cache,
            )
            .expect("fixture admits"),
            [false]
        );
        let geographic = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0));
        let geographic_cache = crate::geometry::FrameDependentCaches::default();
        assert_eq!(
            scalar_vs_shapes(
                &Predicate::Intersects.spec(),
                &geographic,
                rows.iter()
                    .enumerate()
                    .map(|(index, shape)| (index, Some(ShapeRow::Shape(shape)))),
                true,
                None,
                TopologyDomain::Geographic(degrees()),
                &geographic_cache,
            )
            .expect("fixture admits"),
            [false]
        );
        assert_eq!(geographic.geographic_exact_topology_accesses(), 0);
    }

    #[test]
    fn admitted_scalar_rows_use_exact_membership() {
        let rows = [line((-1.0, 0.5), (3.0, 0.5)), line((-1.0, 1.5), (3.0, 1.5))];
        let cartesian = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0));
        let cartesian_cache = crate::geometry::FrameDependentCaches::default();
        assert_eq!(
            scalar_vs_shapes(
                &Predicate::Intersects.spec(),
                &cartesian,
                rows.iter()
                    .enumerate()
                    .map(|(index, shape)| (index, Some(ShapeRow::Shape(shape)))),
                true,
                None,
                TopologyDomain::Cartesian,
                &cartesian_cache,
            )
            .expect("fixture admits"),
            [true, true]
        );
        let line_scalar = PreparedPlan::new(line((-1.0, 1.0), (3.0, 1.0)));
        let polygon_rows = [rectangle(0.0, 0.0, 2.0, 2.0)];
        for scalar_is_left in [true, false] {
            assert_eq!(
                scalar_vs_shapes(
                    &Predicate::Intersects.spec(),
                    &line_scalar,
                    polygon_rows
                        .iter()
                        .enumerate()
                        .map(|(index, shape)| (index, Some(ShapeRow::Shape(shape)))),
                    scalar_is_left,
                    None,
                    TopologyDomain::Cartesian,
                    &crate::geometry::FrameDependentCaches::default(),
                )
                .expect("fixture admits"),
                [true]
            );
        }
        let geographic = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0));
        let geographic_cache = crate::geometry::FrameDependentCaches::default();
        assert_eq!(
            scalar_vs_shapes(
                &Predicate::Intersects.spec(),
                &geographic,
                rows.iter()
                    .enumerate()
                    .map(|(index, shape)| (index, Some(ShapeRow::Shape(shape)))),
                true,
                None,
                TopologyDomain::Geographic(degrees()),
                &geographic_cache,
            )
            .expect("fixture admits"),
            [true, true]
        );
    }

    #[test]
    fn geographic_line_scalar_does_not_enter_cartesian_direct_relation() {
        let scalar = PreparedPlan::new(line((175.0, 0.0), (-175.0, 0.0)));
        let polygon = Shape::Polygon(Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(170.0, -10.0),
                    Point::new_raw_xy(-170.0, -10.0),
                    Point::new_raw_xy(-170.0, 10.0),
                    Point::new_raw_xy(170.0, 10.0),
                    Point::new_raw_xy(170.0, -10.0),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        ));
        assert_eq!(
            scalar_vs_shapes(
                &Predicate::Within.spec(),
                &scalar,
                [(0, Some(ShapeRow::Shape(&polygon)))].into_iter(),
                true,
                None,
                TopologyDomain::Geographic(degrees()),
                &crate::geometry::FrameDependentCaches::default(),
            )
            .expect("fixture admits"),
            [true]
        );
    }

    #[test]
    fn scalar_rows_relate_prepared_array_rows_from_fixed_graphs() {
        let scalar = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0));
        let array = PyGeometryArray::mixed_shapes(
            vec![line((-1.0, 1.0), (3.0, 1.0)), line((3.0, 3.0), (4.0, 4.0))],
            Frame::None,
        )
        .unwrap();
        let cache = crate::geometry::FrameDependentCaches::default();
        let actual = scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &scalar,
            array
                .row_operation()
                .rows_with_missing(|| ())
                .map(|row| match row {
                    crate::array::ArrayRow::Missing { logical, .. } => (logical, None),
                    crate::array::ArrayRow::Present(row) => (row.logical(), Some(row.row())),
                }),
            false,
            Some(&array),
            TopologyDomain::Cartesian,
            &cache,
        )
        .expect("fixture admits");
        assert_eq!(actual, [true, false]);
    }

    /// The pre-topology gates are wired into the array, index and coverer
    /// lanes; the scalar lane went straight to the relation, so
    /// `gm.intersects(a, b)` on operands that do not overlap still built exact
    /// pair topology. The gate can only be an admission filter, never an
    /// answer change, so this asserts it against the relation it short-circuits
    /// — including the operands it does *not* gate.
    #[test]
    fn the_scalar_lane_gate_never_changes_an_answer() {
        let line = |points: Vec<Point>| {
            Shape::LineString(
                LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap())
                    .expect("two vertices"),
            )
        };
        let corpus = [
            // Strictly disjoint envelopes — every spelling is gated.
            (
                line(vec![
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(1.0, 1.0),
                ]),
                line(vec![
                    Point::new_raw_xy(10.0, 10.0),
                    Point::new_raw_xy(11.0, 11.0),
                ]),
            ),
            // Envelopes that merely touch: the gate is strict, so these reach
            // the relation and really do touch.
            (
                line(vec![
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(1.0, 0.0),
                ]),
                line(vec![
                    Point::new_raw_xy(1.0, 0.0),
                    Point::new_raw_xy(2.0, 0.0),
                ]),
            ),
            // Overlapping envelopes, crossing linework.
            (
                line(vec![
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(2.0, 2.0),
                ]),
                line(vec![
                    Point::new_raw_xy(0.0, 2.0),
                    Point::new_raw_xy(2.0, 0.0),
                ]),
            ),
            // Overlapping envelopes, no shared point.
            (
                line(vec![
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(2.0, 1.0),
                ]),
                line(vec![
                    Point::new_raw_xy(0.0, 2.0),
                    Point::new_raw_xy(2.0, 3.0),
                ]),
            ),
            // Areal containment, where the nesting gates apply.
            (
                rectangle(0.0, 0.0, 10.0, 10.0),
                rectangle(2.0, 2.0, 4.0, 4.0),
            ),
        ];
        for predicate in [
            Predicate::Intersects,
            Predicate::Disjoint,
            Predicate::Contains,
            Predicate::ContainsProperly,
            Predicate::Within,
            Predicate::Covers,
            Predicate::CoveredBy,
            Predicate::Equals,
            Predicate::Touches,
            Predicate::Crosses,
            Predicate::Overlaps,
        ] {
            let spec = predicate.spec();
            for (left, right) in &corpus {
                let left_plan = PreparedPlan::new(left.clone());
                let right_plan = PreparedPlan::new(right.clone());
                let pair = PreparedTopologyPair::Cartesian {
                    left: &left_plan,
                    right: &right_plan,
                };
                let relation = predicate_matrix_value(&spec, pair.relate().unwrap());
                let pair = PreparedTopologyPair::Cartesian {
                    left: &left_plan,
                    right: &right_plan,
                };
                assert_eq!(
                    topology_scalar_pair(&spec, &pair).unwrap(),
                    relation,
                    "{predicate:?} disagreed with its own relation",
                );
            }
        }
    }

    #[test]
    fn disjoint_support_refutations_match_partition_matrices() {
        for case in FAR_APART_SUPPORT_CASES {
            let left = parse_wkt_shape(case.left);
            let right = parse_wkt_shape(case.right);
            let (left_plan, right_plan) = prepared_pair(&left, &right);
            let pair = PreparedTopologyPair::cartesian(&left_plan, &right_plan);
            let matrix = pair.relate().expect("fixture relation succeeds");
            if let Some(expected) = case.matrix {
                assert_eq!(matrix.text(), expected, "{} matrix", case.name);
            }

            for predicate in Predicate::ALL {
                let spec = predicate.spec();
                let without_pre_topology_gate = topology_pair_after_gates(&spec, &pair)
                    .expect("ungated Cartesian predicate succeeds");
                let with_pre_topology_gate =
                    topology_scalar_pair(&spec, &pair).expect("Cartesian predicate succeeds");
                assert_eq!(
                    with_pre_topology_gate, without_pre_topology_gate,
                    "{}: pre-topology gate changed {predicate:?}",
                    case.name,
                );
                assert_eq!(
                    with_pre_topology_gate,
                    predicate_matrix_value(&spec, matrix),
                    "{}: {predicate:?} disagrees with the exact matrix",
                    case.name,
                );
            }
        }
    }

    #[test]
    fn disjoint_bounds_refute_intersections_and_only_certified_equality() {
        let intersections = [
            Predicate::Intersects,
            Predicate::Contains,
            Predicate::ContainsProperly,
            Predicate::Within,
            Predicate::Covers,
            Predicate::CoveredBy,
            Predicate::Touches,
            Predicate::Crosses,
            Predicate::Overlaps,
        ];
        for case in FAR_APART_SUPPORT_CASES {
            let left = parse_wkt_shape(case.left);
            let right = parse_wkt_shape(case.right);
            let (left_plan, right_plan) = prepared_pair(&left, &right);
            let (left_gate, right_gate) = (
                GateOperand::cartesian(ShapeRow::Shape(left_plan.shape()), left_plan.bounds()),
                GateOperand::cartesian(ShapeRow::Shape(right_plan.shape()), right_plan.bounds()),
            );
            let Some((left_bounds, right_bounds)) = left_gate.bounds().zip(right_gate.bounds())
            else {
                for predicate in Predicate::ALL {
                    assert_eq!(
                        predicate.spec().bounds_gate(&left_gate, &right_gate),
                        None,
                        "{} has no two bounds",
                        case.name,
                    );
                }
                continue;
            };
            assert!(
                !left_bounds.intersects(right_bounds),
                "{} fixture bounds must be disjoint",
                case.name
            );
            for predicate in intersections {
                assert_eq!(
                    predicate.spec().bounds_gate(&left_gate, &right_gate),
                    Some(false),
                    "{}: {predicate:?} requires shared support",
                    case.name,
                );
            }
            assert_eq!(
                Predicate::Disjoint
                    .spec()
                    .bounds_gate(&left_gate, &right_gate),
                Some(true),
                "{}",
                case.name,
            );
            assert_eq!(
                Predicate::Equals
                    .spec()
                    .bounds_gate(&left_gate, &right_gate),
                case.both_supports_certified_nonempty.then_some(false),
                "{}",
                case.name,
            );
        }
    }

    #[test]
    fn disjoint_contains_gate_skips_relation_evaluator_allocations() {
        let (left, right) = prepared_pair(
            &crate::io::parse_wkt("POLYGON ((0 0,10 0,2 8,0 0))").unwrap(),
            &crate::io::parse_wkt("POLYGON ((100 100,110 100,102 108,100 100))").unwrap(),
        );
        assert!(left.bounds().is_some());
        assert!(right.bounds().is_some());
        let pair = PreparedTopologyPair::cartesian(&left, &right);
        let spec = Predicate::Contains.spec();
        let (matrix, evaluator_allocations) =
            crate::test_support::allocation_count(|| pair.relate().expect("matrix succeeds"));
        assert!(
            evaluator_allocations > 0,
            "the general evaluator is the allocation control"
        );
        assert!(!predicate_matrix_value(&spec, matrix));

        let (answer, gate_allocations) = crate::test_support::allocation_count(|| {
            topology_scalar_pair(&spec, &pair).expect("Cartesian predicate succeeds")
        });
        assert!(!answer);
        assert_eq!(
            gate_allocations, 0,
            "disjoint bounds stop before witness evaluation"
        );
    }

    #[test]
    fn geographic_crossing_rows_fail_open_the_bounds_gate() {
        let query = PreparedPlan::new(rectangle(172.0, 42.0, 178.0, 48.0));
        let crossing = rectangle(170.0, 40.0, -170.0, 50.0);
        let cache = crate::geometry::FrameDependentCaches::default();
        let actual = scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &query,
            [(0, Some(ShapeRow::Shape(&crossing)))].into_iter(),
            true,
            None,
            TopologyDomain::Geographic(degrees()),
            &cache,
        )
        .expect("fixture admits");
        assert_eq!(actual, [true]);

        let crossing_contains_query = scalar_vs_shapes(
            &Predicate::Contains.spec(),
            &query,
            [(0, Some(ShapeRow::Shape(&crossing)))].into_iter(),
            false,
            None,
            TopologyDomain::Geographic(degrees()),
            &cache,
        )
        .expect("fixture admits");
        assert_eq!(crossing_contains_query, [true]);
    }

    #[test]
    fn prepared_intersects_rectangle_queries_use_exact_partition_and_respect_holes() {
        let outer = rectangle(0.0, 0.0, 10.0, 10.0);
        let Shape::Polygon(outer) = outer else {
            unreachable!("rectangle helper returns a polygon");
        };
        let hole = Ring::from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(3.0, 3.0),
                Point::new_raw_xy(7.0, 3.0),
                Point::new_raw_xy(7.0, 7.0),
                Point::new_raw_xy(3.0, 7.0),
                Point::new_raw_xy(3.0, 3.0),
            ])
            .unwrap(),
        )
        .unwrap();
        let scalar = PreparedPlan::new(Shape::Polygon(Polygon::new(outer.shell, vec![hole])));
        let cache = crate::geometry::FrameDependentCaches::default();
        let inside_hole = rectangle(4.0, 4.0, 6.0, 6.0);
        let inside_shell = rectangle(1.0, 1.0, 2.0, 2.0);
        let actual = scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &scalar,
            [
                (0, Some(ShapeRow::Shape(&inside_hole))),
                (1, Some(ShapeRow::Shape(&inside_shell))),
            ]
            .into_iter(),
            true,
            None,
            TopologyDomain::Geographic(degrees()),
            &cache,
        )
        .expect("rectangle queries admit");
        assert_eq!(actual, [false, true]);
        assert_eq!(scalar.geographic_exact_topology_builds(), 0);
        assert_eq!(scalar.geographic_exact_topology_accesses(), 0);
    }

    #[test]
    fn geographic_crossing_packed_rows_fail_open_the_bounds_gate() {
        let query = PreparedPlan::new(rectangle(172.0, 42.0, 178.0, 48.0));
        let coords = CoordSeq::try_from_points(&[
            Point::new_raw_xy(170.0, 40.0),
            Point::new_raw_xy(-170.0, 40.0),
            Point::new_raw_xy(-170.0, 50.0),
            Point::new_raw_xy(170.0, 50.0),
            Point::new_raw_xy(170.0, 40.0),
        ])
        .expect("test ring coordinates are valid");
        let ring_offsets =
            crate::geometry::CsrOffsetColumn::<crate::geometry::RingLevel>::try_new(vec![0, 5], 5)
                .expect("test ring offsets are valid");
        let row = ShapeRow::from_packed(crate::geometry::storage::PackedRow::polygon_window(
            0,
            &coords,
            &ring_offsets,
            0..1,
        ));
        let cache = crate::geometry::FrameDependentCaches::default();
        let actual = scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &query,
            [(0, Some(row))].into_iter(),
            true,
            None,
            TopologyDomain::Geographic(degrees()),
            &cache,
        )
        .expect("fixture admits");
        assert_eq!(actual, [true]);
    }

    #[test]
    fn geographic_scalar_rows_cover_sound_and_unsound_operands_in_both_orders() {
        let sound_scalar = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0));
        let sound_row = line((-1.0, 1.0), (3.0, 1.0));
        let unsound_scalar = PreparedPlan::new(rectangle(172.0, 42.0, 178.0, 48.0));
        let unsound_row = rectangle(170.0, 40.0, -170.0, 50.0);

        for (scalar, row) in [(&sound_scalar, &sound_row), (&unsound_scalar, &unsound_row)] {
            let cache = crate::geometry::FrameDependentCaches::default();
            for scalar_is_left in [true, false] {
                let actual = scalar_vs_shapes(
                    &Predicate::Intersects.spec(),
                    scalar,
                    [(0, Some(ShapeRow::Shape(row)))].into_iter(),
                    scalar_is_left,
                    None,
                    TopologyDomain::Geographic(degrees()),
                    &cache,
                )
                .expect("fixture admits");
                assert_eq!(actual, [true]);
            }
        }
    }

    #[test]
    fn geographic_scalar_rows_relate_prepared_unsound_array_rows() {
        let scalar = PreparedPlan::new(rectangle(172.0, 42.0, 178.0, 48.0));
        let array =
            PyGeometryArray::mixed_shapes(vec![rectangle(170.0, 40.0, -170.0, 50.0)], Frame::None)
                .unwrap();
        let cache = crate::geometry::FrameDependentCaches::default();
        let actual = scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &scalar,
            array
                .row_operation()
                .rows_with_missing(|| ())
                .map(|row| match row {
                    crate::array::ArrayRow::Missing { logical, .. } => (logical, None),
                    crate::array::ArrayRow::Present(row) => (row.logical(), Some(row.row())),
                }),
            false,
            Some(&array),
            TopologyDomain::Geographic(degrees()),
            &cache,
        )
        .expect("fixture admits");
        assert_eq!(actual, [true]);
    }

    #[test]
    fn native_relate_pattern_matches_disjoint_areal_literals() {
        let (a, b) = (rectangle(0.0, 0.0, 1.0, 1.0), rectangle(3.0, 3.0, 4.0, 4.0));
        for (literal, expected) in [
            (b'F', true),
            (b'T', false),
            (b'*', true),
            (b'0', false),
            (b'1', false),
            (b'2', false),
        ] {
            let mut cells = *b"*********";
            cells[0] = literal;
            let pattern = std::str::from_utf8(&cells).unwrap();
            assert_eq!(
                a.relate_pattern(&b, pattern).unwrap(),
                expected,
                "literal {}",
                literal as char
            );
        }
        assert!(a.relate_pattern(&b, "FF2FF1212").unwrap());
        a.relate_pattern(&b, "bad").unwrap_err();
    }

    #[test]
    fn geographic_invalid_latitude_is_not_a_bounds_false() {
        let query = PreparedPlan::new(rectangle(0.0, 0.0, 1.0, 1.0));
        let invalid = Shape::Point(Point::new_raw_xy(0.5, 91.0));
        let cache = crate::geometry::FrameDependentCaches::default();
        let error = scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &query,
            [(0, Some(ShapeRow::Shape(&invalid)))].into_iter(),
            true,
            None,
            TopologyDomain::Geographic(degrees()),
            &cache,
        )
        .unwrap_err();
        let message = error.1.to_string();
        assert!(
            message.contains("invalid longitude/latitude (0.5, 91)"),
            "{message}"
        );
    }

    fn cartesian_predicate(predicate: Predicate, left: &Shape, right: &Shape) -> bool {
        let left = PreparedPlan::transient(left);
        let right = PreparedPlan::transient(right);
        let pair = PreparedTopologyPair::cartesian(&left, &right);
        topology_scalar_pair(&predicate.spec(), &pair).expect("Cartesian predicate succeeds")
    }

    fn shared_collection(shape: Shape, depth: usize) -> Shape {
        let mut shared = shape;
        for _ in 0..depth {
            shared = Shape::geometry_collection(vec![shared.clone(), shared])
                .expect("shared collection depth is admitted");
        }
        shared
    }

    fn wkb(shape: &Shape) -> Vec<u8> {
        crate::io::to_wkb(shape, None, false).expect("predicate fixture serializes")
    }

    #[test]
    fn shared_boolean_point_predicates_match_unshared_wkb_expansion() {
        let cases = [
            (
                crate::io::parse_wkt("POLYGON ((0 0,2 0,2 2,0 2,0 0))").unwrap(),
                Point::new_raw_xy(1.0, 1.0),
            ),
            (
                crate::io::parse_wkt("LINESTRING (0 0,2 0)").unwrap(),
                Point::new_raw_xy(0.0, 0.0),
            ),
            (
                crate::io::parse_wkt("POINT (1 1)").unwrap(),
                Point::new_raw_xy(1.0, 1.0),
            ),
            (
                crate::io::parse_wkt(
                    "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))",
                )
                .unwrap(),
                Point::new_raw_xy(0.5, 0.5),
            ),
            (
                crate::io::parse_wkt(
                    "GEOMETRYCOLLECTION (POINT ZM (0 0 1 10), POINT ZM (0 0 2 20), POINT ZM (1 1 3 30))",
                )
                .unwrap(),
                Point::new_raw_xy(0.0, 0.0),
            ),
            (
                Shape::geometry_collection(vec![
                    crate::io::parse_wkt("POLYGON ((0 0,2 0,2 2,0 2,0 0))").unwrap(),
                    crate::io::parse_wkt("LINESTRING (-1 1,3 1)").unwrap(),
                    crate::io::parse_wkt("POINT (1 1)").unwrap(),
                ])
                .unwrap(),
                Point::new_raw_xy(1.0, 1.0),
            ),
        ];

        for (base, query) in cases {
            let point = Shape::Point(query);
            for depth in [6, 10] {
                let shared = shared_collection(base.clone(), depth);
                let expanded = crate::io::parse_wkb(&wkb(&shared))
                    .expect("expanded WKB parses")
                    .shape;

                for predicate in [
                    Predicate::Intersects,
                    Predicate::Disjoint,
                    Predicate::Covers,
                ] {
                    assert_eq!(
                        cartesian_predicate(predicate, &shared, &point),
                        cartesian_predicate(predicate, &expanded, &point),
                        "{predicate:?}, depth {depth}"
                    );
                }
                assert_eq!(
                    cartesian_predicate(Predicate::CoveredBy, &point, &shared),
                    cartesian_predicate(Predicate::CoveredBy, &point, &expanded),
                    "CoveredBy, depth {depth}"
                );
            }
        }
    }

    #[test]
    fn exact_location_predicates_keep_occurrence_boundary_parity() {
        let line = crate::io::parse_wkt("LINESTRING (0 0,1 0)").unwrap();
        let point = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        let shared = shared_collection(line, 6);
        let expanded = crate::io::parse_wkb(&wkb(&shared))
            .expect("expanded WKB parses")
            .shape;

        for predicate in [Predicate::Contains, Predicate::Touches] {
            assert_eq!(
                cartesian_predicate(predicate, &shared, &point),
                cartesian_predicate(predicate, &expanded, &point),
                "{predicate:?}"
            );
        }
    }

    #[test]
    fn depth_40_and_128_shared_point_intersects_complete() {
        let square = rectangle(0.0, 0.0, 1.0, 1.0);
        let point = Shape::Point(Point::new_raw_xy(0.5, 0.5));
        for depth in [40, 128] {
            let shared = shared_collection(square.clone(), depth);
            assert!(cartesian_predicate(Predicate::Intersects, &shared, &point));
        }
    }

    #[test]
    fn shared_collection_point_batches_do_not_require_expanded_coordinate_counts() {
        let line = crate::io::parse_wkt("LINESTRING (0 0,2 0)").unwrap();
        let shared = shared_collection(line, 64);
        let point = Point::new_raw_xy(1.0, 0.0);
        assert!(crate::geometry::cartesian_point_covers(&shared, point));
        let array =
            PyGeometryArray::from_shapes(vec![Shape::Point(point); 8], Frame::None).unwrap();
        let scalar = PreparedPlan::transient(&shared);
        let actual = scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &scalar,
            (0..8).map(|index| (index, Some(ShapeRow::Point(point)))),
            true,
            Some(&array),
            TopologyDomain::Cartesian,
            &crate::geometry::FrameDependentCaches::default(),
        )
        .unwrap();
        assert_eq!(actual, vec![true; 8]);
    }

    #[test]
    fn geographic_try_for_domain_rejects_invalid_right_latitude() {
        let left = PreparedPlan::new(rectangle(0.0, 0.0, 1.0, 1.0));
        let right = PreparedPlan::new(Shape::Point(Point::new_raw_xy(10.0, 91.0)));
        let left_cache = crate::geometry::FrameDependentCaches::default();
        let right_cache = crate::geometry::FrameDependentCaches::default();
        let Err(error) = PreparedTopologyPair::try_for_domain(
            &left,
            &left_cache,
            &right,
            &right_cache,
            TopologyDomain::Geographic(degrees()),
        ) else {
            panic!("invalid latitude must fail admission")
        };
        let message = error.to_string();
        assert!(
            message.contains("invalid longitude/latitude (10, 91)"),
            "{message}"
        );
    }
}
