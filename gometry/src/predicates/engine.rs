//! The one source of truth for boolean spatial predicates.
//!
//! Every surface that evaluates a named predicate — free functions, `Geometry`
//! and `GeometryArray` methods, `PreparedGeometry`, the spatial index refine
//! loop, and `join` — reads its facts from [`Predicate::spec`] and projects the
//! complete matrix produced by the graph relation owner. Batch and scalar
//! surfaces therefore cannot drift in predicate semantics.

#![allow(clippy::similar_names, reason = "bounds are named by axis and extreme")]
use std::cell::OnceCell;

use crate::geometry::{
    Bounds, CompiledPattern, GeographicPrepared, PreparedPlan, TopologyDomain, graph_relate_data,
    line_scalar_prefers_direct_polygon_relate,
};

#[derive(Clone, Copy)]
pub(crate) enum PreparedTopologyPair<'a> {
    Cartesian {
        left: &'a PreparedPlan,
        right: &'a PreparedPlan,
    },
    CartesianLeftGraph {
        left: &'a PreparedPlan,
        right: &'a PreparedPlan,
        left_graph: &'a crate::geometry::TopologyGraph,
    },
    CartesianRightGraph {
        left: &'a PreparedPlan,
        right: &'a PreparedPlan,
        right_graph: &'a crate::geometry::TopologyGraph,
    },
    Geographic {
        left: GeographicPrepared<'a>,
        right: GeographicPrepared<'a>,
    },
    GeographicLeftGraph {
        left: &'a PreparedPlan,
        right: GeographicPrepared<'a>,
        left_graph: &'a crate::geometry::TopologyGraph,
    },
    GeographicRightGraph {
        left: GeographicPrepared<'a>,
        right: &'a PreparedPlan,
        right_graph: &'a crate::geometry::TopologyGraph,
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

    pub(crate) fn try_geographic(
        left: &'a PreparedPlan,
        left_cache: &'a crate::geometry::FrameDependentCaches,
        right: &'a PreparedPlan,
        right_cache: &'a crate::geometry::FrameDependentCaches,
        frame: crate::geographic::GeographicFrame,
    ) -> crate::error::Result<Self> {
        Ok(Self::Geographic {
            left: GeographicPrepared::try_new(left, left_cache, frame)?,
            right: GeographicPrepared::try_new(right, right_cache, frame)?,
        })
    }

    /// The two operands as the pre-topology gates read them, when this pair
    /// still holds plain plans. A variant carrying an already-built graph has
    /// paid the cost the gates exist to avoid, so it goes straight to the
    /// relation.
    fn gate_operands(&self) -> Option<(GateOperand<'a>, GateOperand<'a>)> {
        match *self {
            Self::Cartesian { left, right } => Some((
                GateOperand::cartesian(ShapeRow::Shape(left.shape()), left.bounds()),
                GateOperand::cartesian(ShapeRow::Shape(right.shape()), right.bounds()),
            )),
            Self::Geographic { left, right } => Some((
                GateOperand::from_prepared(&left),
                GateOperand::from_prepared(&right),
            )),
            Self::CartesianLeftGraph { .. }
            | Self::CartesianRightGraph { .. }
            | Self::GeographicLeftGraph { .. }
            | Self::GeographicRightGraph { .. } => None,
        }
    }

    /// Both plans of a Cartesian pair, whichever graphs it carries.
    const fn cartesian_plans(&self) -> Option<(&'a PreparedPlan, &'a PreparedPlan)> {
        match *self {
            Self::Cartesian { left, right }
            | Self::CartesianLeftGraph { left, right, .. }
            | Self::CartesianRightGraph { left, right, .. } => Some((left, right)),
            Self::Geographic { .. }
            | Self::GeographicLeftGraph { .. }
            | Self::GeographicRightGraph { .. } => None,
        }
    }

    pub(crate) fn relate(self) -> crate::error::Result<crate::geometry::De9im> {
        match self {
            Self::Cartesian { left, right } => graph_relate_data(left, right),
            Self::CartesianLeftGraph {
                left,
                right,
                left_graph,
            } => crate::geometry::graph_relate_data_with_left_graph(left, left_graph, right),
            Self::CartesianRightGraph {
                left,
                right,
                right_graph,
            } => crate::geometry::graph_relate_data_with_right_graph(left, right, right_graph),
            Self::Geographic { left, right } => {
                crate::geometry::graph_relate_data_geographic(left, right)
            },
            Self::GeographicLeftGraph {
                left,
                right,
                left_graph,
            } => crate::geometry::graph_relate_data_geographic_with_left_graph(
                left, left_graph, right,
            ),
            Self::GeographicRightGraph {
                left,
                right,
                right_graph,
            } => crate::geometry::graph_relate_data_geographic_with_right_graph(
                left,
                right,
                right_graph,
            ),
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

    /// Compile-time kind×kind refutations for Crosses and Overlaps.
    ///
    /// Keys off **declared** [`GeometryKind`] (`Point` vs `MultiPoint`), never
    /// topological dimension alone — a MultiPoint can cross an area while a
    /// single Point never can. `GeometryCollection` on either side returns
    /// `None` (fall through). All non-refuted pairs, including empties and
    /// equality, use the matrix path.
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
            index_envelope,
        }
    }
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

    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
    )]
    /// Every answer available before either operand is prepared.
    ///
    /// Ordered by what each stage has to touch: declared kinds refute without
    /// reading a coordinate, bounds refute without topology, and two
    /// rectangles that meet settle `intersects` outright. `None` hands the
    /// pair to the exact kernel.
    pub(crate) fn pre_topology(
        &self,
        left: GateOperand<'_>,
        right: GateOperand<'_>,
    ) -> crate::error::Result<Option<bool>> {
        if let Some(verdict) = self.predicate.kind_gate(left.kind, right.kind) {
            return Ok(Some(verdict));
        }
        if !left.bounds_sound || !right.bounds_sound {
            return Ok(None);
        }
        let (Some(left_bounds), Some(right_bounds)) = (left.bounds, right.bounds) else {
            return Ok(None);
        };
        if let Some(verdict) = self.bounds_gate(left_bounds, right_bounds) {
            return Ok(Some(verdict));
        }
        self.rectangles_meet(left, left_bounds, right, right_bounds)
    }

    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
    )]
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
        left: GateOperand<'_>,
        left_bounds: Bounds,
        right: GateOperand<'_>,
        right_bounds: Bounds,
    ) -> crate::error::Result<Option<bool>> {
        let meeting = match self.predicate {
            Predicate::Intersects => true,
            Predicate::Disjoint => false,
            _ => return Ok(None),
        };
        let rectangle = |operand: GateOperand<'_>, bounds: Bounds| {
            operand
                .row
                .with_shape(|shape| crate::geometry::is_axis_aligned_rectangle(shape, bounds))
        };
        Ok((rectangle(left, left_bounds)? && rectangle(right, right_bounds)?).then_some(meeting))
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
}

impl<'a> GateOperand<'a> {
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
        }
    }

    /// Geographic gate facts derived from one admitted operand. There is no
    /// geographic operand except from a live [`GeographicPrepared`].
    pub(crate) fn from_prepared(prepared: &GeographicPrepared<'a>) -> Self {
        let plan = prepared.plan();
        let row = ShapeRow::Shape(plan.shape());
        let (kind, _) = row_kind_empty(row);
        let bounds = plan.bounds();
        let bounds_sound = bounds.is_some_and(|bounds| {
            crate::geometry::admitted_bounds_are_sound(prepared.view(), bounds)
        });
        Self {
            row,
            kind,
            bounds,
            bounds_sound,
        }
    }

    /// Whether this operand's coordinate box means what it says, so a caller
    /// may compare it as a plain rectangle. False across the seam, at a pole,
    /// and at a longitude the frame would move.
    pub(crate) const fn bounds_are_sound(&self) -> bool {
        self.bounds_sound
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
#[expect(
    clippy::too_many_lines,
    reason = "cohesive kernel; splitting obscures the algorithm"
)]
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
    let element_bounds = array.and_then(PyGeometryArray::cached_element_bounds);
    let row_is_missing = |index: usize| array.is_some_and(|array| array.is_row_missing(index));
    let scalar_cartesian_graph = OnceCell::new();
    let mut row_operation = array.map(PyGeometryArray::row_operation);
    match domain {
        TopologyDomain::Cartesian => {
            let scalar_operand =
                GateOperand::cartesian(ShapeRow::Shape(scalar.shape()), scalar_bounds);
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
                if let Some(verdict) = spec.pre_topology(left, right)? {
                    return Ok(Some(verdict));
                }
                if spec.predicate != Predicate::Intersects
                    || !left.bounds_are_sound()
                    || !right.bounds_are_sound()
                {
                    return Ok(None);
                }
                if row.with_shape(|shape| {
                    line_scalar_prefers_direct_polygon_relate(scalar.shape(), shape)
                })? {
                    return Ok(None);
                }
                row.try_with_shape(|shape| {
                    let Some(bounds) = shape.bounds() else {
                        return Ok(None);
                    };
                    if !crate::geometry::is_axis_aligned_rectangle(shape, bounds) {
                        return Ok(None);
                    }
                    let scalar_graph =
                        scalar_cartesian_graph.get_or_try_init(|| scalar.topology_graph())?;
                    Ok(Some(crate::geometry::graph_intersects_bounds(
                        scalar_graph,
                        bounds,
                    )))
                })
            };
            elements
                .map(|(index, row)| {
                    let mut evaluate = || -> crate::error::Result<bool> {
                        if row_is_missing(index) {
                            return Ok(false);
                        }
                        let Some(row) = row else { return Ok(false) };
                        if let Some(verdict) = gate(index, row)? {
                            return Ok(verdict);
                        }
                        let element = row_operation.as_mut().map_or_else(
                            || crate::array::PreparedRow::transient(row),
                            |operation| operation.prepared_at(index, row),
                        )?;
                        if line_scalar_prefers_direct_polygon_relate(
                            scalar.shape(),
                            element.shape(),
                        ) {
                            let matrix = if scalar_is_left {
                                graph_relate_data(scalar, &element)
                            } else {
                                graph_relate_data(&element, scalar)
                            }?;
                            return Ok(predicate_matrix_value(spec, matrix));
                        }
                        let scalar_graph =
                            scalar_cartesian_graph.get_or_try_init(|| scalar.topology_graph())?;
                        let operands = if scalar_is_left {
                            PreparedTopologyPair::CartesianLeftGraph {
                                left: scalar,
                                right: &element,
                                left_graph: scalar_graph,
                            }
                        } else {
                            PreparedTopologyPair::CartesianRightGraph {
                                left: &element,
                                right: scalar,
                                right_graph: scalar_graph,
                            }
                        };
                        topology_scalar_pair(spec, operands)
                    };
                    evaluate().map_err(|error| (index, error))
                })
                .collect()
        },
        TopologyDomain::Geographic(frame) => {
            let (scalar_kind, _) = row_kind_empty(ShapeRow::Shape(scalar.shape()));
            let scalar_prepared = OnceCell::new();
            let scalar_geographic_graph = OnceCell::new();
            elements
                .map(|(index, row)| {
                    if row_is_missing(index) {
                        return Ok(false);
                    }
                    let Some(row) = row else {
                        return Ok(false);
                    };
                    let (row_kind, _) = row_kind_empty(row);
                    let (left_kind, right_kind) = if scalar_is_left {
                        (scalar_kind, row_kind)
                    } else {
                        (row_kind, scalar_kind)
                    };
                    if let Some(verdict) = spec.predicate.kind_gate(left_kind, right_kind) {
                        return Ok(verdict);
                    }
                    let element = row_operation
                        .as_mut()
                        .map_or_else(
                            || crate::array::PreparedRow::transient(row),
                            |operation| operation.prepared_at(index, row),
                        )
                        .map_err(|error| (index, error))?;
                    let transient_cache = crate::geometry::FrameDependentCaches::default();
                    let row_cache = row_operation
                        .as_mut()
                        .map(|operation| operation.frame_cache(index))
                        .transpose()
                        .map_err(|error| (index, error))?;
                    let element_cache = row_cache.as_deref().unwrap_or(&transient_cache);
                    let element_prepared =
                        GeographicPrepared::try_new(&element, element_cache, frame)
                            .map_err(|error| (index, error))?;
                    let prepared = *scalar_prepared
                        .get_or_try_init(|| {
                            GeographicPrepared::try_new(scalar, scalar_cache, frame)
                        })
                        .map_err(|error| (index, error))?;
                    let scalar_operand = GateOperand::from_prepared(&prepared);
                    let row_operand = GateOperand::from_prepared(&element_prepared);
                    let (left, right) = if scalar_is_left {
                        (scalar_operand, row_operand)
                    } else {
                        (row_operand, scalar_operand)
                    };
                    if let Some(verdict) = spec
                        .pre_topology(left, right)
                        .map_err(|error| (index, error))?
                    {
                        return Ok(verdict);
                    }
                    if spec.predicate == Predicate::Intersects
                        && left.bounds_are_sound()
                        && right.bounds_are_sound()
                    {
                        let rectangle = element_prepared.plan().shape();
                        if let Some(bounds) = rectangle.bounds()
                            && crate::geometry::is_axis_aligned_rectangle(rectangle, bounds)
                        {
                            let scalar_graph = scalar_geographic_graph
                                .get_or_try_init(|| prepared.graph())
                                .map_err(|error| (index, error))?
                                .as_ref();
                            return Ok(crate::geometry::graph_intersects_bounds(
                                scalar_graph,
                                bounds,
                            ));
                        }
                    }
                    let scalar_graph = scalar_geographic_graph
                        .get_or_try_init(|| prepared.graph())
                        .map_err(|error| (index, error))?
                        .as_ref();
                    let operands = if scalar_is_left {
                        PreparedTopologyPair::GeographicLeftGraph {
                            left: scalar,
                            right: element_prepared,
                            left_graph: scalar_graph,
                        }
                    } else {
                        PreparedTopologyPair::GeographicRightGraph {
                            left: element_prepared,
                            right: scalar,
                            right_graph: scalar_graph,
                        }
                    };
                    topology_scalar_pair(spec, operands).map_err(|error| (index, error))
                })
                .collect()
        },
    }
}

/// Declared kind + emptiness for one storage row (no materialize for packed).
pub(crate) fn row_kind_empty(row: ShapeRow<'_>) -> (GeometryKind, bool) {
    match row {
        ShapeRow::Point(_) => (GeometryKind::Point, false),
        ShapeRow::Line(_, start, end) => (GeometryKind::LineString, start == end),
        ShapeRow::Rings(_, _, start, end) => (GeometryKind::Polygon, start == end),
        ShapeRow::Handle(handle) => (GeometryKind::of(handle.shape()), handle.shape().is_empty()),
        ShapeRow::Shape(shape) => (GeometryKind::of(shape), shape.is_empty()),
    }
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
pub(crate) fn topology_scalar_pair(
    spec: &PredicateSpec,
    operands: PreparedTopologyPair<'_>,
) -> crate::error::Result<bool> {
    // The array, index and coverer paths all run the pre-topology gates before
    // admitting a pair to the relation; the scalar pair was the one lane that
    // did not, so `gm.intersects(a, b)` on two operands a hundred units apart
    // still built both topology graphs and paired their edges to discover
    // they share no point.
    if let Some((left, right)) = operands.gate_operands()
        && let Some(verdict) = spec.pre_topology(left, right)?
    {
        return Ok(verdict);
    }
    // The direct engines need the plans, not a graph: a pair that already
    // carries the scalar's graph (the scalar-vs-array lane) is admitted the
    // same way, and the graph stays for what they decline.
    if let Some((left, right)) = operands.cartesian_plans()
        && let Some(verdict) = crate::geometry::direct_linework_predicate(spec, left, right)
            .or_else(|| crate::geometry::direct_areal_predicate(spec, left, right))
            .or_else(|| crate::geometry::direct_areal_linework_predicate(spec, left, right))
    {
        return Ok(verdict);
    }
    Ok(predicate_matrix_value(spec, operands.relate()?))
}

/// A DE-9IM pattern over a scalar pair, decided as early as the evidence
/// allows: sound disjoint bounds settle every cell the bounds prove (a
/// curve's boundary cells stay open, since whether a closed line has a
/// boundary is a staging fact), then the direct engines answer the pattern
/// three-valued, and only a pair they decline builds the matrix.
pub(crate) fn topology_scalar_pattern(
    pattern: CompiledPattern,
    operands: &PreparedTopologyPair<'_>,
) -> crate::error::Result<bool> {
    if let Some((left, right)) = operands.gate_operands()
        && let Some(verdict) = pattern_bounds_gate(pattern, &left, &right)?
    {
        return Ok(verdict);
    }
    if let Some((left, right)) = operands.cartesian_plans()
        && let Some(verdict) = crate::geometry::direct_linework_pattern(pattern, left, right)
            .or_else(|| crate::geometry::direct_areal_pattern(pattern, left, right))
            .or_else(|| crate::geometry::direct_areal_linework_pattern(pattern, left, right))
    {
        return Ok(verdict);
    }
    Ok(operands.relate()?.matches(pattern))
}

/// What sound, disjoint bounds prove about a pattern: the four inner cells
/// are empty and each operand lies wholly in the other's exterior. The
/// interval leaves a curve's boundary cells open, so a pattern that reads
/// them stays undecided here rather than guessed.
fn pattern_bounds_gate(
    pattern: CompiledPattern,
    left: &GateOperand<'_>,
    right: &GateOperand<'_>,
) -> crate::error::Result<Option<bool>> {
    use crate::geometry::{De9imInterval, Dimension, Loc, OperandShape, dimension};
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
                    PreparedPlan::transient(left).unwrap(),
                    PreparedPlan::transient(right).unwrap(),
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
    }

    use super::*;
    use crate::boundary::Frame;
    use crate::geometry::{CoordSeq, LineSeq, Point, Polygon, PreparedPlan, Ring, Shape};

    fn degrees() -> crate::geographic::GeographicFrame {
        crate::crs::info("EPSG:4326")
            .unwrap()
            .geographic_frame()
            .unwrap()
    }

    fn rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Shape {
        Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(minx, miny),
                        Point::new_raw_xy(maxx, miny),
                        Point::new_raw_xy(maxx, maxy),
                        Point::new_raw_xy(minx, maxy),
                        Point::new_raw_xy(minx, miny),
                    ])
                    .unwrap(),
                ),
                Vec::new(),
            )
            .expect("test polygon is valid"),
        )
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

    #[test]
    fn scalar_rows_relate_from_fixed_graphs_in_both_cartesian_orders() {
        let scalar = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0)).unwrap();
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
        let cartesian = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0)).unwrap();
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
        assert_eq!(cartesian.topology_graph_accesses(), 0);

        let geographic = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0)).unwrap();
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
        assert_eq!(geographic_cache.geographic_topology_accesses(), 0);
    }

    #[test]
    fn admitted_scalar_rows_reuse_one_query_graph() {
        let rows = [line((-1.0, 0.5), (3.0, 0.5)), line((-1.0, 1.5), (3.0, 1.5))];
        let cartesian = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0)).unwrap();
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
        assert_eq!(cartesian.topology_graph_accesses(), 1);

        let line_scalar = PreparedPlan::new(line((-1.0, 1.0), (3.0, 1.0))).unwrap();
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
        assert_eq!(line_scalar.topology_graph_accesses(), 0);

        let geographic = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0)).unwrap();
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
        assert_eq!(geographic_cache.geographic_topology_accesses(), 1);
    }

    #[test]
    fn geographic_line_scalar_does_not_enter_cartesian_direct_relation() {
        let scalar = PreparedPlan::new(line((175.0, 0.0), (-175.0, 0.0))).unwrap();
        let polygon = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(170.0, -10.0),
                        Point::new_raw_xy(-170.0, -10.0),
                        Point::new_raw_xy(-170.0, 10.0),
                        Point::new_raw_xy(170.0, 10.0),
                        Point::new_raw_xy(170.0, -10.0),
                    ])
                    .unwrap(),
                ),
                Vec::new(),
            )
            .expect("test polygon is valid"),
        );
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
        let scalar = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0)).unwrap();
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
    /// `gm.intersects(a, b)` on operands that do not overlap still built two
    /// topology graphs. The gate can only be an admission filter, never an
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
                let left_plan = PreparedPlan::new(left.clone()).unwrap();
                let right_plan = PreparedPlan::new(right.clone()).unwrap();
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
                    topology_scalar_pair(&spec, pair).unwrap(),
                    relation,
                    "{predicate:?} disagreed with its own relation",
                );
            }
        }
    }

    #[test]
    fn geographic_crossing_rows_fail_open_the_bounds_gate() {
        let query = PreparedPlan::new(rectangle(172.0, 42.0, 178.0, 48.0)).unwrap();
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
    }

    #[test]
    fn geographic_crossing_packed_rows_fail_open_the_bounds_gate() {
        let query = PreparedPlan::new(rectangle(172.0, 42.0, 178.0, 48.0)).unwrap();
        let coords = CoordSeq::try_from_points(&[
            Point::new_raw_xy(170.0, 40.0),
            Point::new_raw_xy(-170.0, 40.0),
            Point::new_raw_xy(-170.0, 50.0),
            Point::new_raw_xy(170.0, 50.0),
            Point::new_raw_xy(170.0, 40.0),
        ])
        .expect("test ring coordinates are valid");
        let offsets = [0, 5];
        let cache = crate::geometry::FrameDependentCaches::default();
        let actual = scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &query,
            [(0, Some(ShapeRow::Rings(&coords, &offsets, 0, 1)))].into_iter(),
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
        let sound_scalar = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0)).unwrap();
        let sound_row = line((-1.0, 1.0), (3.0, 1.0));
        let unsound_scalar = PreparedPlan::new(rectangle(172.0, 42.0, 178.0, 48.0)).unwrap();
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
        let scalar = PreparedPlan::new(rectangle(172.0, 42.0, 178.0, 48.0)).unwrap();
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
        assert_eq!(a.relate_pattern(&b, "FF2FF1212").unwrap(), true);
        assert!(a.relate_pattern(&b, "bad").is_err());
    }

    #[test]
    fn geographic_nested_admission_allocation_is_resource() {
        let nested = (0..5).fold(line((0.0, 0.0), (1.0, 0.0)), |shape, _| {
            Shape::geometry_collection(vec![shape]).expect("test collection is valid")
        });
        let nested = PreparedPlan::new(nested).unwrap();
        let other = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0)).unwrap();
        let nested_cache = crate::geometry::FrameDependentCaches::default();
        let other_cache = crate::geometry::FrameDependentCaches::default();
        let frame = degrees();
        let error = crate::test_support::fail_allocations_after(0, || {
            match PreparedTopologyPair::try_for_domain(
                &nested,
                &nested_cache,
                &other,
                &other_cache,
                TopologyDomain::Geographic(frame),
            ) {
                Ok(_) => panic!("nested path allocation must fail admission"),
                Err(error) => error,
            }
        });
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::Allocation)
        ));
    }

    #[test]
    fn geographic_invalid_latitude_is_not_a_bounds_false() {
        let query = PreparedPlan::new(rectangle(0.0, 0.0, 1.0, 1.0)).unwrap();
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

    #[test]
    fn geographic_try_for_domain_rejects_invalid_right_latitude() {
        let left = PreparedPlan::new(rectangle(0.0, 0.0, 1.0, 1.0)).unwrap();
        let right = PreparedPlan::new(Shape::Point(Point::new_raw_xy(10.0, 91.0))).unwrap();
        let left_cache = crate::geometry::FrameDependentCaches::default();
        let right_cache = crate::geometry::FrameDependentCaches::default();
        let error = match PreparedTopologyPair::try_for_domain(
            &left,
            &left_cache,
            &right,
            &right_cache,
            TopologyDomain::Geographic(degrees()),
        ) {
            Ok(_) => panic!("invalid latitude must fail admission"),
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(
            message.contains("invalid longitude/latitude (10, 91)"),
            "{message}"
        );
    }
}
