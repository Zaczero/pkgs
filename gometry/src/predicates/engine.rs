//! The one source of truth for boolean spatial predicates.
//!
//! Every surface that evaluates a named predicate — free functions, `Geometry`
//! and `GeometryArray` methods, `PreparedGeometry`, the spatial index refine
//! loop, and `join` — reads its facts from [`Predicate::spec`] and projects the
//! complete matrix produced by the graph relation owner. Batch and scalar
//! surfaces therefore cannot drift in predicate semantics.

use std::cell::OnceCell;

use crate::geometry::{
    Bounds, GeographicPrepared, PreparedPlan, TopologyDomain, graph_relate_data,
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
    pub(crate) fn for_domain(
        left: &'a PreparedPlan,
        left_cache: &'a crate::geometry::FrameDependentCaches,
        right: &'a PreparedPlan,
        right_cache: &'a crate::geometry::FrameDependentCaches,
        domain: TopologyDomain,
    ) -> Self {
        match domain {
            TopologyDomain::Cartesian => Self::Cartesian { left, right },
            TopologyDomain::Geographic(frame) => Self::Geographic {
                left: GeographicPrepared::new(left, left_cache, frame),
                right: GeographicPrepared::new(right, right_cache, frame),
            },
        }
    }

    /// The two operands as the pre-topology gates read them, when this pair
    /// still holds plain plans. A variant carrying an already-built graph has
    /// paid the cost the gates exist to avoid, so it goes straight to the
    /// relation.
    fn gate_operands(&self) -> Option<(GateOperand<'a>, GateOperand<'a>)> {
        let (left, right, domain) = match *self {
            Self::Cartesian { left, right } => (left, right, TopologyDomain::Cartesian),
            Self::Geographic { left, right } => (
                left.plan(),
                right.plan(),
                TopologyDomain::Geographic(left.frame()),
            ),
            Self::CartesianLeftGraph { .. }
            | Self::CartesianRightGraph { .. }
            | Self::GeographicLeftGraph { .. }
            | Self::GeographicRightGraph { .. } => return None,
        };
        Some((
            GateOperand::new(ShapeRow::Shape(left.shape()), left.bounds(), domain),
            GateOperand::new(ShapeRow::Shape(right.shape()), right.bounds(), domain),
        ))
    }

    /// Both plans of a Cartesian pair, whichever graphs it carries.
    fn cartesian_plans(&self) -> Option<(&'a PreparedPlan, &'a PreparedPlan)> {
        match *self {
            Self::Cartesian { left, right }
            | Self::CartesianLeftGraph { left, right, .. }
            | Self::CartesianRightGraph { left, right, .. } => Some((left, right)),
            Self::Geographic { .. }
            | Self::GeographicLeftGraph { .. }
            | Self::GeographicRightGraph { .. } => None,
        }
    }

    pub(crate) fn relate(self) -> crate::geometry::De9im {
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
    pub(crate) fn token(self) -> &'static str {
        self.spec().token
    }

    /// The dispatch facts for this predicate — the only place they live.
    pub(crate) fn spec(self) -> PredicateSpec {
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

    fn symmetric_spec(
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

    fn directed_spec(self, token: &'static str) -> PredicateSpec {
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
    ) -> Option<bool> {
        if let Some(verdict) = self.predicate.kind_gate(left.kind, right.kind) {
            return Some(verdict);
        }
        if !left.bounds_sound || !right.bounds_sound {
            return None;
        }
        let (Some(left_bounds), Some(right_bounds)) = (left.bounds, right.bounds) else {
            return None;
        };
        if let Some(verdict) = self.bounds_gate(left_bounds, right_bounds) {
            return Some(verdict);
        }
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
        left: GateOperand<'_>,
        left_bounds: Bounds,
        right: GateOperand<'_>,
        right_bounds: Bounds,
    ) -> Option<bool> {
        let meeting = match self.predicate {
            Predicate::Intersects => true,
            Predicate::Disjoint => false,
            _ => return None,
        };
        let rectangle = |operand: GateOperand<'_>, bounds: Bounds| {
            operand
                .row
                .with_shape(|shape| crate::geometry::is_axis_aligned_rectangle(shape, bounds))
        };
        (rectangle(left, left_bounds) && rectangle(right, right_bounds)).then_some(meeting)
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
    /// Read the gate facts off `row`, preferring `bounds` when the caller
    /// already holds them cached.
    pub(crate) fn new(row: ShapeRow<'a>, bounds: Option<Bounds>, domain: TopologyDomain) -> Self {
        let (kind, _) = row_kind_empty(row);
        let bounds = bounds.or_else(|| row.quick_bounds());
        // An axis-aligned box is a lie wherever longitude is a spelling rather
        // than a position: across the seam, at a pole, and at a coordinate the
        // frame would canonicalize. Those operands must reach the
        // quotient-aware graph.
        let bounds_sound = match domain {
            TopologyDomain::Cartesian => true,
            TopologyDomain::Geographic(frame) => bounds.is_some_and(|bounds| {
                row.with_shape(|shape| {
                    crate::geometry::raw_coordinate_bounds_are_sound_in_frame(
                        Some(shape),
                        bounds,
                        frame,
                    )
                })
            }),
        };
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
) -> Vec<bool> {
    let scalar_orig = scalar;
    let scalar_bounds = scalar.bounds();
    let element_bounds = array.and_then(PyGeometryArray::cached_element_bounds);
    let row_is_missing = |index: usize| array.is_some_and(|array| array.is_row_missing(index));
    // The scalar operand is fixed for the whole call, so its gate facts are
    // read once here and every row reads only its own.
    let scalar_operand =
        GateOperand::new(ShapeRow::Shape(scalar_orig.shape()), scalar_bounds, domain);
    // Admit topology only when a certificate or exact pair reaches it. Once
    // admitted, the fixed scalar graph remains query-local and shared by rows.
    let scalar_cartesian_graph = OnceCell::new();
    let scalar_geographic_graph = OnceCell::new();
    let mut row_operation = array.map(PyGeometryArray::row_operation);
    let gate = |index: usize, row: ShapeRow<'_>| -> Option<bool> {
        let row_operand = GateOperand::new(
            row,
            element_bounds
                .as_ref()
                .and_then(|bounds| bounds.get(index).copied().flatten()),
            domain,
        );
        let (left, right) = if scalar_is_left {
            (scalar_operand, row_operand)
        } else {
            (row_operand, scalar_operand)
        };
        spec.pre_topology(left, right).or_else(|| {
            // The graph-backed certificate reads one operand as a raw box and
            // answers it against the other's charted graph, so it rests on the
            // same premise the gates do and needs it of both sides: a box is a
            // rectangle only where the frame would not move its longitudes.
            if spec.predicate != Predicate::Intersects
                || !left.bounds_are_sound()
                || !right.bounds_are_sound()
            {
                return None;
            }
            if domain == TopologyDomain::Cartesian
                && row.with_shape(|shape| {
                    line_scalar_prefers_direct_polygon_relate(scalar.shape(), shape)
                })
            {
                return None;
            }
            row.with_shape(|shape| {
                let bounds = shape.bounds()?;
                if !crate::geometry::is_axis_aligned_rectangle(shape, bounds) {
                    return None;
                }
                let scalar_graph = match domain {
                    TopologyDomain::Cartesian => {
                        scalar_cartesian_graph.get_or_init(|| scalar.topology_graph())
                    },
                    TopologyDomain::Geographic(frame) => scalar_geographic_graph
                        .get_or_init(|| scalar_cache.geographic_topology_graph(scalar, frame))
                        .as_ref(),
                };
                crate::geometry::graph_intersects_rectangle_data(
                    scalar_orig,
                    Some(scalar_graph),
                    match domain {
                        TopologyDomain::Cartesian => None,
                        TopologyDomain::Geographic(frame) => {
                            Some(GeographicPrepared::new(scalar_orig, scalar_cache, frame))
                        },
                    },
                    shape,
                )
            })
        })
    };
    elements
        .map(|(index, row)| {
            if row_is_missing(index) || row.is_none() {
                false
            } else {
                let row = row.expect("present logical row carries storage");
                gate(index, row).unwrap_or_else(|| {
                    let element = row_operation.as_mut().map_or_else(
                        || crate::array::PreparedRow::transient(row),
                        |operation| operation.prepared_at(index, row),
                    );
                    let transient_cache = crate::geometry::FrameDependentCaches::default();
                    let row_cache = row_operation
                        .as_mut()
                        .map(|operation| operation.frame_cache(index));
                    let element_cache = row_cache.as_deref().unwrap_or(&transient_cache);
                    // A line scalar has no reusable support for the direct
                    // polygon/line fold. Select that fold before resolving the
                    // scalar graph, so this path never constructs a line graph
                    // only to transpose the polygon-side result.
                    if domain == TopologyDomain::Cartesian
                        && line_scalar_prefers_direct_polygon_relate(
                            scalar.shape(),
                            element.shape(),
                        )
                    {
                        let matrix = if scalar_is_left {
                            graph_relate_data(scalar, &element)
                        } else {
                            graph_relate_data(&element, scalar)
                        };
                        return predicate_matrix_value(spec, matrix);
                    }
                    match domain {
                        TopologyDomain::Cartesian => {
                            let scalar_graph =
                                scalar_cartesian_graph.get_or_init(|| scalar.topology_graph());
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
                        },
                        TopologyDomain::Geographic(frame) => {
                            let scalar_graph = scalar_geographic_graph
                                .get_or_init(|| {
                                    scalar_cache.geographic_topology_graph(scalar, frame)
                                })
                                .as_ref();
                            let operands = if scalar_is_left {
                                PreparedTopologyPair::GeographicLeftGraph {
                                    left: scalar,
                                    right: GeographicPrepared::new(&element, element_cache, frame),
                                    left_graph: scalar_graph,
                                }
                            } else {
                                PreparedTopologyPair::GeographicRightGraph {
                                    left: GeographicPrepared::new(&element, element_cache, frame),
                                    right: scalar,
                                    right_graph: scalar_graph,
                                }
                            };
                            topology_scalar_pair(spec, operands)
                        },
                    }
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

pub(crate) fn topology_scalar_pair(
    spec: &PredicateSpec,
    operands: PreparedTopologyPair<'_>,
) -> bool {
    // The array, index and coverer paths all run the pre-topology gates before
    // admitting a pair to the relation; the scalar pair was the one lane that
    // did not, so `gm.intersects(a, b)` on two operands a hundred units apart
    // still built both topology graphs and paired their edges to discover
    // they share no point.
    if let Some((left, right)) = operands.gate_operands()
        && let Some(verdict) = spec.pre_topology(left, right)
    {
        return verdict;
    }
    // The direct engines need the plans, not a graph: a pair that already
    // carries the scalar's graph (the scalar-vs-array lane) is admitted the
    // same way, and the graph stays for what they decline.
    if let Some((left, right)) = operands.cartesian_plans()
        && let Some(verdict) =
            crate::geometry::direct_linework_predicate(spec, left, right)
                .or_else(|| crate::geometry::direct_areal_predicate(spec, left, right))
                .or_else(|| crate::geometry::direct_areal_linework_predicate(spec, left, right))
    {
        return verdict;
    }
    predicate_matrix_value(spec, operands.relate())
}

/// A DE-9IM pattern over a scalar pair, decided from bounds or direct-engine
/// evidence before materializing the complete matrix.
pub(crate) fn topology_scalar_pattern(pattern: [u8; 9], operands: &PreparedTopologyPair<'_>) -> bool {
    if let Some((left, right)) = operands.gate_operands()
        && let Some(verdict) = pattern_bounds_gate(pattern, &left, &right)
    {
        return verdict;
    }
    if let Some((left, right)) = operands.cartesian_plans()
        && let Some(verdict) = crate::geometry::direct_linework_pattern(pattern, left, right)
            .or_else(|| crate::geometry::direct_areal_pattern(pattern, left, right))
            .or_else(|| crate::geometry::direct_areal_linework_pattern(pattern, left, right))
    {
        return verdict;
    }
    operands.relate().matches_bytes(pattern)
}

/// The cells sound disjoint bounds settle. Curve-boundary cells stay open:
/// whether a curve is closed is a staging fact, not a property of its box.
fn pattern_bounds_gate(pattern: [u8; 9], left: &GateOperand<'_>, right: &GateOperand<'_>) -> Option<bool> {
    use crate::geometry::{De9imInterval, Dimension, Loc, OperandShape, dimension};
    if !left.bounds_sound || !right.bounds_sound {
        return None;
    }
    let (Some(left_bounds), Some(right_bounds)) = (left.bounds, right.bounds) else {
        return None;
    };
    if left_bounds.intersects(right_bounds) {
        return None;
    }
    let shape_of = |operand: GateOperand<'_>| -> Option<OperandShape> {
        let interior = operand.row.with_shape(dimension)?;
        Some(OperandShape {
            interior,
            boundary: match interior {
                Dimension::Surface => Some(Dimension::Curve),
                Dimension::Curve => Some(Dimension::Point),
                Dimension::Point => None,
            },
        })
    };
    let (a, b) = (shape_of(*left)?, shape_of(*right)?);
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
    interval.pattern(pattern)
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
        Shape::Polygon(Polygon::new(
            Ring::from_closed(vec![
                Point::new_raw_xy(minx, miny),
                Point::new_raw_xy(maxx, miny),
                Point::new_raw_xy(maxx, maxy),
                Point::new_raw_xy(minx, maxy),
                Point::new_raw_xy(minx, miny),
            ]),
            Vec::new(),
        ))
    }

    fn line(start: (f64, f64), end: (f64, f64)) -> Shape {
        Shape::LineString(
            LineSeq::try_new(CoordSeq::from_vecs(
                vec![start.0, end.0],
                vec![start.1, end.1],
                None,
                None,
            ))
            .expect("fixture line has two vertices"),
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
            );
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
            ),
            [false]
        );
        assert_eq!(cartesian.topology_graph_accesses(), 0);

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
            ),
            [false]
        );
        assert_eq!(geographic_cache.geographic_topology_accesses(), 0);
    }

    #[test]
    fn admitted_scalar_rows_reuse_one_query_graph() {
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
            ),
            [true, true]
        );
        assert_eq!(cartesian.topology_graph_accesses(), 1);

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
                ),
                [true]
            );
        }
        assert_eq!(line_scalar.topology_graph_accesses(), 0);

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
            ),
            [true, true]
        );
        assert_eq!(geographic_cache.geographic_topology_accesses(), 1);
    }

    #[test]
    fn geographic_line_scalar_does_not_enter_cartesian_direct_relation() {
        let scalar = PreparedPlan::new(line((175.0, 0.0), (-175.0, 0.0)));
        let polygon = Shape::Polygon(Polygon::new(
            Ring::from_closed(vec![
                Point::new_raw_xy(170.0, -10.0),
                Point::new_raw_xy(-170.0, -10.0),
                Point::new_raw_xy(-170.0, 10.0),
                Point::new_raw_xy(170.0, 10.0),
                Point::new_raw_xy(170.0, -10.0),
            ]),
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
            ),
            [true]
        );
    }

    #[test]
    fn scalar_rows_relate_prepared_array_rows_from_fixed_graphs() {
        let scalar = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0));
        let array = PyGeometryArray::mixed_shapes(
            vec![line((-1.0, 1.0), (3.0, 1.0)), line((3.0, 3.0), (4.0, 4.0))],
            Frame::None,
        );
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
        );
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
            Shape::LineString(LineSeq::try_new(CoordSeq::from(points)).expect("two vertices"))
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
                let relation = predicate_matrix_value(&spec, pair.relate());
                let pair = PreparedTopologyPair::Cartesian {
                    left: &left_plan,
                    right: &right_plan,
                };
                assert_eq!(
                    topology_scalar_pair(&spec, pair),
                    relation,
                    "{predicate:?} disagreed with its own relation",
                );
            }
        }
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
        );
        assert_eq!(actual, [true]);
    }

    #[test]
    fn geographic_crossing_packed_rows_fail_open_the_bounds_gate() {
        let query = PreparedPlan::new(rectangle(172.0, 42.0, 178.0, 48.0));
        let coords = CoordSeq::from(vec![
            Point::new_raw_xy(170.0, 40.0),
            Point::new_raw_xy(-170.0, 40.0),
            Point::new_raw_xy(-170.0, 50.0),
            Point::new_raw_xy(170.0, 50.0),
            Point::new_raw_xy(170.0, 40.0),
        ]);
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
        );
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
                );
                assert_eq!(actual, [true]);
            }
        }
    }

    #[test]
    fn geographic_scalar_rows_relate_prepared_unsound_array_rows() {
        let scalar = PreparedPlan::new(rectangle(172.0, 42.0, 178.0, 48.0));
        let array =
            PyGeometryArray::mixed_shapes(vec![rectangle(170.0, 40.0, -170.0, 50.0)], Frame::None);
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
        );
        assert_eq!(actual, [true]);
    }
}
