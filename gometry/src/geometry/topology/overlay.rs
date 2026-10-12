//! Exact n-ary geographic set selection and output emission over global Q.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use super::emission::{
    DirectedSpan, EmissionNodeRegistry, FeatureFamilyRegistry, FragmentRecipes, LiteralPoint,
    LiteralPolygon, LiteralSequence, NativeMaterializedParts, PreparedNativeSource,
    PublicationInventory, QSourceImage, RecipeWalk, RecipeWalkIndex, RecipeWalkOwner,
    SealedQDrawing, SealedSourceDrawing, SelectedUse, SourceFrozenDrawing, index_recipe_walks,
    next_recipe_walk, write_completed_native,
};
use super::exact::{
    ExactChartPoint, ExactSourceEdge, ExactSourceRange, ExactSourceRing, SourceEdgeId,
    SourceEdgeKind, SourceRingId, SourceRowId,
};
use super::q::{ExactGeographicTopology, GlobalFaceId, QDartId, QNodeId, QPortId, row_locations};
use super::q_embedding::{
    EmittedNodeKey, EmittedVertex, QCoordinates, QEmbeddingEmission, build_cut_node_ports,
    canonical_bits, certify_and_search_q_embedding, certify_nearest_ring_roles, emitted_node_key,
    for_each_q_cycle_vertex, node_injectivity_collisions_with_work, source_ring_emitted_vertices,
};
#[cfg(test)]
use super::q_embedding::{
    NearestQCertificate, certify_node_injectivity, certify_q_embedding_nearest,
    node_injectivity_collisions,
};
use crate::error::{ResourceError, Result};
use crate::geographic::{ChartPole, SeamIdentity};
use crate::geometry::embedding_completion::{
    CheckedAssignment, FrozenLiteralBuffers, LiteralCheck, ShapeLiteralLayout, bind_literals,
};
use crate::geometry::embedding_search::{
    EvaluationBudget, EvaluationOnly, SearchAccount, SearchOutcome,
};
use crate::geometry::ordinates::{
    NodeOrdinates, RequestRowInput, RootId, RootRequest, SourceArcMetricAccess,
    resolve_request_rows,
};
use crate::geometry::overlay::{
    CertifiedOverlayOrder, OverlayOp, build_certified_overlay_shape_with_axes,
    build_certified_overlay_shape_with_axes_and_order,
};
use crate::geometry::segments::try_sort_by_with_work;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{
    CoordSeq, CoordSeqBuilder, CoordinateAxes, ExpansionBudget, GeometryErrorKind, GeometryKind,
    HasM, HasZ, LineSeq, Loc, MOrdinate, Point, Polygon, Ring, Shape, Work, XY, ZOrdinate,
};

#[derive(Clone, Copy)]
pub(super) struct LineStep {
    span: usize,
    from_endpoint: SpanEndpoint,
    owner: usize,
}

#[derive(Clone, Copy)]
struct SelectedLineSpan {
    owner: usize,
}

struct SourceLineSpan {
    low: ExactRational,
    high: ExactRational,
    span: usize,
    owner: usize,
    reversed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QLineOrigin {
    Curve,
    ArealBoundary,
}

pub(super) struct QLinePrimitive<T> {
    origin: QLineOrigin,
    pub(super) recipe_walk: RecipeWalkOwner,
    pub(super) path: T,
}

pub(super) type PlannedQLine = QLinePrimitive<Vec<LineStep>>;
pub(super) type DrawnQLine<'topology> = QLinePrimitive<Vec<EmittedVertex<'topology>>>;

#[derive(Clone, Copy)]
struct BoundarySupport {
    has_area: bool,
    has_curve: bool,
    has_point: bool,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum SpanEndpoint {
    Start,
    End,
}

impl SpanEndpoint {
    const fn opposite(self) -> Self {
        match self {
            Self::Start => Self::End,
            Self::End => Self::Start,
        }
    }
}

pub(super) struct BoundaryCycle {
    pub(super) darts: Vec<QDartId>,
    pub(super) region: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct SelectedRegionPoles {
    north: bool,
    south: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SelectedRegionFact {
    pub(super) root: usize,
    pub(super) poles: SelectedRegionPoles,
}

impl SelectedRegionPoles {
    pub(super) const fn includes(self, pole: ChartPole) -> bool {
        match pole {
            ChartPole::North => self.north,
            ChartPole::South => self.south,
        }
    }
}

pub(super) struct QEmissionPlan {
    whole_sphere: bool,
    force_native: bool,
    boundary_support: Option<BoundarySupport>,
    pub(super) polygons: PolygonPlan,
    pub(super) cycles: Vec<BoundaryCycle>,
    pub(super) selected_regions: Vec<SelectedRegionFact>,
    pub(super) selected_face_roots: Vec<Option<usize>>,
    pub(super) fragment_recipes: FragmentRecipes,
    pub(super) line_paths: Vec<PlannedQLine>,
    pub(super) point_nodes: Vec<QNodeId>,
}

pub(super) enum PolygonPlan {
    WholeSphere(Vec<usize>),
    Regions {
        rings: Vec<PlannedAreaRing>,
        region_count: usize,
    },
}

pub(super) enum DrawnPolygons<'topology> {
    WholeSphere(Vec<Vec<DrawnSourceRing<'topology>>>),
    Regions {
        rings: Vec<DrawnAreaRing<'topology>>,
        region_count: usize,
    },
}

pub(super) struct DrawnSourceRing<'topology> {
    pub(super) hole: bool,
    pub(super) recipe_walk: RecipeWalkOwner,
    pub(super) vertices: Vec<EmittedVertex<'topology>>,
}

pub(super) struct DrawnAreaRing<'topology> {
    pub(super) region: usize,
    pub(super) origin: DrawnAreaOrigin,
    pub(super) recipe_walk: RecipeWalkOwner,
    pub(super) native_start_port: Option<QPortId>,
    pub(super) vertices: Vec<EmittedVertex<'topology>>,
}

#[derive(Clone, Copy)]
pub(super) enum DrawnAreaOrigin {
    Source,
    Cycle,
}

pub(super) struct QEmissionDrawing<'topology> {
    pub(super) polygons: DrawnPolygons<'topology>,
    pub(super) lines: Vec<DrawnQLine<'topology>>,
    pub(super) points: Vec<EmittedVertex<'topology>>,
}

impl QEmissionDrawing<'_> {
    pub(super) fn live_node_keys(
        &self,
        frame: crate::geographic::GeographicFrame,
        work: &mut impl Work,
    ) -> Result<Vec<(EmittedNodeKey, QNodeId)>> {
        let mut live = Vec::new();
        self.for_each_vertex(
            |vertex| record_emitted_node(frame, vertex.port.node, vertex.point, &mut live),
            work,
        )?;
        Ok(live)
    }

    pub(super) fn axes(
        &self,
        declared_axes: CoordinateAxes,
        ordinates: &[NodeOrdinates],
        work: &mut impl Work,
    ) -> Result<CoordinateAxes> {
        let mut has_z = declared_axes.has_z();
        let mut has_m = declared_axes.has_m();
        self.for_each_vertex(
            |vertex| {
                let node_ordinates =
                    ordinates.get(vertex.port.node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q emitted an unknown node".into())
                    })?;
                has_z &= node_ordinates.z.value().is_some();
                has_m &= node_ordinates.m.value().is_some();
                Ok(())
            },
            work,
        )?;
        Ok(CoordinateAxes::new(HasZ(has_z), HasM(has_m)))
    }

    fn for_each_vertex(
        &self,
        mut visit: impl FnMut(&EmittedVertex<'_>) -> Result<()>,
        work: &mut impl Work,
    ) -> Result<()> {
        match &self.polygons {
            DrawnPolygons::WholeSphere(polygons) => {
                for polygon in polygons {
                    for ring in polygon {
                        for vertex in &ring.vertices {
                            work.charge(1)?;
                            visit(vertex)?;
                        }
                    }
                }
            },
            DrawnPolygons::Regions { rings, .. } => {
                for ring in rings {
                    for vertex in &ring.vertices {
                        work.charge(1)?;
                        visit(vertex)?;
                    }
                }
            },
        }
        for line in &self.lines {
            for vertex in &line.path {
                work.charge(1)?;
                visit(vertex)?;
            }
        }
        for vertex in &self.points {
            work.charge(1)?;
            visit(vertex)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(super) enum PlannedAreaRing {
    Source {
        ring: usize,
        region: usize,
        reverse: bool,
        cycle_start: Option<QPortId>,
    },
    Cycle {
        cycle: usize,
        region: usize,
    },
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QEmbeddingChoice {
    Nearest,
    Searched,
}

struct CertifiedQEmission {
    sealed: SealedQDrawing,
    family_state: Option<FeatureFamilyRegistry>,
    #[cfg(test)]
    choice: QEmbeddingChoice,
    #[cfg(test)]
    coordinates: QCoordinates,
}

struct WrittenQDrawing {
    shape: Shape,
    line_traces: Vec<super::emission::TracedLine>,
    family_state: Option<FeatureFamilyRegistry>,
}

fn write_certified_q_emission(embedding: CertifiedQEmission) -> Result<WrittenQDrawing> {
    let family_state = embedding.family_state;
    match embedding.sealed {
        SealedQDrawing::Source(source) => Ok(WrittenQDrawing {
            shape: source.sealed.into_shape(),
            line_traces: source.line_traces,
            family_state,
        }),
        SealedQDrawing::Native(native) => {
            let written = write_completed_native(native)?;
            Ok(WrittenQDrawing {
                shape: written.shape,
                line_traces: written.line_traces,
                family_state,
            })
        },
    }
}

fn resolve_q_embedding_certified<
    'topology,
    'access,
    M: crate::geometry::ChartMetric,
    A: SearchAccount,
>(
    topology: &'topology ExactGeographicTopology,
    plan: &QEmissionPlan,
    source_points: &[Option<EmittedVertex<'topology>>],
    nearest: &QCoordinates,
    live_node_keys: &[(EmittedNodeKey, QNodeId)],
    drawing: QEmissionDrawing<'topology>,
    ordinates_axes: CoordinateAxes,
    strict: bool,
    operation: &'static str,
    metric_access: Option<&'access SourceArcMetricAccess<'access, '_, M>>,
    expansion_budget: &mut ExpansionBudget,
    account: &mut A,
) -> Result<(super::q_embedding::QEmbeddingPass, Vec<NodeOrdinates>)> {
    let ordinates = node_ordinates(topology, ordinates_axes, metric_access, account)?;
    let published_inventory = plan.fragment_recipes.publish_with_expansion_budget(
        topology,
        nearest,
        expansion_budget,
        account,
    )?;
    let emission =
        plan.embedding_emission(topology, source_points, &published_inventory, account)?;
    let pass = match certify_and_search_q_embedding(
        topology,
        &emission,
        nearest,
        live_node_keys,
        drawing,
        &ordinates,
        ordinates_axes,
        strict,
        operation,
        metric_access,
        account,
    )? {
        SearchOutcome::Pass(pass) => pass,
        SearchOutcome::Exhausted => {
            return Err(GeometryErrorKind::unrepresentable_output(operation));
        },
    };
    Ok((pass, ordinates))
}

fn resolve_q_embedding<M: crate::geometry::ChartMetric, A: SearchAccount>(
    topology: &ExactGeographicTopology,
    plan: &QEmissionPlan,
    source_points: &[Option<EmittedVertex<'_>>],
    nearest: &QCoordinates,
    declared_axes: CoordinateAxes,
    strict: bool,
    operation: &'static str,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    expansion_budget: &mut ExpansionBudget,
    preparation_work: &mut A,
) -> Result<CertifiedQEmission> {
    let drawing = plan.drawing(topology, nearest, source_points, preparation_work)?;
    let mut live_node_keys = drawing.live_node_keys(topology.sources.frame, preparation_work)?;
    let identity_conflicts =
        node_injectivity_collisions_with_work(&mut live_node_keys, preparation_work)?;
    preparation_work
        .charge(u64::try_from(live_node_keys.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let has_rounded_constructed_node = nearest.has_inexact_nodes_with_work(
        topology,
        live_node_keys.iter().map(|(_, node)| *node),
        preparation_work,
    )?;
    let source_reuse =
        plan.source_reuse_is_proven(topology, &drawing, source_points, preparation_work)?;
    let needs_q_certificate = !identity_conflicts.is_empty()
        || has_rounded_constructed_node
        || plan.force_native
        || !source_reuse;
    #[cfg(test)]
    let mut choice = QEmbeddingChoice::Nearest;
    #[cfg(test)]
    let mut test_coordinates = nearest.clone();
    let (sealed, family_state) = if needs_q_certificate {
        let (pass, _ordinates) = resolve_q_embedding_certified(
            topology,
            plan,
            source_points,
            nearest,
            &live_node_keys,
            drawing,
            declared_axes,
            strict,
            operation,
            metric_access,
            expansion_budget,
            preparation_work,
        )?;
        #[cfg(test)]
        {
            choice = if pass.searched {
                QEmbeddingChoice::Searched
            } else {
                QEmbeddingChoice::Nearest
            };
            test_coordinates = pass.coordinates.clone();
        }
        let output = pass.output;
        (output.sealed, output.family_state)
    } else {
        let ordinates = node_ordinates(topology, declared_axes, metric_access, preparation_work)?;
        let ring_roles = certify_nearest_ring_roles(topology, &drawing, preparation_work)?;
        let source = plan.seal_source_drawing(
            drawing,
            &ring_roles,
            &ordinates,
            None,
            None,
            declared_axes,
            strict,
            operation,
            preparation_work,
        )?;
        let images = CheckedAssignment::new(super::q_embedding::QSourceImages {
            topology,
            source_ports: None,
            publication: &[],
        });
        let mut chart = super::q_embedding::AssignedSourceChart {
            topology,
            source_ports: None,
            source_ordinates: &ordinates,
            publication_points: &[],
            publication_ordinates: &[],
        };
        let sealed = match bind_literals(source.buffers, &mut chart, &images, preparation_work)? {
            LiteralCheck::Pass(sealed) => sealed,
            LiteralCheck::Reject(_) => {
                return Err(GeometryErrorKind::Invalid(
                    "unchanged Q source output failed its literal binding".into(),
                )
                .into());
            },
        };
        (
            SealedQDrawing::Source(SealedSourceDrawing {
                sealed,
                line_traces: Vec::new(),
            }),
            None,
        )
    };
    Ok(CertifiedQEmission {
        sealed,
        family_state,
        #[cfg(test)]
        choice,
        #[cfg(test)]
        coordinates: test_coordinates,
    })
}

/// Select all dimensions from the one exact source chart and its global Q.
/// Rounding happens only after face, span, and node selection is complete.
pub(in crate::geometry) fn exact_geographic_overlay<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    operation: OverlayOp,
    declared_axes: CoordinateAxes,
    strict: bool,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Shape> {
    let mut budget = EvaluationBudget::new(operation.name(), 0)?;
    let mut preparation_work = EvaluationOnly::new(&mut budget);
    let mut expansion_budget = ExpansionBudget::new(operation.name(), "computed edges");
    let plan = QEmissionPlan::build(topology, operation, &mut preparation_work)?;
    let nearest = QCoordinates::build_with_work(topology, &mut preparation_work)?;
    let source_points = source_point_coordinates_with_work(topology, &mut preparation_work)?;
    let embedding = resolve_q_embedding(
        topology,
        &plan,
        &source_points,
        &nearest,
        declared_axes,
        strict,
        operation.name(),
        metric_access,
        &mut expansion_budget,
        &mut preparation_work,
    )?;
    publish_q_overlay(embedding)
}

pub(in crate::geometry) fn prepare_native_source_lines<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    line_members: &[usize],
    declared_axes: CoordinateAxes,
    strict: bool,
    operation: &'static str,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    expansion_budget: &mut ExpansionBudget,
    budget: &mut EvaluationBudget,
) -> Result<PreparedNativeSource> {
    let plan = QEmissionPlan::build_source_lines(topology, line_members, budget)?;
    if plan.line_paths.is_empty() {
        return Ok(PreparedNativeSource {
            shape: Shape::typed_empty(GeometryKind::MultiLineString, declared_axes),
            line_traces: Vec::new(),
            source_members: Vec::new(),
            family_state: None,
        });
    }
    let mut source_members = Vec::with_capacity(plan.line_paths.len());
    for line in &plan.line_paths {
        let RecipeWalkOwner::Line(member) = line.recipe_walk else {
            return Err(GeometryErrorKind::Invalid(
                "native source-line plan contains a non-line producer".into(),
            )
            .into());
        };
        source_members.push(member);
    }
    let nearest = QCoordinates::build_with_work(topology, budget)?;
    let source_points = source_point_coordinates_with_work(topology, budget)?;
    let embedding = resolve_q_embedding(
        topology,
        &plan,
        &source_points,
        &nearest,
        declared_axes,
        strict,
        operation,
        metric_access,
        expansion_budget,
        budget,
    )?;
    let written = write_certified_q_emission(embedding)?;
    let line_count = match &written.shape {
        Shape::LineString(_) => 1,
        Shape::MultiLineString(lines) => lines.len(),
        Shape::Empty(GeometryKind::LineString | GeometryKind::MultiLineString, _) => 0,
        _ => {
            return Err(GeometryErrorKind::Invalid(
                "native line preparation emitted a non-lineal shape".into(),
            )
            .into());
        },
    };
    if line_count != written.line_traces.len() {
        return Err(GeometryErrorKind::Invalid(
            "native line preparation lost its written trace rows".into(),
        )
        .into());
    }
    Ok(PreparedNativeSource {
        shape: written.shape,
        line_traces: written.line_traces,
        source_members,
        family_state: written.family_state,
    })
}

fn publish_q_overlay(embedding: CertifiedQEmission) -> Result<Shape> {
    Ok(write_certified_q_emission(embedding)?.shape)
}

pub(in crate::geometry) fn exact_geographic_boundary<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    declared_axes: CoordinateAxes,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Shape> {
    if topology.sources.row_count != 1 {
        return Err(GeometryErrorKind::Invalid(
            "an exact geographic boundary requires one source row".into(),
        )
        .into());
    }
    let row = SourceRowId(0);
    let mut budget = EvaluationBudget::new("geographic boundary", 0)?;
    let mut preparation_work = EvaluationOnly::new(&mut budget);
    let mut expansion_budget = ExpansionBudget::new("geographic boundary", "computed edges");
    let (plan, _support) = QEmissionPlan::build_boundary(topology, &mut preparation_work)?;
    let source_points = boundary_source_points_with_work(topology, row, &mut preparation_work)?;
    let nearest = QCoordinates::build_with_work(topology, &mut preparation_work)?;
    let embedding = resolve_q_embedding(
        topology,
        &plan,
        &source_points,
        &nearest,
        declared_axes,
        false,
        "geographic boundary",
        metric_access,
        &mut expansion_budget,
        &mut preparation_work,
    )?;
    Ok(write_certified_q_emission(embedding)?.shape)
}

fn boundary_cycle_line_paths(
    topology: &ExactGeographicTopology,
    cycles: &[BoundaryCycle],
    work: &mut impl Work,
) -> Result<(Vec<PlannedQLine>, Vec<bool>)> {
    let q = &topology.q;
    work.charge(
        u64::try_from(
            cycles
                .len()
                .checked_add(q.nodes.len())
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut paths = Vec::with_capacity(cycles.len());
    let mut covered = vec![false; q.nodes.len()];
    for cycle in cycles {
        work.charge(1)?;
        let mut path = Vec::with_capacity(cycle.darts.len());
        for &dart_id in &cycle.darts {
            work.charge(1)?;
            let dart = q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary cycle has an unknown dart".into())
            })?;
            let span_index = dart.span.0 as usize;
            let span = q.spans.get(span_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary cycle has an unknown span".into())
            })?;
            let from_endpoint = if dart.from_node == span.nodes[0] {
                SpanEndpoint::Start
            } else if dart.from_node == span.nodes[1] {
                SpanEndpoint::End
            } else {
                return Err(GeometryErrorKind::Invalid(
                    "Q boundary dart does not start at a span endpoint".into(),
                )
                .into());
            };
            let to_node = match from_endpoint {
                SpanEndpoint::Start => span.nodes[1],
                SpanEndpoint::End => span.nodes[0],
            };
            if dart.to_node != to_node {
                return Err(GeometryErrorKind::Invalid(
                    "Q boundary dart does not end at the opposite span endpoint".into(),
                )
                .into());
            }
            *covered.get_mut(dart.from_node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary dart starts at an unknown node".into())
            })? = true;
            *covered.get_mut(dart.to_node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary dart ends at an unknown node".into())
            })? = true;
            path.push(LineStep {
                span: span_index,
                from_endpoint,
                owner: area_owner_index(topology, span_index, work)?,
            });
        }
        paths.push(QLinePrimitive {
            origin: QLineOrigin::ArealBoundary,
            recipe_walk: RecipeWalkOwner::Line(paths.len()),
            path,
        });
    }
    Ok((paths, covered))
}

pub(super) fn source_ring_reuse_matches(
    topology: &ExactGeographicTopology,
    source_ring: &ExactSourceRing,
    reverse: bool,
    vertices: &[EmittedVertex<'_>],
    recipes: &[super::emission::FragmentRecipe],
    walk: &RecipeWalk,
) -> Result<bool> {
    let edge_count = source_ring.source_edges.len();
    if vertices.len()
        != edge_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?
        || walk.fragments.len() != edge_count
    {
        return Ok(false);
    }
    let expected_range = if reverse {
        (ExactRational::one(), ExactRational::zero())
    } else {
        (ExactRational::zero(), ExactRational::one())
    };
    for index in 0..edge_count {
        let edge_index = if reverse {
            source_ring
                .source_edges
                .end
                .checked_sub(index.checked_add(1).ok_or(ResourceError::SizeOverflow)?)
                .ok_or(ResourceError::SizeOverflow)?
        } else {
            source_ring
                .source_edges
                .start
                .checked_add(index)
                .ok_or(ResourceError::SizeOverflow)?
        };
        let edge = topology.sources.edges.get(edge_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("source ring reuse edge is unknown".into())
        })?;
        let SourceEdgeKind::Ring { ring, .. } = edge.kind else {
            return Ok(false);
        };
        if ring != source_ring.id {
            return Ok(false);
        }
        let recipe_id = *walk.fragments.get(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("source ring reuse recipe is unknown".into())
        })?;
        let recipe = recipes.get(recipe_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("source ring reuse recipe row is unknown".into())
        })?;
        let Some(range) = recipe.ranges.first() else {
            return Ok(false);
        };
        let ports = topology
            .q
            .source_edge_ports
            .get(edge.id.0 as usize)
            .copied()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("source ring reuse edge has no Q ports".into())
            })?;
        let expected_ports = if reverse { [ports[1], ports[0]] } else { ports };
        let start = vertices.get(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("source ring reuse has no start vertex".into())
        })?;
        let end = vertices
            .get(index.checked_add(1).ok_or(ResourceError::SizeOverflow)?)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("source ring reuse has no end vertex".into())
            })?;
        let (source_start, source_end) = if reverse {
            (edge.source[1], edge.source[0])
        } else {
            (edge.source[0], edge.source[1])
        };
        if !edge.source_interval_reads_as(
            topology.sources.frame,
            reverse,
            start.point,
            end.point,
        )? {
            return Ok(false);
        }
        if recipe.use_kind != SelectedUse::AreaBoundary
            || recipe.ranges.len() != 1
            || range.source_edge != edge.id
            || range.range.start != expected_range.0
            || range.range.end != expected_range.1
            || range.producer_start != ExactRational::zero()
            || range.producer_end != ExactRational::one()
            || range.endpoint_only
            || !range.emitted
            || recipe.endpoints != expected_ports
            || start.port.id != expected_ports[0]
            || end.port.id != expected_ports[1]
            || start.point != source_start
            || end.point != source_end
        {
            return Ok(false);
        }
    }
    Ok(true)
}

impl QEmissionPlan {
    pub(super) const fn is_whole_sphere(&self) -> bool {
        self.whole_sphere
    }

    pub(super) fn build(
        topology: &ExactGeographicTopology,
        operation: OverlayOp,
        work: &mut impl Work,
    ) -> Result<Self> {
        let q = &topology.q;
        let row_count = topology.sources.row_count;
        let faces = select_faces(q, row_count, operation, work)?;
        let mut whole_sphere = !faces.is_empty();
        for &selected in &faces {
            work.charge(1)?;
            whole_sphere &= selected;
            if !whole_sphere {
                break;
            }
        }
        let mut face_regions = face_regions(q, &faces, work)?;
        let line_spans = select_line_spans(topology, &faces, row_count, operation, work)?;
        let cycles = selected_area_boundaries(q, &faces, &mut face_regions, work)?;
        let point_nodes = select_points(topology, &faces, &line_spans, row_count, operation, work)?;
        let line_paths =
            planned_line_paths(topology, line_steps(topology, &line_spans, work)?, work)?;
        let polygons = PolygonPlan::build(topology, whole_sphere, operation, &cycles, work)?;
        let region_roots = match &polygons {
            PolygonPlan::WholeSphere(_) => Vec::new(),
            PolygonPlan::Regions { rings, .. } => rings
                .iter()
                .map(|ring| {
                    work.charge(1)?;
                    Ok(match *ring {
                        PlannedAreaRing::Source { region, .. }
                        | PlannedAreaRing::Cycle { region, .. } => region,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        };
        let selected_regions =
            selected_region_facts(q, &faces, &mut face_regions, &region_roots, work)?;
        let selected_face_roots = selected_face_roots(q, &faces, &mut face_regions, work)?;
        let fragment_recipes =
            build_fragment_recipes(topology, &polygons, &cycles, &line_paths, work)?;
        Ok(Self {
            whole_sphere,
            force_native: false,
            boundary_support: None,
            polygons,
            cycles,
            selected_regions,
            selected_face_roots,
            fragment_recipes,
            line_paths,
            point_nodes,
        })
    }

    fn build_boundary(
        topology: &ExactGeographicTopology,
        work: &mut impl Work,
    ) -> Result<(Self, BoundarySupport)> {
        let q = &topology.q;
        let row = SourceRowId(0);
        let selected_faces = select_faces(q, 1, OverlayOp::Union, work)?;
        let mut has_area = false;
        for &selected in &selected_faces {
            work.charge(1)?;
            has_area |= selected;
            if has_area {
                break;
            }
        }
        let mut regions = face_regions(q, &selected_faces, work)?;
        let cycles = selected_area_boundaries(q, &selected_faces, &mut regions, work)?;
        work.charge(
            u64::try_from(
                cycles
                    .len()
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?,
            )
            .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let region_roots = cycles.iter().map(|cycle| cycle.region).collect::<Vec<_>>();
        let selected_regions =
            selected_region_facts(q, &selected_faces, &mut regions, &region_roots, work)?;
        let selected_face_roots = selected_face_roots(q, &selected_faces, &mut regions, work)?;
        let (line_paths, covered_by_area_cycles) =
            boundary_cycle_line_paths(topology, &cycles, work)?;
        let polygons = PolygonPlan::Regions {
            rings: Vec::new(),
            region_count: q.global_face_count,
        };
        let fragment_recipes =
            build_fragment_recipes(topology, &polygons, &cycles, &line_paths, work)?;

        let row_locations = row_locations(topology, row)?;
        let mut point_nodes = Vec::new();
        let mut has_point = false;
        for (node, &covered) in q.nodes.iter().zip(&covered_by_area_cycles) {
            work.charge(1)?;
            let fan_start = *q.outgoing_offsets.get(node.id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary node has no fan start".into())
            })?;
            let fan_end = *q
                .outgoing_offsets
                .get(node.id.0 as usize + 1)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q boundary node has no fan end".into())
                })?;
            work.charge(
                u64::try_from(fan_end.checked_sub(fan_start).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q boundary node has reversed fan offsets".into())
                })?)
                .map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let location = row_locations.node(node.id)?;
            has_point |= location == Loc::Interior;
            if location == Loc::Boundary && !covered {
                point_nodes.push(node.id);
            }
        }

        let mut has_curve = false;
        for span_index in 0..q.spans.len() {
            work.charge(1)?;
            if row_locations.span(span_index)? == Some(Loc::Interior) {
                has_curve = true;
                break;
            }
        }

        let support = BoundarySupport {
            has_area,
            has_curve,
            has_point,
        };
        Ok((
            Self {
                whole_sphere: false,
                force_native: false,
                boundary_support: Some(support),
                polygons,
                cycles: Vec::new(),
                selected_regions,
                selected_face_roots,
                fragment_recipes,
                line_paths,
                point_nodes,
            },
            support,
        ))
    }

    pub(super) fn build_source_lines(
        topology: &ExactGeographicTopology,
        line_members: &[usize],
        work: &mut impl Work,
    ) -> Result<Self> {
        let q = &topology.q;
        work.charge(
            u64::try_from(topology.sources.edges.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut spans_by_source = Vec::with_capacity(topology.sources.edges.len());
        spans_by_source.resize_with(topology.sources.edges.len(), Vec::<SourceLineSpan>::new);
        for (span_index, span) in q.spans.iter().enumerate() {
            work.charge(1)?;
            for (owner_index, owner) in span.owners.iter().enumerate() {
                work.charge(1)?;
                let row = spans_by_source
                    .get_mut(owner.source_edge.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q span has an unknown source edge".into())
                    })?;
                let (low, high) = if owner
                    .source_range
                    .start
                    .try_cmp(&owner.source_range.end)?
                    .is_le()
                {
                    (
                        owner.source_range.start.clone(),
                        owner.source_range.end.clone(),
                    )
                } else {
                    (
                        owner.source_range.end.clone(),
                        owner.source_range.start.clone(),
                    )
                };
                row.push(SourceLineSpan {
                    low,
                    high,
                    span: span_index,
                    owner: owner_index,
                    reversed: owner.reversed,
                });
            }
        }

        work.charge(u64::try_from(line_members.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut line_paths = Vec::with_capacity(line_members.len());
        for &member in line_members {
            let line = topology.sources.lines.get(member).ok_or_else(|| {
                GeometryErrorKind::Invalid("native source line member is unknown".into())
            })?;
            let mut path = Vec::new();
            for source_edge in line.source_edges.clone() {
                work.charge(1)?;
                let edge = topology.sources.edges.get(source_edge).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native source line edge is unknown".into())
                })?;
                if !matches!(
                    edge.kind,
                    SourceEdgeKind::Line { line: source_line } if source_line.0 as usize == member
                ) {
                    return Err(GeometryErrorKind::Invalid(
                        "native source line range contains another component".into(),
                    )
                    .into());
                }
                let spans = spans_by_source.get_mut(source_edge).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native source line span row is unknown".into())
                })?;
                try_sort_by_with_work(spans, work, |left, right, work| {
                    work.charge(1)?;
                    Ok(left
                        .low
                        .try_cmp(&right.low)?
                        .then(left.high.try_cmp(&right.high)?)
                        .then(left.span.cmp(&right.span))
                        .then(left.owner.cmp(&right.owner)))
                })?;
                if spans.is_empty() {
                    continue;
                }
                let zero = ExactRational::zero();
                let one = ExactRational::one();
                let mut covered_to = zero;
                for source_span in spans.iter() {
                    work.charge(1)?;
                    if source_span.low.try_cmp(&covered_to)?.is_gt() {
                        return Err(GeometryErrorKind::Invalid(
                            "native source line has a gap in its Q span coverage".into(),
                        )
                        .into());
                    }
                    if source_span.high.try_cmp(&covered_to)?.is_gt() {
                        covered_to = source_span.high.clone();
                    }
                    path.push(LineStep {
                        span: source_span.span,
                        from_endpoint: if source_span.reversed {
                            SpanEndpoint::End
                        } else {
                            SpanEndpoint::Start
                        },
                        owner: source_span.owner,
                    });
                }
                if covered_to.try_cmp(&one)?.is_lt() {
                    return Err(GeometryErrorKind::Invalid(
                        "native source line Q spans do not reach their endpoint".into(),
                    )
                    .into());
                }
            }
            if path.is_empty() {
                continue;
            }
            line_paths.push(QLinePrimitive {
                origin: QLineOrigin::Curve,
                recipe_walk: RecipeWalkOwner::Line(member),
                path,
            });
        }
        let polygons = PolygonPlan::Regions {
            rings: Vec::new(),
            region_count: q.global_face_count,
        };
        let fragment_recipes = build_fragment_recipes(topology, &polygons, &[], &line_paths, work)?;
        work.charge(u64::try_from(q.global_face_count).map_err(|_| ResourceError::SizeOverflow)?)?;
        Ok(Self {
            whole_sphere: false,
            force_native: true,
            boundary_support: None,
            polygons,
            cycles: Vec::new(),
            selected_regions: Vec::new(),
            selected_face_roots: vec![None; q.global_face_count],
            fragment_recipes,
            line_paths,
            point_nodes: Vec::new(),
        })
    }

    pub(super) fn seal_source_drawing(
        &self,
        drawing: QEmissionDrawing<'_>,
        ring_roles: &[bool],
        ordinates: &[NodeOrdinates],
        publication: Option<SourcePublication<'_>>,
        source_ports: Option<&[Option<ExactChartPoint>]>,
        declared_axes: CoordinateAxes,
        strict_axes: bool,
        operation: &'static str,
        work: &mut impl Work,
    ) -> Result<SourceFrozenDrawing> {
        let walks = publication
            .map(|_| index_recipe_walks(self.fragment_recipes.walks(), work))
            .transpose()?;
        if let Some(publication) = publication
            && (publication.points.len() != publication.inventory.samples.len()
                || publication.ordinates.len() != publication.inventory.samples.len()
                || publication.live_samples.len() != publication.inventory.samples.len())
        {
            return Err(GeometryErrorKind::Invalid(
                "source publication buffers do not match their sample rows".into(),
            )
            .into());
        }
        let source_axes = drawing.axes(declared_axes, ordinates, work)?;
        let mut has_z = source_axes.has_z();
        let mut has_m = source_axes.has_m();
        if let Some(publication) = publication {
            for (sample, &live) in publication.live_samples.iter().enumerate() {
                if !live {
                    continue;
                }
                work.charge(1)?;
                let sample_ordinates = publication.ordinates.get(sample).ok_or_else(|| {
                    GeometryErrorKind::Invalid("live publication sample has no ordinate row".into())
                })?;
                has_z &= sample_ordinates.z.value().is_some();
                has_m &= sample_ordinates.m.value().is_some();
            }
        }
        let axes = CoordinateAxes::new(HasZ(has_z), HasM(has_m));
        if strict_axes && axes != declared_axes {
            return Err(GeometryErrorKind::ordinate_dropped(operation));
        }
        let (points, lines, polygons, layout) = write_drawing(
            drawing,
            ring_roles,
            ordinates,
            publication,
            source_ports,
            walks.as_ref(),
            axes,
            work,
        )?;
        let (shape, order) = seal_output_parts(
            self,
            points,
            lines,
            polygons,
            axes,
            declared_axes,
            true,
            work,
        )?;
        let layout = layout.in_output_order(
            order.ok_or_else(|| {
                GeometryErrorKind::Invalid("source shape lost its literal output ordering".into())
            })?,
            work,
        )?;
        Ok(SourceFrozenDrawing {
            buffers: FrozenLiteralBuffers::shapes(shape, layout),
        })
    }

    pub(super) fn seal_native_parts(
        &self,
        parts: NativeMaterializedParts,
        declared_axes: CoordinateAxes,
        work: &mut impl Work,
    ) -> Result<super::emission::NativeFrozenDrawing> {
        parts.validate_literal_uses()?;
        let NativeMaterializedParts {
            axes,
            points,
            lines,
            polygons,
            line_traces,
            literal_uses,
        } = parts;
        let (shape, order) = seal_output_parts(
            self,
            points,
            lines,
            polygons,
            axes,
            declared_axes,
            true,
            work,
        )?;
        let (literal_uses, line_traces) = super::emission::native_literal_uses_in_output_order(
            literal_uses,
            line_traces,
            order.ok_or_else(|| {
                GeometryErrorKind::Invalid("native shape lost its literal output ordering".into())
            })?,
            work,
        )?;
        Ok(super::emission::NativeFrozenDrawing {
            buffers: crate::geometry::embedding_completion::FrozenLiteralBuffers::shapes(
                shape,
                literal_uses,
            ),
            line_traces,
        })
    }

    #[cfg(test)]
    fn live_node_keys(
        &self,
        topology: &ExactGeographicTopology,
        coordinates: &QCoordinates,
        source_points: &[Option<EmittedVertex<'_>>],
    ) -> Result<Vec<(EmittedNodeKey, QNodeId)>> {
        let mut work = crate::geometry::Unmetered;
        let drawing = self.drawing(topology, coordinates, source_points, &mut work)?;
        let mut work = crate::geometry::Unmetered;
        drawing.live_node_keys(topology.sources.frame, &mut work)
    }

    pub(super) fn embedding_emission<'topology, 'emission>(
        &'emission self,
        topology: &ExactGeographicTopology,
        source_points: &'emission [Option<EmittedVertex<'topology>>],
        publication: &'emission PublicationInventory,
        work: &mut impl Work,
    ) -> Result<QEmbeddingEmission<'topology, 'emission>> {
        let mut spans = Vec::new();
        let mut areal_spans = Vec::new();
        let mut live_nodes = Vec::new();
        let q = &topology.q;
        match &self.polygons {
            PolygonPlan::WholeSphere(source_polygons) => {
                for &polygon_index in source_polygons {
                    work.charge(1)?;
                    let polygon =
                        topology
                            .sources
                            .polygons
                            .get(polygon_index)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q whole-sphere plan references an unknown polygon".into(),
                                )
                            })?;
                    for ring_index in polygon.rings.clone() {
                        work.charge(1)?;
                        let ring = topology.sources.rings.get(ring_index).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q whole-sphere plan references an unknown ring".into(),
                            )
                        })?;
                        append_source_ring_live_nodes(topology, ring, &mut live_nodes, work)?;
                    }
                }
            },
            PolygonPlan::Regions {
                rings: planned_rings,
                ..
            } => {
                for planned in planned_rings {
                    work.charge(1)?;
                    match *planned {
                        PlannedAreaRing::Source { ring, .. } => {
                            let source_ring =
                                topology.sources.rings.get(ring).ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "Q area plan references an unknown source ring".into(),
                                    )
                                })?;
                            append_source_ring_live_nodes(
                                topology,
                                source_ring,
                                &mut live_nodes,
                                work,
                            )?;
                            for cycle in &self.cycles {
                                work.charge(1)?;
                                if !cycle_belongs_to_source_ring(
                                    topology,
                                    cycle,
                                    source_ring.id,
                                    work,
                                )? {
                                    continue;
                                }
                                for &dart_id in &cycle.darts {
                                    work.charge(1)?;
                                    let dart =
                                        q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                                            GeometryErrorKind::Invalid(
                                                "Q source cycle references an unknown dart".into(),
                                            )
                                        })?;
                                    let span = dart.span.0 as usize;
                                    spans.push(span);
                                    areal_spans.push(span);
                                }
                            }
                        },
                        PlannedAreaRing::Cycle { cycle, .. } => {
                            let cycle = self.cycles.get(cycle).ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q area plan references an unknown cycle".into(),
                                )
                            })?;
                            for &dart_id in &cycle.darts {
                                work.charge(1)?;
                                let dart = q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "Q cycle references an unknown dart".into(),
                                    )
                                })?;
                                let span = dart.span.0 as usize;
                                spans.push(span);
                                areal_spans.push(span);
                                for port_id in [dart.from_port, dart.to_port] {
                                    work.charge(1)?;
                                    let port =
                                        q.ports.get(port_id.0 as usize).ok_or_else(|| {
                                            GeometryErrorKind::Invalid(
                                                "Q cycle references an unknown port".into(),
                                            )
                                        })?;
                                    if !port.cut {
                                        live_nodes.push(port.node);
                                    }
                                }
                            }
                        },
                    }
                }
            },
        }
        for line in &self.line_paths {
            work.charge(1)?;
            if line.origin == QLineOrigin::ArealBoundary {
                for step in &line.path {
                    work.charge(1)?;
                    areal_spans.push(step.span);
                }
            }
            for step in &line.path {
                work.charge(1)?;
                spans.push(step.span);
                let span = q.spans.get(step.span).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q line plan references an unknown span".into())
                })?;
                for port_id in span.ports {
                    work.charge(1)?;
                    let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q line plan references an unknown port".into())
                    })?;
                    if !port.cut {
                        live_nodes.push(port.node);
                    }
                }
            }
        }
        for recipe in self.fragment_recipes.as_slice() {
            work.charge(1)?;
            for port_id in recipe.endpoints {
                work.charge(1)?;
                let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q emission recipe endpoint references an unknown port".into(),
                    )
                })?;
                live_nodes.push(port.node);
            }
        }
        work.charge(
            u64::try_from(self.point_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        live_nodes.extend_from_slice(&self.point_nodes);
        try_sort_by_with_work(&mut spans, work, |left, right, _work| Ok(left.cmp(right)))?;
        work.charge(u64::try_from(spans.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        spans.dedup();
        try_sort_by_with_work(&mut areal_spans, work, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        work.charge(u64::try_from(areal_spans.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        areal_spans.dedup();
        try_sort_by_with_work(&mut live_nodes, work, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        work.charge(u64::try_from(live_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        live_nodes.dedup();

        let mut selected_ports = Vec::new();
        for &span_index in &spans {
            work.charge(1)?;
            let span = q.spans.get(span_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q embedding index references an unknown span".into())
            })?;
            for port_id in span.ports {
                work.charge(1)?;
                let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q embedding index references an unknown port".into(),
                    )
                })?;
                selected_ports.push((port.node, port_id));
            }
        }
        for &node_id in &self.point_nodes {
            work.charge(1)?;
            let node = q.nodes.get(node_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q embedding index references an unknown point".into())
            })?;
            for &port_id in q.node_ports.get(node.ports.clone()).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q embedding point has an invalid port run".into())
            })? {
                work.charge(1)?;
                selected_ports.push((node_id, port_id));
            }
        }
        try_sort_by_with_work(&mut selected_ports, work, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        work.charge(u64::try_from(selected_ports.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        selected_ports.dedup();
        let mut selected_nodes = selected_ports
            .iter()
            .map(|(node, _)| *node)
            .collect::<Vec<_>>();
        try_sort_by_with_work(&mut selected_nodes, work, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        work.charge(u64::try_from(selected_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        selected_nodes.dedup();
        let cut_nodes = build_cut_node_ports(topology, &spans, &selected_ports, work)?;
        let source_nodes = EmissionNodeRegistry::with_sources(topology, &live_nodes, work)?;
        Ok(QEmbeddingEmission {
            plan: self,
            force_native: self.force_native,
            source_points,
            selected_regions: &self.selected_regions,
            selected_face_roots: &self.selected_face_roots,
            fragment_recipes: self.fragment_recipes.as_slice(),
            fragment_walks: self.fragment_recipes.walks(),
            publication,
            source_nodes,
            spans,
            areal_spans,
            point_nodes: &self.point_nodes,
            live_nodes,
            selected_ports,
            selected_nodes,
            cut_nodes,
        })
    }

    pub(super) fn source_reuse_is_proven(
        &self,
        topology: &ExactGeographicTopology,
        drawing: &QEmissionDrawing<'_>,
        source_points: &[Option<EmittedVertex<'_>>],
        work: &mut impl Work,
    ) -> Result<bool> {
        let walks = super::emission::index_recipe_walks(self.fragment_recipes.walks(), work)?;
        let mut cursors = BTreeMap::new();
        match (&self.polygons, &drawing.polygons) {
            (PolygonPlan::WholeSphere(selected), DrawnPolygons::WholeSphere(polygons)) => {
                if selected.len() != polygons.len() {
                    return Ok(false);
                }
                for (&polygon_index, rings) in selected.iter().zip(polygons) {
                    work.charge(1)?;
                    let source_polygon =
                        topology
                            .sources
                            .polygons
                            .get(polygon_index)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "whole-sphere source reuse polygon is unknown".into(),
                                )
                            })?;
                    if rings.len() != source_polygon.rings.len() {
                        return Ok(false);
                    }
                    for (ring_index, drawn) in source_polygon.rings.clone().zip(rings) {
                        work.charge(1)?;
                        let source_ring =
                            topology.sources.rings.get(ring_index).ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "whole-sphere source reuse ring is unknown".into(),
                                )
                            })?;
                        if source_ring.polygon.0 as usize != polygon_index
                            || drawn.hole != source_ring.hole
                            || drawn.recipe_walk != RecipeWalkOwner::SourceRing(ring_index)
                            || !source_ring_reuse_matches(
                                topology,
                                source_ring,
                                false,
                                &drawn.vertices,
                                self.fragment_recipes.as_slice(),
                                super::emission::next_recipe_walk(
                                    &walks,
                                    &mut cursors,
                                    drawn.recipe_walk,
                                    work,
                                )?,
                            )?
                        {
                            return Ok(false);
                        }
                    }
                }
            },
            (
                PolygonPlan::Regions { rings: planned, .. },
                DrawnPolygons::Regions { rings: drawn, .. },
            ) => {
                if planned.len() != drawn.len() {
                    return Ok(false);
                }
                for (planned, drawn) in planned.iter().zip(drawn) {
                    work.charge(1)?;
                    let walk = super::emission::next_recipe_walk(
                        &walks,
                        &mut cursors,
                        drawn.recipe_walk,
                        work,
                    )?;
                    let PlannedAreaRing::Source {
                        ring,
                        region,
                        reverse,
                        cycle_start,
                    } = *planned
                    else {
                        return Ok(false);
                    };
                    let source_ring = topology.sources.rings.get(ring).ok_or_else(|| {
                        GeometryErrorKind::Invalid("source reuse ring is unknown".into())
                    })?;
                    let source_ring_matches = source_ring_reuse_matches(
                        topology,
                        source_ring,
                        reverse,
                        &drawn.vertices,
                        self.fragment_recipes.as_slice(),
                        walk,
                    )?;
                    if !matches!(drawn.origin, DrawnAreaOrigin::Source)
                        || drawn.region != region
                        || drawn.recipe_walk != RecipeWalkOwner::SourceRing(ring)
                        || drawn.native_start_port != cycle_start
                        || !source_ring_matches
                    {
                        return Ok(false);
                    }
                }
            },
            _ => return Ok(false),
        }
        if self.line_paths.len() != drawing.lines.len()
            || self.point_nodes.len() != drawing.points.len()
        {
            return Ok(false);
        }
        for (index, (planned, drawn)) in self.line_paths.iter().zip(&drawing.lines).enumerate() {
            work.charge(1)?;
            let owner = RecipeWalkOwner::Line(index);
            if planned.recipe_walk != owner || drawn.recipe_walk != owner {
                return Ok(false);
            }
            let walk = super::emission::next_recipe_walk(&walks, &mut cursors, owner, work)?;
            if !super::q_embedding::source_line_reuse_is_complete(
                topology,
                self.fragment_recipes.as_slice(),
                None,
                walk,
                &drawn.path,
                work,
            )? {
                return Ok(false);
            }
        }
        for (&node, drawn) in self.point_nodes.iter().zip(&drawing.points) {
            work.charge(1)?;
            let source = source_points.get(node.0 as usize).copied().flatten();
            if source.is_none_or(|source| {
                source.port.node != node
                    || source.port.id != drawn.port.id
                    || source.point != drawn.point
            }) {
                return Ok(false);
            }
        }
        for (owner, owner_walks) in &walks {
            work.charge(1)?;
            if cursors.get(owner).copied().unwrap_or_default() != owner_walks.len() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn drawing<'topology>(
        &self,
        topology: &'topology ExactGeographicTopology,
        coordinates: &QCoordinates,
        source_points: &[Option<EmittedVertex<'topology>>],
        work: &mut impl Work,
    ) -> Result<QEmissionDrawing<'topology>> {
        let polygons = match &self.polygons {
            PolygonPlan::WholeSphere(source_polygons) => {
                let mut drawn_polygons = Vec::with_capacity(source_polygons.len());
                for &polygon_index in source_polygons {
                    work.charge(1)?;
                    let polygon =
                        topology
                            .sources
                            .polygons
                            .get(polygon_index)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q whole-sphere plan references an unknown polygon".into(),
                                )
                            })?;
                    let mut rings = Vec::with_capacity(polygon.rings.len());
                    for ring_index in polygon.rings.clone() {
                        work.charge(1)?;
                        let ring = topology.sources.rings.get(ring_index).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q whole-sphere plan references an unknown ring".into(),
                            )
                        })?;
                        let vertices = source_ring_emitted_vertices(topology, ring, false, work)?;
                        rings.push(DrawnSourceRing {
                            hole: ring.hole,
                            recipe_walk: RecipeWalkOwner::SourceRing(ring_index),
                            vertices,
                        });
                    }
                    drawn_polygons.push(rings);
                }
                DrawnPolygons::WholeSphere(drawn_polygons)
            },
            PolygonPlan::Regions {
                rings,
                region_count,
            } => {
                let mut drawn_rings = Vec::with_capacity(rings.len());
                for planned in rings {
                    work.charge(1)?;
                    let (region, origin, recipe_walk, native_start_port, vertices) = match *planned
                    {
                        PlannedAreaRing::Source {
                            ring,
                            region,
                            reverse,
                            cycle_start,
                        } => {
                            let source_ring =
                                topology.sources.rings.get(ring).ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "Q area plan references an unknown source ring".into(),
                                    )
                                })?;
                            let vertices =
                                source_ring_emitted_vertices(topology, source_ring, reverse, work)?;
                            (
                                region,
                                DrawnAreaOrigin::Source,
                                RecipeWalkOwner::SourceRing(ring),
                                cycle_start,
                                vertices,
                            )
                        },
                        PlannedAreaRing::Cycle {
                            cycle: cycle_index,
                            region,
                        } => {
                            let cycle = self.cycles.get(cycle_index).ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q area plan references an unknown cycle".into(),
                                )
                            })?;
                            let vertices = draw_cycle_vertices(topology, coordinates, cycle, work)?;
                            (
                                region,
                                DrawnAreaOrigin::Cycle,
                                RecipeWalkOwner::Cycle(cycle_index),
                                vertices.first().map(|vertex| vertex.port.id),
                                vertices,
                            )
                        },
                    };
                    drawn_rings.push(DrawnAreaRing {
                        region,
                        origin,
                        recipe_walk,
                        native_start_port,
                        vertices,
                    });
                }
                DrawnPolygons::Regions {
                    rings: drawn_rings,
                    region_count: *region_count,
                }
            },
        };
        let lines = draw_lines(topology, &self.line_paths, coordinates, work)?;
        let points = draw_points(
            topology,
            &self.point_nodes,
            source_points,
            coordinates,
            work,
        )?;
        Ok(QEmissionDrawing {
            polygons,
            lines,
            points,
        })
    }
}

fn build_fragment_recipes(
    topology: &ExactGeographicTopology,
    polygons: &PolygonPlan,
    cycles: &[BoundaryCycle],
    line_paths: &[PlannedQLine],
    work: &mut impl Work,
) -> Result<FragmentRecipes> {
    let mut recipes = FragmentRecipes::default();
    match polygons {
        PolygonPlan::WholeSphere(source_polygons) => {
            for &polygon_index in source_polygons {
                work.charge(1)?;
                let polygon = topology
                    .sources
                    .polygons
                    .get(polygon_index)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q whole-sphere recipe polygon is unknown".into(),
                        )
                    })?;
                for ring_index in polygon.rings.clone() {
                    work.charge(1)?;
                    let ring = topology.sources.rings.get(ring_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q whole-sphere recipe ring is unknown".into())
                    })?;
                    let fragments =
                        push_source_ring_recipes(topology, ring, false, &mut recipes, work)?;
                    recipes.record_walk(RecipeWalkOwner::SourceRing(ring_index), fragments);
                }
            }
        },
        PolygonPlan::Regions { rings, .. } => {
            for planned in rings {
                work.charge(1)?;
                match *planned {
                    PlannedAreaRing::Source { ring, reverse, .. } => {
                        let source_ring = topology.sources.rings.get(ring).ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q source recipe ring is unknown".into())
                        })?;
                        let fragments = push_source_ring_recipes(
                            topology,
                            source_ring,
                            reverse,
                            &mut recipes,
                            work,
                        )?;
                        recipes.record_walk(RecipeWalkOwner::SourceRing(ring), fragments);
                    },
                    PlannedAreaRing::Cycle {
                        cycle: cycle_index, ..
                    } => {
                        let cycle = cycles.get(cycle_index).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q recipe references an unknown boundary cycle".into(),
                            )
                        })?;
                        let mut spans = Vec::with_capacity(cycle.darts.len());
                        for &dart_id in &cycle.darts {
                            work.charge(1)?;
                            let dart =
                                topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "Q recipe references an unknown dart".into(),
                                    )
                                })?;
                            let span =
                                topology.q.spans.get(dart.span.0 as usize).ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "Q recipe references an unknown span".into(),
                                    )
                                })?;
                            spans.push(DirectedSpan {
                                span: span.id,
                                from: dart.from_port,
                                to: dart.to_port,
                                owner_index: area_owner_index(
                                    topology,
                                    dart.span.0 as usize,
                                    work,
                                )?,
                            });
                        }
                        let mut pivot = None;
                        for (index, span) in spans.iter().enumerate() {
                            work.charge(1)?;
                            if topology
                                .q
                                .ports
                                .get(span.from.0 as usize)
                                .is_some_and(|port| !port.cut)
                            {
                                pivot = Some(index);
                                break;
                            }
                        }
                        if let Some(pivot) = pivot {
                            spans.rotate_left(pivot);
                        }
                        let fragments = recipes.push_walk(
                            topology,
                            &spans,
                            true,
                            SelectedUse::AreaBoundary,
                            work,
                        )?;
                        recipes.record_walk(RecipeWalkOwner::Cycle(cycle_index), fragments);
                    },
                }
            }
        },
    }
    for line in line_paths {
        work.charge(1)?;
        let mut spans = Vec::with_capacity(line.path.len());
        for step in &line.path {
            work.charge(1)?;
            let span = topology.q.spans.get(step.span).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line recipe span is unknown".into())
            })?;
            let (from, to) = match step.from_endpoint {
                SpanEndpoint::Start => (span.ports[0], span.ports[1]),
                SpanEndpoint::End => (span.ports[1], span.ports[0]),
            };
            spans.push(DirectedSpan {
                span: span.id,
                from,
                to,
                owner_index: step.owner,
            });
        }
        let mut pivot = None;
        for (index, span) in spans.iter().enumerate() {
            work.charge(1)?;
            if topology
                .q
                .ports
                .get(span.from.0 as usize)
                .is_some_and(|port| !port.cut)
            {
                pivot = Some(index);
                break;
            }
        }
        if let Some(pivot) = pivot {
            spans.rotate_left(pivot);
        }
        let closed = if let (Some(first), Some(last)) = (spans.first(), spans.last()) {
            work.charge(1)?;
            let first = topology.q.ports.get(first.from.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line recipe start is unknown".into())
            })?;
            let last =
                topology.q.ports.get(last.to.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q line recipe end is unknown".into())
                })?;
            first.node == last.node
        } else {
            false
        };
        let use_kind = match line.origin {
            QLineOrigin::Curve => SelectedUse::Curve,
            QLineOrigin::ArealBoundary => SelectedUse::ArealBoundaryLine,
        };
        let fragments = recipes.push_walk(topology, &spans, closed, use_kind, work)?;
        recipes.record_walk(line.recipe_walk, fragments);
    }
    Ok(recipes)
}

fn push_source_ring_recipes(
    topology: &ExactGeographicTopology,
    ring: &ExactSourceRing,
    reverse: bool,
    recipes: &mut FragmentRecipes,
    work: &mut impl Work,
) -> Result<Vec<super::emission::FragmentRecipeId>> {
    let mut fragments = Vec::with_capacity(ring.source_edges.len());
    if reverse {
        for edge_index in ring.source_edges.clone().rev() {
            fragments.push(push_source_edge_recipe(
                topology, edge_index, true, recipes, work,
            )?);
        }
    } else {
        for edge_index in ring.source_edges.clone() {
            fragments.push(push_source_edge_recipe(
                topology, edge_index, false, recipes, work,
            )?);
        }
    }
    Ok(fragments)
}

fn push_source_edge_recipe(
    topology: &ExactGeographicTopology,
    edge_index: usize,
    reverse: bool,
    recipes: &mut FragmentRecipes,
    work: &mut impl Work,
) -> Result<super::emission::FragmentRecipeId> {
    let one = ExactRational::one();
    let zero = ExactRational::zero();
    let period = ExactRational::from_f64(topology.sources.frame.longitude().period());
    let edge = topology
        .sources
        .edges
        .get(edge_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q source recipe edge is unknown".into()))?;
    let ports = topology
        .q
        .source_edge_ports
        .get(edge.id.0 as usize)
        .copied()
        .ok_or_else(|| GeometryErrorKind::Invalid("Q source recipe ports are unknown".into()))?;
    let retained = edge.retained_longitude();
    let [start, end] = retained.endpoints();
    let native_delta =
        ExactRational::from_f64(end.get()).try_subtract(&ExactRational::from_f64(start.get()))?;
    let lift = retained.periods();
    let period_lift = ExactRational::from_dyadic(
        crate::geometry::tessellation::exact::ExactDyadic::from_i64(lift),
    );
    let mut displacement = native_delta.try_add(&period.try_product(&period_lift)?)?;
    let (endpoints, range) = if reverse {
        displacement = ExactRational::zero().try_subtract(&displacement)?;
        ([ports[1], ports[0]], ExactSourceRange {
            start: one,
            end: zero,
        })
    } else {
        (ports, ExactSourceRange {
            start: zero,
            end: one,
        })
    };
    recipes.push_source_edge(
        endpoints,
        edge.id,
        range,
        displacement,
        SelectedUse::AreaBoundary,
        work,
    )
}

impl PolygonPlan {
    fn build(
        topology: &ExactGeographicTopology,
        whole_sphere: bool,
        operation: OverlayOp,
        cycles: &[BoundaryCycle],
        work: &mut impl Work,
    ) -> Result<Self> {
        if whole_sphere {
            let mut source_polygons = Vec::new();
            for (index, source_polygon) in topology.sources.polygons.iter().enumerate() {
                work.charge(1)?;
                if !source_polygon_contributes_to_whole_sphere(source_polygon, operation) {
                    continue;
                }
                let mut has_areal_shell = false;
                for ring_index in source_polygon.rings.clone() {
                    work.charge(1)?;
                    if topology
                        .sources
                        .rings
                        .get(ring_index)
                        .is_some_and(|ring| !ring.hole && ring.has_areal_winding())
                    {
                        has_areal_shell = true;
                        break;
                    }
                }
                if has_areal_shell {
                    source_polygons.push(index);
                }
            }
            return Ok(Self::WholeSphere(source_polygons));
        }

        let mut rings = Vec::new();
        work.charge(u64::try_from(cycles.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut emitted_cycles = vec![false; cycles.len()];
        let source_cycles_by_ring = source_cycles_by_ring(topology, cycles, work)?;
        for (ring_index, source_ring) in topology.sources.rings.iter().enumerate() {
            work.charge(1)?;
            let source_cycles = source_cycles_by_ring.get(ring_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source-ring cycle index is incomplete".into())
            })?;
            if source_cycles.is_empty() {
                continue;
            }
            if source_ring.summary.exact_axis_band && source_cycles.len() != 2 {
                continue;
            }
            let Some(&first_cycle) = source_cycles.first() else {
                continue;
            };
            let region = cycles
                .get(first_cycle)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q source-ring cycle is unknown".into()))?
                .region;
            let mut cycles_are_consistent = true;
            for &cycle_index in source_cycles {
                work.charge(1)?;
                let cycle = cycles.get(cycle_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source-ring cycle is unknown".into())
                })?;
                let emitted = emitted_cycles.get(cycle_index).copied().ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source-ring cycle has no emission state".into())
                })?;
                if cycle.region != region || emitted {
                    cycles_are_consistent = false;
                    break;
                }
            }
            if !cycles_are_consistent
                || !source_ring_cycles_cover_boundary(
                    topology,
                    source_ring,
                    cycles,
                    &source_cycles,
                    work,
                )?
            {
                continue;
            }
            let Some(follows_source) = source_ring_cycles_follow_source(
                topology,
                source_ring,
                cycles,
                &source_cycles,
                work,
            )?
            else {
                continue;
            };
            let cycle_start = if source_cycles.len() == 1 {
                let cycle = cycles.get(first_cycle).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source-ring start cycle is unknown".into())
                })?;
                Some(source_cycle_start_port(topology, cycle, work)?)
            } else {
                None
            };
            rings.push(PlannedAreaRing::Source {
                ring: ring_index,
                region,
                reverse: !follows_source,
                cycle_start,
            });
            for &cycle_index in source_cycles {
                work.charge(1)?;
                *emitted_cycles.get_mut(cycle_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source-ring cycle has no emission state".into())
                })? = true;
            }
        }
        for (cycle_index, (cycle, &emitted)) in cycles.iter().zip(&emitted_cycles).enumerate() {
            work.charge(1)?;
            if !emitted {
                rings.push(PlannedAreaRing::Cycle {
                    cycle: cycle_index,
                    region: cycle.region,
                });
            }
        }
        Ok(Self::Regions {
            rings,
            region_count: topology.q.global_face_count,
        })
    }
}

const fn selects_support(
    count: usize,
    first: bool,
    row_count: usize,
    operation: OverlayOp,
) -> bool {
    match operation {
        OverlayOp::Union => count != 0,
        OverlayOp::Intersection => count == row_count,
        OverlayOp::Difference => first && count == 1,
        OverlayOp::SymmetricDifference => !count.is_multiple_of(2),
    }
}

/// Incident faces can differ only on rows owning an incident source edge.
/// Count their union from one face and those rows, without expanding ancestors.
#[derive(Default)]
struct AreaSupport {
    faces: Vec<GlobalFaceId>,
    rows: Vec<SourceRowId>,
}

impl AreaSupport {
    fn clear(&mut self) {
        self.faces.clear();
        self.rows.clear();
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "span indices come from this topology's span table or admitted darts"
    )]
    fn append_span(
        &mut self,
        topology: &ExactGeographicTopology,
        span: usize,
        work: &mut impl Work,
    ) -> Result<()> {
        for owner in &topology.q.spans[span].owners {
            work.charge(1)?;
            let edge = topology
                .sources
                .edges
                .get(owner.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q support references an unknown source edge".into())
                })?;
            self.rows.push(edge.row);
        }
        Ok(())
    }

    fn contains(
        &self,
        q: &super::q::ExactGeographicQ,
        row: SourceRowId,
        work: &mut impl Work,
    ) -> Result<bool> {
        for &face in &self.faces {
            work.charge(1)?;
            if q.face_rows.contains(face, row) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the union counts distinct admitted source rows"
    )]
    fn count(&mut self, q: &super::q::ExactGeographicQ, work: &mut impl Work) -> Result<usize> {
        let Some(&first) = self.faces.first() else {
            return Ok(0);
        };
        try_sort_by_with_work(&mut self.rows, work, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        work.charge(u64::try_from(self.rows.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        self.rows.dedup();
        let mut count = q.face_rows.count(first);
        for &row in &self.rows {
            work.charge(1)?;
            if !q.face_rows.contains(first, row) && self.contains(q, row, work)? {
                count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            }
        }
        Ok(count)
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "sorted unique support rows add only rows absent from the area union"
    )]
    fn with_rows(
        &mut self,
        q: &super::q::ExactGeographicQ,
        rows: &[SourceRowId],
        work: &mut impl Work,
    ) -> Result<(usize, bool)> {
        let mut count = self.count(q, work)?;
        for &row in rows {
            work.charge(1)?;
            if !self.contains(q, row, work)? {
                count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            }
        }
        work.charge(1)?;
        let first = rows.binary_search(&SourceRowId(0)).is_ok()
            || self.contains(q, SourceRowId(0), work)?;
        Ok((count, first))
    }
}

fn append_unique_rows(
    output: &mut Vec<SourceRowId>,
    left: &[SourceRowId],
    right: &[SourceRowId],
    work: &mut impl Work,
) -> Result<()> {
    output.clear();
    work.charge(
        u64::try_from(
            left.len()
                .checked_add(right.len())
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    output.extend_from_slice(left);
    output.extend_from_slice(right);
    try_sort_by_with_work(output, work, |left, right, _work| Ok(left.cmp(right)))?;
    work.charge(u64::try_from(output.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    output.dedup();
    Ok(())
}

fn select_faces(
    q: &super::q::ExactGeographicQ,
    row_count: usize,
    operation: OverlayOp,
    work: &mut impl Work,
) -> Result<Vec<bool>> {
    let mut selected = Vec::with_capacity(q.global_face_count);
    for face in 0..q.global_face_count {
        work.charge(1)?;
        let face = GlobalFaceId(u32::try_from(face).map_err(|_| ResourceError::SizeOverflow)?);
        selected.push(selects_support(
            q.face_rows.count(face),
            q.face_rows.contains(face, SourceRowId(0)),
            row_count,
            operation,
        ));
    }
    Ok(selected)
}

pub(super) struct FaceUnion {
    parents: Vec<usize>,
    selected: Vec<bool>,
}

impl FaceUnion {
    fn new(selected: &[bool], work: &mut impl Work) -> Result<Self> {
        work.charge(
            u64::try_from(
                selected
                    .len()
                    .checked_mul(2)
                    .ok_or(ResourceError::SizeOverflow)?,
            )
            .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        Ok(Self {
            parents: (0..selected.len()).collect(),
            selected: selected.to_vec(),
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "Face IDs enter the union only from its selected-face table"
    )]
    fn find(&mut self, mut face: usize, work: &mut impl Work) -> Result<usize> {
        while self.parents[face] != face {
            work.charge(1)?;
            self.parents[face] = self.parents[self.parents[face]];
            face = self.parents[face];
        }
        Ok(face)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "Both joined face IDs were admitted against the selected-face table"
    )]
    fn join(&mut self, left: usize, right: usize, work: &mut impl Work) -> Result<()> {
        let (left, right) = (self.find(left, work)?, self.find(right, work)?);
        if left != right {
            self.parents[left.max(right)] = left.min(right);
        }
        Ok(())
    }

    fn selected_root(&mut self, face: GlobalFaceId, work: &mut impl Work) -> Result<usize> {
        let index = face.0 as usize;
        let selected = self.selected.get(index).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q boundary references an unknown global face".into())
        })?;
        if !selected {
            return Err(GeometryErrorKind::Invalid(
                "Q boundary references an unselected global face".into(),
            )
            .into());
        }
        self.find(index, work)
    }
}

fn face_regions(
    q: &super::q::ExactGeographicQ,
    selected: &[bool],
    work: &mut impl Work,
) -> Result<FaceUnion> {
    face_regions_with_owner_labels(q, selected, None, work)
}

pub(super) fn coverage_face_regions(
    q: &super::q::ExactGeographicQ,
    face_owners: &[Option<usize>],
    work: &mut impl Work,
) -> Result<FaceUnion> {
    if face_owners.len() != q.global_face_count {
        return Err(GeometryErrorKind::Invalid(
            "coverage face owners do not match Q's global face table".into(),
        )
        .into());
    }
    work.charge(u64::try_from(face_owners.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let selected = face_owners.iter().map(Option::is_some).collect::<Vec<_>>();
    face_regions_with_owner_labels(q, &selected, Some(face_owners), work)
}

fn face_regions_with_owner_labels(
    q: &super::q::ExactGeographicQ,
    selected: &[bool],
    owner_labels: Option<&[Option<usize>]>,
    work: &mut impl Work,
) -> Result<FaceUnion> {
    if selected.len() != q.global_face_count {
        return Err(GeometryErrorKind::Invalid(
            "Q face selection does not match its global face table".into(),
        )
        .into());
    }
    if owner_labels.is_some_and(|owners| owners.len() != selected.len()) {
        return Err(GeometryErrorKind::Invalid(
            "Q face owner labels do not match its global face table".into(),
        )
        .into());
    }
    let mut regions = FaceUnion::new(selected, work)?;
    for faces in &q.span_global_faces {
        work.charge(1)?;
        let Some([left, right]) = faces else {
            continue;
        };
        let left = left.0 as usize;
        let right = right.0 as usize;
        let left_selected = selected.get(left).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q span references an unknown global face".into())
        })?;
        let right_selected = selected.get(right).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q span references an unknown global face".into())
        })?;
        let same_owner = if let Some(owners) = owner_labels {
            let left_owner = owners.get(left).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q span has no left face owner label".into())
            })?;
            let right_owner = owners.get(right).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q span has no right face owner label".into())
            })?;
            left_owner.is_some() && left_owner == right_owner
        } else {
            true
        };
        if left_selected && right_selected && same_owner {
            regions.join(left, right, work)?;
        }
    }
    Ok(regions)
}

pub(super) fn selected_region_facts(
    q: &super::q::ExactGeographicQ,
    selected: &[bool],
    regions: &mut FaceUnion,
    region_roots: &[usize],
    work: &mut impl Work,
) -> Result<Vec<SelectedRegionFact>> {
    if selected.len() != q.global_face_count {
        return Err(GeometryErrorKind::Invalid(
            "Q face selection does not match its global face table".into(),
        )
        .into());
    }
    work.charge(u64::try_from(region_roots.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut roots = region_roots.to_vec();
    for &root in &roots {
        work.charge(1)?;
        if !selected.get(root).copied().unwrap_or(false) || regions.find(root, work)? != root {
            return Err(GeometryErrorKind::Invalid(
                "emitted region is not a selected FaceUnion root".into(),
            )
            .into());
        }
    }
    try_sort_by_with_work(&mut roots, work, |left, right, _work| Ok(left.cmp(right)))?;
    work.charge(u64::try_from(roots.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    roots.dedup();
    work.charge(u64::try_from(roots.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut facts = roots
        .into_iter()
        .map(|root| SelectedRegionFact {
            root,
            poles: SelectedRegionPoles::default(),
        })
        .collect::<Vec<_>>();
    if facts.is_empty() {
        return Ok(facts);
    }

    for pole in [ChartPole::North, ChartPole::South] {
        q.for_each_pole_sector_with_work(pole, work, |sector, work| {
            work.charge(1)?;
            let face = sector.face();
            if !selected.get(face.0 as usize).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q pole sector references an unknown face".into())
            })? {
                return Ok(());
            }
            let root = regions.selected_root(face, work)?;
            work.charge(1)?;
            let index = facts
                .binary_search_by_key(&root, |fact| fact.root)
                .map_err(|_| {
                    GeometryErrorKind::Invalid(
                        "selected Q pole sector has no retained region root".into(),
                    )
                })?;
            let poles = &mut facts
                .get_mut(index)
                .expect("binary search returned a selected-region fact")
                .poles;
            match pole {
                ChartPole::North => poles.north = true,
                ChartPole::South => poles.south = true,
            }
            Ok(())
        })?;
    }
    Ok(facts)
}

pub(super) fn selected_face_roots(
    q: &super::q::ExactGeographicQ,
    selected: &[bool],
    regions: &mut FaceUnion,
    work: &mut impl Work,
) -> Result<Vec<Option<usize>>> {
    if selected.len() != q.global_face_count {
        return Err(GeometryErrorKind::Invalid(
            "Q face selection does not match its global face table".into(),
        )
        .into());
    }
    let mut roots = Vec::with_capacity(selected.len());
    for (index, &is_selected) in selected.iter().enumerate() {
        work.charge(1)?;
        if !is_selected {
            roots.push(None);
            continue;
        }
        let face = GlobalFaceId(u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?);
        roots.push(Some(regions.selected_root(face, work)?));
    }
    Ok(roots)
}

#[expect(
    clippy::indexing_slicing,
    reason = "global face indices and the selected-face table belong to the same admitted Q"
)]
fn select_line_spans(
    topology: &ExactGeographicTopology,
    selected_faces: &[bool],
    row_count: usize,
    operation: OverlayOp,
    work: &mut impl Work,
) -> Result<Vec<Option<SelectedLineSpan>>> {
    let q = &topology.q;
    let mut selected = Vec::with_capacity(q.spans.len());
    let mut area = AreaSupport::default();
    let mut curve_rows = Vec::new();
    for (span_index, faces) in q.span_global_faces.iter().enumerate() {
        work.charge(1)?;
        if q.span_is_collapsed_pole(span_index)? {
            selected.push(None);
            continue;
        }
        area.clear();
        if let Some([left, right]) = faces {
            if selected_faces[left.0 as usize] || selected_faces[right.0 as usize] {
                selected.push(None);
                continue;
            }
            area.faces.extend([*left, *right]);
            area.append_span(topology, span_index, work)?;
        }
        append_unique_rows(
            &mut curve_rows,
            q.line_rows_for_span(span_index)?,
            q.collapsed_rows_for_span(span_index)?,
            work,
        )?;
        let curve_selected = match operation {
            OverlayOp::Union => !curve_rows.is_empty(),
            OverlayOp::Intersection => area.with_rows(q, &curve_rows, work)?.0 == row_count,
            OverlayOp::Difference => {
                curve_rows.binary_search(&SourceRowId(0)).is_ok()
                    && area.with_rows(q, &curve_rows, work)?.0 == 1
            },
            OverlayOp::SymmetricDifference => !curve_rows.len().is_multiple_of(2),
        };
        selected.push(if curve_selected {
            Some(SelectedLineSpan {
                owner: line_owner_index(topology, span_index, work)?,
            })
        } else {
            None
        });
    }
    Ok(selected)
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "node, dart, local and global face indices and fan offsets are admitted by this Q"
)]
fn select_points(
    topology: &ExactGeographicTopology,
    selected_faces: &[bool],
    selected_spans: &[Option<SelectedLineSpan>],
    row_count: usize,
    operation: OverlayOp,
    work: &mut impl Work,
) -> Result<Vec<QNodeId>> {
    let q = &topology.q;
    work.charge(u64::try_from(q.nodes.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut line_nodes = vec![false; q.nodes.len()];
    for (span, &selected) in q.spans.iter().zip(selected_spans) {
        work.charge(1)?;
        if selected.is_some() {
            for node in span.nodes {
                work.charge(1)?;
                *line_nodes.get_mut(node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q span references an unknown node".into())
                })? = true;
            }
        }
    }

    let mut output = Vec::new();
    let mut area = AreaSupport::default();
    let mut support_rows = Vec::new();
    for (node_index, node) in q.nodes.iter().enumerate() {
        work.charge(1)?;
        if line_nodes[node_index] {
            continue;
        }
        area.clear();
        support_rows.clear();
        let fan = &q.outgoing[q.outgoing_offsets[node_index]..q.outgoing_offsets[node_index + 1]];
        if fan.is_empty() {
            let face = q.source_node_faces[node_index].ok_or_else(|| {
                GeometryErrorKind::Invalid("an isolated Q node has no host face".into())
            })?;
            area.faces.push(face);
        } else {
            for &dart in fan {
                work.charge(1)?;
                let span_index = q.darts[dart.0 as usize].span.0 as usize;
                let line_rows = q.line_rows_for_span(span_index)?;
                let collapsed_rows = q.collapsed_rows_for_span(span_index)?;
                work.charge(
                    u64::try_from(
                        line_rows
                            .len()
                            .checked_add(collapsed_rows.len())
                            .ok_or(ResourceError::SizeOverflow)?,
                    )
                    .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                support_rows.extend_from_slice(line_rows);
                support_rows.extend_from_slice(collapsed_rows);
                let local = q.dart_faces[dart.0 as usize];
                area.faces.push(q.global_face_of_local[local.0 as usize]);
                area.append_span(topology, span_index, work)?;
            }
        }
        let mut face_selected = false;
        for &face in &area.faces {
            work.charge(1)?;
            if selected_faces[face.0 as usize] {
                face_selected = true;
                break;
            }
        }
        if face_selected {
            continue;
        }
        let point_rows = q.point_rows_for_node(node_index)?;
        work.charge(u64::try_from(point_rows.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        support_rows.extend_from_slice(point_rows);
        try_sort_by_with_work(&mut support_rows, work, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        work.charge(u64::try_from(support_rows.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        support_rows.dedup();
        let (count, first) = area.with_rows(q, &support_rows, work)?;
        if selects_support(count, first, row_count, operation) {
            output.push(node.id);
        }
    }
    Ok(output)
}

fn selected_area_boundaries(
    q: &super::q::ExactGeographicQ,
    selected_faces: &[bool],
    regions: &mut FaceUnion,
    work: &mut impl Work,
) -> Result<Vec<BoundaryCycle>> {
    if selected_faces.len() != q.global_face_count {
        return Err(GeometryErrorKind::Invalid(
            "Q face selection does not match its global face table".into(),
        )
        .into());
    }
    let mut boundary = vec![false; q.darts.len()];
    for (span_index, faces) in q.span_global_faces.iter().enumerate() {
        work.charge(1)?;
        let Some([left, right]) = faces else {
            continue;
        };
        let left_selected = selected_faces
            .get(left.0 as usize)
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown left face".into()))?;
        let right_selected = selected_faces
            .get(right.0 as usize)
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown right face".into()))?;
        if left_selected == right_selected {
            continue;
        }
        let forward = q
            .span_darts
            .get(span_index)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("a selected Q face boundary has no dart".into())
            })?;
        let dart = if left_selected {
            forward
        } else {
            QDartId(forward.0 ^ 1)
        };
        let slot = boundary.get_mut(dart.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("a Q face boundary dart is unknown".into())
        })?;
        *slot = true;
    }

    Ok(boundary_cycles_by_owner(
        q,
        |dart| {
            boundary
                .get(dart.0 as usize)
                .copied()
                .map(|selected| selected.then_some(0))
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q overlay boundary dart is unknown".into()).into()
                })
        },
        regions,
        work,
    )?
    .into_iter()
    .map(|(_, cycle)| cycle)
    .collect())
}

pub(super) fn coverage_area_boundaries(
    q: &super::q::ExactGeographicQ,
    face_owners: &[Option<usize>],
    regions: &mut FaceUnion,
    work: &mut impl Work,
) -> Result<Vec<(usize, BoundaryCycle)>> {
    if face_owners.len() != q.global_face_count {
        return Err(GeometryErrorKind::Invalid(
            "coverage face owners do not match Q's global face table".into(),
        )
        .into());
    }
    let mut boundary = vec![None; q.darts.len()];
    for (span_index, faces) in q.span_global_faces.iter().enumerate() {
        work.charge(1)?;
        let Some([left, right]) = faces else {
            continue;
        };
        let left_owner = face_owners.get(left.0 as usize).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q span has an unknown left coverage owner".into())
        })?;
        let right_owner = face_owners.get(right.0 as usize).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q span has an unknown right coverage owner".into())
        })?;
        if left_owner == right_owner {
            continue;
        }
        let forward = q
            .span_darts
            .get(span_index)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("a coverage Q boundary has no dart".into())
            })?;
        if let Some(row) = left_owner {
            set_coverage_boundary_owner(&mut boundary, forward, row)?;
        }
        if let Some(row) = right_owner {
            set_coverage_boundary_owner(&mut boundary, QDartId(forward.0 ^ 1), row)?;
        }
    }

    boundary_cycles_by_owner(
        q,
        |dart| {
            let owner = boundary.get(dart.0 as usize).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage Q boundary dart is unknown".into())
            })?;
            let owner = owner
                .map(|owner| {
                    owner
                        .get()
                        .checked_sub(1)
                        .ok_or(ResourceError::SizeOverflow)
                })
                .transpose()?;
            Ok(owner)
        },
        regions,
        work,
    )
}

fn set_coverage_boundary_owner(
    boundary: &mut [Option<NonZeroUsize>],
    dart: QDartId,
    row: usize,
) -> Result<()> {
    let encoded_row = row
        .checked_add(1)
        .and_then(NonZeroUsize::new)
        .ok_or(ResourceError::SizeOverflow)?;
    let slot = boundary
        .get_mut(dart.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("coverage Q boundary dart is unknown".into()))?;
    if slot.replace(encoded_row).is_some() {
        return Err(GeometryErrorKind::Invalid(
            "a coverage Q boundary dart was selected more than once".into(),
        )
        .into());
    }
    Ok(())
}

fn boundary_cycles_by_owner(
    q: &super::q::ExactGeographicQ,
    mut owner_of: impl FnMut(QDartId) -> Result<Option<usize>>,
    regions: &mut FaceUnion,
    work: &mut impl Work,
) -> Result<Vec<(usize, BoundaryCycle)>> {
    work.charge(
        u64::try_from(
            q.darts
                .len()
                .checked_mul(2)
                .and_then(|darts| darts.checked_add(q.nodes.len()))
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut visited = vec![false; q.darts.len()];
    let mut positions = vec![None; q.nodes.len()];
    let mut walks = Vec::new();
    for seed in 0..q.darts.len() {
        work.charge(1)?;
        let start = QDartId(u32::try_from(seed).map_err(|_| ResourceError::SizeOverflow)?);
        let Some(owner) = owner_of(start)? else {
            continue;
        };
        let was_visited = visited.get(seed).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q overlay boundary has no visited state".into())
        })?;
        if was_visited {
            continue;
        }
        let mut current = start;
        let mut walk = Vec::new();
        loop {
            work.charge(1)?;
            let current_index = current.0 as usize;
            if visited.get(current_index).copied().unwrap_or(false) {
                if current == start {
                    break;
                }
                return Err(GeometryErrorKind::Invalid(
                    "Q overlay boundary walks intersect before closing".into(),
                )
                .into());
            }
            *visited.get_mut(current_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q overlay boundary dart is unknown".into())
            })? = true;
            let dart = *q.darts.get(current_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q overlay boundary dart is unknown".into())
            })?;
            *positions
                .get_mut(dart.from_node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q boundary starts at an unknown node".into())
                })? = Some(walk.len());
            walk.push(current);

            if let Some(begin) = positions.get(dart.to_node.0 as usize).copied().flatten() {
                let mut cycle = Vec::with_capacity(walk.len().saturating_sub(begin));
                for dart in walk.drain(begin..) {
                    work.charge(1)?;
                    let node = q
                        .darts
                        .get(dart.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q boundary cycle has an unknown dart".into(),
                            )
                        })?
                        .from_node;
                    *positions.get_mut(node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q boundary cycle has an unknown node".into())
                    })? = None;
                    cycle.push(dart);
                }
                if !cycle.is_empty() {
                    walks.push((owner, cycle));
                }
            }
            current = next_boundary_dart_by_owner(q, current, owner, &mut owner_of, work)?;
            if current == start {
                break;
            }
        }
        if !walk.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "Q overlay boundary did not decompose into closed cycles".into(),
            )
            .into());
        }
    }
    for (index, was_visited) in visited.into_iter().enumerate() {
        work.charge(1)?;
        let dart = QDartId(u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?);
        if owner_of(dart)?.is_some() && !was_visited {
            return Err(GeometryErrorKind::Invalid(
                "Q overlay left a selected boundary dart unvisited".into(),
            )
            .into());
        }
    }

    let mut cycles = Vec::with_capacity(walks.len());
    for (owner, darts) in walks {
        work.charge(1)?;
        let first = *darts.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q overlay emitted an empty boundary cycle".into())
        })?;
        let local = *q.dart_faces.get(first.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q boundary cycle has no left face".into())
        })?;
        let face = *q
            .global_face_of_local
            .get(local.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary cycle left face has no global ID".into())
            })?;
        let region = regions.selected_root(face, work)?;
        cycles.push((owner, BoundaryCycle { darts, region }));
    }
    Ok(cycles)
}

fn next_boundary_dart_by_owner(
    q: &super::q::ExactGeographicQ,
    dart_id: QDartId,
    owner: usize,
    owner_of: &mut impl FnMut(QDartId) -> Result<Option<usize>>,
    work: &mut impl Work,
) -> Result<QDartId> {
    let dart = q
        .darts
        .get(dart_id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart is unknown".into()))?;
    let twin = QDartId(dart_id.0 ^ 1);
    let twin_position = *q
        .dart_positions
        .get(twin.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary twin has no fan position".into()))?;
    let start = *q
        .outgoing_offsets
        .get(dart.to_node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary node has no fan start".into()))?;
    let end = *q
        .outgoing_offsets
        .get(dart.to_node.0 as usize + 1)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary node has no fan end".into()))?;
    let length = end.checked_sub(start).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q boundary node has reversed fan offsets".into())
    })?;
    if length == 0 || twin_position >= length {
        return Err(
            GeometryErrorKind::Invalid("Q boundary twin is outside its node fan".into()).into(),
        );
    }
    let fan = q
        .outgoing
        .get(start..end)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q node fan is incomplete".into()))?;
    let (before_twin, twin_and_after) = fan.split_at(twin_position);
    let (twin, after_twin) = twin_and_after.split_first().ok_or_else(|| {
        GeometryErrorKind::Invalid("Q boundary twin is outside its node fan".into())
    })?;
    for &candidate in std::iter::once(twin)
        .chain(before_twin.iter().rev())
        .chain(after_twin.iter().rev())
    {
        work.charge(1)?;
        if owner_of(candidate)? == Some(owner) {
            return Ok(candidate);
        }
    }
    Err(GeometryErrorKind::Invalid("a selected Q boundary dart has no successor".into()).into())
}

fn source_edge_at(
    topology: &ExactGeographicTopology,
    source_edge: SourceEdgeId,
) -> Result<&ExactSourceEdge> {
    topology
        .sources
        .edges
        .get(source_edge.0 as usize)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("Q owner has an unknown source edge".into()).into()
        })
}

fn line_owner_index(
    topology: &ExactGeographicTopology,
    span_index: usize,
    work: &mut impl Work,
) -> Result<usize> {
    let q = &topology.q;
    let span = q
        .spans
        .get(span_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
    let line_rows = q.line_rows_for_span(span_index)?;
    let collapsed_rows = q.collapsed_rows_for_span(span_index)?;
    let mut ring_owner = None;
    for (owner_index, owner) in span.owners.iter().enumerate() {
        work.charge(1)?;
        let edge = source_edge_at(topology, owner.source_edge)?;
        let supported = match edge.kind {
            SourceEdgeKind::Line { .. } => {
                work.charge(1)?;
                line_rows.binary_search(&edge.row).is_ok()
            },
            SourceEdgeKind::Ring {
                hole: false,
                kind: crate::geometry::RingKind::Collapsed,
                ..
            } => {
                work.charge(1)?;
                collapsed_rows.binary_search(&edge.row).is_ok()
            },
            SourceEdgeKind::Ring { .. } => {
                ring_owner.get_or_insert(owner_index);
                false
            },
        };
        if supported {
            return Ok(owner_index);
        }
    }
    if let Some(owner) = ring_owner {
        return Ok(owner);
    }
    Err(
        GeometryErrorKind::Invalid("selected Q curve span has no source interval owner".into())
            .into(),
    )
}

fn area_owner_index(
    topology: &ExactGeographicTopology,
    span_index: usize,
    work: &mut impl Work,
) -> Result<usize> {
    let span = topology
        .q
        .spans
        .get(span_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q area span is unknown".into()))?;
    for (owner_index, owner) in span.owners.iter().enumerate() {
        work.charge(1)?;
        if matches!(
            source_edge_at(topology, owner.source_edge)?.kind,
            SourceEdgeKind::Ring { .. }
        ) {
            return Ok(owner_index);
        }
    }
    Err(GeometryErrorKind::Invalid("selected Q area boundary has no ring owner".into()).into())
}

fn line_steps(
    topology: &ExactGeographicTopology,
    selected: &[Option<SelectedLineSpan>],
    work: &mut impl Work,
) -> Result<Vec<Vec<LineStep>>> {
    let q = &topology.q;
    let node_count = q.nodes.len();
    work.charge(
        u64::try_from(
            node_count
                .checked_mul(2)
                .and_then(|nodes| nodes.checked_add(q.spans.len()))
                .and_then(|items| items.checked_add(1))
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut degree = vec![0_usize; node_count];
    let mut incidences = Vec::new();
    work.charge(u64::try_from(selected.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let edge_count = selected.iter().flatten().count();
    incidences.reserve(
        edge_count
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?,
    );
    for (span_index, (span, selection)) in q.spans.iter().zip(selected).enumerate() {
        work.charge(1)?;
        if selection.is_none() {
            continue;
        }
        let [start_node, end_node] = span.nodes;
        for (endpoint, node_id) in [
            (SpanEndpoint::Start, start_node),
            (SpanEndpoint::End, end_node),
        ] {
            work.charge(1)?;
            let node = node_id.0 as usize;
            let next = degree.get_mut(node).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line span has an unknown node".into())
            })?;
            *next = next.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            incidences.push((node, span_index, endpoint));
        }
    }
    try_sort_by_with_work(&mut incidences, work, |left, right, _work| {
        Ok(left.cmp(right))
    })?;
    let offset_count = node_count
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    let mut offsets = Vec::with_capacity(offset_count);
    offsets.push(0_usize);
    for count in &degree {
        work.charge(1)?;
        offsets.push(
            offsets
                .last()
                .copied()
                .expect("line incidence offsets start at zero")
                .checked_add(*count)
                .ok_or(ResourceError::SizeOverflow)?,
        );
    }
    if offsets.last().copied() != Some(incidences.len()) {
        return Err(GeometryErrorKind::Invalid(
            "Q line incidence offsets do not match their entries".into(),
        )
        .into());
    }
    let mut visited = vec![false; q.spans.len()];
    let mut paths = Vec::new();
    for (node, &node_degree) in degree.iter().enumerate() {
        work.charge(1)?;
        if node_degree == 2 {
            continue;
        }
        let [start, end] = offsets
            .get(node..node.saturating_add(2))
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line node has invalid offsets".into()))?
        else {
            return Err(
                GeometryErrorKind::Invalid("Q line node has invalid offsets".into()).into(),
            );
        };
        let incident = incidences.get(*start..*end).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q line node has an invalid incidence range".into())
        })?;
        for &(_, span, endpoint) in incident {
            work.charge(1)?;
            let selection = selected.get(span).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line incidence references an unknown span".into())
            })?;
            let already_visited = visited.get(span).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line incidence has no visited state".into())
            })?;
            if selection.is_some() && !already_visited {
                paths.push(walk_line_steps(
                    topology,
                    selected,
                    &degree,
                    &offsets,
                    &incidences,
                    &mut visited,
                    span,
                    endpoint,
                    work,
                )?);
            }
        }
    }
    for (span, selection) in selected.iter().enumerate() {
        work.charge(1)?;
        let already_visited = visited
            .get(span)
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line span has no visited state".into()))?;
        if selection.is_some() && !already_visited {
            paths.push(walk_line_steps(
                topology,
                selected,
                &degree,
                &offsets,
                &incidences,
                &mut visited,
                span,
                SpanEndpoint::Start,
                work,
            )?);
        }
    }
    Ok(paths)
}

fn planned_line_paths(
    topology: &ExactGeographicTopology,
    paths: Vec<Vec<LineStep>>,
    work: &mut impl Work,
) -> Result<Vec<PlannedQLine>> {
    let mut output = Vec::new();
    for path in paths {
        work.charge(1)?;
        let mut current_origin = None;
        let mut current_steps = Vec::new();
        for step in path {
            work.charge(1)?;
            let span = topology.q.spans.get(step.span).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line step references an unknown span".into())
            })?;
            let owner = span.owners.get(step.owner).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line step references an unknown owner".into())
            })?;
            let origin = match source_edge_at(topology, owner.source_edge)?.kind {
                SourceEdgeKind::Line { .. }
                | SourceEdgeKind::Ring {
                    kind: crate::geometry::RingKind::Collapsed,
                    ..
                } => QLineOrigin::Curve,
                SourceEdgeKind::Ring { .. } => QLineOrigin::ArealBoundary,
            };
            if let Some(previous) = current_origin
                && previous != origin
            {
                let index = output.len();
                output.push(QLinePrimitive {
                    origin: previous,
                    recipe_walk: RecipeWalkOwner::Line(index),
                    path: std::mem::take(&mut current_steps),
                });
            }
            current_origin = Some(origin);
            current_steps.push(step);
        }
        if let Some(origin) = current_origin {
            let index = output.len();
            output.push(QLinePrimitive {
                origin,
                recipe_walk: RecipeWalkOwner::Line(index),
                path: current_steps,
            });
        }
    }
    Ok(output)
}

fn walk_line_steps(
    topology: &ExactGeographicTopology,
    selected: &[Option<SelectedLineSpan>],
    degree: &[usize],
    offsets: &[usize],
    incidences: &[(usize, usize, SpanEndpoint)],
    visited: &mut [bool],
    start_span: usize,
    start_endpoint: SpanEndpoint,
    work: &mut impl Work,
) -> Result<Vec<LineStep>> {
    let q = &topology.q;
    let mut steps = Vec::new();
    let (mut span_index, mut from_endpoint) = (start_span, start_endpoint);
    loop {
        work.charge(1)?;
        let already_visited = visited
            .get_mut(span_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
        if *already_visited {
            if span_index == start_span && from_endpoint == start_endpoint {
                break;
            }
            return Err(GeometryErrorKind::Invalid(
                "Q line traversal entered an earlier span".into(),
            )
            .into());
        }
        *already_visited = true;
        let owner = selected
            .get(span_index)
            .copied()
            .flatten()
            .map(|selection| selection.owner)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "selected Q curve span has no source interval owner".into(),
                )
            })?;
        steps.push(LineStep {
            span: span_index,
            from_endpoint,
            owner,
        });
        let span = q
            .spans
            .get(span_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
        let arrived_endpoint = from_endpoint.opposite();
        let [start_node, end_node] = span.nodes;
        let node = match arrived_endpoint {
            SpanEndpoint::Start => start_node,
            SpanEndpoint::End => end_node,
        }
        .0 as usize;
        if degree.get(node).copied() != Some(2) {
            break;
        }
        let start = *offsets.get(node).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q line node has no incidence start".into())
        })?;
        let end = *offsets
            .get(node + 1)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line node has no incidence end".into()))?;
        let mut next = None;
        for &incidence in incidences.get(start..end).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q line node has an invalid incidence range".into())
        })? {
            work.charge(1)?;
            if incidence.1 != span_index || incidence.2 != arrived_endpoint {
                next = Some(incidence);
                break;
            }
        }
        let next = next
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line node has no continuation".into()))?;
        if visited.get(next.1).copied().unwrap_or(false) {
            if next.1 == start_span && next.2 == start_endpoint {
                break;
            }
            return Err(GeometryErrorKind::Invalid(
                "Q line cycle closes on a different used span".into(),
            )
            .into());
        }
        span_index = next.1;
        from_endpoint = next.2;
    }
    if steps.is_empty() {
        return Err(GeometryErrorKind::Invalid("Q line traversal emitted no spans".into()).into());
    }
    Ok(steps)
}

fn node_ordinates<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    declared_axes: CoordinateAxes,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    work: &mut impl Work,
) -> Result<Vec<NodeOrdinates>> {
    let q = &topology.q;
    if q.source_edge_ports.len() != topology.sources.edges.len() {
        return Err(GeometryErrorKind::Invalid(
            "Q source-edge ports do not match their source intervals".into(),
        )
        .into());
    }
    if q.source_point_ports.len() != topology.sources.points.len() {
        return Err(GeometryErrorKind::Invalid(
            "Q source-point ports do not match their source points".into(),
        )
        .into());
    }
    work.charge(u64::try_from(q.nodes.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut output = vec![NodeOrdinates::default(); q.nodes.len()];
    if !declared_axes.has_z() && !declared_axes.has_m() {
        return Ok(output);
    }
    for (edge, ports) in topology
        .sources
        .edges
        .iter()
        .zip(q.source_edge_ports.iter())
    {
        work.charge(1)?;
        for (point, &port) in edge.source.iter().zip(ports) {
            work.charge(1)?;
            let node = q
                .ports
                .get(port.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source endpoint port is unknown".into())
                })?
                .node;
            let values = output.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source endpoint node is unknown".into())
            })?;
            values.z.observe(point.z());
            values.m.observe(point.m());
        }
    }
    let mut request_rows = Vec::new();
    let mut request_nodes = Vec::new();
    for span in &q.spans {
        work.charge(1)?;
        for owner in &span.owners {
            work.charge(1)?;
            let start_parameter = if owner.reversed {
                owner.source_range.end.try_clone_with_work(work)?
            } else {
                owner.source_range.start.try_clone_with_work(work)?
            };
            let end_parameter = if owner.reversed {
                owner.source_range.start.try_clone_with_work(work)?
            } else {
                owner.source_range.end.try_clone_with_work(work)?
            };
            for (node, parameter) in [
                (span.nodes[0], start_parameter),
                (span.nodes[1], end_parameter),
            ] {
                work.charge(3)?;
                request_rows.push(RequestRowInput {
                    axes: declared_axes,
                    requests: vec![RootRequest::new_with_work(
                        RootId::from_index(owner.source_edge.0),
                        parameter,
                        work,
                    )?],
                });
                request_nodes.push(node);
            }
        }
    }
    let resolved = resolve_request_rows(&request_rows, topology, metric_access, work)?;
    for (node, ordinates) in request_nodes.into_iter().zip(resolved) {
        work.charge(1)?;
        output
            .get_mut(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q span endpoint node is unknown".into()))?
            .merge(ordinates);
    }
    for (source, &port) in topology
        .sources
        .points
        .iter()
        .zip(q.source_point_ports.iter())
    {
        work.charge(1)?;
        let node = q
            .ports
            .get(port.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source point port is unknown".into()))?
            .node;
        let values = output
            .get_mut(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source point node is unknown".into()))?;
        values.z.observe(source.point.z());
        values.m.observe(source.point.m());
    }
    Ok(output)
}

fn emitted_point<'topology>(
    topology: &'topology ExactGeographicTopology,
    coordinates: &QCoordinates,
    span_index: usize,
    owner_index: usize,
    port: QPortId,
) -> Result<EmittedVertex<'topology>> {
    let q = &topology.q;
    let span = q
        .spans
        .get(span_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted span is unknown".into()))?;
    span.owners
        .get(owner_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted owner is unknown".into()))?;
    if !span.ports.contains(&port) {
        return Err(GeometryErrorKind::Invalid("Q port is not on its emitted span".into()).into());
    }
    let q_port = q
        .ports
        .get(port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted port is unknown".into()))?;
    Ok(EmittedVertex {
        port: q_port,
        point: coordinates.point(topology, port)?,
        literal_image: QSourceImage::Port(port),
    })
}

fn push_line_part<'topology>(
    output: &mut Vec<DrawnQLine<'topology>>,
    origin: QLineOrigin,
    recipe_walk: RecipeWalkOwner,
    vertices: &mut Vec<EmittedVertex<'topology>>,
) {
    if vertices.len() >= 2 {
        output.push(QLinePrimitive {
            origin,
            recipe_walk,
            path: std::mem::take(vertices),
        });
    } else {
        vertices.clear();
    }
}

#[derive(Clone, Copy)]
pub(super) struct SourcePublication<'a> {
    pub(super) inventory: &'a PublicationInventory,
    pub(super) points: &'a [XY],
    pub(super) ordinates: &'a [NodeOrdinates],
    pub(super) live_samples: &'a [bool],
}

pub(super) fn source_live_publication_samples(
    plan: &QEmissionPlan,
    publication: &PublicationInventory,
    work: &mut impl Work,
) -> Result<Vec<bool>> {
    work.charge(
        u64::try_from(publication.samples.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut live = vec![false; publication.samples.len()];
    for walk in plan.fragment_recipes.walks() {
        for &recipe in &walk.fragments {
            work.charge(1)?;
            let fragment = publication
                .fragments
                .get(recipe.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source producer walk has no published fragment".into(),
                    )
                })?;
            if fragment.recipe != recipe
                || fragment.sample_range.start > fragment.sample_range.end
                || fragment.sample_range.end > publication.samples.len()
            {
                return Err(GeometryErrorKind::Invalid(
                    "source published fragment has invalid sample ownership".into(),
                )
                .into());
            }
            for sample_index in fragment.sample_range.clone() {
                work.charge(1)?;
                let sample = publication.samples.get(sample_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source published fragment sample index is unknown".into(),
                    )
                })?;
                if sample.family.fragment_recipe() != Some(recipe) {
                    return Err(GeometryErrorKind::Invalid(
                        "source publication sample belongs to another recipe".into(),
                    )
                    .into());
                }
                *live.get_mut(sample_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source publication sample is outside its liveness table".into(),
                    )
                })? = true;
            }
        }
    }
    Ok(live)
}

pub(super) fn with_publication_axes(
    point: XY,
    ordinates: NodeOrdinates,
    axes: CoordinateAxes,
) -> Result<Point> {
    let z = if axes.has_z() {
        Some(ordinates.z.value().ok_or_else(|| {
            GeometryErrorKind::Invalid("source publication Z ordinate is missing".into())
        })?)
    } else {
        None
    };
    let m = if axes.has_m() {
        Some(ordinates.m.value().ok_or_else(|| {
            GeometryErrorKind::Invalid("source publication M ordinate is missing".into())
        })?)
    } else {
        None
    };
    Ok(Point::new_raw_axes(
        point.x,
        point.y,
        ZOrdinate(z),
        MOrdinate(m),
    ))
}

fn with_assigned_source_vertex_axes(
    vertex: EmittedVertex<'_>,
    ordinates: &[NodeOrdinates],
    source_ports: Option<&[Option<ExactChartPoint>]>,
    axes: CoordinateAxes,
    work: &mut impl Work,
) -> Result<Point> {
    let point = match (vertex.port.cut, source_ports) {
        (true, Some(source_ports)) => {
            let image = source_ports
                .get(vertex.port.id.0 as usize)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "assigned Q source cut has no checked chart image".into(),
                    )
                })?;
            work.charge(1)?;
            Point::new_raw_xy(image.x.round()?, image.y.round()?)
        },
        _ => vertex.point,
    };
    with_vertex_axes(
        EmittedVertex {
            port: vertex.port,
            point,
            literal_image: vertex.literal_image,
        },
        ordinates,
        axes,
    )
}

const fn source_literal_image(
    vertex: EmittedVertex<'_>,
    source_ports: Option<&[Option<ExactChartPoint>]>,
) -> QSourceImage {
    if vertex.port.cut && source_ports.is_some() {
        QSourceImage::Port(vertex.port.id)
    } else {
        vertex.literal_image
    }
}

struct SourceWrittenPath {
    points: Vec<Point>,
    locations: LiteralSequence<QSourceImage>,
}

fn write_source_path(
    vertices: &[EmittedVertex<'_>],
    output: u32,
    owner: RecipeWalkOwner,
    recipe_rotation: usize,
    ordinates: &[NodeOrdinates],
    source_ports: Option<&[Option<ExactChartPoint>]>,
    publication: Option<SourcePublication<'_>>,
    walks: Option<&RecipeWalkIndex<'_>>,
    cursors: &mut BTreeMap<RecipeWalkOwner, usize>,
    axes: CoordinateAxes,
    work: &mut impl Work,
) -> Result<SourceWrittenPath> {
    let walk = match (publication, walks) {
        (Some(_), Some(walks)) => Some(next_recipe_walk(walks, cursors, owner, work)?),
        (None, None) => None,
        _ => {
            return Err(GeometryErrorKind::Invalid(
                "source publication inventory and walk map have different lifetimes".into(),
            )
            .into());
        },
    };
    let mut rotated_fragments = None;
    if let Some(walk) = walk
        && recipe_rotation != 0
    {
        if recipe_rotation >= walk.fragments.len() {
            return Err(GeometryErrorKind::Invalid(
                "source ring rotation exceeds its producer walk".into(),
            )
            .into());
        }
        // Keep each frozen publication sample on the producer edge that owns it.
        work.charge(u64::try_from(walk.fragments.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut fragments = walk.fragments.clone();
        fragments.rotate_left(recipe_rotation);
        rotated_fragments = Some(fragments);
    }
    let walk_fragments = match walk {
        Some(walk) => rotated_fragments.as_deref().unwrap_or(&walk.fragments),
        None => &[],
    };
    let edge_count = vertices.len().saturating_sub(1);
    if walk.is_some() && walk_fragments.len() != edge_count {
        return Err(GeometryErrorKind::Invalid(
            "source drawing path and published producer walk have different edge counts".into(),
        )
        .into());
    }
    let mut capacity = vertices.len();
    if let (Some(publication), Some(_walk)) = (publication, walk) {
        for &recipe in walk_fragments {
            let fragment = publication
                .inventory
                .fragments
                .get(recipe.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("source path references an unknown fragment".into())
                })?;
            capacity = capacity
                .checked_add(fragment.sample_range.len())
                .ok_or(ResourceError::SizeOverflow)?;
        }
    }
    work.charge(u64::try_from(capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut points = Vec::with_capacity(capacity);
    let mut images = Vec::with_capacity(capacity);
    if let Some(&first) = vertices.first() {
        points.push(with_assigned_source_vertex_axes(
            first,
            ordinates,
            source_ports,
            axes,
            work,
        )?);
        images.push(source_literal_image(first, source_ports));
    }
    for (edge_index, pair) in vertices.array_windows::<2>().enumerate() {
        let [_, end] = pair;
        work.charge(1)?;
        if let (Some(publication), Some(_walk)) = (publication, walk) {
            let recipe = *walk_fragments.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("source path has no recipe for an output edge".into())
            })?;
            let fragment = publication
                .inventory
                .fragments
                .get(recipe.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source path recipe has no published fragment".into(),
                    )
                })?;
            let mut previous_vertex = 0_usize;
            let last_vertex = fragment.points.len().checked_sub(1).ok_or_else(|| {
                GeometryErrorKind::Invalid("source published fragment has no endpoint".into())
            })?;
            for sample_index in fragment.sample_range.clone() {
                work.charge(1)?;
                let sample = publication
                    .inventory
                    .samples
                    .get(sample_index)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("source path sample row is unknown".into())
                    })?;
                if sample.family.fragment_recipe() != Some(recipe)
                    || sample.request_row.index() != sample_index
                    || sample.written_vertex <= previous_vertex
                    || sample.written_vertex >= last_vertex
                    || fragment.points.get(sample.written_vertex) != Some(&sample.point)
                {
                    return Err(GeometryErrorKind::Invalid(
                        "source publication sample is not an ordered interior fragment vertex"
                            .into(),
                    )
                    .into());
                }
                previous_vertex = sample.written_vertex;
                let point = *publication.points.get(sample_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source publication sample has no assigned coordinate".into(),
                    )
                })?;
                let sample_ordinates =
                    *publication.ordinates.get(sample_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "source publication sample has no resolved ordinate row".into(),
                        )
                    })?;
                points.push(with_publication_axes(point, sample_ordinates, axes)?);
                images.push(QSourceImage::PublicationSample(
                    u32::try_from(sample_index).map_err(|_| ResourceError::SizeOverflow)?,
                ));
            }
        }
        points.push(with_assigned_source_vertex_axes(
            *end,
            ordinates,
            source_ports,
            axes,
            work,
        )?);
        images.push(source_literal_image(*end, source_ports));
    }
    Ok(SourceWrittenPath {
        points,
        locations: LiteralSequence {
            output,
            axes,
            images,
        },
    })
}

fn write_ring(
    vertices: &[EmittedVertex<'_>],
    output: u32,
    owner: RecipeWalkOwner,
    recipe_rotation: usize,
    ordinates: &[NodeOrdinates],
    source_ports: Option<&[Option<ExactChartPoint>]>,
    publication: Option<SourcePublication<'_>>,
    walks: Option<&RecipeWalkIndex<'_>>,
    cursors: &mut BTreeMap<RecipeWalkOwner, usize>,
    axes: CoordinateAxes,
    work: &mut impl Work,
) -> Result<(Ring, LiteralSequence<QSourceImage>)> {
    let path = write_source_path(
        vertices,
        output,
        owner,
        recipe_rotation,
        ordinates,
        source_ports,
        publication,
        walks,
        cursors,
        axes,
        work,
    )?;
    Ok((
        Ring::try_from_closed(CoordSeq::try_from_points(&path.points)?)?,
        path.locations,
    ))
}

fn rotate_area_ring_vertices<'topology>(
    vertices: &[EmittedVertex<'topology>],
    start_port: Option<QPortId>,
    work: &mut impl Work,
) -> Result<Option<(Vec<EmittedVertex<'topology>>, usize)>> {
    let Some(start_port) = start_port else {
        return Ok(None);
    };
    let edge_count = vertices
        .len()
        .checked_sub(1)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q area ring has no closing vertex".into()))?;
    let start_index = vertices
        .get(..edge_count)
        .and_then(|open| open.iter().position(|vertex| vertex.port.id == start_port))
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("Q area ring lost its native cycle start".into())
        })?;
    if start_index == 0 {
        return Ok(None);
    }
    work.charge(u64::try_from(vertices.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut rotated = Vec::with_capacity(vertices.len());
    let open = vertices.get(..edge_count).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q area ring has an invalid open vertex range".into())
    })?;
    rotated.extend_from_slice(&open[start_index..]);
    rotated.extend_from_slice(&open[..start_index]);
    let first = *rotated.first().ok_or_else(|| {
        GeometryErrorKind::Invalid("Q area ring rotation removed every vertex".into())
    })?;
    rotated.push(first);
    Ok(Some((rotated, start_index)))
}

fn write_drawing(
    drawing: QEmissionDrawing<'_>,
    ring_roles: &[bool],
    ordinates: &[NodeOrdinates],
    publication: Option<SourcePublication<'_>>,
    source_ports: Option<&[Option<ExactChartPoint>]>,
    walks: Option<&RecipeWalkIndex<'_>>,
    axes: CoordinateAxes,
    work: &mut impl Work,
) -> Result<(
    Vec<Point>,
    Vec<CoordSeq>,
    Vec<Polygon>,
    ShapeLiteralLayout<QSourceImage>,
)> {
    let mut walk_cursors = BTreeMap::new();
    let (polygons, polygon_locations) = match drawing.polygons {
        DrawnPolygons::WholeSphere(source_polygons) => {
            work.charge(
                u64::try_from(source_polygons.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut output = Vec::with_capacity(source_polygons.len());
            let mut output_locations = Vec::with_capacity(source_polygons.len());
            for (output_index, source_rings) in source_polygons.into_iter().enumerate() {
                work.charge(1)?;
                let output_id =
                    u32::try_from(output_index).map_err(|_| ResourceError::SizeOverflow)?;
                let mut shell = None;
                let mut shell_locations = None;
                let mut holes = Vec::new();
                let mut hole_locations = Vec::new();
                for source_ring in source_rings {
                    work.charge(1)?;
                    let (ring, locations) = write_ring(
                        &source_ring.vertices,
                        output_id,
                        source_ring.recipe_walk,
                        0,
                        ordinates,
                        source_ports,
                        publication,
                        walks,
                        &mut walk_cursors,
                        axes,
                        work,
                    )?;
                    if source_ring.hole {
                        holes.push(ring);
                        hole_locations.push(locations);
                    } else if shell.replace(ring).is_some()
                        || shell_locations.replace(locations).is_some()
                    {
                        return Err(GeometryErrorKind::Invalid(
                            "Q source polygon has more than one shell".into(),
                        )
                        .into());
                    }
                }
                let shell = shell.ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source polygon has no shell".into())
                })?;
                output.push(Polygon::new(shell, holes));
                output_locations.push(LiteralPolygon {
                    output: output_id,
                    axes,
                    shell: shell_locations.ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q source polygon has no shell map".into())
                    })?,
                    holes: hole_locations,
                });
            }
            if output.is_empty() {
                return Err(GeometryErrorKind::Invalid(
                    "Q selected the whole sphere without source area polygons".into(),
                )
                .into());
            }
            (output, output_locations)
        },
        DrawnPolygons::Regions {
            rings,
            region_count,
        } => {
            work.charge(u64::try_from(region_count).map_err(|_| ResourceError::SizeOverflow)?)?;
            let mut shells = Vec::with_capacity(region_count);
            let mut holes = Vec::with_capacity(region_count);
            let mut shell_locations = Vec::with_capacity(region_count);
            let mut hole_locations = Vec::with_capacity(region_count);
            shells.resize_with(region_count, Vec::<Ring>::new);
            holes.resize_with(region_count, Vec::<Ring>::new);
            shell_locations.resize_with(region_count, Vec::<LiteralSequence<QSourceImage>>::new);
            hole_locations.resize_with(region_count, Vec::<LiteralSequence<QSourceImage>>::new);
            for (ring_index, drawn) in rings.into_iter().enumerate() {
                work.charge(1)?;
                let is_hole = *ring_roles.get(ring_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q certified ring roles are incomplete".into())
                })?;
                let region_id =
                    u32::try_from(drawn.region).map_err(|_| ResourceError::SizeOverflow)?;
                let rotated_vertices =
                    rotate_area_ring_vertices(&drawn.vertices, drawn.native_start_port, work)?;
                let (vertices, recipe_rotation) = rotated_vertices
                    .as_ref()
                    .map_or((&drawn.vertices[..], 0), |(vertices, rotation)| {
                        (vertices.as_slice(), *rotation)
                    });
                let (ring, locations) = write_ring(
                    vertices,
                    region_id,
                    drawn.recipe_walk,
                    recipe_rotation,
                    ordinates,
                    source_ports,
                    publication,
                    walks,
                    &mut walk_cursors,
                    axes,
                    work,
                )?;
                let destination = if is_hole {
                    holes.get_mut(drawn.region)
                } else {
                    shells.get_mut(drawn.region)
                }
                .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary region is unknown".into()))?;
                destination.push(ring);
                let location_destination = if is_hole {
                    hole_locations.get_mut(drawn.region)
                } else {
                    shell_locations.get_mut(drawn.region)
                }
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q boundary region literal map is unknown".into())
                })?;
                location_destination.push(locations);
            }

            let mut output = Vec::new();
            let mut output_locations = Vec::new();
            for (
                region,
                (((mut region_shells, region_holes), mut region_shell_maps), mut region_hole_maps),
            ) in shells
                .into_iter()
                .zip(holes)
                .zip(shell_locations)
                .zip(hole_locations)
                .enumerate()
            {
                work.charge(1)?;
                if region_shells.is_empty() && region_holes.is_empty() {
                    continue;
                }
                if region_shells.len() != 1 {
                    return Err(GeometryErrorKind::Invalid(
                        format!(
                            "selected Q region {region} has {} shells",
                            region_shells.len(),
                        )
                        .into(),
                    )
                    .into());
                }
                let shell = region_shells
                    .pop()
                    .expect("the selected region has one shell");
                if region_shell_maps.len() != 1 {
                    return Err(GeometryErrorKind::Invalid(
                        "selected Q region has no unique shell literal map".into(),
                    )
                    .into());
                }
                let output_id =
                    u32::try_from(output.len()).map_err(|_| ResourceError::SizeOverflow)?;
                let mut shell_map = region_shell_maps
                    .pop()
                    .expect("the selected region has one shell literal map");
                shell_map.output = output_id;
                for hole_map in &mut region_hole_maps {
                    hole_map.output = output_id;
                }
                output.push(Polygon::new(shell, region_holes));
                output_locations.push(LiteralPolygon {
                    output: output_id,
                    axes,
                    shell: shell_map,
                    holes: region_hole_maps,
                });
            }
            (output, output_locations)
        },
    };
    work.charge(u64::try_from(drawing.lines.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut lines = Vec::with_capacity(drawing.lines.len());
    let mut line_locations = Vec::with_capacity(drawing.lines.len());
    for (line_index, line) in drawing.lines.iter().enumerate() {
        let output = u32::try_from(line_index).map_err(|_| ResourceError::SizeOverflow)?;
        let path = write_source_path(
            &line.path,
            output,
            line.recipe_walk,
            0,
            ordinates,
            source_ports,
            publication,
            walks,
            &mut walk_cursors,
            axes,
            work,
        )?;
        lines.push(CoordSeq::try_from_points(&path.points)?);
        line_locations.push(path.locations);
    }
    work.charge(u64::try_from(drawing.points.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut points = Vec::with_capacity(drawing.points.len());
    let mut point_locations = Vec::with_capacity(drawing.points.len());
    for (point_index, &vertex) in drawing.points.iter().enumerate() {
        work.charge(1)?;
        points.push(with_assigned_source_vertex_axes(
            vertex,
            ordinates,
            source_ports,
            axes,
            work,
        )?);
        point_locations.push(LiteralPoint {
            output: u32::try_from(point_index).map_err(|_| ResourceError::SizeOverflow)?,
            axes,
            image: source_literal_image(vertex, source_ports),
        });
    }
    if let Some(walks) = walks {
        for (owner, owner_walks) in walks {
            work.charge(1)?;
            if walk_cursors.get(owner).copied().unwrap_or_default() != owner_walks.len() {
                return Err(GeometryErrorKind::Invalid(
                    "source drawing did not consume each producer walk exactly once".into(),
                )
                .into());
            }
        }
    }
    Ok((points, lines, polygons, ShapeLiteralLayout {
        points: point_locations,
        lines: line_locations,
        polygons: polygon_locations,
    }))
}

fn identity_output_order(
    point_count: usize,
    line_count: usize,
    polygon_count: usize,
    work: &mut impl Work,
) -> Result<CertifiedOverlayOrder> {
    let count = point_count
        .checked_add(line_count)
        .and_then(|count| count.checked_add(polygon_count))
        .ok_or(ResourceError::SizeOverflow)?;
    work.charge(u64::try_from(count).map_err(|_| ResourceError::SizeOverflow)?)?;
    Ok(CertifiedOverlayOrder {
        points: identity_output_indices(point_count, work)?,
        lines: identity_output_indices(line_count, work)?,
        polygons: identity_output_indices(polygon_count, work)?,
    })
}

fn identity_output_indices(length: usize, work: &mut impl Work) -> Result<Vec<usize>> {
    work.charge(u64::try_from(length).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut order = Vec::with_capacity(length);
    for index in 0..length {
        work.charge(1)?;
        order.push(index);
    }
    Ok(order)
}

fn seal_output_parts(
    plan: &QEmissionPlan,
    points: Vec<Point>,
    lines: Vec<CoordSeq>,
    mut polygons: Vec<Polygon>,
    axes: CoordinateAxes,
    declared_axes: CoordinateAxes,
    track_order: bool,
    work: &mut impl Work,
) -> Result<(Shape, Option<CertifiedOverlayOrder>)> {
    let output_count = points
        .len()
        .checked_add(lines.len())
        .and_then(|count| count.checked_add(polygons.len()))
        .ok_or(ResourceError::SizeOverflow)?;
    work.charge(u64::try_from(output_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let point_count = points.len();
    let line_count = lines.len();

    if let Some(support) = plan.boundary_support {
        let point_part = if points.is_empty() {
            None
        } else {
            work.charge(u64::try_from(points.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
            let mut coordinates = CoordSeqBuilder::with_capacity(axes, points.len());
            for point in points {
                work.charge(1)?;
                coordinates.push(point);
            }
            Some(Shape::multi_point(axes, coordinates.finish()?))
        };
        let line_part = if lines.is_empty() {
            None
        } else {
            work.charge(u64::try_from(lines.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
            let values = lines.into_iter().map(LineSeq::from_proven).collect();
            Some(Shape::narrow_lines(axes, values))
        };

        if support.has_area {
            let line_part = line_part.unwrap_or_else(|| {
                Shape::typed_empty(GeometryKind::MultiLineString, declared_axes)
            });
            let keep_points = point_part.is_some();
            let keep_lines = line_count != 0;
            let shape = match point_part {
                Some(point_part) => {
                    work.charge(2)?;
                    Shape::geometry_collection(vec![line_part, point_part])
                },
                None => Ok(line_part),
            }?;
            let order = track_order
                .then(|| {
                    identity_output_order(
                        if keep_points { point_count } else { 0 },
                        if keep_lines { line_count } else { 0 },
                        0,
                        work,
                    )
                })
                .transpose()?;
            return Ok((shape, order));
        }
        if support.has_curve {
            let keep_points = point_part.is_some();
            let shape = point_part
                .unwrap_or_else(|| Shape::typed_empty(GeometryKind::MultiPoint, declared_axes));
            let order = track_order
                .then(|| {
                    identity_output_order(if keep_points { point_count } else { 0 }, 0, 0, work)
                })
                .transpose()?;
            return Ok((shape, order));
        }
        if support.has_point {
            let shape = Shape::typed_empty(GeometryKind::GeometryCollection, declared_axes);
            let order = track_order
                .then(|| identity_output_order(0, 0, 0, work))
                .transpose()?;
            return Ok((shape, order));
        }
        let shape = Shape::typed_empty(GeometryKind::MultiLineString, declared_axes);
        let order = track_order
            .then(|| identity_output_order(0, 0, 0, work))
            .transpose()?;
        return Ok((shape, order));
    }

    if plan.whole_sphere {
        let polygon_count = polygons.len();
        let shape = match polygons.len() {
            0 => Err(GeometryErrorKind::Invalid(
                "Q selected the whole sphere without source area polygons".into(),
            )
            .into()),
            1 => {
                Ok(Shape::Polygon(polygons.pop().expect(
                    "a one-polygon whole-sphere output has its polygon",
                )))
            },
            _ => {
                work.charge(
                    u64::try_from(polygons.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                Shape::geometry_collection(polygons.into_iter().map(Shape::Polygon).collect())
            },
        }?;
        let order = track_order
            .then(|| identity_output_order(0, 0, polygon_count, work))
            .transpose()?;
        return Ok((shape, order));
    }

    if track_order {
        let (shape, order) =
            build_certified_overlay_shape_with_axes_and_order(points, lines, polygons, axes, work)?;
        Ok((shape, Some(order)))
    } else {
        Ok((
            build_certified_overlay_shape_with_axes(points, lines, polygons, axes)?,
            None,
        ))
    }
}

const fn source_polygon_contributes_to_whole_sphere(
    source_polygon: &super::exact::ExactSourcePolygon,
    operation: OverlayOp,
) -> bool {
    match operation {
        OverlayOp::Intersection | OverlayOp::Difference => source_polygon.row.0 == 0,
        OverlayOp::Union | OverlayOp::SymmetricDifference => true,
    }
}

fn cycle_belongs_to_source_ring(
    topology: &ExactGeographicTopology,
    cycle: &BoundaryCycle,
    source_ring: SourceRingId,
    work: &mut impl Work,
) -> Result<bool> {
    let q = &topology.q;
    for &dart_id in &cycle.darts {
        work.charge(1)?;
        let dart = q
            .darts
            .get(dart_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart is unknown".into()))?;
        let span = q
            .spans
            .get(dart.span.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary span is unknown".into()))?;
        let mut owned = false;
        for owner in &span.owners {
            work.charge(1)?;
            let edge = source_edge_at(topology, owner.source_edge)?;
            if matches!(
                edge.kind,
                SourceEdgeKind::Ring { ring, .. } if ring == source_ring
            ) {
                owned = true;
                break;
            }
        }
        if !owned {
            return Ok(false);
        }
    }
    Ok(true)
}

fn source_cycles_by_ring(
    topology: &ExactGeographicTopology,
    cycles: &[BoundaryCycle],
    work: &mut impl Work,
) -> Result<Vec<Vec<usize>>> {
    let mut output = vec![Vec::new(); topology.sources.rings.len()];
    for (cycle_index, cycle) in cycles.iter().enumerate() {
        work.charge(1)?;
        let mut common_rings: Option<BTreeSet<SourceRingId>> = None;
        for &dart_id in &cycle.darts {
            work.charge(1)?;
            let dart =
                topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q boundary dart is unknown".into())
                })?;
            let span =
                topology.q.spans.get(dart.span.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q boundary span is unknown".into())
                })?;
            let mut span_rings = BTreeSet::new();
            for owner in &span.owners {
                work.charge(1)?;
                if let SourceEdgeKind::Ring { ring, .. } =
                    source_edge_at(topology, owner.source_edge)?.kind
                {
                    span_rings.insert(ring);
                }
            }
            if let Some(common_rings) = &mut common_rings {
                work.charge(
                    u64::try_from(common_rings.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                common_rings.retain(|ring| span_rings.contains(ring));
            } else {
                common_rings = Some(span_rings);
            }
            if common_rings.as_ref().is_some_and(BTreeSet::is_empty) {
                break;
            }
        }
        if let Some(common_rings) = common_rings {
            for ring in common_rings {
                work.charge(1)?;
                let ring_index =
                    usize::try_from(ring.0).map_err(|_| ResourceError::SizeOverflow)?;
                output
                    .get_mut(ring_index)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q boundary ring owner is unknown".into())
                    })?
                    .push(cycle_index);
            }
        }
    }
    Ok(output)
}

fn source_cycle_start_port(
    topology: &ExactGeographicTopology,
    cycle: &BoundaryCycle,
    work: &mut impl Work,
) -> Result<QPortId> {
    for &dart_id in &cycle.darts {
        work.charge(1)?;
        let dart = topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q cycle start references an unknown dart".into())
        })?;
        let port = topology
            .q
            .ports
            .get(dart.from_port.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q cycle start references an unknown port".into())
            })?;
        if !port.cut {
            return Ok(dart.from_port);
        }
    }
    for &dart_id in &cycle.darts {
        work.charge(1)?;
        let dart = topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q cycle start references an unknown dart".into())
        })?;
        let port = topology
            .q
            .ports
            .get(dart.to_port.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q cycle start references an unknown port".into())
            })?;
        if !port.cut {
            return Ok(dart.to_port);
        }
    }
    Err(GeometryErrorKind::Invalid(
        "Q area cycle has no source node to anchor its native port order".into(),
    )
    .into())
}

fn source_ring_cycles_follow_source(
    topology: &ExactGeographicTopology,
    source_ring: &ExactSourceRing,
    all_cycles: &[BoundaryCycle],
    cycle_indices: &[usize],
    work: &mut impl Work,
) -> Result<Option<bool>> {
    let q = &topology.q;
    let mut follows_source = None;
    for &cycle_index in cycle_indices {
        work.charge(1)?;
        let cycle = all_cycles
            .get(cycle_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source-ring cycle is unknown".into()))?;
        for &dart_id in &cycle.darts {
            work.charge(1)?;
            let dart = q
                .darts
                .get(dart_id.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart is unknown".into()))?;
            let span = q
                .spans
                .get(dart.span.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary span is unknown".into()))?;
            let mut span_direction = None;
            for owner in &span.owners {
                work.charge(1)?;
                let edge = source_edge_at(topology, owner.source_edge)?;
                if !matches!(
                    edge.kind,
                    SourceEdgeKind::Ring { ring, .. } if ring == source_ring.id
                ) {
                    continue;
                }
                let forward = (dart_id.0 & 1 == 0) != owner.reversed;
                if span_direction.is_some_and(|previous| previous != forward) {
                    return Ok(None);
                }
                span_direction = Some(forward);
            }
            let Some(span_direction) = span_direction else {
                return Ok(None);
            };
            if follows_source.is_some_and(|previous| previous != span_direction) {
                return Ok(None);
            }
            follows_source = Some(span_direction);
        }
    }
    Ok(follows_source)
}

fn source_ring_cycles_cover_boundary(
    topology: &ExactGeographicTopology,
    source_ring: &super::exact::ExactSourceRing,
    all_cycles: &[BoundaryCycle],
    cycle_indices: &[usize],
    work: &mut impl Work,
) -> Result<bool> {
    if source_ring.summary.exact_axis_band {
        let axis = topology.sources.frame.longitude();
        let south = canonical_bits(source_ring.summary.south.get());
        let north = canonical_bits(source_ring.summary.north.get());
        let mut parallel_edges = Vec::new();
        let mut parallel_mask = 0_u8;
        for edge_index in source_ring.source_edges.clone() {
            work.charge(1)?;
            let edge = topology.sources.edges.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring edge is unknown".into())
            })?;
            let latitude = canonical_bits(edge.source[0].y);
            if latitude != canonical_bits(edge.source[1].y) {
                continue;
            }
            let parallel = if latitude == south {
                1
            } else if latitude == north {
                2
            } else {
                continue;
            };
            let full_turn = axis
                .chart_step(edge.source[0].x, edge.source[1].x)
                .expect("Q source longitude was admitted")
                .is_circle();
            if !full_turn {
                let start = axis
                    .canonicalize(edge.source[0].x)
                    .expect("source longitude was admitted")
                    .get();
                let end = axis
                    .canonicalize(edge.source[1].x)
                    .expect("source longitude was admitted")
                    .get();
                if start.to_bits() == end.to_bits() {
                    continue;
                }
            }
            parallel_mask |= parallel;
            parallel_edges.push(edge.id);
        }
        if parallel_mask != 0b11 {
            return Ok(false);
        }
        for edge_id in parallel_edges {
            work.charge(1)?;
            let mut ranges = Vec::new();
            for &cycle_index in cycle_indices {
                work.charge(1)?;
                let cycle = all_cycles.get(cycle_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q axis-band cycle is unknown".into())
                })?;
                for &dart_id in &cycle.darts {
                    work.charge(1)?;
                    let dart = topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q axis-band dart is unknown".into())
                    })?;
                    let span = topology.q.spans.get(dart.span.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q axis-band span is unknown".into())
                    })?;
                    for owner in &span.owners {
                        work.charge(1)?;
                        if owner.source_edge == edge_id {
                            work.charge(1)?;
                            ranges.push(owner.source_range.clone());
                        }
                    }
                }
            }
            if !source_ranges_cover_unit_interval(&mut ranges, work)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }

    let mut ranges_by_edge = BTreeMap::<SourceEdgeId, Vec<ExactSourceRange>>::new();
    for &cycle_index in cycle_indices {
        work.charge(1)?;
        let cycle = all_cycles
            .get(cycle_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source-ring cycle is unknown".into()))?;
        for &dart_id in &cycle.darts {
            work.charge(1)?;
            let dart = topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source-ring dart is unknown".into())
            })?;
            let span = topology.q.spans.get(dart.span.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source-ring span is unknown".into())
            })?;
            for owner in &span.owners {
                work.charge(1)?;
                let edge = source_edge_at(topology, owner.source_edge)?;
                if matches!(
                    edge.kind,
                    SourceEdgeKind::Ring { ring, .. } if ring == source_ring.id
                ) {
                    ranges_by_edge
                        .entry(edge.id)
                        .or_default()
                        .push(owner.source_range.clone());
                }
            }
        }
    }
    for edge_index in source_ring.source_edges.clone() {
        work.charge(1)?;
        let edge =
            topology.sources.edges.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring edge is unknown".into())
            })?;
        if matches!(edge.kind, SourceEdgeKind::Ring {
            kind: crate::geometry::RingKind::Collapsed,
            ..
        }) {
            ranges_by_edge.remove(&edge.id);
            continue;
        }
        let Some(ranges) = ranges_by_edge.get_mut(&edge.id) else {
            return Ok(false);
        };
        if !source_ranges_cover_unit_interval(ranges, work)? {
            return Ok(false);
        }
        ranges_by_edge.remove(&edge.id);
    }
    Ok(ranges_by_edge.is_empty())
}

fn source_ranges_cover_unit_interval(
    ranges: &mut [ExactSourceRange],
    work: &mut impl Work,
) -> Result<bool> {
    try_sort_by_with_work(ranges, work, |left, right, work| {
        work.charge(1)?;
        left.start.try_cmp(&right.start)
    })?;
    let mut covered = ExactRational::zero();
    let one = ExactRational::one();
    for range in ranges {
        work.charge(1)?;
        if range.start.try_cmp(&covered)?.is_gt() {
            return Ok(false);
        }
        if range.end.try_cmp(&covered)?.is_gt() {
            covered = range.end.clone();
        }
    }
    Ok(covered.try_cmp(&one)?.is_ge())
}

fn append_source_ring_live_nodes(
    topology: &ExactGeographicTopology,
    source_ring: &ExactSourceRing,
    live_nodes: &mut Vec<QNodeId>,
    work: &mut impl Work,
) -> Result<()> {
    for edge_index in source_ring.source_edges.clone() {
        work.charge(1)?;
        let edge =
            topology.sources.edges.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring edge is unknown".into())
            })?;
        let ports = topology
            .q
            .source_edge_ports
            .get(edge.id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source ring ports are unknown".into()))?;
        for &port_id in ports {
            work.charge(1)?;
            let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring port is unknown".into())
            })?;
            live_nodes.push(port.node);
        }
    }
    Ok(())
}

fn draw_cycle_vertices<'topology>(
    topology: &'topology ExactGeographicTopology,
    coordinates: &QCoordinates,
    cycle: &BoundaryCycle,
    work: &mut impl Work,
) -> Result<Vec<EmittedVertex<'topology>>> {
    let mut vertices = Vec::with_capacity(cycle.darts.len().saturating_add(1));
    for_each_q_cycle_vertex(
        topology,
        &cycle.darts,
        |vertex, work| {
            let owner_index = area_owner_index(topology, vertex.span, work)?;
            vertices.push(emitted_point(
                topology,
                coordinates,
                vertex.span,
                owner_index,
                vertex.port,
            )?);
            Ok(())
        },
        work,
    )?;
    Ok(vertices)
}

fn draw_lines<'topology>(
    topology: &'topology ExactGeographicTopology,
    paths: &[PlannedQLine],
    coordinates: &QCoordinates,
    work: &mut impl Work,
) -> Result<Vec<DrawnQLine<'topology>>> {
    let q = &topology.q;
    let mut output = Vec::new();
    for line in paths {
        work.charge(1)?;
        let recipe_walk = line.recipe_walk;
        let path = &line.path;
        let mut start = None;
        for (index, step) in path.iter().enumerate() {
            work.charge(1)?;
            let span = q
                .spans
                .get(step.span)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
            let [start_port, end_port] = span.ports;
            let from_port = match step.from_endpoint {
                SpanEndpoint::Start => start_port,
                SpanEndpoint::End => end_port,
            };
            let port = q.ports.get(from_port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line span has an unknown port".into())
            })?;
            if !port.cut {
                start = Some(index);
                break;
            }
        }
        let start = start.unwrap_or(0);
        let (before_start, from_start) = path.split_at(start);
        let mut points: Vec<EmittedVertex<'topology>> = Vec::new();
        let mut previous: Option<EmittedVertex<'topology>> = None;
        for step in from_start.iter().chain(before_start) {
            work.charge(1)?;
            let span = q
                .spans
                .get(step.span)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
            let [start_port, end_port] = span.ports;
            let (from_port, to_port) = match step.from_endpoint {
                SpanEndpoint::Start => (start_port, end_port),
                SpanEndpoint::End => (end_port, start_port),
            };
            let from = emitted_point(topology, coordinates, step.span, step.owner, from_port)?;
            let to = emitted_point(topology, coordinates, step.span, step.owner, to_port)?;
            let from_cut = q
                .ports
                .get(from_port.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q line port is unknown".into()))?
                .cut;
            let to_cut = q
                .ports
                .get(to_port.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q line port is unknown".into()))?
                .cut;

            if !from_cut {
                if let Some(previous) = previous {
                    if previous.port.node != from.port.node {
                        return Err(GeometryErrorKind::Invalid(
                            "Q line path changed quotient nodes between spans".into(),
                        )
                        .into());
                    }
                    if previous.point != from.point {
                        let previous_key =
                            emitted_node_key(topology.sources.frame, previous.point.xy())?;
                        let from_key = emitted_node_key(topology.sources.frame, from.point.xy())?;
                        if previous_key != from_key {
                            return Err(GeometryErrorKind::ExactNodeCollision {
                                first: previous.port.node.0,
                                second: from.port.node.0,
                            }
                            .into());
                        }
                        let is_seam = matches!(
                            topology
                                .sources
                                .frame
                                .longitude()
                                .canonicalize(from.point.x)
                                .map_err(|error| {
                                    GeometryErrorKind::Invalid(error.to_string().into())
                                })?
                                .identity(topology.sources.frame.longitude()),
                            SeamIdentity::Seam
                        );
                        let is_pole = matches!(
                            from_key,
                            EmittedNodeKey::NorthPole | EmittedNodeKey::SouthPole
                        );
                        if is_seam || is_pole {
                            push_line_part(&mut output, line.origin, recipe_walk, &mut points);
                        } else if points
                            .last()
                            .is_some_and(|last: &EmittedVertex<'_>| last.point != previous.point)
                        {
                            return Err(GeometryErrorKind::Invalid(
                                "Q line endpoint aliases disagree away from a quotient singularity"
                                    .into(),
                            )
                            .into());
                        }
                    }
                }
                if points
                    .last()
                    .is_none_or(|last: &EmittedVertex<'_>| last.point != from.point)
                {
                    points.push(from);
                }
            }
            if !to_cut {
                points.push(to);
            }
            previous = Some(to);
        }
        push_line_part(&mut output, line.origin, recipe_walk, &mut points);
    }
    Ok(output)
}

fn draw_points<'topology>(
    topology: &'topology ExactGeographicTopology,
    nodes: &[QNodeId],
    source_points: &[Option<EmittedVertex<'topology>>],
    coordinates: &QCoordinates,
    work: &mut impl Work,
) -> Result<Vec<EmittedVertex<'topology>>> {
    let q = &topology.q;
    let mut output = Vec::with_capacity(nodes.len());
    for &node in nodes {
        work.charge(1)?;
        let vertex = if let Some(source) = source_points.get(node.0 as usize).copied().flatten() {
            source
        } else {
            let node_data = q
                .nodes
                .get(node.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q point node is unknown".into()))?;
            let port_id = *q.node_ports.get(node_data.ports.start).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q point node has no first port".into())
            })?;
            let port = q
                .ports
                .get(port_id.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q point port is unknown".into()))?;
            EmittedVertex {
                port,
                point: coordinates.point(topology, port_id)?,
                literal_image: QSourceImage::Port(port_id),
            }
        };
        output.push(vertex);
    }
    Ok(output)
}

#[cfg(test)]
fn source_point_coordinates(
    topology: &ExactGeographicTopology,
) -> Result<Vec<Option<EmittedVertex<'_>>>> {
    let mut work = crate::geometry::Unmetered;
    source_point_coordinates_with_work(topology, &mut work)
}

fn source_point_coordinates_with_work<'topology>(
    topology: &'topology ExactGeographicTopology,
    work: &mut impl Work,
) -> Result<Vec<Option<EmittedVertex<'topology>>>> {
    work.charge(
        u64::try_from(
            topology
                .sources
                .points
                .len()
                .checked_add(topology.q.nodes.len())
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut source_points = vec![None; topology.q.nodes.len()];
    for (source_index, (source, &port)) in topology
        .sources
        .points
        .iter()
        .zip(topology.q.source_point_ports.iter())
        .enumerate()
    {
        let q_port =
            topology.q.ports.get(port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source point port is unknown".into())
            })?;
        let slot = source_points
            .get_mut(q_port.node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source point node is unknown".into()))?;
        if slot.is_none() {
            *slot = Some(EmittedVertex {
                port: q_port,
                point: Point::new_raw_xy(source.point.x, source.point.y),
                literal_image: QSourceImage::SourcePoint(
                    u32::try_from(source_index).map_err(|_| ResourceError::SizeOverflow)?,
                ),
            });
        }
    }
    Ok(source_points)
}

#[cfg(test)]
fn boundary_source_points(
    topology: &ExactGeographicTopology,
    row: SourceRowId,
) -> Result<Vec<Option<EmittedVertex<'_>>>> {
    let mut work = crate::geometry::Unmetered;
    boundary_source_points_with_work(topology, row, &mut work)
}

fn boundary_source_points_with_work<'topology>(
    topology: &'topology ExactGeographicTopology,
    row: SourceRowId,
    work: &mut impl Work,
) -> Result<Vec<Option<EmittedVertex<'topology>>>> {
    let q = &topology.q;
    work.charge(u64::try_from(q.nodes.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut selected = Vec::with_capacity(q.nodes.len());
    selected.resize(q.nodes.len(), None::<(bool, SourceEdgeId, QPortId, Point)>);
    for (span_index, span) in q.spans.iter().enumerate() {
        work.charge(1)?;
        if span.nodes[0] == span.nodes[1] || !q.collapsed_rows_for_span(span_index)?.contains(&row)
        {
            continue;
        }
        for owner in &span.owners {
            work.charge(1)?;
            let edge = source_edge_at(topology, owner.source_edge)?;
            if edge.row != row
                || !matches!(edge.kind, SourceEdgeKind::Ring {
                    kind: crate::geometry::RingKind::Collapsed,
                    ..
                })
            {
                continue;
            }
            let [start_port, end_port] = q
                .source_edge_ports
                .get(edge.id.0 as usize)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q collapsed edge has no source ports".into())
                })?;
            let (start_parameter, end_parameter) = if owner.reversed {
                (&owner.source_range.end, &owner.source_range.start)
            } else {
                (&owner.source_range.start, &owner.source_range.end)
            };
            let [start_node, end_node] = span.nodes;
            for (node, parameter) in [(start_node, start_parameter), (end_node, end_parameter)] {
                work.charge(1)?;
                // Prefer a stored source start at the node; its ring traversal spelling wins
                // over the canonical chart port and over an incoming edge's endpoint alias.
                let candidate = if parameter.is_zero() {
                    Some((false, edge.id, start_port, edge.source[0]))
                } else if parameter.is_one() {
                    Some((true, edge.id, end_port, edge.source[1]))
                } else {
                    None
                };
                let Some(candidate) = candidate else {
                    continue;
                };
                let stored = selected.get_mut(node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source endpoint has an unknown node".into())
                })?;
                if stored
                    .is_none_or(|previous| (candidate.0, candidate.1) < (previous.0, previous.1))
                {
                    *stored = Some(candidate);
                }
            }
        }
    }
    let mut output = Vec::with_capacity(selected.len());
    for entry in selected {
        work.charge(1)?;
        let Some((is_end, edge, port_id, point)) = entry else {
            output.push(None);
            continue;
        };
        let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q collapsed edge endpoint port is unknown".into())
        })?;
        output.push(Some(EmittedVertex {
            port,
            point,
            literal_image: if is_end {
                QSourceImage::CollapsedEnd(edge)
            } else {
                QSourceImage::CollapsedStart(edge)
            },
        }));
    }
    Ok(output)
}

fn record_emitted_node(
    frame: crate::geographic::GeographicFrame,
    node: QNodeId,
    point: Point,
    live: &mut Vec<(EmittedNodeKey, QNodeId)>,
) -> Result<()> {
    live.push((emitted_node_key(frame, point.xy())?, node));
    Ok(())
}

fn with_axes(point: Point, ordinates: NodeOrdinates, axes: CoordinateAxes) -> Result<Point> {
    let z = if axes.has_z() {
        Some(ordinates.z.value().ok_or_else(|| {
            GeometryErrorKind::Invalid("retained Q output Z has no source interval".into())
        })?)
    } else {
        None
    };
    let m = if axes.has_m() {
        Some(ordinates.m.value().ok_or_else(|| {
            GeometryErrorKind::Invalid("retained Q output M has no source interval".into())
        })?)
    } else {
        None
    };
    Ok(Point::new_raw_axes(
        point.x,
        point.y,
        ZOrdinate(z),
        MOrdinate(m),
    ))
}

pub(super) fn with_vertex_axes(
    vertex: EmittedVertex<'_>,
    ordinates: &[NodeOrdinates],
    axes: CoordinateAxes,
) -> Result<Point> {
    let source = ordinates
        .get(vertex.port.node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q output node has no ordinates".into()))?;
    with_axes(vertex.point, *source, axes)
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::panic_in_result_fn,
        reason = "Result-returning tests use assertions for behavior and ? for fallible setup"
    )]

    use super::*;
    use crate::geographic::{AngularFactor, GeographicFrame, GeographicInputs};
    use crate::geometry::Coordinates as _;
    use crate::geometry::overlay::{Strictness, overlay_all};
    use crate::geometry::topology::q_embedding::{
        QFaceStructure, canonicalize_ring_vertices, compare_q_ring_role,
        face_structure_mismatch_nodes, round_canonical_longitude, subgraph_chart,
    };

    fn build_q_plan(
        topology: &ExactGeographicTopology,
        operation: OverlayOp,
    ) -> Result<QEmissionPlan> {
        let mut work = crate::geometry::Unmetered;
        QEmissionPlan::build(topology, operation, &mut work)
    }

    fn build_q_boundary_plan(
        topology: &ExactGeographicTopology,
    ) -> Result<(QEmissionPlan, BoundarySupport)> {
        let mut work = crate::geometry::Unmetered;
        QEmissionPlan::build_boundary(topology, &mut work)
    }

    fn build_q_emission_unmetered<'topology, 'emission>(
        plan: &'emission QEmissionPlan,
        topology: &'topology ExactGeographicTopology,
        source_points: &'emission [Option<EmittedVertex<'topology>>],
        publication: &'emission PublicationInventory,
    ) -> Result<QEmbeddingEmission<'topology, 'emission>> {
        let mut work = crate::geometry::Unmetered;
        plan.embedding_emission(topology, source_points, publication, &mut work)
    }

    fn draw_q_plan_unmetered<'topology>(
        plan: &QEmissionPlan,
        topology: &'topology ExactGeographicTopology,
        coordinates: &QCoordinates,
        source_points: &[Option<EmittedVertex<'topology>>],
    ) -> Result<QEmissionDrawing<'topology>> {
        let mut work = crate::geometry::Unmetered;
        plan.drawing(topology, coordinates, source_points, &mut work)
    }

    #[test]
    fn canonical_full_band_ring_uses_its_chart_orientation() {
        let Shape::Polygon(polygon) = crate::io::parse_wkt(
            "POLYGON ((-180 -80, -60 -80, 60 -80, -180 -80, \
             -180 80, 60 80, -60 80, -180 80, -180 -80))",
        )
        .unwrap() else {
            unreachable!()
        };
        let frame = GeographicFrame::crs84();
        assert_eq!(
            super::super::q_embedding::quotient_ring_role(frame, &polygon.shell).unwrap(),
            Some(false),
        );
        assert_eq!(
            super::super::q_embedding::quotient_ring_role(
                frame,
                &polygon.shell.reversed().unwrap(),
            )
            .unwrap(),
            Some(true),
        );
    }

    fn exact_boundary(shape: &Shape) -> Shape {
        let frame = GeographicFrame::crs84();
        let inputs = GeographicInputs::try_from_shapes(frame, std::iter::once(shape))
            .expect("source admits");
        let topology =
            ExactGeographicTopology::from_views(frame, inputs.views().iter().copied()).unwrap();
        exact_geographic_boundary::<crate::crs::GeodesicMetric>(&topology, shape.axes(), None)
            .unwrap()
    }

    fn source_boundary_sink(
        topology: &ExactGeographicTopology,
        plan: &QEmissionPlan,
    ) -> Result<(Shape, ShapeLiteralLayout<QSourceImage>)> {
        let source_points = boundary_source_points(topology, SourceRowId(0))?;
        source_sink(topology, plan, &source_points)
    }

    fn source_sink<'topology>(
        topology: &'topology ExactGeographicTopology,
        plan: &QEmissionPlan,
        source_points: &[Option<EmittedVertex<'topology>>],
    ) -> Result<(Shape, ShapeLiteralLayout<QSourceImage>)> {
        let mut work = crate::geometry::Unmetered;
        let nearest = QCoordinates::build(topology)?;
        let drawing = plan.drawing(topology, &nearest, source_points, &mut work)?;
        let ring_roles = certify_nearest_ring_roles(topology, &drawing, &mut work)?;
        let ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
        let axes = CoordinateAxes::XY;
        let (points, lines, polygons, layout) = write_drawing(
            drawing,
            &ring_roles,
            &ordinates,
            None,
            None,
            None,
            axes,
            &mut work,
        )?;
        let (shape, order) =
            seal_output_parts(plan, points, lines, polygons, axes, axes, true, &mut work)?;
        let layout = layout.in_output_order(
            order.ok_or_else(|| {
                GeometryErrorKind::Invalid("source boundary lost its literal row order".into())
            })?,
            &mut work,
        )?;
        Ok((shape, layout))
    }

    fn bind_source_shape(
        topology: &ExactGeographicTopology,
        shape: Shape,
        layout: ShapeLiteralLayout<QSourceImage>,
    ) -> Result<LiteralCheck<QSourceImage>> {
        let mut work = crate::geometry::Unmetered;
        let images = super::super::q_embedding::QSourceImages {
            topology,
            source_ports: None,
            publication: &[],
        };
        let ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
        let mut chart = super::super::q_embedding::AssignedSourceChart {
            topology,
            source_ports: None,
            source_ordinates: &ordinates,
            publication_points: &[],
            publication_ordinates: &[],
        };
        bind_literals(
            FrozenLiteralBuffers::shapes(shape, layout),
            &mut chart,
            &images,
            &mut work,
        )
    }

    fn test_node_ordinates(topology: &ExactGeographicTopology) -> Result<Vec<NodeOrdinates>> {
        let mut work = crate::geometry::Unmetered;
        node_ordinates::<crate::crs::GeodesicMetric>(topology, CoordinateAxes::XY, None, &mut work)
    }

    #[test]
    fn meridian_recipe_uses_latitude_for_its_producer_parameter() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let shape = crate::io::parse_wkt("LINESTRING ZM (0 0 1 10, 0 10 3 30)")?;
        let inputs = GeographicInputs::try_from_shapes(frame, std::iter::once(&shape))
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let topology = ExactGeographicTopology::from_views(frame, inputs.views().iter().copied())?;
        let plan = build_q_plan(&topology, OverlayOp::Union)?;
        let recipe = plan
            .fragment_recipes
            .as_slice()
            .iter()
            .find(|recipe| recipe.use_kind == super::super::emission::SelectedUse::Curve)
            .ok_or_else(|| GeometryErrorKind::Invalid("meridian recipe is missing".into()))?;
        if !recipe.exact_displacement.is_zero() {
            return Err(GeometryErrorKind::Invalid(
                "meridian recipe acquired a longitude displacement".into(),
            )
            .into());
        }
        let range = recipe.ranges.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("meridian recipe has no source range".into())
        })?;
        assert_eq!(range.producer_start, ExactRational::zero());
        assert_eq!(range.producer_end, ExactRational::one());

        let mut work = crate::geometry::Unmetered;
        let trace = plan
            .fragment_recipes
            .source_trace_fragment(recipe.id, &mut work)?;
        let requests = trace
            .rows
            .requests_at(0, &ExactRational::from_f64(0.5), &mut work)?;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].parameter(), &ExactRational::from_f64(0.5));
        Ok(())
    }

    #[test]
    fn synthetic_cut_is_affine_and_source_vertices_split_recipes() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let line = crate::io::parse_wkt(
            "LINESTRING ZM (-180 0 1 10, -60 0 2 20, 60 0 3 30, \
             120 10 4 40, 180 0 5 50)",
        )?;
        let point = crate::io::parse_wkt("POINT (-120 90)")?;
        let shapes = [line, point];
        let inputs = GeographicInputs::try_from_shapes(frame, shapes.iter())
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let topology = ExactGeographicTopology::from_views(frame, inputs.views().iter().copied())?;
        let plan = build_q_plan(&topology, OverlayOp::Union)?;
        let directed_ports = |step: LineStep| -> Result<[QPortId; 2]> {
            let span = topology.q.spans.get(step.span).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q curve path names an unknown span".into())
            })?;
            Ok(match step.from_endpoint {
                SpanEndpoint::Start => span.ports,
                SpanEndpoint::End => [span.ports[1], span.ports[0]],
            })
        };
        let mut cut_join = None;
        for (line_index, line_path) in plan
            .line_paths
            .iter()
            .enumerate()
            .filter(|(_, line)| line.origin == QLineOrigin::Curve)
        {
            let (Some(first_step), Some(last_step)) = (
                line_path.path.first().copied(),
                line_path.path.last().copied(),
            ) else {
                continue;
            };
            let first_from = directed_ports(first_step)?[0];
            let last_to = directed_ports(last_step)?[1];
            let first_node = topology
                .q
                .ports
                .get(first_from.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("curve start port is unknown".into()))?
                .node;
            let last_node = topology
                .q
                .ports
                .get(last_to.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("curve end port is unknown".into()))?
                .node;
            let line_closed = first_node == last_node;
            for (step_index, &left) in line_path.path.iter().enumerate() {
                let right = match line_path.path.get(step_index + 1).copied() {
                    Some(right) => right,
                    None if line_closed => *line_path.path.first().ok_or_else(|| {
                        GeometryErrorKind::Invalid("closed curve path is empty".into())
                    })?,
                    None => continue,
                };
                let [left_from, left_to] = directed_ports(left)?;
                let [right_from, _] = directed_ports(right)?;
                let left_span =
                    topology.q.spans.get(left.span).ok_or_else(|| {
                        GeometryErrorKind::Invalid("left Q span is unknown".into())
                    })?;
                let right_span =
                    topology.q.spans.get(right.span).ok_or_else(|| {
                        GeometryErrorKind::Invalid("right Q span is unknown".into())
                    })?;
                let left_port =
                    topology.q.ports.get(left_to.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("left cut port is unknown".into())
                    })?;
                let right_port = topology.q.ports.get(right_from.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("right cut port is unknown".into())
                })?;
                if !left_port.cut || !right_port.cut || left_port.node != right_port.node {
                    continue;
                }
                let left_owner = left_span.owners.get(left.owner).ok_or_else(|| {
                    GeometryErrorKind::Invalid("left cut span has no source owner".into())
                })?;
                let right_owner = right_span.owners.get(right.owner).ok_or_else(|| {
                    GeometryErrorKind::Invalid("right cut span has no source owner".into())
                })?;
                if left_owner.source_edge != right_owner.source_edge {
                    continue;
                }
                let left_forward = (left_from == left_span.ports[0]) != left_owner.reversed;
                let right_forward = (right_from == right_span.ports[0]) != right_owner.reversed;
                let left_end = if left_forward {
                    &left_owner.source_range.end
                } else {
                    &left_owner.source_range.start
                };
                let right_start = if right_forward {
                    &right_owner.source_range.start
                } else {
                    &right_owner.source_range.end
                };
                if left_end != right_start {
                    continue;
                }
                cut_join = Some((
                    line_index,
                    line_closed,
                    left_owner.source_edge,
                    left_end.clone(),
                ));
                break;
            }
            if cut_join.is_some() {
                break;
            }
        }
        let (line_index, line_closed, source_edge, source_parameter) =
            cut_join.ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "curve paths have no adjacent or closure synthetic cut ports".into(),
                )
            })?;

        let walk = plan
            .fragment_recipes
            .walks()
            .iter()
            .find(|walk| walk.owner == super::super::emission::RecipeWalkOwner::Line(line_index))
            .ok_or_else(|| GeometryErrorKind::Invalid("full-turn recipe walk is missing".into()))?;
        let mut cut_recipe = None;
        let mut recipe_ranges = Vec::new();
        for &recipe_id in &walk.fragments {
            let recipe = plan
                .fragment_recipes
                .as_slice()
                .get(recipe_id.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("line recipe is unknown".into()))?;
            let recipe_sources = recipe
                .ranges
                .iter()
                .map(|range| range.source_edge)
                .collect::<BTreeSet<_>>();
            if !recipe.ranges.is_empty() {
                assert_eq!(recipe_sources.len(), 1);
            }
            recipe_ranges.extend(recipe.ranges.iter().map(|range| {
                (
                    range.source_edge,
                    range.range.start.clone(),
                    range.range.end.clone(),
                    range.producer_start.clone(),
                    range.producer_end.clone(),
                )
            }));
        }
        for index in 0..recipe_ranges.len() {
            let left = recipe_ranges
                .get(index)
                .ok_or_else(|| GeometryErrorKind::Invalid("cut recipe range is unknown".into()))?;
            let right = match recipe_ranges.get(index + 1) {
                Some(right) => right,
                None if line_closed => recipe_ranges.first().ok_or_else(|| {
                    GeometryErrorKind::Invalid("closed cut recipe has no range".into())
                })?,
                None => continue,
            };
            if left.0 == source_edge
                && right.0 == source_edge
                && left.2 == source_parameter
                && right.1 == source_parameter
            {
                cut_recipe = Some((left.3.clone(), left.4.clone(), right.4.clone()));
                break;
            }
        }
        let (start, middle, end) = cut_recipe.ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "synthetic cut join did not retain adjacent source intervals".into(),
            )
        })?;
        assert_eq!(start, ExactRational::zero());
        assert_eq!(source_parameter, ExactRational::from_f64(0.5));
        assert_eq!(middle, ExactRational::from_f64(0.5));
        assert_eq!(end, ExactRational::one());
        Ok(())
    }

    fn resolve_q_embedding_xy(
        topology: &ExactGeographicTopology,
        plan: &QEmissionPlan,
        source_points: &[Option<EmittedVertex<'_>>],
        nearest: &QCoordinates,
        operation: &'static str,
    ) -> Result<CertifiedQEmission> {
        let metric_access: Option<&SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric>> =
            None;
        let mut budget = EvaluationBudget::new(operation, 0)?;
        let mut preparation_work = EvaluationOnly::new(&mut budget);
        let mut expansion_budget = ExpansionBudget::new(operation, "computed edges");
        resolve_q_embedding(
            topology,
            plan,
            source_points,
            nearest,
            CoordinateAxes::XY,
            false,
            operation,
            metric_access,
            &mut expansion_budget,
            &mut preparation_work,
        )
    }

    fn certify_q_embedding_nearest_xy<'topology>(
        topology: &'topology ExactGeographicTopology,
        emission: &QEmbeddingEmission<'topology, '_>,
        nearest: &QCoordinates,
        live_keys: &[(EmittedNodeKey, QNodeId)],
        drawing: QEmissionDrawing<'topology>,
        operation: &'static str,
        budget: &mut EvaluationBudget,
    ) -> Result<NearestQCertificate> {
        let ordinates = test_node_ordinates(topology)?;
        let metric_access: Option<&SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric>> =
            None;
        certify_q_embedding_nearest(
            topology,
            emission,
            nearest,
            live_keys,
            drawing,
            &ordinates,
            CoordinateAxes::XY,
            false,
            operation,
            metric_access,
            budget,
        )
    }

    fn q_union_one_source(shape: &Shape, frame: GeographicFrame) -> Shape {
        let inputs = GeographicInputs::try_from_shapes(frame, std::iter::once(shape))
            .expect("source admits");
        let topology =
            ExactGeographicTopology::from_views_as_set(frame, inputs.views().iter().copied())
                .expect("Q topology builds");
        exact_geographic_overlay::<crate::crs::GeodesicMetric>(
            &topology,
            OverlayOp::Union,
            shape.axes(),
            false,
            None,
        )
        .expect("Q source-only union emits")
    }

    #[test]
    fn q_intersection_classifies_a_shared_ring_edge_as_areal_linework() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let left = crate::io::parse_wkt("POLYGON ((179 0, -179 0, -179.8 -1, 179 0))")?;
        let right = crate::io::parse_wkt("POLYGON ((-179 0, 179 0, 179.8 1, -179 0))")?;
        let shapes = [left, right];
        let inputs = GeographicInputs::try_from_shapes(frame, shapes.iter())
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let topology =
            ExactGeographicTopology::from_views_as_set(frame, inputs.views().iter().copied())?;
        let plan = build_q_plan(&topology, OverlayOp::Intersection)?;
        assert!(!plan.line_paths.is_empty());
        assert!(
            plan.line_paths
                .iter()
                .all(|line| line.origin == QLineOrigin::ArealBoundary)
        );
        assert!(
            plan.fragment_recipes
                .as_slice()
                .iter()
                .any(|recipe| recipe.use_kind == SelectedUse::ArealBoundaryLine)
        );
        Ok(())
    }

    fn emitted_single_point(shape: &Shape) -> Point {
        match shape {
            Shape::Point(point) => *point,
            Shape::MultiPoint(points) if points.len() == 1 => points.point_at(0),
            Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..)
            | Shape::MultiPoint(_) => panic!("Q emitted one point, got {shape:?}"),
        }
    }

    fn collect_points(shape: &Shape, output: &mut Vec<Point>) {
        match shape {
            Shape::Point(point) => output.push(*point),
            Shape::MultiPoint(points) => output.extend(points.iter_coords()),
            Shape::GeometryCollection(geometries) => {
                for geometry in geometries {
                    collect_points(geometry, output);
                }
            },
            Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::Empty(..) => {},
        }
    }

    #[test]
    fn q_source_only_points_keep_admitted_latitude_bits_and_chart_identity() {
        let frame = GeographicFrame::crs84();
        let pole = frame.pole();
        for (source_y, identity) in [
            (
                -0.0,
                EmittedNodeKey::Coordinate(0.0_f64.to_bits(), 0.0_f64.to_bits()),
            ),
            (pole.next_up(), EmittedNodeKey::NorthPole),
        ] {
            let source = Point::new_raw_xy(0.0, source_y);
            let output = q_union_one_source(&Shape::Point(source), frame);
            let emitted = emitted_single_point(&output);

            assert_eq!(emitted.y.to_bits(), source_y.to_bits());
            assert_eq!(emitted_node_key(frame, emitted.xy()).unwrap(), identity);
            assert_eq!(emitted_node_key(frame, source.xy()).unwrap(), identity);
        }
    }

    #[test]
    fn mixed_geographic_overlay_routes_a_retained_source_point_through_q() {
        let frame = GeographicFrame::crs84();
        let polygon = Shape::Polygon(Polygon::new(
            Ring::try_from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(170.0, 2.0),
                    Point::new_raw_xy(-170.0, 2.0),
                    Point::new_raw_xy(-165.0, 12.0),
                    Point::new_raw_xy(160.0, 10.0),
                    Point::new_raw_xy(170.0, 2.0),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        ));
        let source_point = Point::new_raw_xy(0.0, -0.0);
        let shapes = [polygon, Shape::Point(source_point)];
        let inputs =
            GeographicInputs::try_from_shapes(frame, shapes.iter()).expect("sources admit");
        assert_eq!(
            crate::geometry::geographic_topology_route(inputs.views().iter().copied()),
            crate::geometry::GeographicTopologyRoute::Quotient
        );

        let output = overlay_all(
            &shapes,
            crate::geometry::TopologyDomain::Geographic(frame),
            OverlayOp::Union,
            Strictness::Lenient,
        )
        .expect("mixed geographic union emits");
        let mut points = Vec::new();
        collect_points(&output, &mut points);
        assert!(points.iter().any(|point| {
            point.x.to_bits() == source_point.x.to_bits()
                && point.y.to_bits() == source_point.y.to_bits()
        }));
    }

    #[test]
    fn q_source_only_whole_sphere_ring_keeps_source_latitude_bits() {
        let frame = GeographicFrame::crs84();
        let source_points = [
            Point::new_raw_xy(-180.0, -90.0),
            Point::new_raw_xy(-90.0, -90.0),
            Point::new_raw_xy(0.0, -90.0),
            Point::new_raw_xy(90.0, -90.0),
            Point::new_raw_xy(180.0, -90.0),
            Point::new_raw_xy(180.0, 90.0),
            Point::new_raw_xy(90.0, 90.0),
            Point::new_raw_xy(0.0, 90.0),
            Point::new_raw_xy(-90.0, 90.0),
            Point::new_raw_xy(-180.0, 90.0),
            Point::new_raw_xy(-180.0, -0.0),
            Point::new_raw_xy(-180.0, -90.0),
        ];
        let shell = Ring::try_from_closed(CoordSeq::try_from_points(&source_points).unwrap())
            .expect("closed pole-to-pole source ring");
        let source = Shape::Polygon(Polygon::new(shell, Vec::new()));
        let inputs = GeographicInputs::try_from_shapes(frame, std::iter::once(&source))
            .expect("source admits");
        let topology =
            ExactGeographicTopology::from_views_as_set(frame, inputs.views().iter().copied())
                .expect("Q topology builds");

        let plan = build_q_plan(&topology, OverlayOp::Union).expect("build Q plan");
        assert!(matches!(
            &plan.polygons,
            PolygonPlan::WholeSphere(source_polygons) if source_polygons == &[0]
        ));
        let output = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
            &topology,
            OverlayOp::Union,
            source.axes(),
            false,
            None,
        )
        .expect("Q source-only union emits");
        let Shape::Polygon(polygon) = output else {
            panic!("Q source-only whole-sphere ring remains a polygon");
        };
        let emitted = polygon.shell.coords().iter_coords().collect::<Vec<_>>();
        let mut expected_bits = source_points
            .iter()
            .map(|point| (point.x.to_bits(), point.y.to_bits()))
            .collect::<Vec<_>>();
        let mut emitted_bits = emitted
            .iter()
            .map(|point| (point.x.to_bits(), point.y.to_bits()))
            .collect::<Vec<_>>();
        expected_bits.sort_unstable();
        emitted_bits.sort_unstable();
        assert_eq!(emitted_bits, expected_bits);
        assert_eq!(
            emitted.first().unwrap().y.to_bits(),
            emitted.last().unwrap().y.to_bits(),
            "the full closing source record is retained"
        );
        assert_eq!(
            emitted
                .iter()
                .find(|point| {
                    point.x.to_bits() == (-180.0_f64).to_bits()
                        && point.y.to_bits() == (-0.0_f64).to_bits()
                })
                .map(|point| emitted_node_key(frame, point.xy()).unwrap()),
            Some(EmittedNodeKey::Coordinate(
                (-180.0_f64).to_bits(),
                0.0_f64.to_bits()
            ))
        );
    }

    #[test]
    fn q_node_ordinates_return_xy_defaults_before_source_metric_access() {
        let frame = GeographicFrame::crs84();
        let seam_crossing = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_axes(
                        170.0,
                        -10.0,
                        ZOrdinate(Some(-30.0)),
                        MOrdinate(Some(-300.0)),
                    )
                    .unwrap(),
                    Point::new_axes(-170.0, -10.0, ZOrdinate(Some(30.0)), MOrdinate(Some(300.0)))
                        .unwrap(),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let inputs =
            GeographicInputs::try_from_shapes(frame, std::iter::once(&seam_crossing)).unwrap();
        assert_eq!(
            crate::geometry::geographic_topology_route(inputs.views().iter().copied()),
            crate::geometry::GeographicTopologyRoute::Quotient
        );
        let topology =
            ExactGeographicTopology::from_views_as_set(frame, inputs.views().iter().copied())
                .unwrap();
        let metric_accesses = std::cell::Cell::new(0_usize);
        let no_metric = || -> Result<&crate::crs::GeodesicMetric> {
            metric_accesses.set(metric_accesses.get().wrapping_add(1));
            Err(crate::crs::CrsError::invalid(
                "unexpected source-arc metric request",
            ))
        };

        let mut work = crate::geometry::Unmetered;
        let xy = node_ordinates::<crate::crs::GeodesicMetric>(
            &topology,
            CoordinateAxes::XY,
            Some(&no_metric),
            &mut work,
        )
        .unwrap();
        assert!(
            xy.iter()
                .all(|ordinates| ordinates.z.value().is_none() && ordinates.m.value().is_none())
        );
        assert_eq!(xy.len(), topology.q.nodes.len());
        assert_eq!(metric_accesses.get(), 0);
    }

    type TestPoint = (u64, u64);
    type TestEdge = (TestPoint, TestPoint);

    fn edge(left: (f64, f64), right: (f64, f64)) -> TestEdge {
        let left = (left.0.to_bits(), left.1.to_bits());
        let right = (right.0.to_bits(), right.1.to_bits());
        if left <= right {
            (left, right)
        } else {
            (right, left)
        }
    }

    fn line_edges(shape: &Shape) -> Vec<TestEdge> {
        let mut edges = Vec::new();
        let mut append = |line: &CoordSeq| {
            for [start, end] in line.segment_pairs() {
                edges.push(edge((start.x, start.y), (end.x, end.y)));
            }
        };
        match shape {
            Shape::LineString(line) => append(line.as_coords()),
            Shape::MultiLineString(lines) => {
                for line in lines {
                    append(line.as_coords());
                }
            },
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => panic!("area boundary emits linework, got {shape:?}"),
        }
        edges.sort_unstable();
        edges
    }

    fn q_ring_chart_points(
        frame: GeographicFrame,
        coordinates: &[(f64, f64)],
        repeated_start_nodes: &[usize],
    ) -> Result<Vec<Point>> {
        let last = coordinates.len().wrapping_sub(1);
        let ports = coordinates
            .iter()
            .enumerate()
            .map(|(index, &(x, y))| {
                let node = if index == 0 || index == last || repeated_start_nodes.contains(&index) {
                    QNodeId(0)
                } else {
                    QNodeId(u32::try_from(index).expect("fixed Q cycle fits its node IDs"))
                };
                super::super::q::QPort {
                    id: QPortId(u32::try_from(index).expect("fixed Q cycle fits its port IDs")),
                    point: super::super::exact::ExactChartPoint::from_rationals(
                        ExactRational::from_f64(x),
                        ExactRational::from_f64(y),
                    ),
                    node,
                    cut: false,
                }
            })
            .collect::<Vec<_>>();
        let vertices = coordinates
            .iter()
            .zip(&ports)
            .map(|(&(longitude, latitude), port)| EmittedVertex {
                port,
                point: Point::new_raw_xy(longitude, latitude),
                literal_image: QSourceImage::Port(port.id),
            })
            .collect::<Vec<_>>();
        let mut chart_points = vertices
            .iter()
            .map(|vertex| vertex.point)
            .collect::<Vec<_>>();
        let mut work = crate::geometry::Unmetered;
        canonicalize_ring_vertices(frame, &vertices, &mut chart_points, &mut work)?;
        Ok(chart_points)
    }

    const B3_COUNTRIES_FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/osm_countries_0_1.geojson"
    );

    fn b3_read_countries(codes: &[&str]) -> Vec<Shape> {
        let document: serde_json::Value = serde_json::from_slice(
            &std::fs::read(B3_COUNTRIES_FIXTURE).expect("read OSM country GeoJSON fixture"),
        )
        .expect("parse OSM country GeoJSON fixture");
        let features = document
            .get("features")
            .and_then(serde_json::Value::as_array)
            .expect("OSM country feature collection");
        codes
            .iter()
            .map(|code| {
                let feature = features
                    .iter()
                    .find(|feature| {
                        feature
                            .get("properties")
                            .and_then(|properties| properties.get("tags"))
                            .and_then(|tags| tags.get("ISO3166-1:alpha2"))
                            .and_then(serde_json::Value::as_str)
                            == Some(*code)
                    })
                    .unwrap_or_else(|| panic!("country fixture has no ISO alpha-2 code {code}"));
                crate::io::parse_geojson(feature.get("geometry").expect("country geometry"))
                    .expect("parse country geometry")
            })
            .collect()
    }

    fn append_xy_bits(shape: &Shape, output: &mut Vec<(u64, u64)>) {
        let mut append = |point: Point| output.push((point.x.to_bits(), point.y.to_bits()));
        match shape {
            Shape::Point(point) => append(*point),
            Shape::MultiPoint(points) => points.iter().for_each(&mut append),
            Shape::LineString(line) => line.iter().for_each(&mut append),
            Shape::MultiLineString(lines) => {
                for line in lines.iter() {
                    line.iter().for_each(&mut append);
                }
            },
            Shape::Polygon(polygon) => {
                for ring in std::iter::once(&polygon.shell).chain(polygon.holes.iter()) {
                    ring.coords().iter().for_each(&mut append);
                }
            },
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons.iter() {
                    for ring in std::iter::once(&polygon.shell).chain(polygon.holes.iter()) {
                        ring.coords().iter().for_each(&mut append);
                    }
                }
            },
            Shape::GeometryCollection(collection) => {
                for member in collection {
                    append_xy_bits(member, output);
                }
            },
            Shape::Empty(..) => {},
        }
    }

    fn xy_bitstream(shape: &Shape) -> Vec<(u64, u64)> {
        let mut output = Vec::new();
        append_xy_bits(shape, &mut output);
        output
    }

    fn test_topology(shapes: &[Shape]) -> ExactGeographicTopology {
        let frame = GeographicFrame::crs84();
        let views = shapes.iter().map(|shape| {
            crate::geographic::ScalarGeographicView::try_new(frame, shape)
                .expect("test geometry is admitted")
        });
        ExactGeographicTopology::from_views(frame, views).expect("build exact test Q topology")
    }

    #[test]
    fn source_ring_rotation_keeps_publication_samples_with_their_recipes() -> Result<()> {
        use super::super::emission::{
            FeatureFamily, FeatureFamilyParent, FragmentRecipeId, PublishedFragment,
            PublishedSample, SampleProducerKind, SourceTrace,
        };
        use crate::geometry::ordinates::RequestRowId;

        let shape = crate::io::parse_wkt("POLYGON ((0 0, 10 0, 10 10, 0 0))")?;
        let topology = test_topology(&[shape]);
        let source_ring =
            topology.sources.rings.first().ok_or_else(|| {
                GeometryErrorKind::Invalid("sampled source ring is missing".into())
            })?;
        let mut vertices = Vec::with_capacity(source_ring.source_edges.len().saturating_add(1));
        for edge_index in source_ring.source_edges.clone() {
            let edge = topology.sources.edges.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("sampled source ring edge is unknown".into())
            })?;
            let port_id = topology
                .q
                .source_edge_ports
                .get(edge.id.0 as usize)
                .and_then(|ports| ports.first())
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("sampled source ring start port is missing".into())
                })?;
            let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("sampled source ring start port is unknown".into())
            })?;
            vertices.push(EmittedVertex {
                port,
                point: edge.source[0],
                literal_image: QSourceImage::Port(port_id),
            });
        }
        let last_edge = topology
            .sources
            .edges
            .get(source_ring.source_edges.end.checked_sub(1).ok_or_else(|| {
                GeometryErrorKind::Invalid("sampled source ring has no closing edge".into())
            })?)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("sampled source ring closing edge is unknown".into())
            })?;
        let end_port_id = topology
            .q
            .source_edge_ports
            .get(last_edge.id.0 as usize)
            .and_then(|ports| ports.get(1))
            .copied()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("sampled source ring end port is missing".into())
            })?;
        let end_port = topology
            .q
            .ports
            .get(end_port_id.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("sampled source ring end port is unknown".into())
            })?;
        vertices.push(EmittedVertex {
            port: end_port,
            point: last_edge.source[1],
            literal_image: QSourceImage::Port(end_port_id),
        });
        let edge_count = source_ring.source_edges.len();
        if edge_count != 3 {
            return Err(GeometryErrorKind::Invalid(
                "sampled source-ring fixture has an unexpected edge count".into(),
            )
            .into());
        }
        let start_port = vertices
            .get(2)
            .ok_or_else(|| GeometryErrorKind::Invalid("rotated ring start is missing".into()))?
            .port
            .id;
        let mut work = crate::geometry::Unmetered;
        let (rotated_vertices, recipe_rotation) =
            rotate_area_ring_vertices(&vertices, Some(start_port), &mut work)?.ok_or_else(
                || GeometryErrorKind::Invalid("ring rotation was not applied".into()),
            )?;
        assert_eq!(recipe_rotation, 2);

        let recipes = (0..edge_count)
            .map(|index| FragmentRecipeId(u32::try_from(index).expect("three recipe IDs")))
            .collect::<Vec<_>>();
        let owner = RecipeWalkOwner::SourceRing(source_ring.id.0 as usize);
        let walk_rows = [RecipeWalk {
            owner,
            fragments: recipes.clone(),
        }];
        let walks = index_recipe_walks(&walk_rows, &mut work)?;
        let mut fragments = Vec::with_capacity(edge_count);
        let mut samples = Vec::with_capacity(edge_count);
        let mut assigned_samples = Vec::with_capacity(edge_count);
        for (index, recipe) in recipes.iter().copied().enumerate() {
            let start = vertices.get(index).ok_or_else(|| {
                GeometryErrorKind::Invalid("sampled source recipe start is missing".into())
            })?;
            let end = vertices.get(index + 1).ok_or_else(|| {
                GeometryErrorKind::Invalid("sampled source recipe end is missing".into())
            })?;
            let source_sample = Point::new_raw_xy(
                f64::midpoint(start.point.x, end.point.x),
                f64::midpoint(start.point.y, end.point.y),
            );
            let assigned = XY::new(source_sample.x + 0.125, source_sample.y + 0.125);
            let exact_start = ExactChartPoint::from_rationals(
                ExactRational::from_f64(start.point.x),
                ExactRational::from_f64(start.point.y),
            );
            let exact_end = ExactChartPoint::from_rationals(
                ExactRational::from_f64(end.point.x),
                ExactRational::from_f64(end.point.y),
            );
            let exact_sample = ExactChartPoint::from_rationals(
                ExactRational::from_f64(source_sample.x),
                ExactRational::from_f64(source_sample.y),
            );
            fragments.push(PublishedFragment {
                recipe,
                points: vec![start.point, source_sample, end.point],
                trace: SourceTrace::from_edge_rows([Vec::new()], &mut work)?,
                sample_range: index..index + 1,
                source_records: [None, None],
                exact_endpoints: [exact_start, exact_end],
            });
            samples.push(PublishedSample {
                request_row: RequestRowId::from_index(index)?,
                family: FeatureFamily {
                    parent: FeatureFamilyParent::Fragment(recipe),
                    producer: SampleProducerKind::Publication,
                    occurrence: u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?,
                },
                parameter: ExactRational::from_f64(0.5),
                written_vertex: 1,
                point: source_sample,
                exact_point: exact_sample.clone(),
                published_chart_point: exact_sample,
                source_trace: None,
                endpoint_only: false,
            });
            assigned_samples.push(assigned);
        }
        let sample_ordinates = vec![NodeOrdinates::default(); samples.len()];
        let publication = PublicationInventory {
            fragments,
            samples,
            counts: Vec::new(),
            ordinates: sample_ordinates.clone(),
        };
        let source_ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
        let source_publication = SourcePublication {
            inventory: &publication,
            points: &assigned_samples,
            ordinates: &sample_ordinates,
            live_samples: &[true; 3],
        };
        let mut cursors = BTreeMap::new();

        let (ring, locations) = write_ring(
            &rotated_vertices,
            0,
            owner,
            recipe_rotation,
            &source_ordinates,
            None,
            Some(source_publication),
            Some(&walks),
            &mut cursors,
            CoordinateAxes::XY,
            &mut work,
        )?;

        let ring_points = ring.coords().iter().collect::<Vec<_>>();
        assert_eq!(ring_points.len(), 7);
        let written_samples = locations
            .images
            .iter()
            .zip(&ring_points)
            .filter_map(|(image, point)| match image {
                QSourceImage::PublicationSample(sample) => {
                    Some((*sample as usize, point.x, point.y))
                },
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(written_samples, vec![
            (2, assigned_samples[2].x, assigned_samples[2].y),
            (0, assigned_samples[0].x, assigned_samples[0].y),
            (1, assigned_samples[1].x, assigned_samples[1].y),
        ]);
        Ok(())
    }

    #[test]
    fn synthetic_seam_shared_edge_intersection_has_a_native_result() -> Result<()> {
        let shapes = [
            crate::io::parse_wkt("POLYGON ((179 0, -179 0, -179.8 -1, 179 0))")?,
            crate::io::parse_wkt("POLYGON ((-179 0, 179 0, 179.8 1, -179 0))")?,
        ];
        let topology = test_topology(&shapes);
        let intersection = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
            &topology,
            OverlayOp::Intersection,
            CoordinateAxes::XY,
            false,
            None,
        )?;

        assert!(!line_edges(&intersection).is_empty());
        let union = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
            &topology,
            OverlayOp::Union,
            CoordinateAxes::XY,
            false,
            None,
        )?;
        assert!(matches!(union, Shape::Polygon(_) | Shape::MultiPolygon(_)));
        Ok(())
    }

    #[test]
    fn synthetic_sliver_dissolve_has_a_native_result() -> Result<()> {
        let seam = crate::io::parse_wkt("POLYGON ((179 0, -179 0, -179 1, 179 1, 179 0))")?;
        let argentina = crate::io::parse_wkt(
            "MULTIPOLYGON (((-58.10222180941994 -33.010937543229446, -58.084289 -33.0009615, -58.085477253434156 -32.99, -58.089648910878246 -32.99, -58.0878711 -32.9965279, -58.10222180941994 -33.010937543229446)), ((-58.11 -33.00166081596568, -58.10769305699579 -32.99, -58.11 -32.99, -58.11 -33.00166081596568)))",
        )?;
        let uruguay = crate::io::parse_wkt(
            "POLYGON ((-58.11 -33.02, -58.08 -33.02, -58.08 -32.99, -58.085477253434156 -32.99, -58.084289 -33.0009615, -58.10256757394891 -33.011129892448544, -58.0878711 -32.9965279, -58.089648910878246 -32.99, -58.10765630407339 -32.99, -58.11 -33.001904214459806, -58.11 -33.02))",
        )?;
        let topology = test_topology(&[seam, argentina, uruguay]);
        let union = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
            &topology,
            OverlayOp::Union,
            CoordinateAxes::XY,
            false,
            None,
        )?;

        assert!(matches!(union, Shape::Polygon(_) | Shape::MultiPolygon(_)));
        Ok(())
    }

    fn q_grid_parallelogram(u: (i32, i32), v: (i32, i32)) -> Result<Shape> {
        // 2^-46 is the spacing between adjacent binary64 values at 100.0.
        let unit = f64::from_bits(0x3D10_0000_0000_0000);
        let add =
            |left: i32, right: i32| left.checked_add(right).expect("Q grid offsets are bounded");
        let subtract =
            |left: i32, right: i32| left.checked_sub(right).expect("Q grid offsets are bounded");
        let negate = |value: i32| value.checked_neg().expect("Q grid offsets are bounded");
        let points = [
            (add(u.0, v.0), add(u.1, v.1)),
            (add(negate(u.0), v.0), add(negate(u.1), v.1)),
            (subtract(negate(u.0), v.0), subtract(negate(u.1), v.1)),
            (subtract(u.0, v.0), subtract(u.1, v.1)),
            (add(u.0, v.0), add(u.1, v.1)),
        ];
        let wkt = points
            .iter()
            .map(|&(x, y)| {
                format!(
                    "{} {}",
                    100.0 + f64::from(x) * unit,
                    80.0 + f64::from(y) * unit
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        crate::io::parse_wkt(&format!("POLYGON (({wkt}))"))
    }

    fn next_q_grid_pair(random: &mut u64) -> Result<[Shape; 2]> {
        let mut draw = || {
            *random ^= *random << 13;
            *random ^= *random >> 7;
            *random ^= *random << 17;
            i32::try_from(*random % 61)
                .expect("bounded")
                .checked_sub(30)
                .expect("Q grid draw is bounded")
        };
        let determinant = |u: (i32, i32), v: (i32, i32)| {
            i64::from(u.0)
                .checked_mul(i64::from(v.1))
                .expect("Q grid determinant is bounded")
                .checked_sub(
                    i64::from(u.1)
                        .checked_mul(i64::from(v.0))
                        .expect("Q grid determinant is bounded"),
                )
                .expect("Q grid determinant is bounded")
        };
        loop {
            let (a_u, a_v, b_u, b_v) = (
                (draw(), draw()),
                (draw(), draw()),
                (draw(), draw()),
                (draw(), draw()),
            );
            if determinant(a_u, a_v).abs() >= 100 && determinant(b_u, b_v).abs() >= 100 {
                return Ok([
                    q_grid_parallelogram(a_u, a_v)?,
                    q_grid_parallelogram(b_u, b_v)?,
                ]);
            }
        }
    }

    #[derive(Debug, Default)]
    struct QGridSweepCounts {
        pairs: usize,
        nearest_certified: usize,
        search_paths: usize,
    }

    fn q_grid_certificate_sweep(pair_count: usize) -> Result<QGridSweepCounts> {
        let mut random = 0x81C4_4B29_35AF_108D_u64;
        let mut counts = QGridSweepCounts::default();
        for _ in 0..pair_count {
            let operands = next_q_grid_pair(&mut random)?;
            let topology = test_topology(&operands);
            let plan = build_q_plan(&topology, OverlayOp::Intersection)?;
            let source_points = source_point_coordinates(&topology)?;
            let nearest = QCoordinates::build(&topology)?;
            let nearest_nodes = nearest.node.clone();
            let resolved = resolve_q_embedding_xy(
                &topology,
                &plan,
                &source_points,
                &nearest,
                OverlayOp::Intersection.name(),
            )?;
            counts.pairs = counts
                .pairs
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            match resolved.choice {
                QEmbeddingChoice::Nearest => {
                    counts.nearest_certified = counts
                        .nearest_certified
                        .checked_add(1)
                        .ok_or(ResourceError::SizeOverflow)?;
                    assert_eq!(
                        resolved.coordinates.node, nearest_nodes,
                        "nearest certificate pass must publish the nearest table"
                    );
                },
                QEmbeddingChoice::Searched => {
                    counts.search_paths = counts
                        .search_paths
                        .checked_add(1)
                        .ok_or(ResourceError::SizeOverflow)?;
                },
            }
        }
        Ok(counts)
    }

    #[test]
    fn geographic_search_moves_a_constructed_vertex_off_a_rounded_source_vertex() {
        let left = crate::io::parse_wkt("POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))")
            .expect("parse left polygon");
        let right = crate::io::parse_wkt(
            "POLYGON ((0 0.022727272727272728, 45 -1, 45 -2, 0 -2, 0 0.022727272727272728))",
        )
        .expect("parse right polygon");
        let shapes = [left, right];
        let topology = test_topology(&shapes);
        let plan = build_q_plan(&topology, OverlayOp::Intersection).expect("build Q plan");
        let source_points = source_point_coordinates(&topology).expect("source point coordinates");
        let coordinates = QCoordinates::build(&topology).expect("nearest Q coordinates");
        let mut live = plan
            .live_node_keys(&topology, &coordinates, &source_points)
            .expect("nearest live nodes");
        assert_eq!(
            node_injectivity_collisions(&mut live)
                .expect("nearest node collisions")
                .len(),
            2
        );

        let result = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
            &topology,
            OverlayOp::Intersection,
            CoordinateAxes::XY,
            false,
            None,
        )
        .expect("Q embedding search preserves the tiny intersection");
        let Shape::Polygon(polygon) = result else {
            panic!("the overlapping sliver remains one polygon");
        };
        let vertices = polygon.shell.coords().iter().collect::<Vec<_>>();
        assert_eq!(vertices.len(), 4);
        assert!(
            vertices.iter().any(|point| {
                point.x.to_bits() == 1.000_000_000_000_000_2_f64.to_bits()
                    && point.y.to_bits() == 0.0_f64.to_bits()
            }),
            "the collision-free crossing uses the next longitude with its exact latitude: {vertices:?}"
        );
    }

    #[test]
    fn geographic_face_certificate_rejects_a_collision_free_neighbor_with_changed_rotation()
    -> Result<()> {
        let left = crate::io::parse_wkt("POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))")?;
        let right = crate::io::parse_wkt(
            "POLYGON ((0 0.022727272727272728, 45 -1, 45 -2, 0 -2, 0 0.022727272727272728))",
        )?;
        let topology = test_topology(&[left, right]);
        let plan = build_q_plan(&topology, OverlayOp::Intersection)?;
        let source_points = source_point_coordinates(&topology)?;
        let nearest = QCoordinates::build(&topology)?;
        let publication = plan.fragment_recipes.publish(
            &topology,
            &nearest,
            OverlayOp::Intersection.name(),
            &mut crate::geometry::Unmetered,
        )?;
        let emission = build_q_emission_unmetered(&plan, &topology, &source_points, &publication)?;
        let mut live = plan.live_node_keys(&topology, &nearest, &source_points)?;
        let collisions = node_injectivity_collisions(&mut live)?;
        assert_eq!(collisions.len(), 2);

        let one = ExactRational::from_f64(1.0);
        let mut moved = None;
        for &node_id in &collisions {
            let node =
                topology.q.nodes.get(node_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("collision node is unknown".into())
                })?;
            let ports = topology
                .q
                .node_ports
                .get(node.ports.clone())
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("collision node ports are unknown".into())
                })?;
            for &port_id in ports {
                let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("collision port is unknown".into())
                })?;
                if !port.cut && port.point.x.try_cmp(&one)?.is_gt() && port.point.y.is_zero() {
                    moved = Some(node_id);
                    break;
                }
            }
        }
        let moved = moved.ok_or_else(|| {
            GeometryErrorKind::Invalid("constructed crossing node is missing".into())
        })?;
        let mut assigned = nearest.clone();
        let candidate = assigned.xy(moved)?;
        *assigned.node.get_mut(moved.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("constructed Q node coordinate is missing".into())
        })? = crate::geometry::XY::new(candidate.x.next_down(), candidate.y);
        let mut assigned_live = plan.live_node_keys(&topology, &assigned, &source_points)?;
        assert!(node_injectivity_collisions(&mut assigned_live)?.is_empty());

        let exact_chart = subgraph_chart(
            &topology,
            &emission,
            &nearest,
            None,
            &mut crate::geometry::Unmetered,
        )?;
        let exact_faces = QFaceStructure::build(&topology, &emission, exact_chart)?;
        let assigned_chart = subgraph_chart(
            &topology,
            &emission,
            &nearest,
            Some(&assigned),
            &mut crate::geometry::Unmetered,
        )?;
        let assigned_faces = QFaceStructure::build(&topology, &emission, assigned_chart)?;
        let witnesses =
            face_structure_mismatch_nodes(&exact_faces, &assigned_faces, &emission.live_nodes)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "collision-free next-down assignment preserved the exact Q faces".into(),
                    )
                })?;
        assert!(
            witnesses.contains(&moved),
            "rotation-system witness should name the moved crossing node: {witnesses:?}"
        );
        Ok(())
    }

    #[test]
    fn q_nearest_even_tie_is_published_byte_identically_when_certified() -> Result<()> {
        let shapes = [
            q_grid_parallelogram((30, -22), (-10, 27))?,
            q_grid_parallelogram((4, 13), (10, -18))?,
        ];
        let topology = test_topology(&shapes);
        let plan = build_q_plan(&topology, OverlayOp::Intersection)?;
        let source_points = source_point_coordinates(&topology)?;
        let nearest = QCoordinates::build(&topology)?;
        let publication = plan.fragment_recipes.publish(
            &topology,
            &nearest,
            OverlayOp::Intersection.name(),
            &mut crate::geometry::Unmetered,
        )?;
        let emission = build_q_emission_unmetered(&plan, &topology, &source_points, &publication)?;
        let mut live_node_keys = plan.live_node_keys(&topology, &nearest, &source_points)?;
        assert!(
            node_injectivity_collisions(&mut live_node_keys)?.is_empty(),
            "the nearest output nodes are injective"
        );
        assert!(
            nearest.has_inexact_nodes(&topology, live_node_keys.iter().map(|(_, node)| *node),)?,
            "the fixture has constructed intersections that need certification"
        );

        let midpoint = ExactRational::from_f64(100.0)
            .try_subtract(&ExactRational::from_f64(2.0_f64.powi(-47)))?;
        let node = QNodeId(5);
        let q_node = topology
            .q
            .nodes
            .get(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("tie witness node is missing".into()))?;
        let port_id = topology
            .q
            .node_ports
            .get(q_node.ports.clone())
            .and_then(|ports| {
                ports.iter().copied().find(|port| {
                    topology
                        .q
                        .ports
                        .get(port.0 as usize)
                        .is_some_and(|port| !port.cut)
                })
            })
            .ok_or_else(|| GeometryErrorKind::Invalid("tie witness port is missing".into()))?;
        let exact_x = &topology
            .q
            .ports
            .get(port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("tie witness port is unknown".into()))?
            .point
            .x;
        assert!(exact_x.try_cmp(&midpoint)?.is_eq());
        assert_eq!(nearest.xy(node)?.x, 100.0);
        let down =
            ExactRational::from_f64(nearest.xy(node)?.x.next_down()).try_subtract(&midpoint)?;
        let even = ExactRational::from_f64(nearest.xy(node)?.x).try_subtract(&midpoint)?;
        assert!(
            down.try_product(&down)?
                .try_cmp(&even.try_product(&even)?)?
                .is_eq()
        );

        let drawing = draw_q_plan_unmetered(&plan, &topology, &nearest, &source_points)?;
        let mut budget = EvaluationBudget::new(OverlayOp::Intersection.name(), 0)?;
        let output = match certify_q_embedding_nearest_xy(
            &topology,
            &emission,
            &nearest,
            &live_node_keys,
            drawing,
            OverlayOp::Intersection.name(),
            &mut budget,
        )? {
            NearestQCertificate::Pass(output) => output,
            NearestQCertificate::SearchRequired => {
                return Err(GeometryErrorKind::Invalid(
                    "the midpoint tie fixture should certify at nearest-even".into(),
                )
                .into());
            },
            NearestQCertificate::Unrepresentable => {
                return Err(GeometryErrorKind::Invalid(
                    "the midpoint tie fixture should be representable at nearest-even".into(),
                )
                .into());
            },
        };
        let expected = match output.sealed {
            SealedQDrawing::Source(source) => source.sealed.into_shape(),
            SealedQDrawing::Native(native) => native.sealed.into_shape(),
        };
        let actual = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
            &topology,
            OverlayOp::Intersection,
            CoordinateAxes::XY,
            false,
            None,
        )?;
        assert!(actual.validate()?.is_none(), "nearest output must be valid");
        assert_eq!(
            crate::io::to_wkb(&actual, None, false)?,
            crate::io::to_wkb(&expected, None, false)?,
            "a passing nearest-even certificate must preserve the exact output bytes"
        );
        Ok(())
    }

    #[test]
    fn q_search_repairs_two_constructed_nodes_with_one_nearest_key() -> Result<()> {
        let shapes = [
            q_grid_parallelogram((-14, 4), (24, -18))?,
            q_grid_parallelogram((23, -29), (-17, -8))?,
        ];
        let topology = test_topology(&shapes);
        let plan = build_q_plan(&topology, OverlayOp::Intersection)?;
        let source_points = source_point_coordinates(&topology)?;
        let nearest = QCoordinates::build(&topology)?;
        let publication = plan.fragment_recipes.publish(
            &topology,
            &nearest,
            OverlayOp::Intersection.name(),
            &mut crate::geometry::Unmetered,
        )?;
        let emission = build_q_emission_unmetered(&plan, &topology, &source_points, &publication)?;
        let source_nodes = b3_source_endpoint_nodes(&topology);
        let mut nearest_keys = plan.live_node_keys(&topology, &nearest, &source_points)?;
        let conflicts = node_injectivity_collisions(&mut nearest_keys)?;
        let mut groups = BTreeMap::<EmittedNodeKey, BTreeSet<QNodeId>>::new();
        for (key, node) in plan.live_node_keys(&topology, &nearest, &source_points)? {
            groups.entry(key).or_default().insert(node);
        }
        let (collision_key, collision_group) = groups
            .iter()
            .find(|(_, nodes)| {
                nodes
                    .iter()
                    .filter(|node| !source_nodes.contains(node))
                    .count()
                    == 2
            })
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "fixture has no two-constructed-node rounding collision".into(),
                )
            })?;
        let constructed_nodes = collision_group
            .iter()
            .copied()
            .filter(|node| !source_nodes.contains(node))
            .collect::<Vec<_>>();
        assert_eq!(constructed_nodes, [QNodeId(2), QNodeId(3)]);
        assert!(
            conflicts
                .iter()
                .any(|node| constructed_nodes.contains(node)),
            "nearest live-key check must witness both constructed nodes"
        );
        assert_eq!(
            nearest.xy(constructed_nodes[0])?,
            nearest.xy(constructed_nodes[1])?
        );
        assert_eq!(
            *collision_key,
            emitted_node_key(topology.sources.frame, nearest.xy(constructed_nodes[0])?)?
        );

        let certified = resolve_q_embedding_xy(
            &topology,
            &plan,
            &source_points,
            &nearest,
            OverlayOp::Intersection.name(),
        )?;
        assert_eq!(certified.choice, QEmbeddingChoice::Searched);
        let certified_coordinates = certified.coordinates.clone();
        let mut certified_keys =
            plan.live_node_keys(&topology, &certified_coordinates, &source_points)?;
        assert!(node_injectivity_collisions(&mut certified_keys)?.is_empty());
        let emitted_nodes = emission.live_nodes.iter().copied().collect::<BTreeSet<_>>();
        for node in source_nodes.intersection(&emitted_nodes) {
            assert_eq!(certified_coordinates.xy(*node)?, nearest.xy(*node)?);
        }

        let written = write_certified_q_emission(certified)?;
        let expected = written.shape;
        let actual = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
            &topology,
            OverlayOp::Intersection,
            CoordinateAxes::XY,
            false,
            None,
        )?;
        assert_eq!(
            xy_bitstream(&actual),
            xy_bitstream(&expected),
            "publication must use the accepted drawing's node coordinates"
        );
        assert!(
            actual.validate()?.is_none(),
            "searched overlay must be valid"
        );
        assert_eq!(
            crate::io::to_wkb(&actual, None, false)?,
            crate::io::to_wkb(&expected, None, false)?,
            "the published output must use the certified node assignment"
        );
        Ok(())
    }

    #[test]
    fn q_component_hosts_match_nested_shell_point_location_expectations() -> Result<()> {
        let outer = crate::io::parse_wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")?;
        let inner = crate::io::parse_wkt("POLYGON ((3 3, 7 3, 7 7, 3 7, 3 3))")?;
        let outside = Shape::Point(Point::new_raw_xy(15.0, 5.0));
        let annulus = Shape::Point(Point::new_raw_xy(2.0, 5.0));
        let outer_location = test_topology(std::slice::from_ref(&outer));
        let inner_location = test_topology(std::slice::from_ref(&inner));
        let shapes = [
            Shape::geometry_collection(vec![outer, outside])?,
            Shape::geometry_collection(vec![inner, annulus])?,
        ];
        let topology = test_topology(&shapes);
        assert_eq!(
            outer_location
                .locate_point(Point::new_raw_xy(15.0, 5.0))?
                .area,
            crate::geometry::Loc::Exterior
        );
        assert_eq!(
            outer_location
                .locate_point(Point::new_raw_xy(2.0, 5.0))?
                .area,
            crate::geometry::Loc::Interior
        );
        assert_eq!(
            inner_location
                .locate_point(Point::new_raw_xy(2.0, 5.0))?
                .area,
            crate::geometry::Loc::Exterior
        );

        let source_point_index = |x: f64, y: f64| -> Result<usize> {
            topology
                .sources
                .points
                .iter()
                .position(|source| {
                    source.point.x.to_bits() == x.to_bits()
                        && source.point.y.to_bits() == y.to_bits()
                })
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("host witness point is missing".into()).into()
                })
        };
        let face_rows_for_point = |index: usize| -> Result<Vec<SourceRowId>> {
            let face = topology
                .q
                .source_point_faces
                .get(index)
                .copied()
                .flatten()
                .ok_or_else(|| GeometryErrorKind::Invalid("host witness has no face".into()))?;
            let mut rows = Vec::new();
            for row in 0..topology.sources.row_count {
                let row = SourceRowId(u32::try_from(row).map_err(|_| ResourceError::SizeOverflow)?);
                if topology.q.face_rows.contains(face, row) {
                    rows.push(row);
                }
            }
            Ok(rows)
        };
        let outside_index = source_point_index(15.0, 5.0)?;
        let annulus_index = source_point_index(2.0, 5.0)?;
        assert!(face_rows_for_point(outside_index)?.is_empty());
        assert_eq!(
            face_rows_for_point(annulus_index)?,
            [SourceRowId(0)],
            "the point between the nested shells belongs to the outer row"
        );

        let source_polygon_node = |row: SourceRowId| -> Result<QNodeId> {
            let polygon = topology
                .sources
                .polygons
                .iter()
                .find(|polygon| polygon.row == row)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("host witness polygon is missing".into())
                })?;
            let ring = topology
                .sources
                .rings
                .get(polygon.rings.start)
                .ok_or_else(|| GeometryErrorKind::Invalid("host witness ring is missing".into()))?;
            let edge = topology
                .sources
                .edges
                .get(ring.source_edges.start)
                .ok_or_else(|| GeometryErrorKind::Invalid("host witness edge is missing".into()))?;
            let port_id = *topology
                .q
                .source_edge_ports
                .get(edge.id.0 as usize)
                .and_then(|ports| ports.first())
                .ok_or_else(|| GeometryErrorKind::Invalid("host witness port is missing".into()))?;
            topology
                .q
                .ports
                .get(port_id.0 as usize)
                .map(|port| port.node)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("host witness port is unknown".into()).into()
                })
        };
        let plan = build_q_plan(&topology, OverlayOp::Difference)?;
        let source_points = source_point_coordinates(&topology)?;
        let nearest = QCoordinates::build(&topology)?;
        let publication = plan.fragment_recipes.publish(
            &topology,
            &nearest,
            OverlayOp::Difference.name(),
            &mut crate::geometry::Unmetered,
        )?;
        let emission = build_q_emission_unmetered(&plan, &topology, &source_points, &publication)?;
        let exact_chart = subgraph_chart(
            &topology,
            &emission,
            &nearest,
            None,
            &mut crate::geometry::Unmetered,
        )?;
        let exact_faces = QFaceStructure::build(&topology, &emission, exact_chart)?;

        let component_for_node = |node: QNodeId| -> Result<usize> {
            exact_faces
                .component_nodes
                .iter()
                .position(|nodes| nodes.contains(&node))
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("host witness component is missing".into()).into()
                })
        };
        let host_for_component = |component: usize| -> Result<GlobalFaceId> {
            exact_faces
                .global
                .component_hosts
                .get(component)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("host witness component has no host".into()).into()
                })
        };
        let host_for_point = |index: usize| -> Result<GlobalFaceId> {
            topology
                .q
                .source_point_faces
                .get(index)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("host witness point has no face".into()).into()
                })
        };
        let outer_node = source_polygon_node(SourceRowId(0))?;
        let inner_node = source_polygon_node(SourceRowId(1))?;
        assert_eq!(
            host_for_component(component_for_node(outer_node)?)?,
            host_for_point(outside_index)?,
            "the outer ring component is hosted in the exterior face"
        );
        assert_eq!(
            host_for_component(component_for_node(inner_node)?)?,
            host_for_point(annulus_index)?,
            "the inner ring component is hosted in the outer polygon's annulus"
        );
        Ok(())
    }

    #[test]
    fn q_nearest_first_grid_sweep_64_keeps_certified_nearest_tables() -> Result<()> {
        let counts = q_grid_certificate_sweep(64)?;
        assert_eq!(counts.pairs, 64);
        assert!(
            counts.nearest_certified > 0,
            "the sample must include nearest-even certificate passes: {counts:?}"
        );
        assert_eq!(counts.nearest_certified + counts.search_paths, counts.pairs);
        eprintln!("Q nearest-first 64-pair sweep: {counts:?}");
        Ok(())
    }

    #[test]
    #[ignore = "2,000-pair Q nearest-first acceptance audit; run out of suite with cargo test"]
    fn q_nearest_first_grid_sweep_2000_out_of_suite() -> Result<()> {
        let counts = q_grid_certificate_sweep(2_000)?;
        assert_eq!(counts.pairs, 2_000);
        eprintln!("Q nearest-first 2,000-pair sweep: {counts:?}");
        assert_eq!(counts.nearest_certified + counts.search_paths, counts.pairs);
        Ok(())
    }

    #[test]
    fn candidate_subgraph_rebuilds_cut_ports_on_a_full_longitude_band() -> Result<()> {
        let band = crate::io::parse_wkt(
            "POLYGON ((-180 20, -90 20, 0 20, 90 20, 180 20, 180 30, 90 30, 0 30, -90 30, -180 30, -180 20))",
        )?;
        let topology = test_topology(&[band]);
        let plan = build_q_plan(&topology, OverlayOp::Union)?;
        assert!(!plan.whole_sphere);
        let source_points = source_point_coordinates(&topology)?;
        let coordinates = QCoordinates::build(&topology)?;
        let publication = plan.fragment_recipes.publish(
            &topology,
            &coordinates,
            OverlayOp::Union.name(),
            &mut crate::geometry::Unmetered,
        )?;
        let emission = build_q_emission_unmetered(&plan, &topology, &source_points, &publication)?;
        let selected_cut_ports = emission
            .spans
            .iter()
            .filter_map(|&span| topology.q.spans.get(span))
            .flat_map(|span| span.ports)
            .filter(|port| {
                topology
                    .q
                    .ports
                    .get(port.0 as usize)
                    .is_some_and(|port| port.cut)
            })
            .count();
        assert!(
            selected_cut_ports > 0,
            "the full-longitude band must exercise paired chart-cut ports"
        );

        let exact_chart = subgraph_chart(
            &topology,
            &emission,
            &coordinates,
            None,
            &mut crate::geometry::Unmetered,
        )?;
        let exact_faces = QFaceStructure::build(&topology, &emission, exact_chart)?;
        let candidate_chart = subgraph_chart(
            &topology,
            &emission,
            &coordinates,
            Some(&coordinates),
            &mut crate::geometry::Unmetered,
        )?;
        let candidate_faces = QFaceStructure::build(&topology, &emission, candidate_chart)?;
        assert!(
            face_structure_mismatch_nodes(&exact_faces, &candidate_faces, &emission.live_nodes)
                .is_none(),
            "nearest cut reconstruction must preserve the quotient faces"
        );
        Ok(())
    }

    fn b3_selected_subgraph(
        topology: &ExactGeographicTopology,
        plan: &QEmissionPlan,
    ) -> Result<Option<(Vec<usize>, Vec<QNodeId>)>> {
        if plan.whole_sphere {
            return Ok(None);
        }
        let mut work = crate::geometry::Unmetered;
        let mut spans = BTreeSet::new();
        match &plan.polygons {
            PolygonPlan::WholeSphere(_) => return Ok(None),
            PolygonPlan::Regions { rings, .. } => {
                for ring in rings {
                    match *ring {
                        PlannedAreaRing::Cycle { cycle, .. } => {
                            let cycle = plan.cycles.get(cycle).ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "B3 polygon plan references an unknown cycle".into(),
                                )
                            })?;
                            for &dart_id in &cycle.darts {
                                let dart =
                                    topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                                        GeometryErrorKind::Invalid(
                                            "B3 cycle references an unknown dart".into(),
                                        )
                                    })?;
                                spans.insert(dart.span.0 as usize);
                            }
                        },
                        PlannedAreaRing::Source { ring, .. } => {
                            let source_ring =
                                topology.sources.rings.get(ring).ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "B3 polygon plan references an unknown source ring".into(),
                                    )
                                })?;
                            for cycle in &plan.cycles {
                                if !cycle_belongs_to_source_ring(
                                    topology,
                                    cycle,
                                    source_ring.id,
                                    &mut work,
                                )? {
                                    continue;
                                }
                                for &dart_id in &cycle.darts {
                                    let dart = topology
                                        .q
                                        .darts
                                        .get(dart_id.0 as usize)
                                        .ok_or_else(|| {
                                            GeometryErrorKind::Invalid(
                                                "B3 source cycle references an unknown dart".into(),
                                            )
                                        })?;
                                    spans.insert(dart.span.0 as usize);
                                }
                            }
                        },
                    }
                }
            },
        }
        for line in &plan.line_paths {
            spans.extend(line.path.iter().map(|step| step.span));
        }
        let mut nodes = plan.point_nodes.iter().copied().collect::<BTreeSet<_>>();
        for &span_index in &spans {
            let span = topology.q.spans.get(span_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("B3 subgraph references an unknown span".into())
            })?;
            nodes.extend(span.nodes);
        }
        Ok(Some((
            spans.into_iter().collect(),
            nodes.into_iter().collect(),
        )))
    }

    fn b3_same_rotation_system(exact: &[QDartId], emitted: &[QDartId], offsets: &[usize]) -> bool {
        if exact.len() != emitted.len() || offsets.len() < 2 {
            return false;
        }
        for row in offsets.windows(2) {
            let Some((&start, &end)) = row.first().zip(row.get(1)) else {
                return false;
            };
            let Some(exact_row) = exact.get(start..end) else {
                return false;
            };
            let Some(emitted_row) = emitted.get(start..end) else {
                return false;
            };
            if exact_row.len() != emitted_row.len() {
                return false;
            }
            if exact_row.is_empty() {
                continue;
            }
            let Some(rotation) = emitted_row
                .iter()
                .position(|dart| exact_row.first() == Some(dart))
            else {
                return false;
            };
            for (index, dart) in exact_row.iter().enumerate() {
                let Some(offset) = rotation.checked_add(index) else {
                    return false;
                };
                let Some(emitted_index) = offset.checked_rem(emitted_row.len()) else {
                    return false;
                };
                if emitted_row.get(emitted_index) != Some(dart) {
                    return false;
                }
            }
        }
        true
    }

    fn b3_face_structures_match(
        exact: &super::super::q::B3SubgraphFaceStructure,
        emitted: &super::super::q::B3SubgraphFaceStructure,
    ) -> bool {
        fn bind(
            exact: GlobalFaceId,
            emitted: GlobalFaceId,
            exact_to_emitted: &mut [Option<GlobalFaceId>],
            emitted_to_exact: &mut [Option<GlobalFaceId>],
        ) -> bool {
            let Some(exact_slot) = exact_to_emitted.get_mut(exact.0 as usize) else {
                return false;
            };
            let Some(emitted_slot) = emitted_to_exact.get_mut(emitted.0 as usize) else {
                return false;
            };
            if exact_slot.is_some_and(|previous| previous != emitted)
                || emitted_slot.is_some_and(|previous| previous != exact)
            {
                return false;
            }
            *exact_slot = Some(emitted);
            *emitted_slot = Some(exact);
            true
        }

        let exact_global = &exact.global;
        let emitted_global = &emitted.global;
        if exact_global.count != emitted_global.count
            || exact_global.span_global_faces.len() != emitted_global.span_global_faces.len()
            || exact_global.component_hosts.len() != emitted_global.component_hosts.len()
            || exact_global.cut_interval_faces.len() != emitted_global.cut_interval_faces.len()
            || exact_global.source_node_faces.len() != emitted_global.source_node_faces.len()
            || exact.outgoing_offsets != emitted.outgoing_offsets
            || !b3_same_rotation_system(&exact.outgoing, &emitted.outgoing, &exact.outgoing_offsets)
        {
            return false;
        }
        let mut exact_to_emitted = vec![None; exact_global.count];
        let mut emitted_to_exact = vec![None; emitted_global.count];
        if exact_global.count == 1
            && !bind(
                GlobalFaceId(0),
                GlobalFaceId(0),
                &mut exact_to_emitted,
                &mut emitted_to_exact,
            )
        {
            return false;
        }
        for (exact_faces, emitted_faces) in exact_global
            .span_global_faces
            .iter()
            .zip(&emitted_global.span_global_faces)
        {
            match (exact_faces, emitted_faces) {
                (None, None) => {},
                (Some(exact_faces), Some(emitted_faces)) => {
                    for (exact_face, emitted_face) in exact_faces.iter().zip(emitted_faces) {
                        if !bind(
                            *exact_face,
                            *emitted_face,
                            &mut exact_to_emitted,
                            &mut emitted_to_exact,
                        ) {
                            return false;
                        }
                    }
                },
                _ => return false,
            }
        }
        for (exact_face, emitted_face) in exact_global
            .component_hosts
            .iter()
            .zip(&emitted_global.component_hosts)
        {
            match (exact_face, emitted_face) {
                (None, None) => {},
                (Some(exact_face), Some(emitted_face)) => {
                    if !bind(
                        *exact_face,
                        *emitted_face,
                        &mut exact_to_emitted,
                        &mut emitted_to_exact,
                    ) {
                        return false;
                    }
                },
                _ => return false,
            }
        }
        for (&exact_face, &emitted_face) in exact_global
            .cut_interval_faces
            .iter()
            .zip(&emitted_global.cut_interval_faces)
        {
            if !bind(
                exact_face,
                emitted_face,
                &mut exact_to_emitted,
                &mut emitted_to_exact,
            ) {
                return false;
            }
        }
        for (exact_face, emitted_face) in exact_global
            .source_node_faces
            .iter()
            .zip(&emitted_global.source_node_faces)
        {
            match (exact_face, emitted_face) {
                (None, None) => {},
                (Some(exact_face), Some(emitted_face)) => {
                    if !bind(
                        *exact_face,
                        *emitted_face,
                        &mut exact_to_emitted,
                        &mut emitted_to_exact,
                    ) {
                        return false;
                    }
                },
                _ => return false,
            }
        }
        exact_to_emitted.iter().all(Option::is_some) && emitted_to_exact.iter().all(Option::is_some)
    }

    fn b3_source_endpoint_nodes(topology: &ExactGeographicTopology) -> BTreeSet<QNodeId> {
        let mut nodes = BTreeSet::new();
        for ports in &topology.q.source_edge_ports {
            for &port_id in ports {
                if let Some(port) = topology.q.ports.get(port_id.0 as usize) {
                    nodes.insert(port.node);
                }
            }
        }
        for &port_id in &topology.q.source_point_ports {
            if let Some(port) = topology.q.ports.get(port_id.0 as usize) {
                nodes.insert(port.node);
            }
        }
        nodes
    }

    fn b3_constructed_collision_nodes(
        plan: &QEmissionPlan,
        topology: &ExactGeographicTopology,
        coordinates: &QCoordinates,
        source_points: &[Option<EmittedVertex<'_>>],
    ) -> Result<Vec<QNodeId>> {
        let live = plan.live_node_keys(topology, coordinates, source_points)?;
        let source_nodes = b3_source_endpoint_nodes(topology);
        let mut groups = BTreeMap::<EmittedNodeKey, BTreeSet<QNodeId>>::new();
        for (key, node) in live {
            groups.entry(key).or_default().insert(node);
        }
        Ok(groups
            .into_values()
            .filter(|nodes| nodes.len() > 1)
            .flat_map(|nodes| {
                nodes
                    .into_iter()
                    .filter(|node| !source_nodes.contains(node))
            })
            .collect())
    }

    fn b3_compare_assignment(
        topology: &ExactGeographicTopology,
        plan: &QEmissionPlan,
        source_points: &[Option<EmittedVertex<'_>>],
        coordinates: &QCoordinates,
        spans: &[usize],
        point_nodes: &[QNodeId],
        exact: &super::super::q::B3SubgraphFaceStructure,
    ) -> Result<bool> {
        let mut live = plan.live_node_keys(topology, coordinates, source_points)?;
        certify_node_injectivity(&mut live)?;
        let emitted = super::super::q::b3_subgraph_face_structure(
            topology.sources.frame,
            &topology.q,
            spans,
            point_nodes,
            Some((&coordinates.node, &coordinates.pole_port_longitude)),
        )?;
        Ok(b3_face_structures_match(exact, &emitted))
    }

    #[test]
    fn q_ring_role_rejects_the_globe_complement_of_a_full_turn_cap() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let exact_ports = [(0.0, 10.0), (120.0, 10.0), (240.0, 10.0), (0.0, 10.0)].map(|(x, y)| {
            super::super::exact::ExactChartPoint::from_rationals(
                ExactRational::from_f64(x),
                ExactRational::from_f64(y),
            )
        });
        let complement = Ring::try_from_closed(CoordSeq::try_from_points(&[
            Point::new_raw_xy(0.0, 10.0),
            Point::new_raw_xy(240.0, 10.0),
            Point::new_raw_xy(120.0, 10.0),
            Point::new_raw_xy(0.0, 10.0),
        ])?)?;

        let role = compare_q_ring_role(frame, &complement, &complement, &exact_ports, true)?
            .expect("both polar chart roles are classifiable");
        assert!(!role.exact_is_hole, "the exact orientation selects the cap");
        assert!(
            role.emitted_is_hole,
            "reversal selects its globe complement"
        );
        assert!(!role.matches(), "QC-R rejects the complementary role");
        Ok(())
    }

    #[test]
    fn q_whole_sphere_union_keeps_source_rings_and_never_searches() -> Result<()> {
        let source = Shape::Polygon(Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(&[
                Point::new_raw_xy(-180.0, -90.0),
                Point::new_raw_xy(-90.0, -90.0),
                Point::new_raw_xy(0.0, -90.0),
                Point::new_raw_xy(90.0, -90.0),
                Point::new_raw_xy(180.0, -90.0),
                Point::new_raw_xy(180.0, 90.0),
                Point::new_raw_xy(90.0, 90.0),
                Point::new_raw_xy(0.0, 90.0),
                Point::new_raw_xy(-90.0, 90.0),
                Point::new_raw_xy(-180.0, 90.0),
                Point::new_raw_xy(-180.0, -90.0),
            ])?)?,
            Vec::new(),
        ));
        let topology = test_topology(std::slice::from_ref(&source));
        let operation = OverlayOp::Union;
        let plan = build_q_plan(&topology, operation)?;
        assert!(plan.whole_sphere);
        let source_points = source_point_coordinates(&topology)?;
        let nearest = QCoordinates::build(&topology)?;
        let embedding =
            resolve_q_embedding_xy(&topology, &plan, &source_points, &nearest, operation.name())?;
        assert_eq!(embedding.choice, QEmbeddingChoice::Nearest);
        let result = publish_q_overlay(embedding)?;
        assert_eq!(
            crate::io::to_wkb(&result, None, false)?,
            crate::io::to_wkb(&source, None, false)?,
            "whole-sphere publication keeps the stored source ring"
        );
        Ok(())
    }

    #[test]
    fn q_source_axis_band_emission_checks_the_exact_ring_role() {
        let source = Shape::Polygon(Polygon::new(
            Ring::try_from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(-180.0, 10.0),
                    Point::new_raw_xy(-90.0, 10.0),
                    Point::new_raw_xy(0.0, 10.0),
                    Point::new_raw_xy(90.0, 10.0),
                    Point::new_raw_xy(180.0, 10.0),
                    Point::new_raw_xy(180.0, 20.0),
                    Point::new_raw_xy(90.0, 20.0),
                    Point::new_raw_xy(0.0, 20.0),
                    Point::new_raw_xy(-90.0, 20.0),
                    Point::new_raw_xy(-180.0, 20.0),
                    Point::new_raw_xy(-180.0, 10.0),
                ])
                .expect("build the full-longitude source band"),
            )
            .expect("close the full-longitude source band"),
            Vec::new(),
        ));
        let topology = test_topology(std::slice::from_ref(&source));
        let operation = OverlayOp::Union;
        let plan = build_q_plan(&topology, operation).expect("build Q plan");
        assert!(!plan.whole_sphere);
        let PolygonPlan::Regions { rings, .. } = &plan.polygons else {
            panic!("the axis band has a bounded selected region");
        };
        assert!(
            rings
                .iter()
                .any(|ring| matches!(ring, PlannedAreaRing::Source { .. })),
            "the plan keeps the source axis-band ring"
        );

        let source_points = source_point_coordinates(&topology).expect("source points");
        let nearest = QCoordinates::build(&topology).expect("nearest Q coordinates");
        let embedding =
            resolve_q_embedding_xy(&topology, &plan, &source_points, &nearest, operation.name())
                .expect("source axis-band ring is representable");
        assert_eq!(embedding.choice, QEmbeddingChoice::Nearest);
        let result = publish_q_overlay(embedding).expect("publish the source axis-band ring");
        assert_eq!(
            crate::io::to_wkb(&result, None, false).expect("serialize Q result"),
            crate::io::to_wkb(&source, None, false).expect("serialize source band")
        );
    }

    #[test]
    fn b3_tv_ar_uy_symdiff_wkb_is_byte_identical_across_all_orders() -> Result<()> {
        let shapes = b3_read_countries(&["TV", "AR", "UY"]);
        let orders = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [
            2, 1, 0,
        ]];
        let mut expected = None;
        for order in orders {
            let operands = order.map(|index| shapes[index].clone());
            let topology = test_topology(&operands);
            let result = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
                &topology,
                OverlayOp::SymmetricDifference,
                CoordinateAxes::XY,
                false,
                None,
            )?;
            let bytes = crate::io::to_wkb(&result, None, false)?;
            if let Some(first) = &expected {
                assert_eq!(
                    &bytes, first,
                    "TV/AR/UY symmetric difference WKB differs in order {order:?}"
                );
            } else {
                expected = Some(bytes);
            }
        }
        Ok(())
    }

    #[test]
    fn b3_tv_ar_uy_symdiff_is_valid_and_byte_identical_in_all_orders() -> Result<()> {
        let shapes = b3_read_countries(&["TV", "AR", "UY"]);
        assert_eq!(shapes.len(), 3);
        let orders = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [
            2, 1, 0,
        ]];
        let mut expected = None;
        for order in orders {
            let operands = order.map(|index| shapes[index].clone());
            let topology = test_topology(&operands);
            let plan = build_q_plan(&topology, OverlayOp::SymmetricDifference)?;
            let source_points = source_point_coordinates(&topology)?;
            let nearest = QCoordinates::build(&topology)?;
            let nearest_nodes = nearest.node.clone();
            let mut live_node_keys = plan.live_node_keys(&topology, &nearest, &source_points)?;
            let identity_conflicts = node_injectivity_collisions(&mut live_node_keys)?;
            let embedding = resolve_q_embedding_xy(
                &topology,
                &plan,
                &source_points,
                &nearest,
                OverlayOp::SymmetricDifference.name(),
            )?;
            let certificate_path = embedding.choice;
            let moved_nodes = nearest_nodes
                .iter()
                .zip(&embedding.coordinates.node)
                .enumerate()
                .filter_map(|(index, (before, after))| (before != after).then_some(index))
                .collect::<Vec<_>>();
            let result = publish_q_overlay(embedding)?;
            let issue = crate::geometry::validate_shape_in_frame(
                &result,
                crate::geometry::TopologyDomain::Geographic(GeographicFrame::crs84()),
            )?;
            if let Some(issue) = issue {
                return Err(GeometryErrorKind::Invalid(
                    format!(
                        "TV/AR/UY symmetric difference is invalid in operand order {order:?}: {issue:?}; certificate={certificate_path:?}, identity_conflicts={identity_conflicts:?}, moved_nodes={moved_nodes:?}"
                    )
                    .into(),
                )
                .into());
            }
            let bytes = crate::io::to_wkb(&result, None, false)?;
            if let Some(first) = &expected {
                assert_eq!(
                    &bytes, first,
                    "TV/AR/UY symmetric difference must be byte-identical in every order"
                );
            } else {
                expected = Some(bytes);
            }
        }
        Ok(())
    }

    #[test]
    fn geographic_nearest_certificate_preserves_parent_au_pg_union_wkb() -> Result<()> {
        use sha2::{Digest as _, Sha256};

        let shapes = b3_read_countries(&["AU", "PG"]);
        let topology = test_topology(&shapes);
        let plan = build_q_plan(&topology, OverlayOp::Union)?;
        let source_points = source_point_coordinates(&topology)?;
        let nearest = QCoordinates::build(&topology)?;
        let embedding = resolve_q_embedding_xy(
            &topology,
            &plan,
            &source_points,
            &nearest,
            OverlayOp::Union.name(),
        )?;
        assert_eq!(embedding.choice, QEmbeddingChoice::Nearest);
        let result = publish_q_overlay(embedding)?;
        assert!(
            crate::geometry::validate_shape_in_frame(
                &result,
                crate::geometry::TopologyDomain::Geographic(GeographicFrame::crs84()),
            )?
            .is_none(),
            "the baseline union remains valid"
        );
        assert_eq!(
            format!(
                "{:x}",
                Sha256::digest(crate::io::to_wkb(&result, None, false)?)
            ),
            "3de292238e1cfcfa472ddd95548bb3ea28321182c2124f801d1fb63186fad206",
            "nearest-even Q output must retain the exact-parent WKB bytes"
        );
        Ok(())
    }

    #[test]
    fn b3_rebuild_and_compare_certifies_the_ar_uy_tv_neighbor_assignment() {
        let shapes = b3_read_countries(&["TV", "AR", "UY"]);
        assert_eq!(shapes.len(), 3);
        let topology = test_topology(&shapes);
        let plan = build_q_plan(&topology, OverlayOp::SymmetricDifference).expect("Q plan");
        assert!(!plan.whole_sphere);
        let source_points = source_point_coordinates(&topology).expect("source points");
        let coordinates = QCoordinates::build(&topology).expect("nearest Q coordinates");
        let (spans, point_nodes) = b3_selected_subgraph(&topology, &plan)
            .expect("selected H")
            .expect("area H");
        let selected_cut_nodes = spans
            .iter()
            .flat_map(|&span_index| {
                topology
                    .q
                    .spans
                    .get(span_index)
                    .into_iter()
                    .flat_map(|span| span.ports)
            })
            .filter_map(|port_id| topology.q.ports.get(port_id.0 as usize))
            .filter(|port| port.cut)
            .map(|port| port.node)
            .collect::<BTreeSet<_>>();
        assert!(
            selected_cut_nodes.is_empty(),
            "the AR/UY/TV witness must not rely on unchanged cut ports"
        );

        let exact = super::super::q::b3_subgraph_face_structure(
            topology.sources.frame,
            &topology.q,
            &spans,
            &point_nodes,
            None,
        )
        .expect("exact H rebuild");
        let exact_again = super::super::q::b3_subgraph_face_structure(
            topology.sources.frame,
            &topology.q,
            &spans,
            &point_nodes,
            None,
        )
        .expect("second exact H rebuild");
        assert!(b3_face_structures_match(&exact, &exact_again));

        let nearest_error = b3_compare_assignment(
            &topology,
            &plan,
            &source_points,
            &coordinates,
            &spans,
            &point_nodes,
            &exact,
        )
        .expect_err("nearest collision bypass must be rejected");
        assert!(
            nearest_error.to_string().contains("distinct nodes"),
            "nearest bypass was rejected for an unrelated reason: {nearest_error}"
        );

        let constructed =
            b3_constructed_collision_nodes(&plan, &topology, &coordinates, &source_points)
                .expect("nearest collision groups");
        assert_eq!(
            constructed.len(),
            2,
            "the study has two constructed collisions"
        );
        let mut witness = QCoordinates {
            node: coordinates.node.clone(),
            longitude_periods: coordinates.longitude_periods.clone(),
            pole_port_longitude: coordinates.pole_port_longitude.clone(),
            pole_port_periods: coordinates.pole_port_periods.clone(),
        };
        for node in &constructed {
            let xy = witness
                .node
                .get_mut(node.0 as usize)
                .expect("constructed Q node coordinate");
            xy.x = xy.x.next_down();
        }
        for node in &constructed {
            let nearest = coordinates
                .node
                .get(node.0 as usize)
                .expect("nearest constructed Q node coordinate");
            let assigned = witness
                .node
                .get(node.0 as usize)
                .expect("assigned constructed Q node coordinate");
            assert_eq!(assigned.x.to_bits(), nearest.x.next_down().to_bits());
            assert_eq!(assigned.y.to_bits(), nearest.y.to_bits());
        }
        assert!(
            b3_compare_assignment(
                &topology,
                &plan,
                &source_points,
                &witness,
                &spans,
                &point_nodes,
                &exact,
            )
            .expect("the study's neighbor assignment is injective"),
            "the exact subgraph faces must match the study's certified assignment"
        );

        let mut swap_pairs = BTreeSet::new();
        for &span_index in &spans {
            let span = topology
                .q
                .spans
                .get(span_index)
                .expect("selected span for swap control");
            let [start_node, end_node] = span.nodes;
            if start_node == end_node
                || span.ports.iter().any(|port_id| {
                    topology
                        .q
                        .ports
                        .get(port_id.0 as usize)
                        .is_some_and(|port| port.cut)
                })
            {
                continue;
            }
            let pair = if start_node < end_node {
                (start_node, end_node)
            } else {
                (end_node, start_node)
            };
            swap_pairs.insert(pair);
        }
        let mut swap_face_mismatch = None;
        let mut swap_rebuild_error = None;
        for (left, right) in swap_pairs {
            let left_xy = witness
                .node
                .get(left.0 as usize)
                .copied()
                .expect("left swap coordinate");
            let right_xy = witness
                .node
                .get(right.0 as usize)
                .copied()
                .expect("right swap coordinate");
            if (left_xy.x - right_xy.x).abs() >= 1.0
                || (left_xy.y - right_xy.y).abs() >= 1.0
                || topology
                    .q
                    .nodes
                    .get(left.0 as usize)
                    .and_then(|node| topology.q.node_ports.get(node.ports.clone()))
                    .is_some_and(|ports| {
                        ports.iter().any(|port_id| {
                            witness
                                .pole_port_longitude
                                .get(port_id.0 as usize)
                                .is_some_and(Option::is_some)
                        })
                    })
                || topology
                    .q
                    .nodes
                    .get(right.0 as usize)
                    .and_then(|node| topology.q.node_ports.get(node.ports.clone()))
                    .is_some_and(|ports| {
                        ports.iter().any(|port_id| {
                            witness
                                .pole_port_longitude
                                .get(port_id.0 as usize)
                                .is_some_and(Option::is_some)
                        })
                    })
            {
                continue;
            }
            let mut swapped = QCoordinates {
                node: witness.node.clone(),
                longitude_periods: witness.longitude_periods.clone(),
                pole_port_longitude: witness.pole_port_longitude.clone(),
                pole_port_periods: witness.pole_port_periods.clone(),
            };
            swapped.node.swap(left.0 as usize, right.0 as usize);
            let Ok(mut live) = plan.live_node_keys(&topology, &swapped, &source_points) else {
                continue;
            };
            if certify_node_injectivity(&mut live).is_err() {
                continue;
            }
            match super::super::q::b3_subgraph_face_structure(
                topology.sources.frame,
                &topology.q,
                &spans,
                &point_nodes,
                Some((&swapped.node, &swapped.pole_port_longitude)),
            ) {
                Ok(emitted) if !b3_face_structures_match(&exact, &emitted) => {
                    let stage = if b3_same_rotation_system(
                        &exact.outgoing,
                        &emitted.outgoing,
                        &exact.outgoing_offsets,
                    ) {
                        "global_face_structure"
                    } else {
                        "rotation_system"
                    };
                    swap_face_mismatch = Some((left, right, stage));
                    break;
                },
                Ok(_) => {},
                Err(error) => {
                    swap_rebuild_error.get_or_insert_with(|| (left, right, error.to_string()));
                },
            }
        }
        let swap_rejected = swap_face_mismatch.is_some() || swap_rebuild_error.is_some();
        assert!(
            swap_rejected,
            "no collision-free swap across a selected chord reached and failed the emitted rebuild or face comparison"
        );
    }

    #[test]
    fn emitted_longitude_reduces_chart_period_before_rounding() {
        let frame = GeographicFrame::crs84();
        let source = 122.427_194_f64;
        let period = ExactRational::from_f64(frame.longitude().period());
        let shifted = ExactRational::from_f64(source).try_add(&period).unwrap();
        let rounded_chart_alias = source + frame.longitude().period();

        assert_eq!(
            frame
                .longitude()
                .canonicalize(rounded_chart_alias)
                .unwrap()
                .get()
                .to_bits(),
            source.next_down().to_bits()
        );
        assert_eq!(
            round_canonical_longitude(frame, &shifted)
                .unwrap()
                .to_bits(),
            source.to_bits()
        );
    }

    #[test]
    fn emitted_nodes_reject_distinct_ids_for_one_rounded_key() {
        let frame = GeographicFrame::crs84();
        let third = ExactRational::from_f64(1.0)
            .try_quotient(&ExactRational::from_f64(3.0))
            .unwrap();
        let original = ExactChartPoint::from_rationals(
            ExactRational::from_f64(1.0 / 3.0),
            ExactRational::zero(),
        );
        let inserted = ExactChartPoint::from_rationals(third, ExactRational::zero());
        let original_key = emitted_node_key(
            frame,
            XY::new(round_canonical_longitude(frame, &original.x).unwrap(), 0.0),
        )
        .unwrap();
        let inserted_key = emitted_node_key(
            frame,
            XY::new(round_canonical_longitude(frame, &inserted.x).unwrap(), 0.0),
        )
        .unwrap();
        assert_eq!(original_key, inserted_key);
        let mut distinct_nodes = vec![(original_key, QNodeId(7)), (inserted_key, QNodeId(8))];
        let error = certify_node_injectivity(&mut distinct_nodes).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactNodeCollision {
                first: 7,
                second: 8,
            })
        ));

        let shifted = ExactChartPoint::from_rationals(
            original
                .x
                .try_add(&ExactRational::from_f64(frame.longitude().period()))
                .unwrap(),
            original.y,
        );
        let shifted_key = emitted_node_key(
            frame,
            XY::new(round_canonical_longitude(frame, &shifted.x).unwrap(), 0.0),
        )
        .unwrap();
        let mut same_node_alias = vec![(original_key, QNodeId(7)), (shifted_key, QNodeId(7))];
        certify_node_injectivity(&mut same_node_alias).unwrap();
    }

    #[test]
    fn boundary_keeps_the_valid_seam_component_in_a_multirow_union() {
        let shape = crate::io::parse_wkt(
            "MULTIPOLYGON (\
             ((20 20,40 20,40 30,20 30,20 20)),\
             ((0 10,20 10,20 20,0 20,0 10)),\
             ((170 20,190 20,190 40,170 40,170 20))\
             )",
        )
        .expect("three-box multipolygon parses");
        let mut expected = vec![
            edge((0.0, 10.0), (20.0, 10.0)),
            edge((20.0, 10.0), (20.0, 20.0)),
            edge((20.0, 20.0), (0.0, 20.0)),
            edge((0.0, 20.0), (0.0, 10.0)),
            edge((20.0, 20.0), (40.0, 20.0)),
            edge((40.0, 20.0), (40.0, 30.0)),
            edge((40.0, 30.0), (20.0, 30.0)),
            edge((20.0, 30.0), (20.0, 20.0)),
            edge((170.0, 40.0), (170.0, 20.0)),
            edge((170.0, 20.0), (-180.0, 20.0)),
            edge((-180.0, 20.0), (-170.0, 20.0)),
            edge((-170.0, 20.0), (-170.0, 40.0)),
            edge((-170.0, 40.0), (-180.0, 40.0)),
            edge((-180.0, 40.0), (170.0, 40.0)),
        ];
        expected.sort_unstable();
        let actual = line_edges(&exact_boundary(&shape));
        assert_eq!(actual, expected);
        assert!(!actual.contains(&edge((-180.0, 20.0), (-180.0, 40.0))));
    }

    #[test]
    fn boundary_keeps_a_pentagon_when_its_host_ray_hits_a_seam_box_node() {
        let shape = crate::io::parse_wkt(
            "MULTIPOLYGON (\
             ((0 10,20 10,20 20,40 20,0 30,0 10)),\
             ((170 20,190 20,190 40,170 40,170 20))\
             )",
        )
        .expect("pentagon and seam box parse");
        let mut expected = vec![
            edge((0.0, 30.0), (0.0, 10.0)),
            edge((0.0, 10.0), (20.0, 10.0)),
            edge((20.0, 10.0), (20.0, 20.0)),
            edge((20.0, 20.0), (40.0, 20.0)),
            edge((40.0, 20.0), (0.0, 30.0)),
            edge((170.0, 40.0), (170.0, 20.0)),
            edge((170.0, 20.0), (-180.0, 20.0)),
            edge((-180.0, 20.0), (-170.0, 20.0)),
            edge((-170.0, 20.0), (-170.0, 40.0)),
            edge((-170.0, 40.0), (-180.0, 40.0)),
            edge((-180.0, 40.0), (170.0, 40.0)),
        ];
        expected.sort_unstable();
        let actual = line_edges(&exact_boundary(&shape));
        assert_eq!(actual, expected);
        assert!(!actual.contains(&edge((-180.0, 20.0), (-180.0, 40.0))));
    }

    #[test]
    fn boundary_certificate_rejects_a_reversed_constructed_hole_triangle() -> Result<()> {
        let unit = f64::from_bits(0x3D10_0000_0000_0000);
        let bar = |vertices: &[(i32, i32)]| -> Result<Polygon> {
            let coordinates = vertices
                .iter()
                .map(|&(x, y)| {
                    format!(
                        "{} {}",
                        100.0 + f64::from(x) * unit,
                        80.0 + f64::from(y) * unit
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            let shape = crate::io::parse_wkt(&format!("POLYGON (({coordinates}))"))?;
            let Shape::Polygon(polygon) = shape else {
                return Err(GeometryErrorKind::Invalid(
                    "a triangle bar must parse as a polygon".into(),
                )
                .into());
            };
            Ok(polygon)
        };
        let bars = vec![
            bar(&[(-30, -20), (30, -20), (30, 0), (-30, 0), (-30, -20)])?,
            bar(&[(-30, -10), (30, 11), (30, 31), (-30, 10), (-30, -10)])?,
            bar(&[(-30, 11), (30, -10), (30, 10), (-30, 31), (-30, 11)])?,
        ];
        let members = bars
            .into_iter()
            .map(crate::geometry::MultiMember::Value)
            .collect::<Vec<_>>();
        let source = Shape::MultiPolygon(crate::geometry::MultiPolygon::try_new(
            CoordinateAxes::XY,
            members,
        )?);
        let topology = test_topology(std::slice::from_ref(&source));
        let (plan, support) = build_q_boundary_plan(&topology)?;
        assert!(support.has_area);
        let source_points = boundary_source_points(&topology, SourceRowId(0))?;
        let nearest = QCoordinates::build(&topology)?;
        let publication = plan.fragment_recipes.publish(
            &topology,
            &nearest,
            "geographic boundary",
            &mut crate::geometry::Unmetered,
        )?;
        let mut witness_found = false;
        let mut work = crate::geometry::Unmetered;
        for recipe in plan.fragment_recipes.as_slice() {
            let references = recipe.reference_endpoints(&topology, &mut work)?;
            let start_x = references[0].x.round_with_work(&mut work)?;
            let end_x = references[1].x.round_with_work(&mut work)?;
            let anchored_end = ExactRational::from_f64(start_x)
                .try_add_with_work(&recipe.exact_displacement, &mut work)?
                .round_with_work(&mut work)?;
            if start_x.to_bits() != 100.00000000000001_f64.to_bits()
                || anchored_end.to_bits() != 99.99999999999997_f64.to_bits()
            {
                continue;
            }
            let assigned_end = nearest.point(&topology, recipe.endpoints[1])?;
            assert_eq!(assigned_end.x.to_bits(), 99.99999999999999_f64.to_bits());
            assert_eq!(end_x.to_bits(), assigned_end.x.to_bits());
            let fragment = publication
                .fragments
                .get(recipe.id.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("near-ULP publication fragment is missing".into())
                })?;
            assert_eq!(fragment.exact_endpoints, references);
            assert!(fragment.source_records[1].is_none());
            let published_end = fragment.points.last().ok_or_else(|| {
                GeometryErrorKind::Invalid("near-ULP publication has no end point".into())
            })?;
            assert_eq!(published_end.x.to_bits(), end_x.to_bits());
            assert_eq!(
                published_end.y.to_bits(),
                references[1].y.round_with_work(&mut work)?.to_bits()
            );
            witness_found = true;
        }
        assert!(
            witness_found,
            "the captured near-ULP Q-reference interval must be published from its exact endpoints"
        );
        let emission = build_q_emission_unmetered(&plan, &topology, &source_points, &publication)?;

        let exact_apex_x = ExactRational::from_f64(100.0);
        let half_unit = ExactRational::from_f64(unit / 2.0);
        let exact_apex_y = ExactRational::from_f64(80.0).try_add(&half_unit)?;
        let mut apex = None;
        for &node_id in &emission.live_nodes {
            let node = topology.q.nodes.get(node_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("triangle-hole node is unknown".into())
            })?;
            let ports = topology
                .q
                .node_ports
                .get(node.ports.clone())
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("triangle-hole node ports are unknown".into())
                })?;
            for &port_id in ports {
                let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("triangle-hole port is unknown".into())
                })?;
                if !port.cut
                    && port.point.x.try_cmp(&exact_apex_x)?.is_eq()
                    && port.point.y.try_cmp(&exact_apex_y)?.is_eq()
                {
                    apex = Some(node_id);
                    break;
                }
            }
        }
        let apex = apex.ok_or_else(|| {
            GeometryErrorKind::Invalid("constructed triangular-hole apex is missing".into())
        })?;
        assert_eq!(nearest.xy(apex)?.y.to_bits(), 80.0_f64.to_bits());

        let mut reversed = nearest.clone();
        let nearest_apex = reversed.xy(apex)?;
        *reversed.node.get_mut(apex.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("triangle-hole coordinate is missing".into())
        })? = crate::geometry::XY::new(nearest_apex.x, 80.0_f64.next_down());
        let mut live = plan.live_node_keys(&topology, &reversed, &source_points)?;
        assert!(
            node_injectivity_collisions(&mut live)?.is_empty(),
            "reversing the degree-two hole triangle remains node-injective"
        );

        let exact_chart = subgraph_chart(
            &topology,
            &emission,
            &nearest,
            None,
            &mut crate::geometry::Unmetered,
        )?;
        let exact_faces = QFaceStructure::build(&topology, &emission, exact_chart)?;
        let reversed_chart = subgraph_chart(
            &topology,
            &emission,
            &nearest,
            Some(&reversed),
            &mut crate::geometry::Unmetered,
        )?;
        let reversed_faces = QFaceStructure::build(&topology, &emission, reversed_chart)?;
        let witnesses =
            face_structure_mismatch_nodes(&exact_faces, &reversed_faces, &emission.live_nodes)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "reversed hole triangle preserved the exact face/host structure".into(),
                    )
                })?;
        assert!(
            witnesses.contains(&apex),
            "the face/host witness should name the moved apex: {witnesses:?}"
        );

        let drawing = draw_q_plan_unmetered(&plan, &topology, &reversed, &source_points)?;
        let mut budget = EvaluationBudget::new("geographic boundary", 0)?;
        let certificate = certify_q_embedding_nearest_xy(
            &topology,
            &emission,
            &reversed,
            &live,
            drawing,
            "geographic boundary",
            &mut budget,
        )?;
        let NearestQCertificate::SearchRequired = certificate else {
            return Err(GeometryErrorKind::Invalid(
                "the full boundary certificate accepted the reversed hole triangle".into(),
            )
            .into());
        };

        let accepted = resolve_q_embedding_xy(
            &topology,
            &plan,
            &source_points,
            &nearest,
            "geographic boundary",
        )?;
        let accepted_chart = subgraph_chart(
            &topology,
            &emission,
            &nearest,
            Some(&accepted.coordinates),
            &mut crate::geometry::Unmetered,
        )?;
        let accepted_faces = QFaceStructure::build(&topology, &emission, accepted_chart)?;
        assert!(
            face_structure_mismatch_nodes(&exact_faces, &accepted_faces, &emission.live_nodes)
                .is_none(),
            "the accepted table preserves the triangular-hole face"
        );
        Ok(())
    }

    #[test]
    fn geographic_boundary_repairs_a_nearest_embedding_before_writing() -> Result<()> {
        let parts = [
            q_grid_parallelogram((-14, 4), (24, -18))?,
            q_grid_parallelogram((23, -29), (-17, -8))?,
        ];
        let source = Shape::geometry_collection(parts.into_iter().collect())?;
        let topology = test_topology(std::slice::from_ref(&source));
        let (plan, support) = build_q_boundary_plan(&topology)?;
        assert!(support.has_area);

        let source_points = boundary_source_points(&topology, SourceRowId(0))?;
        let nearest = QCoordinates::build(&topology)?;
        let publication = plan.fragment_recipes.publish(
            &topology,
            &nearest,
            "geographic boundary",
            &mut crate::geometry::Unmetered,
        )?;
        let emission = build_q_emission_unmetered(&plan, &topology, &source_points, &publication)?;
        let exact_chart = subgraph_chart(
            &topology,
            &emission,
            &nearest,
            None,
            &mut crate::geometry::Unmetered,
        )?;
        let exact_faces = QFaceStructure::build(&topology, &emission, exact_chart)?;
        assert!(
            subgraph_chart(
                &topology,
                &emission,
                &nearest,
                Some(&nearest),
                &mut crate::geometry::Unmetered,
            )
            .and_then(|chart| QFaceStructure::build(&topology, &emission, chart))
            .err()
            .is_some_and(|error| error.to_string().contains("coincident tangent rays")),
            "nearest boundary rounding must fail the exact area face/host control"
        );
        let embedding = resolve_q_embedding_xy(
            &topology,
            &plan,
            &source_points,
            &nearest,
            "geographic boundary",
        )?;
        assert_eq!(
            embedding.choice,
            QEmbeddingChoice::Searched,
            "the nearest boundary drawing must be repaired before publication"
        );
        let accepted_chart = subgraph_chart(
            &topology,
            &emission,
            &nearest,
            Some(&embedding.coordinates),
            &mut crate::geometry::Unmetered,
        )?;
        let accepted_faces = QFaceStructure::build(&topology, &emission, accepted_chart)?;
        assert!(
            face_structure_mismatch_nodes(&exact_faces, &accepted_faces, &emission.live_nodes)
                .is_none(),
            "the searched boundary drawing must restore the exact area face/host structure"
        );
        let written = write_certified_q_emission(embedding)?;
        let mut line_members = 0_usize;
        let mut polygon_members = 0_usize;
        written
            .shape
            .try_for_each_component(&mut |component| -> Result<()> {
                match component {
                    crate::geometry::Component::LineString(_) => {
                        line_members = line_members.saturating_add(1);
                    },
                    crate::geometry::Component::Polygon(_) => {
                        polygon_members = polygon_members.saturating_add(1);
                    },
                    crate::geometry::Component::Point(_) => {},
                }
                Ok(())
            })?;
        assert!(
            line_members > 0,
            "the repaired areal boundary publishes linework"
        );
        assert_eq!(polygon_members, 0);
        Ok(())
    }

    #[test]
    fn source_boundary_binds_the_pinned_y_sign_after_shape_assembly() -> Result<()> {
        let source = Shape::Polygon(Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(&[
                Point::new_raw_xy(-180.0, -0.0),
                Point::new_raw_xy(180.0, -0.0),
                Point::new_raw_xy(180.0, 10.0),
                Point::new_raw_xy(-180.0, 10.0),
                Point::new_raw_xy(-180.0, -0.0),
            ])?)?,
            Vec::new(),
        ));
        let topology = test_topology(std::slice::from_ref(&source));
        let (plan, _) = build_q_boundary_plan(&topology)?;

        let (unchanged, unchanged_layout) = source_boundary_sink(&topology, &plan)?;
        let Shape::MultiPoint(unchanged_points) = &unchanged else {
            panic!("the seam-only boundary is its collapsed source points");
        };
        assert!(
            unchanged_points
                .iter_coords()
                .any(|point| point.y.to_bits() == (-0.0_f64).to_bits()),
            "the collapsed source writer retains the original negative-zero Y bits"
        );
        assert!(matches!(
            bind_source_shape(&topology, unchanged, unchanged_layout)?,
            LiteralCheck::Pass(_)
        ));

        let (shape, layout) = source_boundary_sink(&topology, &plan)?;
        let Shape::MultiPoint(points) = shape else {
            panic!("the source boundary writer returns a multipoint sink");
        };
        let mut coordinates = points.iter_coords().collect::<Vec<_>>();
        let ordinal = coordinates
            .iter()
            .position(|point| point.y.to_bits() == (-0.0_f64).to_bits())
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("pinned source Y record is missing".into())
            })?;
        let image = layout
            .points
            .get(ordinal)
            .map(|location| location.image)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("pinned source Y slot has no literal receipt".into())
            })?;
        assert!(
            matches!(
                image,
                QSourceImage::CollapsedStart(_) | QSourceImage::CollapsedEnd(_)
            ),
            "the pinned record must name its source collapsed-edge endpoint"
        );
        coordinates
            .get_mut(ordinal)
            .ok_or_else(|| GeometryErrorKind::Invalid("source point slot is unknown".into()))?
            .y = 0.0;
        let mutated = Shape::MultiPoint(crate::geometry::MultiPoint::from_coordseq(
            CoordSeq::try_from_points(&coordinates)?,
        )?);
        let LiteralCheck::Reject(rejected) = bind_source_shape(&topology, mutated, layout)? else {
            panic!("a final-buffer ±0 mutation must fail the retained source-record check");
        };
        assert!(rejected.contains(&image));
        Ok(())
    }

    #[test]
    fn k0_source_point_y_bits_are_rechecked_in_the_final_shape() -> Result<()> {
        let shapes = [
            Shape::Point(Point::new_raw_xy(5.0, 0.0)),
            Shape::Point(Point::new_raw_xy(10.0, 2.0)),
        ];
        let topology = test_topology(&shapes);
        let plan = build_q_plan(&topology, OverlayOp::Union)?;
        let source_points = source_point_coordinates(&topology)?;
        let nearest = QCoordinates::build(&topology)?;
        let drawing = draw_q_plan_unmetered(&plan, &topology, &nearest, &source_points)?;
        assert!(
            plan.source_reuse_is_proven(
                &topology,
                &drawing,
                &source_points,
                &mut crate::geometry::Unmetered,
            )?,
            "unchanged explicit source points take the proven K0 writer path"
        );

        let actual = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
            &topology,
            OverlayOp::Union,
            CoordinateAxes::XY,
            false,
            None,
        )?;
        let Shape::MultiPoint(actual_points) = actual else {
            panic!("the two disjoint source points publish as a MultiPoint");
        };
        assert!(
            actual_points.iter_coords().any(|point| {
                point.x.to_bits() == 5.0_f64.to_bits() && point.y.to_bits() == 0.0_f64.to_bits()
            }),
            "K0 retains the source point's positive-zero Y record"
        );

        let (shape, layout) = source_sink(&topology, &plan, &source_points)?;
        let Shape::MultiPoint(points) = shape else {
            panic!("the K0 writer returns a MultiPoint source sink");
        };
        let mut coordinates = points.iter_coords().collect::<Vec<_>>();
        let ordinal = coordinates
            .iter()
            .position(|point| {
                point.x.to_bits() == 5.0_f64.to_bits() && point.y.to_bits() == 0.0_f64.to_bits()
            })
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("pinned K0 source point is missing".into())
            })?;
        let image = layout
            .points
            .get(ordinal)
            .map(|location| location.image)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("K0 source point has no literal receipt".into())
            })?;
        assert!(
            matches!(image, QSourceImage::SourcePoint(_)),
            "the literal slot retains its original source-point identity"
        );
        coordinates
            .get_mut(ordinal)
            .ok_or_else(|| GeometryErrorKind::Invalid("K0 point slot is unknown".into()))?
            .y = -0.0;
        let mutated = Shape::MultiPoint(crate::geometry::MultiPoint::from_coordseq(
            CoordSeq::try_from_points(&coordinates)?,
        )?);
        let LiteralCheck::Reject(rejected) = bind_source_shape(&topology, mutated, layout)? else {
            panic!("a final K0 buffer sign-bit mutation must fail source binding");
        };
        assert!(rejected.contains(&image));
        Ok(())
    }

    #[test]
    fn source_pole_point_keeps_its_raw_longitude_record() -> Result<()> {
        let shapes = [
            Shape::Point(Point::new_raw_xy(720.0, 90.0)),
            Shape::Point(Point::new_raw_xy(10.0, 0.0)),
        ];
        let topology = test_topology(&shapes);
        let plan = build_q_plan(&topology, OverlayOp::Union)?;
        let source_points = source_point_coordinates(&topology)?;
        let nearest = QCoordinates::build(&topology)?;
        let drawing = draw_q_plan_unmetered(&plan, &topology, &nearest, &source_points)?;
        assert!(plan.source_reuse_is_proven(
            &topology,
            &drawing,
            &source_points,
            &mut crate::geometry::Unmetered,
        )?);

        let actual = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
            &topology,
            OverlayOp::Union,
            CoordinateAxes::XY,
            false,
            None,
        )?;
        let Shape::MultiPoint(actual_points) = actual else {
            panic!("the source pole point and ordinary point remain a MultiPoint");
        };
        assert!(
            actual_points.iter_coords().any(|point| {
                point.x.to_bits() == 720.0_f64.to_bits() && point.y.to_bits() == 90.0_f64.to_bits()
            }),
            "the selected source-point record owns its raw pole longitude"
        );

        let (shape, layout) = source_sink(&topology, &plan, &source_points)?;
        let pole_source_image = layout
            .points
            .iter()
            .map(|location| location.image)
            .find(|image| match image {
                QSourceImage::SourcePoint(index) => topology
                    .sources
                    .points
                    .get(*index as usize)
                    .is_some_and(|source| {
                        source.point.x.to_bits() == 720.0_f64.to_bits()
                            && source.point.y.to_bits() == 90.0_f64.to_bits()
                    }),
                _ => false,
            })
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "source pole slot lost its source-record authority".into(),
                )
            })?;
        assert!(matches!(
            bind_source_shape(&topology, shape, layout)?,
            LiteralCheck::Pass(_)
        ));
        assert!(matches!(pole_source_image, QSourceImage::SourcePoint(_)));
        Ok(())
    }

    #[test]
    fn k0_source_ring_y_bits_are_rechecked_in_the_final_shape() -> Result<()> {
        let source = crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))")?;
        let topology = test_topology(std::slice::from_ref(&source));
        let plan = build_q_plan(&topology, OverlayOp::Union)?;
        let source_points = source_point_coordinates(&topology)?;
        let nearest = QCoordinates::build(&topology)?;
        let drawing = plan.drawing(
            &topology,
            &nearest,
            &source_points,
            &mut crate::geometry::Unmetered,
        )?;
        assert!(plan.source_reuse_is_proven(
            &topology,
            &drawing,
            &source_points,
            &mut crate::geometry::Unmetered,
        )?);
        let actual = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
            &topology,
            OverlayOp::Union,
            CoordinateAxes::XY,
            false,
            None,
        )?;
        let Shape::Polygon(actual_polygon) = actual else {
            panic!("the K0 source ring remains one polygon");
        };
        assert!(
            actual_polygon.shell.coords().points().any(|point| {
                point.x.to_bits() == 2.0_f64.to_bits() && point.y.to_bits() == 0.0_f64.to_bits()
            }),
            "the unchanged source ring keeps its positive-zero Y record"
        );

        let (shape, layout) = source_sink(&topology, &plan, &source_points)?;
        let Shape::Polygon(polygon) = shape else {
            panic!("the K0 source ring remains one polygon");
        };
        let mut coordinates = polygon.shell.coords().points().collect::<Vec<_>>();
        let ordinal = coordinates
            .iter()
            .position(|point| {
                point.x.to_bits() == 2.0_f64.to_bits() && point.y.to_bits() == 0.0_f64.to_bits()
            })
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("pinned source ring Y record is missing".into())
            })?;
        let image = layout
            .polygons
            .first()
            .and_then(|polygon| polygon.shell.images.get(ordinal))
            .copied()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("pinned source ring slot has no literal receipt".into())
            })?;
        assert!(
            matches!(
                image,
                QSourceImage::SourceRingStart(_) | QSourceImage::SourceRingEnd(_)
            ),
            "the literal slot retains its original source-edge endpoint"
        );
        coordinates
            .get_mut(ordinal)
            .ok_or_else(|| GeometryErrorKind::Invalid("source ring slot is unknown".into()))?
            .y = -0.0;
        let shell = Ring::try_from_closed(CoordSeq::try_from_points(&coordinates)?)?;
        let mutated = Shape::Polygon(Polygon::new(shell, polygon.holes.to_vec()));
        let LiteralCheck::Reject(rejected) = bind_source_shape(&topology, mutated, layout)? else {
            panic!("a final K0 source-ring sign-bit mutation must fail binding");
        };
        assert!(rejected.contains(&image));
        Ok(())
    }

    #[test]
    fn computed_q_line_alias_does_not_acquire_source_xy_bit_pinning() -> Result<()> {
        let line = Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(&[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
        ])?)?);
        let topology = test_topology(std::slice::from_ref(&line));
        let plan = build_q_plan(&topology, OverlayOp::Union)?;
        let source_points = source_point_coordinates(&topology)?;
        let (shape, layout) = source_sink(&topology, &plan, &source_points)?;
        let image = layout
            .lines
            .first()
            .and_then(|row| row.images.first())
            .copied()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("computed Q line has no literal image".into())
            })?;
        assert!(matches!(image, QSourceImage::Port(_)));

        let Shape::LineString(line) = shape else {
            panic!("one computed Q line is a LineString sink");
        };
        let mut coordinates = line.as_coords().points().collect::<Vec<_>>();
        let point = coordinates
            .first_mut()
            .ok_or_else(|| GeometryErrorKind::Invalid("computed Q line is empty".into()))?;
        assert_eq!(point.y.to_bits(), 0.0_f64.to_bits());
        point.y = -0.0;
        let alias = Shape::LineString(LineSeq::from_proven(CoordSeq::try_from_points(
            &coordinates,
        )?));
        assert!(matches!(
            bind_source_shape(&topology, alias, layout)?,
            LiteralCheck::Pass(_)
        ));
        Ok(())
    }

    #[test]
    fn seam_only_collapsed_support_publishes_its_odd_endpoints() {
        let shape = crate::io::parse_wkt("POLYGON ((-180 -85,180 -85,180 85,-180 85,-180 -85))")
            .expect("seam-only polygon parses");
        let Shape::MultiPoint(points) = exact_boundary(&shape) else {
            panic!("seam-only boundary is multipoint");
        };
        let frame = GeographicFrame::crs84();
        let mut actual = points
            .iter_coords()
            .map(|point| {
                (
                    frame
                        .longitude()
                        .canonicalize(point.x)
                        .expect("emitted longitude is in the frame")
                        .get()
                        .to_bits(),
                    point.y.to_bits(),
                )
            })
            .collect::<Vec<_>>();
        actual.sort_unstable();
        let mut expected = [
            ((-180.0_f64).to_bits(), (-85.0_f64).to_bits()),
            ((-180.0_f64).to_bits(), 85.0_f64.to_bits()),
        ];
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }

    #[test]
    fn q_ring_cycles_keep_the_lifted_alias_after_two_native_periods() {
        let grads_unit = AngularFactor::try_new("grad", std::f64::consts::TAU / 400.0).unwrap();
        let radians_unit = AngularFactor::try_new("radian", 1.0).unwrap();
        let cases: [(&str, GeographicFrame, &[f64], f64, f64); 3] = [
            (
                "degree",
                GeographicFrame::crs84(),
                [
                    122.427_194,
                    -177.572_806,
                    -117.572_806,
                    -57.572_806,
                    2.427_194,
                    62.427_194,
                ]
                .as_slice(),
                842.427_194,
                -597.572_806,
            ),
            (
                "grad",
                GeographicFrame::try_new(grads_unit, grads_unit, 0.0).unwrap(),
                [
                    25.0,
                    75.0,
                    124.999_999_999_999_99,
                    174.999_999_999_999_97,
                    -174.999_999_999_999_97,
                    -124.999_999_999_999_97,
                    -74.999_999_999_999_99,
                    -24.999_999_999_999_993,
                ]
                .as_slice(),
                825.0,
                -775.0,
            ),
            (
                "radian",
                GeographicFrame::try_new(radians_unit, radians_unit, 0.0).unwrap(),
                [
                    0.25,
                    1.035_398_163_397_448_3,
                    1.820_796_326_794_896_6,
                    2.606_194_490_192_345,
                    -2.891_592_653_589_793,
                    -2.106_194_490_192_345,
                    -1.320_796_326_794_896_6,
                    -0.535_398_163_397_448_3,
                ]
                .as_slice(),
                12.816_370_614_359_172,
                -12.316_370_614_359_172,
            ),
        ];

        for (name, frame, lap, positive, negative) in cases {
            for (direction, expected) in [(1.0, positive), (-1.0, negative)] {
                let path = if direction > 0.0 {
                    lap.to_vec()
                } else {
                    std::iter::once(lap[0])
                        .chain(lap[1..].iter().rev().copied())
                        .collect()
                };
                let mut coordinates = path
                    .iter()
                    .map(|&longitude| (longitude, 0.1))
                    .collect::<Vec<_>>();
                let repeated_start = coordinates.len();
                coordinates.push((lap[0], 0.1));
                coordinates.extend(path.iter().skip(1).map(|&longitude| (longitude, 0.1)));
                let high_latitude_vertex = coordinates.len();
                coordinates.push((lap[0], 0.2));
                coordinates.push((lap[0], 0.1));

                let chart_points =
                    q_ring_chart_points(frame, &coordinates, &[repeated_start]).unwrap();

                assert_eq!(
                    chart_points[high_latitude_vertex].x.to_bits(),
                    expected.to_bits(),
                    "{name} frame, direction {direction}"
                );
                assert_eq!(
                    chart_points.last().unwrap().x.to_bits(),
                    chart_points.first().unwrap().x.to_bits(),
                    "{name} frame closure"
                );
            }
        }
    }

    #[test]
    fn q_ring_half_period_ties_keep_each_native_seam_spelling() {
        let grads_unit = AngularFactor::try_new("grad", std::f64::consts::TAU / 400.0).unwrap();
        let radians_unit = AngularFactor::try_new("radian", 1.0).unwrap();
        let cases = [
            ("degree", GeographicFrame::crs84(), -180.0),
            (
                "grad",
                GeographicFrame::try_new(grads_unit, grads_unit, 0.0).unwrap(),
                -199.999_999_999_999_97,
            ),
            (
                "radian",
                GeographicFrame::try_new(radians_unit, radians_unit, 0.0).unwrap(),
                -std::f64::consts::PI,
            ),
        ];

        for (name, frame, seam) in cases {
            let chart_points =
                q_ring_chart_points(frame, &[(0.0, 0.1), (seam, 0.1), (0.0, 0.1)], &[]).unwrap();
            assert_eq!(chart_points[1].x.to_bits(), seam.to_bits(), "{name} seam");
            assert_eq!(
                chart_points.last().unwrap().x.to_bits(),
                chart_points.first().unwrap().x.to_bits(),
                "{name} closure"
            );
        }
    }
}
