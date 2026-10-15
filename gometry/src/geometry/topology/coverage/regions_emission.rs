use std::sync::Arc;

use super::super::emission::draw::source_point_coordinates_with_work;
use super::super::emission::ordinates::native_chord_points;
use super::super::emission::plan::{
    BoundaryCycle, PlannedAreaRing, PolygonPlan, QEmissionPlan, QProducer, SelectedRegionFact,
};
use super::super::emission::sites::QCoordinates;
use super::super::emission::{
    AssignedSite, CompletedQDrawing, DirectedSpan, EmissionChordId, ExpandedEmissionPlan,
    FragmentRecipes, RecipeWalkOwner, SelectedUse,
};
use super::super::exact::SourceEdgeKind;
use super::super::overlay::{
    build_fragment_recipes, push_source_ring_recipes, resolve_q_embedding,
};
use super::{GeographicCoverageAnalysis, GeographicCoverageFaceAssignment};
use crate::array::StreamingShapes;
use crate::error::Result;
use crate::geometry::embedding_completion::literals::{FlatLiteralLayout, LiteralAddress};
use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
use crate::geometry::embedding_search::account::EvaluationBudget;
use crate::geometry::ordinates::{
    NodeOrdinates, RequestRowInput, SourceArcMetricAccess, resolve_request_rows,
};
use crate::geometry::publication::{ProducerDrawingReceipt, StorageTarget};
use crate::geometry::segments::ordering::try_sort_by_with_work;
use crate::geometry::storage::StorageLayout;
use crate::geometry::{
    ChartMetric, CoordSeq, CoordinateAxes, ExpansionBudget, GeometryErrorKind, HasM, HasZ,
    OverlayOp, Polygon, Ring, Shape, SourceArcContext, TopologyDomain,
};
use crate::work::Work;

pub(in crate::geometry::topology) struct CoverageRows {
    pub(in crate::geometry::topology) declared_axes: CoordinateAxes,
    source_axes: Vec<CoordinateAxes>,
    region_rows: Vec<Option<usize>>,
    target: StorageTarget,
}

impl GeographicCoverageAnalysis {
    pub(in crate::geometry) fn coverage_drawing_receipt<M: ChartMetric>(
        &self,
        assignment: &GeographicCoverageFaceAssignment,
        source_axes: &[CoordinateAxes],
        context: &SourceArcContext<'_, '_, M>,
        target: StorageTarget,
        work: &mut EvaluationBudget,
    ) -> Result<ProducerDrawingReceipt> {
        let TopologyDomain::Geographic(frame) = context.domain() else {
            return Err(GeometryErrorKind::Invalid(
                "geographic coverage cleaning requires its admitted chart frame".into(),
            )
            .into());
        };
        if frame != self.topology.sources.frame
            || source_axes.len() != self.topology.sources.row_count
        {
            return Err(GeometryErrorKind::Invalid(
                "coverage drawing changed its admitted frame or source rows".into(),
            )
            .into());
        }
        if target == StorageTarget::Groups {
            return Err(GeometryErrorKind::Invalid(
                "coverage boundary receipts do not publish grouped rows".into(),
            )
            .into());
        }
        let declared_axes = source_axes
            .iter()
            .fold(CoordinateAxes::XY, |axes, row_axes| axes.union(*row_axes));
        let plans = self.cleaning_boundary_plans(assignment, work)?;
        if target == StorageTarget::Scalar
            && assignment.owners.iter().flatten().any(|&row| row != 0)
        {
            return Err(GeometryErrorKind::Invalid(
                "scalar coverage boundary selection has a live row other than row zero".into(),
            )
            .into());
        }
        let mut region_rows = vec![None; plans.topology.q.global_face_count];
        let mut cycles = Vec::new();
        let mut coverage_cycles = Vec::new();
        let mut selected_regions = Vec::new();
        let mut fragment_recipes = FragmentRecipes::default();
        let whole_sphere = plans.rows.iter().any(|row| row.whole_sphere);
        for row in &plans.rows {
            for region in &row.regions {
                work.charge_into_error(1)?;
                let root = region.id.local_region_root.0 as usize;
                *region_rows.get_mut(root).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage region root is outside Q".into())
                })? = Some(row.output_row);
                selected_regions.push(SelectedRegionFact {
                    root,
                    poles: region.poles,
                });
                for cycle in &region.cycles {
                    let mut darts = Vec::with_capacity(cycle.uses.len());
                    for use_ in &cycle.uses {
                        work.charge_into_error(1)?;
                        darts.push(use_.dart);
                    }
                    cycles.push(BoundaryCycle {
                        darts,
                        region: root,
                    });
                    coverage_cycles.push(cycle);
                }
            }
        }
        try_sort_by_with_work(&mut selected_regions, work, |left, right, _work| {
            Ok(left.root.cmp(&right.root))
        })?;
        let selected_face_roots = plans
            .face_regions
            .iter()
            .map(|region| region.map(|region| region.local_region_root.0 as usize))
            .collect::<Vec<_>>();
        for (owner, region) in assignment.owners.iter().zip(&plans.face_regions) {
            if owner != &region.map(|region| region.output_row) {
                return Err(GeometryErrorKind::Invalid(
                    "coverage output regions changed their assigned owner row".into(),
                )
                .into());
            }
            if let Some(region) = region {
                *region_rows
                    .get_mut(region.local_region_root.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("coverage face region is outside Q".into())
                    })? = Some(region.output_row);
            }
        }
        let polygons = PolygonPlan::build_with_source_filter(
            plans.topology,
            whole_sphere,
            OverlayOp::Union,
            &cycles,
            |ring, source_cycles, work| {
                let source_ring = plans.topology.sources.rings.get(ring).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage source ring is unknown".into())
                })?;
                for &cycle in source_cycles {
                    let cycle = coverage_cycles.get(cycle).ok_or_else(|| {
                        GeometryErrorKind::Invalid("coverage source cycle is unknown".into())
                    })?;
                    for use_ in &cycle.uses {
                        work.charge_into_error(1)?;
                        let source = plans
                            .topology
                            .q
                            .spans
                            .get(use_.span.0 as usize)
                            .and_then(|span| span.owners.get(use_.selected_owner))
                            .and_then(|owner| {
                                plans
                                    .topology
                                    .sources
                                    .edges
                                    .get(owner.source_edge.0 as usize)
                            })
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "coverage selected source edge is unknown".into(),
                                )
                            })?;
                        if !matches!(source.kind, SourceEdgeKind::Ring { ring, .. } if ring == source_ring.id)
                        {
                            return Ok(false);
                        }
                    }
                }
                Ok(true)
            },
            work,
        )?;
        match &polygons {
            PolygonPlan::WholeSphere(_) => {
                fragment_recipes =
                    build_fragment_recipes(plans.topology, &polygons, &cycles, &[], work)?;
            },
            PolygonPlan::Regions { rings, .. } => {
                for planned in rings {
                    match *planned {
                        PlannedAreaRing::Source {
                            ring,
                            region,
                            reverse,
                            ..
                        } => {
                            let source_ring =
                                plans.topology.sources.rings.get(ring).ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "coverage retained source ring is unknown".into(),
                                    )
                                })?;
                            let fragments = push_source_ring_recipes(
                                plans.topology,
                                source_ring,
                                reverse,
                                &mut fragment_recipes,
                                work,
                            )?;
                            fragment_recipes.record_walk(
                                RecipeWalkOwner::SourceRing(ring, Some(region)),
                                fragments,
                            );
                        },
                        PlannedAreaRing::Cycle { cycle, .. } => {
                            let boundary = coverage_cycles.get(cycle).ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "coverage recipe cycle is unknown".into(),
                                )
                            })?;
                            let mut spans = boundary
                                .uses
                                .iter()
                                .map(|use_| DirectedSpan {
                                    span: use_.span,
                                    from: use_.from,
                                    to: use_.to,
                                    owner_index: use_.selected_owner,
                                })
                                .collect::<Vec<_>>();
                            if let Some(pivot) = spans.iter().position(|span| {
                                plans
                                    .topology
                                    .q
                                    .ports
                                    .get(span.from.0 as usize)
                                    .is_some_and(|port| !port.cut)
                            }) {
                                spans.rotate_left(pivot);
                            }
                            let fragments = fragment_recipes.push_walk(
                                plans.topology,
                                &spans,
                                true,
                                SelectedUse::AreaBoundary,
                                work,
                            )?;
                            fragment_recipes.record_walk(RecipeWalkOwner::Cycle(cycle), fragments);
                        },
                    }
                }
            },
        }
        let plan = QEmissionPlan {
            whole_sphere,
            force_native: true,
            boundary_support: None,
            producer: QProducer::Overlay,
            coverage_rows: Some(CoverageRows {
                declared_axes,
                source_axes: source_axes.to_vec(),
                region_rows,
                target,
            }),
            polygons,
            cycles,
            selected_regions,
            selected_face_roots,
            fragment_recipes,
            line_paths: Vec::new(),
            point_nodes: Vec::new(),
        };
        let nearest = QCoordinates::build_with_work(plans.topology, work)?;
        let source_points = source_point_coordinates_with_work(plans.topology, work)?;
        let operation = if target == StorageTarget::Scalar {
            "coverage_union_all"
        } else {
            "coverage_clean"
        };
        let mut expansion = ExpansionBudget::new(operation, "computed edges");
        let embedding = resolve_q_embedding(
            plans.topology,
            &plan,
            &source_points,
            &nearest,
            CoordinateAxes::XY,
            false,
            operation,
            context.metric_access(),
            &mut expansion,
            work,
        )?;
        match embedding.drawing {
            CompletedQDrawing::Native(drawing) => drawing
                .coverage_receipt
                .map(|receipt| *receipt)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "admitted coverage drawing lost its selected output rows".into(),
                    )
                    .into()
                }),
            CompletedQDrawing::Source(_) | CompletedQDrawing::Drawing { .. } => {
                Err(GeometryErrorKind::Invalid(
                    "coverage row selection bypassed native Q admission".into(),
                )
                .into())
            },
        }
    }
}

impl CoverageRows {
    pub(in crate::geometry::topology) fn drawing_receipt<M: ChartMetric>(
        &self,
        plan: &ExpandedEmissionPlan<'_>,
        sites: &[AssignedSite],
        request_rows: &[RequestRowInput],
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
        work: &mut impl Work,
    ) -> Result<ProducerDrawingReceipt> {
        let ordinates = resolve_request_rows(request_rows, plan.topology, metric_access, work)?;
        let row_count = if self.target == StorageTarget::Scalar {
            1
        } else {
            self.source_axes.len()
        };
        let mut row_parts = std::iter::repeat_with(Vec::new)
            .take(row_count)
            .collect::<Vec<_>>();
        for part in &plan.parts {
            work.charge_into_error(1)?;
            let row = self
                .region_rows
                .get(part.region)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("admitted coverage part has no selected row".into())
                })?;
            row_parts
                .get_mut(row)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage part escaped its scalar selection".into())
                })?
                .push(part);
        }
        let mut writer = StreamingShapes::with_capacity(0, row_count);
        let mut scalar = None;
        let mut row_map = FlatLiteralLayout::with_capacity(row_count);
        let mut literals = Vec::new();
        for (logical_row, parts) in row_parts.into_iter().enumerate() {
            let mut axes = if parts.is_empty() && self.target == StorageTarget::Array {
                *self.source_axes.get(logical_row).ok_or_else(|| {
                    GeometryErrorKind::Invalid("empty coverage row has no source axes".into())
                })?
            } else {
                self.declared_axes
            };
            for part in &parts {
                for chord_id in part.shell.iter().chain(part.holes.iter().flatten()) {
                    let chord = plan.chords.get(chord_id.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("coverage part chord is unknown".into())
                    })?;
                    for port in chord.ports {
                        let value = ordinates.get(port.0 as usize).ok_or_else(|| {
                            GeometryErrorKind::Invalid("coverage port ordinates are unknown".into())
                        })?;
                        axes = CoordinateAxes::new(
                            HasZ(axes.has_z() && value.z.value().is_some()),
                            HasM(axes.has_m() && value.m.value().is_some()),
                        );
                    }
                }
            }
            let mut polygons = Vec::with_capacity(parts.len());
            for part in parts {
                let shell = write_ring(plan, sites, &ordinates, &part.shell, axes, work)?;
                let mut holes = Vec::with_capacity(part.holes.len());
                for hole in &part.holes {
                    let ring = write_ring(plan, sites, &ordinates, hole, axes, work)?;
                    holes.push(ring);
                }
                polygons.push(Polygon::new(shell, holes));
            }
            let shape = Shape::narrow_polygons(axes, polygons);
            ProducerDrawingReceipt::capture_shape(
                &mut row_map,
                &mut literals,
                &shape,
                LiteralAddress {
                    operation: 0,
                    logical_row,
                    group_row: None,
                    value_row: None,
                    output: 0,
                },
            )?;
            if self.target == StorageTarget::Scalar {
                scalar = Some(shape);
            } else {
                writer.try_push(shape)?;
            }
        }
        let buffers = if let Some(shape) = scalar {
            FrozenLiteralBuffers::flat_shape(shape, row_map)
        } else {
            let (storage, ..) = writer.finish_parts()?;
            FrozenLiteralBuffers::flat_rows(
                Arc::new(storage),
                None,
                StorageLayout::PresentOnly,
                row_map,
                work,
            )?
        };
        Ok(ProducerDrawingReceipt::new(buffers, literals))
    }
}

fn write_ring(
    plan: &ExpandedEmissionPlan<'_>,
    sites: &[AssignedSite],
    ordinates: &[NodeOrdinates],
    chords: &[EmissionChordId],
    axes: CoordinateAxes,
    work: &mut impl Work,
) -> Result<Ring> {
    let drawing = native_chord_points(plan, sites, ordinates, chords, axes, true, work)?;
    for _ in &drawing.points {
        work.charge_into_error(1)?;
    }
    Ring::try_from_closed(CoordSeq::try_from_points_with_work(&drawing.points, work)?)
}
