use super::{
    BoundaryCycle, DiagnosticKind, DiagnosticSpan, DirectedSpan, ExactDyadic,
    ExactGeographicTopology, ExactRational, ExactSourceRange, ExactSourceRing, FragmentRecipes,
    GeometryErrorKind, LineStep, PlannedAreaRing, PlannedQLine, PolygonPlan, QLineOrigin,
    QLinePrimitive, RecipeWalkOwner, ResourceError, Result, SelectedUse, SpanEndpoint,
    boundary_owner_index,
};
use crate::work::Work;

pub(in crate::geometry::topology) fn build_fragment_recipes(
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
                work.charge_into_error(1)?;
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
                    work.charge_into_error(1)?;
                    let ring = topology.sources.rings.get(ring_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q whole-sphere recipe ring is unknown".into())
                    })?;
                    let fragments =
                        push_source_ring_recipes(topology, ring, false, &mut recipes, work)?;
                    recipes.record_walk(RecipeWalkOwner::SourceRing(ring_index, None), fragments);
                }
            }
        },
        PolygonPlan::Regions { rings, .. } => {
            for planned in rings {
                work.charge_into_error(1)?;
                match *planned {
                    PlannedAreaRing::Source {
                        ring,
                        region,
                        reverse,
                        ..
                    } => {
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
                        recipes.record_walk(
                            RecipeWalkOwner::SourceRing(ring, Some(region)),
                            fragments,
                        );
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
                            work.charge_into_error(1)?;
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
                                owner_index: boundary_owner_index(
                                    topology,
                                    dart.span.0 as usize,
                                    work,
                                )?,
                            });
                        }
                        let mut pivot = None;
                        for (index, span) in spans.iter().enumerate() {
                            work.charge_into_error(1)?;
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
        work.charge_into_error(1)?;
        let mut spans = Vec::with_capacity(line.path.len());
        for step in &line.path {
            work.charge_into_error(1)?;
            let span = topology.q.spans.get(step.span).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line recipe span is unknown".into())
            })?;
            let (from, to) = match step.from_endpoint {
                SpanEndpoint::Start => span.ports.into(),
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
            work.charge_into_error(1)?;
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
        let closed = line.close
            || if let (Some(first), Some(last)) = (spans.first(), spans.last()) {
                work.charge_into_error(1)?;
                let first = topology.q.ports.get(first.from.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q line recipe start is unknown".into())
                })?;
                let last = topology.q.ports.get(last.to.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q line recipe end is unknown".into())
                })?;
                first.node == last.node
            } else {
                false
            };
        let use_kind = match line.origin {
            QLineOrigin::Curve | QLineOrigin::Noded | QLineOrigin::Diagnostic(_) => {
                SelectedUse::Curve
            },
            QLineOrigin::ArealBoundary => SelectedUse::ArealBoundaryLine,
        };
        let fragments = recipes.push_walk(topology, &spans, closed, use_kind, work)?;
        recipes.record_walk(line.recipe_walk, fragments);
    }
    Ok(recipes)
}
pub(in crate::geometry::topology) fn diagnostic_line_paths(
    topology: &ExactGeographicTopology,
    selected_boundary_spans: &[bool],
    work: &mut impl Work,
) -> Result<Vec<PlannedQLine>> {
    let selected =
        super::super::linework::diagnostic_spans(topology, selected_boundary_spans, work)?;
    work.charge_into_error(
        u64::try_from(selected.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut paths = Vec::with_capacity(selected.len());
    for DiagnosticSpan {
        span,
        owner,
        row,
        kind,
    } in selected
    {
        work.charge_into_error(1)?;
        paths.push(QLinePrimitive {
            origin: QLineOrigin::Diagnostic(kind),
            recipe_walk: RecipeWalkOwner::Line(paths.len()),
            source_row: Some(row),
            close: kind == DiagnosticKind::InvalidRing,
            path: vec![LineStep {
                span,
                from_endpoint: SpanEndpoint::Start,
                owner,
            }],
        });
    }
    Ok(paths)
}
pub(in crate::geometry::topology) fn push_source_ring_recipes(
    topology: &ExactGeographicTopology,
    ring: &ExactSourceRing,
    reverse: bool,
    recipes: &mut FragmentRecipes,
    work: &mut impl Work,
) -> Result<Vec<super::super::emission::FragmentRecipeId>> {
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
pub(in crate::geometry::topology) fn push_source_edge_recipe(
    topology: &ExactGeographicTopology,
    edge_index: usize,
    reverse: bool,
    recipes: &mut FragmentRecipes,
    work: &mut impl Work,
) -> Result<super::super::emission::FragmentRecipeId> {
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
    let period_lift = ExactRational::from_dyadic(ExactDyadic::from_i64(lift));
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
