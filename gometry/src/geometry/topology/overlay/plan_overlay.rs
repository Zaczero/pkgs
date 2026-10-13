use super::{
    BoundaryCycle, ExactGeographicTopology, GeometryErrorKind, OverlayOp, PlannedAreaRing,
    PolygonPlan, QEmissionPlan, QProducer, ResourceError, Result, build_fragment_recipes,
    face_regions, line_steps, planned_line_paths, select_faces, select_line_spans, select_points,
    selected_area_boundaries, selected_face_roots, selected_region_facts, source_cycle_start_port,
    source_cycles_by_ring, source_ring_cycles_cover_boundary, source_ring_cycles_follow_source,
};
use crate::work::Work;

impl PolygonPlan {
    pub(in crate::geometry::topology) fn build(
        topology: &ExactGeographicTopology,
        whole_sphere: bool,
        operation: OverlayOp,
        cycles: &[BoundaryCycle],
        work: &mut impl Work,
    ) -> Result<Self> {
        if whole_sphere {
            let mut source_polygons = Vec::new();
            for (index, source_polygon) in topology.sources.polygons.iter().enumerate() {
                work.charge_into_error(1)?;
                if !source_polygon_contributes_to_whole_sphere(source_polygon, operation) {
                    continue;
                }
                let mut has_areal_shell = false;
                for ring_index in source_polygon.rings.clone() {
                    work.charge_into_error(1)?;
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
        work.charge_into_error(
            u64::try_from(cycles.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut emitted_cycles = vec![false; cycles.len()];
        let source_cycles_by_ring = source_cycles_by_ring(topology, cycles, work)?;
        for (ring_index, source_ring) in topology.sources.rings.iter().enumerate() {
            work.charge_into_error(1)?;
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
                work.charge_into_error(1)?;
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
                    source_cycles,
                    work,
                )?
            {
                continue;
            }
            let Some(follows_source) = source_ring_cycles_follow_source(
                topology,
                source_ring,
                cycles,
                source_cycles,
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
                work.charge_into_error(1)?;
                *emitted_cycles.get_mut(cycle_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source-ring cycle has no emission state".into())
                })? = true;
            }
        }
        for (cycle_index, (cycle, &emitted)) in cycles.iter().zip(&emitted_cycles).enumerate() {
            work.charge_into_error(1)?;
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
pub(in crate::geometry::topology) const fn source_polygon_contributes_to_whole_sphere(
    source_polygon: &super::super::exact::ExactSourcePolygon,
    operation: OverlayOp,
) -> bool {
    match operation {
        OverlayOp::Intersection | OverlayOp::Difference => source_polygon.row.0 == 0,
        OverlayOp::Union | OverlayOp::SymmetricDifference => true,
    }
}

impl QEmissionPlan {
    pub(in crate::geometry::topology) fn build(
        topology: &ExactGeographicTopology,
        operation: OverlayOp,
        work: &mut impl Work,
    ) -> Result<Self> {
        let q = &topology.q;
        let row_count = topology.sources.row_count;
        let faces = select_faces(q, row_count, operation, work)?;
        let mut whole_sphere = !faces.is_empty();
        for &selected in &faces {
            work.charge_into_error(1)?;
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
                    work.charge_into_error(1)?;
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
            producer: QProducer::Overlay,
            retain_literals_for_republication: false,
            polygons,
            cycles,
            selected_regions,
            selected_face_roots,
            fragment_recipes,
            line_paths,
            point_nodes,
        })
    }
}
