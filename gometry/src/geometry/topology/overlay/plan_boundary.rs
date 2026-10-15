use super::{
    BoundarySupport, ExactGeographicTopology, GeometryErrorKind, Loc, OverlayOp, PolygonPlan,
    QEmissionPlan, QProducer, ResourceError, Result, SourceRowId, boundary_cycle_line_paths,
    build_fragment_recipes, face_regions, row_locations, select_faces, selected_area_boundaries,
    selected_face_roots, selected_region_facts,
};
use crate::work::Work;

impl QEmissionPlan {
    pub(in crate::geometry::topology) fn build_boundary(
        topology: &ExactGeographicTopology,
        work: &mut impl Work,
    ) -> Result<(Self, BoundarySupport)> {
        let q = &topology.q;
        let row = SourceRowId(0);
        let selected_faces = select_faces(q, 1, OverlayOp::Union, work)?;
        let mut has_area = false;
        for &selected in &selected_faces {
            work.charge_into_error(1)?;
            has_area |= selected;
            if has_area {
                break;
            }
        }
        let mut regions = face_regions(q, &selected_faces, work)?;
        let cycles = selected_area_boundaries(q, &selected_faces, &mut regions, work)?;
        work.charge_into_error(
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
            work.charge_into_error(1)?;
            let fan_start = *q.outgoing_offsets.get(node.id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary node has no fan start".into())
            })?;
            let fan_end = *q
                .outgoing_offsets
                .get(node.id.0 as usize + 1)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q boundary node has no fan end".into())
                })?;
            work.charge_into_error(
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
            work.charge_into_error(1)?;
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
                producer: QProducer::Boundary,
                coverage_rows: None,
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
}
