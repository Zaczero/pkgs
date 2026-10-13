use super::{
    BoundaryCycle, ExactGeographicTopology, FaceUnion, LineStep, PlannedAreaRing, PolygonPlan,
    QEmissionPlan, QLineOrigin, QLinePrimitive, QProducer, RecipeWalkOwner, ResourceError, Result,
    SpanEndpoint, build_fragment_recipes, diagnostic_line_paths, even_odd_face_selection,
    face_regions, minimal_face_selection, noded_spans, selected_area_boundaries,
    selected_face_roots, selected_region_facts,
};
use crate::work::Work;

impl QEmissionPlan {
    pub(in crate::geometry::topology) fn build_node(
        topology: &ExactGeographicTopology,
        work: &mut impl Work,
    ) -> Result<Self> {
        let q = &topology.q;
        let selected = noded_spans(topology, work)?;
        work.charge_into_error(
            u64::try_from(selected.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut line_paths = Vec::with_capacity(selected.len());
        for (index, selected) in selected.into_iter().enumerate() {
            work.charge_into_error(1)?;
            line_paths.push(QLinePrimitive {
                origin: QLineOrigin::Noded,
                recipe_walk: RecipeWalkOwner::Line(index),
                source_row: None,
                close: false,
                path: vec![LineStep {
                    span: selected.span,
                    from_endpoint: SpanEndpoint::Start,
                    owner: selected.owner,
                }],
            });
        }
        let polygons = PolygonPlan::Regions {
            rings: Vec::new(),
            region_count: q.global_face_count,
        };
        let fragment_recipes = build_fragment_recipes(topology, &polygons, &[], &line_paths, work)?;
        work.charge_into_error(
            u64::try_from(q.global_face_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        Ok(Self {
            whole_sphere: false,
            force_native: false,
            boundary_support: None,
            producer: QProducer::Node,
            retain_literals_for_republication: false,
            polygons,
            cycles: Vec::new(),
            selected_regions: Vec::new(),
            selected_face_roots: vec![None; q.global_face_count],
            fragment_recipes,
            line_paths,
            point_nodes: Vec::new(),
        })
    }
    pub(in crate::geometry::topology) fn build_polygonize(
        topology: &ExactGeographicTopology,
        diagnostics: bool,
        work: &mut impl Work,
    ) -> Result<Self> {
        let q = &topology.q;
        let selection = minimal_face_selection(topology, work)?;
        let mut regions = FaceUnion::new(&selection.selected_faces, work)?;
        let cycles = selection
            .cycles
            .into_iter()
            .map(|cycle| BoundaryCycle {
                darts: cycle.darts,
                region: cycle.face.0 as usize,
            })
            .collect::<Vec<_>>();
        let mut roots = Vec::new();
        for (face, &selected) in selection.selected_faces.iter().enumerate() {
            work.charge_into_error(1)?;
            if selected {
                roots.push(face);
            }
        }
        let selected_regions =
            selected_region_facts(q, &selection.selected_faces, &mut regions, &roots, work)?;
        let selected_face_roots =
            selected_face_roots(q, &selection.selected_faces, &mut regions, work)?;
        let mut rings = Vec::with_capacity(cycles.len());
        for (cycle, boundary) in cycles.iter().enumerate() {
            work.charge_into_error(1)?;
            rings.push(PlannedAreaRing::Cycle {
                cycle,
                region: boundary.region,
            });
        }
        let polygons = PolygonPlan::Regions {
            rings,
            region_count: q.global_face_count,
        };
        let line_paths = if diagnostics {
            diagnostic_line_paths(topology, work)?
        } else {
            Vec::new()
        };
        let fragment_recipes =
            build_fragment_recipes(topology, &polygons, &cycles, &line_paths, work)?;
        Ok(Self {
            whole_sphere: false,
            force_native: false,
            boundary_support: None,
            producer: QProducer::Polygonize,
            retain_literals_for_republication: false,
            polygons,
            cycles,
            selected_regions,
            selected_face_roots,
            fragment_recipes,
            line_paths,
            point_nodes: Vec::new(),
        })
    }
    pub(in crate::geometry::topology) fn build_area(
        topology: &ExactGeographicTopology,
        work: &mut impl Work,
    ) -> Result<Self> {
        let q = &topology.q;
        let selection = minimal_face_selection(topology, work)?;
        let selected_faces =
            even_odd_face_selection(topology, &selection.cycles, &selection.cycle_roles, work)?;
        let mut regions = face_regions(q, &selected_faces, work)?;
        let cycles = selected_area_boundaries(q, &selected_faces, &mut regions, work)?;
        work.charge_into_error(
            u64::try_from(cycles.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let roots = cycles.iter().map(|cycle| cycle.region).collect::<Vec<_>>();
        let selected_regions =
            selected_region_facts(q, &selected_faces, &mut regions, &roots, work)?;
        let selected_face_roots = selected_face_roots(q, &selected_faces, &mut regions, work)?;
        let mut rings = Vec::with_capacity(cycles.len());
        for (cycle, boundary) in cycles.iter().enumerate() {
            work.charge_into_error(1)?;
            rings.push(PlannedAreaRing::Cycle {
                cycle,
                region: boundary.region,
            });
        }
        let polygons = PolygonPlan::Regions {
            rings,
            region_count: q.global_face_count,
        };
        let fragment_recipes = build_fragment_recipes(topology, &polygons, &cycles, &[], work)?;
        Ok(Self {
            whole_sphere: false,
            force_native: false,
            boundary_support: None,
            producer: QProducer::BuildArea,
            retain_literals_for_republication: false,
            polygons,
            cycles,
            selected_regions,
            selected_face_roots,
            fragment_recipes,
            line_paths: Vec::new(),
            point_nodes: Vec::new(),
        })
    }
}
