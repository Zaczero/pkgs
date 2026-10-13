use super::{
    BoundaryCycle, ExactGeographicTopology, ExactRational, FaceUnion, GeometryErrorKind, LineStep,
    PlannedAreaRing, PolygonPlan, QEmissionPlan, QLineOrigin, QLinePrimitive, QProducer,
    RecipeWalkOwner, ResourceError, Result, SourceEdgeKind, SourceLineSpan, SpanEndpoint, Work,
    build_fragment_recipes, diagnostic_line_paths, even_odd_face_selection, face_regions,
    minimal_face_selection, noded_spans, selected_area_boundaries, selected_face_roots,
    selected_region_facts, try_sort_by_with_work,
};

impl QEmissionPlan {
    pub(in crate::geometry::topology) fn build_source_lines(
        topology: &ExactGeographicTopology,
        line_members: &[usize],
        work: &mut impl Work,
    ) -> Result<Self> {
        let q = &topology.q;
        work.charge_into_error(
            u64::try_from(topology.sources.edges.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut spans_by_source = Vec::with_capacity(topology.sources.edges.len());
        spans_by_source.resize_with(topology.sources.edges.len(), Vec::<SourceLineSpan>::new);
        for (span_index, span) in q.spans.iter().enumerate() {
            work.charge_into_error(1)?;
            for (owner_index, owner) in span.owners.iter().enumerate() {
                work.charge_into_error(1)?;
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

        work.charge_into_error(
            u64::try_from(line_members.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut line_paths = Vec::with_capacity(line_members.len());
        for &member in line_members {
            let line = topology.sources.lines.get(member).ok_or_else(|| {
                GeometryErrorKind::Invalid("native source line member is unknown".into())
            })?;
            let mut path = Vec::new();
            for source_edge in line.source_edges.clone() {
                work.charge_into_error(1)?;
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
                    work.charge_into_error(1)?;
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
                    work.charge_into_error(1)?;
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
                source_row: None,
                close: false,
                path,
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
            force_native: true,
            boundary_support: None,
            producer: QProducer::SourceLines,
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
