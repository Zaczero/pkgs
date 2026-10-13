use super::super::{
    ChartPole, ExactChartPoint, ExactRational, FeatureFamilyRegistry, GeographicFrame,
    GeometryErrorKind, NativeAreaEdge, NativeAreaEdgeKind, NativeChartSpellingKey,
    NativeGraphEdgeOwner, NativeGraphSewingKind, NativePortData, NativeReference, OccurrenceSource,
    QCandidate, ResourceError, Result, SearchAccount, UnionFind, Work, XY, native_chart_vertex_key,
    native_graph_cycle_vertex,
};

pub(in crate::geometry::topology::q_embedding) fn native_whole_sphere_cell_points(
    frame: GeographicFrame,
    cell: u8,
    budget: &mut impl Work,
) -> Result<[ExactChartPoint; 6]> {
    fn point(
        longitude: &ExactRational,
        latitude: &ExactRational,
        budget: &mut impl Work,
    ) -> Result<ExactChartPoint> {
        Ok(ExactChartPoint::from_rationals(
            longitude.try_clone_with_work(budget)?,
            latitude.try_clone_with_work(budget)?,
        ))
    }

    // Away from the collapsed poles and sewn seam, these are disjoint strips.
    let half = ExactRational::from_f64(frame.longitude().half_period());
    let quarter = half.try_product_with_work(&ExactRational::from_f64(0.5), budget)?;
    let zero = ExactRational::zero();
    let negative_half = zero.try_subtract_with_work(&half, budget)?;
    let negative_quarter = zero.try_subtract_with_work(&quarter, budget)?;
    let south = ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::South));
    let north = ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::North));
    if !negative_half
        .try_cmp_with_work(&negative_quarter, budget)?
        .is_lt()
        || !negative_quarter.try_cmp_with_work(&zero, budget)?.is_lt()
        || !zero.try_cmp_with_work(&quarter, budget)?.is_lt()
        || !quarter.try_cmp_with_work(&half, budget)?.is_lt()
        || !south.try_cmp_with_work(&north, budget)?.is_lt()
    {
        return Err(GeometryErrorKind::Invalid(
            "native whole-sphere chart does not have two ordered longitude strips".into(),
        )
        .into());
    }
    Ok(match cell {
        0 => [
            point(&negative_half, &south, budget)?,
            point(&negative_quarter, &south, budget)?,
            point(&zero, &south, budget)?,
            point(&zero, &north, budget)?,
            point(&negative_quarter, &north, budget)?,
            point(&negative_half, &north, budget)?,
        ],
        1 => [
            point(&zero, &south, budget)?,
            point(&quarter, &south, budget)?,
            point(&half, &south, budget)?,
            point(&half, &north, budget)?,
            point(&quarter, &north, budget)?,
            point(&zero, &north, budget)?,
        ],
        _ => {
            return Err(GeometryErrorKind::Invalid(
                "native whole-sphere chart cell is unknown".into(),
            )
            .into());
        },
    })
}
pub(in crate::geometry::topology::q_embedding) fn validate_native_whole_sphere_cover(
    reference: &NativeReference<'_>,
    budget: &mut impl Work,
) -> Result<()> {
    // The two exact strip walks and their shared/sewn meridians certify the quotient cover.
    let graph = &reference.graph;
    if !reference.plan.whole_sphere
        || !reference.line_graph_edges.is_empty()
        || reference.area_edges.len() != 12
        || graph.atoms.len() != 11
        || graph.faces.count != 3
        || graph.cycles.len() != 2
        || graph.parts.len() != 2
        || reference.plan.parts.len() != 2
    {
        return Err(GeometryErrorKind::Invalid(
            "native whole-sphere reference is not the fixed two-cell chart cover".into(),
        )
        .into());
    }
    budget.charge_into_error(
        u64::try_from(graph.points.exact.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut area_vertices = vec![false; graph.points.exact.len()];
    for endpoints in &graph.edge_nodes {
        for &vertex in endpoints {
            budget.charge_into_error(1)?;
            let slot = area_vertices.get_mut(vertex as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "whole-sphere edge endpoint is outside its chart-vertex table".into(),
                )
            })?;
            *slot = true;
        }
    }
    let mut area_vertex_count = 0_usize;
    for &used in &area_vertices {
        budget.charge_into_error(1)?;
        area_vertex_count = area_vertex_count
            .checked_add(usize::from(used))
            .ok_or(ResourceError::SizeOverflow)?;
    }
    if area_vertex_count != 10 {
        return Err(GeometryErrorKind::Invalid(
            "native whole-sphere cover does not have ten chart-local vertices".into(),
        )
        .into());
    }
    budget.charge_into_error(
        u64::try_from(reference.area_edges.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut cell_edge_counts = [0_usize; 2];
    let mut region = None;
    for edge in &reference.area_edges {
        budget.charge_into_error(1)?;
        let NativeAreaEdgeKind::WholeSphereCover { cell } = &edge.kind else {
            return Err(GeometryErrorKind::Invalid(
                "native whole-sphere chart cover contains a non-cover edge".into(),
            )
            .into());
        };
        let NativeGraphEdgeOwner::Area {
            region: edge_region,
        } = edge.owner
        else {
            return Err(GeometryErrorKind::Invalid(
                "native whole-sphere cover edge has no area owner".into(),
            )
            .into());
        };
        let count = cell_edge_counts.get_mut(*cell as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("native whole-sphere edge has an unknown cell".into())
        })?;
        *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        if region.is_some_and(|previous| previous != edge_region) {
            return Err(GeometryErrorKind::Invalid(
                "native whole-sphere chart cells disagree on their selected region".into(),
            )
            .into());
        }
        region = Some(edge_region);
    }
    if cell_edge_counts != [6, 6] {
        return Err(GeometryErrorKind::Invalid(
            "native whole-sphere chart cells do not have six directed edges each".into(),
        )
        .into());
    }
    let selected_region = reference
        .plan
        .selected_face_roots
        .iter()
        .flatten()
        .copied()
        .next()
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere cover has no selected source region".into())
        })?;
    if reference.plan.selected_face_roots.len() != reference.plan.topology.q.global_face_count
        || region != Some(selected_region)
        || reference
            .plan
            .selected_face_roots
            .iter()
            .any(|root| *root != Some(selected_region))
    {
        return Err(GeometryErrorKind::Invalid(
            "whole-sphere chart cells changed the selected source region".into(),
        )
        .into());
    }

    let frame = reference.plan.topology.sources.frame;
    let half = ExactRational::from_f64(frame.longitude().half_period());
    let west_longitude = ExactRational::zero().try_subtract_with_work(&half, budget)?;
    let period = ExactRational::from_f64(frame.longitude().period());
    let mut pole_sewing = UnionFind::new(graph.points.exact.len());
    for alias in &graph.sewing {
        if alias.kind == NativeGraphSewingKind::PoleLongitude {
            budget.charge_into_error(1)?;
            pole_sewing.union_with_work(
                alias.vertices[0] as usize,
                alias.vertices[1] as usize,
                budget,
            )?;
        }
    }
    let mut seam_bank_edges: [Option<[u32; 2]>; 2] = std::array::from_fn(|_| None);
    let mut shared_edge = None;
    for (atom_index, atom) in graph.atoms.iter().enumerate() {
        budget.charge_into_error(1)?;
        match atom.contributors.as_slice() {
            [_, _] => {
                if shared_edge.is_some() {
                    return Err(GeometryErrorKind::Invalid(
                        "native whole-sphere cover has multiple shared chart edges".into(),
                    )
                    .into());
                }
                let first = atom.contributors.first().ok_or_else(|| {
                    GeometryErrorKind::Invalid("shared cover edge lost its first owner".into())
                })?;
                let second = atom.contributors.get(1).ok_or_else(|| {
                    GeometryErrorKind::Invalid("shared cover edge lost its second owner".into())
                })?;
                let first_written =
                    reference
                        .area_edges
                        .get(first.written_edge)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid("shared cover edge owner is unknown".into())
                        })?;
                let second_written =
                    reference
                        .area_edges
                        .get(second.written_edge)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid("shared cover edge owner is unknown".into())
                        })?;
                let (
                    NativeAreaEdgeKind::WholeSphereCover { cell: first_cell },
                    NativeAreaEdgeKind::WholeSphereCover { cell: second_cell },
                ) = (&first_written.kind, &second_written.kind)
                else {
                    return Err(GeometryErrorKind::Invalid(
                        "shared whole-sphere edge is not owned by both chart cells".into(),
                    )
                    .into());
                };
                let (Some(first_region), Some(second_region)) = (first.region, second.region)
                else {
                    return Err(GeometryErrorKind::Invalid(
                        "shared whole-sphere edge has a non-area contributor".into(),
                    )
                    .into());
                };
                if first_cell == second_cell
                    || first.reversed == second.reversed
                    || first_region != second_region
                {
                    return Err(GeometryErrorKind::Invalid(
                        "shared whole-sphere edge lost its opposite signed cell contributors"
                            .into(),
                    )
                    .into());
                }
                let endpoints = graph.edge_nodes.get(atom_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("shared whole-sphere edge has no endpoints".into())
                })?;
                for &vertex in endpoints {
                    budget.charge_into_error(1)?;
                    let point = graph.points.exact.get(vertex as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "shared whole-sphere edge endpoint is unknown".into(),
                        )
                    })?;
                    if !point.x.is_zero() {
                        return Err(GeometryErrorKind::Invalid(
                            "whole-sphere cells do not share the zero-meridian edge".into(),
                        )
                        .into());
                    }
                }
                shared_edge = Some(atom_index);
            },
            [_] => {},
            _ => {
                return Err(GeometryErrorKind::Invalid(
                    "native whole-sphere chart atom has an invalid contributor count".into(),
                )
                .into());
            },
        }
    }
    if shared_edge.is_none() {
        return Err(GeometryErrorKind::Invalid(
            "native whole-sphere cells have no shared zero-meridian edge".into(),
        )
        .into());
    }

    let mut part_cells = [false; 2];
    for part in &graph.parts {
        budget.charge_into_error(1)?;
        if !part.holes.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "whole-sphere chart cell unexpectedly has a hole".into(),
            )
            .into());
        }
        let cycle = graph.cycles.get(part.shell).ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere chart cell shell is unknown".into())
        })?;
        if cycle.edges.len() != 6 || !cycle.shell || cycle.region != part.region {
            return Err(GeometryErrorKind::Invalid(
                "whole-sphere chart cell shell differs from its selected face".into(),
            )
            .into());
        }
        let mut cell = None;
        for edge in &cycle.edges {
            budget.charge_into_error(1)?;
            for &written_edge in &edge.contributors {
                budget.charge_into_error(1)?;
                let written = reference.area_edges.get(written_edge).ok_or_else(|| {
                    GeometryErrorKind::Invalid("whole-sphere shell contributor is unknown".into())
                })?;
                let NativeAreaEdgeKind::WholeSphereCover { cell: edge_cell } = &written.kind else {
                    return Err(GeometryErrorKind::Invalid(
                        "whole-sphere shell contains a non-cover contributor".into(),
                    )
                    .into());
                };
                if cell.is_some_and(|previous| previous != *edge_cell) {
                    return Err(GeometryErrorKind::Invalid(
                        "whole-sphere shell mixes the two chart-cell recipes".into(),
                    )
                    .into());
                }
                cell = Some(*edge_cell);
            }
        }
        let cell = cell.ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere shell has no written cell owner".into())
        })?;
        let expected_vertices =
            native_whole_sphere_cell_points(reference.plan.topology.sources.frame, cell, budget)?;
        let mut expected_edges = [false; 6];
        for cycle_edge in &cycle.edges {
            budget.charge_into_error(1)?;
            let start = native_graph_cycle_vertex(
                reference.plan.topology.sources.frame,
                graph,
                cycle_edge,
                0,
                &reference.area_edges,
                budget,
            )?;
            let end = native_graph_cycle_vertex(
                reference.plan.topology.sources.frame,
                graph,
                cycle_edge,
                1,
                &reference.area_edges,
                budget,
            )?;
            let mut expected_index = None;
            for start_index in 0..expected_vertices.len() {
                budget.charge_into_error(1)?;
                let end_index = start_index
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?
                    .checked_rem(expected_vertices.len())
                    .ok_or(ResourceError::SizeOverflow)?;
                let expected_start = expected_vertices.get(start_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("whole-sphere cell start is unknown".into())
                })?;
                let expected_end = expected_vertices.get(end_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("whole-sphere cell end is unknown".into())
                })?;
                if &start.point == expected_start && &end.point == expected_end {
                    expected_index = Some(start_index);
                    break;
                }
            }
            let expected_index = expected_index.ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "whole-sphere shell differs from its exact longitude strip".into(),
                )
            })?;
            let seen = expected_edges.get_mut(expected_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("whole-sphere expected edge is unknown".into())
            })?;
            if std::mem::replace(seen, true) {
                return Err(GeometryErrorKind::Invalid(
                    "whole-sphere shell repeats an exact template edge".into(),
                )
                .into());
            }
            if start.point.x == end.point.x
                && (start.point.x == west_longitude || start.point.x == half)
            {
                let seam = seam_bank_edges.get_mut(cell as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("whole-sphere seam cell is unknown".into())
                })?;
                if seam
                    .replace([
                        graph.local_vertex(native_chart_vertex_key(&start), budget)?,
                        graph.local_vertex(native_chart_vertex_key(&end), budget)?,
                    ])
                    .is_some()
                {
                    return Err(GeometryErrorKind::Invalid(
                        "whole-sphere cell has multiple seam-bank edges".into(),
                    )
                    .into());
                }
            }
        }
        if expected_edges.iter().any(|&seen| !seen) {
            return Err(GeometryErrorKind::Invalid(
                "whole-sphere cell omits an exact longitude-strip edge".into(),
            )
            .into());
        }
        if graph
            .face_cells
            .get(part.face.0 as usize)
            .copied()
            .flatten()
            != Some(cell)
        {
            return Err(GeometryErrorKind::Invalid(
                "whole-sphere shell lost its assigned chart-cell face".into(),
            )
            .into());
        }
        *part_cells.get_mut(cell as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere part has an unknown cell owner".into())
        })? = true;
    }
    if part_cells != [true, true] {
        return Err(GeometryErrorKind::Invalid(
            "native whole-sphere parts do not preserve both chart cells".into(),
        )
        .into());
    }
    let [Some(west_seam), Some(east_seam)] = seam_bank_edges else {
        return Err(GeometryErrorKind::Invalid(
            "native whole-sphere cover lost one side of its sewn bank edge".into(),
        )
        .into());
    };
    for (west_vertex, east_vertex) in [(west_seam[0], east_seam[1]), (west_seam[1], east_seam[0])] {
        budget.charge_into_error(1)?;
        let west_key = graph.vertex_keys.get(west_vertex as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere west seam vertex is unknown".into())
        })?;
        let east_key = graph.vertex_keys.get(east_vertex as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("whole-sphere east seam vertex is unknown".into())
        })?;
        let west_point = graph
            .points
            .exact
            .get(west_vertex as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("whole-sphere west seam point is unknown".into())
            })?;
        let east_point = graph
            .points
            .exact
            .get(east_vertex as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("whole-sphere east seam point is unknown".into())
            })?;
        if west_key.node != east_key.node
            || !matches!(west_key.spelling, NativeChartSpellingKey::Pole(_))
            || !matches!(east_key.spelling, NativeChartSpellingKey::Pole(_))
            || !west_point.x.try_eq_with_work(&west_longitude, budget)?
            || !east_point.x.try_cmp_with_work(&half, budget)?.is_eq()
            || !east_point
                .x
                .try_subtract_with_work(&west_point.x, budget)?
                .try_cmp_with_work(&period, budget)?
                .is_eq()
            || !east_point
                .y
                .try_cmp_with_work(&west_point.y, budget)?
                .is_eq()
            || pole_sewing.find_with_work(west_vertex as usize, budget)?
                != pole_sewing.find_with_work(east_vertex as usize, budget)?
        {
            return Err(GeometryErrorKind::Invalid(
                "whole-sphere bank edges are not oppositely sewn at their pole endpoints".into(),
            )
            .into());
        }
    }
    for part in &reference.plan.parts {
        budget.charge_into_error(1)?;
        if part.shell.len() != 6 || !part.holes.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "literal whole-sphere part differs from its fixed six-edge cell".into(),
            )
            .into());
        }
    }
    Ok(())
}
pub(in crate::geometry::topology::q_embedding) fn native_whole_sphere_cell_edges(
    builder: &mut super::super::super::emission::EmissionBuilder<'_>,
    frame: GeographicFrame,
    region: usize,
    presentation_candidates: &mut Vec<Option<QCandidate>>,
    families: &mut FeatureFamilyRegistry,
    budget: &mut impl SearchAccount,
) -> Result<Vec<NativeAreaEdge>> {
    const CELL_EDGES: [(u8, usize, usize); 12] = [
        (0, 0, 1),
        (0, 1, 2),
        (0, 2, 7),
        (0, 7, 6),
        (0, 6, 5),
        (0, 5, 0),
        (1, 2, 3),
        (1, 3, 4),
        (1, 4, 9),
        (1, 9, 8),
        (1, 8, 7),
        (1, 7, 2),
    ];

    let longitude = frame.longitude();
    let half = ExactRational::from_f64(longitude.half_period());
    let quarter = half.try_product_with_work(&ExactRational::from_f64(0.5), budget)?;
    let zero = ExactRational::zero();
    let longitudes = [
        zero.try_subtract_with_work(&half, budget)?,
        zero.try_subtract_with_work(&quarter, budget)?,
        ExactRational::zero(),
        quarter.try_clone_with_work(budget)?,
        half,
    ];
    budget.charge_into_error(10)?;
    let mut poles = Vec::with_capacity(10);
    for (pole_index, pole) in [ChartPole::South, ChartPole::North].into_iter().enumerate() {
        let latitude = ExactRational::from_f64(frame.chart_pole_latitude(pole));
        let candidate = QCandidate {
            native: XY::new(0.0, latitude.round_with_work(budget)?),
            periods: 0,
        };
        for (longitude_index, native_longitude) in longitudes.iter().enumerate() {
            budget.charge_into_error(1)?;
            let point = ExactChartPoint::from_rationals(
                native_longitude.try_clone_with_work(budget)?,
                latitude.try_clone_with_work(budget)?,
            );
            let node = builder.intern_point(&point, budget)?;
            let node_index = node.0 as usize;
            if node_index > presentation_candidates.len() {
                return Err(GeometryErrorKind::Invalid(
                    "whole-sphere presentation node is outside its assigned-site table".into(),
                )
                .into());
            }
            if node_index == presentation_candidates.len() {
                budget.charge_into_error(1)?;
                presentation_candidates.push(Some(candidate));
            } else {
                let slot = presentation_candidates.get_mut(node_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "whole-sphere presentation site slot is unknown".into(),
                    )
                })?;
                if slot.is_some_and(|previous| previous != candidate) {
                    return Err(GeometryErrorKind::Invalid(
                        "whole-sphere pole aliases have conflicting fixed sites".into(),
                    )
                    .into());
                }
                *slot = Some(candidate);
            }
            budget.charge_into_error(1)?;
            poles.push(NativePortData {
                node,
                point,
                spelling: super::super::super::emission::PortSpelling::Pole {
                    native_longitude: native_longitude.round_with_work(budget)?,
                },
                source_record: None,
                occurrences: vec![OccurrenceSource {
                    origin: super::super::super::emission::VertexOrigin::Presentation,
                    source_trace: None,
                }],
            });
            let occurrence = pole_index
                .checked_mul(longitudes.len())
                .and_then(|base| base.checked_add(longitude_index))
                .ok_or(ResourceError::SizeOverflow)?;
            families.register(
                super::super::super::emission::FeatureFamily {
                    parent: super::super::super::emission::FeatureFamilyParent::Region(region),
                    producer: super::super::super::emission::SampleProducerKind::WholeSphereCover,
                    occurrence: u32::try_from(occurrence)
                        .map_err(|_| ResourceError::SizeOverflow)?,
                },
                budget,
            )?;
        }
    }
    families.synchronize_budget(budget)?;

    budget.charge_into_error(
        u64::try_from(CELL_EDGES.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut edges = Vec::with_capacity(CELL_EDGES.len());
    for (cell, start, end) in CELL_EDGES {
        let vertices = [
            poles
                .get(start)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("whole-sphere cell start port is unknown".into())
                })?
                .try_clone_with_work(budget)?,
            poles
                .get(end)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("whole-sphere cell end port is unknown".into())
                })?
                .try_clone_with_work(budget)?,
        ];
        edges.push(NativeAreaEdge {
            vertices,
            owner: NativeGraphEdgeOwner::Area { region },
            kind: NativeAreaEdgeKind::WholeSphereCover { cell },
        });
    }
    Ok(edges)
}
