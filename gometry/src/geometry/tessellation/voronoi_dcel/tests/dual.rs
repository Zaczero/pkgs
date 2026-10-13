#[test]
fn direct_dual_subnormal_admission_matches_legacy_builder() {
    for (units, admitted) in [(1_u64, false), (4, true)] {
        let step = f64::from_bits(units);
        let sites: Vec<_> = [(0.0, 0.0), (step, 0.0), (0.0, step)]
            .into_iter()
            .enumerate()
            .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
            .collect();
        let mut dual_budget = ExpansionBudget::new("test", "subnormal dual");
        let dual = dual_build(
            &sites,
            VoronoiBoundary::Envelope,
            &mut dual_budget,
            &certified_delaunay(&sites).unwrap(),
        );
        let mut legacy_budget = ExpansionBudget::new("test", "subnormal legacy");
        let legacy = build(
            &sites,
            VoronoiBoundary::Envelope,
            &mut legacy_budget,
            false,
            &certified_delaunay(&sites).unwrap(),
        );

        match (admitted, dual, legacy) {
            (false, Err(dual), Err(legacy)) => {
                assert!(
                    matches!(
                        dual.kind(),
                        crate::error::ErrorKind::Geometry(
                            GeometryErrorKind::ExactEmbeddingUnrepresentable { .. }
                        )
                    ),
                    "unexpected dual error: {dual}"
                );
                assert!(
                    matches!(
                        legacy.kind(),
                        crate::error::ErrorKind::Geometry(
                            GeometryErrorKind::ExactEmbeddingUnrepresentable { .. }
                        )
                    ),
                    "unexpected legacy error: {legacy}"
                );
            },
            (true, Ok(dual), Ok(legacy)) => {
                let clip =
                    Shape::Polygon(Polygon::new(rectangle(0.0, 0.0, step, step), Vec::new()));
                for diagram in [dual, legacy] {
                    assert_eq!(diagram.polygons.len(), 3, "units={units}");
                    let cells: Vec<_> = diagram
                        .polygons
                        .iter()
                        .map(|output| Shape::Polygon(output.polygon.clone()))
                        .collect();
                    assert!(cells.iter().all(|cell| matches!(cell, Shape::Polygon(polygon) if polygon_is_valid(polygon).unwrap())));
                    assert!(coverage_is_valid(&cells, 0.0).unwrap(), "units={units}");
                    let union = Shape::union_all(&cells, Strictness::Lenient).unwrap();
                    assert!(
                        clip.difference(&union, Strictness::Lenient)
                            .unwrap()
                            .is_empty()
                    );
                    assert!(
                        union
                            .difference(&clip, Strictness::Lenient)
                            .unwrap()
                            .is_empty()
                    );
                }
            },
            (expected, dual, legacy) => panic!(
                "units={units} expected admitted={expected}, dual admitted={}, legacy admitted={}",
                dual.is_ok(),
                legacy.is_ok(),
            ),
        }
    }
}

#[test]
fn direct_dual_edge_budget_counts_only_the_published_line_column() {
    let source = Shape::MultiPoint(
        vec![
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(2.0, 0.0),
            Point::new_raw_xy(0.0, 2.0),
            Point::new_raw_xy(2.0, 2.0),
            Point::new_raw_xy(0.8, 1.1),
        ]
        .into(),
    );
    let mut budget = ExpansionBudget::new("voronoi_edge_lines", "sites/clip topology");
    let output = source
        .voronoi_edge_column_in_frame_budgeted(0.0, VoronoiBoundary::Envelope, None, &mut budget)
        .unwrap();
    assert_eq!(output.receipt.producer(), VoronoiProducer::Dual);
    assert_receipt_graph(&output.receipt);
    assert_edge_column_receipt_alignment(&output.coordinates, &output.offsets, &output.receipt);
    let edge_count = output.offsets.as_slice().len() - 1;
    assert!(edge_count > 0);
    assert_eq!(output.offsets.as_slice().first(), Some(&0));
    assert_eq!(
        output
            .offsets
            .as_slice()
            .last()
            .copied()
            .map(|end| end as usize),
        Some(output.coordinates.len())
    );
    assert_eq!(budget.used(), output.coordinates.len() + edge_count);
}

#[test]
fn dual_completeness_rejects_a_missing_cell_mutant() {
    let sites: Vec<_> = [(0.0, 0.0), (2.0, 0.0), (0.0, 2.0), (2.0, 2.0), (0.8, 1.1)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let clip = normalize_clip(&sites, VoronoiBoundary::Envelope, false).unwrap();
    let mesh = certified_delaunay(&sites).unwrap();
    let complex = delaunay_complex(&mesh, &sites).unwrap();
    let restored = dual_build_rings(&sites, &mesh, complex, &clip).unwrap();
    let mut mutated = restored.clone();
    mutated.rings.pop();

    let mut restored_budget = ExpansionBudget::new("test", "restored dual completeness");
    let restored_diagram = dual_embed(
        &restored,
        &clip,
        &sites,
        VoronoiProduct::Both,
        &mut restored_budget,
        None,
    )
    .unwrap();
    let restored_admitted = true;
    let mut missing_rows: Vec<_> = restored_diagram
        .polygons
        .iter()
        .map(|output| Shape::Polygon(output.polygon.clone()))
        .collect();
    missing_rows.pop();
    assert!(
        coverage_is_valid(&missing_rows, 0.0).unwrap(),
        "coverage validity deliberately does not certify clip completeness"
    );
    let mut mutated_budget = ExpansionBudget::new("test", "mutated dual completeness");
    let mutated_admitted = dual_embed(
        &mutated,
        &clip,
        &sites,
        VoronoiProduct::Both,
        &mut mutated_budget,
        None,
    )
    .is_ok();
    assert_ne!(
        mutated_admitted, restored_admitted,
        "mutated missing-cell admitted={mutated_admitted}, restored admitted={restored_admitted}"
    );
    assert!(!mutated_admitted && restored_admitted);
}

fn shared_dual_vertex(rings: &DualRings) -> (usize, usize, exact::ExactPoint) {
    for (ring_index, ring) in rings.rings.iter().enumerate() {
        for (vertex_index, vertex) in ring.iter().enumerate() {
            let point = dual_vertex_point(vertex, &rings.centers);
            if rings.rings.iter().enumerate().any(|(other_index, other)| {
                other_index != ring_index
                    && other.iter().any(|other_vertex| {
                        point
                            .same_position(dual_vertex_point(other_vertex, &rings.centers))
                            .unwrap()
                    })
            }) {
                return (ring_index, vertex_index, point.clone());
            }
        }
    }
    panic!("dual mutation fixture has no shared vertex");
}

fn shared_boundary_vertex(
    rings: &DualRings,
    bounds: [f64; 4],
) -> (usize, usize, exact::ExactPoint, usize) {
    for (ring_index, ring) in rings.rings.iter().enumerate() {
        for (vertex_index, vertex) in ring.iter().enumerate() {
            let point = dual_vertex_point(vertex, &rings.centers);
            let rounded = point.round_nearest_even().unwrap();
            let Some(side) = rounded_boundary_side(rounded, bounds) else {
                continue;
            };
            if rings.rings.iter().enumerate().any(|(other_index, other)| {
                other_index != ring_index
                    && other.iter().any(|other_vertex| {
                        point
                            .same_position(dual_vertex_point(other_vertex, &rings.centers))
                            .unwrap()
                    })
            }) {
                return (ring_index, vertex_index, point.clone(), side);
            }
        }
    }
    panic!("dual mutation fixture has no shared boundary vertex");
}

fn rounded_duplicate(point: &exact::ExactPoint) -> exact::ExactPoint {
    let rounded = point.round_nearest_even().unwrap();
    let base = exact::ExactPoint::from_xy(rounded);
    let upper = exact::ExactPoint::from_xy(XY::new(rounded.x.next_up(), rounded.y));
    base.midpoint(&upper).unwrap().midpoint(&base).unwrap()
}

#[expect(clippy::float_cmp, reason = "certified Voronoi topology classifies exact frame and vertex identity before rounding")]
fn rounded_boundary_side(point: XY, bounds: [f64; 4]) -> Option<usize> {
    if point.y == bounds[2] {
        Some(0)
    } else if point.x == bounds[1] {
        Some(1)
    } else if point.y == bounds[3] {
        Some(2)
    } else if point.x == bounds[0] {
        Some(3)
    } else {
        None
    }
}

#[test]
fn dual_local_validator_rejects_deterministic_partition_mutations() {
    let sites: Vec<_> = [
        (0.0, 0.0),
        (1.0, 0.0),
        (0.0, 1.0),
        (1.0, 1.0),
        (0.5, 0.4),
        (0.2, 0.7),
    ]
    .into_iter()
    .enumerate()
    .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
    .collect();
    let clip = normalize_clip(&sites, VoronoiBoundary::Envelope, false).unwrap();
    let mesh = certified_delaunay(&sites).unwrap();
    let complex = delaunay_complex(&mesh, &sites).unwrap();
    let restored = dual_build_rings(&sites, &mesh, complex, &clip).unwrap();
    let mut restored_budget = ExpansionBudget::new("test", "restored dual mutation fixture");
    dual_embed(
        &restored,
        &clip,
        &sites,
        VoronoiProduct::Both,
        &mut restored_budget,
        None,
    )
    .unwrap();

    let mut mutations = Vec::new();

    let mut missing_cell = restored.clone();
    missing_cell.rings.pop();
    mutations.push(("missing cell", missing_cell));

    let mut reversed_ring = restored.clone();
    reversed_ring.rings[0].reverse();
    mutations.push(("reversed ring", reversed_ring));

    let (shared_ring, shared_vertex, shared_point) = shared_dual_vertex(&restored);
    let mut moved_shared_vertex = restored.clone();
    let moved = shared_point.round_nearest_even().unwrap();
    moved_shared_vertex.rings[shared_ring][shared_vertex] = DualVertex::Generated(Box::new(
        exact::ExactPoint::from_xy(XY::new(moved.x + 0.125, moved.y)),
    ));
    mutations.push(("moved shared vertex", moved_shared_vertex));

    let mut duplicate_key = restored.clone();
    let duplicate = rounded_duplicate(&shared_point);
    assert_eq!(
        PointKey::new(duplicate.round_nearest_even().unwrap()),
        PointKey::new(shared_point.round_nearest_even().unwrap())
    );
    assert!(!duplicate.same_position(&shared_point).unwrap());
    duplicate_key.rings[shared_ring][shared_vertex] = DualVertex::Generated(Box::new(duplicate));
    mutations.push(("duplicate rounded key", duplicate_key));

    let bounds = clip.frame_bounds();
    let mut wrong_boundary_direction = restored.clone();
    let mut swapped_boundary = false;
    'find_boundary: for ring in &mut wrong_boundary_direction.rings {
        for index in 0..ring.len() {
            let next = (index + 1) % ring.len();
            let left = dual_vertex_point(&ring[index], &restored.centers)
                .round_nearest_even()
                .unwrap();
            let right = dual_vertex_point(&ring[next], &restored.centers)
                .round_nearest_even()
                .unwrap();
            if rounded_boundary_side(left, bounds).is_some()
                && rounded_boundary_side(left, bounds) == rounded_boundary_side(right, bounds)
            {
                ring.swap(index, next);
                swapped_boundary = true;
                break 'find_boundary;
            }
        }
    }
    assert!(
        swapped_boundary,
        "dual mutation fixture has no boundary edge"
    );
    mutations.push(("wrong boundary direction", wrong_boundary_direction));

    let (boundary_ring, boundary_vertex, boundary_point, boundary_side) =
        shared_boundary_vertex(&restored, bounds);
    let boundary_rounded = boundary_point.round_nearest_even().unwrap();
    for (name, direction) in [("gap", 1.0), ("overlap", -1.0)] {
        let mut mutation = restored.clone();
        let shifted = if boundary_side % 2 == 0 {
            XY::new(boundary_rounded.x + direction * 0.125, boundary_rounded.y)
        } else {
            XY::new(boundary_rounded.x, boundary_rounded.y + direction * 0.125)
        };
        mutation.rings[boundary_ring][boundary_vertex] =
            DualVertex::Generated(Box::new(exact::ExactPoint::from_xy(shifted)));
        mutations.push((name, mutation));
    }

    let mut crossed_ring = restored.clone();
    let crossed_index = crossed_ring
        .rings
        .iter()
        .position(|ring| ring.len() >= 5)
        .expect("dual mutation fixture has no crossable ring");
    crossed_ring.rings[crossed_index].swap(1, 2);
    mutations.push(("crossed ring", crossed_ring));

    for (name, mutation) in mutations {
        let mut budget = ExpansionBudget::new("test", "mutated dual partition");
        assert!(
            dual_embed(
                &mutation,
                &clip,
                &sites,
                VoronoiProduct::Both,
                &mut budget,
                None,
            )
            .is_err(),
            "dual mutation unexpectedly admitted: {name}"
        );
    }
}
