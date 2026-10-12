#[expect(
    clippy::arithmetic_side_effects,
    reason = "the Euler characteristic arithmetic is bounded by the admitted u32 vertex, edge, face, and component counts"
)]
fn assert_boundary_walk_euler(subdivision: &ExactSubdivision) {
    let components = subdivision
        .topology
        .component_of
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let faces = subdivision
        .topology
        .face_starts
        .array_windows::<2>()
        .count();
    assert_eq!(
        subdivision.vertices.len() as isize - subdivision.edges.len() as isize + faces as isize,
        2 * components as isize
    );
    if !subdivision.labels.is_empty() {
        assert_eq!(subdivision.labels.len(), faces);
    }
}

#[test]
fn owner_local_containment_checks_shell_before_source_ordered_nesting() {
    let shell_first = [
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: false,
        },
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Outside,
            nested_descendant: true,
        },
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: true,
        },
    ];
    assert_eq!(
        first_owner_cycle_failure(&shell_first, 0),
        Some((1, OwnerCycleFailure::OutsideShell))
    );

    let source_order = [
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: false,
        },
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: false,
        },
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: true,
        },
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Outside,
            nested_descendant: false,
        },
    ];
    assert_eq!(
        first_owner_cycle_failure(&source_order, 0),
        Some((2, OwnerCycleFailure::NestedHole))
    );

    let boundary_only = [
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: false,
        },
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::BoundaryOnly,
            nested_descendant: false,
        },
    ];
    assert_eq!(
        first_owner_cycle_failure(&boundary_only, 0),
        Some((1, OwnerCycleFailure::BoundaryOnly))
    );
}

fn walk_points(subdivision: &ExactSubdivision, face: BoundaryFaceId) -> Vec<exact::ExactPoint> {
    let topology = &subdivision.topology;
    let walk = &topology.face_slots[topology.face_starts[face.0 as usize] as usize
        ..topology.face_starts[face.0 as usize + 1] as usize];
    walk.iter()
        .map(|&slot| {
            subdivision.vertices[topology.owners[slot as usize] as usize]
                .point
                .clone()
        })
        .collect()
}

fn first_equal_label_misattaches_a_hole(subdivision: &ExactSubdivision) -> bool {
    let mut positives = Vec::new();
    let mut negatives = Vec::new();
    for (index, label) in subdivision.labels.iter().enumerate() {
        if !selected_cell_label(*label) {
            continue;
        }
        let face = BoundaryFaceId(index as u32);
        match exact::cycle_orientation(&walk_points(subdivision, face)).unwrap() {
            ExactSign::Positive => positives.push((face, *label)),
            ExactSign::Negative => negatives.push((face, *label)),
            ExactSign::Zero => panic!("selected walk has zero orientation"),
        }
    }
    positives.sort_unstable_by_key(|(face, _)| *face);
    negatives.into_iter().any(|(negative, label)| {
        let parent = positives
            .iter()
            .find(|(_, other)| other == &label)
            .expect("negative walk has a same-label positive")
            .0;
        let probe = &walk_points(subdivision, negative)[0];
        exact::point_in_cycle(&walk_points(subdivision, parent), probe).unwrap()
            != exact::PointInCycle::Inside
    })
}

#[test]
fn interior_first_separator_order_emits_one_full_chain() {
    let sites = sites_xy(&[(0.0, -1.0), (0.0, 1.0), (2.0, 0.5)]);
    let polygon = Polygon::new(
        closed_ring(&[
            (-1.0, 0.0),
            (-0.5, 0.0),
            (0.0, 0.0),
            (0.5, 0.0),
            (0.5, 2.0),
            (-1.0, 2.0),
        ]),
        Vec::new(),
    );
    let atoms = node_polygon(&sites, &polygon);
    let mut subdivision = build_exact_subdivision(atoms).unwrap();
    let primal_edges = certified_delaunay(&sites).unwrap().primal_edges();
    let adjacency = certified_site_adjacency(&primal_edges, sites.len()).unwrap();
    graft_components(&mut subdivision, &sites, &adjacency).unwrap();
    let assembled = assemble_edges(&subdivision).unwrap();
    let separator = [SiteId(0), SiteId(1)];
    let matching: Vec<_> = assembled
        .iter()
        .filter(|chain| chain.separator == separator)
        .collect();
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0].edges.len(), 3);
    assert_eq!(matching[0].vertices.len(), 4);
    let known = [
        XY::new(-1.0, 0.0),
        XY::new(-0.5, 0.0),
        XY::new(0.0, 0.0),
        XY::new(0.5, 0.0),
    ];
    let round = |vertex: VertexId| {
        let vertex = &subdivision.vertices[vertex.0 as usize];
        vertex
            .fixed_xy
            .map_or_else(|| vertex.point.round_nearest_even(), Ok)
            .unwrap()
    };
    let assembled_coordinates: Vec<_> = matching[0].vertices.iter().copied().map(round).collect();
    assert_eq!(assembled_coordinates.as_slice(), known.as_slice());
    let mut group: Vec<_> = matching[0]
        .edges
        .iter()
        .map(|&edge| (separator, edge))
        .collect();
    group.rotate_left(1);
    let [left, right] = subdivision.edges[group[0].1.0 as usize].endpoints;
    let mut degree = std::collections::BTreeMap::<VertexId, usize>::new();
    for (_, edge_id) in &group {
        for vertex in subdivision.edges[edge_id.0 as usize].endpoints {
            *degree.entry(vertex).or_default() += 1;
        }
    }
    assert_eq!(degree[&left], 2);
    assert_eq!(degree[&right], 2);
    let mut chains = Vec::new();
    assemble_separator_group(&subdivision, separator, &group, &mut chains).unwrap();
    assert_eq!(chains.len(), 1);
    assert_eq!(chains[0].edges.len(), 3);
    assert_eq!(chains[0].vertices.len(), 4);
    let shuffled_coordinates: Vec<_> = chains[0].vertices.iter().copied().map(round).collect();
    assert_eq!(shuffled_coordinates.as_slice(), known.as_slice());
}

#[test]
fn owner_local_hosting_keeps_all_1334_noncollinear_interior_holes() {
    let sites = sites_xy(&[(1000.0, 5.0), (1000.0, 1_000_000.0), (1_000_000.0, 5.0)]);
    let holes = (0_u32..1_334)
        .map(|index| {
            let x = 1.0 + f64::from(index) * 1.4;
            closed_ring(&[(x, 5.0), (x + 0.25, 4.5), (x - 0.25, 4.5)])
        })
        .collect();
    let clip = Polygon::new(rectangle(0.0, 0.0, 2000.0, 10.0), holes);
    let subdivision = grafted_clip_subdivision(&sites, &clip);
    assembly::reset_owner_local_work();
    let parts = assemble_cells(&subdivision).unwrap();
    let work = assembly::owner_local_work();

    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].holes.len(), 1_334);
    assert_eq!(work.owner_ring_edges, 4_006);
    assert_eq!(work.owner_vertices, 4_006);
    assert_eq!(work.host_queries, 1_335);
    assert_eq!(work.ray_hits, 1_334);
    assert_eq!(work.ring_side_checks, 1_335);
    assert_eq!(work.global_edge_slot_visits, subdivision.edge_of_slot.len());
    let shell_points = exact_ring_points(&subdivision, &parts[0].shell);
    let hole_points: Vec<_> = parts[0]
        .holes
        .iter()
        .map(|hole| exact_ring_points(&subdivision, hole))
        .collect();
    assert_holes_inside_exact_shell(&shell_points, &hole_points);
    let mut emitted_rings = Vec::with_capacity(1 + parts[0].holes.len());
    emitted_rings.push(exact_ring_coordinate_key(&subdivision, &parts[0].shell));
    emitted_rings.extend(
        parts[0]
            .holes
            .iter()
            .map(|hole| exact_ring_coordinate_key(&subdivision, hole)),
    );
    emitted_rings.sort_unstable();
    let mut source_rings = Vec::with_capacity(1 + clip.holes.len());
    source_rings.push(ring_coordinate_key(&clip.shell));
    source_rings.extend(clip.holes.iter().map(ring_coordinate_key));
    source_rings.sort_unstable();
    assert_eq!(
        emitted_rings, source_rings,
        "owner-local assembly preserves every exact clip ring coordinate set"
    );
}

#[test]
fn task163_noncollinear_1334_public_output_preserves_owner_parts_and_rings() {
    let sites = sites_xy(&[(500.0, 5.0), (1500.0, 5.0), (1000.0, 7.0)]);
    let clip = task163_tangent_clip(1_334);
    let subdivision = grafted_clip_subdivision(&sites, &clip);
    let parts = assemble_cells(&subdivision).unwrap();
    let mut owner_parts_holes = std::collections::BTreeMap::<u32, (usize, usize)>::new();
    for part in &parts {
        let counts = owner_parts_holes.entry(part.owner.0).or_default();
        counts.0 += 1;
        counts.1 += part.holes.len();
    }
    assert_eq!(
        owner_parts_holes,
        std::collections::BTreeMap::from([(0, (1, 535)), (1, (1, 441)), (2, (3, 356)),])
    );

    let mut expected_parts: Vec<_> = parts
        .iter()
        .map(|part| {
            let mut holes: Vec<_> = part
                .holes
                .iter()
                .map(|ring| exact_ring_coordinate_key(&subdivision, ring))
                .collect();
            holes.sort_unstable();
            (exact_ring_coordinate_key(&subdivision, &part.shell), holes)
        })
        .collect();
    expected_parts.sort_unstable();

    let mut budget = ExpansionBudget::new("test", "1334 tangent holes");
    assembly::reset_owner_local_work();
    let cells = site_shape(&[(500.0, 5.0), (1500.0, 5.0), (1000.0, 7.0)])
        .voronoi_cells_budgeted(0.0, VoronoiBoundary::Polygon(&clip), &mut budget)
        .unwrap();
    let work = assembly::owner_local_work();
    let mut output_parts = Vec::with_capacity(cells.len());
    let mut output_holes = 0;
    for cell in &cells {
        let Shape::Polygon(polygon) = cell else {
            panic!("Voronoi polygon output contains only polygons");
        };
        output_holes += polygon.holes.len();
        let (shell, holes) = polygon_ring_keys(polygon);
        output_parts.push((shell, holes));
    }
    output_parts.sort_unstable();

    assert_eq!(cells.len(), 5);
    assert_eq!(output_holes, 1_332);
    assert_eq!(budget.used(), 1_358);
    assert_eq!(output_parts, expected_parts);
    assert_eq!(work.owner_ring_edges, 5_356);
    assert_eq!(work.owner_vertices, 4_022);
    assert_eq!(work.face_walks, 1_340);
    assert_eq!(work.components, 3);
    assert_eq!(work.host_queries, 3);
    assert_eq!(work.ray_hits, 0);
    assert_eq!(work.ring_side_checks, 1_337);
}

#[test]
fn crossed_component_keeps_distinct_faces_with_the_same_owner() {
    let sites: Vec<_> = [(-10.0, -10.0), (0.0, 10.0), (10.0, -10.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
        -12.0, -1.0, 12.0, 1.0,
    )]);
    let atoms = node_polygon(&sites, &clip_polygon);
    let mut subdivision = build_exact_subdivision(atoms).unwrap();
    assert_boundary_walk_euler(&subdivision);
    let primal_edges = certified_delaunay(&sites).unwrap().primal_edges();
    let adjacency = certified_site_adjacency(&primal_edges, sites.len()).unwrap();
    graft_components(&mut subdivision, &sites, &adjacency).unwrap();
    assert_boundary_walk_euler(&subdivision);
    let parts = assemble_cells(&subdivision).unwrap();
    for part in &parts {
        assert_eq!(part.shell.edges.len() + 1, part.shell.vertices.len());
        assert!(
            part.holes
                .iter()
                .all(|hole| hole.edges.len() + 1 == hole.vertices.len())
        );
    }
    let top_faces: std::collections::BTreeSet<_> = parts
        .iter()
        .filter(|part| part.owner == SiteId(1))
        .map(|part| part.face)
        .collect();
    assert_eq!(parts.len(), 4);
    assert_eq!(top_faces.len(), 2);
}

#[test]
fn isolated_component_exterior_inherits_the_containing_face() {
    let sites: Vec<_> = [(-10.0, -10.0), (0.0, 10.0), (10.0, -10.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
        -1.0, -1.0, 1.0, 1.0,
    )]);
    let atoms = node_polygon(&sites, &clip_polygon);
    let mut subdivision = build_exact_subdivision(atoms).unwrap();
    assert_boundary_walk_euler(&subdivision);
    let primal_edges = certified_delaunay(&sites).unwrap().primal_edges();
    let adjacency = certified_site_adjacency(&primal_edges, sites.len()).unwrap();
    graft_components(&mut subdivision, &sites, &adjacency).unwrap();
    assert_boundary_walk_euler(&subdivision);
    let inherited: Vec<_> = subdivision
        .labels
        .iter()
        .enumerate()
        .filter(|(index, label)| {
            selected_cell_label(**label)
                && exact::cycle_orientation(&walk_points(
                    &subdivision,
                    BoundaryFaceId(*index as u32),
                ))
                .unwrap()
                    == ExactSign::Negative
        })
        .map(|(_, label)| *label)
        .collect();
    assert_eq!(inherited.len(), 1);
    assert_eq!(inherited[0].frame_winding, 1);
    assert_eq!(inherited[0].clip_winding, 1);
    assert!(inherited[0].owner.is_some());
    let parts = assemble_cells(&subdivision).unwrap();
    assert_eq!(parts.iter().map(|part| part.holes.len()).sum::<usize>(), 1);
}

#[test]
fn owner_local_faces_match_exact_membership_on_touching_producer_rings() {
    let sites = sites_xy(&[(-10.0, -10.0), (10.0, -10.0), (0.0, 10.0)]);
    let fixtures = [
        (
            "one-hole-multi-owner",
            Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
                -1.0, -1.0, 1.0, 1.0,
            )]),
            3,
            4,
        ),
        (
            "shell-tangent",
            Polygon::new(rectangle(0.0, 0.0, 8.0, 8.0), vec![closed_ring(&[
                (0.0, 4.0),
                (2.0, 5.0),
                (2.0, 3.0),
            ])]),
            2,
            3,
        ),
        (
            "two-holes-touch",
            Polygon::new(rectangle(0.0, 0.0, 10.0, 10.0), vec![
                rectangle(2.0, 2.0, 4.0, 4.0),
                rectangle(4.0, 4.0, 6.0, 6.0),
            ]),
            2,
            4,
        ),
    ];

    for (name, clip, expected_parts, expected_rings) in fixtures {
        let subdivision = grafted_clip_subdivision(&sites, &clip);
        assembly::reset_owner_local_work();
        let parts = assemble_cells(&subdivision).unwrap_or_else(|error| {
            panic!("{name}: owner-local assembly rejected producer output: {error}")
        });
        let work = assembly::owner_local_work();

        assert_eq!(parts.len(), expected_parts, "{name}");
        assert_eq!(
            parts.iter().map(|part| 1 + part.holes.len()).sum::<usize>(),
            expected_rings,
            "{name}"
        );
        assert_owner_parts_match_exact_point_oracle(&subdivision, &parts);
        assert_eq!(
            work.global_edge_slot_visits,
            subdivision.edge_of_slot.len(),
            "{name}: build one global slot inverse, independent of owner count"
        );
        assert!(
            work.owner_ring_edges <= subdivision.edge_of_slot.len(),
            "{name}"
        );
        assert!(work.host_queries <= work.components, "{name}");
        assert!(
            work.face_walks <= work.owner_ring_edges + work.components,
            "{name}"
        );
        assert!(work.ring_side_checks > 0, "{name}");

        if name == "one-hole-multi-owner" {
            let slots_by_edge = assembly::inverse_edge_slots(&subdivision);
            let owner_zero = parts
                .iter()
                .find(|part| part.owner == SiteId(0))
                .expect("minimal source fixture has owner zero");
            let owner_only = assembly::OwnerLocalFaces::build(
                &subdivision,
                &[&owner_zero.shell],
                &slots_by_edge,
            )
            .unwrap();
            assert!(
                owner_only
                    .ring_side_faces(&subdivision, &slots_by_edge, &owner_zero.shell)
                    .is_ok(),
                "filtering foreign darts must leave a constant shell side"
            );

            let all_owner_rings: Vec<_> = parts
                .iter()
                .flat_map(|part| std::iter::once(&part.shell).chain(part.holes.iter()))
                .collect();
            let foreign_boundary_mutant =
                assembly::OwnerLocalFaces::build(&subdivision, &all_owner_rings, &slots_by_edge)
                    .unwrap();
            let mutant_error = foreign_boundary_mutant
                .ring_side_faces(&subdivision, &slots_by_edge, &owner_zero.shell)
                .expect_err("foreign owner ring edges must not be retained as barriers");
            assert!(
                mutant_error
                    .to_string()
                    .contains("nonconstant left or right face"),
                "foreign-boundary mutant failed unexpectedly: {mutant_error}"
            );
        }
    }
}

#[test]
fn reversed_owner_local_host_face_is_rejected_by_exact_shell_membership() {
    let sites = sites_xy(&[(-10.0, -10.0), (10.0, -10.0), (0.0, 10.0)]);
    let clip = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
        -1.0, -1.0, 1.0, 1.0,
    )]);
    let subdivision = grafted_clip_subdivision(&sites, &clip);
    let parts = assemble_cells(&subdivision).unwrap();
    assert_eq!(parts.len(), 3);
    assert_owner_parts_match_exact_point_oracle(&subdivision, &parts);

    let part = parts
        .iter()
        .find(|part| !part.holes.is_empty())
        .expect("the producer fixture has one hole-bearing owner part");
    let owner_rings: Vec<_> = std::iter::once(&part.shell)
        .chain(part.holes.iter())
        .collect();
    let cycle_rings: Vec<_> = (0..owner_rings.len()).collect();
    let slots_by_edge = assembly::inverse_edge_slots(&subdivision);
    let reversed = assembly::OwnerLocalFaces::reversed_host_cycle_facts(
        &subdivision,
        &owner_rings,
        &slots_by_edge,
        &cycle_rings,
        &part.shell,
        &part.holes,
    );
    let rejected = match reversed {
        Err(error) => {
            assert!(
                error.to_string().contains("owner-local"),
                "reversed host face failed for an unrelated reason: {error}"
            );
            true
        },
        Ok(facts) => validate_owner_cycle_facts(&facts, owner_rings.len(), 0).is_err(),
    };
    assert!(
        rejected,
        "choosing the west face of a leftward-ray hit must not satisfy the exact shell oracle"
    );
}

#[test]
fn owner_local_work_scales_with_selected_ring_edges_and_keeps_no_hole_fast_path() {
    let sites = sites_xy(&[(1000.0, 5.0), (1000.0, 1_000_000.0), (1_000_000.0, 5.0)]);
    for hole_count in [128_usize, 256, 512] {
        let holes = (0..hole_count)
            .map(|index| {
                let ordinal =
                    f64::from(u32::try_from(index).expect("the fixture has fewer than u32 holes"));
                let x = 1.0 + ordinal * 1.4;
                closed_ring(&[(x, 5.0), (x + 0.25, 4.5), (x - 0.25, 4.5)])
            })
            .collect();
        let clip = Polygon::new(rectangle(0.0, 0.0, 2000.0, 10.0), holes);
        let subdivision = grafted_clip_subdivision(&sites, &clip);
        assembly::reset_owner_local_work();
        let (parts, allocations) =
            crate::test_support::allocation_count(|| assemble_cells(&subdivision).unwrap());
        let work = assembly::owner_local_work();
        eprintln!(
            "owner-local scaling holes={hole_count} allocations={allocations} \
             ring_edges={} global_slot_visits={} vertices={} faces={} components={} \
             queries={} hits={} axis_comparisons={} orientations={}",
            work.owner_ring_edges,
            work.global_edge_slot_visits,
            work.owner_vertices,
            work.face_walks,
            work.components,
            work.host_queries,
            work.ray_hits,
            work.ray_axis_comparisons,
            work.ray_orientations,
        );

        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].holes.len(), hole_count);
        assert_eq!(work.owner_ring_edges, 4 + 3 * hole_count);
        assert_eq!(work.owner_vertices, work.owner_ring_edges);
        assert_eq!(work.host_queries, hole_count + 1);
        assert_eq!(work.ray_hits, hole_count);
        assert_eq!(work.ring_side_checks, hole_count + 1);
        assert_eq!(work.global_edge_slot_visits, subdivision.edge_of_slot.len());
        assert!(work.global_edge_slot_visits <= work.owner_ring_edges * 2);
        assert!(work.face_walks <= work.owner_ring_edges + work.components);
        assert!(work.ray_axis_comparisons < work.owner_ring_edges * 256);
        assert!(work.ray_orientations < work.owner_ring_edges * 256);
    }

    let clip = Polygon::new(rectangle(0.0, 0.0, 2000.0, 10.0), Vec::new());
    let subdivision = grafted_clip_subdivision(&sites, &clip);
    assembly::reset_owner_local_work();
    let parts = assemble_cells(&subdivision).unwrap();
    let work = assembly::owner_local_work();
    assert_eq!(parts.len(), 1);
    assert_eq!(work, assembly::OwnerLocalWork::default());
}

#[test]
fn two_isolated_holes_in_split_same_owner_cell_pair_by_containment() {
    let sites: Vec<_> = [(-10.0, -10.0), (0.0, 10.0), (10.0, -10.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![
        rectangle(-12.0, -1.0, 12.0, 1.0),
        rectangle(-8.0, 6.0, -6.0, 8.0),
        rectangle(-1.2, -1.7, -0.6, -1.2),
    ]);
    let subdivision = grafted_clip_subdivision(&sites, &clip_polygon);
    assert_boundary_walk_euler(&subdivision);
    assert!(
        first_equal_label_misattaches_a_hole(&subdivision),
        "first equal-label attachment must pick a non-containing shell on this split same-owner diagram"
    );
    let restored = assemble_cells(&subdivision).unwrap();
    let top: Vec<_> = restored
        .iter()
        .filter(|part| part.owner == SiteId(1))
        .collect();
    assert_eq!(top.len(), 2);
    assert_eq!(top.iter().map(|part| part.holes.len()).sum::<usize>(), 2);
    assert!(top.iter().all(|part| part.holes.len() == 1));
}

#[test]
fn validate_rejects_a_mutated_twin_walk_label() {
    let sites: Vec<_> = [(-10.0, -10.0), (0.0, 10.0), (10.0, -10.0)]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site::raw(id, Point::new_raw_xy(x, y), id))
        .collect();
    let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
        -1.0, -1.0, 1.0, 1.0,
    )]);
    let mut subdivision = grafted_clip_subdivision(&sites, &clip_polygon);
    validate_exact_subdivision(&subdivision).unwrap();
    let slot = (0..subdivision.topology.targets.len())
        .find(|&slot| {
            let left = walk_label(
                &subdivision,
                BoundaryFaceId(subdivision.topology.face_of[slot]),
            );
            selected_cell_label(left)
        })
        .expect("selected walk has a slot");
    let face = subdivision.topology.face_of[slot] as usize;
    subdivision.labels[face] = RegionLabel {
        frame_winding: 0,
        clip_winding: 0,
        owner: None,
    };
    assert!(validate_exact_subdivision(&subdivision).is_err());
}
