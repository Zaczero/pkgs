#[test]
fn box_index_keeps_ordinal_and_bounded_count_limits_distinct() {
    let full_range = (u32::MAX as usize).wrapping_add(1);
    assert_eq!(
        admit_box_ordinals(full_range).map_err(|error| error.to_string()),
        Ok(())
    );

    assert_eq!(
        checked_index_count(u32::MAX as usize).map_err(|error| error.to_string()),
        Ok(u32::MAX)
    );
    assert_eq!(
        checked_index_count(full_range).map_err(|error| error.to_string()),
        Err("resource size arithmetic overflowed".to_owned())
    );

    let too_many = full_range.wrapping_add(1);
    assert_eq!(
        admit_box_ordinals(too_many).map_err(|error| error.to_string()),
        Err("resource size arithmetic overflowed".to_owned())
    );
}

fn cycle_xy(points: &[(f64, f64)]) -> Vec<XY> {
    points.iter().map(|&(x, y)| XY::new(x, y)).collect()
}

fn rounded_clip_segment_certificate(
    segment: Segment,
    exact_rings: &[Vec<exact::ExactPoint>],
    rounded_rings: &[Vec<XY>],
    events: &mut Vec<exact::ExactPoint>,
    parity: &mut ClipRingParity,
) -> Result<bool> {
    let mut budget = EvaluationBudget::new("rounded clip segment test", 0)?;
    let mut clip_edges = Vec::new();
    for (ring_index, (exact_ring, rounded_ring)) in
        exact_rings.iter().zip(rounded_rings).enumerate()
    {
        for ((start, end), (rounded_start, rounded_end)) in exact_ring
            .iter()
            .zip(exact_ring.iter().cycle().skip(1))
            .zip(rounded_ring.iter().zip(rounded_ring.iter().cycle().skip(1)))
        {
            clip_edges.push(ClipCertificateEdge {
                ring: ring_index,
                atom: EdgeId(
                    u32::try_from(clip_edges.len())
                        .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                ),
                start: start.clone(),
                end: end.clone(),
                rounded: [*rounded_start, *rounded_end],
            });
        }
    }
    let polygon = Polygon::new(
        Ring::closed(
            rounded_rings[0]
                .iter()
                .map(|point| Point::new_raw_xy(point.x, point.y))
                .collect(),
        )?,
        rounded_rings[1..]
            .iter()
            .map(|ring| {
                Ring::closed(
                    ring.iter()
                        .map(|point| Point::new_raw_xy(point.x, point.y))
                        .collect(),
                )
            })
            .collect::<Result<Vec<_>>>()?,
    );
    let point_index = PolygonPointIndex::build(&polygon)?;
    let edge_ids: Vec<_> = (0..clip_edges.len()).collect();
    let y_index = YStabbingIndex::build(edge_ids, |&edge| {
        let [start, end] = clip_edges[edge].rounded;
        (
            start.x.min(end.x),
            start.x.max(end.x),
            start.y.min(end.y),
            start.y.max(end.y),
        )
    })?;
    let candidates: Vec<_> = (0..clip_edges.len()).collect();
    rounded_segment_covered_by_clip(
        segment,
        &clip_edges,
        &candidates,
        &point_index,
        &y_index,
        events,
        parity,
        &mut budget,
    )
    .map(|failure| failure.is_none())
}

#[test]
fn rounded_segment_certificate_matches_closed_clip_membership() {
    let rectangle_points = [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)];
    let hole_points = [(1.0, 1.0), (3.0, 1.0), (3.0, 3.0), (1.0, 3.0)];
    let rectangle = cycle(&rectangle_points);
    let rectangle_xy = cycle_xy(&rectangle_points);
    let hole = cycle(&hole_points);
    let hole_xy = cycle_xy(&hole_points);
    let mut events = Vec::new();
    let mut parity = ClipRingParity::new(2);
    assert!(
        rounded_clip_segment_certificate(
            Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(4.0, 0.0),
            },
            std::slice::from_ref(&rectangle),
            std::slice::from_ref(&rectangle_xy),
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    assert!(
        rounded_clip_segment_certificate(
            Segment {
                start: XY::new(0.5, 0.5),
                end: XY::new(3.5, 3.5),
            },
            std::slice::from_ref(&rectangle),
            std::slice::from_ref(&rectangle_xy),
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    assert!(
        !rounded_clip_segment_certificate(
            Segment {
                start: XY::new(0.5, 2.0),
                end: XY::new(3.5, 2.0),
            },
            &[rectangle.clone(), hole.clone()],
            &[rectangle_xy.clone(), hole_xy.clone()],
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    assert!(
        rounded_clip_segment_certificate(
            Segment {
                start: XY::new(1.0, 1.0),
                end: XY::new(3.0, 1.0),
            },
            &[rectangle.clone(), hole],
            &[rectangle_xy.clone(), hole_xy],
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    let concave_points = [
        (0.0, 0.0),
        (4.0, 0.0),
        (4.0, 4.0),
        (3.0, 4.0),
        (3.0, 1.0),
        (1.0, 1.0),
        (1.0, 4.0),
        (0.0, 4.0),
    ];
    let concave = cycle(&concave_points);
    let concave_xy = cycle_xy(&concave_points);
    assert!(
        !rounded_clip_segment_certificate(
            Segment {
                start: XY::new(0.5, 3.5),
                end: XY::new(3.5, 3.5),
            },
            &[concave],
            &[concave_xy],
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    let l_shape_points = [
        (0.0, 0.0),
        (4.0, 0.0),
        (4.0, 1.0),
        (1.0, 1.0),
        (1.0, 4.0),
        (0.0, 4.0),
    ];
    let l_shape = cycle(&l_shape_points);
    let l_shape_xy = cycle_xy(&l_shape_points);
    assert!(
        !rounded_clip_segment_certificate(
            Segment {
                start: XY::new(4.0, 1.0),
                end: XY::new(1.0, 4.0),
            },
            &[l_shape],
            &[l_shape_xy],
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
    let slanted_edge_points = [
        (2.949_570_888_116_326e-6, 1.769_742_532_869_795_6e-6),
        (3.097_351_995_393_694_8e-6, 1.858_411_197_236_216_9e-6),
        (3.0e-6, 2.5e-6),
    ];
    let slanted_edge = cycle(&slanted_edge_points);
    let slanted_edge_xy = cycle_xy(&slanted_edge_points);
    let slanted_segment = Segment {
        start: XY::new(2.949_570_888_116_328e-6, 1.769_742_532_869_796_8e-6),
        end: XY::new(3.097_351_995_393_692_7e-6, 1.858_411_197_236_215_6e-6),
    };
    let slanted_midpoint = XY::new(
        f64::midpoint(slanted_segment.start.x, slanted_segment.end.x),
        f64::midpoint(slanted_segment.start.y, slanted_segment.end.y),
    );
    assert_eq!(
        exact::orient2d(
            slanted_segment.start,
            slanted_segment.end,
            slanted_edge_xy[0],
        ),
        ExactSign::Zero
    );
    assert_eq!(
        exact::orient2d(
            slanted_segment.start,
            slanted_segment.end,
            slanted_edge_xy[1],
        ),
        ExactSign::Zero
    );
    assert_eq!(
        exact::orient2d(
            slanted_edge_xy[0],
            slanted_edge_xy[1],
            slanted_segment.start,
        ),
        ExactSign::Zero
    );
    assert_eq!(
        exact::orient2d(slanted_edge_xy[0], slanted_edge_xy[1], slanted_segment.end,),
        ExactSign::Zero
    );
    assert!(slanted_edge_xy[0].x < slanted_segment.start.x);
    assert!(slanted_segment.end.x < slanted_edge_xy[1].x);
    assert_ne!(
        exact::orient2d(slanted_segment.start, slanted_segment.end, slanted_midpoint,),
        ExactSign::Zero
    );
    let slanted_result = rounded_clip_segment_certificate(
        slanted_segment,
        &[slanted_edge],
        &[slanted_edge_xy],
        &mut events,
        &mut parity,
    )
    .unwrap();
    assert!(slanted_result);
    assert!(
        !rounded_clip_segment_certificate(
            Segment {
                start: XY::new(-1.0, 2.0),
                end: XY::new(2.0, 2.0),
            },
            &[rectangle],
            &[rectangle_xy],
            &mut events,
            &mut parity,
        )
        .unwrap()
    );
}

fn validates(cycles: &[Vec<exact::ExactPoint>]) -> bool {
    let refs: Vec<_> = cycles.iter().map(Vec::as_slice).collect();
    validate_exact_cycles(&refs, 0).is_ok()
}

fn noded_fixture(
    cycles: &[Vec<exact::ExactPoint>],
) -> Result<(ExactSubdivision, Vec<ExactSimpleRing>)> {
    let mut vertices: Vec<ExactVertex> = Vec::new();
    let mut edges = Vec::new();
    let mut rings = Vec::with_capacity(cycles.len());
    for (ring_index, cycle) in cycles.iter().enumerate() {
        let vertex_capacity = cycle
            .len()
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let mut ring_vertices = Vec::with_capacity(vertex_capacity);
        for point in cycle {
            let mut vertex = None;
            for (index, known) in vertices.iter().enumerate() {
                if known.point.same_position(point)? {
                    vertex = Some(VertexId(
                        u32::try_from(index)
                            .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                    ));
                    break;
                }
            }
            let vertex = if let Some(vertex) = vertex {
                vertex
            } else {
                let vertex = VertexId(
                    u32::try_from(vertices.len())
                        .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                );
                vertices.push(ExactVertex {
                    point: point.clone(),
                    fixed_xy: None,
                });
                vertex
            };
            ring_vertices.push(vertex);
        }
        let closing = *ring_vertices.first().ok_or_else(exact_cycle_error)?;
        ring_vertices.push(closing);
        let mut ring_edges = Vec::with_capacity(cycle.len());
        for pair in ring_vertices.windows(2) {
            let [start, end] = pair else {
                return Err(exact_cycle_error());
            };
            let edge = EdgeId(
                u32::try_from(edges.len())
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            );
            let endpoints = if start < end {
                [*start, *end]
            } else {
                [*end, *start]
            };
            edges.push(AtomicEdge {
                endpoints,
                forward: Transition {
                    frame_delta: 0,
                    clip_delta: 0,
                    owner: OwnerTransition::Preserve,
                },
                separator: None,
                clip_sources: Vec::new(),
                frame_side: None,
            });
            ring_edges.push(edge);
        }
        rings.push(ExactSimpleRing {
            face: BoundaryFaceId(
                u32::try_from(ring_index).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            ),
            vertices: ring_vertices,
            edges: ring_edges,
            orientation: ExactSign::Positive,
        });
    }
    Ok((
        ExactSubdivision {
            vertices,
            edges,
            topology: IndexedTopology::default(),
            edge_of_slot: Vec::new(),
            labels: Vec::new(),
        },
        rings,
    ))
}

#[test]
fn exact_cycle_oracle_accepts_a_finite_max_triangle() {
    let below = f64::MAX.next_down();
    assert!(validates(&[cycle(&[
        (below, below),
        (f64::MAX, below),
        (f64::MAX, f64::MAX)
    ])]));
}

#[test]
fn exact_rectangle_contact_validator_rejects_crossings_and_overlaps() {
    let rectangle = cycle(&[(0.0, 0.0), (4.0, 0.0), (4.0, 3.0), (0.0, 3.0)]);
    validate_exact_rectangle(rectangle.as_slice().try_into().unwrap()).unwrap();

    let crossed = cycle(&[(0.0, 0.0), (4.0, 3.0), (4.0, 0.0), (0.0, 3.0)]);
    assert!(validate_exact_rectangle(crossed.as_slice().try_into().unwrap()).is_err());

    let overlapping = cycle(&[(0.0, 0.0), (4.0, 0.0), (2.0, 0.0), (0.0, 3.0)]);
    assert!(validate_exact_rectangle(overlapping.as_slice().try_into().unwrap()).is_err());
}

#[test]
fn assembled_and_generic_validators_preserve_a_one_ulp_boundary_gap() {
    let inner_right = 1.0_f64.next_down();
    assert_eq!(inner_right.next_up(), 1.0);
    let cycles = [
        cycle(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]),
        cycle(&[
            (0.5, 0.25),
            (inner_right, 0.25),
            (inner_right, 0.75),
            (0.5, 0.75),
        ]),
    ];
    let (subdivision, rings) = noded_fixture(&cycles).unwrap();
    let noded_cycles: Vec<_> = cycles
        .iter()
        .zip(&rings)
        .map(|(points, ring)| NodedExactCycle { points, ring })
        .collect();
    let point_cycles: Vec<_> = cycles.iter().map(Vec::as_slice).collect();

    validate_assembled_exact_cycles(&subdivision, &noded_cycles, 0, || {
        validate_exact_cycle_nesting(&point_cycles, 0)
    })
    .unwrap();
    let generic = validate_exact_cycles(&point_cycles, 0).unwrap();
    assert!(generic.contacts.is_empty());

    let index = NodedSegmentIndex::new(&noded_cycles).unwrap();
    let mut endpoint_pairs = 0_usize;
    for_each_assembled_pair(&point_cycles, &index, |_, contact| {
        endpoint_pairs = endpoint_pairs.checked_add(1).unwrap();
        assert!(contact.is_none());
        Ok(())
    })
    .unwrap();
    assert_eq!(endpoint_pairs, 8);
    assert_eq!(
        for_each_touch_fan(&index, |_, _, _, _, _| Ok(())).unwrap(),
        0
    );
}

#[test]
fn arbitrary_exact_cycle_oracle_keeps_crossing_t_and_overlap_rejections() {
    let make = |points: &[(f64, f64)]| cycle(points);
    let square = make(&[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0)]);
    let crossing_diamond = make(&[(4.0, -2.0), (10.0, 4.0), (4.0, 10.0), (-2.0, 4.0)]);
    assert!(!validates(&[square.clone(), crossing_diamond]));

    let bowtie = make(&[(0.0, 0.0), (4.0, 4.0), (0.0, 4.0), (4.0, 0.0)]);
    assert!(!validates(&[bowtie]));

    let one_t = make(&[(0.0, 4.0), (2.0, 5.0), (2.0, 3.0)]);
    assert!(validates(&[square.clone(), one_t.clone()]));
    let noded_square = make(&[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0), (0.0, 4.0)]);
    assert!(validates(&[noded_square, one_t]));

    let two_t = make(&[(0.0, 2.0), (2.0, 4.0), (0.0, 6.0)]);
    assert!(!validates(&[square, two_t]));

    let overlap_shell = make(&[(-2.0, -2.0), (2.0, -2.0), (2.0, 2.0), (-2.0, 2.0)]);
    let unshared_overlap = make(&[(-1.0, -2.0), (1.0, -2.0), (1.0, -3.0), (-1.0, -3.0)]);
    assert!(!validates(&[overlap_shell, unshared_overlap]));

    let shared_overlap_shell = make(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)]);
    let shared_overlap = make(&[(0.0, 0.0), (2.0, 0.0), (2.0, -2.0), (0.0, -2.0)]);
    assert!(!validates(&[shared_overlap_shell, shared_overlap]));

    let adjacent_overlap = make(&[(0.0, 0.0), (4.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0)]);
    assert!(!validates(&[adjacent_overlap]));
}

#[test]
fn embedded_clip_admission_rejects_crossed_polygon() {
    let ring = Ring::closed(vec![
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(2.0, 2.0),
        Point::new_raw_xy(0.0, 2.0),
        Point::new_raw_xy(2.0, 0.0),
        Point::new_raw_xy(0.0, 0.0),
    ])
    .unwrap();
    assert!(admit_embedded_clip(&Polygon::new(ring, Vec::new())).is_err());
}

fn reversed_cycles(cycles: &[Vec<exact::ExactPoint>]) -> Vec<Vec<exact::ExactPoint>> {
    std::iter::once(&cycles[0])
        .chain(cycles[1..].iter().rev())
        .map(|cycle| cycle.iter().cloned().rev().collect())
        .collect()
}

#[test]
fn assembled_touch_fans_match_generic_exact_contacts_in_source_order() {
    let shell = cycle(&[(-10.0, -10.0), (10.0, -10.0), (10.0, 10.0), (-10.0, 10.0)]);
    let holes = [
        cycle(&[(0.0, 0.0), (4.0, -1.0), (4.0, 1.0)]),
        cycle(&[(0.0, 0.0), (1.0, 4.0), (-1.0, 4.0)]),
        cycle(&[(0.0, 0.0), (-4.0, 1.0), (-4.0, -1.0)]),
        cycle(&[(0.0, 0.0), (-1.0, -4.0), (1.0, -4.0)]),
    ];
    let cycles = std::iter::once(shell).chain(holes).collect::<Vec<_>>();
    let (subdivision, rings) = noded_fixture(&cycles).unwrap();
    let noded_cycles: Vec<_> = cycles
        .iter()
        .zip(&rings)
        .map(|(points, ring)| NodedExactCycle { points, ring })
        .collect();
    let point_cycles: Vec<_> = cycles.iter().map(Vec::as_slice).collect();
    validate_assembled_exact_cycles(&subdivision, &noded_cycles, 0, || {
        validate_exact_cycle_nesting(&point_cycles, 0)
    })
    .unwrap();
    let expected = validate_exact_cycles(&point_cycles, 0).unwrap().contacts;
    let index = NodedSegmentIndex::new(&noded_cycles).unwrap();
    let mut actual = Vec::new();
    for_each_assembled_pair(&point_cycles, &index, |segments, contact| {
        if let Some(contact) = contact {
            assert_eq!(contact.segments, segments);
            actual.push(contact);
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(actual.len(), 24);
    assert_exact_contacts_equal(&actual, &expected);
    assert!(
        actual
            .array_windows::<2>()
            .all(|[left, right]| left.segments < right.segments)
    );
}

#[test]
fn exact_cycle_validator_accepts_one_shell_hole_tangency() {
    let shell = cycle(&[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0)]);
    let hole = cycle(&[(0.0, 4.0), (2.0, 5.0), (2.0, 3.0)]);
    assert!(validates(&[shell, hole]));
}

#[test]
fn exact_cycle_validator_rejects_two_shell_hole_contacts() {
    let shell = cycle(&[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0)]);
    let hole = cycle(&[(0.0, 2.0), (2.0, 4.0), (0.0, 6.0)]);
    assert!(!validates(&[shell, hole]));
}

#[test]
fn exact_cycle_validator_rejects_crossed_ring() {
    let crossed = cycle(&[(0.0, 0.0), (4.0, 4.0), (0.0, 4.0), (4.0, 0.0)]);
    assert!(!validates(&[crossed]));
}

#[test]
fn exact_touch_graph_covers_all_five_outcomes_and_reversed_discovery() {
    let shell = cycle(&[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0)]);
    let one_shell_touch = vec![shell.clone(), cycle(&[(0.0, 4.0), (2.0, 5.0), (2.0, 3.0)])];
    let two_shell_touches = vec![shell.clone(), cycle(&[(0.0, 2.0), (2.0, 4.0), (0.0, 6.0)])];
    let two_holes_touch_once = vec![
        shell.clone(),
        cycle(&[(2.0, 2.0), (4.0, 2.0), (4.0, 4.0), (2.0, 4.0)]),
        cycle(&[(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0)]),
    ];
    let four_hole_star = vec![
        cycle(&[(-10.0, -10.0), (10.0, -10.0), (10.0, 10.0), (-10.0, 10.0)]),
        cycle(&[(0.0, 0.0), (4.0, -1.0), (4.0, 1.0)]),
        cycle(&[(0.0, 0.0), (1.0, 4.0), (-1.0, 4.0)]),
        cycle(&[(0.0, 0.0), (-4.0, 1.0), (-4.0, -1.0)]),
        cycle(&[(0.0, 0.0), (-1.0, -4.0), (1.0, -4.0)]),
    ];
    let interleaved = vec![
        shell,
        cycle(&[(2.0, 2.0), (6.0, 2.0), (6.0, 6.0), (2.0, 6.0)]),
        cycle(&[(4.0, 0.0), (8.0, 4.0), (4.0, 8.0), (0.0, 4.0)]),
    ];
    let cases = [
        (one_shell_touch, true),
        (two_shell_touches, false),
        (two_holes_touch_once, true),
        (four_hole_star, true),
        (interleaved, false),
    ];
    for (cycles, expected) in cases {
        let restored = validates(&cycles);
        let reversed = validates(&reversed_cycles(&cycles));
        assert_eq!(restored, expected);
        assert_eq!(reversed, restored);
    }
}

#[test]
fn assembled_contact_validation_precedes_owner_local_containment() {
    let shell = cycle(&[
        (0.0, 0.0),
        (8.0, 0.0),
        (8.0, 8.0),
        (0.0, 8.0),
        (0.0, 6.0),
        (0.0, 2.0),
    ]);
    let twice_touching_hole = cycle(&[(0.0, 2.0), (2.0, 4.0), (0.0, 6.0)]);
    let invalid_cycles = [shell.clone(), twice_touching_hole];
    let (invalid_subdivision, invalid_rings) = noded_fixture(&invalid_cycles).unwrap();
    let invalid_noded: Vec<_> = invalid_cycles
        .iter()
        .zip(&invalid_rings)
        .map(|(points, ring)| NodedExactCycle { points, ring })
        .collect();
    let invalid_touch_facts = [
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: false,
        },
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: false,
        },
    ];
    let invalid_nesting_called = std::cell::Cell::new(false);
    let Err(invalid_facts) =
        validate_assembled_exact_cycles(&invalid_subdivision, &invalid_noded, 0, || {
            invalid_nesting_called.set(true);
            validate_owner_cycle_facts(&invalid_touch_facts, invalid_noded.len(), 0)
        })
    else {
        panic!("the exact touch graph admitted two shell contacts")
    };
    assert!(
        !invalid_nesting_called.get(),
        "contact validation must finish before owner-local containment runs"
    );
    assert!(
        invalid_facts
            .to_string()
            .contains("exact Voronoi cycle set is not a valid polygon boundary"),
        "the exact touch graph must reject before owner-local containment facts: {invalid_facts}"
    );

    let contained_hole = cycle(&[(2.0, 2.0), (6.0, 2.0), (6.0, 6.0), (2.0, 6.0)]);
    let valid_cycles = [shell, contained_hole];
    let (valid_subdivision, valid_rings) = noded_fixture(&valid_cycles).unwrap();
    let valid_noded: Vec<_> = valid_cycles
        .iter()
        .zip(&valid_rings)
        .map(|(points, ring)| NodedExactCycle { points, ring })
        .collect();
    let valid_facts = [
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: false,
        },
        OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: false,
        },
    ];
    let valid_nesting_called = std::cell::Cell::new(false);
    validate_assembled_exact_cycles(&valid_subdivision, &valid_noded, 0, || {
        valid_nesting_called.set(true);
        validate_owner_cycle_facts(&valid_facts, valid_noded.len(), 0)
    })
    .unwrap();
    assert!(valid_nesting_called.get());
}

fn old_frame_halfplane_holds(edge: &AtomicEdge, vertices: &[NodedVertex], sites: &[Site]) -> bool {
    let OwnerTransition::Boundary { left, right } = edge.forward.owner else {
        return true;
    };
    let start = &vertices[edge.endpoints[0].0 as usize].point;
    let end = &vertices[edge.endpoints[1].0 as usize].point;
    left.is_none_or(|site| {
        exact::incidence::orient_points(
            start,
            end,
            &exact::ExactPoint::from_xy(sites[site.0 as usize].point.xy()),
        )
        .unwrap()
            == ExactSign::Positive
    }) && right.is_none_or(|site| {
        exact::incidence::orient_points(
            start,
            end,
            &exact::ExactPoint::from_xy(sites[site.0 as usize].point.xy()),
        )
        .unwrap()
            == ExactSign::Negative
    })
}

#[test]
fn public_polygon_admits_sites_on_and_outside_clip_aabb() {
    let sites = sites_xy(&[(0.0, 0.0), (1.0, 0.0), (3.0, 1.0), (1.0, 1.0)]);
    let polygon = Polygon::new(rectangle(0.0, 0.0, 2.0, 2.0), Vec::new());
    let mut budget = ExpansionBudget::new("test", "sites on and outside R");
    let atoms = node_polygon(&sites, &polygon);
    assert!(
        atoms.edges.iter().any(|edge| {
            edge.separator.is_none()
                && edge.frame_side.is_some()
                && !old_frame_halfplane_holds(edge, &atoms.vertices, &sites)
        }),
        "old frame halfplane still holds on every frame-only atom"
    );
    build(
        &sites,
        VoronoiBoundary::Polygon(&polygon),
        &mut budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap();
}

#[test]
fn subnormal_clip_aabb_exact_lane_admits_sites_on_corners() {
    let step = f64::from_bits(4);
    let sites = sites_xy(&[(0.0, 0.0), (step, 0.0), (0.0, step)]);
    let mut envelope_budget = ExpansionBudget::new("test", "units4 envelope");
    build(
        &sites,
        VoronoiBoundary::Envelope,
        &mut envelope_budget,
        false,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap();
    let polygon = Polygon::new(rectangle(0.0, 0.0, step, step), Vec::new());
    let mut polygon_budget = ExpansionBudget::new("test", "units4 polygon");
    build(
        &sites,
        VoronoiBoundary::Polygon(&polygon),
        &mut polygon_budget,
        true,
        &certified_delaunay(&sites).unwrap(),
    )
    .unwrap();
}

#[test]
fn semantic_admission_mutants_change_validity_overlap_gap_and_spill() {
    let shape = |min_x, min_y, max_x, max_y| {
        Shape::Polygon(Polygon::new(
            rectangle(min_x, min_y, max_x, max_y),
            Vec::new(),
        ))
    };
    let clip = shape(0.0, 0.0, 4.0, 4.0);
    let restored_rows = vec![shape(0.0, 0.0, 2.0, 4.0), shape(2.0, 0.0, 4.0, 4.0)];
    let mutated_overlap = vec![clip.clone(), shape(0.0, 0.0, 2.0, 4.0)];
    let restored_coverage = coverage_is_valid(&restored_rows, 0.0).unwrap();
    let mutated_coverage = coverage_is_valid(&mutated_overlap, 0.0).unwrap();
    assert_ne!(
        mutated_coverage, restored_coverage,
        "mutated overlap={mutated_coverage}, restored partition={restored_coverage}"
    );
    assert!(!mutated_coverage && restored_coverage);

    let restored_union = Shape::union_all(&restored_rows, Strictness::Lenient).unwrap();
    let mutated_gap_union = Shape::union_all(
        &[shape(0.0, 0.0, 1.9, 4.0), shape(2.1, 0.0, 4.0, 4.0)],
        Strictness::Lenient,
    )
    .unwrap();
    let restored_gap = !clip
        .difference(&restored_union, Strictness::Lenient)
        .unwrap()
        .is_empty();
    let mutated_gap = !clip
        .difference(&mutated_gap_union, Strictness::Lenient)
        .unwrap()
        .is_empty();
    assert_ne!(
        mutated_gap, restored_gap,
        "mutated gap={mutated_gap}, restored gap={restored_gap}"
    );

    let mutated_spill_union = Shape::union_all(
        &[clip.clone(), shape(5.0, 0.0, 6.0, 1.0)],
        Strictness::Lenient,
    )
    .unwrap();
    let restored_spill = !restored_union
        .difference(&clip, Strictness::Lenient)
        .unwrap()
        .is_empty();
    let mutated_spill = !mutated_spill_union
        .difference(&clip, Strictness::Lenient)
        .unwrap()
        .is_empty();
    assert_ne!(
        mutated_spill, restored_spill,
        "mutated spill={mutated_spill}, restored spill={restored_spill}"
    );

    let restored_polygon = Polygon::new(rectangle(0.0, 0.0, 4.0, 4.0), Vec::new());
    let mutated_polygon = Polygon::new(
        Ring::closed(
            [(0.0, 0.0), (4.0, 4.0), (0.0, 4.0), (4.0, 0.0), (0.0, 0.0)]
                .into_iter()
                .map(|(x, y)| Point::new_raw_xy(x, y))
                .collect(),
        )
        .unwrap(),
        Vec::new(),
    );
    let restored_valid = polygon_is_valid(&restored_polygon).unwrap();
    let mutated_valid = polygon_is_valid(&mutated_polygon).unwrap();
    assert_ne!(
        mutated_valid, restored_valid,
        "mutated valid={mutated_valid}, restored valid={restored_valid}"
    );
}

#[test]
fn transition_and_budget_mutants_report_distinct_restored_values() {
    let left = RegionLabel {
        frame_winding: 1,
        clip_winding: 1,
        owner: Some(SiteId(0)),
    };
    let transition = Transition {
        frame_delta: 0,
        clip_delta: 1,
        owner: OwnerTransition::Boundary {
            left: Some(SiteId(0)),
            right: Some(SiteId(1)),
        },
    };
    let restored = crossed_label(left, transition).unwrap();
    let mutated = crossed_label(left, Transition {
        clip_delta: 0,
        owner: OwnerTransition::Preserve,
        ..transition
    })
    .unwrap();
    assert_ne!(
        mutated, restored,
        "mutated transition={mutated:?}, restored transition={restored:?}"
    );
    assert_eq!(restored.clip_winding, 0);
    assert_eq!(restored.owner, Some(SiteId(1)));

    let inherited = left;
    let zero_seeded = RegionLabel {
        frame_winding: 0,
        clip_winding: 0,
        owner: None,
    };
    assert_ne!(
        zero_seeded, inherited,
        "mutated inherited label={zero_seeded:?}, restored inherited label={inherited:?}"
    );

    let exact = ExpansionBudget::check("test", "work", GENERATED_ITEM_LIMIT).unwrap();
    let plus_one_rejected =
        ExpansionBudget::check("test", "work", GENERATED_ITEM_LIMIT + 1).is_err();
    assert_ne!(
        plus_one_rejected,
        exact != GENERATED_ITEM_LIMIT,
        "limit+1 rejected={plus_one_rejected}, exact admitted={exact}"
    );
    let finite_product = ExpansionBudget::product("test", "work", 4, 4).unwrap();
    let overflow_rejected = ExpansionBudget::product("test", "work", usize::MAX, 2).is_err();
    assert_ne!(
        overflow_rejected,
        finite_product != 16,
        "overflow rejected={overflow_rejected}, restored product={finite_product}"
    );
}
