#[expect(
    clippy::arithmetic_side_effects,
    reason = "ExactSimpleRing carries the repeated closing vertex admitted by simple_ring"
)]
fn exact_ring_points(
    subdivision: &ExactSubdivision,
    ring: &assembly::ExactSimpleRing,
) -> Vec<exact::ExactPoint> {
    ring.vertices[..ring.vertices.len() - 1]
        .iter()
        .map(|&vertex| subdivision.vertex(vertex).point.clone())
        .collect()
}

fn exact_ring_coordinate_key(
    subdivision: &ExactSubdivision,
    ring: &assembly::ExactSimpleRing,
) -> RingCoordinateKey {
    let (_, open_vertices) = ring
        .vertices
        .split_last()
        .expect("simple exact rings are closed");
    let mut coordinates: Vec<_> = open_vertices
        .iter()
        .map(|&vertex| {
            let point = subdivision
                .vertex(vertex)
                .point
                .round_nearest_even()
                .unwrap();
            (point.x.to_bits(), point.y.to_bits())
        })
        .collect();
    coordinates.sort_unstable();
    coordinates
}

fn assert_owner_parts_match_exact_point_oracle(
    subdivision: &ExactSubdivision,
    parts: &[ExactCellPart],
) {
    for part in parts {
        let shell = exact_ring_points(subdivision, &part.shell);
        let holes: Vec<_> = part
            .holes
            .iter()
            .map(|hole| exact_ring_points(subdivision, hole))
            .collect();
        assert_holes_inside_exact_shell(&shell, &holes);
        let mut remaining = holes.as_slice();
        while let Some((left, later)) = remaining.split_first() {
            for right in later {
                assert!(
                    right.iter().all(|point| {
                        exact::point_in_cycle(left, point).unwrap() != exact::PointInCycle::Inside
                    }),
                    "one emitted hole is nested inside another"
                );
                assert!(
                    left.iter().all(|point| {
                        exact::point_in_cycle(right, point).unwrap() != exact::PointInCycle::Inside
                    }),
                    "one emitted hole is nested inside another"
                );
            }
            remaining = later;
        }
    }
}

fn assert_holes_inside_exact_shell(shell: &[exact::ExactPoint], holes: &[Vec<exact::ExactPoint>]) {
    for hole in holes {
        let mut saw_inside = false;
        for point in hole {
            match exact::point_in_cycle(shell, point).unwrap() {
                exact::PointInCycle::Outside => {
                    panic!("owner-local shell does not contain its emitted hole")
                },
                exact::PointInCycle::Boundary => {},
                exact::PointInCycle::Inside => saw_inside = true,
            }
        }
        assert!(saw_inside, "a boundary-only ring is not a hole");
    }
}

#[test]
fn owner_ray_sort_and_sweep_use_exact_order_when_a_hint_is_unavailable() {
    let zero = exact::ExactPoint::from_xy(XY::new(0.0, 0.0));
    let shallow = exact::ExactPoint::from_xy(XY::new(1.0, f64::from_bits(1)));
    let one = exact::ExactPoint::from_xy(XY::new(0.0, 1.0));
    let one_next = exact::ExactPoint::from_xy(XY::new(1.0, 1.0));
    let high = exact::line_intersection(
        &exact::ExactLine::through_points(&zero, &shallow).unwrap(),
        &exact::ExactLine::through_points(&one, &one_next).unwrap(),
    )
    .unwrap();
    assert!(
        high.finite_rounding().unwrap().is_none(),
        "the exact intersection lies above the finite binary64 range"
    );

    let exact_vertices = [
        ExactVertex {
            point: high,
            fixed_xy: None,
        },
        ExactVertex {
            point: exact::ExactPoint::from_xy(XY::new(2.0, 0.0)),
            fixed_xy: None,
        },
        ExactVertex {
            point: exact::ExactPoint::from_xy(XY::new(-2.0, 0.0)),
            fixed_xy: None,
        },
    ];
    let global_vertices = [VertexId(0), VertexId(1), VertexId(2)];
    let points = assembly::OwnerRayPoints::new(&exact_vertices, &global_vertices);
    assert_eq!(
        [0_u32, 1, 2].map(|vertex| RayPoints::rounded(&points, vertex)),
        [XY::new(0.0, 0.0); 3],
        "one unavailable hint makes the whole key table constant"
    );

    let mut order = [0_u32, 1, 2];
    RayPoints::sort_by_axis(
        &points,
        &mut order,
        RayAxis::X,
        |vertex| vertex,
        |_, _| Ok(()),
    )
    .unwrap();
    assert_eq!(order, [2, 1, 0]);
    assert_eq!(
        shoot_left(&points, 1, |_| [0, 2], &[1]).unwrap(),
        [Some(0)],
        "constant hints must leave exact leftward-ray admission active"
    );
}

#[test]
fn owner_ray_sort_breaks_nearest_even_hint_ties_with_exact_coordinates() {
    let lower = exact::ExactPoint::from_xy(XY::new(1.0, 0.0));
    let upper = exact::ExactPoint::from_xy(XY::new(1.0_f64.next_up(), 0.0));
    let midpoint = lower.midpoint(&upper).unwrap();
    assert_ne!(lower.compare_x(&midpoint).unwrap(), Ordering::Equal);
    assert_eq!(
        lower.round_nearest_even().unwrap(),
        midpoint.round_nearest_even().unwrap()
    );

    let vertices = [
        ExactVertex {
            point: midpoint,
            fixed_xy: None,
        },
        ExactVertex {
            point: upper,
            fixed_xy: None,
        },
        ExactVertex {
            point: lower,
            fixed_xy: None,
        },
    ];
    let ids = [VertexId(0), VertexId(1), VertexId(2)];
    let points = assembly::OwnerRayPoints::new(&vertices, &ids);
    let mut order = [0_u32, 1, 2];
    RayPoints::sort_by_axis(
        &points,
        &mut order,
        RayAxis::X,
        |vertex| vertex,
        |_, _| Ok(()),
    )
    .unwrap();
    assert_eq!(order, [2, 0, 1]);
}
