fn clean_ring_atom_labels(
    left: &Shape,
    right: &Shape,
    ring_operand: matrix_predicates::Operand,
    representative: XY,
) -> Result<(Loc, Loc)> {
    let [left_row, right_row] = [ShapeRow::Shape(left), ShapeRow::Shape(right)];
    let (ring_row, other_row) = match ring_operand {
        matrix_predicates::Operand::Left => (left_row, right_row),
        matrix_predicates::Operand::Right => (right_row, left_row),
    };
    let rows = [left_row, right_row];
    let sources = StagedExactSources::from_rows_for_relation(rows.into_iter())?;
    let mut staged = stage_components(sources, &rows)?;
    let segments = staged.sources.segments();
    let ring_owner = Operand::from_row(ring_row);
    let other_owner = Operand::from_row(other_row);
    for component in &mut staged.components {
        if component.side != ring_operand || component.source.ring_kind != Some(RingKind::Winding) {
            continue;
        }
        let Some(segment) = segments.get(component.source.segments.start).copied() else {
            continue;
        };
        if PointKey::new(segment.start) != PointKey::new(representative) {
            continue;
        }
        let point = Point::new_raw_xy(representative.x, representative.y);
        component.seed = Some(ring_owner.locate_stored(point)?);
        let exact = stored_exact_point(segment.start);
        let direction = segment_direction(segment)?;
        let atom = ExactAtom {
            start: AtomPoint::Endpoint(segment.start),
            end: AtomPoint::Endpoint(segment.end),
            start_exact: OnceCell::new(),
            midpoint: OnceCell::new(),
            segment,
            direction: &OnceCell::new(),
        };
        let forest = ExactPointForest::with_capacity(segments, 0);
        let own = atom_owner_label(
            &ring_owner,
            component,
            &atom,
            &mut ExactCoordinates::default(),
            &forest,
        )?;
        let other_location = other_owner.locate_exact_offset(&exact, &direction)?;
        let other = if component.dirty {
            other_location.location
        } else {
            other_location.edge_atom_location()
        };
        return Ok((own, other));
    }
    Err(GeometryErrorKind::Invalid(
        "test geometry has no clean winding-ring atom at its representative".into(),
    )
    .into())
}

#[test]
fn stored_dyadic_recovery_preserves_binary64_edges_without_rounding() {
    use crate::geometry::source_support::exact_stored_point;
    for value in [
        0.0,
        -0.0,
        1.0,
        -1.0,
        f64::MAX,
        -f64::MAX,
        f64::MIN_POSITIVE,
        f64::from_bits(1),
        -f64::from_bits(1),
        f64::from_bits(0x000F_FFFF_FFFF_FFFF),
        f64::from_bits(0x3FF0_0000_0000_0001),
    ] {
        let exact = stored_exact_point(XY {
            x: value,
            y: -value,
        });
        let point = exact_stored_point(&exact).unwrap();
        assert_eq!(point.x, value);
        assert_eq!(point.y, -value);
    }
    let midpoint = homogeneous_midpoint(
        &stored_exact_point(XY { x: 1.0, y: 1.0 }),
        &stored_exact_point(XY {
            x: f64::from_bits(0x3FF0_0000_0000_0001),
            y: 1.0,
        }),
    )
    .unwrap();
    assert!(exact_stored_point(&midpoint).is_none());
    let underflow = [
        ExactDyadic::from_f64(f64::from_bits(1)),
        ExactDyadic::zero(),
        ExactDyadic::from_f64(2.0),
    ];
    assert!(exact_stored_point(&underflow).is_none());
}

#[test]
#[cfg(debug_assertions)]
fn stored_recovery_does_not_round_ratios_or_reconvert_coordinates() {
    use crate::geometry::source_support::exact_stored_point;
    use crate::geometry::tessellation::exact::round_ratio;
    use crate::test_support::exact_operation_counts::measure;
    let point = stored_exact_point(XY::new(7.25, -3.125));
    let (recovered, counts) = measure(|| exact_stored_point(&point).unwrap());
    assert_eq!(recovered.x, 7.25);
    assert_eq!(recovered.y, -3.125);
    assert_eq!(counts.conversions, 0);
    assert_eq!(counts.ratio_roundings, 0);
    let (value, control) = measure(|| ExactDyadic::from_f64(0.75));
    assert_eq!(control.conversions, 1);
    let (rounded, control) = measure(|| round_ratio(&value, &ExactDyadic::one()).unwrap());
    assert_eq!(rounded, 0.75);
    assert_eq!(control.ratio_roundings, 1);
}

#[test]
#[cfg(debug_assertions)]
fn clean_shared_boundary_atoms_do_not_construct_exact_probes() {
    let left =
        crate::io::parse_wkt("POLYGON ((0 0,1 0,2 0,3 0,4 0,4 3,3 3,2 3,1 3,0 3,0 0))").unwrap();
    for (index, right) in [
        left.clone(),
        crate::io::parse_wkt("POLYGON ((0 0,1 0,2 0,3 0,4 0,4 -3,3 -3,2 -3,1 -3,0 -3,0 0))")
            .unwrap(),
    ]
    .into_iter()
    .enumerate()
    {
        let left_plan = PreparedPlan::transient(&left);
        let right_plan = PreparedPlan::transient(&right);
        let oracle = partition_oracle(&left, &right).unwrap();
        ATOM_METRICS.with(|cell| cell.set(AtomMetrics::default()));
        assert_eq!(relate(&left_plan, &right_plan).unwrap(), oracle);
        let metrics = ATOM_METRICS.with(std::cell::Cell::get);
        assert_eq!(metrics.midpoints, 0, "{metrics:?}");
        assert_eq!(metrics.node_locations, 0, "{metrics:?}");
        if index == 0 {
            assert_eq!(metrics.directions, 0, "{metrics:?}");
            assert!(metrics.stored_vertices <= 2, "{metrics:?}");
        } else {
            assert!(metrics.stored_vertices <= 16, "{metrics:?}");
        }
    }
}

#[test]
#[cfg(debug_assertions)]
fn nonareal_atoms_skip_probes_and_dirty_atoms_reuse_stored_vertices() {
    let left = crate::io::parse_wkt("LINESTRING (0 0,2 0,4 0,6 0)").unwrap();
    let right = crate::io::parse_wkt("LINESTRING (3 -2,3 2)").unwrap();
    let left_plan = PreparedPlan::transient(&left);
    let right_plan = PreparedPlan::transient(&right);
    ATOM_METRICS.with(|cell| cell.set(AtomMetrics::default()));
    assert_eq!(
        relate(&left_plan, &right_plan).unwrap(),
        partition_oracle(&left, &right).unwrap()
    );
    let metrics = ATOM_METRICS.with(std::cell::Cell::get);
    assert_eq!(metrics.stored_vertices, 4, "{metrics:?}");
    assert_eq!(metrics.midpoints, 0, "{metrics:?}");
    assert_eq!(metrics.directions, 0, "{metrics:?}");

    let left = crate::io::parse_wkt("POLYGON ((0 0,4 4,0 4,4 0,0 0))").unwrap();
    let right = crate::io::parse_wkt("POLYGON ((0 0,4 4,0 4,4 0,0 0))").unwrap();
    let left_plan = PreparedPlan::transient(&left);
    let right_plan = PreparedPlan::transient(&right);
    ATOM_METRICS.with(|cell| cell.set(AtomMetrics::default()));
    assert_eq!(
        relate(&left_plan, &right_plan).unwrap(),
        partition_oracle(&left, &right).unwrap()
    );
    let metrics = ATOM_METRICS.with(std::cell::Cell::get);
    assert!(
        metrics.midpoints > 0 && metrics.directions > 0,
        "{metrics:?}"
    );
    assert!(
        metrics.stored_vertices > 0 && metrics.stored_vertices <= 4,
        "{metrics:?}"
    );
}

#[test]
fn uncut_atom_tables_do_not_allocate_per_segment() {
    for count in [1, 32, 128] {
        let segments = (0..count)
            .map(|i| Segment {
                start: XY::new(i as f64, 0.0),
                end: XY::new(i as f64 + 1.0, 0.0),
            })
            .collect::<Vec<_>>();
        let mut cuts = vec![Vec::new(); count];
        let mut forest = ExactPointForest::with_capacity(&segments, 0);
        let mut coordinates = ExactCoordinates::default();
        let ((boundaries, coverage), allocations) = crate::test_support::allocation_count(|| {
            let (boundaries, positions) =
                order_event_cuts(&segments, &mut cuts, &mut forest, &mut coordinates).unwrap();
            let coverage = build_atom_coverage(
                &[],
                &segments,
                &[],
                &boundaries,
                &positions,
                &mut forest,
                &mut coordinates,
            )
            .unwrap();
            (boundaries, coverage)
        });
        assert_eq!(boundaries.len(), count);
        assert_eq!(coverage.len(), count);
        assert!(
            allocations <= 2,
            "{count} uncut segments allocated {allocations} times"
        );
    }
}

#[test]
fn corrected_clean_ring_atom_labels_keep_full_oracle_matrices() {
    let line = crate::io::parse_wkt("LINESTRING (10 10,12 10)").unwrap();
    assert!(matches!(&line, Shape::LineString(_)));
    let external_hole = "POLYGON ((0 0,2 0,2 2,0 2,0 0),(10 10,12 10,12 12,10 12,10 10))";
    let cases = [
        ("POINT (10 10)", "F02FF1102"),
        ("POINT (11 10)", "0F2FF1102"),
        ("MULTIPOINT ((10 10),(11 10))", "002FF1102"),
        ("MULTIPOINT ((10 10),(12 10),(12 12),(10 12))", "F02FF11F2"),
    ];
    for (point_wkt, expected_matrix) in cases {
        let external =
            crate::io::parse_wkt(&format!("GEOMETRYCOLLECTION ({external_hole},{point_wkt})"))
                .unwrap();
        let oracle = partition_oracle(&external, &line).unwrap();
        assert_eq!(oracle.text(), expected_matrix);
        assert_full_oracle_and_controls(point_wkt, &external, &line, oracle);
        assert_locator_oracle_parity(point_wkt, &external, &line);

        let node = PreparedPlan::transient(&external)
            .locate_stored(Point::new_raw_xy(10.0, 10.0))
            .unwrap();
        match point_wkt {
            "POINT (10 10)" => {
                assert_eq!(
                    node.location,
                    Loc::Interior,
                    "first vertex keeps point support"
                );
                assert!(!node.area);
            }
            "POINT (11 10)" => {
                assert_eq!(node.location, Loc::Exterior);
                let midpoint = PreparedPlan::transient(&external)
                    .locate_stored(Point::new_raw_xy(11.0, 10.0))
                    .unwrap();
                assert_eq!(midpoint.location, Loc::Interior);
                assert!(!midpoint.area);
            }
            "MULTIPOINT ((10 10),(11 10))" => {
                assert_eq!(node.location, Loc::Interior);
                assert!(!node.area);
                let midpoint = PreparedPlan::transient(&external)
                    .locate_stored(Point::new_raw_xy(11.0, 10.0))
                    .unwrap();
                assert_eq!(midpoint.location, Loc::Interior);
                assert!(!midpoint.area);
            }
            _ => {
                for (x, y) in [(10.0, 10.0), (12.0, 10.0), (12.0, 12.0), (10.0, 12.0)] {
                    let node = PreparedPlan::transient(&external)
                        .locate_stored(Point::new_raw_xy(x, y))
                        .unwrap();
                    assert_eq!(node.location, Loc::Interior);
                    assert!(!node.area);
                }
            }
        }
    }

    let filled = crate::io::parse_wkt(
        "GEOMETRYCOLLECTION (\
             POLYGON ((0 0,20 0,20 20,0 20,0 0)),POINT (5 5))",
    )
    .unwrap();
    let inner_ring = crate::io::parse_wkt("POLYGON ((5 5,10 5,10 10,5 10,5 5))").unwrap();
    let oracle = partition_oracle(&filled, &inner_ring).unwrap();
    assert_eq!(oracle.text(), "212FF1FF2");
    assert_full_oracle_and_controls("ring inside area", &filled, &inner_ring, oracle);
    assert_locator_oracle_parity("ring inside area", &filled, &inner_ring);

    let node = PreparedPlan::transient(&filled)
        .locate_stored(Point::new_raw_xy(5.0, 5.0))
        .unwrap();
    assert_eq!(node.location, Loc::Interior);
    assert!(node.area);
}

#[test]
fn clean_ring_atom_labels_use_only_areal_support() {
    let line = crate::io::parse_wkt("LINESTRING (10 10,12 10)").unwrap();
    let external_hole = "POLYGON ((0 0,2 0,2 2,0 2,0 0),(10 10,12 10,12 12,10 12,10 10))";
    for point_wkt in [
        "POINT (10 10)",
        "POINT (11 10)",
        "MULTIPOINT ((10 10),(11 10))",
        "MULTIPOINT ((10 10),(12 10),(12 12),(10 12))",
    ] {
        let external =
            crate::io::parse_wkt(&format!("GEOMETRYCOLLECTION ({external_hole},{point_wkt})"))
                .unwrap();
        let (edge, _) = clean_ring_atom_labels(
            &external,
            &line,
            matrix_predicates::Operand::Left,
            XY::new(10.0, 10.0),
        )
        .unwrap();
        assert_eq!(edge, Loc::Exterior, "{point_wkt}: external-hole edge atom");
    }

    let filled = crate::io::parse_wkt(
        "GEOMETRYCOLLECTION (\
             POLYGON ((0 0,20 0,20 20,0 20,0 0)),POINT (5 5))",
    )
    .unwrap();
    let inner_ring = crate::io::parse_wkt("POLYGON ((5 5,10 5,10 10,5 10,5 5))").unwrap();
    let (_, filled_edge) = clean_ring_atom_labels(
        &filled,
        &inner_ring,
        matrix_predicates::Operand::Right,
        XY::new(5.0, 5.0),
    )
    .unwrap();
    assert_eq!(filled_edge, Loc::Interior);
}
