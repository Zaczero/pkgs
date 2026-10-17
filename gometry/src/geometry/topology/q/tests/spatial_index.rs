#[test]
fn exact_point_query_visits_only_nearby_indexed_candidates() {
    let dense = Shape::geometry_collection(nested_box_shapes(8)).unwrap();
    let dense_topology = exact_topology(&dense);
    let dense_span_count = dense_topology.q.spans.len();
    dense_topology.q.spatial_index.reset_candidate_visits();
    assert_eq!(
        dense_topology
            .locate_point(Point::new_raw_xy(0.0, 80.0))
            .unwrap()
            .geometry,
        Loc::Exterior
    );
    assert!(
        dense_topology.q.spatial_index.candidate_visits() < dense_span_count / 8,
        "dense point query visited {} of {dense_span_count} span candidates",
        dense_topology.q.spatial_index.candidate_visits()
    );

    let sparse = Shape::geometry_collection(
        (0..16)
            .map(|index| {
                Shape::Point(Point::new_raw_xy(
                    -120.0 + f64::from(index) * 14.0,
                    40.0 + f64::from(index % 12),
                ))
            })
            .collect(),
    )
    .unwrap();
    let sparse_topology = exact_topology(&sparse);
    sparse_topology.q.spatial_index.reset_candidate_visits();
    assert_eq!(
        sparse_topology
            .locate_point(Point::new_raw_xy(0.0, 0.0))
            .unwrap()
            .geometry,
        Loc::Exterior
    );
    assert_eq!(sparse_topology.q.spatial_index.candidate_visits(), 0);
}

fn exact_topology_for_line_index_queries() -> (
    ExactGeographicTopology,
    [(Point, ExactGeographicPointLocation); 9],
) {
    let source = Shape::geometry_collection(vec![
        line(&[(-10.0, -1.0), (10.0, 3.0)]),
        line(&[(170.0, -2.0), (-170.0, 4.0)]),
        line(&[(-180.0, -3.0), (180.0, 3.0)]),
    ])
    .unwrap();
    let topology = exact_topology(&source);
    let probes = [
        (Point::new_raw_xy(0.0, 1.0), ExactGeographicPointLocation {
            geometry: Loc::Interior,
            area: Loc::Exterior,
        }),
        (
            Point::new_raw_xy(360.0, 1.0),
            ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Exterior,
            },
        ),
        (
            Point::new_raw_xy(180.0, 0.0),
            ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Exterior,
            },
        ),
        (
            Point::new_raw_xy(-180.0, 0.0),
            ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Exterior,
            },
        ),
        (
            Point::new_raw_xy(180.0, 1.0),
            ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Exterior,
            },
        ),
        (
            Point::new_raw_xy(-180.0, 1.0),
            ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Exterior,
            },
        ),
        (
            Point::new_raw_xy(180.0, 3.0),
            ExactGeographicPointLocation {
                geometry: Loc::Boundary,
                area: Loc::Exterior,
            },
        ),
        (
            Point::new_raw_xy(23.0, 30.0),
            ExactGeographicPointLocation {
                geometry: Loc::Exterior,
                area: Loc::Exterior,
            },
        ),
        (
            Point::new_raw_xy(-123.0, 90.0),
            ExactGeographicPointLocation {
                geometry: Loc::Exterior,
                area: Loc::Exterior,
            },
        ),
    ];
    (topology, probes)
}

#[test]
fn exact_point_queries_classify_source_and_seam_linework() {
    let (topology, probes) = exact_topology_for_line_index_queries();
    for (point, expected) in probes {
        assert_eq!(
            topology.locate_point(point).unwrap(),
            expected,
            "exact location at {point:?}",
        );
    }
}

#[test]
fn exact_point_queries_reuse_the_index_without_allocations() {
    let (topology, probes) = exact_topology_for_line_index_queries();
    let ((), allocations) = crate::test_support::allocation_count(|| {
        for _ in 0..8 {
            for (point, expected) in probes {
                assert_eq!(topology.locate_point(point).unwrap(), expected);
            }
        }
    });
    assert_eq!(allocations, 0);
}

#[test]
fn exact_high_degree_absent_line_queries_reuse_the_index_without_allocations() {
    let lines = (0..64)
        .map(|index| {
            let offset = (f64::from(index) + 0.5) / 64.0 - 0.5;
            line(&[(-1.0, -1.0 + offset), (1.0, 1.0 + offset)])
        })
        .collect();
    let source = Shape::geometry_collection(lines).unwrap();
    let topology = exact_topology(&source);
    let points = [
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(360.0, 0.0),
        Point::new_raw_xy(180.0, 30.0),
        Point::new_raw_xy(12.0, 30.0),
    ];
    let ((), allocations) = crate::test_support::allocation_count(|| {
        for _ in 0..8 {
            for point in points {
                assert_eq!(
                    topology.locate_point(point).unwrap(),
                    ExactGeographicPointLocation {
                        geometry: Loc::Exterior,
                        area: Loc::Exterior,
                    },
                );
            }
        }
    });
    assert_eq!(allocations, 0);
}
