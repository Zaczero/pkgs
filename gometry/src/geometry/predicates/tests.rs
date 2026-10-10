use crate::geometry::predicates::*;
use crate::geometry::*;

#[test]
fn geographic_line_closedness_uses_quotient_vertex_identity() {
    let point = Point::new_raw_xy;
    let line = |points: &[Point]| CoordSeq::try_from_points(points).unwrap();
    let crs84 = TopologyDomain::Geographic(crate::geographic::GeographicFrame::crs84());

    assert!(line_is_closed_in_domain(
        &line(&[point(0.0, 10.0), point(360.0, 10.0)]),
        crs84,
    ));
    assert!(line_is_closed_in_domain(
        &line(&[point(180.0, 0.0), point(170.0, 20.0), point(-180.0, 0.0)]),
        crs84,
    ));
    assert!(!line_is_closed_in_domain(
        &line(&[point(180.0, 0.0), point(170.0, 20.0), point(-180.0, 1.0)]),
        crs84,
    ));
    assert!(line_is_closed_in_domain(
        &line(&[point(-35.0, 90.0), point(135.0, 90.0)]),
        crs84,
    ));
    assert!(line_is_closed_in_domain(
        &line(&[point(-0.0, -0.0), point(0.0, 0.0)]),
        crs84,
    ));

    assert!(!line_is_closed(&line(&[
        point(0.0, 10.0),
        point(360.0, 10.0)
    ])));
    assert!(line_is_closed(&line(&[point(-0.0, -0.0), point(0.0, 0.0)])));
}

#[test]
fn ogc_mod2_multiline_point_relations_have_known_answers() {
    let point = Point::new_raw_xy;
    let multiline = Shape::MultiLineString(
        MultiLineString::try_new(CoordinateAxes::XY, vec![
            MultiMember::Value(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[point(0.0, 0.0), point(4.0, 0.0)]).unwrap(),
                )
                .expect("test line is valid"),
            ),
            MultiMember::Value(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[point(4.0, 0.0), point(4.0, 4.0)]).unwrap(),
                )
                .expect("test line is valid"),
            ),
        ])
        .unwrap(),
    );
    let cases = [
        (
            "odd-incidence boundary",
            point(0.0, 0.0),
            "F0FFFF102",
            "FF10F0FF2",
            false,
            true,
            true,
            false,
        ),
        (
            "even-incidence shared node",
            point(4.0, 0.0),
            "0FFFFF102",
            "0F1FF0FF2",
            true,
            true,
            false,
            false,
        ),
        (
            "off-line point",
            point(7.0, 9.0),
            "FF0FFF102",
            "FF1FF00F2",
            false,
            false,
            false,
            true,
        ),
    ];

    for (name, point, point_first, multiline_first, contains, covers, touches, disjoint) in cases {
        let point = Shape::Point(point);
        assert_eq!(point.relate(&multiline).unwrap(), point_first, "{name}");
        assert_eq!(multiline.relate(&point).unwrap(), multiline_first, "{name}");

        assert!(!point.contains(&multiline).unwrap(), "{name}");
        assert!(!point.contains_properly(&multiline).unwrap(), "{name}");
        assert!(!point.covers(&multiline).unwrap(), "{name}");
        assert_eq!(point.within(&multiline).unwrap(), contains, "{name}");
        assert_eq!(point.covered_by(&multiline).unwrap(), covers, "{name}");
        assert_eq!(point.touches(&multiline).unwrap(), touches, "{name}");
        assert_eq!(point.intersects(&multiline).unwrap(), !disjoint, "{name}");
        assert_eq!(point.disjoint(&multiline).unwrap(), disjoint, "{name}");

        assert_eq!(multiline.contains(&point).unwrap(), contains, "{name}");
        assert_eq!(
            multiline.contains_properly(&point).unwrap(),
            contains,
            "{name}"
        );
        assert_eq!(multiline.covers(&point).unwrap(), covers, "{name}");
        assert!(!multiline.within(&point).unwrap(), "{name}");
        assert!(!multiline.covered_by(&point).unwrap(), "{name}");
        assert_eq!(multiline.touches(&point).unwrap(), touches, "{name}");
        assert_eq!(multiline.intersects(&point).unwrap(), !disjoint, "{name}");
        assert_eq!(multiline.disjoint(&point).unwrap(), disjoint, "{name}");
    }
}

#[test]
fn touching_diagonal_polygons_keep_their_de9im_matrix() {
    let high = f64::from_bits(1.0_f64.to_bits() + 1);
    let polygon = |points: &[(f64, f64)]| {
        Shape::Polygon(Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(
                    &points
                        .iter()
                        .map(|&(x, y)| Point::new(x, y).unwrap())
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        ))
    };
    // The upper polygon is encountered first, so its ray gets the lower
    // target id despite being geometrically second around the shared vertex.
    let upper = polygon(&[(0.0, 0.0), (1.0, high), (2.0, 3.0), (0.0, 0.0)]);
    let lower = polygon(&[(0.0, 0.0), (1.0, 1.0), (2.0, 0.0), (0.0, 0.0)]);

    assert_eq!(upper.relate(&lower).unwrap(), "FF2F01212");
}

mod convex_tests {
    use super::*;

    /// Trusted O(n²) convexity oracle: a non-degenerate simple polygon is
    /// convex iff every edge's supporting line keeps all vertices weakly on
    /// ONE side (this also rejects self-intersecting "stars", whose chords
    /// straddle vertices). A zero-area (all-collinear) ring is not a region.
    fn oracle_convex(ring: &[Point]) -> bool {
        if ring.len() < 3 {
            return true;
        }
        let mut any_turn = false;
        for (a, b) in ring
            .iter()
            .copied()
            .zip(ring.iter().copied().cycle().skip(1))
        {
            let (mut cw, mut ccw) = (false, false);
            for &c in ring {
                match orientation(a, b, c) {
                    Orientation::Clockwise => cw = true,
                    Orientation::CounterClockwise => ccw = true,
                    Orientation::Collinear => {},
                }
            }
            if cw && ccw {
                return false;
            }
            any_turn |= cw || ccw;
        }
        any_turn
    }

    /// The atan2-free [`shell_is_convex`] must agree with the oracle on
    /// representative general-position rings of each admitted class.
    #[test]
    fn convexity_matches_oracle_on_curated_rings() {
        let p = |x, y| Point::new_raw_xy(x, y);
        let cases = [
            ("convex_cw_triangle", vec![
                p(-3.0, 3.0),
                p(2.0, -2.0),
                p(1.0, -4.0),
            ]),
            ("convex_ccw_pentagon", vec![
                p(-3.0, -4.0),
                p(3.0, -1.0),
                p(3.0, 4.0),
                p(-1.0, 1.0),
                p(-2.0, -1.0),
            ]),
            ("simple_reflex_ccw", vec![
                p(-2.0, 1.0),
                p(-2.0, 3.0),
                p(-4.0, -4.0),
                p(0.0, 3.0),
            ]),
            ("simple_reflex_cw", vec![
                p(-1.0, 0.0),
                p(3.0, 1.0),
                p(0.0, -1.0),
                p(-1.0, -3.0),
                p(0.0, 0.0),
                p(-4.0, -1.0),
            ]),
            ("longer_reflex", vec![
                p(-1.0, 0.0),
                p(-1.0, -2.0),
                p(3.0, 3.0),
                p(2.0, -1.0),
                p(3.0, -2.0),
                p(-2.0, -4.0),
                p(-4.0, -4.0),
            ]),
            ("self_crossing_six", vec![
                p(3.0, 4.0),
                p(1.0, -2.0),
                p(-2.0, 3.0),
                p(-2.0, -4.0),
                p(3.0, -2.0),
                p(-1.0, 3.0),
            ]),
            ("self_crossing_eight", vec![
                p(0.0, -3.0),
                p(-2.0, 0.0),
                p(-3.0, -1.0),
                p(-1.0, 4.0),
                p(-1.0, 3.0),
                p(-2.0, 4.0),
                p(0.0, -1.0),
                p(3.0, 1.0),
            ]),
        ];
        for (name, ring) in cases {
            assert_eq!(
                shell_is_convex(ring.as_slice()),
                oracle_convex(&ring),
                "convexity disagreement in {name}: {ring:?}"
            );
        }
    }

    /// Collinear and degenerate cases excluded from the general-position table.
    #[test]
    fn collinear_and_degenerate_cases() {
        let p = |x: f64, y: f64| Point::new_raw_xy(x, y);
        let convex: &[&[Point]] = &[
            &[p(0.0, 0.0), p(2.0, 0.0), p(1.0, 2.0)], // triangle
            &[p(0.0, 0.0), p(2.0, 0.0), p(2.0, 2.0), p(0.0, 2.0)], // square
            // square with a benign collinear vertex midway down the left edge
            &[
                p(0.0, 0.0),
                p(2.0, 0.0),
                p(2.0, 2.0),
                p(0.0, 2.0),
                p(0.0, 1.0),
            ],
            // collinear vertices along the bottom edge
            &[
                p(0.0, 0.0),
                p(1.0, 0.0),
                p(2.0, 0.0),
                p(2.0, 2.0),
                p(0.0, 2.0),
            ],
        ];
        let not_convex: &[&[Point]] = &[
            &[p(0.0, 0.0), p(2.0, 0.0), p(2.0, 2.0), p(1.0, 0.5)], // concave dart
            // pentagram: distinct vertices, all-same-turn, winds twice
            &[
                p(0.0, 2.0),
                p(1.0, -1.0),
                p(-1.6, 1.0),
                p(1.6, 1.0),
                p(-1.0, -1.0),
            ],
            // overlapping edges along y = x (non-simple)
            &[
                p(0.0, 0.0),
                p(3.0, 3.0),
                p(-1.0, 1.0),
                p(-1.0, -1.0),
                p(1.0, 1.0),
            ],
            &[p(0.0, 0.0), p(2.0, 0.0), p(0.0, 0.0), p(0.0, 2.0)], // repeated-vertex spike
        ];
        for ring in convex {
            assert!(shell_is_convex(*ring), "expected convex: {ring:?}");
        }
        for ring in not_convex {
            assert!(!shell_is_convex(*ring), "expected non-convex: {ring:?}");
        }
    }

    #[test]
    fn convexity_keeps_only_the_terminal_closure_duplicate() {
        let p = |x: f64, y: f64| Point::new_raw_xy(x, y);
        let square = [p(0.0, 0.0), p(4.0, 0.0), p(4.0, 4.0), p(0.0, 4.0)];
        let closed = [square[0], square[1], square[2], square[3], square[0]];
        assert!(shell_is_convex(closed.as_slice()));
        let repeated_closure = [
            square[0], square[0], square[1], square[2], square[3], square[3], square[0],
        ];
        assert!(shell_is_convex(repeated_closure.as_slice()));
        // The return to the first vertex is intermediate rather than the
        // terminal closure, so it remains in the cyclic sequence and the
        // self-crossing ring is not admitted as convex.
        let intermediate_return = [square[0], square[1], square[0], square[2], square[3]];
        assert!(!shell_is_convex(intermediate_return.as_slice()));
    }

    #[test]
    fn convexity_over_a_closed_coordseq_does_not_allocate() {
        let shell = CoordSeq::try_from_points(&[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(4.0, 0.0),
            Point::new_raw_xy(4.0, 4.0),
            Point::new_raw_xy(0.0, 4.0),
            Point::new_raw_xy(0.0, 0.0),
        ])
        .unwrap();
        let (convex, allocations) =
            crate::test_support::allocation_count(|| shell_is_convex(&shell));
        assert!(convex);
        assert_eq!(allocations, 0);
    }

    #[test]
    fn axis_rectangle_detection_and_intersects_fast_path() {
        let p = |x: f64, y: f64| Point::new_raw_xy(x, y);
        let rect = |minx, miny, maxx, maxy| {
            Shape::Polygon(Polygon::new(
                Ring::from_closed(
                    CoordSeq::try_from_points(&[
                        p(minx, miny),
                        p(maxx, miny),
                        p(maxx, maxy),
                        p(minx, maxy),
                        p(minx, miny),
                    ])
                    .unwrap(),
                )
                .unwrap(),
                Vec::new(),
            ))
        };
        let bounds = |shape: &Shape| shape.bounds().expect("bounded");

        // A box from any corner order is detected; degenerate / holed / non-axis
        // polygons are not.
        let r = rect(0.0, 0.0, 10.0, 10.0);
        assert!(is_axis_aligned_rectangle(&r, bounds(&r)));
        let line_box = rect(0.0, 0.0, 10.0, 0.0); // zero height → not a rectangle
        assert!(!is_axis_aligned_rectangle(&line_box, bounds(&line_box)));
        let triangle = Shape::Polygon(Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[p(0.0, 0.0), p(10.0, 0.0), p(5.0, 10.0), p(0.0, 0.0)])
                    .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        ));
        assert!(!is_axis_aligned_rectangle(&triangle, bounds(&triangle)));
        let diamond = Shape::Polygon(Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    p(5.0, 0.0),
                    p(10.0, 5.0),
                    p(5.0, 10.0),
                    p(0.0, 5.0),
                    p(5.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        ));
        assert!(!is_axis_aligned_rectangle(&diamond, bounds(&diamond)));

        let densified = Shape::Polygon(Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    p(0.0, 0.0),
                    p(5.0, 0.0),
                    p(10.0, 0.0),
                    p(10.0, 10.0),
                    p(5.0, 10.0),
                    p(0.0, 10.0),
                    p(0.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        ));
        assert!(is_axis_aligned_rectangle(&densified, bounds(&densified)));
        assert!(
            PreparedPlan::transient(&densified)
                .intersects(&PreparedPlan::transient(&r))
                .unwrap(),
            "cached planar intersects recognizes a densified rectangle"
        );

        // The fast path must agree with the general kernel on every box relation:
        // overlap, crossing (no corner of one inside the other), edge-touch,
        // corner-touch, nested, and disjoint.
        let base = rect(0.0, 0.0, 10.0, 10.0);
        let cases = [
            (rect(5.0, 5.0, 15.0, 15.0), true),    // offset overlap
            (rect(2.0, 2.0, 8.0, 8.0), true),      // nested
            (rect(10.0, 0.0, 20.0, 10.0), true),   // edge touch
            (rect(10.0, 10.0, 20.0, 20.0), true),  // corner touch
            (rect(20.0, 20.0, 30.0, 30.0), false), // disjoint
            (rect(4.0, -5.0, 6.0, 15.0), true),    // crossing "+": no corner inside
        ];
        for (other, expected) in cases {
            assert_eq!(
                base.intersects(&other).unwrap(),
                expected,
                "rect intersects mismatch for {other:?}"
            );
            // symmetric
            assert_eq!(other.intersects(&base).unwrap(), expected);
        }
    }

    #[test]
    fn prepared_intersects_agrees_with_the_relate_matrix() {
        // Representative probes plus boundary contact decide `intersects`
        // without the matrix; holes, touching boundaries and nesting are
        // where either half alone would answer wrongly.
        let donut = "POLYGON ((0 0,10 0,10 10,0 10,0 0),(3 3,7 3,7 7,3 7,3 3))";
        let pairs = [
            (donut, "POLYGON ((4 4,6 4,6 6,4 6,4 4))"),
            (donut, "POLYGON ((3 4,6 4,6 6,3 6,3 4))"),
            (donut, "POLYGON ((2 4,6 4,6 6,2 6,2 4))"),
            (donut, "POLYGON ((-1 -1,11 -1,11 11,-1 11,-1 -1))"),
            (donut, "LINESTRING (4 5,6 5)"),
            (donut, "LINESTRING (4 5,8 5)"),
            (donut, "MULTIPOINT ((5 5),(20 20))"),
            (donut, "MULTIPOINT ((5 5),(1 1))"),
            ("POLYGON ((0 0,4 0,0 4,0 0))", "POLYGON ((4 0,8 0,8 4,4 0))"),
            (
                "POLYGON ((0 0,4 0,0 4,0 0))",
                "POLYGON ((4.5 0,8 0,8 4,4.5 0))",
            ),
            (
                "MULTIPOLYGON (((0 0,1 0,1 1,0 1,0 0)),((5 5,6 5,6 6,5 6,5 5)))",
                "POLYGON ((5.5 5.5,9 5.5,9 9,5.5 9,5.5 5.5))",
            ),
            (
                "MULTIPOLYGON (((0 0,1 0,1 1,0 1,0 0)),((5 5,6 5,6 6,5 6,5 5)))",
                "LINESTRING (2 2,4 4)",
            ),
            ("LINESTRING (0 0,10 10)", "LINESTRING (0 10,10 0)"),
            ("LINESTRING (0 0,10 10)", "LINESTRING (1 0,11 10)"),
        ];
        for (left, right) in pairs {
            let left = crate::io::parse_wkt(left).unwrap();
            let right = crate::io::parse_wkt(right).unwrap();
            let expected = graph_relate_shapes(&left, &right).unwrap().is_intersects();
            let (left, right) = (
                PreparedPlan::transient(&left),
                PreparedPlan::transient(&right),
            );
            assert_eq!(
                left.intersects(&right).unwrap(),
                expected,
                "{left:?} {right:?}"
            );
            assert_eq!(
                right.intersects(&left).unwrap(),
                expected,
                "{right:?} {left:?}"
            );
        }
    }

    #[test]
    fn linework_repair_cancels_retraced_occurrences_within_one_ring() {
        let retraced =
            crate::io::parse_wkt("POLYGON ((0 0,2 0,2 2,0 2,0 0,2 0,2 2,0 2,0 0))").unwrap();
        let repaired = retraced
            .repair_with_context(
                RepairMethod::Linework,
                false,
                &crate::geometry::SourceArcContext::cartesian(),
            )
            .unwrap();
        assert_eq!(crate::io::to_wkt(&repaired).unwrap(), "POLYGON EMPTY");
    }

    #[test]
    fn repair_linework_unions_distinct_polygon_members() {
        let overlap =
            crate::io::parse_wkt("MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((2 2,6 2,6 6,2 6,2 2)))")
                .unwrap();
        let linework = overlap
            .repair_with_context(
                RepairMethod::Linework,
                false,
                &crate::geometry::SourceArcContext::cartesian(),
            )
            .unwrap();
        let structure = overlap
            .repair_with_context(
                RepairMethod::Structure,
                false,
                &crate::geometry::SourceArcContext::cartesian(),
            )
            .unwrap();
        assert_eq!(linework.geometry_type(), "Polygon");
        assert_eq!(linework.area(), 28.0);
        assert_eq!(structure.geometry_type(), "Polygon");
        assert_eq!(structure.area(), 28.0);

        let nested = crate::io::parse_wkt(
            "POLYGON ((0 0,10 0,10 10,0 10,0 0),(2 2,8 2,8 8,2 8,2 2),(4 4,6 4,6 6,4 6,4 4))",
        )
        .unwrap();
        assert_eq!(
            nested
                .repair_with_context(
                    RepairMethod::Linework,
                    false,
                    &crate::geometry::SourceArcContext::cartesian(),
                )
                .unwrap()
                .area(),
            64.0
        );
    }

    #[test]
    fn geographic_repair_keeps_collinear_source_vertices_on_selected_edges() {
        let bowtie = crate::io::parse_wkt("POLYGON ((0 0,0.5 0.5,2 2,0 2,2 0,0 0))").unwrap();
        let repaired = crate::geometry::repair_shape_in_frame(
            &bowtie,
            TopologyDomain::Geographic(crate::geographic::GeographicFrame::crs84()),
            RepairMethod::Linework,
            &crate::geometry::SourceArcContext::new(
                TopologyDomain::Geographic(crate::geographic::GeographicFrame::crs84()),
                None,
            ),
        )
        .unwrap()
        .unwrap();
        assert!(
            repaired
                .points_vec()
                .unwrap()
                .contains(&Point::new_raw_xy(0.5, 0.5))
        );
    }
}
