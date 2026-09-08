use crate::geometry::overlay::*;
use crate::geometry::*;

#[test]
fn sourced_noder_orders_extreme_horizontal_cuts_and_keeps_owner() {
    for scale in [2.0_f64.powi(-600), 2.0_f64.powi(600)] {
        let source = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(4.0 * scale, 0.0),
        };
        let cutters = [3.0, 1.0, 2.0].map(|multiple| Segment {
            start: XY::new(multiple * scale, -scale),
            end: XY::new(multiple * scale, scale),
        });
        let (atomic, provenance) =
            self_node_segments_sourced(std::iter::once(source).chain(cutters)).unwrap();
        let source_atoms: Vec<_> = atomic
            .iter()
            .enumerate()
            .filter(|(_, segment)| segment.start.y == 0.0 && segment.end.y == 0.0)
            .collect();
        assert_eq!(source_atoms.len(), 4);
        assert_eq!(
            source_atoms
                .iter()
                .map(|(_, segment)| (segment.start.x, segment.end.x))
                .collect::<Vec<_>>(),
            vec![
                (0.0, scale),
                (scale, 2.0 * scale),
                (2.0 * scale, 3.0 * scale),
                (3.0 * scale, 4.0 * scale),
            ]
        );
        for (index, _) in source_atoms {
            assert_eq!(provenance.owners(index), &[(0, false)]);
        }
    }
}

mod binary_areal_overlay_tests {
    use super::*;

    /// The bounds-component partition is an optimization, so the only
    /// evidence it can give is agreement with the unpartitioned lane on
    /// inputs that actually split: several clusters of overlapping blobs
    /// with clear water between them, plus a lone empty input and a lone
    /// point so singleton components of every dimension are exercised.
    #[test]
    fn partitioned_union_matches_the_single_arrangement() {
        use crate::geometry::{Strictness, TopologyDomain};
        let blob = |cx: f64, cy: f64, r: f64| -> Shape {
            let points: Vec<(f64, f64)> = (0..12)
                .map(|i| {
                    let theta = f64::from(i) / 12.0 * std::f64::consts::TAU;
                    (cx + r * theta.cos(), cy + r * theta.sin())
                })
                .chain(std::iter::once((cx + r, cy)))
                .collect();
            Shape::Polygon(polygon(&points))
        };
        let mut inputs: Vec<Shape> = Vec::new();
        // Three clusters, each internally overlapping, far apart from each other.
        for (ox, oy) in [(0.0, 0.0), (100.0, 0.0), (0.0, 100.0)] {
            for k in 0..5 {
                inputs.push(blob(ox + 1.3 * f64::from(k), oy + 0.7 * f64::from(k), 1.5));
            }
        }
        inputs.push(Shape::Point(crate::geometry::Point::new_raw_xy(50.0, 50.0)));
        inputs.push(Shape::empty_polygon());
        for op in [OverlayOp::Union, OverlayOp::SymmetricDifference] {
            let partitioned = super::nary::overlay_all_with_bounds_certificates(
                &inputs,
                TopologyDomain::Cartesian,
                op,
                Strictness::Lenient,
                true,
            )
            .unwrap();
            let single = super::nary::overlay_all_with_bounds_certificates(
                &inputs,
                TopologyDomain::Cartesian,
                op,
                Strictness::Lenient,
                false,
            )
            .unwrap();
            assert!(
                partitioned.equals(&single).unwrap(),
                "{op:?}: partitioned overlay differs from the single arrangement"
            );
        }
    }

    fn polygon(points: &[(f64, f64)]) -> Polygon {
        Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(
                    &points
                        .iter()
                        .map(|&(x, y)| Point::new(x, y).unwrap())
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
            ),
            Vec::new(),
        )
        .unwrap()
    }

    fn square(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Shape {
        Shape::Polygon(polygon(&[
            (minx, miny),
            (maxx, miny),
            (maxx, maxy),
            (minx, maxy),
            (minx, miny),
        ]))
    }

    fn regular_polygon(cx: f64, cy: f64, radius: f64, vertices: usize) -> Shape {
        let mut points: Vec<Point> = (0..vertices)
            .map(|index| {
                let angle = std::f64::consts::TAU * index as f64 / vertices as f64;
                Point::new_raw_xy(cx + radius * angle.cos(), cy + radius * angle.sin())
            })
            .collect();
        points.push(points[0]);
        Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    fn zm_square(minx: f64, miny: f64, maxx: f64, maxy: f64, offset: f64) -> Shape {
        let points = [
            (minx, miny),
            (maxx, miny),
            (maxx, maxy),
            (minx, maxy),
            (minx, miny),
        ]
        .into_iter()
        .map(|(x, y)| {
            Point::new_axes(
                x,
                y,
                ZOrdinate(Some(offset + x + y)),
                MOrdinate(Some(offset - x + y)),
            )
        })
        .collect::<Result<Vec<_>>>()
        .unwrap();
        Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    fn m_square(minx: f64, miny: f64, maxx: f64, maxy: f64, offset: f64) -> Shape {
        let points = [
            (minx, miny),
            (maxx, miny),
            (maxx, maxy),
            (minx, maxy),
            (minx, miny),
        ]
        .into_iter()
        .map(|(x, y)| Point::new_axes(x, y, ZOrdinate(None), MOrdinate(Some(offset + x + y))))
        .collect::<Result<Vec<_>>>()
        .unwrap();
        Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    fn zm_line(start: (f64, f64), end: (f64, f64)) -> Shape {
        let points = [start, end]
            .into_iter()
            .map(|(x, y)| {
                Point::new_axes(
                    x,
                    y,
                    ZOrdinate(Some(x + y + 10.0)),
                    MOrdinate(Some(y - x + 10.0)),
                )
            })
            .collect::<Result<Vec<_>>>()
            .unwrap();
        Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap())
                .expect("test line is valid"),
        )
    }

    fn invalid_zm_bowtie() -> Shape {
        let points = [(0.0, 0.0), (2.0, 2.0), (0.0, 2.0), (2.0, 0.0), (0.0, 0.0)]
            .into_iter()
            .map(|(x, y)| {
                Point::new_axes(
                    x,
                    y,
                    ZOrdinate(Some(x + y + 10.0)),
                    MOrdinate(Some(y - x + 10.0)),
                )
            })
            .collect::<Result<Vec<_>>>()
            .unwrap();
        Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    fn z_square(minx: f64, miny: f64, maxx: f64, maxy: f64, offset: f64) -> Shape {
        let points = [
            (minx, miny),
            (maxx, miny),
            (maxx, maxy),
            (minx, maxy),
            (minx, miny),
        ]
        .into_iter()
        .map(|(x, y)| Point::new_axes(x, y, ZOrdinate(Some(offset + x + y)), MOrdinate(None)))
        .collect::<Result<Vec<_>>>()
        .unwrap();
        Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    fn line_seq(points: &[(f64, f64)]) -> LineSeq {
        LineSeq::try_new(
            CoordSeq::try_from_points(
                &points
                    .iter()
                    .map(|&(x, y)| Point::new(x, y).unwrap())
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .expect("test line is valid")
    }

    fn line(points: &[(f64, f64)]) -> Shape {
        Shape::LineString(line_seq(points))
    }

    fn multiline(lines: &[&[(f64, f64)]]) -> Shape {
        Shape::MultiLineString(
            MultiLineString::from_values(
                CoordinateAxes::XY,
                lines
                    .iter()
                    .map(|points| MultiMember::Value(line_seq(points)))
                    .collect(),
            )
            .unwrap(),
        )
    }

    fn disjoint_multiline(count: usize, offset: f64) -> Shape {
        Shape::MultiLineString(
            MultiLineString::from_values(
                CoordinateAxes::XY,
                (0..count)
                    .map(|index| {
                        let x = offset + index as f64 * 2.0;
                        MultiMember::Value(line_seq(&[(x, 0.0), (x, 1.0)]))
                    })
                    .collect(),
            )
            .unwrap(),
        )
    }

    fn point(x: f64, y: f64) -> Shape {
        Shape::Point(Point::new(x, y).unwrap())
    }

    fn manual_left_fold(inputs: &[&Shape], op: OverlayOp) -> Shape {
        let (first, rest) = inputs.split_first().expect("non-empty test inputs");
        let mut accumulated = (*first).clone();
        for input in rest {
            accumulated = accumulated.overlay(input, op, Strictness::Strict).unwrap();
        }
        accumulated
    }

    fn assert_all_points_have_z(shape: &Shape) {
        shape.for_each_point(|point| {
            assert!(point.z().is_some(), "missing Z in {shape:?}");
        });
    }

    fn assert_topologically_equal(left: &Shape, right: &Shape) {
        let delta = left
            .symmetric_difference(right, Strictness::Strict)
            .unwrap();
        assert!(
            delta.is_empty(),
            "left={left:?}\nright={right:?}\ndelta={delta:?}"
        );
    }

    fn wkb(shape: &Shape) -> Vec<u8> {
        crate::io::to_wkb(shape, None, false).expect("overlay WKB")
    }

    fn assert_disjoint_certificate_matches_graph(inputs: &[&Shape], op: OverlayOp) {
        let certified = overlay_all(
            inputs,
            crate::geometry::TopologyDomain::Cartesian,
            op,
            Strictness::Strict,
        )
        .unwrap();
        let graph = super::super::nary::overlay_all_without_bounds_certificates(
            inputs,
            crate::geometry::TopologyDomain::Cartesian,
            op,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(
            wkb(&certified),
            wkb(&graph),
            "{}: {certified:?} != {graph:?}",
            op.name()
        );
    }

    /// The overlay engine preserves INPUT VERTICES bit-exactly — the wart
    /// the `i_overlay` path had (it snapped every coordinate to its grid,
    /// perturbing untouched vertices in the 8th decimal).
    #[test]
    fn overlay_preserves_input_vertices_bit_exactly() {
        let left = polygon(&[
            (0.123_456_789_012_345_6, 0.987_654_321_098_765),
            (10.765_432_109_876_5, 0.123_456_789),
            (5.555_555_555_555_5, 9.999_999_999_99),
            (0.123_456_789_012_345_6, 0.987_654_321_098_765),
        ]);
        let right = polygon(&[
            (3.333_333_333_333_3, -1.111_111_111_111),
            (12.222_222_222_222_2, 4.444_444_444_444),
            (4.040_404_040_404, 8.080_808_080_808),
            (3.333_333_333_333_3, -1.111_111_111_111),
        ]);
        let union = binary_areal_overlay(
            std::slice::from_ref(&left),
            std::slice::from_ref(&right),
            OverlayOp::Union,
        )
        .unwrap();
        let output: Vec<Point> = union
            .iter()
            .flat_map(Polygon::rings)
            .flat_map(|ring| ring.iter_coords().collect::<Vec<_>>())
            .collect();
        for original in left.shell.coords().iter_coords() {
            assert!(
                output
                    .iter()
                    .any(|point| point.x.to_bits() == original.x.to_bits()
                        && point.y.to_bits() == original.y.to_bits()),
                "vertex {original:?} not preserved"
            );
        }
    }

    fn assert_disjoint_results_match(inputs: &[&Shape], op: OverlayOp) {
        let certified = overlay_all(
            inputs,
            crate::geometry::TopologyDomain::Cartesian,
            op,
            Strictness::Strict,
        );
        let forced = super::super::nary::overlay_all_without_bounds_certificates(
            inputs,
            crate::geometry::TopologyDomain::Cartesian,
            op,
            Strictness::Strict,
        );
        match (certified, forced) {
            (Ok(certified), Ok(forced)) => {
                assert_eq!(wkb(&certified), wkb(&forced), "{}", op.name());
            },
            (Err(certified), Err(forced)) => {
                assert_eq!(certified.to_string(), forced.to_string(), "{}", op.name());
            },
            (certified, forced) => panic!(
                "{} diverged: certified={certified:?}, forced={forced:?}",
                op.name()
            ),
        }
    }

    fn assert_line_overlay_matches_generic(inputs: &[&Shape], op: OverlayOp) {
        let fast = overlay_all(
            inputs,
            crate::geometry::TopologyDomain::Cartesian,
            op,
            Strictness::Strict,
        );
        let generic = super::super::nary::overlay_all_line_oracle(inputs, op, Strictness::Strict);
        match (fast, generic) {
            (Ok(fast), Ok(generic)) => {
                assert!(
                    fast.equals(&generic).unwrap(),
                    "{}: {fast:?} != {generic:?}",
                    op.name()
                );
            },
            (Err(fast), Err(generic)) => assert_eq!(
                fast.to_string(),
                generic.to_string(),
                "{}: {fast} != {generic}",
                op.name()
            ),
            (fast, generic) => panic!("{} diverged: fast={fast:?}, generic={generic:?}", op.name()),
        }
    }

    /// The areal arrangement lane reports every lower-dimensional contact the
    /// joint graph does: shared boundary runs, isolated touches, a region plus
    /// a separate touch, nested with a shared side, identical operands.
    #[test]
    fn areal_intersection_lane_matches_joint_graph_on_contacts() {
        let cases = [
            (
                "shared edge",
                square(0.0, 0.0, 2.0, 2.0),
                square(2.0, 0.0, 4.0, 2.0),
            ),
            (
                "corner touch",
                square(0.0, 0.0, 2.0, 2.0),
                square(2.0, 2.0, 4.0, 4.0),
            ),
            (
                "edge interior touch",
                square(0.0, 0.0, 2.0, 2.0),
                square(2.0, 1.0, 4.0, 3.0),
            ),
            (
                "two separate shared boundary runs",
                Shape::Polygon(polygon(&[
                    (0.0, 0.0),
                    (4.0, 0.0),
                    (4.0, 1.0),
                    (1.0, 1.0),
                    (1.0, 3.0),
                    (4.0, 3.0),
                    (4.0, 4.0),
                    (0.0, 4.0),
                    (0.0, 0.0),
                ])),
                Shape::Polygon(polygon(&[
                    (2.0, 1.0),
                    (6.0, 1.0),
                    (6.0, 3.0),
                    (2.0, 3.0),
                    (2.0, 1.0),
                ])),
            ),
            (
                "nested sharing a side",
                square(0.0, 0.0, 4.0, 4.0),
                square(0.0, 1.0, 2.0, 3.0),
            ),
            (
                "identical",
                square(0.0, 0.0, 2.0, 2.0),
                square(0.0, 0.0, 2.0, 2.0),
            ),
            (
                "disjoint",
                square(0.0, 0.0, 2.0, 2.0),
                square(3.0, 3.0, 4.0, 4.0),
            ),
            (
                "hole boundary touch",
                Shape::Polygon(
                    Polygon::new(
                        polygon(&[(0.0, 0.0), (6.0, 0.0), (6.0, 6.0), (0.0, 6.0), (0.0, 0.0)])
                            .shell,
                        vec![
                            polygon(&[(2.0, 2.0), (4.0, 2.0), (4.0, 4.0), (2.0, 4.0), (2.0, 2.0)])
                                .shell,
                        ],
                    )
                    .unwrap(),
                ),
                square(2.0, 2.0, 3.0, 3.0),
            ),
        ];
        for (name, left, right) in cases {
            let forward = assert_intersection_matches_joint_graph(name, &left, &right);
            let reverse = assert_intersection_matches_joint_graph(name, &right, &left);
            assert_eq!(wkb(&forward), wkb(&reverse), "{name}");
        }
    }

    #[test]
    fn areal_intersection_preserves_region_and_isolated_point() {
        let left = Shape::geometry_collection(vec![
            square(0.0, 0.0, 2.0, 2.0),
            square(4.0, 4.0, 6.0, 6.0),
        ])
        .unwrap();
        let right = square(1.0, 1.0, 4.0, 4.0);
        let expected = Shape::geometry_collection(vec![
            square(1.0, 1.0, 2.0, 2.0),
            Shape::Point(Point::new_raw_xy(4.0, 4.0)),
        ])
        .unwrap();
        for (left, right) in [(&left, &right), (&right, &left)] {
            let result =
                assert_intersection_matches_joint_graph("region and isolated point", left, right);
            assert!(
                result.equals(&expected).unwrap(),
                "{result:?} != {expected:?}"
            );
        }
    }

    #[test]
    fn collapsed_shell_does_not_emit_an_areal_hole_as_linework() {
        let shell = polygon(&[(0.0, 0.0), (4.0, 0.0), (2.0, 0.0), (0.0, 0.0)]).shell;
        let hole = polygon(&[(1.0, 1.0), (3.0, 1.0), (3.0, 3.0), (1.0, 3.0), (1.0, 1.0)]).shell;
        let source = Shape::Polygon(Polygon::new(shell, vec![hole]).unwrap());
        let enclosing = square(-1.0, -1.0, 5.0, 5.0);
        let expected = line(&[(0.0, 0.0), (4.0, 0.0)]);
        for (left, right) in [(&source, &enclosing), (&enclosing, &source)] {
            let result = left.intersection(right, Strictness::Lenient).unwrap();
            assert!(
                result.equals(&expected).unwrap(),
                "{result:?} != {expected:?}"
            );
        }
    }

    #[test]
    fn areal_intersection_matches_graph_on_degenerate_boundaries() {
        let cases: &[(&str, &[(f64, f64)])] = &[
            ("collapsed ring", &[
                (0.0, 0.0),
                (2.0, 0.0),
                (4.0, 0.0),
                (0.0, 0.0),
            ]),
            ("repeated vertex", &[
                (0.0, 0.0),
                (2.0, 0.0),
                (2.0, 0.0),
                (2.0, 2.0),
                (0.0, 2.0),
                (0.0, 0.0),
            ]),
            ("retraced exterior spur", &[
                (0.0, 0.0),
                (2.0, 0.0),
                (4.0, 0.0),
                (2.0, 0.0),
                (2.0, 2.0),
                (0.0, 2.0),
                (0.0, 0.0),
            ]),
        ];
        for &(name, coordinates) in cases {
            let left = Shape::Polygon(polygon(coordinates));
            let right = square(3.0, -1.0, 5.0, 1.0);
            for (left, right) in [(&left, &right), (&right, &left)] {
                let result = assert_intersection_matches_joint_graph(name, left, right);
                if name == "collapsed ring" {
                    assert!(
                        result.equals(&line(&[(3.0, 0.0), (4.0, 0.0)])).unwrap(),
                        "{result:?}"
                    );
                }
                if name == "retraced exterior spur" {
                    // Opposite traversals cancel under the areal nonzero rule.
                    assert!(result.is_empty(), "{result:?}");
                }
            }
        }
    }

    #[test]
    fn areal_intersection_matches_joint_graph_on_triangle_contacts() {
        let left = square(0.0, 0.0, 2.0, 2.0);
        let cases: &[(&str, &[(f64, f64)])] = &[
            ("vertex on edge", &[
                (2.0, 1.0),
                (3.0, 0.0),
                (3.0, 2.0),
                (2.0, 1.0),
            ]),
            ("shared diagonal endpoints", &[
                (0.0, 0.0),
                (2.0, 2.0),
                (3.0, 0.0),
                (0.0, 0.0),
            ]),
            ("crossing edges", &[
                (-1.0, 1.0),
                (3.0, 1.0),
                (1.0, 3.0),
                (-1.0, 1.0),
            ]),
            ("shared edge reversed", &[
                (0.0, 0.0),
                (1.0, -2.0),
                (2.0, 0.0),
                (0.0, 0.0),
            ]),
        ];
        for &(name, coordinates) in cases {
            let right = Shape::Polygon(polygon(coordinates));
            assert_intersection_matches_joint_graph(name, &left, &right);
            assert_intersection_matches_joint_graph(name, &right, &left);
        }
    }

    #[test]
    fn areal_intersection_lane_preserves_zm_and_operand_order() {
        let left = zm_square(0.0, 0.0, 2.0, 2.0, 10.0);
        for right in [
            zm_square(1.0, 1.0, 3.0, 3.0, 20.0),
            zm_square(2.0, 0.0, 4.0, 2.0, 20.0),
            zm_square(2.0, 2.0, 4.0, 4.0, 20.0),
        ] {
            for strictness in [Strictness::Strict, Strictness::Lenient] {
                let fast = overlay_all(
                    &[&left, &right],
                    crate::geometry::TopologyDomain::Cartesian,
                    OverlayOp::Intersection,
                    strictness,
                );
                let reverse = overlay_all(
                    &[&right, &left],
                    crate::geometry::TopologyDomain::Cartesian,
                    OverlayOp::Intersection,
                    strictness,
                );
                let joint =
                    super::super::nary::binary_intersection_joint_graph(&left, &right, strictness);
                match (fast, reverse, joint) {
                    (Ok(fast), Ok(reverse), Ok(joint)) => {
                        assert_eq!(wkb(&fast), wkb(&joint), "{fast:?} != {joint:?}");
                        assert_eq!(wkb(&fast), wkb(&reverse), "{fast:?} != {reverse:?}");
                        assert_eq!(fast.has_z(), joint.has_z(), "{fast:?}");
                        assert_eq!(fast.has_m(), joint.has_m(), "{fast:?}");
                    },
                    (Err(fast), Err(reverse), Err(joint)) => {
                        assert_eq!(fast.to_string(), joint.to_string());
                        assert_eq!(fast.to_string(), reverse.to_string());
                    },
                    (fast, reverse, joint) => {
                        panic!("diverged: fast={fast:?} reverse={reverse:?} joint={joint:?}")
                    },
                }
            }
        }
    }

    fn assert_intersection_matches_joint_graph(name: &str, left: &Shape, right: &Shape) -> Shape {
        let fast = overlay_all(
            &[left, right],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        let exact =
            super::super::nary::binary_intersection_joint_graph(left, right, Strictness::Strict)
                .unwrap();
        assert_eq!(wkb(&fast), wkb(&exact), "{name}: {fast:?} != {exact:?}");
        fast
    }

    /// Pinch resolution: corner-touching squares union into a VALID
    /// `MultiPolygon` of two simple lobes (never one figure-eight ring),
    /// and the maximal walk's split classifies a shell-touching hole
    /// correctly (the S2 dissolve lens — pinned by the cell-parity
    /// pytest); identical operands xor to nothing.
    #[test]
    fn pinched_overlays_resolve_to_simple_rings() {
        let lower = polygon(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]);
        let upper = polygon(&[(1.0, 1.0), (2.0, 1.0), (2.0, 2.0), (1.0, 2.0), (1.0, 1.0)]);
        let union = binary_areal_overlay(
            std::slice::from_ref(&lower),
            std::slice::from_ref(&upper),
            OverlayOp::Union,
        )
        .unwrap();
        assert_eq!(union.len(), 2, "{union:?}");
        assert!(union.iter().all(|part| part.holes.is_empty()));

        let xor = binary_areal_overlay(
            std::slice::from_ref(&lower),
            std::slice::from_ref(&lower),
            OverlayOp::SymmetricDifference,
        )
        .unwrap();
        assert!(xor.is_empty(), "{xor:?}");
    }

    #[test]
    fn intersection_all_typed_empty_uses_global_min_dimension() {
        let area = z_square(0.0, 0.0, 2.0, 2.0, 3.0);
        let later_disjoint_point = Shape::Point(
            Point::new_axes(10.0, 10.0, ZOrdinate(None), MOrdinate(Some(4.0))).unwrap(),
        );
        let result = overlay_all(
            &[&area, &later_disjoint_point],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        assert!(
            matches!(result, Shape::Empty(GeometryKind::Point, _)),
            "{result:?}"
        );
        assert!(result.has_z() && result.has_m(), "{result:?}");
    }

    #[test]
    fn intersection_refines_contained_and_disjoint_inputs() {
        let outer = square(0.0, 0.0, 10.0, 10.0);
        let inner = square(2.0, 2.0, 4.0, 4.0);
        let contained = outer
            .intersection(&inner, Strictness::Strict)
            .expect("contained intersection");
        assert_topologically_equal(&contained, &inner);

        let disjoint = outer
            .intersection(&square(20.0, 20.0, 22.0, 22.0), Strictness::Strict)
            .expect("disjoint intersection");
        assert!(disjoint.is_empty(), "{disjoint:?}");

        // The inner envelope is strictly inside the outer envelope, but the
        // inner polygon lies in the outer polygon's concave notch. A bounds
        // clone would return the wrong non-empty result here.
        let concave = Shape::Polygon(polygon(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (6.0, 10.0),
            (6.0, 4.0),
            (4.0, 4.0),
            (4.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]));
        let notch = square(4.5, 5.0, 5.5, 9.0);
        let refined = concave
            .intersection(&notch, Strictness::Strict)
            .expect("concave refinement");
        assert!(refined.is_empty(), "{refined:?}");
    }

    #[test]
    fn convex_intersection_fast_path_matches_joint_graph() {
        let left = square(0.0, 0.0, 4.0, 4.0);
        let right = Shape::Polygon(polygon(&[
            (2.0, -1.0),
            (5.0, 2.0),
            (2.0, 5.0),
            (-1.0, 2.0),
            (2.0, -1.0),
        ]));
        let fast = assert_intersection_matches_joint_graph("crossing", &left, &right);
        assert!(matches!(fast, Shape::Polygon(_)), "{fast:?}");
    }

    #[test]
    fn disjoint_convex_intersection_preserves_canonical_empty_fallback() {
        let cases = [
            (
                "overlapping envelopes",
                regular_polygon(0.0, 0.0, 1.0, 256),
                regular_polygon(1.5, 1.5, 1.0, 256),
            ),
            (
                "disjoint envelopes",
                regular_polygon(0.0, 0.0, 1.0, 256),
                regular_polygon(4.0, 0.0, 1.0, 256),
            ),
        ];
        for (name, left, right) in cases {
            // The first pair makes the candidate decline and must fall through
            // the original exact assembly chain. The second is settled before
            // dispatch by the bounds certificate. Both keep POLYGON EMPTY.
            let fast = assert_intersection_matches_joint_graph(name, &left, &right);
            assert!(matches!(
                fast,
                Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XY)
            ));
        }
    }

    #[test]
    fn convex_intersection_defers_contacts_and_concavity_to_joint_graph() {
        let cases = [
            (
                "edge touch",
                square(0.0, 0.0, 2.0, 2.0),
                square(2.0, 0.0, 4.0, 2.0),
            ),
            (
                "vertex touch",
                square(0.0, 0.0, 2.0, 2.0),
                square(2.0, 2.0, 4.0, 4.0),
            ),
            (
                "nested",
                square(0.0, 0.0, 4.0, 4.0),
                square(1.0, 1.0, 2.0, 2.0),
            ),
            (
                "disjoint",
                square(0.0, 0.0, 2.0, 2.0),
                square(4.0, 4.0, 6.0, 6.0),
            ),
            (
                "equal",
                square(0.0, 0.0, 2.0, 2.0),
                square(0.0, 0.0, 2.0, 2.0),
            ),
            (
                "collinear overlap",
                square(0.0, 0.0, 4.0, 4.0),
                Shape::Polygon(polygon(&[
                    (2.0, 0.0),
                    (6.0, 0.0),
                    (6.0, 2.0),
                    (2.0, 2.0),
                    (2.0, 0.0),
                ])),
            ),
            (
                "concave",
                Shape::Polygon(polygon(&[
                    (0.0, 0.0),
                    (6.0, 0.0),
                    (6.0, 1.0),
                    (1.0, 1.0),
                    (1.0, 5.0),
                    (6.0, 5.0),
                    (6.0, 6.0),
                    (0.0, 6.0),
                    (0.0, 0.0),
                ])),
                Shape::Polygon(polygon(&[
                    (0.0, 0.0),
                    (2.0, 0.0),
                    (2.0, 2.0),
                    (5.0, 2.0),
                    (5.0, 5.0),
                    (0.0, 5.0),
                    (0.0, 0.0),
                ])),
            ),
        ];
        for (name, left, right) in cases {
            let _ = assert_intersection_matches_joint_graph(name, &left, &right);
        }

        let concave_left = polygon(&[
            (0.0, 0.0),
            (6.0, 0.0),
            (6.0, 1.0),
            (1.0, 1.0),
            (1.0, 5.0),
            (6.0, 5.0),
            (6.0, 6.0),
            (0.0, 6.0),
            (0.0, 0.0),
        ]);
        let concave_right = polygon(&[
            (0.0, 0.0),
            (2.0, 0.0),
            (2.0, 2.0),
            (5.0, 2.0),
            (5.0, 5.0),
            (0.0, 5.0),
            (0.0, 0.0),
        ]);
        // The area-only binary shortcut is not a valid mixed-dimension
        // intersection oracle: this concave pair also contributes a line.
        let unrestricted = polygon_parts_to_shape(
            binary_areal_overlay(
                std::slice::from_ref(&concave_left),
                std::slice::from_ref(&concave_right),
                OverlayOp::Intersection,
            )
            .unwrap(),
        )
        .unwrap();
        let concave_left_shape = Shape::Polygon(concave_left);
        let concave_right_shape = Shape::Polygon(concave_right);
        let exact = assert_intersection_matches_joint_graph(
            "concave",
            &concave_left_shape,
            &concave_right_shape,
        );
        assert_ne!(wkb(&unrestricted), wkb(&exact));
    }

    #[test]
    fn convex_intersection_rejects_holes_multipolygons_and_invalid_shells() {
        let holed = Shape::Polygon(
            Polygon::new(
                polygon(&[(0.0, 0.0), (6.0, 0.0), (6.0, 6.0), (0.0, 6.0), (0.0, 0.0)]).shell,
                vec![Ring::from_closed(
                    CoordSeq::try_from_points(&[
                        Point::new(2.0, 2.0).unwrap(),
                        Point::new(4.0, 2.0).unwrap(),
                        Point::new(4.0, 4.0).unwrap(),
                        Point::new(2.0, 4.0).unwrap(),
                        Point::new(2.0, 2.0).unwrap(),
                    ])
                    .unwrap(),
                )],
            )
            .unwrap(),
        );
        let multipolygon = Shape::MultiPolygon(
            vec![
                polygon(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]),
                polygon(&[(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0), (4.0, 4.0)]),
            ]
            .into(),
        );
        let invalid = Shape::Polygon(polygon(&[
            (0.0, 0.0),
            (4.0, 4.0),
            (0.0, 4.0),
            (4.0, 0.0),
            (0.0, 0.0),
        ]));
        let cutter = square(1.0, 1.0, 5.0, 5.0);
        for (name, input) in [
            ("hole", holed),
            ("multipolygon", multipolygon),
            ("invalid", invalid),
        ] {
            let _ = assert_intersection_matches_joint_graph(name, &input, &cutter);
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "a table-driven corpus: the cases are data, and splitting the table hides which oracle they all answer to"
    )]
    #[test]
    fn polygon_line_intersection_matches_generic_boundary_and_hole_cases() {
        let holed = Shape::Polygon(
            Polygon::new(
                polygon(&[
                    (0.0, 0.0),
                    (10.0, 0.0),
                    (10.0, 10.0),
                    (0.0, 10.0),
                    (0.0, 0.0),
                ])
                .shell,
                vec![Ring::from_closed(
                    CoordSeq::try_from_points(&[
                        Point::new(4.0, 4.0).unwrap(),
                        Point::new(6.0, 4.0).unwrap(),
                        Point::new(6.0, 6.0).unwrap(),
                        Point::new(4.0, 6.0).unwrap(),
                        Point::new(4.0, 4.0).unwrap(),
                    ])
                    .unwrap(),
                )],
            )
            .unwrap(),
        );
        let multipart = Shape::MultiPolygon(
            vec![
                polygon(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]),
                polygon(&[(4.0, 0.0), (6.0, 0.0), (6.0, 2.0), (4.0, 2.0), (4.0, 0.0)]),
            ]
            .into(),
        );
        let cases = [
            (
                "crossing",
                square(0.0, 0.0, 4.0, 4.0),
                line(&[(-1.0, 2.0), (5.0, 2.0)]),
            ),
            (
                "contained",
                square(0.0, 0.0, 4.0, 4.0),
                line(&[(1.0, 1.0), (3.0, 3.0)]),
            ),
            (
                "exterior",
                square(0.0, 0.0, 4.0, 4.0),
                line(&[(-2.0, 5.0), (-1.0, 5.0)]),
            ),
            (
                "boundary-collinear",
                square(0.0, 0.0, 4.0, 4.0),
                line(&[(-1.0, 0.0), (5.0, 0.0)]),
            ),
            (
                "vertex-only",
                square(0.0, 0.0, 4.0, 4.0),
                line(&[(-1.0, -1.0), (0.0, 0.0)]),
            ),
            (
                "tangent",
                square(0.0, 0.0, 4.0, 4.0),
                line(&[(-1.0, 1.0), (1.0, -1.0)]),
            ),
            ("hole", holed.clone(), line(&[(1.0, 5.0), (9.0, 5.0)])),
            ("hole exterior", holed, line(&[(4.5, 5.0), (5.5, 5.0)])),
            ("multipart", multipart, line(&[(-1.0, 1.0), (7.0, 1.0)])),
        ];

        for (name, polygon, line) in cases {
            let result = assert_intersection_matches_joint_graph(name, &polygon, &line);
            match name {
                "crossing" | "contained" | "boundary-collinear" => {
                    assert!(matches!(result, Shape::LineString(_)), "{name}: {result:?}");
                },
                "exterior" | "hole exterior" => {
                    assert!(
                        matches!(result, Shape::Empty(GeometryKind::LineString, _)),
                        "{name}: {result:?}"
                    );
                },
                "vertex-only" | "tangent" => {
                    assert!(
                        matches!(result, Shape::Point(point) if point.x == 0.0 && point.y == 0.0),
                        "{name}: {result:?}"
                    );
                },
                "hole" | "multipart" => {
                    assert!(
                        matches!(result, Shape::MultiLineString(_)),
                        "{name}: {result:?}"
                    );
                },
                _ => unreachable!("enumerated case"),
            }
        }

        let reverse = assert_intersection_matches_joint_graph(
            "reverse",
            &line(&[(-1.0, 2.0), (5.0, 2.0)]),
            &square(0.0, 0.0, 4.0, 4.0),
        );
        assert!(matches!(reverse, Shape::LineString(_)), "{reverse:?}");

        let multi_line = multiline(&[&[(-1.0, 1.0), (5.0, 1.0)], &[(1.0, 3.0), (3.0, 3.0)], &[
            (-2.0, 9.0),
            (-1.0, 9.0),
        ]]);
        let multi_result = assert_intersection_matches_joint_graph(
            "multiple line components",
            &square(0.0, 0.0, 4.0, 4.0),
            &multi_line,
        );
        assert!(
            matches!(multi_result, Shape::MultiLineString(_)),
            "{multi_result:?}"
        );

        let zm_result = assert_intersection_matches_joint_graph(
            "ZM",
            &zm_square(0.0, 0.0, 4.0, 4.0, 10.0),
            &zm_line((-1.0, 2.0), (5.0, 2.0)),
        );
        assert!(zm_result.has_z() && zm_result.has_m(), "{zm_result:?}");
        assert_all_points_have_z(&zm_result);
        zm_result.for_each_point(|point| assert!(point.m().is_some(), "{zm_result:?}"));

        let invalid = Shape::Polygon(polygon(&[
            (0.0, 0.0),
            (4.0, 4.0),
            (0.0, 4.0),
            (4.0, 0.0),
            (0.0, 0.0),
        ]));
        let _ = assert_intersection_matches_joint_graph(
            "invalid polygon",
            &invalid,
            &line(&[(-1.0, 2.0), (5.0, 2.0)]),
        );
    }

    #[test]
    fn polygon_line_intersection_falls_back_for_ambiguous_support() {
        let polygon = square(0.0, 0.0, 4.0, 4.0);
        let live = line(&[(-1.0, 2.0), (5.0, 2.0)]);
        let collapsed = line(&[(1.0, 1.0), (1.0, 1.0)]);
        let empty = Shape::Empty(GeometryKind::LineString, CoordinateAxes::XY);
        let collection = Shape::geometry_collection(vec![live, point(8.0, 8.0)])
            .expect("test collection is valid");
        let empty_member = Shape::MultiLineString(
            MultiLineString::from_values(CoordinateAxes::XY, vec![
                MultiMember::Empty,
                MultiMember::Value(line_seq(&[(-1.0, 2.0), (5.0, 2.0)])),
            ])
            .unwrap(),
        );

        for (name, line) in [
            ("collapsed", collapsed),
            ("empty", empty),
            ("collection", collection),
            ("empty member", empty_member),
        ] {
            let _ = assert_intersection_matches_joint_graph(name, &polygon, &line);
        }
    }

    #[test]
    fn generic_polygon_intersection_absorbs_retained_boundary_lines() {
        let cases = [
            (
                "rounded shared boundary",
                polygon(&[(0.5, 0.5), (-2.0, 1.5), (-1.5, -1.5), (0.5, 0.5)]),
                polygon(&[(0.0, 1.0), (-1.0, 0.5), (0.0, 0.0), (0.0, 1.0)]),
                // The crossing is exactly (-1/3, 5/6); both ordinates are the
                // nearest binary64 to it.
                polygon(&[
                    (-1.0, 0.5),
                    (0.0, 0.0),
                    (0.0, 0.7),
                    (-0.333_333_333_333_333_3, 0.833_333_333_333_333_4),
                    (-1.0, 0.5),
                ]),
            ),
            (
                "integer shared boundary",
                polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]),
                polygon(&[(1.0, 0.0), (3.0, 0.0), (2.0, 2.0), (1.0, 0.0)]),
                polygon(&[(1.0, 0.0), (3.0, 0.0), (2.0, 2.0), (1.0, 0.0)]),
            ),
            (
                "integer collinear crossing",
                polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]),
                polygon(&[(1.0, 0.0), (3.0, 0.0), (5.0, 2.0), (3.0, 3.0), (1.0, 0.0)]),
                polygon(&[
                    (1.0, 0.0),
                    (3.0, 0.0),
                    (4.0, 1.0),
                    (4.0, 2.5),
                    (3.0, 3.0),
                    (1.0, 0.0),
                ]),
            ),
        ];

        for (name, left, right, expected) in cases {
            let left = Shape::Polygon(left);
            let right = Shape::Polygon(right);
            let expected = Shape::Polygon(expected);
            let result = super::super::nary::binary_intersection_joint_graph(
                &left,
                &right,
                Strictness::Strict,
            )
            .unwrap();
            assert_eq!(wkb(&result), wkb(&expected), "{name}: {result:?}");
            assert!(matches!(result, Shape::Polygon(_)), "{name}: {result:?}");
        }

        let square = square(0.0, 0.0, 4.0, 4.0);
        let partial_boundary = line(&[(1.0, 0.0), (3.0, 0.0)]);
        let result = super::super::nary::binary_intersection_joint_graph(
            &square,
            &partial_boundary,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(wkb(&result), wkb(&partial_boundary), "{result:?}");
        assert!(matches!(result, Shape::LineString(_)), "{result:?}");
    }

    fn assert_point_intersection_matches_generic(
        name: &str,
        left: &Shape,
        right: &Shape,
        strictness: Strictness,
    ) -> Option<Shape> {
        let fast = overlay_all(
            &[left, right],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            strictness,
        );
        let generic = super::super::nary::binary_intersection_joint_graph(left, right, strictness);
        match (fast, generic) {
            (Ok(fast), Ok(generic)) => {
                assert_eq!(wkb(&fast), wkb(&generic), "{name}: {fast:?} != {generic:?}");
                Some(fast)
            },
            (Err(fast), Err(generic)) => {
                assert_eq!(fast.to_string(), generic.to_string(), "{name}");
                None
            },
            (fast, generic) => panic!("{name} diverged: fast={fast:?}, generic={generic:?}"),
        }
    }

    #[test]
    fn scalar_point_intersection_matches_generic_for_line_and_polygon_cases() {
        let line_cases = [
            (
                "line interior",
                point(1.0, 0.0),
                line(&[(0.0, 0.0), (2.0, 0.0)]),
            ),
            (
                "line endpoint",
                point(0.0, 0.0),
                line(&[(0.0, 0.0), (2.0, 0.0)]),
            ),
            (
                "line overlap miss",
                point(1.0, 1.0),
                line(&[(0.0, 0.0), (2.0, 0.0)]),
            ),
            (
                "line disjoint",
                point(10.0, 10.0),
                line(&[(0.0, 0.0), (2.0, 0.0)]),
            ),
            (
                "line repeated vertices",
                point(1.0, 0.0),
                line(&[(0.0, 0.0), (1.0, 0.0), (1.0, 0.0), (2.0, 0.0)]),
            ),
            (
                "line collapsed",
                point(1.0, 1.0),
                line(&[(1.0, 1.0), (1.0, 1.0)]),
            ),
        ];
        for (name, point, line) in line_cases {
            assert_point_intersection_matches_generic(name, &point, &line, Strictness::Strict);
            assert_point_intersection_matches_generic(name, &line, &point, Strictness::Strict);
        }

        let holed = Shape::Polygon(
            Polygon::new(
                polygon(&[(0.0, 0.0), (6.0, 0.0), (6.0, 6.0), (0.0, 6.0), (0.0, 0.0)]).shell,
                vec![Ring::from_closed(
                    CoordSeq::try_from_points(&[
                        Point::new(2.0, 2.0).unwrap(),
                        Point::new(4.0, 2.0).unwrap(),
                        Point::new(4.0, 4.0).unwrap(),
                        Point::new(2.0, 4.0).unwrap(),
                        Point::new(2.0, 2.0).unwrap(),
                    ])
                    .unwrap(),
                )],
            )
            .unwrap(),
        );
        let polygon_cases = [
            (
                "polygon interior",
                point(1.0, 1.0),
                square(0.0, 0.0, 4.0, 4.0),
            ),
            (
                "polygon shell boundary",
                point(0.0, 2.0),
                square(0.0, 0.0, 4.0, 4.0),
            ),
            (
                "polygon shell vertex",
                point(0.0, 0.0),
                square(0.0, 0.0, 4.0, 4.0),
            ),
            (
                "polygon overlap miss",
                point(4.5, 2.0),
                square(0.0, 0.0, 4.0, 4.0),
            ),
            ("polygon hole interior", point(3.0, 3.0), holed.clone()),
            ("polygon hole boundary", point(2.0, 3.0), holed.clone()),
            ("polygon outside hole", point(1.0, 1.0), holed),
        ];
        for (name, point, polygon) in polygon_cases {
            assert_point_intersection_matches_generic(name, &point, &polygon, Strictness::Strict);
            assert_point_intersection_matches_generic(name, &polygon, &point, Strictness::Strict);
        }

        let invalid = Shape::Polygon(polygon(&[
            (0.0, 0.0),
            (4.0, 4.0),
            (0.0, 4.0),
            (4.0, 0.0),
            (0.0, 0.0),
        ]));
        assert_point_intersection_matches_generic(
            "invalid polygon",
            &point(1.0, 1.0),
            &invalid,
            Strictness::Strict,
        );
    }

    #[test]
    fn scalar_point_intersection_reconciles_ordinates_in_both_orders() {
        let xy_line = line(&[(0.0, 0.0), (2.0, 0.0)]);
        let z_line = zm_line((0.0, 0.0), (2.0, 0.0));
        let z_point = Shape::Point(
            Point::new_axes(1.0, 0.0, ZOrdinate(Some(10.0)), MOrdinate(None)).unwrap(),
        );
        let matching_point = Shape::Point(
            Point::new_axes(1.0, 0.0, ZOrdinate(Some(11.0)), MOrdinate(Some(9.0))).unwrap(),
        );
        let xy_point = point(1.0, 0.0);
        for (name, left, right, axes) in [
            ("point Z and line XY", z_point, xy_line, "XYZ"),
            ("point XY and line ZM", xy_point, z_line.clone(), "XYZM"),
        ] {
            for (left, right) in [(&left, &right), (&right, &left)] {
                let result = assert_point_intersection_matches_generic(
                    name,
                    left,
                    right,
                    Strictness::Lenient,
                )
                .expect("ordinate hit");
                assert_eq!(result.coordinate_axes(), axes, "{name}: {result:?}");
            }
        }
        for (left, right) in [(&matching_point, &z_line), (&z_line, &matching_point)] {
            let result = assert_point_intersection_matches_generic(
                "matching point and line ordinates",
                left,
                right,
                Strictness::Strict,
            )
            .expect("matching ordinate result");
            assert_eq!(result.coordinate_axes(), "XYZM", "{result:?}");
        }

        let conflicting = Shape::Point(
            Point::new_axes(1.0, 0.0, ZOrdinate(Some(99.0)), MOrdinate(Some(9.0))).unwrap(),
        );
        for (left, right) in [(&conflicting, &z_line), (&z_line, &conflicting)] {
            assert_point_intersection_matches_generic(
                "strict ordinate conflict",
                left,
                right,
                Strictness::Strict,
            );
            let result = assert_point_intersection_matches_generic(
                "lenient ordinate conflict",
                left,
                right,
                Strictness::Lenient,
            )
            .expect("lenient ordinate result");
            assert_eq!(result.coordinate_axes(), "XYM", "{result:?}");
        }

        let z_polygon = z_square(0.0, 0.0, 4.0, 4.0, 10.0);
        let boundary = point(2.0, 0.0);
        let boundary_result = assert_point_intersection_matches_generic(
            "polygon boundary interpolation",
            &boundary,
            &z_polygon,
            Strictness::Lenient,
        )
        .expect("polygon boundary result");
        assert_eq!(boundary_result.coordinate_axes(), "XYZ");
        let boundary_point = boundary_result.points_vec().unwrap()[0];
        assert_eq!(boundary_point.z(), Some(12.0));

        let interior_point = point(1.0, 1.0);
        let interior_result = assert_point_intersection_matches_generic(
            "polygon interior does not invent Z",
            &interior_point,
            &z_polygon,
            Strictness::Lenient,
        )
        .expect("polygon interior result");
        assert_eq!(interior_result.coordinate_axes(), "XY");
    }

    #[test]
    fn copied_point_intersection_requires_exact_segment_incidence() {
        let z_polygon = z_square(0.0, 0.0, 4.0, 4.0, 10.0);
        let near_interior = point(1e-10, 2.0);
        let lenient = overlay_all(
            &[&near_interior, &z_polygon],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Lenient,
        )
        .unwrap();
        assert_eq!(lenient.coordinate_axes(), "XY", "{lenient:?}");
        let kept = lenient.points_vec().unwrap()[0];
        assert_eq!((kept.x, kept.y), (1e-10, 2.0));
        assert_eq!(kept.z(), None);

        let error = overlay_all(
            &[&near_interior, &z_polygon],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap_err();
        assert!(
            matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::OrdinateDropped(operation))
                    if operation.as_ref() == "intersection"
            ),
            "{error}"
        );

        let boundary = point(2.0, 0.0);
        let boundary_result = overlay_all(
            &[&boundary, &z_polygon],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(boundary_result.coordinate_axes(), "XYZ");
        assert_eq!(boundary_result.points_vec().unwrap()[0].z(), Some(12.0));

        let mixed = Shape::MultiPoint(
            vec![
                Point::new(1e-10, 2.0).unwrap(),
                Point::new(2.0, 0.0).unwrap(),
            ]
            .into(),
        );
        let mixed_lenient = overlay_all(
            &[&mixed, &z_polygon],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Lenient,
        )
        .unwrap();
        assert_eq!(mixed_lenient.coordinate_axes(), "XY", "{mixed_lenient:?}");
        assert_eq!(
            mixed_lenient
                .points_vec()
                .unwrap()
                .into_iter()
                .map(|point| (point.x, point.y, point.z()))
                .collect::<Vec<_>>(),
            vec![(1e-10, 2.0, None), (2.0, 0.0, None)]
        );
        let mixed_strict = overlay_all(
            &[&mixed, &z_polygon],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap_err();
        assert!(
            mixed_strict
                .to_string()
                .contains("intersection cannot represent Z/M"),
            "{mixed_strict}"
        );
    }

    #[test]
    fn multipart_point_intersection_matches_generic_for_lineal_carriers() {
        let live = multiline(&[&[(0.0, 0.0), (2.0, 2.0)], &[(4.0, 0.0), (4.0, 2.0)]]);
        let multipoint = Shape::MultiPoint(
            vec![
                Point::new(4.0, 1.0).unwrap(),
                Point::new(1.0, 1.0).unwrap(),
                Point::new(8.0, 8.0).unwrap(),
                Point::new(1.0, 1.0).unwrap(),
            ]
            .into(),
        );
        let scalar = point(1.0, 1.0);
        for (left, right) in [(&scalar, &live), (&live, &scalar)] {
            assert_point_intersection_matches_generic(
                "point and multiline",
                left,
                right,
                Strictness::Strict,
            );
        }
        for (left, right) in [(&multipoint, &live), (&live, &multipoint)] {
            let result = assert_point_intersection_matches_generic(
                "multipoint and multiline",
                left,
                right,
                Strictness::Strict,
            )
            .expect("line hits");
            assert!(matches!(result, Shape::MultiPoint(_)), "{result:?}");
            assert_eq!(
                result
                    .points_vec()
                    .unwrap()
                    .into_iter()
                    .map(|point| (point.x, point.y))
                    .collect::<Vec<_>>(),
                vec![(1.0, 1.0), (4.0, 1.0)]
            );
        }

        let miss = Shape::MultiPoint(vec![Point::new(1.0, 0.0).unwrap()].into());
        assert_point_intersection_matches_generic(
            "multipoint and multiline miss",
            &miss,
            &live,
            Strictness::Strict,
        );

        let collapsed = Shape::MultiLineString(
            MultiLineString::from_values(CoordinateAxes::XY, vec![
                MultiMember::Value(line_seq(&[(1.0, 1.0), (1.0, 1.0)])),
                MultiMember::Value(line_seq(&[(0.0, 0.0), (2.0, 2.0)])),
            ])
            .unwrap(),
        );
        let empty_member = Shape::MultiLineString(
            MultiLineString::from_values(CoordinateAxes::XY, vec![
                MultiMember::Empty,
                MultiMember::Value(line_seq(&[(0.0, 0.0), (2.0, 2.0)])),
            ])
            .unwrap(),
        );
        for (name, carrier) in [
            ("collapsed member", collapsed),
            ("empty member", empty_member),
        ] {
            assert_point_intersection_matches_generic(
                name,
                &multipoint,
                &carrier,
                Strictness::Strict,
            );
        }
    }

    #[test]
    fn multipart_point_intersection_matches_generic_for_polygonal_carriers() {
        let carrier = Shape::MultiPolygon(
            vec![
                polygon(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]),
                polygon(&[(4.0, 0.0), (6.0, 0.0), (6.0, 2.0), (4.0, 2.0), (4.0, 0.0)]),
            ]
            .into(),
        );
        let points = Shape::MultiPoint(
            vec![
                Point::new(5.0, 1.0).unwrap(),
                Point::new(0.0, 1.0).unwrap(),
                Point::new(1.0, 1.0).unwrap(),
                Point::new(3.0, 1.0).unwrap(),
                Point::new(1.0, 1.0).unwrap(),
            ]
            .into(),
        );
        let scalar = point(1.0, 1.0);
        for (left, right) in [(&scalar, &carrier), (&carrier, &scalar)] {
            assert_point_intersection_matches_generic(
                "point and multipolygon",
                left,
                right,
                Strictness::Strict,
            );
        }
        for (left, right) in [(&points, &carrier), (&carrier, &points)] {
            let result = assert_point_intersection_matches_generic(
                "multipoint and multipolygon",
                left,
                right,
                Strictness::Strict,
            )
            .expect("polygon hits");
            assert!(matches!(result, Shape::MultiPoint(_)), "{result:?}");
            assert_eq!(
                result
                    .points_vec()
                    .unwrap()
                    .into_iter()
                    .map(|point| (point.x, point.y))
                    .collect::<Vec<_>>(),
                vec![(0.0, 1.0), (1.0, 1.0), (5.0, 1.0)]
            );
        }

        let hole_polygon = Polygon::new(
            polygon(&[(0.0, 0.0), (6.0, 0.0), (6.0, 6.0), (0.0, 6.0), (0.0, 0.0)]).shell,
            vec![Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new(2.0, 2.0).unwrap(),
                    Point::new(4.0, 2.0).unwrap(),
                    Point::new(4.0, 4.0).unwrap(),
                    Point::new(2.0, 4.0).unwrap(),
                    Point::new(2.0, 2.0).unwrap(),
                ])
                .unwrap(),
            )],
        )
        .unwrap();
        let hole = Shape::MultiPolygon(
            MultiPolygon::from_dense(vec![hole_polygon])
                .unwrap()
                .unwrap(),
        );
        let hole_points = Shape::MultiPoint(
            vec![
                Point::new(1.0, 1.0).unwrap(),
                Point::new(2.0, 3.0).unwrap(),
                Point::new(3.0, 3.0).unwrap(),
            ]
            .into(),
        );
        for (left, right) in [(&hole_points, &hole), (&hole, &hole_points)] {
            assert_point_intersection_matches_generic(
                "multipoint and holed multipolygon",
                left,
                right,
                Strictness::Strict,
            );
        }
    }

    #[test]
    fn multipart_point_intersection_falls_back_for_ambiguous_polygonal_carriers() {
        let points = Shape::MultiPoint(
            vec![Point::new(1.0, 1.0).unwrap(), Point::new(5.0, 5.0).unwrap()].into(),
        );
        let empty_member = Shape::MultiPolygon(
            MultiPolygon::from_values(CoordinateAxes::XY, vec![
                MultiMember::Empty,
                MultiMember::Value(polygon(&[
                    (0.0, 0.0),
                    (2.0, 0.0),
                    (2.0, 2.0),
                    (0.0, 2.0),
                    (0.0, 0.0),
                ])),
            ])
            .unwrap(),
        );
        assert_point_intersection_matches_generic(
            "multipoint and multipolygon empty member",
            &points,
            &empty_member,
            Strictness::Strict,
        );

        let overlapping = Shape::MultiPolygon(
            vec![
                polygon(&[(0.0, 0.0), (3.0, 0.0), (3.0, 3.0), (0.0, 3.0), (0.0, 0.0)]),
                polygon(&[(2.0, 2.0), (4.0, 2.0), (4.0, 4.0), (2.0, 4.0), (2.0, 2.0)]),
            ]
            .into(),
        );
        assert_point_intersection_matches_generic(
            "multipoint and overlapping invalid multipolygon",
            &points,
            &overlapping,
            Strictness::Strict,
        );
    }

    #[test]
    fn multipart_point_intersection_reconciles_ordinates_and_extremes() {
        let z_points = Shape::MultiPoint(
            vec![
                Point::new_axes(1.0, 0.0, ZOrdinate(Some(10.0)), MOrdinate(None)).unwrap(),
                Point::new_axes(1.0, 0.0, ZOrdinate(Some(11.0)), MOrdinate(None)).unwrap(),
            ]
            .into(),
        );
        let xy_line = line(&[(0.0, 0.0), (2.0, 0.0)]);
        assert_point_intersection_matches_generic(
            "strict duplicate ordinate conflict",
            &z_points,
            &xy_line,
            Strictness::Strict,
        );
        let lenient = assert_point_intersection_matches_generic(
            "lenient duplicate ordinate conflict",
            &z_points,
            &xy_line,
            Strictness::Lenient,
        )
        .expect("lenient duplicate result");
        assert_eq!(lenient.coordinate_axes(), "XY");

        let points = Shape::MultiPoint(vec![Point::new(2.0, 0.0).unwrap()].into());
        let z_polygon = z_square(0.0, 0.0, 4.0, 4.0, 10.0);
        let result = assert_point_intersection_matches_generic(
            "multipoint polygon boundary interpolation",
            &points,
            &z_polygon,
            Strictness::Strict,
        )
        .expect("interpolated boundary");
        assert_eq!(result.coordinate_axes(), "XYZ");
        assert_eq!(result.points_vec().unwrap()[0].z(), Some(12.0));
        let z_multipolygon = match &z_polygon {
            Shape::Polygon(polygon) => Shape::MultiPolygon(vec![polygon.clone()].into()),
            _ => unreachable!("z_square returns a polygon"),
        };
        let scalar = point(2.0, 0.0);
        let scalar_result = assert_point_intersection_matches_generic(
            "point multipolygon boundary interpolation",
            &scalar,
            &z_multipolygon,
            Strictness::Strict,
        )
        .expect("scalar multipolygon boundary");
        assert_eq!(scalar_result.coordinate_axes(), "XYZ");
        assert_eq!(scalar_result.points_vec().unwrap()[0].z(), Some(12.0));

        let repeated_line = Shape::MultiLineString(
            MultiLineString::from_values(CoordinateAxes::XYZ, vec![MultiMember::Value(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_axes(0.0, 0.0, ZOrdinate(Some(1.0)), MOrdinate(None)).unwrap(),
                        Point::new_axes(0.0, 0.0, ZOrdinate(Some(99.0)), MOrdinate(None)).unwrap(),
                        Point::new_axes(2.0, 0.0, ZOrdinate(Some(2.0)), MOrdinate(None)).unwrap(),
                    ])
                    .unwrap(),
                )
                .expect("repeated line"),
            )])
            .unwrap(),
        );
        let repeated_point = Shape::MultiPoint(vec![Point::new(0.0, 0.0).unwrap()].into());
        let repeated_result = assert_point_intersection_matches_generic(
            "repeated zero-length source edge",
            &repeated_point,
            &repeated_line,
            Strictness::Strict,
        )
        .expect("repeated edge result");
        assert_eq!(repeated_result.points_vec().unwrap()[0].z(), Some(99.0));

        let extreme = Shape::MultiPoint(vec![Point::new(0.0, 0.0).unwrap()].into());
        let extreme_line = line(&[(-1.0e150, -1.0e150), (1.0e150, 1.0e150)]);
        for (left, right) in [(&extreme, &extreme_line), (&extreme_line, &extreme)] {
            assert_point_intersection_matches_generic(
                "extreme multipoint line",
                left,
                right,
                Strictness::Strict,
            );
        }
    }

    #[test]
    fn intersection_all_matches_left_fold_for_permutations() {
        let shapes = [
            square(0.0, 0.0, 4.0, 4.0),
            square(1.0, -1.0, 5.0, 3.0),
            square(2.0, 1.0, 6.0, 5.0),
        ];
        for order in [[0, 1, 2], [2, 0, 1], [1, 2, 0], [2, 1, 0]] {
            let inputs = order
                .iter()
                .map(|&index| &shapes[index])
                .collect::<Vec<_>>();
            let canonical = overlay_all(
                &inputs,
                crate::geometry::TopologyDomain::Cartesian,
                OverlayOp::Intersection,
                Strictness::Strict,
            )
            .unwrap();
            let folded = manual_left_fold(&inputs, OverlayOp::Intersection);
            assert_topologically_equal(&canonical, &folded);
        }
    }

    #[test]
    fn intersection_all_mixed_dimension_narrows_like_left_fold() {
        let area = square(0.0, 0.0, 4.0, 4.0);
        let crossing = line(&[(-1.0, 2.0), (5.0, 2.0)]);
        let marker = point(2.0, 2.0);
        let inputs = [&area, &crossing, &marker];
        let canonical = overlay_all(
            &inputs,
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        let folded = manual_left_fold(&inputs, OverlayOp::Intersection);
        assert!(matches!(canonical, Shape::Point(_)), "{canonical:?}");
        assert_topologically_equal(&canonical, &folded);
    }

    #[test]
    fn intersection_all_singleton_is_faithful_and_keeps_ordinates() {
        let input = Shape::geometry_collection(vec![
            Shape::Point(Point::new_axes(1.0, 2.0, ZOrdinate(Some(3.0)), MOrdinate(None)).unwrap()),
            Shape::Point(Point::new_axes(4.0, 5.0, ZOrdinate(None), MOrdinate(Some(6.0))).unwrap()),
        ])
        .expect("test collection is valid");
        // A single operand intersects to itself — faithful, so both policies
        // keep Z/M (force_2d is the way to flatten).
        for strictness in [Strictness::Lenient, Strictness::Strict] {
            let kept = overlay_all(
                &[&input],
                crate::geometry::TopologyDomain::Cartesian,
                OverlayOp::Intersection,
                strictness,
            )
            .unwrap();
            assert_eq!(kept, input);
            assert!(kept.has_z() && kept.has_m(), "{kept:?}");
        }
    }

    #[test]
    fn intersection_all_carries_z_once_from_original_inputs() {
        let first = z_square(0.0, 0.0, 4.0, 4.0, 10.0);
        let second = z_square(1.0, -1.0, 5.0, 3.0, 10.0);
        let third = z_square(2.0, 1.0, 6.0, 5.0, 10.0);
        let result = overlay_all(
            &[&first, &second, &third],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Lenient,
        )
        .unwrap();
        assert!(result.has_z(), "{result:?}");
        assert_all_points_have_z(&result);
    }

    #[test]
    fn intersection_all_unresolved_survivor_degrades_under_auto_raises_under_strict() {
        let source = z_square(0.0, 0.0, 10.0, 10.0, 10.0);
        let interior = square(2.0, 2.0, 4.0, 4.0);
        // 'auto' (strict=false): an interior survivor vertex cannot source Z, so
        // the result degrades to 2D rather than raising.
        let auto = overlay_all(
            &[&source, &interior],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Lenient,
        )
        .unwrap();
        assert_topologically_equal(&auto, &interior);
        assert!(!auto.has_z() && !auto.has_m(), "{auto:?}");
        // 'error' (strict=true) raises on the unsourceable survivor.
        let error = overlay_all(
            &[&source, &interior],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        );
        assert!(error.is_err(), "{error:?}");
    }

    #[test]
    fn symmetric_difference_all_permutation_invariant_for_polygons() {
        let shapes = [
            square(0.0, 0.0, 3.0, 3.0),
            square(1.0, 1.0, 4.0, 4.0),
            square(2.0, -1.0, 5.0, 2.0),
        ];
        let baseline = overlay_all(
            &[&shapes[0], &shapes[1], &shapes[2]],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        for order in [[2, 1, 0], [1, 0, 2], [0, 2, 1]] {
            let inputs = order
                .iter()
                .map(|&index| &shapes[index])
                .collect::<Vec<_>>();
            let canonical = overlay_all(
                &inputs,
                crate::geometry::TopologyDomain::Cartesian,
                OverlayOp::SymmetricDifference,
                Strictness::Strict,
            )
            .unwrap();
            let folded = manual_left_fold(&inputs, OverlayOp::SymmetricDifference);
            assert_topologically_equal(&canonical, &baseline);
            assert_topologically_equal(&canonical, &folded);
        }
    }

    #[test]
    fn symmetric_difference_all_mixed_polygon_duplicates_leave_contained_line() {
        let polygon = square(0.0, 0.0, 4.0, 4.0);
        let contained_line = line(&[(1.0, 1.0), (3.0, 3.0)]);
        let result = overlay_all(
            &[&polygon, &polygon, &contained_line],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(result, contained_line);
    }

    #[test]
    fn symmetric_difference_all_mixed_collection_sees_non_empty_component_dimensions() {
        let polygon = square(0.0, 0.0, 4.0, 4.0);
        let contained_line = line(&[(1.0, 1.0), (3.0, 3.0)]);
        let collection =
            Shape::geometry_collection(vec![contained_line.clone(), Shape::empty_polygon()])
                .expect("test collection is valid");
        let result = overlay_all(
            &[&polygon, &polygon, &collection],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(result, contained_line);
    }

    #[test]
    fn symmetric_difference_all_mixed_line_duplicates_leave_point() {
        let line = line(&[(-1.0, 0.0), (1.0, 0.0)]);
        let point_on_line = point(0.0, 0.0);
        let result = overlay_all(
            &[&line, &line, &point_on_line],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(result, point_on_line);
    }

    #[test]
    fn symmetric_difference_all_same_dimension_polygons_match_left_fold() {
        let shapes = [
            square(0.0, 0.0, 2.0, 2.0),
            square(0.0, 0.0, 2.0, 2.0),
            square(3.0, 3.0, 5.0, 5.0),
            square(3.0, 3.0, 5.0, 5.0),
        ];
        let inputs = [&shapes[0], &shapes[1], &shapes[2], &shapes[3]];
        let result = overlay_all(
            &inputs,
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        let folded = manual_left_fold(&inputs, OverlayOp::SymmetricDifference);
        assert_topologically_equal(&result, &folded);
        assert!(
            matches!(result, Shape::Empty(GeometryKind::Polygon, _)),
            "{result:?}"
        );
    }

    #[test]
    fn symmetric_difference_all_same_dimension_collections_are_canonical() {
        let duplicate =
            Shape::geometry_collection(vec![square(0.0, 0.0, 2.0, 2.0), Shape::empty_polygon()])
                .expect("test collection is valid");
        let survivor =
            Shape::geometry_collection(vec![square(3.0, 3.0, 5.0, 5.0), Shape::empty_polygon()])
                .expect("test collection is valid");
        let result = overlay_all(
            &[&duplicate, &duplicate, &survivor],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        assert_topologically_equal(&result, &survivor);
    }

    #[test]
    fn symmetric_difference_all_duplicate_polygons_cancel_to_polygon_empty() {
        let polygon = square(0.0, 0.0, 2.0, 2.0);
        let result = overlay_all(
            &[&polygon, &polygon],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        assert!(
            matches!(result, Shape::Empty(GeometryKind::Polygon, _)),
            "{result:?}"
        );
    }

    #[test]
    fn symmetric_difference_all_duplicate_pair_cancels_before_survivor() {
        let duplicate = square(0.0, 0.0, 2.0, 2.0);
        let survivor = square(1.0, 1.0, 3.0, 3.0);
        let result = overlay_all(
            &[&duplicate, &duplicate, &survivor],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(result, survivor);
    }

    #[test]
    fn symmetric_difference_all_disjoint_inputs_match_union() {
        let shapes = [square(0.0, 0.0, 1.0, 1.0), square(3.0, 3.0, 4.0, 4.0)];
        let canonical = overlay_all(
            &[&shapes[0], &shapes[1]],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        let union = Shape::union_all(&shapes, Strictness::Strict).unwrap();
        assert_topologically_equal(&canonical, &union);
    }

    #[test]
    fn disjoint_bounds_certificates_match_joint_graph_assembly() {
        let polygons = [square(0.0, 0.0, 1.0, 1.0), square(3.0, 3.0, 4.0, 4.0)];
        let mixed = [
            square(0.0, 0.0, 1.0, 1.0),
            line(&[(3.0, 3.0), (4.0, 3.0)]),
            point(6.0, 6.0),
        ];
        let multipart = Shape::geometry_collection(vec![
            Shape::MultiPolygon(
                vec![polygon(&[
                    (0.0, 0.0),
                    (1.0, 0.0),
                    (1.0, 1.0),
                    (0.0, 1.0),
                    (0.0, 0.0),
                ])]
                .into(),
            ),
            Shape::MultiLineString(
                vec![
                    LineSeq::try_new(
                        CoordSeq::try_from_points(&[
                            Point::new(2.0, 2.0).unwrap(),
                            Point::new(3.0, 2.0).unwrap(),
                        ])
                        .unwrap(),
                    )
                    .unwrap(),
                ]
                .into(),
            ),
        ])
        .expect("test collection is valid");
        let remote = square(6.0, 6.0, 7.0, 7.0);
        let empty_z = Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XYZ);

        for op in [
            OverlayOp::Union,
            OverlayOp::Difference,
            OverlayOp::SymmetricDifference,
        ] {
            assert_disjoint_certificate_matches_graph(&[&polygons[0], &polygons[1]], op);
            assert_disjoint_certificate_matches_graph(&[&mixed[0], &mixed[1], &mixed[2]], op);
            assert_disjoint_certificate_matches_graph(&[&multipart, &remote], op);
            assert_disjoint_results_match(&[&polygons[0], &empty_z, &polygons[1]], op);
        }
    }

    #[test]
    fn disjoint_ordinate_axes_match_forced_graph_wkb() {
        let inputs = [
            (
                "XYZ",
                z_square(0.0, 0.0, 1.0, 1.0, 10.0),
                z_square(3.0, 3.0, 4.0, 4.0, 20.0),
            ),
            (
                "XYM",
                m_square(0.0, 0.0, 1.0, 1.0, 10.0),
                m_square(3.0, 3.0, 4.0, 4.0, 20.0),
            ),
            (
                "XYZM",
                zm_square(0.0, 0.0, 1.0, 1.0, 10.0),
                zm_square(3.0, 3.0, 4.0, 4.0, 20.0),
            ),
        ];
        for (axes, left, right) in inputs {
            assert_eq!(left.coordinate_axes(), axes);
            assert_eq!(right.coordinate_axes(), axes);
            for op in [OverlayOp::Union, OverlayOp::SymmetricDifference] {
                assert_disjoint_certificate_matches_graph(&[&left, &right], op);
            }
        }
    }

    #[test]
    fn disjoint_invalid_zm_area_and_line_use_the_same_ordinate_path() {
        let polygon = invalid_zm_bowtie();
        let line = zm_line((10.0, 0.0), (11.0, 1.0));
        for op in [OverlayOp::Union, OverlayOp::SymmetricDifference] {
            let certified = overlay_all(
                &[&polygon, &line],
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            )
            .expect("certified mixed ZM overlay");
            let forced = super::super::nary::overlay_all_without_bounds_certificates(
                &[&polygon, &line],
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            )
            .expect("forced mixed ZM overlay");
            assert_eq!(wkb(&certified), wkb(&forced), "{}", op.name());
        }
    }

    #[test]
    fn empty_ordinate_disjoint_paths_return_the_same_error() {
        let polygon = square(0.0, 0.0, 1.0, 1.0);
        let empty_z = Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XYZ);
        let remote = square(3.0, 3.0, 4.0, 4.0);
        for op in [OverlayOp::Union, OverlayOp::SymmetricDifference] {
            let certified = overlay_all(
                &[&polygon, &empty_z, &remote],
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            );
            let forced = super::super::nary::overlay_all_without_bounds_certificates(
                &[&polygon, &empty_z, &remote],
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            );
            let (certified, forced) = (certified.unwrap_err(), forced.unwrap_err());
            assert_eq!(certified.to_string(), forced.to_string(), "{}", op.name());
            assert!(certified.to_string().contains("cannot represent Z/M"));
        }
    }

    #[test]
    fn symmetric_difference_all_carries_resolvable_ordinates() {
        let first = zm_square(0.0, 0.0, 1.0, 1.0, 10.0);
        let second = zm_square(3.0, 3.0, 4.0, 4.0, 20.0);
        // Disjoint symdiff is faithful — both policies carry Z/M.
        for strictness in [Strictness::Lenient, Strictness::Strict] {
            let kept = overlay_all(
                &[&first, &second],
                crate::geometry::TopologyDomain::Cartesian,
                OverlayOp::SymmetricDifference,
                strictness,
            )
            .unwrap();
            assert!(kept.has_z() && kept.has_m(), "{kept:?}");
        }
        // Overlapping symdiff: seam crossings lie on the uniform-Z/M edges, so
        // they resolve and Z/M carries through under 'auto'.
        let overlap = zm_square(0.5, 0.5, 1.5, 1.5, 10.0);
        let carried = overlay_all(
            &[&first, &overlap],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Lenient,
        )
        .unwrap();
        assert!(carried.has_z() && carried.has_m(), "{carried:?}");
    }

    #[test]
    fn overlay_resolves_conflicting_z_and_equal_m_independently() {
        let point =
            |z, m| Point::new_axes(0.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap();
        let left = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    point(1.0, 7.0),
                    Point::new_axes(2.0, 0.0, ZOrdinate(Some(3.0)), MOrdinate(Some(9.0))).unwrap(),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        );
        let right = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    point(11.0, 7.0),
                    Point::new_axes(2.0, 0.0, ZOrdinate(Some(13.0)), MOrdinate(Some(9.0))).unwrap(),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        );

        let result = left.union(&right, Strictness::Lenient).unwrap();
        assert_eq!(result.coordinate_axes(), "XYM");
        result.for_each_point(|point| {
            assert_eq!(point.m(), Some(if point.x == 0.0 { 7.0 } else { 9.0 }));
            assert!(point.z().is_none());
        });
    }

    #[test]
    fn overlay_preserves_signed_zero_and_drops_absent_difference_axis() {
        let left = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_axes(0.0, 0.0, ZOrdinate(Some(-0.0)), MOrdinate(None)).unwrap(),
                    Point::new_axes(4.0, 0.0, ZOrdinate(Some(4.0)), MOrdinate(None)).unwrap(),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        );
        let xy = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new(0.0, 0.0).unwrap(),
                    Point::new(4.0, 0.0).unwrap(),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        );
        let intersection = left.intersection(&xy, Strictness::Lenient).unwrap();
        assert_eq!(intersection.coordinate_axes(), "XYZ");
        let first = intersection
            .points_vec()
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(first.z().unwrap().to_bits(), (-0.0_f64).to_bits());
        assert!(first.m().is_none());

        let measured = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_axes(2.0, -1.0, ZOrdinate(None), MOrdinate(Some(2.0))).unwrap(),
                    Point::new_axes(2.0, 1.0, ZOrdinate(None), MOrdinate(Some(4.0))).unwrap(),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        );
        let difference = left.difference(&measured, Strictness::Lenient).unwrap();
        assert_eq!(difference.coordinate_axes(), "XYZ");
        difference.for_each_point(|point| assert!(point.m().is_none()));
    }

    #[test]
    fn empty_overlay_declares_union_axes_without_coordinates() {
        let z = z_square(0.0, 0.0, 1.0, 1.0, 1.0);
        let m = zm_square(2.0, 2.0, 3.0, 3.0, 1.0);
        let result = z.intersection(&m, Strictness::Lenient).unwrap();
        assert!(matches!(
            result,
            Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XYZM)
        ));
    }

    #[test]
    fn commutative_overlay_operand_reversal_keeps_axes_and_ordinates() {
        let left = zm_square(0.0, 0.0, 2.0, 2.0, 10.0);
        let right = zm_square(1.0, 1.0, 3.0, 3.0, 10.0);
        let signature = |shape: &Shape| {
            let mut points = shape
                .points_vec()
                .unwrap()
                .into_iter()
                .map(|point| {
                    (
                        point.x.to_bits(),
                        point.y.to_bits(),
                        point.z().map(f64::to_bits),
                        point.m().map(f64::to_bits),
                    )
                })
                .collect::<Vec<_>>();
            points.sort_unstable();
            points.dedup();
            (shape.coordinate_axes(), points)
        };
        for op in [
            OverlayOp::Union,
            OverlayOp::Intersection,
            OverlayOp::SymmetricDifference,
        ] {
            let forward = op.apply(&left, &right, Strictness::Lenient).unwrap();
            let reversed = op.apply(&right, &left, Strictness::Lenient).unwrap();
            assert_eq!(signature(&forward), signature(&reversed), "{}", op.name());
        }
    }

    #[test]
    fn line_merge_drops_only_conflicting_shared_node_axes() {
        let line = |z, m, end| {
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_axes(0.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
                    Point::new_axes(end, 0.0, ZOrdinate(Some(z + 1.0)), MOrdinate(Some(m + 1.0)))
                        .unwrap(),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices")
        };
        let lines = MultiLineString::from_values(CoordinateAxes::XYZM, vec![
            MultiMember::Value(line(1.0, 10.0, 1.0)),
            MultiMember::Value(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_axes(1.0, 0.0, ZOrdinate(Some(99.0)), MOrdinate(Some(11.0)))
                            .unwrap(),
                        Point::new_axes(2.0, 0.0, ZOrdinate(Some(100.0)), MOrdinate(Some(12.0)))
                            .unwrap(),
                    ])
                    .unwrap(),
                )
                .expect("fixture line has two vertices"),
            ),
        ])
        .unwrap();
        let merged = Shape::MultiLineString(lines).line_merge().unwrap();
        assert_eq!(merged.coordinate_axes(), "XYM");
        merged.for_each_point(|point| assert!(point.z().is_none()));
        merged.for_each_point(|point| assert!(point.m().is_some()));
    }

    #[test]
    fn line_only_overlay_matches_generic_assembly_for_line_cases() {
        let cases = vec![
            vec![
                line(&[(0.0, 0.0), (4.0, 0.0)]),
                line(&[(1.0, 0.0), (3.0, 0.0)]),
            ],
            vec![
                line(&[(0.0, 0.0), (4.0, 0.0)]),
                line(&[(3.0, 0.0), (1.0, 0.0)]),
            ],
            vec![
                multiline(&[&[(0.0, 0.0), (4.0, 0.0)], &[(0.0, 0.0), (4.0, 0.0)]]),
                line(&[(1.0, 0.0), (3.0, 0.0)]),
            ],
            vec![
                line(&[(0.0, 0.0), (2.0, 0.0)]),
                line(&[(2.0, 0.0), (2.0, 2.0)]),
            ],
            vec![
                line(&[(0.0, 0.0), (2.0, 2.0)]),
                line(&[(0.0, 2.0), (2.0, 0.0)]),
            ],
            vec![
                line(&[(0.0, 0.0), (1.0, 0.0)]),
                line(&[(3.0, 0.0), (4.0, 0.0)]),
            ],
            // Overlapping bounds, no contact: the no-interaction lane.
            vec![
                line(&[(0.0, 0.0), (2.0, 1.0), (4.0, 2.0)]),
                line(&[(0.0, 1.0), (2.0, 2.0), (4.0, 3.0)]),
            ],
            // Same, with a repeated vertex the dissolve would drop and a part
            // that ends where the next begins.
            vec![
                multiline(&[&[(0.0, 0.0), (1.0, 1.0), (1.0, 1.0), (2.0, 1.0)], &[
                    (2.0, 1.0),
                    (3.0, 3.0),
                ]]),
                line(&[(0.0, 2.0), (3.0, 4.0)]),
            ],
            vec![
                line(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]),
                line(&[
                    (1.0, -1.0),
                    (3.0, -1.0),
                    (3.0, 1.0),
                    (1.0, 1.0),
                    (1.0, -1.0),
                ]),
            ],
            vec![
                multiline(&[&[(0.0, 0.0), (2.0, 0.0)], &[(10.0, 0.0), (12.0, 0.0)]]),
                multiline(&[&[(1.0, 0.0), (3.0, 0.0)], &[(11.0, 0.0), (13.0, 0.0)]]),
            ],
        ];

        for case in cases {
            let inputs = case.iter().collect::<Vec<_>>();
            for op in EVERY_OP {
                assert_line_overlay_matches_generic(&inputs, op);
            }
        }

        let nary = [
            line(&[(0.0, 0.0), (4.0, 0.0)]),
            line(&[(1.0, 0.0), (3.0, 0.0)]),
            line(&[(2.0, 0.0), (5.0, 0.0)]),
        ];
        let inputs = nary.iter().collect::<Vec<_>>();
        for op in EVERY_OP {
            assert_line_overlay_matches_generic(&inputs, op);
        }

        for scale in [2.0_f64.powi(-600), 2.0_f64.powi(600)] {
            let cuts = [3.0, 1.0, 2.0]
                .map(|multiple| line(&[(multiple * scale, -scale), (multiple * scale, scale)]));
            let source = line(&[(0.0, 0.0), (4.0 * scale, 0.0)]);
            let inputs = std::iter::once(&source)
                .chain(cuts.iter())
                .collect::<Vec<_>>();
            for op in EVERY_OP {
                assert_line_overlay_matches_generic(&inputs, op);
            }
        }
    }

    const EVERY_OP: [OverlayOp; 4] = [
        OverlayOp::Union,
        OverlayOp::Intersection,
        OverlayOp::Difference,
        OverlayOp::SymmetricDifference,
    ];

    #[test]
    fn line_only_intersection_drops_contact_points_on_shared_edges() {
        let left = line(&[(0.0, 0.0), (4.0, 0.0)]);
        let right = line(&[(1.0, 0.0), (3.0, 0.0)]);
        let result = overlay_all(
            &[&left, &right],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        assert!(matches!(result, Shape::LineString(_)), "{result:?}");
        assert_eq!(result.points_vec().unwrap().len(), 2, "{result:?}");
        assert_line_overlay_matches_generic(&[&left, &right], OverlayOp::Intersection);
    }

    #[test]
    fn line_only_overlay_preserves_zm_and_operand_order() {
        let left = zm_line((0.0, 0.0), (2.0, 2.0));
        let right = zm_line((0.0, 2.0), (2.0, 0.0));
        for op in EVERY_OP {
            let forward = overlay_all(
                &[&left, &right],
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            )
            .unwrap();
            let reverse = overlay_all(
                &[&right, &left],
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            )
            .unwrap();
            if op != OverlayOp::Difference {
                assert_eq!(wkb(&forward), wkb(&reverse), "{}", op.name());
            }
            assert!(forward.has_z() && forward.has_m(), "{forward:?}");
            forward.for_each_point(|point| {
                assert!(point.z().is_some(), "{forward:?}");
                assert!(point.m().is_some(), "{forward:?}");
            });
            assert_line_overlay_matches_generic(&[&left, &right], op);
        }
    }

    #[test]
    fn line_only_intersection_preserves_zm_for_overlapping_bounds_without_contact() {
        let cases = [(
            zm_line((0.0, 0.0), (4.0, 4.0)),
            zm_line((0.0, 1.0), (4.0, 5.0)),
        )];
        for (left, right) in cases {
            for strictness in [Strictness::Strict, Strictness::Lenient] {
                let fast = overlay_all(
                    &[&left, &right],
                    crate::geometry::TopologyDomain::Cartesian,
                    OverlayOp::Intersection,
                    strictness,
                )
                .unwrap();
                let generic = super::super::nary::overlay_all_line_oracle(
                    &[&left, &right],
                    OverlayOp::Intersection,
                    strictness,
                )
                .unwrap();
                assert_eq!(wkb(&fast), wkb(&generic), "{strictness:?}");
                assert!(
                    matches!(
                        fast,
                        Shape::Empty(GeometryKind::LineString, CoordinateAxes::XYZM)
                    ),
                    "{fast:?}"
                );
            }
        }
    }

    #[test]
    fn line_only_guard_falls_back_for_non_line_and_collapsed_support() {
        let line_shape = line(&[(0.0, 0.0), (2.0, 0.0)]);
        let collapsed = line(&[(4.0, 4.0), (4.0, 4.0)]);
        let empty = Shape::Empty(GeometryKind::LineString, CoordinateAxes::XY);
        let polygon = square(4.0, 4.0, 6.0, 6.0);
        let mixed = Shape::geometry_collection(vec![line_shape.clone(), polygon.clone()])
            .expect("test collection is valid");
        let empty_member = Shape::MultiLineString(
            MultiLineString::from_values(CoordinateAxes::XY, vec![
                MultiMember::Empty,
                MultiMember::Value(line_seq(&[(8.0, 0.0), (9.0, 0.0)])),
            ])
            .unwrap(),
        );

        assert!(super::super::nary::nondegenerate_line_only(&line_shape));
        for input in [&collapsed, &empty, &polygon, &mixed, &empty_member] {
            assert!(
                !super::super::nary::nondegenerate_line_only(input),
                "{input:?}"
            );
        }

        let point_off_line = point(0.0, 1.0);
        let point_on_line = point(0.0, 0.0);
        assert_line_overlay_matches_generic(&[&line_shape, &point_off_line], OverlayOp::Union);
        assert_line_overlay_matches_generic(
            &[&line_shape, &point_on_line],
            OverlayOp::Intersection,
        );
        assert_line_overlay_matches_generic(&[&line_shape, &collapsed], OverlayOp::Union);
        assert_line_overlay_matches_generic(&[&line_shape, &collapsed], OverlayOp::Intersection);
        assert_line_overlay_matches_generic(&[&line_shape, &empty], OverlayOp::Union);
        assert_line_overlay_matches_generic(&[&line_shape, &polygon], OverlayOp::Union);
        assert_line_overlay_matches_generic(&[&line_shape, &mixed], OverlayOp::Union);
        assert_line_overlay_matches_generic(&[&line_shape, &empty_member], OverlayOp::Union);

        let point_result = line_shape
            .intersection(&point_on_line, Strictness::Strict)
            .unwrap();
        assert_eq!(point_result, point_on_line);
    }

    #[test]
    fn disjoint_line_bounds_keep_certificate_before_provenance() {
        let count = 128;
        let left = disjoint_multiline(count, 0.0);
        let right = disjoint_multiline(count, 10_000.0);

        let union = overlay_all(
            &[&left, &right],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Union,
            Strictness::Strict,
        )
        .unwrap();
        assert!(matches!(union, Shape::MultiLineString(_)), "{union:?}");
        assert_eq!(union.part_count(), count * 2);
        assert_line_overlay_matches_generic(&[&left, &right], OverlayOp::Union);

        let intersection = overlay_all(
            &[&left, &right],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        assert!(
            matches!(intersection, Shape::Empty(GeometryKind::LineString, _)),
            "{intersection:?}"
        );
        assert_line_overlay_matches_generic(&[&left, &right], OverlayOp::Intersection);
    }

    #[test]
    fn aggregate_overlay_wkb_is_permutation_invariant() {
        let shapes = [
            square(0.0, 0.0, 3.0, 3.0),
            square(1.0, 1.0, 4.0, 4.0),
            square(2.0, -1.0, 5.0, 2.0),
        ];
        for op in [
            OverlayOp::Union,
            OverlayOp::Intersection,
            OverlayOp::SymmetricDifference,
        ] {
            let baseline = wkb(&overlay_all(
                &[&shapes[0], &shapes[1], &shapes[2]],
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            )
            .unwrap());
            for order in [[2, 1, 0], [1, 0, 2], [0, 2, 1]] {
                let inputs = order
                    .iter()
                    .map(|&index| &shapes[index])
                    .collect::<Vec<_>>();
                assert_eq!(
                    wkb(&overlay_all(
                        &inputs,
                        crate::geometry::TopologyDomain::Cartesian,
                        op,
                        Strictness::Strict
                    )
                    .unwrap()),
                    baseline,
                    "{} order={order:?}",
                    op.name()
                );
            }
        }
    }

    #[test]
    fn mixed_symmetric_difference_is_associative() {
        let point = point(10.0, 10.0);
        let line = line(&[(1.0, 1.0), (3.0, 3.0)]);
        let area = square(0.0, 0.0, 4.0, 4.0);
        let direct = overlay_all(
            &[&point, &line, &area],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        let left = point
            .symmetric_difference(&line, Strictness::Strict)
            .unwrap()
            .symmetric_difference(&area, Strictness::Strict)
            .unwrap();
        let right = point
            .symmetric_difference(
                &line
                    .symmetric_difference(&area, Strictness::Strict)
                    .unwrap(),
                Strictness::Strict,
            )
            .unwrap();
        assert_eq!(wkb(&direct), wkb(&left));
        assert_eq!(wkb(&direct), wkb(&right));
    }

    #[test]
    fn difference_assigns_contained_polygon_as_hole() {
        let result = square(0.0, 0.0, 4.0, 4.0)
            .difference(&square(1.0, 1.0, 3.0, 3.0), Strictness::Strict)
            .expect("contained polygon difference");
        let Shape::Polygon(polygon) = result else {
            panic!("expected holed polygon");
        };
        assert_eq!(polygon.holes.len(), 1);
    }
}

/// Every overlay lane leaves through one builder, so a node on a straight
/// run never survives to the output, whichever lane produced the ring.
#[test]
fn overlay_output_rings_carry_no_collinear_nodes() {
    let shape = |wkt: &str| crate::io::parse_wkt(wkt).unwrap();
    let wkt = |shape: &Shape| crate::io::to_wkt(shape).unwrap();
    let holed = shape("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))");
    let plain = shape("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))");
    let partner = shape("POLYGON ((1 1, 3 1, 3 4, 1 4, 1 1))");
    let strict = Strictness::Strict;
    assert_eq!(
        wkt(&plain.union(&partner, strict).unwrap()),
        "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"
    );
    assert_eq!(
        wkt(&holed.union(&partner, strict).unwrap()),
        "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"
    );
    assert_eq!(
        wkt(&holed.difference(&partner, strict).unwrap()),
        "POLYGON ((0 0, 4 0, 4 4, 3 4, 3 1, 1 1, 1 4, 0 4, 0 0))"
    );
}
