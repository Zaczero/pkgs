use crate::geometry::overlay::*;
use crate::geometry::*;

fn shared_set_case(name: &str) -> Shape {
    match name {
        "polygon" => crate::io::parse_wkt("POLYGON ((0 0,2 0,2 2,0 2,0 0))").unwrap(),
        "line" => crate::io::parse_wkt("LINESTRING (0 0,2 0,2 1)").unwrap(),
        "point" => crate::io::parse_wkt("POINT (1 1)").unwrap(),
        "polygon with hole" => {
            crate::io::parse_wkt("POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))").unwrap()
        },
        "Z/M points sharing XY" => crate::io::parse_wkt(
            "GEOMETRYCOLLECTION (POINT ZM (0 0 1 10), POINT ZM (0 0 2 20), POINT ZM (1 1 3 30))",
        )
        .unwrap(),
        "mixed collection" => Shape::geometry_collection(vec![
            crate::io::parse_wkt("POLYGON ((0 0,2 0,2 2,0 2,0 0))").unwrap(),
            crate::io::parse_wkt("LINESTRING (-1 1,3 1)").unwrap(),
            crate::io::parse_wkt("POINT (1 1)").unwrap(),
        ])
        .unwrap(),
        _ => unreachable!("known shared-set fixture"),
    }
}

fn nested_shared_set(shape: Shape, depth: usize) -> Shape {
    let mut shared = shape;
    for _ in 0..depth {
        shared = Shape::geometry_collection(vec![shared.clone(), shared])
            .expect("shared collection depth is admitted");
    }
    shared
}

fn shape_wkb(shape: &Shape) -> Vec<u8> {
    crate::io::to_wkb(shape, None, false).expect("shared-set result writes WKB")
}

fn subdivision_wkb(shape: &Shape) -> Vec<Vec<u8>> {
    shape
        .subdivide(8, false)
        .expect("shared-set subdivision succeeds")
        .iter()
        .map(shape_wkb)
        .collect()
}

fn shapes_wkb(shapes: &[Shape]) -> Vec<Vec<u8>> {
    shapes.iter().map(shape_wkb).collect()
}

fn assert_shared_set_overlay_identity(name: &str, depth: usize) {
    let shared = nested_shared_set(shared_set_case(name), depth);
    let expanded = crate::io::parse_wkb(&shape_wkb(&shared))
        .expect("WKB round-trip expands shared members")
        .shape;

    let shared_parts = subdivision_wkb(&shared);
    assert_eq!(
        shared_parts,
        subdivision_wkb(&expanded),
        "{name}, depth {depth}"
    );

    let shared_union =
        Shape::union_all_topo(&[&shared], TopologyDomain::Cartesian, Strictness::Lenient)
            .expect("shared set union succeeds");
    let expanded_union =
        Shape::union_all_topo(&[&expanded], TopologyDomain::Cartesian, Strictness::Lenient)
            .expect("expanded set union succeeds");
    assert_eq!(
        shape_wkb(&shared_union),
        shape_wkb(&expanded_union),
        "{name}, depth {depth}"
    );

    assert_eq!(
        shape_wkb(&shared.node(Strictness::Lenient).unwrap()),
        shape_wkb(&expanded.node(Strictness::Lenient).unwrap()),
        "node: {name}, depth {depth}"
    );
    assert_eq!(
        shapes_wkb(&Shape::polygonize_all(&[&shared], false).unwrap()),
        shapes_wkb(&Shape::polygonize_all(&[&expanded], false).unwrap()),
        "polygonize_all: {name}, depth {depth}"
    );
    assert_eq!(
        shapes_wkb(&Shape::build_area_all(&[&shared], false).unwrap()),
        shapes_wkb(&Shape::build_area_all(&[&expanded], false).unwrap()),
        "build_area_all: {name}, depth {depth}"
    );

    let clip =
        crate::io::parse_wkt("POLYGON ((-0.5 -0.5,0.5 -0.5,0.5 0.5,-0.5 0.5,-0.5 -0.5))").unwrap();
    for operation in [
        OverlayOp::Union,
        OverlayOp::Intersection,
        OverlayOp::Difference,
        OverlayOp::SymmetricDifference,
    ] {
        assert_eq!(
            shape_wkb(
                &shared
                    .overlay(&clip, operation, Strictness::Lenient)
                    .unwrap()
            ),
            shape_wkb(
                &expanded
                    .overlay(&clip, operation, Strictness::Lenient)
                    .unwrap()
            ),
            "{operation:?}: {name}, depth {depth}"
        );
    }
}

macro_rules! shared_set_overlay_identity_tests {
    ($($test:ident => ($name:literal, $depth:literal)),+ $(,)?) => {
        $(
            #[test]
            fn $test() {
                assert_shared_set_overlay_identity($name, $depth);
            }
        )+
    };
}

shared_set_overlay_identity_tests! {
    shared_polygon_set_ops_match_expansion_at_depth_6 => ("polygon", 6),
    shared_line_set_ops_match_expansion_at_depth_6 => ("line", 6),
    shared_point_set_ops_match_expansion_at_depth_6 => ("point", 6),
    shared_holed_polygon_set_ops_match_expansion_at_depth_6 => ("polygon with hole", 6),
    shared_zm_set_ops_match_expansion_at_depth_6 => ("Z/M points sharing XY", 6),
    shared_mixed_set_ops_match_expansion_at_depth_6 => ("mixed collection", 6),
    shared_polygon_set_ops_match_expansion_at_depth_10 => ("polygon", 10),
    shared_line_set_ops_match_expansion_at_depth_10 => ("line", 10),
    shared_point_set_ops_match_expansion_at_depth_10 => ("point", 10),
    shared_holed_polygon_set_ops_match_expansion_at_depth_10 => ("polygon with hole", 10),
    shared_zm_set_ops_match_expansion_at_depth_10 => ("Z/M points sharing XY", 10),
    shared_mixed_set_ops_match_expansion_at_depth_10 => ("mixed collection", 10),
}

#[test]
fn depth_128_shared_set_overlay_finishes_with_correct_support() {
    let base = crate::io::parse_wkt("POLYGON ((0 0,1 0,1 1,0 1,0 0))").unwrap();
    for depth in [40, 128] {
        let mut shared = base.clone();
        for _ in 0..depth {
            shared = Shape::geometry_collection(vec![shared.clone(), shared])
                .expect("shared collection depth is admitted");
        }

        let parts = shared
            .subdivide(8, false)
            .expect("deep subdivision succeeds");
        assert!(!parts.is_empty(), "depth {depth}");
        let area = Shape::union_all_topo(&parts, TopologyDomain::Cartesian, Strictness::Lenient)
            .expect("deep subdivision parts union");
        assert_eq!(area.area(), 1.0, "depth {depth}");

        let union =
            Shape::union_all_topo(&[&shared], TopologyDomain::Cartesian, Strictness::Lenient)
                .expect("deep union succeeds");
        assert_eq!(union.area(), 1.0, "depth {depth}");
    }
}

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
            )
            .unwrap(),
            Vec::new(),
        )
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
        Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
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
        Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
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
        Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
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
        Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
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
        Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
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
            MultiLineString::try_new(
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
            MultiLineString::try_new(
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

    /// Every lower-dimensional contact of two areal operands is the same in
    /// both operand orders: shared boundary runs, isolated touches, a region
    /// plus a separate touch, nested with a shared side, identical operands.
    #[test]
    fn areal_intersection_contacts_are_operand_order_invariant() {
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
                Shape::Polygon(Polygon::new(
                    polygon(&[(0.0, 0.0), (6.0, 0.0), (6.0, 6.0), (0.0, 6.0), (0.0, 0.0)]).shell,
                    vec![
                        polygon(&[(2.0, 2.0), (4.0, 2.0), (4.0, 4.0), (2.0, 4.0), (2.0, 2.0)])
                            .shell,
                    ],
                )),
                square(2.0, 2.0, 3.0, 3.0),
            ),
        ];
        for (name, left, right) in cases {
            let forward = intersection(&left, &right);
            let reverse = intersection(&right, &left);
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
            let result = intersection(left, right);
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
        let source = Shape::Polygon(Polygon::new(shell, vec![hole]));
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
    fn areal_intersection_on_degenerate_boundaries() {
        let cases: &[(&str, &[(f64, f64)])] = &[
            ("collapsed ring", &[
                (0.0, 0.0),
                (2.0, 0.0),
                (4.0, 0.0),
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
                let result = intersection(left, right);
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
                match (fast, reverse) {
                    (Ok(fast), Ok(reverse)) => {
                        assert_eq!(wkb(&fast), wkb(&reverse), "{fast:?} != {reverse:?}");
                    },
                    (Err(fast), Err(reverse)) => {
                        assert_eq!(fast.to_string(), reverse.to_string());
                    },
                    (fast, reverse) => panic!("diverged: fast={fast:?} reverse={reverse:?}"),
                }
            }
        }
    }

    fn intersection(left: &Shape, right: &Shape) -> Shape {
        overlay_all(
            &[left, right],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap()
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
    fn convex_intersection_fast_path_yields_a_polygon() {
        let left = square(0.0, 0.0, 4.0, 4.0);
        let right = Shape::Polygon(polygon(&[
            (2.0, -1.0),
            (5.0, 2.0),
            (2.0, 5.0),
            (-1.0, 2.0),
            (2.0, -1.0),
        ]));
        let fast = intersection(&left, &right);
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
            let fast = intersection(&left, &right);
            assert!(
                matches!(
                    fast,
                    Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XY)
                ),
                "{name}: {fast:?}"
            );
        }
    }

    #[test]
    fn concave_intersection_keeps_the_line_the_areal_shortcut_drops() {
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
        );
        let concave_left_shape = Shape::Polygon(concave_left);
        let concave_right_shape = Shape::Polygon(concave_right);
        let exact = intersection(&concave_left_shape, &concave_right_shape);
        assert_ne!(wkb(&unrestricted), wkb(&exact));
    }

    #[test]
    fn convex_intersection_rejects_an_invalid_shell() {
        let invalid = Shape::Polygon(polygon(&[
            (0.0, 0.0),
            (4.0, 4.0),
            (0.0, 4.0),
            (4.0, 0.0),
            (0.0, 0.0),
        ]));
        let cutter = square(1.0, 1.0, 5.0, 5.0);
        let result = intersection(&invalid, &cutter);
        assert!(matches!(result, Shape::MultiPolygon(_)), "{result:?}");
        assert_eq!(result.area(), 4.5);
    }

    #[test]
    fn polygon_line_intersection_boundary_and_hole_cases() {
        let holed = Shape::Polygon(Polygon::new(
            polygon(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ])
            .shell,
            vec![
                Ring::from_closed(
                    CoordSeq::try_from_points(&[
                        Point::new(4.0, 4.0).unwrap(),
                        Point::new(6.0, 4.0).unwrap(),
                        Point::new(6.0, 6.0).unwrap(),
                        Point::new(4.0, 6.0).unwrap(),
                        Point::new(4.0, 4.0).unwrap(),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            ],
        ));
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
            let result = intersection(&polygon, &line);
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

        let reverse = intersection(
            &line(&[(-1.0, 2.0), (5.0, 2.0)]),
            &square(0.0, 0.0, 4.0, 4.0),
        );
        assert!(matches!(reverse, Shape::LineString(_)), "{reverse:?}");

        let multi_line = multiline(&[&[(-1.0, 1.0), (5.0, 1.0)], &[(1.0, 3.0), (3.0, 3.0)], &[
            (-2.0, 9.0),
            (-1.0, 9.0),
        ]]);
        let multi_result = intersection(&square(0.0, 0.0, 4.0, 4.0), &multi_line);
        assert!(
            matches!(multi_result, Shape::MultiLineString(_)),
            "{multi_result:?}"
        );

        let zm_result = intersection(
            &zm_square(0.0, 0.0, 4.0, 4.0, 10.0),
            &zm_line((-1.0, 2.0), (5.0, 2.0)),
        );
        assert!(zm_result.has_z() && zm_result.has_m(), "{zm_result:?}");
        assert_all_points_have_z(&zm_result);
        zm_result.for_each_point(|point| assert!(point.m().is_some(), "{zm_result:?}"));
    }

    #[test]
    fn polygon_intersection_with_a_collapsed_line_is_its_point() {
        let polygon = square(0.0, 0.0, 4.0, 4.0);
        let collapsed = line(&[(1.0, 1.0), (1.0, 1.0)]);
        let collapsed_result = polygon
            .intersection(&collapsed, Strictness::Strict)
            .unwrap();
        assert_eq!(collapsed_result, point(1.0, 1.0));
    }

    #[test]
    fn polygon_intersection_absorbs_retained_boundary_lines() {
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
            let result = intersection(&left, &right);
            assert_eq!(wkb(&result), wkb(&expected), "{name}: {result:?}");
            assert!(matches!(result, Shape::Polygon(_)), "{name}: {result:?}");
        }

        let square = square(0.0, 0.0, 4.0, 4.0);
        let partial_boundary = line(&[(1.0, 0.0), (3.0, 0.0)]);
        let result = intersection(&square, &partial_boundary);
        assert_eq!(wkb(&result), wkb(&partial_boundary), "{result:?}");
        assert!(matches!(result, Shape::LineString(_)), "{result:?}");
    }

    fn point_intersection(left: &Shape, right: &Shape, strictness: Strictness) -> Shape {
        overlay_all(
            &[left, right],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            strictness,
        )
        .unwrap()
    }

    #[test]
    fn scalar_point_intersection_with_a_collapsed_line_is_the_point() {
        let collapsed = line(&[(1.0, 1.0), (1.0, 1.0)]);
        let collapsed_point = point(1.0, 1.0);
        for (left, right) in [
            (&collapsed_point, &collapsed),
            (&collapsed, &collapsed_point),
        ] {
            let inputs = vec![left, right];
            let result = overlay_all(
                &inputs,
                crate::geometry::TopologyDomain::Cartesian,
                OverlayOp::Intersection,
                Strictness::Strict,
            )
            .unwrap();
            assert_eq!(result, point(1.0, 1.0));
        }
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
                let result = point_intersection(left, right, Strictness::Lenient);
                assert_eq!(result.coordinate_axes(), axes, "{name}: {result:?}");
            }
        }
        for (left, right) in [(&matching_point, &z_line), (&z_line, &matching_point)] {
            let result = point_intersection(left, right, Strictness::Strict);
            assert_eq!(result.coordinate_axes(), "XYZM", "{result:?}");
        }

        let conflicting = Shape::Point(
            Point::new_axes(1.0, 0.0, ZOrdinate(Some(99.0)), MOrdinate(Some(9.0))).unwrap(),
        );
        for (left, right) in [(&conflicting, &z_line), (&z_line, &conflicting)] {
            let result = point_intersection(left, right, Strictness::Lenient);
            assert_eq!(result.coordinate_axes(), "XYM", "{result:?}");
        }

        let z_polygon = z_square(0.0, 0.0, 4.0, 4.0, 10.0);
        let boundary = point(2.0, 0.0);
        let boundary_result = point_intersection(&boundary, &z_polygon, Strictness::Lenient);
        assert_eq!(boundary_result.coordinate_axes(), "XYZ");
        let boundary_point = boundary_result.points_vec().unwrap()[0];
        assert_eq!(boundary_point.z(), Some(12.0));

        let interior_point = point(1.0, 1.0);
        let interior_result = point_intersection(&interior_point, &z_polygon, Strictness::Lenient);
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
    fn multipart_point_intersection_for_lineal_carriers() {
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
        for (left, right) in [(&multipoint, &live), (&live, &multipoint)] {
            let result = point_intersection(left, right, Strictness::Strict);
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
    }

    #[test]
    fn multipart_point_intersection_for_polygonal_carriers() {
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
        for (left, right) in [(&points, &carrier), (&carrier, &points)] {
            let result = point_intersection(left, right, Strictness::Strict);
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
    }

    #[test]
    fn multipart_point_intersection_reconciles_ordinates() {
        let z_points = Shape::MultiPoint(
            vec![
                Point::new_axes(1.0, 0.0, ZOrdinate(Some(10.0)), MOrdinate(None)).unwrap(),
                Point::new_axes(1.0, 0.0, ZOrdinate(Some(11.0)), MOrdinate(None)).unwrap(),
            ]
            .into(),
        );
        let xy_line = line(&[(0.0, 0.0), (2.0, 0.0)]);
        let lenient = point_intersection(&z_points, &xy_line, Strictness::Lenient);
        assert_eq!(lenient.coordinate_axes(), "XY");

        let points = Shape::MultiPoint(vec![Point::new(2.0, 0.0).unwrap()].into());
        let z_polygon = z_square(0.0, 0.0, 4.0, 4.0, 10.0);
        let result = point_intersection(&points, &z_polygon, Strictness::Strict);
        assert_eq!(result.coordinate_axes(), "XYZ");
        assert_eq!(result.points_vec().unwrap()[0].z(), Some(12.0));
        let Shape::Polygon(polygon) = &z_polygon else {
            unreachable!("z_square returns a polygon");
        };
        let z_multipolygon = Shape::MultiPolygon(vec![polygon.clone()].into());
        let scalar = point(2.0, 0.0);
        let scalar_result = point_intersection(&scalar, &z_multipolygon, Strictness::Strict);
        assert_eq!(scalar_result.coordinate_axes(), "XYZ");
        assert_eq!(scalar_result.points_vec().unwrap()[0].z(), Some(12.0));

        let repeated_line = Shape::MultiLineString(
            MultiLineString::try_new(CoordinateAxes::XYZ, vec![MultiMember::Value(
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
        let repeated_error = overlay_all(
            &[&repeated_point, &repeated_line],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap_err();
        assert!(repeated_error.to_string().contains("cannot represent Z/M"));
        let repeated_result = overlay_all(
            &[&repeated_point, &repeated_line],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Lenient,
        )
        .unwrap();
        assert_eq!(repeated_result.coordinate_axes(), "XY");
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
    fn mixed_symmetric_difference_keeps_line_support_outside_selected_area() {
        let left = crate::io::parse_wkt(
            "GEOMETRYCOLLECTION(LINESTRING(1 1,3 1),POLYGON((0 0,4 0,4 4,0 4,0 0)))",
        )
        .unwrap();
        let right = crate::io::parse_wkt("POLYGON((2 -1,6 -1,6 5,2 5,2 -1))").unwrap();
        let result = left
            .symmetric_difference(&right, Strictness::Strict)
            .unwrap();
        assert_eq!(
            crate::io::to_wkt(&result).unwrap(),
            "MULTIPOLYGON (((0 0, 2 0, 2 4, 0 4, 0 0)), ((2 -1, 6 -1, 6 5, 2 5, 2 4, 4 4, 4 0, 2 0, 2 -1)))"
        );
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

    /// Three bounds components: an overlapping pair, an invalid bowtie with a
    /// square across it, and a remote square. Each component is partitioned
    /// alone, a difference keeps only its left operand's component, and an
    /// intersection whose bounds share no box builds nothing.
    #[test]
    fn bounds_components_partition_alone_and_match_one_arrangement() {
        use crate::geometry::overlay::exact_partition::{partition_builds, reset_partition_builds};
        let first = square(0.0, 0.0, 2.0, 2.0);
        let second = square(1.0, 1.0, 3.0, 3.0);
        let bowtie = Shape::Polygon(polygon(&[
            (10.0, 10.0),
            (12.0, 12.0),
            (10.0, 12.0),
            (12.0, 10.0),
            (10.0, 10.0),
        ]));
        let across = square(11.0, 11.0, 13.0, 13.0);
        let remote = square(20.0, 20.0, 21.0, 21.0);
        let inputs = [&first, &second, &bowtie, &across, &remote];
        for (op, builds) in [
            (OverlayOp::Union, 3),
            (OverlayOp::SymmetricDifference, 3),
            (OverlayOp::Difference, 1),
            (OverlayOp::Intersection, 0),
        ] {
            reset_partition_builds();
            let certified = overlay_all(
                &inputs,
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            )
            .unwrap();
            assert_eq!(partition_builds(), builds, "{}", op.name());
            let joint = super::super::nary::overlay_all_without_bounds_certificates(
                &inputs,
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            )
            .unwrap();
            assert_eq!(wkb(&certified), wkb(&joint), "{}", op.name());
        }
    }

    /// Emission checks run where rounding moved a crossing, and nested
    /// operands, whose emitted nodes are all stored, skip them.
    #[test]
    fn emission_checks_run_only_where_rounding_moved_a_crossing() {
        use crate::geometry::overlay::exact_partition::{
            partition_metrics, reset_partition_builds,
        };
        let outer = square(0.0, 0.0, 4.0, 4.0);
        for (right, moved) in [
            (square(1.0, 1.0, 2.0, 2.0), false),
            (regular_polygon(4.0, 4.0, 1.5, 7), true),
        ] {
            reset_partition_builds();
            overlay_all(
                &[&outer, &right],
                crate::geometry::TopologyDomain::Cartesian,
                OverlayOp::Intersection,
                Strictness::Strict,
            )
            .unwrap();
            assert_eq!(partition_metrics().unmoved_emissions == 0, moved);
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
        let lines = MultiLineString::try_new(CoordinateAxes::XYZM, vec![
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
    fn line_only_support_includes_collapsed_points() {
        let line_shape = line(&[(0.0, 0.0), (2.0, 0.0)]);
        let collapsed = line(&[(4.0, 4.0), (4.0, 4.0)]);
        let point_on_line = point(0.0, 0.0);
        let collapsed_union = overlay_all(
            &[&line_shape, &collapsed],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Union,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(
            crate::io::to_wkt(&collapsed_union).unwrap(),
            "GEOMETRYCOLLECTION (POINT (4 4), LINESTRING (0 0, 2 0))"
        );
        let collapsed_intersection = overlay_all(
            &[&line_shape, &collapsed],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        assert!(matches!(
            collapsed_intersection,
            Shape::Empty(GeometryKind::LineString, _)
        ));

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

mod rectangle_clip_tests {
    use super::*;
    use crate::geometry::segments::clip_segment_rect_interval;
    use crate::geometry::tessellation::exact::compare_ratios;

    fn bounds(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Bounds {
        Bounds::new(minx, miny, maxx, maxy).expect("finite ordered rectangle")
    }

    fn ring(points: &[(f64, f64)]) -> Ring {
        Ring::from_closed(
            CoordSeq::try_from_points(
                &points
                    .iter()
                    .map(|&(x, y)| Point::new_raw_xy(x, y))
                    .collect::<Vec<_>>(),
            )
            .expect("test coordinates form a sequence"),
        )
        .expect("test coordinates form a closed ring")
    }

    fn polygon(points: &[(f64, f64)]) -> Polygon {
        Polygon::new(ring(points), Vec::new())
    }

    fn rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Polygon {
        polygon(&[
            (minx, miny),
            (maxx, miny),
            (maxx, maxy),
            (minx, maxy),
            (minx, miny),
        ])
    }

    fn compare_clip_results(
        name: &str,
        actual: Result<Vec<Polygon>>,
        expected: Result<Vec<Polygon>>,
        matching_errors_are_expected: bool,
    ) {
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => {
                let actual = clip::polygon_parts_to_shape(actual);
                let expected = clip::polygon_parts_to_shape(expected);
                assert!(
                    actual.equals(&expected).unwrap(),
                    "{name}: {actual:?} differs topologically from {expected:?}"
                );
                assert!(
                    actual.validate().unwrap().is_none(),
                    "{name}: invalid rectangle clip output {actual:?}"
                );
            },
            (Err(actual), Err(expected)) if matching_errors_are_expected => {
                assert_eq!(
                    actual.to_string(),
                    expected.to_string(),
                    "{name}: local partition and exact overlay disagree on failure"
                );
            },
            (actual, expected) => panic!(
                "{name}: local partition returned {actual:?}, exact overlay returned {expected:?}"
            ),
        }
    }

    fn assert_clip_matches_oracle(name: &str, subjects: &[Polygon], bounds: Bounds) {
        let rectangle = rect_polygon(bounds).expect("clip rectangle polygon");
        let expected = binary_areal_overlay(
            subjects,
            std::slice::from_ref(&rectangle),
            OverlayOp::Intersection,
        );
        let actual = clip_polygonal_rectangle(subjects, bounds);
        compare_clip_results(name, actual, expected, false);
    }

    fn assert_clip_oracle_failure_or_equal(name: &str, subjects: &[Polygon], bounds: Bounds) {
        let rectangle = rect_polygon(bounds).expect("clip rectangle polygon");
        compare_clip_results(
            name,
            clip_polygonal_rectangle(subjects, bounds),
            binary_areal_overlay(
                subjects,
                std::slice::from_ref(&rectangle),
                OverlayOp::Intersection,
            ),
            true,
        );
    }

    fn next_random(state: &mut u64) -> u64 {
        let mut value = *state;
        value ^= value.wrapping_shl(13);
        value ^= value.wrapping_shr(7);
        value ^= value.wrapping_shl(17);
        *state = value;
        value
    }

    fn random_unit(state: &mut u64) -> f64 {
        (next_random(state) >> 11) as f64 / (1_u64 << 53) as f64
    }

    fn regular_ellipse(cx: f64, cy: f64, rx: f64, ry: f64, count: usize, phase: f64) -> Polygon {
        let mut points = Vec::with_capacity(count.wrapping_add(1));
        for index in 0..count {
            let angle = phase + std::f64::consts::TAU * index as f64 / count as f64;
            points.push(Point::new_raw_xy(
                cx + rx * angle.cos(),
                cy + ry * angle.sin(),
            ));
        }
        points.push(points[0]);
        Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        )
    }

    #[test]
    fn rectangle_local_partition_matches_the_oracle_on_the_full_areal_matrix() {
        let square = rectangle(0.0, 0.0, 4.0, 4.0);
        let shell = ring(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]);
        let hole = ring(&[(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0), (4.0, 4.0)]);
        let holed = Polygon::new(shell, vec![hole]);
        let touching = [rectangle(0.0, 0.0, 2.0, 2.0), rectangle(2.0, 2.0, 4.0, 4.0)];
        let concave = polygon(&[
            (0.0, 0.0),
            (6.0, 0.0),
            (6.0, 6.0),
            (4.0, 6.0),
            (4.0, 2.0),
            (2.0, 2.0),
            (2.0, 6.0),
            (0.0, 6.0),
            (0.0, 0.0),
        ]);
        let spike = polygon(&[
            (0.0, 0.0),
            (4.0, 0.0),
            (4.0, 4.0),
            (2.0, 4.0),
            (2.0, 2.0),
            (3.0, 2.0),
            (2.0, 2.0),
            (2.0, 4.0),
            (0.0, 4.0),
            (0.0, 0.0),
        ]);

        let cases = [
            (
                "contained-window",
                vec![rectangle(-4.0, -3.0, 4.0, 3.0)],
                bounds(-1.0, -1.5, 1.25, 1.5),
            ),
            (
                "source-disjoint-from-window",
                vec![rectangle(0.0, 0.0, 1.0, 1.0)],
                bounds(5.0, 5.0, 6.0, 6.0),
            ),
            (
                "source-vertices-on-side-and-corner",
                vec![square.clone()],
                bounds(0.0, 1.0, 2.0, 4.0),
            ),
            (
                "coincident-shell-and-window",
                vec![square.clone()],
                bounds(0.0, 0.0, 4.0, 4.0),
            ),
            (
                "left-side-overlap",
                vec![square.clone()],
                bounds(0.0, 1.0, 2.0, 3.0),
            ),
            (
                "right-side-overlap",
                vec![square.clone()],
                bounds(2.0, 1.0, 4.0, 3.0),
            ),
            (
                "bottom-side-overlap",
                vec![square.clone()],
                bounds(1.0, 0.0, 3.0, 2.0),
            ),
            (
                "top-side-overlap",
                vec![square.clone()],
                bounds(1.0, 2.0, 3.0, 4.0),
            ),
            (
                "corner-only-contact",
                vec![square.clone()],
                bounds(4.0, 4.0, 5.0, 5.0),
            ),
            (
                "zero-width-window",
                vec![square.clone()],
                bounds(2.0, 0.0, 2.0, 4.0),
            ),
            (
                "zero-height-window",
                vec![square.clone()],
                bounds(0.0, 2.0, 4.0, 2.0),
            ),
            (
                "hole-crossed-by-window",
                vec![holed.clone()],
                bounds(3.0, 3.0, 7.0, 7.0),
            ),
            (
                "hole-contained-by-window",
                vec![holed],
                bounds(2.0, 2.0, 8.0, 8.0),
            ),
            (
                "multiple-shells",
                vec![square, rectangle(5.0, 1.0, 9.0, 5.0)],
                bounds(1.0, 1.0, 7.0, 3.0),
            ),
            (
                "point-touching-parts",
                touching.to_vec(),
                bounds(1.0, 1.0, 3.0, 3.0),
            ),
            (
                "disconnected-concave-result",
                vec![concave],
                bounds(0.5, 3.0, 5.5, 5.0),
            ),
            ("retraced-spike", vec![spike], bounds(0.5, 0.5, 3.5, 3.5)),
        ];
        for (name, polygons, rect) in cases {
            assert_clip_matches_oracle(name, &polygons, rect);
        }
    }

    #[test]
    fn contained_and_crossing_seeded_convex_rings_match_the_oracle() {
        let mut state = 0xD1B5_4A32_D192_ED03;
        for case in 0..24 {
            let count = 5 + (next_random(&mut state) % 25) as usize;
            let (cx, cy) = (
                200.0 * random_unit(&mut state) - 100.0,
                200.0 * random_unit(&mut state) - 100.0,
            );
            let (rx, ry) = (
                2.0 + 8.0 * random_unit(&mut state),
                2.0 + 8.0 * random_unit(&mut state),
            );
            let phase = std::f64::consts::TAU * random_unit(&mut state);
            let polygon = regular_ellipse(cx, cy, rx, ry, count, phase);
            assert_clip_matches_oracle(
                &format!("seeded-contained-{case}"),
                std::slice::from_ref(&polygon),
                bounds(
                    cx - 0.03 * rx,
                    cy - 0.03 * ry,
                    cx + 0.03 * rx,
                    cy + 0.03 * ry,
                ),
            );
            assert_clip_matches_oracle(
                &format!("seeded-crossing-{case}"),
                &[polygon],
                bounds(cx + 0.7 * rx, cy - 0.1 * ry, cx + 1.1 * rx, cy + 0.1 * ry),
            );
        }
    }

    #[test]
    fn ellipse_and_close_rounded_crossings_match_the_oracle() {
        let ellipse = regular_ellipse(0.0, 0.0, 4.0, 3.0, 4096, 0.0);
        assert_clip_matches_oracle(
            "ellipse-4096-contained",
            &[ellipse],
            bounds(-1.0, -1.5, 1.25, 1.5),
        );

        let ulp = f64::from_bits(1.0_f64.to_bits().wrapping_add(1)) - 1.0;
        let quarter = polygon(&[(-1.0, 1.0), (7.0, 1.0 + ulp), (7.0, 0.0), (-1.0, 1.0)]);
        let half = polygon(&[(-1.0, 1.0), (3.0, 1.0 + ulp), (3.0, 0.0), (-1.0, 1.0)]);
        let quarter_interval = clip_segment_rect_interval(
            Segment {
                start: XY::new(-1.0, 1.0),
                end: XY::new(7.0, 1.0 + ulp),
            },
            bounds(1.0, 0.0, 2.0, 2.0),
        )
        .unwrap()
        .unwrap();
        let half_interval = clip_segment_rect_interval(
            Segment {
                start: XY::new(-1.0, 1.0),
                end: XY::new(3.0, 1.0 + ulp),
            },
            bounds(1.0, 0.0, 2.0, 2.0),
        )
        .unwrap()
        .unwrap();
        assert_eq!(quarter_interval.start.point.y.to_bits(), 1.0_f64.to_bits());
        assert_eq!(half_interval.start.point.y.to_bits(), 1.0_f64.to_bits());
        let [quarter_x, quarter_y, quarter_w] = *quarter_interval.start.exact.unwrap();
        let [half_x, half_y, half_w] = *half_interval.start.exact.unwrap();
        assert_eq!(
            compare_ratios(&quarter_x, &quarter_w, &half_x, &half_w).unwrap(),
            std::cmp::Ordering::Equal
        );
        assert!(
            compare_ratios(&quarter_y, &quarter_w, &half_y, &half_w)
                .unwrap()
                .is_lt(),
            "the exact boundary crossings differ despite sharing one rounded coordinate"
        );
        assert_clip_oracle_failure_or_equal(
            "distinct-exact-crossings-one-rounded-coordinate",
            &[quarter, half],
            bounds(1.0, 0.0, 2.0, 2.0),
        );
    }

    #[test]
    fn rectangle_local_contact_guard_rejects_disjoint_source_edges() {
        let subject = rectangle(0.0, 0.0, 1.0, 1.0);
        assert_clip_matches_oracle(
            "disjoint-source-window",
            &[subject],
            bounds(5.0, 5.0, 6.0, 6.0),
        );
    }

    #[test]
    fn rectangle_clip_line_contacts_and_zm_carry_keep_the_existing_contract() {
        assert_eq!(
            clip_segment(
                XY::new(-2.0, 0.0),
                XY::new(2.0, 0.0),
                bounds(0.0, 0.0, 1.0, 1.0),
            )
            .unwrap(),
            Some((XY::new(0.0, 0.0), XY::new(1.0, 0.0)))
        );
        assert_eq!(
            clip_segment(
                XY::new(-1.0, 1.0),
                XY::new(0.0, 0.0),
                bounds(0.0, 0.0, 1.0, 1.0),
            )
            .unwrap(),
            None
        );
        assert_eq!(
            clip_segment(
                XY::new(0.0, -1.0),
                XY::new(0.0, 2.0),
                bounds(0.0, 0.0, 0.0, 1.0),
            )
            .unwrap(),
            Some((XY::new(0.0, 0.0), XY::new(0.0, 1.0)))
        );

        let line_point =
            |x, z, m| Point::new_axes(x, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap();
        let source_line = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    line_point(-1.0, 10.0, 100.0),
                    line_point(2.0, 40.0, 400.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let expected_line = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    line_point(0.0, 20.0, 200.0),
                    line_point(1.0, 30.0, 300.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        assert!(
            source_line
                .clip_by_rect(bounds(0.0, -1.0, 1.0, 1.0), true)
                .unwrap()
                .equals_exact(&expected_line, 0.0, true, true)
        );

        let polygon = Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(
                    &[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]
                        .into_iter()
                        .map(|(x, y)| {
                            Point::new_axes(
                                x,
                                y,
                                ZOrdinate(Some(x + y + 7.0)),
                                MOrdinate(Some(x - y + 13.0)),
                            )
                            .unwrap()
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        );
        let rect = bounds(1.0, 0.5, 3.0, 3.5);
        let expected_xy = clip::polygon_parts_to_shape(
            binary_areal_overlay(
                std::slice::from_ref(&polygon),
                &[rect_polygon(rect).unwrap()],
                OverlayOp::Intersection,
            )
            .unwrap(),
        );
        let source = Shape::Polygon(polygon);
        let expected = carry_ordinates(expected_xy, &[&source], "clip_by_rect", false).unwrap();
        let actual = source.clip_by_rect(rect, false).unwrap();
        assert!(actual.equals_exact(&expected, 0.0, true, true));
        let _error = source.clip_by_rect(rect, true).unwrap_err();
    }
}
mod exact_geographic_nary_tests {
    use super::*;

    fn frame() -> crate::geographic::GeographicFrame {
        crate::geographic::GeographicFrame::crs84()
    }

    fn polygon(points: &[(f64, f64)]) -> Shape {
        let points = points
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect::<Vec<_>>();
        Shape::Polygon(Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
    }

    fn line(points: &[Point]) -> Shape {
        Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(points).unwrap()).unwrap())
    }

    fn exact_location(shape: &Shape, point: Point) -> Loc {
        let frame = frame();
        let plan = crate::geometry::PreparedPlan::transient(shape);
        let cache = crate::geometry::FrameDependentCaches::default();
        let prepared = crate::geometry::GeographicPrepared::try_new(&plan, &cache, frame).unwrap();
        prepared
            .exact_topology()
            .unwrap()
            .locate_point(point)
            .unwrap()
            .geometry
    }

    #[test]
    fn nary_global_q_preserves_seam_and_disconnected_area_rows() {
        let seam = polygon(&[
            (170.0, -10.0),
            (-170.0, -10.0),
            (-170.0, 10.0),
            (170.0, 10.0),
            (170.0, -10.0),
        ]);
        let distant = polygon(&[
            (30.0, 40.0),
            (32.0, 40.0),
            (32.0, 42.0),
            (30.0, 42.0),
            (30.0, 40.0),
        ]);
        let output = super::super::nary::overlay_all(
            &[&seam, &distant],
            TopologyDomain::Geographic(frame()),
            OverlayOp::Union,
            Strictness::Strict,
        )
        .unwrap();

        assert_eq!(
            exact_location(&output, Point::new_raw_xy(175.0, 0.0)),
            Loc::Interior
        );
        assert_eq!(
            exact_location(&output, Point::new_raw_xy(-175.0, 0.0)),
            Loc::Interior
        );
        assert_eq!(
            exact_location(&output, Point::new_raw_xy(0.0, 0.0)),
            Loc::Exterior
        );
        assert_eq!(
            exact_location(&output, Point::new_raw_xy(31.0, 41.0)),
            Loc::Interior
        );
    }

    #[test]
    fn nary_intersection_keeps_a_pole_only_contact() {
        let left = polygon(&[(0.0, 90.0), (10.0, 80.0), (-10.0, 80.0), (0.0, 90.0)]);
        let right = polygon(&[(100.0, 90.0), (90.0, 80.0), (110.0, 80.0), (100.0, 90.0)]);
        let output = super::super::nary::overlay_all(
            &[&left, &right],
            TopologyDomain::Geographic(frame()),
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        assert!(matches!(output, Shape::Point(_)), "{output:?}");
        assert_eq!(
            exact_location(&output, Point::new_raw_xy(-120.0, 90.0)),
            Loc::Interior
        );
    }

    #[test]
    fn nary_difference_uses_global_face_rows_to_emit_a_hole() {
        let shell = polygon(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]);
        let first_hole = polygon(&[(2.0, 2.0), (4.0, 2.0), (4.0, 4.0), (2.0, 4.0), (2.0, 2.0)]);
        let second_hole = polygon(&[(6.0, 6.0), (8.0, 6.0), (8.0, 8.0), (6.0, 8.0), (6.0, 6.0)]);
        let output = super::super::nary::overlay_all(
            &[&shell, &first_hole, &second_hole],
            TopologyDomain::Geographic(frame()),
            OverlayOp::Difference,
            Strictness::Strict,
        )
        .unwrap();
        let Shape::Polygon(output) = output else {
            panic!("a nested polygon difference has one holed shell: {output:?}");
        };
        assert_eq!(output.holes.len(), 2);
        assert_eq!(
            exact_location(&Shape::Polygon(output.clone()), Point::new_raw_xy(1.0, 1.0)),
            Loc::Interior
        );
        assert_eq!(
            exact_location(&Shape::Polygon(output.clone()), Point::new_raw_xy(3.0, 3.0)),
            Loc::Exterior
        );
        assert_eq!(
            exact_location(&Shape::Polygon(output), Point::new_raw_xy(7.0, 7.0)),
            Loc::Exterior
        );
    }

    #[test]
    fn nary_line_overlay_selects_exact_spans_for_union_intersection_and_difference() {
        let source = line(&[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(10.0, 0.0)]);
        let mask = line(&[Point::new_raw_xy(4.0, 0.0), Point::new_raw_xy(6.0, 0.0)]);

        let union = super::super::nary::overlay_all(
            &[&source, &mask],
            TopologyDomain::Geographic(frame()),
            OverlayOp::Union,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(
            exact_location(&union, Point::new_raw_xy(2.0, 0.0)),
            Loc::Interior
        );
        assert_eq!(
            exact_location(&union, Point::new_raw_xy(5.0, 0.0)),
            Loc::Interior
        );

        let intersection = super::super::nary::overlay_all(
            &[&source, &mask],
            TopologyDomain::Geographic(frame()),
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(
            exact_location(&intersection, Point::new_raw_xy(5.0, 0.0)),
            Loc::Interior
        );
        assert_eq!(
            exact_location(&intersection, Point::new_raw_xy(2.0, 0.0)),
            Loc::Exterior
        );

        let difference = super::super::nary::overlay_all(
            &[&source, &mask],
            TopologyDomain::Geographic(frame()),
            OverlayOp::Difference,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(
            exact_location(&difference, Point::new_raw_xy(2.0, 0.0)),
            Loc::Interior
        );
        assert_eq!(
            exact_location(&difference, Point::new_raw_xy(5.0, 0.0)),
            Loc::Exterior
        );
        assert_eq!(
            exact_location(&difference, Point::new_raw_xy(8.0, 0.0)),
            Loc::Interior
        );
    }

    #[test]
    fn nary_point_overlay_selects_points_and_absorbs_them_in_area() {
        let area = polygon(&[
            (-10.0, -10.0),
            (10.0, -10.0),
            (10.0, 10.0),
            (-10.0, 10.0),
            (-10.0, -10.0),
        ]);
        let inside = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        let outside = Shape::Point(Point::new_raw_xy(30.0, 30.0));

        let intersection = super::super::nary::overlay_all(
            &[&area, &inside],
            TopologyDomain::Geographic(frame()),
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        assert!(matches!(&intersection, Shape::Point(_)), "{intersection:?}");
        assert_eq!(
            exact_location(&intersection, Point::new_raw_xy(0.0, 0.0)),
            Loc::Interior
        );

        let empty = super::super::nary::overlay_all(
            &[&area, &outside],
            TopologyDomain::Geographic(frame()),
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        assert!(empty.is_empty());

        let union = super::super::nary::overlay_all(
            &[&area, &inside],
            TopologyDomain::Geographic(frame()),
            OverlayOp::Union,
            Strictness::Strict,
        )
        .unwrap();
        assert!(matches!(union, Shape::Polygon(_)), "{union:?}");

        let union = super::super::nary::overlay_all(
            &[&area, &outside],
            TopologyDomain::Geographic(frame()),
            OverlayOp::Union,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(
            exact_location(&union, Point::new_raw_xy(30.0, 30.0)),
            Loc::Interior
        );
    }

    #[test]
    fn nary_union_emits_a_horizontal_full_turn_ring_as_its_selected_cap() {
        let cap = polygon(&[
            (0.0, 80.0),
            (90.0, 80.0),
            (180.0, 80.0),
            (-90.0, 80.0),
            (0.0, 80.0),
        ]);
        let output = super::super::nary::overlay_all(
            &[&cap],
            TopologyDomain::Geographic(frame()),
            OverlayOp::Union,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(
            exact_location(&output, Point::new_raw_xy(0.0, 85.0)),
            Loc::Interior
        );
        assert_eq!(
            exact_location(&output, Point::new_raw_xy(0.0, 70.0)),
            Loc::Exterior
        );
    }

    #[test]
    fn nary_union_emits_a_full_longitude_axis_band_without_turn_fill() {
        let band = polygon(&[
            (-180.0, -10.0),
            (180.0, -10.0),
            (180.0, 10.0),
            (-180.0, 10.0),
            (-180.0, -10.0),
        ]);
        let output = super::super::nary::overlay_all(
            &[&band],
            TopologyDomain::Geographic(frame()),
            OverlayOp::Union,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(
            exact_location(&output, Point::new_raw_xy(0.0, 0.0)),
            Loc::Interior
        );
        assert_eq!(
            exact_location(&output, Point::new_raw_xy(0.0, 20.0)),
            Loc::Exterior
        );
        assert_eq!(
            exact_location(&output, Point::new_raw_xy(0.0, 80.0)),
            Loc::Exterior
        );
    }

    #[test]
    fn nary_union_emits_a_source_owned_whole_sphere() {
        let middle = polygon(&[
            (-180.0, -80.0),
            (180.0, -80.0),
            (180.0, 80.0),
            (-180.0, 80.0),
            (-180.0, -80.0),
        ]);
        let south_cap = polygon(&[
            (0.0, -80.0),
            (90.0, -80.0),
            (180.0, -80.0),
            (-90.0, -80.0),
            (0.0, -80.0),
        ]);
        let north_cap = polygon(&[
            (0.0, 80.0),
            (90.0, 80.0),
            (180.0, 80.0),
            (-90.0, 80.0),
            (0.0, 80.0),
        ]);
        assert_eq!(
            exact_location(&middle, Point::new_raw_xy(0.0, 0.0)),
            Loc::Interior
        );
        assert_eq!(
            exact_location(&south_cap, Point::new_raw_xy(0.0, -85.0)),
            Loc::Interior
        );
        assert_eq!(
            exact_location(&north_cap, Point::new_raw_xy(0.0, 85.0)),
            Loc::Interior
        );
        assert_eq!(
            exact_location(&north_cap, Point::new_raw_xy(0.0, 89.0)),
            Loc::Interior
        );
        let source_union =
            Shape::geometry_collection(vec![south_cap.clone(), middle.clone(), north_cap.clone()])
                .unwrap();
        assert_eq!(
            exact_location(&source_union, Point::new_raw_xy(0.0, 89.0)),
            Loc::Interior
        );
        let one_row = super::super::nary::overlay_all(
            &[&source_union],
            TopologyDomain::Geographic(frame()),
            OverlayOp::Union,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(
            exact_location(&one_row, Point::new_raw_xy(0.0, 89.0)),
            Loc::Interior,
            "{one_row:?}"
        );
        let output = super::super::nary::overlay_all(
            &[&south_cap, &middle, &north_cap],
            TopologyDomain::Geographic(frame()),
            OverlayOp::Union,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(
            exact_location(&output, Point::new_raw_xy(0.0, 0.0)),
            Loc::Interior
        );
        assert_eq!(
            exact_location(&output, Point::new_raw_xy(0.0, 89.0)),
            Loc::Interior,
            "{output:?}"
        );
        assert_eq!(
            exact_location(&output, Point::new_raw_xy(0.0, -89.0)),
            Loc::Interior
        );
        assert_eq!(
            exact_location(&output, Point::new_raw_xy(40.0, 90.0)),
            Loc::Interior
        );
    }

    #[test]
    fn nary_seam_intersection_carries_the_original_source_interval() {
        let crossing = line(&[
            Point::new_axes(170.0, 0.0, ZOrdinate(Some(0.0)), MOrdinate(Some(10.0))).unwrap(),
            Point::new_axes(-170.0, 0.0, ZOrdinate(Some(20.0)), MOrdinate(Some(30.0))).unwrap(),
        ]);
        let meridian = line(&[
            Point::new_raw_xy(180.0, -1.0),
            Point::new_raw_xy(180.0, 1.0),
        ]);
        let output = super::super::nary::overlay_all(
            &[&crossing, &meridian],
            TopologyDomain::Geographic(frame()),
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        let Shape::Point(point) = output else {
            panic!("a crossing of two lines is a point: {output:?}");
        };
        assert_eq!(point.z(), Some(10.0));
        assert_eq!(point.m(), Some(20.0));
        assert_eq!(point.y, 0.0);
    }

    #[test]
    fn nary_emission_rejects_distinct_exact_crossings_that_round_together() {
        let sources = Shape::geometry_collection(vec![
            line(&[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(3.0, 1.0)]),
            line(&[Point::new_raw_xy(1.0, -1.0), Point::new_raw_xy(1.0, 1.0)]),
            line(&[
                Point::new_raw_xy(0.0, 1.0),
                Point::new_raw_xy(2.0, -1.0 / 3.0),
            ]),
        ])
        .unwrap();
        let error = super::super::nary::overlay_all(
            &[&sources],
            TopologyDomain::Geographic(frame()),
            OverlayOp::Intersection,
            Strictness::Lenient,
        )
        .expect_err("the output cannot preserve both exact crossing nodes");
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactNodeCollision { .. })
        ));
    }
}
