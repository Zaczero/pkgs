use crate::geometry::overlay::*;
use crate::geometry::*;
mod binary_areal_overlay_tests {
    use super::*;

    fn polygon(points: &[(f64, f64)]) -> Polygon {
        Polygon::new(
            Ring::from_closed(
                points
                    .iter()
                    .map(|&(x, y)| Point::new(x, y).unwrap())
                    .collect::<Vec<_>>(),
            ),
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
        Shape::Polygon(Polygon::new(Ring::from_closed(points), Vec::new()))
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
        Shape::Polygon(Polygon::new(Ring::from_closed(points), Vec::new()))
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
        Shape::Polygon(Polygon::new(Ring::from_closed(points), Vec::new()))
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
        Shape::LineString(LineSeq::try_new(CoordSeq::from(points)).expect("test line is valid"))
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
        Shape::Polygon(Polygon::new(Ring::from_closed(points), Vec::new()))
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
        Shape::Polygon(Polygon::new(Ring::from_closed(points), Vec::new()))
    }

    fn line(points: &[(f64, f64)]) -> Shape {
        Shape::LineString(
            LineSeq::try_new(CoordSeq::from(
                points
                    .iter()
                    .map(|&(x, y)| Point::new(x, y).unwrap())
                    .collect::<Vec<_>>(),
            ))
            .expect("test line is valid"),
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
        );
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
                assert_eq!(wkb(&certified), wkb(&forced), "{}", op.name())
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

    fn assert_intersection_matches_joint_graph(name: &str, left: &Shape, right: &Shape) -> Shape {
        let fast = overlay_all(
            &[left, right],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        let exact = super::super::nary::binary_intersection_without_convex_shortcut(
            left,
            right,
            Strictness::Strict,
        )
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
        );
        assert_eq!(union.len(), 2, "{union:?}");
        assert!(union.iter().all(|part| part.holes.is_empty()));

        let xor = binary_areal_overlay(
            std::slice::from_ref(&lower),
            std::slice::from_ref(&lower),
            OverlayOp::SymmetricDifference,
        );
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
        let unrestricted = polygon_parts_to_shape(binary_areal_overlay(
            std::slice::from_ref(&concave_left),
            std::slice::from_ref(&concave_right),
            OverlayOp::Intersection,
        ));
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
        let holed = Shape::Polygon(Polygon::new(
            polygon(&[(0.0, 0.0), (6.0, 0.0), (6.0, 6.0), (0.0, 6.0), (0.0, 0.0)]).shell,
            vec![Ring::from_closed(vec![
                Point::new(2.0, 2.0).unwrap(),
                Point::new(4.0, 2.0).unwrap(),
                Point::new(4.0, 4.0).unwrap(),
                Point::new(2.0, 4.0).unwrap(),
                Point::new(2.0, 2.0).unwrap(),
            ])],
        ));
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
                    LineSeq::try_new(CoordSeq::from(vec![
                        Point::new(2.0, 2.0).unwrap(),
                        Point::new(3.0, 2.0).unwrap(),
                    ]))
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
            LineSeq::try_new(CoordSeq::from(vec![
                point(1.0, 7.0),
                Point::new_axes(2.0, 0.0, ZOrdinate(Some(3.0)), MOrdinate(Some(9.0))).unwrap(),
            ]))
            .expect("line coordinate sequence must contain at least two vertices"),
        );
        let right = Shape::LineString(
            LineSeq::try_new(CoordSeq::from(vec![
                point(11.0, 7.0),
                Point::new_axes(2.0, 0.0, ZOrdinate(Some(13.0)), MOrdinate(Some(9.0))).unwrap(),
            ]))
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
            LineSeq::try_new(CoordSeq::from(vec![
                Point::new_axes(0.0, 0.0, ZOrdinate(Some(-0.0)), MOrdinate(None)).unwrap(),
                Point::new_axes(4.0, 0.0, ZOrdinate(Some(4.0)), MOrdinate(None)).unwrap(),
            ]))
            .expect("line coordinate sequence must contain at least two vertices"),
        );
        let xy = Shape::LineString(
            LineSeq::try_new(CoordSeq::from(vec![
                Point::new(0.0, 0.0).unwrap(),
                Point::new(4.0, 0.0).unwrap(),
            ]))
            .expect("line coordinate sequence must contain at least two vertices"),
        );
        let intersection = left.intersection(&xy, Strictness::Lenient).unwrap();
        assert_eq!(intersection.coordinate_axes(), "XYZ");
        let first = intersection.points_vec().into_iter().next().unwrap();
        assert_eq!(first.z().unwrap().to_bits(), (-0.0_f64).to_bits());
        assert!(first.m().is_none());

        let measured = Shape::LineString(
            LineSeq::try_new(CoordSeq::from(vec![
                Point::new_axes(2.0, -1.0, ZOrdinate(None), MOrdinate(Some(2.0))).unwrap(),
                Point::new_axes(2.0, 1.0, ZOrdinate(None), MOrdinate(Some(4.0))).unwrap(),
            ]))
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
            LineSeq::try_new(CoordSeq::from(vec![
                Point::new_axes(0.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
                Point::new_axes(end, 0.0, ZOrdinate(Some(z + 1.0)), MOrdinate(Some(m + 1.0)))
                    .unwrap(),
            ]))
            .expect("line coordinate sequence must contain at least two vertices")
        };
        let lines = MultiLineString::from_values(CoordinateAxes::XYZM, vec![
            MultiMember::Value(line(1.0, 10.0, 1.0)),
            MultiMember::Value(
                LineSeq::try_new(CoordSeq::from(vec![
                    Point::new_axes(1.0, 0.0, ZOrdinate(Some(99.0)), MOrdinate(Some(11.0)))
                        .unwrap(),
                    Point::new_axes(2.0, 0.0, ZOrdinate(Some(100.0)), MOrdinate(Some(12.0)))
                        .unwrap(),
                ]))
                .expect("fixture line has two vertices"),
            ),
        ]);
        let merged = Shape::MultiLineString(lines).line_merge().unwrap();
        assert_eq!(merged.coordinate_axes(), "XYM");
        merged.for_each_point(|point| assert!(point.z().is_none()));
        merged.for_each_point(|point| assert!(point.m().is_some()));
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
