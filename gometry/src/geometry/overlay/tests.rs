use crate::geometry::overlay::*;
use crate::geometry::*;
mod binary_areal_overlay_tests {
    use super::*;

    fn polygon(points: &[(f64, f64)]) -> Polygon {
        Polygon::new(
            Ring::from_trusted_closed(
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
        Shape::Polygon(Polygon::new(Ring::from_trusted_closed(points), Vec::new()))
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
        Shape::Polygon(Polygon::new(Ring::from_trusted_closed(points), Vec::new()))
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
        let area = square(0.0, 0.0, 2.0, 2.0);
        let later_disjoint_point = point(10.0, 10.0);
        let result =
            Shape::intersection_all_ordered(&[&area, &later_disjoint_point], Strictness::Strict)
                .unwrap();
        assert!(
            matches!(result, Shape::Empty(EmptyKind::Point, _)),
            "{result:?}"
        );
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
            let reduced = Shape::intersection_all_ordered(&inputs, Strictness::Strict).unwrap();
            let folded = manual_left_fold(&inputs, OverlayOp::Intersection);
            assert_topologically_equal(&reduced, &folded);
        }
    }

    #[test]
    fn intersection_all_mixed_dimension_narrows_like_left_fold() {
        let area = square(0.0, 0.0, 4.0, 4.0);
        let crossing = line(&[(-1.0, 2.0), (5.0, 2.0)]);
        let marker = point(2.0, 2.0);
        let inputs = [&area, &crossing, &marker];
        let reduced = Shape::intersection_all_ordered(&inputs, Strictness::Strict).unwrap();
        let folded = manual_left_fold(&inputs, OverlayOp::Intersection);
        assert!(matches!(reduced, Shape::Point(_)), "{reduced:?}");
        assert_topologically_equal(&reduced, &folded);
    }

    #[test]
    fn intersection_all_singleton_is_faithful_and_keeps_ordinates() {
        let input = Shape::GeometryCollection(vec![
            Shape::Point(Point::new_axes(1.0, 2.0, ZOrdinate(Some(3.0)), MOrdinate(None)).unwrap()),
            Shape::Point(Point::new_axes(4.0, 5.0, ZOrdinate(None), MOrdinate(Some(6.0))).unwrap()),
        ]);
        // A single operand intersects to itself — faithful, so both policies
        // keep Z/M (force_2d is the way to flatten).
        for strictness in [Strictness::Lenient, Strictness::Strict] {
            let kept = Shape::intersection_all_ordered(&[&input], strictness).unwrap();
            assert_eq!(kept, input);
            assert!(kept.has_z() && kept.has_m(), "{kept:?}");
        }
    }

    #[test]
    fn intersection_all_carries_z_once_from_original_inputs() {
        let first = z_square(0.0, 0.0, 4.0, 4.0, 10.0);
        let second = z_square(1.0, -1.0, 5.0, 3.0, 20.0);
        let third = z_square(2.0, 1.0, 6.0, 5.0, 30.0);
        let result =
            Shape::intersection_all_ordered(&[&first, &second, &third], Strictness::Lenient)
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
        let auto =
            Shape::intersection_all_ordered(&[&source, &interior], Strictness::Lenient).unwrap();
        assert_topologically_equal(&auto, &interior);
        assert!(!auto.has_z() && !auto.has_m(), "{auto:?}");
        // 'error' (strict=true) raises on the unsourceable survivor.
        let error = Shape::intersection_all_ordered(&[&source, &interior], Strictness::Strict);
        assert!(error.is_err(), "{error:?}");
    }

    #[test]
    fn symmetric_difference_all_permutation_invariant_for_polygons() {
        let shapes = [
            square(0.0, 0.0, 3.0, 3.0),
            square(1.0, 1.0, 4.0, 4.0),
            square(2.0, -1.0, 5.0, 2.0),
        ];
        let baseline = Shape::symmetric_difference_all_balanced(
            &[&shapes[0], &shapes[1], &shapes[2]],
            Strictness::Strict,
        )
        .unwrap();
        for order in [[2, 1, 0], [1, 0, 2], [0, 2, 1]] {
            let inputs = order
                .iter()
                .map(|&index| &shapes[index])
                .collect::<Vec<_>>();
            let reduced =
                Shape::symmetric_difference_all_balanced(&inputs, Strictness::Strict).unwrap();
            let folded = manual_left_fold(&inputs, OverlayOp::SymmetricDifference);
            assert_topologically_equal(&reduced, &baseline);
            assert_topologically_equal(&reduced, &folded);
        }
    }

    #[test]
    fn symmetric_difference_all_mixed_polygon_duplicates_leave_contained_line() {
        let polygon = square(0.0, 0.0, 4.0, 4.0);
        let contained_line = line(&[(1.0, 1.0), (3.0, 3.0)]);
        let result = Shape::symmetric_difference_all_balanced(
            &[&polygon, &polygon, &contained_line],
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
            Shape::GeometryCollection(vec![contained_line.clone(), Shape::empty_polygon()]);
        assert!(has_mixed_non_empty_topological_dimensions(&[
            &polygon,
            &polygon,
            &collection
        ]));

        let result = Shape::symmetric_difference_all_balanced(
            &[&polygon, &polygon, &collection],
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(result, contained_line);
    }

    #[test]
    fn symmetric_difference_all_mixed_line_duplicates_leave_point() {
        let line = line(&[(-1.0, 0.0), (1.0, 0.0)]);
        let point_on_line = point(0.0, 0.0);
        let result = Shape::symmetric_difference_all_balanced(
            &[&line, &line, &point_on_line],
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(result, point_on_line);
    }

    #[test]
    fn symmetric_difference_all_same_dimension_polygons_match_left_fold_fast_path() {
        let shapes = [
            square(0.0, 0.0, 2.0, 2.0),
            square(0.0, 0.0, 2.0, 2.0),
            square(3.0, 3.0, 5.0, 5.0),
            square(3.0, 3.0, 5.0, 5.0),
        ];
        let inputs = [&shapes[0], &shapes[1], &shapes[2], &shapes[3]];
        let result = Shape::symmetric_difference_all_balanced(&inputs, Strictness::Strict).unwrap();
        let folded = manual_left_fold(&inputs, OverlayOp::SymmetricDifference);
        assert_topologically_equal(&result, &folded);
        assert!(
            matches!(result, Shape::Empty(EmptyKind::Polygon, _)),
            "{result:?}"
        );
    }

    #[test]
    fn symmetric_difference_all_same_dimension_collections_stay_balanced() {
        let duplicate =
            Shape::GeometryCollection(vec![square(0.0, 0.0, 2.0, 2.0), Shape::empty_polygon()]);
        let survivor =
            Shape::GeometryCollection(vec![square(3.0, 3.0, 5.0, 5.0), Shape::empty_polygon()]);
        assert!(!has_mixed_non_empty_topological_dimensions(&[
            &duplicate, &duplicate, &survivor
        ]));

        let result = Shape::symmetric_difference_all_balanced(
            &[&duplicate, &duplicate, &survivor],
            Strictness::Strict,
        )
        .unwrap();
        assert_topologically_equal(&result, &survivor);
    }

    #[test]
    fn symmetric_difference_all_duplicate_polygons_cancel_to_polygon_empty() {
        let polygon = square(0.0, 0.0, 2.0, 2.0);
        let result =
            Shape::symmetric_difference_all_balanced(&[&polygon, &polygon], Strictness::Strict)
                .unwrap();
        assert!(
            matches!(result, Shape::Empty(EmptyKind::Polygon, _)),
            "{result:?}"
        );
    }

    #[test]
    fn symmetric_difference_all_duplicate_pair_cancels_before_survivor() {
        let duplicate = square(0.0, 0.0, 2.0, 2.0);
        let survivor = square(1.0, 1.0, 3.0, 3.0);
        let result = Shape::symmetric_difference_all_balanced(
            &[&duplicate, &duplicate, &survivor],
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(result, survivor);
    }

    #[test]
    fn symmetric_difference_all_disjoint_inputs_match_union() {
        let shapes = [square(0.0, 0.0, 1.0, 1.0), square(3.0, 3.0, 4.0, 4.0)];
        let reduced =
            Shape::symmetric_difference_all_balanced(&[&shapes[0], &shapes[1]], Strictness::Strict)
                .unwrap();
        let union = Shape::union_all(&shapes, Strictness::Strict).unwrap();
        assert_topologically_equal(&reduced, &union);
    }

    #[test]
    fn symmetric_difference_all_carries_resolvable_ordinates() {
        let first = zm_square(0.0, 0.0, 1.0, 1.0, 10.0);
        let second = zm_square(3.0, 3.0, 4.0, 4.0, 20.0);
        // Disjoint symdiff is faithful — both policies carry Z/M.
        for strictness in [Strictness::Lenient, Strictness::Strict] {
            let kept =
                Shape::symmetric_difference_all_balanced(&[&first, &second], strictness).unwrap();
            assert!(kept.has_z() && kept.has_m(), "{kept:?}");
        }
        // Overlapping symdiff: seam crossings lie on the uniform-Z/M edges, so
        // they resolve and Z/M carries through under 'auto'.
        let overlap = zm_square(0.5, 0.5, 1.5, 1.5, 30.0);
        let carried =
            Shape::symmetric_difference_all_balanced(&[&first, &overlap], Strictness::Lenient)
                .unwrap();
        assert!(carried.has_z() && carried.has_m(), "{carried:?}");
    }
}

fn grid_polygons(count: usize) -> Vec<Polygon> {
    let side = count.isqrt().max(1);
    let spacing = 10.0;
    let mut polygons = Vec::with_capacity(count);
    'grid: for row in 0..side {
        for col in 0..side {
            if polygons.len() >= count {
                break 'grid;
            }
            let minx = col as f64 * spacing;
            let miny = row as f64 * spacing;
            polygons.push(Polygon::new(
                Ring::from_trusted_closed(vec![
                    Point::new(minx, miny).unwrap(),
                    Point::new(minx + 4.0, miny).unwrap(),
                    Point::new(minx + 4.0, miny + 4.0).unwrap(),
                    Point::new(minx, miny + 4.0).unwrap(),
                    Point::new(minx, miny).unwrap(),
                ]),
                Vec::new(),
            ));
        }
    }
    polygons
}

fn grid_line_segments(polygon_count: usize) -> Vec<CoordSeq> {
    let side = polygon_count.isqrt().max(1);
    let spacing = 10.0;
    let mut lines = Vec::with_capacity(polygon_count * 2);
    for row in 0..side {
        for col in 0..side {
            let x = col as f64 * spacing + 2.0;
            let y = row as f64 * spacing + 2.0;
            lines.push(CoordSeq::from(vec![
                Point::new(x - 1.5, y).unwrap(),
                Point::new(x + 1.5, y).unwrap(),
            ]));
            lines.push(CoordSeq::from(vec![
                Point::new(x, y - 1.5).unwrap(),
                Point::new(x, y + 1.5).unwrap(),
            ]));
        }
    }
    lines
}

#[test]
fn union_lines_polygon_covers_performance_cliff() {
    use super::test_counters;

    let mut counts = Vec::new();
    for polygon_count in [4, 8, 16, 32] {
        let polygons = grid_polygons(polygon_count);
        let lines = grid_line_segments(polygon_count);
        test_counters::reset();
        test_counters::enable();
        for _ in 0..5 {
            let _ = union_lines(&lines, &polygons);
        }
        test_counters::disable();
        let count = test_counters::count() / 5;
        eprintln!("union_lines polygons={polygon_count}: {count} candidate pairs");
        counts.push((polygon_count, count));
    }
    let ratio = counts[3].1 as f64 / counts[2].1 as f64;
    assert!(
        ratio < 3.5,
        "union_lines polygon covers grew too fast (32/16 pair ratio {ratio:.2}, expected sub-quadratic)"
    );
}
