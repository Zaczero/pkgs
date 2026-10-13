use super::test_support::{
    assert_all_points_have_z, assert_topologically_equal, line, manual_left_fold, point, square,
    z_square,
};
use crate::geometry::overlay::{OverlayOp, Strictness, overlay_all};
use crate::geometry::{GeometryKind, MOrdinate, Point, Shape, ZOrdinate};

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
