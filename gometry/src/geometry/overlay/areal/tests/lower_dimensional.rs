use super::test_support::{EVERY_OP, disjoint_multiline, line, point, square, wkb, zm_line};
use crate::geometry::overlay::{OverlayOp, Strictness, overlay_all};
use crate::geometry::{CoordinateAxes, GeometryKind, Shape};

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
