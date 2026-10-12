use super::test_support::{line, multiline, point, polygon, z_square, zm_line};
use crate::geometry::overlay::{OverlayOp, Strictness, overlay_all};
use crate::geometry::{
    CoordSeq, CoordinateAxes, GeometryErrorKind, LineSeq, MOrdinate, MultiLineString, MultiMember,
    Point, Shape, ZOrdinate,
};

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
    let z_point =
        Shape::Point(Point::new_axes(1.0, 0.0, ZOrdinate(Some(10.0)), MOrdinate(None)).unwrap());
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
