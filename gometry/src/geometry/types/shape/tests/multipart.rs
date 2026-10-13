use super::*;

fn xyz_point(x: f64, y: f64, z: f64) -> Point {
    Point::new_raw_axes(
        x,
        y,
        crate::geometry::ZOrdinate(Some(z)),
        crate::geometry::MOrdinate(None),
    )
}

fn xyz_line() -> LineSeq {
    let points = [xyz_point(0.0, 0.0, 1.0), xyz_point(1.0, 1.0, 2.0)];
    LineSeq::try_new(CoordSeq::try_from_points(&points).expect("XYZ points")).expect("valid line")
}

fn xyz_polygon() -> Polygon {
    let points = [
        xyz_point(0.0, 0.0, 1.0),
        xyz_point(1.0, 0.0, 2.0),
        xyz_point(0.0, 1.0, 3.0),
        xyz_point(0.0, 0.0, 1.0),
    ];
    let shell = crate::geometry::Ring::try_from_closed(
        CoordSeq::try_from_points(&points).expect("XYZ ring coordinates"),
    )
    .expect("closed ring");
    Polygon::new(shell, Vec::new())
}

#[test]
fn dense_shapes_infer_member_axes_and_keep_declared_empty_axes() {
    let Shape::MultiLineString(lines) =
        Shape::multi_line_string(CoordinateAxes::XY, vec![xyz_line()])
    else {
        unreachable!("nonempty dense line payload");
    };
    assert_eq!(lines.axes(), CoordinateAxes::XYZ);
    assert_eq!(
        Shape::multi_line_string(CoordinateAxes::XYZM, Vec::new()),
        Shape::typed_empty(GeometryKind::MultiLineString, CoordinateAxes::XYZM)
    );
    assert_eq!(
        Shape::narrow_lines(CoordinateAxes::XYZM, Vec::new()),
        Shape::typed_empty(GeometryKind::LineString, CoordinateAxes::XYZM)
    );

    let Shape::MultiPolygon(polygons) =
        Shape::multi_polygon(CoordinateAxes::XY, vec![xyz_polygon()])
    else {
        unreachable!("nonempty dense polygon payload");
    };
    assert_eq!(polygons.axes(), CoordinateAxes::XYZ);
    assert_eq!(
        Shape::multi_polygon(CoordinateAxes::XYZM, Vec::new()),
        Shape::typed_empty(GeometryKind::MultiPolygon, CoordinateAxes::XYZM)
    );
    assert_eq!(
        Shape::narrow_polygons(CoordinateAxes::XYZM, Vec::new()),
        Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XYZM)
    );
}

#[test]
fn sparse_shapes_keep_empty_positions_and_member_admission_errors() {
    let lines = MultiLineString::try_new(CoordinateAxes::XYZ, vec![
        MultiMember::Empty,
        MultiMember::Value(xyz_line()),
        MultiMember::Empty,
    ])
    .expect("sparse lines");
    assert!(matches!(&lines.members, MultipartMembers::Sparse(_)));
    assert_eq!(lines.members().collect::<Vec<_>>(), [
        MultiMember::Empty,
        MultiMember::Value(0),
        MultiMember::Empty,
    ]);

    let polygons = MultiPolygon::try_new(CoordinateAxes::XYZ, vec![
        MultiMember::Empty,
        MultiMember::Value(xyz_polygon()),
        MultiMember::Empty,
    ])
    .expect("sparse polygons");
    assert!(matches!(&polygons.members, MultipartMembers::Sparse(_)));
    assert_eq!(polygons.members().collect::<Vec<_>>(), [
        MultiMember::Empty,
        MultiMember::Value(0),
        MultiMember::Empty,
    ]);

    let error = MultiLineString::try_new(CoordinateAxes::XY, vec![MultiMember::Value(xyz_line())])
        .expect_err("axes mismatch is still admitted as an error");
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::CoordinateAxesMismatch {
            declared: CoordinateAxes::XY,
            got: CoordinateAxes::XYZ,
        })
    ));

    let error = MultiPolygon::try_new(CoordinateAxes::XYZ, Vec::new())
        .expect_err("a multipart without any members is not admitted");
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::EmptyGeometrySequence {
            operation: "geometry payload",
        })
    ));
}

#[test]
fn dense_and_sparse_members_are_canonical() {
    let dense = MultiPoint::try_new(CoordinateAxes::XY, vec![
        MultiMember::Value(Point::new_raw_xy(1.0, 2.0)),
        MultiMember::Value(Point::new_raw_xy(3.0, 4.0)),
    ])
    .expect("dense multipoint");
    assert!(matches!(dense.members, MultipartMembers::Dense));
    assert_eq!(dense.members().collect::<Vec<_>>(), [
        MultiMember::Value(0),
        MultiMember::Value(1)
    ]);
    assert!(!dense.has_empty_members());
    assert_eq!(dense.members.heap_bytes(), 0);

    let sparse = MultiPoint::try_new(CoordinateAxes::XY, vec![
        MultiMember::Empty,
        MultiMember::Value(Point::new_raw_xy(1.0, 2.0)),
        MultiMember::Empty,
    ])
    .expect("sparse multipoint");
    assert!(matches!(sparse.members, MultipartMembers::Sparse(_)));
    assert_eq!(sparse.members().collect::<Vec<_>>(), [
        MultiMember::Empty,
        MultiMember::Value(0),
        MultiMember::Empty
    ]);
    assert!(sparse.has_empty_members());
    let MultipartMembers::Sparse(members) = &sparse.members else {
        unreachable!("sparse fixture must retain member metadata");
    };
    assert!(sparse.members.heap_bytes() >= std::mem::size_of_val(&**members));
}

#[test]
fn multipoint_value_iteration_skips_empty_members_and_counts_values() {
    let first = Point::new_raw_axes(
        1.0,
        2.0,
        crate::geometry::ZOrdinate(Some(3.0)),
        crate::geometry::MOrdinate(Some(4.0)),
    );
    let second = Point::new_raw_axes(
        11.0,
        12.0,
        crate::geometry::ZOrdinate(Some(13.0)),
        crate::geometry::MOrdinate(Some(14.0)),
    );
    let points = MultiPoint::try_new(CoordinateAxes::XYZM, vec![
        MultiMember::Empty,
        MultiMember::Value(first),
        MultiMember::Empty,
        MultiMember::Value(second),
        MultiMember::Empty,
    ])
    .expect("sparse XYZM multipoint");

    assert_eq!(points.axes(), CoordinateAxes::XYZM);
    assert_eq!(points.member_count(), 5);
    assert_eq!(points.members().collect::<Vec<_>>(), [
        MultiMember::Empty,
        MultiMember::Value(0),
        MultiMember::Empty,
        MultiMember::Value(1),
        MultiMember::Empty,
    ]);
    let mut values = points.iter();
    assert_eq!(values.len(), 2);
    assert_eq!(values.next(), Some(first));
    assert_eq!(values.len(), 1);
    assert_eq!(values.next_back(), Some(second));
    assert_eq!(values.len(), 0);
    assert_eq!(values.next(), None);
    assert_eq!(points.points().collect::<Vec<_>>(), [first, second]);
    assert_eq!((&points).into_iter().collect::<Vec<_>>(), [first, second]);
    assert_eq!(points.to_vec(), [first, second]);

    let empty_members = MultiPoint::try_new(CoordinateAxes::XYZM, vec![
        MultiMember::Empty,
        MultiMember::Empty,
    ])
    .expect("all-empty multipoint");
    assert_eq!(empty_members.axes(), CoordinateAxes::XYZM);
    assert!(empty_members.is_empty());
    assert_eq!(empty_members.iter().len(), 0);
    assert_eq!(empty_members.members().collect::<Vec<_>>(), [
        MultiMember::Empty,
        MultiMember::Empty
    ]);
}

#[test]
fn sparse_value_mapping_reuses_member_layout() {
    let line = LineSeq::segment(Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(1.0, 1.0))
        .expect("finite line");
    let source = MultiLineString::try_new(CoordinateAxes::XY, vec![
        MultiMember::Empty,
        MultiMember::Value(line.clone()),
        MultiMember::Empty,
        MultiMember::Value(line),
    ])
    .expect("sparse lines");
    let mut mapped_count = 0;
    let mapped = source
        .try_map_values(CoordinateAxes::XY, |line| {
            mapped_count += 1;
            Ok::<_, crate::error::Error>(line.clone())
        })
        .expect("identity mapping");
    assert_eq!(mapped_count, 2);
    assert_eq!(
        mapped.members().collect::<Vec<_>>(),
        source.members().collect::<Vec<_>>()
    );
    let (MultipartMembers::Sparse(source_layout), MultipartMembers::Sparse(mapped_layout)) =
        (&source.members, &mapped.members)
    else {
        unreachable!("sparse mapping retains member positions");
    };
    assert!(std::ptr::eq(
        source_layout.as_slice().as_ptr(),
        mapped_layout.as_slice().as_ptr()
    ));
}

#[test]
fn sparse_point_and_line_maps_keep_member_layout_when_axes_change() {
    let point = Point::new_raw_xy(1.0, 2.0);
    let source_points = MultiPoint::try_new(CoordinateAxes::XY, vec![
        MultiMember::Empty,
        MultiMember::Value(point),
    ])
    .unwrap();
    let mapped_points = source_points
        .try_map_values(|point| {
            Ok::<_, crate::error::Error>(Point::new_raw_axes(
                point.x,
                point.y,
                crate::geometry::ZOrdinate(Some(3.0)),
                crate::geometry::MOrdinate(None),
            ))
        })
        .unwrap();
    assert_eq!(mapped_points.axes(), CoordinateAxes::XYZ);
    let (MultipartMembers::Sparse(before_points), MultipartMembers::Sparse(after_points)) =
        (&source_points.members, &mapped_points.members)
    else {
        unreachable!("point mapping keeps sparse positions");
    };
    assert!(std::ptr::eq(
        before_points.as_slice().as_ptr(),
        after_points.as_slice().as_ptr()
    ));

    let line = LineSeq::segment(point, Point::new_raw_xy(2.0, 3.0)).unwrap();
    let source_lines = MultiLineString::try_new(CoordinateAxes::XY, vec![
        MultiMember::Empty,
        MultiMember::Value(line),
    ])
    .unwrap();
    let mapped_lines = Shape::MultiLineString(source_lines.clone())
        .map_points(&|point| {
            Ok(Point::new_raw_axes(
                point.x,
                point.y,
                crate::geometry::ZOrdinate(Some(3.0)),
                crate::geometry::MOrdinate(None),
            ))
        })
        .unwrap();
    let Shape::MultiLineString(mapped_lines) = mapped_lines else {
        unreachable!("point mapping keeps multiline shape");
    };
    assert_eq!(mapped_lines.axes(), CoordinateAxes::XYZ);
    let (MultipartMembers::Sparse(before_lines), MultipartMembers::Sparse(after_lines)) =
        (&source_lines.members, &mapped_lines.members)
    else {
        unreachable!("line mapping keeps sparse positions");
    };
    assert!(std::ptr::eq(
        before_lines.as_slice().as_ptr(),
        after_lines.as_slice().as_ptr()
    ));
}
