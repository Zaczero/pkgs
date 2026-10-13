use super::*;

#[test]
fn shapes_iter_size_hint_exact_mixed() {
    let storage = GeometryArrayStorage::Mixed(vec![
        Shape::Point(Point::new_raw_xy(0.0, 0.0)),
        Shape::Point(Point::new_raw_xy(1.0, 1.0)),
        Shape::typed_empty(
            crate::geometry::GeometryKind::Point,
            crate::geometry::CoordinateAxes::XY,
        ),
    ]);
    assert_shapes_iter_exact(&storage);
}

#[test]
fn shapes_iter_size_hint_exact_points_identity_window_gather() {
    let coords = Arc::new(
        CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 2.0, 3.0],
            vec![0.0, 1.0, 2.0, 3.0],
            None,
            None,
        )
        .unwrap(),
    );
    let identity = GeometryArrayStorage::Packed(PackedStorage::Points {
        coords: Arc::clone(&coords),
        row_map: RowSelection::Identity,
    });
    assert_shapes_iter_exact(&identity);

    let window = GeometryArrayStorage::Packed(PackedStorage::Points {
        coords: Arc::clone(&coords),
        row_map: row_selection_from_logical_rows(RowSelectionRef::Identity, 4, 1..3),
    });
    assert_shapes_iter_exact(&window);

    let gather = GeometryArrayStorage::Packed(PackedStorage::Points {
        coords,
        row_map: RowSelection::gather_checked_native(vec![3_usize, 0, 2], 4).unwrap(),
    });
    assert_shapes_iter_exact(&gather);
}

#[test]
fn shapes_iter_size_hint_exact_lines_identity_window_gather() {
    let coords = Arc::new(
        CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
            vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
            None,
            None,
        )
        .unwrap(),
    );
    // Three lines: verts [0,2), [2,4), [4,6)
    let offsets = crate::geometry::CsrOffsetColumn::try_new(vec![0, 2, 4, 6], 6).unwrap();
    let identity = GeometryArrayStorage::Packed(PackedStorage::Lines {
        coords: Arc::clone(&coords),
        offsets: offsets.clone(),
        row_map: RowSelection::Identity,
    });
    assert_shapes_iter_exact(&identity);

    let window = GeometryArrayStorage::Packed(PackedStorage::Lines {
        coords: Arc::clone(&coords),
        offsets: offsets.clone(),
        row_map: row_selection_from_logical_rows(RowSelectionRef::Identity, 3, 0..2),
    });
    assert_shapes_iter_exact(&window);

    let gather = GeometryArrayStorage::Packed(PackedStorage::Lines {
        coords,
        offsets,
        row_map: RowSelection::gather_checked_native(vec![2_usize, 0], 3).unwrap(),
    });
    assert_shapes_iter_exact(&gather);
}

#[test]
fn shapes_iter_size_hint_exact_polygons_identity_window_gather() {
    // Two triangles: ring verts 0..4 and 4..8 (closed).
    let coords = Arc::new(
        CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 1.0, 0.0, 10.0, 11.0, 11.0, 10.0],
            vec![0.0, 0.0, 1.0, 0.0, 10.0, 10.0, 11.0, 10.0],
            None,
            None,
        )
        .unwrap(),
    );
    let ring_offsets =
        crate::geometry::CsrOffsetColumn::<crate::geometry::RingLevel>::try_new(vec![0, 4, 8], 8)
            .unwrap();
    let polygon_offsets =
        crate::geometry::CsrOffsetColumn::<crate::geometry::PolygonLevel>::try_new(
            vec![0, 1, 2],
            2,
        )
        .unwrap();
    let identity = GeometryArrayStorage::Packed(PackedStorage::Polygons {
        coords: Arc::clone(&coords),
        ring_offsets: ring_offsets.clone(),
        polygon_offsets: polygon_offsets.clone(),
        row_map: RowSelection::Identity,
    });
    assert_shapes_iter_exact(&identity);

    let window = GeometryArrayStorage::Packed(PackedStorage::Polygons {
        coords: Arc::clone(&coords),
        ring_offsets: ring_offsets.clone(),
        polygon_offsets: polygon_offsets.clone(),
        row_map: row_selection_from_logical_rows(RowSelectionRef::Identity, 2, 1..2),
    });
    assert_shapes_iter_exact(&window);

    let gather = GeometryArrayStorage::Packed(PackedStorage::Polygons {
        coords,
        ring_offsets,
        polygon_offsets,
        row_map: RowSelection::gather_checked_native(vec![1_usize, 0], 2).unwrap(),
    });
    assert_shapes_iter_exact(&gather);
}

#[test]
fn shapes_iter_size_hint_exact_empty_storages() {
    assert_shapes_iter_exact(&GeometryArrayStorage::Mixed(Vec::new()));
    let empty_pts = GeometryArrayStorage::Packed(PackedStorage::Points {
        coords: Arc::new(CoordSeq::try_from_vecs(vec![], vec![], None, None).unwrap()),
        row_map: RowSelection::Identity,
    });
    assert_shapes_iter_exact(&empty_pts);
}
