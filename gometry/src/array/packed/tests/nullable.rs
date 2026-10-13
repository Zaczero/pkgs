use super::*;

#[test]
fn nullable_points_retain_only_present_coordinates_and_rank_scalar_rows() {
    let present = PyGeometryArray::packed_points(
        CoordSeq::try_from_vecs(vec![1.0, 3.0], vec![2.0, 4.0], None, None).unwrap(),
        Frame::None,
    )
    .unwrap();
    let GeometryArrayStorage::Packed(PackedStorage::Points {
        coords: present_coords,
        ..
    }) = present.storage()
    else {
        unreachable!()
    };
    let present_coords = Arc::clone(present_coords);
    let mask = ValidityMask::from_vec(5, vec![true, false, true, false, true]).unwrap();
    let array = PyGeometryArray::scatter_present_rows(&present, mask).unwrap();
    assert_eq!(array.len(), 5);
    let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) = array.storage()
    else {
        panic!("nullable points remain homogeneous")
    };
    assert!(row_map.is_identity());
    assert_eq!(coords.len(), 2);
    assert!(Arc::ptr_eq(coords, &present_coords));
    assert_eq!(
        array.geometry_at(1).unwrap().shape.shape(),
        &Shape::Point(Point::new_raw_xy(1.0, 2.0))
    );
    assert_eq!(
        array.geometry_at(3).unwrap().shape.shape(),
        &Shape::Point(Point::new_raw_xy(3.0, 4.0))
    );
}

#[test]
fn compact_nullable_storage_charges_present_coordinates_once() {
    let present = PyGeometryArray::packed_points(
        CoordSeq::try_from_vecs(vec![1.0, 3.0], vec![2.0, 4.0], None, None).unwrap(),
        Frame::None,
    )
    .unwrap();
    let compact = PyGeometryArray::scatter_present_rows(
        &present,
        ValidityMask::from_vec(3, vec![false, true, false]).unwrap(),
    )
    .unwrap();

    assert_eq!(
        compact.storage().logical_heap_bytes(),
        present.storage().logical_heap_bytes(),
        "logical nullable rows must not be charged as physical coordinates"
    );
}

#[test]
fn compact_point_ranges_ignore_missing_logical_rows() {
    let present =
        PyGeometryArray::packed_points(CoordSeq::empty(CoordinateAxes::XY), Frame::None).unwrap();
    let compact = PyGeometryArray::scatter_present_rows(
        &present,
        ValidityMask::from_vec(3, vec![true, true, true]).unwrap(),
    )
    .unwrap();

    assert_eq!(
        compact.logical_heap_bytes_range(0..3),
        compact.logical_heap_bytes_range(0..0),
        "compact point ranges must not turn missing rows into coordinates"
    );
}

#[test]
fn drop_missing_reuses_compact_points_without_prefix_rank_work() {
    let present = PyGeometryArray::packed_points(
        CoordSeq::try_from_vecs(
            (0..10_000).map(f64::from).collect(),
            (0..10_000).map(|row| f64::from(row * 2)).collect(),
            None,
            None,
        )
        .unwrap(),
        Frame::None,
    )
    .unwrap();
    let array = PyGeometryArray::scatter_present_rows(
        &present,
        ValidityMask::from_vec(10_001, (0..10_001).map(|row| row == 9_999).collect()).unwrap(),
    )
    .unwrap();
    let source_coords = match array.storage() {
        GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) => Arc::clone(coords),
        GeometryArrayStorage::Packed(
            PackedStorage::Lines { .. } | PackedStorage::Polygons { .. },
        )
        | GeometryArrayStorage::Mixed(_) => {
            panic!("nullable point fixture must remain packed")
        },
    };

    missing::reset_count_valid_bytes();
    let dense = array.try_drop_missing().unwrap();

    assert_eq!(dense.len(), 10_000);
    assert!(!dense.has_missing());
    assert_eq!(missing::count_valid_bytes(), 0);
    assert!(matches!(
        dense.storage(),
        GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. })
            if Arc::ptr_eq(coords, &source_coords)
    ));
}

#[test]
fn nullable_line_offsets_are_logical_without_missing_payload() {
    let present = PyGeometryArray::packed_lines(
        CoordSeq::try_from_vecs(vec![0.0, 1.0], vec![2.0, 3.0], None, None).unwrap(),
        CsrOffsetColumn::try_new(vec![0, 0, 2], 2).unwrap(),
        Frame::None,
    )
    .unwrap();
    let mask = ValidityMask::from_vec(4, vec![true, false, false, true]).unwrap();
    let array = PyGeometryArray::scatter_present_rows(&present, mask).unwrap();
    let GeometryArrayStorage::Packed(PackedStorage::Lines {
        coords, offsets, ..
    }) = array.storage()
    else {
        panic!("nullable lines remain homogeneous")
    };
    assert_eq!(coords.len(), 2);
    assert_eq!(offsets.as_slice(), &[0, 0, 0, 2, 2]);
    assert!(matches!(
        array.logical_row(0),
        Some(LogicalRow::Missing { .. })
    ));
    assert!(matches!(
        array.logical_row(1),
        Some(LogicalRow::Present {
            row: ShapeRow::Packed(row),
            ..
        }) if row.vertex_window() == Some(0..0)
    ));
    assert!(matches!(
        array.logical_row(2),
        Some(LogicalRow::Present {
            row: ShapeRow::Packed(row),
            ..
        }) if row.vertex_window() == Some(0..2)
    ));
    assert!(matches!(
        array.logical_row(3),
        Some(LogicalRow::Missing { .. })
    ));
}

#[test]
fn point_validity_never_zips_against_present_payload_positions() {
    let present = PyGeometryArray::packed_points(
        CoordSeq::try_from_vecs(vec![10.0, 20.0], vec![1.0, 2.0], None, None).unwrap(),
        Frame::None,
    )
    .unwrap();
    for missing in [
        vec![true, false, false],
        vec![false, true, false],
        vec![false, false, true],
        vec![true, false, true, false],
    ] {
        let mask = ValidityMask::from_vec(missing.len(), missing.clone()).unwrap();
        let array = PyGeometryArray::scatter_present_rows(&present, mask).unwrap();
        let mut values = [10.0, 20.0].into_iter();
        let expected = missing
            .iter()
            .map(|&missing| (!missing).then(|| values.next().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(logical_point_xs(&array), expected);
    }
}

#[test]
fn nullable_layout_mismatch_is_rejected_at_result_admission() {
    let storage = Arc::new(GeometryArrayStorage::Packed(PackedStorage::Points {
        coords: Arc::new(
            CoordSeq::try_from_vecs(vec![1.0, 2.0], vec![3.0, 4.0], None, None).unwrap(),
        ),
        row_map: RowSelection::Identity,
    }));
    let mask = ValidityMask::from_vec(3, vec![false, true, false]).unwrap();
    let error = PyGeometryArray::from_result_storage(
        storage,
        Frame::None,
        Some(mask),
        StorageLayout::LogicalRows,
        ComputedCaches::fresh(),
    )
    .unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(crate::geometry::GeometryErrorKind::Invalid(_))
    ));
}

#[test]
fn already_masked_results_are_not_scattered_again() {
    let present = PyGeometryArray::packed_points(
        CoordSeq::try_from_vecs(vec![1.0], vec![2.0], None, None).unwrap(),
        Frame::None,
    )
    .unwrap();
    let source = PyGeometryArray::scatter_present_rows(
        &present,
        ValidityMask::from_vec(2, vec![false, true]).unwrap(),
    )
    .unwrap();
    let result = source.scatter_present_result(source.clone()).unwrap();
    assert!(Arc::ptr_eq(result.storage_arc(), source.storage_arc()));
    assert!(result.row_products.shares_owner_with(&source.row_products));
}

#[test]
fn newly_computed_data_never_reuses_source_products() {
    let source =
        PyGeometryArray::from_shapes(vec![Shape::Point(Point::new_raw_xy(1.0, 2.0))], Frame::None)
            .unwrap();
    let source_cache = source.row_products.frame_cache(0);
    let computed =
        PyGeometryArray::from_shapes(vec![Shape::Point(Point::new_raw_xy(1.0, 2.0))], Frame::None)
            .unwrap();
    let computed_cache = computed.row_products.frame_cache(0);
    assert!(!Arc::ptr_eq(&source_cache, &computed_cache));
    assert!(
        !computed
            .row_products
            .shares_owner_with(&source.row_products)
    );
}

#[test]
fn sliced_xyzm_selection_keeps_the_independent_m_owner_and_window() {
    let m = SharedSlice::from_vec((50..60).map(f64::from).collect());
    let coords = CoordSeq::from_shared_columns_with_window(
        SharedSlice::from_vec((0..6).map(f64::from).collect()),
        SharedSlice::from_vec(vec![10.0; 6]),
        None,
        Some(MColumn::Local {
            owner: m.clone(),
            offset: 4,
        }),
        CoordWindow::from_range(2..5, 6),
    )
    .unwrap();
    let source = PyGeometryArray::packed_points(coords, Frame::None).unwrap();
    let selected = source.gather_logical_rows(&[2, 0]).unwrap();
    let values = selected
        .logical_rows()
        .map(|row| match row {
            LogicalRow::Present {
                row: ShapeRow::Point(point),
                ..
            } => point.m(),
            LogicalRow::Missing { .. } | LogicalRow::Present { .. } => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(values, [Some(56.0), Some(54.0)]);
    let GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) = selected.storage()
    else {
        panic!("selection changed point storage kind");
    };
    assert!(Arc::ptr_eq(
        &coords.column_arcs().m_parts().unwrap().0.owner().unwrap(),
        &m.owner().unwrap(),
    ));
}

#[test]
fn nullable_polygon_distinguishes_missing_from_typed_empty() {
    let array = crate::test_support::nullable_polygon_typed_empty_fixture();
    let GeometryArrayStorage::Packed(PackedStorage::Polygons {
        coords,
        polygon_offsets,
        ..
    }) = array.storage()
    else {
        panic!("nullable polygons remain homogeneous")
    };
    assert_eq!(coords.len(), 5);
    assert_eq!(polygon_offsets.as_slice(), &[0, 0, 0, 0, 1]);
    assert!(matches!(
        array.logical_row(0),
        Some(LogicalRow::Missing { .. })
    ));
    assert!(matches!(
        array.logical_row(1),
        Some(LogicalRow::Present {
            row: ShapeRow::Packed(row),
            ..
        }) if row.polygon_rings() == Some(0..0)
    ));
    assert!(matches!(
        array.logical_row(2),
        Some(LogicalRow::Missing { .. })
    ));
    assert!(matches!(
        array.logical_row(3),
        Some(LogicalRow::Present {
            row: ShapeRow::Packed(row),
            ..
        }) if row.polygon_rings() == Some(0..1)
    ));

    let empty = Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XY);
    let Some(LogicalRow::Present { row, .. }) = array.logical_row(1) else {
        panic!("the present zero-ring row must remain present");
    };
    assert_eq!(row.with_shape(Clone::clone).unwrap(), empty);
    assert_eq!(
        array.storage().row(1).with_shape(Clone::clone).unwrap(),
        empty
    );

    let mut shapes = array.storage().iter_shapes();
    assert_eq!(shapes.next().unwrap().unwrap().as_ref(), &empty);
    assert_eq!(shapes.next().unwrap().unwrap().as_ref(), &empty);

    assert_eq!(array.is_simple_unary_packed().unwrap().unwrap(), [
        false, true, false, true
    ]);
    assert_eq!(array.is_valid_unary_packed().unwrap().unwrap(), [
        false, true, false, true
    ]);

    let shape_at = |array: &PyGeometryArray, logical| match array
        .logical_row(logical)
        .expect("test row is in range")
    {
        LogicalRow::Present { row, .. } => row.with_shape(Clone::clone).unwrap(),
        LogicalRow::Missing { .. } => panic!("test row is present"),
    };
    let selected = array.gather_logical_rows(&[3, 1, 3]).unwrap();
    assert_eq!(shape_at(&selected, 0), shape_at(&array, 3));
    assert_eq!(shape_at(&selected, 1), empty);
    assert_eq!(shape_at(&selected, 2), shape_at(&array, 3));
}

#[test]
fn nonidentity_nullable_compaction_reserves_selected_duplicate_payload() {
    let coords = CoordSeq::try_from_vecs(vec![0.0, 1.0], vec![0.0, 1.0], None, None).unwrap();
    let storage = Arc::new(GeometryArrayStorage::Packed(PackedStorage::Lines {
        coords: Arc::new(coords),
        offsets: CsrOffsetColumn::try_new(vec![0, 2], 2).unwrap(),
        row_map: RowSelection::gather_checked_native(vec![0, 0, 0], 1).unwrap(),
    }));
    let mask = ValidityMask::from_vec(4, vec![false, false, false, true]).unwrap();
    let result = PyGeometryArray::from_result_storage(
        storage,
        Frame::None,
        Some(mask),
        StorageLayout::PresentOnly,
        ComputedCaches::fresh(),
    )
    .unwrap();
    let GeometryArrayStorage::Packed(PackedStorage::Lines { coords, .. }) = result.storage() else {
        panic!("duplicate line selection must stay packed");
    };
    assert_eq!(coords.len(), 6);
    assert!(coords.column_arcs().xs.capacity() >= 6);

    let coords = CoordSeq::try_from_vecs(
        vec![0.0, 1.0, 1.0, 0.0, 0.0],
        vec![0.0, 0.0, 1.0, 1.0, 0.0],
        None,
        None,
    )
    .unwrap();
    let storage = Arc::new(GeometryArrayStorage::Packed(PackedStorage::Polygons {
        coords: Arc::new(coords),
        ring_offsets: CsrOffsetColumn::<RingLevel>::try_new(vec![0, 5], 5).unwrap(),
        polygon_offsets: CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 1], 1).unwrap(),
        row_map: RowSelection::gather_checked_native(vec![0, 0], 1).unwrap(),
    }));
    let mask = ValidityMask::from_vec(3, vec![false, false, true]).unwrap();
    let result = PyGeometryArray::from_result_storage(
        storage,
        Frame::None,
        Some(mask),
        StorageLayout::PresentOnly,
        ComputedCaches::fresh(),
    )
    .unwrap();
    let GeometryArrayStorage::Packed(PackedStorage::Polygons { coords, .. }) = result.storage()
    else {
        panic!("duplicate polygon selection must stay packed");
    };
    assert_eq!(coords.len(), 10);
    assert!(coords.column_arcs().xs.capacity() >= 10);
}
