use super::*;

#[test]
fn packed_size_endpoints_match_row_replay_for_views_and_gathers() {
    let lines = measured_lines();
    let line_window = lines.gather_logical_row_range(1..5).unwrap();
    let line_nested = line_window.gather_logical_row_range(1..3).unwrap();
    let line_gather = lines.gather_logical_rows(&[3, 1, 3, 0]).unwrap();
    let nullable_lines = PyGeometryArray::scatter_present_rows(
        &lines.gather_logical_rows(&[0, 2, 3]).unwrap(),
        ValidityMask::from_vec(5, vec![false, true, false, false, true]).unwrap(),
    )
    .unwrap();

    let polygons = measured_polygons();
    let polygon_window = polygons.gather_logical_row_range(1..4).unwrap();
    let polygon_nested = polygon_window.gather_logical_row_range(1..3).unwrap();
    let polygon_gather = polygons.gather_logical_rows(&[3, 0, 3, 1]).unwrap();
    let nullable_polygons = PyGeometryArray::scatter_present_rows(
        &polygons.gather_logical_rows(&[0, 2, 3]).unwrap(),
        ValidityMask::from_vec(4, vec![false, true, false, false]).unwrap(),
    )
    .unwrap();

    let points = measured_points();
    let point_window = points.gather_logical_row_range(1..5).unwrap();
    let point_gather = points.gather_logical_rows(&[4, 1, 4, 0]).unwrap();
    let nullable_points = PyGeometryArray::scatter_present_rows(
        &points.gather_logical_rows(&[0, 2, 4]).unwrap(),
        ValidityMask::from_vec(5, vec![false, true, false, true, false]).unwrap(),
    )
    .unwrap();
    let nullable_point_window = nullable_points.gather_logical_row_range(1..5).unwrap();

    let shared_shape = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_vecs(vec![0.0, 2.0, 4.0], vec![1.0, 3.0, 5.0], None, None).unwrap(),
        )
        .unwrap(),
    );
    let mixed =
        PyGeometryArray::mixed_shapes(vec![shared_shape.clone(), shared_shape], Frame::None)
            .unwrap();

    let arrays = [
        &lines,
        &line_window,
        &line_nested,
        &nullable_lines,
        &polygons,
        &polygon_window,
        &polygon_nested,
        &nullable_polygons,
        &points,
        &point_window,
        &nullable_points,
        &nullable_point_window,
        &mixed,
    ];
    for array in arrays {
        assert_eq!(
            array.storage().logical_heap_bytes(),
            prior_storage_heap_bytes(array.storage())
        );
        let full = 0..array.len();
        assert_eq!(
            array.logical_heap_bytes_range(full.clone()),
            prior_range_heap_bytes(array, full)
        );
        let selected = if array.len() > 2 {
            1..array.len().wrapping_sub(1)
        } else {
            0..array.len()
        };
        assert_eq!(
            array.logical_heap_bytes_range(selected.clone()),
            prior_range_heap_bytes(array, selected)
        );
    }

    let contiguous = [
        &lines,
        &line_window,
        &line_nested,
        &nullable_lines,
        &polygons,
        &polygon_window,
        &polygon_nested,
        &nullable_polygons,
        &points,
        &point_window,
        &nullable_points,
        &nullable_point_window,
    ];
    storage::reset_csr_size_row_visits();
    for array in contiguous {
        let _ = array.storage().logical_heap_bytes();
        let _ = array.logical_heap_bytes_range(0..array.len());
    }
    assert_eq!(storage::csr_size_row_visits(), 0);

    for array in [&line_gather, &polygon_gather, &point_gather] {
        assert_eq!(
            array.storage().logical_heap_bytes(),
            prior_storage_heap_bytes(array.storage())
        );
        assert_eq!(
            array.logical_heap_bytes_range(0..array.len()),
            prior_range_heap_bytes(array, 0..array.len())
        );
    }
}

#[rstest::rstest]
#[case::disabled(false, false, CoordinateAxes::XY)]
#[case::enabled(true, false, CoordinateAxes::XYZ)]
#[case::false_be2275af(false, true, CoordinateAxes::XYM)]
#[case::true_141345ef(true, true, CoordinateAxes::XYZM)]
fn compact_points_preserve_every_coordinate_layout(
    #[case] z: bool,
    #[case] m: bool,
    #[case] axes: CoordinateAxes,
) {
    let present = PyGeometryArray::packed_points(
        CoordSeq::try_from_vecs(
            vec![1.0],
            vec![2.0],
            z.then(|| vec![3.0]),
            m.then(|| vec![4.0]),
        )
        .unwrap(),
        Frame::None,
    )
    .unwrap();
    let array = PyGeometryArray::scatter_present_rows(
        &present,
        ValidityMask::from_vec(3, vec![true, false, true]).unwrap(),
    )
    .unwrap();
    assert_eq!(array.uniform_axes(), Some(axes));
    let all_missing = array.gather_logical_rows(&[0, 2]).unwrap();
    assert_eq!(all_missing.uniform_axes(), Some(axes));
}

#[test]
fn point_gathers_preserve_missing_order_duplicates_and_all_missing_type() {
    let present = PyGeometryArray::packed_points(
        CoordSeq::try_from_vecs(vec![10.0, 20.0], vec![1.0, 2.0], None, None).unwrap(),
        Frame::None,
    )
    .unwrap();
    let array = PyGeometryArray::scatter_present_rows(
        &present,
        ValidityMask::from_vec(4, vec![true, false, true, false]).unwrap(),
    )
    .unwrap();
    let gathered = array.gather_logical_rows(&[3, 0, 1, 3]).unwrap();
    assert_eq!(logical_point_xs(&gathered), [
        Some(20.0),
        None,
        Some(10.0),
        Some(20.0)
    ]);
    let nested = gathered.gather_logical_rows(&[2, 0, 2]).unwrap();
    assert_eq!(logical_point_xs(&nested), [
        Some(10.0),
        Some(20.0),
        Some(10.0)
    ]);
    let missing = array.gather_logical_rows(&[0, 2, 0]).unwrap();
    assert!(
        matches!(missing.storage(), GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) if coords.is_empty())
    );
    assert_eq!(logical_point_xs(&missing), [None, None, None]);
    assert_eq!(
        missing.row_products.heap_bytes(),
        crate::heap_size::ARC_CONTROL_BYTES + std::mem::size_of::<prepared::RowProductOwner>()
    );
    assert!(matches!(
        missing.logical_row(1),
        Some(LogicalRow::Missing { .. })
    ));
    assert_eq!(
        missing.row_products.heap_bytes(),
        crate::heap_size::ARC_CONTROL_BYTES + std::mem::size_of::<prepared::RowProductOwner>()
    );
}

#[test]
fn line_and_polygon_gathers_keep_nested_csr_windows_and_missing_rows() {
    let mask = ValidityMask::from_vec(5, vec![false, true, false, false, true]).unwrap();
    let lines = PyGeometryArray::packed_lines(
        CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 10.0, 11.0, 20.0, 21.0],
            vec![0.0; 6],
            None,
            None,
        )
        .unwrap(),
        CsrOffsetColumn::try_new(vec![0, 2, 4, 6], 6).unwrap(),
        Frame::None,
    )
    .unwrap();
    let lines = PyGeometryArray::scatter_present_rows(&lines, mask.clone()).unwrap();
    let selected = lines.gather_logical_rows(&[3, 1, 0, 3]).unwrap();
    let line_first_x = |array: &PyGeometryArray| {
        array
            .logical_rows()
            .map(|row| match row {
                LogicalRow::Missing { .. } => None,
                LogicalRow::Present { row, .. } => Some(
                    row.with_shape(|shape| {
                        if let Shape::LineString(line) = shape {
                            line.xs().first().copied().unwrap_or(f64::NAN)
                        } else {
                            f64::NAN
                        }
                    })
                    .expect("selected packed line row materializes"),
                ),
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(line_first_x(&selected), [
        Some(20.0),
        None,
        Some(0.0),
        Some(20.0)
    ]);
    let nested = selected.gather_logical_rows(&[3, 0, 3]).unwrap();
    assert_eq!(line_first_x(&nested), [Some(20.0), Some(20.0), Some(20.0)]);

    let polygons = PyGeometryArray::packed_polygons(
        CoordSeq::try_from_vecs(
            vec![
                0.0, 1.0, 1.0, 0.0, 0.0, 10.0, 11.0, 11.0, 10.0, 10.0, 20.0, 21.0, 21.0, 20.0, 20.0,
            ],
            vec![
                0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0,
            ],
            None,
            None,
        )
        .unwrap(),
        CsrOffsetColumn::<RingLevel>::try_new(vec![0, 5, 10, 15], 15).unwrap(),
        CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 1, 2, 3], 3).unwrap(),
        Frame::None,
    )
    .unwrap();
    let polygons = PyGeometryArray::scatter_present_rows(&polygons, mask).unwrap();
    let selected = polygons.gather_logical_rows(&[3, 1, 0, 3]).unwrap();
    let polygon_first_x = |array: &PyGeometryArray| {
        array
            .logical_rows()
            .map(|row| match row {
                LogicalRow::Missing { .. } => None,
                LogicalRow::Present { row, .. } => Some(
                    row.with_shape(|shape| {
                        if let Shape::Polygon(polygon) = shape {
                            polygon
                                .shell
                                .coords()
                                .xs()
                                .first()
                                .copied()
                                .unwrap_or(f64::NAN)
                        } else {
                            f64::NAN
                        }
                    })
                    .expect("selected packed polygon row materializes"),
                ),
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(polygon_first_x(&selected), [
        Some(20.0),
        None,
        Some(0.0),
        Some(20.0)
    ]);
    let nested = selected.gather_logical_rows(&[3, 0, 3]).unwrap();
    assert_eq!(polygon_first_x(&nested), [
        Some(20.0),
        Some(20.0),
        Some(20.0)
    ]);
}

#[test]
fn masking_dense_lines_and_polygons_removes_missing_csr_payload_windows() {
    let lines = PyGeometryArray::packed_lines(
        CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 10.0, 11.0, 20.0, 21.0],
            vec![0.0; 6],
            None,
            None,
        )
        .unwrap(),
        CsrOffsetColumn::try_new(vec![0, 2, 4, 6], 6).unwrap(),
        Frame::None,
    )
    .unwrap();
    let source_line_cache = lines.row_products.frame_cache(0);
    let lines = lines
        .with_missing_mask(ValidityMask::from_vec(3, vec![false, true, false]))
        .unwrap();
    let GeometryArrayStorage::Packed(PackedStorage::Lines {
        coords, offsets, ..
    }) = lines.storage()
    else {
        panic!("masked lines changed storage kind");
    };
    assert_eq!(coords.len(), 4);
    assert_eq!(offsets.as_slice(), &[0, 2, 2, 4]);
    assert!(matches!(
        lines.logical_row(1),
        Some(LogicalRow::Missing { logical: 1 })
    ));
    assert!(Arc::ptr_eq(
        &source_line_cache,
        &lines.row_products.frame_cache(0),
    ));

    let polygons = PyGeometryArray::packed_polygons(
        CoordSeq::try_from_vecs(
            vec![
                0.0, 1.0, 1.0, 0.0, 0.0, 10.0, 11.0, 11.0, 10.0, 10.0, 20.0, 21.0, 21.0, 20.0, 20.0,
            ],
            vec![
                0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0,
            ],
            None,
            None,
        )
        .unwrap(),
        CsrOffsetColumn::<RingLevel>::try_new(vec![0, 5, 10, 15], 15).unwrap(),
        CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 1, 2, 3], 3).unwrap(),
        Frame::None,
    )
    .unwrap();
    let source_polygon_cache = polygons.row_products.frame_cache(0);
    let polygons = polygons
        .with_missing_mask(ValidityMask::from_vec(3, vec![false, true, false]))
        .unwrap();
    let GeometryArrayStorage::Packed(PackedStorage::Polygons {
        coords,
        ring_offsets,
        polygon_offsets,
        ..
    }) = polygons.storage()
    else {
        panic!("masked polygons changed storage kind");
    };
    assert_eq!(coords.len(), 10);
    assert_eq!(ring_offsets.as_slice(), &[0, 5, 10]);
    assert_eq!(polygon_offsets.as_slice(), &[0, 1, 1, 2]);
    assert!(matches!(
        polygons.logical_row(1),
        Some(LogicalRow::Missing { logical: 1 })
    ));
    assert!(Arc::ptr_eq(
        &source_polygon_cache,
        &polygons.row_products.frame_cache(0),
    ));
}
