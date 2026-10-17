#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn point_view_is_one_coordinate() {
    let view = CoordinateView::from_shape(Arc::new(ShapeData::new(Shape::Point(pt(1.0, 2.0)))));
    assert_eq!(view.len(), 1);
    assert!(view.single_seq().is_none());
    assert_eq!(view.point_at(0).unwrap(), pt(1.0, 2.0));
    assert!(view.point_at(1).is_none());
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn linestring_view_is_zero_copy() {
    let view = CoordinateView::from_shape(line(&[(0.0, 0.0), (1.0, 2.0), (3.0, 4.0)]));
    assert_eq!(view.len(), 3);
    let seq = view.single_seq().expect("one contiguous run");
    assert_eq!(seq.xs(), &[0.0, 1.0, 3.0]);
    assert_eq!(seq.ys(), &[0.0, 2.0, 4.0]);
    assert!(matches!(
        view.column(CoordinateAxis::X),
        CoordinateColumnRef::Borrowed(_)
    ));
    assert!(matches!(
        view.column(CoordinateAxis::Z),
        CoordinateColumnRef::Missing
    ));
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn polygon_view_flattens_shell_then_holes() {
    let shell = Ring::closed(vec![pt(0.0, 0.0), pt(4.0, 0.0), pt(4.0, 4.0), pt(0.0, 4.0)]).unwrap();
    let hole = Ring::closed(vec![pt(1.0, 1.0), pt(2.0, 1.0), pt(2.0, 2.0), pt(1.0, 2.0)]).unwrap();
    let view = CoordinateView::from_shape(Arc::new(ShapeData::new(Shape::Polygon(Polygon {
        shell,
        holes: SharedSlice::from_vec(vec![hole]),
    }))));
    assert!(view.single_seq().is_none());
    let points = view.collect_points();
    assert_eq!(points.len(), view.len());
    // Shell first, then the hole.
    assert_eq!(points[3], pt(0.0, 4.0));
    assert_eq!(points[5], pt(1.0, 1.0));
    // X column over the multipart polygon materializes (no single run).
    assert!(matches!(
        view.column(CoordinateAxis::X),
        CoordinateColumnRef::Dense(_)
    ));
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn packed_polygon_view_walks_csr_rows_and_rings_in_order() {
    let coords = CoordSeq::try_from_points(&Vec::<Point>::from([
        pt(0.0, 0.0),
        pt(4.0, 0.0),
        pt(4.0, 4.0),
        pt(0.0, 4.0),
        pt(0.0, 0.0),
        pt(1.0, 1.0),
        pt(2.0, 1.0),
        pt(2.0, 2.0),
        pt(1.0, 2.0),
        pt(1.0, 1.0),
        pt(10.0, 10.0),
        pt(11.0, 10.0),
        pt(11.0, 11.0),
        pt(10.0, 11.0),
        pt(10.0, 10.0),
    ]))
    .unwrap();
    let ring_offsets = CsrOffsetColumn::<RingLevel>::try_new(vec![0, 5, 10, 15], 15).unwrap();
    let polygon_offsets = CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 2, 3], 3).unwrap();
    let array = PyGeometryArray::packed_polygons(
        coords.clone(),
        ring_offsets.clone(),
        polygon_offsets.clone(),
        Frame::default(),
    )
    .unwrap();
    let points = array.coordinate_view().collect_points();

    assert_eq!(points.len(), 15);
    assert_eq!(points[5], pt(1.0, 1.0));
    assert_eq!(points[10], pt(10.0, 10.0));
    assert_eq!(points[13], pt(10.0, 11.0));
    assert_eq!(
        array.coordinate_view().row_index_i64(),
        [[0; 10].as_slice(), &[1; 5]].concat()
    );

    let gathered = PyGeometryArray::packed_polygons_mapped(
        coords,
        ring_offsets,
        polygon_offsets,
        Frame::default(),
        RowSelection::gather_checked_native(vec![1, 0], 2).unwrap(),
    )
    .unwrap();
    let gathered_points = gathered.coordinate_view().collect_points();
    assert_eq!(gathered_points.len(), 15);
    assert_eq!(gathered_points[0], pt(10.0, 10.0));
    assert_eq!(gathered_points[5], pt(0.0, 0.0));
    assert_eq!(gathered_points[10], pt(1.0, 1.0));
    assert_eq!(
        gathered.coordinate_view().row_index_i64(),
        [[0; 5].as_slice(), &[1; 10]].concat()
    );

    let masked = gathered
        .with_missing_mask(ValidityMask::from_vec(2, vec![true, false]))
        .unwrap();
    let masked_view = masked.coordinate_view();
    let masked_points = masked_view.collect_points();
    assert_eq!(masked_view.len(), 10);
    assert_eq!(masked_points.len(), 10);
    assert_eq!(masked_view.row_index_i64(), vec![1; 10]);
    assert_eq!(masked_points[0], pt(0.0, 0.0));
    assert_eq!(masked_points[5], pt(1.0, 1.0));
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn identity_packed_lines_row_index_fills_from_csr_offsets() {
    let coords = CoordSeq::try_from_points(&[
        pt(0.0, 0.0),
        pt(1.0, 0.0),
        pt(2.0, 0.0),
        pt(3.0, 0.0),
        pt(4.0, 0.0),
    ])
    .unwrap();
    let offsets = CsrOffsetColumn::try_new(vec![0, 2, 5], 5).unwrap();
    let array = PyGeometryArray::packed_lines(coords, offsets, Frame::default()).unwrap();
    let view = array.coordinate_view();
    assert_eq!(view.row_index_i64(), vec![0, 0, 1, 1, 1]);
    assert_eq!(view.row_index(), vec![0, 0, 1, 1, 1]);
    assert!(view.single_seq().is_some());
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn identity_packed_polygons_row_index_spans_all_rings() {
    let coords = CoordSeq::try_from_points(&Vec::<Point>::from([
        pt(0.0, 0.0),
        pt(4.0, 0.0),
        pt(4.0, 4.0),
        pt(0.0, 4.0),
        pt(0.0, 0.0),
        pt(1.0, 1.0),
        pt(2.0, 1.0),
        pt(2.0, 2.0),
        pt(1.0, 2.0),
        pt(1.0, 1.0),
        pt(10.0, 10.0),
        pt(11.0, 10.0),
        pt(11.0, 11.0),
        pt(10.0, 11.0),
        pt(10.0, 10.0),
    ]))
    .unwrap();
    let ring_offsets = CsrOffsetColumn::<RingLevel>::try_new(vec![0, 5, 10, 15], 15).unwrap();
    let polygon_offsets = CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 2, 3], 3).unwrap();
    let array =
        PyGeometryArray::packed_polygons(coords, ring_offsets, polygon_offsets, Frame::default())
            .unwrap();
    let view = array.coordinate_view();
    // Row 0: shell (5) + hole (5); row 1: shell (5).
    assert_eq!(view.row_index_i64(), vec![
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1
    ]);
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn gathered_packed_lines_row_index_uses_logical_rows() {
    let coords = CoordSeq::try_from_points(&Vec::<Point>::from([
        pt(0.0, 0.0),
        pt(1.0, 0.0),
        pt(2.0, 0.0),
        pt(3.0, 0.0),
        pt(4.0, 0.0),
        pt(5.0, 0.0),
    ]))
    .unwrap();
    let offsets = CsrOffsetColumn::try_new(vec![0, 2, 4, 6], 6).unwrap();
    let gathered = PyGeometryArray::packed_lines_mapped(
        coords,
        offsets,
        Frame::default(),
        RowSelection::gather_checked_native(vec![2, 0], 3).unwrap(),
    )
    .unwrap();
    let view = gathered.coordinate_view();
    // Logical row 0 = physical 2 (2 verts); logical row 1 = physical 0 (2 verts).
    assert_eq!(view.row_index_i64(), vec![0, 0, 1, 1]);
    let points = view.collect_points();
    assert_eq!(points[0], pt(4.0, 0.0));
    assert_eq!(points[2], pt(0.0, 0.0));
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn collection_flattens_in_declaration_order() {
    let collection = Shape::geometry_collection(vec![
        Shape::Point(pt(9.0, 9.0)),
        Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_points(&[pt(0.0, 0.0), pt(1.0, 1.0)]).unwrap())
                .expect("test line is valid"),
        ),
    ])
    .expect("test collection is valid");
    let view = CoordinateView::from_shape(Arc::new(ShapeData::new(collection)));
    assert_eq!(view.len(), 3);
    let points = view.collect_points();
    assert_eq!(points[0], pt(9.0, 9.0));
    assert_eq!(points[1], pt(0.0, 0.0));
    assert_eq!(points[2], pt(1.0, 1.0));
    assert_eq!(view.row_index(), vec![0, 0, 0]);
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn optional_z_preserves_absent_vs_nan() {
    // Mixed Z presence must keep None for axes-absent vertices and
    // Some(NaN) for a measured NaN — the list path needs the distinction,
    // while NumPy maps both to NaN at the boundary.
    use crate::geometry::{MOrdinate, ZOrdinate};
    let collection = Shape::geometry_collection(vec![
        Shape::Point(pt(0.0, 0.0)),
        Shape::Point(Point::new_raw_axes(
            1.0,
            1.0,
            ZOrdinate(Some(f64::NAN)),
            MOrdinate(None),
        )),
        Shape::Point(
            Point::new_axes(2.0, 2.0, ZOrdinate(Some(9.0)), MOrdinate(None))
                .expect("finite test Z"),
        ),
    ])
    .expect("test collection is valid");
    let view = CoordinateView::from_shape(Arc::new(ShapeData::new(collection)));
    match view.column(CoordinateAxis::Z) {
        CoordinateColumnRef::Nullable(values) => {
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], None);
            assert!(values[1].is_some_and(f64::is_nan));
            assert_eq!(values[2], Some(9.0));
        },
        CoordinateColumnRef::Missing => panic!("expected Nullable Z column, got Missing"),
        CoordinateColumnRef::Borrowed(_) => panic!("expected Nullable Z column, got Borrowed"),
        CoordinateColumnRef::Dense(_) => panic!("expected Nullable Z column, got Dense"),
    }
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn packed_polygon_window_column_borrows_visible_span() {
    use crate::geometry::{MColumn, MOrdinate, ZOrdinate};
    let ring = |z: f64, m: f64| {
        [
            Point::new_axes(0.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
            Point::new_axes(1.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
            Point::new_axes(1.0, 1.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
            Point::new_axes(0.0, 1.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
            Point::new_axes(0.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
        ]
    };
    let xy = concat3(&ring(1.0, 10.0), &ring(2.0, 20.0), &ring(3.0, 30.0));
    let xs: [f64; 15] = std::array::from_fn(|i| xy[i].x);
    let ys: [f64; 15] = std::array::from_fn(|i| xy[i].y);
    let zs: [f64; 15] = std::array::from_fn(|i| xy[i].z().unwrap());
    let m_owner = SharedSlice::from_vec((0_u8..20).map(|value| f64::from(value) + 50.0).collect());
    let coords = CoordSeq::from_shared_columns(
        SharedSlice::from_vec(xs.to_vec()),
        SharedSlice::from_vec(ys.to_vec()),
        Some(SharedSlice::from_vec(zs.to_vec())),
        Some(MColumn::Local {
            owner: m_owner.clone(),
            offset: 3,
        }),
    )
    .unwrap();
    let array = packed_polygons(coords.clone(), vec![0, 5, 10, 15], vec![0, 1, 2, 3], 1..2);
    let view = array.coordinate_view();
    let (_, allocations) = crate::test_support::allocation_count(|| view.column(CoordinateAxis::X));
    assert_eq!(allocations, 0);
    let CoordinateColumnRef::Borrowed(column) = view.column(CoordinateAxis::X) else {
        panic!("expected borrowed X");
    };
    assert_eq!(column, &coords.xs()[5..10]);
    assert_eq!(column.as_ptr(), coords.xs()[5..10].as_ptr());
    let CoordinateColumnRef::Borrowed(column) = view.column(CoordinateAxis::Z) else {
        panic!("expected borrowed Z");
    };
    assert_eq!(column, &zs[5..10]);
    assert_eq!(column.as_ptr(), coords.zs().unwrap()[5..10].as_ptr());
    let CoordinateColumnRef::Borrowed(column) = view.column(CoordinateAxis::M) else {
        panic!("expected borrowed M");
    };
    assert_eq!(column, &m_owner.as_slice()[8..13]);
    assert_eq!(column.as_ptr(), m_owner.as_slice()[8..13].as_ptr());
    let xy_only = packed_polygons(
        CoordSeq::try_from_points(&concat3(
            &closed_box(0.0, 0.0, 1.0, 1.0),
            &closed_box(0.0, 0.0, 1.0, 1.0),
            &closed_box(0.0, 0.0, 1.0, 1.0),
        ))
        .unwrap(),
        vec![0, 5, 10, 15],
        vec![0, 1, 2, 3],
        1..2,
    );
    assert!(matches!(
        xy_only.coordinate_view().column(CoordinateAxis::Z),
        CoordinateColumnRef::Missing
    ));
    assert!(matches!(
        xy_only.coordinate_view().column(CoordinateAxis::M),
        CoordinateColumnRef::Missing
    ));
    assert!(!view.equal_visible(&xy_only.coordinate_view(), None, None));
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn packed_line_window_column_borrows_visible_span() {
    let coords = CoordSeq::try_from_points(&[
        pt(9.0, 9.0),
        pt(9.0, 8.0),
        pt(1.0, 0.0),
        pt(2.0, 0.0),
        pt(3.0, 0.0),
        pt(4.0, 0.0),
        pt(5.0, 0.0),
    ])
    .unwrap();
    let offsets = CsrOffsetColumn::try_new(vec![0, 2, 5, 7], 7).unwrap();
    let array = PyGeometryArray::packed_lines_mapped(
        coords.clone(),
        offsets,
        Frame::default(),
        storage::row_selection_from_logical_rows(storage::RowSelectionRef::Identity, 3, 1..3),
    )
    .unwrap();
    let view = array.coordinate_view();
    let (_, allocations) = crate::test_support::allocation_count(|| view.column(CoordinateAxis::X));
    assert_eq!(allocations, 0);
    let CoordinateColumnRef::Borrowed(xs) = view.column(CoordinateAxis::X) else {
        panic!("expected borrowed X");
    };
    assert_eq!(xs, &coords.xs()[2..7]);
    assert_eq!(xs.as_ptr(), coords.xs()[2..7].as_ptr());
}
