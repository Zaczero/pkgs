use super::super::resolve::resolve_point_columns;
use super::*;

#[test]
fn point_buffers_reserve_all_output_columns_once() {
    let allocations = [2_usize, 128].map(|rows| {
        let storage = GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(
                CoordSeq::try_from_vecs(vec![0.0; rows], vec![0.0; rows], None, None).unwrap(),
            ),
            row_map: RowSelection::Identity,
        });
        let owner = packed_owner(storage, None, StorageLayout::LogicalRows);
        let Some(PackedColumns::Points(points)) = owner.packed_columns() else {
            panic!("point fixture must resolve packed points");
        };
        let (output, count) = allocation_count(|| {
            packed_column_kernels::buffer_point_columns(
                &points,
                1.0,
                std::num::NonZeroU32::new(4).unwrap(),
                None,
            )
        });
        let Ok(Some(PackedColumnOutput::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
        })) = output
        else {
            panic!("point buffers must produce packed polygons");
        };
        assert_eq!(coords.len(), rows.wrapping_mul(17));
        assert_eq!(ring_offsets.len(), rows.wrapping_add(1));
        assert_eq!(polygon_offsets.len(), rows.wrapping_add(1));
        assert_eq!(coords.xs().first(), coords.xs().last());
        assert_eq!(coords.ys().first(), coords.ys().last());
        count
    });
    assert_eq!(allocations.first(), allocations.last());
}

#[test]
fn geodesic_xy_lanes_borrow_contiguous_rows_and_copy_gathers() {
    let coords = coords();
    let gather = RowSelection::gather_checked_native(vec![4, 1, 4], coords.len()).unwrap();
    for (selection, expected_xs, expected_ys, expected_allocations) in [
        (
            gather.as_deref(),
            &[4.0, 1.0, 4.0][..],
            &[4.0, 1.0, 4.0][..],
            2,
        ),
        (RowSelectionRef::Identity, coords.xs(), coords.ys(), 0),
        (
            RowSelectionRef::Window { start: 2, len: 3 },
            &[2.0, 3.0, 4.0][..],
            &[0.0, 3.0, 4.0][..],
            0,
        ),
        (
            RowSelectionRef::Window { start: 6, len: 0 },
            &[][..],
            &[][..],
            0,
        ),
    ] {
        let columns = resolve_point_columns(&coords, selection, None);
        let ((xs, ys), allocations) = allocation_count(|| columns.geodesic_xy_lanes());
        assert_eq!(allocations, expected_allocations);
        assert_eq!(&*xs, expected_xs);
        assert_eq!(&*ys, expected_ys);
    }
}

#[test]
fn geographic_point_bounds_allocate_only_the_output() {
    let coords = coords();
    let gather = RowSelection::gather_checked_native(vec![4, 1, 4], coords.len()).unwrap();
    let missing = ValidityMask::from_vec(3, vec![false, true, false]).unwrap();
    let storage = GeometryArrayStorage::Packed(PackedStorage::Points {
        coords: Arc::new(coords),
        row_map: gather,
    });
    let dense = packed_owner(storage.clone(), None, StorageLayout::LogicalRows);
    let nullable = packed_owner(storage, Some(missing), StorageLayout::LogicalRows);
    for (owner, expected) in [
        (&dense, [
            4.0, 4.0, 4.0, 4.0, 1.0, 1.0, 1.0, 1.0, 4.0, 4.0, 4.0, 4.0,
        ]),
        (&nullable, [
            4.0,
            4.0,
            4.0,
            4.0,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            4.0,
            4.0,
            4.0,
            4.0,
        ]),
    ] {
        let columns = owner.packed_columns().expect("admitted packed points");
        let (bounds, allocations) = allocation_count(|| {
            packed_column_kernels::geographic_bounds_values_from_columns_masked(
                &columns,
                crate::geographic::GeographicFrame::crs84(),
                owner.missing(),
            )
        });
        let bounds = bounds.unwrap();
        assert_eq!(allocations, 1);
        assert_eq!(bounds.len(), expected.len());
        for (actual, expected) in bounds.iter().zip(expected) {
            if expected.is_nan() {
                assert!(actual.is_nan());
            } else {
                assert_eq!(*actual, expected);
            }
        }
    }

    let block_coords = CoordSeq::try_from_vecs(
        vec![-0.0, 1.0, 2.0, 3.0, 450.0, -270.0, 180.0, 540.0, 40.0],
        vec![-0.0, 11.0, 12.0, 13.0, 3.0, 4.0, 5.0, 6.0, 90.0],
        None,
        None,
    )
    .expect("nine CRS84 point rows have parallel XY columns");
    let dense_columns = PackedColumns::Points(resolve_point_columns(
        &block_coords,
        RowSelectionRef::Identity,
        None,
    ));
    let dense_bounds = packed_column_kernels::geographic_bounds_values_from_columns(
        &dense_columns,
        GeographicFrame::crs84(),
    )
    .expect("dense geographic point bounds");
    let expected_dense = [
        [-0.0, -0.0, -0.0, -0.0],
        [1.0, 11.0, 1.0, 11.0],
        [2.0, 12.0, 2.0, 12.0],
        [3.0, 13.0, 3.0, 13.0],
        [90.0, 3.0, 90.0, 3.0],
        [90.0, 4.0, 90.0, 4.0],
        [-180.0, 5.0, -180.0, 5.0],
        [-180.0, 6.0, -180.0, 6.0],
        [40.0, 90.0, 40.0, 90.0],
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    assert_eq!(dense_bounds, expected_dense);
    let (dense_rows, _) = dense_bounds.as_chunks::<4>();
    assert!(
        dense_rows
            .first()
            .expect("the first point has one bounds row")
            .iter()
            .all(|ordinate| ordinate.to_bits() == (-0.0_f64).to_bits())
    );

    let window_columns = PackedColumns::Points(resolve_point_columns(
        &block_coords,
        RowSelectionRef::Window { start: 1, len: 8 },
        None,
    ));
    let window_bounds = packed_column_kernels::geographic_bounds_values_from_columns(
        &window_columns,
        GeographicFrame::crs84(),
    )
    .expect("offset contiguous geographic point bounds");
    let expected_window = [
        [1.0, 11.0, 1.0, 11.0],
        [2.0, 12.0, 2.0, 12.0],
        [3.0, 13.0, 3.0, 13.0],
        [90.0, 3.0, 90.0, 3.0],
        [90.0, 4.0, 90.0, 4.0],
        [-180.0, 5.0, -180.0, 5.0],
        [-180.0, 6.0, -180.0, 6.0],
        [40.0, 90.0, 40.0, 90.0],
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    assert_eq!(window_bounds, expected_window);

    let selected = RowSelection::gather_checked_native(vec![3, 1, 3, 4], block_coords.len())
        .expect("selected point rows are in range");
    let selected_missing = ValidityMask::from_vec(4, vec![false, true, false, false]).unwrap();
    let selected_owner = packed_owner(
        GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(block_coords.clone()),
            row_map: selected,
        }),
        Some(selected_missing),
        StorageLayout::LogicalRows,
    );
    let selected_columns = selected_owner
        .packed_columns()
        .expect("nullable selected points admit a packed view");
    let selected_bounds = packed_column_kernels::geographic_bounds_values_from_columns_masked(
        &selected_columns,
        GeographicFrame::crs84(),
        selected_owner.missing(),
    )
    .expect("gathered nullable geographic point bounds");
    let expected_selected = [
        [3.0, 13.0, 3.0, 13.0],
        [f64::NAN; 4],
        [3.0, 13.0, 3.0, 13.0],
        [90.0, 3.0, 90.0, 3.0],
    ]
    .into_iter()
    .flatten();
    assert_eq!(selected_bounds.len(), 16);
    for (actual, expected) in selected_bounds.iter().zip(expected_selected) {
        if expected.is_nan() {
            assert!(actual.is_nan());
        } else {
            assert_eq!(actual.to_bits(), expected.to_bits());
        }
    }
}

#[test]
fn selected_views_resolve_physical_rows_directly() {
    let selection = || RowSelection::gather_checked_native(vec![1_usize, 0, 1], 2).unwrap();
    let point_storage = GeometryArrayStorage::Packed(PackedStorage::Points {
        coords: Arc::new(coords()),
        row_map: RowSelection::gather_checked_native(vec![2_usize, 0, 2], 6).unwrap(),
    });
    let line_storage = GeometryArrayStorage::Packed(PackedStorage::Lines {
        coords: Arc::new(coords()),
        offsets: CsrOffsetColumn::try_new(vec![0, 3, 6], 6).expect("valid offsets"),
        row_map: selection(),
    });
    let polygon_storage = GeometryArrayStorage::Packed(PackedStorage::Polygons {
        coords: Arc::new(coords()),
        ring_offsets: CsrOffsetColumn::<RingLevel>::try_new(vec![0, 3, 6], 6)
            .expect("valid ring offsets"),
        polygon_offsets: CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 1, 2], 2)
            .expect("valid polygon offsets"),
        row_map: selection(),
    });

    let point_owner = packed_owner(point_storage, None, StorageLayout::LogicalRows);
    let Some(points) = point_owner.packed_columns() else {
        panic!("packed point view resolution failed");
    };
    assert_eq!(points.row_count(), 3);
    assert_eq!(
        points
            .collect_rows::<_, Vec<_>>(|row| { row.map(|row| (row.index(), row.vertex_window())) }),
        [
            Some((0, Some(2..3))),
            Some((1, Some(0..1))),
            Some((2, Some(2..3))),
        ]
    );
    drop(packed_column_kernels::bounds_values_from_columns(&points));

    for storage in [line_storage, polygon_storage] {
        let owner = packed_owner(storage, None, StorageLayout::LogicalRows);
        let Some(columns) = owner.packed_columns() else {
            panic!("packed view resolution failed");
        };
        let rows = columns
            .collect_rows::<_, Vec<_>>(|row| row.map(|row| (row.index(), row.vertex_window())));
        assert_eq!(rows, [
            Some((0, Some(3..6))),
            Some((1, Some(0..3))),
            Some((2, Some(3..6))),
        ]);
    }
}

#[test]
fn selected_validation_and_total_bounds_do_not_stage_rows() {
    let storage = gathered_lines(4);
    let missing = ValidityMask::from_vec(4, vec![false, true, false, true]).unwrap();
    for mask in [None, Some(&missing)] {
        let owner = packed_owner(storage.clone(), mask.cloned(), StorageLayout::LogicalRows);
        let columns = owner.packed_columns().expect("packed columns");
        let (admission, admission_allocations) = allocation_count(|| {
            packed_column_kernels::ensure_geographic_columns_present(
                &columns,
                crate::geographic::GeographicFrame::crs84(),
            )
        });
        admission.unwrap();
        let (bounds, bounds_allocations) =
            allocation_count(|| packed_column_kernels::raw_total_bounds_from_columns(&columns));
        assert_eq!((admission_allocations, bounds_allocations), (0, 0));
        assert_eq!(
            bounds.unwrap().into_array(),
            if mask.is_some() {
                [0.0, 0.0, 2.0, 1.0]
            } else {
                [0.0, 0.0, 5.0, 4.0]
            }
        );
    }
}

#[test]
fn selected_reduction_allocates_only_its_result() {
    let storage = gathered_lines(32);
    let owner = packed_owner(storage, None, StorageLayout::LogicalRows);
    let columns = owner.packed_columns().expect("packed columns");
    let (values, allocations) =
        allocation_count(|| packed_column_kernels::bounds_values_from_columns(&columns));
    assert_eq!(values.len(), 32 * 4);
    assert_eq!(allocations, 1);
}

#[test]
fn selected_reverse_reserves_coordinate_and_offset_columns() {
    let counts = [2, 128].map(|rows| {
        let storage = gathered_lines(rows);
        let owner = packed_owner(storage, None, StorageLayout::LogicalRows);
        let Some(PackedColumns::Lines(columns)) = owner.packed_columns() else {
            panic!("line fixture requires line columns");
        };
        let (output, allocations) = allocation_count(|| columns.reverse_selected());
        let (coords, offsets) = output.unwrap();
        assert_eq!(coords.len(), rows.wrapping_mul(3));
        assert_eq!(offsets.len(), rows.wrapping_add(1));
        assert_eq!(coords.xs().first(), Some(&2.0));
        assert_eq!(coords.xs().last(), Some(&3.0));
        allocations
    });
    assert_eq!(counts.first(), counts.last());
}

#[test]
fn selected_affine_reserves_only_visible_coordinates() {
    use crate::HeapSize as _;

    let source = coords();
    let prefix = source.view(CoordWindow::from_range(0..3, source.len()));
    let source_offsets = CsrOffsetColumn::try_new(vec![0, 3, 6], source.len()).unwrap();
    let prefix_offsets = CsrOffsetColumn::try_new(vec![0, 3], prefix.len()).unwrap();
    let linear = crate::geometry::AffineLinear::new([2.0, 0.0, 0.0, 2.0]).unwrap();
    let footprints = [
        LineColumns::from_parts(&source, &source_offsets, RowSelectionRef::Window {
            start: 0,
            len: 1,
        }),
        LineColumns::from_parts(&prefix, &prefix_offsets, RowSelectionRef::Identity),
    ]
    .map(|columns| {
        let Ok(PackedColumnOutput::Lines { coords, offsets }) =
            packed_column_kernels::packed_per_row_self_origin_affine_columns(
                PackedColumns::Lines(columns),
                crate::geometry::OriginSpec::Center,
                &linear,
            )
        else {
            panic!("line affine must produce line columns");
        };
        assert_eq!(coords.xs(), &[-1.0, 1.0, 3.0]);
        assert_eq!(coords.ys(), &[-0.5, 1.5, -0.5]);
        assert_eq!(offsets.as_slice(), &[0, 3]);
        coords.heap_bytes()
    });
    assert_eq!(footprints.first(), footprints.last());
}

#[test]
fn selected_affine_does_not_allocate_per_row_coordinates() {
    let counts = [2, 128].map(|rows| {
        let storage = gathered_lines(rows);
        let owner = packed_owner(storage, None, StorageLayout::LogicalRows);
        let columns = owner.packed_columns().expect("packed columns");
        let linear = crate::geometry::AffineLinear::new([2.0, 0.0, 0.0, 2.0]).unwrap();
        let (output, allocations) = allocation_count(|| {
            packed_column_kernels::packed_per_row_self_origin_affine_columns(
                columns,
                crate::geometry::OriginSpec::Center,
                &linear,
            )
        });
        let Ok(PackedColumnOutput::Lines { coords, offsets }) = output else {
            panic!("line affine must produce line columns");
        };
        assert_eq!(coords.len(), rows.wrapping_mul(3));
        assert_eq!(offsets.len(), rows.wrapping_add(1));
        assert_eq!(coords.xs().first(), Some(&-1.0));
        assert_eq!(coords.xs().last(), Some(&6.0));
        allocations
    });
    assert_eq!(counts.first(), counts.last());
}

#[test]
fn selected_pointwise_map_has_constant_output_owner_allocations() {
    fn count(rows: usize) -> usize {
        let storage = gathered_lines(rows);
        let owner = packed_owner(storage, None, StorageLayout::LogicalRows);
        let columns = owner.packed_columns().expect("packed columns");
        let (output, allocations) = allocation_count(|| {
            let Ok(output) = map_selected_points(columns, CoordinateAxes::XY, |point| {
                point.translate_xy(1.0, -1.0)
            }) else {
                panic!("pointwise map failed");
            };
            output
        });
        let PackedColumnOutput::Lines { coords, offsets } = output else {
            panic!("line input must produce line output");
        };
        assert_eq!(coords.len(), rows * 3);
        assert_eq!(offsets.len(), rows + 1);
        allocations
    }

    let small = count(2);
    let large = count(128);
    assert_eq!(small, large);
}

#[test]
fn reverse_selected_lines_copies_only_gathered_windows() {
    let coords = CoordSeq::try_from_vecs(
        vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
        vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
        None,
        None,
    )
    .unwrap();
    let offsets = CsrOffsetColumn::try_new(vec![0, 3, 6], 6).unwrap();
    let map = gather_map(vec![1, 1], 2);
    let (reversed, out_offsets) = LineColumns::from_parts(&coords, &offsets, map.as_deref())
        .reverse_selected()
        .unwrap();
    assert_eq!(reversed.xs(), &[5.0, 4.0, 3.0, 5.0, 4.0, 3.0]);
    assert_eq!(out_offsets.as_slice(), &[0, 3, 6]);
}
