use super::*;

#[test]
fn nullable_point_cursor_uses_only_admitted_present_ranks() {
    let compact_map = RowSelection::gather_checked_native(vec![5_usize, 2, 5], 6).unwrap();
    let compact_mask = ValidityMask::from_vec(6, vec![true, false, true, true, false, false])
        .expect("compact mask");
    let owner = packed_owner(
        GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(coords()),
            row_map: compact_map,
        }),
        Some(compact_mask),
        StorageLayout::PresentOnly,
    );
    let Some(PackedColumns::Points(points)) = owner.packed_columns() else {
        panic!("nullable packed points resolve through owner admission");
    };
    assert_eq!(points.rows().present_count(), 3);
    assert_eq!(points.rows().cursor().collect::<Vec<_>>(), [
        (0, None),
        (1, Some(0)),
        (2, None),
        (3, None),
        (4, Some(1)),
        (5, Some(2))
    ]);
    assert_eq!(points.coords().xs().to_vec(), [5.0, 2.0, 5.0]);
}

#[test]
fn nullable_point_cursor_decodes_non_byte_aligned_validity_windows() {
    let parent = ValidityMask::from_sparse(80, &[4, 7, 11, 19, 27, 32, 40, 63, 72]).unwrap();
    let window = parent.window(3, 65).expect("window contains missing rows");
    assert_eq!(window.arrow_bitmap().1, 3);
    let missing = [1_usize, 4, 8, 16, 24, 29, 37, 60];

    let owner = packed_owner(
        GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(
                CoordSeq::try_from_vecs(vec![0.0; 57], vec![1.0; 57], None, None)
                    .expect("57 point rows"),
            ),
            row_map: RowSelection::Identity,
        }),
        Some(window),
        StorageLayout::PresentOnly,
    );
    let Some(PackedColumns::Points(points)) = owner.packed_columns() else {
        panic!("nullable packed points resolve through owner admission");
    };
    let compact = points.rows();
    let mut present = 0;
    let expected_compact: Vec<_> = (0..65)
        .map(|logical| {
            if missing.contains(&logical) {
                (logical, None)
            } else {
                let physical = present;
                present += 1;
                (logical, Some(physical))
            }
        })
        .collect();
    assert_eq!(compact.cursor().collect::<Vec<_>>(), expected_compact);
    assert_eq!(present, compact.present_count());
}

#[test]
fn point_row_cursor_preserves_dense_selections_and_nullable_values() {
    let empty_owner = packed_owner(
        GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(CoordSeq::empty(CoordinateAxes::XY)),
            row_map: RowSelection::Identity,
        }),
        None,
        StorageLayout::LogicalRows,
    );
    let Some(PackedColumns::Points(empty)) = empty_owner.packed_columns() else {
        panic!("empty packed points resolve");
    };
    assert_eq!(
        empty.rows().cursor().collect::<Vec<_>>(),
        Vec::<(usize, Option<usize>)>::new()
    );

    let all_missing = ValidityMask::from_vec(9, vec![true; 9]).unwrap();
    let missing_owner = packed_owner(
        GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(CoordSeq::empty(CoordinateAxes::XY)),
            row_map: RowSelection::Identity,
        }),
        Some(all_missing),
        StorageLayout::PresentOnly,
    );
    let Some(PackedColumns::Points(compact_missing)) = missing_owner.packed_columns() else {
        panic!("all-missing packed points resolve");
    };
    assert_eq!(
        compact_missing.rows().cursor().collect::<Vec<_>>(),
        (0..9).map(|logical| (logical, None)).collect::<Vec<_>>()
    );

    let dense_map = RowSelection::gather_checked_native(vec![4_usize, 1, 4, 0], 5).unwrap();
    let dense_owner = packed_owner(
        GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(
                CoordSeq::try_from_vecs(
                    vec![10.0, 20.0, 30.0, 40.0, 50.0],
                    vec![1.0; 5],
                    None,
                    None,
                )
                .expect("five point rows"),
            ),
            row_map: dense_map,
        }),
        None,
        StorageLayout::LogicalRows,
    );
    let Some(PackedColumns::Points(dense_gather)) = dense_owner.packed_columns() else {
        panic!("dense gathered packed points resolve");
    };
    assert_eq!(dense_gather.rows().cursor().collect::<Vec<_>>(), [
        (0, Some(4)),
        (1, Some(1)),
        (2, Some(4)),
        (3, Some(0))
    ]);

    let compact_mask = ValidityMask::from_vec(4, vec![false, true, false, false]).unwrap();
    let compact_owner = packed_owner(
        GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(
                CoordSeq::try_from_vecs(vec![10.0, 20.0, 30.0], vec![1.0; 3], None, None)
                    .expect("present point rows"),
            ),
            row_map: RowSelection::gather_checked_native(vec![2_usize, 1, 2], 3).unwrap(),
        }),
        Some(compact_mask),
        StorageLayout::PresentOnly,
    );
    let Some(PackedColumns::Points(compact_gather)) = compact_owner.packed_columns() else {
        panic!("nullable gathered packed points resolve");
    };
    assert_eq!(compact_gather.rows().cursor().collect::<Vec<_>>(), [
        (0, Some(0)),
        (1, None),
        (2, Some(1)),
        (3, Some(2))
    ]);
    assert_eq!(compact_gather.coords().xs(), &[30.0, 20.0, 30.0]);

    let dense_window = packed_owner(
        GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(
                CoordSeq::try_from_vecs(vec![0.0; 8], vec![1.0; 8], None, None)
                    .expect("eight point rows"),
            ),
            row_map: storage::row_selection_from_logical_rows(RowSelectionRef::Identity, 8, 3..8),
        }),
        None,
        StorageLayout::LogicalRows,
    );
    let Some(PackedColumns::Points(dense_window)) = dense_window.packed_columns() else {
        panic!("dense windowed packed points resolve");
    };
    assert_eq!(dense_window.rows().cursor().collect::<Vec<_>>(), [
        (0, Some(3)),
        (1, Some(4)),
        (2, Some(5)),
        (3, Some(6)),
        (4, Some(7))
    ]);
}
