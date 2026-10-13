    #[test]
    fn cell_array_heap_bytes_counts_ids_and_missing_flags_once() {
        use crate::HeapSize as _;

        let cells = PyCellArray::from_ids_with_missing(GridKind::H3Cell, vec![1, 2, 3], vec![
            false, true, false,
        ]);
        let arc = crate::heap_size::ARC_CONTROL_BYTES;
        assert_eq!(
            cells.heap_bytes(),
            arc + size_of::<Vec<u64>>() + 3 * size_of::<u64>() + arc + size_of::<Vec<u8>>() + 1
        );

        let selected = cells.slice(1, 2, 1, 1);
        assert_eq!(
            selected.heap_bytes(),
            arc + size_of::<Vec<u64>>() + size_of::<u64>() + arc + size_of::<Vec<u8>>() + 1
        );
    }

    #[test]
    fn fallible_logical_mapping_stops_at_the_first_present_row_error() {
        let cells = PyCellArray::from_ids_with_missing(
            GridKind::H3Cell,
            vec![
                h3_id(37.7749, -122.4194, 7),
                0,
                h3_id(37.7749, -122.4194, 8),
            ],
            vec![false, true, false],
        )
        .select_logical_rows([2, 1, 0, 2]);
        let mut visits = Vec::new();
        let result = cells.try_map_logical_cells::<h3o::CellIndex, _, _>(Vec::new(), |cell| {
            visits.push(u8::from(cell.resolution()));
            if visits.len() == 2 {
                Err(visits.len())
            } else {
                Ok(cell)
            }
        });
        assert_eq!(result, Err(2));
        assert_eq!(visits, [8, 7]);
    }
