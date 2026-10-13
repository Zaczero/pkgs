use super::*;

#[test]
fn checked_gather_rejects_out_of_range_physical_rows() {
    assert!(RowSelection::gather_checked_native(vec![0_usize, 3], 3).is_none());
}

#[test]
fn checked_gather_preserves_valid_physical_rows() {
    let row_map = RowSelection::gather_checked_native(vec![2_usize, 0], 3).unwrap();
    assert_eq!(row_map.as_deref().len(3), 2);
    assert_eq!(physical_row(row_map.as_deref(), 0), 2);
    assert_eq!(physical_row(row_map.as_deref(), 1), 0);
}

#[cfg(test)]
mod row_selection_admission_tests {
    use super::*;

    #[test]
    fn checked_gather_rejects_out_of_range_physical_rows() {
        assert!(RowSelection::gather_checked_native(vec![4_usize], 4).is_none());
    }

    #[test]
    fn logical_row_composition_builds_a_private_window() {
        let selection = row_selection_from_logical_rows(RowSelectionRef::Identity, 4, 1..3);
        assert!(matches!(selection, RowSelection::Window(_)));
    }

    #[test]
    fn logical_row_composition_preserves_empty_range_at_physical_end() {
        let selection = row_selection_from_logical_rows(RowSelectionRef::Identity, 4, 4..4);
        assert_eq!(selection.as_deref().len(4), 0);
    }

    #[test]
    #[should_panic(expected = "logical row is outside selected row map")]
    fn logical_row_composition_rejects_identity_logical_end() {
        drop(row_selection_from_logical_rows(
            RowSelectionRef::Identity,
            4,
            [4],
        ));
    }

    #[test]
    #[should_panic(expected = "logical row is outside selected row map")]
    fn logical_row_composition_rejects_window_logical_end_before_resolution() {
        let row_map = RowSelectionRef::Window {
            start: usize::MAX - 1,
            len: 1,
        };
        drop(row_selection_from_logical_rows(row_map, usize::MAX, [1]));
    }

    #[test]
    #[should_panic(expected = "physical row is outside backing storage")]
    fn logical_row_composition_rejects_gathered_physical_end() {
        let row_map = RowSelectionRef::Gather(&[4]);
        drop(row_selection_from_logical_rows(row_map, 4, [0]));
    }
}
