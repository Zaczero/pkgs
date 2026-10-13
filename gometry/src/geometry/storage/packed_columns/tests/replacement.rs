use super::*;

#[test]
fn nullable_polygon_replacement_reports_the_logical_ring_row() {
    let array = crate::test_support::nullable_polygon_typed_empty_fixture();
    let GeometryArrayStorage::Packed(PackedStorage::Polygons { coords, .. }) = array.storage()
    else {
        panic!("nullable polygon fixture remains packed");
    };
    let mut xs = coords.xs().to_vec();
    *xs.last_mut().expect("present polygon has a ring") = -1.0;
    let replacement = CoordinateReplacement {
        xs: Some(SharedSlice::from_vec(xs)),
        ys: Some(SharedSlice::from_vec(coords.ys().to_vec())),
        zs: ReplacementAxis::Carry,
        ms: ReplacementAxis::Carry,
        len: coords.len(),
        axes: coords.axes(),
        positional: false,
    };

    let columns = array
        .row_owner()
        .packed_columns()
        .expect("packed nullable polygon resolves to columns");
    let mut replacement_index = 0;
    let Err(error) = replace_packed_columns_dispatch(columns, &replacement, &mut replacement_index)
    else {
        panic!("open replacement ring is rejected");
    };
    assert!(matches!(error, PackedColumnError::Row { row: 3, .. }));
}
