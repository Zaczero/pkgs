use super::*;

#[test]
fn selected_packed_materializations_admit_duplicate_output_width() {
    let maximum = i32::MAX as usize;
    let duplicate_rows = [0, 0];
    let selection = RowSelectionRef::Gather(&duplicate_rows);
    let line_offsets = [0, i32::MAX];

    assert_eq!(
        packed_lines_coord_len(&line_offsets, selection),
        maximum * 2
    );
    assert!(matches!(
        materialized_packed_lines_len(&line_offsets, selection)
            .unwrap_err()
            .kind(),
        ErrorKind::Geometry(GeometryErrorKind::OffsetCapacityExceeded)
    ));

    let ring_offsets = [0, i32::MAX];
    let polygon_offsets = [0, 1];
    assert_eq!(
        packed_polygons_coord_len(&ring_offsets, &polygon_offsets, selection),
        maximum * 2
    );
    assert_eq!(packed_polygons_ring_len(&polygon_offsets, selection), 2);
    assert!(matches!(
        materialized_packed_polygons_len(&ring_offsets, &polygon_offsets, selection)
            .unwrap_err()
            .kind(),
        ErrorKind::Geometry(GeometryErrorKind::OffsetCapacityExceeded)
    ));

    assert_eq!(admit_packed_coord_count(maximum).unwrap(), maximum);
    assert!(matches!(
        admit_packed_coord_count(maximum + 1).unwrap_err().kind(),
        ErrorKind::Geometry(GeometryErrorKind::OffsetCapacityExceeded)
    ));
}
