use super::*;

#[test]
fn planar_one_shot_locate_admits_source_lengths_before_empty_query() {
    let coordinates = CoordSeq::try_from_points(&[
        Point::new_raw_xy(-1e308, 0.0),
        Point::new_raw_xy(1e308, 0.0),
    ])
    .unwrap();
    let old_error = LineIndex::build_coordseq(&coordinates, &PlanarMetric)
        .err()
        .expect("source segment length overflows")
        .to_string();
    let new_error = locate_planar_route(
        std::iter::once(&coordinates),
        Point::new_raw_xy(f64::NAN, f64::NAN),
        false,
    )
    .unwrap_err()
    .to_string();
    assert_eq!(new_error, old_error);
}
