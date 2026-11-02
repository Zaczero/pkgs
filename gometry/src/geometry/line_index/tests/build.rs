use super::*;

#[test]
fn planar_one_shot_locate_admits_source_lengths_before_empty_query() {
    let coordinates = CoordSeq::try_from_points(&[
        Point::new_raw_xy(-1e308, 0.0),
        Point::new_raw_xy(1e308, 0.0),
    ])
    .unwrap();
    let index = LineIndex::build_coordseq(&coordinates, &PlanarMetric).unwrap();
    assert!(index.wide.is_some());
    let result = locate_planar_route(
        std::iter::once(&coordinates),
        Point::new_raw_xy(f64::NAN, f64::NAN),
        false,
        1.0,
    )
    .unwrap();
    assert!(result.is_nan());
}
