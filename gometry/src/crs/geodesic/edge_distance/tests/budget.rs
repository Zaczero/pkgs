use super::*;

#[rstest::rstest]
#[case::exact_north_pole("exact north pole", Point::new_raw_xy(0.0, 90.0), Point::new_raw_xy(-45.0, 60.0), Point::new_raw_xy(45.0, 60.0))]
#[case::near_north_pole_plateau("near north pole plateau", Point::new_raw_xy(0.0, 89.999_999_999), Point::new_raw_xy(-45.0, 80.0), Point::new_raw_xy(45.0, 80.0))]
#[case::north_pole_plateau_transition("north pole plateau transition", Point::new_raw_xy(0.0, 89.9), Point::new_raw_xy(-45.0, 89.0), Point::new_raw_xy(45.0, 89.0))]
#[case::near_south_pole_plateau("near south pole plateau", Point::new_raw_xy(0.0, -89.999_999), Point::new_raw_xy(-45.0, -70.0), Point::new_raw_xy(45.0, -70.0))]
fn point_edge_pole_plateaus_close_within_the_budget(
    #[case] name: &'static str,
    #[case] point: Point,
    #[case] start: Point,
    #[case] end: Point,
) {
    let frame = GeographicFrame::crs84();
    let metric = metric(&Geodesic::wgs84(), frame);

    let edge = metric.chart_edge(start, end).unwrap();
    let minimum = point_edge_minimum(&metric, point, start, end);
    let oracle = dense_oracle(&metric, point, &edge);
    assert!(
        (minimum.distance - oracle).abs() <= 1.0e-4,
        "{name}: {} vs {oracle}",
        minimum.distance
    );
    assert!(minimum.evaluations <= 128, "{name}");
}
