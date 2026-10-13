use super::*;

#[test]
fn full_turn_parallel_lrs_round_trips_across_the_circle() {
    use geographiclib_rs::Geodesic;

    use crate::crs::EllipsoidMetric;
    use crate::geographic::GeographicFrame;

    let shape = crate::io::parse_wkt("LINESTRING (10 60, 370 60)").expect("one explicit full turn");
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, GeographicFrame::crs84());
    let index = ChartLineIndex::build_chart(&shape, &metric).expect("chart-edge line index");
    let cases = [28.0, 100.0, -170.0, -80.0, -8.0];
    let start_ulp = 10.0_f64.next_up() - 10.0;
    let latitude_ulp = 60.0_f64.next_up() - 60.0;
    let mut first_station = None;
    let mut previous_station = f64::NEG_INFINITY;

    for longitude in cases {
        let query = Point::new_raw_xy(longitude, 60.0);
        let station = index
            .locate_chart_point(query, true, &metric)
            .expect("full-turn chart locate");
        assert!(
            station > previous_station,
            "full-turn stations must increase: {previous_station} then {station}"
        );
        if first_station.is_none() {
            first_station = Some(station);
        }
        let interpolated = index
            .interpolate_route_position(station, true, &metric)
            .expect("full-turn chart interpolate");
        let longitude_ulp = longitude.abs().next_up() - longitude.abs();
        let longitude_budget = 4.0 * start_ulp.max(longitude_ulp) + 360.0 * 4.0 * f64::EPSILON;
        let latitude_budget = 4.0 * latitude_ulp;
        assert!(
            (interpolated.x - longitude).abs() <= longitude_budget,
            "round-trip longitude {} differs from {longitude} by more than {longitude_budget}",
            interpolated.x
        );
        assert!(
            (interpolated.y - 60.0).abs() <= latitude_budget,
            "round-trip latitude {} differs from 60 by more than {latitude_budget}",
            interpolated.y
        );
        previous_station = station;
    }
    assert!(first_station.is_some_and(|station| station < 0.1));
    assert!(previous_station > 0.9);
}
