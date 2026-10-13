#[test]
fn chart_metric_trait_constructs_and_measures_a_chart_edge() {
    let metric = GeodesicMetric {
        crs: Arc::new("EPSG:4326".to_owned()),
        geodesic: Box::new(wgs84()),
        authalic_series: AuthalicSeries::new(&wgs84()),
        frame: degrees(),
        state: crate::crs::runtime_state_snapshot().unwrap(),
    };
    let edge = metric
        .chart_edge(Point::new_raw_xy(0.0, 60.0), Point::new_raw_xy(90.0, 60.0))
        .unwrap();
    let measured = metric.chart_edge_length(&edge).unwrap();
    let expected = 5_022_000.141_519_251;
    assert!((measured - expected).abs() <= 2.0e-9);
}
