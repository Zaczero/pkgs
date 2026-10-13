use super::*;

pub(super) fn metric(geodesic: &Geodesic, frame: GeographicFrame) -> EllipsoidChartMetric {
    let authalic_series = AuthalicSeries::new(geodesic);
    EllipsoidChartMetric {
        crs: Arc::new("test geographic CRS".to_owned()),
        geodesic: Box::new(*geodesic),
        authalic_series,
        frame,
        state: crate::crs::runtime_state_snapshot().expect("runtime state"),
    }
}

pub(super) fn point_edge_minimum(
    metric: &EllipsoidChartMetric,
    point: Point,
    start: Point,
    end: Point,
) -> PointEdgeMinimum {
    let edge = metric.chart_edge(start, end).expect("admitted chart edge");
    let mut scratch = PointEdgeScratch::default();
    match metric
        .point_edge_query(
            point,
            &edge,
            metric.point_envelope_bound(point)(edge.bounds()),
            PointEdgeQuery::Minimum {
                cutoff: f64::INFINITY,
            },
            &mut scratch,
        )
        .expect("point-edge query")
    {
        PointEdgeQueryResult::Minimum(minimum) => minimum,
        result @ (PointEdgeQueryResult::Excluded
        | PointEdgeQueryResult::Within { .. }
        | PointEdgeQueryResult::Outside { .. }
        | PointEdgeQueryResult::Unresolved { .. }) => {
            panic!("unbounded point-edge query did not return a minimum: {result:?}")
        },
    }
}

pub(super) fn oracle_distance(
    metric: &EllipsoidChartMetric,
    point: Point,
    edge: &ChartEdge,
    parameter: f64,
) -> f64 {
    let point = metric
        .frame
        .to_geodesic_point(point)
        .expect("oracle point is admitted");
    let target = edge.geodetic_position(parameter).unwrap();
    let distance: f64 = metric
        .geodesic
        .inverse(point.y, point.x, target.y, target.x);
    distance
}

pub(super) fn dense_oracle(metric: &EllipsoidChartMetric, point: Point, edge: &ChartEdge) -> f64 {
    const LAST_SAMPLE: u32 = 4_000;
    let mut samples = Vec::with_capacity(LAST_SAMPLE as usize + 1);
    let mut best_distance = f64::INFINITY;
    for index in 0..=LAST_SAMPLE {
        let parameter = f64::from(index) / f64::from(LAST_SAMPLE);
        let distance = oracle_distance(metric, point, edge, parameter);
        best_distance = best_distance.min(distance);
        samples.push(distance);
    }
    if let Some(first) = samples.first() {
        best_distance = best_distance.min(*first);
    }
    if let Some(last) = samples.last() {
        best_distance = best_distance.min(*last);
    }
    let golden = 0.5 * (5.0_f64.sqrt() - 1.0);
    let parameter_step = 1.0 / f64::from(LAST_SAMPLE);
    for (index, window) in (1..LAST_SAMPLE).zip(samples.windows(3)) {
        let [before, current, after] = window else {
            unreachable!("a three-sample window has three values")
        };
        if current <= before && current <= after && (current < before || current < after) {
            let center = f64::from(index) * parameter_step;
            let mut left = center - parameter_step;
            let mut right = center + parameter_step;
            let mut x1 = right - golden * (right - left);
            let mut x2 = left + golden * (right - left);
            let mut f1 = oracle_distance(metric, point, edge, x1);
            let mut f2 = oracle_distance(metric, point, edge, x2);
            for _ in 0..80 {
                if f1 <= f2 {
                    right = x2;
                    x2 = x1;
                    f2 = f1;
                    x1 = right - golden * (right - left);
                    f1 = oracle_distance(metric, point, edge, x1);
                } else {
                    left = x1;
                    x1 = x2;
                    f1 = f2;
                    x2 = left + golden * (right - left);
                    f2 = oracle_distance(metric, point, edge, x2);
                }
            }
            best_distance = best_distance.min(f1).min(f2);
        }
    }
    best_distance
}
