use super::*;

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn triangle_stream_publisher_carries_inserted_native_samples_by_ellipsoidal_arclength() {
    const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
    let state = crate::crs::runtime_state_snapshot().unwrap();
    let model = crate::crs::metric_model_at(Some(CRS), &state).unwrap();
    let crate::crs::ResolvedMetric::Geodesic(metric) =
        crate::crs::ResolvedMetric::from_model(&model).unwrap()
    else {
        panic!("the custom longlat CRS has a geodesic metric");
    };
    let frame = metric.frame;
    let first =
        Point::new_axes(760.0, 45.0, ZOrdinate(Some(35.25)), MOrdinate(Some(551.0))).unwrap();
    let second =
        Point::new_axes(1035.0, 72.5, ZOrdinate(Some(49.5)), MOrdinate(Some(726.0))).unwrap();
    let third =
        Point::new_axes(900.0, 90.0, ZOrdinate(Some(61.0)), MOrdinate(Some(901.0))).unwrap();
    let vertices = [first, second, third, first];

    let metric_accesses = std::sync::atomic::AtomicUsize::new(0);
    let metric_access = || {
        metric_accesses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(&metric)
    };
    let context = SourceArcContext::new(TopologyDomain::Geographic(frame), Some(&metric_access));
    let mut budget = ExpansionBudget::new("triangulate", "method");
    budget.add(vertices.len()).unwrap();
    let published =
        publish_triangle_vertex_stream_budgeted(&vertices, frame, &context, &mut budget).unwrap();

    assert_eq!(published.ring_ends, [5]);
    assert_eq!(
        metric_accesses.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        published.points.first().copied(),
        Some(Point::new_axes(40.0, 45.0, ZOrdinate(Some(35.25)), MOrdinate(Some(551.0))).unwrap())
    );
    assert_eq!(published.points.last(), published.points.first());
    let sample = published
        .points
        .iter()
        .find(|point| {
            point.x.to_bits() == 177.5_f64.to_bits() && point.y.to_bits() == 58.75_f64.to_bits()
        })
        .expect("the 275-degree triangle edge is subdivided at its midpoint");
    assert!((sample.z().unwrap() - 43.730_417_185_196_416).abs() <= 2.0e-8);
    assert!((sample.m().unwrap() - 655.145_474_204_166_5).abs() <= 2.0e-7);

    let xy_vertices = [
        Point::new_raw_xy(760.0, 45.0),
        Point::new_raw_xy(1035.0, 72.5),
        Point::new_raw_xy(900.0, 90.0),
        Point::new_raw_xy(760.0, 45.0),
    ];
    let xy_metric_accesses = std::sync::atomic::AtomicUsize::new(0);
    let xy_metric_access = || {
        xy_metric_accesses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(&metric)
    };
    let xy_context =
        SourceArcContext::new(TopologyDomain::Geographic(frame), Some(&xy_metric_access));
    let mut xy_budget = ExpansionBudget::new("triangulate", "method");
    xy_budget.add(xy_vertices.len()).unwrap();
    let xy_published =
        publish_triangle_vertex_stream_budgeted(&xy_vertices, frame, &xy_context, &mut xy_budget)
            .unwrap();
    assert_eq!(
        xy_metric_accesses.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        published
            .points
            .iter()
            .map(|point| (point.x, point.y))
            .collect::<Vec<_>>(),
        xy_published
            .points
            .iter()
            .map(|point| (point.x, point.y))
            .collect::<Vec<_>>()
    );
}
