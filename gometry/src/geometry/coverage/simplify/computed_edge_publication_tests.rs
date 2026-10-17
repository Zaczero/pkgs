use super::coverage_simplify_with_context;
use crate::geographic::GeographicFrame;
use crate::geometry::{
    CoordSeq, MOrdinate, Point, Polygon, Ring, Shape, SimplifyMethod, SourceArcContext,
    TopologyDomain, ZOrdinate,
};

#[test]
fn coverage_simplification_subdivides_a_wide_generated_chord() {
    let source = Shape::Polygon(Polygon::new(
        Ring::try_from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(90.0, 0.0),
                Point::new_raw_xy(180.0, 0.0),
                Point::new_raw_xy(270.0, 0.0),
                Point::new_raw_xy(270.0, 10.0),
                Point::new_raw_xy(180.0, 10.0),
                Point::new_raw_xy(90.0, 10.0),
                Point::new_raw_xy(0.0, 10.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    let output = coverage_simplify_with_context(
        std::slice::from_ref(&source),
        0.1,
        SimplifyMethod::Vw,
        true,
        &SourceArcContext::new(TopologyDomain::Geographic(GeographicFrame::crs84()), None),
    )
    .unwrap();
    let Shape::Polygon(polygon) = output.first().unwrap() else {
        panic!("coverage simplification preserves the polygon row");
    };
    let points = polygon.shell.coords().iter().collect::<Vec<_>>();
    assert!(points.contains(&Point::new_raw_xy(135.0, 0.0)));
    assert!(points.contains(&Point::new_raw_xy(135.0, 10.0)));
    assert!(points.iter().all(
        |point| (-180.0..180.0).contains(&point.x) || point.x.to_bits() == 270.0_f64.to_bits()
    ));
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn coverage_edge_role_publisher_carries_native_samples_by_ellipsoidal_arclength() {
    const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
    let state = crate::crs::runtime_state_snapshot().unwrap();
    let model = crate::crs::metric_model_at(Some(CRS), &state).unwrap();
    let crate::crs::ResolvedMetric::Geodesic(metric) =
        crate::crs::ResolvedMetric::from_model(&model).unwrap()
    else {
        panic!("the custom longlat CRS has a geodesic metric");
    };
    let frame = metric.frame;
    let points = [
        Point::new_axes(0.0, 45.0, ZOrdinate(Some(35.25)), MOrdinate(Some(551.0))).unwrap(),
        Point::new_axes(90.0, 54.0, ZOrdinate(Some(40.0)), MOrdinate(Some(600.0))).unwrap(),
        Point::new_axes(180.0, 63.0, ZOrdinate(Some(45.0)), MOrdinate(Some(675.0))).unwrap(),
        Point::new_axes(275.0, 72.5, ZOrdinate(Some(49.5)), MOrdinate(Some(726.0))).unwrap(),
        Point::new_axes(275.0, 82.5, ZOrdinate(Some(60.0)), MOrdinate(Some(800.0))).unwrap(),
        Point::new_axes(180.0, 73.0, ZOrdinate(Some(55.0)), MOrdinate(Some(775.0))).unwrap(),
        Point::new_axes(90.0, 64.0, ZOrdinate(Some(50.0)), MOrdinate(Some(650.0))).unwrap(),
        Point::new_axes(0.0, 55.0, ZOrdinate(Some(45.0)), MOrdinate(Some(600.0))).unwrap(),
        Point::new_axes(0.0, 45.0, ZOrdinate(Some(35.25)), MOrdinate(Some(551.0))).unwrap(),
    ];
    let source = Shape::Polygon(Polygon::new(
        Ring::try_from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        Vec::new(),
    ));
    let metric_accesses = std::sync::atomic::AtomicUsize::new(0);
    let metric_access = || {
        metric_accesses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(&metric)
    };
    let source_context =
        SourceArcContext::new(TopologyDomain::Geographic(frame), Some(&metric_access));
    let output = coverage_simplify_with_context(
        std::slice::from_ref(&source),
        0.1,
        SimplifyMethod::Vw,
        true,
        &source_context,
    )
    .unwrap();
    let Shape::Polygon(polygon) = output.first().unwrap() else {
        panic!("coverage simplification preserves its polygon row");
    };
    let published = polygon.shell.coords().iter().collect::<Vec<_>>();
    assert_eq!(
        metric_accesses.load(std::sync::atomic::Ordering::Relaxed),
        2
    );
    assert_eq!(published.first(), Some(&points[0]));
    assert_eq!(published.last(), published.first());
    let sample = published
        .iter()
        .find(|point| {
            point.x.to_bits() == 137.5_f64.to_bits() && point.y.to_bits() == 58.75_f64.to_bits()
        })
        .expect("the 275-degree computed coverage edge is subdivided");
    assert!((sample.z().unwrap() - 43.730_417_185_196_416).abs() <= 2.0e-8);
    assert!((sample.m().unwrap() - 655.145_474_204_166_5).abs() <= 2.0e-7);

    let xy_points = points.map(|point| Point::new_raw_xy(point.x, point.y));
    let xy_source = Shape::Polygon(Polygon::new(
        Ring::try_from_closed(CoordSeq::try_from_points(&xy_points).unwrap()).unwrap(),
        Vec::new(),
    ));
    let xy_metric_accesses = std::sync::atomic::AtomicUsize::new(0);
    let xy_metric_access = || {
        xy_metric_accesses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(&metric)
    };
    let xy_context =
        SourceArcContext::new(TopologyDomain::Geographic(frame), Some(&xy_metric_access));
    let xy_output = coverage_simplify_with_context(
        std::slice::from_ref(&xy_source),
        0.1,
        SimplifyMethod::Vw,
        true,
        &xy_context,
    )
    .unwrap();
    let Shape::Polygon(xy_polygon) = xy_output.first().unwrap() else {
        panic!("XY coverage simplification preserves its polygon row");
    };
    assert_eq!(
        xy_metric_accesses.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        published
            .iter()
            .map(|point| (point.x, point.y))
            .collect::<Vec<_>>(),
        xy_polygon
            .shell
            .coords()
            .iter()
            .map(|point| (point.x, point.y))
            .collect::<Vec<_>>()
    );
}
