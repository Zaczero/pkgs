use super::*;

#[test]
fn raw_computed_shape_publication_subdivides_the_generated_line() {
    let source = Shape::LineString(
        LineSeq::try_new(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(10.0, 20.0),
                Point::new_raw_xy(260.0, 40.0),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    let output =
        publish_raw_computed_shape(&source, GeographicFrame::crs84(), "computed_line").unwrap();
    let Shape::LineString(line) = output else {
        panic!("computed line remains lineal");
    };
    let points = line.as_coords().iter().collect::<Vec<_>>();
    assert_eq!(points, [
        Point::new_raw_xy(10.0, 20.0),
        Point::new_raw_xy(135.0, 30.0),
        Point::new_raw_xy(-100.0, 40.0),
    ]);
}
#[test]
fn smooth_publication_preserves_copied_knots_and_canonicalizes_samples() {
    let source_points = [
        Point::new_raw_xy(370.0, 0.0),
        Point::new_raw_xy(500.0, 10.0),
        Point::new_raw_xy(620.0, 0.0),
    ];
    let source = Shape::LineString(
        LineSeq::try_new(CoordSeq::try_from_points(&source_points).unwrap()).unwrap(),
    );
    let mut budget = ExpansionBudget::new("smooth", "iterations");
    let (smoothed, preserved_vertices) = source
        .smooth_budgeted_with_vertex_intents(2, SmoothMethod::CatmullRom, true, &mut budget)
        .unwrap();
    let Shape::LineString(smoothed_line) = &smoothed else {
        panic!("smoothing retains its line structure");
    };
    assert_eq!(preserved_vertices, [vec![0, 4, 8]]);

    let published = super::publish_changed_raw_computed_shape(
        &source,
        &smoothed,
        GeographicFrame::crs84(),
        &preserved_vertices,
        &SourceArcContext::cartesian(),
        &mut budget,
    )
    .unwrap();
    let Shape::LineString(published_line) = published else {
        panic!("publication retains its line structure");
    };
    assert_eq!(published_line.len(), smoothed_line.len());
    let output = published_line.as_coords().iter().collect::<Vec<_>>();
    for point in source_points {
        assert!(
            output.contains(&point),
            "preserved knot {point:?} was rewritten"
        );
    }
    for point in output {
        if !source_points.contains(&point) {
            assert!(
                (-180.0..180.0).contains(&point.x),
                "computed sample was not canonical: {point:?}"
            );
        }
    }
}
#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn smooth_publisher_carries_inserted_native_chord_samples_by_ellipsoidal_arclength() {
    const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
    let state = crate::crs::runtime_state_snapshot().unwrap();
    let model = crate::crs::metric_model_at(Some(CRS), &state).unwrap();
    let crate::crs::ResolvedMetric::Geodesic(metric) =
        crate::crs::ResolvedMetric::from_model(&model).unwrap()
    else {
        panic!("the custom longlat CRS has a geodesic metric");
    };
    let frame = metric.frame;
    let source = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_axes(10.0, -30.0, ZOrdinate(Some(-0.0)), MOrdinate(Some(101.0)))
                    .unwrap(),
                Point::new_axes(1010.0, 70.0, ZOrdinate(Some(47.0)), MOrdinate(Some(701.0)))
                    .unwrap(),
                Point::new_axes(1110.0, 80.0, ZOrdinate(Some(57.0)), MOrdinate(Some(801.0)))
                    .unwrap(),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    let mut budget = ExpansionBudget::new("smooth", "iterations");
    let (smoothed, preserved_vertices) = source
        .smooth_budgeted_with_vertex_intents(1, SmoothMethod::Chaikin, true, &mut budget)
        .unwrap();
    let Shape::LineString(smoothed_line) = &smoothed else {
        panic!("smoothing retains its line structure");
    };
    assert_eq!(smoothed_line.len(), 4);
    assert_eq!(preserved_vertices, [vec![0, 3]]);

    let metric_accesses = std::sync::atomic::AtomicUsize::new(0);
    let metric_access = || {
        metric_accesses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(&metric)
    };
    let context = SourceArcContext::new(TopologyDomain::Geographic(frame), Some(&metric_access));
    let published = publish_changed_raw_computed_shape(
        &source,
        &smoothed,
        frame,
        &preserved_vertices,
        &context,
        &mut budget,
    )
    .unwrap();
    let Shape::LineString(published_line) = published else {
        panic!("publication retains its line structure");
    };
    let points = published_line.as_coords().iter().collect::<Vec<_>>();
    assert!(points.len() > smoothed_line.len());
    assert!(metric_accesses.load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert_eq!(points[0].x, 10.0);
    assert_eq!(points[0].z().unwrap().to_bits(), (-0.0_f64).to_bits());
    assert_eq!(points[0].m(), Some(101.0));
    assert_eq!(points.last().unwrap().x, 1110.0);
    assert_eq!(points.last().unwrap().z(), Some(57.0));
    assert_eq!(points.last().unwrap().m(), Some(801.0));

    let xy_source = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(10.0, -30.0),
                Point::new_raw_xy(1010.0, 70.0),
                Point::new_raw_xy(1110.0, 80.0),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    let mut xy_budget = ExpansionBudget::new("smooth", "iterations");
    let (xy_smoothed, xy_preserved) = xy_source
        .smooth_budgeted_with_vertex_intents(1, SmoothMethod::Chaikin, true, &mut xy_budget)
        .unwrap();
    let xy_metric_accesses = std::sync::atomic::AtomicUsize::new(0);
    let xy_metric_access = || {
        xy_metric_accesses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(&metric)
    };
    let xy_context =
        SourceArcContext::new(TopologyDomain::Geographic(frame), Some(&xy_metric_access));
    let xy_published = publish_changed_raw_computed_shape(
        &xy_source,
        &xy_smoothed,
        frame,
        &xy_preserved,
        &xy_context,
        &mut xy_budget,
    )
    .unwrap();
    let Shape::LineString(xy_line) = xy_published else {
        panic!("XY publication retains its line structure");
    };
    assert_eq!(
        xy_metric_accesses.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        points
            .iter()
            .map(|point| (point.x, point.y))
            .collect::<Vec<_>>(),
        xy_line
            .as_coords()
            .iter()
            .map(|point| (point.x, point.y))
            .collect::<Vec<_>>()
    );
    assert_eq!(xy_line.axes(), CoordinateAxes::XY);

    let sample = points
        .iter()
        .find(|point| (point.x - 177.5).abs() <= 1e-12 && (point.y - 58.75).abs() <= 1e-12)
        .expect("the 275-degree generated edge is subdivided at its midpoint");
    assert!(
        (sample.z().unwrap() - 43.730_417_185_196_416).abs() <= 2.0e-8,
        "inserted Z={:?}",
        sample.z()
    );
    assert!(
        (sample.m().unwrap() - 655.145_474_204_166_5).abs() <= 2.0e-7,
        "inserted M={:?}",
        sample.m()
    );
    assert!(
        (sample.z().unwrap() - 42.375).abs() > 1.0,
        "chart-linear Z would have been 42.375"
    );
}
