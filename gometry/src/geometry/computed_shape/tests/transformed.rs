use super::*;

fn geographic_transform_plan() -> crate::crs::TransformPlan {
    let source = Frame::Crs(crs_arc_static("EPSG:4326"));
    let frame = GeometryTransformFrame::new(
        &source,
        crs_arc_static("EPSG:4326"),
        None,
        crate::crs::TransformOptions::default(),
    )
    .unwrap();
    crate::crs::TransformPlan::from_geometry_frame(frame).unwrap()
}
fn publish_xy_transform_line(source_x: [f64; 2], output_x: [f64; 2]) -> Vec<Point> {
    let frame = GeographicFrame::crs84();
    let source = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(source_x[0], 0.0),
                Point::new_raw_xy(source_x[1], 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    let output = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(output_x[0], 0.0),
                Point::new_raw_xy(output_x[1], 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    let plan = geographic_transform_plan();
    let mut metric = plan.target_metric_scratch();
    let Shape::LineString(line) =
        publish_transformed_shape(&source, &output, Some(frame), frame, "to_crs", &mut metric)
            .unwrap()
    else {
        panic!("transformed line publication retains line structure");
    };
    line.iter().collect()
}

#[test]
fn transformed_shape_chain_provider_uses_each_original_endpoint_pair() {
    let frame = GeographicFrame::crs84();
    let source = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(10.0, 0.0),
                Point::new_raw_xy(370.0, 0.0),
                Point::new_raw_xy(370.0, 1.0),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    let output = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(-180.0, 0.0),
                Point::new_raw_xy(-180.0, 1.0),
            ])
            .unwrap(),
        )
        .unwrap(),
    );

    let published =
        map_transformed_shape_chains(&source, &output, frame, frame, |edges, closed| {
            assert!(!closed);
            assert_eq!(edges.edge_count(), 2);
            assert_eq!(edges.source().nth_coord(0).x, 10.0);
            assert_eq!(edges.output().nth_coord(0).x, 0.0);
            assert_eq!(edges.retained_connection(0)?.periods(), 2);
            assert_eq!(edges.retained_connection(1)?.periods(), 0);
            Ok(None)
        })
        .unwrap();

    assert_eq!(published, output);
}
#[test]
fn transformed_shape_chain_provider_keeps_collection_member_correspondence() {
    let frame = GeographicFrame::crs84();
    let source = Shape::geometry_collection(vec![
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(10.0, 0.0),
                    Point::new_raw_xy(370.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        ),
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(20.0, 0.0),
                    Point::new_raw_xy(20.0, 1.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        ),
    ])
    .unwrap();
    let output = Shape::geometry_collection(vec![
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(0.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        ),
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(-180.0, 1.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        ),
    ])
    .unwrap();
    let mut lifts = Vec::new();

    let published =
        map_transformed_shape_chains(&source, &output, frame, frame, |edges, closed| {
            assert!(!closed);
            lifts.push(edges.retained_connection(0)?.periods());
            Ok(None)
        })
        .unwrap();

    assert_eq!(lifts, [1, 0]);
    assert_eq!(published, output);
}
#[test]
fn transformed_packed_windows_keep_gathered_and_duplicate_source_pairing() {
    let frame = GeographicFrame::crs84();
    let source = CoordSeq::try_from_points(&[
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(360.0, 0.0),
        Point::new_raw_xy(10.0, 0.0),
        Point::new_raw_xy(10.0, 0.0),
    ])
    .unwrap();
    let output = CoordSeq::try_from_points(&[
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(-180.0, 0.0),
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(-180.0, 0.0),
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(-180.0, 0.0),
    ])
    .unwrap();
    let source_windows = [2..4, 0..2, 0..2];
    let output_windows = [0..2, 2..4, 4..6];

    let periods = source_windows
        .into_iter()
        .zip(output_windows)
        .map(|(source_window, output_window)| {
            let source = source.view(CoordWindow::from_range(source_window, source.len()));
            let output = output.view(CoordWindow::from_range(output_window, output.len()));
            TransformedChartEdgeProvider::try_new(&source, &output, frame, frame)?
                .retained_connection(0)
                .map(|edge| edge.periods())
        })
        .collect::<crate::error::Result<Vec<_>>>()
        .unwrap();

    assert_eq!(periods, [0, 2, 2]);
}
#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn transformed_circle_reaches_the_native_publisher_with_its_forward_lift() {
    assert_eq!(publish_xy_transform_line([10.0, 370.0], [0.0, -180.0]), [
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(135.0, 0.0),
        Point::new_raw_xy(-90.0, 0.0),
        Point::new_raw_xy(45.0, 0.0),
        Point::new_raw_xy(-180.0, 0.0),
    ]);
}
#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn transformed_circle_reaches_the_native_publisher_with_its_reverse_lift() {
    assert_eq!(publish_xy_transform_line([370.0, 10.0], [-180.0, 0.0]), [
        Point::new_raw_xy(-180.0, 0.0),
        Point::new_raw_xy(45.0, 0.0),
        Point::new_raw_xy(-90.0, 0.0),
        Point::new_raw_xy(135.0, 0.0),
        Point::new_raw_xy(0.0, 0.0),
    ]);
}
