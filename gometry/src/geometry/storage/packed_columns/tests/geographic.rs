use super::*;

#[test]
fn ordinary_geographic_screen_declines_a_seam_vertex_in_row_k() {
    let rows = geographic_array(vec![
        geographic_polygon(0.0, 31.0),
        geographic_polygon(179.0, 31.0),
        geographic_polygon(4.0, 31.0),
    ]);
    assert!(!ordinary_geographic(&rows, GeographicFrame::crs84()));

    let explicit_period = geographic_array(vec![Shape::Polygon(Polygon::new(
        geographic_ring(&[(0.0, -10.0), (360.0, -10.0), (300.0, 10.0), (0.0, -10.0)]),
        Vec::new(),
    ))]);
    assert!(!ordinary_geographic(
        &explicit_period,
        GeographicFrame::crs84()
    ));
}

#[test]
fn faithful_geographic_screen_keeps_alias_rows_and_declines_mixed_charts() {
    let frame = GeographicFrame::crs84();
    let faithful = |array: &PyGeometryArray| {
        array
            .packed_columns()
            .unwrap()
            .faithful_geographic_predicates(frame)
    };
    let aliases = geographic_array(vec![
        geographic_polygon(200.0, 31.0),
        geographic_polygon(-210.0, 31.0),
        geographic_polygon(350.0, 31.0),
    ]);
    assert!(faithful(&aliases));
    assert!(faithful(&aliases.gather_logical_rows(&[2, 0, 2]).unwrap()));
    let seam = geographic_array(vec![geographic_polygon(179.0, 31.0)]);
    assert!(!faithful(&seam));
    let long_edge = geographic_array(vec![Shape::Polygon(Polygon::new(
        geographic_ring(&[
            (200.0, 0.0),
            (500.0, 0.0),
            (500.0, 5.0),
            (200.0, 5.0),
            (200.0, 0.0),
        ]),
        Vec::new(),
    ))]);
    assert!(!faithful(&long_edge));
    let alias_hole = geographic_array(vec![Shape::Polygon(Polygon::new(
        geographic_ring(&[
            (200.0, 0.0),
            (210.0, 0.0),
            (210.0, 10.0),
            (200.0, 10.0),
            (200.0, 0.0),
        ]),
        vec![geographic_ring(&[
            (-159.0, 1.0),
            (-158.0, 1.0),
            (-158.0, 2.0),
            (-159.0, 2.0),
            (-159.0, 1.0),
        ])],
    ))]);
    assert!(!faithful(&alias_hole));
    let invalid = geographic_array(vec![geographic_polygon(200.0, 91.0)]);
    assert!(!faithful(&invalid));
}

#[rstest::rstest]
#[case::pole_90_0_f64(90.0_f64)]
#[case::negative_pole_90_0(-90.0)]
fn ordinary_geographic_screen_declines_pole_linework_but_keeps_point_rows(#[case] pole: f64) {
    let frame = GeographicFrame::crs84();

    let line = |latitude| {
        geographic_array(vec![Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(vec![0.0, 20.0, 10.0], vec![latitude; 3], None, None)
                    .unwrap(),
            )
            .unwrap(),
        )])
    };
    assert!(!ordinary_geographic(&line(pole), frame));
    assert!(ordinary_geographic(
        &line(90.0_f64.next_down().copysign(pole)),
        frame
    ));
    assert!(ordinary_geographic(
        &geographic_array(vec![Shape::Point(Point::new_raw_xy(0.0, pole))]),
        frame
    ));
}

#[test]
fn ordinary_geographic_screen_resets_at_each_row_and_obeys_frame_bounds() {
    let rows = geographic_array(vec![
        geographic_polygon(-160.0, 31.0),
        geographic_polygon(150.0, 31.0),
    ]);
    assert!(
        ordinary_geographic(&rows, GeographicFrame::crs84()),
        "the longitude jump between rows is not a source edge",
    );

    let invalid_latitude = geographic_array(vec![geographic_polygon(0.0, 91.0)]);
    assert!(!ordinary_geographic(
        &invalid_latitude,
        GeographicFrame::crs84()
    ));

    let factor = AngularFactor::try_new("unit", std::f64::consts::TAU / 400.0_f64.next_up())
        .expect("rounded-up grad period has a valid angular factor");
    let grads = GeographicFrame::try_new(factor, factor, 0.0)
        .expect("rounded-up grad period has a valid frame");
    let rounded_alias = geographic_array(vec![geographic_polygon(199.0, 31.0)]);
    assert!(!ordinary_geographic(&rounded_alias, grads));

    let exterior_pole_ulp = geographic_array(vec![geographic_polygon(0.0, 90.0_f64.next_up())]);
    assert!(!ordinary_geographic(
        &exterior_pole_ulp,
        GeographicFrame::crs84()
    ));
}

#[test]
fn ordinary_geographic_screen_handles_points_lines_and_selected_rows() {
    let points = geographic_array(vec![
        Shape::Point(Point::new_raw_xy(1.0, 2.0)),
        Shape::Point(Point::new_raw_xy(180.0, 2.0)),
        Shape::Point(Point::new_raw_xy(4.0, 5.0)),
    ]);
    assert!(!ordinary_geographic(&points, GeographicFrame::crs84()));
    assert!(ordinary_geographic(
        &points.gather_logical_rows(&[2, 0, 2]).unwrap(),
        GeographicFrame::crs84()
    ));

    let present = geographic_array(vec![
        Shape::Point(Point::new_raw_xy(1.0, 2.0)),
        Shape::Point(Point::new_raw_xy(4.0, 5.0)),
    ]);
    let compact = PyGeometryArray::scatter_present_rows(
        &present,
        ValidityMask::from_vec(4, vec![true, false, true, false]).expect("compact point mask"),
    )
    .expect("present points scatter into compact nullable storage");
    assert!(ordinary_geographic(&compact, GeographicFrame::crs84()));

    let line = |xs| {
        Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_vecs(xs, vec![30.0, 31.0], None, None).unwrap())
                .unwrap(),
        )
    };
    let lines = geographic_array(vec![
        line(vec![-160.0, -150.0]),
        line(vec![150.0, 160.0]),
        line(vec![-170.0, 170.0]),
    ]);
    assert!(!ordinary_geographic(&lines, GeographicFrame::crs84()));
    assert!(ordinary_geographic(
        &lines.gather_logical_row_range(0..2).unwrap(),
        GeographicFrame::crs84()
    ));
}

#[test]
fn ordinary_geographic_screen_resets_at_each_hole_boundary() {
    let shell = geographic_ring(&[
        (-170.0, -20.0),
        (0.0, -20.0),
        (170.0, -20.0),
        (170.0, 20.0),
        (0.0, 20.0),
        (-170.0, 20.0),
        (-170.0, -20.0),
    ]);
    let hole = |west| {
        geographic_ring(&[
            (west, -10.0),
            (west + 10.0, -10.0),
            (west + 10.0, 10.0),
            (west, 10.0),
            (west, -10.0),
        ])
    };
    let polygon = geographic_array(vec![Shape::Polygon(Polygon::new(shell, vec![
        hole(-160.0),
        hole(150.0),
    ]))]);
    assert!(
        ordinary_geographic(&polygon, GeographicFrame::crs84()),
        "the longitude jump between holes is not a source edge",
    );
}
