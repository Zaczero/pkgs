#[test]
fn emitted_longitude_reduces_chart_period_before_rounding() {
    let frame = GeographicFrame::crs84();
    let source = 122.427_194_f64;
    let period = ExactRational::from_f64(frame.longitude().period());
    let shifted = ExactRational::from_f64(source).try_add(&period).unwrap();
    let rounded_chart_alias = source + frame.longitude().period();

    assert_eq!(
        frame
            .longitude()
            .canonicalize(rounded_chart_alias)
            .unwrap()
            .get()
            .to_bits(),
        source.next_down().to_bits()
    );
    assert_eq!(
        round_canonical_longitude(frame, &shifted)
            .unwrap()
            .to_bits(),
        source.to_bits()
    );
}

#[test]
fn emitted_nodes_reject_distinct_ids_for_one_rounded_key() {
    let frame = GeographicFrame::crs84();
    let third = ExactRational::from_f64(1.0)
        .try_quotient(&ExactRational::from_f64(3.0))
        .unwrap();
    let original = ExactChartPoint::from_rationals(
        ExactRational::from_f64(1.0 / 3.0),
        ExactRational::zero(),
    );
    let inserted = ExactChartPoint::from_rationals(third, ExactRational::zero());
    let original_key = emitted_node_key(
        frame,
        XY::new(round_canonical_longitude(frame, &original.x).unwrap(), 0.0),
    )
    .unwrap();
    let inserted_key = emitted_node_key(
        frame,
        XY::new(round_canonical_longitude(frame, &inserted.x).unwrap(), 0.0),
    )
    .unwrap();
    assert_eq!(original_key, inserted_key);
    let mut distinct_nodes = vec![(original_key, QNodeId(7)), (inserted_key, QNodeId(8))];
    let error = certify_node_injectivity(&mut distinct_nodes).unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactNodeCollision {
            first: 7,
            second: 8,
        })
    ));

    let shifted = ExactChartPoint::from_rationals(
        original
            .x
            .try_add(&ExactRational::from_f64(frame.longitude().period()))
            .unwrap(),
        original.y,
    );
    let shifted_key = emitted_node_key(
        frame,
        XY::new(round_canonical_longitude(frame, &shifted.x).unwrap(), 0.0),
    )
    .unwrap();
    let mut same_node_alias = vec![(original_key, QNodeId(7)), (shifted_key, QNodeId(7))];
    certify_node_injectivity(&mut same_node_alias).unwrap();
}
