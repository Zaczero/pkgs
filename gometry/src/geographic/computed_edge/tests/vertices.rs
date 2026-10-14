use super::*;

#[test]
fn sampled_closed_chain_observes_the_preserved_first_full_record() {
    let first = Point::new_raw_axes(-0.0, 0.0, ZOrdinate(Some(3.0)), MOrdinate(Some(11.0)));
    let middle = Point::new_raw_axes(10.0, 0.0, ZOrdinate(Some(7.0)), MOrdinate(Some(19.0)));
    let top = Point::new_raw_axes(10.0, 10.0, ZOrdinate(Some(13.0)), MOrdinate(Some(23.0)));
    let ring = [first, middle, top, first];
    let mut budget = ExpansionBudget::new("sampled closure", "computed edges");
    let mut interior_samples = 0_usize;
    let mut observed = Vec::new();
    let output = publish_computed_chain_with_vertex_intents_and_samples(
        degrees(),
        ring.as_slice(),
        true,
        &mut budget,
        |_, start, end| {
            ComputedEdgeIntent::retained_binary64_pair(degrees().longitude(), [start.x, end.x])
        },
        |index, _| {
            if index == ring.len().wrapping_sub(1) {
                ComputedVertexIntent::Sampled
            } else if index == 0 {
                ComputedVertexIntent::Preserved
            } else {
                ComputedVertexIntent::Sampled
            }
        },
        |_, endpoints, _, parameter, _| {
            let &[start, end] = endpoints;
            interior_samples = interior_samples.wrapping_add(1);
            Ok(ComputedSampleOrdinates {
                y: interpolate_subdivision_ordinate(start.y, end.y, parameter),
                z: ZOrdinate(interpolate_optional_ordinate(start.z(), end.z(), parameter)),
                m: MOrdinate(interpolate_optional_ordinate(start.m(), end.m(), parameter)),
            })
        },
        |edge, _, _, parameter, _, point| {
            observed.push((edge, parameter, point));
            Ok(())
        },
    )
    .unwrap()
    .unwrap();

    assert_eq!(interior_samples, 0);
    assert_eq!(output.first(), output.last());
    assert_eq!(output[0].x.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(output[3].x.to_bits(), output[0].x.to_bits());
    assert_eq!(output[3].z(), output[0].z());
    assert_eq!(output[3].m(), output[0].m());
    let (edge, parameter, observed_closure) = observed
        .last()
        .expect("publisher observes the closing sample");
    assert_eq!((*edge, *parameter), (2, 1.0));
    assert_eq!(observed_closure.x.to_bits(), output[0].x.to_bits());
    assert_eq!(observed_closure.z(), output[0].z());
    assert_eq!(observed_closure.m(), output[0].m());
}
#[test]
fn split_computed_edges_keep_preserved_endpoint_records() {
    let points = [
        Point::new_raw_xy(370.0, 20.0),
        Point::new_raw_xy(260.0, 40.0),
    ];
    let mut budget = ExpansionBudget::new("test", "computed edges");
    let output = publish_computed_chain_with_vertex_intents(
        degrees(),
        points.as_slice(),
        false,
        &mut budget,
        |_, _, _| {
            let axis = degrees().longitude();
            let edge = RetainedChartDisplacement::from_native_endpoints(
                [
                    axis.canonicalize(points[0].x).unwrap(),
                    axis.canonicalize(points[1].x).unwrap(),
                ],
                1,
            );
            Ok(ComputedEdgeIntent::retained_native(edge))
        },
        |_, _| ComputedVertexIntent::Preserved,
    )
    .unwrap()
    .unwrap();
    assert_eq!(output, [
        points[0],
        Point::new_raw_xy(135.0, 30.0),
        points[1],
    ]);
    assert_eq!(
        degrees()
            .longitude()
            .chart_step(output[0].x, output[1].x)
            .unwrap()
            .delta(degrees().longitude()),
        125.0
    );
    assert_eq!(
        degrees()
            .longitude()
            .chart_step(output[1].x, output[2].x)
            .unwrap()
            .delta(degrees().longitude()),
        125.0
    );
}
#[test]
fn mixed_chains_replay_source_edges_and_split_only_computed_edges() {
    let points = [
        Point::new_raw_xy(370.0, 0.0),
        Point::new_raw_xy(0.0, 5.0),
        Point::new_raw_xy(250.0, 10.0),
    ];
    let mut budget = ExpansionBudget::new("test", "computed edges");
    let output = publish_computed_chain_with_vertex_intents(
        degrees(),
        points.as_slice(),
        false,
        &mut budget,
        |index, start, end| {
            Ok(if index == 0 {
                ComputedEdgeIntent::preserved(end.x - start.x == 0.0)
            } else {
                ComputedEdgeIntent::retained_binary64_pair(degrees().longitude(), [start.x, end.x])?
            })
        },
        |index, _| {
            if index < 2 {
                ComputedVertexIntent::Preserved
            } else {
                ComputedVertexIntent::Canonical
            }
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(output, [
        points[0],
        points[1],
        Point::new_raw_xy(125.0, 7.5),
        Point::new_raw_xy(-110.0, 10.0),
    ]);
}
#[test]
fn published_samples_keep_the_original_root_parameter_and_lift() {
    let points = [
        Point::new_raw_xy(10.0, 20.0),
        Point::new_raw_xy(260.0, 40.0),
    ];
    let frame = degrees();
    let mut budget = ExpansionBudget::new("exact interval", "computed edges");
    let mut observed = Vec::new();
    let output = publish_computed_chain_with_vertex_intents_and_samples(
        frame,
        points.as_slice(),
        false,
        &mut budget,
        |_, start, end| {
            ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
        },
        |_, _| ComputedVertexIntent::Sampled,
        |_, endpoints, _, parameter, _| {
            let &[start, end] = endpoints;
            Ok(ComputedSampleOrdinates {
                y: interpolate_subdivision_ordinate(start.y, end.y, parameter),
                z: ZOrdinate(None),
                m: MOrdinate(None),
            })
        },
        |edge, _, _, parameter, retained, point| {
            observed.push((edge, parameter, retained.longitude.get(), point));
            Ok(())
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(output, [
        Point::new_raw_xy(10.0, 20.0),
        Point::new_raw_xy(135.0, 30.0),
        Point::new_raw_xy(-100.0, 40.0),
    ]);
    assert_eq!(observed, [
        (0, 0.0, 10.0, Point::new_raw_xy(10.0, 20.0)),
        (0, 0.5, 135.0, Point::new_raw_xy(135.0, 30.0)),
        (0, 1.0, -100.0, Point::new_raw_xy(-100.0, 40.0)),
    ]);
}
#[test]
fn canonical_period_line_carries_zm_at_original_root_arclength() {
    let start = Point::new_raw_axes(10.0, 20.0, ZOrdinate(Some(3.0)), MOrdinate(Some(11.0)));
    let end = Point::new_raw_axes(370.0, 40.0, ZOrdinate(Some(19.0)), MOrdinate(Some(43.0)));
    let output = publish(&[start, end], false, &[360.0]);

    assert_eq!(output.first(), Some(&start));
    assert_eq!(
        output.last(),
        Some(&Point::new_raw_axes(
            10.0,
            40.0,
            ZOrdinate(Some(19.0)),
            MOrdinate(Some(43.0)),
        ))
    );
    assert_eq!(
        output
            .iter()
            .map(|point| (point.x, point.y))
            .collect::<Vec<_>>(),
        [
            (10.0, 20.0),
            (100.0, 25.0),
            (-170.0, 30.0),
            (-80.0, 35.0),
            (10.0, 40.0)
        ]
    );
    for (point, (expected_z, expected_m)) in output.iter().skip(1).take(3).zip([
        (7.284_932_436_674_63, 19.569_864_873_349_26),
        (11.400_416_857_860_609, 27.800_833_715_721_218),
        (15.315_320_037_823_382, 35.630_640_075_646_77),
    ]) {
        assert!((point.z().unwrap() - expected_z).abs() <= 2.0e-12);
        assert!((point.m().unwrap() - expected_m).abs() <= 4.0e-12);
    }
}
