use super::*;

#[test]
fn computed_only_zero_longitude_class_uses_canonical_spelling() {
    let points = [Point::new_raw_xy(370.0, 0.0), Point::new_raw_xy(370.0, 1.0)];
    let mut budget = ExpansionBudget::new("computed meridian", "computed edges");
    let output = publish_computed_chain(
        degrees(),
        points.as_slice(),
        false,
        &mut budget,
        |_, start, end| {
            ComputedEdgeIntent::retained_binary64_pair(degrees().longitude(), [start.x, end.x])
        },
    )
    .unwrap()
    .unwrap();

    assert_eq!(output, [
        Point::new_raw_xy(10.0, 0.0),
        Point::new_raw_xy(10.0, 1.0),
    ]);
    assert_eq!(
        degrees()
            .longitude()
            .chart_step(output[0].x, output[1].x)
            .unwrap()
            .delta(degrees().longitude()),
        0.0
    );

    let mut stored_budget = ExpansionBudget::new("stored meridian", "computed edges");
    let stored = publish_computed_chain(
        degrees(),
        points.as_slice(),
        false,
        &mut stored_budget,
        |_, _, _| {
            ComputedEdgeIntent::retained_chart_witness(
                degrees(),
                [points[0].x, points[1].x],
                ChartStep::Shortest(EdgeDirection::Minor(0.0)),
            )
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(stored, output);
}
#[test]
fn zero_longitude_class_crosses_ring_closure_without_rewriting_nonzero_neighbors() {
    let points = [
        Point::new_raw_xy(250.0, 0.0),
        Point::new_raw_xy(250.0, 1.0),
        Point::new_raw_xy(260.0, 2.0),
        Point::new_raw_xy(250.0, -1.0),
        Point::new_raw_xy(250.0, 0.0),
    ];
    let mut budget = ExpansionBudget::new("ring meridian", "computed edges");
    let output = publish_computed_chain_with_vertex_intents(
        degrees(),
        points.as_slice(),
        true,
        &mut budget,
        |_, start, end| {
            ComputedEdgeIntent::retained_binary64_pair(degrees().longitude(), [start.x, end.x])
        },
        |index, _| {
            if index == 1 {
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
        Point::new_raw_xy(-100.0, 2.0),
        points[3],
        points[0],
    ]);
}
#[test]
fn tiny_nonzero_edge_next_to_preserved_meridian_stays_canonical() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let tiny = exact_power_of_two(-20);
    let points = [
        Point::new_raw_xy(250.0, 0.0),
        Point::new_raw_xy(250.0, 1.0),
        Point::new_raw_xy(250.0 + tiny, 2.0),
    ];
    let mut budget = ExpansionBudget::new("tiny edge", "computed edges");
    let output = publish_computed_chain_with_vertex_intents(
        degrees(),
        points.as_slice(),
        false,
        &mut budget,
        |index, start, end| {
            Ok(if index == 0 {
                ComputedEdgeIntent::preserved(true)
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
    let delta = points[2].x - points[1].x;

    assert_eq!(output.len(), 3);
    assert_eq!(output[0], points[0]);
    assert_eq!(output[1], points[1]);
    assert_eq!(
        output[2].x,
        degrees()
            .longitude()
            .canonicalize(points[2].x)
            .unwrap()
            .get()
    );
    assert_eq!(
        degrees()
            .longitude()
            .chart_step(output[1].x, output[2].x)
            .unwrap()
            .delta(degrees().longitude()),
        delta
    );
}
#[test]
fn zero_longitude_class_rejects_conflicting_preserved_spellings() {
    let points = [
        Point::new_raw_xy(250.0, 0.0),
        Point::new_raw_xy(-110.0, 1.0),
    ];
    let mut budget = ExpansionBudget::new("zero class conflict", "computed edges");
    let error = publish_computed_chain_with_vertex_intents(
        degrees(),
        points.as_slice(),
        false,
        &mut budget,
        |_, _, _| {
            let axis = degrees().longitude();
            Ok(ComputedEdgeIntent::retained_native(
                RetainedChartDisplacement::from_native_endpoints(
                    [
                        axis.canonicalize(points[0].x).unwrap(),
                        axis.canonicalize(points[1].x).unwrap(),
                    ],
                    0,
                ),
            ))
        },
        |_, _| ComputedVertexIntent::Preserved,
    )
    .unwrap_err();

    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
            operation: "zero class conflict",
            ..
        })
    ));
    let message = error.to_string();
    assert!(message.contains("250.0"), "{message}");
    assert!(message.contains("-110.0"), "{message}");
}
