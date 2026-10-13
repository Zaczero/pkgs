fn metric_support_for(wkt: &str) -> ExactMetricSupport {
    let shape = crate::io::parse_wkt(wkt).expect("valid metric-support WKT");
    let inventory = ExactSourceInventory::from_shapes(&[&shape]).unwrap();
    let mut budget = ExpansionBudget::new("hausdorff", "metric support");
    let partition = inventory.partition(&mut budget).unwrap();
    inventory
        .metric_support(&partition, super::AreaLocationStrategy::PersistentSlabs)
        .unwrap()
}

fn metric_support_with_boundary_for(wkt: &str) -> ExactMetricSupport {
    let shape = crate::io::parse_wkt(wkt).expect("valid metric-support WKT");
    let inventory = ExactSourceInventory::from_shapes(&[&shape]).unwrap();
    let mut budget = ExpansionBudget::new("maximum_inscribed_circle", "source");
    let partition = inventory.partition(&mut budget).unwrap();
    inventory
        .metric_support_with_boundary(&partition, super::AreaLocationStrategy::PersistentSlabs)
        .unwrap()
}

fn dyadic(value: f64) -> ExactDyadic {
    ExactDyadic::from_f64(value)
}

fn ratio_matches(actual_num: &ExactDyadic, actual_den: &ExactDyadic, num: f64, den: f64) -> bool {
    compare_ratios(actual_num, actual_den, &dyadic(num), &dyadic(den)).unwrap() == Ordering::Equal
}

fn node_at(node: &ExactMetricNode, x: f64, y: f64) -> bool {
    node_at_ratios(node, x, 1.0, y, 1.0)
}

fn node_at_ratios(node: &ExactMetricNode, x_num: f64, x_den: f64, y_num: f64, y_den: f64) -> bool {
    let [actual_x, actual_y, actual_w] = node.homogeneous();
    ratio_matches(actual_x, actual_w, x_num, x_den)
        && ratio_matches(actual_y, actual_w, y_num, y_den)
}

fn span_between(span: &ExactMetricSpan, first: (f64, f64), second: (f64, f64)) -> bool {
    (node_at(&span.start, first.0, first.1) && node_at(&span.end, second.0, second.1))
        || (node_at(&span.start, second.0, second.1) && node_at(&span.end, first.0, first.1))
}

fn source_is(span: &ExactMetricSpan, source: Segment) -> bool {
    PointKey::new(span.source.start) == PointKey::new(source.start)
        && PointKey::new(span.source.end) == PointKey::new(source.end)
}

fn assert_ratio(actual: &[ExactDyadic; 2], num: f64, den: f64) {
    assert!(
        ratio_matches(&actual[0], &actual[1], num, den),
        "expected {num}/{den}, got {actual:?}"
    );
}

#[test]
fn metric_support_unions_retraced_linework_and_drops_covered_points() {
    let support = metric_support_for(
        "GEOMETRYCOLLECTION(\
         LINESTRING (0 0,2 0,4 0),\
         LINESTRING (4 0,1 0,4 0),\
         LINESTRING (4 0,0 0),\
         POINT (2 0),\
         POINT (8 1))",
    );
    assert!(support.faces.is_empty());
    assert_eq!(support.spans.len(), 3);
    assert!(support.boundary_spans.is_none());
    assert!(
        support
            .spans
            .iter()
            .any(|span| span_between(span, (0.0, 0.0), (1.0, 0.0)))
    );
    assert!(
        support
            .spans
            .iter()
            .any(|span| span_between(span, (1.0, 0.0), (2.0, 0.0)))
    );
    assert!(
        support
            .spans
            .iter()
            .any(|span| span_between(span, (2.0, 0.0), (4.0, 0.0)))
    );
    assert_eq!(support.points.len(), 1);
    assert!(support.boundary_points.is_none());
    assert!(node_at(&support.points[0], 8.0, 1.0));
}

#[test]
fn metric_support_keeps_crossing_nodes_and_original_source_parameters_exact() {
    let support = metric_support_for(
        "GEOMETRYCOLLECTION(\
         LINESTRING (0 0,3 1),\
         LINESTRING (1 -1,1 2))",
    );
    let source = seg(0.0, 0.0, 3.0, 1.0);
    let spans: Vec<_> = support
        .spans
        .iter()
        .filter(|span| source_is(span, source))
        .collect();
    assert_eq!(spans.len(), 2);
    let mut crossing_nodes = Vec::new();
    for span in spans {
        let endpoint = if node_at_ratios(&span.start, 1.0, 1.0, 1.0, 3.0) {
            assert_ratio(&span.start_parameter, 1.0, 3.0);
            &span.start
        } else {
            assert!(node_at_ratios(&span.end, 1.0, 1.0, 1.0, 3.0));
            assert_ratio(&span.end_parameter, 1.0, 3.0);
            &span.end
        };
        crossing_nodes.push(endpoint.homogeneous().clone());
    }
    assert_eq!(crossing_nodes.len(), 2);
    assert_eq!(crossing_nodes[0], crossing_nodes[1]);
    assert!(
        support.spans.iter().any(|span| {
            source_is(span, source)
                && ((ratio_matches(&span.start_parameter[0], &span.start_parameter[1], 0.0, 1.0)
                    && ratio_matches(&span.end_parameter[0], &span.end_parameter[1], 1.0, 3.0))
                    || (ratio_matches(
                        &span.start_parameter[0],
                        &span.start_parameter[1],
                        1.0,
                        3.0,
                    ) && ratio_matches(
                        &span.end_parameter[0],
                        &span.end_parameter[1],
                        1.0,
                        1.0,
                    )))
        }),
        "the oblique source must retain both exact parameter windows"
    );
}

#[test]
fn metric_support_groups_filled_hole_walks_and_ignores_external_holes() {
    let support = metric_support_for(
        "POLYGON ((0 0,6 0,6 6,0 6,0 0),\
                  (1 1,3 1,3 3,1 3,1 1),\
                  (10 10,12 10,12 12,10 12,10 10))",
    );
    assert_eq!(support.spans.len(), 8);
    assert_eq!(support.points.len(), 0);
    assert_eq!(support.faces.len(), 1);
    assert_eq!(support.faces[0].len(), 2);
    let mut cycle_lengths: Vec<_> = support.faces[0].iter().map(Vec::len).collect();
    cycle_lengths.sort_unstable();
    assert_eq!(cycle_lengths, [4, 4]);
    assert!(support.spans.iter().all(|span| {
        [&span.start, &span.end]
            .into_iter()
            .all(|node| !node_at(node, 10.0, 10.0) && !node_at(node, 12.0, 12.0))
    }));
    assert!(
        support.faces[0]
            .iter()
            .flatten()
            .all(|node| { !node_at(node, 10.0, 10.0) && !node_at(node, 12.0, 12.0) })
    );
}

#[test]
fn metric_support_handles_bowties_collapsed_rings_and_coincident_cancellation() {
    let bowtie = metric_support_for("POLYGON ((0 0,4 4,0 4,4 0,0 0))");
    assert_eq!(bowtie.spans.len(), 6);
    assert_eq!(bowtie.faces.len(), 2);
    assert!(bowtie.faces.iter().all(|face| face.len() == 1));
    assert!(bowtie.faces.iter().all(|face| face[0].len() == 3));

    let collapsed = metric_support_for("POLYGON ((0 0,4 0,2 0,0 0))");
    assert!(collapsed.points.is_empty());
    assert!(collapsed.faces.is_empty());
    assert_eq!(collapsed.spans.len(), 2);
    assert!(
        collapsed
            .spans
            .iter()
            .any(|span| span_between(span, (0.0, 0.0), (2.0, 0.0)))
    );
    assert!(
        collapsed
            .spans
            .iter()
            .any(|span| span_between(span, (2.0, 0.0), (4.0, 0.0)))
    );

    for wkt in [
        "POLYGON ((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0))",
        "POLYGON ((0 0,4 0,2 0,0 0),(0 0,4 0,2 0,0 0))",
    ] {
        let cancelled = metric_support_for(wkt);
        assert!(cancelled.spans.is_empty(), "{wkt}");
        assert!(cancelled.points.is_empty(), "{wkt}");
        assert!(cancelled.faces.is_empty(), "{wkt}");
    }
}

#[test]
fn metric_support_preserves_multipart_union_and_mixed_dimension_points() {
    let multipart = metric_support_for(
        "MULTIPOLYGON (\
         ((0 0,2 0,2 2,0 2,0 0)),\
         ((2 0,4 0,4 2,2 2,2 0)))",
    );
    assert_eq!(multipart.faces.len(), 2);
    assert_eq!(multipart.spans.len(), 7);
    for endpoint in [
        ((0.0, 0.0), (2.0, 0.0)),
        ((2.0, 0.0), (4.0, 0.0)),
        ((4.0, 0.0), (4.0, 2.0)),
        ((4.0, 2.0), (2.0, 2.0)),
        ((2.0, 2.0), (0.0, 2.0)),
        ((0.0, 2.0), (0.0, 0.0)),
    ] {
        assert!(
            multipart
                .spans
                .iter()
                .any(|span| span_between(span, endpoint.0, endpoint.1))
        );
    }

    let mixed = metric_support_for(
        "GEOMETRYCOLLECTION(\
         POLYGON ((0 0,4 0,4 4,0 4,0 0)),\
         POINT (10 1))",
    );
    assert_eq!(mixed.spans.len(), 4);
    assert_eq!(mixed.faces.len(), 1);
    assert_eq!(mixed.faces[0].len(), 1);
    assert_eq!(mixed.points.len(), 1);
    assert!(node_at(&mixed.points[0], 10.0, 1.0));
}

#[test]
fn metric_support_boundary_incidence_excludes_internal_and_residual_items() {
    let adjacent = metric_support_with_boundary_for(
        "MULTIPOLYGON (\
         ((0 0,2 0,2 2,0 2,0 0)),\
         ((2 0,4 0,4 2,2 2,2 0)))",
    );
    let adjacent_boundary = adjacent
        .boundary_spans
        .as_ref()
        .expect("boundary selection");
    assert_eq!(adjacent_boundary.len(), adjacent.spans.len());
    assert_eq!(
        adjacent_boundary
            .iter()
            .filter(|&&selected| selected)
            .count(),
        6
    );
    assert!(
        adjacent
            .spans
            .iter()
            .zip(adjacent_boundary)
            .any(|(span, &selected)| { !selected && span_between(span, (2.0, 0.0), (2.0, 2.0)) })
    );

    let overlapping = metric_support_with_boundary_for(
        "MULTIPOLYGON (\
         ((0 0,4 0,4 4,0 4,0 0)),\
         ((2 0,6 0,6 4,2 4,2 0)))",
    );
    let overlapping_boundary = overlapping
        .boundary_spans
        .as_ref()
        .expect("boundary selection");
    assert!(
        overlapping
            .spans
            .iter()
            .zip(overlapping_boundary)
            .any(|(span, &selected)| { !selected && span_between(span, (2.0, 0.0), (2.0, 4.0)) })
    );
    assert!(
        overlapping
            .spans
            .iter()
            .zip(overlapping_boundary)
            .any(|(span, &selected)| { !selected && span_between(span, (4.0, 0.0), (4.0, 4.0)) })
    );

    let holed = metric_support_with_boundary_for(
        "POLYGON ((0 0,10 0,10 10,0 10,0 0),\
                  (4 4,6 4,6 6,4 6,4 4))",
    );
    let holed_boundary = holed.boundary_spans.as_ref().expect("boundary selection");
    assert!(
        holed
            .spans
            .iter()
            .zip(holed_boundary)
            .all(|(_, &selected)| selected)
    );

    let tangent_hole =
        metric_support_with_boundary_for("POLYGON ((0 0,6 0,6 6,0 6,0 0),(0 3,2 2,2 4,0 3))");
    let tangent_hole_boundary = tangent_hole
        .boundary_spans
        .as_ref()
        .expect("boundary selection");
    assert_eq!(tangent_hole_boundary.len(), tangent_hole.spans.len());
    assert!(tangent_hole_boundary.iter().all(|&selected| selected));
    for edge in [
        ((0.0, 3.0), (2.0, 2.0)),
        ((2.0, 2.0), (2.0, 4.0)),
        ((2.0, 4.0), (0.0, 3.0)),
    ] {
        assert!(
            tangent_hole
                .spans
                .iter()
                .zip(tangent_hole_boundary)
                .any(|(span, &selected)| selected && span_between(span, edge.0, edge.1))
        );
    }

    let residual = metric_support_with_boundary_for(
        "GEOMETRYCOLLECTION(\
         POLYGON ((0 0,4 0,4 4,0 4,0 0)),\
         LINESTRING (1 1,3 1,1 1),\
         POINT (2 2))",
    );
    assert!(!residual.faces.is_empty());
    assert_eq!(residual.points.len(), 1);
    assert!(
        residual
            .boundary_points
            .as_ref()
            .expect("boundary selection")
            .iter()
            .all(|&selected| !selected)
    );
    let residual_boundary = residual
        .boundary_spans
        .as_ref()
        .expect("boundary selection");
    assert_eq!(
        residual_boundary
            .iter()
            .filter(|&&selected| selected)
            .count(),
        4
    );
    assert!(
        residual
            .spans
            .iter()
            .zip(residual_boundary)
            .any(|(span, &selected)| { !selected && span_between(span, (1.0, 1.0), (3.0, 1.0)) })
    );
    let external = metric_support_with_boundary_for(
        "GEOMETRYCOLLECTION(\
         POLYGON ((0 0,4 0,4 4,0 4,0 0)),\
         LINESTRING (10 0,12 0))",
    );
    let external_boundary = external
        .boundary_spans
        .as_ref()
        .expect("boundary selection");
    assert!(
        external
            .spans
            .iter()
            .zip(external_boundary)
            .any(|(span, &selected)| { !selected && span_between(span, (10.0, 0.0), (12.0, 0.0)) })
    );
    let collapsed_inside = metric_support_with_boundary_for(
        "GEOMETRYCOLLECTION(\
         POLYGON ((0 0,4 0,4 4,0 4,0 0)),\
         POLYGON ((1 1,3 1,1 1,1 1)))",
    );
    let collapsed_inside_boundary = collapsed_inside
        .boundary_spans
        .as_ref()
        .expect("boundary selection");
    assert_eq!(
        collapsed_inside_boundary
            .iter()
            .filter(|&&selected| selected)
            .count(),
        4
    );
    assert!(collapsed_inside_boundary.iter().any(|&selected| !selected));

    let collapsed = metric_support_with_boundary_for("POLYGON ((0 0,4 0,2 0,0 0))");
    assert!(collapsed.faces.is_empty());
    assert!(
        collapsed
            .boundary_spans
            .as_ref()
            .expect("boundary selection")
            .iter()
            .all(|&selected| selected)
    );

    let line_support = metric_support_with_boundary_for(
        "GEOMETRYCOLLECTION(LINESTRING (0 0,1 0,2 0), POINT (8 1))",
    );
    assert!(line_support.faces.is_empty());
    assert!(
        line_support
            .boundary_spans
            .as_ref()
            .expect("boundary selection")
            .iter()
            .all(|&selected| selected)
    );
    assert_eq!(line_support.boundary_points, Some(vec![true]));
}

#[test]
fn metric_support_boundary_incidence_scales_without_pairwise_matching() {
    let count = 128;
    let mut wkt = String::from("MULTIPOLYGON (");
    for index in 0..count {
        if index != 0 {
            wkt.push(',');
        }
        write!(
            wkt,
            "(({} 0,{} 0,{} 1,{} 1,{} 0))",
            index,
            index + 1,
            index + 1,
            index,
            index
        )
        .unwrap();
    }
    wkt.push(')');
    let support = metric_support_with_boundary_for(&wkt);
    let boundary_spans = support.boundary_spans.as_ref().expect("boundary selection");
    assert_eq!(boundary_spans.len(), support.spans.len());
    assert_eq!(
        boundary_spans.iter().filter(|&&selected| selected).count(),
        2 * count + 2
    );
    assert_eq!(
        boundary_spans.iter().filter(|&&selected| !selected).count(),
        count - 1
    );
}

#[test]
fn metric_support_returns_boundary_flags_only_when_requested() {
    let wkt = "POLYGON ((0 0,4 0,4 4,0 4,0 0))";
    let ordinary = metric_support_for(wkt);
    assert!(ordinary.boundary_spans.is_none());
    assert!(ordinary.boundary_points.is_none());

    let boundary = metric_support_with_boundary_for(wkt);
    assert_eq!(boundary.boundary_spans, Some(vec![true; 4]));
    assert_eq!(boundary.boundary_points, Some(Vec::new()));
}
