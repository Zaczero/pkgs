fn rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Polygon {
    Polygon::new(
        Ring::from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(minx, miny),
                Point::new_raw_xy(maxx, miny),
                Point::new_raw_xy(maxx, maxy),
                Point::new_raw_xy(minx, maxy),
                Point::new_raw_xy(minx, miny),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    )
}
fn donut() -> Polygon {
    Polygon::new(rectangle(0.0, 0.0, 10.0, 10.0).shell, vec![
        Ring::from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(3.0, 3.0),
                Point::new_raw_xy(7.0, 3.0),
                Point::new_raw_xy(7.0, 7.0),
                Point::new_raw_xy(3.0, 7.0),
                Point::new_raw_xy(3.0, 3.0),
            ])
            .unwrap(),
        )
        .unwrap(),
    ])
}
fn prepared_q_topology(
    shape: &Shape,
    frame: GeographicFrame,
) -> std::sync::Arc<ExactGeographicTopology> {
    let plan = PreparedPlan::transient(shape);
    let cache = crate::geometry::FrameDependentCaches::default();
    GeographicPrepared::try_new(&plan, &cache, frame)
        .expect("fixture admits")
        .exact_topology()
        .unwrap()
}
fn polygon(points: &[[f64; 2]]) -> Polygon {
    Polygon::new(
        Ring::from_closed(
            CoordSeq::try_from_points(
                &points
                    .iter()
                    .map(|&[x, y]| Point::new_raw_xy(x, y))
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    )
}
fn radial_polygon(vertex_count: usize) -> Polygon {
    let mut points: Vec<_> = (0..vertex_count)
        .map(|index| {
            let angle = 2.0 * std::f64::consts::PI * index as f64 / vertex_count as f64;
            let scale = 1.0 + 0.3 * (3.0 * angle).sin() + 0.08 * (7.0 * angle).cos();
            Point::new_raw_xy(10.0 * scale * angle.cos(), 10.0 * scale * angle.sin())
        })
        .collect();
    points.push(points[0]);
    Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        Vec::new(),
    )
}
fn oscillating_line(case: usize) -> Shape {
    let phase = 0.31 * case as f64;
    Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(-20.0, -12.0 + 24.0 * phase.sin()),
                Point::new_raw_xy(20.0, 12.0 * phase.cos()),
                Point::new_raw_xy(-18.0, 10.0 * (phase + 0.7).sin()),
                Point::new_raw_xy(18.0, -10.0 * (phase + 0.4).cos()),
            ])
            .unwrap(),
        )
        .expect("oscillating line has four vertices"),
    )
}
fn line(points: &[[f64; 2]]) -> Shape {
    Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(
                &points
                    .iter()
                    .map(|&[x, y]| Point::new_raw_xy(x, y))
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .expect("line has at least two vertices"),
    )
}
fn assert_polygon_line_case(name: &str, polygon: &Shape, line: &Shape, expected: &str) {
    let polygon_plan = PreparedPlan::transient(polygon);
    let line_plan = PreparedPlan::transient(line);
    let forward = partition_relate_data(&polygon_plan, &line_plan).unwrap();
    assert_eq!(forward.text(), expected, "{name}: known matrix");
    assert_eq!(
        super::super::evaluator::relate(&polygon_plan, &line_plan).unwrap(),
        forward,
        "{name}: witness evaluator"
    );
    assert_eq!(
        partition_relate_data(&line_plan, &polygon_plan).unwrap().0,
        forward.transpose().0,
        "{name}: transpose"
    );
    assert_eq!(
        super::super::evaluator::relate(&line_plan, &polygon_plan).unwrap(),
        forward.transpose(),
        "{name}: witness evaluator transpose"
    );
    assert_eq!(
        graph_relate_shapes(polygon, line).unwrap().0,
        forward.0,
        "{name}: ordinary dispatch"
    );
    assert_eq!(
        graph_relate_shapes(line, polygon).unwrap().0,
        forward.transpose().0,
        "{name}: reverse ordinary dispatch"
    );
}
fn assert_exact_relation_transposes(name: &str, left: &Shape, right: &Shape) {
    let left_plan = PreparedPlan::transient(left);
    let right_plan = PreparedPlan::transient(right);
    let forward = partition_relate_data(&left_plan, &right_plan).unwrap();
    let reverse = partition_relate_data(&right_plan, &left_plan).unwrap();
    assert_eq!(reverse.0, forward.transpose().0, "{name}: transpose");
    assert_eq!(
        super::super::evaluator::relate(&left_plan, &right_plan).unwrap(),
        forward,
        "{name}: witness evaluator"
    );
    assert_eq!(
        super::super::evaluator::relate(&right_plan, &left_plan).unwrap(),
        forward.transpose(),
        "{name}: witness evaluator transpose"
    );
}
fn assert_relation_case(name: &str, left: &Shape, right: &Shape, expected: &str) {
    let left_plan = PreparedPlan::transient(left);
    let right_plan = PreparedPlan::transient(right);
    let forward = partition_relate_data(&left_plan, &right_plan).unwrap();
    assert_eq!(forward.text(), expected, "{name}: forward");
    assert_eq!(
        super::super::evaluator::relate(&left_plan, &right_plan).unwrap(),
        forward,
        "{name}: witness evaluator"
    );
    assert_eq!(
        partition_relate_data(&right_plan, &left_plan).unwrap().0,
        forward.transpose().0,
        "{name}: transpose"
    );
    assert_eq!(
        super::super::evaluator::relate(&right_plan, &left_plan).unwrap(),
        forward.transpose(),
        "{name}: witness evaluator transpose"
    );
}
