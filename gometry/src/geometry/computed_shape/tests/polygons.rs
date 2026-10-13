use super::*;

#[test]
fn polygon_edge_roles_preserve_clip_fragments_and_publish_bisectors() {
    let source = Polygon::new(
        Ring::try_from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(370.0, 0.0),
                Point::new_raw_xy(0.0, 5.0),
                Point::new_raw_xy(250.0, 10.0),
                Point::new_raw_xy(250.0, 30.0),
                Point::new_raw_xy(370.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    );
    let mut budget = ExpansionBudget::new("voronoi", "computed edges");
    let output = publish_polygon_with_edge_roles(
        &source,
        &[vec![false, true, true, false]],
        GeographicFrame::crs84(),
        &mut budget,
    )
    .unwrap();
    assert_eq!(output.shell.coords().iter().collect::<Vec<_>>(), [
        Point::new_raw_xy(370.0, 0.0),
        Point::new_raw_xy(0.0, 5.0),
        Point::new_raw_xy(125.0, 7.5),
        Point::new_raw_xy(250.0, 10.0),
        Point::new_raw_xy(250.0, 30.0),
        Point::new_raw_xy(370.0, 0.0),
    ]);
}
