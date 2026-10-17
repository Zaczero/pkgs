use super::*;

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn packed_to_crs_lines_rebuild_offsets_after_edge_subdivision() {
    let source = PyGeometryArray::packed_lines(
        CoordSeq::try_from_vecs(
            vec![20_000_000.0, -20_000_000.0],
            vec![0.0, 0.0],
            None,
            None,
        )
        .unwrap(),
        CsrOffsetColumn::try_new(vec![0, 2], 2).unwrap(),
        Frame::Crs(crs_arc_static("EPSG:3857")),
    )
    .unwrap();
    let columns = source
        .packed_columns()
        .expect("packed line source resolves");
    let source_frame = Frame::Crs(crs_arc_static("EPSG:3857"));
    let transform_frame = GeometryTransformFrame::new(
        &source_frame,
        crs_arc_static("EPSG:4326"),
        None,
        crate::crs::TransformOptions::default(),
    )
    .unwrap();
    let transformer = crate::crs::TransformPlan::from_geometry_frame(transform_frame).unwrap();
    let PackedColumnOutput::Lines { coords, offsets } =
        map_selected_to_crs(columns, &transformer).unwrap()
    else {
        panic!("line transforms remain packed lines");
    };
    assert_eq!(offsets.as_slice(), &[0, 3]);
    assert!(coords.xs()[0] > 170.0);
    assert_eq!(coords.xs()[1], 0.0);
    assert!(coords.xs()[2] < -170.0);
    let axis = crate::geographic::GeographicFrame::crs84().longitude();
    for pair in coords.xs().array_windows::<2>() {
        let step = axis.chart_step(pair[0], pair[1]).unwrap().delta(axis);
        assert!(step.abs() <= axis.half_period());
    }
}
