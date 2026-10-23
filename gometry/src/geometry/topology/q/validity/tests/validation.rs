use super::support::{
    box_ring, frame, line, multi_line, multi_polygon, pole_hop, polygon, ring, shape, signature,
    signature_from_issue,
};
use crate::geometry::topology::q::ExactGeographicTopology;
use crate::geometry::{
    CoordSeq, MOrdinate, Point, Polygon, Ring, Shape, TopologyDomain, ZOrdinate,
};

#[test]
fn tv_ar_uy_symmetric_difference_order_012_output_is_valid() {
    let wkt = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/tv-ar-uy-symmetric-difference-order-012.wkt"
    ));
    let geometry = crate::io::parse_wkt(wkt).unwrap();
    assert_eq!(
        crate::geometry::validate_shape_in_frame(&geometry, TopologyDomain::Geographic(frame()))
            .unwrap(),
        None,
        "the TV/AR/UY symmetric-difference output in operand order [0, 1, 2]"
    );
}

#[test]
fn public_validation_accepts_both_opposite_side_seam_spellings() {
    let east = multi_polygon(&[vec![box_ring(170.0, 180.0, 0.0, 10.0)], vec![box_ring(
        180.0, 190.0, 0.0, 10.0,
    )]]);
    let west = multi_polygon(&[vec![box_ring(-190.0, -180.0, 0.0, 10.0)], vec![box_ring(
        -180.0, -170.0, 0.0, 10.0,
    )]]);
    for geometry in [east, west] {
        assert_eq!(
            crate::geometry::validate_shape_in_frame(
                &geometry,
                TopologyDomain::Geographic(frame())
            )
            .unwrap(),
            None
        );
        let intersections = crate::geometry::self_intersections_in_frame(
            &geometry,
            TopologyDomain::Geographic(frame()),
        )
        .unwrap();
        assert!(intersections.is_empty(), "{intersections:?}");
    }

    let same_side = multi_polygon(&[vec![box_ring(180.0, 190.0, 0.0, 10.0)], vec![box_ring(
        -180.0, -170.0, 0.0, 10.0,
    )]]);
    assert_eq!(
        crate::geometry::validate_shape_in_frame(&same_side, TopologyDomain::Geographic(frame()))
            .unwrap()
            .map(|issue| issue.code),
        Some("shared_linework")
    );
    assert_eq!(
        crate::geometry::self_intersections_in_frame(
            &same_side,
            TopologyDomain::Geographic(frame())
        )
        .unwrap()
        .len(),
        4
    );

    let zigzag = shape(
        &[vec![
            (0.0, 0.0),
            (120.0, 0.0),
            (240.0, 0.0),
            (360.0, 0.0),
            (370.0, 10.0),
            (360.0, 20.0),
            (240.0, 20.0),
            (120.0, 20.0),
            (0.0, 20.0),
            (10.0, 10.0),
            (0.0, 0.0),
        ]],
        false,
    );
    assert_eq!(
        crate::geometry::validate_shape_in_frame(&zigzag, TopologyDomain::Geographic(frame()))
            .unwrap(),
        None
    );
}

#[test]
fn quotient_validity_keeps_geometry_collection_members_independent() {
    let collection = Shape::geometry_collection(vec![
        Shape::Polygon(polygon(&[box_ring(170.0, 190.0, 0.0, 10.0)])),
        Shape::Polygon(polygon(&[box_ring(175.0, 185.0, 0.0, 10.0)])),
        Shape::Polygon(polygon(&[box_ring(-20.0, -10.0, 0.0, 10.0)])),
    ])
    .unwrap();
    assert_eq!(
        crate::geometry::validate_shape_in_frame(&collection, TopologyDomain::Geographic(frame()))
            .unwrap(),
        None
    );
}

#[test]
fn quotient_polygon_self_intersections_report_canonical_q_nodes() {
    let points = [
        (170.0, -10.0),
        (190.0, 10.0),
        (170.0, 10.0),
        (190.0, -10.0),
        (170.0, -10.0),
    ];
    let make_shape = |shift: f64| {
        Shape::Polygon(Polygon::new(
            ring(&points.map(|(x, y)| (x + shift, y))),
            Vec::new(),
        ))
    };
    let expected = [((-180.0_f64).to_bits(), 0.0_f64.to_bits())];
    let domain = TopologyDomain::Geographic(frame());
    for geometry in [make_shape(0.0), make_shape(360.0)] {
        let points = crate::geometry::self_intersections_in_frame(&geometry, domain).unwrap();
        assert_eq!(
            points
                .iter()
                .map(|point| (point.x.to_bits(), point.y.to_bits()))
                .collect::<Vec<_>>(),
            expected
        );
        let prepared = crate::geometry::PreparedPlan::transient(&geometry);
        assert!(!crate::geometry::is_simple_data_in_frame(&prepared, domain).unwrap());
    }

    let pole_pair = shape(&[pole_hop(-180.0, 80.0, 90.0)], false);
    assert_eq!(
        crate::geometry::validate_shape_in_frame(&pole_pair, domain).unwrap(),
        None
    );
    assert!(
        crate::geometry::self_intersections_in_frame(&pole_pair, domain)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn quotient_line_self_intersections_and_multiline_boundaries_use_q_nodes() {
    let domain = TopologyDomain::Geographic(frame());
    let crossing = line(&[(170.0, -10.0), (190.0, 10.0), (170.0, 10.0), (190.0, -10.0)]);
    assert_eq!(
        crate::geometry::self_intersections_in_frame(&crossing, domain).unwrap(),
        [Point::new_raw_xy(-180.0, 0.0)]
    );
    let crossing_alias = line(&[(530.0, -10.0), (550.0, 10.0), (530.0, 10.0), (550.0, -10.0)]);
    assert_eq!(
        crate::geometry::self_intersections_in_frame(&crossing_alias, domain).unwrap(),
        [Point::new_raw_xy(-180.0, 0.0)]
    );
    let prepared = crate::geometry::PreparedPlan::transient(&crossing);
    assert!(!crate::geometry::is_simple_data_in_frame(&prepared, domain).unwrap());

    let stutter = line(&[(170.0, 0.0), (170.0, 0.0), (190.0, 10.0)]);
    assert!(
        crate::geometry::self_intersections_in_frame(&stutter, domain)
            .unwrap()
            .is_empty()
    );
    let prepared = crate::geometry::PreparedPlan::transient(&stutter);
    assert!(crate::geometry::is_simple_data_in_frame(&prepared, domain).unwrap());

    let endpoint_touch = multi_line(&[&[(170.0, 0.0), (180.0, 0.0)], &[
        (180.0, 0.0),
        (180.0, 10.0),
    ]]);
    assert!(
        crate::geometry::self_intersections_in_frame(&endpoint_touch, domain)
            .unwrap()
            .is_empty()
    );
    let prepared = crate::geometry::PreparedPlan::transient(&endpoint_touch);
    assert!(crate::geometry::is_simple_data_in_frame(&prepared, domain).unwrap());

    let interior_crossing = multi_line(&[&[(170.0, -10.0), (190.0, 10.0)], &[
        (170.0, 10.0),
        (190.0, -10.0),
    ]]);
    assert_eq!(
        crate::geometry::self_intersections_in_frame(&interior_crossing, domain).unwrap(),
        [Point::new_raw_xy(-180.0, 0.0)]
    );
    let prepared = crate::geometry::PreparedPlan::transient(&interior_crossing);
    assert!(!crate::geometry::is_simple_data_in_frame(&prepared, domain).unwrap());

    let quotient_ring = line(&[
        (170.0, -10.0),
        (190.0, -10.0),
        (190.0, 10.0),
        (170.0, 10.0),
        (150.0, 0.0),
        (-190.0, -10.0),
    ]);
    let prepared = crate::geometry::PreparedPlan::transient(&quotient_ring);
    assert!(crate::geometry::is_ring_data_in_frame(&prepared, domain).unwrap());

    let closed_multiline =
        multi_line(&[&[(170.0, 0.0), (190.0, 0.0), (180.0, 10.0), (170.0, 0.0)]]);
    let prepared = crate::geometry::PreparedPlan::transient(&closed_multiline);
    assert!(!crate::geometry::is_ring_data_in_frame(&prepared, domain).unwrap());
}

#[test]
fn canonical_precedence_orders_members_then_issue_class() {
    let shell = box_ring(0.0, 20.0, 0.0, 20.0);
    let outside_hole = box_ring(25.0, 30.0, 5.0, 10.0);
    let outer_hole = box_ring(2.0, 12.0, 2.0, 12.0);
    let inner_hole = box_ring(4.0, 8.0, 4.0, 8.0);
    let polygon_with_two_defects =
        Shape::Polygon(polygon(&[shell, outside_hole, outer_hole, inner_hole]));
    let issue = signature(&polygon_with_two_defects);
    assert_eq!(issue.code, Some("hole_outside_shell"));
    assert_eq!(issue.path.as_deref(), Some("$"));

    let bowtie = vec![
        (0.0, 0.0),
        (10.0, 10.0),
        (10.0, 0.0),
        (0.0, 10.0),
        (0.0, 0.0),
    ];
    let members = multi_polygon(&[vec![bowtie], vec![box_ring(40.0, 50.0, 0.0, 10.0)], vec![
        box_ring(45.0, 55.0, 5.0, 15.0),
    ]]);
    let issue = signature(&members);
    assert_eq!(issue.code, Some("self_intersection"));
    assert_eq!(issue.path.as_deref(), Some("$[0]"));
}

#[test]
fn geographic_q_validity_uses_xy_without_losing_zm_storage() {
    let points = [
        (170.0, -10.0),
        (190.0, 10.0),
        (170.0, 10.0),
        (190.0, -10.0),
        (170.0, -10.0),
    ];
    let xy = Shape::Polygon(Polygon::new(ring(&points), Vec::new()));
    let zm_points = points
        .iter()
        .enumerate()
        .map(|(index, &(x, y))| {
            Point::new_raw_axes(
                x,
                y,
                ZOrdinate(Some(index as f64)),
                MOrdinate(Some((index as f64) * 10.0)),
            )
        })
        .collect::<Vec<_>>();
    let zm_ring = Ring::from_closed(CoordSeq::try_from_points(&zm_points).unwrap()).unwrap();
    let zm = Shape::Polygon(Polygon::new(zm_ring, Vec::new()));
    assert_eq!(signature(&xy), signature(&zm));
}

#[rstest::rstest]
#[case::simple_shell(Shape::Polygon(polygon(&[box_ring(0.0, 10.0, 0.0, 10.0)])))]
#[case::crossed_shell(Shape::Polygon(Polygon::new(
    ring(&[
        (0.0, 0.0),
        (10.0, 10.0),
        (10.0, 0.0),
        (0.0, 10.0),
        (0.0, 0.0),
    ]),
    Vec::new(),
)))]
#[case::external_hole(Shape::Polygon(polygon(&[
    box_ring(0.0, 20.0, 0.0, 20.0),
    box_ring(25.0, 30.0, 5.0, 10.0),
])))]
fn exact_partition_route_matches_planar_validity_codes_paths_and_witnesses(#[case] shape: Shape) {
    let frame = frame();
    let view = crate::geographic::ScalarGeographicView::try_new(frame, &shape).unwrap();
    assert_eq!(
        crate::geometry::classify_geographic_chart(view),
        crate::geometry::GeographicTopologyRoute::Cartesian
    );
    let topology = ExactGeographicTopology::from_views(frame, std::iter::once(view)).unwrap();
    assert_eq!(
        signature_from_issue(topology.validity_issue(&shape).unwrap()),
        signature_from_issue(shape.validate().unwrap()),
        "{shape:?}"
    );
}
