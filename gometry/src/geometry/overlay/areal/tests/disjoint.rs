use super::super::test_support::test_ring;
use super::test_support::{
    assert_disjoint_certificate_matches_graph, assert_disjoint_results_match, line, point, polygon,
    regular_polygon, square, wkb,
};
use crate::geometry::overlay::{OverlayOp, Strictness, overlay_all};
use crate::geometry::{CoordSeq, CoordinateAxes, GeometryKind, LineSeq, Point, Polygon, Shape};

#[rstest::rstest]
#[case::overlayop_union(OverlayOp::Union)]
#[case::overlayop_difference(OverlayOp::Difference)]
#[case::overlayop_symmetricdifference(OverlayOp::SymmetricDifference)]
fn disjoint_bounds_certificates_match_joint_graph_assembly(#[case] op: OverlayOp) {
    let polygons = [square(0.0, 0.0, 1.0, 1.0), square(3.0, 3.0, 4.0, 4.0)];
    let mixed = [
        square(0.0, 0.0, 1.0, 1.0),
        line(&[(3.0, 3.0), (4.0, 3.0)]),
        point(6.0, 6.0),
    ];
    let multipart = Shape::geometry_collection(vec![
        Shape::MultiPolygon(
            vec![polygon(&[
                (0.0, 0.0),
                (1.0, 0.0),
                (1.0, 1.0),
                (0.0, 1.0),
                (0.0, 0.0),
            ])]
            .into(),
        ),
        Shape::MultiLineString(
            vec![
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new(2.0, 2.0).unwrap(),
                        Point::new(3.0, 2.0).unwrap(),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            ]
            .into(),
        ),
    ])
    .expect("test collection is valid");
    let remote = square(6.0, 6.0, 7.0, 7.0);
    let empty_z = Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XYZ);

    assert_disjoint_certificate_matches_graph(&[&polygons[0], &polygons[1]], op);
    assert_disjoint_certificate_matches_graph(&[&mixed[0], &mixed[1], &mixed[2]], op);
    assert_disjoint_certificate_matches_graph(&[&multipart, &remote], op);
    assert_disjoint_results_match(&[&polygons[0], &empty_z, &polygons[1]], op);
}

/// Three bounds components: an overlapping pair, an invalid bowtie with a
/// square across it, and a remote square. Each component is partitioned
/// alone, a difference keeps only its left operand's component, and an
/// intersection whose bounds share no box builds nothing.
#[test]
fn bounds_components_partition_alone_and_match_one_arrangement() {
    use crate::geometry::overlay::exact_partition::{partition_builds, reset_partition_builds};
    let first = square(0.0, 0.0, 2.0, 2.0);
    let second = square(1.0, 1.0, 3.0, 3.0);
    let bowtie = Shape::Polygon(polygon(&[
        (10.0, 10.0),
        (12.0, 12.0),
        (10.0, 12.0),
        (12.0, 10.0),
        (10.0, 10.0),
    ]));
    let across = square(11.0, 11.0, 13.0, 13.0);
    let remote = square(20.0, 20.0, 21.0, 21.0);
    let inputs = [&first, &second, &bowtie, &across, &remote];
    for (op, builds) in [
        (OverlayOp::Union, 3),
        (OverlayOp::SymmetricDifference, 3),
        (OverlayOp::Difference, 1),
        (OverlayOp::Intersection, 0),
    ] {
        reset_partition_builds();
        let certified = overlay_all(
            &inputs,
            crate::geometry::TopologyDomain::Cartesian,
            op,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(partition_builds(), builds, "{}", op.name());
        let joint = super::super::nary::overlay_all_without_bounds_certificates(
            &inputs,
            crate::geometry::TopologyDomain::Cartesian,
            op,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(wkb(&certified), wkb(&joint), "{}", op.name());
    }
}

/// Emission checks run where rounding moved a crossing, and nested
/// operands, whose emitted nodes are all stored, skip them.
#[test]
fn emission_checks_run_only_where_rounding_moved_a_crossing() {
    use crate::geometry::overlay::exact_partition::{partition_metrics, reset_partition_builds};
    let outer = Shape::Polygon(Polygon::new(
        test_ring(&[
            (0.0, 0.0),
            (2.0, 0.0),
            (4.0, 0.0),
            (4.0, 4.0),
            (0.0, 4.0),
            (0.0, 0.0),
        ]),
        Vec::new(),
    ));
    let inner = Shape::Polygon(Polygon::new(
        test_ring(&[
            (1.0, 1.0),
            (1.5, 1.0),
            (2.0, 1.0),
            (2.0, 2.0),
            (1.0, 2.0),
            (1.0, 1.0),
        ]),
        Vec::new(),
    ));
    for (right, moved) in [(inner, false), (regular_polygon(4.0, 4.0, 1.5, 7), true)] {
        reset_partition_builds();
        overlay_all(
            &[&outer, &right],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(partition_metrics().unmoved_emissions == 0, moved);
    }
}
