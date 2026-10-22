use super::test_support::{
    assert_disjoint_certificate_matches_graph, invalid_zm_bowtie, m_square, polygon, square, wkb,
    z_square, zm_line, zm_square,
};
use crate::geometry::overlay::exact_partition::{partition_metrics, reset_partition_builds};
use crate::geometry::overlay::shape::{
    line_merge_source_candidates, reset_line_merge_source_candidates,
};
use crate::geometry::overlay::{OverlayOp, Strictness, binary_areal_overlay, overlay_all};
use crate::geometry::{
    CoordSeq, CoordinateAxes, Coordinates as _, GeometryKind, LineSeq, MOrdinate, MultiLineString,
    MultiMember, Point, Polygon, Shape, TopologyDomain, ZOrdinate,
};

/// The overlay engine preserves INPUT VERTICES bit-exactly — the wart
/// the `i_overlay` path had (it snapped every coordinate to its grid,
/// perturbing untouched vertices in the 8th decimal).
#[test]
fn overlay_preserves_input_vertices_bit_exactly() {
    let left = polygon(&[
        (0.123_456_789_012_345_6, 0.987_654_321_098_765),
        (10.765_432_109_876_5, 0.123_456_789),
        (5.555_555_555_555_5, 9.999_999_999_99),
        (0.123_456_789_012_345_6, 0.987_654_321_098_765),
    ]);
    let right = polygon(&[
        (3.333_333_333_333_3, -1.111_111_111_111),
        (12.222_222_222_222_2, 4.444_444_444_444),
        (4.040_404_040_404, 8.080_808_080_808),
        (3.333_333_333_333_3, -1.111_111_111_111),
    ]);
    let union = binary_areal_overlay(
        std::slice::from_ref(&left),
        std::slice::from_ref(&right),
        OverlayOp::Union,
    )
    .unwrap();
    let output: Vec<Point> = union
        .iter()
        .flat_map(Polygon::rings)
        .flat_map(|ring| ring.iter_coords().collect::<Vec<_>>())
        .collect();
    for original in left.shell.coords().iter_coords() {
        assert!(
            output
                .iter()
                .any(|point| point.x.to_bits() == original.x.to_bits()
                    && point.y.to_bits() == original.y.to_bits()),
            "vertex {original:?} not preserved"
        );
    }
}

#[rstest::rstest]
#[case::xyz(
    "XYZ",
    z_square(0.0, 0.0, 1.0, 1.0, 10.0),
    z_square(3.0, 3.0, 4.0, 4.0, 20.0)
)]
#[case::xym(
    "XYM",
    m_square(0.0, 0.0, 1.0, 1.0, 10.0),
    m_square(3.0, 3.0, 4.0, 4.0, 20.0)
)]
#[case::xyzm(
    "XYZM",
    zm_square(0.0, 0.0, 1.0, 1.0, 10.0),
    zm_square(3.0, 3.0, 4.0, 4.0, 20.0)
)]
fn disjoint_ordinate_axes_match_forced_graph_wkb(
    #[case] axes: &'static str,
    #[case] left: Shape,
    #[case] right: Shape,
) {
    assert_eq!(left.coordinate_axes(), axes);
    assert_eq!(right.coordinate_axes(), axes);
    for op in [OverlayOp::Union, OverlayOp::SymmetricDifference] {
        assert_disjoint_certificate_matches_graph(&[&left, &right], op);
    }
}

#[rstest::rstest]
#[case::overlayop_union(OverlayOp::Union)]
#[case::overlayop_symmetricdifference(OverlayOp::SymmetricDifference)]
fn disjoint_invalid_zm_area_and_line_use_the_same_ordinate_path(#[case] op: OverlayOp) {
    let polygon = invalid_zm_bowtie();
    let line = zm_line((10.0, 0.0), (11.0, 1.0));

    let certified = overlay_all(
        &[&polygon, &line],
        crate::geometry::TopologyDomain::Cartesian,
        op,
        Strictness::Strict,
    )
    .expect("certified mixed ZM overlay");
    let forced = super::super::nary::overlay_all_without_bounds_certificates(
        &[&polygon, &line],
        crate::geometry::TopologyDomain::Cartesian,
        op,
        Strictness::Strict,
    )
    .expect("forced mixed ZM overlay");
    assert_eq!(wkb(&certified), wkb(&forced), "{}", op.name());
}

#[rstest::rstest]
#[case::overlayop_union(OverlayOp::Union)]
#[case::overlayop_symmetricdifference(OverlayOp::SymmetricDifference)]
fn empty_ordinate_disjoint_paths_return_the_same_error(#[case] op: OverlayOp) {
    let polygon = square(0.0, 0.0, 1.0, 1.0);
    let empty_z = Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XYZ);
    let remote = square(3.0, 3.0, 4.0, 4.0);

    let certified = overlay_all(
        &[&polygon, &empty_z, &remote],
        crate::geometry::TopologyDomain::Cartesian,
        op,
        Strictness::Strict,
    );
    let forced = super::super::nary::overlay_all_without_bounds_certificates(
        &[&polygon, &empty_z, &remote],
        crate::geometry::TopologyDomain::Cartesian,
        op,
        Strictness::Strict,
    );
    let (certified, forced) = (certified.unwrap_err(), forced.unwrap_err());
    assert_eq!(certified.to_string(), forced.to_string(), "{}", op.name());
    assert!(certified.to_string().contains("cannot represent Z/M"));
}

#[test]
fn symmetric_difference_all_carries_resolvable_ordinates() {
    let first = zm_square(0.0, 0.0, 1.0, 1.0, 10.0);
    let second = zm_square(3.0, 3.0, 4.0, 4.0, 20.0);
    // Disjoint symdiff is faithful — both policies carry Z/M.
    for strictness in [Strictness::Lenient, Strictness::Strict] {
        let kept = overlay_all(
            &[&first, &second],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            strictness,
        )
        .unwrap();
        assert!(kept.has_z() && kept.has_m(), "{kept:?}");
    }
    // Overlapping symdiff: seam crossings lie on the uniform-Z/M edges, so
    // they resolve and Z/M carries through under 'auto'.
    let overlap = zm_square(0.5, 0.5, 1.5, 1.5, 10.0);
    let carried = overlay_all(
        &[&first, &overlap],
        crate::geometry::TopologyDomain::Cartesian,
        OverlayOp::SymmetricDifference,
        Strictness::Lenient,
    )
    .unwrap();
    assert!(carried.has_z() && carried.has_m(), "{carried:?}");
}

#[test]
fn overlay_resolves_conflicting_z_and_equal_m_independently() {
    let point = |z, m| Point::new_axes(0.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap();
    let left = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                point(1.0, 7.0),
                Point::new_axes(2.0, 0.0, ZOrdinate(Some(3.0)), MOrdinate(Some(9.0))).unwrap(),
            ])
            .unwrap(),
        )
        .expect("line coordinate sequence must contain at least two vertices"),
    );
    let right = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                point(11.0, 7.0),
                Point::new_axes(2.0, 0.0, ZOrdinate(Some(13.0)), MOrdinate(Some(9.0))).unwrap(),
            ])
            .unwrap(),
        )
        .expect("line coordinate sequence must contain at least two vertices"),
    );

    let result = left.union(&right, Strictness::Lenient).unwrap();
    assert_eq!(result.coordinate_axes(), "XYM");
    result.for_each_point(|point| {
        assert_eq!(point.m(), Some(if point.x == 0.0 { 7.0 } else { 9.0 }));
        assert!(point.z().is_none());
    });
}

#[test]
fn overlay_preserves_signed_zero_and_drops_absent_difference_axis() {
    let left = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_axes(0.0, 0.0, ZOrdinate(Some(-0.0)), MOrdinate(None)).unwrap(),
                Point::new_axes(4.0, 0.0, ZOrdinate(Some(4.0)), MOrdinate(None)).unwrap(),
            ])
            .unwrap(),
        )
        .expect("line coordinate sequence must contain at least two vertices"),
    );
    let xy = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new(0.0, 0.0).unwrap(),
                Point::new(4.0, 0.0).unwrap(),
            ])
            .unwrap(),
        )
        .expect("line coordinate sequence must contain at least two vertices"),
    );
    let intersection = left.intersection(&xy, Strictness::Lenient).unwrap();
    assert_eq!(intersection.coordinate_axes(), "XYZ");
    let first = intersection
        .points_vec()
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(first.z().unwrap().to_bits(), (-0.0_f64).to_bits());
    assert!(first.m().is_none());

    let measured = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_axes(2.0, -1.0, ZOrdinate(None), MOrdinate(Some(2.0))).unwrap(),
                Point::new_axes(2.0, 1.0, ZOrdinate(None), MOrdinate(Some(4.0))).unwrap(),
            ])
            .unwrap(),
        )
        .expect("line coordinate sequence must contain at least two vertices"),
    );
    let difference = left.difference(&measured, Strictness::Lenient).unwrap();
    assert_eq!(difference.coordinate_axes(), "XYZ");
    difference.for_each_point(|point| assert!(point.m().is_none()));
}

#[test]
fn empty_overlay_declares_union_axes_without_coordinates() {
    let z = z_square(0.0, 0.0, 1.0, 1.0, 1.0);
    let m = zm_square(2.0, 2.0, 3.0, 3.0, 1.0);
    let result = z.intersection(&m, Strictness::Lenient).unwrap();
    assert!(matches!(
        result,
        Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XYZM)
    ));
}

#[rstest::rstest]
#[case::overlayop_union(OverlayOp::Union)]
#[case::overlayop_intersection(OverlayOp::Intersection)]
#[case::overlayop_symmetricdifference(OverlayOp::SymmetricDifference)]
fn commutative_overlay_operand_reversal_keeps_axes_and_ordinates(#[case] op: OverlayOp) {
    let left = zm_square(0.0, 0.0, 2.0, 2.0, 10.0);
    let right = zm_square(1.0, 1.0, 3.0, 3.0, 10.0);
    let signature = |shape: &Shape| {
        let mut points = shape
            .points_vec()
            .unwrap()
            .into_iter()
            .map(|point| {
                (
                    point.x.to_bits(),
                    point.y.to_bits(),
                    point.z().map(f64::to_bits),
                    point.m().map(f64::to_bits),
                )
            })
            .collect::<Vec<_>>();
        points.sort_unstable();
        points.dedup();
        (shape.coordinate_axes(), points)
    };

    let forward = op.apply(&left, &right, Strictness::Lenient).unwrap();
    let reversed = op.apply(&right, &left, Strictness::Lenient).unwrap();
    assert_eq!(signature(&forward), signature(&reversed), "{}", op.name());
}

#[test]
fn line_merge_drops_only_conflicting_shared_node_axes() {
    let line = |z, m, end| {
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_axes(0.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
                Point::new_axes(end, 0.0, ZOrdinate(Some(z + 1.0)), MOrdinate(Some(m + 1.0)))
                    .unwrap(),
            ])
            .unwrap(),
        )
        .expect("line coordinate sequence must contain at least two vertices")
    };
    let lines = MultiLineString::try_new(CoordinateAxes::XYZM, vec![
        MultiMember::Value(line(1.0, 10.0, 1.0)),
        MultiMember::Value(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_axes(1.0, 0.0, ZOrdinate(Some(99.0)), MOrdinate(Some(11.0)))
                        .unwrap(),
                    Point::new_axes(2.0, 0.0, ZOrdinate(Some(100.0)), MOrdinate(Some(12.0)))
                        .unwrap(),
                ])
                .unwrap(),
            )
            .expect("fixture line has two vertices"),
        ),
    ])
    .unwrap();
    let merged = Shape::MultiLineString(lines).line_merge().unwrap();
    assert_eq!(merged.coordinate_axes(), "XYM");
    merged.for_each_point(|point| assert!(point.z().is_none()));
    merged.for_each_point(|point| assert!(point.m().is_some()));
}

#[test]
fn geographic_line_merge_does_not_node_disconnected_crossings() {
    let axes = CoordinateAxes::XYZ;
    let input = geographic_line_merge_crossing_grid(axes);
    let context = crate::geometry::SourceArcContext::new(
        TopologyDomain::Geographic(crate::geographic::GeographicFrame::crs84()),
        None,
    );

    reset_partition_builds();
    reset_line_merge_source_candidates();
    let output = input
        .line_merge_with_context(&context, false)
        .expect("geographic XYZ line merge succeeds");
    let Shape::MultiLineString(merged) = output else {
        panic!("endpoint-disconnected lines remain a MultiLineString");
    };
    assert_eq!(merged.len(), 32);
    assert!(merged.iter().all(|line| line.len() == 2));
    assert_eq!(merged.axes(), axes);
    let metrics = partition_metrics();
    let source_candidates = line_merge_source_candidates();

    assert!(
        metrics.canonical_nodes <= 64,
        "line merge materialized irrelevant interior crossings: {} nodes for {} endpoint-only lines",
        metrics.canonical_nodes,
        32
    );
    assert!(
        source_candidates <= 64,
        "line merge tested too many source incidences: {source_candidates} candidates for 16 horizontal and vertical pairs"
    );
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "the fixed 16-row test grid bounds its coordinate and Z labels"
)]
fn geographic_line_merge_crossing_grid(axes: CoordinateAxes) -> Shape {
    const COUNT: usize = 16;

    let mut lines = Vec::with_capacity(32);
    for index in 0..COUNT {
        let coordinate = (index + 1) as f64 * 10.0 / (COUNT + 1) as f64;
        for (line, ((start_x, start_y), (end_x, end_y))) in [
            ((0.0, coordinate), (10.0, coordinate)),
            ((coordinate, 0.0), (coordinate, 10.0)),
        ]
        .into_iter()
        .enumerate()
        {
            let start = Point::new_axes(
                start_x,
                start_y,
                ZOrdinate(axes.has_z().then_some((line + index * 2) as f64)),
                MOrdinate(None),
            )
            .unwrap();
            let end = Point::new_axes(
                end_x,
                end_y,
                ZOrdinate(axes.has_z().then_some((line + index * 2 + 1) as f64)),
                MOrdinate(None),
            )
            .unwrap();
            lines.push(MultiMember::Value(
                LineSeq::try_new(CoordSeq::try_from_points(&[start, end]).unwrap()).unwrap(),
            ));
        }
    }
    Shape::MultiLineString(
        MultiLineString::try_new(axes, lines).expect("crossing linework has valid members"),
    )
}
