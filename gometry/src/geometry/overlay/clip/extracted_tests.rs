use super::clip;
use super::test_support::shape_wkb;
use crate::error::Result;
use crate::geometry::overlay::{
    OverlayOp, Strictness, binary_areal_overlay, overlay_all, rect_polygon,
};
use crate::geometry::segments::clip::clip_segment_rect_interval;
use crate::geometry::tessellation::exact::compare_ratios;
use crate::geometry::{
    Bounds, CoordSeq, CoordinateAxes, LineSeq, MOrdinate, Point, Polygon, Ring, Segment, Shape, XY,
    ZOrdinate, carry_ordinates,
};

fn bounds(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Bounds {
    Bounds::new(minx, miny, maxx, maxy).expect("finite ordered rectangle")
}

fn ring(points: &[(f64, f64)]) -> Ring {
    Ring::from_closed(
        CoordSeq::try_from_points(
            &points
                .iter()
                .map(|&(x, y)| Point::new_raw_xy(x, y))
                .collect::<Vec<_>>(),
        )
        .expect("test coordinates form a sequence"),
    )
    .expect("test coordinates form a closed ring")
}

fn polygon(points: &[(f64, f64)]) -> Polygon {
    Polygon::new(ring(points), Vec::new())
}

fn rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Polygon {
    polygon(&[
        (minx, miny),
        (maxx, miny),
        (maxx, maxy),
        (minx, maxy),
        (minx, miny),
    ])
}

fn compare_clip_results(
    name: &str,
    actual: Result<Shape>,
    expected: Result<Shape>,
    matching_errors_are_expected: bool,
) {
    match (actual, expected) {
        (Ok(actual), Ok(expected)) => {
            assert_eq!(
                shape_wkb(&actual),
                shape_wkb(&expected),
                "{name}: rectangle intersection {actual:?} differs from full partition {expected:?}"
            );
            assert!(
                actual.validate().unwrap().is_none(),
                "{name}: invalid rectangle clip output {actual:?}"
            );
        },
        (Err(actual), Err(expected)) if matching_errors_are_expected => {
            assert_eq!(
                actual.to_string(),
                expected.to_string(),
                "{name}: local partition and exact overlay disagree on failure"
            );
        },
        (actual, expected) => panic!(
            "{name}: local partition returned {actual:?}, exact overlay returned {expected:?}"
        ),
    }
}

fn assert_clip_matches_oracle(name: &str, subjects: &[Polygon], bounds: Bounds) {
    let subject = Shape::narrow_polygons(CoordinateAxes::XY, subjects.to_vec());
    let rectangle_polygon = rect_polygon(bounds).expect("clip rectangle polygon");
    let rectangle = Shape::Polygon(rectangle_polygon.clone());
    let expected = super::nary::full_partition_cartesian_overlay(
        &[&subject, &rectangle],
        OverlayOp::Intersection,
        Strictness::Lenient,
    );
    let expected = expected.unwrap();
    let actual = super::intersect_rectangle(
        &subject,
        &rectangle_polygon,
        bounds,
        crate::geometry::compare_shapes(&rectangle, &subject).is_lt(),
        Strictness::Lenient,
    )
    .unwrap()
    .unwrap_or_else(|| expected.clone());
    compare_clip_results(name, Ok(actual), Ok(expected), false);
}

fn assert_clip_oracle_failure_or_equal(name: &str, subjects: &[Polygon], bounds: Bounds) {
    let subject = Shape::narrow_polygons(CoordinateAxes::XY, subjects.to_vec());
    let rectangle = Shape::Polygon(rect_polygon(bounds).expect("clip rectangle polygon"));
    let inputs = [&subject, &rectangle];
    compare_clip_results(
        name,
        overlay_all(
            &inputs,
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Lenient,
        ),
        super::nary::full_partition_cartesian_overlay(
            &inputs,
            OverlayOp::Intersection,
            Strictness::Lenient,
        ),
        true,
    );
}

fn assert_public_clip_matches_intersection(name: &str, source_wkt: &str, rect: Bounds) -> Shape {
    let subject = crate::io::parse_wkt(source_wkt).expect("test geometry parses");
    let rectangle = Shape::Polygon(rect_polygon(rect).expect("clip rectangle polygon"));
    let actual = subject.clip_by_rect(rect, false);
    let expected = subject.intersection(&rectangle, Strictness::Lenient);
    match (actual, expected) {
        (Ok(actual), Ok(expected)) => {
            assert_eq!(
                shape_wkb(&actual),
                shape_wkb(&expected),
                "{name}: clip {actual:?} differs from intersection {expected:?}"
            );
            actual
        },
        (Err(actual), Err(expected)) => {
            assert_eq!(
                actual.to_string(),
                expected.to_string(),
                "{name}: clip and intersection failed differently"
            );
            panic!("{name}: enumerated input must be accepted")
        },
        (actual, expected) => {
            panic!("{name}: clip returned {actual:?}, intersection returned {expected:?}")
        },
    }
}

#[rstest::rstest]
#[case::crossing_at_window_end(
    "crossing-at-window-end",
    "MULTILINESTRING ((-1 -1,1 1),(-1 1,1 -1))",
    bounds(0.0, 0.0, 1.0, 1.0),
    "LINESTRING (0 0,1 1)"
)]
#[case::point_at_constructed_end(
    "point-at-constructed-end",
    "GEOMETRYCOLLECTION (LINESTRING (-1 -1,2 2),POINT (0 0),POINT (0 1),POINT (2 0))",
    bounds(0.0, 0.0, 1.0, 1.0),
    "GEOMETRYCOLLECTION (POINT (0 1),LINESTRING (0 0,1 1))"
)]
#[case::overlapping_carriers(
    "overlapping-carriers",
    "MULTILINESTRING ((-1 0.5,2 0.5),(0.25 0.5,3 0.5))",
    bounds(0.0, 0.0, 1.0, 1.0),
    "LINESTRING (0 0.5,0.25 0.5,1 0.5)"
)]
#[case::interior_crossing(
    "interior-crossing",
    "MULTILINESTRING ((-1 -1,2 2),(-1 2,2 -1))",
    bounds(0.0, 0.0, 1.0, 1.0),
    "MULTILINESTRING ((0 0,0.5 0.5),(0 1,0.5 0.5),(0.5 0.5,1 0),(0.5 0.5,1 1))"
)]
#[case::point_window(
    "point-window",
    "LINESTRING (-1 -1,2 2)",
    bounds(0.0, 0.0, 0.0, 0.0),
    "POINT (0 0)"
)]
#[case::zero_length_source(
    "zero-length-source",
    "MULTILINESTRING ((0 0,0 0),(-1 1,-1 1))",
    bounds(0.0, 0.0, 1.0, 1.0),
    "POINT (0 0)"
)]
#[case::zm_window_parameters(
    "zm-window-parameters",
    "LINESTRING ZM (-1 0.5 0 2,2 0.5 9 11)",
    bounds(0.0, 0.0, 1.0, 1.0),
    "LINESTRING ZM (0 0.5 3 5,1 0.5 6 8)"
)]
fn rectangle_linework_preserves_exact_contacts_and_ordinates(
    #[case] name: &'static str,
    #[case] source: &'static str,
    #[case] rect: Bounds,
    #[case] expected: &'static str,
) {
    let actual = assert_public_clip_matches_intersection(name, source, rect);
    let expected = crate::io::parse_wkt(expected).unwrap().normalize().unwrap();
    assert_eq!(
        shape_wkb(&actual),
        shape_wkb(&expected),
        "{name}: {actual:?}"
    );
}

#[rstest::rstest]
#[case::crossing_bottom_edge(
    "crossing bottom edge",
    "LINESTRING (-1 0, 2 0, 5 0)",
    "LINESTRING (0 0, 4 0)"
)]
#[case::contained_bottom_edge(
    "contained bottom edge",
    "LINESTRING (0 0, 2 0, 4 0)",
    "LINESTRING (0 0, 4 0)"
)]
#[case::crossing_top_edge(
    "crossing top edge",
    "LINESTRING (-1 4, 2 4, 5 4)",
    "LINESTRING (0 4, 4 4)"
)]
#[case::contained_left_edge(
    "contained left edge",
    "LINESTRING (0 0, 0 2, 0 4)",
    "LINESTRING (0 0, 0 4)"
)]
#[case::crossing_right_edge(
    "crossing right edge",
    "LINESTRING (4 -1, 4 2, 4 5)",
    "LINESTRING (4 0, 4 4)"
)]
#[case::interior_vertices_remain(
    "interior vertices remain",
    "LINESTRING (1 1, 2 1, 3 1)",
    "LINESTRING (1 1, 2 1, 3 1)"
)]
#[case::boundary_corner_remains(
    "boundary corner remains",
    "LINESTRING (0 1, 0 2, 1 2)",
    "LINESTRING (0 1, 0 2, 1 2)"
)]
#[case::branch_node_remains(
    "branch node remains",
    "MULTILINESTRING ((-1 0, 2 0, 5 0), (2 0, 2 2))",
    "MULTILINESTRING ((0 0, 2 0), (2 0, 2 2), (2 0, 4 0))"
)]
#[case::explicit_point_node_remains(
    "explicit point node remains",
    "GEOMETRYCOLLECTION (LINESTRING (-1 0, 2 0, 5 0), POINT (2 0))",
    "LINESTRING (0 0, 2 0, 4 0)"
)]
fn rectangle_line_emission_matches_ring_vertex_dissolution(
    #[case] name: &'static str,
    #[case] source: &'static str,
    #[case] expected: &'static str,
) {
    let rect = bounds(0.0, 0.0, 4.0, 4.0);
    let rectangle = Shape::Polygon(rect_polygon(rect).unwrap());

    let subject = crate::io::parse_wkt(source).unwrap();
    let expected = crate::io::parse_wkt(expected).unwrap().normalize().unwrap();
    let full = super::nary::full_partition_cartesian_overlay(
        &[&subject, &rectangle],
        OverlayOp::Intersection,
        Strictness::Lenient,
    )
    .unwrap();
    assert_eq!(shape_wkb(&full), shape_wkb(&expected), "{name}");
    for actual in [
        subject.clip_by_rect(rect, false).unwrap(),
        subject
            .intersection(&rectangle, Strictness::Lenient)
            .unwrap(),
    ] {
        assert_eq!(shape_wkb(&actual), shape_wkb(&expected), "{name}");
    }
}

#[test]
fn clip_by_rect_matches_closed_box_intersection_on_contacts_and_degeneracies() {
    let rect = bounds(1.0, 1.0, 4.0, 4.0);
    let cases = [
        ("line-point-contact", "LINESTRING (5 3, 4 1)", rect),
        ("line-edge-contact", "LINESTRING (5 2, 4 2)", rect),
        ("line-corner-contact", "LINESTRING (5 5, 4 4)", rect),
        ("line-runs-along-edge", "LINESTRING (1 1, 4 1)", rect),
        (
            "multiline-mixed-contact",
            "MULTILINESTRING ((5 3, 4 1), (1 1, 4 1))",
            rect,
        ),
        (
            "multipolygon-point-contact-with-area",
            "MULTIPOLYGON (((1 1, 2 1, 2 2, 1 2, 1 1)), ((4 4, 6 4, 6 6, 4 6, 4 4)))",
            rect,
        ),
        (
            "multipolygon-edge-contact-with-area",
            "MULTIPOLYGON (((1 1, 2 1, 2 2, 1 2, 1 1)), ((4 2, 6 2, 6 3, 4 3, 4 2)))",
            rect,
        ),
        (
            "polygon-edge-contact",
            "POLYGON ((4 2, 6 2, 6 3, 4 3, 4 2))",
            rect,
        ),
        (
            "polygon-corner-contact",
            "POLYGON ((4 4, 6 4, 6 6, 4 6, 4 4))",
            rect,
        ),
        (
            "polygon-runs-along-edge",
            "POLYGON ((0 1, 4 1, 4 3, 0 3, 0 1))",
            bounds(1.0, 1.0, 3.0, 4.0),
        ),
        (
            "holed-polygon-edge-contact",
            "POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0), (4 2, 5 2, 5 4, 4 4, 4 2))",
            bounds(4.0, 1.0, 6.0, 5.0),
        ),
        (
            "holed-polygon-corner-contact",
            "POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0), (3 3, 5 3, 5 5, 3 5, 3 3))",
            bounds(5.0, 5.0, 7.0, 7.0),
        ),
        (
            "zero-width-line",
            "LINESTRING (0 0, 4 4)",
            bounds(2.0, 1.0, 2.0, 3.0),
        ),
        (
            "zero-height-line",
            "LINESTRING (0 2, 4 2)",
            bounds(1.0, 2.0, 3.0, 2.0),
        ),
        (
            "zero-width-polygon",
            "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))",
            bounds(2.0, 0.0, 2.0, 4.0),
        ),
        (
            "zero-height-polygon",
            "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))",
            bounds(0.0, 2.0, 4.0, 2.0),
        ),
        (
            "spike",
            "POLYGON ((0 0, 4 0, 4 4, 2 4, 2 2, 3 2, 2 2, 2 4, 0 4, 0 0))",
            bounds(0.5, 0.5, 3.5, 3.5),
        ),
        (
            "collapsed-shell",
            "POLYGON ((0 0, 2 2, 4 4, 0 0))",
            bounds(-1.0, 1.0, 5.0, 3.0),
        ),
        (
            "shell-equals-hole",
            "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (0 0, 4 0, 4 4, 0 4, 0 0))",
            bounds(-1.0, -1.0, 5.0, 5.0),
        ),
        (
            "bow-tie",
            "POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))",
            bounds(-1.0, -1.0, 3.0, 1.0),
        ),
        (
            "twice-wound",
            "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0, 4 0, 4 4, 0 4, 0 0))",
            bounds(-1.0, -1.0, 5.0, 5.0),
        ),
        (
            "hole-touching-shell",
            "POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0), (0 2, 2 2, 2 4, 0 4, 0 2))",
            bounds(-1.0, -1.0, 7.0, 7.0),
        ),
        (
            "external-hole",
            "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (5 1, 6 1, 6 2, 5 2, 5 1))",
            bounds(-1.0, -1.0, 7.0, 7.0),
        ),
        ("empty", "POLYGON EMPTY", rect),
        (
            "collapsed-member-multipolygon",
            "MULTIPOLYGON (((0 0, 2 2, 4 4, 0 0)), ((5 0, 7 2, 9 4, 5 0)))",
            bounds(-1.0, -1.0, 10.0, 5.0),
        ),
        (
            "nearest-rounded-line-crossings",
            "LINESTRING (0 5, 5 2)",
            rect,
        ),
    ];
    for (name, source_wkt, rect) in cases {
        assert_public_clip_matches_intersection(name, source_wkt, rect);
    }

    let point_contact = assert_public_clip_matches_intersection(
        "known-line-point-contact",
        "LINESTRING (5 3, 4 1)",
        rect,
    );
    assert_eq!(
        shape_wkb(&point_contact),
        shape_wkb(&crate::io::parse_wkt("POINT (4 1)").unwrap())
    );
    let edge_contact = assert_public_clip_matches_intersection(
        "known-polygon-edge-contact",
        "POLYGON ((4 2, 6 2, 6 3, 4 3, 4 2))",
        rect,
    );
    assert_eq!(
        shape_wkb(&edge_contact),
        shape_wkb(&crate::io::parse_wkt("LINESTRING (4 2, 4 3)").unwrap())
    );
    let corner_contact = assert_public_clip_matches_intersection(
        "known-polygon-corner-contact",
        "POLYGON ((4 4, 6 4, 6 6, 4 6, 4 4))",
        rect,
    );
    assert_eq!(
        shape_wkb(&corner_contact),
        shape_wkb(&crate::io::parse_wkt("POINT (4 4)").unwrap())
    );
    let mixed_point = assert_public_clip_matches_intersection(
        "known-mixed-point-and-area",
        "MULTIPOLYGON (((1 1, 2 1, 2 2, 1 2, 1 1)), ((4 4, 6 4, 6 6, 4 6, 4 4)))",
        rect,
    );
    assert_eq!(
        shape_wkb(&mixed_point),
        shape_wkb(
            &crate::io::parse_wkt(
                "GEOMETRYCOLLECTION (POINT (4 4), POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1)))"
            )
            .unwrap()
        )
    );
    let nearest_crossing = assert_public_clip_matches_intersection(
        "nearest-rounded-crossing",
        "LINESTRING (0 5, 5 2)",
        rect,
    );
    assert_eq!(
        shape_wkb(&nearest_crossing),
        shape_wkb(&crate::io::parse_wkt("LINESTRING (1.6666666666666667 4, 4 2.6)").unwrap())
    );
    let bow_tie = assert_public_clip_matches_intersection(
        "known-bow-tie-lobes",
        "POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))",
        bounds(-1.0, -1.0, 3.0, 1.0),
    );
    assert_eq!(
        shape_wkb(&bow_tie),
        shape_wkb(
            &crate::io::parse_wkt("MULTIPOLYGON (((0 0, 1 1, 0 1, 0 0)), ((1 1, 2 0, 2 1, 1 1)))")
                .unwrap()
        )
    );
}

#[rstest::rstest]
#[case::noncanonical_shell_and_hole(
    "noncanonical-shell-and-hole",
    "POLYGON ((4 0, 2 0, 0 0, 0 4, 4 4, 4 0), (1 1, 1 3, 3 3, 3 1, 1 1))"
)]
#[case::noncanonical_multipolygon_order(
    "noncanonical-multipolygon-order",
    "MULTIPOLYGON (((4 0, 6 0, 6 2, 4 2, 4 0)), ((0 0, 2 0, 2 2, 0 2, 0 0)))"
)]
fn contained_valid_polygonal_intersection_preserves_the_operand_verbatim(
    #[case] name: &'static str,
    #[case] source_wkt: &'static str,
) {
    let bounds = bounds(-1.0, -1.0, 7.0, 5.0);

    let subject = crate::io::parse_wkt(source_wkt).unwrap();
    let expected = shape_wkb(&subject);
    let actual = assert_public_clip_matches_intersection(name, source_wkt, bounds);
    assert_eq!(
        shape_wkb(&actual),
        expected,
        "{name}: retained operand changed"
    );
    let rectangle = Shape::Polygon(rect_polygon(bounds).unwrap());
    let reversed = rectangle
        .intersection(&subject, Strictness::Lenient)
        .unwrap();
    assert_eq!(
        shape_wkb(&reversed),
        expected,
        "{name}: reversed operands changed the retained value"
    );
}

#[test]
fn contained_invalid_polygon_intersection_preserves_the_operand_verbatim() {
    let source_wkt = "POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))";
    let subject = crate::io::parse_wkt(source_wkt).unwrap();
    let expected = shape_wkb(&subject);
    let actual = assert_public_clip_matches_intersection(
        "contained-bow-tie",
        source_wkt,
        bounds(-1.0, -1.0, 3.0, 3.0),
    );
    assert_eq!(shape_wkb(&actual), expected);
}

#[test]
fn contained_zm_polygon_intersection_preserves_the_operand_verbatim() {
    let subject = crate::io::parse_wkt(
        "POLYGON ZM ((0 0 10 100, 2 0 12 120, 4 0 14 140, \
             4 4 18 180, 0 4 20 200, 0 0 10 100))",
    )
    .unwrap();
    let expected = shape_wkb(&subject);
    let bounds = bounds(0.0, 0.0, 4.0, 4.0);
    let rectangle = Shape::Polygon(rect_polygon(bounds).unwrap());

    let clipped = subject.clip_by_rect(bounds, false).unwrap();
    let intersection = subject
        .intersection(&rectangle, Strictness::Lenient)
        .unwrap();

    assert_eq!(shape_wkb(&clipped), expected);
    assert_eq!(shape_wkb(&intersection), expected);
}

#[test]
fn contained_boundary_touching_polygon_intersection_preserves_the_operand() {
    let subject = crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 1, 2 2, 0 2, 0 0))").unwrap();
    let expected = shape_wkb(&subject);
    let bounds = bounds(-1.0, -1.0, 2.0, 3.0);
    let rectangle = Shape::Polygon(rect_polygon(bounds).unwrap());

    let clipped = subject.clip_by_rect(bounds, false).unwrap();
    let intersection = subject
        .intersection(&rectangle, Strictness::Lenient)
        .unwrap();

    assert_eq!(shape_wkb(&clipped), expected);
    assert_eq!(shape_wkb(&intersection), expected);
}

#[test]
fn contained_point_touching_multipolygon_intersection_returns_input_verbatim() {
    let subject = crate::io::parse_wkt(
        "MULTIPOLYGON (((0 0, 2 0, 2 1, 2 2, 0 2, 0 0)), \
             ((2 1, 3 0, 4 1, 3 2, 2 1)))",
    )
    .unwrap();
    assert!(subject.validate().unwrap().is_none());
    let rectangle = bounds(-1.0, -1.0, 5.0, 3.0);
    let clipped = assert_public_clip_matches_intersection(
        "contained-point-touching-members",
        "MULTIPOLYGON (((0 0, 2 0, 2 1, 2 2, 0 2, 0 0)), \
             ((2 1, 3 0, 4 1, 3 2, 2 1)))",
        rectangle,
    );
    assert_eq!(
        shape_wkb(&clipped),
        shape_wkb(&subject),
        "contained touching members retain their original presentation"
    );
}

#[test]
fn contained_rectangle_difference_returns_the_typed_empty() {
    let subject = crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))").unwrap();
    let rectangle = Shape::Polygon(rect_polygon(bounds(-1.0, -1.0, 3.0, 3.0)).unwrap());

    let difference = subject.difference(&rectangle, Strictness::Lenient).unwrap();

    assert_eq!(
        shape_wkb(&difference),
        shape_wkb(&crate::io::parse_wkt("POLYGON EMPTY").unwrap())
    );
}

#[test]
fn rectangle_union_retains_the_covering_rectangle() {
    let subject = crate::io::parse_wkt("POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))").unwrap();
    let rectangle = Shape::Polygon(rect_polygon(bounds(0.0, 0.0, 4.0, 4.0)).unwrap());
    let expected = shape_wkb(&rectangle);

    let union = subject.union(&rectangle, Strictness::Lenient).unwrap();
    let reversed_union = rectangle.union(&subject, Strictness::Lenient).unwrap();

    assert_eq!(shape_wkb(&union), expected);
    assert_eq!(shape_wkb(&reversed_union), expected);
}

#[test]
fn disjoint_rectangle_intersection_returns_the_typed_empty() {
    let subject = crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))").unwrap();
    let rectangle = Shape::Polygon(rect_polygon(bounds(5.0, 5.0, 6.0, 6.0)).unwrap());

    let intersection = subject
        .intersection(&rectangle, Strictness::Lenient)
        .unwrap();
    let reversed = rectangle
        .intersection(&subject, Strictness::Lenient)
        .unwrap();

    assert_eq!(
        shape_wkb(&intersection),
        shape_wkb(&crate::io::parse_wkt("POLYGON EMPTY").unwrap())
    );
    assert_eq!(shape_wkb(&reversed), shape_wkb(&intersection));
}

#[test]
fn clip_by_rect_matches_full_intersection_for_country_source_arcs() {
    use crate::geometry::test_support::argentina_uruguay_source_arc_polygons;

    for (name, subject) in ["Argentina", "Uruguay"]
        .into_iter()
        .zip(argentina_uruguay_source_arc_polygons())
    {
        let window = bounds(-58.3, -33.1, -58.0, -32.9);
        let rectangle = Shape::Polygon(rect_polygon(window).unwrap());
        let clipped = subject.clip_by_rect(window, false).unwrap();
        let intersection = subject
            .intersection(&rectangle, Strictness::Lenient)
            .unwrap();
        let full = super::nary::full_partition_cartesian_overlay(
            &[&subject, &rectangle],
            OverlayOp::Intersection,
            Strictness::Lenient,
        )
        .unwrap();
        assert!(!clipped.is_empty(), "{name}");
        assert_eq!(shape_wkb(&clipped), shape_wkb(&intersection), "{name}");
        assert_eq!(shape_wkb(&clipped), shape_wkb(&full), "{name}");
    }
}

fn regular_ellipse(cx: f64, cy: f64, rx: f64, ry: f64, count: usize, phase: f64) -> Polygon {
    let mut points = Vec::with_capacity(count.wrapping_add(1));
    for index in 0..count {
        let angle = phase + std::f64::consts::TAU * index as f64 / count as f64;
        points.push(Point::new_raw_xy(
            cx + rx * angle.cos(),
            cy + ry * angle.sin(),
        ));
    }
    points.push(points[0]);
    Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        Vec::new(),
    )
}

#[rstest::rstest]
#[case::contained_window("contained-window", vec![rectangle(-4.0, -3.0, 4.0, 3.0)], bounds(-1.0, -1.5, 1.25, 1.5))]
#[case::source_disjoint_from_window("source-disjoint-from-window", vec![rectangle(0.0, 0.0, 1.0, 1.0)], bounds(5.0, 5.0, 6.0, 6.0))]
#[case::source_vertices_on_side_and_corner(
    "source-vertices-on-side-and-corner",
    {
        let square = rectangle(0.0, 0.0, 4.0, 4.0);
        vec![square]
    },
    bounds(0.0, 1.0, 2.0, 4.0)
)]
#[case::coincident_shell_and_window(
    "coincident-shell-and-window",
    {
        let square = rectangle(0.0, 0.0, 4.0, 4.0);
        vec![square]
    },
    bounds(0.0, 0.0, 4.0, 4.0)
)]
#[case::left_side_overlap(
    "left-side-overlap",
    {
        let square = rectangle(0.0, 0.0, 4.0, 4.0);
        vec![square]
    },
    bounds(0.0, 1.0, 2.0, 3.0)
)]
#[case::right_side_overlap(
    "right-side-overlap",
    {
        let square = rectangle(0.0, 0.0, 4.0, 4.0);
        vec![square]
    },
    bounds(2.0, 1.0, 4.0, 3.0)
)]
#[case::bottom_side_overlap(
    "bottom-side-overlap",
    {
        let square = rectangle(0.0, 0.0, 4.0, 4.0);
        vec![square]
    },
    bounds(1.0, 0.0, 3.0, 2.0)
)]
#[case::top_side_overlap(
    "top-side-overlap",
    {
        let square = rectangle(0.0, 0.0, 4.0, 4.0);
        vec![square]
    },
    bounds(1.0, 2.0, 3.0, 4.0)
)]
#[case::corner_only_contact(
    "corner-only-contact",
    {
        let square = rectangle(0.0, 0.0, 4.0, 4.0);
        vec![square]
    },
    bounds(4.0, 4.0, 5.0, 5.0)
)]
#[case::zero_width_window(
    "zero-width-window",
    {
        let square = rectangle(0.0, 0.0, 4.0, 4.0);
        vec![square]
    },
    bounds(2.0, 0.0, 2.0, 4.0)
)]
#[case::zero_height_window(
    "zero-height-window",
    {
        let square = rectangle(0.0, 0.0, 4.0, 4.0);
        vec![square]
    },
    bounds(0.0, 2.0, 4.0, 2.0)
)]
#[case::hole_crossed_by_window(
    "hole-crossed-by-window",
    {
        let shell = ring(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]);
        let hole = ring(&[(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0), (4.0, 4.0)]);
        let holed = Polygon::new(shell, vec![hole]);
        vec![holed]
    },
    bounds(3.0, 3.0, 7.0, 7.0)
)]
#[case::hole_contained_by_window(
    "hole-contained-by-window",
    {
        let shell = ring(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]);
        let hole = ring(&[(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0), (4.0, 4.0)]);
        let holed = Polygon::new(shell, vec![hole]);
        vec![holed]
    },
    bounds(2.0, 2.0, 8.0, 8.0)
)]
#[case::multiple_shells(
    "multiple-shells",
    {
        let square = rectangle(0.0, 0.0, 4.0, 4.0);
        vec![square, rectangle(5.0, 1.0, 9.0, 5.0)]
    },
    bounds(1.0, 1.0, 7.0, 3.0)
)]
#[case::point_touching_parts(
    "point-touching-parts",
    {
        let touching = [rectangle(0.0, 0.0, 2.0, 2.0), rectangle(2.0, 2.0, 4.0, 4.0)];
        touching.to_vec()
    },
    bounds(1.0, 1.0, 3.0, 3.0)
)]
#[case::disconnected_concave_result(
    "disconnected-concave-result",
    {
        let concave = polygon(&[
            (0.0, 0.0),
            (6.0, 0.0),
            (6.0, 6.0),
            (4.0, 6.0),
            (4.0, 2.0),
            (2.0, 2.0),
            (2.0, 6.0),
            (0.0, 6.0),
            (0.0, 0.0),
        ]);
        vec![concave]
    },
    bounds(0.5, 3.0, 5.5, 5.0)
)]
#[case::retraced_spike(
    "retraced-spike",
    {
        let spike = polygon(&[
            (0.0, 0.0),
            (4.0, 0.0),
            (4.0, 4.0),
            (2.0, 4.0),
            (2.0, 2.0),
            (3.0, 2.0),
            (2.0, 2.0),
            (2.0, 4.0),
            (0.0, 4.0),
            (0.0, 0.0),
        ]);
        vec![spike]
    },
    bounds(0.5, 0.5, 3.5, 3.5)
)]
#[case::collapsed_shell("collapsed-shell", vec![polygon(&[(0.0, 0.0), (2.0, 2.0), (4.0, 4.0), (0.0, 0.0)])], bounds(-1.0, 1.0, 5.0, 3.0))]
#[case::shell_equals_hole(
    "shell-equals-hole",
    {
        let square_ring = ring(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
        let shell_equal_hole = Polygon::new(square_ring.clone(), vec![square_ring]);
        vec![shell_equal_hole]
    },
    bounds(-1.0, -1.0, 5.0, 5.0)
)]
#[case::bow_tie(
    "bow-tie",
    vec![polygon(&[
        (0.0, 0.0),
        (2.0, 2.0),
        (2.0, 0.0),
        (0.0, 2.0),
        (0.0, 0.0),
    ])],
    bounds(-1.0, -1.0, 3.0, 1.0)
)]
#[case::twice_wound(
    "twice-wound",
    vec![polygon(&[
        (0.0, 0.0),
        (4.0, 0.0),
        (4.0, 4.0),
        (0.0, 4.0),
        (0.0, 0.0),
        (4.0, 0.0),
        (4.0, 4.0),
        (0.0, 4.0),
        (0.0, 0.0),
    ])],
    bounds(-1.0, -1.0, 5.0, 5.0)
)]
#[case::hole_touching_shell(
    "hole-touching-shell",
    {
        let touching_hole_shell = Polygon::new(
            ring(&[(0.0, 0.0), (6.0, 0.0), (6.0, 6.0), (0.0, 6.0), (0.0, 0.0)]),
            vec![ring(&[
                (0.0, 2.0),
                (2.0, 2.0),
                (2.0, 4.0),
                (0.0, 4.0),
                (0.0, 2.0),
            ])],
        );
        vec![touching_hole_shell]
    },
    bounds(-1.0, -1.0, 7.0, 7.0)
)]
#[case::external_hole(
    "external-hole",
    {
        let square_ring = ring(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
        let external_hole = Polygon::new(square_ring, vec![ring(&[
            (5.0, 1.0),
            (6.0, 1.0),
            (6.0, 2.0),
            (5.0, 2.0),
            (5.0, 1.0),
        ])]);
        vec![external_hole]
    },
    bounds(-1.0, -1.0, 7.0, 7.0)
)]
#[case::empty_polygon("empty-polygon", Vec::new(), bounds(1.0, 1.0, 4.0, 4.0))]
#[case::collapsed_member_multipolygon(
    "collapsed-member-multipolygon",
    vec![
        polygon(&[(0.0, 0.0), (2.0, 2.0), (4.0, 4.0), (0.0, 0.0)]),
        polygon(&[(5.0, 0.0), (7.0, 2.0), (9.0, 4.0), (5.0, 0.0)]),
    ],
    bounds(-1.0, -1.0, 10.0, 5.0)
)]
fn rectangle_local_partition_matches_the_oracle_on_the_full_areal_matrix(
    #[case] name: &str,
    #[case] polygons: Vec<Polygon>,
    #[case] rect: Bounds,
) {
    assert_clip_matches_oracle(name, &polygons, rect);
}

#[rstest::rstest]
#[case::pentagon_contained_window("pentagon-contained-window", regular_ellipse(-17.0, 12.0, 8.0, 4.0, 5, std::f64::consts::PI / 9.0), bounds(-17.2, 11.8, -16.8, 12.2))]
#[case::nine_vertex_crossing_window("nine-vertex-crossing-window", regular_ellipse(14.0, -9.0, 11.0, 3.0, 9, std::f64::consts::PI / 7.0), bounds(18.0, -10.0, 24.0, -5.0))]
#[case::thirty_two_vertex_crossing_window("thirty-two-vertex-crossing-window", regular_ellipse(-40.0, -25.0, 16.0, 9.0, 32, std::f64::consts::PI / 13.0), bounds(-29.0, -27.0, -18.0, -23.0))]
fn named_convex_rings_match_the_rectangle_clip_oracle(
    #[case] name: &'static str,
    #[case] polygon: Polygon,
    #[case] rect: Bounds,
) {
    assert_clip_matches_oracle(name, &[polygon], rect);
}

#[test]
fn ellipse_above_the_candidate_index_threshold_matches_the_oracle() {
    let ellipse = regular_ellipse(0.0, 0.0, 4.0, 3.0, 65, 0.0);
    assert_clip_matches_oracle(
        "ellipse-65-contained",
        &[ellipse],
        bounds(-1.0, -1.5, 1.25, 1.5),
    );
}

#[test]
fn close_rounded_crossings_match_the_oracle() {
    let ulp = f64::from_bits(1.0_f64.to_bits().wrapping_add(1)) - 1.0;
    let quarter = polygon(&[(-1.0, 1.0), (7.0, 1.0 + ulp), (7.0, 0.0), (-1.0, 1.0)]);
    let half = polygon(&[(-1.0, 1.0), (3.0, 1.0 + ulp), (3.0, 0.0), (-1.0, 1.0)]);
    let quarter_interval = clip_segment_rect_interval(
        Segment {
            start: XY::new(-1.0, 1.0),
            end: XY::new(7.0, 1.0 + ulp),
        },
        bounds(1.0, 0.0, 2.0, 2.0),
    )
    .unwrap()
    .unwrap();
    let half_interval = clip_segment_rect_interval(
        Segment {
            start: XY::new(-1.0, 1.0),
            end: XY::new(3.0, 1.0 + ulp),
        },
        bounds(1.0, 0.0, 2.0, 2.0),
    )
    .unwrap()
    .unwrap();
    assert_eq!(quarter_interval.start.point.y.to_bits(), 1.0_f64.to_bits());
    assert_eq!(half_interval.start.point.y.to_bits(), 1.0_f64.to_bits());
    let [quarter_x, quarter_y, quarter_w] = *quarter_interval.start.exact.unwrap();
    let [half_x, half_y, half_w] = *half_interval.start.exact.unwrap();
    assert_eq!(
        compare_ratios(&quarter_x, &quarter_w, &half_x, &half_w).unwrap(),
        std::cmp::Ordering::Equal
    );
    assert!(
        compare_ratios(&quarter_y, &quarter_w, &half_y, &half_w)
            .unwrap()
            .is_lt(),
        "the exact boundary crossings differ despite sharing one rounded coordinate"
    );
    assert_clip_oracle_failure_or_equal(
        "distinct-exact-crossings-one-rounded-coordinate",
        &[quarter, half],
        bounds(1.0, 0.0, 2.0, 2.0),
    );
}

#[test]
fn rectangle_local_contact_guard_rejects_disjoint_source_edges() {
    let subject = rectangle(0.0, 0.0, 1.0, 1.0);
    assert_clip_matches_oracle(
        "disjoint-source-window",
        &[subject],
        bounds(5.0, 5.0, 6.0, 6.0),
    );
}

#[test]
fn rectangle_clip_intersection_preserves_zm_carry() {
    let line_point =
        |x, z, m| Point::new_axes(x, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap();
    let source_line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                line_point(-1.0, 10.0, 100.0),
                line_point(2.0, 40.0, 400.0),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    let expected_line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                line_point(0.0, 20.0, 200.0),
                line_point(1.0, 30.0, 300.0),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    assert!(
        source_line
            .clip_by_rect(bounds(0.0, -1.0, 1.0, 1.0), true)
            .unwrap()
            .equals_exact(&expected_line, 0.0, true, true)
    );

    let polygon = Polygon::new(
        Ring::from_closed(
            CoordSeq::try_from_points(
                &[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]
                    .into_iter()
                    .map(|(x, y)| {
                        Point::new_axes(
                            x,
                            y,
                            ZOrdinate(Some(x + y + 7.0)),
                            MOrdinate(Some(x - y + 13.0)),
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    );
    let rect = bounds(1.0, 0.5, 3.0, 3.5);
    let expected_xy = clip::polygon_parts_to_shape(
        binary_areal_overlay(
            std::slice::from_ref(&polygon),
            &[rect_polygon(rect).unwrap()],
            OverlayOp::Intersection,
        )
        .unwrap(),
    );
    let source = Shape::Polygon(polygon);
    let expected = carry_ordinates(
        expected_xy,
        &[&source],
        "clip_by_rect",
        false,
        &crate::geometry::SourceArcContext::cartesian(),
    )
    .unwrap();
    let actual = source.clip_by_rect(rect, false).unwrap();
    assert!(actual.equals_exact(&expected, 0.0, true, true));
    let _error = source.clip_by_rect(rect, true).unwrap_err();
}
