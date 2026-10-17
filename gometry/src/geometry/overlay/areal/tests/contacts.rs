use super::super::test_support::shape_wkb;
use super::test_support::{
    assert_all_points_have_z, assert_topologically_equal, line, multiline, polygon,
    regular_polygon, square, wkb, z_square, zm_line, zm_square,
};
use crate::geometry::overlay::{
    OverlayOp, Strictness, binary_areal_overlay, overlay_all, polygon_parts_to_shape,
};
use crate::geometry::{
    CoordSeq, CoordinateAxes, GeometryKind, MOrdinate, Point, Polygon, Ring, Shape, ZOrdinate,
};

/// Every lower-dimensional contact of two areal operands is the same in
/// both operand orders: shared boundary runs, isolated touches, a region
/// plus a separate touch, nested with a shared side, identical operands.
#[test]
fn areal_intersection_contacts_are_operand_order_invariant() {
    let cases = [
        (
            "shared edge",
            square(0.0, 0.0, 2.0, 2.0),
            square(2.0, 0.0, 4.0, 2.0),
        ),
        (
            "corner touch",
            square(0.0, 0.0, 2.0, 2.0),
            square(2.0, 2.0, 4.0, 4.0),
        ),
        (
            "edge interior touch",
            square(0.0, 0.0, 2.0, 2.0),
            square(2.0, 1.0, 4.0, 3.0),
        ),
        (
            "two separate shared boundary runs",
            Shape::Polygon(polygon(&[
                (0.0, 0.0),
                (4.0, 0.0),
                (4.0, 1.0),
                (1.0, 1.0),
                (1.0, 3.0),
                (4.0, 3.0),
                (4.0, 4.0),
                (0.0, 4.0),
                (0.0, 0.0),
            ])),
            Shape::Polygon(polygon(&[
                (2.0, 1.0),
                (6.0, 1.0),
                (6.0, 3.0),
                (2.0, 3.0),
                (2.0, 1.0),
            ])),
        ),
        (
            "nested sharing a side",
            square(0.0, 0.0, 4.0, 4.0),
            square(0.0, 1.0, 2.0, 3.0),
        ),
        (
            "identical",
            square(0.0, 0.0, 2.0, 2.0),
            square(0.0, 0.0, 2.0, 2.0),
        ),
        (
            "disjoint",
            square(0.0, 0.0, 2.0, 2.0),
            square(3.0, 3.0, 4.0, 4.0),
        ),
        (
            "hole boundary touch",
            Shape::Polygon(Polygon::new(
                polygon(&[(0.0, 0.0), (6.0, 0.0), (6.0, 6.0), (0.0, 6.0), (0.0, 0.0)]).shell,
                vec![polygon(&[(2.0, 2.0), (4.0, 2.0), (4.0, 4.0), (2.0, 4.0), (2.0, 2.0)]).shell],
            )),
            square(2.0, 2.0, 3.0, 3.0),
        ),
    ];
    for (name, left, right) in cases {
        let forward = intersection(&left, &right);
        let reverse = intersection(&right, &left);
        assert_eq!(wkb(&forward), wkb(&reverse), "{name}");
    }
}

#[test]
fn areal_intersection_preserves_region_and_isolated_point() {
    let left =
        Shape::geometry_collection(vec![square(0.0, 0.0, 2.0, 2.0), square(4.0, 4.0, 6.0, 6.0)])
            .unwrap();
    let right = square(1.0, 1.0, 4.0, 4.0);
    let expected = Shape::geometry_collection(vec![
        square(1.0, 1.0, 2.0, 2.0),
        Shape::Point(Point::new_raw_xy(4.0, 4.0)),
    ])
    .unwrap();
    for (left, right) in [(&left, &right), (&right, &left)] {
        let result = intersection(left, right);
        assert!(
            result.equals(&expected).unwrap(),
            "{result:?} != {expected:?}"
        );
    }
}

#[test]
fn collapsed_shell_does_not_emit_an_areal_hole_as_linework() {
    let shell = polygon(&[(0.0, 0.0), (4.0, 0.0), (2.0, 0.0), (0.0, 0.0)]).shell;
    let hole = polygon(&[(1.0, 1.0), (3.0, 1.0), (3.0, 3.0), (1.0, 3.0), (1.0, 1.0)]).shell;
    let source = Shape::Polygon(Polygon::new(shell, vec![hole]));
    let enclosing = square(-1.0, -1.0, 5.0, 5.0);
    let expected = line(&[(0.0, 0.0), (4.0, 0.0)]);
    for (left, right) in [(&source, &enclosing), (&enclosing, &source)] {
        let result = left.intersection(right, Strictness::Lenient).unwrap();
        assert!(
            result.equals(&expected).unwrap(),
            "{result:?} != {expected:?}"
        );
    }
}

#[test]
fn areal_intersection_on_degenerate_boundaries() {
    let cases: &[(&str, &[(f64, f64)])] = &[
        ("collapsed ring", &[
            (0.0, 0.0),
            (2.0, 0.0),
            (4.0, 0.0),
            (0.0, 0.0),
        ]),
        ("retraced exterior spur", &[
            (0.0, 0.0),
            (2.0, 0.0),
            (4.0, 0.0),
            (2.0, 0.0),
            (2.0, 2.0),
            (0.0, 2.0),
            (0.0, 0.0),
        ]),
    ];
    for &(name, coordinates) in cases {
        let left = Shape::Polygon(polygon(coordinates));
        let right = square(3.0, -1.0, 5.0, 1.0);
        for (left, right) in [(&left, &right), (&right, &left)] {
            let result = intersection(left, right);
            if name == "collapsed ring" {
                assert!(
                    result.equals(&line(&[(3.0, 0.0), (4.0, 0.0)])).unwrap(),
                    "{result:?}"
                );
            }
            if name == "retraced exterior spur" {
                // Opposite traversals cancel under the areal nonzero rule.
                assert!(result.is_empty(), "{result:?}");
            }
        }
    }
}

#[test]
fn areal_intersection_lane_preserves_zm_and_operand_order() {
    let left = zm_square(0.0, 0.0, 2.0, 2.0, 10.0);
    for right in [
        zm_square(1.0, 1.0, 3.0, 3.0, 20.0),
        zm_square(2.0, 0.0, 4.0, 2.0, 20.0),
        zm_square(2.0, 2.0, 4.0, 4.0, 20.0),
    ] {
        for strictness in [Strictness::Strict, Strictness::Lenient] {
            let fast = overlay_all(
                &[&left, &right],
                crate::geometry::TopologyDomain::Cartesian,
                OverlayOp::Intersection,
                strictness,
            );
            let reverse = overlay_all(
                &[&right, &left],
                crate::geometry::TopologyDomain::Cartesian,
                OverlayOp::Intersection,
                strictness,
            );
            match (fast, reverse) {
                (Ok(fast), Ok(reverse)) => {
                    assert_eq!(wkb(&fast), wkb(&reverse), "{fast:?} != {reverse:?}");
                },
                (Err(fast), Err(reverse)) => {
                    assert_eq!(fast.to_string(), reverse.to_string());
                },
                (fast, reverse) => panic!("diverged: fast={fast:?} reverse={reverse:?}"),
            }
        }
    }
}

fn intersection(left: &Shape, right: &Shape) -> Shape {
    overlay_all(
        &[left, right],
        crate::geometry::TopologyDomain::Cartesian,
        OverlayOp::Intersection,
        Strictness::Strict,
    )
    .unwrap()
}

/// Pinch resolution: corner-touching squares union into a VALID
/// `MultiPolygon` of two simple lobes (never one figure-eight ring),
/// and the maximal walk's split classifies a shell-touching hole
/// correctly (the S2 dissolve lens — pinned by the cell-parity
/// pytest); identical operands xor to nothing.
#[test]
fn pinched_overlays_resolve_to_simple_rings() {
    let lower = polygon(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]);
    let upper = polygon(&[(1.0, 1.0), (2.0, 1.0), (2.0, 2.0), (1.0, 2.0), (1.0, 1.0)]);
    let union = binary_areal_overlay(
        std::slice::from_ref(&lower),
        std::slice::from_ref(&upper),
        OverlayOp::Union,
    )
    .unwrap();
    assert_eq!(union.len(), 2, "{union:?}");
    assert!(union.iter().all(|part| part.holes.is_empty()));

    let xor = binary_areal_overlay(
        std::slice::from_ref(&lower),
        std::slice::from_ref(&lower),
        OverlayOp::SymmetricDifference,
    )
    .unwrap();
    assert!(xor.is_empty(), "{xor:?}");
}

#[test]
fn intersection_all_typed_empty_uses_global_min_dimension() {
    let area = z_square(0.0, 0.0, 2.0, 2.0, 3.0);
    let later_disjoint_point =
        Shape::Point(Point::new_axes(10.0, 10.0, ZOrdinate(None), MOrdinate(Some(4.0))).unwrap());
    let result = overlay_all(
        &[&area, &later_disjoint_point],
        crate::geometry::TopologyDomain::Cartesian,
        OverlayOp::Intersection,
        Strictness::Strict,
    )
    .unwrap();
    assert!(
        matches!(result, Shape::Empty(GeometryKind::Point, _)),
        "{result:?}"
    );
    assert!(result.has_z() && result.has_m(), "{result:?}");
}

#[test]
fn intersection_refines_contained_and_disjoint_inputs() {
    let outer = square(0.0, 0.0, 10.0, 10.0);
    let inner = square(2.0, 2.0, 4.0, 4.0);
    let contained = outer
        .intersection(&inner, Strictness::Strict)
        .expect("contained intersection");
    assert_topologically_equal(&contained, &inner);

    let disjoint = outer
        .intersection(&square(20.0, 20.0, 22.0, 22.0), Strictness::Strict)
        .expect("disjoint intersection");
    assert!(disjoint.is_empty(), "{disjoint:?}");

    // The inner envelope is strictly inside the outer envelope, but the
    // inner polygon lies in the outer polygon's concave notch. A bounds
    // clone would return the wrong non-empty result here.
    let concave = Shape::Polygon(polygon(&[
        (0.0, 0.0),
        (10.0, 0.0),
        (10.0, 10.0),
        (6.0, 10.0),
        (6.0, 4.0),
        (4.0, 4.0),
        (4.0, 10.0),
        (0.0, 10.0),
        (0.0, 0.0),
    ]));
    let notch = square(4.5, 5.0, 5.5, 9.0);
    let refined = concave
        .intersection(&notch, Strictness::Strict)
        .expect("concave refinement");
    assert!(refined.is_empty(), "{refined:?}");
}

#[test]
fn convex_intersection_fast_path_yields_a_polygon() {
    let left = square(0.0, 0.0, 4.0, 4.0);
    let right = Shape::Polygon(polygon(&[
        (2.0, -1.0),
        (5.0, 2.0),
        (2.0, 5.0),
        (-1.0, 2.0),
        (2.0, -1.0),
    ]));
    let fast = intersection(&left, &right);
    assert!(matches!(fast, Shape::Polygon(_)), "{fast:?}");
}

#[test]
fn disjoint_convex_intersection_preserves_canonical_empty_fallback() {
    let cases = [
        (
            "overlapping envelopes",
            regular_polygon(0.0, 0.0, 1.0, 256),
            regular_polygon(1.5, 1.5, 1.0, 256),
        ),
        (
            "disjoint envelopes",
            regular_polygon(0.0, 0.0, 1.0, 256),
            regular_polygon(4.0, 0.0, 1.0, 256),
        ),
    ];
    for (name, left, right) in cases {
        // The first pair makes the candidate decline and must fall through
        // the original exact assembly chain. The second is settled before
        // dispatch by the bounds certificate. Both keep POLYGON EMPTY.
        let fast = intersection(&left, &right);
        assert!(
            matches!(
                fast,
                Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XY)
            ),
            "{name}: {fast:?}"
        );
    }
}

#[test]
fn concave_intersection_keeps_the_line_the_areal_shortcut_drops() {
    let concave_left = polygon(&[
        (0.0, 0.0),
        (6.0, 0.0),
        (6.0, 1.0),
        (1.0, 1.0),
        (1.0, 5.0),
        (6.0, 5.0),
        (6.0, 6.0),
        (0.0, 6.0),
        (0.0, 0.0),
    ]);
    let concave_right = polygon(&[
        (0.0, 0.0),
        (2.0, 0.0),
        (2.0, 2.0),
        (5.0, 2.0),
        (5.0, 5.0),
        (0.0, 5.0),
        (0.0, 0.0),
    ]);
    // The area-only binary shortcut is not a valid mixed-dimension
    // intersection oracle: this concave pair also contributes a line.
    let unrestricted = polygon_parts_to_shape(
        binary_areal_overlay(
            std::slice::from_ref(&concave_left),
            std::slice::from_ref(&concave_right),
            OverlayOp::Intersection,
        )
        .unwrap(),
    );
    let concave_left_shape = Shape::Polygon(concave_left);
    let concave_right_shape = Shape::Polygon(concave_right);
    let exact = intersection(&concave_left_shape, &concave_right_shape);
    assert_ne!(wkb(&unrestricted), wkb(&exact));
}

#[test]
fn convex_intersection_rejects_an_invalid_shell() {
    let invalid = Shape::Polygon(polygon(&[
        (0.0, 0.0),
        (4.0, 4.0),
        (0.0, 4.0),
        (4.0, 0.0),
        (0.0, 0.0),
    ]));
    let cutter = square(1.0, 1.0, 5.0, 5.0);
    let result = intersection(&invalid, &cutter);
    assert!(matches!(result, Shape::MultiPolygon(_)), "{result:?}");
    approx::assert_relative_eq!(result.area(), 4.5, max_relative = 64.0 * f64::EPSILON);
}

#[test]
fn polygon_line_intersection_boundary_and_hole_cases() {
    let holed = Shape::Polygon(Polygon::new(
        polygon(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ])
        .shell,
        vec![
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new(4.0, 4.0).unwrap(),
                    Point::new(6.0, 4.0).unwrap(),
                    Point::new(6.0, 6.0).unwrap(),
                    Point::new(4.0, 6.0).unwrap(),
                    Point::new(4.0, 4.0).unwrap(),
                ])
                .unwrap(),
            )
            .unwrap(),
        ],
    ));
    let multipart = Shape::MultiPolygon(
        vec![
            polygon(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]),
            polygon(&[(4.0, 0.0), (6.0, 0.0), (6.0, 2.0), (4.0, 2.0), (4.0, 0.0)]),
        ]
        .into(),
    );
    let cases = [
        (
            "crossing",
            square(0.0, 0.0, 4.0, 4.0),
            line(&[(-1.0, 2.0), (5.0, 2.0)]),
        ),
        (
            "contained",
            square(0.0, 0.0, 4.0, 4.0),
            line(&[(1.0, 1.0), (3.0, 3.0)]),
        ),
        (
            "exterior",
            square(0.0, 0.0, 4.0, 4.0),
            line(&[(-2.0, 5.0), (-1.0, 5.0)]),
        ),
        (
            "boundary-collinear",
            square(0.0, 0.0, 4.0, 4.0),
            line(&[(-1.0, 0.0), (5.0, 0.0)]),
        ),
        (
            "vertex-only",
            square(0.0, 0.0, 4.0, 4.0),
            line(&[(-1.0, -1.0), (0.0, 0.0)]),
        ),
        (
            "tangent",
            square(0.0, 0.0, 4.0, 4.0),
            line(&[(-1.0, 1.0), (1.0, -1.0)]),
        ),
        ("hole", holed.clone(), line(&[(1.0, 5.0), (9.0, 5.0)])),
        ("hole exterior", holed, line(&[(4.5, 5.0), (5.5, 5.0)])),
        ("multipart", multipart, line(&[(-1.0, 1.0), (7.0, 1.0)])),
    ];

    for (name, polygon, line) in cases {
        let result = intersection(&polygon, &line);
        match name {
            "crossing" | "contained" | "boundary-collinear" => {
                assert!(matches!(result, Shape::LineString(_)), "{name}: {result:?}");
            },
            "exterior" | "hole exterior" => {
                assert!(
                    matches!(result, Shape::Empty(GeometryKind::LineString, _)),
                    "{name}: {result:?}"
                );
            },
            "vertex-only" | "tangent" => {
                assert!(
                    matches!(result, Shape::Point(point) if point.x == 0.0 && point.y == 0.0),
                    "{name}: {result:?}"
                );
            },
            "hole" | "multipart" => {
                assert!(
                    matches!(result, Shape::MultiLineString(_)),
                    "{name}: {result:?}"
                );
            },
            _ => unreachable!("enumerated case"),
        }
    }

    let reverse = intersection(
        &line(&[(-1.0, 2.0), (5.0, 2.0)]),
        &square(0.0, 0.0, 4.0, 4.0),
    );
    assert!(matches!(reverse, Shape::LineString(_)), "{reverse:?}");

    let multi_line = multiline(&[&[(-1.0, 1.0), (5.0, 1.0)], &[(1.0, 3.0), (3.0, 3.0)], &[
        (-2.0, 9.0),
        (-1.0, 9.0),
    ]]);
    let multi_result = intersection(&square(0.0, 0.0, 4.0, 4.0), &multi_line);
    assert!(
        matches!(multi_result, Shape::MultiLineString(_)),
        "{multi_result:?}"
    );

    let zm_result = intersection(
        &zm_square(0.0, 0.0, 4.0, 4.0, 10.0),
        &zm_line((-1.0, 2.0), (5.0, 2.0)),
    );
    assert!(zm_result.has_z() && zm_result.has_m(), "{zm_result:?}");
    assert_all_points_have_z(&zm_result);
    zm_result.for_each_point(|point| assert!(point.m().is_some(), "{zm_result:?}"));
}

#[test]
fn rectangle_contained_collapsed_line_is_preserved_verbatim() {
    let polygon = square(0.0, 0.0, 4.0, 4.0);
    let collapsed = line(&[(1.0, 1.0), (1.0, 1.0)]);
    for (left, right) in [(&polygon, &collapsed), (&collapsed, &polygon)] {
        let result = left.intersection(right, Strictness::Strict).unwrap();
        assert_eq!(shape_wkb(&result), shape_wkb(&collapsed));
    }
}

#[test]
fn polygon_intersection_absorbs_retained_boundary_lines() {
    let cases = [
        (
            "rounded shared boundary",
            polygon(&[(0.5, 0.5), (-2.0, 1.5), (-1.5, -1.5), (0.5, 0.5)]),
            polygon(&[(0.0, 1.0), (-1.0, 0.5), (0.0, 0.0), (0.0, 1.0)]),
            // The crossing is exactly (-1/3, 5/6); both ordinates are the
            // nearest binary64 to it.
            polygon(&[
                (-1.0, 0.5),
                (0.0, 0.0),
                (0.0, 0.7),
                (-0.333_333_333_333_333_3, 0.833_333_333_333_333_4),
                (-1.0, 0.5),
            ]),
        ),
        (
            "integer shared boundary",
            polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]),
            polygon(&[(1.0, 0.0), (3.0, 0.0), (2.0, 2.0), (1.0, 0.0)]),
            polygon(&[(1.0, 0.0), (3.0, 0.0), (2.0, 2.0), (1.0, 0.0)]),
        ),
        (
            "integer collinear crossing",
            polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]),
            polygon(&[(1.0, 0.0), (3.0, 0.0), (5.0, 2.0), (3.0, 3.0), (1.0, 0.0)]),
            polygon(&[
                (1.0, 0.0),
                (3.0, 0.0),
                (4.0, 1.0),
                (4.0, 2.5),
                (3.0, 3.0),
                (1.0, 0.0),
            ]),
        ),
    ];

    for (name, left, right, expected) in cases {
        let left = Shape::Polygon(left);
        let right = Shape::Polygon(right);
        let expected = Shape::Polygon(expected);
        let result = intersection(&left, &right);
        assert_eq!(wkb(&result), wkb(&expected), "{name}: {result:?}");
        assert!(matches!(result, Shape::Polygon(_)), "{name}: {result:?}");
    }

    let square = square(0.0, 0.0, 4.0, 4.0);
    let partial_boundary = line(&[(1.0, 0.0), (3.0, 0.0)]);
    let result = intersection(&square, &partial_boundary);
    assert_eq!(wkb(&result), wkb(&partial_boundary), "{result:?}");
    assert!(matches!(result, Shape::LineString(_)), "{result:?}");
}
