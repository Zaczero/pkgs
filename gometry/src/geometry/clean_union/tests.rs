use crate::geometry::clean_union::*;
use crate::geometry::overlay::{OverlayOp, binary_areal_overlay, polygon_parts_to_shape};
use crate::geometry::{CoordSeq, Point, Polygon, Ring};
/// One closed radial ring: vertices at evenly-spaced angles with per-vertex
/// radii (always SIMPLE; non-convex when the radii vary). `clockwise`
/// reverses the winding for use as a hole.
fn radial_ring(cx: f64, cy: f64, radii: &[f64], rot: f64, clockwise: bool) -> CoordSeq {
    let n = radii.len();
    let mut points: Vec<Point> = (0..n)
        .map(|i| {
            let angle = rot + std::f64::consts::TAU * i as f64 / n as f64;
            Point::new_unchecked_xy(cx + radii[i] * angle.cos(), cy + radii[i] * angle.sin())
        })
        .collect();
    if clockwise {
        points.reverse();
    }
    points.push(points[0]);
    CoordSeq::from(points)
}

/// Holeless radial polygon.
fn radial(cx: f64, cy: f64, radii: &[f64], rot: f64) -> Polygon {
    Polygon::new(
        Ring::from_trusted_closed(radial_ring(cx, cy, radii, rot, false)),
        Vec::new(),
    )
}

/// Radial polygon with a concentric (smaller, CW) radial hole — the
/// lakes-with-islands shape that real overlay must handle.
fn radial_holed(cx: f64, cy: f64, radii: &[f64], rot: f64, hole_scale: f64) -> Polygon {
    let hole_radii: Vec<f64> = radii.iter().map(|r| r * hole_scale).collect();
    Polygon::new(
        Ring::from_trusted_closed(radial_ring(cx, cy, radii, rot, false)),
        vec![Ring::from_trusted_closed(radial_ring(
            cx,
            cy,
            &hole_radii,
            rot,
            true,
        ))],
    )
}

fn rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Polygon {
    Polygon::new(
        Ring::from_trusted_closed(vec![
            Point::new_unchecked_xy(minx, miny),
            Point::new_unchecked_xy(maxx, miny),
            Point::new_unchecked_xy(maxx, maxy),
            Point::new_unchecked_xy(minx, maxy),
            Point::new_unchecked_xy(minx, miny),
        ]),
        Vec::new(),
    )
}

fn holed_rectangle(
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
    hole: (f64, f64, f64, f64),
) -> Polygon {
    let (hx0, hy0, hx1, hy1) = hole;
    Polygon::new(rectangle(minx, miny, maxx, maxy).shell, vec![
        Ring::from_trusted_closed(vec![
            Point::new_unchecked_xy(hx0, hy0),
            Point::new_unchecked_xy(hx1, hy0),
            Point::new_unchecked_xy(hx1, hy1),
            Point::new_unchecked_xy(hx0, hy1),
            Point::new_unchecked_xy(hx0, hy0),
        ]),
    ])
}

fn assert_fast_matches(name: &str, a: &[Polygon], b: &[Polygon], op: OverlayOp) {
    let fast = clean_overlay(a, b, op)
        .unwrap_or_else(|| panic!("clean_overlay deferred {name} for {op:?}"));
    let exact = polygon_parts_to_shape(binary_areal_overlay(a, b, op));
    let area_ok = (fast.area() - exact.area()).abs() <= 1e-6 * exact.area().max(1.0);
    assert!(
        area_ok && fast.equals(&exact),
        "clean_overlay {name} {op:?} disagrees with exact engine:\n  fast  area={}\n  exact area={}",
        fast.area(),
        exact.area(),
    );
}

fn assert_deferred(name: &str, a: &[Polygon], b: &[Polygon], op: OverlayOp) {
    assert!(
        clean_overlay(a, b, op).is_none(),
        "clean_overlay unexpectedly handled {name} for {op:?}"
    );
}

/// Deterministic shared-edge fixtures flagged by the review pair as
/// supported-but-unfixtured op×config combos — pinned against the oracle so
/// a future change to `shared_arc_rule`/reseed can't silently break them.
#[test]
fn shared_edge_review_fixtures_match_oracle() {
    let big = rectangle(0.0, 0.0, 10.0, 10.0);
    // Same-direction shared bottom edge + a proper cross of B's top through A:
    // INTERSECTION (review B: only union/difference were fixtured before).
    assert_fast_matches(
        "shared+cross intersection",
        std::slice::from_ref(&big),
        std::slice::from_ref(&rectangle(4.0, 0.0, 14.0, 6.0)),
        OverlayOp::Intersection,
    );
    // Inner box sharing only A's bottom sub-edge (no proper cross): shared-only
    // DIFFERENCE and SYMMETRIC_DIFFERENCE (review B: only union/intersection
    // were fixtured before) — the notch with the shared sub-edge cancelled.
    let inner = rectangle(2.0, 0.0, 8.0, 4.0);
    for op in [OverlayOp::Difference, OverlayOp::SymmetricDifference] {
        assert_fast_matches(
            "shared-only bottom sub-edge",
            std::slice::from_ref(&big),
            std::slice::from_ref(&inner),
            op,
        );
    }
}

/// The clean fast path must match the exact arrangement engine on every
/// curated general-position and shared-boundary admission case.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the full curated admission/defer table stays explicit for auditability"
)]
fn clean_overlay_matches_exact_engine_on_curated_cases() {
    let ops = [
        OverlayOp::Union,
        OverlayOp::Difference,
        OverlayOp::SymmetricDifference,
        OverlayOp::Intersection,
    ];
    let general_cases = [
        (
            "convex_transverse",
            vec![radial(0.0, 0.0, &[4.0; 6], 0.0)],
            vec![radial(2.0, 0.0, &[3.0; 6], std::f64::consts::FRAC_PI_6)],
        ),
        (
            "reflex_transverse",
            vec![radial(
                0.0,
                0.0,
                &[4.0, 1.5, 4.0, 1.5, 4.0, 1.5, 4.0, 1.5],
                0.0,
            )],
            vec![radial(
                1.5,
                0.0,
                &[3.5, 1.25, 3.5, 1.25, 3.5, 1.25, 3.5, 1.25],
                std::f64::consts::FRAC_PI_8,
            )],
        ),
        (
            "holed_transverse",
            vec![radial_holed(
                0.0,
                0.0,
                &[5.0, 4.0, 5.0, 4.0, 5.0, 4.0, 5.0, 4.0],
                0.0,
                0.35,
            )],
            vec![radial(2.0, 0.0, &[3.0; 8], std::f64::consts::FRAC_PI_8)],
        ),
        (
            "two_component_transverse",
            vec![
                radial(0.0, 0.0, &[4.0; 6], 0.0),
                radial(20.0, 0.0, &[4.0; 6], 0.0),
            ],
            vec![
                radial(2.0, 0.0, &[3.0; 6], std::f64::consts::FRAC_PI_6),
                radial(22.0, 0.0, &[3.0; 6], std::f64::consts::FRAC_PI_6),
            ],
        ),
    ];
    for (name, a, b) in general_cases {
        for op in ops {
            assert_fast_matches(name, &a, &b, op);
        }
    }

    let whole_edge = (vec![rectangle(0.0, 0.0, 4.0, 3.0)], vec![rectangle(
        4.0, 0.0, 8.0, 3.0,
    )]);
    for op in [
        OverlayOp::Union,
        OverlayOp::SymmetricDifference,
        OverlayOp::Difference,
    ] {
        assert_fast_matches("whole_edge", &whole_edge.0, &whole_edge.1, op);
    }
    assert_deferred(
        "whole_edge intersection",
        &whole_edge.0,
        &whole_edge.1,
        OverlayOp::Intersection,
    );

    let partial_shared_subedge = (vec![rectangle(0.0, 0.0, 6.0, 6.0)], vec![rectangle(
        6.0, 2.0, 10.0, 5.0,
    )]);
    for op in [OverlayOp::Union, OverlayOp::Difference] {
        assert_fast_matches(
            "partial_shared_subedge",
            &partial_shared_subedge.0,
            &partial_shared_subedge.1,
            op,
        );
    }

    let same_direction_shared_bottom = (vec![rectangle(0.0, 0.0, 10.0, 10.0)], vec![rectangle(
        2.0, 0.0, 8.0, 4.0,
    )]);
    for op in [OverlayOp::Intersection, OverlayOp::Union] {
        assert_fast_matches(
            "same_direction_shared_bottom",
            &same_direction_shared_bottom.0,
            &same_direction_shared_bottom.1,
            op,
        );
    }

    let filled_hole = (
        vec![holed_rectangle(0.0, 0.0, 10.0, 10.0, (3.0, 3.0, 7.0, 7.0))],
        vec![rectangle(3.0, 3.0, 7.0, 7.0)],
    );
    for op in [OverlayOp::Union, OverlayOp::SymmetricDifference] {
        assert_fast_matches("filled_hole", &filled_hole.0, &filled_hole.1, op);
    }

    let multiple_shared_spans = (
        vec![
            rectangle(-3.0, 1.0, 0.0, 3.0),
            rectangle(-3.0, 6.0, 0.0, 8.0),
        ],
        vec![rectangle(0.0, 0.0, 5.0, 10.0)],
    );
    assert_fast_matches(
        "multiple_shared_spans",
        &multiple_shared_spans.0,
        &multiple_shared_spans.1,
        OverlayOp::Union,
    );

    let shared_plus_cross = (vec![rectangle(0.0, 0.0, 8.0, 8.0)], vec![rectangle(
        3.0, 0.0, 11.0, 5.0,
    )]);
    for op in [OverlayOp::Union, OverlayOp::Difference] {
        assert_fast_matches(
            "shared_plus_cross",
            &shared_plus_cross.0,
            &shared_plus_cross.1,
            op,
        );
    }
    assert_deferred(
        "shared_plus_cross symmetric_difference",
        &shared_plus_cross.0,
        &shared_plus_cross.1,
        OverlayOp::SymmetricDifference,
    );

    let triple_coincident = (vec![rectangle(0.0, 0.0, 4.0, 3.0)], vec![
        rectangle(4.0, 0.0, 8.0, 3.0),
        rectangle(4.0, 0.0, 8.0, 3.0),
    ]);
    assert_deferred(
        "triple_coincident",
        &triple_coincident.0,
        &triple_coincident.1,
        OverlayOp::Union,
    );
    let corner_touch = (vec![rectangle(0.0, 0.0, 2.0, 2.0)], vec![rectangle(
        2.0, 2.0, 4.0, 4.0,
    )]);
    assert_deferred(
        "corner_touch",
        &corner_touch.0,
        &corner_touch.1,
        OverlayOp::Union,
    );
}
