use crate::geometry::relate::areal_relate_arrangement_oracle;
use crate::geometry::relate_ng::*;
use crate::geometry::{CoordSeq, Point, Ring, Shape};

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

fn radial(cx: f64, cy: f64, radii: &[f64], rot: f64) -> Polygon {
    Polygon::new(
        Ring::from_trusted_closed(radial_ring(cx, cy, radii, rot, false)),
        Vec::new(),
    )
}

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

fn rotate_closed(points: &[Point], offset: usize) -> CoordSeq {
    let open = &points[..points.len() - 1];
    let mut rotated: Vec<Point> = (0..open.len())
        .map(|index| open[(index + offset) % open.len()])
        .collect();
    rotated.push(rotated[0]);
    CoordSeq::from(rotated)
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the full curated oracle table stays explicit for auditability"
)]
fn areal_relate_ng_matches_arrangement_oracle_on_curated_cases() {
    let check = |name: &str, left: Vec<Polygon>, right: Vec<Polygon>, require_fast: bool| {
        // Prepared testers built from the same parts must produce the
        // byte-identical matrix as the raw per-ring scan path — this locks
        // the hierarchical `PointBatchTester` fast lane against the
        // arrangement oracle.
        let left_tester = PointBatchTester::new(&Shape::MultiPolygon(left.clone()));
        let right_tester = PointBatchTester::new(&Shape::MultiPolygon(right.clone()));
        let prepared = AreaTesters {
            left: Some(&left_tester),
            right: Some(&right_tester),
        };
        let Some(RelateDecision::Matrix(fast)) =
            areal_relate_ng(&left, &right, RelateGoal::Matrix, AreaTesters::default())
        else {
            let _ = areal_relate_arrangement_oracle(&left, &right).expect("non-empty parts");
            assert!(
                areal_relate_ng(&left, &right, RelateGoal::Matrix, prepared).is_none(),
                "{name}: prepared lane took a case the raw lane deferred\nleft={left:?}\nright={right:?}"
            );
            assert!(
                !require_fast,
                "{name}: required-fast case deferred\nleft={left:?}\nright={right:?}"
            );
            return;
        };
        let exact = areal_relate_arrangement_oracle(&left, &right).expect("non-empty parts");
        assert_eq!(
            fast.text(),
            exact.text(),
            "{name}: left={left:?}\nright={right:?}"
        );
        let Some(RelateDecision::Matrix(prepared_fast)) =
            areal_relate_ng(&left, &right, RelateGoal::Matrix, prepared)
        else {
            panic!(
                "{name}: prepared lane deferred a case the raw lane graded\nleft={left:?}\nright={right:?}"
            );
        };
        assert_eq!(
            prepared_fast.text(),
            fast.text(),
            "{name}: prepared raycaster disagreed with raw scan\nleft={left:?}\nright={right:?}"
        );
        let Some(RelateDecision::Matrix(transposed)) =
            areal_relate_ng(&right, &left, RelateGoal::Matrix, AreaTesters::default())
        else {
            return;
        };
        assert_eq!(
            transposed.text(),
            fast.transpose().text(),
            "{name}: reverse lane"
        );
    };
    let box4 = || rectangle(0.0, 0.0, 4.0, 4.0);
    check(
        "shared_full",
        vec![box4()],
        vec![rectangle(4.0, 0.0, 6.8, 4.0)],
        false,
    );
    check(
        "shared_partial",
        vec![box4()],
        vec![rectangle(4.0, 1.0, 6.8, 3.0)],
        false,
    );
    check(
        "shared_corner",
        vec![box4()],
        vec![rectangle(4.0, 4.0, 6.4, 6.4)],
        false,
    );
    check("identical", vec![box4()], vec![box4()], false);
    check(
        "ordinary_overlap",
        vec![box4()],
        vec![rectangle(2.0, 0.0, 6.0, 4.0)],
        false,
    );
    let holed = holed_rectangle(0.0, 0.0, 10.0, 10.0, (2.5, 2.5, 7.5, 7.5));
    check(
        "hole_shared_edge",
        vec![holed.clone()],
        vec![rectangle(2.5, 2.5, 5.0, 5.0)],
        true,
    );
    check(
        "filled_hole",
        vec![holed],
        vec![rectangle(2.5, 2.5, 7.5, 7.5)],
        true,
    );
    let large = rectangle(1e12, -1e12, 1e12 + 4000.0, -1e12 + 3000.0);
    check(
        "large_vertical_shared",
        vec![large.clone()],
        vec![rectangle(
            1e12 + 4000.0,
            -1e12 + 600.0,
            1e12 + 6800.0,
            -1e12 + 2400.0,
        )],
        false,
    );
    check(
        "large_horizontal_shared",
        vec![large],
        vec![rectangle(
            1e12 + 800.0,
            -1e12 + 3000.0,
            1e12 + 3200.0,
            -1e12 + 4800.0,
        )],
        false,
    );
    let cyclic = holed_rectangle(0.0, 0.0, 8.0, 8.0, (2.4, 2.0, 5.2, 5.6));
    let cyclic_rotated = Polygon::new(
        Ring::from_trusted_closed(rotate_closed(&cyclic.shell.iter().collect::<Vec<_>>(), 2)),
        vec![Ring::from_trusted_closed(rotate_closed(
            &cyclic.holes[0].iter().collect::<Vec<_>>(),
            1,
        ))],
    );
    check(
        "cyclic_permutation",
        vec![cyclic],
        vec![cyclic_rotated],
        false,
    );
    check(
        "gapped_multipolygon",
        vec![
            rectangle(-3.0, 0.8, 0.0, 3.0),
            rectangle(-3.0, 5.5, 0.0, 7.7),
        ],
        vec![rectangle(0.0, 0.0, 5.0, 10.0)],
        true,
    );
    let vertex_through = Polygon::new(
        Ring::from_trusted_closed(vec![
            Point::new_unchecked_xy(2.0, 0.0),
            Point::new_unchecked_xy(4.0, 2.2),
            Point::new_unchecked_xy(6.0, 0.0),
            Point::new_unchecked_xy(4.0, -2.2),
            Point::new_unchecked_xy(2.0, 0.0),
        ]),
        Vec::new(),
    );
    check(
        "vertex_through",
        vec![rectangle(0.0, 0.0, 8.0, 4.0)],
        vec![vertex_through],
        false,
    );
    check(
        "corner_touch",
        vec![rectangle(0.0, 0.0, 2.0, 2.0)],
        vec![rectangle(2.0, 2.0, 4.0, 4.0)],
        true,
    );
    let base = 1e16;
    let x0 = base + 8.0;
    check(
        "one_ulp_section",
        vec![rectangle(base, base, base + 8.0, base + 8.0)],
        vec![rectangle(
            x0,
            base + 2.0,
            f64::from_bits(x0.to_bits() + 1),
            base + 6.0,
        )],
        false,
    );
    check(
        "reflex_radial",
        vec![radial(0.0, 0.0, &[3.0, 1.4, 2.8, 1.2, 3.2, 1.5, 2.6], 0.0)],
        vec![radial(1.0, 0.5, &[2.7, 1.3, 3.0, 1.1, 2.5, 1.5], 0.25)],
        false,
    );
    check(
        "holed_radial",
        vec![radial_holed(
            0.0,
            0.0,
            &[4.0, 3.0, 4.0, 3.0, 4.0, 3.0, 4.0, 3.0],
            0.0,
            0.3,
        )],
        vec![radial(1.5, 0.0, &[3.0; 8], std::f64::consts::FRAC_PI_8)],
        false,
    );
    check(
        "overlapping_parts",
        vec![
            radial(0.0, 0.0, &[3.0; 6], 0.0),
            radial(1.5, 0.0, &[2.5; 6], std::f64::consts::FRAC_PI_6),
        ],
        vec![radial_holed(
            0.75,
            0.5,
            &[2.8; 8],
            std::f64::consts::FRAC_PI_8,
            0.25,
        )],
        false,
    );
}
