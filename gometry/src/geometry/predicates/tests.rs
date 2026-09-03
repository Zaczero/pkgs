use crate::geometry::predicates::*;
use crate::geometry::*;

#[test]
fn ogc_mod2_multiline_point_relations_have_known_answers() {
    let point = Point::new_raw_xy;
    let multiline = Shape::MultiLineString(
        vec![
            LineSeq::try_new(CoordSeq::from(vec![point(0.0, 0.0), point(4.0, 0.0)]))
                .expect("test line is valid"),
            LineSeq::try_new(CoordSeq::from(vec![point(4.0, 0.0), point(4.0, 4.0)]))
                .expect("test line is valid"),
        ]
        .into(),
    );
    let cases = [
        (
            "odd-incidence boundary",
            point(0.0, 0.0),
            "F0FFFF102",
            "FF10F0FF2",
            false,
            true,
            true,
            false,
        ),
        (
            "even-incidence shared node",
            point(4.0, 0.0),
            "0FFFFF102",
            "0F1FF0FF2",
            true,
            true,
            false,
            false,
        ),
        (
            "off-line point",
            point(7.0, 9.0),
            "FF0FFF102",
            "FF1FF00F2",
            false,
            false,
            false,
            true,
        ),
    ];

    for (name, point, point_first, multiline_first, contains, covers, touches, disjoint) in cases {
        let point = Shape::Point(point);
        assert_eq!(point.relate(&multiline), point_first, "{name}");
        assert_eq!(multiline.relate(&point), multiline_first, "{name}");

        assert!(!point.contains(&multiline), "{name}");
        assert!(!point.contains_properly(&multiline), "{name}");
        assert!(!point.covers(&multiline), "{name}");
        assert_eq!(point.within(&multiline), contains, "{name}");
        assert_eq!(point.covered_by(&multiline), covers, "{name}");
        assert_eq!(point.touches(&multiline), touches, "{name}");
        assert_eq!(point.intersects(&multiline), !disjoint, "{name}");
        assert_eq!(point.disjoint(&multiline), disjoint, "{name}");

        assert_eq!(multiline.contains(&point), contains, "{name}");
        assert_eq!(multiline.contains_properly(&point), contains, "{name}");
        assert_eq!(multiline.covers(&point), covers, "{name}");
        assert!(!multiline.within(&point), "{name}");
        assert!(!multiline.covered_by(&point), "{name}");
        assert_eq!(multiline.touches(&point), touches, "{name}");
        assert_eq!(multiline.intersects(&point), !disjoint, "{name}");
        assert_eq!(multiline.disjoint(&point), disjoint, "{name}");
    }
}

#[test]
fn touching_diagonal_polygons_keep_their_de9im_matrix() {
    let high = f64::from_bits(1.0_f64.to_bits() + 1);
    let polygon = |points: &[(f64, f64)]| {
        Shape::Polygon(Polygon::new(
            Ring::from_closed(
                points
                    .iter()
                    .map(|&(x, y)| Point::new(x, y).unwrap())
                    .collect::<Vec<_>>(),
            ),
            Vec::new(),
        ))
    };
    // The upper polygon is encountered first, so its ray gets the lower
    // target id despite being geometrically second around the shared vertex.
    let upper = polygon(&[(0.0, 0.0), (1.0, high), (2.0, 3.0), (0.0, 0.0)]);
    let lower = polygon(&[(0.0, 0.0), (1.0, 1.0), (2.0, 0.0), (0.0, 0.0)]);

    assert_eq!(upper.relate(&lower), "FF2F01212");
}

mod convex_tests {
    use super::*;

    /// Trusted O(n²) convexity oracle: a non-degenerate simple polygon is
    /// convex iff every edge's supporting line keeps all vertices weakly on
    /// ONE side (this also rejects self-intersecting "stars", whose chords
    /// straddle vertices). A zero-area (all-collinear) ring is not a region.
    fn oracle_convex(ring: &[Point]) -> bool {
        let n = ring.len();
        if n < 3 {
            return true;
        }
        let mut any_turn = false;
        for i in 0..n {
            let (a, b) = (ring[i], ring[(i + 1) % n]);
            let (mut cw, mut ccw) = (false, false);
            for &c in ring {
                match orientation(a, b, c) {
                    Orientation::Clockwise => cw = true,
                    Orientation::CounterClockwise => ccw = true,
                    Orientation::Collinear => {},
                }
            }
            if cw && ccw {
                return false;
            }
            any_turn |= cw || ccw;
        }
        any_turn
    }

    /// The atan2-free [`shell_is_convex`] must agree with the oracle on
    /// representative general-position rings of each admitted class.
    #[test]
    fn convexity_matches_oracle_on_curated_rings() {
        let p = |x, y| Point::new_raw_xy(x, y);
        let cases = [
            ("convex_cw_triangle", vec![
                p(-3.0, 3.0),
                p(2.0, -2.0),
                p(1.0, -4.0),
            ]),
            ("convex_ccw_pentagon", vec![
                p(-3.0, -4.0),
                p(3.0, -1.0),
                p(3.0, 4.0),
                p(-1.0, 1.0),
                p(-2.0, -1.0),
            ]),
            ("simple_reflex_ccw", vec![
                p(-2.0, 1.0),
                p(-2.0, 3.0),
                p(-4.0, -4.0),
                p(0.0, 3.0),
            ]),
            ("simple_reflex_cw", vec![
                p(-1.0, 0.0),
                p(3.0, 1.0),
                p(0.0, -1.0),
                p(-1.0, -3.0),
                p(0.0, 0.0),
                p(-4.0, -1.0),
            ]),
            ("longer_reflex", vec![
                p(-1.0, 0.0),
                p(-1.0, -2.0),
                p(3.0, 3.0),
                p(2.0, -1.0),
                p(3.0, -2.0),
                p(-2.0, -4.0),
                p(-4.0, -4.0),
            ]),
            ("self_crossing_six", vec![
                p(3.0, 4.0),
                p(1.0, -2.0),
                p(-2.0, 3.0),
                p(-2.0, -4.0),
                p(3.0, -2.0),
                p(-1.0, 3.0),
            ]),
            ("self_crossing_eight", vec![
                p(0.0, -3.0),
                p(-2.0, 0.0),
                p(-3.0, -1.0),
                p(-1.0, 4.0),
                p(-1.0, 3.0),
                p(-2.0, 4.0),
                p(0.0, -1.0),
                p(3.0, 1.0),
            ]),
        ];
        for (name, ring) in cases {
            assert_eq!(
                shell_is_convex(ring.as_slice()),
                oracle_convex(&ring),
                "convexity disagreement in {name}: {ring:?}"
            );
        }
    }

    /// Collinear and degenerate cases excluded from the general-position table.
    #[test]
    fn collinear_and_degenerate_cases() {
        let p = |x: f64, y: f64| Point::new_raw_xy(x, y);
        let convex: &[&[Point]] = &[
            &[p(0.0, 0.0), p(2.0, 0.0), p(1.0, 2.0)], // triangle
            &[p(0.0, 0.0), p(2.0, 0.0), p(2.0, 2.0), p(0.0, 2.0)], // square
            // square with a benign collinear vertex midway down the left edge
            &[
                p(0.0, 0.0),
                p(2.0, 0.0),
                p(2.0, 2.0),
                p(0.0, 2.0),
                p(0.0, 1.0),
            ],
            // collinear vertices along the bottom edge
            &[
                p(0.0, 0.0),
                p(1.0, 0.0),
                p(2.0, 0.0),
                p(2.0, 2.0),
                p(0.0, 2.0),
            ],
        ];
        let not_convex: &[&[Point]] = &[
            &[p(0.0, 0.0), p(2.0, 0.0), p(2.0, 2.0), p(1.0, 0.5)], // concave dart
            // pentagram: distinct vertices, all-same-turn, winds twice
            &[
                p(0.0, 2.0),
                p(1.0, -1.0),
                p(-1.6, 1.0),
                p(1.6, 1.0),
                p(-1.0, -1.0),
            ],
            // overlapping edges along y = x (non-simple)
            &[
                p(0.0, 0.0),
                p(3.0, 3.0),
                p(-1.0, 1.0),
                p(-1.0, -1.0),
                p(1.0, 1.0),
            ],
            &[p(0.0, 0.0), p(2.0, 0.0), p(0.0, 0.0), p(0.0, 2.0)], // repeated-vertex spike
        ];
        for ring in convex {
            assert!(shell_is_convex(*ring), "expected convex: {ring:?}");
        }
        for ring in not_convex {
            assert!(!shell_is_convex(*ring), "expected non-convex: {ring:?}");
        }
    }

    #[test]
    fn axis_rectangle_detection_and_intersects_fast_path() {
        let p = |x: f64, y: f64| Point::new_raw_xy(x, y);
        let rect = |minx, miny, maxx, maxy| {
            Shape::Polygon(Polygon::new(
                Ring::from_closed(vec![
                    p(minx, miny),
                    p(maxx, miny),
                    p(maxx, maxy),
                    p(minx, maxy),
                    p(minx, miny),
                ]),
                Vec::new(),
            ))
        };
        let bounds = |shape: &Shape| shape.bounds().expect("bounded");

        // A box from any corner order is detected; degenerate / holed / non-axis
        // polygons are not.
        let r = rect(0.0, 0.0, 10.0, 10.0);
        assert!(is_axis_aligned_rectangle(&r, bounds(&r)));
        let line_box = rect(0.0, 0.0, 10.0, 0.0); // zero height → not a rectangle
        assert!(!is_axis_aligned_rectangle(&line_box, bounds(&line_box)));
        let triangle = Shape::Polygon(Polygon::new(
            Ring::from_closed(vec![p(0.0, 0.0), p(10.0, 0.0), p(5.0, 10.0), p(0.0, 0.0)]),
            Vec::new(),
        ));
        assert!(!is_axis_aligned_rectangle(&triangle, bounds(&triangle)));
        let diamond = Shape::Polygon(Polygon::new(
            Ring::from_closed(vec![
                p(5.0, 0.0),
                p(10.0, 5.0),
                p(5.0, 10.0),
                p(0.0, 5.0),
                p(5.0, 0.0),
            ]),
            Vec::new(),
        ));
        assert!(!is_axis_aligned_rectangle(&diamond, bounds(&diamond)));

        let densified = Shape::Polygon(Polygon::new(
            Ring::from_closed(vec![
                p(0.0, 0.0),
                p(5.0, 0.0),
                p(10.0, 0.0),
                p(10.0, 10.0),
                p(5.0, 10.0),
                p(0.0, 10.0),
                p(0.0, 0.0),
            ]),
            Vec::new(),
        ));
        assert!(is_axis_aligned_rectangle(&densified, bounds(&densified)));
        assert!(
            PreparedPlan::transient(&densified).intersects(&PreparedPlan::transient(&r)),
            "cached planar intersects recognizes a densified rectangle"
        );

        // The fast path must agree with the general kernel on every box relation:
        // overlap, crossing (no corner of one inside the other), edge-touch,
        // corner-touch, nested, and disjoint.
        let base = rect(0.0, 0.0, 10.0, 10.0);
        let cases = [
            (rect(5.0, 5.0, 15.0, 15.0), true),    // offset overlap
            (rect(2.0, 2.0, 8.0, 8.0), true),      // nested
            (rect(10.0, 0.0, 20.0, 10.0), true),   // edge touch
            (rect(10.0, 10.0, 20.0, 20.0), true),  // corner touch
            (rect(20.0, 20.0, 30.0, 30.0), false), // disjoint
            (rect(4.0, -5.0, 6.0, 15.0), true),    // crossing "+": no corner inside
        ];
        for (other, expected) in cases {
            assert_eq!(
                base.intersects(&other),
                expected,
                "rect intersects mismatch for {other:?}"
            );
            // symmetric
            assert_eq!(other.intersects(&base), expected);
        }
    }
}
