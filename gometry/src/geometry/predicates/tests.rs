use crate::geometry::predicates::*;
use crate::geometry::*;
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
        let p = |x, y| Point::new_unchecked_xy(x, y);
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
        let p = |x: f64, y: f64| Point::new_unchecked_xy(x, y);
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
        let p = |x: f64, y: f64| Point::new_unchecked_xy(x, y);
        let rect = |minx, miny, maxx, maxy| {
            Shape::Polygon(Polygon::new(
                Ring::from_trusted_closed(vec![
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

        // A box from any corner order is detected; degenerate / holed / non-quad
        // / non-axis polygons are not.
        let r = rect(0.0, 0.0, 10.0, 10.0);
        assert!(r.is_axis_aligned_rectangle(bounds(&r)));
        let line_box = rect(0.0, 0.0, 10.0, 0.0); // zero height → not a rectangle
        assert!(!line_box.is_axis_aligned_rectangle(bounds(&line_box)));
        let triangle = Shape::Polygon(Polygon::new(
            Ring::from_trusted_closed(vec![p(0.0, 0.0), p(10.0, 0.0), p(5.0, 10.0), p(0.0, 0.0)]),
            Vec::new(),
        ));
        assert!(!triangle.is_axis_aligned_rectangle(bounds(&triangle)));
        let diamond = Shape::Polygon(Polygon::new(
            Ring::from_trusted_closed(vec![
                p(5.0, 0.0),
                p(10.0, 5.0),
                p(5.0, 10.0),
                p(0.0, 5.0),
                p(5.0, 0.0),
            ]),
            Vec::new(),
        ));
        assert!(!diamond.is_axis_aligned_rectangle(bounds(&diamond)));

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
