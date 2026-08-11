use std::num::NonZeroU32;

use crate::geometry::constructive::walk::reflex_cross;
use crate::geometry::constructive::{
    WalkJoinRule, winding_buffer, winding_erosion, winding_stroke,
};
use crate::geometry::{
    BufferCapStyle, CoordSeq, LineSeq, Point, Polygon, Ring, Segment, Shape, XY,
};

#[test]
fn reflex_cross_retains_an_exact_endpoint_local_parameter() {
    let length = 2.0_f64.powi(1023);
    let x = 2.0_f64.powi(-52);
    let incoming = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(length, 0.0),
    };
    let outgoing = Segment {
        start: XY::new(x, -1.0),
        end: XY::new(x, 1.0),
    };
    let (point, t_in, t_out) = reflex_cross(incoming, outgoing).unwrap();
    assert_eq!(point, XY::new(x, 0.0));
    assert!(t_in.uses_exact_ratio());
    assert!(!t_in.is_start() && !t_in.is_end());
    assert_eq!(t_in.fraction().to_bits(), 0.0_f64.to_bits());
    assert_eq!(t_out.fraction().to_bits(), 0.5_f64.to_bits());
}

#[test]
fn affine_scale_keeps_the_independent_reciprocal_axis() {
    let source = Shape::Point(Point::new(1e308, 1e-300).unwrap());
    let transformed = source.scale(0.5, 1.0, (-1e308, 0.0)).unwrap();
    let Shape::Point(point) = transformed else {
        panic!("point scaling must preserve its kind")
    };
    assert_eq!(point.x.to_bits(), 0.0_f64.to_bits());
    assert_eq!(point.y.to_bits(), 1e-300_f64.to_bits());
}

#[cfg(test)]
mod winding_buffer_tests {
    use super::*;
    use crate::geometry::Coordinates as _;

    fn ring(points: &[(f64, f64)]) -> Ring {
        Ring::from_trusted_closed(
            points
                .iter()
                .map(|&(x, y)| Point::new(x, y).unwrap())
                .collect::<Vec<_>>(),
        )
    }

    /// Full annihilation under erosion must yield the empty polygon even
    /// for deep-notched shells whose inverted inward loop winds the whole
    /// neighborhood positively (the fuzz repro that caught a divergence
    /// between the engine and the reference construction).
    #[test]
    fn erosion_annihilates_deep_notched_shell() {
        let shell = ring(&[
            (-0.514_876_326_055_279_8, 48.442_629_193_004_55),
            (3.661_534_463_294_989_5, 48.442_629_193_004_55),
            (3.861_534_463_294_989_7, 50.911_843_795_191_2),
            (4.061_534_463_294_99, 48.442_629_193_004_55),
            (6.096_036_601_959_777, 48.442_629_193_004_55),
            (6.096_036_601_959_777, 54.615_665_698_471_18),
            (-0.514_876_326_055_279_8, 54.615_665_698_471_18),
            (-0.514_876_326_055_279_8, 48.442_629_193_004_55),
        ]);
        let polygon = Polygon::new(shell, Vec::new());
        let eroded = winding_erosion(
            std::slice::from_ref(&polygon),
            3.938_095_265_107_087_5,
            WalkJoinRule::Arc,
            NonZeroU32::new(8).unwrap(),
        )
        .unwrap();
        assert!(eroded.is_empty(), "expected empty, got {eroded:?}");
    }

    /// The winding-number buffer engine (the routed general positive
    /// round-join path): correctness pins for the cases that killed
    /// earlier designs.
    #[test]
    fn winding_engine_handles_holes_notches_and_inversions() {
        // Surviving square hole: sharp shrunk corners, exact area.
        let with_hole = Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![ring(&[
                (4.0, 4.0),
                (6.0, 4.0),
                (6.0, 6.0),
                (4.0, 6.0),
                (4.0, 4.0),
            ])],
        );
        let buffered = winding_buffer(
            std::slice::from_ref(&with_hole),
            0.5,
            WalkJoinRule::Arc,
            NonZeroU32::new(4).unwrap(),
        )
        .unwrap();
        let expected =
            100.0 + 4.0 * (10.0 * 0.5) + 0.5 * 16.0 * 0.25 * (std::f64::consts::PI / 8.0).sin()
                - 1.0;
        assert!(
            (buffered.area() - expected).abs() < 1e-9,
            "{}",
            buffered.area()
        );

        // Vanished hole: distance beyond the inradius cancels exactly.
        let vanished = winding_buffer(
            std::slice::from_ref(&with_hole),
            1.5,
            WalkJoinRule::Arc,
            NonZeroU32::new(4).unwrap(),
        )
        .unwrap();
        let solid = Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            Vec::new(),
        );
        let solid_buffered = winding_buffer(
            std::slice::from_ref(&solid),
            1.5,
            WalkJoinRule::Arc,
            NonZeroU32::new(4).unwrap(),
        )
        .unwrap();
        assert!((vanished.area() - solid_buffered.area()).abs() < 1e-9);

        // Deep notch seals; the result covers the input.
        let notched = Polygon::new(
            ring(&[
                (0.0, 0.0),
                (4.3, 0.0),
                (4.5, 1.8),
                (4.7, 0.0),
                (9.0, 0.0),
                (9.0, 6.0),
                (0.0, 6.0),
                (0.0, 0.0),
            ]),
            Vec::new(),
        );
        let sealed = winding_buffer(
            std::slice::from_ref(&notched),
            0.3,
            WalkJoinRule::Arc,
            NonZeroU32::new(2).unwrap(),
        )
        .unwrap();
        assert!(sealed.covers(&Shape::Polygon(notched)));
    }

    /// Miter and bevel join rules: a square's 90-degree corners are well
    /// within the default limit, so the miter expansion is the exact
    /// larger square and the bevel expansion cuts each corner to a
    /// half-`d^2` triangle. Erosion of a convex shape never touches the
    /// outside-join rule (all walk turns are inside turns), so every
    /// style erodes a square to the same smaller square.
    #[test]
    fn miter_and_bevel_rules_are_exact_on_squares() {
        let square = Polygon::new(
            ring(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]),
            Vec::new(),
        );
        let parts = std::slice::from_ref(&square);
        let miter = WalkJoinRule::Miter { limit: 5.0 };
        let mitered = winding_buffer(parts, 1.0, miter, NonZeroU32::new(8).unwrap()).unwrap();
        assert!((mitered.area() - 36.0).abs() < 1e-12, "{}", mitered.area());
        let bevelled =
            winding_buffer(parts, 1.0, WalkJoinRule::Bevel, NonZeroU32::new(8).unwrap()).unwrap();
        assert!(
            (bevelled.area() - 34.0).abs() < 1e-12,
            "{}",
            bevelled.area()
        );
        for rule in [WalkJoinRule::Arc, WalkJoinRule::Bevel, miter] {
            let eroded = winding_erosion(parts, 1.0, rule, NonZeroU32::new(8).unwrap()).unwrap();
            assert!((eroded.area() - 4.0).abs() < 1e-12, "{}", eroded.area());
        }
    }

    /// Over-limit corners clip flat at exactly `limit * distance` along
    /// the corner bisector (the GEOS limited-miter shape — continuous in
    /// the corner angle), leaving the area strictly between the bevel
    /// and the unlimited miter.
    #[test]
    fn sharp_miter_clips_at_the_limit_reach() {
        // Sharp spike at (12, 1): ratio 1/sin(theta/2) far above 2.
        let spike = Polygon::new(
            ring(&[(0.0, 0.0), (12.0, 1.0), (0.0, 2.0), (0.0, 0.0)]),
            Vec::new(),
        );
        let parts = std::slice::from_ref(&spike);
        let distance = 0.5;
        let limit = 2.0;
        let clipped = winding_buffer(
            parts,
            distance,
            WalkJoinRule::Miter { limit },
            NonZeroU32::new(8).unwrap(),
        )
        .unwrap();
        let bevelled = winding_buffer(
            parts,
            distance,
            WalkJoinRule::Bevel,
            NonZeroU32::new(8).unwrap(),
        )
        .unwrap();
        let unlimited = winding_buffer(
            parts,
            distance,
            WalkJoinRule::Miter { limit: 1e9 },
            NonZeroU32::new(8).unwrap(),
        )
        .unwrap();
        assert!(bevelled.area() < clipped.area() && clipped.area() < unlimited.area());
        // The farthest output vertex from the spike tip sits at the clip:
        // its bisector projection is exactly `limit * distance`.
        let tip = Point::new(12.0, 1.0).unwrap();
        let Shape::Polygon(out) = &clipped else {
            panic!("expected a polygon, got {clipped:?}");
        };
        let reach = out
            .shell
            .coords()
            .iter_coords()
            .map(|point| {
                // Bisector at the tip points along +x (symmetric spike).
                point.x - tip.x
            })
            .fold(f64::NEG_INFINITY, f64::max);
        assert!(
            (reach - limit * distance).abs() < 1e-9,
            "clip reach {reach}"
        );
    }

    /// Convex erosion certificate: accept a simple inward offset of a
    /// convex hole-free shell; reject over-erosion past the inscribed
    /// radius (empty via winding fallback). Concave/styled cases stay
    /// on the winding path (see bevel pin below).
    #[test]
    fn convex_erosion_certificate_accepts_box_rejects_past_inradius() {
        use super::super::convex_buffer;

        let box_poly = Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            Vec::new(),
        );
        let q = NonZeroU32::new(8).unwrap();
        // Accept: distance 1 < inradius 5 → simple eroded square area 64.
        let accepted = convex_buffer(&box_poly, -1.0, WalkJoinRule::Arc, q).unwrap();
        assert!(
            (accepted.area() - 64.0).abs() < 1e-12,
            "accepted area {}",
            accepted.area()
        );
        assert!(!accepted.is_empty());
        // Reject certificate past inradius: convex_buffer returns None and
        // winding_erosion yields empty (the public buffer path composes both).
        assert!(
            convex_buffer(&box_poly, -6.0, WalkJoinRule::Arc, q).is_none(),
            "over-erosion must reject the certificate"
        );
        let empty =
            winding_erosion(std::slice::from_ref(&box_poly), 6.0, WalkJoinRule::Arc, q).unwrap();
        assert!(empty.is_empty(), "expected empty, got {empty:?}");
        // Concave L-shape must never take the certificate (not convex).
        let l_shape = Polygon::new(
            ring(&[
                (0.0, 0.0),
                (5.0, 0.0),
                (5.0, 2.0),
                (2.0, 2.0),
                (2.0, 5.0),
                (0.0, 5.0),
                (0.0, 0.0),
            ]),
            Vec::new(),
        );
        assert!(
            convex_buffer(&l_shape, -1.2, WalkJoinRule::Bevel, q).is_none(),
            "concave input must not enter the certificate path"
        );
    }

    /// SEMANTICS PIN — styled erosion is the exact monotone offset
    /// construction, NOT GEOS replication: the bevel allowance at a
    /// reflex vertex survives even as an isolated component (GEOS's
    /// depth machinery drops it; the same machinery also manufactures
    /// phantom slivers past the inscribed radius — adjudicated in the
    /// fuzz campaign). Eroding this L by 1.2 annihilates both arms; the
    /// bevel chord across the grown reflex corner keeps the exact
    /// triangle `{x >= 1.2, y >= 1.2, x + y <= 2.8}` (area 0.08), which
    /// shrinks continuously to nothing at `d = 4/3`.
    #[test]
    fn bevel_erosion_keeps_the_exact_corner_allowance() {
        let l_shape = Polygon::new(
            ring(&[
                (0.0, 0.0),
                (5.0, 0.0),
                (5.0, 2.0),
                (2.0, 2.0),
                (2.0, 5.0),
                (0.0, 5.0),
                (0.0, 0.0),
            ]),
            Vec::new(),
        );
        let parts = std::slice::from_ref(&l_shape);
        let kept =
            winding_erosion(parts, 1.2, WalkJoinRule::Bevel, NonZeroU32::new(8).unwrap()).unwrap();
        assert!((kept.area() - 0.08).abs() < 1e-12, "{}", kept.area());
        let gone = winding_erosion(
            parts,
            4.0 / 3.0 + 1e-9,
            WalkJoinRule::Bevel,
            NonZeroU32::new(8).unwrap(),
        )
        .unwrap();
        assert!(gone.is_empty(), "expected empty, got {gone:?}");
    }

    /// Stroke joins follow the same rules: an L-shaped flat-capped stroke
    /// has exact miter (full corner square) and bevel (corner chord)
    /// areas.
    #[test]
    fn stroke_join_rules_are_exact_on_right_angles() {
        let chain: CoordSeq = vec![
            Point::new(0.0, 0.0).unwrap(),
            Point::new(2.0, 0.0).unwrap(),
            Point::new(2.0, 2.0).unwrap(),
        ]
        .into();
        let chains = [&chain];
        let miter = WalkJoinRule::Miter { limit: 5.0 };
        let mitered = winding_stroke(
            &chains,
            0.5,
            BufferCapStyle::Flat,
            miter,
            NonZeroU32::new(8).unwrap(),
        )
        .unwrap();
        assert!((mitered.area() - 4.0).abs() < 1e-12, "{}", mitered.area());
        let bevelled = winding_stroke(
            &chains,
            0.5,
            BufferCapStyle::Flat,
            WalkJoinRule::Bevel,
            NonZeroU32::new(8).unwrap(),
        )
        .unwrap();
        assert!(
            (bevelled.area() - 3.875).abs() < 1e-12,
            "{}",
            bevelled.area()
        );
    }

    /// Puntal and collection routing: a point buffers to the exact
    /// inscribed circle (arc doctrine), a zero-length chain IS that disk
    /// bit-for-bit, negative distances annihilate puntal/lineal input to
    /// the typed empty polygon, and a mixed collection buffers in one
    /// winding pass covering every part.
    #[test]
    fn puntal_and_collection_buffers_route_through_the_engine() {
        let disc = Shape::Point(Point::new(1.0, 1.0).unwrap())
            .buffer(2.0)
            .unwrap();
        // Inscribed 32-gon: area = n/2 * r^2 * sin(tau/n).
        let expected = 16.0 * 4.0 * (std::f64::consts::TAU / 32.0).sin();
        assert!((disc.area() - expected).abs() < 1e-12, "{}", disc.area());

        let zero_chain: CoordSeq =
            vec![Point::new(1.0, 1.0).unwrap(), Point::new(1.0, 1.0).unwrap()].into();
        let stroked = Shape::LineString(LineSeq::try_new(zero_chain).expect("test line is valid"))
            .buffer(2.0)
            .unwrap();
        assert!(stroked.equals_exact(&disc, 0.0, false, false));

        for shape in [
            Shape::Point(Point::new(0.0, 0.0).unwrap()),
            Shape::LineString(
                LineSeq::try_new(CoordSeq::from(vec![
                    Point::new(0.0, 0.0).unwrap(),
                    Point::new(5.0, 0.0).unwrap(),
                ]))
                .expect("test line is valid"),
            ),
        ] {
            let annihilated = shape.buffer(-1.0).unwrap();
            assert!(annihilated.is_empty(), "{annihilated:?}");
        }

        let collection = Shape::GeometryCollection(vec![
            Shape::Point(Point::new(12.0, 3.0).unwrap()),
            Shape::LineString(
                LineSeq::try_new(CoordSeq::from(vec![
                    Point::new(0.0, 0.0).unwrap(),
                    Point::new(4.0, 0.0).unwrap(),
                ]))
                .expect("test line is valid"),
            ),
            Shape::Polygon(Polygon::new(
                ring(&[(6.0, 0.0), (10.0, 0.0), (10.0, 4.0), (6.0, 4.0), (6.0, 0.0)]),
                Vec::new(),
            )),
        ]);
        let buffered = collection.buffer(0.5).unwrap();
        assert!(buffered.validate().is_none(), "{:?}", buffered.validate());
        for part in match &collection {
            Shape::GeometryCollection(parts) => parts,
            _ => unreachable!(),
        } {
            assert!(buffered.covers(part), "missing {part:?}");
        }
        // Negative distance keeps only the eroded polygon: the exact
        // inner square.
        let eroded = collection.buffer(-0.5).unwrap();
        assert!((eroded.area() - 9.0).abs() < 1e-12, "{}", eroded.area());
    }

    /// `buffer(0)` is the d -> 0 limit through the engine: a valid
    /// polygon is returned as-is (holes included), a self-crossing
    /// bowtie keeps its positively-wound lobe (NOT empty, the old geo
    /// answer), and puntal/lineal input annihilates exactly.
    #[test]
    fn zero_distance_buffer_is_the_winding_limit() {
        let holed = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![ring(&[
                (4.0, 4.0),
                (6.0, 4.0),
                (6.0, 6.0),
                (4.0, 6.0),
                (4.0, 4.0),
            ])],
        ));
        let same = holed.buffer(0.0).unwrap();
        assert!((same.area() - 96.0).abs() < 1e-12, "{}", same.area());

        let bowtie = Shape::Polygon(Polygon::new(
            ring(&[(0.0, 0.0), (4.0, 4.0), (4.0, 0.0), (0.0, 4.0), (0.0, 0.0)]),
            Vec::new(),
        ));
        let lobe = bowtie.buffer(0.0).unwrap();
        assert!((lobe.area() - 4.0).abs() < 1e-12, "{}", lobe.area());

        let line = Shape::LineString(
            LineSeq::try_new(CoordSeq::from(vec![
                Point::new(0.0, 0.0).unwrap(),
                Point::new(1.0, 1.0).unwrap(),
            ]))
            .expect("test line is valid"),
        );
        assert!(line.buffer(0.0).unwrap().is_empty());
    }

    #[test]
    fn collinear_linestring_buffers_to_its_extent_stadium() {
        // Every collinear polyline over the same span — open, doubled back,
        // or closed on itself (`A..A`) — covers exactly that span, so its
        // buffer is the segment stadium, NOT a thin self-retracing region.
        let line = |points: &[(f64, f64)]| {
            Shape::LineString(
                LineSeq::try_new(CoordSeq::from(
                    points
                        .iter()
                        .map(|&(x, y)| Point::new(x, y).unwrap())
                        .collect::<Vec<_>>(),
                ))
                .expect("test line is valid"),
            )
        };
        let open = line(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)])
            .buffer(2.0)
            .unwrap();
        let doubled = line(&[(0.0, 0.0), (2.0, 0.0), (1.0, 0.0)])
            .buffer(2.0)
            .unwrap();
        let closed = line(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (0.0, 0.0)])
            .buffer(2.0)
            .unwrap();
        assert!(
            (doubled.area() - open.area()).abs() < 1e-9,
            "doubled {} vs open {}",
            doubled.area(),
            open.area()
        );
        assert!(
            (closed.area() - open.area()).abs() < 1e-9,
            "closed {} vs open {}",
            closed.area(),
            open.area()
        );
        // The true stadium of a length-2 segment at radius 2 is `8 + 4*pi`;
        // the inscribed-arc caps undershoot it slightly (never the ~14 the
        // folded-loop bug produced).
        let stadium = 8.0 + 4.0 * std::f64::consts::PI;
        assert!(
            open.area() > stadium - 0.1 && open.area() <= stadium,
            "{}",
            open.area()
        );
    }
}

#[cfg(test)]
mod consecutive_dedup_tests {
    use super::*;
    use crate::geometry::constructive::offset::{offset_line, offset_source};
    use crate::geometry::constructive::strict_cycle;

    fn point(x: f64, y: f64) -> Point {
        Point::new(x, y).unwrap()
    }

    fn closed_ring(points: &[(f64, f64)]) -> CoordSeq {
        let mut coords: Vec<Point> = points.iter().map(|&(x, y)| point(x, y)).collect();
        coords.push(coords[0]);
        coords.into()
    }

    #[test]
    fn strict_cycle_collapses_consecutive_duplicates() {
        let ring = closed_ring(&[(0.0, 0.0), (0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]);
        let strict = strict_cycle(&ring, false).expect("valid ring");
        assert_eq!(strict.len(), 4);
    }

    #[test]
    fn strict_cycle_rejects_degenerate_after_dedup() {
        let ring = closed_ring(&[(0.0, 0.0), (0.0, 0.0), (1.0, 0.0), (1.0, 0.0), (0.0, 0.0)]);
        assert!(strict_cycle(&ring, false).is_none());
    }

    #[test]
    fn offset_source_dedups_before_close_handling() {
        let line: CoordSeq = vec![
            point(0.0, 0.0),
            point(0.0, 0.0),
            point(1.0, 0.0),
            point(1.0, 0.0),
        ]
        .into();
        assert!(offset_source(&line).is_some());
        assert!(offset_line(&line, 1.0).is_some());
        let collapsed: CoordSeq = vec![point(0.0, 0.0), point(0.0, 0.0)].into();
        assert!(offset_source(&collapsed).is_none());
    }

    #[test]
    fn snap_to_grid_recloses_ring_after_dedup() {
        let ring = Ring::from_trusted_closed(closed_ring(&[
            (0.0, 0.0),
            (0.0, 0.0),
            (2.0, 0.0),
            (2.0, 2.0),
            (0.0, 2.0),
            (0.0, 0.0),
        ]));
        let polygon = Shape::Polygon(Polygon::new(ring, Vec::new()));
        let cleaned = polygon.snap_to_grid((1.0, 1.0), (0.0, 0.0)).unwrap();
        let Shape::Polygon(polygon) = cleaned else {
            panic!("expected polygon");
        };
        assert_eq!(polygon.shell.coords().len(), 5);
        let shell = polygon.shell.coords();
        let first = shell.first().expect("closure");
        let last = shell.last().expect("closure");
        assert_eq!(first.x, last.x);
        assert_eq!(first.y, last.y);
    }
}

#[cfg(test)]
mod subdivide_columns_tests {
    use super::*;
    use crate::geometry::ExpansionBudget;
    use crate::geometry::constructive::{densify_points_budgeted, segmentize_points_budgeted};

    #[test]
    fn densify_xyzm_interpolates_every_ordinate_lane() {
        use std::sync::Arc;

        let line = CoordSeq::from_columns(
            Arc::from([0.0, 4.0]),
            Arc::from([0.0, 0.0]),
            Some(Arc::from([10.0, 30.0])),
            Some(Arc::from([100.0, 300.0])),
        );
        let mut budget = ExpansionBudget::new("densify", "fraction");
        let densified = densify_points_budgeted(&line, 0.25, &mut budget).unwrap();
        assert_eq!(densified.len(), 5);
        assert_eq!(densified.xs(), &[0.0, 1.0, 2.0, 3.0, 4.0]);
        assert_eq!(densified.ys(), &[0.0, 0.0, 0.0, 0.0, 0.0]);
        assert_eq!(densified.zs().unwrap(), &[10.0, 15.0, 20.0, 25.0, 30.0]);
        assert_eq!(densified.ms().unwrap(), &[
            100.0, 150.0, 200.0, 250.0, 300.0
        ]);
    }

    #[test]
    fn segmentize_matches_fraction_dedup_reference() {
        let line: CoordSeq =
            vec![Point::new(0.0, 0.0).unwrap(), Point::new(3.0, 0.0).unwrap()].into();
        let mut budget = ExpansionBudget::new("segmentize", "max_segment_length");
        let segmented = segmentize_points_budgeted(
            &line,
            1.0,
            crate::geometry::SegmentPlacement::Planar,
            &mut budget,
        )
        .unwrap();
        assert_eq!(segmented.len(), 4);
        assert_eq!(segmented.xs(), &[0.0, 1.0, 2.0, 3.0]);
    }

    #[test]
    fn pathological_subdivision_counts_fail_before_allocation() {
        let line: CoordSeq =
            vec![Point::new(0.0, 0.0).unwrap(), Point::new(1.0, 0.0).unwrap()].into();
        let mut segmentize_budget = ExpansionBudget::new("segmentize", "max_segment_length");
        let segmentize = segmentize_points_budgeted(
            &line,
            f64::MIN_POSITIVE,
            crate::geometry::SegmentPlacement::Planar,
            &mut segmentize_budget,
        )
        .unwrap_err();
        assert!(segmentize.to_string().contains("max_segment_length"));
        let mut densify_budget = ExpansionBudget::new("densify", "fraction");
        let densify =
            densify_points_budgeted(&line, f64::MIN_POSITIVE, &mut densify_budget).unwrap_err();
        assert!(densify.to_string().contains("fraction"));
    }
}

#[cfg(test)]
mod expansion_budget_threading_tests {
    use super::*;
    use crate::geometry::{
        BufferCapStyle, BufferJoinStyle, ExpansionBudget, GENERATED_ITEM_LIMIT, SmoothMethod,
    };
    use crate::numeric::Positive;

    fn exhausted(operation: &'static str, parameter: &'static str) -> ExpansionBudget {
        let mut budget = ExpansionBudget::new(operation, parameter);
        budget.add(GENERATED_ITEM_LIMIT).unwrap();
        budget
    }

    fn bent_line() -> Shape {
        Shape::LineString(LineSeq::from_trusted(
            vec![
                Point::new(0.0, 0.0).unwrap(),
                Point::new(1.0, 0.0).unwrap(),
                Point::new(1.0, 1.0).unwrap(),
            ]
            .into(),
        ))
    }

    fn straight_line() -> LineSeq {
        LineSeq::from_trusted(
            vec![Point::new(0.0, 0.0).unwrap(), Point::new(1.0, 0.0).unwrap()].into(),
        )
    }

    #[test]
    fn buffer_and_offset_collection_children_use_the_callers_budget() {
        let style = Positive::try_new("miter_limit", 5.0).unwrap();
        let mut buffer_budget = exhausted("buffer", "quadrant_segments");
        let buffer = Shape::GeometryCollection(vec![Shape::Point(Point::new(0.0, 0.0).unwrap())]);
        buffer
            .buffer_with_style_budgeted(
                1.0,
                BufferCapStyle::Round,
                BufferJoinStyle::Round,
                NonZeroU32::new(1).unwrap(),
                style,
                &mut buffer_budget,
            )
            .unwrap_err();

        let mut offset_budget = exhausted("offset_curve", "quadrant_segments");
        Shape::GeometryCollection(vec![bent_line()])
            .offset_curve_budgeted(
                1.0,
                BufferJoinStyle::Round,
                NonZeroU32::new(1).unwrap(),
                style,
                &mut offset_budget,
            )
            .unwrap_err();
    }

    #[test]
    fn smooth_children_and_triangle_emission_use_the_callers_budget() {
        let mut smooth_budget = exhausted("smooth", "iterations");
        Shape::GeometryCollection(vec![bent_line()])
            .smooth_budgeted(1, SmoothMethod::CatmullRom, true, &mut smooth_budget)
            .unwrap_err();

        let mut triangle_budget = exhausted("triangulate", "method");
        Shape::MultiPoint(
            vec![
                Point::new(0.0, 0.0).unwrap(),
                Point::new(1.0, 0.0).unwrap(),
                Point::new(0.0, 1.0).unwrap(),
            ]
            .into(),
        )
        .delaunay_triangles_budgeted(&mut triangle_budget)
        .unwrap_err();
    }

    #[test]
    fn segmentize_shares_the_callers_budget_across_parts_and_rings() {
        // One generated point per straight part: the first part lands exactly
        // on the cap and the second must observe that same exhausted owner.
        let mut parts_budget = ExpansionBudget::new("segmentize", "max_segment_length");
        parts_budget.add(GENERATED_ITEM_LIMIT - 1).unwrap();
        Shape::MultiLineString(vec![straight_line(), straight_line()])
            .segmentize_budgeted(
                0.5,
                crate::geometry::SegmentPlacement::Planar,
                &mut parts_budget,
            )
            .unwrap_err();
        assert_eq!(parts_budget.used(), GENERATED_ITEM_LIMIT);

        // A unit square contributes twelve inserted vertices at this step. The
        // hole must reuse the shell's owner rather than start another budget.
        let square = |min: f64, max: f64| {
            Ring::closed(vec![
                Point::new(min, min).unwrap(),
                Point::new(max, min).unwrap(),
                Point::new(max, max).unwrap(),
                Point::new(min, max).unwrap(),
                Point::new(min, min).unwrap(),
            ])
            .unwrap()
        };
        let mut rings_budget = ExpansionBudget::new("segmentize", "max_segment_length");
        rings_budget.add(GENERATED_ITEM_LIMIT - 12).unwrap();
        Shape::Polygon(Polygon::new(square(0.0, 1.0), vec![square(0.25, 0.75)]))
            .segmentize_budgeted(
                0.25,
                crate::geometry::SegmentPlacement::Planar,
                &mut rings_budget,
            )
            .unwrap_err();
        assert_eq!(rings_budget.used(), GENERATED_ITEM_LIMIT);
    }
}
