use super::*;
use crate::geometry::{axis_pow2_scale, scaled_residual};
use crate::io::MAX_NESTING;

#[test]
fn buffer_keeps_a_vertex_below_the_removed_input_simplification_tolerance() {
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(1.0, 5e-4),
                Point::new_raw_xy(2.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    let buffered = line
        .buffer_with_style(
            1.0,
            BufferCapStyle::Flat,
            BufferJoinStyle::Bevel,
            NonZeroU32::new(1).unwrap(),
            Positive::try_new("miter_limit", 5.0).unwrap(),
        )
        .unwrap();
    let bounds = buffered.bounds().unwrap();
    assert!(bounds.maxy() > 1.0004, "vertex was removed: {bounds:?}");
}
#[test]
fn offset_curve_unavailability_stays_empty_while_budget_errors_propagate() {
    let overflow = CoordSeq::try_from_points(&[
        Point::new_raw_xy(1.5e308, 0.0),
        Point::new_raw_xy(1.5e308, 1.0),
    ])
    .unwrap();
    let mut overflow_budget = ExpansionBudget::new("offset_curve", "quadrant_segments");
    assert!(
        offset_curve_points(
            &overflow,
            -1.5e308,
            WalkJoinRule::Arc,
            NonZeroU32::new(8).unwrap(),
            &mut overflow_budget,
        )
        .unwrap()
        .is_none()
    );

    let line =
        CoordSeq::try_from_points(&[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(1.0, 0.0)])
            .unwrap();
    let mut budget = ExpansionBudget::new("offset_curve", "quadrant_segments");
    budget.add(crate::geometry::GENERATED_ITEM_LIMIT).unwrap();
    offset_curve_points(
        &line,
        1.0,
        WalkJoinRule::Arc,
        NonZeroU32::new(1).unwrap(),
        &mut budget,
    )
    .unwrap_err();
}
#[test]
fn buffer_accepts_an_edge_whose_raw_delta_overflows_when_offsets_are_finite() {
    // Mutation: restoring the removed raw-delta admission guard rejects this
    // source before the constructive path gets to its representable offsets.
    let start = Point::new_raw_xy(-1.5e308, 0.0);
    let end = Point::new_raw_xy(1.5e308, 0.0);
    assert!(!(end.x - start.x).is_finite());
    let line = Shape::LineString(
        LineSeq::try_new(CoordSeq::try_from_points(&[start, end]).unwrap()).unwrap(),
    );
    let radius = 1.0e300;
    let buffered = line
        .buffer_with_style(
            radius,
            BufferCapStyle::Round,
            BufferJoinStyle::Round,
            NonZeroU32::new(8).unwrap(),
            Positive::try_new("miter_limit", 5.0).unwrap(),
        )
        .expect("finite emitted offsets");

    assert!(!buffered.is_empty());
    let scale = axis_pow2_scale(start.x.abs().max(end.x.abs()).max(radius));
    let end_x = scaled_residual(end.x, start.x, scale);
    let radius = radius * scale;
    buffered
        .try_for_each_point::<()>(|point| {
            assert!(point.x.is_finite() && point.y.is_finite());
            let x = scaled_residual(point.x, start.x, scale);
            let y = scaled_residual(point.y, 0.0, scale);
            let distance = (x - x.clamp(0.0, end_x)).hypot(y);
            assert!(
                distance <= radius + 32.0 * f64::EPSILON,
                "vertex is outside its scaled source buffer: {distance} > {radius}"
            );
            Ok(())
        })
        .expect("all buffered vertices are finite");
}
#[test]
fn buffer_keeps_vertical_diagonal_and_tiny_source_geometry_finite() {
    let cases = [
        (
            Point::new_raw_xy(-1.5e308, -1.5e308),
            Point::new_raw_xy(1.5e308, 1.5e308),
            1.0e300,
        ),
        (
            Point::new_raw_xy(1.5e308, -1.5e308),
            Point::new_raw_xy(1.5e308, 1.5e308),
            1.0e300,
        ),
        (
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0e-200, 2.0e-200),
            1.0e-200,
        ),
    ];
    for (start, end, radius) in cases {
        let line = Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_points(&[start, end]).unwrap()).unwrap(),
        );
        for cap_style in [
            BufferCapStyle::Round,
            BufferCapStyle::Flat,
            BufferCapStyle::Square,
        ] {
            let buffered = line
                .buffer_with_style(
                    radius,
                    cap_style,
                    BufferJoinStyle::Miter,
                    NonZeroU32::new(4).unwrap(),
                    Positive::try_new("miter_limit", 5.0).unwrap(),
                )
                .expect("representable buffer output");
            assert!(!buffered.is_empty());
            buffered
                .try_for_each_point::<()>(|point| {
                    assert!(point.x.is_finite() && point.y.is_finite());
                    Ok(())
                })
                .expect("all emitted coordinates are finite");
        }
    }
}
#[test]
fn buffer_rejects_a_coordinate_that_the_requested_offset_cannot_represent() {
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(1.5e308, 0.0),
                Point::new_raw_xy(1.5e308, 1.0),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    let error = line.buffer_with_style(
        1.5e308,
        BufferCapStyle::Flat,
        BufferJoinStyle::Round,
        NonZeroU32::new(1).unwrap(),
        Positive::try_new("miter_limit", 5.0).unwrap(),
    );
    assert!(
        error
            .expect_err("the right-side offset exceeds binary64")
            .to_string()
            .contains("non-finite")
    );
}

#[cfg(test)]
mod winding_buffer_tests {
    use super::*;
    use crate::geometry::Coordinates as _;
    use crate::geometry::constructive::winding::degenerate::ring_as_open_line;

    fn ring(points: &[(f64, f64)]) -> Ring {
        Ring::from_closed(
            CoordSeq::try_from_points(
                &points
                    .iter()
                    .map(|&(x, y)| Point::new(x, y).unwrap())
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn open_degenerate_ring_preserves_ordinates_without_allocating() {
        let source = CoordSeq::try_from_vecs(
            vec![1.0, 2.0, 3.0, 1.0],
            vec![4.0, 5.0, 6.0, 4.0],
            Some(vec![7.0, 8.0, 9.0, 7.0]),
            Some(vec![10.0, 11.0, 12.0, 10.0]),
        )
        .unwrap();
        let (open, allocations) =
            crate::test_support::allocation_count(|| ring_as_open_line(&source));
        assert_eq!(allocations, 0);
        drop(source);
        assert_eq!(open.xs(), &[1.0, 2.0, 3.0]);
        assert_eq!(open.ys(), &[4.0, 5.0, 6.0]);
        assert_eq!(open.zs().unwrap(), &[7.0, 8.0, 9.0]);
        assert_eq!(open.ms().unwrap(), &[10.0, 11.0, 12.0]);
    }

    #[test]
    fn degenerate_buffer_at_collection_limit_returns_an_error() {
        let degenerate = Shape::MultiPolygon(
            MultiPolygon::from_dense(vec![Polygon::new(
                ring(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (0.0, 0.0)]),
                Vec::new(),
            )])
            .unwrap(),
        );
        let mut nested = degenerate;
        for _ in 0..MAX_NESTING {
            nested = Shape::geometry_collection(vec![nested]).unwrap();
        }
        let error = nested.buffer(1.0).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("geometry collection nesting exceeds the limit of 128")
        );
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
        assert!(sealed.covers(&Shape::Polygon(notched)).unwrap());
    }

    /// Miter and bevel join rules: a square's 90-degree corners are well
    /// within the default limit, so the miter expansion is the exact
    /// larger square and the bevel expansion cuts each corner to a
    /// half-`d²` triangle. Erosion of a convex shape never touches the
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
    /// the corner bisector (continuous in the corner angle), leaving the area
    /// strictly between the bevel and the unlimited miter.
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
    /// construction. The bevel allowance at a reflex vertex survives even
    /// as an isolated component. Eroding this L by 1.2 annihilates both
    /// arms; the bevel chord across the grown reflex corner keeps the exact
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
        let chain = CoordSeq::try_from_points(&[
            Point::new(0.0, 0.0).unwrap(),
            Point::new(2.0, 0.0).unwrap(),
            Point::new(2.0, 2.0).unwrap(),
        ])
        .unwrap();
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
    /// inscribed circle (arc doctrine), a zero-length chain has the same
    /// sampled disk, negative distances annihilate puntal/lineal input to
    /// the typed empty polygon, and a mixed collection buffers in one
    /// winding pass covering every part.
    #[test]
    fn puntal_and_collection_buffers_route_through_the_engine() {
        let disc = Shape::Point(Point::new(1.0, 1.0).unwrap())
            .buffer(2.0)
            .unwrap();
        // Inscribed 32-gon: area = n/2 * r² * sin(tau/n).
        let expected = 16.0 * 4.0 * (std::f64::consts::TAU / 32.0).sin();
        assert!((disc.area() - expected).abs() < 1e-12, "{}", disc.area());

        let zero_chain = CoordSeq::try_from_points(&[
            Point::new(1.0, 1.0).unwrap(),
            Point::new(1.0, 1.0).unwrap(),
        ])
        .unwrap();
        let stroked = Shape::LineString(LineSeq::try_new(zero_chain).expect("test line is valid"))
            .buffer(2.0)
            .unwrap();
        assert!(stroked.equals_exact(&disc, 128.0 * f64::EPSILON, false, false));

        for shape in [
            Shape::Point(Point::new(0.0, 0.0).unwrap()),
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new(0.0, 0.0).unwrap(),
                        Point::new(5.0, 0.0).unwrap(),
                    ])
                    .unwrap(),
                )
                .expect("test line is valid"),
            ),
        ] {
            let annihilated = shape.buffer(-1.0).unwrap();
            assert!(annihilated.is_empty(), "{annihilated:?}");
        }

        let collection = Shape::geometry_collection(vec![
            Shape::Point(Point::new(12.0, 3.0).unwrap()),
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new(0.0, 0.0).unwrap(),
                        Point::new(4.0, 0.0).unwrap(),
                    ])
                    .unwrap(),
                )
                .expect("test line is valid"),
            ),
            Shape::Polygon(Polygon::new(
                ring(&[(6.0, 0.0), (10.0, 0.0), (10.0, 4.0), (6.0, 4.0), (6.0, 0.0)]),
                Vec::new(),
            )),
        ])
        .expect("test collection is valid");
        let buffered = collection.buffer(0.5).unwrap();
        assert!(
            buffered.validate().unwrap().is_none(),
            "{:?}",
            buffered.validate()
        );
        let Shape::GeometryCollection(parts) = &collection else {
            unreachable!("collection fixture produces a geometry collection");
        };
        for part in parts {
            assert!(buffered.covers(part).unwrap(), "missing {part:?}");
        }
        // Negative distance keeps only the eroded polygon: the exact
        // inner square.
        let eroded = collection.buffer(-0.5).unwrap();
        assert!((eroded.area() - 9.0).abs() < 1e-12, "{}", eroded.area());
    }

    #[test]
    fn touching_multipolygon_erosion_matches_its_union() {
        let rectangle = |min_x: f64, max_x: f64| {
            Polygon::new(
                ring(&[
                    (min_x, 0.0),
                    (max_x, 0.0),
                    (max_x, 4.0),
                    (min_x, 4.0),
                    (min_x, 0.0),
                ]),
                Vec::new(),
            )
        };
        let left = rectangle(0.0, 2.0);
        let right = rectangle(2.0, 4.0);
        let multipart = Shape::MultiPolygon(
            MultiPolygon::from_dense(vec![left.clone(), right.clone()]).unwrap(),
        );
        let union = Shape::Polygon(left)
            .union(&Shape::Polygon(right), crate::geometry::Strictness::Lenient)
            .unwrap();

        let eroded = multipart.buffer(-0.5).unwrap();
        let expected = union.buffer(-0.5).unwrap();

        approx::assert_relative_eq!(expected.area(), 9.0, max_relative = 64.0 * f64::EPSILON);
        approx::assert_relative_eq!(eroded.area(), 9.0, max_relative = 64.0 * f64::EPSILON);
        assert!(eroded.equals(&expected).unwrap());
        assert!(multipart.buffer(0.0).unwrap().equals(&union).unwrap());
    }

    #[test]
    fn grouped_zero_support_uses_one_partition_for_nested_signed_sources() {
        let bowtie = Polygon::new(
            ring(&[(0.0, 0.0), (4.0, 4.0), (4.0, 0.0), (0.0, 4.0), (0.0, 0.0)]),
            Vec::new(),
        );
        let collapsed_shell = Polygon::new(
            ring(&[(6.0, 0.0), (8.0, 0.0), (10.0, 0.0), (6.0, 0.0)]),
            Vec::new(),
        );
        let shell = ring(&[
            (10.0, 0.0),
            (14.0, 0.0),
            (14.0, 4.0),
            (10.0, 4.0),
            (10.0, 0.0),
        ]);
        let collapsed_hole = Polygon::new(shell, vec![ring(&[
            (11.0, 2.0),
            (12.0, 2.0),
            (13.0, 2.0),
            (11.0, 2.0),
        ])]);
        let nested = Shape::geometry_collection(vec![
            Shape::Polygon(bowtie),
            Shape::geometry_collection(vec![
                Shape::Polygon(collapsed_shell),
                Shape::Polygon(collapsed_hole),
            ])
            .unwrap(),
        ])
        .unwrap();

        reset_partition_calls();
        let normalized = nested.buffer(0.0).unwrap();

        assert_eq!(partition_calls(), 1);
        approx::assert_relative_eq!(normalized.area(), 20.0, max_relative = 64.0 * f64::EPSILON);
        assert!(!normalized.is_empty());
    }

    #[test]
    fn grouped_union_fills_another_polygon_hole() {
        let frame = Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![ring(&[
                (3.0, 3.0),
                (7.0, 3.0),
                (7.0, 7.0),
                (3.0, 7.0),
                (3.0, 3.0),
            ])],
        );
        let filler = Polygon::new(
            ring(&[(3.0, 3.0), (7.0, 3.0), (7.0, 7.0), (3.0, 7.0), (3.0, 3.0)]),
            Vec::new(),
        );
        let source = Shape::MultiPolygon(MultiPolygon::from_dense(vec![frame, filler]).unwrap());

        let normalized = source.buffer(0.0).unwrap();

        approx::assert_relative_eq!(normalized.area(), 100.0, max_relative = 64.0 * f64::EPSILON);
        assert!(
            normalized
                .covers(&Shape::Point(Point::new(5.0, 5.0).unwrap()))
                .unwrap()
        );
    }

    #[test]
    fn overlapping_separated_and_nested_areal_parts_erode_as_one_support() {
        let rectangle = |min_x: f64, max_x: f64| {
            Polygon::new(
                ring(&[
                    (min_x, 0.0),
                    (max_x, 0.0),
                    (max_x, 4.0),
                    (min_x, 4.0),
                    (min_x, 0.0),
                ]),
                Vec::new(),
            )
        };
        let matches_union = |left: Polygon, right: Polygon| {
            let multipart = Shape::MultiPolygon(
                MultiPolygon::from_dense(vec![left.clone(), right.clone()]).unwrap(),
            );
            let union = Shape::Polygon(left)
                .union(&Shape::Polygon(right), crate::geometry::Strictness::Lenient)
                .unwrap();
            let eroded = multipart.buffer(-0.5).unwrap();
            let expected = union.buffer(-0.5).unwrap();
            assert!(eroded.equals(&expected).unwrap());
            eroded
        };

        approx::assert_relative_eq!(
            matches_union(rectangle(0.0, 3.0), rectangle(2.0, 5.0)).area(),
            12.0,
            max_relative = 64.0 * f64::EPSILON
        );
        let separated = matches_union(rectangle(0.0, 2.0), rectangle(5.0, 7.0));
        approx::assert_relative_eq!(separated.area(), 6.0, max_relative = 64.0 * f64::EPSILON);
        assert!(matches!(separated, Shape::MultiPolygon(_)));

        let nested = Shape::geometry_collection(vec![
            Shape::Polygon(rectangle(0.0, 2.0)),
            Shape::geometry_collection(vec![Shape::Polygon(rectangle(2.0, 4.0))]).unwrap(),
        ])
        .unwrap();
        let nested_eroded = nested.buffer(-0.5).unwrap();
        let nested_union = Shape::Polygon(rectangle(0.0, 2.0))
            .union(
                &Shape::Polygon(rectangle(2.0, 4.0)),
                crate::geometry::Strictness::Lenient,
            )
            .unwrap();
        assert!(
            nested_eroded
                .equals(&nested_union.buffer(-0.5).unwrap())
                .unwrap()
        );
    }

    #[test]
    fn erosion_opens_and_closes_polygon_holes_at_the_union_boundary() {
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

        let open = holed.buffer(-1.0).unwrap();
        assert!(
            !open
                .covers(&Shape::Point(Point::new(5.0, 5.0).unwrap()))
                .unwrap()
        );
        assert!(open.area() > 48.0);
        let closed = holed.buffer(-2.5).unwrap();
        assert!(closed.is_empty());
    }

    #[test]
    fn collapsed_areal_support_is_empty_and_collapsed_holes_are_ignored() {
        let collapsed = Shape::Polygon(Polygon::new(
            ring(&[(0.0, 0.0), (2.0, 0.0), (4.0, 0.0), (0.0, 0.0)]),
            Vec::new(),
        ));
        assert!(collapsed.buffer(0.0).unwrap().is_empty());
        assert!(collapsed.buffer(-1.0).unwrap().is_empty());

        let shell = ring(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]);
        let collapsed_hole = Shape::Polygon(Polygon::new(shell.clone(), vec![ring(&[
            (2.0, 5.0),
            (5.0, 5.0),
            (8.0, 5.0),
            (2.0, 5.0),
        ])]));
        let solid = Shape::Polygon(Polygon::new(shell, Vec::new()));
        assert!(
            collapsed_hole
                .buffer(-1.0)
                .unwrap()
                .equals(&solid.buffer(-1.0).unwrap())
                .unwrap()
        );
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
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new(0.0, 0.0).unwrap(),
                    Point::new(1.0, 1.0).unwrap(),
                ])
                .unwrap(),
            )
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
                LineSeq::try_new(
                    CoordSeq::try_from_points(
                        &points
                            .iter()
                            .map(|&(x, y)| Point::new(x, y).unwrap())
                            .collect::<Vec<_>>(),
                    )
                    .unwrap(),
                )
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
