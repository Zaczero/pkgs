use std::fmt::Write as _;

use super::*;
use crate::geometry::derived;
use crate::geometry::generated_budget::GENERATED_ITEM_LIMIT;

fn line(vertices: &[(f64, f64)]) -> Shape {
    let points: Vec<_> = vertices
        .iter()
        .map(|&(x, y)| Point::new_raw_xy(x, y))
        .collect();
    Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap())
}

fn polygon(vertices: &[(f64, f64)]) -> Shape {
    let points: Vec<_> = vertices.iter().map(|&(x, y)| XY::new(x, y)).collect();
    Shape::Polygon(crate::geometry::Polygon::new(
        crate::geometry::Ring::from_closed(CoordSeq::try_from_xy(&points).unwrap()).unwrap(),
        Vec::new(),
    ))
}

fn adjacent_box_union(count: usize) -> Shape {
    let mut wkt = String::from("MULTIPOLYGON (");
    for index in 0..count {
        if index != 0 {
            wkt.push(',');
        }
        write!(
            wkt,
            "(({} 0,{} 0,{} 1,{} 1,{} 0))",
            index,
            index + 1,
            index + 1,
            index,
            index
        )
        .unwrap();
    }
    wkt.push(')');
    crate::io::parse_wkt(&wkt).expect("valid adjacent-box union")
}

fn adjacent_box_prepared_support(count: usize) -> (PreparedSupport, PreparedSupport) {
    let extent = count as f64;
    let source = PreparedSupport::from_parts(Vec::new(), Vec::new(), vec![vec![vec![
        ExactPoint::from_xy(XY::new(0.0, 0.0)),
        ExactPoint::from_xy(XY::new(extent, 0.0)),
        ExactPoint::from_xy(XY::new(extent, 1.0)),
        ExactPoint::from_xy(XY::new(0.0, 1.0)),
    ]]])
    .unwrap();
    let mut spans = Vec::with_capacity(2 * count + 2);
    for index in 0..count {
        let start = index as f64;
        let end = start + 1.0;
        spans.push(test_span(Segment {
            start: XY::new(start, 0.0),
            end: XY::new(end, 0.0),
        }));
        spans.push(test_span(Segment {
            start: XY::new(start, 1.0),
            end: XY::new(end, 1.0),
        }));
    }
    spans.push(test_span(Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(0.0, 1.0),
    }));
    spans.push(test_span(Segment {
        start: XY::new(extent, 1.0),
        end: XY::new(extent, 0.0),
    }));
    let boundary = PreparedSupport::from_parts(spans, Vec::new(), Vec::new()).unwrap();
    (source, boundary)
}

fn directed_candidate_for(shape: &Shape, tolerance: f64, scale: f64) -> DirectedCandidate {
    let accuracy = Accuracy::new(Some(1.0), 1.0).unwrap();
    let mut context = SearchContext::new_directed(
        &accuracy,
        Rational::from_f64(scale),
        Rational::from_f64(tolerance),
    );
    let source = PreparedSupport::from_shape_with_boundary(shape, &mut context.budget).unwrap();
    let boundary = source.boundary_clone().unwrap();
    run_directed(&source, &boundary, &mut context).unwrap()
}

fn test_span(source: Segment) -> SupportSpan {
    let start = ExactPoint::from_xy(source.start);
    let end = ExactPoint::from_xy(source.end);
    let bounds = FloatBox::from_point(&start)
        .unwrap()
        .include(FloatBox::from_point(&end).unwrap());
    SupportSpan {
        source,
        carrier: ExactCarrier::from_segment(source).unwrap(),
        t_lo: Rational::zero(),
        t_hi: Rational::one(),
        start,
        bounds,
    }
}

fn reduced_point_span_distance_squared(point: &ExactPoint, span: &SupportSpan) -> Result<Rational> {
    let carrier = &span.carrier;
    let px = point.x.try_subtract(&carrier.ax)?;
    let py = point.y.try_subtract(&carrier.ay)?;
    let dot = px
        .try_product(&carrier.dx)?
        .try_add(&py.try_product(&carrier.dy)?)?;
    let low = span.t_lo.try_product(&carrier.norm_squared)?;
    let high = span.t_hi.try_product(&carrier.norm_squared)?;
    let parameter = if carrier.norm_squared.is_zero() || dot.try_cmp(&low)? == Ordering::Less {
        &span.t_lo
    } else if dot.try_cmp(&high)? == Ordering::Greater {
        &span.t_hi
    } else {
        let cross = px
            .try_product(&carrier.dy)?
            .try_subtract(&py.try_product(&carrier.dx)?)?;
        return cross.try_square()?.try_quotient(&carrier.norm_squared);
    };
    let projected = span_point(span, parameter)?;
    let dx = point.x.try_subtract(&projected.x)?;
    let dy = point.y.try_subtract(&projected.y)?;
    dx.try_square()?.try_add(&dy.try_square()?)
}

fn squared(value: f64) -> Rational {
    Rational::from_f64(value)
        .try_square()
        .expect("squared value")
}

fn squared_midpoint(lower: f64, upper: f64) -> Rational {
    let midpoint = ExactDyadic::from_f64(lower)
        .try_add(ExactDyadic::from_f64(upper))
        .expect("midpoint sum")
        .try_product(&ExactDyadic::from_f64(0.5))
        .expect("midpoint");
    Rational::from_dyadic(midpoint)
        .try_square()
        .expect("squared midpoint")
}

#[test]
fn ordinary_l_shape_refinement_work_stays_bounded() {
    let coordinates = [
        (0.0, 0.0),
        (9.0, 0.0),
        (9.0, 1.0),
        (4.0, 1.0),
        (4.0, 8.0),
        (0.0, 8.0),
        (0.0, 0.0),
    ];
    let source = polygon(&coordinates);
    let target = line(&coordinates);
    let limit = 1_000;
    let _guard = limit_refinement_work(limit);
    let result = distance(&source, &target, Some(1e-12), 1.0);
    let work = refinement_work();
    let actual = result.unwrap_or_else(|error| {
        panic!("ordinary L-shape exceeded {limit} refinements at {work}: {error}")
    });
    assert!(
        (actual - 2.0).abs() <= 1e-12,
        "ordinary L-shape returned {actual}, expected 2.0 within 1e-12"
    );
    assert!(
        work <= limit,
        "ordinary L-shape used {work} refinements, limit {limit}"
    );
}

#[test]
fn directed_adjacent_box_refinement_work_scales_with_boundary_spans() {
    for count in [8, 16, 32, 64] {
        let shape = adjacent_box_union(count);
        let tolerance = derived::scale_aware_default_tolerance(&shape).unwrap();
        let (source, boundary) = adjacent_box_prepared_support(count);
        let limit = count * 64;
        let (result, work) = {
            let _guard = limit_refinement_work(limit);
            let accuracy = Accuracy::new(Some(1.0), 1.0).unwrap();
            let mut context = SearchContext::new_directed(
                &accuracy,
                Rational::one(),
                Rational::from_f64(tolerance),
            );
            let result = run_directed(&source, &boundary, &mut context);
            let work = refinement_work();
            (result, work)
        };
        let result = result.unwrap();
        let radius = downward_sqrt(&result.center_squared).unwrap();
        assert_eq!(radius, 0.5);
        assert!(
            work <= limit,
            "adjacent boxes used {work} refinements, limit {limit}"
        );
    }
}

fn regular_polygon(count: usize, offset: (f64, f64)) -> Shape {
    let vertices: Vec<(f64, f64)> = (0..=count)
        .map(|index| {
            let angle = std::f64::consts::TAU * (index % count) as f64 / count as f64;
            (
                100.0 * angle.cos() + offset.0,
                100.0 * angle.sin() + offset.1,
            )
        })
        .collect();
    polygon(&vertices)
}

#[test]
fn filled_translation_refinement_stays_near_the_boundary_maximum() {
    // A convex polygon against its translation by t is exactly |t| apart.
    // Boxes that cannot hold an interior maximum are discarded instead of
    // being refined until their corners fall below it; without that the
    // 64-gon needed over 13,000 refinements, and now about ten.
    for count in [4, 16, 64] {
        let source = regular_polygon(count, (0.0, 0.0));
        let target = regular_polygon(count, (1.0, 0.5));
        let _guard = limit_refinement_work(100);
        let value = distance(&source, &target, None, 1.0).unwrap();
        assert!(
            (value - 1.25_f64.sqrt()).abs() <= 1e-9 * value,
            "{count}: {value}"
        );
    }
}

#[test]
fn filled_interior_maximum_inside_a_target_hole_survives_box_discards() {
    // The hole is a 3-4-5 right triangle, so the square's farthest point
    // from the holed target is the hole's incenter at clearance 1. Its
    // three nearest edges form a closed cycle around it.
    let shell = [(2.0, 2.0), (8.0, 2.0), (8.0, 8.0), (2.0, 8.0), (2.0, 2.0)];
    let hole = [(3.0, 3.0), (3.0, 6.0), (7.0, 3.0), (3.0, 3.0)];
    let ring = |points: &[(f64, f64)]| {
        let points: Vec<_> = points.iter().map(|&(x, y)| XY::new(x, y)).collect();
        crate::geometry::Ring::from_closed(CoordSeq::try_from_xy(&points).unwrap()).unwrap()
    };
    let holed = Shape::Polygon(crate::geometry::Polygon::new(ring(&shell), vec![ring(
        &hole,
    )]));
    let value = distance(&polygon(&shell), &holed, None, 1.0).unwrap();
    assert!((value - 1.0).abs() <= 1e-9, "{value}");
}

#[test]
fn ordinary_hausdorff_keeps_external_line_support_with_filled_area() {
    let area = polygon(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]);
    let external = line(&[(10.0, 0.0), (12.0, 0.0)]);
    let mixed = Shape::geometry_collection(vec![area, external]).unwrap();
    let target = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    assert!((distance(&mixed, &target, Some(1e-9), 1.0).unwrap() - 12.0).abs() <= 1e-9);

    let mut budget = ExpansionBudget::new("hausdorff", "support");
    let prepared = PreparedSupport::from_shape(&mixed, &mut budget).unwrap();
    assert!(prepared.initial_patches().unwrap().iter().any(|patch| {
        matches!(
            patch,
            SourcePatch::Span { span, .. }
                if prepared.spans[*span].source.start == XY::new(10.0, 0.0)
                    && prepared.spans[*span].source.end == XY::new(12.0, 0.0)
        )
    }));
}

#[test]
fn misaligned_metric_boundary_incidence_is_rejected_before_consumption() {
    for support in [
        ExactMetricSupport {
            spans: Vec::new(),
            points: Vec::new(),
            faces: Vec::new(),
            boundary_spans: Some(vec![true]),
            boundary_points: None,
        },
        ExactMetricSupport {
            spans: Vec::new(),
            points: Vec::new(),
            faces: Vec::new(),
            boundary_spans: None,
            boundary_points: Some(vec![true]),
        },
    ] {
        let error = PreparedSupport::from_metric_support(support).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(crate::geometry::GeometryErrorKind::Invalid(_))
        ));
    }
}

fn assert_sqrt_certificate(name: &str, squared: &Rational) -> SqrtInterval {
    let interval = SqrtInterval::from_squared(squared).expect("sqrt interval");
    let lower = Rational::from_dyadic(interval.lower.try_clone().expect("lower clone"))
        .try_square()
        .expect("lower square");
    assert_ne!(
        lower.try_cmp(squared).expect("lower comparison"),
        Ordering::Greater,
        "{name}: lower endpoint is above the exact root"
    );
    let lower_value = dyadic_as_f64(&interval.lower).expect("lower binary64");
    if let Some(upper) = &interval.upper {
        let upper_squared = Rational::from_dyadic(upper.try_clone().expect("upper clone"))
            .try_square()
            .expect("upper square");
        assert_ne!(
            upper_squared.try_cmp(squared).expect("upper comparison"),
            Ordering::Less,
            "{name}: upper endpoint is below the exact root"
        );
        let upper_value = dyadic_as_f64(upper).expect("upper binary64");
        if lower_value.to_bits() != upper_value.to_bits() {
            assert_eq!(
                upper_value.to_bits() - lower_value.to_bits(),
                1,
                "{name}: finite endpoints must be adjacent"
            );
        }
    } else {
        assert_eq!(
            lower_value.to_bits(),
            f64::MAX.to_bits(),
            "{name}: an overflow interval must start at f64::MAX"
        );
        let upper = interval
            .upper_rational()
            .expect("overflow upper")
            .expect("finite exact overflow upper");
        let upper_squared = upper.try_square().expect("overflow upper square");
        assert_ne!(
            upper_squared
                .try_cmp(squared)
                .expect("overflow upper comparison"),
            Ordering::Less,
            "{name}: exact overflow upper is below the root"
        );
    }
    interval
}

#[test]
fn verified_sqrt_brackets_cover_irrational_exact_and_boundary_values() {
    reset_sqrt_stats();
    let minimum_subnormal = f64::from_bits(1);
    let largest_subnormal = f64::from_bits(0x000F_FFFF_FFFF_FFFF);
    let minimum_normal = f64::from_bits(0x0010_0000_0000_0000);
    let maximum = f64::MAX;
    let below_minimum = ExactDyadic::from_f64(minimum_subnormal)
        .try_product(&ExactDyadic::from_f64(0.5))
        .unwrap();
    let underflowing_squared = squared(1.5e-162);
    let cases = [
        ("irrational two", Rational::from_f64(2.0)),
        ("exact square", squared(3.0)),
        (
            "normal rounding midpoint",
            squared_midpoint(1.0, 1.0_f64.next_up()),
        ),
        (
            "minimum subnormal rounding midpoint",
            squared_midpoint(minimum_subnormal, f64::from_bits(2)),
        ),
        (
            "subnormal normal rounding midpoint",
            squared_midpoint(largest_subnormal, minimum_normal),
        ),
        ("minimum subnormal root", squared(minimum_subnormal)),
        ("minimum normal root", squared(minimum_normal)),
        ("rounded squared underflow", underflowing_squared),
        (
            "root below minimum subnormal",
            Rational::from_dyadic(below_minimum).try_square().unwrap(),
        ),
        ("maximum square", squared(maximum)),
        (
            "root just beyond maximum",
            Rational::from_dyadic(
                ExactDyadic::from_f64(maximum)
                    .try_add(ExactDyadic::from_f64(2.0_f64.powi(969)))
                    .unwrap(),
            )
            .try_square()
            .unwrap(),
        ),
        (
            "root at overflow rounding midpoint",
            Rational::from_dyadic(
                ExactDyadic::from_f64(maximum)
                    .try_add(ExactDyadic::from_f64(2.0_f64.powi(970)))
                    .unwrap(),
            )
            .try_square()
            .unwrap(),
        ),
    ];
    for (name, value) in &cases {
        assert_sqrt_certificate(name, value);
    }
    assert!(
        sqrt_stats().fast_admissions > 0,
        "ordinary cases should admit the verified candidate"
    );
    assert!(
        sqrt_stats().fallbacks > 0,
        "underflow and overflow cases should use the exact fallback"
    );
}

#[test]
fn endpoint_verification_rejects_a_reversed_sqrt_comparison() {
    reset_sqrt_stats();
    let squared = Rational::from_f64(2.0);
    let proposal = 2.0_f64.sqrt();
    assert_eq!(
        squared
            .try_cmp(&square_f64(proposal).expect("proposal square"))
            .unwrap(),
        Ordering::Less,
        "the deliberately chosen libm proposal is above the exact root"
    );
    assert_sqrt_certificate("reversed-comparison negative control", &squared);
    assert_eq!(sqrt_stats().fast_admissions, 1);
}

#[test]
fn verified_sqrt_candidate_matches_complete_rounding_at_extrema() {
    let minimum_subnormal = f64::from_bits(1);
    let largest_subnormal = f64::from_bits(0x000F_FFFF_FFFF_FFFF);
    let minimum_normal = f64::from_bits(0x0010_0000_0000_0000);
    let maximum = f64::MAX;
    let below_minimum = ExactDyadic::from_f64(minimum_subnormal)
        .try_product(&ExactDyadic::from_f64(0.5))
        .unwrap();
    let cases = [
        Rational::zero(),
        Rational::from_f64(2.0),
        Rational::from_f64(3.0),
        Rational::from_f64(minimum_subnormal),
        Rational::from_f64(minimum_normal),
        Rational::from_f64(maximum),
        squared(minimum_subnormal),
        squared(minimum_normal),
        squared_midpoint(1.0, 1.0_f64.next_up()),
        squared_midpoint(minimum_subnormal, f64::from_bits(2)),
        squared_midpoint(largest_subnormal, minimum_normal),
        Rational::from_dyadic(below_minimum).try_square().unwrap(),
        squared(maximum),
        Rational::from_dyadic(
            ExactDyadic::from_f64(maximum)
                .try_add(ExactDyadic::from_f64(2.0_f64.powi(969)))
                .unwrap(),
        )
        .try_square()
        .unwrap(),
        Rational::from_dyadic(
            ExactDyadic::from_f64(maximum)
                .try_add(ExactDyadic::from_f64(2.0_f64.powi(970)))
                .unwrap(),
        )
        .try_square()
        .unwrap(),
    ];
    for value in &cases {
        let actual = SqrtInterval::candidate_from_squared(value).unwrap();
        let expected = value.round_sqrt().unwrap();
        if expected.is_infinite() {
            assert_eq!(
                actual.map(f64::to_bits),
                Some(f64::MAX.to_bits()),
                "a finite tolerance candidate must retain f64::MAX above its range"
            );
        } else {
            assert_eq!(
                actual.map(f64::to_bits),
                Some(expected.to_bits()),
                "candidate differs from complete exact rounding"
            );
        }
    }
}

#[test]
fn fast_and_complete_sqrt_paths_have_identical_bounded_results() {
    let origin = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    let diagonal = Shape::Point(Point::new_raw_xy(1.0, 1.0));
    let source_line = line(&[(0.0, 0.0), (2.0, 0.0)]);
    let target_line = line(&[(0.0, 1.0), (2.0, 1.0)]);
    let triangle = polygon(&[(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)]);
    let tiny = Shape::Point(Point::new_raw_xy(1.5e-162, 0.0));
    let length = 10.0_f64.powi(155);
    let height = 10.0_f64.powi(-155);
    let reciprocal_baseline = line(&[(0.0, 0.0), (length, 0.0)]);
    let reciprocal_bend = line(&[
        (0.0, height),
        (length / 2.0, 2.0 * height),
        (length, height),
    ]);
    let cases = [
        (&origin, &diagonal, 1e-6, 1.0),
        (&source_line, &target_line, 1e-6, 1.0),
        (&triangle, &source_line, 1e-6, 1.0),
        (&origin, &tiny, 1e-174, 1.0),
        (
            &reciprocal_baseline,
            &reciprocal_bend,
            2.0 * height * 1e-12,
            1.0,
        ),
    ];

    reset_sqrt_stats();
    let fast: Vec<_> = cases
        .iter()
        .map(|(left, right, error, scale)| {
            let _path = force_exact_sqrt_path(false);
            distance(left, right, Some(*error), *scale).expect("fast bounded result")
        })
        .collect();
    let fast_stats = sqrt_stats();
    assert!(fast_stats.fast_admissions > 0);
    assert!(fast_stats.fallbacks > 0);

    reset_sqrt_stats();
    let complete: Vec<_> = cases
        .iter()
        .map(|(left, right, error, scale)| {
            let _path = force_exact_sqrt_path(true);
            distance(left, right, Some(*error), *scale).expect("complete bounded result")
        })
        .collect();
    let complete_stats = sqrt_stats();
    assert_eq!(complete_stats.fast_admissions, 0);
    assert!(complete_stats.fallbacks > 0);
    assert_eq!(
        fast.iter().map(|value| value.to_bits()).collect::<Vec<_>>(),
        complete
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>()
    );
}

#[test]
fn queue_enclosure_keeps_unbounded_patches_and_later_witnesses() {
    let accuracy = Accuracy::new(Some(1e-6), 1.0).unwrap();
    let mut context = SearchContext::new(&accuracy);
    let mut queue = PatchQueue::new(false);
    let patch = || {
        SourcePatch::Box(FloatBox {
            min_x: 0.0,
            max_x: 1.0,
            min_y: 0.0,
            max_y: 1.0,
        })
    };
    queue
        .push(PatchRecord {
            patch: patch(),
            upper: Some(Rational::one()),
        })
        .unwrap();
    context.update_lower(Rational::from_f64(4.0)).unwrap();
    let upper = queue_upper(&queue, &context).unwrap().unwrap();
    assert_ne!(
        upper.try_cmp(&Rational::from_f64(2.0)).unwrap(),
        Ordering::Less
    );
    queue
        .push(PatchRecord {
            patch: patch(),
            upper: None,
        })
        .unwrap();
    assert!(queue_upper(&queue, &context).unwrap().is_none());
}

#[test]
fn box_radius_covers_unrepresentable_midpoint_corners() {
    let source = PreparedSupport::from_line_columns(&[0.0], &[0.0]).unwrap();
    let patch = SourcePatch::Box(FloatBox {
        min_x: 1.0,
        max_x: 1.0_f64.next_up(),
        min_y: 0.0,
        max_y: f64::from_bits(1),
    });
    let (center, radius) = patch.center_and_radius_squared(&source).unwrap();
    for corner in patch.corners(&source).unwrap() {
        let dx = corner.x.try_subtract(&center.x).unwrap();
        let dy = corner.y.try_subtract(&center.y).unwrap();
        let squared = dx
            .try_square()
            .unwrap()
            .try_add(&dy.try_square().unwrap())
            .unwrap();
        assert_ne!(squared.try_cmp(&radius).unwrap(), Ordering::Greater);
    }
}

/// Exact squared distance from a point to a box, the oracle for the
/// binary64 box bound.
fn aabb_lower_squared(point: &ExactPoint, bounds: FloatBox) -> Result<Rational> {
    let min_x = Rational::from_f64(bounds.min_x);
    let max_x = Rational::from_f64(bounds.max_x);
    let min_y = Rational::from_f64(bounds.min_y);
    let max_y = Rational::from_f64(bounds.max_y);
    let zero = Rational::zero();
    let dx = if point.x.try_cmp(&min_x)? == Ordering::Less {
        min_x.try_subtract(&point.x)?
    } else if point.x.try_cmp(&max_x)? == Ordering::Greater {
        point.x.try_subtract(&max_x)?
    } else {
        zero.try_clone()?
    };
    let dy = if point.y.try_cmp(&min_y)? == Ordering::Less {
        min_y.try_subtract(&point.y)?
    } else if point.y.try_cmp(&max_y)? == Ordering::Greater {
        point.y.try_subtract(&max_y)?
    } else {
        zero
    };
    dx.try_square()?.try_add(&dy.try_square()?)
}

#[test]
fn floating_box_pruning_stays_below_exact_distance() {
    let target = FloatBox {
        min_x: -2.0,
        max_x: -1.0,
        min_y: 1.0,
        max_y: 2.0,
    };
    for (x, y) in [
        (0.0, 0.0),
        (0.1, -0.3),
        (f64::from_bits(1), 0.0),
        (f64::MAX, -f64::MAX),
    ] {
        let point = ExactPoint::from_xy(XY::new(x, y));
        let lower = box_distance_lower(FloatBox::from_point(&point).unwrap(), target);
        let exact = aabb_lower_squared(&point, target).unwrap();
        assert_ne!(
            Rational::from_f64(lower).try_cmp(&exact).unwrap(),
            Ordering::Greater,
        );
    }
}

#[test]
fn empty_area_patch_cannot_invent_a_source_witness() {
    let shape = polygon(&[(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)]);
    let accuracy = Accuracy::new(Some(1e-6), 1.0).unwrap();
    let mut context = SearchContext::new(&accuracy);
    let source = PreparedSupport::from_shape(&shape, &mut context.budget).unwrap();
    let target = PreparedSupport::from_line_columns(&[1.0], &[1.0]).unwrap();
    let patch = SourcePatch::Box(FloatBox {
        min_x: 10.0,
        max_x: 11.0,
        min_y: 10.0,
        max_y: 11.0,
    });
    assert!(
        build_patch(patch, &source, &target, &mut context)
            .unwrap()
            .is_none()
    );
    assert!(context.lower_squared.is_none());
    let span = SourcePatch::Span {
        span: 0,
        lo: Rational::zero(),
        hi: Rational::one(),
    };
    assert!(
        !span
            .witness_is_valid(&source, &ExactPoint::from_xy(XY::new(10.0, 10.0)))
            .unwrap()
    );
}

#[test]
fn subdivision_uses_the_shared_generated_work_budget() {
    let mut budget = ExpansionBudget::new("hausdorff_distance", "max_error");
    budget.add(GENERATED_ITEM_LIMIT).unwrap();
    let patch = SourcePatch::Box(FloatBox {
        min_x: 0.0,
        max_x: 1.0,
        min_y: 0.0,
        max_y: 1.0,
    });
    let error = patch.split(&mut budget).unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(
            crate::geometry::GeometryErrorKind::GeneratedOutputTooLarge { .. }
        )
    ));
}

#[test]
fn box_subdivision_continues_on_the_representable_axis() {
    let mut budget = ExpansionBudget::new("hausdorff_distance", "max_error");
    let min_x = 1.0_f64;
    let max_x = min_x.next_up();
    let children = SourcePatch::Box(FloatBox {
        min_x,
        max_x,
        min_y: 0.0,
        max_y: 2.0,
    })
    .split(&mut budget)
    .unwrap()
    .unwrap();
    assert_eq!(children.len(), 2);
    for (index, child) in children.into_iter().enumerate() {
        let SourcePatch::Box(bounds) = child else {
            panic!("box split produced a span");
        };
        assert_eq!((bounds.min_x, bounds.max_x), (min_x, max_x));
        assert_eq!(
            (bounds.min_y, bounds.max_y),
            if index == 0 { (0.0, 1.0) } else { (1.0, 2.0) }
        );
    }
}

#[test]
fn longest_axis_box_split_handles_extreme_equal_and_unrepresentable_axes() {
    let mut ordinary_budget = ExpansionBudget::new("hausdorff_distance", "max_error");
    let ordinary_children = SourcePatch::Box(FloatBox {
        min_x: 0.0,
        max_x: 1.0,
        min_y: 0.0,
        max_y: 1.0,
    })
    .split(&mut ordinary_budget)
    .unwrap()
    .unwrap();
    assert_eq!(ordinary_children.len(), 4);
    assert_eq!(ordinary_budget.used(), 4);

    let mut budget = ExpansionBudget::new("maximum_inscribed_circle", "tolerance");
    let children = SourcePatch::Box(FloatBox {
        min_x: -f64::MAX,
        max_x: f64::MAX,
        min_y: -1.0,
        max_y: 1.0,
    })
    .split_with_strategy(&mut budget, BoxSplitStrategy::LongestAxis)
    .unwrap()
    .unwrap();
    assert_eq!(children.len(), 2);
    assert_eq!(budget.used(), 2);
    for child in children {
        let SourcePatch::Box(bounds) = child else {
            panic!("longest-axis split produced a non-box child");
        };
        assert_eq!((bounds.min_y, bounds.max_y), (-1.0, 1.0));
        assert!(bounds.min_x < bounds.max_x);
    }

    let mut budget = ExpansionBudget::new("maximum_inscribed_circle", "tolerance");
    let children = SourcePatch::Box(FloatBox {
        min_x: 0.0,
        max_x: 1.0,
        min_y: 0.0,
        max_y: 1.0,
    })
    .split_with_strategy(&mut budget, BoxSplitStrategy::LongestAxis)
    .unwrap()
    .unwrap();
    assert_eq!(children.len(), 2);
    assert_eq!(budget.used(), 2);
    let first = children.into_iter().next().unwrap();
    let children = first
        .split_with_strategy(&mut budget, BoxSplitStrategy::LongestAxis)
        .unwrap()
        .unwrap();
    assert_eq!(children.len(), 2);
    assert_eq!(budget.used(), 4);
    for child in children {
        let SourcePatch::Box(bounds) = child else {
            panic!("longest-axis split produced a non-box child");
        };
        assert!(bounds.min_y < bounds.max_y);
        assert!(bounds.max_x - bounds.min_x < 1.0);
    }

    let y_min = 1.0e-10_f64;
    let y_max = y_min.next_up().next_up();
    let mut budget = ExpansionBudget::new("maximum_inscribed_circle", "tolerance");
    let children = SourcePatch::Box(FloatBox {
        min_x: 1.0,
        max_x: 1.0_f64.next_up(),
        min_y: y_min,
        max_y: y_max,
    })
    .split_with_strategy(&mut budget, BoxSplitStrategy::LongestAxis)
    .unwrap()
    .unwrap();
    assert_eq!(children.len(), 2);
    assert_eq!(budget.used(), 2);
    for child in children {
        let SourcePatch::Box(bounds) = child else {
            panic!("longest-axis fallback produced a non-box child");
        };
        assert_eq!((bounds.min_x, bounds.max_x), (1.0, 1.0_f64.next_up()));
        assert!(bounds.min_y < bounds.max_y);
    }
}

#[test]
fn parallel_strip_rejects_outside_points_with_reversed_carriers() {
    let target =
        PreparedSupport::from_line_columns(&[0.0, 4.0, 4.0, 0.0], &[0.0, 0.0, 2.0, 2.0]).unwrap();
    let source = PreparedSupport::from_line_columns(&[2.0], &[1.0]).unwrap();
    let accuracy = Accuracy::new(Some(1e-6), 1.0).unwrap();
    let context = SearchContext::new(&accuracy);
    for (y, inside) in [(1.0, true), (3.0, false), (-1.0, false)] {
        let patch = SourcePatch::Box(FloatBox {
            min_x: 2.0,
            max_x: 2.0,
            min_y: y,
            max_y: y,
        });
        let upper = parallel_strip_upper(
            &patch,
            &source,
            &target,
            &[PrimitiveId::Span(0), PrimitiveId::Span(2)],
            &context,
        )
        .unwrap();
        if inside {
            assert_eq!(
                upper.unwrap().try_cmp(&Rational::one()).unwrap(),
                Ordering::Equal
            );
        } else {
            assert!(upper.is_none());
        }
    }
}

#[test]
fn supporting_line_lower_stays_below_exact_segment_distance() {
    let subnormal = f64::from_bits(1);
    let cases = [
        (
            Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(3.0, 4.0),
            },
            XY::new(2.0, -1.0),
        ),
        (
            Segment {
                start: XY::new(1.0e200, -1.0e200),
                end: XY::new(2.0e200, 1.0e200),
            },
            XY::new(1.5e200, 0.5e200),
        ),
        (
            Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(8.0 * subnormal, 0.0),
            },
            XY::new(4.0 * subnormal, 8.0 * subnormal),
        ),
        (
            Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(1.0, 1.0),
            },
            XY::new(0.5, 0.5 + 2.0_f64.powi(-52)),
        ),
        (
            Segment {
                start: XY::new(4.0, 4.0),
                end: XY::new(-2.0, -1.0),
            },
            XY::new(0.0, 5.0),
        ),
    ];
    let third = Rational::new(ExactDyadic::from_f64(1.0), ExactDyadic::from_f64(3.0)).unwrap();

    for (source, query) in cases {
        let span = test_span(source);
        let stored = ExactPoint::from_xy(query);
        let subdivision = span_point(&span, &third).unwrap();
        for point in [stored, subdivision] {
            let lower = Rational::from_f64(supporting_line_lower_squared(
                FloatBox::from_point(&point).unwrap(),
                &span,
            ));
            let exact = point_span_distance_squared(&point, &span)
                .unwrap()
                .to_rational()
                .unwrap();
            assert_ne!(lower.try_cmp(&exact).unwrap(), Ordering::Greater);
        }
    }
}

#[test]
fn line_filter_skips_only_after_an_exact_best_is_known() {
    let target = PreparedSupport::from_parts(
        vec![
            test_span(Segment {
                start: XY::new(0.0, 1.0),
                end: XY::new(10.0, 1.0),
            }),
            test_span(Segment {
                start: XY::new(0.0, 10.0),
                end: XY::new(10.0, 0.0),
            }),
        ],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let point = ExactPoint::from_xy(XY::new(5.0, 0.0));
    let run = |enabled| {
        let _guard = set_line_filter_enabled(enabled);
        reset_exact_span_evaluations();
        let result = target.distance_squared(&point).unwrap();
        (result, exact_span_evaluations())
    };

    let (without_filter, without_filter_evaluations) = run(false);
    let (with_filter, with_filter_evaluations) = run(true);
    assert_eq!(
        with_filter.try_cmp(&without_filter).unwrap(),
        Ordering::Equal
    );
    assert_eq!(without_filter_evaluations, 2);
    assert_eq!(with_filter_evaluations, 1);
}

#[test]
fn interval_span_bounds_enclose_the_exact_distance_over_the_point_box() {
    // Every regime: beyond either end, over the interior, straddling an
    // end normal, near-contact, and wide point boxes. A bound that fails
    // to enclose would let the solver prune the true maximum.
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (state >> 11) as f64 / (1_u64 << 53) as f64 - 0.5
    };
    let mut checked = 0;
    for scale in [1e-6, 1.0, 1e6] {
        for _ in 0..2_000 {
            let (ax, ay, bx, by) = (
                next() * scale,
                next() * scale,
                next() * scale,
                next() * scale,
            );
            let support = PreparedSupport::from_line_columns(&[ax, bx], &[ay, by]).unwrap();
            let (px, py) = (next() * 3.0 * scale, next() * 3.0 * scale);
            let width = [0.0, 1e-12 * scale, 1e-3 * scale][checked % 3];
            let bounds = FloatBox {
                min_x: px,
                max_x: px + width,
                min_y: py,
                max_y: py + width,
            };
            let Some((lower, upper)) = point_span_squared_bounds(bounds, &support.spans[0]) else {
                continue;
            };
            for (x, y) in bounds.corners() {
                let exact = support
                    .primitive_distance_squared(
                        PrimitiveId::Span(0),
                        &ExactPoint::from_xy(XY::new(x, y)),
                    )
                    .unwrap()
                    .to_rational()
                    .unwrap();
                assert_ne!(
                    Rational::from_f64(lower).try_cmp(&exact).unwrap(),
                    Ordering::Greater
                );
                assert_ne!(
                    Rational::from_f64(upper).try_cmp(&exact).unwrap(),
                    Ordering::Less
                );
            }
            checked += 1;
        }
    }
    assert!(checked > 5_000, "{checked}");

    // An exactly representable cross product whose square is not: round
    // to nearest lands below the exact squared distance, so only the
    // outward step keeps the upper bound above it, over the interior
    // and past an end.
    let support = PreparedSupport::from_line_columns(&[0.0, 1.0], &[0.0, 0.0]).unwrap();
    let offset = 1.0 + f64::EPSILON;
    for (x, y) in [(0.5, offset), (-offset, offset)] {
        let point = ExactPoint::from_xy(XY::new(x, y));
        let (lower, upper) =
            point_span_squared_bounds(FloatBox::from_point(&point).unwrap(), &support.spans[0])
                .expect("a decided regime");
        let exact = support
            .primitive_distance_squared(PrimitiveId::Span(0), &point)
            .unwrap()
            .to_rational()
            .unwrap();
        assert_ne!(
            Rational::from_f64(lower).try_cmp(&exact).unwrap(),
            Ordering::Greater
        );
        assert_ne!(
            Rational::from_f64(upper).try_cmp(&exact).unwrap(),
            Ordering::Less
        );
    }
}

#[test]
fn unreduced_distances_match_reduced_point_and_span_references() {
    let point_support = PreparedSupport::from_line_columns(&[0.0], &[0.0]).unwrap();
    for point in [
        ExactPoint::from_xy(XY::new(3.0, 4.0)),
        ExactPoint::from_xy(XY::new(2.0_f64.powi(500), -2.0_f64.powi(500))),
        ExactPoint::from_xy(XY::new(f64::from_bits(1), -f64::from_bits(2))),
    ] {
        let actual = point_support
            .primitive_distance_squared(PrimitiveId::Point(0), &point)
            .unwrap();
        let dx = point.x.try_subtract(&point_support.points[0].x).unwrap();
        let dy = point.y.try_subtract(&point_support.points[0].y).unwrap();
        let expected = dx
            .try_square()
            .unwrap()
            .try_add(&dy.try_square().unwrap())
            .unwrap();
        assert_eq!(actual.try_cmp_rational(&expected).unwrap(), Ordering::Equal);
    }

    let cases = [
        ([0.0, 3.0], [0.0, 4.0], (0.0, -1.0)),
        ([0.0, 3.0], [0.0, 4.0], (1.0, 3.0)),
        ([3.0, 0.0], [4.0, 0.0], (1.0, 3.0)),
        (
            [0.0, 2.0_f64.powi(500)],
            [0.0, 0.0],
            (0.0, 2.0_f64.powi(500)),
        ),
        (
            [0.0, f64::from_bits(2)],
            [0.0, 0.0],
            (0.0, f64::from_bits(1)),
        ),
    ];
    for (xs, ys, point) in cases {
        let support = PreparedSupport::from_line_columns(&xs, &ys).unwrap();
        let point = ExactPoint::from_xy(XY::new(point.0, point.1));
        let actual = point_span_distance_squared(&point, &support.spans[0]).unwrap();
        let expected = reduced_point_span_distance_squared(&point, &support.spans[0]).unwrap();
        assert_eq!(actual.try_cmp_rational(&expected).unwrap(), Ordering::Equal);
    }

    let mut subspan = PreparedSupport::from_line_columns(&[0.0, 6.0], &[0.0, 0.0]).unwrap();
    subspan.spans[0].t_lo =
        Rational::new(ExactDyadic::from_f64(1.0), ExactDyadic::from_f64(3.0)).unwrap();
    subspan.spans[0].t_hi =
        Rational::new(ExactDyadic::from_f64(2.0), ExactDyadic::from_f64(3.0)).unwrap();
    let point = ExactPoint::from_xy(XY::new(0.0, 0.0));
    let actual = point_span_distance_squared(&point, &subspan.spans[0]).unwrap();
    let expected = reduced_point_span_distance_squared(&point, &subspan.spans[0]).unwrap();
    assert_eq!(actual.try_cmp_rational(&expected).unwrap(), Ordering::Equal);
}

#[test]
fn selected_span_clamps_oblique_projection() {
    let mut support = PreparedSupport::from_line_columns(&[0.0, 3.0], &[0.0, 4.0]).unwrap();
    for (lo, hi, x, y, expected) in [
        (0.0, 1.0, 1.0, 3.0, 1.0),
        (0.0, 1.0, -3.0, -4.0, 25.0),
        (0.0, 1.0, 6.0, 8.0, 25.0),
        (0.25, 0.75, 0.0, 0.0, 1.5625),
        (0.25, 0.75, 3.0, 4.0, 1.5625),
    ] {
        support.spans[0].t_lo = Rational::from_f64(lo);
        support.spans[0].t_hi = Rational::from_f64(hi);
        let value =
            point_span_distance_squared(&ExactPoint::from_xy(XY::new(x, y)), &support.spans[0])
                .unwrap();
        assert_eq!(
            value
                .try_cmp_rational(&Rational::from_f64(expected))
                .unwrap(),
            Ordering::Equal
        );
    }
}

#[test]
fn filled_target_membership_and_holes_remain_selected() {
    let ring = |points: &[XY]| {
        crate::geometry::Ring::from_closed(CoordSeq::try_from_xy(points).unwrap()).unwrap()
    };
    let shell = [
        XY::new(0.0, 0.0),
        XY::new(4.0, 0.0),
        XY::new(4.0, 4.0),
        XY::new(0.0, 4.0),
        XY::new(0.0, 0.0),
    ];
    let target = Shape::Polygon(crate::geometry::Polygon::new(ring(&shell), vec![
        ring(&[
            XY::new(1.0, 1.0),
            XY::new(3.0, 1.0),
            XY::new(3.0, 3.0),
            XY::new(1.0, 3.0),
            XY::new(1.0, 1.0),
        ]),
        ring(&[
            XY::new(6.0, 6.0),
            XY::new(7.0, 6.0),
            XY::new(7.0, 7.0),
            XY::new(6.0, 7.0),
            XY::new(6.0, 6.0),
        ]),
    ]));
    let mut budget = ExpansionBudget::new("test", "support");
    let support = PreparedSupport::from_shape(&target, &mut budget).unwrap();
    assert!(
        support
            .distance_squared(&ExactPoint::from_xy(XY::new(0.5, 0.5)))
            .unwrap()
            .is_zero()
    );
    assert_eq!(
        support
            .distance_squared(&ExactPoint::from_xy(XY::new(2.0, 2.0)))
            .unwrap()
            .round()
            .unwrap(),
        1.0
    );
    let filled = Shape::Polygon(crate::geometry::Polygon::new(ring(&shell), Vec::new()));
    assert!((distance(&filled, &target, Some(1e-6), 1.0).unwrap() - 1.0).abs() <= 1e-6);
}

#[test]
fn directed_inradius_retains_the_rounded_center_and_global_enclosure() {
    let shape = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
    let result = directed_candidate_for(&shape, 1e-6, 1.0);
    assert_eq!(result.proof.witness_squared.round().unwrap(), 4.0);
    assert_eq!((result.center.x, result.center.y), (2.0, 2.0));
    assert_eq!(result.center_squared.round().unwrap(), 4.0);
    assert_eq!(result.proof.certified_squared.round().unwrap(), 4.0);
    assert_ne!(
        result
            .proof
            .upper
            .try_cmp(&Rational::from_f64(2.0 + 1e-6))
            .unwrap(),
        Ordering::Greater
    );
    assert_eq!(
        (
            result.proof.witness.x.round().unwrap(),
            result.proof.witness.y.round().unwrap()
        ),
        (2.0, 2.0)
    );
}

#[test]
fn directed_inradius_uses_a_stable_center_on_a_parallel_plateau() {
    let shape = polygon(&[(0.0, 0.0), (8.0, 0.0), (8.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
    let first = directed_candidate_for(&shape, 1e-9, 1.0);
    let second = directed_candidate_for(&shape, 1e-9, 1.0);
    assert_eq!(
        (first.center.x.to_bits(), first.center.y.to_bits()),
        (second.center.x.to_bits(), second.center.y.to_bits())
    );
    assert_eq!(
        (
            first.proof.witness.x.round().unwrap(),
            first.proof.witness.y.round().unwrap()
        ),
        (
            second.proof.witness.x.round().unwrap(),
            second.proof.witness.y.round().unwrap()
        )
    );
    assert_eq!(first.center.x, 4.0);
    assert_eq!(first.center.y, 2.0);
    assert_eq!(
        first
            .proof
            .upper
            .try_cmp(&Rational::from_f64(2.0 + 1e-9))
            .unwrap(),
        Ordering::Less
    );
}

#[test]
fn directed_inradius_selects_the_first_equal_multipart_winner() {
    let shape = crate::io::parse_wkt(
        "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),\
                       ((10 0,14 0,14 4,10 4,10 0)))",
    )
    .unwrap();
    let result = directed_candidate_for(&shape, 1e-9, 1.0);
    assert!(result.center.x < 4.0 && result.center.y > 0.0 && result.center.y < 4.0);
    assert_eq!(
        result
            .proof
            .upper
            .try_cmp(&Rational::from_f64(2.0 + 1e-9))
            .unwrap(),
        Ordering::Less
    );
}

#[test]
fn directed_inradius_accounts_for_rounding_an_exact_witness_center() {
    let shape = polygon(&[
        (1.0, 0.0),
        (2.0_f64.powi(60), 0.0),
        (2.0_f64.powi(60), 4.0),
        (1.0, 4.0),
        (1.0, 0.0),
    ]);
    let result = directed_candidate_for(&shape, 1.0, 1.0);
    let rounded = ExactPoint::from_xy(result.center.xy());
    assert!(
        result.proof.witness.x.try_cmp(&rounded.x).unwrap() != Ordering::Equal
            || result.proof.witness.y.try_cmp(&rounded.y).unwrap() != Ordering::Equal
    );
    assert_eq!(
        result
            .proof
            .certified_squared
            .try_cmp(&result.center_squared)
            .unwrap(),
        Ordering::Less
    );
}

#[test]
fn directed_inradius_checks_the_final_output_scale_before_stopping() {
    let shape = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
    let scale_value = 0.1;
    let tolerance = 1e-12;
    let result = directed_candidate_for(&shape, tolerance, scale_value);
    let scale = Rational::from_f64(scale_value);
    let radius = downward_sqrt(
        &result
            .center_squared
            .try_product(&scale.try_square().unwrap())
            .unwrap(),
    )
    .unwrap();
    let upper = result.proof.upper.try_product(&scale).unwrap();
    let gap = upper.try_subtract(&Rational::from_f64(radius)).unwrap();
    assert_ne!(
        gap.try_cmp(&Rational::from_f64(tolerance)).unwrap(),
        Ordering::Greater
    );
    assert!(radius <= 0.2);
}

#[test]
fn boundary_clone_keeps_filled_source_separate_from_boundary_target() {
    let ring = |points: &[XY]| {
        crate::geometry::Ring::from_closed(CoordSeq::try_from_xy(points).unwrap()).unwrap()
    };
    let shape = Shape::Polygon(crate::geometry::Polygon::new(
        ring(&[
            XY::new(0.0, 0.0),
            XY::new(4.0, 0.0),
            XY::new(4.0, 4.0),
            XY::new(0.0, 4.0),
            XY::new(0.0, 0.0),
        ]),
        vec![ring(&[
            XY::new(1.0, 1.0),
            XY::new(3.0, 1.0),
            XY::new(3.0, 3.0),
            XY::new(1.0, 3.0),
            XY::new(1.0, 1.0),
        ])],
    ));
    let accuracy = Accuracy::new(Some(1e-6), 1.0).unwrap();
    let mut context = SearchContext::new(&accuracy);
    let source = PreparedSupport::from_shape(&shape, &mut context.budget).unwrap();
    let target = source.boundary_clone().unwrap();
    let center = ExactPoint::from_xy(XY::new(0.5, 0.5));
    assert!(!source.faces.is_empty());
    assert!(target.faces.is_empty());
    assert!(source.distance_squared(&center).unwrap().is_zero());
    assert_eq!(
        target.distance_squared(&center).unwrap().round().unwrap(),
        0.25
    );
}

#[test]
fn selected_source_candidate_proof_checks_vertices_chords_and_hole_cycles() {
    let ring = |points: &[XY]| {
        crate::geometry::Ring::from_closed(CoordSeq::try_from_xy(points).unwrap()).unwrap()
    };
    let shape = Shape::Polygon(crate::geometry::Polygon::new(
        ring(&[
            XY::new(-100.0, -100.0),
            XY::new(100.0, -100.0),
            XY::new(100.0, 100.0),
            XY::new(-100.0, 100.0),
            XY::new(-100.0, -100.0),
        ]),
        vec![ring(&[
            XY::new(-2.0, 8.0),
            XY::new(2.0, 8.0),
            XY::new(2.0, 10.0),
            XY::new(-2.0, 10.0),
            XY::new(-2.0, 8.0),
        ])],
    ));
    let accuracy = Accuracy::new(Some(1e-6), 1.0).unwrap();
    let mut context = SearchContext::new(&accuracy);
    let source = PreparedSupport::from_shape(&shape, &mut context.budget).unwrap();
    let boundary = source.boundary_clone().unwrap();
    let valid = [
        Point::new_raw_xy(3.0, 2.0),
        Point::new_raw_xy(2.0, 3.0),
        Point::new_raw_xy(1.0, 2.0),
        Point::new_raw_xy(2.0, 1.0),
    ];
    assert!(
        source
            .candidate_is_contained(&boundary, Point::new_raw_xy(2.0, 2.0), &valid, true)
            .unwrap()
    );
    let hole_enclosing = [
        Point::new_raw_xy(12.0, 0.0),
        Point::new_raw_xy(0.0, 12.0),
        Point::new_raw_xy(-12.0, 0.0),
        Point::new_raw_xy(0.0, -12.0),
    ];
    assert!(
        !source
            .candidate_is_contained(
                &boundary,
                Point::new_raw_xy(0.0, 0.0),
                &hole_enclosing,
                true,
            )
            .unwrap()
    );
}

#[test]
fn boundary_target_cancels_shared_edges_of_filled_multipart_support() {
    let shape = crate::io::parse_wkt(
        "MULTIPOLYGON (((0 0,10 0,10 10,0 10,0 0),\
                        (20 20,22 20,22 22,20 22,20 20)),\
                       ((15 15,30 15,30 30,15 30,15 15)))",
    )
    .unwrap();
    let mut budget = ExpansionBudget::new("test", "support");
    let source = PreparedSupport::from_shape_with_boundary(&shape, &mut budget).unwrap();
    let target = source.boundary_clone().unwrap();
    assert_eq!(target.spans.len(), 8);
    let result = directed_inradius_scaled(&shape, 1e-9, 1.0).unwrap();
    assert_eq!((result.center.x, result.center.y), (22.5, 22.5));
    assert_eq!(downward_sqrt(&result.center_squared).unwrap(), 7.5);
    let center = ExactPoint::from_xy(result.center.xy());
    assert_eq!(
        result
            .boundary
            .distance_squared(&center)
            .unwrap()
            .try_cmp(&result.center_squared)
            .unwrap(),
        Ordering::Equal
    );
    assert!(result.source.contains_area(&center).unwrap());
}

#[test]
fn directed_result_drops_boundary_incidence_after_boundary_clone() {
    let shape = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
    let result = directed_inradius_scaled(&shape, 1e-3, 1.0).unwrap();

    assert!(
        result.source.boundary_spans.is_none(),
        "source support no longer needs boundary-span incidence after cloning"
    );
    assert!(
        result.source.boundary_points.is_none(),
        "source support no longer needs boundary-point incidence after cloning"
    );
    assert!(
        result.boundary.boundary_spans.is_none() && result.boundary.boundary_points.is_none(),
        "boundary clone carries geometry only"
    );
}

#[test]
fn directed_inradius_ignores_collapsed_support_inside_positive_area() {
    let outer = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_xy(&[XY::new(1.0, 1.0), XY::new(3.0, 1.0), XY::new(1.0, 1.0)])
                .unwrap(),
        )
        .unwrap(),
    );
    let shape =
        Shape::geometry_collection(vec![outer, line, Shape::Point(Point::new_raw_xy(2.0, 2.0))])
            .unwrap();
    let result = directed_inradius_scaled(&shape, 1e-9, 1.0).unwrap();
    assert_eq!(downward_sqrt(&result.center_squared).unwrap(), 2.0);
    assert!(
        result
            .source
            .contains_area(&ExactPoint::from_xy(result.center.xy()))
            .unwrap()
    );
}

#[test]
fn directed_inradius_propagates_the_subdivision_budget() {
    let shape = polygon(&[(0.0, 0.0), (4.0, 0.0), (0.0, 4.0), (0.0, 0.0)]);
    let accuracy = Accuracy::new(Some(1.0), 1.0).unwrap();
    let mut context = SearchContext::new_directed(&accuracy, Rational::one(), Rational::one());
    let source = PreparedSupport::from_shape(&shape, &mut context.budget).unwrap();
    let target = source.boundary_clone().unwrap();
    context.budget.add(GENERATED_ITEM_LIMIT).unwrap();
    let error = run_directed(&source, &target, &mut context).unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(
            crate::geometry::GeometryErrorKind::GeneratedOutputTooLarge {
                operation: "maximum_inscribed_circle",
                parameter: "tolerance",
                ..
            }
        )
    ));
}

#[test]
fn multipart_area_boxes_and_mixed_points_use_only_source_witnesses() {
    let multipart = crate::io::parse_wkt(
        "MULTIPOLYGON (((0 0,1 0,1 1,0 1,0 0)),\
                       ((3 3,5 3,5 5,3 5,3 3)))",
    )
    .unwrap();
    for ((x, y), expected_squared) in [((3.0, 1.0), 20.0), ((5.0, 3.0), 34.0)] {
        let target = Shape::Point(Point::new_raw_xy(x, y));
        let actual = distance(&multipart, &target, Some(1e-12), 1.0).unwrap();
        assert!((actual - f64::sqrt(expected_squared)).abs() <= 1e-12);
    }

    let mixed = Shape::geometry_collection(vec![
        line(&[(0.0, 0.0), (1.0, 0.0)]),
        Shape::Point(Point::new_raw_xy(10.0, 0.0)),
    ])
    .unwrap();
    let origin = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    assert!((distance(&mixed, &origin, Some(1e-12), 1.0).unwrap() - 10.0).abs() <= 1e-12);
}

#[test]
fn nested_island_target_fill_is_part_of_the_closed_support() {
    let source = crate::io::parse_wkt(
        "MULTILINESTRING (\
         (0 0,10 0,10 10,0 10,0 0),\
         (2 2,8 2,8 8,2 8,2 2))",
    )
    .unwrap();
    let target = crate::io::parse_wkt(
        "GEOMETRYCOLLECTION (\
         POLYGON ((0 0,10 0,10 10,0 10,0 0),(2 2,8 2,8 8,2 8,2 2)),\
         POLYGON ((4 4,6 4,6 6,4 6,4 4)))",
    )
    .unwrap();
    assert!((distance(&source, &target, Some(1e-12), 1.0).unwrap() - 3.0).abs() <= 1e-12);
}

#[test]
fn metric_support_consumption_preserves_multipart_faces_and_mixed_points() {
    for (wkt, inside, outside) in [
        (
            "MULTIPOLYGON (((0 0,2 0,2 2,0 2,0 0)),\
                           ((2 0,4 0,4 2,2 2,2 0)))",
            [(1.0, 1.0), (3.0, 1.0), (2.0, 1.0)],
            [(5.0, 1.0), (2.0, 3.0), (-1.0, 1.0)],
        ),
        (
            "MULTIPOLYGON (((0 0,3 0,3 3,0 3,0 0)),\
                           ((2 0,5 0,5 3,2 3,2 0)))",
            [(1.0, 1.0), (2.5, 1.0), (4.0, 1.0)],
            [(6.0, 1.0), (2.5, 4.0), (-1.0, 1.0)],
        ),
    ] {
        let shape = crate::io::parse_wkt(wkt).unwrap();
        let mut budget = ExpansionBudget::new("test", "metric support");
        let support = PreparedSupport::from_shape(&shape, &mut budget).unwrap();
        assert!(!support.faces.is_empty(), "{wkt}");
        for (x, y) in inside {
            assert!(
                support
                    .contains_area(&ExactPoint::from_xy(XY::new(x, y)))
                    .unwrap(),
                "expected ({x}, {y}) inside {wkt}"
            );
        }
        for (x, y) in outside {
            assert!(
                !support
                    .contains_area(&ExactPoint::from_xy(XY::new(x, y)))
                    .unwrap(),
                "expected ({x}, {y}) outside {wkt}"
            );
        }
    }

    let shape = crate::io::parse_wkt(
        "GEOMETRYCOLLECTION(\
         POLYGON ((0 0,4 0,4 4,0 4,0 0)),\
         POINT (10 1))",
    )
    .unwrap();
    let mut budget = ExpansionBudget::new("test", "metric support");
    let support = PreparedSupport::from_shape(&shape, &mut budget).unwrap();
    assert_eq!(support.spans.len(), 4);
    assert_eq!(support.points.len(), 1);
    assert_eq!(support.faces.len(), 1);
    assert_eq!(support.faces[0].len(), 1);
    assert!(
        support
            .contains_area(&ExactPoint::from_xy(XY::new(2.0, 2.0)))
            .unwrap()
    );
    assert!(
        !support
            .contains_area(&ExactPoint::from_xy(XY::new(10.0, 1.0)))
            .unwrap()
    );
    assert!(support.points.iter().any(|point| {
        point
            .same_position(&ExactPoint::from_xy(XY::new(10.0, 1.0)))
            .unwrap()
    }));
}

#[test]
fn segment_to_point_distance_attains_the_farthest_endpoint() {
    let source = line(&[(0.0, 0.0), (2.0, 0.0)]);
    let target = Shape::Point(Point::new_raw_xy(1.0, 1.0));
    let value = distance(&source, &target, Some(1e-6), 1.0).unwrap();
    assert!((value - 2.0_f64.sqrt()).abs() <= 1e-6);
}

#[test]
fn filled_square_to_point_distance_attains_the_farthest_corner() {
    let source = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
    let target = Shape::Point(Point::new_raw_xy(6.0, 2.0));
    let value = distance(&source, &target, Some(1e-3), 1.0).unwrap();
    assert!((value - 40.0_f64.sqrt()).abs() <= 1e-3);
}

#[test]
fn output_units_preserve_extreme_finite_distances() {
    for (coordinate, scale) in [
        (2.0_f64.powi(1023), 2.0_f64.powi(-1023)),
        (2.0_f64.powi(-1023), 2.0_f64.powi(1023)),
    ] {
        let left = Shape::Point(Point::new_raw_xy(-coordinate, 0.0));
        let right = Shape::Point(Point::new_raw_xy(coordinate, 0.0));
        assert_eq!(distance(&left, &right, Some(1e-9), scale).unwrap(), 2.0);
    }
}

#[test]
fn output_range_and_scaled_absolute_error_are_certified() {
    let huge = 2.0_f64.powi(1023);
    let left = Shape::Point(Point::new_raw_xy(-huge, 0.0));
    let right = Shape::Point(Point::new_raw_xy(huge, 0.0));
    distance(&left, &right, Some(1.0), 1.0).unwrap_err();

    let origin = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    let large = Shape::Point(Point::new_raw_xy(1e200, 1e200));
    let expected = 2.0_f64.sqrt() * 1e200;
    let actual = distance(&origin, &large, None, 1.0).unwrap();
    assert!((actual - expected).abs() <= 1e-9 * expected);

    let vertices = [
        (0.0, 0.0),
        (9.0, 0.0),
        (9.0, 3.0),
        (4.0, 3.0),
        (4.0, 8.0),
        (0.0, 8.0),
        (0.0, 0.0),
    ];
    let expected = 100.0 * (7.0 - 24.0_f64.sqrt());
    let actual = distance(&polygon(&vertices), &line(&vertices), Some(0.01), 100.0).unwrap();
    assert!((actual - expected).abs() <= 0.01);
}

#[test]
fn overflow_newton_steps_remain_exact_upper_bounds() {
    let maximum = Rational::from_f64(f64::MAX);
    let scale = Rational::from_f64(1.0_f64.next_up());
    let squared = maximum
        .try_square()
        .unwrap()
        .try_product(&scale.try_square().unwrap())
        .unwrap();
    let mut upper = squared.try_quotient(&maximum).unwrap();
    for step in 0..=OVERFLOW_NEWTON_STEPS {
        assert_ne!(
            upper.try_square().unwrap().try_cmp(&squared).unwrap(),
            Ordering::Less,
            "Newton upper step {step} fell below the exact squared value"
        );
        if step != OVERFLOW_NEWTON_STEPS {
            let next = overflow_newton_step(&squared, &upper).unwrap();
            assert_ne!(
                next.try_cmp(&upper).unwrap(),
                Ordering::Greater,
                "Newton step {step} must not loosen the upper"
            );
            upper = next;
        }
    }
}

#[test]
fn overflowed_exact_point_distance_can_certify_maximum_with_large_error() {
    let left = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    let right = Shape::Point(Point::new_raw_xy(f64::MAX, 0.0));
    let scale = 1.0_f64.next_up();
    let maximum = Rational::from_f64(f64::MAX);
    let scale_exact = Rational::from_f64(scale);
    let scaled_squared = maximum
        .try_square()
        .unwrap()
        .try_product(&scale_exact.try_square().unwrap())
        .unwrap();
    let exact_distance = maximum.try_product(&scale_exact).unwrap();
    let true_gap = exact_distance.try_subtract(&maximum).unwrap();
    let error = Rational::from_f64(6e292);
    assert_ne!(
        true_gap.try_cmp(&error).unwrap(),
        Ordering::Greater,
        "the exact point distance gap must fit the chosen error"
    );
    let target = maximum.try_add(&error).unwrap();
    assert_ne!(
        scaled_squared
            .try_cmp(&target.try_square().unwrap())
            .unwrap(),
        Ordering::Greater,
        "the exact squared distance must fit the chosen candidate allowance"
    );
    let interval = SqrtInterval::from_squared_with_error(&scaled_squared, Some(&error)).unwrap();
    let upper = interval
        .upper_rational()
        .unwrap()
        .expect("finite exact overflow upper");
    let initial_upper = scaled_squared.try_quotient(&maximum).unwrap();
    let initial_gap = initial_upper.try_subtract(&maximum).unwrap();
    assert_eq!(
        true_gap.try_cmp(&error).unwrap(),
        Ordering::Less,
        "this is the between-gaps case, not an exact-boundary tie"
    );
    assert_eq!(
        initial_gap.try_cmp(&error).unwrap(),
        Ordering::Greater,
        "q/MAX must be too loose for the chosen error"
    );
    let small_error = Rational::from_f64(3e292);
    assert_eq!(
        true_gap.try_cmp(&small_error).unwrap(),
        Ordering::Greater,
        "the negative control must be below the exact point-distance gap"
    );
    assert_ne!(
        upper.try_cmp(&initial_upper).unwrap(),
        Ordering::Greater,
        "Newton refinement must not loosen q/MAX"
    );
    assert_ne!(
        upper.try_cmp(&target).unwrap(),
        Ordering::Greater,
        "error-directed Newton refinement must fit MAX + error"
    );
    let sum = sum_upper(&interval, &SqrtInterval::singleton(1.0))
        .unwrap()
        .expect("overflow upper remains finite when summed");
    assert_eq!(
        sum.try_cmp(&upper.try_add(&Rational::one()).unwrap())
            .unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        scaled_squared
            .try_cmp(&maximum.try_square().unwrap())
            .unwrap(),
        Ordering::Greater
    );
    assert_eq!(
        distance(&left, &right, Some(6e292), scale)
            .unwrap()
            .to_bits(),
        f64::MAX.to_bits()
    );
    assert_eq!(
        distance(&left, &right, None, scale).unwrap().to_bits(),
        f64::MAX.to_bits()
    );
    distance(&left, &right, Some(3e292), scale)
        .expect_err("an allowance below the exact max-to-upper gap must not certify f64::MAX");
}

#[test]
fn filled_interior_and_parallel_plateau_obey_the_error_bound() {
    for vertices in [vec![(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)], vec![
        (0.0, 0.0),
        (100.0, 0.0),
        (100.0, 2.0),
        (0.0, 2.0),
        (0.0, 0.0),
    ]] {
        let value = distance(&polygon(&vertices), &line(&vertices), Some(1e-6), 1.0).unwrap();
        assert!((value - 1.0).abs() <= 1e-6, "{value}");
    }
}

#[test]
fn default_relative_error_is_certified_without_blurring_zero() {
    const DEFAULT_RELATIVE_ERROR: f64 = 1e-9;

    let triangle = polygon(&[(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)]);
    let boundary = line(&[(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)]);
    let value = distance(&triangle, &boundary, None, 1.0).unwrap();
    assert!((value - 1.0).abs() <= DEFAULT_RELATIVE_ERROR);

    let diagonal = Shape::Point(Point::new_raw_xy(1.0, 1.0));
    let diagonal_value = distance(
        &Shape::Point(Point::new_raw_xy(0.0, 0.0)),
        &diagonal,
        None,
        1.0,
    )
    .unwrap();
    let expected = 2.0_f64.sqrt();
    assert!((diagonal_value - expected).abs() <= DEFAULT_RELATIVE_ERROR * expected);

    let origin = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    let close = Shape::Point(Point::new_raw_xy(1e-200, 0.0));
    assert_eq!(distance(&origin, &origin, None, 1.0).unwrap(), 0.0);
    assert!(distance(&origin, &close, None, 1.0).unwrap() > 0.0);
}

#[test]
fn default_allowance_is_bounded_by_the_true_distance() {
    const DEFAULT_RELATIVE_ERROR: f64 = 1e-9;

    let accuracy = Accuracy::new(None, 1.0).unwrap();
    let squared = Rational::from_f64(2.0);
    let candidate = squared.round_sqrt().unwrap();
    let error = accuracy.absolute_error(&squared, candidate).unwrap();
    let relative = Rational::from_f64(DEFAULT_RELATIVE_ERROR);
    let relative_squared = relative
        .try_square()
        .unwrap()
        .try_product(&squared)
        .unwrap();
    assert_ne!(
        error
            .try_square()
            .unwrap()
            .try_cmp(&relative_squared)
            .unwrap(),
        Ordering::Greater,
    );
}

#[test]
fn default_relative_error_accepts_a_valid_minimum_subnormal_result() {
    let minimum_subnormal = f64::from_bits(1);
    let almost_one = 1.0_f64.next_down();
    let small_ordinate = 3.0 * 2.0_f64.powi(-28);
    let origin = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    let nearby = Shape::Point(Point::new_raw_xy(almost_one, small_ordinate));

    assert_eq!(
        distance(&origin, &nearby, None, minimum_subnormal).unwrap(),
        minimum_subnormal
    );
}

#[test]
fn zero_distinguishes_equal_support_from_close_support() {
    let baseline = line(&[(0.0, 0.0), (2.0, 0.0)]);
    let segmented = line(&[(2.0, 0.0), (1.0, 0.0), (0.0, 0.0)]);
    assert_eq!(
        distance(&baseline, &segmented, Some(1e-3), 1.0).unwrap(),
        0.0
    );
    let nearby = line(&[(0.0, 1e-9), (2.0, 1e-9)]);
    let value = distance(&baseline, &nearby, Some(1e-3), 1.0).unwrap();
    assert!(value > 0.0);
    assert!((value - 1e-9).abs() <= 1e-3);
    let vertices = [(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)];
    let value = distance(&polygon(&vertices), &line(&vertices), Some(2.0), 1.0).unwrap();
    assert!(value > 0.0);
    assert!((value - 1.0).abs() <= 2.0);
}

#[test]
fn identical_cancelled_polygon_support_is_undefined() {
    let ring = crate::geometry::Ring::from_closed(
        CoordSeq::try_from_xy(&[
            XY::new(0.0, 0.0),
            XY::new(1.0, 0.0),
            XY::new(2.0, 0.0),
            XY::new(0.0, 0.0),
        ])
        .unwrap(),
    )
    .unwrap();
    let shape = Shape::Polygon(crate::geometry::Polygon::new(ring.clone(), vec![ring]));
    distance(&shape, &shape, Some(1e-6), 1.0).unwrap_err();
}

#[test]
fn invalid_accuracy_is_rejected_before_identity() {
    let point = Shape::Point(Point::new_raw_xy(3.0, 4.0));
    for error in [0.0, -1.0, f64::INFINITY, f64::NAN] {
        distance(&point, &point, Some(error), 1.0).unwrap_err();
    }
}
