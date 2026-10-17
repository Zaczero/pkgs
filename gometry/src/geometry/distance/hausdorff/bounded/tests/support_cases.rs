fn test_span(source: Segment) -> SupportSpan {
    let start = ExactPoint::from_xy(source.start);
    let end = ExactPoint::from_xy(source.end);
    let bounds = FloatBox::from_point(&start)
        .unwrap()
        .include(FloatBox::from_point(&end).unwrap());
    SupportSpan {
        source,
        carrier: std::sync::OnceLock::new(),
        range: None,
        bounds,
    }
}

#[test]
fn nominated_sites_respect_noded_span_windows() {
    let source = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(10.0, 0.0),
    };
    let full = test_span(source);
    let denominator = ExactDyadic::from_f64(5.0);
    let spans: Vec<_> = (0_usize..5)
        .map(|index| {
            let low =
                Rational::new(ExactDyadic::from_f64(index as f64), denominator.clone()).unwrap();
            let high = Rational::new(
                ExactDyadic::from_f64(index.wrapping_add(1) as f64),
                denominator.clone(),
            )
            .unwrap();
            let start = span_point(&full, &low).unwrap();
            let end = span_point(&full, &high).unwrap();
            let bounds = FloatBox::from_point(&start)
                .unwrap()
                .include(FloatBox::from_point(&end).unwrap());
            SupportSpan {
                source,
                carrier: std::sync::OnceLock::new(),
                range: Some(Box::new(SelectedSpan {
                    parameters: [low, high],
                    start,
                })),
                bounds,
            }
        })
        .collect();
    let support = PreparedSupport::from_parts(spans, Vec::new(), Vec::new()).unwrap();
    let point = ExactPoint::from_xy(XY::new(9.5, 0.0));

    let nominated = support.index.nominate(&support, &point).unwrap();

    assert_eq!(nominated.first(), Some(&PrimitiveId::Span(4)));
}

fn reduced_point_span_distance_squared(point: &ExactPoint, span: &SupportSpan) -> Result<Rational> {
    let carrier = span.carrier().unwrap();
    let px = point.x().try_subtract(&carrier.ax)?;
    let py = point.y().try_subtract(&carrier.ay)?;
    let dot = px
        .try_product(&carrier.dx)?
        .try_add(&py.try_product(&carrier.dy)?)?;
    let low = span.lo().try_product(&carrier.norm_squared)?;
    let high = span.hi().try_product(&carrier.norm_squared)?;
    let parameter = if carrier.norm_squared.is_zero() || dot.try_cmp(&low)? == Ordering::Less {
        span.lo()
    } else if dot.try_cmp(&high)? == Ordering::Greater {
        span.hi()
    } else {
        let cross = px
            .try_product(&carrier.dy)?
            .try_subtract(&py.try_product(&carrier.dx)?)?;
        return cross.try_square()?.try_quotient(&carrier.norm_squared);
    };
    let projected = span_point(span, parameter)?;
    let dx = point.x().try_subtract(projected.x())?;
    let dy = point.y().try_subtract(projected.y())?;
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
fn misaligned_metric_boundary_incidence_is_rejected_before_consumption() {
    for support in [
        ExactMetricSupport {
            exact_points: Vec::new().into(),
            spans: Vec::new(),
            points: Vec::new(),
            faces: Vec::new(),
            area_location_cells: None,
            boundary_spans: Some(vec![true]),
            boundary_points: None,
        },
        ExactMetricSupport {
            exact_points: Vec::new().into(),
            spans: Vec::new(),
            points: Vec::new(),
            faces: Vec::new(),
            area_location_cells: None,
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

/// Exact squared distance from a point to a box, the oracle for the
/// binary64 box bound.
fn aabb_lower_squared(point: &ExactPoint, bounds: FloatBox) -> Result<Rational> {
    let min_x = Rational::from_f64(bounds.min_x);
    let max_x = Rational::from_f64(bounds.max_x);
    let min_y = Rational::from_f64(bounds.min_y);
    let max_y = Rational::from_f64(bounds.max_y);
    let zero = Rational::zero();
    let dx = if point.x().try_cmp(&min_x)? == Ordering::Less {
        min_x.try_subtract(point.x())?
    } else if point.x().try_cmp(&max_x)? == Ordering::Greater {
        point.x().try_subtract(&max_x)?
    } else {
        zero.clone()
    };
    let dy = if point.y().try_cmp(&min_y)? == Ordering::Less {
        min_y.try_subtract(point.y())?
    } else if point.y().try_cmp(&max_y)? == Ordering::Greater {
        point.y().try_subtract(&max_y)?
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
fn disjoint_carrier_slopes_skip_only_impossible_parallel_strips() {
    let distinct = PreparedSupport::from_line_columns(&[0.0, 1.0, 2.0], &[0.0, 0.0, 1.0]).unwrap();
    assert!(!distinct.parallel_carriers);
    let horizontal =
        PreparedSupport::from_line_columns(&[0.0, 1.0, 2.0], &[0.0, 0.0, 0.0]).unwrap();
    assert!(horizontal.parallel_carriers);
    let vertical = PreparedSupport::from_line_columns(&[0.0, 0.0, 0.0], &[0.0, 1.0, 2.0]).unwrap();
    assert!(vertical.parallel_carriers);
}

#[test]
fn exact_metric_nodes_share_the_frozen_area_location_point_table() {
    let shape = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
    let mut budget = ExpansionBudget::new("maximum_inscribed_circle", "tolerance");
    let inventory = ExactSourceInventory::from_shapes(&[&shape]).unwrap();
    let partition = inventory.partition(&mut budget).unwrap();
    let support = inventory
        .metric_support_with_boundary(&partition, AreaLocationStrategy::PersistentSlabs)
        .unwrap();
    let cells = support
        .area_location_cells
        .as_ref()
        .expect("filled metric support has an area locator");
    assert!(Arc::ptr_eq(&support.exact_points, &cells.geometry.points));
}

#[test]
fn floating_box_pruning_covers_normal_sums_of_subnormal_products_and_overflow() {
    for (target, probe) in [
        (XY::new(0.0, 0.0), XY::new(1.2, 1.2)),
        (XY::new(0.0, 0.0), XY::new(1.1e-154, 1.1e-154)),
        (XY::new(0.0, 0.0), XY::new(1e-160, 1e-160)),
        (XY::new(0.0, 0.0), XY::new(1e200, 0.0)),
        (XY::new(-f64::MAX, 0.0), XY::new(f64::MAX, 0.0)),
    ] {
        let bounds = FloatBox {
            min_x: target.x,
            max_x: target.x,
            min_y: target.y,
            max_y: target.y,
        };
        let point = ExactPoint::from_xy(probe);
        let lower = box_distance_lower(FloatBox::from_point(&point).unwrap(), bounds);
        let exact = aabb_lower_squared(&point, bounds).unwrap();
        assert!(lower.is_finite());
        assert!(lower >= 0.0);
        assert_ne!(
            Rational::from_f64(lower).try_cmp(&exact).unwrap(),
            Ordering::Greater,
            "target={target:?}, probe={probe:?}"
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
    use crate::geometry::numerics::product::exact_power_of_two;
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
            XY::new(0.5, 0.5 + exact_power_of_two(-52)),
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
            let exact =
                point_span_distance_squared(&point, span.carrier().unwrap(), span.lo(), span.hi())
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
    use crate::geometry::numerics::product::exact_power_of_two;
    let point_support = PreparedSupport::from_line_columns(&[0.0], &[0.0]).unwrap();
    for point in [
        ExactPoint::from_xy(XY::new(3.0, 4.0)),
        ExactPoint::from_xy(XY::new(exact_power_of_two(500), -(exact_power_of_two(500)))),
        ExactPoint::from_xy(XY::new(f64::from_bits(1), -f64::from_bits(2))),
    ] {
        let actual = point_support
            .primitive_distance_squared(PrimitiveId::Point(0), &point)
            .unwrap();
        let dx = point.x().try_subtract(point_support.points[0].x()).unwrap();
        let dy = point.y().try_subtract(point_support.points[0].y()).unwrap();
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
            [0.0, exact_power_of_two(500)],
            [0.0, 0.0],
            (0.0, exact_power_of_two(500)),
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
        let span = &support.spans[0];
        let actual =
            point_span_distance_squared(&point, span.carrier().unwrap(), span.lo(), span.hi())
                .unwrap();
        let expected = reduced_point_span_distance_squared(&point, span).unwrap();
        assert_eq!(actual.try_cmp_rational(&expected).unwrap(), Ordering::Equal);
    }

    let mut subspan = PreparedSupport::from_line_columns(&[0.0, 6.0], &[0.0, 0.0]).unwrap();
    Arc::make_mut(&mut subspan.spans)[0].range = Some(Box::new(SelectedSpan {
        parameters: [
            Rational::new(ExactDyadic::from_f64(1.0), ExactDyadic::from_f64(3.0)).unwrap(),
            Rational::new(ExactDyadic::from_f64(2.0), ExactDyadic::from_f64(3.0)).unwrap(),
        ],
        start: ExactPoint::from_xy(XY::new(2.0, 0.0)),
    }));
    let point = ExactPoint::from_xy(XY::new(0.0, 0.0));
    let span = &subspan.spans[0];
    let actual =
        point_span_distance_squared(&point, span.carrier().unwrap(), span.lo(), span.hi()).unwrap();
    let expected = reduced_point_span_distance_squared(&point, span).unwrap();
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
        Arc::make_mut(&mut support.spans)[0].range = Some(Box::new(SelectedSpan {
            parameters: [Rational::from_f64(lo), Rational::from_f64(hi)],
            start: ExactPoint::from_xy(XY::new(3.0 * lo, 4.0 * lo)),
        }));
        let span = &support.spans[0];
        let value = point_span_distance_squared(
            &ExactPoint::from_xy(XY::new(x, y)),
            span.carrier().unwrap(),
            span.lo(),
            span.hi(),
        )
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
fn binary64_span_filter_encloses_interior_and_endpoint_clearances() {
    for (xs, ys, probe) in [
        ([0.0, 3.0], [0.0, 4.0], (1.0, 3.0)),
        ([0.0, 3.0], [0.0, 4.0], (-3.0, -4.0)),
        ([0.0, 3.0], [0.0, 4.0], (6.0, 8.0)),
        ([3.0, 0.0], [4.0, 0.0], (1.0, 3.0)),
        ([0.0, 1.0], [0.0, 0.0], (0.5, 1.0 + f64::EPSILON)),
        (
            [0.0, 1.0],
            [0.0, 0.0],
            (-1.0 - f64::EPSILON, 1.0 + f64::EPSILON),
        ),
        ([0.0, 1e-100], [0.0, 0.0], (5e-101, 1e-100)),
    ] {
        let support = PreparedSupport::from_line_columns(&xs, &ys).unwrap();
        let point = ExactPoint::from_xy(XY::new(probe.0, probe.1));
        let bounds = FloatBox::from_point(&point).unwrap();
        let interval = point_span_squared_bounds_with::<exact::Interval>(bounds, &support.spans[0])
            .expect("the binary64 filter decides these finite separated regimes");
        let filtered = point_span_binary64_bounds(bounds, &support.spans[0])
            .expect("the distance-error filter decides these finite separated regimes");
        let exact = support
            .primitive_distance_squared(PrimitiveId::Span(0), &point)
            .unwrap()
            .to_rational()
            .unwrap();
        for (lower, upper) in [interval, filtered] {
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
}

#[test]
fn nearest_distance_refines_overflowing_squared_distance_terms() {
    for (end_x, probe_x, clearance) in [(1e100, 5e99, 1e100), (1e200, 5e199, 1e200)] {
        let support = PreparedSupport::from_line_columns(&[0.0, end_x], &[0.0, 0.0]).unwrap();
        let point = ExactPoint::from_xy(XY::new(probe_x, clearance));
        let bounds = FloatBox::from_point(&point).unwrap();
        assert!(
            point_span_squared_bounds_with::<exact::Interval>(bounds, &support.spans[0]).is_none()
        );
        let nearest = support
            .index
            .nearest(
                &support,
                &point,
                Some(Tightness {
                    relative: 0.0,
                    absolute: 0.0,
                }),
            )
            .unwrap();
        let expected = Rational::from_f64(clearance).try_square().unwrap();
        assert_eq!(
            nearest.bounds.lower.try_cmp(&expected).unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            nearest.bounds.upper.try_cmp(&expected).unwrap(),
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
