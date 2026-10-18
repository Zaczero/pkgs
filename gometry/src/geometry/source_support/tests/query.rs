#[test]
fn stored_query_axis_order_preserves_topological_identity() {
    let first_nan = f64::from_bits(0x7FF8_0000_0000_0001);
    let second_nan = f64::from_bits(0x7FF8_0000_0000_0002);
    let query = StoredQuery(XY::new(first_nan, 0.0));
    assert_eq!(
        query.compare_axis(QueryAxis::X, first_nan).unwrap(),
        Ordering::Equal
    );
    assert_ne!(
        query.compare_axis(QueryAxis::X, second_nan).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        query.compare_axis(QueryAxis::Y, -0.0).unwrap(),
        Ordering::Equal
    );

    let finite = StoredQuery(XY::new(1.0, -0.0));
    assert!(finite.axis_relation(QueryAxis::X, 2.0).unwrap().query_below);
    assert!(!finite.axis_relation(QueryAxis::X, 0.0).unwrap().query_below);
    assert!(finite.axis_relation(QueryAxis::Y, 0.0).unwrap().equal);
}

#[test]
fn exact_query_index_keys_use_the_tight_bracket_and_offset_side() {
    let third = 1.0_f64 / 3.0;
    let subnormal = f64::from_bits(1);
    for (numerator, denominator, expected, negative_offset) in [
        (1.0, 3.0, third, third),
        (-1.0, -3.0, third, third),
        (2.0, 3.0, third * 2.0, third * 2.0),
        (1.0, 1.0, 1.0, 1.0_f64.next_down()),
        (-0.0, 1.0, 0.0, -subnormal),
        (subnormal, 1.0, subnormal, 0.0),
        (-subnormal, 1.0, -subnormal, -subnormal * 2.0),
        (subnormal, 2.0, 0.0, 0.0),
        (-subnormal, 2.0, -subnormal, -subnormal),
    ] {
        let point = [
            ExactDyadic::from_f64(numerator),
            ExactDyadic::from_f64(numerator),
            ExactDyadic::from_f64(denominator),
        ];
        let probe = ExactProbe::new(&point);
        let query = probe.source_query().unwrap();
        let direction = [ExactDyadic::from_f64(-1.0), ExactDyadic::from_f64(1.0)];
        let offset = query.offset(&direction);
        for axis in [QueryAxis::X, QueryAxis::Y] {
            assert_eq!(query.index_coordinate(axis), expected);
        }
        assert_eq!(offset.index_coordinate(QueryAxis::X), negative_offset);
        assert_eq!(offset.index_coordinate(QueryAxis::Y), expected);
    }
}

#[test]
#[cfg(debug_assertions)]
fn exact_probe_admits_each_ratio_once_across_consumers() {
    use crate::test_support::exact_operation_counts::measure;

    let point = [
        ExactDyadic::from_f64(1.0),
        ExactDyadic::from_f64(0.0),
        ExactDyadic::from_f64(3.0),
    ];
    let probe = ExactProbe::new(&point);
    let (_, fresh) = measure(|| probe.source_query().unwrap());
    assert_eq!(fresh.ratio_roundings, 2);
    let shape = crate::io::parse_wkt(
        "POLYGON ((0 0,0.5 0,1 0,1.5 0,2 0,2.5 0,3 0,3.5 0,4 0,4.5 0,5 0,\
         5 0.5,5 1,5 1.5,5 2,5 2.5,5 3,5 3.5,5 4,5 4.5,5 5,\
         4.5 5,4 5,3.5 5,2.5 5,2 5,1.5 5,1 5,0.5 5,0 5,\
         0 4.5,0 4,0 3.5,0 3,0 2.5,0 2,0 1.5,0 1,0 0.5,0 0))",
    )
    .unwrap();
    let plan = crate::geometry::PreparedPlan::transient(&shape);
    let rings = plan.prepare_point_queries(4).unwrap();
    assert!(rings.uses_indexed_plan());
    let up = [ExactDyadic::zero(), ExactDyadic::one()];
    let down = [ExactDyadic::zero(), ExactDyadic::from_f64(-1.0)];
    let tangent = [ExactDyadic::one(), ExactDyadic::zero()];
    let ((), reused) = measure(|| {
        for query in [
            crate::geometry::PreparedPointQueries::streaming(&shape),
            rings,
        ] {
            assert_eq!(query.locate_probe(&probe).unwrap(), SourcePointLocation {
                location: Loc::Boundary,
                area: true,
            });
            assert_eq!(
                query.locate_probe_offset(&probe, &up).unwrap(),
                SourcePointLocation {
                    location: Loc::Interior,
                    area: true,
                }
            );
            assert_eq!(
                query.locate_probe_offset(&probe, &down).unwrap(),
                SourcePointLocation {
                    location: Loc::Exterior,
                    area: false,
                }
            );
            assert_eq!(
                query.locate_probe_offset(&probe, &tangent).unwrap(),
                SourcePointLocation {
                    location: Loc::Boundary,
                    area: true,
                }
            );
        }
    });
    assert_eq!(reused.ratio_roundings, 0);
}
