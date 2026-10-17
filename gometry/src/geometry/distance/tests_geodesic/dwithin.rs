use super::support::disconnected_bvh_target;
use super::*;

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn scalar_geodesic_dwithin_at_infinity_does_not_allocate() -> Result<()> {
    let left_shape = line(vec![p(-2.0, 0.0), p(2.0, 0.0)]);
    let right_shape = line(vec![p(-2.0, 1.0), p(2.0, 1.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&left_shape, &right_shape], |metric| {
        let left = PreparedPlan::new(left_shape.clone());
        let right = PreparedPlan::new(right_shape.clone());
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        let infinite = || {
            let left_cache = FrameDependentCaches::default();
            let right_cache = FrameDependentCaches::default();
            left.geodesic_dwithin_cached(
                &left_cache,
                &right,
                &right_cache,
                "EPSG:4326",
                semi_major,
                flattening,
                metric,
                f64::INFINITY,
            )
        };
        assert!(crate::test_support::assert_no_allocations(infinite)?);
        Ok(())
    })?;
    Ok(())
}
#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn fixed_geodesic_point_rows_at_infinity_still_validate_latitudes() -> Result<()> {
    let cases = [
        line(vec![p(-1.0, 0.0), p(1.0, 0.0)]),
        disconnected_bvh_target(),
    ];
    for target in cases {
        with_ellipsoid_metric("EPSG:4326", &[&target], |metric| {
            let data = PreparedPlan::new(target.clone());
            let frame_cache = FrameDependentCaches::default();
            let (semi_major, flattening) = metric.ellipsoid_parameters();
            let xs = [0.0, 10.0];
            let ys = [0.0, 0.0];
            let inf_run = || {
                data.geodesic_dwithin_points(
                    &frame_cache,
                    std::iter::zip(xs.iter().copied(), ys.iter().copied()),
                    "EPSG:4326",
                    semi_major,
                    flattening,
                    metric,
                    f64::INFINITY,
                    metric.frame(),
                )
            };
            assert_eq!(inf_run()?, [true, true]);
            let invalid_ys = [0.0, 95.0];
            let error = data
                .geodesic_dwithin_points(
                    &frame_cache,
                    std::iter::zip(xs[..2].iter().copied(), invalid_ys.iter().copied()),
                    "EPSG:4326",
                    semi_major,
                    flattening,
                    metric,
                    f64::INFINITY,
                    metric.frame(),
                )
                .expect_err("invalid point-row latitude must be rejected");
            assert!(error.to_string().contains("invalid longitude/latitude"));
            Ok(())
        })?;
    }
    Ok(())
}
#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn public_dwithin_keeps_an_off_grid_interior_chart_witness() -> Result<()> {
    crate::test_support::initialize_python();
    let geodesic = Geodesic::wgs84();
    let cases = [
        (
            "long oblique",
            p(-70.0, 10.0),
            p(80.0, 55.0),
            0.314_159_265_358_979_3,
            0.0,
            0.0001,
        ),
        (
            "meridian between derivative nodes",
            p(0.0, -72.384_836_123_684_47),
            p(0.0, 72.384_836_123_684_47),
            0.314_159_265_358_979_3,
            0.001,
            0.0,
        ),
        (
            "near-pole parallel",
            p(-45.0, 80.0),
            p(45.0, 80.0),
            0.314_159_265_358_979_3,
            0.0,
            9.999_999_999,
        ),
        (
            "near-antipodal short edge",
            p(0.0, 0.0),
            p(1.0e-10, 0.0),
            0.314_159_265_358_979_3,
            179.999,
            0.001,
        ),
    ];
    let mut observations = Vec::with_capacity(cases.len());
    with_ellipsoid_metric("EPSG:4326", &[], |metric| {
        for (name, start, end, parameter, dx, dy) in cases {
            let witness = p(
                start.x + (end.x - start.x) * parameter,
                start.y + (end.y - start.y) * parameter,
            );
            let query = p(witness.x + dx, witness.y + dy);
            let attained: f64 = geodesic.inverse(query.y, query.x, witness.y, witness.x);
            let scale = attained.max(1.0);
            let rounding = 32.0 * (scale.next_up() - scale);
            let limit = (attained + 4.0e-11 * 6_378_137.0 + rounding).next_up();
            let edge = metric.chart_edge(start, end)?;
            let envelope_lower = metric.point_envelope_bound(query)(edge.bounds());
            let internal = metric.point_edge_query(
                query,
                &edge,
                envelope_lower,
                crate::geometry::PointEdgeQuery::Dwithin { limit },
                &mut PointEdgeScratch::default(),
            )?;
            let classification = match internal {
                crate::geometry::PointEdgeQueryResult::Within { .. } => "within",
                crate::geometry::PointEdgeQueryResult::Outside { lower_bound, .. } => {
                    assert!(
                        lower_bound > limit,
                        "{name}: outside requires lower {lower_bound} > limit {limit}"
                    );
                    "outside"
                },
                crate::geometry::PointEdgeQueryResult::Unresolved { .. } => "unresolved",
                other @ (PointEdgeQueryResult::Excluded | PointEdgeQueryResult::Minimum(_)) => {
                    panic!("{name}: unexpected threshold result {other:?}")
                },
            };
            observations.push((name, start, end, query, limit, attained, classification));
        }
        Ok(())
    })?;

    for (name, start, end, query, limit, attained, classification) in observations {
        Python::attach(|py| {
            let frame = Frame::new(Some(crs_arc_static("EPSG:4326")), None)
                .expect("WGS 84 is a valid geographic frame");
            let point = crate::Typed(crate::PyGeometry::with_frame(
                Shape::Point(query),
                frame.clone(),
            ))
            .into_pyobject(py)
            .expect("query point binds as its public typed geometry");
            let line = crate::Typed(crate::PyGeometry::with_frame(line(vec![start, end]), frame))
                .into_pyobject(py)
                .expect("source line binds as its public typed geometry");
            let distance = limit
                .into_pyobject(py)
                .expect("threshold binds as a Python float");
            let result = crate::dwithin(py, point.as_any(), line.as_any(), distance.as_any(), None);
            match classification {
                "within" => assert!(
                    result
                        .expect("a sampled chart witness within the radius returns True")
                        .bind(py)
                        .extract::<bool>()
                        .expect("dwithin result is bool")
                ),
                "unresolved" => assert!(
                    result
                        .expect_err("an unresolved threshold raises instead of returning False")
                        .to_string()
                        .contains("geodesic dwithin could not certify the threshold within its evaluation work limit")
                ),
                "outside" => {
                    assert!(
                        attained <= limit,
                        "{name}: independent point witness {attained} exceeds {limit}"
                    );
                    assert!(
                        result
                            .expect("independent interior witness proves the result within")
                            .bind(py)
                            .extract::<bool>()
                            .expect("dwithin result is bool")
                    );
                },
                _ => unreachable!("the internal result classified the threshold"),
            }
        });
    }
    Ok(())
}
