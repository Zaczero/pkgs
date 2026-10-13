use super::*;

const PUBLIC_HAUSDORFF_WORK_VERTEX_COUNTS: &[usize] = &[8, 64, 128];
const PUBLIC_HAUSDORFF_SOURCE_WORK_LIMIT: u64 = 5_000;
const PUBLIC_HAUSDORFF_POINT_EDGE_WORK_LIMIT: u64 = 250_000;

#[derive(Debug)]
pub(super) struct PublicHausdorffWork {
    pub(super) distance: f64,
    pub(super) source_evaluations: u64,
    pub(super) moving_evaluations: u64,
    pub(super) moving_endpoint_minima: u64,
    pub(super) target_candidates: u64,
    pub(super) point_edge_samples: u64,
}

pub(super) fn public_hausdorff_work(
    crs: &str,
    source: Shape,
    target: Shape,
    source_limit: u64,
    sample_limit: u64,
) -> PublicHausdorffWork {
    Python::initialize();
    Python::attach(|py| {
        let frame = Frame::new(Some(crs_arc(crs)), None).expect("geographic CRS");
        let source = crate::Typed(crate::PyGeometry::with_frame(source, frame.clone()))
            .into_pyobject(py)
            .expect("source line binds as a public typed geometry");
        let target = crate::Typed(crate::PyGeometry::with_frame(target, frame))
            .into_pyobject(py)
            .expect("target line binds as a public typed geometry");
        chart_hausdorff_counters::reset();
        chart_parts_counters::reset();
        point_edge_test_counters::reset();
        chart_hausdorff_counters::set_source_evaluation_limit(source_limit);
        point_edge_test_counters::set_sample_limit(sample_limit);

        let result = crate::hausdorff_distance(py, source.as_any(), target.as_any(), None, None);
        let source_evaluations = chart_hausdorff_counters::source_evaluations();
        let moving_evaluations = chart_hausdorff_counters::moving_correspondence_evaluations();
        let moving_endpoint_minima = chart_hausdorff_counters::moving_endpoint_minima();
        let target_candidates = chart_parts_counters::target_edge_candidates();
        let point_edge_samples = point_edge_test_counters::samples();
        point_edge_test_counters::reset();
        if let Err(error) = &result {
            eprintln!(
                "hausdorff-failure-work source={source_evaluations} moving={moving_evaluations} moving_endpoint_minima={moving_endpoint_minima} point_edge={point_edge_samples} bvh={target_candidates}: {error}"
            );
        }
        let distance = result
            .expect("public geographic Hausdorff call")
            .bind(py)
            .extract::<f64>()
            .expect("Hausdorff result is a float");
        PublicHausdorffWork {
            distance,
            source_evaluations,
            moving_evaluations,
            moving_endpoint_minima,
            target_candidates,
            point_edge_samples,
        }
    })
}

pub(super) fn disconnected_bvh_target() -> Shape {
    Shape::geometry_collection(
        (0..65)
            .map(|index| {
                let longitude = f64::from(index) * 2.0;
                line(vec![p(longitude, 0.0), p(longitude, 1.0)])
            })
            .collect(),
    )
    .expect("disconnected BVH target is valid")
}

pub(super) fn directed_chart_hausdorff(
    source: &Shape,
    target: &Shape,
    metric: &(impl GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>),
) -> ChartHausdorffWitness {
    let source_parts = ChartParts::build(source, metric).expect("source chart parts");
    let target_parts = ChartParts::build(target, metric).expect("target chart parts");
    let mut scratch = chart_scratch();
    chart_directed_hausdorff_distance(&source_parts, &target_parts, metric, &mut scratch)
        .expect("directed chart Hausdorff")
}

pub(super) fn parallel_point_distance(geodesic: &Geodesic, colatitude: f64, parameter: f64) -> f64 {
    geodesic.inverse(90.0 - colatitude, 10.0, 85.0, 179.0 * parameter)
}

pub(super) fn parallel_point_maximum_oracle(geodesic: &Geodesic, colatitude: f64) -> f64 {
    const LAST_SAMPLE: u32 = 4_000;
    let distances = (0..=LAST_SAMPLE)
        .map(|index| {
            parallel_point_distance(
                geodesic,
                colatitude,
                f64::from(index) / f64::from(LAST_SAMPLE),
            )
        })
        .collect::<Vec<_>>();
    let (maximum_index, mut maximum) = distances
        .iter()
        .copied()
        .enumerate()
        .max_by(|left, right| left.1.total_cmp(&right.1))
        .expect("the oracle includes the parallel endpoints");
    let step = 1.0 / f64::from(LAST_SAMPLE);
    let mut lower = (f64::from(maximum_index as u32) * step - step).max(0.0);
    let mut upper = (f64::from(maximum_index as u32) * step + step).min(1.0);
    let golden = (5.0_f64.sqrt() - 1.0) / 2.0;
    let mut left = upper - golden * (upper - lower);
    let mut right = lower + golden * (upper - lower);
    let mut left_value = parallel_point_distance(geodesic, colatitude, left);
    let mut right_value = parallel_point_distance(geodesic, colatitude, right);
    for _ in 0..80 {
        if left_value >= right_value {
            upper = right;
            right = left;
            right_value = left_value;
            left = upper - golden * (upper - lower);
            left_value = parallel_point_distance(geodesic, colatitude, left);
        } else {
            lower = left;
            left = right;
            left_value = right_value;
            right = lower + golden * (upper - lower);
            right_value = parallel_point_distance(geodesic, colatitude, right);
        }
    }
    maximum = maximum.max(left_value).max(right_value);
    maximum.max(
        parallel_point_distance(geodesic, colatitude, 0.0)
            .max(parallel_point_distance(geodesic, colatitude, 1.0)),
    )
}

pub(super) fn chart_edge_point_distance(
    geodesic: &Geodesic,
    start: Point,
    end: Point,
    target: Point,
    parameter: f64,
) -> f64 {
    let mut longitude_delta = end.x - start.x;
    if longitude_delta > 180.0 {
        longitude_delta -= 360.0;
    } else if longitude_delta <= -180.0 {
        longitude_delta += 360.0;
    }
    let longitude = start.x + longitude_delta * parameter;
    let latitude = start.y + (end.y - start.y) * parameter;
    geodesic.inverse(target.y, target.x, latitude, longitude)
}

pub(super) fn chart_edge_point_maximum_oracle(start: Point, end: Point, target: Point) -> f64 {
    const LAST_SAMPLE: u32 = 4_000;
    let geodesic = Geodesic::wgs84();
    let distance = |parameter| chart_edge_point_distance(&geodesic, start, end, target, parameter);
    let samples = (0..=LAST_SAMPLE)
        .map(|index| distance(f64::from(index) / f64::from(LAST_SAMPLE)))
        .collect::<Vec<_>>();
    let (index, mut maximum) = samples
        .iter()
        .copied()
        .enumerate()
        .max_by(|left, right| left.1.total_cmp(&right.1))
        .expect("the oracle includes both chart-edge endpoints");
    let step = 1.0 / f64::from(LAST_SAMPLE);
    let mut lower = (index as f64 * step - step).max(0.0);
    let mut upper = (index as f64 * step + step).min(1.0);
    let golden = (5.0_f64.sqrt() - 1.0) / 2.0;
    let mut left = upper - golden * (upper - lower);
    let mut right = lower + golden * (upper - lower);
    let mut left_value = distance(left);
    let mut right_value = distance(right);
    for _ in 0..80 {
        if left_value >= right_value {
            upper = right;
            right = left;
            right_value = left_value;
            left = upper - golden * (upper - lower);
            left_value = distance(left);
        } else {
            lower = left;
            left = right;
            left_value = right_value;
            right = lower + golden * (upper - lower);
            right_value = distance(right);
        }
    }
    maximum = maximum.max(left_value).max(right_value);
    maximum.max(distance(0.0).max(distance(1.0)))
}

#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
pub(super) fn assert_chart_edge_point_hausdorff(
    name: &str,
    start: Point,
    end: Point,
    target_point: Point,
) -> Result<()> {
    let source = line(vec![start, end]);
    let target = Shape::Point(target_point);
    let expected = chart_edge_point_maximum_oracle(start, end, target_point);
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        let witness = directed_chart_hausdorff(&source, &target, metric);
        let attained = metric.segment_length(witness.source, witness.target);
        let scale = expected.max(witness.distance).max(1.0);
        let rounding = 32.0 * (scale.next_up() - scale);
        assert!(
            witness.distance <= expected + rounding,
            "{name}: reported {} above the independent maximum {expected}",
            witness.distance,
        );
        assert!(
            expected - witness.distance <= 0.01 + rounding,
            "{name}: reported {} below the independent maximum {expected}",
            witness.distance,
        );
        assert!(
            (attained - witness.distance).abs() <= rounding,
            "{name}: witness distance {attained} does not attain {}",
            witness.distance,
        );
        Ok(())
    })?;
    Ok(())
}

#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
pub(super) fn assert_chart_pair_contract<M>(
    left_shape: &Shape,
    right_shape: &Shape,
    expected_intersects: bool,
    metric: &M,
    semi_major: f64,
    flattening: f64,
) -> Result<()>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    let left = PreparedPlan::new(left_shape.clone());
    let right = PreparedPlan::new(right_shape.clone());
    let left_cache = FrameDependentCaches::default();
    let right_cache = FrameDependentCaches::default();
    let frame = metric.chart_frame();
    let left_topology = GeographicPrepared::try_new(&left, &left_cache, frame)?;
    let right_topology = GeographicPrepared::try_new(&right, &right_cache, frame)?;
    let intersects = graph_relate_data_geographic(left_topology, right_topology)?.is_intersects();
    assert_eq!(intersects, expected_intersects);

    let distance = left.geodesic_distance_cached(
        &left_cache,
        &right,
        &right_cache,
        "EPSG:4326",
        semi_major,
        flattening,
        metric,
    )?;
    // Exact topology proves zero for intersections; a near-zero disjoint
    // metric result does not decide exact topology.
    if intersects {
        assert_eq!(distance, 0.0);
    }
    assert_eq!(
        left.geodesic_dwithin_cached(
            &left_cache,
            &right,
            &right_cache,
            "EPSG:4326",
            semi_major,
            flattening,
            metric,
            0.0,
        )?,
        intersects
    );
    let nearest = left
        .geodesic_nearest_points_cached(
            &left_cache,
            &right,
            &right_cache,
            "EPSG:4326",
            semi_major,
            flattening,
            metric,
        )?
        .expect("the fixture operands are non-empty");
    let attained = metric.segment_length(nearest.0, nearest.1);
    let scale = distance.abs().max(attained.abs()).max(1.0);
    let rounding_floor = 32.0 * (scale.next_up() - scale);
    assert!(
        (distance - attained).abs() <= rounding_floor,
        "distance {distance} was not attained by nearest witnesses ({attained})"
    );
    if distance > 0.02 {
        assert!(!left.geodesic_dwithin_cached(
            &left_cache,
            &right,
            &right_cache,
            "EPSG:4326",
            semi_major,
            flattening,
            metric,
            distance - 0.01,
        )?);
        assert!(left.geodesic_dwithin_cached(
            &left_cache,
            &right,
            &right_cache,
            "EPSG:4326",
            semi_major,
            flattening,
            metric,
            distance + 0.01,
        )?);
    }
    Ok(())
}

pub(super) fn assert_public_hausdorff_work_scaling(
    name: &str,
    growing_span: bool,
    latitude_origin: f64,
    latitude_slope: f64,
) {
    let mut rows = Vec::with_capacity(PUBLIC_HAUSDORFF_WORK_VERTEX_COUNTS.len());
    for &vertex_count in PUBLIC_HAUSDORFF_WORK_VERTEX_COUNTS {
        let mut source_points = Vec::with_capacity(vertex_count);
        let edge_count = u32::try_from(vertex_count)
            .expect("test vertex count fits u32")
            .checked_sub(1)
            .expect("work-scaling supports contain at least two vertices");
        for index in 0..vertex_count {
            let index = f64::from(u32::try_from(index).expect("test vertex index fits u32"));
            let longitude = if growing_span {
                10.0 + 0.1 * index
            } else {
                10.0 + 20.0 * index / f64::from(edge_count)
            };
            let latitude = latitude_origin + latitude_slope * (longitude - 10.0);
            source_points.push(p(longitude, latitude));
        }
        let first = *source_points
            .first()
            .expect("work-scaling support has at least two vertices");
        let last = *source_points
            .last()
            .expect("work-scaling support has at least two vertices");
        let source = line(source_points);
        let target = line(vec![
            p(first.x + 1.0e-6, first.y + 0.01),
            p(last.x + 1.0e-6, last.y + 0.01),
        ]);
        let work = public_hausdorff_work(
            "EPSG:4326",
            source,
            target,
            PUBLIC_HAUSDORFF_SOURCE_WORK_LIMIT,
            PUBLIC_HAUSDORFF_POINT_EDGE_WORK_LIMIT,
        );
        assert!(
            work.source_evaluations <= PUBLIC_HAUSDORFF_SOURCE_WORK_LIMIT,
            "{name} {vertex_count}: {work:?}"
        );
        assert!(
            work.point_edge_samples <= PUBLIC_HAUSDORFF_POINT_EDGE_WORK_LIMIT,
            "{name} {vertex_count}: {work:?}"
        );
        eprintln!(
            "hausdorff-public-symmetric-scaling {name} vertices={vertex_count} source={} moving={} endpoint-minima={} point-edge={} bvh={}",
            work.source_evaluations,
            work.moving_evaluations,
            work.moving_endpoint_minima,
            work.point_edge_samples,
            work.target_candidates,
        );
        rows.push((vertex_count, work));
    }

    for pair in rows.windows(2) {
        let [(small_vertices, small), (large_vertices, large)] = pair else {
            unreachable!("adjacent work-scaling pairs have two rows");
        };
        for (counter, small_work, large_work) in [
            ("source", small.source_evaluations, large.source_evaluations),
            ("moving", small.moving_evaluations, large.moving_evaluations),
            (
                "moving-endpoint-minima",
                small.moving_endpoint_minima,
                large.moving_endpoint_minima,
            ),
            (
                "point-edge",
                small.point_edge_samples,
                large.point_edge_samples,
            ),
            ("bvh", small.target_candidates, large.target_candidates),
        ] {
            let exponent = ((large_work.max(1) as f64) / (small_work.max(1) as f64)).ln()
                / ((*large_vertices as f64) / (*small_vertices as f64)).ln();
            eprintln!(
                "hausdorff-public-symmetric-scaling-exponent {name} {counter} {small_vertices}->{large_vertices}: {exponent:.3}"
            );
            assert!(
                exponent <= 1.4,
                "{name} {counter} work exponent {exponent:.3} exceeds 1.4"
            );
        }
    }
}
