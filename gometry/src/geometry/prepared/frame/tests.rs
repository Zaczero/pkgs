use super::*;

#[cfg(test)]
mod frame_cache_tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;
    use crate::geographic::GeographicFrame;
    use crate::geometry::{CoordSeq, LineSeq};
    use crate::test_support::allocation_count;
    use crate::{PlanarMetric, Point, PreparedPlan, Shape};

    fn retains_key(cache: &FrameDependentCaches, key: &GeographicMetricKey) -> bool {
        cache
            .entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|(candidate, _)| candidate == key)
    }

    fn cached_measured_locate(shape: &PreparedPlan, point: Point) -> crate::error::Result<f64> {
        shape.measured_line_index()?.locate(point, &PlanarMetric)
    }

    #[test]
    fn frame_cache_key_includes_crs_ellipsoid_and_runtime_generation() {
        let base = GeographicMetricKey::new_at_generation(
            "EPSG:4326",
            6_378_137.0,
            1.0 / 298.257_223_563,
            4,
        );
        assert_ne!(
            base,
            GeographicMetricKey::new_at_generation(
                "EPSG:4267",
                6_378_137.0,
                1.0 / 298.257_223_563,
                4,
            )
        );
        assert_ne!(
            base,
            GeographicMetricKey::new_at_generation(
                "EPSG:4326",
                6_378_136.0,
                1.0 / 298.257_223_563,
                4,
            )
        );
        assert_ne!(
            base,
            GeographicMetricKey::new_at_generation(
                "EPSG:4326",
                6_378_137.0,
                1.0 / 298.257_223_563,
                5,
            )
        );
    }

    #[test]
    fn differing_frame_or_generation_replaces_the_single_entry() {
        let cache = FrameDependentCaches::default();
        let first = GeographicMetricKey::new_at_generation(
            "EPSG:4326",
            6_378_137.0,
            1.0 / 298.257_223_563,
            4,
        );
        let next_generation = GeographicMetricKey::new_at_generation(
            "EPSG:4326",
            6_378_137.0,
            1.0 / 298.257_223_563,
            5,
        );
        let other_frame = GeographicMetricKey::new_at_generation(
            "EPSG:4267",
            6_378_206.4,
            1.0 / 294.978_698_2,
            5,
        );
        assert!(cache.merge(first.clone(), |_| {}).is_some());
        assert!(retains_key(&cache, &first));
        assert!(cache.merge(next_generation.clone(), |_| {}).is_some());
        assert!(!retains_key(&cache, &first));
        assert!(retains_key(&cache, &next_generation));
        assert!(cache.merge(other_frame.clone(), |_| {}).is_some());
        assert!(!retains_key(&cache, &next_generation));
        assert!(retains_key(&cache, &other_frame));
    }

    #[test]
    fn stale_generation_cannot_replace_a_newer_entry() {
        let cache = FrameDependentCaches::default();
        let older = GeographicMetricKey::new_at_generation("EPSG:4326", 6_378_137.0, 0.0, 4);
        let newer = GeographicMetricKey::new_at_generation("EPSG:4326", 6_378_137.0, 0.0, 5);
        assert!(cache.merge(newer.clone(), |_| {}).is_some());
        assert!(cache.merge(older.clone(), |_| {}).is_none());
        assert!(retains_key(&cache, &newer));
        assert!(!retains_key(&cache, &older));
    }

    #[test]
    fn frame_product_lookup_is_narrow_and_held_arcs_survive_replacement() {
        let shape = PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(vec![0.0, 1.0, 2.0], vec![0.0, 1.0, 0.0], None, None)
                    .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ));
        let cache = FrameDependentCaches::default();
        let key = GeographicMetricKey::new_at_generation("TEST", 6_378_137.0, 0.0, 3);
        let geodesic = geographiclib_rs::Geodesic::wgs84();
        let metric = crate::crs::EllipsoidMetric::for_geodesic_at_frame(
            &geodesic,
            3,
            GeographicFrame::crs84(),
        );
        let chart_parts = Arc::new(ChartParts::build(shape.shape(), &metric).unwrap());
        let line_index = Arc::new(
            ChartLineIndex::from_chart_parts(shape.shape(), Arc::clone(&chart_parts), &metric)
                .unwrap(),
        );
        let measured_parts = Arc::new(
            ChartParts::measured_route(
                [
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(1.0, 1.0),
                    Point::new_raw_xy(2.0, 0.0),
                ]
                .into_iter(),
                &metric,
            )
            .unwrap(),
        );
        cache
            .merge(key.clone(), |entry| {
                entry.chart_parts = Some(Arc::clone(&chart_parts));
                entry.line_index = Some(Arc::clone(&line_index));
                entry.measured_chart_parts = Some(Arc::clone(&measured_parts));
            })
            .unwrap();

        let (chart_count, line_count, measured_count) = (
            Arc::strong_count(&chart_parts),
            Arc::strong_count(&line_index),
            Arc::strong_count(&measured_parts),
        );
        let selected = cache.cached_chart_parts(&key).unwrap();
        assert_eq!(Arc::strong_count(&chart_parts), chart_count + 1);
        assert_eq!(Arc::strong_count(&line_index), line_count);
        assert_eq!(Arc::strong_count(&measured_parts), measured_count);
        drop(selected);

        let selected = cache.cached_line_index(&key).unwrap();
        assert_eq!(Arc::strong_count(&chart_parts), chart_count);
        assert_eq!(Arc::strong_count(&line_index), line_count + 1);
        assert_eq!(Arc::strong_count(&measured_parts), measured_count);
        let retained_line_index = Arc::clone(&selected);
        drop(selected);

        let selected = cache.cached_measured_chart_parts(&key).unwrap();
        assert_eq!(Arc::strong_count(&chart_parts), chart_count);
        assert_eq!(Arc::strong_count(&line_index), line_count + 1);
        assert_eq!(Arc::strong_count(&measured_parts), measured_count + 1);
        drop(selected);

        let empty = FrameDependentCaches::default();
        assert!(empty.cached_chart_parts(&key).is_none());
        assert!(empty.cached_line_index(&key).is_none());
        assert!(empty.cached_measured_chart_parts(&key).is_none());
        assert!(
            empty
                .entry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none()
        );

        let replacement = GeographicMetricKey::new_at_generation("TEST", 6_378_137.0, 0.0, 4);
        cache.merge(replacement, |_| {}).unwrap();
        assert_eq!(retained_line_index.parts().edges.len(), 2);
    }

    #[test]
    fn repeated_lrs_reuses_the_same_line_index_arc() {
        let shape = PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(vec![0.0, 1.0, 2.0], vec![0.0, 1.0, 0.0], None, None)
                    .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ));
        let cache = FrameDependentCaches::default();
        let geodesic = geographiclib_rs::Geodesic::wgs84();
        let metric = crate::crs::EllipsoidMetric::for_geodesic_at_frame(
            &geodesic,
            0,
            GeographicFrame::crs84(),
        );
        let admission =
            crate::geometry::GeographicPrepared::try_new(&shape, &cache, GeographicFrame::crs84())
                .expect("line admits in the metric frame");
        let first = shape
            .chart_line_index(
                &cache, "TEST", geodesic.a, geodesic.f, 0, &metric, admission,
            )
            .expect("line index");
        let second = shape
            .chart_line_index(
                &cache, "TEST", geodesic.a, geodesic.f, 0, &metric, admission,
            )
            .expect("cached line index");
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn repeated_measured_lrs_reuses_one_column_product() {
        let shape = PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(
                    vec![0.0, 1.0, 2.0],
                    vec![0.0, 1.0, 0.0],
                    None,
                    Some(vec![0.0, 1.0, 2.0]),
                )
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ));
        let first = std::ptr::from_ref(shape.measured_line_index().expect("measured line index"));
        let second = std::ptr::from_ref(
            shape
                .measured_line_index()
                .expect("cached measured line index"),
        );
        assert_eq!(first, second);
        assert!(shape.heap_bytes() > 0);
        let (interpolation, interpolation_allocations) = allocation_count(|| {
            shape
                .line_interpolate_point_m_with(1.5, &PlanarMetric)
                .expect("measured interpolation")
        });
        let (repeat_interpolation, repeat_interpolation_allocations) = allocation_count(|| {
            shape
                .line_interpolate_point_m_with(1.5, &PlanarMetric)
                .expect("repeated measured interpolation")
        });
        let (located, locate_allocations) = allocation_count(|| {
            cached_measured_locate(&shape, Point::new_raw_xy(1.0, 0.0)).expect("measured locate")
        });
        let (repeat_located, repeat_locate_allocations) = allocation_count(|| {
            cached_measured_locate(&shape, Point::new_raw_xy(1.0, 0.0))
                .expect("repeated measured locate")
        });
        assert_eq!(interpolation, repeat_interpolation);
        assert_eq!(located, repeat_located);
        assert_eq!(interpolation_allocations, 0);
        assert_eq!(repeat_interpolation_allocations, 0);
        assert_eq!(locate_allocations, 0);
        assert_eq!(repeat_locate_allocations, 0);
        let Shape::Point(interpolation) = interpolation else {
            panic!("measured interpolation must return a point");
        };
        assert_eq!(interpolation.x.to_bits(), 1.5_f64.to_bits());
        assert_eq!(interpolation.y.to_bits(), 0.5_f64.to_bits());
        assert_eq!(interpolation.m().unwrap().to_bits(), 1.5_f64.to_bits());
    }

    #[test]
    fn measured_planar_lrs_clamps_endpoints_and_carries_zm() {
        let shape = PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(
                    vec![0.0, 1.0, 2.0],
                    vec![0.0, 1.0, 0.0],
                    Some(vec![10.0, 20.0, 30.0]),
                    Some(vec![0.0, 1.0, 2.0]),
                )
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ));
        shape.measured_line_index().expect("measured line index");

        let (interpolation, allocations) = allocation_count(|| {
            shape
                .line_interpolate_point_m_with(1.5, &PlanarMetric)
                .expect("interior measured interpolation")
        });
        assert_eq!(allocations, 0);
        let Shape::Point(interpolation) = interpolation else {
            panic!("interior measured interpolation must return a point");
        };
        assert_eq!(interpolation.x.to_bits(), 1.5_f64.to_bits());
        assert_eq!(interpolation.y.to_bits(), 0.5_f64.to_bits());
        assert_eq!(interpolation.z().unwrap().to_bits(), 25.0_f64.to_bits());
        assert_eq!(interpolation.m().unwrap().to_bits(), 1.5_f64.to_bits());

        let Shape::Point(first) = shape
            .line_interpolate_point_m_with(-1.0, &PlanarMetric)
            .expect("lower measured clamp")
        else {
            panic!("lower measured clamp must return a point");
        };
        let Shape::Point(last) = shape
            .line_interpolate_point_m_with(3.0, &PlanarMetric)
            .expect("upper measured clamp")
        else {
            panic!("upper measured clamp must return a point");
        };
        assert_eq!(first.x.to_bits(), 0.0_f64.to_bits());
        assert_eq!(first.z().unwrap().to_bits(), 10.0_f64.to_bits());
        assert_eq!(first.m().unwrap().to_bits(), 0.0_f64.to_bits());
        assert_eq!(last.x.to_bits(), 2.0_f64.to_bits());
        assert_eq!(last.z().unwrap().to_bits(), 30.0_f64.to_bits());
        assert_eq!(last.m().unwrap().to_bits(), 2.0_f64.to_bits());

        let (located, allocations) = allocation_count(|| {
            cached_measured_locate(&shape, Point::new_raw_xy(1.0, 0.0))
                .expect("interior measured locate")
        });
        assert_eq!(located.to_bits(), 0.5_f64.to_bits());
        assert_eq!(allocations, 0);
    }

    #[test]
    fn measured_planar_lrs_skips_duplicate_zero_length_segment() {
        let shape = PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(
                    vec![0.0, 0.0, 2.0],
                    vec![0.0, 0.0, 0.0],
                    Some(vec![7.0, 7.0, 9.0]),
                    Some(vec![0.0, 0.0, 2.0]),
                )
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ));
        shape.measured_line_index().expect("measured line index");

        let (located, allocations) = allocation_count(|| {
            cached_measured_locate(&shape, Point::new_raw_xy(1.0, 1.0))
                .expect("locate past duplicate segment")
        });
        assert_eq!(located.to_bits(), 1.0_f64.to_bits());
        assert_eq!(allocations, 0);

        let Shape::Point(interpolated) = shape
            .line_interpolate_point_m_with(0.0, &PlanarMetric)
            .expect("duplicate endpoint interpolation")
        else {
            panic!("duplicate endpoint interpolation must return a point");
        };
        assert_eq!(interpolated.x.to_bits(), 0.0_f64.to_bits());
        assert_eq!(interpolated.z().unwrap().to_bits(), 7.0_f64.to_bits());
        assert_eq!(interpolated.m().unwrap().to_bits(), 0.0_f64.to_bits());
    }

    #[test]
    fn measured_planar_lrs_exact_locate_preserves_subnormal_projection() {
        let minimum = f64::from_bits(1);
        let maximum = f64::MAX;
        let shape = PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(
                    vec![0.0, maximum],
                    vec![0.0, 0.0],
                    None,
                    Some(vec![0.0, maximum]),
                )
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ));
        shape.measured_line_index().expect("measured line index");

        let located = cached_measured_locate(&shape, Point::new_raw_xy(minimum, 1.0))
            .expect("exact subnormal measured locate");
        assert_eq!(located.to_bits(), minimum.to_bits());
    }

    #[test]
    fn exact_measured_fraction_is_allocation_free() {
        let max = f64::MAX;
        let shape = PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(
                    vec![0.0, 2.0],
                    vec![0.0, 0.0],
                    Some(vec![10.0, 20.0]),
                    Some(vec![-max, max]),
                )
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ));
        shape.measured_line_index().expect("measured line index");

        let (_, allocations) = allocation_count(|| {
            shape
                .line_interpolate_point_m_with(0.0, &PlanarMetric)
                .expect("exact measured interpolation")
        });
        // Planar exact measured interpolation is intentionally allocation-free;
        // lock zero so a future heap spill fails rather than weakening coverage.
        assert_eq!(allocations, 0);
    }

    #[test]
    fn frame_entry_heap_bytes_survive_poison() {
        let shape = PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(vec![0.0, 1.0, 2.0], vec![0.0, 1.0, 0.0], None, None)
                    .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ));
        let cache = FrameDependentCaches::default();
        let geodesic = geographiclib_rs::Geodesic::wgs84();
        let metric = crate::crs::EllipsoidMetric::for_geodesic_at_frame(
            &geodesic,
            0,
            GeographicFrame::crs84(),
        );
        let admission =
            crate::geometry::GeographicPrepared::try_new(&shape, &cache, GeographicFrame::crs84())
                .expect("line admits in the metric frame");
        shape
            .chart_line_index(
                &cache, "TEST", geodesic.a, geodesic.f, 0, &metric, admission,
            )
            .expect("line index");
        let before = cache.heap_bytes();
        assert!(before > 0);

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = cache.entry.lock().unwrap();
            panic!("poison the frame entry mutex");
        }));
        assert!(result.is_err());

        assert_eq!(cache.heap_bytes(), before);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn prepared_geographic_exact_topology_heap_bytes_survive_poison() {
        let frame = crate::crs::info("EPSG:4326")
            .unwrap()
            .geographic_frame()
            .unwrap();
        let point = Shape::Point(Point::new_raw_xy(1.0, 2.0));
        let plan = PreparedPlan::new(point);
        let view = crate::geographic::ScalarGeographicView::try_new(frame, plan.shape())
            .expect("fixture admits");
        let topology = plan.topology();
        let products = topology.geographic_products(frame);
        let before = topology.heap_bytes();
        let exact = plan
            .geographic_exact_topology(view)
            .expect("fixture builds exact geographic topology");
        let expected = HeapSize::heap_bytes(&exact);
        assert!(expected > 0);
        assert_eq!(topology.heap_bytes(), before.saturating_add(expected));

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = topology.geographic.lock().unwrap();
            panic!("poison the exact geographic topology mutex");
        }));
        assert!(result.is_err());

        assert_eq!(topology.heap_bytes(), before.saturating_add(expected));
        assert!(Arc::ptr_eq(&products, &topology.geographic_products(frame)));
    }

    #[test]
    fn single_entry_container_adds_no_heap_allocation() {
        let cache = FrameDependentCaches::default();
        let key = GeographicMetricKey::new_at_generation("TEST", 1.0, 0.0, 0);
        let key_heap = key.heap_bytes();
        assert!(cache.merge(key, |_| {}).is_some());
        // The key's existing string storage and the empty entry account for
        // all retained bytes; unlike the former Vec-backed LRU, the container
        // itself retains no additional heap allocation.
        assert_eq!(cache.heap_bytes(), key_heap);
    }

    #[test]
    fn frame_cache_remains_one_entry_under_concurrent_replacement() {
        let cache = Arc::new(FrameDependentCaches::default());
        std::thread::scope(|scope| {
            for worker in 0..8_u64 {
                let cache = Arc::clone(&cache);
                scope.spawn(move || {
                    for generation in 0..64_u64 {
                        let key = GeographicMetricKey::new_at_generation(
                            "EPSG:4326",
                            6_378_137.0,
                            1.0 / 298.257_223_563,
                            generation + worker,
                        );
                        let _ = cache.merge(key, |_| {});
                    }
                });
            }
        });
        let latest = GeographicMetricKey::new_at_generation(
            "EPSG:4326",
            6_378_137.0,
            1.0 / 298.257_223_563,
            70,
        );
        assert!(retains_key(&cache, &latest));
    }
}
