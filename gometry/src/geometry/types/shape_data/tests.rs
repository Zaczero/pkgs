use super::*;
use crate::geometry::PreparedPlan;

#[cfg(test)]
mod footprint_tests {
    use super::*;

    // The footprint bounds this test used to assert are now `const _`
    // assertions beside the struct definition, so they fail the build rather
    // than only `cargo test`.

    #[test]
    fn retained_heap_bytes_does_not_force_caches() {
        let data = PreparedPlan::new(Shape::Point(Point::new_raw_xy(1.0, 2.0)));
        let cold = data.heap_bytes();
        assert!(data.metric.get().is_none());
        assert!(data.topology.get().is_none());
        assert_eq!(data.heap_bytes(), cold);
        let _ = data.bounds();
        // Bounds are owned by ShapeData; they do not force a boxed product.
        assert!(data.metric.get().is_none());
        assert!(data.topology.get().is_none());
        assert_eq!(data.heap_bytes(), cold);
        let _ = data.distance_parts().unwrap();
        assert!(data.metric.get().is_some());
        assert!(data.heap_bytes() > cold);
    }

    #[test]
    fn nested_metric_product_initializes_once_and_reuses_storage() {
        let plan = PreparedPlan::new(Shape::Point(Point::new_raw_xy(1.0, 2.0)));
        plan.metric();
        assert!(plan.metric.get().expect("sidecar").distance.get().is_none());
        let first = std::ptr::from_ref(plan.distance_parts().expect("initialize distance parts"));
        let second = std::ptr::from_ref(crate::test_support::assert_no_allocations(|| {
            plan.distance_parts().expect("cached distance parts")
        }));
        assert_eq!(first, second);
    }
}
