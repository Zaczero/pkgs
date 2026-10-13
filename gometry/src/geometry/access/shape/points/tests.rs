use super::*;

#[cfg(test)]
mod unique_points_capacity_tests {
    use super::*;

    #[test]
    fn oversized_unique_point_capacity_is_only_a_hint() {
        let capacity = 1_usize << 57;
        // The vector layout fits, but the hash table cannot represent this
        // many points on the supported 64-bit target.
        let _layout = std::alloc::Layout::array::<Point>(capacity)
            .expect("point vector layout fits on the supported 64-bit target");

        let mut seen = HashSet::new();
        let mut unique = Vec::new();
        reserve_unique_points_capacity(capacity, &mut seen, &mut unique);

        assert!(seen.is_empty());
        assert!(unique.is_empty());
    }
}
