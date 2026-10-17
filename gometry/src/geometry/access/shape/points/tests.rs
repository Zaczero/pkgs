use super::*;

#[cfg(test)]
mod unique_points_capacity_tests {
    use super::*;

    #[test]
    fn unique_point_capacity_reserves_both_outputs_without_publishing_values() {
        let capacity = 17;
        let mut seen = HashSet::new();
        let mut unique = Vec::new();
        reserve_unique_points_capacity(capacity, &mut seen, &mut unique);

        assert!(seen.is_empty());
        assert!(unique.is_empty());
        assert!(seen.capacity() >= capacity);
        assert!(unique.capacity() >= capacity);
    }
}
