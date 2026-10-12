    fn assert_allocation_count(
        expected_allocations: usize,
        run: impl FnOnce() -> crate::error::Result<()>,
    ) {
        let ((), allocations) = allocation_count(|| run().unwrap());
        assert_eq!(allocations, expected_allocations);
    }
