use crate::error::Result;
use crate::test_support::allocation_count;

pub(super) fn assert_allocation_count(
    expected_allocations: usize,
    run: impl FnOnce() -> Result<()>,
) {
    let ((), allocations) = allocation_count(|| run().unwrap());
    assert_eq!(allocations, expected_allocations);
}
