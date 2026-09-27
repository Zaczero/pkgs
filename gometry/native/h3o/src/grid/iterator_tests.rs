use core::convert::TryFrom;

use super::*;

#[test]
fn stop_after_none() {
    let origin = CellIndex::try_from(0x8908000001bffff).expect("origin");
    let iter = DiskDistancesUnsafe::new(origin, 1);

    assert_eq!(iter.count(), 5);
}

#[test]
fn fast_ring_size_hint_obeys_the_iterator_lower_bound() {
    // gometry: A disk bound is not a valid lower bound for fast ring output.
    let origin = CellIndex::try_from(0x8a1fb46622dffff).expect("origin");
    let mut ring = RingUnsafe::new(origin, 2).expect("ring avoids pentagonal distortion");
    let (lower, upper) = ring.size_hint();
    let count = ring.by_ref().take(13).count();

    assert_eq!(count, 12);
    assert!(lower <= count);
    if let Some(upper) = upper {
        assert!(count <= upper);
    }
}
