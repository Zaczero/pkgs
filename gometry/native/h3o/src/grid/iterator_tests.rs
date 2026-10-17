use core::convert::TryFrom;

use super::*;

#[test]
fn disk_distance_safe_is_fused() {
    let origin = CellIndex::try_from(0x85002C27FFFFFFF).unwrap();
    let mut iter = DiskDistancesSafe::new(origin, 1);

    for _ in 0..7 {
        assert!(iter.next().is_some());
    }
    for _ in 0..7 {
        assert!(iter.next().is_none());
    }
}

#[test]
fn disk_distance_unsafe_stop_after_none() {
    let origin = CellIndex::try_from(0x8908000001BFFFF).expect("origin");
    let iter = DiskDistancesUnsafe::new(origin, 1);

    // Only 5 because a pentagon was encountered.
    assert_eq!(iter.count(), 5);
}

#[test]
fn fast_ring_size_hint_obeys_the_iterator_lower_bound() {
    // gometry: A disk bound is not a valid lower bound for fast ring output.
    let origin = CellIndex::try_from(0x8A1FB46622DFFFF).expect("origin");
    let mut ring = RingUnsafe::new(origin, 2).expect("ring avoids pentagonal distortion");
    let (lower, upper) = ring.size_hint();
    let count = ring.by_ref().take(13).count();

    assert_eq!(count, 12);
    assert!(lower <= count);
    if let Some(upper) = upper {
        assert!(count <= upper);
    }
}

#[test]
fn disk_distance_unsafe_is_fused() {
    let origin = CellIndex::try_from(0x85002C27FFFFFFF).unwrap();
    let mut iter = DiskDistancesUnsafe::new(origin, 1);

    for _ in 0..7 {
        assert!(iter.next().is_some());
    }
    for _ in 0..7 {
        assert!(iter.next().is_none());
    }
}

#[test]
fn ring_unsafe_is_fused() {
    let origin = CellIndex::try_from(0x85002C27FFFFFFF).unwrap();
    let mut iter = RingUnsafe::new(origin, 1).unwrap();

    for _ in 0..6 {
        assert!(iter.next().is_some());
    }
    for _ in 0..6 {
        assert!(iter.next().is_none());
    }
}
