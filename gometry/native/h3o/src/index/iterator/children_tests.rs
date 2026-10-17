use core::convert::TryFrom;

use super::*;

#[test]
fn disk_distance_safe_is_fused() {
    let origin = CellIndex::try_from(0x85002C27FFFFFFF).unwrap();
    let mut iter = Children::new(origin, Resolution::Six);

    for _ in 0..7 {
        assert!(iter.next().is_some());
    }
    for _ in 0..7 {
        assert!(iter.next().is_none());
    }
}
