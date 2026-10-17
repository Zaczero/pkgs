use core::convert::TryFrom;

use super::*;

#[test]
fn disk_distance_safe_is_fused() {
    let origin = CellIndex::try_from(0x85002C27FFFFFFF).unwrap();
    let destination = CellIndex::try_from(0x85015337FFFFFFF).unwrap();
    let mut iter = GridPathCells::new(origin, destination).unwrap();

    for _ in 0..9 {
        assert!(iter.next().is_some());
    }
    for _ in 0..9 {
        assert!(iter.next().is_none());
    }
}
