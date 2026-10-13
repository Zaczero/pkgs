use super::*;
use crate::error::ErrorKind;

/// Exhaust `ShapesIter` and assert every remaining `size_hint` equals the
/// actual remaining yield count (TrustedLen contract).
fn assert_shapes_iter_exact(storage: &GeometryArrayStorage) {
    let expected_len = storage.len();
    let mut iter = storage.iter_shapes();
    let (lo, hi) = iter.size_hint();
    assert_eq!(lo, expected_len);
    assert_eq!(hi, Some(expected_len));
    let mut yielded = 0_usize;
    while iter.next().is_some() {
        yielded = yielded
            .checked_add(1)
            .expect("an iterator cannot yield more than usize::MAX rows");
        let remaining = expected_len
            .checked_sub(yielded)
            .expect("ShapesIter cannot yield beyond its exact size");
        let (lo, hi) = iter.size_hint();
        assert_eq!(lo, remaining, "size_hint lower after {yielded} yields");
        assert_eq!(
            hi,
            Some(remaining),
            "size_hint upper after {yielded} yields"
        );
    }
    assert_eq!(yielded, expected_len);
    assert_eq!(iter.size_hint(), (0, Some(0)));
    assert!(iter.next().is_none());
}

mod iter;
mod rows;
mod selection;
mod sizes;
