use std::ops::Deref;
use std::sync::Arc;

use crate::HeapSize;
use crate::error::Result;

/// Immutable, allocation-fallible ownership for a slice-facing payload.
///
/// Empty payloads have no allocation. Non-empty payloads retain the original
/// `Vec` so its capacity remains part of retained heap accounting.
#[expect(
    clippy::rc_buffer,
    reason = "nonempty owner is a fallibly allocated Arc<Vec<T>> so capacity remains in heap accounting; Arc<[T]> would lose that publication boundary"
)]
pub(crate) struct SharedSlice<T> {
    owner: Option<Arc<Vec<T>>>,
}

impl<T> SharedSlice<T> {
    pub(crate) const fn empty() -> Self {
        Self { owner: None }
    }

    pub(crate) fn try_from_vec(values: Vec<T>) -> Result<Self> {
        if values.is_empty() {
            return Ok(Self::empty());
        }
        Ok(Self {
            owner: Some(Arc::try_new(values).map_err(crate::error::Error::from)?),
        })
    }

    /// Adopt an already fallibly allocated vector without allocating another
    /// owner. Empty vectors use the canonical no-owner representation.
    #[expect(
        clippy::rc_buffer,
        reason = "the Arrow decoder transfers its already fallibly allocated Arc<Vec> owner"
    )]
    pub(crate) fn from_arc_vec(owner: Arc<Vec<T>>) -> Self {
        if owner.is_empty() {
            return Self::empty();
        }
        Self { owner: Some(owner) }
    }

    pub(crate) fn as_slice(&self) -> &[T] {
        self.owner.as_deref().map_or(&[], Vec::as_slice)
    }

    pub(crate) fn len(&self) -> usize {
        self.as_slice().len()
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.owner.as_deref().map_or(0, Vec::capacity)
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.owner.is_none()
    }

    #[expect(
        clippy::rc_buffer,
        reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
    )]
    #[cfg(test)]
    pub(crate) fn owner(&self) -> Option<Arc<Vec<T>>> {
        self.owner.clone()
    }

    pub(crate) fn iter_owned(&self) -> SharedSliceIter<T>
    where
        T: Clone,
    {
        SharedSliceIter {
            owner: self.clone(),
            index: 0,
        }
    }
}

pub struct SharedSliceIter<T> {
    owner: SharedSlice<T>,
    index: usize,
}

impl<T: Clone> Iterator for SharedSliceIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.owner.as_slice().get(self.index)?.clone();
        self.index += 1;
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.owner.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

impl<T: Clone> ExactSizeIterator for SharedSliceIter<T> {}

impl<T> Clone for SharedSlice<T> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
        }
    }
}

impl<T: PartialEq> PartialEq for SharedSlice<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: Eq> Eq for SharedSlice<T> {}

impl<T: std::hash::Hash> std::hash::Hash for SharedSlice<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

impl<T> Deref for SharedSlice<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for SharedSlice<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_list().entries(self.as_slice()).finish()
    }
}

impl<T: HeapSize> HeapSize for SharedSlice<T> {
    fn heap_bytes(&self) -> usize {
        self.owner.heap_bytes()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::SharedSlice;
    use crate::geometry::{
        CoordSeq, CoordWindow, CoordinateAxes, MultiMember, MultiPolygon, Point, Polygon, Ring,
        Shape,
    };
    use crate::heap_size::HeapSize as _;
    use crate::test_support::{allocation_count, fail_allocations_after};

    #[derive(Clone)]
    struct DropProbe(Arc<AtomicUsize>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn square(offset: f64) -> Polygon {
        let points = [
            Point::new_raw_xy(offset, 0.0),
            Point::new_raw_xy(offset + 4.0, 0.0),
            Point::new_raw_xy(offset + 4.0, 4.0),
            Point::new_raw_xy(offset, 4.0),
            Point::new_raw_xy(offset, 0.0),
        ];
        Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()),
            Vec::new(),
        )
        .unwrap()
    }

    #[test]
    fn empty_axes_preserve_present_z_and_m_without_owner_allocations() {
        for axes in [
            CoordinateAxes::XY,
            CoordinateAxes::XYZ,
            CoordinateAxes::XYM,
            CoordinateAxes::XYZM,
        ] {
            let (sequence, allocations) = allocation_count(|| CoordSeq::empty(axes));
            assert_eq!(allocations, 0);
            assert_eq!(sequence.axes(), axes);
            assert_eq!(sequence.zs().is_some(), axes.has_z());
            assert_eq!(sequence.ms().is_some(), axes.has_m());
        }
    }

    #[test]
    fn nested_views_advance_local_m_offset_without_copying() {
        let sequence = CoordSeq::try_from_vecs(
            (0..6).map(f64::from).collect(),
            vec![0.0; 6],
            None,
            Some((10..16).map(f64::from).collect()),
        )
        .unwrap();
        let outer = sequence.view(CoordWindow::from_range(1..5, sequence.len()));
        let nested = outer.view(CoordWindow::from_range(1..3, outer.len()));
        assert_eq!(outer.ms(), Some(&[11.0, 12.0, 13.0, 14.0][..]));
        assert_eq!(nested.ms(), Some(&[12.0, 13.0][..]));
        assert_eq!(nested.xs(), &[2.0, 3.0]);
    }

    #[test]
    fn shape_clone_with_holes_sparse_multipart_and_collection_is_allocation_free() {
        let shell = square(0.0);
        let hole = Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(1.0, 1.0),
                    Point::new_raw_xy(2.0, 1.0),
                    Point::new_raw_xy(2.0, 2.0),
                    Point::new_raw_xy(1.0, 2.0),
                    Point::new_raw_xy(1.0, 1.0),
                ])
                .unwrap(),
            ),
            Vec::new(),
        )
        .unwrap();
        let holed = Polygon::new(shell.shell, vec![hole.shell]).unwrap();
        let multipart = MultiPolygon::try_new(CoordinateAxes::XY, vec![
            MultiMember::Value(holed.clone()),
            MultiMember::Empty,
            MultiMember::Value(square(8.0)),
        ])
        .unwrap();
        let shape =
            Shape::geometry_collection(vec![Shape::Polygon(holed), Shape::MultiPolygon(multipart)])
                .unwrap();
        let (clone, allocations) = allocation_count(|| shape.clone());
        assert_eq!(clone, shape);
        assert_eq!(allocations, 0);
    }

    #[test]
    fn failed_freeze_drops_non_copy_values() {
        let drops = Arc::new(AtomicUsize::new(0));
        let values = vec![DropProbe(Arc::clone(&drops))];
        let Err(error) = fail_allocations_after(0, || SharedSlice::try_from_vec(values)) else {
            panic!("freeze must fail under allocation rejection");
        };
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::Allocation)
        ));
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn concat_reports_allocation_error_but_axes_mismatch_is_none() {
        let left = CoordSeq::try_from_vecs(vec![0.0], vec![0.0], None, None).unwrap();
        let right = CoordSeq::try_from_vecs(vec![1.0], vec![1.0], None, None).unwrap();
        let xyz = CoordSeq::try_from_vecs(vec![1.0], vec![1.0], Some(vec![2.0]), None).unwrap();
        assert!(matches!(left.concat(&xyz), Ok(None)));
        fail_allocations_after(0, || left.concat(&right)).unwrap_err();
    }

    #[test]
    fn retained_accounting_includes_the_frozen_vector_capacity() {
        let mut values = Vec::with_capacity(17);
        values.extend([1.0, 2.0]);
        let capacity = values.capacity();
        let owner = SharedSlice::try_from_vec(values).unwrap();
        assert_eq!(owner.capacity(), capacity);
        assert!(owner.heap_bytes() >= capacity * std::mem::size_of::<f64>());
    }
}
