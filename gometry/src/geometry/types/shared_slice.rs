use std::ops::Deref;
use std::sync::Arc;

use crate::HeapSize;

/// Immutable shared ownership for a slice-facing payload.
///
/// Empty payloads have no allocation. Non-empty payloads retain the original
/// `Vec` so its capacity remains part of retained heap accounting.
#[expect(
    clippy::rc_buffer,
    reason = "nonempty owner is Arc<Vec<T>> so retained capacity remains visible to heap accounting; Arc<[T]> would lose it"
)]
pub(crate) struct SharedSlice<T> {
    owner: Option<Arc<Vec<T>>>,
}

impl<T> SharedSlice<T> {
    pub(crate) const fn empty() -> Self {
        Self { owner: None }
    }

    pub(crate) fn from_vec(values: Vec<T>) -> Self {
        if values.is_empty() {
            return Self::empty();
        }
        Self {
            owner: Some(Arc::new(values)),
        }
    }

    /// Adopt an existing vector owner without allocating another owner.
    /// Empty vectors use the canonical no-owner representation.
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

    /// Retained bytes for this owner with caller-supplied nested accounting.
    /// The allocation keeps its actual vector capacity; only the nested
    /// values vary between full owners and logical views.
    pub(crate) fn heap_bytes_with(&self, nested: impl Fn(&T) -> usize) -> usize {
        self.owner.as_ref().map_or(0, |owner| {
            crate::heap_size::ARC_CONTROL_BYTES
                + std::mem::size_of::<Vec<T>>()
                + owner.capacity() * std::mem::size_of::<T>()
                + owner.iter().map(nested).sum::<usize>()
        })
    }

    /// Logical bytes for a contiguous view into this owner. A full-owner view
    /// retains the vector's capacity; a sub-view retains only its visible
    /// element payload and the shared owner/header.
    pub(crate) fn logical_heap_bytes(&self, range: std::ops::Range<usize>) -> usize
    where
        T: HeapSize,
    {
        let Some(owner) = self.owner.as_ref() else {
            return 0;
        };
        debug_assert!(range.end <= owner.len());
        if range.start == 0 && range.end == owner.len() {
            return self.heap_bytes();
        }
        crate::heap_size::ARC_CONTROL_BYTES
            + std::mem::size_of::<Vec<T>>()
            + range.len() * std::mem::size_of::<T>()
            + owner[range].iter().map(HeapSize::heap_bytes).sum::<usize>()
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
        self.heap_bytes_with(HeapSize::heap_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::SharedSlice;
    use crate::geometry::{
        CoordSeq, CoordWindow, CoordinateAxes, MultiMember, MultiPolygon, Point, Polygon, Ring,
        Shape,
    };
    use crate::heap_size::HeapSize as _;
    use crate::test_support::allocation_count;

    fn square(offset: f64) -> Polygon {
        let points = [
            Point::new_raw_xy(offset, 0.0),
            Point::new_raw_xy(offset + 4.0, 0.0),
            Point::new_raw_xy(offset + 4.0, 4.0),
            Point::new_raw_xy(offset, 4.0),
            Point::new_raw_xy(offset, 0.0),
        ];
        Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        )
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
            )
            .unwrap(),
            Vec::new(),
        );
        let holed = Polygon::new(shell.shell, vec![hole.shell]);
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
    fn concat_axes_mismatch_is_none() {
        let left = CoordSeq::try_from_vecs(vec![0.0], vec![0.0], None, None).unwrap();
        let xyz = CoordSeq::try_from_vecs(vec![1.0], vec![1.0], Some(vec![2.0]), None).unwrap();
        assert!(matches!(left.concat(&xyz), Ok(None)));
    }

    #[test]
    fn retained_accounting_includes_the_frozen_vector_capacity() {
        let mut values = Vec::with_capacity(17);
        values.extend([1.0, 2.0]);
        let capacity = values.capacity();
        let owner = SharedSlice::from_vec(values);
        assert_eq!(owner.capacity(), capacity);
        assert!(owner.heap_bytes() >= capacity * std::mem::size_of::<f64>());
    }
}
