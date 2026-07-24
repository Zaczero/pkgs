#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Lazy per-row prepared handles for packed array rows.

use std::sync::{Arc, OnceLock};

use super::*;
use crate::geometry::{FrameDependentCaches, PointBatchTester, Shape, ShapeData};
use crate::heap_size::HeapSize;

/// Sidecar cache: one [`OnceLock`] per logical row, shared across array clones.
pub(crate) type PreparedRowCache = Arc<OnceLock<Arc<[OnceLock<Arc<ShapeData>>]>>>;

/// Frame-dependent row caches stored in one contiguous allocation.
///
/// Selections remap logical rows onto the original slots, so clones, slices,
/// and gathers share initialized caches without requiring one `Arc`
/// allocation per row.
#[derive(Clone, Debug)]
pub(crate) struct FrameCacheRows {
    slots: Arc<[OnceLock<Arc<FrameDependentCaches>>]>,
    selection: RowSelection,
}

impl FrameCacheRows {
    pub(crate) fn cache(&self, logical: usize) -> Arc<FrameDependentCaches> {
        let physical = self.selection.as_deref().physical(logical);
        self.slots[physical]
            .get_or_init(|| Arc::new(FrameDependentCaches::default()))
            .clone()
    }

    fn initialized(&self, logical: usize) -> Option<Arc<FrameDependentCaches>> {
        let physical = self.selection.as_deref().physical(logical);
        self.slots[physical].get().cloned()
    }

    fn selected(&self, rows: impl IntoIterator<Item = usize>) -> Self {
        let current = self.selection.as_deref();
        let mut first = None;
        let mut count = 0_usize;
        let mut gather = None::<Vec<usize>>;

        for logical in rows {
            let physical = current.physical(logical);
            if let Some(indices) = &mut gather {
                indices.push(physical);
            } else if let Some(start) = first {
                if physical != start + count {
                    let mut indices = Vec::with_capacity(count.saturating_add(1));
                    indices.extend(start..start + count);
                    indices.push(physical);
                    gather = Some(indices);
                }
            } else {
                first = Some(physical);
            }
            count += 1;
        }

        let selection = gather.map_or_else(
            || {
                let start = first.unwrap_or(0);
                if start == 0 && count == self.slots.len() {
                    RowSelection::Identity
                } else {
                    RowSelection::window_trusted(start, count, self.slots.len())
                }
            },
            |indices| RowSelection::gather_trusted(indices.into(), self.slots.len()),
        );
        Self {
            slots: Arc::clone(&self.slots),
            selection,
        }
    }

    pub(super) fn mapped_from(source: &Self, unchanged: impl IntoIterator<Item = bool>) -> Self {
        let unchanged: Vec<bool> = unchanged.into_iter().collect();
        if unchanged.iter().all(|&value| value) {
            return source.clone();
        }
        let slots: Arc<[OnceLock<Arc<FrameDependentCaches>>]> = unchanged
            .into_iter()
            .enumerate()
            .map(|(row, unchanged)| {
                let slot = OnceLock::new();
                if unchanged && let Some(cache) = source.initialized(row) {
                    let _ = slot.set(cache);
                }
                slot
            })
            .collect();
        Self {
            slots,
            selection: RowSelection::Identity,
        }
    }
}

impl HeapSize for FrameCacheRows {
    fn heap_bytes(&self) -> usize {
        self.slots.heap_bytes() + self.selection.heap_bytes()
    }
}

pub(crate) fn fresh_frame_caches(len: usize) -> FrameCacheRows {
    FrameCacheRows {
        slots: Arc::from_iter(std::iter::repeat_with(OnceLock::new).take(len)),
        selection: RowSelection::Identity,
    }
}

pub(crate) fn fresh_prepared_cache() -> PreparedRowCache {
    Arc::new(OnceLock::new())
}

/// Vertex count below which a packed row's transient handle is cheaper than
/// caching (facet-tree / band-raycaster build dominates tiny rings).
pub(crate) const PREPARED_ROW_MIN_COORDS: usize = PointBatchTester::MIN_PROBES;

impl PyGeometryArray {
    pub(crate) fn row_frame_cache(&self, index: usize) -> Arc<FrameDependentCaches> {
        self.frame_caches.cache(index)
    }

    pub(crate) fn selected_frame_caches(
        &self,
        rows: impl IntoIterator<Item = usize>,
    ) -> FrameCacheRows {
        self.frame_caches.selected(rows)
    }

    fn prepared_slots(&self) -> Arc<[OnceLock<Arc<ShapeData>>]> {
        self.prepared_cache
            .get_or_init(|| Arc::from(vec![OnceLock::new(); self.storage().len()]))
            .clone()
    }

    fn row_bounds(&self, index: usize) -> Option<Bounds> {
        self.cached_element_bounds()
            .as_ref()
            .and_then(|bounds| bounds.get(index).copied().flatten())
    }

    /// Run `f` on the row's prepared [`ShapeData`]: persistent handles for
    /// `Mixed` rows, cached handles for large packed line/polygon rows, and
    /// the existing transient stack handle for small packed rows.
    pub(crate) fn with_row_data<R>(
        &self,
        index: usize,
        row: ShapeRow<'_>,
        f: impl FnOnce(&ShapeData) -> R,
    ) -> R {
        match row {
            ShapeRow::Handle(handle) => f(handle),
            ShapeRow::Point(point) => f(&ShapeData::new(Shape::Point(point))),
            packed @ (ShapeRow::Line(..) | ShapeRow::Rings(..))
                if packed.packed_coord_count() >= PREPARED_ROW_MIN_COORDS =>
            {
                let bounds = self.row_bounds(index);
                let slots = self.prepared_slots();
                let handle = slots[index].get_or_init(|| Arc::new(packed.into_shape_data(bounds)));
                f(handle)
            },
            packed => f(&packed.into_shape_data(self.row_bounds(index))),
        }
    }
}

#[cfg(test)]
mod frame_cache_row_tests {
    use super::*;

    fn points() -> PyGeometryArray {
        PyGeometryArray::packed_points(
            CoordSeq::from_vecs(vec![0.0, 1.0, 2.0], vec![0.0, 1.0, 2.0], None, None),
            Frame::None,
        )
    }

    #[test]
    fn clones_slices_and_scalar_extraction_share_row_slots() {
        let array = points();
        let clone = array.clone();
        assert!(Arc::ptr_eq(
            &array.row_frame_cache(1),
            &clone.row_frame_cache(1)
        ));

        let slice = array.gather_logical_rows(&[2, 0]);
        assert!(Arc::ptr_eq(
            &array.row_frame_cache(2),
            &slice.row_frame_cache(0)
        ));
        let extracted =
            slice
                .storage()
                .geometry_at(0, slice.frame.clone(), slice.row_frame_cache(0));
        assert!(Arc::ptr_eq(
            &extracted.frame_cache,
            &array.row_frame_cache(2)
        ));
    }

    #[test]
    fn array_retag_reuses_storage_but_refreshes_every_frame_cache_slot() {
        let array = points();
        let retagged = array.retag_frame(
            Frame::new(Some(crs_arc_static("EPSG:4326")), None).expect("valid test frame"),
        );
        assert!(Arc::ptr_eq(array.storage_arc(), retagged.storage_arc()));
        for row in 0..array.storage().len() {
            assert!(!Arc::ptr_eq(
                &array.row_frame_cache(row),
                &retagged.row_frame_cache(row)
            ));
        }
    }

    #[test]
    fn frame_cache_slots_use_one_contiguous_allocation() {
        let array = points();
        assert_eq!(Arc::strong_count(&array.frame_caches.slots), 1);
        let gathered = array.gather_logical_rows(&[2, 0]);
        assert!(Arc::ptr_eq(
            &array.frame_caches.slots,
            &gathered.frame_caches.slots
        ));
    }
}
