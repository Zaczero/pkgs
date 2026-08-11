//! Lazy per-row prepared handles for packed array rows.

use std::sync::{Arc, OnceLock};

use crate::array::{
    Bounds, GeometryArrayStorage, PyGeometry, PyGeometryArray, RowSelection, RowSelectionRef,
    ShapeRow,
};
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
        Arc::clone(self.slots[physical].get_or_init(|| Arc::new(FrameDependentCaches::default())))
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

    pub(super) fn mapped_from(source: &Self, unchanged: impl AsRef<[bool]>) -> Self {
        let unchanged = unchanged.as_ref();
        if unchanged.iter().all(|&value| value) {
            return source.clone();
        }
        let slots: Arc<[OnceLock<Arc<FrameDependentCaches>>]> = unchanged
            .iter()
            .enumerate()
            .map(|(row, &keep)| {
                let slot = OnceLock::new();
                if keep && let Some(cache) = source.initialized(row) {
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
        // Logical retained footprint: charge only the selected logical rows'
        // slots (plus selection metadata), not the full shared parent table.
        // A tiny window/gather over a large parent must not report the parent's
        // full OnceLock table as its own size (NumPy-view doctrine).
        let logical_len = match self.selection.as_deref() {
            RowSelectionRef::Identity => self.slots.len(),
            RowSelectionRef::Window { len, .. } => len,
            RowSelectionRef::Gather(map) => map.len(),
        };
        let slot_bytes = logical_len * std::mem::size_of::<OnceLock<Arc<FrameDependentCaches>>>();
        let initialized: usize = (0..logical_len)
            .filter_map(|logical| self.initialized(logical))
            .map(|cache| std::mem::size_of_val(cache.as_ref()) + cache.heap_bytes())
            .sum();
        slot_bytes + initialized + self.selection.heap_bytes()
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

/// Carry initialized prepared slots across a shared map when the corresponding
/// row shape was reused (`unchanged[i] == true`).
pub(crate) fn mapped_prepared_cache(
    source: &PreparedRowCache,
    unchanged: &[bool],
) -> PreparedRowCache {
    let Some(source_slots) = source.get() else {
        return fresh_prepared_cache();
    };
    if unchanged.iter().all(|&value| value) && source_slots.len() == unchanged.len() {
        return Arc::clone(source);
    }
    let slots: Arc<[OnceLock<Arc<ShapeData>>]> = unchanged
        .iter()
        .enumerate()
        .map(|(row, &keep)| {
            let slot = OnceLock::new();
            if keep && let Some(handle) = source_slots.get(row).and_then(OnceLock::get) {
                let _ = slot.set(Arc::clone(handle));
            }
            slot
        })
        .collect();
    let out = Arc::new(OnceLock::new());
    let _ = out.set(slots);
    out
}

/// Vertex count below which a packed row's transient handle is cheaper than
/// caching (`PointBatchTester` / hierarchical Y-stabbing build dominates tiny rings).
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
        Arc::clone(
            self.prepared_cache
                .get_or_init(|| Arc::from(vec![OnceLock::new(); self.storage().len()])),
        )
    }

    fn row_bounds(&self, index: usize) -> Option<Bounds> {
        self.cached_element_bounds()
            .as_ref()
            .and_then(|bounds| bounds.get(index).copied().flatten())
    }

    /// Run `f` on the row's prepared [`ShapeData`]: array-cached handles for
    /// mixed rows and large packed line/polygon rows, persistent handles for
    /// already-prepared scalars, and a transient stack handle for small
    /// packed rows.
    pub(crate) fn with_row_data<R>(
        &self,
        index: usize,
        row: ShapeRow<'_>,
        f: impl FnOnce(&ShapeData) -> R,
    ) -> R {
        match row {
            ShapeRow::Handle(handle) => f(handle),
            ShapeRow::Shape(shape) => {
                // Seed bounds only when the array-level element-bounds cache
                // exists. Seeding `None` means "known empty" — not "unknown".
                // Mixed storage has no packed fold until first materialization,
                // so leave ShapeData's OnceLock unset and let it compute.
                let slots = self.prepared_slots();
                let handle = slots[index].get_or_init(|| {
                    let data = ShapeData::new(shape.clone());
                    if let Some(element_bounds) = self.cached_element_bounds() {
                        Arc::new(
                            data.with_seeded_bounds(element_bounds.get(index).copied().flatten()),
                        )
                    } else {
                        Arc::new(data)
                    }
                });
                f(handle)
            },
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

    /// Scalar geometry for a logical row: mixed rows share one array-cached
    /// [`ShapeData`]; packed rows build a fresh handle (same as before).
    pub(crate) fn geometry_at(&self, index: usize) -> PyGeometry {
        let frame_cache = self.row_frame_cache(index);
        let frame = self.frame.clone();
        match self.storage() {
            GeometryArrayStorage::Mixed(shapes) => {
                let slots = self.prepared_slots();
                let shape = Arc::clone(
                    slots[index].get_or_init(|| Arc::new(ShapeData::new(shapes[index].clone()))),
                );
                PyGeometry {
                    shape,
                    frame_cache,
                    frame,
                }
            },
            _ => self.storage().geometry_at(index, frame, frame_cache),
        }
    }

    /// Retained heap of initialized prepared-row slots (mixed + large packed).
    ///
    /// Counts only this array's logical rows — a window/gather view must not
    /// charge the full parent prepared table.
    pub(crate) fn prepared_cache_heap_bytes(&self) -> usize {
        let Some(slots) = self.prepared_cache.get() else {
            return 0;
        };
        let n = self.storage().len().min(slots.len());
        (0..n).fold(0_usize, |acc, row| {
            acc + slots[row].get().map_or(0, |data| {
                std::mem::size_of_val(data.as_ref()) + data.retained_heap_bytes()
            })
        })
    }
}

#[cfg(test)]
mod frame_cache_row_tests {
    use super::*;
    use crate::boundary::{Frame, crs_arc_static};
    use crate::geometry::{CoordSeq, LineSeq, Point};

    fn points() -> PyGeometryArray {
        PyGeometryArray::packed_points(
            CoordSeq::from_vecs(vec![0.0, 1.0, 2.0], vec![0.0, 1.0, 2.0], None, None),
            Frame::None,
        )
    }

    fn mixed_shapes() -> PyGeometryArray {
        PyGeometryArray::mixed_shapes(
            vec![
                Shape::Point(Point::new_unchecked_xy(1.0, 2.0)),
                Shape::LineString(LineSeq::from_trusted(CoordSeq::from_vecs(
                    vec![0.0, 1.0],
                    vec![0.0, 1.0],
                    None,
                    None,
                ))),
                Shape::Point(Point::new_unchecked_xy(3.0, 4.0)),
            ],
            Frame::None,
        )
    }

    #[test]
    fn mixed_geometry_at_shares_prepared_shape_data_arc() {
        let array = mixed_shapes();
        let a = array.geometry_at(1);
        let b = array.geometry_at(1);
        assert!(
            Arc::ptr_eq(&a.shape, &b.shape),
            "arr[i] must reuse the array-side prepared ShapeData Arc"
        );
        assert!(Arc::ptr_eq(&a.frame_cache, &b.frame_cache));
        // Different rows stay independent.
        let c = array.geometry_at(0);
        assert!(!Arc::ptr_eq(&a.shape, &c.shape));
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
