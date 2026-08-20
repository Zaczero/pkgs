//! Lazy per-row prepared handles for packed array rows.

use std::ops::Deref;
use std::sync::{Arc, OnceLock};

use crate::array::{
    GeometryArrayStorage, PyGeometry, PyGeometryArray, RowSelection, RowSelectionRef, ShapeRow,
};
use crate::geometry::{Bounds, FrameDependentCaches, Shape, ShapeData};
use crate::heap_size::HeapSize;

/// Sidecar cache: one [`OnceLock`] per logical row, shared across array clones.
pub(crate) type PreparedRowCache = Arc<OnceLock<Arc<[OnceLock<Arc<ShapeData>>]>>>;

#[expect(
    clippy::large_enum_variant,
    reason = "Transient avoids an allocation on the linear packed fast path"
)]
pub(crate) enum PreparedRow<'a> {
    Shared(&'a Arc<ShapeData>),
    Transient(ShapeData),
}

impl Deref for PreparedRow<'_> {
    type Target = ShapeData;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Shared(data) => data,
            Self::Transient(data) => data,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum BoundsSeed {
    Unset,
    Value(Option<Bounds>),
}

impl<'a> PreparedRow<'a> {
    pub(crate) fn transient(row: ShapeRow<'a>) -> Self {
        Self::from_row(row, BoundsSeed::Unset)
    }

    pub(crate) fn transient_with_bounds(row: ShapeRow<'a>, bounds: Option<Bounds>) -> Self {
        Self::from_row(row, BoundsSeed::Value(bounds))
    }

    pub(crate) fn transient_with_seed(row: ShapeRow<'a>, seed: BoundsSeed) -> Self {
        Self::from_row(row, seed)
    }

    fn from_row(row: ShapeRow<'a>, seeded_bounds: BoundsSeed) -> Self {
        let data = match row {
            ShapeRow::Handle(handle) => return Self::Shared(handle),
            ShapeRow::Shape(shape) => ShapeData::new(shape.clone()),
            ShapeRow::Point(point) => ShapeData::new(Shape::Point(point)),
            ShapeRow::Line(coords, start, end) => ShapeData::new(Shape::LineString(
                crate::geometry::LineSeq::from_trusted(coords.view(
                    crate::geometry::CoordWindow::trusted(start..end, coords.len()),
                )),
            )),
            ShapeRow::Rings(coords, ring_offsets, start, end) => ShapeData::new(Shape::Polygon(
                GeometryArrayStorage::polygon_view(coords, ring_offsets, start..end),
            )),
        };
        Self::Transient(match seeded_bounds {
            BoundsSeed::Value(bounds) => data.with_seeded_bounds(bounds),
            BoundsSeed::Unset => data,
        })
    }

    /// Ownership boundary for durable boxed geometry; refine paths borrow this guard instead.
    pub(crate) fn into_owned_data(self) -> Arc<ShapeData> {
        match self {
            Self::Shared(data) => Arc::clone(data),
            Self::Transient(data) => Arc::new(data),
        }
    }
}

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

fn packed_row_persistent(row: ShapeRow<'_>) -> bool {
    use crate::geometry::uses_linear_plan_for_len;
    match row {
        ShapeRow::Line(_, start, end) => !uses_linear_plan_for_len((end - start).saturating_sub(1)),
        ShapeRow::Rings(_, offsets, start, end) => {
            let rings_non_linear = (start..end).any(|ring| {
                let open = (offsets[ring + 1] - offsets[ring]).saturating_sub(1) as usize;
                !uses_linear_plan_for_len(open)
            });
            rings_non_linear || !uses_linear_plan_for_len(end.saturating_sub(start + 1))
        },
        ShapeRow::Shape(_) => true,
        _ => false,
    }
}

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

    pub(crate) fn row_bounds_seed(&self, index: usize) -> BoundsSeed {
        self.cached_element_bounds()
            .map_or(BoundsSeed::Unset, |bounds| {
                BoundsSeed::Value(bounds.get(index).copied().flatten())
            })
    }

    pub(crate) fn prepared_row<'a>(&'a self, index: usize, row: ShapeRow<'a>) -> PreparedRow<'a> {
        match row {
            ShapeRow::Handle(handle) => PreparedRow::Shared(handle),
            ShapeRow::Shape(shape) => {
                // Seed bounds only when the array-level element-bounds cache
                // exists. Seeding `None` means "known empty" — not "unknown".
                // Mixed storage has no packed fold until first materialization,
                // so leave ShapeData's OnceLock unset and let it compute.
                let slots = self
                    .prepared_cache
                    .get_or_init(|| Arc::from(vec![OnceLock::new(); self.storage().len()]));
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
                PreparedRow::Shared(handle)
            },
            packed @ (ShapeRow::Line(..) | ShapeRow::Rings(..))
                if packed_row_persistent(packed) =>
            {
                let bounds = self.row_bounds_seed(index);
                let slots = self
                    .prepared_cache
                    .get_or_init(|| Arc::from(vec![OnceLock::new(); self.storage().len()]));
                let handle = slots[index].get_or_init(|| {
                    match PreparedRow::transient_with_seed(packed, bounds) {
                        PreparedRow::Transient(data) => Arc::new(data),
                        PreparedRow::Shared(data) => Arc::clone(data),
                    }
                });
                PreparedRow::Shared(handle)
            },
            packed => PreparedRow::transient_with_seed(packed, self.row_bounds_seed(index)),
        }
    }

    pub(crate) fn warm_prepared_row_or_transient<'a>(
        &'a self,
        index: usize,
        row: ShapeRow<'a>,
        bounds: Option<Bounds>,
    ) -> PreparedRow<'a> {
        if let ShapeRow::Handle(handle) = row {
            return PreparedRow::Shared(handle);
        }
        let Some(slots) = self.prepared_cache.get() else {
            return PreparedRow::transient_with_bounds(row, bounds);
        };
        slots[index].get().map_or_else(
            || PreparedRow::transient_with_bounds(row, bounds),
            PreparedRow::Shared,
        )
    }

    /// Scalar geometry for a logical row: mixed rows share one array-cached
    /// [`ShapeData`]; packed rows build a fresh handle (same as before).
    pub(crate) fn geometry_at(&self, index: usize) -> PyGeometry {
        let frame_cache = self.row_frame_cache(index);
        let frame = self.frame.clone();
        match self.storage() {
            GeometryArrayStorage::Mixed(shapes) => {
                let slots = self
                    .prepared_cache
                    .get_or_init(|| Arc::from(vec![OnceLock::new(); self.storage().len()]));
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

#[cfg(test)]
mod prepared_row_ownership_tests {
    use std::sync::Arc;

    use super::*;
    use crate::array::{CoordSeq, CsrOffsetColumn, PolygonLevel, RingLevel};
    use crate::boundary::Frame;
    use crate::geometry::{Point, Shape, uses_linear_plan_for_len};

    fn lines(vertices: usize) -> PyGeometryArray {
        let coords = CoordSeq::from_vecs(
            (0..vertices).map(|value| value as f64).collect(),
            vec![0.0; vertices],
            None,
            None,
        );
        PyGeometryArray::packed_lines(
            coords,
            CsrOffsetColumn::try_new(vec![0, vertices], vertices).unwrap(),
            Frame::None,
        )
    }

    #[test]
    fn packed_row_persistence_matches_the_execution_plan_boundary() {
        let linear = lines(3);
        assert!(uses_linear_plan_for_len(2));
        assert!(linear.prepared_cache.get().is_none());
        let _ = linear.prepared_row(0, linear.storage().row(0));
        assert!(linear.prepared_cache.get().is_none());

        let nonlinear = lines(34);
        assert!(!uses_linear_plan_for_len(33));
        let first = nonlinear.prepared_row(0, nonlinear.storage().row(0));
        let slots = nonlinear
            .prepared_cache
            .get()
            .expect("non-linear row persists");
        assert!(slots[0].get().is_some());
        let second = nonlinear.prepared_row(0, nonlinear.storage().row(0));
        assert!(matches!(first, PreparedRow::Shared(_)));
        assert!(matches!(second, PreparedRow::Shared(_)));

        // A short shell with many holes is non-linear because of hole count,
        // not because any individual ring crosses the length threshold.
        let mut xs = vec![0.0, 1.0, 1.0, 0.0, 0.0];
        let mut ys = vec![0.0, 0.0, 1.0, 1.0, 0.0];
        let mut ring_offsets = vec![0_usize, 5];
        for hole in 0..33 {
            let x = 2.0 + f64::from(hole);
            xs.extend([x, x + 0.5, x + 0.5, x, x]);
            ys.extend([0.0, 0.0, 0.5, 0.5, 0.0]);
            ring_offsets.push(ring_offsets.last().copied().unwrap() + 5);
        }
        let polygons = PyGeometryArray::packed_polygons(
            CoordSeq::from_vecs(xs, ys, None, None),
            CsrOffsetColumn::<RingLevel>::try_new(ring_offsets, 170).unwrap(),
            CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 34], 34).unwrap(),
            Frame::None,
        );
        assert!(uses_linear_plan_for_len(4));
        assert!(!uses_linear_plan_for_len(33));
        let _ = polygons.prepared_row(0, polygons.storage().row(0));
        assert!(polygons.prepared_cache.get().is_some());
    }

    #[test]
    fn transient_point_conversion_preserves_shape_and_bounds() {
        let array = PyGeometryArray::packed_points(
            CoordSeq::from_vecs(vec![2.0], vec![3.0], None, None),
            Frame::None,
        );
        let prepared = array
            .prepared_row(0, array.storage().row(0))
            .into_owned_data();
        assert!(matches!(prepared.shape(), Shape::Point(Point { x, y, .. })
            if (*x - 2.0).abs() < f64::EPSILON && (*y - 3.0).abs() < f64::EPSILON));
        assert_eq!(
            prepared.bounds(),
            Some(crate::geometry::Bounds::new_unchecked(2.0, 3.0, 2.0, 3.0))
        );
        assert!(array.prepared_cache.get().is_none());
    }

    #[test]
    fn unseeded_transient_computes_its_bounds() {
        let array = lines(3);
        let prepared = PreparedRow::transient(array.storage().row(0));
        assert_eq!(
            prepared.bounds(),
            Some(crate::geometry::Bounds::new_unchecked(0.0, 0.0, 2.0, 0.0))
        );
    }

    #[test]
    fn array_dwithin_unknown_bounds_stay_lazy() {
        let array = PyGeometryArray::mixed_shapes(
            vec![Shape::LineString(crate::geometry::LineSeq::from_trusted(
                CoordSeq::from_vecs(vec![0.0, 1.0], vec![0.0, 1.0], None, None),
            ))],
            Frame::None,
        );
        let seed = array.row_bounds_seed(0);
        assert!(matches!(seed, BoundsSeed::Unset));

        // The array dwithin lane passes this seed to its transient query
        // ShapeData. Unset must remain unset until exact refinement asks for it.
        let prepared = PreparedRow::transient_with_seed(array.storage().row(0), seed);
        assert_eq!(
            prepared.bounds(),
            Some(crate::geometry::Bounds::new_unchecked(0.0, 0.0, 1.0, 1.0))
        );
    }

    #[test]
    fn one_warm_packed_row_has_one_shared_owner() {
        let array = lines(34);
        let source_size = crate::HeapSize::heap_bytes(&array);
        let owned = array
            .prepared_row(0, array.storage().row(0))
            .into_owned_data();
        let warm_size = crate::HeapSize::heap_bytes(&array);
        assert!(warm_size > source_size);
        let slots = array.prepared_cache.get().unwrap();
        assert!(Arc::ptr_eq(&owned, slots[0].get().unwrap()));
        assert_eq!(array.prepared_cache_heap_bytes(), warm_size - source_size);
        assert_eq!(slots.iter().filter(|slot| slot.get().is_some()).count(), 1);
    }

    #[test]
    fn transient_non_disjoint_overlay_does_not_retain_packed_rows() {
        let left = lines(34);
        let right = lines(34);
        let cold_left = crate::HeapSize::heap_bytes(&left);
        let cold_right = crate::HeapSize::heap_bytes(&right);

        for _ in 0..5 {
            let left_row = left.storage().row(0);
            let right_row = right.storage().row(0);
            let left_bounds = left_row.quick_bounds();
            let right_bounds = right_row.quick_bounds();
            assert!(
                !left_row.with_shape(|left| { right_row.with_shape(|right| left.disjoint(right)) })
            );

            let left_data = left.warm_prepared_row_or_transient(0, left_row, left_bounds);
            let right_data = right.warm_prepared_row_or_transient(0, right_row, right_bounds);
            let _ = left_data
                .intersection(&right_data, crate::geometry::Strictness::Lenient)
                .unwrap();
        }

        assert_eq!(crate::HeapSize::heap_bytes(&left), cold_left);
        assert_eq!(crate::HeapSize::heap_bytes(&right), cold_right);
        assert!(left.prepared_cache.get().is_none());
        assert!(right.prepared_cache.get().is_none());
    }

    #[test]
    fn handle_conversion_is_borrowed_not_rebuilt() {
        let shape = ShapeData::new(Shape::Point(Point::new_unchecked_xy(1.0, 2.0)));
        let source = Arc::new(shape);
        let prepared = PreparedRow::Shared(&source).into_owned_data();
        assert!(Arc::ptr_eq(&source, &prepared));
    }
}
