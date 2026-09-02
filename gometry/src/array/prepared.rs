//! Lazy per-row prepared handles for packed array rows.

use std::collections::HashMap;
use std::ops::Deref;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::array::{GeometryArrayStorage, PyGeometry, PyGeometryArray, RowSelection, ShapeRow};
use crate::geometry::{Bounds, FrameDependentCaches, PreparedPlan, Shape, ShapeData};
use crate::heap_size::HeapSize;

const ROWS_PER_CHUNK: usize = 64;

#[cfg(test)]
static DIRECTORY_RESOLUTIONS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Default)]
struct RowProductCell {
    prepared: OnceLock<Arc<PreparedPlan>>,
    frame: OnceLock<Arc<FrameDependentCaches>>,
}

#[derive(Debug)]
struct RowProductChunk {
    cells: Box<[RowProductCell]>,
}

impl RowProductChunk {
    fn try_new() -> crate::error::Result<Self> {
        let mut cells = Vec::new();
        crate::boundary::try_reserve_vec(&mut cells, ROWS_PER_CHUNK)?;
        cells.resize_with(ROWS_PER_CHUNK, RowProductCell::default);
        Ok(Self {
            cells: cells.into_boxed_slice(),
        })
    }
}

/// Demand-allocated products owned by one immutable physical row source.
#[derive(Debug)]
pub(crate) struct RowProductOwner {
    physical_len: usize,
    chunks: Mutex<HashMap<usize, Arc<RowProductChunk>>>,
    aliases: HashMap<usize, (Arc<RowProductOwner>, usize)>,
}

impl RowProductOwner {
    fn new(physical_len: usize) -> Self {
        Self {
            physical_len,
            chunks: Mutex::new(HashMap::new()),
            aliases: HashMap::new(),
        }
    }

    fn cell(
        self: &Arc<Self>,
        physical: usize,
    ) -> crate::error::Result<(Arc<Self>, usize, Arc<RowProductChunk>)> {
        if let Some((owner, source_physical)) = self.aliases.get(&physical) {
            return owner.cell(*source_physical);
        }
        let chunk_index = physical / ROWS_PER_CHUNK;
        let offset = physical % ROWS_PER_CHUNK;
        #[cfg(test)]
        DIRECTORY_RESOLUTIONS.fetch_add(1, Ordering::Relaxed);
        let mut chunks = self
            .chunks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(chunk) = chunks.get(&chunk_index) {
            return Ok((Arc::clone(self), offset, Arc::clone(chunk)));
        }
        let chunk = Arc::new(RowProductChunk::try_new()?);
        chunks
            .try_reserve(1)
            .map_err(|_| crate::error::ResourceError::Allocation)?;
        chunks.insert(chunk_index, Arc::clone(&chunk));
        drop(chunks);
        Ok((Arc::clone(self), offset, chunk))
    }

    fn existing_cell(
        self: &Arc<Self>,
        physical: usize,
    ) -> Option<(Arc<Self>, usize, Arc<RowProductChunk>)> {
        if let Some((owner, source_physical)) = self.aliases.get(&physical) {
            return owner.existing_cell(*source_physical);
        }
        let chunk_index = physical / ROWS_PER_CHUNK;
        let offset = physical % ROWS_PER_CHUNK;
        #[cfg(test)]
        DIRECTORY_RESOLUTIONS.fetch_add(1, Ordering::Relaxed);
        let chunks = self
            .chunks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        chunks
            .get(&chunk_index)
            .map(|chunk| (Arc::clone(self), offset, Arc::clone(chunk)))
    }
}

/// Logical view onto a physical row-product owner.
#[derive(Clone, Debug)]
pub(crate) struct RowProducts {
    owner: Arc<RowProductOwner>,
    selection: RowSelection,
}

#[derive(Debug)]
struct ResolvedRowProductChunk {
    physical: usize,
    owner: Arc<RowProductOwner>,
    chunk_index: usize,
    chunk: Arc<RowProductChunk>,
}

/// Operation-scoped access to one logical row-product view.
///
/// The owner remains shared and immutable from the operation's perspective;
/// only the last resolved directory entry is retained here. The mutex is
/// therefore acquired when a new chunk is reached, never for each product
/// access within a chunk.
#[derive(Debug)]
pub(crate) struct RowProductsHandle {
    owner: Arc<RowProductOwner>,
    selection: RowSelection,
    current: Option<ResolvedRowProductChunk>,
}

impl RowProductsHandle {
    fn cell(
        &mut self,
        logical: usize,
    ) -> crate::error::Result<Option<(&Arc<RowProductChunk>, usize)>> {
        let physical = self.selection.as_deref().physical(logical);
        let has_aliases = !self.owner.aliases.is_empty();
        let cached = self.current.as_ref().is_some_and(|current| {
            if has_aliases {
                current.physical == physical
            } else {
                Arc::ptr_eq(&current.owner, &self.owner)
                    && current.chunk_index == physical / ROWS_PER_CHUNK
            }
        });
        if !cached {
            let (owner, offset, chunk) = self.owner.cell(physical)?;
            self.current = Some(ResolvedRowProductChunk {
                physical,
                owner,
                chunk_index: physical / ROWS_PER_CHUNK,
                chunk,
            });
            return Ok(self
                .current
                .as_ref()
                .map(|current| (&current.chunk, offset)));
        }
        Ok(self
            .current
            .as_ref()
            .map(|current| (&current.chunk, physical % ROWS_PER_CHUNK)))
    }

    fn existing_cell(
        &mut self,
        logical: usize,
    ) -> Option<(&Arc<RowProductChunk>, usize)> {
        let physical = self.selection.as_deref().physical(logical);
        let has_aliases = !self.owner.aliases.is_empty();
        let cached = self.current.as_ref().is_some_and(|current| {
            if has_aliases {
                current.physical == physical
            } else {
                Arc::ptr_eq(&current.owner, &self.owner)
                    && current.chunk_index == physical / ROWS_PER_CHUNK
            }
        });
        if !cached {
            let (owner, offset, chunk) = self.owner.existing_cell(physical)?;
            self.current = Some(ResolvedRowProductChunk {
                physical,
                owner,
                chunk_index: physical / ROWS_PER_CHUNK,
                chunk,
            });
            return self
                .current
                .as_ref()
                .map(|current| (&current.chunk, offset));
        }
        self.current
            .as_ref()
            .map(|current| (&current.chunk, physical % ROWS_PER_CHUNK))
    }

    pub(crate) fn frame_cache(&mut self, logical: usize) -> Arc<FrameDependentCaches> {
        let Some((chunk, offset)) = self.cell(logical).ok().flatten() else {
            return Arc::new(FrameDependentCaches::default());
        };
        Arc::clone(
            chunk.cells[offset]
                .frame
                .get_or_init(|| Arc::new(FrameDependentCaches::default())),
        )
    }

    pub(crate) fn prepared_plan(
        &mut self,
        logical: usize,
        shape: Arc<ShapeData>,
    ) -> Arc<PreparedPlan> {
        let Some((chunk, offset)) = self.cell(logical).ok().flatten() else {
            return Arc::new(PreparedPlan::new(shape));
        };
        Arc::clone(
            chunk.cells[offset]
                .prepared
                .get_or_init(|| Arc::new(PreparedPlan::new(shape))),
        )
    }

    pub(crate) fn cached_prepared_plan(&mut self, logical: usize) -> Option<Arc<PreparedPlan>> {
        let (chunk, offset) = self.existing_cell(logical)?;
        chunk.cells[offset].prepared.get().cloned()
    }

    pub(crate) fn install_prepared_plan(
        &mut self,
        logical: usize,
        plan: Arc<PreparedPlan>,
    ) -> Arc<PreparedPlan> {
        let Some((chunk, offset)) = self.cell(logical).ok().flatten() else {
            return plan;
        };
        Arc::clone(chunk.cells[offset].prepared.get_or_init(|| plan))
    }
}

impl RowProducts {
    pub(crate) fn fresh(physical_len: usize) -> Self {
        Self {
            owner: Arc::new(RowProductOwner::new(physical_len)),
            selection: RowSelection::Identity,
        }
    }

    pub(crate) fn operation(&self) -> RowProductsHandle {
        RowProductsHandle {
            owner: Arc::clone(&self.owner),
            selection: self.selection.clone(),
            current: None,
        }
    }

    pub(crate) fn frame_cache(&self, logical: usize) -> Arc<FrameDependentCaches> {
        self.operation().frame_cache(logical)
    }

    pub(crate) fn prepared_plan(&self, logical: usize, shape: Arc<ShapeData>) -> Arc<PreparedPlan> {
        self.operation().prepared_plan(logical, shape)
    }

    pub(crate) fn cached_prepared_plan(&self, logical: usize) -> Option<Arc<PreparedPlan>> {
        self.operation().cached_prepared_plan(logical)
    }

    /// Retained products for one scalar view into this row owner. The scalar
    /// must not charge products initialized for its array siblings.
    pub(crate) fn heap_bytes_at(&self, logical: usize) -> usize {
        let mut products = self.operation();
        let Some((chunk, offset)) = products.existing_cell(logical) else {
            return 0;
        };
        std::mem::size_of::<RowProductChunk>()
            + chunk.cells.len() * std::mem::size_of::<RowProductCell>()
            + chunk.cells[offset].prepared.get().map_or(0, |plan| {
                std::mem::size_of_val(plan.as_ref()) + plan.retained_heap_bytes()
            })
            + chunk.cells[offset].frame.get().map_or(0, |cache| {
                std::mem::size_of_val(cache.as_ref()) + cache.heap_bytes()
            })
    }

    pub(crate) fn install_prepared_plan(
        &self,
        logical: usize,
        plan: Arc<PreparedPlan>,
    ) -> Arc<PreparedPlan> {
        self.operation().install_prepared_plan(logical, plan)
    }

    #[cfg(test)]
    pub(crate) fn reset_directory_resolution_count() {
        DIRECTORY_RESOLUTIONS.store(0, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn directory_resolution_count() -> usize {
        DIRECTORY_RESOLUTIONS.load(Ordering::Relaxed)
    }

    fn selected(&self, rows: impl IntoIterator<Item = usize>) -> Self {
        let current = self.selection.as_deref();
        let physical_len = self.owner.physical_len;
        let selection = crate::array::row_selection_from_logical_rows(current, physical_len, rows);
        Self {
            owner: Arc::clone(&self.owner),
            selection,
        }
    }

    pub(super) fn with_selection(&self, selection: RowSelection) -> Self {
        Self {
            owner: Arc::clone(&self.owner),
            selection,
        }
    }

    pub(super) fn preserving_physical_rows(
        physical_len: usize,
        source: &Self,
        unchanged_rows: &[usize],
    ) -> crate::error::Result<Self> {
        let current = source.selection.as_deref();
        let mut aliases = HashMap::new();
        aliases
            .try_reserve(unchanged_rows.len())
            .map_err(|_| crate::error::ResourceError::Allocation)?;
        for &row in unchanged_rows {
            aliases.insert(row, (Arc::clone(&source.owner), current.physical(row)));
        }
        Ok(Self {
            owner: Arc::new(RowProductOwner {
                physical_len,
                chunks: Mutex::new(HashMap::new()),
                aliases,
            }),
            selection: RowSelection::Identity,
        })
    }

    #[cfg(test)]
    pub(crate) fn shares_owner_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.owner, &other.owner) && self.selection == other.selection
    }

    #[cfg(test)]
    pub(crate) fn initialized_counts(&self) -> (usize, usize, usize) {
        let mut chunks = std::collections::HashSet::new();
        let mut prepared = std::collections::HashSet::new();
        let mut frame = std::collections::HashSet::new();
        for logical in 0..self.selection.as_deref().len(self.owner.physical_len) {
            let physical = self.selection.as_deref().physical(logical);
            if let Some((_, offset, chunk)) = self.owner.existing_cell(physical) {
                let chunk_id = Arc::as_ptr(&chunk) as usize;
                chunks.insert(chunk_id);
                if chunk.cells[offset].prepared.get().is_some() {
                    prepared.insert((chunk_id, offset));
                }
                if chunk.cells[offset].frame.get().is_some() {
                    frame.insert((chunk_id, offset));
                }
            }
        }
        (chunks.len(), prepared.len(), frame.len())
    }
}

pub(crate) enum PreparedRow<'a> {
    Shared(&'a Arc<PreparedPlan>),
    Owned(Arc<PreparedPlan>),
    Transient(PreparedPlan),
}

impl Deref for PreparedRow<'_> {
    type Target = PreparedPlan;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Shared(data) => data,
            Self::Owned(data) => data,
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
            ShapeRow::Handle(handle) => {
                return Self::Shared(handle);
            },
            ShapeRow::Shape(shape) => PreparedPlan::new(ShapeData::new(shape.clone())),
            ShapeRow::Point(point) => PreparedPlan::new(ShapeData::new(Shape::Point(point))),
            ShapeRow::Line(coords, start, end) => {
                PreparedPlan::new(ShapeData::new(Shape::LineString(
                    crate::geometry::LineSeq::try_new(coords.view(
                        crate::geometry::CoordWindow::from_range(start..end, coords.len()),
                    ))
                    .expect("validated line window has at least two vertices"),
                )))
            },
            ShapeRow::Rings(coords, ring_offsets, start, end) => {
                PreparedPlan::new(ShapeData::new(Shape::Polygon(
                    GeometryArrayStorage::polygon_view(coords, ring_offsets, start..end),
                )))
            },
        };
        Self::Transient(match seeded_bounds {
            BoundsSeed::Value(bounds) => data.with_seeded_bounds(bounds),
            BoundsSeed::Unset => data,
        })
    }

    /// Ownership boundary for durable boxed geometry; refine paths borrow this guard instead.
    pub(crate) fn into_owned_plan(self) -> Arc<PreparedPlan> {
        match self {
            Self::Shared(data) => Arc::clone(data),
            Self::Owned(data) => data,
            Self::Transient(data) => Arc::new(data),
        }
    }
}

impl HeapSize for RowProducts {
    fn heap_bytes(&self) -> usize {
        let chunks = self
            .owner
            .chunks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let retained = chunks.values().fold(0, |total, chunk| {
            total
                + std::mem::size_of::<RowProductChunk>()
                + chunk.cells.len() * std::mem::size_of::<RowProductCell>()
                + chunk.cells.iter().fold(0, |cell_total, cell| {
                    cell_total
                        + cell.prepared.get().map_or(0, |plan| {
                            std::mem::size_of_val(plan.as_ref()) + plan.retained_heap_bytes()
                        })
                        + cell.frame.get().map_or(0, |cache| {
                            std::mem::size_of_val(cache.as_ref()) + cache.heap_bytes()
                        })
                })
        });
        retained
            + chunks.capacity() * std::mem::size_of::<(usize, Arc<RowProductChunk>)>()
            + self.selection.heap_bytes()
    }
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
    pub(crate) fn selected_row_products(
        &self,
        rows: impl IntoIterator<Item = usize>,
    ) -> RowProducts {
        self.row_products.selected(rows)
    }

    pub(crate) fn row_bounds_seed(&self, index: usize) -> BoundsSeed {
        self.cached_element_bounds()
            .map_or(BoundsSeed::Unset, |bounds| {
                BoundsSeed::Value(bounds.get(index).copied().flatten())
            })
    }

    pub(crate) fn prepared_row<'a>(&'a self, index: usize, row: ShapeRow<'a>) -> PreparedRow<'a> {
        let mut products = self.row_products.operation();
        self.prepared_row_with(&mut products, index, row)
    }

    pub(crate) fn prepared_row_with<'a>(
        &self,
        products: &mut RowProductsHandle,
        index: usize,
        row: ShapeRow<'a>,
    ) -> PreparedRow<'a> {
        match row {
            ShapeRow::Handle(handle) => PreparedRow::Shared(handle),
            ShapeRow::Shape(shape) => {
                // Seed bounds only when the array-level element-bounds cache
                // exists. Seeding `None` means "known empty" — not "unknown".
                // Mixed storage has no packed fold until first materialization,
                // so leave ShapeData's OnceLock unset and let it compute.
                let Some((chunk, offset)) = products.cell(index).ok().flatten() else {
                    return PreparedRow::transient_with_seed(
                        ShapeRow::Shape(shape),
                        self.row_bounds_seed(index),
                    );
                };
                let handle = chunk.cells[offset].prepared.get_or_init(|| {
                    let data = PreparedPlan::new(ShapeData::new(shape.clone()));
                    if let Some(element_bounds) = self.cached_element_bounds() {
                        Arc::new(
                            data.with_seeded_bounds(element_bounds.get(index).copied().flatten()),
                        )
                    } else {
                        Arc::new(data)
                    }
                });
                PreparedRow::Owned(Arc::clone(handle))
            },
            packed @ (ShapeRow::Line(..) | ShapeRow::Rings(..))
                if packed_row_persistent(packed) =>
            {
                let bounds = self.row_bounds_seed(index);
                let Some((chunk, offset)) = products.cell(index).ok().flatten() else {
                    return PreparedRow::transient_with_seed(packed, bounds);
                };
                let handle = chunk.cells[offset].prepared.get_or_init(|| {
                    match PreparedRow::transient_with_seed(packed, bounds) {
                        PreparedRow::Transient(data) => Arc::new(data),
                        PreparedRow::Shared(data) => Arc::clone(data),
                        PreparedRow::Owned(data) => data,
                    }
                });
                PreparedRow::Owned(Arc::clone(handle))
            },
            packed => PreparedRow::transient_with_seed(packed, self.row_bounds_seed(index)),
        }
    }

    #[cfg(test)]
    pub(crate) fn warm_prepared_row_or_transient<'a>(
        &'a self,
        index: usize,
        row: ShapeRow<'a>,
        bounds: Option<Bounds>,
    ) -> PreparedRow<'a> {
        let mut products = self.row_products.operation();
        self.warm_prepared_row_or_transient_with(&mut products, index, row, bounds)
    }

    pub(crate) fn warm_prepared_row_or_transient_with<'a>(
        &self,
        products: &mut RowProductsHandle,
        index: usize,
        row: ShapeRow<'a>,
        bounds: Option<Bounds>,
    ) -> PreparedRow<'a> {
        if let ShapeRow::Handle(handle) = row {
            return PreparedRow::Shared(handle);
        }
        let Some((chunk, offset)) = products.existing_cell(index) else {
            return PreparedRow::transient_with_bounds(row, bounds);
        };
        chunk.cells[offset].prepared.get().map_or_else(
            || PreparedRow::transient_with_bounds(row, bounds),
            |plan| PreparedRow::Owned(Arc::clone(plan)),
        )
    }

    /// Scalar geometry for a logical row: mixed rows share one array-cached
    /// [`PreparedPlan`]; packed rows build a fresh handle (same as before).
    pub(crate) fn geometry_at(&self, index: usize) -> PyGeometry {
        let frame = self.frame.clone();
        match self.storage() {
            GeometryArrayStorage::Mixed(shapes) => {
                let plan = self
                    .row_products
                    .prepared_plan(index, Arc::new(ShapeData::new(shapes[index].clone())));
                PyGeometry::with_row_products(
                    Arc::clone(plan.shape_arc()),
                    frame,
                    self.row_products.clone(),
                    index,
                )
            },
            _ if self.rows.is_compact_nullable() => {
                let crate::array::LogicalRow::Present { row, .. } = self
                    .logical_row(index)
                    .expect("geometry row must be in bounds")
                else {
                    unreachable!("missing rows are rejected before scalar extraction")
                };
                PyGeometry::with_row_products(
                    Arc::new(ShapeData::new(row.with_shape(Clone::clone))),
                    frame,
                    self.row_products.clone(),
                    index,
                )
            },
            _ => PyGeometry::with_row_products(
                self.storage().shape_data_at(index),
                frame,
                self.row_products.clone(),
                index,
            ),
        }
    }
}

#[cfg(test)]
mod frame_cache_row_tests {
    use super::*;
    use crate::boundary::{Frame, crs_arc_static};
    use crate::geometry::{CoordSeq, LineSeq, PlanarMetric, Point};

    fn points() -> PyGeometryArray {
        PyGeometryArray::packed_points(
            CoordSeq::from_vecs(vec![0.0, 1.0, 2.0], vec![0.0, 1.0, 2.0], None, None),
            Frame::None,
        )
    }

    #[test]
    fn huge_owners_are_cold_and_chunk_boundaries_are_physical() {
        for len in [1_000_000, 100_000_000] {
            let products = RowProducts::fresh(len);
            assert!(products.owner.chunks.lock().unwrap().is_empty());
            assert_eq!(products.heap_bytes(), 0);
        }

        let products = RowProducts::fresh(100_000_000);
        let mut handle = products.operation();
        let row_63 = handle
            .cell(63)
            .unwrap()
            .map(|(chunk, offset)| (Arc::as_ptr(chunk), offset));
        assert_eq!(products.owner.chunks.lock().unwrap().len(), 1);
        let row_64 = handle
            .cell(64)
            .unwrap()
            .map(|(chunk, offset)| (Arc::as_ptr(chunk), offset));
        assert_eq!(products.owner.chunks.lock().unwrap().len(), 2);
        assert_ne!(row_63.unwrap().0, row_64.unwrap().0);
    }

    #[test]
    fn duplicate_logical_rows_share_one_physical_cell() {
        let products = RowProducts::fresh(3).selected([2, 2]);
        let mut handle = products.operation();
        let first = handle
            .cell(0)
            .unwrap()
            .map(|(chunk, offset)| (Arc::as_ptr(chunk), offset));
        let second = handle
            .cell(1)
            .unwrap()
            .map(|(chunk, offset)| (Arc::as_ptr(chunk), offset));
        assert_eq!(first.unwrap(), second.unwrap());
    }

    #[test]
    fn operation_resolves_each_sequential_chunk_once() {
        let products = RowProducts::fresh(192);
        let mut handle = products.operation();
        RowProducts::reset_directory_resolution_count();
        for row in 0..192 {
            let _ = handle.frame_cache(row);
            let _ = handle.frame_cache(row);
        }
        assert_eq!(RowProducts::directory_resolution_count(), 3);
    }

    #[test]
    fn prepared_and_frame_access_share_one_resolved_slot() {
        let products = RowProducts::fresh(1);
        let mut handle = products.operation();
        RowProducts::reset_directory_resolution_count();
        let _ = handle.prepared_plan(
            0,
            Arc::new(ShapeData::new(Shape::Point(Point::new_raw_xy(1.0, 2.0)))),
        );
        let _ = handle.frame_cache(0);
        assert_eq!(RowProducts::directory_resolution_count(), 1);
    }

    #[test]
    fn existing_only_cold_lookup_does_not_allocate() {
        let products = RowProducts::fresh(1_000_000);
        let mut handle = products.operation();
        RowProducts::reset_directory_resolution_count();
        assert!(handle.cached_prepared_plan(0).is_none());
        assert!(products.owner.chunks.lock().unwrap().is_empty());
        assert_eq!(RowProducts::directory_resolution_count(), 1);
    }

    #[test]
    fn concurrent_first_publication_keeps_one_prepared_arc() {
        let products = Arc::new(RowProducts::fresh(1));
        let mut threads: Vec<_> = (0..8)
            .map(|_| {
                let products = Arc::clone(&products);
                std::thread::spawn(move || {
                    products.operation().prepared_plan(
                        0,
                        Arc::new(ShapeData::new(Shape::Point(Point::new_raw_xy(1.0, 2.0)))),
                    )
                })
            })
            .collect();
        let first = threads
            .remove(0)
            .join()
            .expect("publication thread must finish");
        for thread in threads {
            assert!(Arc::ptr_eq(
                &first,
                &thread.join().expect("publication thread must finish")
            ));
        }
    }

    fn mixed_shapes() -> PyGeometryArray {
        PyGeometryArray::mixed_shapes(
            vec![
                Shape::Point(Point::new_raw_xy(1.0, 2.0)),
                Shape::LineString(
                    LineSeq::try_new(CoordSeq::from_vecs(
                        vec![0.0, 1.0],
                        vec![0.0, 1.0],
                        None,
                        None,
                    ))
                    .expect("fixture line has two vertices"),
                ),
                Shape::Point(Point::new_raw_xy(3.0, 4.0)),
            ],
            Frame::None,
        )
    }

    #[test]
    fn mixed_geometry_at_shares_prepared_plan_shape_data_arc() {
        let array = mixed_shapes();
        let a = array.geometry_at(1);
        let b = array.geometry_at(1);
        assert!(
            Arc::ptr_eq(&a.shape, &b.shape),
            "arr[i] must reuse the array-side prepared plan's ShapeData Arc"
        );
        assert!(Arc::ptr_eq(&a.frame_cache(), &b.frame_cache()));
        // Different rows stay independent.
        let c = array.geometry_at(0);
        assert!(!Arc::ptr_eq(&a.shape, &c.shape));
    }

    #[test]
    fn clones_slices_and_scalar_extraction_share_row_slots() {
        let array = points();
        let clone = array.clone();
        assert!(Arc::ptr_eq(
            &array.row_products.frame_cache(1),
            &clone.row_products.frame_cache(1)
        ));

        let slice = array.gather_logical_rows(&[2, 0]);
        assert!(Arc::ptr_eq(
            &array.row_products.frame_cache(2),
            &slice.row_products.frame_cache(0)
        ));
        let extracted = slice.geometry_at(0);
        assert!(Arc::ptr_eq(
            &extracted.frame_cache(),
            &array.row_products.frame_cache(2)
        ));
    }

    #[test]
    fn array_retag_reuses_storage_and_row_products() {
        let array = points();
        let retagged = array.retag_frame(
            Frame::new(Some(crs_arc_static("EPSG:4326")), None).expect("valid test frame"),
        );
        assert!(Arc::ptr_eq(array.storage_arc(), retagged.storage_arc()));
        for row in 0..array.len() {
            assert!(Arc::ptr_eq(
                &array.row_products.frame_cache(row),
                &retagged.row_products.frame_cache(row)
            ));
        }
    }

    #[test]
    fn retagged_gathered_scalars_keep_their_physical_product_cells() {
        let source = points().gather_logical_rows(&[2, 2, 1]);
        let before = source.row_products.initialized_counts();
        let retagged = source.retag_frame(Frame::Crs(crs_arc_static("EPSG:4326")));

        assert!(
            source
                .row_products
                .shares_owner_with(&retagged.row_products)
        );
        assert_eq!(before, retagged.row_products.initialized_counts());
        let first = retagged.geometry_at(0);
        let duplicate = retagged.geometry_at(1);
        let source_row = source.geometry_at(0);
        assert_eq!(first.product_row, 0);
        assert_eq!(duplicate.product_row, 1);
        assert!(Arc::ptr_eq(&first.frame_cache(), &duplicate.frame_cache()));
        assert!(Arc::ptr_eq(&first.frame_cache(), &source_row.frame_cache()));
        assert_eq!(source.row_products.initialized_counts(), (1, 0, 1));
    }

    #[test]
    fn retags_share_the_inner_keyed_frame_cache_and_prepared_plan() {
        let a = mixed_shapes().retag_frame(Frame::Crs(crs_arc_static("EPSG:4326")));
        let prepared_a = a.prepared_row(1, a.storage().row(1)).into_owned_plan();
        let cache_a = a.row_products.frame_cache(1);
        let index_a = prepared_a
            .geodesic_line_index(
                &cache_a,
                "EPSG:4326",
                6_378_137.0,
                1.0 / 298.257_223_563,
                4,
                &PlanarMetric,
            )
            .expect("first keyed line index");

        let b = a.retag_frame(Frame::Crs(crs_arc_static("EPSG:4269")));
        let prepared_b = b.prepared_row(1, b.storage().row(1)).into_owned_plan();
        let cache_b = b.row_products.frame_cache(1);
        assert!(Arc::ptr_eq(&prepared_a, &prepared_b));
        assert!(Arc::ptr_eq(&cache_a, &cache_b));
        let index_b = prepared_b
            .geodesic_line_index(
                &cache_b,
                "EPSG:4269",
                6_378_137.0,
                1.0 / 298.257_222_101,
                4,
                &PlanarMetric,
            )
            .expect("retagged keyed line index");
        assert!(!Arc::ptr_eq(&index_a, &index_b));

        let back_to_a = b.retag_frame(Frame::Crs(crs_arc_static("EPSG:4326")));
        let cache_a2 = back_to_a.row_products.frame_cache(1);
        assert!(Arc::ptr_eq(&cache_a, &cache_a2));
        let index_a2 = prepared_a
            .geodesic_line_index(
                &cache_a2,
                "EPSG:4326",
                6_378_137.0,
                1.0 / 298.257_223_563,
                4,
                &PlanarMetric,
            )
            .expect("restored keyed line index");
        assert!(!Arc::ptr_eq(&index_a, &index_a2));
    }

    #[test]
    fn cold_owner_allocates_chunks_only_for_touched_rows() {
        let array = points();
        assert!(array.row_products.owner.chunks.lock().unwrap().is_empty());
        let _ = array.row_products.frame_cache(0);
        assert_eq!(array.row_products.owner.chunks.lock().unwrap().len(), 1);
        let gathered = array.gather_logical_rows(&[2, 0]);
        assert!(Arc::ptr_eq(
            &array.row_products.owner,
            &gathered.row_products.owner
        ));
    }

    #[test]
    fn planar_metric_over_million_logical_rows_initializes_no_products() {
        let source = points();
        let rows: Vec<usize> = (0..1_000_000).map(|row| row % 3).collect();
        let array = source.gather_logical_rows(&rows);
        let probe = PreparedPlan::new(Shape::Point(Point::new_raw_xy(4.0, 4.0)));

        for row in array.storage().iter_rows() {
            let data = PreparedRow::transient(row);
            crate::broadcast::pair_distance_planar(1.0, &data, &probe).unwrap();
        }

        assert_eq!(array.row_products.initialized_counts(), (0, 0, 0));
    }

    #[test]
    fn duplicate_selections_share_only_admitted_frame_cells() {
        let array = points().gather_logical_rows(&[1, 1, 2, 0, 1]);
        let first = array.row_products.frame_cache(0);
        let duplicate = array.row_products.frame_cache(1);
        let refined = array.row_products.frame_cache(2);

        assert!(Arc::ptr_eq(&first, &duplicate));
        assert!(!Arc::ptr_eq(&first, &refined));
        assert_eq!(array.row_products.initialized_counts(), (1, 0, 2));
    }

    #[test]
    fn geodesic_metric_initializes_only_refined_duplicate_row() {
        let array = points().gather_logical_rows(&[1, 1, 2]);
        let probe = PreparedPlan::new(Shape::Point(Point::new_raw_xy(3.0, 3.0)));
        let probe_cache = FrameDependentCaches::default();
        let model = crate::crs::metric_model(Some("EPSG:4326")).unwrap();
        let resolved = crate::crs::ResolvedMetric::from_model(&model).unwrap();
        let crate::crs::ResolvedMetric::Geodesic {
            crs,
            geodesic,
            frame,
            state,
        } = &resolved
        else {
            panic!("EPSG:4326 resolves geodesically")
        };

        for row in [0, 1] {
            let data = PreparedRow::transient(array.storage().row(row));
            crate::broadcast::pair_distance_geodesic(
                crs,
                geodesic,
                *frame,
                state,
                &data,
                &array.row_products.frame_cache(row),
                &probe,
                &probe_cache,
            )
            .unwrap();
        }

        assert_eq!(array.row_products.initialized_counts(), (1, 0, 1));
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
        assert!(linear.row_products.owner.chunks.lock().unwrap().is_empty());
        let _ = linear.prepared_row(0, linear.storage().row(0));
        assert!(linear.row_products.owner.chunks.lock().unwrap().is_empty());

        let nonlinear = lines(34);
        assert!(!uses_linear_plan_for_len(33));
        let first = nonlinear.prepared_row(0, nonlinear.storage().row(0));
        assert_eq!(nonlinear.row_products.owner.chunks.lock().unwrap().len(), 1);
        let second = nonlinear.prepared_row(0, nonlinear.storage().row(0));
        assert!(matches!(first, PreparedRow::Owned(_)));
        assert!(matches!(second, PreparedRow::Owned(_)));

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
        assert_eq!(polygons.row_products.owner.chunks.lock().unwrap().len(), 1);
    }

    #[test]
    fn packed_geometry_at_reuses_an_initialized_prepared_plan() {
        let array = lines(34);
        let plan = array
            .prepared_row(0, array.storage().row(0))
            .into_owned_plan();
        let geometry = array.geometry_at(0);
        let extracted = geometry.prepared_plan();
        assert!(Arc::ptr_eq(&plan, &extracted));
        assert!(Arc::ptr_eq(
            &geometry.frame_cache(),
            &array.row_products.frame_cache(0)
        ));
    }

    #[test]
    fn transient_point_conversion_preserves_shape_and_bounds() {
        let array = PyGeometryArray::packed_points(
            CoordSeq::from_vecs(vec![2.0], vec![3.0], None, None),
            Frame::None,
        );
        let prepared = array
            .prepared_row(0, array.storage().row(0))
            .into_owned_plan();
        assert!(matches!(prepared.shape(), Shape::Point(Point { x, y, .. })
            if (x - 2.0).abs() < f64::EPSILON && (y - 3.0).abs() < f64::EPSILON));
        assert_eq!(
            prepared.bounds(),
            Some(crate::geometry::Bounds::new_raw(2.0, 3.0, 2.0, 3.0))
        );
        assert!(array.row_products.owner.chunks.lock().unwrap().is_empty());
    }

    #[test]
    fn unseeded_transient_computes_its_bounds() {
        let array = lines(3);
        let prepared = PreparedRow::transient(array.storage().row(0));
        assert_eq!(
            prepared.bounds(),
            Some(crate::geometry::Bounds::new_raw(0.0, 0.0, 2.0, 0.0))
        );
    }

    #[test]
    fn array_dwithin_unknown_bounds_stay_lazy() {
        let array = PyGeometryArray::mixed_shapes(
            vec![Shape::LineString(
                crate::geometry::LineSeq::try_new(CoordSeq::from_vecs(
                    vec![0.0, 1.0],
                    vec![0.0, 1.0],
                    None,
                    None,
                ))
                .expect("fixture line has two vertices"),
            )],
            Frame::None,
        );
        let seed = array.row_bounds_seed(0);
        assert!(matches!(seed, BoundsSeed::Unset));

        // The array dwithin lane passes this seed to its transient query
        // ShapeData. Unset must remain unset until exact refinement asks for it.
        let prepared = PreparedRow::transient_with_seed(array.storage().row(0), seed);
        assert_eq!(
            prepared.bounds(),
            Some(crate::geometry::Bounds::new_raw(0.0, 0.0, 1.0, 1.0))
        );
    }

    #[test]
    fn one_warm_packed_row_has_one_shared_owner() {
        let array = lines(34);
        let source_size = crate::HeapSize::heap_bytes(&array);
        let owned = array
            .prepared_row(0, array.storage().row(0))
            .into_owned_plan();
        let warm_size = crate::HeapSize::heap_bytes(&array);
        assert!(warm_size > source_size);
        let chunks = array.row_products.owner.chunks.lock().unwrap();
        assert!(Arc::ptr_eq(
            &owned,
            chunks.get(&0).unwrap().cells[0].prepared.get().unwrap()
        ));
        assert_eq!(chunks.len(), 1);
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
        assert!(left.row_products.owner.chunks.lock().unwrap().is_empty());
        assert!(right.row_products.owner.chunks.lock().unwrap().is_empty());
    }

    #[test]
    fn handle_conversion_is_borrowed_not_rebuilt() {
        let shape = PreparedPlan::new(Shape::Point(Point::new_raw_xy(1.0, 2.0)));
        let source = Arc::new(shape);
        let prepared = PreparedRow::Shared(&source).into_owned_plan();
        assert!(Arc::ptr_eq(&source, &prepared));
    }
}
