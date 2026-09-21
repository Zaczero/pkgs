//! Lazy per-row prepared handles for packed array rows.

#[cfg(test)]
use std::cell::Cell;
use std::ops::Deref;
use std::sync::{Arc, Mutex, OnceLock};

use crate::array::{
    GeometryArrayStorage, LogicalRow, PyGeometry, PyGeometryArray, RowSelection, ShapeRow,
};
use crate::geometry::{Bounds, FrameDependentCaches, PreparedPlan, Shape, ShapeData};
use crate::heap_size::HeapSize;

const ROWS_PER_CHUNK: usize = 64;

#[cfg(test)]
std::thread_local! {
    static DIRECTORY_RESOLUTIONS: Cell<usize> = const { Cell::new(0) };
}

#[derive(Debug, Default)]
struct RowProductCell {
    prepared: OnceLock<Arc<PreparedPlan>>,
    frame: OnceLock<Arc<FrameDependentCaches>>,
}

#[derive(Debug)]
struct RowProductChunk {
    cells: Vec<RowProductCell>,
}

impl RowProductChunk {
    fn try_new() -> crate::error::Result<Self> {
        let mut cells = Vec::new();
        crate::boundary::try_reserve_vec(&mut cells, ROWS_PER_CHUNK)?;
        cells.resize_with(ROWS_PER_CHUNK, RowProductCell::default);
        Ok(Self { cells })
    }

    fn heap_bytes_for_cell(&self, offset: usize) -> usize {
        crate::heap_size::ARC_CONTROL_BYTES
            + std::mem::size_of::<Self>()
            + self.cells.capacity() * std::mem::size_of::<RowProductCell>()
            + self.cells[offset].heap_bytes()
    }

    fn heap_bytes_for_selection(
        &self,
        selection: crate::array::RowSelectionRef<'_>,
        chunk_index: usize,
    ) -> usize {
        let mut retained = crate::heap_size::ARC_CONTROL_BYTES
            + std::mem::size_of::<Self>()
            + self.cells.capacity() * std::mem::size_of::<RowProductCell>();
        let chunk_start = chunk_index.saturating_mul(ROWS_PER_CHUNK);
        match selection {
            crate::array::RowSelectionRef::Window { start, len } => {
                let end = start.saturating_add(len);
                let first = start.saturating_sub(chunk_start);
                let last = end.saturating_sub(chunk_start).min(self.cells.len());
                if first < last {
                    retained += self.cells[first..last]
                        .iter()
                        .map(HeapSize::heap_bytes)
                        .sum::<usize>();
                }
            },
            crate::array::RowSelectionRef::Gather(rows) => {
                let mut selected = 0_u64;
                for &row in rows {
                    if row / ROWS_PER_CHUNK == chunk_index {
                        selected |= 1_u64 << (row - chunk_start);
                    }
                }
                for (offset, cell) in self.cells.iter().enumerate() {
                    if selected & (1_u64 << offset) != 0 {
                        retained += cell.heap_bytes();
                    }
                }
            },
            crate::array::RowSelectionRef::Identity => unreachable!(),
        }
        retained
    }
}

impl HeapSize for RowProductCell {
    fn heap_bytes(&self) -> usize {
        self.prepared.heap_bytes() + self.frame.heap_bytes()
    }
}

impl HeapSize for RowProductChunk {
    fn heap_bytes(&self) -> usize {
        self.cells.heap_bytes()
    }
}

/// Demand-allocated products owned by one immutable physical row source.
#[derive(Debug)]
pub(crate) struct RowProductOwner {
    physical_len: usize,
    chunks: Mutex<crate::collections::IndexMap<usize, Arc<RowProductChunk>>>,
}

impl RowProductOwner {
    fn new(physical_len: usize) -> Self {
        Self {
            physical_len,
            chunks: Mutex::new(crate::collections::IndexMap::default()),
        }
    }

    fn cell(&self, physical: usize) -> crate::error::Result<Arc<RowProductChunk>> {
        let chunk_index = physical / ROWS_PER_CHUNK;
        #[cfg(test)]
        DIRECTORY_RESOLUTIONS.with(|count| count.set(count.get() + 1));
        let mut chunks = self
            .chunks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(chunk) = chunks.get(&chunk_index) {
            return Ok(Arc::clone(chunk));
        }
        let chunk = Arc::new(RowProductChunk::try_new()?);
        chunks.try_reserve(1).map_err(crate::error::Error::from)?;
        chunks.insert(chunk_index, Arc::clone(&chunk));
        drop(chunks);
        Ok(chunk)
    }

    fn existing_cell(&self, physical: usize) -> Option<Arc<RowProductChunk>> {
        let chunk_index = physical / ROWS_PER_CHUNK;
        #[cfg(test)]
        DIRECTORY_RESOLUTIONS.with(|count| count.set(count.get() + 1));
        let chunks = self
            .chunks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        chunks.get(&chunk_index).map(Arc::clone)
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

/// One sequential operation over an array's logical rows.
///
/// The operation owns the directory cursor and is borrowed for the same
/// lifetime as the array rows. This keeps logical identity, missing emission,
/// prepared lookup, and frame-cache lookup on one path without requiring a
/// self-referential iterator or boxing row values.
pub(crate) struct ArrayRowOperation<'a> {
    array: &'a PyGeometryArray,
    products: RowProductsHandle,
    compact_rank: Option<crate::array::ValidityRank<'a>>,
}

#[derive(Clone, Copy)]
pub(crate) struct PresentRow<'a> {
    source: &'a PyGeometryArray,
    logical: usize,
    row: ShapeRow<'a>,
}

#[derive(Clone, Copy)]
pub(crate) enum ArrayRow<'a, T> {
    Missing { logical: usize, value: T },
    Present(PresentRow<'a>),
}

impl<'a> PresentRow<'a> {
    const fn new(source: &'a PyGeometryArray, logical: usize, row: ShapeRow<'a>) -> Self {
        Self {
            source,
            logical,
            row,
        }
    }

    pub(crate) const fn logical(self) -> usize {
        self.logical
    }

    pub(crate) const fn row(self) -> ShapeRow<'a> {
        self.row
    }
}

impl<'a> ArrayRowOperation<'a> {
    pub(crate) fn new(array: &'a PyGeometryArray) -> Self {
        Self {
            array,
            products: array.row_products.operation(),
            compact_rank: None,
        }
    }

    /// Return logical rows while making the missing output explicit at the
    /// call site. The factory avoids imposing `Clone` on output values.
    pub(crate) fn rows_with_missing<T>(
        &self,
        missing: impl Fn() -> T + Copy + 'a,
    ) -> impl ExactSizeIterator<Item = ArrayRow<'a, T>> + 'a {
        let array = self.array;
        array.logical_rows().map(move |row| match row {
            LogicalRow::Missing { logical } => ArrayRow::Missing {
                logical,
                value: missing(),
            },
            LogicalRow::Present { logical, row } => {
                ArrayRow::Present(PresentRow::new(array, logical, row))
            },
        })
    }

    /// Present-only traversal for operations that intentionally skip missing
    /// rows, such as index joins and sparse geometry collection builders.
    pub(crate) fn present_rows(&self) -> impl Iterator<Item = PresentRow<'a>> + 'a {
        let array = self.array;
        array.logical_rows().filter_map(move |row| match row {
            LogicalRow::Missing { .. } => None,
            LogicalRow::Present { logical, row } => Some(PresentRow::new(array, logical, row)),
        })
    }

    /// Publish a scalar geometry from an already-resolved row. The source
    /// owner and logical identity travel with the row, so compact nullable
    /// points are never ranked a second time by a bulk caller.
    pub(crate) fn geometry(&mut self, row: PresentRow<'a>) -> crate::error::Result<PyGeometry> {
        row.source
            .geometry_from_present_row(&mut self.products, row)
    }

    /// Resolve one logical row into the owner-bound present-row value used by
    /// indexed and other arbitrary-order operations. Compact points build one
    /// directory for this operation; sequential callers should keep using
    /// [`Self::rows_with_missing`] so they can use the cheaper validity cursor.
    pub(crate) fn present_row_at(
        &mut self,
        logical: usize,
    ) -> crate::error::Result<Option<PresentRow<'a>>> {
        if logical >= self.array.len() {
            return Ok(None);
        }
        if self.array.is_compact_nullable()
            && let Some(mask) = self.array.missing()
            && matches!(
                self.array.storage(),
                GeometryArrayStorage::Packed(crate::array::PackedStorage::Points { .. })
            )
        {
            if self.compact_rank.is_none() {
                self.compact_rank = Some(mask.full_rank()?);
            }
            let Some(present) = self
                .compact_rank
                .as_ref()
                .expect("compact rank was initialized")
                .resolve(logical)
            else {
                return Ok(None);
            };
            return Ok(Some(PresentRow::new(
                self.array,
                logical,
                self.array.storage().row(present),
            )));
        }
        Ok(match self.array.logical_row(logical) {
            Some(LogicalRow::Missing { .. }) | None => None,
            Some(LogicalRow::Present { row, .. }) => {
                Some(PresentRow::new(self.array, logical, row))
            },
        })
    }

    pub(crate) fn prepared(
        &mut self,
        row: PresentRow<'a>,
    ) -> crate::error::Result<PreparedRow<'a>> {
        self.array
            .prepared_row_with(&mut self.products, row.logical, row.row)
    }

    pub(crate) fn prepared_at(
        &mut self,
        logical: usize,
        row: ShapeRow<'a>,
    ) -> crate::error::Result<PreparedRow<'a>> {
        self.array
            .prepared_row_with(&mut self.products, logical, row)
    }

    pub(crate) fn warm_prepared_at(
        &mut self,
        logical: usize,
        row: ShapeRow<'a>,
        bounds: Option<Bounds>,
    ) -> crate::error::Result<PreparedRow<'a>> {
        self.array
            .warm_prepared_row_or_transient_with(&mut self.products, logical, row, bounds)
    }

    pub(crate) fn frame_cache(
        &mut self,
        logical: usize,
    ) -> crate::error::Result<Arc<FrameDependentCaches>> {
        self.products.frame_cache(logical)
    }

    pub(crate) fn bounds_seed(&self, logical: usize) -> BoundsSeed {
        self.array.row_bounds_seed(logical)
    }
}

impl RowProductsHandle {
    fn cell(&mut self, logical: usize) -> crate::error::Result<(&Arc<RowProductChunk>, usize)> {
        let physical = self.selection.as_deref().physical(logical);
        let chunk_index = physical / ROWS_PER_CHUNK;
        if self
            .current
            .as_ref()
            .is_none_or(|current| current.chunk_index != chunk_index)
        {
            let chunk = self.owner.cell(physical)?;
            self.current = Some(ResolvedRowProductChunk { chunk_index, chunk });
        }
        let current = self.current.as_ref().expect("cached chunk");
        Ok((&current.chunk, physical % ROWS_PER_CHUNK))
    }

    fn existing_cell(&mut self, logical: usize) -> Option<(&Arc<RowProductChunk>, usize)> {
        let physical = self.selection.as_deref().physical(logical);
        let chunk_index = physical / ROWS_PER_CHUNK;
        if self
            .current
            .as_ref()
            .is_none_or(|current| current.chunk_index != chunk_index)
        {
            let chunk = self.owner.existing_cell(physical)?;
            self.current = Some(ResolvedRowProductChunk { chunk_index, chunk });
        }
        self.current
            .as_ref()
            .map(|current| (&current.chunk, physical % ROWS_PER_CHUNK))
    }

    pub(crate) fn frame_cache(
        &mut self,
        logical: usize,
    ) -> crate::error::Result<Arc<FrameDependentCaches>> {
        let (chunk, offset) = self.cell(logical)?;
        Ok(Arc::clone(
            chunk.cells[offset]
                .frame
                .get_or_init(|| Arc::new(FrameDependentCaches::default())),
        ))
    }

    pub(crate) fn prepared_plan(
        &mut self,
        logical: usize,
        shape: impl FnOnce() -> crate::error::Result<Arc<ShapeData>>,
    ) -> crate::error::Result<Arc<PreparedPlan>> {
        let (chunk, offset) = self.cell(logical)?;
        Ok(Arc::clone(chunk.cells[offset].prepared.get_or_try_init(
            || Ok::<_, crate::error::Error>(Arc::new(PreparedPlan::new(shape()?))),
        )?))
    }

    pub(crate) fn cached_prepared_plan(&mut self, logical: usize) -> Option<Arc<PreparedPlan>> {
        let (chunk, offset) = self.existing_cell(logical)?;
        chunk.cells[offset].prepared.get().cloned()
    }

    pub(crate) fn install_prepared_plan(
        &mut self,
        logical: usize,
        plan: Arc<PreparedPlan>,
    ) -> crate::error::Result<Arc<PreparedPlan>> {
        let (chunk, offset) = self.cell(logical)?;
        Ok(Arc::clone(
            chunk.cells[offset].prepared.get_or_init(|| plan),
        ))
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

    pub(crate) fn frame_cache(
        &self,
        logical: usize,
    ) -> crate::error::Result<Arc<FrameDependentCaches>> {
        self.operation().frame_cache(logical)
    }

    pub(crate) fn prepared_plan(
        &self,
        logical: usize,
        shape: impl FnOnce() -> crate::error::Result<Arc<ShapeData>>,
    ) -> crate::error::Result<Arc<PreparedPlan>> {
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
            return crate::heap_size::ARC_CONTROL_BYTES
                + std::mem::size_of::<RowProductOwner>()
                + self.selection.heap_bytes();
        };
        crate::heap_size::ARC_CONTROL_BYTES
            + std::mem::size_of::<RowProductOwner>()
            + std::mem::size_of::<(usize, Arc<RowProductChunk>)>()
            + chunk.heap_bytes_for_cell(offset)
            + self.selection.heap_bytes()
    }

    pub(crate) fn install_prepared_plan(
        &self,
        logical: usize,
        plan: Arc<PreparedPlan>,
    ) -> crate::error::Result<Arc<PreparedPlan>> {
        self.operation().install_prepared_plan(logical, plan)
    }

    #[cfg(test)]
    pub(crate) fn reset_directory_resolution_count() {
        DIRECTORY_RESOLUTIONS.with(|count| count.set(0));
    }

    #[cfg(test)]
    pub(crate) fn directory_resolution_count() -> usize {
        DIRECTORY_RESOLUTIONS.with(Cell::get)
    }

    fn selected(&self, rows: impl IntoIterator<Item = usize>) -> crate::error::Result<Self> {
        let current = self.selection.as_deref();
        let physical_len = self.owner.physical_len;
        let selection = crate::array::row_selection_from_logical_rows(current, physical_len, rows)?;
        Ok(Self {
            owner: Arc::clone(&self.owner),
            selection,
        })
    }

    #[cfg(test)]
    pub(crate) fn shares_owner_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.owner, &other.owner) && self.selection == other.selection
    }

    #[cfg(test)]
    pub(crate) fn owner_weak(&self) -> std::sync::Weak<RowProductOwner> {
        Arc::downgrade(&self.owner)
    }

    #[cfg(test)]
    pub(crate) fn owner_strong_count(&self) -> usize {
        Arc::strong_count(&self.owner)
    }

    #[cfg(test)]
    pub(crate) fn initialized_counts(&self) -> (usize, usize, usize) {
        let mut chunks = std::collections::HashSet::new();
        let mut prepared = std::collections::HashSet::new();
        let mut frame = std::collections::HashSet::new();
        for logical in 0..self.selection.as_deref().len(self.owner.physical_len) {
            let physical = self.selection.as_deref().physical(logical);
            if let Some(chunk) = self.owner.existing_cell(physical) {
                let offset = physical % ROWS_PER_CHUNK;
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
    pub(crate) fn transient(row: ShapeRow<'a>) -> crate::error::Result<Self> {
        Self::from_row(row, BoundsSeed::Unset)
    }

    pub(crate) fn transient_with_bounds(
        row: ShapeRow<'a>,
        bounds: Option<Bounds>,
    ) -> crate::error::Result<Self> {
        Self::from_row(row, BoundsSeed::Value(bounds))
    }

    pub(crate) fn transient_with_seed(
        row: ShapeRow<'a>,
        seed: BoundsSeed,
    ) -> crate::error::Result<Self> {
        Self::from_row(row, seed)
    }

    fn from_row(row: ShapeRow<'a>, seeded_bounds: BoundsSeed) -> crate::error::Result<Self> {
        let data = match row {
            ShapeRow::Handle(handle) => {
                return Ok(Self::Shared(handle));
            },
            ShapeRow::Shape(shape) => PreparedPlan::new(ShapeData::new(shape.clone())),
            ShapeRow::Point(point) => PreparedPlan::new(ShapeData::new(Shape::Point(point))),
            ShapeRow::Line(coords, start, end) => PreparedPlan::new(ShapeData::new(
                Shape::LineString(crate::geometry::LineSeq::try_new(coords.view(
                    crate::geometry::CoordWindow::from_range(start..end, coords.len()),
                ))?),
            )),
            ShapeRow::Rings(coords, ring_offsets, start, end) => {
                PreparedPlan::new(ShapeData::new(Shape::Polygon(
                    GeometryArrayStorage::polygon_view(coords, ring_offsets, start..end)?,
                )))
            },
        };
        Ok(Self::Transient(match seeded_bounds {
            BoundsSeed::Value(bounds) => data.with_seeded_bounds(bounds),
            BoundsSeed::Unset => data,
        }))
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
        let chunk_bytes = {
            let chunks = self
                .owner
                .chunks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match self.selection.as_deref() {
                crate::array::RowSelectionRef::Identity => {
                    chunks.capacity() * std::mem::size_of::<(usize, Arc<RowProductChunk>)>()
                        + chunks.values().map(HeapSize::heap_bytes).sum::<usize>()
                },
                selection => chunks
                    .iter()
                    .filter(|&(&chunk_index, chunk)| {
                        selection_touches_chunk(selection, chunk_index, chunk.cells.len())
                    })
                    .map(|(&chunk_index, chunk)| {
                        std::mem::size_of::<(usize, Arc<RowProductChunk>)>()
                            + chunk.heap_bytes_for_selection(selection, chunk_index)
                    })
                    .sum::<usize>(),
            }
        };
        let retained = crate::heap_size::ARC_CONTROL_BYTES
            + std::mem::size_of::<RowProductOwner>()
            + chunk_bytes;
        retained + self.selection.heap_bytes()
    }
}

fn selection_touches_chunk(
    selection: crate::array::RowSelectionRef<'_>,
    chunk_index: usize,
    chunk_len: usize,
) -> bool {
    match selection {
        crate::array::RowSelectionRef::Identity => true,
        crate::array::RowSelectionRef::Window { start, len } => {
            let chunk_start = chunk_index.saturating_mul(ROWS_PER_CHUNK);
            start < chunk_start.saturating_add(chunk_len) && chunk_start < start.saturating_add(len)
        },
        crate::array::RowSelectionRef::Gather(rows) => {
            rows.iter().any(|&row| row / ROWS_PER_CHUNK == chunk_index)
        },
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
    pub(crate) fn row_operation(&self) -> ArrayRowOperation<'_> {
        ArrayRowOperation::new(self)
    }

    pub(crate) fn selected_row_products(
        &self,
        rows: impl IntoIterator<Item = usize>,
    ) -> crate::error::Result<RowProducts> {
        self.row_products.selected(rows)
    }

    pub(crate) fn row_bounds_seed(&self, index: usize) -> BoundsSeed {
        self.existing_element_bounds()
            .map_or(BoundsSeed::Unset, |bounds| {
                BoundsSeed::Value(bounds.get(index).copied().flatten())
            })
    }

    pub(crate) fn prepared_row<'a>(
        &'a self,
        index: usize,
        row: ShapeRow<'a>,
    ) -> crate::error::Result<PreparedRow<'a>> {
        let mut products = self.row_products.operation();
        self.prepared_row_with(&mut products, index, row)
    }

    pub(crate) fn prepared_row_with<'a>(
        &self,
        products: &mut RowProductsHandle,
        index: usize,
        row: ShapeRow<'a>,
    ) -> crate::error::Result<PreparedRow<'a>> {
        match row {
            ShapeRow::Handle(handle) => Ok(PreparedRow::Shared(handle)),
            ShapeRow::Shape(shape) => {
                // Seed bounds only when the array-level element-bounds cache
                // exists. Seeding `None` means "known empty" — not "unknown".
                // Mixed storage has no packed fold until first materialization,
                // so leave ShapeData's OnceLock unset and let it compute.
                let (chunk, offset) = products.cell(index)?;
                let handle = chunk.cells[offset].prepared.get_or_init(|| {
                    let data = PreparedPlan::new(ShapeData::new(shape.clone()));
                    if let Some(element_bounds) = self.existing_element_bounds() {
                        Arc::new(
                            data.with_seeded_bounds(element_bounds.get(index).copied().flatten()),
                        )
                    } else {
                        Arc::new(data)
                    }
                });
                Ok(PreparedRow::Owned(Arc::clone(handle)))
            },
            packed @ (ShapeRow::Line(..) | ShapeRow::Rings(..))
                if packed_row_persistent(packed) =>
            {
                let (chunk, offset) = products.cell(index)?;
                if let Some(plan) = chunk.cells[offset].prepared.get() {
                    return Ok(PreparedRow::Owned(Arc::clone(plan)));
                }
                // Seeded only for the row being built: a warm row returns above.
                let bounds = self.row_bounds_seed(index);
                let transient = PreparedRow::transient_with_seed(packed, bounds)?;
                let handle = chunk.cells[offset]
                    .prepared
                    .get_or_init(|| match transient {
                        PreparedRow::Transient(data) => Arc::new(data),
                        PreparedRow::Shared(data) => Arc::clone(data),
                        PreparedRow::Owned(data) => data,
                    });
                Ok(PreparedRow::Owned(Arc::clone(handle)))
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
    ) -> crate::error::Result<PreparedRow<'a>> {
        let mut products = self.row_products.operation();
        self.warm_prepared_row_or_transient_with(&mut products, index, row, bounds)
    }

    pub(crate) fn warm_prepared_row_or_transient_with<'a>(
        &self,
        products: &mut RowProductsHandle,
        index: usize,
        row: ShapeRow<'a>,
        bounds: Option<Bounds>,
    ) -> crate::error::Result<PreparedRow<'a>> {
        if let ShapeRow::Handle(handle) = row {
            return Ok(PreparedRow::Shared(handle));
        }
        let Some((chunk, offset)) = products.existing_cell(index) else {
            return PreparedRow::transient_with_bounds(row, bounds);
        };
        chunk.cells[offset].prepared.get().map_or_else(
            || PreparedRow::transient_with_bounds(row, bounds),
            |plan| Ok(PreparedRow::Owned(Arc::clone(plan))),
        )
    }

    /// Scalar geometry for a logical row: mixed rows share one array-cached
    /// [`PreparedPlan`]; packed rows build a fresh handle (same as before).
    pub(crate) fn geometry_at(&self, index: usize) -> crate::error::Result<PyGeometry> {
        let crate::array::LogicalRow::Present { row, .. } = self
            .logical_row(index)
            .expect("geometry row must be in bounds")
        else {
            unreachable!("missing rows are rejected before scalar extraction")
        };
        let mut operation = self.row_operation();
        operation.geometry(PresentRow::new(self, index, row))
    }

    fn geometry_from_present_row(
        &self,
        products: &mut RowProductsHandle,
        row: PresentRow<'_>,
    ) -> crate::error::Result<PyGeometry> {
        let frame = self.frame.clone();
        if matches!(self.storage(), GeometryArrayStorage::Mixed(_)) {
            let plan = products.prepared_plan(row.logical, || {
                Ok(Arc::new(ShapeData::new(row.row.with_shape(Clone::clone)?)))
            })?;
            return Ok(PyGeometry::with_row_products(
                Arc::clone(plan.shape_arc()),
                frame,
                self.row_products.clone(),
                row.logical,
            ));
        }
        Ok(PyGeometry::with_row_products(
            Arc::new(ShapeData::new(row.row.with_shape(Clone::clone)?)),
            frame,
            self.row_products.clone(),
            row.logical,
        ))
    }
}

#[cfg(test)]
mod frame_cache_row_tests {
    use std::cell::Cell;

    use super::*;
    use crate::boundary::{Frame, crs_arc_static};
    use crate::geometry::{CoordSeq, LineSeq, PlanarMetric, Point};
    use crate::test_support::allocation_count;

    fn points() -> PyGeometryArray {
        PyGeometryArray::packed_points(
            CoordSeq::try_from_vecs(vec![0.0, 1.0, 2.0], vec![0.0, 1.0, 2.0], None, None).unwrap(),
            Frame::None,
        )
        .unwrap()
    }

    #[test]
    fn row_operation_keeps_dense_compact_and_duplicate_logical_identity() {
        let dense = points();
        let masked = PyGeometryArray::scatter_present_rows(
            &dense,
            crate::array::ValidityMask::from_vec(5, vec![false, true, false, true, false])
                .unwrap()
                .expect("validity length"),
        )
        .unwrap();
        assert!(masked.is_compact_nullable());

        let logical_xs = |array: &PyGeometryArray| {
            array
                .row_operation()
                .rows_with_missing(|| Option::<f64>::None)
                .map(|row| match row {
                    ArrayRow::Missing { value, .. } => value,
                    ArrayRow::Present(row) => match row.row() {
                        ShapeRow::Point(point) => Some(point.x),
                        _ => unreachable!("point fixture yields point rows"),
                    },
                })
                .collect::<Vec<_>>()
        };

        assert_eq!(logical_xs(&masked), [
            Some(0.0),
            None,
            Some(1.0),
            None,
            Some(2.0)
        ]);
        let gathered = masked.gather_logical_rows(&[4, 2, 4, 1]).unwrap();
        assert_eq!(logical_xs(&gathered), [
            Some(2.0),
            Some(1.0),
            Some(2.0),
            None
        ]);
    }

    #[test]
    fn arbitrary_present_row_resolution_keeps_logical_and_payload_identity() {
        let source = PyGeometryArray::scatter_present_rows(
            &points(),
            crate::array::ValidityMask::from_vec(5, vec![false, true, false, true, false])
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let mut operation = source.row_operation();
        let rows = [3, 0, 3, 2];
        let actual = rows
            .iter()
            .map(|&logical| {
                operation.present_row_at(logical).unwrap().map(|row| {
                    let ShapeRow::Point(point) = row.row() else {
                        unreachable!("point fixture yields point rows")
                    };
                    (row.logical(), point.x)
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, [None, Some((0, 0.0)), None, Some((2, 1.0)),]);
    }

    #[test]
    fn row_operation_does_not_prepare_missing_rows() {
        let source = PyGeometryArray::mixed_shapes(
            vec![
                Shape::Point(Point::new_raw_xy(0.0, 0.0)),
                Shape::Point(Point::new_raw_xy(1.0, 1.0)),
                Shape::Point(Point::new_raw_xy(2.0, 2.0)),
            ],
            Frame::None,
        )
        .unwrap()
        .with_missing_mask(Some(
            crate::array::ValidityMask::from_vec(3, vec![false, true, false])
                .unwrap()
                .expect("validity length"),
        ))
        .unwrap();
        let mut operation = source.row_operation();
        for row in operation.rows_with_missing(|| ()) {
            if let ArrayRow::Present(row) = row {
                let logical = row.logical();
                let _ = operation.prepared(row);
                let _ = operation.frame_cache(logical).unwrap();
            }
        }
        assert_eq!(source.row_products.initialized_counts(), (1, 2, 2));
    }

    #[test]
    fn huge_owners_are_cold_and_chunk_boundaries_are_physical() {
        for len in [1_000_000, 100_000_000] {
            let products = RowProducts::fresh(len);
            assert!(products.owner.chunks.lock().unwrap().is_empty());
            assert_eq!(
                products.heap_bytes(),
                crate::heap_size::ARC_CONTROL_BYTES + std::mem::size_of::<RowProductOwner>()
            );
        }

        let products = RowProducts::fresh(100_000_000);
        let mut handle = products.operation();
        let (chunk, offset) = handle.cell(63).unwrap();
        let row_63 = (Arc::as_ptr(chunk), offset);
        assert_eq!(products.owner.chunks.lock().unwrap().len(), 1);
        let (chunk, offset) = handle.cell(64).unwrap();
        let row_64 = (Arc::as_ptr(chunk), offset);
        assert_eq!(products.owner.chunks.lock().unwrap().len(), 2);
        assert_ne!(row_63.0, row_64.0);
    }

    #[test]
    fn packed_scalar_size_uses_its_visible_coordinate_window() {
        let rows = 10_000;
        let array = PyGeometryArray::packed_lines(
            CoordSeq::try_from_vecs(
                (0..rows * 2).map(|value| value as f64).collect(),
                vec![0.0; rows * 2],
                None,
                None,
            )
            .unwrap(),
            crate::geometry::CsrOffsetColumn::try_new(
                (0..=rows).map(|row| row * 2).collect(),
                rows * 2,
            )
            .unwrap(),
            Frame::None,
        )
        .unwrap();
        let scalar = array.geometry_at(0).unwrap();

        assert!(
            scalar.heap_bytes() < array.heap_bytes(),
            "a packed scalar must not retain its whole parent coordinate payload"
        );
    }

    #[test]
    fn selected_row_products_do_not_charge_warmed_sibling_cells() {
        let source = RowProducts::fresh(128);
        source.frame_cache(0).unwrap();
        source.frame_cache(1).unwrap();
        source.frame_cache(64).unwrap();
        let selected = source.selected([1, 1]).unwrap();
        let unpopulated = RowProducts::fresh(128);
        let _ = unpopulated.operation().cell(1).unwrap();
        unpopulated.frame_cache(1).unwrap();
        let selected_without_products = unpopulated.selected([1, 1]).unwrap();

        assert_eq!(
            selected.heap_bytes(),
            selected_without_products.heap_bytes(),
            "a logical row view must not charge cached products for unselected rows"
        );
    }

    #[test]
    fn tiny_fresh_result_does_not_retain_a_huge_warmed_source_owner() {
        let source = RowProducts::fresh(100_000_000);
        let shape = Arc::new(ShapeData::new(Shape::Point(Point::new_raw_xy(7.0, 11.0))));
        let source_plan = source
            .prepared_plan(99_999_999, || Ok(Arc::clone(&shape)))
            .unwrap();
        let source_frame = source.frame_cache(99_999_999).unwrap();
        let source_owner = source.owner_weak();
        let result = RowProducts::fresh(1);
        let source_counts = {
            let chunks = source.owner.chunks.lock().unwrap();
            let chunk = chunks
                .get(&(99_999_999 / ROWS_PER_CHUNK))
                .expect("warmed source chunk");
            (
                chunks.len(),
                usize::from(
                    chunk.cells[99_999_999 % ROWS_PER_CHUNK]
                        .prepared
                        .get()
                        .is_some(),
                ),
                usize::from(
                    chunk.cells[99_999_999 % ROWS_PER_CHUNK]
                        .frame
                        .get()
                        .is_some(),
                ),
            )
        };
        let result_counts = result.initialized_counts();
        let source_heap = source.heap_bytes();
        let result_heap = result.heap_bytes();

        assert_eq!(source.owner_strong_count(), 1);
        assert_eq!(result.owner_strong_count(), 1);
        assert_eq!(source_counts, (1, 1, 1));
        assert_eq!(result_counts, (0, 0, 0));
        assert!(source_heap > result_heap);
        println!(
            "row_product_diagnostic source_counts={source_counts:?} \
             result_counts={result_counts:?} source_heap={source_heap} \
             result_heap={result_heap} source_owner_strong={} \
             result_owner_strong={} source_alive_before_drop={}",
            source.owner_strong_count(),
            result.owner_strong_count(),
            source_owner.upgrade().is_some(),
        );

        drop(source_plan);
        drop(source_frame);
        drop(source);
        assert!(source_owner.upgrade().is_none());
        println!(
            "row_product_diagnostic source_alive_after_drop={}",
            source_owner.upgrade().is_some()
        );
    }

    #[test]
    fn duplicate_logical_rows_share_one_physical_cell() {
        let products = RowProducts::fresh(3).selected([2, 2]).unwrap();
        let mut handle = products.operation();
        let (chunk, offset) = handle.cell(0).unwrap();
        let first = (Arc::as_ptr(chunk), offset);
        let (chunk, offset) = handle.cell(1).unwrap();
        let second = (Arc::as_ptr(chunk), offset);
        assert_eq!(first, second);
    }

    #[test]
    fn operation_resolves_each_sequential_chunk_once() {
        let products = RowProducts::fresh(192);
        let mut handle = products.operation();
        RowProducts::reset_directory_resolution_count();
        for row in 0..192 {
            let _ = handle.frame_cache(row).unwrap();
            let _ = handle.frame_cache(row).unwrap();
        }
        assert_eq!(RowProducts::directory_resolution_count(), 3);
    }

    #[test]
    fn prepared_and_frame_access_share_one_resolved_slot() {
        let products = RowProducts::fresh(1);
        let mut handle = products.operation();
        RowProducts::reset_directory_resolution_count();
        let _ = handle
            .prepared_plan(0, || {
                Ok(Arc::new(ShapeData::new(Shape::Point(Point::new_raw_xy(
                    1.0, 2.0,
                )))))
            })
            .unwrap();
        let _ = handle.frame_cache(0).unwrap();
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
    fn point_selection_preserves_source_products_after_payload_rebasing() {
        use crate::py::row::RowGetItemContainer as _;

        let source = PyGeometryArray::packed_points(
            CoordSeq::try_from_vecs(vec![3.0, 7.0, 13.0], vec![5.0, 11.0, 17.0], None, None)
                .unwrap(),
            Frame::None,
        )
        .unwrap();
        let expected = source.row_products.frame_cache(1).unwrap();
        let other = source.row_products.frame_cache(0).unwrap();
        assert!(!Arc::ptr_eq(&expected, &other));
        for selected in [
            source.gather_logical_row_range(1..3).unwrap(),
            source.gather_logical_rows(&[1, 2]).unwrap(),
            source.slice_rows(1, 3, 1).unwrap(),
            source.slice_rows(1, -1, -1).unwrap(),
        ] {
            assert!(Arc::ptr_eq(
                &selected.row_products.frame_cache(0).unwrap(),
                &expected
            ));
            let duplicates = selected.gather_logical_rows(&[0, 0]).unwrap();
            assert!(Arc::ptr_eq(
                &duplicates.row_products.frame_cache(1).unwrap(),
                &expected
            ));
        }
    }

    #[test]
    fn concurrent_first_publication_keeps_one_prepared_arc() {
        let products = Arc::new(RowProducts::fresh(1));
        let mut threads: Vec<_> = std::iter::repeat_with(|| {
            let products = Arc::clone(&products);
            std::thread::spawn(move || {
                products
                    .operation()
                    .prepared_plan(0, || {
                        Ok(Arc::new(ShapeData::new(Shape::Point(Point::new_raw_xy(
                            1.0, 2.0,
                        )))))
                    })
                    .unwrap()
            })
        })
        .take(8)
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
                    LineSeq::try_new(
                        CoordSeq::try_from_vecs(vec![0.0, 1.0], vec![0.0, 1.0], None, None)
                            .unwrap(),
                    )
                    .expect("fixture line has two vertices"),
                ),
                Shape::Point(Point::new_raw_xy(3.0, 4.0)),
            ],
            Frame::None,
        )
        .unwrap()
    }

    #[test]
    fn mixed_storage_accounts_nested_shape_owners() {
        use crate::heap_size::HeapSize as _;

        let shape = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(vec![0.0, 1.0], vec![0.0, 1.0], None, None).unwrap(),
            )
            .unwrap(),
        );
        let array = PyGeometryArray::mixed_shapes(vec![shape.clone()], Frame::None).unwrap();
        let nested_owners = shape.heap_bytes().saturating_sub(shape.coordinate_bytes());
        let expected = crate::heap_size::ARC_CONTROL_BYTES
            + std::mem::size_of::<GeometryArrayStorage>()
            + shape.coordinate_bytes()
            + std::mem::size_of::<Shape>()
            + nested_owners;

        assert_eq!(array.storage().logical_heap_bytes(), expected);
    }

    #[test]
    fn mixed_geometry_at_shares_prepared_plan_shape_data_arc() {
        let array = mixed_shapes();
        let a = array.geometry_at(1).unwrap();
        let b = array.geometry_at(1).unwrap();
        assert!(
            Arc::ptr_eq(&a.shape, &b.shape),
            "arr[i] must reuse the array-side prepared plan's ShapeData Arc"
        );
        assert!(Arc::ptr_eq(
            &a.frame_cache().unwrap(),
            &b.frame_cache().unwrap()
        ));
        // Different rows stay independent.
        let c = array.geometry_at(0).unwrap();
        assert!(!Arc::ptr_eq(&a.shape, &c.shape));
    }

    #[test]
    fn prepared_plan_factory_is_cold_once_and_lazy_when_warm() {
        let shape = Arc::new(ShapeData::new(Shape::Point(Point::new_raw_xy(1.0, 2.0))));
        let cold_products = RowProducts::fresh(1);
        let cold_calls = Cell::new(0);
        let cold_plan = cold_products
            .prepared_plan(0, || {
                cold_calls.set(cold_calls.get() + 1);
                Ok(Arc::clone(&shape))
            })
            .unwrap();
        assert_eq!(cold_calls.get(), 1);
        assert!(Arc::ptr_eq(cold_plan.shape_arc(), &shape));

        let warm_products = RowProducts::fresh(1);
        let warm_calls = Cell::new(0);
        let warm_plan = warm_products
            .prepared_plan(0, || {
                warm_calls.set(warm_calls.get() + 1);
                Ok(Arc::clone(&shape))
            })
            .unwrap();
        assert_eq!(warm_calls.get(), 1);
        let reused = warm_products
            .prepared_plan(0, || panic!("warm prepared-plan factory was invoked"))
            .unwrap();
        assert!(Arc::ptr_eq(&warm_plan, &reused));
        assert_eq!(warm_calls.get(), 1);
    }

    #[test]
    fn warm_mixed_geometry_at_avoids_rebuilding_shape_data() {
        let line = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(
                    vec![0.0, 1.0],
                    vec![0.0, 1.0],
                    Some(vec![2.0, 3.0]),
                    Some(vec![4.0, 5.0]),
                )
                .unwrap(),
            )
            .expect("fixture line has XYZM vertices"),
        );
        let array = PyGeometryArray::mixed_shapes(
            vec![
                line.clone(),
                Shape::Point(Point::new_raw_xy(3.0, 4.0)),
                line.clone(),
            ],
            Frame::None,
        )
        .unwrap()
        .with_missing_mask(Some(
            crate::array::ValidityMask::from_vec(3, vec![false, true, false])
                .unwrap()
                .expect("validity length"),
        ))
        .unwrap();

        let cold = array.geometry_at(0).unwrap();
        let (_, eager_allocations) = allocation_count(|| Arc::new(ShapeData::new(line.clone())));
        let (warm, warm_allocations) = allocation_count(|| array.geometry_at(0).unwrap());
        assert!(Arc::ptr_eq(&cold.shape, &warm.shape));
        assert!(warm_allocations < eager_allocations);
    }

    #[test]
    fn clones_slices_and_scalar_extraction_share_row_slots() {
        let array = points();
        let clone = array.clone();
        assert!(Arc::ptr_eq(
            &array.row_products.frame_cache(1).unwrap(),
            &clone.row_products.frame_cache(1).unwrap()
        ));

        let slice = array.gather_logical_rows(&[2, 0]).unwrap();
        assert!(Arc::ptr_eq(
            &array.row_products.frame_cache(2).unwrap(),
            &slice.row_products.frame_cache(0).unwrap()
        ));
        let extracted = slice.geometry_at(0).unwrap();
        assert!(Arc::ptr_eq(
            &extracted.frame_cache().unwrap(),
            &array.row_products.frame_cache(2).unwrap()
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
                &array.row_products.frame_cache(row).unwrap(),
                &retagged.row_products.frame_cache(row).unwrap()
            ));
        }
    }

    #[test]
    fn retagged_gathered_scalars_keep_their_physical_product_cells() {
        let source = points().gather_logical_rows(&[2, 2, 1]).unwrap();
        let before = source.row_products.initialized_counts();
        let retagged = source.retag_frame(Frame::Crs(crs_arc_static("EPSG:4326")));

        assert!(
            source
                .row_products
                .shares_owner_with(&retagged.row_products)
        );
        assert_eq!(before, retagged.row_products.initialized_counts());
        let first = retagged.geometry_at(0).unwrap();
        let duplicate = retagged.geometry_at(1).unwrap();
        let source_row = source.geometry_at(0).unwrap();
        assert_eq!(first.product_row, 0);
        assert_eq!(duplicate.product_row, 1);
        assert!(Arc::ptr_eq(
            &first.frame_cache().unwrap(),
            &duplicate.frame_cache().unwrap()
        ));
        assert!(Arc::ptr_eq(
            &first.frame_cache().unwrap(),
            &source_row.frame_cache().unwrap()
        ));
        assert_eq!(source.row_products.initialized_counts(), (1, 0, 1));
    }

    #[test]
    fn retags_share_the_inner_keyed_frame_cache_and_prepared_plan() {
        let a = mixed_shapes().retag_frame(Frame::Crs(crs_arc_static("EPSG:4326")));
        let prepared_a = a
            .prepared_row(1, a.storage().row(1))
            .unwrap()
            .into_owned_plan();
        let cache_a = a.row_products.frame_cache(1).unwrap();
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
        let prepared_b = b
            .prepared_row(1, b.storage().row(1))
            .unwrap()
            .into_owned_plan();
        let cache_b = b.row_products.frame_cache(1).unwrap();
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
        let cache_a2 = back_to_a.row_products.frame_cache(1).unwrap();
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
        let _ = array.row_products.frame_cache(0).unwrap();
        assert_eq!(array.row_products.owner.chunks.lock().unwrap().len(), 1);
        let gathered = array.gather_logical_rows(&[2, 0]).unwrap();
        assert!(Arc::ptr_eq(
            &array.row_products.owner,
            &gathered.row_products.owner
        ));
    }

    #[test]
    fn planar_metric_over_million_logical_rows_initializes_no_products() {
        let source = points();
        let rows: Vec<usize> = (0..1_000_000).map(|row| row % 3).collect();
        let array = source.gather_logical_rows(&rows).unwrap();
        let probe = PreparedPlan::new(Shape::Point(Point::new_raw_xy(4.0, 4.0)));

        for row in array.storage().iter_rows() {
            let data = PreparedRow::transient(row).unwrap();
            crate::broadcast::pair_distance_planar(1.0, &data, &probe).unwrap();
        }

        assert_eq!(array.row_products.initialized_counts(), (0, 0, 0));
    }

    #[test]
    fn duplicate_selections_share_only_admitted_frame_cells() {
        let array = points().gather_logical_rows(&[1, 1, 2, 0, 1]).unwrap();
        let first = array.row_products.frame_cache(0).unwrap();
        let duplicate = array.row_products.frame_cache(1).unwrap();
        let refined = array.row_products.frame_cache(2).unwrap();

        assert!(Arc::ptr_eq(&first, &duplicate));
        assert!(!Arc::ptr_eq(&first, &refined));
        assert_eq!(array.row_products.initialized_counts(), (1, 0, 2));
    }

    #[test]
    fn geodesic_metric_initializes_only_refined_duplicate_row() {
        let array = points().gather_logical_rows(&[1, 1, 2]).unwrap();
        let probe = PreparedPlan::new(Shape::Point(Point::new_raw_xy(3.0, 3.0)));
        let probe_cache = FrameDependentCaches::default();
        let model = crate::crs::metric_model(Some("EPSG:4326")).unwrap();
        let resolved = crate::crs::ResolvedMetric::from_model(&model).unwrap();
        let crate::crs::ResolvedMetric::Geodesic(metric) = &resolved else {
            panic!("EPSG:4326 resolves geodesically")
        };

        for row in [0, 1] {
            let data = PreparedRow::transient(array.storage().row(row)).unwrap();
            crate::broadcast::pair_distance_geodesic(
                metric,
                &data,
                &array.row_products.frame_cache(row).unwrap(),
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
        let coords = CoordSeq::try_from_vecs(
            (0..vertices).map(|value| value as f64).collect(),
            vec![0.0; vertices],
            None,
            None,
        )
        .unwrap();
        PyGeometryArray::packed_lines(
            coords,
            CsrOffsetColumn::try_new(vec![0, vertices], vertices).unwrap(),
            Frame::None,
        )
        .unwrap()
    }

    #[test]
    fn packed_row_persistence_matches_the_execution_plan_boundary() {
        let linear = lines(3);
        assert!(uses_linear_plan_for_len(2));
        assert!(linear.row_products.owner.chunks.lock().unwrap().is_empty());
        let _ = linear.prepared_row(0, linear.storage().row(0)).unwrap();
        assert!(linear.row_products.owner.chunks.lock().unwrap().is_empty());

        let nonlinear = lines(34);
        assert!(!uses_linear_plan_for_len(33));
        let first = nonlinear
            .prepared_row(0, nonlinear.storage().row(0))
            .unwrap();
        assert_eq!(nonlinear.row_products.owner.chunks.lock().unwrap().len(), 1);
        let second = nonlinear
            .prepared_row(0, nonlinear.storage().row(0))
            .unwrap();
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
            CoordSeq::try_from_vecs(xs, ys, None, None).unwrap(),
            CsrOffsetColumn::<RingLevel>::try_new(ring_offsets, 170).unwrap(),
            CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 34], 34).unwrap(),
            Frame::None,
        )
        .unwrap();
        assert!(uses_linear_plan_for_len(4));
        assert!(!uses_linear_plan_for_len(33));
        let _ = polygons.prepared_row(0, polygons.storage().row(0)).unwrap();
        assert_eq!(polygons.row_products.owner.chunks.lock().unwrap().len(), 1);
    }

    #[test]
    fn packed_geometry_at_reuses_an_initialized_prepared_plan() {
        let array = lines(34);
        let plan = array
            .prepared_row(0, array.storage().row(0))
            .unwrap()
            .into_owned_plan();
        let geometry = array.geometry_at(0).unwrap();
        let extracted = geometry.prepared_plan().unwrap();
        assert!(Arc::ptr_eq(&plan, &extracted));
        assert!(Arc::ptr_eq(
            &geometry.frame_cache().unwrap(),
            &array.row_products.frame_cache(0).unwrap()
        ));
    }

    #[test]
    fn transient_point_conversion_preserves_shape_and_bounds() {
        let array = PyGeometryArray::packed_points(
            CoordSeq::try_from_vecs(vec![2.0], vec![3.0], None, None).unwrap(),
            Frame::None,
        )
        .unwrap();
        let prepared = array
            .prepared_row(0, array.storage().row(0))
            .unwrap()
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
        let prepared = PreparedRow::transient(array.storage().row(0)).unwrap();
        assert_eq!(
            prepared.bounds(),
            Some(crate::geometry::Bounds::new_raw(0.0, 0.0, 2.0, 0.0))
        );
    }

    #[test]
    fn array_dwithin_unknown_bounds_stay_lazy() {
        let array = PyGeometryArray::mixed_shapes(
            vec![Shape::LineString(
                crate::geometry::LineSeq::try_new(
                    CoordSeq::try_from_vecs(vec![0.0, 1.0], vec![0.0, 1.0], None, None).unwrap(),
                )
                .expect("fixture line has two vertices"),
            )],
            Frame::None,
        )
        .unwrap();
        let seed = array.row_bounds_seed(0);
        assert!(matches!(seed, BoundsSeed::Unset));

        // The array dwithin lane passes this seed to its transient query
        // ShapeData. Unset must remain unset until exact refinement asks for it.
        let prepared = PreparedRow::transient_with_seed(array.storage().row(0), seed).unwrap();
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
            .unwrap()
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
                !left_row
                    .try_with_shape(|left| right_row.try_with_shape(|right| left.disjoint(right)))
                    .unwrap()
            );

            let left_data = left
                .warm_prepared_row_or_transient(0, left_row, left_bounds)
                .unwrap();
            let right_data = right
                .warm_prepared_row_or_transient(0, right_row, right_bounds)
                .unwrap();
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
