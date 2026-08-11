//! Generic coverage iterator + rectangular-grid coverage `#[pymethods]` helpers.
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};

use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyList};

use crate::geometry::{CoordSeq, LineSeq, Point, Polygon, Shape, ShapeData, is_geographic_frame};
use crate::grid::affine_source::normalize_grid_source;
use crate::grid::cell::{CellDepth, GridCell, RectGridCell};
use crate::grid::coverer;
use crate::py::cells::cell_ops::cell_array_from_keys;
use crate::py::cells::{CellRule, CoveragePredicate, GridKind, PyCellArray, coverage_to_polygon};
use crate::py::errors::{GeometryError, InvalidGeometryError, integer_parameter_error};
use crate::py::row::RowContainer;
use crate::{
    HeapSize, PyGeometry, Typed, exact_geometry, expected_geometry_or_array, lonlat_shape,
    validate_lonlat_shape,
};

/// One typed coverage cell stored canonically as its public 64-bit key.
pub(crate) trait CoverageCell: Copy + HeapSize {
    fn from_coverage_id(id: u64) -> Self;
    fn coverage_id(self) -> u64;
}

impl CoverageCell for crate::py::cells::h3::PyH3Cell {
    fn from_coverage_id(id: u64) -> Self {
        Self {
            cell: h3o::CellIndex::try_from(id).expect("coverage contains validated H3 ids"),
        }
    }

    fn coverage_id(self) -> u64 {
        self.cell.into()
    }
}

impl CoverageCell for crate::py::cells::s2::PyS2Cell {
    fn from_coverage_id(id: u64) -> Self {
        Self {
            cell: crate::grid::s2::cellid::CellId::from_raw(id)
                .expect("coverage contains validated S2 ids"),
        }
    }

    fn coverage_id(self) -> u64 {
        self.cell.raw()
    }
}

impl CoverageCell for crate::py::cells::geohash::PyGeohashCell {
    fn from_coverage_id(id: u64) -> Self {
        Self {
            cell: crate::py::cells::grid_kind::geohash_from_identity_key(id)
                .expect("coverage contains validated geohash ids"),
        }
    }

    fn coverage_id(self) -> u64 {
        self.cell.identity_key()
    }
}

impl CoverageCell for crate::py::cells::tiles::PyTile {
    fn from_coverage_id(id: u64) -> Self {
        Self {
            cell: crate::grid::tile::Tile::from_id(id)
                .expect("coverage contains validated tile ids"),
        }
    }

    fn coverage_id(self) -> u64 {
        self.cell.id()
    }
}

/// Canonical coverage-set identity column. Construction sorts and
/// deduplicates once, so every later binary search and partition walk is
/// correct by type rather than convention. This type is internal: public
/// `CellArray` continues to preserve order and duplicates.
#[derive(Clone, Debug)]
struct SortedUniqueCellIds(Arc<[u64]>);

impl SortedUniqueCellIds {
    fn from_cells<C: CoverageCell>(cells: impl IntoIterator<Item = C>) -> Self {
        let mut ids: Vec<u64> = cells.into_iter().map(CoverageCell::coverage_id).collect();
        ids.sort_unstable();
        ids.dedup();
        Self(ids.into())
    }

    fn from_sorted_ids(ids: Vec<u64>) -> Self {
        debug_assert!(
            ids.windows(2).all(|pair| pair[0] < pair[1]),
            "coverage ids must be sorted and unique"
        );
        Self(ids.into())
    }

    fn shared(&self) -> Arc<[u64]> {
        Arc::clone(&self.0)
    }
}

impl std::ops::Deref for SortedUniqueCellIds {
    type Target = [u64];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Sorted unique physical rows into one `SortedUniqueCellIds` column.
#[derive(Clone, Debug)]
struct CheckedSelection(Arc<[usize]>);

impl CheckedSelection {
    fn new(rows: Vec<usize>, source_len: usize) -> Self {
        debug_assert!(
            rows.iter().all(|&row| row < source_len),
            "coverage selection row out of bounds"
        );
        debug_assert!(
            rows.windows(2).all(|pair| pair[0] < pair[1]),
            "coverage selection must be sorted and unique"
        );
        Self(rows.into())
    }
}

impl std::ops::Deref for CheckedSelection {
    type Target = [usize];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Logical rows over one canonical sorted id column. Selected rows use native
/// physical indices, matching the backing slice without duplicating cell ids.
#[derive(Clone, Debug)]
pub(crate) struct CoverageCells<C> {
    ids: SortedUniqueCellIds,
    selected: Option<CheckedSelection>,
    marker: PhantomData<C>,
}

impl<C: CoverageCell> CoverageCells<C> {
    const fn all(ids: SortedUniqueCellIds) -> Self {
        Self {
            ids,
            selected: None,
            marker: PhantomData,
        }
    }

    const fn selected(ids: SortedUniqueCellIds, selected: CheckedSelection) -> Self {
        Self {
            ids,
            selected: Some(selected),
            marker: PhantomData,
        }
    }

    pub(crate) fn from_cells(cells: Vec<C>) -> Self {
        Self::all(SortedUniqueCellIds::from_cells(cells))
    }

    pub(crate) fn len(&self) -> usize {
        self.selected
            .as_ref()
            .map_or(self.ids.len(), |rows| rows.len())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn physical(&self, logical: usize) -> usize {
        self.selected.as_ref().map_or(logical, |rows| rows[logical])
    }

    pub(crate) fn get(&self, logical: usize) -> Option<C> {
        (logical < self.len()).then(|| C::from_coverage_id(self.ids[self.physical(logical)]))
    }

    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = C> + DoubleEndedIterator + '_ {
        (0..self.len()).map(|logical| C::from_coverage_id(self.ids[self.physical(logical)]))
    }

    pub(crate) fn contains_id(&self, id: u64) -> bool {
        let Ok(physical) = self.ids.binary_search(&id) else {
            return false;
        };
        self.selected
            .as_ref()
            .is_none_or(|rows| rows.binary_search(&physical).is_ok())
    }

    pub(crate) fn logical_index(&self, id: u64) -> Option<usize> {
        let physical = self.ids.binary_search(&id).ok()?;
        self.selected
            .as_ref()
            .map_or(Some(physical), |rows| rows.binary_search(&physical).ok())
    }

    pub(crate) fn cell_array(&self, kind: GridKind) -> PyCellArray {
        let selection = self
            .selected
            .as_ref()
            .map_or(crate::array::RowSelection::Identity, |rows| {
                crate::array::RowSelection::gather_trusted(rows.to_vec().into(), self.ids.len())
            });
        PyCellArray::from_shared_ids(kind, self.ids.shared(), selection)
    }

    fn shares_ids(&self, ids: &SortedUniqueCellIds) -> bool {
        Arc::ptr_eq(&self.ids.0, &ids.0)
    }

    fn selection_heap_bytes(&self) -> usize {
        self.selected
            .as_ref()
            .map_or(0, |rows| rows.len() * std::mem::size_of::<usize>())
    }

    pub(crate) fn additional_heap_bytes(&self, partition: &CoveragePartition<C>) -> usize {
        self.selection_heap_bytes()
            + if self.shares_ids(&partition.ids) {
                0
            } else {
                self.ids.len() * std::mem::size_of::<u64>()
            }
    }
}

impl<C: CoverageCell> HeapSize for CoverageCells<C> {
    fn heap_bytes(&self) -> usize {
        self.ids.len() * std::mem::size_of::<u64>() + self.selection_heap_bytes()
    }
}

/// Rule-independent outer/interior classification over one canonical sorted
/// id column. Interior is a compact selected-row vector, not a second id set.
#[derive(Clone, Debug)]
pub(crate) struct CoveragePartition<C> {
    ids: SortedUniqueCellIds,
    interior: CheckedSelection,
    marker: PhantomData<C>,
}

impl<C: CoverageCell> CoveragePartition<C> {
    /// Build from a **sorted** `(cell, is_interior)` stream (coverer / polyfill
    /// contract). Adjacent duplicate tags are merged (interior wins); the stream
    /// is not re-sorted.
    pub(crate) fn from_sorted_tagged(cells: impl IntoIterator<Item = (C, bool)>) -> Self {
        let mut ids = Vec::new();
        let mut interior = Vec::new();
        let mut last_id: Option<u64> = None;
        for (cell, is_interior) in cells {
            let id = cell.coverage_id();
            if let Some(prev) = last_id {
                debug_assert!(
                    id >= prev,
                    "coverage tagged stream must be sorted by cell id"
                );
                if id == prev {
                    if is_interior && interior.last() != Some(&(ids.len() - 1)) {
                        interior.push(ids.len() - 1);
                    }
                    continue;
                }
            }
            if is_interior {
                interior.push(ids.len());
            }
            ids.push(id);
            last_id = Some(id);
        }
        let ids = SortedUniqueCellIds::from_sorted_ids(ids);
        let interior = CheckedSelection::new(interior, ids.len());
        Self {
            ids,
            interior,
            marker: PhantomData,
        }
    }

    pub(crate) fn all(&self) -> CoverageCells<C> {
        CoverageCells::all(self.ids.clone())
    }

    pub(crate) fn interior(&self) -> CoverageCells<C> {
        CoverageCells::selected(self.ids.clone(), self.interior.clone())
    }

    pub(crate) fn boundary(&self) -> CoverageCells<C> {
        let mut selected = Vec::with_capacity(self.ids.len() - self.interior.len());
        let mut interior = self.interior.iter().copied().peekable();
        for row in 0..self.ids.len() {
            if interior.peek() == Some(&row) {
                interior.next();
            } else {
                selected.push(row);
            }
        }
        CoverageCells::selected(
            self.ids.clone(),
            CheckedSelection::new(selected, self.ids.len()),
        )
    }

    pub(crate) fn outer_len(&self) -> usize {
        self.ids.len()
    }

    pub(crate) fn interior_len(&self) -> usize {
        self.interior.len()
    }

    pub(crate) fn heap_bytes(&self) -> usize {
        self.ids.len() * std::mem::size_of::<u64>()
            + self.interior.len() * std::mem::size_of::<usize>()
    }
}

/// Frozen cell buffer walked by coverage iterators.
#[derive(Clone)]
pub(crate) struct CoverageIterCells<C> {
    cells: CoverageCells<C>,
}

impl<C: CoverageCell> CoverageIterCells<C> {
    pub(crate) fn new(cells: &CoverageCells<C>) -> Self {
        Self {
            cells: cells.clone(),
        }
    }
}

impl<C: CoverageCell> HeapSize for CoverageIterCells<C> {
    fn heap_bytes(&self) -> usize {
        self.cells.heap_bytes()
    }
}

impl<C> RowContainer for CoverageIterCells<C>
where
    C: CoverageCell + for<'py> IntoPyObjectExt<'py>,
{
    const LABEL: &'static str = "coverage";

    fn row_count(&self) -> usize {
        self.cells.len()
    }

    fn scalar_row(&self, py: Python<'_>, row: usize) -> PyResult<Py<PyAny>> {
        self.cells
            .get(row)
            .expect("iterator row is in bounds")
            .into_py_any(py)
    }
}

/// One exact membership answer after the grid's canonical geographic mapping.
///
/// Construction and membership must consume the identical source
/// representation.  In particular, the accepted one-ULP latitude sliver at a
/// physical pole has no distinct location: leaving a query raw here would let
/// an otherwise exact coverage reject the very source that created it.
pub(crate) fn coverage_member(
    geometry: &PyGeometry,
    candidate: &Shape,
    predicate: CoveragePredicate,
) -> PyResult<bool> {
    let candidate = normalize_grid_source(candidate);
    if let Shape::Point(point) = &candidate {
        return coverage_member_point(geometry, *point, predicate);
    }
    Ok(predicate.test_data(geometry, &geometry.shape, &ShapeData::from(candidate)))
}

/// One exact membership answer for a lon/lat point.
pub(crate) fn coverage_member_point(
    geometry: &PyGeometry,
    point: Point,
    predicate: CoveragePredicate,
) -> PyResult<bool> {
    crate::boundary::geographic::validate_lonlat_point(point)?;
    let Shape::Point(point) = normalize_grid_source(&Shape::Point(point)) else {
        unreachable!("normalizing a point preserves its geometry kind")
    };
    Ok(predicate.test_point(geometry, &geometry.shape, point))
}

pub(crate) trait HierarchicalCoverageOps: Clone + Sized {
    type Cell: CoverageCell;

    fn coverage_cells(&self) -> &CoverageCells<Self::Cell>;
    fn cell_level(cell: &Self::Cell) -> u8;
    fn parse_floor(value: i64) -> PyResult<u8>;
    fn parse_target_depth(value: &Bound<'_, PyAny>) -> PyResult<u8>;
    fn compact_cells(cells: Vec<Self::Cell>, floor: u8) -> PyResult<Vec<Self::Cell>>;
    fn uncompact_cells(cells: Vec<Self::Cell>, depth: u8) -> PyResult<Vec<Self::Cell>>;
    fn parent_cell(cell: &Self::Cell, depth: u8) -> PyResult<Option<Self::Cell>>;
    fn uncompact_floor_error(cell: &Self::Cell) -> PyErr;
    fn with_compacted_cells(&self, cells: Vec<Self::Cell>) -> Self;
    fn with_uncompacted_cells(&self, cells: Vec<Self::Cell>, depth: u8) -> Self;
    fn with_parent_cells(&self, cells: Vec<Self::Cell>, floor: u8) -> Self;
}

/// Strip pure `with_parents` ancestors that already have a descendant present,
/// yielding the hierarchical frontier that covers the same leaf region.
///
/// Compact/uncompact must operate on this frontier so decorative ancestors
/// cannot expand into unrelated sibling branches or absorb into a coarser
/// pure-ancestor covering of a larger area (N7).
fn hierarchical_coverage_frontier<C>(cells: &CoverageCells<C::Cell>) -> PyResult<Vec<C::Cell>>
where
    C: HierarchicalCoverageOps,
{
    use std::collections::HashSet;

    let present: HashSet<u64> = cells.iter().map(CoverageCell::coverage_id).collect();
    let mut dominated = HashSet::new();
    for cell in cells.iter() {
        let mut current = cell;
        let mut level = C::cell_level(&current);
        while level > 0 {
            let parent_depth = level - 1;
            let Some(parent) = C::parent_cell(&current, parent_depth)? else {
                break;
            };
            let parent_id = parent.coverage_id();
            if present.contains(&parent_id) {
                dominated.insert(parent_id);
            }
            current = parent;
            level = parent_depth;
        }
    }
    Ok(cells
        .iter()
        .filter(|cell| !dominated.contains(&cell.coverage_id()))
        .collect())
}

pub(crate) fn hierarchical_coverage_compact<C>(coverage: &C, floor: i64) -> PyResult<C>
where
    C: HierarchicalCoverageOps,
{
    let floor = C::parse_floor(floor)?;
    // Decorative with_parents ancestors must not participate in compact.
    let frontier = hierarchical_coverage_frontier::<C>(coverage.coverage_cells())?;
    let cells = C::compact_cells(frontier, floor)?;
    Ok(coverage.with_compacted_cells(cells))
}

pub(crate) fn hierarchical_coverage_uncompact<C>(
    coverage: &C,
    depth: &Bound<'_, PyAny>,
) -> PyResult<C>
where
    C: HierarchicalCoverageOps,
{
    let depth = C::parse_target_depth(depth)?;
    if let Some(cell) = coverage
        .coverage_cells()
        .iter()
        .find(|cell| C::cell_level(cell) > depth)
    {
        return Err(C::uncompact_floor_error(&cell));
    }
    // Expand only the leaf frontier — uncompacting decorative parents would
    // invent sibling branches outside the factory covering.
    let frontier = hierarchical_coverage_frontier::<C>(coverage.coverage_cells())?;
    let cells = C::uncompact_cells(frontier, depth)?;
    Ok(coverage.with_uncompacted_cells(cells, depth))
}

pub(crate) fn hierarchical_coverage_with_parents<C>(coverage: &C, floor: i64) -> PyResult<C>
where
    C: HierarchicalCoverageOps,
{
    let floor = C::parse_floor(floor)?;
    // Explicit user transform — no cell budget re-cap.
    let mut cells = Vec::with_capacity(coverage.coverage_cells().len());
    for cell in coverage.coverage_cells().iter() {
        cells.push(cell);
        for depth in floor..C::cell_level(&cell) {
            if let Some(parent) = C::parent_cell(&cell, depth)? {
                cells.push(parent);
            }
        }
    }
    Ok(coverage.with_parent_cells(cells, floor))
}

pub(crate) trait GridDissolver {
    type Cell: Copy + Ord;

    fn fast_path_cells(cells: &[Self::Cell]) -> PyResult<Option<Vec<Self::Cell>>>;
    /// Cheap neighbor ids for adjacency pre-scan (no edge geometry).
    fn adjacency_neighbors(cell: Self::Cell) -> impl Iterator<Item = Self::Cell>;
    /// Exterior edge segments only (`neighbor` not in the prepared set).
    fn exterior_edge_segments(
        cell: Self::Cell,
        is_member: &dyn Fn(Self::Cell) -> bool,
    ) -> Vec<CoordSeq>;
    fn crosses_seam(segment: &CoordSeq) -> bool;
    fn fallback_shape(cell: Self::Cell) -> crate::error::Result<Shape>;
}

pub(crate) fn dissolve_grid_cells<D>(cells: &[D::Cell]) -> PyResult<Typed>
where
    D: GridDissolver,
{
    let fallback_cells = if let Some(prepared) = D::fast_path_cells(cells)? {
        // Pre-scan adjacency without materializing edge geometry. Zero shared
        // edges → MultiPolygon of per-cell polygons (no polygonize).
        let any_adjacent = prepared.iter().any(|&cell| {
            D::adjacency_neighbors(cell).any(|neighbor| prepared.binary_search(&neighbor).is_ok())
        });
        if !any_adjacent {
            // No shared edges: assemble a MultiPolygon of the per-cell
            // polygons directly — never pay union/polygonize.
            return multipolygon_from_cell_shapes(
                prepared
                    .into_iter()
                    .map(D::fallback_shape)
                    .collect::<crate::error::Result<Vec<_>>>()?,
            );
        }
        let is_member = |neighbor: D::Cell| prepared.binary_search(&neighbor).is_ok();
        let mut outline: Vec<CoordSeq> = Vec::with_capacity(prepared.len() * 2);
        let mut crosses_seam = false;
        'scan: for &cell in &prepared {
            for segment in D::exterior_edge_segments(cell, &is_member) {
                if D::crosses_seam(&segment) {
                    crosses_seam = true;
                    break 'scan;
                }
                outline.push(segment);
            }
        }
        if !crosses_seam && let Some(typed) = polygonize_outline(outline)? {
            return Ok(typed);
        }
        prepared
    } else {
        cells.to_vec()
    };
    let shapes: Vec<Shape> = fallback_cells
        .into_iter()
        .map(D::fallback_shape)
        .collect::<crate::error::Result<_>>()?;
    coverage_to_polygon(&shapes)
}

/// Assemble one MultiPolygon (or single Polygon) from already-disjoint cell
/// shapes — used when the dissolve pre-scan finds zero shared edges.
fn multipolygon_from_cell_shapes(shapes: Vec<Shape>) -> PyResult<Typed> {
    if shapes.is_empty() {
        return Err(GeometryError::new_err(
            "a coverage needs at least one cell to dissolve into a polygon",
        ));
    }
    let mut polygons = Vec::with_capacity(shapes.len());
    for shape in shapes {
        match shape {
            Shape::Polygon(polygon) => polygons.push(polygon),
            Shape::MultiPolygon(parts) => polygons.extend(parts),
            Shape::GeometryCollection(parts) => {
                for part in parts {
                    match part {
                        Shape::Polygon(polygon) => polygons.push(polygon),
                        Shape::MultiPolygon(multi) => polygons.extend(multi),
                        _ => {},
                    }
                }
            },
            _ => {},
        }
    }
    if polygons.is_empty() {
        return Err(GeometryError::new_err(
            "a coverage needs at least one cell to dissolve into a polygon",
        ));
    }
    let shape = if polygons.len() == 1 {
        Shape::Polygon(polygons.into_iter().next().expect("one polygon"))
    } else {
        Shape::MultiPolygon(polygons)
    };
    Ok(PyGeometry::typed_wgs84(shape))
}

fn polygonize_outline(outline: Vec<CoordSeq>) -> PyResult<Option<Typed>> {
    let polygons: Vec<Polygon> = Shape::build_area_all(
        &[&Shape::MultiLineString(
            outline.into_iter().map(LineSeq::from_trusted).collect(),
        )],
        true,
    )?
    .into_iter()
    .filter_map(|shape| match shape {
        Shape::Polygon(polygon) => Some(polygon),
        _ => None,
    })
    .collect();
    if polygons.is_empty() {
        return Ok(None);
    }
    let shape = if polygons.len() == 1 {
        Shape::Polygon(polygons.into_iter().next().expect("one polygon"))
    } else {
        Shape::MultiPolygon(polygons)
    };
    Ok(Some(PyGeometry::typed_wgs84(shape)))
}

pub(crate) fn coordseq_crosses_lon_seam(segment: &CoordSeq) -> bool {
    let (mut lo, mut hi) = (f64::INFINITY, f64::NEG_INFINITY);
    for point in segment.points() {
        lo = lo.min(point.x);
        hi = hi.max(point.x);
    }
    hi - lo > 180.0
}

pub(crate) fn rect_cell_polygon<C>(cell: C) -> PyGeometry
where
    C: RectGridCell + GridCell,
{
    PyGeometry::wgs84(cell.boundary_shape())
}

/// Broadcast raw lon/lat columns through the same prepared point-coordinate
/// kernel used by ``contains_xy`` / ``intersects_xy``.
pub(crate) fn coverage_members_xy(
    py: Python<'_>,
    geometry: &PyGeometry,
    x: &Bound<'_, PyAny>,
    y: &Bound<'_, PyAny>,
    predicate: CoveragePredicate,
) -> PyResult<Py<PyAny>> {
    let mut x = crate::coordinate_input(py, x, "x")?;
    let mut y = crate::coordinate_input(py, y, "y")?;
    let scalar = x.scalar && y.scalar;
    crate::broadcast_coordinate_group([(&mut x, "x"), (&mut y, "y")], "x and y")?;
    for (&x, &y) in x.values.iter().zip(&y.values) {
        crate::boundary::geographic::validate_lonlat_xy(x, y)?;
    }
    crate::broadcast::xy_predicate_values(
        py,
        geometry,
        x.values,
        y.values,
        scalar,
        predicate.includes_boundary_for_points(),
    )
}

/// Static knobs for one rectangular grid coverage backend.
pub(crate) trait RectCoverSpec {
    type Cell: RectGridCell + crate::grid::cell_set::HierarchicalId;

    const KIND: GridKind;

    fn roots() -> Vec<Self::Cell>;
    fn level_of(cell: &Self::Cell) -> u8;
    fn parse_depth(value: &Bound<'_, PyAny>) -> PyResult<u8>;
    fn coverage_label() -> &'static str;
}

/// Python cell wrapper that carries one rectangular-grid kernel cell.
pub(crate) trait RectCoverageCell: CoverageCell {
    type Cell: RectGridCell + crate::grid::cell_set::HierarchicalId;

    fn from_rect_cell(cell: Self::Cell) -> Self;
    fn level(self) -> u8;
}

/// Rule-independent overlap partition for rectangular-grid inspection
/// (`interior_cells` / `boundary_cells` / `explain`), keyed by source +
/// factory depth + `max_cells`. Built lazily: visible-cell selection never
/// forces it for non-overlap rules (same contract as H3/S2).
#[derive(Debug)]
pub(crate) struct RectMembership<W: RectCoverageCell> {
    partition: OnceLock<Result<CoveragePartition<W>, crate::grid::CoverBudgetExceeded>>,
    /// Split-normalized working shape for delayed overlap recompute.
    cover_shape: Shape,
    /// True when antimeridian split created storage distinct from the
    /// membership geometry (only then is `cover_shape` counted in heap).
    cover_is_split: bool,
    factory_depth: u8,
    marker: PhantomData<W>,
}

impl<W: RectCoverageCell> RectMembership<W> {
    pub(crate) fn lazy(cover_shape: Shape, cover_is_split: bool, factory_depth: u8) -> Arc<Self> {
        Arc::new(Self {
            partition: OnceLock::new(),
            cover_shape,
            cover_is_split,
            factory_depth,
            marker: PhantomData,
        })
    }

    pub(crate) fn seeded(
        partition: CoveragePartition<W>,
        cover_shape: Shape,
        cover_is_split: bool,
        factory_depth: u8,
    ) -> Arc<Self> {
        let lock = OnceLock::new();
        let _ = lock.set(Ok(partition));
        Arc::new(Self {
            partition: lock,
            cover_shape,
            cover_is_split,
            factory_depth,
            marker: PhantomData,
        })
    }

    pub(crate) const fn partition_slot(
        &self,
    ) -> &OnceLock<Result<CoveragePartition<W>, crate::grid::CoverBudgetExceeded>> {
        &self.partition
    }

    pub(crate) const fn cover_shape(&self) -> &Shape {
        &self.cover_shape
    }

    pub(crate) const fn factory_depth(&self) -> u8 {
        self.factory_depth
    }
}

impl<W: RectCoverageCell> HeapSize for RectMembership<W> {
    fn heap_bytes(&self) -> usize {
        let cover_bytes = if self.cover_is_split {
            self.cover_shape.coordinate_bytes()
        } else {
            0
        };
        cover_bytes
            + match self.partition.get() {
                Some(Ok(partition)) => partition.heap_bytes(),
                Some(Err(_)) | None => 0,
            }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RectCoverageState<W: RectCoverageCell> {
    pub(crate) geometry: PyGeometry,
    pub(crate) cells: CoverageCells<W>,
    pub(crate) membership: Arc<RectMembership<W>>,
    pub(crate) cell_rule: CellRule,
    pub(crate) depth: CellDepth,
    /// Factory `max_cells` budget (serialized for pickle — D07).
    /// `None` = unlimited (adult factory choice; recompute stays unbounded).
    pub(crate) max_cells: Option<usize>,
}

impl<W: RectCoverageCell> RectCoverageState<W> {
    pub(crate) fn retained_heap_bytes(&self) -> usize {
        let geometry_bytes = self.geometry.shape.shape().coordinate_bytes();
        let membership_bytes = self.membership.heap_bytes();
        match self.membership.partition_slot().get() {
            Some(Ok(partition)) => {
                geometry_bytes + membership_bytes + self.cells.additional_heap_bytes(partition)
            },
            Some(Err(_)) | None => geometry_bytes + membership_bytes + self.cells.heap_bytes(),
        }
    }

    /// Resolve the overlap inspection partition, computing it once on first use.
    /// May raise when the delayed overlap pass exceeds the factory `max_cells`.
    pub(crate) fn partition<S: RectCoverSpec<Cell = W::Cell>>(
        &self,
    ) -> PyResult<&CoveragePartition<W>> {
        let max_cells = self.max_cells;
        let factory_depth = self.membership.factory_depth();
        let cover_shape = self.membership.cover_shape();
        let ready = self.membership.partition_slot().get_or_init(|| {
            let covering = coverer::cover(cover_shape, S::roots(), factory_depth, max_cells)?;
            Ok(CoveragePartition::from_sorted_tagged(
                covering
                    .into_iter()
                    .map(|(cell, interior)| (W::from_rect_cell(cell), interior)),
            ))
        });
        match ready {
            Ok(partition) => Ok(partition),
            Err(err) => Err(cover_budget_err(*err)),
        }
    }

    /// Re-represent the visible cells without touching the exact membership
    /// (which answers against the source geometry, not the cell set).
    pub(crate) fn with_cells(&self, cells: Vec<W>) -> Self {
        self.with_cells_depth(cells, self.depth)
    }

    pub(crate) fn with_cells_depth(&self, cells: Vec<W>, fallback: CellDepth) -> Self {
        let depth =
            CellDepth::from_levels(cells.iter().map(|cell| cell.level())).unwrap_or(fallback);
        Self {
            geometry: self.geometry.clone(),
            cells: CoverageCells::from_cells(cells),
            membership: Arc::clone(&self.membership),
            cell_rule: self.cell_rule,
            depth,
            max_cells: self.max_cells,
        }
    }
}

/// Wrap rect kernel cells; `CoverageCells::from_cells` canonicalizes order.
pub(crate) fn rect_coverage_cells<W: RectCoverageCell>(cells: Vec<W::Cell>) -> Vec<W> {
    cells.into_iter().map(W::from_rect_cell).collect()
}

/// Build the shared geohash/tile coverage core.
pub(crate) fn empty_coverage_err(label: &str) -> PyErr {
    InvalidGeometryError::new_err(format!("{label} coverage requires a non-empty geometry"))
}

/// Convert a covering cell-budget overflow into the Python `GeometryError`
/// (the shared boundary mapping for every grid's `cover(...)` factory).
pub(crate) fn cover_budget_err(err: crate::grid::CoverBudgetExceeded) -> PyErr {
    integer_parameter_error(err.to_string(), "max_cells", err.limit as i64)
}

/// Parse the shared cover-factory `max_cells` parameter.
///
/// - omitted default is applied by the pyfunction signature (`1_000_000`)
/// - `None` (Python `None`) → unlimited
/// - positive int → that budget
/// - `<= 0` → typed value error naming `max_cells`
pub(crate) fn parse_max_cells(max_cells: Option<i64>) -> PyResult<Option<usize>> {
    match max_cells {
        None => Ok(None),
        Some(n) if n <= 0 => Err(integer_parameter_error(
            format!("max_cells must be greater than zero, got {n}"),
            "max_cells",
            n,
        )),
        Some(n) => usize::try_from(n)
            .map(Some)
            .map_err(|_| integer_parameter_error("max_cells is too large", "max_cells", n)),
    }
}

pub(crate) fn build_rect_coverage_state<S, W>(
    geometry: &PyGeometry,
    depth: &Bound<'_, PyAny>,
    cell_rule: CellRule,
    max_cells: Option<usize>,
) -> PyResult<RectCoverageState<W>>
where
    S: RectCoverSpec<Cell = W::Cell>,
    W: RectCoverageCell,
{
    let depth = S::parse_depth(depth)?;
    let (membership_geometry, cover_shape, cover_is_split) =
        coverage_factory_shapes(geometry, S::coverage_label())?;
    let (membership, cells) = match cell_rule {
        // Overlap/bbox already produce the full tagged product — seed so
        // interior/boundary do not re-cover.
        CellRule::Overlap | CellRule::Bbox => {
            let covering = coverer::cover(&cover_shape, S::roots(), depth, max_cells)
                .map_err(cover_budget_err)?;
            let partition = CoveragePartition::from_sorted_tagged(
                covering
                    .into_iter()
                    .map(|(cell, interior)| (W::from_rect_cell(cell), interior)),
            );
            let cells = partition.all();
            (
                RectMembership::seeded(partition, cover_shape, cover_is_split, depth),
                cells,
            )
        },
        CellRule::Within => {
            let covering = coverer::cover(&cover_shape, S::roots(), depth, max_cells)
                .map_err(cover_budget_err)?;
            let cells = CoverageCells::from_cells(
                covering
                    .into_iter()
                    .filter(|&(_, interior)| interior)
                    .map(|(cell, _)| W::from_rect_cell(cell))
                    .collect(),
            );
            (
                RectMembership::lazy(cover_shape, cover_is_split, depth),
                cells,
            )
        },
        // Native center descent: budget counts only visible center cells;
        // inspection partition stays cold until first interior/boundary use.
        CellRule::Center => {
            let centers = coverer::cover_center(&cover_shape, S::roots(), depth, max_cells)
                .map_err(cover_budget_err)?;
            let cells = CoverageCells::from_cells(rect_coverage_cells::<W>(centers));
            (
                RectMembership::lazy(cover_shape, cover_is_split, depth),
                cells,
            )
        },
    };
    let depth = CellDepth::from_levels(cells.iter().map(RectCoverageCell::level))
        .unwrap_or(CellDepth::Uniform(depth));
    Ok(RectCoverageState {
        geometry: membership_geometry,
        cells,
        membership,
        cell_rule,
        depth,
        max_cells,
    })
}

/// Strip pure ancestors that already have a descendant in `cells`, yielding the
/// hierarchical frontier (so `with_parents` reduces to the leaf covering).
pub(crate) fn coverage_frontier<G: crate::grid::cell_set::HierarchicalId>(cells: &[G]) -> Vec<G> {
    use std::collections::HashSet;

    // Hierarchical range keys uniquely identify a cell in each grid.
    let keys: HashSet<(u64, u64)> = cells
        .iter()
        .map(|cell| (cell.range_min(), cell.range_max()))
        .collect();
    let mut dominated = HashSet::new();
    for &cell in cells {
        let mut current = cell;
        while let Some(parent) = current.parent() {
            let key = (parent.range_min(), parent.range_max());
            if keys.contains(&key) {
                dominated.insert(key);
            }
            current = parent;
        }
    }
    cells
        .iter()
        .copied()
        .filter(|cell| !dominated.contains(&(cell.range_min(), cell.range_max())))
        .collect()
}

/// Collect a coverage pickle cell/id sequence as an **exact built-in list** of
/// exact primitive elements (D09).
///
/// Rejects iterators, list subclasses, and non-list sequences so an infinite
/// iterable never hangs unpickle.
pub(crate) fn collect_coverage_sequence<T>(value: &Bound<'_, PyAny>, what: &str) -> PyResult<Vec<T>>
where
    for<'a, 'py> T: FromPyObject<'a, 'py>,
{
    let list = value
        .cast_exact::<PyList>()
        .map_err(|_| PyTypeError::new_err(format!("{what} must be a list")))?;
    let mut out = crate::try_vec_with_capacity_hint(list.len())?;
    for item in list.iter() {
        let parsed: T = item
            .extract()
            .map_err(|_| PyTypeError::new_err(format!("{what} elements have the wrong type")))?;
        crate::try_push(&mut out, parsed)?;
    }
    Ok(out)
}

/// Lon/lat working shape for the coverer: same geographic antimeridian
/// split-normalization topology uses. Membership storage keeps the unsplit
/// lon/lat form (pole probes need the original container; non-point membership
/// re-normalizes through [`topology_scalar_pair`]).
///
/// Returns `(working_shape, is_split)` — `is_split` is true only when
/// antimeridian normalization allocated distinct storage from `lonlat`.
pub(crate) fn cover_working_shape(
    frame: &crate::boundary::metadata::Frame,
    lonlat: &Shape,
) -> PyResult<(Shape, bool)> {
    if is_geographic_frame(frame) && lonlat.crosses_antimeridian() {
        Ok((lonlat.split_antimeridian()?, true))
    } else {
        Ok((lonlat.clone(), false))
    }
}

/// Factory source retained by every coverage: canonical WGS84 lon/lat
/// membership geometry.  New certified coverers consume this exact source
/// directly; only the historical planar tilers need a split working image.
pub(crate) fn coverage_factory_geometry(
    geometry: &PyGeometry,
    label: &str,
) -> PyResult<PyGeometry> {
    let shape = lonlat_shape(geometry)?;
    validate_lonlat_shape(&shape)?;
    // This is the sole public grid-source normalization point.  Every grid
    // lane receives the same physical-pole spelling before it can derive a
    // working image, a rectangle certificate, or aggregate components.
    let shape = normalize_grid_source(&shape);
    if shape.is_empty() {
        return Err(empty_coverage_err(label));
    }
    Ok(PyGeometry::wgs84(shape))
}

/// Factory source for the historical planar cover lanes: retained WGS84
/// membership geometry (unsplit) plus their split-normalized working shape.
///
/// Returns `(membership_geometry, cover_shape, cover_is_split)`.
pub(crate) fn coverage_factory_shapes(
    geometry: &PyGeometry,
    label: &str,
) -> PyResult<(PyGeometry, Shape, bool)> {
    let membership_geometry = coverage_factory_geometry(geometry, label)?;
    let shape = membership_geometry.shape.as_ref();
    let (cover_shape, cover_is_split) = cover_working_shape(&geometry.frame, shape)?;
    Ok((membership_geometry, cover_shape, cover_is_split))
}

/// Normalize a coverage source through the same path as the public factories.
///
/// Returns `(membership_geometry, cover_shape, cover_is_split)`.
pub(crate) fn normalize_coverage_source(
    geometry: &Bound<'_, PyAny>,
    label: &str,
) -> PyResult<(PyGeometry, Shape, bool)> {
    let geometry = exact_geometry(geometry)
        .ok_or_else(expected_geometry_or_array)?
        .clone();
    coverage_factory_shapes(&geometry, label)
}

/// Rebuild a rectangular-grid coverage from a pickle payload.
///
/// - Source is normalized through the factory lon/lat path.
/// - `factory_depth` is the original cover depth (lazy partition key).
/// - `visible_depth` restores empty post-transform depth (uncompact etc.).
/// - `max_cells` is the factory budget applied when inspection first materializes
///   the partition (D07). `None` = unlimited.
/// - Partition stays **cold** (same contract as H3 unpickle).
/// - Visible `cells` are an exact list of primitive ids/tokens (D09).
pub(crate) fn unpickle_rect_coverage_state<S, W, T, D, P>(
    geometry: &Bound<'_, PyAny>,
    cells: &Bound<'_, PyAny>,
    cell_rule: &str,
    factory_depth: u8,
    visible_depth: Option<u8>,
    max_cells: Option<i64>,
    parse_depth_u8: P,
    decode: D,
) -> PyResult<RectCoverageState<W>>
where
    S: RectCoverSpec<Cell = W::Cell>,
    W: RectCoverageCell,
    D: Fn(Vec<T>) -> PyResult<Vec<W::Cell>>,
    P: Fn(u8) -> PyResult<u8>,
    T: for<'a, 'py> pyo3::FromPyObject<'a, 'py> + Eq + std::hash::Hash + Clone,
{
    let label = S::coverage_label();
    let (geometry, cover_shape, cover_is_split) = normalize_coverage_source(geometry, label)?;
    let factory_depth = parse_depth_u8(factory_depth)?;
    if let Some(visible) = visible_depth {
        parse_depth_u8(visible)?;
    }
    let max_cells = parse_max_cells(max_cells)?;
    let cell_rule = CellRule::parse(cell_rule)
        .map_err(|message| crate::py::errors::parameter_error(message, "cell_rule"))?;
    let cells_raw: Vec<T> = collect_coverage_sequence(cells, &format!("{label} coverage cells"))?;
    let cells = CoverageCells::from_cells(rect_coverage_cells::<W>(decode(cells_raw)?));
    // Lazy membership: no overlap recompute on unpickle (D07 budget applies
    // when inspection first materializes the partition).
    let membership = RectMembership::lazy(cover_shape, cover_is_split, factory_depth);
    let depth = CellDepth::from_levels(cells.iter().map(RectCoverageCell::level))
        .or_else(|| visible_depth.map(CellDepth::Uniform))
        .unwrap_or(CellDepth::Uniform(factory_depth));
    Ok(RectCoverageState {
        geometry,
        cells,
        membership,
        cell_rule,
        depth,
        max_cells,
    })
}

pub(crate) fn rect_cell_array_for<S: RectCoverSpec>(
    cells: impl IntoIterator<Item = S::Cell>,
) -> PyCellArray {
    cell_array_from_keys(S::KIND, cells)
}

#[cfg(test)]
mod invariant_tests {
    use super::*;

    #[derive(Clone, Copy, Debug)]
    struct TestCell(u64);

    impl HeapSize for TestCell {
        fn heap_bytes(&self) -> usize {
            0
        }
    }

    impl CoverageCell for TestCell {
        fn from_coverage_id(id: u64) -> Self {
            Self(id)
        }

        fn coverage_id(self) -> u64 {
            self.0
        }
    }

    #[test]
    fn coverage_cells_canonicalize_without_changing_cell_array_contract() {
        let cells = CoverageCells::from_cells(vec![TestCell(4), TestCell(2), TestCell(4)]);
        assert_eq!(cells.iter().map(|cell| cell.0).collect::<Vec<_>>(), [2, 4]);
        assert!(cells.contains_id(2));
        assert_eq!(cells.logical_index(4), Some(1));
    }

    #[test]
    fn partition_merges_adjacent_duplicate_tags_on_sorted_stream() {
        // Caller promises a sorted stream; adjacent duplicates merge with
        // interior winning.
        let partition = CoveragePartition::from_sorted_tagged([
            (TestCell(2), true),
            (TestCell(4), false),
            (TestCell(4), true),
        ]);
        assert_eq!(
            partition
                .all()
                .iter()
                .map(|cell| cell.0)
                .collect::<Vec<_>>(),
            [2, 4]
        );
        assert_eq!(
            partition
                .interior()
                .iter()
                .map(|cell| cell.0)
                .collect::<Vec<_>>(),
            [2, 4]
        );
        assert!(partition.boundary().is_empty());
    }

    // Guards a `debug_assert!`, so it only exists where `debug-assertions` is
    // on -- otherwise it cannot pass under `cargo nextest run --release`.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "coverage selection must be sorted and unique")]
    fn checked_selection_rejects_unsorted_rows() {
        let _ = CheckedSelection::new(vec![1, 0], 2);
    }

    #[test]
    fn coverage_frontier_strips_pure_ancestors() {
        use crate::grid::cell::GridCell;
        use crate::grid::tile::Tile;
        let local = Tile::from_lonlat(0.05, 0.05, 12).expect("in domain");
        let parent = GridCell::parent_at(local, 6).expect("parent");
        assert_eq!(coverage_frontier(&[local, parent]), [local]);
    }
}
