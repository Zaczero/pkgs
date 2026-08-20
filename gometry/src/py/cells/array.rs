#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Typed bulk cell storage — the CellArray Python surface.
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use std::collections::hash_map::Entry;
use std::ops::Range;
use std::sync::Arc;

use numpy::PyArrayMethods as _;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::{PyResult, Python, pyclass};
use pyo3::types::{PyAny, PyBytes, PyBytesMethods as _, PyTuple, PyType};
use pyo3::{Bound, Py};

use crate::HeapSize;
use crate::array::{RowSelection, RowSelectionRef, physical_row, row_selection_from_logical_rows};
use crate::broadcast::py_bool_or_not_implemented;
use crate::collections::{HashMap, HashMapExt as _};
use crate::geometry::CoordSeq;
use crate::grid::cell::GridCell;
use crate::py::cells::grid_kind::{collect_ids, collect_inferred_ids, grid_kind_from_type};
use crate::py::cells::{
    GeometryError, GridKind, H3_MAX_RESOLUTION, IntoPyObject as _, PyAnyMethods as _,
    PyCellArrayIter, PyRef, checked_depth, py_i64_required, pyfunction, rect_cells_to_polygon,
    uncompact_floor_error,
};
use crate::py::row::{RowContainer, RowGetItemContainer, array_getitem};

/// An immutable array of one grid cell type backed by a shared ``uint64`` id
/// column.
///
/// Index with an integer for the typed cell object; slice or mask for a new
/// `CellArray`. Build from a non-empty homogeneous iterable of typed cell
/// objects. For raw ids, tokens, arrays, buffers, or empty inputs, pass
/// ``type=`` explicitly; every id is validated for that grid.
///
/// Parameters
/// ----------
/// values : numpy.ndarray or iterable
///     Typed cell objects, or raw ids/tokens when ``type`` is supplied.
/// type : type, optional
///     Native cell class. Inferred only from non-empty homogeneous typed cells.
#[pyclass(
    name = "CellArray",
    module = "gometry",
    frozen,
    immutable_type,
    sequence,
    generic,
    weakref,
    skip_from_py_object
)]
#[derive(Clone, Debug)]
pub(crate) struct PyCellArray {
    storage: CellStorage,
    selection: RowSelection,
}

#[derive(Clone, Debug)]
struct CellStorage {
    kind: GridKind,
    ids: Arc<[u64]>,
    /// Per-physical-row missing flags. `None` means every row is present
    /// (the common dense factory path). When `Some`, length equals `ids`.
    missing: Option<Arc<[bool]>>,
}

impl CellStorage {
    fn from_trusted_ids(kind: GridKind, ids: Vec<u64>) -> Self {
        Self::from_shared_ids(kind, Arc::from(ids))
    }

    fn from_trusted_ids_with_missing(kind: GridKind, ids: Vec<u64>, missing: Vec<bool>) -> Self {
        debug_assert_eq!(ids.len(), missing.len());
        Self {
            kind,
            ids: Arc::from(ids),
            missing: Some(Arc::from(missing)),
        }
    }

    const fn from_shared_ids(kind: GridKind, ids: Arc<[u64]>) -> Self {
        Self {
            kind,
            ids,
            missing: None,
        }
    }

    const fn kind(&self) -> GridKind {
        self.kind
    }

    const fn ids(&self) -> &Arc<[u64]> {
        &self.ids
    }
}

trait CellArrayGrid: GridCell {
    fn from_validated_id(id: u64) -> Self;
    fn parse_depth(value: &Bound<'_, PyAny>) -> PyResult<u8>;
    fn parse_compact_floor(depth: Option<&Bound<'_, PyAny>>) -> PyResult<u8>;
    fn compact_cells(cells: Vec<Self>, floor: u8) -> PyResult<Vec<Self>>;
    fn uncompact_cells(cells: Vec<Self>, depth: u8) -> PyResult<Vec<Self>>;
    fn uncompact_floor_token(cell: &Self) -> String;
}

impl CellArrayGrid for h3o::CellIndex {
    fn from_validated_id(id: u64) -> Self {
        Self::try_from(id).expect("ids validated at construction")
    }

    fn parse_depth(value: &Bound<'_, PyAny>) -> PyResult<u8> {
        super::h3::parse_h3_resolution(value).map(Into::into)
    }

    fn parse_compact_floor(depth: Option<&Bound<'_, PyAny>>) -> PyResult<u8> {
        let value = match depth {
            Some(value) if !value.is_none() => py_i64_required("H3 min_resolution", value)?,
            _ => 0,
        };
        checked_depth(
            value,
            "H3 min_resolution",
            "min_resolution",
            0,
            i64::from(H3_MAX_RESOLUTION),
        )
    }

    fn compact_cells(cells: Vec<Self>, floor: u8) -> PyResult<Vec<Self>> {
        super::h3::compact_cells(cells, super::h3::resolution_from_depth(floor)?)
    }

    fn uncompact_cells(cells: Vec<Self>, depth: u8) -> PyResult<Vec<Self>> {
        Ok(super::h3::uncompact_cells_unlimited(
            cells,
            super::h3::resolution_from_depth(depth)?,
        ))
    }

    fn uncompact_floor_token(cell: &Self) -> String {
        cell.to_string()
    }
}

impl CellArrayGrid for crate::grid::s2::cellid::CellId {
    fn from_validated_id(id: u64) -> Self {
        Self::from_raw(id).expect("ids validated at construction")
    }

    fn parse_depth(value: &Bound<'_, PyAny>) -> PyResult<u8> {
        super::s2::parse_s2_level(value)
    }

    fn parse_compact_floor(depth: Option<&Bound<'_, PyAny>>) -> PyResult<u8> {
        let value = match depth {
            Some(value) if !value.is_none() => py_i64_required("S2 min_level", value)?,
            _ => 0,
        };
        super::s2::parse_s2_min_level_value(value)
    }

    fn compact_cells(cells: Vec<Self>, floor: u8) -> PyResult<Vec<Self>> {
        Ok(crate::grid::s2::cell_set::compact_with_floor(cells, floor))
    }

    fn uncompact_cells(cells: Vec<Self>, depth: u8) -> PyResult<Vec<Self>> {
        Ok(crate::grid::s2::cell_set::uncompact_unlimited(
            &crate::grid::s2::cell_set::normalize(cells),
            depth,
        ))
    }

    fn uncompact_floor_token(cell: &Self) -> String {
        cell.token()
    }
}

impl CellArrayGrid for crate::grid::tile::Tile {
    fn from_validated_id(id: u64) -> Self {
        Self::from_id(id).expect("ids validated at construction")
    }

    fn parse_depth(value: &Bound<'_, PyAny>) -> PyResult<u8> {
        super::tiles::parse_tile_zoom(value)
    }

    fn parse_compact_floor(depth: Option<&Bound<'_, PyAny>>) -> PyResult<u8> {
        let value = match depth {
            Some(value) if !value.is_none() => py_i64_required("tile min_zoom", value)?,
            _ => 0,
        };
        checked_depth(
            value,
            "tile min_zoom",
            "min_zoom",
            0,
            i64::from(crate::grid::tile::TILE_MAX_ZOOM),
        )
    }

    fn compact_cells(cells: Vec<Self>, floor: u8) -> PyResult<Vec<Self>> {
        Ok(crate::grid::tile::compact_with_floor(cells, floor))
    }

    fn uncompact_cells(cells: Vec<Self>, depth: u8) -> PyResult<Vec<Self>> {
        Ok(crate::grid::tile::uncompact_unlimited(&cells, depth))
    }

    fn uncompact_floor_token(cell: &Self) -> String {
        cell.quadkey()
    }
}

impl CellArrayGrid for crate::grid::geohash::Geohash {
    fn from_validated_id(id: u64) -> Self {
        super::grid_kind::geohash_from_identity_key(id).expect("ids validated at construction")
    }

    fn parse_depth(value: &Bound<'_, PyAny>) -> PyResult<u8> {
        super::geohash::parse_geohash_precision(value)
    }

    fn parse_compact_floor(depth: Option<&Bound<'_, PyAny>>) -> PyResult<u8> {
        let value = match depth {
            Some(value) if !value.is_none() => py_i64_required("geohash min_precision", value)?,
            _ => 1,
        };
        checked_depth(
            value,
            "geohash min_precision",
            "min_precision",
            1,
            i64::from(crate::grid::geohash::GEOHASH_MAX_PRECISION),
        )
    }

    fn compact_cells(cells: Vec<Self>, floor: u8) -> PyResult<Vec<Self>> {
        Ok(crate::grid::geohash::compact_with_floor(cells, floor))
    }

    fn uncompact_cells(cells: Vec<Self>, depth: u8) -> PyResult<Vec<Self>> {
        Ok(crate::grid::geohash::uncompact_unlimited(&cells, depth))
    }

    fn uncompact_floor_token(cell: &Self) -> String {
        cell.token()
    }
}

macro_rules! dispatch_cell_grid {
    ($self:expr, $grid:ident, $body:block) => {{
        match $self.kind() {
            GridKind::H3Cell => {
                type $grid = h3o::CellIndex;
                $body
            },
            GridKind::S2Cell => {
                type $grid = crate::grid::s2::cellid::CellId;
                $body
            },
            GridKind::Tile => {
                type $grid = crate::grid::tile::Tile;
                $body
            },
            GridKind::GeohashCell => {
                type $grid = crate::grid::geohash::Geohash;
                $body
            },
        }
    }};
}

impl PyCellArray {
    pub(crate) fn from_trusted_ids(kind: GridKind, ids: Vec<u64>) -> Self {
        Self {
            storage: CellStorage::from_trusted_ids(kind, ids),
            selection: RowSelection::Identity,
        }
    }

    /// Build with a parallel missing mask (one flag per id). Used by bulk
    /// factories that degrade missing geometry rows instead of rejecting them.
    pub(crate) fn from_trusted_ids_with_missing(
        kind: GridKind,
        ids: Vec<u64>,
        missing: Vec<bool>,
    ) -> Self {
        Self {
            storage: CellStorage::from_trusted_ids_with_missing(kind, ids, missing),
            selection: RowSelection::Identity,
        }
    }

    /// Whether logical row `i` is a missing factory placeholder.
    pub(crate) fn is_row_missing(&self, logical: usize) -> bool {
        let Some(mask) = self.storage.missing.as_ref() else {
            return false;
        };
        mask[physical_row(self.selection_ref(), logical)]
    }

    fn selection_ref(&self) -> RowSelectionRef<'_> {
        self.selection.as_deref()
    }

    pub(crate) fn len(&self) -> usize {
        self.selection_ref().len(self.storage.ids().len())
    }

    /// The grid kind of these cells.
    pub(crate) const fn kind(&self) -> GridKind {
        self.storage.kind()
    }

    /// A contiguous logical sub-range as a new array (zero-copy selection).
    /// Used by [`crate::py::vectors::Groups`] to view one ragged row of cells.
    pub(crate) fn logical_row_range(&self, range: Range<usize>) -> Self {
        self.slice(range.start as isize, range.end as isize, 1)
    }

    /// Materialized logical cell ids (for CSR value equality / pickling).
    pub(crate) fn logical_ids(&self) -> Vec<u64> {
        self.logical_ids_vec()
    }

    /// Value-equal when the grid kind, missing mask, and present cell ids match.
    pub(crate) fn logical_eq(&self, other: &Self) -> bool {
        if self.kind() != other.kind() || self.len() != other.len() {
            return false;
        }
        (0..self.len()).all(|row| {
            let left_missing = self.is_row_missing(row);
            let right_missing = other.is_row_missing(row);
            left_missing == right_missing && (left_missing || self.id_at(row) == other.id_at(row))
        })
    }

    /// The logical cells as boxed Python objects (one per row).
    pub(crate) fn to_cell_list(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        (0..self.len())
            .map(|logical| self.cell_at(py, logical))
            .collect()
    }

    pub(super) fn id_at(&self, logical: usize) -> u64 {
        self.storage.ids()[physical_row(self.selection_ref(), logical)]
    }

    pub(super) fn cell_at(&self, py: Python<'_>, logical: usize) -> PyResult<Py<PyAny>> {
        if self.is_row_missing(logical) {
            return Ok(py.None());
        }
        Ok(self.kind().cell_from_id(py, self.id_at(logical))?.unbind())
    }

    fn with_selection(&self, selection: RowSelection) -> Self {
        Self {
            storage: self.storage.clone(),
            selection,
        }
    }

    fn select_logical_rows(&self, rows: impl IntoIterator<Item = usize>) -> Self {
        self.with_selection(row_selection_from_logical_rows(
            self.selection_ref(),
            self.storage.ids().len(),
            rows,
        ))
    }

    fn slice(&self, start: isize, stop: isize, step: isize) -> Self {
        if let Some(logical) = CoordSeq::contiguous_positive_slice(start, stop, step) {
            let len = logical.end - logical.start;
            let selection = match self.selection_ref() {
                RowSelectionRef::Identity => {
                    RowSelection::window_trusted(logical.start, len, self.storage.ids().len())
                },
                RowSelectionRef::Window {
                    start: base,
                    len: base_len,
                } if logical.start <= base_len && logical.end <= base_len => {
                    RowSelection::window_trusted(
                        base + logical.start,
                        len,
                        self.storage.ids().len(),
                    )
                },
                map => row_selection_from_logical_rows(map, self.storage.ids().len(), logical),
            };
            return self.with_selection(selection);
        }
        let mut rows = Vec::new();
        let mut i = start;
        while (step > 0 && i < stop) || (step < 0 && i > stop) {
            rows.push(i as usize);
            i += step;
        }
        self.select_logical_rows(rows)
    }

    fn logical_contiguous_ids(&self) -> Option<&[u64]> {
        match self.selection_ref() {
            RowSelectionRef::Identity => Some(self.storage.ids()),
            RowSelectionRef::Window { start, len } => self.storage.ids().get(start..start + len),
            RowSelectionRef::Gather(_) => None,
        }
    }

    fn logical_ids_vec(&self) -> Vec<u64> {
        self.logical_id_iter().collect()
    }

    fn logical_id_iter(&self) -> impl ExactSizeIterator<Item = u64> + '_ {
        (0..self.len()).map(|logical| self.id_at(logical))
    }

    /// Unique ids + per-unique counts (value_counts pass).
    fn value_count_entries(&self) -> (Vec<u64>, Vec<i64>) {
        let mut slot_of: HashMap<u64, usize> = HashMap::with_capacity(self.len());
        let mut uniques = Vec::new();
        let mut counts = Vec::new();
        for row in 0..self.len() {
            if self.is_row_missing(row) {
                continue;
            }
            let id = self.id_at(row);
            let slot = match slot_of.entry(id) {
                Entry::Occupied(entry) => *entry.get(),
                Entry::Vacant(entry) => {
                    let slot = uniques.len();
                    entry.insert(slot);
                    uniques.push(id);
                    counts.push(0_i64);
                    slot
                },
            };
            counts[slot] += 1;
        }
        (uniques, counts)
    }

    /// Unique ids + dense codes (factorize pass).
    fn factorize_entries(&self) -> (Vec<u64>, Vec<i64>) {
        let mut slot_of: HashMap<u64, usize> = HashMap::with_capacity(self.len());
        let mut uniques = Vec::new();
        let mut codes = Vec::with_capacity(self.len());
        for row in 0..self.len() {
            if self.is_row_missing(row) {
                codes.push(-1);
                continue;
            }
            let id = self.id_at(row);
            let slot = match slot_of.entry(id) {
                Entry::Occupied(entry) => *entry.get(),
                Entry::Vacant(entry) => {
                    let slot = uniques.len();
                    entry.insert(slot);
                    uniques.push(id);
                    slot
                },
            };
            codes.push(slot as i64);
        }
        (uniques, codes)
    }

    fn ids_from_cells<G: GridCell>(cells: Vec<G>) -> Vec<u64> {
        cells.into_iter().map(GridCell::hash_key).collect()
    }

    fn logical_cells<'a, G: CellArrayGrid + 'a>(&'a self) -> impl Iterator<Item = G> + 'a {
        (0..self.len())
            .filter(|&logical| !self.is_row_missing(logical))
            .map(|logical| G::from_validated_id(self.id_at(logical)))
    }

    fn missing_mask(&self) -> Option<crate::array::missing::MissingMask> {
        crate::array::missing::MissingMask::from_vec(
            self.len(),
            (0..self.len())
                .map(|row| self.is_row_missing(row))
                .collect(),
        )
    }

    fn map_logical_cells<G, T>(&self, f: impl FnMut(G) -> T) -> Vec<T>
    where
        G: CellArrayGrid,
    {
        self.logical_cells::<G>().map(f).collect()
    }

    fn numpy_array_kind(
        &self,
        py: Python<'_>,
        dtype: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<CellNumpyArrayKind> {
        // Omitted dtype and explicit None both mean the grid default.
        if dtype.is_none_or(pyo3::types::PyAnyMethods::is_none) {
            return Ok(
                if self.kind() == GridKind::GeohashCell
                    || (0..self.len()).any(|row| self.is_row_missing(row))
                {
                    CellNumpyArrayKind::Objects
                } else {
                    CellNumpyArrayKind::Ids
                },
            );
        }
        let dtype = dtype.expect("checked non-None above");
        let numpy = crate::py::numpy::numpy_module(py)?;
        let dtype = numpy.getattr("dtype")?.call1((dtype,))?;
        let kind = dtype.getattr("kind")?.extract::<String>()?;
        let itemsize = dtype.getattr("itemsize")?.extract::<usize>()?;
        if kind == "O" {
            Ok(CellNumpyArrayKind::Objects)
        } else if kind == "u" && itemsize == size_of::<u64>() {
            if self.kind() == GridKind::GeohashCell {
                return Err(GeometryError::new_err(
                    "geohash CellArray has string tokens, not public uint64 ids; use dtype=object or .token",
                ));
            }
            if (0..self.len()).any(|row| self.is_row_missing(row)) {
                return Err(GeometryError::new_err(
                    "masked CellArray cannot be exported as uint64",
                ));
            }
            Ok(CellNumpyArrayKind::Ids)
        } else {
            Err(GeometryError::new_err(
                "dtype must be uint64, object, or None",
            ))
        }
    }

    fn object_numpy_array(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let values = self.to_cell_list(py)?;
        let array =
            numpy::PyArray1::from_owned_object_array(py, numpy::ndarray::Array1::from_vec(values));
        array.try_readwrite()?.make_nonwriteable();
        Ok(array.into_any().unbind())
    }

    fn hierarchy_predicate(
        &self,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
        intersects: bool,
    ) -> PyResult<Py<PyAny>> {
        let kind = self.kind();
        let values = if let Ok(other) = other.cast::<Self>() {
            let other = other.borrow();
            if kind != other.kind() {
                return Err(GeometryError::new_err(format!(
                    "cell types must match, got {} and {}",
                    kind.class_name(),
                    other.kind().class_name()
                )));
            }
            if self.len() != other.len() {
                return Err(GeometryError::new_err(format!(
                    "cell arrays must have equal lengths, got {} and {}",
                    self.len(),
                    other.len()
                )));
            }
            dispatch_cell_grid!(self, G, {
                (0..self.len())
                    .map(|row| {
                        if self.is_row_missing(row) || other.is_row_missing(row) {
                            return false;
                        }
                        let left = G::from_validated_id(self.id_at(row));
                        let right = G::from_validated_id(other.id_at(row));
                        left.contains_cell(right) || (intersects && right.contains_cell(left))
                    })
                    .collect::<Vec<_>>()
            })
        } else {
            let other_kind = grid_kind_from_type(&other.get_type())?;
            if kind != other_kind {
                return Err(GeometryError::new_err(format!(
                    "cell types must match, got {} and {}",
                    kind.class_name(),
                    other_kind.class_name()
                )));
            }
            let other_id = kind.id_from_value(other)?;
            dispatch_cell_grid!(self, G, {
                let right = G::from_validated_id(other_id);
                (0..self.len())
                    .map(|row| {
                        if self.is_row_missing(row) {
                            return false;
                        }
                        let left = G::from_validated_id(self.id_at(row));
                        left.contains_cell(right) || (intersects && right.contains_cell(left))
                    })
                    .collect::<Vec<_>>()
            })
        };
        crate::py::numpy::bool_array(py, values)
    }
}

enum CellNumpyArrayKind {
    Ids,
    Objects,
}

impl HeapSize for PyCellArray {
    fn heap_bytes(&self) -> usize {
        self.nbytes() + self.selection.heap_bytes()
    }
}

impl RowContainer for PyCellArray {
    const LABEL: &'static str = "CellArray";
    const INDEX_LABEL: &'static str = "cell array";

    fn row_count(&self) -> usize {
        self.len()
    }

    fn scalar_row(&self, py: Python<'_>, row: usize) -> PyResult<Py<PyAny>> {
        self.cell_at(py, row)
    }
}

impl RowGetItemContainer for PyCellArray {
    fn gather_rows(&self, rows: &[usize]) -> Self {
        self.select_logical_rows(rows.iter().copied())
    }

    fn slice_rows(&self, start: isize, stop: isize, step: isize) -> Self {
        self.slice(start, stop, step)
    }

    fn empty(&self) -> Self {
        Self {
            storage: CellStorage::from_trusted_ids(self.kind(), Vec::new()),
            selection: RowSelection::Identity,
        }
    }

    fn container_to_py(py: Python<'_>, value: Self) -> PyResult<Py<PyAny>> {
        Ok(value.into_pyobject(py)?.into_any().unbind())
    }
}

/// Per-kind bulk dispatch over the logical cells: monomorphize `$body` once
/// per grid type (ids are pre-validated by construction, so the id -> cell
/// conversions cannot fail).
macro_rules! map_grid_cells {
    ($self:expr, |$cell:ident| $body:expr) => {{ dispatch_cell_grid!($self, G, { $self.map_logical_cells::<G, _>(|$cell| $body) }) }};
}

frozen_pymethods! {
impl PyCellArray {
    // NEP 13: opt out of numpy ufunc dispatch — cell ids are opaque keys, not
    // numbers to broadcast over.
    #[classattr]
    #[expect(non_upper_case_globals, reason = "Python dunder name")]
    const __array_ufunc__: Option<Py<PyAny>> = None;

    #[new]
    #[pyo3(signature = (values, *, r#type=None))]
    fn new(
        values: &Bound<'_, PyAny>,
        r#type: Option<&Bound<'_, PyType>>,
    ) -> PyResult<Self> {
        let (kind, ids) = if let Some(cell_type) = r#type {
            let kind = grid_kind_from_type(cell_type)?;
            (kind, collect_ids(values, kind)?)
        } else {
            collect_inferred_ids(values)?
        };
        Ok(Self {
            storage: CellStorage::from_trusted_ids(kind, ids),
            selection: RowSelection::Identity,
        })
    }

    /// ``sys.getsizeof`` support: the wrapper plus this array's logical
    /// ``uint64`` id payload and any row-selection map. Shared backing buffers
    /// are reported like NumPy views, not as the full parent allocation.
    fn __sizeof__(&self) -> usize {
        std::mem::size_of::<Self>() + self.nbytes() + self.selection.heap_bytes()
    }

    /// Grid-system token for the stored cell type.
    ///
    /// Returns
    /// -------
    /// str
    #[getter]
    const fn grid(&self) -> &'static str {
        self.kind().token()
    }

    /// Logical id payload in bytes (`len * 8`).
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    fn nbytes(&self) -> usize {
        self.len() * size_of::<u64>()
    }

    /// Number of cells.
    ///
    /// Returns
    /// -------
    /// int
    fn __len__(&self) -> usize {
        self.len()
    }

    /// ``False`` only when the array is empty.
    ///
    /// Returns
    /// -------
    /// bool
    fn __bool__(&self) -> bool {
        self.len() != 0
    }

    /// Select cells by integer, slice, or fancy index.
    ///
    /// An ``int`` returns one cell object. A ``slice`` or fancy index returns
    /// a ``CellArray`` of the same cell kind.
    ///
    /// Returns
    /// -------
    /// Cell or CellArray
    fn __getitem__(&self, py: Python<'_>, index: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        array_getitem(self, py, index)
    }

    /// Iterate cells in row order.
    ///
    /// Returns
    /// -------
    /// iterator of Cell
    fn __iter__(&self) -> PyCellArrayIter {
        PyCellArrayIter::new(self.clone(), false)
    }

    /// Iterate cells in reverse row order.
    ///
    /// Returns
    /// -------
    /// iterator of Cell
    fn __reversed__(&self) -> PyCellArrayIter {
        PyCellArrayIter::new(self.clone(), true)
    }

    fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> Py<PyAny> {
        py_bool_or_not_implemented(py, other.extract::<PyRef<Self>>().ok(), |other| {
            self.logical_eq(&other)
        })
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash as _, Hasher as _};
        let mut hasher = crate::collections::python_hasher();
        self.kind().token().hash(&mut hasher);
        self.len().hash(&mut hasher);
        for row in 0..self.len() {
            self.is_row_missing(row).hash(&mut hasher);
            if !self.is_row_missing(row) {
                self.id_at(row).hash(&mut hasher);
            }
        }
        hasher.finish()
    }

    /// Per-row missing mask as a read-only boolean NumPy array.
    ///
    /// Bulk cell factories set a missing row when the corresponding geometry
    /// was missing; dense constructions return all-``False``.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray of bool
    #[getter]
    fn is_missing<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, numpy::PyArray1<bool>>> {
        let flags: Vec<bool> = (0..self.len()).map(|row| self.is_row_missing(row)).collect();
        let array = numpy::PyArray1::from_vec(py, flags);
        array.try_readwrite()?.make_nonwriteable();
        Ok(array)
    }

    /// Whether a cell id / cell object appears in the array.
    ///
    /// Returns
    /// -------
    /// bool
    fn __contains__(&self, item: &Bound<'_, PyAny>) -> bool {
        if item.is_none() {
            return (0..self.len()).any(|row| self.is_row_missing(row));
        }
        self.kind()
            .id_from_value(item)
            .is_ok_and(|needle| {
                (0..self.len()).any(|row| !self.is_row_missing(row) && self.id_at(row) == needle)
            })
    }

    /// Test whether every row hierarchically contains the paired cell.
    ///
    /// Parameters
    /// ----------
    /// other : cell or CellArray
    ///     One same-grid cell broadcast to every row, or a same-length array.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One read-only boolean per row.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> neighbor = list(cell.neighbors)[0]
    /// >>> assert neighbor is not None
    /// >>> gm.CellArray([cell, neighbor]).contains(cell).tolist()
    /// [True, False]
fn contains(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.hierarchy_predicate(py, other, false)
    }

    /// Test whether every row hierarchically intersects the paired cell.
    ///
    /// Two cells intersect when either is an ancestor of the other.
    ///
    /// Parameters
    /// ----------
    /// other : cell or CellArray
    ///     One same-grid cell broadcast to every row, or a same-length array.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One read-only boolean per row.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> neighbor = list(cell.neighbors)[0]
    /// >>> assert neighbor is not None
    /// >>> gm.CellArray([cell, neighbor]).intersects(cell).tolist()
    /// [True, False]
fn intersects(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.hierarchy_predicate(py, other, true)
    }

    /// First index of an equal cell id in `[start, stop)`.
    ///
    /// Parameters
    /// ----------
    /// value : cell object or int id
    ///     The element to locate.
    /// start : int, default 0
    ///     First position searched.
    /// stop : int, optional
    ///     One past the last position searched (the array length when
    ///     omitted).
    ///
    /// Returns
    /// -------
    /// int
    ///     The first matching position.
    ///
    /// Raises
    /// ------
    /// ValueError
    ///     If no element in the window equals ``value``.
    #[pyo3(signature = (value, start = 0, stop = None), text_signature = "($self, value, start=0, stop=None)")]
    fn index(&self, value: &Bound<'_, PyAny>, start: i64, stop: Option<i64>) -> PyResult<usize> {
        let needle = if value.is_none() {
            None
        } else {
            Some(self.kind().id_from_value(value).map_err(|_| {
            let value = value
                .repr()
                .and_then(|repr| repr.extract::<String>())
                .unwrap_or_else(|_| "value".to_owned());
            PyValueError::new_err(format!("{value} is not in array"))
            })?)
        };
        let len = self.len();
        let clamp = |bound: i64| -> usize {
            let resolved = if bound < 0 {
                bound + i64::try_from(len).unwrap_or(i64::MAX)
            } else {
                bound
            };
            usize::try_from(resolved.max(0)).unwrap_or(0).min(len)
        };
        let start = clamp(start);
        let stop = stop.map_or(len, clamp);
        if start < stop {
            for row in start..stop {
                if needle.is_none() == self.is_row_missing(row)
                    && needle.is_none_or(|needle| self.id_at(row) == needle)
                {
                    return Ok(row);
                }
            }
        }
        let value = value
            .repr()
            .and_then(|repr| repr.extract::<String>())
            .unwrap_or_else(|_| "cell".to_owned());
        Err(PyValueError::new_err(format!("{value} is not in array")))
    }

    /// Number of elements with the same cell id.
    ///
    /// Parameters
    /// ----------
    /// value : cell object or int id
    ///     The element to count.
    ///
    /// Returns
    /// -------
    /// int
    fn count(&self, value: &Bound<'_, PyAny>) -> usize {
        if value.is_none() {
            return (0..self.len()).filter(|&row| self.is_row_missing(row)).count();
        }
        self.kind().id_from_value(value).map_or(0, |needle| {
            (0..self.len())
                .filter(|&row| !self.is_row_missing(row) && self.id_at(row) == needle)
                .count()
        })
    }

    /// Unique cells and counts, ordered by descending count (pandas
    /// value_counts parity), with first appearance breaking ties.
    ///
    /// Returns
    /// -------
    /// tuple
    ///     ``(unique_cells, counts)`` where ``unique_cells`` is a CellArray
    ///     and ``counts`` is a read-only `int64` ndarray.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> unique, counts = gm.CellArray([cell, cell]).value_counts()
    /// >>> counts.tolist()
    /// [2]
fn value_counts(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (uniques, counts) = self.value_count_entries();
        let mut order: Vec<usize> = (0..uniques.len()).collect();
        order.sort_by(|&left, &right| {
            counts[right]
                .cmp(&counts[left])
                .then_with(|| left.cmp(&right))
        });
        let sorted_uniques = order.iter().map(|&slot| uniques[slot]).collect();
        let sorted_counts = order.iter().map(|&slot| counts[slot]).collect();
        let cells = Self::from_trusted_ids(self.kind(), sorted_uniques)
            .into_pyobject(py)?
            .into_any()
            .unbind();
        let counts = crate::py::numpy::int64_array(py, sorted_counts)?;
        Ok(PyTuple::new(py, [cells, counts])?.into_any().unbind())
    }

    /// Factorize cells into dense integer codes and first-seen uniques
    /// (pandas factorize parity).
    ///
    /// Returns
    /// -------
    /// tuple
    ///     ``(codes, unique_cells)`` where ``codes`` is a read-only `int64`
    ///     ndarray and ``unique_cells`` is a CellArray.
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> neighbor = list(cell.neighbors)[0]
    /// >>> assert neighbor is not None
    /// >>> codes, unique = gm.CellArray([cell, neighbor]).factorize()
    /// >>> codes.tolist()
    /// [0, 1]
fn factorize(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (uniques, codes) = self.factorize_entries();
        let codes = crate::py::numpy::int64_array(py, codes)?;
        let cells = Self::from_trusted_ids(self.kind(), uniques)
            .into_pyobject(py)?
            .into_any()
            .unbind();
        Ok(PyTuple::new(py, [codes, cells])?.into_any().unbind())
    }

    #[expect(
        clippy::type_complexity,
        reason = "pickle tuple matches CPython reduce contract"
    )]
    fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, (Py<PyAny>, String, Py<PyAny>))> {
        let callable = crate::gometry_lib_module(py)?
            .getattr(pyo3::intern!(py, "_unpickle_cell_array"))?
            .unbind();
        let payload = if self.kind() == GridKind::GeohashCell {
            let tokens = dispatch_cell_grid!(self, G, {
                (0..self.len())
                    .filter(|&row| !self.is_row_missing(row))
                    .map(|row| G::from_validated_id(self.id_at(row)).token())
                    .collect::<Vec<_>>()
            });
            tokens.into_py_any(py)?
        } else {
            let ids: Vec<u64> = (0..self.len())
                .filter(|&row| !self.is_row_missing(row))
                .map(|row| self.id_at(row))
                .collect();
            ids.into_py_any(py)?
        };
        let mask: Py<PyAny> = if (0..self.len()).any(|row| self.is_row_missing(row)) {
            let bytes: Vec<u8> = (0..self.len())
                .map(|row| u8::from(self.is_row_missing(row)))
                .collect();
            PyBytes::new(py, &bytes).into_any().unbind()
        } else {
            py.None()
        };
        Ok((
            callable,
            (payload, self.kind().token().to_owned(), mask),
        ))
    }

    pub(crate) fn __repr__(&self) -> String {
        format!("<CellArray[{}] len={}>", self.kind().class_name(), self.len())
    }

    /// Return a read-only NumPy identity column.
    ///
    /// H3, S2, and tile arrays expose their validated ids as uint64
    /// (zero-copy for contiguous selections). Geohash has no public numeric
    /// id, so it returns an object array of typed GeohashCell values.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> type(gm.CellArray([cell]).to_numpy()).__name__
    /// 'ndarray'
fn to_numpy(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        let owner = slf.clone().into_any();
        let borrowed = slf.borrow();
        if borrowed.kind() == GridKind::GeohashCell
            || (0..borrowed.len()).any(|row| borrowed.is_row_missing(row))
        {
            return borrowed.object_numpy_array(owner.py());
        }
        if let Some(values) = borrowed.logical_contiguous_ids() {
// SAFETY: values is tied to slf, and owner pins that object as
            // the array base.
            return unsafe { crate::py::numpy::uint64_slice_array(owner, values) };
        }
        crate::py::numpy::uint64_array(owner.py(), borrowed.logical_ids_vec())
    }

    /// NumPy array protocol.
    ///
    /// With `dtype=None`, H3/S2/tile arrays export raw uint64 ids and
    /// Geohash arrays export typed GeohashCell objects. `dtype=uint64` is
    /// available only for the numeric grids; `dtype=object` exports typed
    /// cell objects for every grid, matching iteration.
    ///
    /// Parameters
    /// ----------
    /// dtype : uint64 or object, optional
    /// copy : bool, optional
    ///     ``False`` requires a contiguous zero-copy numeric-id export;
    ///     gathered ids and every object export raise because they materialize.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    #[pyo3(signature = (dtype=None, copy=None))]
    fn __array__(
        slf: Bound<'_, Self>,
        dtype: Option<&Bound<'_, PyAny>>,
        copy: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let kind = slf.borrow().numpy_array_kind(py, dtype)?;
        match kind {
            CellNumpyArrayKind::Ids => {
                if copy == Some(true) {
                    let ids = slf.borrow().logical_ids_vec();
                    return crate::py::numpy::uint64_array(py, ids);
                }
                if copy == Some(false) {
                    let can_borrow = slf.borrow().logical_contiguous_ids().is_some();
                    if !can_borrow {
                        return Err(GeometryError::new_err(
                            "gometry cannot return the requested array without copying",
                        ));
                    }
                }
                Self::to_numpy(slf)
            }
            CellNumpyArrayKind::Objects => {
                if copy == Some(false) {
                    return Err(GeometryError::new_err(
                        "gometry cannot return the requested array without copying",
                    ));
                }
                slf.borrow().object_numpy_array(py)
            }
        }
    }

    /// Center points of every cell, as a packed WGS84 point array.
    ///
    /// The bulk twin of each scalar cell's center property — one
    /// vectorized call for the index-millions-of-points workflow, returning
    /// zero-copy packed point storage.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///     One ``Point`` (lon/lat, ``OGC:CRS84``) per cell.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> arr = gm.CellArray([gm.Tile(x=0, y=0, zoom=1)])
    /// >>> arr.center.to_wkt()[0]
    /// 'POINT (-90 42.5255643899033)'
    #[getter]
    pub(crate) fn center(&self) -> crate::PyGeometryArray {
        use crate::grid::cell::GridCell as _;
        let points = map_grid_cells!(self, |cell| {
            let point = cell.center_point();
            crate::geometry::XY {
                x: point.x,
                y: point.y,
            }
        });
        let coords = CoordSeq::from_xy(&points);
        let result = crate::PyGeometryArray::packed_points(
            coords,
            crate::Frame::Crs(crate::wgs84_crs()),
        );
        self.missing_mask().map_or_else(|| result.clone(), |mask| {
            crate::PyGeometryArray::scatter_present_rows(&result, mask)
        })
    }

    /// Geodesic cell areas in square meters.
    ///
    /// The bulk twin of each scalar cell's ``area`` property: H3 and S2 use
    /// their exact cell geometry; geohash and tile cells use the ellipsoidal
    /// area of their lon/lat rectangle.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One ``float64`` area (m²) per cell.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> arr = gm.CellArray([gm.Tile(x=0, y=0, zoom=0)])
    /// >>> float(arr.area[0]) > 1e14
    /// True
    #[getter]
    pub(crate) fn area(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let values = dispatch_cell_grid!(self, G, {
            (0..self.len())
                .map(|row| if self.is_row_missing(row) { f64::NAN } else { G::from_validated_id(self.id_at(row)).area_m2() })
                .collect()
        });
        crate::py::numpy::float64_array(py, values)
    }

    /// Filled WGS84 polygon of every cell, as a geometry array.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    #[getter]
    pub(crate) fn polygon(&self) -> crate::PyGeometryArray {
        let result = crate::PyGeometryArray::from_shapes(
            dispatch_cell_grid!(self, G, {
                self.logical_cells::<G>().map(crate::grid::cell::GridCell::boundary_shape).collect()
            }),
            crate::Frame::Crs(crate::wgs84_crs()),
        );
        self.missing_mask().map_or_else(|| result.clone(), |mask| {
            crate::PyGeometryArray::scatter_present_rows(&result, mask)
        })
    }

    /// Number of descendant cells each cell has at ``depth``.
    ///
    /// The columnar mirror of the scalar ``children_count`` — the count only,
    /// without materializing the children (which ``children`` does, as ragged
    /// rows). Present-row counts are integer-valued but are represented as
    /// ``float64``; they are exact only when within float64's integer precision.
    /// Missing rows are represented by ``NaN`` to preserve row alignment.
    ///
    /// Parameters
    /// ----------
    /// depth : int, optional
    ///     Target depth (resolution / level / precision / zoom). Omitted means
    ///     one step finer than each cell.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``;
    ///     missing rows are ``NaN``.
    ///
    /// Raises
    /// ------
    /// GeometryError
    ///     If ``depth`` is out of range for the grid.
    ///
    /// See Also
    /// --------
    /// children : The descendant cells themselves, as ragged rows.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> neighbor = list(cell.neighbors)[0]
    /// >>> assert neighbor is not None
    /// >>> cells = gm.CellArray([cell, neighbor])
    /// >>> cells.children_count(9).tolist()
    /// [49.0, 49.0]
    #[pyo3(signature = (depth = None, /), text_signature = "($self, depth=None, /)")]
    pub(crate) fn children_count(
        &self,
        py: Python<'_>,
        depth: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let values = dispatch_cell_grid!(self, G, {
            (0..self.len()).map(|row| {
                if self.is_row_missing(row) {
                    Ok(f64::NAN)
                } else {
                    crate::py::cells::cell_ops::cell_descendant_count(
                        G::from_validated_id(self.id_at(row)), depth, G::parse_depth,
                    ).map(|value| value as f64)
                }
            }).collect::<Vec<_>>()
        })
        .into_iter()
        .collect::<PyResult<Vec<_>>>()?;
        crate::py::numpy::float64_array(py, values)
    }

    /// Parent cell of every input cell.
    ///
    /// Parameters
    /// ----------
    /// depth : int, optional
    ///     Target depth; defaults to one coarser than each row.
    ///
    /// Returns
    /// -------
    /// CellArray
    #[pyo3(signature = (depth = None, /), text_signature = "($self, depth=None, /)")]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> neighbor = list(cell.neighbors)[0]
    /// >>> assert neighbor is not None
    /// >>> cells = gm.CellArray([cell, neighbor])
    /// >>> parent = cells.parent(6)[0]
    /// >>> assert parent is not None
    /// >>> parent.token
    /// '86283082fffffff'
    pub(crate) fn parent(&self, depth: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let kind = self.kind();
        let present = dispatch_cell_grid!(self, G, {
            self.logical_cells::<G>().map(|cell| {
                crate::py::cells::cell_ops::cell_parent(cell, depth, G::parse_depth)
                    .map(GridCell::hash_key)
            }).collect::<Vec<_>>()
        })
        .into_iter()
        .collect::<PyResult<Vec<_>>>()?;
        let mut ids = Vec::with_capacity(self.len());
        let mut next = present.into_iter();
        for row in 0..self.len() {
            ids.push(if self.is_row_missing(row) {
                0
            } else {
                next.next().expect("present parent count matches")
            });
        }
        Ok(Self::from_trusted_ids_with_missing(
            kind,
            ids,
            (0..self.len()).map(|row| self.is_row_missing(row)).collect(),
        ))
    }

    /// The edge-adjacent cells of every cell, as ragged rows.
    ///
    /// Returns
    /// -------
    /// Groups of CellArray
    ///     One row of neighbors per input cell, in input order. Neighbor
    ///     counts vary, so the result is a Groups, not a rectangular
    ///     CellArray.
    #[getter]
    pub(crate) fn neighbors(&self) -> PyResult<crate::py::vectors::Groups> {
        let kind = self.kind();
        let mut builder = crate::grid::CellGroupsBuilder::new("CellArray.neighbors");
        dispatch_cell_grid!(self, G, {
            for row in 0..self.len() {
                if self.is_row_missing(row) {
                    builder.push_row(std::iter::empty::<u64>()).map_err(super::cell_limit_err)?;
                    continue;
                }
                let cell = G::from_validated_id(self.id_at(row));
                builder
                    .push_row(
                        GridCell::neighbors(cell)
                            .into_iter()
                            .map(GridCell::hash_key),
                    )
                    .map_err(super::cell_limit_err)?;
            }
            Ok::<(), pyo3::PyErr>(())
        })?;
        let (ids, offsets) = builder.finish();
        crate::py::vectors::Groups::from_cell_flat(kind, ids, offsets)
    }

    /// Return the child cells of every cell at a finer depth, as ragged rows.
    ///
    /// Parameters
    /// ----------
    /// depth : int, optional
    ///     Target depth; must not be coarser than any input cell. Defaults to
    ///     one finer than each cell's own depth.
    ///
    /// Returns
    /// -------
    /// Groups of CellArray
    ///     One row of children per input cell, in input order.
    #[pyo3(signature = (depth = None, /), text_signature = "($self, depth=None, /)")]
///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> children = gm.CellArray([cell]).children(8)[0]
    /// >>> assert children is not None
    /// >>> len(children)
    /// 7
    pub(crate) fn children(
        &self,
        depth: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<crate::py::vectors::Groups> {
        use crate::py::cells::cell_ops::cell_children;
        let kind = self.kind();
        let mut builder = crate::grid::CellGroupsBuilder::new("CellArray.children");
        dispatch_cell_grid!(self, G, {
            for row in 0..self.len() {
                if self.is_row_missing(row) {
                    builder.push_row(std::iter::empty::<u64>()).map_err(super::cell_limit_err)?;
                    continue;
                }
                let cell = G::from_validated_id(self.id_at(row));
                builder
                    .push_row(
                        cell_children(cell, depth, G::parse_depth)?
                            .into_iter()
                            .map(GridCell::hash_key),
                    )
                    .map_err(super::cell_limit_err)?;
            }
            Ok::<(), pyo3::PyErr>(())
        })?;
        let (ids, offsets) = builder.finish();
        crate::py::vectors::Groups::from_cell_flat(kind, ids, offsets)
    }

    /// Compact this cell set to the coarsest exact covering.
    /// Missing rows are skipped; this is a set operation over present cells.
    ///
    /// Parameters
    /// ----------
    /// depth : int, optional
    ///     Coarsest depth allowed.
    ///
    /// Returns
    /// -------
    /// CellArray
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> neighbor = list(cell.neighbors)[0]
    /// >>> assert neighbor is not None
    /// >>> cells = gm.CellArray([cell, neighbor])
    /// >>> len(cells.compact(5))
    /// 2
    #[pyo3(signature = (depth = None, /), text_signature = "($self, depth=None, /)")]
    pub(crate) fn compact(&self, depth: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let kind = self.kind();
        let compacted = dispatch_cell_grid!(self, G, {
            Self::ids_from_cells(G::compact_cells(
                self.logical_cells::<G>().collect(),
                G::parse_compact_floor(depth)?,
            )?)
        });
        Ok(Self::from_trusted_ids(kind, compacted))
    }

    /// Expand this cell set to a uniform depth.
    /// Missing rows are skipped; this is a set operation over present cells.
    ///
    /// Parameters
    /// ----------
    /// depth : int
    ///     Target depth; no coarser than any row.
    ///
    /// Returns
    /// -------
    /// CellArray
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> len(gm.CellArray([cell]).uncompact(8))
    /// 7
    #[pyo3(signature = (depth, /), text_signature = "($self, depth, /)")]
    pub(crate) fn uncompact(&self, depth: &Bound<'_, PyAny>) -> PyResult<Self> {
        let kind = self.kind();
        let expanded = dispatch_cell_grid!(self, G, {
            let depth = G::parse_depth(depth)?;
            let cells: Vec<G> = self.logical_cells::<G>().collect();
            if let Some(cell) = cells.iter().find(|cell| cell.depth() > depth) {
                return Err(uncompact_floor_error(
                    kind,
                    G::DEPTH_NAME,
                    G::uncompact_floor_token(cell),
                ));
            }
            Self::ids_from_cells(G::uncompact_cells(cells, depth)?)
        });
        Ok(Self::from_trusted_ids(kind, expanded))
    }

    /// Dissolve this cell set into one outline geometry.
    /// Missing rows are skipped; this is a set operation over present cells.
    ///
    /// Returns
    /// -------
    /// Polygon or MultiPolygon
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> gm.CellArray([cell]).to_polygon().geometry_type
    /// 'Polygon'
    fn to_polygon(&self) -> PyResult<crate::Typed> {
        match self.kind() {
            GridKind::H3Cell => {
                let cells = self.logical_cells::<h3o::CellIndex>().collect();
                super::h3::dissolve_cells(cells)
            },
            GridKind::S2Cell => {
                let cells = self
                    .logical_cells::<crate::grid::s2::cellid::CellId>()
                    .collect::<Vec<_>>();
                super::s2::s2_dissolve(&cells)
            },
            GridKind::Tile => {
                let cells = self
                    .logical_cells::<crate::grid::tile::Tile>()
                    .collect::<Vec<_>>();
                rect_cells_to_polygon(cells)
            },
            GridKind::GeohashCell => {
                let cells = self
                    .logical_cells::<crate::grid::geohash::Geohash>()
                    .collect::<Vec<_>>();
                rect_cells_to_polygon(cells)
            },
        }
    }

    /// Canonical string token of every cell, in order.
    ///
    /// For H3, S2, and tiles this is the text form of the numeric id exposed
    /// by `to_numpy()`. For Geohash it is the public string identity itself;
    /// Geohash `to_numpy()` instead returns typed `GeohashCell` objects.
    ///
    /// Returns
    /// -------
    /// list of str or None
    ///     One canonical token per logical row (H3 hex, S2 token, geohash, or
    ///     tile quadkey); ``None`` marks a missing row.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CellArray([gm.GeohashCell('u33d')]).token
    /// ['u33d']
    #[getter]
    pub(crate) fn token(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        use pyo3::IntoPyObjectExt as _;
        let tokens: Vec<Option<String>> = (0..self.len()).map(|row| {
            if self.is_row_missing(row) { None } else {
                Some(dispatch_cell_grid!(self, G, {
                    crate::grid::cell::GridCell::token(G::from_validated_id(self.id_at(row)))
                }))
            }
        }).collect();
        tokens.into_py_any(py)
    }

}
}

/// Rebuild a pickled CellArray from its public identities and logical mask.
#[pyfunction]
pub(crate) fn _unpickle_cell_array(
    ids: &Bound<'_, PyAny>,
    grid: &str,
    mask: &Bound<'_, PyAny>,
) -> PyResult<PyCellArray> {
    let kind = GridKind::parse(grid)?;
    let (ids, missing) = if mask.is_none() {
        (super::grid_kind::collect_ids(ids, kind)?, None)
    } else {
        let mask = mask
            .cast::<PyBytes>()
            .map_err(|_| PyValueError::new_err("CellArray pickle mask must be bytes or None"))?;
        let bytes = mask.as_bytes();
        let mut present_count = 0;
        for &value in bytes {
            match value {
                0 => present_count += 1,
                1 => {},
                _ => {
                    return Err(PyValueError::new_err(
                        "CellArray pickle mask must contain only 0 or 1",
                    ));
                },
            }
        }
        let present = super::grid_kind::collect_ids(ids, kind)?;
        if present_count != present.len() {
            return Err(PyValueError::new_err(
                "CellArray pickle mask present count does not match identities",
            ));
        }
        let mut logical_ids = Vec::with_capacity(bytes.len());
        let mut present_index = 0;
        for &value in bytes {
            logical_ids.push(if value == 0 {
                let id = present[present_index];
                present_index += 1;
                id
            } else {
                0
            });
        }
        (
            logical_ids,
            Some(bytes.iter().map(|&value| value == 1).collect()),
        )
    };
    Ok(PyCellArray {
        storage: match missing {
            Some(missing) => CellStorage::from_trusted_ids_with_missing(kind, ids, missing),
            None => CellStorage::from_trusted_ids(kind, ids),
        },
        selection: RowSelection::Identity,
    })
}
use pyo3::IntoPyObjectExt as _;
