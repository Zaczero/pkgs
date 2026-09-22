#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
//! Typed bulk cell storage — the CellArray Python surface.
use std::collections::hash_map::Entry;
use std::ops::Range;

use numpy::PyArrayMethods as _;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::{PyResult, Python, pyclass};
use pyo3::types::{
    PyAny, PyBytes, PyBytesMethods as _, PyList, PyListMethods as _, PyString, PyTuple, PyType,
};
use pyo3::{Bound, Py, PyErr};

use crate::HeapSize;
use crate::array::missing::ValidityMask;
use crate::array::{RowSelection, RowSelectionRef, physical_row, row_selection_from_logical_rows};
use crate::broadcast::py_bool_or_not_implemented;
use crate::collections::{HashMap, HashMapExt as _};
use crate::geometry::{CoordSeq, CoordSeqBuilder, CoordinateAxes};
use crate::grid::cell::{GridCell, HierarchicalCell as _};
use crate::grid::cell_set;
use crate::grid::geohash::{GEOHASH_MAX_PRECISION, Geohash};
use crate::grid::s2::cellid::CellId;
use crate::grid::tile::{TILE_MAX_ZOOM, Tile};
use crate::py::cells::grid_kind::{
    collect_ids, collect_inferred_ids, grid_kind_from_type, membership_id,
};
use crate::py::cells::{
    FactorizationError, GeometryError, GridKind, H3_MAX_RESOLUTION, IntoPyObject as _,
    PyAnyMethods as _, PyCellArrayIter, PyRef, cell_ops, checked_depth, py_i64_required,
    pyfunction, rect_cells_to_polygon, uncompact_floor_error,
};
use crate::py::numpy::{
    astype_readonly, bool_array, float64_array, int64_array, numpy_module, uint64_array,
    uint64_slice_array,
};
use crate::py::row::{RowContainer, RowGetItemContainer, array_getitem};
use crate::py::vectors::Groups;

/// An immutable array of one grid cell type backed by a shared ``uint64`` id
/// column.
///
/// Index with an integer for the typed cell object or `None` for a missing row;
/// slice or mask for a new `CellArray`. Build from an iterable containing
/// typed cell objects and `None`; the grid is inferred from the first concrete
/// cell. Raw ids and tokens are accepted when a concrete cell supplies the
/// family. For raw-only ids/tokens, arrays, buffers, or all-missing inputs,
/// pass ``type=`` explicitly; every id is validated for that grid.
///
/// Parameters
/// ----------
/// values : numpy.ndarray or iterable
///     Typed cell objects and ``None``; raw ids/tokens are also accepted when
///     a concrete cell supplies the family.
/// type : type, optional
///     Native cell class. Inferred from a concrete typed cell when present.
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
    ids: crate::geometry::SharedSlice<u64>,
    /// Per-physical-row missing flags. `None` means every row is present
    /// (the common dense factory path). When `Some`, length equals `ids`.
    missing: Option<crate::array::ValidityMask>,
}

impl CellStorage {
    fn new(kind: GridKind, ids: Vec<u64>, missing: Option<crate::array::ValidityMask>) -> Self {
        debug_assert!(missing.as_ref().is_none_or(|mask| mask.len() == ids.len()));
        Self {
            kind,
            ids: crate::geometry::SharedSlice::from_vec(ids),
            missing,
        }
    }
}

trait CellArrayGrid: GridCell {
    fn from_validated_id(id: u64) -> Self;
    fn parse_depth(value: &Bound<'_, PyAny>) -> PyResult<u8>;
    fn parse_compact_floor(depth: Option<&Bound<'_, PyAny>>) -> PyResult<u8>;
    fn compact_cells(cells: Vec<Self>, floor: u8) -> PyResult<Vec<Self>> {
        Ok(cell_set::compact_with_floor(cells, floor).into_public_vec())
    }
    fn compact_ids(ids: Vec<u64>, floor: u8) -> PyResult<Vec<u64>> {
        let cells: Vec<_> = ids.into_iter().map(Self::from_validated_id).collect();
        let cells = Self::compact_cells(cells, floor)?;
        Ok(cells.into_iter().map(Self::hash_key).collect())
    }
    fn uncompact_cells(cells: Vec<Self>, depth: u8) -> PyResult<Vec<Self>> {
        Ok(cell_set::uncompact(&cells, depth)?.into_public_vec())
    }
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

    fn uncompact_floor_token(cell: &Self) -> String {
        cell.to_string()
    }
}

impl CellArrayGrid for CellId {
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

    fn uncompact_floor_token(cell: &Self) -> String {
        cell.token()
    }
}

impl CellArrayGrid for Tile {
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
            i64::from(TILE_MAX_ZOOM),
        )
    }

    fn uncompact_floor_token(cell: &Self) -> String {
        cell.quadkey()
    }
}

impl CellArrayGrid for Geohash {
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
            i64::from(GEOHASH_MAX_PRECISION),
        )
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
    pub(crate) fn from_ids(kind: GridKind, ids: Vec<u64>) -> Self {
        Self {
            storage: CellStorage::new(kind, ids, None),
            selection: RowSelection::Identity,
        }
    }

    /// Build with a parallel missing mask (one flag per id). Used by bulk
    /// factories that degrade missing geometry rows instead of rejecting them.
    pub(crate) fn from_ids_with_missing(
        kind: GridKind,
        ids: Vec<u64>,
        missing: Vec<bool>,
    ) -> crate::error::Result<Self> {
        let missing = crate::array::ValidityMask::from_vec(ids.len(), missing)?;
        Ok(Self {
            storage: CellStorage::new(kind, ids, missing),
            selection: RowSelection::Identity,
        })
    }

    /// Whether logical row `i` is a missing factory placeholder.
    pub(crate) fn is_row_missing(&self, logical: usize) -> bool {
        let Some(mask) = self.storage.missing.as_ref() else {
            return false;
        };
        mask.is_missing(physical_row(self.selection_ref(), logical))
    }

    fn selection_ref(&self) -> RowSelectionRef<'_> {
        self.selection.as_deref()
    }

    pub(crate) fn len(&self) -> usize {
        self.selection_ref().len(self.storage.ids.len())
    }

    pub(crate) fn logical_heap_bytes_range(&self, range: Range<usize>) -> usize {
        debug_assert!(range.end <= self.len());
        let ids = if range.is_empty() {
            0
        } else {
            crate::heap_size::ARC_CONTROL_BYTES
                + size_of::<Vec<u64>>()
                + range.len() * size_of::<u64>()
        };
        let missing = self.storage.missing.as_ref().map_or(0, |_| {
            crate::heap_size::ARC_CONTROL_BYTES + size_of::<Vec<u8>>() + range.len().div_ceil(8)
        });
        ids + missing
    }

    /// The grid kind of these cells.
    pub(crate) const fn kind(&self) -> GridKind {
        self.storage.kind
    }

    /// A contiguous logical sub-range as a new array (zero-copy selection).
    /// Used by [`crate::py::vectors::Groups`] to view one ragged row of cells.
    pub(crate) fn logical_row_range(&self, range: Range<usize>) -> crate::error::Result<Self> {
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
        let mut output = Vec::with_capacity(self.len());
        for logical in 0..self.len() {
            output.push(self.cell_at(py, logical)?);
        }
        Ok(output)
    }

    pub(super) fn id_at(&self, logical: usize) -> u64 {
        self.storage.ids[physical_row(self.selection_ref(), logical)]
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

    fn select_logical_rows(
        &self,
        rows: impl IntoIterator<Item = usize>,
    ) -> crate::error::Result<Self> {
        Ok(self.with_selection(row_selection_from_logical_rows(
            self.selection_ref(),
            self.storage.ids.len(),
            rows,
        )?))
    }

    fn slice(&self, start: isize, stop: isize, step: isize) -> crate::error::Result<Self> {
        if let Some(logical) = CoordSeq::contiguous_positive_slice(start, stop, step) {
            let selection = row_selection_from_logical_rows(
                self.selection_ref(),
                self.storage.ids.len(),
                logical,
            )?;
            return Ok(self.with_selection(selection));
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
            RowSelectionRef::Identity => Some(&self.storage.ids),
            RowSelectionRef::Window { start, len } => self.storage.ids.get(start..start + len),
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
    fn value_count_entries(&self) -> std::result::Result<(Vec<u64>, Vec<i64>), FactorizationError> {
        let mut slot_of: HashMap<u64, usize> = HashMap::with_capacity(self.len());
        let mut uniques = Vec::with_capacity(self.len());
        let mut counts = Vec::with_capacity(self.len());
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
            let Some(count) = counts[slot].checked_add(1) else {
                return Err(FactorizationError::CountOverflow);
            };
            counts[slot] = count;
        }
        Ok((uniques, counts))
    }

    /// Unique ids + dense codes (factorize pass).
    fn factorize_entries(&self) -> std::result::Result<(Vec<u64>, Vec<i64>), FactorizationError> {
        let mut slot_of: HashMap<u64, usize> = HashMap::with_capacity(self.len());
        let mut uniques = Vec::with_capacity(self.len());
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
            let Ok(code) = i64::try_from(slot) else {
                return Err(FactorizationError::CodeOverflow);
            };
            codes.push(code);
        }
        Ok((uniques, codes))
    }

    fn ids_from_cells<G: GridCell>(cells: Vec<G>) -> Vec<u64> {
        cells.into_iter().map(GridCell::hash_key).collect()
    }

    fn logical_cells<'a, G: CellArrayGrid + 'a>(&'a self) -> impl Iterator<Item = G> + 'a {
        (0..self.len())
            .filter(|&logical| !self.is_row_missing(logical))
            .map(|logical| G::from_validated_id(self.id_at(logical)))
    }

    fn missing_mask(&self) -> PyResult<Option<ValidityMask>> {
        if matches!(self.selection_ref(), RowSelectionRef::Identity) {
            return Ok(self.storage.missing.clone());
        }
        Ok(ValidityMask::from_missing_iter(
            self.len(),
            (0..self.len()).map(|row| self.is_row_missing(row)),
        )?)
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
                    CellNumpyArrayKind::Ids {
                        dtype: None,
                        native: true,
                    }
                },
            );
        }
        let dtype = dtype.expect("checked non-None above");
        let numpy = numpy_module(py)?;
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
            Ok(CellNumpyArrayKind::Ids {
                native: dtype.getattr("isnative")?.extract::<bool>()?,
                dtype: Some(dtype.unbind()),
            })
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
                let mut values = Vec::with_capacity(self.len());
                for row in 0..self.len() {
                    let value = if self.is_row_missing(row) || other.is_row_missing(row) {
                        false
                    } else {
                        let left = G::from_validated_id(self.id_at(row));
                        let right = G::from_validated_id(other.id_at(row));
                        left.contains_cell(right) || (intersects && right.contains_cell(left))
                    };
                    values.push(value);
                }
                values
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
                let mut values = Vec::with_capacity(self.len());
                for row in 0..self.len() {
                    let value = if self.is_row_missing(row) {
                        false
                    } else {
                        let left = G::from_validated_id(self.id_at(row));
                        left.contains_cell(right) || (intersects && right.contains_cell(left))
                    };
                    values.push(value);
                }
                values
            })
        };
        bool_array(py, values)
    }
}

enum CellNumpyArrayKind {
    Ids {
        dtype: Option<Py<PyAny>>,
        native: bool,
    },
    Objects,
}

impl HeapSize for PyCellArray {
    fn heap_bytes(&self) -> usize {
        self.logical_heap_bytes_range(0..self.len()) + self.selection.heap_bytes()
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
    fn gather_rows(&self, rows: &[usize]) -> PyResult<Self> {
        Ok(self.select_logical_rows(rows.iter().copied())?)
    }

    fn slice_rows(&self, start: isize, stop: isize, step: isize) -> PyResult<Self> {
        Ok(self.slice(start, stop, step)?)
    }

    fn empty(&self) -> PyResult<Self> {
        Ok(Self {
            storage: CellStorage::new(self.kind(), Vec::new(), None),
            selection: RowSelection::Identity,
        })
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
        let (kind, collected) = if let Some(cell_type) = r#type {
            let kind = grid_kind_from_type(cell_type)?;
            (kind, collect_ids(values, kind)?)
        } else {
            collect_inferred_ids(values)?
        };
        let storage = CellStorage::new(kind, collected.ids, collected.missing);
        Ok(Self {
            storage,
            selection: RowSelection::Identity,
        })
    }

    /// ``sys.getsizeof`` support: the wrapper plus this array's logical
    /// ``uint64`` id payload, missing flags, and any row-selection map. Shared
    /// backing buffers are reported like NumPy views, not as the full parent
    /// allocation.
    fn __sizeof__(&self) -> usize {
        self.total_size()
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
        let array = numpy::PyArray1::<bool>::zeros(py, self.len(), false);
        let mut write = array.try_readwrite()?;
        for (row, output) in write.as_slice_mut()?.iter_mut().enumerate() {
            *output = self.is_row_missing(row);
        }
        write.make_nonwriteable();
        Ok(array)
    }

    /// Whether a cell id / cell object appears in the array.
    ///
    /// Returns
    /// -------
    /// bool
    fn __contains__(&self, py: Python<'_>, item: &Bound<'_, PyAny>) -> PyResult<bool> {
        if item.is_none() {
            let array = self.clone();
            return Ok(py.detach(move || (0..array.len()).any(|row| array.is_row_missing(row))));
        }
        let kind = self.kind();
        let Some(needle) = membership_id(item, |item| kind.id_from_value(item))? else {
            return Ok(false);
        };
        let array = self.clone();
        Ok(py.detach(move || {
            (0..array.len()).any(|row| !array.is_row_missing(row) && array.id_at(row) == needle)
        }))
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
    fn index(
        &self,
        value: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = crate::strict_i64)] start: i64,
        #[pyo3(from_py_with = crate::strict_optional_i64)] stop: Option<i64>,
    ) -> PyResult<usize> {
        let needle = if value.is_none() {
            None
        } else {
            let kind = self.kind();
            let Some(needle) = membership_id(value, |value| kind.id_from_value(value))? else {
                let value = value
                    .repr()
                    .and_then(|repr| repr.extract::<String>())
                    .unwrap_or_else(|_| "value".to_owned());
                return Err(PyValueError::new_err(format!("{value} is not in array")));
            };
            Some(needle)
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
    fn count(&self, value: &Bound<'_, PyAny>) -> PyResult<usize> {
        if value.is_none() {
            return Ok((0..self.len()).filter(|&row| self.is_row_missing(row)).count());
        }
        let kind = self.kind();
        let Some(needle) = membership_id(value, |value| kind.id_from_value(value))? else {
            return Ok(0);
        };
        Ok((0..self.len())
            .filter(|&row| !self.is_row_missing(row) && self.id_at(row) == needle)
            .count())
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
        let result = (|| -> std::result::Result<(Vec<u64>, Vec<i64>), FactorizationError> {
            let (uniques, counts) = self.value_count_entries()?;
            let mut order: Vec<_> = (0..uniques.len()).collect();
            order.sort_unstable_by(|&left, &right| {
                counts[right]
                    .cmp(&counts[left])
                    .then_with(|| left.cmp(&right))
            });
            let mut sorted_uniques = Vec::with_capacity(order.len());
            let mut sorted_counts = Vec::with_capacity(order.len());
            for &slot in &order {
                sorted_uniques.push(uniques[slot]);
                sorted_counts.push(counts[slot]);
            }
            Ok((sorted_uniques, sorted_counts))
        })();
        let (sorted_uniques, sorted_counts) = match result {
            Ok(values) => values,
            Err(error) => {
                return Err(error.into_py_err(
                    "cell value count overflows int64",
                    "cell factor code overflows int64",
                ));
            },
        };
        let cells = match Self::from_ids(self.kind(), sorted_uniques).into_pyobject(py) {
            Ok(cells) => cells.into_any().unbind(),
            Err(error) => {
                drop(sorted_counts);
                return Err(error);
            },
        };
        let counts = match int64_array(py, sorted_counts) {
            Ok(counts) => counts,
            Err(error) => {
                drop(cells);
                return Err(error);
            },
        };
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
        let (uniques, codes) = match self.factorize_entries() {
            Ok(values) => values,
            Err(error) => {
                return Err(error.into_py_err(
                    "cell value count overflows int64",
                    "cell factor code overflows int64",
                ));
            },
        };
        let codes = match int64_array(py, codes) {
            Ok(codes) => codes,
            Err(error) => {
                drop(uniques);
                return Err(error);
            },
        };
        let cells = match Self::from_ids(self.kind(), uniques).into_pyobject(py) {
            Ok(cells) => cells.into_any().unbind(),
            Err(error) => {
                drop(codes);
                return Err(error);
            },
        };
        Ok(PyTuple::new(py, [codes, cells])?.into_any().unbind())
    }

    #[expect(
        clippy::type_complexity,
        reason = "pickle tuple matches CPython reduce contract"
    )]
    fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, (Py<PyAny>, String, Py<PyAny>))> {
        let callable = crate::gometry_lib_module(py)?
            .getattr(pyo3::intern!(py, "_unpickle_cell_array_v1"))?
            .unbind();
        let payload = if self.kind() == GridKind::GeohashCell {
            let tokens = dispatch_cell_grid!(self, G, {
                let mut tokens = Vec::with_capacity(self.len());
                for row in 0..self.len() {
                    if !self.is_row_missing(row) {
                        tokens.push(G::from_validated_id(self.id_at(row)).token());
                    }
                }
                tokens
            });
            tokens.into_py_any(py)?
        } else {
            let mut ids = Vec::with_capacity(self.len());
            for row in 0..self.len() {
                if !self.is_row_missing(row) {
                    ids.push(self.id_at(row));
                }
            }
            ids.into_py_any(py)?
        };
        let mask: Py<PyAny> = if (0..self.len()).any(|row| self.is_row_missing(row)) {
            let bytes: Vec<_> = (0..self.len())
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
            return unsafe { uint64_slice_array(owner, values) };
        }
        uint64_array(owner.py(), borrowed.logical_ids_vec())
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
    ///     opposite-endian numeric ids, gathered ids, and every object export
    ///     raise because they materialize.
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
            CellNumpyArrayKind::Ids { dtype, native } => {
                if !native && copy == Some(false) {
                    return Err(GeometryError::new_err(
                        "gometry cannot return the requested array without copying",
                    ));
                }
                if copy == Some(false) {
                    let can_borrow = slf.borrow().logical_contiguous_ids().is_some();
                    if !can_borrow {
                        return Err(GeometryError::new_err(
                            "gometry cannot return the requested array without copying",
                        ));
                    }
                }
                let array = if copy == Some(true) {
                    let ids = slf.borrow().logical_ids_vec();
                    uint64_array(py, ids)?
                } else {
                    Self::to_numpy(slf)?
                };
                if !native {
                    let dtype = dtype.expect("non-native ids require a requested dtype");
                    return astype_readonly(array.bind(py), dtype.bind(py));
                }
                Ok(array)
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
    pub(crate) fn center(&self, py: Python<'_>) -> PyResult<crate::PyGeometryArray> {
        use crate::grid::cell::GridCell as _;
        let cells = self.clone();
        let points = py.detach(move || map_grid_cells!(&cells, |cell| cell.center_point()));
        let mut builder = CoordSeqBuilder::with_capacity(CoordinateAxes::XY, points.len());
        for point in points {
            builder.push(point);
        }
        let coords = builder.finish()?;
        let result = crate::PyGeometryArray::packed_points(
            coords,
            crate::Frame::Crs(crate::wgs84_crs()),
        )?;
        Ok(match self.missing_mask()? {
            None => result,
            Some(mask) => crate::PyGeometryArray::scatter_present_rows(&result, mask)?,
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
        let cells = self.clone();
        let values = py.detach(move || {
            dispatch_cell_grid!(&cells, G, {
                let mut values = Vec::with_capacity(cells.len());
                for row in 0..cells.len() {
                    values.push(if cells.is_row_missing(row) {
                        f64::NAN
                    } else {
                        G::from_validated_id(cells.id_at(row)).area_m2()
                    });
                }
                values
            })
        });
        float64_array(py, values)
    }

    /// Filled WGS84 polygon of every cell, as a geometry array.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    #[getter]
    pub(crate) fn polygon(&self, py: Python<'_>) -> PyResult<crate::PyGeometryArray> {
        let cells = self.clone();
        let shapes = py.detach(move || {
            dispatch_cell_grid!(&cells, G, {
                cells.map_logical_cells::<G, _>(|cell| {
                    GridCell::boundary_shape(cell).map_err(PyErr::from)
                })
            })
        });
        let shapes = shapes.into_iter().collect::<PyResult<Vec<_>>>()?;
        let result = crate::PyGeometryArray::from_shapes(
            shapes,
            crate::Frame::Crs(crate::wgs84_crs()),
        )?;
        Ok(match self.missing_mask()? {
            None => result,
            Some(mask) => crate::PyGeometryArray::scatter_present_rows(&result, mask)?,
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
            let mut values = Vec::with_capacity(self.len());
            for row in 0..self.len() {
                let value = if self.is_row_missing(row) {
                    f64::NAN
                } else {
                    cell_ops::cell_descendant_count(
                        G::from_validated_id(self.id_at(row)), depth, G::parse_depth,
                    )? as f64
                };
                values.push(value);
            }
            Ok::<_, PyErr>(values)
        })?;
        float64_array(py, values)
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
    #[pyo3(signature = (depth = None, /), text_signature = "($self, depth=None, /)")]
    pub(crate) fn parent(&self, depth: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let kind = self.kind();
        let present = dispatch_cell_grid!(self, G, {
            let mut present = Vec::with_capacity(self.len());
            for cell in self.logical_cells::<G>() {
                present.push(cell_ops::cell_parent(cell, depth, G::parse_depth)?.hash_key());
            }
            Ok::<_, PyErr>(present)
        })?;
        let mut ids = Vec::with_capacity(self.len());
        let mut next = present.into_iter();
        for row in 0..self.len() {
            ids.push(if self.is_row_missing(row) {
                0
            } else {
                next.next().expect("present parent count matches")
            });
        }
        let missing = self.missing_mask()?;
        Ok(Self {
            storage: CellStorage::new(kind, ids, missing),
            selection: RowSelection::Identity,
        })
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
    pub(crate) fn neighbors(&self, py: Python<'_>) -> PyResult<Groups> {
        let kind = self.kind();
        let cells = self.clone();
        let (ids, offsets) = py.detach(move || {
            dispatch_cell_grid!(&cells, G, {
                let mut builder = crate::CsrBuilder::<u64>::new();
                for row in 0..cells.len() {
                    if cells.is_row_missing(row) {
                        builder.push_row(std::iter::empty::<u64>())?;
                        continue;
                    }
                    let cell = G::from_validated_id(cells.id_at(row));
                    builder.push_row(
                        GridCell::neighbors(cell)
                            .into_iter()
                            .map(GridCell::hash_key),
                    )?;
                }
                Ok::<_, PyErr>(builder.finish())
            })
        })?;
        Groups::from_cell_flat(kind, ids, offsets)
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
    #[pyo3(signature = (depth = None, /), text_signature = "($self, depth=None, /)")]
    pub(crate) fn children(
        &self,
        py: Python<'_>,
        depth: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Groups> {
        let kind = self.kind();
        let cells = self.clone();
        let depth = dispatch_cell_grid!(self, G, {
            match cells.logical_cells::<G>().next() {
                Some(_) => {
                    let depth = depth
                        .filter(|value| !value.is_none())
                        .map(G::parse_depth)
                        .transpose()?;
                    if let Some(target) = depth
                        && cells.logical_cells::<G>().any(|cell| target < cell.depth())
                    {
                        return Err(GeometryError::new_err(format!(
                            "children {} must be >= cell {}",
                            G::DEPTH_NAME,
                            G::DEPTH_NAME
                        )));
                    }
                    depth
                },
                None => None,
            }
        });
        let (ids, offsets) = py.detach(move || {
            dispatch_cell_grid!(&cells, G, {
                let mut builder = crate::CsrBuilder::<u64>::new();
                for row in 0..cells.len() {
                    if cells.is_row_missing(row) {
                        builder.push_row(std::iter::empty::<u64>())?;
                        continue;
                    }
                    let cell = G::from_validated_id(cells.id_at(row));
                    let target = depth.or_else(|| {
                        cell.depth()
                            .checked_add(1)
                            .filter(|target| *target <= G::MAX_DEPTH)
                    });
                    let Some(target) = target else {
                        builder.push_row(std::iter::empty::<u64>())?;
                        continue;
                    };
                    builder.push_row(
                        cell.children_to(target)?
                            .into_iter()
                            .map(GridCell::hash_key),
                    )?;
                }
                Ok::<_, PyErr>(builder.finish())
            })
        })?;
        Groups::from_cell_flat(kind, ids, offsets)
    }

    /// Compact this cell set to the coarsest exact covering, returned in the
    /// grid's public identity order. Duplicates and descendants are absorbed;
    /// complete sibling groups merge recursively, stopping at ``depth``.
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
    pub(crate) fn compact(
        &self,
        py: Python<'_>,
        depth: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let kind = self.kind();
        let cells = self.clone();
        let compacted = dispatch_cell_grid!(self, G, {
            let ids = (0..cells.len())
                .filter(|&logical| !cells.is_row_missing(logical))
                .map(|logical| cells.id_at(logical))
                .collect();
            let floor = G::parse_compact_floor(depth)?;
            py.detach(move || G::compact_ids(ids, floor))?
        });
        Ok(Self::from_ids(kind, compacted))
    }

    /// Expand this cell set to a uniform depth, returned in the grid's public
    /// identity order. Expansion is bounded only by checked size arithmetic
    /// and allocator capacity; it has no fixed cell-count limit.
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
    pub(crate) fn uncompact(&self, py: Python<'_>, depth: &Bound<'_, PyAny>) -> PyResult<Self> {
        let kind = self.kind();
        let cells = self.clone();
        let expanded = dispatch_cell_grid!(self, G, {
            let depth = G::parse_depth(depth)?;
            if let Some(cell) = cells.logical_cells::<G>().find(|cell| cell.depth() > depth) {
                return Err(uncompact_floor_error(
                    kind,
                    G::DEPTH_NAME,
                    G::uncompact_floor_token(&cell),
                ));
            }
            py.detach(move || {
                let cells = cells.map_logical_cells::<G, _>(|cell| cell);
                Ok::<_, PyErr>(Self::ids_from_cells(G::uncompact_cells(cells, depth)?))
            })?
        });
        Ok(Self::from_ids(kind, expanded))
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
    fn to_polygon(&self, py: Python<'_>) -> PyResult<crate::Typed> {
        let cells = self.clone();
        py.detach(move || match cells.kind() {
            GridKind::H3Cell => {
                let cells = cells.map_logical_cells::<h3o::CellIndex, _>(|cell| cell);
                super::h3::dissolve_cells(cells)
            },
            GridKind::S2Cell => {
                let cells = cells.map_logical_cells::<CellId, _>(|cell| cell);
                super::s2::s2_dissolve(&cells)
            },
            GridKind::Tile => {
                let cells = cells.map_logical_cells::<Tile, _>(|cell| cell);
                rect_cells_to_polygon(cells)
            },
            GridKind::GeohashCell => {
                let cells = cells.map_logical_cells::<Geohash, _>(|cell| cell);
                rect_cells_to_polygon(cells)
            },
        })
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
        let tokens = match self.kind() {
            GridKind::H3Cell => self.tokens::<h3o::CellIndex>(),
            GridKind::S2Cell => self.tokens::<CellId>(),
            GridKind::Tile => self.tokens::<Tile>(),
            GridKind::GeohashCell => return self.geohash_tokens(py),
        };
        tokens.into_py_any(py)
    }

}
}

impl PyCellArray {
    fn tokens<G: CellArrayGrid>(&self) -> Vec<Option<String>> {
        let mut tokens = Vec::with_capacity(self.len());
        for row in 0..self.len() {
            let token = if self.is_row_missing(row) {
                None
            } else {
                Some(G::from_validated_id(self.id_at(row)).token())
            };
            tokens.push(token);
        }
        tokens
    }

    fn geohash_tokens(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let tokens = PyList::empty(py);
        for row in 0..self.len() {
            if self.is_row_missing(row) {
                tokens.append(py.None())?;
                continue;
            }
            let cell = Geohash::from_validated_id(self.id_at(row));
            let (bytes, length) = cell.token_bytes();
            let token = std::str::from_utf8(&bytes[..length]).expect("geohash alphabet is ASCII");
            tokens.append(PyString::new(py, token))?;
        }
        Ok(tokens.into_any().unbind())
    }
}

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
/// Rebuild a pickled CellArray from its public identities and logical mask.
#[pyfunction]
pub(crate) fn _unpickle_cell_array_v1(
    ids: &Bound<'_, PyAny>,
    grid: &str,
    mask: &Bound<'_, PyAny>,
) -> PyResult<PyCellArray> {
    let kind = GridKind::parse(grid)?;
    let (ids, missing) = if mask.is_none() {
        let collected = super::grid_kind::collect_ids(ids, kind)?;
        (collected.ids, collected.missing)
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
        if present.missing.is_some() {
            return Err(PyValueError::new_err(
                "CellArray pickle identities must not contain missing rows",
            ));
        }
        let present = present.ids;
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
        let missing = crate::array::ValidityMask::from_missing_iter(
            bytes.len(),
            bytes.iter().map(|&value| value == 1),
        )?;
        (logical_ids, missing)
    };
    Ok(PyCellArray {
        storage: CellStorage::new(kind, ids, missing),
        selection: RowSelection::Identity,
    })
}
use pyo3::IntoPyObjectExt as _;

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;

    use super::{GridKind, PyCellArray};

    fn h3_id(lat: f64, lon: f64, resolution: u8) -> u64 {
        u64::from(
            h3o::LatLng::new(lat, lon)
                .expect("finite latlng")
                .to_cell(h3o::Resolution::try_from(resolution).expect("resolution")),
        )
    }

    #[test]
    fn empty_missing_and_max_depth_neighbors_children_succeed() {
        Python::initialize();
        Python::attach(|py| {
            let empty = PyCellArray::from_ids(GridKind::H3Cell, Vec::new());
            match empty.neighbors(py) {
                Ok(groups) => drop(groups),
                Err(error) => panic!("{error}"),
            }
            match empty.children(py, None) {
                Ok(groups) => drop(groups),
                Err(error) => panic!("{error}"),
            }

            let present = h3_id(37.7749, -122.4194, 7);
            let missing =
                PyCellArray::from_ids_with_missing(GridKind::H3Cell, vec![present, 0], vec![
                    false, true,
                ])
                .unwrap();
            match missing.neighbors(py) {
                Ok(groups) => drop(groups),
                Err(error) => panic!("{error}"),
            }

            let max = PyCellArray::from_ids(GridKind::H3Cell, vec![h3_id(37.7749, -122.4194, 15)]);
            match max.children(py, None) {
                Ok(groups) => drop(groups),
                Err(error) => panic!("{error}"),
            }
        });
    }

    #[test]
    fn cell_array_heap_bytes_counts_ids_and_missing_flags_once() {
        use crate::HeapSize as _;

        let cells = PyCellArray::from_ids_with_missing(GridKind::H3Cell, vec![1, 2, 3], vec![
            false, true, false,
        ])
        .unwrap();
        let arc = crate::heap_size::ARC_CONTROL_BYTES;
        assert_eq!(
            cells.heap_bytes(),
            arc + size_of::<Vec<u64>>() + 3 * size_of::<u64>() + arc + size_of::<Vec<u8>>() + 1
        );

        let selected = cells.slice(1, 2, 1).unwrap();
        assert_eq!(
            selected.heap_bytes(),
            arc + size_of::<Vec<u64>>() + size_of::<u64>() + arc + size_of::<Vec<u8>>() + 1
        );
    }
}
