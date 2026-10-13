#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
//! Typed bulk cell storage — the CellArray Python surface.
use std::ops::Range;

use numpy::PyArrayMethods as _;
use pyo3::exceptions::{PyOverflowError, PyValueError};
use pyo3::prelude::{PyResult, Python, pyclass};
use pyo3::types::{
    PyAny, PyBytes, PyBytesMethods as _, PyList, PyListMethods as _, PyString, PyTuple, PyType,
};
use pyo3::{Bound, Py, PyErr};

use crate::HeapSize;
use crate::broadcast::py_bool_or_not_implemented;
use crate::collections::{HashMap, HashMapExt as _};
use crate::geometry::storage::validity::ValidityMask;
use crate::geometry::storage::{
    RowSelection, RowSelectionRef, physical_row, row_selection_from_logical_rows,
};
use crate::geometry::{CoordSeq, storage};
use crate::grid::cell::{GridCell, HierarchicalCell as _};
use crate::grid::cell_set;
use crate::grid::geohash::{GEOHASH_MAX_PRECISION, Geohash};
use crate::grid::s2::cellid::CellId;
use crate::grid::tile::{TILE_MAX_ZOOM, Tile};
use crate::py::arrow_c;
use crate::py::cells::grid_kind::{
    collect_ids, collect_inferred_ids, grid_kind_from_type, grid_kind_from_value, is_raw_identity,
    membership_id,
};
use crate::py::cells::{
    FactorizationError, GeometryError, GridKind, H3_MAX_RESOLUTION, IntoPyObject as _,
    PyAnyMethods as _, PyCellArrayIter, PyRef, cell_ops, checked_depth, py_i64_required,
    pyfunction, rect_cells_to_polygon, sort_value_counts, uncompact_floor_error,
};
use crate::py::numpy::{
    astype_readonly, bool_array, float64_array, int64_array, numpy_module, uint64_array,
    uint64_slice_array,
};
use crate::py::row::{RowContainer, RowGetItemContainer, array_getitem, slice_row_indices};
use crate::py::sizeof::python_sizeof;
use crate::py::vectors::Groups;

/// An immutable array of one grid cell type backed by a shared ``uint64`` id
/// column.
///
/// Index with an integer for the typed cell object or `None` for a missing row;
/// slice or mask for a new `CellArray`. Build from an iterable containing
/// typed cell objects and `None`; the grid is inferred from the first concrete
/// cell. Raw ids for H3, S2, and Tile and tokens for any grid may accompany a
/// typed cell of the same grid. Geohash cells use tokens, not integer ids. For
/// raw-only values, numeric arrays or buffers, empty input, or all-missing
/// input, pass ``type=`` explicitly; every id and token is validated.
///
/// Parameters
/// ----------
/// values : iterable, numpy.ndarray, or buffer
///     Typed cell objects and ``None``; raw ids for H3/S2/Tile and tokens for
///     any grid are accepted when a concrete cell supplies the family.
/// type : type
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
    missing: Option<storage::ValidityMask>,
}

impl CellStorage {
    fn new(kind: GridKind, ids: Vec<u64>, missing: Option<storage::ValidityMask>) -> Self {
        debug_assert!(missing.as_ref().is_none_or(|mask| mask.len() == ids.len()));
        Self {
            kind,
            ids: crate::geometry::SharedSlice::from_vec(ids),
            missing,
        }
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

use hierarchy::CellArrayGrid;

impl PyCellArray {
    pub(crate) fn from_ids(kind: GridKind, ids: Vec<u64>) -> Self {
        Self {
            storage: CellStorage::new(kind, ids, None),
            selection: RowSelection::Identity,
        }
    }

    /// Build with a parallel missing mask (one flag per id). Used by bulk
    /// factories that degrade missing geometry rows instead of rejecting them.
    pub(crate) fn from_ids_with_missing(kind: GridKind, ids: Vec<u64>, missing: Vec<bool>) -> Self {
        let missing = storage::ValidityMask::from_vec(ids.len(), missing);
        Self {
            storage: CellStorage::new(kind, ids, missing),
            selection: RowSelection::Identity,
        }
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
                .saturating_add(size_of::<Vec<u64>>())
                .saturating_add(range.len().saturating_mul(size_of::<u64>()))
        };
        let missing = self.storage.missing.as_ref().map_or(0, |_| {
            crate::heap_size::ARC_CONTROL_BYTES
                .saturating_add(size_of::<Vec<u8>>())
                .saturating_add(range.len().div_ceil(8))
        });
        ids.saturating_add(missing)
    }

    /// The grid kind of these cells.
    pub(crate) const fn kind(&self) -> GridKind {
        self.storage.kind
    }

    /// A contiguous logical sub-range as a new array (zero-copy selection).
    /// Used by [`crate::py::vectors::Groups`] to view one ragged row of cells.
    pub(crate) fn logical_row_range(&self, range: Range<usize>) -> Self {
        self.select_logical_rows(range)
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
        *self
            .storage
            .ids
            .get(physical_row(self.selection_ref(), logical))
            .expect("logical cell row maps to a stored id")
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
            self.storage.ids.len(),
            rows,
        ))
    }

    fn slice(&self, start: isize, stop: isize, step: isize, count: usize) -> Self {
        if let Some(logical) = CoordSeq::contiguous_positive_slice(start, stop, step) {
            let selection = row_selection_from_logical_rows(
                self.selection_ref(),
                self.storage.ids.len(),
                logical,
            );
            return self.with_selection(selection);
        }
        self.select_logical_rows(slice_row_indices(start, step, count))
    }

    fn logical_contiguous_ids(&self) -> Option<&[u64]> {
        match self.selection_ref() {
            RowSelectionRef::Identity => Some(&self.storage.ids),
            RowSelectionRef::Window { start, len } => {
                self.storage.ids.get(start..).and_then(|ids| ids.get(..len))
            },
            RowSelectionRef::Gather(_) => None,
        }
    }

    fn logical_ids_vec(&self) -> Vec<u64> {
        self.logical_id_iter().collect()
    }

    fn logical_id_iter(&self) -> impl ExactSizeIterator<Item = u64> + '_ {
        (0..self.len()).map(|logical| self.id_at(logical))
    }

    fn logical_cells<'a, G: CellArrayGrid + 'a>(&'a self) -> impl Iterator<Item = G> + 'a {
        (0..self.len())
            .filter(|&logical| !self.is_row_missing(logical))
            .map(|logical| G::from_validated_id(self.id_at(logical)))
    }

    fn missing_mask_and_present_count(&self) -> (Option<ValidityMask>, usize) {
        let missing = if matches!(self.selection_ref(), RowSelectionRef::Identity) {
            self.storage.missing.clone()
        } else {
            ValidityMask::from_missing_iter(
                self.len(),
                (0..self.len()).map(|row| self.is_row_missing(row)),
            )
        };
        let present_count = missing
            .as_ref()
            .map_or(self.len(), ValidityMask::present_count);
        (missing, present_count)
    }

    fn map_logical_cells<G, T>(&self, mut f: impl FnMut(G) -> T) -> Vec<T>
    where
        G: CellArrayGrid,
    {
        self.try_map_logical_cells::<G, _, std::convert::Infallible>(Vec::new(), |cell| Ok(f(cell)))
            .expect("infallible cell mapping")
    }

    fn try_map_logical_cells<G, T, E>(
        &self,
        mut output: Vec<T>,
        mut f: impl FnMut(G) -> Result<T, E>,
    ) -> Result<Vec<T>, E>
    where
        G: CellArrayGrid,
    {
        for row in 0..self.len() {
            if self.is_row_missing(row) {
                continue;
            }
            output.push(f(G::from_validated_id(self.id_at(row)))?);
        }
        Ok(output)
    }
}

mod factorize;
mod geometry;
mod hierarchy;
mod interchange;
mod methods;
#[path = "array/storage.rs"]
mod row_storage;
#[cfg(test)]
mod tests;

pub(crate) use interchange::_unpickle_cell_array_v1;
use interchange::CellNumpyArrayKind;

use super::{geohash, grid_kind, h3, s2, tiles};
