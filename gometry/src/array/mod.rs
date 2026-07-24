#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! The `GeometryArray` Python method surface (`#[pymethods] impl
//! PyGeometryArray`).
//!
//! Cohesive `#[pymethods]` blocks are split across files beside the packed
//! array storage they operate on. Broadcast dispatch and sibling crate types
//! enter through `use super::*`; methods auto-register with the
//! `GeometryArray` class wherever the impl is compiled.

pub(crate) use methods_indexing::{
    CollectedFancyIndex, NumpyFancyIndex, classify_and_collect_fancy_index, is_bool_scalar,
    numpy_fancy_index,
};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyTuple};

use super::*;
// Child `#[pymethods]` modules glob `use super::*`; re-export imports here so
// they are parent-defined names (ancestor `use` alone is not globbed).
pub(crate) use crate::broadcast::{CollectRows, rows_err};
use crate::error::{Error, Result};
use crate::geometry::CsrOffsetColumn;
pub(crate) use crate::geometry::{
    CsrOffsetBuilder, PolygonLevel, RingLevel, affine_about, concat_coord_columns,
    line_crosses_antimeridian, line_is_ccw, line_is_closed, line_is_simple, line_is_valid,
    polygon_is_valid, polygon_row_point_membership,
};
pub(crate) use crate::py::errors::{CRSError, GeometryError, InvalidGeometryError};
use crate::py::numpy::bool_array;

/// An immutable array of geometries sharing one CRS/epoch frame.
///
/// The vectorized counterpart of ``Geometry``: methods mirror the scalar
/// names element-wise (``arr.area``, ``(arr).buffer(10.0)``,
/// ``contains(arr, geom)``) and run as batched Rust kernels with no
/// per-element Python. Homogeneous point, line, and polygon arrays pack into
/// shared coordinate columns (with CSR offsets where needed) for zero-copy
/// Arrow and columnar kernels. Build with ``GeometryArray(...)`` /
/// ``points(...)`` or the WKT/WKB/GeoJSON/Arrow importers; index and
/// slice it like a sequence.
#[derive(Clone, Debug)]
enum ArrayRows {
    /// Every logical row is present. This is the allocation-free hot path.
    Dense(Arc<GeometryArrayStorage>),
    /// Storage and its row-aligned missingness are one indivisible state.
    Masked {
        storage: Arc<GeometryArrayStorage>,
        missing: MissingMask,
    },
}

impl ArrayRows {
    fn new(storage: Arc<GeometryArrayStorage>, missing: Option<MissingMask>) -> Self {
        match missing {
            Some(missing) if missing.any() => {
                assert_eq!(
                    storage.len(),
                    missing.len(),
                    "missing mask length must match array length"
                );
                Self::Masked { storage, missing }
            },
            Some(missing) => {
                assert_eq!(
                    storage.len(),
                    missing.len(),
                    "missing mask length must match array length"
                );
                Self::Dense(storage)
            },
            None => Self::Dense(storage),
        }
    }

    const fn storage(&self) -> &Arc<GeometryArrayStorage> {
        match self {
            Self::Dense(storage) | Self::Masked { storage, .. } => storage,
        }
    }

    const fn missing(&self) -> Option<&MissingMask> {
        match self {
            Self::Dense(_) => None,
            Self::Masked { missing, .. } => Some(missing),
        }
    }

    fn with_missing(&self, missing: Option<MissingMask>) -> Self {
        Self::new(Arc::clone(self.storage()), missing)
    }
}

#[pyclass(
    name = "GeometryArray",
    module = "gometry",
    frozen,
    sequence,
    generic,
    weakref,
    skip_from_py_object
)]
#[derive(Clone, Debug)]
/// An immutable, vectorized geometry column with one shared CRS/epoch frame.
///
/// Homogeneous arrays use packed coordinate storage and batched Rust kernels;
/// indexing and slicing preserve zero-copy views where possible.
pub struct PyGeometryArray {
    /// The single source of truth for row storage and row-aligned missingness.
    /// Masked slots contain rectangular packed placeholders, but callers can
    /// never replace storage and its mask independently.
    rows: ArrayRows,
    /// The coordinate reference frame shared by every row (CRS + optional
    /// epoch). One `Frame` makes epoch-without-CRS unrepresentable (#37); the
    /// `crs`/`epoch` Python getters and the `crs_ref`/`crs_str` internal
    /// accessors derive from it.
    pub(crate) frame: Frame,
    /// Lazy per-element bounding boxes, shared across clones — built ONCE from
    /// the packed columns and reused by every predicate refutation, mirroring
    /// shapely's per-geometry envelope cache (the array is frozen, never
    /// stale). `Arc<OnceLock>` so cloning the handle shares the cache.
    pub(crate) bounds_cache: Arc<std::sync::OnceLock<Option<ElementBounds>>>,
    /// Lazy per-row prepared handles for packed line/polygon rows — shared
    /// across clones so repeated predicates reuse facet trees and point
    /// testers.
    pub(crate) prepared_cache: prepared::PreparedRowCache,
    /// Frame-owned per-row geodesic/LRS products. Row slots are reference
    /// counted so clones, slices, gathers, and scalar extraction share the
    /// same lazy cache without attaching CRS-derived state to coordinates.
    pub(crate) frame_caches: prepared::FrameCacheRows,
    /// Lazy materialization of a fancy-selected (`RowSelection::Gather`)
    /// storage, shared across clones like `bounds_cache`. Filter-then-compute
    /// workflows (`arr[mask].area` + `.length` + ...) resolve the gather ONCE;
    /// every later packed-column op reuses the contiguous columns zero-copy.
    /// `None`-typed until the first packed op on a gathered array; Identity/
    /// Window storages never populate it (they are already zero-copy views).
    pub(crate) gathered_memo: Arc<std::sync::OnceLock<Arc<GeometryArrayStorage>>>,
}

/// Cached per-element bounding boxes (one entry per row, `None` for empty
/// rows).
#[expect(
    clippy::redundant_pub_crate,
    reason = "crate-visible for the predicate broadcast dispatch"
)]
pub(crate) type ElementBounds = Arc<[Option<Bounds>]>;
mod linref_kernels;
mod masked;
mod methods_convert;
mod methods_core;
mod methods_coverage;
mod methods_distance;
mod methods_indexing;
mod methods_interop;
mod methods_measures;
mod methods_predicates;
mod methods_topology;
pub(crate) mod missing;
pub(crate) use missing::MissingMask;

mod methods_unary_io;
mod ops;
pub(crate) use linref_kernels::{line_interpolate_points_rows, line_locate_point_on_lines};
mod iter;
pub(crate) use iter::PyGeometryArrayIter;
mod prepared;
pub(crate) use prepared::{FrameCacheRows, fresh_prepared_cache};
mod storage_helpers;
mod storage_types;
pub(crate) use packed_column_builder::{PackedColumnBuilder, PointColumnBuilder};
pub(crate) use storage_helpers::{
    column_window, packed_centroid_xy, packed_surface_point, row_bounds, row_bounds_3d,
    row_bounds_values, row_ord_extremes,
};
pub(crate) use storage_impl::reverse_coord_windows;
mod packed;
mod packed_column_kernels;
mod packed_columns;
mod packed_gather;
pub(crate) use packed_column_kernels::{
    bounds_3d_values_from_columns, bounds_values_from_columns_masked,
    ensure_geographic_columns_present, geographic_bounds_values_from_columns, line_measure_masked,
    map_coordseq_to_crs, packed_per_row_self_origin_affine_columns, polygon_measure_masked,
    reduce_lines_or_polygons, subdivide_line_columns, subdivide_polygon_columns,
    total_bounds_from_columns,
};
pub(crate) use packed_columns::{PackedColumnError, PackedColumnOutput, PackedColumns};
mod pack_admission;
mod packed_builders;
mod packed_column_builder;
mod packed_equal;
mod packed_ops;
mod streaming;
mod uniform_columns;
pub(crate) use pack_admission::{
    packable_closed_ring_len, polygon_pack_axes, ring_seq_is_packable,
};
pub(crate) use packed::sparse_missing_mask;
pub(crate) use streaming::StreamingShapes;
mod shape_row;
mod storage_impl;
pub(crate) use packed_equal::pair_packed_equals_exact;
mod tests;

pub(crate) use shape_row::ShapeRow;
pub(crate) use storage_types::*;
