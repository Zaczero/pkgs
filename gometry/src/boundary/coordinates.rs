//! Shape-wide coordinate access — the structural `SoA` view behind
//! `geom.coords`.
//!
//! The view traverses nested rings and collections in canonical depth-first
//! order, and borrows the underlying [`CoordSeq`] columns directly when the
//! view is one contiguous run so per-axis access stays zero-copy.
//!
//! Packed array owners retain their row selection and missing-value semantics.

use std::ops::Range;
use std::sync::Arc;

use pyo3::exceptions::PyOverflowError;
use pyo3::{PyResult, Python};

use crate::array::RingLevel;
use crate::geometry::storage::{ArrayRows, GeometryArrayStorage, ValidityMask};
use crate::geometry::{CoordSeq, CsrOffsetColumn, Point, Polygon, Shape, ShapeData};

pub(crate) mod count;
mod cursor;
mod equality;
mod integer;
pub(crate) mod payload;
mod point_access;
#[cfg(test)]
mod tests;
mod view;

use payload::{ExactPayloadBytes, PayloadBytesQuery};

#[derive(Default)]
pub(crate) struct CoordinateMeasureQuery<'py> {
    collections: count::CollectionMeasures<'py>,
}

pub(crate) fn measure_shape<'py>(
    py: Python<'py>,
    shape: &Shape,
) -> PyResult<count::CoordinateMeasure<'py>> {
    CoordinateMeasureQuery::default().shape(py, shape)
}

pub(crate) fn measure_storage<'py>(
    py: Python<'py>,
    storage: &GeometryArrayStorage,
    missing: Option<&ValidityMask>,
) -> PyResult<count::CoordinateMeasure<'py>> {
    let logical_len = missing.map_or_else(|| storage.len(), ValidityMask::len);
    measure_storage_range(py, storage, missing, 0..logical_len)
}

pub(crate) fn measure_storage_range<'py>(
    py: Python<'py>,
    storage: &GeometryArrayStorage,
    missing: Option<&ValidityMask>,
    rows: Range<usize>,
) -> PyResult<count::CoordinateMeasure<'py>> {
    // ShapeOwnerKey holds addresses into these owners; keep its memo inside the
    // borrow that names them instead of returning a reusable query.
    let mut query = CoordinateMeasureQuery::default();
    query.storage_range(py, storage, missing, rows)
}

pub(crate) fn measure_storage_payload_bytes<'py>(
    py: Python<'py>,
    storage: &GeometryArrayStorage,
    missing: Option<&ValidityMask>,
    rows: Range<usize>,
) -> PyResult<ExactPayloadBytes<'py>> {
    // ShapeOwnerKey addresses storage-backed members, so the memo cannot
    // outlive the storage and mask borrowed for this range.
    let mut query = PayloadBytesQuery::default();
    query.storage_range(py, storage, missing, rows)
}

pub(crate) fn checked_vector_len<T>(count: usize, label: &str) -> PyResult<usize> {
    let max_len = (isize::MAX as usize)
        .checked_div(std::mem::size_of::<T>())
        .unwrap_or(usize::MAX);
    if count > max_len {
        return Err(PyOverflowError::new_err(format!(
            "{label} output is too large to materialize"
        )));
    }
    Ok(count)
}

/// One ordinate in a coordinate layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CoordinateAxis {
    X,
    Y,
    Z,
    M,
}

/// A per-axis column resolved from a view: borrowed when the view is one
/// contiguous run, materialized otherwise.
pub(crate) enum CoordinateColumnRef<'a> {
    /// A borrowed contiguous `f64` column.
    Borrowed(&'a [f64]),
    /// Dense but gathered across runs (X/Y over a multipart view).
    Dense(Vec<f64>),
    /// Z/M over a view where some coordinates lack the ordinate; `None` where
    /// absent.
    Nullable(Vec<Option<f64>>),
    /// No coordinate in the view carries this ordinate.
    Missing,
}

/// Owner handle for a coordinate view. Both variants are immutable and cheap to
/// clone; the `Arc` lets the view outlive the borrow that produced it without
/// copying coordinates. Packed [`crate::geometry::storage::GeometryArrayStorage`] variants sit behind
/// the `Array` owner.
#[derive(Clone, Debug)]
enum CoordinateOwner {
    Shape(Arc<ShapeData>),
    Array(ArrayRows),
}

/// The geometry source behind a view, for nested (topology-shaped) coordinate
/// rendering that the flat columns intentionally do not preserve.
pub(crate) enum CoordinateSource<'a> {
    Shape(&'a Shape),
    Array(&'a ArrayRows),
}

/// Shape-wide coordinate access. Flattening order matches
/// [`Shape::points`](crate::geometry::Shape::points): point, then each part /
/// ring in declaration order, exterior ring before interiors. An array owner
/// flattens its geometries in row order, each coordinate tagged with its row.
#[derive(Clone, Debug)]
pub struct CoordinateView {
    owner: CoordinateOwner,
}

enum CoordinateFrame {
    Shape(Shape),
    Sequence {
        coordinates: CoordSeq,
        remaining: std::ops::Range<usize>,
    },
    Collection {
        shape: Shape,
        remaining: std::ops::Range<usize>,
    },
    MultiLines {
        shape: Shape,
        remaining: std::ops::Range<usize>,
    },
    MultiPolygons {
        shape: Shape,
        remaining: std::ops::Range<usize>,
    },
    PolygonRings {
        polygon: Polygon,
        remaining: std::ops::Range<usize>,
    },
    ArrayRows {
        rows: ArrayRows,
        remaining: std::ops::Range<usize>,
        compact_point_rank: Option<usize>,
    },
    PackedRings {
        coordinates: Arc<CoordSeq>,
        offsets: CsrOffsetColumn<RingLevel>,
        remaining: std::ops::Range<usize>,
    },
}

enum CoordinateStep {
    Continue,
    Point(Point),
    Push(CoordinateFrame),
    Pop,
}

/// Incremental depth-first traversal that never needs a flattened count or a
/// materialized point vector.
pub(crate) struct CoordinatePointCursor {
    view: CoordinateView,
    frames: Vec<CoordinateFrame>,
    reverse: bool,
}
