//! Shape-wide coordinate access — the structural `SoA` view behind
//! `geom.coords`.
//!
//! The view traverses nested rings and collections in canonical depth-first
//! order, and borrows the underlying [`CoordSeq`] columns directly when the
//! view is one contiguous run so per-axis access stays zero-copy.
//!
//! Packed array owners retain their row selection and missing-value semantics.

use std::collections::HashMap;
use std::convert::Infallible;
use std::ops::{ControlFlow, Range};
use std::sync::Arc;

use pyo3::exceptions::PyOverflowError;
use pyo3::types::{PyAnyMethods as _, PyInt};
use pyo3::{Bound, PyResult, Python};

use crate::GeometryArrayStorage;
use crate::array::RingLevel;
use crate::geometry::storage::{PackedStorage, RowSelectionRef, ValidityMask, csr_value_window};
use crate::geometry::types::ShapeOwnerKey;
use crate::geometry::{
    CoordRun, CoordSeq, CoordWindow, CoordinateAxes, CsrOffsetColumn, MOrdinate, Point, Polygon,
    Shape, ShapeData, ZOrdinate, storage,
};

#[derive(Clone)]
enum ExactUnsigned<'py> {
    Small(u128),
    Big(Bound<'py, PyInt>),
}

impl<'py> ExactUnsigned<'py> {
    const ZERO: Self = Self::Small(0);

    fn add(&self, py: Python<'py>, other: &Self) -> PyResult<Self> {
        match (self, other) {
            (Self::Small(left), Self::Small(right)) => left.checked_add(*right).map_or_else(
                || {
                    let left = PyInt::new(py, *left);
                    let right = PyInt::new(py, *right);
                    Self::add_python_ints(&left, &right)
                },
                |sum| Ok(Self::Small(sum)),
            ),
            (Self::Big(left), Self::Small(right)) => {
                let right = PyInt::new(py, *right);
                Self::add_python_ints(left, &right)
            },
            (Self::Small(left), Self::Big(right)) => {
                let left = PyInt::new(py, *left);
                Self::add_python_ints(&left, right)
            },
            (Self::Big(left), Self::Big(right)) => Self::add_python_ints(left, right),
        }
    }

    fn add_python_ints(left: &Bound<'py, PyInt>, right: &Bound<'py, PyInt>) -> PyResult<Self> {
        add_python_integers(left, right).map(Self::Big)
    }

    fn into_pyint(self, py: Python<'py>) -> Bound<'py, PyInt> {
        match self {
            Self::Small(value) => usize::try_from(value)
                .map_or_else(|_| PyInt::new(py, value), |value| PyInt::new(py, value)),
            Self::Big(value) => value,
        }
    }

    const fn fits_py_ssize(&self) -> Option<usize> {
        match self {
            Self::Small(value) if *value <= isize::MAX as u128 => Some(*value as usize),
            Self::Small(_) | Self::Big(_) => None,
        }
    }

    fn equals(&self, py: Python<'py>, other: &Self) -> PyResult<bool> {
        match (self, other) {
            (Self::Small(left), Self::Small(right)) => Ok(left == right),
            (Self::Big(left), Self::Big(right)) => left.as_any().eq(right.as_any()),
            (Self::Big(left), Self::Small(right)) => left.as_any().eq(PyInt::new(py, *right)),
            (Self::Small(left), Self::Big(right)) => right.as_any().eq(PyInt::new(py, *left)),
        }
    }
}

#[derive(Clone)]
enum ExactPayloadValue<'py> {
    Small(usize),
    Wide(u128),
    Big(Bound<'py, PyInt>),
}

fn add_python_integers<'py>(
    left: &Bound<'py, PyInt>,
    right: &Bound<'py, PyInt>,
) -> PyResult<Bound<'py, PyInt>> {
    Ok(left.as_any().add(right.as_any())?.cast_into::<PyInt>()?)
}

#[derive(Clone)]
pub(crate) struct ExactPayloadBytes<'py>(ExactPayloadValue<'py>);

impl<'py> ExactPayloadBytes<'py> {
    const ZERO: Self = Self(ExactPayloadValue::Small(0));

    const fn from_native_bytes(bytes: usize) -> Self {
        Self(ExactPayloadValue::Small(bytes))
    }

    const fn from_wide_bytes(bytes: u128) -> Self {
        Self(ExactPayloadValue::Wide(bytes))
    }

    fn from_count_and_axes(count: u128, axes: CoordinateAxes) -> Self {
        let width = u128::try_from(axes.byte_width(1)).expect("coordinate byte width fits u128");
        if let Ok(count) = usize::try_from(count)
            && let Some(bytes) =
                count.checked_mul(usize::try_from(width).expect("f64 axes fit usize"))
        {
            return Self(ExactPayloadValue::Small(bytes));
        }
        Self(ExactPayloadValue::Wide(count.wrapping_mul(width)))
    }

    fn from_native_count_and_axes(count: usize, axes: CoordinateAxes) -> Self {
        let width = axes.byte_width(1);
        if let Some(bytes) = count.checked_mul(width) {
            return Self(ExactPayloadValue::Small(bytes));
        }
        let count = u128::try_from(count).expect("usize coordinate count fits u128");
        let width = u128::try_from(width).expect("f64 axes fit u128");
        Self(ExactPayloadValue::Wide(count.wrapping_mul(width)))
    }

    fn add(&self, py: Python<'py>, other: &Self) -> PyResult<Self> {
        match (&self.0, &other.0) {
            (ExactPayloadValue::Small(left), ExactPayloadValue::Small(right)) => {
                Ok(Self(left.checked_add(*right).map_or_else(
                    || {
                        // Two usize operands fit in u128 even when their sum
                        // does not fit in usize.
                        ExactPayloadValue::Wide(
                            u128::try_from(*left)
                                .expect("usize byte count fits u128")
                                .wrapping_add(
                                    u128::try_from(*right).expect("usize byte count fits u128"),
                                ),
                        )
                    },
                    ExactPayloadValue::Small,
                )))
            },
            (ExactPayloadValue::Small(left), ExactPayloadValue::Wide(right)) => Self::add_wide(
                py,
                u128::try_from(*left).expect("usize byte count fits u128"),
                *right,
            ),
            (ExactPayloadValue::Wide(left), ExactPayloadValue::Small(right)) => Self::add_wide(
                py,
                *left,
                u128::try_from(*right).expect("usize byte count fits u128"),
            ),
            (ExactPayloadValue::Wide(left), ExactPayloadValue::Wide(right)) => {
                Self::add_wide(py, *left, *right)
            },
            (ExactPayloadValue::Big(left), ExactPayloadValue::Small(right)) => {
                let right = PyInt::new(py, *right);
                add_python_integers(left, &right)
                    .map(ExactPayloadValue::Big)
                    .map(Self)
            },
            (ExactPayloadValue::Big(left), ExactPayloadValue::Wide(right)) => {
                let right = PyInt::new(py, *right);
                add_python_integers(left, &right)
                    .map(ExactPayloadValue::Big)
                    .map(Self)
            },
            (ExactPayloadValue::Big(left), ExactPayloadValue::Big(right)) => {
                add_python_integers(left, right)
                    .map(ExactPayloadValue::Big)
                    .map(Self)
            },
            (ExactPayloadValue::Small(left), ExactPayloadValue::Big(right)) => {
                let left = PyInt::new(py, *left);
                add_python_integers(&left, right)
                    .map(ExactPayloadValue::Big)
                    .map(Self)
            },
            (ExactPayloadValue::Wide(left), ExactPayloadValue::Big(right)) => {
                let left = PyInt::new(py, *left);
                add_python_integers(&left, right)
                    .map(ExactPayloadValue::Big)
                    .map(Self)
            },
        }
    }

    fn add_wide(py: Python<'py>, left: u128, right: u128) -> PyResult<Self> {
        left.checked_add(right).map_or_else(
            || {
                let left = PyInt::new(py, left);
                let right = PyInt::new(py, right);
                add_python_integers(&left, &right)
                    .map(ExactPayloadValue::Big)
                    .map(Self)
            },
            |sum| Ok(Self(ExactPayloadValue::Wide(sum))),
        )
    }

    pub(crate) fn add_native_bytes(self, py: Python<'py>, bytes: usize) -> PyResult<Self> {
        self.add(py, &Self(ExactPayloadValue::Small(bytes)))
    }

    pub(crate) fn into_pyint(self, py: Python<'py>) -> Bound<'py, PyInt> {
        match self.0 {
            ExactPayloadValue::Small(value) => PyInt::new(py, value),
            ExactPayloadValue::Wide(value) => usize::try_from(value)
                .map_or_else(|_| PyInt::new(py, value), |value| PyInt::new(py, value)),
            ExactPayloadValue::Big(value) => value,
        }
    }

    fn as_usize(&self) -> Option<usize> {
        match &self.0 {
            ExactPayloadValue::Small(value) => Some(*value),
            ExactPayloadValue::Wide(value) => usize::try_from(*value).ok(),
            ExactPayloadValue::Big(_) => None,
        }
    }
}

/// Exact flattened coordinate count and stored ordinate payload for one view.
///
/// Flat packed columns stay in checked `u128`. Nested shared collections
/// promote only after that bound is crossed, using Python integers while the
/// owning metadata query is attached to Python.
#[derive(Clone)]
pub(crate) struct CoordinateMeasure<'py> {
    coordinates: ExactUnsigned<'py>,
    bytes: ExactUnsigned<'py>,
    shape_axes: CoordinateAxes,
    present_axes: CoordinateAxes,
}

impl<'py> CoordinateMeasure<'py> {
    const ZERO: Self = Self {
        coordinates: ExactUnsigned::ZERO,
        bytes: ExactUnsigned::ZERO,
        shape_axes: CoordinateAxes::XY,
        present_axes: CoordinateAxes::XY,
    };

    fn from_count_and_axes(count: u128, axes: CoordinateAxes) -> Self {
        let width = u128::try_from(axes.byte_width(1)).expect("coordinate byte width fits u128");
        let present_axes = if count == 0 { CoordinateAxes::XY } else { axes };
        Self {
            coordinates: ExactUnsigned::Small(count),
            bytes: ExactUnsigned::Small(
                count
                    .checked_mul(width)
                    .expect("one stored coordinate sequence fits u128"),
            ),
            shape_axes: axes,
            present_axes,
        }
    }

    const fn with_shape_axes(mut self, axes: CoordinateAxes) -> Self {
        self.shape_axes = axes;
        self
    }

    const fn present_only(mut self) -> Self {
        self.shape_axes = CoordinateAxes::XY;
        self
    }

    fn add(&self, py: Python<'py>, other: &Self) -> PyResult<Self> {
        Ok(Self {
            coordinates: self.coordinates.add(py, &other.coordinates)?,
            bytes: self.bytes.add(py, &other.bytes)?,
            shape_axes: self.shape_axes.union(other.shape_axes),
            present_axes: self.present_axes.union(other.present_axes),
        })
    }

    pub(crate) fn count_pyint(self, py: Python<'py>) -> Bound<'py, PyInt> {
        self.coordinates.into_pyint(py)
    }

    pub(crate) fn bytes_pyint(self, py: Python<'py>) -> Bound<'py, PyInt> {
        self.bytes.into_pyint(py)
    }

    pub(crate) const fn shape_axes(&self) -> CoordinateAxes {
        self.shape_axes
    }

    pub(crate) const fn present_axes(&self) -> CoordinateAxes {
        self.present_axes
    }

    pub(crate) fn len_for_python(&self) -> PyResult<usize> {
        self.coordinates.fits_py_ssize().ok_or_else(|| {
            PyOverflowError::new_err(
                "Coordinates length exceeds the maximum Python sequence length",
            )
        })
    }

    pub(crate) fn length_hint(&self) -> usize {
        match &self.coordinates {
            ExactUnsigned::Small(count) => {
                usize::try_from((*count).min(isize::MAX as u128)).expect("Python length hint fits")
            },
            ExactUnsigned::Big(_) => isize::MAX as usize,
        }
    }

    fn has_same_count(&self, py: Python<'py>, other: &Self) -> PyResult<bool> {
        self.coordinates.equals(py, &other.coordinates)
    }

    pub(crate) const fn count_i64(&self) -> Option<i64> {
        match &self.coordinates {
            ExactUnsigned::Small(value) if *value <= i64::MAX as u128 => Some(*value as i64),
            ExactUnsigned::Small(_) | ExactUnsigned::Big(_) => None,
        }
    }
}

type CollectionMeasures<'py> = Option<HashMap<ShapeOwnerKey, CoordinateMeasure<'py>>>;

#[derive(Default)]
enum CollectionPayloadMemo<'py> {
    #[default]
    Empty,
    One {
        owner: ShapeOwnerKey,
        bytes: ExactPayloadBytes<'py>,
    },
    Many(HashMap<ShapeOwnerKey, ExactPayloadBytes<'py>>),
}

impl<'py> CollectionPayloadMemo<'py> {
    fn get(&self, owner: ShapeOwnerKey) -> Option<&ExactPayloadBytes<'py>> {
        match self {
            Self::Empty => None,
            Self::One {
                owner: cached_owner,
                bytes,
            } => (*cached_owner == owner).then_some(bytes),
            Self::Many(collections) => collections.get(&owner),
        }
    }

    fn insert(&mut self, owner: ShapeOwnerKey, bytes: ExactPayloadBytes<'py>) {
        let previous = std::mem::replace(self, Self::Empty);
        *self = match previous {
            Self::Empty => Self::One { owner, bytes },
            Self::One {
                owner: cached_owner,
                ..
            } if cached_owner == owner => Self::One { owner, bytes },
            Self::One {
                owner: cached_owner,
                bytes: cached_bytes,
            } => {
                let mut collections = HashMap::with_capacity(2);
                collections.insert(cached_owner, cached_bytes);
                collections.insert(owner, bytes);
                Self::Many(collections)
            },
            Self::Many(mut collections) => {
                collections.insert(owner, bytes);
                Self::Many(collections)
            },
        };
    }
}

fn packed_range_count_and_axes(
    storage: &PackedStorage,
    missing: Option<&ValidityMask>,
    rows: Range<usize>,
) -> (u128, CoordinateAxes) {
    // Vec-bounded row selections and i32 CSR coordinates keep totals below 2^92.
    match storage {
        PackedStorage::Points { coords, row_map } => {
            let storage_rows = storage::point_logical_len(coords, row_map.as_deref());
            let logical_len = missing.map_or(storage_rows, ValidityMask::len);
            debug_assert!(rows.start <= rows.end);
            debug_assert!(rows.end <= logical_len);
            debug_assert_eq!(
                missing.map_or(storage_rows, ValidityMask::present_count),
                storage_rows
            );
            let present = missing.map_or_else(
                || rows.len(),
                |mask| {
                    mask.window(rows.start, rows.len())
                        .map_or_else(|| rows.len(), |window| window.present_count())
                },
            );
            (
                u128::try_from(present).expect("logical point count fits u128"),
                if present == 0 {
                    CoordinateAxes::XY
                } else {
                    coords.axes()
                },
            )
        },
        PackedStorage::Lines {
            coords,
            offsets,
            row_map,
        } => {
            let offsets = offsets.as_slice();
            let map = row_map.as_deref();
            let storage_rows = storage::line_logical_len(offsets, map);
            let logical_len = missing.map_or(storage_rows, ValidityMask::len);
            debug_assert!(rows.start <= rows.end);
            debug_assert!(rows.end <= logical_len);
            debug_assert_eq!(storage_rows, logical_len);
            let mut count = 0_u128;
            for row in rows {
                if missing.is_some_and(|mask| mask.is_missing(row)) {
                    continue;
                }
                count = count.wrapping_add(
                    u128::try_from(map.csr_window(offsets, row).len())
                        .expect("packed line count fits u128"),
                );
            }
            (
                count,
                if count == 0 {
                    CoordinateAxes::XY
                } else {
                    coords.axes()
                },
            )
        },
        PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        } => {
            let rings = ring_offsets.as_slice();
            let polygons = polygon_offsets.as_slice();
            let map = row_map.as_deref();
            let storage_rows = storage::polygon_logical_len(polygons, map);
            let logical_len = missing.map_or(storage_rows, ValidityMask::len);
            debug_assert!(rows.start <= rows.end);
            debug_assert!(rows.end <= logical_len);
            debug_assert_eq!(storage_rows, logical_len);
            let mut count = 0_u128;
            for row in rows {
                if missing.is_some_and(|mask| mask.is_missing(row)) {
                    continue;
                }
                let row_rings = map.csr_window(polygons, row);
                if !row_rings.is_empty() {
                    let vertices = csr_value_window(rings, row_rings).len();
                    count = count.wrapping_add(
                        u128::try_from(vertices).expect("packed polygon count fits u128"),
                    );
                }
            }
            (
                count,
                if count == 0 {
                    CoordinateAxes::XY
                } else {
                    coords.axes()
                },
            )
        },
    }
}

#[derive(Default)]
pub(crate) struct CoordinateMeasureQuery<'py> {
    collections: CollectionMeasures<'py>,
}

impl<'py> CoordinateMeasureQuery<'py> {
    pub(crate) fn shape(
        &mut self,
        py: Python<'py>,
        shape: &Shape,
    ) -> PyResult<CoordinateMeasure<'py>> {
        shape_measure(py, shape, &mut self.collections)
    }

    fn storage_range(
        &mut self,
        py: Python<'py>,
        storage: &GeometryArrayStorage,
        missing: Option<&ValidityMask>,
        rows: Range<usize>,
    ) -> PyResult<CoordinateMeasure<'py>> {
        match storage {
            GeometryArrayStorage::Mixed(shapes) => {
                let logical_len = missing.map_or(shapes.len(), ValidityMask::len);
                debug_assert!(rows.start <= rows.end);
                debug_assert!(rows.end <= logical_len);
                debug_assert_eq!(shapes.len(), logical_len);
                let mut measure = CoordinateMeasure::ZERO;
                let selected = shapes
                    .get(rows.clone())
                    .expect("logical row range is within mixed shape storage");
                for (shape, logical) in selected.iter().zip(rows) {
                    if missing.is_some_and(|mask| mask.is_missing(logical)) {
                        continue;
                    }
                    let shape_measure = self.shape(py, shape)?;
                    measure = measure.add(py, &shape_measure.present_only())?;
                }
                Ok(measure)
            },
            GeometryArrayStorage::Packed(packed) => {
                let (count, axes) = packed_range_count_and_axes(packed, missing, rows);
                Ok(CoordinateMeasure::from_count_and_axes(count, axes))
            },
        }
    }
}

/// Expand leaf byte counts at hot row folds so they avoid an out-of-line call
/// through the recursive collection matcher. Returns `None` for aggregates and
/// does not perform caller control flow.
macro_rules! simple_shape_payload_bytes {
    ($shape:expr) => {
        match $shape {
            Shape::Point(point) => Some(CoordinateAxes::from_point(*point).byte_width(1)),
            Shape::MultiPoint(seq) => Some(sequence_payload_bytes_usize(seq)),
            Shape::LineString(line) => Some(sequence_payload_bytes_usize(line.as_coords())),
            Shape::Empty(..) => Some(0),
            Shape::MultiLineString(..)
            | Shape::Polygon(..)
            | Shape::MultiPolygon(..)
            | Shape::GeometryCollection(..) => None,
        }
    };
}

#[derive(Default)]
struct PayloadBytesQuery<'py> {
    collections: CollectionPayloadMemo<'py>,
}

impl<'py> PayloadBytesQuery<'py> {
    fn storage_range(
        &mut self,
        py: Python<'py>,
        storage: &GeometryArrayStorage,
        missing: Option<&ValidityMask>,
        rows: Range<usize>,
    ) -> PyResult<ExactPayloadBytes<'py>> {
        match storage {
            GeometryArrayStorage::Mixed(shapes) => {
                let logical_len = missing.map_or(shapes.len(), ValidityMask::len);
                debug_assert!(rows.start <= rows.end);
                debug_assert!(rows.end <= logical_len);
                debug_assert_eq!(shapes.len(), logical_len);
                let selected = shapes
                    .get(rows.clone())
                    .expect("logical row range is within mixed shape storage");
                let mut selected_rows = selected.iter().zip(rows);
                // A selected range is bounded by its `Vec<Shape>` owner. Even
                // `isize::MAX` rows of `usize::MAX` bytes fit in `u128`.
                match selected_rows.try_fold(0_u128, |bytes, (shape, logical)| {
                    if missing.is_some_and(|mask| mask.is_missing(logical)) {
                        return ControlFlow::Continue(bytes);
                    }
                    let Some(shape_bytes) = simple_shape_payload_bytes!(shape)
                        .or_else(|| self.complex_shape_payload_bytes_usize(shape))
                    else {
                        return ControlFlow::Break((bytes, shape));
                    };
                    ControlFlow::Continue(bytes.wrapping_add(
                        u128::try_from(shape_bytes).expect("usize payload fits u128"),
                    ))
                }) {
                    ControlFlow::Continue(bytes) => Ok(ExactPayloadBytes::from_wide_bytes(bytes)),
                    ControlFlow::Break((native_bytes, shape)) => {
                        let mut bytes = ExactPayloadBytes::from_wide_bytes(native_bytes)
                            .add(py, &self.shape(py, shape)?)?;
                        for (shape, logical) in selected_rows {
                            if missing.is_some_and(|mask| mask.is_missing(logical)) {
                                continue;
                            }
                            bytes = if let Some(shape_bytes) = simple_shape_payload_bytes!(shape)
                                .or_else(|| self.complex_shape_payload_bytes_usize(shape))
                            {
                                bytes.add_native_bytes(py, shape_bytes)?
                            } else {
                                bytes.add(py, &self.shape(py, shape)?)?
                            };
                        }
                        Ok(bytes)
                    },
                }
            },
            GeometryArrayStorage::Packed(packed) => {
                let (count, axes) = packed_range_count_and_axes(packed, missing, rows);
                Ok(ExactPayloadBytes::from_count_and_axes(count, axes))
            },
        }
    }

    fn shape(&mut self, py: Python<'py>, shape: &Shape) -> PyResult<ExactPayloadBytes<'py>> {
        shape_payload_bytes(py, shape, &mut self.collections)
    }

    fn complex_shape_payload_bytes_usize(&mut self, shape: &Shape) -> Option<usize> {
        match shape {
            Shape::MultiLineString(lines) => lines.iter().try_fold(0_usize, |bytes, line| {
                bytes.checked_add(sequence_payload_bytes_usize(line.as_coords()))
            }),
            Shape::Polygon(polygon) => polygon_payload_bytes_usize(polygon),
            Shape::MultiPolygon(polygons) => polygons.iter().try_fold(0_usize, |bytes, polygon| {
                bytes.checked_add(polygon_payload_bytes_usize(polygon)?)
            }),
            Shape::GeometryCollection(collection) => {
                let members = collection.as_slice();
                let owner = collection
                    .members_are_shared()
                    .then(|| ShapeOwnerKey::new(members));
                if let Some(owner) = owner
                    && let Some(bytes) = self.collections.get(owner)
                {
                    return bytes.as_usize();
                }
                let bytes = members.iter().try_fold(0_usize, |bytes, member| {
                    let member_bytes = simple_shape_payload_bytes!(member)
                        .or_else(|| self.complex_shape_payload_bytes_usize(member))?;
                    bytes.checked_add(member_bytes)
                })?;
                if let Some(owner) = owner {
                    self.collections
                        .insert(owner, ExactPayloadBytes::from_native_bytes(bytes));
                }
                Some(bytes)
            },
            Shape::Point(..) | Shape::MultiPoint(..) | Shape::LineString(..) | Shape::Empty(..) => {
                None
            },
        }
    }
}

fn sequence_measure<'py>(seq: &CoordSeq) -> CoordinateMeasure<'py> {
    CoordinateMeasure::from_count_and_axes(
        u128::try_from(seq.len()).expect("coordinate count fits u128"),
        seq.axes(),
    )
}

fn polygon_measure<'py>(
    py: Python<'py>,
    polygon: &crate::geometry::Polygon,
) -> PyResult<CoordinateMeasure<'py>> {
    let mut measure = sequence_measure(polygon.shell.coords());
    for ring in polygon.holes.iter() {
        measure = measure.add(py, &sequence_measure(ring.coords()))?;
    }
    Ok(measure.with_shape_axes(polygon.axes()))
}

fn shape_measure<'py>(
    py: Python<'py>,
    shape: &Shape,
    collections: &mut CollectionMeasures<'py>,
) -> PyResult<CoordinateMeasure<'py>> {
    match shape {
        Shape::Point(point) => Ok(CoordinateMeasure::from_count_and_axes(
            1,
            CoordinateAxes::from_point(*point),
        )),
        Shape::MultiPoint(seq) => Ok(sequence_measure(seq)),
        Shape::LineString(line) => Ok(sequence_measure(line.as_coords())),
        Shape::MultiLineString(lines) => {
            let mut measure = CoordinateMeasure::ZERO;
            for line in lines.iter() {
                measure = measure.add(py, &sequence_measure(line.as_coords()))?;
            }
            Ok(measure.with_shape_axes(lines.axes()))
        },
        Shape::Polygon(polygon) => polygon_measure(py, polygon),
        Shape::MultiPolygon(polygons) => {
            let mut measure = CoordinateMeasure::ZERO;
            for polygon in polygons.iter() {
                measure = measure.add(py, &polygon_measure(py, polygon)?)?;
            }
            Ok(measure.with_shape_axes(polygons.axes()))
        },
        Shape::GeometryCollection(collection) => {
            let members = collection.as_slice();
            let owner = collection
                .members_are_shared()
                .then(|| ShapeOwnerKey::new(members));
            if let Some(owner) = owner
                && let Some(measure) = collections.as_ref().and_then(|cached| cached.get(&owner))
            {
                return Ok(measure.clone());
            }
            let mut measure = CoordinateMeasure::ZERO;
            for member in members {
                measure = measure.add(py, &shape_measure(py, member, collections)?)?;
            }
            if let Some(owner) = owner {
                collections
                    .get_or_insert_with(HashMap::new)
                    .insert(owner, measure.clone());
            }
            Ok(measure)
        },
        Shape::Empty(_, axes) => Ok(CoordinateMeasure::ZERO.with_shape_axes(*axes)),
    }
}

fn sequence_payload_bytes<'py>(seq: &CoordSeq) -> ExactPayloadBytes<'py> {
    ExactPayloadBytes::from_native_count_and_axes(seq.len(), seq.axes())
}

fn sequence_payload_bytes_usize(seq: &CoordSeq) -> usize {
    // CoordSeq admission caps visible rows at i32::MAX, so even XYZM fits usize.
    seq.coordinate_bytes()
}

fn polygon_payload_bytes<'py>(
    py: Python<'py>,
    polygon: &Polygon,
) -> PyResult<ExactPayloadBytes<'py>> {
    let mut bytes = sequence_payload_bytes(polygon.shell.coords());
    for ring in polygon.holes.iter() {
        bytes = bytes.add(py, &sequence_payload_bytes(ring.coords()))?;
    }
    Ok(bytes)
}

fn polygon_payload_bytes_usize(polygon: &Polygon) -> Option<usize> {
    polygon.holes.iter().try_fold(
        sequence_payload_bytes_usize(polygon.shell.coords()),
        |bytes, ring| bytes.checked_add(sequence_payload_bytes_usize(ring.coords())),
    )
}

fn shape_payload_bytes<'py>(
    py: Python<'py>,
    shape: &Shape,
    collections: &mut CollectionPayloadMemo<'py>,
) -> PyResult<ExactPayloadBytes<'py>> {
    match shape {
        Shape::Point(point) => Ok(ExactPayloadBytes::from_native_count_and_axes(
            1,
            CoordinateAxes::from_point(*point),
        )),
        Shape::MultiPoint(seq) => Ok(sequence_payload_bytes(seq)),
        Shape::LineString(line) => Ok(sequence_payload_bytes(line.as_coords())),
        Shape::MultiLineString(lines) => {
            let mut bytes = ExactPayloadBytes::ZERO;
            for line in lines.iter() {
                bytes = bytes.add(py, &sequence_payload_bytes(line.as_coords()))?;
            }
            Ok(bytes)
        },
        Shape::Polygon(polygon) => polygon_payload_bytes(py, polygon),
        Shape::MultiPolygon(polygons) => {
            let mut bytes = ExactPayloadBytes::ZERO;
            for polygon in polygons.iter() {
                bytes = bytes.add(py, &polygon_payload_bytes(py, polygon)?)?;
            }
            Ok(bytes)
        },
        Shape::GeometryCollection(collection) => {
            let members = collection.as_slice();
            let owner = collection
                .members_are_shared()
                .then(|| ShapeOwnerKey::new(members));
            if let Some(owner) = owner
                && let Some(bytes) = collections.get(owner)
            {
                return Ok(bytes.clone());
            }
            let mut bytes = ExactPayloadBytes::ZERO;
            for member in members {
                bytes = bytes.add(py, &shape_payload_bytes(py, member, collections)?)?;
            }
            if let Some(owner) = owner {
                collections.insert(owner, bytes.clone());
            }
            Ok(bytes)
        },
        Shape::Empty(..) => Ok(ExactPayloadBytes::ZERO),
    }
}

pub(crate) fn measure_shape<'py>(
    py: Python<'py>,
    shape: &Shape,
) -> PyResult<CoordinateMeasure<'py>> {
    CoordinateMeasureQuery::default().shape(py, shape)
}

pub(crate) fn measure_storage<'py>(
    py: Python<'py>,
    storage: &GeometryArrayStorage,
    missing: Option<&ValidityMask>,
) -> PyResult<CoordinateMeasure<'py>> {
    let logical_len = missing.map_or_else(|| storage.len(), ValidityMask::len);
    measure_storage_range(py, storage, missing, 0..logical_len)
}

pub(crate) fn measure_storage_range<'py>(
    py: Python<'py>,
    storage: &GeometryArrayStorage,
    missing: Option<&ValidityMask>,
    rows: Range<usize>,
) -> PyResult<CoordinateMeasure<'py>> {
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
/// copying coordinates. Packed [`GeometryArrayStorage`] variants sit behind
/// the `Array` owner.
#[derive(Clone, Debug)]
enum CoordinateOwner {
    Shape(Arc<ShapeData>),
    Array {
        storage: Arc<GeometryArrayStorage>,
        missing: Option<ValidityMask>,
    },
}

/// The geometry source behind a view, for nested (topology-shaped) coordinate
/// rendering that the flat columns intentionally do not preserve.
pub(crate) enum CoordinateSource<'a> {
    Shape(&'a Shape),
    Array(&'a GeometryArrayStorage, Option<&'a ValidityMask>),
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
        storage: Arc<GeometryArrayStorage>,
        missing: Option<ValidityMask>,
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

impl CoordinatePointCursor {
    fn new(view: CoordinateView, reverse: bool) -> Self {
        let mut frames = Vec::new();
        match &view.owner {
            CoordinateOwner::Shape(shape) => {
                frames.push(CoordinateFrame::Shape(shape.shape().clone()));
            },
            CoordinateOwner::Array { storage, missing } => {
                let (rows, compact_point_rank) = match storage.as_ref() {
                    GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                        let rows = storage::point_logical_len(coords, row_map.as_deref());
                        let compact = missing.as_ref().filter(|mask| mask.len() != rows);
                        (
                            compact.map_or(rows, ValidityMask::len),
                            compact.map(|mask| if reverse { mask.present_count() } else { 0 }),
                        )
                    },
                    GeometryArrayStorage::Mixed(_)
                    | GeometryArrayStorage::Packed(
                        PackedStorage::Lines { .. } | PackedStorage::Polygons { .. },
                    ) => (storage.len(), None),
                };
                frames.push(CoordinateFrame::ArrayRows {
                    storage: Arc::clone(storage),
                    missing: missing.clone(),
                    remaining: 0..rows,
                    compact_point_rank,
                });
            },
        }
        Self {
            view,
            frames,
            reverse,
        }
    }

    pub(crate) fn next_point(&mut self) -> Option<Point> {
        loop {
            if matches!(self.frames.last(), Some(CoordinateFrame::Shape(_))) {
                let Some(CoordinateFrame::Shape(shape)) = self.frames.pop() else {
                    unreachable!("shape frame checked above");
                };
                match shape {
                    Shape::Point(point) => return Some(point),
                    Shape::MultiPoint(points) => {
                        let coordinates = CoordSeq::clone(&points);
                        self.frames.push(CoordinateFrame::Sequence {
                            remaining: 0..coordinates.len(),
                            coordinates,
                        });
                    },
                    Shape::LineString(line) => {
                        let coordinates = line.as_coords().clone();
                        self.frames.push(CoordinateFrame::Sequence {
                            remaining: 0..coordinates.len(),
                            coordinates,
                        });
                    },
                    Shape::MultiLineString(lines) => {
                        let len = lines.as_slice().len();
                        self.frames.push(CoordinateFrame::MultiLines {
                            shape: Shape::MultiLineString(lines),
                            remaining: 0..len,
                        });
                    },
                    Shape::Polygon(polygon) => {
                        let rings = polygon.holes.len().wrapping_add(1);
                        self.frames.push(CoordinateFrame::PolygonRings {
                            polygon,
                            remaining: 0..rings,
                        });
                    },
                    Shape::MultiPolygon(polygons) => {
                        let len = polygons.as_slice().len();
                        self.frames.push(CoordinateFrame::MultiPolygons {
                            shape: Shape::MultiPolygon(polygons),
                            remaining: 0..len,
                        });
                    },
                    Shape::GeometryCollection(collection) => {
                        let len = collection.len();
                        self.frames.push(CoordinateFrame::Collection {
                            shape: Shape::GeometryCollection(collection),
                            remaining: 0..len,
                        });
                    },
                    Shape::Empty(..) => {},
                }
                continue;
            }

            let step = self
                .frames
                .last_mut()
                .map_or(CoordinateStep::Pop, |frame| frame.advance(self.reverse));
            match step {
                CoordinateStep::Continue => {},
                CoordinateStep::Point(point) => return Some(point),
                CoordinateStep::Push(frame) => self.frames.push(frame),
                CoordinateStep::Pop => {
                    self.frames.pop()?;
                },
            }
        }
    }

    pub(crate) fn logical_heap_bytes(&self) -> usize {
        self.view.logical_heap_bytes().saturating_add(
            self.frames
                .capacity()
                .saturating_mul(std::mem::size_of::<CoordinateFrame>()),
        )
    }
}

impl CoordinateFrame {
    fn advance(&mut self, reverse: bool) -> CoordinateStep {
        match self {
            Self::Sequence {
                coordinates,
                remaining,
            } => {
                let index = next_index(remaining, reverse);
                index.map_or(CoordinateStep::Pop, |index| {
                    CoordinateStep::Point(coordinates.point_at(index))
                })
            },
            Self::Collection { shape, remaining } => {
                let index = next_index(remaining, reverse);
                index.map_or(CoordinateStep::Pop, |index| {
                    let Shape::GeometryCollection(collection) = shape else {
                        unreachable!("collection frame owns a geometry collection");
                    };
                    CoordinateStep::Push(Self::Shape(
                        collection
                            .as_slice()
                            .get(index)
                            .expect("collection frame index is within its admitted member range")
                            .clone(),
                    ))
                })
            },
            Self::MultiLines { shape, remaining } => {
                let index = next_index(remaining, reverse);
                index.map_or(CoordinateStep::Pop, |index| {
                    let Shape::MultiLineString(lines) = shape else {
                        unreachable!("line frame owns a multi-line string");
                    };
                    let coordinates = lines
                        .as_slice()
                        .get(index)
                        .expect("multi-line frame index is within its admitted member range")
                        .as_coords()
                        .clone();
                    CoordinateStep::Push(Self::Sequence {
                        remaining: 0..coordinates.len(),
                        coordinates,
                    })
                })
            },
            Self::MultiPolygons { shape, remaining } => {
                let index = next_index(remaining, reverse);
                index.map_or(CoordinateStep::Pop, |index| {
                    let Shape::MultiPolygon(polygons) = shape else {
                        unreachable!("polygon frame owns a multi-polygon");
                    };
                    CoordinateStep::Push(Self::Shape(Shape::Polygon(
                        polygons
                            .as_slice()
                            .get(index)
                            .expect("multi-polygon frame index is within its admitted member range")
                            .clone(),
                    )))
                })
            },
            Self::PolygonRings { polygon, remaining } => {
                let index = next_index(remaining, reverse);
                index.map_or(CoordinateStep::Pop, |index| {
                    let coordinates = if index == 0 {
                        polygon.shell.coords()
                    } else {
                        polygon
                            .holes
                            .get(index.wrapping_sub(1))
                            .expect("polygon ring index follows its shell")
                            .coords()
                    }
                    .clone();
                    CoordinateStep::Push(Self::Sequence {
                        remaining: 0..coordinates.len(),
                        coordinates,
                    })
                })
            },
            Self::ArrayRows {
                storage,
                missing,
                remaining,
                compact_point_rank,
            } => {
                let Some(row) = next_index(remaining, reverse) else {
                    return CoordinateStep::Pop;
                };
                match storage.as_ref() {
                    GeometryArrayStorage::Mixed(shapes) => {
                        if missing.as_ref().is_some_and(|mask| mask.is_missing(row)) {
                            CoordinateStep::Continue
                        } else {
                            CoordinateStep::Push(Self::Shape(
                                shapes
                                    .get(row)
                                    .expect(
                                        "array row frame index is within its admitted row range",
                                    )
                                    .clone(),
                            ))
                        }
                    },
                    GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                        let map = row_map.as_deref();
                        let physical_row = if let Some(mask) = missing
                            && compact_point_rank.is_some()
                        {
                            if mask.is_missing(row) {
                                return CoordinateStep::Continue;
                            }
                            let rank = compact_point_rank
                                .as_mut()
                                .expect("compact point rank accompanies a mismatched mask");
                            if reverse {
                                *rank = rank
                                    .checked_sub(1)
                                    .expect("a present compact row has a stored rank");
                                *rank
                            } else {
                                let present = *rank;
                                *rank = rank
                                    .checked_add(1)
                                    .expect("present point rank fits the stored point count");
                                present
                            }
                        } else {
                            if missing.as_ref().is_some_and(|mask| mask.is_missing(row)) {
                                return CoordinateStep::Continue;
                            }
                            row
                        };
                        CoordinateStep::Point(
                            coords.point_at(storage::physical_row(map, physical_row)),
                        )
                    },
                    GeometryArrayStorage::Packed(PackedStorage::Lines {
                        coords,
                        offsets,
                        row_map,
                    }) => {
                        if missing.as_ref().is_some_and(|mask| mask.is_missing(row)) {
                            return CoordinateStep::Continue;
                        }
                        let window = row_map.as_deref().csr_window(offsets.as_slice(), row);
                        let coordinates =
                            coords.view(CoordWindow::from_range(window, coords.len()));
                        CoordinateStep::Push(Self::Sequence {
                            remaining: 0..coordinates.len(),
                            coordinates,
                        })
                    },
                    GeometryArrayStorage::Packed(PackedStorage::Polygons {
                        coords,
                        ring_offsets,
                        polygon_offsets,
                        row_map,
                    }) => {
                        if missing.as_ref().is_some_and(|mask| mask.is_missing(row)) {
                            return CoordinateStep::Continue;
                        }
                        let rings = row_map
                            .as_deref()
                            .csr_window(polygon_offsets.as_slice(), row);
                        if rings.is_empty() {
                            return CoordinateStep::Continue;
                        }
                        CoordinateStep::Push(Self::PackedRings {
                            coordinates: Arc::clone(coords),
                            offsets: ring_offsets.clone(),
                            remaining: rings,
                        })
                    },
                }
            },
            Self::PackedRings {
                coordinates,
                offsets,
                remaining,
            } => {
                let Some(ring) = next_index(remaining, reverse) else {
                    return CoordinateStep::Pop;
                };
                let window = csr_value_window(offsets.as_slice(), ring..ring.wrapping_add(1));
                let coordinates =
                    coordinates.view(CoordWindow::from_range(window, coordinates.len()));
                CoordinateStep::Push(Self::Sequence {
                    remaining: 0..coordinates.len(),
                    coordinates,
                })
            },
            Self::Shape(_) => unreachable!("shape frames are expanded before advancing"),
        }
    }
}

fn next_index(range: &mut std::ops::Range<usize>, reverse: bool) -> Option<usize> {
    if reverse {
        range.next_back()
    } else {
        range.next()
    }
}

impl CoordinateView {
    pub const fn from_shape(shape: Arc<ShapeData>) -> Self {
        Self {
            owner: CoordinateOwner::Shape(shape),
        }
    }

    pub(crate) const fn from_array_masked(
        storage: Arc<GeometryArrayStorage>,
        missing: Option<ValidityMask>,
    ) -> Self {
        Self {
            owner: CoordinateOwner::Array { storage, missing },
        }
    }

    /// The geometry source, for topology-shaped (nested) rendering.
    pub(crate) fn source(&self) -> CoordinateSource<'_> {
        match &self.owner {
            CoordinateOwner::Shape(shape) => CoordinateSource::Shape(shape),
            CoordinateOwner::Array { storage, missing } => {
                CoordinateSource::Array(storage, missing.as_ref())
            },
        }
    }

    /// Drive `visit` over every run / scalar point of the owner, dispatching
    /// scalar shapes vs array rows. The traversal primitive behind every
    /// method. Prefer [`try_for_each`] when the visitor may stop early.
    pub(crate) fn for_each<F>(&self, visit: &mut F)
    where
        F: for<'x> FnMut(Option<usize>, CoordRun<'x>),
    {
        let ControlFlow::Continue(()): ControlFlow<Infallible> =
            self.try_for_each(&mut |geometry, run| {
                visit(geometry, run);
                ControlFlow::Continue(())
            });
    }

    /// Breakable traversal: `ControlFlow::Break` stops at the current run.
    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "admitted polygon CSR rows bound both ring endpoints; retain direct hot-path column reads"
    )]
    fn try_for_each<B, F>(&self, visit: &mut F) -> ControlFlow<B>
    where
        F: for<'x> FnMut(Option<usize>, CoordRun<'x>) -> ControlFlow<B>,
    {
        match &self.owner {
            CoordinateOwner::Shape(shape) => shape.for_each_coord_run(&mut |run| visit(None, run)),
            CoordinateOwner::Array { storage, missing } => match storage.as_ref() {
                GeometryArrayStorage::Mixed(shapes) => {
                    for (row, shape) in shapes.iter().enumerate() {
                        if missing.as_ref().is_some_and(|mask| mask.is_missing(row)) {
                            continue;
                        }
                        shape.for_each_coord_run(&mut |run| visit(Some(row), run))?;
                    }
                    ControlFlow::Continue(())
                },
                // Packed points: each row is one scalar point geometry, so it
                // contributes one coordinate tagged with its own row index.
                GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                    let map = row_map.as_deref();
                    if let Some(mask) = missing
                        && mask.len() != storage::point_logical_len(coords, map)
                    {
                        for (row, present) in mask.cursor() {
                            let Some(present) = present else { continue };
                            visit(
                                Some(row),
                                CoordRun::Point(
                                    coords.point_at(storage::physical_row(map, present)),
                                ),
                            )?;
                        }
                        return ControlFlow::Continue(());
                    }
                    for row in 0..storage::point_logical_len(coords, map) {
                        if missing.as_ref().is_some_and(|mask| mask.is_missing(row)) {
                            continue;
                        }
                        visit(
                            Some(row),
                            CoordRun::Point(coords.point_at(storage::physical_row(map, row))),
                        )?;
                    }
                    ControlFlow::Continue(())
                },
                // Packed lines: each row contributes its zero-copy column
                // window as one run, exactly like a scalar LineString.
                GeometryArrayStorage::Packed(PackedStorage::Lines {
                    coords,
                    offsets,
                    row_map,
                }) => {
                    let map = row_map.as_deref();
                    let rows = storage::line_logical_len(offsets.as_slice(), map);
                    for row in 0..rows {
                        if missing.as_ref().is_some_and(|mask| mask.is_missing(row)) {
                            continue;
                        }
                        let window = map.csr_window(offsets.as_slice(), row);
                        let view = coords.view(CoordWindow::from_range(window, coords.len()));
                        visit(Some(row), CoordRun::Seq(&view))?;
                    }
                    ControlFlow::Continue(())
                },
                // Packed polygons: walk the two CSR levels directly. Building
                // a temporary Polygon view per row would allocate its hole Arc
                // and then make `walk` traverse the same offsets again.
                GeometryArrayStorage::Packed(PackedStorage::Polygons {
                    coords,
                    ring_offsets,
                    polygon_offsets,
                    row_map,
                }) => {
                    let map = row_map.as_deref();
                    let ring_offsets = ring_offsets.as_slice();
                    let rows = storage::polygon_logical_len(polygon_offsets.as_slice(), map);
                    for row in 0..rows {
                        if missing.as_ref().is_some_and(|mask| mask.is_missing(row)) {
                            continue;
                        }
                        for physical_ring in map.csr_window(polygon_offsets.as_slice(), row) {
                            let window = ring_offsets[physical_ring] as usize
                                ..ring_offsets[physical_ring + 1] as usize;
                            let view = coords.view(CoordWindow::from_range(window, coords.len()));
                            visit(Some(row), CoordRun::Seq(&view))?;
                        }
                    }
                    ControlFlow::Continue(())
                },
            },
        }
    }

    pub(crate) fn exact_measure<'py>(&self, py: Python<'py>) -> PyResult<CoordinateMeasure<'py>> {
        match &self.owner {
            CoordinateOwner::Shape(shape) => measure_shape(py, shape.shape()),
            CoordinateOwner::Array { storage, missing } => {
                measure_storage(py, storage, missing.as_ref())
            },
        }
    }

    pub(crate) fn len_for_python(&self, py: Python<'_>) -> PyResult<usize> {
        self.exact_measure(py)?.len_for_python()
    }

    pub(crate) fn nbytes_pyint<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyInt>> {
        Ok(self.exact_measure(py)?.bytes_pyint(py))
    }

    pub(crate) fn point_cursor(&self, reverse: bool) -> CoordinatePointCursor {
        CoordinatePointCursor::new(self.clone(), reverse)
    }

    #[cfg(test)]
    fn attach_test_python<T>(f: impl for<'py> FnOnce(Python<'py>) -> T) -> T {
        Python::initialize();
        Python::attach(f)
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        Self::attach_test_python(|py| {
            self.len_for_python(py)
                .expect("test coordinate count fits Python sequence length")
        })
    }

    #[cfg(test)]
    pub fn axes(&self) -> CoordinateAxes {
        Self::attach_test_python(|py| {
            self.axes_for_python(py)
                .expect("test coordinate axes are measurable")
        })
    }

    #[cfg(test)]
    pub(crate) fn column(&self, axis: CoordinateAxis) -> CoordinateColumnRef<'_> {
        Self::attach_test_python(|py| {
            self.try_column(py, axis)
                .expect("test coordinate column materializes")
        })
    }

    #[cfg(test)]
    pub fn collect_points(&self) -> Vec<Point> {
        Self::attach_test_python(|py| {
            let count = self
                .len_for_python(py)
                .expect("test coordinate count fits Python sequence length");
            let capacity = checked_vector_len::<Point>(count, "coordinate")
                .expect("test coordinate points fit Vec layout");
            self.collect_points_admitted(capacity)
        })
    }

    #[cfg(test)]
    pub fn row_index_i64(&self) -> Vec<i64> {
        Self::attach_test_python(|py| {
            self.try_row_index_i64(py)
                .expect("test coordinate row indexes materialize")
        })
    }

    /// Logical Rust-side heap retained by the view's owner.
    pub fn logical_heap_bytes(&self) -> usize {
        match &self.owner {
            CoordinateOwner::Shape(shape) => shape.shape().logical_heap_bytes(),
            CoordinateOwner::Array { storage, .. } => storage.logical_heap_bytes(),
        }
    }

    /// Union of every coordinate's axes — Z/M present if present anywhere.
    ///
    /// For a scalar geometry owner this is the shape's declared axes, so a
    /// typed empty (`POINT Z EMPTY`, `POLYGON M EMPTY`, …) reports its carried
    /// empty axes even though the view has zero vertices. Array owners still
    /// fold over present rows (missing rows contribute nothing).
    pub(crate) fn axes_for_python(&self, py: Python<'_>) -> PyResult<CoordinateAxes> {
        let measure = self.exact_measure(py)?;
        match &self.owner {
            CoordinateOwner::Shape(_) => Ok(measure.shape_axes()),
            CoordinateOwner::Array { .. } => Ok(measure.present_axes()),
        }
    }

    /// `Some` only when the whole view is exactly one contiguous [`CoordSeq`]
    /// (`MultiPoint`/`LineString`), enabling zero-copy columns. `None` for
    /// `Point` and any multipart view.
    pub fn single_seq(&self) -> Option<&CoordSeq> {
        match &self.owner {
            CoordinateOwner::Shape(shape) => match shape.shape() {
                Shape::MultiPoint(seq) => Some(seq),
                Shape::LineString(seq) => Some(seq.as_coords()),
                Shape::Point(_)
                | Shape::MultiLineString(_)
                | Shape::Polygon(_)
                | Shape::MultiPolygon(_)
                | Shape::GeometryCollection(_)
                | Shape::Empty(..) => None,
            },
            // A packed point array's coordinates are exactly its shared column,
            // so `.x`/`.y` can borrow it directly (zero-copy on the Rust side).
            CoordinateOwner::Array { storage, missing } => {
                if missing.is_some() {
                    return None;
                }
                match storage.as_ref() {
                    GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                        row_map.is_identity().then_some(coords)
                    },
                    // Packed line/polygon coordinate columns borrow only when the
                    // logical rows are the whole physical storage. Row-selected
                    // arrays materialize through `for_each`, so public buffers
                    // match logical row order and never expose excluded parent
                    // coordinates.
                    GeometryArrayStorage::Packed(
                        PackedStorage::Lines {
                            coords, row_map, ..
                        }
                        | PackedStorage::Polygons {
                            coords, row_map, ..
                        },
                    ) => row_map.is_identity().then_some(coords),
                    GeometryArrayStorage::Mixed(_) => None,
                }
            },
        }
    }

    /// Resolve one axis to a borrowed or materialized column.
    pub(crate) fn try_column(
        &self,
        py: Python<'_>,
        axis: CoordinateAxis,
    ) -> PyResult<CoordinateColumnRef<'_>> {
        if let CoordinateOwner::Array {
            storage,
            missing: None,
        } = &self.owner
            && let GeometryArrayStorage::Packed(packed) = storage.as_ref()
            && let Some((coords, span)) = packed_contiguous_coord_span(packed)
        {
            return Ok(borrowed_axis(coords, span, axis));
        }
        if let CoordinateOwner::Array { storage, missing } = &self.owner
            && let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) =
                storage.as_ref()
            && missing.is_none()
            && let Some(map) = row_map.explicit_indices()
        {
            let capacity = checked_vector_len::<f64>(map.len(), "coordinate column")?;
            let values = match axis {
                CoordinateAxis::X => coords.xs(),
                CoordinateAxis::Y => coords.ys(),
                CoordinateAxis::Z => {
                    let Some(zs) = coords.zs() else {
                        return Ok(CoordinateColumnRef::Missing);
                    };
                    zs
                },
                CoordinateAxis::M => {
                    let Some(ms) = coords.ms() else {
                        return Ok(CoordinateColumnRef::Missing);
                    };
                    ms
                },
            };
            let mut out = Vec::with_capacity(capacity);
            out.extend(map.iter().map(|&row| {
                *values
                    .get(row)
                    .expect("point row selection is bounded by its coordinate owner")
            }));
            return Ok(CoordinateColumnRef::Dense(out));
        }
        if let Some(seq) = self.single_seq() {
            return Ok(match axis {
                CoordinateAxis::X => CoordinateColumnRef::Borrowed(seq.xs()),
                CoordinateAxis::Y => CoordinateColumnRef::Borrowed(seq.ys()),
                CoordinateAxis::Z => seq
                    .zs()
                    .map_or(CoordinateColumnRef::Missing, CoordinateColumnRef::Borrowed),
                CoordinateAxis::M => seq
                    .ms()
                    .map_or(CoordinateColumnRef::Missing, CoordinateColumnRef::Borrowed),
            });
        }
        Ok(match axis {
            CoordinateAxis::X => {
                CoordinateColumnRef::Dense(self.gather_ordinate(py, |point| point.x)?)
            },
            CoordinateAxis::Y => {
                CoordinateColumnRef::Dense(self.gather_ordinate(py, |point| point.y)?)
            },
            CoordinateAxis::Z => self.gather_optional(py, Point::z)?,
            CoordinateAxis::M => self.gather_optional(py, Point::m)?,
        })
    }

    /// Stream every flattened coordinate without
    /// materializing a `Vec<Point>` — the engine behind the list /
    /// numpy / row-index surfaces. Use [`collect_points`](Self::collect_points)
    /// only where random access is genuinely needed (slicing).
    #[expect(
        clippy::impl_trait_in_params,
        reason = "visitor type is intentionally opaque at this one-pass traversal boundary"
    )]
    pub fn for_each_point(&self, mut visit: impl FnMut(Point)) {
        let ControlFlow::Continue(()): ControlFlow<Infallible> =
            self.try_for_each_point(&mut |coord| {
                visit(coord);
                ControlFlow::Continue(())
            });
    }

    /// Breakable point stream: `ControlFlow::Break` stops at the current
    /// coordinate. Used by `index` / membership / equality so a first hit or
    /// first mismatch never walks the tail.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "visitor type is intentionally opaque at this one-pass traversal boundary"
    )]
    #[expect(
        clippy::indexing_slicing,
        reason = "CoordSeq admits equal-length ordinate columns and this loop is bounded by their shared length"
    )]
    pub fn try_for_each_point<B>(
        &self,
        visit: &mut impl FnMut(Point) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.try_for_each(&mut |_, run| match run {
            CoordRun::Seq(seq) => {
                // Length-exact column windows: loop bound IS each slice's len,
                // so per-vertex access carries no bounds checks (elision).
                let xs = seq.xs();
                let ys = seq.ys();
                let width = xs.len();
                let ys = &ys[..width];
                let zs = seq.zs().map(|column| &column[..width]);
                let ms = seq.ms().map(|column| &column[..width]);
                for coord in 0..width {
                    visit(Point::new_raw_axes(
                        xs[coord],
                        ys[coord],
                        ZOrdinate(zs.map(|column| column[coord])),
                        MOrdinate(ms.map(|column| column[coord])),
                    ))?;
                }
                ControlFlow::Continue(())
            },
            CoordRun::Point(point) => visit(point),
        })
    }

    /// Flattened coordinates.
    #[cfg(test)]
    pub(crate) fn collect_points_admitted(&self, capacity: usize) -> Vec<Point> {
        let mut points = Vec::with_capacity(capacity);
        self.for_each_point(|coord| points.push(coord));
        points
    }

    /// Per-coordinate geometry-row index (all `0` for a scalar geometry; the
    /// logical source row for an array owner). Backs `coords.row_index`.
    #[cfg(test)]
    pub fn row_index(&self) -> Vec<usize> {
        Self::attach_test_python(|py| {
            let count = self
                .len_for_python(py)
                .expect("test coordinates fit Python length");
            let capacity = checked_vector_len::<usize>(count, "coordinate row index")
                .expect("test row index fits Vec layout");
            self.row_index_cast(capacity, |row| row)
        })
    }

    /// Per-coordinate geometry-row index as NumPy's `int64` lane, built at
    /// coordinate-run altitude (CSR window fills / one fill per run) rather
    /// than per-vertex push. Mirrors [`row_index`] for the `Coordinates.index`
    /// getter and `get_coordinates(..., return_index=True)`.
    pub(crate) fn try_row_index_i64(&self, py: Python<'_>) -> PyResult<Vec<i64>> {
        let count = self.len_for_python(py)?;
        let capacity = checked_vector_len::<i64>(count, "coordinate row index")?;
        let rows = match &self.owner {
            CoordinateOwner::Shape(_) => 1,
            CoordinateOwner::Array { storage, .. } => storage.len(),
        };
        if rows.saturating_sub(1) > i64::MAX as usize {
            return Err(PyOverflowError::new_err(
                "coordinate row index exceeds the int64 output range",
            ));
        }
        Ok(self.row_index_cast(capacity, |row| row as i64))
    }

    /// Shared run-wise row-index emitter. Identity packed storage fills from
    /// CSR offsets (or the point column length); every other owner walks
    /// coordinate runs once and fills each run's span with its logical row id.
    #[expect(
        clippy::indexing_slicing,
        reason = "identity row indexes and CSR endpoints are admitted against the exact output capacity"
    )]
    fn row_index_cast<T: Copy>(&self, capacity: usize, cast: impl Fn(usize) -> T) -> Vec<T> {
        let zero = cast(0);

        if let CoordinateOwner::Array {
            storage,
            missing: None,
        } = &self.owner
        {
            match storage.as_ref() {
                GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map })
                    if row_map.is_identity() =>
                {
                    return (0..coords.len()).map(cast).collect();
                },
                GeometryArrayStorage::Packed(PackedStorage::Lines {
                    offsets, row_map, ..
                }) if row_map.is_identity() => {
                    let offsets = offsets.as_slice();
                    let mut indexes = vec![zero; capacity];
                    for (row, &[start, end]) in offsets.array_windows::<2>().enumerate() {
                        let start = start as usize;
                        let end = end as usize;
                        indexes[start..end].fill(cast(row));
                    }
                    return indexes;
                },
                GeometryArrayStorage::Packed(PackedStorage::Polygons {
                    ring_offsets,
                    polygon_offsets,
                    row_map,
                    ..
                }) if row_map.is_identity() => {
                    let rings = ring_offsets.as_slice();
                    let polys = polygon_offsets.as_slice();
                    let mut indexes = vec![zero; capacity];
                    for (row, &[ring_start, ring_end]) in polys.array_windows::<2>().enumerate() {
                        let ring_start = ring_start as usize;
                        let ring_end = ring_end as usize;
                        if ring_start == ring_end {
                            continue;
                        }
                        let start = rings[ring_start] as usize;
                        let end = rings[ring_end] as usize;
                        indexes[start..end].fill(cast(row));
                    }
                    return indexes;
                },
                GeometryArrayStorage::Mixed(_) | GeometryArrayStorage::Packed(_) => {},
            }
        }

        // Scalar geometry: every coordinate belongs to the single owner (id 0).
        if matches!(&self.owner, CoordinateOwner::Shape(_)) {
            return vec![zero; capacity];
        }

        // Mask / gather / window / mixed: one fill per coordinate run, logical
        // row ids, missing rows skipped by `for_each`.
        let mut indexes = vec![zero; capacity];
        let mut remaining = indexes.as_mut_slice();
        self.for_each(&mut |geometry, run| {
            let len = match run {
                CoordRun::Seq(seq) => seq.len(),
                CoordRun::Point(_) => 1,
            };
            let id = cast(geometry.unwrap_or(0));
            let (output, rest) = std::mem::take(&mut remaining).split_at_mut(len);
            output.fill(id);
            remaining = rest;
        });
        debug_assert!(remaining.is_empty());
        indexes
    }

    /// The coordinate at `index` (flattened), or `None` if out of range.
    ///
    /// Packed identity columns and single-sequence shapes resolve in O(1) (or
    /// O(log runs) via CSR binary search). General owners walk runs with early
    /// break so `coords[0]` never scans past the first hit.
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "each remaining-index decrement is guarded by a comparison proving the minuend is large enough"
    )]
    pub fn point_at(&self, index: usize) -> Option<Point> {
        if let Some(point) = self.point_at_fast(index) {
            return Some(point);
        }
        let mut remaining = index;
        let result = self.try_for_each(&mut |_, run| match run {
            CoordRun::Seq(seq) => {
                let len = seq.len();
                if remaining < len {
                    ControlFlow::Break(seq.point_at(remaining))
                } else {
                    remaining -= len;
                    ControlFlow::Continue(())
                }
            },
            CoordRun::Point(point) => {
                if remaining == 0 {
                    ControlFlow::Break(point)
                } else {
                    remaining -= 1;
                    ControlFlow::Continue(())
                }
            },
        });
        match result {
            ControlFlow::Break(point) => Some(point),
            ControlFlow::Continue(()) => None,
        }
    }

    /// Storage-shaped random access for packed identity / window / gather
    /// columns and single-run shapes. O(1) for points; O(log rows) for
    /// contiguous CSR; gather of lines/polys falls through to the run walker.
    fn point_at_fast(&self, index: usize) -> Option<Point> {
        match &self.owner {
            CoordinateOwner::Shape(shape) => match shape.shape() {
                Shape::Point(point) if index == 0 => Some(*point),
                Shape::LineString(seq) if index < seq.len() => Some(seq.point_at(index)),
                Shape::MultiPoint(seq) if index < seq.len() => Some(seq.point_at(index)),
                Shape::Point(_)
                | Shape::MultiPoint(_)
                | Shape::LineString(_)
                | Shape::MultiLineString(_)
                | Shape::Polygon(_)
                | Shape::MultiPolygon(_)
                | Shape::GeometryCollection(_)
                | Shape::Empty(..) => None,
            },
            CoordinateOwner::Array {
                storage,
                missing: None,
            } => match storage.as_ref() {
                GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                    let map = row_map.as_deref();
                    let len = storage::point_logical_len(coords, map);
                    if index >= len {
                        return None;
                    }
                    let physical = map.physical(index);
                    Some(coords.point_at(physical))
                },
                GeometryArrayStorage::Packed(PackedStorage::Lines {
                    coords,
                    offsets,
                    row_map,
                }) => {
                    let map = row_map.as_deref();
                    let offsets = offsets.as_slice();
                    match map {
                        storage::RowSelectionRef::Identity
                        | crate::geometry::storage::RowSelectionRef::Window { .. } => {
                            packed_line_point_at(coords, offsets, map, index)
                        },
                        storage::RowSelectionRef::Gather(_) => None,
                    }
                },
                GeometryArrayStorage::Packed(PackedStorage::Polygons {
                    coords,
                    ring_offsets,
                    polygon_offsets,
                    row_map,
                }) => {
                    let map = row_map.as_deref();
                    match map {
                        storage::RowSelectionRef::Identity
                        | crate::geometry::storage::RowSelectionRef::Window { .. } => {
                            packed_polygon_point_at(
                                coords,
                                ring_offsets.as_slice(),
                                polygon_offsets.as_slice(),
                                map,
                                index,
                            )
                        },
                        storage::RowSelectionRef::Gather(_) => None,
                    }
                },
                // Mixed / masked / gather-of-ragged: flat index is not a
                // storage-shaped O(1) key — fall through to the run walker.
                GeometryArrayStorage::Mixed(_) => None,
            },
            CoordinateOwner::Array {
                missing: Some(_), ..
            } => None,
        }
    }

    /// Visible-value equality of two flattened views. Uses owner-identity and
    /// packed columnar bulk paths for full equal scans, and a dual sequential
    /// stream for the general / early-mismatch case. Never indexes the right
    /// side with per-coordinate CSR `point_at` (that is O(n log rows)).
    pub(crate) fn try_equal_visible(
        &self,
        py: Python<'_>,
        other: &Self,
        self_layout: Option<CoordinateAxes>,
        other_layout: Option<CoordinateAxes>,
    ) -> PyResult<bool> {
        // Same owner Arc (+ missing mask): identical flattened content.
        if owners_identical(&self.owner, &other.owner) && self_layout == other_layout {
            return Ok(true);
        }
        let left = self.exact_measure(py)?;
        let right = other.exact_measure(py)?;
        if !left.has_same_count(py, &right)? {
            return Ok(false);
        }
        // Same packed structure, no layout override: compare coordinate
        // columns run-wise without materializing Points.
        if self_layout.is_none()
            && other_layout.is_none()
            && let Some(equal) = try_columnar_storage_equal(&self.owner, &other.owner)
        {
            return Ok(equal);
        }
        // Dual sequential stream: walk left, materialize right once into a
        // visible-value buffer. Full equal is O(n); first mismatch early-exits
        // the left walk (right is already buffered — still O(n) prep, never
        // O(n log rows)).
        let count = right.len_for_python()?;
        let capacity =
            checked_vector_len::<([Option<u64>; 4], usize)>(count, "coordinate equality")?;
        let mut right_visible = Vec::with_capacity(capacity);
        other.for_each_point(|coord| {
            right_visible.push(visible_coordinate_bits(coord, other_layout));
        });
        let mut idx = 0_usize;
        let equal = self.try_for_each_point(&mut |coord| {
            let left = visible_coordinate_bits(coord, self_layout);
            if right_visible.get(idx).copied() != Some(left) {
                return ControlFlow::Break(false);
            }
            idx = idx.wrapping_add(1);
            ControlFlow::Continue(())
        });
        Ok(!matches!(equal, ControlFlow::Break(false)))
    }

    #[cfg(test)]
    pub fn equal_visible(
        &self,
        other: &Self,
        self_layout: Option<CoordinateAxes>,
        other_layout: Option<CoordinateAxes>,
    ) -> bool {
        Self::attach_test_python(|py| {
            self.try_equal_visible(py, other, self_layout, other_layout)
                .expect("test coordinate equality fits native storage")
        })
    }

    fn gather_ordinate(
        &self,
        py: Python<'_>,
        project: impl Fn(Point) -> f64,
    ) -> PyResult<Vec<f64>> {
        let count = self.len_for_python(py)?;
        let capacity = checked_vector_len::<f64>(count, "coordinate column")?;
        let mut out = Vec::with_capacity(capacity);
        self.for_each_point(|point| out.push(project(point)));
        Ok(out)
    }
}

fn owners_identical(left: &CoordinateOwner, right: &CoordinateOwner) -> bool {
    match (left, right) {
        (CoordinateOwner::Shape(a), CoordinateOwner::Shape(b)) => Arc::ptr_eq(a, b),
        (
            CoordinateOwner::Array {
                storage: sa,
                missing: ma,
            },
            CoordinateOwner::Array {
                storage: sb,
                missing: mb,
            },
        ) => Arc::ptr_eq(sa, sb) && ma == mb,
        _ => false,
    }
}

/// Bit-pattern column equality (NaN-stable, matching `Point`/`PartialEq`).
fn f64_columns_bit_eq(left: &[f64], right: &[f64]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right.iter())
        .all(|(a, b)| a.to_bits() == b.to_bits())
}

fn coordseq_columns_bit_eq(left: &CoordSeq, right: &CoordSeq) -> bool {
    left.axes() == right.axes()
        && f64_columns_bit_eq(left.xs(), right.xs())
        && f64_columns_bit_eq(left.ys(), right.ys())
        && match (left.zs(), right.zs()) {
            (None, None) => true,
            (Some(a), Some(b)) => f64_columns_bit_eq(a, b),
            _ => false,
        }
        && match (left.ms(), right.ms()) {
            (None, None) => true,
            (Some(a), Some(b)) => f64_columns_bit_eq(a, b),
            _ => false,
        }
}

#[expect(
    clippy::indexing_slicing,
    reason = "the packed span is checked against the shared CoordSeq column length before it is borrowed"
)]
fn borrowed_axis(
    coords: &CoordSeq,
    span: std::ops::Range<usize>,
    axis: CoordinateAxis,
) -> CoordinateColumnRef<'_> {
    match axis {
        CoordinateAxis::X => CoordinateColumnRef::Borrowed(&coords.xs()[span]),
        CoordinateAxis::Y => CoordinateColumnRef::Borrowed(&coords.ys()[span]),
        CoordinateAxis::Z => coords.zs().map_or(CoordinateColumnRef::Missing, |zs| {
            CoordinateColumnRef::Borrowed(&zs[span])
        }),
        CoordinateAxis::M => coords.ms().map_or(CoordinateColumnRef::Missing, |ms| {
            CoordinateColumnRef::Borrowed(&ms[span])
        }),
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "the checked end bounds both endpoints of this admitted CSR row window"
)]
fn line_window_coord_span(
    offsets: &[i32],
    coords_len: usize,
    start: usize,
    len: usize,
) -> Option<std::ops::Range<usize>> {
    let end = start.checked_add(len)?;
    if offsets.len() <= end {
        return None;
    }
    let start = offsets[start] as usize;
    let end = offsets[end] as usize;
    (end <= coords_len && start <= end).then_some(start..end)
}

#[expect(
    clippy::indexing_slicing,
    reason = "the checked polygon and ring endpoints bound the admitted nested CSR window"
)]
fn polygon_window_coord_span(
    ring_offsets: &[i32],
    polygon_offsets: &[i32],
    coords_len: usize,
    start: usize,
    len: usize,
) -> Option<std::ops::Range<usize>> {
    let end = start.checked_add(len)?;
    if polygon_offsets.len() <= end {
        return None;
    }
    let ring_start = polygon_offsets[start] as usize;
    let ring_end = polygon_offsets[end] as usize;
    if ring_offsets.len() <= ring_end || ring_start > ring_end {
        return None;
    }
    let start = ring_offsets[ring_start] as usize;
    let end = ring_offsets[ring_end] as usize;
    (end <= coords_len && start <= end).then_some(start..end)
}

/// Visible contiguous coordinate span for unmasked identity or windowed packed
/// storage. Gathered maps are not one span.
fn packed_contiguous_coord_span(
    packed: &PackedStorage,
) -> Option<(&CoordSeq, std::ops::Range<usize>)> {
    match packed {
        PackedStorage::Points { coords, row_map } => match row_map.as_deref() {
            RowSelectionRef::Identity => Some((coords.as_ref(), 0..coords.len())),
            RowSelectionRef::Window { start, len } => {
                let end = start.checked_add(len)?;
                (end <= coords.len()).then_some((coords.as_ref(), start..end))
            },
            RowSelectionRef::Gather(_) => None,
        },
        PackedStorage::Lines {
            coords,
            offsets,
            row_map,
        } => match row_map.as_deref() {
            RowSelectionRef::Identity => Some((coords.as_ref(), 0..coords.len())),
            RowSelectionRef::Window { start, len } => {
                let span = line_window_coord_span(offsets.as_slice(), coords.len(), start, len)?;
                Some((coords.as_ref(), span))
            },
            RowSelectionRef::Gather(_) => None,
        },
        PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        } => match row_map.as_deref() {
            RowSelectionRef::Identity => Some((coords.as_ref(), 0..coords.len())),
            RowSelectionRef::Window { start, len } => {
                let span = polygon_window_coord_span(
                    ring_offsets.as_slice(),
                    polygon_offsets.as_slice(),
                    coords.len(),
                    start,
                    len,
                )?;
                Some((coords.as_ref(), span))
            },
            RowSelectionRef::Gather(_) => None,
        },
    }
}

fn coord_views_bit_eq(
    left: &CoordSeq,
    left_span: std::ops::Range<usize>,
    right: &CoordSeq,
    right_span: std::ops::Range<usize>,
) -> bool {
    let left = left.view(CoordWindow::from_range(left_span, left.len()));
    let right = right.view(CoordWindow::from_range(right_span, right.len()));
    coordseq_columns_bit_eq(&left, &right)
}

fn row_selection_eq(left: &storage::RowSelection, right: &storage::RowSelection) -> bool {
    match (left.as_deref(), right.as_deref()) {
        (storage::RowSelectionRef::Identity, storage::RowSelectionRef::Identity) => true,
        (
            crate::geometry::storage::RowSelectionRef::Window { start: sa, len: la },
            crate::geometry::storage::RowSelectionRef::Window { start: sb, len: lb },
        ) => sa == sb && la == lb,
        (storage::RowSelectionRef::Gather(a), storage::RowSelectionRef::Gather(b)) => a == b,
        _ => false,
    }
}

/// When both owners are packed arrays with comparable structure, compare
/// columns (and CSR maps) without Point materialization. `None` means fall
/// back to the dual stream (mixed / masked / cross-kind).
fn try_columnar_storage_equal(left: &CoordinateOwner, right: &CoordinateOwner) -> Option<bool> {
    match (left, right) {
        (CoordinateOwner::Shape(a), CoordinateOwner::Shape(b)) => shape_columnar_equal(a, b),
        (
            CoordinateOwner::Array {
                storage: sa,
                missing: None,
            },
            CoordinateOwner::Array {
                storage: sb,
                missing: None,
            },
        ) => packed_columnar_equal(sa, sb),
        _ => None,
    }
}

fn shape_columnar_equal(left: &ShapeData, right: &ShapeData) -> Option<bool> {
    match (left.shape(), right.shape()) {
        (Shape::LineString(la), Shape::LineString(lb)) => {
            Some(coordseq_columns_bit_eq(la.as_coords(), lb.as_coords()))
        },
        (Shape::MultiPoint(la), Shape::MultiPoint(lb)) => Some(coordseq_columns_bit_eq(la, lb)),
        (Shape::Point(pa), Shape::Point(pb)) => Some(pa == pb),
        (Shape::Empty(ka, aa), Shape::Empty(kb, ab)) => Some(ka == kb && aa == ab),
        // Multipart / polygon: fall back to dual stream.
        _ => None,
    }
}

fn packed_columnar_equal(
    left: &GeometryArrayStorage,
    right: &GeometryArrayStorage,
) -> Option<bool> {
    match (left, right) {
        (
            GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: ca,
                row_map: ma,
            }),
            GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: cb,
                row_map: mb,
            }),
        ) => packed_points_columnar_eq(ca, ma, cb, mb),
        (
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords: ca,
                offsets: oa,
                row_map: ma,
            }),
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords: cb,
                offsets: ob,
                row_map: mb,
            }),
        ) => packed_lines_columnar_eq(ca, oa.as_slice(), ma, cb, ob.as_slice(), mb),
        (
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords: ca,
                ring_offsets: ra,
                polygon_offsets: pa,
                row_map: ma,
            }),
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords: cb,
                ring_offsets: rb,
                polygon_offsets: pb,
                row_map: mb,
            }),
        ) => packed_polygons_columnar_eq(
            ca,
            ra.as_slice(),
            pa.as_slice(),
            ma,
            cb,
            rb.as_slice(),
            pb.as_slice(),
            mb,
        ),
        // Mixed or cross-kind: stream.
        _ => None,
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "admitted row windows make start plus length representable before constructing their coordinate spans"
)]
fn packed_points_columnar_eq(
    ca: &CoordSeq,
    ma: &storage::RowSelection,
    cb: &CoordSeq,
    mb: &storage::RowSelection,
) -> Option<bool> {
    // Cross row-map pairs (identity vs window/gather, gather vs rebuild) can
    // still be value-equal — never answer False solely from map shape.
    if !row_selection_eq(ma, mb) {
        return None;
    }
    match ma.as_deref() {
        storage::RowSelectionRef::Identity => Some(coordseq_columns_bit_eq(ca, cb)),
        crate::geometry::storage::RowSelectionRef::Window { start, len } => {
            let la = ca.view(CoordWindow::from_range(start..start + len, ca.len()));
            let lb = cb.view(CoordWindow::from_range(start..start + len, cb.len()));
            Some(coordseq_columns_bit_eq(&la, &lb))
        },
        // Gathered points: physical rows may differ — stream is the safe path.
        storage::RowSelectionRef::Gather(_) => None,
    }
}

/// Flattened `Coordinates` equality cares about the visible vertex stream only.
/// CSR row/ring breaks do not change flatten order when both sides walk the
/// same packed column in row order — never require absolute offsets to match.
fn packed_lines_columnar_eq(
    ca: &CoordSeq,
    oa: &[i32],
    ma: &storage::RowSelection,
    cb: &CoordSeq,
    ob: &[i32],
    mb: &storage::RowSelection,
) -> Option<bool> {
    if !row_selection_eq(ma, mb) {
        return None;
    }
    match ma.as_deref() {
        // Identity: flattened stream IS the coord columns (offsets only
        // partition rows; different breaks with the same verts stay equal).
        storage::RowSelectionRef::Identity => Some(coordseq_columns_bit_eq(ca, cb)),
        crate::geometry::storage::RowSelectionRef::Window { start, len } => {
            // Per-side absolute spans (prefixes may differ); compare only the
            // visible coordinate slices, never the absolute offset numbers.
            let Some(left) = line_window_coord_span(oa, ca.len(), start, len) else {
                return Some(false);
            };
            let Some(right) = line_window_coord_span(ob, cb.len(), start, len) else {
                return Some(false);
            };
            Some(coord_views_bit_eq(ca, left, cb, right))
        },
        storage::RowSelectionRef::Gather(_) => None,
    }
}

fn packed_polygons_columnar_eq(
    ca: &CoordSeq,
    ra: &[i32],
    pa: &[i32],
    ma: &storage::RowSelection,
    cb: &CoordSeq,
    rb: &[i32],
    pb: &[i32],
    mb: &storage::RowSelection,
) -> Option<bool> {
    if !row_selection_eq(ma, mb) {
        return None;
    }
    match ma.as_deref() {
        // Same as lines: flatten order is the packed coord column; CSR only
        // partitions rings/polygons.
        storage::RowSelectionRef::Identity => Some(coordseq_columns_bit_eq(ca, cb)),
        crate::geometry::storage::RowSelectionRef::Window { start, len } => {
            // Visible polygons start..end → ring range via polygon_offsets →
            // coord span via ring_offsets. Different absolute prefixes are fine.
            let Some(left) = polygon_window_coord_span(ra, pa, ca.len(), start, len) else {
                return Some(false);
            };
            let Some(right) = polygon_window_coord_span(rb, pb, cb.len(), start, len) else {
                return Some(false);
            };
            Some(coord_views_bit_eq(ca, left, cb, right))
        },
        storage::RowSelectionRef::Gather(_) => None,
    }
}

/// Visible ordinate fingerprint used by layout-aware equality (NaN-stable).
#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "the closed XY/Z/M layout has at most four slots, and n advances only for present axes"
)]
fn visible_coordinate_bits(
    point: Point,
    layout: Option<CoordinateAxes>,
) -> ([Option<u64>; 4], usize) {
    let axes = layout.unwrap_or_else(|| CoordinateAxes::from_point(point));
    let mut values = [None; 4];
    values[0] = Some(point.x.to_bits());
    values[1] = Some(point.y.to_bits());
    let mut n = 2;
    if axes.has_z() {
        values[n] = point.z().map(f64::to_bits);
        n += 1;
    }
    if axes.has_m() {
        values[n] = point.m().map(f64::to_bits);
        n += 1;
    }
    (values, n)
}

impl CoordinateView {
    fn gather_optional(
        &self,
        py: Python<'_>,
        project: impl Fn(Point) -> Option<f64>,
    ) -> PyResult<CoordinateColumnRef<'_>> {
        // One pass into a dense f64 lane + 1-byte presence mask (≈9 B/coord).
        // Homogeneous present → Dense (no Option). Fully absent → Missing.
        // Mixed → Nullable once at the end so list paths keep None for absent
        // ordinates while still preserving Some(NaN) as a real measured value
        // (a NaN-only lane would collapse the two). Peak is the dense+mask
        // working set, not a 16 B Option lane overlapping a dense prefix.
        let count = self.len_for_python(py)?;
        let capacity = checked_vector_len::<f64>(count, "coordinate column")?;
        checked_vector_len::<bool>(count, "coordinate presence mask")?;
        checked_vector_len::<Option<f64>>(count, "nullable coordinate column")?;
        let mut values = Vec::with_capacity(capacity);
        let mut present = Vec::with_capacity(capacity);
        let mut any = false;
        let mut all_present = true;
        self.for_each_point(|coord| {
            let (value, is_present) = project(coord).map_or_else(
                || {
                    all_present = false;
                    (0.0, false)
                },
                |value| {
                    any = true;
                    (value, true)
                },
            );
            values.push(value);
            present.push(is_present);
        });
        if !any {
            return Ok(CoordinateColumnRef::Missing);
        }
        if all_present {
            return Ok(CoordinateColumnRef::Dense(values));
        }
        let nullable = values
            .into_iter()
            .zip(present)
            .map(|(value, is_present)| is_present.then_some(value))
            .collect();
        Ok(CoordinateColumnRef::Nullable(nullable))
    }
}

/// O(1) flat-index lookup into packed line CSR under identity or a contiguous
/// row window, whose coordinates are one contiguous span. Gather falls
/// through (non-contiguous coord spans).
#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "identity and window rows are admitted by their CSR column; the flat index is checked against its resolved span"
)]
fn packed_line_point_at(
    coords: &CoordSeq,
    offsets: &[i32],
    map: storage::RowSelectionRef<'_>,
    index: usize,
) -> Option<Point> {
    if offsets.len() < 2 {
        return None;
    }
    let span = match map {
        storage::RowSelectionRef::Identity => 0..offsets[offsets.len() - 1] as usize,
        crate::geometry::storage::RowSelectionRef::Window { start, len } => {
            offsets[start] as usize..offsets[start + len] as usize
        },
        storage::RowSelectionRef::Gather(_) => return None,
    };
    (index < span.len()).then(|| coords.point_at(span.start + index))
}

/// O(1) flat-index lookup into packed polygon CSR under identity or a
/// contiguous row window, whose coordinates are one contiguous span.
#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "identity and window rows are admitted by nested CSR columns; the flat index is checked against its resolved span"
)]
fn packed_polygon_point_at(
    coords: &CoordSeq,
    rings: &[i32],
    polys: &[i32],
    map: storage::RowSelectionRef<'_>,
    index: usize,
) -> Option<Point> {
    if rings.len() < 2 || polys.len() < 2 {
        return None;
    }
    let span = match map {
        storage::RowSelectionRef::Identity => 0..rings[rings.len() - 1] as usize,
        crate::geometry::storage::RowSelectionRef::Window { start, len } => {
            rings[polys[start] as usize] as usize..rings[polys[start + len] as usize] as usize
        },
        storage::RowSelectionRef::Gather(_) => return None,
    };
    (index < span.len()).then(|| coords.point_at(span.start + index))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::storage::RowSelection;
    use crate::geometry::{LineSeq, PolygonLevel, Ring, SharedSlice};
    use crate::{Frame, PyGeometryArray};

    fn pt(x: f64, y: f64) -> Point {
        Point::new(x, y).unwrap()
    }

    fn line(coords: &[(f64, f64)]) -> Arc<ShapeData> {
        Arc::new(ShapeData::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(
                    &coords.iter().map(|&(x, y)| pt(x, y)).collect::<Vec<_>>(),
                )
                .unwrap(),
            )
            .expect("test line is valid"),
        )))
    }

    #[test]
    fn point_view_is_one_coordinate() {
        let view = CoordinateView::from_shape(Arc::new(ShapeData::new(Shape::Point(pt(1.0, 2.0)))));
        assert_eq!(view.len(), 1);
        assert!(view.single_seq().is_none());
        assert_eq!(view.point_at(0).unwrap(), pt(1.0, 2.0));
        assert!(view.point_at(1).is_none());
    }

    #[test]
    fn linestring_view_is_zero_copy() {
        let view = CoordinateView::from_shape(line(&[(0.0, 0.0), (1.0, 2.0), (3.0, 4.0)]));
        assert_eq!(view.len(), 3);
        let seq = view.single_seq().expect("one contiguous run");
        assert_eq!(seq.xs(), &[0.0, 1.0, 3.0]);
        assert_eq!(seq.ys(), &[0.0, 2.0, 4.0]);
        assert!(matches!(
            view.column(CoordinateAxis::X),
            CoordinateColumnRef::Borrowed(_)
        ));
        assert!(matches!(
            view.column(CoordinateAxis::Z),
            CoordinateColumnRef::Missing
        ));
    }

    #[test]
    fn polygon_view_flattens_shell_then_holes() {
        let shell =
            Ring::closed(vec![pt(0.0, 0.0), pt(4.0, 0.0), pt(4.0, 4.0), pt(0.0, 4.0)]).unwrap();
        let hole =
            Ring::closed(vec![pt(1.0, 1.0), pt(2.0, 1.0), pt(2.0, 2.0), pt(1.0, 2.0)]).unwrap();
        let view = CoordinateView::from_shape(Arc::new(ShapeData::new(Shape::Polygon(Polygon {
            shell,
            holes: SharedSlice::from_vec(vec![hole]),
        }))));
        assert!(view.single_seq().is_none());
        let points = view.collect_points();
        assert_eq!(points.len(), view.len());
        // Shell first, then the hole.
        assert_eq!(points[3], pt(0.0, 4.0));
        assert_eq!(points[5], pt(1.0, 1.0));
        // X column over the multipart polygon materializes (no single run).
        assert!(matches!(
            view.column(CoordinateAxis::X),
            CoordinateColumnRef::Dense(_)
        ));
    }

    #[test]
    fn packed_polygon_view_walks_csr_rows_and_rings_in_order() {
        let coords = CoordSeq::try_from_points(&Vec::<Point>::from([
            pt(0.0, 0.0),
            pt(4.0, 0.0),
            pt(4.0, 4.0),
            pt(0.0, 4.0),
            pt(0.0, 0.0),
            pt(1.0, 1.0),
            pt(2.0, 1.0),
            pt(2.0, 2.0),
            pt(1.0, 2.0),
            pt(1.0, 1.0),
            pt(10.0, 10.0),
            pt(11.0, 10.0),
            pt(11.0, 11.0),
            pt(10.0, 11.0),
            pt(10.0, 10.0),
        ]))
        .unwrap();
        let ring_offsets = CsrOffsetColumn::<RingLevel>::try_new(vec![0, 5, 10, 15], 15).unwrap();
        let polygon_offsets = CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 2, 3], 3).unwrap();
        let array = PyGeometryArray::packed_polygons(
            coords.clone(),
            ring_offsets.clone(),
            polygon_offsets.clone(),
            Frame::default(),
        )
        .unwrap();
        let points = array.coordinate_view().collect_points();

        assert_eq!(points.len(), 15);
        assert_eq!(points[5], pt(1.0, 1.0));
        assert_eq!(points[10], pt(10.0, 10.0));
        assert_eq!(points[13], pt(10.0, 11.0));
        assert_eq!(
            array.coordinate_view().row_index_i64(),
            [[0; 10].as_slice(), &[1; 5]].concat()
        );

        let gathered = PyGeometryArray::packed_polygons_mapped(
            coords,
            ring_offsets,
            polygon_offsets,
            Frame::default(),
            RowSelection::gather_checked_native(vec![1, 0], 2).unwrap(),
        )
        .unwrap();
        let gathered_points = gathered.coordinate_view().collect_points();
        assert_eq!(gathered_points.len(), 15);
        assert_eq!(gathered_points[0], pt(10.0, 10.0));
        assert_eq!(gathered_points[5], pt(0.0, 0.0));
        assert_eq!(gathered_points[10], pt(1.0, 1.0));
        assert_eq!(
            gathered.coordinate_view().row_index_i64(),
            [[0; 5].as_slice(), &[1; 10]].concat()
        );

        let masked = gathered
            .with_missing_mask(ValidityMask::from_vec(2, vec![true, false]))
            .unwrap();
        let masked_view = masked.coordinate_view();
        let masked_points = masked_view.collect_points();
        assert_eq!(masked_view.len(), 10);
        assert_eq!(masked_points.len(), 10);
        assert_eq!(masked_view.row_index_i64(), vec![1; 10]);
        assert_eq!(masked_points[0], pt(0.0, 0.0));
        assert_eq!(masked_points[5], pt(1.0, 1.0));
    }

    #[test]
    fn identity_packed_lines_row_index_fills_from_csr_offsets() {
        let coords = CoordSeq::try_from_points(&[
            pt(0.0, 0.0),
            pt(1.0, 0.0),
            pt(2.0, 0.0),
            pt(3.0, 0.0),
            pt(4.0, 0.0),
        ])
        .unwrap();
        let offsets = CsrOffsetColumn::try_new(vec![0, 2, 5], 5).unwrap();
        let array = PyGeometryArray::packed_lines(coords, offsets, Frame::default()).unwrap();
        let view = array.coordinate_view();
        assert_eq!(view.row_index_i64(), vec![0, 0, 1, 1, 1]);
        assert_eq!(view.row_index(), vec![0, 0, 1, 1, 1]);
        assert!(view.single_seq().is_some());
    }

    #[test]
    fn identity_packed_polygons_row_index_spans_all_rings() {
        let coords = CoordSeq::try_from_points(&Vec::<Point>::from([
            pt(0.0, 0.0),
            pt(4.0, 0.0),
            pt(4.0, 4.0),
            pt(0.0, 4.0),
            pt(0.0, 0.0),
            pt(1.0, 1.0),
            pt(2.0, 1.0),
            pt(2.0, 2.0),
            pt(1.0, 2.0),
            pt(1.0, 1.0),
            pt(10.0, 10.0),
            pt(11.0, 10.0),
            pt(11.0, 11.0),
            pt(10.0, 11.0),
            pt(10.0, 10.0),
        ]))
        .unwrap();
        let ring_offsets = CsrOffsetColumn::<RingLevel>::try_new(vec![0, 5, 10, 15], 15).unwrap();
        let polygon_offsets = CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 2, 3], 3).unwrap();
        let array = PyGeometryArray::packed_polygons(
            coords,
            ring_offsets,
            polygon_offsets,
            Frame::default(),
        )
        .unwrap();
        let view = array.coordinate_view();
        // Row 0: shell (5) + hole (5); row 1: shell (5).
        assert_eq!(view.row_index_i64(), vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1
        ]);
    }

    #[test]
    fn gathered_packed_lines_row_index_uses_logical_rows() {
        let coords = CoordSeq::try_from_points(&Vec::<Point>::from([
            pt(0.0, 0.0),
            pt(1.0, 0.0),
            pt(2.0, 0.0),
            pt(3.0, 0.0),
            pt(4.0, 0.0),
            pt(5.0, 0.0),
        ]))
        .unwrap();
        let offsets = CsrOffsetColumn::try_new(vec![0, 2, 4, 6], 6).unwrap();
        let gathered = PyGeometryArray::packed_lines_mapped(
            coords,
            offsets,
            Frame::default(),
            RowSelection::gather_checked_native(vec![2, 0], 3).unwrap(),
        )
        .unwrap();
        let view = gathered.coordinate_view();
        // Logical row 0 = physical 2 (2 verts); logical row 1 = physical 0 (2 verts).
        assert_eq!(view.row_index_i64(), vec![0, 0, 1, 1]);
        let points = view.collect_points();
        assert_eq!(points[0], pt(4.0, 0.0));
        assert_eq!(points[2], pt(0.0, 0.0));
    }

    #[test]
    fn collection_flattens_in_declaration_order() {
        let collection = Shape::geometry_collection(vec![
            Shape::Point(pt(9.0, 9.0)),
            Shape::LineString(
                LineSeq::try_new(CoordSeq::try_from_points(&[pt(0.0, 0.0), pt(1.0, 1.0)]).unwrap())
                    .expect("test line is valid"),
            ),
        ])
        .expect("test collection is valid");
        let view = CoordinateView::from_shape(Arc::new(ShapeData::new(collection)));
        assert_eq!(view.len(), 3);
        let points = view.collect_points();
        assert_eq!(points[0], pt(9.0, 9.0));
        assert_eq!(points[1], pt(0.0, 0.0));
        assert_eq!(points[2], pt(1.0, 1.0));
        assert_eq!(view.row_index(), vec![0, 0, 0]);
    }

    #[test]
    fn optional_z_preserves_absent_vs_nan() {
        // Mixed Z presence must keep None for axes-absent vertices and
        // Some(NaN) for a measured NaN — the list path needs the distinction,
        // while NumPy maps both to NaN at the boundary.
        use crate::geometry::{MOrdinate, ZOrdinate};
        let collection = Shape::geometry_collection(vec![
            Shape::Point(pt(0.0, 0.0)),
            Shape::Point(Point::new_raw_axes(
                1.0,
                1.0,
                ZOrdinate(Some(f64::NAN)),
                MOrdinate(None),
            )),
            Shape::Point(
                Point::new_axes(2.0, 2.0, ZOrdinate(Some(9.0)), MOrdinate(None))
                    .expect("finite test Z"),
            ),
        ])
        .expect("test collection is valid");
        let view = CoordinateView::from_shape(Arc::new(ShapeData::new(collection)));
        match view.column(CoordinateAxis::Z) {
            CoordinateColumnRef::Nullable(values) => {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], None);
                assert!(values[1].is_some_and(f64::is_nan));
                assert_eq!(values[2], Some(9.0));
            },
            CoordinateColumnRef::Missing => panic!("expected Nullable Z column, got Missing"),
            CoordinateColumnRef::Borrowed(_) => panic!("expected Nullable Z column, got Borrowed"),
            CoordinateColumnRef::Dense(_) => panic!("expected Nullable Z column, got Dense"),
        }
    }

    fn closed_box(left: f64, bottom: f64, right: f64, top: f64) -> [Point; 5] {
        [
            pt(left, bottom),
            pt(right, bottom),
            pt(right, top),
            pt(left, top),
            pt(left, bottom),
        ]
    }

    fn packed_polygons(
        coords: CoordSeq,
        ring_ends: Vec<usize>,
        polygon_ends: Vec<usize>,
        rows: std::ops::Range<usize>,
    ) -> PyGeometryArray {
        let n_rings = ring_ends
            .len()
            .checked_sub(1)
            .expect("ring offsets include their initial zero");
        let n_polygons = polygon_ends
            .len()
            .checked_sub(1)
            .expect("polygon offsets include their initial zero");
        let ring_offsets = CsrOffsetColumn::<RingLevel>::try_new(ring_ends, coords.len()).unwrap();
        let polygon_offsets =
            CsrOffsetColumn::<PolygonLevel>::try_new(polygon_ends, n_rings).unwrap();
        PyGeometryArray::packed_polygons_mapped(
            coords,
            ring_offsets,
            polygon_offsets,
            Frame::default(),
            storage::row_selection_from_logical_rows(
                storage::RowSelectionRef::Identity,
                n_polygons,
                rows,
            ),
        )
        .unwrap()
    }

    fn concat3(a: &[Point; 5], b: &[Point; 5], c: &[Point; 5]) -> [Point; 15] {
        let mut points = [pt(0.0, 0.0); 15];
        points[..5].copy_from_slice(a);
        points[5..10].copy_from_slice(b);
        points[10..].copy_from_slice(c);
        points
    }

    #[test]
    fn packed_polygon_windows_with_unequal_prefixes_equal_without_buffering() {
        let left_points = concat3(
            &closed_box(9.0, 9.0, 9.5, 9.5),
            &closed_box(1.0, 0.0, 3.0, 2.0),
            &closed_box(4.0, 0.0, 5.0, 1.0),
        );
        let left = packed_polygons(
            CoordSeq::try_from_points(&left_points).unwrap(),
            vec![0, 5, 10, 15],
            vec![0, 1, 2, 3],
            1..3,
        );
        let prefix = [pt(8.0, 8.0), pt(8.0, 7.0), pt(7.0, 8.0), pt(8.0, 8.0)];
        let vis0 = closed_box(1.0, 0.0, 3.0, 2.0);
        let vis1 = closed_box(4.0, 0.0, 5.0, 1.0);
        let mut right_points = [pt(0.0, 0.0); 14];
        right_points[..4].copy_from_slice(&prefix);
        right_points[4..9].copy_from_slice(&vis0);
        right_points[9..].copy_from_slice(&vis1);
        let right = packed_polygons(
            CoordSeq::try_from_points(&right_points).unwrap(),
            vec![0, 4, 9, 14],
            vec![0, 1, 2, 3],
            1..3,
        );
        let left_view = left.coordinate_view();
        let right_view = right.coordinate_view();
        assert_eq!(left_view.len(), 10);
        assert_eq!(right_view.len(), 10);
        let equal = crate::test_support::assert_no_allocations(|| {
            left_view.equal_visible(&right_view, None, None)
        });
        assert!(equal);
        let mut mismatched = right_points;
        mismatched[6] = pt(1.5, 0.5);
        let other = packed_polygons(
            CoordSeq::try_from_points(&mismatched).unwrap(),
            vec![0, 4, 9, 14],
            vec![0, 1, 2, 3],
            1..3,
        );
        assert!(!left_view.equal_visible(&other.coordinate_view(), None, None));
    }

    #[test]
    fn packed_polygon_empty_windows_compare_equal() {
        let left = packed_polygons(
            CoordSeq::try_from_points(&closed_box(0.0, 0.0, 1.0, 1.0)).unwrap(),
            vec![0, 5],
            vec![0, 1],
            0..0,
        );
        let right = packed_polygons(
            CoordSeq::try_from_points(&[pt(2.0, 2.0), pt(3.0, 2.0), pt(2.0, 3.0), pt(2.0, 2.0)])
                .unwrap(),
            vec![0, 4],
            vec![0, 1],
            0..0,
        );
        assert_eq!(left.coordinate_view().len(), 0);
        assert_eq!(right.coordinate_view().len(), 0);
        assert!(
            left.coordinate_view()
                .equal_visible(&right.coordinate_view(), None, None)
        );
    }

    #[test]
    fn packed_polygon_window_column_borrows_visible_span() {
        use crate::geometry::{MColumn, MOrdinate, ZOrdinate};
        let ring = |z: f64, m: f64| {
            [
                Point::new_axes(0.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
                Point::new_axes(1.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
                Point::new_axes(1.0, 1.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
                Point::new_axes(0.0, 1.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
                Point::new_axes(0.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
            ]
        };
        let xy = concat3(&ring(1.0, 10.0), &ring(2.0, 20.0), &ring(3.0, 30.0));
        let xs: [f64; 15] = std::array::from_fn(|i| xy[i].x);
        let ys: [f64; 15] = std::array::from_fn(|i| xy[i].y);
        let zs: [f64; 15] = std::array::from_fn(|i| xy[i].z().unwrap());
        let m_owner =
            SharedSlice::from_vec((0_u8..20).map(|value| f64::from(value) + 50.0).collect());
        let coords = CoordSeq::from_shared_columns(
            SharedSlice::from_vec(xs.to_vec()),
            SharedSlice::from_vec(ys.to_vec()),
            Some(SharedSlice::from_vec(zs.to_vec())),
            Some(MColumn::Local {
                owner: m_owner.clone(),
                offset: 3,
            }),
        )
        .unwrap();
        let array = packed_polygons(coords.clone(), vec![0, 5, 10, 15], vec![0, 1, 2, 3], 1..2);
        let view = array.coordinate_view();
        let (_, allocations) =
            crate::test_support::allocation_count(|| view.column(CoordinateAxis::X));
        assert_eq!(allocations, 0);
        let CoordinateColumnRef::Borrowed(column) = view.column(CoordinateAxis::X) else {
            panic!("expected borrowed X");
        };
        assert_eq!(column, &coords.xs()[5..10]);
        assert_eq!(column.as_ptr(), coords.xs()[5..10].as_ptr());
        let CoordinateColumnRef::Borrowed(column) = view.column(CoordinateAxis::Z) else {
            panic!("expected borrowed Z");
        };
        assert_eq!(column, &zs[5..10]);
        assert_eq!(column.as_ptr(), coords.zs().unwrap()[5..10].as_ptr());
        let CoordinateColumnRef::Borrowed(column) = view.column(CoordinateAxis::M) else {
            panic!("expected borrowed M");
        };
        assert_eq!(column, &m_owner.as_slice()[8..13]);
        assert_eq!(column.as_ptr(), m_owner.as_slice()[8..13].as_ptr());
        let xy_only = packed_polygons(
            CoordSeq::try_from_points(&concat3(
                &closed_box(0.0, 0.0, 1.0, 1.0),
                &closed_box(0.0, 0.0, 1.0, 1.0),
                &closed_box(0.0, 0.0, 1.0, 1.0),
            ))
            .unwrap(),
            vec![0, 5, 10, 15],
            vec![0, 1, 2, 3],
            1..2,
        );
        assert!(matches!(
            xy_only.coordinate_view().column(CoordinateAxis::Z),
            CoordinateColumnRef::Missing
        ));
        assert!(matches!(
            xy_only.coordinate_view().column(CoordinateAxis::M),
            CoordinateColumnRef::Missing
        ));
        assert!(!view.equal_visible(&xy_only.coordinate_view(), None, None));
    }

    #[test]
    fn packed_line_window_column_borrows_visible_span() {
        let coords = CoordSeq::try_from_points(&[
            pt(9.0, 9.0),
            pt(9.0, 8.0),
            pt(1.0, 0.0),
            pt(2.0, 0.0),
            pt(3.0, 0.0),
            pt(4.0, 0.0),
            pt(5.0, 0.0),
        ])
        .unwrap();
        let offsets = CsrOffsetColumn::try_new(vec![0, 2, 5, 7], 7).unwrap();
        let array = PyGeometryArray::packed_lines_mapped(
            coords.clone(),
            offsets,
            Frame::default(),
            storage::row_selection_from_logical_rows(storage::RowSelectionRef::Identity, 3, 1..3),
        )
        .unwrap();
        let view = array.coordinate_view();
        let (_, allocations) =
            crate::test_support::allocation_count(|| view.column(CoordinateAxis::X));
        assert_eq!(allocations, 0);
        let CoordinateColumnRef::Borrowed(xs) = view.column(CoordinateAxis::X) else {
            panic!("expected borrowed X");
        };
        assert_eq!(xs, &coords.xs()[2..7]);
        assert_eq!(xs.as_ptr(), coords.xs()[2..7].as_ptr());
    }

    #[test]
    fn packed_polygon_window_zm_compares_optional_axes() {
        use crate::geometry::{MOrdinate, ZOrdinate};
        let ring = |z: f64, m: f64| {
            [
                Point::new_axes(0.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
                Point::new_axes(1.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
                Point::new_axes(1.0, 1.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
                Point::new_axes(0.0, 1.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
                Point::new_axes(0.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
            ]
        };
        let mut left_points = [pt(0.0, 0.0); 10];
        left_points[..5].copy_from_slice(&ring(0.0, 10.0));
        left_points[5..].copy_from_slice(&ring(1.0, 20.0));
        let mut right_points = left_points;
        let prefix_start =
            Point::new_axes(2.0, 2.0, ZOrdinate(Some(9.0)), MOrdinate(Some(10.0))).unwrap();
        right_points[0] = prefix_start;
        right_points[4] = prefix_start;
        let left = packed_polygons(
            CoordSeq::try_from_points(&left_points).unwrap(),
            vec![0, 5, 10],
            vec![0, 1, 2],
            1..2,
        );
        let right = packed_polygons(
            CoordSeq::try_from_points(&right_points).unwrap(),
            vec![0, 5, 10],
            vec![0, 1, 2],
            1..2,
        );
        assert!(
            left.coordinate_view()
                .equal_visible(&right.coordinate_view(), None, None)
        );
        let mut z_mismatch = right_points;
        let z_start =
            Point::new_axes(0.0, 0.0, ZOrdinate(Some(3.0)), MOrdinate(Some(20.0))).unwrap();
        z_mismatch[5] = z_start;
        z_mismatch[9] = z_start;
        let z_other = packed_polygons(
            CoordSeq::try_from_points(&z_mismatch).unwrap(),
            vec![0, 5, 10],
            vec![0, 1, 2],
            1..2,
        );
        assert!(
            !left
                .coordinate_view()
                .equal_visible(&z_other.coordinate_view(), None, None)
        );
        let mut m_mismatch = right_points;
        m_mismatch[6] =
            Point::new_axes(1.0, 0.0, ZOrdinate(Some(1.0)), MOrdinate(Some(99.0))).unwrap();
        let m_other = packed_polygons(
            CoordSeq::try_from_points(&m_mismatch).unwrap(),
            vec![0, 5, 10],
            vec![0, 1, 2],
            1..2,
        );
        assert!(
            !left
                .coordinate_view()
                .equal_visible(&m_other.coordinate_view(), None, None)
        );
    }
}
