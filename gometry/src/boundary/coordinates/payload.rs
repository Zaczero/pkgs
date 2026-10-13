use std::collections::HashMap;
use std::ops::{ControlFlow, Range};

use pyo3::types::PyInt;
use pyo3::{Bound, PyResult, Python};

use super::count::packed_range_count_and_axes;
use super::integer::add_python_integers;
use super::{CoordSeq, CoordinateView, Polygon, Shape};
use crate::geometry::CoordinateAxes;
use crate::geometry::storage::{GeometryArrayStorage, ValidityMask};
use crate::geometry::types::ShapeOwnerKey;

#[derive(Clone)]
enum ExactPayloadValue<'py> {
    Small(usize),
    Wide(u128),
    Big(Bound<'py, PyInt>),
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
pub(super) struct PayloadBytesQuery<'py> {
    collections: CollectionPayloadMemo<'py>,
}

impl<'py> PayloadBytesQuery<'py> {
    pub(super) fn storage_range(
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

impl CoordinateView {
    pub(crate) fn nbytes_pyint<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyInt>> {
        Ok(self.exact_measure(py)?.bytes_pyint(py))
    }
}
