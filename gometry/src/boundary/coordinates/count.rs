use std::collections::HashMap;
use std::ops::Range;

use pyo3::exceptions::PyOverflowError;
use pyo3::types::PyInt;
use pyo3::{Bound, PyResult, Python};

use super::integer::ExactUnsigned;
use super::{
    CoordSeq, CoordinateMeasureQuery, CoordinateOwner, CoordinateView, Shape, measure_shape,
    measure_storage,
};
use crate::geometry::CoordinateAxes;
use crate::geometry::storage::{
    self, GeometryArrayStorage, PackedStorage, ShapeRow, ValidityMask, csr_value_window,
};
use crate::geometry::types::ShapeOwnerKey;

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

    pub(super) fn has_same_count(&self, py: Python<'py>, other: &Self) -> PyResult<bool> {
        self.coordinates.equals(py, &other.coordinates)
    }

    pub(crate) const fn count_i64(&self) -> Option<i64> {
        match &self.coordinates {
            ExactUnsigned::Small(value) if *value <= i64::MAX as u128 => Some(*value as i64),
            ExactUnsigned::Small(_) | ExactUnsigned::Big(_) => None,
        }
    }
}

pub(super) type CollectionMeasures<'py> = Option<HashMap<ShapeOwnerKey, CoordinateMeasure<'py>>>;

pub(super) fn packed_range_count_and_axes(
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

impl<'py> CoordinateMeasureQuery<'py> {
    pub(crate) fn row(
        &mut self,
        py: Python<'py>,
        row: ShapeRow<'_>,
    ) -> PyResult<CoordinateMeasure<'py>> {
        match row {
            ShapeRow::Packed(row) => Ok(CoordinateMeasure::from_count_and_axes(
                row.coord_count() as u128,
                row.axes(),
            )),
            ShapeRow::Point(point) => Ok(CoordinateMeasure::from_count_and_axes(
                1,
                CoordinateAxes::from_point(point),
            )),
            ShapeRow::Handle(handle) => self.shape(py, handle.shape()),
            ShapeRow::Shape(shape) => self.shape(py, shape),
        }
    }

    pub(crate) fn shape(
        &mut self,
        py: Python<'py>,
        shape: &Shape,
    ) -> PyResult<CoordinateMeasure<'py>> {
        shape_measure(py, shape, &mut self.collections)
    }

    pub(super) fn storage_range(
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

fn sequence_measure<'py>(seq: &CoordSeq) -> CoordinateMeasure<'py> {
    CoordinateMeasure::from_count_and_axes(
        u128::try_from(seq.len()).expect("coordinate count fits u128"),
        seq.axes(),
    )
}

fn polygon_coordinate_count(polygon: &crate::geometry::Polygon) -> u128 {
    // A Vec-bounded ring list and i32 coordinate windows fit in u128,
    // including the widest four-ordinate payload.
    polygon
        .rings()
        .map(|ring| ring.coords().len() as u128)
        .sum()
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
            let count = lines.iter().map(|line| line.len() as u128).sum();
            Ok(CoordinateMeasure::from_count_and_axes(count, lines.axes()))
        },
        Shape::Polygon(polygon) => Ok(CoordinateMeasure::from_count_and_axes(
            polygon_coordinate_count(polygon),
            polygon.axes(),
        )),
        Shape::MultiPolygon(polygons) => {
            let count = polygons.iter().try_fold(0_u128, |count, polygon| {
                count.checked_add(polygon_coordinate_count(polygon))
            });
            if let Some(count) = count
                && count
                    .checked_mul(polygons.axes().byte_width(1) as u128)
                    .is_some()
            {
                return Ok(CoordinateMeasure::from_count_and_axes(
                    count,
                    polygons.axes(),
                ));
            }
            let mut measure = CoordinateMeasure::ZERO;
            for polygon in polygons.iter() {
                let polygon = CoordinateMeasure::from_count_and_axes(
                    polygon_coordinate_count(polygon),
                    polygon.axes(),
                );
                measure = measure.add(py, &polygon)?;
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

impl CoordinateView {
    pub(crate) fn exact_measure<'py>(&self, py: Python<'py>) -> PyResult<CoordinateMeasure<'py>> {
        match &self.owner {
            CoordinateOwner::Shape(shape) => measure_shape(py, shape.shape()),
            CoordinateOwner::Array(rows) => measure_storage(py, rows.storage(), rows.missing()),
        }
    }

    pub(crate) fn len_for_python(&self, py: Python<'_>) -> PyResult<usize> {
        self.exact_measure(py)?.len_for_python()
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
            CoordinateOwner::Array(_) => Ok(measure.present_axes()),
        }
    }
}
