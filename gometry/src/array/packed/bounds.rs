use super::{
    Arc, Bounds, ElementBounds, GeometryArrayStorage, PackedColumns, PackedStorage, Py, PyAny,
    PyGeometryArray, PyResult, Python, Shape, float64_array, row_ord_extremes,
};

impl PyGeometryArray {
    /// Shared driver for the Z-extreme accessors (`min_z`/`max_z`):
    /// reads packed per-row extremes when storage exposes them, else falls back
    /// to per-shape computation. See [`Self::extreme_lane`].
    pub(crate) fn z_extreme_lane(
        &self,
        py: Python<'_>,
        project: impl Fn(f64, f64) -> f64 + pyo3::marker::Ungil,
        from_shape: impl Fn(&Shape) -> Option<f64> + pyo3::marker::Ungil,
    ) -> PyResult<Py<PyAny>> {
        self.extreme_lane(
            py,
            |columns| match columns {
                PackedColumns::Points(point_columns) => point_columns.coords().zs(),
                PackedColumns::Lines(line_columns) => line_columns.coords().zs(),
                PackedColumns::Polygons(polygon_columns) => polygon_columns.coords().zs(),
            },
            project,
            from_shape,
        )
    }

    /// Shared driver for the M-extreme accessors (`min_m`/`max_m`):
    /// reads packed per-row extremes when storage exposes them, else falls back
    /// to per-shape computation. See [`Self::extreme_lane`].
    pub(crate) fn m_extreme_lane(
        &self,
        py: Python<'_>,
        project: impl Fn(f64, f64) -> f64 + pyo3::marker::Ungil,
        from_shape: impl Fn(&Shape) -> Option<f64> + pyo3::marker::Ungil,
    ) -> PyResult<Py<PyAny>> {
        self.extreme_lane(
            py,
            |columns| match columns {
                PackedColumns::Points(point_columns) => point_columns.coords().ms(),
                PackedColumns::Lines(line_columns) => line_columns.coords().ms(),
                PackedColumns::Polygons(polygon_columns) => polygon_columns.coords().ms(),
            },
            project,
            from_shape,
        )
    }

    /// Shared per-row ordinate-extreme reduction behind the Z and M accessors.
    /// When storage is packed, `ordinate` selects the contiguous
    /// ordinate column and `row_ord_extremes` reduces each row's window; `project`
    /// maps a row's `(low, high)` pair to the emitted value. Otherwise
    /// `from_shape` recomputes per shape. Absent ordinates publish as `nan`.
    fn extreme_lane(
        &self,
        py: Python<'_>,
        ordinate: impl for<'a> Fn(&'a PackedColumns<'_>) -> Option<&'a [f64]> + pyo3::marker::Ungil,
        project: impl Fn(f64, f64) -> f64 + pyo3::marker::Ungil,
        from_shape: impl Fn(&Shape) -> Option<f64> + pyo3::marker::Ungil,
    ) -> PyResult<Py<PyAny>> {
        let values = py.detach(|| {
            if !self.is_compact_nullable()
                && let Some(columns) = self.packed_columns()
            {
                let Some(column) = ordinate(&columns) else {
                    return Ok(vec![f64::NAN; self.len()]);
                };
                return Ok(columns.collect_rows::<_, Vec<_>>(|row| {
                    row.and_then(|row| row.vertex_window())
                        .and_then(|window| row_ord_extremes(column, window))
                        .map_or(f64::NAN, |(low, high)| project(low, high))
                }));
            }
            self.row_operation()
                .rows_with_missing(|| f64::NAN)
                .map(|row| match row {
                    crate::array::ArrayRow::Missing { value, .. } => Ok(value),
                    crate::array::ArrayRow::Present(row) => row
                        .row()
                        .with_shape(&from_shape)
                        .map(|value| value.unwrap_or(f64::NAN)),
                })
                .collect::<crate::error::Result<Vec<_>>>()
        })?;
        float64_array(py, values)
    }

    /// Return the published per-element bounds without initializing the cache.
    /// Cold callers must fall back to their row-local bounds computation.
    pub(crate) fn existing_element_bounds(&self) -> Option<ElementBounds> {
        self.bounds_cache.get().cloned()
    }

    /// Build and publish per-element bounds for packed storage.
    pub(crate) fn element_bounds(&self) -> Option<ElementBounds> {
        if matches!(self.storage(), GeometryArrayStorage::Mixed(_)) {
            return None;
        }
        Some(Arc::clone(self.bounds_cache.get_or_init(|| {
            let mut values = Vec::with_capacity(self.len());
            let Ok(_) = self.try_for_each_element_bounds(|bounds| {
                values.push(bounds);
                Ok::<_, std::convert::Infallible>(())
            });
            Arc::new(values)
        })))
    }

    /// Visit each logical row's planar bounds in row order, straight off the
    /// packed columns: `None` for a missing or empty row. Mixed storage has
    /// no columnar fold; it visits nothing and returns `false`.
    pub(crate) fn try_for_each_element_bounds<E>(
        &self,
        mut visit: impl FnMut(Option<Bounds>) -> std::result::Result<(), E>,
    ) -> std::result::Result<bool, E> {
        let storage = self.storage();
        let Some(mask) = self.missing() else {
            return storage.try_for_each_element_bounds(visit);
        };
        let mut logical = 0;
        if !matches!(
            storage,
            GeometryArrayStorage::Packed(PackedStorage::Points { .. })
        ) {
            // CSR keeps a window per logical row; Mixed has no columnar fold.
            return storage.try_for_each_element_bounds(|bounds| {
                let missing = mask.is_missing(logical);
                logical = logical.wrapping_add(1);
                visit(bounds.filter(|_| !missing))
            });
        }
        // Present-only payload: the mask places each stored row.
        let columnar = storage.try_for_each_element_bounds(|bounds| {
            while mask.is_missing(logical) {
                visit(None)?;
                logical = logical.wrapping_add(1);
            }
            logical = logical.wrapping_add(1);
            visit(bounds)
        })?;
        if columnar {
            for _ in logical..mask.len() {
                visit(None)?;
            }
        }
        Ok(columnar)
    }
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;

    use super::*;
    use crate::Frame;
    use crate::geometry::storage::ValidityMask;

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn selected_ordinate_extents_keep_missing_rows_and_duplicate_order() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let first = crate::io::parse_wkt("LINESTRING ZM (1 2 3 -7, 4 5 5 9)").unwrap();
            let last = crate::io::parse_wkt("LINESTRING ZM (6 7 -2 4, 8 9 10 8)").unwrap();
            for hidden in [first.clone(), Shape::empty_point()] {
                let array = PyGeometryArray::from_shapes(
                    vec![first.clone(), hidden, last.clone()],
                    Frame::None,
                )
                .unwrap()
                .with_missing_mask(ValidityMask::from_sparse(3, &[1]))
                .unwrap()
                .gather_logical_rows(&[2, 1, 0, 2])
                .unwrap();
                for (output, expected) in [
                    (array.min_z(py), [-2.0, f64::NAN, 3.0, -2.0]),
                    (array.max_z(py), [10.0, f64::NAN, 5.0, 10.0]),
                    (array.min_m(py), [4.0, f64::NAN, -7.0, 4.0]),
                    (array.max_m(py), [8.0, f64::NAN, 9.0, 8.0]),
                ] {
                    let actual = output
                        .unwrap()
                        .bind(py)
                        .call_method0("tolist")
                        .unwrap()
                        .extract::<Vec<f64>>()
                        .unwrap();
                    assert_eq!(actual.len(), expected.len());
                    for (actual, expected) in actual.into_iter().zip(expected) {
                        if expected.is_nan() {
                            assert!(actual.is_nan());
                        } else {
                            assert_eq!(actual, expected);
                        }
                    }
                }
            }
        });
    }
}
