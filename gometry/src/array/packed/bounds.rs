use super::{
    Arc, Bounds, ElementBounds, GeometryArrayStorage, PackedColumns, PackedStorage, Py, PyAny,
    PyGeometryArray, PyResult, Python, Shape, optional_float64_array, row_ord_extremes,
};

impl PyGeometryArray {
    /// Shared driver for the Z-extreme accessors (`min_z`/`max_z`):
    /// reads packed per-row extremes when storage exposes them, else falls back
    /// to per-shape computation. See [`Self::extreme_lane`].
    pub(crate) fn z_extreme_lane(
        &self,
        py: Python<'_>,
        project: impl Fn(f64, f64) -> f64 + pyo3::marker::Ungil,
        from_shape: impl Fn(&Shape) -> Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        self.extreme_lane(
            py,
            self.has_z(),
            |columns| match columns {
                PackedColumns::Points(point_columns) => point_columns.coords().zs(),
                PackedColumns::Lines(line_columns) => line_columns.coords().zs(),
                PackedColumns::Polygons(polygon_columns) => polygon_columns.coords().zs(),
            },
            row_ord_extremes,
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
        from_shape: impl Fn(&Shape) -> Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        self.extreme_lane(
            py,
            self.has_m(),
            |columns| match columns {
                PackedColumns::Points(point_columns) => point_columns.coords().ms(),
                PackedColumns::Lines(line_columns) => line_columns.coords().ms(),
                PackedColumns::Polygons(polygon_columns) => polygon_columns.coords().ms(),
            },
            row_ord_extremes,
            project,
            from_shape,
        )
    }

    /// Shared per-row ordinate-extreme reduction behind the Z and M accessors.
    /// When `present` and storage is packed, `ordinate` selects the contiguous
    /// ordinate column and `row_extremes` reduces each row's window; `project`
    /// maps a row's `(low, high)` pair to the emitted value. Otherwise
    /// `from_shape` recomputes per shape. Absent ordinate stays `None` (`nan`).
    fn extreme_lane(
        &self,
        py: Python<'_>,
        present: bool,
        ordinate: impl for<'a> Fn(&'a PackedColumns<'_>) -> Option<&'a [f64]> + pyo3::marker::Ungil,
        row_extremes: impl Fn(&[f64], std::ops::Range<usize>) -> Option<(f64, f64)>
        + pyo3::marker::Ungil,
        project: impl Fn(f64, f64) -> f64 + pyo3::marker::Ungil,
        from_shape: impl Fn(&Shape) -> Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        if self.is_compact_nullable() {
            let values = self
                .row_operation()
                .rows_with_missing(|| None::<f64>)
                .map(|row| match row {
                    crate::array::ArrayRow::Missing { value, .. } => Ok(value),
                    crate::array::ArrayRow::Present(row) => row
                        .row()
                        .with_shape(&from_shape)
                        .map_err(crate::PyErr::from),
                })
                .collect::<pyo3::PyResult<Vec<_>>>()?;
            return optional_float64_array(py, values);
        }
        if present
            && let Some(values) = self.reduce_packed_columns_detached(py, move |columns| {
                let column = ordinate(&columns).expect("checked ordinate presence");
                Ok(columns.collect_rows::<_, Vec<_>>(|row| {
                    row.and_then(|row| row.vertex_window())
                        .and_then(|window| row_extremes(column, window))
                        .map(|(low, high)| project(low, high))
                }))
            })?
        {
            return optional_float64_array(py, values);
        }
        optional_float64_array(
            py,
            self.storage()
                .iter_shapes()
                .enumerate()
                .map(|(row, shape)| {
                    let shape = shape.map_err(crate::PyErr::from)?;
                    Ok((!self.is_row_missing(row))
                        .then(|| from_shape(&shape))
                        .flatten())
                })
                .collect::<pyo3::PyResult<Vec<_>>>()?,
        )
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
