use crate::array::{
    Arc, Py, PyAny, PyGeometryArray, PyResult, Python, Shape, ValidityMask, coordinates,
};

impl PyGeometryArray {
    /// Iterate storage rows that are present in the logical array.
    ///
    /// Dense arrays reuse the raw storage iterator; masked arrays skip the
    /// placeholder rows and keep the original logical row index for diagnostics
    /// and row-seeded kernels.
    pub(crate) fn present_shape_rows(
        &self,
    ) -> impl Iterator<Item = crate::error::Result<(usize, Shape)>> + '_ {
        self.row_operation().present_rows().map(|row| {
            row.row()
                .with_shape(Clone::clone)
                .map(|shape| (row.logical(), shape))
        })
    }

    /// Present row shapes as owned values for n-ary kernels that stage a
    /// coverage or noding universe.
    pub(crate) fn present_shapes(&self) -> crate::error::Result<Vec<Shape>> {
        let mut shapes = Vec::with_capacity(
            self.missing()
                .map_or(self.len(), ValidityMask::present_count),
        );
        for row in self.present_shape_rows() {
            shapes.push(row?.1);
        }
        Ok(shapes)
    }

    /// Coordinate view that skips missing rows while preserving source row ids.
    pub(crate) fn coordinate_view(&self) -> coordinates::CoordinateView {
        coordinates::CoordinateView::from_array_masked(
            Arc::clone(self.storage_arc()),
            self.missing().cloned(),
        )
    }

    /// Reattach this array's missing mask to a row-aligned geometry result that
    /// was computed from only the present rows.
    pub(crate) fn scatter_present_result(&self, present: Self) -> crate::error::Result<Self> {
        match self.missing() {
            None => Ok(present),
            Some(mask) if present.missing() == Some(mask) => Ok(present),
            Some(_) if present.missing().is_some() => {
                Err(crate::geometry::GeometryErrorKind::message(
                    "already-masked result does not match the source missing mask",
                ))
            },
            Some(mask) => Self::scatter_present_rows(&present, mask.clone()),
        }
    }

    /// Build a Python list from present-row outputs, filling missing rows with
    /// ``None``. Dense arrays keep the straight conversion path.
    pub(crate) fn masked_present_row_list<T>(
        &self,
        py: Python<'_>,
        rows: Vec<(usize, T)>,
    ) -> PyResult<Py<PyAny>>
    where
        T: for<'py> pyo3::IntoPyObjectExt<'py>,
    {
        use pyo3::IntoPyObjectExt as _;
        let Some(mask) = self.missing() else {
            return Ok(
                pyo3::types::PyList::new(py, rows.into_iter().map(|(_, value)| value))?
                    .into_any()
                    .unbind(),
            );
        };
        let mut present = rows.into_iter();
        let out: Vec<Py<PyAny>> = mask
            .iter()
            .enumerate()
            .map(|(row, missing)| {
                if missing {
                    Ok(py.None())
                } else {
                    let (present_row, value) = present
                        .next()
                        .unwrap_or_else(|| unreachable!("present rows match mask"));
                    debug_assert_eq!(present_row, row);
                    value.into_py_any(py)
                }
            })
            .collect::<PyResult<_>>()?;
        let exhausted = present.next().is_none();
        debug_assert!(exhausted);
        out.into_py_any(py)
    }
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;

    use super::*;
    use crate::boundary::Frame;
    use crate::geometry::Point;

    #[test]
    fn dense_row_list_publishes_without_native_staging() {
        Python::initialize();
        let array = PyGeometryArray::from_shapes(
            vec![
                Shape::Point(Point::new_raw_xy(0.0, 0.0)),
                Shape::Point(Point::new_raw_xy(1.0, 1.0)),
            ],
            Frame::None,
        )
        .unwrap();
        Python::attach(|py| {
            let rows = vec![(0, "first".to_owned()), (1, "second".to_owned())];
            let output = crate::test_support::assert_no_allocations(|| {
                array.masked_present_row_list(py, rows)
            })
            .unwrap();
            assert_eq!(output.bind(py).extract::<Vec<String>>().unwrap(), [
                "first", "second"
            ],);
        });
    }
}
