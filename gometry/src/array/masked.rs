use crate::array::{
    Arc, Py, PyAny, PyGeometryArray, PyResult, Python, Shape, ValidityMask, coordinates,
};

impl PyGeometryArray {
    /// Publish logical-row outputs as a Python list with ``None`` at missing rows.
    pub(crate) fn masked_row_list<T>(&self, py: Python<'_>, rows: Vec<T>) -> PyResult<Py<PyAny>>
    where
        T: for<'py> pyo3::IntoPyObjectExt<'py>,
    {
        let output = match self.missing() {
            None => pyo3::types::PyList::new(py, rows)?,
            Some(mask) => pyo3::types::PyList::new(
                py,
                rows.into_iter()
                    .enumerate()
                    .map(|(row, value)| (!mask.is_missing(row)).then_some(value)),
            )?,
        };
        Ok(output.into_any().unbind())
    }

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
        let Some(mask) = self.missing() else {
            return Ok(
                pyo3::types::PyList::new(py, rows.into_iter().map(|(_, value)| value))?
                    .into_any()
                    .unbind(),
            );
        };
        let mut present = rows.into_iter();
        let output = pyo3::types::PyList::new(
            py,
            mask.iter().enumerate().map(|(row, missing)| {
                if missing {
                    None
                } else {
                    let (present_row, value) = present
                        .next()
                        .unwrap_or_else(|| unreachable!("present rows match mask"));
                    debug_assert_eq!(present_row, row);
                    Some(value)
                }
            }),
        )?;
        let exhausted = present.next().is_none();
        debug_assert!(exhausted);
        Ok(output.into_any().unbind())
    }
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;

    use super::*;
    use crate::crs::frame::Frame;
    use crate::geometry::Point;

    #[test]
    fn nullable_row_lists_publish_without_native_staging() {
        Python::initialize();
        let array = PyGeometryArray::from_shapes(
            vec![Shape::Point(Point::new_raw_xy(0.0, 0.0)); 3],
            Frame::None,
        )
        .unwrap()
        .with_missing_mask(ValidityMask::from_sparse(3, &[1]))
        .unwrap();
        Python::attach(|py| {
            let rows = vec!["first", "hidden", "last"];
            let present = vec![(0, "first"), (2, "last")];
            let (all_output, all_allocations) =
                crate::test_support::allocation_count(|| array.masked_row_list(py, rows));
            let (present_output, present_allocations) =
                crate::test_support::allocation_count(|| {
                    array.masked_present_row_list(py, present)
                });
            let expected = vec![Some("first".to_owned()), None, Some("last".to_owned())];
            for output in [all_output, present_output] {
                assert_eq!(
                    output
                        .unwrap()
                        .bind(py)
                        .extract::<Vec<Option<String>>>()
                        .unwrap(),
                    expected
                );
            }
            assert_eq!((all_allocations, present_allocations), (0, 0));
        });
    }

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
