#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::borrow::Cow;

use crate::array::{
    Arc, ArrayRow, PointRows, Py, PyAny, PyGeometryArray, PyResult, Python, Shape, ValidityMask,
    coordinates,
};

impl PyGeometryArray {
    /// Packed point rows together with their logical missingness.
    ///
    /// Keeping the raw packed-row access in this mask-aware boundary prevents
    /// point fast paths from accidentally dropping the companion mask.
    pub(crate) fn masked_point_rows(&self) -> Option<(PointRows<'_>, Option<&ValidityMask>)> {
        if self.is_compact_nullable() {
            return None;
        }
        self.storage()
            .point_rows()
            .map(|points| (points, self.missing()))
    }

    /// Iterate storage rows that are present in the logical array.
    ///
    /// Dense arrays reuse the raw storage iterator; masked arrays skip the
    /// placeholder rows and keep the original logical row index for diagnostics
    /// and row-seeded kernels.
    pub(crate) fn present_shape_rows(
        &self,
    ) -> impl Iterator<Item = crate::error::Result<(usize, Cow<'_, Shape>)>> + '_ {
        self.row_operation().present_rows().map(|row| {
            row.row()
                .with_shape(Clone::clone)
                .map(|shape| (row.logical(), Cow::Owned(shape)))
        })
    }

    /// Fixed-width 3D bounds facts: missing rows occupy an ndarray row and
    /// therefore become the same ``NaN`` row as an empty/no-Z geometry.
    pub(crate) fn bounds_3d_rows(
        &self,
    ) -> impl Iterator<Item = crate::error::Result<Option<crate::geometry::Bounds3D>>> + '_ {
        self.row_operation()
            .rows_with_missing(|| ())
            .map(|row| match row {
                ArrayRow::Missing { .. } => Ok(None),
                ArrayRow::Present(row) => row.row().with_shape(Shape::bounds_3d),
            })
    }

    /// Present row shapes as owned values for n-ary kernels that stage a
    /// coverage or noding universe.
    pub(crate) fn present_shapes(&self) -> crate::error::Result<Vec<Shape>> {
        self.present_shape_rows()
            .map(|result| result.map(|(_, shape)| shape.into_owned()))
            .collect()
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
            return rows
                .into_iter()
                .map(|(_, value)| value)
                .collect::<Vec<_>>()
                .into_py_any(py);
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
