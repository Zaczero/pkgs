#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::borrow::Cow;
use std::iter::Enumerate;

use crate::array::{
    Arc, MissingMask, PointRows, Py, PyAny, PyGeometryArray, PyResult, Python, RowsIter, Shape,
    ShapeRow, ShapesIter, coordinates,
};

pub(crate) enum PresentShapeRows<'a> {
    Dense(Enumerate<ShapesIter<'a>>),
    Masked {
        iter: Enumerate<ShapesIter<'a>>,
        mask: &'a [bool],
    },
}

impl<'a> Iterator for PresentShapeRows<'a> {
    type Item = (usize, Cow<'a, Shape>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense(iter) => iter.next(),
            Self::Masked { iter, mask } => iter.find(|(row, _)| !mask[*row]),
        }
    }
}

pub(crate) enum MaskedShapeRows<'a> {
    Dense(ShapesIter<'a>),
    Masked {
        iter: Enumerate<ShapesIter<'a>>,
        mask: &'a [bool],
    },
}

impl<'a> Iterator for MaskedShapeRows<'a> {
    type Item = (bool, Cow<'a, Shape>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense(iter) => iter.next().map(|shape| (false, shape)),
            Self::Masked { iter, mask } => iter.next().map(|(row, shape)| (mask[row], shape)),
        }
    }
}

pub(crate) enum MaskedStorageRows<'a> {
    Dense(RowsIter<'a>),
    Masked {
        iter: Enumerate<RowsIter<'a>>,
        mask: &'a [bool],
    },
}

impl<'a> Iterator for MaskedStorageRows<'a> {
    type Item = (bool, ShapeRow<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense(iter) => iter.next().map(|row| (false, row)),
            Self::Masked { iter, mask } => iter.next().map(|(row, shape)| (mask[row], shape)),
        }
    }
}

impl PyGeometryArray {
    /// Packed point rows together with their logical missingness.
    ///
    /// Keeping the raw packed-row access in this mask-aware boundary prevents
    /// point fast paths from accidentally dropping the companion mask.
    pub(crate) fn masked_point_rows(&self) -> Option<(PointRows<'_>, Option<&MissingMask>)> {
        self.storage()
            .point_rows()
            .map(|points| (points, self.missing()))
    }

    /// Iterate storage rows that are present in the logical array.
    ///
    /// Dense arrays reuse the raw storage iterator; masked arrays skip the
    /// placeholder rows and keep the original logical row index for diagnostics
    /// and row-seeded kernels.
    pub(crate) fn present_shape_rows(&self) -> PresentShapeRows<'_> {
        self.missing().map_or_else(
            || PresentShapeRows::Dense(self.storage().iter_shapes().enumerate()),
            |mask| PresentShapeRows::Masked {
                iter: self.storage().iter_shapes().enumerate(),
                mask,
            },
        )
    }

    /// Iterate every storage row with its missing flag. Python-facing preview
    /// and export code use this instead of reading storage directly.
    pub(crate) fn masked_shape_rows(&self) -> MaskedShapeRows<'_> {
        self.missing().map_or_else(
            || MaskedShapeRows::Dense(self.storage().iter_shapes()),
            |mask| MaskedShapeRows::Masked {
                iter: self.storage().iter_shapes().enumerate(),
                mask,
            },
        )
    }

    /// Iterate every storage row with its missing flag, preserving packed
    /// row-view access for callers such as the spatial index.
    pub(crate) fn masked_storage_rows(&self) -> MaskedStorageRows<'_> {
        self.missing().map_or_else(
            || MaskedStorageRows::Dense(self.storage().iter_rows()),
            |mask| MaskedStorageRows::Masked {
                iter: self.storage().iter_rows().enumerate(),
                mask,
            },
        )
    }

    /// Fixed-width 3D bounds facts: missing rows occupy an ndarray row and
    /// therefore become the same ``NaN`` row as an empty/no-Z geometry.
    pub(crate) fn bounds_3d_rows(
        &self,
    ) -> impl Iterator<Item = Option<crate::geometry::Bounds3D>> + '_ {
        self.masked_shape_rows()
            .map(|(missing, shape)| (!missing).then(|| shape.bounds_3d()).flatten())
    }

    /// Present row shapes as owned values for n-ary kernels that stage a
    /// coverage or noding universe.
    pub(crate) fn present_shapes(&self) -> Vec<Shape> {
        self.present_shape_rows()
            .map(|(_, shape)| shape.into_owned())
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
    pub(crate) fn scatter_present_result(&self, present: Self) -> Self {
        match self.missing() {
            None => present,
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
            .map(|(row, &missing)| {
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
