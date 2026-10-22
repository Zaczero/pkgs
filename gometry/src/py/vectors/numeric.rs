use numpy::{
    PyArrayDescrMethods as _, PyReadonlyArray1, PyUntypedArray, PyUntypedArrayMethods as _,
};
use pyo3::prelude::*;

use super::{Arc, Groups, GroupsValues};
use crate::py::numpy::reject_masked_array;

impl Groups {
    pub(super) fn from_float64_csr(
        values: Vec<f64>,
        offsets: Vec<i64>,
    ) -> crate::error::Result<Self> {
        let count = super::ctor::validate_csr_offsets(&offsets, values.len())?;
        Ok(Self {
            values: GroupsValues::Float64(Arc::from(values)),
            offsets: Arc::from(offsets),
            rows: 0..count,
            missing: None,
        })
    }

    pub(super) fn from_numeric_values(
        values: &Bound<'_, PyAny>,
        offsets: Vec<i64>,
    ) -> PyResult<Self> {
        reject_masked_array(values)?;
        let array = values.cast::<PyUntypedArray>().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err(
                "Groups values must be an int64 or float64 ndarray, GeometryArray, or CellArray",
            )
        })?;
        let dtype = array.dtype();
        if array.ndim() != 1 || dtype.itemsize() != 8 {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "Groups numeric values must be a one-dimensional int64 or float64 ndarray",
            ));
        }
        let native_values = |name: &str| {
            if dtype.is_native_byteorder() == Some(false) {
                values.call_method1("astype", (name,))
            } else {
                Ok(values.clone())
            }
        };
        match dtype.kind() {
            b'i' => {
                let array = native_values("int64")?;
                let values = array.extract::<PyReadonlyArray1<'_, i64>>()?;
                Ok(Self::from_int64_values_csr(
                    values.as_array().iter().copied().collect(),
                    offsets,
                )?)
            },
            b'f' => {
                let array = native_values("float64")?;
                let values = array.extract::<PyReadonlyArray1<'_, f64>>()?;
                Ok(Self::from_float64_csr(
                    values.as_array().iter().copied().collect(),
                    offsets,
                )?)
            },
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Groups numeric values must have int64 or float64 dtype",
            )),
        }
    }

    pub(super) fn float64_row_slice(&self, row: usize) -> &[f64] {
        let GroupsValues::Float64(values) = &self.values else {
            unreachable!("float64 row access requires float64 backing");
        };
        values
            .get(self.row_values_window(row))
            .expect("admitted float Groups window")
    }

    pub(super) fn numeric_rows(&self) -> Vec<Option<Vec<f64>>> {
        (0..self.rows.len())
            .map(|row| (!self.row_is_missing(row)).then(|| self.float64_row_slice(row).to_vec()))
            .collect()
    }

    pub(super) fn parse_float64_row_item(item: &Bound<'_, PyAny>) -> PyResult<Option<Vec<f64>>> {
        let Some(iter) = crate::py_iter_or_none(item)? else {
            return Ok(None);
        };
        let mut values = Vec::new();
        for value in iter {
            let value = value?;
            let Some(value) = crate::f64_scalar_named("row value", &value)? else {
                return Ok(None);
            };
            values.push(value);
        }
        Ok(Some(values))
    }

    pub(crate) fn numeric_row(&self, row: usize) -> PyResult<Option<Vec<f64>>> {
        if self.row_is_missing(row) {
            return Ok(None);
        }
        match &self.values {
            GroupsValues::Float64(_) => Ok(Some(self.float64_row_slice(row).to_vec())),
            GroupsValues::Int64(_) => Ok(Some(
                self.int64_row_slice(row)
                    .iter()
                    .map(|&value| value as f64)
                    .collect(),
            )),
            GroupsValues::Geometry(_) | GroupsValues::Cells(_) => Err(
                pyo3::exceptions::PyTypeError::new_err("at requires numeric Groups"),
            ),
        }
    }

    pub(crate) fn row_len(&self) -> usize {
        self.rows.len()
    }

    pub(crate) const fn is_numeric(&self) -> bool {
        matches!(
            &self.values,
            GroupsValues::Float64(_) | GroupsValues::Int64(_)
        )
    }

    pub(crate) fn is_missing_row(&self, row: usize) -> bool {
        self.row_is_missing(row)
    }
}
