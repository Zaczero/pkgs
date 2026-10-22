use pyo3::prelude::*;

use super::{Arc, CsrBuilder, Groups, GroupsValues, ValidityMask};

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

    pub(super) fn from_numeric_rows(rows: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut builder = CsrBuilder::<f64>::with_capacity(0, 0);
        let mut missing = Vec::new();
        for (row, item) in rows.try_iter()?.enumerate() {
            crate::check_python_signals(rows.py(), row)?;
            let item = item?;
            missing.push(item.is_none());
            if !item.is_none() {
                for value in item.try_iter()? {
                    let value = value?;
                    let value =
                        crate::f64_scalar_named("Groups value", &value)?.ok_or_else(|| {
                            pyo3::exceptions::PyTypeError::new_err(
                                "Groups rows must contain numbers",
                            )
                        })?;
                    builder.values_mut().push(value);
                }
            }
            builder.push_end(builder.len())?;
        }
        let mask = ValidityMask::from_missing_iter(missing.len(), missing);
        let (values, offsets) = builder.finish();
        Ok(Self::from_float64_csr(values, offsets)?.with_row_missing(mask))
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
