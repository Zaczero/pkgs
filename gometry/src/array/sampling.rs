use pyo3::prelude::*;

use super::{ArrayRow, I64Param, PyGeometryArray, Shape, row_sample_seed};
use crate::boundary::input::int_min_error;
use crate::error::{BatchError, BatchResult, Error, ResourceError};
use crate::geometry::{ExpansionBudget, PreparedSampling, TopologyDomain};
use crate::py::vectors::Groups;

fn sample_seed(seed: &I64Param, row: usize) -> u64 {
    match seed {
        I64Param::Scalar(value) => row_sample_seed(*value as u64, row),
        I64Param::PerElement(_) => row_sample_seed(seed.get(row) as u64, 0),
    }
}

impl PyGeometryArray {
    pub(crate) fn sample_points_impl(
        &self,
        count: &Bound<'_, PyAny>,
        seed: &Bound<'_, PyAny>,
    ) -> PyResult<Groups> {
        let py = count.py();
        let count = I64Param::parse(count, "count", self.len())?;
        let negative_count = match &count {
            I64Param::Scalar(value) => (*value < 0).then_some(*value),
            I64Param::PerElement(values) => {
                py.detach(|| values.iter().copied().find(|value| *value < 0))
            },
        };
        if let Some(value) = negative_count {
            return Err(int_min_error("sample_points", "count", 0, value));
        }
        let seed = I64Param::parse(seed, "seed", self.len())?;
        Ok(py.detach(|| self.sample_point_groups(&count, &seed))?)
    }

    fn sample_point_groups(&self, count: &I64Param, seed: &I64Param) -> BatchResult<Groups> {
        let mut total = 0_usize;
        for row in 0..self.len() {
            if !self.is_row_missing(row) {
                let count = usize::try_from(count.get(row)).expect("validated non-negative");
                ExpansionBudget::check("sample_points", "count", count)?;
                total = total
                    .checked_add(count)
                    .ok_or_else(|| Error::from(ResourceError::SizeOverflow))?;
            }
        }
        let domain = if total == 0 {
            TopologyDomain::Cartesian
        } else {
            crate::geometry::topology_domain(&self.frame)?
        };

        if domain.is_geographic() {
            let mut samples = Vec::with_capacity(self.len());
            for row in self.row_operation().rows_with_missing(|| ()) {
                match row {
                    ArrayRow::Missing { .. } => samples.push(None),
                    ArrayRow::Present(row) => {
                        let logical = row.logical();
                        let count =
                            usize::try_from(count.get(logical)).expect("validated non-negative");
                        let seed = sample_seed(seed, logical);
                        samples.push(Some(row.row().try_with_shape(|shape| {
                            shape.sample_point_receipts_in_domain(count, seed, logical, domain)
                        })?));
                    },
                }
            }
            return Ok(crate::geometry::publish_sampling_groups(
                samples,
                self.frame.clone(),
                self.missing().cloned(),
            )?);
        }

        let mut builder = crate::CsrBuilder::with_capacity(total, self.len());
        for row in self.row_operation().rows_with_missing(|| ()) {
            let (row, points) = match row {
                ArrayRow::Missing { logical, .. } => (logical, None),
                ArrayRow::Present(row) => {
                    let logical = row.logical();
                    let count =
                        usize::try_from(count.get(logical)).expect("validated non-negative");
                    let seed = sample_seed(seed, logical);
                    let points = row.row().try_with_shape(|shape| {
                        shape
                            .sample_point_receipts_in_domain(count, seed, logical, domain)
                            .map(PreparedSampling::into_points)
                    })?;
                    (logical, Some(points))
                },
            };
            builder
                .push_row(points.into_iter().flatten().map(Shape::Point))
                .map_err(|error| BatchError::Row { row, error })?;
        }
        Ok(Groups::from_geometry_builder(
            self.frame.clone(),
            builder,
            self.missing().cloned(),
        )?)
    }
}
