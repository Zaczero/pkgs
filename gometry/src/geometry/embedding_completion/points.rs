use std::cmp::Ordering;

#[cfg(test)]
use crate::error::ResourceError;
use crate::error::Result;
use crate::geometry::graph::{RayAxis, RayPoints, RayPointsWithWork};
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::topology::{
    ExactChartPoint, vector_cross_with_work, vector_difference_with_work,
};
use crate::geometry::{GeometryErrorKind, XY};
use crate::work::{Unmetered, Work};

/// Exact points are chart images already admitted by their producer receipt.
/// An adapter must form a lifted longitude as exact native X plus the original
/// arbitrary-precision sheet times the exact period, then apply any bounded
/// relative neighbor offset. A finite-domain `i64` offset is never the original
/// sheet, and the global reference X is never rounded through `f64`.
#[cfg_attr(test, derive(Clone))]
pub(in crate::geometry) struct ExactPoints {
    pub(in crate::geometry) exact: Vec<ExactChartPoint>,
    pub(in crate::geometry) rounded: Vec<XY>,
}

impl ExactPoints {
    #[cfg(test)]
    pub(in crate::geometry) fn from_exact(
        exact: Vec<ExactChartPoint>,
        work: &mut impl Work,
    ) -> Result<Self> {
        work.charge_into_error(
            u64::try_from(exact.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut rounded = Vec::with_capacity(exact.len());
        for point in &exact {
            work.charge_into_error(1)?;
            let rounded_point = XY::new(
                point.x.round_with_work(work)?,
                point.y.round_with_work(work)?,
            );
            if !rounded_point.x.is_finite() || !rounded_point.y.is_finite() {
                return Err(GeometryErrorKind::Invalid(
                    "known graph point is outside the finite coordinate domain".into(),
                )
                .into());
            }
            rounded.push(rounded_point);
        }
        Ok(Self { exact, rounded })
    }
}

impl RayPoints for ExactPoints {
    fn rounded(&self, index: u32) -> XY {
        self.rounded
            .get(index as usize)
            .copied()
            .expect("graph point IDs belong to the admitted point table")
    }

    fn compare_axis(&self, left: u32, right: u32, axis: RayAxis) -> Result<Ordering> {
        self.compare_axis_with_work(left, right, axis, &mut Unmetered)
    }

    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign> {
        self.orientation_with_work(a, b, c, &mut Unmetered)
    }
}

impl RayPointsWithWork for ExactPoints {
    fn compare_axis_with_work<W: Work>(
        &self,
        left: u32,
        right: u32,
        axis: RayAxis,
        work: &mut W,
    ) -> Result<Ordering> {
        let left = self
            .exact
            .get(left as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("known graph point ID is unknown".into()))?;
        let right = self
            .exact
            .get(right as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("known graph point ID is unknown".into()))?;
        match axis {
            RayAxis::X => left.x.try_cmp_with_work(&right.x, work),
            RayAxis::Y => left.y.try_cmp_with_work(&right.y, work),
        }
    }

    fn orientation_with_work<W: Work>(
        &self,
        a: u32,
        b: u32,
        c: u32,
        work: &mut W,
    ) -> Result<ExactSign> {
        let a = self
            .exact
            .get(a as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("known graph point ID is unknown".into()))?;
        let b = self
            .exact
            .get(b as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("known graph point ID is unknown".into()))?;
        let c = self
            .exact
            .get(c as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("known graph point ID is unknown".into()))?;
        let direction = vector_difference_with_work(a, b, work)?;
        let offset = vector_difference_with_work(a, c, work)?;
        vector_cross_with_work(&direction, &offset, work).map(|cross| cross.sign())
    }
}
