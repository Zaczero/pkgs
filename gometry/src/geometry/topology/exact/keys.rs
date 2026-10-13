use std::cmp::Ordering;

use super::{ChartPort, ChartPortId, ExactChartPiece, ExactChartPoint};
use crate::error::Result;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{Bounds, GeometryErrorKind, Segment, XY};
#[cfg(test)]
use crate::work::Unmetered;
use crate::work::Work;

impl ExactChartPiece {
    pub(in crate::geometry::topology::exact) fn outward_proxy(&self) -> Result<Option<Segment>> {
        let [start_min_x, start_min_y, start_max_x, start_max_y] = self.start.outward_bounds()?;
        let [end_min_x, end_min_y, end_max_x, end_max_y] = self.end.outward_bounds()?;
        let bounds = [
            start_min_x.min(end_min_x),
            start_min_y.min(end_min_y),
            start_max_x.max(end_max_x),
            start_max_y.max(end_max_y),
        ];
        if !bounds.into_iter().all(f64::is_finite) {
            return Ok(None);
        }
        let [min_x, min_y, max_x, max_y] = bounds;
        Ok(Some(Segment {
            start: XY::new(min_x, min_y),
            end: XY::new(max_x, max_y),
        }))
    }

    pub(in crate::geometry::topology::exact) fn outward_bounds(&self) -> Result<Option<Bounds>> {
        let Some(proxy) = self.outward_proxy()? else {
            return Ok(None);
        };
        Ok(Some(Bounds::new_raw(
            proxy.start.x,
            proxy.start.y,
            proxy.end.x,
            proxy.end.y,
        )))
    }

    pub(in crate::geometry::topology::exact) fn shifted_x(
        &self,
        shift: &ExactRational,
    ) -> Result<Self> {
        let shift_point = |point: &ExactChartPoint| -> Result<ExactChartPoint> {
            Ok(ExactChartPoint::from_rationals(
                point.x.try_add(shift)?,
                point.y.clone(),
            ))
        };
        Ok(Self {
            id: self.id,
            source_edge: self.source_edge,
            start: shift_point(&self.start)?,
            end: shift_point(&self.end)?,
            source_range: self.source_range.clone(),
            lift: self.lift,
        })
    }
}
#[cfg(test)]
pub(in crate::geometry::topology::exact) fn find_chart_port(
    ports: &[ChartPort],
    point: &ExactChartPoint,
) -> Result<ChartPortId> {
    let mut work = Unmetered;
    find_chart_port_with_work(ports, point, &mut work)
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "binary-search bounds remain within the sorted exact chart-port slice"
)]
pub(in crate::geometry::topology::exact) fn find_chart_port_with_work<W: Work>(
    ports: &[ChartPort],
    point: &ExactChartPoint,
    work: &mut W,
) -> Result<ChartPortId> {
    let mut low = 0_usize;
    let mut high = ports.len();
    while low < high {
        work.charge_into_error(1)?;
        let middle = low + (high - low) / 2;
        match ports[middle].point.compare_lex(point)? {
            Ordering::Less => low = middle + 1,
            Ordering::Greater => high = middle,
            Ordering::Equal => return Ok(ports[middle].id),
        }
    }
    Err(GeometryErrorKind::Invalid("exact chart port was not interned".into()).into())
}
