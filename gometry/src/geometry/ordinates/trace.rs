#[cfg(test)]
use std::cell::Cell;

use super::{
    NodeOrdinates, RootMetricState, SourceArcMetricAccess, SourceArcRequest, source_arc_ordinates,
};
use crate::error::{ResourceError, Result};
use crate::geographic::{GeographicFrame, RetainedChartDisplacement};
use crate::geometry::segments::try_sort_by_with_work;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{ChartMetric, CoordinateAxes, GeometryErrorKind, Point, Work};

#[cfg(test)]
thread_local! {
    static ROOT_RANGE_ENDPOINT_REQUESTS: Cell<usize> = const { Cell::new(0) };
    static ROOT_RANGE_INTERIOR_MAPPINGS: Cell<usize> = const { Cell::new(0) };
    static ROOT_REQUEST_TABLE_BUILDS: Cell<usize> = const { Cell::new(0) };
    static ROOT_REQUEST_ROW_SORTS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_root_range_mapping_counts() {
    ROOT_RANGE_ENDPOINT_REQUESTS.with(|count| count.set(0));
    ROOT_RANGE_INTERIOR_MAPPINGS.with(|count| count.set(0));
    ROOT_REQUEST_TABLE_BUILDS.with(|count| count.set(0));
    ROOT_REQUEST_ROW_SORTS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn root_range_mapping_counts() -> (usize, usize) {
    (
        ROOT_RANGE_ENDPOINT_REQUESTS.with(Cell::get),
        ROOT_RANGE_INTERIOR_MAPPINGS.with(Cell::get),
    )
}

#[cfg(test)]
pub(crate) fn root_request_table_builds() -> usize {
    ROOT_REQUEST_TABLE_BUILDS.with(Cell::get)
}

#[cfg(test)]
pub(crate) fn root_request_row_sorts() -> usize {
    ROOT_REQUEST_ROW_SORTS.with(Cell::get)
}

fn reserve_with_work<T>(
    values: &mut Vec<T>,
    additional: usize,
    work: &mut impl Work,
) -> Result<()> {
    if additional == 0 {
        return Ok(());
    }
    prepay_slots(additional, work)?;
    values.reserve(additional);
    Ok(())
}

fn prepay_slots(additional: usize, work: &mut impl Work) -> Result<()> {
    if additional > 0 {
        work.charge(u64::try_from(additional).map_err(|_| ResourceError::SizeOverflow)?)?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct RootId(u32);

impl RootId {
    pub(crate) const fn from_index(index: u32) -> Self {
        Self(index)
    }

    pub(crate) const fn index(self) -> usize {
        self.0 as usize
    }
}

pub(crate) trait RootCatalog {
    fn resolve<M: ChartMetric, W: Work>(
        &self,
        root: RootId,
        parameter: &ExactRational,
        axes: CoordinateAxes,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
        state: &mut RootMetricState,
        fraction: &mut Option<f64>,
        work: &mut W,
    ) -> Result<NodeOrdinates>;
}

pub(crate) fn resolve_source_root<M: ChartMetric>(
    frame: GeographicFrame,
    source: &[Point; 2],
    longitude: RetainedChartDisplacement,
    parameter: &ExactRational,
    axes: CoordinateAxes,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    state: &mut RootMetricState,
    fraction: &mut Option<f64>,
    work: &mut impl Work,
) -> Result<NodeOrdinates> {
    let request = SourceArcRequest::new(frame, source, axes, parameter, || Ok(longitude))?;
    source_arc_ordinates(
        &request,
        || {
            metric_access.map_or_else(
                || Err(crate::crs::CrsError::invalid(
                "geographic source-ordinate carry requires the operation's CRS metric authority",
                )),
                |access| access(),
            )
        },
        state,
        fraction,
        work,
    )
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct RootRange {
    root: RootId,
    source_start: ExactRational,
    source_end: ExactRational,
    producer_start: ExactRational,
    producer_end: ExactRational,
}

impl RootRange {
    pub(crate) fn try_clone_with_work(&self, work: &mut impl Work) -> Result<Self> {
        Ok(Self {
            root: self.root,
            source_start: self.source_start.try_clone_with_work(work)?,
            source_end: self.source_end.try_clone_with_work(work)?,
            producer_start: self.producer_start.try_clone_with_work(work)?,
            producer_end: self.producer_end.try_clone_with_work(work)?,
        })
    }

    #[cfg(test)]
    pub(crate) fn whole(root: RootId) -> Self {
        Self {
            root,
            source_start: ExactRational::zero(),
            source_end: ExactRational::one(),
            producer_start: ExactRational::zero(),
            producer_end: ExactRational::one(),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_mapped(
        root: RootId,
        source_start: ExactRational,
        source_end: ExactRational,
        producer_start: ExactRational,
        producer_end: ExactRational,
    ) -> Result<Self> {
        let mut work = crate::geometry::Unmetered;
        Self::from_mapped_with_work(
            root,
            source_start,
            source_end,
            producer_start,
            producer_end,
            &mut work,
        )
    }

    pub(crate) fn from_mapped_with_work(
        root: RootId,
        source_start: ExactRational,
        source_end: ExactRational,
        producer_start: ExactRational,
        producer_end: ExactRational,
        work: &mut impl Work,
    ) -> Result<Self> {
        for parameter in [&source_start, &source_end, &producer_start, &producer_end] {
            check_unit_parameter_with_work(parameter, work)?;
        }
        if producer_start
            .try_cmp_with_work(&producer_end, work)?
            .is_eq()
            && !source_start.try_cmp_with_work(&source_end, work)?.is_eq()
        {
            return Err(GeometryErrorKind::Invalid(
                "point producer support has distinct source endpoints".into(),
            )
            .into());
        }
        Ok(Self {
            root,
            source_start,
            source_end,
            producer_start,
            producer_end,
        })
    }

    pub(crate) fn try_eq_with_work(&self, other: &Self, work: &mut impl Work) -> Result<bool> {
        if self.root != other.root {
            return Ok(false);
        }
        Ok(self
            .source_start
            .try_eq_with_work(&other.source_start, work)?
            && self.source_end.try_eq_with_work(&other.source_end, work)?
            && self
                .producer_start
                .try_eq_with_work(&other.producer_start, work)?
            && self
                .producer_end
                .try_eq_with_work(&other.producer_end, work)?)
    }

    #[cfg(test)]
    pub(crate) fn point(
        root: RootId,
        source_parameter: ExactRational,
        producer_parameter: ExactRational,
    ) -> Result<Self> {
        let mut work = crate::geometry::Unmetered;
        Self::point_with_work(root, source_parameter, producer_parameter, &mut work)
    }

    pub(crate) fn point_with_work(
        root: RootId,
        source_parameter: ExactRational,
        producer_parameter: ExactRational,
        work: &mut impl Work,
    ) -> Result<Self> {
        check_unit_parameter_with_work(&source_parameter, work)?;
        check_unit_parameter_with_work(&producer_parameter, work)?;
        Ok(Self {
            root,
            source_start: source_parameter.try_clone_with_work(work)?,
            source_end: source_parameter,
            producer_start: producer_parameter.try_clone_with_work(work)?,
            producer_end: producer_parameter,
        })
    }

    #[cfg(test)]
    pub(crate) fn compose(
        &self,
        local_start: &ExactRational,
        local_end: &ExactRational,
    ) -> Result<Option<Self>> {
        let mut work = crate::geometry::Unmetered;
        self.compose_with_work(local_start, local_end, &mut work)
    }

    pub(crate) fn compose_with_work(
        &self,
        local_start: &ExactRational,
        local_end: &ExactRational,
        work: &mut impl Work,
    ) -> Result<Option<Self>> {
        check_unit_parameter_with_work(local_start, work)?;
        check_unit_parameter_with_work(local_end, work)?;
        if local_start.try_cmp_with_work(local_end, work)?.is_gt() {
            return Err(GeometryErrorKind::Invalid(
                "source trace child interval is reversed".into(),
            )
            .into());
        }
        if local_start.try_cmp_with_work(local_end, work)?.is_eq() {
            return Ok(None);
        }
        if self
            .producer_start
            .try_cmp_with_work(&self.producer_end, work)?
            .is_eq()
        {
            if self
                .producer_start
                .try_cmp_with_work(local_start, work)?
                .is_lt()
                || self
                    .producer_start
                    .try_cmp_with_work(local_end, work)?
                    .is_gt()
            {
                return Ok(None);
            }
            let child_width = local_end.try_subtract_with_work(local_start, work)?;
            let local_parameter = self
                .producer_start
                .try_subtract_with_work(local_start, work)?
                .try_quotient_with_work(&child_width, work)?;
            return Ok(Some(Self::point_with_work(
                self.root,
                self.source_start.try_clone_with_work(work)?,
                local_parameter,
                work,
            )?));
        }
        let producer_order = self
            .producer_start
            .try_cmp_with_work(&self.producer_end, work)?;
        let owner_low = if producer_order.is_lt() {
            &self.producer_start
        } else {
            &self.producer_end
        };
        let owner_high = if producer_order.is_lt() {
            &self.producer_end
        } else {
            &self.producer_start
        };
        let overlap_start = if owner_low.try_cmp_with_work(local_start, work)?.is_gt() {
            owner_low.try_clone_with_work(work)?
        } else {
            local_start.try_clone_with_work(work)?
        };
        let overlap_end = if owner_high.try_cmp_with_work(local_end, work)?.is_lt() {
            owner_high.try_clone_with_work(work)?
        } else {
            local_end.try_clone_with_work(work)?
        };
        if overlap_start.try_cmp_with_work(&overlap_end, work)?.is_ge() {
            return Ok(None);
        }
        let producer_width = self
            .producer_end
            .try_subtract_with_work(&self.producer_start, work)?;
        let source_start =
            self.producer_to_source_with_work(&overlap_start, &producer_width, work)?;
        let source_end = self.producer_to_source_with_work(&overlap_end, &producer_width, work)?;
        let child_width = local_end.try_subtract_with_work(local_start, work)?;
        Ok(Some(Self {
            root: self.root,
            source_start,
            source_end,
            producer_start: overlap_start
                .try_subtract_with_work(local_start, work)?
                .try_quotient_with_work(&child_width, work)?,
            producer_end: overlap_end
                .try_subtract_with_work(local_start, work)?
                .try_quotient_with_work(&child_width, work)?,
        }))
    }

    fn producer_to_source_with_work(
        &self,
        producer_parameter: &ExactRational,
        producer_width: &ExactRational,
        work: &mut impl Work,
    ) -> Result<ExactRational> {
        let local = producer_parameter
            .try_subtract_with_work(&self.producer_start, work)?
            .try_quotient_with_work(producer_width, work)?;
        self.source_start.try_add_with_work(
            &self
                .source_end
                .try_subtract_with_work(&self.source_start, work)?
                .try_product_with_work(&local, work)?,
            work,
        )
    }

    pub(crate) fn parameter_at_with_work(
        &self,
        producer_parameter: &ExactRational,
        work: &mut impl Work,
    ) -> Result<Option<ExactRational>> {
        check_unit_parameter_with_work(producer_parameter, work)?;
        if producer_parameter.is_zero() {
            if self.producer_start.is_zero() {
                #[cfg(test)]
                ROOT_RANGE_ENDPOINT_REQUESTS.with(|count| {
                    count.set(count.get().saturating_add(1));
                });
                return Ok(Some(self.source_start.try_clone_with_work(work)?));
            }
            if self.producer_end.is_zero() {
                #[cfg(test)]
                ROOT_RANGE_ENDPOINT_REQUESTS.with(|count| {
                    count.set(count.get().saturating_add(1));
                });
                return Ok(Some(self.source_end.try_clone_with_work(work)?));
            }
            return Ok(None);
        }
        if producer_parameter.is_one() {
            if self.producer_start.is_one() {
                #[cfg(test)]
                ROOT_RANGE_ENDPOINT_REQUESTS.with(|count| {
                    count.set(count.get().saturating_add(1));
                });
                return Ok(Some(self.source_start.try_clone_with_work(work)?));
            }
            if self.producer_end.is_one() {
                #[cfg(test)]
                ROOT_RANGE_ENDPOINT_REQUESTS.with(|count| {
                    count.set(count.get().saturating_add(1));
                });
                return Ok(Some(self.source_end.try_clone_with_work(work)?));
            }
            return Ok(None);
        }
        if self
            .producer_start
            .try_cmp_with_work(&self.producer_end, work)?
            .is_eq()
        {
            if self
                .producer_start
                .try_cmp_with_work(producer_parameter, work)?
                .is_eq()
            {
                return Ok(Some(self.source_start.try_clone_with_work(work)?));
            }
            return Ok(None);
        }
        let producer_order = self
            .producer_start
            .try_cmp_with_work(&self.producer_end, work)?;
        let low = if producer_order.is_le() {
            &self.producer_start
        } else {
            &self.producer_end
        };
        let high = if producer_order.is_le() {
            &self.producer_end
        } else {
            &self.producer_start
        };
        if producer_parameter.try_cmp_with_work(low, work)?.is_lt()
            || producer_parameter.try_cmp_with_work(high, work)?.is_gt()
        {
            return Ok(None);
        }
        #[cfg(test)]
        ROOT_RANGE_INTERIOR_MAPPINGS.with(|count| {
            count.set(count.get().saturating_add(1));
        });
        let producer_width = self
            .producer_end
            .try_subtract_with_work(&self.producer_start, work)?;
        Ok(Some(self.producer_to_source_with_work(
            producer_parameter,
            &producer_width,
            work,
        )?))
    }

    #[cfg(test)]
    pub(crate) fn request_at(
        &self,
        producer_parameter: &ExactRational,
    ) -> Result<Option<RootRequest>> {
        let mut work = crate::geometry::Unmetered;
        self.request_at_with_work(producer_parameter, &mut work)
    }

    pub(crate) fn request_at_with_work(
        &self,
        producer_parameter: &ExactRational,
        work: &mut impl Work,
    ) -> Result<Option<RootRequest>> {
        let Some(parameter) = self.parameter_at_with_work(producer_parameter, work)? else {
            return Ok(None);
        };
        Ok(Some(RootRequest {
            root: self.root,
            parameter,
        }))
    }

    pub(crate) const fn root(&self) -> RootId {
        self.root
    }

    pub(crate) const fn source_start(&self) -> &ExactRational {
        &self.source_start
    }

    pub(crate) const fn source_end(&self) -> &ExactRational {
        &self.source_end
    }

    pub(crate) const fn producer_start(&self) -> &ExactRational {
        &self.producer_start
    }

    pub(crate) const fn producer_end(&self) -> &ExactRational {
        &self.producer_end
    }

    pub(crate) fn is_point_support_with_work(&self, work: &mut impl Work) -> Result<bool> {
        Ok(self
            .source_start
            .try_cmp_with_work(&self.source_end, work)?
            .is_eq()
            && self
                .producer_start
                .try_cmp_with_work(&self.producer_end, work)?
                .is_eq())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RootRequest {
    root: RootId,
    parameter: ExactRational,
}

impl RootRequest {
    pub(crate) fn new_with_work(
        root: RootId,
        parameter: ExactRational,
        work: &mut impl Work,
    ) -> Result<Self> {
        check_unit_parameter_with_work(&parameter, work)?;
        Ok(Self { root, parameter })
    }

    fn try_clone_with_work(&self, work: &mut impl Work) -> Result<Self> {
        Ok(Self {
            root: self.root,
            parameter: self.parameter.try_clone_with_work(work)?,
        })
    }

    pub(crate) const fn root(&self) -> RootId {
        self.root
    }

    pub(crate) const fn parameter(&self) -> &ExactRational {
        &self.parameter
    }
}

pub(crate) struct RequestRowInput {
    pub(crate) axes: CoordinateAxes,
    pub(crate) requests: Vec<RootRequest>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct RequestRowId(u32);

impl RequestRowId {
    pub(crate) fn from_index(index: usize) -> Result<Self> {
        u32::try_from(index)
            .map(Self)
            .map_err(|_| ResourceError::SizeOverflow.into())
    }

    pub(crate) const fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct UniqueRequestId(u32);

#[derive(Debug)]
pub(crate) struct UniqueRootRequest {
    root: RootId,
    parameter: ExactRational,
    root_state_slot: usize,
    fraction: Option<f64>,
}

pub(crate) struct RootRequestTable {
    records: Vec<UniqueRootRequest>,
    row_offsets: Vec<u32>,
    row_requests: Vec<UniqueRequestId>,
    row_axes: Vec<CoordinateAxes>,
}

fn push_unique_root_request(
    records: &mut Vec<UniqueRootRequest>,
    request: &RootRequest,
    work: &mut impl Work,
) -> Result<UniqueRequestId> {
    let root_state_slot = match records.last() {
        Some(previous_record) if previous_record.root == request.root => {
            previous_record.root_state_slot
        },
        Some(previous_record) => previous_record
            .root_state_slot
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?,
        None => 0,
    };
    let request_id =
        UniqueRequestId(u32::try_from(records.len()).map_err(|_| ResourceError::SizeOverflow)?);
    reserve_with_work(records, 1, work)?;
    records.push(UniqueRootRequest {
        root: request.root,
        parameter: request.parameter.try_clone_with_work(work)?,
        root_state_slot,
        fraction: None,
    });
    Ok(request_id)
}

impl RootRequestTable {
    pub(crate) fn from_rows(rows: &[RequestRowInput], work: &mut impl Work) -> Result<Self> {
        #[cfg(test)]
        ROOT_REQUEST_TABLE_BUILDS.with(|count| count.set(count.get().saturating_add(1)));
        work.charge(u64::try_from(rows.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut request_count = 0_usize;
        for row in rows {
            work.charge(1)?;
            request_count = request_count
                .checked_add(row.requests.len())
                .ok_or(ResourceError::SizeOverflow)?;
        }
        let mut request_views = Vec::new();
        reserve_with_work(&mut request_views, rows.len(), work)?;
        request_views.extend(rows.iter().map(|row| row.requests.as_slice()));
        let row_offset_capacity = rows
            .len()
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut row_offsets = Vec::new();
        reserve_with_work(&mut row_offsets, row_offset_capacity, work)?;
        row_offsets.push(0);
        let mut ordered = Vec::new();
        reserve_with_work(&mut ordered, request_count, work)?;
        for (row_index, row) in request_views.iter().enumerate() {
            for index in 0..row.len() {
                work.charge(1)?;
                ordered.push(RequestIndex {
                    row: row_index,
                    index,
                });
            }
            let offset = u32::try_from(ordered.len()).map_err(|_| ResourceError::SizeOverflow)?;
            row_offsets.push(offset);
        }
        try_sort_by_with_work(&mut ordered, work, |left, right, work| {
            let left = request_at(&request_views, *left)?;
            let right = request_at(&request_views, *right)?;
            let root_order = left.root.cmp(&right.root);
            if root_order.is_eq() {
                work.charge(1)?;
                left.parameter.try_cmp_with_work(&right.parameter, work)
            } else {
                Ok(root_order)
            }
        })?;

        let mut records: Vec<UniqueRootRequest> = Vec::new();
        let mut row_requests = Vec::new();
        reserve_with_work(&mut row_requests, request_count, work)?;
        row_requests.resize(request_count, None);
        let mut previous: Option<UniqueRequestId> = None;
        for occurrence in ordered {
            work.charge(1)?;
            let request = request_at(&request_views, occurrence)?;
            let duplicate = if let Some(previous_id) = previous {
                let previous_record = records.get(previous_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("unique source request index is unknown".into())
                })?;
                work.charge(1)?;
                previous_record.root == request.root
                    && previous_record
                        .parameter
                        .try_cmp_with_work(&request.parameter, work)?
                        .is_eq()
            } else {
                false
            };
            let request_id = if duplicate {
                previous.expect("a duplicate has a previous unique record")
            } else {
                let request_id = push_unique_root_request(&mut records, request, work)?;
                previous = Some(request_id);
                request_id
            };
            let row_offset = *row_offsets.get(occurrence.row).ok_or_else(|| {
                GeometryErrorKind::Invalid("source request row offset is unknown".into())
            })? as usize;
            let flattened_index = row_offset
                .checked_add(occurrence.index)
                .ok_or(ResourceError::SizeOverflow)?;
            *row_requests.get_mut(flattened_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("source request occurrence index is unknown".into())
            })? = Some(request_id);
        }
        let mut indexed_requests = Vec::new();
        reserve_with_work(&mut indexed_requests, request_count, work)?;
        for request in row_requests {
            indexed_requests.push(request.ok_or_else(|| {
                GeometryErrorKind::Invalid("source request occurrence was not indexed".into())
            })?);
        }
        let mut row_axes = Vec::new();
        reserve_with_work(&mut row_axes, rows.len(), work)?;
        for row in rows {
            work.charge(1)?;
            row_axes.push(row.axes);
        }
        Ok(Self {
            records,
            row_offsets,
            row_requests: indexed_requests,
            row_axes,
        })
    }

    pub(crate) fn row(&self, row: RequestRowId) -> Result<&[UniqueRequestId]> {
        let start = *self
            .row_offsets
            .get(row.index())
            .ok_or_else(|| GeometryErrorKind::Invalid("source request row is unknown".into()))?;
        let end_index = row
            .index()
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let end = *self.row_offsets.get(end_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("source request row end is unknown".into())
        })?;
        self.row_requests
            .get(start as usize..end as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("source request row is incomplete".into()).into()
            })
    }

    pub(crate) fn axes(&self, row: RequestRowId) -> Result<CoordinateAxes> {
        self.row_axes.get(row.index()).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("source request row has no axis mask".into()).into()
        })
    }

    pub(crate) fn request_id_at(&self, row: RequestRowId, index: usize) -> Result<UniqueRequestId> {
        self.row(row)?.get(index).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("source request row index is unknown".into()).into()
        })
    }

    pub(crate) fn record(&self, request: UniqueRequestId) -> Result<&UniqueRootRequest> {
        self.records.get(request.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("unique source request is unknown".into()).into()
        })
    }

    pub(crate) fn record_mut(
        &mut self,
        request: UniqueRequestId,
    ) -> Result<&mut UniqueRootRequest> {
        self.records.get_mut(request.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("unique source request is unknown".into()).into()
        })
    }

    pub(crate) fn root_count(&self) -> Result<usize> {
        self.records.last().map_or(Ok(0), |record| {
            record
                .root_state_slot
                .checked_add(1)
                .ok_or_else(|| ResourceError::SizeOverflow.into())
        })
    }

    pub(crate) fn root_state_slot(&self, request: UniqueRequestId) -> Result<usize> {
        Ok(self.record(request)?.root_state_slot)
    }
}

pub(crate) fn resolve_request_rows<M: ChartMetric, C: RootCatalog>(
    rows: &[RequestRowInput],
    roots: &C,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    work: &mut impl Work,
) -> Result<Vec<NodeOrdinates>> {
    let mut table = RootRequestTable::from_rows(rows, work)?;
    let state_count = table.root_count()?;
    let mut states = Vec::new();
    reserve_with_work(&mut states, state_count, work)?;
    states.resize(state_count, RootMetricState::default());
    let mut output = Vec::new();
    reserve_with_work(&mut output, rows.len(), work)?;
    for row_index in 0..rows.len() {
        work.charge(1)?;
        let row_id = RequestRowId::from_index(row_index)?;
        let row_axes = table.axes(row_id)?;
        let request_count = table.row(row_id)?.len();
        let mut ordinates = NodeOrdinates::default();
        for request_index in 0..request_count {
            let request_id = table.request_id_at(row_id, request_index)?;
            let state_slot = table.root_state_slot(request_id)?;
            let state = states.get_mut(state_slot).ok_or_else(|| {
                GeometryErrorKind::Invalid("source root state slot is unknown".into())
            })?;
            work.charge(1)?;
            let record = table.record_mut(request_id)?;
            ordinates.merge(roots.resolve(
                record.root,
                &record.parameter,
                row_axes,
                metric_access,
                state,
                &mut record.fraction,
                work,
            )?);
        }
        work.charge(1)?;
        output.push(ordinates);
    }
    Ok(output)
}

pub(crate) struct TraceRows {
    offsets: Vec<u32>,
    sample_offsets: Vec<u32>,
    ranges: Vec<RootRange>,
}

impl TraceRows {
    pub(crate) fn try_clone_with_work(&self, work: &mut impl Work) -> Result<Self> {
        prepay_slots(self.offsets.len(), work)?;
        prepay_slots(self.sample_offsets.len(), work)?;
        prepay_slots(self.ranges.len(), work)?;
        let mut offsets = Vec::with_capacity(self.offsets.len());
        offsets.extend(self.offsets.iter().copied());
        let mut sample_offsets = Vec::with_capacity(self.sample_offsets.len());
        sample_offsets.extend(self.sample_offsets.iter().copied());
        let mut ranges = Vec::with_capacity(self.ranges.len());
        for range in &self.ranges {
            ranges.push(range.try_clone_with_work(work)?);
        }
        Ok(Self {
            offsets,
            sample_offsets,
            ranges,
        })
    }

    pub(crate) fn row(&self, edge: usize) -> Result<&[RootRange]> {
        let start = *self.offsets.get(edge).ok_or_else(|| {
            GeometryErrorKind::Invalid("source trace has no requested edge row".into())
        })?;
        let end_index = edge.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        let end = *self.offsets.get(end_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("source trace has no terminal edge row".into())
        })?;
        self.ranges
            .get(start as usize..end as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("source trace edge row is incomplete".into()).into()
            })
    }

    fn sample_row(&self, sample: usize) -> Result<&[RootRange]> {
        let start = *self.sample_offsets.get(sample).ok_or_else(|| {
            GeometryErrorKind::Invalid("source trace has no requested sample row".into())
        })?;
        let end_index = sample.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        let end = *self.sample_offsets.get(end_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("source trace has no terminal sample row".into())
        })?;
        self.ranges
            .get(start as usize..end as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("source trace sample row is incomplete".into()).into()
            })
    }

    pub(crate) const fn edge_count(&self) -> usize {
        self.offsets
            .len()
            .checked_sub(1)
            .expect("TraceRows has its initial zero offset")
    }

    #[cfg(test)]
    pub(crate) fn owner_count(&self) -> usize {
        *self
            .offsets
            .last()
            .expect("TraceRows has its initial zero offset") as usize
    }

    pub(crate) fn request_support_at(
        &self,
        edge: usize,
        local: &ExactRational,
        work: &mut impl Work,
    ) -> Result<(bool, bool)> {
        check_unit_parameter_with_work(local, work)?;
        let mut has_request = false;
        let mut all_point_support = true;
        for owner in self.row(edge)? {
            work.charge(1)?;
            if owner.parameter_at_with_work(local, work)?.is_some() {
                has_request = true;
                all_point_support &= owner.is_point_support_with_work(work)?;
            }
        }
        Ok((has_request, has_request && all_point_support))
    }

    pub(crate) fn requests_at(
        &self,
        edge: usize,
        local: &ExactRational,
        work: &mut impl Work,
    ) -> Result<Vec<RootRequest>> {
        if let [owner] = self.row(edge)? {
            return Ok(owner
                .request_at_with_work(local, work)?
                .into_iter()
                .collect());
        }
        Ok(self.requests_with_support_at(edge, local, work)?.0)
    }

    pub(crate) fn append_requests_at(
        &self,
        edge: usize,
        local: &ExactRational,
        requests: &mut Vec<RootRequest>,
        work: &mut impl Work,
    ) -> Result<usize> {
        let owners = self.row(edge)?;
        if let [owner] = owners {
            work.charge(1)?;
            if let Some(request) = owner.request_at_with_work(local, work)? {
                requests.push(request);
                return Ok(1);
            }
            return Ok(0);
        }
        let (additional, _) = unique_requests_at(owners, local, work)?;
        let count = additional.len();
        requests.extend(additional);
        Ok(count)
    }

    pub(crate) fn requests_with_support_at(
        &self,
        edge: usize,
        local: &ExactRational,
        work: &mut impl Work,
    ) -> Result<(Vec<RootRequest>, bool)> {
        check_unit_parameter_with_work(local, work)?;
        unique_requests_at(self.row(edge)?, local, work)
    }

    pub(crate) fn sample_requests_at(
        &self,
        sample: usize,
        work: &mut impl Work,
    ) -> Result<Vec<RootRequest>> {
        Ok(unique_requests_at(self.sample_row(sample)?, &ExactRational::zero(), work)?.0)
    }
}

fn unique_requests_at(
    owners: &[RootRange],
    local: &ExactRational,
    work: &mut impl Work,
) -> Result<(Vec<RootRequest>, bool)> {
    if let [owner] = owners {
        let Some(request) = owner.request_at_with_work(local, work)? else {
            return Ok((Vec::new(), false));
        };
        let endpoint_only = owner.is_point_support_with_work(work)?;
        let mut requests = Vec::new();
        reserve_with_work(&mut requests, 1, work)?;
        requests.push(request);
        return Ok((requests, endpoint_only));
    }
    check_unit_parameter_with_work(local, work)?;
    let mut requests = Vec::new();
    reserve_with_work(&mut requests, owners.len(), work)?;
    let mut has_request = false;
    let mut endpoint_only = true;
    for owner in owners {
        work.charge(1)?;
        let Some(request) = owner.request_at_with_work(local, work)? else {
            continue;
        };
        has_request = true;
        endpoint_only &= owner.is_point_support_with_work(work)?;
        requests.push(request);
    }
    let unique = unique_request_indices(&requests, work)?;
    let mut unique_requests = Vec::new();
    reserve_with_work(&mut unique_requests, unique.len(), work)?;
    for index in unique {
        work.charge(1)?;
        unique_requests.push(
            requests
                .get(index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "unique source request index is outside its row".into(),
                    )
                })?
                .try_clone_with_work(work)?,
        );
    }
    Ok((unique_requests, has_request && endpoint_only))
}

#[derive(Clone, Copy)]
struct RequestIndex {
    row: usize,
    index: usize,
}

fn request_at<'rows>(
    rows: &[&'rows [RootRequest]],
    occurrence: RequestIndex,
) -> Result<&'rows RootRequest> {
    rows.get(occurrence.row)
        .and_then(|row| row.get(occurrence.index))
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("source request index is outside its occurrence row".into())
                .into()
        })
}

fn unique_request_indices(requests: &[RootRequest], work: &mut impl Work) -> Result<Vec<usize>> {
    let mut ordered = true;
    for pair in requests.windows(2) {
        let [left, right] = pair else {
            continue;
        };
        work.charge(1)?;
        let root_order = left.root.cmp(&right.root);
        let order = if root_order.is_eq() {
            work.charge(1)?;
            left.parameter.try_cmp_with_work(&right.parameter, work)?
        } else {
            root_order
        };
        if order.is_gt() {
            ordered = false;
            break;
        }
    }

    if ordered {
        let mut unique = Vec::new();
        reserve_with_work(&mut unique, requests.len(), work)?;
        let mut previous: Option<&RootRequest> = None;
        for (index, request) in requests.iter().enumerate() {
            let duplicate = if let Some(previous) = previous {
                work.charge(1)?;
                previous.root == request.root
                    && previous
                        .parameter
                        .try_cmp_with_work(&request.parameter, work)?
                        .is_eq()
            } else {
                false
            };
            if !duplicate {
                unique.push(index);
                previous = Some(request);
            }
        }
        return Ok(unique);
    }

    #[cfg(test)]
    ROOT_REQUEST_ROW_SORTS.with(|count| count.set(count.get().saturating_add(1)));
    let mut ordered_indices = Vec::new();
    reserve_with_work(&mut ordered_indices, requests.len(), work)?;
    for index in 0..requests.len() {
        work.charge(1)?;
        ordered_indices.push(index);
    }
    try_sort_by_with_work(&mut ordered_indices, work, |&left, &right, work| {
        let left = requests.get(left).ok_or_else(|| {
            GeometryErrorKind::Invalid("source request index is outside its row".into())
        })?;
        let right = requests.get(right).ok_or_else(|| {
            GeometryErrorKind::Invalid("source request index is outside its row".into())
        })?;
        let root_order = left.root.cmp(&right.root);
        if root_order.is_eq() {
            work.charge(1)?;
            left.parameter.try_cmp_with_work(&right.parameter, work)
        } else {
            Ok(root_order)
        }
    })?;

    let mut duplicates = Vec::new();
    reserve_with_work(&mut duplicates, requests.len(), work)?;
    duplicates.resize(requests.len(), false);
    let mut unique_count = 0_usize;
    let mut previous: Option<usize> = None;
    for &index in &ordered_indices {
        work.charge(1)?;
        let current = requests.get(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("source request index is outside its row".into())
        })?;
        let duplicate = if let Some(previous) = previous {
            let previous = requests.get(previous).ok_or_else(|| {
                GeometryErrorKind::Invalid("source request index is outside its row".into())
            })?;
            work.charge(1)?;
            previous.root == current.root
                && previous
                    .parameter
                    .try_cmp_with_work(&current.parameter, work)?
                    .is_eq()
        } else {
            false
        };
        if duplicate {
            *duplicates
                .get_mut(index)
                .ok_or(ResourceError::SizeOverflow)? = true;
        } else {
            previous = Some(index);
            unique_count = unique_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
    }
    let mut unique = Vec::new();
    reserve_with_work(&mut unique, unique_count, work)?;
    for (index, duplicate) in duplicates.into_iter().enumerate() {
        work.charge(1)?;
        if !duplicate {
            unique.push(index);
        }
    }
    Ok(unique)
}

pub(crate) struct TraceRowsBuilder {
    offsets: Vec<u32>,
    ranges: Vec<RootRange>,
    sample_offsets: Vec<u32>,
    sample_ranges: Vec<RootRange>,
}

impl TraceRowsBuilder {
    pub(crate) fn new(work: &mut impl Work) -> Result<Self> {
        prepay_slots(1, work)?;
        prepay_slots(1, work)?;
        let offsets = vec![0];
        let sample_offsets = vec![0];
        Ok(Self {
            offsets,
            ranges: Vec::new(),
            sample_offsets,
            sample_ranges: Vec::new(),
        })
    }

    pub(crate) fn push_row<I>(&mut self, ranges: I, work: &mut impl Work) -> Result<()>
    where
        I: IntoIterator<Item = RootRange>,
        I::IntoIter: ExactSizeIterator,
    {
        let ranges = ranges.into_iter();
        let range_count = ranges.len();
        let new_range_count = self
            .ranges
            .len()
            .checked_add(range_count)
            .ok_or(ResourceError::SizeOverflow)?;
        let offset = u32::try_from(new_range_count).map_err(|_| ResourceError::SizeOverflow)?;
        prepay_slots(range_count, work)?;
        prepay_slots(1, work)?;
        self.ranges.reserve(range_count);
        self.offsets.reserve(1);
        self.ranges.extend(ranges);
        self.offsets.push(offset);
        Ok(())
    }

    pub(crate) fn push_sample_row<I>(&mut self, ranges: I, work: &mut impl Work) -> Result<()>
    where
        I: IntoIterator<Item = RootRange>,
        I::IntoIter: ExactSizeIterator,
    {
        let ranges = ranges.into_iter();
        let range_count = ranges.len();
        let new_range_count = self
            .sample_ranges
            .len()
            .checked_add(range_count)
            .ok_or(ResourceError::SizeOverflow)?;
        let offset = u32::try_from(new_range_count).map_err(|_| ResourceError::SizeOverflow)?;
        prepay_slots(range_count, work)?;
        prepay_slots(1, work)?;
        self.sample_ranges.reserve(range_count);
        self.sample_offsets.reserve(1);
        self.sample_ranges.extend(ranges);
        self.sample_offsets.push(offset);
        Ok(())
    }

    pub(crate) fn push_composed_row(
        &mut self,
        parents: &[RootRange],
        local_start: &ExactRational,
        local_end: &ExactRational,
        work: &mut impl Work,
    ) -> Result<()> {
        prepay_slots(parents.len(), work)?;
        prepay_slots(1, work)?;
        self.ranges.reserve(parents.len());
        self.offsets.reserve(1);
        for parent in parents {
            work.charge(1)?;
            if let Some(child) = parent.compose_with_work(local_start, local_end, work)? {
                self.ranges.push(child);
            }
        }
        self.offsets
            .push(u32::try_from(self.ranges.len()).map_err(|_| ResourceError::SizeOverflow)?);
        Ok(())
    }

    pub(crate) fn finish(self, work: &mut impl Work) -> Result<TraceRows> {
        let edge_range_count =
            u32::try_from(self.ranges.len()).map_err(|_| ResourceError::SizeOverflow)?;
        let total = self
            .ranges
            .len()
            .checked_add(self.sample_ranges.len())
            .ok_or(ResourceError::SizeOverflow)?;
        u32::try_from(total).map_err(|_| ResourceError::SizeOverflow)?;
        prepay_slots(total, work)?;
        let mut sample_offsets = self.sample_offsets;
        if edge_range_count > 0 {
            prepay_slots(sample_offsets.len(), work)?;
            for offset in &mut sample_offsets {
                *offset = offset
                    .checked_add(edge_range_count)
                    .ok_or(ResourceError::SizeOverflow)?;
            }
        }
        let mut ranges = Vec::with_capacity(total);
        ranges.extend(self.ranges);
        ranges.extend(self.sample_ranges);
        Ok(TraceRows {
            offsets: self.offsets,
            sample_offsets,
            ranges,
        })
    }
}

fn check_unit_parameter_with_work(parameter: &ExactRational, work: &mut impl Work) -> Result<()> {
    if parameter.is_zero() || parameter.is_one() {
        work.charge(1)?;
        return Ok(());
    }
    let zero = ExactRational::zero();
    let one = ExactRational::one();
    if parameter.try_cmp_with_work(&zero, work)?.is_lt()
        || parameter.try_cmp_with_work(&one, work)?.is_gt()
    {
        return Err(GeometryErrorKind::Invalid(
            "source trace parameter is outside its directed interval".into(),
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;
    use crate::geometry::Unmetered;
    use crate::geometry::embedding_search::{EvaluationBudget, EvaluationOnly};

    const TRACE_OPERATION: &str = "trace_storage_test";

    #[derive(Clone, Copy)]
    struct ChargeAttempt {
        spent_before: u128,
        ticks: u128,
        progress: usize,
        accepted: bool,
    }

    struct RecordingWork<'a> {
        budget: &'a mut EvaluationBudget,
        progress: Rc<Cell<usize>>,
        spent: u128,
        attempts: Vec<ChargeAttempt>,
    }

    impl Work for RecordingWork<'_> {
        fn charge(&mut self, ticks: u64) -> Result<()> {
            let spent_before = self.spent;
            let result = <EvaluationBudget as Work>::charge(self.budget, ticks);
            self.attempts.push(ChargeAttempt {
                spent_before,
                ticks: u128::from(ticks),
                progress: self.progress.get(),
                accepted: result.is_ok(),
            });
            if result.is_ok() {
                self.spent = self
                    .spent
                    .checked_add(u128::from(ticks))
                    .expect("recorded work total fits u128");
            }
            result
        }

        fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
            let ticks = u128::try_from(count)
                .expect("usize fits u128")
                .checked_mul(u128::try_from(ticks_per_item).expect("usize fits u128"))
                .expect("recorded work product fits u128");
            let spent_before = self.spent;
            let result =
                <EvaluationBudget as Work>::charge_product(self.budget, count, ticks_per_item);
            self.attempts.push(ChargeAttempt {
                spent_before,
                ticks,
                progress: self.progress.get(),
                accepted: result.is_ok(),
            });
            if result.is_ok() {
                self.spent = self
                    .spent
                    .checked_add(ticks)
                    .expect("recorded work total fits u128");
            }
            result
        }
    }

    fn refusing_budget(attempt: ChargeAttempt) -> EvaluationBudget {
        assert!(attempt.ticks > 0);
        EvaluationBudget::with_remaining_ticks_for_test(
            TRACE_OPERATION,
            0,
            attempt
                .spent_before
                .checked_add(
                    attempt
                        .ticks
                        .checked_sub(1)
                        .expect("recorded work refusal has positive cost"),
                )
                .expect("the calibrated allowance is representable"),
        )
        .expect("the calibrated allowance fits the actual EvaluationBudget")
    }

    fn assert_work_limit(error: &crate::error::Error) {
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: TRACE_OPERATION,
                ..
            })
        ));
    }

    struct ProgressRoots(Rc<Cell<usize>>);

    impl RootCatalog for ProgressRoots {
        fn resolve<M: ChartMetric, W: Work>(
            &self,
            root: RootId,
            parameter: &ExactRational,
            axes: CoordinateAxes,
            _metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
            _state: &mut RootMetricState,
            _fraction: &mut Option<f64>,
            _work: &mut W,
        ) -> Result<NodeOrdinates> {
            self.0.set(
                self.0
                    .get()
                    .checked_add(1)
                    .expect("test resolution count fits usize"),
            );
            let root_value =
                u32::try_from(root.index()).map_err(|_| ResourceError::SizeOverflow)?;
            let value = f64::from(root_value) + parameter.round()?;
            let mut ordinates = NodeOrdinates::default();
            if axes.has_z() {
                ordinates.z.observe(Some(value));
            }
            if axes.has_m() {
                ordinates.m.observe(Some(-value));
            }
            Ok(ordinates)
        }
    }

    struct SignedZeroRoots;

    impl RootCatalog for SignedZeroRoots {
        fn resolve<M: ChartMetric, W: Work>(
            &self,
            root: RootId,
            _parameter: &ExactRational,
            axes: CoordinateAxes,
            _metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
            _state: &mut RootMetricState,
            _fraction: &mut Option<f64>,
            _work: &mut W,
        ) -> Result<NodeOrdinates> {
            let mut ordinates = NodeOrdinates::default();
            if axes.has_z() {
                ordinates
                    .z
                    .observe(Some(if root.index() == 0 { 0.0 } else { -0.0 }));
            }
            Ok(ordinates)
        }
    }

    struct Fuel(u64);

    impl Work for Fuel {
        fn charge(&mut self, units: u64) -> Result<()> {
            self.0 = self
                .0
                .checked_sub(units)
                .ok_or_else(|| GeometryErrorKind::Invalid("test work fuel exhausted".into()))?;
            Ok(())
        }
    }

    struct RecordingRoots(RefCell<Vec<(RootId, ExactRational, CoordinateAxes)>>);

    impl RootCatalog for RecordingRoots {
        fn resolve<M: ChartMetric, W: Work>(
            &self,
            root: RootId,
            parameter: &ExactRational,
            axes: CoordinateAxes,
            _metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
            _state: &mut RootMetricState,
            _fraction: &mut Option<f64>,
            _work: &mut W,
        ) -> Result<NodeOrdinates> {
            self.0.borrow_mut().push((root, parameter.clone(), axes));
            Ok(NodeOrdinates::default())
        }
    }

    #[test]
    fn directed_child_ranges_compose_immediately_to_the_original_root() {
        let root = RootId(7);
        let original = RootRange::whole(root);
        let quarter = ExactRational::from_f64(0.25);
        let half = ExactRational::from_f64(0.5);
        let three_quarters = ExactRational::from_f64(0.75);

        let first_child = original
            .compose(&quarter, &three_quarters)
            .unwrap()
            .ok_or_else(|| GeometryErrorKind::Invalid("first trace child is empty".into()))
            .unwrap();
        let nested_child = first_child
            .compose(&half, &ExactRational::one())
            .unwrap()
            .ok_or_else(|| GeometryErrorKind::Invalid("nested trace child is empty".into()))
            .unwrap();
        let direct_child = original
            .compose(&ExactRational::from_f64(0.5), &three_quarters)
            .unwrap()
            .ok_or_else(|| GeometryErrorKind::Invalid("direct trace child is empty".into()))
            .unwrap();

        assert_eq!(nested_child, direct_child);
        assert_eq!(nested_child.source_start, ExactRational::from_f64(0.5));
        assert_eq!(nested_child.source_end, three_quarters);
    }

    #[test]
    fn identical_geometry_keeps_distinct_source_root_occurrences() {
        let first = RootId::from_index(0);
        let second = RootId::from_index(1);
        let mut work = Unmetered;
        let mut rows = TraceRowsBuilder::new(&mut work).unwrap();
        rows.push_row(
            [RootRange::whole(first), RootRange::whole(second)],
            &mut work,
        )
        .unwrap();
        let rows = rows.finish(&mut work).unwrap();
        let requests = rows
            .requests_at(0, &ExactRational::from_f64(0.5), &mut work)
            .unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].root(), first);
        assert_eq!(requests[1].root(), second);
        assert!(
            requests
                .iter()
                .all(|request| { request.parameter() == &ExactRational::from_f64(0.5) })
        );
    }

    #[test]
    fn request_dedup_keeps_first_occurrence_order() {
        let first = RootId::from_index(7);
        let second = RootId::from_index(2);
        reset_root_range_mapping_counts();
        let mut work = Unmetered;
        let mut rows = TraceRowsBuilder::new(&mut work).unwrap();
        rows.push_row(
            [
                RootRange::whole(first),
                RootRange::whole(second),
                RootRange::whole(first),
            ],
            &mut work,
        )
        .unwrap();
        let rows = rows.finish(&mut work).unwrap();
        let requests = rows
            .requests_at(0, &ExactRational::from_f64(0.5), &mut work)
            .unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].root(), first);
        assert_eq!(requests[1].root(), second);
        assert_eq!(root_request_row_sorts(), 1);
    }

    #[test]
    fn ordered_request_dedup_preserves_order_without_sorting() -> Result<()> {
        let first = RootId::from_index(1);
        let second = RootId::from_index(2);
        let third = RootId::from_index(3);
        reset_root_range_mapping_counts();
        let mut work = Unmetered;
        let mut rows = TraceRowsBuilder::new(&mut work)?;
        rows.push_row(
            [
                RootRange::whole(first),
                RootRange::whole(second),
                RootRange::whole(second),
                RootRange::whole(third),
            ],
            &mut work,
        )?;
        let rows = rows.finish(&mut work)?;
        let requests = rows.requests_at(0, &ExactRational::from_f64(0.5), &mut work)?;
        assert_eq!(
            requests
                .iter()
                .map(|request| request.root())
                .collect::<Vec<_>>(),
            [first, second, third]
        );
        assert_eq!(root_request_row_sorts(), 0);
        Ok(())
    }

    #[test]
    fn request_table_shares_exact_keys_and_keeps_each_axis_mask() {
        let root = RootId::from_index(4);
        let half = ExactRational::from_f64(0.5);
        let rounded_twin = half
            .try_add(&ExactRational::from_f64(2.0_f64.powi(-54)))
            .unwrap();
        assert_eq!(rounded_twin.round().unwrap(), 0.5);
        assert_ne!(rounded_twin, half);
        let rows = [
            RequestRowInput {
                axes: CoordinateAxes::XY,
                requests: vec![RootRequest {
                    root,
                    parameter: half.clone(),
                }],
            },
            RequestRowInput {
                axes: CoordinateAxes::XYZM,
                requests: vec![RootRequest {
                    root,
                    parameter: half,
                }],
            },
            RequestRowInput {
                axes: CoordinateAxes::XYZ,
                requests: vec![RootRequest {
                    root,
                    parameter: rounded_twin,
                }],
            },
        ];
        let mut work = Unmetered;
        let table = RootRequestTable::from_rows(&rows, &mut work).unwrap();
        let first = RequestRowId::from_index(0).unwrap();
        let second = RequestRowId::from_index(1).unwrap();
        let rounded = RequestRowId::from_index(2).unwrap();
        let first_request = table.request_id_at(first, 0).unwrap();
        assert_eq!(first_request, table.request_id_at(second, 0).unwrap());
        assert_ne!(first_request, table.request_id_at(rounded, 0).unwrap());
        assert_eq!(table.axes(first).unwrap(), CoordinateAxes::XY);
        assert_eq!(table.axes(second).unwrap(), CoordinateAxes::XYZM);
        assert_eq!(table.axes(rounded).unwrap(), CoordinateAxes::XYZ);
        assert!(table.record(first_request).unwrap().fraction.is_none());
    }

    #[test]
    fn root_slots_are_dense_across_alternating_and_repeated_requests() {
        let first_root = RootId::from_index(7);
        let second_root = RootId::from_index(2);
        let third_root = RootId::from_index(5);
        let first_parameter = ExactRational::from_f64(0.75);
        let second_parameter = ExactRational::from_f64(0.25);
        let third_parameter = ExactRational::from_f64(0.5);
        let rows = [
            RequestRowInput {
                axes: CoordinateAxes::XY,
                requests: vec![
                    RootRequest {
                        root: first_root,
                        parameter: first_parameter.clone(),
                    },
                    RootRequest {
                        root: second_root,
                        parameter: second_parameter,
                    },
                ],
            },
            RequestRowInput {
                axes: CoordinateAxes::XYZM,
                requests: vec![
                    RootRequest {
                        root: first_root,
                        parameter: first_parameter,
                    },
                    RootRequest {
                        root: third_root,
                        parameter: third_parameter,
                    },
                ],
            },
        ];
        let mut work = Unmetered;
        let table = RootRequestTable::from_rows(&rows, &mut work).unwrap();
        let first = RequestRowId::from_index(0).unwrap();
        let second = RequestRowId::from_index(1).unwrap();
        assert_eq!(table.root_count().unwrap(), 3);
        assert_eq!(
            table
                .root_state_slot(table.request_id_at(first, 0).unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            table
                .root_state_slot(table.request_id_at(first, 1).unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            table
                .root_state_slot(table.request_id_at(second, 0).unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            table
                .root_state_slot(table.request_id_at(second, 1).unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            table.request_id_at(first, 0).unwrap(),
            table.request_id_at(second, 0).unwrap()
        );
    }

    #[test]
    fn unique_request_records_pay_capacity_and_exact_parameter_clones() {
        let quarter_turn = ExactRational::from_f64(0.75);
        let mut parameter = ExactRational::one();
        let mut setup = Unmetered;
        for _ in 0..100 {
            parameter = parameter
                .try_product_with_work(&quarter_turn, &mut setup)
                .unwrap();
        }
        let request = RootRequest {
            root: RootId::from_index(3),
            parameter,
        };
        let target_attempts = {
            let progress = Rc::new(Cell::new(0));
            let mut budget = EvaluationBudget::new(TRACE_OPERATION, 0).unwrap();
            let mut work = RecordingWork {
                budget: &mut budget,
                progress: Rc::clone(&progress),
                spent: 0,
                attempts: Vec::new(),
            };
            let mut records = Vec::new();
            let id = push_unique_root_request(&mut records, &request, &mut work).unwrap();
            progress.set(1);
            let record = records
                .get(id.0 as usize)
                .expect("the unique request was stored at its returned identifier");
            assert_eq!(record.root, request.root);
            assert_eq!(record.parameter, request.parameter);
            assert_eq!(progress.get(), 1);
            assert_eq!(work.attempts.first().map(|attempt| attempt.ticks), Some(1));
            assert!(
                work.attempts.len() > 1,
                "the arbitrary-width exact parameter clone has its own Work charges"
            );
            [work.attempts[0], work.attempts[1]]
        };

        for target in target_attempts {
            let progress = Rc::new(Cell::new(0));
            let mut budget = refusing_budget(target);
            let mut work = RecordingWork {
                budget: &mut budget,
                progress: Rc::clone(&progress),
                spent: 0,
                attempts: Vec::new(),
            };
            let mut records = Vec::new();
            let Err(error) = push_unique_root_request(&mut records, &request, &mut work) else {
                panic!("request capacity and parameter payload must be paid before insert");
            };
            assert_work_limit(&error);
            assert!(records.is_empty());
            assert_eq!(progress.get(), 0);
            let refusal = work
                .attempts
                .last()
                .expect("the recorder observes the refused request-record phase");
            assert!(!refusal.accepted);
            assert_eq!(refusal.ticks, target.ticks);
            assert_eq!(refusal.spent_before, target.spent_before);
            assert_eq!(refusal.progress, 0);
        }
    }

    #[test]
    fn root_state_storage_refusal_precedes_first_ordinate_resolution() {
        let requests = (0..7)
            .map(|root| RootRequest {
                root: RootId::from_index(root),
                parameter: ExactRational::from_f64(0.5),
            })
            .collect();
        let rows = [RequestRowInput {
            axes: CoordinateAxes::XYZM,
            requests,
        }];
        let table_attempt_count = {
            let table_progress = Rc::new(Cell::new(0));
            let mut table_budget = EvaluationBudget::new(TRACE_OPERATION, 0).unwrap();
            let mut table_work = RecordingWork {
                budget: &mut table_budget,
                progress: Rc::clone(&table_progress),
                spent: 0,
                attempts: Vec::new(),
            };
            let table = RootRequestTable::from_rows(&rows, &mut table_work).unwrap();
            assert_eq!(table.root_count().unwrap(), 7);
            assert_eq!(
                table_work
                    .attempts
                    .iter()
                    .filter(|attempt| attempt.ticks == 7)
                    .count(),
                3,
                "the three occurrence-index vectors reserve seven slots; unique records pay individually"
            );
            table_work.attempts.len()
        };

        let target = {
            let progress = Rc::new(Cell::new(0));
            let roots = ProgressRoots(Rc::clone(&progress));
            let mut budget = EvaluationBudget::new(TRACE_OPERATION, 0).unwrap();
            let mut work = RecordingWork {
                budget: &mut budget,
                progress: Rc::clone(&progress),
                spent: 0,
                attempts: Vec::new(),
            };
            let ordinates = resolve_request_rows::<crate::crs::GeodesicMetric, _>(
                &rows, &roots, None, &mut work,
            )
            .unwrap();
            assert_eq!(ordinates.len(), 1);
            assert_eq!(progress.get(), 7);
            let target = *work
                .attempts
                .get(table_attempt_count)
                .expect("dense root-state storage follows the complete request table");
            assert!(target.accepted);
            assert_eq!(target.ticks, 7);
            assert_eq!(target.progress, 0);
            target
        };

        let progress = Rc::new(Cell::new(0));
        let roots = ProgressRoots(Rc::clone(&progress));
        let mut budget = refusing_budget(target);
        let mut work = RecordingWork {
            budget: &mut budget,
            progress: Rc::clone(&progress),
            spent: 0,
            attempts: Vec::new(),
        };
        let Err(error) =
            resolve_request_rows::<crate::crs::GeodesicMetric, _>(&rows, &roots, None, &mut work)
        else {
            panic!("root state allocation must be paid before ordinate resolution");
        };
        assert_work_limit(&error);
        assert_eq!(progress.get(), 0);
        let refusal = work
            .attempts
            .last()
            .expect("the recorder observes the refused root-state charge");
        assert!(!refusal.accepted);
        assert_eq!(refusal.ticks, target.ticks);
        assert_eq!(refusal.spent_before, target.spent_before);
        assert_eq!(refusal.progress, 0);
    }

    #[test]
    fn request_table_storage_refusal_precedes_first_ordinate_resolution() {
        let rows = [RequestRowInput {
            axes: CoordinateAxes::XYZM,
            requests: (0..7)
                .map(|root| RootRequest {
                    root: RootId::from_index(root),
                    parameter: ExactRational::from_f64(0.5),
                })
                .collect(),
        }];
        let target = {
            let progress = Rc::new(Cell::new(0));
            let roots = ProgressRoots(Rc::clone(&progress));
            let mut budget = EvaluationBudget::new(TRACE_OPERATION, 0).unwrap();
            let mut work = RecordingWork {
                budget: &mut budget,
                progress: Rc::clone(&progress),
                spent: 0,
                attempts: Vec::new(),
            };
            resolve_request_rows::<crate::crs::GeodesicMetric, _>(&rows, &roots, None, &mut work)
                .unwrap();
            assert_eq!(progress.get(), 7);
            let target = *work
                .attempts
                .iter()
                .rev()
                .find(|attempt| attempt.progress == 0)
                .expect("the first ordinate call has a preceding paid work event");
            assert!(target.accepted);
            target
        };

        let progress = Rc::new(Cell::new(0));
        let roots = ProgressRoots(Rc::clone(&progress));
        let mut budget = refusing_budget(target);
        let mut work = RecordingWork {
            budget: &mut budget,
            progress: Rc::clone(&progress),
            spent: 0,
            attempts: Vec::new(),
        };
        let Err(error) =
            resolve_request_rows::<crate::crs::GeodesicMetric, _>(&rows, &roots, None, &mut work)
        else {
            panic!("request-table storage refusal must precede the first ordinate call");
        };
        assert_work_limit(&error);
        assert_eq!(progress.get(), 0);
        let refusal = work
            .attempts
            .last()
            .expect("the recorder observes the refused final pre-resolution charge");
        assert!(!refusal.accepted);
        assert_eq!(refusal.ticks, target.ticks);
        assert_eq!(refusal.spent_before, target.spent_before);
        assert_eq!(refusal.progress, 0);
    }

    #[test]
    fn evaluation_only_and_unmetered_request_rows_are_bit_identical() {
        let rows = [
            RequestRowInput {
                axes: CoordinateAxes::XYZM,
                requests: vec![RootRequest {
                    root: RootId::from_index(7),
                    parameter: ExactRational::from_f64(0.25),
                }],
            },
            RequestRowInput {
                axes: CoordinateAxes::XYZM,
                requests: vec![RootRequest {
                    root: RootId::from_index(2),
                    parameter: ExactRational::from_f64(0.5),
                }],
            },
            RequestRowInput {
                axes: CoordinateAxes::XYZM,
                requests: vec![RootRequest {
                    root: RootId::from_index(7),
                    parameter: ExactRational::from_f64(0.25),
                }],
            },
        ];
        let ordinate_bits = |values: &[NodeOrdinates]| {
            values
                .iter()
                .map(|value| {
                    (
                        value.z.value().map(f64::to_bits),
                        value.m.value().map(f64::to_bits),
                    )
                })
                .collect::<Vec<_>>()
        };
        let baseline_progress = Rc::new(Cell::new(0));
        let baseline_roots = ProgressRoots(Rc::clone(&baseline_progress));
        let mut unmetered = Unmetered;
        let baseline = resolve_request_rows::<crate::crs::GeodesicMetric, _>(
            &rows,
            &baseline_roots,
            None,
            &mut unmetered,
        )
        .unwrap();

        let evaluation_progress = Rc::new(Cell::new(0));
        let evaluation_roots = ProgressRoots(Rc::clone(&evaluation_progress));
        let mut budget =
            EvaluationBudget::with_remaining_ticks_for_test(TRACE_OPERATION, 0, 0).unwrap();
        let evaluation_only = {
            let mut work = EvaluationOnly::new(&mut budget);
            resolve_request_rows::<crate::crs::GeodesicMetric, _>(
                &rows,
                &evaluation_roots,
                None,
                &mut work,
            )
            .unwrap()
        };
        assert_eq!(ordinate_bits(&baseline), ordinate_bits(&evaluation_only));
        assert_eq!(evaluation_progress.get(), baseline_progress.get());
        assert!(
            <EvaluationBudget as Work>::charge(&mut budget, 1).is_err(),
            "EvaluationOnly must not spend raw primitive-work allowance"
        );

        let ordinary_progress = Rc::new(Cell::new(0));
        let ordinary_roots = ProgressRoots(Rc::clone(&ordinary_progress));
        let mut ordinary = Unmetered;
        let ordinary_result = resolve_request_rows::<crate::crs::GeodesicMetric, _>(
            &rows,
            &ordinary_roots,
            None,
            &mut ordinary,
        )
        .unwrap();
        assert_eq!(ordinate_bits(&baseline), ordinate_bits(&ordinary_result));
        assert_eq!(ordinary_progress.get(), baseline_progress.get());
    }

    #[test]
    fn distinct_root_signed_zero_values_remain_a_conflict() {
        let root_a = RootId::from_index(0);
        let root_b = RootId::from_index(1);
        let rows = [RequestRowInput {
            axes: CoordinateAxes::XYZ,
            requests: vec![
                RootRequest {
                    root: root_a,
                    parameter: ExactRational::from_f64(0.5),
                },
                RootRequest {
                    root: root_b,
                    parameter: ExactRational::from_f64(0.5),
                },
            ],
        }];
        let mut work = Unmetered;
        let output = resolve_request_rows::<crate::crs::GeodesicMetric, _>(
            &rows,
            &SignedZeroRoots,
            None,
            &mut work,
        )
        .unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].z.value(), None);
    }

    #[test]
    fn empty_sample_rows_keep_their_offsets() {
        let root = RootId::from_index(4);
        let mut work = Unmetered;
        let mut builder = TraceRowsBuilder::new(&mut work).unwrap();
        for _ in 0..128 {
            builder.push_sample_row([], &mut work).unwrap();
        }
        builder
            .push_sample_row([RootRange::whole(root)], &mut work)
            .unwrap();
        let rows = builder.finish(&mut work).unwrap();

        for sample in 0..128 {
            assert!(
                rows.sample_requests_at(sample, &mut work)
                    .unwrap()
                    .is_empty()
            );
        }
        let last = rows.sample_requests_at(128, &mut work).unwrap();
        assert_eq!(last.len(), 1);
        assert_eq!(last[0].root(), root);
    }

    #[test]
    fn trace_row_clone_prepays_storage_and_exact_range_copies() {
        let root = RootId::from_index(8);
        let quarter_turn = ExactRational::from_f64(0.75);
        let mut source_start = ExactRational::one();
        let mut setup = Unmetered;
        for _ in 0..100 {
            source_start = source_start
                .try_product_with_work(&quarter_turn, &mut setup)
                .unwrap();
        }
        let range = RootRange::from_mapped(
            root,
            source_start,
            ExactRational::from_f64(0.75),
            ExactRational::from_f64(0.125),
            ExactRational::from_f64(0.875),
        )
        .unwrap();
        let mut builder = TraceRowsBuilder::new(&mut setup).unwrap();
        builder.push_row([range], &mut setup).unwrap();
        let rows = builder.finish(&mut setup).unwrap();

        let target_attempts = {
            let progress = Rc::new(Cell::new(0));
            let mut budget = EvaluationBudget::new(TRACE_OPERATION, 0).unwrap();
            let mut work = RecordingWork {
                budget: &mut budget,
                progress: Rc::clone(&progress),
                spent: 0,
                attempts: Vec::new(),
            };
            let cloned = rows.try_clone_with_work(&mut work).unwrap();
            progress.set(1);
            assert_eq!(cloned.row(0).unwrap(), rows.row(0).unwrap());
            assert_eq!(progress.get(), 1);
            assert_eq!(
                work.attempts
                    .iter()
                    .map(|attempt| attempt.ticks)
                    .take(3)
                    .collect::<Vec<_>>(),
                [2, 1, 1]
            );
            let first_range_copy = work
                .attempts
                .get(3)
                .copied()
                .expect("the original-root exact payload clone has a Work charge");
            assert!(first_range_copy.ticks > 1);
            first_range_copy
        };

        let progress = Rc::new(Cell::new(0));
        let mut budget = refusing_budget(target_attempts);
        let mut work = RecordingWork {
            budget: &mut budget,
            progress: Rc::clone(&progress),
            spent: 0,
            attempts: Vec::new(),
        };
        let Err(error) = rows.try_clone_with_work(&mut work) else {
            panic!("exact root-range copies must be paid before clone completion");
        };
        assert_work_limit(&error);
        assert_eq!(progress.get(), 0);
        let refusal = work
            .attempts
            .last()
            .expect("the recorder observes the refused exact-copy charge");
        assert!(!refusal.accepted);
        assert_eq!(refusal.ticks, target_attempts.ticks);
        assert_eq!(refusal.spent_before, target_attempts.spent_before);
        assert_eq!(refusal.progress, 0);
    }

    fn append_test_trace_row(work: &mut impl Work, progress: &Cell<usize>) -> Result<()> {
        let mut builder = TraceRowsBuilder::new(work)?;
        builder.push_row(
            [
                RootRange::whole(RootId::from_index(0)),
                RootRange::whole(RootId::from_index(1)),
                RootRange::whole(RootId::from_index(2)),
                RootRange::whole(RootId::from_index(3)),
            ],
            work,
        )?;
        progress.set(1);
        Ok(())
    }

    #[test]
    fn trace_row_storage_and_offsets_refuse_before_append_progress() {
        let target_attempts = {
            let progress = Rc::new(Cell::new(0));
            let mut budget = EvaluationBudget::new(TRACE_OPERATION, 0).unwrap();
            let mut work = RecordingWork {
                budget: &mut budget,
                progress: Rc::clone(&progress),
                spent: 0,
                attempts: Vec::new(),
            };
            append_test_trace_row(&mut work, &progress).unwrap();
            assert_eq!(progress.get(), 1);
            assert_eq!(
                work.attempts
                    .iter()
                    .map(|attempt| attempt.ticks)
                    .collect::<Vec<_>>(),
                [1, 1, 4, 1]
            );
            [work.attempts[2], work.attempts[3]]
        };

        for target in target_attempts {
            let progress = Rc::new(Cell::new(0));
            let mut budget = refusing_budget(target);
            let mut work = RecordingWork {
                budget: &mut budget,
                progress: Rc::clone(&progress),
                spent: 0,
                attempts: Vec::new(),
            };
            let error = append_test_trace_row(&mut work, &progress)
                .expect_err("trace row storage and its offset must be paid before append");
            assert_work_limit(&error);
            assert_eq!(progress.get(), 0);
            let refusal = work
                .attempts
                .last()
                .expect("the recorder observes the refused reservation");
            assert!(!refusal.accepted);
            assert_eq!(refusal.ticks, target.ticks);
            assert_eq!(refusal.spent_before, target.spent_before);
            assert_eq!(refusal.progress, 0);
        }
    }

    #[test]
    fn sample_row_storage_and_offset_refuse_before_append_progress() {
        let target_attempts = {
            let progress = Rc::new(Cell::new(0));
            let mut budget = EvaluationBudget::new(TRACE_OPERATION, 0).unwrap();
            let mut work = RecordingWork {
                budget: &mut budget,
                progress: Rc::clone(&progress),
                spent: 0,
                attempts: Vec::new(),
            };
            let mut builder = TraceRowsBuilder::new(&mut work).unwrap();
            builder
                .push_sample_row([RootRange::whole(RootId::from_index(4))], &mut work)
                .unwrap();
            progress.set(1);
            assert_eq!(progress.get(), 1);
            assert_eq!(
                work.attempts
                    .iter()
                    .map(|attempt| attempt.ticks)
                    .collect::<Vec<_>>(),
                [1, 1, 1, 1]
            );
            [work.attempts[2], work.attempts[3]]
        };

        for target in target_attempts {
            let progress = Rc::new(Cell::new(0));
            let mut budget = refusing_budget(target);
            let mut work = RecordingWork {
                budget: &mut budget,
                progress: Rc::clone(&progress),
                spent: 0,
                attempts: Vec::new(),
            };
            let mut builder = TraceRowsBuilder::new(&mut work).unwrap();
            let Err(error) =
                builder.push_sample_row([RootRange::whole(RootId::from_index(4))], &mut work)
            else {
                progress.set(1);
                panic!("sample row storage and offset must be paid before append");
            };
            assert_work_limit(&error);
            assert_eq!(progress.get(), 0);
            let refusal = work
                .attempts
                .last()
                .expect("the recorder observes the refused sample-row reservation");
            assert!(!refusal.accepted);
            assert_eq!(refusal.ticks, target.ticks);
            assert_eq!(refusal.spent_before, target.spent_before);
            assert_eq!(refusal.progress, 0);
        }
    }

    #[test]
    fn trace_finish_prepays_range_copy_and_sample_offsets() {
        fn finish_test_trace(work: &mut impl Work, progress: &Cell<usize>) -> Result<()> {
            let mut setup = Unmetered;
            let mut builder = TraceRowsBuilder::new(&mut setup)?;
            builder.push_row(
                [
                    RootRange::whole(RootId::from_index(0)),
                    RootRange::whole(RootId::from_index(1)),
                    RootRange::whole(RootId::from_index(2)),
                    RootRange::whole(RootId::from_index(3)),
                ],
                &mut setup,
            )?;
            builder.push_sample_row([], &mut setup)?;
            builder.push_sample_row([], &mut setup)?;
            let _rows = builder.finish(work)?;
            progress.set(1);
            Ok(())
        }

        let target_attempts = {
            let progress = Rc::new(Cell::new(0));
            let mut budget = EvaluationBudget::new(TRACE_OPERATION, 0).unwrap();
            let mut work = RecordingWork {
                budget: &mut budget,
                progress: Rc::clone(&progress),
                spent: 0,
                attempts: Vec::new(),
            };
            finish_test_trace(&mut work, &progress).unwrap();
            assert_eq!(progress.get(), 1);
            assert_eq!(
                work.attempts
                    .iter()
                    .map(|attempt| attempt.ticks)
                    .collect::<Vec<_>>(),
                [4, 3]
            );
            [work.attempts[0], work.attempts[1]]
        };

        for target in target_attempts {
            let progress = Rc::new(Cell::new(0));
            let mut budget = refusing_budget(target);
            let mut work = RecordingWork {
                budget: &mut budget,
                progress: Rc::clone(&progress),
                spent: 0,
                attempts: Vec::new(),
            };
            let error = finish_test_trace(&mut work, &progress)
                .expect_err("trace final storage and sample offsets must be paid before finish");
            assert_work_limit(&error);
            assert_eq!(progress.get(), 0);
            let refusal = work
                .attempts
                .last()
                .expect("the recorder observes the refused finish phase");
            assert!(!refusal.accepted);
            assert_eq!(refusal.ticks, target.ticks);
            assert_eq!(refusal.spent_before, target.spent_before);
            assert_eq!(refusal.progress, 0);
        }
    }

    #[test]
    fn grouped_requests_resolve_in_written_occurrence_order() {
        let first_root = RootId::from_index(7);
        let second_root = RootId::from_index(2);
        let third_root = RootId::from_index(5);
        let first_parameter = ExactRational::from_f64(0.75);
        let second_parameter = ExactRational::from_f64(0.25);
        let third_parameter = ExactRational::from_f64(0.5);
        let rows = [
            RequestRowInput {
                axes: CoordinateAxes::XY,
                requests: vec![
                    RootRequest {
                        root: first_root,
                        parameter: first_parameter.clone(),
                    },
                    RootRequest {
                        root: second_root,
                        parameter: second_parameter.clone(),
                    },
                ],
            },
            RequestRowInput {
                axes: CoordinateAxes::XYZM,
                requests: vec![
                    RootRequest {
                        root: first_root,
                        parameter: first_parameter.clone(),
                    },
                    RootRequest {
                        root: third_root,
                        parameter: third_parameter.clone(),
                    },
                ],
            },
        ];
        let roots = RecordingRoots(RefCell::new(Vec::new()));
        let mut work = Unmetered;

        resolve_request_rows::<crate::crs::GeodesicMetric, _>(&rows, &roots, None, &mut work)
            .unwrap();

        assert_eq!(roots.0.borrow().as_slice(), &[
            (first_root, first_parameter.clone(), CoordinateAxes::XY),
            (second_root, second_parameter, CoordinateAxes::XY),
            (first_root, first_parameter, CoordinateAxes::XYZM),
            (third_root, third_parameter, CoordinateAxes::XYZM),
        ]);
    }

    #[test]
    fn endpoint_only_support_has_no_interior_root_requests() {
        let root = RootId::from_index(5);
        let start = ExactRational::from_f64(0.25);
        let end = ExactRational::from_f64(0.75);
        let mut work = Unmetered;
        let mut rows = TraceRowsBuilder::new(&mut work).unwrap();
        rows.push_row(
            [
                RootRange::point(root, start.clone(), ExactRational::zero()).unwrap(),
                RootRange::point(root, end.clone(), ExactRational::one()).unwrap(),
            ],
            &mut work,
        )
        .unwrap();
        let rows = rows.finish(&mut work).unwrap();

        let start_request = rows
            .requests_at(0, &ExactRational::zero(), &mut work)
            .unwrap();
        assert_eq!(start_request.len(), 1);
        assert_eq!(start_request[0].parameter(), &start);
        assert!(
            rows.requests_at(0, &ExactRational::from_f64(0.5), &mut work)
                .unwrap()
                .is_empty()
        );
        let end_request = rows
            .requests_at(0, &ExactRational::one(), &mut work)
            .unwrap();
        assert_eq!(end_request.len(), 1);
        assert_eq!(end_request[0].parameter(), &end);
    }

    #[test]
    fn point_producer_support_cannot_cover_a_source_interval() {
        RootRange::from_mapped(
            RootId::from_index(8),
            ExactRational::zero(),
            ExactRational::one(),
            ExactRational::from_f64(0.5),
            ExactRational::from_f64(0.5),
        )
        .unwrap_err();
    }

    #[test]
    fn endpoint_only_support_survives_a_split_at_its_written_point() {
        let root = RootId::from_index(6);
        let half = ExactRational::from_f64(0.5);
        let source_parameter = ExactRational::from_f64(0.375);
        let owner = RootRange::point(root, source_parameter.clone(), half.clone()).unwrap();
        let mut work = Unmetered;

        let mut left = TraceRowsBuilder::new(&mut work).unwrap();
        left.push_composed_row(
            std::slice::from_ref(&owner),
            &ExactRational::zero(),
            &half,
            &mut work,
        )
        .unwrap();
        let left = left.finish(&mut work).unwrap();
        let mut right = TraceRowsBuilder::new(&mut work).unwrap();
        right
            .push_composed_row(
                std::slice::from_ref(&owner),
                &half,
                &ExactRational::one(),
                &mut work,
            )
            .unwrap();
        let right = right.finish(&mut work).unwrap();

        for (rows, edge, local) in [
            (&left, 0, ExactRational::one()),
            (&right, 0, ExactRational::zero()),
        ] {
            let requests = rows.requests_at(edge, &local, &mut work).unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].root(), root);
            assert_eq!(requests[0].parameter(), &source_parameter);
        }
    }

    #[test]
    fn child_rows_keep_only_the_contributors_covering_their_source_interval() {
        let zero = ExactRational::zero();
        let quarter = ExactRational::from_f64(0.25);
        let half = ExactRational::from_f64(0.5);
        let one = ExactRational::one();
        let owner = RootRange::from_mapped(
            RootId::from_index(3),
            zero.clone(),
            one.clone(),
            quarter.clone(),
            half.clone(),
        )
        .unwrap();
        let mut work = Unmetered;

        let mut left = TraceRowsBuilder::new(&mut work).unwrap();
        left.push_composed_row(std::slice::from_ref(&owner), &zero, &quarter, &mut work)
            .unwrap();
        let left = left.finish(&mut work).unwrap();
        let mut middle = TraceRowsBuilder::new(&mut work).unwrap();
        middle
            .push_composed_row(std::slice::from_ref(&owner), &quarter, &half, &mut work)
            .unwrap();
        let middle = middle.finish(&mut work).unwrap();
        let mut right = TraceRowsBuilder::new(&mut work).unwrap();
        right
            .push_composed_row(std::slice::from_ref(&owner), &half, &one, &mut work)
            .unwrap();
        let right = right.finish(&mut work).unwrap();

        assert_eq!(left.owner_count(), 0);
        assert_eq!(middle.owner_count(), 1);
        let middle_owner = middle
            .row(0)
            .unwrap()
            .first()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("middle trace row has no source owner".into())
            })
            .unwrap();
        assert_eq!(middle_owner.source_start, ExactRational::zero());
        assert_eq!(middle_owner.source_end, ExactRational::one());
        assert_eq!(middle_owner.producer_start, ExactRational::zero());
        assert_eq!(middle_owner.producer_end, ExactRational::one());
        assert_eq!(right.owner_count(), 0);
    }

    #[test]
    fn reversed_source_and_producer_ranges_keep_partial_composition_direction() {
        let quarter = ExactRational::from_f64(0.25);
        let half = ExactRational::from_f64(0.5);
        let three_quarters = ExactRational::from_f64(0.75);
        let owner = RootRange::from_mapped(
            RootId::from_index(9),
            three_quarters.clone(),
            quarter.clone(),
            three_quarters.clone(),
            quarter.clone(),
        )
        .unwrap();

        assert_eq!(
            owner
                .request_at(&three_quarters)
                .unwrap()
                .ok_or_else(|| GeometryErrorKind::Invalid("reversed range lost its start".into()))
                .unwrap()
                .parameter(),
            &three_quarters
        );
        assert_eq!(
            owner
                .request_at(&quarter)
                .unwrap()
                .ok_or_else(|| GeometryErrorKind::Invalid("reversed range lost its end".into()))
                .unwrap()
                .parameter(),
            &quarter
        );

        let child = owner
            .compose(&ExactRational::zero(), &half)
            .unwrap()
            .ok_or_else(|| GeometryErrorKind::Invalid("partial reversed child is empty".into()))
            .unwrap();
        assert_eq!(child.source_start, quarter);
        assert_eq!(child.source_end, half);
        assert_eq!(child.producer_start, ExactRational::from_f64(0.5));
        assert_eq!(child.producer_end, ExactRational::one());
    }

    #[test]
    fn request_at_join_keeps_both_endpoint_occurrences_before_child_composition() {
        let zero = ExactRational::zero();
        let half = ExactRational::from_f64(0.5);
        let one = ExactRational::one();
        let left = RootRange::from_mapped(
            RootId::from_index(3),
            zero.clone(),
            half.clone(),
            zero.clone(),
            half.clone(),
        )
        .unwrap();
        let right = RootRange::from_mapped(
            RootId::from_index(4),
            half.clone(),
            one.clone(),
            half.clone(),
            one,
        )
        .unwrap();
        let mut work = Unmetered;
        let left_copy = left.try_clone_with_work(&mut work).unwrap();
        let right_copy = right.try_clone_with_work(&mut work).unwrap();
        let mut rows = TraceRowsBuilder::new(&mut work).unwrap();
        rows.push_row([left_copy, right_copy], &mut work).unwrap();
        let rows = rows.finish(&mut work).unwrap();
        let requests = rows.requests_at(0, &half, &mut work).unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].root(), RootId::from_index(3));
        assert_eq!(requests[1].root(), RootId::from_index(4));
        assert!(requests.iter().all(|request| request.parameter() == &half));

        assert!(left.compose(&zero, &half).unwrap().is_some());
        assert!(right.compose(&zero, &half).unwrap().is_none());
    }

    #[test]
    fn composed_trace_charges_each_parent_range() {
        let mut work = Fuel(8);
        let mut rows = TraceRowsBuilder::new(&mut work).unwrap();
        let parents = [
            RootRange::whole(RootId::from_index(1)),
            RootRange::whole(RootId::from_index(2)),
        ];

        assert!(
            rows.push_composed_row(
                &parents,
                &ExactRational::zero(),
                &ExactRational::one(),
                &mut work,
            )
            .is_err()
        );
    }

    #[test]
    fn finishing_trace_rows_charges_stored_ranges() {
        let mut setup = Unmetered;
        let mut rows = TraceRowsBuilder::new(&mut setup).unwrap();
        rows.push_row(
            [
                RootRange::whole(RootId::from_index(1)),
                RootRange::whole(RootId::from_index(2)),
            ],
            &mut setup,
        )
        .unwrap();
        let mut work = Fuel(1);

        assert!(rows.finish(&mut work).is_err());
    }
}
