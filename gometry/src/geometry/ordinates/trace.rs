use super::{
    NodeOrdinates, RootMetricState, SourceArcMetricAccess, SourceArcRequest, source_arc_ordinates,
};
use crate::error::{ResourceError, Result};
use crate::geographic::{GeographicFrame, RetainedChartDisplacement};
use crate::geometry::segments::try_sort_by_with_work;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{ChartMetric, CoordinateAxes, GeometryErrorKind, Point, Work};

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RootRange {
    root: RootId,
    source_start: ExactRational,
    source_end: ExactRational,
    producer_start: ExactRational,
    producer_end: ExactRational,
}

impl RootRange {
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
    roots: Vec<RootId>,
}

impl RootRequestTable {
    pub(crate) fn from_rows(rows: &[RequestRowInput], work: &mut impl Work) -> Result<Self> {
        work.charge(u64::try_from(rows.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut request_count = 0_usize;
        for row in rows {
            work.charge(1)?;
            request_count = request_count
                .checked_add(row.requests.len())
                .ok_or(ResourceError::SizeOverflow)?;
        }
        work.charge(u64::try_from(rows.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let request_views = rows
            .iter()
            .map(|row| row.requests.as_slice())
            .collect::<Vec<_>>();
        let row_offset_capacity = rows
            .len()
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge(u64::try_from(row_offset_capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut row_offsets = Vec::with_capacity(row_offset_capacity);
        row_offsets.push(0);
        work.charge(u64::try_from(request_count).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut ordered = Vec::with_capacity(request_count);
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

        work.charge(u64::try_from(request_count).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut records: Vec<UniqueRootRequest> = Vec::with_capacity(request_count);
        let mut roots = Vec::new();
        work.charge(u64::try_from(request_count).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut row_requests = vec![None; request_count];
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
                let root_state_slot = if roots.last() == Some(&request.root) {
                    roots
                        .len()
                        .checked_sub(1)
                        .ok_or(ResourceError::SizeOverflow)?
                } else {
                    let slot = roots.len();
                    roots.push(request.root);
                    slot
                };
                let request_id = UniqueRequestId(
                    u32::try_from(records.len()).map_err(|_| ResourceError::SizeOverflow)?,
                );
                records.push(UniqueRootRequest {
                    root: request.root,
                    parameter: request.parameter.try_clone_with_work(work)?,
                    root_state_slot,
                    fraction: None,
                });
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
        work.charge(u64::try_from(request_count).map_err(|_| ResourceError::SizeOverflow)?)?;
        let row_requests = row_requests
            .into_iter()
            .map(|request| {
                request.ok_or_else(|| {
                    GeometryErrorKind::Invalid("source request occurrence was not indexed".into())
                        .into()
                })
            })
            .collect::<Result<Vec<_>>>()?;
        work.charge(u64::try_from(rows.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut row_axes = Vec::with_capacity(rows.len());
        for row in rows {
            work.charge(1)?;
            row_axes.push(row.axes);
        }
        Ok(Self {
            records,
            row_offsets,
            row_requests,
            row_axes,
            roots,
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

    pub(crate) const fn root_count(&self) -> usize {
        self.roots.len()
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
    work.charge(u64::try_from(table.root_count()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut states = std::iter::repeat_with(|| None)
        .take(table.root_count())
        .collect::<Vec<Option<RootMetricState>>>();
    work.charge(u64::try_from(rows.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut output = Vec::with_capacity(rows.len());
    for row_index in 0..rows.len() {
        work.charge(1)?;
        let row_id = RequestRowId::from_index(row_index)?;
        let row_axes = table.axes(row_id)?;
        let request_count = table.row(row_id)?.len();
        let mut ordinates = NodeOrdinates::default();
        for request_index in 0..request_count {
            let request_id = table.request_id_at(row_id, request_index)?;
            let state_slot = table.root_state_slot(request_id)?;
            let state = states
                .get_mut(state_slot)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("source root state slot is unknown".into())
                })?
                .get_or_insert_with(RootMetricState::default);
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

#[derive(Clone)]
pub(crate) struct TraceRows {
    offsets: Vec<u32>,
    sample_offsets: Vec<u32>,
    ranges: Vec<RootRange>,
}

impl TraceRows {
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
        Ok(self.requests_with_support_at(edge, local, work)?.0)
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
    check_unit_parameter_with_work(local, work)?;
    let mut requests = Vec::new();
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
    let rows = [requests.as_slice()];
    let unique = unique_request_indices(&rows, work)?;
    let mut unique_requests = Vec::with_capacity(unique.len());
    for occurrence in unique {
        work.charge(1)?;
        unique_requests.push(request_at(&rows, occurrence)?.try_clone_with_work(work)?);
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

fn unique_request_indices(
    rows: &[&[RootRequest]],
    work: &mut impl Work,
) -> Result<Vec<RequestIndex>> {
    let capacity = rows.iter().try_fold(0_usize, |count, row| {
        count
            .checked_add(row.len())
            .ok_or(ResourceError::SizeOverflow)
    })?;
    let mut ordered = Vec::with_capacity(capacity);
    for (row_index, row) in rows.iter().enumerate() {
        for index in 0..row.len() {
            work.charge(1)?;
            ordered.push(RequestIndex {
                row: row_index,
                index,
            });
        }
    }
    try_sort_by_with_work(&mut ordered, work, |left, right, work| {
        let left = request_at(rows, *left)?;
        let right = request_at(rows, *right)?;
        let root_order = left.root.cmp(&right.root);
        if root_order.is_eq() {
            work.charge(1)?;
            left.parameter.try_cmp_with_work(&right.parameter, work)
        } else {
            Ok(root_order)
        }
    })?;

    let mut unique = Vec::with_capacity(ordered.len());
    for occurrence in ordered {
        work.charge(1)?;
        let duplicate = if let Some(previous) = unique.last() {
            work.charge(1)?;
            let previous = request_at(rows, *previous)?;
            let current = request_at(rows, occurrence)?;
            previous.root == current.root
                && previous
                    .parameter
                    .try_cmp_with_work(&current.parameter, work)?
                    .is_eq()
        } else {
            false
        };
        if !duplicate {
            unique.push(occurrence);
        }
    }
    try_sort_by_with_work(&mut unique, work, |left, right, work| {
        work.charge(1)?;
        Ok(left
            .row
            .cmp(&right.row)
            .then_with(|| left.index.cmp(&right.index)))
    })?;
    Ok(unique)
}

pub(crate) struct TraceRowsBuilder {
    offsets: Vec<u32>,
    ranges: Vec<RootRange>,
    sample_offsets: Vec<u32>,
    sample_ranges: Vec<RootRange>,
}

impl TraceRowsBuilder {
    pub(crate) fn new() -> Self {
        Self {
            offsets: vec![0],
            ranges: Vec::new(),
            sample_offsets: vec![0],
            sample_ranges: Vec::new(),
        }
    }

    pub(crate) fn push_row(&mut self, ranges: impl IntoIterator<Item = RootRange>) -> Result<()> {
        self.ranges.extend(ranges);
        self.offsets
            .push(u32::try_from(self.ranges.len()).map_err(|_| ResourceError::SizeOverflow)?);
        Ok(())
    }

    pub(crate) fn push_sample_row(
        &mut self,
        ranges: impl IntoIterator<Item = RootRange>,
    ) -> Result<()> {
        self.sample_ranges.extend(ranges);
        self.sample_offsets.push(
            u32::try_from(self.sample_ranges.len()).map_err(|_| ResourceError::SizeOverflow)?,
        );
        Ok(())
    }

    pub(crate) fn push_composed_row(
        &mut self,
        parents: &[RootRange],
        local_start: &ExactRational,
        local_end: &ExactRational,
        work: &mut impl Work,
    ) -> Result<()> {
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
        work.charge(u64::try_from(total).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut sample_offsets = self.sample_offsets;
        for offset in &mut sample_offsets {
            *offset = offset
                .checked_add(edge_range_count)
                .ok_or(ResourceError::SizeOverflow)?;
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

    use super::*;
    use crate::geometry::Unmetered;

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
        let mut rows = TraceRowsBuilder::new();
        rows.push_row([RootRange::whole(first), RootRange::whole(second)])
            .unwrap();
        let mut work = Unmetered;
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
        let mut rows = TraceRowsBuilder::new();
        rows.push_row([
            RootRange::whole(first),
            RootRange::whole(second),
            RootRange::whole(first),
        ])
        .unwrap();
        let mut work = Unmetered;
        let rows = rows.finish(&mut work).unwrap();
        let requests = rows
            .requests_at(0, &ExactRational::from_f64(0.5), &mut work)
            .unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].root(), first);
        assert_eq!(requests[1].root(), second);
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
        let mut rows = TraceRowsBuilder::new();
        rows.push_row([
            RootRange::point(root, start.clone(), ExactRational::zero()).unwrap(),
            RootRange::point(root, end.clone(), ExactRational::one()).unwrap(),
        ])
        .unwrap();
        let mut work = Unmetered;
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

        let mut left = TraceRowsBuilder::new();
        left.push_composed_row(
            std::slice::from_ref(&owner),
            &ExactRational::zero(),
            &half,
            &mut work,
        )
        .unwrap();
        let left = left.finish(&mut work).unwrap();
        let mut right = TraceRowsBuilder::new();
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

        let mut left = TraceRowsBuilder::new();
        left.push_composed_row(std::slice::from_ref(&owner), &zero, &quarter, &mut work)
            .unwrap();
        let left = left.finish(&mut work).unwrap();
        let mut middle = TraceRowsBuilder::new();
        middle
            .push_composed_row(std::slice::from_ref(&owner), &quarter, &half, &mut work)
            .unwrap();
        let middle = middle.finish(&mut work).unwrap();
        let mut right = TraceRowsBuilder::new();
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
        let mut rows = TraceRowsBuilder::new();
        rows.push_row([left.clone(), right.clone()]).unwrap();
        let mut work = Unmetered;
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
        let mut rows = TraceRowsBuilder::new();
        let parents = [
            RootRange::whole(RootId::from_index(1)),
            RootRange::whole(RootId::from_index(2)),
        ];
        let mut work = Fuel(1);

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
        let mut rows = TraceRowsBuilder::new();
        rows.push_row([
            RootRange::whole(RootId::from_index(1)),
            RootRange::whole(RootId::from_index(2)),
        ])
        .unwrap();
        let mut work = Fuel(1);

        assert!(rows.finish(&mut work).is_err());
    }
}
