use super::{
    Bounds, Crs, Frame, GeodesicBoxPruner, GeodesicIndexPruner, GeodesicPruner, Point,
    PreparedPlan, PyGeometry, PySpatialIndex, TopologyDomain, crs, index_envelopes,
    shape_pruner_point,
};
use crate::FrameError;
use crate::error::Result;

impl PySpatialIndex {
    /// Reject a query geometry whose CRS/epoch is operationally incompatible
    /// with the indexed frame. Axis-order-only differences (EPSG:4326 ↔
    /// OGC:CRS84) are accepted — query refinement compares coordinates in the
    /// shared lon/lat storage model. An empty index (`None`) accepts any query.
    pub(crate) fn ensure_query_compatible(
        &self,
        geometry: &PyGeometry,
        operation: &str,
    ) -> Result<()> {
        self.ensure_frame_compatible(geometry.crs_ref(), geometry.epoch(), operation)
    }

    /// Frame check for every index lane — query, nearest, candidates, join,
    /// and insert alike. Bulk lanes check ONCE per array instead of once per
    /// row.
    ///
    /// Insert used to demand exact string identity on the grounds that an
    /// indexed geometry is later returned carrying the index's single label,
    /// so admitting a differently-spelled CRS would relabel it. It does
    /// relabel it — but only ever between spellings of the *same* frame, since
    /// [`crs_operationally_equal`] rejects every genuine difference. Holding
    /// insert stricter than query just meant an index could answer questions
    /// about a geometry it refused to store.
    pub(crate) fn ensure_frame_compatible(
        &self,
        query_crs: Option<&Crs>,
        query_epoch: Option<f64>,
        operation: &str,
    ) -> Result<()> {
        let Some(frame) = &self.metadata else {
            return Ok(());
        };
        let crs_ok = match (frame.crs_ref(), query_crs) {
            (None, None) => true,
            (Some(index), Some(query)) => crate::crs_operationally_equal(index, query)?,
            _ => false,
        };
        if !crs_ok {
            return Err(FrameError::CrsMismatch {
                operation: operation.into(),
                left: frame.crs_ref().cloned(),
                right: query_crs.cloned(),
            }
            .into());
        }
        if query_epoch != frame.epoch() {
            return Err(FrameError::EpochMismatch {
                operation: operation.into(),
                left: frame.epoch(),
                right: query_epoch,
            }
            .into());
        }
        Ok(())
    }

    /// CRS string for metric resolution after a successful frame check.
    ///
    /// Index metadata wins when present (frames already match). An empty
    /// index has no metadata and accepts any query CRS, so fall through to
    /// the caller's string. Shared by every distance-bearing index surface
    /// (`query`/`candidates`/`nearest`/`join`/`self_join`).
    pub(crate) fn metric_crs_str<'a>(&'a self, query_crs: Option<&'a str>) -> Option<&'a str> {
        self.metadata
            .as_ref()
            .and_then(Frame::crs_str)
            .or(query_crs)
    }

    pub(crate) fn geodesic_pruner(
        &self,
        metric: &crs::MetricModel,
        query: Option<Point>,
    ) -> Option<GeodesicPruner> {
        let crs::MetricModel::Geodesic { crs, frame, state } = metric else {
            return None;
        };
        if *frame != crate::geographic::GeographicFrame::crs84() {
            return None;
        }
        if self.non_prunable_live != 0 {
            return None;
        }
        GeodesicPruner::new_at(crs, query?, state)
    }

    pub(crate) fn geodesic_index_pruner(
        &self,
        metric: &crs::MetricModel,
        resolved: &crs::GeodesicMetric,
        query: &PreparedPlan,
        query_bounds: Bounds,
    ) -> Result<Option<GeodesicIndexPruner>> {
        if let Some(point) = shape_pruner_point(query.shape()) {
            return Ok(self
                .geodesic_pruner(metric, Some(point))
                .map(GeodesicIndexPruner::Point));
        }
        crs::ensure_geographic_domain_in_frame(resolved.frame, query.shape())?;
        if resolved.frame != crate::geographic::GeographicFrame::crs84()
            || self.non_prunable_live != 0
        {
            return Ok(None);
        }
        let cover = index_envelopes(
            query.shape(),
            query_bounds,
            TopologyDomain::Geographic(resolved.frame),
        )?;
        Ok(Some(GeodesicIndexPruner::Boxes(GeodesicBoxPruner::new(
            resolved.geodesic.a,
            resolved.geodesic.f,
            cover.entries(),
        ))))
    }

    /// Whether this index's shared frame is geographic. All rows and the query
    /// share this frame (`ensure_query_compatible`), so it is an index-level
    /// property used by the quotient-aware relation path.
    pub(crate) fn domain(&self) -> Result<TopologyDomain> {
        self.metadata
            .as_ref()
            .map(crate::geometry::topology_domain)
            .transpose()
            .map(|domain| domain.unwrap_or(TopologyDomain::Cartesian))
    }
}

pub(super) fn admitted_query<'a>(
    slot: &mut Option<crate::geometry::GeographicPrepared<'a>>,
    plan: &'a PreparedPlan,
    cache: &'a crate::geometry::FrameDependentCaches,
    frame: crate::geographic::GeographicFrame,
) -> crate::error::Result<crate::geometry::GeographicPrepared<'a>> {
    if let Some(prepared) = *slot {
        return Ok(prepared);
    }
    let prepared = crate::geometry::GeographicPrepared::try_new(plan, cache, frame)?;
    *slot = Some(prepared);
    Ok(prepared)
}
