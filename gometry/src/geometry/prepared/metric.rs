use super::{
    Arc, ChartLineIndex, ChartMetric, ChartParts, DistanceParts, FrameDependentCaches,
    GeographicMetricKey, GeographicPrepared, HeapSize, LineIndex, LineIndexSlot, MeasuredLineIndex,
    MeasuredLineIndexSlot, OnceLock, PlanarMetric, PreparedPlan, Result,
};

/// Shape-derived metric products shared by persistent and transient plans.
#[derive(Default)]
pub(super) struct MetricPlan {
    distance: OnceLock<Box<DistanceParts>>,
    distance_3d: OnceLock<Box<crate::geometry::Distance3dParts>>,
    line_index: OnceLock<Box<LineIndexSlot>>,
    measured_line_index: OnceLock<Box<MeasuredLineIndexSlot>>,
}

impl PreparedPlan {
    pub(crate) fn distance_parts(&self) -> Result<&DistanceParts> {
        if let Some(parts) = self.metric.get().and_then(|metric| metric.distance.get()) {
            return Ok(parts.as_ref());
        }
        self.metric()
            .distance
            .get_or_try_init(|| {
                Ok::<_, crate::error::Error>(Box::new(self.shape().distance_parts()?))
            })
            .map(Box::as_ref)
    }

    pub(crate) fn distance_3d_parts(&self) -> &crate::geometry::Distance3dParts {
        if let Some(parts) = self
            .metric
            .get()
            .and_then(|metric| metric.distance_3d.get())
        {
            return parts.as_ref();
        }
        self.metric()
            .distance_3d
            .get_or_init(|| Box::new(crate::geometry::Distance3dParts::build(self.shape())))
            .as_ref()
    }

    pub(crate) fn line_index(&self) -> Result<&LineIndex> {
        self.metric()
            .line_index
            .get_or_try_init(|| {
                Ok::<_, crate::error::Error>(Box::new(LineIndexSlot::build(
                    self.shape(),
                    &PlanarMetric,
                )?))
            })?
            .get()
    }

    pub(crate) fn measured_line_index(&self) -> Result<&MeasuredLineIndex> {
        self.metric()
            .measured_line_index
            .get_or_try_init(|| {
                Ok::<_, crate::error::Error>(Box::new(MeasuredLineIndexSlot::build(self.shape())?))
            })?
            .get()
    }

    pub(crate) fn chart_line_index(
        &self,
        frame_cache: &FrameDependentCaches,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        runtime_generation: u64,
        metric: &impl ChartMetric,
    ) -> Result<Arc<ChartLineIndex>> {
        let key =
            GeographicMetricKey::new_at_generation(crs, semi_major, flattening, runtime_generation);
        let cached = frame_cache.get(&key).and_then(|entry| entry.line_index);
        if let Some(index) = cached {
            return Ok(index);
        }
        if frame_cache.cached_chart_parts(&key).is_none() {
            GeographicPrepared::try_new(self, frame_cache, metric.chart_frame())?;
        }
        self.shape().linework()?;
        let parts = self.cached_chart_parts(frame_cache, &key, metric)?;
        let index = Arc::new(ChartLineIndex::from_chart_parts(
            self.shape(),
            parts,
            metric,
        )?);
        let entry = frame_cache.merge(key, |entry| {
            entry.line_index.get_or_insert_with(|| Arc::clone(&index));
        });
        Ok(entry.and_then(|entry| entry.line_index).unwrap_or(index))
    }

    pub(crate) fn measured_chart_parts(
        &self,
        frame_cache: &FrameDependentCaches,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        generation: u64,
        metric: &impl ChartMetric,
    ) -> Result<Arc<ChartParts>> {
        let key = GeographicMetricKey::new_at_generation(crs, semi_major, flattening, generation);
        if let Some(parts) = frame_cache
            .get(&key)
            .and_then(|entry| entry.measured_chart_parts)
        {
            return Ok(parts);
        }
        GeographicPrepared::try_new(self, frame_cache, metric.chart_frame())?;
        let index = self.measured_line_index()?;
        let parts = Arc::new(ChartParts::measured_route(index.points(), metric)?);
        let entry = frame_cache.merge(key, |entry| {
            entry
                .measured_chart_parts
                .get_or_insert_with(|| Arc::clone(&parts));
        });
        Ok(entry
            .and_then(|entry| entry.measured_chart_parts)
            .unwrap_or(parts))
    }
}

impl HeapSize for MetricPlan {
    fn heap_bytes(&self) -> usize {
        self.distance
            .heap_bytes()
            .saturating_add(self.distance_3d.heap_bytes())
            .saturating_add(self.line_index.heap_bytes())
            .saturating_add(self.measured_line_index.heap_bytes())
    }
}

impl HeapSize for DistanceParts {
    fn heap_bytes(&self) -> usize {
        self.point_only
            .heap_bytes()
            .saturating_add(self.linework.heap_bytes())
            .saturating_add(self.facet_bvh.heap_bytes())
            .saturating_add(self.point_index.heap_bytes())
    }
}

#[cfg(test)]
mod tests;
