use super::*;

/// Sidecar for geodesic products keyed by ellipsoid and PROJ runtime generation.
#[derive(Default)]
pub(crate) struct FrameDependentCaches {
    entry: Mutex<Option<(GeographicMetricKey, FrameDependentEntry)>>,
    /// Admission facts for one geographic frame.
    admission: Mutex<
        Option<(
            crate::geographic::GeographicFrame,
            bool,
            GeographicTopologyRoute,
        )>,
    >,
}

impl fmt::Debug for FrameDependentCaches {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrameDependentCaches")
            .finish_non_exhaustive()
    }
}

impl HeapSize for FrameDependentCaches {
    fn heap_bytes(&self) -> usize {
        self.entry.heap_bytes()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct GeographicTopologyKey {
    longitude_unit: u64,
    longitude_period: u64,
    longitude_east: bool,
    latitude_unit: u64,
    latitude_pole: u64,
    latitude_north: bool,
    prime_meridian: u64,
}

impl GeographicTopologyKey {
    fn new(frame: crate::geographic::GeographicFrame) -> Self {
        Self {
            longitude_unit: canonical_f64_bits(frame.longitude().radians_per_unit()),
            longitude_period: canonical_f64_bits(frame.longitude().period()),
            longitude_east: frame.orientation().longitude_east(),
            latitude_unit: canonical_f64_bits(frame.latitude_radians_per_unit()),
            latitude_pole: canonical_f64_bits(frame.pole()),
            latitude_north: frame.orientation().latitude_north(),
            prime_meridian: canonical_f64_bits(frame.prime_meridian_radians()),
        }
    }
}

#[derive(Default)]
pub(super) struct GeographicTopologyProducts {
    pub(super) route: OnceLock<GeographicTopologyRoute>,
    pub(super) exact: OnceLock<Arc<ExactGeographicTopology>>,
    pub(super) diagnostic_exact: OnceLock<Arc<ExactGeographicTopology>>,
    pub(super) validity: OnceLock<Option<Box<ValidationIssue>>>,
}

#[derive(Clone, Default)]
struct FrameDependentEntry {
    line_index: Option<Arc<ChartLineIndex>>,
    chart_parts: Option<Arc<ChartParts>>,
    measured_chart_parts: Option<Arc<ChartParts>>,
}

impl FrameDependentCaches {
    /// Whether the raw box is sound in `frame`, once the coordinates have
    /// been admitted there; `None` before that admission.
    pub(crate) fn geographic_admission(
        &self,
        frame: crate::geographic::GeographicFrame,
    ) -> Option<(bool, GeographicTopologyRoute)> {
        self.admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .filter(|&(admitted, ..)| admitted == frame)
            .map(|(_, bounds_sound, route)| (bounds_sound, route))
    }

    pub(crate) fn record_admission(
        &self,
        frame: crate::geographic::GeographicFrame,
        bounds_sound: bool,
        route: GeographicTopologyRoute,
    ) {
        *self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some((frame, bounds_sound, route));
    }

    fn get(&self, key: &GeographicMetricKey) -> Option<FrameDependentEntry> {
        self.entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .filter(|(candidate, _)| candidate == key)
            .map(|(_, value)| value.clone())
    }

    fn merge(
        &self,
        key: GeographicMetricKey,
        update: impl FnOnce(&mut FrameDependentEntry),
    ) -> Option<FrameDependentEntry> {
        let mut slot = self
            .entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((current, _)) = slot.as_ref()
            && current.same_frame_parameters(&key)
            && current.runtime_generation() > key.runtime_generation()
        {
            return None;
        }
        let mut value = slot
            .take()
            .filter(|(candidate, _)| candidate == &key)
            .map_or_else(FrameDependentEntry::default, |(_, value)| value);
        update(&mut value);
        *slot = Some((key, value.clone()));
        drop(slot);
        Some(value)
    }

    pub(in crate::geometry) fn cached_chart_parts(
        &self,
        key: &GeographicMetricKey,
    ) -> Option<Arc<ChartParts>> {
        self.get(key).and_then(|entry| entry.chart_parts)
    }

    pub(in crate::geometry) fn merge_chart_parts(
        &self,
        key: &GeographicMetricKey,
        parts: &Arc<ChartParts>,
    ) -> Option<Arc<ChartParts>> {
        self.merge(key.clone(), |entry| {
            entry.chart_parts.get_or_insert_with(|| Arc::clone(parts));
        })
        .and_then(|entry| entry.chart_parts)
    }
}

impl HeapSize for FrameDependentEntry {
    fn heap_bytes(&self) -> usize {
        let shared_chart_parts = self
            .line_index
            .as_ref()
            .zip(self.chart_parts.as_ref())
            .is_some_and(|(index, parts)| Arc::ptr_eq(index.parts(), parts));
        self.line_index
            .heap_bytes()
            .saturating_add(if shared_chart_parts {
                0
            } else {
                self.chart_parts.heap_bytes()
            })
            .saturating_add(self.measured_chart_parts.heap_bytes())
    }
}

impl HeapSize for GeographicTopologyProducts {
    fn heap_bytes(&self) -> usize {
        let exact = self.exact.heap_bytes();
        let diagnostic = self.diagnostic_exact.get().map_or(0, |diagnostic| {
            if self
                .exact
                .get()
                .is_some_and(|exact| Arc::ptr_eq(exact, diagnostic))
            {
                0
            } else {
                diagnostic.heap_bytes()
            }
        });
        exact
            .saturating_add(diagnostic)
            .saturating_add(self.validity.heap_bytes())
    }
}

#[cfg(test)]
mod tests;
