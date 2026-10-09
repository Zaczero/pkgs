use std::sync::OnceLock;

use super::operation::MetricOp;
use crate::DistanceUnit;
use crate::broadcast::{resolve_metric, resolve_metric_at};
use crate::crs::frame::{Crs, Frame};
use crate::crs::{self, MetricModel, ResolvedMetric};
use crate::geometry::TopologyDomain;
#[cfg(test)]
use crate::test_support::binary_dispatch_counters::metric_context_acquisition;

/// CRS/metric resolution policy for a dispatched operation.
#[derive(Debug, Clone, Copy)]
pub(crate) enum MetricResolver {
    /// Pure topology, IO, or coordinate-space ops — no CRS metric model.
    None,
    /// Antimeridian ops allow CRS-free lon/lat but reject projected CRS. They
    /// still need the CRS metric model resolved once per call so row kernels do
    /// not re-derive it for every geometry.
    Antimeridian,
    /// Distance/area/length/buffer, linear referencing, and other CRS-aware
    /// measure/constructive ops.
    Metric {
        op: MetricOp,
        unit: Option<DistanceUnit>,
    },
    /// 3D Euclidean measures (`distance_3d`/`length_3d`). Same unit policy as
    /// [`Self::Metric`], but a geographic CRS is rejected rather than measured
    /// geodesically — see [`crate::broadcast::resolve_metric_3d`].
    Metric3d {
        op: MetricOp,
        unit: Option<DistanceUnit>,
    },
    /// Geographic topology borrows its CRS model and resolves the ellipsoid
    /// only after an attributed interior source arc is admitted.
    GeographicCarry,
}

impl MetricResolver {
    /// Attach a call-site ``unit=`` override to a resolver template from
    /// [`crate::dispatch::Operation::resolver`].
    pub(crate) const fn with_unit(self, unit: Option<DistanceUnit>) -> Self {
        match self {
            Self::Metric { op, .. } => Self::Metric { op, unit },
            Self::Metric3d { op, .. } => Self::Metric3d { op, unit },
            Self::Antimeridian => Self::Antimeridian,
            Self::GeographicCarry => Self::GeographicCarry,
            other @ Self::None => other,
        }
    }

    /// Resolve the whole metric context in one pass, so kernels cannot see an
    /// impossible model/metric pairing.
    pub(crate) fn resolve_ctx<'a>(
        self,
        frame: &Frame,
        scratch: &'a mut MetricScratch,
    ) -> pyo3::PyResult<MetricCtx<'a>> {
        if matches!(self, Self::GeographicCarry) {
            return self
                .resolve_topology_ctx(frame, scratch)
                .map(|(_, metric)| metric);
        }
        scratch.clear();
        self.resolve_ctx_in_scratch(frame, scratch, None)
    }

    fn resolve_ctx_in_scratch<'a>(
        self,
        frame: &Frame,
        scratch: &'a mut MetricScratch,
        state: Option<&crs::RuntimeState>,
    ) -> pyo3::PyResult<MetricCtx<'a>> {
        let model = match self {
            Self::None => return Ok(MetricCtx::None),
            Self::Antimeridian => {
                #[cfg(test)]
                metric_context_acquisition();
                let model = crs::metric_model(frame.crs_str())?;
                let model = scratch.model.get_or_init(|| model);
                return Ok(MetricCtx::Antimeridian(model));
            },
            Self::GeographicCarry => {
                let (_, metric) = self.resolve_topology_ctx(frame, scratch)?;
                return Ok(metric);
            },
            Self::Metric { op, unit } => match state {
                Some(state) => resolve_metric_at(frame.crs_str(), unit, op, state)?,
                None => resolve_metric(frame.crs_str(), unit, op)?,
            },
            Self::Metric3d { op, unit } => {
                crate::broadcast::resolve_metric_3d(frame.crs_str(), unit, op)?
            },
        };
        let metric = match crs::ResolvedMetric::from_model(&model) {
            Ok(metric) => metric,
            Err(error) => {
                drop(model);
                return Err(error.into());
            },
        };
        let model = scratch.model.get_or_init(|| model);
        let metric = scratch.metric.get_or_init(|| metric);
        Ok(MetricCtx::Metric {
            model,
            metric,
            scratch,
        })
    }

    pub(crate) fn resolve_topology_ctx<'a>(
        self,
        frame: &Frame,
        scratch: &'a mut MetricScratch,
    ) -> pyo3::PyResult<(TopologyDomain, MetricCtx<'a>)> {
        if matches!(self, Self::GeographicCarry) {
            scratch.clear();
            if frame.crs_str().is_none() {
                return Ok((TopologyDomain::Cartesian, MetricCtx::None));
            }
            // The WGS84 topology shortcut does not pin its PROJ ellipsoid.
            let state = crs::runtime_state_snapshot()?;
            let domain = crate::geometry::topology_domain_at(frame, Some(&state))?;
            if !domain.is_geographic() {
                return Ok((domain, MetricCtx::None));
            }
            scratch.crs = frame.crs_owned();
            scratch.state = Some(state);
            return Ok((domain, MetricCtx::GeographicCarry { scratch }));
        }
        if self.carries_native_source_samples() {
            scratch.clear();
            // Domain admission and a later source-chart lookup share this snapshot.
            let state = frame
                .crs_str()
                .map(|_| crs::runtime_state_snapshot())
                .transpose()?;
            let domain = crate::geometry::topology_domain_at(frame, state.as_deref())?;
            if matches!(self, Self::Metric {
                unit: Some(DistanceUnit::Planar),
                ..
            }) && domain.is_geographic()
            {
                scratch.state.clone_from(&state);
                scratch.source_metric = Some(OnceLock::new());
            }
            let metric = self.resolve_ctx_in_scratch(frame, scratch, state.as_deref())?;
            return Ok((domain, metric));
        }
        let domain = crate::geometry::topology_domain(frame)?;
        Ok((domain, self.resolve_ctx(frame, scratch)?))
    }

    const fn carries_native_source_samples(self) -> bool {
        matches!(self, Self::Metric {
            op: MetricOp::ConcaveHull | MetricOp::OffsetCurve,
            ..
        })
    }
}

#[derive(Default)]
pub(crate) struct MetricScratch {
    model: OnceLock<MetricModel>,
    metric: OnceLock<ResolvedMetric>,
    crs: Option<Crs>,
    state: Option<std::sync::Arc<crate::crs::RuntimeState>>,
    source_metric: Option<OnceLock<crate::crs::GeodesicMetric>>,
}

impl MetricScratch {
    fn clear(&mut self) {
        self.metric.take();
        self.model.take();
        self.crs = None;
        self.state = None;
        self.source_metric = None;
    }

    fn metric_model(&self) -> crate::error::Result<&MetricModel> {
        self.model
            .get_or_try_init(|| match (self.crs.as_deref(), self.state.as_deref()) {
                (Some(crs), state) => crs::metric_model_at(
                    Some(crs),
                    state.expect("CRS-bearing GeographicCarry retains its operation snapshot"),
                ),
                (None, _) => Ok(MetricModel::COORDINATE),
            })
    }

    fn resolved_metric(&self) -> crate::error::Result<&ResolvedMetric> {
        let model = self.metric_model()?;
        self.metric
            .get_or_try_init(|| ResolvedMetric::from_model(model))
    }

    fn source_chart_metric(
        &self,
        source_crs: &str,
    ) -> crate::error::Result<&crate::crs::GeodesicMetric> {
        let source_metric = self
            .source_metric
            .as_ref()
            .expect("source metric access follows planar/geographic admission");
        let state = self
            .state
            .as_deref()
            .expect("source metric admission retains its operation runtime snapshot");
        source_metric.get_or_try_init(|| {
            let model = crs::metric_model_at(Some(source_crs), state)?;
            crate::crs::GeodesicMetric::from_model(&model)
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) enum MetricCtx<'a> {
    None,
    Antimeridian(&'a MetricModel),
    GeographicCarry {
        scratch: &'a MetricScratch,
    },
    Metric {
        model: &'a MetricModel,
        metric: &'a ResolvedMetric,
        scratch: &'a MetricScratch,
    },
}

impl<'a> MetricCtx<'a> {
    pub(crate) fn model(self) -> Option<&'a MetricModel> {
        match self {
            Self::None => None,
            Self::Antimeridian(model) | Self::Metric { model, .. } => Some(model),
            Self::GeographicCarry { scratch } => scratch.model.get(),
        }
    }

    pub(crate) fn resolved(self) -> Option<&'a ResolvedMetric> {
        match self {
            Self::Metric { metric, .. } => Some(metric),
            Self::GeographicCarry { scratch } => scratch.metric.get(),
            Self::None | Self::Antimeridian(_) => None,
        }
    }

    /// The metric model a metric-resolved kernel is entitled to — the ONE
    /// spelling of the "registry said this op resolves a metric" invariant
    /// (14 kernels used to hand-roll this match with per-op error strings).
    pub(crate) fn require_model(self, op: MetricOp) -> crate::error::Result<&'a MetricModel> {
        match self {
            Self::Metric { model, .. } => Ok(model),
            Self::GeographicCarry { scratch } => scratch.metric_model(),
            Self::None | Self::Antimeridian(_) => Err(crate::crs::CrsError::invalid(format!(
                "internal dispatch error: {} missing metric context",
                op.name()
            ))),
        }
    }

    /// The resolved metric a metric-resolved kernel is entitled to; the
    /// measure-kernel sibling of [`Self::require_model`].
    pub(crate) fn require_resolved(self, op: MetricOp) -> crate::error::Result<&'a ResolvedMetric> {
        match self {
            Self::Metric { metric, .. } => Ok(metric),
            Self::GeographicCarry { scratch } => scratch.resolved_metric(),
            Self::None | Self::Antimeridian(_) => Err(crate::crs::CrsError::invalid(format!(
                "internal dispatch error: {} missing metric context",
                op.name()
            ))),
        }
    }

    pub(crate) fn geographic_chart_metric(
        self,
    ) -> crate::error::Result<Option<&'a crate::crs::GeodesicMetric>> {
        let resolved = match self {
            Self::Metric { metric, .. } => metric,
            Self::GeographicCarry { scratch } => scratch.resolved_metric()?,
            Self::None | Self::Antimeridian(_) => return Ok(None),
        };
        Ok(match resolved {
            ResolvedMetric::Geodesic(metric) => Some(metric),
            ResolvedMetric::Planar { .. } => None,
        })
    }

    /// Resolve the source-chart ellipsoid only for an admitted planar Native
    /// producer; geodesic operations reuse their already-resolved metric.
    pub(crate) fn source_chart_metric(
        self,
        source_crs: Option<&str>,
    ) -> crate::error::Result<Option<&'a crate::crs::GeodesicMetric>> {
        match self {
            Self::Metric {
                metric: ResolvedMetric::Geodesic(metric),
                ..
            } => Ok(Some(metric)),
            Self::Metric {
                metric: ResolvedMetric::Planar { .. },
                scratch,
                ..
            } => {
                let Some(source_crs) = source_crs else {
                    return Ok(None);
                };
                if scratch.source_metric.is_none() {
                    return Ok(None);
                }
                scratch.source_chart_metric(source_crs).map(Some)
            },
            Self::GeographicCarry { .. } => self.geographic_chart_metric(),
            Self::None | Self::Antimeridian(_) => Ok(None),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Lane {
    Scalar,
    Array(usize),
}

impl Lane {
    pub(crate) const fn row(self) -> usize {
        match self {
            Self::Scalar => 0,
            Self::Array(row) => row,
        }
    }

    pub(crate) const fn is_array(self) -> bool {
        matches!(self, Self::Array(_))
    }
}

/// A unary kernel producing one `f64` per geometry — the shape every
/// CRS-aware measure (`area`, `length`, `length_3d`) shares.
pub(crate) type UnaryMeasureKernel =
    fn(&crate::geometry::PreparedPlan, &CachelessRowContext<'_>) -> crate::error::Result<f64>;

/// Per-row context for operations that cannot access frame-dependent products.
pub(crate) struct CachelessRowContext<'a> {
    pub frame: &'a Frame,
    pub domain: TopologyDomain,
    pub metric: MetricCtx<'a>,
    pub lane: Lane,
}

pub(crate) struct LinearReferenceRow<'a> {
    pub op: CachelessRowContext<'a>,
    pub model: &'a MetricModel,
    pub cache: &'a crate::geometry::FrameDependentCaches,
}

pub(crate) struct GeodesicBinaryRow<'a> {
    pub metric: &'a crate::crs::GeodesicMetric,
    pub left_cache: &'a crate::geometry::FrameDependentCaches,
    pub right_cache: &'a crate::geometry::FrameDependentCaches,
}

#[cfg(test)]
mod tests {
    use super::{MetricCtx, MetricResolver, MetricScratch};
    use crate::DistanceUnit;
    use crate::crs::frame::Frame;
    use crate::crs::{MetricModel, ResolvedMetric};
    use crate::dispatch::MetricOp;
    use crate::geometry::TopologyDomain;
    use crate::test_support::binary_dispatch_counters;

    const NON_WGS84_CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";

    fn test_frame(crs: Option<&str>) -> Frame {
        crs.map_or(Frame::None, |crs| Frame::Crs(crs.into()))
    }

    fn planar_offset_resolver() -> MetricResolver {
        MetricResolver::Metric {
            op: MetricOp::OffsetCurve,
            unit: Some(DistanceUnit::Planar),
        }
    }

    #[test]
    fn planar_geographic_source_metric_is_lazy_and_uses_admission_snapshot() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        binary_dispatch_counters::reset();
        let frame = test_frame(Some(NON_WGS84_CRS));
        let mut scratch = MetricScratch::default();
        let (domain, metric) = planar_offset_resolver()
            .resolve_topology_ctx(&frame, &mut scratch)
            .unwrap();
        let MetricCtx::Metric {
            scratch: context_scratch,
            ..
        } = metric
        else {
            panic!("planar offset receives its admitted metric context");
        };

        assert!(matches!(domain, TopologyDomain::Geographic(_)));
        assert!(matches!(
            metric.model(),
            Some(MetricModel::Planar { to_metre }) if to_metre.get().to_bits() == 1.0_f64.to_bits()
        ));
        assert!(matches!(
            metric.resolved(),
            Some(ResolvedMetric::Planar { to_metre }) if to_metre.get().to_bits() == 1.0_f64.to_bits()
        ));
        assert!(
            context_scratch
                .source_metric
                .as_ref()
                .is_some_and(|slot| slot.get().is_none())
        );
        let state = context_scratch
            .state
            .as_deref()
            .expect("geographic source snapshot");
        let generation = state.generation;
        assert_eq!(
            binary_dispatch_counters::snapshot().runtime_state_snapshots,
            1
        );

        let source_metric = metric
            .source_chart_metric(frame.crs_str())
            .unwrap()
            .expect("eligible Native source metric");
        assert_eq!(source_metric.geodesic.a, 7_000_000.0);
        assert_eq!(source_metric.geodesic.f, 1.0 / 150.0);
        assert_eq!(source_metric.state.generation, generation);
        assert!(matches!(
            metric.model(),
            Some(MetricModel::Planar { to_metre }) if to_metre.get().to_bits() == 1.0_f64.to_bits()
        ));
        assert!(matches!(
            metric.resolved(),
            Some(ResolvedMetric::Planar { to_metre }) if to_metre.get().to_bits() == 1.0_f64.to_bits()
        ));
        assert_eq!(
            binary_dispatch_counters::snapshot().runtime_state_snapshots,
            1
        );
    }

    #[test]
    fn geodesic_operation_reuses_its_resolved_metric_for_source_carry() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        binary_dispatch_counters::reset();
        let frame = test_frame(Some(NON_WGS84_CRS));
        let mut scratch = MetricScratch::default();
        let (domain, metric) = MetricResolver::Metric {
            op: MetricOp::OffsetCurve,
            unit: None,
        }
        .resolve_topology_ctx(&frame, &mut scratch)
        .unwrap();
        let MetricCtx::Metric {
            scratch: context_scratch,
            ..
        } = metric
        else {
            panic!("geographic offset receives its resolved metric context");
        };

        assert!(matches!(domain, TopologyDomain::Geographic(_)));
        let Some(ResolvedMetric::Geodesic(operation_metric)) = metric.resolved() else {
            panic!("geographic operation resolves its geodesic metric");
        };
        let source_metric = metric
            .source_chart_metric(frame.crs_str())
            .unwrap()
            .expect("source carry reuses the operation metric");

        assert!(std::ptr::eq(operation_metric, source_metric));
        assert!(context_scratch.source_metric.is_none());
        assert_eq!(
            binary_dispatch_counters::snapshot().runtime_state_snapshots,
            1
        );
    }

    #[test]
    fn planar_source_metric_is_absent_without_a_geographic_domain() {
        binary_dispatch_counters::reset();
        let frame = test_frame(None);
        let mut scratch = MetricScratch::default();
        let (domain, metric) = planar_offset_resolver()
            .resolve_topology_ctx(&frame, &mut scratch)
            .unwrap();
        let MetricCtx::Metric {
            scratch: context_scratch,
            ..
        } = metric
        else {
            panic!("CRS-free offset receives its planar metric context");
        };

        assert!(matches!(domain, TopologyDomain::Cartesian));
        assert!(context_scratch.source_metric.is_none());
        assert!(
            metric
                .source_chart_metric(frame.crs_str())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            binary_dispatch_counters::snapshot().runtime_state_snapshots,
            0
        );
    }

    #[test]
    fn projected_planar_source_metric_is_not_admitted() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        binary_dispatch_counters::reset();
        let frame = test_frame(Some("EPSG:3857"));
        let mut scratch = MetricScratch::default();
        let (domain, metric) = planar_offset_resolver()
            .resolve_topology_ctx(&frame, &mut scratch)
            .unwrap();
        let MetricCtx::Metric {
            scratch: context_scratch,
            ..
        } = metric
        else {
            panic!("projected offset receives its planar metric context");
        };

        assert!(matches!(domain, TopologyDomain::Cartesian));
        assert!(context_scratch.source_metric.is_none());
        assert!(
            metric
                .source_chart_metric(frame.crs_str())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            binary_dispatch_counters::snapshot().runtime_state_snapshots,
            1
        );
    }

    #[test]
    fn wgs84_geographic_carry_captures_runtime_before_lazy_metric_resolution() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        for code in ["EPSG:4326", "EPSG:4979"] {
            binary_dispatch_counters::reset();
            let frame = test_frame(Some(code));
            let mut scratch = MetricScratch::default();
            let (domain, metric) = MetricResolver::GeographicCarry
                .resolve_topology_ctx(&frame, &mut scratch)
                .unwrap();
            let MetricCtx::GeographicCarry {
                scratch: context_scratch,
            } = metric
            else {
                panic!("WGS84 carry retains its operation scratch");
            };

            assert!(matches!(domain, TopologyDomain::Geographic(_)), "{code}");
            assert!(context_scratch.state.is_some(), "{code}");
            assert!(context_scratch.model.get().is_none(), "{code}");
            assert!(context_scratch.metric.get().is_none(), "{code}");
            assert!(context_scratch.source_metric.is_none(), "{code}");
            let generation = context_scratch.state.as_ref().unwrap().generation;
            assert_eq!(
                binary_dispatch_counters::snapshot().runtime_state_snapshots,
                1,
                "{code}"
            );

            let source_metric = metric
                .geographic_chart_metric()
                .unwrap()
                .expect("geographic carry metric");
            assert_eq!(source_metric.state.generation, generation, "{code}");
            assert_eq!(
                binary_dispatch_counters::snapshot().runtime_state_snapshots,
                1,
                "{code}"
            );
        }
    }
}
