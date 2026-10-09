use std::sync::OnceLock;

use super::operation::MetricOp;
use crate::DistanceUnit;
use crate::broadcast::resolve_metric;
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
        scratch.clear();
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
            Self::Metric { op, unit } => resolve_metric(frame.crs_str(), unit, op)?,
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
        Ok(MetricCtx::Metric { model, metric })
    }

    pub(crate) fn resolve_topology_ctx<'a>(
        self,
        frame: &Frame,
        scratch: &'a mut MetricScratch,
    ) -> pyo3::PyResult<(TopologyDomain, MetricCtx<'a>)> {
        if matches!(self, Self::GeographicCarry) {
            scratch.clear();
            return match frame.crs_str() {
                None => Ok((TopologyDomain::Cartesian, MetricCtx::None)),
                Some(crs) if crs::is_wgs84_lonlat(crs) => Ok((
                    {
                        scratch.crs = frame.crs_owned();
                        TopologyDomain::Geographic(crate::geographic::GeographicFrame::crs84())
                    },
                    MetricCtx::GeographicCarry { scratch },
                )),
                Some(_) => {
                    let state = crs::runtime_state_snapshot()?;
                    let domain = crate::geometry::topology_domain_at(frame, Some(&state))?;
                    if !domain.is_geographic() {
                        return Ok((domain, MetricCtx::None));
                    }
                    scratch.crs = frame.crs_owned();
                    scratch.state = Some(state);
                    Ok((domain, MetricCtx::GeographicCarry { scratch }))
                },
            };
        }
        let domain = crate::geometry::topology_domain(frame)?;
        Ok((domain, self.resolve_ctx(frame, scratch)?))
    }
}

#[derive(Default)]
pub(crate) struct MetricScratch {
    model: OnceLock<MetricModel>,
    metric: OnceLock<ResolvedMetric>,
    crs: Option<Crs>,
    state: Option<std::sync::Arc<crate::crs::RuntimeState>>,
}

impl MetricScratch {
    fn clear(&mut self) {
        self.metric.take();
        self.model.take();
        self.crs = None;
        self.state = None;
    }

    fn metric_model(&self) -> crate::error::Result<&MetricModel> {
        self.model
            .get_or_try_init(|| match (self.crs.as_deref(), self.state.as_deref()) {
                (Some(crs), Some(state)) => crs::metric_model_at(Some(crs), state),
                (Some(crs), None) => crs::metric_model(Some(crs)),
                (None, _) => Ok(MetricModel::COORDINATE),
            })
    }

    fn resolved_metric(&self) -> crate::error::Result<&ResolvedMetric> {
        let model = self.metric_model()?;
        self.metric
            .get_or_try_init(|| ResolvedMetric::from_model(model))
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
