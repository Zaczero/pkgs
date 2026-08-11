use crate::DistanceUnit;
use crate::boundary::metadata::Frame;
use crate::broadcast::resolve_metric;
use crate::crs::{self, MetricModel, ResolvedMetric};

/// CRS/metric resolution policy for a dispatched operation.
#[derive(Debug, Clone, Copy)]
pub(crate) enum MetricResolver {
    /// Pure topology, IO, or coordinate-space ops — no CRS metric model.
    None,
    /// Antimeridian ops allow CRS-free lon/lat but reject projected CRS. They
    /// still need the CRS metric model resolved once per call so row kernels do
    /// not re-derive it for every geometry.
    Antimeridian,
    /// Distance/area/length/buffer and other CRS-aware measure/constructive ops.
    Metric { unit: Option<DistanceUnit> },
    /// 3D Euclidean measures (`distance_3d`/`length_3d`). Same unit policy as
    /// [`Self::Metric`], but a geographic CRS is rejected rather than measured
    /// geodesically — see [`crate::broadcast::resolve_metric_3d`].
    Metric3d { unit: Option<DistanceUnit> },
    /// Linear-referencing distances: CRS-aware like [`Self::Metric`], but a
    /// normalized call uses fractions of line length and therefore rejects
    /// `unit=`.
    LineMetric {
        unit: Option<DistanceUnit>,
        normalized: bool,
    },
}

impl MetricResolver {
    /// Attach a call-site ``unit=`` override to a resolver template from
    /// [`crate::dispatch::Operation::resolver`].
    pub(crate) const fn with_unit(self, unit: Option<DistanceUnit>) -> Self {
        match self {
            Self::Metric { .. } => Self::Metric { unit },
            Self::Metric3d { .. } => Self::Metric3d { unit },
            Self::LineMetric { normalized, .. } => Self::LineMetric { unit, normalized },
            Self::Antimeridian => Self::Antimeridian,
            other @ Self::None => other,
        }
    }

    pub(crate) const fn with_line_unit(self, unit: Option<DistanceUnit>, normalized: bool) -> Self {
        match self {
            Self::LineMetric { .. } => Self::LineMetric { unit, normalized },
            Self::Metric { .. } => Self::Metric { unit },
            Self::Metric3d { .. } => Self::Metric3d { unit },
            Self::Antimeridian => Self::Antimeridian,
            other @ Self::None => other,
        }
    }

    /// Resolve the whole metric context in one pass, so kernels cannot see an
    /// impossible model/metric pairing.
    pub(crate) fn resolve_ctx<'a>(
        self,
        frame: &Frame,
        op: &str,
        scratch: &'a mut MetricScratch,
    ) -> pyo3::PyResult<MetricCtx<'a>> {
        scratch.model = None;
        scratch.metric = None;
        match self {
            Self::None => Ok(MetricCtx::None),
            Self::Antimeridian => {
                let model = scratch.model.insert(crs::metric_model(frame.crs_str())?);
                Ok(MetricCtx::Antimeridian(model))
            },
            Self::Metric { unit } => {
                let model = scratch
                    .model
                    .insert(resolve_metric(frame.crs_str(), unit, op)?);
                let metric = scratch
                    .metric
                    .insert(crs::ResolvedMetric::from_model(model)?);
                Ok(MetricCtx::Metric { model, metric })
            },
            Self::Metric3d { unit } => {
                let model = scratch.model.insert(crate::broadcast::resolve_metric_3d(
                    frame.crs_str(),
                    unit,
                    op,
                )?);
                let metric = scratch
                    .metric
                    .insert(crs::ResolvedMetric::from_model(model)?);
                Ok(MetricCtx::Metric { model, metric })
            },
            Self::LineMetric { unit, normalized } => {
                let model = scratch.model.insert(crate::resolve_line_metric(
                    frame.crs_str(),
                    unit,
                    normalized,
                    op,
                )?);
                let metric = scratch
                    .metric
                    .insert(crs::ResolvedMetric::from_model(model)?);
                Ok(MetricCtx::Metric { model, metric })
            },
        }
    }
}

#[derive(Default)]
pub(crate) struct MetricScratch {
    model: Option<MetricModel>,
    metric: Option<ResolvedMetric>,
}

#[derive(Clone, Copy)]
pub(crate) enum MetricCtx<'a> {
    None,
    Antimeridian(&'a MetricModel),
    Metric {
        model: &'a MetricModel,
        metric: &'a ResolvedMetric,
    },
}

impl<'a> MetricCtx<'a> {
    pub(crate) const fn model(self) -> Option<&'a MetricModel> {
        match self {
            Self::None => None,
            Self::Antimeridian(model) | Self::Metric { model, .. } => Some(model),
        }
    }

    pub(crate) const fn resolved(self) -> Option<&'a ResolvedMetric> {
        match self {
            Self::Metric { metric, .. } => Some(metric),
            Self::None | Self::Antimeridian(_) => None,
        }
    }

    /// The metric model a metric-resolved kernel is entitled to — the ONE
    /// spelling of the "registry said this op resolves a metric" invariant
    /// (14 kernels used to hand-roll this match with per-op error strings).
    pub(crate) fn require_model(self, op: &str) -> crate::error::Result<&'a MetricModel> {
        match self {
            Self::Metric { model, .. } => Ok(model),
            Self::None | Self::Antimeridian(_) => Err(crate::crs::CrsError::invalid(format!(
                "internal dispatch error: {op} missing metric context"
            ))),
        }
    }

    /// The resolved metric a metric-resolved kernel is entitled to; the
    /// measure-kernel sibling of [`Self::require_model`].
    pub(crate) fn require_resolved(self, op: &str) -> crate::error::Result<&'a ResolvedMetric> {
        match self {
            Self::Metric { metric, .. } => Ok(metric),
            Self::None | Self::Antimeridian(_) => Err(crate::crs::CrsError::invalid(format!(
                "internal dispatch error: {op} missing metric context"
            ))),
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
    fn(&crate::geometry::ShapeData, &OpCtx<'_>) -> crate::error::Result<f64>;

/// Per-call resolved context handed to unary/binary/grid kernels.
pub(crate) struct OpCtx<'a> {
    pub frame: &'a Frame,
    pub geographic: bool,
    pub metric: MetricCtx<'a>,
    pub lane: Lane,
    pub(crate) left_frame_cache: Option<&'a crate::geometry::FrameDependentCaches>,
    pub(crate) right_frame_cache: Option<&'a crate::geometry::FrameDependentCaches>,
}
