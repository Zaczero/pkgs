use super::operation::MetricOp;
use crate::DistanceUnit;
use crate::boundary::metadata::Frame;
use crate::broadcast::resolve_metric;
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
    /// Distance/area/length/buffer and other CRS-aware measure/constructive ops.
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
    /// Linear-referencing distances: CRS-aware like [`Self::Metric`], but a
    /// normalized call uses fractions of line length and therefore rejects
    /// `unit=`.
    LineMetric {
        op: MetricOp,
        unit: Option<DistanceUnit>,
        normalized: bool,
    },
}

impl MetricResolver {
    /// Attach a call-site ``unit=`` override to a resolver template from
    /// [`crate::dispatch::Operation::resolver`].
    pub(crate) const fn with_unit(self, unit: Option<DistanceUnit>) -> Self {
        match self {
            Self::Metric { op, .. } => Self::Metric { op, unit },
            Self::Metric3d { op, .. } => Self::Metric3d { op, unit },
            Self::LineMetric { op, normalized, .. } => Self::LineMetric {
                op,
                unit,
                normalized,
            },
            Self::Antimeridian => Self::Antimeridian,
            other @ Self::None => other,
        }
    }

    pub(crate) const fn with_line_unit(self, unit: Option<DistanceUnit>, normalized: bool) -> Self {
        match self {
            Self::LineMetric { op, .. } => Self::LineMetric {
                op,
                unit,
                normalized,
            },
            Self::Metric { op, .. } => Self::Metric { op, unit },
            Self::Metric3d { op, .. } => Self::Metric3d { op, unit },
            Self::Antimeridian => Self::Antimeridian,
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
        scratch.model = None;
        scratch.metric = None;
        let model = match self {
            Self::None => return Ok(MetricCtx::None),
            Self::Antimeridian => {
                #[cfg(test)]
                metric_context_acquisition();
                let model = scratch.model.insert(crs::metric_model(frame.crs_str())?);
                return Ok(MetricCtx::Antimeridian(model));
            },
            Self::Metric { op, unit } => resolve_metric(frame.crs_str(), unit, op)?,
            Self::Metric3d { op, unit } => {
                crate::broadcast::resolve_metric_3d(frame.crs_str(), unit, op)?
            },
            Self::LineMetric {
                op,
                unit,
                normalized,
            } => {
                #[cfg(test)]
                metric_context_acquisition();
                crate::resolve_line_metric(frame.crs_str(), unit, normalized, op)?
            },
        };
        let metric = match crs::ResolvedMetric::from_model(&model) {
            Ok(metric) => metric,
            Err(error) => {
                drop(model);
                return Err(error.into());
            },
        };
        let model = scratch.model.insert(model);
        let metric = scratch.metric.insert(metric);
        Ok(MetricCtx::Metric { model, metric })
    }
}

#[cfg(test)]
mod tests {
    use pyo3::exceptions::PyMemoryError;
    use pyo3::prelude::*;

    use super::*;
    use crate::boundary::metadata::Frame;
    use crate::crs::{RUNTIME_STATE_TEST_LOCK, clear_cache};
    use crate::test_support::{allocation_count, fail_allocations_after};

    #[test]
    fn resolve_ctx_keeps_metric_scratch_empty_on_from_model_failure() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        Python::initialize();
        Python::attach(|py| {
            clear_cache();
            let frame = Frame::Crs("EPSG:4326".into());
            let resolver = MetricResolver::Metric {
                op: MetricOp::Distance,
                unit: None,
            };

            // Warm the CRS and geodesic caches so the final allocation in the
            // successful call is Box::new inside GeodesicMetric::from_model.
            let mut warm_scratch = MetricScratch::default();
            match resolver.resolve_ctx(&frame, &mut warm_scratch) {
                Ok(_) => {},
                Err(error) => panic!("warm metric resolution failed: {error}"),
            }
            drop(warm_scratch);

            let ((), allocations) = allocation_count(|| {
                let mut scratch = MetricScratch::default();
                match resolver.resolve_ctx(&frame, &mut scratch) {
                    Ok(_) => {},
                    Err(error) => panic!("counting metric resolution failed: {error}"),
                }
                drop(scratch);
            });
            assert!(allocations > 0, "the warm resolver must allocate");

            let mut scratch = MetricScratch::default();
            let error = fail_allocations_after(allocations - 1, || {
                match resolver.resolve_ctx(&frame, &mut scratch) {
                    Err(error) => error,
                    Ok(_) => panic!("the final from_model allocation must fail"),
                }
            });
            assert!(error.is_instance_of::<PyMemoryError>(py));
            assert!(scratch.model.is_none());
            assert!(scratch.metric.is_none());
            drop(error);
        });
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
    pub(crate) fn require_model(self, op: MetricOp) -> crate::error::Result<&'a MetricModel> {
        match self {
            Self::Metric { model, .. } => Ok(model),
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
            Self::None | Self::Antimeridian(_) => Err(crate::crs::CrsError::invalid(format!(
                "internal dispatch error: {} missing metric context",
                op.name()
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
