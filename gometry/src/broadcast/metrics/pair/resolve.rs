use std::sync::OnceLock;

use super::{
    DistanceUnit, Frame, GeographicFrame, GeometryError, MetricOp, PreparedPlan, PyGeometry,
    PyResult, Shape, SourceArcContext, TopologyDomain, crs, validate_lonlat_shape,
};
#[cfg(test)]
use crate::test_support::binary_dispatch_counters;

/// Monomorphized planar/geodesic dispatch for [`crs::MetricModel`] kernels.
pub(super) fn dispatch_metric_model<R>(
    model: &crs::MetricModel,
    planar: impl FnOnce(f64) -> crate::error::Result<R>,
    geodesic: impl FnOnce(&str, GeographicFrame, &crs::RuntimeState) -> crate::error::Result<R>,
) -> crate::error::Result<R> {
    match model {
        crs::MetricModel::Planar { to_metre } => planar(to_metre.get()),
        crs::MetricModel::Geodesic { crs, frame, state } => geodesic(crs, *frame, state),
    }
}

/// Monomorphized planar/geodesic dispatch for pre-resolved batch metrics.
pub(super) fn dispatch_resolved_metric<R>(
    metric: &crs::ResolvedMetric,
    planar: impl FnOnce(f64) -> crate::error::Result<R>,
    run_geodesic: impl FnOnce(&crs::GeodesicMetric) -> crate::error::Result<R>,
) -> crate::error::Result<R> {
    match metric {
        crs::ResolvedMetric::Planar { to_metre } => planar(to_metre.get()),
        crs::ResolvedMetric::Geodesic(metric) => run_geodesic(metric),
    }
}

/// Provide the admitted source ellipsoid only if a planar witness requests
/// eligible interior ordinates.
pub(crate) fn with_source_arc_context<T>(
    metric: &crs::ResolvedMetric,
    frame: &Frame,
    domain: TopologyDomain,
    publish: impl FnOnce(&SourceArcContext<'_, '_, crs::GeodesicMetric>) -> T,
) -> T {
    match (metric, domain) {
        (crs::ResolvedMetric::Geodesic(metric), TopologyDomain::Geographic(_)) => {
            let metric_access = || Ok(metric);
            let source_context = SourceArcContext::new(domain, Some(&metric_access));
            publish(&source_context)
        },
        (crs::ResolvedMetric::Planar { .. }, TopologyDomain::Geographic(_)) => {
            let metric = OnceLock::new();
            let metric_access = || {
                metric.get_or_try_init(|| {
                    let model = crs::metric_model(frame.crs_str())?;
                    crs::GeodesicMetric::from_model(&model)
                })
            };
            let source_context = SourceArcContext::new(domain, Some(&metric_access));
            publish(&source_context)
        },
        (_, TopologyDomain::Cartesian) => publish(&SourceArcContext::cartesian()),
    }
}

/// Shared geodesic-pair setup: domain validation plus ellipsoid parameters.
pub(super) fn prepare_geodesic_pair<'a>(
    geodesic: &'a geographiclib_rs::Geodesic,
    a: &PreparedPlan,
    b: &PreparedPlan,
    frame: GeographicFrame,
    runtime_generation: u64,
) -> crate::error::Result<(crs::EllipsoidMetric<'a>, f64, f64)> {
    ensure_geodesic_shape(frame, a.shape())?;
    ensure_geodesic_shape(frame, b.shape())?;
    let metric = crs::EllipsoidMetric::for_geodesic_at_frame(geodesic, runtime_generation, frame);
    let (semi_major, flattening) = metric.ellipsoid_parameters();
    Ok((metric, semi_major, flattening))
}

pub(crate) fn ensure_geodesic_shape(
    frame: GeographicFrame,
    shape: &Shape,
) -> crate::error::Result<()> {
    crs::ensure_geographic_domain_in_frame(frame, shape)
}

pub(crate) fn ensure_hausdorff_geodesic_shape(
    frame: GeographicFrame,
    shape: &Shape,
) -> crate::error::Result<()> {
    crs::ensure_geographic_domain_in_frame(frame, shape)?;
    crate::geometry::ensure_geographic_hausdorff_linework(shape)
}

/// Resolve the [`crs::MetricModel`] a CRS-aware metric should use, honoring the
/// caller's `unit` escape hatch over the geometry's CRS:
///
/// * `unit='planar'` → [`crs::MetricModel::COORDINATE`] (raw coordinate units),
/// * `unit='meters'` → the CRS metric, or a hard error on a CRS-free geometry
///   (no meter scale exists — telling the caller, not silently degrading),
/// * `unit=None` (default) → the CRS-natural model (geodesic meters /
///   projected native linear units / CRS-free raw coordinate units).
///
/// This is the single seam every metric op shares (the spatial index included),
/// so `unit` semantics cannot drift between operations.
pub(crate) fn resolve_metric(
    crs: Option<&str>,
    unit: Option<DistanceUnit>,
    operation: MetricOp,
) -> PyResult<crs::MetricModel> {
    resolve_metric_with_state(crs, unit, operation, None)
}

/// Resolve a metric against a snapshot already captured by the operation's
/// topology admission.
pub(crate) fn resolve_metric_at(
    crs: Option<&str>,
    unit: Option<DistanceUnit>,
    operation: MetricOp,
    state: &crs::RuntimeState,
) -> PyResult<crs::MetricModel> {
    resolve_metric_with_state(crs, unit, operation, Some(state))
}

fn resolve_metric_with_state(
    crs: Option<&str>,
    unit: Option<DistanceUnit>,
    operation: MetricOp,
    state: Option<&crs::RuntimeState>,
) -> PyResult<crs::MetricModel> {
    #[cfg(test)]
    binary_dispatch_counters::metric_context_acquisition();
    let operation = operation.name();
    match unit {
        Some(DistanceUnit::Planar) => Ok(crs::MetricModel::COORDINATE),
        Some(DistanceUnit::Meters) if crs.is_none() => Err(GeometryError::new_err(format!(
            "{operation} unit='meters' requires a CRS; CRS-free geometries have no meter scale — \
             use unit='planar' for raw coordinate units, or set a CRS"
        ))),
        Some(DistanceUnit::Meters) => {
            let crs = crs.expect("checked above");
            Ok(match state {
                Some(state) => crs::metric_model_meters_at(crs, state)?,
                None => crs::metric_model_meters(crs)?,
            })
        },
        None => Ok(match state {
            Some(state) => crs::metric_model_at(crs, state)?,
            None => crs::metric_model(crs)?,
        }),
    }
}

/// [`resolve_metric`]'s 3D sibling, for `distance_3d`/`length_3d`.
///
/// The unit policy is identical: omitted reports native linear units for a
/// projected CRS and raw coordinate units without a CRS; `unit='meters'`
/// scales by the axis factor, and `unit='planar'` uses coordinate units. The
/// one divergence is what a geographic CRS means. 2D measures it
/// geodesically; 3D cannot, because a Euclidean norm over degrees and metre
/// heights is not dimensionally meaningful — so `ensure_3d_metric` rejects an
/// angular horizontal under *every* unit, including `unit='planar'`. (2D
/// `unit='planar'` stays sane there because degrees × degrees is at least
/// homogeneous.)
pub(crate) fn resolve_metric_3d(
    crs: Option<&str>,
    unit: Option<DistanceUnit>,
    operation: MetricOp,
) -> PyResult<crs::MetricModel> {
    #[cfg(test)]
    binary_dispatch_counters::metric_context_acquisition();
    let operation = operation.name();
    // Validate 3D eligibility first, so an ineligible CRS reports the
    // dimensional problem rather than a unit one, whatever `unit=` says.
    let to_metre = crs::ensure_3d_metric(crs)?;
    match unit {
        None | Some(DistanceUnit::Planar) => Ok(crs::MetricModel::COORDINATE),
        Some(DistanceUnit::Meters) if crs.is_none() => Err(GeometryError::new_err(format!(
            "{operation} unit='meters' requires a CRS; CRS-free geometries have no meter scale — \
             use unit='planar' for raw coordinate units, or set a CRS"
        ))),
        Some(DistanceUnit::Meters) => Ok(crs::MetricModel::Planar { to_metre }),
    }
}

/// Resolve a shape to validated WGS84 lon/lat under its frame: CRS-free and
/// WGS84 inputs borrow straight through, any other CRS reprojects. The
/// shape-level seam behind [`lonlat_shape`] — array lanes call it row by row
/// without materializing wrappers.
fn lonlat_shape_under<'a>(
    shape: &'a Shape,
    crs: Option<&str>,
) -> crate::error::Result<std::borrow::Cow<'a, Shape>> {
    let resolved = match crs.filter(|crs| !crs::is_wgs84_lonlat(crs)) {
        Some(crs) => std::borrow::Cow::Owned(crs::transform(shape, crs, "EPSG:4326")?),
        None => std::borrow::Cow::Borrowed(shape),
    };
    validate_lonlat_shape(&resolved)?;
    Ok(resolved)
}

pub(crate) fn lonlat_shape(geometry: &PyGeometry) -> PyResult<Shape> {
    Ok(lonlat_shape_under(geometry.shape.shape(), geometry.crs_str())?.into_owned())
}

#[cfg(test)]
mod lonlat_shape_under_tests {
    use super::lonlat_shape_under;
    use crate::crs::CrsError;
    use crate::error::ErrorKind;
    use crate::geometry::{GeometryErrorKind, Point, Shape};

    #[test]
    fn domain_error_is_native_result() {
        let shape = Shape::Point(Point::new_raw_xy(0.0, 95.0));
        for crs in [None, Some("EPSG:4326")] {
            let error = lonlat_shape_under(&shape, crs).expect_err("out of domain");
            assert!(matches!(
                error.kind(),
                ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
            ));
            let message = error.to_string();
            assert!(
                message.contains("invalid longitude/latitude (0, 95)"),
                "{message}"
            );
        }
    }

    #[test]
    fn nad83_transform_error_precedes_lonlat_domain_validation() {
        let shape = Shape::Point(Point::new_raw_xy(0.0, 95.0));
        let direct_error = crate::crs::transform(&shape, "EPSG:4269", "EPSG:4326")
            .expect_err("transformation rejects the source latitude");
        let adapter_error = lonlat_shape_under(&shape, Some("EPSG:4269"))
            .expect_err("the adapter preserves the transformation failure");
        for error in [direct_error, adapter_error] {
            assert!(
                matches!(
                error.kind(),
                ErrorKind::Crs(CrsError::Transform {
                    from,
                    to,
                    message,
                    ..
                }) if from.as_ref() == "EPSG:4269"
                    && to.as_ref() == "EPSG:4326"
                    && message.as_ref() == "projection failed: latitude 9.5e1 is outside the geographic coordinate domain"
                ),
                "expected the original transform context and latitude cause, got {error}"
            );
        }
    }
}
