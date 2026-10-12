use std::sync::OnceLock;

use crate::array::PyGeometryArray;
#[cfg(test)]
use crate::broadcast::metrics::ShapeData;
use crate::broadcast::metrics::{
    CoordinateAxes, DistanceUnit, GeometryArrayStorage, GeometryError, GeometryKind, Point,
    PyGeometry, PyResult, Shape, crs,
};
use crate::crs::frame::Frame;
use crate::crs::local::{LocalInputPurpose, PreparedLocalInput, geodesic_by_identity};
use crate::dispatch::MetricOp;
use crate::geographic::{ComputedEdgeIntent, GeographicFrame, lonlat as geographic};
use crate::geometry::storage::packed_columns::PackedColumns;
use crate::geometry::storage::{PackedStorage, PointRows};
use crate::geometry::{
    FrameDependentCaches, LineSeq, PreparedPlan, ShapeCountQuery, SourceArcCapture,
    SourceArcContext, TopologyDomain, emit_from_original, storage,
};
#[cfg(test)]
use crate::test_support::binary_dispatch_counters;
use crate::{NonNegative, validate_lonlat_shape};

/// Monomorphized planar/geodesic dispatch for [`crs::MetricModel`] kernels.
fn dispatch_metric_model<R>(
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
fn dispatch_resolved_metric<R>(
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
fn prepare_geodesic_pair<'a>(
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

pub(crate) fn geodesic_point_columns_to_shape_values(
    crs: &str,
    xs: &[f64],
    ys: &[f64],
    shape: &PreparedPlan,
    frame_cache: &FrameDependentCaches,
    frame: GeographicFrame,
    state: &crs::RuntimeState,
) -> crate::error::Result<Vec<f64>> {
    crs::with_resolved_ellipsoid_metric_at(crs, &[shape.shape()], state, |crs, metric| {
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        shape.geodesic_distance_points(
            frame_cache,
            xs.iter().copied().zip(ys.iter().copied()),
            crs,
            semi_major,
            flattening,
            metric,
            frame,
        )
    })
}

pub(crate) fn geodesic_point_columns_dwithin_shape_values(
    crs: &str,
    xs: &[f64],
    ys: &[f64],
    shape: &PreparedPlan,
    frame_cache: &FrameDependentCaches,
    distance: f64,
    frame: GeographicFrame,
    state: &crs::RuntimeState,
) -> crate::error::Result<Vec<bool>> {
    crs::with_resolved_ellipsoid_metric_at(crs, &[shape.shape()], state, |crs, metric| {
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        shape.geodesic_dwithin_points(
            frame_cache,
            xs.iter().copied().zip(ys.iter().copied()),
            crs,
            semi_major,
            flattening,
            metric,
            distance,
            frame,
        )
    })
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

/// Array sibling of [`lonlat_shape`]: validated lon/lat points at logical rows.
pub(crate) enum LonLatPointRows<'a> {
    Source(PointRows<'a>),
    Columns {
        xs: Vec<f64>,
        ys: Vec<f64>,
    },
    CompactColumns {
        xs: Vec<f64>,
        ys: Vec<f64>,
        present_rows: Vec<usize>,
        logical_len: usize,
    },
}

impl LonLatPointRows<'_> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Source(points) => points.len(),
            Self::Columns { xs, .. } => xs.len(),
            Self::CompactColumns { logical_len, .. } => *logical_len,
        }
    }

    pub(crate) fn for_each_present(
        &self,
        mut is_missing: impl FnMut(usize) -> bool,
        mut visit: impl FnMut(usize, Point),
    ) {
        match self {
            Self::Source(points) => {
                for row in 0..points.len() {
                    if !is_missing(row) {
                        visit(row, points.get(row));
                    }
                }
            },
            Self::Columns { xs, ys } => {
                for (row, (&x, &y)) in xs.iter().zip(ys).enumerate() {
                    if !is_missing(row) {
                        visit(row, Point::new_raw_xy(x, y));
                    }
                }
            },
            Self::CompactColumns {
                xs,
                ys,
                present_rows,
                ..
            } => {
                for ((&row, &x), &y) in present_rows.iter().zip(xs).zip(ys) {
                    visit(row, Point::new_raw_xy(x, y));
                }
            },
        }
    }
}

/// Gather error provenance. Callers format: grid without a row note, geocode
/// with [`crate::note_array_row`] on [`Self::Row`].
#[derive(Debug)]
pub(crate) enum LonLatPointsError {
    Type,
    Setup(crate::error::Error),
    Row(usize, crate::error::Error),
}

fn lonlat_domain(row: usize, x: f64, y: f64) -> Result<(), LonLatPointsError> {
    geographic::validate_lonlat_xy(x, y).map_err(|error| LonLatPointsError::Row(row, error))
}

fn transform_failure_to_row(
    failure: crate::crs::TransformFailure,
    present_rows: &[usize],
) -> LonLatPointsError {
    let crate::crs::TransformFailure { error, coordinate } = failure;
    match coordinate {
        Some(coordinate) => LonLatPointsError::Row(
            *present_rows
                .get(coordinate)
                .expect("transform failure coordinate belongs to the present-row batch"),
            error,
        ),
        None => LonLatPointsError::Setup(error),
    }
}

fn transform_present(
    crs: &str,
    xs: &mut [f64],
    ys: &mut [f64],
    present: &[usize],
) -> Result<(), LonLatPointsError> {
    let mut px = Vec::with_capacity(present.len());
    let mut py = Vec::with_capacity(present.len());
    for &row in present {
        px.push(
            *xs.get(row)
                .expect("present logical row is inside longitude columns"),
        );
        py.push(
            *ys.get(row)
                .expect("present logical row is inside latitude columns"),
        );
    }
    let plan =
        crate::crs::TransformPlan::new(crs, "EPSG:4326").map_err(LonLatPointsError::Setup)?;
    plan.transform_coordinates_indexed(&mut px, &mut py, crate::Zt::None)
        .map_err(|failure| transform_failure_to_row(failure, present))?;
    for ((&row, x), y) in present.iter().zip(px).zip(py) {
        lonlat_domain(row, x, y)?;
        *xs.get_mut(row)
            .expect("present logical row is inside longitude columns") = x;
        *ys.get_mut(row)
            .expect("present logical row is inside latitude columns") = y;
    }
    Ok(())
}

fn transform_compact_present(
    crs: &str,
    xs: &mut [f64],
    ys: &mut [f64],
    present_rows: &[usize],
) -> Result<(), LonLatPointsError> {
    let plan =
        crate::crs::TransformPlan::new(crs, "EPSG:4326").map_err(LonLatPointsError::Setup)?;
    plan.transform_coordinates_indexed(xs, ys, crate::Zt::None)
        .map_err(|failure| transform_failure_to_row(failure, present_rows))?;
    for (index, &row) in present_rows.iter().enumerate() {
        lonlat_domain(
            row,
            *xs.get(index)
                .expect("transformed longitude column matches present rows"),
            *ys.get(index)
                .expect("transformed latitude column matches present rows"),
        )?;
    }
    Ok(())
}

fn compact_lonlat_points(
    array: &PyGeometryArray,
) -> Result<LonLatPointRows<'_>, LonLatPointsError> {
    let projected_crs = array.crs_str().filter(|crs| !crs::is_wgs84_lonlat(crs));
    let Some(PackedColumns::Points(points)) = array.row_owner().packed_columns() else {
        unreachable!("compact nullable point rows expose packed point columns");
    };
    let point_rows = points.rows();
    let present_count = point_rows.present_count();
    let mut xs = Vec::with_capacity(present_count);
    let mut ys = Vec::with_capacity(present_count);
    let mut present_rows = Vec::with_capacity(present_count);
    for (logical, physical) in point_rows.cursor() {
        let Some(physical) = physical else {
            continue;
        };
        let (x, y) = points.physical_xy_at(physical);
        xs.push(x);
        ys.push(y);
        present_rows.push(logical);
    }
    let Some(crs) = projected_crs else {
        for (index, &row) in present_rows.iter().enumerate() {
            lonlat_domain(
                row,
                *xs.get(index)
                    .expect("compact longitude column matches present rows"),
                *ys.get(index)
                    .expect("compact latitude column matches present rows"),
            )?;
        }
        return Ok(LonLatPointRows::CompactColumns {
            xs,
            ys,
            present_rows,
            logical_len: array.len(),
        });
    };
    if !present_rows.is_empty() {
        transform_compact_present(crs, &mut xs, &mut ys, &present_rows)?;
    }
    Ok(LonLatPointRows::CompactColumns {
        xs,
        ys,
        present_rows,
        logical_len: array.len(),
    })
}

/// Resolve every logical row of `array` to a validated lon/lat point.
pub(crate) fn lonlat_points(
    array: &PyGeometryArray,
) -> Result<LonLatPointRows<'_>, LonLatPointsError> {
    if array.is_compact_nullable() {
        if !matches!(
            array.storage(),
            GeometryArrayStorage::Packed(PackedStorage::Points { .. })
        ) {
            return Err(LonLatPointsError::Type);
        }
        return compact_lonlat_points(array);
    }
    let points = array
        .storage()
        .point_rows()
        .ok_or(LonLatPointsError::Type)?;
    let projected_crs = array.crs_str().filter(|crs| !crs::is_wgs84_lonlat(crs));
    let crs = match projected_crs {
        Some(crs) if !points.is_empty() => crs,
        _ => {
            for (row, point) in points.iter().enumerate() {
                if array.is_row_missing(row) {
                    continue;
                }
                lonlat_domain(row, point.x, point.y)?;
            }
            return Ok(LonLatPointRows::Source(points));
        },
    };
    let mut xs = Vec::with_capacity(points.len());
    let mut ys = Vec::with_capacity(points.len());
    for point in points.iter() {
        xs.push(point.x);
        ys.push(point.y);
    }
    let mut present = Vec::with_capacity(points.len());
    for row in 0..points.len() {
        if !array.is_row_missing(row) {
            present.push(row);
        }
    }
    if !present.is_empty() {
        transform_present(crs, &mut xs, &mut ys, &present)?;
    }
    Ok(LonLatPointRows::Columns { xs, ys })
}

/// CRS-aware minimum distance: native linear units for projected geometry,
/// raw coordinate units for CRS-free geometry, and geodesic meters on the CRS
/// ellipsoid (between the closest pair of points) for geographic geometry.
///
pub(crate) fn pair_distance_planar(
    to_metre: f64,
    a: &PreparedPlan,
    b: &PreparedPlan,
) -> crate::error::Result<f64> {
    Ok(a.distance(b)? * to_metre)
}

pub(crate) fn pair_distance_geodesic(
    metric: &crs::GeodesicMetric,
    a: &PreparedPlan,
    a_cache: &FrameDependentCaches,
    b: &PreparedPlan,
    b_cache: &FrameDependentCaches,
) -> crate::error::Result<f64> {
    if a.shape().is_empty() || b.shape().is_empty() {
        return Ok(f64::INFINITY);
    }
    let (ellipsoid, semi_major, flattening) = prepare_geodesic_pair(
        &metric.geodesic,
        a,
        b,
        metric.frame,
        metric.state.generation,
    )?;
    crs::finite(
        a.geodesic_distance_cached(
            a_cache,
            b,
            b_cache,
            &metric.crs,
            semi_major,
            flattening,
            &ellipsoid,
        )?,
        crs::GeodesicCalculation::Distance,
    )
}

pub(crate) fn pair_dwithin_planar(
    to_metre: f64,
    a: &PreparedPlan,
    b: &PreparedPlan,
    distance: f64,
) -> crate::error::Result<bool> {
    let distance = if distance.is_infinite() && distance.is_sign_positive() {
        distance
    } else {
        NonNegative::try_new("distance", distance)?.get()
    };
    a.dwithin(b, distance / to_metre)
}

pub(crate) fn pair_dwithin_geodesic(
    metric: &crs::GeodesicMetric,
    a: &PreparedPlan,
    a_cache: &FrameDependentCaches,
    b: &PreparedPlan,
    b_cache: &FrameDependentCaches,
    distance: f64,
) -> crate::error::Result<bool> {
    let distance = if distance.is_infinite() && distance.is_sign_positive() {
        distance
    } else {
        NonNegative::try_new("distance", distance)?.get()
    };
    if a.shape().is_empty() || b.shape().is_empty() {
        return Ok(false);
    }
    if distance == f64::INFINITY {
        ensure_geodesic_shape(metric.frame, a.shape())?;
        ensure_geodesic_shape(metric.frame, b.shape())?;
        return Ok(true);
    }
    let (ellipsoid, semi_major, flattening) = prepare_geodesic_pair(
        &metric.geodesic,
        a,
        b,
        metric.frame,
        metric.state.generation,
    )?;
    a.geodesic_dwithin_cached(
        a_cache,
        b,
        b_cache,
        &metric.crs,
        semi_major,
        flattening,
        &ellipsoid,
        distance,
    )
}

/// Element-wise zero metric for array-vs-self when both operands share one
/// `Arc<GeometryArrayStorage>`.
#[expect(
    clippy::indexing_slicing,
    reason = "selected polygon ring ranges are admitted by the corresponding nested CSR offset columns"
)]
pub(crate) fn same_storage_similarity_metric_zeros(storage: &GeometryArrayStorage) -> Vec<f64> {
    let mut values = Vec::with_capacity(storage.len());
    match storage {
        GeometryArrayStorage::Packed(PackedStorage::Lines {
            offsets, row_map, ..
        }) => {
            let map = row_map.as_deref();
            let rows = storage::line_logical_len(offsets.as_slice(), map);
            for logical in 0..rows {
                values.push(if map.csr_window(offsets.as_slice(), logical).is_empty() {
                    f64::INFINITY
                } else {
                    0.0
                });
            }
        },
        GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
            let rows = storage::point_logical_len(coords, row_map.as_deref());
            values.resize(rows, 0.0);
        },
        GeometryArrayStorage::Packed(PackedStorage::Polygons {
            ring_offsets,
            polygon_offsets,
            row_map,
            ..
        }) => {
            let map = row_map.as_deref();
            let rows = storage::polygon_logical_len(polygon_offsets.as_slice(), map);
            for logical in 0..rows {
                let rings = map.csr_window(polygon_offsets.as_slice(), logical);
                values.push(
                    if !rings.is_empty()
                        && (ring_offsets[rings.start] as usize) < (ring_offsets[rings.end] as usize)
                    {
                        0.0
                    } else {
                        f64::INFINITY
                    },
                );
            }
        },
        GeometryArrayStorage::Mixed(shapes) => {
            for shape in shapes {
                values.push(if shape.is_empty() { f64::INFINITY } else { 0.0 });
            }
        },
    }
    values
}

/// CRS-aware continuous Hausdorff distance: native linear units on projected
/// frames, raw coordinate units for CRS-free frames, coordinate-space units
/// under `unit='planar'`, explicit SI meters under `unit='meters'`, or true
/// geodesic meters on a geographic CRS.
pub(crate) fn metric_hausdorff(
    model: &crs::MetricModel,
    a: &Shape,
    b: &Shape,
    max_error: Option<f64>,
) -> crate::error::Result<f64> {
    // Domain validation BEFORE the identity exit: equal operands with
    // out-of-domain latitude must still raise on a geographic model.
    if let crs::MetricModel::Geodesic { frame, .. } = model {
        ensure_hausdorff_geodesic_shape(*frame, a)?;
        ensure_hausdorff_geodesic_shape(*frame, b)?;
    }
    // Like `distance`, a supremum of infima over an empty operand is infinite.
    if a.is_empty() || b.is_empty() {
        return Ok(f64::INFINITY);
    }
    if a == b && !a.has_area_parts() {
        return Ok(0.0);
    }
    dispatch_metric_model(
        model,
        |to_metre| crate::geometry::hausdorff_distance(a, b, max_error, to_metre),
        |crs, _, state| crs::geodesic_hausdorff_at(crs, a, b, state),
    )
}

#[cfg(test)]
mod same_storage_metric_tests {
    use super::*;

    #[test]
    fn same_storage_hausdorff_returns_one_zero_per_row() {
        let storage = GeometryArrayStorage::Mixed(vec![
            Shape::Point(Point::new_raw_xy(0.0, 0.0)),
            Shape::Point(Point::new_raw_xy(1.0, 0.0)),
        ]);
        assert_eq!(same_storage_similarity_metric_zeros(&storage), [0.0, 0.0]);
    }
}

pub(crate) fn metric_frechet_with_scratch(
    model: &crs::MetricModel,
    a: &Shape,
    b: &Shape,
    scratch: &mut crate::geometry::FrechetDpScratch,
) -> crate::error::Result<f64> {
    // Domain validation BEFORE the identity exit (same contract as Hausdorff).
    if let crs::MetricModel::Geodesic { frame, .. } = model {
        crs::ensure_geographic_domain_in_frame(*frame, a)?;
        crs::ensure_geographic_domain_in_frame(*frame, b)?;
    }
    // Emptiness is TOTAL and is decided here, once, for every lane below —
    // planar, geodesic and identity alike. Deciding it per kernel is what let
    // the geodesic arm keep raising after the planar one was fixed, and let an
    // identical pair of empties raise (via `single_linework`'s `EmptyLinework`,
    // a DATA condition) while every non-identical empty pair returned the
    // sentinel. Domain validation stays above it: that is a real error, and an
    // empty shape has no coordinates to validate anyway.
    if a.is_empty() || b.is_empty() {
        return Ok(f64::INFINITY);
    }
    if a == b {
        // Kind check before the identity shortcut — equal polygons must still
        // raise `GeometryTypeError`, not return 0.
        a.single_linework()?;
        return Ok(0.0);
    }
    match model {
        crs::MetricModel::Planar { to_metre } => {
            Ok(a.frechet_distance_with_scratch(b, scratch)? * to_metre.get())
        },
        crs::MetricModel::Geodesic { crs, state, .. } => {
            crs::geodesic_frechet_at_with_scratch(crs, a, b, state, scratch)
        },
    }
}

/// The discrete Fréchet DP is `n · m` cells, so `densify` amplifies
/// quadratically: on a pair of two-vertex lines, `1e-5` is 10¹⁰ cells and
/// `1e-6` is 10¹². Charge the product so an unreachable answer is a typed
/// error naming the parameter to raise, rather than an uninterruptible wait.
/// Undensified operands are input-sized and stay uncharged.
fn charge_frechet_dp(a: &Shape, b: &Shape) -> crate::error::Result<()> {
    let mut counts = ShapeCountQuery::default();
    crate::geometry::ExpansionBudget::pairwise(
        "frechet_distance",
        "densify",
        counts.coordinates(a)?,
        counts.coordinates(b)?,
    )
}

pub(crate) fn metric_frechet_densified_with_scratch(
    model: &crs::MetricModel,
    a: &Shape,
    b: &Shape,
    densify: Option<f64>,
    scratch: &mut crate::geometry::FrechetDpScratch,
) -> crate::error::Result<f64> {
    match densify {
        None => {
            charge_frechet_dp(a, b)?;
            metric_frechet_with_scratch(model, a, b, scratch)
        },
        Some(fraction) => {
            let (a, b) = match model {
                crs::MetricModel::Planar { .. } => {
                    let densified = |shape: &Shape| {
                        shape.densified_budgeted(
                            fraction,
                            &mut crate::geometry::ExpansionBudget::new(
                                "frechet_distance",
                                "densify",
                            ),
                        )
                    };
                    (densified(a)?, densified(b)?)
                },
                crs::MetricModel::Geodesic { crs, frame, state } => {
                    crs::ensure_geographic_domain_in_frame(*frame, a)?;
                    crs::ensure_geographic_domain_in_frame(*frame, b)?;
                    // Copy the cached ellipsoid out before running the kernel: the
                    // geodesic metric kernel may acquire the same thread-local
                    // cache, and holding its RefCell borrow across that call would
                    // panic on the similarity path.
                    let geodesic = crs::with_geodesic_cache_at(
                        crs,
                        state,
                        |error| error,
                        |geodesic| Ok(*geodesic),
                    )?;
                    let mut budget =
                        crate::geometry::ExpansionBudget::new("frechet_distance", "densify");
                    (
                        a.densified_budgeted_with_placement(
                            fraction,
                            &crate::geometry::SegmentPlacement::Chart(&geodesic, *frame),
                            &mut budget,
                        )?,
                        b.densified_budgeted_with_placement(
                            fraction,
                            &crate::geometry::SegmentPlacement::Chart(&geodesic, *frame),
                            &mut budget,
                        )?,
                    )
                },
            };
            charge_frechet_dp(&a, &b)?;
            metric_frechet_with_scratch(model, &a, &b, scratch)
        },
    }
}

/// `metric_nearest_points` against an already-resolved metric. `None` when an
/// operand is empty — the surfaces map it to the output-type EMPTY sentinel
/// (`(POINT EMPTY, POINT EMPTY)` / `LINESTRING EMPTY`), mirroring distance's
/// `inf`.
pub(crate) fn metric_nearest_points_resolved(
    metric: &crs::ResolvedMetric,
    a: &PreparedPlan,
    a_cache: &FrameDependentCaches,
    b: &PreparedPlan,
    b_cache: &FrameDependentCaches,
    source_context: Option<&SourceArcContext<'_, '_, crs::GeodesicMetric>>,
) -> crate::error::Result<Option<(Point, Point)>> {
    if a.shape().is_empty() || b.shape().is_empty() {
        return Ok(None);
    }
    dispatch_resolved_metric(
        metric,
        |_| {
            source_context.map_or_else(
                || a.nearest_points(b),
                |source_context| a.nearest_points_with_source_context(b, source_context),
            )
        },
        |metric| {
            let (ellipsoid, semi_major, flattening) = prepare_geodesic_pair(
                &metric.geodesic,
                a,
                b,
                metric.frame,
                metric.state.generation,
            )?;
            a.geodesic_nearest_points_cached(
                a_cache,
                b,
                b_cache,
                &metric.crs,
                semi_major,
                flattening,
                &ellipsoid,
            )
        },
    )
}

/// CRS-aware shortest connecting line — `metric_nearest_points` as a two-point
/// `LineString` (degenerate when the geometries touch), or `LINESTRING EMPTY`
/// in the operands' common axes when a side is empty.
pub(crate) fn metric_shortest_line_resolved(
    metric: &crs::ResolvedMetric,
    a: &PreparedPlan,
    b: &PreparedPlan,
    domain: TopologyDomain,
    source_context: &SourceArcContext<'_, '_, crs::GeodesicMetric>,
) -> crate::error::Result<Shape> {
    let common = crate::geometry::common_axes(a.shape(), b.shape());
    let source_capture = SourceArcCapture::for_output_axes(common, source_context);
    match metric {
        crs::ResolvedMetric::Planar { .. } => {
            let pair = metric_nearest_points_resolved(
                metric,
                a,
                &FrameDependentCaches::default(),
                b,
                &FrameDependentCaches::default(),
                source_capture.context(),
            )?;
            match (pair.as_ref(), domain) {
                (Some((start, end)), TopologyDomain::Geographic(frame)) => {
                    crate::geometry::nearest_line_with_intent(
                        pair.as_ref(),
                        common,
                        frame,
                        ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [
                            start.x, end.x,
                        ])?,
                        &source_capture,
                    )
                },
                (_, TopologyDomain::Cartesian) => {
                    crate::geometry::nearest_line(pair.as_ref(), common)
                },
                (None, TopologyDomain::Geographic(_)) => {
                    crate::geometry::nearest_line(None, common)
                },
            }
        },
        crs::ResolvedMetric::Geodesic(metric) => {
            let (ellipsoid, semi_major, flattening) = prepare_geodesic_pair(
                &metric.geodesic,
                a,
                b,
                metric.frame,
                metric.state.generation,
            )?;
            let a_cache = FrameDependentCaches::default();
            let b_cache = FrameDependentCaches::default();
            match a.geodesic_nearest_line_cached(
                &a_cache,
                b,
                &b_cache,
                &metric.crs,
                semi_major,
                flattening,
                &ellipsoid,
            )? {
                Some((start, end, intent)) => crate::geometry::nearest_line_with_intent(
                    Some(&(start, end)),
                    common,
                    metric.frame,
                    ComputedEdgeIntent::retained_chart_witness(
                        metric.frame,
                        [start.x, end.x],
                        intent,
                    )?,
                    &source_capture,
                ),
                None => crate::geometry::nearest_line(None, common),
            }
        },
    }
}

/// CRS-aware minimum-clearance witness line: the realizing two-point
/// `LineString` in the geometry's own coordinates. On a geographic CRS the
/// witness is found in the geometry's best local projection (the same frame
/// `metric_minimum_clearance` measures in), but the witness coordinates are
/// emitted from source-frame vertex provenance instead of round-tripping the
/// projected line.
pub(crate) fn metric_minimum_clearance_line(
    model: &crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
) -> crate::error::Result<Shape> {
    metric_minimum_clearance_line_impl(model, crs, shape, true)
}

fn metric_minimum_clearance_line_impl(
    model: &crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
    resolve_source_ordinates: bool,
) -> crate::error::Result<Shape> {
    dispatch_metric_model(
        model,
        |_| shape.minimum_clearance_line(),
        |_, _, state| {
            let mut budget =
                crate::geometry::ExpansionBudget::new("minimum_clearance", "input_refinement");
            geodesic_by_identity(
                shape,
                crs,
                state,
                &mut budget,
                if resolve_source_ordinates {
                    LocalInputPurpose::ClearanceWitness
                } else {
                    LocalInputPurpose::BoundarySupport
                },
                |sites, support, refined_chains| {
                    let Some(witness) = sites.minimum_clearance_plan_with_support(
                        support,
                        refined_chains,
                        resolve_source_ordinates,
                    )?
                    else {
                        return Ok((
                            Vec::new(),
                            Shape::typed_empty(GeometryKind::LineString, CoordinateAxes::XY),
                        ));
                    };
                    let plan = witness.plan;
                    let projected_points = emit_from_original(&witness.points, &plan)?;
                    let [a, b] = projected_points.as_slice() else {
                        unreachable!("minimum-clearance witness plan has two vertices");
                    };
                    Ok((
                        Vec::from(plan),
                        Shape::LineString(LineSeq::segment(*a, *b)?),
                    ))
                },
                |source_points, original_roots, plan, axes, source_frame| {
                    if !resolve_source_ordinates
                        || (!axes.has_z() && !axes.has_m())
                        || !plan.iter().any(|vertex| {
                            matches!(vertex, crate::geometry::VertexProvenance::OnSegment {
                                root_request: Some(_),
                                ..
                            })
                        })
                    {
                        return emit_from_original(source_points, plan);
                    }
                    let source_metric = std::cell::OnceCell::new();
                    let metric_access = || {
                        source_metric
                            .get_or_try_init(|| crate::crs::GeodesicMetric::from_model(model))
                    };
                    crate::geometry::emit_from_original_with_source_roots(
                        source_points,
                        original_roots,
                        plan,
                        axes,
                        source_frame,
                        Some(&metric_access),
                        &mut crate::geometry::Unmetered,
                    )
                },
                |points| {
                    if points.is_empty() {
                        Ok(Shape::typed_empty(
                            GeometryKind::LineString,
                            CoordinateAxes::XY,
                        ))
                    } else {
                        let [a, b] = points.as_slice() else {
                            unreachable!("minimum_clearance witness plan has two vertices");
                        };
                        Ok(Shape::LineString(LineSeq::segment(*a, *b)?))
                    }
                },
            )
        },
    )
}

/// CRS-aware minimum clearance: the planar clearance in the active planar unit,
/// or — on a geographic CRS — the geodesic length of the source-frame
/// provenance witness selected in the geometry's best local projection.
pub(crate) fn metric_minimum_clearance(
    model: &crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
) -> crate::error::Result<f64> {
    dispatch_metric_model(
        model,
        |to_metre| Ok(shape.minimum_clearance()? * to_metre),
        |geodesic_crs, _, state| {
            let line = metric_minimum_clearance_line_impl(model, crs, shape, false)?;
            crs::geodesic_witness_distance_at(geodesic_crs, &line, state)
        },
    )
}

/// CRS-aware maximum inscribed radius — the maximum-clearance
/// center-to-boundary distance, measured in the geometry's best local
/// projection on a geographic CRS (a near-isometric frame) or in the active
/// planar unit on a projected/planar one. The metric twin of the (metric)
/// `maximum_inscribed_circle`, so the radius matches the disk.
pub(crate) fn metric_maximum_inscribed_radius(
    model: &crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
    tolerance: Option<f64>,
) -> crate::error::Result<f64> {
    dispatch_metric_model(
        model,
        |to_metre| shape.maximum_inscribed_radius_in_scale(tolerance, to_metre),
        |_, _, state| {
            let source = crs.unwrap_or("EPSG:4326");
            let mut budget = crate::geometry::ExpansionBudget::new(
                "maximum_inscribed_radius",
                "input_refinement",
            );
            let Some(prepared) = PreparedLocalInput::prepare(
                shape,
                &[shape],
                source,
                None,
                LocalInputPurpose::BoundarySupport,
                state,
                &mut budget,
            )?
            else {
                return Ok(f64::NAN);
            };
            prepared
                .projected_working()
                .maximum_inscribed_radius_in_frame(tolerance)
        },
    )
}

#[cfg(test)]
mod dwithin_parity_tests {
    use super::*;
    use crate::geometry::{point_distance, points_dwithin};

    fn point(x: f64, y: f64) -> Point {
        Point::new_raw_xy(x, y)
    }

    fn scalar_point_dwithin(a: Point, b: Point, limit: f64) -> bool {
        Shape::Point(a).dwithin(&Shape::Point(b), limit).unwrap()
    }

    fn packed_planar_dwithin(a: Point, b: Point, limit: f64) -> bool {
        points_dwithin(a, b, limit)
    }

    fn shape_data_dwithin(a: Point, b: Point, limit: f64) -> bool {
        PreparedPlan::new(ShapeData::new(Shape::Point(a)))
            .dwithin(&PreparedPlan::new(ShapeData::new(Shape::Point(b))), limit)
            .unwrap()
    }

    #[test]
    fn planar_dwithin_scalar_packed_and_shape_data_agree() {
        let coords = [-1e6, -3.0, -1.0, 0.0, 1.0, 3.0, 4.0, 1e6];
        for &ax in &coords {
            for &ay in &coords {
                for &bx in &coords {
                    for &by in &coords {
                        let (a, b) = (point(ax, ay), point(bx, by));
                        let dist = point_distance(a, b);
                        let limits = [
                            0.0,
                            f64::MIN_POSITIVE,
                            dist * 0.5,
                            dist,
                            dist.next_down(),
                            dist.next_up(),
                            dist * 2.0,
                            f64::INFINITY,
                        ];
                        for limit in limits {
                            let scalar = scalar_point_dwithin(a, b, limit);
                            let packed = packed_planar_dwithin(a, b, limit);
                            let data = shape_data_dwithin(a, b, limit);
                            assert_eq!(
                                scalar, packed,
                                "scalar vs packed at ({ax},{ay})-({bx},{by}) limit={limit}"
                            );
                            assert_eq!(
                                scalar, data,
                                "scalar vs ShapeData at ({ax},{ay})-({bx},{by}) limit={limit}"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod lonlat_points_tests {
    use super::{GeometryArrayStorage, LonLatPointRows, LonLatPointsError, lonlat_points};
    use crate::array::PyGeometryArray;
    use crate::crs::frame::{Frame, crs_arc_static};
    use crate::geometry::storage::ValidityMask;
    use crate::geometry::{CoordSeq, Point, Shape};
    use crate::io::parse_wkt;

    fn packed_xy(xs: Vec<f64>, ys: Vec<f64>, frame: Frame) -> PyGeometryArray {
        PyGeometryArray::packed_points(CoordSeq::try_from_vecs(xs, ys, None, None).unwrap(), frame)
            .unwrap()
    }

    fn assert_transform_error_row(array: &PyGeometryArray, expected_row: usize) {
        match lonlat_points(array) {
            Err(LonLatPointsError::Row(row, error)) => {
                assert_eq!(row, expected_row);
                assert!(
                    error.to_string().contains("transverse Mercator domain"),
                    "unexpected projected coordinate error: {error}"
                );
            },
            Err(LonLatPointsError::Type) => panic!("expected a projected row error, got Type"),
            Err(LonLatPointsError::Setup(error)) => {
                panic!("expected a projected row error, got Setup({error})")
            },
            Ok(_) => panic!("expected a projected coordinate error on row {expected_row}"),
        }
    }

    #[test]
    fn dense_lonlat_borrows_source() {
        let array = packed_xy(vec![1.0, 2.0], vec![3.0, 4.0], Frame::None);
        let points = lonlat_points(&array).expect("in-domain packed points");
        assert!(matches!(points, LonLatPointRows::Source(_)));
        assert_eq!(points.len(), 2);
        let mut visited = Vec::new();
        points.for_each_present(|_| false, |row, point| visited.push((row, point)));
        assert_eq!(visited, [
            (0, Point::new_raw_xy(1.0, 3.0)),
            (1, Point::new_raw_xy(2.0, 4.0)),
        ]);
    }

    #[test]
    fn missing_row_is_not_validated_and_keeps_logical_index() {
        let present = packed_xy(vec![1.0], vec![2.0], Frame::None);
        let mask = ValidityMask::from_vec(3, vec![true, false, true]).expect("mask");
        let array = PyGeometryArray::scatter_present_rows(&present, mask).unwrap();
        let points = lonlat_points(&array).expect("present row is in domain");
        assert!(matches!(points, LonLatPointRows::CompactColumns { .. }));
        assert_eq!(points.len(), 3);
        let mut visited = Vec::new();
        points.for_each_present(
            |row| array.is_row_missing(row),
            |row, point| visited.push((row, point)),
        );
        assert_eq!(visited, [(1, Point::new_raw_xy(1.0, 2.0))]);
    }

    #[test]
    fn compact_projected_points_transform_present_columns_in_place() {
        let present = packed_xy(
            vec![960_464.566_564_364_4],
            vec![6_002_020.553_970_727],
            Frame::Crs(crs_arc_static("EPSG:3857")),
        );
        let mask = ValidityMask::from_vec(3, vec![true, false, true]).expect("mask");
        let array = PyGeometryArray::scatter_present_rows(&present, mask).unwrap();
        let points = lonlat_points(&array).expect("projected point transforms to valid lon/lat");

        assert!(matches!(points, LonLatPointRows::CompactColumns { .. }));
        let mut visited = Vec::new();
        points.for_each_present(
            |row| array.is_row_missing(row),
            |row, point| visited.push((row, point)),
        );
        assert_eq!(visited.len(), 1);
        let (row, point) = *visited.first().expect("one point remains present");
        assert_eq!(row, 1);
        assert!((point.x - 8.628).abs() < 1e-10);
        assert!((point.y - 47.366).abs() < 1e-10);
    }

    #[test]
    fn compact_projected_transform_failure_maps_present_rank_to_logical_row() {
        let present = packed_xy(
            vec![500_000.0, 1e300],
            vec![0.0, 1e300],
            Frame::Crs(crs_arc_static("EPSG:32660")),
        );
        let array = PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_vec(4, vec![true, false, true, false]).expect("mask"),
        )
        .unwrap();

        assert_transform_error_row(&array, 3);
    }

    #[test]
    fn mixed_point_transform_failure_maps_present_rank_to_logical_row() {
        let array = PyGeometryArray::mixed_shapes(
            vec![
                Shape::Point(Point::new_raw_xy(10.0, 20.0)),
                Shape::Point(Point::new_raw_xy(500_000.0, 0.0)),
                Shape::Point(Point::new_raw_xy(30.0, 40.0)),
                Shape::Point(Point::new_raw_xy(1e300, 1e300)),
            ],
            Frame::Crs(crs_arc_static("EPSG:32660")),
        )
        .unwrap()
        .with_missing_mask(Some(
            ValidityMask::from_vec(4, vec![true, false, true, false]).expect("mask"),
        ))
        .unwrap();
        assert!(matches!(array.storage(), GeometryArrayStorage::Mixed(_)));

        assert_transform_error_row(&array, 3);
    }

    #[test]
    fn projected_transform_plan_failure_remains_setup() {
        let unavailable = "urn:example:unavailable";
        let array = packed_xy(
            vec![1.0],
            vec![2.0],
            Frame::Crs(crs_arc_static(unavailable)),
        );

        assert!(matches!(
            lonlat_points(&array),
            Err(LonLatPointsError::Setup(_))
        ));
    }

    #[test]
    fn domain_error_carries_logical_row() {
        let array = packed_xy(vec![0.0, 200.0], vec![0.0, 0.0], Frame::None);
        match lonlat_points(&array) {
            Err(LonLatPointsError::Row(row, error)) => {
                assert_eq!(row, 1);
                assert!(error.to_string().contains("invalid longitude/latitude"));
            },
            Err(LonLatPointsError::Type) => panic!("expected Row(1), got Type"),
            Err(LonLatPointsError::Setup(error)) => {
                panic!("expected Row(1), got Setup({error})")
            },
            Ok(_) => panic!("expected Row(1), got Ok"),
        }
    }

    #[test]
    fn compact_missing_domain_error_uses_logical_index() {
        let present = packed_xy(vec![0.0, 200.0], vec![0.0, 0.0], Frame::None);
        let mask = ValidityMask::from_vec(4, vec![true, false, true, false]).expect("mask");
        let array = PyGeometryArray::scatter_present_rows(&present, mask).unwrap();
        match lonlat_points(&array) {
            Err(LonLatPointsError::Row(row, _)) => assert_eq!(row, 3),
            Err(LonLatPointsError::Type) => panic!("expected Row(3), got Type"),
            Err(LonLatPointsError::Setup(error)) => {
                panic!("expected Row(3), got Setup({error})")
            },
            Ok(_) => panic!("expected Row(3), got Ok"),
        }
    }

    #[test]
    fn empty_projected_array_borrows_source_without_transform() {
        let array = packed_xy(
            Vec::new(),
            Vec::new(),
            Frame::Crs(crs_arc_static("EPSG:3857")),
        );
        let points = lonlat_points(&array).expect("empty projected array skips transform");
        assert!(matches!(points, LonLatPointRows::Source(_)));
        assert_eq!(points.len(), 0);
    }

    #[test]
    fn compact_projected_all_missing_skips_transform() {
        // Same unavailable label Frame tests already store without a PROJ
        // query. TransformPlan::new to EPSG:4326 fails; EPSG:3857 would
        // succeed even on empty buffers, so Ok+len would not prove a skip.
        let unavailable = "urn:example:unavailable";
        let present = packed_xy(
            Vec::new(),
            Vec::new(),
            Frame::Crs(crs_arc_static(unavailable)),
        );
        let mask = ValidityMask::from_vec(2, vec![true, true]).expect("mask");
        let array = PyGeometryArray::scatter_present_rows(&present, mask).unwrap();
        assert!(
            crate::crs::TransformPlan::new(unavailable, "EPSG:4326").is_err(),
            "counterfactual: building the plan must fail"
        );
        let points =
            lonlat_points(&array).expect("all-missing compact must not construct TransformPlan");
        assert!(matches!(points, LonLatPointRows::CompactColumns { .. }));
        assert_eq!(points.len(), 2);
    }

    #[test]
    fn non_point_array_is_type_error() {
        let line = parse_wkt("LINESTRING (0 0, 1 1)").expect("line parses");
        let array = PyGeometryArray::from_shapes(vec![line], Frame::None).unwrap();
        assert!(matches!(
            lonlat_points(&array),
            Err(LonLatPointsError::Type)
        ));
    }
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

#[cfg(test)]
mod minimum_clearance_computed_edge_tests {
    use geographiclib_rs::InverseGeodesic as _;

    use super::{metric_minimum_clearance_line, metric_shortest_line_resolved};
    use crate::geographic::GeographicFrame;
    use crate::geometry::{
        CoordSeq, LineSeq, MultiPoint, Point, PreparedPlan, Shape, SourceArcContext, TopologyDomain,
    };

    #[test]
    fn minimum_clearance_line_across_the_seam_uses_a_short_connector() {
        let points = [
            Point::new_raw_xy(179.8, 0.0),
            Point::new_raw_xy(-179.8, 0.0),
        ];
        let shape = Shape::MultiPoint(
            MultiPoint::from_coordseq(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        );
        let model = crate::crs::metric_model(Some("EPSG:4326")).unwrap();
        let output = metric_minimum_clearance_line(&model, Some("EPSG:4326"), &shape).unwrap();
        let Shape::LineString(line) = output else {
            panic!("minimum clearance publishes a line");
        };
        let coordinates = line.as_coords().iter().collect::<Vec<_>>();
        assert_eq!(coordinates.len(), 2);
        assert!(
            coordinates
                .iter()
                .any(|point| point.x.to_bits() == 179.8_f64.to_bits())
        );
        assert!(
            coordinates
                .iter()
                .any(|point| point.x.to_bits() == (-179.8_f64).to_bits())
        );
        let axis = GeographicFrame::crs84().longitude();
        let step = axis
            .chart_step(coordinates[0].x, coordinates[1].x)
            .unwrap()
            .delta(axis);
        assert!(step.abs() < 1.0, "seam connector step is {step}");
    }

    #[test]
    fn minimum_clearance_line_refined_support_uses_original_root_ordinates() {
        fn chart_length(
            geodesic: &geographiclib_rs::Geodesic,
            start: Point,
            end: Point,
            stop: f64,
        ) -> f64 {
            let pieces = 4096_u32;
            let mut length = 0.0;
            let mut previous = start;
            for step in 1..=pieces {
                let parameter = stop * f64::from(step) / f64::from(pieces);
                let next = Point::new_raw_xy(
                    start.x + parameter * (end.x - start.x),
                    start.y + parameter * (end.y - start.y),
                );
                let segment_length: f64 = geodesic.inverse(previous.y, previous.x, next.y, next.x);
                length += segment_length;
                previous = next;
            }
            length
        }

        let root_start = Point::new_raw_axes(
            0.0,
            0.0,
            crate::geometry::ZOrdinate(Some(10.0)),
            crate::geometry::MOrdinate(Some(100.0)),
        );
        let root_end = Point::new_raw_axes(
            1.0,
            1.0,
            crate::geometry::ZOrdinate(Some(210.0)),
            crate::geometry::MOrdinate(Some(500.0)),
        );
        let near_start = Point::new_raw_axes(
            0.5,
            0.5001,
            crate::geometry::ZOrdinate(Some(110.0)),
            crate::geometry::MOrdinate(Some(300.0)),
        );
        let near_end = Point::new_raw_axes(
            0.5002,
            0.5003,
            crate::geometry::ZOrdinate(Some(111.0)),
            crate::geometry::MOrdinate(Some(301.0)),
        );
        let line = |points: [Point; 2]| {
            Shape::LineString(
                LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            )
        };
        let shape = Shape::geometry_collection(vec![
            line([root_start, root_end]),
            line([near_start, near_end]),
        ])
        .unwrap();
        let model = crate::crs::metric_model(Some("OGC:CRS84")).unwrap();
        let output = metric_minimum_clearance_line(&model, Some("OGC:CRS84"), &shape).unwrap();
        let Shape::LineString(witness) = output else {
            panic!("minimum clearance publishes a line");
        };
        let coordinates = witness.as_coords().iter().collect::<Vec<_>>();
        let foot = coordinates[1];
        let parameter = (foot.x - root_start.x) / (root_end.x - root_start.x);
        let geodesic = geographiclib_rs::Geodesic::wgs84();
        let fraction = chart_length(&geodesic, root_start, root_end, parameter)
            / chart_length(&geodesic, root_start, root_end, 1.0);

        assert!((foot.z().unwrap() - (10.0 + 200.0 * fraction)).abs() < 1.0e-8);
        assert!((foot.m().unwrap() - (100.0 + 400.0 * fraction)).abs() < 1.0e-8);
    }

    #[test]
    fn shortest_line_from_geodesic_point_witnesses_uses_its_stored_short_step() {
        let frame = GeographicFrame::crs84();
        let model = crate::crs::metric_model(Some("EPSG:4326")).unwrap();
        let metric = crate::crs::ResolvedMetric::from_model(&model).unwrap();
        let source_context = SourceArcContext::cartesian();
        let left = PreparedPlan::new(Shape::Point(Point::new_raw_xy(179.8, 0.0)));
        let right = PreparedPlan::new(Shape::Point(Point::new_raw_xy(-179.8, 1.0)));
        let output = metric_shortest_line_resolved(
            &metric,
            &left,
            &right,
            TopologyDomain::Geographic(frame),
            &source_context,
        )
        .unwrap();
        let Shape::LineString(line) = output else {
            panic!("shortest_line publishes a line");
        };
        let coordinates = line.as_coords().iter().collect::<Vec<_>>();
        assert_eq!(coordinates.len(), 2);
        assert_eq!(coordinates[0].x, 179.8);
        assert_eq!(coordinates[1].x, -179.8);
        let axis = frame.longitude();
        let step = axis
            .chart_step(coordinates[0].x, coordinates[1].x)
            .unwrap()
            .delta(axis);
        assert!(step.abs() < 1.0, "seam connector step is {step}");
    }

    #[test]
    fn shortest_line_from_chart_edge_witness_retains_its_chart_step() {
        let frame = GeographicFrame::crs84();
        let model = crate::crs::metric_model(Some("EPSG:4326")).unwrap();
        let metric = crate::crs::ResolvedMetric::from_model(&model).unwrap();
        let source_context = SourceArcContext::cartesian();
        let left = PreparedPlan::new(Shape::Point(Point::new_raw_xy(179.8, 0.0)));
        let line = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(-179.8, -1.0),
                    Point::new_raw_xy(-179.8, 1.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let right = PreparedPlan::new(line);
        let output = metric_shortest_line_resolved(
            &metric,
            &left,
            &right,
            TopologyDomain::Geographic(frame),
            &source_context,
        )
        .unwrap();
        let Shape::LineString(line) = output else {
            panic!("shortest_line publishes a line");
        };
        let coordinates = line.as_coords().iter().collect::<Vec<_>>();
        assert_eq!(coordinates.len(), 2);
        assert_eq!(coordinates[0].x, 179.8);
        assert_eq!(coordinates[1].x, -179.8);
        assert!(coordinates[1].y.abs() < 1e-12);
        let axis = frame.longitude();
        let step = axis
            .chart_step(coordinates[0].x, coordinates[1].x)
            .unwrap()
            .delta(axis);
        assert!(step.abs() < 1.0, "seam connector step is {step}");
    }
}

#[cfg(test)]
mod planar_nearest_source_ordinate_tests {
    use super::{metric_nearest_points_resolved, metric_shortest_line_resolved};
    use crate::crs::{GeodesicMetric, MetricModel, ResolvedMetric};
    use crate::geometry::{
        ChartMetric as _, FrameDependentCaches, LineSeq, Point, PreparedPlan, RootWorkCounters,
        Shape, SourceArcContext, TopologyDomain, reset_root_work_counters, root_work_counters,
    };

    const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";

    fn segmented_length(metric: &GeodesicMetric, start: Point, end: Point) -> f64 {
        (0..8)
            .zip(1..=8)
            .map(|(start_index, end_index)| {
                let start_parameter = f64::from(start_index) / 8.0;
                let end_parameter = f64::from(end_index) / 8.0;
                let start_point = Point::new_raw_xy(
                    start.x + (end.x - start.x) * start_parameter,
                    start.y + (end.y - start.y) * start_parameter,
                );
                let end_point = Point::new_raw_xy(
                    start.x + (end.x - start.x) * end_parameter,
                    start.y + (end.y - start.y) * end_parameter,
                );
                let edge = metric.chart_edge(start_point, end_point).unwrap();
                metric.chart_edge_length(&edge).unwrap()
            })
            .sum()
    }

    fn assert_native_samples_resolved_once(line: &LineSeq) {
        let expected_samples = line
            .as_coords()
            .iter()
            .count()
            .checked_sub(2)
            .expect("shortest-line output retains both witness endpoints");
        let work = root_work_counters();
        assert_eq!(work.native_chain_entries, 1);
        assert_eq!(work.source_arc_requests, expected_samples);
        assert_eq!(work.source_arc_resolutions, expected_samples);
        assert_eq!(work.root_requests, 0);
        assert_eq!(work.root_ranges, 0);
        assert_eq!(work.trace_rows_builders, 0);
        assert_eq!(work.request_tables, 0);
        assert_eq!(work.root_catalog_resolutions, 0);
    }

    #[test]
    fn planar_nearest_witness_keeps_xy_and_resolves_source_root_ordinates() {
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let metric_model = crate::crs::metric_model_at(Some(CRS), &state).unwrap();
        let metric = GeodesicMetric::from_model(&metric_model).unwrap();
        let frame = metric.frame;
        let source = Shape::LineString(
            LineSeq::segment(
                Point::new_axes(
                    0.0,
                    0.0,
                    crate::geometry::ZOrdinate(Some(10.0)),
                    crate::geometry::MOrdinate(Some(100.0)),
                )
                .unwrap(),
                Point::new_axes(
                    120.0,
                    60.0,
                    crate::geometry::ZOrdinate(Some(210.0)),
                    crate::geometry::MOrdinate(Some(500.0)),
                )
                .unwrap(),
            )
            .unwrap(),
        );
        let query = Shape::Point(
            Point::new_axes(
                60.0,
                31.0,
                crate::geometry::ZOrdinate(Some(7.0)),
                crate::geometry::MOrdinate(Some(9.0)),
            )
            .unwrap(),
        );
        let planar = ResolvedMetric::from_model(&MetricModel::COORDINATE).unwrap();
        let source_plan = PreparedPlan::new(source);
        let query_plan = PreparedPlan::new(query);
        let metric_access = || Ok(&metric);
        let source_context =
            SourceArcContext::new(TopologyDomain::Geographic(frame), Some(&metric_access));

        let (left, right) = metric_nearest_points_resolved(
            &planar,
            &source_plan,
            &FrameDependentCaches::default(),
            &query_plan,
            &FrameDependentCaches::default(),
            Some(&source_context),
        )
        .unwrap()
        .unwrap();

        assert_eq!((left.x, left.y), (60.4, 30.2));
        assert_eq!((right.x, right.y), (60.0, 31.0));
        let origin = Point::new_raw_xy(0.0, 0.0);
        let fraction = segmented_length(&metric, origin, Point::new_raw_xy(left.x, left.y))
            / segmented_length(&metric, origin, Point::new_raw_xy(120.0, 60.0));
        assert!((left.z().unwrap() - (10.0 + 200.0 * fraction)).abs() < 1.0e-9);
        assert!((left.m().unwrap() - (100.0 + 400.0 * fraction)).abs() < 1.0e-9);
        assert_eq!(right.z(), Some(7.0));
        assert_eq!(right.m(), Some(9.0));
    }

    #[test]
    fn shortest_line_samples_follow_the_retained_half_and_full_turn_chords() {
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let model = crate::crs::metric_model_at(Some(CRS), &state).unwrap();
        let metric = GeodesicMetric::from_model(&model).unwrap();
        let frame = metric.frame;
        let geodesic = ResolvedMetric::from_model(&model).unwrap();
        let planar = ResolvedMetric::from_model(&MetricModel::COORDINATE).unwrap();
        let metric_access = || Ok(&metric);
        let source_context =
            SourceArcContext::new(TopologyDomain::Geographic(frame), Some(&metric_access));

        let half_turn_start = Point::new_axes(
            0.0,
            0.0,
            crate::geometry::ZOrdinate(Some(10.0)),
            crate::geometry::MOrdinate(Some(100.0)),
        )
        .unwrap();
        let half_turn_end = Point::new_axes(
            180.0,
            60.0,
            crate::geometry::ZOrdinate(Some(210.0)),
            crate::geometry::MOrdinate(Some(500.0)),
        )
        .unwrap();
        let left = PreparedPlan::new(Shape::Point(half_turn_start));
        let right = PreparedPlan::new(Shape::Point(half_turn_end));
        let origin = Point::new_raw_xy(0.0, 0.0);
        let half_turn_total = segmented_length(&metric, origin, Point::new_raw_xy(180.0, 60.0));
        for resolved in [&planar, &geodesic] {
            reset_root_work_counters();
            let Shape::LineString(line) = metric_shortest_line_resolved(
                resolved,
                &left,
                &right,
                TopologyDomain::Geographic(frame),
                &source_context,
            )
            .unwrap() else {
                panic!("shortest_line publishes a line");
            };
            assert_native_samples_resolved_once(&line);
            let sample = line
                .as_coords()
                .iter()
                .find(|point| {
                    point.x.abs().to_bits() == 90.0_f64.to_bits()
                        && point.y.to_bits() == 30.0_f64.to_bits()
                })
                .expect("the half-turn producer chord has a midpoint");
            let producer_midpoint = Point::new_raw_xy(90.0, 30.0);
            let fraction = segmented_length(&metric, origin, producer_midpoint) / half_turn_total;
            assert!((sample.z().unwrap() - (10.0 + 200.0 * fraction)).abs() < 1.0e-9);
            assert!((sample.m().unwrap() - (100.0 + 400.0 * fraction)).abs() < 1.0e-9);
        }

        let full_turn_start = Point::new_axes(
            0.0,
            1.0,
            crate::geometry::ZOrdinate(Some(10.0)),
            crate::geometry::MOrdinate(Some(100.0)),
        )
        .unwrap();
        let full_turn_end = Point::new_axes(
            360.0,
            60.0,
            crate::geometry::ZOrdinate(Some(210.0)),
            crate::geometry::MOrdinate(Some(500.0)),
        )
        .unwrap();
        let left = PreparedPlan::new(Shape::Point(full_turn_start));
        let right = PreparedPlan::new(Shape::Point(full_turn_end));
        let origin = Point::new_raw_xy(0.0, 1.0);
        let total = segmented_length(&metric, origin, Point::new_raw_xy(360.0, 60.0));
        reset_root_work_counters();
        let Shape::LineString(line) = metric_shortest_line_resolved(
            &planar,
            &left,
            &right,
            TopologyDomain::Geographic(frame),
            &source_context,
        )
        .unwrap() else {
            panic!("shortest_line publishes a line");
        };
        assert_native_samples_resolved_once(&line);
        for (index, parameter) in [0.25, 0.5, 0.75].into_iter().enumerate() {
            let y = 1.0 + 59.0 * parameter;
            let sample = line
                .as_coords()
                .iter()
                .find(|point| (point.y - y).abs() < 1.0e-12)
                .unwrap_or_else(|| panic!("missing full-turn sample at index {index}"));
            let producer_sample = Point::new_raw_xy(360.0 * parameter, 1.0 + 59.0 * parameter);
            let fraction = segmented_length(&metric, origin, producer_sample) / total;
            assert!((sample.z().unwrap() - (10.0 + 200.0 * fraction)).abs() < 1.0e-9);
            assert!((sample.m().unwrap() - (100.0 + 400.0 * fraction)).abs() < 1.0e-9);
        }
    }

    #[test]
    fn geographic_xy_planar_nearest_points_skip_source_root_capture() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        let planar = ResolvedMetric::from_model(&MetricModel::COORDINATE).unwrap();
        let line = PreparedPlan::new(Shape::LineString(
            LineSeq::segment(Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(120.0, 60.0)).unwrap(),
        ));
        let point = PreparedPlan::new(Shape::Point(Point::new_raw_xy(60.0, 31.0)));

        for (left, right, expected_left, expected_right) in [
            (&line, &point, (60.4, 30.2), (60.0, 31.0)),
            (&point, &line, (60.0, 31.0), (60.4, 30.2)),
        ] {
            reset_root_work_counters();
            let pair = metric_nearest_points_resolved(
                &planar,
                left,
                &FrameDependentCaches::default(),
                right,
                &FrameDependentCaches::default(),
                None,
            )
            .unwrap()
            .expect("nonempty XY nearest pair");
            assert!((pair.0.x - expected_left.0).abs() < 1.0e-12);
            assert!((pair.0.y - expected_left.1).abs() < 1.0e-12);
            assert!((pair.1.x - expected_right.0).abs() < 1.0e-12);
            assert!((pair.1.y - expected_right.1).abs() < 1.0e-12);
            assert_eq!(pair.0.axes(), crate::geometry::CoordinateAxes::XY);
            assert_eq!(pair.1.axes(), crate::geometry::CoordinateAxes::XY);
            assert_eq!(root_work_counters(), RootWorkCounters::default());
        }
    }

    #[test]
    fn geographic_xy_shortest_line_skips_source_root_capture() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let model = crate::crs::metric_model_at(Some(CRS), &state).unwrap();
        let metric = GeodesicMetric::from_model(&model).unwrap();
        let geodesic = ResolvedMetric::from_model(&model).unwrap();
        let planar = ResolvedMetric::from_model(&MetricModel::COORDINATE).unwrap();
        let metric_access = || Ok(&metric);
        let context = SourceArcContext::new(
            TopologyDomain::Geographic(metric.frame),
            Some(&metric_access),
        );
        let left = PreparedPlan::new(Shape::Point(Point::new_raw_xy(0.0, 0.0)));
        let right = PreparedPlan::new(Shape::Point(Point::new_raw_xy(180.0, 60.0)));

        for unit in [&planar, &geodesic] {
            reset_root_work_counters();
            let line = metric_shortest_line_resolved(
                unit,
                &left,
                &right,
                TopologyDomain::Geographic(metric.frame),
                &context,
            )
            .unwrap();
            assert_eq!(line.axes(), crate::geometry::CoordinateAxes::XY);
            assert_eq!(root_work_counters(), RootWorkCounters::default());
        }
    }
}
