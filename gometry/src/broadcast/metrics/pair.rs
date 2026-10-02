use crate::array::{ArrayRow, PackedStorage, PointRows, PyGeometryArray};
use crate::boundary::geographic;
use crate::broadcast::metrics::{
    CoordinateAxes, DistanceUnit, GeometryArrayStorage, GeometryError, GeometryKind, Point,
    PyGeometry, PyResult, Shape, ShapeData, crs,
};
use crate::dispatch::MetricOp;
use crate::geographic::GeographicFrame;
use crate::geometry::{FrameDependentCaches, LineSeq, PreparedPlan, ShapeCountQuery};
use crate::py::support;
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
    crs::ensure_geographic_domain_in_frame(frame, shape)?;
    if shape.has_area_parts() {
        return Err(crs::CrsError::message(
            "geodesic distance for polygon interiors is unsupported",
        ));
    }
    Ok(())
}

pub(crate) fn ensure_hausdorff_geodesic_shape(
    frame: GeographicFrame,
    shape: &Shape,
) -> crate::error::Result<()> {
    crs::ensure_geographic_domain_in_frame(frame, shape)?;
    if shape.has_area_parts() {
        return Err(crs::CrsError::message(
            "geodesic distance for polygon interiors is unsupported",
        ));
    }
    Ok(())
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
    #[cfg(test)]
    binary_dispatch_counters::metric_context_acquisition();
    let operation = operation.name();
    match unit {
        Some(DistanceUnit::Planar) => Ok(crs::MetricModel::COORDINATE),
        Some(DistanceUnit::Meters) if crs.is_none() => Err(GeometryError::new_err(format!(
            "{operation} unit='meters' requires a CRS; CRS-free geometries have no meter scale — \
             use unit='planar' for raw coordinate units, or set a CRS"
        ))),
        Some(DistanceUnit::Meters) => Ok(crs::metric_model_meters(crs.expect("checked above"))?),
        None => Ok(crs::metric_model(crs)?),
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
    Columns { xs: Vec<f64>, ys: Vec<f64> },
}

impl LonLatPointRows<'_> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Source(points) => points.len(),
            Self::Columns { xs, .. } => xs.len(),
        }
    }

    pub(crate) fn get(&self, row: usize) -> Point {
        match self {
            Self::Source(points) => points.get(row),
            Self::Columns { xs, ys } => Point::new_raw_xy(
                *xs.get(row)
                    .expect("longitude columns cover every logical row"),
                *ys.get(row)
                    .expect("latitude columns cover every logical row"),
            ),
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
    plan.transform_coordinates(&mut px, &mut py, crate::Zt::None)
        .map_err(LonLatPointsError::Setup)?;
    for ((&row, x), y) in present.iter().zip(px).zip(py) {
        lonlat_domain(row, x, y)?;
        *xs.get_mut(row)
            .expect("present logical row is inside longitude columns") = x;
        *ys.get_mut(row)
            .expect("present logical row is inside latitude columns") = y;
    }
    Ok(())
}

fn compact_lonlat_points(
    array: &PyGeometryArray,
) -> Result<LonLatPointRows<'_>, LonLatPointsError> {
    let projected_crs = array.crs_str().filter(|crs| !crs::is_wgs84_lonlat(crs));
    let mut xs = Vec::with_capacity(array.len());
    let mut ys = Vec::with_capacity(array.len());
    let mut present = Vec::with_capacity(array.len());
    for logical_row in array
        .row_operation()
        .rows_with_missing(|| (f64::NAN, f64::NAN))
    {
        match logical_row {
            ArrayRow::Missing { value, .. } => {
                xs.push(value.0);
                ys.push(value.1);
            },
            ArrayRow::Present(row) => {
                let logical = row.logical();
                let crate::array::ShapeRow::Point(point) = row.row() else {
                    unreachable!("compact point storage yields point rows")
                };
                xs.push(point.x);
                ys.push(point.y);
                present.push(logical);
            },
        }
    }
    let Some(crs) = projected_crs else {
        for &logical in &present {
            lonlat_domain(
                logical,
                *xs.get(logical)
                    .expect("present logical row is inside longitude columns"),
                *ys.get(logical)
                    .expect("present logical row is inside latitude columns"),
            )?;
        }
        return Ok(LonLatPointRows::Columns { xs, ys });
    };
    if !present.is_empty() {
        transform_present(crs, &mut xs, &mut ys, &present)?;
    }
    Ok(LonLatPointRows::Columns { xs, ys })
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
/// Geodesic antimeridian prelude: the seam-split forms of a pair when a geodesic
/// metric meets a crossing geographic operand, else `None` (use the originals).
/// THE single place the geodesic metric family — distance, dwithin,
/// nearest_points, shortest_line — normalizes crossings, so the kernels cannot
/// drift (a witness or containment test on the unsplit 340° band lands on the
/// wrong edge). hausdorff/frechet deliberately opt out (their per-segment
/// coupling is already geodesic). Recursing on the split form terminates: it no
/// longer crosses.
pub(crate) fn geodesic_split_operands(
    metric: &crs::ResolvedMetric,
    a: &PreparedPlan,
    b: &PreparedPlan,
) -> crate::error::Result<Option<(PreparedPlan, PreparedPlan)>> {
    if let crs::ResolvedMetric::Geodesic(metric) = metric {
        return split_geodesic_operands(metric.frame, a, b);
    }
    Ok(None)
}

fn split_geodesic_operands(
    frame: GeographicFrame,
    a: &PreparedPlan,
    b: &PreparedPlan,
) -> crate::error::Result<Option<(PreparedPlan, PreparedPlan)>> {
    // Filled polygon interiors are not a geodesic linework operand. Leave
    // them untouched so the shared geometry guard reports that contract
    // instead of seam-splitting an unsupported shape first.
    if a.shape().has_area_parts() || b.shape().has_area_parts() {
        return Ok(None);
    }
    if !a.shape().is_empty() && !b.shape().is_empty() {
        // Cached verdicts; split ONLY the operand that actually crosses — the
        // other is passed through unchanged (no needless seam walk + rebuild).
        let a_crosses = crate::geometry::crosses_antimeridian_in_frame(a.shape(), frame);
        let b_crosses = crate::geometry::crosses_antimeridian_in_frame(b.shape(), frame);
        if a_crosses || b_crosses {
            let normalize = |x: &PreparedPlan,
                             crosses: bool|
             -> crate::error::Result<(PreparedPlan, bool)> {
                if !crosses {
                    return Ok((PreparedPlan::new(ShapeData::new(x.shape().clone())), false));
                }
                let normalized_shape = x.shape().split_antimeridian_in_frame(frame)?;
                let changed = normalized_shape != *x.shape()
                    && !crate::geometry::crosses_antimeridian_in_frame(&normalized_shape, frame);
                Ok((
                    PreparedPlan::new(ShapeData::from(if changed {
                        normalized_shape
                    } else {
                        x.shape().clone()
                    })),
                    changed,
                ))
            };
            let (a, a_changed) = normalize(a, a_crosses)?;
            let (b, b_changed) = normalize(b, b_crosses)?;
            if a_changed || b_changed {
                return Ok(Some((a, b)));
            }
        }
    }
    Ok(None)
}

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
    if let Some((a, b)) = split_geodesic_operands(metric.frame, a, b)? {
        return pair_distance_geodesic(
            metric,
            &a,
            &FrameDependentCaches::default(),
            &b,
            &FrameDependentCaches::default(),
        );
    }
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
    if let Some((a, b)) = split_geodesic_operands(metric.frame, a, b)? {
        return pair_dwithin_geodesic(
            metric,
            &a,
            &FrameDependentCaches::default(),
            &b,
            &FrameDependentCaches::default(),
            distance,
        );
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
            let rows = crate::array::line_logical_len(offsets.as_slice(), map);
            for logical in 0..rows {
                values.push(if map.csr_window(offsets.as_slice(), logical).is_empty() {
                    f64::INFINITY
                } else {
                    0.0
                });
            }
        },
        GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
            let rows = crate::array::point_logical_len(coords, row_map.as_deref());
            values.resize(rows, 0.0);
        },
        GeometryArrayStorage::Packed(PackedStorage::Polygons {
            ring_offsets,
            polygon_offsets,
            row_map,
            ..
        }) => {
            let map = row_map.as_deref();
            let rows = crate::array::polygon_logical_len(polygon_offsets.as_slice(), map);
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
                            crate::geometry::SegmentPlacement::Geodesic(&geodesic, *frame),
                            &mut budget,
                        )?,
                        b.densified_budgeted_with_placement(
                            fraction,
                            crate::geometry::SegmentPlacement::Geodesic(&geodesic, *frame),
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
) -> crate::error::Result<Option<(Point, Point)>> {
    if let Some((a, b)) = geodesic_split_operands(metric, a, b)? {
        return metric_nearest_points_resolved(
            metric,
            &a,
            &FrameDependentCaches::default(),
            &b,
            &FrameDependentCaches::default(),
        );
    }
    if a.shape().is_empty() || b.shape().is_empty() {
        return Ok(None);
    }
    dispatch_resolved_metric(
        metric,
        |_| a.nearest_points(b),
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
) -> crate::error::Result<Shape> {
    crate::geometry::nearest_line(
        metric_nearest_points_resolved(
            metric,
            a,
            &FrameDependentCaches::default(),
            b,
            &FrameDependentCaches::default(),
        )?
        .as_ref(),
        crate::geometry::common_axes(a.shape(), b.shape()),
    )
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
    dispatch_metric_model(
        model,
        |_| shape.minimum_clearance_line(),
        |_, _, state| {
            support::geodesic_by_identity(
                shape,
                crs,
                state,
                |projected| {
                    Ok(projected
                        .minimum_clearance_plan()?
                        .map_or_else(Vec::new, |witness| witness.plan.to_vec()))
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
            let line = metric_minimum_clearance_line(model, crs, shape)?;
            crs::geodesic_length_at(geodesic_crs, &line, state)
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
            let local = crs::estimate_local_crs_at(&[shape], source, state)?;
            crs::transform_at(shape, source, &local.definition, state)
                .map_err(|error| local.annotate_error(error))?
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
    use super::{LonLatPointRows, LonLatPointsError, lonlat_points};
    use crate::array::{PyGeometryArray, ValidityMask};
    use crate::boundary::{Frame, crs_arc_static};
    use crate::geometry::{CoordSeq, Point};
    use crate::io::parse_wkt;

    fn packed_xy(xs: Vec<f64>, ys: Vec<f64>, frame: Frame) -> PyGeometryArray {
        PyGeometryArray::packed_points(CoordSeq::try_from_vecs(xs, ys, None, None).unwrap(), frame)
            .unwrap()
    }

    #[test]
    fn dense_lonlat_borrows_source() {
        let array = packed_xy(vec![1.0, 2.0], vec![3.0, 4.0], Frame::None);
        let points = lonlat_points(&array).expect("in-domain packed points");
        assert!(matches!(points, LonLatPointRows::Source(_)));
        assert_eq!(points.len(), 2);
        assert_eq!(points.get(0), Point::new_raw_xy(1.0, 3.0));
        assert_eq!(points.get(1), Point::new_raw_xy(2.0, 4.0));
    }

    #[test]
    fn missing_row_is_not_validated_and_keeps_logical_index() {
        let present = packed_xy(vec![1.0], vec![2.0], Frame::None);
        let mask = ValidityMask::from_vec(3, vec![true, false, true]).expect("mask");
        let array = PyGeometryArray::scatter_present_rows(&present, mask).unwrap();
        assert!(array.is_compact_nullable());
        let points = lonlat_points(&array).expect("present row is in domain");
        assert!(matches!(points, LonLatPointRows::Columns { .. }));
        assert_eq!(points.len(), 3);
        assert_eq!(points.get(1), Point::new_raw_xy(1.0, 2.0));
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
        assert!(array.is_compact_nullable());
        assert!(
            crate::crs::TransformPlan::new(unavailable, "EPSG:4326").is_err(),
            "counterfactual: building the plan must fail"
        );
        let points =
            lonlat_points(&array).expect("all-missing compact must not construct TransformPlan");
        assert!(matches!(points, LonLatPointRows::Columns { .. }));
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
    use crate::geometry::{GeometryErrorKind, Point, Shape};

    #[test]
    fn domain_error_is_native_result() {
        let shape = Shape::Point(Point::new_raw_xy(0.0, 95.0));
        let error = lonlat_shape_under(&shape, None).expect_err("out of domain");
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
        let message = error.to_string();
        assert!(
            message.contains("invalid longitude/latitude (0, 95)"),
            "{message}"
        );
    }

    #[test]
    fn nad83_transform_then_domain_is_native_geometry_error() {
        let shape = Shape::Point(Point::new_raw_xy(0.0, 95.0));
        let transformed = crate::crs::transform(&shape, "EPSG:4269", "EPSG:4326")
            .expect("NAD83 to WGS84 must execute for (0, 95)");
        let Shape::Point(point) = &transformed else {
            panic!("NAD83 point transform must yield a point, got {transformed:?}");
        };
        crate::validate_lonlat_shape(&transformed)
            .expect_err("transformed (0, 95) must fail native domain");
        let error = lonlat_shape_under(&shape, Some("EPSG:4269"))
            .expect_err("owned-Cow path must fail domain after transform");
        assert!(
            matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
            ),
            "expected domain Invalid after a successful transform, got {error}"
        );
        let expected = format!("invalid longitude/latitude ({}, {})", point.x, point.y);
        let message = error.to_string();
        assert!(message.contains(&expected), "{message}");
    }
}
