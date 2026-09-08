#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::types::PyAny;

use crate::array::{ArrayRow, PackedStorage, PointRows, PyGeometryArray};
use crate::broadcast::metrics::{
    Bound, CoordinateAxes, Crs, DistanceUnit, GeometryArrayStorage, GeometryError, GeometryInput,
    GeometryKind, Point, PyAnyMethods as _, PyGeometry, PyResult, Shape, ShapeData, classify_input,
    crs, geometry_type_err,
};
use crate::dispatch::MetricOp;
use crate::geographic::GeographicFrame;
use crate::geometry::{FrameDependentCaches, LineSeq, PreparedPlan};
use crate::py::errors::InvalidGeometryError;
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
    crs::ensure_geographic_domain_in_frame(frame, a.shape())?;
    crs::ensure_geographic_domain_in_frame(frame, b.shape())?;
    let metric = crs::EllipsoidMetric::for_geodesic_at_frame(geodesic, runtime_generation, frame);
    let (semi_major, flattening) = metric.ellipsoid_parameters();
    Ok((metric, semi_major, flattening))
}

/// Densify-refined metric over two shapes — the shared driver behind Hausdorff
/// and Fréchet (identical apart from the kernel).
fn metric_densified(
    model: &crs::MetricModel,
    a: &Shape,
    b: &Shape,
    densify: Option<f64>,
    kernel: impl Fn(&crs::MetricModel, &Shape, &Shape) -> crate::error::Result<f64>,
) -> crate::error::Result<f64> {
    densify.map_or_else(
        || kernel(model, a, b),
        |fraction| match model {
            crs::MetricModel::Planar { .. } => {
                let a = a.densified(fraction)?;
                let b = b.densified(fraction)?;
                kernel(model, &a, &b)
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
                let mut budget = crate::geometry::ExpansionBudget::new("densify", "fraction");
                let a = a.densified_budgeted_with_placement(
                    fraction,
                    crate::geometry::SegmentPlacement::Geodesic(&geodesic, *frame),
                    &mut budget,
                )?;
                let b = b.densified_budgeted_with_placement(
                    fraction,
                    crate::geometry::SegmentPlacement::Geodesic(&geodesic, *frame),
                    &mut budget,
                )?;
                kernel(model, &a, &b)
            },
        },
    )
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
///   projected native units / CRS-free coordinate units).
///
/// This is the single seam every metric op shares (the spatial index included),
/// so `unit` semantics cannot drift between operations.
pub(crate) fn resolve_metric(
    crs: Option<&str>,
    unit: Option<DistanceUnit>,
    operation: MetricOp,
) -> PyResult<crs::MetricModel> {
    let operation = operation.name();
    match unit {
        Some(DistanceUnit::Planar) => Ok(crs::MetricModel::COORDINATE),
        Some(DistanceUnit::Meters) if crs.is_none() => Err(GeometryError::new_err(format!(
            "{operation} unit='meters' requires a CRS; CRS-free geometries have no meter scale — \
             use unit='planar' for coordinate units, or set a CRS"
        ))),
        Some(DistanceUnit::Meters) => Ok(crs::metric_model_meters(crs.expect("checked above"))?),
        None => Ok(crs::metric_model(crs)?),
    }
}

/// [`resolve_metric`]'s 3D sibling, for `distance_3d`/`length_3d`.
///
/// The unit policy is identical: omitted reports the CRS's own linear unit,
/// `unit='meters'` scales by the axis factor, `unit='planar'` is raw coordinate
/// units. The one divergence is what a geographic CRS means. 2D measures it
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
    let operation = operation.name();
    // Validate 3D eligibility first, so an ineligible CRS reports the
    // dimensional problem rather than a unit one, whatever `unit=` says.
    let to_metre = crs::ensure_3d_metric(crs)?;
    match unit {
        None | Some(DistanceUnit::Planar) => Ok(crs::MetricModel::COORDINATE),
        Some(DistanceUnit::Meters) if crs.is_none() => Err(GeometryError::new_err(format!(
            "{operation} unit='meters' requires a CRS; CRS-free geometries have no meter scale — \
             use unit='planar' for coordinate units, or set a CRS"
        ))),
        Some(DistanceUnit::Meters) => Ok(crs::MetricModel::Planar { to_metre }),
    }
}

/// Resolve a shape to validated WGS84 lon/lat under its frame: CRS-free and
/// WGS84 inputs borrow straight through, any other CRS reprojects. The
/// shape-level seam behind [`lonlat_shape`] — array lanes call it row by row
/// without materializing wrappers.
pub(crate) fn lonlat_shape_under<'a>(
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

/// Present rows admitted to WGS84 lon/lat, skipping empties. The current input
/// `Cow`, transform `Cow`, and accumulator drop before the caller converts
/// [`crate::error::Error`] to `PyErr`.
pub(crate) fn lonlat_nonempty_shapes(array: &PyGeometryArray) -> crate::error::Result<Vec<Shape>> {
    let crs = array.crs_str();
    let mut out = Vec::new();
    for row in array.present_shape_rows() {
        let (_, input) = row?;
        let resolved = lonlat_shape_under(&input, crs)?;
        if resolved.is_empty() {
            continue;
        }
        let owned = match resolved {
            std::borrow::Cow::Owned(shape) => shape,
            std::borrow::Cow::Borrowed(_) => input.into_owned(),
        };
        crate::try_push_vec(&mut out, owned)?;
    }
    Ok(out)
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
            Self::Columns { xs, ys } => Point::new_raw_xy(xs[row], ys[row]),
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
    crate::boundary::geographic::validate_lonlat_xy(x, y)
        .map_err(|error| LonLatPointsError::Row(row, error))
}

fn transform_present(
    crs: &str,
    xs: &mut [f64],
    ys: &mut [f64],
    present: &[usize],
) -> Result<(), LonLatPointsError> {
    let mut px = Vec::new();
    let mut py = Vec::new();
    crate::try_reserve_pair(&mut px, &mut py, present.len()).map_err(LonLatPointsError::Setup)?;
    for &row in present {
        crate::try_push_pair(&mut px, &mut py, xs[row], ys[row])
            .map_err(LonLatPointsError::Setup)?;
    }
    let plan =
        crate::crs::TransformPlan::new(crs, "EPSG:4326").map_err(LonLatPointsError::Setup)?;
    plan.transform_coordinates(&mut px, &mut py, crate::Zt::None)
        .map_err(LonLatPointsError::Setup)?;
    for (index, &row) in present.iter().enumerate() {
        lonlat_domain(row, px[index], py[index])?;
        xs[row] = px[index];
        ys[row] = py[index];
    }
    Ok(())
}

fn compact_lonlat_points(
    array: &PyGeometryArray,
) -> Result<LonLatPointRows<'_>, LonLatPointsError> {
    let projected_crs = array.crs_str().filter(|crs| !crs::is_wgs84_lonlat(crs));
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    let mut present = Vec::new();
    crate::try_reserve_pair(&mut xs, &mut ys, array.len()).map_err(LonLatPointsError::Setup)?;
    crate::try_reserve_vec(&mut present, array.len()).map_err(LonLatPointsError::Setup)?;
    for logical_row in array
        .row_operation()
        .rows_with_missing(|| (f64::NAN, f64::NAN))
    {
        match logical_row {
            ArrayRow::Missing { value, .. } => {
                crate::try_push_pair(&mut xs, &mut ys, value.0, value.1)
                    .map_err(LonLatPointsError::Setup)?;
            },
            ArrayRow::Present(row) => {
                let logical = row.logical();
                let crate::array::ShapeRow::Point(point) = row.row() else {
                    unreachable!("compact point storage yields point rows")
                };
                crate::try_push_pair(&mut xs, &mut ys, point.x, point.y)
                    .map_err(LonLatPointsError::Setup)?;
                crate::try_push_vec(&mut present, logical).map_err(LonLatPointsError::Setup)?;
            },
        }
    }
    let Some(crs) = projected_crs else {
        for &logical in &present {
            lonlat_domain(logical, xs[logical], ys[logical])?;
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
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    crate::try_reserve_pair(&mut xs, &mut ys, points.len()).map_err(LonLatPointsError::Setup)?;
    for point in points.iter() {
        crate::try_push_pair(&mut xs, &mut ys, point.x, point.y)
            .map_err(LonLatPointsError::Setup)?;
    }
    let mut present = Vec::new();
    crate::try_reserve_vec(&mut present, points.len()).map_err(LonLatPointsError::Setup)?;
    for row in 0..points.len() {
        if !array.is_row_missing(row) {
            crate::try_push_vec(&mut present, row).map_err(LonLatPointsError::Setup)?;
        }
    }
    if !present.is_empty() {
        transform_present(crs, &mut xs, &mut ys, &present)?;
    }
    Ok(LonLatPointRows::Columns { xs, ys })
}

/// Extract the single point of a `Point` geometry, or error.
pub(crate) fn require_point(geometry: &PyGeometry, operation: MetricOp) -> PyResult<Point> {
    let operation = operation.name();
    match geometry.shape.shape() {
        Shape::Point(point) => Ok(*point),
        Shape::Empty(GeometryKind::Point, _) => Err(InvalidGeometryError::new_err(format!(
            "{operation} requires a non-empty Point"
        ))),
        _ => Err(geometry_type_err(format!("{operation} requires a Point"))),
    }
}

/// CRS-aware minimum distance: planar coordinate units for a projected/CRS-free
/// geometry, geodesic meters on the CRS ellipsoid (between the closest pair of
/// points) for a geographic one.
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
    if contains_filled_area(a.shape()) || contains_filled_area(b.shape()) {
        return Ok(None);
    }
    if !a.shape().is_empty() && !b.shape().is_empty() {
        // Cached verdicts; split ONLY the operand that actually crosses — the
        // other is passed through unchanged (no needless seam walk + rebuild).
        let a_crosses = crate::geometry::crosses_antimeridian_in_frame(a.shape(), frame);
        let b_crosses = crate::geometry::crosses_antimeridian_in_frame(b.shape(), frame);
        if a_crosses || b_crosses {
            let normalize =
                |x: &PreparedPlan, crosses: bool| -> crate::error::Result<PreparedPlan> {
                    Ok(if crosses {
                        PreparedPlan::new(ShapeData::from(
                            x.shape().split_antimeridian_in_frame(frame)?,
                        ))
                    } else {
                        PreparedPlan::new(ShapeData::new(x.shape().clone()))
                    })
                };
            return Ok(Some((normalize(a, a_crosses)?, normalize(b, b_crosses)?)));
        }
    }
    Ok(None)
}

fn contains_filled_area(shape: &Shape) -> bool {
    match shape {
        Shape::Polygon(_) | Shape::MultiPolygon(_) => true,
        Shape::GeometryCollection(geometries) => geometries.iter().any(contains_filled_area),
        _ => false,
    }
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
    let distance = NonNegative::try_new("distance", distance)?;
    Ok(a.dwithin(b, distance.get() / to_metre)?)
}

pub(crate) fn pair_dwithin_geodesic(
    metric: &crs::GeodesicMetric,
    a: &PreparedPlan,
    a_cache: &FrameDependentCaches,
    b: &PreparedPlan,
    b_cache: &FrameDependentCaches,
    distance: f64,
) -> crate::error::Result<bool> {
    let distance = NonNegative::try_new("distance", distance)?.get();
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
    if a.shape().is_empty() || b.shape().is_empty() {
        return Ok(false);
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
/// `Arc<GeometryArrayStorage>` — allocation-free for packed columns.
pub(crate) fn same_storage_similarity_metric_zeros(storage: &GeometryArrayStorage) -> Vec<f64> {
    match storage {
        GeometryArrayStorage::Packed(PackedStorage::Lines {
            offsets, row_map, ..
        }) => {
            let map = row_map.as_deref();
            let rows = crate::array::line_logical_len(offsets.as_slice(), map);
            (0..rows)
                .map(|logical| {
                    if map.csr_window(offsets.as_slice(), logical).is_empty() {
                        f64::INFINITY
                    } else {
                        0.0
                    }
                })
                .collect()
        },
        GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
            vec![0.0; crate::array::point_logical_len(coords, row_map.as_deref())]
        },
        GeometryArrayStorage::Packed(PackedStorage::Polygons {
            ring_offsets,
            polygon_offsets,
            row_map,
            ..
        }) => {
            let map = row_map.as_deref();
            let rows = crate::array::polygon_logical_len(polygon_offsets.as_slice(), map);
            (0..rows)
                .map(|logical| {
                    let rings = map.csr_window(polygon_offsets.as_slice(), logical);
                    if !rings.is_empty()
                        && (ring_offsets[rings.start] as usize) < (ring_offsets[rings.end] as usize)
                    {
                        0.0
                    } else {
                        f64::INFINITY
                    }
                })
                .collect()
        },
        GeometryArrayStorage::Mixed(shapes) => shapes
            .iter()
            .map(|shape| if shape.is_empty() { f64::INFINITY } else { 0.0 })
            .collect(),
    }
}

/// CRS-aware continuous Hausdorff distance: planar coordinate/native units under
/// `unit='planar'`/a planar frame, explicit SI meters under `unit='meters'`,
/// or true geodesic meters on a geographic CRS.
pub(crate) fn metric_hausdorff(
    model: &crs::MetricModel,
    a: &Shape,
    b: &Shape,
) -> crate::error::Result<f64> {
    // Domain validation BEFORE the identity exit: equal operands with
    // out-of-domain latitude must still raise on a geographic model.
    if let crs::MetricModel::Geodesic { frame, .. } = model {
        crs::ensure_geographic_domain_in_frame(*frame, a)?;
        crs::ensure_geographic_domain_in_frame(*frame, b)?;
    }
    require_nonempty_hausdorff(a)?;
    require_nonempty_hausdorff(b)?;
    if a == b {
        return Ok(0.0);
    }
    dispatch_metric_model(
        model,
        |to_metre| Ok(a.hausdorff_distance(b)? * to_metre),
        |crs, _, state| crs::geodesic_hausdorff_at(crs, a, b, state),
    )
}

pub(crate) fn require_nonempty_hausdorff(shape: &Shape) -> crate::error::Result<()> {
    if shape.is_empty() {
        return Err(crate::geometry::GeometryErrorKind::message(
            "hausdorff_distance requires non-empty geometries",
        ));
    }
    Ok(())
}

/// CRS-aware discrete Fréchet distance (planar/native or explicit SI, or
/// geodesic).
pub(crate) fn metric_frechet(
    model: &crs::MetricModel,
    a: &Shape,
    b: &Shape,
) -> crate::error::Result<f64> {
    let mut scratch = crate::geometry::FrechetDpScratch::default();
    metric_frechet_with_scratch(model, a, b, &mut scratch)
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
    dispatch_metric_model(
        model,
        |to_metre| Ok(a.frechet_distance_with_scratch(b, scratch)? * to_metre),
        |crs, _, state| crs::geodesic_frechet_at(crs, a, b, state),
    )
}

/// [`metric_hausdorff`] over `densify`-refined operands: each segment gains
/// evenly spaced vertices along the active metric curve before the continuous
/// Hausdorff kernel.
/// `None` preserves the operands' original segmentization; there is no
/// discrete Hausdorff lane.
pub(crate) fn metric_hausdorff_densified(
    model: &crs::MetricModel,
    a: &Shape,
    b: &Shape,
    densify: Option<f64>,
) -> crate::error::Result<f64> {
    metric_densified(model, a, b, densify, metric_hausdorff)
}

/// [`metric_frechet`] over `densify`-refined operands. Each segment gains
/// evenly spaced vertices before the discrete Fréchet sweep; `None` measures
/// the original vertices only.
pub(crate) fn metric_frechet_densified(
    model: &crs::MetricModel,
    a: &Shape,
    b: &Shape,
    densify: Option<f64>,
) -> crate::error::Result<f64> {
    metric_densified(model, a, b, densify, |model, a, b| {
        charge_frechet_dp(a, b)?;
        metric_frechet(model, a, b)
    })
}

/// The discrete Fréchet DP is `n · m` cells, so `densify` amplifies
/// quadratically: on a pair of two-vertex lines, `1e-5` is 10¹⁰ cells and
/// `1e-6` is 10¹². Charge the product so an unreachable answer is a typed
/// error naming the parameter to raise, rather than an uninterruptible wait.
/// Undensified operands are input-sized and stay uncharged.
fn charge_frechet_dp(a: &Shape, b: &Shape) -> crate::error::Result<()> {
    crate::geometry::ExpansionBudget::pairwise(
        "frechet_distance",
        "densify",
        a.coord_count(),
        b.coord_count(),
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
        None => metric_frechet_with_scratch(model, a, b, scratch),
        Some(fraction) => match model {
            crs::MetricModel::Planar { .. } => {
                let a = a.densified(fraction)?;
                let b = b.densified(fraction)?;
                charge_frechet_dp(&a, &b)?;
                metric_frechet_with_scratch(model, &a, &b, scratch)
            },
            crs::MetricModel::Geodesic { .. } => metric_frechet_densified(model, a, b, densify),
        },
    }
}

/// Boundary validation for the ``densify=`` fraction: finite and in
/// `(0, 1]`, or `None` for the plain vertex metric.
pub(crate) fn validate_densify(densify: Option<f64>) -> PyResult<Option<f64>> {
    if let Some(fraction) = densify
        && !(fraction.is_finite() && fraction > 0.0 && fraction <= 1.0)
    {
        return Err(crate::py::errors::GeometryError::new_err(format!(
            "densify must be in (0, 1], got {fraction}"
        )));
    }
    Ok(densify)
}

/// Optional per-row ``densify=`` lane for array Hausdorff/Fréchet (scalar
/// broadcast or one fraction per geometry).
#[derive(Clone, Debug)]
pub(crate) enum OptionalDensifyParam {
    None,
    Scalar(f64),
    PerElement(Box<[f64]>),
}

impl OptionalDensifyParam {
    pub(crate) fn parse(value: Option<&Bound<'_, PyAny>>, len: usize) -> PyResult<Self> {
        let Some(value) = value else {
            return Ok(Self::None);
        };
        if value.is_none() {
            return Ok(Self::None);
        }
        let param = crate::F64Param::parse(value, "densify", len)?;
        param.try_validate(|fraction| validate_densify(Some(fraction)).map(|_| ()))?;
        Ok(match param {
            crate::F64Param::Scalar(fraction) => Self::Scalar(fraction),
            crate::F64Param::PerElement(fractions) => Self::PerElement(fractions),
        })
    }

    pub(crate) const fn is_per_element(&self) -> bool {
        matches!(self, Self::PerElement(_))
    }

    pub(crate) const fn as_scalar_densify(&self) -> Option<f64> {
        match self {
            Self::Scalar(fraction) => Some(*fraction),
            Self::None | Self::PerElement(_) => None,
        }
    }

    pub(crate) fn at(&self, row: usize) -> Option<f64> {
        match self {
            Self::None => None,
            Self::Scalar(fraction) => Some(*fraction),
            Self::PerElement(fractions) => Some(fractions[row]),
        }
    }
}

/// CRS-aware nearest points (one per geometry). Returns coordinates in the
/// geometry's own frame — planar coordinates for a planar/projected frame,
/// lon/lat for a geographic one (the geodesic witness) — so the result is a
/// pair of points, not a scaled distance.
pub(crate) fn metric_nearest_points(
    model: &crs::MetricModel,
    a: &PreparedPlan,
    b: &PreparedPlan,
) -> crate::error::Result<Option<(Point, Point)>> {
    let a_cache = FrameDependentCaches::default();
    let b_cache = FrameDependentCaches::default();
    metric_nearest_points_resolved(
        &crs::ResolvedMetric::from_model(model)?,
        a,
        &a_cache,
        b,
        &b_cache,
    )
}

/// `metric_nearest_points` against an already-resolved metric (see
/// [`pair_distance_geodesic`]). `None` when an operand is empty — the surfaces
/// map it to the output-type EMPTY sentinel (`(POINT EMPTY, POINT EMPTY)` /
/// `LINESTRING EMPTY`), mirroring distance's empty→`inf`.
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
    // Empty operand → no witness (the sentinel is built at the surface). Gate
    // before the geodesic domain/ellipsoid machinery, like the distance lane.
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
pub(crate) fn metric_shortest_line(
    model: &crs::MetricModel,
    a: &PreparedPlan,
    b: &PreparedPlan,
) -> crate::error::Result<Shape> {
    crate::geometry::nearest_line(
        metric_nearest_points(model, a, b)?,
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
            crate::py::support::geodesic_by_identity(
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

/// CRS-aware maximum inscribed radius — the pole-to-boundary distance, measured
/// in the geometry's best local projection on a geographic CRS (a near-isometric
/// frame) or in the active planar unit on a projected/planar one. The metric
/// twin of the (metric) `maximum_inscribed_circle`, so the radius matches the
/// disk.
pub(crate) fn metric_maximum_inscribed_radius(
    model: &crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
    tolerance: Option<f64>,
) -> crate::error::Result<f64> {
    dispatch_metric_model(
        model,
        |to_metre| {
            Ok(shape.maximum_inscribed_radius(tolerance.map(|value| value / to_metre))? * to_metre)
        },
        |_, _, state| {
            let source = crs.unwrap_or("EPSG:4326");
            let local = crs::estimate_local_crs_at(shape, source, state)?;
            crs::transform_at(shape, source, &local.definition, state)
                .map_err(|error| local.annotate_error(error))?
                .maximum_inscribed_radius(tolerance)
        },
    )
}

/// Representative frame CRS of a binary operation's operands — the shared CRS
/// the metric resolves against. Operands must agree (the broadcast enforces it
/// per pair), so any present operand's CRS is the frame.
pub(crate) fn binary_frame_crs(left: &Bound<'_, PyAny>, right: &Bound<'_, PyAny>) -> Option<Crs> {
    for operand in [left, right] {
        // Take the first operand that actually carries a CRS — a leading
        // CRS-free operand must not mask a CRS-tagged sibling (else
        // `unit='meters'` would wrongly resolve against `None`). The real
        // frame-compatibility check still runs per pair in the broadcast.
        let crs = match classify_input(operand) {
            Some(GeometryInput::One(geometry)) => geometry.crs_ref().cloned(),
            Some(GeometryInput::Many(array)) => array.crs_ref().cloned(),
            None => continue,
        };
        if crs.is_some() {
            return crs;
        }
    }
    None
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

#[cfg(test)]
mod lonlat_nonempty_shapes_tests {
    use super::lonlat_nonempty_shapes;
    use crate::array::{PyGeometryArray, ValidityMask};
    use crate::boundary::Frame;
    use crate::geometry::{CoordSeq, GeometryErrorKind, Point, Polygon, Ring, Shape};

    fn packed_xy(xs: Vec<f64>, ys: Vec<f64>, frame: Frame) -> PyGeometryArray {
        PyGeometryArray::packed_points(CoordSeq::try_from_vecs(xs, ys, None, None).unwrap(), frame)
    }

    fn square() -> Shape {
        Shape::Polygon(
            Polygon::new(
                Ring::closed_coordseq(
                    CoordSeq::try_from_vecs(
                        vec![0.0, 1.0, 1.0, 0.0, 0.0],
                        vec![0.0, 0.0, 1.0, 1.0, 0.0],
                        None,
                        None,
                    )
                    .unwrap(),
                )
                .unwrap(),
                vec![],
            )
            .unwrap(),
        )
    }

    fn domain_error(array: &PyGeometryArray) -> crate::error::Error {
        lonlat_nonempty_shapes(array).expect_err("expected a domain error")
    }

    #[test]
    fn later_row_invalid_is_native_result() {
        let array = packed_xy(vec![13.4, 0.0], vec![52.5, 95.0], Frame::None);
        let error = domain_error(&array);
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
        let message = error.to_string();
        assert!(
            message.contains("invalid longitude/latitude (0, 95)"),
            "{message}"
        );
        assert!(
            !message.contains("(13.4, 52.5)"),
            "earlier in-domain row must not replace the first invalid: {message}"
        );
    }

    #[test]
    fn empty_and_missing_rows_are_skipped() {
        let present = PyGeometryArray::from_shapes(
            vec![
                Shape::empty_polygon(),
                Shape::Point(Point::new_raw_xy(1.0, 2.0)),
            ],
            Frame::None,
        )
        .unwrap();
        let mask = ValidityMask::from_vec(3, vec![false, true, false]).expect("mask");
        let array = PyGeometryArray::scatter_present_rows(&present, mask).unwrap();
        let shapes = lonlat_nonempty_shapes(&array).expect("empty and missing skip");
        assert_eq!(shapes.len(), 1);
        assert_eq!(shapes[0], Shape::Point(Point::new_raw_xy(1.0, 2.0)));
    }

    #[test]
    fn mixed_polygon_order_reports_later_ring_vertex() {
        let array = PyGeometryArray::from_shapes(
            vec![
                square(),
                Shape::Polygon(
                    Polygon::new(
                        Ring::closed_coordseq(
                            CoordSeq::try_from_vecs(
                                vec![0.0, 1.0, 1.0, 0.0, 0.0],
                                vec![95.0, 0.0, 1.0, 1.0, 95.0],
                                None,
                                None,
                            )
                            .unwrap(),
                        )
                        .unwrap(),
                        vec![],
                    )
                    .unwrap(),
                ),
            ],
            Frame::None,
        )
        .unwrap();
        let message = domain_error(&array).to_string();
        assert!(
            message.contains("invalid longitude/latitude (0, 95)"),
            "{message}"
        );
    }

    #[test]
    fn all_empty_is_ok_empty_vec() {
        let array =
            PyGeometryArray::from_shapes(vec![Shape::empty_polygon()], Frame::None).unwrap();
        let shapes = lonlat_nonempty_shapes(&array).expect("empty filter is Ok");
        assert!(shapes.is_empty());
    }

    #[test]
    fn allocation_failure_after_fixture_is_resource() {
        let array = packed_xy(vec![1.0, 2.0], vec![3.0, 4.0], Frame::None);
        let error = crate::test_support::fail_allocations_after(0, || {
            lonlat_nonempty_shapes(&array).expect_err("first collect allocation must fail")
        });
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::Allocation)
        ));
    }
}
