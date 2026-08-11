#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::types::PyAny;

use crate::NonNegative;
use crate::broadcast::metrics::{
    Bound, CoordinateAxes, Crs, DistanceUnit, EmptyKind, GeometryArrayStorage, GeometryError,
    GeometryInput, Point, PyAnyMethods as _, PyGeometry, PyResult, Shape, ShapeData,
    classify_input, crs, geometry_type_err, validate_lonlat_shape,
};
use crate::geometry::{FrameDependentCaches, LineSeq, witness_pair};
use crate::py::errors::InvalidGeometryError;

/// Monomorphized planar/geodesic dispatch for [`crs::MetricModel`] kernels.
fn dispatch_metric_model<R>(
    model: &crs::MetricModel,
    planar: impl FnOnce(f64) -> crate::error::Result<R>,
    geodesic: impl FnOnce(&str) -> crate::error::Result<R>,
) -> crate::error::Result<R> {
    match model {
        crs::MetricModel::Planar { to_metre } => planar(to_metre.get()),
        crs::MetricModel::Geodesic(crs) => geodesic(crs),
    }
}

/// Monomorphized planar/geodesic dispatch for pre-resolved batch metrics.
fn dispatch_resolved_metric<R>(
    metric: &crs::ResolvedMetric,
    planar: impl FnOnce(f64) -> crate::error::Result<R>,
    run_geodesic: impl FnOnce(&str, &geographiclib_rs::Geodesic) -> crate::error::Result<R>,
) -> crate::error::Result<R> {
    match metric {
        crs::ResolvedMetric::Planar { to_metre } => planar(to_metre.get()),
        crs::ResolvedMetric::Geodesic { crs, geodesic } => run_geodesic(crs, geodesic),
    }
}

/// Shared geodesic-pair setup: domain validation plus ellipsoid parameters.
fn prepare_geodesic_pair<'a>(
    geodesic: &'a geographiclib_rs::Geodesic,
    a: &ShapeData,
    b: &ShapeData,
) -> crate::error::Result<(crs::EllipsoidMetric<'a>, f64, f64)> {
    crs::ensure_geographic_domain(a.shape())?;
    crs::ensure_geographic_domain(b.shape())?;
    let metric = crs::EllipsoidMetric::for_geodesic(geodesic);
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
        |fraction| {
            let a = a.densified(fraction)?;
            let b = b.densified(fraction)?;
            kernel(model, &a, &b)
        },
    )
}

pub(crate) fn finite_geodesic_value(value: f64, operation: &str) -> crate::error::Result<f64> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(crs::CrsError::invalid(format!(
            "{operation} calculation failed"
        )))
    }
}

pub(crate) fn geodesic_point_columns_to_shape_values(
    crs: &str,
    xs: &[f64],
    ys: &[f64],
    shape: &ShapeData,
    frame_cache: &FrameDependentCaches,
) -> crate::error::Result<Vec<f64>> {
    crs::with_resolved_ellipsoid_metric(crs, &[shape.shape()], |crs, metric| {
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        shape.geodesic_distance_points(
            frame_cache,
            xs.iter().copied().zip(ys.iter().copied()),
            crs,
            semi_major,
            flattening,
            metric,
        )
    })
}

pub(crate) fn geodesic_point_columns_dwithin_shape_values(
    crs: &str,
    xs: &[f64],
    ys: &[f64],
    shape: &ShapeData,
    frame_cache: &FrameDependentCaches,
    distance: f64,
) -> crate::error::Result<Vec<bool>> {
    crs::with_resolved_ellipsoid_metric(crs, &[shape.shape()], |crs, metric| {
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        shape.geodesic_dwithin_points(
            frame_cache,
            xs.iter().copied().zip(ys.iter().copied()),
            crs,
            semi_major,
            flattening,
            metric,
            distance,
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
    operation: &str,
) -> PyResult<crs::MetricModel> {
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
    operation: &str,
) -> PyResult<crs::MetricModel> {
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
) -> PyResult<std::borrow::Cow<'a, Shape>> {
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

/// Extract the single point of a `Point` geometry, or error.
pub(crate) fn require_point(geometry: &PyGeometry, operation: &str) -> PyResult<Point> {
    match geometry.shape.shape() {
        Shape::Point(point) => Ok(*point),
        Shape::Empty(EmptyKind::Point, _) => Err(InvalidGeometryError::new_err(format!(
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
    a: &ShapeData,
    b: &ShapeData,
) -> crate::error::Result<Option<(ShapeData, ShapeData)>> {
    if let crs::ResolvedMetric::Geodesic { .. } = metric
        && !a.shape().is_empty()
        && !b.shape().is_empty()
    {
        // Cached verdicts; split ONLY the operand that actually crosses — the
        // other is passed through unchanged (no needless seam walk + rebuild).
        let a_crosses = a.crosses_antimeridian();
        let b_crosses = b.crosses_antimeridian();
        if a_crosses || b_crosses {
            let normalize = |x: &ShapeData, crosses: bool| -> crate::error::Result<ShapeData> {
                Ok(if crosses {
                    ShapeData::from(x.shape().split_antimeridian()?)
                } else {
                    ShapeData::new(x.shape().clone())
                })
            };
            return Ok(Some((normalize(a, a_crosses)?, normalize(b, b_crosses)?)));
        }
    }
    Ok(None)
}

/// Bare `ShapeData` distance against an already-resolved metric — the batch lanes
/// (array × array, index queries) resolve the geodesic ONCE then call this per
/// pair, so the per-pair CRS/ellipsoid resolution is paid a single time.
pub(crate) fn pair_distance_resolved_result(
    metric: &crs::ResolvedMetric,
    a: &ShapeData,
    a_cache: &FrameDependentCaches,
    b: &ShapeData,
    b_cache: &FrameDependentCaches,
) -> crate::error::Result<f64> {
    if let Some((a, b)) = geodesic_split_operands(metric, a, b)? {
        return pair_distance_resolved_result(
            metric,
            &a,
            &FrameDependentCaches::default(),
            &b,
            &FrameDependentCaches::default(),
        );
    }
    if let crs::ResolvedMetric::Geodesic { .. } = metric
        && (a.shape().is_empty() || b.shape().is_empty())
    {
        return Ok(f64::INFINITY);
    }
    dispatch_resolved_metric(
        metric,
        |to_metre| Ok(a.distance(b) * to_metre),
        |crs, geodesic| {
            let (ellipsoid, semi_major, flattening) = prepare_geodesic_pair(geodesic, a, b)?;
            finite_geodesic_value(
                a.geodesic_distance_cached(
                    a_cache, b, b_cache, crs, semi_major, flattening, &ellipsoid,
                )?,
                "geodesic distance",
            )
        },
    )
}

pub(crate) fn pair_distance_resolved(
    metric: &crs::ResolvedMetric,
    a: &ShapeData,
    a_cache: &FrameDependentCaches,
    b: &ShapeData,
    b_cache: &FrameDependentCaches,
) -> PyResult<f64> {
    pair_distance_resolved_result(metric, a, a_cache, b, b_cache).map_err(Into::into)
}

/// Whether two shapes lie within `distance` (meters under the model, or
/// coordinate units for [`crs::MetricModel::COORDINATE`]) under an
/// already-resolved metric. A planar model rescales the threshold into
/// coordinate units for the cheap planar `dwithin`; a geodesic model compares
/// the true geodesic closest-pair distance. The pre-resolved kernel the spatial
/// index and `dwithin` surfaces share.
pub(crate) fn pair_dwithin_shapes(
    model: &crs::MetricModel,
    a: &ShapeData,
    b: &ShapeData,
    distance: f64,
) -> PyResult<bool> {
    let a_cache = FrameDependentCaches::default();
    let b_cache = FrameDependentCaches::default();
    pair_dwithin_resolved(
        &crs::ResolvedMetric::from_model(model)?,
        a,
        &a_cache,
        b,
        &b_cache,
        distance,
    )
}

/// `pair_dwithin_shapes` against an already-resolved metric (see
/// [`pair_distance_resolved`]).
pub(crate) fn pair_dwithin_resolved_result(
    metric: &crs::ResolvedMetric,
    a: &ShapeData,
    a_cache: &FrameDependentCaches,
    b: &ShapeData,
    b_cache: &FrameDependentCaches,
    distance: f64,
) -> crate::error::Result<bool> {
    let distance = NonNegative::try_new("distance", distance)?;
    pair_dwithin_resolved_checked_result(metric, a, a_cache, b, b_cache, distance)
}

fn pair_dwithin_resolved_checked_result(
    metric: &crs::ResolvedMetric,
    a: &ShapeData,
    a_cache: &FrameDependentCaches,
    b: &ShapeData,
    b_cache: &FrameDependentCaches,
    distance: NonNegative,
) -> crate::error::Result<bool> {
    if let Some((a, b)) = geodesic_split_operands(metric, a, b)? {
        return pair_dwithin_resolved_checked_result(
            metric,
            &a,
            &FrameDependentCaches::default(),
            &b,
            &FrameDependentCaches::default(),
            distance,
        );
    }
    if let crs::ResolvedMetric::Geodesic { .. } = metric
        && (a.shape().is_empty() || b.shape().is_empty())
    {
        return Ok(false);
    }
    let distance = distance.get();
    dispatch_resolved_metric(
        metric,
        |to_metre| Ok(a.dwithin(b, distance / to_metre)),
        |crs, geodesic| {
            let (ellipsoid, semi_major, flattening) = prepare_geodesic_pair(geodesic, a, b)?;
            a.geodesic_dwithin_cached(
                a_cache, b, b_cache, crs, semi_major, flattening, &ellipsoid, distance,
            )
        },
    )
}

pub(crate) fn pair_dwithin_resolved(
    metric: &crs::ResolvedMetric,
    a: &ShapeData,
    a_cache: &FrameDependentCaches,
    b: &ShapeData,
    b_cache: &FrameDependentCaches,
    distance: f64,
) -> PyResult<bool> {
    pair_dwithin_resolved_result(metric, a, a_cache, b, b_cache, distance).map_err(Into::into)
}

/// Element-wise zero metric for array-vs-self when both operands share one
/// `Arc<GeometryArrayStorage>` — allocation-free for packed columns.
pub(crate) fn same_storage_similarity_metric_zeros(storage: &GeometryArrayStorage) -> Vec<f64> {
    match storage {
        GeometryArrayStorage::Lines {
            offsets, row_map, ..
        } => {
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
        GeometryArrayStorage::Points { coords, row_map } => {
            vec![0.0; crate::array::point_logical_len(coords, row_map.as_deref())]
        },
        GeometryArrayStorage::Polygons {
            ring_offsets,
            polygon_offsets,
            row_map,
            ..
        } => {
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
    if matches!(model, crs::MetricModel::Geodesic(_)) {
        crs::ensure_geographic_domain(a)?;
        crs::ensure_geographic_domain(b)?;
    }
    if a == b {
        return Ok(if a.is_empty() { f64::INFINITY } else { 0.0 });
    }
    if a.is_empty() || b.is_empty() {
        return Ok(f64::INFINITY);
    }
    dispatch_metric_model(
        model,
        |to_metre| Ok(a.hausdorff_distance(b) * to_metre),
        |crs| crs::geodesic_hausdorff(crs, a, b),
    )
}

/// CRS-aware discrete Fréchet distance (planar/native or explicit SI, or
/// geodesic).
pub(crate) fn metric_frechet(
    model: &crs::MetricModel,
    a: &Shape,
    b: &Shape,
) -> crate::error::Result<f64> {
    // Domain validation BEFORE the identity exit (same contract as Hausdorff).
    if matches!(model, crs::MetricModel::Geodesic(_)) {
        crs::ensure_geographic_domain(a)?;
        crs::ensure_geographic_domain(b)?;
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
        |to_metre| Ok(a.frechet_distance(b)? * to_metre),
        |crs| crs::geodesic_frechet(crs, a, b),
    )
}

/// [`metric_hausdorff`] over `densify`-refined operands: each segment gains
/// evenly spaced vertices (in the source coordinates — lon/lat segments
/// densify linearly, like GEOS) before the continuous Hausdorff kernel.
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
    metric_densified(model, a, b, densify, metric_frechet)
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
    a: &ShapeData,
    b: &ShapeData,
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
/// [`pair_distance_resolved`]). `None` when an operand is empty — the surfaces
/// map it to the output-type EMPTY sentinel (`(POINT EMPTY, POINT EMPTY)` /
/// `LINESTRING EMPTY`), mirroring distance's empty→`inf`.
pub(crate) fn metric_nearest_points_resolved(
    metric: &crs::ResolvedMetric,
    a: &ShapeData,
    a_cache: &FrameDependentCaches,
    b: &ShapeData,
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
        |_| Ok(a.nearest_points(b)),
        |crs, geodesic| {
            let (ellipsoid, semi_major, flattening) = prepare_geodesic_pair(geodesic, a, b)?;
            a.geodesic_nearest_points_cached(
                a_cache, b, b_cache, crs, semi_major, flattening, &ellipsoid,
            )
        },
    )
}

/// CRS-aware shortest connecting line — `metric_nearest_points` as a two-point
/// `LineString` (degenerate when the geometries touch), or `LINESTRING EMPTY`
/// in the operands' common axes when a side is empty.
pub(crate) fn metric_shortest_line(
    model: &crs::MetricModel,
    a: &ShapeData,
    b: &ShapeData,
) -> crate::error::Result<Shape> {
    Ok(crate::geometry::nearest_line(
        metric_nearest_points(model, a, b)?,
        crate::geometry::common_axes(a.shape(), b.shape()),
    ))
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
        |_| Ok(shape.minimum_clearance_line()),
        |_| {
            crate::py::support::geodesic_by_identity(
                shape,
                crs,
                |projected| {
                    Ok(projected
                        .minimum_clearance_plan()
                        .map_or_else(Vec::new, |witness| witness.plan.to_vec()))
                },
                |points| {
                    Ok(if points.is_empty() {
                        Shape::LineString(LineSeq::empty(CoordinateAxes::XY))
                    } else {
                        let [a, b] = points.as_slice() else {
                            unreachable!("minimum_clearance witness plan has two vertices");
                        };
                        Shape::LineString(
                            LineSeq::try_new(witness_pair(*a, *b))
                                .expect("minimum-clearance witness has two vertices"),
                        )
                    })
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
        |to_metre| Ok(shape.minimum_clearance() * to_metre),
        |geodesic_crs| {
            let line = metric_minimum_clearance_line(model, crs, shape)?;
            crs::geodesic_length(geodesic_crs, &line)
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
        |_| {
            let source = crs.unwrap_or("EPSG:4326");
            let local = crs::estimate_local_crs(shape, source)?;
            crs::transform(shape, source, &local)?.maximum_inscribed_radius(tolerance)
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
        Point::new_unchecked_xy(x, y)
    }

    fn scalar_point_dwithin(a: Point, b: Point, limit: f64) -> bool {
        Shape::Point(a).dwithin(&Shape::Point(b), limit)
    }

    fn packed_planar_dwithin(a: Point, b: Point, limit: f64) -> bool {
        points_dwithin(a, b, limit)
    }

    fn shape_data_dwithin(a: Point, b: Point, limit: f64) -> bool {
        ShapeData::new(Shape::Point(a)).dwithin(&ShapeData::new(Shape::Point(b)), limit)
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
