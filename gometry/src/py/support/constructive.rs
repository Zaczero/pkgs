use std::sync::Arc;

use crate::geometry::{
    GeodesicMetric as _, Point, PointKey, ShapeCountQuery, VertexProvenance, emit_from_original,
    geometric_xy_cmp, shape_from_selected_hull, smallest_enclosing_circle,
};
use crate::py::errors;
use crate::py::support::{
    Bound, Crs, Frame, FromPyObject, Polygon, PolygonizeFull, Py, PyAny, PyAnyMethods as _, PyErr,
    PyGeometryArray, PyResult, PyTypeError, Python, Result, Shape, VoronoiBoundary, exact_geometry,
    polygonize_result_type,
};
use crate::py::vectors::Groups;

/// Non-optional Voronoi `clip` input: Rust default is padded; explicit Python
/// ``None`` is rejected.
pub enum VoronoiClipInput {
    Default,
    Supplied(Py<PyAny>),
}

impl VoronoiClipInput {
    pub(crate) const DEFAULT: Self = Self::Default;
}

impl<'a, 'py> FromPyObject<'a, 'py> for VoronoiClipInput {
    type Error = PyErr;

    fn extract(value: pyo3::Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if value.is_none() {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "clip cannot be None",
            ));
        }
        Ok(Self::Supplied(value.as_any().clone().unbind()))
    }
}

/// Resolve a supplied Voronoi `clip` value: string modes `'padded'`/`'envelope'`,
/// or a `Polygon` to clip the diagram to.
pub(crate) fn voronoi_boundary_from_value<'a>(
    clip: &'a Bound<'_, PyAny>,
    subject_crs: Option<&Crs>,
    subject_epoch: Option<f64>,
    operation: &str,
) -> PyResult<VoronoiBoundary<'a>> {
    if let Some(geometry) = exact_geometry(clip) {
        let Shape::Polygon(polygon) = geometry.shape.shape() else {
            return Err(errors::geometry_type_err(
                "Voronoi clip geometry must be a Polygon",
            ));
        };
        // A `Polygon` clip is a second operand: it must share the subject's
        // CRS/epoch frame, like every other binary geometry op.
        Frame::compatible_parts(
            subject_crs,
            subject_epoch,
            geometry.crs_ref(),
            geometry.epoch(),
            operation,
        )?;
        return Ok(VoronoiBoundary::Polygon(polygon));
    }
    match clip.extract::<&str>() {
        Ok("padded") => Ok(VoronoiBoundary::Padded),
        Ok("envelope") => Ok(VoronoiBoundary::Envelope),
        Ok(value) => {
            let mut message = crate::tokens::unknown_token_message("Voronoi clip", value, &[
                "padded", "envelope",
            ]);
            message.push_str(", or a Polygon");
            Err(errors::parameter_error(message, "clip"))
        },
        Err(_) => Err(PyTypeError::new_err(
            "Voronoi clip must be 'padded', 'envelope', or a Polygon",
        )),
    }
}

/// Owned form used by detached scalar and array walks. Resolve the supplied
/// clip before detaching; arrays carry the same boundary through the existing
/// cumulative-budget helper across every row.
pub(crate) enum OwnedVoronoiBoundary {
    Padded,
    Envelope,
    Polygon(Polygon),
}

impl OwnedVoronoiBoundary {
    pub(crate) const fn as_borrowed(&self) -> VoronoiBoundary<'_> {
        match self {
            Self::Padded => VoronoiBoundary::Padded,
            Self::Envelope => VoronoiBoundary::Envelope,
            Self::Polygon(polygon) => VoronoiBoundary::Polygon(polygon),
        }
    }
}

pub(crate) fn owned_voronoi_boundary(
    py: Python<'_>,
    clip: &VoronoiClipInput,
    frame: &Frame,
    operation: &'static str,
) -> PyResult<OwnedVoronoiBoundary> {
    let supplied = match clip {
        VoronoiClipInput::Default => return Ok(OwnedVoronoiBoundary::Padded),
        VoronoiClipInput::Supplied(value) => value.bind(py),
    };
    Ok(
        match voronoi_boundary_from_value(supplied, frame.crs_ref(), frame.epoch(), operation)? {
            VoronoiBoundary::Padded => OwnedVoronoiBoundary::Padded,
            VoronoiBoundary::Envelope => OwnedVoronoiBoundary::Envelope,
            VoronoiBoundary::Polygon(polygon) => OwnedVoronoiBoundary::Polygon(polygon.clone()),
        },
    )
}

/// Shared engine for every Voronoi spelling (`voronoi_polygons` /
/// `voronoi_edges` × scalar / array): resolve `tolerance` and the clip
/// boundary once against the subject frame, run the per-shape diagram
/// `kernel`, and flatten the cells into one mixed array carrying that frame.
/// Run one Voronoi kernel over every input shape outside the interpreter
/// lock, one budget for the whole call. The boundary and shapes are fully
/// admitted Rust values by then; Python objects exist only after reattachment.
fn voronoi_outputs<T: Send>(
    py: Python<'_>,
    shapes: impl Iterator<Item = Arc<crate::geometry::ShapeData>>,
    frame: &Frame,
    tolerance: f64,
    clip: &VoronoiClipInput,
    operation: &'static str,
    kernel: fn(
        &Shape,
        f64,
        VoronoiBoundary<'_>,
        &mut crate::geometry::ExpansionBudget,
    ) -> Result<T>,
) -> PyResult<Vec<T>> {
    let boundary = owned_voronoi_boundary(py, clip, frame, operation)?;
    let shapes = shapes.collect::<Vec<_>>();
    Ok(py.detach(move || {
        shapes
            .iter()
            .map(|shape| {
                let mut budget =
                    crate::geometry::ExpansionBudget::new(operation, "sites/clip topology");
                kernel(shape, tolerance, boundary.as_borrowed(), &mut budget)
            })
            .collect::<Result<Vec<T>>>()
    })?)
}

pub(crate) fn voronoi_flatten(
    py: Python<'_>,
    shapes: impl Iterator<Item = Arc<crate::geometry::ShapeData>>,
    frame: Frame,
    tolerance: f64,
    clip: &VoronoiClipInput,
    operation: &'static str,
    kernel: fn(
        &Shape,
        f64,
        VoronoiBoundary<'_>,
        &mut crate::geometry::ExpansionBudget,
    ) -> Result<Vec<Shape>>,
) -> PyResult<PyGeometryArray> {
    let cells = voronoi_outputs(py, shapes, &frame, tolerance, clip, operation, kernel)?;
    Ok(PyGeometryArray::from_shapes(
        cells.into_iter().flatten().collect(),
        frame,
    )?)
}

/// Voronoi edges straight into packed line storage: the kernel hands back one
/// XY column per input shape and the columns concatenate once.
pub(crate) fn voronoi_edges_flatten(
    py: Python<'_>,
    shapes: impl Iterator<Item = Arc<crate::geometry::ShapeData>>,
    frame: Frame,
    tolerance: f64,
    clip: &VoronoiClipInput,
) -> PyResult<PyGeometryArray> {
    let columns = voronoi_outputs(
        py,
        shapes,
        &frame,
        tolerance,
        clip,
        "voronoi_edges",
        Shape::voronoi_edge_column_budgeted,
    )?;
    let (coords, offsets) = crate::array::concat_packed_line_columns(columns)?;
    Ok(if coords.is_empty() {
        PyGeometryArray::from_shapes(Vec::new(), frame)?
    } else {
        PyGeometryArray::packed_lines(coords, offsets, frame)?
    })
}

pub(crate) fn py_polygonize_full(
    py: Python<'_>,
    full: PolygonizeFull,
    crs: Option<&Crs>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    let frame = match Frame::new(crs.cloned(), epoch) {
        Ok(frame) => frame,
        Err(error) => {
            drop(full);
            return Err(error.into());
        },
    };
    let result_type = polygonize_result_type(py)?;
    let parts = Groups::polygonize_full_parts(full, frame)?;
    let result = result_type.call1(parts)?;
    Ok(result.unbind())
}

/// CRS-aware constructive kernel: one owner for the planar-vs-geographic
/// split shared by `buffer` and `offset_curve` (scalar and array lanes).
/// Projected frames scale the meter `distance` into native linear units, and
/// CRS-free frames use raw coordinate units; geographic frames run the kernel
/// at meter scale inside the shape's best local
/// projection.
///
/// **Geographic approximation.** On a geographic CRS, the checked local-
/// projection owner picks one extent-validated
/// [`estimate_local_crs`](crate::crs::estimate_local_crs) projection, reprojects,
/// runs the planar constructive kernel with the distance in meters, then
/// reprojects back (and rejects results that exceed the scale-error limit).
/// The result is **not** a true ellipsoidal offset curve: distortion and seam
/// error grow with geographic extent, buffer radius, and distance from the
/// projection anchor. For city-scale features and modest radii the error is
/// small; for continent-scale inputs or very large buffers, reproject to an
/// appropriate projected CRS (or tile the work) if sub-percent accuracy matters.
pub(crate) fn metric_constructive_shape(
    model: &crate::crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
    distance: f64,
    kernel: impl Fn(&Shape, f64) -> Result<Shape>,
) -> Result<Shape> {
    match model {
        crate::crs::MetricModel::Planar { to_metre } => {
            Ok(kernel(shape, distance / to_metre.get())?)
        },
        crate::crs::MetricModel::Geodesic { state, .. } => {
            geodesic_local_shape(shape, crs, true, state, |shape| kernel(shape, distance))
        },
    }
}

/// [`metric_constructive_shape`] with one generated-work budget threaded
/// through the projected kernel.  Geographic local projection is a frame
/// change, not a new operation: it deliberately receives the caller's budget.
pub(crate) fn metric_constructive_shape_budgeted(
    model: &crate::crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
    distance: f64,
    budget: &mut crate::geometry::ExpansionBudget,
    mut kernel: impl FnMut(&Shape, f64, &mut crate::geometry::ExpansionBudget) -> Result<Shape>,
) -> Result<Shape> {
    match model {
        crate::crs::MetricModel::Planar { to_metre } => {
            kernel(shape, distance / to_metre.get(), budget)
        },
        crate::crs::MetricModel::Geodesic { state, .. } => {
            // A run in a refitted frame replaces the first one, so it starts
            // from the caller's budget rather than from the first run's charge.
            let start = budget.clone();
            geodesic_local_shape(shape, crs, true, state, |shape| {
                *budget = start.clone();
                kernel(shape, distance, budget)
            })
        },
    }
}

/// CRS-aware constructive whose precision is optional. An omitted tolerance
/// stays omitted until the kernel sees the working geometry, allowing its
/// scale-aware default to be computed in the actual execution frame.
pub(crate) fn metric_optional_constructive_shape(
    model: &crate::crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
    tolerance: Option<f64>,
    kernel: impl Fn(&Shape, Option<f64>) -> Result<Shape>,
) -> Result<Shape> {
    match model {
        crate::crs::MetricModel::Planar { to_metre } => {
            kernel(shape, tolerance.map(|value| value / to_metre.get()))
        },
        crate::crs::MetricModel::Geodesic { state, .. } => {
            geodesic_local_shape(shape, crs, true, state, |shape| kernel(shape, tolerance))
        },
    }
}

/// CRS-aware maximum-inscribed-circle construction. Geographic results run
/// the planar kernel in a local projection and retain that operation's
/// approximation contract rather than asserting source-frame containment.
pub(crate) fn metric_maximum_inscribed_circle(
    model: &crate::crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
    tolerance: Option<f64>,
) -> Result<Shape> {
    match model {
        crate::crs::MetricModel::Planar { to_metre } => {
            shape.maximum_inscribed_circle(tolerance.map(|value| value / to_metre.get()))
        },
        crate::crs::MetricModel::Geodesic { state, .. } => {
            geodesic_local_shape(shape, crs, true, state, |shape| {
                shape.maximum_inscribed_circle_in_frame(tolerance, false)
            })
        },
    }
}

/// CRS-aware concave hull. Projected/CRS-free inputs run the native kernel in
/// the receiver coordinate frame (scaling `length_threshold` for projected SI
/// metrics). Geographic inputs use a local projection only to decide which
/// input vertices survive, then emit the original vertices bit-for-bit.
pub(crate) fn metric_concave_hull(
    model: &crate::crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
    concavity: f64,
    length_threshold: f64,
) -> Result<Shape> {
    match model {
        crate::crs::MetricModel::Planar { to_metre } => {
            shape.concave_hull(concavity, length_threshold / to_metre.get())
        },
        crate::crs::MetricModel::Geodesic { state, .. } => {
            let compact = shape
                .has_repeated_shared_owner()
                .then(|| shape.distinct_point_cloud())
                .transpose()?;
            let source = compact.as_ref().unwrap_or(shape);
            geodesic_by_identity(
                source,
                crs,
                state,
                |projected| {
                    let capacity = ShapeCountQuery::default().coordinates(projected).ok();
                    let mut seen = crate::collections::try_hash_set()?;
                    let mut pairs = Vec::new();
                    if let Some(capacity) = capacity {
                        let _seen_reservation = seen.try_reserve(capacity);
                        let _pairs_reservation = pairs.try_reserve(capacity);
                    }
                    let mut point_index = 0_usize;
                    projected.for_each_point(|point| {
                        // Projected vertices are 1:1 with the source; the original
                        // index is the enumeration order (no identity-index slice).
                        let original_index = point_index;
                        point_index = point_index.wrapping_add(1);
                        if seen.insert(PointKey::new(point)) {
                            // A count-derived reservation, when representable,
                            // covers every source vertex.
                            pairs.push((point, original_index));
                        }
                    });
                    if pairs.is_empty() {
                        return Ok(Vec::new());
                    }
                    pairs.sort_unstable_by(|(left, _), (right, _)| geometric_xy_cmp(*left, *right));
                    let mut projected_points = Vec::with_capacity(pairs.len());
                    for &(point, _) in &pairs {
                        projected_points.push(point.to_xy());
                    }
                    let mut provenance = Vec::with_capacity(projected_points.len());
                    for index in crate::geometry::native_concave_hull(
                        &projected_points,
                        concavity,
                        length_threshold,
                    )? {
                        let (_, original_index) = pairs
                            .get(index)
                            .expect("hull vertex index is in the projected input");
                        provenance.push(VertexProvenance::Input(*original_index));
                    }
                    Ok(provenance)
                },
                |points| shape_from_selected_hull(&points, Shape::empty_polygon),
            )
        },
    }
}

pub(crate) fn geodesic_by_identity(
    shape: &Shape,
    crs: Option<&str>,
    state: &crate::crs::RuntimeState,
    plan_fn: impl FnOnce(&Shape) -> Result<Vec<VertexProvenance>>,
    rebuild: impl FnOnce(Vec<Point>) -> Result<Shape>,
) -> Result<Shape> {
    if shape.is_empty() {
        return rebuild(Vec::new());
    }
    let original = shape.points_vec()?;
    let source = crs.unwrap_or("EPSG:4326");
    let local = crate::crs::estimate_local_crs_at(&[shape], source, state)?;
    let projected = crate::crs::transform_at(shape, source, &local.definition, state)
        .map_err(|error| local.annotate_error(error))?;
    if ShapeCountQuery::default().coordinates(&projected)? != original.len() {
        return Err(crate::crs::CrsError::invalid(
            "internal identity projection changed point count",
        ));
    }
    let plan = plan_fn(&projected)?;
    rebuild(emit_from_original(&original, &plan)?)
}

/// CRS-aware minimum enclosing circle. Planar/projected frames build the circle
/// in the receiver coordinates after scaling the requested metric; geographic
/// frames use the same local-projection approximation as metric constructive
/// geometry, so the returned shape is tagged in the source CRS.
pub(crate) fn metric_minimum_bounding_circle(
    model: &crate::crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
) -> Result<Shape> {
    match model {
        crate::crs::MetricModel::Planar { .. } => shape.minimum_bounding_circle(),
        // The bounding circle is an approximate bounding primitive, not a
        // distance-accurate metric op, so it uses the local projection WITHOUT
        // the scale-error fit check (`checked = false`) and stays usable at
        // city-to-continental extent — unlike buffer/offset above.
        crate::crs::MetricModel::Geodesic { state, .. } => {
            let compact = shape
                .has_repeated_shared_owner()
                .then(|| shape.distinct_point_cloud())
                .transpose()?;
            let source = compact.as_ref().unwrap_or(shape);
            geodesic_local_shape(source, crs, false, state, Shape::minimum_bounding_circle)
        },
    }
}

/// CRS-aware minimum enclosing radius. Planar/projected results follow the
/// metric model (native linear units by default; SI meters under
/// ``unit='meters'``); geographic two-point inputs use the exact ellipsoidal
/// midpoint radius; larger geographic point sets use the planar enclosing-
/// circle center and measure support distances on the ellipsoid, so they are
/// CRS-aware but still approximate.
pub(crate) fn metric_minimum_bounding_radius(
    model: &crate::crs::MetricModel,
    shape: &Shape,
) -> Result<f64> {
    match model {
        crate::crs::MetricModel::Planar { to_metre } => {
            let points = shape.try_unique_xy_points()?;
            if points.is_empty() {
                return Ok(f64::NAN);
            }
            let circle = smallest_enclosing_circle(&points)?;
            circle
                .certified_disk(&points)?
                .radius_in_scale(to_metre.get())
        },
        crate::crs::MetricModel::Geodesic {
            crs: geodesic_crs,
            state,
            ..
        } => geodesic_minimum_bounding_radius(shape, geodesic_crs, state),
    }
}

fn geodesic_minimum_bounding_radius(
    shape: &Shape,
    crs: &str,
    state: &crate::crs::RuntimeState,
) -> Result<f64> {
    let points = shape.try_unique_xy_points()?;
    match points.as_slice() {
        [] => Ok(f64::NAN),
        [_] => Ok(0.0),
        [left, right] => crate::crs::with_ellipsoid_metric_at(crs, &[shape], state, |metric| {
            Ok(metric.segment_length(*left, *right) / 2.0)
        }),
        _ => {
            let circle = smallest_enclosing_circle(&points)?;
            let center = circle.certified_disk(&points)?.center()?;
            crate::crs::with_ellipsoid_metric_at(crs, &[shape], state, |metric| {
                let mut radius: f64 = 0.0;
                for point in points {
                    radius = radius.max(metric.segment_length(center, point));
                }
                Ok(radius)
            })
        },
    }
}

/// CRS-aware `interpolate_m`: distribute M by arc length, stationing
/// geodesically on a geographic CRS (so M reflects real-world distance, like
/// the rest of the LRS family) and by planar 2D distance otherwise. Unlike the
/// constructive ops this NEVER reprojects — X/Y/Z stay exact; only the geodesic
/// segment lengths feed the stationing.
pub(crate) fn metric_interpolate_m(
    model: &crate::crs::MetricModel,
    shape: &Shape,
    range: crate::geometry::MeasureRange,
    overwrite: bool,
) -> Result<Shape> {
    match model {
        // The M fraction (traveled / total) is scale-invariant, so planar
        // stationing uses raw 2D distance regardless of `to_metre`.
        crate::crs::MetricModel::Planar { .. } => {
            shape.interpolate_m(range, overwrite, crate::geometry::point_distance)
        },
        crate::crs::MetricModel::Geodesic {
            crs: geodesic_crs,
            state,
            ..
        } => crate::crs::with_ellipsoid_metric_at(geodesic_crs, &[shape], state, |metric| {
            use crate::geometry::GeodesicMetric as _;
            shape.interpolate_m(range, overwrite, |a, b| metric.segment_length(a, b))
        }),
    }
}

/// Run a meter-based planar operation in the shape's best local projection,
/// returning the result in the source CRS after a linear scale-error fit check.
/// Used by the geodesic constructive paths; pure shape work, so array lanes
/// run it detached.
///
/// The operation is a **local projection approximation** of geodesic
/// construction: one extent-validated local frame is chosen, the shape is
/// transformed there, the planar kernel runs at meter scale, and the result is
/// transformed back. The frame must hold the input and the result together. Distortion accumulates when the geometry spans many UTM
/// zones, sits far from the anchor, or uses a large distance relative to the
/// feature size. The returned geometry is tagged with the input CRS; it is a
/// CRS-aware local projection result, not an exact ellipsoidal buffer.
fn geodesic_local_shape(
    shape: &Shape,
    crs: Option<&str>,
    checked: bool,
    state: &crate::crs::RuntimeState,
    mut op: impl FnMut(&Shape) -> Result<Shape>,
) -> Result<Shape> {
    if shape.is_empty() {
        return op(shape);
    }
    let source = crs.unwrap_or("EPSG:4326");
    let mut run_in = |local: &crate::crs::LocalCrsDefinition| -> Result<Shape> {
        let projected = crate::crs::transform_at(shape, source, &local.definition, state)
            .map_err(|error| local.annotate_error(error))?;
        let result = op(&projected)?;
        crate::crs::transform_at(&result, &local.definition, source, state)
            .map_err(|error| local.annotate_error(error))
    };
    let local = crate::crs::estimate_local_crs_at(&[shape], source, state)?;
    let restored = run_in(&local)?;
    // Distance-driven ops (buffer/offset/densify) demand metric accuracy, so
    // they reject a result that exceeds the local projection's linear
    // scale-error budget. The minimum bounding *circle* is an intentionally
    // approximate bounding primitive (like bounds/centroid) that must remain
    // usable at city-to-continental scale, so it skips the fit check.
    if !checked || crate::crs::local_crs_fits_at(&restored, source, &local.definition, state)? {
        return Ok(restored);
    }
    // The frame was fitted to the input, and an operation's reach (a buffer
    // distance) carries the result past it: a point beside a UTM zone edge
    // fits that zone while its buffer does not. Fit one frame to the input
    // and the result together, since the operation measures between them,
    // and run once more there.
    let exceeded = || {
        crate::crs::CrsError::invalid(format!(
            "constructive result exceeds the {:.1}% linear scale-error limit in one local projection; use a smaller distance, split the geometry, or choose a projected CRS explicitly",
            crate::crs::LOCAL_SCALE_ERROR_LIMIT * 100.0
        ))
    };
    let refit = crate::crs::estimate_local_crs_at(&[shape, &restored], source, state)
        .map_err(|error| crate::crs::preserve_resource_error(error, |_| exceeded()))?;
    if refit.definition != local.definition {
        let restored = run_in(&refit)?;
        if crate::crs::local_crs_fits_at(&restored, source, &refit.definition, state)? {
            return Ok(restored);
        }
    }
    Err(exceeded())
}
