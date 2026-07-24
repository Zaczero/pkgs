#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::collections::{HashSet, HashSetExt};
use crate::geometry::{
    GeodesicMetric, Point, PointKey, VertexProvenance, emit_from_original, shape_from_open_hull,
    smallest_enclosing_circle,
};

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
            return Err(PyTypeError::new_err(
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
            Err(crate::py::errors::parameter_error(message, "clip"))
        },
        Err(_) => Err(PyTypeError::new_err(
            "Voronoi clip must be 'padded', 'envelope', or a Polygon",
        )),
    }
}

/// Shared engine for every Voronoi spelling (`voronoi_polygons` /
/// `voronoi_edges` × scalar / array): resolve `tolerance` and the clip
/// boundary once against the subject frame, run the per-shape diagram
/// `kernel`, and flatten the cells into one mixed array carrying that frame.
pub(crate) fn voronoi_flatten<S: std::borrow::Borrow<Shape>>(
    py: Python<'_>,
    shapes: impl Iterator<Item = S>,
    frame: Frame,
    tolerance: f64,
    clip: &VoronoiClipInput,
    operation: &str,
    kernel: fn(&Shape, f64, VoronoiBoundary<'_>) -> Result<Vec<Shape>>,
) -> PyResult<PyGeometryArray> {
    let supplied = match clip {
        VoronoiClipInput::Default => None,
        VoronoiClipInput::Supplied(value) => Some(value.bind(py)),
    };
    let boundary = match &supplied {
        None => VoronoiBoundary::Padded,
        Some(bound) => {
            voronoi_boundary_from_value(bound, frame.crs_ref(), frame.epoch(), operation)?
        },
    };
    let mut items = Vec::new();
    for shape in shapes {
        for cell in kernel(shape.borrow(), tolerance, boundary)? {
            items.push(PyGeometry::with_frame(cell, frame.clone()));
        }
    }
    Ok(PyGeometryArray::mixed(items, frame))
}

/// Per-row [`Groups`](crate::py::vectors::Groups) sibling of [`voronoi_flatten`]:
/// each input geometry's Voronoi cells form one ragged group (missing rows
/// yield empty groups), so which cells came from which input is preserved.
pub(crate) fn voronoi_groups(
    py: Python<'_>,
    array: &PyGeometryArray,
    tolerance: f64,
    clip: &VoronoiClipInput,
    operation: &str,
    kernel: fn(&Shape, f64, VoronoiBoundary<'_>) -> Result<Vec<Shape>>,
) -> PyResult<crate::py::vectors::Groups> {
    let frame = array.frame.clone();
    let supplied = match clip {
        VoronoiClipInput::Default => None,
        VoronoiClipInput::Supplied(value) => Some(value.bind(py)),
    };
    let boundary = match &supplied {
        None => VoronoiBoundary::Padded,
        Some(bound) => {
            voronoi_boundary_from_value(bound, frame.crs_ref(), frame.epoch(), operation)?
        },
    };
    let mut shapes = Vec::new();
    let mut offsets = vec![0_i64];
    for (missing, shape) in array.masked_shape_rows() {
        if !missing {
            shapes.extend(kernel(&shape, tolerance, boundary)?);
        }
        offsets.push(shapes.len() as i64);
    }
    crate::py::vectors::Groups::from_geometry_flat(
        PyGeometryArray::from_shapes(shapes, frame),
        offsets,
    )
}

pub(crate) fn py_polygonize_full(
    py: Python<'_>,
    full: PolygonizeFull,
    crs: Option<&Crs>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    let frame = Frame::new(crs.cloned(), epoch)?;
    let result = polygonize_result_type(py)?.call1((
        py_geometry_array(full.polygons, &frame),
        py_geometry_array(full.cuts, &frame),
        py_geometry_array(full.dangles, &frame),
        py_geometry_array(full.invalid_rings, &frame),
    ))?;
    Ok(result.unbind())
}

/// CRS-aware constructive kernel: one owner for the planar-vs-geographic
/// split shared by `buffer` and `offset_curve` (scalar and array lanes).
/// Planar frames scale the meter `distance` into coordinate units; geographic
/// frames run the kernel at meter scale inside the shape's best local
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
        crate::crs::MetricModel::Geodesic(_) => {
            geodesic_local_shape(shape, crs, true, |shape| kernel(shape, distance))
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
        crate::crs::MetricModel::Geodesic(_) => {
            geodesic_local_shape(shape, crs, true, |shape| kernel(shape, tolerance))
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
        crate::crs::MetricModel::Geodesic(_) => geodesic_by_identity(
            shape,
            crs,
            |projected| {
                let mut seen = HashSet::with_capacity(projected.coord_count());
                let mut pairs = Vec::with_capacity(projected.coord_count());
                let mut point_index = 0;
                projected.for_each_point(|point| {
                    // Projected vertices are 1:1 with the source; the original
                    // index is the enumeration order (no identity-index slice).
                    let original_index = point_index;
                    point_index += 1;
                    if seen.insert(PointKey::new(point)) {
                        pairs.push((point, original_index));
                    }
                });
                if pairs.is_empty() {
                    return Ok(Vec::new());
                }
                pairs.sort_unstable_by(|(left, _), (right, _)| {
                    left.x.total_cmp(&right.x).then(left.y.total_cmp(&right.y))
                });
                let projected_points: Vec<Point> =
                    pairs.iter().map(|&(point, _)| point.to_xy()).collect();
                Ok(crate::geometry::native_concave_hull(
                    &projected_points,
                    concavity,
                    length_threshold,
                )
                .into_iter()
                .map(|index| VertexProvenance::Input(pairs[index].1))
                .collect())
            },
            |points| Ok(shape_from_open_hull(&points, Shape::empty_polygon)),
        ),
    }
}

pub(crate) fn geodesic_by_identity(
    shape: &Shape,
    crs: Option<&str>,
    plan_fn: impl FnOnce(&Shape) -> Result<Vec<VertexProvenance>>,
    rebuild: impl FnOnce(Vec<Point>) -> Result<Shape>,
) -> Result<Shape> {
    if shape.is_empty() {
        return rebuild(Vec::new());
    }
    let original = shape.points_vec();
    let source = crs.unwrap_or("EPSG:4326");
    let local = crate::crs::estimate_local_crs(shape, source)?;
    let projected = crate::crs::transform(shape, source, &local)?;
    if projected.coord_count() != original.len() {
        return Err(crate::crs::CrsError::invalid(
            "internal identity projection changed point count",
        ));
    }
    let plan = plan_fn(&projected)?;
    rebuild(emit_from_original(&original, &plan))
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
        crate::crs::MetricModel::Geodesic(_) => {
            geodesic_local_shape(shape, crs, false, Shape::minimum_bounding_circle)
        },
    }
}

/// CRS-aware minimum enclosing radius. Planar/projected results follow the
/// metric model (native coordinate units by default; SI meters under
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
            Ok(shape.minimum_bounding_radius() * to_metre.get())
        },
        crate::crs::MetricModel::Geodesic(geodesic_crs) => {
            if shape.is_empty() {
                return Ok(f64::NAN);
            }
            geodesic_minimum_bounding_radius(shape, geodesic_crs)
        },
    }
}

fn geodesic_minimum_bounding_radius(shape: &Shape, crs: &str) -> Result<f64> {
    let points = shape.unique_xy_points();
    match points.as_slice() {
        [] => Ok(f64::NAN),
        [_] => Ok(0.0),
        [left, right] => crate::crs::with_ellipsoid_metric(crs, &[shape], |metric| {
            Ok(metric.segment_length(*left, *right) / 2.0)
        }),
        _ => {
            let (center, _) = smallest_enclosing_circle(&points);
            crate::crs::with_ellipsoid_metric(crs, &[shape], |metric| {
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
        crate::crs::MetricModel::Geodesic(geodesic_crs) => {
            crate::crs::with_ellipsoid_metric(geodesic_crs, &[shape], |metric| {
                use crate::geometry::GeodesicMetric;
                shape.interpolate_m(range, overwrite, |a, b| metric.segment_length(a, b))
            })
        },
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
/// transformed back. Distortion accumulates when the geometry spans many UTM
/// zones, sits far from the anchor, or uses a large distance relative to the
/// feature size. The returned geometry is tagged with the input CRS; it is a
/// CRS-aware local projection result, not an exact ellipsoidal buffer.
fn geodesic_local_shape(
    shape: &Shape,
    crs: Option<&str>,
    checked: bool,
    op: impl FnOnce(&Shape) -> Result<Shape>,
) -> Result<Shape> {
    if shape.is_empty() {
        return op(shape);
    }
    let source = crs.unwrap_or("EPSG:4326");
    let local = crate::crs::estimate_local_crs(shape, source)?;
    let projected = crate::crs::transform(shape, source, &local)?;
    let result = op(&projected)?;
    let restored = crate::crs::transform(&result, &local, source)?;
    // Distance-driven ops (buffer/offset/densify) demand metric accuracy, so
    // they reject a result that exceeds the local projection's linear
    // scale-error budget. The minimum bounding *circle* is an intentionally
    // approximate bounding primitive (like bounds/centroid) that must remain
    // usable at city-to-continental scale, so it skips the fit check.
    if checked && !crate::crs::local_crs_fits(&restored, source, &local)? {
        return Err(crate::crs::CrsError::invalid(format!(
            "constructive result exceeds the {:.1}% linear scale-error limit in one local projection; use a smaller distance, split the geometry, or choose a projected CRS explicitly",
            crate::crs::LOCAL_SCALE_ERROR_LIMIT * 100.0
        )));
    }
    Ok(restored)
}
