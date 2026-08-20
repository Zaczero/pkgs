#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::error::Result;
use crate::geometry::{
    Bounds, CoordSeq, Coordinates as _, GeometryErrorKind, Point, Polygon, RepairMethod, Ring,
    Segment, Shape, ShapeData, ValidationIssue, shape_encloses_pole, shape_has_polar_ring,
    shared_segment_part,
};

/// Planar-correct normalization for frame-aware topology: drop Z/M (topology
/// is 2D) and split geographic seam crossings. Valid XY topology splits
/// infallibly; malformed ring arrangements fall back to their planar 2D shape
/// for bool-only predicate kernels, while frame-aware validation reports the
/// split failure explicitly through [`try_topology_split`].
pub(crate) fn topology_split(shape: &Shape) -> Shape {
    try_topology_split(shape).unwrap_or_else(|_| shape.force_2d())
}

fn try_topology_split(shape: &Shape) -> Result<Shape> {
    shape.force_2d().split_antimeridian()
}

fn needs_topology_split(shape: &Shape, geographic: bool) -> bool {
    geographic && shape.crosses_antimeridian()
}

fn rings_share_source_linework(left: &CoordSeq, right: &CoordSeq) -> bool {
    left.segment_pairs().any(|[left_start, left_end]| {
        let left = Segment {
            start: left_start.xy(),
            end: left_end.xy(),
        };
        right.segment_pairs().any(|[right_start, right_end]| {
            shared_segment_part(left, Segment {
                start: right_start.xy(),
                end: right_end.xy(),
            })
            .is_some()
        })
    })
}

/// Validate relationships involving crossing polygon holes before the public
/// split's set-difference assembly. Difference is deliberately repair-like: an
/// outside hole simply subtracts nothing and overlapping holes collapse to
/// their union. That is useful to `repair`, but validity must diagnose the
/// malformed source rather than bless the normalized result.
fn crossing_hole_relationship_issue(shape: &Shape, path: &str) -> Option<ValidationIssue> {
    let polygon_issue = |polygon: &Polygon, path: &str| {
        let crossing: Vec<(usize, &Ring)> = polygon
            .holes
            .iter()
            .enumerate()
            .filter(|(_, hole)| {
                Shape::Polygon(Polygon::new((*hole).clone(), Vec::new())).crosses_antimeridian()
            })
            .collect();
        if crossing.is_empty() {
            return None;
        }

        let crossing_indices: Vec<usize> = crossing.iter().map(|(index, _)| *index).collect();
        let stationary = polygon
            .holes
            .iter()
            .enumerate()
            .filter(|(index, _)| !crossing_indices.contains(index))
            .map(|(_, hole)| hole.clone())
            .collect();
        let container = Shape::Polygon(Polygon::new(polygon.shell.clone(), stationary));
        let container = match container.split_antimeridian() {
            Ok(container) => container,
            Err(error) => {
                return Some(ValidationIssue::new(
                    format!("geographic topology normalization failed: {error}"),
                    None,
                    path,
                ));
            },
        };
        let mut normalized_holes: Vec<(usize, Shape)> = Vec::with_capacity(crossing.len());
        for (index, hole) in crossing {
            let witness = hole.coords().first();
            if rings_share_source_linework(polygon.shell.coords(), hole.coords())
                || polygon
                    .holes
                    .iter()
                    .enumerate()
                    .any(|(other_index, other)| {
                        other_index != index
                            && rings_share_source_linework(other.coords(), hole.coords())
                    })
            {
                return Some(ValidationIssue::new(
                    format!("interior ring at index {index} intersects another ring on a line"),
                    witness,
                    path,
                ));
            }
            let hole = Shape::Polygon(Polygon::new(hole.clone(), Vec::new()));
            let hole = match hole.split_antimeridian() {
                Ok(hole) => hole,
                Err(error) => {
                    return Some(ValidationIssue::new(
                        format!("geographic topology normalization failed: {error}"),
                        witness,
                        format!("{path}.holes[{index}]"),
                    ));
                },
            };
            if !container.contains(&hole) {
                return Some(ValidationIssue::new(
                    format!(
                        "interior ring at index {index} is not contained within the polygon's exterior"
                    ),
                    witness,
                    path,
                ));
            }
            for (other_index, other) in &normalized_holes {
                let relation = other.relate(&hole);
                if relation.as_bytes()[0] != b'F' {
                    return Some(ValidationIssue::new(
                        format!(
                            "interior ring at index {} and interior ring at index {} intersect on an area",
                            index.min(*other_index),
                            index.max(*other_index),
                        ),
                        witness,
                        path,
                    ));
                }
            }
            normalized_holes.push((index, hole));
        }
        None
    };

    match shape {
        Shape::Polygon(polygon) => polygon_issue(polygon, path),
        Shape::MultiPolygon(polygons) => {
            polygons.iter().enumerate().find_map(|(index, polygon)| {
                polygon_issue(polygon, &format!("{path}.geometries[{index}]"))
            })
        },
        Shape::GeometryCollection(parts) => parts.iter().enumerate().find_map(|(index, part)| {
            crossing_hole_relationship_issue(part, &format!("{path}.geometries[{index}]"))
        }),
        _ => None,
    }
}

/// Validate stored geometry in its coordinate frame. Projected and CRS-free
/// geometries retain ordinary planar OGC validity; only seam-crossing
/// geographic geometry is normalized first.
pub(crate) fn validate_shape_in_frame(shape: &Shape, geographic: bool) -> Option<ValidationIssue> {
    if needs_topology_split(shape, geographic) {
        let planar = shape.force_2d();
        if let Some(issue) = crossing_hole_relationship_issue(&planar, "$") {
            return Some(issue);
        }
        try_topology_split(shape).map_or_else(
            |error| {
                Some(ValidationIssue::new(
                    format!("geographic topology normalization failed: {error}"),
                    None,
                    "$",
                ))
            },
            |normalized| normalized.validate(),
        )
    } else {
        shape.validate()
    }
}

/// Cached-handle twin of [`validate_shape_in_frame`].
pub(crate) fn validate_data_in_frame(
    data: &ShapeData,
    geographic: bool,
) -> Option<ValidationIssue> {
    if geographic && data.crosses_antimeridian() {
        validate_shape_in_frame(data.shape(), true)
    } else {
        data.validate_cached().cloned()
    }
}

/// Frame-aware simplicity. Non-crossing inputs keep the frozen handle's cached
/// verdict; seam-crossing geographic inputs answer on normalized topology.
pub(crate) fn is_simple_data_in_frame(data: &ShapeData, geographic: bool) -> bool {
    if geographic && data.crosses_antimeridian() {
        try_topology_split(data.shape()).is_ok_and(|shape| shape.is_simple())
    } else {
        data.is_simple_cached()
    }
}

/// Frame-aware `is_ring`: closure is structural on the original line, while
/// simplicity is evaluated on normalized seam topology.
pub(crate) fn is_ring_data_in_frame(data: &ShapeData, geographic: bool) -> bool {
    if geographic && data.crosses_antimeridian() {
        data.shape().is_closed()
            && try_topology_split(data.shape()).is_ok_and(|shape| shape.is_simple())
    } else {
        data.shape().is_ring()
    }
}

/// Frame-aware self-intersection diagnostics, shared by scalar and grouped
/// array output.
pub(crate) fn self_intersections_in_frame(shape: &Shape, geographic: bool) -> Vec<Point> {
    if needs_topology_split(shape, geographic) {
        match try_topology_split(shape) {
            Ok(normalized) if normalized.is_simple() => Vec::new(),
            Ok(normalized) => normalized.self_intersections(),
            Err(_) => shape.self_intersections(),
        }
    } else {
        shape.self_intersections()
    }
}

fn repair_invalid_shape(shape: &Shape, geographic: bool, method: RepairMethod) -> Result<Shape> {
    let repaired = if needs_topology_split(shape, geographic) {
        // Preserve source Z/M on ordinary seam crossings. Polar closure with
        // Z/M remains an honest error: no source ordinate exists at a
        // fabricated pole vertex.
        shape.split_antimeridian()?.repair(method, false)
    } else {
        shape.repair(method, false)
    }?;
    if let Some(issue) = validate_shape_in_frame(&repaired, geographic) {
        return Err(GeometryErrorKind::repair_failed(format!(
            "repair did not produce valid geometry: {}",
            issue.reason,
        )));
    }
    Ok(repaired)
}

/// Repair only when frame-aware validation finds a defect. `None` means the
/// caller must preserve the original frozen handle/storage unchanged.
pub(crate) fn repair_shape_in_frame(
    shape: &Shape,
    geographic: bool,
    method: RepairMethod,
) -> Result<Option<Shape>> {
    validate_shape_in_frame(shape, geographic)
        .map(|_| repair_invalid_shape(shape, geographic, method))
        .transpose()
}

/// Cached-handle twin of [`repair_shape_in_frame`].
pub(crate) fn repair_data_in_frame(
    data: &ShapeData,
    geographic: bool,
    method: RepairMethod,
) -> Result<Option<Shape>> {
    validate_data_in_frame(data, geographic)
        .map(|_| repair_invalid_shape(data.shape(), geographic, method))
        .transpose()
}

/// Shift western longitudes by ``+360`` so a crossing geometry is contiguous
/// for planar centroid, ``point_on_surface``, and bounds.
pub(crate) fn unwrap_longitude(shape: &Shape) -> Result<Shape> {
    shape.map_xy_with(|x, y| Ok((if x < 0.0 { x + 360.0 } else { x }, y)))
}

/// Wrap a longitude into ``[-180, 180]``.
fn wrap_longitude(lon: f64) -> f64 {
    (lon + 180.0).rem_euclid(360.0) - 180.0
}

/// Rewrap a derived point's longitude after an unwrapped planar kernel.
fn rewrap_point(point: Point) -> Result<Point> {
    point.with_xy(wrap_longitude(point.x), point.y)
}

/// Derived representative point (centroid / `point_on_surface`) on a crossing
/// geographic geometry. Two strategies, because unwrap-rewrap cannot serve both:
///
/// * `interior` callers (`point_on_surface`, which must return a point strictly
///   inside) AND any pole-enclosing geometry route through the seam+pole SPLIT —
///   the unwrapped form of a polar cap is a degenerate longitude sliver (a
///   longitude shift can't reconstruct the cap), and the split's planar kernel
///   yields a point genuinely inside the true region. The split is in
///   `[-180, 180]`, so no rewrap is needed. `force_2d` keeps the split infallible
///   (a Z/M pole closure would otherwise trip the ordinate gate).
/// * every other crossing `centroid` uses unwrap-rewrap, which gives the TRUE
///   geographic centroid (the split's two opposite-seam halves would average to
///   a meaningless mid-longitude).
#[derive(Clone, Copy)]
pub(crate) enum DerivedPointStrategy {
    Centroid,
    Interior,
}

pub(crate) fn derived_point_unwrapped(
    shape: &Shape,
    strategy: DerivedPointStrategy,
    kernel: impl FnOnce(&Shape) -> Result<Shape>,
) -> Result<Shape> {
    if matches!(strategy, DerivedPointStrategy::Interior)
        || shape_encloses_pole(shape, true)
        || shape_encloses_pole(shape, false)
    {
        return kernel(&shape.force_2d().split_antimeridian()?);
    }
    let unwrapped = unwrap_longitude(shape)?;
    match kernel(&unwrapped)? {
        Shape::Point(point) => Ok(Shape::Point(rewrap_point(point)?)),
        other => Ok(other),
    }
}

/// Shared scalar/array gate for derived geographic points.
pub(crate) fn unary_antimeridian_derived(
    shape: &Shape,
    geographic: bool,
    strategy: DerivedPointStrategy,
    kernel: impl FnOnce(&Shape) -> Result<Shape>,
) -> Result<Shape> {
    if geographic && shape.crosses_antimeridian() {
        derived_point_unwrapped(shape, strategy, kernel)
    } else {
        kernel(shape)
    }
}

/// Geographic-crossing bounds in the west>east convention: ``minx`` is the
/// minimum eastern (``lon >= 0``) longitude and ``maxx`` the maximum western
/// (``lon < 0``) longitude; latitude extrema are ordinary planar min/max.
pub(crate) fn geographic_crossing_bounds(shape: &Shape) -> Option<Bounds> {
    geographic_crossing_bounds_for_shapes(std::iter::once(shape))
}

pub(crate) fn geographic_crossing_bounds_for_shapes(
    shapes: impl IntoIterator<Item = impl std::borrow::Borrow<Shape>>,
) -> Option<Bounds> {
    let mut eastern_min = None::<f64>;
    let mut western_max = None::<f64>;
    let mut miny = None::<f64>;
    let mut maxy = None::<f64>;
    let mut encloses_north = false;
    let mut encloses_south = false;
    let mut has_polar_ring = false;
    for shape in shapes {
        let shape = shape.borrow();
        shape.for_each_point(|point| {
            if point.x >= 0.0 {
                eastern_min = Some(eastern_min.map_or(point.x, |min| min.min(point.x)));
            } else {
                western_max = Some(western_max.map_or(point.x, |max| max.max(point.x)));
            }
            miny = Some(miny.map_or(point.y, |min| min.min(point.y)));
            maxy = Some(maxy.map_or(point.y, |max| max.max(point.y)));
        });
        encloses_north |= shape_encloses_pole(shape, true);
        encloses_south |= shape_encloses_pole(shape, false);
        has_polar_ring |= shape_has_polar_ring(shape);
    }
    let (mut minx, mut miny, mut maxx, mut maxy) = (eastern_min?, miny?, western_max?, maxy?);
    // A pole-enclosing ring covers the whole polar cap, so its envelope reaches
    // the pole and spans every longitude — the eastern_min/western_max sliver
    // (which only describes the seam window) would otherwise drop the cap. Mirrors
    // the spatial index's `crossing_index_bounds` so public bounds() and the index
    // agree.
    if has_polar_ring {
        (minx, maxx) = (-180.0, 180.0);
    }
    if encloses_north {
        maxy = 90.0;
    }
    if encloses_south {
        miny = -90.0;
    }
    Some(Bounds::new_unchecked(minx, miny, maxx, maxy))
}
