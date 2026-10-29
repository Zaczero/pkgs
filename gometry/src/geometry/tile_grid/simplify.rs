//! Boundary-protected simplification of prepared integer paths.

use super::{CoordSeq, ExactRational, LineSeq, Point, Polygon, Result, Ring, Shape, SharedSlice};
use crate::geometry::coverage::{TopologyGuard, simplify_chain_kept_with_try_remove};
use crate::geometry::{CoordinateAxes, Segment, SimplifyMethod, XY, same_point};

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "protection flags match the path length and simplification returns indices into that same path"
)]
fn simplify_path(
    points: &[Point],
    cyclic: bool,
    boundaries: &[ExactRational],
    tolerance: f64,
    method: SimplifyMethod,
    guard: Option<&TopologyGuard<'_>>,
) -> Result<Vec<Point>> {
    let mut protected = vec![false; points.len()];
    for (index, point) in points.iter().enumerate() {
        let x = ExactRational::from_f64(point.x);
        let y = ExactRational::from_f64(point.y);
        let mut on_boundary = false;
        for boundary in boundaries {
            if x.try_cmp(boundary)?.is_eq() || y.try_cmp(boundary)?.is_eq() {
                on_boundary = true;
                break;
            }
        }
        if on_boundary {
            protected[index] = true;
            if index > 0 {
                protected[index - 1] = true;
            } else if cyclic && points.len() > 1 {
                protected[points.len() - 2] = true;
            }
            if index + 1 < points.len() {
                protected[index + 1] = true;
            } else if cyclic && points.len() > 1 {
                protected[1] = true;
            }
        }
    }
    let xy = |index: usize| XY::new(points[index].x, points[index].y);
    let kept = simplify_chain_kept_with_try_remove(
        points.len(),
        xy,
        cyclic,
        tolerance,
        method,
        |_, vertex, _, previous, point, next| {
            if protected[vertex] {
                return Ok(false);
            }
            if guard.is_none() {
                let ax = point.x - previous.x;
                let ay = point.y - previous.y;
                let bx = next.x - point.x;
                let by = next.y - point.y;
                if ax * bx + ay * by <= 0.0 || same_point(previous, next) {
                    return Ok(false);
                }
            }
            Ok(guard.is_none_or(|guard| guard.removal_is_safe(previous, point, next, &[])))
        },
    )?;
    Ok(kept.into_iter().map(|index| points[index]).collect())
}

fn segments(shape: &Shape) -> Vec<Segment> {
    let mut result = Vec::new();
    let mut polygon = |polygon: &Polygon| {
        for ring in polygon.rings() {
            let points = ring.iter().collect::<Vec<_>>();
            result.extend(points.array_windows::<2>().map(|[start, end]| Segment {
                start: XY::new(start.x, start.y),
                end: XY::new(end.x, end.y),
            }));
        }
    };
    match shape {
        Shape::Polygon(value) => polygon(value),
        Shape::MultiPolygon(values) => values.iter().for_each(polygon),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => {},
    }
    result
}

pub(super) fn simplify_tile_shape(
    shape: Shape,
    boundaries: &[ExactRational],
    tolerance: f64,
    method: SimplifyMethod,
) -> Result<Shape> {
    let edges = segments(&shape);
    let vertices = edges
        .iter()
        .flat_map(|edge| [[edge.start.x, edge.start.y], [edge.end.x, edge.end.y]])
        .collect();
    let guard = TopologyGuard::new(&edges, vertices);
    let line = |line: &LineSeq| -> Result<LineSeq> {
        let points = line.iter().collect::<Vec<_>>();
        let points = simplify_path(&points, false, boundaries, tolerance, method, None)?;
        LineSeq::try_new(CoordSeq::try_from_points(&points)?)
    };
    let polygon = |polygon: &Polygon| -> Result<Polygon> {
        let ring = |ring: &Ring| -> Result<Ring> {
            let points = ring.iter().collect::<Vec<_>>();
            let points = simplify_path(&points, true, boundaries, tolerance, method, Some(&guard))?;
            Ring::try_from_closed(CoordSeq::try_from_points(&points)?)
        };
        Ok(Polygon {
            shell: ring(&polygon.shell)?,
            holes: SharedSlice::from_vec(
                polygon.holes.iter().map(ring).collect::<Result<Vec<_>>>()?,
            ),
        })
    };
    let output = match &shape {
        Shape::LineString(value) => Shape::LineString(line(value)?),
        Shape::MultiLineString(values) => Shape::narrow_lines(
            CoordinateAxes::XY,
            values.iter().map(line).collect::<Result<Vec<_>>>()?,
        ),
        Shape::Polygon(value) => Shape::Polygon(polygon(value)?),
        Shape::MultiPolygon(values) => Shape::narrow_polygons(
            CoordinateAxes::XY,
            values.iter().map(polygon).collect::<Result<Vec<_>>>()?,
        ),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => {
            return Ok(shape);
        },
    };
    if matches!(output, Shape::Polygon(_) | Shape::MultiPolygon(_)) && output.validate()?.is_some()
    {
        Ok(shape)
    } else {
        Ok(output)
    }
}
