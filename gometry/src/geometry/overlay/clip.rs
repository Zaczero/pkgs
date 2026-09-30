#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use std::simd::cmp::SimdPartialOrd as _;

use crate::error::Result;
use crate::geometry::overlay::clip_polygonal_rectangle;
use crate::geometry::{
    Bounds, CoordSeq, CoordinateAxes, Coordinates, GeometryKind, LineSeq, Point, Polygon,
    REDUCE_LANES, REDUCE_SIMD_MIN, ReduceSimd, Shape, clip_segment, line_segments, same_point,
    simd_select_indices,
};
pub(crate) fn clip_polygonal(polygons: &[Polygon], rect: Bounds) -> crate::error::Result<Shape> {
    Ok(polygon_parts_to_shape(clip_polygonal_parts(
        polygons, rect,
    )?))
}

/// Polygon clipping is an exact partition intersection. Degenerate closed
/// rectangles therefore remain valid lower-dimensional source support until
/// the polygon-only sink narrows the result.
pub(in crate::geometry) fn clip_polygonal_parts<P: AsRef<Polygon>>(
    polygons: &[P],
    rect: Bounds,
) -> Result<Vec<Polygon>> {
    clip_polygonal_rectangle(polygons, rect)
}

pub(crate) fn clipped_linestring<C: Coordinates + ?Sized>(
    points: &C,
    rect: Bounds,
) -> Result<Shape> {
    line_parts_to_shape(clipped_line_parts(points, rect)?)
}

pub(crate) fn clipped_line_parts<C: Coordinates + ?Sized>(
    points: &C,
    rect: Bounds,
) -> Result<Vec<Vec<Point>>> {
    let mut parts: Vec<Vec<Point>> = Vec::new();
    for segment in line_segments(points) {
        let Some((start, end)) = clip_segment(segment.start, segment.end, rect)? else {
            continue;
        };
        if same_point(start, end) {
            continue;
        }
        match parts.last_mut() {
            Some(part) if part.last().is_some_and(|point| point.xy() == start) => {
                part.push(end.point());
            },
            _ => parts.push(vec![start.point(), end.point()]),
        }
    }
    Ok(parts)
}

pub(crate) fn line_parts_to_shape(mut parts: Vec<Vec<Point>>) -> crate::error::Result<Shape> {
    parts.retain(|part| part.len() != 1);
    let axes = parts
        .first()
        .and_then(|part| part.first())
        .map_or(CoordinateAxes::XY, |point| point.axes);
    match parts.len() {
        0 => Ok(Shape::typed_empty(
            GeometryKind::GeometryCollection,
            CoordinateAxes::XY,
        )),
        1 => Ok(Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_points(&parts.remove(0))?,
        ))),
        _ => Ok(Shape::multi_line_string(
            axes,
            parts
                .into_iter()
                .map(|part| CoordSeq::try_from_points(&part).and_then(LineSeq::try_new))
                .collect::<crate::error::Result<Vec<_>>>()?,
        )),
    }
}

/// Collapse a polygonal decomposition to its narrowest shape: `POLYGON EMPTY`
/// for none, the bare polygon for one, a multipolygon otherwise.
pub(crate) fn polygon_parts_to_shape(parts: Vec<Polygon>) -> Shape {
    Shape::narrow_polygons(CoordinateAxes::XY, parts)
}

/// Narrow the surviving merged chains: one chain IS the line, several
/// stay a multi-line (the historical output shapes).
/// Keep multipoint rows whose XY lies inside `rect` (inclusive bounds), Z/M
/// columns carried via [`CoordSeq::select`].
pub(crate) fn clip_multipoint_columns(
    points: &CoordSeq,
    rect: Bounds,
) -> crate::error::Result<CoordSeq> {
    let xs = points.xs();
    let ys = points.ys();
    let len = xs.len();
    if len < REDUCE_SIMD_MIN {
        let kept = points
            .iter()
            .filter(|point| rect.contains_point(*point))
            .collect::<Vec<_>>();
        return CoordSeq::try_from_points(&kept);
    }
    let (minx, miny, maxx, maxy) = (rect.minx(), rect.miny(), rect.maxx(), rect.maxy());
    let (x_chunks, _) = xs.as_chunks::<REDUCE_LANES>();
    let (y_chunks, _) = ys.as_chunks::<REDUCE_LANES>();
    let kept = simd_select_indices(
        len,
        |index| {
            let (x, y) = (xs[index], ys[index]);
            x >= minx && x <= maxx && y >= miny && y <= maxy
        },
        |start| {
            let chunk = start / REDUCE_LANES;
            let x_chunk = ReduceSimd::from_array(x_chunks[chunk]);
            let y_chunk = ReduceSimd::from_array(y_chunks[chunk]);
            x_chunk.simd_ge(ReduceSimd::splat(minx))
                & x_chunk.simd_le(ReduceSimd::splat(maxx))
                & y_chunk.simd_ge(ReduceSimd::splat(miny))
                & y_chunk.simd_le(ReduceSimd::splat(maxy))
        },
    );
    points.select(kept.into_iter())
}
