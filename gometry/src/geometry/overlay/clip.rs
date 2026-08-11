#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::simd::cmp::SimdPartialOrd as _;

use ahash::HashSetExt as _;

use crate::geometry::overlay::{OverlayOp, binary_areal_overlay, rect_polygon};
use crate::geometry::{
    Bounds, CoordSeq, CoordinateAxes, Coordinates, LineSeq, Point, PointKey, Polygon, REDUCE_LANES,
    REDUCE_SIMD_MIN, ReduceSimd, Ring, Shape, XY, clip_segment, held_axis_interpolate,
    line_segments, same_point, simd_select_indices,
};
pub(crate) fn clip_polygonal(polygons: &[Polygon], rect: Bounds) -> Shape {
    polygon_parts_to_shape(clip_polygonal_parts(polygons, rect))
}

/// LINEAR Sutherland–Hodgman first (GEOS's clip is the same shape, but
/// gometry KEEPS the validity contract): the direct lane ships when no
/// hole reaches the window boundary (a crossing hole's arcs belong on
/// the result SHELL) and the clipped walk left no pinch or doubled
/// window-line span. Everything else takes the exact winding overlay.
/// Untouched vertices stay bit-exact on both paths. This is ALSO the
/// areal bucket of a rectangle-operand intersection (the GEOS
/// `RectangleIntersection` routing) — the surrounding overlay machinery
/// keeps the boundary-contact and Z/M semantics.
pub(in crate::geometry) fn clip_polygonal_parts<P: AsRef<Polygon>>(
    polygons: &[P],
    rect: Bounds,
) -> Vec<Polygon> {
    let holes_strictly_inside = polygons.iter().all(|polygon| {
        let polygon = polygon.as_ref();
        polygon.holes.iter().all(|hole| {
            Bounds::from_coords(hole.coords()).is_none_or(|bounds| {
                bounds.minx() > rect.minx()
                    && bounds.maxx() < rect.maxx()
                    && bounds.miny() > rect.miny()
                    && bounds.maxy() < rect.maxy()
            })
        })
    });
    if holes_strictly_inside
        && let Some(parts) = window_clipped_parts(polygons, rect)
        && !window_clip_artifacts(&parts, rect)
    {
        return parts;
    }
    binary_areal_overlay(
        polygons,
        std::slice::from_ref(&rect_polygon(rect)),
        OverlayOp::Intersection,
    )
}

pub(crate) fn clipped_linestring<C: Coordinates + ?Sized>(points: &C, rect: Bounds) -> Shape {
    line_parts_to_shape(clipped_line_parts(points, rect))
}

pub(crate) fn clipped_line_parts<C: Coordinates + ?Sized>(
    points: &C,
    rect: Bounds,
) -> Vec<Vec<Point>> {
    let mut parts: Vec<Vec<Point>> = Vec::new();
    for segment in line_segments(points) {
        let Some((start, end)) = clip_segment(segment.start, segment.end, rect) else {
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
    parts
}

pub(crate) fn line_parts_to_shape(mut parts: Vec<Vec<Point>>) -> Shape {
    parts.retain(|part| part.len() != 1);
    match parts.len() {
        0 => Shape::GeometryCollection(Vec::new()),
        1 => Shape::LineString(
            LineSeq::try_new(CoordSeq::from(parts.remove(0))).expect("line part is lineal"),
        ),
        _ => Shape::MultiLineString(
            parts
                .into_iter()
                .map(CoordSeq::from)
                .filter_map(|part| LineSeq::try_new(part).ok())
                .collect(),
        ),
    }
}

/// Collapse a polygonal decomposition to its narrowest shape: `POLYGON EMPTY`
/// for none, the bare polygon for one, a multipolygon otherwise.
pub(crate) fn polygon_parts_to_shape(mut parts: Vec<Polygon>) -> Shape {
    match parts.len() {
        0 => Shape::empty_polygon(),
        1 => Shape::Polygon(parts.remove(0)),
        _ => Shape::MultiPolygon(parts),
    }
}

/// Narrow the surviving merged chains: one chain IS the line, several
/// stay a multi-line (the historical output shapes).
/// Keep multipoint rows whose XY lies inside `rect` (inclusive bounds), Z/M
/// columns carried via [`CoordSeq::select`].
pub(crate) fn clip_multipoint_columns(points: &CoordSeq, rect: Bounds) -> CoordSeq {
    let xs = points.xs();
    let ys = points.ys();
    let len = xs.len();
    if len < REDUCE_SIMD_MIN {
        return points
            .iter()
            .filter(|point| rect.contains_point(*point))
            .collect::<Vec<_>>()
            .into();
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

/// Linear window clip for the overlay's `OverlayNG`-style optimization: each
/// polygon ring runs Sutherland–Hodgman against the window's four
/// half-planes (untouched vertices stay bit-exact; crossings interpolate on
/// the held axis), lines take the piecewise clipper, points filter. Output
/// quality is ENGINE-grade, not contract-grade: zero-width bridges along the
/// window are legal here because the overlay self-nodes every input — the
/// public `clip_by_rect` keeps its validity contract through the winding
/// engine. Intermediate vertices are X/Y-only; the overlay's final
/// `carry_ordinates` resolves Z/M from the ORIGINAL operands, and
/// window-boundary vertices can never survive into a result (the window
/// strictly contains the other operand's envelope).
pub(in crate::geometry) fn window_clip_shape(shape: &Shape, rect: Bounds) -> Shape {
    match shape {
        Shape::Point(point) => {
            if rect.contains_point(*point) {
                shape.clone()
            } else {
                Shape::GeometryCollection(Vec::new())
            }
        },
        Shape::MultiPoint(points) => Shape::MultiPoint(clip_multipoint_columns(points, rect)),
        Shape::LineString(line) => Shape::MultiLineString(
            clipped_line_parts(line, rect)
                .into_iter()
                .map(CoordSeq::from)
                .filter_map(|part| LineSeq::try_new(part).ok())
                .collect(),
        ),
        Shape::MultiLineString(lines) => Shape::MultiLineString(
            lines
                .iter()
                .flat_map(|line| clipped_line_parts(line, rect))
                .map(CoordSeq::from)
                .filter_map(|part| LineSeq::try_new(part).ok())
                .collect(),
        ),
        Shape::Polygon(polygon) => window_clip_polygons(std::slice::from_ref(polygon), rect),
        Shape::MultiPolygon(polygons) => window_clip_polygons(polygons, rect),
        Shape::GeometryCollection(parts) => Shape::GeometryCollection(
            parts
                .iter()
                .map(|part| window_clip_shape(part, rect))
                .collect(),
        ),
        Shape::Empty(..) => shape.clone(),
    }
}

pub(crate) fn window_clip_polygons(polygons: &[Polygon], rect: Bounds) -> Shape {
    let Some(mut clipped) = window_clipped_parts(polygons, rect) else {
        return polygon_parts_to_shape(binary_areal_overlay(
            polygons,
            std::slice::from_ref(&rect_polygon(rect)),
            OverlayOp::Intersection,
        ));
    };
    match clipped.len() {
        0 => Shape::empty_polygon(),
        1 => Shape::Polygon(clipped.pop().expect("one polygon")),
        _ => Shape::MultiPolygon(clipped),
    }
}

/// The raw per-ring Sutherland–Hodgman pass behind [`window_clip_polygons`]
/// and the public clip's direct lane.
pub(crate) fn window_clipped_parts<P: AsRef<Polygon>>(
    polygons: &[P],
    rect: Bounds,
) -> Option<Vec<Polygon>> {
    let mut clipped = Vec::with_capacity(polygons.len());
    for polygon in polygons {
        let polygon = polygon.as_ref();
        let shell = window_clip_ring(polygon.shell.coords(), rect)?;
        if shell.coord_count() < Ring::MIN_VERTICES_CLOSED {
            continue;
        }
        let holes = polygon
            .holes
            .iter()
            .map(|hole| {
                let ring = window_clip_ring(hole.coords(), rect)?;
                (ring.coord_count() >= Ring::MIN_VERTICES_CLOSED)
                    .then(|| Ring::from_trusted_closed(ring))
            })
            .collect::<Option<Vec<_>>>()?;
        clipped.push(Polygon::new(Ring::from_trusted_closed(shell), holes));
    }
    Some(clipped)
}

/// Sutherland–Hodgman artifact scan gating the public clip's DIRECT lane:
/// on valid input the clipped walk is exact unless it pinches (repeated
/// vertex) or doubles back along a window line (overlapping spans — the
/// bridge a concave crossing leaves). Conservative: a hit only costs the
/// winding-engine fallback. Equality on the window ordinates is exact by
/// construction (crossings hold the clipped axis EXACTLY).
#[expect(
    clippy::float_cmp,
    reason = "window crossings assign the clipped ordinate exactly, so equality detects topological artifacts"
)]
pub(crate) fn window_clip_artifacts(parts: &[Polygon], rect: Bounds) -> bool {
    let mut spans: [Vec<(f64, f64)>; 4] = [const { Vec::new() }; 4];
    let ordered = |a: f64, b: f64| if a <= b { (a, b) } else { (b, a) };
    for polygon in parts {
        for ring in polygon.rings() {
            let (xs, ys) = (ring.xs(), ring.ys());
            let count = xs.len();
            let scan = count.saturating_sub(1); // rings arrive closed
            // Only boundary-resting vertices can repeat (Sutherland–Hodgman
            // passes interior vertices through verbatim once): hash the
            // few on-window vertices, not the whole ring.
            let mut seen: crate::collections::HashSet<PointKey> =
                crate::collections::HashSet::new();
            for index in 0..scan {
                let (x, y) = (xs[index], ys[index]);
                if (x == rect.minx() || x == rect.maxx() || y == rect.miny() || y == rect.maxy())
                    && !seen.insert(PointKey::new(XY::new(x, y)))
                {
                    return true;
                }
                let (x0, y0, x1, y1) = (xs[index], ys[index], xs[index + 1], ys[index + 1]);
                if x0 == rect.minx() && x1 == rect.minx() {
                    spans[0].push(ordered(y0, y1));
                } else if x0 == rect.maxx() && x1 == rect.maxx() {
                    spans[1].push(ordered(y0, y1));
                } else if y0 == rect.miny() && y1 == rect.miny() {
                    spans[2].push(ordered(x0, x1));
                } else if y0 == rect.maxy() && y1 == rect.maxy() {
                    spans[3].push(ordered(x0, x1));
                }
            }
        }
    }
    for line in &mut spans {
        line.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
        let mut max_hi = f64::NEG_INFINITY;
        for &(lo, hi) in line.iter() {
            if lo < max_hi {
                return true;
            }
            max_hi = max_hi.max(hi);
        }
    }
    false
}

/// One closed ring through four Sutherland–Hodgman half-plane passes.
/// Returns the CLOSED clipped ring (first point repeated), or fewer than 4
/// points when the ring leaves the window entirely.
#[expect(
    clippy::too_many_lines,
    reason = "the four ordered clipping passes share one local state machine; splitting it duplicates the hot traversal"
)]
pub(crate) fn window_clip_ring(ring: &CoordSeq, rect: Bounds) -> Option<CoordSeq> {
    // Four MONOMORPHIZED half-plane passes straight over the coordinate
    // columns: the previous fn-pointer plane table cost two indirect
    // calls per vertex per pass, and the `point_at` input walk
    // materialized a 40-byte `Point` per vertex — together 42% of the
    // public clip's profile. Crossings hold the clipped axis EXACT and
    // interpolate the other.
    fn pass(
        xs: &[f64],
        ys: &[f64],
        out_x: &mut Vec<f64>,
        out_y: &mut Vec<f64>,
        inside: impl Fn(f64, f64) -> bool,
        crossing: impl Fn(f64, f64, f64, f64) -> Option<(f64, f64)>,
    ) -> bool {
        out_x.clear();
        out_y.clear();
        let count = xs.len();
        let mut emit = |x0: f64, y0: f64, x1: f64, y1: f64| -> bool {
            let (from_in, to_in) = (inside(x0, y0), inside(x1, y1));
            if from_in {
                out_x.push(x0);
                out_y.push(y0);
            }
            if from_in != to_in {
                let Some((cx, cy)) = crossing(x0, y0, x1, y1) else {
                    return false;
                };
                out_x.push(cx);
                out_y.push(cy);
            }
            true
        };
        // Linear edges, then the closing edge once — no per-edge modulo.
        for index in 1..count {
            if !emit(xs[index - 1], ys[index - 1], xs[index], ys[index]) {
                return false;
            }
        }
        emit(xs[count - 1], ys[count - 1], xs[0], ys[0])
    }
    let count = ring.coord_count();
    if count < Ring::MIN_VERTICES_CLOSED {
        return Some(CoordSeq::empty(CoordinateAxes::XY));
    }
    // Open form (cyclic processing re-closes at the end); the FIRST pass
    // reads the ring's own columns — no upfront copy.
    let mut xs: Vec<f64> = Vec::with_capacity(count + 8);
    let mut ys: Vec<f64> = Vec::with_capacity(count + 8);
    let mut scratch_x: Vec<f64> = Vec::with_capacity(count + 8);
    let mut scratch_y: Vec<f64> = Vec::with_capacity(count + 8);
    if !pass(
        &ring.xs()[..count - 1],
        &ring.ys()[..count - 1],
        &mut xs,
        &mut ys,
        |x, _| x >= rect.minx(),
        |x0, y0, x1, y1| {
            held_axis_interpolate(x0, y0, x1, y1, rect.minx()).map(|y| (rect.minx(), y))
        },
    ) {
        return None;
    }
    if xs.len() >= Ring::MIN_VERTICES_OPEN {
        if !pass(
            &xs,
            &ys,
            &mut scratch_x,
            &mut scratch_y,
            |x, _| x <= rect.maxx(),
            |x0, y0, x1, y1| {
                held_axis_interpolate(x0, y0, x1, y1, rect.maxx()).map(|y| (rect.maxx(), y))
            },
        ) {
            return None;
        }
        std::mem::swap(&mut xs, &mut scratch_x);
        std::mem::swap(&mut ys, &mut scratch_y);
    }
    if xs.len() >= Ring::MIN_VERTICES_OPEN {
        if !pass(
            &xs,
            &ys,
            &mut scratch_x,
            &mut scratch_y,
            |_, y| y >= rect.miny(),
            |x0, y0, x1, y1| {
                held_axis_interpolate(y0, x0, y1, x1, rect.miny()).map(|x| (x, rect.miny()))
            },
        ) {
            return None;
        }
        std::mem::swap(&mut xs, &mut scratch_x);
        std::mem::swap(&mut ys, &mut scratch_y);
    }
    if xs.len() >= Ring::MIN_VERTICES_OPEN {
        if !pass(
            &xs,
            &ys,
            &mut scratch_x,
            &mut scratch_y,
            |_, y| y <= rect.maxy(),
            |x0, y0, x1, y1| {
                held_axis_interpolate(y0, x0, y1, x1, rect.maxy()).map(|x| (x, rect.maxy()))
            },
        ) {
            return None;
        }
        std::mem::swap(&mut xs, &mut scratch_x);
        std::mem::swap(&mut ys, &mut scratch_y);
    }
    if xs.len() < Ring::MIN_VERTICES_OPEN {
        return Some(CoordSeq::empty(CoordinateAxes::XY));
    }
    // Close positionally and hand the columns over as-is — no `Point`
    // staging, no re-packing in `Ring::from_trusted_closed`.
    xs.push(xs[0]);
    ys.push(ys[0]);
    Some(CoordSeq::from_columns(xs.into(), ys.into(), None, None))
}
