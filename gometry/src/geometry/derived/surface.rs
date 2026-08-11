use crate::geometry::{
    Bounds, CoordSeq, LineSeq, Point, Polygon, REDUCE_LANES, ReduceSimd, Shape, column_mean2,
    held_axis_interpolate, interpolate_segment_point, line_length, line_length_columns,
    point_distance, simd_argmin_f64,
};
/// Collect horizontal-bisector chord crossings for one ring's coordinate
/// columns.
fn push_chord_crossings(xs: &[f64], ys: &[f64], mid: f64, crossings: &mut Vec<f64>) -> bool {
    for index in 1..ys.len() {
        let (y0, y1) = (ys[index - 1], ys[index]);
        if (y0 <= mid) != (y1 <= mid) {
            let Some(x) = held_axis_interpolate(y0, xs[index - 1], y1, xs[index], mid) else {
                return false;
            };
            crossings.push(x);
        }
    }
    true
}

/// Finish the scanline interior point from sorted even-odd chord crossings.
fn point_from_chord_crossings(crossings: &mut [f64], mid: f64) -> Option<Point> {
    if crossings.len() < 2 || !crossings.len().is_multiple_of(2) {
        return None;
    }
    crossings.sort_unstable_by(f64::total_cmp);
    let widest = crossings
        .as_chunks::<2>()
        .0
        .iter()
        .max_by(|left, right| (left[1] - left[0]).total_cmp(&(right[1] - right[0])))?;
    Some(Point::new_unchecked_xy(
        f64::midpoint(widest[0], widest[1]),
        mid,
    ))
}

/// Column kernel for polygonal `point_on_surface`: widest even-odd chord on the
/// bbox horizontal bisector over one polygon's ring window range.
fn point_on_surface_polygon_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
    bounds: Bounds,
) -> Option<Point> {
    let mid = f64::midpoint(bounds.miny(), bounds.maxy());
    let mut crossings = Vec::new();
    for ring_index in rings {
        let start = ring_offsets[ring_index] as usize;
        let end = ring_offsets[ring_index + 1] as usize;
        if !push_chord_crossings(&xs[start..end], &ys[start..end], mid, &mut crossings) {
            return None;
        }
    }
    point_from_chord_crossings(&mut crossings, mid)
}

/// Degenerate polygon fallback: arc-length midpoint of the longest boundary
/// ring.
fn point_on_surface_polygon_boundary_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> Option<Point> {
    rings
        .map(|ring_index| {
            let start = ring_offsets[ring_index] as usize;
            let end = ring_offsets[ring_index + 1] as usize;
            (
                line_length_columns(&xs[start..end], &ys[start..end]),
                start,
                end,
            )
        })
        .max_by(|left, right| left.0.total_cmp(&right.0))
        .and_then(|(_, start, end)| point_on_surface_line_columns(&xs[start..end], &ys[start..end]))
}

/// Packed-polygon row kernel: scanline interior, then boundary lineal fallback.
pub(crate) fn point_on_surface_polygon_row_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
    bounds: Option<Bounds>,
) -> Option<Point> {
    if let Some(bounds) = bounds
        && let Some(point) =
            point_on_surface_polygon_columns(xs, ys, ring_offsets, rings.clone(), bounds)
    {
        return Some(point);
    }
    point_on_surface_polygon_boundary_columns(xs, ys, ring_offsets, rings)
}

/// Interior representative of a polygonal shape: the midpoint of the
/// widest even-odd chord where the horizontal bisector of the bounding
/// box crosses the boundary linework. The half-open crossing rule
/// (`start_below != end_below`) keeps the parity consistent through
/// vertices, exactly like the ring raycasts. Holes and all parts
/// participate as plain edges, so the widest interval always lies inside
/// some part and outside every hole. `None` falls back to the generic
/// engine (degenerate zero-area input where the bisector finds no chord).
pub(crate) fn polygonal_surface_point(shape: &Shape) -> Option<Point> {
    let bounds = shape.bounds()?;
    let mid = f64::midpoint(bounds.miny(), bounds.maxy());
    let mut crossings: Vec<f64> = Vec::new();
    // Columnar chord scan: read each ring's coordinate windows directly —
    // no per-segment struct or visitor indirection on the 2n-compare loop.
    let polygons: &[Polygon] = match shape {
        Shape::Polygon(polygon) => std::slice::from_ref(polygon),
        Shape::MultiPolygon(polygons) => polygons,
        _ => return None,
    };
    for polygon in polygons {
        for ring in polygon.rings() {
            if !push_chord_crossings(ring.xs(), ring.ys(), mid, &mut crossings) {
                return None;
            }
        }
    }
    point_from_chord_crossings(&mut crossings, mid)
}

/// Column kernel for lineal `point_on_surface`: arc-length midpoint of one
/// coordinate run (at least two vertices).
pub(crate) fn point_on_surface_line_columns(xs: &[f64], ys: &[f64]) -> Option<Point> {
    if xs.len() < 2 {
        return None;
    }
    let total = line_length_columns(xs, ys);
    if total == 0.0 {
        // Every vertex coincides: the first one is on the line.
        return Some(Point::new_unchecked_xy(xs[0], ys[0]));
    }
    let target = total / 2.0;
    let mut walked = 0.0;
    for index in 1..xs.len() {
        let (start, end) = (
            Point::new_unchecked_xy(xs[index - 1], ys[index - 1]),
            Point::new_unchecked_xy(xs[index], ys[index]),
        );
        let length = point_distance(start, end);
        if walked + length >= target {
            let fraction = if length == 0.0 {
                0.0
            } else {
                (target - walked) / length
            };
            let point = interpolate_segment_point(start, end, fraction);
            return Some(Point::new_unchecked_xy(point.x, point.y));
        }
        walked += length;
    }
    // Float accumulation can land a hair short of `target`: take the last vertex.
    Some(Point::new_unchecked_xy(xs[xs.len() - 1], ys[ys.len() - 1]))
}

/// A representative point ON the linework: the arc-length midpoint of the
/// LONGEST line piece. It always lies on the geometry (so `covered_by` holds —
/// the only contract `point_on_surface` owes), needs no geo-rs conversion, and
/// is a synthesized XY point like the polygonal kernel (Z/M is the ordinate
/// policy's call at the boundary, not carried here).
pub(crate) fn lineal_surface_point<'a>(
    lines: impl IntoIterator<Item = &'a CoordSeq>,
) -> Option<Point> {
    lines
        .into_iter()
        .filter(|line| line.xs().len() >= 2)
        .map(|line| (line_length(line), line))
        .max_by(|(left, _), (right, _)| left.total_cmp(right))
        .and_then(|(_, line)| point_on_surface_line_columns(line.xs(), line.ys()))
}

/// The multipoint vertex nearest its centroid — a representative that is always
/// one of the input points (so `covered_by` holds), no geo-rs conversion.
pub(crate) fn multipoint_surface_point(points: &CoordSeq) -> Option<Point> {
    let (xs, ys) = (points.xs(), points.ys());
    let count = xs.len();
    if count == 0 {
        return None;
    }
    let (centroid_x, centroid_y) = column_mean2(xs, ys)?;
    let score = |index: usize| {
        let (dx, dy) = (xs[index] - centroid_x, ys[index] - centroid_y);
        dx * dx + dy * dy
    };
    let nearest = simd_argmin_f64(count, score, |start| {
        let chunk = start / REDUCE_LANES;
        let (x_chunks, _) = xs.as_chunks::<REDUCE_LANES>();
        let (y_chunks, _) = ys.as_chunks::<REDUCE_LANES>();
        let dx = ReduceSimd::from_array(x_chunks[chunk]) - ReduceSimd::splat(centroid_x);
        let dy = ReduceSimd::from_array(y_chunks[chunk]) - ReduceSimd::splat(centroid_y);
        dx * dx + dy * dy
    })
    .expect("non-empty multipoint");
    Some(Point::new_unchecked_xy(xs[nearest], ys[nearest]))
}

/// Flatten a (possibly nested) collection into its areal, lineal, and puntal
/// components for the interior-point cascade.
fn gather_surface_components<'a>(
    parts: &'a [Shape],
    polygons: &mut Vec<&'a Polygon>,
    lines: &mut Vec<&'a CoordSeq>,
    points: &mut Vec<Point>,
) {
    for part in parts {
        match part {
            Shape::Point(point) => points.push(*point),
            Shape::MultiPoint(seq) => {
                points.extend((0..seq.len()).map(|index| seq.point_at(index)));
            },
            Shape::LineString(seq) => lines.push(seq.as_coords()),
            Shape::MultiLineString(seqs) => lines.extend(seqs.iter().map(LineSeq::as_coords)),
            Shape::Polygon(polygon) => polygons.push(polygon),
            Shape::MultiPolygon(polys) => polygons.extend(polys.iter()),
            Shape::GeometryCollection(inner) => {
                gather_surface_components(inner, polygons, lines, points);
            },
            Shape::Empty(..) => {},
        }
    }
}

/// Interior point of an arbitrary (possibly nested, mixed, or degenerate)
/// collection: JTS's dimensional cascade — the highest present dimension
/// wins, areal degeneracy falls through to its boundary linework, then to its
/// vertices. Reused for bare degenerate polygons too (via `from_ref`), which
/// the scanline kernel leaves to this boundary path.
pub(crate) fn collection_surface_point(parts: &[Shape]) -> Option<Point> {
    let mut polygons: Vec<&Polygon> = Vec::new();
    let mut lines: Vec<&CoordSeq> = Vec::new();
    let mut points: Vec<Point> = Vec::new();
    gather_surface_components(parts, &mut polygons, &mut lines, &mut points);
    if !polygons.is_empty() {
        let areal = Shape::MultiPolygon(polygons.iter().map(|&polygon| polygon.clone()).collect());
        if let Some(point) = polygonal_surface_point(&areal) {
            return Some(point);
        }
        // Degenerate areal: its boundary rings carry the representative point.
        if let Some(point) =
            lineal_surface_point(polygons.iter().flat_map(|polygon| polygon.rings()))
        {
            return Some(point);
        }
    }
    if !lines.is_empty()
        && let Some(point) = lineal_surface_point(lines.iter().copied())
    {
        return Some(point);
    }
    (!points.is_empty())
        .then(|| multipoint_surface_point(&CoordSeq::from_points(&points)))
        .flatten()
}
