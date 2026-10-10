use super::centroid::{centroid_line_columns, lineal_centroid, point_centroid};
use crate::error::Result;
use crate::geometry::{
    Bounds, Component, CoordSeq, Point, Polygon, REDUCE_LANES, ReduceSimd, Ring, RingClass, Shape,
    column_mean2, held_axis_interpolate, line_length_columns, point_on_segment,
    ring_classify_point, ring_classify_point_columns, same_point, simd_argmin_f64,
};

/// Collect horizontal-bisector chord crossings for one ring's coordinate
/// columns.
fn push_chord_crossings(xs: &[f64], ys: &[f64], mid: f64, crossings: &mut Vec<f64>) -> bool {
    // Both columns come from one admitted XY sequence, so their paired
    // iterators cover the same source edges.
    for ((x0, x1), (y0, y1)) in xs
        .iter()
        .zip(xs.iter().skip(1))
        .zip(ys.iter().zip(ys.iter().skip(1)))
    {
        if (*y0 <= mid) != (*y1 <= mid) {
            let Some(x) = held_axis_interpolate(*y0, *x0, *y1, *x1, mid) else {
                return false;
            };
            crossings.push(x);
        }
    }
    true
}

/// Map the widest even-odd chord, retaining the existing later-chord tie rule.
fn widest_even_odd_chord<R, F>(crossings: &mut [f64], map: F) -> Option<R>
where
    F: FnOnce(f64, f64) -> R,
{
    if crossings.len() < 2 || !crossings.len().is_multiple_of(2) {
        return None;
    }
    crossings.sort_unstable_by(f64::total_cmp);
    let [start, end] = crossings
        .as_chunks::<2>()
        .0
        .iter()
        .copied()
        .max_by(|left, right| {
            let [left_start, left_end] = *left;
            let [right_start, right_end] = *right;
            (left_end - left_start).total_cmp(&(right_end - right_start))
        })?;
    Some(map(start, end))
}

fn polygon_covers_point(polygon: &Polygon, point: Point) -> bool {
    ring_classify_point(polygon.shell.coords(), point) != RingClass::Exterior
        && polygon
            .holes
            .iter()
            .all(|hole| ring_classify_point(hole.coords(), point) != RingClass::Interior)
}

fn ring_classify_columns(xs: &[f64], ys: &[f64], start: i32, end: i32, point: Point) -> RingClass {
    let (start, end) = (start as usize, end as usize);
    let (xs_prefix, _) = xs.split_at(end);
    let (_, xs_ring) = xs_prefix.split_at(start);
    let (ys_prefix, _) = ys.split_at(end);
    let (_, ys_ring) = ys_prefix.split_at(start);
    ring_classify_point_columns(xs_ring, ys_ring, point)
}

fn polygon_columns_cover_point(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
    point: Point,
) -> bool {
    let mut saw_shell = false;
    let starts = ring_offsets.iter().skip(rings.start).copied();
    let ends = ring_offsets.iter().skip(rings.start).copied().skip(1);
    for (ordinal, (start, end)) in starts.zip(ends).take(rings.len()).enumerate() {
        let location = ring_classify_columns(xs, ys, start, end, point);
        if ordinal == 0 {
            saw_shell = true;
            if location == RingClass::Exterior {
                return false;
            }
        } else if location == RingClass::Interior {
            return false;
        }
    }
    saw_shell
}

/// Binary64 squared distance used only to rank on-geometry representative candidates.
fn squared_xy_distance(point: Point, center: Point) -> f64 {
    let (dx, dy) = (point.x - center.x, point.y - center.y);
    dx * dx + dy * dy
}

/// Column kernel for polygonal `point_on_surface`: widest even-odd chord on the
/// bbox horizontal bisector over one polygon's ring window range.
fn point_on_surface_polygon_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
    bounds: Bounds,
    crossings: &mut Vec<f64>,
) -> Option<Point> {
    let mid = f64::midpoint(bounds.miny(), bounds.maxy());
    let starts = ring_offsets.iter().skip(rings.start).copied();
    let ends = ring_offsets.iter().skip(rings.start).copied().skip(1);
    for (start, end) in starts.zip(ends).take(rings.len()) {
        let start = start as usize;
        let end = end as usize;
        let (xs_prefix, _) = xs.split_at(end);
        let (_, xs_ring) = xs_prefix.split_at(start);
        let (ys_prefix, _) = ys.split_at(end);
        let (_, ys_ring) = ys_prefix.split_at(start);
        if !push_chord_crossings(xs_ring, ys_ring, mid, crossings) {
            return None;
        }
    }
    widest_even_odd_chord(crossings, |start, end| {
        let point = Point::new_raw_xy(f64::midpoint(start, end), mid);
        polygon_columns_cover_point(xs, ys, ring_offsets, rings, point).then_some(point)
    })
    .flatten()
}

/// Degenerate polygon fallback: arc-length midpoint of the longest boundary
/// ring.
fn point_on_surface_polygon_boundary_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> Option<Point> {
    let starts = ring_offsets.iter().skip(rings.start).copied();
    let ends = ring_offsets.iter().skip(rings.start).copied().skip(1);
    starts
        .zip(ends)
        .take(rings.len())
        .map(|(start, end)| {
            let start = start as usize;
            let end = end as usize;
            let (xs_prefix, _) = xs.split_at(end);
            let (_, xs_ring) = xs_prefix.split_at(start);
            let (ys_prefix, _) = ys.split_at(end);
            let (_, ys_ring) = ys_prefix.split_at(start);
            (line_length_columns(xs_ring, ys_ring), xs_ring, ys_ring)
        })
        .max_by(|left, right| left.0.total_cmp(&right.0))
        .and_then(|(_, xs_ring, ys_ring)| point_on_surface_line_columns(xs_ring, ys_ring))
}

/// Packed-polygon row kernel: scanline interior, then boundary lineal fallback.
pub(crate) fn point_on_surface_polygon_row_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
    bounds: Option<Bounds>,
    crossings: &mut Vec<f64>,
) -> Option<Point> {
    crossings.clear();
    if let Some(bounds) = bounds
        && let Some(point) =
            point_on_surface_polygon_columns(xs, ys, ring_offsets, rings.clone(), bounds, crossings)
    {
        return Some(point);
    }
    point_on_surface_polygon_boundary_columns(xs, ys, ring_offsets, rings)
}

fn polygon_boundary_point(polygon: &Polygon) -> Option<Point> {
    polygon
        .rings()
        .map(|ring| {
            let coords = ring.coords();
            (
                line_length_columns(coords.xs(), coords.ys()),
                coords.xs(),
                coords.ys(),
            )
        })
        .max_by(|left, right| left.0.total_cmp(&right.0))
        .and_then(|(_, xs, ys)| point_on_surface_line_columns(xs, ys))
}

/// Representative of borrowed polygon components. Scan each
/// component's own rings at the horizontal bisector of their combined bounds,
/// then keep the widest component chord, retaining the first component on a
/// tie. The half-open crossing rule (`start_below != end_below`) keeps each
/// component's even-odd parity consistent through vertices, including its
/// holes. The final rounded point is checked against the winning polygon; when
/// rounding moved it outside, the same polygon's boundary supplies the result.
/// `None` falls back to the dimensional cascade when the global bisector finds
/// no chord or an interpolated crossing is unrepresentable.
pub(crate) fn polygonal_surface_point<'a>(
    polygons: impl Iterator<Item = &'a Polygon> + Clone,
) -> Option<Point> {
    let mut polygon_bounds = polygons.clone().filter_map(Polygon::bounds);
    let mut bounds = polygon_bounds.next()?;
    for polygon_bounds in polygon_bounds {
        bounds.include_bounds(polygon_bounds);
    }
    let mid = f64::midpoint(bounds.miny(), bounds.maxy());
    let mut crossings: Vec<f64> = Vec::new();
    // Columnar chord scan: read each ring's coordinate windows directly —
    // no per-segment struct or visitor indirection on the 2n-compare loop.
    let mut best: Option<(f64, f64, f64, &'a Polygon)> = None;
    for polygon in polygons {
        crossings.clear();
        for ring in polygon.rings() {
            if !push_chord_crossings(ring.xs(), ring.ys(), mid, &mut crossings) {
                return None;
            }
        }
        let Some([start, end]) = widest_even_odd_chord(&mut crossings, |start, end| [start, end])
        else {
            continue;
        };
        let width = end - start;
        if best
            .as_ref()
            .is_none_or(|(best_width, ..)| width.total_cmp(best_width).is_gt())
        {
            best = Some((width, start, end, polygon));
        }
    }
    let (_, start, end, polygon) = best?;
    let point = Point::new_raw_xy(f64::midpoint(start, end), mid);
    Some(if polygon_covers_point(polygon, point) {
        point
    } else {
        polygon_boundary_point(polygon)?
    })
}

/// Lineal representative from one coordinate run, preferring the interior
/// vertex nearest its centroid. A two-coordinate line uses its rounded midpoint
/// only when exact predicates prove that point lies strictly inside the segment.
pub(crate) fn point_on_surface_line_columns(xs: &[f64], ys: &[f64]) -> Option<Point> {
    if xs.len() < 2 {
        return None;
    }
    let centroid = (xs.len() > 3)
        .then(|| centroid_line_columns(xs, ys))
        .flatten();
    point_on_surface_line_columns_at(xs, ys, centroid)
}

fn point_on_surface_line_columns_at(
    xs: &[f64],
    ys: &[f64],
    centroid: Option<(f64, f64)>,
) -> Option<Point> {
    if xs.len() < 2 {
        return None;
    }

    if xs.len() > 2 {
        let mut interior = xs
            .iter()
            .zip(ys)
            .enumerate()
            .skip(1)
            .take(xs.len().saturating_sub(2));
        let (_, (&first_x, &first_y)) = interior.next()?;
        let first = Point::new_raw_xy(first_x, first_y);
        let Some((center_x, center_y)) = centroid else {
            return Some(first);
        };
        let center = Point::new_raw_xy(center_x, center_y);
        let mut best_distance = squared_xy_distance(first, center);
        let mut best = first;
        for (_, (&x, &y)) in interior {
            let point = Point::new_raw_xy(x, y);
            let distance = squared_xy_distance(point, center);
            if distance < best_distance {
                best = point;
                best_distance = distance;
            }
        }
        return Some(best);
    }

    let start = Point::new_raw_xy(*xs.first()?, *ys.first()?);
    let end = Point::new_raw_xy(*xs.last()?, *ys.last()?);
    let midpoint = Point::new_raw_xy(f64::midpoint(start.x, end.x), f64::midpoint(start.y, end.y));
    if !same_point(midpoint, start)
        && !same_point(midpoint, end)
        && point_on_segment(midpoint, start, end)
    {
        return Some(midpoint);
    }

    let center = centroid.map_or(midpoint, |(x, y)| Point::new_raw_xy(x, y));
    let start_distance = squared_xy_distance(start, center);
    let end_distance = squared_xy_distance(end, center);
    Some(if end_distance < start_distance {
        end
    } else {
        start
    })
}

/// A representative point ON the linework: the candidate nearest the lineal
/// centroid, using stored interior vertices and certified single-segment
/// midpoints. No rounded interpolation is allowed to leave the source line.
pub(crate) fn lineal_surface_point<'a>(
    lines: impl IntoIterator<Item = &'a CoordSeq>,
    centroid: Option<(f64, f64)>,
) -> Option<Point> {
    let mut best = None;
    let centroid_point = centroid.map(|(x, y)| Point::new_raw_xy(x, y));
    for line in lines {
        let Some(candidate) = point_on_surface_line_columns_at(line.xs(), line.ys(), centroid)
        else {
            continue;
        };
        let Some(center) = centroid_point else {
            return Some(candidate);
        };
        let distance = squared_xy_distance(candidate, center);
        if best.is_none_or(|(best_distance, _)| distance < best_distance) {
            best = Some((distance, candidate));
        }
    }
    best.map(|(_, point)| point)
}

/// The multipoint vertex nearest its centroid — a representative that is always
/// one of the input points (so `covered_by` holds), no geo-rs conversion.
pub(crate) fn multipoint_surface_point(xs: &[f64], ys: &[f64]) -> Option<Point> {
    let count = xs.len();
    if count == 0 {
        return None;
    }
    let (centroid_x, centroid_y) = column_mean2(xs, ys)?;
    multipoint_surface_point_at(xs, ys, Point::new_raw_xy(centroid_x, centroid_y))
}

#[expect(
    clippy::indexing_slicing,
    reason = "argmin ordinals and SIMD lane groups are generated from these equal-length columns"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the selected vertex is defined by the existing IEEE-754 squared-distance score"
)]
pub(crate) fn multipoint_surface_point_at(
    xs: &[f64],
    ys: &[f64],
    centroid: Point,
) -> Option<Point> {
    let count = xs.len();
    if count == 0 {
        return None;
    }
    debug_assert_eq!(xs.len(), ys.len());
    let score =
        |index: usize| squared_xy_distance(Point::new_raw_xy(xs[index], ys[index]), centroid);
    let nearest = simd_argmin_f64(count, score, |start| {
        let chunk = start / REDUCE_LANES;
        let (x_chunks, _) = xs.as_chunks::<REDUCE_LANES>();
        let (y_chunks, _) = ys.as_chunks::<REDUCE_LANES>();
        let dx = ReduceSimd::from_array(x_chunks[chunk]) - ReduceSimd::splat(centroid.x);
        let dy = ReduceSimd::from_array(y_chunks[chunk]) - ReduceSimd::splat(centroid.y);
        dx * dx + dy * dy
    })
    .expect("non-empty multipoint");
    Some(Point::new_raw_xy(xs[nearest], ys[nearest]))
}

/// Point-set representative using a center already computed by the shared
/// centroid owner. Candidate order follows first source occurrence; repeated
/// shared collection owners are visited once because they add no new site.
pub(crate) fn point_set_surface_point_at(shape: &Shape, center: Point) -> Option<Point> {
    match shape {
        Shape::Point(point) => Some(Point::new_raw_xy(point.x, point.y)),
        Shape::MultiPoint(points) => multipoint_surface_point_at(points.xs(), points.ys(), center),
        Shape::GeometryCollection(_) => {
            let mut best: Option<(f64, Point)> = None;
            shape.for_each_first_point_run(|point| {
                let candidate = Point::new_raw_xy(point.x, point.y);
                let distance = squared_xy_distance(candidate, center);
                if best.is_none_or(|(current, _)| distance.total_cmp(&current).is_lt()) {
                    best = Some((distance, candidate));
                }
            });
            best.map(|(_, point)| point)
        },
        Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::Empty(..) => None,
    }
}

fn first_surface_polygons(shape: &Shape) -> Vec<&Polygon> {
    let mut polygons = Vec::new();
    match shape.try_for_each_first_component(&mut |component| {
        if let Component::Polygon(polygon) = component {
            polygons.push(polygon);
        }
        Ok::<_, std::convert::Infallible>(())
    }) {
        Ok(()) => {},
        Err(never) => match never {},
    }
    polygons
}

fn first_surface_lines(shape: &Shape) -> Vec<&CoordSeq> {
    let mut lines = Vec::new();
    match shape.try_for_each_first_component(&mut |component| {
        if let Component::LineString(line) = component {
            lines.push(line.as_coords());
        }
        Ok::<_, std::convert::Infallible>(())
    }) {
        Ok(()) => {},
        Err(never) => match never {},
    }
    lines
}

fn point_component_surface_point(shape: &Shape, center: Point) -> Option<Point> {
    let mut best: Option<(f64, Point)> = None;
    match shape.try_for_each_first_component(&mut |component| {
        if let Component::Point(candidate) = component {
            let distance = squared_xy_distance(candidate, center);
            if best.is_none_or(|(current, _)| distance.total_cmp(&current).is_lt()) {
                best = Some((distance, candidate));
            }
        }
        Ok::<_, std::convert::Infallible>(())
    }) {
        Ok(()) => {},
        Err(never) => match never {},
    }
    best.map(|(_, point)| point)
}

/// Representative of an arbitrary (possibly nested, mixed, or degenerate)
/// collection: the highest present dimension wins, areal degeneracy falls
/// through to its boundary linework, then to its vertices. Reused for bare
/// degenerate polygons too (via `from_ref`), which the scanline kernel leaves
/// to this boundary path.
pub(crate) fn collection_surface_point(shape: &Shape) -> Result<Option<Point>> {
    let polygons = first_surface_polygons(shape);
    if !polygons.is_empty() {
        if let Some(point) = polygonal_surface_point(polygons.iter().copied()) {
            return Ok(Some(point));
        }
        // Degenerate areal: its boundary rings carry the representative point.
        if let Some(point) = lineal_surface_point(
            polygons
                .iter()
                .flat_map(|polygon| polygon.rings().map(Ring::coords)),
            lineal_centroid(shape)?,
        ) {
            return Ok(Some(point));
        }
    }
    let lines = first_surface_lines(shape);
    if let [line] = lines.as_slice() {
        return Ok(point_on_surface_line_columns(line.xs(), line.ys()));
    }
    if !lines.is_empty()
        && let Some(point) = lineal_surface_point(lines.iter().copied(), lineal_centroid(shape)?)
    {
        return Ok(Some(point));
    }
    let Some((x, y)) = point_centroid(shape)? else {
        return Ok(None);
    };
    Ok(point_component_surface_point(
        shape,
        Point::new_raw_xy(x, y),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{
        CoordinateAxes, Coordinates as _, GeometryKind, LineSeq, MultiMember, MultiPoint,
        MultiPolygon,
    };

    fn xy(x: f64, y: f64) -> Point {
        Point::new_raw_xy(x, y)
    }

    fn closed_ring(points: &[(f64, f64)]) -> Ring {
        Ring::closed(points.iter().copied().map(|(x, y)| xy(x, y)).collect()).expect("test ring")
    }

    fn polygon(shell: &[(f64, f64)], holes: &[&[(f64, f64)]]) -> Shape {
        Shape::Polygon(Polygon::new(
            closed_ring(shell),
            holes.iter().copied().map(closed_ring).collect(),
        ))
    }

    fn box_polygon(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Shape {
        polygon(
            &[
                (minx, miny),
                (maxx, miny),
                (maxx, maxy),
                (minx, maxy),
                (minx, miny),
            ],
            &[],
        )
    }

    fn surface_point(shape: &Shape) -> Point {
        match shape.point_on_surface().expect("test surface point") {
            Shape::Point(point) => point,
            other @ (Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..)) => panic!("expected a point, got {other:?}"),
        }
    }

    fn surface_xy(shape: &Shape) -> (f64, f64) {
        let point = surface_point(shape);
        (point.x, point.y)
    }

    fn scaled_dyadic(value: f64, scale: u32) -> i128 {
        if value == 0.0 {
            return 0;
        }
        let bits = value.to_bits();
        let sign = if bits >> 63 == 0 { 1_i128 } else { -1_i128 };
        let exponent = ((bits >> 52) & 0x7FF) as i32;
        let fraction = bits & ((1_u64 << 52) - 1);
        let (significand, power) = if exponent == 0 {
            (i128::from(fraction), -1074)
        } else {
            (i128::from((1_u64 << 52) | fraction), exponent - 1023 - 52)
        };
        let (numerator, denominator) = if power >= 0 {
            (sign * (significand << power), 0)
        } else {
            (sign * significand, (-power) as u32)
        };
        numerator << (scale - denominator)
    }

    fn exact_orientation(a: Point, b: Point, point: Point) -> std::cmp::Ordering {
        // These fixtures are dyadic; a common power-of-two denominator makes
        // every orientation an exact integer determinant.
        let values = [a.x, a.y, b.x, b.y, point.x, point.y];
        let scale = values
            .into_iter()
            .map(|value| {
                let exponent = ((value.to_bits() >> 52) & 0x7FF) as i32;
                if exponent == 0 || value == 0.0 {
                    0
                } else {
                    (1023 + 52 - exponent).max(0) as u32
                }
            })
            .max()
            .unwrap_or(0);
        let [ax, ay, bx, by, px, py] = values.map(|value| scaled_dyadic(value, scale));
        ((bx - ax) * (py - ay) - (by - ay) * (px - ax)).cmp(&0)
    }

    fn exact_ring_location(ring: &Ring, point: Point) -> RingClass {
        let vertices: Vec<_> = ring.coords().iter_coords().collect();
        let mut inside = false;
        for edge in vertices.windows(2) {
            let [a, b] = edge else {
                unreachable!("window size is two");
            };
            let orientation = exact_orientation(*a, *b, point);
            if orientation.is_eq()
                && point.x >= a.x.min(b.x)
                && point.x <= a.x.max(b.x)
                && point.y >= a.y.min(b.y)
                && point.y <= a.y.max(b.y)
            {
                return RingClass::Boundary;
            }
            if a.y <= point.y && point.y < b.y && orientation.is_gt()
                || b.y <= point.y && point.y < a.y && orientation.is_lt()
            {
                inside = !inside;
            }
        }
        if inside {
            RingClass::Interior
        } else {
            RingClass::Exterior
        }
    }

    fn exact_polygon_covers(polygon: &Polygon, point: Point) -> bool {
        exact_ring_location(&polygon.shell, point) != RingClass::Exterior
            && polygon
                .holes
                .iter()
                .all(|hole| exact_ring_location(hole, point) != RingClass::Interior)
    }

    fn exact_shape_covers(shape: &Shape, point: Point) -> bool {
        match shape {
            Shape::Polygon(polygon) => exact_polygon_covers(polygon, point),
            Shape::MultiPolygon(polygons) => polygons
                .iter()
                .any(|polygon| exact_polygon_covers(polygon, point)),
            Shape::GeometryCollection(parts) => {
                parts.iter().any(|part| exact_shape_covers(part, point))
            },
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Empty(..) => false,
        }
    }

    fn first_component_work(shape: &Shape) -> usize {
        let mut visits = 0;
        match shape.try_for_each_first_component(&mut |_| {
            visits += 1;
            Ok::<_, std::convert::Infallible>(())
        }) {
            Ok(()) => {},
            Err(never) => match never {},
        }
        visits
    }

    #[test]
    fn empty_geometry_yields_empty_point_without_staging() {
        for empty in [
            Shape::empty_point(),
            Shape::empty_polygon(),
            Shape::typed_empty(GeometryKind::GeometryCollection, CoordinateAxes::XY),
        ] {
            let result = crate::test_support::assert_no_allocations(|| empty.point_on_surface());
            assert_eq!(result.expect("empty surface point"), Shape::empty_point());
        }
    }

    #[test]
    fn adjacent_float_triangles_publish_only_exactly_covered_points() {
        let unit = 2.0_f64.powi(-52);
        let vertices = [(1.0, 0.0), (1.0 + unit, 2.0), (1.0 + unit, 3.0), (1.0, 0.0)];
        for (reflect_x, reflect_y, translate_y) in [
            (false, false, 0.0),
            (true, false, 0.0),
            (false, false, 8.0),
            (true, true, 8.0),
        ] {
            let transformed = vertices.map(|(x, y)| {
                (
                    if reflect_x { -x } else { x },
                    (if reflect_y { -y } else { y }) + translate_y,
                )
            });
            let source = polygon(&transformed, &[]);
            let Shape::Polygon(source_polygon) = &source else {
                unreachable!("test triangle is polygonal");
            };
            let point = surface_point(&source);
            assert!(
                exact_polygon_covers(source_polygon, point),
                "source triangle {transformed:?} does not cover {point:?}"
            );
            let collection =
                Shape::geometry_collection(vec![source.clone()]).expect("test collection");
            let collection_point = surface_point(&collection);
            assert!(
                exact_shape_covers(&collection, collection_point),
                "collection for {transformed:?} does not cover {collection_point:?}"
            );
        }
    }

    #[test]
    fn shared_collection_surface_stages_only_first_owner_candidates() {
        let mut shape = Shape::geometry_collection(vec![
            box_polygon(0.0, 0.0, 2.0, 2.0),
            Shape::LineString(LineSeq::from_proven(
                CoordSeq::try_from_points(&[xy(0.0, 4.0), xy(4.0, 4.0)]).expect("line"),
            )),
            Shape::Point(xy(10.0, 10.0)),
        ])
        .expect("mixed leaf");
        for _ in 0..=8 {
            assert_eq!(first_surface_polygons(&shape).len(), 1);
            assert_eq!(first_component_work(&shape), 3);
            assert_eq!(surface_xy(&shape), (1.0, 1.0));
            shape = Shape::geometry_collection(vec![shape.clone(), shape]).expect("shared level");
        }
    }

    #[test]
    fn holed_polygon_uses_widest_even_odd_chord_outside_the_hole() {
        let donut = polygon(
            &[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ],
            &[&[(3.0, 3.0), (7.0, 3.0), (7.0, 7.0), (3.0, 7.0), (3.0, 3.0)]],
        );
        let point = surface_point(&donut);
        assert_eq!((point.x, point.y), (8.5, 5.0));
        let Shape::Polygon(donut) = &donut else {
            unreachable!("test donut is polygonal");
        };
        assert!(exact_polygon_covers(donut, point));
    }

    #[test]
    fn equal_width_multipolygon_tie_keeps_first_component_and_is_covered() {
        let Shape::Polygon(first) = box_polygon(0.0, 0.0, 10.0, 10.0) else {
            unreachable!("test box is polygonal");
        };
        let Shape::Polygon(second) = box_polygon(20.0, 0.0, 30.0, 10.0) else {
            unreachable!("test box is polygonal");
        };
        let shape = Shape::MultiPolygon(
            MultiPolygon::try_new(CoordinateAxes::XY, vec![
                MultiMember::Value(first),
                MultiMember::Value(second),
            ])
            .expect("valid test multipolygon"),
        );
        let point = surface_point(&shape);
        assert_eq!((point.x, point.y), (5.0, 5.0));
        assert!(exact_shape_covers(&shape, point));
    }

    #[test]
    fn equal_width_chord_tie_keeps_later_chord_and_is_covered() {
        let shape = polygon(
            &[
                (0.0, 0.0),
                (2.0, 0.0),
                (2.0, 6.0),
                (6.0, 6.0),
                (6.0, 0.0),
                (8.0, 0.0),
                (8.0, 8.0),
                (0.0, 8.0),
                (0.0, 0.0),
            ],
            &[],
        );
        let point = surface_point(&shape);
        assert_eq!((point.x, point.y), (7.0, 4.0));
        let Shape::Polygon(source) = &shape else {
            unreachable!("test polygon is areal");
        };
        assert!(exact_polygon_covers(source, point));
    }

    #[test]
    fn mixed_polygon_collection_keeps_the_widest_combined_chord() {
        let collection = Shape::geometry_collection(vec![
            box_polygon(0.0, 0.0, 2.0, 4.0),
            box_polygon(10.0, 0.0, 14.0, 4.0),
        ])
        .expect("test collection");
        assert_eq!(surface_xy(&collection), (12.0, 2.0));
    }

    #[test]
    fn equal_width_polygon_components_keep_first_nested_shared_input_order() {
        let first = box_polygon(170.0, 0.0, 180.0, 10.0);
        let second = box_polygon(180.0, 0.0, 190.0, 10.0);

        let forward_parts =
            Shape::geometry_collection(vec![first.clone(), second.clone()]).expect("parts");
        let forward = Shape::geometry_collection(vec![forward_parts.clone(), forward_parts])
            .expect("forward");
        assert_eq!(surface_xy(&forward), (175.0, 5.0));

        let reverse_parts =
            Shape::geometry_collection(vec![second, first]).expect("reversed parts");
        let reverse = Shape::geometry_collection(vec![reverse_parts.clone(), reverse_parts])
            .expect("reverse");
        assert_eq!(surface_xy(&reverse), (185.0, 5.0));
    }

    #[test]
    fn equal_width_component_tie_keeps_each_polygons_hole_parity() {
        let donut = polygon(
            &[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ],
            &[&[(3.0, 3.0), (7.0, 3.0), (7.0, 7.0), (3.0, 7.0), (3.0, 3.0)]],
        );
        let other = box_polygon(20.0, 0.0, 23.0, 10.0);
        let collection = Shape::geometry_collection(vec![donut, other]).expect("collection");

        assert_eq!(surface_xy(&collection), (8.5, 5.0));
    }

    #[test]
    fn vertically_disjoint_polygons_keep_global_bisector_boundary_fallback() {
        let first = box_polygon(0.0, 0.0, 4.0, 2.0);
        let second = box_polygon(10.0, 8.0, 14.0, 10.0);
        let (Shape::Polygon(first_polygon), Shape::Polygon(second_polygon)) = (&first, &second)
        else {
            unreachable!("test boxes are polygons");
        };
        assert!(polygonal_surface_point([first_polygon, second_polygon].into_iter()).is_none());

        let collection = Shape::geometry_collection(vec![first, second]).expect("collection");
        let (x, y) = surface_xy(&collection);
        let point = xy(x, y);
        let boundary = [
            (xy(0.0, 0.0), xy(4.0, 0.0)),
            (xy(4.0, 0.0), xy(4.0, 2.0)),
            (xy(4.0, 2.0), xy(0.0, 2.0)),
            (xy(0.0, 2.0), xy(0.0, 0.0)),
            (xy(10.0, 8.0), xy(14.0, 8.0)),
            (xy(14.0, 8.0), xy(14.0, 10.0)),
            (xy(14.0, 10.0), xy(10.0, 10.0)),
            (xy(10.0, 10.0), xy(10.0, 8.0)),
        ];
        assert!(
            boundary
                .into_iter()
                .any(|(start, end)| point_on_segment(point, start, end))
        );
    }

    #[test]
    fn nested_collection_prefers_areal_over_lineal_and_puntal() {
        let collection = Shape::geometry_collection(vec![
            Shape::geometry_collection(vec![Shape::Point(xy(0.0, 0.0))]).expect("nested points"),
            Shape::LineString(LineSeq::from_proven(
                CoordSeq::try_from_points(&[xy(0.0, 0.0), xy(8.0, 0.0)]).expect("line"),
            )),
            box_polygon(0.0, 0.0, 4.0, 4.0),
        ])
        .expect("mixed collection");
        assert_eq!(surface_xy(&collection), (2.0, 2.0));
    }

    #[test]
    fn degenerate_collinear_polygon_returns_a_boundary_point() {
        let collapsed = polygon(&[(0.0, 0.0), (1.0, 0.0), (3.0, 0.0), (0.0, 0.0)], &[]);
        let point = match collapsed
            .point_on_surface()
            .expect("collapsed surface point")
        {
            Shape::Point(point) => point,
            other @ (Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..)) => panic!("expected a point, got {other:?}"),
        };
        assert!(
            [
                (xy(0.0, 0.0), xy(1.0, 0.0)),
                (xy(1.0, 0.0), xy(3.0, 0.0)),
                (xy(3.0, 0.0), xy(0.0, 0.0)),
            ]
            .into_iter()
            .any(|(start, end)| point_on_segment(point, start, end))
        );
    }

    #[test]
    fn point_collection_selects_the_vertex_nearest_the_centroid() {
        let collection = Shape::geometry_collection(vec![
            Shape::Point(xy(0.0, 0.0)),
            Shape::Point(xy(2.0, 0.0)),
            Shape::Point(xy(10.0, 0.0)),
        ])
        .expect("point collection");
        assert_eq!(surface_xy(&collection), (2.0, 0.0));
    }

    #[test]
    fn shared_point_occurrences_keep_two_to_one_centroid_weight() {
        let repeated = Shape::geometry_collection(vec![Shape::Point(xy(3.0, 0.0))])
            .expect("shared point owner");
        let collection = Shape::geometry_collection(vec![
            Shape::Point(xy(0.0, 0.0)),
            repeated.clone(),
            repeated,
        ])
        .expect("weighted point collection");

        assert_eq!(
            point_centroid(&collection).expect("point centroid"),
            Some((2.0, 0.0))
        );
        assert_eq!(surface_xy(&collection), (3.0, 0.0));
    }

    #[test]
    fn sparse_multipoint_member_is_skipped() {
        let collection = Shape::geometry_collection(vec![Shape::MultiPoint(
            MultiPoint::try_new(CoordinateAxes::XY, vec![
                MultiMember::Empty,
                MultiMember::Value(xy(1.0, 2.0)),
            ])
            .expect("sparse multipoint"),
        )])
        .expect("collection");
        assert_eq!(surface_xy(&collection), (1.0, 2.0));
    }

    #[test]
    fn packed_polygon_row_matches_the_scanline_interior_point() {
        let xs = [0.0, 2.0, 2.0, 0.0, 0.0];
        let ys = [0.0, 0.0, 4.0, 4.0, 0.0];
        let ring_offsets = [0, 5];
        let mut crossings = Vec::new();
        let point = point_on_surface_polygon_row_columns(
            &xs,
            &ys,
            &ring_offsets,
            0..1,
            Some(Bounds::new_raw(0.0, 0.0, 2.0, 4.0)),
            &mut crossings,
        )
        .expect("interior chord");
        assert_eq!((point.x, point.y), (1.0, 2.0));
    }

    #[test]
    fn packed_polygon_row_falls_back_when_rounded_point_is_outside() {
        let unit = 2.0_f64.powi(-52);
        let source = polygon(
            &[(1.0, 0.0), (1.0 + unit, 2.0), (1.0 + unit, 3.0), (1.0, 0.0)],
            &[],
        );
        let Shape::Polygon(source) = source else {
            unreachable!("test triangle is polygonal");
        };
        let xs = source.shell.coords().xs();
        let ys = source.shell.coords().ys();
        let ring_offsets = [0, 4];
        let bounds = Bounds::new_raw(1.0, 0.0, 1.0 + unit, 3.0);
        let point = point_on_surface_polygon_row_columns(
            xs,
            ys,
            &ring_offsets,
            0..1,
            Some(bounds),
            &mut Vec::new(),
        )
        .expect("the boundary fallback supplies a representative");
        assert!(exact_polygon_covers(&source, point));
    }

    #[test]
    fn packed_nullable_reversed_duplicate_gather_keeps_exact_polygon_membership() {
        use crate::geometry::storage::{GeometryArrayStorage, PackedStorage, ValidityMask};

        crate::test_support::initialize_python();
        let unit = 2.0_f64.powi(-52);
        let triangle = polygon(
            &[(1.0, 0.0), (1.0 + unit, 2.0), (1.0 + unit, 3.0), (1.0, 0.0)],
            &[],
        );
        let translated = polygon(
            &[
                (1.0, 8.0),
                (1.0 + unit, 10.0),
                (1.0 + unit, 11.0),
                (1.0, 8.0),
            ],
            &[],
        );
        let masked = crate::PyGeometryArray::from_shapes(
            vec![
                triangle.clone(),
                box_polygon(20.0, 20.0, 22.0, 22.0),
                translated.clone(),
            ],
            crate::Frame::None,
        )
        .expect("test polygons pack")
        .with_missing_mask(Some(
            ValidityMask::from_sparse(3, &[1]).expect("test missing mask"),
        ))
        .expect("test mask applies");
        let gathered = masked
            .gather_logical_rows(&[2, 1, 0, 2])
            .expect("reversed duplicate gather");
        assert!(matches!(
            gathered.storage(),
            GeometryArrayStorage::Packed(PackedStorage::Polygons { .. })
        ));

        let result = gathered
            .point_on_surface_unary_packed()
            .expect("packed point-on-surface");
        assert!(matches!(
            result.storage(),
            GeometryArrayStorage::Packed(PackedStorage::Points { .. })
        ));
        let expected = [Some(&translated), None, Some(&triangle), Some(&translated)];
        for (result_row, source) in result
            .row_operation()
            .rows_with_missing(|| ())
            .zip(expected)
        {
            match (result_row, source) {
                (crate::array::ArrayRow::Missing { .. }, None) => {},
                (crate::array::ArrayRow::Present(present), Some(Shape::Polygon(source))) => {
                    let point = present.row().point().expect("present packed output point");
                    assert!(exact_polygon_covers(source, point));
                },
                _ => panic!("packed point rows preserve the gathered nullable order"),
            }
        }
    }
}
