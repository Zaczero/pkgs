use super::centroid::{centroid_line_columns, lineal_centroid};
use crate::error::Result;
use crate::geometry::{
    Bounds, CoordSeq, LineSeq, Point, Polygon, REDUCE_LANES, ReduceSimd, Ring, Shape, column_mean2,
    held_axis_interpolate, line_length_columns, point_on_segment, same_point, simd_argmin_f64,
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

/// Finish the scanline interior point from sorted even-odd chord crossings.
fn point_from_chord_crossings(crossings: &mut [f64], mid: f64) -> Option<Point> {
    if crossings.len() < 2 || !crossings.len().is_multiple_of(2) {
        return None;
    }
    crossings.sort_unstable_by(f64::total_cmp);
    let [left, right] = crossings
        .as_chunks::<2>()
        .0
        .iter()
        .copied()
        .max_by(|left, right| {
            let [left_start, left_end] = *left;
            let [right_start, right_end] = *right;
            (left_end - left_start).total_cmp(&(right_end - right_start))
        })?;
    Some(Point::new_raw_xy(f64::midpoint(left, right), mid))
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
    point_from_chord_crossings(crossings, mid)
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

/// Interior representative of borrowed polygon components: the midpoint of
/// the widest even-odd chord where the horizontal bisector of their combined
/// bounding box crosses the boundary linework. The half-open crossing rule
/// (`start_below != end_below`) keeps the parity consistent through
/// vertices, exactly like the ring raycasts. Holes and all parts
/// participate as plain edges, so the widest interval always lies inside
/// some part and outside every hole. `None` falls back to the generic
/// engine (degenerate zero-area input where the bisector finds no chord).
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
    for polygon in polygons {
        for ring in polygon.rings() {
            if !push_chord_crossings(ring.xs(), ring.ys(), mid, &mut crossings) {
                return None;
            }
        }
    }
    point_from_chord_crossings(&mut crossings, mid)
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
#[expect(
    clippy::indexing_slicing,
    reason = "argmin ordinals and SIMD lane groups are generated from these equal-length columns"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the selected vertex is defined by the existing IEEE-754 squared-distance score"
)]
pub(crate) fn multipoint_surface_point(xs: &[f64], ys: &[f64]) -> Option<Point> {
    let count = xs.len();
    if count == 0 {
        return None;
    }
    let (centroid_x, centroid_y) = column_mean2(xs, ys)?;
    let centroid = Point::new_raw_xy(centroid_x, centroid_y);
    let score =
        |index: usize| squared_xy_distance(Point::new_raw_xy(xs[index], ys[index]), centroid);
    let nearest = simd_argmin_f64(count, score, |start| {
        let chunk = start / REDUCE_LANES;
        let (x_chunks, _) = xs.as_chunks::<REDUCE_LANES>();
        let (y_chunks, _) = ys.as_chunks::<REDUCE_LANES>();
        let dx = ReduceSimd::from_array(x_chunks[chunk]) - ReduceSimd::splat(centroid_x);
        let dy = ReduceSimd::from_array(y_chunks[chunk]) - ReduceSimd::splat(centroid_y);
        dx * dx + dy * dy
    })
    .expect("non-empty multipoint");
    Some(Point::new_raw_xy(xs[nearest], ys[nearest]))
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
                points.reserve(seq.xs().len());
                points.extend(seq.iter());
            },
            Shape::LineString(seq) => lines.push(seq.as_coords()),
            Shape::MultiLineString(seqs) => {
                lines.reserve(seqs.as_slice().len());
                lines.extend(seqs.iter().map(LineSeq::as_coords));
            },
            Shape::Polygon(polygon) => polygons.push(polygon),
            Shape::MultiPolygon(polys) => {
                polygons.reserve(polys.as_slice().len());
                polygons.extend(polys.iter());
            },
            Shape::GeometryCollection(inner) => {
                gather_surface_components(inner, polygons, lines, points);
            },
            Shape::Empty(..) => {},
        }
    }
}

/// Interior point of an arbitrary (possibly nested, mixed, or degenerate)
/// collection: the highest present dimension wins, areal degeneracy falls
/// through to its boundary linework, then to its vertices. Reused for bare
/// degenerate polygons too (via `from_ref`), which the scanline kernel leaves
/// to this boundary path.
pub(crate) fn collection_surface_point(shape: &Shape) -> Result<Option<Point>> {
    let mut polygons: Vec<&Polygon> = Vec::new();
    let mut lines: Vec<&CoordSeq> = Vec::new();
    let mut points: Vec<Point> = Vec::new();
    gather_surface_components(
        std::slice::from_ref(shape),
        &mut polygons,
        &mut lines,
        &mut points,
    );
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
    if let [line] = lines.as_slice() {
        return Ok(point_on_surface_line_columns(line.xs(), line.ys()));
    }
    if !lines.is_empty()
        && let Some(point) = lineal_surface_point(lines.iter().copied(), lineal_centroid(shape)?)
    {
        return Ok(Some(point));
    }
    if points.is_empty() {
        return Ok(None);
    }
    let mut xs = Vec::with_capacity(points.len());
    let mut ys = Vec::with_capacity(points.len());
    xs.extend(points.iter().map(|point| point.x));
    ys.extend(points.iter().map(|point| point.y));
    Ok(multipoint_surface_point(&xs, &ys))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{CoordinateAxes, GeometryKind, MultiMember, MultiPoint};

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

    fn surface_xy(shape: &Shape) -> (f64, f64) {
        match shape.point_on_surface().expect("test surface point") {
            Shape::Point(point) => (point.x, point.y),
            other @ (Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..)) => panic!("expected a point, got {other:?}"),
        }
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
        assert_eq!(surface_xy(&donut), (8.5, 5.0));
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
}
