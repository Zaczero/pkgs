use super::split::ring_is_ccw;
use super::{
    ChartPole, CoordSeq, GeographicFrame, GeometryErrorKind, MOrdinate, Point, PoleClosure,
    Polygon, Result, Ring, Shape, ZOrdinate, same_point,
};

pub(super) fn polygons_from_shape(shape: Shape) -> Result<Vec<Polygon>> {
    match shape {
        Shape::Polygon(polygon) => Ok(vec![polygon]),
        Shape::MultiPolygon(polygons) => Ok(polygons.iter().cloned().collect()),
        Shape::Empty(..) => Ok(Vec::new()),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_) => Err(GeometryErrorKind::antimeridian_split_failed(
            "subtracting crossing holes produced non-areal output",
        )),
    }
}

/// Build the final polygons from stitched seam segments: rings, the
/// single-ring pole-covering wrap, and kept-hole assignment.
pub(super) fn assemble_polygons(
    segments: Vec<Vec<Point>>,
    kept_holes: &[&Ring],
) -> Result<Vec<Polygon>> {
    let mut shells = build_rings(segments);
    if shells.is_empty() {
        return Err(GeometryErrorKind::antimeridian_split_failed(
            "no ring could be stitched along the seam",
        ));
    }
    // A single non-CCW result encloses both poles: represent it as the world
    // rectangle with the ring as a hole.
    if let [shell] = shells.as_slice()
        && !ring_is_ccw(shell)
    {
        // The fabricated world rectangle carries the ring's axes so shell and
        // hole stay axis-consistent (a measured/3D pole wrap is rejected
        // downstream; the placeholder ordinate is never observed).
        let like = shell.first().copied().unwrap_or(Point::new(0.0, 0.0)?);
        let corner = |lon: f64, lat: f64| -> Result<Point> {
            Point::new_axes(lon, lat, ZOrdinate(like.z()), MOrdinate(like.m()))
        };
        let world = vec![
            corner(-180.0, 90.0)?,
            corner(-180.0, -90.0)?,
            corner(180.0, -90.0)?,
            corner(180.0, 90.0)?,
            corner(-180.0, 90.0)?,
        ];
        let hole = close_ring(shells.pop().expect("the shell slice contains one ring"));
        return Ok(vec![Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(&world)?)?,
            vec![Ring::try_from_closed(CoordSeq::try_from_points(&hole)?)?],
        )]);
    }
    let mut polygons = Vec::with_capacity(shells.len());
    for shell in shells {
        polygons.push(Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(&close_ring(shell))?)?,
            Vec::new(),
        ));
    }
    // Assign each non-crossing hole to the part that contains it. Accumulate
    // every hole assigned to the same part.
    let candidate_shapes = polygons
        .iter()
        .cloned()
        .map(Shape::Polygon)
        .collect::<Vec<_>>();
    let mut assigned_holes = vec![Vec::<Ring>::new(); polygons.len()];
    for hole in kept_holes {
        let hole_shape = Shape::LineString(hole.as_line());
        let mut owner = None;
        for (candidate, assigned) in candidate_shapes.iter().zip(&mut assigned_holes) {
            if candidate.contains(&hole_shape)? {
                owner = Some(assigned);
                break;
            }
        }
        match owner {
            Some(assigned) => assigned.push((*hole).clone()),
            None => {
                return Err(GeometryErrorKind::antimeridian_split_failed(
                    "a hole does not lie inside any split part",
                ));
            },
        }
    }
    for (polygon, mut holes) in polygons.iter_mut().zip(assigned_holes) {
        if holes.is_empty() {
            continue;
        }
        let combined = if polygon.holes.is_empty() {
            holes
        } else {
            let mut combined = polygon.holes.to_vec();
            combined.append(&mut holes);
            combined
        };
        *polygon = Polygon::new(polygon.shell.clone(), combined);
    }
    Ok(polygons)
}

fn close_ring(mut coords: Vec<Point>) -> Vec<Point> {
    if let (Some(first), Some(last)) = (coords.first().copied(), coords.last().copied())
        && !same_point(first, last)
    {
        coords.push(first);
    }
    coords
}

/// A seam end with no seam start between it and the pole closes over that pole.
/// Implying both poles under `Auto` means the winding was inverted, so the
/// original segments come back reversed. Returns the segments and whether any
/// pole closure was applied.
pub(super) fn extend_over_poles(
    mut segments: Vec<Vec<Point>>,
    closure: PoleClosure,
) -> Result<(Vec<Vec<Point>>, bool)> {
    let chart_frame = GeographicFrame::crs84();
    let north_pole = chart_frame.chart_pole_latitude(ChartPole::North);
    let south_pole = chart_frame.chart_pole_latitude(ChartPole::South);
    // A fabricated pole-cap vertex must carry the SAME axes as the chain it
    // extends, or the assembled ring would mix Z/M presence — violating the
    // CoordSeq axis-homogeneity invariant (a `from_points` debug-assert, and a
    // silently malformed sequence in release). The placeholder ordinate (copied
    // from the capped chain's endpoint) is never observed: a measured/3D ring
    // whose cap survives is rejected by `reject_fabricated_pole_ordinates`, and
    // an XY ring carries no ordinate to fabricate.
    let pole_vertex = |like: Point, lon: f64, lat: f64| -> Result<Point> {
        Point::new_axes(lon, lat, ZOrdinate(like.z()), MOrdinate(like.m()))
    };
    let mut left_start: Option<f64> = None;
    let mut right_start: Option<f64> = None;
    let mut left_end: Option<(usize, f64)> = None;
    let mut right_end: Option<(usize, f64)> = None;
    for (index, segment) in segments.iter().enumerate() {
        let start = *segment.first().expect("segments are non-empty");
        let end = *segment.last().expect("segments are non-empty");
        if start.x == -180.0 && left_start.is_none_or(|latitude| start.y < latitude) {
            left_start = Some(start.y);
        } else if start.x == 180.0 && right_start.is_none_or(|latitude| start.y > latitude) {
            right_start = Some(start.y);
        }
        if end.x == -180.0 && left_end.is_none_or(|(_, latitude)| end.y < latitude) {
            left_end = Some((index, end.y));
        } else if end.x == 180.0 && right_end.is_none_or(|(_, latitude)| end.y > latitude) {
            right_end = Some((index, end.y));
        }
    }
    let original = segments.clone();
    let mut over_north = false;
    let mut over_south = false;
    if let Some((index, latitude)) = left_end {
        if closure == PoleClosure::Chart(ChartPole::North)
            && right_end.is_none()
            && left_start.is_none_or(|start| latitude > start)
        {
            // Forced north from the west side: cap and reverse.
            over_north = true;
            let segment = segments
                .get_mut(index)
                .expect("the pole endpoint came from this segment");
            let like = *segment.last().expect("segments are non-empty");
            segment.push(pole_vertex(like, -180.0, north_pole)?);
            segment.push(pole_vertex(like, 180.0, north_pole)?);
            segment.reverse();
        } else if closure == PoleClosure::Chart(ChartPole::South)
            || left_start.is_none_or(|start| latitude < start)
        {
            over_south = true;
            let segment = segments
                .get_mut(index)
                .expect("the pole endpoint came from this segment");
            let like = *segment.last().expect("segments are non-empty");
            segment.push(pole_vertex(like, -180.0, south_pole)?);
            segment.push(pole_vertex(like, 180.0, south_pole)?);
        }
    }
    if let Some((index, latitude)) = right_end {
        if closure == PoleClosure::Chart(ChartPole::South)
            && right_start.is_none_or(|start| latitude < start)
        {
            // Forced south from the east side: cap and reverse.
            over_south = true;
            let segment = segments
                .get_mut(index)
                .expect("the pole endpoint came from this segment");
            let like = *segment.last().expect("segments are non-empty");
            segment.push(pole_vertex(like, 180.0, south_pole)?);
            segment.push(pole_vertex(like, -180.0, south_pole)?);
            segment.reverse();
        } else if closure == PoleClosure::Chart(ChartPole::North)
            || right_start.is_none_or(|start| latitude > start)
        {
            over_north = true;
            let segment = segments
                .get_mut(index)
                .expect("the pole endpoint came from this segment");
            let like = *segment.last().expect("segments are non-empty");
            segment.push(pole_vertex(like, 180.0, north_pole)?);
            segment.push(pole_vertex(like, -180.0, north_pole)?);
        }
    }
    let capped = over_north || over_south;
    if closure == PoleClosure::Auto && over_north && over_south {
        // Both poles implied: the input ring was wound backwards. Reverse
        // every original segment instead.
        let mut reversed = original;
        for segment in &mut reversed {
            segment.reverse();
        }
        return Ok((reversed, false));
    }
    Ok((segments, capped))
}

fn is_self_closing(segment: &[Point]) -> bool {
    let start = *segment.first().expect("segments are non-empty");
    let end = *segment.last().expect("segments are non-empty");
    let is_right = end.x == 180.0;
    start.x == end.x && ((is_right && start.y > end.y) || (!is_right && start.y < end.y))
}

/// Build polygons iteratively: pop a segment, find the closest seam start
/// toward the pole on the same side (including the donut clause), join and
/// repeat; a segment with no candidate closes on itself.
fn build_rings(mut segments: Vec<Vec<Point>>) -> Vec<Vec<Point>> {
    let mut rings = Vec::new();
    while let Some(mut segment) = segments.pop() {
        let end = *segment.last().expect("segments are non-empty");
        let start = *segment.first().expect("segments are non-empty");
        let is_right = end.x == 180.0;
        let mut best: Option<(usize, f64)> = None;
        let self_candidate = is_self_closing(&segment).then_some(start.y);
        for (index, candidate) in segments.iter().enumerate() {
            let candidate_start = *candidate.first().expect("segments are non-empty");
            let candidate_end = *candidate.last().expect("segments are non-empty");
            if candidate_start.x != end.x {
                continue;
            }
            let toward_pole = if is_right {
                candidate_start.y > end.y
                    && (!is_self_closing(candidate) || candidate_end.y < start.y)
            } else {
                candidate_start.y < end.y
                    && (!is_self_closing(candidate) || candidate_end.y > start.y)
            };
            if toward_pole {
                let better = best.is_none_or(|(_, latitude)| {
                    if is_right {
                        candidate_start.y < latitude
                    } else {
                        candidate_start.y > latitude
                    }
                });
                if better {
                    best = Some((index, candidate_start.y));
                }
            }
        }
        // The self-closing latitude competes with the joins: when it is
        // closer to the end than every candidate start, the segment closes
        // on itself.
        let join = match (best, self_candidate) {
            (Some((index, latitude)), Some(own)) => {
                let self_wins = if is_right {
                    own < latitude
                } else {
                    own > latitude
                };
                if self_wins { None } else { Some(index) }
            },
            (Some((index, _)), None) => Some(index),
            (None, _) => None,
        };
        if let Some(index) = join {
            let joined = segments.remove(index);
            segment.extend(joined);
            segments.push(segment);
            continue;
        }
        // Self-closing: drop fully degenerate single-point rings (an input
        // corner exactly on the seam).
        if !segment.iter().all(|point| same_point(*point, start)) {
            rings.push(segment);
        }
    }
    rings
}
