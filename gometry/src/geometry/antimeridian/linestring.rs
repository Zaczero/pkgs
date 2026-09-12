use crate::geometry::{Coordinates, MOrdinate, Point, ZOrdinate, interpolate_optional, same_point};

pub(super) fn collect_points<C: Coordinates + ?Sized>(points: &C) -> Vec<Point> {
    points.iter_coords().collect()
}

/// Resolve the two exact chart aliases of a quotient seam from the incoming
/// edge. Pole aliases remain interchangeable. No neighborhood is snapped.
pub(super) fn resolve_seam_aliases(coords: &mut [Point]) {
    let Some(last) = coords.last() else {
        return;
    };
    let mut previous = last.x;
    for point in coords.iter_mut() {
        let source = point.x;
        if point.y.abs() != 90.0 {
            if source == 180.0 && previous == -180.0 {
                point.x = -180.0;
            } else if source == -180.0 && previous == 180.0 {
                point.x = 180.0;
            }
        }
        previous = source;
    }
}

/// Drop exact consecutive duplicates. One-ULP neighbors are distinct source
/// vertices; Z/M differences also retain the source segment boundary.
pub(super) fn dedup_consecutive(coords: Vec<Point>) -> Vec<Point> {
    let mut result: Vec<Point> = Vec::with_capacity(coords.len());
    for point in coords {
        if let Some(last) = result.last()
            && point.x.to_bits() == last.x.to_bits()
            && point.y.to_bits() == last.y.to_bits()
            && point.z() == last.z()
            && point.m() == last.m()
        {
            continue;
        }
        result.push(point);
    }
    result
}

/// Walk a coordinate chain and split it at every antimeridian crossing.
/// Returns the seam-separated pieces, or empty when nothing crosses. A
/// closed input ring re-joins its trailing piece onto the leading one so
/// every returned piece starts and ends on the seam.
pub(crate) fn segment_coords(points: &[Point]) -> Vec<Vec<Point>> {
    let mut coords = points.to_vec();
    resolve_seam_aliases(&mut coords);
    let coords = dedup_consecutive(coords);
    let mut piece: Vec<Point> = Vec::new();
    let mut pieces: Vec<Vec<Point>> = Vec::new();
    for &[start, end] in coords.array_windows::<2>() {
        piece.push(start);
        let delta = end.x - start.x;
        if delta > 180.0 && delta != 360.0 {
            // Westward exit: close on -180, reopen on 180.
            let (west, east) = crossing_points(start, end);
            piece.push(west);
            pieces.push(std::mem::replace(&mut piece, vec![east]));
        } else if -delta > 180.0 && -delta != 360.0 {
            // Eastward exit: close on 180, reopen on -180.
            let (west, east) = crossing_points(end, start);
            piece.push(east);
            pieces.push(std::mem::replace(&mut piece, vec![west]));
        }
    }
    if pieces.is_empty() {
        return pieces;
    }
    let last = *coords.last().expect("segmented chains have coordinates");
    let first_piece_start = pieces[0][0];
    if same_point(last, first_piece_start) {
        // A ring: the trailing run continues the leading piece.
        piece.append(&mut pieces[0]);
        pieces[0] = piece;
    } else {
        piece.push(last);
        pieces.push(piece);
    }
    pieces
}

/// The pair of seam points `((-180, lat), (180, lat))` for a segment from
/// `west` (the side whose longitude is near +17x) to `east`. The latitude is
/// computed from the great-circle intersection and rounded to 7 decimals;
/// Z/M are interpolated along the great circle.
fn crossing_points(west: Point, east: Point) -> (Point, Point) {
    let (latitude, fraction) = if west.x.abs() == 180.0 {
        (west.y, 0.0)
    } else if east.x.abs() == 180.0 {
        (east.y, 1.0)
    } else {
        crossing_latitude_great_circle(west, east)
    };
    let z = interpolate_optional(west.z(), east.z(), fraction);
    let m = interpolate_optional(west.m(), east.m(), fraction);
    let make = |lon: f64| {
        Point::new_axes(lon, latitude, ZOrdinate(z), MOrdinate(m))
            .expect("seam coordinates are finite")
    };
    (make(-180.0), make(180.0))
}

pub(crate) fn crossing_latitude_in_frame(
    start: Point,
    end: Point,
    frame: crate::geographic::GeographicFrame,
) -> f64 {
    let axis = frame.longitude();
    if (end.x - start.x).abs() >= axis.period() {
        return start.y;
    }
    let start_longitude = axis
        .canonicalize(start.x)
        .expect("validated longitude")
        .get();
    let end_longitude = axis.canonicalize(end.x).expect("validated longitude").get();
    if start_longitude.abs() == axis.half_period() {
        return start.y;
    }
    if end_longitude.abs() == axis.half_period() {
        return end.y;
    }
    if start_longitude == end_longitude {
        return start.y;
    }
    let (start, end) = if start_longitude < end_longitude {
        (start, end)
    } else {
        (end, start)
    };
    let longitude_scale = axis.unit().radians_per_unit().to_degrees();
    let latitude_scale = frame.latitude_unit().radians_per_unit().to_degrees();
    let (latitude, _) = crossing_latitude_great_circle(
        Point::new_raw_xy(start.x * longitude_scale, start.y * latitude_scale),
        Point::new_raw_xy(end.x * longitude_scale, end.y * latitude_scale),
    );
    latitude / latitude_scale
}

fn unit_sphere(point: Point) -> [f64; 3] {
    let (lon, lat) = (point.x.to_radians(), point.y.to_radians());
    [lon.cos() * lat.cos(), lon.sin() * lat.cos(), lat.sin()]
}

fn cross(a: [f64; 3], b: [f64; 3]) -> [f64; 3] {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

fn dot(a: [f64; 3], b: [f64; 3]) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

fn norm(a: [f64; 3]) -> f64 {
    dot(a, a).sqrt()
}

/// Great-circle crossing latitude (degrees, rounded to 7 decimals) and the
/// along-arc fraction of the crossing for Z/M interpolation. The plane
/// through both points crossed with the meridian plane `-Y` lands in the XZ
/// plane; the segment direction guarantees the antimeridian (x <= 0) branch.
fn crossing_latitude_great_circle(start: Point, end: Point) -> (f64, f64) {
    let p1 = unit_sphere(start);
    let p2 = unit_sphere(end);
    let n1 = cross(p1, p2);
    // cross(n1, (0, -1, 0)) spelled out.
    let intersection = [n1[2], 0.0, -n1[0]];
    let length = norm(intersection);
    debug_assert!(intersection[0] <= 0.0, "crossing is on the antimeridian");
    // `+ 0.0` canonicalizes a negative-zero crossing latitude.
    let latitude = ((intersection[2] / length).asin().to_degrees() * 1e7).round() / 1e7 + 0.0;
    let crossing = [
        intersection[0] / length,
        intersection[1] / length,
        intersection[2] / length,
    ];
    let arc = |a: [f64; 3], b: [f64; 3]| norm(cross(a, b)).atan2(dot(a, b));
    let total = arc(p1, p2);
    let fraction = if total > 0.0 {
        (arc(p1, crossing) / total).clamp(0.0, 1.0)
    } else {
        0.0
    };
    (latitude, fraction)
}
