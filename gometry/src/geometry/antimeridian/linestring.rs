use crate::geometry::{
    Coordinates, MOrdinate, Point, ZOrdinate, interpolate_subdivision_ordinate, same_point,
};

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

/// The pair of seam points `((-180, lat), (180, lat))` for an ordered
/// unwrapped chart segment from the lower longitude `west` to the higher
/// longitude `east`. Latitude and Z/M are interpolated at the same affine
/// fraction along that chart segment.
fn crossing_points(west: Point, east: Point) -> (Point, Point) {
    let (latitude, fraction) = crossing_latitude_and_fraction(west, east);
    let interpolate_ordinate = |start: Option<f64>, end: Option<f64>| match (start, end) {
        (Some(start), Some(end)) => Some(interpolate_subdivision_ordinate(start, end, fraction)),
        _ => None,
    };
    let z = interpolate_ordinate(west.z(), east.z());
    let m = interpolate_ordinate(west.m(), east.m());
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
    let longitude_scale = axis.unit().radians_per_unit().to_degrees();
    let latitude_scale = frame.latitude_unit().radians_per_unit().to_degrees();
    let (west, east) = if start_longitude < end_longitude {
        (
            Point::new_raw_xy(start_longitude * longitude_scale, start.y * latitude_scale),
            Point::new_raw_xy(end_longitude * longitude_scale, end.y * latitude_scale),
        )
    } else {
        (
            Point::new_raw_xy(end_longitude * longitude_scale, end.y * latitude_scale),
            Point::new_raw_xy(start_longitude * longitude_scale, start.y * latitude_scale),
        )
    };
    let (latitude, _) = crossing_latitude_and_fraction(west, east);
    latitude / latitude_scale
}

/// Return the seam latitude and affine fraction for an ordered chart segment.
///
/// The endpoints are canonical chart longitudes, with `west` below `east`.
/// A seam endpoint remains the exact source ordinate; otherwise the crossing
/// fraction is the linear distance from `west` to −180 over the full
/// unwrapped interval.
fn crossing_latitude_and_fraction(west: Point, east: Point) -> (f64, f64) {
    if west.x.abs() == 180.0 {
        return (west.y, 0.0);
    }
    if east.x.abs() == 180.0 {
        return (east.y, 1.0);
    }
    let west_distance = west.x + 180.0;
    let east_distance = 180.0 - east.x;
    let fraction = west_distance / (west_distance + east_distance);
    // `+ 0.0` canonicalizes a negative-zero crossing latitude.
    (
        interpolate_subdivision_ordinate(west.y, east.y, fraction) + 0.0,
        fraction,
    )
}
