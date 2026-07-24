use super::*;

/// Upstream's `math.isclose` (relative 1e-9) against ±180 — the seam
/// snapping tolerance for a coordinate's own longitude.
fn on_seam(lon: f64, seam: f64) -> bool {
    (lon - seam).abs() <= 180.0 * 1e-9
}

/// Upstream's `numpy.isclose` (atol 1e-8 + rtol 1e-5) — the looser
/// previous-point check deciding which seam side a ±180 vertex belongs to.
fn near(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1e-8 + 1e-5 * right.abs()
}

pub(super) fn collect_points<C: Coordinates + ?Sized>(points: &C) -> Vec<Point> {
    points.iter_coords().collect()
}

/// Snap near-±180 longitudes onto the seam, resolving the side from the
/// previous vertex (a `180` vertex right after a `-180` one is really on
/// the west side, and vice versa — upstream issue 81; pole vertices are
/// exempt). When every vertex sits on the seam the ring is degenerate and
/// passes through untouched. In-domain input needs no modular wrap.
pub(super) fn normalize(coords: &mut [Point]) {
    if coords.is_empty() {
        return;
    }
    // A ring entirely on the ±180 seam is degenerate — leave it untouched.
    if coords
        .iter()
        .all(|point| on_seam(point.x, 180.0) || on_seam(point.x, -180.0))
    {
        return;
    }
    // Snap each on-seam vertex to the side implied by the PREVIOUS vertex's
    // ORIGINAL longitude, carrying that longitude forward (seeded with the
    // cyclically-last vertex) instead of indexing `original[(i + n - 1) % n]` —
    // this drops both the per-vertex `% n` div and the whole `original` copy +
    // restore pass. Bit-identical: same side decisions, same restore semantics
    // (the all-on-seam no-op is the early return above).
    let mut previous = coords[coords.len() - 1].x;
    for point in coords.iter_mut() {
        let lon = point.x;
        if on_seam(lon, 180.0) {
            point.x = if point.y.abs() != 90.0 && near(previous, -180.0) {
                -180.0
            } else {
                180.0
            };
        } else if on_seam(lon, -180.0) {
            point.x = if point.y.abs() != 90.0 && near(previous, 180.0) {
                180.0
            } else {
                -180.0
            };
        }
        previous = lon;
    }
}

/// Drop consecutive near-duplicates (upstream `numpy.allclose` tolerance).
/// Z/M must match exactly: collapsing a same-XY vertex with different
/// ordinates would shift which source segment the seam interpolates from.
pub(super) fn dedup_near(coords: Vec<Point>) -> Vec<Point> {
    let mut result: Vec<Point> = Vec::with_capacity(coords.len());
    for point in coords {
        if let Some(last) = result.last()
            && near(point.x, last.x)
            && near(point.y, last.y)
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
pub(super) fn segment_coords(points: &[Point]) -> Vec<Vec<Point>> {
    let mut coords = points.to_vec();
    normalize(&mut coords);
    let coords = dedup_near(coords);
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
/// `west` (the side whose longitude is near +17x) to `east` — upstream's
/// `crossing_latitude` with the great-circle formula, rounded to 7
/// decimals, plus Z/M interpolated along the great circle.
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

/// Great-circle crossing latitude (degrees, rounded to 7 decimals like
/// upstream) and the along-arc fraction of the crossing for Z/M
/// interpolation. The plane through both points crossed with the meridian
/// plane `-Y` lands in the XZ plane; the segment direction guarantees the
/// antimeridian (x <= 0) branch.
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
