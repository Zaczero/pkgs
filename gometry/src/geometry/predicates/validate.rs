use crate::geometry::*;
pub(crate) fn validate_point(point: Point, path: &str) -> Option<ValidationIssue> {
    (!point.x.is_finite() || !point.y.is_finite())
        .then(|| ValidationIssue::new("point coordinates must be finite", Some(point), path))
}

pub(crate) fn validate_points<C: Coordinates + ?Sized>(
    points: &C,
    path: &str,
) -> Option<ValidationIssue> {
    // Columnar storage takes the shared finiteness kernel first; the indexed
    // per-vertex rescan (and its `format!`) runs only on the rare failure.
    if let Some((xs, ys)) = points.xy_columns()
        && column_all_finite(xs)
        && column_all_finite(ys)
    {
        return None;
    }
    points
        .iter_coords()
        .enumerate()
        .find(|(_, point)| !point.x.is_finite() || !point.y.is_finite())
        .map(|(idx, point)| {
            ValidationIssue::new(
                "point coordinates must be finite",
                Some(point),
                format!("{path}[{idx}]"),
            )
        })
}

pub(crate) fn validate_line<C: Coordinates + ?Sized>(
    points: &C,
    name: &str,
) -> Option<ValidationIssue> {
    if points.coord_count() == 1 {
        Some(ValidationIssue::new(
            format!("{name} requires at least two coordinates"),
            points.first_coord(),
            "$",
        ))
    } else {
        validate_points(points, "$")
    }
}

pub(crate) fn validate_ring<C: Coordinates + ?Sized>(
    points: &C,
    // Labels are built ONLY on failure: eager `format!` at the call sites
    // was 4% of a bulk `is_valid` profile over small valid polygons.
    label: impl Fn() -> (String, String),
) -> Option<ValidationIssue> {
    let len = points.coord_count();
    if len < 4 {
        let (name, path) = label();
        return Some(ValidationIssue::new(
            format!("{name} requires at least four coordinates"),
            points.first_coord(),
            path,
        ));
    }
    if let Some(issue) = validate_points(points, "$") {
        let (_, path) = label();
        return Some(issue.with_path_prefix(&path));
    }
    let first = points.nth_coord(0);
    let last = points.nth_coord(len - 1);
    if !same_point(first, last) {
        let (name, path) = label();
        return Some(ValidationIssue::new(
            format!("{name} must be closed"),
            Some(last),
            path,
        ));
    }
    // Collapse guard. Simplicity/validity now ELIDE zero-length stutter
    // segments (repeated consecutive vertices), so a ring that stutters down to
    // a point or a back-and-forth segment — e.g. `POLYGON((0 0,0 0,0 0,0 0))` —
    // passes the raw length + closure gates yet encloses nothing and would
    // otherwise read as having no self-intersection. Count the DISTINCT corners
    // (consecutive-deduped, closing repeat dropped): a real ring needs three.
    let mut corners = 1_usize;
    let mut previous = first;
    for index in 1..len {
        let coord = points.nth_coord(index);
        if !same_point(previous, coord) {
            corners += 1;
            previous = coord;
        }
    }
    (corners - 1 < 3).then(|| {
        let (name, path) = label();
        ValidationIssue::new(
            format!("{name} collapses to fewer than three distinct vertices"),
            Some(first),
            path,
        )
    })
}

pub(crate) fn validate_geo_multi_polygon(
    polygons: &[Polygon],
    path: &str,
) -> Option<ValidationIssue> {
    multi_polygon_members_issue(polygons, path)
}

/// Convexity fold over a shell ring (closing duplicate skipped): robust
/// `orient2d` per turn (collinear allowed, any opposing pair means
/// concave) PLUS a turning-number check — a star polygon's turns all share
/// one sign yet wind more than once, so the total turning must be exactly
/// ±2π for a simple convex ring.
pub(in crate::geometry) fn shell_is_convex<C: Coordinates + ?Sized>(shell: &C) -> bool {
    // Convexity is planar, so gather the XY engine coord (16 bytes) rather than
    // the ordinate-carrying Point (40 bytes) — 2.5× less to allocate and scan.
    let mut points: Vec<XY> = shell.iter_coords().map(XY::from).collect();
    points.dedup();
    // Strip the closing duplicate, then the wraparound duplicate a repeated
    // final vertex can leave behind — zero-length edges have no direction.
    if points.len() > 1 && points[0] == points[points.len() - 1] {
        points.pop();
    }
    let ring = points.as_slice();
    let n = ring.len();
    if n < 3 {
        return true;
    }
    // Schorn-Fisher convexity, atan2-free: a simple polygon is convex iff
    // every turn keeps ONE orientation AND its edge direction winds exactly
    // once. `orient2d` gives the robust turn sign; winding-once is read off
    // the direction signs — each component (dx, dy) reverses sign exactly
    // TWICE around a single convex loop (its direction angle sweeps one full
    // turn, so cos/sin each cross zero twice), whereas a self-intersecting
    // "all-same-turn" star winds 2+ times and overshoots. This replaces the
    // per-vertex `atan2` turning sum that profiled at ~30% of a
    // convex-polygon buffer / first point-query (`convex_shell` cache init).
    let component_sign = |value: f64| i32::from(value > 0.0) - i32::from(value < 0.0);
    let mut sign = Orientation::Collinear;
    let (mut x_flips, mut y_flips) = (0_u32, 0_u32);
    // First and most-recent NON-ZERO direction sign per axis: zero components
    // (axis-aligned edges) carry the previous sign so they never count as a
    // flip, and the wrap edge closes the cyclic sequence at the end.
    let (mut first_x, mut last_x) = (0_i32, 0_i32);
    let (mut first_y, mut last_y) = (0_i32, 0_i32);
    for index in 0..n {
        let a = ring[index];
        let b = ring[wrap_index(index + 1, n)];
        let c = ring[wrap_index(index + 2, n)];
        let turn = orientation_xy(a.x, a.y, b.x, b.y, c.x, c.y);
        if turn != Orientation::Collinear {
            if sign == Orientation::Collinear {
                sign = turn;
            } else if turn != sign {
                return false;
            }
        }
        let sx = component_sign(b.x - a.x);
        if sx != 0 {
            if last_x == 0 {
                first_x = sx;
            } else if sx != last_x {
                x_flips += 1;
            }
            last_x = sx;
        }
        let sy = component_sign(b.y - a.y);
        if sy != 0 {
            if last_y == 0 {
                first_y = sy;
            } else if sy != last_y {
                y_flips += 1;
            }
            last_y = sy;
        }
    }
    // All turns collinear is a degenerate sliver, not a convex region.
    if sign == Orientation::Collinear {
        return false;
    }
    // Close the cyclic direction sequences (last non-zero sign back to first).
    if last_x != 0 && last_x != first_x {
        x_flips += 1;
    }
    if last_y != 0 && last_y != first_y {
        y_flips += 1;
    }
    x_flips == 2 && y_flips == 2
}
