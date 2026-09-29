use crate::geometry::{
    Coordinates, Orientation, Point, ValidationIssue, XY, column_all_finite, orientation_xy,
    same_point, same_topological_coordinate,
};

const SMALL_RING_FUSED_LIMIT: usize = 16;
pub(crate) fn validate_point(point: Point, path: &str) -> Option<ValidationIssue> {
    (!point.x.is_finite() || !point.y.is_finite()).then(|| {
        ValidationIssue::new(
            "non_finite_coordinate",
            "point coordinates must be finite",
            Some(point),
            path,
        )
    })
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
                "non_finite_coordinate",
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
            "ring_too_short",
            format!("{name} requires at least two coordinates"),
            points.first_coord(),
            "$",
        ))
    } else if let Some(issue) = validate_points(points, "$") {
        Some(issue)
    } else {
        collapsed_line_point(points).map(|point| {
            ValidationIssue::new(
                "line_collapsed",
                format!("{name} collapses to fewer than two distinct XY positions"),
                Some(point),
                "$",
            )
        })
    }
}

pub(crate) fn collapsed_line_point<C: Coordinates + ?Sized>(points: &C) -> Option<Point> {
    if points.coord_count() < 2 {
        return None;
    }
    let first = points.nth_coord(0);
    if !first.x.is_finite() || !first.y.is_finite() {
        return None;
    }
    points
        .iter_coords()
        .skip(1)
        .all(|point| point.x.is_finite() && point.y.is_finite() && same_point(first, point))
        .then_some(first)
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
            "ring_too_short",
            format!("{name} requires at least four coordinates"),
            points.first_coord(),
            path,
        ));
    }
    // Packed rings in ordinary polygons are tiny. Fuse the XY finiteness,
    // closure, and consecutive-corner checks for them; the SIMD finiteness
    // scan remains the better shape for larger rings.
    if len <= SMALL_RING_FUSED_LIMIT
        && let Some((xs, ys)) = points.xy_columns()
    {
        let mut corners = 1_usize;
        let mut previous_x = xs[0];
        let mut previous_y = ys[0];
        for index in 0..len {
            let x = xs[index];
            let y = ys[index];
            if !x.is_finite() || !y.is_finite() {
                let (_, path) = label();
                return Some(
                    ValidationIssue::new(
                        "non_finite_coordinate",
                        "point coordinates must be finite",
                        Some(points.nth_coord(index)),
                        format!("$[{index}]"),
                    )
                    .with_path_prefix(&path),
                );
            }
            if index > 0
                && (!same_topological_coordinate(previous_x, x)
                    || !same_topological_coordinate(previous_y, y))
            {
                corners += 1;
                previous_x = x;
                previous_y = y;
            }
        }
        let first = points.nth_coord(0);
        let last = points.nth_coord(len - 1);
        if !same_point(first, last) {
            let (name, path) = label();
            return Some(ValidationIssue::new(
                "ring_not_closed",
                format!("{name} must be closed"),
                Some(last),
                path,
            ));
        }
        return (corners - 1 < 3).then(|| {
            let (name, path) = label();
            ValidationIssue::new(
                "ring_collapsed",
                format!("{name} collapses to fewer than three distinct vertices"),
                Some(first),
                path,
            )
        });
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
            "ring_not_closed",
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
            "ring_collapsed",
            format!("{name} collapses to fewer than three distinct vertices"),
            Some(first),
            path,
        )
    })
}

/// Convexity fold over a shell ring (closing duplicate skipped): robust
/// `orient2d` per turn (collinear allowed, any opposing pair means
/// concave) PLUS a turning-number check — a star polygon's turns all share
/// one sign yet wind more than once, so the total turning must be exactly
/// ±2π for a simple convex ring.
pub(in crate::geometry) fn shell_is_convex<C: Coordinates + ?Sized>(shell: &C) -> bool {
    shell_convexity(shell, false)
}

/// Strict convexity certificate for a finite, closed shell. Unlike
/// [`shell_is_convex`], a collinear turn or a zero-length edge declines: the
/// relation shortcut only admits polygons whose boundary contact classes are
/// unambiguous from the convex shell certificate.
pub(in crate::geometry) fn shell_is_strictly_convex<C: Coordinates + ?Sized>(shell: &C) -> bool {
    shell_convexity(shell, true)
}

fn shell_convexity<C: Coordinates + ?Sized>(shell: &C, strict: bool) -> bool {
    if strict {
        let Some((first, last)) = shell.first_coord().zip(shell.last_coord()) else {
            return false;
        };
        if !same_point(first, last)
            || shell
                .iter_coords()
                .any(|point| !point.x.is_finite() || !point.y.is_finite())
            || shell
                .segment_pairs()
                .any(|[start, end]| same_point(start, end))
        {
            return false;
        }
    }
    // Convexity is planar, but this predicate is a hot admission gate and its
    // normalized ring is only a cursor over the source: avoid materializing a
    // second XY vector just to remove consecutive duplicates and the terminal
    // closing coordinate. The first pass recovers the exact sequence length
    // and the second streams its cyclic triples.
    let len = shell.coord_count();
    let (distinct, first, last) = normalized_shell_extent(shell, len);
    let n = if distinct > 1 && first == last {
        distinct - 1
    } else {
        distinct
    };
    if n < 3 {
        return !strict;
    }
    let mut state = ConvexityState::new(strict);
    let mut previous = None;
    let mut first_point = None;
    let mut second_point = None;
    let mut edge_start = None;
    let mut edge_end = None;
    let mut emitted = 0;
    for index in 0..len {
        let point = XY::from(shell.nth_coord(index));
        if previous == Some(point) {
            continue;
        }
        previous = Some(point);
        if emitted == n {
            // Only the final duplicate of the first distinct coordinate is
            // removed. An intermediate return to that coordinate remains in
            // the normalized cycle, exactly as Vec::dedup followed by pop.
            break;
        }
        match emitted {
            0 => first_point = Some(point),
            1 => second_point = Some(point),
            _ => {
                let a = edge_start.expect("normalized shell has an edge start");
                let b = edge_end.expect("normalized shell has an edge end");
                if !state.push(a, b, point) {
                    return false;
                }
            },
        }
        edge_start = edge_end;
        edge_end = Some(point);
        emitted += 1;
    }
    debug_assert_eq!(emitted, n);
    let first_point = first_point.expect("a normalized shell with three points has a first point");
    let second_point =
        second_point.expect("a normalized shell with three points has a second point");
    let penultimate = edge_start.expect("normalized shell has a penultimate point");
    let last = edge_end.expect("normalized shell has a terminal point");
    if !state.push(penultimate, last, first_point) || !state.push(last, first_point, second_point) {
        return false;
    }
    state.finish()
}

fn normalized_shell_extent<C: Coordinates + ?Sized>(
    shell: &C,
    len: usize,
) -> (usize, Option<XY>, Option<XY>) {
    let mut distinct = 0;
    let mut first = None;
    let mut last = None;
    let mut previous = None;
    for index in 0..len {
        let point = XY::from(shell.nth_coord(index));
        if previous == Some(point) {
            continue;
        }
        if first.is_none() {
            first = Some(point);
        }
        previous = Some(point);
        last = Some(point);
        distinct += 1;
    }
    (distinct, first, last)
}

struct ConvexityState {
    strict: bool,
    // Schorn-Fisher convexity, atan2-free: a simple polygon is convex iff
    // every turn keeps ONE orientation AND its edge direction winds exactly
    // once. `orient2d` gives the robust turn sign; winding-once is read off
    // the direction signs — each component (dx, dy) reverses sign exactly
    // TWICE around a single convex loop (its direction angle sweeps one full
    // turn, so cos/sin each cross zero twice), whereas a self-intersecting
    // "all-same-turn" star winds 2+ times and overshoots.
    sign: Orientation,
    x_flips: u32,
    y_flips: u32,
    first_x: i32,
    last_x: i32,
    first_y: i32,
    last_y: i32,
}

impl ConvexityState {
    const fn new(strict: bool) -> Self {
        Self {
            strict,
            sign: Orientation::Collinear,
            x_flips: 0,
            y_flips: 0,
            first_x: 0,
            last_x: 0,
            first_y: 0,
            last_y: 0,
        }
    }

    fn push(&mut self, a: XY, b: XY, c: XY) -> bool {
        let turn = orientation_xy(a.x, a.y, b.x, b.y, c.x, c.y);
        if turn != Orientation::Collinear {
            if self.sign == Orientation::Collinear {
                self.sign = turn;
            } else if turn != self.sign {
                return false;
            }
        } else if self.strict {
            return false;
        }
        let component_sign = |value: f64| i32::from(value > 0.0) - i32::from(value < 0.0);
        let sx = component_sign(b.x - a.x);
        if sx != 0 {
            if self.last_x == 0 {
                self.first_x = sx;
            } else if sx != self.last_x {
                self.x_flips += 1;
            }
            self.last_x = sx;
        }
        let sy = component_sign(b.y - a.y);
        if sy != 0 {
            if self.last_y == 0 {
                self.first_y = sy;
            } else if sy != self.last_y {
                self.y_flips += 1;
            }
            self.last_y = sy;
        }
        true
    }

    fn finish(mut self) -> bool {
        if self.sign == Orientation::Collinear {
            return false;
        }
        if self.last_x != 0 && self.last_x != self.first_x {
            self.x_flips += 1;
        }
        if self.last_y != 0 && self.last_y != self.first_y {
            self.y_flips += 1;
        }
        self.x_flips == 2 && self.y_flips == 2
    }
}
