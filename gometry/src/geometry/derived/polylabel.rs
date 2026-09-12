#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::num::NonZeroU32;

use crate::error::Result;
use crate::geometry::tessellation::exact::{ExactDyadic, round_sqrt_ratio, try_compare_ratios};
use crate::geometry::{
    AxisFrame, BVH_MIN_INDEXED_SEGMENTS, Bounds, CoordSeq, CoordSeqBuilder, Coordinates as _,
    ExpansionBudget, FacetBvh, Orientation, Point, Polygon, PolylabelCell, PreparedLinework, Ring,
    Segment, Shape, XY, orientation,
};
impl Eq for PolylabelCell {}

impl PartialEq for PolylabelCell {
    fn eq(&self, other: &Self) -> bool {
        self.max_distance.to_bits() == other.max_distance.to_bits()
    }
}

impl Ord for PolylabelCell {
    fn cmp(&self, other: &Self) -> Ordering {
        self.max_distance.total_cmp(&other.max_distance)
    }
}

impl PartialOrd for PolylabelCell {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Welzl's smallest enclosing circle (iterative move-to-front form, expected
/// O(n) after a deterministic LCG shuffle), returning the center and a
/// support vertex (an input vertex on the circle — the radius is their
/// distance). The kernel runs in a unit frame — coordinates translated to the
/// bounds midpoint and scaled by the half-extent — so squared terms can
/// neither overflow (`1e155`-scale inputs) nor flush collinearity tests to
/// zero (subnormal-scale inputs); containment tests use the ordinary
/// binary64 squared-distance comparison without scale-dependent slack.
pub(crate) fn smallest_enclosing_circle(points: &[Point]) -> (Point, Point) {
    fn contains(center: Point, radius_sq: f64, point: Point) -> bool {
        crate::geometry::segments::point_distance_squared(center, point) <= radius_sq
    }
    fn from_two(a: Point, b: Point) -> (Point, f64) {
        let center = Point::new_raw_xy(f64::midpoint(a.x, b.x), f64::midpoint(a.y, b.y));
        (
            center,
            crate::geometry::segments::point_distance_squared(center, a),
        )
    }
    fn circumcircle(a: Point, b: Point, c: Point) -> Option<(Point, f64)> {
        let bx = b.x - a.x;
        let by = b.y - a.y;
        let cx = c.x - a.x;
        let cy = c.y - a.y;
        let d = 2.0 * (bx * cy - by * cx);
        if d == 0.0 {
            return None; // collinear
        }
        let b2 = bx * bx + by * by;
        let c2 = cx * cx + cy * cy;
        let ux = (cy * b2 - by * c2) / d;
        let uy = (bx * c2 - cx * b2) / d;
        let center = Point::new_raw_xy(a.x + ux, a.y + uy);
        Some((center, ux * ux + uy * uy))
    }
    /// Largest of the three pairwise circles (the collinear fallback).
    fn widest_pair(a: Point, b: Point, c: Point) -> (Point, f64) {
        [from_two(a, b), from_two(a, c), from_two(b, c)]
            .into_iter()
            .max_by(|left, right| left.1.total_cmp(&right.1))
            .expect("three candidates")
    }

    // Unit frame: translate to the bounds midpoint, then derive the extent
    // from finite distances to that midpoint. Halving both extrema first can
    // underflow at the smallest scale (`±s / 2` is zero); midpoint distances
    // retain a nonzero extent even there. Original input vertices are kept
    // aside for the support.
    let bounds = Bounds::from_points(points.iter().copied());
    let mid = Point::new_raw_xy(
        f64::midpoint(bounds.minx(), bounds.maxx()),
        f64::midpoint(bounds.miny(), bounds.maxy()),
    );
    let extent = (bounds.maxx() - mid.x)
        .max(mid.x - bounds.minx())
        .max(bounds.maxy() - mid.y)
        .max(mid.y - bounds.miny());
    let scale = if extent > 0.0 { extent } else { 1.0 };
    let originals = points;
    let mut points = points
        .iter()
        .map(|point| Point::new_raw_xy((point.x - mid.x) / scale, (point.y - mid.y) / scale))
        .collect::<Vec<_>>();

    // Deterministic shuffle (LCG) to defeat adversarial input order.
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15 ^ (points.len() as u64);
    for index in (1..points.len()).rev() {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let other = (state >> 33) as usize % (index + 1);
        points.swap(index, other);
    }

    let mut circle = (points[0], 0.0);
    for i in 1..points.len() {
        if contains(circle.0, circle.1, points[i]) {
            continue;
        }
        circle = (points[i], 0.0);
        for j in 0..i {
            if contains(circle.0, circle.1, points[j]) {
                continue;
            }
            circle = from_two(points[i], points[j]);
            for k in 0..j {
                if contains(circle.0, circle.1, points[k]) {
                    continue;
                }
                circle = circumcircle(points[i], points[j], points[k])
                    .unwrap_or_else(|| widest_pair(points[i], points[j], points[k]));
            }
        }
    }

    // The support is the original vertex farthest from the denormalized
    // center — measured against the *original* coordinates (in the unit
    // scale, so the comparison cannot overflow) so the reported radius
    // includes the center's denormalization rounding.
    let center = Point::new_raw_xy(mid.x + circle.0.x * scale, mid.y + circle.0.y * scale);
    let scaled_distance = |point: &Point| {
        let dx = (point.x - center.x) / scale;
        let dy = (point.y - center.y) / scale;
        dx * dx + dy * dy
    };
    let support = originals
        .iter()
        .max_by(|a, b| scaled_distance(a).total_cmp(&scaled_distance(b)))
        .expect("non-empty");
    (center, *support)
}

/// The pole of inaccessibility, returned after denormalizing the search center.
/// Maximum-circle clearance is recomputed separately in source coordinates;
/// a rounded nearest-point witness is not a continuum-distance certificate.
struct PolylabelFrame {
    shape: Shape,
    frame: AxisFrame,
    default_tolerance: f64,
    max_inradius: f64,
}

/// Euclidean distance in an [`AxisFrame`]'s local coordinates.
///
/// The frame intentionally has a distinct scale for each axis.  That keeps a
/// reciprocal rectangle representable, but it also means its local space is
/// *not* Euclidean: a plain local nearest-point query would choose a vertical
/// edge over the physically-near horizontal one.  Keep the metric here, next
/// to the only consumer that needs it, rather than silently changing the
/// geometry it is measuring.
fn framed_metric_distance(frame: AxisFrame, left: XY, right: XY) -> f64 {
    ((left.x - right.x) / frame.scale_x()).hypot((left.y - right.y) / frame.scale_y())
}

/// Nearest point on a local-frame segment under the frame's original
/// Euclidean metric.  Normalizing the segment direction before its dot
/// product avoids both the huge-axis overflow and the tiny-axis false zero.
fn framed_metric_nearest_on_segment(
    frame: AxisFrame,
    point: Point,
    segment: Segment,
) -> (Point, f64) {
    let ux = (point.x - segment.start.x) / frame.scale_x();
    let uy = (point.y - segment.start.y) / frame.scale_y();
    let vx = (segment.end.x - segment.start.x) / frame.scale_x();
    let vy = (segment.end.y - segment.start.y) / frame.scale_y();
    let magnitude = vx.abs().max(vy.abs());
    let fraction = if magnitude == 0.0 {
        0.0
    } else {
        let nx = vx / magnitude;
        let ny = vy / magnitude;
        // Do not form `infinity * 0.0` for a probe very far along the other
        // axis of a short horizontal/vertical segment.
        let dot = if nx == 0.0 {
            0.0
        } else {
            (ux / magnitude) * nx
        } + if ny == 0.0 {
            0.0
        } else {
            (uy / magnitude) * ny
        };
        (dot / (nx * nx + ny * ny)).clamp(0.0, 1.0)
    };
    let nearest = Point::new_raw_xy(
        segment.start.x + (segment.end.x - segment.start.x) * fraction,
        segment.start.y + (segment.end.y - segment.start.y) * fraction,
    );
    (
        nearest,
        framed_metric_distance(frame, point.xy(), nearest.xy()),
    )
}

fn framed_metric_cell_radius(frame: AxisFrame, half_size: f64) -> f64 {
    (half_size / frame.scale_x()).hypot(half_size / frame.scale_y())
}

fn normalized_polylabel_frame(
    shape: &Shape,
    bounds: Bounds,
) -> crate::error::Result<Option<PolylabelFrame>> {
    let midpoint = Point::new_raw_xy(
        f64::midpoint(bounds.minx(), bounds.maxx()),
        f64::midpoint(bounds.miny(), bounds.maxy()),
    );
    let half_width = bounds.maxx() / 2.0 - bounds.minx() / 2.0;
    let half_height = bounds.maxy() / 2.0 - bounds.miny() / 2.0;
    let Some(frame) = AxisFrame::from_origin_extents(
        midpoint,
        half_width
            .abs()
            .max(bounds.minx().abs())
            .max(bounds.maxx().abs()),
        half_height
            .abs()
            .max(bounds.miny().abs())
            .max(bounds.maxy().abs()),
    ) else {
        return Ok(None);
    };
    let xy = shape.force_2d()?;
    let normalized = xy.try_map_coordseqs(
        |seq| {
            let mut builder = CoordSeqBuilder::try_with_capacity(seq.axes(), seq.len())?;
            for point in seq.iter_coords() {
                let xy = frame.frame_xy(point.x, point.y);
                builder.push(point.with_xy(xy.x, xy.y)?)?;
            }
            builder.finish()
        },
        |point| {
            let xy = frame.frame_xy(point.x, point.y);
            point.with_xy(xy.x, xy.y)
        },
    )?;
    let normalized_bounds = normalized
        .bounds()
        .expect("non-empty normalized polygon retains bounds");
    Ok(Some(PolylabelFrame {
        shape: normalized,
        frame,
        // Tolerances and bounds are public Euclidean distances.  The local
        // frame is only a representation rescue, so convert each axis back
        // independently before comparing them.
        default_tolerance: ((normalized_bounds.maxx() - normalized_bounds.minx()).abs()
            / frame.scale_x())
        .min((normalized_bounds.maxy() - normalized_bounds.miny()).abs() / frame.scale_y())
            / 500.0,
        max_inradius: ((normalized_bounds.maxx() - normalized_bounds.minx()).abs()
            / frame.scale_x())
        .min((normalized_bounds.maxy() - normalized_bounds.miny()).abs() / frame.scale_y())
            / 2.0,
    }))
}

pub(crate) fn polylabel_point(shape: &Shape, tolerance: Option<f64>) -> Result<Point> {
    let Some(bounds) = shape.bounds() else {
        let origin = Point::new_raw_xy(0.0, 0.0);
        return Ok(origin);
    };
    // Degenerate input — a zero-width bounding box (a sliver or a point): the
    // representative point is the answer. Build the original-frame field only
    // on this rare branch.
    let Some(frame) = normalized_polylabel_frame(shape, bounds)? else {
        return representative_point(shape, bounds);
    };
    let normalized_bounds = frame
        .shape
        .bounds()
        .expect("non-empty normalized polygon retains bounds");
    let mut field = PolylabelField::new(&frame.shape, frame.max_inradius, Some(frame.frame))?;
    if field.is_empty() {
        return representative_point(shape, bounds);
    }

    // Tolerance remains in public Euclidean coordinates.  The framed field
    // measures in that same metric, so an explicit tolerance is not silently
    // reinterpreted as a coordinate-space tolerance.
    let tolerance = tolerance.unwrap_or(frame.default_tolerance);

    let mut best = field.cell(
        representative_point(&frame.shape, normalized_bounds)?,
        0.0,
        None,
    )?;
    let bbox_center = Point::new_raw_xy(0.0, 0.0);
    let bbox_cell = field.cell(bbox_center, 0.0, None)?;
    if bbox_cell.distance > best.distance {
        best = bbox_cell;
    }

    let mut budget = ExpansionBudget::new("polylabel", "tolerance");
    budget.add(1)?;
    let mut cells = BinaryHeap::with_capacity(1);
    cells.push(field.cell(bbox_center, 1.0, None)?);

    while let Some(cell) = cells.pop() {
        if cell.distance > best.distance {
            best = cell;
        }
        if cell.max_distance - best.distance <= tolerance {
            continue;
        }

        let half_size = cell.half_size / 2.0;
        if half_size <= 0.0 {
            continue;
        }
        budget.add(4)?;
        for dx in [-half_size, half_size] {
            for dy in [-half_size, half_size] {
                cells.push(field.cell(
                    Point::new_raw_xy(cell.center.x + dx, cell.center.y + dy),
                    half_size,
                    Some(&cell),
                )?);
            }
        }
    }

    let denormalize = |point: Point| {
        let world = frame.frame.unframe_xy(point.xy());
        Point::new_raw_xy(world.x, world.y)
    };
    let center = denormalize(best.center);
    Ok(center)
}

type ExactRatio = (ExactDyadic, ExactDyadic);

fn compare_exact_ratios(left: &ExactRatio, right: &ExactRatio) -> Result<Ordering> {
    try_compare_ratios(&left.0, &left.1, &right.0, &right.1)
}

fn exact_difference(left: f64, right: f64) -> Result<ExactDyadic> {
    ExactDyadic::try_from_f64(left)?.try_subtract(ExactDyadic::try_from_f64(right)?)
}

fn exact_squared_sum(left: &ExactDyadic, right: &ExactDyadic) -> Result<ExactDyadic> {
    left.try_product(left)?.try_add(right.try_product(right)?)
}

fn exact_point_pair_squared(left: XY, right: XY) -> Result<ExactRatio> {
    let dx = exact_difference(left.x, right.x)?;
    let dy = exact_difference(left.y, right.y)?;
    Ok((
        exact_squared_sum(&dx, &dy)?,
        ExactDyadic::try_from_f64(1.0)?,
    ))
}

fn exact_point_segment_squared(point: Point, segment: Segment) -> Result<ExactRatio> {
    let point = point.xy();
    let dx = exact_difference(segment.end.x, segment.start.x)?;
    let dy = exact_difference(segment.end.y, segment.start.y)?;
    let qx = exact_difference(point.x, segment.start.x)?;
    let qy = exact_difference(point.y, segment.start.y)?;
    let denominator = dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)?;
    let zero = ExactDyadic::try_from_f64(0.0)?;
    if denominator.is_zero() {
        return exact_point_pair_squared(point, segment.start);
    }
    let numerator = qx.try_product(&dx)?.try_add(qy.try_product(&dy)?)?;
    if numerator.cmp(&zero) != Ordering::Greater {
        return exact_point_pair_squared(point, segment.start);
    }
    if numerator.cmp(&denominator) != Ordering::Less {
        return exact_point_pair_squared(point, segment.end);
    }
    let point_squared = exact_squared_sum(&qx, &qy)?;
    Ok((
        point_squared
            .try_product(&denominator)?
            .try_subtract(numerator.try_product(&numerator)?)?,
        denominator,
    ))
}

fn exact_boundary_radius(shape: &Shape, center: Point) -> Result<f64> {
    let mut best: Option<ExactRatio> = None;
    let mut error = None;
    shape.for_each_segment(|segment| {
        if error.is_some() {
            return;
        }
        let candidate = match exact_point_segment_squared(center, segment) {
            Ok(candidate) => candidate,
            Err(failure) => {
                error = Some(failure);
                return;
            },
        };
        let replace =
            best.as_ref()
                .is_none_or(|current| match compare_exact_ratios(&candidate, current) {
                    Ok(order) => order == Ordering::Less,
                    Err(failure) => {
                        error = Some(failure);
                        false
                    },
                });
        if replace {
            best = Some(candidate);
        }
    });
    if let Some(error) = error {
        return Err(error);
    }
    best.map_or(Ok(0.0), |distance| {
        round_sqrt_ratio(&distance.0, &distance.1)
    })
}

fn exact_cross(
    left_x: &ExactDyadic,
    left_y: &ExactDyadic,
    right_x: &ExactDyadic,
    right_y: &ExactDyadic,
) -> Result<ExactDyadic> {
    left_x
        .try_product(right_y)?
        .try_subtract(left_y.try_product(right_x)?)
}

fn update_lower(lower: &mut ExactRatio, lower_closed: &mut bool, bound: ExactRatio) -> Result<()> {
    match compare_exact_ratios(&bound, lower)? {
        Ordering::Greater => {
            *lower = bound;
            *lower_closed = false;
        },
        Ordering::Equal => *lower_closed = false,
        Ordering::Less => {},
    }
    Ok(())
}

fn update_upper(upper: &mut ExactRatio, upper_closed: &mut bool, bound: ExactRatio) -> Result<()> {
    match compare_exact_ratios(&bound, upper)? {
        Ordering::Less => {
            *upper = bound;
            *upper_closed = false;
        },
        Ordering::Equal => *upper_closed = false,
        Ordering::Greater => {},
    }
    Ok(())
}

fn interval_is_empty(
    lower: &ExactRatio,
    lower_closed: bool,
    upper: &ExactRatio,
    upper_closed: bool,
) -> Result<bool> {
    Ok(match compare_exact_ratios(lower, upper)? {
        Ordering::Greater => true,
        Ordering::Equal => !(lower_closed && upper_closed),
        Ordering::Less => false,
    })
}

/// Whether a source segment has any point in the candidate's open interior.
/// The candidate is convex, so each edge contributes one exact affine strict
/// half-plane in the source segment parameter. The interval remains closed at
/// the source endpoints; only candidate boundary contacts are strict.
fn segment_enters_candidate(
    segment: Segment,
    candidate: &[Point],
    counter_clockwise: bool,
) -> Result<bool> {
    let zero = ExactDyadic::try_from_f64(0.0)?;
    let one = ExactDyadic::try_from_f64(1.0)?;
    let lower_bound = (zero.try_clone()?, one.try_clone()?);
    let upper_bound = (one.try_clone()?, one);
    let mut lower = lower_bound;
    let mut upper = upper_bound;
    let mut lower_closed = true;
    let mut upper_closed = true;
    let source_dx = exact_difference(segment.end.x, segment.start.x)?;
    let source_dy = exact_difference(segment.end.y, segment.start.y)?;

    for index in 0..candidate.len() {
        let start = candidate[index];
        let end = candidate[(index + 1) % candidate.len()];
        let edge_dx = exact_difference(end.x, start.x)?;
        let edge_dy = exact_difference(end.y, start.y)?;
        let start_x = exact_difference(segment.start.x, start.x)?;
        let start_y = exact_difference(segment.start.y, start.y)?;
        let c0 = exact_cross(&edge_dx, &edge_dy, &start_x, &start_y)?;
        let c1 = exact_cross(&edge_dx, &edge_dy, &source_dx, &source_dy)?;
        let (c0, c1) = if counter_clockwise {
            (c0, c1)
        } else {
            (
                zero.try_clone()?.try_subtract(c0)?,
                zero.try_clone()?.try_subtract(c1)?,
            )
        };
        match c1.sign() {
            crate::geometry::exact_arith::ExactSign::Zero => {
                if c0.sign() != crate::geometry::exact_arith::ExactSign::Positive {
                    return Ok(false);
                }
            },
            crate::geometry::exact_arith::ExactSign::Positive => {
                update_lower(
                    &mut lower,
                    &mut lower_closed,
                    (zero.try_clone()?.try_subtract(c0)?, c1),
                )?;
            },
            crate::geometry::exact_arith::ExactSign::Negative => {
                update_upper(
                    &mut upper,
                    &mut upper_closed,
                    (c0, zero.try_clone()?.try_subtract(c1)?),
                )?;
            },
        }
        if interval_is_empty(&lower, lower_closed, &upper, upper_closed)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn candidate_center_is_strictly_inside(
    candidate: &[Point],
    center: Point,
    counter_clockwise: bool,
) -> bool {
    candidate.iter().enumerate().all(|(index, &start)| {
        let end = candidate[(index + 1) % candidate.len()];
        matches!(
            (counter_clockwise, orientation(start, end, center)),
            (true, Orientation::CounterClockwise) | (false, Orientation::Clockwise)
        )
    })
}

fn source_boundary_enters_candidate(
    shape: &Shape,
    candidate: &[Point],
    counter_clockwise: bool,
) -> Result<bool> {
    let mut enters = false;
    let mut error = None;
    shape.for_each_segment(|segment| {
        if enters || error.is_some() {
            return;
        }
        match segment_enters_candidate(segment, candidate, counter_clockwise) {
            Ok(true) => enters = true,
            Ok(false) => {},
            Err(failure) => error = Some(failure),
        }
    });
    if let Some(error) = error {
        return Err(error);
    }
    Ok(enters)
}

pub(crate) fn polylabel_center_radius(
    shape: &Shape,
    tolerance: Option<f64>,
) -> Result<(Point, f64)> {
    let center = polylabel_point(shape, tolerance)?;
    Ok((center, exact_boundary_radius(shape, center)?))
}

pub(crate) fn emit_zero_radius_point(shape: &Shape, center: Point) -> Result<Shape> {
    // A positive subnormal clearance may round to zero; closed membership, not
    // the rounded radius, decides whether the emitted point belongs to source.
    if center.x.is_finite() && center.y.is_finite() && shape.covers_point(center)? {
        Ok(Shape::Point(center))
    } else {
        Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ))
    }
}

pub(crate) fn build_inscribed_circle(
    shape: &Shape,
    center: Point,
    radius: f64,
    quadrant_segments: NonZeroU32,
    certify_source: bool,
) -> Result<Shape> {
    let steps = 4_usize
        .checked_mul(quadrant_segments.get() as usize)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let vertices = steps
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let mut budget = ExpansionBudget::new("maximum_inscribed_circle", "quadrant_segments");
    budget.add(vertices)?;
    if !center.x.is_finite() || !center.y.is_finite() || !radius.is_finite() {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ));
    }
    let mut points = Vec::new();
    crate::try_reserve_vec(&mut points, steps)?;
    let quadrant = quadrant_segments.get() as usize;
    for step in 0..steps {
        let (unit_x, unit_y) = if step.is_multiple_of(quadrant) {
            match (step / quadrant) % 4 {
                0 => (1.0, 0.0),
                1 => (0.0, 1.0),
                2 => (-1.0, 0.0),
                _ => (0.0, -1.0),
            }
        } else {
            let (sin, cos) = (std::f64::consts::TAU * step as f64 / steps as f64).sin_cos();
            (cos, sin)
        };
        let x = center.x + radius * unit_x;
        let y = center.y + radius * unit_y;
        if !x.is_finite() || !y.is_finite() {
            return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
                "maximum_inscribed_circle",
            ));
        }
        points.push(Point::new_raw_xy(x, y));
    }
    let mut closed = Vec::new();
    crate::try_reserve_vec(&mut closed, vertices)?;
    closed.extend_from_slice(&points);
    closed.push(points[0]);
    let seq = CoordSeq::try_from_points(&closed)?;
    let winding = crate::geometry::ring_winding(&seq);
    let strict_convex = crate::geometry::predicates::shell_is_strictly_convex(&seq);
    if winding.is_degenerate() || !strict_convex {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ));
    }
    let counter_clockwise = winding.is_ccw();
    let center_in_candidate =
        candidate_center_is_strictly_inside(&points, center, counter_clockwise);
    if !center_in_candidate
        || (certify_source
            && (!shape.contains_point(center)?
                || source_boundary_enters_candidate(shape, &points, counter_clockwise)?))
    {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ));
    }
    Ok(Shape::Polygon(Polygon::new(
        Ring::from_closed(seq),
        Vec::new(),
    )?))
}

pub(crate) fn representative_point(shape: &Shape, bounds: Bounds) -> Result<Point> {
    match shape.point_on_surface()? {
        Shape::Point(point) => Ok(point),
        _ => Ok(Point::new_raw_xy(
            bounds.minx() + (bounds.maxx() - bounds.minx()) / 2.0,
            bounds.miny() + (bounds.maxy() - bounds.miny()) / 2.0,
        )),
    }
}

/// The indexed boundary a polylabel search probes once per cell. The facet
/// BVH is built ONCE over the linework, so each cell's distance is an
/// `O(log segments)` SIMD branch-and-bound descent rather than the per-cell
/// `O(segments)` scan it replaces. Containment (the distance sign) rides a
/// direct ray cast — needed for only the seed and boundary-straddling cells,
/// far below a full hierarchical `PointBatchTester` build's payoff. The
/// maximum-inscribed-circle owner performs its own exact source-coordinate
/// clearance fold after the public center is denormalized.
struct PolylabelField<'a> {
    shape: &'a Shape,
    linework: PreparedLinework,
    bvh: Option<FacetBvh>,
    /// Branch-and-bound traversal stack, reused across every cell probe.
    stack: Vec<u32>,
    max_inradius: f64,
    /// `Some` keeps nearest-point and cell bounds in the original Euclidean
    /// metric while the stored shape remains in its lossless local frame.
    frame: Option<AxisFrame>,
}

impl<'a> PolylabelField<'a> {
    fn new(
        shape: &'a Shape,
        max_inradius: f64,
        frame: Option<AxisFrame>,
    ) -> crate::error::Result<Self> {
        let linework = PreparedLinework::build(shape)?;
        // Below the indexing crossover the flat SIMD facet scan beats a tree
        // descent (its stack churn does not pay for a handful of facets).
        let bvh = (linework.segment_count() >= BVH_MIN_INDEXED_SEGMENTS)
            .then(|| FacetBvh::build(&linework))
            .flatten();
        Ok(Self {
            shape,
            linework,
            bvh,
            stack: Vec::new(),
            max_inradius,
            frame,
        })
    }

    const fn is_empty(&self) -> bool {
        self.linework.segment_count() == 0
    }

    /// Unsigned distance from `point` to the boundary linework. The unframed
    /// path runs its SIMD **squared** kernel (eight segment distances per
    /// vector op) and the single `sqrt` lifts the winner back to Euclidean
    /// units. A framed path stays in unsquared original-metric units because
    /// its independent local-axis scales make squared local distances
    /// meaningless.
    ///
    /// `bound` seeds the branch-and-bound with a known upper bound on the
    /// result, so the descent prunes every node farther than it from the
    /// outset (the true nearest is never pruned — it lies at or below the
    /// bound). `f64::INFINITY` means "no prior knowledge" (the seed cells).
    fn boundary_distance(&mut self, point: Point, bound: f64) -> Result<f64> {
        if let Some(frame) = self.frame {
            let linework = &self.linework;
            let mut refine = |facet: crate::geometry::facet_bvh::Facet| {
                let mut best = f64::INFINITY;
                for index in 0..facet.segment_count as usize {
                    best = best.min(
                        framed_metric_nearest_on_segment(
                            frame,
                            point,
                            linework.segment(facet, index),
                        )
                        .1,
                    );
                }
                best
            };
            let distance = if let Some(bvh) = self.bvh.as_ref() {
                bvh.min_point_distance_with_stack_framed(
                    linework,
                    point,
                    frame,
                    &mut self.stack,
                    bound,
                    &mut refine,
                )?
            } else {
                let mut best = f64::INFINITY;
                linework.for_each_segment(|segment| {
                    best = best.min(framed_metric_nearest_on_segment(frame, point, segment).1);
                });
                best
            };
            return Ok(distance);
        }
        // A positive subnormal seed can square to zero. Zero is a valid exact
        // contact seed, but an underflowed positive seed would prune every
        // positive AABB bound before the squared refinement gets a chance to
        // run.
        let bound_squared = bound * bound;
        let bound_squared = if bound > 0.0 && bound_squared == 0.0 {
            f64::INFINITY
        } else {
            bound_squared
        };
        let squared = match &self.bvh {
            Some(bvh) => bvh.min_point_distance_with_stack::<true>(
                &self.linework,
                point.x,
                point.y,
                &mut self.stack,
                bound_squared,
            ),
            None => self
                .linework
                .min_points_distance::<true>(std::iter::once((point.x, point.y)), bound_squared),
        };
        Ok(squared?.sqrt())
    }

    /// Build a cell with its signed boundary distance (`+d` inside the area,
    /// `-d` outside — the even-odd `contains_point` rule).
    ///
    /// `parent` is the cell being refined, when any. A child center lies
    /// within `parent.half_size·(1/√2)` of the parent center, so when the
    /// boundary provably cannot reach across that gap
    /// (`|parent.distance| >` the gap) the child shares the parent's side and
    /// the containment probe is skipped. The search spends most of its cells
    /// refining deep inside the optimum, where this holds — so the
    /// containment probe runs only for the few cells straddling the boundary.
    fn cell(
        &mut self,
        center: Point,
        half_size: f64,
        parent: Option<&PolylabelCell>,
    ) -> Result<PolylabelCell> {
        // The child center lies within `parent.half_size·(1/√2)` of the parent
        // center (the diagonal quadrant offset), so by the triangle inequality
        // its boundary distance is at most `|parent.distance| + that gap`. Seed
        // the search with that bound (padded by a hair so float rounding never
        // prunes the true nearest facet). The same gap decides the sign for
        // free when the boundary cannot reach across it.
        let (bound, inherited_sign) = parent.map_or((f64::INFINITY, None), |parent| {
            let gap = self.frame.map_or(
                parent.half_size * std::f64::consts::FRAC_1_SQRT_2,
                |frame| framed_metric_cell_radius(frame, parent.half_size / 2.0),
            );
            let bound = (parent.distance.abs() + gap) * (1.0 + 1e-9);
            let sign = (parent.distance.abs() > gap).then_some(parent.distance > 0.0);
            (bound, sign)
        });
        let distance = self.boundary_distance(center, bound)?;
        // Containment is needed only for the seed cells and the few cells that
        // straddle the boundary (deep cells inherit their parent's sign), so a
        // direct even-odd ray cast beats building a hierarchical
        // `PointBatchTester` whose O(segments·log) sort never amortizes over
        // so few probes.
        let inside = match inherited_sign {
            Some(inside) => inside,
            None => self.shape.contains_point(center)?,
        };
        let signed = if inside { distance } else { -distance };
        let cell_radius = self
            .frame
            .map_or(half_size * std::f64::consts::SQRT_2, |frame| {
                framed_metric_cell_radius(frame, half_size)
            });
        Ok(PolylabelCell {
            center,
            half_size,
            distance: signed,
            max_distance: (cell_radius + signed).min(self.max_inradius),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ErrorKind, ResourceError};
    use crate::{CoordSeq, Polygon, Ring};

    fn circle_points(center: Point, radius: f64) -> Vec<Point> {
        let mut points = Vec::with_capacity(64);
        for step in 0..64 {
            let (unit_x, unit_y) = if step % 16 == 0 {
                match (step / 16) % 4 {
                    0 => (1.0, 0.0),
                    1 => (0.0, 1.0),
                    2 => (-1.0, 0.0),
                    _ => (0.0, -1.0),
                }
            } else {
                let (sin, cos) = (std::f64::consts::TAU * f64::from(step) / 64.0).sin_cos();
                (cos, sin)
            };
            points.push(Point::new_raw_xy(
                center.x + radius * unit_x,
                center.y + radius * unit_y,
            ));
        }
        points
    }

    fn degenerate_line_shape() -> Shape {
        Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    crate::geometry::CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.0, 0.0),
                        Point::new_raw_xy(2.0, 0.0),
                        Point::new_raw_xy(1.0, 0.0),
                        Point::new_raw_xy(0.0, 0.0),
                    ])
                    .unwrap(),
                ),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    fn dense_reciprocal_polygon(subdivisions: usize) -> Shape {
        let extent = 1.0e162;
        let half_height = 1.0e-162;
        let mut points = Vec::with_capacity(2 * (subdivisions + 1) + 1);
        for index in 0..=subdivisions {
            let fraction = index as f64 / subdivisions as f64;
            let x = -extent + 2.0 * extent * fraction;
            points.push(Point::new_raw_xy(x, -half_height));
        }
        for index in (0..=subdivisions).rev() {
            let fraction = index as f64 / subdivisions as f64;
            let x = -extent + 2.0 * extent * fraction;
            points.push(Point::new_raw_xy(x, half_height));
        }
        points.push(points[0]);
        Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    fn dense_unit_polygon(subdivisions: usize) -> Shape {
        let mut points = Vec::with_capacity(2 * (subdivisions + 1) + 1);
        for index in 0..=subdivisions {
            let fraction = index as f64 / subdivisions as f64;
            points.push(Point::new_raw_xy(-1.0 + 2.0 * fraction, -1.0));
        }
        for index in (0..=subdivisions).rev() {
            let fraction = index as f64 / subdivisions as f64;
            points.push(Point::new_raw_xy(-1.0 + 2.0 * fraction, 1.0));
        }
        points.push(points[0]);
        Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    fn normalized_dense_reciprocal_polygon() -> PolylabelFrame {
        let shape = dense_reciprocal_polygon(256);
        let bounds = shape.bounds().expect("dense polygon has bounds");
        normalized_polylabel_frame(&shape, bounds)
            .unwrap()
            .expect("reciprocal axes have a frame")
    }

    fn flat_framed_distance(frame: AxisFrame, linework: &PreparedLinework, point: Point) -> f64 {
        let mut best = f64::INFINITY;
        linework.for_each_segment(|segment| {
            best = best.min(framed_metric_nearest_on_segment(frame, point, segment).1);
        });
        best
    }

    fn indexed_framed_distance(
        frame: AxisFrame,
        linework: &PreparedLinework,
        bvh: &FacetBvh,
        point: Point,
    ) -> f64 {
        let mut stack = Vec::new();
        bvh.min_point_distance_with_stack_framed(
            linework,
            point,
            frame,
            &mut stack,
            f64::INFINITY,
            |facet| {
                let mut best = f64::INFINITY;
                for index in 0..facet.segment_count as usize {
                    best = best.min(
                        framed_metric_nearest_on_segment(
                            frame,
                            point,
                            linework.segment(facet, index),
                        )
                        .1,
                    );
                }
                best
            },
        )
        .unwrap()
    }

    #[test]
    fn reciprocal_ribbon_keeps_the_physical_nearest_edge() {
        let extent = 1.0e162;
        let half_height = 1.0 / extent;
        let shape = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(-extent, -half_height),
                        Point::new_raw_xy(extent, -half_height),
                        Point::new_raw_xy(extent, half_height),
                        Point::new_raw_xy(-extent, half_height),
                        Point::new_raw_xy(-extent, -half_height),
                    ])
                    .unwrap(),
                ),
                Vec::new(),
            )
            .unwrap(),
        );
        let center = polylabel_point(&shape, None).expect("finite ribbon");
        assert_eq!(center, Point::new_raw_xy(0.0, 0.0));
    }

    #[test]
    fn reciprocal_axis_dense_indexed_field_preserves_framed_distance() {
        let normalized = normalized_dense_reciprocal_polygon();
        let linework = PreparedLinework::build(&normalized.shape).unwrap();
        let bvh = FacetBvh::build(&linework).expect("dense linework has an index");
        let mut field = PolylabelField::new(
            &normalized.shape,
            normalized.max_inradius,
            Some(normalized.frame),
        )
        .unwrap();

        for point in [
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(0.25, -0.2),
            Point::new_raw_xy(-0.75, 0.4),
        ] {
            let flat_distance = flat_framed_distance(normalized.frame, &linework, point);
            let indexed_distance =
                indexed_framed_distance(normalized.frame, &linework, &bvh, point);
            let field_distance = field.boundary_distance(point, f64::INFINITY).unwrap();
            assert_eq!(indexed_distance.to_bits(), flat_distance.to_bits());
            assert_eq!(field_distance.to_bits(), flat_distance.to_bits());
        }
    }

    #[test]
    fn framed_subnormal_seed_stays_in_unsquared_distance_units() {
        let frame = AxisFrame::from_origin_extents(Point::new_raw_xy(0.0, 0.0), 1.0, 1.0e-320)
            .expect("positive frame extents");
        let linework = PreparedLinework::from_open_xy_columns(&[0.0, 1.0], &[1.0, 1.0]).unwrap();
        let bvh = FacetBvh::build(&linework).expect("one facet has a tree root");
        let point = Point::new_raw_xy(0.0, 0.0);
        let expected =
            framed_metric_nearest_on_segment(frame, point, linework.segment(linework.facets[0], 0))
                .1;
        assert!(expected > 0.0 && expected < f64::MIN_POSITIVE);
        let mut stack = Vec::new();
        let actual = bvh
            .min_point_distance_with_stack_framed(
                &linework,
                point,
                frame,
                &mut stack,
                expected,
                |facet| {
                    let mut best = f64::INFINITY;
                    for index in 0..facet.segment_count as usize {
                        best = best.min(
                            framed_metric_nearest_on_segment(
                                frame,
                                point,
                                linework.segment(facet, index),
                            )
                            .1,
                        );
                    }
                    best
                },
            )
            .unwrap();
        assert_eq!(actual.to_bits(), expected.to_bits());
    }

    #[test]
    fn unframed_positive_seed_underflow_does_not_prune_positive_distance() {
        let shape = dense_unit_polygon(256);
        let mut field = PolylabelField::new(&shape, 1.0, None).unwrap();
        let tiny_seed = f64::from_bits(1);
        assert_eq!(tiny_seed * tiny_seed, 0.0);
        let distance = field
            .boundary_distance(Point::new_raw_xy(0.0, 0.0), tiny_seed)
            .unwrap();
        assert_eq!(distance.to_bits(), 1.0_f64.to_bits());
    }

    #[test]
    fn reciprocal_axis_index_refines_fewer_facets_and_segments_than_flat() {
        let normalized = normalized_dense_reciprocal_polygon();
        let linework = PreparedLinework::build(&normalized.shape).unwrap();
        let bvh = FacetBvh::build(&linework).expect("dense linework has an index");
        let point = Point::new_raw_xy(0.0, 0.0);
        let mut refined_facets = 0;
        let mut refined_segments = 0;
        let mut stack = Vec::new();
        let indexed_distance = bvh
            .min_point_distance_with_stack_framed(
                &linework,
                point,
                normalized.frame,
                &mut stack,
                f64::INFINITY,
                |facet| {
                    refined_facets += 1;
                    refined_segments += facet.segment_count as usize;
                    let mut best = f64::INFINITY;
                    for index in 0..facet.segment_count as usize {
                        best = best.min(
                            framed_metric_nearest_on_segment(
                                normalized.frame,
                                point,
                                linework.segment(facet, index),
                            )
                            .1,
                        );
                    }
                    best
                },
            )
            .unwrap();
        let flat_distance = flat_framed_distance(normalized.frame, &linework, point);
        assert_eq!(indexed_distance.to_bits(), flat_distance.to_bits());
        assert!(refined_facets < linework.facets.len());
        assert!(refined_segments < linework.segment_count());
    }

    #[test]
    fn zero_radius_boundary_point_uses_closed_membership() {
        let shape = degenerate_line_shape();
        let result = shape.maximum_inscribed_circle(None).unwrap();
        let Shape::Point(point) = result else {
            panic!("degenerate maximum circle must emit its point center");
        };
        assert!(shape.covers_point(point).unwrap());
        assert!(!shape.contains_point(point).unwrap());
    }

    #[test]
    fn zero_radius_outside_point_is_rejected_at_emission_boundary() {
        let shape = degenerate_line_shape();
        let result = emit_zero_radius_point(&shape, Point::new_raw_xy(1.0, 1.0));
        assert!(matches!(
            result,
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::Geometry(
                        crate::geometry::GeometryErrorKind::UnrepresentableOutput {
                            operation: "maximum_inscribed_circle"
                        }
                    )
                )
        ));
    }

    #[test]
    fn source_tangent_does_not_enter_inscribed_circle() {
        let center = Point::new_raw_xy(1.0, 1.0);
        let points = circle_points(center, 1.0);
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(2.0, 0.0),
        };
        assert!(!segment_enters_candidate(segment, &points, true).unwrap());
        for segment in [
            Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(2.0, 0.0),
            },
            Segment {
                start: XY::new(2.0, 0.0),
                end: XY::new(2.0, 2.0),
            },
            Segment {
                start: XY::new(2.0, 2.0),
                end: XY::new(0.0, 2.0),
            },
            Segment {
                start: XY::new(0.0, 2.0),
                end: XY::new(0.0, 0.0),
            },
        ] {
            assert!(!segment_enters_candidate(segment, &points, true).unwrap());
        }
        let mut closed = points.clone();
        closed.push(points[0]);
        let seq = CoordSeq::try_from_points(&closed).unwrap();
        assert!(!crate::geometry::ring_winding(&seq).is_degenerate());
        assert!(crate::geometry::shell_is_convex(&seq));
        assert!(candidate_center_is_strictly_inside(&points, center, true));
        let source = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    crate::geometry::CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.0, 0.0),
                        Point::new_raw_xy(2.0, 0.0),
                        Point::new_raw_xy(2.0, 2.0),
                        Point::new_raw_xy(0.0, 2.0),
                        Point::new_raw_xy(0.0, 0.0),
                    ])
                    .unwrap(),
                ),
                Vec::new(),
            )
            .unwrap(),
        );
        assert!(source.contains_point(center).unwrap());
        assert!(!source_boundary_enters_candidate(&source, &points, true).unwrap());
    }

    #[test]
    fn exact_boundary_radius_uses_the_smallest_stored_distance() {
        let shape = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    crate::geometry::CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.1, 0.1),
                        Point::new_raw_xy(0.3, 0.1),
                        Point::new_raw_xy(0.3, 0.3),
                        Point::new_raw_xy(0.1, 0.3),
                        Point::new_raw_xy(0.1, 0.1),
                    ])
                    .unwrap(),
                ),
                Vec::new(),
            )
            .unwrap(),
        );
        assert_eq!(
            exact_boundary_radius(&shape, Point::new_raw_xy(0.2, 0.2))
                .unwrap()
                .to_bits(),
            0x3FB9_9999_9999_9998
        );
    }

    #[test]
    fn exact_boundary_radius_handles_reciprocal_source_axes() {
        let extent = 1.0e150;
        let half_height = 1.0e-150;
        let shape = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    crate::geometry::CoordSeq::try_from_points(&[
                        Point::new_raw_xy(-extent, -half_height),
                        Point::new_raw_xy(extent, -half_height),
                        Point::new_raw_xy(extent, half_height),
                        Point::new_raw_xy(-extent, half_height),
                        Point::new_raw_xy(-extent, -half_height),
                    ])
                    .unwrap(),
                ),
                Vec::new(),
            )
            .unwrap(),
        );
        assert_eq!(
            exact_boundary_radius(&shape, Point::new_raw_xy(0.0, 0.0))
                .unwrap()
                .to_bits(),
            half_height.to_bits()
        );
    }

    #[test]
    fn source_hole_inside_candidate_is_rejected_without_boundary_contact() {
        let source = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    crate::geometry::CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.0, 0.0),
                        Point::new_raw_xy(10.0, 0.0),
                        Point::new_raw_xy(10.0, 10.0),
                        Point::new_raw_xy(0.0, 10.0),
                        Point::new_raw_xy(0.0, 0.0),
                    ])
                    .unwrap(),
                ),
                vec![Ring::from_closed(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(4.5, 4.5),
                        Point::new_raw_xy(5.5, 4.5),
                        Point::new_raw_xy(5.5, 5.5),
                        Point::new_raw_xy(4.5, 5.5),
                        Point::new_raw_xy(4.5, 4.5),
                    ])
                    .unwrap(),
                )],
            )
            .unwrap(),
        );
        let candidate = circle_points(Point::new_raw_xy(5.0, 3.0), 3.0);
        assert!(source.contains_point(Point::new_raw_xy(5.0, 3.0)).unwrap());
        assert!(source_boundary_enters_candidate(&source, &candidate, true).unwrap());
    }

    #[test]
    fn candidate_center_must_be_strictly_inside_every_edge() {
        let candidate = circle_points(Point::new_raw_xy(5.0, 3.0), 3.0);
        assert!(candidate_center_is_strictly_inside(
            &candidate,
            Point::new_raw_xy(5.0, 3.0),
            true,
        ));
        assert!(!candidate_center_is_strictly_inside(
            &candidate,
            Point::new_raw_xy(5.0, 6.0),
            true,
        ));
    }

    #[test]
    fn exact_boundary_radius_propagates_allocation_failure() {
        let shape = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    crate::geometry::CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.0, 0.0),
                        Point::new_raw_xy(2.0, 0.0),
                        Point::new_raw_xy(2.0, 2.0),
                        Point::new_raw_xy(0.0, 2.0),
                        Point::new_raw_xy(0.0, 0.0),
                    ])
                    .unwrap(),
                ),
                Vec::new(),
            )
            .unwrap(),
        );
        let result = crate::test_support::fail_allocations_after(0, || {
            exact_boundary_radius(&shape, Point::new_raw_xy(1.0, 1.0))
        });
        assert!(matches!(
            result,
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::Resource(ResourceError::Allocation)
                )
        ));
    }

    #[test]
    fn representative_point_propagates_surface_allocation_failure() {
        let shape = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    crate::geometry::CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.0, 0.0),
                        Point::new_raw_xy(4.0, 0.0),
                        Point::new_raw_xy(0.0, 4.0),
                        Point::new_raw_xy(0.0, 0.0),
                    ])
                    .unwrap(),
                ),
                Vec::new(),
            )
            .unwrap(),
        );
        let bounds = shape.bounds().expect("nonempty polygon");
        let bbox_seed = Point::new_raw_xy(
            bounds.minx() + (bounds.maxx() - bounds.minx()) / 2.0,
            bounds.miny() + (bounds.maxy() - bounds.miny()) / 2.0,
        );
        let (_, count) = crate::test_support::allocation_count(|| {
            representative_point(&shape, bounds).expect("count staging")
        });
        assert!(count > 0, "surface staging must allocate");
        let error =
            crate::test_support::fail_allocations_after(0, || representative_point(&shape, bounds))
                .expect_err("must not swallow staging failure as a bounding-box seed");
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
        let recovered = representative_point(&shape, bounds).expect("retry");
        assert_eq!((recovered.x, recovered.y), (1.0, 2.0));
        assert_ne!((recovered.x, recovered.y), (bbox_seed.x, bbox_seed.y));
    }
}
