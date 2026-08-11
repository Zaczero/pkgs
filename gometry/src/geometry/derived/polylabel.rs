#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::geometry::{
    AxisFrame, BVH_MIN_INDEXED_SEGMENTS, Bounds, CoordSeqBuilder, Coordinates as _,
    ExpansionBudget, FacetBvh, GeometryErrorKind, Point, PolylabelCell, PreparedLinework, Segment,
    Shape, XY,
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
/// zero (subnormal-scale inputs); containment tests use a scale-free relative
/// epsilon.
pub(crate) fn smallest_enclosing_circle(points: &[Point]) -> (Point, Point) {
    fn contains(center: Point, radius_sq: f64, point: Point) -> bool {
        crate::geometry::segments::point_distance_squared(center, point)
            <= radius_sq * (1.0 + 1e-12) + f64::MIN_POSITIVE
    }
    fn from_two(a: Point, b: Point) -> (Point, f64) {
        let center = Point::new_unchecked_xy(f64::midpoint(a.x, b.x), f64::midpoint(a.y, b.y));
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
        let center = Point::new_unchecked_xy(a.x + ux, a.y + uy);
        Some((center, ux * ux + uy * uy))
    }
    /// Largest of the three pairwise circles (the collinear fallback).
    fn widest_pair(a: Point, b: Point, c: Point) -> (Point, f64) {
        [from_two(a, b), from_two(a, c), from_two(b, c)]
            .into_iter()
            .max_by(|left, right| left.1.total_cmp(&right.1))
            .expect("three candidates")
    }

    // Unit frame: translate to the bounds midpoint, scale by the half-extent
    // (per-point `max/2 - min/2` stays finite even when the full extent
    // overflows). Original input vertices are kept aside for the support.
    let bounds = Bounds::from_points(points.iter().copied());
    let mid = Point::new_unchecked_xy(
        f64::midpoint(bounds.minx(), bounds.maxx()),
        f64::midpoint(bounds.miny(), bounds.maxy()),
    );
    let half_extent =
        (bounds.maxx() / 2.0 - bounds.minx() / 2.0).max(bounds.maxy() / 2.0 - bounds.miny() / 2.0);
    let scale = if half_extent > 0.0 { half_extent } else { 1.0 };
    let originals = points;
    let mut points = points
        .iter()
        .map(|point| Point::new_unchecked_xy((point.x - mid.x) / scale, (point.y - mid.y) / scale))
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
    let center = Point::new_unchecked_xy(mid.x + circle.0.x * scale, mid.y + circle.0.y * scale);
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

/// The pole of inaccessibility (`center`) and its nearest point on the
/// boundary linework (`witness`) — the inscribed circle's center and a point
/// on its circumference. The witness rides the same boundary index the search
/// builds, so it costs one extra `O(log segments)` descent rather than the
/// `boundary()` ring clone plus brute `O(segments)` nearest-point scan it
/// replaces (the whole fixed `maximum_inscribed_circle` tail).
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
    let nearest = Point::new_unchecked_xy(
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
    let midpoint = Point::new_unchecked_xy(
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
    let xy = shape.force_2d();
    let normalized = xy.try_map_coordseqs(
        |seq| {
            let mut builder = CoordSeqBuilder::like_coords(seq, 0);
            builder.try_reserve_exact(seq.len()).map_err(|_| {
                GeometryErrorKind::message("polylabel could not allocate normalized coordinates")
            })?;
            for point in seq.iter_coords() {
                let xy = frame.frame_xy(point.x, point.y);
                builder.push(point.with_xy(xy.x, xy.y)?);
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

pub(crate) fn polylabel_point(
    shape: &Shape,
    tolerance: Option<f64>,
) -> crate::error::Result<(Point, Point)> {
    let Some(bounds) = shape.bounds() else {
        let origin = Point::new_unchecked_xy(0.0, 0.0);
        return Ok((origin, origin));
    };
    // Degenerate input — a zero-width bounding box (a sliver or a point): the
    // representative point is the answer and its boundary projection the
    // witness. Build the original-frame field only on this rare branch.
    let Some(frame) = normalized_polylabel_frame(shape, bounds)? else {
        let original_field = PolylabelField::new(shape, 0.0, None);
        let center = representative_point(shape, bounds);
        return Ok((
            center,
            original_field.nearest_boundary(center).unwrap_or(center),
        ));
    };
    let normalized_bounds = frame
        .shape
        .bounds()
        .expect("non-empty normalized polygon retains bounds");
    let mut field = PolylabelField::new(&frame.shape, frame.max_inradius, Some(frame.frame));
    if field.is_empty() {
        let center = representative_point(shape, bounds);
        return Ok((center, center));
    }

    // Tolerance remains in public Euclidean coordinates.  The framed field
    // measures in that same metric, so an explicit tolerance is not silently
    // reinterpreted as a coordinate-space tolerance.
    let tolerance = tolerance.unwrap_or(frame.default_tolerance);

    let mut best = field.cell(
        representative_point(&frame.shape, normalized_bounds),
        0.0,
        None,
    );
    let bbox_center = Point::new_unchecked_xy(0.0, 0.0);
    let bbox_cell = field.cell(bbox_center, 0.0, None);
    if bbox_cell.distance > best.distance {
        best = bbox_cell;
    }

    let mut budget = ExpansionBudget::new("polylabel", "tolerance");
    budget.add(1)?;
    let mut cells = BinaryHeap::with_capacity(1);
    cells.push(field.cell(bbox_center, 1.0, None));

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
                    Point::new_unchecked_xy(cell.center.x + dx, cell.center.y + dy),
                    half_size,
                    Some(&cell),
                ));
            }
        }
    }

    let denormalize = |point: Point| {
        let world = frame.frame.unframe_xy(point.xy());
        Point::new_unchecked_xy(world.x, world.y)
    };
    let center = denormalize(best.center);
    let witness = field
        .nearest_boundary(best.center)
        .map_or(center, denormalize);
    Ok((center, witness))
}

pub(crate) fn representative_point(shape: &Shape, bounds: Bounds) -> Point {
    match shape.point_on_surface() {
        Ok(Shape::Point(point)) => point,
        _ => Point::new_unchecked_xy(
            bounds.minx() + (bounds.maxx() - bounds.minx()) / 2.0,
            bounds.miny() + (bounds.maxy() - bounds.miny()) / 2.0,
        ),
    }
}

/// The indexed boundary a polylabel search probes once per cell. The facet
/// BVH is built ONCE over the linework, so each cell's distance is an
/// `O(log segments)` SIMD branch-and-bound descent rather than the per-cell
/// `O(segments)` scan it replaces. Containment (the distance sign) rides a
/// direct ray cast — needed for only the seed and boundary-straddling cells,
/// far below a full hierarchical `PointBatchTester` build's payoff. The same
/// BVH then yields the inscribed circle's boundary contact point, so
/// `maximum_inscribed_circle` never rebuilds or rescans the boundary for its
/// radius.
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
    fn new(shape: &'a Shape, max_inradius: f64, frame: Option<AxisFrame>) -> Self {
        let linework = PreparedLinework::build(shape);
        // Below the indexing crossover the flat SIMD facet scan beats a tree
        // descent (its stack churn does not pay for a handful of facets).
        let bvh = (frame.is_none() && linework.segment_count() >= BVH_MIN_INDEXED_SEGMENTS)
            .then(|| FacetBvh::build(&linework))
            .flatten();
        Self {
            shape,
            linework,
            bvh,
            stack: Vec::new(),
            max_inradius,
            frame,
        }
    }

    const fn is_empty(&self) -> bool {
        self.linework.segment_count() == 0
    }

    /// The point on the boundary linework nearest `center` (the inscribed
    /// circle's contact point), full ordinates reconstructed. Reuses the
    /// search's index — one branch-and-bound descent — so the witness costs
    /// nothing like the brute scan it replaces. `None` only when there is no
    /// linework.
    fn nearest_boundary(&self, center: Point) -> Option<Point> {
        if let Some(frame) = self.frame {
            let mut nearest = None;
            let mut best = f64::INFINITY;
            self.linework.for_each_segment(|segment| {
                let (candidate, distance) =
                    framed_metric_nearest_on_segment(frame, center, segment);
                if distance < best {
                    best = distance;
                    nearest = Some(candidate);
                }
            });
            return nearest;
        }
        let candidate = self.bvh.as_ref().map_or_else(
            || {
                self.linework
                    .nearest_to_points(std::iter::once(center), None)
            },
            |bvh| bvh.nearest_to_points(&self.linework, std::iter::once(center), None),
        );
        candidate.map(|candidate| candidate.target)
    }

    /// Unsigned distance from `point` to the boundary linework. The BVH runs
    /// its SIMD **squared** kernel (eight segment distances per vector op) and
    /// the single `sqrt` lifts the winner back to Euclidean units for the
    /// priority bound `half_size·√2 + d` — far cheaper than the scalar `hypot`
    /// path, which evaluates each segment distance one lane at a time.
    ///
    /// `bound_squared` seeds the branch-and-bound with a known upper bound on
    /// the result, so the descent prunes every node farther than it from the
    /// outset (the true nearest is never pruned — it lies at or below the
    /// bound). `f64::INFINITY` means "no prior knowledge" (the seed cells).
    fn boundary_distance(&mut self, point: Point, bound_squared: f64) -> f64 {
        if let Some(frame) = self.frame {
            let mut best = f64::INFINITY;
            self.linework.for_each_segment(|segment| {
                best = best.min(framed_metric_nearest_on_segment(frame, point, segment).1);
            });
            return best;
        }
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
        squared.sqrt()
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
    ) -> PolylabelCell {
        // The child center lies within `parent.half_size·(1/√2)` of the parent
        // center (the diagonal quadrant offset), so by the triangle inequality
        // its boundary distance is at most `|parent.distance| + that gap`. Seed
        // the search with that bound (padded by a hair so float rounding never
        // prunes the true nearest facet). The same gap decides the sign for
        // free when the boundary cannot reach across it.
        let (bound_squared, inherited_sign) = parent.map_or((f64::INFINITY, None), |parent| {
            let gap = self.frame.map_or(
                parent.half_size * std::f64::consts::FRAC_1_SQRT_2,
                |frame| framed_metric_cell_radius(frame, parent.half_size / 2.0),
            );
            let bound = (parent.distance.abs() + gap) * (1.0 + 1e-9);
            let sign = (parent.distance.abs() > gap).then_some(parent.distance > 0.0);
            (bound * bound, sign)
        });
        let distance = self.boundary_distance(center, bound_squared);
        // Containment is needed only for the seed cells and the few cells that
        // straddle the boundary (deep cells inherit their parent's sign), so a
        // direct even-odd ray cast beats building a hierarchical
        // `PointBatchTester` whose O(segments·log) sort never amortizes over
        // so few probes.
        let inside = inherited_sign.unwrap_or_else(|| self.shape.contains_point(center));
        let signed = if inside { distance } else { -distance };
        let cell_radius = self
            .frame
            .map_or(half_size * std::f64::consts::SQRT_2, |frame| {
                framed_metric_cell_radius(frame, half_size)
            });
        PolylabelCell {
            center,
            half_size,
            distance: signed,
            max_distance: (cell_radius + signed).min(self.max_inradius),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Polygon, Ring};

    #[test]
    fn reciprocal_ribbon_keeps_the_physical_nearest_edge() {
        let extent = 1.0e162;
        let half_height = 1.0 / extent;
        let shape = Shape::Polygon(Polygon::new(
            Ring::from_trusted_closed(vec![
                Point::new_unchecked_xy(-extent, -half_height),
                Point::new_unchecked_xy(extent, -half_height),
                Point::new_unchecked_xy(extent, half_height),
                Point::new_unchecked_xy(-extent, half_height),
                Point::new_unchecked_xy(-extent, -half_height),
            ]),
            Vec::new(),
        ));
        let (center, witness) = polylabel_point(&shape, None).expect("finite ribbon");
        assert_eq!(center, Point::new_unchecked_xy(0.0, 0.0));
        assert_eq!(center.y.hypot(witness.y).to_bits(), half_height.to_bits());
        assert_eq!(
            center.x.to_bits(),
            witness.x.to_bits(),
            "nearest edge is horizontal"
        );
    }
}
