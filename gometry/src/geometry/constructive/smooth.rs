use crate::geometry::constructive::{Arc, Result};
use crate::geometry::{
    CoordSeq, CoordSeqBuilder, ExpansionBudget, GENERATED_ITEM_LIMIT, GeometryErrorKind, LineSeq,
    MOrdinate, Point, Polygon, Ring, Shape, SmoothMethod, ZOrdinate, interpolate_f64, lerp_point,
    point_distance,
};

/// Maximum coordinate count one `smooth` call may materialize across all of its
/// output parts. Chaikin roughly doubles the vertex count every iteration and
/// centripetal Catmull-Rom emits `2**iterations` samples per source segment, so
/// a tiny valid input paired with a large-but-valid `iterations` would otherwise
/// grow without bound — or overflow the `edge_count * samples + 1` capacity
/// arithmetic — and exhaust memory. 16 million coordinates is generous for
/// legitimate smoothing yet rejects the pathological blow-up before any
/// allocation, mirroring the grid coverers' cell-output budget.
pub(crate) const SMOOTH_MAX_COORDS: usize = GENERATED_ITEM_LIMIT;

/// The domain error raised when a projected `smooth` output would exceed
/// [`SMOOTH_MAX_COORDS`]. Names the driving parameter (`iterations`) and the
/// computed size — a parameter-value violation, so it maps to Python
/// `GeometryError`.
fn smooth_budget_error(produced: usize) -> crate::error::Error {
    GeometryErrorKind::GeneratedOutputTooLarge {
        operation: "smooth",
        parameter: "iterations",
        produced,
        limit: SMOOTH_MAX_COORDS,
    }
    .into()
}

/// Output coordinate count for one Chaikin iteration of a chain of `count`
/// vertices: both open forms produce `2*(count-1)`, the closed form
/// `2*(count-1)+1`; below the minimum vertex count the pass is identity.
/// `None` signals `usize` overflow (a blow-up far past any budget).
const fn chaikin_next_len<const CLOSED: bool>(count: usize) -> Option<usize> {
    if CLOSED {
        if count < 4 {
            return Some(count);
        }
        match (count - 1).checked_mul(2) {
            Some(doubled) => doubled.checked_add(1),
            None => None,
        }
    } else {
        if count < 3 {
            return Some(count);
        }
        (count - 1).checked_mul(2)
    }
}

/// Projected output coordinate count for one line/ring, computed with checked
/// arithmetic so the exponential blow-up is rejected BEFORE any allocation.
/// Fails with [`smooth_budget_error`] when the part alone exceeds the budget.
fn part_output_len<const CLOSED: bool>(
    count: usize,
    iterations: i32,
    method: SmoothMethod,
) -> Result<usize> {
    // Identity cases (mirror `smooth_coord_seq`): pass-through, no growth.
    if iterations <= 0 || count < 2 || (!CLOSED && count < 3) {
        return Ok(count);
    }
    match method {
        SmoothMethod::Chaikin => {
            let mut current = count;
            for _ in 0..iterations {
                current = match chaikin_next_len::<CLOSED>(current) {
                    Some(len) if len <= SMOOTH_MAX_COORDS => len,
                    Some(len) => return Err(smooth_budget_error(len)),
                    None => return Err(smooth_budget_error(usize::MAX)),
                };
            }
            Ok(current)
        },
        SmoothMethod::CatmullRom => {
            let samples = catmull_rom_samples_per_segment(iterations);
            match (count - 1)
                .checked_mul(samples)
                .and_then(|v| v.checked_add(1))
            {
                Some(len) if len <= SMOOTH_MAX_COORDS => Ok(len),
                Some(len) => Err(smooth_budget_error(len)),
                None => Err(smooth_budget_error(usize::MAX)),
            }
        },
    }
}

/// Exact-capacity SoA builder for one smoothed part. The budget check already
/// caps `cap` at [`SMOOTH_MAX_COORDS`]; `try_reserve_exact` only fails on a
/// genuine out-of-memory condition at the (bounded) final allocation.
fn reserve_builder(coords: &CoordSeq, cap: usize) -> Result<CoordSeqBuilder> {
    let mut builder = CoordSeqBuilder::like_coords(coords, 0);
    // Grow each live column once — same OOM contract as the prior `Vec<Point>`
    // path, without the AoS staging buffer or the SoA transpose.
    builder.try_reserve_exact(cap).map_err(|_| {
        GeometryErrorKind::message(format!(
            "smooth could not allocate {cap} output coordinates"
        ))
    })?;
    Ok(builder)
}

/// Corner-cutting Chaikin smoothing and centripetal Catmull-Rom subdivision
/// for line/polygon boundary chains. Points, empties, and chains with fewer
/// than two vertices pass through unchanged; Z/M interpolate linearly along
/// each source edge (Catmull-Rom XY follows the spline, Z/M stay edge-linear).
///
/// This same per-part output path charges the caller-owned budget before its
/// exact-capacity emitter allocates, so rows, children, and rings share one
/// counter without a separate estimating traversal.
pub(crate) fn smooth_coord_seq<const CLOSED: bool>(
    points: &CoordSeq,
    iterations: i32,
    method: SmoothMethod,
    keep_endpoints: bool,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    if iterations <= 0 || points.len() < 2 || (!CLOSED && points.len() < 3) {
        return Ok(points.clone());
    }
    let output_len = part_output_len::<CLOSED>(points.len(), iterations, method)?;
    budget.add(output_len.saturating_sub(points.len()))?;
    Ok(match method {
        SmoothMethod::Chaikin => {
            let mut current = points.clone();
            for _ in 0..iterations {
                current = if CLOSED {
                    chaikin_closed(&current)?
                } else {
                    chaikin_open(&current, keep_endpoints)?
                };
            }
            current
        },
        SmoothMethod::CatmullRom => {
            let samples = catmull_rom_samples_per_segment(iterations);
            catmull_rom_subdivide::<CLOSED>(points, samples)?
        },
    })
}

fn chaikin_open(points: &CoordSeq, keep_endpoints: bool) -> Result<CoordSeq> {
    let count = points.len();
    if count < 2 {
        return Ok(points.clone());
    }
    if !keep_endpoints {
        let mut out = reserve_builder(points, 2 * (count - 1))?;
        for index in 0..count - 1 {
            let start = points.point_at(index);
            let end = points.point_at(index + 1);
            out.push(lerp_point(start, end, 0.25));
            out.push(lerp_point(start, end, 0.75));
        }
        return Ok(out.finish_infallible());
    }
    let mut out = reserve_builder(points, 2 * count - 2)?;
    out.push_at(points, 0);
    for index in 0..count - 1 {
        let start = points.point_at(index);
        let end = points.point_at(index + 1);
        if index == 0 {
            out.push(lerp_point(start, end, 0.75));
        } else if index == count - 2 {
            out.push(lerp_point(start, end, 0.25));
        } else {
            out.push(lerp_point(start, end, 0.25));
            out.push(lerp_point(start, end, 0.75));
        }
    }
    out.push_at(points, count - 1);
    Ok(out.finish_infallible())
}

fn chaikin_closed(points: &CoordSeq) -> Result<CoordSeq> {
    let count = points.len();
    if count < 4 {
        return Ok(points.clone());
    }
    let edge_count = count - 1;
    let mut out = reserve_builder(points, 2 * edge_count + 1)?;
    // Capture the first emitted corner so the ring can close bit-identically
    // without reading back from the SoA builder.
    let mut first_corner = None;
    for index in 0..edge_count {
        let start = points.point_at(index);
        let end = points.point_at(index + 1);
        let q = lerp_point(start, end, 0.25);
        let r = lerp_point(start, end, 0.75);
        if index == 0 {
            first_corner = Some(q);
        }
        out.push(q);
        out.push(r);
    }
    out.push(first_corner.expect("closed Chaikin has at least one edge"));
    Ok(out.finish_infallible())
}

/// Subdivide every segment into ``samples`` equal parametric pieces (keeping
/// every original vertex). ``samples=1`` is identity.
pub(crate) fn catmull_rom_subdivide<const CLOSED: bool>(
    points: &CoordSeq,
    samples: usize,
) -> Result<CoordSeq> {
    let count = points.len();
    if count < 2 || samples <= 1 {
        return Ok(points.clone());
    }
    let edge_count = count - 1;
    let total = edge_count * samples + 1;
    let mut out = reserve_builder(points, total)?;
    out.push_at(points, 0);
    let first = points.point_at(0);
    for edge in 0..edge_count {
        let p0 = catmull_control::<CLOSED>(points, edge as isize - 1);
        let p1 = points.point_at(edge);
        let p2 = points.point_at(edge + 1);
        let p3 = catmull_control::<CLOSED>(points, edge as isize + 2);
        // Knot increments are invariant for the edge — prepare once, then
        // evaluate every sample fraction from the cached t0..t3 (was 8 sqrts
        // per interior sample).
        let spline = PreparedCatmullRom::new(p0, p1, p2, p3);
        let z0 = p1.z();
        let z1 = p2.z();
        let m0 = p1.m();
        let m1 = p2.m();
        for step in 1..samples {
            let fraction = step as f64 / samples as f64;
            let mut point = spline.evaluate(fraction);
            point = Point::new_unchecked_axes(
                point.x,
                point.y,
                ZOrdinate(match (z0, z1) {
                    (Some(z0), Some(z1)) => Some(interpolate_f64(z0, z1, fraction)),
                    _ => None,
                }),
                MOrdinate(match (m0, m1) {
                    (Some(m0), Some(m1)) => Some(interpolate_f64(m0, m1, fraction)),
                    _ => None,
                }),
            );
            out.push(point);
        }
        if !CLOSED || edge < edge_count - 1 {
            out.push_at(points, edge + 1);
        }
    }
    if CLOSED {
        out.push(first);
    }
    Ok(out.finish_infallible())
}

fn catmull_control<const CLOSED: bool>(points: &CoordSeq, index: isize) -> Point {
    let count = points.len();
    let edge_count = count - 1;
    if CLOSED {
        let wrapped = index.rem_euclid(edge_count as isize) as usize;
        points.point_at(wrapped)
    } else if index < 0 {
        reflect_endpoint(points.point_at(0), points.point_at(1))
    } else if index >= count as isize {
        reflect_endpoint(points.point_at(count - 1), points.point_at(count - 2))
    } else {
        points.point_at(index as usize)
    }
}

fn reflect_endpoint(end: Point, interior: Point) -> Point {
    // `2*end - interior` overflows for extreme-but-finite endpoints; use the
    // equivalent `end + (end - interior)` with a finite fallback to `end`.
    let reflect = |e: f64, i: f64| {
        let delta = e - i;
        let out = e + delta;
        if out.is_finite() { out } else { e }
    };
    Point::new_unchecked_axes(
        reflect(end.x, interior.x),
        reflect(end.y, interior.y),
        ZOrdinate(match (end.z(), interior.z()) {
            (Some(ez), Some(iz)) => Some(reflect(ez, iz)),
            _ => None,
        }),
        MOrdinate(match (end.m(), interior.m()) {
            (Some(em), Some(im)) => Some(reflect(em, im)),
            _ => None,
        }),
    )
}

fn knot_increment(a: Point, b: Point) -> f64 {
    // Centripetal α=0.5: τ = |Δ|^0.5. Squared-distance then two sqrts
    // overflows for extreme-but-finite coords; hypot-style distance then one
    // sqrt stays finite whenever their coordinate delta is representable.
    // Duplicate / nonfinite spans retain the historical fallback. The tiny
    // non-collinear case has a finite distance; preserving this branch keeps
    // duplicate-point curves unchanged.
    let dist = point_distance(a.xy(), b.xy());
    if dist == 0.0 || !dist.is_finite() {
        1e-4
    } else {
        dist.sqrt()
    }
}

/// Per-edge centripetal Catmull–Rom state: control points + knot parameters.
/// Knot increments are independent of the sample fraction, so they are built
/// once per source edge and every output sample reuses them.
struct PreparedCatmullRom {
    p0: Point,
    p1: Point,
    p2: Point,
    p3: Point,
    t0: f64,
    t1: f64,
    t2: f64,
    t3: f64,
}

impl PreparedCatmullRom {
    fn new(p0: Point, p1: Point, p2: Point, p3: Point) -> Self {
        let t0 = 0.0;
        let t1 = knot_increment(p0, p1);
        let t2 = t1 + knot_increment(p1, p2);
        let t3 = t2 + knot_increment(p2, p3);
        Self {
            p0,
            p1,
            p2,
            p3,
            t0,
            t1,
            t2,
            t3,
        }
    }

    fn evaluate(&self, fraction: f64) -> Point {
        let t = interpolate_f64(self.t1, self.t2, fraction);
        let a1 = lerp_knot(self.p0, self.p1, self.t0, self.t1, t);
        let a2 = lerp_knot(self.p1, self.p2, self.t1, self.t2, t);
        let a3 = lerp_knot(self.p2, self.p3, self.t2, self.t3, t);
        let b1 = lerp_knot(a1, a2, self.t0, self.t2, t);
        let b2 = lerp_knot(a2, a3, self.t1, self.t3, t);
        let out = lerp_knot(b1, b2, self.t1, self.t2, t);
        // Finite control points must yield finite samples: fall back to the
        // chord between the segment endpoints when the knot ladder overflows.
        if out.x.is_finite() && out.y.is_finite() {
            out
        } else {
            lerp_point(self.p1, self.p2, fraction)
        }
    }
}

fn lerp_knot(a: Point, b: Point, ta: f64, tb: f64, t: f64) -> Point {
    let span = tb - ta;
    // Knot spans inherit the coordinate scale: a non-zero `1e-20` span is
    // perfectly valid for a `1e-40` curve.  Only an actual collapsed or
    // non-finite knot falls back to its left control point.
    if !span.is_finite() || span == 0.0 {
        return a;
    }
    let weight = (t - ta) / span;
    if !weight.is_finite() {
        return a;
    }
    lerp_point(a, b, weight)
}

/// Samples per source segment for Catmull-Rom: ``2**iterations`` (monotone in
/// ``iterations``; ``0`` → identity).
pub(crate) fn catmull_rom_samples_per_segment(iterations: i32) -> usize {
    if iterations <= 0 {
        return 1;
    }
    1_usize << iterations.min(30) as u32
}

impl Shape {
    pub fn smooth(
        &self,
        iterations: i32,
        method: SmoothMethod,
        keep_endpoints: bool,
    ) -> Result<Self> {
        let mut budget = ExpansionBudget::new("smooth", "iterations");
        self.smooth_budgeted(iterations, method, keep_endpoints, &mut budget)
    }

    pub(crate) fn smooth_budgeted(
        &self,
        iterations: i32,
        method: SmoothMethod,
        keep_endpoints: bool,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        Ok(match self {
            Self::Point(point) => Self::Point(*point),
            Self::MultiPoint(points) => Self::MultiPoint(points.clone()),
            Self::LineString(points) => Self::LineString(LineSeq::from_trusted(
                smooth_coord_seq::<false>(points, iterations, method, keep_endpoints, budget)?,
            )),
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines
                    .iter()
                    .map(|line| {
                        Ok(LineSeq::from_trusted(smooth_coord_seq::<false>(
                            line,
                            iterations,
                            method,
                            keep_endpoints,
                            budget,
                        )?))
                    })
                    .collect::<Result<_>>()?,
            ),
            Self::Polygon(polygon) => Self::Polygon(smooth_polygon_rings(
                polygon,
                iterations,
                method,
                keep_endpoints,
                budget,
            )?),
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons
                    .iter()
                    .map(|polygon| {
                        smooth_polygon_rings(polygon, iterations, method, keep_endpoints, budget)
                    })
                    .collect::<Result<_>>()?,
            ),
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries
                    .iter()
                    .map(|geometry| {
                        geometry.smooth_budgeted(iterations, method, keep_endpoints, budget)
                    })
                    .collect::<Result<_, _>>()?,
            ),
            Self::Empty(..) => self.clone(),
        })
    }
}

/// Smooth every ring (shell then holes) of a polygon, fallibly (the caller has
/// already validated the whole-geometry coordinate budget; only a genuine
/// allocator failure can surface here).
fn smooth_polygon_rings(
    polygon: &Polygon,
    iterations: i32,
    method: SmoothMethod,
    keep_endpoints: bool,
    budget: &mut ExpansionBudget,
) -> Result<Polygon> {
    let shell = Ring::from_trusted_closed(smooth_coord_seq::<true>(
        polygon.shell.coords(),
        iterations,
        method,
        keep_endpoints,
        budget,
    )?);
    let holes = polygon
        .holes
        .iter()
        .map(|hole| {
            Ok(Ring::from_trusted_closed(smooth_coord_seq::<true>(
                hole.coords(),
                iterations,
                method,
                keep_endpoints,
                budget,
            )?))
        })
        .collect::<Result<Arc<[Ring]>>>()?;
    Ok(Polygon { shell, holes })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{CoordSeq, LineSeq};

    fn line(n: usize) -> Shape {
        let points: Vec<Point> = (0..n)
            .map(|index| Point::new_unchecked_xy(index as f64, 0.0))
            .collect();
        Shape::LineString(LineSeq::try_new(CoordSeq::from(points)).expect("valid test line"))
    }

    #[test]
    fn part_output_len_projects_growth_and_rejects_overflow() {
        // Chaikin open: 2*(count-1). 5 vertices, 1 iteration -> 8.
        assert_eq!(
            part_output_len::<false>(5, 1, SmoothMethod::Chaikin).expect("within budget"),
            8
        );
        // Catmull: edge_count*samples+1 = 4*4+1 = 17 at iterations=2.
        assert_eq!(
            part_output_len::<false>(5, 2, SmoothMethod::CatmullRom).expect("within budget"),
            17
        );
        // Identity below the minimum vertex count / non-positive iterations.
        assert_eq!(
            part_output_len::<false>(2, 3, SmoothMethod::Chaikin).expect("identity"),
            2
        );
        // A large iterations count blows the budget and is rejected without
        // ever iterating to completion (checked arithmetic, no allocation).
        part_output_len::<false>(5, 40, SmoothMethod::Chaikin).unwrap_err();
        part_output_len::<false>(5, 40, SmoothMethod::CatmullRom).unwrap_err();
        part_output_len::<false>(5, i32::MAX, SmoothMethod::Chaikin).unwrap_err();
    }

    #[test]
    fn smooth_rejects_iterations_that_blow_past_the_coordinate_budget() {
        let src = line(5);
        for iterations in [30_i32, 31, i32::MAX] {
            for method in [SmoothMethod::Chaikin, SmoothMethod::CatmullRom] {
                let message = src
                    .smooth(iterations, method, true)
                    .expect_err("budget overflow must be rejected")
                    .to_string();
                assert!(
                    message.contains("reduce iterations"),
                    "iterations={iterations} method={method:?}: {message}"
                );
            }
        }
    }

    #[test]
    fn ordinary_smooth_stays_within_budget_and_identity_passes_through() {
        let src = line(5);
        let chaikin = src
            .smooth(2, SmoothMethod::Chaikin, true)
            .expect("ordinary smooth is within budget");
        assert!(chaikin.segment_count() > src.segment_count());
        // Non-positive iterations pass through bit-identically.
        let identity = src
            .smooth(0, SmoothMethod::Chaikin, true)
            .expect("identity smooth");
        assert_eq!(identity, src);
    }

    #[test]
    fn tiny_nonzero_knot_span_interpolates_instead_of_collapsing() {
        let left = Point::new_unchecked_xy(0.0, 0.0);
        let right = Point::new_unchecked_xy(1e-40, 2e-40);
        let span = knot_increment(left, right);
        assert!(span > 0.0 && span < f64::EPSILON);

        let midpoint = lerp_knot(left, right, 0.0, span, span / 2.0);
        assert_eq!(midpoint.x.to_bits(), (right.x / 2.0).to_bits());
        assert_eq!(midpoint.y.to_bits(), (right.y / 2.0).to_bits());
    }
}
