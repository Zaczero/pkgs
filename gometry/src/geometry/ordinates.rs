#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Z/M ordinate carry — rebuilding Z/M on XY-only geometry-engine output from
//! the operation's input vertices and segments (`carry_ordinates`/`carry_each`
//! and the `OrdinateSource` lookup), shared by overlay and decomposition.

use rstar::{AABB, RTree, RTreeObject};

use super::*;
use crate::collections::{HashMap, HashMapExt};
use crate::error::Result;

/// Z/M lookup table built from an operation's input vertices and segments,
/// used to restore ordinates on XY-only geometry-engine output. Exact
/// vertex matches — the overwhelmingly common case (engine output keeps
/// input vertices) — resolve through a hash map keyed like [`same_point`]
/// (bit-exact with ±0.0 unified), so restoring N output vertices is O(N)
/// instead of O(N x input size); only genuinely new vertices pay the
/// segment-interpolation scan.
pub(super) struct OrdinateSource {
    vertices: HashMap<PointKey, Point>,
    segments: Vec<(Point, Point)>,
    /// Lazy R-tree over `segments` for the miss path: engine-jittered
    /// output vertices miss the exact-match map and would otherwise scan
    /// every Z/M source segment per vertex (measured: 34 ms of a 35 ms
    /// Z union). Built only when a miss occurs; candidates test in input
    /// order, so resolution stays identical to the linear scan.
    segment_tree: std::cell::OnceCell<RTree<SegmentEnvelope>>,
    /// Scale-aware slack for the segment stage: the boolean engine snaps
    /// coordinates to a grid proportional to the input EXTENT, and the
    /// crossing of two snapped segments lands up to ~extent x 2^-29 from
    /// the true intersection (measured: 1.9e-8 at extent 12) — an
    /// ABSOLUTE error the per-segment relative epsilon cannot cover on
    /// short segments. `extent x 2^-28` clears it with 2x margin while
    /// staying far below any real feature separation (~0.15 mm at
    /// geographic-degree scale).
    tolerance: f64,
}

/// The engine-snap allowance relative to the inputs' coordinate magnitude.
const OVERLAY_SNAP_RELATIVE: f64 = 1.0 / (1_u64 << 28) as f64;

/// One source segment's acceptance envelope (bbox padded by its own
/// relative on-segment epsilon) plus its input position for order-stable
/// candidate testing.
struct SegmentEnvelope {
    index: usize,
    envelope: AABB<[f64; 2]>,
}

impl RTreeObject for SegmentEnvelope {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

impl OrdinateSource {
    fn build(inputs: &[&Shape]) -> Self {
        // Upper bound: every input vertex carries ordinates (the common
        // case — XYZ/XYM inputs are usually uniformly dimensioned).
        let total_coords: usize = inputs.iter().map(|shape| shape.coord_count()).sum();
        let mut vertices = HashMap::with_capacity(total_coords);
        let mut segments = Vec::with_capacity(total_coords);
        let mut magnitude: f64 = 0.0;
        for shape in inputs {
            if let Some(bounds) = shape.bounds() {
                magnitude = magnitude
                    .max(bounds.minx().abs())
                    .max(bounds.maxx().abs())
                    .max(bounds.miny().abs())
                    .max(bounds.maxy().abs());
            }
            shape.for_each_point(|point| {
                if point.z().is_some() || point.m().is_some() {
                    // First occurrence wins, like the scan it replaces.
                    vertices.entry(PointKey::new(point)).or_insert(point);
                }
            });
            shape.for_each_vertex_pair(|start, end| {
                if start.z().is_some()
                    || start.m().is_some()
                    || end.z().is_some()
                    || end.m().is_some()
                {
                    segments.push((start, end));
                }
            });
        }
        Self {
            vertices,
            segments,
            segment_tree: std::cell::OnceCell::new(),
            tolerance: magnitude * OVERLAY_SNAP_RELATIVE,
        }
    }

    fn segment_tree(&self) -> &RTree<SegmentEnvelope> {
        self.segment_tree.get_or_init(|| {
            RTree::bulk_load(
                self.segments
                    .iter()
                    .enumerate()
                    .map(|(index, &(start, end))| {
                        let pad = ON_SEGMENT_EPSILON * point_distance(start, end);
                        SegmentEnvelope {
                            index,
                            envelope: AABB::from_corners(
                                [start.x.min(end.x) - pad, start.y.min(end.y) - pad],
                                [start.x.max(end.x) + pad, start.y.max(end.y) + pad],
                            ),
                        }
                    })
                    .collect(),
            )
        })
    }

    /// Resolve Z/M for one XY output point; `None` if it derives from no input
    /// vertex or segment (the caller then applies its operation semantics).
    fn resolve(&self, point: Point) -> Option<Point> {
        if let Some(vertex) = self.vertices.get(&PointKey::new(point)) {
            return vertex.with_xy(point.x, point.y).ok();
        }
        if self.segments.is_empty() {
            return None;
        }
        let pad = self.tolerance;
        let query = AABB::from_corners([point.x - pad, point.y - pad], [
            point.x + pad,
            point.y + pad,
        ]);
        // Deterministic winner = the LOWEST-index resolving segment, found
        // by streaming the (tiny) candidate set — no per-miss Vec + sort.
        let mut winner = usize::MAX;
        let mut ordinates = None;
        for entry in self.segment_tree().locate_in_envelope_intersecting(query) {
            if entry.index > winner {
                continue;
            }
            let (start, end) = self.segments[entry.index];
            if let Some(resolved) = segment_ordinate_at(start, end, point, self.tolerance) {
                (winner, ordinates) = (entry.index, Some(resolved));
            }
        }
        let (z, m) = ordinates?;
        Point::new_axes(point.x, point.y, ZOrdinate(z), MOrdinate(m)).ok()
    }
}

/// Interpolate Z/M for `point` if it lies on the segment `start`..`end`,
/// restoring Z/M relative to the segment length.
pub(super) fn segment_ordinate_at(
    start: Point,
    end: Point,
    point: Point,
    tolerance: f64,
) -> Option<(Option<f64>, Option<f64>)> {
    let dx = end.x - start.x;
    let dy = end.y - start.y;
    // Plain ops: scalar `mul_add` is a libm call below x86-64-v3.
    let squared_length = dx * dx + dy * dy;
    if squared_length == 0.0 {
        return same_point(start, point).then_some((start.z(), start.m()));
    }
    // The perpendicular slack combines the per-segment relative epsilon
    // with the caller's ABSOLUTE engine-snap tolerance: short segments in
    // a large extent would otherwise reject the engine's jittered copies
    // of their own vertices.
    let length = squared_length.sqrt();
    let allowed = (ON_SEGMENT_EPSILON * length).max(tolerance);
    if (cross_magnitude(dx, dy, start, point) / length) > allowed {
        return None;
    }
    let fraction = ((point.x - start.x) * dx + (point.y - start.y) * dy) / squared_length;
    let slack = ON_SEGMENT_EPSILON.max(tolerance / length);
    if !(-slack..=1.0 + slack).contains(&fraction) {
        return None;
    }
    let interpolated = lerp_point(start, end, fraction.clamp(0.0, 1.0));
    Some((interpolated.z(), interpolated.m()))
}

fn cross_magnitude(dx: f64, dy: f64, start: Point, point: Point) -> f64 {
    // Plain ops: compared against a slack floored at ON_SEGMENT_EPSILON, so
    // fused rounding buys nothing and scalar `mul_add` is a libm call below
    // x86-64-v3.
    (dx * (point.y - start.y) - dy * (point.x - start.x)).abs()
}

/// Carry Z/M from an operation's inputs onto a single XY output shape. In
/// strict internal verification, an unsourceable vertex raises; ordinary
/// constructive operations naturally degrade the result to XY.
pub(super) fn carry_ordinates(
    output: Shape,
    inputs: &[&Shape],
    operation: &str,
    strict: bool,
) -> Result<Shape> {
    // Pure-XY inputs: nothing to carry, the XY output is already final.
    if !inputs.iter().any(|shape| shape.has_z() || shape.has_m()) {
        return Ok(output);
    }
    let source = OrdinateSource::build(inputs);
    carry_or_degrade(&output, &source, operation, strict)
}

/// Carry Z/M onto each shape in a decomposition result (see
/// [`carry_ordinates`] for the strict/degrading semantics).
pub(super) fn carry_each(
    outputs: Vec<Shape>,
    inputs: &[&Shape],
    operation: &str,
    strict: bool,
) -> Result<Vec<Shape>> {
    if !inputs.iter().any(|shape| shape.has_z() || shape.has_m()) {
        return Ok(outputs);
    }
    let source = OrdinateSource::build(inputs);
    outputs
        .iter()
        .map(|output| carry_or_degrade(output, &source, operation, strict))
        .collect()
}

/// Source every vertex's Z/M from `source`; on the first unsourceable vertex
/// either raise in strict internal verification or return the shape flattened
/// to XY. A fully-sourceable shape carries its Z/M through.
fn carry_or_degrade(
    output: &Shape,
    source: &OrdinateSource,
    operation: &str,
    strict: bool,
) -> Result<Shape> {
    match output.map_points(&|point| {
        source
            .resolve(point)
            .ok_or_else(|| GeometryErrorKind::ordinate_dropped(operation))
    }) {
        Ok(carried) => Ok(carried),
        Err(_) if !strict => Ok(output.force_2d()),
        Err(error) => Err(error),
    }
}
