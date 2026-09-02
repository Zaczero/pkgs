//! Z/M ordinate carry — rebuilding Z/M on XY-only geometry-engine output from
//! the operation's input vertices and segments (`carry_ordinates`/`carry_each`
//! and the `OrdinateSource` lookup), shared by overlay and decomposition.

use rstar::{AABB, RTree, RTreeObject};

use crate::collections::{HashMap, HashMapExt as _};
use crate::error::Result;
use crate::geometry::topology::TopologyGraph;
use crate::geometry::{
    CoordinateAxes, GeometryErrorKind, HasM, HasZ, MOrdinate, ON_SEGMENT_EPSILON, OverlayOp, Point,
    PointKey, Segment, Shape, XY, ZOrdinate, axis_pow2_scale, point_distance, same_point,
    scaled_residual, segment_projection,
};

/// Z/M lookup table built from an operation's input vertices and segments,
/// used to restore ordinates on XY-only geometry-engine output. Exact vertex
/// matches — the overwhelmingly common case (engine output keeps input
/// vertices) — resolve through a hash map keyed like [`same_point`] (bit-exact
/// with ±0.0 unified), so restoring N output vertices is O(N) instead of
/// O(N x input size); only genuinely new vertices pay the segment scan.
pub(super) struct OrdinateSource {
    vertices: HashMap<PointKey, Vec<Point>>,
    segments: Vec<(Point, Point)>,
    /// Lazy R-tree over `segments` for the miss path: engine-jittered output
    /// vertices miss the exact-match map and would otherwise scan every Z/M
    /// source segment per vertex (measured: 34 ms of a 35 ms Z union).
    /// Built only when a miss occurs.
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
            if !(shape.has_z() || shape.has_m()) {
                continue;
            }
            let graph = TopologyGraph::cartesian(shape);
            for source in graph.source_points.iter() {
                add_vertex(&mut vertices, source.point);
            }
            for source in graph.source_edges.iter() {
                add_vertex(&mut vertices, source.start);
                add_vertex(&mut vertices, source.end);
                if source.start.z().is_some()
                    || source.start.m().is_some()
                    || source.end.z().is_some()
                    || source.end.m().is_some()
                {
                    segments.push(canonical_segment(source.start, source.end));
                }
            }
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

    /// Resolve each axis independently. A missing source value is not a zero,
    /// and distinct bit patterns (including signed zero) are conflicting
    /// values rather than an arbitrary first-source winner.
    fn resolve(&self, point: Point) -> (Option<f64>, Option<f64>) {
        crate::geometry::overlay_work::add(crate::geometry::OverlayWorkPhase::OrdinatePoint, 1);
        let mut z = AxisValue::default();
        let mut m = AxisValue::default();
        if let Some(candidates) = self.vertices.get(&PointKey::new(point)) {
            for &candidate in candidates {
                z.observe(candidate.z());
                m.observe(candidate.m());
            }
        }
        if self.segments.is_empty() || (z.conflict && m.conflict) {
            return (z.value(), m.value());
        }
        let pad = self.tolerance;
        let query = AABB::from_corners([point.x - pad, point.y - pad], [
            point.x + pad,
            point.y + pad,
        ]);
        for entry in self.segment_tree().locate_in_envelope_intersecting(query) {
            crate::geometry::overlay_work::add(
                crate::geometry::OverlayWorkPhase::OrdinateRefinement,
                1,
            );
            let (start, end) = self.segments[entry.index];
            if let Some(resolved) = segment_ordinate_at(start, end, point, self.tolerance) {
                // Exact endpoint candidates are already represented by the
                // vertex table. Interior source edges still contribute when a
                // separate source vertex happens to share that XY position.
                if !same_point(start, point) && !same_point(end, point) {
                    z.observe(resolved.0);
                    m.observe(resolved.1);
                }
            }
        }
        (z.value(), m.value())
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct AxisValue {
    value: Option<f64>,
    conflict: bool,
}

impl AxisValue {
    fn observe(&mut self, candidate: Option<f64>) {
        let Some(candidate) = candidate else {
            return;
        };
        match self.value {
            None => self.value = Some(candidate),
            Some(value) if value.to_bits() == candidate.to_bits() => {},
            Some(_) => self.conflict = true,
        }
    }

    fn value(self) -> Option<f64> {
        (!self.conflict).then_some(self.value).flatten()
    }
}

fn add_vertex(vertices: &mut HashMap<PointKey, Vec<Point>>, point: Point) {
    vertices
        .entry(PointKey::new(point))
        .or_default()
        .push(point);
}

fn canonical_segment(start: Point, end: Point) -> (Point, Point) {
    if PointKey::new(start) <= PointKey::new(end) {
        (start, end)
    } else {
        (end, start)
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
        if dx == 0.0 && dy == 0.0 {
            return same_point(start, point).then_some((start.z(), start.m()));
        }
        // A live reciprocal axis can have a zero squared length.  Reuse the
        // segment owner's anisotropic residual rescue, then certify the
        // projected witness in source distance space before carrying Z/M.
        let segment = Segment {
            start: start.xy(),
            end: end.xy(),
        };
        let projection = segment_projection(point, segment);
        let witness = projection.interpolate_xy(segment);
        let length = point_distance(segment.start, segment.end);
        let allowed = (ON_SEGMENT_EPSILON * length).max(tolerance);
        if point_distance(point.xy(), witness) > allowed {
            return None;
        }
        let lifted = projection.interpolate_point(start, end);
        return Some((lifted.z(), lifted.m()));
    }
    // The perpendicular slack combines the per-segment relative epsilon
    // with the caller's ABSOLUTE engine-snap tolerance: short segments in
    // a large extent would otherwise reject the engine's jittered copies
    // of their own vertices.
    let (dx, dy, px, py, length, tolerance) = if squared_length.is_finite() {
        let length = squared_length.sqrt();
        (
            dx,
            dy,
            point.x - start.x,
            point.y - start.y,
            length,
            tolerance,
        )
    } else {
        // The overlay engine returns ordinary finite XY vertices, but the
        // source segment can span opposite-sign extremes.  Frame the ORIGINAL
        // operands before every subtraction: `(x * s) - (origin * s)`, never
        // `(x - origin) * s`, whose first subtraction is already infinite.
        let max_abs = start
            .x
            .abs()
            .max(start.y.abs())
            .max(end.x.abs())
            .max(end.y.abs())
            .max(point.x.abs())
            .max(point.y.abs());
        let scale = axis_pow2_scale(max_abs);
        let dx = scaled_residual(end.x, start.x, scale);
        let dy = scaled_residual(end.y, start.y, scale);
        let px = scaled_residual(point.x, start.x, scale);
        let py = scaled_residual(point.y, start.y, scale);
        let length = point_distance(XY::new(0.0, 0.0), XY::new(dx, dy));
        (dx, dy, px, py, length, tolerance * scale)
    };
    if length == 0.0 || !length.is_finite() {
        return same_point(start, point).then_some((start.z(), start.m()));
    }
    let allowed = (ON_SEGMENT_EPSILON * length).max(tolerance);
    if (dx * py - dy * px).abs() / length > allowed {
        return None;
    }
    let denominator = (dx * dx) + (dy * dy);
    let fraction = ((px * dx) + (py * dy)) / denominator;
    let slack = ON_SEGMENT_EPSILON.max(tolerance / length);
    if !(-slack..=1.0 + slack).contains(&fraction) {
        return None;
    }
    let projection = segment_projection(point, Segment {
        start: start.xy(),
        end: end.xy(),
    });
    let lifted = projection.interpolate_point(start, end);
    Some((lifted.z(), lifted.m()))
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
    carry_or_degrade(
        &output,
        &source,
        inputs
            .iter()
            .fold(CoordinateAxes::XY, |axes, shape| axes.union(shape.axes())),
        operation,
        strict,
    )
}

/// Carry ordinates for an overlay. The declared axes of an empty result follow
/// the set operation, while non-empty vertices are admitted by provenance.
pub(crate) fn carry_overlay_ordinates(
    output: Shape,
    inputs: &[&Shape],
    operation: OverlayOp,
    strict: bool,
) -> Result<Shape> {
    if !inputs.iter().any(|shape| shape.has_z() || shape.has_m()) {
        return Ok(output);
    }
    let source = OrdinateSource::build(inputs);
    let declared_axes = crate::geometry::overlay::overlay_declared_axes(inputs, operation);
    carry_or_degrade(&output, &source, declared_axes, operation.name(), strict)
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
    let declared_axes = inputs
        .iter()
        .fold(CoordinateAxes::XY, |axes, shape| axes.union(shape.axes()));
    outputs
        .iter()
        .map(|output| carry_or_degrade(output, &source, declared_axes, operation, strict))
        .collect()
}

pub(super) fn carry_each_rows(
    outputs: Vec<Vec<Shape>>,
    inputs: &[&Shape],
    operation: &str,
    strict: bool,
) -> Result<Vec<Vec<Shape>>> {
    if !inputs.iter().any(|shape| shape.has_z() || shape.has_m()) {
        return Ok(outputs);
    }
    let source = OrdinateSource::build(inputs);
    let declared_axes = inputs
        .iter()
        .fold(CoordinateAxes::XY, |axes, shape| axes.union(shape.axes()));
    outputs
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|output| carry_or_degrade(&output, &source, declared_axes, operation, strict))
                .collect()
        })
        .collect()
}

/// Source every vertex's Z/M from `source`; strict internal verification raises
/// when a declared axis cannot be carried. Lenient output drops only axes that
/// have a conflict or an unsourced vertex.
fn carry_or_degrade(
    output: &Shape,
    source: &OrdinateSource,
    declared_axes: CoordinateAxes,
    operation: &str,
    strict: bool,
) -> Result<Shape> {
    match output {
        Shape::Empty(kind, _) => Ok(Shape::typed_empty(*kind, declared_axes)),
        Shape::GeometryCollection(geometries) => Shape::geometry_collection(
            geometries
                .iter()
                .map(|geometry| {
                    carry_or_degrade(geometry, source, declared_axes, operation, strict)
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        _ => {
            let axes = output_axes(output, source, declared_axes);
            if strict && axes != declared_axes && !output.is_empty() {
                return Err(GeometryErrorKind::ordinate_dropped(operation));
            }
            output.map_points(&|point| {
                let (z, m) = source.resolve(point);
                debug_assert!(
                    !axes.has_z() || z.is_some(),
                    "retained Z is not reproducible at {point:?}; axes={axes:?}"
                );
                debug_assert!(
                    !axes.has_m() || m.is_some(),
                    "retained M is not reproducible at {point:?}; axes={axes:?}"
                );
                Ok(Point::new_raw_axes(
                    point.x,
                    point.y,
                    ZOrdinate(axes.has_z().then(|| z.expect("retained Z is sourced"))),
                    MOrdinate(axes.has_m().then(|| m.expect("retained M is sourced"))),
                ))
            })
        },
    }
}

fn output_axes(
    output: &Shape,
    source: &OrdinateSource,
    declared_axes: CoordinateAxes,
) -> CoordinateAxes {
    let mut has_z = declared_axes.has_z();
    let mut has_m = declared_axes.has_m();
    output.for_each_point(|point| {
        let (z, m) = source.resolve(point);
        has_z &= z.is_some();
        has_m &= m.is_some();
    });
    CoordinateAxes::new(HasZ(has_z), HasM(has_m))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reciprocal_axis_segment_interpolates_live_ordinate() {
        let large = 1e162;
        let tiny = 1e-162;
        let start =
            Point::new_axes(large, 0.0, ZOrdinate(Some(2.0)), MOrdinate(Some(4.0))).unwrap();
        let end = Point::new_axes(large, tiny, ZOrdinate(Some(6.0)), MOrdinate(Some(8.0))).unwrap();
        let midpoint = Point::new_raw_xy(large, tiny / 2.0);
        assert_eq!(
            segment_ordinate_at(start, end, midpoint, 0.0),
            Some((Some(4.0), Some(6.0)))
        );
    }
}
