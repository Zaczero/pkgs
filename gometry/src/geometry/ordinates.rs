//! Z/M ordinate carry — rebuilding Z/M on XY-only geometry-engine output from
//! the operation's input vertices and segments (`carry_ordinates`/`carry_each`
//! and the `OrdinateSource` lookup), shared by overlay and decomposition.

use std::borrow::Borrow;

use crate::collections::HashMap;
use crate::error::Result;
use crate::geographic::RetainedChartDisplacement;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{
    ChartMetric, CoordinateAxes, GeometryErrorKind, HasM, HasZ, MOrdinate, ON_SEGMENT_EPSILON,
    OverlayOp, Point, PointKey, RunIdentity, Segment, Shape, XY, ZOrdinate, axis_pow2_scale,
    interpolate_optional_ordinate, overlay, point_distance, point_on_segment,
    point_segment_distance_and_projection, same_point, scaled_residual, segment_projection,
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

impl OrdinateSource {
    fn new<S: Borrow<Shape>>(inputs: &[S]) -> Result<Self> {
        // Shared XY owners bound dedup storage without expanding logical
        // occurrences. Non-collection shapes use their exact count.
        let capacity = inputs.iter().try_fold(0_usize, |total, input| {
            let shape = Borrow::<Shape>::borrow(input);
            shape
                .point_set_capacity_hint(RunIdentity::XY)
                .map(|count| total.wrapping_add(count))
        });
        let mut vertices = crate::collections::try_hash_map()?;
        let mut segments = Vec::new();
        if let Some(capacity) = capacity {
            let _vertices_reservation = vertices.try_reserve(capacity);
            let _segments_reservation = segments.try_reserve(capacity);
        }
        Ok(Self {
            vertices,
            segments,
            tolerance: ordinate_tolerance(inputs),
        })
    }

    fn add_source_edge(&mut self, start: Point, end: Point) {
        add_vertex(&mut self.vertices, start);
        add_vertex(&mut self.vertices, end);
        if start.z().is_some() || start.m().is_some() || end.z().is_some() || end.m().is_some() {
            self.segments.push(canonical_segment(start, end));
        }
    }

    fn build_direct<S: Borrow<Shape>>(inputs: &[S]) -> Result<Self> {
        let mut result = Self::new(inputs)?;
        for input in inputs {
            let shape = Borrow::<Shape>::borrow(input);
            match shape {
                Shape::Point(point) => add_vertex(&mut result.vertices, *point),
                Shape::MultiPoint(points) => {
                    for point in points.points() {
                        add_vertex(&mut result.vertices, point);
                    }
                },
                Shape::Empty(..)
                | Shape::LineString(_)
                | Shape::MultiLineString(_)
                | Shape::Polygon(_)
                | Shape::MultiPolygon(_)
                | Shape::GeometryCollection(_) => {},
            }
            if !(shape.has_z() || shape.has_m()) {
                continue;
            }
            shape.for_each_vertex_pair(|start, end| {
                if same_point(start, end) {
                    // A collapsed source edge still contributes both endpoint occurrences.
                    add_vertex(&mut result.vertices, start);
                    add_vertex(&mut result.vertices, end);
                } else {
                    result.add_source_edge(start, end);
                }
            });
        }
        Ok(result)
    }

    /// Resolve each axis independently. A missing source value is not a zero,
    /// and distinct bit patterns (including signed zero) are conflicting
    /// values rather than an arbitrary first-source winner.
    fn resolve(&self, point: Point) -> Result<(Option<f64>, Option<f64>)> {
        let mut z = AxisValue::default();
        let mut m = AxisValue::default();
        let source_vertex = self.vertices.get(&PointKey::new(point));
        if let Some(candidates) = source_vertex {
            for &candidate in candidates {
                z.observe(candidate.z());
                m.observe(candidate.m());
            }
        }
        // An exact source occurrence owns its XY even when it is merely near
        // another ordinate-carrying segment. Do not turn that near miss into
        // a fabricated interpolation through a tolerance envelope.
        if source_vertex.is_some()
            && !self
                .segments
                .iter()
                .any(|&(start, end)| point_on_segment(point.xy(), start.xy(), end.xy()))
        {
            return Ok((z.value(), m.value()));
        }
        if self.segments.is_empty() || (z.conflict && m.conflict) {
            return Ok((z.value(), m.value()));
        }
        let pad = self.tolerance;
        for &(start, end) in &self.segments {
            let segment_pad = ON_SEGMENT_EPSILON * point_distance(start, end);
            let min_x = start.x.min(end.x) - segment_pad;
            let max_x = start.x.max(end.x) + segment_pad;
            let min_y = start.y.min(end.y) - segment_pad;
            let max_y = start.y.max(end.y) + segment_pad;
            if point.x < min_x - pad
                || point.x > max_x + pad
                || point.y < min_y - pad
                || point.y > max_y + pad
            {
                continue;
            }
            if let Some(resolved) = segment_ordinate_at(start, end, point, self.tolerance)? {
                // Exact endpoint candidates are already represented by the
                // vertex table. Interior source edges still contribute when a
                // separate source vertex happens to share that XY position.
                if !same_point(start, point) && !same_point(end, point) {
                    z.observe(resolved.0);
                    m.observe(resolved.1);
                }
            }
        }
        Ok((z.value(), m.value()))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::geometry) struct AxisValue {
    value: Option<f64>,
    conflict: bool,
}

impl AxisValue {
    pub(in crate::geometry) const fn observe(&mut self, candidate: Option<f64>) {
        let Some(candidate) = candidate else {
            return;
        };
        match self.value {
            None => self.value = Some(candidate),
            Some(value) if value.to_bits() == candidate.to_bits() => {},
            Some(_) => self.conflict = true,
        }
    }

    pub(in crate::geometry) fn value(self) -> Option<f64> {
        (!self.conflict).then_some(self.value).flatten()
    }
}

#[derive(Clone, Copy, Default)]
pub(in crate::geometry) struct NodeOrdinates {
    pub(in crate::geometry) z: AxisValue,
    pub(in crate::geometry) m: AxisValue,
}

/// The source-side information needed to emit one Q node's ordinates.
pub(in crate::geometry) enum SourceArcRequest<'a> {
    NoOrdinates,
    Endpoint {
        point: Point,
        axes: CoordinateAxes,
    },
    Interior {
        source: &'a [Point; 2],
        longitude: RetainedChartDisplacement,
        chart_parameter: &'a ExactRational,
        axes: CoordinateAxes,
    },
}

pub(crate) type SourceArcMetricAccess<'a, M = crate::crs::GeodesicMetric> =
    dyn Fn() -> crate::error::Result<&'a M> + 'a;

pub(crate) type ParallelSourceArcMetricAccess<'a> =
    dyn Fn() -> crate::error::Result<&'a crate::crs::GeodesicMetric> + Sync + 'a;

impl<'a> SourceArcRequest<'a> {
    pub(in crate::geometry) fn new(
        source: &'a [Point; 2],
        axes: CoordinateAxes,
        chart_parameter: &'a ExactRational,
        longitude: impl FnOnce() -> crate::error::Result<RetainedChartDisplacement>,
    ) -> crate::error::Result<Self> {
        if !axes.has_z() && !axes.has_m() {
            return Ok(Self::NoOrdinates);
        }
        let zero = ExactRational::zero();
        let one = ExactRational::one();
        if chart_parameter.try_cmp(&zero)?.is_lt() || chart_parameter.try_cmp(&one)?.is_gt() {
            return Err(GeometryErrorKind::Invalid(
                "source chart parameter is outside the retained edge".into(),
            )
            .into());
        }
        if chart_parameter.is_zero() {
            return Ok(Self::Endpoint {
                point: source[0],
                axes,
            });
        }
        if chart_parameter.is_one() {
            return Ok(Self::Endpoint {
                point: source[1],
                axes,
            });
        }
        let eligible_axes = CoordinateAxes::new(
            HasZ(axes.has_z() && source[0].z().is_some() && source[1].z().is_some()),
            HasM(axes.has_m() && source[0].m().is_some() && source[1].m().is_some()),
        );
        if !eligible_axes.has_z() && !eligible_axes.has_m() {
            return Ok(Self::NoOrdinates);
        }
        Ok(Self::Interior {
            source,
            longitude: longitude()?,
            chart_parameter,
            axes: eligible_axes,
        })
    }
}

/// Resolve one source vertex's available axes from partial/total chart-edge
/// arclength. The metric accessor runs only for an interior request with at
/// least one eligible source axis.
pub(in crate::geometry) fn source_arc_ordinates<'m, M: ChartMetric + 'm>(
    request: &SourceArcRequest<'_>,
    metric: impl FnOnce() -> crate::error::Result<&'m M>,
) -> crate::error::Result<NodeOrdinates> {
    match request {
        SourceArcRequest::NoOrdinates => Ok(NodeOrdinates::default()),
        SourceArcRequest::Endpoint { point, axes } => Ok(node_ordinates_at(*point, *axes)),
        SourceArcRequest::Interior {
            source,
            longitude,
            chart_parameter,
            axes,
        } => {
            let metric = metric()?;
            let parameter = chart_parameter.round()?;
            let [start, end] = **source;
            let edge = metric.chart_edge_from_retained(start, end, *longitude)?;
            let fraction = metric.chart_edge_arclength_fraction(&edge, parameter, None)?;
            let (z, m) = source_ordinate_values_at_fraction(start, end, *axes, fraction);
            let mut ordinates = NodeOrdinates::default();
            ordinates.z.observe(z.0);
            ordinates.m.observe(m.0);
            Ok(ordinates)
        },
    }
}

fn node_ordinates_at(point: Point, axes: CoordinateAxes) -> NodeOrdinates {
    let mut ordinates = NodeOrdinates::default();
    if axes.has_z() {
        ordinates.z.observe(point.z());
    }
    if axes.has_m() {
        ordinates.m.observe(point.m());
    }
    ordinates
}

pub(crate) fn source_ordinate_values_at_fraction(
    start: Point,
    end: Point,
    axes: CoordinateAxes,
    fraction: f64,
) -> (ZOrdinate, MOrdinate) {
    let z = if axes.has_z() {
        interpolate_optional_ordinate(start.z(), end.z(), fraction)
    } else {
        None
    };
    let m = if axes.has_m() {
        interpolate_optional_ordinate(start.m(), end.m(), fraction)
    } else {
        None
    };
    (ZOrdinate(z), MOrdinate(m))
}

fn add_vertex(vertices: &mut HashMap<PointKey, Vec<Point>>, point: Point) {
    let key = PointKey::new(point);
    let values = vertices.entry(key).or_default();
    values.push(point);
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
) -> crate::error::Result<Option<(Option<f64>, Option<f64>)>> {
    let dx = end.x - start.x;
    let dy = end.y - start.y;
    // Plain ops: scalar `mul_add` is a libm call below x86-64-v3.
    let squared_length = dx * dx + dy * dy;
    if squared_length == 0.0 {
        if dx == 0.0 && dy == 0.0 {
            return Ok(same_point(start, point).then_some((start.z(), start.m())));
        }
        // A live reciprocal axis can have a zero squared length. Reuse the
        // segment owner's source distance before carrying Z/M.
        let segment = Segment {
            start: start.xy(),
            end: end.xy(),
        };
        let (distance, projection) = point_segment_distance_and_projection(point.xy(), segment)?;
        let length = point_distance(segment.start, segment.end);
        let allowed = (ON_SEGMENT_EPSILON * length).max(tolerance);
        if distance > allowed {
            return Ok(None);
        }
        let lifted = projection.interpolate_point(start, end)?;
        return Ok(Some((lifted.z(), lifted.m())));
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
        return Ok(same_point(start, point).then_some((start.z(), start.m())));
    }
    let allowed = (ON_SEGMENT_EPSILON * length).max(tolerance);
    if (dx * py - dy * px).abs() / length > allowed {
        return Ok(None);
    }
    let denominator = (dx * dx) + (dy * dy);
    let fraction = ((px * dx) + (py * dy)) / denominator;
    let slack = ON_SEGMENT_EPSILON.max(tolerance / length);
    if !(-slack..=1.0 + slack).contains(&fraction) {
        return Ok(None);
    }
    let projection = segment_projection(point, Segment {
        start: start.xy(),
        end: end.xy(),
    })?;
    let lifted = projection.interpolate_point(start, end)?;
    Ok(Some((lifted.z(), lifted.m())))
}

fn ordinate_tolerance<S: Borrow<Shape>>(inputs: &[S]) -> f64 {
    let mut magnitude: f64 = 0.0;
    for input in inputs {
        let shape = Borrow::<Shape>::borrow(input);
        if let Some(bounds) = shape.bounds() {
            magnitude = magnitude
                .max(bounds.minx().abs())
                .max(bounds.maxx().abs())
                .max(bounds.miny().abs())
                .max(bounds.maxy().abs());
        }
    }
    magnitude * OVERLAY_SNAP_RELATIVE
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
    let source = OrdinateSource::build_direct(inputs)?;
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
    let source = OrdinateSource::build_direct(inputs)?;
    let declared_axes = overlay::overlay_declared_axes(inputs, operation);
    carry_or_degrade(&output, &source, declared_axes, operation.name(), strict)
}

/// Carry Z/M onto each shape in a decomposition result (see
/// [`carry_ordinates`] for the strict/degrading semantics).
pub(super) fn carry_each<S: Borrow<Shape>>(
    outputs: Vec<Shape>,
    inputs: &[S],
    operation: &str,
    strict: bool,
) -> Result<Vec<Shape>> {
    if !inputs.iter().any(|shape| {
        let shape = Borrow::<Shape>::borrow(shape);
        shape.has_z() || shape.has_m()
    }) {
        return Ok(outputs);
    }
    let source = OrdinateSource::build_direct(inputs)?;
    let declared_axes = inputs.iter().fold(CoordinateAxes::XY, |axes, shape| {
        axes.union(Borrow::<Shape>::borrow(shape).axes())
    });
    let mut carried = Vec::with_capacity(outputs.len());
    for output in &outputs {
        carried.push(carry_or_degrade(
            output,
            &source,
            declared_axes,
            operation,
            strict,
        )?);
    }
    Ok(carried)
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
        Shape::GeometryCollection(geometries) => {
            Ok(Shape::GeometryCollection(geometries.try_map_members(
                |geometry| carry_or_degrade(geometry, source, declared_axes, operation, strict),
            )?))
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_) => {
            let axes = output_axes(output, source, declared_axes)?;
            if strict && axes != declared_axes && !output.is_empty() {
                return Err(GeometryErrorKind::ordinate_dropped(operation));
            }
            output.map_points(&|point| {
                let (z, m) = source.resolve(point)?;
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
) -> Result<CoordinateAxes> {
    let mut has_z = declared_axes.has_z();
    let mut has_m = declared_axes.has_m();
    output.try_for_each_point(|point| {
        let (z, m) = source.resolve(point)?;
        has_z &= z.is_some();
        has_m &= m.is_some();
        Ok::<_, crate::error::Error>(())
    })?;
    Ok(CoordinateAxes::new(HasZ(has_z), HasM(has_m)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retained_source_longitude(
        frame: crate::geographic::GeographicFrame,
        source: &[Point; 2],
    ) -> RetainedChartDisplacement {
        frame
            .longitude()
            .chart_connection(source[0].x, source[1].x)
            .expect("source test endpoints are admitted")
            .retained()
    }

    #[test]
    fn reciprocal_axis_segment_interpolates_live_ordinate() {
        let large = 1e162;
        let tiny = 1e-162;
        let start =
            Point::new_axes(large, 0.0, ZOrdinate(Some(2.0)), MOrdinate(Some(4.0))).unwrap();
        let end = Point::new_axes(large, tiny, ZOrdinate(Some(6.0)), MOrdinate(Some(8.0))).unwrap();
        let midpoint = Point::new_raw_xy(large, tiny / 2.0);
        assert_eq!(
            segment_ordinate_at(start, end, midpoint, 0.0).unwrap(),
            Some((Some(4.0), Some(6.0)))
        );
    }

    #[test]
    fn assigned_xy_uses_the_original_arc_request_on_an_actual_non_wgs84_ellipsoid() {
        const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
        let state = crate::crs::runtime_state_snapshot().unwrap();
        crate::crs::with_ellipsoid_metric_at(CRS, &[], &state, |metric| {
            assert_eq!(metric.ellipsoid_parameters(), (7_000_000.0, 1.0 / 150.0));
            let frame = metric.chart_frame();
            let source = [
                Point::new_axes(0.0, -30.0, ZOrdinate(Some(7.0)), MOrdinate(Some(101.0)))?,
                Point::new_axes(0.0, 70.0, ZOrdinate(Some(47.0)), MOrdinate(Some(701.0)))?,
            ];
            let range_start = ExactRational::from_f64(0.125);
            let range_end = ExactRational::from_f64(0.375);
            let local_parameter = ExactRational::from_f64(0.5);
            let range_delta = range_end.try_subtract(&range_start)?;
            let parameter = range_start.try_add(&range_delta.try_product(&local_parameter)?)?;
            assert_eq!(parameter, ExactRational::from_f64(0.25));
            let mut longitude_accesses = 0;
            let request = SourceArcRequest::new(&source, CoordinateAxes::XYZM, &parameter, || {
                longitude_accesses += 1;
                Ok(retained_source_longitude(frame, &source))
            })?;
            let mut metric_accesses = 0;
            let ordinates = source_arc_ordinates(&request, || {
                metric_accesses += 1;
                Ok(metric)
            })?;

            assert_eq!(longitude_accesses, 1);
            assert_eq!(metric_accesses, 1);
            assert!((ordinates.z.value().unwrap() - 16.963_739_317_749_453).abs() <= 2.0e-13);
            assert!((ordinates.m.value().unwrap() - 250.456_089_766_241_8).abs() <= 2.0e-12);
            let assigned = Point::new_axes(
                12.25,
                -4.75,
                ZOrdinate(ordinates.z.value()),
                MOrdinate(ordinates.m.value()),
            )?;
            assert_eq!((assigned.x, assigned.y), (12.25, -4.75));
            assert!((assigned.m().unwrap() - 250.456_089_766_241_8).abs() <= 2.0e-12);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn source_endpoint_records_do_not_acquire_an_interior_metric() {
        const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
        let state = crate::crs::runtime_state_snapshot().unwrap();
        crate::crs::with_ellipsoid_metric_at(CRS, &[], &state, |metric| {
            let frame = metric.chart_frame();
            let source = [
                Point::new_axes(0.0, -30.0, ZOrdinate(Some(-0.0)), MOrdinate(Some(101.0)))?,
                Point::new_axes(0.0, 70.0, ZOrdinate(None), MOrdinate(Some(701.0)))?,
            ];
            let parameter = ExactRational::zero();
            let mut longitude_accesses = 0;
            let request = SourceArcRequest::new(&source, CoordinateAxes::XYZM, &parameter, || {
                longitude_accesses += 1;
                Ok(retained_source_longitude(frame, &source))
            })?;
            let mut metric_accesses = 0;
            let ordinates = source_arc_ordinates(&request, || {
                metric_accesses += 1;
                Ok(metric)
            })?;

            assert_eq!(longitude_accesses, 0);
            assert_eq!(metric_accesses, 0);
            assert_eq!(ordinates.z.value().unwrap().to_bits(), (-0.0_f64).to_bits());
            assert_eq!(ordinates.m.value().unwrap().to_bits(), 101.0_f64.to_bits());

            let end_parameter = ExactRational::one();
            let end_request =
                SourceArcRequest::new(&source, CoordinateAxes::XYZM, &end_parameter, || {
                    longitude_accesses += 1;
                    Ok(retained_source_longitude(frame, &source))
                })?;
            let end_ordinates = source_arc_ordinates(&end_request, || {
                metric_accesses += 1;
                Ok(metric)
            })?;
            assert_eq!(metric_accesses, 0);
            assert_eq!(longitude_accesses, 0);
            assert_eq!(end_ordinates.z.value(), None);
            assert_eq!(end_ordinates.m.value().unwrap(), 701.0);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn presentation_and_axis_ineligible_source_requests_do_not_acquire_a_metric() {
        const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
        let state = crate::crs::runtime_state_snapshot().unwrap();
        crate::crs::with_ellipsoid_metric_at(CRS, &[], &state, |metric| {
            let frame = metric.chart_frame();
            let source = [
                Point::new_axes(0.0, -30.0, ZOrdinate(Some(7.0)), MOrdinate(None))?,
                Point::new_axes(0.0, 70.0, ZOrdinate(None), MOrdinate(None))?,
            ];
            let parameter = ExactRational::from_f64(0.25);
            let mut longitude_accesses = 0;
            let unavailable_request =
                SourceArcRequest::new(&source, CoordinateAxes::XYZM, &parameter, || {
                    longitude_accesses += 1;
                    Ok(retained_source_longitude(frame, &source))
                })?;
            let mut metric_accesses = 0;
            let ordinate_less = source_arc_ordinates(&SourceArcRequest::NoOrdinates, || {
                metric_accesses += 1;
                Ok(metric)
            })?;
            let unavailable = source_arc_ordinates(&unavailable_request, || {
                metric_accesses += 1;
                Ok(metric)
            })?;
            let xy_request =
                SourceArcRequest::new(&source, CoordinateAxes::XY, &parameter, || {
                    longitude_accesses += 1;
                    Ok(retained_source_longitude(frame, &source))
                })?;
            let xy = source_arc_ordinates(&xy_request, || {
                metric_accesses += 1;
                Ok(metric)
            })?;

            assert_eq!(longitude_accesses, 0);
            assert_eq!(metric_accesses, 0);
            assert_eq!(ordinate_less.z.value(), None);
            assert_eq!(ordinate_less.m.value(), None);
            assert_eq!(unavailable.z.value(), None);
            assert_eq!(unavailable.m.value(), None);
            assert_eq!(xy.z.value(), None);
            assert_eq!(xy.m.value(), None);

            let subnormal = ExactRational::from_f64(f64::from_bits(1));
            let quarter = ExactRational::from_f64(0.25);
            let near_zero = subnormal.try_product(&quarter)?;
            let near_one = ExactRational::one().try_subtract(&near_zero)?;
            assert_eq!(near_zero.round()?, 0.0);
            assert_eq!(near_one.round()?, 1.0);
            let interior_start_missing = [
                Point::new_axes(0.0, -30.0, ZOrdinate(Some(7.0)), MOrdinate(None))?,
                Point::new_axes(0.0, 70.0, ZOrdinate(None), MOrdinate(None))?,
            ];
            let near_start_request = SourceArcRequest::new(
                &interior_start_missing,
                CoordinateAxes::XYZM,
                &near_zero,
                || Ok(retained_source_longitude(frame, &interior_start_missing)),
            )?;
            let near_end_request = SourceArcRequest::new(
                &interior_start_missing,
                CoordinateAxes::XYZM,
                &near_one,
                || Ok(retained_source_longitude(frame, &interior_start_missing)),
            )?;
            let near_start = source_arc_ordinates(&near_start_request, || {
                metric_accesses += 1;
                Ok(metric)
            })?;
            let near_end = source_arc_ordinates(&near_end_request, || {
                metric_accesses += 1;
                Ok(metric)
            })?;
            assert_eq!(metric_accesses, 0);
            assert_eq!(near_start.z.value(), None);
            assert_eq!(near_end.z.value(), None);
            Ok(())
        })
        .unwrap();
    }
}
