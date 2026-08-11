use crate::geometry::distance::hausdorff::{
    HausdorffTargetLike, push_equidistant_roots_on_interval, sqrt_distance_squared, stats,
};
use crate::geometry::{
    Orientation, Segment, XY, orientation, point_distance_squared, point_segment_distance_squared,
    try_point_distance_squared,
};

#[derive(Clone, Copy)]
pub(crate) enum HausdorffFeature {
    Point(XY),
    Segment(Segment),
}

pub(crate) fn max_point_to_target_squared_on_segment_culled<T: HausdorffTargetLike>(
    source: Segment,
    target: &T,
    cmax_sq: Option<f64>,
) -> f64 {
    let ax = source.start.x;
    let ay = source.start.y;
    let dx = source.end.x - ax;
    let dy = source.end.y - ay;
    if dx == 0.0 && dy == 0.0 {
        return target.distance_squared(source.start);
    }

    // A point-only target has one shared t² coefficient. Its lower envelope
    // is therefore the lower hull of affine terms, not the all-pairs roots
    // used by the mixed point/segment kernel below. Keep the existing kernel
    // as the exact fallback whenever the ordinary squared-space coefficients
    // are not trustworthy.
    if target.segments_len() == 0
        && target.points_len() > 0
        && let Some(answer) = max_point_to_point_target_squared_on_segment_envelope(source, target)
    {
        return answer;
    }

    max_point_to_target_squared_on_segment_culled_legacy(source, target, cmax_sq)
}

fn max_point_to_target_squared_on_segment_culled_legacy<T: HausdorffTargetLike>(
    source: Segment,
    target: &T,
    cmax_sq: Option<f64>,
) -> f64 {
    let ax = source.start.x;
    let ay = source.start.y;
    let dx = source.end.x - ax;
    let dy = source.end.y - ay;

    let mut params = Vec::new();
    collect_hausdorff_segment_params_culled(ax, ay, dx, dy, target, &mut params, cmax_sq);
    evaluate_max_point_to_target_squared_on_segment_culled(
        ax,
        ay,
        dx,
        dy,
        &mut params,
        target,
        cmax_sq,
    )
}

/// One affine term of a point target's squared-distance field after removing
/// the source segment's shared `t² * |delta|²` term.
#[derive(Clone, Copy)]
struct PointEnvelopeLine {
    slope: f64,
    intercept: f64,
    point: XY,
}

impl PointEnvelopeLine {
    fn from_target_point(source: Segment, point: XY) -> Option<Self> {
        let dx = source.end.x - source.start.x;
        let dy = source.end.y - source.start.y;
        let intercept = try_point_distance_squared(source.start, point)?;
        let rx = source.start.x - point.x;
        let ry = source.start.y - point.y;
        let dot = dx * rx + dy * ry;
        let slope = 2.0 * dot;
        slope.is_finite().then_some(Self {
            slope,
            intercept,
            point,
        })
    }

    fn breakpoint_after(self, next: Self) -> Option<f64> {
        debug_assert!(self.slope > next.slope);
        let denominator = self.slope - next.slope;
        let numerator = next.intercept - self.intercept;
        let t = numerator / denominator;
        t.is_finite().then_some(t)
    }
}

/// Exact lower-envelope maximum for a non-degenerate source segment against
/// an isolated-point target. Every point has
/// `|delta|² t² + slope*t + intercept`; removing the shared convex quadratic
/// leaves affine functions. The lower hull's switches, plus the two segment
/// endpoints, are the only possible maxima.
///
/// Hull membership is a decision path: `orientation` is the shared adaptive
/// certified interval-plus-exact-dyadic orientation predicate, never a raw
/// dual-space cross product. A
/// breakpoint division is inexact, so each switch is sampled with its adjacent
/// representable parameters as well as the computed value. Extra samples are
/// conservative for a maximum; omitting one could lose the answer.
fn max_point_to_point_target_squared_on_segment_envelope<T: HausdorffTargetLike>(
    source: Segment,
    target: &T,
) -> Option<f64> {
    // The envelope route is deliberately limited to normal squared space.
    // The framed continuous finisher owns the subnormal/overflow cases.
    try_point_distance_squared(source.start, source.end)?;
    let mut lines = Vec::with_capacity(target.points_len());
    for index in 0..target.points_len() {
        lines.push(PointEnvelopeLine::from_target_point(
            source,
            target.point_xy_at(index),
        )?);
    }
    let (candidate_count, hull) = point_envelope_lower_hull(lines);
    if candidate_count == 0 {
        return Some(f64::INFINITY);
    }

    stats::inc_point_envelope_candidates(candidate_count);
    stats::inc_point_envelope_breakpoints(hull.len().saturating_sub(1));

    // Endpoints are unconditional candidates. Retain the target query here:
    // it preserves the existing nearest-point reduction exactly at t=0 and 1.
    let mut best = target
        .distance_squared(source.start)
        .max(target.distance_squared(source.end));
    stats::inc_point_envelope_samples(2);
    // Every out-of-range switch clamps to one of these endpoint-neighbour
    // parameters. Cache the full target query so a long inactive hull tail
    // cannot turn conservative clipping into a quadratic path.
    let mut clipped_samples = Vec::<(u64, f64)>::with_capacity(4);
    for pair in hull.array_windows::<2>() {
        let t = pair[0].breakpoint_after(pair[1])?;
        let interior = t.total_cmp(&0.0).is_gt() && t.total_cmp(&1.0).is_lt();
        // A division can round a true boundary switch just outside [0, 1].
        // Clamp first, then include the two adjacent representable parameters.
        // At a clipped endpoint the adjacent pair need not be the global
        // nearest pair, so retain the target's complete reduction there.
        let t = if t.to_bits() << 1 == 0 {
            0.0
        } else {
            t.clamp(0.0, 1.0)
        };
        for t in [t.next_down().max(0.0), t, t.next_up().min(1.0)] {
            let probe = XY::new(
                source.start.x + t * (source.end.x - source.start.x),
                source.start.y + t * (source.end.y - source.start.y),
            );
            let distance = if interior {
                // The adjacent hull lines cover this switch and its immediate
                // floating neighbours. Taking their min avoids ever promoting
                // a non-nearest candidate into the maximum.
                point_distance_squared(probe, pair[0].point)
                    .min(point_distance_squared(probe, pair[1].point))
            } else if let Some((_, distance)) = clipped_samples
                .iter()
                .find(|(bits, _)| *bits == t.to_bits())
            {
                *distance
            } else {
                let distance = target.distance_squared(probe);
                clipped_samples.push((t.to_bits(), distance));
                distance
            };
            if !distance.is_finite() {
                return None;
            }
            best = best.max(distance);
            stats::inc_point_envelope_samples(1);
        }
    }
    Some(best)
}

/// Exact numerical slope equality for envelope grouping. Coefficients are
/// already finite; the two zero encodings are one affine slope, while every
/// other value must retain its own computed line.
const fn same_envelope_slope(left: f64, right: f64) -> bool {
    let left_bits = left.to_bits();
    let right_bits = right.to_bits();
    left_bits == right_bits || (left_bits << 1 == 0 && right_bits << 1 == 0)
}

/// Lower hull of affine distance terms in descending slope order. A
/// clockwise dual turn is the only shape with a non-empty lower-envelope
/// interval for the middle line.
fn point_envelope_lower_hull(mut lines: Vec<PointEnvelopeLine>) -> (usize, Vec<PointEnvelopeLine>) {
    lines.sort_unstable_by(|left, right| {
        if same_envelope_slope(left.slope, right.slope) {
            left.intercept.total_cmp(&right.intercept)
        } else {
            right.slope.total_cmp(&left.slope)
        }
    });

    // Parallel affine terms never switch. Keep their lowest intercept.
    let mut unique = Vec::with_capacity(lines.len());
    for line in lines {
        if unique.last().is_some_and(|previous: &PointEnvelopeLine| {
            same_envelope_slope(previous.slope, line.slope)
        }) {
            continue;
        }
        unique.push(line);
    }

    let candidate_count = unique.len();
    let mut hull: Vec<PointEnvelopeLine> = Vec::with_capacity(candidate_count);
    for line in unique {
        while hull.len() >= 2 {
            let before = hull[hull.len() - 2];
            let previous = hull[hull.len() - 1];
            // Descending slopes: a clockwise dual turn is exactly an active
            // lower-envelope line. Collinear/counter-clockwise middle lines
            // have an empty interval and must be removed.
            if orientation(
                XY::new(before.slope, before.intercept),
                XY::new(previous.slope, previous.intercept),
                XY::new(line.slope, line.intercept),
            ) == Orientation::Clockwise
            {
                break;
            }
            hull.pop();
        }
        hull.push(line);
    }
    (candidate_count, hull)
}

#[cfg(test)]
pub(crate) fn point_envelope_dual_hull_len_for_test(lines: &[(f64, f64)]) -> usize {
    point_envelope_lower_hull(
        lines
            .iter()
            .map(|&(slope, intercept)| PointEnvelopeLine {
                slope,
                intercept,
                point: XY::new(0.0, 0.0),
            })
            .collect(),
    )
    .1
    .len()
}

#[cfg(test)]
pub(crate) fn max_point_to_target_squared_on_segment_culled_legacy_for_test<
    T: HausdorffTargetLike,
>(
    source: Segment,
    target: &T,
) -> f64 {
    max_point_to_target_squared_on_segment_culled_legacy(source, target, None)
}

/// Stack buffer for breakpoint parameters on small Hausdorff targets (at most
/// 16 segments × 2 interior roots plus endpoints).
pub(crate) struct SmallHausdorffParams {
    values: [f64; 34],
    len: usize,
}

impl SmallHausdorffParams {
    pub(crate) const fn new() -> Self {
        Self {
            values: [0.0; 34],
            len: 0,
        }
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [f64] {
        debug_assert!(self.len <= self.values.len());
        &mut self.values[..self.len]
    }
}

impl HausdorffParamSink for SmallHausdorffParams {
    fn push_param(&mut self, value: f64) {
        assert!(
            self.len < self.values.len(),
            "small Hausdorff breakpoint buffer overflow"
        );
        self.values[self.len] = value;
        self.len += 1;
    }
}

impl HausdorffParamSink for Vec<f64> {
    fn push_param(&mut self, value: f64) {
        self.push(value);
    }
}

/// Stack buffer for equidistant roots on small Hausdorff targets.
///
/// A small line target has at most 15 segment features. Every unordered
/// feature pair contributes at most two roots on one parameter interval, so
/// the proved maximum is `2 * C(15, 2) = 210`.
pub(crate) struct SmallEquidistantRoots {
    values: [f64; 210],
    len: usize,
}

impl SmallEquidistantRoots {
    pub(crate) const fn new() -> Self {
        Self {
            values: [0.0; 210],
            len: 0,
        }
    }
}

impl EquidistantRootSink for SmallEquidistantRoots {
    fn clear(&mut self) {
        self.len = 0;
    }

    fn push_root(&mut self, value: f64) {
        assert!(
            self.len < self.values.len(),
            "small equidistant root buffer overflow"
        );
        self.values[self.len] = value;
        self.len += 1;
    }

    fn roots(&self) -> &[f64] {
        &self.values[..self.len]
    }
}

impl EquidistantRootSink for Vec<f64> {
    fn clear(&mut self) {
        Self::clear(self);
    }

    fn push_root(&mut self, value: f64) {
        self.push(value);
    }

    fn roots(&self) -> &[f64] {
        self.as_slice()
    }
}

pub(crate) trait HausdorffParamSink {
    fn push_param(&mut self, value: f64);
}

pub(crate) trait EquidistantRootSink {
    fn clear(&mut self);
    fn push_root(&mut self, value: f64);
    fn roots(&self) -> &[f64];
}

const fn hausdorff_feature_bbox(feature: HausdorffFeature) -> (f64, f64, f64, f64) {
    match feature {
        HausdorffFeature::Point(point) => (point.x, point.y, point.x, point.y),
        HausdorffFeature::Segment(segment) => (
            segment.start.x.min(segment.end.x),
            segment.start.y.min(segment.end.y),
            segment.start.x.max(segment.end.x),
            segment.start.y.max(segment.end.y),
        ),
    }
}

/// True when every point of `feature` lies farther than `margin` from every
/// point of the source segment — so the feature cannot influence the
/// distance-to-set field along that segment while `cmax` holds.
pub(crate) fn hausdorff_feature_bbox_disjoint_from_expanded_source(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    feature: HausdorffFeature,
    margin: f64,
) -> bool {
    let (min_x, min_y) = (ax.min(ax + dx), ay.min(ay + dy));
    let (max_x, max_y) = (ax.max(ax + dx), ay.max(ay + dy));
    let (f_min_x, f_min_y, f_max_x, f_max_y) = hausdorff_feature_bbox(feature);
    f_max_x < min_x - margin
        || f_min_x > max_x + margin
        || f_max_y < min_y - margin
        || f_min_y > max_y + margin
}

pub(crate) fn collect_hausdorff_segment_params_culled<T: HausdorffTargetLike>(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    target: &T,
    params: &mut impl HausdorffParamSink,
    cmax_sq: Option<f64>,
) {
    params.push_param(0.0);
    params.push_param(1.0);
    let prune_margin = cmax_sq.and_then(|cmax_sq| {
        if cmax_sq <= 0.0 || !cmax_sq.is_finite() {
            return None;
        }
        let length = sqrt_distance_squared(dx * dx + dy * dy);
        Some(sqrt_distance_squared(cmax_sq) + length)
    });
    for index in 0..target.segments_len() {
        let segment = target.segment_at(index);
        if prune_margin.is_some_and(|margin| {
            hausdorff_feature_bbox_disjoint_from_expanded_source(
                ax,
                ay,
                dx,
                dy,
                HausdorffFeature::Segment(segment),
                margin,
            )
        }) {
            continue;
        }
        push_segment_projection_breakpoints(ax, ay, dx, dy, segment, params);
    }
    for index in 0..target.points_len() {
        let point = target.point_xy_at(index);
        if prune_margin.is_some_and(|margin| {
            hausdorff_feature_bbox_disjoint_from_expanded_source(
                ax,
                ay,
                dx,
                dy,
                HausdorffFeature::Point(point),
                margin,
            )
        }) {
            continue;
        }
        push_point_on_line_breakpoint(ax, ay, dx, dy, point, params);
    }
}

pub(crate) fn compact_hausdorff_params(params: &mut [f64]) -> usize {
    params.sort_by(f64::total_cmp);
    if params.is_empty() {
        return 0;
    }
    let mut write = 1_usize;
    for read in 1..params.len() {
        if params[read].to_bits() != params[write - 1].to_bits() {
            params[write] = params[read];
            write += 1;
        }
    }
    write
}

pub(crate) fn evaluate_max_point_to_target_squared_on_segment_culled<T: HausdorffTargetLike>(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    params: &mut [f64],
    target: &T,
    cmax_sq: Option<f64>,
) -> f64 {
    evaluate_max_point_to_target_squared_on_segment_with_roots_culled(
        ax,
        ay,
        dx,
        dy,
        params,
        target,
        &mut Vec::new(),
        cmax_sq,
    )
}

pub(crate) fn evaluate_max_point_to_target_squared_on_segment_with_roots_culled<
    T: HausdorffTargetLike,
>(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    params: &mut [f64],
    target: &T,
    interval: &mut impl EquidistantRootSink,
    cmax_sq: Option<f64>,
) -> f64 {
    let param_count = compact_hausdorff_params(params);
    let params = &params[..param_count];

    let mut best = 0.0_f64;
    for &t in params {
        best = best.max(sample_hausdorff_on_segment(ax, ay, dx, dy, t, target));
    }

    let prune_margin = cmax_sq.and_then(|cmax_sq| {
        if cmax_sq <= 0.0 || !cmax_sq.is_finite() {
            return None;
        }
        let length = sqrt_distance_squared(dx * dx + dy * dy);
        Some(sqrt_distance_squared(cmax_sq) + length)
    });
    let features = target.features_slice();
    let culled = |feature: HausdorffFeature| {
        prune_margin.is_some_and(|margin| {
            hausdorff_feature_bbox_disjoint_from_expanded_source(ax, ay, dx, dy, feature, margin)
        })
    };
    // Always enumerate equidistant roots. A midpoint-only fallback above a
    // feature×param budget changes the metric (continuous max → coarse sample)
    // and is not an optimization of the same answer.
    for &[ta, tb] in params.array_windows::<2>() {
        if tb <= ta {
            continue;
        }
        interval.clear();
        for (left, &left_feature) in features.iter().enumerate() {
            if culled(left_feature) {
                continue;
            }
            for &right_feature in &features[(left + 1)..] {
                if culled(right_feature) {
                    continue;
                }
                push_equidistant_roots_on_interval(
                    ax,
                    ay,
                    dx,
                    dy,
                    ta,
                    tb,
                    left_feature,
                    right_feature,
                    interval,
                );
            }
        }
        for &t in interval.roots() {
            best = best.max(sample_hausdorff_on_segment(ax, ay, dx, dy, t, target));
        }
    }
    best
}

pub(crate) fn sample_hausdorff_on_segment<T: HausdorffTargetLike>(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    t: f64,
    target: &T,
) -> f64 {
    target.distance_squared(XY::new(ax + t * dx, ay + t * dy))
}

pub(crate) fn push_linear_parameter(
    numerator: f64,
    denominator: f64,
    params: &mut impl HausdorffParamSink,
) {
    if denominator == 0.0 {
        return;
    }
    let t = numerator / denominator;
    if t.is_finite() && (0.0..=1.0).contains(&t) {
        params.push_param(t);
    }
}

pub(crate) fn push_segment_projection_breakpoints(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    segment: Segment,
    params: &mut impl HausdorffParamSink,
) {
    let ex = segment.end.x - segment.start.x;
    let ey = segment.end.y - segment.start.y;
    let denom = dx * ex + dy * ey;
    if denom == 0.0 {
        return;
    }
    let acx = ax - segment.start.x;
    let acy = ay - segment.start.y;
    push_linear_parameter(-(acx * ex + acy * ey), denom, params);
    let adx = ax - segment.end.x;
    let ady = ay - segment.end.y;
    push_linear_parameter(-(adx * ex + ady * ey), denom, params);
}

pub(crate) fn push_point_on_line_breakpoint(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    point: XY,
    params: &mut impl HausdorffParamSink,
) {
    let length2 = dx * dx + dy * dy;
    if length2 == 0.0 {
        return;
    }
    let t = ((point.x - ax) * dx + (point.y - ay) * dy) / length2;
    if t.is_finite() && (0.0..=1.0).contains(&t) {
        params.push_param(t);
    }
}

pub(crate) fn hausdorff_feature_distance_squared_at(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    t: f64,
    feature: HausdorffFeature,
) -> f64 {
    let point = XY::new(ax + t * dx, ay + t * dy);
    match feature {
        HausdorffFeature::Point(target) => point_distance_squared(point, target),
        HausdorffFeature::Segment(segment) => point_segment_distance_squared(point, segment),
    }
}
