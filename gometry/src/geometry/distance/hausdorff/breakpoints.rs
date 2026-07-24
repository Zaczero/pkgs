#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;

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
pub(crate) struct SmallEquidistantRoots {
    values: [f64; 8],
    len: usize,
}

impl SmallEquidistantRoots {
    pub(crate) const fn new() -> Self {
        Self {
            values: [0.0; 8],
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
        if (params[read] - params[write - 1]).abs() > 1e-15 {
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
    let full_pairs =
        target.feature_count().saturating_mul(params.len()) <= HAUSDORFF_EQUIDISTANT_FULL_PAIRS;
    for &[ta, tb] in params.array_windows::<2>() {
        if tb <= ta {
            continue;
        }
        if full_pairs {
            interval.clear();
            for (left, &left_feature) in features.iter().enumerate() {
                if culled(left_feature) {
                    continue;
                }
                for &right_feature in &features[(left + 1)..] {
                    if culled(right_feature) {
                        continue;
                    }
                    push_equidistant_root_bisect(
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
        } else {
            let mid = f64::midpoint(ta, tb);
            best = best.max(sample_hausdorff_on_segment(ax, ay, dx, dy, mid, target));
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
