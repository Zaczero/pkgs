#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
pub(crate) const HAUSDORFF_EQUIDISTANT_FULL_PAIRS: usize = 0x4000;

/// Directed Hausdorff targets with at most this many vertices skip spatial
/// indexes and use a linear segment scan plus stack-local breakpoint buffers.
pub(crate) const HAUSDORFF_SMALL_TARGET_MAX_VERTICES: usize = 16;

/// One directed Hausdorff operand using the facet/SIMD point-query engine.
pub(crate) struct HausdorffTarget {
    pub(crate) query: HausdorffLineworkQuery,
    pub(crate) points: Vec<Point>,
    pub(crate) point_index: Option<PointSetIndex>,
    pub(crate) features: Vec<HausdorffFeature>,
}

#[cfg(test)]
pub(crate) const fn should_build_index(segment_count: usize, point_count: usize) -> bool {
    let vertices = if point_count == 0 && segment_count > 0 {
        segment_count + 1
    } else if segment_count == 0 {
        point_count
    } else {
        segment_count + point_count
    };
    vertices > HAUSDORFF_SMALL_TARGET_MAX_VERTICES
}

impl HausdorffTarget {
    pub(crate) fn build(shape: &Shape) -> Self {
        let points = shape.point_only_points();
        Self::from_query(
            HausdorffLineworkQuery::from_shape(shape, false),
            points,
            None,
        )
    }

    pub(crate) fn from_xy_columns(xs: &[f64], ys: &[f64]) -> Self {
        Self::from_query(
            HausdorffLineworkQuery::from_open_xy_columns(xs, ys, false),
            Vec::new(),
            None,
        )
    }

    #[cfg(test)]
    pub(crate) fn from_parts(
        segments: &[Segment],
        points: Vec<Point>,
        force_index: Option<bool>,
    ) -> Self {
        let mut xs = Vec::with_capacity(segments.len() + 1);
        let mut ys = Vec::with_capacity(segments.len() + 1);
        if let Some(first) = segments.first() {
            xs.push(first.start.x);
            ys.push(first.start.y);
            for segment in segments {
                xs.push(segment.end.x);
                ys.push(segment.end.y);
            }
        }
        let force_bvh =
            force_index.unwrap_or_else(|| should_build_index(segments.len(), points.len()));
        Self::from_query(
            HausdorffLineworkQuery::from_open_xy_columns(&xs, &ys, force_bvh),
            points,
            force_index,
        )
    }

    fn from_query(
        query: HausdorffLineworkQuery,
        points: Vec<Point>,
        force_index: Option<bool>,
    ) -> Self {
        let build_point_index = force_index.is_some_and(|force| force) && !points.is_empty();
        let point_index = build_point_index.then(|| PointSetIndex::build(points.iter().copied()));
        let mut features = Vec::with_capacity(query.linework.segment_count() + points.len());
        query.linework.for_each_segment(|segment| {
            features.push(HausdorffFeature::Segment(segment));
        });
        features.extend(
            points
                .iter()
                .map(|point| HausdorffFeature::Point(point.xy())),
        );
        Self {
            query,
            points,
            point_index,
            features,
        }
    }

    fn linework_distance_squared(&self, point: XY) -> f64 {
        self.query.distance_squared(point)
    }

    pub(crate) fn distance_squared(&self, point: XY) -> f64 {
        let mut best = self.linework_distance_squared(point);
        if let Some(index) = &self.point_index {
            let probe = Point::new_unchecked_xy(point.x, point.y);
            best = best.min(index.nearest_distance_squared(probe));
        } else {
            for &target in &self.points {
                best = best.min(point_distance_squared(point, target.xy()));
            }
        }
        best
    }

    pub(crate) const fn feature_count(&self) -> usize {
        self.features.len()
    }
}

/// Stack-backed directed Hausdorff target for column windows with at most
/// [`HAUSDORFF_SMALL_TARGET_MAX_VERTICES`] vertices — features are built once
/// and reused across every source-segment evaluation (no per-segment `Vec`
/// allocation).
#[derive(Clone, Copy)]
pub(crate) struct SmallLineTarget {
    pub(crate) segments: [Segment; 15],
    pub(crate) n_segments: u8,
    pub(crate) features: [HausdorffFeature; 16],
    pub(crate) n_features: u8,
}

impl SmallLineTarget {
    pub(crate) fn from_xy_columns(xs: &[f64], ys: &[f64]) -> Self {
        debug_assert_eq!(xs.len(), ys.len());
        debug_assert!(xs.len() <= HAUSDORFF_SMALL_TARGET_MAX_VERTICES);
        let mut segments = [Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(0.0, 0.0),
        }; 15];
        let mut n_segments = 0_u8;
        for index in 1..xs.len() {
            segments[n_segments as usize] = Segment {
                start: XY::new(xs[index - 1], ys[index - 1]),
                end: XY::new(xs[index], ys[index]),
            };
            n_segments += 1;
        }
        let mut features = [HausdorffFeature::Point(XY::new(0.0, 0.0)); 16];
        let mut n_features = 0_u8;
        for index in 0..n_segments as usize {
            features[index] = HausdorffFeature::Segment(segments[index]);
            n_features += 1;
        }
        Self {
            segments,
            n_segments,
            features,
            n_features,
        }
    }
}

pub(crate) fn small_line_target_distance_squared(target: &SmallLineTarget, point: XY) -> f64 {
    let probe = Point::new_unchecked_xy(point.x, point.y);
    let mut best = f64::INFINITY;
    for index in 0..target.n_segments as usize {
        best = best.min(point_segment_distance_squared(
            probe,
            target.segments[index],
        ));
    }
    best
}

pub(crate) fn small_line_target_features_slice(target: &SmallLineTarget) -> &[HausdorffFeature] {
    &target.features[..target.n_features as usize]
}

pub(crate) fn max_point_to_target_squared_on_segment_small_culled(
    source: Segment,
    target: &SmallLineTarget,
    cmax_sq: Option<f64>,
) -> f64 {
    let ax = source.start.x;
    let ay = source.start.y;
    let dx = source.end.x - ax;
    let dy = source.end.y - ay;
    if dx == 0.0 && dy == 0.0 {
        return small_line_target_distance_squared(target, source.start);
    }
    let mut params = SmallHausdorffParams::new();
    collect_hausdorff_segment_params_small_culled(ax, ay, dx, dy, target, &mut params, cmax_sq);
    evaluate_max_point_to_target_squared_on_segment_small_culled(
        ax,
        ay,
        dx,
        dy,
        params.as_mut_slice(),
        target,
        cmax_sq,
    )
}

#[cfg(test)]
pub(crate) fn collect_hausdorff_segment_params_small(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    target: &SmallLineTarget,
    params: &mut impl HausdorffParamSink,
) {
    collect_hausdorff_segment_params_small_culled(ax, ay, dx, dy, target, params, None);
}

pub(crate) fn collect_hausdorff_segment_params_small_culled(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    target: &SmallLineTarget,
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
    for index in 0..target.n_segments as usize {
        let segment = target.segments[index];
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
}

pub(crate) fn sample_hausdorff_on_segment_small(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    t: f64,
    target: &SmallLineTarget,
) -> f64 {
    small_line_target_distance_squared(target, XY::new(ax + t * dx, ay + t * dy))
}

pub(crate) fn evaluate_max_point_to_target_squared_on_segment_small_culled(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    params: &mut [f64],
    target: &SmallLineTarget,
    cmax_sq: Option<f64>,
) -> f64 {
    evaluate_max_point_to_target_squared_on_segment_quadratic_small_culled(
        ax, ay, dx, dy, params, target, cmax_sq,
    )
}

#[cfg(test)]
pub(crate) fn evaluate_max_point_to_target_squared_on_segment_bisect_small(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    params: &mut [f64],
    target: &SmallLineTarget,
) -> f64 {
    evaluate_max_point_to_target_squared_on_segment_with_roots_small(
        ax,
        ay,
        dx,
        dy,
        params,
        target,
        &mut SmallEquidistantRoots::new(),
    )
}

#[cfg(test)]
pub(crate) fn evaluate_max_point_to_target_squared_on_segment_with_roots_small(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    params: &mut [f64],
    target: &SmallLineTarget,
    interval: &mut impl EquidistantRootSink,
) -> f64 {
    let param_count = compact_hausdorff_params(params);
    let params = &params[..param_count];

    let mut best = 0.0_f64;
    for &t in params {
        best = best.max(sample_hausdorff_on_segment_small(ax, ay, dx, dy, t, target));
    }

    let features = small_line_target_features_slice(target);
    let full_pairs =
        features.len().saturating_mul(params.len()) <= HAUSDORFF_EQUIDISTANT_FULL_PAIRS;
    for &[ta, tb] in params.array_windows::<2>() {
        if tb <= ta {
            continue;
        }
        if full_pairs {
            interval.clear();
            for left in 0..features.len() {
                for right in (left + 1)..features.len() {
                    push_equidistant_root_bisect(
                        ax,
                        ay,
                        dx,
                        dy,
                        ta,
                        tb,
                        features[left],
                        features[right],
                        interval,
                    );
                }
            }
            for &t in interval.roots() {
                best = best.max(sample_hausdorff_on_segment_small(ax, ay, dx, dy, t, target));
            }
        } else {
            let mid = f64::midpoint(ta, tb);
            best = best.max(sample_hausdorff_on_segment_small(
                ax, ay, dx, dy, mid, target,
            ));
        }
    }
    best
}

pub(crate) fn evaluate_max_point_to_target_squared_on_segment_quadratic_small_culled(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    params: &mut [f64],
    target: &SmallLineTarget,
    cmax_sq: Option<f64>,
) -> f64 {
    let param_count = compact_hausdorff_params(params);
    let params = &params[..param_count];

    let mut best = 0.0_f64;
    for &t in params {
        best = best.max(sample_hausdorff_on_segment_small(ax, ay, dx, dy, t, target));
    }

    let prune_margin = cmax_sq.and_then(|cmax_sq| {
        if cmax_sq <= 0.0 || !cmax_sq.is_finite() {
            return None;
        }
        let length = sqrt_distance_squared(dx * dx + dy * dy);
        Some(sqrt_distance_squared(cmax_sq) + length)
    });
    let features = small_line_target_features_slice(target);
    let culled = |feature: HausdorffFeature| {
        prune_margin.is_some_and(|margin| {
            hausdorff_feature_bbox_disjoint_from_expanded_source(ax, ay, dx, dy, feature, margin)
        })
    };
    let mut interval = SmallEquidistantRoots::new();
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
                    &mut interval,
                );
            }
        }
        for &t in interval.roots() {
            best = best.max(sample_hausdorff_on_segment_small(ax, ay, dx, dy, t, target));
        }
    }
    best
}
