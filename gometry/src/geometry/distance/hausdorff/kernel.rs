#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;

pub(crate) trait HausdorffTargetLike {
    fn distance_squared(&self, point: XY) -> f64;
    fn batch_probe(
        &self,
        probes: &[(f64, f64)],
        out: &mut [HausdorffProbeResult],
        stack: &mut Vec<u32>,
    );
    fn feature_count(&self) -> usize;
    fn features_slice(&self) -> &[HausdorffFeature];
    fn segments_len(&self) -> usize;
    fn segment_at(&self, index: usize) -> Segment;
    fn points_len(&self) -> usize;
    fn point_xy_at(&self, index: usize) -> XY;
    fn max_distance_squared_on_segment_culled(&self, source: Segment, cmax_sq: f64) -> f64;
}

fn fold_isolated_point_probes(
    target: &HausdorffTarget,
    probes: &[(f64, f64)],
    out: &mut [HausdorffProbeResult],
) {
    for (index, &(x, y)) in probes.iter().enumerate() {
        let point = XY::new(x, y);
        let mut best = out[index].distance_squared;
        let mut witness = out[index].feature_id;
        if let Some(point_index) = &target.point_index {
            let probe = Point::new_unchecked_xy(x, y);
            if let Some((_, distance_squared)) = point_index.nearest(probe)
                && distance_squared < best
            {
                best = distance_squared;
                witness = u32::MAX;
            }
        } else {
            for point_index in 0..target.points.len() {
                let feature_index = target.query.linework.segment_count() + point_index;
                let distance_squared =
                    point_distance_squared(point, target.points[point_index].xy());
                if distance_squared < best {
                    best = distance_squared;
                    witness = feature_index as u32;
                }
            }
        }
        out[index] = HausdorffProbeResult {
            distance_squared: best,
            feature_id: witness,
        };
    }
}

impl HausdorffTargetLike for SmallLineTarget {
    fn max_distance_squared_on_segment_culled(&self, source: Segment, cmax_sq: f64) -> f64 {
        max_point_to_target_squared_on_segment_small_culled(source, self, Some(cmax_sq))
    }

    fn batch_probe(
        &self,
        probes: &[(f64, f64)],
        out: &mut [HausdorffProbeResult],
        _stack: &mut Vec<u32>,
    ) {
        debug_assert_eq!(probes.len(), out.len());
        for (slot, &(x, y)) in std::iter::zip(out.iter_mut(), probes) {
            let mut best = f64::INFINITY;
            let mut witness = u32::MAX;
            for index in 0..self.n_segments as usize {
                let distance_squared = point_segment_distance_squared(
                    Point::new_unchecked_xy(x, y),
                    self.segments[index],
                );
                if distance_squared < best {
                    best = distance_squared;
                    witness = index as u32;
                }
            }
            *slot = HausdorffProbeResult {
                distance_squared: best,
                feature_id: witness,
            };
        }
    }

    fn distance_squared(&self, point: XY) -> f64 {
        small_line_target_distance_squared(self, point)
    }

    fn feature_count(&self) -> usize {
        self.n_features as usize
    }

    fn features_slice(&self) -> &[HausdorffFeature] {
        small_line_target_features_slice(self)
    }

    fn segments_len(&self) -> usize {
        self.n_segments as usize
    }

    fn segment_at(&self, index: usize) -> Segment {
        self.segments[index]
    }

    fn points_len(&self) -> usize {
        0
    }

    fn point_xy_at(&self, index: usize) -> XY {
        let _ = index;
        unreachable!("small line targets have no isolated points")
    }
}

impl HausdorffTargetLike for HausdorffTarget {
    fn max_distance_squared_on_segment_culled(&self, source: Segment, cmax_sq: f64) -> f64 {
        max_point_to_target_squared_on_segment_culled(source, self, Some(cmax_sq))
    }

    fn batch_probe(
        &self,
        probes: &[(f64, f64)],
        out: &mut [HausdorffProbeResult],
        stack: &mut Vec<u32>,
    ) {
        self.query.batch_probe(probes, out, stack);
        if !self.points.is_empty() {
            fold_isolated_point_probes(self, probes, out);
        }
    }

    fn distance_squared(&self, point: XY) -> f64 {
        Self::distance_squared(self, point)
    }

    fn feature_count(&self) -> usize {
        Self::feature_count(self)
    }

    fn features_slice(&self) -> &[HausdorffFeature] {
        &self.features
    }

    fn segments_len(&self) -> usize {
        self.query.linework.segment_count()
    }

    fn segment_at(&self, index: usize) -> Segment {
        let mut current = 0_usize;
        let mut found = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(0.0, 0.0),
        };
        self.query.linework.for_each_segment(|segment| {
            if current == index {
                found = segment;
            }
            current += 1;
        });
        found
    }

    fn points_len(&self) -> usize {
        self.points.len()
    }

    fn point_xy_at(&self, index: usize) -> XY {
        self.points[index].xy()
    }
}

pub(crate) fn directed_hausdorff_distance_squared_shapes(source: &Shape, target: &Shape) -> f64 {
    if source.is_empty() || target.is_empty() {
        return f64::INFINITY;
    }
    let target = HausdorffTarget::build(target);
    directed_hausdorff_distance_squared_with_target_shape(source, &target)
}

pub(crate) fn directed_hausdorff_distance_squared_with_target_shape<T: HausdorffTargetLike>(
    source: &Shape,
    target: &T,
) -> f64 {
    let mut cmax = 0.0_f64;
    source.for_each_point(|point| {
        cmax = cmax.max(target.distance_squared(point.xy()));
    });
    source.for_each_segment(|segment| {
        let dx = segment.end.x - segment.start.x;
        let dy = segment.end.y - segment.start.y;
        if dx == 0.0 && dy == 0.0 {
            cmax = cmax.max(target.distance_squared(segment.start));
            return;
        }
        let bound_sq = hausdorff_segment_upper_bound_squared(
            target.distance_squared(segment.start),
            target.distance_squared(segment.end),
            dx * dx + dy * dy,
        );
        if bound_sq <= cmax {
            return;
        }
        cmax = cmax.max(target.max_distance_squared_on_segment_culled(segment, cmax));
    });
    cmax
}

/// Planar Hausdorff between two uniform 4-vertex line windows (closed-form
/// quadratic equidistant roots — no bisection hot loop).
pub(crate) fn hausdorff_distance_uniform_4v(
    left_xs: &[f64],
    left_ys: &[f64],
    right_xs: &[f64],
    right_ys: &[f64],
) -> f64 {
    hausdorff_distance_4v_exact(left_xs, left_ys, right_xs, right_ys)
}

/// Closed-form 4×4 Hausdorff kernel (quadratic critical-`t` enumeration).
pub(crate) fn hausdorff_distance_4v_exact(
    left_xs: &[f64],
    left_ys: &[f64],
    right_xs: &[f64],
    right_ys: &[f64],
) -> f64 {
    hausdorff_distance_4v_with_targets(
        left_xs,
        left_ys,
        &SmallLineTarget::from_xy_columns(left_xs, left_ys),
        right_xs,
        right_ys,
        &SmallLineTarget::from_xy_columns(right_xs, right_ys),
    )
}

/// Bisection oracle for 4×4 Hausdorff (test / fallback reference).
#[cfg(test)]
pub(crate) fn hausdorff_distance_4v_fused(
    left_xs: &[f64],
    left_ys: &[f64],
    right_xs: &[f64],
    right_ys: &[f64],
) -> f64 {
    debug_assert_eq!(left_xs.len(), 4);
    debug_assert_eq!(right_xs.len(), 4);
    let right = SmallLineTarget::from_xy_columns(right_xs, right_ys);
    let left = SmallLineTarget::from_xy_columns(left_xs, left_ys);
    directed_hausdorff_4v_bisect(left_xs, left_ys, &right)
        .max(directed_hausdorff_4v_bisect(right_xs, right_ys, &left))
        .sqrt()
}

#[cfg(test)]
pub(crate) fn directed_hausdorff_4v_bisect(
    xs: &[f64],
    ys: &[f64],
    target: &SmallLineTarget,
) -> f64 {
    debug_assert_eq!(xs.len(), 4);
    let mut best = 0.0_f64;
    for index in 0..4 {
        best = best.max(small_line_target_distance_squared(
            target,
            XY::new(xs[index], ys[index]),
        ));
    }
    for index in 1..4 {
        let ax = xs[index - 1];
        let ay = ys[index - 1];
        let dx = xs[index] - ax;
        let dy = ys[index] - ay;
        if dx == 0.0 && dy == 0.0 {
            best = best.max(small_line_target_distance_squared(target, XY::new(ax, ay)));
        } else {
            let mut params = SmallHausdorffParams::new();
            collect_hausdorff_segment_params_small(ax, ay, dx, dy, target, &mut params);
            best = best.max(
                evaluate_max_point_to_target_squared_on_segment_bisect_small(
                    ax,
                    ay,
                    dx,
                    dy,
                    params.as_mut_slice(),
                    target,
                ),
            );
        }
    }
    best
}

pub(crate) fn hausdorff_distance_4v_with_targets(
    left_xs: &[f64],
    left_ys: &[f64],
    left: &SmallLineTarget,
    right_xs: &[f64],
    right_ys: &[f64],
    right: &SmallLineTarget,
) -> f64 {
    directed_hausdorff_4v_fused(left_xs, left_ys, right)
        .max(directed_hausdorff_4v_fused(right_xs, right_ys, left))
        .sqrt()
}

pub(crate) fn directed_hausdorff_4v_fused(xs: &[f64], ys: &[f64], target: &SmallLineTarget) -> f64 {
    debug_assert_eq!(xs.len(), 4);
    directed_hausdorff_distance_squared_with_target_pruned(xs, ys, target)
}

/// Symmetric planar Hausdorff² between two column windows (one target build
/// per direction).
pub(crate) fn hausdorff_distance_squared_line_columns(
    left_xs: &[f64],
    left_ys: &[f64],
    right_xs: &[f64],
    right_ys: &[f64],
) -> f64 {
    if left_xs.is_empty() || right_xs.is_empty() {
        return f64::INFINITY;
    }
    if left_xs.len() == 4 && right_xs.len() == 4 {
        let left = SmallLineTarget::from_xy_columns(left_xs, left_ys);
        let right = SmallLineTarget::from_xy_columns(right_xs, right_ys);
        return directed_hausdorff_4v_fused(left_xs, left_ys, &right)
            .max(directed_hausdorff_4v_fused(right_xs, right_ys, &left));
    }
    let right_target = hausdorff_target_from_xy_columns(right_xs, right_ys);
    let left_target = hausdorff_target_from_xy_columns(left_xs, left_ys);
    let lb_ab = directed_hausdorff_vertex_lower_bound_squared(left_xs, left_ys, &right_target);
    let lb_ba = directed_hausdorff_vertex_lower_bound_squared(right_xs, right_ys, &left_target);
    if lb_ab >= lb_ba {
        let exact_ab =
            directed_hausdorff_distance_squared_with_target_pruned(left_xs, left_ys, &right_target);
        exact_ab.max(
            directed_hausdorff_distance_squared_with_target_pruned_initial(
                right_xs,
                right_ys,
                &left_target,
                exact_ab,
            ),
        )
    } else {
        let exact_ba = directed_hausdorff_distance_squared_with_target_pruned(
            right_xs,
            right_ys,
            &left_target,
        );
        exact_ba.max(
            directed_hausdorff_distance_squared_with_target_pruned_initial(
                left_xs,
                left_ys,
                &right_target,
                exact_ba,
            ),
        )
    }
}

fn hausdorff_target_from_xy_columns(xs: &[f64], ys: &[f64]) -> HausdorffTargetBorrowed {
    if xs.len() <= HAUSDORFF_SMALL_TARGET_MAX_VERTICES {
        HausdorffTargetBorrowed::Small(SmallLineTarget::from_xy_columns(xs, ys))
    } else {
        HausdorffTargetBorrowed::Indexed(Box::new(HausdorffTarget::from_xy_columns(xs, ys)))
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "SmallLineTarget is a fixed stack buffer; Indexed is boxed"
)]
enum HausdorffTargetBorrowed {
    Small(SmallLineTarget),
    Indexed(Box<HausdorffTarget>),
}

impl HausdorffTargetLike for HausdorffTargetBorrowed {
    fn max_distance_squared_on_segment_culled(&self, source: Segment, cmax_sq: f64) -> f64 {
        match self {
            Self::Small(target) => target.max_distance_squared_on_segment_culled(source, cmax_sq),
            Self::Indexed(target) => target.max_distance_squared_on_segment_culled(source, cmax_sq),
        }
    }

    fn batch_probe(
        &self,
        probes: &[(f64, f64)],
        out: &mut [HausdorffProbeResult],
        stack: &mut Vec<u32>,
    ) {
        match self {
            Self::Small(target) => target.batch_probe(probes, out, stack),
            Self::Indexed(target) => target.batch_probe(probes, out, stack),
        }
    }

    fn distance_squared(&self, point: XY) -> f64 {
        match self {
            Self::Small(target) => target.distance_squared(point),
            Self::Indexed(target) => target.distance_squared(point),
        }
    }

    fn feature_count(&self) -> usize {
        match self {
            Self::Small(target) => target.feature_count(),
            Self::Indexed(target) => target.feature_count(),
        }
    }

    fn features_slice(&self) -> &[HausdorffFeature] {
        match self {
            Self::Small(target) => target.features_slice(),
            Self::Indexed(target) => target.features_slice(),
        }
    }

    fn segments_len(&self) -> usize {
        match self {
            Self::Small(target) => target.segments_len(),
            Self::Indexed(target) => target.segments_len(),
        }
    }

    fn segment_at(&self, index: usize) -> Segment {
        match self {
            Self::Small(target) => target.segment_at(index),
            Self::Indexed(target) => target.segment_at(index),
        }
    }

    fn points_len(&self) -> usize {
        match self {
            Self::Small(target) => target.points_len(),
            Self::Indexed(target) => target.points_len(),
        }
    }

    fn point_xy_at(&self, index: usize) -> XY {
        match self {
            Self::Small(target) => target.point_xy_at(index),
            Self::Indexed(target) => target.point_xy_at(index),
        }
    }
}

/// Planar Hausdorff between two line coordinate windows (vertex metric).
pub(crate) fn hausdorff_distance_line_columns(
    left_xs: &[f64],
    left_ys: &[f64],
    right_xs: &[f64],
    right_ys: &[f64],
) -> f64 {
    hausdorff_distance_squared_line_columns(left_xs, left_ys, right_xs, right_ys).sqrt()
}

/// Element-wise planar Hausdorff over packed line CSR windows (one column
/// kernel per pair, identity shortcuts for identical windows).
pub(crate) fn hausdorff_distance_line_columns_batch(
    left: crate::geometry::packed_line_metrics::PackedLineColumnView<'_>,
    right: crate::geometry::packed_line_metrics::PackedLineColumnView<'_>,
    out: &mut [f64],
) {
    let len = left.logical_len();
    debug_assert_eq!(len, right.logical_len());
    debug_assert_eq!(out.len(), len);
    for (index, slot) in out.iter_mut().enumerate() {
        let (left_xs, left_ys) = left.row_xy(index);
        let (right_xs, right_ys) = right.row_xy(index);
        *slot = crate::geometry::packed_line_metrics::packed_line_same_window_value(
            left_xs,
            left_ys,
            right_xs,
            right_ys,
            f64::INFINITY,
        )
        .unwrap_or_else(|| {
            if left_xs.len() == 4 && right_xs.len() == 4 {
                hausdorff_distance_uniform_4v(left_xs, left_ys, right_xs, right_ys)
            } else {
                hausdorff_distance_squared_line_columns(left_xs, left_ys, right_xs, right_ys).sqrt()
            }
        });
    }
}
