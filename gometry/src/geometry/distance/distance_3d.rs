use std::sync::OnceLock;

use crate::HeapSize;
use crate::geometry::facet_bvh::BVH_MIN_INDEXED_SEGMENTS;
use crate::geometry::{BvhCode, BvhNode, CoordSeq, Shape, linear};

type Aabb3d = [f64; 6];

#[derive(Clone, Copy)]
pub(crate) struct Segment3d {
    pub start: [f64; 3],
    pub end: [f64; 3],
    pub aabb: Aabb3d,
}

pub(crate) struct Distance3dParts {
    pub(crate) segments: Vec<Segment3d>,
    bvh: OnceLock<Option<SegmentBvh3d>>,
}

struct SegmentBvh3d {
    aabbs: Vec<Aabb3d>,
    codes: Vec<u32>,
}

#[cfg(test)]
mod traversal_counters {
    use std::cell::Cell;

    #[derive(Clone, Copy, Debug, Default)]
    pub(crate) struct Snapshot {
        pub bound_calls: u64,
        pub pruned_nodes: u64,
        pub visited_leaves: u64,
    }

    thread_local! {
        static BOUND_CALLS: Cell<u64> = const { Cell::new(0) };
        static PRUNED_NODES: Cell<u64> = const { Cell::new(0) };
        static VISITED_LEAVES: Cell<u64> = const { Cell::new(0) };
    }

    pub(crate) fn bound_call() {
        BOUND_CALLS.set(BOUND_CALLS.get().wrapping_add(1));
    }

    pub(crate) fn pruned_node() {
        PRUNED_NODES.set(PRUNED_NODES.get().wrapping_add(1));
    }

    pub(crate) fn visited_leaf() {
        VISITED_LEAVES.set(VISITED_LEAVES.get().wrapping_add(1));
    }

    pub(crate) fn reset() {
        BOUND_CALLS.set(0);
        PRUNED_NODES.set(0);
        VISITED_LEAVES.set(0);
    }

    pub(crate) fn snapshot() -> Snapshot {
        Snapshot {
            bound_calls: BOUND_CALLS.get(),
            pruned_nodes: PRUNED_NODES.get(),
            visited_leaves: VISITED_LEAVES.get(),
        }
    }
}

crate::heapless!(Segment3d);

impl HeapSize for SegmentBvh3d {
    fn heap_bytes(&self) -> usize {
        self.aabbs
            .heap_bytes()
            .saturating_add(self.codes.heap_bytes())
    }
}

impl HeapSize for Distance3dParts {
    fn heap_bytes(&self) -> usize {
        self.segments
            .heap_bytes()
            .saturating_add(self.bvh.heap_bytes())
    }
}

const fn segment_aabb3(start: [f64; 3], end: [f64; 3]) -> Aabb3d {
    [
        start[0].min(end[0]),
        start[1].min(end[1]),
        start[2].min(end[2]),
        start[0].max(end[0]),
        start[1].max(end[1]),
        start[2].max(end[2]),
    ]
}

/// Take a nonnegative operation result one representable step below itself.
///
/// Under the binary64 round-to-nearest operation contract, the predecessor of
/// a finite result is no greater than the exact result that operation rounded.
/// An overflowing result has no finite predecessor, so the largest finite
/// value is the useful lower floor. The callers below handle exact zero
/// identities before reaching this helper.
const fn downward_nonnegative(value: f64) -> f64 {
    if value.is_infinite() {
        f64::MAX
    } else {
        value.next_down().max(0.0)
    }
}

fn downward_positive_difference(larger: f64, smaller: f64) -> f64 {
    downward_nonnegative(larger - smaller)
}

fn aabb3_gap(a_min: f64, a_max: f64, b_min: f64, b_max: f64) -> f64 {
    // A valid interval pair has only one separated direction. Select it
    // before subtraction so the other rounded direction cannot win.
    if a_min > b_max {
        downward_positive_difference(a_min, b_max)
    } else if b_min > a_max {
        downward_positive_difference(b_min, a_max)
    } else {
        0.0
    }
}

fn aabb3_gaps(a: Aabb3d, b: Aabb3d) -> (f64, f64, f64) {
    (
        aabb3_gap(a[0], a[3], b[0], b[3]),
        aabb3_gap(a[1], a[4], b[1], b[4]),
        aabb3_gap(a[2], a[5], b[2], b[5]),
    )
}

#[expect(
    clippy::float_cmp,
    reason = "exact equality preserves the required ratio-one identity"
)]
fn downward_ratio(value: f64, scale: f64) -> f64 {
    if value == 0.0 {
        0.0
    } else if value == scale {
        1.0
    } else {
        downward_nonnegative(value / scale)
    }
}

fn downward_square(value: f64) -> f64 {
    if value == 0.0 {
        0.0
    } else {
        downward_nonnegative(value * value)
    }
}

fn downward_add(left: f64, right: f64) -> f64 {
    if left == 0.0 {
        right.min(f64::MAX)
    } else if right == 0.0 {
        left.min(f64::MAX)
    } else {
        downward_nonnegative(left + right)
    }
}

fn downward_sqrt(value: f64) -> f64 {
    if value == 0.0 {
        0.0
    } else {
        downward_nonnegative(value.sqrt())
    }
}

fn downward_scale(value: f64, scale: f64) -> f64 {
    if value == 0.0 || scale == 0.0 {
        0.0
    } else {
        downward_nonnegative(value * scale)
    }
}

/// Conservative lower-bound distance between two 3D AABBs (0 when overlapping).
pub(crate) fn aabb3_distance_lower_bound(a: Aabb3d, b: Aabb3d) -> f64 {
    #[cfg(test)]
    traversal_counters::bound_call();

    let (dx, dy, dz) = aabb3_gaps(a, b);
    let scale = dx.max(dy).max(dz);
    if scale == 0.0 {
        return 0.0;
    }

    // The positive endpoint comparison above selects the represented input
    // difference before any subtraction can round. Each nonzero ratio,
    // square, sum, root, and final scale is then rounded downward. Therefore
    // this scaled norm is no greater than the norm of the exact box gaps.
    // Keep these as separate basic binary64 operations: fast-math
    // reassociation or contraction would invalidate the per-stage proof.
    let ratios = [
        downward_ratio(dx, scale),
        downward_ratio(dy, scale),
        downward_ratio(dz, scale),
    ];
    let squared = downward_add(
        downward_add(downward_square(ratios[0]), downward_square(ratios[1])),
        downward_square(ratios[2]),
    );
    downward_scale(scale, downward_sqrt(squared))
}

fn point_xyz(point: crate::geometry::Point) -> [f64; 3] {
    [point.x, point.y, point.z().expect("Z column present")]
}

fn points_xyz(coords: &CoordSeq) -> impl Iterator<Item = [f64; 3]> + '_ {
    let zs = coords.zs().expect("Z column present");
    coords
        .xs()
        .iter()
        .copied()
        .zip(coords.ys().iter().copied())
        .zip(zs.iter().copied())
        .map(|((x, y), z)| [x, y, z])
}

fn push_segment_3d(start: [f64; 3], end: [f64; 3], out: &mut Vec<Segment3d>) {
    out.push(Segment3d {
        start,
        end,
        aabb: segment_aabb3(start, end),
    });
}

fn push_segments_3d(coords: &CoordSeq, out: &mut Vec<Segment3d>) {
    let mut points = points_xyz(coords);
    let Some(start) = points.next() else {
        return;
    };
    let Some(mut end) = points.next() else {
        push_segment_3d(start, start, out);
        return;
    };
    push_segment_3d(start, end, out);
    for point in points {
        let start = end;
        end = point;
        push_segment_3d(start, end, out);
    }
}

fn collect_segments_3d(shape: &Shape, out: &mut Vec<Segment3d>) {
    match shape {
        Shape::Point(point) => {
            let point = point_xyz(*point);
            push_segment_3d(point, point, out);
        },
        Shape::MultiPoint(coords) => {
            for point in points_xyz(coords) {
                push_segment_3d(point, point, out);
            }
        },
        Shape::LineString(coords) => push_segments_3d(coords, out),
        Shape::MultiLineString(lines) => {
            for line in lines {
                push_segments_3d(line, out);
            }
        },
        Shape::Polygon(polygon) => {
            push_segments_3d(polygon.shell.coords(), out);
            for hole in polygon.holes.iter() {
                push_segments_3d(hole.coords(), out);
            }
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                push_segments_3d(polygon.shell.coords(), out);
                for hole in polygon.holes.iter() {
                    push_segments_3d(hole.coords(), out);
                }
            }
        },
        Shape::GeometryCollection(parts) => {
            for part in parts {
                collect_segments_3d(part, out);
            }
        },
        Shape::Empty(..) => {},
    }
}

impl Distance3dParts {
    pub(crate) fn build(shape: &Shape) -> Self {
        let mut segments = Vec::with_capacity(shape.coord_count());
        collect_segments_3d(shape, &mut segments);
        Self {
            segments,
            bvh: OnceLock::new(),
        }
    }

    fn bvh(&self) -> Option<&SegmentBvh3d> {
        self.bvh
            .get_or_init(|| {
                if self.segments.len() < BVH_MIN_INDEXED_SEGMENTS {
                    None
                } else {
                    SegmentBvh3d::build(&self.segments)
                }
            })
            .as_ref()
    }
}

impl SegmentBvh3d {
    fn build(segments: &[Segment3d]) -> Option<Self> {
        let Ok(segment_count) = u32::try_from(segments.len()) else {
            return None;
        };
        if segment_count == 0 || segment_count > BvhCode::LEAF_BIT / 2 {
            return None;
        }
        let mut ids = Vec::with_capacity(segments.len());
        ids.extend(0..segment_count);
        // `LEAF_BIT / 2` admission makes this exact node count fit `usize`.
        let node_count = (segment_count as usize).wrapping_mul(2).wrapping_sub(1);
        let mut tree = Self {
            aabbs: vec![[0.0; 6]; node_count],
            codes: vec![0; node_count],
        };
        let mut next = 1;
        tree.fill_node(0, &mut next, segments, &mut ids);
        Some(tree)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "node and segment IDs are generated from the admitted u32-sized tree and its exact node arrays"
    )]
    fn fill_node(
        &mut self,
        node: usize,
        next: &mut usize,
        segments: &[Segment3d],
        ids: &mut [u32],
    ) {
        if ids.len() == 1 {
            self.aabbs[node] = segments[ids[0] as usize].aabb;
            self.codes[node] = BvhCode::leaf(ids[0] as usize).raw();
            return;
        }
        let aabb = ids
            .iter()
            .map(|&id| segments[id as usize].aabb)
            .reduce(union_aabb3)
            .expect("non-empty ids");
        let axis = longest_axis(aabb);
        let pivot = ids.len() / 2;
        ids.select_nth_unstable_by(pivot, |&left, &right| {
            axis.lower(segments[left as usize].aabb)
                .total_cmp(&axis.lower(segments[right as usize].aabb))
        });
        let (lower, upper) = ids.split_at_mut(pivot);
        let first_child = *next;
        *next = next.wrapping_add(2);
        self.aabbs[node] = aabb;
        self.codes[node] = BvhCode::internal(first_child as u32).raw();
        self.fill_node(first_child, next, segments, lower);
        self.fill_node(first_child.wrapping_add(1), next, segments, upper);
    }

    fn distance_to_parts(
        &self,
        probe_parts: &Distance3dParts,
        target_segments: &[Segment3d],
        mut best: f64,
    ) -> crate::error::Result<f64> {
        let mut stack = Vec::new();
        for &probe in &probe_parts.segments {
            self.visit_with_targets(&probe, target_segments, &mut stack, &mut best)?;
            if best == 0.0 {
                return Ok(0.0);
            }
        }
        Ok(best)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "stack node IDs and decoded leaf IDs originate in this immutable BVH built from the same target segment slice"
    )]
    fn visit_with_targets(
        &self,
        probe: &Segment3d,
        targets: &[Segment3d],
        stack: &mut Vec<usize>,
        best: &mut f64,
    ) -> crate::error::Result<()> {
        stack.clear();
        stack.push(0);
        while let Some(node) = stack.pop() {
            if aabb3_distance_lower_bound(self.aabbs[node], probe.aabb) >= *best {
                #[cfg(test)]
                traversal_counters::pruned_node();
                continue;
            }
            match BvhCode::new(self.codes[node]).decode() {
                BvhNode::Leaf(index) => {
                    #[cfg(test)]
                    traversal_counters::visited_leaf();
                    let target = targets[index];
                    let distance = segment_segment_distance_3d(
                        probe.start,
                        probe.end,
                        target.start,
                        target.end,
                    )?;
                    if distance < *best {
                        *best = distance;
                    }
                },
                BvhNode::Internal(first_child) => {
                    self.push_nearer_last(stack, first_child, probe.aabb);
                },
            }
        }
        Ok(())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "first_child names the adjacent pair written by fill_node in the admitted node arrays"
    )]
    fn push_nearer_last(&self, stack: &mut Vec<usize>, first_child: usize, probe_aabb: Aabb3d) {
        let left_bound = aabb3_distance_lower_bound(self.aabbs[first_child], probe_aabb);
        let right_bound =
            aabb3_distance_lower_bound(self.aabbs[first_child.wrapping_add(1)], probe_aabb);
        if left_bound <= right_bound {
            stack.push(first_child.wrapping_add(1));
            stack.push(first_child);
        } else {
            stack.push(first_child);
            stack.push(first_child.wrapping_add(1));
        }
    }
}

#[derive(Clone, Copy)]
enum Axis3d {
    X,
    Y,
    Z,
}

impl Axis3d {
    fn lower(self, aabb: Aabb3d) -> f64 {
        match self {
            Self::X => aabb[0],
            Self::Y => aabb[1],
            Self::Z => aabb[2],
        }
    }
}

fn longest_axis(aabb: Aabb3d) -> Axis3d {
    let spans = [aabb[3] - aabb[0], aabb[4] - aabb[1], aabb[5] - aabb[2]];
    [Axis3d::X, Axis3d::Y, Axis3d::Z]
        .into_iter()
        .zip(spans)
        .max_by(|(_, left), (_, right)| left.total_cmp(right))
        .map_or(Axis3d::X, |(axis, _)| axis)
}

const fn union_aabb3(left: Aabb3d, right: Aabb3d) -> Aabb3d {
    [
        left[0].min(right[0]),
        left[1].min(right[1]),
        left[2].min(right[2]),
        left[3].max(right[3]),
        left[4].max(right[4]),
        left[5].max(right[5]),
    ]
}

pub(crate) fn segment_segment_distance_3d(
    p1: [f64; 3],
    q1: [f64; 3],
    p2: [f64; 3],
    q2: [f64; 3],
) -> crate::error::Result<f64> {
    linear::segment_segment_distance_3d(p1, q1, p2, q2)
}

pub(crate) fn distance_3d_with_parts(
    left: &Distance3dParts,
    right: &Distance3dParts,
) -> crate::error::Result<f64> {
    if left.segments.is_empty() || right.segments.is_empty() {
        return Ok(f64::INFINITY);
    }
    let (probe, target) = if left.segments.len() <= right.segments.len() {
        (left, right)
    } else {
        (right, left)
    };
    let mut best = f64::INFINITY;
    if let Some(bvh) = target.bvh() {
        return bvh.distance_to_parts(probe, &target.segments, best);
    }
    for &left_segment in &probe.segments {
        for &right_segment in &target.segments {
            let bound = aabb3_distance_lower_bound(left_segment.aabb, right_segment.aabb);
            if bound >= best {
                continue;
            }
            let distance = segment_segment_distance_3d(
                left_segment.start,
                left_segment.end,
                right_segment.start,
                right_segment.end,
            )?;
            if distance < best {
                best = distance;
                if best == 0.0 {
                    return Ok(0.0);
                }
            }
        }
    }
    Ok(best)
}

#[cfg(test)]
pub(crate) fn distance_3d_brute_parts(
    left: &Distance3dParts,
    right: &Distance3dParts,
) -> crate::error::Result<f64> {
    let mut best = f64::INFINITY;
    for &left_segment in &left.segments {
        for &right_segment in &right.segments {
            let distance = segment_segment_distance_3d(
                left_segment.start,
                left_segment.end,
                right_segment.start,
                right_segment.end,
            )?;
            if distance < best {
                best = distance;
                if best == 0.0 {
                    return Ok(0.0);
                }
            }
        }
    }
    Ok(best)
}

#[cfg(test)]
mod bound_tests {
    use std::cmp::Ordering;

    use super::*;
    use crate::geometry::tessellation::exact::{ExactDyadic, round_sqrt_ratio};

    fn exact_gap(a_min: f64, a_max: f64, b_min: f64, b_max: f64) -> ExactDyadic {
        let pair = if a_min > b_max {
            (a_min, b_max)
        } else if b_min > a_max {
            (b_min, a_max)
        } else {
            return ExactDyadic::from_f64(0.0);
        };
        ExactDyadic::from_f64(pair.0)
            .try_subtract(ExactDyadic::from_f64(pair.1))
            .expect("exact endpoint difference")
    }

    fn exact_aabb_distance_floor(a: Aabb3d, b: Aabb3d) -> f64 {
        let gaps = [
            exact_gap(a[0], a[3], b[0], b[3]),
            exact_gap(a[1], a[4], b[1], b[4]),
            exact_gap(a[2], a[5], b[2], b[5]),
        ];
        let mut squared = ExactDyadic::from_f64(0.0);
        for gap in &gaps {
            squared = squared
                .try_add(gap.try_product(gap).expect("exact gap square"))
                .expect("exact squared sum");
        }
        let rounded =
            round_sqrt_ratio(&squared, &ExactDyadic::from_f64(1.0)).expect("exact AABB root");
        if rounded.is_infinite() {
            return f64::MAX;
        }
        let rounded_exact = ExactDyadic::from_f64(rounded);
        let rounded_squared = rounded_exact
            .try_product(&rounded_exact)
            .expect("rounded root square");
        if rounded_squared.cmp(&squared) == Ordering::Greater {
            rounded.next_down()
        } else {
            rounded
        }
    }

    fn point_segment(point: [f64; 3]) -> Segment3d {
        Segment3d {
            start: point,
            end: point,
            aabb: segment_aabb3(point, point),
        }
    }

    fn parts(segments: impl IntoIterator<Item = Segment3d>) -> Distance3dParts {
        Distance3dParts {
            segments: segments.into_iter().collect(),
            bvh: OnceLock::new(),
        }
    }

    fn assert_bound_is_below_exact(name: &str, a: Aabb3d, b: Aabb3d) {
        let exact = exact_aabb_distance_floor(a, b);
        let actual = aabb3_distance_lower_bound(a, b);
        assert!(
            !actual.is_nan(),
            "{name}: lower bound is NaN, exact={exact:?}"
        );
        assert!(
            actual <= exact,
            "{name}: lower={actual:?} exact_floor={exact:?} a={a:?} b={b:?}"
        );
        if exact == 0.0 {
            assert_eq!(actual.to_bits(), 0.0_f64.to_bits(), "{name}: zero overlap");
        }
    }

    #[test]
    fn counterexample_gap_is_never_overestimated() {
        let gap = f64::from_bits(0x1E58_0000_0000_0000);
        let a = [gap, 0.0, 0.0, gap, 0.0, 0.0];
        let b = [0.0; 6];
        let old_squared = gap * gap;
        let old = old_squared.sqrt().next_down().max(0.0);
        let exact = exact_aabb_distance_floor(a, b);
        let actual = aabb3_distance_lower_bound(a, b);

        assert_eq!(gap.to_bits(), 0x1E58_0000_0000_0000);
        assert_eq!(exact.to_bits(), gap.to_bits());
        assert_eq!(old_squared.to_bits(), 1);
        assert_eq!(old.to_bits(), 0x1E5F_FFFF_FFFF_FFFF);
        assert!(old > exact, "the former bound must reproduce the defect");
        assert!(actual <= exact, "new bound must not overestimate the gap");
    }

    #[test]
    fn bound_covers_overlap_gaps_extrema_and_signed_zero() {
        let min_subnormal = f64::from_bits(1);
        let min_normal = f64::from_bits(0x0010_0000_0000_0000);
        let huge_origin = 2.0_f64.powi(900);
        let cases = [
            ("overlap", [0.0, 0.0, 0.0, 1.0, 1.0, 1.0], [
                0.5, 0.5, 0.5, 2.0, 2.0, 2.0,
            ]),
            ("touch", [0.0, 0.0, 0.0, 1.0, 1.0, 1.0], [
                1.0, 1.0, 1.0, 2.0, 2.0, 2.0,
            ]),
            ("one_gap", [0.0, 0.0, 0.0, 1.0, 1.0, 1.0], [
                3.0, 0.0, 0.0, 4.0, 1.0, 1.0,
            ]),
            ("reverse_gap", [3.0, 0.0, 0.0, 4.0, 1.0, 1.0], [
                0.0, 0.0, 0.0, 1.0, 1.0, 1.0,
            ]),
            ("two_gaps", [0.0, 0.0, 0.0, 1.0, 1.0, 1.0], [
                3.0, 4.0, 0.0, 4.0, 5.0, 1.0,
            ]),
            ("three_gaps", [0.0, 0.0, 0.0, 1.0, 1.0, 1.0], [
                3.0, 4.0, 5.0, 4.0, 5.0, 6.0,
            ]),
            (
                "minimum_subnormal",
                [min_subnormal, 0.0, 0.0, min_subnormal, 0.0, 0.0],
                [0.0; 6],
            ),
            (
                "minimum_normal",
                [min_normal, 0.0, 0.0, min_normal, 0.0, 0.0],
                [0.0; 6],
            ),
            (
                "huge_cancellation",
                [
                    huge_origin.next_up(),
                    huge_origin.next_up(),
                    huge_origin.next_up(),
                    huge_origin.next_up(),
                    huge_origin.next_up(),
                    huge_origin.next_up(),
                ],
                [
                    huge_origin,
                    huge_origin,
                    huge_origin,
                    huge_origin,
                    huge_origin,
                    huge_origin,
                ],
            ),
            ("maximum_opposite_signs", [f64::MAX; 6], [-f64::MAX; 6]),
            (
                "mixed_exponents",
                [
                    min_subnormal,
                    1.0,
                    2.0_f64.powi(500),
                    min_subnormal,
                    1.0,
                    2.0_f64.powi(500),
                ],
                [0.0; 6],
            ),
            (
                "signed_zero",
                [-0.0, -0.0, -0.0, -0.0, -0.0, -0.0],
                [0.0; 6],
            ),
        ];

        for (name, a, b) in cases {
            assert_bound_is_below_exact(name, a, b);
        }
    }

    fn adversarial_boundary_values() -> Vec<f64> {
        let fractions = [0, 1, 2, (1_u64 << 52) - 2, (1_u64 << 52) - 1];
        let exponents = [0, 1, 2, 0x3FE, 0x3FF, 0x400, 0x7FD, 0x7FE];
        let mut values = Vec::new();
        for exponent in exponents {
            for fraction in fractions {
                let value = f64::from_bits((exponent << 52) | fraction);
                if value.is_finite() && value > 0.0 {
                    values.push(value);
                }
            }
        }
        values
    }

    #[test]
    fn boundary_bit_sweep_stays_below_exact_box_norm() {
        let zero = [0.0; 6];
        let values = adversarial_boundary_values();
        for (index, value) in values.iter().copied().enumerate() {
            let signed = if index.is_multiple_of(2) {
                value
            } else {
                -value
            };
            let point = [signed, signed, signed, signed, signed, signed];
            assert_bound_is_below_exact("signed boundary point", point, zero);

            let successor = value.next_up();
            if successor.is_finite() {
                let successor_point = [
                    successor, successor, successor, successor, successor, successor,
                ];
                let origin_point = [value, value, value, value, value, value];
                assert_bound_is_below_exact(
                    "successor cancellation",
                    successor_point,
                    origin_point,
                );
            }
        }

        for index in (0..values.len()).step_by(3) {
            let point = [
                values[index],
                values[(index + 1) % values.len()],
                values[(index + 2) % values.len()],
            ];
            let point_aabb = [point[0], point[1], point[2], point[0], point[1], point[2]];
            assert_bound_is_below_exact("mixed boundary ratios", point_aabb, zero);
        }
    }

    #[test]
    fn downward_bound_weakening_stays_small_for_normal_boxes() {
        let cases: [(Aabb3d, Aabb3d); 3] = [
            ([0.0, 0.0, 0.0, 1.0, 1.0, 1.0], [
                3.0, 4.0, 5.0, 4.0, 5.0, 6.0,
            ]),
            ([0.0, 0.0, 0.0, 1.0, 1.0, 1.0], [
                10.0, 20.0, 30.0, 11.0, 21.0, 31.0,
            ]),
            ([0.0, 0.0, 0.0, 1.0, 1.0, 1.0], [
                3.0, 0.0, 0.0, 4.0, 1.0, 1.0,
            ]),
        ];
        for (a, b) in cases {
            let old_gaps = (
                (a[0] - b[3]).max(b[0] - a[3]).max(0.0),
                (a[1] - b[4]).max(b[1] - a[4]).max(0.0),
                (a[2] - b[5]).max(b[2] - a[5]).max(0.0),
            );
            let old = (old_gaps.0 * old_gaps.0 + old_gaps.1 * old_gaps.1 + old_gaps.2 * old_gaps.2)
                .sqrt()
                .next_down()
                .max(0.0);
            let new = aabb3_distance_lower_bound(a, b);
            assert!(new > 0.0);
            assert!(
                new <= old,
                "new bound should not exceed the old normal-range bound: new={new:?} old={old:?}"
            );
        }
    }

    #[test]
    fn downward_helpers_keep_zero_and_exact_ratio_identities() {
        let value = 3.0;
        assert_eq!(
            downward_positive_difference(value, 0.0).to_bits(),
            value.next_down().to_bits()
        );
        assert_eq!(downward_ratio(0.0, value).to_bits(), 0.0_f64.to_bits());
        assert_eq!(downward_ratio(value, value).to_bits(), 1.0_f64.to_bits());
        assert_eq!(downward_square(0.0).to_bits(), 0.0_f64.to_bits());
        assert_eq!(downward_add(0.0, value).to_bits(), value.to_bits());
        assert_eq!(downward_add(value, 0.0).to_bits(), value.to_bits());
        assert_eq!(downward_sqrt(0.0).to_bits(), 0.0_f64.to_bits());
        assert_eq!(downward_scale(0.0, value).to_bits(), 0.0_f64.to_bits());

        assert_eq!(
            downward_positive_difference(f64::INFINITY, 1.0).to_bits(),
            f64::MAX.to_bits()
        );
        assert_eq!(
            downward_ratio(f64::INFINITY, 1.0).to_bits(),
            f64::MAX.to_bits()
        );
        assert_eq!(downward_square(f64::INFINITY).to_bits(), f64::MAX.to_bits());
        assert_eq!(downward_square(f64::MAX).to_bits(), f64::MAX.to_bits());
        assert_eq!(
            downward_add(0.0, f64::INFINITY).to_bits(),
            f64::MAX.to_bits()
        );
        assert_eq!(
            downward_add(f64::INFINITY, 1.0).to_bits(),
            f64::MAX.to_bits()
        );
        assert_eq!(
            downward_add(f64::MAX, f64::MAX).to_bits(),
            f64::MAX.to_bits()
        );
        assert_eq!(downward_sqrt(f64::INFINITY).to_bits(), f64::MAX.to_bits());
        assert_eq!(
            downward_scale(1.0, f64::INFINITY).to_bits(),
            f64::MAX.to_bits()
        );
        assert_eq!(downward_scale(f64::MAX, 2.0).to_bits(), f64::MAX.to_bits());
    }

    #[test]
    fn downward_helpers_are_monotone_on_nonnegative_inputs() {
        let values = [
            0.0,
            f64::from_bits(1),
            f64::MIN_POSITIVE,
            1.0,
            2.0,
            f64::MAX,
        ];
        // The exact add-zero identity intentionally creates a one-ulp
        // discontinuity at zero; test monotonicity away from that identity.
        for (index, &left) in values.iter().enumerate() {
            for &right in &values[index..] {
                assert!(downward_square(left) <= downward_square(right));
                assert!(downward_sqrt(left) <= downward_sqrt(right));
                assert!(downward_scale(left, 3.0) <= downward_scale(right, 3.0));
                if left != 0.0 {
                    assert!(downward_add(left, 1.0) <= downward_add(right, 1.0));
                }
                assert!(downward_ratio(left, f64::MAX) <= downward_ratio(right, f64::MAX));
            }
        }
        for &smaller in &values {
            for (index, &left) in values.iter().enumerate() {
                for &right in &values[index..] {
                    assert!(
                        downward_positive_difference(left, smaller)
                            <= downward_positive_difference(right, smaller)
                    );
                }
            }
        }
        for &larger in &values {
            for (index, &smaller) in values.iter().enumerate() {
                for &right in &values[..=index] {
                    assert!(
                        downward_positive_difference(larger, smaller)
                            <= downward_positive_difference(larger, right)
                    );
                }
            }
        }
    }

    #[test]
    fn indexed_distance_matches_exhaustive_for_segment_shapes() {
        let cases = [
            (
                "skew",
                ([0.0, 0.0, 0.0], [1.0, 0.0, 0.0]),
                ([0.0, 1.0, 1.0], [1.0, 1.0, 2.0]),
            ),
            (
                "parallel",
                ([0.0, 0.0, 0.0], [1.0, 0.0, 0.0]),
                ([0.0, 2.0, 1.0], [1.0, 2.0, 1.0]),
            ),
            (
                "interior",
                ([0.0, 0.0, 0.0], [10.0, 0.0, 0.0]),
                ([5.0, -2.0, 1.0], [5.0, 2.0, 1.0]),
            ),
            (
                "endpoint",
                ([0.0, 0.0, 0.0], [0.0, 0.0, 0.0]),
                ([2.0, 1.0, 1.0], [4.0, 3.0, 3.0]),
            ),
            (
                "reversed",
                ([10.0, 0.0, 0.0], [0.0, 0.0, 0.0]),
                ([5.0, 2.0, 1.0], [5.0, -2.0, 1.0]),
            ),
            (
                "mixed_exponents",
                ([0.0, 0.0, 0.0], [2.0_f64.powi(-500), 0.0, 0.0]),
                ([0.0, 2.0_f64.powi(-450), 2.0_f64.powi(-500)], [
                    0.0,
                    2.0_f64.powi(-449),
                    2.0_f64.powi(-500),
                ]),
            ),
            (
                "point_degenerate",
                ([0.0, 0.0, 0.0], [0.0, 0.0, 0.0]),
                ([3.0, 4.0, 12.0], [3.0, 4.0, 12.0]),
            ),
            (
                "permuted",
                ([0.0, 0.0, 0.0], [1.0, 0.0, 0.0]),
                ([0.0, 1.0, 1.0], [1.0, 1.0, 2.0]),
            ),
        ];

        for (case_index, (name, (probe_start, probe_end), (target_start, target_end))) in
            cases.into_iter().enumerate()
        {
            for target_count in [
                1,
                BVH_MIN_INDEXED_SEGMENTS - 1,
                BVH_MIN_INDEXED_SEGMENTS,
                BVH_MIN_INDEXED_SEGMENTS + 1,
                128,
            ] {
                let mut targets = Vec::with_capacity(target_count);
                for index in 0..target_count.saturating_sub(1) {
                    let coordinate = 1_000.0 + 10.0 * index as f64;
                    let start = [coordinate, coordinate + 1.0, coordinate + 2.0];
                    let end = [coordinate + 3.0, coordinate + 4.0, coordinate + 5.0];
                    targets.push(Segment3d {
                        start,
                        end,
                        aabb: segment_aabb3(start, end),
                    });
                }
                let target = Segment3d {
                    start: target_start,
                    end: target_end,
                    aabb: segment_aabb3(target_start, target_end),
                };
                if case_index.is_multiple_of(2) {
                    targets.push(target);
                } else {
                    targets.insert(0, target);
                }
                let probe = Segment3d {
                    start: probe_start,
                    end: probe_end,
                    aabb: segment_aabb3(probe_start, probe_end),
                };
                let left = parts([probe]);
                let right = parts(targets);
                let expected =
                    distance_3d_brute_parts(&left, &right).expect("exhaustive segment distance");
                let actual =
                    distance_3d_with_parts(&left, &right).expect("indexed segment distance");
                assert_eq!(
                    actual.to_bits(),
                    expected.to_bits(),
                    "{name} target_count={target_count}"
                );
            }
        }
    }

    #[test]
    fn indexed_distance_does_not_prune_subnormal_nearest_node() {
        let gap = f64::from_bits(0x1E58_0000_0000_0000);
        let middle = f64::from_bits(0x1E5C_0000_0000_0000);
        let middle_coordinate = middle * 2.0_f64.sqrt();
        let probe = parts([point_segment([0.0, middle_coordinate, 0.0])]);
        let nearest = Segment3d {
            start: [gap, -100.0, 0.0],
            end: [gap, 100.0, 0.0],
            aabb: segment_aabb3([gap, -100.0, 0.0], [gap, 100.0, 0.0]),
        };
        let middle_segment = Segment3d {
            start: [-1.0, -1.0, 0.0],
            end: [1.0, 1.0, 0.0],
            aabb: segment_aabb3([-1.0, -1.0, 0.0], [1.0, 1.0, 0.0]),
        };
        let mut targets = vec![nearest];
        targets.extend(std::iter::repeat_with(|| point_segment([1.0, -100.0, 0.0])).take(31));
        targets.push(middle_segment);
        targets.extend(std::iter::repeat_with(|| point_segment([1.0, 0.0, 0.0])).take(31));
        let target = parts(targets);
        let expected = distance_3d_brute_parts(&probe, &target).expect("exhaustive distance");
        assert_eq!(expected.to_bits(), gap.to_bits());
        let old_bound = (gap * gap).sqrt().next_down().max(0.0);
        assert!(middle > gap && middle < old_bound);

        traversal_counters::reset();
        let actual = distance_3d_with_parts(&probe, &target).expect("indexed distance");
        let counters = traversal_counters::snapshot();

        assert_eq!(actual.to_bits(), expected.to_bits());
        assert!(counters.pruned_nodes > 0);
    }

    #[test]
    fn indexed_distance_prunes_ordinary_separated_points() {
        let target_count = 128;
        let target = parts(
            (0..target_count)
                .map(|index| point_segment([10.0 + 10.0 * f64::from(index), 0.0, 0.0])),
        );
        let probe = parts([point_segment([0.0, 0.0, 0.0])]);
        let expected = distance_3d_brute_parts(&probe, &target).expect("exhaustive distance");

        traversal_counters::reset();
        let actual = distance_3d_with_parts(&probe, &target).expect("indexed distance");
        let counters = traversal_counters::snapshot();

        assert_eq!(actual.to_bits(), expected.to_bits());
        assert!(counters.bound_calls > 0);
        assert!(counters.pruned_nodes > 0);
        assert!(
            counters.visited_leaves < target_count as u64,
            "ordinary separated data should prune leaves: {counters:?}"
        );
    }
}

#[cfg(test)]
mod bvh_cache_tests {
    use super::*;

    #[test]
    fn cached_3d_bvh_reuses_storage_and_matches_brute_force() {
        let fixture = || Distance3dParts {
            segments: (0..=BVH_MIN_INDEXED_SEGMENTS)
                .map(|index| {
                    let start = [index as f64, 0.0, 0.0];
                    let end = [index as f64, 1.0, 1.0];
                    Segment3d {
                        start,
                        end,
                        aabb: segment_aabb3(start, end),
                    }
                })
                .collect(),
            bvh: OnceLock::new(),
        };
        let target = fixture();
        let first = target.bvh().unwrap();
        let cached = crate::test_support::assert_no_allocations(|| target.bvh()).unwrap();
        assert!(std::ptr::eq(first, cached));
        let probe = Distance3dParts {
            segments: vec![Segment3d {
                start: [0.0, 0.0, 3.0],
                end: [0.0, 0.0, 3.0],
                aabb: segment_aabb3([0.0, 0.0, 3.0], [0.0, 0.0, 3.0]),
            }],
            bvh: OnceLock::new(),
        };
        let cold = fixture();
        assert_eq!(
            distance_3d_with_parts(&probe, &cold).unwrap(),
            distance_3d_brute_parts(&probe, &cold).unwrap()
        );
    }
}
