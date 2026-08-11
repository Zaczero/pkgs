#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::sync::OnceLock;

use crate::HeapSize;
use crate::geometry::facet_bvh::BVH_MIN_INDEXED_SEGMENTS;
use crate::geometry::{BvhCode, BvhNode, CoordSeq, Shape};

type Aabb3d = [f64; 6];

#[derive(Clone, Copy)]
pub(crate) struct Segment3d {
    pub start: [f64; 3],
    pub end: [f64; 3],
    pub aabb: Aabb3d,
}

pub(crate) struct Distance3dParts {
    pub(crate) segments: Box<[Segment3d]>,
    bvh: OnceLock<Option<SegmentBvh3d>>,
}

struct SegmentBvh3d {
    aabbs: Box<[Aabb3d]>,
    codes: Box<[u32]>,
}

impl Distance3dParts {
    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(crate) fn heap_bytes(&self) -> usize {
        HeapSize::heap_bytes(self)
    }
}

crate::heapless!(Segment3d);

impl HeapSize for SegmentBvh3d {
    fn heap_bytes(&self) -> usize {
        self.aabbs.heap_bytes() + self.codes.heap_bytes()
    }
}

impl HeapSize for Distance3dParts {
    fn heap_bytes(&self) -> usize {
        self.segments.heap_bytes() + self.bvh.heap_bytes()
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

fn aabb3_gaps(a: Aabb3d, b: Aabb3d) -> (f64, f64, f64) {
    (
        (a[0] - b[3]).max(b[0] - a[3]).max(0.0),
        (a[1] - b[4]).max(b[1] - a[4]).max(0.0),
        (a[2] - b[5]).max(b[2] - a[5]).max(0.0),
    )
}

/// Conservative lower-bound distance between two 3D AABBs (0 when overlapping).
pub(crate) fn aabb3_distance_lower_bound(a: Aabb3d, b: Aabb3d) -> f64 {
    let (dx, dy, dz) = aabb3_gaps(a, b);
    let squared = dx * dx + dy * dy + dz * dz;
    let distance = if squared.is_finite() {
        squared.sqrt()
    } else {
        dx.hypot(dy).hypot(dz)
    };
    distance.next_down().max(0.0)
}

fn push_segments_3d(coords: &CoordSeq, out: &mut Vec<Segment3d>) {
    let p3 = |index: usize| {
        let point = coords.point_at(index);
        [point.x, point.y, point.z().expect("Z column present")]
    };
    match coords.len() {
        0 => {},
        1 => {
            let point = p3(0);
            out.push(Segment3d {
                start: point,
                end: point,
                aabb: segment_aabb3(point, point),
            });
        },
        n => {
            for index in 0..n - 1 {
                let (start, end) = (p3(index), p3(index + 1));
                out.push(Segment3d {
                    start,
                    end,
                    aabb: segment_aabb3(start, end),
                });
            }
        },
    }
}

fn collect_segments_3d(shape: &Shape, out: &mut Vec<Segment3d>) {
    match shape {
        Shape::Point(point) => {
            let p = [point.x, point.y, point.z().expect("Z present")];
            out.push(Segment3d {
                start: p,
                end: p,
                aabb: segment_aabb3(p, p),
            });
        },
        Shape::MultiPoint(coords) => {
            for index in 0..coords.len() {
                let point = coords.point_at(index);
                let p = [point.x, point.y, point.z().expect("Z present")];
                out.push(Segment3d {
                    start: p,
                    end: p,
                    aabb: segment_aabb3(p, p),
                });
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
        let mut segments = Vec::new();
        collect_segments_3d(shape, &mut segments);
        Self {
            segments: segments.into_boxed_slice(),
            bvh: OnceLock::new(),
        }
    }

    fn bvh(&self) -> Option<&SegmentBvh3d> {
        self.bvh
            .get_or_init(|| {
                (self.segments.len() >= BVH_MIN_INDEXED_SEGMENTS)
                    .then(|| SegmentBvh3d::build(&self.segments))
                    .flatten()
            })
            .as_ref()
    }
}

impl SegmentBvh3d {
    fn build(segments: &[Segment3d]) -> Option<Self> {
        let segment_count = u32::try_from(segments.len()).ok()?;
        if segment_count == 0 || segment_count > BvhCode::LEAF_BIT / 2 {
            return None;
        }
        let segment_aabbs: Vec<Aabb3d> = segments.iter().map(|segment| segment.aabb).collect();
        let mut ids: Vec<u32> = (0..segment_count).collect();
        let node_count = 2 * segment_count as usize - 1;
        let mut tree = Self {
            aabbs: vec![[0.0; 6]; node_count].into_boxed_slice(),
            codes: vec![0; node_count].into_boxed_slice(),
        };
        let mut next = 1;
        tree.fill_node(0, &mut next, &segment_aabbs, &mut ids);
        Some(tree)
    }

    fn fill_node(
        &mut self,
        node: usize,
        next: &mut usize,
        segment_aabbs: &[Aabb3d],
        ids: &mut [u32],
    ) {
        if ids.len() == 1 {
            self.aabbs[node] = segment_aabbs[ids[0] as usize];
            self.codes[node] = BvhCode::leaf(ids[0] as usize).raw();
            return;
        }
        let aabb = ids
            .iter()
            .map(|&id| segment_aabbs[id as usize])
            .reduce(union_aabb3)
            .expect("non-empty ids");
        let axis = longest_axis(aabb);
        let pivot = ids.len() / 2;
        ids.select_nth_unstable_by(pivot, |&left, &right| {
            segment_aabbs[left as usize][axis].total_cmp(&segment_aabbs[right as usize][axis])
        });
        let (lower, upper) = ids.split_at_mut(pivot);
        let first_child = *next;
        *next += 2;
        self.aabbs[node] = aabb;
        self.codes[node] = BvhCode::internal(first_child as u32).raw();
        self.fill_node(first_child, next, segment_aabbs, lower);
        self.fill_node(first_child + 1, next, segment_aabbs, upper);
    }

    fn distance_to_parts(
        &self,
        probe_parts: &Distance3dParts,
        target_segments: &[Segment3d],
        mut best: f64,
    ) -> f64 {
        let mut stack = Vec::new();
        for &probe in &probe_parts.segments {
            self.visit_with_targets(probe, target_segments, &mut stack, &mut best);
            if best == 0.0 {
                return 0.0;
            }
        }
        best
    }

    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
    )]
    fn visit_with_targets(
        &self,
        probe: Segment3d,
        targets: &[Segment3d],
        stack: &mut Vec<usize>,
        best: &mut f64,
    ) {
        stack.clear();
        stack.push(0);
        while let Some(node) = stack.pop() {
            if aabb3_distance_lower_bound(self.aabbs[node], probe.aabb) >= *best {
                continue;
            }
            match BvhCode::new(self.codes[node]).decode() {
                BvhNode::Leaf(index) => {
                    let target = targets[index];
                    let distance = segment_segment_distance_3d(
                        probe.start,
                        probe.end,
                        target.start,
                        target.end,
                    );
                    if distance < *best {
                        *best = distance;
                    }
                },
                BvhNode::Internal(first_child) => {
                    self.push_nearer_last(stack, first_child, probe.aabb);
                },
            }
        }
    }

    fn push_nearer_last(&self, stack: &mut Vec<usize>, first_child: usize, probe_aabb: Aabb3d) {
        let left_bound = aabb3_distance_lower_bound(self.aabbs[first_child], probe_aabb);
        let right_bound = aabb3_distance_lower_bound(self.aabbs[first_child + 1], probe_aabb);
        if left_bound <= right_bound {
            stack.push(first_child + 1);
            stack.push(first_child);
        } else {
            stack.push(first_child);
            stack.push(first_child + 1);
        }
    }
}

fn longest_axis(aabb: Aabb3d) -> usize {
    let spans = [aabb[3] - aabb[0], aabb[4] - aabb[1], aabb[5] - aabb[2]];
    spans
        .iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| left.total_cmp(right))
        .map_or(0, |(axis, _)| axis)
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
) -> f64 {
    crate::geometry::linear::segment_segment_distance_3d(p1, q1, p2, q2)
}

pub(crate) fn distance_3d_with_parts(left: &Distance3dParts, right: &Distance3dParts) -> f64 {
    if left.segments.is_empty() || right.segments.is_empty() {
        return f64::INFINITY;
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
            );
            if distance < best {
                best = distance;
                if best == 0.0 {
                    return 0.0;
                }
            }
        }
    }
    best
}

#[cfg(test)]
pub(crate) fn distance_3d_brute_parts(left: &Distance3dParts, right: &Distance3dParts) -> f64 {
    let mut best = f64::INFINITY;
    for &left_segment in &left.segments {
        for &right_segment in &right.segments {
            let distance = segment_segment_distance_3d(
                left_segment.start,
                left_segment.end,
                right_segment.start,
                right_segment.end,
            );
            if distance < best {
                best = distance;
                if best == 0.0 {
                    return 0.0;
                }
            }
        }
    }
    best
}
