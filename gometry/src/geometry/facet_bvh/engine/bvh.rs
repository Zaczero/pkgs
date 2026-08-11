use std::ops::ControlFlow;

use crate::HeapSize;
use crate::geometry::facet_bvh::{
    Facet, PreparedLinework, aabb_distance, aabb_distance_squared, aabb_max_distance_squared,
    aabbs_overlap, point_aabb, segment_aabb, union_aabb,
};
use crate::geometry::{
    BvhCode, BvhNode, Point, Segment, SquaredDistanceKey, point_on_segment,
    point_segment_distance_squared, point_segment_selection, segments_intersect,
};

/// One nearest-pair candidate: a probe vertex, its projection onto the
/// target linework, and the squared distance between them.
pub(crate) struct NearestCandidate {
    pub(in crate::geometry) probe: Point,
    pub(in crate::geometry) target: Point,
    pub(in crate::geometry) distance_squared: f64,
    pub(in crate::geometry) distance_key: SquaredDistanceKey,
}

/// Scalar-refine one facet for the argmin sweeps: full-ordinate segments,
/// exact projection (Z/M interpolated like the brute path). The COMPARISON
/// distance and exact-selection key come from [`point_segment_selection`].
/// The projected witness point is built only for an improving candidate.
pub(super) fn refine_facet_nearest(
    linework: &PreparedLinework,
    facet: Facet,
    probe: Point,
    best: &mut Option<NearestCandidate>,
) {
    for index in 0..facet.segment_count as usize {
        let (start, end) = linework.vertex_pair_full(facet, index);
        let segment = Segment {
            start: start.xy(),
            end: end.xy(),
        };
        let selection = point_segment_selection(probe.xy(), segment);
        if best.as_ref().is_none_or(|candidate| {
            selection
                .cmp(probe.xy(), segment, &candidate.distance_key)
                .is_lt()
        }) {
            let projection = selection.projection(probe.xy(), segment);
            let target = projection.interpolate_point(start, end);
            *best = Some(NearestCandidate {
                probe,
                target,
                distance_squared: selection.distance_squared,
                distance_key: selection.distance_key(probe.xy(), segment),
            });
        }
    }
}

/// Flat binary AABB tree over a [`PreparedLinework`]'s facets,
/// longest-axis-median built. `2F − 1` nodes for `F` facets, root at 0.
///
/// Layout is traversal-driven: the AABBs live in their own dense array
/// (32 B each — the only bytes the pruned majority of node visits touch,
/// two per cache line with no straddle), the per-node code words in a
/// second; and a node's two children are ALLOCATED ADJACENTLY, so the
/// nearer-child ordering reads both child bounds from neighboring memory
/// instead of two distant tree regions.
pub(crate) struct FacetBvh {
    aabbs: Box<[[f64; 4]]>,
    codes: Box<[u32]>,
}

impl FacetBvh {
    /// Build over `linework`'s facets; `None` when it has none — or so many
    /// that a NODE index (`2F − 1` of them) could reach [`BvhCode::LEAF_BIT`] and a
    /// first-child code would read as a leaf (those astronomically large
    /// shapes keep the flat facet scans, which are index-width-agnostic).
    pub(crate) fn build(linework: &PreparedLinework) -> Option<Self> {
        let facet_count = u32::try_from(linework.facets.len()).ok()?;
        if facet_count == 0 || facet_count > BvhCode::LEAF_BIT / 2 {
            return None;
        }
        let facet_aabbs: Vec<[f64; 4]> = linework
            .facets
            .iter()
            .map(|&facet| linework.facet_aabb(facet))
            .collect();
        let mut ids: Vec<u32> = (0..facet_count).collect();
        let node_count = 2 * facet_count as usize - 1;
        let mut tree = Self {
            aabbs: vec![[0.0; 4]; node_count].into_boxed_slice(),
            codes: vec![0; node_count].into_boxed_slice(),
        };
        // Root at 0; children allocated in adjacent pairs from the cursor.
        let mut next = 1;
        tree.fill_node(0, &mut next, &facet_aabbs, &mut ids);
        Some(tree)
    }

    /// Longest-axis median split of `ids` (facet indices into `facet_aabbs`)
    /// into the node slot `node`, allocating child pairs from `next`.
    fn fill_node(
        &mut self,
        node: usize,
        next: &mut usize,
        facet_aabbs: &[[f64; 4]],
        ids: &mut [u32],
    ) {
        if let [id] = ids {
            self.aabbs[node] = facet_aabbs[*id as usize];
            self.codes[node] = BvhCode::leaf(*id as usize).raw();
            return;
        }
        let aabb = ids
            .iter()
            .map(|&id| facet_aabbs[id as usize])
            .reduce(union_aabb)
            .expect("split halves are non-empty");
        // Median split on facet centers along the box's longer axis; ties
        // break by facet index so the build is deterministic.
        let axis = usize::from(aabb[3] - aabb[1] > aabb[2] - aabb[0]);
        let center = |id: u32| {
            let facet = facet_aabbs[id as usize];
            facet[axis] + facet[axis + 2]
        };
        let mid = ids.len() / 2;
        ids.select_nth_unstable_by(mid, |&a, &b| {
            center(a).total_cmp(&center(b)).then_with(|| a.cmp(&b))
        });
        let (lower, upper) = ids.split_at_mut(mid);
        let first_child = *next;
        *next += 2;
        self.aabbs[node] = aabb;
        self.codes[node] = BvhCode::internal(first_child as u32).raw();
        self.fill_node(first_child, next, facet_aabbs, lower);
        self.fill_node(first_child + 1, next, facet_aabbs, upper);
    }

    /// Minimum squared (or `hypot`) distance from a probe vertex set to the
    /// linework — one branch-and-bound descent per probe over a single
    /// shared traversal stack, carrying `best` across the whole sweep so
    /// every later probe prunes on what earlier ones learned. `SQUARED`
    /// selects squared-space bounds + the SIMD facet kernel; `false` runs
    /// overflow-safe `hypot` space throughout.
    pub(crate) fn min_points_distance<const SQUARED: bool>(
        &self,
        linework: &PreparedLinework,
        probes: impl Iterator<Item = (f64, f64)>,
        mut best: f64,
    ) -> f64 {
        let mut stack: Vec<u32> = Vec::new();
        for (x, y) in probes {
            best = self.min_point_distance_with_stack::<SQUARED>(linework, x, y, &mut stack, best);
        }
        best
    }

    /// One probe's branch-and-bound descent with witness segment index.
    pub(crate) fn min_point_distance_with_witness_stack<const SQUARED: bool>(
        &self,
        linework: &PreparedLinework,
        x: f64,
        y: f64,
        stack: &mut Vec<u32>,
        mut best: f64,
    ) -> (f64, u32) {
        let probe = point_aabb(x, y);
        let bound = |aabb: [f64; 4]| {
            if SQUARED {
                aabb_distance_squared(aabb, probe)
            } else {
                aabb_distance(aabb, probe)
            }
        };
        let mut witness = u32::MAX;
        stack.clear();
        stack.push(0);
        while let Some(index) = stack.pop() {
            if bound(self.aabbs[index as usize]) >= best {
                continue;
            }
            match BvhCode::new(self.codes[index as usize]).decode() {
                BvhNode::Leaf(facet_index) => {
                    let facet = linework.facets[facet_index];
                    let candidate = if SQUARED {
                        linework.facet_point_distance_squared(facet, x, y)
                    } else {
                        linework.facet_point_distance(facet, Point::new_unchecked_xy(x, y))
                    };
                    if candidate < best {
                        best = candidate;
                        witness = linework
                            .facet_point_distance_with_witness(facet, facet_index, x, y)
                            .1;
                    }
                },
                BvhNode::Internal(first_child) => {
                    self.push_nearer_last(stack, first_child as u32, bound);
                },
            }
        }
        (best, witness)
    }

    /// Batch BVH point queries with per-probe witness segment indices.
    pub(crate) fn batch_min_point_distance_with_witness<const SQUARED: bool>(
        &self,
        linework: &PreparedLinework,
        probes: &[(f64, f64)],
        out_dist: &mut [f64],
        out_witness: &mut [u32],
        stack: &mut Vec<u32>,
    ) {
        debug_assert_eq!(probes.len(), out_dist.len());
        debug_assert_eq!(probes.len(), out_witness.len());
        for (index, &(x, y)) in probes.iter().enumerate() {
            let (distance_squared, witness) = self
                .min_point_distance_with_witness_stack::<SQUARED>(
                    linework,
                    x,
                    y,
                    stack,
                    f64::INFINITY,
                );
            out_dist[index] = distance_squared;
            out_witness[index] = witness;
        }
    }

    /// One probe's branch-and-bound descent over a caller-provided (reused)
    /// stack — the per-row engine behind
    /// [`min_points_distance`](Self::min_points_distance) and the packed
    /// point-array lanes (which need per-row results, not one fold). Returns
    /// `best` folded with this probe's minimum.
    pub(crate) fn min_point_distance_with_stack<const SQUARED: bool>(
        &self,
        linework: &PreparedLinework,
        x: f64,
        y: f64,
        stack: &mut Vec<u32>,
        mut best: f64,
    ) -> f64 {
        let probe = point_aabb(x, y);
        let bound = |aabb: [f64; 4]| {
            if SQUARED {
                aabb_distance_squared(aabb, probe)
            } else {
                aabb_distance(aabb, probe)
            }
        };
        stack.clear();
        stack.push(0);
        while let Some(index) = stack.pop() {
            if bound(self.aabbs[index as usize]) >= best {
                continue;
            }
            match BvhCode::new(self.codes[index as usize]).decode() {
                BvhNode::Leaf(facet_index) => {
                    let facet = linework.facets[facet_index];
                    let candidate = if SQUARED {
                        linework.facet_point_distance_squared(facet, x, y)
                    } else {
                        linework.facet_point_distance(facet, Point::new_unchecked_xy(x, y))
                    };
                    best = best.min(candidate);
                },
                BvhNode::Internal(first_child) => {
                    self.push_nearer_last(stack, first_child as u32, bound);
                },
            }
        }
        best
    }

    /// Whether any probe vertex lies within `limit` (squared, inclusive) of
    /// the linework, over a single shared traversal stack. The AABB upper
    /// bound answers TRUE without exact tests when a whole subtree sits
    /// inside the limit. `simd` enables the vector facet kernel (callers
    /// gate it on squared-space-safe coordinates; the scalar kernel carries
    /// its own extreme-coordinate rescue).
    pub(crate) fn any_points_within(
        &self,
        linework: &PreparedLinework,
        mut probes: impl Iterator<Item = (f64, f64)>,
        limit: f64,
        simd: bool,
    ) -> bool {
        let mut stack: Vec<u32> = Vec::new();
        probes.any(|(x, y)| self.point_within_with_stack(linework, x, y, limit, simd, &mut stack))
    }

    /// One probe's within-limit descent over a caller-provided (reused) stack
    /// — the per-row engine behind
    /// [`any_points_within`](Self::any_points_within) and the packed
    /// point-array lanes.
    pub(crate) fn point_within_with_stack(
        &self,
        linework: &PreparedLinework,
        x: f64,
        y: f64,
        limit: f64,
        simd: bool,
        stack: &mut Vec<u32>,
    ) -> bool {
        let probe = point_aabb(x, y);
        stack.clear();
        stack.push(0);
        while let Some(index) = stack.pop() {
            let aabb = self.aabbs[index as usize];
            if aabb_distance_squared(aabb, probe) > limit {
                continue;
            }
            // Upper-bound early-TRUE: every point of the node's box
            // (hence every segment vertex beneath it) is within limit.
            if aabb_max_distance_squared(aabb, probe) <= limit {
                return true;
            }
            match BvhCode::new(self.codes[index as usize]).decode() {
                BvhNode::Leaf(facet_index) => {
                    let facet = linework.facets[facet_index];
                    let candidate = if simd {
                        linework.facet_point_distance_squared(facet, x, y)
                    } else {
                        let point = Point::new_unchecked_xy(x, y);
                        (0..facet.segment_count as usize).fold(f64::INFINITY, |best, i| {
                            best.min(point_segment_distance_squared(
                                point,
                                linework.segment(facet, i),
                            ))
                        })
                    };
                    if candidate <= limit {
                        return true;
                    }
                },
                BvhNode::Internal(first_child) => {
                    let first_child = first_child as u32;
                    stack.push(first_child);
                    stack.push(first_child + 1);
                },
            }
        }
        false
    }

    /// Visit every box-overlapping segment pair between the two trees'
    /// linework — dual descent with box-OVERLAP pruning (disjoint boxes
    /// cannot contain touching segments), stopping at the visitor's first
    /// `Break`. The crossing and contact-classifying oracles share this
    /// skeleton.
    pub(crate) fn for_each_overlapping_segment_pair<B>(
        &self,
        linework: &PreparedLinework,
        other: &Self,
        other_linework: &PreparedLinework,
        visit: impl FnMut(Segment, Segment) -> ControlFlow<B>,
    ) -> Option<B> {
        let mut stack = Vec::new();
        self.for_each_overlapping_segment_pair_with_stack(
            linework,
            other,
            other_linework,
            &mut stack,
            visit,
        )
    }

    /// [`for_each_overlapping_segment_pair`] with a caller-owned stack so an
    /// outer probe loop can allocate once and `clear` per call.
    pub(crate) fn for_each_overlapping_segment_pair_with_stack<B>(
        &self,
        linework: &PreparedLinework,
        other: &Self,
        other_linework: &PreparedLinework,
        stack: &mut Vec<(u32, u32)>,
        mut visit: impl FnMut(Segment, Segment) -> ControlFlow<B>,
    ) -> Option<B> {
        stack.clear();
        stack.push((0, 0));
        while let Some((left_index, right_index)) = stack.pop() {
            if !aabbs_overlap(
                self.aabbs[left_index as usize],
                other.aabbs[right_index as usize],
            ) {
                continue;
            }
            let left = BvhCode::new(self.codes[left_index as usize]).decode();
            let right = BvhCode::new(other.codes[right_index as usize]).decode();
            match (left, right) {
                (BvhNode::Leaf(left_index), BvhNode::Leaf(right_index)) => {
                    let left_facet = linework.facets[left_index];
                    let right_facet = other_linework.facets[right_index];
                    for i in 0..left_facet.segment_count as usize {
                        let probe = linework.segment(left_facet, i);
                        for j in 0..right_facet.segment_count as usize {
                            if let ControlFlow::Break(value) =
                                visit(probe, other_linework.segment(right_facet, j))
                            {
                                return Some(value);
                            }
                        }
                    }
                },
                (BvhNode::Internal(left_first), _) => {
                    let left_first = left_first as u32;
                    stack.push((left_first, right_index));
                    stack.push((left_first + 1, right_index));
                },
                (BvhNode::Leaf(_), BvhNode::Internal(right_first)) => {
                    let right_first = right_first as u32;
                    stack.push((left_index, right_first));
                    stack.push((left_index, right_first + 1));
                },
            }
        }
        None
    }

    /// Whether any segment of this tree's linework touches or crosses any
    /// segment of `other`'s — the crossing half of the distance/dwithin
    /// disjointness check.
    pub(crate) fn any_segments_intersect(
        &self,
        linework: &PreparedLinework,
        other: &Self,
        other_linework: &PreparedLinework,
    ) -> bool {
        self.for_each_overlapping_segment_pair(linework, other, other_linework, |left, right| {
            if segments_intersect(left, right) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .is_some()
    }

    /// Whether `point` lies ON any linework segment (boundary-inclusive,
    /// exact `point_on_segment` at leaves) — the tree-accelerated
    /// `covers_point` for 1D linework: a point can only lie on a segment
    /// whose box contains it, so the descent prunes by box containment.
    pub(crate) fn covers_point(&self, linework: &PreparedLinework, point: Point) -> bool {
        let mut stack = Vec::new();
        self.covers_point_with_stack(linework, point, &mut stack)
    }

    /// [`covers_point`] with a caller-owned stack for batch probes.
    pub(crate) fn covers_point_with_stack(
        &self,
        linework: &PreparedLinework,
        point: Point,
        stack: &mut Vec<u32>,
    ) -> bool {
        stack.clear();
        stack.push(0);
        while let Some(index) = stack.pop() {
            let aabb = self.aabbs[index as usize];
            if point.x < aabb[0] || point.x > aabb[2] || point.y < aabb[1] || point.y > aabb[3] {
                continue;
            }
            match BvhCode::new(self.codes[index as usize]).decode() {
                BvhNode::Leaf(facet_index) => {
                    let facet = linework.facets[facet_index];
                    for i in 0..facet.segment_count as usize {
                        let segment = linework.segment(facet, i);
                        if point_on_segment(point, segment.start, segment.end) {
                            return true;
                        }
                    }
                },
                BvhNode::Internal(first_child) => {
                    let first_child = first_child as u32;
                    stack.push(first_child);
                    stack.push(first_child + 1);
                },
            }
        }
        false
    }

    /// Visit every linework segment whose box overlaps `probe`'s — the
    /// single-tree sibling of [`for_each_overlapping_segment_pair`] for a
    /// side below the tree crossover.
    pub(crate) fn for_each_segment_overlapping<B>(
        &self,
        linework: &PreparedLinework,
        probe: Segment,
        visit: impl FnMut(Segment) -> ControlFlow<B>,
    ) -> Option<B> {
        let mut stack = Vec::new();
        self.for_each_segment_overlapping_with_stack(linework, probe, &mut stack, visit)
    }

    /// [`for_each_segment_overlapping`] with a caller-owned stack — the one-
    /// sided probe loop in `parts_segments_cross` allocates once and clears.
    pub(crate) fn for_each_segment_overlapping_with_stack<B>(
        &self,
        linework: &PreparedLinework,
        probe: Segment,
        stack: &mut Vec<u32>,
        mut visit: impl FnMut(Segment) -> ControlFlow<B>,
    ) -> Option<B> {
        let probe_aabb = segment_aabb(probe);
        stack.clear();
        stack.push(0);
        while let Some(index) = stack.pop() {
            if !aabbs_overlap(self.aabbs[index as usize], probe_aabb) {
                continue;
            }
            match BvhCode::new(self.codes[index as usize]).decode() {
                BvhNode::Leaf(facet_index) => {
                    let facet = linework.facets[facet_index];
                    for i in 0..facet.segment_count as usize {
                        if let ControlFlow::Break(value) = visit(linework.segment(facet, i)) {
                            return Some(value);
                        }
                    }
                },
                BvhNode::Internal(first_child) => {
                    let first_child = first_child as u32;
                    stack.push(first_child);
                    stack.push(first_child + 1);
                },
            }
        }
        None
    }

    /// Whether `probe` touches or crosses any linework segment, reusing a
    /// caller-owned traversal stack across probes.
    pub(crate) fn any_segment_intersecting_with_stack(
        &self,
        linework: &PreparedLinework,
        probe: Segment,
        stack: &mut Vec<u32>,
    ) -> bool {
        self.for_each_segment_overlapping_with_stack(linework, probe, stack, |candidate| {
            if segments_intersect(probe, candidate) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .is_some()
    }

    /// Argmin sibling of the min sweep: the nearest `(probe vertex,
    /// projection)` pair from a full-point probe stream to the linework.
    /// The SIMD kernel prunes; the winning facet is scalar-refined with full
    /// ordinates (Z/M interpolate exactly like the brute path).
    pub(crate) fn nearest_to_points(
        &self,
        linework: &PreparedLinework,
        probes: impl Iterator<Item = Point>,
        mut best: Option<NearestCandidate>,
    ) -> Option<NearestCandidate> {
        let mut stack: Vec<u32> = Vec::new();
        for probe in probes {
            let probe_aabb = point_aabb(probe.x, probe.y);
            let bound = |aabb: [f64; 4]| aabb_distance_squared(aabb, probe_aabb);
            stack.clear();
            stack.push(0);
            while let Some(index) = stack.pop() {
                let limit = best
                    .as_ref()
                    .map_or(f64::INFINITY, |b| b.distance_key.upper_bound());
                if bound(self.aabbs[index as usize]) > limit {
                    continue;
                }
                match BvhCode::new(self.codes[index as usize]).decode() {
                    BvhNode::Leaf(facet_index) => {
                        let facet = linework.facets[facet_index];
                        if linework.facet_point_distance_squared(facet, probe.x, probe.y) <= limit {
                            refine_facet_nearest(linework, facet, probe, &mut best);
                        }
                    },
                    BvhNode::Internal(first_child) => {
                        self.push_nearer_last(&mut stack, first_child as u32, bound);
                    },
                }
            }
        }
        best
    }

    fn push_nearer_last(
        &self,
        stack: &mut Vec<u32>,
        first_child: u32,
        bound: impl Fn([f64; 4]) -> f64,
    ) {
        let left = first_child as usize;
        let right = left + 1;
        let left_bound = bound(self.aabbs[left]);
        let right_bound = bound(self.aabbs[right]);
        if left_bound <= right_bound {
            stack.push(first_child + 1);
            stack.push(first_child);
        } else {
            stack.push(first_child);
            stack.push(first_child + 1);
        }
    }

    /// First touching/crossing segment pair between two trees' lineworks
    /// (full ordinates) — the witness extractor for intersecting operands.
    /// Rides the shared dual descent
    /// ([`for_each_overlapping_segment_pair`]), returning
    /// the first crossing pair instead of a boolean.
    pub(crate) fn find_intersecting_pair(
        &self,
        linework: &PreparedLinework,
        other: &Self,
        other_linework: &PreparedLinework,
    ) -> Option<(Segment, Segment)> {
        self.for_each_overlapping_segment_pair(linework, other, other_linework, |left, right| {
            if segments_intersect(left, right) {
                ControlFlow::Break((left, right))
            } else {
                ControlFlow::Continue(())
            }
        })
    }

    /// One-sided sibling of [`find_intersecting_pair`]: the first linework
    /// segment touching `probe` (full ordinates), or `None`. Rides the shared
    /// single-tree descent ([`for_each_segment_overlapping`]).
    pub(crate) fn find_intersecting_segment(
        &self,
        linework: &PreparedLinework,
        probe: Segment,
    ) -> Option<Segment> {
        self.for_each_segment_overlapping(linework, probe, |candidate| {
            if segments_intersect(probe, candidate) {
                ControlFlow::Break(candidate)
            } else {
                ControlFlow::Continue(())
            }
        })
    }
}

impl HeapSize for FacetBvh {
    fn heap_bytes(&self) -> usize {
        self.aabbs.heap_bytes() + self.codes.heap_bytes()
    }
}
