use std::ops::ControlFlow;

use crate::HeapSize;
use crate::geometry::facet_bvh::{
    Facet, PreparedLinework, aabb_distance, aabb_distance_squared, aabb_max_distance_squared,
    aabbs_overlap, point_aabb, segment_aabb, union_aabb,
};
use crate::geometry::{
    BvhCode, BvhNode, Point, Segment, SquaredDistanceKey, point_on_segment,
    point_segment_distance_squared, point_segment_selection, segment_projection,
    segments_intersect,
};

#[cfg(test)]
std::thread_local! {
    pub(crate) static NEAREST_WORK: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

/// One nearest-pair candidate. Source segments retain their ordinal until
/// the final witness is emitted with full source ordinates.
pub(crate) struct NearestCandidate {
    pub(in crate::geometry) probe: Point,
    pub(in crate::geometry) target: NearestTarget,
    pub(in crate::geometry) distance_squared: f64,
    pub(in crate::geometry) distance_key: SquaredDistanceKey,
}

pub(crate) enum NearestTarget {
    Segment(usize),
    Point(Point),
}

impl NearestCandidate {
    pub(crate) fn segment_ordinal(&self) -> Option<usize> {
        match self.target {
            NearestTarget::Segment(ordinal) => Some(ordinal),
            NearestTarget::Point(_) => None,
        }
    }

    pub(crate) fn target_point(&self, linework: &PreparedLinework) -> crate::error::Result<Point> {
        match self.target {
            NearestTarget::Point(point) => Ok(point),
            NearestTarget::Segment(ordinal) => {
                let (start, end) = linework.vertex_pair_at_segment(ordinal);
                let segment = Segment {
                    start: start.xy(),
                    end: end.xy(),
                };
                segment_projection(self.probe, segment)?.interpolate_point(start, end)
            },
        }
    }
}

/// Scalar-refine one facet's exact selection keys, deferring projection
/// and ordinate interpolation until the selected witness is emitted.
pub(super) fn refine_facet_nearest<const BOUNDED: bool>(
    linework: &PreparedLinework,
    facet: Facet,
    probe: Point,
    upper_bound: f64,
    tolerance: f64,
    keep: &impl Fn(Point, Segment) -> bool,
    best: &mut Option<NearestCandidate>,
) -> crate::error::Result<bool> {
    let mut changed = false;
    for index in 0..facet.segment_count as usize {
        #[cfg(test)]
        NEAREST_WORK.with(|work| work.set(work.get().saturating_add(1)));
        let segment = linework.segment(facet, index);
        if !keep(probe, segment) {
            continue;
        }
        let selection = point_segment_selection(probe.xy(), segment)?;
        if BOUNDED && selection.distance_squared > upper_bound {
            continue;
        }
        if BOUNDED
            && (selection.distance() > tolerance
                || tolerance == 0.0
                    && !selection
                        .distance_key(probe.xy(), segment)
                        .is_exact_zero()?)
        {
            continue;
        }
        let closer = match best.as_ref() {
            None => true,
            Some(candidate) => {
                selection.precedes(probe.xy(), segment, &candidate.distance_key, || {
                    candidate
                        .segment_ordinal()
                        .is_some_and(|best| linework.segment_ordinal(facet, index) < best)
                })?
            },
        };
        if closer {
            changed = true;
            *best = Some(NearestCandidate {
                probe,
                target: NearestTarget::Segment(linework.segment_ordinal(facet, index)),
                distance_squared: selection.distance_squared,
                distance_key: selection.distance_key(probe.xy(), segment),
            });
        }
    }
    Ok(changed)
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
    slabs: Option<Box<[DirectionalSlab]>>,
}

#[derive(Clone, Copy, Default)]
struct DirectionalSlab {
    normal: [f64; 2],
    low: f64,
    high: f64,
}

impl DirectionalSlab {
    fn build(linework: &PreparedLinework, ids: &[u32]) -> Self {
        let facet = facet_for_leaf(linework, ids[ids.len() / 2] as usize);
        let mut direction = [0.0_f64; 2];
        let mut longest = 0.0_f64;
        for index in 0..facet.segment_count as usize {
            let segment = linework.segment(facet, index);
            let dx = segment.end.x - segment.start.x;
            let dy = segment.end.y - segment.start.y;
            let length = dx.abs().max(dy.abs());
            if length > longest {
                longest = length;
                direction = [-dy / length, dx / length];
            }
        }
        if longest == 0.0 || !longest.is_finite() {
            return Self::default();
        }
        let mut slab = Self {
            normal: direction,
            low: f64::INFINITY,
            high: f64::NEG_INFINITY,
        };
        for &id in ids {
            let facet = facet_for_leaf(linework, id as usize);
            for index in 0..facet.segment_count as usize {
                let segment = linework.segment(facet, index);
                for point in [segment.start, segment.end] {
                    let Some([low, high]) = slab.project(point.x, point.y) else {
                        return Self::default();
                    };
                    slab.low = slab.low.min(low);
                    slab.high = slab.high.max(high);
                }
            }
        }
        slab
    }

    fn project(self, x: f64, y: f64) -> Option<[f64; 2]> {
        let left = x * self.normal[0];
        let right = y * self.normal[1];
        let value = left + right;
        // Two products and their sum, including gradual-underflow error.
        let error = (left.abs() + right.abs()) * (4.0 * f64::EPSILON) + f64::MIN_POSITIVE;
        let low = (value - error).next_down();
        let high = (value + error).next_up();
        (low.is_finite() && high.is_finite()).then_some([low, high])
    }

    fn distance_squared(self, point: Point) -> f64 {
        let Some([low, high]) = self.project(point.x, point.y) else {
            return 0.0;
        };
        let gap = if low > self.high {
            (low - self.high).next_down()
        } else if high < self.low {
            (self.low - high).next_down()
        } else {
            return 0.0;
        };
        let [nx, ny] = self.normal;
        let norm = ((nx * nx).next_up() + (ny * ny).next_up()).next_up();
        if norm == 0.0 {
            return 0.0;
        }
        let squared = (gap * gap).next_down().max(0.0);
        (squared / norm).next_down().max(0.0)
    }
}

fn facet_for_leaf(linework: &PreparedLinework, index: usize) -> Facet {
    *linework
        .facets
        .get(index)
        .expect("BVH leaf facet index is within its prepared linework")
}

impl FacetBvh {
    /// Build over `linework`'s facets; `None` when it has none — or so many
    /// that a NODE index (`2F − 1` of them) could reach [`BvhCode::LEAF_BIT`] and a
    /// first-child code would read as a leaf (those astronomically large
    /// shapes keep the flat facet scans, which are index-width-agnostic).
    pub(crate) fn build(linework: &PreparedLinework) -> Option<Self> {
        Self::build_with_bounds(linework, false)
    }

    pub(crate) fn build_for_clearance(linework: &PreparedLinework) -> Option<Self> {
        Self::build_with_bounds(linework, true)
    }

    pub(crate) fn has_slabs(&self) -> bool {
        self.slabs.is_some()
    }

    fn build_with_bounds(linework: &PreparedLinework, clearance: bool) -> Option<Self> {
        let facet_count = u32::try_from(linework.facets.len()).ok()?;
        if facet_count == 0 || facet_count > BvhCode::LEAF_BIT / 2 {
            return None;
        }
        let facet_aabbs: Vec<[f64; 4]> = linework
            .facets
            .iter()
            .map(|&facet| linework.facet_aabb(facet))
            .collect();
        let tight = clearance && {
            let extent = facet_aabbs
                .iter()
                .copied()
                .reduce(union_aabb)
                .expect("nonempty linework has facet bounds");
            let span = (extent[2] - extent[0]).max(extent[3] - extent[1]);
            let mut lengths = 0.0;
            linework.for_each_segment(|segment| {
                lengths += (segment.end.x - segment.start.x)
                    .abs()
                    .max((segment.end.y - segment.start.y).abs());
            });
            lengths / linework.segment_count() as f64 * 16.0 > span
        };
        let mut ids: Vec<u32> = (0..facet_count).collect();
        let directions = tight.then(|| {
            (0..facet_count)
                .map(|id| {
                    let [mut x, mut y] = DirectionalSlab::build(linework, &[id]).normal;
                    if y < 0.0 || y == 0.0 && x < 0.0 {
                        x = -x;
                        y = -y;
                    }
                    y.atan2(x)
                })
                .collect::<Vec<_>>()
        });
        let node_count = (facet_count as usize).wrapping_mul(2).wrapping_sub(1);
        let mut tree = Self {
            aabbs: vec![[0.0; 4]; node_count].into_boxed_slice(),
            codes: vec![0; node_count].into_boxed_slice(),
            slabs: tight.then(|| vec![DirectionalSlab::default(); node_count].into_boxed_slice()),
        };
        // Root at 0; children allocated in adjacent pairs from the cursor.
        let mut next = 1;
        tree.fill_node(
            0,
            &mut next,
            &facet_aabbs,
            &mut ids,
            linework,
            directions.as_deref(),
        );
        Some(tree)
    }

    /// Median split of facet centers, or their normal directions when using
    /// directional slabs, into the node slot `node`.
    #[expect(
        clippy::indexing_slicing,
        reason = "Facet ids are below facet_aabbs.len(); recursive node ids are builder-produced and below the allocated 2F-1 aabbs/codes lengths"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "facet_count <= LEAF_BIT/2 keeps 2F-1 in usize; next advances only across sibling pairs within the allocated node_count"
    )]
    fn fill_node(
        &mut self,
        node: usize,
        next: &mut usize,
        facet_aabbs: &[[f64; 4]],
        ids: &mut [u32],
        linework: &PreparedLinework,
        directions: Option<&[f64]>,
    ) {
        if let Some(slabs) = &mut self.slabs {
            slabs[node] = DirectionalSlab::build(linework, ids);
        }
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
        // Facet ordinals break equal partition keys deterministically.
        let axis = usize::from(aabb[3] - aabb[1] > aabb[2] - aabb[0]);
        let center = |id: u32| {
            directions.map_or_else(
                || {
                    let facet = facet_aabbs[id as usize];
                    facet[axis] + facet[axis + 2]
                },
                |directions| directions[id as usize],
            )
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
        self.fill_node(first_child, next, facet_aabbs, lower, linework, directions);
        self.fill_node(
            first_child + 1,
            next,
            facet_aabbs,
            upper,
            linework,
            directions,
        );
    }

    fn node_aabb(&self, index: u32) -> [f64; 4] {
        *self
            .aabbs
            .get(index as usize)
            .expect("BVH traversal node index is within the built tree")
    }

    fn decode_node(&self, index: u32) -> BvhNode {
        BvhCode::new(
            *self
                .codes
                .get(index as usize)
                .expect("BVH traversal node index is within the built tree"),
        )
        .decode()
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
    ) -> crate::error::Result<f64> {
        let mut stack: Vec<u32> = Vec::new();
        for (x, y) in probes {
            best =
                self.min_point_distance_with_stack::<SQUARED>(linework, x, y, &mut stack, best)?;
        }
        Ok(best)
    }

    /// One probe's branch-and-bound descent over a caller-provided (reused)
    /// stack — the per-row engine behind
    /// [`min_points_distance`](Self::min_points_distance) and the packed
    /// point-array lanes (which need per-row results, not one fold). Returns
    /// `best` folded with this probe's minimum.
    #[expect(
        clippy::indexing_slicing,
        reason = "Stack ids start at root 0 and follow builder-encoded children below aabbs/codes.len(); leaf ids are below linework.facets.len()"
    )]
    fn min_point_distance_with_stack_by(
        &self,
        linework: &PreparedLinework,
        stack: &mut Vec<u32>,
        mut best: f64,
        bound: impl Fn([f64; 4]) -> f64,
        mut refine: impl FnMut(Facet) -> crate::error::Result<f64>,
    ) -> crate::error::Result<f64> {
        stack.clear();
        stack.push(0);
        while let Some(index) = stack.pop() {
            if bound(self.aabbs[index as usize]) >= best {
                continue;
            }
            match BvhCode::new(self.codes[index as usize]).decode() {
                BvhNode::Leaf(facet_index) => {
                    let candidate = refine(linework.facets[facet_index])?;
                    best = best.min(candidate);
                },
                BvhNode::Internal(first_child) => {
                    self.push_nearer_last(stack, first_child as u32, &bound);
                },
            }
        }
        Ok(best)
    }

    pub(crate) fn min_point_distance_with_stack<const SQUARED: bool>(
        &self,
        linework: &PreparedLinework,
        x: f64,
        y: f64,
        stack: &mut Vec<u32>,
        best: f64,
    ) -> crate::error::Result<f64> {
        let bound = |aabb: [f64; 4]| {
            if SQUARED {
                aabb_distance_squared(aabb, point_aabb(x, y))
            } else {
                aabb_distance(aabb, point_aabb(x, y))
            }
        };
        self.min_point_distance_with_stack_by(linework, stack, best, bound, |facet| {
            if SQUARED {
                linework.facet_point_distance_squared(facet, x, y)
            } else {
                linework.facet_point_distance(facet, Point::new_raw_xy(x, y))
            }
        })
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
        probes: impl Iterator<Item = (f64, f64)>,
        limit: f64,
        simd: bool,
    ) -> crate::error::Result<bool> {
        let mut stack: Vec<u32> = Vec::new();
        for (x, y) in probes {
            if self.point_within_with_stack(linework, x, y, limit, simd, &mut stack)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// One probe's within-limit descent over a caller-provided (reused) stack
    /// — the per-row engine behind
    /// [`any_points_within`](Self::any_points_within) and the packed
    /// point-array lanes.
    #[expect(
        clippy::indexing_slicing,
        reason = "Stack ids start at root 0 and follow builder-encoded children below self.aabbs/codes.len(); leaf ids are below linework.facets.len()"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "A decoded internal node owns an allocated adjacent child pair, so first_child + 1 is below self.aabbs.len()"
    )]
    pub(crate) fn point_within_with_stack(
        &self,
        linework: &PreparedLinework,
        x: f64,
        y: f64,
        limit: f64,
        simd: bool,
        stack: &mut Vec<u32>,
    ) -> crate::error::Result<bool> {
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
                return Ok(true);
            }
            match BvhCode::new(self.codes[index as usize]).decode() {
                BvhNode::Leaf(facet_index) => {
                    let facet = linework.facets[facet_index];
                    let candidate = if simd {
                        linework.facet_point_distance_squared(facet, x, y)?
                    } else {
                        let point = Point::new_raw_xy(x, y);
                        let mut best = f64::INFINITY;
                        for i in 0..facet.segment_count as usize {
                            best = best.min(point_segment_distance_squared(
                                point,
                                linework.segment(facet, i),
                            )?);
                        }
                        best
                    };
                    if candidate <= limit {
                        return Ok(true);
                    }
                },
                BvhNode::Internal(first_child) => {
                    let first_child = first_child as u32;
                    stack.push(first_child);
                    stack.push(first_child + 1);
                },
            }
        }
        Ok(false)
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
    #[expect(
        clippy::indexing_slicing,
        reason = "Root/child ids are below each tree's aabbs/codes.len(); leaf facet ids are below each linework.facets.len()"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "Internal nodes encode allocated sibling pairs, so first_child + 1 stays below the corresponding aabbs.len()"
    )]
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
            let aabb = self.node_aabb(index);
            if point.x < aabb[0] || point.x > aabb[2] || point.y < aabb[1] || point.y > aabb[3] {
                continue;
            }
            match self.decode_node(index) {
                BvhNode::Leaf(facet_index) => {
                    let facet = facet_for_leaf(linework, facet_index);
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
                    stack.push(first_child.wrapping_add(1));
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
            if !aabbs_overlap(self.node_aabb(index), probe_aabb) {
                continue;
            }
            match self.decode_node(index) {
                BvhNode::Leaf(facet_index) => {
                    let facet = facet_for_leaf(linework, facet_index);
                    for i in 0..facet.segment_count as usize {
                        if let ControlFlow::Break(value) = visit(linework.segment(facet, i)) {
                            return Some(value);
                        }
                    }
                },
                BvhNode::Internal(first_child) => {
                    let first_child = first_child as u32;
                    stack.push(first_child);
                    stack.push(first_child.wrapping_add(1));
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
    /// Safe squares use SIMD facet bounds; other exponents use distance space.
    /// Winning segments retain their full source ordinates.
    pub(crate) fn nearest_to_points<const SQUARED: bool>(
        &self,
        linework: &PreparedLinework,
        probes: impl Iterator<Item = Point>,
        best: &mut Option<NearestCandidate>,
    ) -> crate::error::Result<bool> {
        let mut stack: Vec<u32> = Vec::new();
        let mut changed = false;
        for probe in probes {
            changed |= self.nearest_point_with_stack::<false, SQUARED, false>(
                linework,
                probe,
                f64::INFINITY,
                f64::INFINITY,
                &|_, _| true,
                &mut stack,
                best,
            )?;
        }
        Ok(changed)
    }

    pub(crate) fn nearest_point_with_stack<
        const TIGHT: bool,
        const SQUARED: bool,
        const BOUNDED: bool,
    >(
        &self,
        linework: &PreparedLinework,
        probe: Point,
        upper_bound: f64,
        tolerance: f64,
        keep: &impl Fn(Point, Segment) -> bool,
        stack: &mut Vec<u32>,
        best: &mut Option<NearestCandidate>,
    ) -> crate::error::Result<bool> {
        let mut changed_any = false;
        let probe_aabb = point_aabb(probe.x, probe.y);
        let bound = |aabb: [f64; 4]| {
            if SQUARED {
                aabb_distance_squared(aabb, probe_aabb)
            } else {
                aabb_distance(aabb, probe_aabb)
            }
        };
        let mut distance_limit = if SQUARED {
            upper_bound
        } else {
            best.as_ref()
                .map(|best| best.distance_key.distance_upper_bound())
                .transpose()?
                .unwrap_or(upper_bound)
        };
        stack.clear();
        stack.push(0);
        while let Some(index) = stack.pop() {
            #[cfg(test)]
            NEAREST_WORK.with(|work| work.set(work.get().saturating_add(1)));
            let limit = if SQUARED {
                best.as_ref().map_or(upper_bound, |b| {
                    if BOUNDED {
                        upper_bound.min(b.distance_key.upper_bound())
                    } else {
                        b.distance_key.upper_bound()
                    }
                })
            } else {
                distance_limit
            };
            if bound(self.node_aabb(index)) > limit {
                continue;
            }
            if TIGHT
                && self
                    .slabs
                    .as_ref()
                    .is_some_and(|slabs| slabs[index as usize].distance_squared(probe) > limit)
            {
                continue;
            }
            match self.decode_node(index) {
                BvhNode::Leaf(facet_index) => {
                    let facet = facet_for_leaf(linework, facet_index);
                    let facet_distance = if SQUARED {
                        linework.facet_point_distance_squared(facet, probe.x, probe.y)?
                    } else {
                        linework.facet_point_distance(facet, probe)?
                    };
                    if facet_distance <= limit {
                        let changed = refine_facet_nearest::<BOUNDED>(
                            linework,
                            facet,
                            probe,
                            upper_bound,
                            tolerance,
                            keep,
                            best,
                        )?;
                        changed_any |= changed;
                        if !SQUARED && changed {
                            distance_limit = best
                                .as_ref()
                                .expect("refinement improved the nearest candidate")
                                .distance_key
                                .distance_upper_bound()?;
                        }
                    }
                },
                BvhNode::Internal(first_child) => {
                    if BOUNDED {
                        stack.push(first_child as u32);
                        stack.push((first_child as u32).wrapping_add(1));
                    } else {
                        self.push_nearer_last(stack, first_child as u32, &bound);
                    }
                },
            }
        }
        Ok(changed_any)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "first_child is builder-produced and first_child + 1 is below self.aabbs.len()"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "first_child names an allocated sibling pair, so right = left + 1 and stack child + 1 stay below self.aabbs.len()"
    )]
    fn push_nearer_last(
        &self,
        stack: &mut Vec<u32>,
        first_child: u32,
        bound: &impl Fn([f64; 4]) -> f64,
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
        self.aabbs
            .heap_bytes()
            .saturating_add(self.codes.heap_bytes())
            .saturating_add(self.slabs.as_ref().map_or(0, |slabs| {
                slabs
                    .len()
                    .saturating_mul(std::mem::size_of::<DirectionalSlab>())
            }))
    }
}

#[cfg(test)]
mod public_nearest_work_tests {
    use pyo3::prelude::*;
    use pyo3::types::PyList;

    use super::NEAREST_WORK;
    #[test]
    fn public_clearance_bounds_radial_candidate_work() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let module = PyModule::new(py, "gometry._lib").unwrap();
            crate::lib(&module).unwrap();
            let mut previous = None;
            for count in [1024_usize, 4096] {
                let coords = PyList::empty(py);
                for index in 0..count {
                    let radius = 0.5 + (index.wrapping_mul(17) % 31) as f64 / 31.0;
                    let angle = std::f64::consts::TAU * index as f64 / count as f64;
                    coords
                        .append((radius * angle.cos(), radius * angle.sin()))
                        .unwrap();
                }
                let polygon = module
                    .getattr("Polygon")
                    .unwrap()
                    .call1((&coords,))
                    .unwrap();
                for operation in ["minimum_clearance", "minimum_clearance_line"] {
                    NEAREST_WORK.with(|work| work.set(0));
                    let result = polygon.call_method0(operation).unwrap();
                    if operation == "minimum_clearance" {
                        assert!(result.extract::<f64>().unwrap() > 0.0);
                    } else {
                        assert!(
                            !result
                                .getattr("is_empty")
                                .unwrap()
                                .extract::<bool>()
                                .unwrap()
                        );
                    }
                    let work = NEAREST_WORK.with(std::cell::Cell::get);
                    eprintln!("{operation}: {count} vertices, {work} candidate visits");
                    assert!(
                        work > count,
                        "the public call did not reach nearest queries"
                    );
                    if let Some(previous) = previous {
                        assert!(
                            work < previous * 6,
                            "{count} radial vertices took {work} visits versus {previous} at one quarter the size",
                        );
                    }
                    if operation == "minimum_clearance_line" {
                        previous = Some(work);
                    }
                }
            }
        });
    }
}
