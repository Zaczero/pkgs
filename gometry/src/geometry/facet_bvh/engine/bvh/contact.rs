use super::{
    BvhCode, BvhNode, ControlFlow, FacetBvh, Point, PreparedLinework, Segment, aabbs_overlap,
    facet_for_leaf, point_on_segment, segment_aabb, segments_intersect,
};

impl FacetBvh {
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
                    for segment in linework.facet_segments(facet) {
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
