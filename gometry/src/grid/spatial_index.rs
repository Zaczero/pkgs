//! A packed bounding-volume hierarchy for grid query envelopes.

use std::cmp::Ordering;

use rstar::AABB;

const PACKED_BVH_LEAF_SIZE: usize = 8;

#[derive(Clone, Copy, Debug)]
struct PackedBvhNodeId(usize);

#[derive(Debug)]
pub(super) struct PackedBvhEntry<T> {
    value: T,
    envelope: AABB<[f64; 2]>,
    order: usize,
}

impl<T> PackedBvhEntry<T> {
    pub(super) const fn new(value: T, envelope: AABB<[f64; 2]>) -> Self {
        Self {
            value,
            envelope,
            order: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct PackedBvhNode {
    envelope: AABB<[f64; 2]>,
    kind: PackedBvhNodeKind,
}

#[derive(Clone, Copy, Debug)]
enum PackedBvhNodeKind {
    Leaf {
        start: usize,
        end: usize,
    },
    Branch {
        left: PackedBvhNodeId,
        right: PackedBvhNodeId,
    },
}

#[derive(Debug)]
pub(super) struct PackedBvh<T> {
    entries: Vec<PackedBvhEntry<T>>,
    nodes: Vec<PackedBvhNode>,
    root: Option<PackedBvhNodeId>,
}

impl<T> PackedBvh<T> {
    pub(super) fn build(mut entries: Vec<PackedBvhEntry<T>>) -> Self {
        let entry_count = entries.len();
        let leaf_count = entry_count.div_ceil(PACKED_BVH_LEAF_SIZE);
        let node_capacity = if entry_count == 0 {
            0
        } else {
            // For every usize entry count, ceil(n / 8) is at most 2^61
            // on 64-bit targets, so this capacity hint is representable.
            // Recursive halving can create more nodes than the hint; Vec
            // growth remains ordinary when that occurs.
            leaf_count.wrapping_mul(2).wrapping_sub(1)
        };
        let mut nodes = Vec::with_capacity(node_capacity);
        for (order, entry) in entries.iter_mut().enumerate() {
            entry.order = order;
        }
        let root = if entry_count == 0 {
            None
        } else {
            Some(Self::build_node(&mut entries, &mut nodes, 0, entry_count))
        };
        Self {
            entries,
            nodes,
            root,
        }
    }

    fn build_node(
        entries: &mut [PackedBvhEntry<T>],
        nodes: &mut Vec<PackedBvhNode>,
        start: usize,
        end: usize,
    ) -> PackedBvhNodeId {
        let envelope = envelope_for_entries(
            entries
                .get(start..end)
                .expect("BVH node range belongs to its entry column"),
        );
        let node = PackedBvhNodeId(nodes.len());
        nodes.push(PackedBvhNode {
            envelope,
            kind: PackedBvhNodeKind::Leaf { start, end },
        });
        let range_len = end.checked_sub(start).expect("BVH node ranges are ordered");
        if range_len > PACKED_BVH_LEAF_SIZE {
            let axis = longest_axis(envelope);
            entries
                .get_mut(start..end)
                .expect("BVH node range belongs to its entry column")
                .sort_unstable_by(|left, right| {
                    proxy_center(left.envelope, axis)
                        .total_cmp(&proxy_center(right.envelope, axis))
                        .then_with(|| compare_envelopes(left.envelope, right.envelope))
                        .then_with(|| left.order.cmp(&right.order))
                });
            let middle = start
                .checked_add(range_len / 2)
                .expect("middle of a BVH node range is representable");
            let left = Self::build_node(entries, nodes, start, middle);
            let right = Self::build_node(entries, nodes, middle, end);
            let node = nodes
                .get_mut(node.0)
                .expect("BVH node slot was appended before its children");
            node.kind = PackedBvhNodeKind::Branch { left, right };
        }
        node
    }

    pub(super) fn visit(
        &self,
        query: AABB<[f64; 2]>,
        visit: &mut impl FnMut(&T) -> crate::error::Result<()>,
    ) -> crate::error::Result<()> {
        let Some(root) = self.root else {
            return Ok(());
        };
        self.visit_node(root, query, visit)
    }

    fn visit_node(
        &self,
        node: PackedBvhNodeId,
        query: AABB<[f64; 2]>,
        visit: &mut impl FnMut(&T) -> crate::error::Result<()>,
    ) -> crate::error::Result<()> {
        let current = self
            .nodes
            .get(node.0)
            .expect("BVH node IDs are constructed by the owning builder");
        if !aabbs_intersect(current.envelope, query) {
            return Ok(());
        }
        match current.kind {
            PackedBvhNodeKind::Branch { left, right } => {
                self.visit_node(left, query, visit)?;
                self.visit_node(right, query, visit)?;
            },
            PackedBvhNodeKind::Leaf { start, end } => {
                let entries = self
                    .entries
                    .get(start..end)
                    .expect("BVH leaf range belongs to its entry column");
                for entry in entries {
                    if aabbs_intersect(entry.envelope, query) {
                        visit(&entry.value)?;
                    }
                }
            },
        }
        Ok(())
    }
}

fn envelope_for_entries<T>(entries: &[PackedBvhEntry<T>]) -> AABB<[f64; 2]> {
    let (first, remaining) = entries
        .split_first()
        .expect("BVH node entry ranges are nonempty");
    let first = first.envelope;
    let mut lower = first.lower();
    let mut upper = first.upper();
    for entry in remaining {
        let entry_lower = entry.envelope.lower();
        let entry_upper = entry.envelope.upper();
        lower[0] = lower[0].min(entry_lower[0]);
        lower[1] = lower[1].min(entry_lower[1]);
        upper[0] = upper[0].max(entry_upper[0]);
        upper[1] = upper[1].max(entry_upper[1]);
    }
    AABB::from_bounds(lower, upper)
}

#[derive(Clone, Copy)]
enum PackedBvhAxis {
    X,
    Y,
}

fn longest_axis(envelope: AABB<[f64; 2]>) -> PackedBvhAxis {
    let lower = envelope.lower();
    let upper = envelope.upper();
    let x = upper[0] - lower[0];
    let y = upper[1] - lower[1];
    if y > x {
        PackedBvhAxis::Y
    } else {
        PackedBvhAxis::X
    }
}

fn proxy_center(envelope: AABB<[f64; 2]>, axis: PackedBvhAxis) -> f64 {
    let lower = envelope.lower();
    let upper = envelope.upper();
    match axis {
        PackedBvhAxis::X => f64::midpoint(lower[0], upper[0]),
        PackedBvhAxis::Y => f64::midpoint(lower[1], upper[1]),
    }
}

fn compare_envelopes(left: AABB<[f64; 2]>, right: AABB<[f64; 2]>) -> Ordering {
    let left_lower = left.lower();
    let left_upper = left.upper();
    let right_lower = right.lower();
    let right_upper = right.upper();
    left_lower[0]
        .total_cmp(&right_lower[0])
        .then_with(|| left_lower[1].total_cmp(&right_lower[1]))
        .then_with(|| left_upper[0].total_cmp(&right_upper[0]))
        .then_with(|| left_upper[1].total_cmp(&right_upper[1]))
}

fn aabbs_intersect(left: AABB<[f64; 2]>, right: AABB<[f64; 2]>) -> bool {
    let left_lower = left.lower();
    let left_upper = left.upper();
    let right_lower = right.lower();
    let right_upper = right.upper();
    left_lower[0] <= right_upper[0]
        && right_lower[0] <= left_upper[0]
        && left_lower[1] <= right_upper[1]
        && right_lower[1] <= left_upper[1]
}

#[cfg(test)]
mod tests {
    use rstar::AABB;

    use super::{PackedBvh, PackedBvhEntry};

    #[rstest::rstest]
    #[case::count_0(0)]
    #[case::count_1(1)]
    #[case::count_9(9)]
    #[case::count_41(41)]
    fn packed_bvh_leaf_and_branch_boundaries_preserve_visit_order(#[case] count: usize) {
        let envelope = AABB::from_corners([0.0, 0.0], [1.0, 1.0]);

        let entries = (0..count)
            .map(|value| PackedBvhEntry::new(value, envelope))
            .collect();
        let tree = PackedBvh::build(entries);
        let mut visited = Vec::new();
        tree.visit(envelope, &mut |value| {
            visited.push(*value);
            Ok(())
        })
        .expect("visitor");
        assert_eq!(visited, (0..count).collect::<Vec<_>>());
    }
}
