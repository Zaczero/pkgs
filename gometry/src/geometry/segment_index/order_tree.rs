use std::cmp::Ordering;
use std::ops::Range;

struct OrderNode<T> {
    item: T,
    left: Option<usize>,
    right: Option<usize>,
    parent: Option<usize>,
    height: usize,
    subtree_len: usize,
}

/// An index-backed AVL sequence. Geometry is supplied by each search; no
/// mutable geometric ordering is stored in the tree.
pub(in crate::geometry) struct OrderTree<T: Copy> {
    nodes: Vec<Option<OrderNode<T>>>,
    free: Vec<usize>,
    root: Option<usize>,
}

impl<T: Copy> Default for OrderTree<T> {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            free: Vec::new(),
            root: None,
        }
    }
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "exact contact sweeps also use rank lower-bounds, block splices, and ordered range extraction"
    )
)]
impl<T: Copy> OrderTree<T> {
    pub(in crate::geometry) fn with_capacity(capacity: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(capacity),
            free: Vec::with_capacity(capacity),
            root: None,
        }
    }

    pub(in crate::geometry) fn len(&self) -> usize {
        self.subtree_len(self.root)
    }

    pub(in crate::geometry) const fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the root and child links contain only live arena slots allocated by this tree"
    )]
    fn node(&self, index: usize) -> &OrderNode<T> {
        self.nodes[index]
            .as_ref()
            .expect("live AVL node has an arena value")
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the root and child links contain only live arena slots allocated by this tree"
    )]
    fn node_mut(&mut self, index: usize) -> &mut OrderNode<T> {
        self.nodes[index]
            .as_mut()
            .expect("live AVL node has an arena value")
    }

    fn height(&self, root: Option<usize>) -> usize {
        root.map_or(0, |index| self.node(index).height)
    }

    fn subtree_len(&self, root: Option<usize>) -> usize {
        root.map_or(0, |index| self.node(index).subtree_len)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "a reused slot comes from this tree's free list and is vacant"
    )]
    fn allocate(&mut self, item: T) -> usize {
        let node = OrderNode {
            item,
            left: None,
            right: None,
            parent: None,
            height: 1,
            subtree_len: 1,
        };
        if let Some(index) = self.free.pop() {
            self.nodes[index] = Some(node);
            index
        } else {
            self.nodes.push(Some(node));
            self.nodes.len().wrapping_sub(1)
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the removed slot is a live arena node returned by this tree's rank descent"
    )]
    fn release(&mut self, index: usize) {
        self.nodes[index] = None;
        self.free.push(index);
    }

    fn set_left(&mut self, parent: usize, child: Option<usize>) {
        self.node_mut(parent).left = child;
        if let Some(child) = child {
            self.node_mut(child).parent = Some(parent);
        }
    }

    fn set_right(&mut self, parent: usize, child: Option<usize>) {
        self.node_mut(parent).right = child;
        if let Some(child) = child {
            self.node_mut(child).parent = Some(parent);
        }
    }

    fn take_left(&mut self, parent: usize) -> Option<usize> {
        let child = self.node_mut(parent).left.take();
        if let Some(child) = child {
            self.node_mut(child).parent = None;
        }
        child
    }

    fn take_right(&mut self, parent: usize) -> Option<usize> {
        let child = self.node_mut(parent).right.take();
        if let Some(child) = child {
            self.node_mut(child).parent = None;
        }
        child
    }

    fn update(&mut self, index: usize) {
        let (left, right) = {
            let node = self.node(index);
            (node.left, node.right)
        };
        let height = self.height(left).max(self.height(right)).wrapping_add(1);
        let subtree_len = self
            .subtree_len(left)
            .wrapping_add(self.subtree_len(right))
            .wrapping_add(1);
        let node = self.node_mut(index);
        node.height = height;
        node.subtree_len = subtree_len;
    }

    fn balance(&self, index: usize) -> isize {
        let node = self.node(index);
        // AVL height is logarithmic in the usize-bounded node count, so both
        // heights and their difference fit in `isize`.
        (self.height(node.left) as isize).wrapping_sub(self.height(node.right) as isize)
    }

    fn rotate_left(&mut self, root: usize) -> usize {
        let parent = self.node(root).parent;
        let pivot = self
            .node(root)
            .right
            .expect("left rotation has a right child");
        let middle = self.take_left(pivot);
        self.set_right(root, middle);
        self.set_left(pivot, Some(root));
        self.node_mut(pivot).parent = parent;
        self.update(root);
        self.update(pivot);
        pivot
    }

    fn rotate_right(&mut self, root: usize) -> usize {
        let parent = self.node(root).parent;
        let pivot = self
            .node(root)
            .left
            .expect("right rotation has a left child");
        let middle = self.take_right(pivot);
        self.set_left(root, middle);
        self.set_right(pivot, Some(root));
        self.node_mut(pivot).parent = parent;
        self.update(root);
        self.update(pivot);
        pivot
    }

    fn rebalance(&mut self, root: usize) -> usize {
        self.update(root);
        match self.balance(root) {
            balance if balance > 1 => {
                let left = self.node(root).left.expect("left-heavy node has a child");
                if self.balance(left) < 0 {
                    let left = self.rotate_left(left);
                    self.set_left(root, Some(left));
                }
                self.rotate_right(root)
            },
            balance if balance < -1 => {
                let right = self.node(root).right.expect("right-heavy node has a child");
                if self.balance(right) > 0 {
                    let right = self.rotate_right(right);
                    self.set_right(root, Some(right));
                }
                self.rotate_left(root)
            },
            _ => root,
        }
    }

    /// Rank of the first item for which `compare(item)` is not `Less`.
    pub(in crate::geometry) fn lower_bound_by(
        &self,
        mut compare: impl FnMut(T) -> crate::error::Result<std::cmp::Ordering>,
    ) -> crate::error::Result<usize> {
        self.partition_point_by(|item| Ok(compare(item)?.is_lt()))
    }

    /// Rank at the end of the prefix for which `before(item)` is true.
    pub(in crate::geometry) fn partition_point_by(
        &self,
        mut before: impl FnMut(T) -> crate::error::Result<bool>,
    ) -> crate::error::Result<usize> {
        let (mut current, mut rank) = (self.root, 0_usize);
        while let Some(index) = current {
            let node = self.node(index);
            let left_len = self.subtree_len(node.left);
            if before(node.item)? {
                rank = rank.wrapping_add(left_len).wrapping_add(1);
                current = node.right;
            } else {
                current = node.left;
            }
        }
        Ok(rank)
    }

    pub(in crate::geometry) fn item_at(&self, rank: usize) -> Option<T> {
        if rank >= self.len() {
            return None;
        }
        let mut current = self.root;
        let mut rank = rank;
        while let Some(index) = current {
            let node = self.node(index);
            let left_len = self.subtree_len(node.left);
            match rank.cmp(&left_len) {
                Ordering::Less => current = node.left,
                Ordering::Equal => return Some(node.item),
                Ordering::Greater => {
                    rank = rank.wrapping_sub(left_len).wrapping_sub(1);
                    current = node.right;
                },
            }
        }
        None
    }

    pub(in crate::geometry) fn insert_at_rank(&mut self, rank: usize, item: T) {
        assert!(rank <= self.len(), "AVL insertion rank is in bounds");
        let node = self.allocate(item);
        self.root = Some(self.insert(self.root, rank, node));
        if let Some(root) = self.root {
            self.node_mut(root).parent = None;
        }
    }

    fn insert(&mut self, root: Option<usize>, rank: usize, inserted: usize) -> usize {
        let Some(root) = root else {
            return inserted;
        };
        let left_len = self.subtree_len(self.node(root).left);
        if rank <= left_len {
            let left = self.insert(self.node(root).left, rank, inserted);
            self.set_left(root, Some(left));
        } else {
            let right = self.insert(
                self.node(root).right,
                rank.wrapping_sub(left_len).wrapping_sub(1),
                inserted,
            );
            self.set_right(root, Some(right));
        }
        self.rebalance(root)
    }

    pub(in crate::geometry) fn remove_at_rank(&mut self, rank: usize) -> T {
        assert!(rank < self.len(), "AVL removal rank is in bounds");
        let (root, item, removed) = self.remove(self.root, rank);
        self.root = root;
        if let Some(root) = self.root {
            self.node_mut(root).parent = None;
        }
        self.release(removed);
        item
    }

    fn remove(&mut self, root: Option<usize>, rank: usize) -> (Option<usize>, T, usize) {
        let root = root.expect("AVL removal descends through a live node");
        let left_len = self.subtree_len(self.node(root).left);
        if rank < left_len {
            let (left, item, removed) = self.remove(self.node(root).left, rank);
            self.set_left(root, left);
            return (Some(self.rebalance(root)), item, removed);
        }
        if rank > left_len {
            let (right, item, removed) = self.remove(
                self.node(root).right,
                rank.wrapping_sub(left_len).wrapping_sub(1),
            );
            self.set_right(root, right);
            return (Some(self.rebalance(root)), item, removed);
        }

        let item = self.node(root).item;
        let left = self.take_left(root);
        let right = self.take_right(root);
        (self.join(left, right), item, root)
    }

    /// Replace one contiguous rank range with items already sorted in their
    /// desired order. All fallible geometric comparisons happen before entry.
    pub(in crate::geometry) fn splice(&mut self, range: Range<usize>, items: &[T]) {
        assert!(
            range.start <= range.end && range.end <= self.len(),
            "AVL splice range is in bounds"
        );
        let root = self.root.take();
        let (prefix, tail) = self.split(root, range.start);
        let (removed, suffix) = self.split(tail, range.end.wrapping_sub(range.start));
        self.release_tree(removed);
        let inserted = self.build_balanced(items, 0, items.len());
        let prefix = self.join(prefix, inserted);
        let joined = self.join(prefix, suffix);
        self.root = joined;
        if let Some(root) = self.root {
            self.node_mut(root).parent = None;
        }
    }

    fn split(&mut self, root: Option<usize>, rank: usize) -> (Option<usize>, Option<usize>) {
        let Some(root) = root else {
            return (None, None);
        };
        let left = self.take_left(root);
        let right = self.take_right(root);
        let left_len = self.subtree_len(left);
        let (before, after) = if rank <= left_len {
            let (before, left_remainder) = self.split(left, rank);
            (
                before,
                Some(self.join_with_root(left_remainder, root, right)),
            )
        } else {
            let (right_prefix, after) =
                self.split(right, rank.wrapping_sub(left_len).wrapping_sub(1));
            (Some(self.join_with_root(left, root, right_prefix)), after)
        };
        if let Some(before) = before {
            self.node_mut(before).parent = None;
        }
        if let Some(after) = after {
            self.node_mut(after).parent = None;
        }
        (before, after)
    }

    fn join(&mut self, left: Option<usize>, right: Option<usize>) -> Option<usize> {
        match (left, right) {
            (None, right) => right,
            (left, None) => left,
            (Some(left), Some(right)) => {
                let (right, pivot) = self.take_min(right);
                Some(self.join_with_root(Some(left), pivot, right))
            },
        }
    }

    fn take_min(&mut self, root: usize) -> (Option<usize>, usize) {
        let left = self.node(root).left;
        let Some(left) = left else {
            let right = self.take_right(root);
            self.node_mut(root).parent = None;
            self.update(root);
            return (right, root);
        };
        let (left, minimum) = self.take_min(left);
        self.set_left(root, left);
        (Some(self.rebalance(root)), minimum)
    }

    fn join_with_root(&mut self, left: Option<usize>, pivot: usize, right: Option<usize>) -> usize {
        let left_height = self.height(left);
        let right_height = self.height(right);
        if left_height > right_height.wrapping_add(1) {
            let root = left.expect("tall left join has a root");
            let right_child = self.take_right(root);
            let joined = self.join_with_root(right_child, pivot, right);
            self.set_right(root, Some(joined));
            return self.rebalance(root);
        }
        if right_height > left_height.wrapping_add(1) {
            let root = right.expect("tall right join has a root");
            let left_child = self.take_left(root);
            let joined = self.join_with_root(left, pivot, left_child);
            self.set_left(root, Some(joined));
            return self.rebalance(root);
        }

        self.node_mut(pivot).left = None;
        self.node_mut(pivot).right = None;
        self.node_mut(pivot).parent = None;
        self.set_left(pivot, left);
        self.set_right(pivot, right);
        self.update(pivot);
        pivot
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the nonempty recursive range proves its midpoint is inside the input slice"
    )]
    fn build_balanced(&mut self, items: &[T], start: usize, end: usize) -> Option<usize> {
        if start == end {
            return None;
        }
        let middle = start.wrapping_add(end.wrapping_sub(start) / 2);
        let root = self.allocate(items[middle]);
        let left = self.build_balanced(items, start, middle);
        let right = self.build_balanced(items, middle.wrapping_add(1), end);
        self.set_left(root, left);
        self.set_right(root, right);
        self.update(root);
        Some(root)
    }

    fn release_tree(&mut self, root: Option<usize>) {
        let Some(root) = root else {
            return;
        };
        let left = self.take_left(root);
        let right = self.take_right(root);
        self.release_tree(left);
        self.release_tree(right);
        self.release(root);
    }

    fn append_range(
        &self,
        root: Option<usize>,
        base: usize,
        range: &Range<usize>,
        output: &mut Vec<T>,
    ) {
        let Some(root) = root else {
            return;
        };
        let node = self.node(root);
        let left_len = self.subtree_len(node.left);
        let rank = base.wrapping_add(left_len);
        if range.start < rank {
            self.append_range(node.left, base, range, output);
        }
        if range.start <= rank && rank < range.end {
            output.push(node.item);
        }
        let after = rank.wrapping_add(1);
        if after < range.end {
            self.append_range(node.right, after, range, output);
        }
    }

    pub(in crate::geometry) fn append_range_to(&self, range: Range<usize>, output: &mut Vec<T>) {
        assert!(
            range.start <= range.end && range.end <= self.len(),
            "AVL range is in bounds"
        );
        self.append_range(self.root, 0, &range, output);
    }

    #[cfg(test)]
    pub(in crate::geometry) fn assert_valid(&self) -> Vec<T> {
        fn visit<T: Copy>(
            tree: &OrderTree<T>,
            root: Option<usize>,
            parent: Option<usize>,
            output: &mut Vec<T>,
        ) -> (usize, usize) {
            let Some(root) = root else {
                return (0, 0);
            };
            let node = tree.node(root);
            assert_eq!(node.parent, parent);
            let (left_height, left_len) = visit(tree, node.left, Some(root), output);
            output.push(node.item);
            let (right_height, right_len) = visit(tree, node.right, Some(root), output);
            assert!(left_height.abs_diff(right_height) <= 1);
            assert_eq!(node.height, left_height.max(right_height).wrapping_add(1));
            assert_eq!(
                node.subtree_len,
                left_len.wrapping_add(right_len).wrapping_add(1)
            );
            (node.height, node.subtree_len)
        }

        let mut output = Vec::with_capacity(self.len());
        let (height, len) = visit(self, self.root, None, &mut output);
        assert_eq!(len, self.len());
        if len > 0 {
            assert!(
                (height as f64) < 1.45 * ((len as f64) + 2.0).log2(),
                "AVL height {height} exceeds logarithmic bound for {len} nodes"
            );
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::OrderTree;

    #[test]
    fn rank_insert_remove_and_splice_match_sequence() {
        let mut tree = OrderTree::with_capacity(2048);
        let mut expected = Vec::new();
        for item in 0..2048_usize {
            let rank = if item % 2 == 0 { 0 } else { expected.len() };
            tree.insert_at_rank(rank, item);
            expected.insert(rank, item);
        }
        assert_eq!(tree.assert_valid(), expected);

        let mut replacement = expected[333..1701].to_vec();
        replacement.reverse();
        tree.splice(333..1701, &replacement);
        expected.splice(333..1701, replacement);
        assert_eq!(tree.assert_valid(), expected);

        while !expected.is_empty() {
            let rank = expected.len() / 2;
            assert_eq!(tree.remove_at_rank(rank), expected.remove(rank));
        }
        assert!(tree.is_empty());
        assert!(tree.assert_valid().is_empty());
    }

    #[test]
    fn bounded_event_block_splice_keeps_other_ranks_in_order() {
        let mut tree = OrderTree::with_capacity(4096);
        let mut expected: Vec<_> = (0..4096_usize).collect();
        for &item in &expected {
            let rank = tree.len();
            tree.insert_at_rank(rank, item);
        }
        let mut comparisons = 0_usize;
        let rank = tree
            .lower_bound_by(|item| {
                comparisons = comparisons.wrapping_add(1);
                Ok(item.cmp(&2048))
            })
            .unwrap();
        assert_eq!(rank, 2048);
        assert!(
            comparisons < 32,
            "rank search used {comparisons} comparisons"
        );
        let mut prefix_comparisons = 0_usize;
        let prefix_rank = tree
            .partition_point_by(|item| {
                prefix_comparisons = prefix_comparisons.wrapping_add(1);
                Ok(item < 2048)
            })
            .unwrap();
        assert_eq!(prefix_rank, 2048);
        assert!(
            prefix_comparisons < 32,
            "prefix search used {prefix_comparisons} comparisons"
        );
        let block = [3500, 3501, 3502, 3503, 3504, 3505];
        tree.splice(1000..1006, &block);
        expected.splice(1000..1006, block);
        let mut actual = Vec::new();
        tree.append_range_to(998..1010, &mut actual);
        assert_eq!(actual, expected[998..1010]);
        assert_eq!(tree.assert_valid(), expected);
    }
}
