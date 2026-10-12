use std::cmp::Ordering;
use std::ops::Range;
#[cfg(test)]
use std::{cell::Cell, thread_local};

use crate::geometry::{Unmetered, Work, WorkResult};

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct UpdateProgress {
    attempted: usize,
    completed: usize,
    nonroot_attempted: usize,
    nonroot_completed: usize,
}

#[cfg(test)]
thread_local! {
    static UPDATE_PROGRESS: Cell<UpdateProgress> = const { Cell::new(UpdateProgress {
        attempted: 0,
        completed: 0,
        nonroot_attempted: 0,
        nonroot_completed: 0,
    }) };
}

#[cfg(test)]
fn reset_update_progress() {
    UPDATE_PROGRESS.with(|progress| progress.set(UpdateProgress::default()));
}

#[cfg(test)]
fn update_progress() -> UpdateProgress {
    UPDATE_PROGRESS.with(Cell::get)
}

#[cfg(test)]
fn record_update_attempt(nonroot: bool) {
    UPDATE_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.attempted = value.attempted.wrapping_add(1);
        if nonroot {
            value.nonroot_attempted = value.nonroot_attempted.wrapping_add(1);
        }
        progress.set(value);
    });
}

#[cfg(test)]
fn record_update_complete(nonroot: bool) {
    UPDATE_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.completed = value.completed.wrapping_add(1);
        if nonroot {
            value.nonroot_completed = value.nonroot_completed.wrapping_add(1);
        }
        progress.set(value);
    });
}

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
    fn allocate<W: Work>(&mut self, item: T, work: &mut W) -> WorkResult<W, usize> {
        work.charge(1)?;
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
            Ok(index)
        } else {
            self.nodes.push(Some(node));
            Ok(self.nodes.len().wrapping_sub(1))
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the removed slot is a live arena node returned by this tree's rank descent"
    )]
    fn release<W: Work>(&mut self, index: usize, work: &mut W) -> WorkResult<W, ()> {
        work.charge(1)?;
        self.nodes[index] = None;
        self.free.push(index);
        Ok(())
    }

    fn set_left<W: Work>(
        &mut self,
        parent: usize,
        child: Option<usize>,
        work: &mut W,
    ) -> WorkResult<W, ()> {
        work.charge(1)?;
        self.node_mut(parent).left = child;
        if let Some(child) = child {
            self.node_mut(child).parent = Some(parent);
        }
        Ok(())
    }

    fn set_right<W: Work>(
        &mut self,
        parent: usize,
        child: Option<usize>,
        work: &mut W,
    ) -> WorkResult<W, ()> {
        work.charge(1)?;
        self.node_mut(parent).right = child;
        if let Some(child) = child {
            self.node_mut(child).parent = Some(parent);
        }
        Ok(())
    }

    fn take_left<W: Work>(&mut self, parent: usize, work: &mut W) -> WorkResult<W, Option<usize>> {
        work.charge(1)?;
        let child = self.node_mut(parent).left.take();
        if let Some(child) = child {
            self.node_mut(child).parent = None;
        }
        Ok(child)
    }

    fn take_right<W: Work>(&mut self, parent: usize, work: &mut W) -> WorkResult<W, Option<usize>> {
        work.charge(1)?;
        let child = self.node_mut(parent).right.take();
        if let Some(child) = child {
            self.node_mut(child).parent = None;
        }
        Ok(child)
    }

    fn update<W: Work>(&mut self, index: usize, work: &mut W) -> WorkResult<W, ()> {
        #[cfg(test)]
        let nonroot = self.node(index).parent.is_some();
        #[cfg(test)]
        record_update_attempt(nonroot);
        work.charge(1)?;
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
        #[cfg(test)]
        record_update_complete(nonroot);
        Ok(())
    }

    fn balance(&self, index: usize) -> isize {
        let node = self.node(index);
        // AVL height is logarithmic in the usize-bounded node count, so both
        // heights and their difference fit in `isize`.
        (self.height(node.left) as isize).wrapping_sub(self.height(node.right) as isize)
    }

    fn rotate_left<W: Work>(&mut self, root: usize, work: &mut W) -> WorkResult<W, usize> {
        work.charge(1)?;
        let parent = self.node(root).parent;
        let pivot = self
            .node(root)
            .right
            .expect("left rotation has a right child");
        let middle = self.take_left(pivot, work)?;
        self.set_right(root, middle, work)?;
        self.set_left(pivot, Some(root), work)?;
        self.node_mut(pivot).parent = parent;
        self.update(root, work)?;
        self.update(pivot, work)?;
        Ok(pivot)
    }

    fn rotate_right<W: Work>(&mut self, root: usize, work: &mut W) -> WorkResult<W, usize> {
        work.charge(1)?;
        let parent = self.node(root).parent;
        let pivot = self
            .node(root)
            .left
            .expect("right rotation has a left child");
        let middle = self.take_right(pivot, work)?;
        self.set_left(root, middle, work)?;
        self.set_right(pivot, Some(root), work)?;
        self.node_mut(pivot).parent = parent;
        self.update(root, work)?;
        self.update(pivot, work)?;
        Ok(pivot)
    }

    fn rebalance<W: Work>(&mut self, root: usize, work: &mut W) -> WorkResult<W, usize> {
        work.charge(1)?;
        self.update(root, work)?;
        match self.balance(root) {
            balance if balance > 1 => {
                let left = self.node(root).left.expect("left-heavy node has a child");
                if self.balance(left) < 0 {
                    let left = self.rotate_left(left, work)?;
                    self.set_left(root, Some(left), work)?;
                }
                self.rotate_right(root, work)
            },
            balance if balance < -1 => {
                let right = self.node(root).right.expect("right-heavy node has a child");
                if self.balance(right) > 0 {
                    let right = self.rotate_right(right, work)?;
                    self.set_right(root, Some(right), work)?;
                }
                self.rotate_left(root, work)
            },
            _ => Ok(root),
        }
    }

    /// Rank of the first item for which `compare(item)` is not `Less`.
    pub(in crate::geometry) fn lower_bound_by(
        &self,
        mut compare: impl FnMut(T) -> crate::error::Result<std::cmp::Ordering>,
    ) -> crate::error::Result<usize> {
        self.lower_bound_by_with_work(&mut Unmetered, |item, _work| compare(item))
    }

    pub(in crate::geometry) fn lower_bound_by_with_work<W: Work>(
        &self,
        work: &mut W,
        mut compare: impl FnMut(T, &mut W) -> crate::error::Result<std::cmp::Ordering>,
    ) -> crate::error::Result<usize> {
        self.partition_point_by_with_work(work, |item, work| Ok(compare(item, work)?.is_lt()))
    }

    /// Rank at the end of the prefix for which `before(item)` is true.
    pub(in crate::geometry) fn partition_point_by(
        &self,
        mut before: impl FnMut(T) -> crate::error::Result<bool>,
    ) -> crate::error::Result<usize> {
        self.partition_point_by_with_work(&mut Unmetered, |item, _work| before(item))
    }

    pub(in crate::geometry) fn partition_point_by_with_work<W: Work>(
        &self,
        work: &mut W,
        mut before: impl FnMut(T, &mut W) -> crate::error::Result<bool>,
    ) -> crate::error::Result<usize> {
        let (mut current, mut rank) = (self.root, 0_usize);
        while let Some(index) = current {
            work.charge_into_error(1)?;
            let node = self.node(index);
            let left_len = self.subtree_len(node.left);
            if before(node.item, work)? {
                rank = rank.wrapping_add(left_len).wrapping_add(1);
                current = node.right;
            } else {
                current = node.left;
            }
        }
        Ok(rank)
    }

    pub(in crate::geometry) fn item_at_with_work<W: Work>(
        &self,
        rank: usize,
        work: &mut W,
    ) -> WorkResult<W, Option<T>> {
        if rank >= self.len() {
            return Ok(None);
        }
        let mut current = self.root;
        let mut rank = rank;
        while let Some(index) = current {
            work.charge(1)?;
            let node = self.node(index);
            let left_len = self.subtree_len(node.left);
            match rank.cmp(&left_len) {
                Ordering::Less => current = node.left,
                Ordering::Equal => return Ok(Some(node.item)),
                Ordering::Greater => {
                    rank = rank.wrapping_sub(left_len).wrapping_sub(1);
                    current = node.right;
                },
            }
        }
        Ok(None)
    }

    pub(in crate::geometry) fn insert_at_rank(&mut self, rank: usize, item: T) {
        self.insert_at_rank_with_work(rank, item, &mut Unmetered)
            .unwrap();
    }

    #[expect(
        clippy::panic_in_result_fn,
        reason = "the sweep status validates insertion ranks before entering the AVL mutation"
    )]
    pub(in crate::geometry) fn insert_at_rank_with_work<W: Work>(
        &mut self,
        rank: usize,
        item: T,
        work: &mut W,
    ) -> WorkResult<W, ()> {
        assert!(rank <= self.len(), "AVL insertion rank is in bounds");
        let node = self.allocate(item, work)?;
        self.root = Some(self.insert(self.root, rank, node, work)?);
        if let Some(root) = self.root {
            work.charge(1)?;
            self.node_mut(root).parent = None;
        }
        Ok(())
    }

    fn insert<W: Work>(
        &mut self,
        root: Option<usize>,
        rank: usize,
        inserted: usize,
        work: &mut W,
    ) -> WorkResult<W, usize> {
        let Some(root) = root else {
            return Ok(inserted);
        };
        work.charge(1)?;
        let left_len = self.subtree_len(self.node(root).left);
        if rank <= left_len {
            let left = self.insert(self.node(root).left, rank, inserted, work)?;
            self.set_left(root, Some(left), work)?;
        } else {
            let right = self.insert(
                self.node(root).right,
                rank.wrapping_sub(left_len).wrapping_sub(1),
                inserted,
                work,
            )?;
            self.set_right(root, Some(right), work)?;
        }
        self.rebalance(root, work)
    }

    pub(in crate::geometry) fn remove_at_rank(&mut self, rank: usize) -> T {
        self.remove_at_rank_with_work(rank, &mut Unmetered).unwrap()
    }

    #[expect(
        clippy::panic_in_result_fn,
        reason = "the sweep status validates removal ranks before entering the AVL mutation"
    )]
    pub(in crate::geometry) fn remove_at_rank_with_work<W: Work>(
        &mut self,
        rank: usize,
        work: &mut W,
    ) -> WorkResult<W, T> {
        assert!(rank < self.len(), "AVL removal rank is in bounds");
        let (root, item, removed) = self.remove(self.root, rank, work)?;
        self.root = root;
        if let Some(root) = self.root {
            work.charge(1)?;
            self.node_mut(root).parent = None;
        }
        self.release(removed, work)?;
        Ok(item)
    }

    fn remove<W: Work>(
        &mut self,
        root: Option<usize>,
        rank: usize,
        work: &mut W,
    ) -> WorkResult<W, (Option<usize>, T, usize)> {
        work.charge(1)?;
        let root = root.expect("AVL removal descends through a live node");
        let left_len = self.subtree_len(self.node(root).left);
        if rank < left_len {
            let (left, item, removed) = self.remove(self.node(root).left, rank, work)?;
            self.set_left(root, left, work)?;
            return Ok((Some(self.rebalance(root, work)?), item, removed));
        }
        if rank > left_len {
            let (right, item, removed) = self.remove(
                self.node(root).right,
                rank.wrapping_sub(left_len).wrapping_sub(1),
                work,
            )?;
            self.set_right(root, right, work)?;
            return Ok((Some(self.rebalance(root, work)?), item, removed));
        }

        let item = self.node(root).item;
        let left = self.take_left(root, work)?;
        let right = self.take_right(root, work)?;
        Ok((self.join(left, right, work)?, item, root))
    }

    /// Replace one contiguous rank range with items already sorted in their
    /// desired order. All fallible geometric comparisons happen before entry.
    pub(in crate::geometry) fn splice(&mut self, range: Range<usize>, items: &[T]) {
        self.splice_with_work(range, items, &mut Unmetered).unwrap();
    }

    #[expect(
        clippy::panic_in_result_fn,
        reason = "the sweep status supplies a validated in-bounds splice range"
    )]
    pub(in crate::geometry) fn splice_with_work<W: Work>(
        &mut self,
        range: Range<usize>,
        items: &[T],
        work: &mut W,
    ) -> WorkResult<W, ()> {
        assert!(
            range.start <= range.end && range.end <= self.len(),
            "AVL splice range is in bounds"
        );
        work.charge(1)?;
        let root = self.root.take();
        let (prefix, tail) = self.split(root, range.start, work)?;
        let (removed, suffix) = self.split(tail, range.end.wrapping_sub(range.start), work)?;
        self.release_tree(removed, work)?;
        let inserted = self.build_balanced(items, 0, items.len(), work)?;
        let prefix = self.join(prefix, inserted, work)?;
        let joined = self.join(prefix, suffix, work)?;
        self.root = joined;
        if let Some(root) = self.root {
            work.charge(1)?;
            self.node_mut(root).parent = None;
        }
        Ok(())
    }

    fn split<W: Work>(
        &mut self,
        root: Option<usize>,
        rank: usize,
        work: &mut W,
    ) -> WorkResult<W, (Option<usize>, Option<usize>)> {
        let Some(root) = root else {
            return Ok((None, None));
        };
        work.charge(1)?;
        let left = self.take_left(root, work)?;
        let right = self.take_right(root, work)?;
        let left_len = self.subtree_len(left);
        let (before, after) = if rank <= left_len {
            let (before, left_remainder) = self.split(left, rank, work)?;
            (
                before,
                Some(self.join_with_root(left_remainder, root, right, work)?),
            )
        } else {
            let (right_prefix, after) =
                self.split(right, rank.wrapping_sub(left_len).wrapping_sub(1), work)?;
            (
                Some(self.join_with_root(left, root, right_prefix, work)?),
                after,
            )
        };
        if let Some(before) = before {
            work.charge(1)?;
            self.node_mut(before).parent = None;
        }
        if let Some(after) = after {
            work.charge(1)?;
            self.node_mut(after).parent = None;
        }
        Ok((before, after))
    }

    fn join<W: Work>(
        &mut self,
        left: Option<usize>,
        right: Option<usize>,
        work: &mut W,
    ) -> WorkResult<W, Option<usize>> {
        match (left, right) {
            (None, right) => Ok(right),
            (left, None) => Ok(left),
            (Some(left), Some(right)) => {
                work.charge(1)?;
                let (right, pivot) = self.take_min(right, work)?;
                Ok(Some(self.join_with_root(Some(left), pivot, right, work)?))
            },
        }
    }

    fn take_min<W: Work>(
        &mut self,
        root: usize,
        work: &mut W,
    ) -> WorkResult<W, (Option<usize>, usize)> {
        work.charge(1)?;
        let left = self.node(root).left;
        let Some(left) = left else {
            let right = self.take_right(root, work)?;
            work.charge(1)?;
            self.node_mut(root).parent = None;
            self.update(root, work)?;
            return Ok((right, root));
        };
        let (left, minimum) = self.take_min(left, work)?;
        self.set_left(root, left, work)?;
        Ok((Some(self.rebalance(root, work)?), minimum))
    }

    fn join_with_root<W: Work>(
        &mut self,
        left: Option<usize>,
        pivot: usize,
        right: Option<usize>,
        work: &mut W,
    ) -> WorkResult<W, usize> {
        work.charge(1)?;
        let left_height = self.height(left);
        let right_height = self.height(right);
        if left_height > right_height.wrapping_add(1) {
            let root = left.expect("tall left join has a root");
            let right_child = self.take_right(root, work)?;
            let joined = self.join_with_root(right_child, pivot, right, work)?;
            self.set_right(root, Some(joined), work)?;
            return self.rebalance(root, work);
        }
        if right_height > left_height.wrapping_add(1) {
            let root = right.expect("tall right join has a root");
            let left_child = self.take_left(root, work)?;
            let joined = self.join_with_root(left, pivot, left_child, work)?;
            self.set_left(root, Some(joined), work)?;
            return self.rebalance(root, work);
        }

        work.charge(1)?;
        self.node_mut(pivot).left = None;
        self.node_mut(pivot).right = None;
        self.node_mut(pivot).parent = None;
        self.set_left(pivot, left, work)?;
        self.set_right(pivot, right, work)?;
        self.update(pivot, work)?;
        Ok(pivot)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the nonempty recursive range proves its midpoint is inside the input slice"
    )]
    fn build_balanced<W: Work>(
        &mut self,
        items: &[T],
        start: usize,
        end: usize,
        work: &mut W,
    ) -> WorkResult<W, Option<usize>> {
        if start == end {
            return Ok(None);
        }
        let middle = start.wrapping_add(end.wrapping_sub(start) / 2);
        let root = self.allocate(items[middle], work)?;
        let left = self.build_balanced(items, start, middle, work)?;
        let right = self.build_balanced(items, middle.wrapping_add(1), end, work)?;
        self.set_left(root, left, work)?;
        self.set_right(root, right, work)?;
        self.update(root, work)?;
        Ok(Some(root))
    }

    fn release_tree<W: Work>(&mut self, root: Option<usize>, work: &mut W) -> WorkResult<W, ()> {
        let Some(root) = root else {
            return Ok(());
        };
        work.charge(1)?;
        let left = self.take_left(root, work)?;
        let right = self.take_right(root, work)?;
        self.release_tree(left, work)?;
        self.release_tree(right, work)?;
        self.release(root, work)
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
    use std::cell::Cell;

    use super::{OrderTree, UpdateProgress, reset_update_progress, update_progress};
    use crate::error::{ErrorKind, Result};
    use crate::geometry::embedding_search::{EvaluationBudget, EvaluationOnly};
    use crate::geometry::{Unmetered, Work};

    #[derive(Clone, Copy)]
    struct TreeChargeEvent {
        spent_before: u128,
        ticks: u128,
        rank_comparisons: usize,
        progress: UpdateProgress,
    }

    struct TreeRecordingWork<'a> {
        budget: &'a mut EvaluationBudget,
        rank_comparisons: &'a Cell<usize>,
        spent: u128,
        charges: Vec<TreeChargeEvent>,
        refused: Option<TreeChargeEvent>,
    }

    impl<'a> TreeRecordingWork<'a> {
        fn new(budget: &'a mut EvaluationBudget, rank_comparisons: &'a Cell<usize>) -> Self {
            Self {
                budget,
                rank_comparisons,
                spent: 0,
                charges: Vec::new(),
                refused: None,
            }
        }

        fn record(&mut self, ticks: u128, result: &Result<()>) {
            let event = TreeChargeEvent {
                spent_before: self.spent,
                ticks,
                rank_comparisons: self.rank_comparisons.get(),
                progress: update_progress(),
            };
            if result.is_ok() {
                self.charges.push(event);
                self.spent = self.spent.wrapping_add(ticks);
            } else {
                self.refused = Some(event);
            }
        }
    }

    impl Work for TreeRecordingWork<'_> {
        type Error = crate::error::Error;

        fn charge(&mut self, ticks: u64) -> Result<()> {
            let result = <EvaluationBudget as Work>::charge(self.budget, ticks);
            self.record(u128::from(ticks), &result);
            result
        }

        fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
            let ticks = (count as u128).wrapping_mul(ticks_per_item as u128);
            let result =
                <EvaluationBudget as Work>::charge_product(self.budget, count, ticks_per_item);
            self.record(ticks, &result);
            result
        }
    }

    fn budget_refusing_before(
        operation: &'static str,
        event: &TreeChargeEvent,
    ) -> EvaluationBudget {
        let remaining = event
            .spent_before
            .checked_add(event.ticks)
            .and_then(|ticks| ticks.checked_sub(1))
            .expect("the target tree charge is positive and fits the budget");
        EvaluationBudget::with_remaining_ticks_for_test(operation, 1, remaining)
            .expect("calibrated tree budget is representable")
    }

    fn assert_limit_error(error: &crate::error::Error, operation: &'static str) {
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(crate::geometry::GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: actual,
                ..
            }) if *actual == operation
        ));
    }

    fn filled_tree(count: usize) -> OrderTree<usize> {
        let mut tree = OrderTree::with_capacity(count);
        for item in 0..count {
            tree.insert_at_rank(item, item);
        }
        tree
    }

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

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the recorder assertions pin AVL rank probes and refusal before the next lookup"
    )]
    fn native_order_tree_nonroot_lookup_refuses_before_next_rank_probe() -> Result<()> {
        const OPERATION: &str = "native_order_tree_lookup";
        reset_update_progress();
        let tree = filled_tree(63);
        let comparisons = Cell::new(0_usize);
        let (rank, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = TreeRecordingWork::new(&mut budget, &comparisons);
            let rank = tree.partition_point_by_with_work(&mut work, |item, _work| {
                comparisons.set(comparisons.get().wrapping_add(1));
                Ok(item < 42)
            })?;
            (rank, work.charges)
        };
        assert_eq!(rank, 42);
        assert!(comparisons.get() > 1);
        let target = charges
            .into_iter()
            .find(|event| event.rank_comparisons == 1 && event.ticks > 0)
            .expect("nonroot rank descent pays before the next comparison");

        reset_update_progress();
        let comparisons = Cell::new(0_usize);
        let mut budget = budget_refusing_before(OPERATION, &target);
        let error = {
            let mut work = TreeRecordingWork::new(&mut budget, &comparisons);
            let error = tree
                .partition_point_by_with_work(&mut work, |item, _work| {
                    comparisons.set(comparisons.get().wrapping_add(1));
                    Ok(item < 42)
                })
                .expect_err("rank descent refuses before its next node comparison");
            assert_eq!(
                work.refused.map(|event| event.rank_comparisons),
                Some(target.rank_comparisons)
            );
            error
        };
        assert_limit_error(&error, OPERATION);
        assert_eq!(comparisons.get(), 1);

        assert_eq!(
            tree.partition_point_by(|item| Ok(item < 42))?,
            rank,
            "ordinary rank lookup preserves the same order"
        );
        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        let evaluation_comparisons = Cell::new(0_usize);
        {
            let mut work = EvaluationOnly::new(&mut ordinary_budget);
            let evaluation_rank = tree.partition_point_by_with_work(&mut work, |item, _work| {
                evaluation_comparisons.set(evaluation_comparisons.get().wrapping_add(1));
                Ok(item < 42)
            })?;
            assert_eq!(evaluation_rank, rank);
        }
        assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the recorder assertions pin AVL metadata work and refusal before the next update"
    )]
    fn native_order_tree_nonroot_update_refuses_before_metadata_write() -> Result<()> {
        const OPERATION: &str = "native_order_tree_update";
        let mut tree = filled_tree(31);
        reset_update_progress();
        let comparisons = Cell::new(0_usize);
        let charges = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = TreeRecordingWork::new(&mut budget, &comparisons);
            tree.insert_at_rank_with_work(15, 999, &mut work)?;
            work.charges
        };
        let target = charges
            .into_iter()
            .find(|event| {
                event.progress.nonroot_attempted == event.progress.nonroot_completed.wrapping_add(1)
                    && event.progress.nonroot_attempted > 0
            })
            .expect("insertion rebalances a nonroot node under the same Work owner");
        assert_eq!(tree.assert_valid().len(), 32);
        reset_update_progress();

        let mut tree = filled_tree(31);
        reset_update_progress();
        let mut budget = budget_refusing_before(OPERATION, &target);
        let error = {
            let mut work = TreeRecordingWork::new(&mut budget, &comparisons);
            let error = tree
                .insert_at_rank_with_work(15, 999, &mut work)
                .expect_err("nonroot metadata update refuses before writing");
            assert_eq!(
                work.refused.map(|event| event.progress),
                Some(target.progress)
            );
            error
        };
        assert_limit_error(&error, OPERATION);
        assert_eq!(update_progress(), target.progress);

        let mut unmetered_tree = filled_tree(31);
        unmetered_tree.insert_at_rank_with_work(15, 999, &mut Unmetered)?;
        let expected = unmetered_tree.assert_valid();
        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        let mut evaluation_tree = filled_tree(31);
        {
            let mut work = EvaluationOnly::new(&mut ordinary_budget);
            evaluation_tree.insert_at_rank_with_work(15, 999, &mut work)?;
        }
        assert_eq!(evaluation_tree.assert_valid(), expected);
        assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
        Ok(())
    }
}
