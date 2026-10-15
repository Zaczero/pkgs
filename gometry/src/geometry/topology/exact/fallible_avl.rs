use std::cmp::Ordering;

use super::{AvlNode, FallibleAvl};
use crate::error::{ResourceError, Result};
use crate::work::{Unmetered, Work};

#[expect(
    clippy::indexing_slicing,
    reason = "AVL child links are private indices into the node vector and are maintained by rotations"
)]
impl<T> FallibleAvl<T> {
    pub(in crate::geometry::topology) fn with_capacity(capacity: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(capacity),
            root: None,
        }
    }

    pub(in crate::geometry::topology) const fn len(&self) -> usize {
        self.nodes.len()
    }

    #[cfg(test)]
    pub(in crate::geometry::topology) fn tree_height(&self) -> usize {
        self.height(self.root)
    }

    fn height(&self, node: Option<usize>) -> usize {
        node.map_or(0, |node| self.nodes[node].height)
    }

    fn refresh(&mut self, node: usize) {
        let child_height = self
            .height(self.nodes[node].left)
            .max(self.height(self.nodes[node].right));
        self.nodes[node].height = child_height
            .checked_add(1)
            .expect("tree height fits the allocated node vector");
    }

    fn rotate_left(&mut self, root: usize) -> usize {
        let top = self.nodes[root]
            .right
            .expect("a left rotation has a right child");
        self.nodes[root].right = self.nodes[top].left;
        self.nodes[top].left = Some(root);
        self.refresh(root);
        self.refresh(top);
        top
    }

    fn rotate_right(&mut self, root: usize) -> usize {
        let top = self.nodes[root]
            .left
            .expect("a right rotation has a left child");
        self.nodes[root].left = self.nodes[top].right;
        self.nodes[top].right = Some(root);
        self.refresh(root);
        self.refresh(top);
        top
    }

    fn balance(&mut self, root: usize) -> usize {
        self.refresh(root);
        let left_height = self.height(self.nodes[root].left);
        let right_height = self.height(self.nodes[root].right);
        if left_height > right_height && left_height.abs_diff(right_height) > 1 {
            let left = self.nodes[root]
                .left
                .expect("a left-heavy node has a left child");
            if self.height(self.nodes[left].left) < self.height(self.nodes[left].right) {
                self.nodes[root].left = Some(self.rotate_left(left));
            }
            return self.rotate_right(root);
        }
        if right_height > left_height && right_height.abs_diff(left_height) > 1 {
            let right = self.nodes[root]
                .right
                .expect("a right-heavy node has a right child");
            if self.height(self.nodes[right].right) < self.height(self.nodes[right].left) {
                self.nodes[root].right = Some(self.rotate_right(right));
            }
            return self.rotate_left(root);
        }
        root
    }

    pub(in crate::geometry::topology) fn insert(
        &mut self,
        value: T,
        compare: &mut impl FnMut(&T, &T) -> Result<Ordering>,
    ) -> Result<bool> {
        let mut work = Unmetered;
        self.insert_with_work(value, &mut work, &mut |left, right, _work| {
            compare(left, right)
        })
    }

    pub(in crate::geometry::topology) fn insert_with_work<W: Work>(
        &mut self,
        value: T,
        work: &mut W,
        compare: &mut impl FnMut(&T, &T, &mut W) -> Result<Ordering>,
    ) -> Result<bool> {
        fn descend<T, W: Work>(
            tree: &mut FallibleAvl<T>,
            root: Option<usize>,
            value: T,
            work: &mut W,
            compare: &mut impl FnMut(&T, &T, &mut W) -> Result<Ordering>,
        ) -> Result<(usize, bool)> {
            work.charge_into_error(1)?;
            let Some(root) = root else {
                work.charge_into_error(1)?;
                let index = tree.nodes.len();
                tree.nodes.push(AvlNode {
                    value,
                    left: None,
                    right: None,
                    height: 1,
                });
                return Ok((index, true));
            };
            work.charge_into_error(1)?;
            match compare(&value, &tree.nodes[root].value, work)? {
                Ordering::Equal => Ok((root, false)),
                Ordering::Less => {
                    let (left, inserted) =
                        descend(tree, tree.nodes[root].left, value, work, compare)?;
                    if !inserted {
                        return Ok((root, false));
                    }
                    work.charge_into_error(1)?;
                    tree.nodes[root].left = Some(left);
                    Ok((tree.balance(root), inserted))
                },
                Ordering::Greater => {
                    let (right, inserted) =
                        descend(tree, tree.nodes[root].right, value, work, compare)?;
                    if !inserted {
                        return Ok((root, false));
                    }
                    work.charge_into_error(1)?;
                    tree.nodes[root].right = Some(right);
                    Ok((tree.balance(root), inserted))
                },
            }
        }

        let (root, inserted) = descend(self, self.root, value, work, compare)?;
        work.charge_into_error(1)?;
        self.root = Some(root);
        Ok(inserted)
    }

    pub(in crate::geometry::topology) fn get(
        &self,
        value: &T,
        compare: &mut impl FnMut(&T, &T) -> Result<Ordering>,
    ) -> Result<Option<&T>> {
        let mut work = Unmetered;
        self.get_with_work(value, &mut work, &mut |left, right, _work| {
            compare(left, right)
        })
    }

    pub(in crate::geometry::topology) fn get_with_work<W: Work>(
        &self,
        value: &T,
        work: &mut W,
        compare: &mut impl FnMut(&T, &T, &mut W) -> Result<Ordering>,
    ) -> Result<Option<&T>> {
        let mut cursor = self.root;
        while let Some(index) = cursor {
            work.charge_into_error(1)?;
            match compare(value, &self.nodes[index].value, work)? {
                Ordering::Equal => return Ok(Some(&self.nodes[index].value)),
                Ordering::Less => cursor = self.nodes[index].left,
                Ordering::Greater => cursor = self.nodes[index].right,
            }
        }
        Ok(None)
    }

    pub(in crate::geometry::topology) fn in_order_indices_with_work<W: Work>(
        &self,
        work: &mut W,
    ) -> Result<Vec<usize>> {
        work.charge_into_error(
            u64::try_from(self.nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut ordered = Vec::with_capacity(self.nodes.len());
        let mut stack = Vec::with_capacity(self.height(self.root));
        let mut current = self.root;
        loop {
            while let Some(node) = current {
                work.charge_into_error(1)?;
                stack.push(node);
                current = self.nodes[node].left;
            }
            work.charge_into_error(1)?;
            let Some(node) = stack.pop() else {
                break;
            };
            work.charge_into_error(1)?;
            ordered.push(node);
            current = self.nodes[node].right;
        }
        Ok(ordered)
    }

    pub(in crate::geometry::topology) fn remove(
        &mut self,
        value: &T,
        compare: &mut impl FnMut(&T, &T) -> Result<Ordering>,
    ) -> Result<bool>
    where
        T: Clone,
    {
        fn remove_min<T: Clone>(tree: &mut FallibleAvl<T>, root: usize) -> (Option<usize>, usize) {
            let Some(left) = tree.nodes[root].left else {
                return (tree.nodes[root].right, root);
            };
            let (left, minimum) = remove_min(tree, left);
            tree.nodes[root].left = left;
            (Some(tree.balance(root)), minimum)
        }

        fn descend<T: Clone>(
            tree: &mut FallibleAvl<T>,
            root: Option<usize>,
            value: &T,
            compare: &mut impl FnMut(&T, &T) -> Result<Ordering>,
        ) -> Result<(Option<usize>, bool)> {
            let Some(root) = root else {
                return Ok((None, false));
            };
            match compare(value, &tree.nodes[root].value)? {
                Ordering::Less => {
                    let (left, removed) = descend(tree, tree.nodes[root].left, value, compare)?;
                    if removed {
                        tree.nodes[root].left = left;
                        Ok((Some(tree.balance(root)), true))
                    } else {
                        Ok((Some(root), false))
                    }
                },
                Ordering::Greater => {
                    let (right, removed) = descend(tree, tree.nodes[root].right, value, compare)?;
                    if removed {
                        tree.nodes[root].right = right;
                        Ok((Some(tree.balance(root)), true))
                    } else {
                        Ok((Some(root), false))
                    }
                },
                Ordering::Equal => {
                    let (Some(left), Some(right)) = (tree.nodes[root].left, tree.nodes[root].right)
                    else {
                        return Ok((tree.nodes[root].left.or(tree.nodes[root].right), true));
                    };
                    let (right, successor) = remove_min(tree, right);
                    tree.nodes[root].value = tree.nodes[successor].value.clone();
                    tree.nodes[root].left = Some(left);
                    tree.nodes[root].right = right;
                    Ok((Some(tree.balance(root)), true))
                },
            }
        }

        let (root, removed) = descend(self, self.root, value, compare)?;
        if removed {
            self.root = root;
        }
        Ok(removed)
    }

    pub(in crate::geometry::topology) fn predecessor<Q>(
        &self,
        query: &Q,
        mut compare: impl FnMut(&T, &Q) -> Result<Ordering>,
    ) -> Result<Option<&T>> {
        let mut cursor = self.root;
        let mut predecessor = None;
        while let Some(index) = cursor {
            match compare(&self.nodes[index].value, query)? {
                Ordering::Less => {
                    predecessor = Some(&self.nodes[index].value);
                    cursor = self.nodes[index].right;
                },
                Ordering::Equal | Ordering::Greater => cursor = self.nodes[index].left,
            }
        }
        Ok(predecessor)
    }

    pub(in crate::geometry::topology) fn for_each(
        &self,
        visit: &mut impl FnMut(&T) -> Result<()>,
    ) -> Result<()> {
        fn walk<T>(
            tree: &FallibleAvl<T>,
            root: Option<usize>,
            visit: &mut impl FnMut(&T) -> Result<()>,
        ) -> Result<()> {
            let Some(root) = root else {
                return Ok(());
            };
            walk(tree, tree.nodes[root].left, visit)?;
            visit(&tree.nodes[root].value)?;
            walk(tree, tree.nodes[root].right, visit)
        }

        walk(self, self.root, visit)
    }
}
