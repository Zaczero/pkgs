//! Path-copy AVL search sequences for immutable, ordered versions.
//!
//! Each update copies its search and rotation paths, leaving prior roots
//! readable. Item ordering stays with the geometry owner; the nodes store no
//! mutable geometric keys or parent links.

use std::cmp::Ordering;

use crate::error::Result;

#[derive(Clone, Copy, Debug)]
pub(in crate::geometry) struct PersistentOrderNode<T: Copy> {
    pub(in crate::geometry) item: T,
    pub(in crate::geometry) left: Option<u32>,
    pub(in crate::geometry) right: Option<u32>,
    height: u32,
}

#[derive(Debug)]
pub(in crate::geometry) struct PersistentOrder<T: Copy> {
    nodes: Vec<PersistentOrderNode<T>>,
}

impl<T: Copy> crate::HeapSize for PersistentOrder<T> {
    fn heap_bytes(&self) -> usize {
        self.nodes
            .capacity()
            .saturating_mul(std::mem::size_of::<PersistentOrderNode<T>>())
    }
}

impl<T: Copy> Default for PersistentOrder<T> {
    fn default() -> Self {
        Self { nodes: Vec::new() }
    }
}

impl<T: Copy> PersistentOrder<T> {
    pub(in crate::geometry) fn with_capacity(capacity: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(capacity),
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "roots and child links are IDs allocated by this immutable node arena"
    )]
    pub(in crate::geometry) fn node(&self, id: u32) -> PersistentOrderNode<T> {
        self.nodes[id as usize]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "roots and child links are IDs allocated by this immutable node arena"
    )]
    fn height(&self, root: Option<u32>) -> u32 {
        root.map_or(0, |id| self.nodes[id as usize].height)
    }

    fn push_node(&mut self, item: T, left: Option<u32>, right: Option<u32>) -> Result<u32> {
        let id = u32::try_from(self.nodes.len())
            .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        self.nodes.push(PersistentOrderNode {
            item,
            left,
            right,
            height: self.height(left).max(self.height(right)).saturating_add(1),
        });
        Ok(id)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "child links are IDs allocated by this immutable node arena"
    )]
    fn balance(&mut self, root: u32) -> Result<u32> {
        let node = self.nodes[root as usize];
        let left_height = self.height(node.left);
        let right_height = self.height(node.right);
        let left_heavy = left_height > right_height && left_height.abs_diff(right_height) > 1;
        let right_heavy = right_height > left_height && right_height.abs_diff(left_height) > 1;
        if left_heavy {
            let left_id = node.left.expect("left-heavy AVL node has a left child");
            let left = self.nodes[left_id as usize];
            if self.height(left.left) < self.height(left.right) {
                let rotated = self.rotate_left(left_id)?;
                let root = self.push_node(node.item, Some(rotated), node.right)?;
                return self.rotate_right(root);
            }
            return self.rotate_right(root);
        }
        if right_heavy {
            let right_id = node.right.expect("right-heavy AVL node has a right child");
            let right = self.nodes[right_id as usize];
            if self.height(right.right) < self.height(right.left) {
                let rotated = self.rotate_right(right_id)?;
                let root = self.push_node(node.item, node.left, Some(rotated))?;
                return self.rotate_left(root);
            }
            return self.rotate_left(root);
        }
        Ok(root)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "rotation roots and children are IDs allocated by this immutable node arena"
    )]
    fn rotate_left(&mut self, root: u32) -> Result<u32> {
        let node = self.nodes[root as usize];
        let pivot_id = node.right.expect("left rotation has a right child");
        let pivot = self.nodes[pivot_id as usize];
        let left = self.push_node(node.item, node.left, pivot.left)?;
        self.push_node(pivot.item, Some(left), pivot.right)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "rotation roots and children are IDs allocated by this immutable node arena"
    )]
    fn rotate_right(&mut self, root: u32) -> Result<u32> {
        let node = self.nodes[root as usize];
        let pivot_id = node.left.expect("right rotation has a left child");
        let pivot = self.nodes[pivot_id as usize];
        let right = self.push_node(node.item, pivot.right, node.right)?;
        self.push_node(pivot.item, pivot.left, Some(right))
    }

    pub(in crate::geometry) fn insert(
        &mut self,
        root: Option<u32>,
        item: T,
        mut compare: impl FnMut(T, T) -> Result<Ordering>,
    ) -> Result<u32> {
        self.insert_from(root, item, &mut compare)
    }

    fn insert_from(
        &mut self,
        root: Option<u32>,
        item: T,
        compare: &mut impl FnMut(T, T) -> Result<Ordering>,
    ) -> Result<u32> {
        let Some(root) = root else {
            return self.push_node(item, None, None);
        };
        let node = self.node(root);
        match compare(item, node.item)? {
            Ordering::Less => {
                let left = self.insert_from(node.left, item, compare)?;
                let rebuilt = self.push_node(node.item, Some(left), node.right)?;
                self.balance(rebuilt)
            },
            Ordering::Greater => {
                let right = self.insert_from(node.right, item, compare)?;
                let rebuilt = self.push_node(node.item, node.left, Some(right))?;
                self.balance(rebuilt)
            },
            Ordering::Equal => Err(crate::geometry::GeometryErrorKind::Invalid(
                "persistent AVL inserted a duplicate ordered item".into(),
            )
            .into()),
        }
    }

    pub(in crate::geometry) fn remove(
        &mut self,
        root: Option<u32>,
        item: T,
        mut compare: impl FnMut(T, T) -> Result<Ordering>,
    ) -> Result<Option<u32>> {
        self.remove_from(root, item, &mut compare)
    }

    fn remove_from(
        &mut self,
        root: Option<u32>,
        item: T,
        compare: &mut impl FnMut(T, T) -> Result<Ordering>,
    ) -> Result<Option<u32>> {
        let Some(root) = root else {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "persistent AVL could not find a removed item".into(),
            )
            .into());
        };
        let node = self.node(root);
        match compare(item, node.item)? {
            Ordering::Less => {
                let left = self.remove_from(node.left, item, compare)?;
                let rebuilt = self.push_node(node.item, left, node.right)?;
                Ok(Some(self.balance(rebuilt)?))
            },
            Ordering::Greater => {
                let right = self.remove_from(node.right, item, compare)?;
                let rebuilt = self.push_node(node.item, node.left, right)?;
                Ok(Some(self.balance(rebuilt)?))
            },
            Ordering::Equal => match (node.left, node.right) {
                (None, right) => Ok(right),
                (left, None) => Ok(left),
                (Some(left), Some(right)) => {
                    let (successor, right) = self.remove_min(right)?;
                    let rebuilt = self.push_node(successor, Some(left), right)?;
                    Ok(Some(self.balance(rebuilt)?))
                },
            },
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the root and child links are IDs allocated by this immutable node arena"
    )]
    fn remove_min(&mut self, root: u32) -> Result<(T, Option<u32>)> {
        let node = self.nodes[root as usize];
        let Some(left) = node.left else {
            return Ok((node.item, node.right));
        };
        let (item, left) = self.remove_min(left)?;
        let rebuilt = self.push_node(node.item, left, node.right)?;
        Ok((item, Some(self.balance(rebuilt)?)))
    }
}
