//! Exact intersection decision sweep for a normalized polygon ring.
//!
//! Lexicographic endpoint order is the sweep in the symbolic frame
//! `(x + εy, y)`: vertical segments need no special arithmetic. Until a
//! forbidden contact, active segments keep their order. Checking neighbours
//! on insertion and removal therefore detects a contact before an order
//! change could invalidate the tree. Adjacent edges meet only at their shared
//! endpoint; removing incoming edges before inserting outgoing edges handles
//! that permitted order change without constructed intersection events.

use std::cmp::Ordering;

use super::earcut::ring_segments_adjacent;
use crate::geometry::{Bounds, Orientation, Point, Segment, XY, orientation, segments_intersect};

#[cfg(test)]
thread_local! {
    static WORK: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn count_work<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    WORK.with(|count| count.set(0));
    let result = operation();
    (result, WORK.with(std::cell::Cell::get))
}

#[cfg(test)]
pub(super) fn record_work() {
    WORK.with(|count| count.set(count.get().wrapping_add(1)));
}

fn point_order(a: Point, b: Point) -> Ordering {
    point_order_xy(a.xy(), b.xy())
}

fn point_order_xy(a: XY, b: XY) -> Ordering {
    // Ring admission has already excluded nonfinite coordinates. Numeric
    // comparison keeps both signs of zero at the same sweep position.
    a.x.partial_cmp(&b.x)
        .expect("finite ring x")
        .then_with(|| a.y.partial_cmp(&b.y).expect("finite ring y"))
}

#[expect(
    clippy::indexing_slicing,
    reason = "event and edge ordinals enumerate this unchanged admitted ring"
)]
pub(super) fn ring_has_contact(ring: &[Point]) -> bool {
    // For short rings, direct box rejection costs less than sorting events
    // and maintaining the active tree.
    if ring.len() <= 32 {
        return direct_ring_contact(ring);
    }
    let mut events: Vec<_> = (0..ring.len()).collect();
    events.sort_unstable_by(|&a, &b| point_order(ring[a], ring[b]));
    let mut active = ActiveSegments::new(ring);
    for vertex in events {
        let previous = if vertex == 0 {
            ring.len().wrapping_sub(1)
        } else {
            vertex.wrapping_sub(1)
        };
        let next = if vertex.wrapping_add(1) == ring.len() {
            0
        } else {
            vertex.wrapping_add(1)
        };
        let incoming = [(previous, previous), (vertex, next)];
        for (edge, other) in incoming {
            if point_order(ring[other], ring[vertex]).is_lt() && active.remove(edge) {
                return true;
            }
        }
        for (edge, other) in incoming {
            if point_order(ring[vertex], ring[other]).is_lt() && active.insert(edge) {
                return true;
            }
        }
    }
    false
}

fn direct_ring_contact(ring: &[Point]) -> bool {
    let segments: Vec<_> = ring
        .iter()
        .zip(ring.iter().cycle().skip(1))
        .map(|(&a, &b)| {
            (
                Segment {
                    start: a.xy(),
                    end: b.xy(),
                },
                Bounds::new_raw(a.x.min(b.x), a.y.min(b.y), a.x.max(b.x), a.y.max(b.y)),
            )
        })
        .collect();
    for (a, &(left, bounds)) in segments.iter().enumerate() {
        for (b, &(right, other)) in segments.iter().enumerate().skip(a.wrapping_add(2)) {
            #[cfg(test)]
            record_work();
            if !ring_segments_adjacent(ring.len(), a, b)
                && bounds.intersects(other)
                && segments_intersect(left, right)
            {
                return true;
            }
        }
    }
    false
}

#[derive(Clone, Default)]
struct ActiveNode {
    parent: Option<usize>,
    children: [Option<usize>; 2],
    height: usize,
}

struct ActiveSegments {
    segments: Vec<Segment>,
    nodes: Vec<ActiveNode>,
    root: Option<usize>,
}

#[expect(
    clippy::indexing_slicing,
    reason = "segment ordinals and AVL links are built from this unchanged ring and never escape the sweep"
)]
impl ActiveSegments {
    fn new(ring: &[Point]) -> Self {
        Self {
            segments: ring
                .iter()
                .zip(ring.iter().cycle().skip(1))
                .map(|(&a, &b)| {
                    let (a, b) = if point_order(a, b).is_lt() {
                        (a, b)
                    } else {
                        (b, a)
                    };
                    Segment {
                        start: a.xy(),
                        end: b.xy(),
                    }
                })
                .collect(),
            nodes: vec![ActiveNode::default(); ring.len()],
            root: None,
        }
    }

    fn endpoints(&self, edge: usize) -> (XY, XY) {
        let segment = self.segments[edge];
        (segment.start, segment.end)
    }

    fn contact(&self, a: usize, b: usize) -> bool {
        #[cfg(test)]
        record_work();
        if ring_segments_adjacent(self.segments.len(), a.min(b), a.max(b)) {
            return false;
        }
        segments_intersect(self.segments[a], self.segments[b])
    }

    fn order(&self, a: usize, b: usize) -> Ordering {
        #[cfg(test)]
        record_work();
        let (a0, a1) = self.endpoints(a);
        let (b0, b1) = self.endpoints(b);
        let sign = match point_order_xy(a0, b0) {
            Ordering::Less => orientation(a0, a1, b0),
            Ordering::Greater => {
                return match orientation(b0, b1, a0) {
                    Orientation::CounterClockwise => Ordering::Greater,
                    Orientation::Clockwise => Ordering::Less,
                    Orientation::Collinear => a.cmp(&b),
                };
            },
            Ordering::Equal => orientation(a0, a1, b1),
        };
        match sign {
            Orientation::CounterClockwise => Ordering::Less,
            Orientation::Clockwise => Ordering::Greater,
            Orientation::Collinear => a.cmp(&b),
        }
    }

    fn height(&self, node: Option<usize>) -> usize {
        node.map_or(0, |index| self.nodes[index].height)
    }

    fn update(&mut self, node: usize) {
        self.nodes[node].height = self
            .height(self.nodes[node].children[0])
            .max(self.height(self.nodes[node].children[1]))
            .wrapping_add(1);
    }

    fn replace(&mut self, old: usize, new: Option<usize>) {
        let parent = self.nodes[old].parent;
        if let Some(parent) = parent {
            let side = usize::from(self.nodes[parent].children[1] == Some(old));
            self.nodes[parent].children[side] = new;
        } else {
            self.root = new;
        }
        if let Some(new) = new {
            self.nodes[new].parent = parent;
        }
    }

    fn rotate(&mut self, node: usize, side: usize) -> usize {
        let raised = self.nodes[node].children[side].expect("rotation child");
        let middle = self.nodes[raised].children[side ^ 1];
        self.replace(node, Some(raised));
        self.nodes[node].children[side] = middle;
        if let Some(middle) = middle {
            self.nodes[middle].parent = Some(node);
        }
        self.nodes[raised].children[side ^ 1] = Some(node);
        self.nodes[node].parent = Some(raised);
        self.update(node);
        self.update(raised);
        raised
    }

    fn rebalance(&mut self, mut cursor: Option<usize>) {
        while let Some(mut node) = cursor {
            self.update(node);
            let [left, right] = self.nodes[node].children;
            let lh = self.height(left);
            let rh = self.height(right);
            if lh > rh.wrapping_add(1) {
                let left = left.expect("left-heavy subtree");
                if self.height(self.nodes[left].children[0])
                    < self.height(self.nodes[left].children[1])
                {
                    self.rotate(left, 1);
                }
                node = self.rotate(node, 0);
            } else if rh > lh.wrapping_add(1) {
                let right = right.expect("right-heavy subtree");
                if self.height(self.nodes[right].children[1])
                    < self.height(self.nodes[right].children[0])
                {
                    self.rotate(right, 0);
                }
                node = self.rotate(node, 1);
            }
            cursor = self.nodes[node].parent;
        }
    }

    fn neighbour(&self, node: usize, side: usize) -> Option<usize> {
        if let Some(mut child) = self.nodes[node].children[side] {
            while let Some(next) = self.nodes[child].children[side ^ 1] {
                child = next;
            }
            return Some(child);
        }
        let mut child = node;
        while let Some(parent) = self.nodes[child].parent {
            if self.nodes[parent].children[side ^ 1] == Some(child) {
                return Some(parent);
            }
            child = parent;
        }
        None
    }

    fn insert(&mut self, edge: usize) -> bool {
        let mut cursor = self.root;
        let mut parent = None;
        let mut side = 0;
        while let Some(node) = cursor {
            side = usize::from(self.order(edge, node).is_gt());
            parent = cursor;
            cursor = self.nodes[node].children[side];
        }
        self.nodes[edge] = ActiveNode {
            parent,
            children: [None; 2],
            height: 1,
        };
        if let Some(parent) = parent {
            self.nodes[parent].children[side] = Some(edge);
        } else {
            self.root = Some(edge);
        }
        self.rebalance(parent);
        [0, 1].into_iter().any(|side| {
            self.neighbour(edge, side)
                .is_some_and(|other| self.contact(edge, other))
        })
    }

    fn remove(&mut self, edge: usize) -> bool {
        let before = self.neighbour(edge, 0);
        let after = self.neighbour(edge, 1);
        if before.is_some_and(|other| self.contact(edge, other))
            || after.is_some_and(|other| self.contact(edge, other))
        {
            return true;
        }
        let [left, right] = self.nodes[edge].children;
        let start = if let (Some(left), Some(right)) = (left, right) {
            let successor = after.expect("two-child node has a successor");
            let parent = self.nodes[successor].parent.expect("successor parent");
            let start = if parent == edge {
                Some(successor)
            } else {
                let child = self.nodes[successor].children[1];
                self.replace(successor, child);
                self.nodes[successor].children[1] = Some(right);
                self.nodes[right].parent = Some(successor);
                Some(parent)
            };
            self.replace(edge, Some(successor));
            self.nodes[successor].children[0] = Some(left);
            self.nodes[left].parent = Some(successor);
            start
        } else {
            let parent = self.nodes[edge].parent;
            self.replace(edge, left.or(right));
            parent
        };
        self.nodes[edge] = ActiveNode::default();
        self.rebalance(start);
        before.zip(after).is_some_and(|(a, b)| self.contact(a, b))
    }
}
