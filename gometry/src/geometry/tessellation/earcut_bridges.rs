use std::cmp::Ordering;

use rstar::primitives::{GeomWithData, Rectangle};
use rstar::{AABB, RTreeNode, RTreeObject as _};

use super::earcut::point_in_triangle;
use crate::geometry::{BulkRTree, Orientation, Point, orientation, point_on_segment, same_point};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct BridgeId(usize);

struct BridgeNode {
    point: Point,
    prev: BridgeId,
    next: BridgeId,
    // Bridge selection filters collinear occurrences; tessellation retains
    // them to split both sides of a bridge instead of emitting T-junctions.
    boundary_next: BridgeId,
    active: bool,
    batch: usize,
    depth: usize,
    ancestors: Vec<BridgeId>,
}

type Entry = GeomWithData<Rectangle<[f64; 2]>, BridgeId>;

#[cfg(test)]
thread_local! {
    static BRIDGE_WORK: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn record_bridge_work() {
    BRIDGE_WORK.with(|count| count.set(count.get().wrapping_add(1)));
}

#[cfg(test)]
pub(super) fn count_bridge_work<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    BRIDGE_WORK.with(|count| count.set(0));
    let result = operation();
    (result, BRIDGE_WORK.with(std::cell::Cell::get))
}

pub(super) struct BridgeArena {
    nodes: Vec<BridgeNode>,
    edges: BulkRTree<Entry>,
    vertices: BulkRTree<Entry>,
}

#[expect(
    clippy::indexing_slicing,
    reason = "BridgeId and ancestor links are created by this arena and remain stable through splices"
)]
#[expect(
    clippy::float_cmp,
    reason = "bridge selection preserves the exact floating comparisons and tie rules of earcut"
)]
impl BridgeArena {
    pub(super) fn new(points: &[Point], capacity: usize) -> Self {
        let root = BridgeId(0);
        let mut nodes = Vec::with_capacity(capacity.wrapping_add(1));
        nodes.push(BridgeNode {
            point: points[0],
            prev: root,
            next: root,
            boundary_next: root,
            active: false,
            batch: 0,
            depth: 0,
            ancestors: Vec::new(),
        });
        for (index, &point) in points.iter().enumerate() {
            nodes.push(BridgeNode {
                point,
                prev: BridgeId(if index == 0 { points.len() } else { index }),
                next: BridgeId(if index.wrapping_add(1) == points.len() {
                    1
                } else {
                    index.wrapping_add(2)
                }),
                boundary_next: BridgeId(if index.wrapping_add(1) == points.len() {
                    1
                } else {
                    index.wrapping_add(2)
                }),
                active: true,
                batch: 0,
                depth: 1,
                ancestors: vec![root],
            });
        }
        let mut arena = Self {
            nodes,
            edges: BulkRTree::new_with_params(),
            vertices: BulkRTree::new_with_params(),
        };
        arena.edges = BulkRTree::bulk_load_with_params(
            (1..arena.nodes.len())
                .filter(|&index| arena.descends(BridgeId(index)))
                .map(|index| arena.edge_entry(BridgeId(index)))
                .collect(),
        );
        arena.vertices = BulkRTree::bulk_load_with_params(
            (1..arena.nodes.len())
                .map(|index| arena.vertex_entry(BridgeId(index)))
                .collect(),
        );
        arena
    }

    fn point(&self, id: BridgeId) -> Point {
        self.nodes[id.0].point
    }

    fn vertex_entry(&self, id: BridgeId) -> Entry {
        let point = self.point(id);
        GeomWithData::new(
            Rectangle::from_corners([point.x, point.y], [point.x, point.y]),
            id,
        )
    }

    fn edge_entry(&self, id: BridgeId) -> Entry {
        let a = self.point(id);
        let b = self.point(self.nodes[id.0].next);
        // With y between the endpoints, every operation in the stored ray
        // expression is monotone. Its two rounded endpoint evaluations bound
        // the actual floating hit, including cancellation outside the raw box.
        let endpoint = a.x + (b.y - a.y) * (b.x - a.x) / (b.y - a.y);
        let (minx, maxx) = if endpoint.is_finite() {
            (a.x.min(b.x).min(endpoint), a.x.max(b.x).max(endpoint))
        } else {
            (f64::NEG_INFINITY, f64::INFINITY)
        };
        GeomWithData::new(
            Rectangle::from_corners([minx, a.y.min(b.y)], [maxx, a.y.max(b.y)]),
            id,
        )
    }

    fn descends(&self, id: BridgeId) -> bool {
        self.point(id).y > self.point(self.nodes[id.0].next).y
    }

    fn insert_edge(&mut self, id: BridgeId) {
        if self.descends(id) {
            self.edges.insert(self.edge_entry(id));
        }
    }

    fn remove_edge(&mut self, id: BridgeId) {
        if self.descends(id) {
            self.edges.remove(&self.edge_entry(id));
        }
    }

    fn append(&mut self, point: Point, parent: BridgeId, batch: usize) -> BridgeId {
        let id = BridgeId(self.nodes.len());
        let depth = self.nodes[parent.0].depth.wrapping_add(1);
        let mut ancestors = Vec::with_capacity((depth.ilog2() as usize).wrapping_add(1));
        ancestors.push(parent);
        while (1_usize << ancestors.len()) <= depth {
            let previous = ancestors[ancestors.len().wrapping_sub(1)];
            let ancestor = self.nodes[previous.0]
                .ancestors
                .get(ancestors.len().wrapping_sub(1))
                .copied()
                .unwrap_or(BridgeId(0));
            ancestors.push(ancestor);
        }
        self.nodes.push(BridgeNode {
            point,
            prev: id,
            next: id,
            boundary_next: id,
            active: true,
            batch,
            depth,
            ancestors,
        });
        id
    }

    fn lift(&self, mut id: BridgeId, mut steps: usize) -> BridgeId {
        while steps != 0 {
            let level = steps.trailing_zeros() as usize;
            id = self.nodes[id.0].ancestors[level];
            steps &= steps.wrapping_sub(1);
        }
        id
    }

    // A splice inserts a new child sequence immediately after its bridge,
    // before earlier child sequences. Preorder of this immutable insertion
    // forest is the ring's linear order even when an ancestor is filtered out.
    fn compare(&self, mut left: BridgeId, mut right: BridgeId) -> Ordering {
        #[cfg(test)]
        record_bridge_work();
        if left == right {
            return Ordering::Equal;
        }
        let left_depth = self.nodes[left.0].depth;
        let right_depth = self.nodes[right.0].depth;
        if left_depth > right_depth {
            left = self.lift(left, left_depth.wrapping_sub(right_depth));
            if left == right {
                return Ordering::Greater;
            }
        } else if right_depth > left_depth {
            right = self.lift(right, right_depth.wrapping_sub(left_depth));
            if left == right {
                return Ordering::Less;
            }
        }
        let levels = self.nodes[left.0].ancestors.len();
        for level in (0..levels).rev() {
            let l = self.nodes[left.0]
                .ancestors
                .get(level)
                .copied()
                .unwrap_or(BridgeId(0));
            let r = self.nodes[right.0]
                .ancestors
                .get(level)
                .copied()
                .unwrap_or(BridgeId(0));
            if l != r {
                left = l;
                right = r;
            }
        }
        self.nodes[right.0]
            .batch
            .cmp(&self.nodes[left.0].batch)
            .then_with(|| left.0.cmp(&right.0))
    }

    fn ray_bridge(&self, hole: Point) -> Option<(BridgeId, f64)> {
        let mut stack: Vec<_> = self.edges.root().children().iter().collect();
        let mut best = None;
        let mut qx = f64::NEG_INFINITY;
        while let Some(node) = stack.pop() {
            #[cfg(test)]
            record_bridge_work();
            let envelope = node.envelope();
            let lower = envelope.lower();
            let upper = envelope.upper();
            if lower[1] > hole.y || upper[1] < hole.y || upper[0] < qx || lower[0] > hole.x {
                continue;
            }
            match node {
                RTreeNode::Parent(parent) => {
                    let start = stack.len();
                    stack.extend(parent.children());
                    stack[start..].sort_unstable_by(|left, right| {
                        left.envelope().upper()[0].total_cmp(&right.envelope().upper()[0])
                    });
                },
                RTreeNode::Leaf(entry) => {
                    let id = entry.data;
                    let a = self.point(id);
                    let next = self.nodes[id.0].next;
                    let b = self.point(next);
                    let x = a.x + (hole.y - a.y) * (b.x - a.x) / (b.y - a.y);
                    if x <= hole.x
                        && (x > qx
                            || (x == qx
                                && best.is_some_and(|(old, _)| self.compare(id, old).is_lt())))
                    {
                        qx = x;
                        best = Some((id, if a.x < b.x { id } else { next }));
                    }
                },
            }
        }
        best.map(|(_, bridge)| (bridge, qx))
    }

    fn locally_inside(&self, id: BridgeId, target: Point) -> bool {
        let node = &self.nodes[id.0];
        let prev = self.point(node.prev);
        let curr = node.point;
        let next = self.point(node.next);
        if orientation(prev, curr, next) == Orientation::CounterClockwise {
            orientation(curr, target, next) != Orientation::CounterClockwise
                && orientation(curr, prev, target) != Orientation::CounterClockwise
        } else {
            orientation(curr, target, prev) == Orientation::CounterClockwise
                || orientation(curr, next, target) == Orientation::CounterClockwise
        }
    }

    fn sector_ok(&self, m: BridgeId, p: BridgeId) -> bool {
        orientation(
            self.point(self.nodes[m.0].prev),
            self.point(m),
            self.point(self.nodes[p.0].prev),
        ) == Orientation::CounterClockwise
            && orientation(
                self.point(self.nodes[p.0].next),
                self.point(m),
                self.point(self.nodes[m.0].next),
            ) == Orientation::CounterClockwise
    }

    pub(super) fn find_bridge(&self, hole: Point) -> Option<BridgeId> {
        let (initial, qx) = self.ray_bridge(hole)?;
        if hole.x == qx {
            return Some(initial);
        }
        let initial_point = self.point(initial);
        let (mx, my) = (initial_point.x, initial_point.y);
        if mx == hole.x {
            return Some(initial);
        }
        let inside = |point: Point| {
            if hole.y < my {
                point_in_triangle(hole.x, hole.y, mx, my, qx, hole.y, point.x, point.y)
            } else {
                point_in_triangle(qx, hole.y, mx, my, hole.x, hole.y, point.x, point.y)
            }
        };
        let mut best = initial;
        let mut tangent = f64::INFINITY;
        if mx <= hole.x && inside(initial_point) && self.locally_inside(initial, hole) {
            let tan = (hole.y - my).abs() / (hole.x - mx);
            if tan < tangent {
                tangent = tan;
            }
        }
        // A replacing candidate has RN(abs(RN(hy-py)) / RN(hx-px)) <= tangent.
        // Widen the quotient threshold, product and subtraction so the box
        // also contains candidates admitted by underflow or rounded ties.
        let radius = (tangent.next_up() * (hole.x - mx)).next_up().next_up();
        let (miny, maxy) = if radius.is_finite() {
            ((hole.y - radius).next_down(), (hole.y + radius).next_up())
        } else {
            (f64::NEG_INFINITY, f64::INFINITY)
        };
        let envelope = AABB::from_corners([mx, miny], [hole.x.next_down(), maxy]);
        let mut candidates: Vec<_> = self
            .vertices
            .locate_in_envelope_intersecting(envelope)
            .map(|entry| entry.data)
            .filter(|&id| id != initial)
            .collect();
        candidates.sort_unstable_by(|&left, &right| {
            let left_wrap = self.compare(left, initial).is_lt();
            let right_wrap = self.compare(right, initial).is_lt();
            left_wrap
                .cmp(&right_wrap)
                .then_with(|| self.compare(left, right))
        });
        for id in candidates {
            #[cfg(test)]
            record_bridge_work();
            let point = self.point(id);
            if hole.x >= point.x && point.x >= mx && hole.x != point.x && inside(point) {
                let tan = (hole.y - point.y).abs() / (hole.x - point.x);
                if self.locally_inside(id, hole)
                    && (tan < tangent
                        || (tan == tangent
                            && (point.x > self.point(best).x
                                || (point.x == self.point(best).x && self.sector_ok(best, id)))))
                {
                    best = id;
                    tangent = tan;
                }
            }
        }
        Some(best)
    }

    fn remove(&mut self, id: BridgeId) {
        let prev = self.nodes[id.0].prev;
        let next = self.nodes[id.0].next;
        self.remove_edge(prev);
        self.remove_edge(id);
        self.vertices.remove(&self.vertex_entry(id));
        self.nodes[prev.0].next = next;
        self.nodes[next.0].prev = prev;
        self.nodes[id.0].active = false;
        self.insert_edge(prev);
    }

    pub(super) fn splice(&mut self, bridge: BridgeId, hole: &[Point], batch: usize) {
        let next = self.nodes[bridge.0].next;
        let boundary_next = self.nodes[bridge.0].boundary_next;
        let start = BridgeId(self.nodes.len());
        for &point in hole {
            self.append(point, bridge, batch);
        }
        self.append(hole[0], bridge, batch);
        let end = self.append(self.point(bridge), bridge, batch);
        self.remove_edge(bridge);
        let mut prev = bridge;
        for index in start.0..=end.0 {
            let id = BridgeId(index);
            self.nodes[prev.0].next = id;
            self.nodes[prev.0].boundary_next = id;
            self.nodes[id.0].prev = prev;
            prev = id;
        }
        self.nodes[end.0].next = next;
        self.nodes[end.0].boundary_next = boundary_next;
        self.nodes[next.0].prev = end;
        self.insert_edge(bridge);
        for index in start.0..=end.0 {
            let id = BridgeId(index);
            self.insert_edge(id);
            self.vertices.insert(self.vertex_entry(id));
        }
        let mut pending = vec![bridge, start, BridgeId(end.0.wrapping_sub(1)), end];
        while let Some(id) = pending.pop() {
            if !self.nodes[id.0].active {
                continue;
            }
            let prev = self.nodes[id.0].prev;
            let next = self.nodes[id.0].next;
            let a = self.point(prev);
            let b = self.point(id);
            let c = self.point(next);
            if (orientation(a, b, c) == Orientation::Collinear && point_on_segment(b, a, c))
                || same_point(a, b)
            {
                self.remove(id);
                pending.extend([prev, next]);
            }
        }
    }

    pub(super) fn into_points(self) -> Vec<Point> {
        let mut points = Vec::with_capacity(self.nodes.len().wrapping_sub(1));
        let mut id = BridgeId(1);
        loop {
            points.push(self.point(id));
            id = self.nodes[id.0].boundary_next;
            if id == BridgeId(1) {
                break;
            }
        }
        points
    }

    #[cfg(test)]
    pub(super) fn filtered_walk(&self) -> Vec<(BridgeId, Point)> {
        let mut ids: Vec<_> = self
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(index, node)| node.active.then_some(BridgeId(index)))
            .collect();
        ids.sort_unstable_by(|&left, &right| self.compare(left, right));
        ids.into_iter().map(|id| (id, self.point(id))).collect()
    }
}
