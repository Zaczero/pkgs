use std::collections::BTreeSet;

use ahash::HashMapExt as _;

use super::super::exact::orient2d_normal_components;
use super::*;

#[expect(
    clippy::indexing_slicing,
    reason = "NodeId is enumerated from the same fixed EarArena node allocation"
)]
const fn arena_node(nodes: &[EarNode], id: NodeId) -> &EarNode {
    &nodes[id.index()]
}

#[expect(
    clippy::indexing_slicing,
    reason = "NodeId is enumerated from the same fixed EarArena node allocation"
)]
const fn arena_node_mut(nodes: &mut [EarNode], id: NodeId) -> &mut EarNode {
    &mut nodes[id.index()]
}

#[derive(Clone, Copy)]
struct EarNode {
    point: Point,
    identity: EarcutCornerId,
    prev: NodeId,
    next: NodeId,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct NodeId(usize);

impl NodeId {
    const fn from_index(index: usize) -> Self {
        Self(index)
    }

    const fn index(self) -> usize {
        self.0
    }
}

struct EarArena {
    nodes: Vec<EarNode>,
    active: usize,
    candidates: Option<EarCandidateTree>,
}

impl EarArena {
    #[cfg(test)]
    fn new(points: &[Point]) -> Self {
        let identities = points
            .iter()
            .enumerate()
            .map(|(index, _)| EarcutCornerId::source(0, index, index))
            .collect::<Vec<_>>();
        Self::new_with_ids(points, &identities)
    }

    fn new_with_ids(points: &[Point], identities: &[EarcutCornerId]) -> Self {
        debug_assert_eq!(points.len(), identities.len());
        let nodes: Vec<EarNode> = points
            .iter()
            .copied()
            .enumerate()
            .map(|(index, point)| EarNode {
                point,
                identity: *identities
                    .get(index)
                    .expect("source identities stay parallel with Earcut points"),
                prev: NodeId::from_index(wrap_index(
                    index.wrapping_add(points.len()).wrapping_sub(1),
                    points.len(),
                )),
                next: NodeId::from_index(wrap_index(index.wrapping_add(1), points.len())),
            })
            .collect();
        let candidates = (points.len() > 64).then(|| EarCandidateTree::new(points));
        Self {
            nodes,
            active: points.len(),
            candidates,
        }
    }

    fn node(&self, id: NodeId) -> &EarNode {
        arena_node(&self.nodes, id)
    }

    fn node_mut(&mut self, id: NodeId) -> &mut EarNode {
        arena_node_mut(&mut self.nodes, id)
    }

    fn remove(&mut self, id: NodeId) {
        let (prev, next) = {
            let node = self.node(id);
            (node.prev, node.next)
        };
        self.node_mut(prev).next = next;
        self.node_mut(next).prev = prev;
        let (nodes, candidates) = (&self.nodes, &mut self.candidates);
        if let Some(candidates) = candidates {
            candidates.convex.remove(&id);
            for neighbor in [prev, next] {
                let node = arena_node(nodes, neighbor);
                let reflex = orientation(
                    arena_node(nodes, node.prev).point,
                    node.point,
                    arena_node(nodes, node.next).point,
                ) != Orientation::CounterClockwise;
                candidates.set_reflex(neighbor, reflex);
            }
        }
        self.active = self.active.wrapping_sub(1);
    }
}

enum EarCandidateBranch {
    Vertex(NodeId),
    Children([usize; 2]),
}

struct EarCandidateNode {
    bounds: Bounds,
    parent: Option<usize>,
    live: usize,
    branch: EarCandidateBranch,
}

struct EarCandidateTree {
    // An ear obstruction has a reflex or collinear witness. Subtree counts
    // and boxes track those corners; cuts update the two surviving neighbors.
    nodes: Vec<EarCandidateNode>,
    // Bridge copies at one XY share a geometric witness but retain distinct
    // corner orientations in the ring.
    leaves: Vec<usize>,
    reflex: Vec<bool>,
    // Removal preserves original ring ordinal order, including wraparound.
    convex: BTreeSet<NodeId>,
    normal_arithmetic: bool,
}

#[expect(
    clippy::indexing_slicing,
    reason = "candidate node indices and leaf addresses are created together by the balanced tree builder"
)]
impl EarCandidateTree {
    fn new(points: &[Point]) -> Self {
        const MIN_COORDINATE: f64 = f64::from_bits((1023 - 200) << 52);
        const MAX_COORDINATE: f64 = f64::from_bits((1023 + 200) << 52);

        fn build(
            tree: &mut EarCandidateTree,
            points: &[Point],
            live: &[usize],
            order: &mut [NodeId],
            parent: Option<usize>,
        ) -> usize {
            let bounds = Bounds::from_xy_iter(order.iter().map(|id| points[id.index()].xy()));
            let index = tree.nodes.len();
            tree.nodes.push(EarCandidateNode {
                bounds,
                parent,
                live: order.len(),
                branch: EarCandidateBranch::Vertex(order[0]),
            });
            if order.len() == 1 {
                let vertex = order[0].index();
                tree.leaves[vertex] = index;
                tree.nodes[index].live = live[vertex];
            } else {
                let split = order.len() / 2;
                let x_axis = bounds.maxx() - bounds.minx() >= bounds.maxy() - bounds.miny();
                order.select_nth_unstable_by(split, |left, right| {
                    let l = points[left.index()];
                    let r = points[right.index()];
                    (if x_axis {
                        l.x.total_cmp(&r.x)
                    } else {
                        l.y.total_cmp(&r.y)
                    })
                    .then_with(|| left.index().cmp(&right.index()))
                });
                let (left, right) = order.split_at_mut(split);
                let left = build(tree, points, live, left, Some(index));
                let right = build(tree, points, live, right, Some(index));
                tree.nodes[index].branch = EarCandidateBranch::Children([left, right]);
                tree.refresh(index);
            }
            index
        }
        let mut representatives = HashMap::with_capacity(points.len());
        let mut order = Vec::with_capacity(points.len());
        let mut aliases = Vec::with_capacity(points.len());
        let mut live = vec![0_usize; points.len()];
        let mut reflex = Vec::with_capacity(points.len());
        // Such stored ordinates lie on the 2^-252 lattice, so any nonzero
        // rounded difference is at least 2^-252 and at most 2^201. Box
        // extrema select stored ordinates and inherit the same certificate.
        let normal_arithmetic = points.iter().all(|point| {
            [point.x, point.y].iter().all(|value| {
                *value == 0.0 || (MIN_COORDINATE..=MAX_COORDINATE).contains(&value.abs())
            })
        });
        for (index, &point) in points.iter().enumerate() {
            let vertex = NodeId::from_index(index);
            let representative =
                *representatives
                    .entry(PointKey::new(point))
                    .or_insert_with(|| {
                        order.push(vertex);
                        vertex
                    });
            aliases.push(representative);
            let previous = wrap_index(
                index.wrapping_add(points.len()).wrapping_sub(1),
                points.len(),
            );
            let next = wrap_index(index.wrapping_add(1), points.len());
            let is_reflex =
                orientation(points[previous], point, points[next]) != Orientation::CounterClockwise;
            reflex.push(is_reflex);
            live[representative.index()] =
                live[representative.index()].wrapping_add(usize::from(is_reflex));
        }
        let convex = reflex
            .iter()
            .enumerate()
            .filter_map(|(index, &reflex)| (!reflex).then_some(NodeId::from_index(index)))
            .collect();
        let mut tree = Self {
            nodes: Vec::with_capacity(order.len().wrapping_mul(2).wrapping_sub(1)),
            leaves: vec![0; points.len()],
            reflex,
            convex,
            normal_arithmetic,
        };
        build(&mut tree, points, &live, &mut order, None);
        tree.leaves = aliases
            .iter()
            .map(|vertex| tree.leaves[vertex.index()])
            .collect();
        tree
    }

    fn is_reflex(&self, vertex: NodeId) -> bool {
        self.reflex[vertex.index()]
    }

    fn next_convex(&self, vertex: NodeId) -> Option<NodeId> {
        self.convex
            .range(vertex..)
            .next()
            .or_else(|| self.convex.first())
            .copied()
    }

    fn refresh(&mut self, index: usize) {
        let EarCandidateBranch::Children([left, right]) = self.nodes[index].branch else {
            unreachable!("only internal candidate nodes are refreshed");
        };
        let left = &self.nodes[left];
        let right = &self.nodes[right];
        let live = left.live.wrapping_add(right.live);
        let bounds = if left.live == 0 {
            right.bounds
        } else if right.live == 0 {
            left.bounds
        } else {
            let mut bounds = left.bounds;
            bounds.include_bounds(right.bounds);
            bounds
        };
        self.nodes[index].live = live;
        self.nodes[index].bounds = bounds;
    }

    fn set_reflex(&mut self, vertex: NodeId, reflex: bool) {
        if self.is_reflex(vertex) == reflex {
            return;
        }
        let leaf = self.leaves[vertex.index()];
        self.reflex[vertex.index()] = reflex;
        if reflex {
            self.convex.remove(&vertex);
        } else {
            self.convex.insert(vertex);
        }
        self.nodes[leaf].live = if reflex {
            self.nodes[leaf].live.wrapping_add(1)
        } else {
            self.nodes[leaf].live.wrapping_sub(1)
        };
        let mut cursor = self.nodes[leaf].parent;
        while let Some(index) = cursor {
            self.refresh(index);
            cursor = self.nodes[index].parent;
        }
    }

    fn any_in(
        &self,
        bounds: Bounds,
        triangle: [XY; 3],
        mut predicate: impl FnMut(NodeId) -> bool,
    ) -> bool {
        fn visit(
            tree: &EarCandidateTree,
            index: usize,
            bounds: Bounds,
            triangle: &[XY; 3],
            predicate: &mut impl FnMut(NodeId) -> bool,
        ) -> bool {
            #[cfg(test)]
            EAR_CANDIDATE_VISITS.with(|count| count.set(count.get().wrapping_add(1)));
            let node = &tree.nodes[index];
            if node.live == 0 || !node.bounds.intersects(bounds) {
                return false;
            }
            match node.branch {
                EarCandidateBranch::Vertex(vertex) => predicate(vertex),
                EarCandidateBranch::Children([left, right]) => {
                    if tree.normal_arithmetic {
                        for (a, b) in triangle.iter().zip(triangle.iter().cycle().skip(1)) {
                            // Maximize the edge determinant over this box.
                            // A certified negative maximum excludes it; zero
                            // and uncertain signs preserve boundary witnesses.
                            let corner = XY::new(
                                if b.y < a.y {
                                    node.bounds.maxx()
                                } else {
                                    node.bounds.minx()
                                },
                                if b.x > a.x {
                                    node.bounds.maxy()
                                } else {
                                    node.bounds.miny()
                                },
                            );
                            if orient2d_normal_components(
                                a.x - corner.x,
                                a.y - corner.y,
                                b.x - corner.x,
                                b.y - corner.y,
                            ) == Some(ExactSign::Negative)
                            {
                                return false;
                            }
                        }
                    }
                    visit(tree, left, bounds, triangle, predicate)
                        || visit(tree, right, bounds, triangle, predicate)
                },
            }
        }
        visit(self, 0, bounds, &triangle, &mut predicate)
    }
}

pub(crate) fn earcut_linked_ring_with_ids(
    points: &[Point],
    identities: &[EarcutCornerId],
    emit: &mut impl FnMut(EarcutTriangle) -> Result<()>,
) -> Result<()> {
    if points.len() < 3 {
        return Ok(());
    }
    if points.len() != identities.len() {
        return Err(GeometryErrorKind::Invalid(
            "earcut site identities do not match the ring".into(),
        )
        .into());
    }
    let mut arena = EarArena::new_with_ids(points, identities);
    let mut current = NodeId::from_index(0);
    let mut guard = 0_usize;
    let mut selected_face = 0_usize;
    while arena.active > 3 {
        if let Some(candidates) = &arena.candidates
            && candidates.is_reflex(current)
        {
            current = candidates.next_convex(current).unwrap_or(current);
        }
        if is_ear(&arena, current) {
            let (prev, next) = {
                let node = arena.node(current);
                (node.prev, node.next)
            };
            let nodes = [arena.node(prev), arena.node(current), arena.node(next)];
            emit(EarcutTriangle {
                face: selected_face,
                points: nodes.map(|node| node.point),
                corners: nodes.map(|node| node.identity),
            })?;
            selected_face = selected_face.wrapping_add(1);
            arena.remove(current);
            current = arena.node(next).next;
            guard = 0;
            continue;
        }
        current = arena.node(current).next;
        guard = guard.wrapping_add(1);
        if guard > arena.nodes.len() {
            return Err(GeometryErrorKind::triangulation(
                "earcut failed to find a valid ear",
            ));
        }
    }
    if arena.active == 3 {
        let first = current;
        let second = arena.node(first).next;
        let third = arena.node(second).next;
        let nodes = [arena.node(first), arena.node(second), arena.node(third)];
        emit(EarcutTriangle {
            face: selected_face,
            points: nodes.map(|node| node.point),
            corners: nodes.map(|node| node.identity),
        })?;
    }
    Ok(())
}

fn is_ear(arena: &EarArena, index: NodeId) -> bool {
    #[cfg(test)]
    EAR_SCAN_STEPS.with(|count| count.set(count.get().wrapping_add(1)));
    let (prev, next) = {
        let node = arena.node(index);
        (node.prev, node.next)
    };
    let (a, b, c) = (
        arena.node(prev).point,
        arena.node(index).point,
        arena.node(next).point,
    );
    let reflex = arena.candidates.as_ref().map_or_else(
        || orientation(a, b, c) != Orientation::CounterClockwise,
        |tree| tree.is_reflex(index),
    );
    if reflex {
        return false;
    }
    !ear_contains_vertex(arena, prev, index, next, a, b, c)
}

fn ear_contains_vertex(
    arena: &EarArena,
    prev: NodeId,
    current: NodeId,
    next: NodeId,
    a: Point,
    b: Point,
    c: Point,
) -> bool {
    let minx = a.x.min(b.x).min(c.x);
    let miny = a.y.min(b.y).min(c.y);
    let maxx = a.x.max(b.x).max(c.x);
    let maxy = a.y.max(b.y).max(c.y);
    if let Some(candidates) = &arena.candidates {
        let bounds = Bounds::new_raw(minx, miny, maxx, maxy);
        return candidates.any_in(bounds, [a.xy(), b.xy(), c.xy()], |candidate| {
            candidate_inside_ear(
                candidate,
                prev,
                current,
                next,
                arena.node(candidate).point,
                a,
                b,
                c,
            )
        });
    }
    let mut candidate = arena.node(next).next;
    while candidate != prev {
        let point = arena.node(candidate).point;
        if point.x >= minx
            && point.x <= maxx
            && point.y >= miny
            && point.y <= maxy
            && candidate_inside_ear(candidate, prev, current, next, point, a, b, c)
        {
            return true;
        }
        candidate = arena.node(candidate).next;
    }
    false
}

fn candidate_inside_ear(
    candidate: NodeId,
    prev: NodeId,
    current: NodeId,
    next: NodeId,
    point: Point,
    a: Point,
    b: Point,
    c: Point,
) -> bool {
    #[cfg(test)]
    EAR_CANDIDATE_VISITS.with(|count| count.set(count.get().wrapping_add(1)));
    candidate != prev
        && candidate != current
        && candidate != next
        && !same_point(point, a)
        && !same_point(point, b)
        && !same_point(point, c)
        && point_in_ccw_triangle(point, a, b, c)
}

#[cfg(test)]
thread_local! {
    static EAR_CANDIDATE_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static EAR_SCAN_STEPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(in crate::geometry::tessellation) fn count_ear_candidates<T>(
    operation: impl FnOnce() -> T,
) -> (T, usize) {
    EAR_CANDIDATE_VISITS.with(|count| count.set(0));
    EAR_SCAN_STEPS.with(|count| count.set(0));
    let result = operation();
    (result, EAR_CANDIDATE_VISITS.with(std::cell::Cell::get))
}

#[cfg(test)]
pub(in crate::geometry::tessellation) fn ear_scan_steps() -> usize {
    EAR_SCAN_STEPS.with(std::cell::Cell::get)
}

#[cfg(test)]
#[path = "linked/tests.rs"]
mod tests;
