//! Farthest-site Voronoi point location for convex, cyclically ordered sites.
//!
//! Randomized exterior insertion builds the upper Delaunay triangulation.
//! A persistent AVL sweep over its exact Voronoi vertices provides logarithmic
//! point location. Queries are stored source vertices, so the source Y extent
//! bounds the sweep without rounding or clipping constructed vertices.

use std::cmp::Ordering;

use super::point;
use crate::error::Result;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::stored_cross_sign;
use crate::geometry::tessellation::exact::{self, ExactLine, ExactPoint};
use crate::geometry::tessellation::{EMPTY, next_halfedge, prev_halfedge};
use crate::geometry::{XY, try_sort_by};

struct Mesh {
    vertices: Vec<usize>,
    twins: Vec<usize>,
    boundary: Vec<usize>,
}

impl Mesh {
    fn build(sites: &[usize], xs: &[f64], ys: &[f64], mut seed: u64) -> Result<Self> {
        let count = sites.len();
        let mut order: Vec<_> = (0..count).collect();
        for end in (1..count).rev() {
            seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut random = seed;
            random = (random ^ (random >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            random = (random ^ (random >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            random ^= random >> 31;
            let population = end.wrapping_add(1);
            #[expect(
                clippy::arithmetic_side_effects,
                reason = "reverse Fisher-Yates has a nonzero population bounded by the allocated sites"
            )]
            let site = (random % population as u64) as usize;
            order.swap(end, site);
        }
        let mut previous: Vec<_> = (0..count)
            .map(|index| {
                if index == 0 {
                    count.wrapping_sub(1)
                } else {
                    index.wrapping_sub(1)
                }
            })
            .collect();
        let mut next: Vec<_> = (0..count)
            .map(|index| {
                if index.wrapping_add(1) == count {
                    0
                } else {
                    index.wrapping_add(1)
                }
            })
            .collect();
        let mut removed = Vec::with_capacity(count.wrapping_sub(3));
        for &site in order
            .get(..count.wrapping_sub(3))
            .expect("convex site population")
        {
            let left = *previous.get(site).expect("live predecessor");
            let right = *next.get(site).expect("live successor");
            removed.push((site, left, right));
            *next.get_mut(left).expect("live predecessor") = right;
            *previous.get_mut(right).expect("live successor") = left;
        }
        let first = *order.get(count.wrapping_sub(3)).expect("seed hull site");
        let second = *next.get(first).expect("seed hull successor");
        let third = *next.get(second).expect("seed hull successor");
        let capacity = count.wrapping_sub(2).wrapping_mul(3);
        let mut mesh = Self {
            vertices: Vec::with_capacity(capacity),
            twins: Vec::with_capacity(capacity),
            boundary: vec![EMPTY; count],
        };
        mesh.vertices.extend([first, second, third]);
        mesh.twins.extend([EMPTY; 3]);
        for (slot, site) in [first, second, third].into_iter().enumerate() {
            *mesh.boundary.get_mut(site).expect("seed hull boundary") = slot;
        }
        let xy = |local: usize| point(xs, ys, *sites.get(local).expect("convex site ordinal"));
        let mut pending = Vec::new();
        for &(site, left, right) in removed.iter().rev() {
            let old = *mesh
                .boundary
                .get(left)
                .expect("exterior insertion boundary");
            let base = mesh.vertices.len();
            mesh.vertices.extend([left, site, right]);
            mesh.twins.extend([EMPTY, EMPTY, old]);
            *mesh
                .twins
                .get_mut(old)
                .expect("exterior insertion boundary") = base.wrapping_add(2);
            *mesh.boundary.get_mut(left).expect("exterior predecessor") = base;
            *mesh.boundary.get_mut(site).expect("new exterior site") = base.wrapping_add(1);
            pending.push(old);
            while let Some(mut edge) = pending.pop() {
                let mut twin = *mesh.twins.get(edge).expect("legalization edge");
                if twin == EMPTY {
                    continue;
                }
                if mesh.vertex(prev_halfedge(twin)) != site {
                    std::mem::swap(&mut edge, &mut twin);
                }
                debug_assert_eq!(mesh.vertex(prev_halfedge(twin)), site);
                let a = xy(mesh.vertex(edge));
                let b = xy(mesh.vertex(next_halfedge(edge)));
                let c = xy(mesh.vertex(prev_halfedge(edge)));
                let d = xy(mesh.vertex(prev_halfedge(twin)));
                if exact::incircle(a, b, c, d)? != ExactSign::Negative {
                    continue;
                }
                pending.extend(mesh.flip(edge, twin));
            }
        }
        Ok(mesh)
    }

    fn vertex(&self, edge: usize) -> usize {
        *self.vertices.get(edge).expect("triangulation halfedge")
    }

    fn link(&mut self, edge: usize, twin: usize) {
        *self.twins.get_mut(edge).expect("triangulation halfedge") = twin;
        if twin == EMPTY {
            let site = self.vertex(edge);
            *self.boundary.get_mut(site).expect("hull site") = edge;
        } else {
            *self.twins.get_mut(twin).expect("triangulation twin") = edge;
        }
    }

    fn flip(&mut self, edge: usize, twin: usize) -> [usize; 2] {
        let (en, ep) = (next_halfedge(edge), prev_halfedge(edge));
        let (tn, tp) = (next_halfedge(twin), prev_halfedge(twin));
        let (u, v, w, p) = (
            self.vertex(edge),
            self.vertex(en),
            self.vertex(ep),
            self.vertex(tp),
        );
        let links = [
            *self.twins.get(en).expect("quad edge"),
            *self.twins.get(ep).expect("quad edge"),
            *self.twins.get(tn).expect("quad edge"),
            *self.twins.get(tp).expect("quad edge"),
        ];
        for (slot, site) in [(edge, w), (en, p), (ep, v), (twin, p), (tn, w), (tp, u)] {
            *self.vertices.get_mut(slot).expect("quad halfedge") = site;
        }
        self.link(edge, twin);
        let [vw, wu, up, pv] = links;
        self.link(en, pv);
        self.link(ep, vw);
        self.link(tn, wu);
        self.link(tp, up);
        [ep, tn]
    }
}

struct Boundary {
    sites: [usize; 2],
}

#[derive(Clone, Copy)]
struct Node {
    boundary: usize,
    children: [usize; 2],
    height: u8,
}

#[derive(Default)]
struct Status {
    nodes: Vec<Node>,
}

impl Status {
    fn height(&self, root: usize) -> u8 {
        self.nodes.get(root).map_or(0, |node| node.height)
    }

    fn node(&self, root: usize) -> Node {
        *self.nodes.get(root).expect("persistent search node")
    }

    fn publish(&mut self, boundary: usize, left: usize, right: usize) -> usize {
        let index = self.nodes.len();
        self.nodes.push(Node {
            boundary,
            children: [left, right],
            height: self.height(left).max(self.height(right)).wrapping_add(1),
        });
        index
    }

    fn balanced(&mut self, boundary: usize, left: usize, right: usize) -> usize {
        if self.height(left) > self.height(right).wrapping_add(1) {
            let pivot = self.node(left);
            let [ll, lr] = pivot.children;
            if self.height(ll) >= self.height(lr) {
                let right = self.publish(boundary, lr, right);
                return self.publish(pivot.boundary, ll, right);
            }
            let middle = self.node(lr);
            let [ml, mr] = middle.children;
            let left = self.publish(pivot.boundary, ll, ml);
            let right = self.publish(boundary, mr, right);
            return self.publish(middle.boundary, left, right);
        }
        if self.height(right) > self.height(left).wrapping_add(1) {
            let pivot = self.node(right);
            let [rl, rr] = pivot.children;
            if self.height(rr) >= self.height(rl) {
                let left = self.publish(boundary, left, rl);
                return self.publish(pivot.boundary, left, rr);
            }
            let middle = self.node(rl);
            let [ml, mr] = middle.children;
            let left = self.publish(boundary, left, ml);
            let right = self.publish(pivot.boundary, mr, rr);
            return self.publish(middle.boundary, left, right);
        }
        self.publish(boundary, left, right)
    }

    fn insert(
        &mut self,
        root: usize,
        boundary: usize,
        compare: &impl Fn(usize, usize) -> Result<Ordering>,
    ) -> Result<usize> {
        if root == EMPTY {
            return Ok(self.publish(boundary, EMPTY, EMPTY));
        }
        let node = self.node(root);
        let [mut left, mut right] = node.children;
        if compare(boundary, node.boundary)?.is_lt() {
            left = self.insert(left, boundary, compare)?;
        } else {
            right = self.insert(right, boundary, compare)?;
        }
        Ok(self.balanced(node.boundary, left, right))
    }

    fn remove(
        &mut self,
        root: usize,
        boundary: usize,
        compare: &impl Fn(usize, usize) -> Result<Ordering>,
    ) -> Result<usize> {
        let node = self.node(root);
        let [mut left, mut right] = node.children;
        match compare(boundary, node.boundary)? {
            Ordering::Less => left = self.remove(left, boundary, compare)?,
            Ordering::Greater => right = self.remove(right, boundary, compare)?,
            Ordering::Equal => {
                if left == EMPTY {
                    return Ok(right);
                }
                if right == EMPTY {
                    return Ok(left);
                }
                let (replacement, remainder) = self.remove_first(right);
                return Ok(self.balanced(replacement, left, remainder));
            },
        }
        Ok(self.balanced(node.boundary, left, right))
    }

    fn remove_first(&mut self, root: usize) -> (usize, usize) {
        let node = self.node(root);
        let [left, right] = node.children;
        if left == EMPTY {
            return (node.boundary, right);
        }
        let (first, left) = self.remove_first(left);
        (first, self.balanced(node.boundary, left, right))
    }
}

struct Event {
    centre: usize,
    boundary: usize,
    entering: bool,
}

pub(super) struct Diagram {
    boundaries: Vec<Boundary>,
    centres: Vec<ExactPoint>,
    levels: Vec<(usize, usize)>,
    initial: usize,
    status: Status,
}

impl Diagram {
    pub(super) fn build(
        sites: &[usize],
        xs: &[f64],
        ys: &[f64],
        y_extent: [f64; 2],
        seed: u64,
    ) -> Result<Self> {
        let mesh = Mesh::build(sites, xs, ys, seed)?;
        let mut centres = Vec::with_capacity(sites.len().wrapping_sub(2));
        let xy = |local: usize| point(xs, ys, *sites.get(local).expect("diagram site"));
        for &[a, b, c] in mesh.vertices.as_chunks::<3>().0 {
            centres.push(exact::circumcenter(xy(a), xy(b), xy(c))?);
        }
        let [minimum_y, maximum_y] = y_extent;
        let low = ExactPoint::from_xy(XY::new(0.0, minimum_y));
        let high = ExactPoint::from_xy(XY::new(0.0, maximum_y));
        let horizontal =
            ExactLine::through_points(&low, &ExactPoint::from_xy(XY::new(1.0, minimum_y)))?;
        let mut boundaries = Vec::with_capacity(mesh.vertices.len());
        let mut events = Vec::with_capacity(mesh.vertices.len().wrapping_mul(2));
        let mut active = Vec::new();
        for edge in 0..mesh.vertices.len() {
            let twin = *mesh.twins.get(edge).expect("diagram dual edge");
            if twin != EMPTY && twin < edge {
                continue;
            }
            let (u, v) = (mesh.vertex(edge), mesh.vertex(next_halfedge(edge)));
            let (a, b) = (xy(u), xy(v));
            #[expect(
                clippy::float_cmp,
                reason = "equal stored X ordinates identify an exact horizontal Voronoi boundary"
            )]
            let same_x = a.x == b.x;
            if same_x {
                continue; // Horizontal Voronoi boundaries are event-height ties.
            }
            let boundary = boundaries.len();
            let pair = if a.x < b.x { [u, v] } else { [v, u] };
            let first = edge / 3;
            let first_point = centres.get(first).expect("dual vertex");
            let (lower, upper) = if twin == EMPTY {
                if b.x > a.x {
                    (Some(first), None)
                } else {
                    (None, Some(first))
                }
            } else {
                let second = twin / 3;
                let second_point = centres.get(second).expect("dual vertex");
                match first_point.compare_y(second_point)? {
                    Ordering::Less => (Some(first), Some(second)),
                    Ordering::Greater => (Some(second), Some(first)),
                    Ordering::Equal => continue,
                }
            };
            boundaries.push(Boundary { sites: pair });
            let starts_below = match lower {
                None => true,
                Some(centre) => centres
                    .get(centre)
                    .expect("dual vertex")
                    .compare_y(&low)?
                    .is_lt(),
            };
            let ends_above = match upper {
                None => true,
                Some(centre) => !centres
                    .get(centre)
                    .expect("dual vertex")
                    .compare_y(&low)?
                    .is_lt(),
            };
            if starts_below && ends_above {
                let bisector = ExactLine::perpendicular_bisector(a, b)?;
                active.push((
                    boundary,
                    exact::incidence::line_intersection(&bisector, &horizontal)?,
                ));
            }
            for (centre, entering) in [(lower, true), (upper, false)] {
                if let Some(centre) = centre {
                    let vertex = centres.get(centre).expect("dual vertex");
                    if !vertex.compare_y(&low)?.is_lt() && !vertex.compare_y(&high)?.is_gt() {
                        events.push(Event {
                            centre,
                            boundary,
                            entering,
                        });
                    }
                }
            }
        }
        try_sort_by(&mut active, |left, right| {
            let x_order = left.1.compare_x(&right.1)?;
            if !x_order.is_eq() {
                return Ok(x_order);
            }
            let [lu, lv] = boundaries.get(left.0).expect("initial boundary").sites;
            let [ru, rv] = boundaries.get(right.0).expect("initial boundary").sites;
            let slope = match stored_cross_sign(xy(lu), xy(lv), xy(ru), xy(rv)) {
                ExactSign::Negative => Ordering::Less,
                ExactSign::Zero => Ordering::Equal,
                ExactSign::Positive => Ordering::Greater,
            };
            Ok(slope.reverse().then(left.0.cmp(&right.0)))
        })?;
        try_sort_by(&mut events, |left, right| {
            let left_point = centres.get(left.centre).expect("event vertex");
            let right_point = centres.get(right.centre).expect("event vertex");
            Ok(left_point
                .compare_y(right_point)?
                .then(left_point.compare_x(right_point)?))
        })?;
        let mut status = Status::default();
        let mut root = EMPTY;
        for (boundary, _) in active {
            // Initial boundaries arrive in their complete X order.
            root = status.insert(root, boundary, &|_, _| Ok(Ordering::Greater))?;
        }
        let initial = root;
        let mut levels = Vec::with_capacity(events.len());
        let mut cursor = 0;
        while cursor < events.len() {
            let first = events.get(cursor).expect("event cursor");
            let centre = first.centre;
            let vertex = centres.get(centre).expect("event vertex");
            let mut end = cursor.wrapping_add(1);
            while let Some(event) = events.get(end) {
                if !vertex.same_position(centres.get(event.centre).expect("event vertex"))? {
                    break;
                }
                end = end.wrapping_add(1);
            }
            let compare = |left: usize, right: usize, after: bool| -> Result<Ordering> {
                if left == right {
                    return Ok(Ordering::Equal);
                }
                let [lu, lv] = boundaries.get(left).expect("sweep boundary").sites;
                let [ru, rv] = boundaries.get(right).expect("sweep boundary").sites;
                let left_side =
                    exact::incidence::squared_distance_cmp_point(vertex, xy(lu), xy(lv))?;
                let right_side =
                    exact::incidence::squared_distance_cmp_point(vertex, xy(ru), xy(rv))?;
                let x_order = if left_side.is_eq() {
                    right_side
                } else if right_side.is_eq() {
                    left_side.reverse()
                } else {
                    unreachable!("one compared boundary ends at the sweep vertex")
                };
                if !x_order.is_eq() {
                    return Ok(x_order);
                }
                let slope = match stored_cross_sign(xy(lu), xy(lv), xy(ru), xy(rv)) {
                    ExactSign::Negative => Ordering::Less,
                    ExactSign::Zero => Ordering::Equal,
                    ExactSign::Positive => Ordering::Greater,
                };
                Ok((if after { slope } else { slope.reverse() }).then(left.cmp(&right)))
            };
            for event in events.get(cursor..end).expect("event group") {
                if !event.entering {
                    root = status.remove(root, event.boundary, &|left, right| {
                        compare(left, right, false)
                    })?;
                }
            }
            for event in events.get(cursor..end).expect("event group") {
                if event.entering {
                    root = status.insert(root, event.boundary, &|left, right| {
                        compare(left, right, true)
                    })?;
                }
            }
            levels.push((centre, root));
            cursor = end;
        }
        Ok(Self {
            boundaries,
            centres,
            levels,
            initial,
            status,
        })
    }

    pub(super) fn query(
        &self,
        query: XY,
        sites: &[usize],
        xs: &[f64],
        ys: &[f64],
    ) -> Result<usize> {
        let point = ExactPoint::from_xy(query);
        let (mut low, mut high) = (0_usize, self.levels.len());
        while low < high {
            let middle = low.wrapping_add(high.wrapping_sub(low) / 2);
            let (centre, _) = *self.levels.get(middle).expect("point-location level");
            if self
                .centres
                .get(centre)
                .expect("level vertex")
                .compare_y(&point)?
                .is_gt()
            {
                high = middle;
            } else {
                low = middle.wrapping_add(1);
            }
        }
        let mut root = if low == 0 {
            self.initial
        } else {
            self.levels
                .get(low.wrapping_sub(1))
                .expect("point-location level")
                .1
        };
        let mut winner = None;
        while root != EMPTY {
            let node = self.status.node(root);
            let [left, right] = self
                .boundaries
                .get(node.boundary)
                .expect("point-location boundary")
                .sites;
            let left_source = *sites.get(left).expect("diagram site");
            let right_source = *sites.get(right).expect("diagram site");
            let order =
                crate::geometry::point_pair_distance_key(query, super::point(xs, ys, left_source))
                    .cmp_distance(&crate::geometry::point_pair_distance_key(
                        query,
                        super::point(xs, ys, right_source),
                    ))?;
            let [left_child, right_child] = node.children;
            if order.is_lt() {
                winner = Some(right_source);
                root = left_child;
            } else {
                winner = Some(left_source);
                root = right_child;
            }
        }
        Ok(winner.expect("a noncollinear farthest diagram has an active boundary"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::point_pair_distance_key;

    #[test]
    fn persistent_farthest_cells_match_exact_distance_over_convex_sites() {
        let sites = [
            (4.0, 0.0),
            (3.0, 3.0),
            (0.0, 4.0),
            (-3.0, 3.0),
            (-4.0, 0.0),
            (-3.0, -3.0),
            (0.0, -4.0),
            (3.0, -3.0),
        ];
        let queries = [
            (-10.0, -10.0),
            (0.0, 0.0),
            (0.0, 1.0),
            (1.0, 0.0),
            (10.0, 10.0),
            (4.0, 0.0),
            (0.0, -4.0),
        ];
        let xs: Vec<_> = sites.iter().chain(&queries).map(|&(x, _)| x).collect();
        let ys: Vec<_> = sites.iter().chain(&queries).map(|&(_, y)| y).collect();
        for count in 3..=sites.len() {
            let ordinals: Vec<_> = (0..count).collect();
            for seed in [0, 1, 2, 3, u64::MAX] {
                let diagram = Diagram::build(&ordinals, &xs, &ys, [-10.0, 10.0], seed).unwrap();
                for &(x, y) in &queries {
                    let query = XY::new(x, y);
                    let winner = diagram.query(query, &ordinals, &xs, &ys).unwrap();
                    let best = point_pair_distance_key(query, point(&xs, &ys, winner));
                    for &source in &ordinals {
                        assert!(
                            !point_pair_distance_key(query, point(&xs, &ys, source))
                                .cmp_distance(&best)
                                .unwrap()
                                .is_gt(),
                            "count={count} seed={seed} query={query:?} winner={winner} source={source}",
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn coincident_voronoi_vertices_and_initial_height_ties_preserve_maxima() {
        for sites in [vec![(0.0, 0.0), (2.0, 0.0), (1.0, 1.0)], vec![
            (-1.0, -1.0),
            (1.0, -1.0),
            (1.0, 1.0),
            (-1.0, 1.0),
        ]] {
            let queries = [(-2.0, 0.0), (0.0, 0.0), (2.0, 0.0), (0.0, 1.0)];
            let xs: Vec<_> = sites.iter().chain(&queries).map(|&(x, _)| x).collect();
            let ys: Vec<_> = sites.iter().chain(&queries).map(|&(_, y)| y).collect();
            let ordinals: Vec<_> = (0..sites.len()).collect();
            for seed in [0, 17, u64::MAX] {
                let diagram = Diagram::build(&ordinals, &xs, &ys, [0.0, 1.0], seed).unwrap();
                for &(x, y) in &queries {
                    let query = XY::new(x, y);
                    let winner = diagram.query(query, &ordinals, &xs, &ys).unwrap();
                    let best = point_pair_distance_key(query, point(&xs, &ys, winner));
                    for &source in &ordinals {
                        assert!(
                            !point_pair_distance_key(query, point(&xs, &ys, source))
                                .cmp_distance(&best)
                                .unwrap()
                                .is_gt()
                        );
                    }
                }
            }
        }
    }
}
