//! Constrained Delaunay triangulation and bounded Ruppert refinement on the
//! exact mesh.
//!
//! A constraint is forced into a Delaunay mesh by flipping the edges its
//! segment crosses until none remain, every crossing decided by exact
//! `orient2d`; once present it is never flipped again, and a cavity search
//! never crosses it, so later insertions keep it. Faces inside the polygon
//! are found by the same parity flood the old engine used: from the hull
//! inwards, crossing a constraint toggles inside and outside. Refinement is
//! Ruppert's: a retained face that misses the angle or area target has its
//! circumcenter inserted, unless that center encroaches a constraint's
//! diametral circle, in which case the constraint is split at its midpoint
//! instead. The vertex budget bounds the loop, and a mesh the budget could
//! not finish is rejected rather than returned as if it met the request.

#![allow(
    clippy::many_single_char_names,
    reason = "a, b, c, d name the vertices of a quad and e, f, t, u its slots, the geometry kernel convention"
)]

use std::collections::{HashSet, VecDeque};

use crate::geometry::exact_arith::ExactSign;
use crate::geometry::tessellation::exact;
use crate::geometry::{Point, XY};

use super::exact_delaunay::{EMPTY, Located, Mesh, next_halfedge, prev_halfedge};

/// The mesh with its constraint flags, one per halfedge slot.
pub(super) struct ConstrainedMesh {
    pub(super) mesh: Mesh,
    pub(super) constrained: Vec<bool>,
}

impl ConstrainedMesh {
    /// Delaunay-triangulate `points`, then force every `edges` pair in.
    pub(super) fn build(points: &[Point], edges: &[[usize; 2]]) -> Option<Self> {
        let mesh = Mesh::delaunay(points)?;
        let constrained = vec![false; mesh.halfedges.len()];
        let mut this = Self { mesh, constrained };
        let xy = |site: usize| points[site].xy();
        for &[u, v] in edges {
            this.insert_constraint(&xy, u, v);
        }
        Some(this)
    }

    /// Force the segment `u -> v` to be an edge, splitting it at any vertex it
    /// passes through exactly, and flipping every edge it crosses.
    fn insert_constraint(&mut self, xy: &impl Fn(usize) -> XY, u: usize, v: usize) {
        if u == v {
            return;
        }
        if let Some(slot) = self.mesh.edge_slot(u, v) {
            self.mark(slot);
            return;
        }
        // Walk the fan around `u` to find the triangle the segment leaves
        // through, or a vertex it passes through exactly.
        let (pu, pv) = (xy(u), xy(v));
        let mut crossing: Vec<usize> = Vec::new();
        let mut start = None;
        for slot in self.mesh.outgoing(u) {
            let t = slot / 3;
            let a = self.mesh.triangles[next_halfedge(slot)];
            let b = self.mesh.triangles[prev_halfedge(slot)];
            // Segment u->v exits triangle (u, a, b) through edge a->b when v
            // is left of u->a and right of u->b (strictly), or through a
            // vertex when collinear.
            let sa = exact::orient2d(pu, xy(a), pv);
            let sb = exact::orient2d(pu, xy(b), pv);
            if sa == ExactSign::Zero && between(pu, pv, xy(a)) {
                self.insert_constraint(xy, u, a);
                self.insert_constraint(xy, a, v);
                return;
            }
            if sb == ExactSign::Zero && between(pu, pv, xy(b)) {
                self.insert_constraint(xy, u, b);
                self.insert_constraint(xy, b, v);
                return;
            }
            if sa == ExactSign::Positive && sb == ExactSign::Negative {
                start = Some(next_halfedge(slot)); // the edge a -> b
                let _ = t;
                break;
            }
        }
        let Some(mut edge) = start else {
            unreachable!("a segment between two mesh vertices leaves the start fan somewhere");
        };
        // March across triangles: `edge` is the crossed edge of the current
        // triangle; enter its twin's triangle and find the next exit.
        loop {
            crossing.push(edge);
            let twin = self.mesh.halfedges[edge];
            debug_assert_ne!(twin, EMPTY, "a constraint inside the hull never reaches the hull");
            let apex = self.mesh.triangles[prev_halfedge(twin)];
            if apex == v {
                break;
            }
            let pa = xy(apex);
            let s = exact::orient2d(pu, pv, pa);
            if s == ExactSign::Zero && between(pu, pv, pa) {
                // Passes exactly through `apex`: constrain u->apex by flipping
                // what we have so far, then continue from apex.
                self.flip_until_clear(xy, u, apex, crossing);
                self.insert_constraint(xy, apex, v);
                return;
            }
            edge = if s == ExactSign::Positive {
                // apex is left of u->v: exit through edge (twin.next: apex-side)
                prev_halfedge(twin)
            } else {
                next_halfedge(twin)
            };
        }
        self.flip_until_clear(xy, u, v, crossing);
    }

    /// Flip the crossed edges until none cross `u -> v`, then mark the edge.
    fn flip_until_clear(&mut self, xy: &impl Fn(usize) -> XY, u: usize, v: usize, mut crossing: Vec<usize>) {
        let (pu, pv) = (xy(u), xy(v));
        let mut guard = 0_usize;
        while let Some(edge) = crossing.pop() {
            guard += 1;
            assert!(guard < 1_000_000, "constraint insertion did not converge");
            let twin = self.mesh.halfedges[edge];
            let (a, b) = (self.mesh.triangles[edge], self.mesh.triangles[next_halfedge(edge)]);
            let c = self.mesh.triangles[prev_halfedge(edge)];
            let d = self.mesh.triangles[prev_halfedge(twin)];
            // The quad a,b with apexes c,d is flippable only when convex.
            let convex = exact::orient2d(xy(c), xy(d), xy(a)) != exact::orient2d(xy(c), xy(d), xy(b))
                && exact::orient2d(xy(a), xy(b), xy(c)) != exact::orient2d(xy(a), xy(b), xy(d));
            if !convex {
                crossing.insert(0, edge);
                continue;
            }
            let new_edge = self.mesh.flip(edge);
            let (na, nb) = (self.mesh.triangles[new_edge], self.mesh.triangles[next_halfedge(new_edge)]);
            let still_crosses = {
                let s1 = exact::orient2d(pu, pv, xy(na));
                let s2 = exact::orient2d(pu, pv, xy(nb));
                s1 != ExactSign::Zero && s2 != ExactSign::Zero && s1 != s2
                    && exact::orient2d(xy(na), xy(nb), pu) != exact::orient2d(xy(na), xy(nb), pv)
            };
            if still_crosses {
                crossing.insert(0, new_edge);
            }
        }
        let slot = self.mesh.edge_slot(u, v).expect("the constraint is an edge once nothing crosses it");
        self.mark(slot);
    }

    fn mark(&mut self, slot: usize) {
        self.constrained[slot] = true;
        let twin = self.mesh.halfedges[slot];
        if twin != EMPTY {
            self.constrained[twin] = true;
        }
    }

    /// Faces inside the constraint polygon: the hull faces are outside, and
    /// crossing a constraint toggles, crossing anything else does not.
    pub(super) fn inside_faces(&self) -> Vec<bool> {
        let count = self.mesh.triangle_count();
        let mut label: Vec<Option<bool>> = vec![None; count];
        let mut queue = VecDeque::new();
        // Seed from the hull: beyond a hull edge is the outside, so a face is
        // outside unless that hull edge is itself a constraint, in which case
        // the polygon's boundary runs along the hull and the face is inside.
        for e in 0..self.mesh.halfedges.len() {
            if self.mesh.halfedges[e] == EMPTY && self.mesh.alive(e / 3) && label[e / 3].is_none() {
                label[e / 3] = Some(self.constrained[e]);
                queue.push_back(e / 3);
            }
        }
        while let Some(t) = queue.pop_front() {
            let inside = label[t].expect("queued faces are labelled");
            for i in 0..3 {
                let e = 3 * t + i;
                let twin = self.mesh.halfedges[e];
                if twin == EMPTY {
                    continue;
                }
                let n = twin / 3;
                if label[n].is_none() {
                    label[n] = Some(if self.constrained[e] { !inside } else { inside });
                    queue.push_back(n);
                }
            }
        }
        label.into_iter().map(|l| l.unwrap_or(false)).collect()
    }

    /// Ruppert refinement of the inside faces to `min_angle` degrees and/or
    /// `max_area`, inserting at most `limit` vertices. `Ok(false)` means the
    /// budget ran out before the mesh met the request.
    pub(super) fn refine(
        &mut self,
        points: &mut Vec<Point>,
        min_angle: Option<f64>,
        max_area: Option<f64>,
        limit: usize,
    ) -> bool {
        let mut inserted = 0_usize;
        loop {
            let inside = self.inside_faces();
            let xy = |site: usize, points: &[Point]| points[site].xy();
            let mut bad = None;
            for (t, &kept) in inside.iter().enumerate() {
                if !kept || !self.mesh.alive(t) {
                    continue;
                }
                let [a, b, c] = self.mesh.triangle(t);
                let (pa, pb, pc) = (xy(a, points), xy(b, points), xy(c, points));
                let violates = min_angle.is_some_and(|limit| triangle_min_angle_degrees(pa, pb, pc) < limit)
                    || max_area.is_some_and(|limit| triangle_area(pa, pb, pc) > limit);
                if violates {
                    bad = Some(t);
                    break;
                }
            }
            let Some(t) = bad else {
                return true;
            };
            if inserted >= limit {
                return false;
            }
            let [a, b, c] = self.mesh.triangle(t);
            let center = circumcenter(xy(a, points), xy(b, points), xy(c, points));
            // Ruppert: a constraint whose diametral circle holds the center is
            // split at its midpoint instead, and so is any constraint, or the
            // hull, that the walk toward the center would have to cross: a
            // center beyond a constraint is one that encroaches it. A split
            // is topological, so the midpoint's rounded position decides only
            // which face receives it.
            let encroached = (0..self.mesh.halfedges.len()).find(|&e| {
                self.constrained[e] && self.mesh.alive(e / 3) && {
                    let (s, t2) = (self.mesh.triangles[e], self.mesh.triangles[next_halfedge(e)]);
                    in_diametral_circle(xy(s, points), xy(t2, points), center)
                }
            });
            let midpoint = |e: usize, points: &[Point]| {
                let (s, t2) = (self.mesh.triangles[e], self.mesh.triangles[next_halfedge(e)]);
                let (ps, pt) = (xy(s, points), xy(t2, points));
                XY::new(f64::midpoint(ps.x, pt.x), f64::midpoint(ps.y, pt.y))
            };
            let xy_now = |s: usize| points[s].xy();
            let split = |e: usize| {
                let m = midpoint(e, points);
                self.locate_split(&xy_now, e, m).map(|located| (m, located))
            };
            let placed = if let Some(e) = encroached {
                split(e)
            } else {
                match self.mesh.locate_bounded(&xy_now, t, center, &self.constrained) {
                    Ok(located) => Some((center, located)),
                    Err(e) => split(e),
                }
            };
            let Some((candidate, located)) = placed else {
                return false;
            };
            if matches!(located, Located::OnVertex) {
                return false;
            }
            let site = points.len();
            // The cavity reuses slots, so remember constraints as vertex pairs
            // and re-mark from those once the mesh has settled; a split
            // constraint becomes its two halves.
            let mut pairs = self.constrained_pairs();
            if let Located::OnEdge(e) = located
                && self.constrained[e]
            {
                let (s, t2) = (self.mesh.triangles[e], self.mesh.triangles[next_halfedge(e)]);
                pairs.remove(&ordered(s, t2));
                pairs.insert(ordered(s, site));
                pairs.insert(ordered(site, t2));
            }
            points.push(Point::new_raw_xy(candidate.x, candidate.y));
            self.mesh.admit_vertex(site);
            let xy2 = |s: usize| points[s].xy();
            let skip = self.constrained.clone();
            self.mesh.insert_constrained(&xy2, site, located, &skip);
            self.remark(&pairs);
            inserted += 1;
        }
    }

    /// Where the midpoint `m` of edge `e` goes. A split is topological: `m`
    /// stands on `e` whenever its rounded position is on `e` or inside either
    /// face beside it, and only a midpoint that rounding carried onto another
    /// edge or vertex, or out of both faces, is refused.
    fn locate_split(&self, xy: &impl Fn(usize) -> XY, e: usize, m: XY) -> Option<Located> {
        let twin = self.mesh.halfedges[e];
        let faces = [Some(e / 3), (twin != EMPTY).then_some(twin / 3)];
        for face in faces.into_iter().flatten() {
            match self.mesh.classify(xy, face, m) {
                Err(crossed) if crossed == e || crossed == twin => {},
                Ok(Located::Inside(_)) => return Some(Located::OnEdge(e)),
                Ok(Located::OnEdge(slot)) => return (slot == e || slot == twin).then_some(Located::OnEdge(e)),
                Ok(Located::OnVertex) | Err(_) => return None,
            }
        }
        // Across `e` from both sides can only be rounding on a hull edge.
        Some(Located::OnEdge(e))
    }

    fn constrained_pairs(&self) -> HashSet<(usize, usize)> {
        (0..self.mesh.halfedges.len())
            .filter(|&e| self.constrained[e] && self.mesh.alive(e / 3))
            .map(|e| ordered(self.mesh.triangles[e], self.mesh.triangles[next_halfedge(e)]))
            .collect()
    }

    /// Re-derive the per-slot flags from the constrained vertex pairs after
    /// an insertion has renumbered the cavity's slots.
    fn remark(&mut self, pairs: &HashSet<(usize, usize)>) {
        self.constrained = vec![false; self.mesh.halfedges.len()];
        for e in 0..self.mesh.halfedges.len() {
            if !self.mesh.alive(e / 3) {
                continue;
            }
            if pairs.contains(&ordered(self.mesh.triangles[e], self.mesh.triangles[next_halfedge(e)])) {
                self.constrained[e] = true;
            }
        }
    }
}

const fn ordered(a: usize, b: usize) -> (usize, usize) {
    if a < b { (a, b) } else { (b, a) }
}

/// Whether `p`, collinear with `a -> b`, lies strictly between them.
fn between(a: XY, b: XY, p: XY) -> bool {
    let (lo_x, hi_x) = if a.x < b.x { (a.x, b.x) } else { (b.x, a.x) };
    let (lo_y, hi_y) = if a.y < b.y { (a.y, b.y) } else { (b.y, a.y) };
    lo_x <= p.x && p.x <= hi_x && lo_y <= p.y && p.y <= hi_y && p != a && p != b
}

pub(super) fn triangle_area(a: XY, b: XY, c: XY) -> f64 {
    ((b.x - a.x) * (c.y - a.y) - (c.x - a.x) * (b.y - a.y)).abs() / 2.0
}

pub(super) fn triangle_min_angle_degrees(a: XY, b: XY, c: XY) -> f64 {
    let side = |p: XY, q: XY| ((p.x - q.x).powi(2) + (p.y - q.y).powi(2)).sqrt();
    let (ab, bc, ca) = (side(a, b), side(b, c), side(c, a));
    let angle = |opposite: f64, s1: f64, s2: f64| {
        (((s1 * s1 + s2 * s2 - opposite * opposite) / (2.0 * s1 * s2)).clamp(-1.0, 1.0)).acos()
    };
    [angle(bc, ab, ca), angle(ca, ab, bc), angle(ab, bc, ca)]
        .into_iter()
        .fold(f64::INFINITY, f64::min)
        .to_degrees()
}

/// The exact circumcenter rounded to nearest, which is what keeps a face
/// with reciprocal extents from producing a center that has cancelled to
/// the wrong side of the mesh; where no binary64 point can hold it, the
/// longest edge's midpoint still shrinks the face.
fn circumcenter(a: XY, b: XY, c: XY) -> XY {
    exact::circumcenter(a, b, c)
        .and_then(|center| center.round_nearest_even())
        .unwrap_or_else(|_| {
            let length = |p: XY, q: XY| (p.x - q.x).hypot(p.y - q.y);
            let (p, q) = if length(a, b) >= length(b, c) && length(a, b) >= length(c, a) {
                (a, b)
            } else if length(b, c) >= length(c, a) {
                (b, c)
            } else {
                (c, a)
            };
            XY::new(f64::midpoint(p.x, q.x), f64::midpoint(p.y, q.y))
        })
}

fn in_diametral_circle(s: XY, t: XY, p: XY) -> bool {
    // Inside the circle with diameter s-t: the angle at p is obtuse.
    (s.x - p.x) * (t.x - p.x) + (s.y - p.y) * (t.y - p.y) < 0.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn points(xy: &[(f64, f64)]) -> Vec<Point> {
        xy.iter().map(|&(x, y)| Point::new_raw_xy(x, y)).collect()
    }

    #[test]
    fn a_constraint_becomes_an_edge_and_survives() {
        // Four corners of a square plus a point off the diagonal that makes
        // the Delaunay diagonal the "wrong" one for the constraint.
        let pts = points(&[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (2.0, 8.0)]);
        let mesh = ConstrainedMesh::build(&pts, &[[0, 2]]).unwrap();
        assert!(mesh.mesh.edge_slot(0, 2).is_some(), "the constraint 0-2 is an edge");
        let slot = mesh.mesh.edge_slot(0, 2).unwrap();
        assert!(mesh.constrained[slot]);
    }

    #[test]
    fn parity_flood_finds_the_inside_of_a_square_with_a_hole() {
        let outer = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)];
        let hole = [(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0)];
        let pts = points(&outer.iter().chain(hole.iter()).copied().collect::<Vec<_>>());
        let ring = |base: usize| (0..4).map(|i| [base + i, base + (i + 1) % 4]).collect::<Vec<_>>();
        let edges: Vec<[usize; 2]> = ring(0).into_iter().chain(ring(4)).collect();
        let mesh = ConstrainedMesh::build(&pts, &edges).unwrap();
        let inside = mesh.inside_faces();
        let xy = |s: usize| pts[s].xy();
        for (t, &kept) in inside.iter().enumerate() {
            if !mesh.mesh.alive(t) {
                continue;
            }
            let [a, b, c] = mesh.mesh.triangle(t);
            let cx = (xy(a).x + xy(b).x + xy(c).x) / 3.0;
            let cy = (xy(a).y + xy(b).y + xy(c).y) / 3.0;
            let in_hole = (4.0..6.0).contains(&cx) && (4.0..6.0).contains(&cy);
            assert_eq!(kept, !in_hole, "face {t} centroid ({cx}, {cy})");
        }
    }

    #[test]
    fn refinement_meets_an_area_target_within_budget_and_reports_when_it_cannot() {
        let mut pts = points(&[(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)]);
        let edges: Vec<[usize; 2]> = (0..4).map(|i| [i, (i + 1) % 4]).collect();
        let mut mesh = ConstrainedMesh::build(&pts, &edges).unwrap();
        assert!(mesh.refine(&mut pts, None, Some(25.0), 10_000), "a generous budget completes");
        let inside = mesh.inside_faces();
        for (t, &kept) in inside.iter().enumerate() {
            if kept && mesh.mesh.alive(t) {
                let [a, b, c] = mesh.mesh.triangle(t);
                assert!(triangle_area(pts[a].xy(), pts[b].xy(), pts[c].xy()) <= 25.0);
            }
        }
        let mut pts2 = points(&[(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)]);
        let mut mesh2 = ConstrainedMesh::build(&pts2, &edges).unwrap();
        assert!(!mesh2.refine(&mut pts2, None, Some(0.5), 3), "three vertices cannot reach 0.5 area");
    }
}

/// Inputs the public suite once broke this engine with: a clockwise ring
/// whose constraints lie on the hull, a sliver whose circumcenters leave the
/// hull, and reciprocal extents whose float circumcenter cancels.
#[cfg(test)]
mod regressions {
    use super::*;
    use crate::geometry::Point;

    fn pts(xy: &[(f64, f64)]) -> Vec<Point> {
        xy.iter().map(|&(x, y)| Point::new_raw_xy(x, y)).collect()
    }
    fn ring_edges(n: usize) -> Vec<[usize; 2]> {
        (0..n).map(|i| [i, (i + 1) % n]).collect()
    }
    fn inside_count(cdt: &ConstrainedMesh) -> usize {
        let inside = cdt.inside_faces();
        (0..cdt.mesh.triangle_count()).filter(|&t| cdt.mesh.alive(t) && inside[t]).count()
    }

    #[test]
    fn reciprocal_pentagon_swapped_axes_is_three_faces() {
        for exponent in [22_i32, 154, 162] {
            let (large, tiny) = (10_f64.powi(exponent), 10_f64.powi(-exponent));
            let base = [(-large, 0.0), (-0.5 * large, -tiny), (0.5 * large, -tiny), (large, 0.0), (0.0, tiny)];
            let swapped: Vec<(f64, f64)> = base.iter().map(|&(x, y)| (y, x)).collect();
            let points = pts(&swapped);
            let cdt = ConstrainedMesh::build(&points, &ring_edges(5)).expect("five corners");
            assert_eq!(inside_count(&cdt), 3, "exponent {exponent}");
        }
    }

    #[test]
    fn sliver_refines_to_the_angle_without_leaving_the_hull() {
        let mut points = pts(&[(0.0, 0.0), (20.0, 0.0), (20.0, 0.2), (0.1, 0.05)]);
        let mut cdt = ConstrainedMesh::build(&points, &ring_edges(4)).unwrap();
        let before = inside_count(&cdt);
        assert!(cdt.refine(&mut points, Some(25.0), None, 100_000), "refinement completes");
        assert!(inside_count(&cdt) > before);
    }

    #[test]
    fn reciprocal_rectangle_max_area_splits_into_four() {
        for exponent in [77_i32, 159, 200, 300] {
            for swap in [false, true] {
                let (large, tiny) = (10_f64.powi(exponent), 10_f64.powi(-exponent));
                let mut base = vec![(0.0, 0.0), (large, 0.0), (large, tiny), (0.0, tiny)];
                if swap {
                    base = base.iter().map(|&(x, y)| (y, x)).collect();
                }
                let mut points = pts(&base);
                let mut cdt = ConstrainedMesh::build(&points, &ring_edges(4)).unwrap();
                assert!(cdt.refine(&mut points, None, Some(0.4), 100_000), "exponent {exponent} swap {swap}");
                assert_eq!(inside_count(&cdt), 4, "exponent {exponent} swap {swap}");
            }
        }
    }
}
