//! Exact Delaunay triangulation by incremental insertion.
//!
//! Every decision is an exact predicate: the hull is the crate's exact corner
//! hull, the seed fan over it is legalized by Lawson flips under exact
//! `incircle`, point location walks by exact `orient2d`, and a cavity is the
//! set of triangles whose circumcircle strictly contains the new site. Cavity
//! insertion keeps a Delaunay mesh Delaunay, so the mesh is Delaunay as built
//! and nothing has to be certified or repaired afterwards. Cocircular sites are left as whatever
//! diagonal the insertion order produced, which is Delaunay either way, and
//! every such edge is flagged from one final exact pass so the Voronoi dual can
//! collapse the fan it belongs to.
//!
//! Sites are inserted in Hilbert order, so each insertion lands near the last
//! and the walk from the previous triangle is short; the seed is the fan of the
//! exact hull over all sites, so every later insertion is interior and the
//! hull never changes. Output uses the halfedge layout the rest of the
//! tessellation code already reads: triangle `t` owns slots `3t..3t + 3`,
//! `halfedges[e]` is the twin slot or `EMPTY`, and `hull` is the counter-
//! clockwise hull cycle.

#![allow(
    clippy::many_single_char_names,
    reason = "a, b, c, d name the vertices of a quad and e, f, t, u its slots, the geometry kernel convention"
)]

use crate::error::Result;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::tessellation::exact;
use crate::geometry::{Point, XY};

pub(crate) const EMPTY: usize = usize::MAX;

pub(crate) const fn next_halfedge(edge: usize) -> usize {
    if edge % 3 == 2 { edge - 2 } else { edge + 1 }
}

pub(crate) const fn prev_halfedge(edge: usize) -> usize {
    if edge.is_multiple_of(3) {
        edge + 2
    } else {
        edge - 1
    }
}

/// The halfedge triangle mesh every tessellation consumer reads: triangle `t`
/// owns slots `3t..3t + 3`, `halfedges[e]` is the twin slot or [`EMPTY`], and
/// `hull` is the counter-clockwise hull cycle.
#[derive(Debug)]
pub(crate) struct Triangulation {
    pub(crate) triangles: Vec<usize>,
    pub(crate) halfedges: Vec<usize>,
    pub(crate) hull: Vec<usize>,
}

/// The finished mesh: the layout above, plus the cocircular flag per slot.
pub(super) struct ExactMesh {
    pub(super) triangles: Vec<usize>,
    pub(super) halfedges: Vec<usize>,
    pub(super) hull: Vec<usize>,
    pub(super) cocircular: Vec<bool>,
}

impl ExactMesh {
    pub(super) fn into_topology(self) -> Triangulation {
        Triangulation {
            triangles: self.triangles,
            halfedges: self.halfedges,
            hull: self.hull,
        }
    }
}

/// Triangulate `points` exactly. Fewer than three non-collinear sites give an
/// empty mesh whose hull is the collinear chain the crate already reports.
pub(super) fn exact_delaunay(points: &[Point]) -> ExactMesh {
    Mesh::delaunay(points).map_or_else(
        || ExactMesh {
            triangles: Vec::new(),
            halfedges: Vec::new(),
            hull: super::exact_hull_cycle(points),
            cocircular: Vec::new(),
        },
        |mesh| mesh.finish(|site| points[site].xy()),
    )
}

impl Mesh {
    /// The exact Delaunay mesh of `points`, live (uncompacted) so constraints
    /// and refinement can keep inserting into it; `None` when the sites are
    /// collinear and there is no triangle to build.
    pub(super) fn delaunay(points: &[Point]) -> Option<Self> {
        Self::try_delaunay(points)
            .unwrap_or_else(|_| unreachable!("unconstrained mesh allocation failed"))
    }

    /// Fallible exact Delaunay construction for the constrained path. The
    /// legacy infallible wrapper above remains the owner of the unconstrained
    /// public shape, while every allocation needed before constrained
    /// publication is returned to its caller.
    pub(super) fn try_delaunay(points: &[Point]) -> Result<Option<Self>> {
        let hull = super::try_exact_corner_hull_cycle(points)?;
        if hull.len() < 3 {
            return Ok(None);
        }
        let xy = |site: usize| points[site].xy();
        let mut mesh = Self::try_seed(&hull, points.len())?;
        mesh.try_legalize(&xy)?;

        // Interior sites: not on the hull, not an exact duplicate of a site
        // already placed, in Hilbert order over the point bounds.
        let mut placed = crate::collections::try_hash_set_with_capacity(points.len())?;
        for &site in &hull {
            placed.insert(key(xy(site)));
        }
        let mut hull_sites = crate::collections::try_hash_set_with_capacity(hull.len())?;
        for &site in &hull {
            hull_sites.insert(site);
        }
        let (min, max) = bounds(points);
        let mut interior = Vec::new();
        crate::try_reserve_vec(&mut interior, points.len())?;
        for site in 0..points.len() {
            if !hull_sites.contains(&site) && placed.insert(key(xy(site))) {
                crate::try_push_vec(&mut interior, (hilbert16(xy(site), min, max), site))?;
            }
        }
        interior.sort_unstable();

        let mut last = 0_usize;
        for (_, site) in interior {
            let located = mesh.locate(&xy, last, xy(site));
            last = mesh.try_insert_with(&xy, site, located, None)?;
        }
        Ok(Some(mesh))
    }
}

/// One coordinate pair as an exact identity key (bit patterns, with both
/// signed zeros as one point).
fn key(p: XY) -> [u64; 2] {
    let norm = |v: f64| {
        if v == 0.0 {
            0.0_f64.to_bits()
        } else {
            v.to_bits()
        }
    };
    [norm(p.x), norm(p.y)]
}

fn bounds(points: &[Point]) -> (XY, XY) {
    let mut min = XY::new(f64::INFINITY, f64::INFINITY);
    let mut max = XY::new(f64::NEG_INFINITY, f64::NEG_INFINITY);
    for point in points {
        let p = point.xy();
        min = XY::new(min.x.min(p.x), min.y.min(p.y));
        max = XY::new(max.x.max(p.x), max.y.max(p.y));
    }
    (min, max)
}

/// A 16-bit-per-axis Hilbert index over the bounds: a locality order for the
/// insertions, never a geometric decision.
fn hilbert16(p: XY, min: XY, max: XY) -> u32 {
    const N: u32 = 1 << 16;
    const SIDE: f64 = 65_535.0;
    let scale = |v: f64, lo: f64, hi: f64| -> u32 {
        if hi <= lo {
            0
        } else {
            (((v - lo) / (hi - lo)).clamp(0.0, 1.0) * SIDE) as u32
        }
    };
    // The canonical `xy2d`: at each level the quadrant contributes `s * s`
    // cells, and a lower-left or lower-right quadrant rotates the frame so the
    // curve stays continuous into the next level.
    let (mut x, mut y) = (scale(p.x, min.x, max.x), scale(p.y, min.y, max.y));
    let mut d = 0_u32;
    let mut s = N >> 1;
    while s > 0 {
        let rx = u32::from(x & s > 0);
        let ry = u32::from(y & s > 0);
        d += s * s * ((3 * rx) ^ ry);
        if ry == 0 {
            if rx == 1 {
                x = N - 1 - x;
                y = N - 1 - y;
            }
            std::mem::swap(&mut x, &mut y);
        }
        s >>= 1;
    }
    d
}

/// Where a site landed relative to the current mesh.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Located {
    /// Strictly inside this triangle.
    Inside(usize),
    /// On this halfedge (exactly collinear with it, strictly between its ends).
    OnEdge(usize),
    /// Exactly on an existing vertex: nothing to insert.
    OnVertex,
}

pub(super) struct Mesh {
    pub(super) triangles: Vec<usize>,
    pub(super) halfedges: Vec<usize>,
    pub(super) hull: Vec<usize>,
    /// One outgoing halfedge slot per vertex (`EMPTY` for a vertex not in
    /// the mesh), kept valid across insertions so a fan can be walked.
    incident: Vec<usize>,
    /// Slots of triangles removed by a cavity, reused before the mesh grows.
    free: Vec<usize>,
    /// Per triangle: a stamp for the cavity search, so the visited set costs
    /// no allocation per insertion.
    stamp: Vec<u32>,
    epoch: u32,
}

impl Mesh {
    fn try_seed(hull: &[usize], vertex_count: usize) -> Result<Self> {
        let count = hull.len() - 2;
        let slots = count
            .checked_mul(3)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let mut triangles = Vec::new();
        crate::try_reserve_vec(&mut triangles, slots)?;
        let mut halfedges = Vec::new();
        crate::try_reserve_vec(&mut halfedges, slots)?;
        halfedges.resize(slots, EMPTY);
        for (t, index) in (1..hull.len() - 1).enumerate() {
            triangles.extend([hull[0], hull[index], hull[index + 1]]);
            // Fan triangles share the spoke `hull[0]..hull[index + 1]`: slot 2
            // of triangle t (edge hull[index+1] -> hull[0]) twins slot 0 of
            // triangle t + 1 (edge hull[0] -> hull[index+1]).
            if t + 1 < count {
                halfedges[3 * t + 2] = 3 * (t + 1);
                halfedges[3 * (t + 1)] = 3 * t + 2;
            }
        }
        let mut incident = Vec::new();
        crate::try_reserve_vec(&mut incident, vertex_count)?;
        incident.resize(vertex_count, EMPTY);
        for (slot, &v) in triangles.iter().enumerate() {
            if incident[v] == EMPTY {
                incident[v] = slot;
            }
        }
        let mut hull_copy = Vec::new();
        crate::try_reserve_vec(&mut hull_copy, hull.len())?;
        hull_copy.extend_from_slice(hull);
        let mut stamp = Vec::new();
        crate::try_reserve_vec(&mut stamp, count)?;
        stamp.resize(count, 0);
        Ok(Self {
            triangles,
            halfedges,
            hull: hull_copy,
            incident,
            free: Vec::new(),
            stamp,
            epoch: 0,
        })
    }

    pub(super) const fn triangle_count(&self) -> usize {
        self.triangles.len() / 3
    }

    pub(super) fn alive(&self, t: usize) -> bool {
        self.triangles[3 * t] != EMPTY
    }

    /// Every halfedge slot leaving `u`, walking its fan both ways so a hull
    /// vertex's open fan is covered too.
    pub(super) fn outgoing(&self, u: usize) -> Result<Vec<usize>> {
        let start = self.incident[u];
        let mut slots = Vec::new();
        crate::try_reserve_vec(&mut slots, self.triangle_count())?;
        if start == EMPTY || !self.alive(start / 3) || self.triangles[start] != u {
            // The remembered slot died in a cavity: fall back to a scan.
            for e in 0..self.triangles.len() {
                if self.alive(e / 3) && self.triangles[e] == u {
                    crate::try_push_vec(&mut slots, e)?;
                }
            }
            return Ok(slots);
        }
        // Counter-clockwise: across the edge into `u`.
        let mut e = start;
        loop {
            crate::try_push_vec(&mut slots, e)?;
            let twin = self.halfedges[prev_halfedge(e)];
            if twin == EMPTY {
                break;
            }
            e = twin;
            if e == start {
                return Ok(slots);
            }
        }
        // Clockwise from the start: across the edge out of `u`.
        let mut e = start;
        loop {
            let twin = self.halfedges[e];
            if twin == EMPTY {
                break;
            }
            e = next_halfedge(twin);
            if e == start {
                break;
            }
            crate::try_push_vec(&mut slots, e)?;
        }
        Ok(slots)
    }

    /// The slot of the edge between `u` and `v` in either direction, if it
    /// is an edge; a hull edge exists as one halfedge only.
    pub(super) fn edge_slot(&self, u: usize, v: usize) -> Result<Option<usize>> {
        if let Some(slot) = self.directed_slot(u, v)? {
            return Ok(Some(slot));
        }
        self.directed_slot(v, u)
    }

    fn directed_slot(&self, u: usize, v: usize) -> Result<Option<usize>> {
        self.outgoing(u).map(|slots| {
            slots
                .into_iter()
                .find(|&e| self.triangles[next_halfedge(e)] == v)
        })
    }

    /// Lawson legalization of every interior edge: flip while the twin's apex
    /// lies strictly inside the circumcircle. The seed fan triangulates the
    /// hull without being Delaunay, and cavity insertion preserves the
    /// Delaunay property only from a Delaunay start. A slot left on the stack
    /// by a flip names whatever edge now lives there; re-checking a legal
    /// edge is harmless, and every edge a flip disturbed is pushed again.
    fn try_legalize(&mut self, xy: &impl Fn(usize) -> XY) -> Result<()> {
        let mut stack = Vec::new();
        crate::try_reserve_vec(&mut stack, self.halfedges.len())?;
        for e in 0..self.halfedges.len() {
            if self.halfedges[e] != EMPTY && self.halfedges[e] > e {
                crate::try_push_vec(&mut stack, e)?;
            }
        }
        while let Some(e) = stack.pop() {
            let f = self.halfedges[e];
            if f == EMPTY {
                continue;
            }
            let [a, b, c] = self.triangle(e / 3);
            let d = self.triangles[prev_halfedge(f)];
            if exact::try_incircle(xy(a), xy(b), xy(c), xy(d))? != ExactSign::Positive {
                continue;
            }
            let g = self.flip(e);
            let h = self.halfedges[g];
            for edge in [
                next_halfedge(g),
                prev_halfedge(g),
                next_halfedge(h),
                prev_halfedge(h),
            ] {
                crate::try_push_vec(&mut stack, edge)?;
            }
        }
        Ok(())
    }

    /// Flip the edge at slot `e` (`a -> b`, apex `c`; twin apex `d`) to
    /// `c -> d`, returning the slot of `c -> d`. The caller has checked the
    /// quad is convex.
    pub(super) fn flip(&mut self, e: usize) -> usize {
        let f = self.halfedges[e];
        debug_assert_ne!(f, EMPTY, "a hull edge cannot flip");
        let (t, u) = (e / 3, f / 3);
        let (a, b, c) = (
            self.triangles[e],
            self.triangles[next_halfedge(e)],
            self.triangles[prev_halfedge(e)],
        );
        let d = self.triangles[prev_halfedge(f)];
        // Outside neighbours that change owner: across a-d and across b-c.
        let across_ad = self.halfedges[next_halfedge(f)];
        let across_bc = self.halfedges[next_halfedge(e)];
        let keep_ca = self.halfedges[prev_halfedge(e)];
        let keep_db = self.halfedges[prev_halfedge(f)];
        // t becomes (a, d, c); u becomes (b, c, d).
        self.triangles[3 * t] = a;
        self.triangles[3 * t + 1] = d;
        self.triangles[3 * t + 2] = c;
        self.triangles[3 * u] = b;
        self.triangles[3 * u + 1] = c;
        self.triangles[3 * u + 2] = d;
        self.halfedges[3 * t] = across_ad;
        if across_ad != EMPTY {
            self.halfedges[across_ad] = 3 * t;
        }
        self.halfedges[3 * t + 1] = 3 * u + 1;
        self.halfedges[3 * u + 1] = 3 * t + 1;
        self.halfedges[3 * t + 2] = keep_ca;
        if keep_ca != EMPTY {
            self.halfedges[keep_ca] = 3 * t + 2;
        }
        self.halfedges[3 * u] = across_bc;
        if across_bc != EMPTY {
            self.halfedges[across_bc] = 3 * u;
        }
        self.halfedges[3 * u + 2] = keep_db;
        if keep_db != EMPTY {
            self.halfedges[keep_db] = 3 * u + 2;
        }
        self.incident[a] = 3 * t;
        self.incident[d] = 3 * t + 1;
        self.incident[c] = 3 * t + 2;
        self.incident[b] = 3 * u;
        3 * u + 1
    }

    /// Walk from triangle `start` toward `p` by exact orientation. Every
    /// site the Delaunay driver inserts is interior to the seed hull, so the
    /// walk never reaches a hull edge.
    pub(super) fn locate(&self, xy: &impl Fn(usize) -> XY, start: usize, p: XY) -> Located {
        self.locate_bounded(xy, start, p, &[])
            .unwrap_or_else(|_| unreachable!("an interior site never leaves the hull"))
    }

    /// The orientation walk that never crosses a hull edge or a slot `stop`
    /// marks: `Err(e)` is the edge it would have had to cross. A constrained
    /// mesh is not Delaunay, where the walk can cycle, so a walk longer than
    /// the mesh settles by scanning every live face.
    pub(super) fn locate_bounded(
        &self,
        xy: &impl Fn(usize) -> XY,
        start: usize,
        p: XY,
        stop: &[bool],
    ) -> Result<Located, usize> {
        let mut t = if self.alive(start) {
            start
        } else {
            self.any_alive()
        };
        let mut steps = 0_usize;
        loop {
            let e = match self.classify(xy, t, p) {
                Ok(located) => return Ok(located),
                Err(e) => e,
            };
            let twin = self.halfedges[e];
            if twin == EMPTY || stop.get(e).is_some_and(|&stopped| stopped) {
                return Err(e);
            }
            steps += 1;
            if steps > self.triangle_count() {
                return (0..self.triangle_count())
                    .filter(|&f| self.alive(f))
                    .find_map(|f| self.classify(xy, f, p).ok())
                    .ok_or(e);
            }
            t = twin / 3;
        }
    }

    /// Where `p` stands relative to live triangle `t`: `Err(e)` names the
    /// first edge `p` is strictly to the right of.
    pub(super) fn classify(
        &self,
        xy: &impl Fn(usize) -> XY,
        t: usize,
        p: XY,
    ) -> Result<Located, usize> {
        let mut collinear = [false; 3];
        for (i, on) in collinear.iter_mut().enumerate() {
            let e = 3 * t + i;
            match exact::orient2d(
                xy(self.triangles[e]),
                xy(self.triangles[next_halfedge(e)]),
                p,
            ) {
                ExactSign::Negative => return Err(e),
                ExactSign::Zero => *on = true,
                ExactSign::Positive => {},
            }
        }
        Ok(match collinear.iter().filter(|&&on| on).count() {
            0 => Located::Inside(t),
            1 => {
                let e = 3 * t + collinear.iter().position(|&on| on).unwrap_or(0);
                let (a, b) = (self.triangles[e], self.triangles[next_halfedge(e)]);
                if p == xy(a) || p == xy(b) {
                    Located::OnVertex
                } else {
                    Located::OnEdge(e)
                }
            },
            _ => Located::OnVertex,
        })
    }

    fn any_alive(&self) -> usize {
        (0..self.triangle_count())
            .find(|&t| self.alive(t))
            .expect("a seeded mesh has a triangle")
    }

    /// Insert `site` at `located`, returning a triangle incident to it for the
    /// next walk to start from. A cavity never crosses a slot `skip` marks:
    /// that is how a constraint, once in, stays in.
    pub(super) fn try_insert_with(
        &mut self,
        xy: &impl Fn(usize) -> XY,
        site: usize,
        located: Located,
        skip: Option<&[bool]>,
    ) -> Result<usize> {
        let p = xy(site);
        // A site on a hull edge splits that edge: the cavity boundary is then
        // an open chain from the edge's end to its start, the site joins the
        // hull between them, and the two chain ends become hull edges.
        let mut split_hull: Option<(usize, usize)> = None;
        let mut seeds = Vec::new();
        crate::try_reserve_vec(&mut seeds, 2)?;
        match located {
            Located::OnVertex => return Ok(self.any_alive()),
            Located::Inside(t) => crate::try_push_vec(&mut seeds, t)?,
            Located::OnEdge(e) => {
                let twin = self.halfedges[e];
                if twin == EMPTY {
                    split_hull = Some((self.triangles[e], self.triangles[next_halfedge(e)]));
                    crate::try_push_vec(&mut seeds, e / 3)?;
                } else {
                    crate::try_push_vec(&mut seeds, e / 3)?;
                    crate::try_push_vec(&mut seeds, twin / 3)?;
                }
            },
        }
        // The cavity: every triangle reachable from the seeds across edges
        // whose far triangle's circumcircle strictly contains p. The seeds
        // themselves are in by construction (p is inside or on them).
        self.epoch += 1;
        let epoch = self.epoch;
        let mut cavity = Vec::new();
        crate::try_reserve_vec(&mut cavity, self.triangle_count())?;
        let mut stack = seeds;
        for &t in &stack {
            self.stamp[t] = epoch;
        }
        while let Some(t) = stack.pop() {
            crate::try_push_vec(&mut cavity, t)?;
            for i in 0..3 {
                let e = 3 * t + i;
                let twin = self.halfedges[e];
                if twin == EMPTY || skip.is_some_and(|skip| skip[e]) {
                    continue;
                }
                let n = twin / 3;
                if self.stamp[n] == epoch {
                    continue;
                }
                let [a, b, c] = self.triangle(n);
                if exact::try_incircle(xy(a), xy(b), xy(c), p)? == ExactSign::Positive {
                    self.stamp[n] = epoch;
                    crate::try_push_vec(&mut stack, n)?;
                }
            }
        }
        // The cavity boundary: halfedges of cavity triangles whose twin is
        // outside the cavity (or the hull). Each becomes one new triangle
        // with the site as its third vertex, in the boundary's own order.
        let mut boundary: Vec<(usize, usize, usize)> = Vec::new(); // (a, b, twin slot)
        let boundary_capacity = cavity
            .len()
            .checked_mul(3)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        crate::try_reserve_vec(&mut boundary, boundary_capacity)?;
        for &t in &cavity {
            for i in 0..3 {
                let e = 3 * t + i;
                let twin = self.halfedges[e];
                let ends = (self.triangles[e], self.triangles[next_halfedge(e)]);
                if split_hull == Some(ends) {
                    continue; // the split edge is not a boundary: the site lies on it
                }
                if twin == EMPTY || self.stamp[twin / 3] != epoch {
                    crate::try_push_vec(&mut boundary, (ends.0, ends.1, twin))?;
                }
            }
        }
        // Chain the boundary edges: a -> b, then the edge starting at b, and
        // so on. A closed cycle around an interior site; for a hull split, an
        // open chain from the split edge's end around to its start.
        let start = split_hull.map_or(boundary[0].0, |(_, b)| b);
        let mut ordered = Vec::new();
        crate::try_reserve_vec(&mut ordered, boundary.len())?;
        let mut at = start;
        for _ in 0..boundary.len() {
            let (index, &(a, b, twin)) = boundary
                .iter()
                .enumerate()
                .find(|(_, entry)| entry.0 == at)
                .expect("a cavity boundary is one chain");
            crate::try_push_vec(&mut ordered, (a, b, twin))?;
            boundary.swap_remove(index);
            at = b;
        }
        debug_assert!(
            split_hull.is_some_and(|(a, _)| a == at) || at == start,
            "the cavity boundary closed"
        );
        let free_after_cavity = self
            .free
            .len()
            .checked_add(cavity.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let new_slots = ordered.len().saturating_sub(free_after_cavity);
        let new_triangles = new_slots
            .checked_mul(3)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        self.free.try_reserve(cavity.len())?;
        self.triangles.try_reserve(new_triangles)?;
        self.halfedges.try_reserve(new_triangles)?;
        self.stamp.try_reserve(new_slots)?;
        if split_hull.is_some() {
            self.hull.try_reserve(1)?;
        }
        let mut slots = Vec::new();
        crate::try_reserve_vec(&mut slots, ordered.len())?;
        for &t in &cavity {
            self.triangles[3 * t] = EMPTY;
            self.free.push(t);
        }
        // New triangles (a, b, site): slot 0 = a->b twins the outside, slot 1 =
        // b->site twins slot 2 of the next, slot 2 = site->a twins slot 1 of
        // the previous.
        let count = ordered.len();
        for _ in 0..count {
            crate::try_push_vec(&mut slots, self.take_slot())?;
        }
        let mut first = 0;
        for (k, &(a, b, twin)) in ordered.iter().enumerate() {
            let t = slots[k];
            self.triangles[3 * t] = a;
            self.triangles[3 * t + 1] = b;
            self.triangles[3 * t + 2] = site;
            self.halfedges[3 * t] = twin;
            if twin != EMPTY {
                self.halfedges[twin] = 3 * t;
            }
            if split_hull.is_some() && k + 1 == count {
                // Open chain: the last triangle's `b -> site` and the first
                // triangle's `site -> a` are the two new hull edges.
                self.halfedges[3 * t + 1] = EMPTY;
                self.halfedges[3 * slots[0] + 2] = EMPTY;
            } else {
                let next = slots[(k + 1) % count];
                self.halfedges[3 * t + 1] = 3 * next + 2;
                self.halfedges[3 * next + 2] = 3 * t + 1;
            }
            self.incident[a] = 3 * t;
            self.incident[site] = 3 * t + 2;
            first = t;
        }
        if let Some((a, _)) = split_hull {
            let position = self
                .hull
                .iter()
                .position(|&h| h == a)
                .expect("a hull edge starts on the hull");
            self.hull.insert(position + 1, site);
        }
        Ok(first)
    }

    pub(super) fn insert_constrained(
        &mut self,
        xy: &impl Fn(usize) -> XY,
        site: usize,
        located: Located,
        skip: &[bool],
    ) -> Result<usize> {
        self.try_insert_with(xy, site, located, Some(skip))
    }

    pub(super) fn triangle(&self, t: usize) -> [usize; 3] {
        [
            self.triangles[3 * t],
            self.triangles[3 * t + 1],
            self.triangles[3 * t + 2],
        ]
    }

    fn take_slot(&mut self) -> usize {
        if let Some(t) = self.free.pop() {
            self.stamp[t] = 0;
            return t;
        }
        let t = self.triangle_count();
        self.triangles.extend([EMPTY; 3]);
        self.halfedges.extend([EMPTY; 3]);
        self.stamp.push(0);
        t
    }

    /// Grow the incidence table for sites appended after construction
    /// (refinement vertices).
    pub(super) fn admit_vertex(&mut self, site: usize) -> Result<()> {
        let required = site
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        if self.incident.len() < required {
            self.incident.try_reserve(required - self.incident.len())?;
            self.incident.resize(required, EMPTY);
        }
        Ok(())
    }

    /// Compact out the dead slots, remap twins, and flag cocircular edges
    /// with one exact pass over the finished mesh.
    pub(super) fn finish(self, xy: impl Fn(usize) -> XY) -> ExactMesh {
        let mut remap = vec![EMPTY; self.triangle_count()];
        let mut triangles = Vec::with_capacity(self.triangles.len());
        let mut old_slots = Vec::with_capacity(self.triangles.len());
        for (t, slot) in remap.iter_mut().enumerate() {
            if self.alive(t) {
                *slot = triangles.len() / 3;
                triangles.extend(self.triangle(t));
                old_slots.extend([3 * t, 3 * t + 1, 3 * t + 2]);
            }
        }
        let halfedges: Vec<usize> = old_slots
            .iter()
            .map(|&old| {
                let twin = self.halfedges[old];
                if twin == EMPTY {
                    EMPTY
                } else {
                    3 * remap[twin / 3] + twin % 3
                }
            })
            .collect();
        let mut cocircular = vec![false; halfedges.len()];
        for e in 0..halfedges.len() {
            let twin = halfedges[e];
            if twin == EMPTY || twin < e {
                continue;
            }
            let t = e / 3;
            let [a, b, c] = [triangles[3 * t], triangles[3 * t + 1], triangles[3 * t + 2]];
            let opposite = triangles[prev_halfedge(twin)];
            if exact::incircle(xy(a), xy(b), xy(c), xy(opposite)) == ExactSign::Zero {
                cocircular[e] = true;
                cocircular[twin] = true;
            }
        }
        ExactMesh {
            triangles,
            halfedges,
            hull: self.hull,
            cocircular,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn points(xy: &[(f64, f64)]) -> Vec<Point> {
        xy.iter().map(|&(x, y)| Point::new_raw_xy(x, y)).collect()
    }

    fn random(n: usize, seed: u64) -> Vec<Point> {
        let mut s = seed;
        let mut next = move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s >> 11) as f64 / (1_u64 << 53) as f64
        };
        std::iter::repeat_with(|| Point::new_raw_xy(next() * 1000.0, next() * 1000.0))
            .take(n)
            .collect()
    }

    fn grid(side: usize) -> Vec<Point> {
        (0..side)
            .flat_map(|y| (0..side).map(move |x| Point::new_raw_xy(x as f64, y as f64)))
            .collect()
    }

    fn assert_halfedges_consistent(mesh: &ExactMesh) {
        for e in 0..mesh.halfedges.len() {
            let twin = mesh.halfedges[e];
            if twin == EMPTY {
                continue;
            }
            assert_eq!(mesh.halfedges[twin], e, "twin of twin is self at {e}");
            assert_eq!(
                mesh.triangles[e],
                mesh.triangles[next_halfedge(twin)],
                "twins run opposite at {e}"
            );
            assert_eq!(mesh.triangles[next_halfedge(e)], mesh.triangles[twin]);
        }
    }

    fn assert_delaunay(mesh: &ExactMesh, points: &[Point]) {
        let xy = |s: usize| points[s].xy();
        for e in 0..mesh.halfedges.len() {
            let twin = mesh.halfedges[e];
            if twin == EMPTY {
                continue;
            }
            let t = e / 3;
            let [a, b, c] = [
                mesh.triangles[3 * t],
                mesh.triangles[3 * t + 1],
                mesh.triangles[3 * t + 2],
            ];
            assert_eq!(
                exact::orient2d(xy(a), xy(b), xy(c)),
                ExactSign::Positive,
                "triangle {t} is counter-clockwise"
            );
            let opposite = mesh.triangles[prev_halfedge(twin)];
            let sign = exact::incircle(xy(a), xy(b), xy(c), xy(opposite));
            assert_ne!(
                sign,
                ExactSign::Positive,
                "opposite vertex {opposite} inside circumcircle of {t}"
            );
            assert_eq!(
                sign == ExactSign::Zero,
                mesh.cocircular[e],
                "cocircular flag at {e}"
            );
        }
    }

    fn triangle_set(triangles: &[usize]) -> Vec<[usize; 3]> {
        let mut set: Vec<[usize; 3]> = triangles
            .as_chunks::<3>()
            .0
            .iter()
            .map(|&t| {
                let mut t = t;
                t.sort_unstable();
                t
            })
            .collect();
        set.sort_unstable();
        set
    }

    #[test]
    fn random_sites_are_delaunay_consistent_and_match_the_current_engine() {
        for seed in [1_u64, 7, 42, 1234] {
            let pts = random(400, seed);
            let mesh = exact_delaunay(&pts);
            assert_halfedges_consistent(&mesh);
            assert_delaunay(&mesh, &pts);
            // Random sites have no cocircular quads, so the Delaunay
            // triangulation is unique and must equal the current engine's.
            assert!(
                !mesh.cocircular.iter().any(|&f| f),
                "random sites should not be cocircular"
            );
            let current = super::super::delaunay_triangulation(&pts);
            assert_eq!(
                triangle_set(&mesh.triangles),
                triangle_set(&current.triangles),
                "seed {seed}"
            );
            assert_eq!(mesh.hull, current.hull);
            // Euler for a triangulated convex polygon: T = 2n - h - 2.
            assert_eq!(
                mesh.triangles.len() / 3,
                2 * pts.len() - mesh.hull.len() - 2
            );
        }
    }

    #[test]
    fn hull_only_regular_polygons_are_delaunay() {
        // Every site is a hull corner, so nothing is ever inserted and the
        // seed fan has to be legalized on its own: a regular n-gon's fan from
        // one corner is far from Delaunay.
        for n in 5..=40_usize {
            let points: Vec<Point> = (0..n)
                .map(|i| {
                    let t = std::f64::consts::TAU * i as f64 / n as f64;
                    Point::new_raw_xy(t.cos(), t.sin())
                })
                .collect();
            let mesh = exact_delaunay(&points);
            assert_eq!(mesh.triangles.len() / 3, n - 2, "n={n} triangles");
            assert_eq!(mesh.hull.len(), n, "n={n} hull");
            assert_halfedges_consistent(&mesh);
            assert_delaunay(&mesh, &points);
        }
    }

    #[test]
    fn a_grid_is_delaunay_with_every_diagonal_flagged_cocircular() {
        let pts = grid(20);
        let mesh = exact_delaunay(&pts);
        assert_halfedges_consistent(&mesh);
        assert_delaunay(&mesh, &pts);
        assert_eq!(
            mesh.triangles.len() / 3,
            2 * pts.len() - mesh.hull.len() - 2
        );
        // Every interior diagonal of a unit square is cocircular.
        let flagged = mesh.cocircular.iter().filter(|&&f| f).count() / 2;
        assert_eq!(flagged, 19 * 19, "one cocircular diagonal per grid cell");
    }

    /// Duplicate sites are deduplicated by identity and the distinct sites
    /// are triangulated, hull corners and all; the previous engine refused
    /// them with an empty triangulation.
    #[test]
    fn duplicate_sites_triangulate_the_distinct_points() {
        let dup = points(&[
            (0.0, 0.0),
            (1.0, 0.0),
            (1.0, 0.0),
            (0.0, 1.0),
            (1.0, 1.0),
            (0.5, 0.5),
            (0.5, 0.5),
        ]);
        let mesh = exact_delaunay(&dup);
        assert_halfedges_consistent(&mesh);
        assert_delaunay(&mesh, &dup);
        assert_eq!(mesh.hull, super::super::exact_corner_hull_cycle(&dup));
        // Five distinct sites, four on the hull: 2*5 - 4 - 2 = 4 triangles.
        assert_eq!(mesh.triangles.len() / 3, 4);
        let public = super::super::delaunay_triangulation(&dup);
        assert_eq!(
            public.triangles.len() / 3,
            4,
            "the public entry point triangulates the distinct sites"
        );
    }

    #[test]
    fn collinear_input_matches_the_current_hull() {
        let line = points(&[(0.0, 0.0), (1.0, 1.0), (2.0, 2.0), (3.0, 3.0)]);
        let mesh = exact_delaunay(&line);
        assert!(mesh.triangles.is_empty());
        assert_eq!(mesh.hull, super::super::delaunay_triangulation(&line).hull);
    }

    #[test]
    fn sites_on_existing_edges_and_vertices_insert_exactly() {
        // A square, then its centre, then midpoints of the two diagonals'
        // halves (on edges), then a repeat of the centre (on a vertex).
        let pts = points(&[
            (0.0, 0.0),
            (4.0, 0.0),
            (4.0, 4.0),
            (0.0, 4.0),
            (2.0, 2.0),
            (1.0, 1.0),
            (3.0, 3.0),
            (2.0, 2.0),
        ]);
        let mesh = exact_delaunay(&pts);
        assert_halfedges_consistent(&mesh);
        assert_delaunay(&mesh, &pts);
        assert_eq!(mesh.triangles.len() / 3, 2 * 7 - 4 - 2);
    }
}
