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
//! Sites are inserted in Hilbert order, and each point-location walk starts
//! from the previous insertion's face. The seed fans the strict corner hull;
//! later sites can lie on a hull edge, where insertion extends that hull. Output
//! uses the halfedge layout the rest of the
//! tessellation code already reads: triangle `t` owns slots `3t..3t + 3`,
//! `halfedges[e]` is the twin slot or `EMPTY`, and `hull` is the counter-
//! clockwise hull cycle.

#![expect(
    clippy::many_single_char_names,
    reason = "a, b, c, d name the vertices of a quad and e, f, t, u its slots, the geometry kernel convention"
)]

use crate::error::Result;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::tessellation::exact;
use crate::geometry::{Point, XY};

pub(crate) const EMPTY: usize = usize::MAX;

pub(crate) const fn next_halfedge(edge: usize) -> usize {
    if edge.wrapping_add(1).is_multiple_of(3) {
        edge.wrapping_sub(2)
    } else {
        edge.wrapping_add(1)
    }
}

pub(crate) const fn prev_halfedge(edge: usize) -> usize {
    if edge.is_multiple_of(3) {
        edge.wrapping_add(2)
    } else {
        edge.wrapping_sub(1)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FaceId(usize);

impl FaceId {
    const fn from_index(index: usize) -> Self {
        Self(index)
    }

    pub(super) const fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct HalfedgeId(usize);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FaceSide {
    First,
    Second,
    Third,
}

impl FaceSide {
    pub(super) const ALL: [Self; 3] = [Self::First, Self::Second, Self::Third];

    const fn offset(self) -> usize {
        match self {
            Self::First => 0,
            Self::Second => 1,
            Self::Third => 2,
        }
    }
}

impl HalfedgeId {
    pub(super) const fn from_index(index: usize) -> Self {
        Self(index)
    }

    pub(super) const fn index(self) -> usize {
        self.0
    }

    pub(super) const fn in_face(face: FaceId, side: FaceSide) -> Self {
        Self(face.index().wrapping_mul(3).wrapping_add(side.offset()))
    }

    pub(super) const fn face(self) -> FaceId {
        FaceId::from_index(self.0 / 3)
    }

    const fn side(self) -> FaceSide {
        if self.0.is_multiple_of(3) {
            FaceSide::First
        } else if self.0.wrapping_add(1).is_multiple_of(3) {
            FaceSide::Third
        } else {
            FaceSide::Second
        }
    }

    pub(super) const fn next(self) -> Self {
        Self(next_halfedge(self.0))
    }

    pub(super) const fn prev(self) -> Self {
        Self(prev_halfedge(self.0))
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
pub(super) fn exact_delaunay(points: &[Point]) -> Result<ExactMesh> {
    let Some(mesh) = Mesh::try_delaunay(points)? else {
        return Ok(ExactMesh {
            triangles: Vec::new(),
            halfedges: Vec::new(),
            hull: super::exact_hull_cycle(points),
            cocircular: Vec::new(),
        });
    };
    mesh.finish(|site| site_xy(points, site))
}

#[expect(
    clippy::indexing_slicing,
    reason = "site ordinals are admitted by this exact mesh from the same point slice"
)]
pub(super) const fn site_xy(points: &[Point], site: usize) -> XY {
    points[site].xy()
}

impl Mesh {
    /// Exact Delaunay construction for constrained and unconstrained publication.
    pub(super) fn try_delaunay(points: &[Point]) -> Result<Option<Self>> {
        let hull = super::exact_corner_hull_cycle(points);
        if hull.len() < 3 {
            return Ok(None);
        }
        let xy = |site| site_xy(points, site);
        let mut mesh = Self::seed(&hull, points.len());
        mesh.try_legalize(&xy)?;

        // Remaining distinct sites exclude only strict hull corners, so a
        // collinear hull site is inserted along with the interior sites.
        let mut placed = crate::collections::try_hash_set_with_capacity(points.len())?;
        for &site in &hull {
            placed.insert(key(xy(site)));
        }
        let mut hull_sites = crate::collections::try_hash_set_with_capacity(hull.len())?;
        for &site in &hull {
            hull_sites.insert(site);
        }
        let (min, max) = bounds(points);
        let mut interior = Vec::with_capacity(points.len());
        for site in 0..points.len() {
            if !hull_sites.contains(&site) && placed.insert(key(xy(site))) {
                interior.push((hilbert16(xy(site), min, max), site));
            }
        }
        interior.sort_unstable();

        let mut last = FaceId::from_index(0);
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
        let quadrant = 3_u32.wrapping_mul(rx) ^ ry;
        d = d.wrapping_add(s.wrapping_mul(s).wrapping_mul(quadrant));
        if ry == 0 {
            if rx == 1 {
                let mask = N.wrapping_sub(1);
                x ^= mask;
                y ^= mask;
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
    Inside(FaceId),
    /// On this halfedge (exactly collinear with it, strictly between its ends).
    OnEdge(HalfedgeId),
    /// Exactly on an existing vertex: nothing to insert.
    OnVertex,
    /// Outside the current convex hull, where the insertion must extend it.
    Outside,
}

pub(super) struct Mesh {
    triangles: Vec<usize>,
    halfedges: Vec<usize>,
    hull: Vec<usize>,
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
    #[expect(
        clippy::indexing_slicing,
        reason = "FaceId is minted by this mesh's seed and slot allocator; dead faces retain their slot"
    )]
    fn face_slots(&self, face: FaceId) -> &[usize; 3] {
        &self.triangles.as_chunks::<3>().0[face.index()]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "FaceId is minted by this mesh's seed and slot allocator; dead faces retain their slot"
    )]
    fn face_slots_mut(&mut self, face: FaceId) -> &mut [usize; 3] {
        &mut self.triangles.as_chunks_mut::<3>().0[face.index()]
    }

    const fn slot<T: Copy>(row: &[T; 3], side: FaceSide) -> T {
        let [first, second, third] = *row;
        match side {
            FaceSide::First => first,
            FaceSide::Second => second,
            FaceSide::Third => third,
        }
    }

    fn set_slot<T>(row: &mut [T; 3], side: FaceSide, value: T) {
        let [first, second, third] = row;
        match side {
            FaceSide::First => *first = value,
            FaceSide::Second => *second = value,
            FaceSide::Third => *third = value,
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "HalfedgeId is minted from this mesh's admitted three-slot rows"
    )]
    const fn set_flat_halfedge_slot(slots: &mut [usize], edge: HalfedgeId, value: usize) {
        slots[edge.index()] = value;
    }

    fn set_face_vertex(&mut self, face: FaceId, side: FaceSide, vertex: usize) {
        Self::set_slot(self.face_slots_mut(face), side, vertex);
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "HalfedgeId identifies an admitted stable flat slot owned by this mesh"
    )]
    pub(super) fn edge_start(&self, edge: HalfedgeId) -> usize {
        self.triangles[edge.index()]
    }

    pub(super) fn edge_end(&self, edge: HalfedgeId) -> usize {
        self.edge_start(edge.next())
    }

    pub(super) fn edge_apex(&self, edge: HalfedgeId) -> usize {
        self.edge_start(edge.prev())
    }

    pub(super) fn edge_vertices(&self, edge: HalfedgeId) -> (usize, usize) {
        (self.edge_start(edge), self.edge_end(edge))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "HalfedgeId identifies an admitted stable flat slot owned by this mesh"
    )]
    pub(super) fn twin(&self, edge: HalfedgeId) -> usize {
        self.halfedges[edge.index()]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "HalfedgeId identifies an admitted stable flat slot owned by this mesh"
    )]
    fn set_twin(&mut self, edge: HalfedgeId, twin: usize) {
        self.halfedges[edge.index()] = twin;
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the side table is admitted with one entry for every mesh halfedge"
    )]
    const fn side_value<T: Copy>(side: &[T], edge: HalfedgeId) -> T {
        Self::slot(&side.as_chunks::<3>().0[edge.face().index()], edge.side())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the side table is admitted with one entry for every mesh halfedge"
    )]
    fn set_side_face<T>(side: &mut [T], face: FaceId, values: [T; 3]) {
        let row = &mut side.as_chunks_mut::<3>().0[face.index()];
        let [first, second, third] = row;
        let [value_first, value_second, value_third] = values;
        *first = value_first;
        *second = value_second;
        *third = value_third;
    }

    pub(super) const fn edge_count(&self) -> usize {
        self.halfedges.len()
    }

    pub(super) fn edge_ids(&self) -> impl Iterator<Item = HalfedgeId> + '_ {
        (0..self.halfedges.len()).map(HalfedgeId::from_index)
    }

    pub(super) fn face_ids(&self) -> impl Iterator<Item = FaceId> + '_ {
        (0..self.triangle_count()).map(FaceId::from_index)
    }

    fn incident(&self, site: usize) -> usize {
        Self::site_entry(&self.incident, site)
    }

    fn set_incident(&mut self, site: usize, edge: usize) {
        Self::set_site_entry(&mut self.incident, site, edge);
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "site ordinals in the incidence table come from the admitted point slice"
    )]
    const fn site_entry(values: &[usize], site: usize) -> usize {
        values[site]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "site ordinals in the incidence table come from the admitted point slice"
    )]
    const fn set_site_entry(values: &mut [usize], site: usize, value: usize) {
        values[site] = value;
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "FaceId is minted by this mesh's seed and slot allocator"
    )]
    fn stamp(&self, face: FaceId) -> u32 {
        self.stamp[face.index()]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "FaceId is minted by this mesh's seed and slot allocator"
    )]
    fn set_stamp(&mut self, face: FaceId, stamp: u32) {
        self.stamp[face.index()] = stamp;
    }

    fn seed(hull: &[usize], vertex_count: usize) -> Self {
        let count = hull.len().wrapping_sub(2);
        let slots = count.wrapping_mul(3);
        let mut triangles = Vec::with_capacity(slots);
        let mut halfedges = vec![EMPTY; slots];
        let mut hull_sites = hull.iter().copied();
        let root = hull_sites
            .next()
            .expect("a two-dimensional hull has corners");
        let first = hull_sites
            .next()
            .expect("a two-dimensional hull has corners");
        let mut previous = hull_sites
            .next()
            .expect("a two-dimensional hull has three corners");
        triangles.extend([root, first, previous]);
        let mut previous_face = FaceId::from_index(0);
        for next in hull_sites {
            let face = FaceId::from_index(previous_face.index().wrapping_add(1));
            // Consecutive fan faces share slot 2 of the previous face with
            // slot 0 of the new face. The rows stay in the public slot order.
            let previous_spoke = HalfedgeId::in_face(previous_face, FaceSide::Third);
            let next_spoke = HalfedgeId::in_face(face, FaceSide::First);
            Self::set_flat_halfedge_slot(&mut halfedges, previous_spoke, next_spoke.index());
            Self::set_flat_halfedge_slot(&mut halfedges, next_spoke, previous_spoke.index());
            triangles.extend([root, previous, next]);
            previous = next;
            previous_face = face;
        }
        let mut incident = vec![EMPTY; vertex_count];
        for (face, vertices) in triangles.as_chunks::<3>().0.iter().enumerate() {
            let [first, second, third] = *vertices;
            for (side, vertex) in [
                (FaceSide::First, first),
                (FaceSide::Second, second),
                (FaceSide::Third, third),
            ] {
                if Self::site_entry(&incident, vertex) == EMPTY {
                    Self::set_site_entry(
                        &mut incident,
                        vertex,
                        HalfedgeId::in_face(FaceId::from_index(face), side).index(),
                    );
                }
            }
        }
        Self {
            triangles,
            halfedges,
            hull: hull.to_vec(),
            incident,
            free: Vec::new(),
            stamp: vec![0; count],
            epoch: 0,
        }
    }

    pub(super) const fn triangle_count(&self) -> usize {
        self.triangles.len() / 3
    }

    pub(super) fn alive(&self, face: FaceId) -> bool {
        let [first, _, _] = *self.face_slots(face);
        first != EMPTY
    }

    /// Every halfedge slot leaving `u`, walking its fan both ways so a hull
    /// vertex's open fan is covered too.
    pub(super) fn outgoing(&self, u: usize) -> Vec<HalfedgeId> {
        let start = self.incident(u);
        let mut slots = Vec::with_capacity(self.triangle_count());
        let start = HalfedgeId::from_index(start);
        if start.index() == EMPTY || !self.alive(start.face()) || self.edge_start(start) != u {
            // The remembered slot died in a cavity: fall back to a scan.
            for (face, vertices) in self.triangles.as_chunks::<3>().0.iter().enumerate() {
                let face = FaceId::from_index(face);
                let [first, second, third] = *vertices;
                if !self.alive(face) {
                    continue;
                }
                for (side, vertex) in [
                    (FaceSide::First, first),
                    (FaceSide::Second, second),
                    (FaceSide::Third, third),
                ] {
                    if vertex == u {
                        slots.push(HalfedgeId::in_face(face, side));
                    }
                }
            }
            return slots;
        }
        // Counter-clockwise: across the edge into `u`.
        let mut e = start;
        loop {
            slots.push(e);
            let twin = self.twin(e.prev());
            if twin == EMPTY {
                break;
            }
            e = HalfedgeId::from_index(twin);
            if e == start {
                return slots;
            }
        }
        // Clockwise from the start: across the edge out of `u`.
        let mut e = start;
        loop {
            let twin = self.twin(e);
            if twin == EMPTY {
                break;
            }
            e = HalfedgeId::from_index(twin).next();
            if e == start {
                break;
            }
            slots.push(e);
        }
        slots
    }

    /// The slot of the edge between `u` and `v` in either direction, if it
    /// is an edge; a hull edge exists as one halfedge only.
    pub(super) fn edge_slot(&self, u: usize, v: usize) -> Option<HalfedgeId> {
        if let Some(slot) = self.directed_slot(u, v) {
            return Some(slot);
        }
        self.directed_slot(v, u)
    }

    fn directed_slot(&self, u: usize, v: usize) -> Option<HalfedgeId> {
        self.outgoing(u)
            .into_iter()
            .find(|&e| self.edge_end(e) == v)
    }

    /// Lawson legalization of every interior edge: flip while the twin's apex
    /// lies strictly inside the circumcircle. The seed fan triangulates the
    /// hull without being Delaunay, and cavity insertion preserves the
    /// Delaunay property only from a Delaunay start. A slot left on the stack
    /// by a flip names whatever edge now lives there; re-checking a legal
    /// edge is harmless, and every edge a flip disturbed is pushed again.
    fn try_legalize(&mut self, xy: &impl Fn(usize) -> XY) -> Result<()> {
        let mut stack = Vec::with_capacity(self.halfedges.len());
        for (e, &twin) in self.halfedges.iter().enumerate() {
            if twin != EMPTY && twin > e {
                stack.push(HalfedgeId::from_index(e));
            }
        }
        while let Some(edge) = stack.pop() {
            let twin = self.twin(edge);
            if twin == EMPTY {
                continue;
            }
            let [a, b, c] = self.triangle(edge.face());
            let d = self.edge_apex(HalfedgeId::from_index(twin));
            if exact::incircle(xy(a), xy(b), xy(c), xy(d))? != ExactSign::Positive {
                continue;
            }
            let diagonal = self.flip(edge);
            let twin = self.twin(diagonal);
            debug_assert_ne!(twin, EMPTY, "an interior diagonal has a twin");
            let twin = HalfedgeId::from_index(twin);
            for edge in [diagonal.next(), diagonal.prev(), twin.next(), twin.prev()] {
                stack.push(edge);
            }
        }
        Ok(())
    }

    pub(super) fn try_legalize_constrained_from(
        &mut self,
        xy: &impl Fn(usize) -> XY,
        constrained: &mut [bool],
        mut stack: Vec<HalfedgeId>,
    ) -> Result<()> {
        while let Some(edge) = stack.pop() {
            let twin = self.twin(edge);
            if twin == EMPTY || Self::side_value(constrained, edge) {
                continue;
            }
            let [a, b, c] = self.triangle(edge.face());
            let opposite = self.edge_apex(HalfedgeId::from_index(twin));
            if exact::incircle(xy(a), xy(b), xy(c), xy(opposite))? != ExactSign::Positive {
                continue;
            }
            let diagonal = self.flip_carrying(edge, constrained, false);
            let twin = self.twin(diagonal);
            if twin == EMPTY {
                return Err(crate::geometry::GeometryErrorKind::triangulation(
                    "constrained legalization lost an interior diagonal",
                ));
            }
            let twin = HalfedgeId::from_index(twin);
            for edge in [diagonal.next(), diagonal.prev(), twin.next(), twin.prev()] {
                stack.push(edge);
            }
        }
        Ok(())
    }

    /// Flip the edge at slot `e` (`a -> b`, apex `c`; twin apex `d`) to
    /// `c -> d`, returning the slot of `c -> d`. The caller has checked the
    /// quad is convex.
    pub(super) fn flip(&mut self, e: HalfedgeId) -> HalfedgeId {
        let f = self.twin(e);
        debug_assert_ne!(f, EMPTY, "a hull edge cannot flip");
        let f = HalfedgeId::from_index(f);
        let (t, u) = (e.face(), f.face());
        let (a, b, c) = (self.edge_start(e), self.edge_end(e), self.edge_apex(e));
        let d = self.edge_apex(f);
        // Outside neighbours that change owner: across a-d and across b-c.
        let across_ad = self.twin(f.next());
        let across_bc = self.twin(e.next());
        let keep_ca = self.twin(e.prev());
        let keep_db = self.twin(f.prev());
        // t becomes (a, d, c); u becomes (b, c, d).
        {
            let [first, second, third] = self.face_slots_mut(t);
            *first = a;
            *second = d;
            *third = c;
        }
        {
            let [first, second, third] = self.face_slots_mut(u);
            *first = b;
            *second = c;
            *third = d;
        }
        let t_first = HalfedgeId::in_face(t, FaceSide::First);
        let t_second = HalfedgeId::in_face(t, FaceSide::Second);
        let t_third = HalfedgeId::in_face(t, FaceSide::Third);
        let u_first = HalfedgeId::in_face(u, FaceSide::First);
        let u_second = HalfedgeId::in_face(u, FaceSide::Second);
        let u_third = HalfedgeId::in_face(u, FaceSide::Third);
        self.set_twin(t_first, across_ad);
        if across_ad != EMPTY {
            self.set_twin(HalfedgeId::from_index(across_ad), t_first.index());
        }
        self.set_twin(t_second, u_second.index());
        self.set_twin(u_second, t_second.index());
        self.set_twin(t_third, keep_ca);
        if keep_ca != EMPTY {
            self.set_twin(HalfedgeId::from_index(keep_ca), t_third.index());
        }
        self.set_twin(u_first, across_bc);
        if across_bc != EMPTY {
            self.set_twin(HalfedgeId::from_index(across_bc), u_first.index());
        }
        self.set_twin(u_third, keep_db);
        if keep_db != EMPTY {
            self.set_twin(HalfedgeId::from_index(keep_db), u_third.index());
        }
        self.set_incident(a, t_first.index());
        self.set_incident(d, t_second.index());
        self.set_incident(c, t_third.index());
        self.set_incident(b, u_first.index());
        u_second
    }

    /// [`Self::flip`] that moves a per-halfedge side table with the four outer
    /// edges it relocates; both halves of the new diagonal take `diagonal`.
    pub(super) fn flip_carrying<T: Copy>(
        &mut self,
        e: HalfedgeId,
        side: &mut [T],
        diagonal: T,
    ) -> HalfedgeId {
        let f = HalfedgeId::from_index(self.twin(e));
        let outer = [
            Self::side_value(side, f.next()),
            Self::side_value(side, e.prev()),
            Self::side_value(side, e.next()),
            Self::side_value(side, f.prev()),
        ];
        let new_edge = self.flip(e);
        Self::set_side_face(side, e.face(), [outer[0], diagonal, outer[1]]);
        Self::set_side_face(side, f.face(), [outer[2], diagonal, outer[3]]);
        new_edge
    }

    /// Walk from triangle `start` toward `p` by exact orientation. Every
    /// inserted site lies in the closed seed hull; a hull-edge site is
    /// classified as `OnEdge` before the walk could cross the hull.
    pub(super) fn locate(&self, xy: &impl Fn(usize) -> XY, start: FaceId, p: XY) -> Located {
        self.locate_bounded(xy, start, p, &[])
            .unwrap_or_else(|_| unreachable!("a site in the closed hull is locatable"))
    }

    /// The orientation walk that never crosses a hull edge or a slot `stop`
    /// marks: `Err(e)` is the edge it would have had to cross. A constrained
    /// mesh is not Delaunay, where the walk can cycle, so a walk longer than
    /// the mesh settles by scanning every live face.
    pub(super) fn locate_bounded(
        &self,
        xy: &impl Fn(usize) -> XY,
        start: FaceId,
        p: XY,
        stop: &[bool],
    ) -> Result<Located, HalfedgeId> {
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
            let twin = self.twin(e);
            if twin == EMPTY || stop.get(e.index()).is_some_and(|&stopped| stopped) {
                return Err(e);
            }
            steps = steps.wrapping_add(1);
            if steps > self.triangle_count() {
                return (0..self.triangle_count())
                    .map(FaceId::from_index)
                    .filter(|&f| self.alive(f))
                    .find_map(|f| self.classify(xy, f, p).ok())
                    .ok_or(e);
            }
            t = HalfedgeId::from_index(twin).face();
        }
    }

    pub(super) fn outside_hull(&self, xy: &impl Fn(usize) -> XY, point: XY) -> bool {
        let hull_len = self.hull.len();
        if hull_len < 3 {
            return true;
        }
        self.hull.iter().enumerate().any(|(index, &start)| {
            self.hull
                .get(index.wrapping_add(1) % hull_len)
                .is_some_and(|&end| {
                    exact::orient2d(xy(start), xy(end), point) == ExactSign::Negative
                })
        })
    }

    /// Where `p` stands relative to live triangle `t`: `Err(e)` names the
    /// first edge `p` is strictly to the right of.
    pub(super) fn classify(
        &self,
        xy: &impl Fn(usize) -> XY,
        t: FaceId,
        p: XY,
    ) -> Result<Located, HalfedgeId> {
        let mut collinear_count = 0_usize;
        let mut collinear_edge = FaceSide::First;
        for side in FaceSide::ALL {
            let edge = HalfedgeId::in_face(t, side);
            match exact::orient2d(xy(self.edge_start(edge)), xy(self.edge_end(edge)), p) {
                ExactSign::Negative => return Err(edge),
                ExactSign::Zero => {
                    collinear_count = collinear_count.wrapping_add(1);
                    collinear_edge = side;
                },
                ExactSign::Positive => {},
            }
        }
        Ok(match collinear_count {
            0 => Located::Inside(t),
            1 => {
                let edge = HalfedgeId::in_face(t, collinear_edge);
                let (a, b) = (self.edge_start(edge), self.edge_end(edge));
                if p == xy(a) || p == xy(b) {
                    Located::OnVertex
                } else {
                    Located::OnEdge(edge)
                }
            },
            _ => Located::OnVertex,
        })
    }

    fn any_alive(&self) -> FaceId {
        (0..self.triangle_count())
            .map(FaceId::from_index)
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
    ) -> Result<FaceId> {
        // A site on a hull edge splits that edge: the cavity boundary is then
        // an open chain from the edge's end to its start, the site joins the
        // hull between them, and the two chain ends become hull edges.
        let (split_hull, seeds) = match located {
            Located::OnVertex => return Ok(self.any_alive()),
            Located::Inside(t) => (None, vec![t]),
            Located::Outside => {
                return Err(crate::geometry::GeometryErrorKind::triangulation(
                    "outside insertion requires the hull expansion path",
                ));
            },
            Located::OnEdge(edge) => {
                let twin = self.twin(edge);
                if twin == EMPTY {
                    (Some(self.edge_vertices(edge)), vec![edge.face()])
                } else {
                    (None, vec![edge.face(), HalfedgeId::from_index(twin).face()])
                }
            },
        };
        let p = xy(site);
        // The cavity: every triangle reachable from the seeds across edges
        // whose far triangle's circumcircle strictly contains p. The seeds
        // themselves are in by construction (p is inside or on them).
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.stamp.fill(0);
            self.epoch = 1;
        }
        let epoch = self.epoch;
        let mut cavity = Vec::with_capacity(self.triangle_count());
        let mut stack = seeds;
        for &face in &stack {
            self.set_stamp(face, epoch);
        }
        while let Some(face) = stack.pop() {
            cavity.push(face);
            for side in FaceSide::ALL {
                let edge = HalfedgeId::in_face(face, side);
                let twin = self.twin(edge);
                if twin == EMPTY || skip.is_some_and(|skip| Self::side_value(skip, edge)) {
                    continue;
                }
                let neighbor = HalfedgeId::from_index(twin).face();
                if self.stamp(neighbor) == epoch {
                    continue;
                }
                let [a, b, c] = self.triangle(neighbor);
                if exact::incircle(xy(a), xy(b), xy(c), p)? == ExactSign::Positive {
                    self.set_stamp(neighbor, epoch);
                    stack.push(neighbor);
                }
            }
        }
        // The cavity boundary: halfedges of cavity triangles whose twin is
        // outside the cavity (or the hull). Each becomes one new triangle
        // with the site as its third vertex, in the boundary's own order.
        let mut boundary: Vec<(usize, usize, usize)> = Vec::new(); // (a, b, twin slot)
        let boundary_capacity = cavity.len().wrapping_mul(3);
        boundary.reserve(boundary_capacity);
        for &face in &cavity {
            for side in FaceSide::ALL {
                let edge = HalfedgeId::in_face(face, side);
                let twin = self.twin(edge);
                let ends = self.edge_vertices(edge);
                if split_hull == Some(ends) {
                    continue; // the split edge is not a boundary: the site lies on it
                }
                if twin == EMPTY || self.stamp(HalfedgeId::from_index(twin).face()) != epoch {
                    boundary.push((ends.0, ends.1, twin));
                }
            }
        }
        // Chain the boundary edges: a -> b, then the edge starting at b, and
        // so on. A closed cycle around an interior site; for a hull split, an
        // open chain from the split edge's end around to its start.
        let start = split_hull.map_or_else(
            || boundary.first().expect("a cavity has a boundary").0,
            |(_, b)| b,
        );
        let mut ordered = Vec::with_capacity(boundary.len());
        let mut at = start;
        if boundary.len() <= 16 {
            for _ in 0..boundary.len() {
                let (index, &(a, b, twin)) = boundary
                    .iter()
                    .enumerate()
                    .find(|(_, entry)| entry.0 == at)
                    .expect("a cavity boundary is one chain");
                ordered.push((a, b, twin));
                boundary.swap_remove(index);
                at = b;
            }
        } else {
            boundary.sort_unstable_by_key(|&(a, ..)| a);
            for _ in 0..boundary.len() {
                let index = boundary
                    .binary_search_by_key(&at, |&(a, ..)| a)
                    .expect("a cavity boundary is one chain");
                let &(a, b, twin) = boundary
                    .get(index)
                    .expect("the boundary lookup returns its own index");
                ordered.push((a, b, twin));
                at = b;
            }
        }
        debug_assert!(
            split_hull.is_some_and(|(a, _)| a == at) || at == start,
            "the cavity boundary closed"
        );
        let free_after_cavity = self.free.len().wrapping_add(cavity.len());
        let new_slots = ordered.len().saturating_sub(free_after_cavity);
        let new_triangles = new_slots.wrapping_mul(3);
        self.free.reserve(cavity.len());
        self.triangles.reserve(new_triangles);
        self.halfedges.reserve(new_triangles);
        self.stamp.reserve(new_slots);
        if split_hull.is_some() {
            self.hull.reserve(1);
        }
        let mut slots = Vec::with_capacity(ordered.len());
        for &face in &cavity {
            self.set_face_vertex(face, FaceSide::First, EMPTY);
            self.free.push(face.index());
        }
        // New triangles (a, b, site): slot 0 = a->b twins the outside, slot 1 =
        // b->site twins slot 2 of the next, slot 2 = site->a twins slot 1 of
        // the previous.
        for _ in 0..ordered.len() {
            slots.push(self.take_slot());
        }
        let first = *slots.first().expect("a cavity has a boundary");
        let mut slots = slots.into_iter().peekable();
        let mut last = first;
        for (a, b, twin) in ordered.iter().copied() {
            let face = slots.next().expect("each boundary edge has one face slot");
            let next = slots.peek().copied();
            self.set_face_vertex(face, FaceSide::First, a);
            self.set_face_vertex(face, FaceSide::Second, b);
            self.set_face_vertex(face, FaceSide::Third, site);
            let first_edge = HalfedgeId::in_face(face, FaceSide::First);
            let second_edge = HalfedgeId::in_face(face, FaceSide::Second);
            let third_edge = HalfedgeId::in_face(face, FaceSide::Third);
            self.set_twin(first_edge, twin);
            if twin != EMPTY {
                self.set_twin(HalfedgeId::from_index(twin), first_edge.index());
            }
            if split_hull.is_some() && next.is_none() {
                // Open chain: the last triangle's `b -> site` and the first
                // triangle's `site -> a` are the two new hull edges.
                self.set_twin(second_edge, EMPTY);
            } else {
                let next = next.unwrap_or(first);
                let next_edge = HalfedgeId::in_face(next, FaceSide::Third);
                self.set_twin(second_edge, next_edge.index());
                self.set_twin(next_edge, second_edge.index());
            }
            self.set_incident(a, first_edge.index());
            self.set_incident(site, third_edge.index());
            last = face;
        }
        if split_hull.is_some() {
            self.set_twin(HalfedgeId::in_face(first, FaceSide::Third), EMPTY);
        }
        if let Some((a, _)) = split_hull {
            let position = self
                .hull
                .iter()
                .position(|&h| h == a)
                .expect("a hull edge starts on the hull");
            self.hull.insert(position.wrapping_add(1), site);
        }
        Ok(last)
    }

    pub(super) fn insert_constrained(
        &mut self,
        xy: &impl Fn(usize) -> XY,
        site: usize,
        located: Located,
        skip: &[bool],
    ) -> Result<FaceId> {
        self.try_insert_with(xy, site, located, Some(skip))
    }

    pub(super) fn insert_outside(
        &mut self,
        xy: &impl Fn(usize) -> XY,
        site: usize,
    ) -> Result<(FaceId, Vec<HalfedgeId>)> {
        let hull_len = self.hull.len();
        if hull_len < 3 {
            return Err(crate::geometry::GeometryErrorKind::triangulation(
                "outside insertion needs a two-dimensional hull",
            ));
        }
        let point = xy(site);
        let mut visible = vec![false; hull_len];
        let mut visible_count = 0_usize;
        for (index, &start) in self.hull.iter().enumerate() {
            let end_index = index.wrapping_add(1) % hull_len;
            let Some(&end) = self.hull.get(end_index) else {
                return Err(crate::geometry::GeometryErrorKind::triangulation(
                    "outside insertion found an incomplete hull",
                ));
            };
            if exact::orient2d(xy(start), xy(end), point) == ExactSign::Negative {
                if let Some(is_visible) = visible.get_mut(index) {
                    *is_visible = true;
                    visible_count = visible_count.wrapping_add(1);
                }
            }
        }
        if visible_count == 0 {
            return Err(crate::geometry::GeometryErrorKind::triangulation(
                "outside point cannot see the mesh hull",
            ));
        }
        let Some(start_index) = visible
            .iter()
            .cycle()
            .skip(hull_len.wrapping_sub(1))
            .zip(&visible)
            .position(|(previous, current)| !*previous && *current)
        else {
            return Err(crate::geometry::GeometryErrorKind::triangulation(
                "outside insertion has no visible hull chain",
            ));
        };
        let mut chain_len = 0_usize;
        while visible
            .get(start_index.wrapping_add(chain_len) % hull_len)
            .is_some_and(|&is_visible| is_visible)
        {
            chain_len = chain_len.wrapping_add(1);
        }
        if chain_len != visible_count {
            return Err(crate::geometry::GeometryErrorKind::triangulation(
                "outside point sees disjoint hull chains",
            ));
        }
        let mut visible_edges = Vec::with_capacity(chain_len);
        for offset in 0..chain_len {
            let index = start_index.wrapping_add(offset) % hull_len;
            let Some(&start) = self.hull.get(index) else {
                return Err(crate::geometry::GeometryErrorKind::triangulation(
                    "outside insertion found an incomplete hull",
                ));
            };
            let Some(&end) = self.hull.get(index.wrapping_add(1) % hull_len) else {
                return Err(crate::geometry::GeometryErrorKind::triangulation(
                    "outside insertion found an incomplete hull",
                ));
            };
            let Some(edge) = self.directed_slot(start, end) else {
                return Err(crate::geometry::GeometryErrorKind::triangulation(
                    "outside insertion could not find a hull edge",
                ));
            };
            if self.twin(edge) != EMPTY
                || self.edge_vertices(edge) != (start, end)
                || exact::orient2d(xy(end), xy(start), point) != ExactSign::Positive
            {
                return Err(crate::geometry::GeometryErrorKind::triangulation(
                    "outside insertion found an invalid visible hull edge",
                ));
            }
            visible_edges.push((start, end, edge));
        }
        let start = visible_edges
            .first()
            .map(|&(start, ..)| start)
            .ok_or_else(|| {
                crate::geometry::GeometryErrorKind::triangulation(
                    "outside insertion has no visible hull edge",
                )
            })?;
        let end_index = start_index.wrapping_add(chain_len) % hull_len;
        let mut new_hull = Vec::with_capacity(hull_len.wrapping_sub(chain_len).wrapping_add(2));
        new_hull.push(start);
        new_hull.push(site);
        for offset in 0..hull_len.wrapping_sub(chain_len) {
            let index = end_index.wrapping_add(offset) % hull_len;
            let Some(&vertex) = self.hull.get(index) else {
                return Err(crate::geometry::GeometryErrorKind::triangulation(
                    "outside insertion found an incomplete hull",
                ));
            };
            new_hull.push(vertex);
        }
        let new_slots = chain_len.saturating_sub(self.free.len());
        let new_edges = new_slots
            .checked_mul(3)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        self.triangles.reserve(new_edges);
        self.halfedges.reserve(new_edges);
        self.stamp.reserve(new_slots);
        self.free.reserve(chain_len);
        self.admit_vertex(site);
        let mut faces = Vec::with_capacity(chain_len);
        for _ in 0..chain_len {
            faces.push(self.take_slot());
        }
        let mut seeds = Vec::with_capacity(chain_len);
        for (index, &(start, end, old_edge)) in visible_edges.iter().enumerate() {
            let Some(&face) = faces.get(index) else {
                return Err(crate::geometry::GeometryErrorKind::triangulation(
                    "outside insertion lost a triangle slot",
                ));
            };
            let first = HalfedgeId::in_face(face, FaceSide::First);
            let second = HalfedgeId::in_face(face, FaceSide::Second);
            let third = HalfedgeId::in_face(face, FaceSide::Third);
            self.set_face_vertex(face, FaceSide::First, end);
            self.set_face_vertex(face, FaceSide::Second, start);
            self.set_face_vertex(face, FaceSide::Third, site);
            self.set_twin(first, old_edge.index());
            self.set_twin(old_edge, first.index());
            if index == 0 {
                self.set_twin(second, EMPTY);
            } else {
                let Some(&previous) = faces.get(index.wrapping_sub(1)) else {
                    return Err(crate::geometry::GeometryErrorKind::triangulation(
                        "outside insertion lost a previous triangle",
                    ));
                };
                let previous_third = HalfedgeId::in_face(previous, FaceSide::Third);
                self.set_twin(second, previous_third.index());
                self.set_twin(previous_third, second.index());
            }
            if index.wrapping_add(1) == chain_len {
                self.set_twin(third, EMPTY);
            } else {
                let Some(&next) = faces.get(index.wrapping_add(1)) else {
                    return Err(crate::geometry::GeometryErrorKind::triangulation(
                        "outside insertion lost a next triangle",
                    ));
                };
                let next_second = HalfedgeId::in_face(next, FaceSide::Second);
                self.set_twin(third, next_second.index());
                self.set_twin(next_second, third.index());
            }
            self.set_incident(end, first.index());
            self.set_incident(start, second.index());
            self.set_incident(site, third.index());
            seeds.push(old_edge);
        }
        let Some(&first) = faces.first() else {
            return Err(crate::geometry::GeometryErrorKind::triangulation(
                "outside insertion lost its first triangle",
            ));
        };
        self.hull = new_hull;
        Ok((first, seeds))
    }

    pub(super) fn triangle(&self, face: FaceId) -> [usize; 3] {
        *self.face_slots(face)
    }

    fn take_slot(&mut self) -> FaceId {
        if let Some(face) = self.free.pop() {
            let face = FaceId::from_index(face);
            self.set_stamp(face, 0);
            return face;
        }
        // try_insert_with reserves all three buffers for every slot that
        // cannot be reused before it consumes the free list.
        let face = FaceId::from_index(self.triangle_count());
        self.triangles.extend([EMPTY; 3]);
        self.halfedges.extend([EMPTY; 3]);
        self.stamp.push(0);
        face
    }

    /// Grow the incidence table for sites appended after construction
    /// (refinement vertices).
    pub(super) fn admit_vertex(&mut self, site: usize) {
        let required = site.wrapping_add(1);
        if self.incident.len() < required {
            self.incident
                .reserve(required.wrapping_sub(self.incident.len()));
            self.incident.resize(required, EMPTY);
        }
    }

    /// Compact out the dead slots, remap twins, and flag cocircular edges
    /// with one exact pass over the finished mesh.
    pub(super) fn finish(self, xy: impl Fn(usize) -> XY) -> Result<ExactMesh> {
        let mut remap = vec![EMPTY; self.triangle_count()];
        let mut triangles = Vec::with_capacity(self.triangles.len());
        for (vertices, slot) in self.triangles.as_chunks::<3>().0.iter().zip(&mut remap) {
            let [first, second, third] = *vertices;
            if first != EMPTY {
                *slot = triangles.len() / 3;
                triangles.extend([first, second, third]);
            }
        }
        let mut halfedges = Vec::with_capacity(triangles.len());
        for (vertices, twins) in self
            .triangles
            .as_chunks::<3>()
            .0
            .iter()
            .zip(self.halfedges.as_chunks::<3>().0)
        {
            let [first, _, _] = *vertices;
            if first == EMPTY {
                continue;
            }
            for &twin in twins {
                halfedges.push(if twin == EMPTY {
                    EMPTY
                } else {
                    Self::remap_twin(&remap, HalfedgeId::from_index(twin))?
                });
            }
        }
        let mut cocircular = vec![false; halfedges.len()];
        for (face, (vertices, twins)) in triangles
            .as_chunks::<3>()
            .0
            .iter()
            .zip(halfedges.as_chunks::<3>().0)
            .enumerate()
        {
            let [a, b, c] = *vertices;
            let face = FaceId::from_index(face);
            for (side, &twin) in FaceSide::ALL.into_iter().zip(twins) {
                let edge = HalfedgeId::in_face(face, side);
                if twin == EMPTY || twin < edge.index() {
                    continue;
                }
                let opposite_edge = HalfedgeId::from_index(twin).prev();
                let opposite = Self::output_vertex(&triangles, opposite_edge);
                if exact::incircle(xy(a), xy(b), xy(c), xy(opposite))? == ExactSign::Zero {
                    Self::set_output_flag(&mut cocircular, edge, true);
                    Self::set_output_flag(&mut cocircular, HalfedgeId::from_index(twin), true);
                }
            }
        }
        Ok(ExactMesh {
            triangles,
            halfedges,
            hull: self.hull,
            cocircular,
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "a live twin in this mesh resolves to a face in the same compaction remap"
    )]
    const fn remapped_face(remap: &[usize], face: FaceId) -> usize {
        remap[face.index()]
    }

    fn remap_twin(remap: &[usize], twin: HalfedgeId) -> Result<usize> {
        Self::remapped_face(remap, twin.face())
            .checked_mul(3)
            .and_then(|base| base.checked_add(twin.side().offset()))
            .ok_or_else(|| crate::error::ResourceError::SizeOverflow.into())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "a remapped halfedge twin names a site in the same compact triangle table"
    )]
    const fn output_vertex(triangles: &[usize], edge: HalfedgeId) -> usize {
        triangles[edge.index()]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "output flags use the same admitted halfedge ID domain as the finished topology"
    )]
    const fn set_output_flag(flags: &mut [bool], edge: HalfedgeId, value: bool) {
        flags[edge.index()] = value;
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
        for (face, (vertices, twins)) in mesh
            .triangles
            .as_chunks::<3>()
            .0
            .iter()
            .zip(mesh.halfedges.as_chunks::<3>().0)
            .enumerate()
        {
            let [a, b, c] = *vertices;
            assert_eq!(
                exact::orient2d(xy(a), xy(b), xy(c)),
                ExactSign::Positive,
                "triangle {face} is counter-clockwise"
            );
            for (side, &twin) in FaceSide::ALL.into_iter().zip(twins) {
                if twin == EMPTY {
                    continue;
                }
                let edge = HalfedgeId::in_face(FaceId::from_index(face), side);
                let opposite = mesh.triangles[HalfedgeId::from_index(twin).prev().index()];
                let sign = exact::incircle(xy(a), xy(b), xy(c), xy(opposite)).unwrap();
                assert_ne!(
                    sign,
                    ExactSign::Positive,
                    "opposite vertex {opposite} inside circumcircle of {face}"
                );
                assert_eq!(
                    sign == ExactSign::Zero,
                    mesh.cocircular[edge.index()],
                    "cocircular flag at {}",
                    edge.index()
                );
            }
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
            let mesh = exact_delaunay(&pts).unwrap();
            assert_halfedges_consistent(&mesh);
            assert_delaunay(&mesh, &pts);
            // Random sites have no cocircular quads, so the Delaunay
            // triangulation is unique and must equal the current engine's.
            assert!(
                !mesh.cocircular.iter().any(|&f| f),
                "random sites should not be cocircular"
            );
            let current = super::super::delaunay_triangulation(&pts).unwrap();
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
            let mesh = exact_delaunay(&points).unwrap();
            assert_eq!(mesh.triangles.len() / 3, n - 2, "n={n} triangles");
            assert_eq!(mesh.hull.len(), n, "n={n} hull");
            assert_halfedges_consistent(&mesh);
            assert_delaunay(&mesh, &points);
        }
    }

    #[test]
    fn a_grid_is_delaunay_with_every_diagonal_flagged_cocircular() {
        let pts = grid(20);
        let mesh = exact_delaunay(&pts).unwrap();
        assert_halfedges_consistent(&mesh);
        assert_delaunay(&mesh, &pts);
        assert_eq!(
            mesh.hull.len(),
            76,
            "all grid boundary sites are on the hull"
        );
        assert_eq!(
            mesh.triangles.as_chunks::<3>().0.len(),
            722,
            "the 400-site grid with a 76-site hull has 722 faces"
        );
        let boundary_axis = [0.0_f64.to_bits(), 19.0_f64.to_bits()];
        let expected_hull: std::collections::HashSet<[u64; 2]> = pts
            .iter()
            .filter(|point| {
                boundary_axis.contains(&point.x.to_bits())
                    || boundary_axis.contains(&point.y.to_bits())
            })
            .map(|point| [point.x.to_bits(), point.y.to_bits()])
            .collect();
        let actual_hull: std::collections::HashSet<[u64; 2]> = mesh
            .hull
            .iter()
            .map(|&site| {
                let point = pts.get(site).expect("hull site is an input site");
                [point.x.to_bits(), point.y.to_bits()]
            })
            .collect();
        assert_eq!(
            actual_hull, expected_hull,
            "the hull retains every boundary site"
        );
        let center = Point::new_raw_xy(10.0, 10.0);
        for (&start, &end) in mesh.hull.iter().zip(mesh.hull.iter().cycle().skip(1)) {
            let start = pts.get(start).expect("hull site is an input site").xy();
            let end = pts.get(end).expect("hull site is an input site").xy();
            let dx = (start.x - end.x).abs().to_bits();
            let dy = (start.y - end.y).abs().to_bits();
            assert!(
                (dx == 1.0_f64.to_bits() && dy == 0.0_f64.to_bits())
                    || (dx == 0.0_f64.to_bits() && dy == 1.0_f64.to_bits()),
                "adjacent hull sites must be consecutive grid points"
            );
            assert_eq!(
                exact::orient2d(start, end, center.xy()),
                ExactSign::Positive,
                "the hull cycle is counter-clockwise"
            );
        }
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
        let mesh = exact_delaunay(&dup).unwrap();
        assert_halfedges_consistent(&mesh);
        assert_delaunay(&mesh, &dup);
        assert_eq!(mesh.hull, super::super::exact_corner_hull_cycle(&dup));
        // Five distinct sites, four on the hull: 2*5 - 4 - 2 = 4 triangles.
        assert_eq!(mesh.triangles.len() / 3, 4);
        let public = super::super::delaunay_triangulation(&dup).unwrap();
        assert_eq!(
            public.triangles.len() / 3,
            4,
            "the public entry point triangulates the distinct sites"
        );
    }

    #[test]
    fn collinear_input_matches_the_current_hull() {
        let line = points(&[(0.0, 0.0), (1.0, 1.0), (2.0, 2.0), (3.0, 3.0)]);
        let mesh = exact_delaunay(&line).unwrap();
        assert!(mesh.triangles.is_empty());
        assert_eq!(
            mesh.hull,
            super::super::delaunay_triangulation(&line).unwrap().hull
        );
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
        let mesh = exact_delaunay(&pts).unwrap();
        assert_halfedges_consistent(&mesh);
        assert_delaunay(&mesh, &pts);
        assert_eq!(mesh.triangles.len() / 3, 2 * 7 - 4 - 2);
    }

    #[test]
    fn cavity_stamp_rollover_keeps_unvisited_neighbor_reachable() {
        let pts = points(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (1.0, 0.5)]);
        let xy = |site: usize| pts[site].xy();
        let seed = || {
            let mut mesh = Mesh::seed(&[0, 1, 2, 3], pts.len());
            mesh.try_legalize(&xy).unwrap();
            mesh
        };
        let mut ordinary = seed();
        let mut wrapped = seed();
        wrapped.epoch = u32::MAX;
        wrapped.stamp.fill(1);
        let located = ordinary.locate(&xy, FaceId::from_index(0), xy(4));
        assert!(matches!(located, Located::Inside(_)));
        ordinary.try_insert_with(&xy, 4, located, None).unwrap();
        wrapped.try_insert_with(&xy, 4, located, None).unwrap();
        let ordinary = ordinary.finish(xy).unwrap();
        let wrapped = wrapped.finish(xy).unwrap();
        assert_delaunay(&ordinary, &pts);
        assert_delaunay(&wrapped, &pts);
        assert_eq!(
            triangle_set(&ordinary.triangles),
            triangle_set(&wrapped.triangles)
        );
        assert_eq!(ordinary.cocircular, wrapped.cocircular);
    }
}
