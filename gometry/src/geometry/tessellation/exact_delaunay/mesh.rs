//! Mutable halfedge mesh construction, location, insertion, and finalization.

use super::{
    EMPTY, ExactMesh, FaceId, FaceSide, HalfedgeId, Located, LocationOutcome, Mesh,
    MeshCoordinates, MeshHull, OutsideInsertion, canonical_incircle_for,
};
use crate::error::Result;
use crate::geometry::XY;
use crate::geometry::numerics::product::ExactSign;

pub(super) fn bounds<C: MeshCoordinates>(coordinates: &C, site_count: usize) -> (XY, XY) {
    let mut min = XY::new(f64::INFINITY, f64::INFINITY);
    let mut max = XY::new(f64::NEG_INFINITY, f64::NEG_INFINITY);
    for site in 0..site_count {
        let p = coordinates.locality_xy(site);
        min = XY::new(min.x.min(p.x), min.y.min(p.y));
        max = XY::new(max.x.max(p.x), max.y.max(p.y));
    }
    (min, max)
}

/// A 16-bit-per-axis Hilbert index over the bounds: a locality order for the
/// insertions, never a geometric decision.
pub(super) fn hilbert16(p: XY, min: XY, max: XY) -> u32 {
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

#[cfg(test)]
thread_local! {
    pub(super) static PERIMETER_STEPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl MeshHull {
    fn new(vertices: &[usize], vertex_count: usize) -> Self {
        let mut links = vec![[EMPTY; 2]; vertex_count];
        for (&start, &end) in vertices.iter().zip(vertices.iter().cycle().skip(1)) {
            links
                .get_mut(start)
                .expect("hull vertices belong to the mesh")[1] = end;
            links
                .get_mut(end)
                .expect("hull vertices belong to the mesh")[0] = start;
        }
        Self {
            first: vertices.first().copied().unwrap_or(EMPTY),
            links,
            len: vertices.len(),
        }
    }

    const fn len(&self) -> usize {
        self.len
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &usize> + Clone {
        std::iter::successors((self.len != 0).then_some(&self.first), |&&site| {
            self.links.get(site).map(|link| &link[1])
        })
        .take(self.len)
    }

    fn split(&mut self, start: usize, site: usize) {
        let end = self.next(start);
        self.replace_chain(start, end, site, 1);
    }

    fn next(&self, site: usize) -> usize {
        self.links.get(site).expect("the site belongs to the hull")[1]
    }

    pub(super) fn previous(&self, site: usize) -> usize {
        self.links.get(site).expect("the site belongs to the hull")[0]
    }

    fn replace_chain(&mut self, start: usize, end: usize, site: usize, edges: usize) {
        let required = site.wrapping_add(1);
        self.links
            .resize(self.links.len().max(required), [EMPTY; 2]);
        let mut at = self.next(start);
        while at != end {
            let next = self.next(at);
            *self
                .links
                .get_mut(at)
                .expect("the removed site belongs to the hull") = [EMPTY; 2];
            at = next;
        }
        *self
            .links
            .get_mut(site)
            .expect("the new hull site is admitted") = [start, end];
        self.links
            .get_mut(start)
            .expect("the chain starts on the hull")[1] = site;
        self.links.get_mut(end).expect("the chain ends on the hull")[0] = site;
        self.len = self.len.wrapping_sub(edges).wrapping_add(2);
    }

    fn expand(&mut self, start: usize, end: usize, site: usize, edges: usize) {
        self.replace_chain(start, end, site, edges);
        self.first = start;
    }

    fn into_vertices(self) -> Vec<usize> {
        self.iter().copied().collect()
    }
}

#[cfg(test)]
impl PartialEq<Vec<usize>> for MeshHull {
    fn eq(&self, other: &Vec<usize>) -> bool {
        self.iter().eq(other.iter())
    }
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
    pub(in crate::geometry::tessellation::delaunay) fn edge_start(
        &self,
        edge: HalfedgeId,
    ) -> usize {
        self.triangles[edge.index()]
    }

    pub(in crate::geometry::tessellation::delaunay) fn edge_end(&self, edge: HalfedgeId) -> usize {
        self.edge_start(edge.next())
    }

    pub(in crate::geometry::tessellation::delaunay) fn edge_apex(&self, edge: HalfedgeId) -> usize {
        self.edge_start(edge.prev())
    }

    pub(in crate::geometry::tessellation::delaunay) fn edge_vertices(
        &self,
        edge: HalfedgeId,
    ) -> (usize, usize) {
        (self.edge_start(edge), self.edge_end(edge))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "HalfedgeId identifies an admitted stable flat slot owned by this mesh"
    )]
    pub(in crate::geometry::tessellation::delaunay) fn twin(&self, edge: HalfedgeId) -> usize {
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
    pub(in crate::geometry::tessellation::delaunay) const fn side_value<T: Copy>(
        side: &[T],
        edge: HalfedgeId,
    ) -> T {
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

    pub(in crate::geometry::tessellation::delaunay) const fn edge_count(&self) -> usize {
        self.halfedges.len()
    }

    pub(in crate::geometry::tessellation::delaunay) fn edge_ids(
        &self,
    ) -> impl Iterator<Item = HalfedgeId> + '_ {
        (0..self.halfedges.len()).map(HalfedgeId::from_index)
    }

    pub(in crate::geometry::tessellation::delaunay) fn face_ids(
        &self,
    ) -> impl Iterator<Item = FaceId> + '_ {
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

    pub(super) fn seed(hull: &[usize], vertex_count: usize) -> Self {
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
            hull: MeshHull::new(hull, vertex_count),
            incident,
            free: Vec::new(),
            stamp: vec![0; count],
            epoch: 0,
        }
    }

    pub(in crate::geometry::tessellation::delaunay) const fn triangle_count(&self) -> usize {
        self.triangles.len() / 3
    }

    pub(in crate::geometry::tessellation::delaunay) fn alive(&self, face: FaceId) -> bool {
        let [first, _, _] = *self.face_slots(face);
        first != EMPTY
    }

    /// Every halfedge slot leaving `u`, walking its fan both ways so a hull
    /// vertex's open fan is covered too.
    pub(in crate::geometry::tessellation::delaunay) fn outgoing(
        &self,
        u: usize,
    ) -> Vec<HalfedgeId> {
        let mut slots = Vec::new();
        let _ = self.find_outgoing(u, |edge| {
            slots.push(edge);
            false
        });
        slots
    }

    fn find_outgoing(
        &self,
        u: usize,
        mut matches: impl FnMut(HalfedgeId) -> bool,
    ) -> Option<HalfedgeId> {
        let start = self.incident(u);
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
                        let edge = HalfedgeId::in_face(face, side);
                        if matches(edge) {
                            return Some(edge);
                        }
                    }
                }
            }
            return None;
        }
        // Counter-clockwise: across the edge into `u`.
        let mut e = start;
        loop {
            if matches(e) {
                return Some(e);
            }
            let twin = self.twin(e.prev());
            if twin == EMPTY {
                break;
            }
            e = HalfedgeId::from_index(twin);
            if e == start {
                return None;
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
            if matches(e) {
                return Some(e);
            }
        }
        None
    }

    /// The slot of the edge between `u` and `v` in either direction, if it
    /// is an edge; a hull edge exists as one halfedge only.
    pub(in crate::geometry::tessellation::delaunay) fn edge_slot(
        &self,
        u: usize,
        v: usize,
    ) -> Option<HalfedgeId> {
        if let Some(slot) = self.directed_slot(u, v) {
            return Some(slot);
        }
        self.directed_slot(v, u)
    }

    pub(super) fn directed_slot(&self, u: usize, v: usize) -> Option<HalfedgeId> {
        self.find_outgoing(u, |edge| self.edge_end(edge) == v)
    }

    /// Lawson legalization of every interior edge: flip while the twin's apex
    /// lies strictly inside the circumcircle. The seed fan triangulates the
    /// hull without being Delaunay, and cavity insertion preserves the
    /// Delaunay property only from a Delaunay start. A slot left on the stack
    /// by a flip names whatever edge now lives there; re-checking a legal
    /// edge is harmless, and every edge a flip disturbed is pushed again.
    pub(super) fn try_legalize<C: MeshCoordinates>(&mut self, coordinates: &C) -> Result<()> {
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
            if canonical_incircle_for(coordinates, [
                coordinates.query(a),
                coordinates.query(b),
                coordinates.query(c),
                coordinates.query(d),
            ])? != ExactSign::Positive
            {
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

    pub(in crate::geometry::tessellation::delaunay) fn try_legalize_constrained_from<
        C: MeshCoordinates,
    >(
        &mut self,
        coordinates: &C,
        constrained: &mut [bool],
        mut stack: Vec<HalfedgeId>,
    ) -> Result<Vec<FaceId>> {
        let mut changed = Vec::new();
        while let Some(edge) = stack.pop() {
            let twin = self.twin(edge);
            if twin == EMPTY || Self::side_value(constrained, edge) {
                continue;
            }
            let [a, b, c] = self.triangle(edge.face());
            let opposite = self.edge_apex(HalfedgeId::from_index(twin));
            if canonical_incircle_for(coordinates, [
                coordinates.query(a),
                coordinates.query(b),
                coordinates.query(c),
                coordinates.query(opposite),
            ])? != ExactSign::Positive
            {
                continue;
            }
            let diagonal = self.flip_carrying(edge, constrained, false);
            changed.push(diagonal.face());
            let twin = self.twin(diagonal);
            if twin == EMPTY {
                return Err(crate::geometry::GeometryErrorKind::triangulation(
                    "constrained legalization lost an interior diagonal",
                ));
            }
            let twin = HalfedgeId::from_index(twin);
            changed.push(twin.face());
            for edge in [diagonal.next(), diagonal.prev(), twin.next(), twin.prev()] {
                stack.push(edge);
            }
        }
        Ok(changed)
    }

    /// Flip the edge at slot `e` (`a -> b`, apex `c`; twin apex `d`) to
    /// `c -> d`, returning the slot of `c -> d`. The caller has checked the
    /// quad is convex.
    pub(in crate::geometry::tessellation::delaunay) fn flip(
        &mut self,
        e: HalfedgeId,
    ) -> HalfedgeId {
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
    pub(in crate::geometry::tessellation::delaunay) fn flip_carrying<T: Copy>(
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
    pub(in crate::geometry::tessellation::delaunay) fn locate<C: MeshCoordinates>(
        &self,
        coordinates: &C,
        start: FaceId,
        point: C::Query,
    ) -> Result<Located> {
        match self.locate_bounded(coordinates, start, point, &[])? {
            LocationOutcome::Located(located) => Ok(located),
            LocationOutcome::Blocked(_) => {
                unreachable!("an admitted Delaunay site lies in its closed seed hull")
            },
        }
    }

    /// The orientation walk that never crosses a hull edge or a slot `stop`
    /// mark returns `Blocked(e)` for the edge it would have had to cross.
    /// Arithmetic errors stay in the outer Result. A constrained mesh is not
    /// Delaunay, where the walk can cycle, so a walk longer than the mesh
    /// settles by scanning every live face.
    pub(in crate::geometry::tessellation::delaunay) fn locate_bounded<C: MeshCoordinates>(
        &self,
        coordinates: &C,
        start: FaceId,
        point: C::Query,
        stop: &[bool],
    ) -> Result<LocationOutcome> {
        let mut t = if self.alive(start) {
            start
        } else {
            self.any_alive()
        };
        let mut steps = 0_usize;
        loop {
            let e = match self.classify(coordinates, t, point)? {
                LocationOutcome::Located(located) => {
                    return Ok(LocationOutcome::Located(located));
                },
                LocationOutcome::Blocked(edge) => edge,
            };
            let twin = self.twin(e);
            if twin == EMPTY || stop.get(e.index()).is_some_and(|&stopped| stopped) {
                return Ok(LocationOutcome::Blocked(e));
            }
            steps = steps.wrapping_add(1);
            if steps > self.triangle_count() {
                for face in (0..self.triangle_count())
                    .map(FaceId::from_index)
                    .filter(|&face| self.alive(face))
                {
                    if let LocationOutcome::Located(located) =
                        self.classify(coordinates, face, point)?
                    {
                        return Ok(LocationOutcome::Located(located));
                    }
                }
                return Ok(LocationOutcome::Blocked(e));
            }
            t = HalfedgeId::from_index(twin).face();
        }
    }

    pub(in crate::geometry::tessellation::delaunay) fn outside_edge<C: MeshCoordinates>(
        &self,
        coordinates: &C,
        point: C::Query,
    ) -> Result<Option<HalfedgeId>> {
        let hull_len = self.hull.len();
        if hull_len < 3 {
            return Ok(None);
        }
        for (&start, &end) in self.hull.iter().zip(self.hull.iter().cycle().skip(1)) {
            if coordinates.orient([coordinates.query(start), coordinates.query(end), point])?
                == ExactSign::Negative
                && let Some(edge) = self.directed_slot(start, end)
            {
                return Ok(Some(edge));
            }
        }
        Ok(None)
    }

    /// Where the query stands relative to live triangle `t`, or the first edge
    /// it is strictly to the right of.
    pub(in crate::geometry::tessellation::delaunay) fn classify<C: MeshCoordinates>(
        &self,
        coordinates: &C,
        t: FaceId,
        point: C::Query,
    ) -> Result<LocationOutcome> {
        let mut collinear_count = 0_usize;
        let mut collinear_edge = FaceSide::First;
        for side in FaceSide::ALL {
            let edge = HalfedgeId::in_face(t, side);
            match coordinates.orient([
                coordinates.query(self.edge_start(edge)),
                coordinates.query(self.edge_end(edge)),
                point,
            ])? {
                ExactSign::Negative => return Ok(LocationOutcome::Blocked(edge)),
                ExactSign::Zero => {
                    collinear_count = collinear_count.wrapping_add(1);
                    collinear_edge = side;
                },
                ExactSign::Positive => {},
            }
        }
        Ok(LocationOutcome::Located(match collinear_count {
            0 => Located::Inside(t),
            1 => {
                let edge = HalfedgeId::in_face(t, collinear_edge);
                let (a, b) = (self.edge_start(edge), self.edge_end(edge));
                if coordinates.compare(point, coordinates.query(a)).is_eq()
                    || coordinates.compare(point, coordinates.query(b)).is_eq()
                {
                    Located::OnVertex
                } else {
                    Located::OnEdge(edge)
                }
            },
            _ => Located::OnVertex,
        }))
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
    #[expect(
        clippy::panic_in_result_fn,
        reason = "a broken admitted cavity is a topology invariant failure, not recoverable input"
    )]
    pub(in crate::geometry::tessellation::delaunay) fn try_insert_with<C: MeshCoordinates, R>(
        &mut self,
        coordinates: &C,
        site: usize,
        located: Located,
        mut skip: Option<&mut Vec<bool>>,
        finish_changes: impl FnOnce(Vec<FaceId>, &[FaceId]) -> R,
    ) -> Result<(FaceId, R)> {
        // A site on a hull edge splits that edge: the cavity boundary is then
        // an open chain from the edge's end to its start, the site joins the
        // hull between them, and the two chain ends become hull edges.
        let (split_hull, seeds) = match located {
            Located::OnVertex => {
                return Ok((self.any_alive(), finish_changes(Vec::new(), &[])));
            },
            Located::Inside(t) => (None, vec![t]),
            Located::Outside(_) => {
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
        let point = coordinates.query(site);
        // The cavity starts with the seeds and crosses an edge only when its
        // far triangle's canonical incircle sign is positive. A geometric zero
        // uses the symbolic lift to select the diagonal without changing the
        // exact cocircular flag retained for the Voronoi dual.
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.stamp.fill(0);
            self.epoch = 1;
        }
        let epoch = self.epoch;
        let mut cavity = Vec::new();
        let mut stack = seeds;
        for &face in &stack {
            self.set_stamp(face, epoch);
        }
        while let Some(face) = stack.pop() {
            cavity.push(face);
            for side in FaceSide::ALL {
                let edge = HalfedgeId::in_face(face, side);
                let twin = self.twin(edge);
                if twin == EMPTY
                    || skip
                        .as_deref()
                        .is_some_and(|skip| Self::side_value(skip, edge))
                {
                    continue;
                }
                let neighbor = HalfedgeId::from_index(twin).face();
                if self.stamp(neighbor) == epoch {
                    continue;
                }
                let [a, b, c] = self.triangle(neighbor);
                if canonical_incircle_for(coordinates, [
                    coordinates.query(a),
                    coordinates.query(b),
                    coordinates.query(c),
                    point,
                ])? == ExactSign::Positive
                {
                    self.set_stamp(neighbor, epoch);
                    stack.push(neighbor);
                }
            }
        }
        let excluded = match located {
            Located::OnEdge(edge) if split_hull.is_some() => Some(edge),
            Located::Inside(_) | Located::Outside(_) | Located::OnVertex | Located::OnEdge(_) => {
                None
            },
        };
        let first = excluded.map_or_else(
            || {
                cavity
                    .iter()
                    .flat_map(|&face| FaceSide::ALL.map(|side| HalfedgeId::in_face(face, side)))
                    .find(|&edge| {
                        let twin = self.twin(edge);
                        twin == EMPTY || self.stamp(HalfedgeId::from_index(twin).face()) != epoch
                    })
                    .expect("a cavity has a boundary")
            },
            HalfedgeId::next,
        );
        let end = excluded.unwrap_or(first);
        let budget = cavity.len().wrapping_mul(3);
        let mut ordered = Vec::with_capacity(budget);
        let mut edge = first;
        #[cfg(test)]
        let mut visited = crate::collections::try_hash_set()?;
        // Twin crossing and face-next permute the admitted cavity halfedges.
        // Stop at the excluded hull edge, or close the interior perimeter.
        for _ in 0..budget {
            #[cfg(test)]
            assert!(
                visited.insert(edge.index()),
                "fan revisited a directed edge"
            );
            let twin = self.twin(edge);
            if twin != EMPTY && self.stamp(HalfedgeId::from_index(twin).face()) == epoch {
                edge = HalfedgeId::from_index(twin).next();
            } else {
                let (a, b) = self.edge_vertices(edge);
                let constrained = skip
                    .as_deref()
                    .is_some_and(|skip| Self::side_value(skip, edge));
                ordered.push((a, b, twin, constrained));
                edge = edge.next();
            }
            if edge == end {
                break;
            }
        }
        assert_eq!(edge, end, "a cavity perimeter ends within its halfedges");
        #[cfg(test)]
        PERIMETER_STEPS.with(|steps| steps.set(visited.len()));
        #[cfg(test)]
        self.check_perimeter_order(
            &cavity,
            split_hull,
            skip.as_deref().map(Vec::as_slice),
            &ordered,
        );
        let free_after_cavity = self.free.len().wrapping_add(cavity.len());
        let new_slots = ordered.len().saturating_sub(free_after_cavity);
        let new_triangles = new_slots.wrapping_mul(3);
        self.free.reserve(cavity.len());
        self.triangles.reserve(new_triangles);
        self.halfedges.reserve(new_triangles);
        self.stamp.reserve(new_slots);
        let mut slots = Vec::with_capacity(ordered.len());
        for &face in &cavity {
            self.set_face_vertex(face, FaceSide::First, EMPTY);
            if let Some(skip) = skip.as_deref_mut() {
                Self::set_side_face(skip, face, [false; 3]);
            }
            self.free.push(face.index());
        }
        // New triangles (a, b, site): slot 0 = a->b twins the outside, slot 1 =
        // b->site twins slot 2 of the next, slot 2 = site->a twins slot 1 of
        // the previous.
        for _ in 0..ordered.len() {
            slots.push(self.take_slot());
        }
        if let Some(skip) = skip.as_deref_mut() {
            skip.resize(self.edge_count(), false);
        }
        let changed = finish_changes(cavity, &slots);
        let first = *slots.first().expect("a cavity has a boundary");
        let mut slots = slots.into_iter().peekable();
        let mut last = first;
        for (a, b, twin, constrained) in ordered.iter().copied() {
            let face = slots.next().expect("each boundary edge has one face slot");
            let next = slots.peek().copied();
            self.set_face_vertex(face, FaceSide::First, a);
            self.set_face_vertex(face, FaceSide::Second, b);
            self.set_face_vertex(face, FaceSide::Third, site);
            let first_edge = HalfedgeId::in_face(face, FaceSide::First);
            let second_edge = HalfedgeId::in_face(face, FaceSide::Second);
            let third_edge = HalfedgeId::in_face(face, FaceSide::Third);
            if let Some(skip) = skip.as_deref_mut() {
                Self::set_side_face(skip, face, [constrained, false, false]);
            }
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
            self.set_incident(b, second_edge.index());
            self.set_incident(site, third_edge.index());
            last = face;
        }
        if split_hull.is_some() {
            self.set_twin(HalfedgeId::in_face(first, FaceSide::Third), EMPTY);
        }
        if let Some((a, _)) = split_hull {
            self.hull.split(a, site);
        }
        Ok((last, changed))
    }

    pub(in crate::geometry::tessellation::delaunay) fn insert_constrained<C: MeshCoordinates>(
        &mut self,
        coordinates: &C,
        site: usize,
        located: Located,
        skip: &mut Vec<bool>,
    ) -> Result<Vec<FaceId>> {
        self.try_insert_with(
            coordinates,
            site,
            located,
            Some(skip),
            |mut cavity, slots| {
                cavity.extend_from_slice(slots);
                cavity
            },
        )
        .map(|(_, changed)| changed)
    }

    #[cfg(test)]
    fn check_perimeter_order(
        &self,
        cavity: &[FaceId],
        split_hull: Option<(usize, usize)>,
        skip: Option<&[bool]>,
        actual: &[(usize, usize, usize, bool)],
    ) {
        let mut boundary = Vec::new();
        for &face in cavity {
            for side in FaceSide::ALL {
                let edge = HalfedgeId::in_face(face, side);
                let twin = self.twin(edge);
                let ends = self.edge_vertices(edge);
                if split_hull == Some(ends) {
                    continue;
                }
                if twin == EMPTY || self.stamp(HalfedgeId::from_index(twin).face()) != self.epoch {
                    boundary.push((
                        ends.0,
                        ends.1,
                        twin,
                        skip.is_some_and(|skip| Self::side_value(skip, edge)),
                    ));
                }
            }
        }
        let start = split_hull.map_or_else(
            || boundary.first().expect("a cavity has a boundary").0,
            |(_, b)| b,
        );
        let mut expected = Vec::with_capacity(boundary.len());
        let mut at = start;
        if boundary.len() <= 16 {
            for _ in 0..boundary.len() {
                let (index, &(a, b, twin, constrained)) = boundary
                    .iter()
                    .enumerate()
                    .find(|(_, entry)| entry.0 == at)
                    .expect("a cavity boundary is one chain");
                expected.push((a, b, twin, constrained));
                boundary.swap_remove(index);
                at = b;
            }
        } else {
            boundary.sort_unstable_by_key(|&(a, ..)| a);
            for _ in 0..boundary.len() {
                let index = boundary
                    .binary_search_by_key(&at, |&(a, ..)| a)
                    .expect("a cavity boundary is one chain");
                let &(a, b, twin, constrained) = boundary
                    .get(index)
                    .expect("the boundary lookup returns its own index");
                expected.push((a, b, twin, constrained));
                at = b;
            }
        }
        assert_eq!(actual, expected, "fan changed the ordered boundary tuples");
    }

    pub(in crate::geometry::tessellation::delaunay) fn insert_outside<C: MeshCoordinates>(
        &mut self,
        coordinates: &C,
        site: usize,
        visible_edge: HalfedgeId,
        constrained: &mut Vec<bool>,
    ) -> Result<OutsideInsertion> {
        let hull_len = self.hull.len();
        if hull_len < 3 {
            return Err(crate::geometry::GeometryErrorKind::triangulation(
                "outside insertion needs a two-dimensional hull",
            ));
        }
        let point = coordinates.query(site);
        let (mut start, end) = self.edge_vertices(visible_edge);
        if self.twin(visible_edge) != EMPTY
            || coordinates.orient([coordinates.query(start), coordinates.query(end), point])?
                != ExactSign::Negative
        {
            return Err(crate::geometry::GeometryErrorKind::triangulation(
                "outside insertion needs a visible hull edge",
            ));
        }
        let first_visible = start;
        loop {
            let previous = self.hull.previous(start);
            if coordinates.orient([coordinates.query(previous), coordinates.query(start), point])?
                != ExactSign::Negative
            {
                break;
            }
            start = previous;
            if start == first_visible {
                return Err(crate::geometry::GeometryErrorKind::triangulation(
                    "outside insertion has no visible hull chain",
                ));
            }
        }
        let mut visible_edges = Vec::new();
        let mut at = start;
        loop {
            let end = self.hull.next(at);
            if coordinates.orient([coordinates.query(at), coordinates.query(end), point])?
                != ExactSign::Negative
            {
                break;
            }
            let Some(edge) = self.directed_slot(at, end) else {
                return Err(crate::geometry::GeometryErrorKind::triangulation(
                    "outside insertion could not find a hull edge",
                ));
            };
            visible_edges.push((at, end, edge));
            at = end;
            if at == start {
                return Err(crate::geometry::GeometryErrorKind::triangulation(
                    "outside insertion sees the entire hull",
                ));
            }
        }
        let chain_len = visible_edges.len();
        if chain_len == 0 {
            return Err(crate::geometry::GeometryErrorKind::triangulation(
                "outside insertion has no visible hull edge",
            ));
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
        constrained.resize(self.edge_count(), false);
        let mut changed = faces.clone();
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
            let base_constrained = Self::side_value(constrained, old_edge);
            Self::set_side_face(constrained, face, [base_constrained, false, false]);
            changed.push(old_edge.face());
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
        self.hull.expand(start, at, site, chain_len);
        Ok(OutsideInsertion {
            changed,
            legalization: seeds,
        })
    }

    pub(in crate::geometry::tessellation::delaunay) fn triangle(&self, face: FaceId) -> [usize; 3] {
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
    pub(in crate::geometry::tessellation::delaunay) fn admit_vertex(&mut self, site: usize) {
        let required = site.wrapping_add(1);
        if self.incident.len() < required {
            self.incident
                .reserve(required.wrapping_sub(self.incident.len()));
            self.incident.resize(required, EMPTY);
        }
    }

    /// Compact out the dead slots, remap twins, and flag cocircular edges
    /// with one exact pass over the finished mesh.
    pub(in crate::geometry::tessellation::delaunay) fn finish<C: MeshCoordinates>(
        self,
        coordinates: &C,
    ) -> Result<ExactMesh> {
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
                if coordinates.incircle([
                    coordinates.query(a),
                    coordinates.query(b),
                    coordinates.query(c),
                    coordinates.query(opposite),
                ])? == ExactSign::Zero
                {
                    Self::set_output_flag(&mut cocircular, edge, true);
                    Self::set_output_flag(&mut cocircular, HalfedgeId::from_index(twin), true);
                }
            }
        }
        Ok(ExactMesh {
            triangles,
            halfedges,
            hull: self.hull.into_vertices(),
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
