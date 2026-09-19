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
//! instead. A constrained split evaluates the original source segment at an
//! exact parameter and rejects a rounded point that is not on that segment. A
//! center outside binary64 uses the explicit longest-edge midpoint
//! construction; resource and invariant failures remain errors. The vertex
//! budget bounds the loop, and a mesh the budget could not finish is rejected
//! rather than returned as if it met the request.

#![expect(
    clippy::many_single_char_names,
    reason = "a, b, c, d name the vertices of a quad and e, f, t, u its slots, the geometry kernel convention"
)]

use std::collections::VecDeque;

use super::exact_delaunay::{EMPTY, Located, Mesh, next_halfedge, prev_halfedge};
use crate::collections::{HashMap, HashSet};
use crate::error::{ErrorKind, Result};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::tessellation::exact;
use crate::geometry::{GeometryErrorKind, Point, XY, open_xy_cycle_magnitude};

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct RefinementWorkCounters {
    pub(super) inside_label_passes: usize,
    pub(super) bad_face_scans: usize,
    pub(super) constraint_reconstruction_passes: usize,
    pub(super) flag_remark_passes: usize,
}

#[cfg(test)]
thread_local! {
    static REFINEMENT_WORK: std::cell::Cell<RefinementWorkCounters> =
        const { std::cell::Cell::new(RefinementWorkCounters {
            inside_label_passes: 0,
            bad_face_scans: 0,
            constraint_reconstruction_passes: 0,
            flag_remark_passes: 0,
        }) };
}

#[cfg(test)]
fn update_work(update: impl FnOnce(&mut RefinementWorkCounters)) {
    REFINEMENT_WORK.with(|cell| {
        let mut counters = cell.get();
        update(&mut counters);
        cell.set(counters);
    });
}

#[cfg(test)]
pub(super) fn take_refinement_work_counters() -> RefinementWorkCounters {
    REFINEMENT_WORK.with(|cell| cell.replace(RefinementWorkCounters::default()))
}

#[derive(Clone, Debug)]
pub(super) struct SourceConstraint {
    pub(super) id: usize,
    pub(super) start: Point,
    pub(super) end: Point,
    pub(super) start_site: usize,
    pub(super) end_site: usize,
}

#[derive(Debug)]
pub(super) struct SourcePiece {
    pub(super) source: usize,
    pub(super) start_site: usize,
    pub(super) end_site: usize,
    pub(super) lo: exact::ExactRational,
    pub(super) hi: exact::ExactRational,
}

#[derive(Debug)]
pub(super) enum NodeProvenance {
    SourceSite {
        occurrence: usize,
    },
    ConstraintSplit {
        source: usize,
        parameter: exact::ExactRational,
    },
    Generated,
}

/// One constrained mesh and its checked publication inputs.
///
/// The source points, faces, source intervals, and generated-node provenance
/// are retained together so scalar and packed publishers cannot independently
/// reconstruct a different mesh result.
#[derive(Debug)]
pub(super) struct ConstrainedMeshPlan {
    pub(super) vertices: Vec<Point>,
    pub(super) faces: Vec<[usize; 3]>,
    pub(super) provenance: Vec<NodeProvenance>,
    pub(super) source_constraints: Vec<SourceConstraint>,
    pub(super) constraint_pieces: Vec<SourcePiece>,
    pub(super) refinement: super::CdtRefinement,
    pub(super) force_xy: bool,
}

impl ConstrainedMeshPlan {
    /// Consume the retained source certificate immediately before publication.
    ///
    /// The topology builder performs the same checks while the live mesh is
    /// available. Rechecking the detached product prevents a future publisher
    /// from silently dropping source intervals or selected-site provenance.
    pub(super) fn validate_certificate(&self) -> Result<()> {
        if self.provenance.len() != self.vertices.len() {
            return Err(GeometryErrorKind::triangulation(
                "constrained mesh certificate has inconsistent node or edge state",
            ));
        }
        let source_occurrence_count = self
            .provenance
            .iter()
            .filter_map(|provenance| match provenance {
                NodeProvenance::SourceSite { occurrence } => occurrence.checked_add(1),
                NodeProvenance::ConstraintSplit { .. } | NodeProvenance::Generated => None,
            })
            .max()
            .unwrap_or(0);
        let zero = exact::ExactRational::zero();
        let one = exact::ExactRational::one();
        for provenance in &self.provenance {
            match provenance {
                NodeProvenance::SourceSite { occurrence } => {
                    if *occurrence >= source_occurrence_count {
                        return Err(GeometryErrorKind::triangulation(
                            "constrained mesh certificate has an invalid source occurrence",
                        ));
                    }
                },
                NodeProvenance::ConstraintSplit { source, parameter } => {
                    if *source >= self.source_constraints.len()
                        || parameter.try_cmp(&zero)? != std::cmp::Ordering::Greater
                        || parameter.try_cmp(&one)? != std::cmp::Ordering::Less
                    {
                        return Err(GeometryErrorKind::triangulation(
                            "constrained mesh certificate has an invalid split provenance",
                        ));
                    }
                },
                NodeProvenance::Generated => {},
            }
        }
        for (id, source) in self.source_constraints.iter().enumerate() {
            if source.id != id
                || source.start_site >= self.vertices.len()
                || source.end_site >= self.vertices.len()
                || source.start_site == source.end_site
                || !source.start.xy().x.is_finite()
                || !source.start.xy().y.is_finite()
                || !source.end.xy().x.is_finite()
                || !source.end.xy().y.is_finite()
                || self.vertices[source.start_site].xy() != source.start.xy()
                || self.vertices[source.end_site].xy() != source.end.xy()
            {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate has an invalid source site",
                ));
            }
        }
        let mut source_id = 0;
        let mut cursor = exact::ExactRational::zero();
        for piece in &self.constraint_pieces {
            if self.source_constraints.get(piece.source).is_none()
                || self.vertices.get(piece.start_site).is_none()
                || self.vertices.get(piece.end_site).is_none()
                || piece.start_site == piece.end_site
                || piece.lo.try_cmp(&piece.hi)? != std::cmp::Ordering::Less
            {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate has an invalid source piece",
                ));
            }
            if piece.source != source_id {
                if piece.source != source_id + 1
                    || cursor.try_cmp(&one)? != std::cmp::Ordering::Equal
                {
                    return Err(GeometryErrorKind::triangulation(
                        "constrained mesh certificate has a discontinuous source chain",
                    ));
                }
                source_id = piece.source;
                cursor = exact::ExactRational::zero();
            }
            if piece.lo.try_cmp(&cursor)? != std::cmp::Ordering::Equal {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate has an unordered source chain",
                ));
            }
            cursor = piece.hi.try_clone()?;
        }
        if source_id + usize::from(!self.constraint_pieces.is_empty())
            != self.source_constraints.len()
            || (!self.constraint_pieces.is_empty()
                && cursor.try_cmp(&one)? != std::cmp::Ordering::Equal)
        {
            return Err(GeometryErrorKind::triangulation(
                "constrained mesh certificate does not cover all sources",
            ));
        }
        Ok(())
    }
}

struct PreparedSplit {
    candidate: XY,
    located: Located,
    source: Option<(SourcePiece, exact::ExactRational)>,
}

/// The mesh with its constraint flags, one per halfedge slot.
pub(super) struct ConstrainedMesh {
    pub(super) mesh: Mesh,
    pub(super) constrained: Vec<bool>,
    source_constraints: Vec<SourceConstraint>,
    source_pieces: HashMap<(usize, usize), SourcePiece>,
    provenance: Vec<NodeProvenance>,
}

impl ConstrainedMesh {
    /// Delaunay-triangulate `points`, then force every `edges` pair in.
    #[cfg(test)]
    pub(super) fn build(points: &[Point], edges: &[[usize; 2]]) -> Option<Self> {
        let sources: Vec<_> = edges
            .iter()
            .enumerate()
            .map(|(id, &[start_site, end_site])| SourceConstraint {
                id,
                start: points[start_site],
                end: points[end_site],
                start_site,
                end_site,
            })
            .collect();
        Self::build_with_sources(points, &(0..points.len()).collect::<Vec<_>>(), &sources)
            .ok()
            .flatten()
    }

    pub(super) fn build_with_sources(
        points: &[Point],
        occurrences: &[usize],
        sources: &[SourceConstraint],
    ) -> Result<Option<Self>> {
        let Some(mesh) = Mesh::try_delaunay(points)? else {
            return Ok(None);
        };
        let mut constrained = Vec::new();
        crate::try_reserve_vec(&mut constrained, mesh.halfedges.len())?;
        constrained.resize(mesh.halfedges.len(), false);
        let mut source_constraints = Vec::new();
        crate::try_reserve_vec(&mut source_constraints, sources.len())?;
        source_constraints.extend_from_slice(sources);
        let source_pieces = crate::collections::try_hash_map_with_capacity(sources.len())?;
        let mut provenance = Vec::new();
        crate::try_reserve_vec(&mut provenance, occurrences.len())?;
        for &occurrence in occurrences {
            crate::try_push_vec(&mut provenance, NodeProvenance::SourceSite { occurrence })?;
        }
        let mut this = Self {
            mesh,
            constrained,
            source_constraints,
            source_pieces,
            provenance,
        };
        let xy = |site: usize| points[site].xy();
        for source in sources {
            this.insert_constraint(
                &xy,
                source.start_site,
                source.end_site,
                source.id,
                exact::ExactRational::zero(),
                exact::ExactRational::one(),
            )?;
        }
        Ok(Some(this))
    }

    /// Force the segment `u -> v` to be an edge, splitting it at any vertex it
    /// passes through exactly, and flipping every edge it crosses.
    fn insert_constraint(
        &mut self,
        xy: &impl Fn(usize) -> XY,
        u: usize,
        v: usize,
        source: usize,
        lo: exact::ExactRational,
        hi: exact::ExactRational,
    ) -> Result<()> {
        if u == v {
            return Ok(());
        }
        if let Some(slot) = self.mesh.edge_slot(u, v)? {
            self.insert_source_piece(SourcePiece {
                source,
                start_site: u,
                end_site: v,
                lo,
                hi,
            })?;
            self.mark(slot);
            return Ok(());
        }
        // Walk the fan around `u` to find the triangle the segment leaves
        // through, or a vertex it passes through exactly.
        let (pu, pv) = (xy(u), xy(v));
        let mut crossing: Vec<usize> = Vec::new();
        crate::try_reserve_vec(&mut crossing, self.mesh.triangle_count())?;
        let mut start = None;
        for slot in self.mesh.outgoing(u)? {
            let a = self.mesh.triangles[next_halfedge(slot)];
            let b = self.mesh.triangles[prev_halfedge(slot)];
            // Segment u->v exits triangle (u, a, b) through edge a->b when v
            // is left of u->a and right of u->b (strictly), or through a
            // vertex when collinear.
            let sa = exact::orient2d(pu, xy(a), pv);
            let sb = exact::orient2d(pu, xy(b), pv);
            if sa == ExactSign::Zero && between(pu, pv, xy(a)) {
                let split = exact::ExactRational::from_segment(
                    self.source(source).start.xy(),
                    self.source(source).end.xy(),
                    xy(a),
                )?;
                self.insert_constraint(xy, u, a, source, lo, split.try_clone()?)?;
                self.insert_constraint(xy, a, v, source, split, hi)?;
                return Ok(());
            }
            if sb == ExactSign::Zero && between(pu, pv, xy(b)) {
                let split = exact::ExactRational::from_segment(
                    self.source(source).start.xy(),
                    self.source(source).end.xy(),
                    xy(b),
                )?;
                self.insert_constraint(xy, u, b, source, lo, split.try_clone()?)?;
                self.insert_constraint(xy, b, v, source, split, hi)?;
                return Ok(());
            }
            if sa == ExactSign::Positive && sb == ExactSign::Negative {
                start = Some(next_halfedge(slot)); // the edge a -> b
                break;
            }
        }
        let Some(mut edge) = start else {
            unreachable!("a segment between two mesh vertices leaves the start fan somewhere");
        };
        // March across triangles: `edge` is the crossed edge of the current
        // triangle, running from its endpoint right of `u -> v` to the one on
        // the left; enter its twin's triangle and find the next exit. A
        // straight segment enters each triangle at most once.
        loop {
            if self.constrained[edge] {
                return Err(GeometryErrorKind::triangulation(
                    "constrained triangulation boundaries cross",
                ));
            }
            let twin = self.mesh.halfedges[edge];
            if twin == EMPTY || crossing.len() == self.mesh.triangle_count() {
                return Err(GeometryErrorKind::triangulation(
                    "constraint walk left the triangulation",
                ));
            }
            crate::try_push_vec(&mut crossing, edge)?;
            let apex = self.mesh.triangles[prev_halfedge(twin)];
            if apex == v {
                break;
            }
            let pa = xy(apex);
            let s = exact::orient2d(pu, pv, pa);
            if s == ExactSign::Zero {
                if !between(pu, pv, pa) {
                    return Err(GeometryErrorKind::triangulation(
                        "constraint walk met a collinear vertex outside the constraint",
                    ));
                }
                // Passes exactly through `apex`: constrain u->apex by flipping
                // what we have so far, then continue from apex.
                self.flip_until_clear(xy, u, apex, crossing)?;
                let split = exact::ExactRational::from_segment(
                    self.source(source).start.xy(),
                    self.source(source).end.xy(),
                    pa,
                )?;
                self.insert_constraint(xy, u, apex, source, lo, split.try_clone()?)?;
                self.insert_constraint(xy, apex, v, source, split, hi)?;
                return Ok(());
            }
            // The twin runs left to right, so its next edge leaves the right
            // endpoint and its previous edge enters the left one. An apex on
            // the left makes the next edge the crossed one; on the right, the
            // previous.
            edge = if s == ExactSign::Positive {
                next_halfedge(twin)
            } else {
                prev_halfedge(twin)
            };
        }
        self.flip_until_clear(xy, u, v, crossing)?;
        Ok(())
    }

    fn source(&self, id: usize) -> &SourceConstraint {
        self.source_constraints
            .get(id)
            .expect("source constraint ids are dense")
    }

    fn insert_source_piece(&mut self, piece: SourcePiece) -> Result<()> {
        self.source_pieces.try_reserve(1)?;
        self.source_pieces
            .entry(ordered(piece.start_site, piece.end_site))
            .or_insert(piece);
        Ok(())
    }

    /// Flip the crossed edges until none cross `u -> v`, then mark the edge.
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the existing million-step guard is a topology convergence invariant, not an allocation outcome"
    )]
    fn flip_until_clear(
        &mut self,
        xy: &impl Fn(usize) -> XY,
        u: usize,
        v: usize,
        mut crossing: Vec<usize>,
    ) -> Result<()> {
        let (pu, pv) = (xy(u), xy(v));
        let mut guard = 0_usize;
        while let Some(edge) = crossing.pop() {
            guard += 1;
            assert!(guard < 1_000_000, "constraint insertion did not converge");
            let twin = self.mesh.halfedges[edge];
            let (a, b) = (
                self.mesh.triangles[edge],
                self.mesh.triangles[next_halfedge(edge)],
            );
            let c = self.mesh.triangles[prev_halfedge(edge)];
            let d = self.mesh.triangles[prev_halfedge(twin)];
            // The quad a,b with apexes c,d is flippable only when convex.
            let convex = exact::orient2d(xy(c), xy(d), xy(a))
                != exact::orient2d(xy(c), xy(d), xy(b))
                && exact::orient2d(xy(a), xy(b), xy(c)) != exact::orient2d(xy(a), xy(b), xy(d));
            if !convex {
                try_insert_front(&mut crossing, edge)?;
                continue;
            }
            let new_edge = self.mesh.flip_carrying(edge, &mut self.constrained, false);
            let (na, nb) = (
                self.mesh.triangles[new_edge],
                self.mesh.triangles[next_halfedge(new_edge)],
            );
            let still_crosses = {
                let s1 = exact::orient2d(pu, pv, xy(na));
                let s2 = exact::orient2d(pu, pv, xy(nb));
                s1 != ExactSign::Zero
                    && s2 != ExactSign::Zero
                    && s1 != s2
                    && exact::orient2d(xy(na), xy(nb), pu) != exact::orient2d(xy(na), xy(nb), pv)
            };
            if still_crosses {
                try_insert_front(&mut crossing, new_edge)?;
            }
        }
        let slot = self
            .mesh
            .edge_slot(u, v)?
            .expect("the constraint is an edge once nothing crosses it");
        self.mark(slot);
        Ok(())
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
    pub(super) fn inside_faces(&self) -> Result<Vec<bool>> {
        #[cfg(test)]
        update_work(|counters| counters.inside_label_passes += 1);
        let count = self.mesh.triangle_count();
        let mut label = Vec::new();
        crate::try_reserve_vec(&mut label, count)?;
        label.resize(count, None);
        let mut queue = VecDeque::new();
        queue
            .try_reserve(count)
            .map_err(|_| crate::error::ResourceError::Allocation)?;
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
        let mut inside = Vec::new();
        crate::try_reserve_vec(&mut inside, count)?;
        for value in label {
            crate::try_push_vec(&mut inside, value.unwrap_or(false))?;
        }
        Ok(inside)
    }

    /// Ruppert refinement of the inside faces to `min_angle` degrees and/or
    /// `max_area`, inserting at most `limit` vertices. `Ok(false)` means the
    /// budget or a topological placement ran out before the mesh met the
    /// request; exact construction failures are returned as `Err`.
    pub(super) fn refine(
        &mut self,
        points: &mut Vec<Point>,
        min_angle: Option<f64>,
        max_area: Option<f64>,
        limit: usize,
    ) -> Result<bool> {
        let mut inserted = 0_usize;
        loop {
            let inside = self.inside_faces()?;
            let xy = |site: usize, points: &[Point]| points[site].xy();
            let mut bad = None;
            #[cfg(test)]
            update_work(|counters| counters.bad_face_scans += 1);
            for (t, &kept) in inside.iter().enumerate() {
                if !kept || !self.mesh.alive(t) {
                    continue;
                }
                let [a, b, c] = self.mesh.triangle(t);
                let (pa, pb, pc) = (xy(a, points), xy(b, points), xy(c, points));
                let violates = min_angle
                    .is_some_and(|limit| triangle_min_angle_degrees(pa, pb, pc) < limit)
                    || max_area
                        .is_some_and(|limit| open_xy_cycle_magnitude(&[pa, pb, pc]).get() > limit);
                if violates {
                    bad = Some(t);
                    break;
                }
            }
            let Some(t) = bad else {
                return Ok(true);
            };
            if inserted >= limit {
                return Ok(false);
            }
            let [a, b, c] = self.mesh.triangle(t);
            let (pa, pb, pc) = (xy(a, points), xy(b, points), xy(c, points));
            // No finite binary64 can represent this center. Ruppert can
            // still make progress with the explicitly chosen longest-edge
            // midpoint; this is not a recovery for construction errors.
            let center =
                circumcenter(pa, pb, pc)?.unwrap_or_else(|| longest_edge_midpoint(pa, pb, pc));
            // Ruppert: a constraint whose diametral circle holds the center is
            // split at its midpoint instead, and so is any constraint, or the
            // hull, that the walk toward the center would have to cross: a
            // center beyond a constraint is one that encroaches it. A source
            // split is emitted from its original exact parameter; a generated
            // edge midpoint only needs a valid topological placement.
            let encroached = (0..self.mesh.halfedges.len()).find(|&e| {
                self.constrained[e] && self.mesh.alive(e / 3) && {
                    let (s, t2) = (
                        self.mesh.triangles[e],
                        self.mesh.triangles[next_halfedge(e)],
                    );
                    in_diametral_circle(xy(s, points), xy(t2, points), center)
                }
            });
            let xy_now = |s: usize| points[s].xy();
            let placed = if let Some(e) = encroached {
                self.prepare_split(&xy_now, points, e)?
            } else {
                match self
                    .mesh
                    .locate_bounded(&xy_now, t, center, &self.constrained)
                {
                    Ok(Located::OnEdge(e)) if self.constrained[e] => {
                        self.prepare_split(&xy_now, points, e)?
                    },
                    Ok(located) => Some(PreparedSplit {
                        candidate: center,
                        located,
                        source: None,
                    }),
                    Err(e) => self.prepare_split(&xy_now, points, e)?,
                }
            };
            let Some(PreparedSplit {
                candidate,
                located,
                source,
            }) = placed
            else {
                return Ok(false);
            };
            if matches!(located, Located::OnVertex) {
                return Ok(false);
            }
            let site = points.len();
            // The cavity reuses slots, so remember constraints as vertex pairs
            // and re-mark from those once the mesh has settled; a split
            // constraint becomes its two halves.
            let mut pairs = self.constrained_pairs()?;
            if let Some((piece, _)) = &source {
                pairs.remove(&ordered(piece.start_site, piece.end_site));
                pairs.insert(ordered(piece.start_site, site));
                pairs.insert(ordered(site, piece.end_site));
                self.source_pieces.try_reserve(2)?;
            }
            crate::try_reserve_vec(points, 1)?;
            crate::try_reserve_vec(&mut self.provenance, 1)?;
            points.push(Point::new_raw_xy(candidate.x, candidate.y));
            self.mesh.admit_vertex(site)?;
            self.provenance.push(match &source {
                Some((piece, parameter)) => NodeProvenance::ConstraintSplit {
                    source: piece.source,
                    parameter: parameter.try_clone()?,
                },
                None => NodeProvenance::Generated,
            });
            let xy2 = |s: usize| points[s].xy();
            let mut skip = Vec::new();
            crate::try_reserve_vec(&mut skip, self.constrained.len())?;
            skip.extend_from_slice(&self.constrained);
            self.mesh.insert_constrained(&xy2, site, located, &skip)?;
            self.remark(&pairs)?;
            if let Some((piece, parameter)) = source {
                self.source_pieces
                    .remove(&ordered(piece.start_site, piece.end_site));
                self.source_pieces
                    .insert(ordered(piece.start_site, site), SourcePiece {
                        source: piece.source,
                        start_site: piece.start_site,
                        end_site: site,
                        lo: piece.lo,
                        hi: parameter.try_clone()?,
                    });
                self.source_pieces
                    .insert(ordered(site, piece.end_site), SourcePiece {
                        source: piece.source,
                        start_site: site,
                        end_site: piece.end_site,
                        lo: parameter,
                        hi: piece.hi,
                    });
            }
            inserted += 1;
        }
    }

    fn prepare_split(
        &self,
        xy: &impl Fn(usize) -> XY,
        points: &[Point],
        e: usize,
    ) -> Result<Option<PreparedSplit>> {
        let (start, end) = (
            self.mesh.triangles[e],
            self.mesh.triangles[next_halfedge(e)],
        );
        if self.constrained[e] {
            let Some(piece) = self.source_pieces.get(&ordered(start, end)) else {
                return Err(GeometryErrorKind::triangulation(
                    "constrained edge has no source interval",
                ));
            };
            let source = self.source(piece.source);
            let requested = piece.lo.try_midpoint(&piece.hi)?;
            let candidate = match requested.round_segment(source.start.xy(), source.end.xy()) {
                Ok(candidate) => candidate,
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::Geometry(GeometryErrorKind::ExactCoordinateOutOfRange)
                    ) =>
                {
                    return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
                },
                Err(error) => return Err(error),
            };
            let parameter = self.certify_source_split(piece, candidate)?;
            let Some(located) = self.locate_split(xy, e, candidate) else {
                return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
            };
            let source_piece = SourcePiece {
                source: piece.source,
                start_site: piece.start_site,
                end_site: piece.end_site,
                lo: piece.lo.try_clone()?,
                hi: piece.hi.try_clone()?,
            };
            return Ok(Some(PreparedSplit {
                candidate,
                located,
                source: Some((source_piece, parameter)),
            }));
        }
        let ps = points[start].xy();
        let pt = points[self.mesh.triangles[next_halfedge(e)]].xy();
        let midpoint = XY::new(f64::midpoint(ps.x, pt.x), f64::midpoint(ps.y, pt.y));
        let located = match self
            .mesh
            .locate_bounded(xy, e / 3, midpoint, &self.constrained)
        {
            Ok(Located::OnEdge(slot)) if slot == e || slot == self.mesh.halfedges[e] => {
                Located::OnEdge(e)
            },
            Ok(Located::Inside(face)) => Located::Inside(face),
            Ok(Located::OnVertex | Located::OnEdge(_)) | Err(_) => return Ok(None),
        };
        Ok(Some(PreparedSplit {
            candidate: midpoint,
            located,
            source: None,
        }))
    }

    fn certify_source_split(
        &self,
        piece: &SourcePiece,
        candidate: XY,
    ) -> Result<exact::ExactRational> {
        let source = self.source(piece.source);
        if exact::orient2d(source.start.xy(), source.end.xy(), candidate) != ExactSign::Zero {
            return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
        }
        let parameter =
            exact::ExactRational::from_segment(source.start.xy(), source.end.xy(), candidate)?;
        if parameter.try_cmp(&piece.lo)? != std::cmp::Ordering::Greater
            || parameter.try_cmp(&piece.hi)? != std::cmp::Ordering::Less
        {
            return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
        }
        Ok(parameter)
    }

    /// A source split is topologically on an edge only after exact
    /// classification says so. An adjacent-face hit is an interior point, not
    /// evidence that the source edge can be replaced by two edges.
    fn locate_split(&self, xy: &impl Fn(usize) -> XY, e: usize, point: XY) -> Option<Located> {
        let twin = self.mesh.halfedges[e];
        match self.mesh.classify(xy, e / 3, point) {
            Ok(Located::OnEdge(slot)) => (slot == e || slot == twin).then_some(Located::OnEdge(e)),
            Ok(Located::Inside(_) | Located::OnVertex) | Err(_) => None,
        }
    }

    fn constrained_pairs(&self) -> Result<HashSet<(usize, usize)>> {
        #[cfg(test)]
        update_work(|counters| counters.constraint_reconstruction_passes += 1);
        let mut pairs = crate::collections::try_hash_set_with_capacity(self.mesh.halfedges.len())?;
        for e in 0..self.mesh.halfedges.len() {
            if self.constrained[e] && self.mesh.alive(e / 3) {
                pairs.insert(ordered(
                    self.mesh.triangles[e],
                    self.mesh.triangles[next_halfedge(e)],
                ));
            }
        }
        Ok(pairs)
    }

    /// Re-derive the per-slot flags from the constrained vertex pairs after
    /// an insertion has renumbered the cavity's slots.
    fn remark(&mut self, pairs: &HashSet<(usize, usize)>) -> Result<()> {
        #[cfg(test)]
        update_work(|counters| counters.flag_remark_passes += 1);
        let mut constrained = Vec::new();
        crate::try_reserve_vec(&mut constrained, self.mesh.halfedges.len())?;
        constrained.resize(self.mesh.halfedges.len(), false);
        self.constrained = constrained;
        for e in 0..self.mesh.halfedges.len() {
            if !self.mesh.alive(e / 3) {
                continue;
            }
            if pairs.contains(&ordered(
                self.mesh.triangles[e],
                self.mesh.triangles[next_halfedge(e)],
            )) {
                self.constrained[e] = true;
            }
        }
        Ok(())
    }

    fn validate_source_chains(&self, points: &[Point]) -> Result<()> {
        let one = exact::ExactRational::one();
        let constrained = self.constrained_pairs()?;
        for source in &self.source_constraints {
            let mut pieces = Vec::new();
            crate::try_reserve_vec(&mut pieces, self.source_pieces.len())?;
            for piece in self
                .source_pieces
                .values()
                .filter(|piece| piece.source == source.id)
            {
                crate::try_push_vec(&mut pieces, piece)?;
            }
            if pieces.is_empty() {
                return Err(GeometryErrorKind::triangulation(format!(
                    "source constraint {} has no emitted boundary chain",
                    source.id
                )));
            }
            crate::geometry::try_sort_by(&mut pieces, |left, right| {
                let order = left.lo.try_cmp(&right.lo)?;
                if order.is_eq() {
                    left.hi.try_cmp(&right.hi)
                } else {
                    Ok(order)
                }
            })?;
            let mut cursor = exact::ExactRational::zero();
            for piece in pieces {
                if piece.lo.try_cmp(&cursor)? != std::cmp::Ordering::Equal
                    || piece.lo.try_cmp(&piece.hi)? != std::cmp::Ordering::Less
                {
                    return Err(GeometryErrorKind::triangulation(format!(
                        "source constraint {} has a non-ordered parameter chain",
                        source.id
                    )));
                }
                let Some(&start) = points.get(piece.start_site) else {
                    return Err(GeometryErrorKind::triangulation(
                        "source constraint references a missing start site",
                    ));
                };
                let Some(&end) = points.get(piece.end_site) else {
                    return Err(GeometryErrorKind::triangulation(
                        "source constraint references a missing end site",
                    ));
                };
                if exact::orient2d(source.start.xy(), source.end.xy(), start.xy())
                    != ExactSign::Zero
                    || exact::orient2d(source.start.xy(), source.end.xy(), end.xy())
                        != ExactSign::Zero
                {
                    return Err(GeometryErrorKind::triangulation(format!(
                        "source constraint {} has an off-line emitted vertex",
                        source.id
                    )));
                }
                let start_parameter = exact::ExactRational::from_segment(
                    source.start.xy(),
                    source.end.xy(),
                    start.xy(),
                )?;
                let end_parameter = exact::ExactRational::from_segment(
                    source.start.xy(),
                    source.end.xy(),
                    end.xy(),
                )?;
                if start_parameter.try_cmp(&piece.lo)? != std::cmp::Ordering::Equal
                    || end_parameter.try_cmp(&piece.hi)? != std::cmp::Ordering::Equal
                    || !constrained.contains(&ordered(piece.start_site, piece.end_site))
                {
                    return Err(GeometryErrorKind::triangulation(format!(
                        "source constraint {} does not match its emitted edge",
                        source.id
                    )));
                }
                cursor = piece.hi.try_clone()?;
            }
            if cursor.try_cmp(&one)? != std::cmp::Ordering::Equal {
                return Err(GeometryErrorKind::triangulation(format!(
                    "source constraint {} does not cover its source interval",
                    source.id
                )));
            }
        }
        Ok(())
    }

    pub(super) fn finish_plan(
        self,
        points: Vec<Point>,
        refinement: super::CdtRefinement,
        force_xy: bool,
    ) -> Result<ConstrainedMeshPlan> {
        self.validate_source_chains(&points)?;
        let inside = self.inside_faces()?;
        let mut faces = Vec::new();
        crate::try_reserve_vec(&mut faces, self.mesh.triangle_count())?;
        for (t, &kept) in inside.iter().enumerate() {
            if kept && self.mesh.alive(t) {
                crate::try_push_vec(&mut faces, self.mesh.triangle(t))?;
            }
        }
        let mut constraint_pieces = Vec::new();
        crate::try_reserve_vec(&mut constraint_pieces, self.source_pieces.len())?;
        for piece in self.source_pieces.into_values() {
            crate::try_push_vec(&mut constraint_pieces, piece)?;
        }
        crate::geometry::try_sort_by(&mut constraint_pieces, compare_source_pieces)?;
        Ok(ConstrainedMeshPlan {
            vertices: points,
            faces,
            provenance: self.provenance,
            source_constraints: self.source_constraints,
            constraint_pieces,
            refinement,
            force_xy,
        })
    }
}

const fn ordered(a: usize, b: usize) -> (usize, usize) {
    if a < b { (a, b) } else { (b, a) }
}

fn compare_source_pieces(left: &SourcePiece, right: &SourcePiece) -> Result<std::cmp::Ordering> {
    let source = left.source.cmp(&right.source);
    if source.is_ne() {
        return Ok(source);
    }
    let lo = left.lo.try_cmp(&right.lo)?;
    if lo.is_ne() {
        return Ok(lo);
    }
    let hi = left.hi.try_cmp(&right.hi)?;
    if hi.is_ne() {
        return Ok(hi);
    }
    Ok(left
        .start_site
        .cmp(&right.start_site)
        .then_with(|| left.end_site.cmp(&right.end_site)))
}

fn try_insert_front<T>(items: &mut Vec<T>, item: T) -> Result<()> {
    crate::try_reserve_vec(items, 1)?;
    items.insert(0, item);
    Ok(())
}

/// Whether `p`, collinear with `a -> b`, lies strictly between them.
fn between(a: XY, b: XY, p: XY) -> bool {
    let (lo_x, hi_x) = if a.x < b.x { (a.x, b.x) } else { (b.x, a.x) };
    let (lo_y, hi_y) = if a.y < b.y { (a.y, b.y) } else { (b.y, a.y) };
    lo_x <= p.x && p.x <= hi_x && lo_y <= p.y && p.y <= hi_y && p != a && p != b
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

/// The exact circumcenter rounded to nearest. `None` means that the exact
/// center is outside the finite binary64 range; the refinement caller makes
/// the deliberate midpoint choice for that numerical case. Every other error
/// remains an error, including resource failures and mesh invariants.
fn circumcenter(a: XY, b: XY, c: XY) -> Result<Option<XY>> {
    let center = exact::try_circumcenter(a, b, c)?;
    match center.round_nearest_even() {
        Ok(center) => Ok(Some(center)),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::Geometry(GeometryErrorKind::ExactCoordinateOutOfRange)
            ) =>
        {
            Ok(None)
        },
        Err(error) => Err(error),
    }
}

fn longest_edge_midpoint(a: XY, b: XY, c: XY) -> XY {
    let length = |p: XY, q: XY| (p.x - q.x).hypot(p.y - q.y);
    let (p, q) = if length(a, b) >= length(b, c) && length(a, b) >= length(c, a) {
        (a, b)
    } else if length(b, c) >= length(c, a) {
        (b, c)
    } else {
        (c, a)
    };
    XY::new(f64::midpoint(p.x, q.x), f64::midpoint(p.y, q.y))
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
        let pts = points(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (2.0, 8.0),
        ]);
        let mesh = ConstrainedMesh::build(&pts, &[[0, 2]]).unwrap();
        assert!(
            mesh.mesh.edge_slot(0, 2).unwrap().is_some(),
            "the constraint 0-2 is an edge"
        );
        let slot = mesh.mesh.edge_slot(0, 2).unwrap().unwrap();
        assert!(mesh.constrained[slot]);
    }

    /// The constraint flags mark exactly the constrained vertex pairs, on both
    /// halves of each edge.
    fn assert_flags_match(mesh: &ConstrainedMesh, edges: &[[usize; 2]]) {
        for e in 0..mesh.mesh.halfedges.len() {
            if !mesh.mesh.alive(e / 3) {
                continue;
            }
            let pair = ordered(
                mesh.mesh.triangles[e],
                mesh.mesh.triangles[next_halfedge(e)],
            );
            let expected = edges.iter().any(|&[u, v]| ordered(u, v) == pair);
            assert_eq!(mesh.constrained[e], expected, "slot {e} pair {pair:?}");
        }
    }

    #[test]
    fn a_constraint_crossing_many_edges_becomes_an_edge() {
        // Points alternate above and below the axis, so the Delaunay edges
        // between them cross the constraint 0-1 many times.
        let mut xy = vec![(0.0, 0.0), (12.0, 0.0)];
        for step in 1..12 {
            let side = if step % 2 == 0 { 1.0 } else { -1.0 };
            xy.push((f64::from(step), side * (0.5 + 0.1 * f64::from(step % 3))));
        }
        let pts = points(&xy);
        let mesh = ConstrainedMesh::build(&pts, &[[0, 1]]).unwrap();
        assert_flags_match(&mesh, &[[0, 1]]);
    }

    #[test]
    fn flips_carry_earlier_constraint_flags() {
        let pts = points(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (2.0, 8.0),
        ]);
        let edges = [[0, 1], [1, 2], [2, 3], [3, 0], [0, 2]];
        let mesh = ConstrainedMesh::build(&pts, &edges).unwrap();
        assert_flags_match(&mesh, &edges);
    }

    #[test]
    fn crossing_constraints_are_an_error() {
        let pts = points(&[
            (0.0, 0.0),
            (10.0, 10.0),
            (10.0, 0.0),
            (0.0, 10.0),
            (5.0, 1.0),
        ]);
        let result = ConstrainedMesh::build_with_sources(
            &pts,
            &(0..pts.len()).collect::<Vec<_>>(),
            &[[0, 1], [2, 3]]
                .iter()
                .enumerate()
                .map(|(id, &[start_site, end_site])| SourceConstraint {
                    id,
                    start: pts[start_site],
                    end: pts[end_site],
                    start_site,
                    end_site,
                })
                .collect::<Vec<_>>(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn parity_flood_finds_the_inside_of_a_square_with_a_hole() {
        let outer = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)];
        let hole = [(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0)];
        let pts = points(&outer.iter().chain(hole.iter()).copied().collect::<Vec<_>>());
        let ring = |base: usize| {
            (0..4)
                .map(|i| [base + i, base + (i + 1) % 4])
                .collect::<Vec<_>>()
        };
        let edges: Vec<[usize; 2]> = ring(0).into_iter().chain(ring(4)).collect();
        let mesh = ConstrainedMesh::build(&pts, &edges).unwrap();
        let inside = mesh.inside_faces().unwrap();
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
        assert!(
            mesh.refine(&mut pts, None, Some(25.0), 10_000).unwrap(),
            "a generous budget completes"
        );
        let inside = mesh.inside_faces().unwrap();
        for (t, &kept) in inside.iter().enumerate() {
            if kept && mesh.mesh.alive(t) {
                let [a, b, c] = mesh.mesh.triangle(t);
                assert!(
                    open_xy_cycle_magnitude(&[pts[a].xy(), pts[b].xy(), pts[c].xy()]).get() <= 25.0
                );
            }
        }
        let mut pts2 = points(&[(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)]);
        let mut mesh2 = ConstrainedMesh::build(&pts2, &edges).unwrap();
        assert!(
            !mesh2.refine(&mut pts2, None, Some(0.5), 3).unwrap(),
            "three vertices cannot reach 0.5 area"
        );
    }

    #[test]
    fn refinement_rebuild_work_counters_cover_the_existing_passes() {
        let _ = take_refinement_work_counters();
        let mut pts = points(&[(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)]);
        let edges: Vec<[usize; 2]> = (0..4).map(|i| [i, (i + 1) % 4]).collect();
        let mut mesh = ConstrainedMesh::build(&pts, &edges).unwrap();
        assert!(mesh.refine(&mut pts, None, Some(100.0), 100).unwrap());
        let counters = take_refinement_work_counters();
        assert!(counters.inside_label_passes > 0);
        assert!(counters.bad_face_scans > 0);
        assert!(counters.constraint_reconstruction_passes > 0);
        assert!(counters.flag_remark_passes > 0);
    }

    #[test]
    fn rounded_source_midpoint_is_rejected_before_mesh_mutation() {
        let next = 1.0_f64.next_up();
        let mut pts = points(&[(1.0, 0.0), (next, 1.0), (1.0, 0.5)]);
        let before = pts.clone();
        let edges = vec![[0, 1], [1, 2], [2, 0]];
        let mut mesh = ConstrainedMesh::build(&pts, &edges).expect("non-collinear source");
        let error = mesh
            .refine(&mut pts, None, Some(1.0e-30), 32)
            .expect_err("the rounded midpoint is not on the source edge");
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
                operation: "triangulate"
            })
        ));
        assert_eq!(pts, before, "rejected source split does not append a site");
    }

    #[test]
    fn rounded_point_inside_the_adjacent_face_is_not_an_edge_split() {
        let next = 1.0_f64.next_up();
        let pts = points(&[(1.0, 0.0), (next, 1.0), (0.0, 0.5)]);
        let mesh = ConstrainedMesh::build(&pts, &[[0, 1], [1, 2], [2, 0]]).unwrap();
        let edge = mesh.mesh.edge_slot(0, 1).unwrap().unwrap();
        let xy = |site: usize| pts[site].xy();
        assert_eq!(
            mesh.locate_split(&xy, edge, XY::new(1.0, 0.5)),
            None,
            "face containment is not proof of source incidence"
        );
    }

    #[test]
    fn source_piece_preparation_allocation_failures_are_retryable() {
        let pts = points(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)]);
        let mesh = ConstrainedMesh::build(&pts, &[[0, 1], [1, 2], [2, 3], [3, 0]]).unwrap();
        let edge = mesh.mesh.edge_slot(0, 1).unwrap().unwrap();
        let xy = |site: usize| pts[site].xy();
        let (result, allocations) =
            crate::test_support::allocation_count(|| mesh.prepare_split(&xy, &pts, edge));
        result.unwrap();
        assert!(allocations > 0);
        for allowed in 0..allocations {
            let result = crate::test_support::fail_allocations_after(allowed, || {
                mesh.prepare_split(&xy, &pts, edge)
            });
            assert!(matches!(
                result,
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::Resource(crate::error::ResourceError::Allocation)
                    )
            ));
        }
        crate::test_support::fail_allocations_after(allocations + 8, || {
            mesh.prepare_split(&xy, &pts, edge)
        })
        .unwrap();
    }

    #[test]
    fn source_chain_validation_allocation_failures_are_retryable() {
        let pts = points(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)]);
        let mesh = ConstrainedMesh::build(&pts, &[[0, 1], [1, 2], [2, 3], [3, 0]]).unwrap();
        let (result, allocations) =
            crate::test_support::allocation_count(|| mesh.validate_source_chains(&pts));
        result.unwrap();
        assert!(allocations > 0);
        for allowed in 0..allocations {
            let result = crate::test_support::fail_allocations_after(allowed, || {
                mesh.validate_source_chains(&pts)
            });
            assert!(matches!(
                result,
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::Resource(crate::error::ResourceError::Allocation)
                    )
            ));
        }
        crate::test_support::fail_allocations_after(allocations + 8, || {
            mesh.validate_source_chains(&pts)
        })
        .unwrap();
    }

    #[test]
    fn unrepresentable_circumcenter_is_an_explicit_midpoint_case() {
        let a = XY::new(0.0, 0.0);
        let b = XY::new(1.0, 0.0);
        let c = XY::new(0.5, f64::from_bits(1));

        assert_eq!(circumcenter(a, b, c).unwrap(), None);
        assert_eq!(longest_edge_midpoint(a, b, c), XY::new(0.5, 0.0));
    }

    #[test]
    fn collinear_circumcenter_failure_is_not_a_midpoint() {
        let error =
            circumcenter(XY::new(0.0, 0.0), XY::new(1.0, 1.0), XY::new(2.0, 2.0)).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(crate::geometry::GeometryErrorKind::Voronoi(_))
        ));
    }

    #[test]
    fn circumcenter_propagates_rounding_allocation_failure() {
        let a = XY::new(0.0, 0.0);
        let b = XY::new(1.0, 0.0);
        let c = XY::new(0.5, f64::from_bits(1));
        let (result, allocations) = crate::test_support::allocation_count(|| circumcenter(a, b, c));
        assert_eq!(result.unwrap(), None);
        assert!(allocations > 0, "the exact rounding path must allocate");

        let error =
            crate::test_support::fail_allocations_after(allocations - 1, || circumcenter(a, b, c))
                .unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::Allocation)
        ));
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
        let inside = cdt.inside_faces().unwrap();
        (0..cdt.mesh.triangle_count())
            .filter(|&t| cdt.mesh.alive(t) && inside[t])
            .count()
    }

    #[test]
    fn reciprocal_pentagon_swapped_axes_is_three_faces() {
        for exponent in [22_i32, 154, 162] {
            let (large, tiny) = (10_f64.powi(exponent), 10_f64.powi(-exponent));
            let base = [
                (-large, 0.0),
                (-0.5 * large, -tiny),
                (0.5 * large, -tiny),
                (large, 0.0),
                (0.0, tiny),
            ];
            let swapped: Vec<(f64, f64)> = base.iter().map(|&(x, y)| (y, x)).collect();
            let points = pts(&swapped);
            let cdt = ConstrainedMesh::build(&points, &ring_edges(5)).expect("five corners");
            assert_eq!(inside_count(&cdt), 3, "exponent {exponent}");
        }
    }

    #[test]
    fn sliver_refines_to_the_angle_without_leaving_the_hull() {
        let mut points = pts(&[(0.0, 0.0), (20.0, 0.0), (20.0, 0.25), (0.125, 0.0625)]);
        let mut cdt = ConstrainedMesh::build(&points, &ring_edges(4)).unwrap();
        let before = inside_count(&cdt);
        assert!(
            cdt.refine(&mut points, Some(25.0), None, 100_000).unwrap(),
            "refinement completes"
        );
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
                assert!(
                    cdt.refine(&mut points, None, Some(0.4), 100_000).unwrap(),
                    "exponent {exponent} swap {swap}"
                );
                assert_eq!(inside_count(&cdt), 4, "exponent {exponent} swap {swap}");
            }
        }
    }
}
