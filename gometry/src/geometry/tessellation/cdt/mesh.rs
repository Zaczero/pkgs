//! Constraint recovery and parity labeling on the exact halfedge mesh.

use std::collections::VecDeque;

use super::super::exact_delaunay::{
    EMPTY, FaceId, FaceSide, HalfedgeId, Mesh, MeshCoordinates as _,
};
use super::plan::{split_deviation_within, split_half_ulp_bounds};
#[cfg(test)]
use super::update_work;
use super::{
    CdtCoordinates, CdtQuery, ConstrainedMesh, ConstraintPiece, ConstraintPieces, FaceLabels,
    NodeProvenance, PieceCursor, RestrictionConstraint, SourceConstraint, SourceRingStart,
    opposite_nonzero, ordered,
};
use crate::error::Result;
use crate::geometry::exact_arith::ExactSign;
#[cfg(test)]
use crate::geometry::tessellation::RingOwner;
use crate::geometry::tessellation::exact;
use crate::geometry::{GeometryErrorKind, Point};

impl ConstrainedMesh {
    #[expect(
        clippy::indexing_slicing,
        reason = "HalfedgeId comes from this mesh and the constraint column is admitted to its edge count"
    )]
    pub(super) fn is_constrained(&self, edge: HalfedgeId) -> bool {
        self.constrained[edge.index()]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "HalfedgeId comes from this mesh and the constraint column is admitted to its edge count"
    )]
    pub(super) const fn set_constrained(flags: &mut [bool], edge: HalfedgeId, value: bool) {
        flags[edge.index()] = value;
    }

    /// Delaunay-triangulate `points`, then force every `edges` pair in.
    #[cfg(test)]
    pub(super) fn build(points: &[Point], edges: &[[usize; 2]]) -> Option<Self> {
        let sources: Vec<_> = edges
            .iter()
            .enumerate()
            .map(|(id, &[start_site, end_site])| SourceConstraint {
                id,
                source_root: id,
                start: points[start_site],
                end: points[end_site],
                start_site,
                end_site,
                start_occurrence: start_site,
                end_occurrence: end_site,
            })
            .collect();
        Self::build_with_sources(
            points,
            &sources,
            vec![SourceRingStart::new(0, RingOwner::new(0, 0))],
            CdtCoordinates::raw(points, points),
        )
        .ok()
        .flatten()
    }

    #[cfg(test)]
    pub(in crate::geometry::tessellation::delaunay) fn build_with_sources(
        points: &[Point],
        sources: &[SourceConstraint],
        source_rings: Vec<SourceRingStart>,
        coordinates: CdtCoordinates,
    ) -> Result<Option<Self>> {
        Self::build_with_restrictions(points, sources, &[], source_rings, coordinates)
    }

    pub(in crate::geometry::tessellation::delaunay) fn build_with_restrictions(
        points: &[Point],
        sources: &[SourceConstraint],
        restrictions: &[RestrictionConstraint],
        source_rings: Vec<SourceRingStart>,
        coordinates: CdtCoordinates,
    ) -> Result<Option<Self>> {
        let Some(mesh) = Mesh::try_delaunay(&coordinates, points.len())? else {
            return Ok(None);
        };
        let constrained = vec![false; mesh.edge_count()];
        let source_constraints = sources.to_vec();
        let retained_restrictions = restrictions.to_vec();
        let constraint_count = sources
            .len()
            .checked_add(retained_restrictions.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let constraint_pieces = ConstraintPieces::new(constraint_count)?;
        let mut provenance = points
            .iter()
            .map(|_| NodeProvenance::SourceSite)
            .collect::<Vec<_>>();
        for restriction in restrictions {
            for site in [restriction.start_site, restriction.end_site] {
                if coordinates.retained_site_location(site).is_none() {
                    let provenance = provenance.get_mut(site).ok_or_else(|| {
                        GeometryErrorKind::triangulation(
                            "chart restriction endpoint is outside the initial mesh sites",
                        )
                    })?;
                    *provenance = NodeProvenance::ChartRestrictionSite;
                }
            }
        }
        let mut this = Self {
            mesh,
            constrained,
            coordinates,
            source_constraints,
            restrictions: retained_restrictions,
            source_rings,
            constraint_pieces,
            provenance,
            split_certificates: Vec::new(),
        };
        for source in sources {
            this.insert_constraint(
                ConstraintPiece {
                    constraint: source.id,
                    start_site: source.start_site,
                    end_site: source.end_site,
                    lo: exact::ExactRational::zero(),
                    hi: exact::ExactRational::one(),
                },
                &mut PieceCursor::Replace(source.id),
            )?;
        }
        let restriction_base = sources.len();
        for restriction in restrictions {
            let slot = restriction_base
                .checked_add(restriction.id)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            this.insert_constraint(
                ConstraintPiece {
                    constraint: slot,
                    start_site: restriction.start_site,
                    end_site: restriction.end_site,
                    lo: exact::ExactRational::zero(),
                    hi: exact::ExactRational::one(),
                },
                &mut PieceCursor::Replace(slot),
            )?;
        }
        Ok(Some(this))
    }

    /// Force the segment `u -> v` to be an edge, splitting it at any vertex it
    /// passes through exactly, and flipping every edge it crosses.
    pub(super) fn insert_constraint(
        &mut self,
        piece: ConstraintPiece,
        cursor: &mut PieceCursor,
    ) -> Result<Vec<FaceId>> {
        let mut legalization = Vec::new();
        self.recover_constraint(piece, cursor, &mut legalization)?;
        if legalization.is_empty() {
            return Ok(Vec::new());
        }
        let mut changed: Vec<_> = legalization.iter().map(|edge| edge.face()).collect();
        let coordinates = &self.coordinates;
        changed.extend(self.mesh.try_legalize_constrained_from(
            coordinates,
            &mut self.constrained,
            legalization,
        )?);
        Ok(changed)
    }

    fn recover_constraint(
        &mut self,
        piece: ConstraintPiece,
        cursor: &mut PieceCursor,
        legalization: &mut Vec<HalfedgeId>,
    ) -> Result<()> {
        let ConstraintPiece {
            constraint,
            start_site: u,
            end_site: v,
            lo,
            hi,
        } = piece;
        if u == v {
            return Ok(());
        }
        if let Some(slot) = self.mesh.edge_slot(u, v) {
            self.constraint_pieces.record(
                ConstraintPiece {
                    constraint,
                    start_site: u,
                    end_site: v,
                    lo,
                    hi,
                },
                cursor,
            );
            self.mark(slot);
            return Ok(());
        }
        // Walk the fan around `u` to find the triangle the segment leaves
        // through, or a vertex it passes through exactly.
        let (pu, pv) = (
            self.coordinates.site_query(u),
            self.coordinates.site_query(v),
        );
        let mut crossing = Vec::new();
        let mut start = None;
        for slot in self.mesh.outgoing(u) {
            let a = self.mesh.edge_end(slot);
            let b = self.mesh.edge_apex(slot);
            // Segment u->v exits triangle (u, a, b) through edge a->b when v
            // is left of u->a and right of u->b (strictly), or through a
            // vertex when collinear.
            let qa = self.coordinates.site_query(a);
            let qb = self.coordinates.site_query(b);
            let sa = self.coordinates.orient([pu, qa, pv])?;
            let sb = self.coordinates.orient([pu, qb, pv])?;
            if sa == ExactSign::Zero && self.coordinates.between(pu, pv, qa) {
                let split = self
                    .coordinates
                    .interpolate_source_parameter(&lo, &hi, pu, pv, qa)?;
                self.certify_constraint_boundary_point(constraint, &split, qa)?;
                self.recover_constraint(
                    ConstraintPiece {
                        constraint,
                        start_site: u,
                        end_site: a,
                        lo,
                        hi: split.clone(),
                    },
                    cursor,
                    legalization,
                )?;
                self.recover_constraint(
                    ConstraintPiece {
                        constraint,
                        start_site: a,
                        end_site: v,
                        lo: split,
                        hi,
                    },
                    cursor,
                    legalization,
                )?;
                return Ok(());
            }
            if sb == ExactSign::Zero && self.coordinates.between(pu, pv, qb) {
                let split = self
                    .coordinates
                    .interpolate_source_parameter(&lo, &hi, pu, pv, qb)?;
                self.certify_constraint_boundary_point(constraint, &split, qb)?;
                self.recover_constraint(
                    ConstraintPiece {
                        constraint,
                        start_site: u,
                        end_site: b,
                        lo,
                        hi: split.clone(),
                    },
                    cursor,
                    legalization,
                )?;
                self.recover_constraint(
                    ConstraintPiece {
                        constraint,
                        start_site: b,
                        end_site: v,
                        lo: split,
                        hi,
                    },
                    cursor,
                    legalization,
                )?;
                return Ok(());
            }
            if sa == ExactSign::Positive && sb == ExactSign::Negative {
                start = Some(slot.next()); // the edge a -> b
                break;
            }
        }
        let Some(mut edge) = start else {
            return Err(GeometryErrorKind::triangulation(
                "constraint walk could not leave its start vertex",
            ));
        };
        // March across triangles: `edge` is the crossed edge of the current
        // triangle, running from its endpoint right of `u -> v` to the one on
        // the left; enter its twin's triangle and find the next exit. A
        // straight segment enters each triangle at most once.
        loop {
            if self.is_constrained(edge) {
                return Err(GeometryErrorKind::triangulation(
                    "constrained triangulation boundaries cross",
                ));
            }
            let twin = self.mesh.twin(edge);
            if twin == EMPTY || crossing.len() == self.mesh.triangle_count() {
                return Err(GeometryErrorKind::triangulation(
                    "constraint walk left the triangulation",
                ));
            }
            let (a, b) = self.mesh.edge_vertices(edge);
            crossing.push((edge, ordered(a, b)));
            let twin = HalfedgeId::from_index(twin);
            let apex = self.mesh.edge_apex(twin);
            if apex == v {
                break;
            }
            let pa = self.coordinates.site_query(apex);
            let s = self.coordinates.orient([pu, pv, pa])?;
            if s == ExactSign::Zero {
                if !self.coordinates.between(pu, pv, pa) {
                    return Err(GeometryErrorKind::triangulation(
                        "constraint walk met a collinear vertex outside the constraint",
                    ));
                }
                let split = self
                    .coordinates
                    .interpolate_source_parameter(&lo, &hi, pu, pv, pa)?;
                self.certify_constraint_boundary_point(constraint, &split, pa)?;
                // Passes exactly through `apex`: constrain u->apex by flipping
                // what we have so far, then continue from apex.
                self.flip_until_clear(u, apex, crossing, legalization)?;
                self.recover_constraint(
                    ConstraintPiece {
                        constraint,
                        start_site: u,
                        end_site: apex,
                        lo,
                        hi: split.clone(),
                    },
                    cursor,
                    legalization,
                )?;
                self.recover_constraint(
                    ConstraintPiece {
                        constraint,
                        start_site: apex,
                        end_site: v,
                        lo: split,
                        hi,
                    },
                    cursor,
                    legalization,
                )?;
                return Ok(());
            }
            // The twin runs left to right, so its next edge leaves the right
            // endpoint and its previous edge enters the left one. An apex on
            // the left makes the next edge the crossed one; on the right, the
            // previous.
            edge = if s == ExactSign::Positive {
                twin.next()
            } else {
                twin.prev()
            };
        }
        self.flip_until_clear(u, v, crossing, legalization)?;
        self.constraint_pieces.record(
            ConstraintPiece {
                constraint,
                start_site: u,
                end_site: v,
                lo,
                hi,
            },
            cursor,
        );
        Ok(())
    }

    fn certify_constraint_boundary_point(
        &self,
        constraint: usize,
        parameter: &exact::ExactRational,
        point: CdtQuery,
    ) -> Result<()> {
        let (start_site, end_site) = self.constraint_sites(constraint)?;
        let start = self.coordinates.site_query(start_site);
        let end = self.coordinates.site_query(end_site);
        if self.coordinates.orient([start, end, point])? == ExactSign::Zero {
            return Ok(());
        }
        let point_xy = self.coordinates.proposal_xy(point);
        let half_ulp = split_half_ulp_bounds(point_xy)?;
        if !split_deviation_within(&self.coordinates, start, end, parameter, point, &half_ulp)? {
            return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
        }
        Ok(())
    }

    /// Flip the crossed edges until none cross `u -> v`, then mark the edge.
    fn flip_until_clear(
        &mut self,
        u: usize,
        v: usize,
        mut crossing: Vec<(HalfedgeId, (usize, usize))>,
        legalization: &mut Vec<HalfedgeId>,
    ) -> Result<()> {
        let (pu, pv) = (
            self.coordinates.site_query(u),
            self.coordinates.site_query(v),
        );
        let mut steps = 0_usize;
        while let Some((candidate, pair)) = crossing.pop() {
            steps = steps.saturating_add(1);
            if steps >= 1_000_000 {
                return Err(GeometryErrorKind::triangulation(
                    "constraint insertion did not converge",
                ));
            }
            let (start, end) = self.mesh.edge_vertices(candidate);
            let edge = if ordered(start, end) == pair {
                Some(candidate)
            } else {
                self.mesh
                    .edge_slot(pair.0, pair.1)
                    .or_else(|| self.mesh.edge_slot(pair.1, pair.0))
            };
            let Some(edge) = edge else {
                // A flip removed this diagonal; its replacement was queued
                // only if it still crosses the constraint.
                continue;
            };
            let (a, b) = self.mesh.edge_vertices(edge);
            if self.is_constrained(edge) {
                return Err(GeometryErrorKind::triangulation(
                    "constrained triangulation boundaries cross",
                ));
            }
            let twin = self.mesh.twin(edge);
            if twin == EMPTY {
                return Err(GeometryErrorKind::triangulation(
                    "constraint walk left the triangulation",
                ));
            }
            let twin = HalfedgeId::from_index(twin);
            let c = self.mesh.edge_apex(edge);
            let d = self.mesh.edge_apex(twin);
            let first_a = self.coordinates.orient([
                self.coordinates.site_query(c),
                self.coordinates.site_query(d),
                self.coordinates.site_query(a),
            ])?;
            let first_b = self.coordinates.orient([
                self.coordinates.site_query(c),
                self.coordinates.site_query(d),
                self.coordinates.site_query(b),
            ])?;
            let second_c = self.coordinates.orient([
                self.coordinates.site_query(a),
                self.coordinates.site_query(b),
                self.coordinates.site_query(c),
            ])?;
            let second_d = self.coordinates.orient([
                self.coordinates.site_query(a),
                self.coordinates.site_query(b),
                self.coordinates.site_query(d),
            ])?;
            if !opposite_nonzero(first_a, first_b) || !opposite_nonzero(second_c, second_d) {
                crossing.insert(0, (edge, pair));
                continue;
            }
            let new_edge = self.mesh.flip_carrying(edge, &mut self.constrained, false);
            let twin = HalfedgeId::from_index(self.mesh.twin(new_edge));
            // Recovery can make any of the six affected slots illegal. Defer
            // Lawson flips until the complete source chain has been constrained.
            legalization.extend([
                new_edge,
                new_edge.next(),
                new_edge.prev(),
                twin,
                twin.next(),
                twin.prev(),
            ]);
            let (new_start, new_end) = self.mesh.edge_vertices(new_edge);
            if self.coordinates.segments_properly_cross([
                pu,
                pv,
                self.coordinates.site_query(new_start),
                self.coordinates.site_query(new_end),
            ])? {
                crossing.insert(0, (new_edge, ordered(new_start, new_end)));
            }
        }
        let Some(slot) = self.mesh.edge_slot(u, v) else {
            return Err(GeometryErrorKind::triangulation(
                "constraint insertion did not produce an edge",
            ));
        };
        self.mark(slot);
        Ok(())
    }

    fn mark(&mut self, slot: HalfedgeId) {
        Self::set_constrained(&mut self.constrained, slot, true);
        let twin = self.mesh.twin(slot);
        if twin != EMPTY {
            Self::set_constrained(&mut self.constrained, HalfedgeId::from_index(twin), true);
        }
    }

    /// Faces inside the constraint polygon: the hull faces are outside, and
    /// crossing a constraint toggles, crossing anything else does not.
    pub(in crate::geometry::tessellation::delaunay) fn inside_faces(&self) -> Vec<bool> {
        #[cfg(test)]
        update_work(|counters| {
            counters.inside_label_passes = counters.inside_label_passes.saturating_add(1);
        });
        let count = self.mesh.triangle_count();
        let mut labels = FaceLabels::new(count);
        let mut queue = VecDeque::with_capacity(count);
        // Seed from the hull: beyond a hull edge is the outside, so a face is
        // outside unless that hull edge is itself a constraint, in which case
        // the polygon's boundary runs along the hull and the face is inside.
        for edge in self.mesh.edge_ids() {
            let face = edge.face();
            if self.mesh.twin(edge) == EMPTY && self.mesh.alive(face) && labels.get(face).is_none()
            {
                let inside = self.is_constrained(edge);
                labels.set(face, inside);
                queue.push_back((face, inside));
            }
        }
        while let Some((face, inside)) = queue.pop_front() {
            for side in FaceSide::ALL {
                let edge = HalfedgeId::in_face(face, side);
                let twin = self.mesh.twin(edge);
                if twin == EMPTY {
                    continue;
                }
                let neighbor = HalfedgeId::from_index(twin).face();
                if labels.get(neighbor).is_none() {
                    let inside = if self.is_constrained(edge) {
                        !inside
                    } else {
                        inside
                    };
                    labels.set(neighbor, inside);
                    queue.push_back((neighbor, inside));
                }
            }
        }
        labels.into_inside()
    }
}
