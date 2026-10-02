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
//! exact parameter and certifies the nearest binary64 point within half an ulp
//! per axis. A center outside binary64 uses the explicit longest-edge midpoint
//! construction; resource and invariant failures remain errors. The vertex
//! budget bounds the loop, and a mesh the budget could not finish is rejected
//! rather than returned as if it met the request.

#![expect(
    clippy::many_single_char_names,
    reason = "a, b, c, d name the vertices of a quad and e, f, t, u its slots, the geometry kernel convention"
)]

use std::collections::VecDeque;

use super::exact_delaunay::{EMPTY, FaceId, FaceSide, HalfedgeId, Located, Mesh, site_xy};
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
    pub(super) source_piece_visits: usize,
}

#[cfg(test)]
thread_local! {
    static REFINEMENT_WORK: std::cell::Cell<RefinementWorkCounters> =
        const { std::cell::Cell::new(RefinementWorkCounters {
            inside_label_passes: 0,
            bad_face_scans: 0,
            constraint_reconstruction_passes: 0,
            flag_remark_passes: 0,
            source_piece_visits: 0,
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

#[derive(Clone, Debug)]
pub(super) struct SourcePiece {
    pub(super) source: usize,
    pub(super) start_site: usize,
    pub(super) end_site: usize,
    pub(super) lo: exact::ExactRational,
    pub(super) hi: exact::ExactRational,
}

/// A rounded boundary point whose exact source parameter and half-ULP error
/// bound were checked before the point entered the mesh.
#[derive(Clone, Debug)]
struct CertifiedBoundarySplit {
    source: usize,
    parameter: exact::ExactRational,
    point: XY,
    half_ulp: [exact::ExactDyadic; 2],
}

#[derive(Debug)]
pub(super) enum NodeProvenance {
    SourceSite { occurrence: usize },
    ConstraintSplit { certificate: usize },
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
    split_certificates: Vec<CertifiedBoundarySplit>,
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
        for (site, provenance) in self.provenance.iter().enumerate() {
            match provenance {
                NodeProvenance::SourceSite { occurrence } => {
                    if *occurrence >= source_occurrence_count {
                        return Err(GeometryErrorKind::triangulation(
                            "constrained mesh certificate has an invalid source occurrence",
                        ));
                    }
                },
                NodeProvenance::ConstraintSplit { certificate } => {
                    let Some(certificate) = self.split_certificates.get(*certificate) else {
                        return Err(GeometryErrorKind::triangulation(
                            "constrained mesh certificate has a missing split proof",
                        ));
                    };
                    if certificate.source >= self.source_constraints.len()
                        || certificate.parameter.try_cmp(&zero)? != std::cmp::Ordering::Greater
                        || certificate.parameter.try_cmp(&one)? != std::cmp::Ordering::Less
                    {
                        return Err(GeometryErrorKind::triangulation(
                            "constrained mesh certificate has an invalid split provenance",
                        ));
                    }
                    let point = self.vertices.get(site).ok_or_else(|| {
                        GeometryErrorKind::triangulation(
                            "constrained mesh certificate has a missing split vertex",
                        )
                    })?;
                    let expected_half_ulp = split_half_ulp_bounds(point.xy())?;
                    if point.xy() != certificate.point || expected_half_ulp != certificate.half_ulp
                    {
                        return Err(GeometryErrorKind::triangulation(
                            "constrained mesh certificate has a mismatched split proof",
                        ));
                    }
                },
                NodeProvenance::Generated => {},
            }
        }
        for (id, source) in self.source_constraints.iter().enumerate() {
            if source.id != id
                || source.start_site == source.end_site
                || !source.start.xy().x.is_finite()
                || !source.start.xy().y.is_finite()
                || !source.end.xy().x.is_finite()
                || !source.end.xy().y.is_finite()
                || self
                    .vertices
                    .get(source.start_site)
                    .is_none_or(|point| point.xy() != source.start.xy())
                || self
                    .vertices
                    .get(source.end_site)
                    .is_none_or(|point| point.xy() != source.end.xy())
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
            {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate has an invalid source piece",
                ));
            }
            if piece.start_site == piece.end_site
                || piece.lo.try_cmp(&piece.hi)? != std::cmp::Ordering::Less
            {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate has an invalid source piece",
                ));
            }
            if piece.source != source_id {
                if source_id.checked_add(1) != Some(piece.source)
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
            cursor = piece.hi.clone();
        }
        let source_count_matches = if self.constraint_pieces.is_empty() {
            self.source_constraints.is_empty()
        } else {
            source_id.checked_add(1) == Some(self.source_constraints.len())
        };
        if !source_count_matches
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

fn exact_source_coordinate(
    start: f64,
    end: f64,
    parameter: &exact::ExactRational,
) -> Result<exact::ExactRational> {
    let start = exact::ExactRational::from_f64(start);
    let delta = exact::ExactRational::from_f64(end).try_subtract(&start)?;
    start.try_add(&delta.try_product(parameter)?)
}

fn split_half_ulp(value: f64) -> Result<exact::ExactDyadic> {
    if !value.is_finite() {
        return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
    }
    let next = value.next_up();
    let previous = value.next_down();
    let up_gap = if next.is_finite() { next - value } else { 0.0 };
    let down_gap = if previous.is_finite() {
        value - previous
    } else {
        0.0
    };
    let gap = up_gap.max(down_gap);
    exact::ExactDyadic::from_f64(gap).try_product(&exact::ExactDyadic::from_f64(0.5))
}

fn split_half_ulp_bounds(point: XY) -> Result<[exact::ExactDyadic; 2]> {
    Ok([split_half_ulp(point.x)?, split_half_ulp(point.y)?])
}

fn split_deviation_within(
    source: &SourceConstraint,
    parameter: &exact::ExactRational,
    point: XY,
    half_ulp: &[exact::ExactDyadic; 2],
) -> Result<bool> {
    let [half_ulp_x, half_ulp_y] = half_ulp;
    let exact_x = exact_source_coordinate(source.start.xy().x, source.end.xy().x, parameter)?;
    let exact_y = exact_source_coordinate(source.start.xy().y, source.end.xy().y, parameter)?;
    let error_x = exact::ExactRational::from_f64(point.x).try_subtract(&exact_x)?;
    let error_y = exact::ExactRational::from_f64(point.y).try_subtract(&exact_y)?;
    let zero = exact::ExactRational::zero();
    let abs_x = if error_x.sign() == ExactSign::Negative {
        zero.try_subtract(&error_x)?
    } else {
        error_x
    };
    let abs_y = if error_y.sign() == ExactSign::Negative {
        zero.try_subtract(&error_y)?
    } else {
        error_y
    };
    Ok(
        abs_x.try_cmp(&exact::ExactRational::from_dyadic(half_ulp_x.clone()))?
            != std::cmp::Ordering::Greater
            && abs_y.try_cmp(&exact::ExactRational::from_dyadic(half_ulp_y.clone()))?
                != std::cmp::Ordering::Greater,
    )
}

fn validate_piece_endpoint(
    source: &SourceConstraint,
    parameter: &exact::ExactRational,
    point: XY,
    site: usize,
    provenance: &[NodeProvenance],
    certificates: &[CertifiedBoundarySplit],
) -> Result<()> {
    match provenance.get(site) {
        Some(NodeProvenance::ConstraintSplit { certificate }) => {
            let Some(certificate) = certificates.get(*certificate) else {
                return Err(GeometryErrorKind::triangulation(
                    "source constraint references a missing rounded split proof",
                ));
            };
            if certificate.source != source.id
                || certificate.parameter.try_cmp(parameter)? != std::cmp::Ordering::Equal
                || certificate.point != point
            {
                return Err(GeometryErrorKind::triangulation(
                    "source constraint interval does not match its rounded split",
                ));
            }
        },
        Some(NodeProvenance::SourceSite { .. } | NodeProvenance::Generated) => {
            if exact::orient2d(source.start.xy(), source.end.xy(), point) != ExactSign::Zero {
                return Err(GeometryErrorKind::triangulation(
                    "source constraint has an off-line emitted vertex",
                ));
            }
            let actual =
                exact::ExactRational::from_segment(source.start.xy(), source.end.xy(), point)?;
            if actual.try_cmp(parameter)? != std::cmp::Ordering::Equal {
                return Err(GeometryErrorKind::triangulation(
                    "source constraint interval does not match its emitted vertex",
                ));
            }
        },
        None => {
            return Err(GeometryErrorKind::triangulation(
                "source constraint references missing provenance",
            ));
        },
    }
    Ok(())
}

struct PreparedSplit {
    candidate: XY,
    located: Located,
    source: Option<PreparedSourceSplit>,
}

struct PreparedSourceSplit {
    piece: SourcePiece,
    certificate: CertifiedBoundarySplit,
}

/// The mesh with its constraint flags, one per halfedge slot.
pub(super) struct ConstrainedMesh {
    pub(super) mesh: Mesh,
    pub(super) constrained: Vec<bool>,
    source_constraints: Vec<SourceConstraint>,
    source_pieces: HashMap<(usize, usize), SourcePiece>,
    provenance: Vec<NodeProvenance>,
    split_certificates: Vec<CertifiedBoundarySplit>,
}

struct FaceLabels(Vec<Option<bool>>);

impl FaceLabels {
    fn new(count: usize) -> Self {
        Self(vec![None; count])
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "FaceId is enumerated from the same live mesh whose face count sizes this label table"
    )]
    fn get(&self, face: FaceId) -> Option<bool> {
        self.0[face.index()]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "FaceId is enumerated from the same live mesh whose face count sizes this label table"
    )]
    fn set(&mut self, face: FaceId, value: bool) {
        self.0[face.index()] = Some(value);
    }

    fn into_inside(self) -> Vec<bool> {
        self.0
            .into_iter()
            .map(|value| value.unwrap_or(false))
            .collect()
    }
}

impl ConstrainedMesh {
    #[expect(
        clippy::indexing_slicing,
        reason = "HalfedgeId comes from this mesh and the constraint column is admitted to its edge count"
    )]
    fn is_constrained(&self, edge: HalfedgeId) -> bool {
        self.constrained[edge.index()]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "HalfedgeId comes from this mesh and the constraint column is admitted to its edge count"
    )]
    const fn set_constrained(flags: &mut [bool], edge: HalfedgeId, value: bool) {
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
        let constrained = vec![false; mesh.edge_count()];
        let source_constraints = sources.to_vec();
        let source_pieces = crate::collections::try_hash_map_with_capacity(sources.len())?;
        let provenance = occurrences
            .iter()
            .copied()
            .map(|occurrence| NodeProvenance::SourceSite { occurrence })
            .collect();
        let mut this = Self {
            mesh,
            constrained,
            source_constraints,
            source_pieces,
            provenance,
            split_certificates: Vec::new(),
        };
        let xy = |site| site_xy(points, site);
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
        let mut legalization = Vec::new();
        self.recover_constraint(xy, u, v, source, lo, hi, &mut legalization)?;
        self.mesh
            .try_legalize_constrained_from(xy, &mut self.constrained, legalization)
    }

    fn recover_constraint(
        &mut self,
        xy: &impl Fn(usize) -> XY,
        u: usize,
        v: usize,
        source: usize,
        lo: exact::ExactRational,
        hi: exact::ExactRational,
        legalization: &mut Vec<HalfedgeId>,
    ) -> Result<()> {
        if u == v {
            return Ok(());
        }
        if let Some(slot) = self.mesh.edge_slot(u, v) {
            self.record_source_piece(u, v, source, lo, hi);
            self.mark(slot);
            return Ok(());
        }
        // Walk the fan around `u` to find the triangle the segment leaves
        // through, or a vertex it passes through exactly.
        let (pu, pv) = (xy(u), xy(v));
        let mut crossing = Vec::new();
        let mut start = None;
        for slot in self.mesh.outgoing(u) {
            let a = self.mesh.edge_end(slot);
            let b = self.mesh.edge_apex(slot);
            // Segment u->v exits triangle (u, a, b) through edge a->b when v
            // is left of u->a and right of u->b (strictly), or through a
            // vertex when collinear.
            let sa = exact::orient2d(pu, xy(a), pv);
            let sb = exact::orient2d(pu, xy(b), pv);
            if sa == ExactSign::Zero && between(pu, pv, xy(a)) {
                let split = interpolate_source_parameter(&lo, &hi, pu, pv, xy(a))?;
                self.certify_source_boundary_point(source, &split, xy(a))?;
                self.recover_constraint(xy, u, a, source, lo, split.clone(), legalization)?;
                self.recover_constraint(xy, a, v, source, split, hi, legalization)?;
                return Ok(());
            }
            if sb == ExactSign::Zero && between(pu, pv, xy(b)) {
                let split = interpolate_source_parameter(&lo, &hi, pu, pv, xy(b))?;
                self.certify_source_boundary_point(source, &split, xy(b))?;
                self.recover_constraint(xy, u, b, source, lo, split.clone(), legalization)?;
                self.recover_constraint(xy, b, v, source, split, hi, legalization)?;
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
            let pa = xy(apex);
            let s = exact::orient2d(pu, pv, pa);
            if s == ExactSign::Zero {
                if !between(pu, pv, pa) {
                    return Err(GeometryErrorKind::triangulation(
                        "constraint walk met a collinear vertex outside the constraint",
                    ));
                }
                let split = interpolate_source_parameter(&lo, &hi, pu, pv, pa)?;
                self.certify_source_boundary_point(source, &split, pa)?;
                // Passes exactly through `apex`: constrain u->apex by flipping
                // what we have so far, then continue from apex.
                self.flip_until_clear(xy, u, apex, crossing, legalization)?;
                self.recover_constraint(xy, u, apex, source, lo, split.clone(), legalization)?;
                self.recover_constraint(xy, apex, v, source, split, hi, legalization)?;
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
        self.flip_until_clear(xy, u, v, crossing, legalization)?;
        self.record_source_piece(u, v, source, lo, hi);
        Ok(())
    }

    fn source(&self, id: usize) -> Result<&SourceConstraint> {
        self.source_constraints.get(id).ok_or_else(|| {
            GeometryErrorKind::triangulation("constraint references a missing source").into()
        })
    }

    fn certify_source_boundary_point(
        &self,
        source_id: usize,
        parameter: &exact::ExactRational,
        point: XY,
    ) -> Result<()> {
        let source = self.source(source_id)?;
        if exact::orient2d(source.start.xy(), source.end.xy(), point) == ExactSign::Zero {
            return Ok(());
        }
        let half_ulp = split_half_ulp_bounds(point)?;
        if !split_deviation_within(source, parameter, point, &half_ulp)? {
            return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
        }
        Ok(())
    }

    fn record_source_piece(
        &mut self,
        start_site: usize,
        end_site: usize,
        source: usize,
        lo: exact::ExactRational,
        hi: exact::ExactRational,
    ) {
        self.source_pieces
            .entry(ordered(start_site, end_site))
            .or_insert(SourcePiece {
                source,
                start_site,
                end_site,
                lo,
                hi,
            });
    }

    /// Flip the crossed edges until none cross `u -> v`, then mark the edge.
    fn flip_until_clear(
        &mut self,
        xy: &impl Fn(usize) -> XY,
        u: usize,
        v: usize,
        mut crossing: Vec<(HalfedgeId, (usize, usize))>,
        legalization: &mut Vec<HalfedgeId>,
    ) -> Result<()> {
        let (pu, pv) = (xy(u), xy(v));
        let mut steps = 0_usize;
        while let Some((candidate, pair)) = crossing.pop() {
            steps = steps.saturating_add(1);
            if steps >= 1_000_000 {
                return Err(GeometryErrorKind::triangulation(
                    "constraint insertion did not converge",
                ));
            }
            let edge = if {
                let (start, end) = self.mesh.edge_vertices(candidate);
                ordered(start, end) == pair
            } {
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
            let first_a = exact::orient2d(xy(c), xy(d), xy(a));
            let first_b = exact::orient2d(xy(c), xy(d), xy(b));
            let second_c = exact::orient2d(xy(a), xy(b), xy(c));
            let second_d = exact::orient2d(xy(a), xy(b), xy(d));
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
            if segments_properly_cross(pu, pv, xy(new_start), xy(new_end)) {
                crossing.insert(0, (new_edge, ordered(new_start, new_end)));
            }
        }
        if let Some(slot) = self.mesh.edge_slot(u, v) {
            self.mark(slot);
            Ok(())
        } else {
            Err(GeometryErrorKind::triangulation(
                "constraint insertion did not produce an edge",
            ))
        }
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
    pub(super) fn inside_faces(&self) -> Vec<bool> {
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
            let inside = self.inside_faces();
            let xy = |site, points: &[Point]| site_xy(points, site);
            let mut bad = None;
            #[cfg(test)]
            update_work(|counters| {
                counters.bad_face_scans = counters.bad_face_scans.saturating_add(1);
            });
            for (face, &kept) in self.mesh.face_ids().zip(&inside) {
                if !kept || !self.mesh.alive(face) {
                    continue;
                }
                let [a, b, c] = self.mesh.triangle(face);
                let (pa, pb, pc) = (xy(a, points), xy(b, points), xy(c, points));
                let violates = min_angle
                    .is_some_and(|limit| triangle_min_angle_degrees(pa, pb, pc) < limit)
                    || max_area
                        .is_some_and(|limit| open_xy_cycle_magnitude(&[pa, pb, pc]).get() > limit);
                if violates {
                    bad = Some(face);
                    break;
                }
            }
            let Some(face) = bad else {
                return Ok(true);
            };
            if inserted >= limit {
                return Ok(false);
            }
            let [a, b, c] = self.mesh.triangle(face);
            let (pa, pb, pc) = (xy(a, points), xy(b, points), xy(c, points));
            // No finite binary64 can represent this center. Ruppert can
            // still make progress with the explicitly chosen longest-edge
            // midpoint; this is not a recovery for construction errors.
            let center =
                circumcenter(pa, pb, pc)?.unwrap_or_else(|| longest_edge_midpoint(pa, pb, pc));
            // A constrained edge or the hull stops this center's location walk.
            // Rounded source splits carry a half-ULP incidence certificate.
            let encroached = self.mesh.edge_ids().find(|&edge| {
                self.is_constrained(edge) && self.mesh.alive(edge.face()) && {
                    let (s, t2) = self.mesh.edge_vertices(edge);
                    in_diametral_circle(xy(s, points), xy(t2, points), center)
                }
            });
            let xy_now = |s| site_xy(points, s);
            let placed = if let Some(edge) = encroached {
                self.prepare_split(&xy_now, points, edge)?
            } else {
                match self
                    .mesh
                    .locate_bounded(&xy_now, face, center, &self.constrained)
                {
                    Ok(Located::OnEdge(edge)) if self.is_constrained(edge) => {
                        self.prepare_split(&xy_now, points, edge)?
                    },
                    Ok(located) => Some(PreparedSplit {
                        candidate: center,
                        located,
                        source: None,
                    }),
                    Err(edge) => self.prepare_split(&xy_now, points, edge)?,
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
                return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
            }
            let site = points.len();
            // The cavity reuses slots, so remember constraints as vertex pairs
            // and re-mark from those once the mesh has settled; a split
            // constraint becomes its two halves.
            let mut pairs = self.constrained_pairs()?;
            if let Some(split) = &source {
                pairs.remove(&ordered(split.piece.start_site, split.piece.end_site));
                pairs.insert(ordered(split.piece.start_site, site));
                pairs.insert(ordered(site, split.piece.end_site));
                self.source_pieces.reserve(2);
                self.split_certificates.reserve(1);
            }
            points.reserve(1);
            self.provenance.reserve(1);
            points.push(Point::new_raw_xy(candidate.x, candidate.y));
            self.mesh.admit_vertex(site);
            let source = source.map(|split| {
                let certificate = self.split_certificates.len();
                let parameter = split.certificate.parameter.clone();
                self.split_certificates.push(split.certificate);
                (split.piece, parameter, certificate)
            });
            self.provenance.push(source.as_ref().map_or(
                NodeProvenance::Generated,
                |(_, _, certificate)| NodeProvenance::ConstraintSplit {
                    certificate: *certificate,
                },
            ));
            let xy2 = |s| site_xy(points, s);
            let outside_seeds = if matches!(located, Located::Outside) {
                let (_, seeds) = self.mesh.insert_outside(&xy2, site)?;
                Some(seeds)
            } else {
                let mut skip = Vec::with_capacity(self.constrained.len());
                skip.extend_from_slice(&self.constrained);
                if let Some(split) = &source {
                    let old_edge = self
                        .mesh
                        .edge_slot(split.0.start_site, split.0.end_site)
                        .ok_or_else(|| {
                            GeometryErrorKind::triangulation(
                                "constrained source edge disappeared before splitting",
                            )
                        })?;
                    Self::set_constrained(&mut skip, old_edge, false);
                    let twin = self.mesh.twin(old_edge);
                    if twin != EMPTY {
                        Self::set_constrained(&mut skip, HalfedgeId::from_index(twin), false);
                    }
                }
                self.mesh.insert_constrained(&xy2, site, located, &skip)?;
                None
            };
            self.remark(&pairs);
            if let Some(seeds) = outside_seeds {
                self.mesh
                    .try_legalize_constrained_from(&xy2, &mut self.constrained, seeds)?;
            }
            if let Some((piece, parameter, _)) = source {
                self.source_pieces
                    .remove(&ordered(piece.start_site, piece.end_site));
                self.insert_constraint(
                    &xy2,
                    piece.start_site,
                    site,
                    piece.source,
                    piece.lo.clone(),
                    parameter.clone(),
                )?;
                self.insert_constraint(
                    &xy2,
                    site,
                    piece.end_site,
                    piece.source,
                    parameter,
                    piece.hi,
                )?;
            }
            inserted = inserted.wrapping_add(1);
        }
    }

    fn prepare_split(
        &self,
        xy: &impl Fn(usize) -> XY,
        points: &[Point],
        e: HalfedgeId,
    ) -> Result<Option<PreparedSplit>> {
        let (start, end) = self.mesh.edge_vertices(e);
        if self.is_constrained(e) {
            let Some(piece) = self.source_pieces.get(&ordered(start, end)) else {
                return Err(GeometryErrorKind::triangulation(
                    "constrained edge has no source interval",
                ));
            };
            let requested = piece.lo.try_midpoint(&piece.hi)?;
            let certificate = self.certify_source_split(piece, &requested)?;
            let candidate = certificate.point;
            let start = xy(piece.start_site);
            let end = xy(piece.end_site);
            // Splitting the current edge in its interior preserves the mesh
            // embedding; bent rounded segments require the global incidence scan.
            if exact::orient2d(start, end, candidate) != ExactSign::Zero
                || !between(start, end, candidate)
            {
                self.certify_split_incidence(xy, piece, candidate)?;
            }
            let located = self.locate_boundary_split(xy, e, candidate)?;
            return Ok(Some(PreparedSplit {
                candidate,
                located,
                source: Some(PreparedSourceSplit {
                    piece: piece.clone(),
                    certificate,
                }),
            }));
        }
        let ps = site_xy(points, start);
        let pt = site_xy(points, end);
        let midpoint = XY::new(f64::midpoint(ps.x, pt.x), f64::midpoint(ps.y, pt.y));
        let located = match self
            .mesh
            .locate_bounded(xy, e.face(), midpoint, &self.constrained)
        {
            Ok(Located::OnEdge(slot)) if slot == e || slot.index() == self.mesh.twin(e) => {
                Located::OnEdge(e)
            },
            Ok(Located::Inside(face)) => Located::Inside(face),
            Ok(Located::OnVertex | Located::OnEdge(_) | Located::Outside) | Err(_) => {
                return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
            },
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
        requested: &exact::ExactRational,
    ) -> Result<CertifiedBoundarySplit> {
        let source = self.source(piece.source)?;
        if requested.try_cmp(&piece.lo)? != std::cmp::Ordering::Greater
            || requested.try_cmp(&piece.hi)? != std::cmp::Ordering::Less
        {
            return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
        }
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
        let half_ulp = split_half_ulp_bounds(candidate)?;
        if exact::orient2d(source.start.xy(), source.end.xy(), candidate) != ExactSign::Zero
            && !split_deviation_within(source, requested, candidate, &half_ulp)?
        {
            return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
        }
        Ok(CertifiedBoundarySplit {
            source: piece.source,
            parameter: requested.clone(),
            point: candidate,
            half_ulp,
        })
    }

    fn certify_split_incidence(
        &self,
        xy: &impl Fn(usize) -> XY,
        piece: &SourcePiece,
        candidate: XY,
    ) -> Result<()> {
        let start = xy(piece.start_site);
        let end = xy(piece.end_site);
        let old_pair = ordered(piece.start_site, piece.end_site);
        let subsegments = [
            (start, candidate, piece.start_site),
            (candidate, end, piece.end_site),
        ];
        for edge in self.mesh.edge_ids() {
            if !self.is_constrained(edge) || !self.mesh.alive(edge.face()) {
                continue;
            }
            let twin = self.mesh.twin(edge);
            if twin != EMPTY && twin < edge.index() {
                continue;
            }
            let (other_start, other_end) = self.mesh.edge_vertices(edge);
            if ordered(other_start, other_end) == old_pair {
                continue;
            }
            let (a, b) = (xy(other_start), xy(other_end));
            if on_segment(a, b, candidate) {
                return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
            }
            for (left, right, shared_site) in subsegments {
                if segments_properly_cross(left, right, a, b) {
                    return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
                }
                if (other_start != shared_site && on_segment(left, right, a))
                    || (other_end != shared_site && on_segment(left, right, b))
                {
                    return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
                }
            }
        }
        Ok(())
    }

    /// Locate a rounded source split while permitting traversal across only the
    /// source edge it replaces.
    fn locate_boundary_split(
        &self,
        xy: &impl Fn(usize) -> XY,
        e: HalfedgeId,
        point: XY,
    ) -> Result<Located> {
        let twin = self.mesh.twin(e);
        let twin = (twin != EMPTY).then(|| HalfedgeId::from_index(twin));
        match self.mesh.locate_bounded(xy, e.face(), point, &[]) {
            Ok(Located::OnVertex | Located::Outside) => {
                Err(GeometryErrorKind::unrepresentable_output("triangulate"))
            },
            Ok(Located::Inside(face)) => Ok(Located::Inside(face)),
            Ok(Located::OnEdge(slot)) if slot == e || twin == Some(slot) => Ok(Located::OnEdge(e)),
            Ok(Located::OnEdge(slot)) if self.is_constrained(slot) => {
                Err(GeometryErrorKind::unrepresentable_output("triangulate"))
            },
            Ok(Located::OnEdge(slot)) => Ok(Located::OnEdge(slot)),
            Err(_) if self.mesh.outside_hull(xy, point) => Ok(Located::Outside),
            Err(_) => Err(GeometryErrorKind::unrepresentable_output("triangulate")),
        }
    }

    fn constrained_pairs(&self) -> Result<HashSet<(usize, usize)>> {
        #[cfg(test)]
        update_work(|counters| {
            counters.constraint_reconstruction_passes =
                counters.constraint_reconstruction_passes.saturating_add(1);
        });
        let mut pairs = crate::collections::try_hash_set_with_capacity(self.mesh.edge_count())?;
        for edge in self.mesh.edge_ids() {
            if self.is_constrained(edge) && self.mesh.alive(edge.face()) {
                let (start, end) = self.mesh.edge_vertices(edge);
                pairs.insert(ordered(start, end));
            }
        }
        Ok(pairs)
    }

    /// Re-derive the per-slot flags from the constrained vertex pairs after
    /// an insertion has renumbered the cavity's slots.
    fn remark(&mut self, pairs: &HashSet<(usize, usize)>) {
        #[cfg(test)]
        update_work(|counters| {
            counters.flag_remark_passes = counters.flag_remark_passes.saturating_add(1);
        });
        self.constrained = vec![false; self.mesh.edge_count()];
        for edge in self.mesh.edge_ids() {
            if !self.mesh.alive(edge.face()) {
                continue;
            }
            if pairs.contains(&ordered(
                self.mesh.edge_start(edge),
                self.mesh.edge_end(edge),
            )) {
                Self::set_constrained(&mut self.constrained, edge, true);
            }
        }
    }

    fn validate_source_chains(&self, points: &[Point], pieces: &[SourcePiece]) -> Result<()> {
        let one = exact::ExactRational::one();
        let constrained = self.constrained_pairs()?;
        let has_split_certificates = !self.split_certificates.is_empty();
        let mut remaining = pieces;
        for source in &self.source_constraints {
            let count = remaining
                .iter()
                .take_while(|piece| {
                    #[cfg(test)]
                    update_work(|work| {
                        work.source_piece_visits = work.source_piece_visits.saturating_add(1);
                    });
                    piece.source == source.id
                })
                .count();
            let (pieces, rest) = remaining.split_at(count);
            remaining = rest;
            if pieces.is_empty() {
                return Err(GeometryErrorKind::triangulation(format!(
                    "source constraint {} has no emitted boundary chain",
                    source.id
                )));
            }
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
                if has_split_certificates {
                    validate_piece_endpoint(
                        source,
                        &piece.lo,
                        start.xy(),
                        piece.start_site,
                        &self.provenance,
                        &self.split_certificates,
                    )?;
                    validate_piece_endpoint(
                        source,
                        &piece.hi,
                        end.xy(),
                        piece.end_site,
                        &self.provenance,
                        &self.split_certificates,
                    )?;
                } else {
                    let start_on_source =
                        exact::orient2d(source.start.xy(), source.end.xy(), start.xy())
                            == ExactSign::Zero;
                    let end_on_source =
                        exact::orient2d(source.start.xy(), source.end.xy(), end.xy())
                            == ExactSign::Zero;
                    if !start_on_source || !end_on_source {
                        return Err(GeometryErrorKind::triangulation(
                            "source constraint has an off-line emitted vertex",
                        ));
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
                    {
                        return Err(GeometryErrorKind::triangulation(
                            "source constraint interval does not match its emitted vertex",
                        ));
                    }
                }
                if !constrained.contains(&ordered(piece.start_site, piece.end_site)) {
                    return Err(GeometryErrorKind::triangulation(format!(
                        "source constraint {} does not match its emitted edge",
                        source.id
                    )));
                }
                cursor = piece.hi.clone();
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
        mut self,
        points: Vec<Point>,
        refinement: super::CdtRefinement,
        force_xy: bool,
    ) -> Result<ConstrainedMeshPlan> {
        let mut constraint_pieces = Vec::with_capacity(self.source_pieces.len());
        constraint_pieces.extend(self.source_pieces.drain().map(|(_, piece)| piece));
        crate::geometry::try_sort_by(&mut constraint_pieces, compare_source_pieces)?;
        self.validate_source_chains(&points, &constraint_pieces)?;
        let inside = self.inside_faces();
        let mut faces = Vec::with_capacity(self.mesh.triangle_count());
        for (face, &kept) in self.mesh.face_ids().zip(&inside) {
            if kept && self.mesh.alive(face) {
                faces.push(self.mesh.triangle(face));
            }
        }
        super::exact_delaunay::canonicalize_triangles(&mut faces, |site| site_xy(&points, site));
        Ok(ConstrainedMeshPlan {
            vertices: points,
            faces,
            provenance: self.provenance,
            source_constraints: self.source_constraints,
            constraint_pieces,
            split_certificates: self.split_certificates,
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

fn interpolate_source_parameter(
    lo: &exact::ExactRational,
    hi: &exact::ExactRational,
    start: XY,
    end: XY,
    point: XY,
) -> Result<exact::ExactRational> {
    let local = exact::ExactRational::from_segment(start, end, point)?;
    lo.try_add(&hi.try_subtract(lo)?.try_product(&local)?)
}

/// Whether `p`, collinear with `a -> b`, lies strictly between them.
fn between(a: XY, b: XY, p: XY) -> bool {
    let (lo_x, hi_x) = if a.x < b.x { (a.x, b.x) } else { (b.x, a.x) };
    let (lo_y, hi_y) = if a.y < b.y { (a.y, b.y) } else { (b.y, a.y) };
    lo_x <= p.x && p.x <= hi_x && lo_y <= p.y && p.y <= hi_y && p != a && p != b
}

fn on_segment(a: XY, b: XY, p: XY) -> bool {
    p == a || p == b || (exact::orient2d(a, b, p) == ExactSign::Zero && between(a, b, p))
}

fn opposite_nonzero(a: ExactSign, b: ExactSign) -> bool {
    a != ExactSign::Zero && b != ExactSign::Zero && a != b
}

fn segments_properly_cross(a: XY, b: XY, c: XY, d: XY) -> bool {
    opposite_nonzero(exact::orient2d(a, b, c), exact::orient2d(a, b, d))
        && opposite_nonzero(exact::orient2d(c, d, a), exact::orient2d(c, d, b))
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
    exact::circumcenter(a, b, c)?.finite_rounding()
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

    #[test]
    fn public_constrained_source_chain_work_is_linear() {
        use pyo3::prelude::*;
        use pyo3::types::{PyDict, PyModule};

        crate::test_support::initialize_python();
        Python::attach(|py| {
            let module = PyModule::new(py, "gometry._lib").unwrap();
            crate::lib(&module).unwrap();
            let options = PyDict::new(py);
            options.set_item("method", "constrained").unwrap();
            for count in [128_u32, 512] {
                let points = (0..count)
                    .map(|index| {
                        let angle = f64::from(index) * std::f64::consts::TAU / f64::from(count);
                        format!("{} {}", angle.cos(), angle.sin())
                    })
                    .collect::<Vec<_>>();
                let ring = points
                    .iter()
                    .chain(points.first())
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(",");
                let polygon = module
                    .getattr("from_wkt")
                    .unwrap()
                    .call1((format!("POLYGON(({ring}))"),))
                    .unwrap();
                let _ = take_refinement_work_counters();
                let triangles = polygon
                    .call_method("triangulate", (), Some(&options))
                    .unwrap();
                assert_eq!(triangles.len().unwrap(), count as usize - 2);
                let work = take_refinement_work_counters();
                assert!(
                    work.source_piece_visits <= count as usize * 2,
                    "{count} source edges inspected {} pieces",
                    work.source_piece_visits
                );
            }
        });
    }

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
            mesh.mesh.edge_slot(0, 2).is_some(),
            "the constraint 0-2 is an edge"
        );
        let slot = mesh.mesh.edge_slot(0, 2).unwrap();
        assert!(mesh.constrained[slot.index()]);
    }

    /// The constraint flags mark exactly the constrained vertex pairs, on both
    /// halves of each edge.
    fn assert_flags_match(mesh: &ConstrainedMesh, edges: &[[usize; 2]]) {
        for edge in mesh.mesh.edge_ids() {
            if !mesh.mesh.alive(edge.face()) {
                continue;
            }
            let (start, end) = mesh.mesh.edge_vertices(edge);
            let pair = ordered(start, end);
            let expected = edges.iter().any(|&[u, v]| ordered(u, v) == pair);
            assert_eq!(
                mesh.constrained[edge.index()],
                expected,
                "slot {} pair {pair:?}",
                edge.index()
            );
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
        assert!(matches!(
            result.as_ref(),
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::Geometry(GeometryErrorKind::Triangulation(message))
                        if message.as_ref() == "constrained triangulation boundaries cross"
                )
        ));
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
        let inside = mesh.inside_faces();
        let xy = |s: usize| pts[s].xy();
        for (face, &kept) in mesh.mesh.face_ids().zip(&inside) {
            if !mesh.mesh.alive(face) {
                continue;
            }
            let [a, b, c] = mesh.mesh.triangle(face);
            let cx = (xy(a).x + xy(b).x + xy(c).x) / 3.0;
            let cy = (xy(a).y + xy(b).y + xy(c).y) / 3.0;
            let in_hole = (4.0..6.0).contains(&cx) && (4.0..6.0).contains(&cy);
            assert_eq!(
                kept,
                !in_hole,
                "face {} centroid ({cx}, {cy})",
                face.index()
            );
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
        let inside = mesh.inside_faces();
        for (face, &kept) in mesh.mesh.face_ids().zip(&inside) {
            if kept && mesh.mesh.alive(face) {
                let [a, b, c] = mesh.mesh.triangle(face);
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
            .expect_err("the rounded midpoint collides with an existing source vertex");
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
                operation: "triangulate"
            })
        ));
        assert_eq!(pts, before, "rejected source split does not append a site");
    }

    #[test]
    fn rounded_point_inside_the_adjacent_face_is_a_boundary_split_location() {
        let next = 1.0_f64.next_up();
        let pts = points(&[(1.0, 0.0), (next, 1.0), (0.0, 0.5)]);
        let mesh = ConstrainedMesh::build(&pts, &[[0, 1], [1, 2], [2, 0]]).unwrap();
        let edge = mesh.mesh.edge_slot(0, 1).unwrap();
        let xy = |site: usize| pts[site].xy();
        assert!(matches!(
            mesh.locate_boundary_split(&xy, edge, XY::new(1.0, 0.5)),
            Ok(Located::Inside(_)),
        ));
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
}

/// Inputs the public suite once broke this engine with: a clockwise ring
/// whose constraints lie on the hull, a sliver whose circumcenters leave the
/// hull, and reciprocal extents whose float circumcenter cancels.
#[cfg(test)]
mod regressions {
    use super::*;

    fn pts(xy: &[(f64, f64)]) -> Vec<Point> {
        xy.iter().map(|&(x, y)| Point::new_raw_xy(x, y)).collect()
    }
    fn ring_edges(n: usize) -> Vec<[usize; 2]> {
        (0..n)
            .map(|i| [i, i.wrapping_add(1).rem_euclid(n)])
            .collect()
    }
    fn inside_count(cdt: &ConstrainedMesh) -> usize {
        let inside = cdt.inside_faces();
        cdt.mesh
            .face_ids()
            .zip(inside)
            .filter(|(face, kept)| cdt.mesh.alive(*face) && *kept)
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
