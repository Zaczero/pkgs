//! Bounded Ruppert refinement and certified source-boundary splits.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};

use rstar::{AABB, RTree, RTreeObject};

use super::super::exact_delaunay::{
    EMPTY, FaceId, FaceSide, HalfedgeId, Located, LocationOutcome, Mesh, MeshCoordinates as _,
};
use super::plan::{
    exact_source_coordinate, round_source_coordinate, split_deviation_within, split_half_ulp_bounds,
};
#[cfg(test)]
use super::update_work;
use super::{
    BadTriangle, CdtCoordinates, CertifiedBoundarySplit, ConstrainedMesh, ConstraintCircle,
    ConstraintIndex, ConstraintPiece, ConstraintPieces, FaceLabels, NodeProvenance,
    PreparedConstraintSplit, PreparedSplit, RefinementConstruction, ordered,
};
use crate::error::Result;
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::tessellation::exact;
use crate::geometry::{GeometryErrorKind, Point, XY, open_xy_cycle_magnitude};

impl PartialEq for BadTriangle {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for BadTriangle {}

impl PartialOrd for BadTriangle {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BadTriangle {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.angle
            .total_cmp(&other.angle)
            .then_with(|| self.area.total_cmp(&other.area))
            .then_with(|| {
                self.corners
                    .iter()
                    .zip(other.corners)
                    .map(|(&left, right)| super::exact_delaunay::compare_xy(right, left))
                    .find(|order| !order.is_eq())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| self.triangle.cmp(&other.triangle))
    }
}

impl BadTriangle {
    fn new(
        face: FaceId,
        triangle: [usize; 3],
        coordinates: &CdtCoordinates,
        min_angle: Option<f64>,
        max_area: Option<f64>,
    ) -> Result<Option<Self>> {
        #[cfg(test)]
        update_work(|work| {
            work.quality_checks = work.quality_checks.saturating_add(1);
        });
        let mut corners = coordinates.metric_frame(triangle)?;
        if max_area.is_some() {
            corners
                .sort_unstable_by(|&left, &right| super::exact_delaunay::compare_xy(left, right));
        }
        let [a, b, c] = corners;
        let angle_bad = min_angle.is_some_and(|limit| triangle_min_angle_degrees(a, b, c) < limit);
        let area = max_area.map_or(0.0, |_| open_xy_cycle_magnitude(&[a, b, c]).get());
        let area_bad = if let Some(limit) = max_area {
            coordinates.triangle_exceeds_area(triangle, limit)?
        } else {
            false
        };
        if !angle_bad && !area_bad {
            return Ok(None);
        }
        if max_area.is_none() {
            corners
                .sort_unstable_by(|&left, &right| super::exact_delaunay::compare_xy(left, right));
        }
        let [a, b, c] = corners;
        Ok(Some(Self {
            face,
            triangle,
            corners,
            angle: if angle_bad {
                radius_edge_ratio(a, b, c)
            } else {
                0.0
            },
            area,
        }))
    }
}

impl FaceLabels {
    fn from_inside(mesh: &Mesh, inside: &[bool]) -> Self {
        let mut faces = Vec::with_capacity(mesh.triangle_count());
        for (face, &inside) in mesh.face_ids().zip(inside) {
            faces.push(mesh.alive(face).then_some(inside));
        }
        Self(faces)
    }

    fn label(&mut self, face: FaceId, value: bool) -> Result<bool> {
        let Some(inside) = self.0.get_mut(face.index()) else {
            return Err(GeometryErrorKind::triangulation(
                "refinement label references an absent face",
            ));
        };
        if let Some(previous) = inside {
            if *previous != value {
                return Err(GeometryErrorKind::triangulation(
                    "refinement boundary has inconsistent inside labels",
                ));
            }
            return Ok(false);
        }
        *inside = Some(value);
        Ok(true)
    }

    fn refresh(
        &mut self,
        mesh: &Mesh,
        constrained: &[bool],
        changed: &mut Vec<FaceId>,
    ) -> Result<()> {
        changed.sort_unstable_by_key(|face| face.index());
        changed.dedup();
        self.0.resize(mesh.triangle_count(), None);
        for &face in changed.iter() {
            let previous = self.0.get_mut(face.index()).ok_or_else(|| {
                GeometryErrorKind::triangulation("refinement changed a missing face")
            })?;
            *previous = None;
        }
        let mut queue = VecDeque::new();
        for &face in changed.iter() {
            if !mesh.alive(face) {
                continue;
            }
            for side in FaceSide::ALL {
                let edge = HalfedgeId::in_face(face, side);
                let twin = mesh.twin(edge);
                let across = if twin == EMPTY {
                    Some(false)
                } else {
                    self.get(HalfedgeId::from_index(twin).face())
                };
                if let Some(across) = across {
                    let label = across ^ Mesh::side_value(constrained, edge);
                    if self.label(face, label)? {
                        queue.push_back((face, label));
                    }
                }
            }
        }
        while let Some((face, inside)) = queue.pop_front() {
            for side in FaceSide::ALL {
                let edge = HalfedgeId::in_face(face, side);
                let twin = mesh.twin(edge);
                if twin == EMPTY {
                    continue;
                }
                let neighbor = HalfedgeId::from_index(twin).face();
                if self.get(neighbor).is_none() {
                    let label = inside ^ Mesh::side_value(constrained, edge);
                    if self.label(neighbor, label)? {
                        queue.push_back((neighbor, label));
                    }
                }
            }
        }
        for &face in changed.iter() {
            if mesh.alive(face) && self.get(face).is_none() {
                return Err(GeometryErrorKind::triangulation(
                    "refinement could not label a changed face",
                ));
            }
        }
        Ok(())
    }
}

impl RTreeObject for ConstraintCircle {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

impl ConstraintCircle {
    fn new(coordinates: &CdtCoordinates, start: usize, end: usize) -> Result<Self> {
        let sites = ordered(start, end);
        let [start_x, start_y] = coordinates.exact_xy(coordinates.site_query(sites.0));
        let [end_x, end_y] = coordinates.exact_xy(coordinates.site_query(sites.1));
        let dx = end_x.clone().try_subtract(start_x.clone())?.abs();
        let dy = end_y.clone().try_subtract(start_y.clone())?.abs();
        // `(|dx| + |dy|) / 2` is a conservative upper bound for the exact
        // diametral-circle radius, so the tree can only add candidates.
        let radius = dx
            .try_add(dy)?
            .try_product(&exact::ExactDyadic::from_f64(0.5))?;
        let (low_x, high_x) = if start_x.cmp(&end_x).is_le() {
            (start_x, end_x)
        } else {
            (end_x, start_x)
        };
        let (low_y, high_y) = if start_y.cmp(&end_y).is_le() {
            (start_y, end_y)
        } else {
            (end_y, start_y)
        };
        let min_x = exact::ExactRational::from_dyadic(low_x.try_subtract(radius.clone())?)
            .outward_bounds()?;
        let max_x =
            exact::ExactRational::from_dyadic(high_x.try_add(radius.clone())?).outward_bounds()?;
        let min_y = exact::ExactRational::from_dyadic(low_y.try_subtract(radius.clone())?)
            .outward_bounds()?;
        let max_y = exact::ExactRational::from_dyadic(high_y.try_add(radius)?).outward_bounds()?;
        let envelope = AABB::from_corners([min_x.0.next_down(), min_y.0.next_down()], [
            max_x.1.next_up(),
            max_y.1.next_up(),
        ]);
        let mut corners = [coordinates.site_xy(sites.0), coordinates.site_xy(sites.1)];
        if coordinates
            .compare(
                coordinates.site_query(sites.1),
                coordinates.site_query(sites.0),
            )
            .is_lt()
        {
            corners.swap(0, 1);
        }
        Ok(Self {
            sites,
            corners,
            envelope,
        })
    }
}

impl ConstraintIndex {
    fn new(pieces: &ConstraintPieces, coordinates: &CdtCoordinates) -> Result<Self> {
        let circles = pieces
            .values()
            .map(|piece| ConstraintCircle::new(coordinates, piece.start_site, piece.end_site))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            tree: RTree::bulk_load(circles),
        })
    }

    fn encroached(
        &self,
        mesh: &Mesh,
        coordinates: &CdtCoordinates,
        center: XY,
    ) -> Result<Option<HalfedgeId>> {
        let query = coordinates.point_query(center);
        let bounds = coordinates.axis_bounds(query)?;
        let [(min_x, max_x), (min_y, max_y)] = bounds;
        let envelope = AABB::from_corners([min_x, min_y], [max_x, max_y]);
        let mut selected = None;
        for circle in self.tree.locate_in_envelope_intersecting(envelope) {
            if !coordinates.in_diametral_circle(circle.sites.0, circle.sites.1, query)? {
                continue;
            }
            let replace = selected.is_none_or(|previous: &ConstraintCircle| {
                compare_constraint_order(coordinates, circle.sites, previous.sites).is_lt()
            });
            if replace {
                selected = Some(circle);
            }
        }
        selected
            .map(|circle| {
                mesh.edge_slot(circle.sites.0, circle.sites.1)
                    .ok_or_else(|| {
                        GeometryErrorKind::triangulation(
                            "refinement constraint index references an absent edge",
                        )
                    })
            })
            .transpose()
    }

    fn remove(&mut self, coordinates: &CdtCoordinates, piece: &ConstraintPiece) -> Result<()> {
        let circle = ConstraintCircle::new(coordinates, piece.start_site, piece.end_site)?;
        self.tree.remove(&circle);
        Ok(())
    }

    fn insert(&mut self, coordinates: &CdtCoordinates, start: usize, end: usize) -> Result<()> {
        self.tree
            .insert(ConstraintCircle::new(coordinates, start, end)?);
        Ok(())
    }
}

impl FaceLabels {
    pub(super) fn new(count: usize) -> Self {
        Self(vec![None; count])
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "FaceId is enumerated from the same live mesh whose face count sizes this label table"
    )]
    pub(super) fn get(&self, face: FaceId) -> Option<bool> {
        self.0[face.index()]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "FaceId is enumerated from the same live mesh whose face count sizes this label table"
    )]
    pub(super) fn set(&mut self, face: FaceId, value: bool) {
        self.0[face.index()] = Some(value);
    }

    pub(super) fn into_inside(self) -> Vec<bool> {
        self.0
            .into_iter()
            .map(|value| value.unwrap_or(false))
            .collect()
    }
}

fn radius_edge_ratio(a: XY, b: XY, c: XY) -> f64 {
    let scale = [a.x, a.y, b.x, b.y, c.x, c.y]
        .into_iter()
        .map(f64::abs)
        .fold(0.0_f64, f64::max);
    let normalized = |point: XY| XY::new(point.x / scale, point.y / scale);
    let (a, b, c) = (normalized(a), normalized(b), normalized(c));
    let length = |left: XY, right: XY| (left.x - right.x).hypot(left.y - right.y);
    let mut lengths = [length(a, b), length(b, c), length(c, a)];
    lengths.sort_unstable_by(f64::total_cmp);
    let [_, middle, longest] = lengths;
    let cross = ((b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x)).abs();
    if cross == 0.0 {
        f64::INFINITY
    } else {
        middle * longest / (2.0 * cross)
    }
}

fn compare_constraint_order(
    coordinates: &CdtCoordinates,
    left: (usize, usize),
    right: (usize, usize),
) -> Ordering {
    let canonical = |(start, end)| {
        if coordinates
            .compare(coordinates.site_query(start), coordinates.site_query(end))
            .is_le()
        {
            (start, end)
        } else {
            (end, start)
        }
    };
    let (left_start, left_end) = canonical(left);
    let (right_start, right_end) = canonical(right);
    coordinates
        .compare(
            coordinates.site_query(left_start),
            coordinates.site_query(right_start),
        )
        .then_with(|| {
            coordinates.compare(
                coordinates.site_query(left_end),
                coordinates.site_query(right_end),
            )
        })
}

pub(in crate::geometry::tessellation::delaunay) fn triangle_min_angle_degrees(
    a: XY,
    b: XY,
    c: XY,
) -> f64 {
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

fn longest_edge_midpoint_for(
    coordinates: &CdtCoordinates,
    [a, b, c]: [usize; 3],
) -> Result<(XY, [usize; 2])> {
    let [pa, pb, pc] = coordinates.metric_frame([a, b, c])?;
    let length = |left: XY, right: XY| (left.x - right.x).hypot(left.y - right.y);
    let edge = if length(pa, pb) >= length(pb, pc) && length(pa, pb) >= length(pc, pa) {
        (a, b)
    } else if length(pb, pc) >= length(pc, pa) {
        (b, c)
    } else {
        (c, a)
    };
    let edge_sites: [usize; 2] = edge.into();
    let [start, end] = edge_sites;
    Ok((coordinates.rounded_midpoint(start, end)?, edge_sites))
}

impl ConstrainedMesh {
    /// Ruppert refinement of the inside faces to `min_angle` degrees and/or
    /// `max_area`, inserting at most `limit` vertices. `Ok(false)` means the
    /// budget or a topological placement ran out before the mesh met the
    /// request; exact construction failures are returned as `Err`.
    pub(in crate::geometry::tessellation::delaunay) fn refine(
        &mut self,
        points: &mut Vec<Point>,
        min_angle: Option<f64>,
        max_area: Option<f64>,
        limit: usize,
    ) -> Result<bool> {
        let inside = self.inside_faces();
        let mut bad = BinaryHeap::new();
        #[cfg(test)]
        update_work(|counters| {
            counters.bad_face_scans = counters.bad_face_scans.saturating_add(1);
        });
        for (face, &kept) in self.mesh.face_ids().zip(&inside) {
            if kept
                && self.mesh.alive(face)
                && let Some(entry) = BadTriangle::new(
                    face,
                    self.mesh.triangle(face),
                    &self.coordinates,
                    min_angle,
                    max_area,
                )?
            {
                bad.push(entry);
            }
        }
        if bad.is_empty() {
            return Ok(true);
        }
        let mut faces = FaceLabels::from_inside(&self.mesh, &inside);
        let mut circles = ConstraintIndex::new(&self.constraint_pieces, &self.coordinates)?;
        let mut inserted = 0_usize;
        while let Some(entry) = bad.pop() {
            let face = entry.face;
            if !self.mesh.alive(face)
                || self.mesh.triangle(face) != entry.triangle
                || faces.get(face) != Some(true)
            {
                continue;
            }
            if inserted >= limit {
                return Ok(false);
            }
            // No finite binary64 can represent this center. Ruppert can
            // still make progress with the explicitly chosen longest-edge
            // midpoint; this is not a recovery for construction errors.
            let (center, center_construction) =
                if let Some(center) = self.coordinates.circumcenter(entry.triangle)? {
                    (center, RefinementConstruction::Circumcenter)
                } else {
                    let (midpoint, edge) =
                        longest_edge_midpoint_for(&self.coordinates, entry.triangle)?;
                    (midpoint, RefinementConstruction::LongestEdgeMidpoint(edge))
                };
            // A constrained edge or the hull stops this center's location walk.
            // Rounded source splits carry a half-ULP incidence certificate.
            let encroached = circles.encroached(&self.mesh, &self.coordinates, center)?;
            let center_query = self.coordinates.point_query(center);
            let placed = if let Some(edge) = encroached {
                self.prepare_split(&circles, edge)?
            } else {
                match self.mesh.locate_bounded(
                    &self.coordinates,
                    face,
                    center_query,
                    &self.constrained,
                )? {
                    LocationOutcome::Located(Located::OnEdge(edge))
                        if self.is_constrained(edge) =>
                    {
                        self.prepare_split(&circles, edge)?
                    },
                    LocationOutcome::Located(located) => Some(PreparedSplit {
                        candidate: center,
                        located,
                        constraint: None,
                        construction: Some(center_construction),
                    }),
                    LocationOutcome::Blocked(edge) => self.prepare_split(&circles, edge)?,
                }
            };
            let Some(PreparedSplit {
                candidate,
                located,
                constraint,
                construction,
            }) = placed
            else {
                return Ok(false);
            };
            if matches!(located, Located::OnVertex) {
                return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
            }
            let site = points.len();
            if self.coordinates.sites.len() != site {
                return Err(GeometryErrorKind::triangulation(
                    "refinement coordinate table lost its mesh site alignment",
                ));
            }
            if let Some(split) = &constraint {
                let edge = self
                    .mesh
                    .edge_slot(split.piece.start_site, split.piece.end_site)
                    .ok_or_else(|| {
                        GeometryErrorKind::triangulation(
                            "constrained edge disappeared before splitting",
                        )
                    })?;
                Self::set_constrained(&mut self.constrained, edge, false);
                let twin = self.mesh.twin(edge);
                if twin != EMPTY {
                    Self::set_constrained(
                        &mut self.constrained,
                        HalfedgeId::from_index(twin),
                        false,
                    );
                }
                circles.remove(&self.coordinates, &split.piece)?;
                self.constraint_pieces.reserve(2);
                self.split_certificates.reserve(1);
            }
            points.reserve(1);
            self.coordinates.reserve_generated();
            self.provenance.reserve(1);
            let admitted_site = self.coordinates.push_generated(candidate);
            debug_assert_eq!(admitted_site, site);
            points.push(Point::new_raw_xy(candidate.x, candidate.y));
            self.mesh.admit_vertex(site);
            let constraint = if let Some(split) = constraint {
                let certificate = self.split_certificates.len();
                let parameter = split.certificate.parameter.clone();
                self.split_certificates.push(split.certificate);
                Some((split.piece, parameter, certificate))
            } else {
                None
            };
            let provenance = if let Some((_, _, certificate)) = &constraint {
                NodeProvenance::ConstraintSplit {
                    certificate: *certificate,
                }
            } else {
                let Some(construction) = construction else {
                    return Err(GeometryErrorKind::triangulation(
                        "refinement candidate has no construction recipe",
                    ));
                };
                NodeProvenance::InteriorRefinement {
                    parent_face: face.index(),
                    parent_sites: entry.triangle,
                    construction,
                }
            };
            self.provenance.push(provenance);
            let mut changed = if let Located::Outside(visible_edge) = located {
                let insertion = self.mesh.insert_outside(
                    &self.coordinates,
                    site,
                    visible_edge,
                    &mut self.constrained,
                )?;
                let mut changed = insertion.changed;
                changed.extend(self.mesh.try_legalize_constrained_from(
                    &self.coordinates,
                    &mut self.constrained,
                    insertion.legalization,
                )?);
                changed
            } else {
                self.mesh.insert_constrained(
                    &self.coordinates,
                    site,
                    located,
                    &mut self.constrained,
                )?
            };
            if let Some((piece, parameter, _)) = constraint {
                let mut cursor = self
                    .constraint_pieces
                    .remove(ordered(piece.start_site, piece.end_site))
                    .expect("the split owns an indexed constraint piece");
                changed.extend(self.insert_constraint(
                    ConstraintPiece {
                        constraint: piece.constraint,
                        start_site: piece.start_site,
                        end_site: site,
                        lo: piece.lo.clone(),
                        hi: parameter.clone(),
                    },
                    &mut cursor,
                )?);
                changed.extend(self.insert_constraint(
                    ConstraintPiece {
                        constraint: piece.constraint,
                        start_site: site,
                        end_site: piece.end_site,
                        lo: parameter,
                        hi: piece.hi,
                    },
                    &mut cursor,
                )?);
                circles.insert(&self.coordinates, piece.start_site, site)?;
                circles.insert(&self.coordinates, site, piece.end_site)?;
            }
            faces.refresh(&self.mesh, &self.constrained, &mut changed)?;
            for &face in &changed {
                if self.mesh.alive(face)
                    && faces.get(face) == Some(true)
                    && let Some(entry) = BadTriangle::new(
                        face,
                        self.mesh.triangle(face),
                        &self.coordinates,
                        min_angle,
                        max_area,
                    )?
                {
                    bad.push(entry);
                }
            }
            if !changed.contains(&entry.face)
                && self.mesh.alive(entry.face)
                && self.mesh.triangle(entry.face) == entry.triangle
                && faces.get(entry.face) == Some(true)
            {
                bad.push(entry);
            }
            inserted = inserted.wrapping_add(1);
        }
        Ok(true)
    }

    fn prepare_split(
        &self,
        circles: &ConstraintIndex,
        e: HalfedgeId,
    ) -> Result<Option<PreparedSplit>> {
        let (start, end) = self.mesh.edge_vertices(e);
        if self.is_constrained(e) {
            let Some(piece) = self.constraint_pieces.get(ordered(start, end)) else {
                return Err(GeometryErrorKind::triangulation(
                    "constrained edge has no retained root interval",
                ));
            };
            let requested = piece.lo.try_midpoint(&piece.hi)?;
            let certificate = self.certify_constraint_split(piece, &requested)?;
            let candidate = certificate.point;
            let start = self.coordinates.site_query(piece.start_site);
            let end = self.coordinates.site_query(piece.end_site);
            let candidate_query = self.coordinates.point_query(candidate);
            // Splitting the current edge in its interior preserves the mesh
            // embedding; bent rounded segments require the incidence query.
            if self.coordinates.orient([start, end, candidate_query])? != ExactSign::Zero
                || !self.coordinates.between(start, end, candidate_query)
            {
                self.certify_split_incidence(circles, piece, candidate)?;
            }
            let located = self.locate_boundary_split(e, candidate)?;
            return Ok(Some(PreparedSplit {
                candidate,
                located,
                constraint: Some(PreparedConstraintSplit {
                    piece: piece.clone(),
                    certificate,
                }),
                construction: None,
            }));
        }
        let midpoint = self.coordinates.rounded_midpoint(start, end)?;
        let located = match self.mesh.locate_bounded(
            &self.coordinates,
            e.face(),
            self.coordinates.point_query(midpoint),
            &self.constrained,
        )? {
            LocationOutcome::Located(Located::OnEdge(slot))
                if slot == e || slot.index() == self.mesh.twin(e) =>
            {
                Located::OnEdge(e)
            },
            LocationOutcome::Located(Located::Inside(face)) => Located::Inside(face),
            LocationOutcome::Located(
                Located::OnVertex | Located::OnEdge(_) | Located::Outside(_),
            )
            | LocationOutcome::Blocked(_) => {
                return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
            },
        };
        Ok(Some(PreparedSplit {
            candidate: midpoint,
            located,
            constraint: None,
            construction: Some(RefinementConstruction::EdgeMidpoint([start, end])),
        }))
    }

    fn certify_constraint_split(
        &self,
        piece: &ConstraintPiece,
        requested: &exact::ExactRational,
    ) -> Result<CertifiedBoundarySplit> {
        if requested.try_cmp(&piece.lo)? != std::cmp::Ordering::Greater
            || requested.try_cmp(&piece.hi)? != std::cmp::Ordering::Less
        {
            return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
        }
        let (root_start, root_end) = self.constraint_sites(piece.constraint)?;
        let [start_x, start_y] = self
            .coordinates
            .exact_xy(self.coordinates.site_query(root_start));
        let [end_x, end_y] = self
            .coordinates
            .exact_xy(self.coordinates.site_query(root_end));
        let candidate = XY::new(
            round_source_coordinate(&exact_source_coordinate(&start_x, &end_x, requested)?)?,
            round_source_coordinate(&exact_source_coordinate(&start_y, &end_y, requested)?)?,
        );
        let half_ulp = split_half_ulp_bounds(candidate)?;
        let candidate_query = self.coordinates.point_query(candidate);
        if self.coordinates.orient([
            self.coordinates.site_query(root_start),
            self.coordinates.site_query(root_end),
            candidate_query,
        ])? != ExactSign::Zero
            && !split_deviation_within(
                &self.coordinates,
                self.coordinates.site_query(root_start),
                self.coordinates.site_query(root_end),
                requested,
                candidate_query,
                &half_ulp,
            )?
        {
            return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
        }
        Ok(CertifiedBoundarySplit {
            constraint: piece.constraint,
            parameter: requested.clone(),
            point: candidate,
            half_ulp,
        })
    }

    fn certify_split_incidence(
        &self,
        circles: &ConstraintIndex,
        piece: &ConstraintPiece,
        candidate: XY,
    ) -> Result<()> {
        let start = self.coordinates.site_query(piece.start_site);
        let end = self.coordinates.site_query(piece.end_site);
        let candidate_query = self.coordinates.point_query(candidate);
        let old_pair = ordered(piece.start_site, piece.end_site);
        let subsegments = [
            (start, candidate_query, piece.start_site),
            (candidate_query, end, piece.end_site),
        ];
        let envelope = self.coordinates.envelope(&[start, candidate_query, end])?;
        for circle in circles.tree.locate_in_envelope_intersecting(envelope) {
            let (other_start, other_end) = circle.sites;
            if ordered(other_start, other_end) == old_pair {
                continue;
            }
            let (a, b) = (
                self.coordinates.site_query(other_start),
                self.coordinates.site_query(other_end),
            );
            if self.coordinates.on_segment(a, b, candidate_query)? {
                return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
            }
            for (left, right, shared_site) in subsegments {
                if self
                    .coordinates
                    .segments_properly_cross([left, right, a, b])?
                {
                    return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
                }
                if (other_start != shared_site && self.coordinates.on_segment(left, right, a)?)
                    || (other_end != shared_site && self.coordinates.on_segment(left, right, b)?)
                {
                    return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
                }
            }
        }
        Ok(())
    }

    /// Locate a rounded source split while permitting traversal across only the
    /// source edge it replaces.
    pub(super) fn locate_boundary_split(&self, e: HalfedgeId, point: XY) -> Result<Located> {
        let twin = self.mesh.twin(e);
        let twin = (twin != EMPTY).then(|| HalfedgeId::from_index(twin));
        let point = self.coordinates.point_query(point);
        match self
            .mesh
            .locate_bounded(&self.coordinates, e.face(), point, &[])?
        {
            LocationOutcome::Located(Located::OnVertex | Located::Outside(_)) => {
                Err(GeometryErrorKind::unrepresentable_output("triangulate"))
            },
            LocationOutcome::Located(Located::Inside(face)) => Ok(Located::Inside(face)),
            LocationOutcome::Located(Located::OnEdge(slot)) if slot == e || twin == Some(slot) => {
                Ok(Located::OnEdge(e))
            },
            LocationOutcome::Located(Located::OnEdge(slot)) if self.is_constrained(slot) => {
                Err(GeometryErrorKind::unrepresentable_output("triangulate"))
            },
            LocationOutcome::Located(Located::OnEdge(slot)) => Ok(Located::OnEdge(slot)),
            LocationOutcome::Blocked(edge) => {
                if self.mesh.twin(edge) == EMPTY {
                    Ok(Located::Outside(edge))
                } else if let Some(edge) = self.mesh.outside_edge(&self.coordinates, point)? {
                    Ok(Located::Outside(edge))
                } else {
                    Err(GeometryErrorKind::unrepresentable_output("triangulate"))
                }
            },
        }
    }
}
