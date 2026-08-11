use std::collections::{HashMap, HashSet};

use spade::Triangulation as _;

use crate::geometry::tessellation::{
    AngleLimit, ConstrainedDelaunayTriangulation, DelaunayTriangulation, Error, FixedFaceHandle,
    FixedVertexHandle, InnerTag, InsertionError, Point2, RefinementParameters, exact,
};
use crate::geometry::{
    AreaSign, AxisFrame, CoordSeq, Coordinates as _, ExpansionBudget, GENERATED_ITEM_LIMIT,
    GeometryErrorKind, Orientation, Point, PointKey, Result, SimilarityFrame, XY, axis_pow2_scale,
    exact_incircle_sign, orientation, power_of_two_exponent, ring_decision_area, same_point,
    scale_by_power_of_two,
};

// Test-only companion to the Delaunay marker: constrained output admission
// must happen before this result-vector reservation.
#[cfg(test)]
thread_local! {
    static CONSTRAINED_OUTPUT_RESERVES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn note_constrained_output_reserve() {
    CONSTRAINED_OUTPUT_RESERVES.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
pub(super) fn take_constrained_output_reserves() -> usize {
    CONSTRAINED_OUTPUT_RESERVES.with(|count| count.replace(0))
}

/// Optional CDT mesh-refinement controls. Active when either quality constraint
/// is set. Ruppert-style refinement is bounded; incomplete meshes are rejected
/// rather than returned as if the requested quality had been satisfied.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) enum CdtRefinement {
    #[default]
    Off,
    On {
        min_angle: Option<crate::Positive>,
        max_area: Option<crate::Positive>,
    },
}

impl CdtRefinement {
    pub(crate) const fn active(self) -> bool {
        matches!(self, Self::On { .. })
    }
}

/// A `max_area` no larger than the source's exact total area can constrain a
/// triangulation; a limit at least that total cannot.  Every retained CDT face
/// lies inside the source and therefore has no more area than the source as a
/// whole.  Dropping that inert request before invoking Spade is both the
/// correct short-circuit and prevents its raw-coordinate area arithmetic from
/// turning a reciprocal/near-collinear input into unbounded refinement.
fn effective_refinement(area: f64, refinement: CdtRefinement) -> CdtRefinement {
    let CdtRefinement::On {
        min_angle,
        max_area: Some(max_area),
    } = refinement
    else {
        return refinement;
    };
    if area.is_finite() && max_area.get() >= area.abs() {
        min_angle.map_or(CdtRefinement::Off, |min_angle| CdtRefinement::On {
            min_angle: Some(min_angle),
            max_area: None,
        })
    } else {
        refinement
    }
}

fn refinement_parameters(
    refinement: CdtRefinement,
    max_additional_vertices: usize,
    frame: Option<AxisFrame>,
) -> RefinementParameters<f64> {
    let mut params = RefinementParameters::new()
        .exclude_outer_faces(true)
        // `max_area` alone must not silently opt into Spade's default 30°
        // angle target; each public constraint controls only its own quality.
        .with_angle_limit(AngleLimit::from_deg(0.0))
        .with_max_additional_vertices(max_additional_vertices);
    let CdtRefinement::On {
        min_angle,
        max_area,
    } = refinement
    else {
        return params;
    };
    if let Some(angle) = min_angle {
        params = params.with_angle_limit(AngleLimit::from_deg(angle.get()));
    }
    if let Some(area) = max_area {
        // Spade sees the independently scaled coordinates.  Its threshold
        // must therefore be the exact source-space max area scaled by the
        // frame determinant; passing source units here was the active-path
        // reciprocal-axis bug (and made refinement chase the wrong target).
        let local_area = frame.map_or_else(
            || area.get(),
            |frame| {
                scale_by_power_of_two(
                    area.get(),
                    power_of_two_exponent(frame.scale_x()) + power_of_two_exponent(frame.scale_y()),
                )
            },
        );
        params = params.with_max_allowed_area(local_area);
    }
    params
}

fn triangle_min_angle_degrees(a: Point2<f64>, b: Point2<f64>, c: Point2<f64>) -> f64 {
    fn angle(a: Point2<f64>, center: Point2<f64>, b: Point2<f64>) -> f64 {
        let (ax, ay) = (a.x - center.x, a.y - center.y);
        let (bx, by) = (b.x - center.x, b.y - center.y);
        let denominator = ax.hypot(ay) * bx.hypot(by);
        if denominator == 0.0 || !denominator.is_finite() {
            return 0.0;
        }
        ((ax * bx + ay * by) / denominator)
            .clamp(-1.0, 1.0)
            .acos()
            .to_degrees()
    }
    angle(b, a, c).min(angle(a, b, c)).min(angle(a, c, b))
}

fn triangle_area(a: Point, b: Point, c: Point) -> f64 {
    // Keep the post-refinement check on the same framed/exact area owner as
    // the preflight admission.  Raw `(dx * dy - dy * dx) / 2` is precisely
    // the reciprocal-axis underflow that made an inert max-area request look
    // unsatisfied.
    ring_decision_area(&[a, b, c, a][..]).magnitude().get()
}

fn validate_refinement_request(area: f64, refinement: CdtRefinement) -> Result<()> {
    if let CdtRefinement::On {
        min_angle: Some(angle),
        ..
    } = refinement
        && angle.get() > 30.0
    {
        return Err(GeometryErrorKind::triangulation(format!(
            "min_angle must be at most 30 degrees for terminating constrained refinement, got {}",
            angle.get()
        )));
    }
    if let CdtRefinement::On {
        max_area: Some(max_area),
        ..
    } = refinement
    {
        // The public polygon area is the only magnitude authority: it owns
        // the common shell/hole frame and its exact fallback. Summing a second
        // per-ring approximation here made cyclic rotations disagree about
        // the same max-area admission.
        let minimum_faces = (area.abs() / max_area.get()).ceil() as usize;
        ExpansionBudget::product("triangulate", "max_area", minimum_faces, 4)?;
    }
    Ok(())
}

fn apply_refinement(
    cdt: &mut ConstrainedDelaunayTriangulation<Point2<f64>>,
    refinement: CdtRefinement,
    frame: Option<AxisFrame>,
) -> Result<()> {
    if !refinement.active() {
        return Ok(());
    }
    let max_additional_vertices = GENERATED_ITEM_LIMIT.saturating_sub(cdt.num_vertices());
    let result = cdt.refine(refinement_parameters(
        refinement,
        max_additional_vertices,
        frame,
    ));
    if result.refinement_complete {
        Ok(())
    } else {
        Err(GeometryErrorKind::triangulation(format!(
            "constrained refinement could not satisfy the requested quality within the limit of {GENERATED_ITEM_LIMIT} generated vertices; relax min_angle/max_area"
        )))
    }
}

fn validate_refined_triangle(
    a: Point,
    b: Point,
    c: Point,
    refinement: CdtRefinement,
) -> Result<()> {
    let CdtRefinement::On {
        min_angle,
        max_area,
    } = refinement
    else {
        return Ok(());
    };
    if let Some(limit) = min_angle
        && triangle_min_angle_degrees(
            spade_point(a.xy()),
            spade_point(b.xy()),
            spade_point(c.xy()),
        ) + 1.0e-9
            < limit.get()
    {
        return Err(GeometryErrorKind::triangulation(format!(
            "constrained refinement did not satisfy min_angle={} degrees",
            limit.get()
        )));
    }
    if let Some(limit) = max_area
        && triangle_area(a, b, c) > limit.get() * (1.0 + 1.0e-12)
    {
        return Err(GeometryErrorKind::triangulation(format!(
            "constrained refinement did not satisfy max_area={}",
            limit.get()
        )));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Site {
    pub(super) id: usize,
    pub(super) point: Point,
}

#[derive(Debug)]
struct CandidateTriangulation {
    topology: PositiveTopology,
    mapped_sites: Option<Vec<XY>>,
}

/// Candidate topology whose bounded faces have already been exact-oriented
/// positively. Construction functions establish this once; certification
/// consumes the typed state instead of repeating the exact face-sign scan.
#[derive(Debug)]
struct PositiveTopology(delaunator::Triangulation);

#[derive(Debug)]
pub(super) struct CertifiedDelaunay {
    topology: delaunator::Triangulation,
    cocircular_halfedges: Vec<bool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CertifiedPrimalEdge {
    Interior {
        sites: [usize; 2],
        faces: [usize; 2],
    },
    Hull {
        sites: [usize; 2],
        face: usize,
        opposite: usize,
    },
}

impl CertifiedDelaunay {
    const fn empty(hull: Vec<usize>) -> Self {
        Self {
            topology: delaunator::Triangulation {
                triangles: Vec::new(),
                halfedges: Vec::new(),
                hull,
            },
            cocircular_halfedges: Vec::new(),
        }
    }

    pub(super) fn triangles(&self) -> &[[usize; 3]] {
        self.topology.triangles.as_chunks::<3>().0
    }

    pub(super) fn halfedges(&self) -> &[usize] {
        &self.topology.halfedges
    }

    pub(super) fn edge_is_cocircular(&self, slot: usize) -> bool {
        self.cocircular_halfedges[slot]
    }

    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Pass C consumes the certified hull topology")
    )]
    pub(super) fn hull(&self) -> &[usize] {
        &self.topology.hull
    }

    pub(super) fn primal_edges(&self) -> Vec<CertifiedPrimalEdge> {
        let mut edges = Vec::new();
        for (slot, &reverse) in self.topology.halfedges.iter().enumerate() {
            if reverse != delaunator::EMPTY && slot > reverse {
                continue;
            }
            let mut sites = [
                self.topology.triangles[slot],
                self.topology.triangles[delaunator::next_halfedge(slot)],
            ];
            sites.sort_unstable();
            if reverse == delaunator::EMPTY {
                edges.push(CertifiedPrimalEdge::Hull {
                    sites,
                    face: slot / 3,
                    opposite: self.topology.triangles[delaunator::prev_halfedge(slot)],
                });
            } else {
                let mut faces = [slot / 3, reverse / 3];
                faces.sort_unstable();
                edges.push(CertifiedPrimalEdge::Interior { sites, faces });
            }
        }
        edges.sort_unstable_by_key(|edge| match *edge {
            CertifiedPrimalEdge::Interior { sites, faces } => (sites, 0, faces, 0),
            CertifiedPrimalEdge::Hull {
                sites,
                face,
                opposite,
            } => (sites, 1, [face, usize::MAX], opposite),
        });
        edges
    }

    pub(super) fn collinear_order(&self) -> Option<&[usize]> {
        self.topology
            .triangles
            .is_empty()
            .then_some(self.topology.hull.as_slice())
    }

    pub(super) fn into_topology(self) -> delaunator::Triangulation {
        let mut triangles = Vec::with_capacity(self.topology.triangles.len());
        for &[a, b, c] in self.topology.triangles.as_chunks::<3>().0 {
            triangles.extend([a, c, b]);
        }
        delaunator::Triangulation {
            halfedges: delaunay_halfedges(&triangles),
            triangles,
            hull: self.topology.hull,
        }
    }
}

impl CandidateTriangulation {
    /// The only transition from proposed topology to trusted Delaunay state.
    #[expect(
        clippy::too_many_lines,
        reason = "the sole certification transition keeps its complete invariant proof visible together"
    )]
    fn try_certify(self, sites: &[Site]) -> Result<CertifiedDelaunay> {
        let topology = self.topology.0;
        let points: Vec<_> = sites.iter().map(|site| site.point).collect();
        if let Some(mapped) = self.mapped_sites
            && !proposal_mapping_is_admissible(&points, &mapped)
        {
            return Err(GeometryErrorKind::triangulation(
                "candidate coordinate mapping is not a bit-exact site bijection",
            ));
        }
        if sites
            .iter()
            .enumerate()
            .any(|(index, site)| site.id != index)
            || !delaunay_euler_complete(points.len(), &topology)
            || !topology.triangles.len().is_multiple_of(3)
            || topology.triangles.len() != topology.halfedges.len()
            || topology.triangles.iter().any(|&site| site >= sites.len())
        {
            return Err(GeometryErrorKind::triangulation(
                "candidate Delaunay topology is incomplete",
            ));
        }
        let used: HashSet<_> = topology.triangles.iter().copied().collect();
        if used.len() != sites.len() {
            return Err(GeometryErrorKind::triangulation(
                "candidate Delaunay topology does not contain every site exactly once",
            ));
        }
        let mut unique_triangles = HashSet::new();
        let mut edge_incidence: HashMap<(usize, usize), Vec<(usize, usize)>> = HashMap::new();
        for (index, triangle) in topology.triangles.as_chunks::<3>().0.iter().enumerate() {
            let mut triangle_key = *triangle;
            triangle_key.sort_unstable();
            if triangle_key[0] == triangle_key[1]
                || triangle_key[1] == triangle_key[2]
                || !unique_triangles.insert(triangle_key)
            {
                return Err(GeometryErrorKind::triangulation(
                    "candidate repeats a vertex or bounded face",
                ));
            }
            for local in 0..3 {
                let edge = index * 3 + local;
                let start = topology.triangles[edge];
                let end = topology.triangles[delaunator::next_halfedge(edge)];
                edge_incidence
                    .entry((start.min(end), start.max(end)))
                    .or_default()
                    .push((edge, index));
                let reverse = topology.halfedges[edge];
                if reverse != delaunator::EMPTY
                    && (reverse >= topology.halfedges.len()
                        || topology.halfedges[reverse] != edge
                        || topology.triangles[edge]
                            != topology.triangles[delaunator::next_halfedge(reverse)]
                        || topology.triangles[delaunator::next_halfedge(edge)]
                            != topology.triangles[reverse])
                {
                    return Err(GeometryErrorKind::triangulation(
                        "candidate half-edges are not reciprocal",
                    ));
                }
            }
        }
        if edge_incidence.values().any(|incidents| {
            incidents.is_empty()
                || incidents.len() > 2
                || (incidents.len() == 1 && topology.halfedges[incidents[0].0] != delaunator::EMPTY)
                || (incidents.len() == 2 && topology.halfedges[incidents[0].0] != incidents[1].0)
        }) {
            return Err(GeometryErrorKind::triangulation(
                "candidate is not a two-manifold disk",
            ));
        }
        if candidate_has_crossing_edges(&points, &topology) {
            return Err(GeometryErrorKind::triangulation(
                "candidate contains crossing non-incident edges",
            ));
        }
        let boundary = candidate_boundary_cycle(&topology)?;
        if boundary != exact_hull_cycle(&points) {
            return Err(GeometryErrorKind::triangulation(
                "candidate boundary does not equal the exact convex hull",
            ));
        }
        let mut reachable = HashSet::from([0_usize]);
        let mut frontier = vec![0_usize];
        while let Some(face) = frontier.pop() {
            for edge in face * 3..face * 3 + 3 {
                let reverse = topology.halfedges[edge];
                if reverse != delaunator::EMPTY && reachable.insert(reverse / 3) {
                    frontier.push(reverse / 3);
                }
            }
        }
        if reachable.len() != topology.triangles.len() / 3 {
            return Err(GeometryErrorKind::triangulation(
                "candidate bounded faces are disconnected",
            ));
        }
        let mut cocircular_halfedges = vec![false; topology.halfedges.len()];
        for edge in 0..topology.halfedges.len() {
            let reverse = topology.halfedges[edge];
            if reverse == delaunator::EMPTY || edge > reverse {
                continue;
            }
            let a = topology.triangles[edge];
            let b = topology.triangles[delaunator::next_halfedge(edge)];
            let c = topology.triangles[delaunator::prev_halfedge(edge)];
            let d = topology.triangles[delaunator::prev_halfedge(reverse)];
            let side_c = exact::orient2d(points[a].xy(), points[b].xy(), points[c].xy());
            let side_d = exact::orient2d(points[a].xy(), points[b].xy(), points[d].xy());
            if side_c == exact::ExactSign::Zero
                || side_d == exact::ExactSign::Zero
                || side_c == side_d
            {
                return Err(GeometryErrorKind::triangulation(
                    "candidate interior-edge opposite vertices are not on opposite sides",
                ));
            }
            let incircle = exact::incircle(
                points[a].xy(),
                points[b].xy(),
                points[c].xy(),
                points[d].xy(),
            );
            if incircle == exact::ExactSign::Positive {
                return Err(GeometryErrorKind::triangulation(
                    "candidate contains an exact non-Delaunay diagonal",
                ));
            }
            if incircle == exact::ExactSign::Zero {
                cocircular_halfedges[edge] = true;
                cocircular_halfedges[reverse] = true;
            }
        }
        Ok(CertifiedDelaunay {
            topology: delaunator::Triangulation {
                triangles: topology.triangles,
                halfedges: topology.halfedges,
                hull: boundary,
            },
            cocircular_halfedges,
        })
    }
}

pub(super) fn certified_delaunay(sites: &[Site]) -> Result<CertifiedDelaunay> {
    let points: Vec<_> = sites.iter().map(|site| site.point).collect();
    if points.len() < 3 {
        return Ok(CertifiedDelaunay::empty((0..points.len()).collect()));
    }
    let corner_hull = exact_corner_hull_cycle(&points);
    if corner_hull.len() < 3 {
        return Ok(CertifiedDelaunay::empty(exact_collinear_order(&points)));
    }
    if let Some(proposal) = scale_only_proposal(&points)
        && let Ok(certified) = proposal.try_certify(sites)
    {
        return Ok(certified);
    }
    // Cold deterministic seed: independent power-of-two axes retain every
    // stored ordinate, then sorted-edge exact Lawson flips choose the source
    // Euclidean topology. The candidate still earns trust only here.
    let seed = exact_incremental_seed(&points).ok_or_else(|| {
        GeometryErrorKind::triangulation(
            "could not construct a complete deterministic Delaunay seed",
        )
    })?;
    let fallback = exact_metric_delaunay(&points, &seed).ok_or_else(|| {
        GeometryErrorKind::triangulation(
            "could not construct a complete deterministic Delaunay seed",
        )
    })?;
    CandidateTriangulation {
        topology: PositiveTopology(fallback),
        mapped_sites: None,
    }
    .try_certify(sites)
}

fn exact_incremental_seed(points: &[Point]) -> Option<delaunator::Triangulation> {
    let hull = exact_corner_hull_cycle(points);
    if hull.len() < 3 {
        return None;
    }
    let mut triangles: Vec<[usize; 3]> = (1..hull.len() - 1)
        .map(|index| [hull[0], hull[index], hull[index + 1]])
        .collect();
    let hull_sites: HashSet<_> = hull.iter().copied().collect();
    for site in (0..points.len()).filter(|site| !hull_sites.contains(site)) {
        let mut containing = Vec::new();
        for (index, &[a, b, c]) in triangles.iter().enumerate() {
            let signs = [
                exact::orient2d(points[a].xy(), points[b].xy(), points[site].xy()),
                exact::orient2d(points[b].xy(), points[c].xy(), points[site].xy()),
                exact::orient2d(points[c].xy(), points[a].xy(), points[site].xy()),
            ];
            if signs.iter().all(|&sign| sign != exact::ExactSign::Negative) {
                containing.push((index, signs));
            }
        }
        if containing.is_empty() {
            return None;
        }
        let mut replacements = Vec::new();
        for &(index, signs) in containing.iter().rev() {
            let [a, b, c] = triangles.swap_remove(index);
            if signs.iter().all(|&sign| sign != exact::ExactSign::Zero) {
                replacements.extend([[a, b, site], [b, c, site], [c, a, site]]);
            } else {
                let (left, right, opposite) = if signs[0] == exact::ExactSign::Zero {
                    (a, b, c)
                } else if signs[1] == exact::ExactSign::Zero {
                    (b, c, a)
                } else {
                    (c, a, b)
                };
                replacements.extend([[left, site, opposite], [site, right, opposite]]);
            }
        }
        triangles.extend(replacements);
    }
    let mut flat = Vec::with_capacity(triangles.len() * 3);
    for [a, b, c] in triangles {
        match exact::orient2d(points[a].xy(), points[b].xy(), points[c].xy()) {
            exact::ExactSign::Positive => flat.extend([a, b, c]),
            exact::ExactSign::Negative => flat.extend([a, c, b]),
            exact::ExactSign::Zero => return None,
        }
    }
    let halfedges = delaunay_halfedges(&flat);
    Some(delaunator::Triangulation {
        triangles: flat,
        halfedges,
        hull: exact_hull_cycle(points),
    })
}

fn scale_only_proposal(points: &[Point]) -> Option<CandidateTriangulation> {
    const MIN_SPADE: f64 = 1.793_662_034_335_766e-43;
    const MAX_SPADE: f64 = 3.213_876_088_517_980_6e60;
    let maximum = points
        .iter()
        .flat_map(|point| [point.x.abs(), point.y.abs()])
        .fold(0.0_f64, f64::max);
    let scale = axis_pow2_scale(maximum);
    let mapped: Vec<_> = points
        .iter()
        .map(|point| XY::new(point.x * scale, point.y * scale))
        .collect();
    if mapped
        .iter()
        .flat_map(|point| [point.x, point.y])
        .any(|value| value != 0.0 && !(MIN_SPADE..=MAX_SPADE).contains(&value.abs()))
    {
        return None;
    }
    let mapped_points: Vec<_> = mapped
        .iter()
        .map(|point| Point::new_unchecked_xy(point.x, point.y))
        .collect();
    let topology = positive_topology(try_spade_delaunay(&mapped_points)?, points)?;
    Some(CandidateTriangulation {
        topology,
        mapped_sites: Some(mapped),
    })
}

fn proposal_mapping_is_admissible(points: &[Point], mapped: &[XY]) -> bool {
    if points.len() != mapped.len() {
        return false;
    }
    let maximum = points
        .iter()
        .flat_map(|point| [point.x.abs(), point.y.abs()])
        .fold(0.0_f64, f64::max);
    let scale = axis_pow2_scale(maximum);
    let mut keys = HashSet::with_capacity(mapped.len());
    points.iter().zip(mapped).all(|(source, mapped)| {
        let expected = XY::new(source.x * scale, source.y * scale);
        let roundtrip = XY::new(mapped.x / scale, mapped.y / scale);
        expected.x.to_bits() == mapped.x.to_bits()
            && expected.y.to_bits() == mapped.y.to_bits()
            && source.x.to_bits() == roundtrip.x.to_bits()
            && source.y.to_bits() == roundtrip.y.to_bits()
            && (source.x == 0.0 || mapped.x != 0.0)
            && (source.y == 0.0 || mapped.y != 0.0)
            && keys.insert(PointKey::new(*mapped))
    })
}

fn positive_topology(
    topology: delaunator::Triangulation,
    source: &[Point],
) -> Option<PositiveTopology> {
    if !topology.triangles.len().is_multiple_of(3) {
        return None;
    }
    let mut triangles = Vec::with_capacity(topology.triangles.len());
    for &[a, b, c] in topology.triangles.as_chunks::<3>().0 {
        match exact::orient2d(source[a].xy(), source[b].xy(), source[c].xy()) {
            exact::ExactSign::Positive => triangles.extend([a, b, c]),
            exact::ExactSign::Negative => triangles.extend([a, c, b]),
            exact::ExactSign::Zero => return None,
        }
    }
    let halfedges = delaunay_halfedges(&triangles);
    Some(PositiveTopology(delaunator::Triangulation {
        triangles,
        halfedges,
        hull: topology.hull,
    }))
}

pub(crate) fn delaunay_triangulation(points: &[Point]) -> delaunator::Triangulation {
    let sites: Vec<_> = points
        .iter()
        .copied()
        .enumerate()
        .map(|(id, point)| Site { id, point })
        .collect();
    certified_delaunay(&sites).map_or_else(
        |_| delaunator::Triangulation {
            triangles: Vec::new(),
            halfedges: Vec::new(),
            hull: exact_hull_cycle(points),
        },
        CertifiedDelaunay::into_topology,
    )
}

fn candidate_boundary_cycle(topology: &delaunator::Triangulation) -> Result<Vec<usize>> {
    let mut next = HashMap::new();
    let mut boundary_edges = 0_usize;
    for edge in 0..topology.halfedges.len() {
        if topology.halfedges[edge] == delaunator::EMPTY {
            boundary_edges += 1;
            next.insert(
                topology.triangles[edge],
                topology.triangles[delaunator::next_halfedge(edge)],
            );
        }
    }
    if next.len() != boundary_edges {
        return Err(GeometryErrorKind::triangulation(
            "candidate boundary branches at a site",
        ));
    }
    let Some(&start) = next.keys().min() else {
        return Ok(Vec::new());
    };
    let mut cycle = Vec::with_capacity(next.len());
    let mut visited = HashSet::with_capacity(next.len());
    let mut current = start;
    loop {
        if !visited.insert(current) {
            break;
        }
        cycle.push(current);
        current = *next.get(&current).ok_or_else(|| {
            GeometryErrorKind::triangulation("candidate boundary is not one cycle")
        })?;
    }
    if current != start || cycle.len() != next.len() {
        return Err(GeometryErrorKind::triangulation(
            "candidate boundary is not one cycle",
        ));
    }
    Ok(rotate_cycle_to_minimum(cycle))
}

fn exact_corner_hull_cycle(points: &[Point]) -> Vec<usize> {
    let mut order: Vec<_> = (0..points.len()).collect();
    order.sort_unstable_by(|&a, &b| {
        points[a]
            .x
            .total_cmp(&points[b].x)
            .then_with(|| points[a].y.total_cmp(&points[b].y))
    });
    let mut lower: Vec<usize> = Vec::new();
    for &site in &order {
        while lower.len() >= 2
            && exact::orient2d(
                points[lower[lower.len() - 2]].xy(),
                points[lower[lower.len() - 1]].xy(),
                points[site].xy(),
            ) != exact::ExactSign::Positive
        {
            lower.pop();
        }
        lower.push(site);
    }
    let mut upper: Vec<usize> = Vec::new();
    for &site in order.iter().rev() {
        while upper.len() >= 2
            && exact::orient2d(
                points[upper[upper.len() - 2]].xy(),
                points[upper[upper.len() - 1]].xy(),
                points[site].xy(),
            ) != exact::ExactSign::Positive
        {
            upper.pop();
        }
        upper.push(site);
    }
    lower.pop();
    upper.pop();
    lower.extend(upper);
    rotate_cycle_to_minimum(lower)
}

fn rotate_cycle_to_minimum(mut cycle: Vec<usize>) -> Vec<usize> {
    if let Some(start) = cycle
        .iter()
        .enumerate()
        .min_by_key(|(_, value)| **value)
        .map(|(index, _)| index)
    {
        cycle.rotate_left(start);
    }
    cycle
}

fn exact_hull_cycle(points: &[Point]) -> Vec<usize> {
    let mut order: Vec<_> = (0..points.len()).collect();
    order.sort_unstable_by(|&a, &b| {
        points[a]
            .x
            .total_cmp(&points[b].x)
            .then_with(|| points[a].y.total_cmp(&points[b].y))
    });
    let Some(&last) = order.last() else {
        return Vec::new();
    };
    if order.len() < 3
        || order.iter().all(|&site| {
            exact::orient2d(points[order[0]].xy(), points[last].xy(), points[site].xy())
                == exact::ExactSign::Zero
        })
    {
        return exact_collinear_order(points);
    }
    let mut lower: Vec<usize> = Vec::new();
    for &site in &order {
        while lower.len() >= 2
            && exact::orient2d(
                points[lower[lower.len() - 2]].xy(),
                points[lower[lower.len() - 1]].xy(),
                points[site].xy(),
            ) == exact::ExactSign::Negative
        {
            lower.pop();
        }
        lower.push(site);
    }
    let mut upper: Vec<usize> = Vec::new();
    for &site in order.iter().rev() {
        while upper.len() >= 2
            && exact::orient2d(
                points[upper[upper.len() - 2]].xy(),
                points[upper[upper.len() - 1]].xy(),
                points[site].xy(),
            ) == exact::ExactSign::Negative
        {
            upper.pop();
        }
        upper.push(site);
    }
    lower.pop();
    upper.pop();
    lower.extend(upper);
    rotate_cycle_to_minimum(lower)
}

fn exact_collinear_order(points: &[Point]) -> Vec<usize> {
    let mut order: Vec<_> = (0..points.len()).collect();
    let (minx, maxx, miny, maxy) = points.iter().fold(
        (
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ),
        |(minx, maxx, miny, maxy), point| {
            (
                minx.min(point.x),
                maxx.max(point.x),
                miny.min(point.y),
                maxy.max(point.y),
            )
        },
    );
    if maxx - minx >= maxy - miny {
        order.sort_unstable_by(|&left, &right| {
            points[left]
                .x
                .total_cmp(&points[right].x)
                .then_with(|| points[left].y.total_cmp(&points[right].y))
        });
    } else {
        order.sort_unstable_by(|&left, &right| {
            points[left]
                .y
                .total_cmp(&points[right].y)
                .then_with(|| points[left].x.total_cmp(&points[right].x))
        });
    }
    order
}

fn candidate_has_crossing_edges(points: &[Point], topology: &delaunator::Triangulation) -> bool {
    #[derive(Clone, Copy)]
    struct EdgeEnvelope {
        edge: (usize, usize),
        envelope: rstar::AABB<[f64; 2]>,
    }
    impl rstar::RTreeObject for EdgeEnvelope {
        type Envelope = rstar::AABB<[f64; 2]>;

        fn envelope(&self) -> Self::Envelope {
            self.envelope
        }
    }

    let mut unique = HashSet::with_capacity(topology.triangles.len());
    for edge in 0..topology.triangles.len() {
        let pair = (
            topology.triangles[edge],
            topology.triangles[delaunator::next_halfedge(edge)],
        );
        let key = (pair.0.min(pair.1), pair.0.max(pair.1));
        unique.insert(key);
    }
    let edges: Vec<_> = unique
        .into_iter()
        .map(|edge @ (a, b)| EdgeEnvelope {
            edge,
            envelope: rstar::AABB::from_corners(
                [points[a].x.min(points[b].x), points[a].y.min(points[b].y)],
                [points[a].x.max(points[b].x), points[a].y.max(points[b].y)],
            ),
        })
        .collect();
    let tree = rstar::RTree::bulk_load(edges.clone());
    edges.iter().any(|entry| {
        let (a, b) = entry.edge;
        tree.locate_in_envelope_intersecting(entry.envelope)
            .any(|other| {
                let (c, d) = other.edge;
                if (a, b) >= (c, d) {
                    return false;
                }
                if a == c || a == d || b == c || b == d {
                    return false;
                }
                let ab_c = exact::orient2d(points[a].xy(), points[b].xy(), points[c].xy());
                let ab_d = exact::orient2d(points[a].xy(), points[b].xy(), points[d].xy());
                let cd_a = exact::orient2d(points[c].xy(), points[d].xy(), points[a].xy());
                let cd_b = exact::orient2d(points[c].xy(), points[d].xy(), points[b].xy());
                let on_segment = |start: usize, end: usize, point: usize| {
                    points[point].x >= points[start].x.min(points[end].x)
                        && points[point].x <= points[start].x.max(points[end].x)
                        && points[point].y >= points[start].y.min(points[end].y)
                        && points[point].y <= points[start].y.max(points[end].y)
                };
                (ab_c != ab_d && cd_a != cd_b)
                    || (ab_c == exact::ExactSign::Zero && on_segment(a, b, c))
                    || (ab_d == exact::ExactSign::Zero && on_segment(a, b, d))
                    || (cd_a == exact::ExactSign::Zero && on_segment(c, d, a))
                    || (cd_b == exact::ExactSign::Zero && on_segment(c, d, b))
            })
    })
}

/// Legalize an affine-frame triangulation in the source Euclidean metric.
///
/// The independent power-of-two frame preserves incidence and crossings, so
/// it is a sound way to obtain a finite seed mesh.  A Lawson flip then uses
/// [`exact_incircle_sign`] on the actual stored doubles for every diagonal;
/// this is the only step that chooses Delaunay topology.
fn exact_metric_delaunay(
    points: &[Point],
    initial: &delaunator::Triangulation,
) -> Option<delaunator::Triangulation> {
    if !delaunay_euler_complete(points.len(), initial) {
        return None;
    }
    let mut triangles: Vec<[usize; 3]> = initial.triangles.as_chunks::<3>().0.to_vec();
    loop {
        let mut edges: HashMap<(usize, usize), Vec<(usize, usize)>> = HashMap::new();
        for (triangle_index, &[a, b, c]) in triangles.iter().enumerate() {
            for (left, right, opposite) in [(a, b, c), (b, c, a), (c, a, b)] {
                let key = (left.min(right), left.max(right));
                edges
                    .entry(key)
                    .or_default()
                    .push((triangle_index, opposite));
            }
        }

        let mut candidates: Vec<_> = edges.iter().collect();
        candidates.sort_unstable_by_key(|(edge, _)| **edge);
        let mut flipped = false;
        for (&(a, b), adjacent) in candidates {
            let [(first_triangle, c), (second_triangle, d)] = adjacent.as_slice() else {
                continue;
            };
            if !flippable_quad(points, a, b, *c, *d)
                || !source_incircle_contains(points, a, b, *c, *d)
            {
                continue;
            }
            triangles[*first_triangle] = [*c, *d, a];
            triangles[*second_triangle] = [*d, *c, b];
            flipped = true;
            break;
        }
        if !flipped {
            break;
        }
    }

    let mut output = Vec::with_capacity(triangles.len() * 3);
    for [a, b, c] in triangles {
        match exact::orient2d(points[a].xy(), points[b].xy(), points[c].xy()) {
            exact::ExactSign::Positive => output.extend([a, b, c]),
            exact::ExactSign::Negative => output.extend([a, c, b]),
            exact::ExactSign::Zero => return None,
        }
    }
    let halfedges = delaunay_halfedges(&output);
    Some(delaunator::Triangulation {
        triangles: output,
        halfedges,
        hull: Vec::new(),
    })
}

fn flippable_quad(points: &[Point], a: usize, b: usize, c: usize, d: usize) -> bool {
    let side = |start: usize, end: usize, point: usize| {
        orientation(points[start].xy(), points[end].xy(), points[point].xy())
    };
    let opposite = |left: Orientation, right: Orientation| {
        matches!(
            (left, right),
            (Orientation::Clockwise, Orientation::CounterClockwise)
                | (Orientation::CounterClockwise, Orientation::Clockwise)
        )
    };
    opposite(side(a, b, c), side(a, b, d)) && opposite(side(c, d, a), side(c, d, b))
}

fn source_incircle_contains(points: &[Point], a: usize, b: usize, c: usize, d: usize) -> bool {
    source_incircle_contains_xy(
        points[a].xy(),
        points[b].xy(),
        points[c].xy(),
        points[d].xy(),
    )
}

fn source_incircle_contains_xy(a: XY, b: XY, c: XY, d: XY) -> bool {
    let sign = exact_incircle_sign(a, b, c, d);
    match orientation(a, b, c) {
        Orientation::CounterClockwise => sign > 0,
        Orientation::Clockwise => sign < 0,
        Orientation::Collinear => false,
    }
}

fn delaunay_halfedges(triangles: &[usize]) -> Vec<usize> {
    let mut halfedges = vec![delaunator::EMPTY; triangles.len()];
    let mut directed = HashMap::with_capacity(triangles.len());
    for (index, &start) in triangles.iter().enumerate() {
        let end = triangles[if index % 3 == 2 { index - 2 } else { index + 1 }];
        if let Some(&reverse) = directed.get(&(end, start)) {
            halfedges[index] = reverse;
            halfedges[reverse] = index;
        } else {
            directed.insert((start, end), index);
        }
    }
    halfedges
}

#[cfg(test)]
fn delaunay_triangulation_raw(points: &[Point]) -> delaunator::Triangulation {
    let sites: Vec<delaunator::Point> = points
        .iter()
        .map(|point| delaunator::Point {
            x: point.x,
            y: point.y,
        })
        .collect();
    delaunator::triangulate(&sites)
}

/// Planar triangulation Euler identity: `T = 2n - 2 - h` for `n` vertices
/// and `h` hull vertices (connected triangulation of a simple polygon's
/// vertex set, or of a point set with convex hull of size `h`).
///
/// Hull size is derived from boundary halfedges. Candidate `hull` storage is
/// advisory input and cannot participate in certification arithmetic.
fn delaunay_euler_complete(n: usize, triangulation: &delaunator::Triangulation) -> bool {
    if n < 3 {
        return triangulation.triangles.is_empty();
    }
    let t = triangulation.triangles.len() / 3;
    if t == 0 {
        return false;
    }
    let h = triangulation
        .halfedges
        .iter()
        .filter(|&&edge| edge == delaunator::EMPTY)
        .count();
    if h < 3 {
        return false;
    }
    // Saturating form avoids underflow when n is tiny relative to h.
    t == n.saturating_mul(2).saturating_sub(2).saturating_sub(h)
}

/// Delaunay triangulation via `spade`, emitted in `delaunator`'s
/// `(triangles, halfedges)` layout so the existing half-edge consumers (the
/// chi-shape peeling) run unchanged. `delaunator`'s sweep-circle `legalize`
/// cascades to ~O(n²·⁹) on near-cocircular inputs (dense points on a smooth
/// curve); `spade`'s bulk loader stays far closer to linear there, so this is
/// the engine `concave_hull` uses. Falls back to `delaunator` if loading is
/// rejected (keeps the point-index ↔ vertex-index mapping exact).
pub(crate) fn delaunay_triangulation_spade(points: &[Point]) -> delaunator::Triangulation {
    let source_points = points;
    // A uniform similarity loses the thin axis of reciprocal-scale inputs.
    // The ordinary owner already seeds in an independent power-of-two frame
    // and legalizes every diagonal in the exact source metric.
    if AxisFrame::from_points(points).is_some_and(AxisFrame::has_reciprocal_axes) {
        return delaunay_triangulation(points);
    }
    let framed = SimilarityFrame::from_points(points).map(|frame| {
        points
            .iter()
            .copied()
            .map(|point| frame.frame_point(point))
            .collect::<Vec<_>>()
    });
    let points = framed.as_deref().unwrap_or(points);
    if let Some(tri) = try_spade_delaunay(points)
        && delaunay_euler_complete(points.len(), &tri)
    {
        return tri;
    }
    // Spade rejection means its raw-predicate coordinate gate did not admit
    // the framed values. Return to the certified source-coordinate owner;
    // unguarded Delaunator is not sound on those rejected values.
    delaunay_triangulation(source_points)
}

fn try_spade_delaunay(points: &[Point]) -> Option<delaunator::Triangulation> {
    let spade_points: Vec<_> = points.iter().map(|point| spade_point(point.xy())).collect();
    let triangulation =
        DelaunayTriangulation::<Point2<f64>>::bulk_load_stable(spade_points).ok()?;
    if triangulation.num_vertices() != points.len() {
        return None;
    }

    // spade inner faces are CCW (gometry's shell canon, matching delaunator).
    let mut triangles: Vec<usize> = Vec::with_capacity(triangulation.num_inner_faces() * 3);
    let mut face_to_dense: HashMap<FixedFaceHandle<InnerTag>, usize> =
        HashMap::with_capacity(triangulation.num_inner_faces());
    for (face_index, face) in triangulation.inner_faces().enumerate() {
        face_to_dense.insert(face.fix(), face_index);
        let [a, b, c] = face.vertices();
        triangles.push(a.index());
        triangles.push(b.index());
        triangles.push(c.index());
    }

    let mut halfedges = Vec::with_capacity(triangles.len());
    for face in triangulation.inner_faces() {
        for edge in face.adjacent_edges() {
            let rev_edge = edge.rev();
            let Some(rev_face) = rev_edge.face().as_inner() else {
                halfedges.push(delaunator::EMPTY);
                continue;
            };
            let rev_face_index = face_to_dense[&rev_face.fix()];
            let rev_edge_handle = rev_edge.fix();
            let rev_local_index = rev_face
                .adjacent_edges()
                .iter()
                .position(|candidate| candidate.fix() == rev_edge_handle)
                .expect("reversed edge must be adjacent to its inner face");
            halfedges.push(rev_face_index * 3 + rev_local_index);
        }
    }
    Some(delaunator::Triangulation {
        triangles,
        halfedges,
        hull: Vec::new(),
    })
}

/// Constrained Delaunay triangulation of an areal geometry (`rings` =
/// every shell and hole as a closed coordinate sequence): the ring edges
/// become CDT constraints, then inner faces whose centroid lies inside the
/// geometry (`contains`) are kept — the triangles strictly within the input.
///
/// This is spade used directly (the engine geo wrapped): collect the
/// constraint segments, exact-key-dedup endpoints, build the CDT, then
/// filter by centroid membership. Valid polygon boundaries are already
/// simple; any crossing constraints fall back to spade's split insertion.
/// Constrained Delaunay triangulation of the polygon interior as a flat
/// `[a, b, c, a]`-per-kept-face vertex stream (XY, since spade's CDT is 2D).
/// `contains` keeps interior faces. The packed array surface builds a
/// `Polygons` layout from this in one pass; the `Vec<Shape>` form wraps it
/// below.
#[expect(
    clippy::too_many_lines,
    reason = "the constrained triangulation kernel keeps one allocation and topology pipeline cohesive"
)]
pub(crate) fn constrained_triangle_vertices<'a>(
    rings: impl Iterator<Item = &'a CoordSeq>,
    area: f64,
    refinement: CdtRefinement,
    budget: &mut ExpansionBudget,
) -> Result<Vec<Point>> {
    let rings: Vec<&CoordSeq> = rings.collect();
    let refinement = effective_refinement(area, refinement);
    validate_refinement_request(area, refinement)?;
    // A nondegenerate three-corner shell is itself its sole constrained
    // Delaunay face. Spade can only see it as collinear after a uniform
    // similarity has underflowed one reciprocal axis; materialize the exact
    // stored-double face directly instead of returning a false empty result.
    // This is a topology decision, so use the exact ring-area sign rather
    // than the public inexact area measurement.
    if !refinement.active()
        && rings.len() == 1
        && rings[0].coord_count() == 4
        && ring_decision_area(rings[0]).sign() != AreaSign::Zero
    {
        let ring = rings[0];
        let (a, b, c, close) = (
            ring.nth_coord(0),
            ring.nth_coord(1),
            ring.nth_coord(2),
            ring.nth_coord(3),
        );
        if same_point(a.xy(), close.xy()) {
            budget.add(4)?;
            return Ok(vec![a, b, c, a]);
        }
    }
    let mut lines: Vec<[XY; 2]> = Vec::new();
    for ring in rings {
        let count = ring.coord_count();
        for index in 0..count.saturating_sub(1) {
            lines.push([ring.nth_coord(index).xy(), ring.nth_coord(index + 1).xy()]);
        }
    }
    let lines = cleanup_constraint_lines(lines);
    let mut vertices = Vec::new();
    let mut vertex_indices: HashMap<PointKey, usize> = HashMap::with_capacity(lines.len() * 2);
    let mut edges = Vec::with_capacity(lines.len());
    for [start, end] in lines {
        let start = constraint_vertex_index(start, &mut vertices, &mut vertex_indices);
        let end = constraint_vertex_index(end, &mut vertices, &mut vertex_indices);
        edges.push([start, end]);
    }

    let source_vertices: Vec<Point> = vertices
        .iter()
        .map(|point| Point::new_unchecked_xy(point.x, point.y))
        .collect();
    let constraint_edges: HashSet<_> = edges
        .iter()
        .map(|&[left, right]| {
            source_edge_key(source_vertices[left].xy(), source_vertices[right].xy())
        })
        .collect();
    let frame =
        AxisFrame::from_points(&source_vertices).filter(|frame| frame.has_reciprocal_axes());
    let local_vertices = frame.map_or(vertices, |frame| {
        source_vertices
            .iter()
            .copied()
            .map(|point| spade_point(frame.frame_point(point).xy()))
            .collect()
    });
    let mut cdt = bulk_load_constraints(local_vertices, edges)?;

    apply_refinement(&mut cdt, refinement, frame)?;
    let exterior = constrained_outer_faces(&cdt);

    // Count the same retained faces the emitter will walk, so the operation-
    // wide budget is admitted before allocating its generated vertex stream.
    let capacity = cdt
        .fixed_inner_faces()
        .filter(|face| !exterior.contains(face))
        .count()
        .checked_mul(4)
        .ok_or_else(|| GeometryErrorKind::triangulation("triangle output size overflow"))?;
    budget.add(capacity)?;
    let mut triangles = Vec::new();
    triangles
        .try_reserve_exact(capacity / 4)
        .map_err(|_| GeometryErrorKind::triangulation("could not allocate constrained faces"))?;
    #[cfg(test)]
    note_constrained_output_reserve();
    let mut output = Vec::new();
    output.try_reserve_exact(capacity).map_err(|_| {
        GeometryErrorKind::triangulation(format!(
            "could not allocate {capacity} constrained-triangulation coordinates"
        ))
    })?;
    for face in cdt.inner_faces() {
        if exterior.contains(&face.fix()) {
            continue;
        }
        let [a, b, c] = face.positions();
        let [a_vertex, b_vertex, c_vertex] = face.vertices();
        let unframe = |vertex: FixedVertexHandle, point: Point2<f64>| {
            // Spade's stable bulk load preserves the original vertex indices.
            // Recover an input constraint point by its index, not by
            // `unframe(frame(point))`: the latter is algebraically identical
            // but can lose a stored-double ULP through multiply/add/divide.
            // Refinement/split vertices are appended, so only those take the
            // ordinary inverse-frame path.
            if let Some(&source) = source_vertices.get(vertex.index()) {
                return source;
            }
            frame.map_or_else(
                || Point::new_unchecked_xy(point.x, point.y),
                |frame| {
                    let point = frame.unframe_xy(XY::new(point.x, point.y));
                    Point::new_unchecked_xy(point.x, point.y)
                },
            )
        };
        let a = unframe(a_vertex.fix(), a);
        let b = unframe(b_vertex.fix(), b);
        let c = unframe(c_vertex.fix(), c);
        triangles.push([a, b, c]);
    }
    // Spade's independently-scaled mesh supplies incidence only.  The
    // constrained interior still owns the source Euclidean diagonal, exactly
    // like the unconstrained path: every shared retained face edge is safe to
    // legalize, while a ring constraint has an exterior/hole face on its
    // other side and is therefore never a two-face flip candidate.
    if frame.is_some_and(AxisFrame::has_reciprocal_axes) {
        exact_metric_constrained_triangles(&mut triangles, &constraint_edges);
    }

    for [a, b, c] in triangles {
        validate_refined_triangle(a, b, c, refinement)?;
        // The exact Lawson flip may reverse a local face; restore gometry's
        // CCW shell convention before emitting its closed ring.
        match orientation(a.xy(), b.xy(), c.xy()) {
            Orientation::CounterClockwise => output.extend_from_slice(&[a, b, c, a]),
            Orientation::Clockwise => output.extend_from_slice(&[a, c, b, a]),
            Orientation::Collinear => {
                return Err(GeometryErrorKind::triangulation(
                    "constrained triangulation produced a collinear face",
                ));
            },
        }
    }
    Ok(output)
}

/// Lawson-legalize the retained CDT faces in the source Euclidean metric.
///
/// Spade necessarily selects its seed topology in the temporary coordinate
/// frame. With reciprocal axes that frame is affine rather than similar, so
/// only the exact stored-double incircle predicate may choose an unconstrained
/// diagonal. A ring constraint is adjacent to at most one retained interior
/// face; by considering only shared retained edges this routine cannot flip a
/// boundary or hole edge.
fn exact_metric_constrained_triangles(
    triangles: &mut [[Point; 3]],
    constraint_edges: &HashSet<(PointKey, PointKey)>,
) {
    loop {
        let mut edges: HashMap<(PointKey, PointKey), Vec<(usize, Point)>> = HashMap::new();
        for (triangle_index, &[a, b, c]) in triangles.iter().enumerate() {
            for (left, right, opposite) in [(a, b, c), (b, c, a), (c, a, b)] {
                let key = source_edge_key(left.xy(), right.xy());
                edges
                    .entry(key)
                    .or_default()
                    .push((triangle_index, opposite));
            }
        }

        let mut candidates: Vec<_> = edges.into_iter().collect();
        candidates.sort_unstable_by_key(|(edge, _)| *edge);
        let mut flipped = false;
        for ((a, b), adjacent) in candidates {
            if constraint_edges.contains(&(a, b)) {
                continue;
            }
            let [(first_triangle, c), (second_triangle, d)] = adjacent.as_slice() else {
                continue;
            };
            let (a, b) = (
                XY::new(f64::from_bits(a.x), f64::from_bits(a.y)),
                XY::new(f64::from_bits(b.x), f64::from_bits(b.y)),
            );
            if !flippable_quad_xy(a, b, c.xy(), d.xy())
                || !source_incircle_contains_xy(a, b, c.xy(), d.xy())
            {
                continue;
            }
            triangles[*first_triangle] = [*c, *d, Point::new_unchecked_xy(a.x, a.y)];
            triangles[*second_triangle] = [*d, *c, Point::new_unchecked_xy(b.x, b.y)];
            flipped = true;
            break;
        }
        if !flipped {
            return;
        }
    }
}

fn source_edge_key(left: XY, right: XY) -> (PointKey, PointKey) {
    let (left, right) = (PointKey::new(left), PointKey::new(right));
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

fn flippable_quad_xy(a: XY, b: XY, c: XY, d: XY) -> bool {
    let opposite = |left: Orientation, right: Orientation| {
        matches!(
            (left, right),
            (Orientation::Clockwise, Orientation::CounterClockwise)
                | (Orientation::CounterClockwise, Orientation::Clockwise)
        )
    };
    opposite(orientation(a, b, c), orientation(a, b, d))
        && opposite(orientation(c, d, a), orientation(c, d, b))
}

/// CDT face membership from constraint topology, not an arithmetic point
/// probe. Beginning at the convex hull, cross ordinary edges within one
/// parity region and defer a constraint crossing to the next one. The final
/// outer parity contains the unbounded exterior and every hole; its complement
/// is exactly the polygonal fill.
///
/// A rounded centroid is useful only as a positive witness. It must never
/// reject a valid face: at reciprocal magnitudes it can round outside the very
/// triangle it is meant to admit.
fn constrained_outer_faces(
    cdt: &ConstrainedDelaunayTriangulation<Point2<f64>>,
) -> HashSet<FixedFaceHandle<InnerTag>> {
    if cdt.all_vertices_on_line() {
        return HashSet::new();
    }
    let mut inner = HashSet::new();
    let mut outer = HashSet::new();
    let mut current: Vec<_> = cdt.convex_hull().map(|edge| edge.rev()).collect();
    let mut next = Vec::new();
    let mut return_outer = true;
    loop {
        while let Some(edge) = current.pop() {
            let (todo, faces) = if edge.is_constraint_edge() {
                (&mut next, &mut inner)
            } else {
                (&mut current, &mut outer)
            };
            if let Some(face) = edge.face().as_inner()
                && faces.insert(face.fix())
            {
                todo.push(edge.prev().rev());
                todo.push(edge.next().rev());
            }
        }
        if next.is_empty() {
            break;
        }
        std::mem::swap(&mut inner, &mut outer);
        std::mem::swap(&mut next, &mut current);
        return_outer = !return_outer;
    }
    if return_outer { outer } else { inner }
}

/// [`constrained_triangle_vertices`] chunked into one triangle `Polygon` each —
/// the `Vec<Shape>` form (Z/M resolved by the caller's `carry_each`).
pub(crate) const fn spade_point(point: XY) -> Point2<f64> {
    Point2::new(point.x, point.y)
}

pub(crate) fn insertion_error(error: InsertionError) -> Error {
    GeometryErrorKind::triangulation(error.to_string())
}

fn constraint_vertex_index(
    point: XY,
    vertices: &mut Vec<Point2<f64>>,
    vertex_indices: &mut HashMap<PointKey, usize>,
) -> usize {
    let next_index = vertices.len();
    *vertex_indices
        .entry(PointKey::new(point))
        .or_insert_with(|| {
            vertices.push(spade_point(point));
            next_index
        })
}

fn bulk_load_constraints(
    vertices: Vec<Point2<f64>>,
    edges: Vec<[usize; 2]>,
) -> Result<ConstrainedDelaunayTriangulation<Point2<f64>>> {
    let mut constraint_conflict = false;
    let cdt = ConstrainedDelaunayTriangulation::<Point2<f64>>::try_bulk_load_cdt(
        vertices.clone(),
        edges.clone(),
        |_| {
            constraint_conflict = true;
        },
    )
    .map_err(insertion_error)?;
    if !constraint_conflict {
        return Ok(cdt);
    }

    let mut cdt =
        ConstrainedDelaunayTriangulation::<Point2<f64>>::bulk_load_cdt(vertices, Vec::new())
            .map_err(insertion_error)?;
    for [start, end] in edges {
        cdt.add_constraint_and_split(
            FixedVertexHandle::from_index(start),
            FixedVertexHandle::from_index(end),
            |point| point,
        );
    }
    Ok(cdt)
}

/// Prepare constraint segments for spade: exact endpoint-key deduplication
/// (bit-identical XY only) and drop zero-length / duplicate undirected edges.
/// Absolute proximity snapping was deleted — a scale-free 1e-4 radius
/// annihilated valid small polygons (5e-5 square → zero triangles).
pub(crate) fn cleanup_constraint_lines(lines: Vec<[XY; 2]>) -> Vec<[XY; 2]> {
    let edge_key = |a: XY, b: XY| -> (PointKey, PointKey) {
        let (ka, kb) = (PointKey::new(a), PointKey::new(b));
        if ka <= kb { (ka, kb) } else { (kb, ka) }
    };
    let mut prepared: Vec<[XY; 2]> = Vec::with_capacity(lines.len());
    let mut seen: HashSet<(PointKey, PointKey)> = HashSet::with_capacity(lines.len());
    for [start, end] in lines {
        // Exact identity only — `same_point` is bit-equal XY via PointKey.
        if !same_point(start, end) && seen.insert(edge_key(start, end)) {
            prepared.push([start, end]);
        }
    }
    prepared
}

#[cfg(test)]
mod exact_metric_cdt_tests {
    use super::*;

    #[test]
    fn reciprocal_quad_flips_the_spade_diagonal_in_source_metric() {
        let large = 1e159;
        let tiny = 1e-159;
        let left = Point::new_unchecked_xy(-large, 0.0);
        let bottom = Point::new_unchecked_xy(0.0, -tiny);
        let right = Point::new_unchecked_xy(large, 0.0);
        let top = Point::new_unchecked_xy(0.0, 2.0 * tiny);
        assert!(
            AxisFrame::from_points(&[left, bottom, right, top])
                .is_some_and(AxisFrame::has_reciprocal_axes)
        );
        let mut triangles = [[left, bottom, right], [left, right, top]];
        exact_metric_constrained_triangles(&mut triangles, &HashSet::new());
        assert!(triangles.iter().any(|triangle| {
            triangle
                .iter()
                .any(|point| same_point(point.xy(), bottom.xy()))
                && triangle
                    .iter()
                    .any(|point| same_point(point.xy(), top.xy()))
        }));

        let ring = CoordSeq::from(vec![left, bottom, right, top, left]);
        let mut budget = ExpansionBudget::new("test", "test");
        let output = constrained_triangle_vertices(
            std::iter::once(&ring),
            4.0,
            CdtRefinement::Off,
            &mut budget,
        )
        .expect("reciprocal constrained mesh");
        assert!(output.as_chunks::<4>().0.iter().any(|triangle| {
            triangle
                .iter()
                .any(|point| same_point(point.xy(), bottom.xy()))
                && triangle
                    .iter()
                    .any(|point| same_point(point.xy(), top.xy()))
        }));
    }
}

#[cfg(test)]
mod certified_tests {
    use super::*;

    fn sites(points: &[(f64, f64)]) -> Vec<Site> {
        points
            .iter()
            .enumerate()
            .map(|(id, &(x, y))| Site {
                id,
                point: Point::new_unchecked_xy(x, y),
            })
            .collect()
    }

    fn candidate(triangles: &[[usize; 3]]) -> CandidateTriangulation {
        let flat: Vec<_> = triangles.iter().flatten().copied().collect();
        CandidateTriangulation {
            topology: PositiveTopology(delaunator::Triangulation {
                halfedges: delaunay_halfedges(&flat),
                triangles: flat,
                hull: Vec::new(),
            }),
            mapped_sites: None,
        }
    }

    #[test]
    fn certified_hull_retains_collinear_boundary_sites_and_collinear_is_empty() {
        let boundary_sites = sites(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0)]);
        let certified = certified_delaunay(&boundary_sites).expect("boundary-site triangulation");
        assert_eq!(certified.hull().len(), 5);
        assert!(certified.hull().contains(&1));

        let collinear = sites(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)]);
        let certified = certified_delaunay(&collinear).expect("collinear empty topology");
        assert!(certified.triangles().is_empty());
        assert_eq!(certified.hull(), &[0, 1, 2]);
    }

    #[test]
    fn certified_cocircular_state_is_reciprocal() {
        let sites = sites(&[(1.0, 0.0), (0.0, 1.0), (-1.0, 0.0), (0.0, -1.0)]);
        let certified = certified_delaunay(&sites).unwrap();
        let mut cocircular_pairs = 0;
        for (slot, &reverse) in certified.halfedges().iter().enumerate() {
            if reverse == delaunator::EMPTY {
                continue;
            }
            assert_eq!(
                certified.edge_is_cocircular(slot),
                certified.edge_is_cocircular(reverse)
            );
            cocircular_pairs += usize::from(certified.edge_is_cocircular(slot));
        }
        assert_eq!(cocircular_pairs, 2);
    }

    #[test]
    fn positive_topology_canonicalizes_face_orientation_once() {
        let points = [
            Point::new_unchecked_xy(0.0, 0.0),
            Point::new_unchecked_xy(2.0, 0.0),
            Point::new_unchecked_xy(0.0, 2.0),
        ];
        let reversed = delaunator::Triangulation {
            triangles: vec![0, 2, 1],
            halfedges: vec![delaunator::EMPTY; 3],
            hull: vec![0, 1, 2],
        };
        assert_eq!(positive_topology(reversed, &points).unwrap().0.triangles, [
            0, 1, 2
        ]);
    }

    #[test]
    fn candidate_remainder_and_manifold_mutations_differ_from_restored() {
        let sites = sites(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0)]);
        let restored_candidate = candidate(&[[0, 1, 2], [0, 2, 3]]);
        let restored = restored_candidate.try_certify(&sites).is_ok();

        let mut remainder = candidate(&[[0, 1, 2], [0, 2, 3]]);
        remainder.topology.0.triangles.push(0);
        remainder.topology.0.halfedges.push(delaunator::EMPTY);
        let remainder_mutated = remainder.try_certify(&sites).is_ok();

        let mut manifold = candidate(&[[0, 1, 2], [0, 2, 3]]);
        let interior = manifold
            .topology
            .0
            .halfedges
            .iter()
            .position(|&reverse| reverse != delaunator::EMPTY)
            .unwrap();
        manifold.topology.0.halfedges[interior] = delaunator::EMPTY;
        let manifold_mutated = manifold.try_certify(&sites).is_ok();
        assert_eq!(
            (remainder_mutated, manifold_mutated, restored),
            (false, false, true)
        );
    }

    #[test]
    fn illegal_diagonal_mutation_differs_from_restored() {
        let sites = sites(&[(0.0, 0.0), (0.0, 2.0), (2.0, 0.0), (2.0, 1.0)]);
        let alternatives = [[[0, 2, 3], [0, 3, 1]], [[0, 2, 1], [2, 3, 1]]];
        let outcomes =
            alternatives.map(|triangles| candidate(&triangles).try_certify(&sites).is_ok());
        assert_eq!(outcomes.iter().filter(|&&accepted| accepted).count(), 1);
        assert_ne!(outcomes[0], outcomes[1]);
    }

    #[test]
    fn crossing_and_wrong_boundary_mutations_differ_from_restored() {
        let restored_points = vec![
            Point::new_unchecked_xy(0.0, 0.0),
            Point::new_unchecked_xy(2.0, 0.0),
            Point::new_unchecked_xy(2.0, 2.0),
            Point::new_unchecked_xy(0.0, 2.0),
        ];
        let restored_topology = candidate(&[[0, 1, 2], [0, 2, 3]]).topology;
        let restored_crossing =
            candidate_has_crossing_edges(&restored_points, &restored_topology.0);
        let crossing_topology = candidate(&[[0, 1, 2], [0, 2, 3], [0, 1, 3], [1, 2, 3]]).topology;
        let mutated_crossing = candidate_has_crossing_edges(&restored_points, &crossing_topology.0);

        // Positive oriented faces in a reciprocal connected disk with an
        // injective convex boundary cannot cross: a crossing would give a
        // point two positive PL preimages. The explicit crossing sweep is
        // defense in depth, so no single-check-isolated crossing candidate
        // exists. This direct mutation still proves its predicate changes.

        let wrong_hull_sites = sites(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.5, 0.5)]);
        let mutated_hull = candidate(&[[0, 1, 4], [1, 2, 4], [0, 4, 3]])
            .try_certify(&wrong_hull_sites)
            .is_ok();
        let restored_hull = certified_delaunay(&wrong_hull_sites).is_ok();
        assert_eq!(
            (
                mutated_crossing,
                restored_crossing,
                mutated_hull,
                restored_hull,
            ),
            (true, false, false, true),
        );
    }

    #[test]
    fn certified_storage_is_independent_of_hostile_insertion_order() {
        let coordinates = [(0.0, 0.0), (3.0, 0.0), (2.0, 2.0), (0.0, 3.0), (0.8, 1.1)];
        let signature = |coordinates: Vec<(f64, f64)>| {
            let sites = sites(&coordinates);
            let certified = certified_delaunay(&sites).unwrap();
            let mut triangles = certified
                .triangles()
                .iter()
                .map(|triangle| {
                    let mut keys = triangle.map(|site| PointKey::new(sites[site].point));
                    keys.sort_unstable();
                    keys
                })
                .collect::<Vec<_>>();
            triangles.sort_unstable();
            let mut hull = certified
                .hull()
                .iter()
                .map(|&site| PointKey::new(sites[site].point))
                .collect::<Vec<_>>();
            let start = hull
                .iter()
                .enumerate()
                .min_by_key(|(_, key)| **key)
                .unwrap()
                .0;
            hull.rotate_left(start);
            let mut reverse = hull.clone();
            reverse.reverse();
            let reverse_start = reverse.iter().position(|key| key == &hull[0]).unwrap();
            reverse.rotate_left(reverse_start);
            (triangles, hull.min(reverse))
        };
        let restored = signature(coordinates.to_vec());
        let mut hostile = coordinates.to_vec();
        hostile.reverse();
        let mutated = signature(hostile);
        assert_eq!(mutated, restored);
    }

    #[test]
    fn certification_accepts_restored_and_rejects_missing_site_topology() {
        let sites = sites(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (1.0, 0.8)]);
        let points: Vec<_> = sites.iter().map(|site| site.point).collect();
        let topology = positive_topology(delaunay_triangulation_raw(&points), &points).unwrap();
        let restored = CandidateTriangulation {
            topology: PositiveTopology(delaunator::Triangulation {
                triangles: topology.0.triangles.clone(),
                halfedges: topology.0.halfedges.clone(),
                hull: topology.0.hull.clone(),
            }),
            mapped_sites: None,
        }
        .try_certify(&sites);
        assert!(restored.is_ok(), "restored candidate: {restored:?}");

        let mut triangles = topology.0.triangles;
        for site in &mut triangles {
            if *site == 4 {
                *site = 0;
            }
        }
        let mutated = CandidateTriangulation {
            topology: PositiveTopology(delaunator::Triangulation {
                triangles,
                halfedges: topology.0.halfedges,
                hull: topology.0.hull,
            }),
            mapped_sites: None,
        }
        .try_certify(&sites);
        assert!(mutated.is_err(), "missing-site mutation became certified");
        // With h derived from boundary halfedges, completeness is also forced
        // by Euler: T=2V-2-h and T=2n-2-h imply used V == source n. Thus an
        // otherwise-clean missing-site mutation cannot exist; the explicit
        // used-site check remains a local diagnostic and defense in depth.
    }

    #[test]
    fn transformed_site_collision_never_becomes_certified() {
        let sites = sites(&[
            (-1e20, -1e20),
            (1e20, -1e20),
            (0.0, 1e20),
            (0.0, 0.0),
            (1.0, 1.0),
        ]);
        let points: Vec<_> = sites.iter().map(|site| site.point).collect();
        let frame = SimilarityFrame::from_points(&points).expect("mixed-scale frame");
        let local: Vec<_> = points
            .iter()
            .copied()
            .map(|point| frame.frame_point(point))
            .collect();
        assert!(
            local
                .iter()
                .copied()
                .map(PointKey::new)
                .collect::<HashSet<_>>()
                .len()
                < sites.len()
        );
        let topology = positive_topology(delaunay_triangulation_raw(&points), &points).unwrap();
        let mutated = CandidateTriangulation {
            topology: PositiveTopology(delaunator::Triangulation {
                triangles: topology.0.triangles.clone(),
                halfedges: topology.0.halfedges.clone(),
                hull: topology.0.hull.clone(),
            }),
            mapped_sites: Some(local.iter().map(Point::xy).collect()),
        }
        .try_certify(&sites);
        assert!(
            mutated.is_err(),
            "colliding transformed sites became certified"
        );
        let restored = CandidateTriangulation {
            topology,
            mapped_sites: None,
        }
        .try_certify(&sites);
        assert!(
            restored.is_ok(),
            "exact fallback did not restore the candidate: {restored:?}"
        );
    }

    #[test]
    fn proposal_mapping_requires_forward_identity_not_only_inverse_roundtrip() {
        let points = vec![Point::new_unchecked_xy(f64::MAX, 0.0)];
        let scale = axis_pow2_scale(f64::MAX);
        let expected = points[0].x * scale;
        let mapped = vec![XY::new(expected, 0.0)];
        assert!(proposal_mapping_is_admissible(&points, &mapped));
        let alternate = f64::from_bits(expected.to_bits() - 1);
        let mutated = vec![XY::new(alternate, 0.0)];
        let restored = proposal_mapping_is_admissible(&points, &mapped);
        let mutated_accepted = proposal_mapping_is_admissible(&points, &mutated);
        assert_ne!(alternate.to_bits(), expected.to_bits());
        assert_eq!((mutated_accepted, restored), (false, true));
    }

    #[test]
    fn exact_fallback_seed_keeps_ccw_hull_for_interior_insertion() {
        let points = vec![
            Point::new_unchecked_xy(0.0, 0.0),
            Point::new_unchecked_xy(2.0, 0.0),
            Point::new_unchecked_xy(1.0, 1.0),
            Point::new_unchecked_xy(0.0, 2.0),
            Point::new_unchecked_xy(2.0, 2.0),
        ];
        let seed = exact_incremental_seed(&points).expect("forced exact fallback seed");
        let restored = candidate_has_crossing_edges(&points, &seed);
        let mut reversed = exact_corner_hull_cycle(&points);
        reversed.reverse();
        let mutated = exact::orient2d(
            points[reversed[0]].xy(),
            points[reversed[1]].xy(),
            points[reversed[2]].xy(),
        );
        assert_eq!(mutated, exact::ExactSign::Negative);
        assert!(!restored, "restored crossing value was {restored}");
    }
}
