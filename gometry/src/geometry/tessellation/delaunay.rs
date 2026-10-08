//! Delaunay and constrained Delaunay triangulation over the exact engine.
//!
//! [`exact_delaunay`] decides every step of the triangulation with exact
//! predicates, so a mesh is Delaunay as built and carries its cocircular
//! flags out of one exact pass; [`cdt`] forces constraint edges into that
//! mesh and refines the faces inside them. This module owns the site and
//! mesh types the Voronoi dual reads, the hull and collinear orders the
//! engine seeds from, and the public entry points.

use std::cmp::Ordering;

use crate::geometry::exact_arith::ExactSign;
use crate::geometry::tessellation::{
    EMPTY, RingOwner, Triangulation, exact, next_halfedge, overlapping_rings_error, prev_halfedge,
};
use crate::geometry::{
    CoordSeq, CoordinateAxes, Coordinates as _, ExpansionBudget, GENERATED_ITEM_LIMIT,
    GeometryErrorKind, HasM, HasZ, MOrdinate, Orientation, Point, PointKey, Result, XY, ZOrdinate,
    open_xy_cycle_magnitude, orientation, same_point,
};

#[path = "cdt.rs"]
mod cdt;
#[path = "exact_delaunay.rs"]
pub(super) mod exact_delaunay;

// Test-only companion to the output budget: constrained output admission
// must happen before the result vector is reserved.
#[cfg(test)]
thread_local! {
    static CONSTRAINED_OUTPUT_RESERVES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn note_constrained_output_reserve() {
    CONSTRAINED_OUTPUT_RESERVES.with(|cell| cell.set(cell.get().saturating_add(1)));
}

#[cfg(test)]
pub(super) fn take_constrained_output_reserves() -> usize {
    CONSTRAINED_OUTPUT_RESERVES.with(|cell| cell.replace(0))
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
/// triangulation; a limit at least that total cannot. Every retained CDT face
/// lies inside the source and therefore has no more area than the source as a
/// whole, so that request is inert and is dropped before refinement starts.
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
        let minimum_faces = (area.abs() / max_area.get()).ceil();
        if !minimum_faces.is_finite() || minimum_faces > (GENERATED_ITEM_LIMIT / 4) as f64 {
            // The quotient is either non-finite or already beyond the
            // generated-output cap; report the bound before converting it to
            // usize or multiplying the face estimate.
            return Err(GeometryErrorKind::GeneratedOutputTooLarge {
                operation: "triangulate",
                parameter: "max_area",
                produced: GENERATED_ITEM_LIMIT + 1,
                limit: GENERATED_ITEM_LIMIT,
            }
            .into());
        }
        let minimum_faces = minimum_faces as usize;
        ExpansionBudget::product("triangulate", "max_area", minimum_faces, 4)?;
    }
    Ok(())
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
        && cdt::triangle_min_angle_degrees(a.xy(), b.xy(), c.xy()) + 1.0e-9 < limit.get()
    {
        return Err(GeometryErrorKind::triangulation(format!(
            "constrained refinement did not satisfy min_angle={} degrees",
            limit.get()
        )));
    }
    if let Some(limit) = max_area
        && open_xy_cycle_magnitude(&[a.xy(), b.xy(), c.xy()]).get() > limit.get() * (1.0 + 1.0e-12)
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

/// The Delaunay mesh the Voronoi dual reads: counter-clockwise faces in the
/// crate's halfedge layout plus one exact cocircular flag per slot, so every
/// fan of cocircular faces can be collapsed onto its one circumcenter.
#[derive(Debug)]
pub(super) struct CertifiedDelaunay {
    topology: Triangulation,
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
            topology: Triangulation {
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

    #[expect(
        clippy::indexing_slicing,
        reason = "the slot comes from this certified mesh's aligned halfedge column"
    )]
    pub(super) fn edge_is_cocircular(&self, slot: usize) -> bool {
        self.cocircular_halfedges[slot]
    }

    #[cfg(test)]
    pub(super) fn hull(&self) -> &[usize] {
        &self.topology.hull
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "each slot and reverse slot comes from the same completed flat mesh columns; direct halfedge-slot access preserves the certified topology layout"
    )]
    pub(super) fn primal_edges(&self) -> Vec<CertifiedPrimalEdge> {
        let mut edges = Vec::with_capacity(self.topology.halfedges.len());
        for (slot, &reverse) in self.topology.halfedges.iter().enumerate() {
            if reverse != EMPTY && slot > reverse {
                continue;
            }
            let mut sites = [
                self.topology.triangles[slot],
                self.topology.triangles[next_halfedge(slot)],
            ];
            sites.sort_unstable();
            if reverse == EMPTY {
                edges.push(CertifiedPrimalEdge::Hull {
                    sites,
                    face: slot / 3,
                    opposite: self.topology.triangles[prev_halfedge(slot)],
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

    /// The public layout: faces run clockwise, which is what every
    /// triangulation consumer outside this module has always read.
    pub(super) fn into_topology(self) -> Result<Triangulation> {
        let Self {
            topology:
                Triangulation {
                    mut triangles,
                    halfedges: previous_halfedges,
                    hull,
                },
            cocircular_halfedges,
        } = self;
        drop(previous_halfedges);
        drop(cocircular_halfedges);
        for face in triangles.as_chunks_mut::<3>().0 {
            face.swap(1, 2);
        }
        let halfedges = delaunay_halfedges(&triangles)?;
        Ok(Triangulation {
            triangles,
            halfedges,
            hull,
        })
    }
}

pub(super) fn certified_delaunay(sites: &[Site]) -> Result<CertifiedDelaunay> {
    // Every site by its input index; duplicates and collinear input are the
    // engine's to settle exactly.
    let mut points = Vec::with_capacity(sites.len());
    for site in sites {
        points.push(site.point);
    }
    let mut mesh = exact_delaunay::exact_delaunay(&points)?;
    if mesh.triangles.is_empty() {
        return Ok(CertifiedDelaunay::empty(mesh.hull));
    }
    let cocircular_halfedges = std::mem::take(&mut mesh.cocircular);
    Ok(CertifiedDelaunay {
        topology: mesh.into_topology(),
        cocircular_halfedges,
    })
}

pub(crate) fn delaunay_triangulation(points: &[Point]) -> Result<Triangulation> {
    let mut sites = Vec::with_capacity(points.len());
    for (id, &point) in points.iter().enumerate() {
        sites.push(Site { id, point });
    }
    certified_delaunay(&sites)?.into_topology()
}

/// The exact convex hull as a counter-clockwise cycle rotated to its smallest
/// site index. With `corners` only strict corners survive, which is the seed
/// the engine fans from; without it collinear boundary sites stay on the
/// cycle, and a wholly collinear input yields its collinear order instead.
#[expect(
    clippy::indexing_slicing,
    reason = "site IDs in order come from points, and the lower and upper last-two reads follow their length checks"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the lower and upper last-two reads follow len >= 2, so subtracting one or two cannot underflow"
)]
fn hull_cycle(points: &[Point], corners: bool) -> Vec<usize> {
    let xy = |site: usize| points[site].xy();
    let orient = |a: usize, b: usize, c: usize| exact::orient2d(xy(a), xy(b), xy(c));
    let mut order = Vec::with_capacity(points.len());
    order.extend(0..points.len());
    order.sort_unstable_by(|&a, &b| {
        geometric_cmp(xy(a).x, xy(b).x).then_with(|| geometric_cmp(xy(a).y, xy(b).y))
    });
    if !corners {
        let Some(&last) = order.last() else {
            return Vec::new();
        };
        if order.len() < 3
            || order
                .iter()
                .all(|&site| orient(order[0], last, site) == ExactSign::Zero)
        {
            return collinear_order(points);
        }
    }
    let pops = |sign: ExactSign| {
        if corners {
            sign != ExactSign::Positive
        } else {
            sign == ExactSign::Negative
        }
    };
    let mut lower: Vec<usize> = Vec::with_capacity(order.len());
    for &site in &order {
        while lower.len() >= 2 && pops(orient(lower[lower.len() - 2], lower[lower.len() - 1], site))
        {
            lower.pop();
        }
        lower.push(site);
    }
    let mut upper: Vec<usize> = Vec::with_capacity(order.len());
    for &site in order.iter().rev() {
        while upper.len() >= 2 && pops(orient(upper[upper.len() - 2], upper[upper.len() - 1], site))
        {
            upper.pop();
        }
        upper.push(site);
    }
    lower.pop();
    upper.pop();
    lower.reserve(upper.len());
    lower.extend(upper);
    if let Some(start) = lower
        .iter()
        .enumerate()
        .min_by_key(|(_, value)| **value)
        .map(|(index, _)| index)
    {
        lower.rotate_left(start);
    }
    lower
}

pub(super) fn exact_corner_hull_cycle(points: &[Point]) -> Vec<usize> {
    hull_cycle(points, true)
}

pub(super) fn exact_hull_cycle(points: &[Point]) -> Vec<usize> {
    hull_cycle(points, false)
}

/// Collinear sites in order along their longer extent.
#[expect(
    clippy::indexing_slicing,
    reason = "order contains only site IDs produced from 0..points.len()"
)]
fn collinear_order(points: &[Point]) -> Vec<usize> {
    let mut order = Vec::with_capacity(points.len());
    order.extend(0..points.len());
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
            geometric_cmp(points[left].x, points[right].x)
                .then_with(|| geometric_cmp(points[left].y, points[right].y))
        });
    } else {
        order.sort_unstable_by(|&left, &right| {
            geometric_cmp(points[left].y, points[right].y)
                .then_with(|| geometric_cmp(points[left].x, points[right].x))
        });
    }
    order
}

/// Geometric coordinate order: values that compare equal, including mixed
/// signed zeros, tie; otherwise IEEE total order. Hull seeds need this lex
/// instead of raw `total_cmp`, which would treat `-0` as left of `+0`.
#[expect(
    clippy::float_cmp,
    reason = "signed-zero identity must tie; total_cmp would order -0 left of +0 and change hull seeds"
)]
fn geometric_cmp(left: f64, right: f64) -> Ordering {
    if left == right {
        Ordering::Equal
    } else {
        left.total_cmp(&right)
    }
}

/// Twin slots for a flat triangle list by directed-edge lookup.
#[expect(
    clippy::indexing_slicing,
    reason = "triangle and halfedge columns have one entry per stable flat slot, and reverse slots are values inserted from this same range"
)]
fn delaunay_halfedges(triangles: &[usize]) -> Result<Vec<usize>> {
    let mut halfedges = vec![EMPTY; triangles.len()];
    let mut directed = crate::collections::try_hash_map_with_capacity(triangles.len())?;
    for index in 0..triangles.len() {
        let start = triangles[index];
        let end = triangles[next_halfedge(index)];
        if let Some(reverse) = directed.remove(&(end, start)) {
            halfedges[index] = reverse;
            halfedges[reverse] = index;
        } else {
            directed.insert((start, end), index);
        }
    }
    Ok(halfedges)
}

/// Planar triangulation Euler identity: `T = 2n - 2 - h` for `n` vertices
/// and `h` hull vertices of a connected triangulation of a point set.
#[cfg(test)]
fn delaunay_euler_complete(n: usize, triangulation: &Triangulation) -> bool {
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
        .filter(|&&edge| edge == EMPTY)
        .count();
    h >= 3 && t == n.saturating_mul(2).saturating_sub(2).saturating_sub(h)
}

/// Build the constrained mesh plan shared by scalar and packed publication.
///
/// `rings` contains every shell and hole as a closed coordinate sequence.
/// Source sites retain the first full `Point` for each XY key, while each
/// nonzero constraint keeps those selected endpoint sites and its stable source
/// identity for exact refinement certificates.
fn constrained_triangle_plan<'a>(
    rings: impl Iterator<Item = (RingOwner, &'a CoordSeq)>,
    area: f64,
    refinement: CdtRefinement,
) -> Result<cdt::ConstrainedMeshPlan> {
    let mut ring_list = Vec::new();
    for ring in rings {
        ring_list.push(ring);
    }
    let rings = ring_list;
    let force_xy = refinement.active();
    let refinement = effective_refinement(area, refinement);
    validate_refinement_request(area, refinement)?;
    let ConstrainedSourcePlan {
        mut vertices,
        occurrences,
        sources,
        source_rings,
    } = constrained_source_plan(&rings)?;
    let Some(mut cdt) =
        cdt::ConstrainedMesh::build_with_sources(&vertices, &occurrences, &sources, source_rings)?
    else {
        return Err(GeometryErrorKind::triangulation(
            "constrained triangulation needs three non-collinear vertices",
        ));
    };
    if let CdtRefinement::On {
        min_angle,
        max_area,
    } = refinement
    {
        // The refinement admission is for vertices inserted by refinement;
        // input constraint vertices are not generated output and must not
        // consume this operation's cap.
        let limit = GENERATED_ITEM_LIMIT;
        let complete = cdt.refine(
            &mut vertices,
            min_angle.map(crate::Positive::get),
            max_area.map(crate::Positive::get),
            limit,
        )?;
        if !complete {
            return Err(GeometryErrorKind::triangulation(format!(
                "constrained refinement could not satisfy the requested quality within the limit of {GENERATED_ITEM_LIMIT} generated vertices; relax min_angle/max_area"
            )));
        }
    }
    cdt.finish_plan(vertices, refinement, force_xy)
}

/// Constrained Delaunay triangulation of an areal geometry as a flat
/// `[a, b, c, a]`-per-kept-face vertex stream. This is the one publisher used
/// by both scalar and packed surfaces; it consumes one checked mesh plan and
/// applies the active-refinement XY policy before emitting.
pub(crate) fn constrained_triangle_vertices<'a>(
    rings: impl Iterator<Item = (RingOwner, &'a CoordSeq)>,
    area: f64,
    refinement: CdtRefinement,
    budget: &mut ExpansionBudget,
) -> Result<Vec<Point>> {
    let plan = constrained_triangle_plan(rings, area, refinement)?;
    publish_constrained_plan(&plan, budget)
}

#[expect(
    clippy::indexing_slicing,
    reason = "the private plan's face site IDs are produced by its completed constrained mesh"
)]
fn publish_constrained_plan(
    plan: &cdt::ConstrainedMeshPlan,
    budget: &mut ExpansionBudget,
) -> Result<Vec<Point>> {
    plan.validate_certificate()?;
    let faces = &plan.faces;
    let capacity = faces.len().wrapping_mul(4);
    for &[a, b, c] in faces {
        validate_refined_triangle(
            plan.vertices[a],
            plan.vertices[b],
            plan.vertices[c],
            plan.refinement,
        )?;
    }
    budget.check_additional(capacity)?;
    #[cfg(test)]
    note_constrained_output_reserve();
    let mut output = Vec::with_capacity(capacity);
    for &[a, b, c] in faces {
        let [a, b, c, _] = selected_triangle_points(&plan.vertices, [a, b, c], plan.force_xy);
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
    budget.add(capacity)?;
    Ok(output)
}

#[expect(
    clippy::indexing_slicing,
    reason = "the selected face contains only site IDs produced by the mesh over this point column"
)]
pub(super) fn selected_triangle_points(
    points: &[Point],
    [a, b, c]: [usize; 3],
    force_xy: bool,
) -> [Point; 4] {
    let points = [points[a], points[b], points[c]];
    if force_xy {
        return [
            points[0].to_xy(),
            points[1].to_xy(),
            points[2].to_xy(),
            points[0].to_xy(),
        ];
    }
    let axes = CoordinateAxes::new(
        HasZ(points.iter().all(|point| point.axes.has_z())),
        HasM(points.iter().all(|point| point.axes.has_m())),
    );
    let point = |point: Point| {
        Point::new_raw_axes(
            point.x,
            point.y,
            ZOrdinate(axes.has_z().then(|| point.z().expect("shared Z axis"))),
            MOrdinate(axes.has_m().then(|| point.m().expect("shared M axis"))),
        )
    };
    [
        point(points[0]),
        point(points[1]),
        point(points[2]),
        point(points[0]),
    ]
}

/// Select the first full source point at each XY key, then order sites by XY
/// for the exact mesh. Keep its original occurrence aligned through source-plan
/// admission; endpoint publication uses the retained point itself.
pub(super) fn selected_source_sites(points: Vec<Point>) -> Result<(Vec<Point>, Vec<usize>)> {
    let mut selected = crate::collections::try_hash_map_with_capacity(points.len())?;
    for (occurrence, point) in points.into_iter().enumerate() {
        selected
            .entry(PointKey::new(point))
            .or_insert((point, occurrence));
    }
    let mut selected_values = Vec::with_capacity(selected.len());
    selected_values.extend(selected.into_values());
    let mut selected = selected_values;
    selected.sort_unstable_by(|(left, _), (right, _)| {
        left.x
            .total_cmp(&right.x)
            .then_with(|| left.y.total_cmp(&right.y))
    });
    let mut sites = Vec::new();
    let mut occurrences = Vec::new();
    sites.reserve(selected.len());
    occurrences.reserve(selected.len());
    for (point, occurrence) in selected {
        sites.push(point);
        occurrences.push(occurrence);
    }
    Ok((sites, occurrences))
}

struct ConstrainedSourcePlan {
    vertices: Vec<Point>,
    occurrences: Vec<usize>,
    sources: Vec<cdt::SourceConstraint>,
    source_rings: Vec<cdt::SourceRingStart>,
}

#[expect(
    clippy::indexing_slicing,
    reason = "source endpoint keys come from the ring points collected above, and their site IDs index the selected vertex column used to build the map"
)]
fn constrained_source_plan(rings: &[(RingOwner, &CoordSeq)]) -> Result<ConstrainedSourcePlan> {
    let capacity = rings.iter().try_fold(0_usize, |total, ring| {
        total
            .checked_add(ring.1.coord_count())
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let mut source_points = Vec::with_capacity(capacity);
    for &(_, ring) in rings {
        for index in 0..ring.coord_count() {
            source_points.push(ring.nth_coord(index));
        }
    }
    let (vertices, occurrences) = selected_source_sites(source_points)?;
    let mut indices = crate::collections::try_hash_map_with_capacity(vertices.len())?;
    for (site, point) in vertices.iter().enumerate() {
        indices.insert(PointKey::new(*point), site);
    }
    let mut sources = Vec::with_capacity(capacity);
    let mut source_rings = Vec::with_capacity(rings.len());
    let mut seen = crate::collections::try_hash_set_with_capacity(capacity)?;
    for &(owner, ring) in rings {
        source_rings.push(cdt::SourceRingStart::new(sources.len(), owner));
        for index in 0..ring.coord_count().saturating_sub(1) {
            let start = ring.nth_coord(index);
            let end = ring.nth_coord(index.wrapping_add(1));
            if same_point(start.xy(), end.xy()) {
                continue;
            }
            let key = ordered_constraint_key(start.xy(), end.xy());
            if !seen.insert(key) {
                let previous = sources
                    .iter()
                    .find(|source: &&cdt::SourceConstraint| {
                        ordered_constraint_key(source.start.xy(), source.end.xy()) == key
                    })
                    .expect("a seen source edge has an admitted source constraint");
                let previous_owner = cdt::source_ring_owner(previous.id, &source_rings)
                    .expect("an admitted source constraint belongs to a source ring");
                return Err(overlapping_rings_error(previous_owner, owner));
            }
            let start_site = indices[&PointKey::new(start)];
            let end_site = indices[&PointKey::new(end)];
            let id = sources.len();
            sources.push(cdt::SourceConstraint {
                id,
                start: vertices[start_site],
                end: vertices[end_site],
                start_site,
                end_site,
            });
        }
    }
    Ok(ConstrainedSourcePlan {
        vertices,
        occurrences,
        sources,
        source_rings,
    })
}

fn ordered_constraint_key(a: XY, b: XY) -> (PointKey, PointKey) {
    let (a, b) = (PointKey::new(a), PointKey::new(b));
    if a <= b { (a, b) } else { (b, a) }
}

/// Prepare constraint segments: exact endpoint-key deduplication
/// (bit-identical XY only) and drop zero-length / duplicate undirected edges.
/// Absolute proximity snapping was deleted — a scale-free 1e-4 radius
/// annihilated valid small polygons (5e-5 square → zero triangles).
#[cfg(test)]
pub(crate) fn cleanup_constraint_lines(lines: Vec<[XY; 2]>) -> Vec<[XY; 2]> {
    let mut prepared: Vec<[XY; 2]> = Vec::with_capacity(lines.len());
    let mut seen = std::collections::HashSet::with_capacity(lines.len());
    for [start, end] in lines {
        if !same_point(start, end) && seen.insert(ordered_constraint_key(start, end)) {
            prepared.push([start, end]);
        }
    }
    prepared
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sites(points: &[(f64, f64)]) -> Vec<Site> {
        points
            .iter()
            .enumerate()
            .map(|(id, &(x, y))| Site {
                id,
                point: Point::new_raw_xy(x, y),
            })
            .collect()
    }

    #[test]
    fn hull_retains_collinear_boundary_sites_and_collinear_input_is_empty() {
        let boundary_sites = sites(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0)]);
        let mesh = certified_delaunay(&boundary_sites).unwrap();
        let mut hull_sites = mesh.hull().to_vec();
        hull_sites.sort_unstable();
        assert_eq!(hull_sites.as_slice(), &[0, 1, 2, 3, 4]);
        let center = Point::new_raw_xy(1.0, 1.0).xy();
        for (&start, &end) in mesh.hull().iter().zip(mesh.hull().iter().cycle().skip(1)) {
            let start = boundary_sites
                .get(start)
                .expect("hull sites come from the input")
                .point
                .xy();
            let end = boundary_sites
                .get(end)
                .expect("hull sites come from the input")
                .point
                .xy();
            assert_eq!(
                exact::orient2d(start, end, center),
                ExactSign::Positive,
                "the hull cycle is counter-clockwise"
            );
        }

        let collinear = sites(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)]);
        let mesh = certified_delaunay(&collinear).unwrap();
        assert!(mesh.triangles().is_empty());
        let mut hull_sites = mesh.hull().to_vec();
        hull_sites.sort_unstable();
        assert_eq!(hull_sites.as_slice(), &[0, 1, 2]);
    }

    #[test]
    fn cocircular_state_is_reciprocal() {
        let sites = sites(&[(1.0, 0.0), (0.0, 1.0), (-1.0, 0.0), (0.0, -1.0)]);
        let mesh = certified_delaunay(&sites).unwrap();
        let mut cocircular_pairs = 0;
        for (slot, &reverse) in mesh.halfedges().iter().enumerate() {
            if reverse == EMPTY {
                continue;
            }
            assert_eq!(
                mesh.edge_is_cocircular(slot),
                mesh.edge_is_cocircular(reverse)
            );
            cocircular_pairs += usize::from(mesh.edge_is_cocircular(slot));
        }
        assert_eq!(cocircular_pairs, 2);
    }

    #[test]
    fn constrained_source_plan_keeps_first_full_signed_zero_occurrence_and_ordinals() {
        let point = |x: f64, y: f64, z: f64, m: f64| {
            Point::new_raw_axes(x, y, ZOrdinate(Some(z)), MOrdinate(Some(m)))
        };
        let first = point(-0.0, 0.0, 7.0, 70.0);
        let duplicate = point(0.0, -0.0, 99.0, 990.0);
        let zero_edge_ring =
            crate::geometry::Ring::closed(vec![first, duplicate, first, first]).unwrap();
        let source_ring = crate::geometry::Ring::closed(vec![
            first,
            duplicate,
            point(2.0, 0.0, 8.0, 80.0),
            point(2.0, 2.0, 9.0, 90.0),
            point(-2.0, 2.0, 10.0, 100.0),
            point(-2.0, 0.0, 11.0, 110.0),
            first,
        ])
        .unwrap();
        let rings = [
            (RingOwner::new(7, 0), zero_edge_ring.coords()),
            (RingOwner::new(7, 1), source_ring.coords()),
        ];

        let plan = constrained_source_plan(&rings).unwrap();

        assert_eq!(
            plan.vertices
                .iter()
                .map(|point| (point.x.to_bits(), point.y.to_bits(), point.z(), point.m(),))
                .collect::<Vec<_>>(),
            [
                (
                    (-2.0_f64).to_bits(),
                    0.0_f64.to_bits(),
                    Some(11.0),
                    Some(110.0)
                ),
                (
                    (-2.0_f64).to_bits(),
                    2.0_f64.to_bits(),
                    Some(10.0),
                    Some(100.0)
                ),
                (
                    (-0.0_f64).to_bits(),
                    0.0_f64.to_bits(),
                    Some(7.0),
                    Some(70.0)
                ),
                (2.0_f64.to_bits(), 0.0_f64.to_bits(), Some(8.0), Some(80.0)),
                (2.0_f64.to_bits(), 2.0_f64.to_bits(), Some(9.0), Some(90.0)),
            ]
        );
        assert_eq!(plan.occurrences, [9, 8, 0, 6, 7]);
        assert_eq!(
            plan.sources
                .iter()
                .map(|source| (source.id, source.start_site, source.end_site))
                .collect::<Vec<_>>(),
            [(0, 2, 3), (1, 3, 4), (2, 4, 1), (3, 1, 0), (4, 0, 2)]
        );
        assert_eq!(
            plan.sources
                .iter()
                .map(|source| (
                    source.start.z(),
                    source.start.m(),
                    source.end.z(),
                    source.end.m()
                ))
                .collect::<Vec<_>>(),
            [
                (Some(7.0), Some(70.0), Some(8.0), Some(80.0)),
                (Some(8.0), Some(80.0), Some(9.0), Some(90.0)),
                (Some(9.0), Some(90.0), Some(10.0), Some(100.0)),
                (Some(10.0), Some(100.0), Some(11.0), Some(110.0)),
                (Some(11.0), Some(110.0), Some(7.0), Some(70.0)),
            ]
        );
        assert_eq!(
            cdt::source_ring_owner(0, &plan.source_rings),
            Some(RingOwner::new(7, 1)),
            "the source row follows a ring with no nonzero XY edges"
        );
    }

    #[test]
    fn public_topology_is_clockwise_and_euler_complete() {
        let points: Vec<Point> = [
            (0.0, 0.0),
            (4.0, 0.0),
            (4.0, 3.0),
            (0.0, 3.0),
            (1.0, 1.0),
            (2.5, 2.0),
        ]
        .into_iter()
        .map(|(x, y)| Point::new_raw_xy(x, y))
        .collect();
        let topology = delaunay_triangulation(&points).unwrap();
        assert!(delaunay_euler_complete(points.len(), &topology));
        for &[a, b, c] in topology.triangles.as_chunks::<3>().0 {
            assert_eq!(
                orientation(points[a].xy(), points[b].xy(), points[c].xy()),
                Orientation::Clockwise
            );
        }
        for (slot, &twin) in topology.halfedges.iter().enumerate() {
            if twin != EMPTY {
                assert_eq!(topology.halfedges[twin], slot);
            }
        }
        assert_eq!(topology.hull, [0, 1, 2, 3]);
    }
}
