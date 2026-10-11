//! Delaunay and constrained Delaunay triangulation over the exact engine.
//!
//! [`exact_delaunay`] decides every step of the triangulation with exact
//! predicates, so a mesh is Delaunay as built and carries its cocircular
//! flags out of one exact pass; [`cdt`] forces constraint edges into that
//! mesh and refines the faces inside them. This module owns the site and
//! mesh types the Voronoi dual reads, the hull and collinear orders the
//! engine seeds from, and the public entry points.

use std::cmp::Ordering;

use crate::geographic::{ChartPointSite, GeographicFrame, PointSelectionChart};
use crate::geometry::exact_arith::{ExactSign, retained_site_longitude, retained_site_orientation};
use crate::geometry::tessellation::{
    EMPTY, MeshEdgeRole, MeshProducer, MeshSiteLocation, MeshUseId, PreparedMeshBoundaryUse,
    PreparedMeshFace, PreparedMeshPublication, PreparedMeshSite, RingOwner, Triangulation, exact,
    next_halfedge, overlapping_rings_error, prev_halfedge,
};
use crate::geometry::{
    CoordSeq, CoordinateAxes, Coordinates as _, ExpansionBudget, GENERATED_ITEM_LIMIT,
    GeometryErrorKind, HasM, HasZ, MOrdinate, Orientation, Point, PointKey, Result, XY, ZOrdinate,
    open_xy_cycle_magnitude, orientation, same_point,
};

#[cfg(test)]
thread_local! {
    static DELAUNAY_OUTPUT_RESERVES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn note_delaunay_output_reserve() {
    DELAUNAY_OUTPUT_RESERVES.with(|count| {
        let reserves = count
            .get()
            .checked_add(1)
            .expect("Delaunay output reserve test counter overflow");
        count.set(reserves);
    });
}

#[cfg(test)]
pub(super) fn take_delaunay_output_reserves() -> usize {
    DELAUNAY_OUTPUT_RESERVES.with(|count| count.replace(0))
}

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
    let xy = |site| exact_delaunay::site_xy(&points, site);
    certified_delaunay_with_coordinates(&xy, points.len())
}

pub(in crate::geometry::tessellation) fn certified_delaunay_with_coordinates<
    C: exact_delaunay::MeshCoordinates,
>(
    coordinates: &C,
    site_count: usize,
) -> Result<CertifiedDelaunay> {
    let mut mesh = exact_delaunay::exact_delaunay_with(coordinates, site_count)?;
    if mesh.triangles.is_empty() {
        return Ok(CertifiedDelaunay::empty(mesh.hull));
    }
    let cocircular_halfedges = std::mem::take(&mut mesh.cocircular);
    Ok(CertifiedDelaunay {
        topology: mesh.into_topology(),
        cocircular_halfedges,
    })
}

/// Borrowed exact coordinates for one already admitted geographic point chart.
/// Mesh ordinals index `sites`; source ordinals remain untouched in each site.
pub(in crate::geometry::tessellation) struct RetainedPointCoordinates<'a> {
    frame: GeographicFrame,
    sites: &'a [ChartPointSite],
}

impl<'a> RetainedPointCoordinates<'a> {
    pub(in crate::geometry::tessellation) const fn new(
        frame: GeographicFrame,
        sites: &'a [ChartPointSite],
    ) -> Self {
        Self { frame, sites }
    }

    fn site(&self, query: usize) -> ChartPointSite {
        self.sites
            .get(query)
            .copied()
            .expect("mesh site ordinals come from the admitted retained site rows")
    }

    fn exact_x(&self, query: usize) -> Result<exact::ExactDyadic> {
        let site = self.site(query);
        retained_site_longitude(self.frame.longitude(), site.longitude())
    }

    fn exact_y(&self, query: usize) -> exact::ExactDyadic {
        exact::ExactDyadic::from_f64(self.site(query).source_y())
    }

    fn exact_xy(&self, query: usize) -> Result<[exact::ExactDyadic; 2]> {
        Ok([self.exact_x(query)?, self.exact_y(query)])
    }
}

impl exact_delaunay::MeshCoordinates for RetainedPointCoordinates<'_> {
    type Query = usize;
    type Key = (i64, u64, u64);

    fn query(&self, site: usize) -> Self::Query {
        site
    }

    fn key(&self, query: Self::Query) -> Self::Key {
        let site = self.site(query);
        let normalize = |value: f64| {
            if value == 0.0 {
                0.0_f64.to_bits()
            } else {
                value.to_bits()
            }
        };
        (
            site.longitude().periods(),
            normalize(site.longitude().native().get()),
            normalize(site.source_y()),
        )
    }

    fn compare(&self, left: Self::Query, right: Self::Query) -> Ordering {
        self.site(left).compare_chart_xy(self.site(right))
    }

    fn compare_x(&self, left: Self::Query, right: Self::Query) -> Ordering {
        let left = self.site(left);
        let right = self.site(right);
        left.longitude()
            .periods()
            .cmp(&right.longitude().periods())
            .then_with(|| {
                left.longitude()
                    .native()
                    .get()
                    .total_cmp(&right.longitude().native().get())
            })
    }

    fn compare_y(&self, left: Self::Query, right: Self::Query) -> Ordering {
        self.site(left)
            .source_y()
            .total_cmp(&self.site(right).source_y())
    }

    fn orient(&self, [a, b, c]: [Self::Query; 3]) -> Result<ExactSign> {
        let a = self.site(a);
        let b = self.site(b);
        let c = self.site(c);
        retained_site_orientation(self.frame.longitude(), &[
            (a.longitude(), a.source_y()),
            (b.longitude(), b.source_y()),
            (c.longitude(), c.source_y()),
        ])
    }

    fn incircle(&self, [a, b, c, d]: [Self::Query; 4]) -> Result<ExactSign> {
        exact::incircle_dyadic_points(
            self.exact_xy(a)?,
            self.exact_xy(b)?,
            self.exact_xy(c)?,
            self.exact_xy(d)?,
        )
    }

    fn compare_axis_spans(
        &self,
        [min_x, max_x]: [Self::Query; 2],
        [min_y, max_y]: [Self::Query; 2],
    ) -> Result<Ordering> {
        let min_chart_x = self.exact_x(min_x)?;
        let max_chart_x = self.exact_x(max_x)?;
        let min_chart_y = self.exact_y(min_y);
        let max_chart_y = self.exact_y(max_y);
        let x = max_chart_x.try_subtract(min_chart_x)?.abs();
        let y = max_chart_y.try_subtract(min_chart_y)?.abs();
        Ok(x.cmp(&y))
    }

    fn locality_xy(&self, site: usize) -> XY {
        let site = self.site(site);
        // This is only a deterministic BRIO/Hilbert proposal. Exact identity,
        // order, orientation and in-circle never consume these rounded values.
        XY::new(site.longitude().native().get(), site.source_y())
    }
}

/// Build the exact primal mesh from retained chart sites without publishing it.
pub(in crate::geometry::tessellation) fn certified_delaunay_for_retained_points(
    frame: GeographicFrame,
    sites: &[ChartPointSite],
) -> Result<CertifiedDelaunay> {
    let coordinates = RetainedPointCoordinates::new(frame, sites);
    certified_delaunay_with_coordinates(&coordinates, sites.len())
}

pub(in crate::geometry::tessellation) fn prepare_retained_delaunay_publication(
    frame: GeographicFrame,
    source_points: &[Point],
    budget: &mut ExpansionBudget,
) -> Result<PreparedMeshPublication> {
    let Some(chart) = PointSelectionChart::try_from_points(frame, source_points)? else {
        return Ok(PreparedMeshPublication::new(
            MeshProducer::Delaunay,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));
    };
    let mut chart_sites = chart.sites_from_points(source_points)?;
    chart_sites.sort_unstable_by(|left, right| left.compare_chart_xy(*right));
    let sites = chart_sites
        .iter()
        .enumerate()
        .map(|(producer_ordinal, &site)| {
            PreparedMeshSite::source(
                producer_ordinal,
                site.source_ordinal(),
                *source_points
                    .get(site.source_ordinal())
                    .expect("retained source ordinal belongs to the source point inventory"),
                MeshSiteLocation::Retained(site),
            )
        })
        .collect::<Vec<_>>();
    let coordinates = RetainedPointCoordinates::new(frame, &chart_sites);
    let mesh = certified_delaunay_for_retained_points(frame, &chart_sites)?;
    prepare_delaunay_publication(&mesh, sites, &coordinates, budget)
}

pub(in crate::geometry::tessellation) fn prepare_raw_delaunay_publication(
    source_points: &[Point],
    budget: &mut ExpansionBudget,
) -> Result<PreparedMeshPublication> {
    let mut indexed_points = source_points
        .iter()
        .copied()
        .enumerate()
        .collect::<Vec<_>>();
    indexed_points.sort_unstable_by(|left, right| {
        left.1
            .x
            .total_cmp(&right.1.x)
            .then_with(|| left.1.y.total_cmp(&right.1.y))
    });
    let sites = indexed_points
        .iter()
        .enumerate()
        .map(|(producer_ordinal, &(occurrence, point))| {
            PreparedMeshSite::source(
                producer_ordinal,
                occurrence,
                point,
                MeshSiteLocation::Cartesian,
            )
        })
        .collect::<Vec<_>>();
    let points = indexed_points
        .iter()
        .map(|&(_, point)| point)
        .collect::<Vec<_>>();
    let provider_sites = points
        .iter()
        .enumerate()
        .map(|(id, &point)| Site { id, point })
        .collect::<Vec<_>>();
    let mesh = certified_delaunay(&provider_sites)?;
    let coordinates = |site| exact_delaunay::site_xy(&points, site);
    prepare_delaunay_publication(&mesh, sites, &coordinates, budget)
}

#[expect(
    clippy::indexing_slicing,
    reason = "canonical side maps and halfedge columns are emitted together by the same certified mesh"
)]
fn prepare_delaunay_publication<C: exact_delaunay::MeshCoordinates>(
    mesh: &CertifiedDelaunay,
    sites: Vec<PreparedMeshSite>,
    coordinates: &C,
    budget: &mut ExpansionBudget,
) -> Result<PreparedMeshPublication> {
    let canonical_faces = exact_delaunay::canonical_triangle_order(coordinates, mesh.triangles());
    let capacity = canonical_faces
        .len()
        .checked_mul(4)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    budget.check_additional(capacity)?;
    #[cfg(test)]
    note_delaunay_output_reserve();

    let mut points = Vec::with_capacity(capacity);
    let mut faces = Vec::with_capacity(canonical_faces.len());
    for canonical in canonical_faces {
        let start = points.len();
        let output_points = canonical.output_sites.map(|site| {
            sites
                .get(site)
                .expect("canonical site ordinal belongs to the prepared site table")
                .source_record()
        });
        let [a, b, c, closure] = selected_triangle_points(&output_points, [0, 1, 2], false);
        points.extend_from_slice(&[a, b, c, closure]);
        let end = points.len();
        let boundary_uses = std::array::from_fn(|output_side| {
            let selected_side = usize::from(canonical.output_to_selected[output_side]);
            let selected_slot = canonical
                .selected_face
                .wrapping_mul(3)
                .wrapping_add(selected_side);
            let reverse_slot = mesh.halfedges()[selected_slot];
            let reverse = (reverse_slot != EMPTY).then(|| {
                MeshUseId::new(
                    reverse_slot / 3,
                    u8::try_from(reverse_slot % 3)
                        .expect("halfedge side is one of the three triangle sides"),
                )
            });
            let selected_use = MeshUseId::new(
                canonical.selected_face,
                u8::try_from(selected_side).expect("triangle side fits u8"),
            );
            PreparedMeshBoundaryUse::new(
                u8::try_from(output_side).expect("triangle side fits u8"),
                selected_use,
                canonical.output_sites[output_side],
                canonical.output_sites[(output_side + 1) % 3],
                reverse,
                canonical.reversed,
                mesh.edge_is_cocircular(selected_slot),
                MeshEdgeRole::Delaunay,
            )
        });
        faces.push(PreparedMeshFace::new(
            canonical.selected_face,
            canonical.selected_sites,
            canonical.output_sites,
            canonical.output_to_selected,
            canonical.reversed,
            start..end,
            boundary_uses,
        ));
    }
    budget.add(capacity)?;
    Ok(PreparedMeshPublication::new(
        MeshProducer::Delaunay,
        sites,
        faces,
        points,
    ))
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
    reason = "site IDs in order come from the provider, and lower/upper last-two reads follow length checks"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "lower and upper last-two reads follow len >= 2, so subtracting one or two cannot underflow"
)]
fn hull_cycle<C: exact_delaunay::MeshCoordinates>(
    coordinates: &C,
    site_count: usize,
    corners: bool,
) -> Result<Vec<usize>> {
    let mut order = Vec::with_capacity(site_count);
    order.extend(0..site_count);
    order
        .sort_unstable_by(|&a, &b| coordinates.compare(coordinates.query(a), coordinates.query(b)));
    if !corners {
        let Some(&last) = order.last() else {
            return Ok(Vec::new());
        };
        let mut all_collinear = order.len() < 3;
        if !all_collinear {
            let first = *order.first().expect("nonempty hull order");
            all_collinear = true;
            for &site in &order {
                if coordinates.orient([
                    coordinates.query(first),
                    coordinates.query(last),
                    coordinates.query(site),
                ])? != ExactSign::Zero
                {
                    all_collinear = false;
                    break;
                }
            }
        }
        if all_collinear {
            return collinear_order(coordinates, order);
        }
    }
    let pops = |sign: ExactSign| {
        if corners {
            sign != ExactSign::Positive
        } else {
            sign == ExactSign::Negative
        }
    };
    let mut lower = Vec::with_capacity(order.len());
    for &site in &order {
        while lower.len() >= 2 {
            let sign = coordinates.orient([
                coordinates.query(lower[lower.len() - 2]),
                coordinates.query(lower[lower.len() - 1]),
                coordinates.query(site),
            ])?;
            if !pops(sign) {
                break;
            }
            lower.pop();
        }
        lower.push(site);
    }
    let mut upper = Vec::with_capacity(order.len());
    for &site in order.iter().rev() {
        while upper.len() >= 2 {
            let sign = coordinates.orient([
                coordinates.query(upper[upper.len() - 2]),
                coordinates.query(upper[upper.len() - 1]),
                coordinates.query(site),
            ])?;
            if !pops(sign) {
                break;
            }
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
    Ok(lower)
}

#[cfg(test)]
pub(super) fn exact_corner_hull_cycle(points: &[Point]) -> Vec<usize> {
    let xy = |site: usize| points[site].xy();
    exact_corner_hull_cycle_with(&xy, points.len()).expect("stored XY orientation is infallible")
}

#[cfg(test)]
pub(super) fn exact_hull_cycle(points: &[Point]) -> Vec<usize> {
    let xy = |site: usize| points[site].xy();
    exact_hull_cycle_with(&xy, points.len()).expect("stored XY hull arithmetic is infallible")
}

pub(in crate::geometry::tessellation) fn exact_corner_hull_cycle_with<
    C: exact_delaunay::MeshCoordinates,
>(
    coordinates: &C,
    site_count: usize,
) -> Result<Vec<usize>> {
    hull_cycle(coordinates, site_count, true)
}

pub(in crate::geometry::tessellation) fn exact_hull_cycle_with<
    C: exact_delaunay::MeshCoordinates,
>(
    coordinates: &C,
    site_count: usize,
) -> Result<Vec<usize>> {
    hull_cycle(coordinates, site_count, false)
}

/// Collinear sites in order along their longer extent.
fn collinear_order<C: exact_delaunay::MeshCoordinates>(
    coordinates: &C,
    mut order: Vec<usize>,
) -> Result<Vec<usize>> {
    let Some(&first) = order.first() else {
        return Ok(order);
    };
    let (mut min_x, mut max_x, mut min_y, mut max_y) = (first, first, first, first);
    for &site in order.iter().skip(1) {
        let query = coordinates.query(site);
        if coordinates
            .compare_x(query, coordinates.query(min_x))
            .is_lt()
        {
            min_x = site;
        }
        if coordinates
            .compare_x(query, coordinates.query(max_x))
            .is_gt()
        {
            max_x = site;
        }
        if coordinates
            .compare_y(query, coordinates.query(min_y))
            .is_lt()
        {
            min_y = site;
        }
        if coordinates
            .compare_y(query, coordinates.query(max_y))
            .is_gt()
        {
            max_y = site;
        }
    }
    let x_is_longer = !coordinates
        .compare_axis_spans([coordinates.query(min_x), coordinates.query(max_x)], [
            coordinates.query(min_y),
            coordinates.query(max_y),
        ])?
        .is_lt();
    if x_is_longer {
        order.sort_unstable_by(|&left, &right| {
            coordinates
                .compare_x(coordinates.query(left), coordinates.query(right))
                .then_with(|| {
                    coordinates.compare_y(coordinates.query(left), coordinates.query(right))
                })
        });
    } else {
        order.sort_unstable_by(|&left, &right| {
            coordinates
                .compare_y(coordinates.query(left), coordinates.query(right))
                .then_with(|| {
                    coordinates.compare_x(coordinates.query(left), coordinates.query(right))
                })
        });
    }
    Ok(order)
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
        sources,
        source_rings,
    } = constrained_source_plan(&rings)?;
    let Some(mut cdt) =
        cdt::ConstrainedMesh::build_with_sources(&vertices, &sources, source_rings)?
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

/// Select the first full source point at each XY key, then order those points
/// by XY for the exact mesh.
fn selected_source_sites(points: Vec<Point>) -> Result<Vec<Point>> {
    let mut selected = crate::collections::try_hash_map_with_capacity(points.len())?;
    for point in points {
        selected.entry(PointKey::new(point)).or_insert(point);
    }
    let mut selected: Vec<_> = selected.into_values().collect();
    selected.sort_unstable_by(|left, right| {
        left.x
            .total_cmp(&right.x)
            .then_with(|| left.y.total_cmp(&right.y))
    });
    Ok(selected)
}

struct ConstrainedSourcePlan {
    vertices: Vec<Point>,
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
    let vertices = selected_source_sites(source_points)?;
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
    fn constrained_source_plan_keeps_first_full_signed_zero_point_and_source_ordinals() {
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
