//! Exact Delaunay triangulation by incremental insertion.
//!
//! Every decision is an exact predicate: the hull is the crate's exact corner
//! hull, the seed fan over it is legalized by Lawson flips under exact
//! `incircle`, point location walks by exact `orient2d`, and a cavity is the
//! set of triangles selected by the canonical incircle sign. Cavity insertion
//! keeps a Delaunay mesh Delaunay, so the mesh is Delaunay as built and nothing
//! has to be certified or repaired afterwards. Cocircular sites use a symbolic
//! lift to select a canonical diagonal independent of insertion order, while a
//! separate final exact pass flags geometric-zero edges for Voronoi fan collapse.
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

use std::hash::Hash;

use mesh::{bounds, hilbert16};
use ordering::{order_interior, symbolic_incircle_tie};

use crate::error::Result;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::tessellation::exact;
use crate::geometry::{Point, XY, canonicalize_zero};

#[path = "exact_delaunay/mesh.rs"]
mod mesh;
#[path = "exact_delaunay/ordering.rs"]
mod ordering;

#[cfg(test)]
#[path = "exact_delaunay/tests.rs"]
mod tests;

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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct FaceId(usize);

impl FaceId {
    const fn from_index(index: usize) -> Self {
        Self(index)
    }

    pub(super) const fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
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
#[cfg(test)]
pub(super) fn exact_delaunay(points: &[Point]) -> Result<ExactMesh> {
    let xy = |site| site_xy(points, site);
    exact_delaunay_with(&xy, points.len())
}

/// A typed coordinate view over one exact mesh. `Query` distinguishes an
/// admitted site from an arbitrary raw XY query used by constrained insertion.
pub(in crate::geometry::tessellation) trait MeshCoordinates {
    type Query: Copy;
    type Key: Eq + Hash;

    fn query(&self, site: usize) -> Self::Query;
    fn key(&self, query: Self::Query) -> Self::Key;
    fn compare(&self, left: Self::Query, right: Self::Query) -> std::cmp::Ordering;
    fn compare_x(&self, left: Self::Query, right: Self::Query) -> std::cmp::Ordering;
    fn compare_y(&self, left: Self::Query, right: Self::Query) -> std::cmp::Ordering;
    fn orient(&self, points: [Self::Query; 3]) -> Result<ExactSign>;
    fn incircle(&self, points: [Self::Query; 4]) -> Result<ExactSign>;
    fn compare_axis_spans(
        &self,
        x_extremes: [Self::Query; 2],
        y_extremes: [Self::Query; 2],
    ) -> Result<std::cmp::Ordering>;
    fn locality_xy(&self, site: usize) -> XY;
}

pub(in crate::geometry::tessellation) fn compare_f64(left: f64, right: f64) -> std::cmp::Ordering {
    canonicalize_zero(left).total_cmp(&canonicalize_zero(right))
}

impl<F: Fn(usize) -> XY> MeshCoordinates for F {
    type Query = XY;
    type Key = [u64; 2];

    fn query(&self, site: usize) -> Self::Query {
        self(site)
    }

    fn key(&self, query: Self::Query) -> Self::Key {
        let normalized = |value: f64| {
            if value == 0.0 {
                0.0_f64.to_bits()
            } else {
                value.to_bits()
            }
        };
        [normalized(query.x), normalized(query.y)]
    }

    fn compare(&self, left: Self::Query, right: Self::Query) -> std::cmp::Ordering {
        compare_xy(left, right)
    }

    fn compare_x(&self, left: Self::Query, right: Self::Query) -> std::cmp::Ordering {
        compare_f64(left.x, right.x)
    }

    fn compare_y(&self, left: Self::Query, right: Self::Query) -> std::cmp::Ordering {
        compare_f64(left.y, right.y)
    }

    fn orient(&self, [a, b, c]: [Self::Query; 3]) -> Result<ExactSign> {
        Ok(exact::orient2d(a, b, c))
    }

    fn incircle(&self, [a, b, c, d]: [Self::Query; 4]) -> Result<ExactSign> {
        exact::incircle(a, b, c, d)
    }

    fn compare_axis_spans(
        &self,
        [min_x, max_x]: [Self::Query; 2],
        [min_y, max_y]: [Self::Query; 2],
    ) -> Result<std::cmp::Ordering> {
        Ok(compare_f64(max_x.x - min_x.x, max_y.y - min_y.y))
    }

    fn locality_xy(&self, site: usize) -> XY {
        self(site)
    }
}

pub(super) fn exact_delaunay_with<C: MeshCoordinates>(
    coordinates: &C,
    site_count: usize,
) -> Result<ExactMesh> {
    let Some(mesh) = Mesh::try_delaunay(coordinates, site_count)? else {
        return Ok(ExactMesh {
            triangles: Vec::new(),
            halfedges: Vec::new(),
            hull: super::exact_hull_cycle_with(coordinates, site_count)?,
            cocircular: Vec::new(),
        });
    };
    mesh.finish(coordinates)
}

#[expect(
    clippy::indexing_slicing,
    reason = "site ordinals are admitted by this exact mesh from the same point slice"
)]
pub(in crate::geometry::tessellation) const fn site_xy(points: &[Point], site: usize) -> XY {
    points[site].xy()
}

pub(in crate::geometry::tessellation) fn compare_xy(left: XY, right: XY) -> std::cmp::Ordering {
    compare_f64(left.x, right.x).then_with(|| compare_f64(left.y, right.y))
}

fn canonical_incircle_for<C: MeshCoordinates>(
    coordinates: &C,
    points: [C::Query; 4],
) -> Result<ExactSign> {
    match coordinates.incircle(points)? {
        ExactSign::Zero => symbolic_incircle_tie(coordinates, points),
        sign @ (ExactSign::Negative | ExactSign::Positive) => Ok(sign),
    }
}

#[cfg(test)]
fn canonical_incircle(a: XY, b: XY, c: XY, d: XY) -> Result<ExactSign> {
    let sites = [a, b, c, d];
    let xy = |site| {
        *sites
            .get(site)
            .expect("the tie predicate receives only its four admitted sites")
    };
    canonical_incircle_for(&xy, [a, b, c, d])
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CanonicalTriangle {
    pub(super) selected_face: usize,
    pub(super) selected_sites: [usize; 3],
    pub(super) output_sites: [usize; 3],
    pub(super) output_to_selected: [u8; 3],
    pub(super) reversed: bool,
}

pub(super) fn canonical_triangle_order<C: MeshCoordinates>(
    coordinates: &C,
    triangles: &[[usize; 3]],
) -> Vec<CanonicalTriangle> {
    let mut faces = triangles
        .iter()
        .copied()
        .enumerate()
        .map(|(selected_face, selected_sites)| {
            let [a, b, c] = selected_sites;
            let rotation = if coordinates
                .compare(coordinates.query(b), coordinates.query(a))
                .is_lt()
                && coordinates
                    .compare(coordinates.query(b), coordinates.query(c))
                    .is_lt()
            {
                1
            } else if coordinates
                .compare(coordinates.query(c), coordinates.query(a))
                .is_lt()
            {
                2
            } else {
                0
            };
            let (output_sites, output_to_selected) = match rotation {
                1 => ([b, c, a], [1, 2, 0]),
                2 => ([c, a, b], [2, 0, 1]),
                _ => ([a, b, c], [0, 1, 2]),
            };
            CanonicalTriangle {
                selected_face,
                selected_sites,
                output_sites,
                output_to_selected,
                reversed: false,
            }
        })
        .collect::<Vec<_>>();
    faces.sort_unstable_by(|left, right| {
        left.output_sites
            .iter()
            .zip(right.output_sites)
            .map(|(&left, right)| {
                coordinates.compare(coordinates.query(left), coordinates.query(right))
            })
            .find(|order| !order.is_eq())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    faces
}

impl Mesh {
    /// Exact Delaunay construction for constrained and unconstrained publication.
    pub(super) fn try_delaunay<C: MeshCoordinates>(
        coordinates: &C,
        site_count: usize,
    ) -> Result<Option<Self>> {
        let hull = super::exact_corner_hull_cycle_with(coordinates, site_count)?;
        if hull.len() < 3 {
            return Ok(None);
        }
        let mut mesh = Self::seed(&hull, site_count);
        mesh.try_legalize(coordinates)?;

        // Remaining distinct sites exclude only strict hull corners, so a
        // collinear hull site is inserted along with the interior sites.
        let mut placed = crate::collections::try_hash_set_with_capacity(site_count)?;
        for &site in &hull {
            placed.insert(coordinates.key(coordinates.query(site)));
        }
        let mut hull_sites = crate::collections::try_hash_set_with_capacity(hull.len())?;
        for &site in &hull {
            hull_sites.insert(site);
        }
        let (min, max) = bounds(coordinates, site_count);
        let mut interior = Vec::with_capacity(site_count);
        for site in 0..site_count {
            if !hull_sites.contains(&site)
                && placed.insert(coordinates.key(coordinates.query(site)))
            {
                interior.push((hilbert16(coordinates.locality_xy(site), min, max), site));
            }
        }
        order_interior(&mut interior);

        let mut last = FaceId::from_index(0);
        for (_, site) in interior {
            let located = mesh.locate(coordinates, last, coordinates.query(site))?;
            last = mesh
                .try_insert_with(coordinates, site, located, None, |_, _| ())?
                .0;
        }
        Ok(Some(mesh))
    }
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
    Outside(HalfedgeId),
}

/// Result of the geometric location walk; arithmetic failures stay in `Result`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LocationOutcome {
    Located(Located),
    Blocked(HalfedgeId),
}

pub(super) struct OutsideInsertion {
    pub(super) changed: Vec<FaceId>,
    pub(super) legalization: Vec<HalfedgeId>,
}

#[derive(Debug)]
struct MeshHull {
    first: usize,
    links: Vec<[usize; 2]>,
    len: usize,
}

pub(super) struct Mesh {
    triangles: Vec<usize>,
    halfedges: Vec<usize>,
    hull: MeshHull,
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
