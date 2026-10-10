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

use crate::error::Result;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::tessellation::exact;
use crate::geometry::{Point, XY, canonicalize_zero};

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

/// Lexicographically ordered infinitesimal vertical lifts of the paraboloid.
/// On a zero determinant, its first nonzero lift cofactor selects one diagonal;
/// the exact geometric predicate remains unchanged for circumcenter certificates.
fn symbolic_incircle_tie<C: MeshCoordinates>(
    coordinates: &C,
    [a, b, c, d]: [C::Query; 4],
) -> Result<ExactSign> {
    let mut sites = [(a, 0), (b, 1), (c, 2), (d, 3)];
    sites.sort_unstable_by(|&(left, _), &(right, _)| coordinates.compare(left, right));
    for (_, row) in sites {
        let (sign, negate) = match row {
            0 => (coordinates.orient([b, c, d])?, false),
            1 => (coordinates.orient([a, c, d])?, true),
            2 => (coordinates.orient([a, b, d])?, false),
            _ => (coordinates.orient([a, b, c])?, true),
        };
        if sign != ExactSign::Zero {
            return Ok(if negate {
                match sign {
                    ExactSign::Positive => ExactSign::Negative,
                    ExactSign::Negative => ExactSign::Positive,
                    ExactSign::Zero => ExactSign::Zero,
                }
            } else {
                sign
            });
        }
    }
    Ok(ExactSign::Zero)
}

macro_rules! canonical_incircle {
    ($coordinates:expr, $a:expr, $b:expr, $c:expr, $d:expr) => {{
        let coordinates = $coordinates;
        let [a, b, c, d] = [$a, $b, $c, $d];
        match coordinates.incircle([a, b, c, d])? {
            ExactSign::Zero => symbolic_incircle_tie(coordinates, [a, b, c, d])?,
            sign => sign,
        }
    }};
}

#[cfg(test)]
fn canonical_incircle(a: XY, b: XY, c: XY, d: XY) -> Result<ExactSign> {
    let sites = [a, b, c, d];
    let xy = |site| {
        *sites
            .get(site)
            .expect("the tie predicate receives only its four admitted sites")
    };
    Ok(canonical_incircle!(&xy, a, b, c, d))
}

pub(in crate::geometry::tessellation) fn canonicalize_triangles<C: MeshCoordinates>(
    coordinates: &C,
    triangles: &mut [[usize; 3]],
) {
    for triangle in triangles.iter_mut() {
        let [a, b, c] = *triangle;
        *triangle = if coordinates
            .compare(coordinates.query(b), coordinates.query(a))
            .is_lt()
            && coordinates
                .compare(coordinates.query(b), coordinates.query(c))
                .is_lt()
        {
            [b, c, a]
        } else if coordinates
            .compare(coordinates.query(c), coordinates.query(a))
            .is_lt()
        {
            [c, a, b]
        } else {
            [a, b, c]
        };
    }
    triangles.sort_unstable_by(|left, right| {
        left.iter()
            .zip(right)
            .map(|(&left, &right)| {
                coordinates.compare(coordinates.query(left), coordinates.query(right))
            })
            .find(|order| !order.is_eq())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
enum InsertionOrder {
    Original,
    Brio,
    Reversed,
}

#[cfg(test)]
thread_local! {
    static INSERTION_ORDER: std::cell::Cell<InsertionOrder> =
        const { std::cell::Cell::new(InsertionOrder::Brio) };
    static PERIMETER_STEPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn order_interior(interior: &mut [(u32, usize)]) {
    #[cfg(test)]
    match INSERTION_ORDER.with(std::cell::Cell::get) {
        InsertionOrder::Original => {
            interior.sort_unstable();
            return;
        },
        InsertionOrder::Reversed => {
            interior.sort_unstable();
            interior.reverse();
            return;
        },
        InsertionOrder::Brio => {},
    }
    interior.sort_unstable_by_key(|&(hilbert, site)| {
        let mut sample = (site as u64).wrapping_add(264);
        sample = (sample ^ (sample >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        sample = (sample ^ (sample >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        sample ^= sample >> 31;
        (std::cmp::Reverse(sample.leading_zeros()), hilbert, site)
    });
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

fn bounds<C: MeshCoordinates>(coordinates: &C, site_count: usize) -> (XY, XY) {
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

    fn iter(&self) -> impl Iterator<Item = &usize> + Clone {
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

    fn previous(&self, site: usize) -> usize {
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
    pub(super) const fn side_value<T: Copy>(side: &[T], edge: HalfedgeId) -> T {
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
            hull: MeshHull::new(hull, vertex_count),
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
    pub(super) fn edge_slot(&self, u: usize, v: usize) -> Option<HalfedgeId> {
        if let Some(slot) = self.directed_slot(u, v) {
            return Some(slot);
        }
        self.directed_slot(v, u)
    }

    fn directed_slot(&self, u: usize, v: usize) -> Option<HalfedgeId> {
        self.find_outgoing(u, |edge| self.edge_end(edge) == v)
    }

    /// Lawson legalization of every interior edge: flip while the twin's apex
    /// lies strictly inside the circumcircle. The seed fan triangulates the
    /// hull without being Delaunay, and cavity insertion preserves the
    /// Delaunay property only from a Delaunay start. A slot left on the stack
    /// by a flip names whatever edge now lives there; re-checking a legal
    /// edge is harmless, and every edge a flip disturbed is pushed again.
    fn try_legalize<C: MeshCoordinates>(&mut self, coordinates: &C) -> Result<()> {
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
            if canonical_incircle!(
                coordinates,
                coordinates.query(a),
                coordinates.query(b),
                coordinates.query(c),
                coordinates.query(d)
            ) != ExactSign::Positive
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

    pub(super) fn try_legalize_constrained_from<C: MeshCoordinates>(
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
            if canonical_incircle!(
                coordinates,
                coordinates.query(a),
                coordinates.query(b),
                coordinates.query(c),
                coordinates.query(opposite)
            ) != ExactSign::Positive
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
    pub(super) fn locate<C: MeshCoordinates>(
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
    pub(super) fn locate_bounded<C: MeshCoordinates>(
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

    pub(super) fn outside_edge<C: MeshCoordinates>(
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
    pub(super) fn classify<C: MeshCoordinates>(
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
    pub(super) fn try_insert_with<C: MeshCoordinates, R>(
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
                if canonical_incircle!(
                    coordinates,
                    coordinates.query(a),
                    coordinates.query(b),
                    coordinates.query(c),
                    point
                ) == ExactSign::Positive
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

    pub(super) fn insert_constrained<C: MeshCoordinates>(
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

    pub(super) fn insert_outside<C: MeshCoordinates>(
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
    pub(super) fn finish<C: MeshCoordinates>(self, coordinates: &C) -> Result<ExactMesh> {
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

    #[test]
    fn symbolic_lift_ties_are_alternating_and_geometric_zeros_remain_zeros() {
        let [a, b, c, d] = [
            XY::new(0.0, 0.0),
            XY::new(1.0, 0.0),
            XY::new(1.0, 1.0),
            XY::new(0.0, 1.0),
        ];
        assert_eq!(exact::incircle(a, b, c, d).unwrap(), ExactSign::Zero);
        assert_eq!(canonical_incircle(a, b, c, d).unwrap(), ExactSign::Positive);
        assert_eq!(canonical_incircle(b, a, c, d).unwrap(), ExactSign::Negative);
        assert_eq!(canonical_incircle(a, c, b, d).unwrap(), ExactSign::Negative);
        assert_eq!(canonical_incircle(a, b, d, c).unwrap(), ExactSign::Negative);
    }

    struct FallbackScanCoordinates {
        points: [XY; 4],
        orientation_calls: std::cell::Cell<usize>,
    }

    impl MeshCoordinates for FallbackScanCoordinates {
        type Query = XY;
        type Key = [u64; 2];

        fn query(&self, site: usize) -> Self::Query {
            *self
                .points
                .get(site)
                .expect("fallback test queries an admitted square site")
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

        fn orient(&self, _points: [Self::Query; 3]) -> Result<ExactSign> {
            let call = self.orientation_calls.get().wrapping_add(1);
            self.orientation_calls.set(call);
            match call {
                3 | 4 | 7 => Ok(ExactSign::Negative),
                8 => Err(crate::error::ResourceError::SizeOverflow.into()),
                // If fallback scanning wrongly swallows call 8's error, the
                // next face receives an inside classification.
                _ => Ok(ExactSign::Positive),
            }
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
            self.query(site)
        }
    }

    #[test]
    fn location_propagates_arithmetic_error_from_the_bounded_fallback_scan() {
        // The seeded square has adjacent faces 0 and 1 across a shared edge.
        // Inject signs that walk 0 -> 1 -> 0 until the step limit selects the
        // fallback scan, then fail on that scan's first face. This is a
        // control-flow fault injection, not a geometric input fixture.
        let coordinates = FallbackScanCoordinates {
            points: [
                XY::new(0.0, 0.0),
                XY::new(2.0, 0.0),
                XY::new(2.0, 2.0),
                XY::new(0.0, 2.0),
            ],
            orientation_calls: std::cell::Cell::new(0),
        };
        let mesh = Mesh::seed(&[0, 1, 2, 3], 4);
        let error = mesh
            .locate_bounded(&coordinates, FaceId::from_index(0), XY::new(10.0, 10.0), &[
            ])
            .unwrap_err();
        assert_eq!(coordinates.orientation_calls.get(), 8);
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
        ));
    }

    #[test]
    fn public_delaunay_triangles_are_canonical_across_insertion_orders() {
        use std::fmt::Write as _;

        use pyo3::prelude::*;
        use pyo3::types::{PyDict, PyModule};

        struct RestoreOrder(InsertionOrder);
        impl Drop for RestoreOrder {
            fn drop(&mut self) {
                INSERTION_ORDER.with(|order| order.set(self.0));
            }
        }

        crate::test_support::initialize_python();
        let _restore = RestoreOrder(INSERTION_ORDER.with(std::cell::Cell::get));
        Python::attach(|py| {
            let module = PyModule::new(py, "gometry._lib").unwrap();
            crate::lib(&module).unwrap();
            let grid = (0..8)
                .flat_map(|x| (0..8).map(move |y| format!("({x} {y})")))
                .collect::<Vec<_>>()
                .join(",");

            let mut holes = String::new();
            for index in 0..8 {
                let x = f64::from(index);
                write!(
                    &mut holes,
                    ",({} 1,{} 1,{} 2,{} 2,{} 1)",
                    x + 0.2,
                    x + 0.8,
                    x + 0.8,
                    x + 0.2,
                    x + 0.2
                )
                .unwrap();
            }
            let fixtures = [
                (format!("MULTIPOINT({grid})"), "delaunay"),
                (
                    format!("POLYGON((0 0,9 0,9 3,0 3,0 0){holes})"),
                    "constrained",
                ),
                (
                    "POLYGON((0 0,8 0,8 8,4 3,0 8,0 0))".to_owned(),
                    "constrained",
                ),
            ];
            for (wkt, method) in fixtures {
                let polygon = module.getattr("from_wkt").unwrap().call1((wkt,)).unwrap();
                let options = PyDict::new(py);
                options.set_item("method", method).unwrap();
                let mut expected = None;
                for order in [
                    InsertionOrder::Original,
                    InsertionOrder::Brio,
                    InsertionOrder::Reversed,
                ] {
                    INSERTION_ORDER.with(|cell| cell.set(order));
                    let output = polygon
                        .call_method("triangulate", (), Some(&options))
                        .unwrap()
                        .call_method0("to_wkt")
                        .unwrap()
                        .extract::<Vec<String>>()
                        .unwrap();
                    if let Some(expected) = &expected {
                        assert_eq!(&output, expected);
                    } else {
                        expected = Some(output);
                    }
                }
            }

            let globals = PyDict::new(py);
            globals.set_item("gm", &module).unwrap();
            py.run(
                c"import math, random
rng = random.Random(0xCD7)
corpus = []
for case in range(200):
    n = rng.randint(4, 8)
    step = math.tau / n
    offset = rng.random() * math.tau
    ring = []
    for index in range(n):
        angle = offset + index * step + rng.uniform(-0.15, 0.15) * step
        radius = rng.randint(20, 80)
        ring.append((float(round(radius * math.cos(angle))), float(round(radius * math.sin(angle)))))
    polygon = gm.Polygon(ring)
    corpus.append((polygon, {'method': 'constrained'}))
",
                Some(&globals),
                None,
            )
            .unwrap();
            let corpus = globals
                .get_item("corpus")
                .unwrap()
                .unwrap()
                .extract::<Vec<(Bound<'_, PyAny>, Bound<'_, PyDict>)>>()
                .unwrap();
            for (case, (polygon, options)) in corpus.into_iter().enumerate() {
                let mut expected = None;
                for order in [
                    InsertionOrder::Original,
                    InsertionOrder::Brio,
                    InsertionOrder::Reversed,
                ] {
                    INSERTION_ORDER.with(|cell| cell.set(order));
                    let output = polygon
                        .call_method("triangulate", (), Some(&options))
                        .unwrap()
                        .call_method0("to_wkt")
                        .unwrap()
                        .extract::<Vec<String>>()
                        .unwrap();
                    if let Some(expected) = &expected {
                        assert_eq!(&output, expected, "corpus observation {case}");
                    } else {
                        expected = Some(output);
                    }
                }
            }
        });
    }

    #[test]
    fn public_refinement_is_canonical_across_insertion_and_ring_orders() {
        use pyo3::prelude::*;
        use pyo3::types::{PyDict, PyModule};

        struct RestoreOrder(InsertionOrder);
        impl Drop for RestoreOrder {
            fn drop(&mut self) {
                INSERTION_ORDER.with(|order| order.set(self.0));
            }
        }
        crate::test_support::initialize_python();
        let _restore = RestoreOrder(INSERTION_ORDER.with(std::cell::Cell::get));
        Python::attach(|py| {
            let module = PyModule::new(py, "gometry._lib").unwrap();
            crate::lib(&module).unwrap();
            let globals = PyDict::new(py);
            globals.set_item("gm", &module).unwrap();
            py.run(
                c"corpus = []
rings = [
    [(-75,16),(-63,-42),(-11,-75),(31,-39),(39,-9),(25,20),(4,70),(-18,23)],
    [(0,0),(20,0),(20,20),(0,20)],
    [(0,0),(8,0),(8,8),(4,3),(0,8)],
    [(0,0),(20,0),(20,2),(0,1)],
    [(33,-5),(43,32),(5,67),(-58,36),(-22,-7),(-15,-47),(26,-60)],
    [(-19,34),(-38,5),(-8,-45),(57,-55),(66,34)],
    [(54,59),(-1,46),(-64,46),(-74,-28),(-22,-71),(12,-21),(36,-5)],
    [(-50,-54),(4,-46),(46,-44),(22,2),(42,49),(-10,61),(-17,15),(-22,-3)],
    [(-37,6),(-36,-17),(-13,-42),(18,-25),(55,-20),(35,16),(5,40),(-38,63)],
]
for ring in rings:
    area = gm.Polygon(ring).area / 8
    for quality in [{'max_area': area}, {'min_angle': 20}, {'min_angle': 20, 'max_area': area}]:
        variants = []
        for reverse in (False, True):
            vertices = list(reversed(ring)) if reverse else ring
            for offset in range(len(vertices)):
                variants.append(gm.Polygon(vertices[offset:] + vertices[:offset]))
        corpus.append((variants, {'method': 'constrained', **quality}))
corpus.append(([gm.Polygon(rings[0])], {'method': 'constrained', 'max_area': 1117.5625}))
",
                Some(&globals),
                None,
            )
            .unwrap();
            let corpus = globals
                .get_item("corpus")
                .unwrap()
                .unwrap()
                .extract::<Vec<(Vec<Bound<'_, PyAny>>, Bound<'_, PyDict>)>>()
                .unwrap();
            for (case, (variants, options)) in corpus.into_iter().enumerate() {
                let mut expected = None;
                for polygon in variants {
                    for order in [
                        InsertionOrder::Original,
                        InsertionOrder::Brio,
                        InsertionOrder::Reversed,
                    ] {
                        INSERTION_ORDER.with(|cell| cell.set(order));
                        let output = polygon
                            .call_method("triangulate", (), Some(&options))
                            .unwrap()
                            .call_method0("to_wkt")
                            .unwrap()
                            .extract::<Vec<String>>()
                            .unwrap();
                        assert!(!output.is_empty(), "refinement case {case}");
                        if let Some(expected) = &expected {
                            assert_eq!(&output, expected, "refinement case {case}, {order:?}");
                        } else {
                            expected = Some(output);
                        }
                    }
                }
            }
        });
    }

    fn assert_refinement_order_identity(wkt: &str, option: &str, value: f64) {
        use pyo3::prelude::*;
        use pyo3::types::PyDict;

        struct RestoreOrder(InsertionOrder);
        impl Drop for RestoreOrder {
            fn drop(&mut self) {
                INSERTION_ORDER.with(|order| order.set(self.0));
            }
        }
        crate::test_support::initialize_python();
        let _restore = RestoreOrder(INSERTION_ORDER.with(std::cell::Cell::get));
        Python::attach(|py| {
            let module = PyModule::new(py, "gometry._lib").unwrap();
            crate::lib(&module).unwrap();
            let polygon = module.getattr("from_wkt").unwrap().call1((wkt,)).unwrap();
            let options = PyDict::new(py);
            options.set_item("method", "constrained").unwrap();
            options.set_item(option, value).unwrap();
            let mut expected = None;
            for order in [
                InsertionOrder::Original,
                InsertionOrder::Brio,
                InsertionOrder::Reversed,
            ] {
                INSERTION_ORDER.with(|cell| cell.set(order));
                let output = polygon
                    .call_method("triangulate", (), Some(&options))
                    .unwrap()
                    .call_method0("to_wkt")
                    .unwrap()
                    .extract::<Vec<String>>()
                    .unwrap();
                assert!(!output.is_empty(), "{wkt}, {option}={value}");
                if let Some(expected) = &expected {
                    assert_eq!(&output, expected, "{wkt}, {option}={value}, {order:?}");
                } else {
                    expected = Some(output);
                }
            }
        });
    }

    macro_rules! refinement_order_case {
        ($name:ident, $wkt:literal, $option:literal, $value:expr) => {
            #[test]
            fn $name() {
                assert_refinement_order_identity($wkt, $option, $value);
            }
        };
    }

    refinement_order_case!(
        area_1024_order_identity,
        "POLYGON((0 0,2 0,2 2,0 2,0 0))",
        "max_area",
        4.0 / 1024.0
    );
    refinement_order_case!(
        area_2048_order_identity,
        "POLYGON((0 0,2 0,2 2,0 2,0 0))",
        "max_area",
        4.0 / 2048.0
    );
    refinement_order_case!(
        area_4096_order_identity,
        "POLYGON((0 0,2 0,2 2,0 2,0 0))",
        "max_area",
        4.0 / 4096.0
    );
    refinement_order_case!(
        area_8192_order_identity,
        "POLYGON((0 0,2 0,2 2,0 2,0 0))",
        "max_area",
        4.0 / 8192.0
    );
    refinement_order_case!(
        angle_1024_order_identity,
        "POLYGON((0 0,256 0,256 1,0 1,0 0))",
        "min_angle",
        20.0
    );
    refinement_order_case!(
        angle_2048_order_identity,
        "POLYGON((0 0,512 0,512 1,0 1,0 0))",
        "min_angle",
        20.0
    );
    refinement_order_case!(
        angle_4096_order_identity,
        "POLYGON((0 0,1024 0,1024 1,0 1,0 0))",
        "min_angle",
        20.0
    );
    refinement_order_case!(
        angle_8192_order_identity,
        "POLYGON((0 0,2048 0,2048 1,0 1,0 0))",
        "min_angle",
        20.0
    );
    refinement_order_case!(
        angle_16384_order_identity,
        "POLYGON((0 0,4096 0,4096 1,0 1,0 0))",
        "min_angle",
        20.0
    );

    #[test]
    fn outside_insertions_replace_only_the_visible_hull_chain() {
        let points: Vec<_> = [
            (0.0, 0.0),
            (2.0, 0.0),
            (2.0, 2.0),
            (0.0, 2.0),
            (-2.0, 1.0),
            (1.0, -3.0),
            (5.0, 2.0),
            (2.0, 6.0),
        ]
        .map(|(x, y)| Point::new_raw_xy(x, y))
        .into();
        let xy = |site| site_xy(&points, site);
        let mut mesh = Mesh::try_delaunay(&xy, 4).unwrap().unwrap();
        let mut constrained = vec![false; mesh.edge_count()];
        for site in 4..points.len() {
            let visible = mesh.outside_edge(&xy, xy(site)).unwrap().unwrap();
            let insertion = mesh
                .insert_outside(&xy, site, visible, &mut constrained)
                .unwrap();
            mesh.try_legalize_constrained_from(&xy, &mut constrained, insertion.legalization)
                .unwrap();
            let expected = Mesh::try_delaunay(&xy, site.wrapping_add(1))
                .unwrap()
                .unwrap();
            let face_set = |mesh: &Mesh| {
                let mut triangles: Vec<_> = mesh
                    .face_ids()
                    .filter(|&face| mesh.alive(face))
                    .map(|face| {
                        let mut corners = mesh.triangle(face);
                        corners.sort_unstable();
                        corners
                    })
                    .collect();
                triangles.sort_unstable();
                triangles
            };
            assert_eq!(face_set(&mesh), face_set(&expected));
            for (&start, &end) in mesh.hull.iter().zip(mesh.hull.iter().cycle().skip(1)) {
                assert_eq!(mesh.hull.previous(end), start);
                let edge = mesh.directed_slot(start, end).unwrap();
                assert_eq!(mesh.twin(edge), EMPTY);
            }
        }
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
                assert_ne!(
                    canonical_incircle(xy(a), xy(b), xy(c), xy(opposite)).unwrap(),
                    ExactSign::Positive,
                    "opposite vertex {opposite} violates the canonical lift of {face}"
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
    fn late_center_insertion_walks_large_cavities_in_boundary_order() {
        for n in [32, 128, 512] {
            let mut pts: Vec<_> = (0..n)
                .map(|i| {
                    let angle = std::f64::consts::TAU * i as f64 / n as f64;
                    Point::new_raw_xy(angle.cos(), angle.sin())
                })
                .collect();
            pts.push(Point::new_raw_xy(0.0, 0.0));
            let xy = |site: usize| pts[site].xy();
            let hull: Vec<_> = (0..n).collect();
            let mut mesh = Mesh::seed(&hull, pts.len());
            mesh.try_legalize(&xy).unwrap();
            let located = mesh.locate(&xy, FaceId::from_index(0), xy(n)).unwrap();
            let (_, changed) = mesh
                .try_insert_with(&xy, n, located, None, |mut cavity, slots| {
                    assert_eq!(cavity.len(), n - 2);
                    assert_eq!(slots.len(), n);
                    cavity.extend_from_slice(slots);
                    cavity
                })
                .unwrap();
            assert_eq!(changed.len(), 2 * n - 2);
            let steps = PERIMETER_STEPS.with(std::cell::Cell::get);
            assert_eq!(steps, 3 * (n - 2));
            eprintln!("late center: boundary={n}, cavity={}, steps={steps}", n - 2);
            let result = mesh.finish(&xy).unwrap();
            assert_halfedges_consistent(&result);
            assert_delaunay(&result, &pts);
        }
    }

    #[test]
    fn insertion_reports_cavity_faces_before_replacement_slots() {
        let pts = points(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (1.0, 0.5)]);
        let xy = |site: usize| pts[site].xy();
        let mut mesh = Mesh::seed(&[0, 1, 2, 3], pts.len());
        mesh.try_legalize(&xy).unwrap();
        let located = mesh.locate(&xy, FaceId::from_index(0), xy(4)).unwrap();
        let (last, changed) = mesh
            .try_insert_with(&xy, 4, located, None, |mut cavity, slots| {
                cavity.extend_from_slice(slots);
                cavity
            })
            .unwrap();
        assert_eq!(
            changed.iter().map(|face| face.index()).collect::<Vec<_>>(),
            [1, 0, 0, 1, 2, 3]
        );
        assert_eq!(last.index(), 3);
        let repeated = mesh.locate(&xy, last, xy(4)).unwrap();
        let (_, changed) = mesh
            .try_insert_with(&xy, 4, repeated, None, |mut cavity, slots| {
                cavity.extend_from_slice(slots);
                cavity
            })
            .unwrap();
        assert!(changed.is_empty());
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
        let located = ordinary.locate(&xy, FaceId::from_index(0), xy(4)).unwrap();
        assert!(matches!(located, Located::Inside(_)));
        ordinary
            .try_insert_with(&xy, 4, located, None, |_, _| ())
            .unwrap();
        wrapped
            .try_insert_with(&xy, 4, located, None, |_, _| ())
            .unwrap();
        let ordinary = ordinary.finish(&xy).unwrap();
        let wrapped = wrapped.finish(&xy).unwrap();
        assert_delaunay(&ordinary, &pts);
        assert_delaunay(&wrapped, &pts);
        assert_eq!(
            triangle_set(&ordinary.triangles),
            triangle_set(&wrapped.triangles)
        );
        assert_eq!(ordinary.cocircular, wrapped.cocircular);
    }
}
