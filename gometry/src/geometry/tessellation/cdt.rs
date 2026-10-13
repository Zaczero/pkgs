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

use rstar::{AABB, RTree};

use super::super::RingOwner;
use super::exact_delaunay::{FaceId, Located, Mesh};
use super::{CdtRefinement, exact_delaunay};
use crate::collections::HashMap;
use crate::geographic::ChartPointSite;
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::publication::DirectedChartRestrictionUse;
use crate::geometry::tessellation::exact;
use crate::geometry::{Point, XY};

#[path = "cdt/coordinates.rs"]
mod coordinates;
#[path = "cdt/mesh.rs"]
mod mesh;
#[path = "cdt/plan.rs"]
mod plan;
#[path = "cdt/refinement.rs"]
mod refinement;

#[derive(Clone, Copy, Debug)]
pub(super) enum CdtQuery {
    Site(usize),
    Source(usize),
    Point(XY),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CdtPointLocation {
    Outside,
    Boundary,
    Inside,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::geometry::tessellation) struct CdtKey([exact::ExactDyadic; 2]);

#[derive(Clone, Debug)]
struct CdtPosition {
    exact: [exact::ExactDyadic; 2],
    proposal: XY,
    retained: Option<ChartPointSite>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CdtCoordinateModel {
    Raw,
    Retained,
}

#[derive(Debug)]
pub(super) struct CdtCoordinates {
    sites: Vec<CdtPosition>,
    source_occurrences: Vec<CdtPosition>,
    model: CdtCoordinateModel,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct RefinementWorkCounters {
    pub(super) inside_label_passes: usize,
    pub(super) bad_face_scans: usize,
    pub(super) constraint_reconstruction_passes: usize,
    pub(super) quality_checks: usize,
    pub(super) source_piece_visits: usize,
}

#[cfg(test)]
thread_local! {
    static REFINEMENT_WORK: std::cell::Cell<RefinementWorkCounters> =
        const { std::cell::Cell::new(RefinementWorkCounters {
            inside_label_passes: 0,
            bad_face_scans: 0,
            constraint_reconstruction_passes: 0,
            quality_checks: 0,
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
pub(crate) struct SourceConstraint {
    pub(crate) id: usize,
    /// Original input segment ordinal, including skipped zero-length edges.
    pub(crate) source_root: usize,
    pub(crate) start: Point,
    pub(crate) end: Point,
    pub(crate) start_site: usize,
    pub(crate) end_site: usize,
    pub(crate) start_occurrence: usize,
    pub(crate) end_occurrence: usize,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct SourceRingStart {
    first_source: usize,
    owner: RingOwner,
}

impl SourceRingStart {
    pub(super) const fn new(first_source: usize, owner: RingOwner) -> Self {
        Self {
            first_source,
            owner,
        }
    }
}

pub(super) fn source_ring_owner(
    source: usize,
    source_rings: &[SourceRingStart],
) -> Option<RingOwner> {
    let index = source_rings
        .partition_point(|ring| ring.first_source <= source)
        .checked_sub(1)?;
    source_rings.get(index).map(|ring| ring.owner)
}

#[derive(Clone, Debug)]
pub(crate) struct SourcePiece {
    pub(crate) source: usize,
    pub(crate) start_site: usize,
    pub(crate) end_site: usize,
    pub(crate) lo: exact::ExactRational,
    pub(crate) hi: exact::ExactRational,
}

#[derive(Clone, Debug)]
pub(crate) struct RestrictionConstraint {
    pub(crate) id: usize,
    pub(crate) owner: RingOwner,
    pub(crate) start_site: usize,
    pub(crate) end_site: usize,
    pub(crate) use_: DirectedChartRestrictionUse,
}

#[derive(Clone, Debug)]
pub(crate) struct RestrictionPiece {
    pub(crate) restriction: usize,
    pub(crate) owner: RingOwner,
    pub(crate) start_site: usize,
    pub(crate) end_site: usize,
    pub(crate) start: exact::ExactRational,
    pub(crate) end: exact::ExactRational,
    pub(crate) use_: DirectedChartRestrictionUse,
}

#[derive(Clone, Debug)]
struct ConstraintPiece {
    constraint: usize,
    start_site: usize,
    end_site: usize,
    lo: exact::ExactRational,
    hi: exact::ExactRational,
}

/// A rounded boundary point whose root parameter and half-ULP error bound
/// were checked before the point entered the mesh.
#[derive(Clone, Debug)]
struct CertifiedBoundarySplit {
    constraint: usize,
    parameter: exact::ExactRational,
    point: XY,
    half_ulp: [exact::ExactDyadic; 2],
}

#[derive(Debug)]
pub(crate) enum NodeProvenance {
    /// A vertex in the source point slice supplied to the mesh builder.
    SourceSite,
    /// A producer-generated cut or pole vertex with no source record.
    ChartRestrictionSite,
    ConstraintSplit {
        certificate: usize,
    },
    InteriorRefinement {
        parent_face: usize,
        parent_sites: [usize; 3],
        construction: RefinementConstruction,
    },
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum RefinementConstruction {
    Circumcenter,
    EdgeMidpoint([usize; 2]),
    LongestEdgeMidpoint([usize; 2]),
}

#[derive(Clone, Debug)]
pub(crate) struct CdtSourceUse {
    pub(crate) source: usize,
    pub(crate) owner: RingOwner,
    pub(crate) start: exact::ExactRational,
    pub(crate) end: exact::ExactRational,
    pub(crate) forward: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum CdtSideBoundaryUse {
    Source(usize),
    /// Index into the plan's directed chart-restriction piece table.
    Restriction(usize),
}

#[derive(Clone, Debug)]
pub(crate) struct CdtSideUse {
    pub(crate) halfedge: usize,
    pub(crate) start_site: usize,
    pub(crate) end_site: usize,
    pub(crate) twin: Option<usize>,
    pub(crate) twin_selected: bool,
    pub(crate) boundary: Option<CdtSideBoundaryUse>,
}

#[derive(Clone, Debug)]
pub(crate) struct CdtFaceUse {
    pub(crate) live_face: usize,
    pub(crate) selected_sites: [usize; 3],
    pub(crate) output_to_selected: [u8; 3],
    pub(crate) sides: Vec<CdtSideUse>,
}

/// One constrained mesh and its checked publication inputs.
///
/// Selected faces, authored source intervals, chart-restriction uses, and
/// generated-node provenance stay together so publishers cannot reconstruct a
/// different mesh result from exported points.
#[derive(Debug)]
pub(crate) struct ConstrainedMeshPlan {
    pub(crate) vertices: Vec<Point>,
    pub(crate) faces: Vec<[usize; 3]>,
    pub(crate) face_uses: Vec<CdtFaceUse>,
    coordinates: CdtCoordinates,
    face_slot_count: usize,
    pub(crate) provenance: Vec<NodeProvenance>,
    pub(crate) source_constraints: Vec<SourceConstraint>,
    pub(crate) source_uses: Vec<CdtSourceUse>,
    pub(crate) restrictions: Vec<RestrictionConstraint>,
    source_rings: Vec<SourceRingStart>,
    pub(crate) constraint_pieces: Vec<SourcePiece>,
    pub(crate) restriction_pieces: Vec<RestrictionPiece>,
    split_certificates: Vec<CertifiedBoundarySplit>,
    pub(crate) refinement: super::CdtRefinement,
    pub(crate) force_xy: bool,
}

struct PreparedSplit {
    candidate: XY,
    located: Located,
    constraint: Option<PreparedConstraintSplit>,
    construction: Option<RefinementConstruction>,
}

struct PreparedConstraintSplit {
    piece: ConstraintPiece,
    certificate: CertifiedBoundarySplit,
}

struct PieceNode {
    piece: Option<ConstraintPiece>,
    next: Option<usize>,
}

enum PieceCursor {
    Replace(usize),
    After(usize),
}

// Constraint IDs own the first slots. A split replaces its existing slot so
// predecessors remain valid when the edge-index key changes.
struct ConstraintPieces {
    edges: HashMap<(usize, usize), usize>,
    nodes: Vec<PieceNode>,
}

/// The mesh with its constraint flags, one per halfedge slot.
pub(super) struct ConstrainedMesh {
    pub(super) mesh: Mesh,
    pub(super) constrained: Vec<bool>,
    coordinates: CdtCoordinates,
    source_constraints: Vec<SourceConstraint>,
    restrictions: Vec<RestrictionConstraint>,
    source_rings: Vec<SourceRingStart>,
    constraint_pieces: ConstraintPieces,
    provenance: Vec<NodeProvenance>,
    split_certificates: Vec<CertifiedBoundarySplit>,
}

struct FaceLabels(Vec<Option<bool>>);

#[derive(Clone, Copy)]
struct BadTriangle {
    face: FaceId,
    triangle: [usize; 3],
    corners: [XY; 3],
    angle: f64,
    area: f64,
}

#[derive(Clone, PartialEq)]
struct ConstraintCircle {
    sites: (usize, usize),
    corners: [XY; 2],
    envelope: AABB<[f64; 2]>,
}

struct ConstraintIndex {
    tree: RTree<ConstraintCircle>,
}

const fn ordered(a: usize, b: usize) -> (usize, usize) {
    if a < b { (a, b) } else { (b, a) }
}

fn opposite_nonzero(a: ExactSign, b: ExactSign) -> bool {
    a != ExactSign::Zero && b != ExactSign::Zero && a != b
}

pub(super) const fn next_triangle_side(side: usize) -> usize {
    match side {
        0 => 1,
        1 => 2,
        _ => 0,
    }
}

#[cfg(test)]
#[path = "cdt/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "cdt/regressions.rs"]
mod regressions;
