use std::sync::Arc;

use super::refine::fan_size;
use super::{
    AffineRectClass, Candidate, CandidateCertificate, CellClass, CellId, CoverError, CoverStats,
    GeographicCoverageSource, PointMembership, Productivity, Shape, WorkingShapeRelation,
};
use crate::geometry::{Bounds, Coordinates as _, Point, PointBatchTester, same_point};
use crate::grid::affine_source::RectangularCandidate;
use crate::grid::coverer::{NativeRectClassifier, RectClass};
use crate::grid::s2::cell::Cell;
use crate::grid::spherical_arc::{
    Bound, CertifiedDegreeWindows, CertifiedLongitudeDegrees, DegreeWindowResult,
};

/// The prepared classification context: built once per covering, reused by
/// every candidate — the hierarchical [`PointBatchTester`] for vertex/center
/// probes and the native rectangle classifier for rect-polygon certificates.
/// Rect Outside is the only negative certificate; the planar chord proxy is
/// never used to prune (see module soundness model).
pub(super) struct CoverContext<'a> {
    pub(super) tester: Arc<PointBatchTester>,
    pub(super) rects: NativeRectClassifier,
    /// For a point carrier (including collapsed linework), every leaf cell
    /// containing a source point under closed spherical-cell semantics.  This
    /// is the exact point fast path; unlike the historical version it uses the
    /// pole/seam-normalized owner representation rather than rounded trig XYZ.
    pub(super) point_leaves: Option<Vec<CellId>>,
    /// The unsplit periodic source owns candidate negatives. Its richer
    /// corroboration is needed only on seam/pole/periodic or otherwise
    /// uncertain inputs.
    pub(super) affine: &'a GeographicCoverageSource,
    pub(super) corroborate: bool,
}

impl<'a> CoverContext<'a> {
    pub(super) fn prepare(
        source: &Shape,
        affine: &'a GeographicCoverageSource,
        relation: WorkingShapeRelation,
    ) -> Result<Self, crate::error::Error> {
        let tester = Arc::new(PointBatchTester::new(source)?);
        let shared_area_tester = matches!(source, Shape::Polygon(_) | Shape::MultiPolygon(_))
            .then(|| Arc::clone(&tester));
        Ok(Self {
            tester,
            rects: NativeRectClassifier::new_with_area_tester(source, shared_area_tester)?,
            point_leaves: closed_point_leaves(source),
            affine,
            corroborate: affine.s2_needs_corroboration(relation),
        })
    }
}

/// Leaf owners of a point-only source under S2's closed-cell relation.
///
/// The source factory has already performed the shared geographic pole
/// normalization. Empty linework represents the same point set as
/// `Point`, so it deliberately enters this exact owner path too; a nonzero
/// segment stays on the general certificate traversal.
fn closed_point_leaves(source: &Shape) -> Option<Vec<CellId>> {
    if matches!(
        source.for_each_first_xy_segment_chain(|chain| {
            if chain
                .segment_pairs()
                .any(|[start, end]| !same_point(start, end))
            {
                std::ops::ControlFlow::Break(())
            } else {
                std::ops::ControlFlow::Continue(())
            }
        }),
        std::ops::ControlFlow::Break(())
    ) {
        return None;
    }

    let mut leaves = Vec::new();
    source.for_each_first_xy_point_run(|point| {
        let xyz = super::super::projection::lonlat_to_closed_owner_point(point.x, point.y);
        let leaf = CellId::from_point(xyz);
        leaves.push(leaf);
        for neighbor in leaf.edge_neighbors() {
            if Cell::from_id(neighbor).contains_point(xyz) {
                leaves.push(neighbor);
                // A corner can meet a diagonal leaf as well.  Discover it
                // from an admitted edge neighbour; this is positive-only and
                // never invents an owner from a proxy-negative result.
                for diagonal in neighbor.edge_neighbors() {
                    if diagonal != leaf && Cell::from_id(diagonal).contains_point(xyz) {
                        leaves.push(diagonal);
                    }
                }
            }
        }
    });
    (!leaves.is_empty()).then(|| {
        leaves.sort_unstable();
        leaves.dedup();
        leaves
    })
}

/// Classify every descendant of `id` at exactly `target` level into `out`.
/// The finite fan owns its probe set: adjacent fans may repeat a point, but no
/// point result survives this call.
pub(super) fn stage_children_at(
    ctx: &CoverContext<'_>,
    id: CellId,
    target: u8,
    reuse: Option<Productivity>,
    out: &mut Vec<Candidate>,
    stats: &mut CoverStats,
) -> Result<(), CoverError> {
    match reuse {
        Some(Productivity::Children(batch)) => {
            let fan_capacity = fan_size(id.level(), target);
            let end = id.child_end_at(target);
            let mut expected = id.child_begin_at(target);
            debug_assert_eq!(batch.candidates.len(), fan_capacity);
            debug_assert!(batch.candidates.iter().all(|candidate| {
                let matches = candidate.certificate.id == expected;
                expected = expected.next();
                matches
            }));
            debug_assert_eq!(expected, end);
            out.clear();
            if out.capacity() < fan_capacity {
                out.reserve(fan_capacity);
            }
            for candidate in batch.candidates {
                let level = candidate.certificate.id.level();
                stats.carried_certificate_hit(level);
                if candidate.productivity.is_some() {
                    stats.productive_path_reuse(level);
                }
                out.push(candidate);
            }
            stats.observe_batch(out);
            return Ok(());
        },
        Some(Productivity::Terminal) => {
            debug_assert!(false, "terminal productivity cannot refine");
        },
        None => {},
    }
    let end = id.child_end_at(target);
    let mut cursor = id.child_begin_at(target);
    let ids = std::iter::from_fn(|| {
        (cursor != end).then(|| {
            let current = cursor;
            cursor = cursor.next();
            current
        })
    });
    classify_ids(ctx, ids, target, fan_size(id.level(), target), out, stats)?;
    stats.observe_batch(out);
    Ok(())
}

pub(super) fn classify_one(
    ctx: &CoverContext<'_>,
    id: CellId,
    stats: &mut CoverStats,
) -> Result<Candidate, CoverError> {
    let mut out = Vec::with_capacity(1);
    classify_ids(ctx, std::iter::once(id), id.level(), 1, &mut out, stats)?;
    Ok(out.pop().expect("one candidate was classified"))
}

/// The native test helper retains its old scalar shape while the traversal
/// consumes the full certificate.
#[cfg(test)]
pub(super) fn classify(ctx: &CoverContext<'_>, id: CellId) -> CellClass {
    let mut stats = CoverStats::default();
    classify_one(ctx, id, &mut stats)
        .expect("test candidate classification")
        .certificate
        .class
}

struct LocalProbe {
    point: Point,
    covered: Option<bool>,
}

struct PendingCandidate {
    id: CellId,
    cell: Cell,
    rectangular: Option<RectangularCandidate>,
    affine: Option<AffineRectClass>,
    probe_indices: [usize; 5],
    membership_known: bool,
}

fn local_probe_index(
    probes: &mut Vec<LocalProbe>,
    point: Point,
    known: Option<bool>,
    stats: &mut CoverStats,
) -> usize {
    if let Some(index) = probes.iter().position(|probe| {
        probe.point.x.to_bits() == point.x.to_bits() && probe.point.y.to_bits() == point.y.to_bits()
    }) {
        stats.local_duplicate_hit();
        if let Some(known) = known {
            let existing = probes
                .get_mut(index)
                .expect("probe position came from the probe vector");
            if let Some(existing_covered) = existing.covered {
                debug_assert_eq!(
                    existing_covered, known,
                    "local probe {:?} has conflicting carried membership",
                    existing.point
                );
            } else {
                existing.covered = Some(known);
            }
        }
        return index;
    }
    let index = probes.len();
    probes.push(LocalProbe {
        point,
        covered: known,
    });
    index
}

/// Classify a fresh finite fan after deduplicating its local point probes.
fn classify_ids(
    ctx: &CoverContext<'_>,
    ids: impl IntoIterator<Item = CellId>,
    level: u8,
    fan_capacity: usize,
    out: &mut Vec<Candidate>,
    stats: &mut CoverStats,
) -> Result<(), CoverError> {
    out.clear();
    if out.capacity() < fan_capacity {
        out.reserve(fan_capacity);
    }
    let mut probes = Vec::with_capacity(fan_capacity.saturating_mul(5));
    let mut pending = Vec::with_capacity(fan_capacity);

    for id in ids {
        stats.candidate_preparation();
        let cell = Cell::from_id(id);
        debug_assert_eq!(id.level(), level);
        let vertices = cell.vertices_lonlat();
        let rect = cell.rect_bound(&vertices);
        let [southwest, southeast, northeast, northwest] = vertices;
        let points = [
            southwest,
            southeast,
            northeast,
            northwest,
            cell.center_lonlat(),
        ];
        let rectangular = ctx
            .point_leaves
            .is_none()
            .then(|| ctx.affine.admit_rectangular_candidate(rect_bounds(rect)));
        let affine = rectangular
            .as_ref()
            .map(|candidate| ctx.affine.classify_rect(candidate))
            .transpose()?;
        let mut probe_indices = [0_usize; 5];
        let probe_count = if affine.is_none_or(|class| class != AffineRectClass::Outside) {
            if ctx.point_leaves.is_some() { 1 } else { 5 }
        } else {
            0
        };
        if probe_count == 1 && ctx.point_leaves.is_some() {
            let center = points.last().copied().expect("one cell center probe");
            let center_probe = probe_indices.last_mut().expect("center probe slot exists");
            *center_probe = local_probe_index(&mut probes, center, None, stats);
        } else if probe_count == 5 {
            for (probe_index, point) in probe_indices.iter_mut().zip(points) {
                *probe_index = local_probe_index(&mut probes, point, None, stats);
            }
        }
        pending.push(PendingCandidate {
            id,
            cell,
            rectangular,
            affine,
            probe_indices,
            membership_known: probe_count != 0,
        });
    }

    for probe in &mut probes {
        if probe.covered.is_none() {
            probe.covered = Some(ctx.tester.covers_point(probe.point));
            stats.unique_local_point_probe();
        }
    }

    for pending in pending {
        let mut membership = PointMembership {
            vertices: [false; 4],
            center: false,
        };
        let [southwest, southeast, northeast, northwest, center] = pending.probe_indices;
        for (vertex, probe_index) in membership
            .vertices
            .iter_mut()
            .zip([southwest, southeast, northeast, northwest])
        {
            if let Some(covered) = probes.get(probe_index).and_then(|probe| probe.covered) {
                *vertex = covered;
            }
        }
        if let Some(covered) = probes.get(center).and_then(|probe| probe.covered) {
            membership.center = covered;
        }
        let membership = pending.membership_known.then_some(membership);
        let class = classify_candidate(
            ctx,
            pending.id,
            pending.cell,
            pending.rectangular.as_ref(),
            pending.affine,
            membership,
            stats,
        )?;
        out.push(Candidate::new(CandidateCertificate {
            id: pending.id,
            class,
            membership,
        }));
    }
    Ok(())
}

/// Classify one candidate after the fan has paid its local membership probes.
fn classify_candidate(
    ctx: &CoverContext<'_>,
    id: CellId,
    cell: Cell,
    rectangular: Option<&RectangularCandidate>,
    affine: Option<AffineRectClass>,
    membership: Option<PointMembership>,
    stats: &mut CoverStats,
) -> crate::error::Result<CellClass> {
    stats.raw_classify(id.level());
    if let Some(leaves) = &ctx.point_leaves {
        let from = leaves.partition_point(|&leaf| leaf < id.range_min());
        return Ok(
            if leaves.get(from).is_some_and(|&leaf| leaf <= id.range_max()) {
                CellClass::Boundary
            } else {
                CellClass::Outside
            },
        );
    }
    // The unsplit periodic authority owns the only candidate negative. A
    // charted source window may refine a retained candidate below, but an
    // internal seam cut can never prune a subtree.
    let affine = affine.expect("non-point candidate carries its affine class");
    if affine == AffineRectClass::Outside {
        return Ok(CellClass::Outside);
    }
    let rectangular = rectangular.expect("non-point candidate carries its rectangle");
    let membership = membership.expect("non-outside candidate carries point facts");
    let proposed = {
        let covered = membership
            .vertices
            .iter()
            .filter(|&&covered| covered)
            .count();
        let center_hit = covered < 4 && membership.center;
        // Seam-crossing: planar Outside is unsound → fail-open Boundary.
        // Polar full-lng (pole closure): vertices away from the pole cannot
        // establish an Outside result for a cell whose closed bound deliberately
        // contains every longitude at the pole. Keep the rect certificate; any
        // remaining ambiguity fails open to Boundary.
        // Still certify Interior when every positive-width bound window is
        // interior (full-cap / seam-crossing within).
        let wrapped = matches!(
            rectangular.windows(),
            DegreeWindowResult::Windows(CertifiedDegreeWindows {
                longitude: CertifiedLongitudeDegrees::Full | CertifiedLongitudeDegrees::Two(_),
                ..
            })
        );
        if wrapped {
            if covered == 4 && wrapped_rect_is_interior(ctx, rectangular)? {
                CellClass::Interior
            } else {
                // Seam-crossing rects: planar Outside is unsound → fail-open
                // Boundary. (True-cell planar refine is also unsound across
                // ±180.)
                CellClass::Boundary
            }
        } else {
            // Rect certificates only: rect ⊇ true cell, so covers(rect) ⇒
            // Interior and !intersects(rect) ⇒ Outside (`rect_bound` may
            // exclude). A Boundary or Interior rect without vertex/center
            // hits fails open to Boundary — `boundary_shape` is a planar
            // chord and may only witness positively, so a negative proxy
            // result must not prune whole subtrees.
            let rect_class = ctx.rects.classify_bounds(rectangular.bounds())?;
            if covered == 4 && rect_class == RectClass::Interior {
                CellClass::Interior
            } else if covered > 0 || center_hit {
                CellClass::Boundary
            } else {
                match rect_class {
                    RectClass::Outside => CellClass::Outside,
                    RectClass::Interior | RectClass::Boundary => CellClass::Boundary,
                }
            }
        }
    };

    // An exact affine certificate only corroborates a planar proposal on
    // hazards.  Boundary stays Boundary: no terminal spherical refiner is
    // smuggled onto S2's ordinary candidate path.
    if !ctx.corroborate {
        return Ok(proposed);
    }
    if proposed == CellClass::Boundary {
        return Ok(CellClass::Boundary);
    }
    Ok(merge_certified_rect(
        proposed,
        affine,
        ctx.affine.s2_raw_owner_descends_from(cell),
    ))
}

/// Intersect the working-shape proposal with the retained affine authority.
/// Neither source may establish a negative alone on a hazard input; a native
/// raw endpoint owner also vetoes a proposed Outside result.
const fn merge_certified_rect(
    proposed: CellClass,
    certified: AffineRectClass,
    raw_owner: bool,
) -> CellClass {
    match (proposed, certified) {
        (CellClass::Outside, AffineRectClass::Outside) if !raw_owner => CellClass::Outside,
        (CellClass::Interior, AffineRectClass::Interior) => CellClass::Interior,
        _ => CellClass::Boundary,
    }
}

/// Sound Interior certificate for seam-crossing / full-longitude cell bounds.
///
/// `rect` contains the true spherical cell. Split into non-wrapped lon
/// windows (full-lng is one world-wide window); if every positive-width
/// window is `Interior` under the native rect classifier, the cell is
/// interior. Never used to certify Outside (that stays fail-open Boundary).
fn wrapped_rect_is_interior(
    ctx: &CoverContext<'_>,
    candidate: &RectangularCandidate,
) -> crate::error::Result<bool> {
    let DegreeWindowResult::Windows(CertifiedDegreeWindows {
        latitude,
        longitude,
    }) = candidate.windows()
    else {
        return Ok(false);
    };
    match longitude {
        CertifiedLongitudeDegrees::Full => {
            Ok(ctx.rects.classify_bounds(candidate.bounds())? == RectClass::Interior)
        },
        CertifiedLongitudeDegrees::Two([west, east]) => {
            if !window_interior(ctx, west, latitude)? {
                return Ok(false);
            }
            window_interior(ctx, east, latitude)
        },
        CertifiedLongitudeDegrees::One(_) => Ok(false),
    }
}

fn window_interior(
    ctx: &CoverContext<'_>,
    longitude: Bound,
    latitude: Bound,
) -> crate::error::Result<bool> {
    // Degenerate zero-width lon strip (seam edge only) cannot host area;
    // treat as vacuously interior so a single-side polar spelling still
    // certifies when its positive-width sibling does.
    if longitude.hi <= longitude.lo {
        return Ok(true);
    }
    Ok(ctx.rects.classify_bounds(Bounds::new_raw(
        longitude.lo,
        latitude.lo,
        longitude.hi,
        latitude.hi,
    ))? == RectClass::Interior)
}

pub(super) const fn rect_bounds(rect: super::super::cell::LatLngRect) -> Bounds {
    Bounds::new_raw(rect.lng_lo, rect.lat_lo, rect.lng_hi, rect.lat_hi)
}
