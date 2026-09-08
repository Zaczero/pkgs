//! The direct relation engine for linework: the DE-9IM matrix of two line
//! operands from segment contacts alone, with no topology graph.
//!
//! The graph path nodes both operands, hashes every vertex into a node map and
//! pairs the two edge sets; on two smooth 800-vertex lines that is ~450 µs to
//! learn what the candidate sweep proves in a few microseconds with zero
//! candidate pairs. The matrix of two lines needs nothing the sweep does not
//! already produce:
//!
//! - a segment `Cross` is interior to both segments, hence `II` at a point;
//! - a `Touch` point is one of the four segment endpoints (bit-exactly — see
//!   `segment_cross_point_with_orientations`), so its class in each operand is
//!   whether that vertex is one of the operand's mod-2 boundary points;
//! - a `Collinear` span's interior is `II` at a curve and its ends are vertices,
//!   classified the same way;
//! - a segment no contact touched has its whole interior in the other
//!   operand's exterior, a boundary vertex no contact reached is in the other's
//!   exterior, and a segment every point of which lies in the other operand is
//!   one whose collinear spans cover it end to end — an exact question, since
//!   the span ends are vertices that order along the segment by coordinate.
//!
//! No intersection point is ever computed: contact *types* and vertex
//! identities carry the whole matrix, which is both exact by construction and
//! the reason this costs what the sweep costs.
//!
//! Evidence accumulates in a [`De9imInterval`] and a predicate stops the sweep
//! the moment it is decided. Admission is deliberately narrow — Cartesian
//! `LineString`/`MultiLineString` pairs whose every member has a real segment;
//! anything else (geographic frames, collections, collapsed members, which the
//! graph classifies as point support) keeps the graph path, which also stays
//! as this engine's differential oracle.

use std::ops::ControlFlow;

use super::interval::{De9imInterval, OperandShape, predicate_verdict};
use super::{De9im, Loc, line_has_nonzero_segment};
use crate::geometry::predicates::LineworkChains;
use crate::geometry::segments::{
    Contact, segment_contact_with_orientations, segment_envelopes_disjoint,
};
use crate::geometry::{
    CompiledPattern, Dimension, PointKey, PreparedPlan, Segment, Shape, XY, same_point,
};
use crate::predicates::{PredicateSpec, predicate_matrix_value};

/// The lines of one admitted operand.
fn admitted_lines(shape: &Shape) -> Option<Vec<&crate::geometry::CoordSeq>> {
    match shape {
        Shape::LineString(line) if line_has_nonzero_segment(line) => Some(vec![line]),
        Shape::MultiLineString(lines)
            if !lines.is_empty() && lines.iter().all(|line| line_has_nonzero_segment(line)) =>
        {
            Some(lines.iter().map(|line| &**line).collect())
        },
        _ => None,
    }
}

/// A linework operand staged for the engine: its chains (segments, monotone
/// runs, bounds) and its sorted mod-2 endpoint set. Built once per operand
/// and cached on a prepared plan, so a scalar swept against an array is
/// staged exactly once for the whole call.
pub(crate) struct StagedLinework {
    chains: LineworkChains,
    boundary: Vec<PointKey>,
}

/// Stage a line operand, or `None` when it is not admitted here: Cartesian
/// `LineString`/`MultiLineString` whose every member has a real segment.
pub(crate) fn stage_linework(shape: &Shape) -> Option<StagedLinework> {
    let lines = admitted_lines(shape)?;
    let mut chains = LineworkChains::default();
    for line in &lines {
        // Admission proved a real segment, so staging never declines.
        chains.push_line(line);
    }
    // OGC line boundaries are the mod-2 endpoints of the whole operand: an
    // end shared by an even number of parts is interior.
    let mut ends: Vec<PointKey> = Vec::with_capacity(2 * chains.line_count());
    for line in 0..chains.line_count() {
        if let Some((start, end)) = chains.line_ends(line) {
            ends.push(PointKey::new(start));
            ends.push(PointKey::new(end));
        }
    }
    ends.sort_unstable();
    let mut boundary = Vec::with_capacity(ends.len());
    let mut run = ends.iter().peekable();
    while let Some(&key) = run.next() {
        let mut multiplicity = 1_usize;
        while run.peek() == Some(&&key) {
            run.next();
            multiplicity += 1;
        }
        if multiplicity % 2 == 1 {
            boundary.push(key);
        }
    }
    Some(StagedLinework { chains, boundary })
}

impl StagedLinework {
    const fn shape(&self) -> OperandShape {
        OperandShape {
            interior: Dimension::Curve,
            boundary: if self.boundary.is_empty() {
                None
            } else {
                Some(Dimension::Point)
            },
        }
    }

    /// Where a point known to lie ON this operand sits: on its boundary if it
    /// is one of the mod-2 endpoints, in its interior otherwise.
    fn class_of(&self, point: XY) -> Loc {
        if self.boundary.binary_search(&PointKey::new(point)).is_ok() {
            Loc::Boundary
        } else {
            Loc::Interior
        }
    }

    fn boundary_slot(&self, point: XY) -> Option<usize> {
        self.boundary.binary_search(&PointKey::new(point)).ok()
    }
}

/// What a caller wants from an engine: the whole matrix, or one predicate
/// decided as early as the evidence allows. Only a predicate can be decided
/// before the sweep, so the pre-sweep probes run for predicates alone.
pub(crate) enum Ask<'a> {
    Matrix,
    Predicate(&'a mut dyn FnMut(&De9imInterval) -> Option<bool>),
}

impl Ask<'_> {
    fn decide(&mut self, interval: &De9imInterval) -> Option<bool> {
        match self {
            Self::Matrix => None,
            Self::Predicate(decide) => decide(interval),
        }
    }

    const fn probes(&self) -> bool {
        matches!(self, Self::Predicate(_))
    }
}

/// Per-segment record of what the sweep saw, for the exterior cells.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Touched {
    /// No contact at all: the whole segment lies in the other's exterior.
    None,
    /// Point contacts only: the interior still reaches the exterior.
    Points,
    /// At least one collinear span: coverage decides.
    Spans,
}

pub(crate) enum Outcome {
    /// The predicate was decided before the matrix was complete.
    Verdict(bool),
    Matrix(De9im),
}

/// Relate two admitted linework operands, stopping as soon as `decide` returns
/// a verdict. `None` when either operand is not admitted.
fn relate_linework(
    left: &PreparedPlan,
    right: &PreparedPlan,
    mut decide: impl FnMut(&De9imInterval) -> Option<bool>,
) -> crate::error::Result<Option<Outcome>> {
    let Some(a) = left.staged_linework()? else {
        return Ok(None);
    };
    let Some(b) = right.staged_linework()? else {
        return Ok(None);
    };
    let mut interval = De9imInterval::open(a.shape(), b.shape());
    if let Some(verdict) = decide(&interval) {
        return Ok(Some(Outcome::Verdict(verdict)));
    }

    let mut touched = [
        vec![Touched::None; a.chains.segments.len()],
        vec![Touched::None; b.chains.segments.len()],
    ];
    let mut a_boundary_hit = vec![false; a.boundary.len()];
    let mut b_boundary_hit = vec![false; b.boundary.len()];
    // Collinear spans per side, per segment ordinal, as the two vertices
    // bounding them.
    let mut spans: [Vec<(usize, XY, XY)>; 2] = [Vec::new(), Vec::new()];

    let verdict = sweep_contacts(&a.chains, &b.chains, |ia, ib, contact| {
        let mut vertex = |point: XY, interval: &mut De9imInterval| {
            let (la, lb) = (a.class_of(point), b.class_of(point));
            if let Some(slot) = a.boundary_slot(point) {
                a_boundary_hit[slot] = true;
            }
            if let Some(slot) = b.boundary_slot(point) {
                b_boundary_hit[slot] = true;
            }
            interval.witness(la, lb, Dimension::Point);
        };
        match contact {
            Contact::Disjoint => return Ok(None),
            Contact::Cross { .. } => {
                interval.witness(Loc::Interior, Loc::Interior, Dimension::Point);
                for (side, ordinal) in [(0, ia), (1, ib)] {
                    if touched[side][ordinal] == Touched::None {
                        touched[side][ordinal] = Touched::Points;
                    }
                }
            },
            Contact::Touch { point } => {
                vertex(point, &mut interval);
                for (side, ordinal) in [(0, ia), (1, ib)] {
                    if touched[side][ordinal] == Touched::None {
                        touched[side][ordinal] = Touched::Points;
                    }
                }
            },
            // Collinear segments that meet end to end share one point, not a
            // span: that is a vertex touch, and it must not raise II to a curve.
            Contact::Collinear { span } if same_point(span.start, span.end) => {
                vertex(span.start, &mut interval);
                for (side, ordinal) in [(0, ia), (1, ib)] {
                    if touched[side][ordinal] == Touched::None {
                        touched[side][ordinal] = Touched::Points;
                    }
                }
            },
            Contact::Collinear { span } => {
                interval.witness(Loc::Interior, Loc::Interior, Dimension::Curve);
                vertex(span.start, &mut interval);
                vertex(span.end, &mut interval);
                touched[0][ia] = Touched::Spans;
                touched[1][ib] = Touched::Spans;
                spans[0].push((ia, span.start, span.end));
                spans[1].push((ib, span.start, span.end));
            },
        }
        Ok(decide(&interval))
    })?;
    if let Some(verdict) = verdict {
        return Ok(Some(Outcome::Verdict(verdict)));
    }

    if let Some(verdict) = settle_after_sweep(
        &mut interval,
        [&a.chains, &b.chains],
        &touched,
        [&a_boundary_hit, &b_boundary_hit],
        spans,
        &mut decide,
    ) {
        return Ok(Some(Outcome::Verdict(verdict)));
    }
    Ok(Some(Outcome::Matrix(
        interval
            .matrix()
            .expect("every cell of a linework relation is settled"),
    )))
}

/// Sweep two staged pools against each other and hand every contact to
/// `on_contact` as `(a_ordinal, b_ordinal, contact)` in the pools' own index
/// spaces, stopping with its verdict as soon as it returns one.
fn sweep_contacts(
    a: &LineworkChains,
    b: &LineworkChains,
    mut on_contact: impl FnMut(usize, usize, Contact) -> crate::error::Result<Option<bool>>,
) -> crate::error::Result<Option<bool>> {
    let mut verdict = None;
    let mut error = None;
    let _ = a.for_each_pair_with(b, |ia, ib| {
        let (sa, sb): (Segment, Segment) = (a.segments[ia], b.segments[ib]);
        if segment_envelopes_disjoint(sa, sb) {
            return ControlFlow::Continue(());
        }
        let (contact, _) = segment_contact_with_orientations(sa, sb);
        match on_contact(ia, ib, contact) {
            Ok(Some(found)) => {
                verdict = Some(found);
                ControlFlow::Break(())
            },
            Ok(None) => ControlFlow::Continue(()),
            Err(found) => {
                error = Some(found);
                ControlFlow::Break(())
            },
        }
    })?;
    if let Some(error) = error {
        return Err(error);
    }
    Ok(verdict)
}

/// The phases after the contact sweep: the inner cells settle, boundary
/// points no contact reached witness the exterior columns, and each interior
/// reaches the other's exterior unless collinear spans cover every segment.
/// `decide` is consulted after each phase.
fn settle_after_sweep(
    interval: &mut De9imInterval,
    chains: [&LineworkChains; 2],
    touched: &[Vec<Touched>; 2],
    boundary_hit: [&[bool]; 2],
    mut spans: [Vec<(usize, XY, XY)>; 2],
    decide: &mut impl FnMut(&De9imInterval) -> Option<bool>,
) -> Option<bool> {
    // Every contact has been seen: the four inner cells cannot rise further.
    for row in [Loc::Interior, Loc::Boundary] {
        for col in [Loc::Interior, Loc::Boundary] {
            interval.settle(row, col);
        }
    }
    // Boundary points no contact reached lie in the other operand's exterior.
    if boundary_hit[0].iter().any(|hit| !hit) {
        interval.witness(Loc::Boundary, Loc::Exterior, Dimension::Point);
    }
    if boundary_hit[1].iter().any(|hit| !hit) {
        interval.witness(Loc::Exterior, Loc::Boundary, Dimension::Point);
    }
    interval.settle(Loc::Boundary, Loc::Exterior);
    interval.settle(Loc::Exterior, Loc::Boundary);
    if let Some(verdict) = decide(interval) {
        return Some(verdict);
    }

    // Interiors reaching the other's exterior: any segment not wholly covered
    // by collinear spans. Untouched and point-touched segments settle it at
    // once; only span-carrying segments need the coverage fold, and only when
    // every other segment of that operand was covered.
    for (side, row, col) in [
        (0, Loc::Interior, Loc::Exterior),
        (1, Loc::Exterior, Loc::Interior),
    ] {
        spans[side].sort_unstable_by_key(|&(ordinal, ..)| ordinal);
        let reaches_exterior =
            chains[side]
                .segments
                .iter()
                .enumerate()
                .any(|(ordinal, segment)| match touched[side][ordinal] {
                    Touched::None | Touched::Points => true,
                    Touched::Spans => !covered(*segment, &spans[side], ordinal),
                });
        if reaches_exterior {
            interval.witness(row, col, Dimension::Curve);
        }
        interval.settle(row, col);
        if let Some(verdict) = decide(interval) {
            return Some(verdict);
        }
    }

    None
}

/// Whether the collinear spans recorded for `ordinal` cover `segment` end to
/// end. Span ends are vertices on the segment's carrier line, so they order
/// exactly along it by the coordinate the segment actually moves in.
#[expect(
    clippy::float_cmp,
    reason = "a vertical segment is exactly one whose ends share an x; any margin would misorder a near-vertical one"
)]
fn covered(segment: Segment, spans: &[(usize, XY, XY)], ordinal: usize) -> bool {
    let along = |point: XY| -> f64 {
        if segment.start.x == segment.end.x {
            point.y
        } else {
            point.x
        }
    };
    let (lo, hi) = {
        let (s, e) = (along(segment.start), along(segment.end));
        (s.min(e), s.max(e))
    };
    let first = spans.partition_point(|&(other, ..)| other < ordinal);
    let mut intervals: Vec<(f64, f64)> = spans[first..]
        .iter()
        .take_while(|&&(other, ..)| other == ordinal)
        .map(|&(_, start, end)| {
            let (s, e) = (along(start), along(end));
            (s.min(e), s.max(e))
        })
        .collect();
    intervals.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
    let mut reach = lo;
    for (start, end) in intervals {
        if start > reach {
            return false;
        }
        if end > reach {
            reach = end;
        }
        if reach >= hi {
            return true;
        }
    }
    reach >= hi
}

/// The full matrix of two linework operands, or `None` when either is not
/// admitted here.
pub(crate) fn direct_linework_relate(
    left: &PreparedPlan,
    right: &PreparedPlan,
) -> crate::error::Result<Option<De9im>> {
    Ok(match relate_linework(left, right, |_| None)? {
        Some(Outcome::Matrix(matrix)) => Some(matrix),
        Some(Outcome::Verdict(_)) => unreachable!("no predicate was asked"),
        None => None,
    })
}

/// One named predicate over two linework operands, decided as early as the
/// evidence allows, or `None` when either operand is not admitted here.
pub(crate) fn direct_linework_predicate(
    spec: &PredicateSpec,
    left: &PreparedPlan,
    right: &PreparedPlan,
) -> crate::error::Result<Option<bool>> {
    Ok(relate_linework(left, right, |interval| {
        predicate_verdict(interval, spec.predicate, Dimension::Curve, Dimension::Curve)
    })?
    .map(|outcome| match outcome {
        Outcome::Verdict(verdict) => verdict,
        Outcome::Matrix(matrix) => predicate_matrix_value(spec, matrix),
    }))
}

/// One DE-9IM pattern over two linework operands, decided as early as the
/// contacts allow under three-valued evaluation, or `None` when the pair is
/// not admitted here.
pub(crate) fn direct_linework_pattern(
    pattern: CompiledPattern,
    left: &PreparedPlan,
    right: &PreparedPlan,
) -> crate::error::Result<Option<bool>> {
    Ok(
        relate_linework(left, right, |interval| interval.pattern(pattern))?.map(|outcome| {
            match outcome {
                Outcome::Verdict(verdict) => verdict,
                Outcome::Matrix(matrix) => matrix.matches(pattern),
            }
        }),
    )
}

// ---------------------------------------------------------------------------
// Areal operands
// ---------------------------------------------------------------------------

/// A polygon operand's rings staged as closed chains, with one vertex of each
/// for the placement phase. Shells and holes are not told apart: whichever
/// side of a ring is the owner's interior, the ring's two sides are one
/// interior and one exterior patch of the owner, and with no contact both
/// sit in the same region of the other operand. Cached on a prepared plan.
pub(crate) struct StagedRings {
    chains: LineworkChains,
    ring_vertices: Vec<XY>,
}

/// Stage an areal operand's rings, or `None` when it is not areal here (see
/// `collect_areal_polygons`); admission proper is decided per call.
pub(crate) fn stage_rings(shape: &Shape) -> Option<StagedRings> {
    let mut polygons = Vec::new();
    super::graph_matrix::collect_polygons(shape, &mut polygons)?;
    if polygons.is_empty() {
        return None;
    }
    let mut chains = LineworkChains::default();
    let mut ring_vertices = Vec::new();
    for polygon in &polygons {
        for ring in polygon.rings() {
            chains.push_line(ring.coords());
            ring_vertices.push(XY::new(ring.coords().xs()[0], ring.coords().ys()[0]));
        }
    }
    Some(StagedRings {
        chains,
        ring_vertices,
    })
}

impl StagedRings {
    const fn shape() -> OperandShape {
        OperandShape {
            interior: Dimension::Surface,
            boundary: Some(Dimension::Curve),
        }
    }
}

/// The polygons of one admitted areal operand. The crossing argument reads a
/// ring segment as a boundary of the operand's interior, which a part sharing
/// an edge with another part, or a hole sharing an edge with its shell, is
/// not — and the arrangement fold deliberately supports such adjacent parts.
/// Those can only arise in a multi-part or holed operand, so exactly those
/// are admitted only once validated (cached on a prepared operand, the way
/// the polygon/line fold already gates); a plain polygon costs nothing here.
fn polygons_of(plan: &PreparedPlan) -> Option<Vec<&crate::geometry::Polygon>> {
    let mut polygons = Vec::new();
    super::graph_matrix::collect_polygons(plan.shape(), &mut polygons)?;
    if polygons.is_empty() {
        return None;
    }
    Some(polygons)
}

/// Whether the contact sweep and ring placement may reason about this
/// operand: every ring has area (staging elides zero-length segments, so
/// repeated vertices need no gate), and a multi-part or holed operand is
/// valid, so its rings form a laminar family. The vertex probes run before
/// this gate: a point-in-polygon reading needs neither.
fn sweep_admits(
    plan: &PreparedPlan,
    polygons: &[&crate::geometry::Polygon],
) -> crate::error::Result<bool> {
    if !plan.areal_rings_have_area()? {
        return Ok(false);
    }
    let adjacency_possible =
        polygons.len() > 1 || polygons.iter().any(|polygon| !polygon.holes.is_empty());
    Ok(!adjacency_possible || plan.validate_cached()?.is_none())
}

/// The first shell vertex of a polygon, the probe point its owner offers.
fn first_vertex(polygon: &crate::geometry::Polygon) -> XY {
    let shell = polygon.shell.coords();
    XY::new(shell.xs()[0], shell.ys()[0])
}

/// Where a point lies with respect to the union of `polygons`: on one of
/// their rings, inside (within a shell and outside that shell's holes), or
/// outside. One classification per ring.
fn locate_in_polygons(polygons: &[&crate::geometry::Polygon], point: XY) -> Loc {
    use crate::geometry::{RingClass, ring_classify_point};
    let point = crate::geometry::Point::new_raw_xy(point.x, point.y);
    let mut interior = false;
    for polygon in polygons {
        let mut inside = match ring_classify_point(polygon.shell.coords(), point) {
            RingClass::Boundary => return Loc::Boundary,
            RingClass::Interior => true,
            RingClass::Exterior => false,
        };
        for hole in polygon.holes.iter() {
            match ring_classify_point(hole.coords(), point) {
                RingClass::Boundary => return Loc::Boundary,
                RingClass::Interior => inside = false,
                RingClass::Exterior => {},
            }
        }
        interior |= inside;
    }
    if interior {
        Loc::Interior
    } else {
        Loc::Exterior
    }
}

/// [`locate_in_polygons`] for a point the contact sweep has already shown to
/// be off every ring; landing on one after all means the sweep and the
/// locator disagree, and the pair goes back to the arrangement fold.
fn locate_off_boundary(polygons: &[&crate::geometry::Polygon], point: XY) -> Option<Loc> {
    match locate_in_polygons(polygons, point) {
        Loc::Boundary => None,
        placement => Some(placement),
    }
}

/// Relate two areal operands from boundary contacts and ring placement.
///
/// A proper crossing of the two boundaries decides the whole matrix: the four
/// local quadrants around it are (in A, in B), (in A, out B), (out A, in B)
/// and (out A, out B), each boundary runs through the other's interior and
/// exterior, and the two boundaries meet at the point — `212101212`, the
/// matrix every named predicate is decided by. A touch or shared span only
/// certifies the boundaries meet (`BB`), which decides `intersects` and
/// `disjoint` and nothing finer, so once the sweep completes with any such
/// contact and the predicate still open, `None` sends the pair to the
/// arrangement fold, which classifies sides. With NO contact at all the
/// rings form a laminar family: every ring of one operand lies wholly in the
/// other's interior or exterior, and both of its sides — one an interior
/// patch of its owner, one an exterior patch, whichever way round — share
/// that placement, which fixes every cell from one located vertex per ring.
fn relate_areal(
    left: &PreparedPlan,
    right: &PreparedPlan,
    mut ask: Ask<'_>,
) -> crate::error::Result<Option<Outcome>> {
    let Some(left_polygons) = polygons_of(left) else {
        return Ok(None);
    };
    let Some(right_polygons) = polygons_of(right) else {
        return Ok(None);
    };
    let mut interval = De9imInterval::open(StagedRings::shape(), StagedRings::shape());
    if let Some(verdict) = ask.decide(&interval) {
        return Ok(Some(Outcome::Verdict(verdict)));
    }
    // One shell vertex of each operand located in the other, before either
    // is admitted, staged or swept: a vertex inside the other decides
    // `intersects` and one outside decides `contains`/`within` from two
    // linear probes, where the sweep is n log n and has to finish to
    // conclude anything. The probe reads each polygon as shell minus holes,
    // the reading the point predicates give, so it needs no soundness or
    // validity gate.
    if ask.probes() {
        interval.witness(
            Loc::Boundary,
            locate_in_polygons(&right_polygons, first_vertex(left_polygons[0])),
            Dimension::Point,
        );
        if let Some(verdict) = ask.decide(&interval) {
            return Ok(Some(Outcome::Verdict(verdict)));
        }
        interval.witness(
            locate_in_polygons(&left_polygons, first_vertex(right_polygons[0])),
            Loc::Boundary,
            Dimension::Point,
        );
        if let Some(verdict) = ask.decide(&interval) {
            return Ok(Some(Outcome::Verdict(verdict)));
        }
    }
    if !sweep_admits(left, &left_polygons)? || !sweep_admits(right, &right_polygons)? {
        return Ok(None);
    }
    let Some(a) = left.staged_rings()? else {
        return Ok(None);
    };
    let Some(b) = right.staged_rings()? else {
        return Ok(None);
    };

    let mut boundaries_met = false;
    let verdict = sweep_contacts(&a.chains, &b.chains, |_ia, _ib, contact| {
        match contact {
            Contact::Disjoint => return Ok(None),
            Contact::Cross { .. } => {
                for (row, col, dim) in [
                    (Loc::Interior, Loc::Interior, Dimension::Surface),
                    (Loc::Interior, Loc::Boundary, Dimension::Curve),
                    (Loc::Interior, Loc::Exterior, Dimension::Surface),
                    (Loc::Boundary, Loc::Interior, Dimension::Curve),
                    (Loc::Boundary, Loc::Boundary, Dimension::Point),
                    (Loc::Boundary, Loc::Exterior, Dimension::Curve),
                    (Loc::Exterior, Loc::Interior, Dimension::Surface),
                    (Loc::Exterior, Loc::Boundary, Dimension::Curve),
                ] {
                    interval.witness(row, col, dim);
                }
            },
            Contact::Touch { .. } => {
                interval.witness(Loc::Boundary, Loc::Boundary, Dimension::Point);
            },
            Contact::Collinear { span } if same_point(span.start, span.end) => {
                interval.witness(Loc::Boundary, Loc::Boundary, Dimension::Point);
            },
            Contact::Collinear { .. } => {
                interval.witness(Loc::Boundary, Loc::Boundary, Dimension::Curve);
            },
        }
        boundaries_met = true;
        Ok(ask.decide(&interval))
    })?;
    if let Some(verdict) = verdict {
        return Ok(Some(Outcome::Verdict(verdict)));
    }
    // Every contact has been seen, so the boundaries' meeting cannot rise
    // further; after a proper crossing that was the one cell still open.
    interval.settle(Loc::Boundary, Loc::Boundary);
    if let Some(matrix) = interval.matrix() {
        return Ok(Some(Outcome::Matrix(matrix)));
    }
    if boundaries_met {
        // Sides at a touch are the arrangement fold's business.
        return Ok(None);
    }

    // Laminar placement: one vertex locates each ring, and the ring's two
    // sides — one interior and one exterior patch of its owner — inherit it.
    for &vertex in &a.ring_vertices {
        let Some(placement) = locate_off_boundary(&right_polygons, vertex) else {
            return Ok(None);
        };
        interval.witness(Loc::Interior, placement, Dimension::Surface);
        interval.witness(Loc::Exterior, placement, Dimension::Surface);
        interval.witness(Loc::Boundary, placement, Dimension::Curve);
    }
    for &vertex in &b.ring_vertices {
        let Some(placement) = locate_off_boundary(&left_polygons, vertex) else {
            return Ok(None);
        };
        interval.witness(placement, Loc::Interior, Dimension::Surface);
        interval.witness(placement, Loc::Exterior, Dimension::Surface);
        interval.witness(placement, Loc::Boundary, Dimension::Curve);
    }
    for row in [Loc::Interior, Loc::Boundary, Loc::Exterior] {
        for col in [Loc::Interior, Loc::Boundary, Loc::Exterior] {
            interval.settle(row, col);
        }
    }
    if let Some(verdict) = ask.decide(&interval) {
        return Ok(Some(Outcome::Verdict(verdict)));
    }
    Ok(Some(Outcome::Matrix(interval.matrix().expect(
        "every cell of a laminar areal relation is settled",
    ))))
}

/// The full matrix of two areal operands whose boundaries cross properly or
/// never meet, or `None` for the touching cases the contact sweep cannot
/// settle alone.
pub(crate) fn direct_areal_relate(
    left: &PreparedPlan,
    right: &PreparedPlan,
) -> crate::error::Result<Option<De9im>> {
    Ok(match relate_areal(left, right, Ask::Matrix)? {
        Some(Outcome::Matrix(matrix)) => Some(matrix),
        Some(Outcome::Verdict(_)) => unreachable!("no predicate was asked"),
        None => None,
    })
}

/// One named predicate over two areal operands, decided as early as the
/// boundary contacts allow, or `None` when they do not decide it.
pub(crate) fn direct_areal_predicate(
    spec: &PredicateSpec,
    left: &PreparedPlan,
    right: &PreparedPlan,
) -> crate::error::Result<Option<bool>> {
    Ok(relate_areal(
        left,
        right,
        Ask::Predicate(&mut |interval| {
            predicate_verdict(
                interval,
                spec.predicate,
                Dimension::Surface,
                Dimension::Surface,
            )
        }),
    )?
    .map(|outcome| match outcome {
        Outcome::Verdict(verdict) => verdict,
        Outcome::Matrix(matrix) => predicate_matrix_value(spec, matrix),
    }))
}

/// [`direct_areal_predicate`] for a pattern.
pub(crate) fn direct_areal_pattern(
    pattern: CompiledPattern,
    left: &PreparedPlan,
    right: &PreparedPlan,
) -> crate::error::Result<Option<bool>> {
    Ok(relate_areal(
        left,
        right,
        Ask::Predicate(&mut |interval| interval.pattern(pattern)),
    )?
    .map(|outcome| match outcome {
        Outcome::Verdict(verdict) => verdict,
        Outcome::Matrix(matrix) => matrix.matches(pattern),
    }))
}

// ---------------------------------------------------------------------------
// Areal against linework
// ---------------------------------------------------------------------------

/// One cut of a line segment by the area's boundary, positioned along the
/// segment by exact projection.
#[derive(Clone)]
struct Cut {
    at: crate::geometry::SegmentProjection,
    point: XY,
    /// A shared span opens (`+1`) or closes (`-1`) here; a point contact is `0`.
    span_delta: i8,
}

/// Relate an areal operand (rows) to a linework operand (columns) from
/// boundary contacts alone, with no topology graph for either side.
///
/// The sweep cuts every line segment where it meets the area's boundary —
/// crossings, touches, and the two ends of each shared span. Between two
/// consecutive cuts the line never meets the boundary, so that whole piece
/// (across as many vertices as it spans) lies on one side of the area, and
/// ONE located point decides it: a line inside a ring costs one location, not
/// one per vertex. A piece inside a shared span lies on the boundary itself.
/// Each cut point is a boundary point of the area, in the line's boundary or
/// interior by mod-2 parity; a line end that is not a cut sits wherever the
/// piece it belongs to does. A bounded area leaves interior and exterior on
/// both sides of any finite line, and its boundary reaches the line's
/// exterior unless shared spans cover every ring segment end to end.
fn relate_areal_linework(
    area: &PreparedPlan,
    line: &PreparedPlan,
    mut ask: Ask<'_>,
) -> crate::error::Result<Option<Outcome>> {
    let Some(polygons) = polygons_of(area) else {
        return Ok(None);
    };
    let Some(linework) = line.staged_linework()? else {
        return Ok(None);
    };
    let mut interval = De9imInterval::open(StagedRings::shape(), linework.shape());
    interval.witness(Loc::Interior, Loc::Exterior, Dimension::Surface);
    // The line's first vertex located in the area before the area is
    // admitted, staged or swept: inside decides `intersects`, outside
    // decides `contains`, from one linear probe.
    if ask.probes() {
        let probe = linework.chains.segments[0].start;
        interval.witness(
            locate_in_polygons(&polygons, probe),
            linework.class_of(probe),
            Dimension::Point,
        );
        if let Some(verdict) = ask.decide(&interval) {
            return Ok(Some(Outcome::Verdict(verdict)));
        }
    }
    if !sweep_admits(area, &polygons)? {
        return Ok(None);
    }
    let Some(rings) = area.staged_rings()? else {
        return Ok(None);
    };

    let mut cuts: Vec<Vec<Cut>> = vec![Vec::new(); linework.chains.segments.len()];
    let mut ring_spans: Vec<(usize, XY, XY)> = Vec::new();
    let verdict = sweep_contacts(
        &rings.chains,
        &linework.chains,
        |ring_ordinal, line_ordinal, contact| {
            let Some(contact_point) = cut_line_at_contact(
                linework.chains.segments[line_ordinal],
                contact,
                &mut cuts[line_ordinal],
            )?
            else {
                return Ok(None);
            };
            if let Contact::Collinear { span } = contact
                && !same_point(span.start, span.end)
            {
                ring_spans.push((ring_ordinal, span.start, span.end));
                interval.witness(Loc::Boundary, Loc::Interior, Dimension::Curve);
            }
            interval.witness(
                Loc::Boundary,
                linework.class_of(contact_point),
                Dimension::Point,
            );
            Ok(ask.decide(&interval))
        },
    )?;
    if let Some(verdict) = verdict {
        return Ok(Some(Outcome::Verdict(verdict)));
    }

    for line_index in 0..linework.chains.line_count() {
        let Some(span) = linework.chains.line_span(line_index) else {
            continue;
        };
        let walked = walk_line_pieces(linework, &polygons, span, &mut cuts, &mut interval)?;
        // Line ends in the boundary column: on a cut they are boundary points
        // of the area, otherwise they sit where their piece does.
        if let Some((start, end)) = linework.chains.line_ends(line_index) {
            for (point, piece) in [(start, walked.first_piece), (end, walked.last_piece)] {
                if linework.class_of(point) != Loc::Boundary {
                    continue;
                }
                let row = if walked.ends_on_cut(point) {
                    Loc::Boundary
                } else {
                    piece.expect("an open line has at least one piece")
                };
                interval.witness(row, Loc::Boundary, Dimension::Point);
            }
        }
        if let Some(verdict) = ask.decide(&interval) {
            return Ok(Some(Outcome::Verdict(verdict)));
        }
    }

    ring_spans.sort_unstable_by_key(|&(ordinal, ..)| ordinal);
    let boundary_covered = rings
        .chains
        .segments
        .iter()
        .enumerate()
        .all(|(ordinal, segment)| covered(*segment, &ring_spans, ordinal));
    if !boundary_covered {
        interval.witness(Loc::Boundary, Loc::Exterior, Dimension::Curve);
    }
    for row in [Loc::Interior, Loc::Boundary, Loc::Exterior] {
        for col in [Loc::Interior, Loc::Boundary, Loc::Exterior] {
            interval.settle(row, col);
        }
    }
    if let Some(verdict) = ask.decide(&interval) {
        return Ok(Some(Outcome::Verdict(verdict)));
    }
    Ok(Some(Outcome::Matrix(interval.matrix().expect(
        "every cell of an areal/linework relation is settled",
    ))))
}

/// Record one boundary contact as cuts of the line segment it fell on, and
/// return one point of it (`None` for no contact).
fn cut_line_at_contact(
    segment: Segment,
    contact: Contact,
    cuts: &mut Vec<Cut>,
) -> crate::error::Result<Option<XY>> {
    use crate::geometry::segment_projection;
    let mut point_cut = |point: XY| -> crate::error::Result<XY> {
        cuts.push(Cut {
            at: segment_projection(point, segment)?,
            point,
            span_delta: 0,
        });
        Ok(point)
    };
    let point = match contact {
        Contact::Disjoint => return Ok(None),
        Contact::Cross { point } | Contact::Touch { point } => point_cut(point)?,
        Contact::Collinear { span } if same_point(span.start, span.end) => point_cut(span.start)?,
        Contact::Collinear { span } => {
            let (open, close) = order_along(segment, span.start, span.end)?;
            cuts.push(Cut {
                at: segment_projection(open, segment)?,
                point: open,
                span_delta: 1,
            });
            cuts.push(Cut {
                at: segment_projection(close, segment)?,
                point: close,
                span_delta: -1,
            });
            span.start
        },
    };
    Ok(Some(point))
}

/// The two ends of a shared span in the order the line segment runs.
fn order_along(segment: Segment, first: XY, second: XY) -> crate::error::Result<(XY, XY)> {
    use crate::geometry::segment_projection;
    let second_projection = segment_projection(second, segment)?;
    let first_projection = segment_projection(first, segment)?;
    if second_projection.cmp_along(&first_projection)?.is_lt() {
        Ok((second, first))
    } else {
        Ok((first, second))
    }
}

/// What one line's piece walk learned that its ends need.
struct WalkedLine {
    first_piece: Option<Loc>,
    last_piece: Option<Loc>,
    cut_ends: [Option<PointKey>; 2],
}

impl WalkedLine {
    fn ends_on_cut(&self, point: XY) -> bool {
        self.cut_ends.contains(&Some(PointKey::new(point)))
    }
}

/// Walk one line's segments in order, cutting each at its boundary contacts
/// and witnessing every piece and cut. A piece that continues across a
/// vertex keeps its location; a cut ends it.
fn walk_line_pieces(
    linework: &StagedLinework,
    polygons: &[&crate::geometry::Polygon],
    span: std::ops::Range<usize>,
    cuts: &mut [Vec<Cut>],
    interval: &mut De9imInterval,
) -> crate::error::Result<WalkedLine> {
    use crate::geometry::segment_midpoint;
    let mut walked = WalkedLine {
        first_piece: None,
        last_piece: None,
        cut_ends: [None, None],
    };
    let mut piece: Option<Loc> = None;
    let mut span_depth = 0_i32;
    let chains = &linework.chains;
    let (line_start, line_end) = (
        chains.segments[span.start].start,
        chains.segments[span.end - 1].end,
    );
    for ordinal in span {
        let segment = chains.segments[ordinal];
        let slot = &mut cuts[ordinal];
        crate::geometry::try_sort_by(slot, |left, right| left.at.cmp_along(&right.at))?;
        let mut cursor = segment.start;
        let mut index = 0;
        while index < slot.len() {
            // Every cut at this position, together.
            let at = index;
            let point = slot[at].point;
            let mut delta = 0_i32;
            while index < slot.len() && slot[index].at.cmp_along(&slot[at].at)?.is_eq() {
                delta += i32::from(slot[index].span_delta);
                index += 1;
            }
            // A stretch inside a shared span lies on the boundary itself and
            // was witnessed when the span arrived; anything else is a piece.
            if !same_point(cursor, point) && span_depth == 0 {
                {
                    let location = *piece.get_or_insert_with(|| {
                        locate_in_polygons(
                            polygons,
                            segment_midpoint(Segment {
                                start: cursor,
                                end: point,
                            })
                            .xy(),
                        )
                    });
                    interval.witness(location, Loc::Interior, Dimension::Curve);
                    walked.first_piece.get_or_insert(location);
                    walked.last_piece = Some(location);
                }
            }
            interval.witness(Loc::Boundary, linework.class_of(point), Dimension::Point);
            if same_point(point, line_start) {
                walked.cut_ends[0] = Some(PointKey::new(point));
            }
            if same_point(point, line_end) {
                walked.cut_ends[1] = Some(PointKey::new(point));
            }
            span_depth += delta;
            cursor = point;
            piece = None;
        }
        if !same_point(cursor, segment.end) && span_depth == 0 {
            let location = *piece.get_or_insert_with(|| {
                locate_in_polygons(
                    polygons,
                    segment_midpoint(Segment {
                        start: cursor,
                        end: segment.end,
                    })
                    .xy(),
                )
            });
            interval.witness(location, Loc::Interior, Dimension::Curve);
            walked.first_piece.get_or_insert(location);
            walked.last_piece = Some(location);
        }
    }
    Ok(walked)
}

/// The full matrix of an areal operand against a linework operand, in
/// either order, or `None` when either is not admitted here.
pub(crate) fn direct_areal_linework_relate(
    left: &PreparedPlan,
    right: &PreparedPlan,
) -> crate::error::Result<Option<De9im>> {
    let unwrap = |outcome: Outcome| match outcome {
        Outcome::Matrix(matrix) => matrix,
        Outcome::Verdict(_) => unreachable!("no predicate was asked"),
    };
    if let Some(outcome) = relate_areal_linework(left, right, Ask::Matrix)? {
        return Ok(Some(unwrap(outcome)));
    }
    Ok(relate_areal_linework(right, left, Ask::Matrix)?.map(|outcome| unwrap(outcome).transpose()))
}

/// One named predicate over an areal and a linework operand, in either
/// order, decided as early as the evidence allows.
pub(crate) fn direct_areal_linework_predicate(
    spec: &PredicateSpec,
    left: &PreparedPlan,
    right: &PreparedPlan,
) -> crate::error::Result<Option<bool>> {
    let finish = |outcome: Outcome, spec: &PredicateSpec| match outcome {
        Outcome::Verdict(verdict) => verdict,
        Outcome::Matrix(matrix) => predicate_matrix_value(spec, matrix),
    };
    if let Some(outcome) = relate_areal_linework(
        left,
        right,
        Ask::Predicate(&mut |interval| {
            predicate_verdict(
                interval,
                spec.predicate,
                Dimension::Surface,
                Dimension::Curve,
            )
        }),
    )? {
        return Ok(Some(finish(outcome, spec)));
    }
    // Swapped operands: the predicate on the (line, area) pair is the same
    // predicate on the transposed (area, line) interval.
    Ok(relate_areal_linework(
        right,
        left,
        Ask::Predicate(&mut |interval| {
            predicate_verdict(
                &interval.transpose(),
                spec.predicate,
                Dimension::Curve,
                Dimension::Surface,
            )
        }),
    )?
    .map(|outcome| match outcome {
        Outcome::Verdict(verdict) => verdict,
        Outcome::Matrix(matrix) => predicate_matrix_value(spec, matrix.transpose()),
    }))
}

/// [`direct_areal_linework_predicate`] for a pattern, in either operand order.
pub(crate) fn direct_areal_linework_pattern(
    pattern: CompiledPattern,
    left: &PreparedPlan,
    right: &PreparedPlan,
) -> crate::error::Result<Option<bool>> {
    let finish = |outcome: Outcome| match outcome {
        Outcome::Verdict(verdict) => verdict,
        Outcome::Matrix(matrix) => matrix.matches(pattern),
    };
    if let Some(outcome) = relate_areal_linework(
        left,
        right,
        Ask::Predicate(&mut |interval| interval.pattern(pattern)),
    )? {
        return Ok(Some(finish(outcome)));
    }
    let transposed = pattern.transpose();
    Ok(relate_areal_linework(
        right,
        left,
        Ask::Predicate(&mut |interval| interval.pattern(transposed)),
    )?
    .map(|outcome| match outcome {
        Outcome::Verdict(verdict) => verdict,
        Outcome::Matrix(matrix) => matrix.transpose().matches(pattern),
    }))
}

#[cfg(test)]
mod tests {
    //! Differential against the graph path, which stays as the oracle. Every
    //! case checks the full matrix (so every predicate) AND each named
    //! predicate through the early-terminating entry, since the latter stops
    //! on partial evidence and could in principle disagree with the settled
    //! matrix.
    use super::*;
    use crate::geometry::relate::graph_matrix::graph_relate_data_oracle;
    use crate::geometry::{CoordSeq, LineSeq, MultiLineString};
    use crate::predicates::{Predicate, predicate_matrix_value};

    fn line(points: &[(f64, f64)]) -> Shape {
        let xy: Vec<XY> = points.iter().map(|&(x, y)| XY::new(x, y)).collect();
        Shape::LineString(LineSeq::try_new(CoordSeq::try_from_xy(&xy).unwrap()).unwrap())
    }

    fn multi(parts: &[&[(f64, f64)]]) -> Shape {
        let lines: Vec<LineSeq> = parts
            .iter()
            .map(|points| {
                let xy: Vec<XY> = points.iter().map(|&(x, y)| XY::new(x, y)).collect();
                LineSeq::try_new(CoordSeq::try_from_xy(&xy).unwrap()).unwrap()
            })
            .collect();
        Shape::MultiLineString(
            MultiLineString::from_dense(lines)
                .unwrap()
                .expect("test multiline payload is nonempty"),
        )
    }

    fn every_predicate() -> [Predicate; 11] {
        [
            Predicate::Intersects,
            Predicate::Disjoint,
            Predicate::Contains,
            Predicate::ContainsProperly,
            Predicate::Within,
            Predicate::Covers,
            Predicate::CoveredBy,
            Predicate::Touches,
            Predicate::Crosses,
            Predicate::Overlaps,
            Predicate::Equals,
        ]
    }

    fn spec_of(predicate: Predicate) -> PredicateSpec {
        PredicateSpec {
            predicate,
            token: "",
            symmetric: false,
            index_envelope: None,
        }
    }

    /// The graph path itself. `graph_relate_shapes` would route through
    /// `graph_relate_data`, which admits this engine first — an oracle that
    /// compared the engine with itself, which is how a wrong collinear
    /// end-to-end case first passed here.
    fn oracle(left: &Shape, right: &Shape) -> De9im {
        graph_relate_data_oracle(
            &PreparedPlan::transient(left).unwrap(),
            &PreparedPlan::transient(right).unwrap(),
        )
        .unwrap()
    }

    fn check(label: &str, left: &Shape, right: &Shape) {
        let expected = oracle(left, right);
        let (left_plan, right_plan) = (
            PreparedPlan::transient(left).unwrap(),
            PreparedPlan::transient(right).unwrap(),
        );
        let direct = direct_linework_relate(&left_plan, &right_plan)
            .unwrap()
            .unwrap_or_else(|| panic!("{label}: the pair should be admitted"));
        assert_eq!(
            direct.text(),
            expected.text(),
            "{label}: direct matrix differs from the graph oracle"
        );
        for predicate in every_predicate() {
            let spec = spec_of(predicate);
            let early = direct_linework_predicate(&spec, &left_plan, &right_plan)
                .unwrap()
                .unwrap();
            assert_eq!(
                early,
                predicate_matrix_value(&spec, expected),
                "{label}: {predicate:?} decided early disagrees with the oracle matrix {}",
                expected.text()
            );
        }
        // And the transpose, since the engine orients pairs.
        let oracle_t = oracle(right, left);
        let direct_t = direct_linework_relate(&right_plan, &left_plan)
            .unwrap()
            .unwrap();
        assert_eq!(direct_t.text(), oracle_t.text(), "{label}: transposed");
    }

    #[test]
    fn structured_corpus_matches_the_graph_oracle() {
        let cases: Vec<(&str, Shape, Shape)> = vec![
            (
                "disjoint, overlapping boxes",
                line(&[(0.0, 0.0), (10.0, 3.0)]),
                line(&[(0.0, 1.0), (7.0, 9.0)]),
            ),
            (
                "proper crossing",
                line(&[(0.0, 0.0), (2.0, 2.0)]),
                line(&[(0.0, 2.0), (2.0, 0.0)]),
            ),
            (
                "shared endpoint",
                line(&[(0.0, 0.0), (1.0, 1.0)]),
                line(&[(1.0, 1.0), (2.0, 0.0)]),
            ),
            (
                "T-junction: endpoint on interior",
                line(&[(0.0, 0.0), (2.0, 0.0)]),
                line(&[(1.0, 0.0), (1.0, 1.0)]),
            ),
            (
                "T-junction from the other side",
                line(&[(1.0, 0.0), (1.0, 1.0)]),
                line(&[(0.0, 0.0), (2.0, 0.0)]),
            ),
            (
                "collinear partial overlap",
                line(&[(0.0, 0.0), (2.0, 0.0)]),
                line(&[(1.0, 0.0), (3.0, 0.0)]),
            ),
            (
                "collinear, meeting end to end",
                line(&[(0.0, 0.0), (1.0, 0.0)]),
                line(&[(1.0, 0.0), (2.0, 0.0)]),
            ),
            (
                "collinear, one ends where the other's interior vertex is",
                line(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)]),
                line(&[(1.0, 0.0), (2.0, 0.0)]),
            ),
            (
                "ring vs collinear line leaving a ring vertex",
                line(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]),
                line(&[(1.0, 0.0), (2.0, 0.0)]),
            ),
            (
                "collinear nested (within)",
                line(&[(0.0, 0.0), (3.0, 0.0)]),
                line(&[(1.0, 0.0), (2.0, 0.0)]),
            ),
            (
                "collinear opposite direction",
                line(&[(0.0, 0.0), (2.0, 0.0)]),
                line(&[(3.0, 0.0), (1.0, 0.0)]),
            ),
            (
                "identical lines",
                line(&[(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)]),
                line(&[(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)]),
            ),
            (
                "equal point sets, different vertices",
                line(&[(0.0, 0.0), (2.0, 0.0)]),
                line(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)]),
            ),
            (
                "equal point sets, reversed",
                line(&[(0.0, 0.0), (2.0, 0.0)]),
                line(&[(2.0, 0.0), (1.0, 0.0), (0.0, 0.0)]),
            ),
            (
                "covered by two spans across a vertex",
                line(&[(0.0, 0.0), (2.0, 0.0)]),
                line(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (1.0, 0.0), (2.0, 0.0)]),
            ),
            (
                "closed ring vs crossing line",
                line(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]),
                line(&[(1.0, -1.0), (1.0, 3.0)]),
            ),
            (
                "closed ring vs line ending on it",
                line(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]),
                line(&[(1.0, -1.0), (1.0, 0.0)]),
            ),
            (
                "closed ring vs line touching at ring start",
                line(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]),
                line(&[(-1.0, -1.0), (0.0, 0.0)]),
            ),
            (
                "two closed rings sharing an edge",
                line(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]),
                line(&[(1.0, 0.0), (2.0, 0.0), (2.0, 1.0), (1.0, 1.0), (1.0, 0.0)]),
            ),
            (
                "multi: three parts at one point (boundary)",
                multi(&[&[(0.0, 0.0), (1.0, 1.0)], &[(0.0, 2.0), (1.0, 1.0)], &[
                    (2.0, 1.0),
                    (1.0, 1.0),
                ]]),
                line(&[(1.0, 1.0), (1.0, 3.0)]),
            ),
            (
                "multi: two parts at one point (interior)",
                multi(&[&[(0.0, 0.0), (1.0, 1.0)], &[(1.0, 1.0), (2.0, 0.0)]]),
                line(&[(1.0, 1.0), (1.0, 3.0)]),
            ),
            (
                "multi: two parts, other line crosses one",
                multi(&[&[(0.0, 0.0), (1.0, 0.0)], &[(5.0, 5.0), (6.0, 5.0)]]),
                line(&[(0.5, -1.0), (0.5, 1.0)]),
            ),
            (
                "multi vs multi with a shared part",
                multi(&[&[(0.0, 0.0), (1.0, 0.0)], &[(5.0, 5.0), (6.0, 5.0)]]),
                multi(&[&[(0.0, 0.0), (1.0, 0.0)], &[(9.0, 9.0), (8.0, 8.0)]]),
            ),
            (
                "repeated vertices",
                line(&[(0.0, 0.0), (1.0, 0.0), (1.0, 0.0), (2.0, 0.0)]),
                line(&[(1.0, 0.0), (1.0, 0.0), (1.0, 2.0)]),
            ),
            (
                "self-crossing bowtie vs line",
                line(&[(0.0, 0.0), (2.0, 2.0), (2.0, 0.0), (0.0, 2.0)]),
                line(&[(1.0, 0.5), (1.0, 1.5)]),
            ),
            (
                "self-overlapping line vs line",
                line(&[(0.0, 0.0), (2.0, 0.0), (1.0, 0.0)]),
                line(&[(1.5, -1.0), (1.5, 1.0)]),
            ),
            (
                "endpoint touches a vertex interior",
                line(&[(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)]),
                line(&[(1.0, 1.0), (1.0, 3.0)]),
            ),
            (
                "line ends inside a collinear span",
                line(&[(0.0, 0.0), (4.0, 0.0)]),
                line(&[(1.0, 0.0), (3.0, 0.0), (3.0, 2.0)]),
            ),
        ];
        for (label, left, right) in &cases {
            check(label, left, right);
        }
    }

    #[test]
    fn a_seeded_lattice_fuzz_matches_the_graph_oracle() {
        // Small integer lattices make shared vertices, collinear overlaps,
        // T-junctions and crossings all common rather than measure-zero.
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut checked = 0;
        for _ in 0..4000 {
            let left_n = 2 + (next() % 5) as usize;
            let right_n = 2 + (next() % 5) as usize;
            let span = 3 + next() % 3;
            let mut points = |count: usize| -> Vec<(f64, f64)> {
                std::iter::repeat_with(|| ((next() % span) as f64, (next() % span) as f64))
                    .take(count)
                    .collect()
            };
            let left = points(left_n);
            let right = points(right_n);
            let label = format!("fuzz {left:?} vs {right:?}");
            let (left, right) = (line(&left), line(&right));
            if stage_linework(&left).is_none() || stage_linework(&right).is_none() {
                continue; // a collapsed line: not admitted, the graph keeps it
            }
            // The graph is the oracle only where it is trusted: on a line
            // that doubles back over itself it drops a shared segment's
            // dimension (see `non_simple_linework_matches_geos`), so the fuzz
            // keeps to simple operands and the non-simple cases are pinned
            // against GEOS by hand.
            // The graph is the oracle only where it is trusted: on lines that
            // double back over themselves it has lost owners and dimensions
            // (see `non_simple_linework_matches_geos`), so the fuzz keeps to
            // simple operands and the non-simple cases are pinned to GEOS.
            if !PreparedPlan::transient(&left)
                .unwrap()
                .is_simple_cached()
                .unwrap()
                || !PreparedPlan::transient(&right)
                    .unwrap()
                    .is_simple_cached()
                    .unwrap()
            {
                continue;
            }
            check(&label, &left, &right);
            checked += 1;
        }
        assert!(checked > 1000, "only {checked} admitted pairs");
    }

    /// Every pattern the engines can be asked, decided early on a partial
    /// interval, must agree with the same pattern read off the oracle's full
    /// matrix. The set mixes the named predicates' patterns with literal-
    /// dimension and wildcard forms, so both the `T`/`F` and `0`/`1`/`2`
    /// literal branches of the three-valued evaluation are exercised.
    #[test]
    fn early_patterns_agree_with_the_oracle_matrix() {
        const PATTERNS: [&[u8; 9]; 14] = [
            b"T********",
            b"FF*FF****",
            b"T*T***T**",
            b"T*****FF*",
            b"**T***T**",
            b"0********",
            b"1********",
            b"*1*******",
            b"*********",
            b"FFFFFFFF*",
            b"1*T***T**",
            b"F*T***T**",
            b"0F1FF0102",
            b"**2******",
        ];
        let corpus: [(Shape, Shape); 4] = [
            (
                line(&[(0.0, 0.0), (2.0, 2.0)]),
                line(&[(0.0, 2.0), (2.0, 0.0)]),
            ),
            (
                line(&[(0.0, 0.0), (2.0, 0.0)]),
                line(&[(1.0, 0.0), (3.0, 0.0)]),
            ),
            (
                line(&[(0.0, 0.0), (1.0, 1.0)]),
                line(&[(5.0, 5.0), (6.0, 6.0)]),
            ),
            (
                multi(&[&[(0.0, 0.0), (1.0, 0.0)], &[(1.0, 0.0), (1.0, 1.0)]]),
                line(&[(1.0, 0.0), (2.0, 0.0)]),
            ),
        ];
        let mut decided_early = 0_usize;
        for (left, right) in &corpus {
            let expected = oracle(left, right);
            let (l, r) = (
                PreparedPlan::transient(left).unwrap(),
                PreparedPlan::transient(right).unwrap(),
            );
            for pattern in PATTERNS {
                let compiled = CompiledPattern::new(std::str::from_utf8(pattern).unwrap()).unwrap();
                let early = direct_linework_pattern(compiled, &l, &r)
                    .unwrap()
                    .unwrap_or_else(|| panic!("pair should be admitted"));
                assert_eq!(
                    early,
                    expected.matches(compiled),
                    "pattern {} on {}",
                    std::str::from_utf8(pattern).unwrap(),
                    expected.text()
                );
                decided_early += 1;
            }
        }
        assert_eq!(decided_early, corpus.len() * PATTERNS.len());
    }

    /// Two long jagged lines: well past the brute-force threshold, so the
    /// pair is joined by the two-pool sweep, and jagged so the sweep bands.
    #[test]
    fn long_jagged_lines_reach_the_two_pool_sweep() {
        let jagged = |count: usize, dx: f64, dy: f64| -> Shape {
            let points: Vec<(f64, f64)> = (0..count)
                .map(|i| {
                    let x = i as f64 * 0.37 + dx;
                    let y = if i % 2 == 0 { 0.0 } else { 1.0 } + dy + (i % 7) as f64 * 0.05;
                    (x, y)
                })
                .collect();
            line(&points)
        };
        // Half a period out of phase, so the sawtooths cross on every tooth;
        // in phase they are parallel offsets that never meet.
        check(
            "long jagged, crossing",
            &jagged(400, 0.0, 0.0),
            &jagged(400, 0.185, -0.4),
        );
        check(
            "long jagged, disjoint",
            &jagged(300, 0.0, 0.0),
            &jagged(300, 0.0, 5.0),
        );
        check(
            "long jagged, separated bands",
            &jagged(300, 0.0, 0.0),
            &jagged(300, 0.0, 2.0),
        );
    }

    #[test]
    fn non_simple_linework_matches_geos() {
        // Expected strings verified against GEOS (shapely 2.x). The graph path
        // answers `0F1F00102` for the first pair — it loses the dimension of
        // the shared first segment once the right operand doubles back over
        // itself — which is the defect that keeps these out of the fuzz.
        let cases: [(&str, Shape, Shape, &str); 4] = [
            (
                "both non-simple, sharing a segment and a vertex",
                line(&[
                    (1.0, 1.0),
                    (2.0, 0.0),
                    (2.0, 1.0),
                    (2.0, 0.0),
                    (0.0, 0.0),
                    (0.0, 0.0),
                ]),
                line(&[(2.0, 0.0), (0.0, 2.0), (1.0, 0.0), (2.0, 2.0), (0.0, 2.0)]),
                "1010F0102",
            ),
            (
                "shared first segment, right operand doubles back",
                line(&[(1.0, 1.0), (2.0, 0.0), (2.0, 0.0), (1.0, 0.0)]),
                line(&[
                    (1.0, 1.0),
                    (2.0, 0.0),
                    (2.0, 1.0),
                    (0.0, 0.0),
                    (2.0, 1.0),
                    (1.0, 2.0),
                ]),
                "1F1F00102",
            ),
            (
                "same, without the doubling back",
                line(&[(1.0, 1.0), (2.0, 0.0), (2.0, 0.0), (1.0, 0.0)]),
                line(&[(1.0, 1.0), (2.0, 0.0), (2.0, 1.0), (1.0, 2.0)]),
                "1F1F00102",
            ),
            (
                "self-overlapping line crossed by a line",
                line(&[(0.0, 0.0), (2.0, 0.0), (1.0, 0.0)]),
                line(&[(1.5, -1.0), (1.5, 1.0)]),
                "0F1FF0102",
            ),
        ];
        for (label, left, right, expected) in &cases {
            let direct = direct_linework_relate(
                &PreparedPlan::transient(left).unwrap(),
                &PreparedPlan::transient(right).unwrap(),
            )
            .unwrap()
            .unwrap();
            assert_eq!(direct.text(), *expected, "{label}");
        }
    }

    mod areal {
        use super::*;
        use crate::geometry::relate::graph_matrix::try_areal_relate;
        use crate::geometry::{MultiPolygon, Polygon, Ring, SharedSlice};

        fn ring(points: &[(f64, f64)]) -> Ring {
            let xy: Vec<XY> = points.iter().map(|&(x, y)| XY::new(x, y)).collect();
            Ring::closed_coordseq(CoordSeq::try_from_xy(&xy).unwrap()).unwrap()
        }

        fn polygon(shell: &[(f64, f64)], holes: &[&[(f64, f64)]]) -> Shape {
            Shape::Polygon(Polygon {
                shell: ring(shell),
                holes: SharedSlice::try_from_vec(holes.iter().map(|hole| ring(hole)).collect())
                    .unwrap(),
            })
        }

        fn multipolygon(parts: &[&[(f64, f64)]]) -> Shape {
            let polygons: Vec<Polygon> = parts
                .iter()
                .map(|shell| Polygon {
                    shell: ring(shell),
                    holes: SharedSlice::empty(),
                })
                .collect();
            Shape::MultiPolygon(
                MultiPolygon::from_dense(polygons)
                    .unwrap()
                    .expect("test multipolygon payload is nonempty"),
            )
        }

        fn square(x: f64, y: f64, size: f64) -> Vec<(f64, f64)> {
            vec![
                (x, y),
                (x + size, y),
                (x + size, y + size),
                (x, y + size),
                (x, y),
            ]
        }

        /// What the direct engine must manage on a case: a proper crossing
        /// decides every predicate, laminar rings yield the whole matrix, and
        /// a touch leaves the finer predicates to the fold.
        #[derive(Clone, Copy, PartialEq, Eq, Debug)]
        enum Expect {
            Crossing,
            Laminar,
            Touching,
        }

        /// Both oracles — the arrangement fold and the graph, which must agree
        /// with each other too — against everything the engine returns, and a
        /// check that it returns what `expect` says it must, so a regression
        /// that merely stops deciding cannot pass as "undecided".
        fn check(label: &str, left: &Shape, right: &Shape, expect: Expect) {
            let fold = try_areal_relate(left, right)
                .unwrap()
                .unwrap_or_else(|| panic!("{label}: fold"));
            let graph = oracle(left, right);
            assert_eq!(
                fold.text(),
                graph.text(),
                "{label}: the two oracles disagree"
            );
            let (left, right) = (
                &PreparedPlan::transient(left).unwrap(),
                &PreparedPlan::transient(right).unwrap(),
            );
            let direct = direct_areal_relate(left, right).unwrap();
            assert_eq!(
                direct.is_some(),
                expect != Expect::Touching,
                "{label}: full matrix availability"
            );
            if let Some(direct) = direct {
                assert_eq!(direct.text(), fold.text(), "{label}: direct matrix");
            }
            for predicate in every_predicate() {
                let spec = spec_of(predicate);
                let early = direct_areal_predicate(&spec, left, right).unwrap();
                let must_decide = match expect {
                    Expect::Crossing | Expect::Laminar => true,
                    Expect::Touching => {
                        matches!(predicate, Predicate::Intersects | Predicate::Disjoint)
                    },
                };
                if must_decide {
                    assert!(early.is_some(), "{label}: {predicate:?} should be decided");
                }
                if let Some(early) = early {
                    assert_eq!(
                        early,
                        predicate_matrix_value(&spec, fold),
                        "{label}: {predicate:?} decided early against {}",
                        fold.text()
                    );
                }
            }
        }

        #[test]
        fn structured_corpus_matches_both_oracles() {
            use Expect::{Crossing, Laminar, Touching};
            let cases: Vec<(&str, Shape, Shape, Expect)> = vec![
                (
                    "disjoint, overlapping bounds",
                    polygon(&[(0.0, 0.0), (4.0, 0.0), (0.0, 4.0), (0.0, 0.0)], &[]),
                    polygon(&[(4.0, 4.0), (1.0, 4.0), (4.0, 1.0), (4.0, 4.0)], &[]),
                    Laminar,
                ),
                (
                    "proper crossing",
                    polygon(&square(0.0, 0.0, 2.0), &[]),
                    polygon(&square(1.0, 1.0, 2.0), &[]),
                    Crossing,
                ),
                (
                    "nested",
                    polygon(&square(0.0, 0.0, 4.0), &[]),
                    polygon(&square(1.0, 1.0, 1.0), &[]),
                    Laminar,
                ),
                (
                    "nested, transposed",
                    polygon(&square(1.0, 1.0, 1.0), &[]),
                    polygon(&square(0.0, 0.0, 4.0), &[]),
                    Laminar,
                ),
                (
                    "inside a hole",
                    polygon(&square(0.0, 0.0, 6.0), &[&square(1.0, 1.0, 4.0)]),
                    polygon(&square(2.0, 2.0, 1.0), &[]),
                    Laminar,
                ),
                (
                    "containing the other's hole",
                    polygon(&square(0.0, 0.0, 6.0), &[&square(2.0, 2.0, 1.0)]),
                    polygon(&square(1.0, 1.0, 4.0), &[]),
                    Laminar,
                ),
                (
                    "hole wraps the other entirely",
                    polygon(&square(1.0, 1.0, 4.0), &[]),
                    polygon(&square(0.0, 0.0, 6.0), &[&square(2.0, 2.0, 1.0)]),
                    Laminar,
                ),
                (
                    "shared edge",
                    polygon(&square(0.0, 0.0, 1.0), &[]),
                    polygon(&square(1.0, 0.0, 1.0), &[]),
                    Touching,
                ),
                (
                    "shared vertex",
                    polygon(&square(0.0, 0.0, 1.0), &[]),
                    polygon(&square(1.0, 1.0, 1.0), &[]),
                    Touching,
                ),
                (
                    "edge partially shared, overlapping",
                    polygon(&square(0.0, 0.0, 2.0), &[]),
                    polygon(&square(1.0, 0.0, 2.0), &[]),
                    Touching,
                ),
                (
                    "hole touching the other's shell",
                    polygon(&square(0.0, 0.0, 6.0), &[&square(1.0, 1.0, 2.0)]),
                    polygon(&square(1.0, 1.0, 1.0), &[]),
                    Touching,
                ),
                (
                    "identical",
                    polygon(&square(0.0, 0.0, 2.0), &[]),
                    polygon(&square(0.0, 0.0, 2.0), &[]),
                    Touching,
                ),
                (
                    "multipolygon: one part inside, one disjoint",
                    multipolygon(&[&square(1.0, 1.0, 1.0), &square(10.0, 10.0, 1.0)]),
                    polygon(&square(0.0, 0.0, 4.0), &[]),
                    Laminar,
                ),
                (
                    "multipolygon: parts on both sides, crossing",
                    multipolygon(&[&square(0.0, 0.0, 2.0), &square(5.0, 5.0, 2.0)]),
                    polygon(&square(1.0, 1.0, 2.0), &[]),
                    Crossing,
                ),
                (
                    "crossing a hole ring only",
                    polygon(&square(0.0, 0.0, 8.0), &[&square(2.0, 2.0, 4.0)]),
                    polygon(&square(1.0, 3.0, 2.0), &[]),
                    Crossing,
                ),
            ];
            for (label, left, right, expect) in &cases {
                check(label, left, right, *expect);
                check(&format!("{label} (transposed)"), right, left, *expect);
            }
        }

        #[test]
        fn seeded_star_polygons_match_both_oracles() {
            // Star-shaped rings about a centre are simple by construction;
            // random radii make crossings, nesting and separation all common.
            let mut state = 0x2545_F491_4F6C_DD1D_u64;
            let mut next = || {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 11) as f64 / (1_u64 << 53) as f64
            };
            let mut decided = 0;
            for _ in 0..600 {
                let n_left = 3 + (next() * 8.0) as usize;
                let n_right = 3 + (next() * 8.0) as usize;
                let scale_left = 1.0 + next();
                let (cx, cy, scale_right) = (next() * 2.5, next() * 2.5, 0.2 + next());
                let mut star = |cx: f64, cy: f64, scale: f64, n: usize| -> Vec<(f64, f64)> {
                    let mut points: Vec<(f64, f64)> = (0..n)
                        .map(|i| {
                            let angle = i as f64 / n as f64 * std::f64::consts::TAU;
                            let radius = scale * (0.4 + 0.6 * next());
                            (cx + radius * angle.cos(), cy + radius * angle.sin())
                        })
                        .collect();
                    points.push(points[0]);
                    points
                };
                let left = polygon(&star(0.0, 0.0, scale_left, n_left), &[]);
                let right = polygon(&star(cx, cy, scale_right, n_right), &[]);
                let Some(fold) = try_areal_relate(&left, &right).unwrap() else {
                    continue;
                };
                // Star shells about distinct centres either cross, nest, or
                // separate; a touch is measure-zero, so every pair must decide.
                let expect = if fold.text() == "212101212" {
                    Expect::Crossing
                } else {
                    Expect::Laminar
                };
                check("fuzz", &left, &right, expect);
                decided += 1;
            }
            assert!(
                decided > 400,
                "only {decided} pairs reached a direct decision"
            );
        }
    }

    #[test]
    fn an_invalid_shell_follows_geos_even_odd() {
        // An asymmetric bowtie is admitted (its net area is nonzero) with no
        // validity gate. Its negative-winding lobe is interior under the
        // nonzero rule both the arrangement fold and this engine use, which is
        // the even-odd reading GEOS gives. Pinned to shapely's answers.
        use crate::geometry::{Polygon, Ring, SharedSlice};
        let ring = |points: &[(f64, f64)]| {
            let xy: Vec<XY> = points.iter().map(|&(x, y)| XY::new(x, y)).collect();
            Ring::closed_coordseq(CoordSeq::try_from_xy(&xy).unwrap()).unwrap()
        };
        let polygon = |points: &[(f64, f64)]| {
            Shape::Polygon(Polygon {
                shell: ring(points),
                holes: SharedSlice::empty(),
            })
        };
        let bowtie = polygon(&[(0.0, 0.0), (2.0, 2.0), (2.0, 0.0), (0.0, 3.0), (0.0, 0.0)]);
        let square = |x: f64, y: f64, size: f64| {
            polygon(&[
                (x, y),
                (x + size, y),
                (x + size, y + size),
                (x, y + size),
                (x, y),
            ])
        };
        for (label, other, geos) in [
            (
                "crossing the negative lobe",
                square(1.5, -0.5, 1.0),
                "212101212",
            ),
            (
                "crossing the positive lobe",
                square(-0.5, 1.8, 1.0),
                "212101212",
            ),
            (
                "inside the negative lobe",
                square(1.6, 0.6, 0.2),
                "212FF1FF2",
            ),
            (
                "inside the positive lobe",
                square(0.2, 1.5, 0.2),
                "212FF1FF2",
            ),
        ] {
            let direct = direct_areal_relate(
                &PreparedPlan::transient(&bowtie).unwrap(),
                &PreparedPlan::transient(&other).unwrap(),
            )
            .unwrap()
            .unwrap_or_else(|| panic!("{label}: admitted"));
            assert_eq!(direct.text(), geos, "{label}");
            let fold = super::super::graph_matrix::try_areal_relate(&bowtie, &other)
                .unwrap()
                .unwrap_or_else(|| panic!("{label}: fold admits the pair"));
            assert_eq!(
                fold.text(),
                geos,
                "{label}: the fold fills nonzero like GEOS"
            );
        }
    }

    mod areal_linework {
        use super::*;
        use crate::geometry::relate::graph_matrix::try_cartesian_polygon_line_relate;
        use crate::geometry::{Polygon, Ring, SharedSlice};

        fn ring(points: &[(f64, f64)]) -> Ring {
            let xy: Vec<XY> = points.iter().map(|&(x, y)| XY::new(x, y)).collect();
            Ring::closed_coordseq(CoordSeq::try_from_xy(&xy).unwrap()).unwrap()
        }
        fn polygon(shell: &[(f64, f64)], holes: &[&[(f64, f64)]]) -> Shape {
            Shape::Polygon(Polygon {
                shell: ring(shell),
                holes: SharedSlice::try_from_vec(holes.iter().map(|hole| ring(hole)).collect())
                    .unwrap(),
            })
        }
        fn square(x: f64, y: f64, size: f64) -> Vec<(f64, f64)> {
            vec![
                (x, y),
                (x + size, y),
                (x + size, y + size),
                (x, y + size),
                (x, y),
            ]
        }

        /// The polygon/line fold is the oracle wherever it admits the pair: it
        /// agrees with GEOS where the graph does not (the graph drops a plain
        /// boundary crossing on the star below — see the ledger). Outside its
        /// admission the graph is what there is.
        fn check(label: &str, area: &Shape, line: &Shape) {
            let (area_plan, line_plan) = (
                PreparedPlan::transient(area).unwrap(),
                PreparedPlan::transient(line).unwrap(),
            );
            let graph = try_cartesian_polygon_line_relate(&area_plan, &line_plan)
                .unwrap()
                .unwrap_or_else(|| oracle(area, line));
            let direct = direct_areal_linework_relate(&area_plan, &line_plan)
                .unwrap()
                .unwrap_or_else(|| panic!("{label}: admitted"));
            assert_eq!(direct.text(), graph.text(), "{label}: direct matrix");
            let swapped = direct_areal_linework_relate(&line_plan, &area_plan)
                .unwrap()
                .unwrap();
            assert_eq!(
                swapped.text(),
                graph.transpose().text(),
                "{label}: swapped matrix"
            );
            for predicate in every_predicate() {
                let spec = spec_of(predicate);
                let early = direct_areal_linework_predicate(&spec, &area_plan, &line_plan)
                    .unwrap()
                    .unwrap_or_else(|| panic!("{label}: {predicate:?} admitted"));
                assert_eq!(
                    early,
                    predicate_matrix_value(&spec, graph),
                    "{label}: {predicate:?}"
                );
                let early_swapped = direct_areal_linework_predicate(&spec, &line_plan, &area_plan)
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    early_swapped,
                    predicate_matrix_value(&spec, graph.transpose()),
                    "{label}: {predicate:?} swapped"
                );
            }
        }

        #[test]
        fn structured_corpus_matches_the_graph_oracle() {
            let unit = polygon(&square(0.0, 0.0, 4.0), &[]);
            let holed = polygon(&square(0.0, 0.0, 6.0), &[&square(2.0, 2.0, 2.0)]);
            let cases: Vec<(&str, &Shape, Shape)> = vec![
                ("crossing", &unit, line(&[(-1.0, 1.0), (5.0, 1.0)])),
                ("inside", &unit, line(&[(1.0, 1.0), (2.0, 2.0)])),
                (
                    "outside, overlapping bounds",
                    &unit,
                    line(&[(-1.0, 5.0), (5.0, -1.0)]),
                ),
                ("along an edge", &unit, line(&[(1.0, 0.0), (3.0, 0.0)])),
                (
                    "touching a vertex from outside",
                    &unit,
                    line(&[(4.0, 4.0), (6.0, 6.0)]),
                ),
                ("through a vertex", &unit, line(&[(3.0, 5.0), (5.0, 3.0)])),
                (
                    "ending on the boundary from inside",
                    &unit,
                    line(&[(2.0, 2.0), (4.0, 2.0)]),
                ),
                (
                    "ending on the boundary from outside",
                    &unit,
                    line(&[(6.0, 2.0), (4.0, 2.0)]),
                ),
                (
                    "entering, leaving, re-entering",
                    &unit,
                    line(&[(2.0, 2.0), (6.0, 2.0), (6.0, 3.0), (2.0, 3.0)]),
                ),
                (
                    "covering the whole boundary",
                    &unit,
                    line(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]),
                ),
                (
                    "covering the boundary and more",
                    &unit,
                    line(&[(-1.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]),
                ),
                ("inside a hole", &holed, line(&[(2.5, 2.5), (3.5, 3.5)])),
                ("crossing a hole", &holed, line(&[(1.0, 3.0), (5.0, 3.0)])),
                ("along a hole edge", &holed, line(&[(2.0, 2.0), (4.0, 2.0)])),
                (
                    "multi: one part inside, one outside",
                    &unit,
                    multi(&[&[(1.0, 1.0), (2.0, 2.0)], &[(10.0, 10.0), (11.0, 11.0)]]),
                ),
                (
                    "multi: parts meeting on the boundary",
                    &unit,
                    multi(&[&[(2.0, 2.0), (4.0, 2.0)], &[(4.0, 2.0), (6.0, 2.0)]]),
                ),
                (
                    "closed line inside",
                    &unit,
                    line(&[(1.0, 1.0), (2.0, 1.0), (2.0, 2.0), (1.0, 1.0)]),
                ),
                (
                    "collinear touch at a segment end",
                    &unit,
                    line(&[(4.0, 1.0), (4.0, 0.0), (5.0, 0.0)]),
                ),
            ];
            for (label, area, probe) in &cases {
                check(label, area, probe);
            }
        }

        #[test]
        fn seeded_lattice_lines_against_a_star_match_the_graph_oracle() {
            let mut state = 0x1234_5678_9ABC_DEF1_u64;
            let mut next = || {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state
            };
            let star: Vec<(f64, f64)> = {
                let n = 9_u32;
                let mut points: Vec<(f64, f64)> = (0..n)
                    .map(|i| {
                        let angle = f64::from(i) / f64::from(n) * std::f64::consts::TAU;
                        let radius = if i % 2 == 0 { 3.0 } else { 1.5 };
                        (
                            (3.0 + radius * angle.cos()).round(),
                            (3.0 + radius * angle.sin()).round(),
                        )
                    })
                    .collect();
                points.push(points[0]);
                points
            };
            let area = polygon(&star, &[]);
            let mut checked = 0;
            for _ in 0..3000 {
                let count = 2 + (next() % 4) as usize;
                let points: Vec<(f64, f64)> =
                    std::iter::repeat_with(|| ((next() % 8) as f64, (next() % 8) as f64))
                        .take(count)
                        .collect();
                let probe = line(&points);
                if stage_linework(&probe).is_none()
                    || !PreparedPlan::transient(&probe)
                        .unwrap()
                        .is_simple_cached()
                        .unwrap()
                {
                    continue;
                }
                check(
                    &format!("fuzz star={star:?} line={points:?}"),
                    &area,
                    &probe,
                );
                checked += 1;
            }
            assert!(checked > 1000, "only {checked} pairs checked");
        }
    }
}
