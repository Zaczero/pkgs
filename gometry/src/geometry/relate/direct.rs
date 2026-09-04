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
use crate::geometry::segments::{Contact, segment_contact_with_orientations, segment_envelopes_disjoint};
use crate::geometry::{Dimension, PointKey, PreparedPlan, Segment, Shape, XY, same_point};
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

/// One operand staged for the engine: its segments occupy `[first, first +
/// count)` of the shared pool; `boundary` is its sorted mod-2 endpoint set.
struct Operand {
    first: usize,
    count: usize,
    boundary: Vec<PointKey>,
}

impl Operand {
    fn stage(chains: &mut LineworkChains, lines: &[&crate::geometry::CoordSeq]) -> Self {
        let first_line = chains.line_count();
        let first = chains.segments.len();
        for line in lines {
            // Admission proved a real segment, so staging never declines.
            chains.push_line(line);
        }
        let line_count = chains.line_count() - first_line;
        // OGC line boundaries are the mod-2 endpoints of the whole operand:
        // an end shared by an even number of parts is interior.
        let mut ends: Vec<PointKey> = Vec::with_capacity(2 * line_count);
        for line in first_line..first_line + line_count {
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
        Self {
            first,
            count: chains.segments.len() - first,
            boundary,
        }
    }

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

    const fn owns(&self, ordinal: usize) -> bool {
        ordinal >= self.first && ordinal < self.first + self.count
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
    left: &Shape,
    right: &Shape,
    mut decide: impl FnMut(&De9imInterval) -> Option<bool>,
) -> Option<Outcome> {
    let left_lines = admitted_lines(left)?;
    let right_lines = admitted_lines(right)?;

    let mut chains = LineworkChains::default();
    let a = Operand::stage(&mut chains, &left_lines);
    let b = Operand::stage(&mut chains, &right_lines);
    let mut interval = De9imInterval::open(a.shape(), b.shape());
    if let Some(verdict) = decide(&interval) {
        return Some(Outcome::Verdict(verdict));
    }

    let mut touched = vec![Touched::None; chains.segments.len()];
    let mut a_boundary_hit = vec![false; a.boundary.len()];
    let mut b_boundary_hit = vec![false; b.boundary.len()];
    // Collinear spans, per segment ordinal, as the two vertices bounding them.
    let mut spans: Vec<(usize, XY, XY)> = Vec::new();

    let verdict = sweep_contacts(&chains, &a, |ia, ib, contact| {
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
            Contact::Disjoint => return None,
            Contact::Cross { .. } => {
                interval.witness(Loc::Interior, Loc::Interior, Dimension::Point);
                for ordinal in [ia, ib] {
                    if touched[ordinal] == Touched::None {
                        touched[ordinal] = Touched::Points;
                    }
                }
            },
            Contact::Touch { point } => {
                vertex(point, &mut interval);
                for ordinal in [ia, ib] {
                    if touched[ordinal] == Touched::None {
                        touched[ordinal] = Touched::Points;
                    }
                }
            },
            // Collinear segments that meet end to end share one point, not a
            // span: that is a vertex touch, and it must not raise II to a curve.
            Contact::Collinear { span } if same_point(span.start, span.end) => {
                vertex(span.start, &mut interval);
                for ordinal in [ia, ib] {
                    if touched[ordinal] == Touched::None {
                        touched[ordinal] = Touched::Points;
                    }
                }
            },
            Contact::Collinear { span } => {
                interval.witness(Loc::Interior, Loc::Interior, Dimension::Curve);
                vertex(span.start, &mut interval);
                vertex(span.end, &mut interval);
                touched[ia] = Touched::Spans;
                touched[ib] = Touched::Spans;
                spans.push((ia, span.start, span.end));
                spans.push((ib, span.start, span.end));
            },
        }
        decide(&interval)
    });
    if let Some(verdict) = verdict {
        return Some(Outcome::Verdict(verdict));
    }

    if let Some(verdict) = settle_after_sweep(
        &mut interval,
        &chains,
        [&a, &b],
        &touched,
        [&a_boundary_hit, &b_boundary_hit],
        spans,
        &mut decide,
    ) {
        return Some(Outcome::Verdict(verdict));
    }
    Some(Outcome::Matrix(
        interval.matrix().expect("every cell of a linework relation is settled"),
    ))
}

/// Drive the candidate sweep over the staged pool and hand every
/// cross-operand contact to `on_contact` as `(a_ordinal, b_ordinal,
/// contact)`, stopping with its verdict as soon as it returns one. The pool
/// is one self-join and `a` owns a prefix of it, so same-operand pairs are
/// skipped here.
fn sweep_contacts(
    chains: &LineworkChains,
    a: &Operand,
    mut on_contact: impl FnMut(usize, usize, Contact) -> Option<bool>,
) -> Option<bool> {
    let mut verdict = None;
    let _ = chains.for_each_candidate_pair(|i, j| {
        let (i_left, j_left) = (a.owns(i), a.owns(j));
        if i_left == j_left {
            return ControlFlow::Continue(());
        }
        let (ia, ib) = if i_left { (i, j) } else { (j, i) };
        let (sa, sb): (Segment, Segment) = (chains.segments[ia], chains.segments[ib]);
        if segment_envelopes_disjoint(sa, sb) {
            return ControlFlow::Continue(());
        }
        let (contact, _) = segment_contact_with_orientations(sa, sb);
        if let Some(found) = on_contact(ia, ib, contact) {
            verdict = Some(found);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    verdict
}

/// The phases after the contact sweep: the inner cells settle, boundary
/// points no contact reached witness the exterior columns, and each interior
/// reaches the other's exterior unless collinear spans cover every segment.
/// `decide` is consulted after each phase.
fn settle_after_sweep(
    interval: &mut De9imInterval,
    chains: &LineworkChains,
    operands: [&Operand; 2],
    touched: &[Touched],
    boundary_hit: [&[bool]; 2],
    mut spans: Vec<(usize, XY, XY)>,
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
    spans.sort_unstable_by_key(|&(ordinal, _, _)| ordinal);
    for (operand, row, col) in [
        (operands[0], Loc::Interior, Loc::Exterior),
        (operands[1], Loc::Exterior, Loc::Interior),
    ] {
        let segments = &chains.segments[operand.first..operand.first + operand.count];
        let reaches_exterior = segments.iter().enumerate().any(|(offset, segment)| {
            let ordinal = operand.first + offset;
            match touched[ordinal] {
                Touched::None | Touched::Points => true,
                Touched::Spans => !covered(*segment, &spans, ordinal),
            }
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
    let first = spans.partition_point(|&(other, _, _)| other < ordinal);
    let mut intervals: Vec<(f64, f64)> = spans[first..]
        .iter()
        .take_while(|&&(other, _, _)| other == ordinal)
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
pub(crate) fn direct_linework_relate(left: &Shape, right: &Shape) -> Option<De9im> {
    match relate_linework(left, right, |_| None)? {
        Outcome::Matrix(matrix) => Some(matrix),
        Outcome::Verdict(_) => unreachable!("no predicate was asked"),
    }
}

/// One named predicate over two linework operands, decided as early as the
/// evidence allows, or `None` when either operand is not admitted here.
pub(crate) fn direct_linework_predicate(
    spec: &PredicateSpec,
    left: &Shape,
    right: &Shape,
) -> Option<bool> {
    let outcome = relate_linework(left, right, |interval| {
        predicate_verdict(interval, spec.predicate, Dimension::Curve, Dimension::Curve)
    })?;
    Some(match outcome {
        Outcome::Verdict(verdict) => verdict,
        Outcome::Matrix(matrix) => predicate_matrix_value(spec, matrix),
    })
}

// ---------------------------------------------------------------------------
// Areal operands
// ---------------------------------------------------------------------------

/// A polygon operand's rings, with one vertex of each for the placement
/// phase. Shells and holes are not told apart: whichever side of a ring is
/// the owner's interior, the ring's two sides are one interior and one
/// exterior patch of the owner, and with no contact both sit in the same
/// region of the other operand.
struct ArealOperand {
    staged: Operand,
    ring_vertices: Vec<XY>,
}

impl ArealOperand {
    fn stage(chains: &mut LineworkChains, polygons: &[&crate::geometry::Polygon]) -> Self {
        let mut lines: Vec<&crate::geometry::CoordSeq> = Vec::new();
        let mut ring_vertices = Vec::new();
        for polygon in polygons {
            for ring in polygon.rings() {
                lines.push(ring.coords());
                ring_vertices.push(XY::new(ring.coords().xs()[0], ring.coords().ys()[0]));
            }
        }
        let staged = Operand::stage(chains, &lines);
        debug_assert!(
            staged.boundary.is_empty(),
            "rings are closed and contribute no mod-2 boundary points"
        );
        Self {
            staged,
            ring_vertices,
        }
    }

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
fn admitted_polygons(plan: &PreparedPlan) -> Option<Vec<&crate::geometry::Polygon>> {
    let mut polygons = Vec::new();
    super::graph_matrix::collect_areal_polygons(plan.shape(), &mut polygons)?;
    if polygons.is_empty() {
        return None;
    }
    let adjacency_possible = polygons.len() > 1 || polygons.iter().any(|polygon| !polygon.holes.is_empty());
    if adjacency_possible && plan.validate_cached().is_some() {
        return None;
    }
    Some(polygons)
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
    if interior { Loc::Interior } else { Loc::Exterior }
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
    mut decide: impl FnMut(&De9imInterval) -> Option<bool>,
) -> Option<Outcome> {
    let left_polygons = admitted_polygons(left)?;
    let right_polygons = admitted_polygons(right)?;

    let mut chains = LineworkChains::default();
    let a = ArealOperand::stage(&mut chains, &left_polygons);
    let b = ArealOperand::stage(&mut chains, &right_polygons);
    let mut interval = De9imInterval::open(ArealOperand::shape(), ArealOperand::shape());
    if let Some(verdict) = decide(&interval) {
        return Some(Outcome::Verdict(verdict));
    }

    let mut boundaries_met = false;
    let verdict = sweep_contacts(&chains, &a.staged, |_ia, _ib, contact| {
        match contact {
            Contact::Disjoint => return None,
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
        decide(&interval)
    });
    if let Some(verdict) = verdict {
        return Some(Outcome::Verdict(verdict));
    }
    // Every contact has been seen, so the boundaries' meeting cannot rise
    // further; after a proper crossing that was the one cell still open.
    interval.settle(Loc::Boundary, Loc::Boundary);
    if let Some(matrix) = interval.matrix() {
        return Some(Outcome::Matrix(matrix));
    }
    if boundaries_met {
        // Sides at a touch are the arrangement fold's business.
        return None;
    }

    // Laminar placement: one vertex locates each ring, and the ring's two
    // sides — one interior and one exterior patch of its owner — inherit it.
    for &vertex in &a.ring_vertices {
        let placement = locate_off_boundary(&right_polygons, vertex)?;
        interval.witness(Loc::Interior, placement, Dimension::Surface);
        interval.witness(Loc::Exterior, placement, Dimension::Surface);
        interval.witness(Loc::Boundary, placement, Dimension::Curve);
    }
    for &vertex in &b.ring_vertices {
        let placement = locate_off_boundary(&left_polygons, vertex)?;
        interval.witness(placement, Loc::Interior, Dimension::Surface);
        interval.witness(placement, Loc::Exterior, Dimension::Surface);
        interval.witness(placement, Loc::Boundary, Dimension::Curve);
    }
    for row in [Loc::Interior, Loc::Boundary, Loc::Exterior] {
        for col in [Loc::Interior, Loc::Boundary, Loc::Exterior] {
            interval.settle(row, col);
        }
    }
    if let Some(verdict) = decide(&interval) {
        return Some(Outcome::Verdict(verdict));
    }
    Some(Outcome::Matrix(
        interval.matrix().expect("every cell of a laminar areal relation is settled"),
    ))
}

/// The full matrix of two areal operands whose boundaries cross properly or
/// never meet, or `None` for the touching cases the contact sweep cannot
/// settle alone.
pub(crate) fn direct_areal_relate(left: &PreparedPlan, right: &PreparedPlan) -> Option<De9im> {
    match relate_areal(left, right, |_| None)? {
        Outcome::Matrix(matrix) => Some(matrix),
        Outcome::Verdict(_) => unreachable!("no predicate was asked"),
    }
}

/// One named predicate over two areal operands, decided as early as the
/// boundary contacts allow, or `None` when they do not decide it.
pub(crate) fn direct_areal_predicate(
    spec: &PredicateSpec,
    left: &PreparedPlan,
    right: &PreparedPlan,
) -> Option<bool> {
    Some(match relate_areal(left, right, |interval| {
        predicate_verdict(interval, spec.predicate, Dimension::Surface, Dimension::Surface)
    })? {
        Outcome::Verdict(verdict) => verdict,
        Outcome::Matrix(matrix) => predicate_matrix_value(spec, matrix),
    })
}

#[cfg(test)]
mod tests {
    //! Differential against the graph path, which stays as the oracle. Every
    //! case checks the full matrix (so every predicate) AND each named
    //! predicate through the early-terminating entry, since the latter stops
    //! on partial evidence and could in principle disagree with the settled
    //! matrix.
    use super::*;
    use crate::geometry::{CoordSeq, LineSeq, MultiLineString};
    use crate::geometry::relate::graph_matrix::graph_relate_data_oracle;
    use crate::predicates::{Predicate, predicate_matrix_value};

    fn line(points: &[(f64, f64)]) -> Shape {
        let xy: Vec<XY> = points.iter().map(|&(x, y)| XY::new(x, y)).collect();
        Shape::LineString(LineSeq::try_new(CoordSeq::from_xy(&xy)).unwrap())
    }

    fn multi(parts: &[&[(f64, f64)]]) -> Shape {
        let lines: Vec<LineSeq> = parts
            .iter()
            .map(|points| {
                let xy: Vec<XY> = points.iter().map(|&(x, y)| XY::new(x, y)).collect();
                LineSeq::try_new(CoordSeq::from_xy(&xy)).unwrap()
            })
            .collect();
        Shape::MultiLineString(MultiLineString::from_dense(lines).unwrap())
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
        graph_relate_data_oracle(&PreparedPlan::transient(left), &PreparedPlan::transient(right))
    }

    fn check(label: &str, left: &Shape, right: &Shape) {
        let expected = oracle(left, right);
        let direct = direct_linework_relate(left, right)
            .unwrap_or_else(|| panic!("{label}: the pair should be admitted"));
        assert_eq!(
            direct.text(),
            expected.text(),
            "{label}: direct matrix differs from the graph oracle"
        );
        for predicate in every_predicate() {
            let spec = spec_of(predicate);
            let early = direct_linework_predicate(&spec, left, right).unwrap();
            assert_eq!(
                early,
                predicate_matrix_value(&spec, expected),
                "{label}: {predicate:?} decided early disagrees with the oracle matrix {}",
                expected.text()
            );
        }
        // And the transpose, since the engine orients pairs.
        let oracle_t = oracle(right, left);
        let direct_t = direct_linework_relate(right, left).unwrap();
        assert_eq!(direct_t.text(), oracle_t.text(), "{label}: transposed");
    }

    #[test]
    fn structured_corpus_matches_the_graph_oracle() {
        let cases: Vec<(&str, Shape, Shape)> = vec![
            ("disjoint, overlapping boxes", line(&[(0.0, 0.0), (10.0, 3.0)]), line(&[(0.0, 1.0), (7.0, 9.0)])),
            ("proper crossing", line(&[(0.0, 0.0), (2.0, 2.0)]), line(&[(0.0, 2.0), (2.0, 0.0)])),
            ("shared endpoint", line(&[(0.0, 0.0), (1.0, 1.0)]), line(&[(1.0, 1.0), (2.0, 0.0)])),
            ("T-junction: endpoint on interior", line(&[(0.0, 0.0), (2.0, 0.0)]), line(&[(1.0, 0.0), (1.0, 1.0)])),
            ("T-junction from the other side", line(&[(1.0, 0.0), (1.0, 1.0)]), line(&[(0.0, 0.0), (2.0, 0.0)])),
            ("collinear partial overlap", line(&[(0.0, 0.0), (2.0, 0.0)]), line(&[(1.0, 0.0), (3.0, 0.0)])),
            ("collinear, meeting end to end", line(&[(0.0, 0.0), (1.0, 0.0)]), line(&[(1.0, 0.0), (2.0, 0.0)])),
            ("collinear, one ends where the other's interior vertex is", line(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)]), line(&[(1.0, 0.0), (2.0, 0.0)])),
            ("ring vs collinear line leaving a ring vertex", line(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]), line(&[(1.0, 0.0), (2.0, 0.0)])),
            ("collinear nested (within)", line(&[(0.0, 0.0), (3.0, 0.0)]), line(&[(1.0, 0.0), (2.0, 0.0)])),
            ("collinear opposite direction", line(&[(0.0, 0.0), (2.0, 0.0)]), line(&[(3.0, 0.0), (1.0, 0.0)])),
            ("identical lines", line(&[(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)]), line(&[(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)])),
            ("equal point sets, different vertices", line(&[(0.0, 0.0), (2.0, 0.0)]), line(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)])),
            ("equal point sets, reversed", line(&[(0.0, 0.0), (2.0, 0.0)]), line(&[(2.0, 0.0), (1.0, 0.0), (0.0, 0.0)])),
            ("covered by two spans across a vertex", line(&[(0.0, 0.0), (2.0, 0.0)]), line(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (1.0, 0.0), (2.0, 0.0)])),
            ("closed ring vs crossing line", line(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]), line(&[(1.0, -1.0), (1.0, 3.0)])),
            ("closed ring vs line ending on it", line(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]), line(&[(1.0, -1.0), (1.0, 0.0)])),
            ("closed ring vs line touching at ring start", line(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]), line(&[(-1.0, -1.0), (0.0, 0.0)])),
            ("two closed rings sharing an edge", line(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]), line(&[(1.0, 0.0), (2.0, 0.0), (2.0, 1.0), (1.0, 1.0), (1.0, 0.0)])),
            ("multi: three parts at one point (boundary)", multi(&[&[(0.0, 0.0), (1.0, 1.0)], &[(0.0, 2.0), (1.0, 1.0)], &[(2.0, 1.0), (1.0, 1.0)]]), line(&[(1.0, 1.0), (1.0, 3.0)])),
            ("multi: two parts at one point (interior)", multi(&[&[(0.0, 0.0), (1.0, 1.0)], &[(1.0, 1.0), (2.0, 0.0)]]), line(&[(1.0, 1.0), (1.0, 3.0)])),
            ("multi: two parts, other line crosses one", multi(&[&[(0.0, 0.0), (1.0, 0.0)], &[(5.0, 5.0), (6.0, 5.0)]]), line(&[(0.5, -1.0), (0.5, 1.0)])),
            ("multi vs multi with a shared part", multi(&[&[(0.0, 0.0), (1.0, 0.0)], &[(5.0, 5.0), (6.0, 5.0)]]), multi(&[&[(0.0, 0.0), (1.0, 0.0)], &[(9.0, 9.0), (8.0, 8.0)]])),
            ("repeated vertices", line(&[(0.0, 0.0), (1.0, 0.0), (1.0, 0.0), (2.0, 0.0)]), line(&[(1.0, 0.0), (1.0, 0.0), (1.0, 2.0)])),
            ("self-crossing bowtie vs line", line(&[(0.0, 0.0), (2.0, 2.0), (2.0, 0.0), (0.0, 2.0)]), line(&[(1.0, 0.5), (1.0, 1.5)])),
            ("self-overlapping line vs line", line(&[(0.0, 0.0), (2.0, 0.0), (1.0, 0.0)]), line(&[(1.5, -1.0), (1.5, 1.0)])),
            ("endpoint touches a vertex interior", line(&[(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)]), line(&[(1.0, 1.0), (1.0, 3.0)])),
            ("line ends inside a collinear span", line(&[(0.0, 0.0), (4.0, 0.0)]), line(&[(1.0, 0.0), (3.0, 0.0), (3.0, 2.0)])),
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
            if direct_linework_relate(&left, &right).is_none() {
                continue; // a collapsed line: not admitted, the graph keeps it
            }
            // The graph is the oracle only where it is trusted: on a line
            // that doubles back over itself it drops a shared segment's
            // dimension (see `non_simple_linework_matches_geos`), so the fuzz
            // keeps to simple operands and the non-simple cases are pinned
            // against GEOS by hand.
            if !PreparedPlan::transient(&left).is_simple_cached()
                || !PreparedPlan::transient(&right).is_simple_cached()
            {
                continue;
            }
            check(&label, &left, &right);
            checked += 1;
        }
        assert!(checked > 1000, "only {checked} admitted pairs");
    }

    #[test]
    fn non_simple_linework_matches_geos() {
        // Expected strings verified against GEOS (shapely 2.x). The graph path
        // answers `0F1F00102` for the first pair — it loses the dimension of
        // the shared first segment once the right operand doubles back over
        // itself — which is the defect that keeps these out of the fuzz.
        let cases: [(&str, Shape, Shape, &str); 3] = [
            (
                "shared first segment, right operand doubles back",
                line(&[(1.0, 1.0), (2.0, 0.0), (2.0, 0.0), (1.0, 0.0)]),
                line(&[(1.0, 1.0), (2.0, 0.0), (2.0, 1.0), (0.0, 0.0), (2.0, 1.0), (1.0, 2.0)]),
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
            let direct = direct_linework_relate(left, right).unwrap();
            assert_eq!(direct.text(), *expected, "{label}");
        }
    }

    mod areal {
        use super::*;
        use crate::geometry::relate::graph_matrix::try_areal_relate;
        use crate::geometry::{MultiPolygon, Polygon, Ring};

        fn ring(points: &[(f64, f64)]) -> Ring {
            let xy: Vec<XY> = points.iter().map(|&(x, y)| XY::new(x, y)).collect();
            Ring::closed_coordseq(CoordSeq::from_xy(&xy)).unwrap()
        }

        fn polygon(shell: &[(f64, f64)], holes: &[&[(f64, f64)]]) -> Shape {
            Shape::Polygon(Polygon {
                shell: ring(shell),
                holes: holes.iter().map(|hole| ring(hole)).collect::<Vec<_>>().into(),
            })
        }

        fn multipolygon(parts: &[&[(f64, f64)]]) -> Shape {
            let polygons: Vec<Polygon> = parts
                .iter()
                .map(|shell| Polygon {
                    shell: ring(shell),
                    holes: Vec::new().into(),
                })
                .collect();
            Shape::MultiPolygon(MultiPolygon::from_dense(polygons).unwrap())
        }

        fn square(x: f64, y: f64, size: f64) -> Vec<(f64, f64)> {
            vec![(x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y)]
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
            let fold = try_areal_relate(left, right).unwrap_or_else(|| panic!("{label}: fold"));
            let graph = oracle(left, right);
            assert_eq!(fold.text(), graph.text(), "{label}: the two oracles disagree");
            let (left, right) = (&PreparedPlan::transient(left), &PreparedPlan::transient(right));
            let direct = direct_areal_relate(left, right);
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
                let early = direct_areal_predicate(&spec, left, right);
                let must_decide = match expect {
                    Expect::Crossing | Expect::Laminar => true,
                    Expect::Touching => matches!(predicate, Predicate::Intersects | Predicate::Disjoint),
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
                ("disjoint, overlapping bounds", polygon(&[(0.0, 0.0), (4.0, 0.0), (0.0, 4.0), (0.0, 0.0)], &[]), polygon(&[(4.0, 4.0), (1.0, 4.0), (4.0, 1.0), (4.0, 4.0)], &[]), Laminar),
                ("proper crossing", polygon(&square(0.0, 0.0, 2.0), &[]), polygon(&square(1.0, 1.0, 2.0), &[]), Crossing),
                ("nested", polygon(&square(0.0, 0.0, 4.0), &[]), polygon(&square(1.0, 1.0, 1.0), &[]), Laminar),
                ("nested, transposed", polygon(&square(1.0, 1.0, 1.0), &[]), polygon(&square(0.0, 0.0, 4.0), &[]), Laminar),
                ("inside a hole", polygon(&square(0.0, 0.0, 6.0), &[&square(1.0, 1.0, 4.0)]), polygon(&square(2.0, 2.0, 1.0), &[]), Laminar),
                ("containing the other's hole", polygon(&square(0.0, 0.0, 6.0), &[&square(2.0, 2.0, 1.0)]), polygon(&square(1.0, 1.0, 4.0), &[]), Laminar),
                ("hole wraps the other entirely", polygon(&square(1.0, 1.0, 4.0), &[]), polygon(&square(0.0, 0.0, 6.0), &[&square(2.0, 2.0, 1.0)]), Laminar),
                ("shared edge", polygon(&square(0.0, 0.0, 1.0), &[]), polygon(&square(1.0, 0.0, 1.0), &[]), Touching),
                ("shared vertex", polygon(&square(0.0, 0.0, 1.0), &[]), polygon(&square(1.0, 1.0, 1.0), &[]), Touching),
                ("edge partially shared, overlapping", polygon(&square(0.0, 0.0, 2.0), &[]), polygon(&square(1.0, 0.0, 2.0), &[]), Touching),
                ("hole touching the other's shell", polygon(&square(0.0, 0.0, 6.0), &[&square(1.0, 1.0, 2.0)]), polygon(&square(1.0, 1.0, 1.0), &[]), Touching),
                ("identical", polygon(&square(0.0, 0.0, 2.0), &[]), polygon(&square(0.0, 0.0, 2.0), &[]), Touching),
                ("multipolygon: one part inside, one disjoint", multipolygon(&[&square(1.0, 1.0, 1.0), &square(10.0, 10.0, 1.0)]), polygon(&square(0.0, 0.0, 4.0), &[]), Laminar),
                ("multipolygon: parts on both sides, crossing", multipolygon(&[&square(0.0, 0.0, 2.0), &square(5.0, 5.0, 2.0)]), polygon(&square(1.0, 1.0, 2.0), &[]), Crossing),
                ("crossing a hole ring only", polygon(&square(0.0, 0.0, 8.0), &[&square(2.0, 2.0, 4.0)]), polygon(&square(1.0, 3.0, 2.0), &[]), Crossing),
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
                let Some(fold) = try_areal_relate(&left, &right) else {
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
            assert!(decided > 400, "only {decided} pairs reached a direct decision");
        }
    }

    #[test]
    fn an_invalid_shell_follows_geos_even_odd() {
        // An asymmetric bowtie is admitted (its net area is nonzero) with no
        // validity gate, and its negative-winding lobe is where the engines
        // part ways: the arrangement fold fills `winding >= 1` and reads that
        // lobe as exterior, while this engine reads every ring segment as a
        // boundary — which is the even-odd reading GEOS gives. Pinned to
        // shapely's answers; the fold's divergence is its own item.
        use crate::geometry::{Polygon, Ring};
        let ring = |points: &[(f64, f64)]| {
            let xy: Vec<XY> = points.iter().map(|&(x, y)| XY::new(x, y)).collect();
            Ring::closed_coordseq(CoordSeq::from_xy(&xy)).unwrap()
        };
        let polygon = |points: &[(f64, f64)]| {
            Shape::Polygon(Polygon {
                shell: ring(points),
                holes: Vec::new().into(),
            })
        };
        let bowtie = polygon(&[(0.0, 0.0), (2.0, 2.0), (2.0, 0.0), (0.0, 3.0), (0.0, 0.0)]);
        let square = |x: f64, y: f64, size: f64| {
            polygon(&[(x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y)])
        };
        for (label, other, geos) in [
            ("crossing the negative lobe", square(1.5, -0.5, 1.0), "212101212"),
            ("crossing the positive lobe", square(-0.5, 1.8, 1.0), "212101212"),
            ("inside the negative lobe", square(1.6, 0.6, 0.2), "212FF1FF2"),
            ("inside the positive lobe", square(0.2, 1.5, 0.2), "212FF1FF2"),
        ] {
            let direct = direct_areal_relate(
                &PreparedPlan::transient(&bowtie),
                &PreparedPlan::transient(&other),
            )
            .unwrap_or_else(|| panic!("{label}: admitted"));
            assert_eq!(direct.text(), geos, "{label}");
        }
    }

}
