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
use crate::geometry::{Dimension, PointKey, Segment, Shape, XY, same_point};
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
    let mut verdict = None;

    let _ = chains.for_each_candidate_pair(|i, j| {
        // The staged pool is one self-join; only cross-operand pairs matter.
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
            Contact::Disjoint => return ControlFlow::Continue(()),
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
        if let Some(found) = decide(&interval) {
            verdict = Some(found);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
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

#[cfg(test)]
mod tests {
    //! Differential against the graph path, which stays as the oracle. Every
    //! case checks the full matrix (so every predicate) AND each named
    //! predicate through the early-terminating entry, since the latter stops
    //! on partial evidence and could in principle disagree with the settled
    //! matrix.
    use super::*;
    use crate::geometry::{CoordSeq, LineSeq, MultiLineString, PreparedPlan};
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
}
