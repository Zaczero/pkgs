//! A DE-9IM matrix known only to within an interval per cell, and predicate
//! evaluation over that interval.
//!
//! The direct relation engine accumulates evidence: a contact witnesses a cell
//! at some dimension (its lower bound rises), and a finished phase proves that
//! no further work can raise a cell (its upper bound falls to meet the lower).
//! A predicate is evaluated under three-valued logic over those bounds — every
//! literal of a DE-9IM pattern is definite once the bounds pin it, and the
//! engine stops the moment the whole pattern is. That is one evaluator for
//! every named predicate and for arbitrary `relate_pattern` strings alike,
//! in place of a hand-written "is it known yet" per predicate.
//!
//! Soundness rests on monotonicity: `lo` only rises, `hi` only falls, and
//! `lo <= hi` throughout, so a literal's verdict never flips once given.

#[cfg(test)]
use super::De9im;
use super::de9im::Loc;
use crate::geometry::{CompiledPattern, Dimension};
#[cfg(test)]
use crate::predicates::engine::predicate_matrix_value;

/// One cell's dimension, with `F` below every real dimension so intervals
/// order naturally.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Cell {
    Empty,
    Dim(Dimension),
}

impl Cell {
    #[cfg(test)]
    const fn ascii(self) -> u8 {
        match self {
            Self::Empty => b'F',
            Self::Dim(Dimension::Point) => b'0',
            Self::Dim(Dimension::Curve) => b'1',
            Self::Dim(Dimension::Surface) => b'2',
        }
    }

    const fn max_of(dims: [Option<Dimension>; 2]) -> Self {
        // The largest dimension a cell shared by two operands can hold is the
        // smaller of the two operands' contributing dimensions; `None` marks a
        // contribution that does not exist (a point has no boundary).
        match dims {
            [Some(left), Some(right)] => Self::Dim(if left.code() < right.code() {
                left
            } else {
                right
            }),
            _ => Self::Empty,
        }
    }
}

/// What one operand can contribute to the matrix: the dimension of its
/// interior and of its boundary (`None` when the boundary is empty — a point,
/// or a closed line whose mod-2 endpoint set cancels).
#[derive(Clone, Copy, Debug)]
pub(crate) struct OperandShape {
    pub interior: Dimension,
    pub boundary: Option<Dimension>,
}

impl OperandShape {
    const fn part(self, loc: Loc) -> Option<Dimension> {
        match loc {
            Loc::Interior => Some(self.interior),
            Loc::Boundary => self.boundary,
            Loc::Exterior => Some(Dimension::Surface),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct De9imInterval {
    lo: [Cell; 9],
    hi: [Cell; 9],
}

const fn index(row: Loc, col: Loc) -> usize {
    (row as usize).wrapping_mul(3).wrapping_add(col as usize)
}

#[expect(
    clippy::indexing_slicing,
    reason = "the three Loc variants map only to cells in the fixed nine-entry DE-9IM array"
)]
const fn interval_cell(cells: &[Cell; 9], row: Loc, col: Loc) -> &Cell {
    &cells[index(row, col)]
}

#[expect(
    clippy::indexing_slicing,
    reason = "the three Loc variants map only to cells in the fixed nine-entry DE-9IM array"
)]
const fn interval_cell_mut(cells: &mut [Cell; 9], row: Loc, col: Loc) -> &mut Cell {
    &mut cells[index(row, col)]
}

const LOCS: [Loc; 3] = [Loc::Interior, Loc::Boundary, Loc::Exterior];

impl De9imInterval {
    /// Nothing witnessed yet; every cell may still reach the largest dimension
    /// the two operand parts can share. Exterior meets exterior at full
    /// dimension for any two bounded operands, so that cell starts decided.
    pub(crate) fn open(left: OperandShape, right: OperandShape) -> Self {
        let mut hi = [Cell::Empty; 9];
        for row in LOCS {
            for col in LOCS {
                *interval_cell_mut(&mut hi, row, col) =
                    Cell::max_of([left.part(row), right.part(col)]);
            }
        }
        let mut lo = [Cell::Empty; 9];
        *interval_cell_mut(&mut lo, Loc::Exterior, Loc::Exterior) = Cell::Dim(Dimension::Surface);
        Self { lo, hi }
    }

    /// Evidence that `row` of the left operand meets `col` of the right at
    /// least at `dim`.
    pub(crate) fn witness(&mut self, row: Loc, col: Loc, dim: Dimension) {
        let maximum = interval_cell(&self.hi, row, col);
        let cell = interval_cell_mut(&mut self.lo, row, col);
        let seen = Cell::Dim(dim);
        debug_assert!(
            seen <= *maximum,
            "witnessed a dimension the operands cannot share"
        );
        if *cell < seen {
            *cell = seen;
        }
    }

    /// Proof that nothing left to do can raise `(row, col)` further.
    pub(crate) const fn settle(&mut self, row: Loc, col: Loc) {
        let minimum = *interval_cell(&self.lo, row, col);
        *interval_cell_mut(&mut self.hi, row, col) = minimum;
    }

    #[cfg(test)]
    pub(crate) fn is_decided(&self) -> bool {
        self.lo == self.hi
    }

    /// The matrix, once every cell is decided.
    #[cfg(test)]
    pub(crate) fn matrix(&self) -> Option<De9im> {
        self.is_decided().then(|| De9im(self.lo.map(Cell::ascii)))
    }

    /// One pattern literal against one cell's bounds.
    fn literal(lo: Cell, hi: Cell, literal: u8) -> Option<bool> {
        match literal {
            b'*' => Some(true),
            b'T' => match (lo, hi) {
                (Cell::Dim(_), _) => Some(true),
                (Cell::Empty, Cell::Empty) => Some(false),
                (Cell::Empty, Cell::Dim(_)) => None,
            },
            b'F' => match (lo, hi) {
                (Cell::Empty, Cell::Empty) => Some(true),
                (Cell::Dim(_), _) => Some(false),
                (Cell::Empty, Cell::Dim(_)) => None,
            },
            b'0' | b'1' | b'2' => {
                let wanted = Cell::Dim(match literal {
                    b'0' => Dimension::Point,
                    b'1' => Dimension::Curve,
                    _ => Dimension::Surface,
                });
                if lo == wanted && hi == wanted {
                    Some(true)
                } else if lo > wanted || hi < wanted {
                    Some(false)
                } else {
                    None
                }
            },
            _ => unreachable!("patterns are validated at their ingress"),
        }
    }

    /// A whole nine-literal pattern (a conjunction), under three-valued
    /// logic: `Some(false)` as soon as any literal fails, `Some(true)` only
    /// when every literal holds, `None` while any literal is still open.
    pub(crate) fn pattern(&self, pattern: CompiledPattern) -> Option<bool> {
        let mut open = false;
        let literals = pattern.cells();
        for ((&lo, &hi), &literal) in self.lo.iter().zip(&self.hi).zip(&literals) {
            match Self::literal(lo, hi, literal) {
                Some(false) => return Some(false),
                Some(true) => {},
                None => open = true,
            }
        }
        (!open).then_some(true)
    }

    /// A disjunction of patterns.
    #[cfg(test)]
    pub(crate) fn any_pattern(&self, patterns: &[CompiledPattern]) -> Option<bool> {
        let mut open = false;
        for &pattern in patterns {
            match self.pattern(pattern) {
                Some(true) => return Some(true),
                Some(false) => {},
                None => open = true,
            }
        }
        (!open).then_some(false)
    }
}

#[cfg(test)]
const fn named_pattern(pattern: &str) -> CompiledPattern {
    match CompiledPattern::new(pattern) {
        Some(compiled) => compiled,
        None => panic!("named DE-9IM pattern is valid"),
    }
}

/// The named predicates as pattern sets over `(left, right)`, with the two
/// dimension-dependent ones resolved from the operand dimensions the engine
/// already knows. These are the OGC definitions `De9im`'s `is_*` methods
/// implement on a decided matrix, spelled as patterns so the interval
/// evaluator can decide them early.
#[cfg(test)]
pub(crate) fn predicate_verdict(
    interval: &De9imInterval,
    predicate: crate::predicates::Predicate,
    left: Dimension,
    right: Dimension,
) -> Option<bool> {
    use crate::predicates::Predicate;
    const INTERSECTS: [CompiledPattern; 4] = [
        named_pattern("T********"),
        named_pattern("*T*******"),
        named_pattern("***T*****"),
        named_pattern("****T****"),
    ];
    const TOUCHES: [CompiledPattern; 3] = [
        named_pattern("FT*******"),
        named_pattern("F**T*****"),
        named_pattern("F***T****"),
    ];
    const COVERS: [CompiledPattern; 4] = [
        named_pattern("T*****FF*"),
        named_pattern("*T****FF*"),
        named_pattern("***T**FF*"),
        named_pattern("****T*FF*"),
    ];
    const COVERED_BY: [CompiledPattern; 4] = [
        named_pattern("T*F**F***"),
        named_pattern("*TF**F***"),
        named_pattern("**FT*F***"),
        named_pattern("**F*TF***"),
    ];
    const CONTAINS: CompiledPattern = named_pattern("T*****FF*");
    const WITHIN: CompiledPattern = named_pattern("T*F**F***");
    const CONTAINS_PROPERLY: CompiledPattern = named_pattern("T**FF*FF*");
    const EQUALS: CompiledPattern = named_pattern("T*F**FFF*");
    const CROSSES_LESS: CompiledPattern = named_pattern("T*T******");
    const CROSSES_GREATER: CompiledPattern = named_pattern("T*****T**");
    const CROSSES_CURVE: CompiledPattern = named_pattern("0********");
    const OVERLAPS_CURVE: CompiledPattern = named_pattern("1*T***T**");
    const OVERLAPS: CompiledPattern = named_pattern("T*T***T**");
    match predicate {
        Predicate::Intersects => interval.any_pattern(&INTERSECTS),
        Predicate::Disjoint => interval.any_pattern(&INTERSECTS).map(|hit| !hit),
        Predicate::Contains => interval.pattern(CONTAINS),
        Predicate::Within => interval.pattern(WITHIN),
        Predicate::Covers => interval.any_pattern(&COVERS),
        Predicate::CoveredBy => interval.any_pattern(&COVERED_BY),
        Predicate::ContainsProperly => interval.pattern(CONTAINS_PROPERLY),
        Predicate::Touches => interval.any_pattern(&TOUCHES),
        Predicate::Equals => interval.pattern(EQUALS),
        Predicate::Crosses => match left.cmp(&right) {
            std::cmp::Ordering::Less => interval.pattern(CROSSES_LESS),
            std::cmp::Ordering::Greater => interval.pattern(CROSSES_GREATER),
            std::cmp::Ordering::Equal if left == Dimension::Curve => {
                interval.pattern(CROSSES_CURVE)
            },
            std::cmp::Ordering::Equal => Some(false),
        },
        Predicate::Overlaps if left != right => Some(false),
        Predicate::Overlaps if left == Dimension::Curve => interval.pattern(OVERLAPS_CURVE),
        Predicate::Overlaps => interval.pattern(OVERLAPS),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::relate_pattern::compile;
    use crate::predicates::Predicate;

    const LINE: OperandShape = OperandShape {
        interior: Dimension::Curve,
        boundary: Some(Dimension::Point),
    };
    const RING: OperandShape = OperandShape {
        interior: Dimension::Curve,
        boundary: None,
    };
    const POINT: OperandShape = OperandShape {
        interior: Dimension::Point,
        boundary: None,
    };
    const AREA: OperandShape = OperandShape {
        interior: Dimension::Surface,
        boundary: Some(Dimension::Curve),
    };

    fn spec_of(predicate: Predicate) -> crate::predicates::PredicateSpec {
        crate::predicates::PredicateSpec {
            predicate,
            token: "",
            symmetric: false,
            index_envelope: None,
        }
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

    /// Settle every cell at its current lower bound.
    fn settle_all(interval: &mut De9imInterval) {
        for row in LOCS {
            for col in LOCS {
                interval.settle(row, col);
            }
        }
    }

    #[test]
    fn the_upper_bounds_follow_the_operand_parts() {
        let open = De9imInterval::open(LINE, POINT);
        // A point has no boundary: the whole B column is impossible.
        for row in LOCS {
            assert_eq!(open.hi[index(row, Loc::Boundary)], Cell::Empty);
        }
        // Interior meets interior at most at a point.
        assert_eq!(
            open.hi[index(Loc::Interior, Loc::Interior)],
            Cell::Dim(Dimension::Point)
        );
        // The line's interior in the point's exterior keeps the line's dimension.
        assert_eq!(
            open.hi[index(Loc::Interior, Loc::Exterior)],
            Cell::Dim(Dimension::Curve)
        );
        assert_eq!(
            open.lo[index(Loc::Exterior, Loc::Exterior)],
            Cell::Dim(Dimension::Surface)
        );
        let ring = De9imInterval::open(RING, LINE);
        for col in LOCS {
            assert_eq!(ring.hi[index(Loc::Boundary, col)], Cell::Empty);
        }
    }

    #[test]
    fn a_decided_interval_agrees_with_the_matrix_methods_on_every_predicate() {
        // Walk a grid of matrices through the interval by witnessing each cell
        // at its target and settling everything, then compare the pattern
        // evaluator against the decided-matrix implementation. Every named
        // predicate, every operand-dimension pairing the patterns depend on.
        let dims = [Dimension::Point, Dimension::Curve, Dimension::Surface];
        let shapes = [POINT, LINE, RING, AREA];
        for &left in &shapes {
            for &right in &shapes {
                let open = De9imInterval::open(left, right);
                // Enumerate every lower-bound assignment consistent with the
                // upper bounds: each cell ranges over F..=hi.
                let choices: Vec<Vec<Cell>> = (0..9)
                    .map(|cell| {
                        let mut options = vec![Cell::Empty];
                        for dim in dims {
                            if Cell::Dim(dim) <= open.hi[cell] {
                                options.push(Cell::Dim(dim));
                            }
                        }
                        options
                    })
                    .collect();
                let mut counters = [0_usize; 9];
                loop {
                    let mut interval = open;
                    for cell in 0..9 {
                        if let Cell::Dim(dim) = choices[cell][counters[cell]] {
                            let (row, col) = (LOCS[cell / 3], LOCS[cell % 3]);
                            interval.witness(row, col, dim);
                        }
                    }
                    settle_all(&mut interval);
                    let matrix = interval.matrix().expect("settled");
                    // A non-empty interior lands at its full dimension
                    // somewhere: the interior row of A (and column of B) peaks
                    // at that operand's dimension. Anything else describes an
                    // operand the direct engine never admits — and is where the
                    // matrix-derived dimension `is_crosses_by_dimension` reads
                    // would diverge from the operand dimension the evaluator is
                    // handed.
                    let peak =
                        |cells: [usize; 3]| cells.into_iter().map(|cell| interval.lo[cell]).max();
                    let realizable = peak([0, 1, 2]) == Some(Cell::Dim(left.interior))
                        && peak([0, 3, 6]) == Some(Cell::Dim(right.interior));
                    for predicate in every_predicate() {
                        if !realizable {
                            break;
                        }
                        assert_eq!(
                            predicate_verdict(&interval, predicate, left.interior, right.interior),
                            Some(predicate_matrix_value(&spec_of(predicate), matrix)),
                            "{predicate:?} on {}",
                            matrix.text()
                        );
                    }
                    // Odometer step.
                    let mut cell = 0;
                    loop {
                        if cell == 9 {
                            break;
                        }
                        counters[cell] += 1;
                        if counters[cell] < choices[cell].len() {
                            break;
                        }
                        counters[cell] = 0;
                        cell += 1;
                    }
                    if cell == 9 {
                        break;
                    }
                }
            }
        }
    }

    #[test]
    fn intersects_decides_on_the_first_witness_and_disjoint_only_on_settlement() {
        let mut interval = De9imInterval::open(LINE, LINE);
        let dims = (Dimension::Curve, Dimension::Curve);
        assert_eq!(
            predicate_verdict(&interval, Predicate::Intersects, dims.0, dims.1),
            None
        );
        assert_eq!(
            predicate_verdict(&interval, Predicate::Disjoint, dims.0, dims.1),
            None
        );
        interval.witness(Loc::Interior, Loc::Interior, Dimension::Point);
        assert_eq!(
            predicate_verdict(&interval, Predicate::Intersects, dims.0, dims.1),
            Some(true)
        );
        assert_eq!(
            predicate_verdict(&interval, Predicate::Disjoint, dims.0, dims.1),
            Some(false)
        );
        // Touches needs II empty, and II is now witnessed: decided false at
        // once, before anything else settles.
        assert_eq!(
            predicate_verdict(&interval, Predicate::Touches, dims.0, dims.1),
            Some(false)
        );

        let mut interval = De9imInterval::open(LINE, LINE);
        for row in [Loc::Interior, Loc::Boundary] {
            for col in [Loc::Interior, Loc::Boundary] {
                interval.settle(row, col);
            }
        }
        assert_eq!(
            predicate_verdict(&interval, Predicate::Disjoint, dims.0, dims.1),
            Some(true)
        );
        // The exterior cells are still open, and `equals` depends on them.
        assert_eq!(
            predicate_verdict(&interval, Predicate::Equals, dims.0, dims.1),
            Some(false),
            "II is settled empty, which alone refutes equality"
        );
        assert!(!interval.is_decided());
    }

    #[test]
    fn crosses_between_lines_waits_for_the_interior_dimension_to_settle() {
        let mut interval = De9imInterval::open(LINE, LINE);
        interval.witness(Loc::Interior, Loc::Interior, Dimension::Point);
        // A point witnessed, but a collinear overlap could still lift II to 1.
        assert_eq!(
            predicate_verdict(
                &interval,
                Predicate::Crosses,
                Dimension::Curve,
                Dimension::Curve
            ),
            None
        );
        interval.settle(Loc::Interior, Loc::Interior);
        assert_eq!(
            predicate_verdict(
                &interval,
                Predicate::Crosses,
                Dimension::Curve,
                Dimension::Curve
            ),
            Some(true)
        );
        let mut overlap = De9imInterval::open(LINE, LINE);
        overlap.witness(Loc::Interior, Loc::Interior, Dimension::Curve);
        assert_eq!(
            predicate_verdict(
                &overlap,
                Predicate::Crosses,
                Dimension::Curve,
                Dimension::Curve
            ),
            Some(false),
            "II already exceeds the point dimension crosses demands"
        );
    }

    #[test]
    fn a_decided_matrix_matches_every_cell_literal_independently() {
        let mut interval = De9imInterval::open(AREA, AREA);
        interval.witness(Loc::Interior, Loc::Boundary, Dimension::Point);
        interval.witness(Loc::Interior, Loc::Exterior, Dimension::Curve);
        interval.witness(Loc::Boundary, Loc::Interior, Dimension::Curve);
        interval.witness(Loc::Boundary, Loc::Exterior, Dimension::Point);
        interval.witness(Loc::Exterior, Loc::Interior, Dimension::Surface);
        interval.witness(Loc::Exterior, Loc::Boundary, Dimension::Curve);
        settle_all(&mut interval);
        let matrix = interval.matrix().expect("every cell settled");
        assert_eq!(matrix.text(), "F011F0212");

        let expected = |entry: u8, literal: u8| -> bool {
            match literal {
                b'*' => true,
                b'T' => entry != b'F',
                b'F' => entry == b'F',
                b'0' | b'1' | b'2' => entry == literal,
                _ => unreachable!(),
            }
        };
        let entries = matrix.text().into_bytes();
        for cell in 0..9 {
            for literal in *b"TF*012" {
                let mut cells = *b"*********";
                cells[cell] = literal;
                let pattern = compile(std::str::from_utf8(&cells).unwrap());
                let want = expected(entries[cell], literal);
                assert_eq!(
                    matrix.matches(pattern),
                    want,
                    "cell {cell} entry {} vs {}",
                    entries[cell] as char,
                    literal as char
                );
                assert_eq!(interval.pattern(pattern), Some(want));
            }
        }
        assert!(!matrix.matches(compile("0********")));
        assert!(!matrix.matches(compile("********F")));
        assert!(!matrix.matches(compile("0*******F")));
        assert!(matrix.matches(compile("F********")));
        assert!(matrix.matches(compile("********2")));
        assert!(matrix.matches(compile("F*******2")));
        assert_eq!(interval.pattern(compile("0********")), Some(false));
        assert_eq!(interval.pattern(compile("********F")), Some(false));
        assert_eq!(interval.pattern(compile("0*******F")), Some(false));
        assert_eq!(interval.pattern(compile("F*******2")), Some(true));
    }

    #[test]
    fn a_pattern_string_is_decided_literal_by_literal() {
        let mut interval = De9imInterval::open(AREA, AREA);
        assert_eq!(interval.pattern(compile("T********")), None);
        assert_eq!(interval.pattern(compile("*********")), Some(true));
        interval.witness(Loc::Interior, Loc::Interior, Dimension::Curve);
        assert_eq!(interval.pattern(compile("T********")), Some(true));
        // Two areas can still meet at a surface, so the exact dimension waits.
        assert_eq!(interval.pattern(compile("1********")), None);
        interval.settle(Loc::Interior, Loc::Interior);
        assert_eq!(interval.pattern(compile("1********")), Some(true));
        assert_eq!(interval.pattern(compile("0********")), Some(false));
        assert_eq!(interval.pattern(compile("F********")), Some(false));
        assert_eq!(interval.pattern(compile("T*******F")), Some(false));
        assert_eq!(interval.pattern(compile("1*T******")), None);
    }

    #[test]
    fn a_witness_at_the_reachable_maximum_decides_the_cell_by_itself() {
        // Area against line: their interiors can share at most a curve, so
        // one collinear-overlap witness settles the cell without a phase end.
        let mut interval = De9imInterval::open(AREA, LINE);
        interval.witness(Loc::Interior, Loc::Interior, Dimension::Curve);
        assert_eq!(interval.pattern(compile("1********")), Some(true));
    }
}
