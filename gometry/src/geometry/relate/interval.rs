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

use super::De9im;
use super::de9im::Loc;
use super::matrix_predicates::{
    self, Cell as MatrixCell, CellMask, Cells as MatrixCells, DimRange, Operand as MatrixOperand,
    Tri,
};
use crate::geometry::{CompiledPattern, Dimension};

/// One cell's dimension, with `F` below every real dimension so intervals
/// order naturally.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Cell {
    Empty,
    Dim(Dimension),
}

impl Cell {
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
    interior_floor: [Option<Dimension>; 2],
}

const fn interval_cell(cells: &[Cell; 9], row: Loc, col: Loc) -> &Cell {
    matrix_predicates::matrix_cell(cells, MatrixCell::from_locations(row, col))
}

const fn interval_cell_mut(cells: &mut [Cell; 9], row: Loc, col: Loc) -> &mut Cell {
    matrix_predicates::matrix_cell_mut(cells, MatrixCell::from_locations(row, col))
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
        Self {
            lo,
            hi,
            interior_floor: [None; 2],
        }
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

    /// A structural certificate that an operand has at least this interior
    /// dimension, even before an intersecting witness has been found.
    pub(crate) fn floor(&mut self, operand: MatrixOperand, dimension: Dimension) {
        let cap = match operand {
            MatrixOperand::Left => max_dimension(
                [MatrixCell::Ii, MatrixCell::Ib, MatrixCell::Ie]
                    .map(|cell| *matrix_predicates::matrix_cell(&self.hi, cell)),
            ),
            MatrixOperand::Right => max_dimension(
                [MatrixCell::Ii, MatrixCell::Bi, MatrixCell::Ei]
                    .map(|cell| *matrix_predicates::matrix_cell(&self.hi, cell)),
            ),
        };
        debug_assert!(
            cap.is_some_and(|cap| cap >= dimension),
            "certified interior dimension exceeds the interval cap"
        );
        let [left_floor, right_floor] = &mut self.interior_floor;
        let floor = match operand {
            MatrixOperand::Left => left_floor,
            MatrixOperand::Right => right_floor,
        };
        if floor.is_none_or(|current| current < dimension) {
            *floor = Some(dimension);
        }
    }

    /// Proof that nothing left to do can raise `(row, col)` further.
    pub(crate) const fn settle(&mut self, row: Loc, col: Loc) {
        let minimum = *interval_cell(&self.lo, row, col);
        *interval_cell_mut(&mut self.hi, row, col) = minimum;
    }

    pub(crate) fn settled_for(&self, cells: CellMask) -> bool {
        MatrixCell::ALL
            .into_iter()
            .filter(|&cell| cells.contains(cell))
            .all(|cell| {
                matrix_predicates::matrix_cell(&self.lo, cell)
                    == matrix_predicates::matrix_cell(&self.hi, cell)
            })
    }

    pub(crate) fn is_decided(&self) -> bool {
        self.lo == self.hi
    }

    /// The matrix, once every cell is decided.
    pub(crate) fn matrix(&self) -> Option<De9im> {
        self.is_decided().then(|| De9im(self.lo.map(Cell::ascii)))
    }

    /// A whole nine-literal pattern (a conjunction), under three-valued
    /// logic: `Some(false)` as soon as any literal fails, `Some(true)` only
    /// when every literal holds, `None` while any literal is still open.
    pub(crate) fn pattern(&self, pattern: CompiledPattern) -> Option<bool> {
        matrix_predicates::eval_pattern(pattern, self).option()
    }
}

fn max_dimension(cells: [Cell; 3]) -> Option<Dimension> {
    cells
        .into_iter()
        .filter_map(|cell| match cell {
            Cell::Empty => None,
            Cell::Dim(dimension) => Some(dimension),
        })
        .max()
}

impl MatrixCells for De9imInterval {
    fn at_least(&self, cell: MatrixCell, dimension: Dimension) -> Tri {
        let lower = *matrix_predicates::matrix_cell(&self.lo, cell);
        let upper = *matrix_predicates::matrix_cell(&self.hi, cell);
        let wanted = Cell::Dim(dimension);
        if lower >= wanted {
            Tri::True
        } else if upper < wanted {
            Tri::False
        } else {
            Tri::Unknown
        }
    }

    fn is_empty(&self, cell: MatrixCell) -> Tri {
        match (
            *matrix_predicates::matrix_cell(&self.lo, cell),
            *matrix_predicates::matrix_cell(&self.hi, cell),
        ) {
            (Cell::Dim(_), _) => Tri::False,
            (Cell::Empty, Cell::Empty) => Tri::True,
            (Cell::Empty, Cell::Dim(_)) => Tri::Unknown,
        }
    }

    fn exactly(&self, cell: MatrixCell, dimension: Dimension) -> Tri {
        let lower = *matrix_predicates::matrix_cell(&self.lo, cell);
        let upper = *matrix_predicates::matrix_cell(&self.hi, cell);
        let wanted = Cell::Dim(dimension);
        if lower == wanted && upper == wanted {
            Tri::True
        } else if lower > wanted || upper < wanted {
            Tri::False
        } else {
            Tri::Unknown
        }
    }

    fn interior_dim(&self, operand: MatrixOperand) -> DimRange {
        let [left_floor, right_floor] = self.interior_floor;
        let (floor, cells) = match operand {
            MatrixOperand::Left => (left_floor, [MatrixCell::Ii, MatrixCell::Ib, MatrixCell::Ie]),
            MatrixOperand::Right => (right_floor, [
                MatrixCell::Ii,
                MatrixCell::Bi,
                MatrixCell::Ei,
            ]),
        };
        let lower =
            max_dimension(cells.map(|cell| *matrix_predicates::matrix_cell(&self.lo, cell)))
                .into_iter()
                .chain(floor)
                .max();
        let upper =
            max_dimension(cells.map(|cell| *matrix_predicates::matrix_cell(&self.hi, cell)));
        DimRange::from_bounds(lower, upper)
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
    const LOCATION_PAIRS: [(Loc, Loc); 9] = [
        (Loc::Interior, Loc::Interior),
        (Loc::Interior, Loc::Boundary),
        (Loc::Interior, Loc::Exterior),
        (Loc::Boundary, Loc::Interior),
        (Loc::Boundary, Loc::Boundary),
        (Loc::Boundary, Loc::Exterior),
        (Loc::Exterior, Loc::Interior),
        (Loc::Exterior, Loc::Boundary),
        (Loc::Exterior, Loc::Exterior),
    ];

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
            assert_eq!(*interval_cell(&open.hi, row, Loc::Boundary), Cell::Empty);
        }
        // Interior meets interior at most at a point.
        assert_eq!(
            *interval_cell(&open.hi, Loc::Interior, Loc::Interior),
            Cell::Dim(Dimension::Point)
        );
        // The line's interior in the point's exterior keeps the line's dimension.
        assert_eq!(
            *interval_cell(&open.hi, Loc::Interior, Loc::Exterior),
            Cell::Dim(Dimension::Curve)
        );
        assert_eq!(
            *interval_cell(&open.lo, Loc::Exterior, Loc::Exterior),
            Cell::Dim(Dimension::Surface)
        );
        let ring = De9imInterval::open(RING, LINE);
        for col in LOCS {
            assert_eq!(*interval_cell(&ring.hi, Loc::Boundary, col), Cell::Empty);
        }
    }

    #[test]
    fn a_decided_interval_agrees_with_the_matrix_methods_on_every_predicate() {
        // Walk matrices through the interval by witnessing each cell at its
        // target and settling everything, then compare interval and decided
        // projections for every matrix admitted by the operand caps.
        let dims = [Dimension::Point, Dimension::Curve, Dimension::Surface];
        let shapes = [POINT, LINE, RING, AREA];
        for &left in &shapes {
            for &right in &shapes {
                let open = De9imInterval::open(left, right);
                // Enumerate every lower-bound assignment consistent with the
                // upper bounds: each cell ranges over F..=hi.
                let choices: Vec<Vec<Cell>> = LOCATION_PAIRS
                    .into_iter()
                    .map(|(row, col)| {
                        let mut options = vec![Cell::Empty];
                        for dim in dims {
                            if Cell::Dim(dim) <= *interval_cell(&open.hi, row, col) {
                                options.push(Cell::Dim(dim));
                            }
                        }
                        options
                    })
                    .collect();
                let mut counters = [0_usize; 9];
                loop {
                    let mut interval = open;
                    for (cell, (row, col)) in LOCATION_PAIRS.into_iter().enumerate() {
                        if let Cell::Dim(dim) = choices[cell][counters[cell]] {
                            interval.witness(row, col, dim);
                        }
                    }
                    settle_all(&mut interval);
                    let matrix = interval.matrix().expect("settled");
                    for predicate in Predicate::ALL {
                        assert_eq!(
                            matrix_predicates::eval(predicate, &interval).option(),
                            Some(matrix_predicates::decided(predicate, matrix)),
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
        assert_eq!(
            matrix_predicates::eval(Predicate::Intersects, &interval).option(),
            None
        );
        assert_eq!(
            matrix_predicates::eval(Predicate::Disjoint, &interval).option(),
            None
        );
        interval.witness(Loc::Interior, Loc::Interior, Dimension::Point);
        assert_eq!(
            matrix_predicates::eval(Predicate::Intersects, &interval).option(),
            Some(true)
        );
        assert_eq!(
            matrix_predicates::eval(Predicate::Disjoint, &interval).option(),
            Some(false)
        );
        // Touches needs II empty, and II is now witnessed: decided false at
        // once, before anything else settles.
        assert_eq!(
            matrix_predicates::eval(Predicate::Touches, &interval).option(),
            Some(false)
        );

        let mut interval = De9imInterval::open(LINE, LINE);
        for row in [Loc::Interior, Loc::Boundary] {
            for col in [Loc::Interior, Loc::Boundary] {
                interval.settle(row, col);
            }
        }
        assert_eq!(
            matrix_predicates::eval(Predicate::Disjoint, &interval).option(),
            Some(true)
        );
        // The exterior cells are still open, so effective emptiness and the
        // empty-equals-empty case are not decided yet.
        assert_eq!(
            matrix_predicates::eval(Predicate::Equals, &interval).option(),
            None,
            "II alone does not settle whether both operands are empty"
        );
        assert!(!interval.is_decided());
    }

    #[test]
    fn crosses_between_lines_waits_for_the_interior_dimension_to_settle() {
        let mut interval = De9imInterval::open(LINE, LINE);
        interval.witness(Loc::Interior, Loc::Interior, Dimension::Point);
        // A point witnessed, but a collinear overlap could still lift II to 1.
        assert_eq!(
            matrix_predicates::eval(Predicate::Crosses, &interval).option(),
            None
        );
        interval.settle(Loc::Interior, Loc::Interior);
        interval.witness(Loc::Interior, Loc::Exterior, Dimension::Curve);
        interval.witness(Loc::Exterior, Loc::Interior, Dimension::Curve);
        assert_eq!(
            matrix_predicates::eval(Predicate::Crosses, &interval).option(),
            Some(true)
        );
        let mut overlap = De9imInterval::open(LINE, LINE);
        overlap.witness(Loc::Interior, Loc::Interior, Dimension::Curve);
        assert_eq!(
            matrix_predicates::eval(Predicate::Crosses, &overlap).option(),
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
