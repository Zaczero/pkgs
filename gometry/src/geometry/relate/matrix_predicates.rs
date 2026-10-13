use super::de9im::Loc;
use crate::geometry::predicate_spec::Predicate;
use crate::geometry::relate::De9im;
use crate::geometry::{CompiledPattern, Dimension};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Tri {
    False,
    True,
    Unknown,
}

impl Tri {
    pub(crate) const fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::True, Self::True) => Self::True,
            _ => Self::Unknown,
        }
    }

    pub(crate) const fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::False, Self::False) => Self::False,
            _ => Self::Unknown,
        }
    }

    pub(crate) const fn not(self) -> Self {
        match self {
            Self::False => Self::True,
            Self::True => Self::False,
            Self::Unknown => Self::Unknown,
        }
    }

    pub(crate) const fn option(self) -> Option<bool> {
        match self {
            Self::False => Some(false),
            Self::True => Some(true),
            Self::Unknown => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Dim4 {
    Empty,
    Point,
    Curve,
    Surface,
}

impl Dim4 {
    fn from_entry(entry: u8) -> Self {
        match entry {
            b'F' => Self::Empty,
            b'0' => Self::Point,
            b'1' => Self::Curve,
            b'2' => Self::Surface,
            _ => unreachable!("DE-9IM matrices contain only F, 0, 1, or 2"),
        }
    }

    const fn from_dimension(dimension: Dimension) -> Self {
        match dimension {
            Dimension::Point => Self::Point,
            Dimension::Curve => Self::Curve,
            Dimension::Surface => Self::Surface,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DimRange {
    lower: Dim4,
    upper: Dim4,
}

impl DimRange {
    const fn exact(dimension: Dim4) -> Self {
        Self {
            lower: dimension,
            upper: dimension,
        }
    }

    pub(crate) fn from_bounds(lower: Option<Dimension>, upper: Option<Dimension>) -> Self {
        let lower = lower.map_or(Dim4::Empty, Dim4::from_dimension);
        let upper = upper.map_or(Dim4::Empty, Dim4::from_dimension);
        debug_assert!(lower <= upper);
        Self { lower, upper }
    }

    fn less_than(self, other: Self) -> Tri {
        if self.upper < other.lower {
            Tri::True
        } else if self.lower >= other.upper {
            Tri::False
        } else {
            Tri::Unknown
        }
    }

    fn greater_than(self, other: Self) -> Tri {
        other.less_than(self)
    }

    fn equals(self, other: Self) -> Tri {
        if self.lower == self.upper && other.lower == other.upper && self.lower == other.lower {
            Tri::True
        } else if self.upper < other.lower || other.upper < self.lower {
            Tri::False
        } else {
            Tri::Unknown
        }
    }

    fn exactly(self, dimension: Dimension) -> Tri {
        let wanted = Dim4::from_dimension(dimension);
        if self.lower == wanted && self.upper == wanted {
            Tri::True
        } else if self.lower > wanted || self.upper < wanted {
            Tri::False
        } else {
            Tri::Unknown
        }
    }

    fn defined(self) -> Tri {
        if self.upper == Dim4::Empty {
            Tri::False
        } else if self.lower > Dim4::Empty {
            Tri::True
        } else {
            Tri::Unknown
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Cell {
    Ii,
    Ib,
    Ie,
    Bi,
    Bb,
    Be,
    Ei,
    Eb,
    Ee,
}

impl Cell {
    pub(crate) const fn from_locations(row: Loc, col: Loc) -> Self {
        match (row, col) {
            (Loc::Interior, Loc::Interior) => Self::Ii,
            (Loc::Interior, Loc::Boundary) => Self::Ib,
            (Loc::Interior, Loc::Exterior) => Self::Ie,
            (Loc::Boundary, Loc::Interior) => Self::Bi,
            (Loc::Boundary, Loc::Boundary) => Self::Bb,
            (Loc::Boundary, Loc::Exterior) => Self::Be,
            (Loc::Exterior, Loc::Interior) => Self::Ei,
            (Loc::Exterior, Loc::Boundary) => Self::Eb,
            (Loc::Exterior, Loc::Exterior) => Self::Ee,
        }
    }

    pub(crate) const ALL: [Self; 9] = [
        Self::Ii,
        Self::Ib,
        Self::Ie,
        Self::Bi,
        Self::Bb,
        Self::Be,
        Self::Ei,
        Self::Eb,
        Self::Ee,
    ];

    pub(crate) const fn index(self) -> usize {
        match self {
            Self::Ii => 0,
            Self::Ib => 1,
            Self::Ie => 2,
            Self::Bi => 3,
            Self::Bb => 4,
            Self::Be => 5,
            Self::Ei => 6,
            Self::Eb => 7,
            Self::Ee => 8,
        }
    }
}

/// The matrix cells a query can inspect. Cell formulas and their read masks
/// share one definition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CellMask([bool; 9]);

impl CellMask {
    pub(crate) const fn all() -> Self {
        Self([true; 9])
    }

    pub(crate) const fn empty() -> Self {
        Self([false; 9])
    }

    pub(crate) const fn contains(self, cell: Cell) -> bool {
        *matrix_cell(&self.0, cell)
    }

    const fn with(mut self, cell: Cell) -> Self {
        *matrix_cell_mut(&mut self.0, cell) = true;
        self
    }

    pub(crate) const fn for_predicate(predicate: Predicate) -> Self {
        let ii = Cell::Ii;
        let ib = Cell::Ib;
        let ie = Cell::Ie;
        let bi = Cell::Bi;
        let bb = Cell::Bb;
        let be = Cell::Be;
        let ei = Cell::Ei;
        let eb = Cell::Eb;
        match predicate {
            Predicate::Intersects | Predicate::Disjoint | Predicate::Touches => {
                Self::empty().with(ii).with(ib).with(bi).with(bb)
            },
            Predicate::Contains => Self::empty().with(ii).with(ei).with(eb),
            Predicate::Within => Self::empty().with(ii).with(ie).with(be),
            Predicate::Covers => Self::empty()
                .with(ii)
                .with(ib)
                .with(bi)
                .with(bb)
                .with(ei)
                .with(eb),
            Predicate::CoveredBy => Self::empty()
                .with(ii)
                .with(ib)
                .with(bi)
                .with(bb)
                .with(ie)
                .with(be),
            Predicate::ContainsProperly => {
                Self::empty().with(ii).with(bi).with(bb).with(ei).with(eb)
            },
            Predicate::Equals => Self::all(),
            Predicate::Crosses | Predicate::Overlaps => {
                Self::empty().with(ii).with(ib).with(ie).with(bi).with(ei)
            },
        }
    }

    pub(crate) fn for_pattern(pattern: CompiledPattern) -> Self {
        let mut mask = Self::empty();
        for (cell, literal) in Cell::ALL.into_iter().zip(pattern.cells()) {
            if literal != b'*' {
                mask = mask.with(cell);
            }
        }
        mask
    }

    pub(crate) const fn needs_dimensions(predicate: Predicate) -> bool {
        matches!(predicate, Predicate::Crosses | Predicate::Overlaps)
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "the closed Cell enum maps only to one of the nine fixed matrix entries"
)]
pub(crate) const fn matrix_cell<T>(cells: &[T; 9], cell: Cell) -> &T {
    &cells[cell.index()]
}

#[expect(
    clippy::indexing_slicing,
    reason = "the closed Cell enum maps only to one of the nine fixed matrix entries"
)]
pub(crate) const fn matrix_cell_mut<T>(cells: &mut [T; 9], cell: Cell) -> &mut T {
    &mut cells[cell.index()]
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Operand {
    Left,
    Right,
}

pub(crate) trait Cells {
    fn at_least(&self, cell: Cell, dimension: Dimension) -> Tri;
    fn is_empty(&self, cell: Cell) -> Tri;
    fn exactly(&self, cell: Cell, dimension: Dimension) -> Tri;
    fn interior_dim(&self, operand: Operand) -> DimRange;
}

pub(crate) struct Decided(pub(crate) De9im);

impl Cells for Decided {
    fn at_least(&self, cell: Cell, dimension: Dimension) -> Tri {
        if Dim4::from_entry(*matrix_cell(&self.0.0, cell)) >= Dim4::from_dimension(dimension) {
            Tri::True
        } else {
            Tri::False
        }
    }

    fn is_empty(&self, cell: Cell) -> Tri {
        if *matrix_cell(&self.0.0, cell) == b'F' {
            Tri::True
        } else {
            Tri::False
        }
    }

    fn exactly(&self, cell: Cell, dimension: Dimension) -> Tri {
        if *matrix_cell(&self.0.0, cell) == super::dimension_entry(dimension) {
            Tri::True
        } else {
            Tri::False
        }
    }

    fn interior_dim(&self, operand: Operand) -> DimRange {
        let cells = match operand {
            Operand::Left => [Cell::Ii, Cell::Ib, Cell::Ie],
            Operand::Right => [Cell::Ii, Cell::Bi, Cell::Ei],
        };
        let dimension = cells
            .into_iter()
            .map(|cell| Dim4::from_entry(*matrix_cell(&self.0.0, cell)))
            .fold(Dim4::Empty, Ord::max);
        DimRange::exact(dimension)
    }
}

fn at_least(cells: &impl Cells, cell: Cell, dimension: Dimension) -> Tri {
    cells.at_least(cell, dimension)
}

fn is_empty(cells: &impl Cells, cell: Cell) -> Tri {
    cells.is_empty(cell)
}

fn all_empty(cells: &impl Cells) -> Tri {
    [
        Cell::Ii,
        Cell::Ib,
        Cell::Ie,
        Cell::Bi,
        Cell::Bb,
        Cell::Be,
        Cell::Ei,
        Cell::Eb,
    ]
    .into_iter()
    .map(|cell| is_empty(cells, cell))
    .fold(Tri::True, Tri::and)
}

fn intersects(cells: &impl Cells) -> Tri {
    [Cell::Ii, Cell::Ib, Cell::Bi, Cell::Bb]
        .into_iter()
        .map(|cell| at_least(cells, cell, Dimension::Point))
        .fold(Tri::False, Tri::or)
}

fn covers(cells: &impl Cells) -> Tri {
    intersects(cells)
        .and(is_empty(cells, Cell::Ei))
        .and(is_empty(cells, Cell::Eb))
}

pub(crate) fn eval(predicate: Predicate, cells: &impl Cells) -> Tri {
    match predicate {
        Predicate::Intersects => intersects(cells),
        Predicate::Disjoint => intersects(cells).not(),
        Predicate::Contains => at_least(cells, Cell::Ii, Dimension::Point)
            .and(is_empty(cells, Cell::Ei))
            .and(is_empty(cells, Cell::Eb)),
        Predicate::Within => at_least(cells, Cell::Ii, Dimension::Point)
            .and(is_empty(cells, Cell::Ie))
            .and(is_empty(cells, Cell::Be)),
        Predicate::Covers => covers(cells),
        Predicate::CoveredBy => intersects(cells)
            .and(is_empty(cells, Cell::Ie))
            .and(is_empty(cells, Cell::Be)),
        Predicate::ContainsProperly => at_least(cells, Cell::Ii, Dimension::Point)
            .and(is_empty(cells, Cell::Bi))
            .and(is_empty(cells, Cell::Bb))
            .and(is_empty(cells, Cell::Ei))
            .and(is_empty(cells, Cell::Eb)),
        Predicate::Touches => is_empty(cells, Cell::Ii).and(
            at_least(cells, Cell::Ib, Dimension::Point)
                .or(at_least(cells, Cell::Bi, Dimension::Point))
                .or(at_least(cells, Cell::Bb, Dimension::Point)),
        ),
        Predicate::Equals => all_empty(cells)
            .and(cells.exactly(Cell::Ee, Dimension::Surface))
            .or(at_least(cells, Cell::Ii, Dimension::Point)
                .and(is_empty(cells, Cell::Ie))
                .and(is_empty(cells, Cell::Be))
                .and(is_empty(cells, Cell::Ei))
                .and(is_empty(cells, Cell::Eb))),
        Predicate::Crosses => {
            let left = cells.interior_dim(Operand::Left);
            let right = cells.interior_dim(Operand::Right);
            left.less_than(right)
                .and(at_least(cells, Cell::Ii, Dimension::Point))
                .and(at_least(cells, Cell::Ie, Dimension::Point))
                .or(left
                    .greater_than(right)
                    .and(at_least(cells, Cell::Ii, Dimension::Point))
                    .and(at_least(cells, Cell::Ei, Dimension::Point)))
                .or(left
                    .exactly(Dimension::Curve)
                    .and(right.exactly(Dimension::Curve))
                    .and(cells.exactly(Cell::Ii, Dimension::Point)))
        },
        Predicate::Overlaps => {
            let left = cells.interior_dim(Operand::Left);
            let right = cells.interior_dim(Operand::Right);
            let same_defined_dimension =
                left.equals(right).and(left.defined()).and(right.defined());
            let curve = left
                .exactly(Dimension::Curve)
                .and(right.exactly(Dimension::Curve))
                .and(cells.exactly(Cell::Ii, Dimension::Curve))
                .and(at_least(cells, Cell::Ie, Dimension::Point))
                .and(at_least(cells, Cell::Ei, Dimension::Point));
            let point_or_surface = (left
                .exactly(Dimension::Point)
                .or(left.exactly(Dimension::Surface)))
            .and(
                at_least(cells, Cell::Ii, Dimension::Point)
                    .and(at_least(cells, Cell::Ie, Dimension::Point))
                    .and(at_least(cells, Cell::Ei, Dimension::Point)),
            );
            same_defined_dimension.and(curve.or(point_or_surface))
        },
    }
}

pub(crate) fn eval_pattern(pattern: CompiledPattern, cells: &impl Cells) -> Tri {
    let literals = pattern.cells();
    Cell::ALL
        .into_iter()
        .zip(literals)
        .map(|(cell, literal)| match literal {
            b'T' => at_least(cells, cell, Dimension::Point),
            b'F' => is_empty(cells, cell),
            b'0' => cells.exactly(cell, Dimension::Point),
            b'1' => cells.exactly(cell, Dimension::Curve),
            b'2' => cells.exactly(cell, Dimension::Surface),
            b'*' => Tri::True,
            _ => unreachable!("compiled DE-9IM pattern is validated"),
        })
        .fold(Tri::True, Tri::and)
}

pub(crate) fn decided(predicate: Predicate, matrix: De9im) -> bool {
    eval(predicate, &Decided(matrix)) == Tri::True
}

#[cfg(test)]
mod tests {
    use super::*;

    struct RecordingCells(std::cell::Cell<CellMask>);

    impl RecordingCells {
        fn new() -> Self {
            Self(std::cell::Cell::new(CellMask::empty()))
        }

        fn record(&self, cell: Cell) {
            self.0.set(self.0.get().with(cell));
        }
    }

    impl Cells for RecordingCells {
        fn at_least(&self, cell: Cell, _: Dimension) -> Tri {
            self.record(cell);
            Tri::Unknown
        }

        fn is_empty(&self, cell: Cell) -> Tri {
            self.record(cell);
            Tri::Unknown
        }

        fn exactly(&self, cell: Cell, _: Dimension) -> Tri {
            self.record(cell);
            Tri::Unknown
        }

        fn interior_dim(&self, operand: Operand) -> DimRange {
            let cells = match operand {
                Operand::Left => [Cell::Ii, Cell::Ib, Cell::Ie],
                Operand::Right => [Cell::Ii, Cell::Bi, Cell::Ei],
            };
            for cell in cells {
                self.record(cell);
            }
            DimRange::from_bounds(None, Some(Dimension::Surface))
        }
    }

    #[test]
    fn query_cell_masks_cover_every_cell_read_by_their_matrix_formula() {
        for predicate in Predicate::ALL {
            let cells = RecordingCells::new();
            let _ = eval(predicate, &cells);
            assert_eq!(
                cells.0.get(),
                CellMask::for_predicate(predicate),
                "{predicate:?}"
            );
        }

        let pattern = CompiledPattern::new("T*F***1**").expect("pattern is valid");
        let cells = RecordingCells::new();
        let _ = eval_pattern(pattern, &cells);
        assert_eq!(cells.0.get(), CellMask::for_pattern(pattern), "pattern");
    }
}
