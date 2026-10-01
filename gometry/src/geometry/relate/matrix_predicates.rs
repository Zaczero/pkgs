use crate::geometry::relate::De9im;
use crate::geometry::{CompiledPattern, Dimension};
use crate::predicates::Predicate;

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
    const ALL: [Self; 9] = [
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

#[expect(
    clippy::indexing_slicing,
    reason = "the closed Cell enum maps only to indexes in the fixed nine-entry DE-9IM matrix"
)]
const fn matrix_entry(matrix: &[u8], cell: Cell) -> u8 {
    matrix[cell.index()]
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
        if Dim4::from_entry(matrix_entry(&self.0.0, cell)) >= Dim4::from_dimension(dimension) {
            Tri::True
        } else {
            Tri::False
        }
    }

    fn is_empty(&self, cell: Cell) -> Tri {
        if matrix_entry(&self.0.0, cell) == b'F' {
            Tri::True
        } else {
            Tri::False
        }
    }

    fn exactly(&self, cell: Cell, dimension: Dimension) -> Tri {
        if matrix_entry(&self.0.0, cell) == super::dimension_entry(dimension) {
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
            .map(|cell| Dim4::from_entry(matrix_entry(&self.0.0, cell)))
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
