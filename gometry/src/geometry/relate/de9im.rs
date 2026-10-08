use crate::geometry::relate::matrix_predicates::{self, Decided, Tri};
use crate::geometry::{CompiledPattern, Dimension};
use crate::predicates::Predicate;

/// (`II IB IE / BI BB BE / EI EB EE`, `A` rows × `B` columns).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct De9im(pub [u8; 9]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Loc {
    Interior = 0,
    Boundary = 1,
    Exterior = 2,
}

impl De9im {
    pub(crate) const fn empty_disjoint() -> Self {
        let mut matrix = [b'F'; 9];
        matrix[8] = b'2';
        Self(matrix)
    }

    pub(crate) const fn set_at_least(&mut self, row: Loc, col: Loc, dim: Dimension) {
        let index = row as usize * 3 + col as usize;
        let value = super::dimension_entry(dim);
        if self.0[index] == b'F' || self.0[index] < value {
            self.0[index] = value;
        }
    }

    pub(crate) fn text(self) -> String {
        String::from_utf8_lossy(&self.0).into_owned()
    }

    /// OGC pattern match on an ingress-validated pattern: `T` = non-empty,
    /// `F` = empty, digits exact, `*` = anything.
    pub(crate) fn matches(self, pattern: CompiledPattern) -> bool {
        matrix_predicates::eval_pattern(pattern, &Decided(self)) == Tri::True
    }

    pub(crate) fn is_touches(self) -> bool {
        matrix_predicates::decided(Predicate::Touches, self)
    }

    pub(crate) fn is_intersects(self) -> bool {
        matrix_predicates::decided(Predicate::Intersects, self)
    }

    pub(crate) fn is_disjoint(self) -> bool {
        matrix_predicates::decided(Predicate::Disjoint, self)
    }

    pub(crate) fn is_crosses_by_dimension(self) -> bool {
        matrix_predicates::decided(Predicate::Crosses, self)
    }

    pub(crate) fn is_overlaps_by_dimension(self) -> bool {
        matrix_predicates::decided(Predicate::Overlaps, self)
    }

    /// Rows and columns swapped — `relate(B, A)` from `relate(A, B)`.
    pub(crate) const fn transpose(self) -> Self {
        let m = self.0;
        Self([m[0], m[3], m[6], m[1], m[4], m[7], m[2], m[5], m[8]])
    }

    pub(crate) fn is_contains(self) -> bool {
        matrix_predicates::decided(Predicate::Contains, self)
    }

    pub(crate) fn is_covers(self) -> bool {
        matrix_predicates::decided(Predicate::Covers, self)
    }

    pub(crate) fn is_contains_properly(self) -> bool {
        matrix_predicates::decided(Predicate::ContainsProperly, self)
    }

    pub(crate) fn is_equal_topo(self) -> bool {
        matrix_predicates::decided(Predicate::Equals, self)
    }
}
