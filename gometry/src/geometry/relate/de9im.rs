use crate::geometry::{CompiledPattern, Dimension};

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
        let value = b'0' + dim.code();
        if self.0[index] == b'F' || self.0[index] < value {
            self.0[index] = value;
        }
    }

    /// Force one cell empty (`F`).
    ///
    /// Named access so consumers stop poking `matrix.0[3] = b'F'` directly:
    /// the ASCII `[u8; 9]` layout is an implementation detail, and a raw index
    /// gives no hint whether `3` is `(B,I)` or `(I,B)`. Every external writer
    /// now goes through here or [`Self::set_at_least`].
    pub(crate) const fn clear(&mut self, row: Loc, col: Loc) {
        self.0[row as usize * 3 + col as usize] = b'F';
    }

    pub(crate) fn left_dimension(self) -> Option<Dimension> {
        [self.0[0], self.0[1], self.0[2]]
            .into_iter()
            .filter(|&entry| entry != b'F')
            .filter_map(dimension_from_de9im_entry)
            .max()
    }

    pub(crate) fn right_dimension(self) -> Option<Dimension> {
        [self.0[0], self.0[3], self.0[6]]
            .into_iter()
            .filter(|&entry| entry != b'F')
            .filter_map(dimension_from_de9im_entry)
            .max()
    }

    pub(crate) fn text(self) -> String {
        String::from_utf8_lossy(&self.0).into_owned()
    }

    /// OGC pattern match on an ingress-validated pattern: `T` = non-empty,
    /// `F` = empty, digits exact, `*` = anything.
    pub(crate) fn matches(self, pattern: CompiledPattern) -> bool {
        let mut verdict = true;
        for (&entry, expected) in self.0.iter().zip(pattern.cells()) {
            verdict &= match expected {
                b'T' => entry != b'F',
                b'F' => entry == b'F',
                b'0' | b'1' | b'2' => entry == expected,
                b'*' => true,
                _ => unreachable!("compiled DE-9IM pattern is validated"),
            };
        }
        verdict
    }

    pub(crate) const fn is_touches(self) -> bool {
        // Contact without interior overlap: any boundary entry non-empty.
        self.0[0] == b'F' && (self.0[1] != b'F' || self.0[3] != b'F' || self.0[4] != b'F')
    }

    pub(crate) const fn is_intersects(self) -> bool {
        self.0[0] != b'F' || self.0[1] != b'F' || self.0[3] != b'F' || self.0[4] != b'F'
    }

    pub(crate) const fn is_disjoint(self) -> bool {
        !self.is_intersects()
    }

    pub(crate) fn is_crosses_by_dimension(self) -> bool {
        let Some(left) = self.left_dimension() else {
            return false;
        };
        let Some(right) = self.right_dimension() else {
            return false;
        };
        match left.cmp(&right) {
            std::cmp::Ordering::Less => self.0[0] != b'F' && self.0[2] != b'F',
            std::cmp::Ordering::Greater => self.0[0] != b'F' && self.0[6] != b'F',
            std::cmp::Ordering::Equal => left == Dimension::Curve && self.0[0] == b'0',
        }
    }

    pub(crate) fn is_overlaps_by_dimension(self) -> bool {
        let (Some(left), Some(right)) = (self.left_dimension(), self.right_dimension()) else {
            return false;
        };
        if left != right {
            return false;
        }
        match left {
            Dimension::Point | Dimension::Surface => {
                self.0[0] != b'F' && self.0[2] != b'F' && self.0[6] != b'F'
            },
            Dimension::Curve => self.0[0] == b'1' && self.0[2] != b'F' && self.0[6] != b'F',
        }
    }

    /// Rows and columns swapped — `relate(B, A)` from `relate(A, B)`.
    pub(crate) const fn transpose(self) -> Self {
        let m = self.0;
        Self([m[0], m[3], m[6], m[1], m[4], m[7], m[2], m[5], m[8]])
    }

    pub(crate) const fn is_contains(self) -> bool {
        // `T*****FF*`.
        self.0[0] != b'F' && self.0[6] == b'F' && self.0[7] == b'F'
    }

    pub(crate) const fn is_covers(self) -> bool {
        // `T*****FF* | *T****FF* | ***T**FF* | ****T*FF*`.
        (self.0[0] != b'F' || self.0[1] != b'F' || self.0[3] != b'F' || self.0[4] != b'F')
            && self.0[6] == b'F'
            && self.0[7] == b'F'
    }

    pub(crate) const fn is_contains_properly(self) -> bool {
        // `T**FF*FF*`.
        self.0[0] != b'F'
            && self.0[3] == b'F'
            && self.0[4] == b'F'
            && self.0[6] == b'F'
            && self.0[7] == b'F'
    }

    pub(crate) fn is_equal_topo(self) -> bool {
        if self == Self::empty_disjoint() {
            return true;
        }
        // `T*F**FFF*`.
        self.0[0] != b'F'
            && self.0[2] == b'F'
            && self.0[5] == b'F'
            && self.0[6] == b'F'
            && self.0[7] == b'F'
    }
}

const fn dimension_from_de9im_entry(entry: u8) -> Option<Dimension> {
    match entry {
        b'0' => Some(Dimension::Point),
        b'1' => Some(Dimension::Curve),
        b'2' => Some(Dimension::Surface),
        _ => None,
    }
}
