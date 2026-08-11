use crate::geometry::relate::De9im;
use crate::geometry::relate_ng::{
    BoundaryContacts, Operand, OperandPool, PointBatchTester, Polygon, StagedRings,
    classify_boundary_sections, probe_interior_faces, scan_boundary_contacts, topology,
};
pub(in crate::geometry) enum RelateGoal<'a> {
    Matrix,
    Pattern(CompiledPattern<'a>),
}

pub(in crate::geometry) enum RelateDecision {
    Matrix(De9im),
    Pattern(bool),
}

#[derive(Clone, Copy)]
pub(crate) struct CompiledPattern<'a> {
    pattern: &'a str,
    bytes: [u8; 9],
}

impl<'a> CompiledPattern<'a> {
    pub(crate) fn new(pattern: &'a str) -> Option<Self> {
        let bytes: [u8; 9] = pattern.as_bytes().try_into().ok()?;
        for byte in bytes.iter().copied() {
            match byte {
                b'T' | b'*' | b'F' | b'0' | b'1' | b'2' => {},
                _ => return None,
            }
        }
        Some(Self { pattern, bytes })
    }

    pub(crate) fn matches(self, matrix: De9im) -> bool {
        matrix
            .matches(self.pattern)
            .expect("compiled DE-9IM pattern is valid")
    }
}

pub(in crate::geometry) struct TopologyComputer<'a> {
    matrix: De9im,
    goal: RelateGoal<'a>,
    decided: Option<bool>,
}

impl<'a> TopologyComputer<'a> {
    pub(crate) const fn new(goal: RelateGoal<'a>) -> Self {
        let mut matrix = [b'F'; 9];
        matrix[8] = b'2';
        let mut computer = Self {
            matrix: De9im(matrix),
            goal,
            decided: None,
        };
        computer.check_pattern_cell(8);
        computer
    }

    pub(crate) const fn add_cell(&mut self, row: usize, col: usize, dim: u8) {
        let index = row * 3 + col;
        let value = b'0' + dim;
        let current = self.matrix.0[index];
        if current == b'F' || current < value {
            self.matrix.0[index] = value;
            self.check_pattern_cell(index);
        }
    }

    const fn check_pattern_cell(&mut self, index: usize) {
        let RelateGoal::Pattern(pattern) = self.goal else {
            return;
        };
        let value = self.matrix.0[index];
        match pattern.bytes[index] {
            b'F' if value != b'F' => self.decided = Some(false),
            b'0' | b'1' | b'2' if value > pattern.bytes[index] => self.decided = Some(false),
            _ => {},
        }
    }

    pub(crate) const fn decided(&self) -> bool {
        self.decided.is_some()
    }

    /// Whether every matrix cell an operand's boundary-section pass can still
    /// raise is already at its maximal value, so further section grading for
    /// that operand cannot change the matrix. Lets the per-section loop stop
    /// adding cells once a ring has contributed both its interior- and
    /// exterior-side runs — the dense-overlap fast exit.
    pub(crate) const fn boundary_cells_saturated(&self, operand: Operand) -> bool {
        let m = &self.matrix.0;
        match operand {
            // Left boundary touches (1,0)=`B I`, (0,0)=`I I`, (2,0)=`E I`,
            // (1,2)=`B E`, (0,2)=`I E`.
            Operand::Left => {
                m[3] == b'1' && m[0] == b'2' && m[6] == b'2' && m[5] == b'1' && m[2] == b'2'
            },
            // Right boundary touches (0,1)=`I B`, (0,0)=`I I`, (0,2)=`I E`,
            // (2,1)=`E B`, (2,0)=`E I`.
            Operand::Right => {
                m[1] == b'1' && m[0] == b'2' && m[2] == b'2' && m[7] == b'1' && m[6] == b'2'
            },
        }
    }

    pub(crate) fn finish(self) -> RelateDecision {
        match self.goal {
            RelateGoal::Matrix => RelateDecision::Matrix(self.matrix),
            RelateGoal::Pattern(pattern) => RelateDecision::Pattern(
                self.decided.unwrap_or_else(|| pattern.matches(self.matrix)),
            ),
        }
    }
}

/// Optional prepared point testers for each operand — the hierarchical
/// [`PointBatchTester`] cached on `ShapeData`. When present, areal point
/// membership is a Y-stabbing lookup; when absent (raw `Shape` relate of a
/// temporary), the functions fall back to the raw per-point ring scans. Both
/// produce identical classes for the valid operands the NG path accepts, so
/// the matrix is byte-identical.
#[derive(Clone, Copy, Default)]
pub(in crate::geometry) struct AreaTesters<'a> {
    pub(in crate::geometry) left: Option<&'a PointBatchTester>,
    pub(in crate::geometry) right: Option<&'a PointBatchTester>,
}

impl<'a> AreaTesters<'a> {
    pub(crate) const fn for_operand(self, operand: Operand) -> Option<&'a PointBatchTester> {
        match operand {
            Operand::Left => self.left,
            Operand::Right => self.right,
        }
    }
}

pub(in crate::geometry) fn areal_relate_ng(
    left: &[Polygon],
    right: &[Polygon],
    goal: RelateGoal<'_>,
    testers: AreaTesters<'_>,
) -> Option<RelateDecision> {
    // Raw-`Shape` path: no cached validity, so the same-operand self-
    // intersection gate must run.
    areal_relate_pool(
        &topology::build_operand_pool(left, right),
        goal,
        testers,
        false,
    )
}

/// Areal relate from pre-staged operand rings (the `ShapeData` cache), so a
/// repeated relate on the same geometry skips re-orienting every ring.
/// `both_simple` is the cached `is_simple` verdict of both operands: when set,
/// the broad phase skips the within-operand self-intersection gate.
pub(in crate::geometry) fn areal_relate_ng_staged(
    left: &StagedRings,
    right: &StagedRings,
    goal: RelateGoal<'_>,
    testers: AreaTesters<'_>,
    both_simple: bool,
) -> Option<RelateDecision> {
    areal_relate_pool(
        &topology::build_operand_pool_staged(left, right),
        goal,
        testers,
        both_simple,
    )
}

pub(crate) fn areal_relate_pool(
    pool: &OperandPool,
    goal: RelateGoal<'_>,
    testers: AreaTesters<'_>,
    both_simple: bool,
) -> Option<RelateDecision> {
    if pool.split == 0 || pool.split == pool.segments.len() {
        return None;
    }

    let mut computer = TopologyComputer::new(goal);
    let mut contacts = BoundaryContacts::new(pool.segments.len());
    if !scan_boundary_contacts(pool, &mut contacts, &mut computer, both_simple) {
        return None;
    }
    if computer.decided() {
        return Some(computer.finish());
    }

    if !classify_boundary_sections(Operand::Left, pool, &mut contacts, &mut computer, testers) {
        return None;
    }
    if computer.decided() {
        return Some(computer.finish());
    }
    if !classify_boundary_sections(Operand::Right, pool, &mut contacts, &mut computer, testers) {
        return None;
    }
    if computer.decided() {
        return Some(computer.finish());
    }
    if !probe_interior_faces(pool, &mut computer, testers) {
        return None;
    }
    Some(computer.finish())
}
