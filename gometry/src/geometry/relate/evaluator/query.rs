use super::model::Operand;
use super::{
    CellMask, CompiledPattern, De9im, De9imInterval, GeometryErrorKind, Loc, PointMembership,
    Predicate, Result, matrix_predicates,
};
use crate::geometry::Coordinates as _;

pub(super) const LOCATIONS: [Loc; 3] = [Loc::Interior, Loc::Boundary, Loc::Exterior];

pub(super) trait Query {
    type Output: Copy;

    fn verdict(&self, interval: &De9imInterval) -> Option<Self::Output>;
    fn finish(&self, interval: &De9imInterval) -> Result<Self::Output>;
    fn decided_matrix(&self, matrix: De9im) -> Self::Output;
    fn same_stored_geometry(&self, operand: &Operand<'_>) -> Result<Option<Self::Output>>;
    fn point_membership(
        &self,
        point_operand: matrix_predicates::Operand,
    ) -> Option<PointMembershipQuery<Self::Output>>;
    fn cells(&self) -> CellMask;
    fn needs_dimensions(&self) -> bool;
}

#[derive(Clone, Copy)]
pub(super) struct PointMembershipQuery<T> {
    pub(super) membership: PointMembership,
    pub(super) all_match: T,
    pub(super) some_miss: T,
}

impl<T: Copy> PointMembershipQuery<T> {
    pub(super) const fn result(self, all_points_match: bool) -> T {
        if all_points_match {
            self.all_match
        } else {
            self.some_miss
        }
    }
}

pub(super) struct MatrixQuery;

impl Query for MatrixQuery {
    type Output = De9im;

    fn verdict(&self, interval: &De9imInterval) -> Option<Self::Output> {
        interval.matrix()
    }

    fn finish(&self, interval: &De9imInterval) -> Result<Self::Output> {
        interval.matrix().ok_or_else(|| {
            GeometryErrorKind::Invalid("Cartesian relation left undecided DE-9IM cells".into())
                .into()
        })
    }

    fn decided_matrix(&self, matrix: De9im) -> Self::Output {
        matrix
    }

    fn same_stored_geometry(&self, _: &Operand<'_>) -> Result<Option<Self::Output>> {
        Ok(None)
    }

    fn point_membership(
        &self,
        _: matrix_predicates::Operand,
    ) -> Option<PointMembershipQuery<Self::Output>> {
        None
    }

    fn cells(&self) -> CellMask {
        CellMask::all()
    }

    fn needs_dimensions(&self) -> bool {
        false
    }
}

pub(super) struct PredicateQuery(pub(super) Predicate);

impl Query for PredicateQuery {
    type Output = bool;

    fn verdict(&self, interval: &De9imInterval) -> Option<Self::Output> {
        matrix_predicates::eval(self.0, interval).option()
    }

    fn finish(&self, interval: &De9imInterval) -> Result<Self::Output> {
        self.verdict(interval).ok_or_else(|| {
            GeometryErrorKind::Invalid("Cartesian predicate left undecided DE-9IM cells".into())
                .into()
        })
    }

    fn decided_matrix(&self, matrix: De9im) -> Self::Output {
        matrix_predicates::decided(self.0, matrix)
    }

    fn same_stored_geometry(&self, operand: &Operand<'_>) -> Result<Option<Self::Output>> {
        if self.0 == Predicate::Equals {
            return Ok(Some(true));
        }
        if !matches!(
            self.0,
            Predicate::Contains | Predicate::Within | Predicate::Covers | Predicate::CoveredBy
        ) {
            return Ok(None);
        }
        let Some(point) = operand
            .row
            .polygon_rings()
            .and_then(|mut rings| rings.next())
            .and_then(|ring| ring.first_coord())
        else {
            return Ok(None);
        };
        let location = operand.locate_stored(point)?;
        // A boundary point with occupied area certifies an interior elsewhere.
        // Empty or cancelled support cannot acquire reflexivity from identity.
        Ok((location.area || location.location == Loc::Interior).then_some(true))
    }

    fn point_membership(
        &self,
        point_operand: matrix_predicates::Operand,
    ) -> Option<PointMembershipQuery<Self::Output>> {
        let membership = self
            .0
            .point_membership(point_operand == matrix_predicates::Operand::Left)?;
        Some(PointMembershipQuery {
            membership,
            all_match: true,
            some_miss: false,
        })
    }

    fn cells(&self) -> CellMask {
        CellMask::for_predicate(self.0)
    }

    fn needs_dimensions(&self) -> bool {
        CellMask::needs_dimensions(self.0)
    }
}

pub(super) struct PatternQuery(pub(super) CompiledPattern);

impl Query for PatternQuery {
    type Output = bool;

    fn verdict(&self, interval: &De9imInterval) -> Option<Self::Output> {
        interval.pattern(self.0)
    }

    fn finish(&self, interval: &De9imInterval) -> Result<Self::Output> {
        self.verdict(interval).ok_or_else(|| {
            GeometryErrorKind::Invalid("Cartesian pattern left undecided DE-9IM cells".into())
                .into()
        })
    }

    fn decided_matrix(&self, matrix: De9im) -> Self::Output {
        matrix.matches(self.0)
    }

    fn same_stored_geometry(&self, _: &Operand<'_>) -> Result<Option<Self::Output>> {
        Ok(None)
    }

    fn point_membership(
        &self,
        _: matrix_predicates::Operand,
    ) -> Option<PointMembershipQuery<Self::Output>> {
        None
    }

    fn cells(&self) -> CellMask {
        CellMask::for_pattern(self.0)
    }

    fn needs_dimensions(&self) -> bool {
        self.0
            .cells()
            .into_iter()
            .any(|literal| matches!(literal, b'0' | b'1' | b'2'))
    }
}

#[cfg(test)]
pub(super) struct EndOnly<Q>(pub(super) Q);

#[cfg(test)]
impl<Q: Query> Query for EndOnly<Q> {
    type Output = Q::Output;

    fn verdict(&self, _: &De9imInterval) -> Option<Self::Output> {
        None
    }

    fn finish(&self, interval: &De9imInterval) -> Result<Self::Output> {
        self.0.finish(interval)
    }

    fn decided_matrix(&self, matrix: De9im) -> Self::Output {
        self.0.decided_matrix(matrix)
    }

    fn same_stored_geometry(&self, _: &Operand<'_>) -> Result<Option<Self::Output>> {
        Ok(None)
    }

    fn point_membership(
        &self,
        _: matrix_predicates::Operand,
    ) -> Option<PointMembershipQuery<Self::Output>> {
        None
    }

    fn cells(&self) -> CellMask {
        self.0.cells()
    }

    fn needs_dimensions(&self) -> bool {
        self.0.needs_dimensions()
    }
}
