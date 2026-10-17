use super::{
    Bounds, GateOperand, GeographicPrepared, GeographicTopologyRoute, OnceCell, PointMembership,
    Predicate, PredicateSpec, PreparedPlan, PreparedTopologyPair, ShapeRow, row_point_intersection,
    topology_pair_after_gates,
};

/// Predicate evaluation against one fixed operand. Callers own row selection,
/// storage products and result publication; this owner retains query facts.
pub(crate) struct FixedPredicate<'a> {
    spec: &'a PredicateSpec,
    scalar: &'a PreparedPlan,
    scalar_is_left: bool,
    gate: GateOperand<'a>,
    point_membership: Option<PointMembership>,
    point_queries: OnceCell<crate::geometry::PreparedPointQueries<'a>>,
    exact_topology: OnceCell<std::sync::Arc<crate::geometry::ExactGeographicTopology>>,
}

impl<'a> FixedPredicate<'a> {
    pub(crate) fn new(
        spec: &'a PredicateSpec,
        scalar: &'a PreparedPlan,
        scalar_is_left: bool,
    ) -> Self {
        Self {
            spec,
            scalar,
            scalar_is_left,
            gate: GateOperand::fixed(scalar.shape(), scalar.bounds()),
            point_membership: spec
                .predicate
                .point_membership(!scalar_is_left)
                .or_else(|| {
                    matches!(spec.predicate, Predicate::Intersects | Predicate::Disjoint)
                        .then_some(PointMembership::BoundaryInclusive)
                }),
            point_queries: OnceCell::new(),
            exact_topology: OnceCell::new(),
        }
    }

    pub(crate) fn kind_gate(&self, row: ShapeRow<'_>) -> Option<bool> {
        let (left, right) = self.ordered(self.gate.kind, row.kind());
        self.spec.predicate.kind_gate(left, right)
    }

    const fn ordered<T>(&self, scalar: T, element: T) -> (T, T) {
        if self.scalar_is_left {
            (scalar, element)
        } else {
            (element, scalar)
        }
    }

    pub(crate) fn cartesian<P: std::ops::Deref<Target = PreparedPlan>>(
        &self,
        row: ShapeRow<'_>,
        bounds: Option<Bounds>,
        element_count: usize,
        prepare: impl FnOnce() -> crate::error::Result<P>,
    ) -> crate::error::Result<bool> {
        let predicate = self.spec.predicate;
        if let Some(point) = row.point()
            && let Some(membership) = self.point_membership
        {
            if self
                .gate
                .bounds
                .is_none_or(|bounds| !bounds.contains_point(point.xy()))
            {
                return Ok(predicate == Predicate::Disjoint);
            }
            let queries = self
                .point_queries
                .get_or_try_init(|| self.scalar.prepare_point_queries(element_count))?;
            return Ok(queries.membership(point, membership)? != (predicate == Predicate::Disjoint));
        }
        self.cartesian_topology(row, bounds, prepare)
    }

    fn cartesian_topology<P: std::ops::Deref<Target = PreparedPlan>>(
        &self,
        row: ShapeRow<'_>,
        bounds: Option<Bounds>,
        prepare: impl FnOnce() -> crate::error::Result<P>,
    ) -> crate::error::Result<bool> {
        let predicate = self.spec.predicate;
        let element = GateOperand::cartesian(row, bounds);
        let (left, right) = self.ordered(&self.gate, &element);
        if let Some(verdict) = self.spec.pre_topology(left, right) {
            return Ok(verdict);
        }
        if predicate == Predicate::Intersects
            && self.gate.rectangle == Some(false)
            && let Some(bounds) = element.bounds
            && crate::geometry::is_axis_aligned_rectangle_row(row, bounds)
        {
            return crate::geometry::exact_intersects_bounds(self.scalar.shape(), bounds);
        }
        if matches!(predicate, Predicate::Intersects | Predicate::Disjoint) {
            if self.gate.kind == super::GeometryKind::Point
                && let Some(verdict) =
                    row_point_intersection(self.spec, ShapeRow::Shape(self.scalar.shape()), row)?
            {
                return Ok(verdict);
            }
        } else {
            return crate::geometry::evaluate_cartesian_prepared_row(
                self.scalar,
                row,
                self.scalar_is_left,
                predicate,
            );
        }
        let element = prepare()?;
        let (left, right) = self.ordered(self.scalar, &element);
        topology_pair_after_gates(self.spec, &PreparedTopologyPair::Cartesian { left, right })
    }

    pub(crate) fn geographic(
        &self,
        scalar: GeographicPrepared<'_>,
        element: GeographicPrepared<'_>,
    ) -> crate::error::Result<bool> {
        let scalar_gate = GateOperand {
            bounds_sound: scalar.bounds_sound(),
            ..self.gate
        };
        let element_gate = GateOperand::from_prepared(&element);
        let (left, right) = self.ordered(&scalar_gate, &element_gate);
        if let Some(verdict) = self.spec.pre_topology(left, right) {
            return Ok(verdict);
        }
        if self.spec.predicate == Predicate::Intersects
            && self.gate.rectangle == Some(false)
            && left.bounds_are_sound()
            && right.bounds_are_sound()
            && let Some(bounds) = element_gate.bounds
            && crate::geometry::is_axis_aligned_rectangle_row(element_gate.row, bounds)
        {
            if scalar.chart_route().combine(element.chart_route())
                == GeographicTopologyRoute::ExactPartition
            {
                return crate::geometry::exact_intersects_bounds(self.scalar.shape(), bounds);
            }
            if let Some(location) = self
                .exact_topology
                .get_or_try_init(|| scalar.exact_topology())?
                .classify_rectangle(bounds)?
            {
                return Ok(matches!(
                    location,
                    crate::geometry::ExactGeographicRectangleLocation::Interior
                        | crate::geometry::ExactGeographicRectangleLocation::Boundary
                ));
            }
        }
        let (left, right) = self.ordered(scalar, element);
        topology_pair_after_gates(
            self.spec,
            &PreparedTopologyPair::from_geographic(left, right),
        )
    }
}
