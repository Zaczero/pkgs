use super::{
    GateOperand, GeographicPrepared, GeographicTopologyRoute, OnceCell, PointMembership, Predicate,
    PredicateSpec, PreparedPlan, PreparedTopologyPair, PyGeometryArray, ShapeRow, TopologyDomain,
    row_kind_empty, row_point_intersection, topology_pair_after_gates, topology_scalar_pair,
};

/// Evaluate `spec` between one fixed `scalar` operand and every row of
/// `elements`, preparing the scalar's frame view at most once.
///
/// `scalar_is_left` names the operand order: `true` evaluates
/// `scalar OP element`, `false` evaluates `element OP scalar`. This is the
/// single batch engine behind the free functions, the array methods,
/// and the spatial-index refine loop. Rows come straight
/// off the storage: `Mixed` rows keep their persistent handles (prepared
/// state accumulates), packed point rows never synthesize one.
pub(crate) fn scalar_vs_shapes<'a>(
    spec: &PredicateSpec,
    scalar: &PreparedPlan,
    elements: impl ExactSizeIterator<Item = (usize, Option<ShapeRow<'a>>)>,
    scalar_is_left: bool,
    array: Option<&PyGeometryArray>,
    domain: TopologyDomain,
    scalar_cache: &crate::geometry::FrameDependentCaches,
) -> Result<Vec<bool>, (usize, crate::error::Error)> {
    let scalar_bounds = scalar.bounds();
    let element_bounds = array.and_then(PyGeometryArray::existing_element_bounds);
    let row_is_missing = |index: usize| array.is_some_and(|array| array.is_row_missing(index));
    let mut row_operation = array.map(PyGeometryArray::row_operation);
    match domain {
        TopologyDomain::Cartesian => {
            let scalar_operand = GateOperand::fixed(scalar.shape(), scalar_bounds);
            let gate = |index: usize, row: ShapeRow<'_>| -> crate::error::Result<Option<bool>> {
                let row_operand = GateOperand::cartesian(
                    row,
                    element_bounds
                        .as_ref()
                        .and_then(|bounds| bounds.get(index).copied().flatten()),
                );
                let (left, right) = if scalar_is_left {
                    (scalar_operand, row_operand)
                } else {
                    (row_operand, scalar_operand)
                };
                if let Some(verdict) = spec.pre_topology(&left, &right)? {
                    return Ok(Some(verdict));
                }
                if spec.predicate != Predicate::Intersects
                    || !left.bounds_are_sound()
                    || !right.bounds_are_sound()
                {
                    return Ok(None);
                }
                row.try_with_shape(|shape| {
                    let Some(bounds) = shape.bounds() else {
                        return Ok(None);
                    };
                    if !crate::geometry::is_axis_aligned_rectangle(shape, bounds) {
                        return Ok(None);
                    }
                    Ok(Some(crate::geometry::exact_intersects_bounds(
                        scalar.shape(),
                        bounds,
                    )?))
                })
            };
            // Point rows probe the scalar once each: index it for a batch.
            let element_count = elements.len();
            let mut point_queries: Option<crate::geometry::PreparedPointQueries<'_>> = None;
            let point_membership = spec
                .predicate
                .point_membership(!scalar_is_left)
                .or_else(|| {
                    matches!(spec.predicate, Predicate::Intersects | Predicate::Disjoint)
                        .then_some(PointMembership::BoundaryInclusive)
                });
            let mut results = Vec::with_capacity(element_count);
            for (index, row) in elements {
                let mut evaluate = || -> crate::error::Result<bool> {
                    if row_is_missing(index) {
                        return Ok(false);
                    }
                    let Some(row) = row else { return Ok(false) };
                    if let Some(point) = row.point()
                        && let Some(membership) = point_membership
                    {
                        if scalar_bounds.is_none_or(|bounds| !bounds.contains_point(point.xy())) {
                            return Ok(spec.predicate == Predicate::Disjoint);
                        }
                        let queries = if let Some(queries) = point_queries {
                            queries
                        } else {
                            let queries = scalar.prepare_point_queries(element_count)?;
                            point_queries = Some(queries);
                            queries
                        };
                        let matches = queries.membership(point, membership)?;
                        return Ok(matches != (spec.predicate == Predicate::Disjoint));
                    }
                    if let Some(verdict) = gate(index, row)? {
                        return Ok(verdict);
                    }
                    if matches!(spec.predicate, Predicate::Intersects | Predicate::Disjoint) {
                        if let Some(verdict) =
                            row_point_intersection(spec, ShapeRow::Shape(scalar.shape()), row)?
                        {
                            return Ok(verdict);
                        }
                    } else {
                        return crate::geometry::evaluate_cartesian_prepared_row(
                            scalar,
                            row,
                            scalar_is_left,
                            spec.predicate,
                        );
                    }
                    let element = row_operation.as_mut().map_or_else(
                        || crate::array::PreparedRow::transient(row),
                        |operation| operation.prepared_at(index, row),
                    )?;
                    let operands = if scalar_is_left {
                        PreparedTopologyPair::Cartesian {
                            left: scalar,
                            right: &element,
                        }
                    } else {
                        PreparedTopologyPair::Cartesian {
                            left: &element,
                            right: scalar,
                        }
                    };
                    topology_pair_after_gates(spec, &operands)
                };
                let verdict = evaluate().map_err(|error| (index, error))?;
                results.push(verdict);
            }
            Ok(results)
        },
        TopologyDomain::Geographic(frame) => {
            let (scalar_kind, _) = row_kind_empty(ShapeRow::Shape(scalar.shape()));
            let scalar_prepared = OnceCell::new();
            let scalar_exact_topology = OnceCell::new();
            let mut results = Vec::with_capacity(elements.len());
            for (index, row) in elements {
                if row_is_missing(index) {
                    results.push(false);
                    continue;
                }
                let Some(row) = row else {
                    results.push(false);
                    continue;
                };
                let (row_kind, _) = row_kind_empty(row);
                let (left_kind, right_kind) = if scalar_is_left {
                    (scalar_kind, row_kind)
                } else {
                    (row_kind, scalar_kind)
                };
                if let Some(verdict) = spec.predicate.kind_gate(left_kind, right_kind) {
                    results.push(verdict);
                    continue;
                }
                let element = row_operation
                    .as_mut()
                    .map_or_else(
                        || crate::array::PreparedRow::transient(row),
                        |operation| operation.prepared_at(index, row),
                    )
                    .map_err(|error| (index, error))?;
                let row_cache = row_operation
                    .as_mut()
                    .map(|operation| operation.frame_cache(index));
                // An array-backed row already owns its frame products; only a
                // row without them pays for a transient set, which has to
                // outlive the branch that builds it.
                let transient_cache;
                #[expect(
                    clippy::option_if_let_else,
                    reason = "map_or_else cannot initialize the binding the borrow outlives"
                )]
                let element_cache = if let Some(cache) = row_cache.as_deref() {
                    cache
                } else {
                    transient_cache = crate::geometry::FrameDependentCaches::default();
                    &transient_cache
                };
                let element_prepared = GeographicPrepared::try_new(&element, element_cache, frame)
                    .map_err(|error| (index, error))?;
                let prepared = *scalar_prepared
                    .get_or_try_init(|| GeographicPrepared::try_new(scalar, scalar_cache, frame))
                    .map_err(|error| (index, error))?;
                let scalar_operand = GateOperand::from_prepared(&prepared);
                let row_operand = GateOperand::from_prepared(&element_prepared);
                let (left, right) = if scalar_is_left {
                    (scalar_operand, row_operand)
                } else {
                    (row_operand, scalar_operand)
                };
                if let Some(verdict) = spec
                    .pre_topology(&left, &right)
                    .map_err(|error| (index, error))?
                {
                    results.push(verdict);
                    continue;
                }
                if spec.predicate == Predicate::Intersects
                    && left.bounds_are_sound()
                    && right.bounds_are_sound()
                {
                    let rectangle = element_prepared.plan().shape();
                    if let Some(bounds) = rectangle.bounds()
                        && crate::geometry::is_axis_aligned_rectangle(rectangle, bounds)
                    {
                        if prepared
                            .chart_route()
                            .combine(element_prepared.chart_route())
                            == GeographicTopologyRoute::ExactPartition
                        {
                            results.push(
                                crate::geometry::exact_intersects_bounds(
                                    prepared.plan().shape(),
                                    bounds,
                                )
                                .map_err(|error| (index, error))?,
                            );
                            continue;
                        }
                        if let Some(location) = scalar_exact_topology
                            .get_or_try_init(|| prepared.exact_topology())
                            .map_err(|error| (index, error))?
                            .classify_rectangle(bounds)
                            .map_err(|error| (index, error))?
                        {
                            results.push(matches!(
                                location,
                                crate::geometry::ExactGeographicRectangleLocation::Interior
                                    | crate::geometry::ExactGeographicRectangleLocation::Boundary
                            ));
                            continue;
                        }
                    }
                }
                let operands = if scalar_is_left {
                    PreparedTopologyPair::from_geographic(prepared, element_prepared)
                } else {
                    PreparedTopologyPair::from_geographic(element_prepared, prepared)
                };
                let verdict =
                    topology_scalar_pair(spec, &operands).map_err(|error| (index, error))?;
                results.push(verdict);
            }
            Ok(results)
        },
    }
}
