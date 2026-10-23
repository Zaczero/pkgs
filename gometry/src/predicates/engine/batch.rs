use super::{
    FixedPredicate, GeographicPrepared, OnceCell, PredicateSpec, PreparedPlan, PyGeometryArray,
    ShapeRow, TopologyDomain,
};

/// Evaluate one fixed operand against storage rows, preserving missing rows and
/// logical error indices. The fixed evaluator also serves spatial-index refinement.
pub(crate) fn scalar_vs_shapes<'a>(
    spec: &PredicateSpec,
    scalar: &PreparedPlan,
    elements: impl ExactSizeIterator<Item = (usize, Option<ShapeRow<'a>>)>,
    scalar_is_left: bool,
    array: Option<&PyGeometryArray>,
    domain: TopologyDomain,
    scalar_cache: &crate::geometry::FrameDependentCaches,
) -> Result<Vec<bool>, (usize, crate::error::Error)> {
    let evaluator = FixedPredicate::new(spec, scalar, scalar_is_left);
    let element_count = elements.len();
    let element_bounds = array.and_then(PyGeometryArray::existing_element_bounds);
    let mut row_operation = array.map(PyGeometryArray::row_operation);
    let scalar_prepared = OnceCell::new();
    let mut results = Vec::with_capacity(element_count);
    for (index, row) in elements {
        let mut evaluate = || -> crate::error::Result<bool> {
            if array.is_some_and(|array| array.is_row_missing(index)) {
                return Ok(false);
            }
            let Some(row) = row else { return Ok(false) };
            match domain {
                TopologyDomain::Cartesian => evaluator.cartesian(
                    row,
                    element_bounds
                        .as_ref()
                        .and_then(|bounds| bounds.get(index).copied().flatten()),
                    element_count,
                    || {
                        row_operation.as_mut().map_or_else(
                            || crate::array::PreparedRow::transient(row),
                            |operation| operation.prepared_at(index, row),
                        )
                    },
                ),
                TopologyDomain::Geographic(frame) => {
                    if let Some(verdict) = evaluator.kind_gate(row) {
                        return Ok(verdict);
                    }
                    let element = row_operation.as_mut().map_or_else(
                        || crate::array::PreparedRow::transient(row),
                        |operation| operation.prepared_at(index, row),
                    )?;
                    let row_cache = row_operation
                        .as_mut()
                        .map(|operation| operation.frame_cache(index));
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
                    let element = GeographicPrepared::try_new(&element, element_cache, frame)?;
                    let scalar = *scalar_prepared.get_or_try_init(|| {
                        GeographicPrepared::try_new(scalar, scalar_cache, frame)
                    })?;
                    evaluator.geographic(scalar, element, element_count)
                },
            }
        };
        results.push(evaluate().map_err(|error| (index, error))?);
    }
    Ok(results)
}
