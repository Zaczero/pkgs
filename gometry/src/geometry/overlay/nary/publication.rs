use super::{GeographicInputs, OverlayOp, Strictness, SourceArcContext, Result, Shape, Frame, Borrow, GeographicFrame, SourceArcMetricAccess, GeometryErrorKind, overlay_view_error, TopologyDomain, geographic_rectangle_operation_result, GeographicTopologyRoute, exact_partition_overlay_with_bounds_certificates, ExactPartitionBudget, overlay_declared_axes, Dimension, exact_puntal_nary_overlay, overlay_bounds_facts_geographic, intersection_bounds_empty, empty_nary_overlay_shape};

pub(in crate::geometry) fn overlay_all_geographic_from_with_metric<
    M: crate::geometry::ChartMetric,
>(
    originals: &GeographicInputs<'_>,
    op: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
    context: &SourceArcContext<'_, '_, M>,
) -> Result<Shape> {
    scalar_shape(overlay_all_geographic_output(
        originals,
        op,
        strictness,
        bounds_certificates,
        context,
        None,
        None,
    )?)
}

pub(in crate::geometry) fn overlay_all_geographic_from_with_output_frame<
    M: crate::geometry::ChartMetric,
>(
    originals: &GeographicInputs<'_>,
    op: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
    context: &SourceArcContext<'_, '_, M>,
    output_frame: &Frame,
) -> Result<Shape> {
    scalar_shape(overlay_all_geographic_output(
        originals,
        op,
        strictness,
        bounds_certificates,
        context,
        Some(output_frame),
        None,
    )?)
}

pub(crate) fn overlay_all_geographic_output_with_metric_and_frame<
    S: Borrow<Shape>,
    M: crate::geometry::ChartMetric,
>(
    geometries: &[S],
    frame: GeographicFrame,
    output_frame: &Frame,
    op: OverlayOp,
    strictness: Strictness,
    metric_access: &SourceArcMetricAccess<'_, '_, M>,
    logical_row: usize,
) -> Result<crate::geometry::publication::GeometryOutput> {
    if geometries.is_empty() {
        return Err(GeometryErrorKind::EmptyGeometrySequence {
            operation: op.name(),
        }
        .into());
    }
    let originals = GeographicInputs::try_from_shapes(frame, geometries.iter().map(Borrow::borrow))
        .map_err(|error| overlay_view_error(&error))?;
    let context = SourceArcContext::new(TopologyDomain::Geographic(frame), Some(metric_access));
    overlay_all_geographic_output(
        &originals,
        op,
        strictness,
        true,
        &context,
        Some(output_frame),
        Some(logical_row),
    )
}

fn scalar_shape(output: crate::geometry::publication::GeometryOutput) -> Result<Shape> {
    match output {
        crate::geometry::publication::GeometryOutput::Shape(shape) => Ok(shape),
        crate::geometry::publication::GeometryOutput::Pending(_) => {
            Err(GeometryErrorKind::Invalid(
                "scalar geographic overlay produced a pending array row".into(),
            )
            .into())
        },
    }
}

fn overlay_all_geographic_output<M: crate::geometry::ChartMetric>(
    originals: &GeographicInputs<'_>,
    op: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
    context: &SourceArcContext<'_, '_, M>,
    output_frame: Option<&Frame>,
    logical_row: Option<usize>,
) -> Result<crate::geometry::publication::GeometryOutput> {
    let frame = match context.domain() {
        TopologyDomain::Geographic(frame) => frame,
        TopologyDomain::Cartesian => {
            return Err(GeometryErrorKind::Invalid(
                "geographic overlay received a Cartesian source context".into(),
            )
            .into());
        },
    };
    if frame != originals.frame() {
        return Err(GeometryErrorKind::Invalid(
            "geographic overlay source context does not match its input frame".into(),
        )
        .into());
    }
    let mut original_refs = Vec::with_capacity(originals.views().len());
    original_refs.extend(originals.views().iter().map(|view| view.shape()));
    if let Some(shape) = geographic_rectangle_operation_result(originals, &original_refs, op) {
        return Ok(crate::geometry::publication::GeometryOutput::Shape(shape));
    }
    if crate::geometry::geographic_topology_route(originals.views().iter().copied())
        == GeographicTopologyRoute::ExactPartition
    {
        return Ok(crate::geometry::publication::GeometryOutput::Shape(
            exact_partition_overlay_with_bounds_certificates::<M>(
                original_refs,
                op,
                strictness,
                bounds_certificates,
                ExactPartitionBudget::Charge,
                context,
            )?,
        ));
    }
    exact_geographic_q_overlay(
        originals,
        &original_refs,
        op,
        strictness,
        bounds_certificates,
        context,
        output_frame,
        logical_row,
    )
}

#[cfg(test)]
pub(super) fn exact_geographic_q_from(
    originals: &GeographicInputs<'_>,
    op: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
) -> Result<Shape> {
    let mut original_refs = Vec::with_capacity(originals.views().len());
    original_refs.extend(originals.views().iter().map(|view| view.shape()));
    scalar_shape(exact_geographic_q_overlay::<crate::crs::GeodesicMetric>(
        originals,
        &original_refs,
        op,
        strictness,
        bounds_certificates,
        &SourceArcContext::new(TopologyDomain::Geographic(originals.frame()), None),
        None,
        None,
    )?)
}

fn exact_geographic_q_overlay<M: crate::geometry::ChartMetric>(
    originals: &GeographicInputs<'_>,
    original_refs: &[&Shape],
    op: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
    context: &SourceArcContext<'_, '_, M>,
    output_frame: Option<&Frame>,
    logical_row: Option<usize>,
) -> Result<crate::geometry::publication::GeometryOutput> {
    let frame = match context.domain() {
        TopologyDomain::Geographic(frame) => frame,
        TopologyDomain::Cartesian => {
            return Err(GeometryErrorKind::Invalid(
                "geographic Q overlay received a Cartesian source context".into(),
            )
            .into());
        },
    };
    let declared_axes = overlay_declared_axes(original_refs, op);
    if matches!(op, OverlayOp::Union | OverlayOp::SymmetricDifference)
        && original_refs
            .iter()
            .all(|shape| shape.topological_dimension() == Dimension::Point)
        && let Some(shape) = exact_puntal_nary_overlay(original_refs, op, strictness, context)?
    {
        return Ok(crate::geometry::publication::GeometryOutput::Shape(shape));
    }

    if bounds_certificates && op == OverlayOp::Intersection && originals.views().len() > 1 {
        let facts = overlay_bounds_facts_geographic(originals);
        if facts.raw_coordinate_bounds_sound && intersection_bounds_empty(&facts.bounds) {
            return Ok(crate::geometry::publication::GeometryOutput::Shape(
                empty_nary_overlay_shape(original_refs, op),
            ));
        }
    }

    let mut views = originals.views().to_vec();
    if op == OverlayOp::Difference {
        let (_, rest) = views
            .split_first_mut()
            .expect("geographic overlays have at least one operand");
        rest.sort_unstable_by(|left, right| {
            crate::geometry::compare_shapes(left.shape(), right.shape())
        });
    } else {
        views.sort_unstable_by(|left, right| {
            crate::geometry::compare_shapes(left.shape(), right.shape())
        });
    }
    let topology = if let Some(output_frame) = output_frame {
        crate::geometry::ExactGeographicTopology::from_views_as_set_with_publication_frame(
            frame,
            output_frame.clone(),
            views.into_iter(),
        )?
    } else {
        crate::geometry::ExactGeographicTopology::from_views_as_set(frame, views.into_iter())?
    };
    if let Some(logical_row) = logical_row {
        let mut output = crate::geometry::publication::GeometryOutput::Pending(
            crate::geometry::topology::overlay::exact_geographic_overlay_pending_row(
                &topology,
                op,
                declared_axes,
                strictness.is_strict(),
                context.metric_access(),
                logical_row,
            )?,
        );
        if let crate::geometry::publication::GeometryOutput::Pending(pending) = &mut output
            && pending.is_empty()
        {
            pending.replace_empty_shape(empty_nary_overlay_shape(original_refs, op))?;
        }
        Ok(output)
    } else {
        let output = crate::geometry::topology::overlay::exact_geographic_overlay(
            &topology,
            op,
            declared_axes,
            strictness.is_strict(),
            context.metric_access(),
        )?;
        Ok(crate::geometry::publication::GeometryOutput::Shape(
            if output.is_empty() {
                empty_nary_overlay_shape(original_refs, op)
            } else {
                output
            },
        ))
    }
}
