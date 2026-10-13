use super::{
    CertifiedQEmission, CoordinateAxes, EvaluationBudget, EvaluationOnly, ExactGeographicTopology,
    ExpansionBudget, OverlayOp, QCoordinates, QEmissionPlan, Result, SealedQDrawing, Shape,
    SourceArcMetricAccess, resolve_q_embedding, source_point_coordinates_with_work,
};
use crate::geometry::publication::PendingPublicationRow;

pub(super) fn exact_geographic_overlay<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    operation: OverlayOp,
    declared_axes: CoordinateAxes,
    strict: bool,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Shape> {
    let (embedding, _budget) =
        certify_geographic_overlay(topology, operation, declared_axes, strict, metric_access)?;
    super::publish_q_overlay(embedding)
}

pub(super) fn exact_geographic_overlay_pending_row<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    operation: OverlayOp,
    declared_axes: CoordinateAxes,
    strict: bool,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    logical_row: usize,
) -> Result<PendingPublicationRow> {
    let (embedding, budget) =
        certify_geographic_overlay(topology, operation, declared_axes, strict, metric_access)?;
    pending_q_array_row(
        embedding.sealed,
        logical_row,
        budget,
        topology.publication_frame(),
    )
}

fn certify_geographic_overlay<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    operation: OverlayOp,
    declared_axes: CoordinateAxes,
    strict: bool,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<(CertifiedQEmission, EvaluationBudget)> {
    let mut budget = EvaluationBudget::new(operation.name(), 0)?;
    let embedding = {
        let mut preparation_work = EvaluationOnly::new(&mut budget);
        let mut expansion_budget = ExpansionBudget::new(operation.name(), "computed edges");
        let plan = QEmissionPlan::build(topology, operation, &mut preparation_work)?;
        let nearest = QCoordinates::build_with_work(topology, &mut preparation_work)?;
        let source_points = source_point_coordinates_with_work(topology, &mut preparation_work)?;
        resolve_q_embedding(
            topology,
            &plan,
            &source_points,
            &nearest,
            declared_axes,
            strict,
            operation.name(),
            metric_access,
            &mut expansion_budget,
            &mut preparation_work,
        )?
    };
    Ok((embedding, budget))
}

fn pending_q_array_row(
    sealed: SealedQDrawing,
    logical_row: usize,
    work: EvaluationBudget,
    output_frame: &crate::Frame,
) -> Result<PendingPublicationRow> {
    match sealed {
        SealedQDrawing::Native(native) => {
            native
                .sealed
                .into_pending_row(logical_row, work, output_frame)
        },
        SealedQDrawing::Source(source) => {
            source
                .sealed
                .into_pending_row(logical_row, work, output_frame)
        },
    }
}
