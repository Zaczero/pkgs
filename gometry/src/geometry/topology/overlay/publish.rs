use super::super::emission::plan::QDiagnosticLine;
use super::{
    CoordinateAxes, EmittedVertex, EvaluationBudget, EvaluationOnly, ExactGeographicTopology,
    ExpansionBudget, GeometryErrorKind, OverlayOp, QCoordinates, QEmbeddingCertification,
    QEmissionPlan, QProducer, ResourceError, Result, SealedQDrawing, SealedSourceDrawing,
    SearchAccount, SearchOutcome, Shape, SourceArcMetricAccess, SourceRowId,
    boundary_source_points_with_work, certify_and_search_q_embedding, certify_nearest_ring_roles,
    finish_geographic, node_injectivity_collisions_with_work, node_ordinates,
    source_point_coordinates_with_work, write_completed_native,
};
use crate::geometry::embedding_completion::literals::LiteralAddress;
use crate::geometry::publication::{
    FinishedGeometry, PendingPublicationRow, ProducerReceipt, PublicationCall, PublicationOutcome,
    PublicationRejection, StorageTarget,
};

impl QEmissionPlan {
    pub(in crate::geometry::topology) fn build_pooled_linework<M: crate::geometry::ChartMetric>(
        topology: &ExactGeographicTopology,
        producer: QProducer,
        diagnostics: bool,
        strict: bool,
        declared_axes: CoordinateAxes,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
        retain_literals_for_republication: bool,
        work: &mut impl SearchAccount,
    ) -> Result<(FinishedGeometry, Vec<QDiagnosticLine>)> {
        if !producer.is_pooled_linework() {
            return Err(GeometryErrorKind::Invalid(
                "pooled linework emitter received a non-linework producer".into(),
            )
            .into());
        }
        let mut expansion_budget = ExpansionBudget::new(producer.operation(), "computed edges");
        let mut plan = match producer {
            QProducer::Node => Self::build_node(topology, work)?,
            QProducer::Polygonize => Self::build_polygonize(topology, diagnostics, work)?,
            QProducer::BuildArea => Self::build_area(topology, work)?,
            QProducer::Overlay | QProducer::Boundary => {
                return Err(GeometryErrorKind::Invalid(
                    "pooled linework emitter received a non-linework producer".into(),
                )
                .into());
            },
        };
        plan.retain_literals_for_republication = retain_literals_for_republication;
        let nearest = QCoordinates::build_with_work(topology, work)?;
        let source_points = source_point_coordinates_with_work(topology, work)?;
        let embedding = resolve_q_embedding(
            topology,
            &plan,
            &source_points,
            &nearest,
            declared_axes,
            strict,
            producer.operation(),
            metric_access,
            &mut expansion_budget,
            work,
        )?;
        Ok(into_finished_q_emission(embedding))
    }
}

pub(super) fn exact_geographic_overlay<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    operation: OverlayOp,
    declared_axes: CoordinateAxes,
    strict: bool,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Shape> {
    let (embedding, _budget) =
        certify_geographic_overlay(topology, operation, declared_axes, strict, metric_access)?;
    publish_q_overlay(embedding)
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

pub(super) fn exact_geographic_boundary<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    declared_axes: CoordinateAxes,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Shape> {
    let (embedding, _budget) = certify_geographic_boundary(topology, declared_axes, metric_access)?;
    publish_q_overlay(embedding)
}

pub(super) fn exact_geographic_boundary_pending_row<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    declared_axes: CoordinateAxes,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    logical_row: usize,
) -> Result<PendingPublicationRow> {
    let (embedding, budget) = certify_geographic_boundary(topology, declared_axes, metric_access)?;
    pending_q_array_row(
        embedding.sealed,
        logical_row,
        budget,
        topology.publication_frame(),
    )
}

fn certify_geographic_boundary<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    declared_axes: CoordinateAxes,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<(CertifiedQEmission, EvaluationBudget)> {
    if topology.sources.row_count != 1 {
        return Err(GeometryErrorKind::Invalid(
            "an exact geographic boundary requires one source row".into(),
        )
        .into());
    }
    let mut budget = EvaluationBudget::new("geographic boundary", 0)?;
    let embedding = {
        let mut preparation_work = EvaluationOnly::new(&mut budget);
        let mut expansion_budget = ExpansionBudget::new("geographic boundary", "computed edges");
        let (plan, _support) = QEmissionPlan::build_boundary(topology, &mut preparation_work)?;
        let source_points =
            boundary_source_points_with_work(topology, SourceRowId(0), &mut preparation_work)?;
        let nearest = QCoordinates::build_with_work(topology, &mut preparation_work)?;
        resolve_q_embedding(
            topology,
            &plan,
            &source_points,
            &nearest,
            declared_axes,
            false,
            "geographic boundary",
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

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry::topology) enum QEmbeddingChoice {
    Nearest,
    Searched,
}

pub(in crate::geometry::topology) struct CertifiedQEmission {
    pub(in crate::geometry::topology) sealed: SealedQDrawing,
    #[cfg(test)]
    pub(in crate::geometry::topology) choice: QEmbeddingChoice,
    #[cfg(test)]
    pub(in crate::geometry::topology) coordinates: QCoordinates,
}

pub(in crate::geometry::topology) fn write_certified_q_emission(
    embedding: CertifiedQEmission,
) -> Result<Shape> {
    match embedding.sealed {
        SealedQDrawing::Source(source) => Ok(source.sealed.into_scalar()?.0),
        SealedQDrawing::Native(native) => write_completed_native(native),
    }
}

fn into_finished_q_emission(
    embedding: CertifiedQEmission,
) -> (FinishedGeometry, Vec<QDiagnosticLine>) {
    match embedding.sealed {
        SealedQDrawing::Source(source) => (source.sealed, source.diagnostic_lines),
        SealedQDrawing::Native(native) => (native.sealed, native.diagnostic_lines),
    }
}

pub(in crate::geometry::topology) fn resolve_q_embedding<
    M: crate::geometry::ChartMetric,
    A: SearchAccount,
>(
    topology: &ExactGeographicTopology,
    plan: &QEmissionPlan,
    source_points: &[Option<EmittedVertex<'_>>],
    nearest: &QCoordinates,
    declared_axes: CoordinateAxes,
    strict: bool,
    operation: &'static str,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    expansion_budget: &mut ExpansionBudget,
    preparation_work: &mut A,
) -> Result<CertifiedQEmission> {
    let drawing = plan.drawing(topology, nearest, source_points, preparation_work)?;
    let mut live_node_keys = drawing.live_node_keys(topology.sources.frame, preparation_work)?;
    let identity_conflicts =
        node_injectivity_collisions_with_work(&mut live_node_keys, preparation_work)?;
    preparation_work.charge_into_error(
        u64::try_from(live_node_keys.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let has_rounded_constructed_node = nearest.has_inexact_nodes_with_work(
        topology,
        live_node_keys.iter().map(|(_, node)| *node),
        preparation_work,
    )?;
    let source_reuse =
        plan.source_reuse_is_proven(topology, &drawing, source_points, preparation_work)?;
    let needs_q_certificate = !identity_conflicts.is_empty()
        || has_rounded_constructed_node
        || plan.force_native
        || !source_reuse;
    #[cfg(test)]
    let mut choice = QEmbeddingChoice::Nearest;
    #[cfg(test)]
    let mut test_coordinates = nearest.clone();
    let sealed = if needs_q_certificate {
        let ordinates = node_ordinates(topology, declared_axes, metric_access, preparation_work)?;
        let published_inventory = plan.fragment_recipes.publish_with_expansion_budget(
            topology,
            nearest,
            expansion_budget,
            preparation_work,
        )?;
        // Each fragment contributes one base leaf; each inserted sample adds one more.
        let published_leaves = published_inventory
            .fragments
            .len()
            .checked_add(published_inventory.samples.len())
            .ok_or(ResourceError::SizeOverflow)?;
        preparation_work
            .budget_mut()
            .set_published_leaves(published_leaves)?;
        let emission = plan.embedding_emission(
            topology,
            source_points,
            &published_inventory,
            preparation_work,
        )?;
        let certification = QEmbeddingCertification {
            topology,
            emission: &emission,
            nearest,
            ordinates: &ordinates,
            declared_axes,
            strict_axes: strict,
            operation,
            metric_access,
        };
        let pass = match certify_and_search_q_embedding(
            &certification,
            &live_node_keys,
            drawing,
            preparation_work,
        )? {
            SearchOutcome::Pass(pass) => pass,
            SearchOutcome::Exhausted => {
                return Err(GeometryErrorKind::unrepresentable_output(operation));
            },
        };
        #[cfg(test)]
        {
            choice = if pass.searched {
                QEmbeddingChoice::Searched
            } else {
                QEmbeddingChoice::Nearest
            };
            test_coordinates = pass.coordinates.clone();
        }
        let output = pass.output;
        output.sealed
    } else {
        let ordinates = node_ordinates(topology, declared_axes, metric_access, preparation_work)?;
        let ring_roles = certify_nearest_ring_roles(topology, &drawing, preparation_work)?;
        if plan.producer.is_pooled_linework() {
            let (shape, layout, diagnostic_lines) = plan.write_source_drawing_parts(
                drawing,
                &ring_roles,
                &ordinates,
                None,
                None,
                declared_axes,
                strict,
                operation,
                preparation_work,
            )?;
            let receipt = super::producer_drawing::receipt_from_shape_layout(
                shape,
                layout,
                LiteralAddress {
                    operation: 0,
                    logical_row: 0,
                    group_row: None,
                    value_row: None,
                    output: 0,
                },
                plan.retain_literals_for_republication,
                preparation_work,
            )?;
            match finish_geographic(
                PublicationCall::new(
                    ProducerReceipt::ProducerDrawing(receipt),
                    topology.publication_frame().clone(),
                    preparation_work,
                ),
                StorageTarget::Scalar,
            )? {
                PublicationOutcome::Pass(finished) => SealedQDrawing::Source(SealedSourceDrawing {
                    sealed: finished,
                    diagnostic_lines,
                }),
                PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
                    return Err(GeometryErrorKind::Invalid(
                        format!(
                            "pooled Q source drawing readback changed {} literal images",
                            images.len()
                        )
                        .into(),
                    )
                    .into());
                },
                PublicationOutcome::Reject(_) => {
                    return Err(GeometryErrorKind::Invalid(
                        "pooled Q source finisher returned a different rejection family".into(),
                    )
                    .into());
                },
            }
        } else {
            let source = plan.seal_source_drawing(
                drawing,
                &ring_roles,
                &ordinates,
                None,
                None,
                declared_axes,
                strict,
                operation,
                preparation_work,
            )?;
            let diagnostic_lines = source.diagnostic_lines.clone();
            let receipt = super::super::q_embedding::publication::SourceOnlyPublication::new(
                source,
                topology,
                None,
                &ordinates,
                None,
                &[],
                &[],
                &[],
            );
            match finish_geographic(
                PublicationCall::new(
                    ProducerReceipt::QSourceOnly(receipt),
                    topology.publication_frame().clone(),
                    preparation_work,
                ),
                StorageTarget::Scalar,
            )? {
                PublicationOutcome::Pass(finished) => SealedQDrawing::Source(SealedSourceDrawing {
                    sealed: finished,
                    diagnostic_lines,
                }),
                PublicationOutcome::Reject(PublicationRejection::SourceOnly(_)) => {
                    return Err(GeometryErrorKind::Invalid(
                        "unchanged Q source output failed its literal binding".into(),
                    )
                    .into());
                },
                PublicationOutcome::Reject(_) => {
                    return Err(GeometryErrorKind::Invalid(
                        "Q source-only finisher returned a non-source rejection".into(),
                    )
                    .into());
                },
            }
        }
    };
    Ok(CertifiedQEmission {
        sealed,
        #[cfg(test)]
        choice,
        #[cfg(test)]
        coordinates: test_coordinates,
    })
}

pub(in crate::geometry::topology) fn publish_q_overlay(
    embedding: CertifiedQEmission,
) -> Result<Shape> {
    write_certified_q_emission(embedding)
}
