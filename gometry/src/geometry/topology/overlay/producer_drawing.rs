use std::borrow::Borrow;

use super::super::emission::plan::{QDiagnosticLine, QEmissionPlan, QProducer};
use super::super::q::ExactGeographicTopology;
use crate::error::{ResourceError, Result};
use crate::geographic::GeographicInputs;
use crate::geometry::embedding_search::account::{EvaluationBudget, EvaluationOnly};
use crate::geometry::ordinates::SourceArcContext;
use crate::geometry::publication::{
    PendingPublicationRow, ProducerDrawingReceipt, StorageTarget, finish_producer_drawing,
    geographic_drawing_pending_row,
};
use crate::geometry::{
    CoordinateAxes, GeometryErrorKind, LineSeq, PolygonizeFull, Shape, TopologyDomain,
};
use crate::work::Work;

pub(in crate::geometry) fn exact_geographic_linework_from_shapes<
    M: crate::geometry::ChartMetric,
>(
    shapes: &[impl Borrow<Shape>],
    producer: QProducer,
    diagnostics: bool,
    strict: bool,
    context: &SourceArcContext<'_, '_, M>,
    publication_frame: crate::Frame,
) -> Result<Shape> {
    let (shape, _) = exact_geographic_linework_drawing_from_shapes(
        shapes,
        producer,
        diagnostics,
        strict,
        context,
        publication_frame.clone(),
    )?;
    finish_producer_drawing(
        ProducerDrawingReceipt::drawing(shape)?,
        publication_frame,
        StorageTarget::Scalar,
    )?
    .into_scalar()
    .map(|(shape, _)| shape)
}

pub(in crate::geometry) fn exact_geographic_linework_pending_row<
    M: crate::geometry::ChartMetric,
>(
    shapes: &[impl Borrow<Shape>],
    producer: QProducer,
    diagnostics: bool,
    strict: bool,
    context: &SourceArcContext<'_, '_, M>,
    publication_frame: &crate::Frame,
    logical_row: usize,
) -> Result<PendingPublicationRow> {
    let (shape, _construction_work) = exact_geographic_linework_drawing_from_shapes(
        shapes,
        producer,
        diagnostics,
        strict,
        context,
        publication_frame.clone(),
    )?;
    geographic_drawing_pending_row(shape, logical_row, producer.operation())
}

pub(in crate::geometry) fn exact_geographic_linework_drawing_from_shapes<
    M: crate::geometry::ChartMetric,
>(
    shapes: &[impl Borrow<Shape>],
    producer: QProducer,
    diagnostics: bool,
    strict: bool,
    context: &SourceArcContext<'_, '_, M>,
    publication_frame: crate::Frame,
) -> Result<(Shape, EvaluationBudget)> {
    let (shape, budget, _) = exact_geographic_linework_drawing_with_diagnostics_from_shapes(
        shapes,
        producer,
        diagnostics,
        strict,
        context,
        publication_frame,
    )?;
    Ok((shape, budget))
}

fn exact_geographic_linework_drawing_with_diagnostics_from_shapes<
    M: crate::geometry::ChartMetric,
>(
    shapes: &[impl Borrow<Shape>],
    producer: QProducer,
    diagnostics: bool,
    strict: bool,
    context: &SourceArcContext<'_, '_, M>,
    publication_frame: crate::Frame,
) -> Result<(Shape, EvaluationBudget, Vec<QDiagnosticLine>)> {
    let TopologyDomain::Geographic(frame) = context.domain() else {
        return Err(GeometryErrorKind::Invalid(
            "exact Q linework requires a geographic source context".into(),
        )
        .into());
    };
    let inputs = GeographicInputs::try_from_shapes(frame, shapes.iter().map(Borrow::borrow))
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    let topology = ExactGeographicTopology::from_views_with_publication_frame(
        frame,
        publication_frame,
        inputs.views().iter().copied(),
    )?;
    let declared_axes = shapes.iter().fold(CoordinateAxes::XY, |axes, shape| {
        axes.union(shape.borrow().axes())
    });
    let mut budget = EvaluationBudget::new(producer.operation(), 0)?;
    let (shape, diagnostic_lines) = {
        let mut work = EvaluationOnly::new(&mut budget);
        QEmissionPlan::build_pooled_linework(
            &topology,
            producer,
            diagnostics,
            strict,
            declared_axes,
            context.metric_access(),
            &mut work,
        )?
    };
    Ok((shape, budget, diagnostic_lines))
}

pub(crate) fn exact_geographic_polygonize_full_from_shapes<M: crate::geometry::ChartMetric>(
    shapes: &[impl Borrow<Shape>],
    strict: bool,
    context: &SourceArcContext<'_, '_, M>,
) -> Result<PolygonizeFull> {
    let (shape, mut budget, diagnostic_lines) =
        exact_geographic_linework_drawing_with_diagnostics_from_shapes(
            shapes,
            QProducer::Polygonize,
            true,
            strict,
            context,
            crate::Frame::None,
        )?;
    let (shape, _) = finish_producer_drawing(
        ProducerDrawingReceipt::drawing(shape)?,
        crate::Frame::None,
        StorageTarget::Scalar,
    )?
    .into_scalar()?;
    let mut polygons = Vec::new();
    collect_q_polygon_parts(&shape, &mut polygons, &mut budget)?;
    budget.charge_into_error(
        u64::try_from(shapes.len())
            .map_err(|_| ResourceError::SizeOverflow)?
            .checked_mul(3)
            .ok_or(ResourceError::SizeOverflow)?,
    )?;
    let mut cuts = Vec::with_capacity(shapes.len());
    let mut dangles = Vec::with_capacity(shapes.len());
    let mut invalid_rings = Vec::with_capacity(shapes.len());
    cuts.resize_with(shapes.len(), Vec::new);
    dangles.resize_with(shapes.len(), Vec::new);
    invalid_rings.resize_with(shapes.len(), Vec::new);
    for diagnostic in diagnostic_lines {
        budget.charge_into_error(1)?;
        let row = usize::try_from(diagnostic.tag.source_row.0)
            .map_err(|_| ResourceError::SizeOverflow)?;
        let group = match diagnostic.tag.kind {
            super::super::emission::plan::DiagnosticKind::Cut => &mut cuts,
            super::super::emission::plan::DiagnosticKind::Dangle => &mut dangles,
            super::super::emission::plan::DiagnosticKind::InvalidRing => &mut invalid_rings,
        };
        group
            .get_mut(row)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q diagnostic source row is out of range".into())
            })?
            .push(Shape::LineString(LineSeq::try_new(diagnostic.coordinates)?));
    }
    Ok(PolygonizeFull {
        polygons,
        cuts,
        dangles,
        invalid_rings,
    })
}

fn collect_q_polygon_parts(
    shape: &Shape,
    polygons: &mut Vec<Shape>,
    work: &mut impl Work,
) -> Result<()> {
    work.charge_into_error(1)?;
    match shape {
        Shape::Polygon(_) => polygons.push(shape.clone()),
        Shape::MultiPolygon(multi) => {
            work.charge_into_error(
                u64::try_from(multi.as_slice().len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            polygons.extend(multi.as_slice().iter().cloned().map(Shape::Polygon));
        },
        Shape::GeometryCollection(collection) => {
            work.charge_into_error(
                u64::try_from(collection.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            for member in collection.as_slice() {
                collect_q_polygon_parts(member, polygons, work)?;
            }
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => {},
    }
    Ok(())
}
