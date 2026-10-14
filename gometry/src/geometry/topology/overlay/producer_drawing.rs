use super::super::emission::plan::{QDiagnosticLine, QEmissionPlan, QProducer};
use super::super::q::ExactGeographicTopology;
use crate::error::{ResourceError, Result};
use crate::geographic::GeographicInputs;
use crate::geometry::embedding_completion::literals::{
    LiteralAddress, LiteralPoint, LiteralPolygon, LiteralRowAddress, LiteralSequence,
    ShapeLiteralLayout, ShapeStructure, visit_shape_literals,
};
use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
use crate::geometry::embedding_search::account::{EvaluationBudget, EvaluationOnly};
use crate::geometry::ordinates::SourceArcContext;
use crate::geometry::publication::{
    FinishedGeometry, PendingPublicationRow, ProducerDrawingReceipt, ProducerImageId,
    ProducerLiteral,
};
use crate::geometry::{
    CoordinateAxes, GeometryErrorKind, LineSeq, PolygonizeFull, Shape, TopologyDomain,
};
use crate::work::Work;

pub(in crate::geometry) fn exact_geographic_linework_from_shapes<
    M: crate::geometry::ChartMetric,
>(
    shapes: &[&Shape],
    producer: QProducer,
    diagnostics: bool,
    strict: bool,
    context: &SourceArcContext<'_, '_, M>,
    publication_frame: crate::Frame,
) -> Result<Shape> {
    exact_geographic_linework_finished_from_shapes(
        shapes,
        producer,
        diagnostics,
        strict,
        context,
        publication_frame,
        false,
    )?
    .0
    .into_scalar()
    .map(|(shape, _)| shape)
}

pub(in crate::geometry) fn exact_geographic_linework_pending_row<
    M: crate::geometry::ChartMetric,
>(
    shapes: &[&Shape],
    producer: QProducer,
    diagnostics: bool,
    strict: bool,
    context: &SourceArcContext<'_, '_, M>,
    publication_frame: &crate::Frame,
    logical_row: usize,
) -> Result<PendingPublicationRow> {
    let (finished, _construction_work) = exact_geographic_linework_finished_from_shapes(
        shapes,
        producer,
        diagnostics,
        strict,
        context,
        publication_frame.clone(),
        true,
    )?;
    let publication_work =
        EvaluationBudget::new(producer.operation(), finished.output_literal_count())?;
    finished.into_pending_row(logical_row, publication_work, publication_frame)
}

pub(in crate::geometry) fn exact_geographic_linework_finished_from_shapes<
    M: crate::geometry::ChartMetric,
>(
    shapes: &[&Shape],
    producer: QProducer,
    diagnostics: bool,
    strict: bool,
    context: &SourceArcContext<'_, '_, M>,
    publication_frame: crate::Frame,
    retain_literals_for_republication: bool,
) -> Result<(FinishedGeometry, EvaluationBudget)> {
    let (finished, budget, _) = exact_geographic_linework_finished_with_diagnostics_from_shapes(
        shapes,
        producer,
        diagnostics,
        strict,
        context,
        publication_frame,
        retain_literals_for_republication,
    )?;
    Ok((finished, budget))
}

fn exact_geographic_linework_finished_with_diagnostics_from_shapes<
    M: crate::geometry::ChartMetric,
>(
    shapes: &[&Shape],
    producer: QProducer,
    diagnostics: bool,
    strict: bool,
    context: &SourceArcContext<'_, '_, M>,
    publication_frame: crate::Frame,
    retain_literals_for_republication: bool,
) -> Result<(FinishedGeometry, EvaluationBudget, Vec<QDiagnosticLine>)> {
    let TopologyDomain::Geographic(frame) = context.domain() else {
        return Err(GeometryErrorKind::Invalid(
            "exact Q linework requires a geographic source context".into(),
        )
        .into());
    };
    let inputs = GeographicInputs::try_from_shapes(frame, shapes.iter().copied())
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    let topology = ExactGeographicTopology::from_views_with_publication_frame(
        frame,
        publication_frame,
        inputs.views().iter().copied(),
    )?;
    let declared_axes = shapes
        .iter()
        .fold(CoordinateAxes::XY, |axes, shape| axes.union(shape.axes()));
    let mut budget = EvaluationBudget::new(producer.operation(), 0)?;
    let (finished, diagnostic_lines) = {
        let mut work = EvaluationOnly::new(&mut budget);
        QEmissionPlan::build_pooled_linework(
            &topology,
            producer,
            diagnostics,
            strict,
            declared_axes,
            context.metric_access(),
            retain_literals_for_republication,
            &mut work,
        )?
    };
    Ok((finished, budget, diagnostic_lines))
}

pub(crate) fn exact_geographic_polygonize_full_from_shapes<M: crate::geometry::ChartMetric>(
    shapes: &[&Shape],
    strict: bool,
    context: &SourceArcContext<'_, '_, M>,
) -> Result<PolygonizeFull> {
    let (finished, mut budget, diagnostic_lines) =
        exact_geographic_linework_finished_with_diagnostics_from_shapes(
            shapes,
            QProducer::Polygonize,
            true,
            strict,
            context,
            crate::Frame::None,
            false,
        )?;
    let (shape, _) = finished.into_scalar()?;
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
        Shape::Polygon(_) => {
            polygons.push(shape.clone());
        },
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

pub(in crate::geometry::topology) fn append_shape_layout<I, W>(
    shape: &Shape,
    layout: ShapeLiteralLayout<I>,
    base_address: LiteralAddress,
    next_image: &mut usize,
    literals: &mut Vec<ProducerLiteral>,
    work: &mut W,
) -> Result<ShapeLiteralLayout<ProducerImageId>>
where
    W: Work,
{
    fn allocate_image<W: Work>(next_image: &mut usize, work: &mut W) -> Result<ProducerImageId> {
        work.charge_into_error(1)?;
        let image = ProducerImageId::new(*next_image);
        *next_image = (*next_image)
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        Ok(image)
    }

    work.charge_into_error(
        u64::try_from(layout.points.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut points = Vec::with_capacity(layout.points.len());
    for point in layout.points {
        let image = allocate_image(next_image, work)?;
        points.push(LiteralPoint {
            output: point.output,
            axes: point.axes,
            image,
        });
    }

    work.charge_into_error(
        u64::try_from(layout.lines.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut lines = Vec::with_capacity(layout.lines.len());
    for line in layout.lines {
        let mut images = Vec::with_capacity(line.images.len());
        for _ in line.images {
            images.push(allocate_image(next_image, work)?);
        }
        lines.push(LiteralSequence {
            output: line.output,
            axes: line.axes,
            images,
        });
    }

    work.charge_into_error(
        u64::try_from(layout.polygons.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut polygons = Vec::with_capacity(layout.polygons.len());
    for polygon in layout.polygons {
        let mut shell_images = Vec::with_capacity(polygon.shell.images.len());
        for _ in polygon.shell.images {
            shell_images.push(allocate_image(next_image, work)?);
        }
        let shell = LiteralSequence {
            output: polygon.shell.output,
            axes: polygon.shell.axes,
            images: shell_images,
        };
        let mut holes = Vec::with_capacity(polygon.holes.len());
        for hole in polygon.holes {
            let mut images = Vec::with_capacity(hole.images.len());
            for _ in hole.images {
                images.push(allocate_image(next_image, work)?);
            }
            holes.push(LiteralSequence {
                output: hole.output,
                axes: hole.axes,
                images,
            });
        }
        polygons.push(LiteralPolygon {
            output: polygon.output,
            axes: polygon.axes,
            shell,
            holes,
        });
    }
    let layout = ShapeLiteralLayout {
        structure: Some(ShapeStructure::capture(shape)),
        points,
        lines,
        polygons,
    };

    let mut emit_literal = |address, image, record, _axes, _stored_axes, _work: &mut W| {
        literals.push(ProducerLiteral::from_point(address, image, record));
        Ok(())
    };
    visit_shape_literals(
        shape,
        &layout,
        LiteralRowAddress::from_literal_address(base_address),
        work,
        &mut emit_literal,
    )?;
    Ok(layout)
}

pub(in crate::geometry::topology) fn receipt_from_shape_layout<I, W>(
    shape: Shape,
    layout: ShapeLiteralLayout<I>,
    base_address: LiteralAddress,
    retain_literals_for_republication: bool,
    work: &mut W,
) -> Result<ProducerDrawingReceipt>
where
    W: Work,
{
    let mut next_image = 0_usize;
    let mut literals = Vec::new();
    let mapped_layout = append_shape_layout(
        &shape,
        layout,
        base_address,
        &mut next_image,
        &mut literals,
        work,
    )?;
    let buffers = FrozenLiteralBuffers::shapes(shape, mapped_layout);
    Ok(if retain_literals_for_republication {
        ProducerDrawingReceipt::new_for_republication(buffers, literals)
    } else {
        ProducerDrawingReceipt::new(buffers, literals)
    })
}
