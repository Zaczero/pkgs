use std::collections::BTreeMap;

use super::super::emission::plan::{QDiagnosticLine, QEmissionPlan, QProducer};
use super::super::emission::{FragmentRecipe, PublicationInventory, QSourceImage};
use super::super::exact::SourceEdgeId;
use super::super::q::ExactGeographicTopology;
use crate::error::{ResourceError, Result};
use crate::geographic::GeographicInputs;
use crate::geometry::embedding_completion::literals::{
    LiteralAddress, LiteralPoint, LiteralPolygon, LiteralRowAddress, LiteralSequence,
    ShapeLiteralLayout, visit_shape_literals,
};
use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
use crate::geometry::embedding_search::account::{EvaluationBudget, EvaluationOnly};
use crate::geometry::ordinates::{RootId, RootRange, SourceArcContext};
use crate::geometry::publication::{
    DirectedChartRestrictionUse, FinishedGeometry, PendingPublicationRow, ProducerDrawingReceipt,
    ProducerImageId, ProducerLiteral, ProducerSourceRootUse,
};
use crate::geometry::tessellation::exact::ExactRational;
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

pub(in crate::geometry::topology) fn source_root_ranges_by_q_port(
    topology: &ExactGeographicTopology,
    selected_recipes: &[FragmentRecipe],
    work: &mut impl Work,
) -> Result<Vec<Vec<RootRange>>> {
    let mut selected_ranges = BTreeMap::<SourceEdgeId, Vec<(ExactRational, ExactRational)>>::new();
    for recipe in selected_recipes {
        work.charge_into_error(1)?;
        for range in recipe.ranges.iter().filter(|range| range.emitted) {
            work.charge_into_error(1)?;
            selected_ranges.entry(range.source_edge).or_default().push((
                range.range.start.try_clone_with_work(work)?,
                range.range.end.try_clone_with_work(work)?,
            ));
        }
    }

    let port_count = topology.q.ports.len();
    work.charge_into_error(u64::try_from(port_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut output = Vec::with_capacity(port_count);
    output.resize_with(port_count, Vec::<RootRange>::new);
    for span in &topology.q.spans {
        work.charge_into_error(1)?;
        let [first_port, second_port] = span.ports;
        for owner in &span.owners {
            work.charge_into_error(1)?;
            let Some(ranges) = selected_ranges.get(&owner.source_edge) else {
                continue;
            };
            let (first_parameter, second_parameter) = if owner.reversed {
                (&owner.source_range.end, &owner.source_range.start)
            } else {
                (&owner.source_range.start, &owner.source_range.end)
            };
            for (port, parameter) in [
                (first_port, first_parameter),
                (second_port, second_parameter),
            ] {
                let mut selected = false;
                for (range_start, range_end) in ranges {
                    work.charge_into_error(1)?;
                    let (lower, upper) = if range_start.try_cmp_with_work(range_end, work)?.is_gt()
                    {
                        (range_end, range_start)
                    } else {
                        (range_start, range_end)
                    };
                    if parameter.try_cmp_with_work(lower, work)?.is_ge()
                        && parameter.try_cmp_with_work(upper, work)?.is_le()
                    {
                        selected = true;
                        break;
                    }
                }
                if selected {
                    output
                        .get_mut(port.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "selected source use references an unknown Q port".into(),
                            )
                        })?
                        .push(RootRange::point_with_work(
                            RootId::from_index(owner.source_edge.0),
                            parameter.try_clone_with_work(work)?,
                            ExactRational::zero(),
                            work,
                        )?);
                }
            }
        }
    }
    Ok(output)
}

pub(in crate::geometry::topology) fn source_root_ranges_for_q_image(
    topology: &ExactGeographicTopology,
    ranges_by_port: &[Vec<RootRange>],
    publication: Option<&PublicationInventory>,
    image: QSourceImage,
    work: &mut impl Work,
) -> Result<Vec<RootRange>> {
    let port = match image {
        QSourceImage::PublicationSample(sample) => {
            let Some(publication) = publication else {
                return Ok(Vec::new());
            };
            let sample = publication.samples.get(sample as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "producer sample image has no publication sample row".into(),
                )
            })?;
            let Some(location) = &sample.source_trace else {
                return Ok(Vec::new());
            };
            return super::super::emission::source_root_ranges_at_location(
                publication,
                location,
                work,
            );
        },
        source @ (QSourceImage::Port(_)
        | QSourceImage::SourcePoint(_)
        | QSourceImage::SourceRingStart(_)
        | QSourceImage::SourceRingEnd(_)
        | QSourceImage::CollapsedStart(_)
        | QSourceImage::CollapsedEnd(_)) => {
            let Some(port) = super::super::q_embedding::q_source_image_port(topology, source)?
            else {
                return Ok(Vec::new());
            };
            port
        },
    };
    let ranges = ranges_by_port.get(port.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q source image references an unknown source-use row".into())
    })?;
    work.charge_into_error(u64::try_from(ranges.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut output = Vec::with_capacity(ranges.len());
    for range in ranges {
        work.charge_into_error(1)?;
        output.push(range.try_clone_with_work(work)?);
    }
    Ok(output)
}

pub(in crate::geometry::topology) fn append_shape_layout<I, W>(
    shape: &Shape,
    layout: ShapeLiteralLayout<I>,
    mut source_ranges: impl FnMut(I, &mut W) -> Result<Vec<RootRange>>,
    base_address: LiteralAddress,
    next_image: &mut usize,
    literals: &mut Vec<ProducerLiteral>,
    producer_source_uses: &mut Vec<ProducerSourceRootUse>,
    work: &mut W,
) -> Result<ShapeLiteralLayout<ProducerImageId>>
where
    I: Copy,
    W: Work,
{
    fn map_image<I, W, F>(
        input: I,
        output: u32,
        next_image: &mut usize,
        producer_source_uses: &mut Vec<ProducerSourceRootUse>,
        source_ranges: &mut F,
        base_address: LiteralAddress,
        work: &mut W,
    ) -> Result<ProducerImageId>
    where
        I: Copy,
        W: Work,
        F: FnMut(I, &mut W) -> Result<Vec<RootRange>>,
    {
        work.charge_into_error(1)?;
        let image = ProducerImageId::new(*next_image);
        *next_image = (*next_image)
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let address = LiteralAddress {
            output,
            ..base_address
        };
        for range in source_ranges(input, work)? {
            work.charge_into_error(1)?;
            producer_source_uses.push(ProducerSourceRootUse {
                address,
                image,
                range,
            });
        }
        Ok(image)
    }

    work.charge_into_error(
        u64::try_from(layout.points.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut points = Vec::with_capacity(layout.points.len());
    for point in layout.points {
        let image = map_image(
            point.image,
            point.output,
            next_image,
            producer_source_uses,
            &mut source_ranges,
            base_address,
            work,
        )?;
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
        for image in line.images {
            images.push(map_image(
                image,
                line.output,
                next_image,
                producer_source_uses,
                &mut source_ranges,
                base_address,
                work,
            )?);
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
        for image in polygon.shell.images {
            shell_images.push(map_image(
                image,
                polygon.shell.output,
                next_image,
                producer_source_uses,
                &mut source_ranges,
                base_address,
                work,
            )?);
        }
        let shell = LiteralSequence {
            output: polygon.shell.output,
            axes: polygon.shell.axes,
            images: shell_images,
        };
        let mut holes = Vec::with_capacity(polygon.holes.len());
        for hole in polygon.holes {
            let mut images = Vec::with_capacity(hole.images.len());
            for image in hole.images {
                images.push(map_image(
                    image,
                    hole.output,
                    next_image,
                    producer_source_uses,
                    &mut source_ranges,
                    base_address,
                    work,
                )?);
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
    source_ranges: impl FnMut(I, &mut W) -> Result<Vec<RootRange>>,
    chart_restrictions: Vec<DirectedChartRestrictionUse>,
    base_address: LiteralAddress,
    retain_literals_for_republication: bool,
    work: &mut W,
) -> Result<ProducerDrawingReceipt>
where
    I: Copy,
    W: Work,
{
    let mut next_image = 0_usize;
    let mut literals = Vec::new();
    let mut source_root_uses = Vec::new();
    let mapped_layout = append_shape_layout(
        &shape,
        layout,
        source_ranges,
        base_address,
        &mut next_image,
        &mut literals,
        &mut source_root_uses,
        work,
    )?;
    work.charge_into_error(
        u64::try_from(chart_restrictions.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let buffers = FrozenLiteralBuffers::shapes(shape, mapped_layout);
    Ok(if retain_literals_for_republication {
        ProducerDrawingReceipt::new_for_republication(
            buffers,
            literals,
            source_root_uses,
            chart_restrictions,
        )
    } else {
        ProducerDrawingReceipt::new(buffers, literals, source_root_uses, chart_restrictions)
    })
}
