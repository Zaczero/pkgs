use std::collections::BTreeMap;

use super::super::emission::QSourceImage;
use super::super::exact::SourceEdgeId;
use super::super::q::ExactGeographicTopology;
use super::QEmissionPlan;
use crate::error::{ResourceError, Result};
use crate::geometry::embedding_completion::{
    FrozenLiteralBuffers, LiteralAddress, LiteralPoint, LiteralPolygon, LiteralSequence,
    ShapeLiteralLayout,
};
use crate::geometry::embedding_search::Work;
use crate::geometry::ordinates::{RootId, RootRange};
use crate::geometry::publication::{
    DirectedChartRestrictionUse, ProducerDrawingReceipt, ProducerImageId, ProducerLiteral,
    ProducerSourceRootUse,
};
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{GeometryErrorKind, Shape};

pub(super) fn source_root_ranges_by_q_port(
    topology: &ExactGeographicTopology,
    plan: &QEmissionPlan,
    work: &mut impl Work,
) -> Result<Vec<Vec<RootRange>>> {
    let mut selected_ranges = BTreeMap::<SourceEdgeId, Vec<(ExactRational, ExactRational)>>::new();
    for recipe in plan.fragment_recipes.as_slice() {
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
        for owner in span.owners.iter() {
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

pub(super) fn source_root_ranges_for_q_image(
    topology: &ExactGeographicTopology,
    ranges_by_port: &[Vec<RootRange>],
    image: QSourceImage,
    work: &mut impl Work,
) -> Result<Vec<RootRange>> {
    let port = match image {
        QSourceImage::Port(port) => Some(port),
        QSourceImage::SourcePoint(index) => Some(
            topology
                .q
                .source_point_ports
                .get(index as usize)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source point image has no retained Q port".into())
                })?,
        ),
        QSourceImage::SourceRingStart(edge) | QSourceImage::CollapsedStart(edge) => {
            if topology.sources.edges.get(edge.0 as usize).is_none() {
                return Err(GeometryErrorKind::Invalid(
                    "Q source image has an unknown source edge".into(),
                )
                .into());
            }
            topology
                .q
                .source_edge_ports
                .get(edge.0 as usize)
                .and_then(|ports| ports.first())
                .copied()
        },
        QSourceImage::SourceRingEnd(edge) | QSourceImage::CollapsedEnd(edge) => {
            if topology.sources.edges.get(edge.0 as usize).is_none() {
                return Err(GeometryErrorKind::Invalid(
                    "Q source image has an unknown source edge".into(),
                )
                .into());
            }
            topology
                .q
                .source_edge_ports
                .get(edge.0 as usize)
                .and_then(|ports| ports.get(1))
                .copied()
        },
        QSourceImage::PublicationSample(_) => None,
    };
    let Some(port) = port else {
        return Ok(Vec::new());
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
    I: Copy + Ord,
    W: Work,
{
    fn map_image<I, W, F>(
        input: I,
        output: u32,
        next_image: &mut usize,
        literal_slots: &mut Vec<(LiteralAddress, ProducerImageId)>,
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
        literal_slots.push((address, image));
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

    let mut literal_slots = Vec::<(LiteralAddress, ProducerImageId)>::new();

    work.charge_into_error(
        u64::try_from(layout.points.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut points = Vec::with_capacity(layout.points.len());
    for point in layout.points {
        let image = map_image(
            point.image,
            point.output,
            next_image,
            &mut literal_slots,
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
                &mut literal_slots,
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
                &mut literal_slots,
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
                    &mut literal_slots,
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

    work.charge_into_error(
        u64::try_from(literal_slots.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut slot = literal_slots.into_iter();
    shape.try_for_each_point(|record| {
        let (address, image) = slot.next().ok_or_else(|| {
            GeometryErrorKind::Invalid("producer literal layout is shorter than its drawing".into())
        })?;
        literals.push(ProducerLiteral::from_point(address, image, record));
        Ok::<(), crate::error::Error>(())
    })?;
    if slot.next().is_some() {
        return Err(GeometryErrorKind::Invalid(
            "producer literal layout is longer than its drawing".into(),
        )
        .into());
    }
    Ok(layout)
}

pub(in crate::geometry::topology) fn receipt_from_shape_layout<I, W>(
    shape: Shape,
    layout: ShapeLiteralLayout<I>,
    source_ranges: impl FnMut(I, &mut W) -> Result<Vec<RootRange>>,
    chart_restrictions: Vec<DirectedChartRestrictionUse>,
    base_address: LiteralAddress,
    work: &mut W,
) -> Result<ProducerDrawingReceipt>
where
    I: Copy + Ord,
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
    Ok(ProducerDrawingReceipt::new(
        FrozenLiteralBuffers::shapes(shape, mapped_layout),
        literals,
        source_root_uses,
        chart_restrictions,
    ))
}
