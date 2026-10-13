use std::sync::Arc;

use crate::array::StreamingShapes;
use crate::error::{ResourceError, Result};
use crate::geometry::embedding_completion::{
    FrozenLiteralBuffers, LiteralAddress, LiteralPolygon, LiteralRowLayout, LiteralSequence,
    ShapeLiteralLayout,
};
use crate::geometry::ordinates::{RootId, RootRange};
use crate::geometry::publication::{
    DirectedChartRestrictionUse, FinishedGeometry, ProducerDrawingReceipt, ProducerImageId,
    ProducerLiteral, ProducerSourceRootUse, StorageTarget, finish_producer_drawing,
};
use crate::geometry::storage::ValidityMask;
use crate::geometry::tessellation::{
    CdtRefinement, CdtSideBoundaryUse, ConstrainedMeshPlan, exact,
};
use crate::geometry::{
    Coordinates as _, ExpansionBudget, GeometryErrorKind, PublishedTriangleVertices, Shape,
    TopologyDomain, Work,
};
use crate::{Frame, PyGeometryArray};

pub(crate) struct PreparedCdtDrawing {
    plan: Option<ConstrainedMeshPlan>,
    vertices: PublishedTriangleVertices,
    face_vertex_indices: Vec<[usize; 4]>,
}

impl PreparedCdtDrawing {
    const fn empty() -> Self {
        Self {
            plan: None,
            vertices: PublishedTriangleVertices {
                points: Vec::new(),
                ring_ends: Vec::new(),
                mesh_publication: None,
            },
            face_vertex_indices: Vec::new(),
        }
    }

    const fn face_count(&self) -> usize {
        self.vertices.ring_ends.len()
    }
}

pub(crate) fn prepare_geographic_cdt_drawing(
    shape: &Shape,
    refinement: CdtRefinement,
    domain: TopologyDomain,
    metric: crate::dispatch::MetricCtx<'_>,
    budget: &mut ExpansionBudget,
) -> Result<PreparedCdtDrawing> {
    let TopologyDomain::Geographic(frame) = domain else {
        return Err(GeometryErrorKind::Invalid(
            "geographic CDT drawing requires a geographic topology domain".into(),
        )
        .into());
    };
    let Some(plan) = shape.prepare_constrained_delaunay_plan_in_domain(refinement, domain)? else {
        return Ok(PreparedCdtDrawing::empty());
    };
    let metric_access = || {
        metric.geographic_chart_metric()?.ok_or_else(|| {
            crate::crs::CrsError::invalid(
                "geographic constrained triangulation requires the operation's CRS metric",
            )
        })
    };
    let source_context = crate::geometry::SourceArcContext::new(domain, Some(&metric_access));
    let (vertices, face_vertex_indices) =
        crate::geometry::publish_cdt_triangle_vertex_stream_budgeted(
            &plan,
            frame,
            &source_context,
            budget,
        )?;
    Ok(PreparedCdtDrawing {
        plan: Some(plan),
        vertices,
        face_vertex_indices,
    })
}

pub(crate) fn publish_cdt_array(
    drawing: PreparedCdtDrawing,
    frame: Frame,
) -> Result<PyGeometryArray> {
    let capacity = drawing.vertices.points.len();
    let face_count = drawing.face_count();
    let mut writer = StreamingShapes::with_capacity(capacity, face_count);
    let mut row_map = Vec::with_capacity(face_count);
    let mut literals = Vec::with_capacity(capacity);
    let mut source_root_uses = Vec::new();
    let mut chart_restrictions = Vec::new();
    let mut next_image = 0_usize;
    let mut work = crate::geometry::Unmetered;
    append_drawing(
        drawing,
        None,
        0,
        &mut writer,
        &mut row_map,
        &mut literals,
        &mut source_root_uses,
        &mut chart_restrictions,
        &mut next_image,
        &mut work,
    )?;

    let (storage, missing, layout) = writer.finish_parts()?;
    let buffers =
        FrozenLiteralBuffers::rows(Arc::new(storage), missing, layout, row_map, &mut work)?;
    let receipt =
        ProducerDrawingReceipt::new(buffers, literals, source_root_uses, chart_restrictions);
    PyGeometryArray::from_finished(finish_producer_drawing(
        receipt,
        frame,
        StorageTarget::Array,
    )?)
}

pub(crate) struct CdtGroupsDrawingWriter {
    row_count: usize,
    missing: Option<ValidityMask>,
    writer: StreamingShapes,
    value_map: Vec<Option<LiteralRowLayout<ProducerImageId>>>,
    literals: Vec<ProducerLiteral>,
    source_root_uses: Vec<ProducerSourceRootUse>,
    chart_restrictions: Vec<DirectedChartRestrictionUse>,
    offsets: Vec<i64>,
    next_image: usize,
    work: crate::geometry::Unmetered,
}

impl CdtGroupsDrawingWriter {
    pub(crate) fn new(row_count: usize, missing: Option<ValidityMask>) -> Result<Self> {
        if missing.as_ref().is_some_and(|mask| mask.len() != row_count) {
            return Err(GeometryErrorKind::Invalid(
                "CDT group validity does not match its logical rows".into(),
            )
            .into());
        }
        let offset_count = row_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut offsets = Vec::with_capacity(offset_count);
        offsets.push(0_i64);
        Ok(Self {
            row_count,
            missing,
            writer: StreamingShapes::new(),
            value_map: Vec::new(),
            literals: Vec::new(),
            source_root_uses: Vec::new(),
            chart_restrictions: Vec::new(),
            offsets,
            next_image: 0,
            work: crate::geometry::Unmetered,
        })
    }

    pub(crate) fn append_row(
        &mut self,
        logical_row: usize,
        drawing: Option<PreparedCdtDrawing>,
    ) -> Result<()> {
        let next_row = self.offsets.len().checked_sub(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("CDT group writer lost its initial offset".into())
        })?;
        if logical_row != next_row || logical_row >= self.row_count {
            return Err(GeometryErrorKind::Invalid(
                "CDT group rows were not appended in logical order".into(),
            )
            .into());
        }
        let is_missing = self
            .missing
            .as_ref()
            .is_some_and(|mask| mask.is_missing(logical_row));
        if is_missing != drawing.is_none() {
            return Err(GeometryErrorKind::Invalid(
                "CDT group receipt does not match its source-row validity".into(),
            )
            .into());
        }
        if let Some(drawing) = drawing {
            append_drawing(
                drawing,
                Some(logical_row),
                self.value_map.len(),
                &mut self.writer,
                &mut self.value_map,
                &mut self.literals,
                &mut self.source_root_uses,
                &mut self.chart_restrictions,
                &mut self.next_image,
                &mut self.work,
            )?;
        }
        self.offsets
            .push(i64::try_from(self.value_map.len()).map_err(|_| ResourceError::SizeOverflow)?);
        Ok(())
    }

    pub(crate) fn finish(mut self, frame: Frame) -> Result<FinishedGeometry> {
        let expected_offsets = self
            .row_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        if self.offsets.len() != expected_offsets {
            return Err(GeometryErrorKind::Invalid(
                "CDT group writer did not receive every logical row".into(),
            )
            .into());
        }
        let (storage, value_missing, layout) = self.writer.finish_parts()?;
        let buffers = FrozenLiteralBuffers::groups(
            Arc::new(storage),
            value_missing,
            layout,
            self.value_map,
            self.offsets,
            self.missing,
            &mut self.work,
        )?;
        let receipt = ProducerDrawingReceipt::new(
            buffers,
            self.literals,
            self.source_root_uses,
            self.chart_restrictions,
        );
        finish_producer_drawing(receipt, frame, StorageTarget::Groups)
    }
}

fn append_drawing(
    drawing: PreparedCdtDrawing,
    group_row: Option<usize>,
    value_row_base: usize,
    writer: &mut StreamingShapes,
    row_map: &mut Vec<Option<LiteralRowLayout<ProducerImageId>>>,
    literals: &mut Vec<ProducerLiteral>,
    source_root_uses: &mut Vec<ProducerSourceRootUse>,
    chart_restrictions: &mut Vec<DirectedChartRestrictionUse>,
    next_image: &mut usize,
    work: &mut impl Work,
) -> Result<usize> {
    let PreparedCdtDrawing {
        plan,
        vertices,
        face_vertex_indices,
    } = drawing;
    let shapes =
        Shape::triangle_shapes_from_published_rings(&vertices.points, &vertices.ring_ends)?;
    if shapes.len() != face_vertex_indices.len()
        || plan.as_ref().is_some_and(|plan| {
            plan.face_uses.len() != shapes.len() || plan.faces.len() != shapes.len()
        })
        || plan.is_none() && !shapes.is_empty()
    {
        return Err(GeometryErrorKind::Invalid(
            "prepared CDT literals do not match their selected faces".into(),
        )
        .into());
    }

    let mut start = 0;
    let array_row_base = row_map.len();
    let row_count = shapes.len();
    for (face_index, (shape, &end)) in shapes.into_iter().zip(&vertices.ring_ends).enumerate() {
        let ring = vertices.points.get(start..end).ok_or_else(|| {
            GeometryErrorKind::Invalid("CDT ring offsets do not match its vertex stream".into())
        })?;
        let face_points = match &shape {
            Shape::Polygon(polygon) => polygon.shell.coords(),
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => {
                return Err(GeometryErrorKind::Invalid(
                    "published CDT face is not a polygon".into(),
                )
                .into());
            },
        };
        if face_points.coord_count() != ring.len()
            || !face_points.iter_coords().eq(ring.iter().copied())
        {
            return Err(GeometryErrorKind::Invalid(
                "final CDT polygon differs from its published ring".into(),
            )
            .into());
        }
        let logical_row = group_row.map_or_else(
            || {
                array_row_base
                    .checked_add(face_index)
                    .ok_or(ResourceError::SizeOverflow)
            },
            Ok,
        )?;
        let value_row = group_row
            .map(|_| {
                value_row_base
                    .checked_add(face_index)
                    .ok_or(ResourceError::SizeOverflow)
            })
            .transpose()?;
        let address = LiteralAddress {
            operation: 0,
            logical_row,
            group_row,
            value_row,
            output: 0,
        };
        let axes = face_points.axes();
        let mut images = Vec::with_capacity(ring.len());
        for &point in ring {
            let image = ProducerImageId::new(*next_image);
            *next_image = next_image
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            images.push(image);
            literals.push(ProducerLiteral::from_point(address, image, point));
        }
        if let Some(plan) = plan.as_ref() {
            let face_use = plan.face_uses.get(face_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("CDT face has no directed side map".into())
            })?;
            let corners = face_vertex_indices.get(face_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("CDT face has no published corner map".into())
            })?;
            for (side_index, side) in face_use.sides.iter().enumerate() {
                let Some(boundary) = side.boundary.as_ref() else {
                    continue;
                };
                match boundary {
                    CdtSideBoundaryUse::Source(source_use_index) => {
                        let source_use =
                            plan.source_uses.get(*source_use_index).ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "CDT source side has no retained source-root use".into(),
                                )
                            })?;
                        let start_image =
                            face_image(&vertices, start, end, *corners, side_index, &images)?;
                        let source =
                            plan.source_constraints
                                .get(source_use.source)
                                .ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "CDT source use has no authored source occurrence".into(),
                                    )
                                })?;
                        let root = RootId::from_index(
                            u32::try_from(source.source_root)
                                .map_err(|_| ResourceError::SizeOverflow)?,
                        );
                        let range = RootRange::from_mapped_with_work(
                            root,
                            source_use.start.try_clone_with_work(work)?,
                            source_use.end.try_clone_with_work(work)?,
                            exact::ExactRational::zero(),
                            exact::ExactRational::one(),
                            work,
                        )?;
                        source_root_uses.push(ProducerSourceRootUse {
                            address,
                            image: start_image,
                            range,
                        });
                    },
                    CdtSideBoundaryUse::Restriction(piece_index) => {
                        let piece = plan.restriction_pieces.get(*piece_index).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "CDT restriction side has no retained directed use".into(),
                            )
                        })?;
                        chart_restrictions.push(piece.use_);
                    },
                }
            }
        }
        writer.try_push(shape)?;
        row_map.push(Some(LiteralRowLayout {
            operation: 0,
            logical_row,
            group_row,
            value_row,
            layout: ShapeLiteralLayout {
                points: Vec::new(),
                lines: Vec::new(),
                polygons: vec![LiteralPolygon {
                    output: 0,
                    axes,
                    shell: LiteralSequence {
                        output: 0,
                        axes,
                        images,
                    },
                    holes: Vec::new(),
                }],
            },
        }));
        start = end;
    }
    if start != vertices.points.len() {
        return Err(GeometryErrorKind::Invalid(
            "CDT ring offsets do not cover their vertex stream".into(),
        )
        .into());
    }
    Ok(row_count)
}

fn face_image(
    vertices: &PublishedTriangleVertices,
    start: usize,
    end: usize,
    corners: [usize; 4],
    side_index: usize,
    images: &[ProducerImageId],
) -> Result<ProducerImageId> {
    let corner = *corners.get(side_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("CDT source side has no start-corner mapping".into())
    })?;
    let local = corner.checked_sub(start).ok_or_else(|| {
        GeometryErrorKind::Invalid("CDT source corner precedes its output ring".into())
    })?;
    if corner >= end || local >= images.len() || vertices.points.get(corner).is_none() {
        return Err(GeometryErrorKind::Invalid(
            "CDT source corner is outside its final output ring".into(),
        )
        .into());
    }
    images.get(local).copied().ok_or_else(|| {
        GeometryErrorKind::Invalid("CDT source corner has no literal image".into()).into()
    })
}
