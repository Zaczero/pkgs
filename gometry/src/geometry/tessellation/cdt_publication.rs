use crate::error::{ResourceError, Result};
use crate::geometry::publication::{FinishedGeometry, PublicationRows};
use crate::geometry::storage::ValidityMask;
use crate::geometry::tessellation::CdtRefinement;
use crate::geometry::{
    Coordinates as _, ExpansionBudget, GeometryErrorKind, PublishedTriangleVertices, Shape,
    TopologyDomain,
};
use crate::{Frame, PyGeometryArray};

pub(crate) struct PreparedCdtDrawing {
    vertices: PublishedTriangleVertices,
}

impl PreparedCdtDrawing {
    const fn empty() -> Self {
        Self {
            vertices: PublishedTriangleVertices {
                points: Vec::new(),
                ring_ends: Vec::new(),
                mesh_publication: None,
            },
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
    let vertices = crate::geometry::publish_cdt_triangle_vertex_stream_budgeted(
        &plan,
        frame,
        &source_context,
        budget,
    )?;
    Ok(PreparedCdtDrawing { vertices })
}

pub(crate) fn publish_cdt_array(
    drawing: PreparedCdtDrawing,
    frame: Frame,
) -> Result<PyGeometryArray> {
    let mut rows = PublicationRows::with_producer_drawing_capacity(drawing.face_count());
    append_drawing(drawing, None, &mut rows)?;
    rows.finish(frame)
}

pub(crate) struct CdtGroupsDrawingWriter {
    row_count: usize,
    missing: Option<ValidityMask>,
    rows: PublicationRows,
}

impl CdtGroupsDrawingWriter {
    pub(crate) fn new(row_count: usize, missing: Option<ValidityMask>) -> Result<Self> {
        if missing.as_ref().is_some_and(|mask| mask.len() != row_count) {
            return Err(GeometryErrorKind::Invalid(
                "CDT group validity does not match its logical rows".into(),
            )
            .into());
        }
        row_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        Ok(Self {
            row_count,
            missing,
            rows: PublicationRows::with_groups_capacity(row_count),
        })
    }

    pub(crate) fn append_row(
        &mut self,
        logical_row: usize,
        drawing: Option<PreparedCdtDrawing>,
    ) -> Result<()> {
        let next_row = self.rows.next_group_row();
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
            append_drawing(drawing, Some(logical_row), &mut self.rows)?;
        }
        self.rows.end_group(is_missing)?;
        Ok(())
    }

    pub(crate) fn finish(self, frame: Frame) -> Result<FinishedGeometry> {
        if self.rows.next_group_row() != self.row_count {
            return Err(GeometryErrorKind::Invalid(
                "CDT group writer did not receive every logical row".into(),
            )
            .into());
        }
        self.rows.finish_groups_geometry(frame, self.missing)
    }
}

fn append_drawing(
    drawing: PreparedCdtDrawing,
    group_row: Option<usize>,
    rows: &mut PublicationRows,
) -> Result<usize> {
    let PreparedCdtDrawing { vertices } = drawing;
    let shapes =
        Shape::triangle_shapes_from_published_rings(&vertices.points, &vertices.ring_ends)?;

    let mut start = 0;
    let row_count = shapes.len();
    for (shape, &end) in shapes.into_iter().zip(&vertices.ring_ends) {
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
        let logical_row = group_row.unwrap_or_else(|| rows.next_value_row());
        rows.try_push_drawing(shape, logical_row, "constrained_delaunay_triangles")?;
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
