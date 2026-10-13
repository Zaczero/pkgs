use std::sync::Arc;

use crate::crs::TransformMetricScratch;
use crate::error::Result;
use crate::geographic::{
    ComputedEdgeIntent, ComputedLongitudeRelation, ComputedVertexIntent, GeographicFrame,
    RetainedChartDisplacement, publish_computed_chain, publish_computed_chain_with_vertex_intents,
    publish_computed_chain_with_vertex_intents_and_samples,
    publish_computed_chain_with_vertex_intents_and_samples_with_edge_ranges,
};
use crate::geometry::exact_arith::select_transformed_chart_displacement;
use crate::geometry::tessellation::{
    ConstrainedMeshPlan, VoronoiDiagramReceipt, VoronoiEdgeColumn, VoronoiEdgeOrigin,
    VoronoiPolygonOutput, VoronoiPolygonOutputs, publish_constrained_plan,
};
use crate::geometry::{
    ChartMetric, CoordSeq, CoordSeqBuilder, CoordWindow, Coordinates as _, CsrOffsetBuilder,
    CsrOffsetColumn, ExpansionBudget, GeometryErrorKind, LineSeq, Polygon, Ring, RootMetricState,
    Shape, ShapeCountQuery, SourceArcContext, Unmetered, computed_native_sample_ordinates,
};

pub(crate) type PolygonPartEdgeRoles = Vec<Vec<Vec<bool>>>;

pub(crate) struct VoronoiReceiptHold {
    _diagram: Arc<VoronoiDiagramReceipt>,
}

pub(crate) struct PublishedVoronoiPolygonOutputs {
    pub(crate) shapes: Vec<Shape>,
    receipt: VoronoiReceiptHold,
}

impl PublishedVoronoiPolygonOutputs {
    pub(crate) fn into_parts(self) -> (Vec<Shape>, VoronoiReceiptHold) {
        (self.shapes, self.receipt)
    }
}

pub(crate) struct PublishedVoronoiEdgeColumn {
    pub(crate) coordinates: CoordSeq,
    pub(crate) offsets: CsrOffsetColumn,
    receipt: VoronoiReceiptHold,
}

impl PublishedVoronoiEdgeColumn {
    pub(crate) fn into_parts(self) -> (CoordSeq, CsrOffsetColumn, VoronoiReceiptHold) {
        (self.coordinates, self.offsets, self.receipt)
    }
}

/// Rebuild shape structure while mapping each line or ring coordinate chain.
///
/// `source` carries producer-side data (for example the projected segment
/// chain), while `output` carries the published coordinate sequence. They
/// must have matching geometry and chain cardinality.
pub(crate) fn map_computed_shape_chains(
    source: &Shape,
    output: &Shape,
    mut map: impl FnMut(&CoordSeq, &CoordSeq, bool) -> Result<Option<Vec<crate::geometry::Point>>>,
) -> Result<Shape> {
    map_shape_chains(source, output, &mut map)
}

/// Pair each original and transformed coordinate chain without staging edge
/// relations; callers request each retained target connection from its
/// original endpoint pair while publishing that chain. Use this route only
/// when the source frame is geographic; projected sources retain their
/// producer-chord interpretation.
pub(crate) fn map_transformed_shape_chains(
    source: &Shape,
    output: &Shape,
    source_frame: GeographicFrame,
    target_frame: GeographicFrame,
    mut map: impl FnMut(
        &TransformedChartEdgeProvider<'_>,
        bool,
    ) -> Result<Option<Vec<crate::geometry::Point>>>,
) -> Result<Shape> {
    map_computed_shape_chains(source, output, |source, output, closed| {
        let edges =
            TransformedChartEdgeProvider::try_new(source, output, source_frame, target_frame)?;
        map(&edges, closed)
    })
}

/// Borrow one original chain and its one-for-one transformed output.
///
/// Each edge connection is derived on request, so scalar chains and selected
/// packed windows retain their caller-owned endpoint pairs without staging a
/// parallel displacement column.
pub(crate) struct TransformedChartEdgeProvider<'a> {
    source: &'a CoordSeq,
    output: &'a CoordSeq,
    source_frame: GeographicFrame,
    target_frame: GeographicFrame,
}

impl<'a> TransformedChartEdgeProvider<'a> {
    pub(crate) fn try_new(
        source: &'a CoordSeq,
        output: &'a CoordSeq,
        source_frame: GeographicFrame,
        target_frame: GeographicFrame,
    ) -> Result<Self> {
        if source.coord_count() != output.coord_count() {
            return Err(GeometryErrorKind::message(
                "transformed coordinate chain changed endpoint correspondence",
            ));
        }
        Ok(Self {
            source,
            output,
            source_frame,
            target_frame,
        })
    }

    pub(crate) const fn source(&self) -> &'a CoordSeq {
        self.source
    }

    pub(crate) const fn output(&self) -> &'a CoordSeq {
        self.output
    }

    pub(crate) fn edge_count(&self) -> usize {
        self.source.coord_count().saturating_sub(1)
    }

    pub(crate) fn retained_connection(&self, edge: usize) -> Result<RetainedChartDisplacement> {
        if edge >= self.edge_count() {
            return Err(GeometryErrorKind::message(
                "transformed edge index is outside its paired coordinate chain",
            ));
        }
        let source_start = self.source.nth_coord(edge);
        let source_end = self.source.nth_coord(edge.wrapping_add(1));
        let output_start = self.output.nth_coord(edge);
        let output_end = self.output.nth_coord(edge.wrapping_add(1));
        select_transformed_chart_displacement(
            self.source_frame,
            [source_start.x, source_end.x],
            self.target_frame,
            [output_start.x, output_end.x],
        )
    }
}

fn publish_transformed_chain_with_edges(
    source: &CoordSeq,
    output: &CoordSeq,
    closed_ring: bool,
    edges: Option<&TransformedChartEdgeProvider<'_>>,
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
    metric: &mut TransformMetricScratch<'_>,
) -> Result<Option<Vec<crate::geometry::Point>>> {
    if edges.is_none() && source.coord_count() != output.coord_count() {
        return Err(GeometryErrorKind::message(
            "transformed coordinate chain changed endpoint correspondence",
        ));
    }

    let mut current_root = None;
    let mut root_metric = RootMetricState::default();
    let mut work = Unmetered;
    publish_computed_chain_with_vertex_intents_and_samples(
        frame,
        output,
        closed_ring,
        budget,
        |index, start, end| {
            if let Some(edges) = edges {
                Ok(ComputedEdgeIntent::retained_native(
                    edges.retained_connection(index)?,
                ))
            } else {
                ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
            }
        },
        |_, _| ComputedVertexIntent::Canonical,
        |index, endpoints, relation, parameter, _retained| {
            if current_root != Some(index) {
                current_root = Some(index);
                root_metric = RootMetricState::default();
            }
            let ComputedLongitudeRelation::Native(longitude) = *relation else {
                unreachable!("transform publication supplies native retained relations");
            };
            crate::geometry::computed_native_sample_ordinates(
                frame,
                endpoints,
                longitude,
                parameter,
                output.axes(),
                || metric.geodesic_metric(),
                &mut root_metric,
                &mut work,
            )
        },
        |_, _, _, _, _, _| Ok(()),
    )
}

/// Publish one provisional output chain using its paired original source edges.
///
/// Geographic sources select a target lift from the original endpoints;
/// projected sources retain the chord the executor actually produced.
pub(crate) fn publish_transformed_chain(
    source: &CoordSeq,
    output: &CoordSeq,
    closed_ring: bool,
    source_frame: Option<GeographicFrame>,
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
    metric: &mut TransformMetricScratch<'_>,
) -> Result<Option<Vec<crate::geometry::Point>>> {
    let edges = source_frame
        .map(|source_frame| {
            TransformedChartEdgeProvider::try_new(source, output, source_frame, frame)
        })
        .transpose()?;
    publish_transformed_chain_with_edges(
        source,
        output,
        closed_ring,
        edges.as_ref(),
        frame,
        budget,
        metric,
    )
}

pub(crate) fn publish_raw_computed_shape(
    shape: &Shape,
    frame: GeographicFrame,
    operation: &'static str,
) -> Result<Shape> {
    let mut budget = ExpansionBudget::new(operation, "computed edges");
    budget.add(ShapeCountQuery::default().coordinates(shape)?)?;
    publish_raw_computed_shape_with_budget(shape, frame, &mut budget)
}

pub(crate) fn publish_computed_point_or_raw_shape(
    shape: &Shape,
    frame: GeographicFrame,
    operation: &'static str,
) -> Result<Shape> {
    match shape {
        Shape::Point(point) => Ok(Shape::Point(
            frame
                .canonical_computed_point(*point)
                .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?,
        )),
        Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => publish_raw_computed_shape(shape, frame, operation),
    }
}

pub(crate) fn publish_raw_computed_shape_with_budget(
    shape: &Shape,
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
) -> Result<Shape> {
    map_computed_shape_chains(shape, shape, |_, coords, closed| {
        publish_computed_chain(frame, coords, closed, budget, |_, start, end| {
            ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
        })
    })
}

pub(crate) fn publish_raw_computed_shape_with_budget_and_context(
    shape: &Shape,
    frame: GeographicFrame,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    budget: &mut ExpansionBudget,
) -> Result<Shape> {
    map_computed_shape_chains(shape, shape, |_, coords, closed| {
        let intent = |_, start: crate::geometry::Point, end: crate::geometry::Point| {
            ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
        };
        let capture =
            crate::geometry::SourceArcCapture::for_output_axes(coords.axes(), source_context);
        publish_computed_chain_with_capture(frame, coords, closed, budget, &capture, intent)
    })
}

pub(crate) fn publish_raw_computed_shape_with_context(
    shape: &Shape,
    frame: GeographicFrame,
    operation: &'static str,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
) -> Result<Shape> {
    let mut budget = ExpansionBudget::new(operation, "computed edges");
    budget.add(ShapeCountQuery::default().coordinates(shape)?)?;
    publish_raw_computed_shape_with_budget_and_context(shape, frame, source_context, &mut budget)
}

pub(crate) fn publish_raw_computed_shape_with_capture(
    shape: &Shape,
    frame: GeographicFrame,
    operation: &'static str,
    source_capture: &crate::geometry::SourceArcCapture<'_, '_, crate::crs::GeodesicMetric>,
) -> Result<Shape> {
    let mut budget = ExpansionBudget::new(operation, "computed edges");
    budget.add(ShapeCountQuery::default().coordinates(shape)?)?;
    map_computed_shape_chains(shape, shape, |_, coords, closed| {
        let intent = |_, start: crate::geometry::Point, end: crate::geometry::Point| {
            ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
        };
        publish_computed_chain_with_capture(
            frame,
            coords,
            closed,
            &mut budget,
            source_capture,
            intent,
        )
    })
}

fn publish_computed_chain_with_capture<'intent>(
    frame: GeographicFrame,
    coordinates: &CoordSeq,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
    source_capture: &crate::geometry::SourceArcCapture<'_, '_, crate::crs::GeodesicMetric>,
    edge_intent: impl Fn(
        usize,
        crate::geometry::Point,
        crate::geometry::Point,
    ) -> Result<ComputedEdgeIntent<'intent>>,
) -> Result<Option<Vec<crate::geometry::Point>>> {
    match source_capture.context() {
        Some(source_context) => publish_native_computed_chain(
            frame,
            coordinates,
            closed_ring,
            budget,
            source_context,
            edge_intent,
            |_, _| ComputedVertexIntent::Canonical,
        ),
        None => publish_computed_chain(frame, coordinates, closed_ring, budget, edge_intent),
    }
}

pub(crate) fn publish_transformed_shape(
    source: &Shape,
    output: &Shape,
    source_frame: Option<GeographicFrame>,
    frame: GeographicFrame,
    operation: &'static str,
    metric: &mut TransformMetricScratch<'_>,
) -> Result<Shape> {
    let canonical_point = |point: crate::geometry::Point| {
        frame
            .canonical_computed_point(point)
            .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))
    };
    match (source, output) {
        (Shape::Point(_), Shape::Point(point)) => Ok(Shape::Point(canonical_point(*point)?)),
        (Shape::MultiPoint(_), Shape::MultiPoint(points)) => {
            Ok(Shape::MultiPoint(points.try_map_values(canonical_point)?))
        },
        (Shape::GeometryCollection(source), Shape::GeometryCollection(output)) => {
            let mut source_members = source.iter();
            let mapped = output.try_map_members(|member| {
                let source = source_members.next().ok_or_else(|| {
                    GeometryErrorKind::message(
                        "computed output changed a geometry-collection structure",
                    )
                })?;
                publish_transformed_shape(source, member, source_frame, frame, operation, metric)
            })?;
            if source_members.next().is_some() {
                return Err(GeometryErrorKind::message(
                    "computed output changed a geometry-collection structure",
                ));
            }
            Ok(Shape::GeometryCollection(mapped))
        },
        (Shape::LineString(_), Shape::LineString(_))
        | (Shape::MultiLineString(_), Shape::MultiLineString(_))
        | (Shape::Polygon(_), Shape::Polygon(_))
        | (Shape::MultiPolygon(_), Shape::MultiPolygon(_)) => {
            let mut budget = ExpansionBudget::new(operation, "computed edges");
            budget.add(ShapeCountQuery::default().coordinates(output)?)?;
            match source_frame {
                Some(source_frame) => map_transformed_shape_chains(
                    source,
                    output,
                    source_frame,
                    frame,
                    |edges, closed| {
                        publish_transformed_chain_with_edges(
                            edges.source(),
                            edges.output(),
                            closed,
                            Some(edges),
                            frame,
                            &mut budget,
                            metric,
                        )
                    },
                ),
                None => map_computed_shape_chains(source, output, |source, output, closed| {
                    publish_transformed_chain(
                        source,
                        output,
                        closed,
                        None,
                        frame,
                        &mut budget,
                        metric,
                    )
                }),
            }
        },
        (Shape::Empty(..), Shape::Empty(..)) => Ok(output.clone()),
        _ => Err(GeometryErrorKind::message(
            "computed output changed geometry structure",
        )),
    }
}

pub(crate) fn publish_raw_computed_shapes_with_context(
    shapes: &[Shape],
    frame: GeographicFrame,
    operation: &'static str,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
) -> Result<Vec<Shape>> {
    let mut budget = ExpansionBudget::new(operation, "computed edges");
    let mut counts = ShapeCountQuery::default();
    for shape in shapes {
        budget.add(counts.coordinates(shape)?)?;
    }
    publish_raw_computed_shapes_with_budget_and_context(shapes, frame, source_context, &mut budget)
}

pub(crate) fn publish_raw_computed_shapes_with_budget_and_context(
    shapes: &[Shape],
    frame: GeographicFrame,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    budget: &mut ExpansionBudget,
) -> Result<Vec<Shape>> {
    shapes
        .iter()
        .map(|shape| {
            publish_raw_computed_shape_with_budget_and_context(shape, frame, source_context, budget)
        })
        .collect()
}

pub(crate) fn publish_voronoi_polygon_outputs(
    outputs: VoronoiPolygonOutputs,
    frame: Option<GeographicFrame>,
    budget: &mut ExpansionBudget,
) -> Result<PublishedVoronoiPolygonOutputs> {
    let receipt = outputs.receipt;
    let shapes = outputs
        .outputs
        .into_iter()
        .map(|output| {
            if let Some(frame) = frame {
                publish_voronoi_polygon_shape(&output, &receipt, frame, budget)
            } else {
                validate_voronoi_owner(&output, &receipt)?;
                Ok(Shape::Polygon(output.polygon))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(PublishedVoronoiPolygonOutputs {
        shapes,
        receipt: VoronoiReceiptHold { _diagram: receipt },
    })
}

pub(in crate::geometry) fn publish_voronoi_polygon_shape(
    output: &VoronoiPolygonOutput,
    receipt: &VoronoiDiagramReceipt,
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
) -> Result<Shape> {
    validate_voronoi_owner(output, receipt)?;
    let computed_edges: Vec<Vec<bool>> = output
        .edge_roles
        .iter()
        .map(|ring| {
            ring.iter()
                .map(|role| {
                    !role
                        .origins
                        .iter()
                        .any(|origin| matches!(origin, VoronoiEdgeOrigin::SourceClip { .. }))
                })
                .collect()
        })
        .collect();
    let polygon = publish_polygon_with_edge_roles(&output.polygon, &computed_edges, frame, budget)?;
    Ok(Shape::Polygon(polygon))
}

fn validate_voronoi_owner(
    output: &VoronoiPolygonOutput,
    receipt: &VoronoiDiagramReceipt,
) -> Result<()> {
    let owner = output.owner.ok_or_else(|| {
        GeometryErrorKind::Invalid("Voronoi output has no admitted site owner".into())
    })?;
    if !receipt.has_site(owner) {
        return Err(GeometryErrorKind::Invalid(
            "Voronoi output owner is outside its diagram receipt".into(),
        )
        .into());
    }
    Ok(())
}

pub(crate) fn publish_voronoi_edge_column_with_budget(
    output: VoronoiEdgeColumn,
    frame: Option<GeographicFrame>,
    budget: &mut ExpansionBudget,
) -> Result<PublishedVoronoiEdgeColumn> {
    let (coordinates, offsets, receipt) = publish_voronoi_edge_column(output, frame, budget)?;
    Ok(PublishedVoronoiEdgeColumn {
        coordinates,
        offsets,
        receipt: VoronoiReceiptHold { _diagram: receipt },
    })
}

pub(in crate::geometry) fn publish_voronoi_edge_column(
    output: VoronoiEdgeColumn,
    frame: Option<GeographicFrame>,
    budget: &mut ExpansionBudget,
) -> Result<(CoordSeq, CsrOffsetColumn, Arc<VoronoiDiagramReceipt>)> {
    let VoronoiEdgeColumn {
        coordinates,
        offsets,
        receipt,
    } = output;
    let (coordinates, offsets) = match frame {
        Some(frame) => {
            publish_computed_line_column_with_budget(&coordinates, &offsets, frame, budget)?
        },
        None => (coordinates, offsets),
    };
    Ok((coordinates, offsets, receipt))
}

pub(crate) fn publish_computed_line_column_with_budget(
    coordinates: &CoordSeq,
    source_offsets: &CsrOffsetColumn,
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
) -> Result<(CoordSeq, CsrOffsetColumn)> {
    let mut output = CoordSeqBuilder::with_capacity(coordinates.axes(), coordinates.len());
    let mut offsets = CsrOffsetBuilder::with_capacity(source_offsets.len().saturating_sub(1));
    for pair in source_offsets.as_slice().array_windows::<2>() {
        let start = usize::try_from(pair[0]).map_err(|_| GeometryErrorKind::MalformedCsrOffsets)?;
        let end = usize::try_from(pair[1]).map_err(|_| GeometryErrorKind::MalformedCsrOffsets)?;
        let window = coordinates.view(CoordWindow::from_range(start..end, coordinates.len()));
        let published = publish_computed_chain(frame, &window, false, budget, |_, left, right| {
            ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [left.x, right.x])
        })?;
        match published {
            Some(points) => {
                for point in points {
                    output.push(point);
                }
            },
            None => output.extend_window(coordinates, start..end),
        }
        offsets.push_end(output.len(), i32::MAX as usize)?;
    }
    let coordinates = output.finish()?;
    let offsets = offsets.finish(coordinates.len())?;
    Ok((coordinates, offsets))
}

pub(crate) struct PublishedTriangleVertices {
    pub(crate) points: Vec<crate::geometry::Point>,
    pub(crate) ring_ends: Vec<usize>,
    pub(crate) mesh_publication: Option<crate::geometry::PreparedMeshPublication>,
}

impl PublishedTriangleVertices {
    pub(crate) fn uniform(points: Vec<crate::geometry::Point>) -> Result<Self> {
        if !points.len().is_multiple_of(4) {
            return Err(GeometryErrorKind::Invalid(
                "triangle vertex stream is not a multiple of four".into(),
            )
            .into());
        }
        let ring_ends = (1..=points.len() / 4)
            .map(|index| index.wrapping_mul(4))
            .collect();
        Ok(Self {
            points,
            ring_ends,
            mesh_publication: None,
        })
    }

    pub(crate) fn uniform_mesh(
        mut publication: crate::geometry::PreparedMeshPublication,
    ) -> Result<Self> {
        let mut published = Self::uniform(publication.take_points())?;
        published.mesh_publication = Some(publication);
        Ok(published)
    }
}

pub(crate) fn publish_triangle_vertex_stream_budgeted(
    vertices: &[crate::geometry::Point],
    frame: GeographicFrame,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    budget: &mut ExpansionBudget,
) -> Result<PublishedTriangleVertices> {
    if !vertices.len().is_multiple_of(4) {
        return Err(GeometryErrorKind::Invalid(
            "triangle vertex stream is not a multiple of four".into(),
        )
        .into());
    }
    let count = vertices.len() / 4;
    let mut points = Vec::with_capacity(vertices.len());
    let mut ring_ends = Vec::with_capacity(count);
    for ring in vertices.as_chunks::<4>().0 {
        match publish_native_computed_chain(
            frame,
            ring.as_slice(),
            true,
            budget,
            source_context,
            |_, start, end| {
                ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
            },
            |_, _| ComputedVertexIntent::Canonical,
        )? {
            Some(published) => points.extend(published),
            None => points.extend_from_slice(ring),
        }
        ring_ends.push(points.len());
    }
    Ok(PublishedTriangleVertices {
        points,
        ring_ends,
        mesh_publication: None,
    })
}

/// Publish selected CDT faces while retaining the written positions of their
/// four ring occurrences. The map is derived as each source edge is expanded,
/// so root and restriction uses never need a rounded-coordinate lookup.
pub(crate) fn publish_cdt_triangle_vertex_stream_budgeted(
    plan: &ConstrainedMeshPlan,
    frame: GeographicFrame,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    budget: &mut ExpansionBudget,
) -> Result<(PublishedTriangleVertices, Vec<[usize; 4]>)> {
    let source_vertices = publish_constrained_plan(plan, budget)?;
    if !source_vertices.len().is_multiple_of(4)
        || plan.faces.len() != source_vertices.len() / 4
        || plan.face_uses.len() != plan.faces.len()
    {
        return Err(GeometryErrorKind::Invalid(
            "prepared CDT faces do not match their triangle vertex stream".into(),
        )
        .into());
    }

    let mut points = Vec::with_capacity(source_vertices.len());
    let mut ring_ends = Vec::with_capacity(plan.faces.len());
    let mut face_vertex_indices = Vec::with_capacity(plan.faces.len());
    let mut edge_ranges = Vec::with_capacity(3);
    for (ring, face_uses) in source_vertices
        .as_chunks::<4>()
        .0
        .iter()
        .zip(&plan.face_uses)
    {
        let edge_intent =
            |output_side: usize, _: crate::geometry::Point, _: crate::geometry::Point| {
                let side = face_uses.sides.get(output_side).ok_or_else(|| {
                    GeometryErrorKind::Invalid("published CDT face has a missing side use".into())
                })?;
                let start = plan.site_chart_location(side.start_site);
                let end = plan.site_chart_location(side.end_site);
                if let (Some(start), Some(end)) = (start, end) {
                    Ok(ComputedEdgeIntent::retained_native(
                        start.displacement_to(end),
                    ))
                } else {
                    let start = plan.chart_proposal(side.start_site);
                    let end = plan.chart_proposal(side.end_site);
                    ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
                }
            };
        edge_ranges.clear();
        let published = publish_native_computed_chain_with_edge_ranges(
            frame,
            ring.as_slice(),
            true,
            budget,
            source_context,
            edge_intent,
            |_, _| ComputedVertexIntent::Canonical,
            &mut edge_ranges,
        )?;
        let output = published.as_deref().unwrap_or(ring);
        if edge_ranges.len() != 3 {
            return Err(GeometryErrorKind::Invalid(
                "geographic CDT publisher lost an input side range".into(),
            )
            .into());
        }
        let spans = edge_ranges.as_slice();
        let first = *spans.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic CDT ring has no first side range".into())
        })?;
        let second = *spans.get(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic CDT ring has no second side range".into())
        })?;
        let last = *spans.get(2).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic CDT ring has no closing side range".into())
        })?;
        if first.0 != 0
            || first.1 != second.0
            || second.1 != last.0
            || last.1.checked_add(1) != Some(output.len())
        {
            return Err(GeometryErrorKind::Invalid(
                "geographic CDT side ranges do not form one closed triangle ring".into(),
            )
            .into());
        }
        let base = points.len();
        face_vertex_indices.push([
            base.checked_add(first.0)
                .ok_or(crate::error::ResourceError::SizeOverflow)?,
            base.checked_add(first.1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?,
            base.checked_add(second.1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?,
            base.checked_add(last.1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?,
        ]);
        points.extend_from_slice(output);
        ring_ends.push(points.len());
    }

    Ok((
        PublishedTriangleVertices {
            points,
            ring_ends,
            mesh_publication: None,
        },
        face_vertex_indices,
    ))
}

pub(crate) fn publish_mesh_triangle_vertex_stream_budgeted(
    mut publication: crate::geometry::PreparedMeshPublication,
    frame: GeographicFrame,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    budget: &mut ExpansionBudget,
) -> Result<PublishedTriangleVertices> {
    use crate::geometry::tessellation::MeshSiteLocation;

    let source_points = publication.take_points();
    if !source_points.len().is_multiple_of(4) || publication.faces.len() != source_points.len() / 4
    {
        return Err(GeometryErrorKind::Invalid(
            "prepared mesh face map does not match the triangle vertex stream".into(),
        )
        .into());
    }

    let mut points = Vec::with_capacity(source_points.len());
    let mut ring_ends = Vec::with_capacity(publication.faces.len());
    for (face_index, (face, ring)) in publication
        .faces
        .iter()
        .zip(source_points.as_chunks::<4>().0)
        .enumerate()
    {
        let start = face_index
            .checked_mul(4)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let end = start
            .checked_add(4)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        if face.output_vertices != (start..end) {
            return Err(GeometryErrorKind::Invalid(
                "prepared mesh face range does not match its triangle ring".into(),
            )
            .into());
        }
        match publish_native_computed_chain(
            frame,
            ring.as_slice(),
            true,
            budget,
            source_context,
            |output_side, _, _| {
                let boundary = face.boundary_uses.get(output_side).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "prepared mesh face is missing a directed side use".into(),
                    )
                })?;
                let start = publication.sites.get(boundary.start_site).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "prepared mesh side start is outside the site table".into(),
                    )
                })?;
                let end = publication.sites.get(boundary.end_site).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "prepared mesh side end is outside the site table".into(),
                    )
                })?;
                let (
                    MeshSiteLocation::Retained(start_chart),
                    MeshSiteLocation::Retained(end_chart),
                ) = (start.location, end.location)
                else {
                    return Err(GeometryErrorKind::Invalid(
                        "geographic mesh side lacks retained chart endpoints".into(),
                    )
                    .into());
                };
                Ok(ComputedEdgeIntent::retained_native(
                    start_chart.displacement_to(end_chart),
                ))
            },
            |_, _| ComputedVertexIntent::Canonical,
        )? {
            Some(published) => points.extend(published),
            None => points.extend_from_slice(ring),
        }
        ring_ends.push(points.len());
    }
    let mut published = PublishedTriangleVertices {
        points,
        ring_ends,
        mesh_publication: None,
    };
    published.mesh_publication = Some(publication);
    Ok(published)
}

pub(crate) fn publish_mesh_publication_for_domain(
    publication: crate::geometry::PreparedMeshPublication,
    domain: crate::geometry::TopologyDomain,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    budget: &mut ExpansionBudget,
) -> Result<PublishedTriangleVertices> {
    match domain {
        crate::geometry::TopologyDomain::Cartesian => {
            PublishedTriangleVertices::uniform_mesh(publication)
        },
        crate::geometry::TopologyDomain::Geographic(frame) => {
            publish_mesh_triangle_vertex_stream_budgeted(publication, frame, source_context, budget)
        },
    }
}

pub(crate) fn publish_earcut_mesh_publication_for_domain(
    mut publication: crate::geometry::PreparedMeshPublication,
    domain: crate::geometry::TopologyDomain,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    budget: &mut ExpansionBudget,
) -> Result<PublishedTriangleVertices> {
    let points = publication.take_points();
    let mut published = match domain {
        crate::geometry::TopologyDomain::Cartesian => PublishedTriangleVertices::uniform(points)?,
        crate::geometry::TopologyDomain::Geographic(frame) => {
            publish_triangle_vertex_stream_budgeted(&points, frame, source_context, budget)?
        },
    };
    published.mesh_publication = Some(publication);
    Ok(published)
}

pub(crate) fn publish_stored_computed_shape(
    shape: &Shape,
    frame: GeographicFrame,
    operation: &'static str,
) -> Result<Shape> {
    if let Shape::Point(point) = shape {
        return Ok(Shape::Point(
            frame
                .canonical_computed_point(*point)
                .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?,
        ));
    }
    let mut budget = ExpansionBudget::new(operation, "computed edges");
    budget.add(ShapeCountQuery::default().coordinates(shape)?)?;
    map_computed_shape_chains(shape, shape, |_, coords, closed| {
        publish_computed_chain(frame, coords, closed, &mut budget, |_, start, end| {
            ComputedEdgeIntent::retained_chart_connection(frame, [start.x, end.x])
        })
    })
}

fn publish_polygon_with_edge_roles(
    polygon: &Polygon,
    computed_edges: &[Vec<bool>],
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
) -> Result<Polygon> {
    publish_polygon_with_edge_roles_inner(polygon, computed_edges, frame, budget, None)
}

pub(crate) fn publish_polygon_with_edge_roles_and_context(
    polygon: &Polygon,
    computed_edges: &[Vec<bool>],
    frame: GeographicFrame,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    budget: &mut ExpansionBudget,
) -> Result<Polygon> {
    publish_polygon_with_edge_roles_inner(
        polygon,
        computed_edges,
        frame,
        budget,
        Some(source_context),
    )
}

fn publish_polygon_with_edge_roles_inner(
    polygon: &Polygon,
    computed_edges: &[Vec<bool>],
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
    source_context: Option<&SourceArcContext<'_, '_, crate::crs::GeodesicMetric>>,
) -> Result<Polygon> {
    if polygon.rings().count() != computed_edges.len() {
        return Err(GeometryErrorKind::message(
            "computed edge roles do not match the polygon rings",
        ));
    }
    let mut published_rings = Vec::with_capacity(computed_edges.len());
    for (ring, computed_edges) in polygon.rings().zip(computed_edges) {
        let points = ring.coords();
        if computed_edges.len() != points.coord_count().saturating_sub(1) {
            return Err(GeometryErrorKind::message(
                "computed edge roles do not match a closed ring",
            ));
        }
        if computed_edges.is_empty() {
            published_rings.push(ring.clone());
            continue;
        }
        let edge_count = computed_edges.len();
        let intent = |index, start: crate::geometry::Point, end: crate::geometry::Point| {
            if *computed_edges
                .get(index)
                .expect("publication edge belongs to its polygon role inventory")
            {
                ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
            } else {
                let delta = frame
                    .longitude()
                    .chart_step(start.x, end.x)
                    .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
                    .delta(frame.longitude());
                Ok(ComputedEdgeIntent::preserved(delta == 0.0))
            }
        };
        let vertex_intent = |index: usize, _: crate::geometry::Point| {
            let touches_preserved_edge = if index == 0 || index == edge_count {
                !*computed_edges.first().expect("nonempty role inventory")
                    || !*computed_edges.last().expect("nonempty role inventory")
            } else {
                !*computed_edges
                    .get(index.wrapping_sub(1))
                    .expect("incoming vertex edge belongs to its polygon role inventory")
                    || !*computed_edges
                        .get(index)
                        .expect("outgoing vertex edge belongs to its polygon role inventory")
            };
            if touches_preserved_edge {
                ComputedVertexIntent::Preserved
            } else {
                ComputedVertexIntent::Canonical
            }
        };
        let published = match source_context {
            Some(source_context) => publish_native_computed_chain(
                frame,
                points,
                true,
                budget,
                source_context,
                intent,
                vertex_intent,
            )?,
            None => publish_computed_chain_with_vertex_intents(
                frame,
                points,
                true,
                budget,
                intent,
                vertex_intent,
            )?,
        };
        published_rings.push(match published {
            Some(points) => Ring::try_from_closed(CoordSeq::try_from_points(&points)?)?,
            None => ring.clone(),
        });
    }
    let mut rings = published_rings.into_iter();
    let shell = rings
        .next()
        .ok_or_else(|| GeometryErrorKind::message("a polygon has an exterior ring"))?;
    Ok(Polygon::new(shell, rings.collect()))
}

pub(crate) fn publish_polygon_parts_with_edge_roles(
    polygons: Vec<Polygon>,
    edge_roles: &[Vec<Vec<bool>>],
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
) -> Result<Vec<Polygon>> {
    if polygons.len() != edge_roles.len() {
        return Err(GeometryErrorKind::message(
            "computed edge roles do not match the polygon parts",
        ));
    }
    polygons
        .into_iter()
        .zip(edge_roles)
        .map(|(polygon, roles)| publish_polygon_with_edge_roles(&polygon, roles, frame, budget))
        .collect()
}

pub(crate) fn publish_changed_raw_computed_shape<'metric, M: ChartMetric + 'metric>(
    source: &Shape,
    output: &Shape,
    frame: GeographicFrame,
    preserved_vertices: &[Vec<usize>],
    source_context: &SourceArcContext<'_, 'metric, M>,
    budget: &mut ExpansionBudget,
) -> Result<Shape> {
    let mut chain_index = 0_usize;
    let published = map_computed_shape_chains(source, output, |source, output, closed| {
        let preserved = preserved_vertices.get(chain_index).ok_or_else(|| {
            GeometryErrorKind::message("computed vertex provenance lost a coordinate chain")
        })?;
        chain_index = chain_index.wrapping_add(1);
        if same_coordinates(source, output) {
            return Ok(None);
        }
        if preserved.iter().any(|&index| index >= output.coord_count())
            || preserved
                .array_windows::<2>()
                .any(|[left, right]| left >= right)
        {
            return Err(GeometryErrorKind::message(
                "computed vertex provenance is not an ordered coordinate subset",
            ));
        }
        publish_native_computed_chain(
            frame,
            output,
            closed,
            budget,
            source_context,
            |_, start, end| {
                ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
            },
            |index, _| {
                if preserved.binary_search(&index).is_ok() {
                    ComputedVertexIntent::Preserved
                } else {
                    ComputedVertexIntent::Canonical
                }
            },
        )
    })?;
    if chain_index != preserved_vertices.len() {
        return Err(GeometryErrorKind::message(
            "computed vertex provenance references an unknown coordinate chain",
        ));
    }
    Ok(published)
}

pub(crate) fn publish_native_computed_chain<'intent, 'metric, P, M>(
    frame: GeographicFrame,
    points: &P,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
    source_context: &SourceArcContext<'_, 'metric, M>,
    intent: impl Fn(
        usize,
        crate::geometry::Point,
        crate::geometry::Point,
    ) -> Result<ComputedEdgeIntent<'intent>>,
    vertex_intent: impl Fn(usize, crate::geometry::Point) -> ComputedVertexIntent,
) -> Result<Option<Vec<crate::geometry::Point>>>
where
    P: crate::geometry::Coordinates + ?Sized,
    M: ChartMetric + 'metric,
{
    publish_native_computed_chain_inner(
        frame,
        points,
        closed_ring,
        budget,
        source_context,
        intent,
        vertex_intent,
        None,
    )
}

pub(crate) fn publish_native_computed_chain_with_edge_ranges<'intent, 'metric, P, M>(
    frame: GeographicFrame,
    points: &P,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
    source_context: &SourceArcContext<'_, 'metric, M>,
    intent: impl Fn(
        usize,
        crate::geometry::Point,
        crate::geometry::Point,
    ) -> Result<ComputedEdgeIntent<'intent>>,
    vertex_intent: impl Fn(usize, crate::geometry::Point) -> ComputedVertexIntent,
    edge_ranges: &mut Vec<(usize, usize)>,
) -> Result<Option<Vec<crate::geometry::Point>>>
where
    P: crate::geometry::Coordinates + ?Sized,
    M: ChartMetric + 'metric,
{
    publish_native_computed_chain_inner(
        frame,
        points,
        closed_ring,
        budget,
        source_context,
        intent,
        vertex_intent,
        Some(edge_ranges),
    )
}

fn publish_native_computed_chain_inner<'intent, 'metric, P, M>(
    frame: GeographicFrame,
    points: &P,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
    source_context: &SourceArcContext<'_, 'metric, M>,
    intent: impl Fn(
        usize,
        crate::geometry::Point,
        crate::geometry::Point,
    ) -> Result<ComputedEdgeIntent<'intent>>,
    vertex_intent: impl Fn(usize, crate::geometry::Point) -> ComputedVertexIntent,
    edge_ranges: Option<&mut Vec<(usize, usize)>>,
) -> Result<Option<Vec<crate::geometry::Point>>>
where
    P: crate::geometry::Coordinates + ?Sized,
    M: ChartMetric + 'metric,
{
    #[cfg(test)]
    crate::geometry::record_root_work(crate::geometry::RootWorkEvent::NativeChainEntry);
    let metric_access = source_context.metric_access();
    let axes = points
        .first_coord()
        .map_or(crate::geometry::CoordinateAxes::XY, |point| point.axes);
    let mut active_root = None;
    let mut root_metric_state = RootMetricState::default();
    let mut work = Unmetered;
    publish_computed_chain_with_vertex_intents_and_samples_with_edge_ranges(
        frame,
        points,
        closed_ring,
        budget,
        intent,
        vertex_intent,
        |root, endpoints, relation, parameter, _retained| {
            if active_root != Some(root) {
                active_root = Some(root);
                root_metric_state = RootMetricState::default();
            }
            let ComputedLongitudeRelation::Native(longitude) = *relation else {
                return Err(GeometryErrorKind::message(
                    "computed Native output requires a retained Native chart edge",
                ));
            };
            computed_native_sample_ordinates(
                frame,
                endpoints,
                longitude,
                parameter,
                axes,
                || {
                    let metric_access = metric_access.ok_or_else(|| {
                        crate::crs::CrsError::invalid(
                            "geographic computed-edge source ordinates require the operation's CRS metric",
                        )
                    })?;
                    metric_access()
                },
                &mut root_metric_state,
                &mut work,
            )
        },
        |_, _, _, _, _, _| Ok(()),
        edge_ranges,
    )
}

fn same_coordinates(left: &CoordSeq, right: &CoordSeq) -> bool {
    left.coord_count() == right.coord_count()
        && (0..left.coord_count()).all(|index| left.nth_coord(index) == right.nth_coord(index))
}

fn map_shape_chains(
    source: &Shape,
    output: &Shape,
    map: &mut impl FnMut(&CoordSeq, &CoordSeq, bool) -> Result<Option<Vec<crate::geometry::Point>>>,
) -> Result<Shape> {
    Ok(match (source, output) {
        (Shape::LineString(source), Shape::LineString(output)) => Shape::LineString(map_line(
            source,
            output,
            source.len() > 2 && source.first() == source.last(),
            map,
        )?),
        (Shape::MultiLineString(source), Shape::MultiLineString(output)) => {
            let mut source_lines = source.iter();
            let mapped = output.try_map_values(output.axes(), |line| {
                let source = source_lines.next().ok_or_else(|| {
                    GeometryErrorKind::message(
                        "computed output changed a multiline coordinate structure",
                    )
                })?;
                map_line(
                    source,
                    line,
                    source.len() > 2 && source.first() == source.last(),
                    map,
                )
            })?;
            if source_lines.next().is_some() {
                return Err(GeometryErrorKind::message(
                    "computed output changed a multiline coordinate structure",
                ));
            }
            Shape::MultiLineString(mapped)
        },
        (Shape::Polygon(source), Shape::Polygon(output)) => {
            Shape::Polygon(map_polygon(source, output, map)?)
        },
        (Shape::MultiPolygon(source), Shape::MultiPolygon(output)) => {
            let mut source_polygons = source.iter();
            let mapped = output.try_map_values(output.axes(), |polygon| {
                let source = source_polygons.next().ok_or_else(|| {
                    GeometryErrorKind::message(
                        "computed output changed a multipolygon coordinate structure",
                    )
                })?;
                map_polygon(source, polygon, map)
            })?;
            if source_polygons.next().is_some() {
                return Err(GeometryErrorKind::message(
                    "computed output changed a multipolygon coordinate structure",
                ));
            }
            Shape::MultiPolygon(mapped)
        },
        (Shape::GeometryCollection(source), Shape::GeometryCollection(output)) => {
            let mut source_members = source.iter();
            let mapped = output.try_map_members(|member| {
                let source = source_members.next().ok_or_else(|| {
                    GeometryErrorKind::message(
                        "computed output changed a geometry-collection structure",
                    )
                })?;
                map_shape_chains(source, member, map)
            })?;
            if source_members.next().is_some() {
                return Err(GeometryErrorKind::message(
                    "computed output changed a geometry-collection structure",
                ));
            }
            Shape::GeometryCollection(mapped)
        },
        (Shape::Point(_), Shape::Point(_))
        | (Shape::MultiPoint(_), Shape::MultiPoint(_))
        | (Shape::Empty(..), Shape::Empty(..)) => output.clone(),
        _ => {
            return Err(GeometryErrorKind::message(
                "computed output changed geometry structure",
            ));
        },
    })
}

fn map_line(
    source: &LineSeq,
    output: &LineSeq,
    closed: bool,
    map: &mut impl FnMut(&CoordSeq, &CoordSeq, bool) -> Result<Option<Vec<crate::geometry::Point>>>,
) -> Result<LineSeq> {
    let coords = map(source.as_coords(), output.as_coords(), closed)?;
    coords.map_or_else(
        || Ok(output.clone()),
        |points| Ok(LineSeq::from_proven(CoordSeq::try_from_points(&points)?)),
    )
}

fn map_polygon(
    source: &Polygon,
    output: &Polygon,
    map: &mut impl FnMut(&CoordSeq, &CoordSeq, bool) -> Result<Option<Vec<crate::geometry::Point>>>,
) -> Result<Polygon> {
    let mut source_rings = source.rings();
    let mut rings = Vec::with_capacity(output.rings().count());
    for ring in output.rings() {
        let source_ring = source_rings.next().ok_or_else(|| {
            GeometryErrorKind::message("computed output changed a polygon ring structure")
        })?;
        rings.push(map_ring(source_ring, ring, map)?);
    }
    if source_rings.next().is_some() {
        return Err(GeometryErrorKind::message(
            "computed output changed a polygon ring structure",
        ));
    }
    let mut rings = rings.into_iter();
    let shell = rings
        .next()
        .ok_or_else(|| GeometryErrorKind::message("a polygon has an exterior ring"))?;
    Ok(Polygon::new(shell, rings.collect()))
}

fn map_ring(
    source: &Ring,
    output: &Ring,
    map: &mut impl FnMut(&CoordSeq, &CoordSeq, bool) -> Result<Option<Vec<crate::geometry::Point>>>,
) -> Result<Ring> {
    map(source.coords(), output.coords(), true)?.map_or_else(
        || Ok(output.clone()),
        |points| Ring::try_from_closed(CoordSeq::try_from_points(&points)?),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        TransformedChartEdgeProvider, map_transformed_shape_chains,
        publish_changed_raw_computed_shape, publish_polygon_with_edge_roles,
        publish_raw_computed_shape, publish_transformed_shape,
        publish_triangle_vertex_stream_budgeted,
    };
    use crate::crs::frame::{Frame, GeometryTransformFrame, crs_arc_static};
    use crate::geographic::GeographicFrame;
    use crate::geometry::{
        CoordSeq, CoordWindow, CoordinateAxes, Coordinates as _, ExpansionBudget, LineSeq,
        MOrdinate, Point, Polygon, Ring, Shape, SmoothMethod, SourceArcContext, TopologyDomain,
        ZOrdinate,
    };

    fn geographic_transform_plan() -> crate::crs::TransformPlan {
        let source = Frame::Crs(crs_arc_static("EPSG:4326"));
        let frame = GeometryTransformFrame::new(
            &source,
            crs_arc_static("EPSG:4326"),
            None,
            crate::crs::TransformOptions::default(),
        )
        .unwrap();
        crate::crs::TransformPlan::from_geometry_frame(frame).unwrap()
    }

    #[test]
    fn transformed_shape_chain_provider_uses_each_original_endpoint_pair() {
        let frame = GeographicFrame::crs84();
        let source = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(10.0, 0.0),
                    Point::new_raw_xy(370.0, 0.0),
                    Point::new_raw_xy(370.0, 1.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let output = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(-180.0, 0.0),
                    Point::new_raw_xy(-180.0, 1.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );

        let published =
            map_transformed_shape_chains(&source, &output, frame, frame, |edges, closed| {
                assert!(!closed);
                assert_eq!(edges.edge_count(), 2);
                assert_eq!(edges.source().nth_coord(0).x, 10.0);
                assert_eq!(edges.output().nth_coord(0).x, 0.0);
                assert_eq!(edges.retained_connection(0)?.periods(), 2);
                assert_eq!(edges.retained_connection(1)?.periods(), 0);
                Ok(None)
            })
            .unwrap();

        assert_eq!(published, output);
    }

    #[test]
    fn transformed_shape_chain_provider_keeps_collection_member_correspondence() {
        let frame = GeographicFrame::crs84();
        let source = Shape::geometry_collection(vec![
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(10.0, 0.0),
                        Point::new_raw_xy(370.0, 0.0),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            ),
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(20.0, 0.0),
                        Point::new_raw_xy(20.0, 1.0),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            ),
        ])
        .unwrap();
        let output = Shape::geometry_collection(vec![
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.0, 0.0),
                        Point::new_raw_xy(0.0, 0.0),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            ),
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.0, 0.0),
                        Point::new_raw_xy(-180.0, 1.0),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            ),
        ])
        .unwrap();
        let mut lifts = Vec::new();

        let published =
            map_transformed_shape_chains(&source, &output, frame, frame, |edges, closed| {
                assert!(!closed);
                lifts.push(edges.retained_connection(0)?.periods());
                Ok(None)
            })
            .unwrap();

        assert_eq!(lifts, [1, 0]);
        assert_eq!(published, output);
    }

    #[test]
    fn transformed_packed_windows_keep_gathered_and_duplicate_source_pairing() {
        let frame = GeographicFrame::crs84();
        let source = CoordSeq::try_from_points(&[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(360.0, 0.0),
            Point::new_raw_xy(10.0, 0.0),
            Point::new_raw_xy(10.0, 0.0),
        ])
        .unwrap();
        let output = CoordSeq::try_from_points(&[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(-180.0, 0.0),
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(-180.0, 0.0),
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(-180.0, 0.0),
        ])
        .unwrap();
        let source_windows = [2..4, 0..2, 0..2];
        let output_windows = [0..2, 2..4, 4..6];

        let periods = source_windows
            .into_iter()
            .zip(output_windows)
            .map(|(source_window, output_window)| {
                let source = source.view(CoordWindow::from_range(source_window, source.len()));
                let output = output.view(CoordWindow::from_range(output_window, output.len()));
                TransformedChartEdgeProvider::try_new(&source, &output, frame, frame)?
                    .retained_connection(0)
                    .map(|edge| edge.periods())
            })
            .collect::<crate::error::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(periods, [0, 2, 2]);
    }

    fn publish_xy_transform_line(source_x: [f64; 2], output_x: [f64; 2]) -> Vec<Point> {
        let frame = GeographicFrame::crs84();
        let source = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(source_x[0], 0.0),
                    Point::new_raw_xy(source_x[1], 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let output = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(output_x[0], 0.0),
                    Point::new_raw_xy(output_x[1], 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let plan = geographic_transform_plan();
        let mut metric = plan.target_metric_scratch();
        let Shape::LineString(line) =
            publish_transformed_shape(&source, &output, Some(frame), frame, "to_crs", &mut metric)
                .unwrap()
        else {
            panic!("transformed line publication retains line structure");
        };
        line.iter().collect()
    }

    #[test]
    fn transformed_circle_reaches_the_native_publisher_with_its_forward_lift() {
        assert_eq!(publish_xy_transform_line([10.0, 370.0], [0.0, -180.0]), [
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(135.0, 0.0),
            Point::new_raw_xy(-90.0, 0.0),
            Point::new_raw_xy(45.0, 0.0),
            Point::new_raw_xy(-180.0, 0.0),
        ]);
    }

    #[test]
    fn transformed_circle_reaches_the_native_publisher_with_its_reverse_lift() {
        assert_eq!(publish_xy_transform_line([370.0, 10.0], [-180.0, 0.0]), [
            Point::new_raw_xy(-180.0, 0.0),
            Point::new_raw_xy(45.0, 0.0),
            Point::new_raw_xy(-90.0, 0.0),
            Point::new_raw_xy(135.0, 0.0),
            Point::new_raw_xy(0.0, 0.0),
        ]);
    }

    #[test]
    fn raw_computed_shape_publication_subdivides_the_generated_line() {
        let source = Shape::LineString(
            LineSeq::try_new(
                crate::geometry::CoordSeq::try_from_points(&[
                    Point::new_raw_xy(10.0, 20.0),
                    Point::new_raw_xy(260.0, 40.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let output =
            publish_raw_computed_shape(&source, GeographicFrame::crs84(), "computed_line").unwrap();
        let Shape::LineString(line) = output else {
            panic!("computed line remains lineal");
        };
        let points = line.as_coords().iter().collect::<Vec<_>>();
        assert_eq!(points, [
            Point::new_raw_xy(10.0, 20.0),
            Point::new_raw_xy(135.0, 30.0),
            Point::new_raw_xy(-100.0, 40.0),
        ]);
    }

    #[test]
    fn triangle_stream_publisher_carries_inserted_native_samples_by_ellipsoidal_arclength() {
        const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let model = crate::crs::metric_model_at(Some(CRS), &state).unwrap();
        let crate::crs::ResolvedMetric::Geodesic(metric) =
            crate::crs::ResolvedMetric::from_model(&model).unwrap()
        else {
            panic!("the custom longlat CRS has a geodesic metric");
        };
        let frame = metric.frame;
        let first =
            Point::new_axes(760.0, 45.0, ZOrdinate(Some(35.25)), MOrdinate(Some(551.0))).unwrap();
        let second =
            Point::new_axes(1035.0, 72.5, ZOrdinate(Some(49.5)), MOrdinate(Some(726.0))).unwrap();
        let third =
            Point::new_axes(900.0, 90.0, ZOrdinate(Some(61.0)), MOrdinate(Some(901.0))).unwrap();
        let vertices = [first, second, third, first];

        let metric_accesses = std::sync::atomic::AtomicUsize::new(0);
        let metric_access = || {
            metric_accesses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(&metric)
        };
        let context =
            SourceArcContext::new(TopologyDomain::Geographic(frame), Some(&metric_access));
        let mut budget = ExpansionBudget::new("triangulate", "method");
        budget.add(vertices.len()).unwrap();
        let published =
            publish_triangle_vertex_stream_budgeted(&vertices, frame, &context, &mut budget)
                .unwrap();

        assert_eq!(published.ring_ends, [5]);
        assert_eq!(
            metric_accesses.load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            published.points.first().copied(),
            Some(
                Point::new_axes(40.0, 45.0, ZOrdinate(Some(35.25)), MOrdinate(Some(551.0)))
                    .unwrap()
            )
        );
        assert_eq!(published.points.last(), published.points.first());
        let sample = published
            .points
            .iter()
            .find(|point| {
                point.x.to_bits() == 177.5_f64.to_bits() && point.y.to_bits() == 58.75_f64.to_bits()
            })
            .expect("the 275-degree triangle edge is subdivided at its midpoint");
        assert!((sample.z().unwrap() - 43.730_417_185_196_416).abs() <= 2.0e-8);
        assert!((sample.m().unwrap() - 655.145_474_204_166_5).abs() <= 2.0e-7);

        let xy_vertices = [
            Point::new_raw_xy(760.0, 45.0),
            Point::new_raw_xy(1035.0, 72.5),
            Point::new_raw_xy(900.0, 90.0),
            Point::new_raw_xy(760.0, 45.0),
        ];
        let xy_metric_accesses = std::sync::atomic::AtomicUsize::new(0);
        let xy_metric_access = || {
            xy_metric_accesses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(&metric)
        };
        let xy_context =
            SourceArcContext::new(TopologyDomain::Geographic(frame), Some(&xy_metric_access));
        let mut xy_budget = ExpansionBudget::new("triangulate", "method");
        xy_budget.add(xy_vertices.len()).unwrap();
        let xy_published = publish_triangle_vertex_stream_budgeted(
            &xy_vertices,
            frame,
            &xy_context,
            &mut xy_budget,
        )
        .unwrap();
        assert_eq!(
            xy_metric_accesses.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            published
                .points
                .iter()
                .map(|point| (point.x, point.y))
                .collect::<Vec<_>>(),
            xy_published
                .points
                .iter()
                .map(|point| (point.x, point.y))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn smooth_publication_preserves_copied_knots_and_canonicalizes_samples() {
        let source_points = [
            Point::new_raw_xy(370.0, 0.0),
            Point::new_raw_xy(500.0, 10.0),
            Point::new_raw_xy(620.0, 0.0),
        ];
        let source = Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_points(&source_points).unwrap()).unwrap(),
        );
        let mut budget = ExpansionBudget::new("smooth", "iterations");
        let (smoothed, preserved_vertices) = source
            .smooth_budgeted_with_vertex_intents(2, SmoothMethod::CatmullRom, true, &mut budget)
            .unwrap();
        let Shape::LineString(smoothed_line) = &smoothed else {
            panic!("smoothing retains its line structure");
        };
        assert_eq!(preserved_vertices, [vec![0, 4, 8]]);

        let published = super::publish_changed_raw_computed_shape(
            &source,
            &smoothed,
            GeographicFrame::crs84(),
            &preserved_vertices,
            &SourceArcContext::cartesian(),
            &mut budget,
        )
        .unwrap();
        let Shape::LineString(published_line) = published else {
            panic!("publication retains its line structure");
        };
        assert_eq!(published_line.len(), smoothed_line.len());
        let output = published_line.as_coords().iter().collect::<Vec<_>>();
        for point in source_points {
            assert!(
                output.contains(&point),
                "preserved knot {point:?} was rewritten"
            );
        }
        for point in output {
            if !source_points.contains(&point) {
                assert!(
                    (-180.0..180.0).contains(&point.x),
                    "computed sample was not canonical: {point:?}"
                );
            }
        }
    }

    #[test]
    fn smooth_publisher_carries_inserted_native_chord_samples_by_ellipsoidal_arclength() {
        const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let model = crate::crs::metric_model_at(Some(CRS), &state).unwrap();
        let crate::crs::ResolvedMetric::Geodesic(metric) =
            crate::crs::ResolvedMetric::from_model(&model).unwrap()
        else {
            panic!("the custom longlat CRS has a geodesic metric");
        };
        let frame = metric.frame;
        let source = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_axes(10.0, -30.0, ZOrdinate(Some(-0.0)), MOrdinate(Some(101.0)))
                        .unwrap(),
                    Point::new_axes(1010.0, 70.0, ZOrdinate(Some(47.0)), MOrdinate(Some(701.0)))
                        .unwrap(),
                    Point::new_axes(1110.0, 80.0, ZOrdinate(Some(57.0)), MOrdinate(Some(801.0)))
                        .unwrap(),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let mut budget = ExpansionBudget::new("smooth", "iterations");
        let (smoothed, preserved_vertices) = source
            .smooth_budgeted_with_vertex_intents(1, SmoothMethod::Chaikin, true, &mut budget)
            .unwrap();
        let Shape::LineString(smoothed_line) = &smoothed else {
            panic!("smoothing retains its line structure");
        };
        assert_eq!(smoothed_line.len(), 4);
        assert_eq!(preserved_vertices, [vec![0, 3]]);

        let metric_accesses = std::sync::atomic::AtomicUsize::new(0);
        let metric_access = || {
            metric_accesses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(&metric)
        };
        let context =
            SourceArcContext::new(TopologyDomain::Geographic(frame), Some(&metric_access));
        let published = publish_changed_raw_computed_shape(
            &source,
            &smoothed,
            frame,
            &preserved_vertices,
            &context,
            &mut budget,
        )
        .unwrap();
        let Shape::LineString(published_line) = published else {
            panic!("publication retains its line structure");
        };
        let points = published_line.as_coords().iter().collect::<Vec<_>>();
        assert!(points.len() > smoothed_line.len());
        assert!(metric_accesses.load(std::sync::atomic::Ordering::Relaxed) > 0);
        assert_eq!(points[0].x, 10.0);
        assert_eq!(points[0].z().unwrap().to_bits(), (-0.0_f64).to_bits());
        assert_eq!(points[0].m(), Some(101.0));
        assert_eq!(points.last().unwrap().x, 1110.0);
        assert_eq!(points.last().unwrap().z(), Some(57.0));
        assert_eq!(points.last().unwrap().m(), Some(801.0));

        let xy_source = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(10.0, -30.0),
                    Point::new_raw_xy(1010.0, 70.0),
                    Point::new_raw_xy(1110.0, 80.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let mut xy_budget = ExpansionBudget::new("smooth", "iterations");
        let (xy_smoothed, xy_preserved) = xy_source
            .smooth_budgeted_with_vertex_intents(1, SmoothMethod::Chaikin, true, &mut xy_budget)
            .unwrap();
        let xy_metric_accesses = std::sync::atomic::AtomicUsize::new(0);
        let xy_metric_access = || {
            xy_metric_accesses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(&metric)
        };
        let xy_context =
            SourceArcContext::new(TopologyDomain::Geographic(frame), Some(&xy_metric_access));
        let xy_published = publish_changed_raw_computed_shape(
            &xy_source,
            &xy_smoothed,
            frame,
            &xy_preserved,
            &xy_context,
            &mut xy_budget,
        )
        .unwrap();
        let Shape::LineString(xy_line) = xy_published else {
            panic!("XY publication retains its line structure");
        };
        assert_eq!(
            xy_metric_accesses.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            points
                .iter()
                .map(|point| (point.x, point.y))
                .collect::<Vec<_>>(),
            xy_line
                .as_coords()
                .iter()
                .map(|point| (point.x, point.y))
                .collect::<Vec<_>>()
        );
        assert_eq!(xy_line.axes(), CoordinateAxes::XY);

        let sample = points
            .iter()
            .find(|point| (point.x - 177.5).abs() <= 1e-12 && (point.y - 58.75).abs() <= 1e-12)
            .expect("the 275-degree generated edge is subdivided at its midpoint");
        assert!(
            (sample.z().unwrap() - 43.730_417_185_196_416).abs() <= 2.0e-8,
            "inserted Z={:?}",
            sample.z()
        );
        assert!(
            (sample.m().unwrap() - 655.145_474_204_166_5).abs() <= 2.0e-7,
            "inserted M={:?}",
            sample.m()
        );
        assert!(
            (sample.z().unwrap() - 42.375).abs() > 1.0,
            "chart-linear Z would have been 42.375"
        );
    }

    #[test]
    fn polygon_edge_roles_preserve_clip_fragments_and_publish_bisectors() {
        let source = Polygon::new(
            Ring::try_from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(370.0, 0.0),
                    Point::new_raw_xy(0.0, 5.0),
                    Point::new_raw_xy(250.0, 10.0),
                    Point::new_raw_xy(250.0, 30.0),
                    Point::new_raw_xy(370.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        );
        let mut budget = ExpansionBudget::new("voronoi", "computed edges");
        let output = publish_polygon_with_edge_roles(
            &source,
            &[vec![false, true, true, false]],
            GeographicFrame::crs84(),
            &mut budget,
        )
        .unwrap();
        assert_eq!(output.shell.coords().iter().collect::<Vec<_>>(), [
            Point::new_raw_xy(370.0, 0.0),
            Point::new_raw_xy(0.0, 5.0),
            Point::new_raw_xy(125.0, 7.5),
            Point::new_raw_xy(250.0, 10.0),
            Point::new_raw_xy(250.0, 30.0),
            Point::new_raw_xy(370.0, 0.0),
        ]);
    }
}
