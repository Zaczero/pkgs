use super::*;

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

fn publish_raw_computed_shape_with_budget(
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

fn same_coordinates(left: &CoordSeq, right: &CoordSeq) -> bool {
    left.coord_count() == right.coord_count()
        && (0..left.coord_count()).all(|index| left.nth_coord(index) == right.nth_coord(index))
}
