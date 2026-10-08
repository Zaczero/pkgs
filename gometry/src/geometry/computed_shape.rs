use crate::error::Result;
use crate::geographic::{
    ComputedEdgeIntent, ComputedVertexIntent, GeographicFrame, publish_computed_chain,
    publish_computed_chain_with_vertex_intents,
};
use crate::geometry::{
    CoordSeq, CoordSeqBuilder, CoordWindow, Coordinates as _, CsrOffsetBuilder, CsrOffsetColumn,
    ExpansionBudget, GeometryErrorKind, LineSeq, Polygon, Ring, Shape, ShapeCountQuery,
};

pub(crate) type PolygonPartEdgeRoles = Vec<Vec<Vec<bool>>>;

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

pub(crate) fn publish_transformed_shape(
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
        Shape::MultiPoint(points) => Ok(Shape::MultiPoint(points.try_map_values(|point| {
            frame
                .canonical_computed_point(point)
                .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))
        })?)),
        Shape::GeometryCollection(collection) => Ok(Shape::GeometryCollection(
            collection
                .try_map_members(|member| publish_transformed_shape(member, frame, operation))?,
        )),
        Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_) => publish_raw_computed_shape(shape, frame, operation),
        Shape::Empty(..) => Ok(shape.clone()),
    }
}

pub(crate) fn publish_raw_computed_shapes(
    shapes: &[Shape],
    frame: GeographicFrame,
    operation: &'static str,
) -> Result<Vec<Shape>> {
    let mut budget = ExpansionBudget::new(operation, "computed edges");
    let mut counts = ShapeCountQuery::default();
    for shape in shapes {
        budget.add(counts.coordinates(shape)?)?;
    }
    publish_raw_computed_shapes_with_budget(shapes, frame, &mut budget)
}

pub(crate) fn publish_raw_computed_shapes_with_budget(
    shapes: &[Shape],
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
) -> Result<Vec<Shape>> {
    shapes
        .iter()
        .map(|shape| publish_raw_computed_shape_with_budget(shape, frame, budget))
        .collect()
}

pub(crate) fn publish_voronoi_polygon_outputs(
    outputs: Vec<crate::geometry::VoronoiPolygonOutput>,
    frame: Option<GeographicFrame>,
    budget: &mut ExpansionBudget,
) -> Result<Vec<Shape>> {
    outputs
        .into_iter()
        .map(|output| {
            let polygon = match frame {
                Some(frame) => publish_polygon_with_edge_roles(
                    &output.polygon,
                    &output.computed_edges,
                    frame,
                    budget,
                )?,
                None => output.polygon,
            };
            Ok(Shape::Polygon(polygon))
        })
        .collect()
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

pub(crate) fn publish_computed_polygon_rings_with_budget(
    coordinates: &CoordSeq,
    source_ring_offsets: &CsrOffsetColumn,
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
) -> Result<(CoordSeq, CsrOffsetColumn)> {
    let mut output = CoordSeqBuilder::with_capacity(coordinates.axes(), coordinates.len());
    let mut ring_offsets =
        CsrOffsetBuilder::with_capacity(source_ring_offsets.len().saturating_sub(1));
    for pair in source_ring_offsets.as_slice().array_windows::<2>() {
        let start = usize::try_from(pair[0]).map_err(|_| GeometryErrorKind::MalformedCsrOffsets)?;
        let end = usize::try_from(pair[1]).map_err(|_| GeometryErrorKind::MalformedCsrOffsets)?;
        let window = coordinates.view(CoordWindow::from_range(start..end, coordinates.len()));
        let published = publish_computed_chain(frame, &window, true, budget, |_, left, right| {
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
        ring_offsets.push_end(output.len(), i32::MAX as usize)?;
    }
    let coordinates = output.finish()?;
    let ring_offsets = ring_offsets.finish(coordinates.len())?;
    Ok((coordinates, ring_offsets))
}

pub(crate) struct PublishedTriangleVertices {
    pub(crate) points: Vec<crate::geometry::Point>,
    pub(crate) ring_ends: Vec<usize>,
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
        Ok(Self { points, ring_ends })
    }
}

pub(crate) fn publish_triangle_vertex_stream_budgeted(
    vertices: &[crate::geometry::Point],
    frame: GeographicFrame,
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
        match publish_computed_chain(frame, ring.as_slice(), true, budget, |_, start, end| {
            ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
        })? {
            Some(published) => points.extend(published),
            None => points.extend_from_slice(ring),
        }
        ring_ends.push(points.len());
    }
    Ok(PublishedTriangleVertices { points, ring_ends })
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

pub(crate) fn publish_polygon_with_edge_roles(
    polygon: &Polygon,
    computed_edges: &[Vec<bool>],
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
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
        let published = publish_computed_chain_with_vertex_intents(
            frame,
            points,
            true,
            budget,
            |index, start, end| {
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
            },
            |index, _| {
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
            },
        )?;
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

pub(crate) fn publish_changed_raw_computed_shape(
    source: &Shape,
    output: &Shape,
    frame: GeographicFrame,
    preserved_vertices: &[Vec<usize>],
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
        publish_computed_chain_with_vertex_intents(
            frame,
            output,
            closed,
            budget,
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
    use super::{publish_polygon_with_edge_roles, publish_raw_computed_shape};
    use crate::geographic::GeographicFrame;
    use crate::geometry::{
        CoordSeq, ExpansionBudget, LineSeq, Point, Polygon, Ring, Shape, SmoothMethod,
    };

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
