use std::collections::hash_map::Entry;

use crate::collections::HashMap;
use crate::error::Result;
use crate::geographic::GeographicInputs;
use crate::geometry::ordinates::SourceArcContext;
use crate::geometry::overlay::{ExactSourceInventory, overlay_view_error};
use crate::geometry::topology::overlay::exact_geographic_boundary;
use crate::geometry::{
    CoordSeq, CoordSeqBuilder, CoordinateAxes, Coordinates, ExpansionBudget,
    GeographicTopologyRoute, GeometryKind, HashMapExt as _, LineSeq, Point, Polygon, PreparedPlan,
    Ring, Shape, TopologyDomain, VertexKey, empty_geometry, geographic_topology_route,
};

pub(crate) fn boundary_with_metric<M: crate::geometry::ChartMetric>(
    data: &PreparedPlan,
    context: &SourceArcContext<'_, '_, M>,
) -> Result<Shape> {
    let domain = context.domain();
    match data.shape() {
        Shape::Point(_) | Shape::MultiPoint(_) => Ok(empty_geometry()),
        Shape::Empty(
            GeometryKind::Point | GeometryKind::GeometryCollection | GeometryKind::MultiPoint,
            axes,
        ) => Ok(Shape::typed_empty(GeometryKind::GeometryCollection, *axes)),
        Shape::Empty(GeometryKind::LineString | GeometryKind::MultiLineString, axes) => {
            Ok(Shape::typed_empty(GeometryKind::MultiPoint, *axes))
        },
        Shape::Empty(GeometryKind::Polygon | GeometryKind::MultiPolygon, axes) => {
            Ok(Shape::typed_empty(GeometryKind::MultiLineString, *axes))
        },
        Shape::LineString(points) => line_boundary(points, points.axes(), domain),
        Shape::MultiLineString(lines) => multiline_boundary(lines, lines.axes(), domain),
        Shape::Polygon(_) | Shape::MultiPolygon(_) => polygonal_boundary(data, context),
        Shape::GeometryCollection(geometries) => {
            let mut boundaries = Vec::new();
            for geometry in geometries {
                let plan = PreparedPlan::transient(geometry);
                let boundary = boundary_with_metric(&plan, context)?;
                if !boundary.is_empty() {
                    boundaries.push(boundary);
                }
            }
            if boundaries.is_empty() {
                Ok(Shape::typed_empty(
                    GeometryKind::GeometryCollection,
                    data.shape().axes(),
                ))
            } else {
                Ok(Shape::GeometryCollection(
                    geometries.try_rebuild_members(boundaries)?,
                ))
            }
        },
    }
}

fn polygonal_boundary<M: crate::geometry::ChartMetric>(
    data: &PreparedPlan,
    context: &SourceArcContext<'_, '_, M>,
) -> Result<Shape> {
    let shape = data.shape();
    match context.domain() {
        TopologyDomain::Cartesian => {
            if data.validate_cached()?.is_none() {
                return Ok(literal_polygon_boundary(shape));
            }
            exact_partition_boundary(shape, context)
        },
        TopologyDomain::Geographic(frame) => {
            let inputs = GeographicInputs::try_from_shapes(frame, std::iter::once(shape))
                .map_err(|error| overlay_view_error(&error))?;
            if geographic_topology_route(inputs.views().iter().copied())
                == GeographicTopologyRoute::Quotient
            {
                let topology = crate::geometry::ExactGeographicTopology::from_views(
                    frame,
                    inputs.views().iter().copied(),
                )?;
                return exact_geographic_boundary(&topology, shape.axes(), context.metric_access());
            }
            if data.validate_cached()?.is_none() {
                return Ok(literal_polygon_boundary(shape));
            }
            exact_partition_boundary(shape, context)
        },
    }
}

fn exact_partition_boundary<M: crate::geometry::ChartMetric>(
    shape: &Shape,
    context: &SourceArcContext<'_, '_, M>,
) -> Result<Shape> {
    let inventory = ExactSourceInventory::from_polygonal_union(&[shape])?
        .expect("the boundary owner receives polygonal input");
    let mut budget = ExpansionBudget::new("boundary", "exact partition");
    let mut partition = inventory.partition(&mut budget)?;
    if let TopologyDomain::Geographic(frame) = context.domain() {
        partition.set_geographic_frame(frame);
    }
    inventory.occupied_boundary_shape(&partition, shape.axes(), context)
}

fn literal_polygon_boundary(shape: &Shape) -> Shape {
    match shape {
        Shape::Polygon(polygon) => {
            rings_to_boundary(polygon.rings().map(Ring::as_line).collect(), polygon.axes())
        },
        Shape::MultiPolygon(polygons) => rings_to_boundary(
            polygons
                .iter()
                .flat_map(Polygon::rings)
                .map(Ring::as_line)
                .collect(),
            polygons.axes(),
        ),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => {
            unreachable!("literal area boundary requires a polygonal shape")
        },
    }
}

fn rings_to_boundary(mut rings: Vec<LineSeq>, empty_axes: CoordinateAxes) -> Shape {
    let axes = rings
        .first()
        .map_or(empty_axes, |ring| ring.as_coords().axes());
    match rings.len() {
        0 => Shape::typed_empty(GeometryKind::MultiLineString, empty_axes),
        1 => Shape::LineString(rings.remove(0)),
        _ => Shape::multi_line_string(axes, rings),
    }
}

fn line_boundary_points<C: Coordinates + ?Sized>(
    points: &C,
    domain: TopologyDomain,
) -> Option<[Point; 2]> {
    let len = points.coord_count();
    if len < 2 {
        return None;
    }
    let start = points.nth_coord(0);
    let end = points.nth_coord(len.wrapping_sub(1));
    (VertexKey::new(start, domain) != VertexKey::new(end, domain)).then_some([start, end])
}

fn line_boundary<C: Coordinates + ?Sized>(
    points: &C,
    empty_axes: CoordinateAxes,
    domain: TopologyDomain,
) -> Result<Shape> {
    let Some(endpoints) = line_boundary_points(points, domain) else {
        return Ok(Shape::typed_empty(GeometryKind::MultiPoint, empty_axes));
    };
    Ok(Shape::multi_point(
        empty_axes,
        CoordSeq::try_from_points(&endpoints)?,
    ))
}

fn multiline_boundary<L: AsRef<CoordSeq>>(
    lines: &[L],
    empty_axes: CoordinateAxes,
    domain: TopologyDomain,
) -> Result<Shape> {
    let endpoint_capacity = lines.len().checked_mul(2).unwrap_or(lines.len());
    let mut endpoints: Vec<(Point, bool)> = Vec::with_capacity(endpoint_capacity);
    let mut slots: HashMap<VertexKey, usize> = HashMap::with_capacity(endpoint_capacity);
    for line in lines {
        let Some([start, end]) = line_boundary_points(line.as_ref(), domain) else {
            continue;
        };
        for point in [start, end] {
            match slots.entry(VertexKey::new(point, domain)) {
                Entry::Occupied(slot) => {
                    let endpoint = endpoints
                        .get_mut(*slot.get())
                        .expect("each endpoint slot names its inserted vector entry");
                    endpoint.1 = !endpoint.1;
                },
                Entry::Vacant(slot) => {
                    slot.insert(endpoints.len());
                    endpoints.push((point, true));
                },
            }
        }
    }
    let output_count = endpoints.iter().filter(|(_, odd)| *odd).count();
    let mut coordinates = CoordSeqBuilder::with_capacity(empty_axes, output_count);
    for (point, odd) in endpoints {
        if odd {
            coordinates.push(point);
        }
    }
    Ok(Shape::multi_point(empty_axes, coordinates.finish()?))
}
