use super::{
    TransformedChartEdgeProvider, map_transformed_shape_chains, publish_changed_raw_computed_shape,
    publish_polygon_with_edge_roles, publish_raw_computed_shape, publish_transformed_shape,
    publish_triangle_vertex_stream_budgeted,
};
use crate::crs::frame::{Frame, GeometryTransformFrame, crs_arc_static};
use crate::geographic::GeographicFrame;
use crate::geometry::{
    CoordSeq, CoordWindow, CoordinateAxes, Coordinates as _, ExpansionBudget, LineSeq, MOrdinate,
    Point, Polygon, Ring, Shape, SmoothMethod, SourceArcContext, TopologyDomain, ZOrdinate,
};

mod polygons;
mod raw;
mod tessellation;
mod transformed;
