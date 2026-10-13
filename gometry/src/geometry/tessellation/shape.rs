use super::delaunay::certified_delaunay_for_retained_points;
use crate::NonNegative;
use crate::error::Result;
use crate::geographic::{ChartPointSite, GeographicFrame, PointSelectionChart};
use crate::geometry::tessellation::{
    CdtRefinement, CertifiedDelaunay, GeographicSampleSegment, PreparedSampling, RingOwner,
    SampleOccurrenceId, SampleRecipe, SampleSupport, Site, certified_delaunay,
    collect_sample_atoms, collect_sample_triangles, earcut_polygon_with, sample_weighted,
    snap_sites, try_triangle_shape, uniform_f64, voronoi_dcel, weight_scale,
};
use crate::geometry::{
    AxisFrame, CoordSeq, CoordinateAxes, Coordinates as _, Dimension, ExpansionBudget,
    GeometryErrorKind, MultiMember, MultiPolygon, Point, PointKey, Polygon, Polygonal, Ring, Shape,
    ShapeCountQuery, TopologyDomain, VoronoiBoundary, axis_extent, line_segments,
};
mod sample;
mod triangles;
mod voronoi;

#[cfg(test)]
pub(super) use triangles::{BudgetedPackedTriangleSink, BudgetedTriangleSink};
#[cfg(test)]
pub(super) use voronoi::canonical_voronoi_sites;
