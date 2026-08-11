//! Tessellation outputs from `Shape`: Delaunay/constrained triangulation,
//! ear-cut polygon triangles, Voronoi polygons/edges, boundary, and linework
//! polygonization.

use spade::handles::{FixedFaceHandle, FixedVertexHandle, InnerTag};
use spade::{
    AngleLimit, ConstrainedDelaunayTriangulation, DelaunayTriangulation, InsertionError, Point2,
    RefinementParameters,
};

pub(crate) use crate::error::Error;

mod boundary;
mod delaunay;
mod earcut;
pub(in crate::geometry) mod exact;
mod polygonize;
mod sampling;
mod shape;
mod voronoi;
mod voronoi_dcel;

pub(crate) use boundary::{
    collect_xy_chains, line_boundary, multiline_boundary, rings_to_boundary,
};
pub(crate) use delaunay::{
    CdtRefinement, constrained_triangle_vertices, delaunay_triangulation_spade,
};
use delaunay::{CertifiedDelaunay, CertifiedPrimalEdge, Site, certified_delaunay};
pub(crate) use earcut::{
    earcut_polygon_with, point_in_ccw_triangle, polygon_triangles, triangle_shape,
};
pub(crate) use polygonize::{
    build_area_lines, minimal_positive_face_rings, polygonize_full, polygonize_lines,
};
pub(crate) use sampling::{
    collect_sample_triangles, row_sample_seed, sample_weighted, uniform_f64, weight_scale,
};
use voronoi::{DelaunayComplex, delaunay_complex, snap_sites};

#[cfg(test)]
mod tests;
