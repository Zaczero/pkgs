//! Tessellation outputs from `Shape`: Delaunay/constrained triangulation,
//! ear-cut polygon triangles, Voronoi polygons/edges, boundary, and linework
//! polygonization.

mod boundary;
mod circumcenter;
mod delaunay;
pub(crate) use delaunay::exact_delaunay::{EMPTY, Triangulation, next_halfedge, prev_halfedge};
mod earcut;
pub(in crate::geometry) mod exact;
mod sampling;
mod shape;
mod voronoi;
mod voronoi_dcel;

pub(crate) use boundary::{line_boundary, multiline_boundary, rings_to_boundary};
pub(crate) use delaunay::{CdtRefinement, constrained_triangle_vertices, delaunay_triangulation};
use delaunay::{CertifiedDelaunay, CertifiedPrimalEdge, Site, certified_delaunay};
pub(crate) use earcut::{earcut_polygon_with, point_in_ccw_triangle, try_triangle_shape};
pub(crate) use sampling::{
    collect_sample_triangles, row_sample_seed, sample_weighted, uniform_f64, weight_scale,
};
use voronoi::{DelaunayComplex, delaunay_complex, snap_sites};

#[cfg(test)]
mod tests;
