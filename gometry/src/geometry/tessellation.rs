//! Tessellation outputs from `Shape`: Delaunay/constrained triangulation,
//! ear-cut polygon triangles, Voronoi polygons/edges, and linework
//! polygonization.

mod circumcenter;
mod delaunay;
pub(crate) use delaunay::exact_delaunay::{EMPTY, Triangulation, next_halfedge, prev_halfedge};
mod earcut;
mod earcut_bridges;
mod earcut_sweep;
pub(in crate::geometry) mod exact;
mod sampling;
mod shape;
mod voronoi;
mod voronoi_dcel;

pub(crate) struct VoronoiPolygonOutput {
    pub(crate) polygon: crate::geometry::Polygon,
    pub(crate) computed_edges: Vec<Vec<bool>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RingOwner {
    polygon: usize,
    ring: usize,
}

impl RingOwner {
    pub(super) const fn new(polygon: usize, ring: usize) -> Self {
        Self { polygon, ring }
    }

    const fn is_shell(self) -> bool {
        self.ring == 0
    }
}

#[derive(Clone, Copy)]
pub(super) enum RingPairIntersection {
    ShellHole,
    HoleHole,
}

pub(super) fn ring_pair_intersection_error(
    intersection: RingPairIntersection,
) -> crate::error::Error {
    match intersection {
        RingPairIntersection::ShellHole => crate::geometry::GeometryErrorKind::triangulation(
            "polygon hole touches or crosses the shell",
        ),
        RingPairIntersection::HoleHole => {
            crate::geometry::GeometryErrorKind::triangulation("polygon holes touch or cross")
        },
    }
}

pub(super) fn overlapping_rings_error(left: RingOwner, right: RingOwner) -> crate::error::Error {
    if left.polygon == right.polygon {
        if left.ring == right.ring {
            return crate::geometry::GeometryErrorKind::triangulation(if left.is_shell() {
                "polygon shell overlaps itself"
            } else {
                "polygon hole overlaps itself"
            });
        }
        return ring_pair_intersection_error(if left.is_shell() != right.is_shell() {
            RingPairIntersection::ShellHole
        } else {
            RingPairIntersection::HoleHole
        });
    }
    crate::geometry::GeometryErrorKind::triangulation(if left.is_shell() && right.is_shell() {
        "polygon shells touch or overlap"
    } else {
        "polygon boundaries touch or overlap"
    })
}

pub(crate) use delaunay::{CdtRefinement, constrained_triangle_vertices, delaunay_triangulation};
use delaunay::{
    CertifiedDelaunay, CertifiedPrimalEdge, Site, certified_delaunay, selected_triangle_points,
};
pub(crate) use earcut::{earcut_polygon_with, point_in_ccw_triangle, try_triangle_shape};
pub(crate) use sampling::{
    collect_sample_triangles, row_sample_seed, sample_weighted, uniform_f64, weight_scale,
};
use voronoi::{DelaunayComplex, delaunay_complex, snap_sites};

#[cfg(test)]
mod tests;
