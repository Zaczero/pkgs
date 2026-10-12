//! Tessellation outputs from `Shape`: Delaunay/constrained triangulation,
//! ear-cut polygon triangles, Voronoi polygons/edges, and linework
//! polygonization.

use std::ops::Deref;
use std::sync::Arc;

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
mod voronoi_receipt;

pub(in crate::geometry) use voronoi_receipt::{
    VoronoiAssignmentReceipt, VoronoiCellReceipt, VoronoiChartCutSide, VoronoiClipAtomReceipt,
    VoronoiClipSegmentReceipt, VoronoiClipSourceRoot, VoronoiDartReceipt, VoronoiDiagramReceipt,
    VoronoiDirectedEdgeUse, VoronoiEdgeOrigin, VoronoiEdgeReceipt, VoronoiEdgeSourceUse,
    VoronoiFrameSide, VoronoiProducer, VoronoiRingEdgeRole, VoronoiSeparatorReceipt,
    VoronoiSiteReceipt, VoronoiSourceSiteDecision, VoronoiSourceSiteReceipt, VoronoiVertexOrigin,
    VoronoiVertexReceipt,
};

pub(crate) struct VoronoiPolygonOutput {
    pub(crate) polygon: crate::geometry::Polygon,
    pub(in crate::geometry) owner: Option<u32>,
    pub(in crate::geometry) edge_roles: Vec<Vec<VoronoiRingEdgeRole>>,
}

pub(crate) struct VoronoiPolygonOutputs {
    pub(crate) outputs: Vec<VoronoiPolygonOutput>,
    pub(in crate::geometry) receipt: Arc<VoronoiDiagramReceipt>,
}

pub(crate) struct VoronoiEdgeColumn {
    pub(crate) coordinates: crate::geometry::CoordSeq,
    pub(crate) offsets: crate::geometry::CsrOffsetColumn,
    pub(in crate::geometry) receipt: Arc<VoronoiDiagramReceipt>,
}

impl Deref for VoronoiPolygonOutputs {
    type Target = [VoronoiPolygonOutput];

    fn deref(&self) -> &Self::Target {
        &self.outputs
    }
}

impl IntoIterator for VoronoiPolygonOutputs {
    type Item = VoronoiPolygonOutput;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.outputs.into_iter()
    }
}

impl<'a> IntoIterator for &'a VoronoiPolygonOutputs {
    type Item = &'a VoronoiPolygonOutput;
    type IntoIter = std::slice::Iter<'a, VoronoiPolygonOutput>;

    fn into_iter(self) -> Self::IntoIter {
        self.outputs.iter()
    }
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
        return ring_pair_intersection_error(if left.is_shell() == right.is_shell() {
            RingPairIntersection::HoleHole
        } else {
            RingPairIntersection::ShellHole
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
