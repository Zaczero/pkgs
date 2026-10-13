use std::ops::Range;

use crate::geographic::ChartPointSite;
use crate::geometry::Point;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MeshProducer {
    Delaunay,
    Earcut,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum MeshSiteLocation {
    Cartesian,
    Retained(ChartPointSite),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PreparedMeshSite {
    pub(crate) producer_ordinal: usize,
    pub(crate) source_ordinal: usize,
    pub(crate) record: Point,
    pub(crate) location: MeshSiteLocation,
}

impl PreparedMeshSite {
    pub(in crate::geometry::tessellation) const fn source(
        producer_ordinal: usize,
        source_ordinal: usize,
        record: Point,
        location: MeshSiteLocation,
    ) -> Self {
        Self {
            producer_ordinal,
            source_ordinal,
            record,
            location,
        }
    }

    pub(crate) fn source_record(self) -> Point {
        if let MeshSiteLocation::Retained(site) = self.location {
            debug_assert_eq!(site.source_ordinal(), self.source_ordinal);
        }
        self.record
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MeshUseId {
    pub(crate) face: usize,
    pub(crate) side: u8,
}

impl MeshUseId {
    pub(in crate::geometry::tessellation) const fn new(face: usize, side: u8) -> Self {
        Self { face, side }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MeshEdgeRole {
    Delaunay,
    EarcutBoundary,
    EarcutDiagonal,
    EarcutBridge,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PreparedMeshBoundaryUse {
    pub(crate) output_side: u8,
    pub(crate) selected_use: MeshUseId,
    pub(crate) start_site: usize,
    pub(crate) end_site: usize,
    pub(crate) reverse: Option<MeshUseId>,
    pub(crate) reversed_for_output: bool,
    pub(crate) cocircular: bool,
    pub(crate) role: MeshEdgeRole,
}

impl PreparedMeshBoundaryUse {
    pub(in crate::geometry::tessellation) const fn new(
        output_side: u8,
        selected_use: MeshUseId,
        start_site: usize,
        end_site: usize,
        reverse: Option<MeshUseId>,
        reversed_for_output: bool,
        cocircular: bool,
        role: MeshEdgeRole,
    ) -> Self {
        Self {
            output_side,
            selected_use,
            start_site,
            end_site,
            reverse,
            reversed_for_output,
            cocircular,
            role,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedMeshFace {
    pub(crate) selected_face: usize,
    pub(crate) selected_sites: [usize; 3],
    pub(crate) output_sites: [usize; 3],
    pub(crate) output_to_selected: [u8; 3],
    pub(crate) reversed: bool,
    pub(crate) output_vertices: Range<usize>,
    pub(crate) boundary_uses: [PreparedMeshBoundaryUse; 3],
}

impl PreparedMeshFace {
    pub(in crate::geometry::tessellation) const fn new(
        selected_face: usize,
        selected_sites: [usize; 3],
        output_sites: [usize; 3],
        output_to_selected: [u8; 3],
        reversed: bool,
        output_vertices: Range<usize>,
        boundary_uses: &[PreparedMeshBoundaryUse; 3],
    ) -> Self {
        Self {
            selected_face,
            selected_sites,
            output_sites,
            output_to_selected,
            reversed,
            output_vertices,
            boundary_uses: *boundary_uses,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedMeshPublication {
    pub(crate) producer: MeshProducer,
    pub(crate) sites: Vec<PreparedMeshSite>,
    pub(crate) faces: Vec<PreparedMeshFace>,
    pub(crate) points: Vec<Point>,
}

impl PreparedMeshPublication {
    pub(in crate::geometry::tessellation) const fn new(
        producer: MeshProducer,
        sites: Vec<PreparedMeshSite>,
        faces: Vec<PreparedMeshFace>,
        points: Vec<Point>,
    ) -> Self {
        Self {
            producer,
            sites,
            faces,
            points,
        }
    }

    pub(in crate::geometry::tessellation) fn into_points(self) -> Vec<Point> {
        self.points
    }

    pub(crate) fn take_points(&mut self) -> Vec<Point> {
        std::mem::take(&mut self.points)
    }
}
