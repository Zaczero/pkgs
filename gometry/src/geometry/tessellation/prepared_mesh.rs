use std::ops::Range;

use crate::geographic::ChartPointSite;
use crate::geometry::Point;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MeshProducer {
    Delaunay,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum MeshSiteOrigin {
    Source { occurrence: usize, record: Point },
}

impl MeshSiteOrigin {
    pub(crate) const fn point(self) -> Point {
        match self {
            Self::Source { record, .. } => record,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum MeshSiteLocation {
    Cartesian,
    Retained(ChartPointSite),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PreparedMeshSite {
    producer_ordinal: usize,
    origin: MeshSiteOrigin,
    location: MeshSiteLocation,
}

impl PreparedMeshSite {
    pub(in crate::geometry::tessellation) const fn source(
        producer_ordinal: usize,
        occurrence: usize,
        record: Point,
        location: MeshSiteLocation,
    ) -> Self {
        Self {
            producer_ordinal,
            origin: MeshSiteOrigin::Source { occurrence, record },
            location,
        }
    }

    pub(crate) const fn producer_ordinal(self) -> usize {
        self.producer_ordinal
    }

    pub(crate) const fn origin(self) -> MeshSiteOrigin {
        self.origin
    }

    pub(crate) const fn location(self) -> MeshSiteLocation {
        self.location
    }

    pub(crate) fn source_record(self) -> Point {
        match (self.origin, self.location) {
            (MeshSiteOrigin::Source { occurrence, record }, MeshSiteLocation::Retained(site)) => {
                debug_assert_eq!(site.source_ordinal(), occurrence);
                record
            },
            (MeshSiteOrigin::Source { record, .. }, MeshSiteLocation::Cartesian) => record,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MeshUseId {
    face: usize,
    side: u8,
}

impl MeshUseId {
    pub(in crate::geometry::tessellation) const fn new(face: usize, side: u8) -> Self {
        Self { face, side }
    }

    pub(crate) const fn face(self) -> usize {
        self.face
    }

    pub(crate) const fn side(self) -> u8 {
        self.side
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MeshEdgeRole {
    Delaunay,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PreparedMeshBoundaryUse {
    output_side: u8,
    selected_use: MeshUseId,
    start_site: usize,
    end_site: usize,
    reverse: Option<MeshUseId>,
    reversed_for_output: bool,
    cocircular: bool,
    role: MeshEdgeRole,
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

    pub(crate) const fn output_side(self) -> u8 {
        self.output_side
    }

    pub(crate) const fn selected_use(self) -> MeshUseId {
        self.selected_use
    }

    pub(crate) const fn endpoints(self) -> [usize; 2] {
        [self.start_site, self.end_site]
    }

    pub(crate) const fn reverse(self) -> Option<MeshUseId> {
        self.reverse
    }

    pub(crate) const fn reversed_for_output(self) -> bool {
        self.reversed_for_output
    }

    pub(crate) const fn cocircular(self) -> bool {
        self.cocircular
    }

    pub(crate) const fn role(self) -> MeshEdgeRole {
        self.role
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedMeshFace {
    selected_face: usize,
    selected_sites: [usize; 3],
    output_sites: [usize; 3],
    output_to_selected: [u8; 3],
    reversed: bool,
    output_vertices: Range<usize>,
    boundary_uses: [PreparedMeshBoundaryUse; 3],
}

impl PreparedMeshFace {
    pub(in crate::geometry::tessellation) const fn new(
        selected_face: usize,
        selected_sites: [usize; 3],
        output_sites: [usize; 3],
        output_to_selected: [u8; 3],
        reversed: bool,
        output_vertices: Range<usize>,
        boundary_uses: [PreparedMeshBoundaryUse; 3],
    ) -> Self {
        Self {
            selected_face,
            selected_sites,
            output_sites,
            output_to_selected,
            reversed,
            output_vertices,
            boundary_uses,
        }
    }

    pub(crate) const fn selected_face(&self) -> usize {
        self.selected_face
    }

    pub(crate) const fn selected_sites(&self) -> [usize; 3] {
        self.selected_sites
    }

    pub(crate) const fn output_sites(&self) -> [usize; 3] {
        self.output_sites
    }

    pub(crate) const fn output_to_selected(&self) -> [u8; 3] {
        self.output_to_selected
    }

    pub(crate) const fn reversed(&self) -> bool {
        self.reversed
    }

    pub(crate) fn output_vertices(&self) -> Range<usize> {
        self.output_vertices.clone()
    }

    pub(crate) const fn boundary_uses(&self) -> &[PreparedMeshBoundaryUse; 3] {
        &self.boundary_uses
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedMeshPublication {
    producer: MeshProducer,
    sites: Vec<PreparedMeshSite>,
    faces: Vec<PreparedMeshFace>,
    points: Vec<Point>,
}

impl PreparedMeshPublication {
    pub(in crate::geometry::tessellation) fn new(
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

    pub(crate) const fn producer(&self) -> MeshProducer {
        self.producer
    }

    pub(crate) fn sites(&self) -> &[PreparedMeshSite] {
        &self.sites
    }

    pub(crate) fn faces(&self) -> &[PreparedMeshFace] {
        &self.faces
    }

    pub(crate) fn points(&self) -> &[Point] {
        &self.points
    }

    pub(in crate::geometry::tessellation) fn into_points(self) -> Vec<Point> {
        self.points
    }

    pub(crate) fn take_points(&mut self) -> Vec<Point> {
        std::mem::take(&mut self.points)
    }
}
