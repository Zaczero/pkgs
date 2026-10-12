use crate::geographic::{ChartPointSite, RetainedLongitudeSite};
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{Point, XY};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) enum VoronoiProducer {
    Exact,
    Dual,
    Collinear,
    LoneSite,
    Empty,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry) enum VoronoiFrameSide {
    Bottom,
    Right,
    Top,
    Left,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry) enum VoronoiChartCutSide {
    Lower,
    Upper,
}

#[derive(Clone, Debug)]
pub(in crate::geometry) struct VoronoiDiagramReceipt {
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes the producer receipt kind")
    )]
    pub(in crate::geometry) producer: VoronoiProducer,
    pub(in crate::geometry) sites: Vec<VoronoiSiteReceipt>,
    pub(in crate::geometry) source_sites: Vec<VoronoiSourceSiteReceipt>,
    pub(in crate::geometry) selected_chart_cuts: Option<[RetainedLongitudeSite; 2]>,
    pub(in crate::geometry) clip_segments: Vec<VoronoiClipSegmentReceipt>,
    pub(in crate::geometry) vertices: Vec<VoronoiVertexReceipt>,
    pub(in crate::geometry) edges: Vec<VoronoiEdgeReceipt>,
    pub(in crate::geometry) darts: Vec<VoronoiDartReceipt>,
    pub(in crate::geometry) cells: Vec<VoronoiCellReceipt>,
    pub(in crate::geometry) separators: Vec<VoronoiSeparatorReceipt>,
    pub(in crate::geometry) assignments: Vec<VoronoiAssignmentReceipt>,
}

impl VoronoiDiagramReceipt {
    pub(in crate::geometry) const fn empty(producer: VoronoiProducer) -> Self {
        Self {
            producer,
            sites: Vec::new(),
            source_sites: Vec::new(),
            selected_chart_cuts: None,
            clip_segments: Vec::new(),
            vertices: Vec::new(),
            edges: Vec::new(),
            darts: Vec::new(),
            cells: Vec::new(),
            separators: Vec::new(),
            assignments: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(in crate::geometry) const fn producer(&self) -> VoronoiProducer {
        self.producer
    }

    pub(in crate::geometry) fn has_site(&self, id: u32) -> bool {
        self.sites
            .get(id as usize)
            .is_some_and(|site| site.id == id)
    }
}

#[derive(Clone, Debug)]
pub(in crate::geometry) struct VoronoiSiteReceipt {
    pub(in crate::geometry) id: u32,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes source occurrence identity")
    )]
    pub(in crate::geometry) source_ordinal: usize,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes the selected full source record")
    )]
    pub(in crate::geometry) source_record: Point,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes the retained exact site coordinate")
    )]
    pub(in crate::geometry) exact_xy: [ExactRational; 2],
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes the retained chart-site identity")
    )]
    pub(in crate::geometry) retained_chart_site: Option<ChartPointSite>,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes quotient-alias provenance")
    )]
    pub(in crate::geometry) quotient_aliases: Vec<usize>,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes tolerance-snap provenance")
    )]
    pub(in crate::geometry) snapped_sources: Vec<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) enum VoronoiSourceSiteDecision {
    Selected { site: u32 },
    QuotientAlias { site: u32 },
    Snapped { site: u32 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) struct VoronoiSourceSiteReceipt {
    pub(in crate::geometry) source_ordinal: usize,
    pub(in crate::geometry) decision: VoronoiSourceSiteDecision,
}

#[derive(Clone, Debug)]
pub(in crate::geometry) struct VoronoiClipSegmentReceipt {
    pub(in crate::geometry) id: u32,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes original clip ring identity")
    )]
    pub(in crate::geometry) ring: u32,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes original clip edge identity")
    )]
    pub(in crate::geometry) edge: u32,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 distinguishes generated frame clips")
    )]
    pub(in crate::geometry) frame_side: Option<VoronoiFrameSide>,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes exact clip-segment endpoints")
    )]
    pub(in crate::geometry) exact_start: [ExactRational; 2],
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes exact clip-segment endpoints")
    )]
    pub(in crate::geometry) exact_end: [ExactRational; 2],
    pub(in crate::geometry) source_roots: Vec<VoronoiClipSourceRoot>,
    pub(in crate::geometry) atoms: Vec<VoronoiClipAtomReceipt>,
}

#[derive(Clone, Debug)]
pub(in crate::geometry) struct VoronoiClipSourceRoot {
    pub(in crate::geometry) ring: usize,
    pub(in crate::geometry) edge: usize,
    pub(in crate::geometry) reversed: bool,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes the original clip-edge start record")
    )]
    pub(in crate::geometry) start: Point,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes the original clip-edge end record")
    )]
    pub(in crate::geometry) end: Point,
}

#[derive(Clone, Debug)]
pub(in crate::geometry) struct VoronoiClipAtomReceipt {
    pub(in crate::geometry) edge: u32,
    pub(in crate::geometry) reversed: bool,
    pub(in crate::geometry) start_parameter: ExactRational,
    pub(in crate::geometry) end_parameter: ExactRational,
}

#[derive(Clone, Debug)]
pub(in crate::geometry) struct VoronoiVertexReceipt {
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes producer vertex identity")
    )]
    pub(in crate::geometry) id: u32,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes the exact producer coordinate")
    )]
    pub(in crate::geometry) exact_xy: [ExactRational; 2],
    pub(in crate::geometry) origins: Vec<VoronoiVertexOrigin>,
    pub(in crate::geometry) assignment: Option<XY>,
}

#[derive(Clone, Debug)]
pub(in crate::geometry) enum VoronoiVertexOrigin {
    SourceSite {
        #[cfg_attr(
            not(test),
            expect(dead_code, reason = "V2 consumes source-site vertex identity")
        )]
        site: u32,
    },
    OverlayNode {
        node: u32,
    },
    Circumcenter {
        #[cfg_attr(
            not(test),
            expect(dead_code, reason = "V2 consumes circumcenter component identity")
        )]
        component: u32,
        #[cfg_attr(
            not(test),
            expect(dead_code, reason = "V2 consumes the defining Delaunay site triple")
        )]
        sites: [u32; 3],
    },
    SourceClip {
        #[cfg_attr(
            not(test),
            expect(dead_code, reason = "V2 consumes the source clip-segment identity")
        )]
        segment: u32,
        #[cfg_attr(
            not(test),
            expect(dead_code, reason = "V2 consumes the exact source parameter")
        )]
        parameter: ExactRational,
        #[cfg_attr(
            not(test),
            expect(dead_code, reason = "V2 consumes source clip ring identity")
        )]
        source_ring: Option<usize>,
        #[cfg_attr(
            not(test),
            expect(dead_code, reason = "V2 consumes source clip edge identity")
        )]
        source_edge: Option<usize>,
    },
    Bisector {
        #[cfg_attr(
            not(test),
            expect(dead_code, reason = "V2 consumes the defining site pair")
        )]
        sites: [u32; 2],
    },
    Frame {
        #[cfg_attr(
            not(test),
            expect(dead_code, reason = "V2 consumes the generated frame side")
        )]
        side: VoronoiFrameSide,
    },
    Pole {
        #[cfg_attr(
            not(test),
            expect(dead_code, reason = "V2 consumes chart-pole vertex provenance")
        )]
        latitude: f64,
    },
    ChartCut {
        #[cfg_attr(
            not(test),
            expect(dead_code, reason = "V2 consumes chart-cut vertex provenance")
        )]
        side: VoronoiChartCutSide,
    },
}

#[derive(Clone, Debug)]
pub(in crate::geometry) struct VoronoiEdgeReceipt {
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes producer edge identity")
    )]
    pub(in crate::geometry) id: u32,
    pub(in crate::geometry) endpoints: [u32; 2],
    pub(in crate::geometry) separator: Option<[u32; 2]>,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes source clip-segment incidence")
    )]
    pub(in crate::geometry) clip_sources: Vec<u32>,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 distinguishes generated frame edges")
    )]
    pub(in crate::geometry) frame_side: Option<VoronoiFrameSide>,
    /// The owners on the left and right of `endpoints`; opposite darts agree on this order.
    pub(in crate::geometry) owner_transition: [Option<u32>; 2],
    pub(in crate::geometry) origins: Vec<VoronoiEdgeOrigin>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) struct VoronoiDirectedEdgeUse {
    pub(in crate::geometry) edge: u32,
    pub(in crate::geometry) forward: bool,
}

#[derive(Clone, Debug)]
pub(in crate::geometry) struct VoronoiDartReceipt {
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes producer dart identity")
    )]
    pub(in crate::geometry) id: u32,
    pub(in crate::geometry) origin: u32,
    pub(in crate::geometry) target: u32,
    pub(in crate::geometry) twin: Option<u32>,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes edge-to-dart incidence")
    )]
    pub(in crate::geometry) edge: u32,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes exact face incidence")
    )]
    pub(in crate::geometry) face: Option<u32>,
    pub(in crate::geometry) owner: Option<u32>,
}

#[derive(Clone, Debug)]
pub(in crate::geometry) struct VoronoiCellReceipt {
    pub(in crate::geometry) owner: u32,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes exact cell face identity")
    )]
    pub(in crate::geometry) face: Option<u32>,
    pub(in crate::geometry) shell: Vec<VoronoiDirectedEdgeUse>,
    pub(in crate::geometry) holes: Vec<Vec<VoronoiDirectedEdgeUse>>,
}

#[derive(Clone, Debug)]
pub(in crate::geometry) struct VoronoiSeparatorReceipt {
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes separator site identity")
    )]
    pub(in crate::geometry) sites: [u32; 2],
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes ordered separator vertex identity")
    )]
    pub(in crate::geometry) vertices: Vec<u32>,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "V2 consumes ordered separator edge identity")
    )]
    pub(in crate::geometry) edges: Vec<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(in crate::geometry) struct VoronoiAssignmentReceipt {
    pub(in crate::geometry) vertex: u32,
    pub(in crate::geometry) embedded: u32,
    pub(in crate::geometry) point: XY,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry) enum VoronoiEdgeOrigin {
    SourceClip { segment: u32 },
    Bisector { sites: [u32; 2] },
    Frame { side: VoronoiFrameSide },
}

#[derive(Clone, Debug)]
pub(in crate::geometry) struct VoronoiRingEdgeRole {
    pub(in crate::geometry) edge: Option<u32>,
    pub(in crate::geometry) forward: bool,
    pub(in crate::geometry) origins: Vec<VoronoiEdgeOrigin>,
    pub(in crate::geometry) source_uses: Vec<VoronoiEdgeSourceUse>,
}

#[derive(Clone, Debug)]
pub(in crate::geometry) struct VoronoiEdgeSourceUse {
    pub(in crate::geometry) source_segment: u32,
    pub(in crate::geometry) source_operand: Option<u32>,
    pub(in crate::geometry) source_ring: Option<u32>,
    pub(in crate::geometry) source_edge: Option<u32>,
    pub(in crate::geometry) forward: bool,
    pub(in crate::geometry) start_parameter: ExactRational,
    pub(in crate::geometry) end_parameter: ExactRational,
}
