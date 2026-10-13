use super::super::{
    AdmittedLayout, AssignedSite, ChartPole, EmissionChordId, EmissionNodeId, ExactSourceRange,
    ExpandedEmissionPlan, FragmentRecipeId, NativeAreaEdge, NativeGraphCycle, NativeGraphPart,
    NativePortData, NativeReferenceGraph, NativeSeamEventData, RootRange,
};
use crate::geometry::topology::emission::SourceTrace;

#[derive(Default)]
pub(in crate::geometry::topology::q_embedding) struct NativeLineRun {
    pub(in crate::geometry::topology::q_embedding) vertices: Vec<NativePortData>,
    pub(in crate::geometry::topology::q_embedding) chords: Vec<NativeLineChordSource>,
}
pub(in crate::geometry::topology::q_embedding) struct NativeLineChordSource {
    pub(in crate::geometry::topology::q_embedding) recipe: FragmentRecipeId,
    pub(in crate::geometry::topology::q_embedding) trace_edge: usize,
    pub(in crate::geometry::topology::q_embedding) producer_range: ExactSourceRange,
    pub(in crate::geometry::topology::q_embedding) trace_ranges: Vec<(usize, RootRange)>,
}
pub(in crate::geometry::topology::q_embedding) enum NativeLiteralChordAuthority {
    Producer {
        recipe: FragmentRecipeId,
        trace_edge: usize,
        producer_range: ExactSourceRange,
        line_trace: usize,
        trace_row: usize,
    },
    ReferenceAtom {
        atom: u32,
        reversed: bool,
        writer_edges: Vec<usize>,
    },
}
pub(in crate::geometry::topology::q_embedding) struct NativeLiteralChordUse {
    pub(in crate::geometry::topology::q_embedding) chord: EmissionChordId,
    pub(in crate::geometry::topology::q_embedding) authority: NativeLiteralChordAuthority,
}
pub(in crate::geometry::topology::q_embedding) struct NativeChordPieces {
    pub(in crate::geometry::topology::q_embedding) edges: Vec<NativeAreaEdge>,
    pub(in crate::geometry::topology::q_embedding) break_after: Vec<bool>,
    pub(in crate::geometry::topology::q_embedding) seam_events: Vec<NativeSeamEventData>,
}
pub(in crate::geometry::topology::q_embedding) struct NativeReference<'topology> {
    pub(in crate::geometry::topology::q_embedding) plan: ExpandedEmissionPlan<'topology>,
    pub(in crate::geometry::topology::q_embedding) dimension: AdmittedLayout,
    pub(in crate::geometry::topology::q_embedding) port_vertices: Vec<u32>,
    pub(in crate::geometry::topology::q_embedding) sites: Vec<Option<AssignedSite>>,
    pub(in crate::geometry::topology::q_embedding) feature_nodes: Vec<EmissionNodeId>,
    pub(in crate::geometry::topology::q_embedding) publication_sample_nodes: Vec<EmissionNodeId>,
    pub(in crate::geometry::topology::q_embedding) line_traces: Vec<SourceTrace>,
    pub(in crate::geometry::topology::q_embedding) literal_chord_uses: Vec<NativeLiteralChordUse>,
    pub(in crate::geometry::topology::q_embedding) line_graph_edges: Vec<NativeAreaEdge>,
    pub(in crate::geometry::topology::q_embedding) area_edges: Vec<NativeAreaEdge>,
    pub(in crate::geometry::topology::q_embedding) pole_sectors: Vec<NativeSelectedPoleSector>,
    pub(in crate::geometry::topology::q_embedding) graph: NativeReferenceGraph,
}
pub(in crate::geometry::topology::q_embedding) struct NativeSelectedPoleSector {
    pub(in crate::geometry::topology::q_embedding) region: usize,
    pub(in crate::geometry::topology::q_embedding) sector: super::super::super::q::PoleSector,
}
pub(in crate::geometry::topology::q_embedding) type NativePoleClosure = (usize, ChartPole);
pub(in crate::geometry::topology::q_embedding) struct NativeSelectedPoleSectors {
    pub(in crate::geometry::topology::q_embedding) sectors: Vec<NativeSelectedPoleSector>,
    pub(in crate::geometry::topology::q_embedding) closures: Vec<NativePoleClosure>,
}
pub(in crate::geometry::topology::q_embedding) struct NativeReferenceGraphResult {
    pub(in crate::geometry::topology::q_embedding) cycles: Vec<NativeGraphCycle>,
    pub(in crate::geometry::topology::q_embedding) parts: Vec<NativeGraphPart>,
    pub(in crate::geometry::topology::q_embedding) face_regions: Vec<Option<usize>>,
    pub(in crate::geometry::topology::q_embedding) face_groups: Vec<u32>,
    pub(in crate::geometry::topology::q_embedding) face_cells: Vec<Option<u8>>,
}
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology::q_embedding) enum NativeChartSpellingKey {
    Interior(i64),
    SeamEast,
    SeamWest,
    Pole(u64),
}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology::q_embedding) struct NativeChartVertexKey {
    pub(in crate::geometry::topology::q_embedding) node: EmissionNodeId,
    pub(in crate::geometry::topology::q_embedding) spelling: NativeChartSpellingKey,
}
#[derive(Clone)]
pub(in crate::geometry::topology::q_embedding) struct NativeChartVertexIndex {
    pub(in crate::geometry::topology::q_embedding) key: NativeChartVertexKey,
    pub(in crate::geometry::topology::q_embedding) index: u32,
}
#[derive(Clone, Copy)]
pub(in crate::geometry::topology::q_embedding) struct NativeChartAtomIndex {
    pub(in crate::geometry::topology::q_embedding) endpoints: [u32; 2],
    pub(in crate::geometry::topology::q_embedding) index: u32,
}
pub(in crate::geometry::topology::q_embedding) struct NativeChartAtom {
    pub(in crate::geometry::topology::q_embedding) contributors: Vec<NativeChartContributor>,
}
#[derive(Clone, Copy)]
pub(in crate::geometry::topology::q_embedding) struct NativeChartContributor {
    pub(in crate::geometry::topology::q_embedding) written_edge: usize,
    pub(in crate::geometry::topology::q_embedding) region: Option<usize>,
    pub(in crate::geometry::topology::q_embedding) reversed: bool,
}
#[derive(Clone, Copy)]
pub(in crate::geometry::topology::q_embedding) struct NativeWrittenChartEdge {
    pub(in crate::geometry::topology::q_embedding) atom: u32,
    pub(in crate::geometry::topology::q_embedding) reversed: bool,
}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology::q_embedding) struct NativeProducerUseKey {
    pub(in crate::geometry::topology::q_embedding) recipe: FragmentRecipeId,
    pub(in crate::geometry::topology::q_embedding) trace_edge: usize,
}
#[derive(Clone)]
pub(in crate::geometry::topology::q_embedding) struct NativeProducerAtomUse {
    pub(in crate::geometry::topology::q_embedding) written_edge: usize,
    pub(in crate::geometry::topology::q_embedding) atom: u32,
    pub(in crate::geometry::topology::q_embedding) reversed: bool,
    pub(in crate::geometry::topology::q_embedding) producer_range: ExactSourceRange,
}
pub(in crate::geometry::topology::q_embedding) struct NativeProducerUseTable {
    pub(in crate::geometry::topology::q_embedding) keys: Vec<NativeProducerUseKey>,
    pub(in crate::geometry::topology::q_embedding) offsets: Vec<u32>,
    pub(in crate::geometry::topology::q_embedding) atom_uses: Vec<NativeProducerAtomUse>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry::topology::q_embedding) struct NativeGraphFaceSourceUse {
    pub(in crate::geometry::topology::q_embedding) written_edge: usize,
    pub(in crate::geometry::topology::q_embedding) source_span: usize,
    pub(in crate::geometry::topology::q_embedding) side: NativeSourceGermSide,
}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology::q_embedding) enum NativeSourceGermSide {
    ForwardLeft,
    ReverseLeft,
}
impl NativeSourceGermSide {
    pub(in crate::geometry::topology::q_embedding) const fn index(self) -> usize {
        match self {
            Self::ForwardLeft => 0,
            Self::ReverseLeft => 1,
        }
    }
}
pub(in crate::geometry::topology::q_embedding) struct NativeGraphFaceSourceTable {
    pub(in crate::geometry::topology::q_embedding) offsets: Vec<u32>,
    pub(in crate::geometry::topology::q_embedding) uses: Vec<NativeGraphFaceSourceUse>,
}
pub(in crate::geometry::topology::q_embedding) struct NativeProducerEdgeRow {
    pub(in crate::geometry::topology::q_embedding) key: NativeProducerUseKey,
    pub(in crate::geometry::topology::q_embedding) written_edge: usize,
    pub(in crate::geometry::topology::q_embedding) atom: u32,
    pub(in crate::geometry::topology::q_embedding) reversed: bool,
    pub(in crate::geometry::topology::q_embedding) producer_range: ExactSourceRange,
}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology::q_embedding) enum NativeGraphSewingKind {
    SeamBank,
    PoleLongitude,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry::topology::q_embedding) struct NativeGraphSewing {
    pub(in crate::geometry::topology::q_embedding) vertices: [u32; 2],
    pub(in crate::geometry::topology::q_embedding) kind: NativeGraphSewingKind,
}
