use super::super::*;

#[derive(Debug)]
pub(in crate::geometry::topology::q_embedding) enum PublishedEndpoint {
    Source {
        port: QPortId,
        source_record: Option<Point>,
        sample: Option<SourceSampleOccurrence>,
    },
    Published {
        node: EmissionNodeId,
        sample: usize,
    },
}
pub(in crate::geometry::topology::q_embedding) struct QPublishedChord {
    pub(in crate::geometry::topology::q_embedding) recipe: FragmentRecipeId,
    pub(in crate::geometry::topology::q_embedding) trace_edge: usize,
    pub(in crate::geometry::topology::q_embedding) generated: bool,
    pub(in crate::geometry::topology::q_embedding) nodes: [QGraphNode; 2],
    pub(in crate::geometry::topology::q_embedding) endpoints: [PublishedEndpoint; 2],
    pub(in crate::geometry::topology::q_embedding) exact: [ExactChartPoint; 2],
    pub(in crate::geometry::topology::q_embedding) reference_offsets: [ExactRational; 2],
    pub(in crate::geometry::topology::q_embedding) nearest: Segment,
    pub(in crate::geometry::topology::q_embedding) boxes: [Bounds; 3],
    pub(in crate::geometry::topology::q_embedding) radius: f64,
    pub(in crate::geometry::topology::q_embedding) image_radii: [f64; 3],
    pub(in crate::geometry::topology::q_embedding) dependencies: Vec<QNodeId>,
    pub(in crate::geometry::topology::q_embedding) source_ranges: Vec<QProducerSourceRange>,
}
#[derive(Clone)]
pub(in crate::geometry::topology::q_embedding) struct QSourceSpanRange {
    pub(in crate::geometry::topology::q_embedding) start: ExactRational,
    pub(in crate::geometry::topology::q_embedding) end: ExactRational,
    pub(in crate::geometry::topology::q_embedding) span: usize,
    pub(in crate::geometry::topology::q_embedding) owner: usize,
    pub(in crate::geometry::topology::q_embedding) source_lift: i64,
    pub(in crate::geometry::topology::q_embedding) source_range: ExactSourceRange,
    pub(in crate::geometry::topology::q_embedding) reversed: bool,
    pub(in crate::geometry::topology::q_embedding) nodes: [QNodeId; 2],
    pub(in crate::geometry::topology::q_embedding) ports: [QPortId; 2],
}
#[derive(Clone)]
pub(in crate::geometry::topology::q_embedding) struct QProducerSourceRange {
    pub(in crate::geometry::topology::q_embedding) root: RootId,
    pub(in crate::geometry::topology::q_embedding) source_edge: SourceEdgeId,
    pub(in crate::geometry::topology::q_embedding) source_lift: i64,
    pub(in crate::geometry::topology::q_embedding) source_range: ExactSourceRange,
    pub(in crate::geometry::topology::q_embedding) producer_range: ExactSourceRange,
    pub(in crate::geometry::topology::q_embedding) span: usize,
    pub(in crate::geometry::topology::q_embedding) owner: usize,
    pub(in crate::geometry::topology::q_embedding) span_range: ExactSourceRange,
    pub(in crate::geometry::topology::q_embedding) nodes: [QNodeId; 2],
    pub(in crate::geometry::topology::q_embedding) ports: [QPortId; 2],
}
pub(in crate::geometry::topology::q_embedding) struct QProducerTrace {
    pub(in crate::geometry::topology::q_embedding) source_ranges: Vec<QProducerSourceRange>,
    pub(in crate::geometry::topology::q_embedding) dependencies: Vec<QNodeId>,
}
pub(in crate::geometry::topology::q_embedding) struct BuiltQPublication {
    pub(in crate::geometry::topology::q_embedding) nodes: EmissionNodeRegistry,
    pub(in crate::geometry::topology::q_embedding) sample_nodes: Vec<EmissionNodeId>,
    pub(in crate::geometry::topology::q_embedding) sample_source_ports: Vec<Option<QPortId>>,
    pub(in crate::geometry::topology::q_embedding) chords: Vec<QPublishedChord>,
    pub(in crate::geometry::topology::q_embedding) source_inventory: Vec<QNodeId>,
    pub(in crate::geometry::topology::q_embedding) domains: PublishedDomains,
}
#[derive(Clone)]
pub(in crate::geometry::topology::q_embedding) struct QProducerFaceGerm {
    pub(in crate::geometry::topology::q_embedding) dart: QDartId,
    pub(in crate::geometry::topology::q_embedding) face: GlobalFaceId,
    pub(in crate::geometry::topology::q_embedding) selected_root: Option<usize>,
}
#[derive(Clone)]
pub(in crate::geometry::topology::q_embedding) struct QProducerGerm {
    pub(in crate::geometry::topology::q_embedding) chord: usize,
    pub(in crate::geometry::topology::q_embedding) endpoint: usize,
    pub(in crate::geometry::topology::q_embedding) source_ranges: Vec<usize>,
}
pub(in crate::geometry::topology::q_embedding) struct QProducerJunction {
    pub(in crate::geometry::topology::q_embedding) node: QNodeId,
    pub(in crate::geometry::topology::q_embedding) ports: Vec<QPortId>,
    pub(in crate::geometry::topology::q_embedding) face_germs: Vec<QProducerFaceGerm>,
    pub(in crate::geometry::topology::q_embedding) producer_germs: Vec<QProducerGerm>,
    pub(in crate::geometry::topology::q_embedding) isolated_host: Option<GlobalFaceId>,
    pub(in crate::geometry::topology::q_embedding) dependencies: Vec<QVariableNode>,
}
pub(in crate::geometry::topology::q_embedding) struct QProducerLinework {
    pub(in crate::geometry::topology::q_embedding) chord: usize,
    pub(in crate::geometry::topology::q_embedding) recipe: FragmentRecipeId,
    pub(in crate::geometry::topology::q_embedding) use_kind: SelectedUse,
    pub(in crate::geometry::topology::q_embedding) source_ranges: Vec<usize>,
    pub(in crate::geometry::topology::q_embedding) dependencies: Vec<QVariableNode>,
}
pub(in crate::geometry::topology::q_embedding) struct QProducerPoint {
    pub(in crate::geometry::topology::q_embedding) node: QNodeId,
    pub(in crate::geometry::topology::q_embedding) host: Option<GlobalFaceId>,
    pub(in crate::geometry::topology::q_embedding) point_rows: Vec<SourceRowId>,
    pub(in crate::geometry::topology::q_embedding) dependencies: Vec<QVariableNode>,
}
#[derive(Default)]
pub(in crate::geometry::topology::q_embedding) struct PreparedProducerWitness {
    pub(in crate::geometry::topology::q_embedding) junctions: Vec<QProducerJunction>,
    pub(in crate::geometry::topology::q_embedding) linework: Vec<QProducerLinework>,
    pub(in crate::geometry::topology::q_embedding) points: Vec<QProducerPoint>,
}
pub(in crate::geometry::topology::q_embedding) struct AssignedProducerLeaf {
    pub(in crate::geometry::topology::q_embedding) recipe: FragmentRecipeId,
    pub(in crate::geometry::topology::q_embedding) trace_edge: usize,
    pub(in crate::geometry::topology::q_embedding) points: [ExactChartPoint; 2],
}
pub(in crate::geometry::topology::q_embedding) struct AssignedProducerFanRayBuilder {
    pub(in crate::geometry::topology::q_embedding) selected_root: Option<usize>,
    pub(in crate::geometry::topology::q_embedding) targets: Vec<ExactChartPoint>,
}
pub(in crate::geometry::topology::q_embedding) struct AssignedProducerProjection {
    pub(in crate::geometry::topology::q_embedding) source_ports: Vec<Option<ExactChartPoint>>,
    pub(in crate::geometry::topology::q_embedding) leaves: Vec<AssignedProducerLeaf>,
    pub(in crate::geometry::topology::q_embedding) rejections: Vec<QVariableNode>,
}
