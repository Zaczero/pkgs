use super::{
    CanonicalQNodeKey, ChartSide, ExactChartPoint, ExactGeographicTopology, ExactQuotientKey,
    FallibleAvl, FragmentRecipeId, GeographicFrame, GeometryErrorKind, Point, QNodeId, Range,
    RequestRowId, ResourceError, Result, TraceRequestLocation, compare_quotient_keys,
    quotient_key_for_chart_position,
};
use crate::work::Work;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) struct EmissionNodeId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry) struct EmissionPortId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) struct EmissionChordId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) struct ChartPartId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) struct NativeFaceId(pub u32);

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) enum NodeAuthority {
    Source(QNodeId),
    Computed(ExactQuotientKey),
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) enum PortSpelling {
    Interior { periods: i64 },
    Seam(ChartSide),
    Pole { native_longitude: f64 },
}

#[derive(Clone, Copy, Debug)]
pub(in crate::geometry::topology) enum VertexOrigin {
    SourceNode(QNodeId),
    SourceSample,
    Presentation,
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) struct EmissionNode {
    pub(in crate::geometry::topology) authority: NodeAuthority,
}

#[derive(Clone)]
pub(super) struct IndexedEmissionNode {
    pub(super) key: ExactQuotientKey,
    pub(super) node: EmissionNodeId,
}

pub(in crate::geometry::topology) struct EmissionNodeRegistry {
    pub(super) nodes: Vec<EmissionNode>,
    pub(super) source_nodes: Vec<Option<EmissionNodeId>>,
    pub(super) by_exact_key: FallibleAvl<IndexedEmissionNode>,
}

impl EmissionNodeRegistry {
    pub(in crate::geometry::topology) fn with_sources(
        topology: &ExactGeographicTopology,
        live_nodes: &[QNodeId],
        work: &mut impl Work,
    ) -> Result<Self> {
        work.charge_into_error(
            u64::try_from(topology.q.nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut registry = Self {
            nodes: Vec::with_capacity(live_nodes.len()),
            source_nodes: vec![None; topology.q.nodes.len()],
            by_exact_key: FallibleAvl::with_capacity(live_nodes.len()),
        };
        for &source in live_nodes {
            work.charge_into_error(1)?;
            let key = match topology.canonical_node_key(source)? {
                CanonicalQNodeKey::Chart(key) => key.clone(),
                CanonicalQNodeKey::Cut {
                    longitude,
                    latitude,
                } => quotient_key_for_chart_position(
                    topology.sources.frame,
                    &ExactChartPoint::from_rationals(longitude.clone(), latitude.clone()),
                )?,
            };
            let node = registry.intern(key, NodeAuthority::Source(source), work)?;
            let slot = registry
                .source_nodes
                .get_mut(source.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("source emission node is unknown".into())
                })?;
            if slot.replace(node).is_some() {
                return Err(GeometryErrorKind::Invalid(
                    "source emission node is registered twice".into(),
                )
                .into());
            }
        }
        Ok(registry)
    }

    pub(in crate::geometry::topology) fn source_node(
        &self,
        node: QNodeId,
    ) -> Result<EmissionNodeId> {
        let source = self.source_nodes.get(node.0 as usize).copied().flatten();
        source.ok_or_else(|| {
            GeometryErrorKind::Invalid("Q node is outside the emitted source inventory".into())
                .into()
        })
    }

    pub(in crate::geometry::topology) fn intern_computed(
        &mut self,
        frame: GeographicFrame,
        point: &ExactChartPoint,
        work: &mut impl Work,
    ) -> Result<EmissionNodeId> {
        work.charge_into_error(1)?;
        let key = quotient_key_for_chart_position(frame, point)?;
        self.intern(key.clone(), NodeAuthority::Computed(key), work)
    }

    pub(in crate::geometry::topology) fn nodes(&self) -> &[EmissionNode] {
        &self.nodes
    }

    pub(super) fn intern(
        &mut self,
        key: ExactQuotientKey,
        authority: NodeAuthority,
        work: &mut impl Work,
    ) -> Result<EmissionNodeId> {
        let probe = IndexedEmissionNode {
            key: key.clone(),
            node: EmissionNodeId(0),
        };
        if let Some(previous) = self.by_exact_key.get(&probe, &mut |left, right| {
            work.charge_into_error(1)?;
            compare_quotient_keys(&left.key, &right.key)
        })? {
            return Ok(previous.node);
        }
        let node = EmissionNodeId(
            u32::try_from(self.nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        );
        self.nodes.push(EmissionNode { authority });
        self.by_exact_key
            .insert(IndexedEmissionNode { key, node }, &mut |left, right| {
                work.charge_into_error(1)?;
                compare_quotient_keys(&left.key, &right.key)
            })?;
        Ok(node)
    }
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) enum PortPosition {
    Source { spelling: PortSpelling },
    Projected { spelling: PortSpelling },
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) struct EmissionPort {
    pub(in crate::geometry::topology) node: EmissionNodeId,
    pub(in crate::geometry::topology) position: PortPosition,
    pub(in crate::geometry::topology) source_record: Option<Point>,
    pub(in crate::geometry::topology) occurrence_range: Range<usize>,
}

#[derive(Debug)]
pub(in crate::geometry::topology) struct OccurrenceSource {
    pub(in crate::geometry::topology) origin: VertexOrigin,
    pub(in crate::geometry::topology) source_trace: Option<TraceRequestLocation>,
}

#[derive(Debug)]
pub(in crate::geometry::topology) struct WrittenOccurrence {
    pub(in crate::geometry::topology) origin: VertexOrigin,
    pub(in crate::geometry::topology) source_trace: Option<TraceRequestLocation>,
    pub(in crate::geometry::topology) request_row: RequestRowId,
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) struct EmissionChord {
    pub(in crate::geometry::topology) ports: [EmissionPortId; 2],
    pub(in crate::geometry::topology) recipe: Option<FragmentRecipeId>,
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) struct EmissionPart {
    pub(in crate::geometry::topology) region: usize,
    pub(in crate::geometry::topology) face: NativeFaceId,
    pub(in crate::geometry::topology) shell: Vec<EmissionChordId>,
    pub(in crate::geometry::topology) holes: Vec<Vec<EmissionChordId>>,
}
