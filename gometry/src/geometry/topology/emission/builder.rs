use super::*;

pub(in crate::geometry::topology) struct ExpandedEmissionPlan<'topology> {
    pub(in crate::geometry::topology) topology: &'topology ExactGeographicTopology,
    pub(in crate::geometry::topology) selected_face_roots: Vec<Option<usize>>,
    pub(in crate::geometry::topology) node_registry: EmissionNodeRegistry,
    pub(in crate::geometry::topology) ports: Vec<EmissionPort>,
    pub(in crate::geometry::topology) occurrences: Vec<WrittenOccurrence>,
    pub(in crate::geometry::topology) chords: Vec<EmissionChord>,
    pub(in crate::geometry::topology) parts: Vec<EmissionPart>,
    pub(in crate::geometry::topology) lines: Vec<Vec<EmissionChordId>>,
    pub(in crate::geometry::topology) points: Vec<EmissionPortId>,
    pub(in crate::geometry::topology) whole_sphere: bool,
}

pub(in crate::geometry::topology) struct EmissionBuilder<'topology> {
    pub(super) topology: &'topology ExactGeographicTopology,
    pub(super) selected_face_roots: Vec<Option<usize>>,
    pub(super) nodes: EmissionNodeRegistry,
    pub(super) ports: Vec<EmissionPort>,
    pub(super) occurrences: Vec<WrittenOccurrence>,
    pub(super) chords: Vec<EmissionChord>,
    pub(super) parts: Vec<EmissionPart>,
    pub(super) lines: Vec<Vec<EmissionChordId>>,
    pub(super) points: Vec<EmissionPortId>,
    pub(super) whole_sphere: bool,
}

impl<'topology> EmissionBuilder<'topology> {
    pub(in crate::geometry::topology) fn new(
        topology: &'topology ExactGeographicTopology,
        selected: &QEmissionPlan,
        live_nodes: &[QNodeId],
        whole_sphere: bool,
        work: &mut impl Work,
    ) -> Result<Self> {
        Ok(Self {
            topology,
            selected_face_roots: selected.selected_face_roots.clone(),
            nodes: EmissionNodeRegistry::with_sources(topology, live_nodes, work)?,
            ports: Vec::new(),
            occurrences: Vec::new(),
            chords: Vec::new(),
            parts: Vec::new(),
            lines: Vec::new(),
            points: Vec::new(),
            whole_sphere,
        })
    }

    pub(in crate::geometry::topology) fn source_port(
        &mut self,
        port: QPortId,
        origin: VertexOrigin,
        source_record: Option<Point>,
        spelling: PortSpelling,
    ) -> Result<EmissionPortId> {
        let source =
            self.topology.q.ports.get(port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("source emission port is unknown".into())
            })?;
        if !matches!(&origin, VertexOrigin::SourceNode(node) if *node == source.node) {
            return Err(GeometryErrorKind::Invalid(
                "borrowed source port must retain its source-node origin".into(),
            )
            .into());
        }
        let node = self.nodes.source_node(source.node)?;
        self.validate_port_origin(node, origin)?;
        self.push_port(
            EmissionPort {
                node,
                position: PortPosition::Source { spelling },
                source_record,
                occurrence_range: 0..0,
            },
            vec![OccurrenceSource {
                origin,
                source_trace: None,
            }],
        )
    }

    pub(in crate::geometry::topology) fn source_node(
        &self,
        node: QNodeId,
    ) -> Result<EmissionNodeId> {
        self.nodes.source_node(node)
    }

    pub(in crate::geometry::topology) fn reserve_publication_nodes(
        &mut self,
        samples: &[PublishedSample],
        work: &mut impl Work,
    ) -> Result<Vec<EmissionNodeId>> {
        let mut nodes = Vec::with_capacity(samples.len());
        for sample in samples {
            work.charge_into_error(1)?;
            let point = sample.exact_point.try_clone_with_work(work)?;
            nodes.push(
                self.nodes
                    .intern_computed(self.topology.sources.frame, &point, work)?,
            );
        }
        Ok(nodes)
    }

    pub(in crate::geometry::topology) fn intern_point(
        &mut self,
        point: &ExactChartPoint,
        work: &mut impl Work,
    ) -> Result<EmissionNodeId> {
        let point = point.try_clone_with_work(work)?;
        self.nodes
            .intern_computed(self.topology.sources.frame, &point, work)
    }

    pub(in crate::geometry::topology) fn chord(
        &mut self,
        ports: [EmissionPortId; 2],
        recipe: Option<FragmentRecipeId>,
    ) -> Result<EmissionChordId> {
        for port in ports {
            self.ports.get(port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("emission chord port is unknown".into())
            })?;
        }
        let id = EmissionChordId(
            u32::try_from(self.chords.len()).map_err(|_| ResourceError::SizeOverflow)?,
        );
        self.chords.push(EmissionChord { ports, recipe });
        Ok(id)
    }

    pub(in crate::geometry::topology) fn part(
        &mut self,
        face: NativeFaceId,
        region: usize,
        shell: Vec<EmissionChordId>,
        holes: Vec<Vec<EmissionChordId>>,
    ) -> Result<ChartPartId> {
        for chord in shell.iter().chain(holes.iter().flatten()) {
            self.chords.get(chord.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("emission part chord is unknown".into())
            })?;
        }
        let id =
            ChartPartId(u32::try_from(self.parts.len()).map_err(|_| ResourceError::SizeOverflow)?);
        self.parts.push(EmissionPart {
            region,
            face,
            shell,
            holes,
        });
        Ok(id)
    }

    pub(in crate::geometry::topology) fn line(
        &mut self,
        chords: Vec<EmissionChordId>,
    ) -> Result<()> {
        for chord in &chords {
            self.chords.get(chord.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("emission line chord is unknown".into())
            })?;
        }
        self.lines.push(chords);
        Ok(())
    }

    pub(in crate::geometry::topology) fn point(&mut self, port: EmissionPortId) -> Result<()> {
        self.ports
            .get(port.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("emission point port is unknown".into()))?;
        self.points.push(port);
        Ok(())
    }

    pub(in crate::geometry::topology) fn port(
        &mut self,
        data: NativePortData,
    ) -> Result<EmissionPortId> {
        for occurrence in &data.occurrences {
            self.validate_port_origin(data.node, occurrence.origin)?;
        }
        self.push_port(
            EmissionPort {
                node: data.node,
                position: PortPosition::Projected {
                    spelling: data.spelling,
                },
                source_record: data.source_record,
                occurrence_range: 0..0,
            },
            data.occurrences,
        )
    }

    pub(in crate::geometry::topology) const fn nodes(&self) -> &EmissionNodeRegistry {
        &self.nodes
    }

    pub(in crate::geometry::topology) fn finish(self) -> ExpandedEmissionPlan<'topology> {
        ExpandedEmissionPlan {
            topology: self.topology,
            selected_face_roots: self.selected_face_roots,
            node_registry: self.nodes,
            ports: self.ports,
            occurrences: self.occurrences,
            chords: self.chords,
            parts: self.parts,
            lines: self.lines,
            points: self.points,
            whole_sphere: self.whole_sphere,
        }
    }

    pub(super) fn push_port(
        &mut self,
        mut port: EmissionPort,
        occurrences: Vec<OccurrenceSource>,
    ) -> Result<EmissionPortId> {
        let start = self.occurrences.len();
        let end = start
            .checked_add(occurrences.len())
            .ok_or(ResourceError::SizeOverflow)?;
        for (offset, occurrence) in occurrences.into_iter().enumerate() {
            let request_row = start
                .checked_add(offset)
                .ok_or(ResourceError::SizeOverflow)?;
            self.occurrences.push(WrittenOccurrence {
                origin: occurrence.origin,
                source_trace: occurrence.source_trace,
                request_row: RequestRowId::from_index(request_row)?,
            });
        }
        port.occurrence_range = start..end;
        let id = EmissionPortId(
            u32::try_from(self.ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
        );
        self.ports.push(port);
        Ok(id)
    }

    pub(super) fn validate_port_origin(
        &self,
        node: EmissionNodeId,
        origin: VertexOrigin,
    ) -> Result<()> {
        let VertexOrigin::SourceNode(source) = origin else {
            return Ok(());
        };
        let Some(EmissionNode {
            authority: NodeAuthority::Source(authority),
        }) = self.nodes.nodes().get(node.0 as usize)
        else {
            return Err(GeometryErrorKind::Invalid(
                "source-origin port references a computed emission node".into(),
            )
            .into());
        };
        if *authority != source {
            return Err(GeometryErrorKind::Invalid(
                "source-origin port disagrees with its emission node authority".into(),
            )
            .into());
        }
        Ok(())
    }
}
