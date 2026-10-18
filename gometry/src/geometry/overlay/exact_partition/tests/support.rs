fn certify_plan(
    plan: &BoundaryPlan,
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
) -> crate::error::Result<(Vec<[NodeId; 2]>, Vec<Segment>)> {
    let emission = plan.emission(topology, edge_of_slot);
    let embedding = partition.embed(&[&emission], "test")?;
    let segments = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(embedding.coordinates(), item))
        .collect();
    Ok((emission.items, segments))
}

fn check_plan_fans(
    plan: &BoundaryPlan,
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    segments: &[Segment],
) -> crate::error::Result<()> {
    let emission = plan.emission(topology, edge_of_slot);
    let mut coordinates = partition.points.rounded.clone();
    for (&[start, end], segment) in emission.items.iter().zip(segments) {
        coordinates[start.0 as usize] = segment.start;
        coordinates[end.0 as usize] = segment.end;
    }
    let embedding = embedding::Embedding::from_owned(coordinates);
    embedding::check_emitted_fans(
        partition,
        &embedding,
        &emission.items[..emission.chords],
        &emission.fan_spans,
        &segments[..emission.chords],
        emission.areal_chords,
    )
}

fn check_plan_faces(
    plan: &BoundaryPlan,
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    items: &[[NodeId; 2]],
    segments: &[Segment],
) -> crate::error::Result<()> {
    let emission = plan.emission(topology, edge_of_slot);
    let mut coordinates = partition.points.rounded.clone();
    for (&[start, end], segment) in items.iter().zip(segments) {
        coordinates[start.0 as usize] = segment.start;
        coordinates[end.0 as usize] = segment.end;
    }
    let embedding = embedding::Embedding::from_owned(coordinates);
    embedding::check_global_faces(
        partition,
        &embedding,
        items,
        emission
            .faces
            .as_deref()
            .expect("boundary plans carry faces"),
    )
}

#[derive(Default)]
struct WitnessLog(Vec<embedding::Witness>);

impl embedding::Sink for WitnessLog {
    fn report(&mut self, witness: embedding::Witness) -> std::ops::ControlFlow<()> {
        self.0.push(witness);
        std::ops::ControlFlow::Continue(())
    }
}

impl ExactPartition<'_> {
    fn stored_node(&self, point: XY) -> Option<NodeId> {
        let key = PointKey::new(point);
        (0..self.node_count())
            .find(|&node| self.stored_key(node) == Some(key))
            .map(|node| NodeId(node as u32))
    }

    /// The span a leftward ray from `query` meets first, by the sweep.
    fn left_contact(&self, query: NodeId) -> Option<usize> {
        shoot_left(
            &self.points,
            self.spans.len(),
            |index| [self.spans[index].start.0, self.spans[index].end.0],
            &[query.0],
        )
        .unwrap()[0]
    }

    fn crossings(&self) -> Vec<NodeId> {
        self.points
            .recipes
            .iter()
            .enumerate()
            .filter(|(_, recipe)| !matches!(recipe, NodeRecipe::Stored))
            .map(|(index, _)| NodeId(index as u32))
            .collect()
    }
}

fn seg(x0: f64, y0: f64, x1: f64, y1: f64) -> Segment {
    Segment {
        start: XY::new(x0, y0),
        end: XY::new(x1, y1),
    }
}

struct Case {
    unique: Vec<Segment>,
    provenance: SegmentProvenance,
    isolated: Vec<XY>,
}

impl Case {
    fn from_segments(segments: &[Segment]) -> Self {
        let mut edges = Vec::new();
        let mut isolated = Vec::new();
        for &segment in segments {
            if same_point(segment.start, segment.end) {
                isolated.push(segment.start);
            } else {
                edges.push(segment);
            }
        }
        let (unique, provenance) = dedup_undirected_segments_sourced(edges).unwrap();
        Self {
            unique,
            provenance,
            isolated,
        }
    }

    fn partition(&self) -> ExactPartition<'_> {
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        partition_unique_segments::<true>(
            &self.unique,
            &self.provenance,
            &self.isolated,
            &mut budget,
        )
        .unwrap()
    }
}

fn run(segments: &[Segment]) -> Case {
    Case::from_segments(segments)
}
