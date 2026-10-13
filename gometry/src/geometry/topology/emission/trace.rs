use super::{
    BTreeMap, ChartSpanId, ExactRational, FeatureFamilyRegistry, FragmentRecipeId,
    GeometryErrorKind, OccurrenceSource, QPortId, RequestRowId, ResourceError, Result, RootId,
    RootRange, RootRequest, Shape, TraceRows, TraceRowsBuilder, VertexOrigin, Work,
    try_sort_by_with_work,
};

impl SourceTrace {
    pub(super) fn split_at_samples(
        &self,
        samples: &[SourceTraceSample],
        work: &mut impl Work,
    ) -> Result<Self> {
        let edge_count = self.rows.edge_count();
        let input_vertex_count = edge_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge_into_error(
            u64::try_from(edge_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut new_samples_by_edge = vec![0_usize; edge_count];
        for sample in samples {
            work.charge_into_error(1)?;
            let count = new_samples_by_edge
                .get_mut(sample.edge_index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "publication sample references an unknown source edge".into(),
                    )
                })?;
            *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
        work.charge_into_error(
            u64::try_from(input_vertex_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut new_samples_before_vertex = vec![0_usize; input_vertex_count];
        for vertex in 1..input_vertex_count {
            work.charge_into_error(1)?;
            let previous_vertex = vertex.checked_sub(1).ok_or(ResourceError::SizeOverflow)?;
            let previous = *new_samples_before_vertex
                .get(previous_vertex)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("publication sample prefix is incomplete".into())
                })?;
            let edge_samples = *new_samples_by_edge.get(previous_vertex).ok_or_else(|| {
                GeometryErrorKind::Invalid("publication sample edge count is incomplete".into())
            })?;
            *new_samples_before_vertex.get_mut(vertex).ok_or_else(|| {
                GeometryErrorKind::Invalid("publication sample prefix is incomplete".into())
            })? = previous
                .checked_add(edge_samples)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        let output_vertex_count = input_vertex_count
            .checked_add(samples.len())
            .ok_or(ResourceError::SizeOverflow)?;
        let output_sample_count = self
            .samples
            .len()
            .checked_add(samples.len())
            .ok_or(ResourceError::SizeOverflow)?;
        let mut output_rows = TraceRowsBuilder::new(work)?;
        work.charge_into_error(
            u64::try_from(output_sample_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut output_samples = Vec::with_capacity(output_sample_count);
        let mut request_row_index = 0_usize;
        for sample in &self.samples {
            work.charge_into_error(1)?;
            if sample.written_vertex > edge_count {
                return Err(GeometryErrorKind::Invalid(
                    "previous source sample is outside the input vertex sequence".into(),
                )
                .into());
            }
            let inserted_before = *new_samples_before_vertex
                .get(sample.written_vertex)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("previous source sample index is unknown".into())
                })?;
            let written_vertex = sample
                .written_vertex
                .checked_add(inserted_before)
                .ok_or(ResourceError::SizeOverflow)?;
            if written_vertex >= output_vertex_count {
                return Err(GeometryErrorKind::Invalid(
                    "previous source sample exceeds the rewritten point chain".into(),
                )
                .into());
            }
            let requests = self
                .rows
                .sample_requests_at(sample.request_row.index(), work)?;
            work.charge_into_error(
                u64::try_from(requests.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let point_ranges = requests
                .iter()
                .map(|request| {
                    RootRange::point_with_work(
                        request.root(),
                        request.parameter().try_clone_with_work(work)?,
                        ExactRational::zero(),
                        work,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            output_rows.push_sample_row(point_ranges, work)?;
            output_samples.push(TracedSourceSample {
                written_vertex,
                edge: sample.edge,
                parameter: sample.parameter.try_clone_with_work(work)?,
                request_row: RequestRowId::from_index(request_row_index)?,
                has_source: sample.has_source,
                endpoint_only: sample.endpoint_only,
            });
            request_row_index = request_row_index
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        let zero = ExactRational::zero();
        let one = ExactRational::one();
        let mut sample_index = 0_usize;
        let mut previous_new_written_vertex = None;
        for edge_index in 0..edge_count {
            work.charge_into_error(1)?;
            let boundary_capacity = new_samples_by_edge
                .get(edge_index)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("publication sample edge count is incomplete".into())
                })?
                .checked_add(2)
                .ok_or(ResourceError::SizeOverflow)?;
            work.charge_into_error(
                u64::try_from(boundary_capacity).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut boundaries = Vec::with_capacity(boundary_capacity);
            boundaries.push(zero.try_clone_with_work(work)?);
            let mut previous_parameter = zero.try_clone_with_work(work)?;
            while let Some(sample) = samples.get(sample_index)
                && sample.edge_index == edge_index
            {
                work.charge_into_error(1)?;
                if sample
                    .local_parameter
                    .try_cmp_with_work(&previous_parameter, work)?
                    .is_le()
                    || sample
                        .local_parameter
                        .try_cmp_with_work(&one, work)?
                        .is_ge()
                    || previous_new_written_vertex
                        .is_some_and(|previous| previous >= sample.written_vertex)
                {
                    return Err(GeometryErrorKind::Invalid(
                        "accepted publication samples are not in written order".into(),
                    )
                    .into());
                }
                let (requests, endpoint_only) = self.rows.requests_with_support_at(
                    edge_index,
                    &sample.local_parameter,
                    work,
                )?;
                let has_source = !requests.is_empty();
                work.charge_into_error(
                    u64::try_from(requests.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                let point_ranges = requests
                    .iter()
                    .map(|request| {
                        RootRange::point_with_work(
                            request.root(),
                            request.parameter().try_clone_with_work(work)?,
                            zero.try_clone_with_work(work)?,
                            work,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                output_rows.push_sample_row(point_ranges, work)?;
                output_samples.push(TracedSourceSample {
                    written_vertex: sample.written_vertex,
                    edge: edge_index,
                    parameter: sample.local_parameter.try_clone_with_work(work)?,
                    request_row: RequestRowId::from_index(request_row_index)?,
                    has_source,
                    endpoint_only,
                });
                request_row_index = request_row_index
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                boundaries.push(sample.local_parameter.try_clone_with_work(work)?);
                previous_parameter = sample.local_parameter.try_clone_with_work(work)?;
                previous_new_written_vertex = Some(sample.written_vertex);
                sample_index = sample_index
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
            }
            boundaries.push(one.try_clone_with_work(work)?);

            for pair in boundaries.array_windows::<2>() {
                let [from, to] = pair;
                work.charge_into_error(1)?;
                output_rows.push_composed_row(self.rows.row(edge_index)?, from, to, work)?;
            }
        }
        if sample_index != samples.len() {
            return Err(GeometryErrorKind::Invalid(
                "publication sample references an unknown source edge".into(),
            )
            .into());
        }
        try_sort_by_with_work(&mut output_samples, work, |left, right, _| {
            Ok(left.written_vertex.cmp(&right.written_vertex))
        })?;
        for pair in output_samples.array_windows::<2>() {
            let [left, right] = pair;
            work.charge_into_error(1)?;
            if left.written_vertex >= right.written_vertex {
                return Err(GeometryErrorKind::Invalid(
                    "written source sample vertices are not strictly ordered".into(),
                )
                .into());
            }
        }
        Ok(Self {
            rows: output_rows.finish(work)?,
            samples: output_samples,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub(in crate::geometry::topology) struct DirectedSpan {
    pub(in crate::geometry::topology) span: ChartSpanId,
    pub(in crate::geometry::topology) from: QPortId,
    pub(in crate::geometry::topology) to: QPortId,
    pub(in crate::geometry::topology) owner_index: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) enum RecipeWalkOwner {
    Cycle(usize),
    Line(usize),
    SourceRing(usize),
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) struct RecipeWalk {
    pub(in crate::geometry::topology) owner: RecipeWalkOwner,
    pub(in crate::geometry::topology) fragments: Vec<FragmentRecipeId>,
}

pub(in crate::geometry::topology) type RecipeWalkIndex<'a> =
    BTreeMap<RecipeWalkOwner, Vec<&'a RecipeWalk>>;

pub(in crate::geometry::topology) fn index_recipe_walks<'walks>(
    walks: &'walks [RecipeWalk],
    work: &mut impl Work,
) -> Result<RecipeWalkIndex<'walks>> {
    work.charge_into_error(u64::try_from(walks.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut index = BTreeMap::new();
    for walk in walks {
        work.charge_into_error(1)?;
        index.entry(walk.owner).or_insert_with(Vec::new).push(walk);
    }
    Ok(index)
}

pub(in crate::geometry::topology) fn next_recipe_walk<'walk>(
    index: &RecipeWalkIndex<'walk>,
    cursors: &mut BTreeMap<RecipeWalkOwner, usize>,
    owner: RecipeWalkOwner,
    work: &mut impl Work,
) -> Result<&'walk RecipeWalk> {
    work.charge_into_error(1)?;
    let cursor = cursors.entry(owner).or_default();
    let walk = index
        .get(&owner)
        .and_then(|walks| walks.get(*cursor))
        .copied()
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("producer recipe walk is missing for its owner".into())
        })?;
    *cursor = cursor.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
    Ok(walk)
}

pub(in crate::geometry) struct SourceTrace {
    pub(in crate::geometry::topology) rows: TraceRows,
    pub(in crate::geometry::topology) samples: Vec<TracedSourceSample>,
}

impl SourceTrace {
    pub(in crate::geometry) fn try_clone_with_work(&self, work: &mut impl Work) -> Result<Self> {
        work.charge_into_error(
            u64::try_from(self.samples.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let rows = self.rows.try_clone_with_work(work)?;
        let mut samples = Vec::with_capacity(self.samples.len());
        for sample in &self.samples {
            samples.push(TracedSourceSample {
                written_vertex: sample.written_vertex,
                edge: sample.edge,
                parameter: sample.parameter.try_clone_with_work(work)?,
                request_row: sample.request_row,
                has_source: sample.has_source,
                endpoint_only: sample.endpoint_only,
            });
        }
        Ok(Self { rows, samples })
    }
}

pub(super) struct SourceTraceSample {
    pub(super) edge_index: usize,
    pub(super) local_parameter: ExactRational,
    pub(super) written_vertex: usize,
}

pub(in crate::geometry) struct TracedLine {
    pub(in crate::geometry) member: usize,
    pub(in crate::geometry) trace: SourceTrace,
}

impl TracedLine {
    pub(super) fn try_clone_with_work(&self, work: &mut impl Work) -> Result<Self> {
        Ok(Self {
            member: self.member,
            trace: self.trace.try_clone_with_work(work)?,
        })
    }
}

pub(in crate::geometry) struct PreparedNativeSource {
    pub(in crate::geometry) shape: Shape,
    pub(in crate::geometry) line_traces: Vec<TracedLine>,
    pub(in crate::geometry) source_members: Vec<usize>,
    pub(in crate::geometry) family_state: Option<FeatureFamilyRegistry>,
}

impl SourceTrace {
    pub(in crate::geometry) fn from_edge_rows(
        edge_rows: impl IntoIterator<Item = Vec<RootRange>>,
        work: &mut impl Work,
    ) -> Result<Self> {
        let mut rows = TraceRowsBuilder::new(work)?;
        for row in edge_rows {
            work.charge_into_error(1)?;
            rows.push_row(row, work)?;
        }
        Ok(Self {
            rows: rows.finish(work)?,
            samples: Vec::new(),
        })
    }

    pub(in crate::geometry) const fn edge_count(&self) -> usize {
        self.rows.edge_count()
    }

    pub(in crate::geometry) fn requests_at(
        &self,
        edge: usize,
        local: &ExactRational,
        work: &mut impl Work,
    ) -> Result<Vec<RootRequest>> {
        self.rows.requests_at(edge, local, work)
    }

    pub(in crate::geometry) fn append_requests_at(
        &self,
        edge: usize,
        local: &ExactRational,
        requests: &mut Vec<RootRequest>,
        work: &mut impl Work,
    ) -> Result<usize> {
        self.rows.append_requests_at(edge, local, requests, work)
    }

    pub(in crate::geometry) fn compose_edges(
        &self,
        slices: &[(usize, ExactRational, ExactRational)],
        work: &mut impl Work,
    ) -> Result<Self> {
        let edge_count = self.rows.edge_count();
        if self.samples.is_empty() {
            let mut rows = TraceRowsBuilder::new(work)?;
            for (edge, start, end) in slices {
                work.charge_into_error(1)?;
                if *edge >= edge_count {
                    return Err(GeometryErrorKind::Invalid(
                        "source trace slice references an unknown edge".into(),
                    )
                    .into());
                }
                rows.push_composed_row(self.rows.row(*edge)?, start, end, work)?;
            }
            return Ok(Self {
                rows: rows.finish(work)?,
                samples: Vec::new(),
            });
        }
        let vertex_count = edge_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge_into_error(
            u64::try_from(vertex_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut requests_by_vertex = vec![Vec::<RootRequest>::new(); vertex_count];
        for sample in &self.samples {
            work.charge_into_error(1)?;
            let requests = self
                .rows
                .sample_requests_at(sample.request_row.index(), work)?;
            let vertex_requests = requests_by_vertex
                .get_mut(sample.written_vertex)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source sample is outside its retained line trace".into(),
                    )
                })?;
            work.charge_into_error(
                u64::try_from(requests.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            vertex_requests.extend(requests);
        }

        let zero = ExactRational::zero();
        let one = ExactRational::one();
        let mut rows = TraceRowsBuilder::new(work)?;
        for (edge, start, end) in slices {
            work.charge_into_error(1)?;
            if *edge >= edge_count {
                return Err(GeometryErrorKind::Invalid(
                    "source trace slice references an unknown edge".into(),
                )
                .into());
            }
            let parents = self.rows.row(*edge)?;
            let start_requests = requests_by_vertex.get(*edge).ok_or_else(|| {
                GeometryErrorKind::Invalid("source trace start vertex is unknown".into())
            })?;
            let end_vertex = (*edge).checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            let end_requests = requests_by_vertex.get(end_vertex).ok_or_else(|| {
                GeometryErrorKind::Invalid("source trace end vertex is unknown".into())
            })?;
            let row_capacity = parents
                .len()
                .checked_add(start_requests.len())
                .and_then(|capacity| capacity.checked_add(end_requests.len()))
                .ok_or(ResourceError::SizeOverflow)?;
            work.charge_into_error(
                u64::try_from(row_capacity).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut row = Vec::with_capacity(row_capacity);
            for parent in parents {
                work.charge_into_error(1)?;
                if let Some(child) = parent.compose_with_work(start, end, work)? {
                    row.push(child);
                }
            }
            if start.try_cmp_with_work(&zero, work)?.is_eq() {
                for request in start_requests {
                    work.charge_into_error(1)?;
                    row.push(RootRange::point_with_work(
                        request.root(),
                        request.parameter().try_clone_with_work(work)?,
                        zero.try_clone_with_work(work)?,
                        work,
                    )?);
                }
            }
            if end.try_cmp_with_work(&one, work)?.is_eq() {
                for request in end_requests {
                    work.charge_into_error(1)?;
                    row.push(RootRange::point_with_work(
                        request.root(),
                        request.parameter().try_clone_with_work(work)?,
                        one.try_clone_with_work(work)?,
                        work,
                    )?);
                }
            }
            rows.push_row(row, work)?;
        }
        Ok(Self {
            rows: rows.finish(work)?,
            samples: Vec::new(),
        })
    }
}

#[derive(Debug)]
pub(in crate::geometry::topology) enum TraceRequestLocation {
    Edge {
        recipe: FragmentRecipeId,
        edge: usize,
        parameter: ExactRational,
    },
    Sample {
        recipe: FragmentRecipeId,
        row: RequestRowId,
    },
    Root {
        root: RootId,
        parameter: ExactRational,
    },
}

impl TraceRequestLocation {
    pub(in crate::geometry::topology) fn try_clone_with_work(
        &self,
        work: &mut impl Work,
    ) -> Result<Self> {
        Ok(match self {
            Self::Edge {
                recipe,
                edge,
                parameter,
            } => Self::Edge {
                recipe: *recipe,
                edge: *edge,
                parameter: parameter.try_clone_with_work(work)?,
            },
            Self::Sample { recipe, row } => Self::Sample {
                recipe: *recipe,
                row: *row,
            },
            Self::Root { root, parameter } => Self::Root {
                root: *root,
                parameter: parameter.try_clone_with_work(work)?,
            },
        })
    }
}

pub(in crate::geometry::topology) fn native_source_sample_occurrences(
    source_trace: Option<TraceRequestLocation>,
    source_requests: &[RootRequest],
    work: &mut impl Work,
) -> Result<Vec<OccurrenceSource>> {
    let capacity = source_requests
        .len()
        .checked_add(usize::from(source_trace.is_some()))
        .ok_or(ResourceError::SizeOverflow)?;
    work.charge_into_error(u64::try_from(capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut occurrences = Vec::with_capacity(capacity);
    if let Some(source_trace) = source_trace {
        work.charge_into_error(1)?;
        occurrences.push(OccurrenceSource {
            origin: VertexOrigin::SourceSample,
            source_trace: Some(source_trace),
        });
    }
    for request in source_requests {
        work.charge_into_error(1)?;
        occurrences.push(OccurrenceSource {
            origin: VertexOrigin::SourceSample,
            source_trace: Some(TraceRequestLocation::Root {
                root: request.root(),
                parameter: request.parameter().try_clone_with_work(work)?,
            }),
        });
    }
    if occurrences.is_empty() {
        work.charge_into_error(1)?;
        occurrences.push(OccurrenceSource {
            origin: VertexOrigin::Presentation,
            source_trace: None,
        });
    }
    Ok(occurrences)
}

#[derive(Debug)]
pub(in crate::geometry::topology) struct SourceSampleOccurrence {
    pub(in crate::geometry::topology) trace: Option<TraceRequestLocation>,
    pub(in crate::geometry::topology) endpoint_only: bool,
    pub(in crate::geometry::topology) publication_sample: Option<usize>,
}

impl SourceSampleOccurrence {
    pub(in crate::geometry::topology) fn try_clone_with_work(
        &self,
        work: &mut impl Work,
    ) -> Result<Self> {
        Ok(Self {
            trace: self
                .trace
                .as_ref()
                .map(|location| location.try_clone_with_work(work))
                .transpose()?,
            endpoint_only: self.endpoint_only,
            publication_sample: self.publication_sample,
        })
    }
}

#[derive(Debug)]
pub(in crate::geometry::topology) struct TracedSourceSample {
    pub(in crate::geometry::topology) written_vertex: usize,
    pub(in crate::geometry::topology) edge: usize,
    pub(in crate::geometry::topology) parameter: ExactRational,
    pub(in crate::geometry::topology) request_row: RequestRowId,
    pub(in crate::geometry::topology) has_source: bool,
    pub(in crate::geometry::topology) endpoint_only: bool,
}

impl SourceTrace {
    pub(in crate::geometry::topology) fn owners_for_edge(
        &self,
        edge_index: usize,
    ) -> Result<&[RootRange]> {
        self.rows.row(edge_index)
    }
}
