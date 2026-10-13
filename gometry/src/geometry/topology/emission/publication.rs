use super::sites::QCoordinates;
use super::{
    ComputedEdgeIntent, ComputedSampleOrdinates, ComputedVertexIntent, CoordSeq,
    DirectedSourceRange, DirectedSpan, EndpointSide, ExactChartPoint, ExactDyadic,
    ExactGeographicTopology, ExactRational, ExactSourceRange, ExpansionBudget, FeatureFamily,
    FeatureFamilyParent, FragmentRecipe, FragmentRecipeId, GeometryErrorKind, MOrdinate,
    NodeOrdinates, PendingSourceRange, Point, PublicationInventory, PublishedFragment,
    PublishedSample, QPortId, RecipeWalk, RecipeWalkOwner, RequestRowId, ResourceError, Result,
    RootId, RootRange, SampleProducerKind, SelectedUse, SourceEdgeId, SourceTrace,
    SourceTraceSample, TraceRequestLocation, TraceRowsBuilder, ZOrdinate,
    interpolate_exact_reference_coordinate, interpolate_exact_reference_point,
    pending_source_ranges, producer_ranges, producer_step_extends_run,
    publish_computed_chain_with_vertex_intents_and_samples,
};
use crate::geographic::retained_exact::sites::retained_exact_native_site_with_work;
use crate::work::Work;

#[derive(Default)]
pub(in crate::geometry::topology) struct FragmentRecipes {
    pub(super) values: Vec<FragmentRecipe>,
    pub(super) walks: Vec<RecipeWalk>,
}

impl FragmentRecipes {
    pub(in crate::geometry::topology) fn as_slice(&self) -> &[FragmentRecipe] {
        &self.values
    }

    pub(in crate::geometry::topology) fn record_walk(
        &mut self,
        owner: RecipeWalkOwner,
        fragments: Vec<FragmentRecipeId>,
    ) {
        self.walks.push(RecipeWalk { owner, fragments });
    }

    pub(in crate::geometry::topology) fn walks(&self) -> &[RecipeWalk] {
        &self.walks
    }

    #[cfg(test)]
    pub(in crate::geometry::topology) fn source_trace(
        &self,
        walk: &RecipeWalk,
        work: &mut impl Work,
    ) -> Result<SourceTrace> {
        self.source_trace_fragments(&walk.fragments, work)
    }

    pub(in crate::geometry::topology) fn source_trace_fragment(
        &self,
        recipe: FragmentRecipeId,
        work: &mut impl Work,
    ) -> Result<SourceTrace> {
        self.source_trace_fragments(std::slice::from_ref(&recipe), work)
    }

    pub(super) fn source_trace_fragments(
        &self,
        fragments: &[FragmentRecipeId],
        work: &mut impl Work,
    ) -> Result<SourceTrace> {
        let mut rows = TraceRowsBuilder::new(work)?;
        for &fragment_id in fragments {
            work.charge_into_error(1)?;
            let fragment = self.values.get(fragment_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("source trace fragment is unknown".into())
            })?;
            let row_capacity =
                fragment
                    .ranges
                    .iter()
                    .try_fold(0_usize, |count, range| -> Result<usize> {
                        work.charge_into_error(1)?;
                        let range_slots = 1_usize
                            .checked_add(usize::from(range.endpoint_only))
                            .ok_or(ResourceError::SizeOverflow)?;
                        count
                            .checked_add(range_slots)
                            .ok_or_else(|| ResourceError::SizeOverflow.into())
                    })?;
            work.charge_into_error(
                u64::try_from(row_capacity).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut row = Vec::with_capacity(row_capacity);
            for range in &fragment.ranges {
                work.charge_into_error(1)?;
                let root = RootId::from_index(range.source_edge.0);
                if range.endpoint_only {
                    row.push(RootRange::point_with_work(
                        root,
                        range.range.start.try_clone_with_work(work)?,
                        range.producer_start.try_clone_with_work(work)?,
                        work,
                    )?);
                    row.push(RootRange::point_with_work(
                        root,
                        range.range.end.try_clone_with_work(work)?,
                        range.producer_end.try_clone_with_work(work)?,
                        work,
                    )?);
                } else {
                    row.push(RootRange::from_mapped_with_work(
                        root,
                        range.range.start.try_clone_with_work(work)?,
                        range.range.end.try_clone_with_work(work)?,
                        range.producer_start.try_clone_with_work(work)?,
                        range.producer_end.try_clone_with_work(work)?,
                        work,
                    )?);
                }
            }
            rows.push_row(row, work)?;
        }
        Ok(SourceTrace {
            rows: rows.finish(work)?,
            samples: Vec::new(),
        })
    }

    #[cfg(test)]
    pub(in crate::geometry::topology) fn publish(
        &self,
        topology: &ExactGeographicTopology,
        coordinates: &QCoordinates,
        operation: &'static str,
        work: &mut impl Work,
    ) -> Result<PublicationInventory> {
        let mut budget = ExpansionBudget::new(operation, "computed edges");
        self.publish_with_expansion_budget(topology, coordinates, &mut budget, work)
    }

    pub(in crate::geometry::topology) fn publish_with_expansion_budget(
        &self,
        topology: &ExactGeographicTopology,
        coordinates: &QCoordinates,
        budget: &mut ExpansionBudget,
        work: &mut impl Work,
    ) -> Result<PublicationInventory> {
        let mut fragments = Vec::with_capacity(self.values.len());
        let mut samples = Vec::new();
        let mut counts = vec![0_usize; self.values.len()];
        for recipe in &self.values {
            work.charge_into_error(1)?;
            let exact_endpoints = recipe.reference_endpoints(topology, work)?;
            let source_start_record =
                recipe.source_endpoint_record(topology, EndpointSide::Start, work)?;
            let source_end_record =
                recipe.source_endpoint_record(topology, EndpointSide::End, work)?;
            let start = match source_start_record {
                Some(point) => point,
                None => coordinates.point(topology, recipe.endpoints[0])?,
            };
            let end = match source_end_record {
                Some(point) => point,
                None => coordinates.point(topology, recipe.endpoints[1])?,
            };
            budget.add(2)?;
            let publisher_start = Point::new_raw_xy(start.x, start.y);
            let publisher_end = Point::new_raw_xy(end.x, end.y);
            let source = CoordSeq::try_from_points(&[publisher_start, publisher_end])?;
            let mut publication_samples = Vec::new();
            let publication = publish_computed_chain_with_vertex_intents_and_samples(
                topology.sources.frame,
                &source,
                false,
                &mut *budget,
                |_, _, _| {
                    Ok(ComputedEdgeIntent::exact_lifted([
                        &exact_endpoints[0].x,
                        &exact_endpoints[1].x,
                    ]))
                },
                |index, _| {
                    if (index == 0 && source_start_record.is_some())
                        || (index == 1 && source_end_record.is_some())
                    {
                        ComputedVertexIntent::Preserved
                    } else {
                        ComputedVertexIntent::Sampled
                    }
                },
                |_, _, _, parameter, _| {
                    let parameter = ExactRational::from_f64(parameter);
                    let y = interpolate_exact_reference_coordinate(
                        &exact_endpoints[0].y,
                        &exact_endpoints[1].y,
                        &parameter,
                        work,
                    )?
                    .round_with_work(work)?;
                    Ok(ComputedSampleOrdinates {
                        y,
                        z: ZOrdinate(None),
                        m: MOrdinate(None),
                    })
                },
                |edge_index, _, _, parameter, _, point| {
                    if parameter > 0.0 && parameter < 1.0 {
                        publication_samples.push((edge_index, parameter, point));
                    }
                    Ok(())
                },
            )?;
            let published_points =
                publication.unwrap_or_else(|| vec![publisher_start, publisher_end]);
            if publication_samples
                .iter()
                .any(|(edge_index, ..)| *edge_index != 0)
            {
                return Err(GeometryErrorKind::Invalid(
                    "single-fragment publication returned another source edge".into(),
                )
                .into());
            }
            let original_trace = self.source_trace_fragment(recipe.id, work)?;
            work.charge_into_error(
                u64::try_from(publication_samples.len())
                    .map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut trace_samples = Vec::with_capacity(publication_samples.len());
            for (sample_index, &(edge_index, parameter, _)) in
                publication_samples.iter().enumerate()
            {
                let written_vertex = sample_index
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                trace_samples.push(SourceTraceSample {
                    edge_index,
                    local_parameter: ExactRational::from_f64(parameter),
                    written_vertex,
                });
            }
            let trace = original_trace.split_at_samples(&trace_samples, work)?;
            if trace.rows.edge_count().saturating_add(1) != published_points.len()
                || trace.samples.len() != publication_samples.len()
            {
                return Err(GeometryErrorKind::Invalid(
                    "publication points and source trace lost ordinal alignment".into(),
                )
                .into());
            }
            let mut published_points = published_points;
            if let Some(record) = source_start_record {
                *published_points.first_mut().ok_or_else(|| {
                    GeometryErrorKind::Invalid("publication chain has no start record".into())
                })? = record;
            }
            if let Some(record) = source_end_record {
                *published_points.last_mut().ok_or_else(|| {
                    GeometryErrorKind::Invalid("publication chain has no end record".into())
                })? = record;
            }
            let sample_start = samples.len();
            for (occurrence, (edge_index, parameter, point)) in
                publication_samples.into_iter().enumerate()
            {
                work.charge_into_error(1)?;
                let trace_sample = trace.samples.get(occurrence).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "publication trace lost an accepted sample row".into(),
                    )
                })?;
                let parameter = ExactRational::from_f64(parameter);
                let exact_point =
                    interpolate_exact_reference_point(&exact_endpoints, &parameter, work)?;
                let retained_site = retained_exact_native_site_with_work(
                    topology.sources.frame.longitude(),
                    &exact_point.x,
                    work,
                )?;
                let period_offset =
                    ExactRational::from_dyadic(ExactDyadic::from_i64(retained_site.periods()));
                let published_x = ExactRational::from_f64(point.x).try_add_with_work(
                    &ExactRational::from_f64(topology.sources.frame.longitude().period())
                        .try_product_with_work(&period_offset, work)?,
                    work,
                )?;
                if trace_sample.written_vertex != occurrence.saturating_add(1)
                    || trace_sample.edge != edge_index
                    || !trace_sample.parameter.try_eq_with_work(&parameter, work)?
                    || published_points.get(trace_sample.written_vertex) != Some(&point)
                {
                    return Err(GeometryErrorKind::Invalid(
                        "accepted publication sample does not match its written point".into(),
                    )
                    .into());
                }
                let request_row = RequestRowId::from_index(samples.len())?;
                samples.push(PublishedSample {
                    request_row,
                    family: FeatureFamily {
                        parent: FeatureFamilyParent::Fragment(recipe.id),
                        producer: SampleProducerKind::Publication,
                        occurrence: u32::try_from(occurrence)
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    },
                    parameter,
                    written_vertex: trace_sample.written_vertex,
                    point,
                    exact_point,
                    published_chart_point: ExactChartPoint::from_rationals(
                        published_x,
                        ExactRational::from_f64(point.y),
                    ),
                    source_trace: trace_sample
                        .has_source
                        .then_some(TraceRequestLocation::Sample {
                            recipe: recipe.id,
                            row: trace_sample.request_row,
                        }),
                    endpoint_only: trace_sample.endpoint_only,
                });
            }
            let sample_end = samples.len();
            *counts.get_mut(recipe.id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("publication recipe count is unknown".into())
            })? = trace.samples.len();
            fragments.push(PublishedFragment {
                recipe: recipe.id,
                points: published_points,
                trace,
                sample_range: sample_start..sample_end,
                source_records: [source_start_record, source_end_record],
                exact_endpoints,
            });
        }
        let ordinates = vec![NodeOrdinates::default(); samples.len()];
        Ok(PublicationInventory {
            fragments,
            samples,
            counts,
            ordinates,
        })
    }

    pub(in crate::geometry::topology) fn push_source_edge(
        &mut self,
        endpoints: [QPortId; 2],
        source_edge: SourceEdgeId,
        range: ExactSourceRange,
        exact_displacement: ExactRational,
        use_kind: SelectedUse,
        work: &mut impl Work,
    ) -> Result<FragmentRecipeId> {
        work.charge_into_error(1)?;
        let id = FragmentRecipeId(
            u32::try_from(self.values.len()).map_err(|_| ResourceError::SizeOverflow)?,
        );
        self.values.push(FragmentRecipe {
            id,
            endpoints,
            ranges: vec![DirectedSourceRange {
                source_edge,
                range,
                emitted: true,
                producer_start: ExactRational::zero(),
                producer_end: ExactRational::one(),
                endpoint_only: false,
            }],
            exact_displacement,
            use_kind,
        });
        Ok(id)
    }

    pub(in crate::geometry::topology) fn push_walk(
        &mut self,
        topology: &ExactGeographicTopology,
        spans: &[DirectedSpan],
        closed: bool,
        use_kind: SelectedUse,
        work: &mut impl Work,
    ) -> Result<Vec<FragmentRecipeId>> {
        let mut emitted = Vec::new();
        let mut first_port = None;
        let mut last_port = None;
        let mut fragment_start = spans.first().map(|span| span.from);
        let mut ranges = Vec::new();
        let mut longitude_displacement = ExactRational::zero();
        let mut latitude_displacement = ExactRational::zero();
        let mut producer_supported = true;
        let mut previous_to: Option<QPortId> = None;

        for directed in spans {
            work.charge_into_error(1)?;
            let span = topology
                .q
                .spans
                .get(directed.span.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("emission recipe span is unknown".into())
                })?;
            if !span.ports.contains(&directed.from) || !span.ports.contains(&directed.to) {
                return Err(GeometryErrorKind::Invalid(
                    "emission recipe endpoints do not belong to their span".into(),
                )
                .into());
            }
            if directed.owner_index >= span.owners.len() {
                return Err(GeometryErrorKind::Invalid(
                    "emission recipe owner is outside its source span".into(),
                )
                .into());
            }
            let from = topology
                .q
                .ports
                .get(directed.from.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("emission recipe start port is unknown".into())
                })?;
            let to = topology
                .q
                .ports
                .get(directed.to.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("emission recipe end port is unknown".into())
                })?;
            if let Some(previous) = previous_to {
                let previous = topology.q.ports.get(previous.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("emission recipe previous port is unknown".into())
                })?;
                if previous.node != from.node {
                    return Err(GeometryErrorKind::Invalid(
                        "emission recipe walk contains disconnected spans".into(),
                    )
                    .into());
                }
            }
            if !from.cut {
                match last_port {
                    None => {
                        if !ranges.is_empty() {
                            let start = fragment_start.ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "emission recipe has no current start port".into(),
                                )
                            })?;
                            self.flush_pending_fragment(
                                start,
                                directed.from,
                                &mut ranges,
                                &mut longitude_displacement,
                                &mut latitude_displacement,
                                producer_supported,
                                use_kind,
                                &mut emitted,
                                work,
                            )?;
                        }
                        first_port = Some(directed.from);
                        last_port = Some(directed.from);
                        fragment_start = Some(directed.from);
                        producer_supported = true;
                    },
                    Some(previous)
                        if previous != directed.from || fragment_start != Some(directed.from) =>
                    {
                        if !ranges.is_empty() {
                            let start = fragment_start.ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "emission recipe has no current start port".into(),
                                )
                            })?;
                            self.flush_pending_fragment(
                                start,
                                directed.from,
                                &mut ranges,
                                &mut longitude_displacement,
                                &mut latitude_displacement,
                                producer_supported,
                                use_kind,
                                &mut emitted,
                                work,
                            )?;
                        }
                        last_port = Some(directed.from);
                        fragment_start = Some(directed.from);
                        producer_supported = true;
                    },
                    Some(_) => {},
                }
            } else if fragment_start.is_none() {
                fragment_start = Some(directed.from);
            }

            let longitude_step = to.point.x.try_subtract_with_work(&from.point.x, work)?;
            let latitude_step = to.point.y.try_subtract_with_work(&from.point.y, work)?;
            let collapsed_pole = topology
                .q
                .span_is_collapsed_pole(directed.span.0 as usize)?;
            let point_support =
                collapsed_pole || (longitude_step.is_zero() && latitude_step.is_zero());
            let mut producer_longitude_end =
                longitude_displacement.try_add_with_work(&longitude_step, work)?;
            let mut producer_latitude_end =
                latitude_displacement.try_add_with_work(&latitude_step, work)?;
            if !ranges.is_empty() {
                let extends_affine_run = producer_step_extends_run(
                    &longitude_displacement,
                    &latitude_displacement,
                    &longitude_step,
                    &latitude_step,
                    producer_supported,
                    point_support,
                    work,
                )?;
                if !extends_affine_run {
                    let previous = previous_to.ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "non-affine producer transition has no preceding Q port".into(),
                        )
                    })?;
                    let start = fragment_start.ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "non-affine producer transition has no recipe start port".into(),
                        )
                    })?;
                    self.flush_pending_fragment(
                        start,
                        previous,
                        &mut ranges,
                        &mut longitude_displacement,
                        &mut latitude_displacement,
                        producer_supported,
                        use_kind,
                        &mut emitted,
                        work,
                    )?;
                    fragment_start = Some(directed.from);
                    producer_supported = true;
                    producer_longitude_end = longitude_step.try_clone_with_work(work)?;
                    producer_latitude_end = latitude_step.try_clone_with_work(work)?;
                }
            }
            let pending = pending_source_ranges(
                span,
                *directed,
                &longitude_displacement,
                &producer_longitude_end,
                &latitude_displacement,
                &producer_latitude_end,
                work,
            )?;
            producer_supported &= !point_support;
            ranges.extend(pending);
            longitude_displacement = producer_longitude_end;
            latitude_displacement = producer_latitude_end;
            previous_to = Some(directed.to);

            if point_support {
                let start = fragment_start.ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "point producer support has no recipe start port".into(),
                    )
                })?;
                self.flush_pending_fragment(
                    start,
                    directed.to,
                    &mut ranges,
                    &mut longitude_displacement,
                    &mut latitude_displacement,
                    false,
                    use_kind,
                    &mut emitted,
                    work,
                )?;
                fragment_start = Some(directed.to);
                producer_supported = true;
                if !to.cut {
                    first_port.get_or_insert(directed.to);
                    last_port = Some(directed.to);
                }
            } else if !to.cut {
                let start = fragment_start.ok_or_else(|| {
                    GeometryErrorKind::Invalid("emission recipe has no current start port".into())
                })?;
                self.flush_pending_fragment(
                    start,
                    directed.to,
                    &mut ranges,
                    &mut longitude_displacement,
                    &mut latitude_displacement,
                    producer_supported,
                    use_kind,
                    &mut emitted,
                    work,
                )?;
                fragment_start = Some(directed.to);
                producer_supported = true;
                first_port.get_or_insert(directed.to);
                last_port = Some(directed.to);
            }
        }

        if !ranges.is_empty() {
            let start = fragment_start.ok_or_else(|| {
                GeometryErrorKind::Invalid("emission recipe has no final start port".into())
            })?;
            let end = spans.last().map_or(start, |span| span.to);
            self.flush_pending_fragment(
                start,
                end,
                &mut ranges,
                &mut longitude_displacement,
                &mut latitude_displacement,
                producer_supported,
                use_kind,
                &mut emitted,
                work,
            )?;
        } else if closed
            && let (Some(first), Some(last)) = (first_port, last_port)
            && first != last
        {
            self.push_fragment(
                last,
                first,
                Vec::new(),
                ExactRational::zero(),
                &ExactRational::zero(),
                true,
                use_kind,
                &mut emitted,
                work,
            )?;
        }
        Ok(emitted)
    }

    pub(super) fn flush_pending_fragment(
        &mut self,
        from: QPortId,
        to: QPortId,
        ranges: &mut Vec<PendingSourceRange>,
        longitude_displacement: &mut ExactRational,
        latitude_displacement: &mut ExactRational,
        support_allowed: bool,
        use_kind: SelectedUse,
        emitted: &mut Vec<FragmentRecipeId>,
        work: &mut impl Work,
    ) -> Result<()> {
        if ranges.is_empty() {
            return Ok(());
        }
        let longitude_displacement =
            std::mem::replace(longitude_displacement, ExactRational::zero());
        let latitude_displacement = std::mem::replace(latitude_displacement, ExactRational::zero());
        self.push_fragment(
            from,
            to,
            std::mem::take(ranges),
            longitude_displacement,
            &latitude_displacement,
            support_allowed,
            use_kind,
            emitted,
            work,
        )
    }

    pub(super) fn push_fragment(
        &mut self,
        from: QPortId,
        to: QPortId,
        ranges: Vec<PendingSourceRange>,
        longitude_displacement: ExactRational,
        latitude_displacement: &ExactRational,
        support_allowed: bool,
        use_kind: SelectedUse,
        emitted: &mut Vec<FragmentRecipeId>,
        work: &mut impl Work,
    ) -> Result<()> {
        work.charge_into_error(1)?;
        let ranges = producer_ranges(
            ranges,
            &longitude_displacement,
            latitude_displacement,
            support_allowed,
            work,
        )?;
        let id = FragmentRecipeId(
            u32::try_from(self.values.len()).map_err(|_| ResourceError::SizeOverflow)?,
        );
        self.values.push(FragmentRecipe {
            id,
            endpoints: [from, to],
            ranges,
            exact_displacement: longitude_displacement,
            use_kind,
        });
        emitted.push(id);
        Ok(())
    }
}
