use super::super::{
    BuiltQPublication, EmissionNodeRegistry, ExactChartPoint, ExactGeographicTopology,
    ExactNodePosition, ExactQuotientKey, ExactRational, GeometryErrorKind, NodeAuthority,
    PublishedEndpoint, QCandidate, QCoordinates, QEmbeddingEmission, QGraphNode, QPortId,
    QPublishedChord, QSourceSpanRange, ResourceError, Result, Segment, SourceSampleOccurrence,
    candidate_chart_point_with_work, chord_boxes, compare_quotient_keys_with_work,
    port_swept_radius, published_domains, q_port_assigned_point_if_representable,
    quotient_key_for_chart_position_with_work, rounded_chart_point_with_work,
    source_port_for_exact_point, source_sample_at_fragment_endpoint, trace_source_nodes,
    try_sort_by_with_work,
};
use crate::work::Work;

pub(in crate::geometry::topology::q_embedding) fn publication_sample_index(
    endpoint: &PublishedEndpoint,
) -> Option<usize> {
    match endpoint {
        PublishedEndpoint::Source { sample, .. } => sample.as_ref()?.publication_sample,
        PublishedEndpoint::Published { sample, .. } => Some(*sample),
    }
}
pub(in crate::geometry::topology::q_embedding) fn publication_endpoint_reference_offset(
    topology: &ExactGeographicTopology,
    publication: &super::super::super::emission::PublicationInventory,
    publication_exact_positions: &[Option<ExactNodePosition>],
    endpoint: &PublishedEndpoint,
    reference: &ExactChartPoint,
    budget: &mut impl Work,
) -> Result<ExactRational> {
    let (base, pole_spelling) = match endpoint {
        PublishedEndpoint::Source { port, sample, .. } => {
            let port = topology.q.ports.get(port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication reference port is unknown".into())
            })?;
            (
                port.point.try_clone_with_work(budget)?,
                sample
                    .as_ref()
                    .is_some_and(|sample| sample.publication_sample.is_some()),
            )
        },
        PublishedEndpoint::Published { node, sample } => {
            let sample_row = publication.samples.get(*sample).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication reference sample is unknown".into())
            })?;
            if !sample_row.exact_point.try_eq_with_work(reference, budget)? {
                return Err(GeometryErrorKind::Invalid(
                    "published endpoint lost its occurrence reference point".into(),
                )
                .into());
            }
            let position = publication_exact_positions
                .get(node.0 as usize)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "computed publication endpoint has no representative reference".into(),
                    )
                })?;
            (
                ExactChartPoint::from_rationals(
                    position.longitude.clone(),
                    position.latitude.clone(),
                ),
                true,
            )
        },
    };
    let base_key =
        quotient_key_for_chart_position_with_work(topology.sources.frame, &base, budget)?;
    let reference_key =
        quotient_key_for_chart_position_with_work(topology.sources.frame, reference, budget)?;
    if !compare_quotient_keys_with_work(&base_key, &reference_key, budget)?.is_eq() {
        return Err(GeometryErrorKind::Invalid(
            "publication endpoint reference changed its exact quotient identity".into(),
        )
        .into());
    }
    let offset = reference.x.try_subtract_with_work(&base.x, budget)?;
    if pole_spelling
        && matches!(
            base_key,
            ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole
        )
    {
        return Ok(offset);
    }
    let period = ExactRational::from_f64(topology.sources.frame.longitude().period());
    let turns = offset.try_quotient_with_work(&period, budget)?;
    if !turns.is_integer() {
        return Err(GeometryErrorKind::Invalid(
            "publication endpoint reference differs by a nonintegral longitude period".into(),
        )
        .into());
    }
    Ok(offset)
}
pub(in crate::geometry::topology::q_embedding) fn lift_assigned_publication_point(
    assigned: &ExactChartPoint,
    reference_offset: &ExactRational,
    budget: &mut impl Work,
) -> Result<ExactChartPoint> {
    Ok(ExactChartPoint::from_rationals(
        assigned.x.try_add_with_work(reference_offset, budget)?,
        assigned.y.try_clone_with_work(budget)?,
    ))
}
pub(in crate::geometry::topology::q_embedding) fn nearest_publication_source_port_point(
    topology: &ExactGeographicTopology,
    nearest: &QCoordinates,
    port: QPortId,
    budget: &mut impl Work,
) -> Result<ExactChartPoint> {
    if let Some(point) =
        q_port_assigned_point_if_representable(topology, nearest, nearest, port, budget)?
    {
        return Ok(point);
    }
    topology
        .q
        .ports
        .get(port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q publication source port is unknown".into()))?
        .point
        .try_clone_with_work(budget)
}
pub(in crate::geometry::topology::q_embedding) fn build_q_publication_chords(
    topology: &ExactGeographicTopology,
    emission: &QEmbeddingEmission<'_, '_>,
    nearest: &QCoordinates,
    domains: &[Vec<QCandidate>],
    source_spans: &[Vec<QSourceSpanRange>],
    budget: &mut impl Work,
) -> Result<BuiltQPublication> {
    let frame = topology.sources.frame;
    budget.charge_into_error(
        u64::try_from(
            emission
                .publication
                .fragments
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let source_capacity = emission
        .live_nodes
        .len()
        .checked_add(emission.selected_nodes.len())
        .ok_or(ResourceError::SizeOverflow)?;
    budget.charge_into_error(
        u64::try_from(source_capacity).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut source_inventory = Vec::with_capacity(source_capacity);
    source_inventory.extend_from_slice(&emission.live_nodes);
    source_inventory.extend_from_slice(&emission.selected_nodes);
    let mut producer_traces = Vec::with_capacity(emission.publication.fragments.len());
    for published in &emission.publication.fragments {
        budget.charge_into_error(1)?;
        let edge_count = published.points.len().checked_sub(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q publication fragment has no written endpoints".into())
        })?;
        budget.charge_into_error(
            u64::try_from(edge_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut fragment_traces = Vec::with_capacity(edge_count);
        for trace_edge in 0..edge_count {
            budget.charge_into_error(1)?;
            let trace =
                trace_source_nodes(topology, &published.trace, trace_edge, source_spans, budget)?;
            budget.charge_into_error(
                u64::try_from(trace.dependencies.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            source_inventory.extend_from_slice(&trace.dependencies);
            fragment_traces.push(trace);
        }
        producer_traces.push(fragment_traces);
    }
    try_sort_by_with_work(&mut source_inventory, budget, |left, right, _work| {
        Ok(left.cmp(right))
    })?;
    budget.charge_into_error(
        u64::try_from(source_inventory.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    source_inventory.dedup();

    let mut nodes = EmissionNodeRegistry::with_sources(topology, &source_inventory, budget)?;
    let mut sample_nodes = Vec::with_capacity(emission.publication.samples.len());
    for sample in &emission.publication.samples {
        budget.charge_into_error(1)?;
        sample_nodes.push(nodes.intern_computed(frame, &sample.exact_point, budget)?);
    }
    let mut sample_source_ports = Vec::with_capacity(sample_nodes.len());
    for (sample_index, &node) in sample_nodes.iter().enumerate() {
        budget.charge_into_error(1)?;
        let authority = &nodes
            .nodes()
            .get(node.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication sample has no authority node".into())
            })?
            .authority;
        let source_port = if let NodeAuthority::Source(source_node) = authority {
            let sample = emission
                .publication
                .samples
                .get(sample_index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication sample row is unknown".into())
                })?;
            Some(source_port_for_exact_point(
                topology,
                *source_node,
                &sample.exact_point,
                budget,
            )?)
        } else {
            None
        };
        sample_source_ports.push(source_port);
    }
    let publication_domains = published_domains(
        topology,
        emission.publication,
        &nodes,
        &sample_nodes,
        budget,
    )?;

    let mut chords = Vec::new();
    budget.charge_into_error(
        u64::try_from(topology.q.ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut nearest_source_ports = vec![None; topology.q.ports.len()];
    let period = frame.longitude().period();
    for (published, fragment_traces) in emission.publication.fragments.iter().zip(producer_traces) {
        budget.charge_into_error(1)?;
        let recipe = emission
            .fragment_recipes
            .get(published.recipe.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q published fragment recipe is unknown".into())
            })?;
        let start_port = topology
            .q
            .ports
            .get(recipe.endpoints[0].0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q published fragment start port is unknown".into())
            })?;
        let end_port = topology
            .q
            .ports
            .get(recipe.endpoints[1].0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q published fragment end port is unknown".into())
            })?;
        let mut start_endpoint_only = false;
        let mut end_endpoint_only = false;
        for range in &recipe.ranges {
            budget.charge_into_error(1)?;
            start_endpoint_only |= range.endpoint_only
                && (range.producer_start.is_zero() || range.producer_end.is_zero());
            end_endpoint_only |= range.endpoint_only
                && (range.producer_start.is_one() || range.producer_end.is_one());
        }
        let start_sample = if start_port.cut || start_endpoint_only {
            Some(source_sample_at_fragment_endpoint(published, true, budget)?)
        } else {
            None
        };
        let end_sample = if end_port.cut || end_endpoint_only {
            Some(source_sample_at_fragment_endpoint(
                published, false, budget,
            )?)
        } else {
            None
        };
        let samples = emission
            .publication
            .samples
            .get(published.sample_range.clone())
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q published sample range is invalid".into())
            })?;
        let generated = !samples.is_empty();
        budget.charge_into_error(
            u64::try_from(published.points.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut endpoints = Vec::with_capacity(published.points.len());
        endpoints.resize_with(published.points.len(), || None);
        for (sample_index, sample) in samples.iter().enumerate() {
            budget.charge_into_error(1)?;
            let written = sample.written_vertex;
            let publication_sample = published
                .sample_range
                .start
                .checked_add(sample_index)
                .ok_or(ResourceError::SizeOverflow)?;
            let node = *sample_nodes.get(publication_sample).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication sample node is unknown".into())
            })?;
            let endpoint = match &nodes
                .nodes()
                .get(node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication node identity is unknown".into())
                })?
                .authority
            {
                NodeAuthority::Source(_) => PublishedEndpoint::Source {
                    port: *sample_source_ports
                        .get(publication_sample)
                        .and_then(Option::as_ref)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "source publication sample has no exact Q port".into(),
                            )
                        })?,
                    source_record: None,
                    sample: Some(SourceSampleOccurrence {
                        trace: sample
                            .source_trace
                            .as_ref()
                            .map(|trace| trace.try_clone_with_work(budget))
                            .transpose()?,
                        endpoint_only: sample.endpoint_only,
                        publication_sample: Some(publication_sample),
                    }),
                },
                NodeAuthority::Computed(_) => PublishedEndpoint::Published {
                    node,
                    sample: publication_sample,
                },
            };
            let slot = endpoints.get_mut(written).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication sample ordinal is unknown".into())
            })?;
            if slot.replace(endpoint).is_some() {
                return Err(GeometryErrorKind::Invalid(
                    "Q publication samples share a written vertex".into(),
                )
                .into());
            }
        }
        if endpoints.first().is_some_and(Option::is_some)
            || endpoints.last().is_some_and(Option::is_some)
        {
            return Err(GeometryErrorKind::Invalid(
                "Q publication sample replaced a source endpoint".into(),
            )
            .into());
        }
        *endpoints
            .first_mut()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q publication has no start".into()))? =
            Some(PublishedEndpoint::Source {
                port: recipe.endpoints[0],
                source_record: published.source_records[0],
                sample: start_sample,
            });
        *endpoints
            .last_mut()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q publication has no end".into()))? =
            Some(PublishedEndpoint::Source {
                port: recipe.endpoints[1],
                source_record: published.source_records[1],
                sample: end_sample,
            });

        let exact_start = published.exact_endpoints[0].clone();
        let exact_end = published.exact_endpoints[1].clone();
        budget.charge_into_error(
            u64::try_from(published.points.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut exact_points = vec![None; published.points.len()];
        *exact_points.first_mut().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q publication has no exact start".into())
        })? = Some(exact_start);
        *exact_points
            .last_mut()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q publication has no exact end".into()))? =
            Some(exact_end);
        for sample in samples {
            budget.charge_into_error(1)?;
            *exact_points.get_mut(sample.written_vertex).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q exact publication sample is unknown".into())
            })? = Some(sample.exact_point.clone());
        }

        let mut nearest_points = Vec::with_capacity(published.points.len());
        let mut graph_nodes = Vec::with_capacity(published.points.len());
        let mut reference_offsets = Vec::with_capacity(endpoints.len());
        for (vertex_index, endpoint) in endpoints.iter().enumerate() {
            budget.charge_into_error(1)?;
            let endpoint = endpoint.as_ref().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication vertex is missing".into())
            })?;
            let reference = exact_points
                .get(vertex_index)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication vertex has no exact reference".into())
                })?;
            let reference_offset = publication_endpoint_reference_offset(
                topology,
                emission.publication,
                &publication_domains.exact_positions,
                endpoint,
                reference,
                budget,
            )?;
            let assigned_base = match endpoint {
                PublishedEndpoint::Source { port, .. } => {
                    let slot = nearest_source_ports
                        .get_mut(port.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q publication source port is unknown".into(),
                            )
                        })?;
                    if slot.is_none() {
                        *slot = Some(nearest_publication_source_port_point(
                            topology, nearest, *port, budget,
                        )?);
                    }
                    slot.as_ref()
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q publication source port has no nearest image".into(),
                            )
                        })?
                        .try_clone_with_work(budget)?
                },
                PublishedEndpoint::Published { node, .. } => {
                    let candidate = publication_domains
                        .nearest
                        .get(node.0 as usize)
                        .copied()
                        .flatten()
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q publication node has no nearest assignment".into(),
                            )
                        })?;
                    candidate_chart_point_with_work(frame, candidate, budget)?
                },
            };
            let point = lift_assigned_publication_point(&assigned_base, &reference_offset, budget)?;
            if let Some(sample_index) = publication_sample_index(endpoint) {
                let sample = emission
                    .publication
                    .samples
                    .get(sample_index)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q publication vertex sample is unknown".into())
                    })?;
                if !point.try_eq_with_work(&sample.published_chart_point, budget)? {
                    return Err(GeometryErrorKind::Invalid(
                        "publication occurrence nearest image differs from its shared Q assignment"
                            .into(),
                    )
                    .into());
                }
            }
            nearest_points.push(rounded_chart_point_with_work(&point, budget)?);
            match endpoint {
                PublishedEndpoint::Source { port, .. } => {
                    let q_port = topology.q.ports.get(port.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q publication source port is unknown".into())
                    })?;
                    graph_nodes.push(QGraphNode::Source(q_port.node));
                },
                PublishedEndpoint::Published { node, .. } => {
                    graph_nodes.push(QGraphNode::Published(*node));
                },
            }
            reference_offsets.push(reference_offset);
        }
        for (index, producer_trace) in fragment_traces.into_iter().enumerate() {
            budget.charge_into_error(1)?;
            let from = endpoints
                .get(index)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication edge start is missing".into())
                })?;
            let to = endpoints
                .get(index.wrapping_add(1))
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication edge end is missing".into())
                })?;
            let [Some(from_exact), Some(to_exact)] = [
                exact_points.get(index).and_then(Option::as_ref),
                exact_points
                    .get(index.wrapping_add(1))
                    .and_then(Option::as_ref),
            ] else {
                return Err(GeometryErrorKind::Invalid(
                    "Q publication edge lost its exact reference".into(),
                )
                .into());
            };
            let [from_node, to_node] = [
                *graph_nodes.get(index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication start node is unknown".into())
                })?,
                *graph_nodes.get(index.wrapping_add(1)).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication end node is unknown".into())
                })?,
            ];
            let mut dependencies = Vec::with_capacity(2);
            let mut radius = 0.0_f64;
            for endpoint in [from, to] {
                budget.charge_into_error(1)?;
                if let PublishedEndpoint::Source { port, .. } = endpoint {
                    let q_port = topology.q.ports.get(port.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q publication dependency port is unknown".into(),
                        )
                    })?;
                    dependencies.push(q_port.node);
                    radius = radius.max(port_swept_radius(
                        topology, emission, nearest, domains, *port, budget,
                    )?);
                }
            }
            dependencies.extend(producer_trace.dependencies);
            try_sort_by_with_work(&mut dependencies, budget, |left, right, _work| {
                Ok(left.cmp(right))
            })?;
            budget.charge_into_error(
                u64::try_from(dependencies.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            dependencies.dedup();
            let segment = Segment {
                start: *nearest_points.get(index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication segment start is unknown".into())
                })?,
                end: *nearest_points.get(index.wrapping_add(1)).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication segment end is unknown".into())
                })?,
            };
            let (boxes, image_radii) = chord_boxes(segment, radius, period, budget)?;
            chords.push(QPublishedChord {
                recipe: published.recipe,
                trace_edge: index,
                generated,
                nodes: [from_node, to_node],
                endpoints: [
                    from.try_clone_with_work(budget)?,
                    to.try_clone_with_work(budget)?,
                ],
                exact: [from_exact.clone(), to_exact.clone()],
                reference_offsets: [
                    reference_offsets
                        .get(index)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q publication start reference offset is unknown".into(),
                            )
                        })?
                        .try_clone_with_work(budget)?,
                    reference_offsets
                        .get(index.wrapping_add(1))
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q publication end reference offset is unknown".into(),
                            )
                        })?
                        .try_clone_with_work(budget)?,
                ],
                nearest: segment,
                boxes,
                radius,
                image_radii,
                dependencies,
                source_ranges: producer_trace.source_ranges,
            });
        }
    }
    Ok(BuiltQPublication {
        nodes,
        sample_nodes,
        sample_source_ports,
        chords,
        source_inventory,
        domains: publication_domains,
    })
}
