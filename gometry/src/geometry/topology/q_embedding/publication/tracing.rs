use super::super::*;

pub(in crate::geometry::topology::q_embedding) fn add_source_reused_recipe_walk(
    walk: &RecipeWalk,
    expected_edges: usize,
    recipes: &[FragmentRecipe],
    publication: &PublicationInventory,
    reused: &mut BTreeSet<FragmentRecipeId>,
    budget: &mut impl Work,
) -> Result<bool> {
    if walk.fragments.len() != expected_edges {
        return Ok(false);
    }
    for &recipe_id in &walk.fragments {
        budget.charge_into_error(1)?;
        let _recipe = recipes.get(recipe_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("source-reuse walk references an unknown recipe".into())
        })?;
        let fragment = publication
            .fragments
            .get(recipe_id.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("source-reuse walk has no published fragment".into())
            })?;
        if fragment.recipe != recipe_id {
            return Err(GeometryErrorKind::Invalid(
                "source-reuse fragment changed its recipe identity".into(),
            )
            .into());
        }
        for sample_index in fragment.sample_range.clone() {
            budget.charge_into_error(1)?;
            let sample = publication.samples.get(sample_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("source-reuse sample row is unknown".into())
            })?;
            if sample.family.fragment_recipe() != Some(recipe_id) || sample.source_trace.is_none() {
                return Ok(false);
            }
        }
    }
    budget.charge_into_error(
        u64::try_from(walk.fragments.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    reused.extend(walk.fragments.iter().copied());
    Ok(true)
}
pub(in crate::geometry::topology::q_embedding) fn source_span_range_index(
    topology: &ExactGeographicTopology,
    budget: &mut impl Work,
) -> Result<Vec<Vec<QSourceSpanRange>>> {
    budget.charge_into_error(
        u64::try_from(topology.sources.edges.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut by_source = vec![Vec::new(); topology.sources.edges.len()];
    for (span_index, span) in topology.q.spans.iter().enumerate() {
        budget.charge_into_error(1)?;
        for (owner_index, owner) in span.owners.iter().enumerate() {
            budget.charge_into_error(1)?;
            let row = by_source
                .get_mut(owner.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q span has an unknown source-edge owner".into())
                })?;
            let (start, end) = if owner
                .source_range
                .start
                .try_cmp_with_work(&owner.source_range.end, budget)?
                .is_le()
            {
                (
                    owner.source_range.start.try_clone_with_work(budget)?,
                    owner.source_range.end.try_clone_with_work(budget)?,
                )
            } else {
                (
                    owner.source_range.end.try_clone_with_work(budget)?,
                    owner.source_range.start.try_clone_with_work(budget)?,
                )
            };
            row.push(QSourceSpanRange {
                start,
                end,
                span: span_index,
                owner: owner_index,
                source_lift: owner.lift,
                source_range: ExactSourceRange {
                    start: owner.source_range.start.try_clone_with_work(budget)?,
                    end: owner.source_range.end.try_clone_with_work(budget)?,
                },
                reversed: owner.reversed,
                nodes: span.nodes,
                ports: span.ports,
            });
        }
    }
    for row in &mut by_source {
        budget.charge_into_error(1)?;
        try_sort_by_with_work(row, budget, |left, right, budget| {
            budget.charge_into_error(1)?;
            Ok(left
                .start
                .try_cmp_with_work(&right.start, budget)?
                .then(left.end.try_cmp_with_work(&right.end, budget)?)
                .then(left.span.cmp(&right.span))
                .then(left.owner.cmp(&right.owner)))
        })?;
        for adjacent in row.windows(2) {
            budget.charge_into_error(1)?;
            let [left, right] = adjacent else {
                unreachable!("windows(2) contains two Q source ranges");
            };
            if right.end.try_cmp_with_work(&left.end, budget)?.is_lt() {
                return Err(GeometryErrorKind::Invalid(
                    "Q source-edge span ranges are not ordered".into(),
                )
                .into());
            }
        }
    }
    Ok(by_source)
}
pub(in crate::geometry::topology::q_embedding) fn producer_parameter_at_source(
    range: &RootRange,
    source_parameter: &ExactRational,
    budget: &mut impl Work,
) -> Result<ExactRational> {
    let source_width = range
        .source_end()
        .try_subtract_with_work(range.source_start(), budget)?;
    if source_width.is_zero() {
        if source_parameter
            .try_cmp_with_work(range.source_start(), budget)?
            .is_eq()
        {
            return range.producer_start().try_clone_with_work(budget);
        }
        return Err(GeometryErrorKind::Invalid(
            "Q producer source parameter is outside its point range".into(),
        )
        .into());
    }
    let local = source_parameter
        .try_subtract_with_work(range.source_start(), budget)?
        .try_quotient_with_work(&source_width, budget)?;
    range.producer_start().try_add_with_work(
        &range
            .producer_end()
            .try_subtract_with_work(range.producer_start(), budget)?
            .try_product_with_work(&local, budget)?,
        budget,
    )
}
pub(in crate::geometry::topology::q_embedding) fn span_parameter_at_source(
    range: &QSourceSpanRange,
    source_parameter: &ExactRational,
    budget: &mut impl Work,
) -> Result<ExactRational> {
    let (start, end) = if range.reversed {
        (&range.source_range.end, &range.source_range.start)
    } else {
        (&range.source_range.start, &range.source_range.end)
    };
    let source_width = end.try_subtract_with_work(start, budget)?;
    if source_width.is_zero() {
        if source_parameter.try_cmp_with_work(start, budget)?.is_eq() {
            return Ok(ExactRational::zero());
        }
        return Err(GeometryErrorKind::Invalid(
            "Q span source parameter is outside its point range".into(),
        )
        .into());
    }
    source_parameter
        .try_subtract_with_work(start, budget)?
        .try_quotient_with_work(&source_width, budget)
}
pub(in crate::geometry::topology::q_embedding) fn trace_source_nodes(
    topology: &ExactGeographicTopology,
    trace: &super::super::super::emission::SourceTrace,
    trace_edge: usize,
    source_spans: &[Vec<QSourceSpanRange>],
    budget: &mut impl Work,
) -> Result<QProducerTrace> {
    let mut nodes = Vec::new();
    let mut source_ranges = Vec::new();
    for root_range in trace.owners_for_edge(trace_edge)? {
        budget.charge_into_error(1)?;
        let source_index = root_range.root().index();
        let source = topology.sources.edges.get(source_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("publication trace references an unknown source edge".into())
        })?;
        if source.id.0 as usize != source_index {
            return Err(GeometryErrorKind::Invalid(
                "publication trace root does not match its source-edge owner".into(),
            )
            .into());
        }
        let row = source_spans.get(source_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("publication trace root has no Q span index".into())
        })?;
        let (requested_low, requested_high) = if root_range
            .source_start()
            .try_cmp_with_work(root_range.source_end(), budget)?
            .is_le()
        {
            (root_range.source_start(), root_range.source_end())
        } else {
            (root_range.source_end(), root_range.source_start())
        };

        let mut low = 0_usize;
        let mut high = row.len();
        while low < high {
            budget.charge_into_error(1)?;
            let middle = low
                .checked_add(high.checked_sub(low).ok_or(ResourceError::SizeOverflow)? / 2)
                .ok_or(ResourceError::SizeOverflow)?;
            let range = row.get(middle).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source-span search ordinal is unknown".into())
            })?;
            if range.end.try_cmp_with_work(requested_low, budget)?.is_lt() {
                low = middle.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            } else {
                high = middle;
            }
        }

        let is_point = requested_low
            .try_cmp_with_work(requested_high, budget)?
            .is_eq();
        let mut covered = Vec::<(ExactRational, ExactRational)>::new();
        let source_start_is_low = root_range
            .source_start()
            .try_cmp_with_work(root_range.source_end(), budget)?
            .is_le();
        let mut cursor = low;
        while let Some(range) = row.get(cursor) {
            budget.charge_into_error(1)?;
            if range
                .start
                .try_cmp_with_work(requested_high, budget)?
                .is_gt()
            {
                break;
            }
            let overlaps = if is_point {
                range
                    .start
                    .try_cmp_with_work(requested_low, budget)?
                    .is_le()
                    && range.end.try_cmp_with_work(requested_low, budget)?.is_ge()
            } else {
                range
                    .start
                    .try_cmp_with_work(requested_high, budget)?
                    .is_lt()
                    && range.end.try_cmp_with_work(requested_low, budget)?.is_gt()
            };
            if overlaps {
                let overlap_low = if range
                    .start
                    .try_cmp_with_work(requested_low, budget)?
                    .is_gt()
                {
                    range.start.try_clone_with_work(budget)?
                } else {
                    requested_low.try_clone_with_work(budget)?
                };
                let overlap_high = if range.end.try_cmp_with_work(requested_high, budget)?.is_lt() {
                    range.end.try_clone_with_work(budget)?
                } else {
                    requested_high.try_clone_with_work(budget)?
                };
                covered.push((
                    overlap_low.try_clone_with_work(budget)?,
                    overlap_high.try_clone_with_work(budget)?,
                ));
                let (source_start, source_end) = if source_start_is_low {
                    (overlap_low, overlap_high)
                } else {
                    (overlap_high, overlap_low)
                };
                budget.charge_into_error(1)?;
                let producer_range = ExactSourceRange {
                    start: producer_parameter_at_source(root_range, &source_start, budget)?,
                    end: producer_parameter_at_source(root_range, &source_end, budget)?,
                };
                let span_range = ExactSourceRange {
                    start: span_parameter_at_source(range, &source_start, budget)?,
                    end: span_parameter_at_source(range, &source_end, budget)?,
                };
                let source_range = ExactSourceRange {
                    start: source_start,
                    end: source_end,
                };
                source_ranges.push(QProducerSourceRange {
                    root: root_range.root(),
                    source_edge: source.id,
                    source_lift: range.source_lift,
                    source_range,
                    producer_range,
                    span: range.span,
                    owner: range.owner,
                    span_range,
                    nodes: range.nodes,
                    ports: range.ports,
                });
                let span = topology.q.spans.get(range.span).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source-span index is stale".into())
                })?;
                budget.charge_into_error(2)?;
                nodes.extend(span.nodes);
            }
            cursor = cursor.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
        if covered.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "publication root interval has no corresponding Q source spans".into(),
            )
            .into());
        }
        try_sort_by_with_work(&mut covered, budget, |left, right, budget| {
            budget.charge_into_error(1)?;
            Ok(left
                .0
                .try_cmp_with_work(&right.0, budget)?
                .then(left.1.try_cmp_with_work(&right.1, budget)?))
        })?;
        let mut covered_to = requested_low.try_clone_with_work(budget)?;
        for (start, end) in covered {
            budget.charge_into_error(1)?;
            if start.try_cmp_with_work(&covered_to, budget)?.is_gt() {
                return Err(GeometryErrorKind::Invalid(
                    "publication root interval has a gap in its Q source spans".into(),
                )
                .into());
            }
            if end.try_cmp_with_work(&covered_to, budget)?.is_gt() {
                covered_to = end;
            }
            if covered_to
                .try_cmp_with_work(requested_high, budget)?
                .is_ge()
            {
                break;
            }
        }
        if covered_to
            .try_cmp_with_work(requested_high, budget)?
            .is_lt()
        {
            return Err(GeometryErrorKind::Invalid(
                "publication root interval is not fully covered by Q source spans".into(),
            )
            .into());
        }
    }
    try_sort_by_with_work(&mut nodes, budget, |left, right, _work| Ok(left.cmp(right)))?;
    budget
        .charge_into_error(u64::try_from(nodes.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    nodes.dedup();
    try_sort_by_with_work(&mut source_ranges, budget, |left, right, budget| {
        budget.charge_into_error(1)?;
        Ok(left
            .root
            .cmp(&right.root)
            .then(
                left.source_range
                    .start
                    .try_cmp_with_work(&right.source_range.start, budget)?,
            )
            .then(
                left.source_range
                    .end
                    .try_cmp_with_work(&right.source_range.end, budget)?,
            )
            .then(left.span.cmp(&right.span)))
    })?;
    Ok(QProducerTrace {
        source_ranges,
        dependencies: nodes,
    })
}
