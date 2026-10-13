use super::{
    ChartPole, ExactChartPoint, ExactDyadic, ExactGeographicTopology, ExactRational, FallibleAvl,
    FragmentRecipe, FragmentRecipeId, GeometryErrorKind, PublicationInventory, ResourceError,
    Result, SearchAccount, SourceTrace, TraceRequestLocation, Work,
    retained_exact_native_site_with_work, try_sort_by_with_work,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) enum FeatureFamilyParent {
    Fragment(FragmentRecipeId),
    Region(usize),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) enum SampleProducerKind {
    Publication,
    NativeCut,
    PolePresentation(ChartPole),
    WholeSphereCover,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) struct FeatureFamily {
    pub(in crate::geometry::topology) parent: FeatureFamilyParent,
    pub(in crate::geometry::topology) producer: SampleProducerKind,
    pub(in crate::geometry::topology) occurrence: u32,
}

impl FeatureFamily {
    pub(in crate::geometry::topology) const fn fragment_recipe(self) -> Option<FragmentRecipeId> {
        match self.parent {
            FeatureFamilyParent::Fragment(recipe) => Some(recipe),
            FeatureFamilyParent::Region(_) => None,
        }
    }
}

#[derive(Debug)]
pub(in crate::geometry::topology) struct NativeCutFeature {
    pub(in crate::geometry::topology) family: FeatureFamily,
    pub(in crate::geometry::topology) seam_period_index: i64,
    pub(in crate::geometry::topology) trace_edge: usize,
    pub(in crate::geometry::topology) edge_parameter: ExactRational,
    pub(in crate::geometry::topology) point: ExactChartPoint,
    pub(in crate::geometry::topology) source_trace: Option<TraceRequestLocation>,
}

pub(in crate::geometry::topology) fn native_cut_features(
    topology: &ExactGeographicTopology,
    recipe: &FragmentRecipe,
    trace: &SourceTrace,
    trace_edge: usize,
    start: &ExactChartPoint,
    end: &ExactChartPoint,
    assigned_displacement: &ExactRational,
    work: &mut impl Work,
) -> Result<Vec<NativeCutFeature>> {
    let axis = topology.sources.frame.longitude();
    let start_site = retained_exact_native_site_with_work(axis, &start.x, work)?;
    let producer_end_x = start.x.try_add_with_work(assigned_displacement, work)?;
    let end_site = retained_exact_native_site_with_work(axis, &producer_end_x, work)?;
    let mut features = Vec::new();
    if start_site.periods() < end_site.periods() {
        let mut period_index = start_site.periods();
        while period_index < end_site.periods() {
            work.charge_into_error(1)?;
            append_native_cut_feature(
                topology,
                recipe,
                trace,
                trace_edge,
                start,
                end,
                assigned_displacement,
                period_index,
                &mut features,
                work,
            )?;
            period_index = period_index
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
    } else {
        let mut period_index = start_site.periods();
        while period_index > end_site.periods() {
            work.charge_into_error(1)?;
            period_index = period_index
                .checked_sub(1)
                .ok_or(ResourceError::SizeOverflow)?;
            append_native_cut_feature(
                topology,
                recipe,
                trace,
                trace_edge,
                start,
                end,
                assigned_displacement,
                period_index,
                &mut features,
                work,
            )?;
        }
    }
    Ok(features)
}

pub(super) fn append_native_cut_feature(
    topology: &ExactGeographicTopology,
    recipe: &FragmentRecipe,
    trace: &SourceTrace,
    trace_edge: usize,
    start: &ExactChartPoint,
    end: &ExactChartPoint,
    assigned_displacement: &ExactRational,
    period_index: i64,
    output: &mut Vec<NativeCutFeature>,
    work: &mut impl Work,
) -> Result<()> {
    work.charge_into_error(1)?;
    let axis = topology.sources.frame.longitude();
    let period = ExactRational::from_f64(axis.period());
    let half = ExactRational::from_f64(axis.half_period());
    let seam = half.try_add_with_work(
        &period.try_product_with_work(
            &ExactRational::from_dyadic(ExactDyadic::from_i64(period_index)),
            work,
        )?,
        work,
    )?;
    let delta_x = assigned_displacement.try_clone_with_work(work)?;
    if delta_x.is_zero() {
        return Ok(());
    }
    let parameter = seam
        .try_subtract_with_work(&start.x, work)?
        .try_quotient_with_work(&delta_x, work)?;
    if parameter
        .try_cmp_with_work(&ExactRational::zero(), work)?
        .is_lt()
        || parameter
            .try_cmp_with_work(&ExactRational::one(), work)?
            .is_gt()
        || parameter.is_zero()
        || parameter.is_one()
    {
        return Ok(());
    }
    let delta_y = end.y.try_subtract_with_work(&start.y, work)?;
    let latitude = start
        .y
        .try_add_with_work(&delta_y.try_product_with_work(&parameter, work)?, work)?;
    let (has_source, _) = trace
        .rows
        .request_support_at(trace_edge, &parameter, work)?;
    let point = ExactChartPoint::from_rationals(seam, latitude);
    output.push(NativeCutFeature {
        family: FeatureFamily {
            parent: FeatureFamilyParent::Fragment(recipe.id),
            producer: SampleProducerKind::NativeCut,
            occurrence: 0,
        },
        seam_period_index: period_index,
        trace_edge,
        edge_parameter: parameter.try_clone_with_work(work)?,
        point,
        source_trace: has_source.then_some(TraceRequestLocation::Edge {
            recipe: recipe.id,
            edge: trace_edge,
            parameter,
        }),
    });
    Ok(())
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) struct FeatureFamilyBound {
    pub(super) publication: usize,
    pub(super) native_cuts: usize,
    pub(super) native_cut_ranges: Vec<NativeCutOccurrenceRange>,
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) struct NativeCutOccurrenceRange {
    pub(super) intervals: Vec<NativeCutOccurrenceInterval>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct NativeCutOccurrenceInterval {
    pub(super) low_period: i64,
    pub(super) high_period: i64,
    pub(super) family_base: u32,
}

pub(in crate::geometry::topology) struct FeatureFamilyBounds {
    pub(super) bounds: Vec<FeatureFamilyBound>,
    pub(super) region_families: Vec<FeatureFamily>,
}

pub(in crate::geometry) struct FeatureFamilyRegistry {
    pub(super) bounds: Vec<FeatureFamilyBound>,
    pub(super) region_families: Vec<FeatureFamily>,
    pub(super) families: FallibleAvl<FeatureFamily>,
    pub(super) source_base: usize,
}

impl FeatureFamilyBounds {
    pub(in crate::geometry::topology) fn from_prepared_domains(
        recipes: &[FragmentRecipe],
        publication: &PublicationInventory,
        native_cut_ranges: &[Vec<Vec<(i64, i64)>>],
        mut region_families: Vec<FeatureFamily>,
        work: &mut impl Work,
    ) -> Result<Self> {
        if publication.fragments.len() != recipes.len()
            || publication.counts.len() != recipes.len()
            || native_cut_ranges.len() != recipes.len()
        {
            return Err(GeometryErrorKind::Invalid(
                "native family bounds do not match the prepared producer table".into(),
            )
            .into());
        }
        work.charge_into_error(
            u64::try_from(recipes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut bounds = Vec::with_capacity(recipes.len());
        for (recipe_index, recipe) in recipes.iter().enumerate() {
            work.charge_into_error(1)?;
            let fragment = publication.fragments.get(recipe_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native family publication fragment is unknown".into())
            })?;
            let ranges = native_cut_ranges.get(recipe_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native family leaf ranges are unknown".into())
            })?;
            if recipe.id.0 as usize != recipe_index
                || fragment.recipe != recipe.id
                || fragment.trace.rows.edge_count() != ranges.len()
            {
                return Err(GeometryErrorKind::Invalid(
                    "native family rows are not aligned with their producer leaves".into(),
                )
                .into());
            }
            let publication_count = *publication.counts.get(recipe_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("publication family count is unknown".into())
            })?;
            if fragment.sample_range.len() != publication_count {
                return Err(GeometryErrorKind::Invalid(
                    "publication family count differs from its sample range".into(),
                )
                .into());
            }
            let mut native_cut_ranges = Vec::with_capacity(ranges.len());
            let mut native_cuts = 0_usize;
            work.charge_into_error(
                u64::try_from(ranges.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            for possible_intervals in ranges {
                work.charge_into_error(
                    u64::try_from(possible_intervals.len())
                        .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                let mut sorted = possible_intervals.clone();
                try_sort_by_with_work(&mut sorted, work, |left, right, _work| Ok(left.cmp(right)))?;
                let mut merged = Vec::<(i64, i64)>::new();
                for (low, high) in sorted {
                    work.charge_into_error(1)?;
                    if low > high {
                        return Err(GeometryErrorKind::Invalid(
                            "native family period range is reversed".into(),
                        )
                        .into());
                    }
                    if let Some(previous) = merged.last_mut() {
                        let adjacent = previous.1.checked_add(1).is_some_and(|next| low <= next);
                        if low <= previous.1 || adjacent {
                            previous.1 = previous.1.max(high);
                            continue;
                        }
                    }
                    merged.push((low, high));
                }
                work.charge_into_error(
                    u64::try_from(merged.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                let mut intervals = Vec::with_capacity(merged.len());
                for (low_period, high_period) in merged {
                    work.charge_into_error(1)?;
                    let width = usize::try_from(
                        high_period
                            .checked_sub(low_period)
                            .ok_or(ResourceError::SizeOverflow)?,
                    )
                    .map_err(|_| ResourceError::SizeOverflow)?
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                    intervals.push(NativeCutOccurrenceInterval {
                        low_period,
                        high_period,
                        family_base: u32::try_from(native_cuts)
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    });
                    native_cuts = native_cuts
                        .checked_add(width)
                        .ok_or(ResourceError::SizeOverflow)?;
                }
                native_cut_ranges.push(NativeCutOccurrenceRange { intervals });
            }
            bounds.push(FeatureFamilyBound {
                publication: publication_count,
                native_cuts,
                native_cut_ranges,
            });
        }
        try_sort_by_with_work(&mut region_families, work, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        work.charge_into_error(
            u64::try_from(region_families.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        region_families.dedup();
        Ok(Self {
            bounds,
            region_families,
        })
    }

    pub(in crate::geometry::topology) fn into_registry(
        self,
        source_base: usize,
    ) -> FeatureFamilyRegistry {
        FeatureFamilyRegistry {
            bounds: self.bounds,
            region_families: self.region_families,
            families: FallibleAvl::with_capacity(0),
            source_base,
        }
    }
}

impl FeatureFamilyRegistry {
    pub(in crate::geometry::topology) fn native_cut_occurrence(
        &self,
        parent: FragmentRecipeId,
        trace_edge: usize,
        seam_period_index: i64,
        work: &mut impl Work,
    ) -> Result<u32> {
        work.charge_into_error(1)?;
        let bound = self.bounds.get(parent.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("native cut family parent is unknown".into())
        })?;
        work.charge_into_error(1)?;
        let range = bound.native_cut_ranges.get(trace_edge).ok_or_else(|| {
            GeometryErrorKind::Invalid("native cut family leaf is unknown".into())
        })?;
        let mut low = 0_usize;
        let mut high = range.intervals.len();
        let mut found = None;
        while low < high {
            work.charge_into_error(1)?;
            let middle = low
                .checked_add(high.checked_sub(low).ok_or(ResourceError::SizeOverflow)? / 2)
                .ok_or(ResourceError::SizeOverflow)?;
            let interval = range.intervals.get(middle).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut period interval is unknown".into())
            })?;
            if seam_period_index < interval.low_period {
                high = middle;
            } else if seam_period_index > interval.high_period {
                low = middle.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            } else {
                found = Some(interval);
                break;
            }
        }
        let interval = found.ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "native cut seam lies outside its retained occurrence ranges".into(),
            )
        })?;
        let within = u32::try_from(
            seam_period_index
                .checked_sub(interval.low_period)
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?;
        interval
            .family_base
            .checked_add(within)
            .ok_or_else(|| ResourceError::SizeOverflow.into())
    }

    pub(in crate::geometry::topology) fn register(
        &mut self,
        family: FeatureFamily,
        work: &mut impl Work,
    ) -> Result<bool> {
        work.charge_into_error(1)?;
        let bound = match family.parent {
            FeatureFamilyParent::Fragment(recipe) => {
                self.bounds.get(recipe.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "feature family references an unknown producer".into(),
                    )
                })?
            },
            FeatureFamilyParent::Region(_) => {
                return self.register_region_family(family, work);
            },
        };
        let occurrence =
            usize::try_from(family.occurrence).map_err(|_| ResourceError::SizeOverflow)?;
        let limit = match family.producer {
            SampleProducerKind::Publication => bound.publication,
            SampleProducerKind::NativeCut => bound.native_cuts,
            SampleProducerKind::PolePresentation(_) | SampleProducerKind::WholeSphereCover => {
                return Err(GeometryErrorKind::Invalid(
                    "region presentation family has a fragment parent".into(),
                )
                .into());
            },
        };
        if occurrence >= limit {
            return Err(GeometryErrorKind::Invalid(
                "materialized feature exceeds its retained producer-family bound".into(),
            )
            .into());
        }
        self.families
            .insert_with_work(family, work, &mut |left, right, _work| Ok(left.cmp(right)))
    }

    pub(super) fn register_region_family(
        &mut self,
        family: FeatureFamily,
        work: &mut impl Work,
    ) -> Result<bool> {
        if !matches!(
            family.producer,
            SampleProducerKind::PolePresentation(_) | SampleProducerKind::WholeSphereCover
        ) {
            return Err(GeometryErrorKind::Invalid(
                "source feature family has a region parent".into(),
            )
            .into());
        }
        let mut low = 0_usize;
        let mut high = self.region_families.len();
        let mut found = false;
        while low < high {
            work.charge_into_error(1)?;
            let middle = low
                .checked_add(high.checked_sub(low).ok_or(ResourceError::SizeOverflow)? / 2)
                .ok_or(ResourceError::SizeOverflow)?;
            let candidate = self.region_families.get(middle).ok_or_else(|| {
                GeometryErrorKind::Invalid("native region family index is unknown".into())
            })?;
            match candidate.cmp(&family) {
                std::cmp::Ordering::Less => {
                    low = middle.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                },
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => {
                    found = true;
                    break;
                },
            }
        }
        if !found {
            return Err(GeometryErrorKind::Invalid(
                "materialized region family is outside the frozen template universe".into(),
            )
            .into());
        }
        self.families
            .insert_with_work(family, work, &mut |left, right, _work| Ok(left.cmp(right)))
    }

    pub(in crate::geometry::topology) const fn additional_constructed(&self) -> usize {
        self.families.len()
    }

    pub(in crate::geometry::topology) fn synchronize_budget(
        &self,
        budget: &mut impl SearchAccount,
    ) -> Result<()> {
        budget.charge_into_error(1)?;
        let constructed = self
            .source_base
            .checked_add(self.additional_constructed())
            .ok_or(ResourceError::SizeOverflow)?;
        budget.budget_mut().include_constructed(constructed)
    }
}
