use super::super::{
    ChartMetric, ChartPole, EvaluationBudget, ExactChartPoint, FeatureFamilyBounds,
    GeometryErrorKind, NativeChordCutRanges, NativeCutFamilyDomains, NativeEndpointPointCache,
    NativeEndpointPointKey, NativePoleClosure, NativeRecipeCutRanges, PublishedEndpoint,
    QCandidate, QEmbeddingProblem, QFamilyState, QPublishedChord, ResourceError, Result,
    SearchAccount, assigned_port_point, candidate_chart_point_with_work, collection_hash_state,
};
use crate::geographic::retained_exact::sites::retained_exact_native_site_with_work;
use crate::work::Work;

impl<M: ChartMetric> QEmbeddingProblem<'_, '_, '_, '_, M> {
    pub(super) fn register_native_pole_families(
        &mut self,
        pole_closures: &[NativePoleClosure],
        budget: &mut impl SearchAccount,
    ) -> Result<()> {
        if pole_closures.is_empty() {
            return Ok(());
        }
        budget.charge_into_error(
            u64::try_from(pole_closures.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut families = Vec::with_capacity(pole_closures.len());
        for &(region, pole) in pole_closures {
            budget.charge_into_error(1)?;
            families.push(super::super::super::emission::FeatureFamily {
                parent: super::super::super::emission::FeatureFamilyParent::Region(region),
                producer: super::super::super::emission::SampleProducerKind::PolePresentation(pole),
                occurrence: 0,
            });
        }
        let QFamilyState::Native(registry) = &mut self.family_state else {
            return Err(GeometryErrorKind::Invalid(
                "native pole families were registered before Native activation".into(),
            )
            .into());
        };
        for family in families {
            registry.register(family, budget)?;
        }
        registry.synchronize_budget(budget)
    }
    fn native_region_family_universe(
        &self,
        budget: &mut impl Work,
    ) -> Result<Vec<super::super::super::emission::FeatureFamily>> {
        let whole_sphere_region = if self.emission.plan.is_whole_sphere() {
            Some(
                self.emission
                    .selected_face_roots
                    .iter()
                    .flatten()
                    .copied()
                    .next()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "whole-sphere family universe has no selected root".into(),
                        )
                    })?,
            )
        } else {
            None
        };
        budget.charge_into_error(
            u64::try_from(self.emission.selected_regions.len())
                .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let possible_poles =
            self.emission
                .selected_regions
                .iter()
                .try_fold(0_usize, |count, region| {
                    let poles = usize::from(region.poles.includes(ChartPole::North))
                        .checked_add(usize::from(region.poles.includes(ChartPole::South)))
                        .ok_or(ResourceError::SizeOverflow)?;
                    count.checked_add(poles).ok_or(ResourceError::SizeOverflow)
                })?;
        let cover_families = if whole_sphere_region.is_some() { 10 } else { 0 };
        let capacity = possible_poles
            .checked_add(cover_families)
            .ok_or(ResourceError::SizeOverflow)?;
        budget
            .charge_into_error(u64::try_from(capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut families = Vec::with_capacity(capacity);
        if let Some(region) = whole_sphere_region {
            for occurrence in 0..10_u32 {
                budget.charge_into_error(1)?;
                families.push(super::super::super::emission::FeatureFamily {
                    parent: super::super::super::emission::FeatureFamilyParent::Region(region),
                    producer: super::super::super::emission::SampleProducerKind::WholeSphereCover,
                    occurrence,
                });
            }
        } else {
            for region in self.emission.selected_regions {
                for pole in [ChartPole::South, ChartPole::North] {
                    budget.charge_into_error(1)?;
                    if region.poles.includes(pole) {
                        families.push(super::super::super::emission::FeatureFamily {
                            parent: super::super::super::emission::FeatureFamilyParent::Region(
                                region.root,
                            ),
                            producer:
                                super::super::super::emission::SampleProducerKind::PolePresentation(
                                    pole,
                                ),
                            occurrence: 0,
                        });
                    }
                }
            }
        }
        Ok(families)
    }
    pub(super) fn freeze_native_families(&mut self, budget: &mut EvaluationBudget) -> Result<()> {
        let QFamilyState::Shared {
            original_source_base,
        } = &self.family_state
        else {
            return Ok(());
        };
        let source_base = *original_source_base;
        let native_cut_ranges = self.native_cut_family_domains(budget)?;
        let region_families = self.native_region_family_universe(budget)?;
        let bounds = FeatureFamilyBounds::from_prepared_domains(
            self.emission.fragment_recipes,
            self.emission.publication,
            &native_cut_ranges,
            region_families,
            budget,
        )?;
        let mut registry = bounds.into_registry(source_base);
        for sample in &self.emission.publication.samples {
            registry.register(sample.family, budget)?;
        }
        registry.synchronize_budget(budget)?;
        self.family_state = QFamilyState::Native(registry);
        Ok(())
    }
    fn native_family_endpoint_domain(
        &self,
        chord: &QPublishedChord,
        endpoint_index: usize,
        source_points: &mut NativeEndpointPointCache,
        budget: &mut impl Work,
    ) -> Result<Vec<(QCandidate, ExactChartPoint)>> {
        let endpoint = chord.endpoints.get(endpoint_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native family endpoint index is unknown".into())
        })?;
        let reference = chord.exact.get(endpoint_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native family endpoint reference is unknown".into())
        })?;
        let reference_offset = chord.reference_offsets.get(endpoint_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native family endpoint lift is unknown".into())
        })?;
        let mut output = Vec::new();
        match endpoint {
            PublishedEndpoint::Source { port, .. } => {
                let q_port = self.topology.q.ports.get(port.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native family source port is unknown".into())
                })?;
                let nearest = self.nearest.candidate(q_port.node)?;
                if q_port.cut {
                    budget.charge_into_error(1)?;
                    output.push((
                        nearest,
                        self.assigned_publication_endpoint_point(
                            endpoint,
                            reference,
                            reference_offset,
                            &q_port.point,
                            budget,
                        )?,
                    ));
                } else {
                    let candidates = self.domains.get(q_port.node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("native family source domain is unknown".into())
                    })?;
                    budget.charge_into_error(
                        u64::try_from(candidates.len()).map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    output.reserve(candidates.len());
                    for &candidate in candidates {
                        budget.charge_into_error(1)?;
                        let point = source_points.get_or_compute(
                            NativeEndpointPointKey::new(*port, candidate),
                            budget,
                            |budget| {
                                assigned_port_point(
                                    self.topology.sources.frame,
                                    &self.topology.q,
                                    *port,
                                    self.nearest.xy(q_port.node)?,
                                    candidate,
                                    self.nearest
                                        .pole_port_longitude
                                        .get(port.0 as usize)
                                        .copied()
                                        .flatten(),
                                    self.nearest
                                        .pole_port_periods
                                        .get(port.0 as usize)
                                        .copied()
                                        .flatten(),
                                    &self.cut_east,
                                    budget,
                                )
                            },
                        )?;
                        if let Some(point) = point {
                            output.push((
                                candidate,
                                self.assigned_publication_endpoint_point(
                                    endpoint,
                                    reference,
                                    reference_offset,
                                    point,
                                    budget,
                                )?,
                            ));
                        }
                    }
                    if output.is_empty() {
                        return Err(GeometryErrorKind::Invalid(
                            "native family endpoint has no admitted source-site image".into(),
                        )
                        .into());
                    }
                }
            },
            PublishedEndpoint::Published { node, .. } => {
                let node_index = node.0 as usize;
                let candidates = self.publication_domains.get(node_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native family publication domain is unknown".into())
                })?;
                budget.charge_into_error(
                    u64::try_from(candidates.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                output.reserve(candidates.len());
                for &candidate in candidates {
                    budget.charge_into_error(1)?;
                    output.push((
                        candidate,
                        self.assigned_publication_endpoint_point(
                            endpoint,
                            reference,
                            reference_offset,
                            &candidate_chart_point_with_work(
                                self.topology.sources.frame,
                                candidate,
                                budget,
                            )?,
                            budget,
                        )?,
                    ));
                }
                if output.is_empty() {
                    return Err(GeometryErrorKind::Invalid(
                        "native family publication domain is empty".into(),
                    )
                    .into());
                }
            },
        }
        Ok(output)
    }
    fn native_cut_family_domains(&self, budget: &mut impl Work) -> Result<NativeCutFamilyDomains> {
        let recipe_count = self.emission.fragment_recipes.len();
        if self.publication_chord_offsets.len()
            != recipe_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?
        {
            return Err(GeometryErrorKind::Invalid(
                "native family chord offsets do not cover every recipe".into(),
            )
            .into());
        }
        budget.charge_into_error(
            u64::try_from(recipe_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        budget.charge_into_error(1)?;
        let mut source_points = NativeEndpointPointCache::new(collection_hash_state()?.clone());
        let mut recipe_ranges: NativeCutFamilyDomains = Vec::with_capacity(recipe_count);
        for recipe_index in 0..recipe_count {
            budget.charge_into_error(1)?;
            let start = *self
                .publication_chord_offsets
                .get(recipe_index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native family recipe start is unknown".into())
                })?;
            let next_recipe = recipe_index
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            let end = *self
                .publication_chord_offsets
                .get(next_recipe)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native family recipe end is unknown".into())
                })?;
            let leaf_count = end.checked_sub(start).ok_or_else(|| {
                GeometryErrorKind::Invalid("native family chord offsets are reversed".into())
            })?;
            budget.charge_into_error(
                u64::try_from(leaf_count).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut leaf_ranges: NativeRecipeCutRanges = Vec::with_capacity(leaf_count);
            for chord_index in start..end {
                budget.charge_into_error(1)?;
                let chord = self.publication_chords.get(chord_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native family chord is unknown".into())
                })?;
                let left =
                    self.native_family_endpoint_domain(chord, 0, &mut source_points, budget)?;
                let right =
                    self.native_family_endpoint_domain(chord, 1, &mut source_points, budget)?;
                let mut ranges: NativeChordCutRanges = Vec::new();
                for (left_candidate, left_point) in &left {
                    for (right_candidate, right_point) in &right {
                        budget.charge_into_error(1)?;
                        if chord.nodes[0] == chord.nodes[1] && left_candidate != right_candidate {
                            continue;
                        }
                        let left_site = retained_exact_native_site_with_work(
                            self.topology.sources.frame.longitude(),
                            &left_point.x,
                            budget,
                        )?;
                        let right_site = retained_exact_native_site_with_work(
                            self.topology.sources.frame.longitude(),
                            &right_point.x,
                            budget,
                        )?;
                        if left_site.periods() == right_site.periods() {
                            continue;
                        }
                        let (low, high) = if left_site.periods() < right_site.periods() {
                            (
                                left_site.periods(),
                                right_site
                                    .periods()
                                    .checked_sub(1)
                                    .ok_or(ResourceError::SizeOverflow)?,
                            )
                        } else {
                            (
                                right_site.periods(),
                                left_site
                                    .periods()
                                    .checked_sub(1)
                                    .ok_or(ResourceError::SizeOverflow)?,
                            )
                        };
                        if low <= high {
                            ranges.push((low, high));
                        }
                    }
                }
                leaf_ranges.push(ranges);
            }
            recipe_ranges.push(leaf_ranges);
        }
        Ok(recipe_ranges)
    }
}
