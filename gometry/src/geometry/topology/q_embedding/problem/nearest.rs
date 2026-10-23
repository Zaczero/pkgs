use super::super::{
    ChartMetric, EmittedNodeKey, GeometryErrorKind, GlobalCheck, NearestQCertificate,
    NodeAuthority, PlannedAreaRing, Problem, QCandidate, QEmbeddingPass, QEmbeddingProblem,
    QEmissionDrawing, QNodeId, QVariableNode, ResourceError, Result, SearchAccount, SearchOutcome,
    planned_area_rings, try_sort_by_with_work,
};
use super::search::search_q_embedding;

impl<'topology, M: ChartMetric> QEmbeddingProblem<'topology, '_, '_, '_, M> {
    pub(super) fn certify_nearest(
        &mut self,
        live_keys: &[(EmittedNodeKey, QNodeId)],
        drawing: QEmissionDrawing<'topology>,
        budget: &mut impl SearchAccount,
    ) -> Result<NearestQCertificate> {
        budget.charge_into_error(
            u64::try_from(self.classes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut failed_classes = vec![false; self.classes.len()];
        let mut unrepresentable = false;
        let (publication_conflicts, publication_unrepresentable) = self
            .publication_identity_conflicts(
                &self.nearest,
                live_keys,
                &self.publication_nearest,
                budget,
            )?;
        unrepresentable |= publication_unrepresentable;
        for node in publication_conflicts {
            budget.charge_into_error(1)?;
            let location = match node {
                QVariableNode::Source(node) => {
                    self.node_location.get(node.0 as usize).copied().flatten()
                },
                QVariableNode::Published(node) => self
                    .publication_location
                    .get(node.0 as usize)
                    .copied()
                    .flatten(),
            };
            if let Some((class, _)) = location {
                *failed_classes.get_mut(class).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication conflict class is unknown".into())
                })? = true;
            } else {
                unrepresentable = true;
            }
        }
        let mut group_start = 0_usize;
        while group_start < live_keys.len() {
            budget.charge_into_error(1)?;
            let key = live_keys
                .get(group_start)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q live-key group is unknown".into()))?
                .0;
            let mut group_end = group_start
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            while let Some((candidate, _)) = live_keys.get(group_end) {
                budget.charge_into_error(1)?;
                if *candidate != key {
                    break;
                }
                group_end = group_end
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
            }
            let mut group_nodes = live_keys
                .get(group_start..group_end)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q live-key group is invalid".into()))?
                .iter()
                .map(|(_, node)| *node)
                .collect::<Vec<_>>();
            budget.charge_into_error(
                u64::try_from(group_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            try_sort_by_with_work(&mut group_nodes, budget, |left, right, _work| {
                Ok(left.cmp(right))
            })?;
            group_nodes.dedup();
            if group_nodes.len() > 1 {
                let mut movable = false;
                for node in group_nodes {
                    budget.charge_into_error(1)?;
                    if let Some((class, _)) =
                        self.node_location.get(node.0 as usize).copied().flatten()
                    {
                        *failed_classes.get_mut(class).ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q identity class is unknown".into())
                        })? = true;
                        movable = true;
                    }
                }
                if !movable {
                    unrepresentable = true;
                }
            }
            group_start = group_end;
        }
        for &node in &self.emission.live_nodes {
            budget.charge_into_error(1)?;
            let nearest_candidate = self.nearest.candidate(node)?;
            let domain = self.domains.get(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q live node has no candidate domain".into())
            })?;
            let mut contains_nearest = false;
            for &candidate in domain {
                budget.charge_into_error(1)?;
                if candidate == nearest_candidate {
                    contains_nearest = true;
                    break;
                }
            }
            if contains_nearest {
                continue;
            }
            if let Some((class, _)) = self.node_location.get(node.0 as usize).copied().flatten() {
                *failed_classes.get_mut(class).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q domain class is unknown".into())
                })? = true;
            } else {
                unrepresentable = true;
            }
        }
        for (node_index, node) in self.publication_nodes.nodes().iter().enumerate() {
            budget.charge_into_error(1)?;
            if !matches!(&node.authority, NodeAuthority::Computed(_)) {
                continue;
            }
            let nearest_candidate = self
                .publication_nearest
                .get(node_index)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication node has no nearest candidate".into())
                })?;
            let domain = self.publication_domains.get(node_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication node has no candidate domain".into())
            })?;
            let mut contains_nearest = false;
            for &candidate in domain {
                budget.charge_into_error(1)?;
                if candidate == nearest_candidate {
                    contains_nearest = true;
                    break;
                }
            }
            if contains_nearest {
                continue;
            }
            if let Some((class, _)) = self.publication_location.get(node_index).copied().flatten() {
                *failed_classes.get_mut(class).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication domain class is unknown".into())
                })? = true;
            } else {
                unrepresentable = true;
            }
        }
        if unrepresentable {
            return Ok(NearestQCertificate::Unrepresentable);
        }

        for (class_index, variables) in self.classes.iter().enumerate() {
            budget.charge_into_error(
                u64::try_from(variables.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let values = variables
                .iter()
                .map(|&node| self.nearest_candidate(node).map(Some))
                .collect::<Result<Vec<_>>>()?;
            if !failed_classes
                .get(class_index)
                .copied()
                .ok_or_else(|| GeometryErrorKind::Invalid("Q nearest class is unknown".into()))?
            {
                let state = self.class_states.get(class_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q nearest class state is unknown".into())
                })?;
                budget.charge_into_error(
                    u64::try_from(values.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                for rank in 0..variables.len() {
                    budget.charge_evaluations(1)?;
                    let prefix = values.get(..=rank).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q nearest prefix is out of range".into())
                    })?;
                    if !<Self as Problem>::check_local(
                        self,
                        state,
                        class_index,
                        variables,
                        rank,
                        prefix,
                        budget,
                    )? {
                        *failed_classes.get_mut(class_index).ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q nearest class is unknown".into())
                        })? = true;
                        break;
                    }
                }
            }
        }

        let mut repair_needed = false;
        for failed in failed_classes {
            budget.charge_into_error(1)?;
            if failed {
                repair_needed = true;
            }
        }
        if repair_needed {
            return Ok(NearestQCertificate::SearchRequired);
        }

        self.charge_global_work(budget)?;
        let clone_entries = [
            self.nearest.node.len(),
            self.nearest.longitude_periods.len(),
            self.nearest.pole_port_longitude.len(),
            self.nearest.pole_port_periods.len(),
            self.publication_nearest.len(),
        ]
        .into_iter()
        .try_fold(0_usize, |count, entries| {
            count
                .checked_add(entries)
                .ok_or(ResourceError::SizeOverflow)
        })?;
        budget.charge_into_error(
            u64::try_from(clone_entries).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let nearest = self.nearest.clone();
        let publication_nearest = self.publication_nearest.clone();
        match self.check_global_candidate(&nearest, &publication_nearest, drawing, budget)? {
            GlobalCheck::Pass(output) => Ok(NearestQCertificate::Pass(Box::new(output))),
            GlobalCheck::Reject(classes) => {
                if classes.is_empty() {
                    Ok(NearestQCertificate::Unrepresentable)
                } else {
                    Ok(NearestQCertificate::SearchRequired)
                }
            },
        }
    }
    pub(super) fn certify_and_search<A: SearchAccount>(
        mut self,
        live_keys: &[(EmittedNodeKey, QNodeId)],
        drawing: QEmissionDrawing<'topology>,
        budget: &mut A,
    ) -> Result<SearchOutcome<QEmbeddingPass>> {
        match self.certify_nearest(live_keys, drawing, budget)? {
            NearestQCertificate::Pass(output) => {
                let output = *output;
                #[cfg(test)]
                {
                    Ok(SearchOutcome::Pass(QEmbeddingPass {
                        output,
                        coordinates: self.nearest.clone(),
                        searched: false,
                    }))
                }
                #[cfg(not(test))]
                {
                    Ok(SearchOutcome::Pass(QEmbeddingPass { output }))
                }
            },
            NearestQCertificate::SearchRequired => search_q_embedding(self, budget),
            NearestQCertificate::Unrepresentable => Ok(SearchOutcome::Exhausted),
        }
    }
    fn nearest_candidate(&self, node: QVariableNode) -> Result<QCandidate> {
        match node {
            QVariableNode::Source(node) => self.nearest.candidate(node),
            QVariableNode::Published(node) => self
                .publication_nearest
                .get(node.0 as usize)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q publication variable has no nearest candidate".into(),
                    )
                    .into()
                }),
        }
    }
    pub(super) fn charge_global_work(&self, budget: &mut impl SearchAccount) -> Result<()> {
        if self.exact_faces.is_some() {
            let item_count = self
                .emission
                .spans
                .len()
                .checked_add(self.emission.point_nodes.len())
                .ok_or(ResourceError::SizeOverflow)?;
            budget.charge_evaluations(
                u64::try_from(item_count.div_ceil(16)).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
        }
        let mut ring_edge_count = 0_usize;
        for ring in planned_area_rings(self.emission.plan) {
            let edge_count = match ring {
                PlannedAreaRing::Cycle { cycle, .. } => self
                    .emission
                    .plan
                    .cycles
                    .get(*cycle)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q area ring references an unknown cycle".into())
                    })?
                    .darts
                    .len(),
                PlannedAreaRing::Source { ring, .. } => self
                    .topology
                    .sources
                    .rings
                    .get(*ring)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q source ring role is unknown".into())
                    })?
                    .source_edges
                    .len(),
            };
            ring_edge_count = ring_edge_count
                .checked_add(edge_count)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        let total = ring_edge_count
            .checked_add(self.publication_pairs.len())
            .ok_or(ResourceError::SizeOverflow)?;
        budget.charge_evaluations(u64::try_from(total).map_err(|_| ResourceError::SizeOverflow)?)
    }
}
