use super::super::{
    BTreeMap, ChartMetric, ClassAssignment, EmittedNodeKey, ExactRational, GeometryErrorKind,
    GlobalCheck, NodeAuthority, Ordering, Problem, QCandidate, QClassState,
    QEmbeddingCertification, QEmbeddingOutput, QEmbeddingPass, QEmbeddingProblem, QEmissionDrawing,
    QNodeId, QVariableNode, ResourceError, Result, SearchAccount, SearchOutcome,
    compare_qnode_keys_with_work, emitted_node_key, exact_squared_displacement,
    pair_preserves_incidence_with_work, run_with_budget, translated_exact_point_with_work,
};
#[cfg(test)]
use crate::geometry::topology::q_embedding::NearestQCertificate;
use crate::work::Work;

impl<M: ChartMetric> Problem for QEmbeddingProblem<'_, '_, '_, '_, M> {
    type Node = QVariableNode;
    type Candidate = QCandidate;
    type Key = EmittedNodeKey;
    type ClassState = QClassState;
    type Output = QEmbeddingOutput;

    fn classes(&self) -> &[Vec<Self::Node>] {
        &self.classes
    }

    fn fixed_keys<W: Work>(
        &self,
        class: usize,
        _variables: &[Self::Node],
        work: &mut W,
    ) -> Result<Vec<Self::Key>> {
        let capacity = self
            .live_nodes
            .len()
            .checked_add(self.publication_nodes.nodes().len())
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge_into_error(u64::try_from(capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut keys = Vec::with_capacity(capacity);
        for &node in self.live_nodes {
            work.charge_into_error(1)?;
            if self
                .node_location
                .get(node.0 as usize)
                .copied()
                .flatten()
                .is_none_or(|(owner, _)| owner != class)
            {
                work.charge_into_error(1)?;
                keys.push(emitted_node_key(
                    self.topology.sources.frame,
                    self.nearest.xy(node)?,
                )?);
            }
        }
        for (index, node) in self.publication_nodes.nodes().iter().enumerate() {
            work.charge_into_error(1)?;
            let NodeAuthority::Computed(_) = &node.authority else {
                continue;
            };
            if self
                .publication_location
                .get(index)
                .copied()
                .flatten()
                .is_none_or(|(owner, _)| owner != class)
            {
                work.charge_into_error(1)?;
                let candidate = self
                    .publication_nearest
                    .get(index)
                    .copied()
                    .flatten()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "fixed publication node has no nearest site".into(),
                        )
                    })?;
                keys.push(emitted_node_key(
                    self.topology.sources.frame,
                    candidate.native,
                )?);
            }
        }
        Ok(keys)
    }

    fn compare_nodes<W: Work>(
        &self,
        left: Self::Node,
        right: Self::Node,
        work: &mut W,
    ) -> Result<Ordering> {
        work.charge_into_error(1)?;
        Ok(match (left, right) {
            (QVariableNode::Source(left), QVariableNode::Source(right)) => {
                let left_key = &self
                    .topology
                    .q
                    .nodes
                    .get(left.0 as usize)
                    .ok_or_else(|| GeometryErrorKind::Invalid("Q search node is unknown".into()))?
                    .key;
                let right_key = &self
                    .topology
                    .q
                    .nodes
                    .get(right.0 as usize)
                    .ok_or_else(|| GeometryErrorKind::Invalid("Q search node is unknown".into()))?
                    .key;
                compare_qnode_keys_with_work(left_key, right_key, work)?
                    .then_with(|| left.cmp(&right))
            },
            (QVariableNode::Source(_), QVariableNode::Published(_)) => Ordering::Less,
            (QVariableNode::Published(_), QVariableNode::Source(_)) => Ordering::Greater,
            (QVariableNode::Published(left), QVariableNode::Published(right)) => left.cmp(&right),
        })
    }

    fn fill_candidates<W: Work>(
        &self,
        node: Self::Node,
        candidates: &mut Vec<Self::Candidate>,
        work: &mut W,
    ) -> Result<()> {
        let domain = match node {
            QVariableNode::Source(node) => self.domains.get(node.0 as usize),
            QVariableNode::Published(node) => self.publication_domains.get(node.0 as usize),
        }
        .ok_or_else(|| GeometryErrorKind::Invalid("Q variable has no candidate domain".into()))?;
        work.charge_product_into_error(domain.len(), 1)?;
        candidates.reserve(domain.len());
        candidates.extend(domain.iter().copied());
        Ok(())
    }

    fn compare_candidates<W: Work>(
        &self,
        node: Self::Node,
        left: Self::Candidate,
        right: Self::Candidate,
        work: &mut W,
    ) -> Result<Ordering> {
        work.charge_into_error(1)?;
        let exact = match node {
            QVariableNode::Source(node) => self
                .exact_positions
                .get(node.0 as usize)
                .and_then(Option::as_ref),
            QVariableNode::Published(node) => self
                .publication_exact_positions
                .get(node.0 as usize)
                .and_then(Option::as_ref),
        }
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("Q movable node has no exact coordinate".into())
        })?;
        let frame = self.topology.sources.frame;
        let order = exact_squared_displacement(frame, exact, left, work)?.try_cmp_with_work(
            &exact_squared_displacement(frame, exact, right, work)?,
            work,
        )?;
        Ok(order
            .then_with(|| left.native.x.total_cmp(&right.native.x))
            .then_with(|| left.native.y.total_cmp(&right.native.y))
            .then_with(|| left.periods.cmp(&right.periods)))
    }

    fn candidate_key(&self, candidate: Self::Candidate) -> Self::Key {
        emitted_node_key(self.topology.sources.frame, candidate.native)
            .expect("search candidates were admitted to the geographic domain")
    }

    fn prepare_class<A: SearchAccount>(
        &self,
        class: usize,
        _variables: &[Self::Node],
        _budget: &mut A,
    ) -> Result<Self::ClassState> {
        self.class_states
            .get(class)
            .cloned()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q class state is unknown".into()).into())
    }

    fn check_local<A: SearchAccount>(
        &self,
        class_state: &Self::ClassState,
        class: usize,
        _variables: &[Self::Node],
        rank: usize,
        assigned: &[Option<Self::Candidate>],
        budget: &mut A,
    ) -> Result<bool> {
        let cut_cache = &mut BTreeMap::<QNodeId, ExactRational>::new();
        for &cut_index in class_state
            .cuts_by_rank
            .get(rank)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q cut rank is unknown".into()))?
        {
            budget.charge_evaluations(1)?;
            let Some(latitude) = self.candidate_cut_latitude(cut_index, class, assigned, budget)?
            else {
                return Ok(false);
            };
            let cut = self.emission.cut_nodes.get(cut_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q cut certificate row is unknown".into())
            })?;
            cut_cache.insert(cut.node, latitude);
        }
        let period = ExactRational::from_f64(self.topology.sources.frame.longitude().period());
        for &pair_index in class_state
            .pairs_by_rank
            .get(rank)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q proximity rank is unknown".into()))?
        {
            budget.charge_evaluations(1)?;
            let pair = *self.proximity_pairs.get(pair_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q proximity pair index is unknown".into())
            })?;
            let left = self
                .chords
                .get(pair.left)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q left pair chord is unknown".into()))?;
            let right = self.chords.get(pair.right).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q right pair chord is unknown".into())
            })?;
            let [left_start_port, left_end_port] = left.ports;
            let [right_start_port, right_end_port] = right.ports;
            let left_exact = [
                self.exact_port_point(left_start_port)?.clone(),
                self.exact_port_point(left_end_port)?.clone(),
            ];
            let right_exact = [
                translated_exact_point_with_work(
                    self.exact_port_point(right_start_port)?,
                    pair.right_shift,
                    &period,
                    budget,
                )?,
                translated_exact_point_with_work(
                    self.exact_port_point(right_end_port)?,
                    pair.right_shift,
                    &period,
                    budget,
                )?,
            ];
            let [Some(left_start), Some(left_end)] = [
                self.candidate_port_point(left_start_port, class, assigned, cut_cache, budget)?,
                self.candidate_port_point(left_end_port, class, assigned, cut_cache, budget)?,
            ] else {
                return Ok(false);
            };
            let [Some(right_start), Some(right_end)] = [
                self.candidate_port_point(right_start_port, class, assigned, cut_cache, budget)?,
                self.candidate_port_point(right_end_port, class, assigned, cut_cache, budget)?,
            ] else {
                return Ok(false);
            };
            let right_candidate = [
                translated_exact_point_with_work(&right_start, pair.right_shift, &period, budget)?,
                translated_exact_point_with_work(&right_end, pair.right_shift, &period, budget)?,
            ];
            let left_candidate = [left_start, left_end];
            if !pair_preserves_incidence_with_work(
                left.nodes,
                &left_exact,
                &left_candidate,
                right.nodes,
                &right_exact,
                &right_candidate,
                budget,
            )? {
                return Ok(false);
            }
        }
        for &pair_index in class_state
            .publication_pairs_by_rank
            .get(rank)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication proximity rank is unknown".into())
            })?
        {
            budget.charge_evaluations(1)?;
            let pair = *self.publication_pairs.get(pair_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication proximity pair is unknown".into())
            })?;
            if !self.publication_pair_matches_local(pair, class, assigned, cut_cache, budget)? {
                return Ok(false);
            }
        }
        for &fan_index in class_state
            .fans_by_rank
            .get(rank)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q fan rank is unknown".into()))?
        {
            budget.charge_evaluations(1)?;
            let fan = self
                .fan_rows
                .get(fan_index)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q fan row is unknown".into()))?;
            if !self.fan_matches(fan, class, assigned, cut_cache, budget)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn check_global<A: SearchAccount>(
        &mut self,
        assignments: &[ClassAssignment<'_, Self::Node, Self::Candidate>],
        budget: &mut A,
    ) -> Result<GlobalCheck<Self::Output>> {
        self.charge_global_work(budget)?;
        let mut coordinates = self.nearest.clone();
        let mut publication_coordinates = self.publication_nearest.clone();
        for assignment in assignments {
            for (&node, &candidate) in assignment.variables.iter().zip(assignment.values) {
                match node {
                    QVariableNode::Source(node) => {
                        let node_index = node.0 as usize;
                        *coordinates.node.get_mut(node_index).ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q global assignment node is unknown".into())
                        })? = candidate.native;
                        *coordinates
                            .longitude_periods
                            .get_mut(node_index)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q assignment has no native longitude lift".into(),
                                )
                            })? = candidate.periods;
                    },
                    QVariableNode::Published(node) => {
                        *publication_coordinates
                            .get_mut(node.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q publication assignment node is unknown".into(),
                                )
                            })? = Some(candidate);
                    },
                }
            }
        }
        let drawing = self.emission.plan.drawing(
            self.topology,
            &coordinates,
            self.emission.source_points,
            budget,
        )?;
        match self.check_global_candidate(
            &coordinates,
            &publication_coordinates,
            drawing,
            budget,
        )? {
            GlobalCheck::Pass(output) => Ok(GlobalCheck::Pass(output)),
            GlobalCheck::Reject(classes) => Ok(GlobalCheck::Reject(classes)),
        }
    }
}
pub(in crate::geometry::topology::q_embedding) fn search_q_embedding<
    M: ChartMetric,
    A: SearchAccount,
>(
    mut problem: QEmbeddingProblem<'_, '_, '_, '_, M>,
    budget: &mut A,
) -> Result<SearchOutcome<QEmbeddingPass>> {
    #[cfg(test)]
    let nearest = problem.nearest.clone();
    match run_with_budget(&mut problem, budget)? {
        SearchOutcome::Exhausted => Ok(SearchOutcome::Exhausted),
        SearchOutcome::Pass(assignment) => {
            let output = assignment.output;
            #[cfg(test)]
            {
                let mut coordinates = nearest;
                for class in assignment.classes {
                    for (node, candidate) in class.variables.into_iter().zip(class.values) {
                        if let QVariableNode::Source(node) = node {
                            let node_index = node.0 as usize;
                            *coordinates.node.get_mut(node_index).ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q search assignment node is unknown".into(),
                                )
                            })? = candidate.native;
                            *coordinates
                                .longitude_periods
                                .get_mut(node_index)
                                .ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "Q assignment has no native longitude lift".into(),
                                    )
                                })? = candidate.periods;
                        }
                    }
                }
                Ok(SearchOutcome::Pass(QEmbeddingPass {
                    output,
                    coordinates,
                    searched: true,
                }))
            }
            #[cfg(not(test))]
            {
                Ok(SearchOutcome::Pass(QEmbeddingPass { output }))
            }
        },
    }
}

pub(in crate::geometry::topology) fn certify_and_search_q_embedding<
    'topology,
    M: ChartMetric,
    A: SearchAccount,
>(
    certification: &QEmbeddingCertification<'topology, '_, '_, '_, M>,
    live_keys: &[(EmittedNodeKey, QNodeId)],
    drawing: QEmissionDrawing<'topology>,
    budget: &mut A,
) -> Result<SearchOutcome<QEmbeddingPass>> {
    certification
        .build_problem(budget)?
        .certify_and_search(live_keys, drawing, budget)
}

#[cfg(test)]
pub(in crate::geometry::topology) fn certify_q_embedding_nearest<
    'topology,
    M: ChartMetric,
    A: SearchAccount,
>(
    certification: &QEmbeddingCertification<'topology, '_, '_, '_, M>,
    live_keys: &[(EmittedNodeKey, QNodeId)],
    drawing: QEmissionDrawing<'topology>,
    budget: &mut A,
) -> Result<NearestQCertificate> {
    certification
        .build_problem(budget)?
        .certify_nearest(live_keys, drawing, budget)
}
