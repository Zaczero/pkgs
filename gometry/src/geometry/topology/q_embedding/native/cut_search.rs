use super::super::{
    ClassAssignment, EmittedNodeKey, EvaluationBudget, ExactChartPoint, ExactNodePosition,
    GeographicFrame, GeometryErrorKind, GlobalCheck, Ordering, Problem, QCandidate, ResourceError,
    Result, SearchAccount, Work, emitted_node_key, exact_squared_displacement,
};

#[derive(Clone, Copy)]
pub(in crate::geometry::topology::q_embedding) enum NativeCutOrderSite {
    Variable(usize),
    Fixed(QCandidate),
}
#[derive(Clone, Copy)]
pub(in crate::geometry::topology::q_embedding) struct NativeCutOrder {
    pub(in crate::geometry::topology::q_embedding) before: NativeCutOrderSite,
    pub(in crate::geometry::topology::q_embedding) after: NativeCutOrderSite,
    pub(in crate::geometry::topology::q_embedding) ascending: bool,
}
pub(in crate::geometry::topology::q_embedding) fn native_cut_order_holds(
    ascending: bool,
    before: QCandidate,
    after: QCandidate,
) -> bool {
    if ascending {
        before.native.y < after.native.y
    } else {
        before.native.y > after.native.y
    }
}
pub(in crate::geometry::topology::q_embedding) struct NativeCutClassState {
    pub(in crate::geometry::topology::q_embedding) rank_by_variable: Vec<Option<usize>>,
    pub(in crate::geometry::topology::q_embedding) orders_by_rank: Vec<Vec<NativeCutOrder>>,
}
pub(in crate::geometry::topology::q_embedding) struct NativeCutProblem<F> {
    pub(in crate::geometry::topology::q_embedding) frame: GeographicFrame,
    pub(in crate::geometry::topology::q_embedding) points: Vec<ExactChartPoint>,
    pub(in crate::geometry::topology::q_embedding) domains: Vec<Vec<QCandidate>>,
    pub(in crate::geometry::topology::q_embedding) classes: Vec<Vec<usize>>,
    pub(in crate::geometry::topology::q_embedding) fixed_keys: Vec<EmittedNodeKey>,
    pub(in crate::geometry::topology::q_embedding) orders: Vec<NativeCutOrder>,
    pub(in crate::geometry::topology::q_embedding) complete: F,
}
#[expect(
    clippy::large_enum_variant,
    reason = "boxing a short-lived successful result would add a heap allocation to Native publication"
)]
pub(in crate::geometry::topology::q_embedding) enum NativeCutSearchOutcome {
    Pass(super::super::super::emission::CompletedNativeDrawing),
    Reject(Vec<usize>),
}
impl<F> NativeCutProblem<F> {
    pub(in crate::geometry::topology::q_embedding) fn candidate_for_site(
        &self,
        site: NativeCutOrderSite,
        class_state: &NativeCutClassState,
        assigned: &[Option<QCandidate>],
    ) -> Result<QCandidate> {
        match site {
            NativeCutOrderSite::Fixed(candidate) => Ok(candidate),
            NativeCutOrderSite::Variable(variable) => {
                let rank = class_state
                    .rank_by_variable
                    .get(variable)
                    .copied()
                    .flatten()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native cut order references an unassigned variable".into(),
                        )
                    })?;
                assigned.get(rank).copied().flatten().ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "native cut order reached an unassigned candidate".into(),
                    )
                    .into()
                })
            },
        }
    }
}
impl<F> Problem for NativeCutProblem<F>
where
    F: FnMut(
        &[QCandidate],
        &mut EvaluationBudget,
    ) -> Result<GlobalCheck<super::super::super::emission::CompletedNativeDrawing>>,
{
    type Node = usize;
    type Candidate = QCandidate;
    type Key = EmittedNodeKey;
    type ClassState = NativeCutClassState;
    type Output = super::super::super::emission::CompletedNativeDrawing;

    fn classes(&self) -> &[Vec<Self::Node>] {
        &self.classes
    }

    fn fixed_keys<W: Work>(
        &self,
        _class: usize,
        _variables: &[Self::Node],
        work: &mut W,
    ) -> Result<Vec<Self::Key>> {
        work.charge_product_into_error(self.fixed_keys.len(), 1)?;
        Ok(self.fixed_keys.clone())
    }

    fn compare_nodes<W: Work>(
        &self,
        left: Self::Node,
        right: Self::Node,
        work: &mut W,
    ) -> Result<Ordering> {
        work.charge_into_error(1)?;
        let left_point = self.points.get(left).ok_or_else(|| {
            GeometryErrorKind::Invalid("native cut search node is unknown".into())
        })?;
        let right_point = self.points.get(right).ok_or_else(|| {
            GeometryErrorKind::Invalid("native cut search node is unknown".into())
        })?;
        let x_order = left_point.x.try_cmp_with_work(&right_point.x, work)?;
        let order = if x_order.is_eq() {
            left_point.y.try_cmp_with_work(&right_point.y, work)?
        } else {
            x_order
        };
        Ok(order.then_with(|| left.cmp(&right)))
    }

    fn fill_candidates<W: Work>(
        &self,
        node: Self::Node,
        candidates: &mut Vec<Self::Candidate>,
        work: &mut W,
    ) -> Result<()> {
        let domain = self.domains.get(node).ok_or_else(|| {
            GeometryErrorKind::Invalid("native cut search node has no candidate domain".into())
        })?;
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
        let point = self.points.get(node).ok_or_else(|| {
            GeometryErrorKind::Invalid("native cut search node is unknown".into())
        })?;
        let exact = ExactNodePosition {
            longitude: point.x.clone(),
            latitude: point.y.clone(),
        };
        Ok(exact_squared_displacement(self.frame, &exact, left, work)?
            .try_cmp_with_work(
                &exact_squared_displacement(self.frame, &exact, right, work)?,
                work,
            )?
            .then_with(|| left.native.x.total_cmp(&right.native.x))
            .then_with(|| left.native.y.total_cmp(&right.native.y))
            .then_with(|| left.periods.cmp(&right.periods)))
    }

    fn candidate_key(&self, candidate: Self::Candidate) -> Self::Key {
        emitted_node_key(self.frame, candidate.native)
            .expect("native cut candidates were admitted to the geographic domain")
    }

    fn prepare_class<A: SearchAccount>(
        &self,
        class: usize,
        variables: &[Self::Node],
        budget: &mut A,
    ) -> Result<Self::ClassState> {
        if class >= self.classes.len() {
            return Err(
                GeometryErrorKind::Invalid("native cut search class is unknown".into()).into(),
            );
        }
        let work = variables
            .len()
            .checked_add(self.orders.len())
            .ok_or(ResourceError::SizeOverflow)?;
        budget.charge_into_error(u64::try_from(work).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut rank_by_variable = vec![None; self.points.len()];
        for (rank, &variable) in variables.iter().enumerate() {
            let slot = rank_by_variable.get_mut(variable).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut class has an unknown variable".into())
            })?;
            if slot.replace(rank).is_some() {
                return Err(GeometryErrorKind::Invalid(
                    "native cut variable appears twice in its class".into(),
                )
                .into());
            }
        }
        let mut orders_by_rank = vec![Vec::new(); variables.len()];
        for &order in &self.orders {
            let before_rank = match order.before {
                NativeCutOrderSite::Variable(variable) => {
                    rank_by_variable.get(variable).copied().flatten()
                },
                NativeCutOrderSite::Fixed(_) => None,
            };
            let after_rank = match order.after {
                NativeCutOrderSite::Variable(variable) => {
                    rank_by_variable.get(variable).copied().flatten()
                },
                NativeCutOrderSite::Fixed(_) => None,
            };
            if let (Some(before), Some(after)) = (before_rank, after_rank)
                && before == after
            {
                continue;
            }
            let Some(rank) = before_rank.into_iter().chain(after_rank).max() else {
                continue;
            };
            orders_by_rank
                .get_mut(rank)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native cut order rank is outside its class".into())
                })?
                .push(order);
        }
        Ok(NativeCutClassState {
            rank_by_variable,
            orders_by_rank,
        })
    }

    fn check_local<A: SearchAccount>(
        &self,
        class_state: &Self::ClassState,
        _class: usize,
        _variables: &[Self::Node],
        rank: usize,
        assigned: &[Option<Self::Candidate>],
        budget: &mut A,
    ) -> Result<bool> {
        let orders = class_state.orders_by_rank.get(rank).ok_or_else(|| {
            GeometryErrorKind::Invalid("native cut local-order rank is unknown".into())
        })?;
        for &order in orders {
            budget.charge_evaluations(1)?;
            let before = self.candidate_for_site(order.before, class_state, assigned)?;
            let after = self.candidate_for_site(order.after, class_state, assigned)?;
            if !native_cut_order_holds(order.ascending, before, after) {
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
        let mut candidates = vec![None; self.points.len()];
        for assignment in assignments {
            for (&node, &candidate) in assignment.variables.iter().zip(assignment.values) {
                *candidates.get_mut(node).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native cut global node is unknown".into())
                })? = Some(candidate);
            }
        }
        budget.charge_evaluations(
            u64::try_from(self.orders.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let class_state = NativeCutClassState {
            rank_by_variable: (0..self.points.len()).map(Some).collect(),
            orders_by_rank: Vec::new(),
        };
        let assigned = candidates.clone();
        for &order in &self.orders {
            let before = self.candidate_for_site(order.before, &class_state, &assigned)?;
            let after = self.candidate_for_site(order.after, &class_state, &assigned)?;
            if !native_cut_order_holds(order.ascending, before, after) {
                return Ok(GlobalCheck::Reject(vec![0]));
            }
        }
        let candidates = candidates
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut global assignment is incomplete".into())
            })?;
        (self.complete)(&candidates, budget.budget_mut())
    }

    fn initial_evaluations(&self) -> u64 {
        0
    }
}
