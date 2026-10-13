use std::cmp::Ordering;

use super::account::SearchAccount;
use super::matching::has_perfect_matching;
use super::{Problem, Work};
use crate::collections::try_hash_set_with_capacity;
use crate::error::Result;
use crate::geometry::GeometryErrorKind;
use crate::geometry::segments::ordering::try_sort_by_with_work;

struct ClassSpec<N> {
    class: usize,
    variables: Vec<N>,
}

pub(super) struct PreparedClass<N, C, K, S> {
    pub(super) class: usize,
    pub(super) variables: Vec<N>,
    pub(super) options: Vec<Vec<(C, K)>>,
    pub(super) local: S,
}

pub(super) type PreparedClassSet<P> = (
    Vec<
        PreparedClass<
            <P as Problem>::Node,
            <P as Problem>::Candidate,
            <P as Problem>::Key,
            <P as Problem>::ClassState,
        >,
    >,
    Vec<usize>,
);

pub(super) struct PreparedOptions<N, C, K> {
    pub(super) class: usize,
    pub(super) variables: Vec<N>,
    pub(super) options: Vec<Vec<(C, K)>>,
}

pub(super) fn prepare_classes<P: Problem, A: SearchAccount>(
    problem: &P,
    account: &mut A,
) -> Result<Option<PreparedClassSet<P>>> {
    let classes = problem.classes();

    account.charge_product_into_error(classes.len(), 1)?;
    let mut specs = Vec::with_capacity(classes.len());
    for (class, variables) in classes.iter().enumerate() {
        if variables.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "embedding search received an empty variable class".into(),
            )
            .into());
        }
        account.charge_product_into_error(variables.len(), 1)?;
        let mut variables = variables.clone();
        try_sort_by_with_work(&mut variables, account, |left, right, work| {
            problem.compare_nodes(*left, *right, work)
        })?;
        account.charge_into_error(1)?;
        specs.push(ClassSpec { class, variables });
    }
    try_sort_by_with_work(&mut specs, account, |left, right, work| {
        let left_class = left.class;
        let right_class = right.class;
        let left_node = *left
            .variables
            .first()
            .expect("variable classes are nonempty");
        let right_node = *right
            .variables
            .first()
            .expect("variable classes are nonempty");
        let order = problem.compare_nodes(left_node, right_node, work)?;
        Ok(if order == Ordering::Equal {
            left_class.cmp(&right_class)
        } else {
            order
        })
    })?;

    let mut candidate_scratch = Vec::new();
    let mut seen_keys = Vec::new();
    account.charge_product_into_error(specs.len(), 1)?;
    let mut admitted = Vec::with_capacity(specs.len());
    for spec in specs {
        let fixed_candidates = problem.fixed_keys(spec.class, &spec.variables, account)?;
        account.charge_product_into_error(fixed_candidates.len(), 1)?;
        let mut fixed_keys = try_hash_set_with_capacity(fixed_candidates.len())?;
        for key in fixed_candidates {
            account.charge_into_error(1)?;
            fixed_keys.insert(key);
        }
        account.charge_product_into_error(spec.variables.len(), 1)?;
        let mut options = Vec::with_capacity(spec.variables.len());
        for &node in &spec.variables {
            account.charge_product_into_error(candidate_scratch.len(), 1)?;
            candidate_scratch.clear();
            problem.fill_candidates(node, &mut candidate_scratch, account)?;
            sort_candidates(problem, node, &mut candidate_scratch, account)?;
            account.charge_product_into_error(seen_keys.len(), 1)?;
            seen_keys.clear();
            let additional_keys = candidate_scratch.len().saturating_sub(seen_keys.capacity());
            account.charge_product_into_error(additional_keys, 1)?;
            seen_keys.reserve(candidate_scratch.len());
            account.charge_product_into_error(candidate_scratch.len(), 1)?;
            let mut node_options = Vec::with_capacity(candidate_scratch.len());
            for candidate in candidate_scratch.iter().copied() {
                account.charge_into_error(1)?;
                let key = problem.candidate_key(candidate);
                account.charge_evaluations(1)?;
                if fixed_keys.contains(&key) || seen_keys.contains(&key) {
                    continue;
                }
                account.charge_into_error(2)?;
                seen_keys.push(key);
                node_options.push((candidate, key));
            }
            if node_options.is_empty() {
                return Ok(None);
            }
            account.charge_into_error(1)?;
            options.push(node_options);
        }

        let class = PreparedOptions {
            class: spec.class,
            variables: spec.variables,
            options,
        };
        if !has_perfect_matching(&class, account)? {
            return Ok(None);
        }
        account.charge_into_error(1)?;
        admitted.push(class);
    }

    account.charge_product_into_error(admitted.len(), 1)?;
    account.charge_product_into_error(classes.len(), 1)?;
    let mut prepared = Vec::with_capacity(admitted.len());
    let mut class_positions = vec![0; classes.len()];
    // Hall failures should reject the whole problem before domain-specific
    // preparation materializes potentially dense proximity partners.
    for class in admitted {
        let local = problem.prepare_class(class.class, &class.variables, account)?;
        let class = PreparedClass {
            class: class.class,
            variables: class.variables,
            options: class.options,
            local,
        };
        let position = prepared.len();
        account.charge_into_error(2)?;
        *class_positions
            .get_mut(class.class)
            .expect("class IDs are the indexes in Problem::classes") = position;
        prepared.push(class);
    }
    Ok(Some((prepared, class_positions)))
}

fn sort_candidates<P: Problem, W: Work>(
    problem: &P,
    node: P::Node,
    candidates: &mut [P::Candidate],
    work: &mut W,
) -> Result<()> {
    for index in 1..candidates.len() {
        work.charge_into_error(1)?;
        let candidate = *candidates
            .get(index)
            .expect("candidate index is within the vector");
        let mut position = index;
        while position > 0 {
            let previous = position.wrapping_sub(1);
            work.charge_into_error(1)?;
            let previous_candidate = *candidates
                .get(previous)
                .expect("previous candidate is within the sorted prefix");
            if problem.compare_candidates(node, candidate, previous_candidate, work)?
                != Ordering::Less
            {
                break;
            }
            work.charge_into_error(2)?;
            candidates.swap(position, previous);
            position = previous;
        }
    }
    Ok(())
}
