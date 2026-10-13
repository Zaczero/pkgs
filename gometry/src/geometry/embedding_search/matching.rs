use std::hash::Hash;

use super::Work;
use super::preparation::PreparedOptions;
use crate::collections::{try_hash_map_with_capacity, try_hash_set_with_capacity};
use crate::error::{ResourceError, Result};

pub(super) fn has_perfect_matching<N, C, K, W: Work>(
    class: &PreparedOptions<N, C, K>,
    work: &mut W,
) -> Result<bool>
where
    K: Copy + Eq + Hash,
{
    let variable_count = class.variables.len();
    let candidate_count = class.options.iter().try_fold(0_usize, |count, options| {
        count
            .checked_add(options.len())
            .ok_or(ResourceError::SizeOverflow)
    })?;
    work.charge_product_into_error(candidate_count, 2)?;
    work.charge_product_into_error(variable_count, 3)?;
    let mut owners = try_hash_map_with_capacity(candidate_count)?;
    let mut seen_keys = try_hash_set_with_capacity(candidate_count)?;
    let mut seen_variables = Vec::with_capacity(variable_count);
    seen_variables.resize(variable_count, usize::MAX);
    let mut parents = Vec::with_capacity(variable_count);
    parents.resize(variable_count, None::<(usize, K)>);
    let mut stack = Vec::with_capacity(variable_count);

    for root in 0..variable_count {
        work.charge_product_into_error(seen_keys.capacity(), 1)?;
        seen_keys.clear();
        work.charge_product_into_error(stack.len(), 1)?;
        stack.clear();
        work.charge_into_error(2)?;
        *seen_variables
            .get_mut(root)
            .expect("root variable is within the class") = root;
        *parents
            .get_mut(root)
            .expect("root variable is within the class") = None;
        work.charge_into_error(1)?;
        stack.push(MatchFrame {
            variable: root,
            next_candidate: 0,
        });

        let mut free_cell = None;
        loop {
            let (variable, candidate_index) = {
                let Some(frame) = stack.last_mut() else {
                    break;
                };
                let options = class
                    .options
                    .get(frame.variable)
                    .expect("matching variable is within the class");
                if frame.next_candidate == options.len() {
                    work.charge_into_error(1)?;
                    stack.pop();
                    continue;
                }
                let index = frame.next_candidate;
                work.charge_into_error(1)?;
                frame.next_candidate = frame.next_candidate.wrapping_add(1);
                (frame.variable, index)
            };
            let (_, key) = *class
                .options
                .get(variable)
                .and_then(|options| options.get(candidate_index))
                .expect("matching candidate is within its row");
            work.charge_into_error(1)?;
            if !seen_keys.insert(key) {
                continue;
            }
            work.charge_into_error(1)?;
            match owners.get(&key).copied() {
                None => {
                    free_cell = Some((variable, key));
                    break;
                },
                Some(owner) => {
                    work.charge_into_error(1)?;
                    if *seen_variables
                        .get(owner)
                        .expect("matched variable is within the class")
                        == root
                    {
                        continue;
                    }
                    work.charge_into_error(2)?;
                    *seen_variables
                        .get_mut(owner)
                        .expect("matched variable is within the class") = root;
                    *parents
                        .get_mut(owner)
                        .expect("matched variable is within the class") = Some((variable, key));
                    work.charge_into_error(1)?;
                    stack.push(MatchFrame {
                        variable: owner,
                        next_candidate: 0,
                    });
                },
            }
        }

        let Some((mut variable, mut key)) = free_cell else {
            return Ok(false);
        };
        loop {
            work.charge_into_error(1)?;
            owners.insert(key, variable);
            work.charge_into_error(1)?;
            let Some((parent, parent_key)) = parents
                .get(variable)
                .copied()
                .expect("matched variable is within the class")
            else {
                break;
            };
            variable = parent;
            key = parent_key;
        }
    }
    Ok(true)
}

struct MatchFrame {
    variable: usize,
    next_candidate: usize,
}
