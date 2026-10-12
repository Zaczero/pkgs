//! Fallible stable ordering and adjacent deduplication for geometry work.

use crate::error::Result;
use crate::geometry::{Ordering, Unmetered, Work};

/// Stable sort by a fallible comparison; the first error ends it. Runs of
/// `RUN` items sort by insertion in place. Longer inputs merge those runs as
/// a permutation through two index buffers, then apply it in place, so no item
/// is cloned.
pub(in crate::geometry) fn try_sort_by<T>(
    items: &mut [T],
    mut compare: impl FnMut(&T, &T) -> Result<Ordering>,
) -> Result<()> {
    try_sort_by_with_work(items, &mut Unmetered, |left, right, _work| {
        compare(left, right)
    })
}

/// Stable fallible sort that shares one raw-work account with its comparator.
pub(in crate::geometry) fn try_sort_by_with_work<T, W: Work>(
    items: &mut [T],
    work: &mut W,
    mut compare: impl FnMut(&T, &T, &mut W) -> Result<Ordering>,
) -> Result<()> {
    const RUN: usize = 16;
    for run in items.chunks_mut(RUN) {
        for index in 1..run.len() {
            let mut cursor = index;
            while cursor > 0 {
                let previous = cursor.wrapping_sub(1);
                work.charge_into_error(1)?;
                #[expect(
                    clippy::indexing_slicing,
                    reason = "previous and cursor are adjacent indices within this admitted RUN16 slice"
                )]
                let ordering = compare(&run[previous], &run[cursor], work)?;
                if ordering != Ordering::Greater {
                    break;
                }
                work.charge_into_error(2)?;
                run.swap(previous, cursor);
                cursor = previous;
            }
        }
    }
    let len = items.len();
    if len <= RUN {
        return Ok(());
    }
    work.charge_product_into_error(len, 2)?;
    let mut order = Vec::with_capacity(len);
    let mut merged = Vec::with_capacity(len);
    order.extend(0..len);
    merged.resize(len, 0);
    let mut width = RUN;
    while width < len {
        // Successful allocation of both usize buffers bounds each doubling,
        // including the final width update, below usize::MAX.
        let stride = width.wrapping_mul(2);
        for (source_run, output_run) in order.chunks(stride).zip(merged.chunks_mut(stride)) {
            let left_len = width.min(source_run.len());
            let (mut left, mut right) = source_run.split_at(left_len);
            for slot in output_run {
                // Only a strictly smaller right item overtakes: ties keep
                // their input order. Suffixes also handle the final short run
                // without offset arithmetic.
                if let Some((left_id, left_rest)) = left.split_first() {
                    if let Some((right_id, right_rest)) = right.split_first() {
                        work.charge_into_error(1)?;
                        let left_item = items
                            .get(*left_id)
                            .expect("merge ordinal belongs to the admitted item slice");
                        let right_item = items
                            .get(*right_id)
                            .expect("merge ordinal belongs to the admitted item slice");
                        let ordering = compare(left_item, right_item, work)?;
                        if ordering == Ordering::Greater {
                            work.charge_into_error(1)?;
                            right = right_rest;
                            *slot = *right_id;
                        } else {
                            work.charge_into_error(1)?;
                            left = left_rest;
                            *slot = *left_id;
                        }
                    } else {
                        work.charge_into_error(1)?;
                        left = left_rest;
                        *slot = *left_id;
                    }
                } else {
                    let (right_id, right_rest) = right
                        .split_first()
                        .expect("each merge output slot has a remaining run head");
                    work.charge_into_error(1)?;
                    right = right_rest;
                    *slot = *right_id;
                }
            }
        }
        std::mem::swap(&mut order, &mut merged);
        width = width.wrapping_mul(2);
    }
    // Position `k` takes the item at `order[k]`; each cycle is walked once.
    for start in 0..len {
        let mut current = start;
        loop {
            #[expect(
                clippy::indexing_slicing,
                reason = "current is admitted by the cycle walk over the same initialized order permutation"
            )]
            let entry = &mut order[current];
            let source = *entry;
            if source == current {
                break;
            }
            work.charge_into_error(1)?;
            *entry = current;
            if source == start {
                break;
            }
            work.charge_into_error(2)?;
            items.swap(current, source);
            current = source;
        }
    }
    Ok(())
}

/// `Vec::dedup_by` with a fallible sameness test of `(later, earlier)`; the
/// first error ends it.
pub(in crate::geometry) fn try_dedup_by<T>(
    items: &mut Vec<T>,
    mut same: impl FnMut(&T, &T) -> Result<bool>,
) -> Result<()> {
    let mut failure = None;
    items.dedup_by(|later, earlier| {
        failure.is_none()
            && same(later, earlier).unwrap_or_else(|error| {
                failure = Some(error);
                false
            })
    });
    failure.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests;
