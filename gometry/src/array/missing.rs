//! Shared missing-row mask helpers — the single null-propagation invariant for
//! binary / linear-reference / predicate / point-navigation broadcasts.
//!
//! A missing mask is `Some(MissingMask)` with `true` at each missing row, or
//! `None` when no row is missing. Null propagation is a row-wise OR: an output
//! row is missing iff any contributing operand row is missing.

use std::sync::Arc;

/// Row-aligned missing mask for a `GeometryArray`.
///
/// Construction owns the length/count invariants so call sites cannot carry a
/// raw bool slice whose row alignment is only assumed. `None` remains the dense
/// no-missing fast path; `Some(MissingMask)` always has at least one missing row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MissingMask {
    mask: Arc<[bool]>,
    missing_count: usize,
}

impl MissingMask {
    pub(crate) fn new(len: usize, mask: Arc<[bool]>) -> Option<Self> {
        assert_eq!(
            mask.len(),
            len,
            "missing mask length must match array length"
        );
        let missing_count = mask.iter().filter(|&&missing| missing).count();
        (missing_count != 0).then_some(Self {
            mask,
            missing_count,
        })
    }

    pub(crate) fn from_vec(len: usize, mask: Vec<bool>) -> Option<Self> {
        Self::new(len, mask.into())
    }

    pub(crate) fn from_sparse(len: usize, missing_rows: &[usize]) -> Option<Self> {
        if missing_rows.is_empty() {
            return None;
        }
        let mut mask = vec![false; len];
        for &row in missing_rows {
            mask[row] = true;
        }
        Self::from_vec(len, mask)
    }

    pub(crate) fn len(&self) -> usize {
        self.mask.len()
    }

    pub(crate) const fn any(&self) -> bool {
        self.missing_count != 0
    }

    pub(crate) fn is_missing(&self, row: usize) -> bool {
        self.mask[row]
    }

    pub(crate) fn iter(&self) -> std::slice::Iter<'_, bool> {
        self.mask.iter()
    }

    pub(crate) fn as_slice(&self) -> &[bool] {
        &self.mask
    }

    pub(crate) const fn missing_count(&self) -> usize {
        self.missing_count
    }

    pub(crate) fn present_count(&self) -> usize {
        self.len() - self.missing_count
    }

    pub(crate) fn to_vec(&self) -> Vec<bool> {
        self.mask.to_vec()
    }
}

impl std::ops::Deref for MissingMask {
    type Target = [bool];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

/// Row-wise OR of two optional masks (binary null propagation).
pub(crate) fn union_pair(
    left: Option<&MissingMask>,
    right: Option<&MissingMask>,
) -> Option<MissingMask> {
    match (left, right) {
        (None, None) => None,
        (Some(mask), None) | (None, Some(mask)) => Some(mask.clone()),
        (Some(left), Some(right)) => {
            assert_eq!(
                left.len(),
                right.len(),
                "missing-mask union requires row-aligned arrays"
            );
            MissingMask::from_vec(
                left.len(),
                left.iter()
                    .zip(right.iter())
                    .map(|(&left, &right)| left || right)
                    .collect(),
            )
        },
    }
}

/// Row-wise OR of many optional masks (n-ary null propagation). Reuses the
/// first mask's allocation as the accumulator; returns `None` when every
/// operand is unmasked.
pub(crate) fn union_many<'a>(
    masks: impl IntoIterator<Item = Option<&'a MissingMask>>,
) -> Option<MissingMask> {
    let mut output: Option<Vec<bool>> = None;
    let mut len = None;
    for mask in masks {
        let Some(mask) = mask else { continue };
        match len {
            Some(len) => assert_eq!(len, mask.len(), "missing-mask union requires row alignment"),
            None => len = Some(mask.len()),
        }
        match &mut output {
            Some(output) => {
                for (out, &missing) in output.iter_mut().zip(mask.iter()) {
                    *out |= missing;
                }
            },
            None => output = Some(mask.to_vec()),
        }
    }
    output.and_then(|output| MissingMask::from_vec(len.expect("mask exists"), output))
}

/// Whether `row` is masked-missing in an optional mask.
pub(crate) fn is_missing_row(mask: Option<&MissingMask>, row: usize) -> bool {
    mask.is_some_and(|mask| mask.is_missing(row))
}
