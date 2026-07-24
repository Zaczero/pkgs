#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Normalized S2 cell-set algebra: sorted, non-overlapping, sibling-merged
//! id vectors with hierarchy-aware union/intersection/difference and the
//! range searches that answer mixed-level membership.
//!
//! S2's complete quadtree makes every cell one contiguous leaf-id range, so
//! all of this is sorted-vector work — no trees, no hashing. The kernels
//! live in [`crate::grid::cell_set`] via [`HierarchicalId`]; this module
//! re-exports them for existing `crate::grid::s2::cell_set::` call sites.

use super::cellid::CellId;
use super::projection::MAX_LEVEL;
pub(crate) use crate::grid::cell_set::{
    compact_with_floor, normalize, uncompact, uncompact_unlimited,
};
#[cfg(test)]
use crate::grid::cell_set::{contains_any, difference, intersection, union};

enum CellIdChildren {
    Leaf,
    Active { index: usize, children: [CellId; 4] },
}

impl Iterator for CellIdChildren {
    type Item = CellId;

    fn next(&mut self) -> Option<CellId> {
        match self {
            Self::Leaf => None,
            Self::Active { index, children } => {
                if *index >= 4 {
                    return None;
                }
                let child = children[*index];
                *index += 1;
                Some(child)
            },
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Leaf => (0, Some(0)),
            Self::Active { index, .. } => {
                let remaining = 4 - *index;
                (remaining, Some(remaining))
            },
        }
    }
}

impl ExactSizeIterator for CellIdChildren {
    fn len(&self) -> usize {
        match self {
            Self::Leaf => 0,
            Self::Active { index, .. } => 4 - *index,
        }
    }
}

impl crate::grid::cell_set::HierarchicalId for CellId {
    fn depth(self) -> u8 {
        self.level()
    }

    fn max_depth() -> u8 {
        MAX_LEVEL
    }

    fn range_min(self) -> u64 {
        Self::range_min(self).raw()
    }

    fn range_max(self) -> u64 {
        Self::range_max(self).raw()
    }

    fn parent(self) -> Option<Self> {
        (self.level() > 0)
            .then(|| Self::parent(self, self.level() - 1))
            .flatten()
    }

    fn children(self) -> impl ExactSizeIterator<Item = Self> {
        Self::children(self).map_or(CellIdChildren::Leaf, |children| CellIdChildren::Active {
            index: 0,
            children,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::projection::MAX_LEVEL;
    use super::*;
    use crate::grid::cell_set::intersects_any;

    fn cell(lon: f64, lat: f64, level: u8) -> CellId {
        CellId::from_lonlat(lon, lat)
            .parent(level)
            .expect("coarser")
    }

    /// Normalize matches the reference crate's `CellUnion::normalize` over
    /// adversarial inputs: duplicates, ancestor/descendant mixes, and
    /// complete sibling quartets that cascade.
    #[test]
    fn normalize_matches_oracle() {
        let base = cell(13.4, 52.5, 10);
        let children = base.children().expect("not leaf");
        let grandchildren = children[0].children().expect("not leaf");
        let cases: Vec<Vec<CellId>> = vec![
            vec![base, base, children[0]],
            children.to_vec(),
            vec![children[1], children[0], children[3], children[2]],
            grandchildren
                .iter()
                .chain(children[1..].iter())
                .copied()
                .collect(),
            vec![cell(0.0, 0.0, 5), cell(10.0, 10.0, 7), cell(0.0, 0.0, 9)],
            Vec::new(),
        ];
        for cells in cases {
            let ours = normalize(cells.clone());
            let mut oracle = ::s2::cellunion::CellUnion(
                cells
                    .iter()
                    .map(|c| ::s2::cellid::CellID(c.raw()))
                    .collect(),
            );
            oracle.normalize();
            assert_eq!(
                ours.iter().map(|c| c.raw()).collect::<Vec<_>>(),
                oracle.0.iter().map(|c| c.0).collect::<Vec<_>>()
            );
            // Idempotent.
            assert_eq!(normalize(ours.clone()), ours);
        }
    }

    /// Floor-aware compaction: merging stops at the floor, coarser cells
    /// pass through.
    #[test]
    fn compact_respects_floor() {
        let base = cell(13.4, 52.5, 10);
        let children = base.children().expect("not leaf");
        // A complete quartet would merge to level 10 — the floor forbids it.
        assert_eq!(compact_with_floor(children.to_vec(), 11), children.to_vec());
        assert_eq!(compact_with_floor(children.to_vec(), 10), vec![base]);
        // Already-coarser cells pass through unchanged.
        let coarse = cell(0.0, 0.0, 3);
        let mixed = compact_with_floor(vec![coarse, children[0]], 5);
        assert_eq!(mixed, normalize(vec![coarse, children[0]]));
    }

    /// Uncompact inverts compact and orders along the curve.
    #[test]
    fn uncompact_round_trips() {
        let base = cell(13.4, 52.5, 10);
        let expanded = uncompact(&[base], 12).expect("within budget");
        assert_eq!(expanded.len(), 16);
        assert!(expanded.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(expanded.iter().all(|&child| base.contains(child)));
        assert_eq!(normalize(expanded), vec![base]);
        // Leaf-level identity.
        let leaf = cell(1.0, 2.0, MAX_LEVEL);
        assert_eq!(uncompact(&[leaf], MAX_LEVEL).expect("within budget"), vec![
            leaf
        ]);
    }

    /// Over-budget uncompact is rejected before allocation.
    #[test]
    fn uncompact_rejects_over_budget() {
        let root = cell(0.0, 0.0, 0);
        let err = uncompact(&[root], 15).expect_err("level 0 → 15 exceeds budget");
        assert!(err.estimated > crate::grid::UNCOMPACT_MAX_CELLS);
    }

    /// Set-algebra identities on hierarchy overlaps.
    #[test]
    fn set_algebra_identities() {
        let base = cell(13.4, 52.5, 10);
        let children = base.children().expect("not leaf");
        let left = normalize(vec![base]);
        let right = normalize(children[..2].to_vec());

        // union(ancestor, descendants) = ancestor.
        assert_eq!(union(left.clone(), right.clone()), left);
        // intersection keeps the finer side.
        assert_eq!(intersection(&left, &right), right);
        // difference splits exactly: base - first two children = last two.
        assert_eq!(
            difference(&left, &right).unwrap(),
            normalize(children[2..].to_vec())
        );
        // x - x = empty; x - empty = x.
        assert!(difference(&left, &left).unwrap().is_empty());
        assert_eq!(difference(&left, &[]).unwrap(), left);
        // Membership range searches.
        assert!(contains_any(&left, children[3]));
        assert!(intersects_any(&right, base));
        assert!(!contains_any(&right, base));
        assert!(!intersects_any(&right, children[3]));
        let far = cell(100.0, -40.0, 10);
        assert!(!intersects_any(&left, far));
    }
}
