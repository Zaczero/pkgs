//! Normalized S2 cell-set algebra: sorted, non-overlapping, sibling-merged
//! id vectors with hierarchy-aware union/intersection/difference and the
//! range searches that answer mixed-level membership.
//!
//! S2's complete quadtree makes every cell one contiguous leaf-id range, so
//! all of this is sorted-vector work — no trees, no hashing. The kernels
//! live in [`crate::grid::cell_set`] via [`HierarchicalCell`].

use crate::grid::cell::HierarchicalCell;
#[cfg(test)]
use crate::grid::cell_set::{
    compact_with_floor, contains_any, difference, intersection, uncompact, union,
};
use crate::grid::s2::cellid::CellId;
use crate::grid::s2::projection::MAX_LEVEL;

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

impl HierarchicalCell for CellId {
    fn depth(self) -> u8 {
        self.level()
    }

    fn max_depth() -> u8 {
        MAX_LEVEL
    }

    fn public_key(self) -> u64 {
        self.raw()
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
    use crate::grid::cell_set::{CanonicalCellSet, intersects_any};

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
            let ours = CanonicalCellSet::new(cells.clone()).unwrap();
            let mut oracle = ::s2::cellunion::CellUnion(
                cells
                    .iter()
                    .map(|c| ::s2::cellid::CellID(c.raw()))
                    .collect(),
            );
            oracle.normalize();
            assert_eq!(
                ours.as_slice().iter().map(|c| c.raw()).collect::<Vec<_>>(),
                oracle.0.iter().map(|c| c.0).collect::<Vec<_>>()
            );
            // Idempotent.
            assert_eq!(
                CanonicalCellSet::new(ours.as_slice().to_vec()).unwrap(),
                ours
            );
        }
    }

    /// Floor-aware compaction: merging stops at the floor, coarser cells
    /// pass through.
    #[test]
    fn compact_respects_floor() {
        let base = cell(13.4, 52.5, 10);
        let children = base.children().expect("not leaf");
        // A complete quartet would merge to level 10 — the floor forbids it.
        assert_eq!(
            compact_with_floor(children.to_vec(), 11)
                .unwrap()
                .into_public_vec(),
            children.to_vec()
        );
        assert_eq!(
            compact_with_floor(children.to_vec(), 10)
                .unwrap()
                .into_public_vec(),
            vec![base]
        );
        // Already-coarser cells pass through unchanged.
        let coarse = cell(0.0, 0.0, 3);
        let mixed = compact_with_floor(vec![coarse, children[0]], 5).unwrap();
        assert_eq!(
            mixed,
            CanonicalCellSet::new(vec![coarse, children[0]]).unwrap()
        );
    }

    /// Uncompact inverts compact and orders along the curve.
    #[test]
    fn uncompact_round_trips() {
        let base = cell(13.4, 52.5, 10);
        let expanded = uncompact(&[base], 12).expect("within allocator");
        assert_eq!(expanded.as_slice().len(), 16);
        assert!(expanded.as_slice().windows(2).all(|pair| pair[0] < pair[1]));
        assert!(
            expanded
                .as_slice()
                .iter()
                .all(|&child| base.contains(child))
        );
        assert_eq!(
            CanonicalCellSet::new(expanded.as_slice().to_vec())
                .unwrap()
                .into_public_vec(),
            vec![base]
        );
        // Leaf-level identity.
        let leaf = cell(1.0, 2.0, MAX_LEVEL);
        assert_eq!(
            uncompact(&[leaf], MAX_LEVEL)
                .expect("within allocator")
                .into_public_vec(),
            vec![leaf]
        );
    }

    /// Set-algebra identities on hierarchy overlaps.
    #[test]
    fn set_algebra_identities() {
        let base = cell(13.4, 52.5, 10);
        let children = base.children().expect("not leaf");
        let left = CanonicalCellSet::new(vec![base]).unwrap();
        let right = CanonicalCellSet::new(children[..2].to_vec()).unwrap();

        // union(ancestor, descendants) = ancestor.
        assert_eq!(union(left.clone(), right.clone()).unwrap(), left);
        // intersection keeps the finer side.
        assert_eq!(intersection(&left, &right).unwrap(), right);
        // difference splits exactly: base - first two children = last two.
        assert_eq!(
            difference(&left, &right).unwrap(),
            CanonicalCellSet::new(children[2..].to_vec()).unwrap()
        );
        // x - x = empty; x - empty = x.
        assert!(difference(&left, &left).unwrap().as_slice().is_empty());
        let empty = CanonicalCellSet::new(Vec::new()).unwrap();
        assert_eq!(difference(&left, &empty).unwrap(), left);
        // Membership range searches.
        assert!(contains_any(left.as_slice(), children[3]));
        assert!(intersects_any(right.as_slice(), base));
        assert!(!contains_any(right.as_slice(), base));
        assert!(!intersects_any(right.as_slice(), children[3]));
        let far = cell(100.0, -40.0, 10);
        assert!(!intersects_any(left.as_slice(), far));
    }
}
