//! Generic hierarchical cell-set algebra.
//!
//! `CanonicalCellSet` stores a normalized, non-overlapping, sibling-merged
//! representation in descendant-range order for the algorithms. Its public
//! conversion is explicitly sorted by each cell's `public_key`, so internal
//! range order never becomes an accidental Python ordering contract.

use super::cell::HierarchicalCell;

/// A fallibly-built canonical cell set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CanonicalCellSet<C: HierarchicalCell> {
    cells: Vec<C>,
}

impl<C: HierarchicalCell> CanonicalCellSet<C> {
    /// Normalize an arbitrary finite vector. Allocation failures are returned
    /// as resource errors rather than aborting through `Vec` growth.
    pub(crate) fn new(cells: Vec<C>) -> crate::error::Result<Self> {
        Ok(Self::from_range_sorted(normalize_with_floor(cells, 0)?))
    }

    const fn from_range_sorted(cells: Vec<C>) -> Self {
        Self { cells }
    }

    pub(crate) fn as_slice(&self) -> &[C] {
        &self.cells
    }

    /// Consume the set in the grid family's documented public identity order.
    /// The range-canonical storage is never exposed to callers.
    pub(crate) fn into_public_vec(mut self) -> Vec<C> {
        self.cells.sort_unstable_by_key(|cell| cell.public_key());
        self.cells
    }
}

fn try_copy<T: Copy>(values: &[T]) -> crate::error::Result<Vec<T>> {
    let mut copied = Vec::new();
    crate::boundary::try_reserve_vec(&mut copied, values.len())?;
    for &value in values {
        crate::boundary::try_push_vec(&mut copied, value)?;
    }
    Ok(copied)
}

/// Canonicalize with sibling groups never merging into parents coarser than
/// `floor` (cells already coarser pass through unchanged). The returned set's
/// public conversion follows the family's explicit identity order.
fn normalize_with_floor<C: HierarchicalCell>(
    mut cells: Vec<C>,
    floor: u8,
) -> crate::error::Result<Vec<C>> {
    // Ancestors first on equal range starts, so absorption is a single
    // forward pass.
    cells.sort_unstable_by(|a, b| {
        a.range_min()
            .cmp(&b.range_min())
            .then(b.range_max().cmp(&a.range_max()))
    });
    cells.dedup();
    let mut out: Vec<C> = Vec::new();
    crate::boundary::try_reserve_vec(&mut out, cells.len())?;
    for cell in cells {
        if out.last().is_some_and(|&last| last.contains(cell)) {
            continue;
        }
        crate::boundary::try_push_vec(&mut out, cell)?;
        // Merge complete sibling groups (cascading upward). `out` is sorted,
        // distinct, and free of containment, so `count` cells one level below
        // `parent` and inside its range are exactly its children.
        while let Some(parent) = out.last().and_then(|&last| last.parent()) {
            if parent.depth() < floor {
                break;
            }
            let count = parent.child_count();
            if out.len() < count {
                break;
            }
            let child_depth = parent.depth() + 1;
            let tail = &out[out.len() - count..];
            if !tail
                .iter()
                .all(|&cell| cell.depth() == child_depth && parent.contains(cell))
            {
                break;
            }
            out.truncate(out.len() - count);
            crate::boundary::try_push_vec(&mut out, parent)?;
        }
    }
    Ok(out)
}

/// Compact to the coarsest covering with merging stopped at `floor`
/// (cells already coarser than the floor pass through unchanged).
pub(crate) fn compact_with_floor<C: HierarchicalCell>(
    cells: Vec<C>,
    floor: u8,
) -> crate::error::Result<CanonicalCellSet<C>> {
    Ok(CanonicalCellSet::from_range_sorted(normalize_with_floor(
        cells, floor,
    )?))
}

/// Expand every cell to `target` (each must be at or above it). Overlapping
/// inputs collapse to a canonical set in public order. The only bounds are
/// checked arithmetic and allocator capacity; there is no fixed cell count
/// budget.
pub(crate) fn uncompact<C: HierarchicalCell>(
    cells: &[C],
    target: u8,
) -> crate::error::Result<CanonicalCellSet<C>> {
    let canonical = CanonicalCellSet::new(try_copy(cells)?)?;
    uncompact_canonical(&canonical, target)
}

fn checked_estimate<C: HierarchicalCell>(cells: &[C], target: u8) -> crate::error::Result<usize> {
    let mut estimated = 0_usize;
    for &cell in cells {
        let count = usize::try_from(cell.descendant_count(target))
            .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        estimated = estimated
            .checked_add(count)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
    }
    Ok(estimated)
}

fn uncompact_canonical<C: HierarchicalCell>(
    cells: &CanonicalCellSet<C>,
    target: u8,
) -> crate::error::Result<CanonicalCellSet<C>> {
    let estimated = checked_estimate(cells.as_slice(), target)?;
    let mut out = Vec::new();
    crate::boundary::try_reserve_vec(&mut out, estimated)?;
    let mut stack = Vec::new();
    crate::boundary::try_reserve_vec(&mut stack, cells.as_slice().len())?;
    for &cell in cells.as_slice().iter().rev() {
        crate::boundary::try_push_vec(&mut stack, cell)?;
    }
    while let Some(cell) = stack.pop() {
        if cell.depth() >= target {
            crate::boundary::try_push_vec(&mut out, cell)?;
        } else {
            let children_start = stack.len();
            for child in cell.children() {
                crate::boundary::try_push_vec(&mut stack, child)?;
            }
            stack[children_start..].reverse();
        }
    }
    Ok(CanonicalCellSet::from_range_sorted(out))
}

/// Hierarchy-aware union: the normalized combination of both sets.
pub(crate) fn union<C: HierarchicalCell>(
    left: CanonicalCellSet<C>,
    right: CanonicalCellSet<C>,
) -> crate::error::Result<CanonicalCellSet<C>> {
    let mut cells = left;
    let additional = right.cells.len();
    crate::boundary::try_reserve_vec(&mut cells.cells, additional)?;
    cells.cells.extend(right.cells);
    CanonicalCellSet::new(cells.cells)
}

/// Hierarchy-aware intersection of two NORMALIZED sets: overlap implies
/// containment, so the finer cell survives on each overlapping range.
pub(crate) fn intersection<C: HierarchicalCell>(
    left: &CanonicalCellSet<C>,
    right: &CanonicalCellSet<C>,
) -> crate::error::Result<CanonicalCellSet<C>> {
    let mut out = Vec::new();
    crate::boundary::try_reserve_vec(&mut out, left.cells.len().min(right.cells.len()))?;
    let (mut i, mut j) = (0, 0);
    while i < left.cells.len() && j < right.cells.len() {
        let (a, b) = (left.cells[i], right.cells[j]);
        if a.range_max() < b.range_min() {
            i += 1;
        } else if b.range_max() < a.range_min() {
            j += 1;
        } else {
            crate::boundary::try_push_vec(&mut out, if a.contains(b) { b } else { a })?;
            if a.range_max() < b.range_max() {
                i += 1;
            } else {
                j += 1;
            }
        }
    }
    Ok(CanonicalCellSet::from_range_sorted(out))
}

/// Hierarchy-aware difference of two NORMALIZED sets: cells partially
/// covered by `right` split into children until the remainder is exact
/// (recursion bottoms out at the system's maximum depth).
pub(crate) fn difference<C: HierarchicalCell>(
    left: &CanonicalCellSet<C>,
    right: &CanonicalCellSet<C>,
) -> crate::error::Result<CanonicalCellSet<C>> {
    fn subtract<C: HierarchicalCell>(
        cell: C,
        subtrahend: &[C],
        out: &mut Vec<C>,
    ) -> crate::error::Result<()> {
        let start = subtrahend.partition_point(|member| member.range_max() < cell.range_min());
        let end = start
            + subtrahend[start..].partition_point(|member| member.range_min() <= cell.range_max());
        let overlapping = &subtrahend[start..end];
        if overlapping.is_empty() {
            crate::boundary::try_push_vec(out, cell)?;
        } else if !overlapping[0].contains(cell) && cell.depth() < C::max_depth() {
            for child in cell.children() {
                subtract(child, overlapping, out)?;
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    crate::boundary::try_reserve_vec(&mut out, left.cells.len())?;
    for &cell in &left.cells {
        subtract(cell, &right.cells, &mut out)?;
    }
    Ok(CanonicalCellSet::from_range_sorted(out))
}

/// Whether a NORMALIZED set contains `cell` entirely (some member is an
/// ancestor-or-equal of it).
#[cfg(test)]
pub(crate) fn contains_any<C: HierarchicalCell>(cells: &[C], cell: C) -> bool {
    let index = cells.partition_point(|member| member.range_min() <= cell.range_min());
    index > 0 && cells[index - 1].range_max() >= cell.range_max()
}

/// Whether a NORMALIZED set overlaps `cell` at all (test oracle helper; the
/// production difference traversal carries narrowed overlapping slices).
#[cfg(test)]
pub(crate) fn intersects_any<C: HierarchicalCell>(cells: &[C], cell: C) -> bool {
    let index = cells.partition_point(|member| member.range_max() < cell.range_min());
    index < cells.len() && cells[index].range_min() <= cell.range_max()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::super::geohash::Geohash;
    use super::super::s2::cellid::CellId;
    use super::super::tile::Tile;
    use super::*;

    static ROOT_CHILD_ENUMERATIONS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct InstrumentedId(u8);

    impl HierarchicalCell for InstrumentedId {
        fn depth(self) -> u8 {
            u8::from(self.0 != 0)
        }

        fn max_depth() -> u8 {
            1
        }

        fn public_key(self) -> u64 {
            u64::from(self.0)
        }

        fn range_min(self) -> u64 {
            u64::from(self.0)
        }

        fn range_max(self) -> u64 {
            if self == Self(0) {
                2
            } else {
                u64::from(self.0)
            }
        }

        fn parent(self) -> Option<Self> {
            None
        }

        fn child_count(self) -> usize {
            2
        }

        fn children(self) -> impl ExactSizeIterator<Item = Self> {
            if self == Self(0) {
                ROOT_CHILD_ENUMERATIONS.fetch_add(1, Ordering::Relaxed);
            }
            [Self(1), Self(2)].into_iter()
        }
    }

    fn assert_uncompact_order<C: HierarchicalCell + std::fmt::Debug>(
        roots: [C; 2],
        target: u8,
        inputs: &[C],
    ) {
        let roots = CanonicalCellSet::new(roots.into_iter().collect()).unwrap();
        let expected = roots
            .as_slice()
            .iter()
            .flat_map(|&root| {
                HierarchicalCell::children(root)
                    .flat_map(HierarchicalCell::children)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let expanded = uncompact(inputs, target).unwrap();
        assert_eq!(expanded.as_slice(), expected);
        assert!(
            expanded
                .as_slice()
                .windows(2)
                .all(|pair| pair[0].range_min() < pair[1].range_min())
        );
    }

    #[test]
    fn geohash_set_algebra_identities() {
        let base = Geohash::from_lonlat(13.4, 52.5, 4);
        let children: Vec<_> = HierarchicalCell::children(base).collect();
        let left = CanonicalCellSet::new(vec![base]).unwrap();
        let right = CanonicalCellSet::new(children[..2].to_vec()).unwrap();

        assert_eq!(union(left.clone(), right.clone()).unwrap(), left);
        assert_eq!(intersection(&left, &right).unwrap(), right);
        let expected = CanonicalCellSet::new(children[2..].to_vec()).unwrap();
        assert_eq!(difference(&left, &right).unwrap(), expected);
        assert!(difference(&left, &left).unwrap().as_slice().is_empty());
        let empty = CanonicalCellSet::new(Vec::new()).unwrap();
        assert_eq!(difference(&left, &empty).unwrap(), left);
        // A complete sibling group merges back into the parent.
        assert_eq!(
            CanonicalCellSet::new(HierarchicalCell::children(base).collect::<Vec<_>>())
                .unwrap()
                .into_public_vec(),
            vec![base]
        );
        assert!(contains_any(left.as_slice(), children[7]));
        assert!(intersects_any(right.as_slice(), base));
        assert!(!contains_any(right.as_slice(), base));
        assert!(!intersects_any(right.as_slice(), children[5]));
    }

    #[test]
    fn tile_set_algebra_identities() {
        let base = Tile::from_lonlat(13.4, 52.5, 8).expect("in domain");
        let children: Vec<_> = HierarchicalCell::children(base).collect();
        let left = CanonicalCellSet::new(vec![base]).unwrap();
        let right = CanonicalCellSet::new(children[..2].to_vec()).unwrap();

        assert_eq!(union(left.clone(), right.clone()).unwrap(), left);
        assert_eq!(intersection(&left, &right).unwrap(), right);
        assert_eq!(
            difference(&left, &right).unwrap(),
            CanonicalCellSet::new(children[2..].to_vec()).unwrap()
        );
        assert!(difference(&left, &left).unwrap().as_slice().is_empty());
        assert_eq!(
            CanonicalCellSet::new(HierarchicalCell::children(base).collect::<Vec<_>>())
                .unwrap()
                .into_public_vec(),
            vec![base]
        );
        // Range keys nest: every child range inside the parent's.
        for child in &children {
            assert!(base.range_min() <= child.range_min());
            assert!(child.range_max() <= base.range_max());
        }
    }

    #[test]
    fn uncompact_normalizes_and_preserves_tile_range_order() {
        let first = Tile { z: 5, x: 3, y: 7 };
        let second = Tile { z: 5, x: 20, y: 11 };
        let first_child = first.children()[0];
        assert_uncompact_order([first, second], 7, &[
            second,
            first_child,
            first,
            second,
            first_child,
        ]);
    }

    #[test]
    fn uncompact_normalizes_and_preserves_geohash_range_order() {
        let first = Geohash::parse("u").unwrap();
        let second = Geohash::parse("e").unwrap();
        let first_child = first.children().next().unwrap();
        assert_uncompact_order([first, second], 3, &[
            second,
            first_child,
            first,
            second,
            first_child,
        ]);
    }

    #[test]
    fn uncompact_normalizes_and_preserves_s2_range_order() {
        let first = CellId::from_lonlat(-120.0, 30.0).parent(5).unwrap();
        let second = CellId::from_lonlat(120.0, 30.0).parent(5).unwrap();
        let first_child = first.children().unwrap()[0];
        assert_uncompact_order([first, second], 7, &[
            second,
            first_child,
            first,
            second,
            first_child,
        ]);
    }

    #[test]
    fn uncompact_normalizes_roots_before_expansion() {
        ROOT_CHILD_ENUMERATIONS.store(0, Ordering::Relaxed);
        let expanded = uncompact(
            &[InstrumentedId(0), InstrumentedId(1), InstrumentedId(0)],
            1,
        )
        .unwrap();

        assert_eq!(expanded.into_public_vec(), vec![
            InstrumentedId(1),
            InstrumentedId(2)
        ]);
        assert_eq!(ROOT_CHILD_ENUMERATIONS.load(Ordering::Relaxed), 1);
    }
}
