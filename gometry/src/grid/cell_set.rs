//! Generic hierarchical cell-set algebra.
//!
//! Normalized (sorted, non-overlapping, sibling-merged) id vectors with
//! hierarchy-aware union/intersection/difference, shared by the H3, S2,
//! geohash, and tile systems through [`HierarchicalId`]. The same
//! algorithms run over a per-system *descendant range key*: any total order
//! in which sibling ranges are disjoint and every descendant's range nests
//! inside its ancestor's. Gaps in the key space are fine — only nesting and
//! disjointness matter.

/// A grid cell id with a strict containment hierarchy.
///
/// Every cell has one parent (down to depth 0) and a fixed child set one
/// level finer, and its descendants occupy a dedicated, nested range of the
/// key space.
pub(crate) trait HierarchicalId: Copy + Eq {
    /// The cell's hierarchy depth (resolution / precision / zoom).
    fn depth(self) -> u8;
    /// The system's maximum depth (recursion floor for `difference`).
    fn max_depth() -> u8;
    /// Start of the cell's descendant key range (inclusive).
    fn range_min(self) -> u64;
    /// End of the cell's descendant key range (inclusive).
    fn range_max(self) -> u64;
    /// The parent one level coarser (`None` at depth 0).
    fn parent(self) -> Option<Self>;
    /// The children one level finer, in range order.
    fn children(self) -> impl ExactSizeIterator<Item = Self>;

    /// The immediate child count (powers the `uncompact` budget estimate).
    fn child_count(self) -> usize {
        self.children().len()
    }

    /// Whether this cell contains `other` (ancestor-or-equal).
    fn contains(self, other: Self) -> bool {
        self.range_min() <= other.range_min() && other.range_max() <= self.range_max()
    }
}

/// Sort and canonicalize: contained cells absorb into their ancestors and
/// complete sibling groups merge into parents (repeatedly), yielding the
/// minimal sorted non-overlapping representation.
pub(crate) fn normalize<C: HierarchicalId>(cells: Vec<C>) -> Vec<C> {
    normalize_with_floor(cells, 0)
}

/// [`normalize`], but sibling groups never merge into parents coarser than
/// `floor` (cells already coarser pass through unchanged).
pub(crate) fn normalize_with_floor<C: HierarchicalId>(mut cells: Vec<C>, floor: u8) -> Vec<C> {
    // Ancestors first on equal range starts, so absorption is a single
    // forward pass.
    cells.sort_unstable_by(|a, b| {
        a.range_min()
            .cmp(&b.range_min())
            .then(b.range_max().cmp(&a.range_max()))
    });
    cells.dedup();
    let mut out: Vec<C> = Vec::with_capacity(cells.len());
    for cell in cells {
        if out.last().is_some_and(|&last| last.contains(cell)) {
            continue;
        }
        out.push(cell);
        // Merge complete sibling groups (cascading upward).
        while let Some(parent) = out.last().and_then(|&last| last.parent()) {
            if parent.depth() < floor {
                break;
            }
            let siblings = parent.children();
            let count = siblings.len();
            if out.len() < count {
                break;
            }
            let tail = &out[out.len() - count..];
            if !tail.iter().zip(siblings).all(|(&a, b)| a == b) {
                break;
            }
            out.truncate(out.len() - count);
            out.push(parent);
        }
    }
    out
}

/// Compact to the coarsest covering with merging stopped at `floor`
/// (cells already coarser than the floor pass through unchanged).
pub(crate) fn compact_with_floor<C: HierarchicalId>(cells: Vec<C>, floor: u8) -> Vec<C> {
    normalize_with_floor(cells, floor)
}

/// Expand every cell to `target` (each must be at or above it). Overlapping
/// inputs collapse to a canonical set in range-key order.
///
/// Rejects when the estimated output would exceed the shared uncompact
/// budget — used by free cell-array paths. Coverage transforms use
/// [`uncompact_unlimited`] (explicit user ops are not re-capped).
pub(crate) fn uncompact<C: HierarchicalId>(
    cells: &[C],
    target: u8,
) -> Result<Vec<C>, crate::grid::UncompactBudgetExceeded> {
    let estimated = cells
        .iter()
        .map(|cell| {
            cell.child_count()
                .saturating_pow(u32::from(target.saturating_sub(cell.depth())))
        })
        .fold(0_usize, usize::saturating_add);
    crate::grid::ensure_uncompact_budget(estimated)?;
    Ok(uncompact_unlimited(cells, target))
}

/// Expand every cell to `target` with no cell-count budget (coverage
/// transforms and other explicit user expansions).
pub(crate) fn uncompact_unlimited<C: HierarchicalId>(cells: &[C], target: u8) -> Vec<C> {
    let estimated = cells
        .iter()
        .map(|cell| {
            cell.child_count()
                .saturating_pow(u32::from(target.saturating_sub(cell.depth())))
        })
        .fold(0_usize, usize::saturating_add);
    let mut out = Vec::with_capacity(estimated.min(1 << 20));
    let roots = normalize(cells.to_vec());
    let mut stack: Vec<C> = roots.into_iter().rev().collect();
    while let Some(cell) = stack.pop() {
        if cell.depth() >= target {
            out.push(cell);
        } else {
            let children_start = stack.len();
            stack.extend(cell.children());
            stack[children_start..].reverse();
        }
    }
    out
}

/// Hierarchy-aware union: the normalized combination of both sets.
pub(crate) fn union<C: HierarchicalId>(left: Vec<C>, right: Vec<C>) -> Vec<C> {
    let mut cells = left;
    cells.extend(right);
    normalize(cells)
}

/// Hierarchy-aware intersection of two NORMALIZED sets: overlap implies
/// containment, so the finer cell survives on each overlapping range.
pub(crate) fn intersection<C: HierarchicalId>(left: &[C], right: &[C]) -> Vec<C> {
    let mut out = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < left.len() && j < right.len() {
        let (a, b) = (left[i], right[j]);
        if a.range_max() < b.range_min() {
            i += 1;
        } else if b.range_max() < a.range_min() {
            j += 1;
        } else {
            out.push(if a.contains(b) { b } else { a });
            if a.range_max() < b.range_max() {
                i += 1;
            } else {
                j += 1;
            }
        }
    }
    out
}

/// Hierarchy-aware difference of two NORMALIZED sets: cells partially
/// covered by `right` split into children until the remainder is exact
/// (recursion bottoms out at the system's maximum depth).
pub(crate) fn difference<C: HierarchicalId>(
    left: &[C],
    right: &[C],
) -> Result<Vec<C>, crate::grid::CellLimitExceeded> {
    fn subtract<C: HierarchicalId>(
        cell: C,
        subtrahend: &[C],
        out: &mut crate::grid::CellCollector<C>,
    ) -> Result<(), crate::grid::CellLimitExceeded> {
        let start = subtrahend.partition_point(|member| member.range_max() < cell.range_min());
        let end = start
            + subtrahend[start..].partition_point(|member| member.range_min() <= cell.range_max());
        let overlapping = &subtrahend[start..end];
        if overlapping.is_empty() {
            out.push(cell)?;
        } else if !overlapping[0].contains(cell) && cell.depth() < C::max_depth() {
            for child in cell.children() {
                subtract(child, overlapping, out)?;
            }
        }
        Ok(())
    }
    let mut out = crate::grid::CellCollector::new("grid difference");
    for &cell in left {
        subtract(cell, right, &mut out)?;
    }
    Ok(normalize(out.into_vec()))
}

/// Whether a NORMALIZED set contains `cell` entirely (some member is an
/// ancestor-or-equal of it).
#[cfg(test)]
pub(crate) fn contains_any<C: HierarchicalId>(cells: &[C], cell: C) -> bool {
    let index = cells.partition_point(|member| member.range_min() <= cell.range_min());
    index > 0 && cells[index - 1].range_max() >= cell.range_max()
}

/// Whether a NORMALIZED set overlaps `cell` at all (test oracle helper; the
/// production difference traversal carries narrowed overlapping slices).
#[cfg(test)]
pub(crate) fn intersects_any<C: HierarchicalId>(cells: &[C], cell: C) -> bool {
    let index = cells.partition_point(|member| member.range_max() < cell.range_min());
    index < cells.len() && cells[index].range_min() <= cell.range_max()
}

impl HierarchicalId for super::geohash::Geohash {
    fn depth(self) -> u8 {
        self.precision
    }

    fn max_depth() -> u8 {
        super::geohash::GEOHASH_MAX_PRECISION
    }

    fn range_min(self) -> u64 {
        // Character bits are left-aligned with trailing zeros: the prefix
        // itself is the smallest descendant key.
        self.bits
    }

    fn range_max(self) -> u64 {
        self.bits | (u64::MAX >> (5 * u32::from(self.precision)))
    }

    fn parent(self) -> Option<Self> {
        (self.precision > 1).then(|| self.parent_at(self.precision - 1))
    }

    fn children(self) -> impl ExactSizeIterator<Item = Self> {
        Self::children(self)
    }
}

impl HierarchicalId for super::tile::Tile {
    fn depth(self) -> u8 {
        self.z
    }

    fn max_depth() -> u8 {
        super::tile::TILE_MAX_ZOOM
    }

    fn range_min(self) -> u64 {
        // The Morton payload left-aligned to the max-zoom bit width (the
        // packed id without its zoom bits): quadkey-prefix order.
        crate::curves::morton_interleave(self.x, self.y) << (58 - 2 * u32::from(self.z))
    }

    fn range_max(self) -> u64 {
        self.range_min() | ((1_u64 << (58 - 2 * u32::from(self.z))) - 1)
    }

    fn parent(self) -> Option<Self> {
        (self.z > 0).then(|| self.parent_at(self.z - 1))
    }

    fn children(self) -> impl ExactSizeIterator<Item = Self> {
        Self::children(self).into_iter()
    }
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

    impl HierarchicalId for InstrumentedId {
        fn depth(self) -> u8 {
            u8::from(self.0 != 0)
        }

        fn max_depth() -> u8 {
            1
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

    fn assert_uncompact_order<C: HierarchicalId + std::fmt::Debug>(
        roots: [C; 2],
        target: u8,
        inputs: &[C],
    ) {
        let roots = normalize(roots.into_iter().collect());
        let expected = roots
            .iter()
            .flat_map(|&root| {
                HierarchicalId::children(root)
                    .flat_map(HierarchicalId::children)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let expanded = uncompact_unlimited(inputs, target);
        assert_eq!(expanded, expected);
        assert!(
            expanded
                .windows(2)
                .all(|pair| pair[0].range_min() < pair[1].range_min())
        );
    }

    #[test]
    fn geohash_set_algebra_identities() {
        let base = Geohash::from_lonlat(13.4, 52.5, 4);
        let children: Vec<_> = HierarchicalId::children(base).collect();
        let left = normalize(vec![base]);
        let right = normalize(children[..2].to_vec());

        assert_eq!(union(left.clone(), right.clone()), left);
        assert_eq!(intersection(&left, &right), right);
        let expected = normalize(children[2..].to_vec());
        assert_eq!(difference(&left, &right).unwrap(), expected);
        assert!(difference(&left, &left).unwrap().is_empty());
        assert_eq!(difference(&left, &[]).unwrap(), left);
        // A complete sibling group merges back into the parent.
        assert_eq!(
            normalize(HierarchicalId::children(base).collect::<Vec<_>>()),
            vec![base]
        );
        assert!(contains_any(&left, children[7]));
        assert!(intersects_any(&right, base));
        assert!(!contains_any(&right, base));
        assert!(!intersects_any(&right, children[5]));
    }

    #[test]
    fn tile_set_algebra_identities() {
        let base = Tile::from_lonlat(13.4, 52.5, 8).expect("in domain");
        let children: Vec<_> = HierarchicalId::children(base).collect();
        let left = normalize(vec![base]);
        let right = normalize(children[..2].to_vec());

        assert_eq!(union(left.clone(), right.clone()), left);
        assert_eq!(intersection(&left, &right), right);
        assert_eq!(
            difference(&left, &right).unwrap(),
            normalize(children[2..].to_vec())
        );
        assert!(difference(&left, &left).unwrap().is_empty());
        assert_eq!(
            normalize(HierarchicalId::children(base).collect::<Vec<_>>()),
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
        let expanded = uncompact_unlimited(
            &[InstrumentedId(0), InstrumentedId(1), InstrumentedId(0)],
            1,
        );

        assert_eq!(expanded, vec![InstrumentedId(1), InstrumentedId(2)]);
        assert_eq!(ROOT_CHILD_ENUMERATIONS.load(Ordering::Relaxed), 1);
    }
}
