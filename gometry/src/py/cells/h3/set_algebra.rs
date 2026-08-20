#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::py::cells::h3::{
    CellIndex, PyH3Cell, Resolution, h3_cell_array, h3_cell_index, parse_h3_resolution,
    validate_h3_index_id,
};
use crate::py::cells::{Bound, H3_MAX_RESOLUTION, PyAny, PyCellArray, PyResult, pyfunction};

/// Range-key adapter making `CellIndex` a
/// [`crate::grid::cell_set::HierarchicalId`]: base cell + the 3-bit digit path
/// as a nested key range. H3's *id* hierarchy is exact (children's paths extend
/// the parent's) even though child geometry does not nest — so the set algebra
/// is id algebra, the same contract as `compact`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct H3SetId(CellIndex);

/// One-level finer children in h3o order without collecting.
pub(super) struct H3ImmediateChildren {
    cell: CellIndex,
    finer: Resolution,
    position: u64,
    remaining: u64,
}

impl Iterator for H3ImmediateChildren {
    type Item = H3SetId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let child = self.cell.child_at(self.position, self.finer)?;
        self.position += 1;
        self.remaining -= 1;
        Some(H3SetId(child))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = usize::try_from(self.remaining).unwrap_or(usize::MAX);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for H3ImmediateChildren {
    fn len(&self) -> usize {
        usize::try_from(self.remaining).unwrap_or(usize::MAX)
    }
}

impl crate::grid::cell_set::HierarchicalId for H3SetId {
    fn depth(self) -> u8 {
        self.0.resolution().into()
    }

    fn max_depth() -> u8 {
        H3_MAX_RESOLUTION
    }

    fn range_min(self) -> u64 {
        // Bits 0..45 hold the 15 base-7 digits (3 bits each, finest last);
        // unused digits of a valid index are all-ones — clearing them gives
        // the smallest descendant path, setting them the largest.
        let raw = u64::from(self.0);
        raw & !h3_unused_digit_mask(self.depth()) & ((1 << 52) - 1)
    }

    fn range_max(self) -> u64 {
        self.range_min() | h3_unused_digit_mask(self.depth())
    }

    fn parent(self) -> Option<Self> {
        let depth: u8 = self.0.resolution().into();
        (depth > 0).then(|| {
            Self(
                self.0
                    .parent(Resolution::try_from(depth - 1).expect("coarser resolution"))
                    .expect("non-zero resolution has a parent"),
            )
        })
    }

    fn children(self) -> impl ExactSizeIterator<Item = Self> {
        let depth: u8 = self.0.resolution().into();
        let finer = Resolution::try_from(depth + 1).expect("not at max depth");
        let remaining = self.0.children_count(finer);
        H3ImmediateChildren {
            cell: self.0,
            finer,
            position: 0,
            remaining,
        }
    }
}

/// All-ones mask over the digit fields below `resolution` (the unused,
/// set-to-7 digits of a valid H3 index).
pub(super) fn h3_unused_digit_mask(resolution: u8) -> u64 {
    let unused = 3 * (15 - u32::from(resolution));
    if unused == 0 { 0 } else { (1 << unused) - 1 }
}

grid_free_functions! {
    @set_algebra {
        cell_set_arg: h3_cell_set_arg,
        cell_type: H3SetId,
        label: "H3",
        cell_doc: "H3Cell",
        item_doc: "H3Cell, int, str, or iterable of those",
        contract_doc: "This is cell-ID algebra (the ``compact`` contract): an H3 child's *geometry* does not nest exactly inside its parent, but its id does.",
        parse_error_doc: "If an id or token is not a valid H3 cell.",
        parse_cell: |cell| h3_cell_index(cell).map(H3SetId),
        array: |cells| h3_cell_array(cells.into_iter().map(|id| id.0).collect()),
        union: h3_union,
        intersection: h3_intersection,
        difference: h3_difference,
        example_union: r"
>>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cover = gm.h3_cover(p, resolution=7)
>>> cell = cover[0]
>>> assert cell is not None
>>> neighbors = cell.neighbors
>>> nbr = neighbors[0]
>>> assert nbr is not None
>>> cells = [item for item in cover if item is not None]
>>> len(gm.h3_union(cells, [item for item in neighbors if item is not None][:1]))
2
",
        example_intersection: r"
>>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cover = gm.h3_cover(p, resolution=7)
>>> cell = cover[0]
>>> assert cell is not None
>>> neighbors = cell.neighbors
>>> nbr = neighbors[0]
>>> assert nbr is not None
>>> cells = [item for item in cover if item is not None]
>>> len(gm.h3_intersection(cells + [nbr], [item for item in neighbors if item is not None][:1]))
1
",
        example_difference: r"
>>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cover = gm.h3_cover(p, resolution=7)
>>> cell = cover[0]
>>> assert cell is not None
>>> neighbors = cell.neighbors
>>> nbr = neighbors[0]
>>> assert nbr is not None
>>> cells = [item for item in cover if item is not None]
>>> len(gm.h3_difference(cells + [nbr], [item for item in neighbors if item is not None][:1]))
1
",
    }
}

/// Rebuild a pickled H3Cell from its 64-bit index (internal; see
/// ``H3Cell.__reduce__``).
#[pyfunction]
pub(super) fn _unpickle_h3_cell(id: u64) -> PyResult<PyH3Cell> {
    Ok(PyH3Cell {
        cell: validate_h3_index_id::<CellIndex>(id)?,
    })
}

/// All pentagon cells at an H3 resolution (twelve per resolution).
///
/// Parameters
/// ----------
/// resolution : int
///     H3 resolution (``0``-``15``).
///
/// Returns
/// -------
/// CellArray of H3Cell
///
/// Raises
/// ------
/// GeometryError
///     If ``resolution`` is out of range.
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> len(gm.h3_pentagons(7))
/// 12
pub(super) fn h3_pentagons(resolution: &Bound<'_, PyAny>) -> PyResult<PyCellArray> {
    let resolution = parse_h3_resolution(resolution)?;
    Ok(h3_cell_array(resolution.pentagons().collect()))
}

/// Return the 122 resolution-0 H3 base cells.
///
/// Returns
/// -------
/// CellArray of H3Cell
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> len(gm.h3_base_cells())
/// 122
pub(super) fn h3_base_cells() -> PyCellArray {
    h3_cell_array(CellIndex::base_cells().collect())
}
