#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::grid::cell::CellDepth;
use crate::py::cells::coverage_ops::CoverageCells;
use crate::py::cells::*;

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
        parse_cell: |cell| h3_cell_index(cell).map(H3SetId),
        array: |cells| h3_cell_array(cells.into_iter().map(|id| id.0).collect()),
        union: h3_union,
        intersection: h3_intersection,
        difference: h3_difference,
        example_union: r"
>>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cell = gm.h3_cover(p, resolution=7).cells[0]
>>> nbr = list(cell.neighbors)[0]
>>> len(gm.h3_union([cell], [nbr]))
2
",
        example_intersection: r"
>>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cell = gm.h3_cover(p, resolution=7).cells[0]
>>> nbr = list(cell.neighbors)[0]
>>> len(gm.h3_intersection([cell, nbr], [nbr]))
1
",
        example_difference: r"
>>> import gometry as gm
>>> p = gm.Point(-122.4194, 37.7749, crs=4326)
>>> cell = gm.h3_cover(p, resolution=7).cells[0]
>>> nbr = list(cell.neighbors)[0]
>>> len(gm.h3_difference([cell, nbr], [nbr]))
1
",
    }
}

/// Rebuild a pickled H3Coverage from its public fields (internal; see
/// ``H3Coverage.__reduce__``).
///
/// Source geometry is normalized through the same lon/lat path as ``h3_cover``;
/// membership is recomputed from the source. Visible cell ids are user-selected
/// state (compact/with_parents); partition recomputation stays under the
/// recorded factory budget.
#[pyfunction]
pub(super) fn _unpickle_h3_coverage(
    geometry: &Bound<'_, PyAny>,
    cell_ids: &Bound<'_, PyAny>,
    cell_rule: &str,
    factory_resolution: u8,
    visible_depth: Option<u8>,
    max_cells: Option<i64>,
) -> PyResult<PyH3Coverage> {
    let geometry_in = exact_geometry(geometry)
        .ok_or_else(expected_geometry_or_array)?
        .clone();
    let (geometry, cover_shape) =
        crate::py::cells::coverage_ops::coverage_factory_shapes(&geometry_in, "H3")?;
    let cell_rule = CellRule::parse(cell_rule)
        .map_err(|message| crate::py::errors::parameter_error(message, "cell_rule"))?;
    // Factory partition depth; independent of post-transform
    // visible depth (uncompact / with_parents). Validate *both* before any
    // empty-cell depth fallback so an impossible visible_depth (e.g. 255)
    // cannot enter the restored CellDepth (D20).
    let resolution = h3_resolution(factory_resolution)?;
    if let Some(visible) = visible_depth {
        h3_resolution(visible)?;
    }
    let max_cells = crate::py::cells::coverage_ops::parse_max_cells(max_cells)?;
    let raw_ids: Vec<u64> = crate::py::cells::coverage_ops::collect_coverage_sequence(
        cell_ids,
        "H3 coverage pickle cells",
    )?;
    let cells = h3_cell_vec(
        raw_ids
            .into_iter()
            .map(validate_h3_index_id::<CellIndex>)
            .collect::<PyResult<Vec<_>>>()?,
    );
    // Bound recompute by the factory's recorded max_cells (D07). Payload
    // max_cells=None is the adult unlimited factory choice — recompute stays
    // unbounded (equals the factory's own work, not amplification).
    let unsplit = geometry.shape.as_ref();
    let membership = Arc::new(
        h3_membership_for_shape(unsplit, &cover_shape, resolution, max_cells)
            .map_err(crate::py::cells::coverage_ops::unpickle_cover_budget_err)?,
    );
    let owned_cells = CoverageCells::from_cells(cells);
    // Expected visible set matches the factory's cell_rule selection.
    let expected = match cell_rule {
        CellRule::Overlap => membership.partition.all(),
        CellRule::Within => membership.partition.interior(),
        CellRule::Center => membership.partition.select(|cell| {
            let center = h3o::LatLng::from(cell.cell);
            let probe = crate::geometry::ShapeData::from(crate::geometry::Shape::Point(
                crate::geometry::Point::new_unchecked_xy(center.lng(), center.lat()),
            ));
            crate::py::functions::predicate::topology_scalar_pair(
                &crate::py::functions::predicate::Predicate::Covers.spec(),
                unsplit,
                &probe,
                true,
            )
        }),
        CellRule::Bbox => {
            let annotated =
                super::tile::h3_tile(&cover_shape, unsplit, resolution, CellRule::Bbox, max_cells)
                    .map_err(crate::py::cells::coverage_ops::unpickle_cover_budget_err)?;
            CoverageCells::from_cells(h3_cell_vec(
                annotated.into_iter().map(|cell| cell.cell).collect(),
            ))
        },
    };
    let cells = if owned_cells.same_ids(&expected) {
        expected
    } else {
        owned_cells
    };
    let depth = CellDepth::from_levels(cells.iter().map(|cell| cell.cell.resolution().into()))
        .or_else(|| visible_depth.map(CellDepth::Uniform))
        .unwrap_or(CellDepth::Uniform(factory_resolution));
    Ok(PyH3Coverage {
        geometry,
        cells,
        cell_rule,
        depth,
        membership,
        max_cells,
    })
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
