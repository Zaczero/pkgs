use crate::py::cells::h3::{
    CellIndex, PyH3Cell, h3_cell_array, h3_cell_index, parse_h3_resolution, validate_h3_index_id,
};
use crate::py::cells::{Bound, PyAny, PyCellArray, PyResult, pyfunction};

grid_free_functions! {
    @set_algebra {
        cell_set_arg: h3_cell_set_arg,
        cell_type: CellIndex,
        label: "H3",
        cell_doc: "H3Cell",
        item_doc: "H3Cell, int, str, or iterable of those",
        contract_doc: "This is cell-ID algebra (the ``compact`` contract): an H3 child's *geometry* does not nest exactly inside its parent, but its id does.",
        parse_error_doc: "If an id or token is not a valid H3 cell.",
        parse_cell: h3_cell_index,
        array: |cells| h3_cell_array(cells),
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

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
/// Rebuild a pickled H3Cell from its 64-bit index (internal; see
/// ``H3Cell.__reduce__``).
#[pyfunction]
pub(super) fn _unpickle_h3_cell(
    #[pyo3(from_py_with = crate::strict_u64)] id: u64,
) -> PyResult<PyH3Cell> {
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
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> len(gm.h3_pentagons(7))
/// 12
#[pyfunction]
pub(super) fn h3_pentagons(resolution: &Bound<'_, PyAny>) -> PyResult<PyCellArray> {
    let resolution = parse_h3_resolution(resolution)?;
    Ok(h3_cell_array(resolution.pentagons()))
}

/// Return the 122 resolution-0 H3 base cells.
///
/// Returns
/// -------
/// CellArray of H3Cell
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> len(gm.h3_base_cells())
/// 122
#[pyfunction]
pub(super) fn h3_base_cells() -> PyCellArray {
    h3_cell_array(CellIndex::base_cells())
}
