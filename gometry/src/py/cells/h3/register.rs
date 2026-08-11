use pyo3::exceptions::PyMemoryError;

use crate::grid::cell::CellDepth;
use crate::grid::h3_coverer::{H3CoverError, H3CoverPlan, H3TraversalRule, h3_cover_shape};
use crate::py::cells::coverage_ops::{CoverageCells, cover_budget_err};
use crate::py::cells::h3::{
    _unpickle_h3_cell, _unpickle_h3_coverage, _unpickle_h3_edge, _unpickle_h3_edge_array,
    _unpickle_h3_vertex, _unpickle_h3_vertex_array, H3Membership, PyH3Cell, PyH3Coverage,
    PyH3CoverageIter, PyH3Edge, PyH3EdgeArray, PyH3EdgeArrayIter, PyH3Vertex, PyH3VertexArray,
    PyH3VertexArrayIter, Resolution, h3_base_cells, h3_bounding_cell, h3_cell_vec, h3_cells,
    h3_cover, h3_difference, h3_intersection, h3_pentagons, h3_tile, h3_union,
};
use crate::py::cells::{
    Bound, CellRule, GeometryError, H3_MAX_RESOLUTION, PyAny, PyErr, PyGeometry, PyModule,
    PyModuleMethods as _, PyResult, py_i64_required,
};

/// Register the H3 classes, flat functions, and pickle rebuilders.
pub(in crate::py::cells) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::add_functions!(m;
        h3_cells, h3_cover, h3_bounding_cell,
        h3_pentagons, h3_base_cells, h3_union, h3_intersection,
        h3_difference, _unpickle_h3_cell, _unpickle_h3_coverage, _unpickle_h3_vertex,
        _unpickle_h3_vertex_array, _unpickle_h3_edge, _unpickle_h3_edge_array,
    );
    crate::add_classes!(
        m;
        PyH3Coverage, PyH3CoverageIter, PyH3Cell, PyH3Vertex, PyH3VertexArray,
        PyH3VertexArrayIter, PyH3Edge, PyH3EdgeArray, PyH3EdgeArrayIter
    );
    Ok(())
}

/// Build an H3 coverage of `geometry` (the ``h3_cover`` backend).
///
/// `within`, `overlap`, and `bbox` share the certified affine traversal; center
/// retains its specialized point-probe tiler.  Inspection remains cold for every
/// visible rule and rebuilds the same certified overlap traversal on demand.
pub(super) fn build_coverage(
    geometry: &PyGeometry,
    resolution: &Bound<'_, PyAny>,
    cell_rule: CellRule,
    max_cells: Option<usize>,
) -> PyResult<PyH3Coverage> {
    use crate::py::cells::coverage_ops::{coverage_factory_geometry, coverage_factory_shapes};

    let resolution = parse_h3_resolution(resolution)?;
    let (membership_geometry, cells) = match cell_rule {
        // Center retains its specialized native tiler. Its split-normalized
        // shape stays contained to this legacy owner.
        CellRule::Center => {
            let (membership_geometry, cover_shape, _) = coverage_factory_shapes(geometry, "H3")?;
            let annotated =
                h3_tile(&cover_shape, resolution, max_cells).map_err(cover_budget_err)?;
            let cells = CoverageCells::from_cells(h3_cell_vec(
                annotated.into_iter().map(|cell| cell.cell).collect(),
            ));
            (membership_geometry, cells)
        },
        CellRule::Within | CellRule::Overlap | CellRule::Bbox => {
            let membership_geometry = coverage_factory_geometry(geometry, "H3")?;
            let unsplit = membership_geometry.shape.as_ref();
            let annotated = h3_cover_shape(
                unsplit,
                &H3CoverPlan::new(resolution),
                match cell_rule {
                    CellRule::Within => H3TraversalRule::Within,
                    CellRule::Bbox => H3TraversalRule::Bbox,
                    CellRule::Overlap => H3TraversalRule::Overlap,
                    CellRule::Center => unreachable!("center uses the specialized tiler"),
                },
                max_cells,
            )
            .map_err(h3_cover_err)?;
            // `within` is the universal mirror of overlap completeness. A
            // chord proxy may witness contact, but it cannot certify that the
            // true spherical cell stays inside the affine lon/lat source.
            // Therefore uncertainty fails CLOSED here (exclude Boundary), the
            // deliberate inverse of overlap's fail-OPEN completeness rule.
            let cells = CoverageCells::from_cells(h3_cell_vec(
                annotated.into_iter().map(|cell| cell.cell).collect(),
            ));
            (membership_geometry, cells)
        },
    };
    Ok(PyH3Coverage {
        geometry: membership_geometry,
        cells,
        cell_rule,
        depth: CellDepth::Uniform(resolution.into()),
        membership: H3Membership::lazy(resolution),
        max_cells,
    })
}

pub(super) fn h3_cover_err(error: H3CoverError) -> PyErr {
    match error {
        H3CoverError::Budget(error) => cover_budget_err(error),
        H3CoverError::Allocation => PyMemoryError::new_err("H3 coverage allocation failed"),
        H3CoverError::CapacityOverflow => {
            GeometryError::new_err("H3 coverage traversal exceeded its representable capacity")
        },
        H3CoverError::Geometry(error) => error.into(),
    }
}

pub(super) fn h3_resolution(value: u8) -> PyResult<Resolution> {
    value.try_into().map_err(|_| {
        GeometryError::new_err(format!(
            "H3 resolution must be between 0 and {H3_MAX_RESOLUTION}, got {value}"
        ))
    })
}

/// Shared i64 → Resolution conversion via the bound checker.
pub(super) fn h3_resolution_from_i64(value: i64) -> PyResult<Resolution> {
    h3_resolution(super::super::checked_depth(
        value,
        "H3 resolution",
        "resolution",
        0,
        i64::from(H3_MAX_RESOLUTION),
    )?)
}

pub(in crate::py::cells) fn parse_h3_resolution(value: &Bound<'_, PyAny>) -> PyResult<Resolution> {
    h3_resolution_from_i64(py_i64_required("H3 resolution", value)?)
}
