use h3o::CellIndex;

use super::tile::H3TileError;
use crate::error::ResourceError;
use crate::grid::h3_coverer::{H3CoverError, H3CoverPlan, H3TraversalRule, h3_cover_shape};
use crate::py::cells::coverage_ops::{
    GridCoverError, coverage_factory_geometry, coverage_factory_shapes,
};
use crate::py::cells::h3::{
    _unpickle_h3_cell, _unpickle_h3_edge, _unpickle_h3_edge_array, _unpickle_h3_vertex,
    _unpickle_h3_vertex_array, PyH3Cell, PyH3Edge, PyH3EdgeArray, PyH3EdgeArrayIter, PyH3Vertex,
    PyH3VertexArray, PyH3VertexArrayIter, Resolution, h3_base_cells, h3_bounding_cell, h3_cells,
    h3_cover, h3_difference, h3_intersection, h3_pentagons, h3_tile, h3_union,
};
use crate::py::cells::{
    Bound, CellRule, GeometryError, H3_MAX_RESOLUTION, PyAny, PyGeometry, PyModule,
    PyModuleMethods as _, PyResult, py_i64_required,
};

impl From<H3CoverError> for GridCoverError {
    fn from(error: H3CoverError) -> Self {
        match error {
            H3CoverError::Budget(error) => Self::Budget(error),
            H3CoverError::Allocation => Self::Native(ResourceError::Allocation.into()),
            H3CoverError::CapacityOverflow => Self::H3Capacity,
            H3CoverError::Geometry(error) => Self::Native(error),
        }
    }
}

impl From<H3TileError> for GridCoverError {
    fn from(error: H3TileError) -> Self {
        match error {
            H3TileError::Budget(error) => Self::Budget(error),
            H3TileError::Allocation => Self::Native(ResourceError::Allocation.into()),
        }
    }
}

/// Register the H3 classes, flat functions, and pickle rebuilders.
pub(in crate::py::cells) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::add_functions!(m;
        h3_cells, h3_cover, h3_bounding_cell,
        h3_pentagons, h3_base_cells, h3_union, h3_intersection,
        h3_difference, _unpickle_h3_cell, _unpickle_h3_vertex,
        _unpickle_h3_vertex_array, _unpickle_h3_edge, _unpickle_h3_edge_array,
    );
    crate::add_classes!(
        m;
        PyH3Cell, PyH3Vertex, PyH3VertexArray,
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
    resolution: Resolution,
    cell_rule: CellRule,
    max_cells: Option<usize>,
) -> Result<Vec<CellIndex>, GridCoverError> {
    let (membership_geometry, cells) = match cell_rule {
        // Center retains its specialized native tiler. The split shape is only
        // a traversal/seed working image; membership stays unsplit and normalized.
        CellRule::Center => {
            let (membership_geometry, cover_shape) = coverage_factory_shapes(geometry, "H3")?;
            let authority = membership_geometry.plan_or_transient();
            let crate::geometry::TopologyDomain::Geographic(frame) =
                crate::geometry::topology_domain(&membership_geometry.frame)?
            else {
                unreachable!("H3 membership geometry has a geographic frame")
            };
            let annotated = h3_tile(
                &cover_shape,
                &authority,
                &membership_geometry.frame_cache(),
                frame,
                resolution,
                max_cells,
            )?;
            let mut cells = Vec::new();
            for cell in annotated {
                crate::try_push_vec(&mut cells, cell.cell)?;
            }
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
            )?;
            // `within` is the universal mirror of overlap completeness. A
            // chord proxy may witness contact, but it cannot certify that the
            // true spherical cell stays inside the affine lon/lat source.
            // Therefore uncertainty fails CLOSED here (exclude Boundary), the
            // deliberate inverse of overlap's fail-OPEN completeness rule.
            let mut cells = Vec::new();
            for cell in annotated {
                crate::try_push_vec(&mut cells, cell.cell)?;
            }
            (membership_geometry, cells)
        },
    };
    let _ = (membership_geometry, cell_rule, max_cells);
    Ok(cells)
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
