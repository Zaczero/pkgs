use h3o::CellIndex;

use super::tile::H3TileError;
use crate::grid::h3_coverer::{H3CoverError, H3CoverPlan, H3TraversalRule, h3_cover_shape};
use crate::py::cells::coverage_ops::{
    GridCoverError, coverage_factory_shapes, envelope_window_shapes,
};
use crate::py::cells::h3::{
    _unpickle_h3_cell_v1, _unpickle_h3_edge_array_v1, _unpickle_h3_edge_v1,
    _unpickle_h3_vertex_array_v1, _unpickle_h3_vertex_v1, PyH3Cell, PyH3Edge, PyH3EdgeArray,
    PyH3EdgeArrayIter, PyH3Vertex, PyH3VertexArray, PyH3VertexArrayIter, Resolution, h3_base_cells,
    h3_bounding_cell, h3_cells, h3_cover, h3_difference, h3_intersection, h3_pentagons, h3_tile,
    h3_union,
};
use crate::py::cells::{
    Bound, CellRule, GeometryError, H3_MAX_RESOLUTION, PyAny, PyGeometry, PyModule,
    PyModuleMethods as _, PyResult, py_i64_required,
};

impl From<H3CoverError> for GridCoverError {
    fn from(error: H3CoverError) -> Self {
        match error {
            H3CoverError::Budget(error) => Self::Budget(error),
            H3CoverError::Native(error) => Self::Native(error),
            H3CoverError::CapacityOverflow => Self::H3Capacity,
        }
    }
}

impl From<H3TileError> for GridCoverError {
    fn from(error: H3TileError) -> Self {
        match error {
            H3TileError::Budget(error) => Self::Budget(error),
            H3TileError::Preparation(error) => Self::Native(error),
        }
    }
}

/// Register the H3 classes, flat functions, and pickle rebuilders.
pub(in crate::py::cells) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    crate::add_functions!(m;
        h3_cells, h3_cover, h3_bounding_cell,
        h3_pentagons, h3_base_cells, h3_union, h3_intersection,
        h3_difference, _unpickle_h3_cell_v1, _unpickle_h3_vertex_v1,
        _unpickle_h3_vertex_array_v1, _unpickle_h3_edge_v1, _unpickle_h3_edge_array_v1,
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
    if geometry.shape.is_empty() {
        return Err(GridCoverError::Empty("H3"));
    }
    match cell_rule {
        // Center retains its specialized native tiler. The split shape is only
        // a traversal/seed working image; membership stays unsplit and normalized.
        CellRule::Center => {
            let cover_shape = coverage_factory_shapes(geometry, "H3")?;
            let authority = geometry.plan_or_transient()?;
            let crate::geometry::TopologyDomain::Geographic(frame) =
                crate::geometry::topology_domain(&geometry.frame)?
            else {
                unreachable!("H3 membership geometry has a geographic frame")
            };
            let annotated = h3_tile(
                &cover_shape,
                &authority,
                &*geometry.frame_cache()?,
                frame,
                resolution,
                max_cells,
            )?;
            Ok(annotated.into_iter().map(|cell| cell.cell).collect())
        },
        CellRule::Within | CellRule::Overlap | CellRule::Bbox => {
            let (source, rule) = match cell_rule {
                CellRule::Within => (None, H3TraversalRule::Within),
                CellRule::Overlap => (None, H3TraversalRule::Overlap),
                CellRule::Bbox => (
                    Some(crate::geometry::Shape::geometry_collection(
                        envelope_window_shapes(&coverage_factory_shapes(geometry, "H3")?)?,
                    )?),
                    H3TraversalRule::Overlap,
                ),
                CellRule::Center => unreachable!("center uses the specialized tiler"),
            };
            let annotated = h3_cover_shape(
                source.as_ref().unwrap_or_else(|| geometry.shape.as_ref()),
                &H3CoverPlan::new(resolution),
                rule,
                max_cells,
            )?;
            // `within` is the universal mirror of overlap completeness. A
            // chord proxy may witness contact, but it cannot certify that the
            // true spherical cell stays inside the affine lon/lat source.
            // Therefore uncertainty fails CLOSED here (exclude Boundary), the
            // deliberate inverse of overlap's fail-OPEN completeness rule.
            Ok(annotated.into_iter().map(|cell| cell.cell).collect())
        },
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
