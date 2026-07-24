use super::*;
use crate::grid::cell::CellDepth;
use crate::py::cells::coverage_ops::{CoverageCells, cover_budget_err};
use crate::py::cells::*;
use crate::py::functions::predicate::{Predicate, topology_scalar_pair};

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
pub(super) fn build_coverage(
    geometry: &PyGeometry,
    resolution: &Bound<'_, PyAny>,
    cell_rule: CellRule,
    max_cells: Option<usize>,
) -> PyResult<PyH3Coverage> {
    use crate::py::cells::coverage_ops::coverage_factory_shapes;

    let resolution = parse_h3_resolution(resolution)?;
    let (membership_geometry, cover_shape) = coverage_factory_shapes(geometry, "H3")?;
    // Unsplit WGS84 source for geographic classification; cover_shape is the
    // split-normalized working shape used for outline tracing.
    let unsplit = membership_geometry.shape.as_ref();
    // Overlap membership is the traversal domain for every rule. Within/center
    // derive from it (no separate mode-specific flood); bbox keeps its own
    // tiling pass. Skipping a wasted within/center annotated flood avoids
    // paying the checked-flood cost for a discarded result.
    let membership = Arc::new(match cell_rule {
        CellRule::Overlap => {
            let annotated = h3_tile(
                &cover_shape,
                unsplit,
                resolution,
                CellRule::Overlap,
                max_cells,
            )
            .map_err(cover_budget_err)?;
            H3Membership::from_annotated(&annotated, resolution)
        },
        CellRule::Within | CellRule::Center | CellRule::Bbox => {
            h3_membership_for_shape(unsplit, &cover_shape, resolution, max_cells)
                .map_err(cover_budget_err)?
        },
    });
    let cells = match cell_rule {
        CellRule::Overlap => membership.partition.all(),
        CellRule::Within => membership.partition.interior(),
        // Center filter against the unsplit source with the geographic gate
        // (poles + antimeridian) so polar caps select the same set the public
        // covers() predicate would.
        CellRule::Center => membership.partition.select(|cell| {
            let center = LatLng::from(cell.cell);
            let probe = crate::geometry::ShapeData::from(Shape::Point(Point::new_unchecked_xy(
                center.lng(),
                center.lat(),
            )));
            topology_scalar_pair(&Predicate::Covers.spec(), unsplit, &probe, true)
        }),
        CellRule::Bbox => {
            let annotated = h3_tile(&cover_shape, unsplit, resolution, CellRule::Bbox, max_cells)
                .map_err(cover_budget_err)?;
            CoverageCells::from_cells(h3_cell_vec(
                annotated.into_iter().map(|cell| cell.cell).collect(),
            ))
        },
    };
    Ok(PyH3Coverage {
        geometry: membership_geometry,
        cells,
        cell_rule,
        depth: CellDepth::Uniform(resolution.into()),
        membership,
        max_cells,
    })
}

pub(super) fn h3_membership_for_shape(
    unsplit_source: &crate::geometry::ShapeData,
    cover_shape: &Shape,
    resolution: Resolution,
    max_cells: Option<usize>,
) -> Result<H3Membership, crate::grid::CoverBudgetExceeded> {
    Ok(H3Membership::from_annotated(
        &h3_tile(
            cover_shape,
            unsplit_source,
            resolution,
            CellRule::Overlap,
            max_cells,
        )?,
        resolution,
    ))
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
