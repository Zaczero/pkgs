#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::geometry::CoordSeq;
use crate::py::cells::coverage_ops::{
    DissolveEdge, GridDissolver, coordseq_crosses_lon_seam, dissolve_grid_cells,
};
use crate::py::cells::*;

pub(super) fn h3_dissolve(mut cells: Vec<CellIndex>) -> PyResult<Typed> {
    // A cell SET dissolves to the same outline regardless of order or repeats,
    // so canonicalize to a set — the user shouldn't have to pre-deduplicate
    // (matching the set semantics of the cell-algebra ops).
    cells.sort_unstable();
    cells.dedup();
    h3_dissolve_sorted(cells)
}

pub(super) fn h3_dissolve_sorted(cells: Vec<CellIndex>) -> PyResult<Typed> {
    dissolve_grid_cells::<H3Dissolver>(&cells)
}

struct H3Dissolver;

impl GridDissolver for H3Dissolver {
    type Cell = CellIndex;

    fn fast_path_cells(cells: &[Self::Cell]) -> PyResult<Option<Vec<Self::Cell>>> {
        let resolution = cells
            .iter()
            .map(|cell| cell.resolution())
            .max()
            .ok_or_else(|| GeometryError::new_err("to_polygon requires at least one cell"))?;
        // Mixed-resolution sets dissolve as their finest-resolution descendants:
        // an H3 cell's boundary does NOT geometrically equal the union of its
        // children's boundaries, so coarse cells expand before edge cancellation.
        if cells.iter().any(|cell| cell.resolution() != resolution) {
            let estimated = ensure_h3_uncompact_budget(cells.iter().copied(), resolution)?;
            let mut expanded = Vec::with_capacity(estimated);
            expanded.extend(CellIndex::uncompact(cells.to_vec(), resolution));
            expanded.sort_unstable();
            expanded.dedup();
            Ok(Some(expanded))
        } else {
            Ok(Some(cells.to_vec()))
        }
    }

    fn boundary_edges(cell: Self::Cell) -> Vec<DissolveEdge<Self::Cell>> {
        cell.edges()
            .map(|edge| {
                let points: Vec<Point> = edge
                    .boundary()
                    .iter()
                    .map(|latlng| Point::new_unchecked_xy(latlng.lng(), latlng.lat()))
                    .collect();
                DissolveEdge {
                    neighbor: edge.destination(),
                    segment: CoordSeq::from_points(&points),
                }
            })
            .collect()
    }

    fn crosses_seam(segment: &CoordSeq) -> bool {
        coordseq_crosses_lon_seam(segment)
    }

    fn fallback_shape(cell: Self::Cell) -> crate::error::Result<Shape> {
        let boundary = h3_cell_shape(cell);
        boundary.split_antimeridian()
    }
}
