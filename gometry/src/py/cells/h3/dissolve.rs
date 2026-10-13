use crate::geometry::CoordSeq;
use crate::grid::cell_set;
use crate::py::cells::coverage_ops::{GridDissolver, SortedCells, dissolve_grid_cells};
use crate::py::cells::grid_outline::coordseq_crosses_lon_seam;
use crate::py::cells::h3::{CellIndex, h3_cell_shape};
use crate::py::cells::{GeometryError, Point, PyResult, Shape, Typed};

pub(super) fn h3_dissolve(cells: Vec<CellIndex>) -> PyResult<Typed> {
    // A cell SET dissolves to the same outline regardless of order or repeats,
    // so canonicalize to a set — the user shouldn't have to pre-deduplicate
    // (matching the set semantics of the cell-algebra ops).
    dissolve_grid_cells::<H3Dissolver>(SortedCells::new(cells))
}

struct H3Dissolver;

impl GridDissolver for H3Dissolver {
    type Cell = CellIndex;

    fn fast_path_cells(
        cells: &SortedCells<Self::Cell>,
    ) -> PyResult<Option<SortedCells<Self::Cell>>> {
        let resolution = cells
            .as_slice()
            .iter()
            .map(|cell| cell.resolution())
            .max()
            .ok_or_else(|| GeometryError::new_err("to_polygon requires at least one cell"))?;
        // Mixed-resolution sets dissolve as their finest-resolution descendants:
        // an H3 cell's boundary does NOT geometrically equal the union of its
        // children's boundaries, so coarse cells expand before edge cancellation.
        if cells
            .as_slice()
            .iter()
            .any(|cell| cell.resolution() != resolution)
        {
            let expanded = cell_set::uncompact(cells.as_slice(), resolution.into())?;
            Ok(Some(SortedCells::new(expanded.into_public_vec())))
        } else {
            Ok(Some(cells.clone()))
        }
    }

    fn adjacency_neighbors(cell: Self::Cell) -> impl Iterator<Item = Self::Cell> {
        cell.edges().map(h3o::DirectedEdgeIndex::destination)
    }

    /// Exterior directed-edge segments as lon/lat polylines.
    ///
    /// These are **planar chord proxies** of H3's spherical edge geometry —
    /// exact region algebra should use cell set ops, not dissolved polygons.
    fn exterior_edge_segments(
        cell: Self::Cell,
        is_member: &dyn Fn(Self::Cell) -> bool,
    ) -> PyResult<Vec<CoordSeq>> {
        let mut segments = Vec::with_capacity(6);
        for edge in cell.edges() {
            if is_member(edge.destination()) {
                continue;
            }
            let boundary = edge.boundary();
            let points: Vec<_> = boundary
                .iter()
                .map(|latlng| Point::new_raw_xy(latlng.lng(), latlng.lat()))
                .collect();
            segments.push(CoordSeq::try_from_points(&points).map_err(pyo3::PyErr::from)?);
        }
        Ok(segments)
    }

    fn crosses_seam(segment: &CoordSeq) -> bool {
        coordseq_crosses_lon_seam(segment)
    }

    fn fallback_shape(cell: Self::Cell) -> crate::error::Result<Shape> {
        let boundary = h3_cell_shape(cell)?;
        boundary.split_antimeridian()
    }
}
