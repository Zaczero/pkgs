use h3o::CoordIJ;
use pyo3::prelude::*;

use super::*;
use crate::grid::cell::GridCell;
use crate::py::cells::cell_ops::{
    cell_boundary, cell_center, cell_children_array, cell_contains, cell_descendant_count,
    cell_hash, cell_intersects, cell_neighbors_array, cell_parent, cell_reduce, cell_richcmp,
};
use crate::py::cells::{GridKind, PyCellArray, construct_h3_cell};
use crate::py::errors::GeometryError;
use crate::{Typed, py_i64_required};

#[pymethods]
impl PyH3Cell {
    /// One H3 cell from an id, token, lon/lat pair, or point geometry.
    ///
    /// Parameters
    /// ----------
    /// lon : H3Cell, int, str, float, or Point
    ///     A cell id/token, the longitude of a ``lon, lat`` pair, or a point
    ///     geometry.
    ///
    /// lat : float, optional
    ///     Latitude when ``lon`` is a scalar longitude.
    ///
    /// resolution : int, optional
    ///     H3 resolution (``0``-``15``); required for coordinate construction.
    ///
    /// Returns
    /// -------
    /// H3Cell
    ///
    /// Raises
    /// ------
    /// ParseError
    ///     If ``value`` is not a valid H3 cell id or token.
    /// GeometryError
    ///     If ``resolution`` is out of range.
    /// InvalidGeometryError
    ///     If a scalar coordinate is non-finite.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.H3Cell(13.4, 52.5, resolution=7).resolution
    /// 7
    #[new]
    #[pyo3(
        signature = (value, /, lat = None, *, resolution = None),
        text_signature = "(value, /, lat=None, *, resolution=None)"
    )]
    fn new(
        value: &Bound<'_, PyAny>,
        lat: Option<&Bound<'_, PyAny>>,
        resolution: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        construct_h3_cell(value, lat, resolution)
    }

    /// H3 resolution of this cell (``0``-``15``).
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    fn resolution(&self) -> u8 {
        self.cell.depth()
    }

    /// The 64-bit H3 cell id.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    fn id(&self) -> u64 {
        u64::from(self.cell)
    }

    /// The 15-character lowercase hex token of the cell id.
    ///
    /// Returns
    /// -------
    /// str
    #[getter]
    fn token(&self) -> String {
        self.cell.token()
    }

    /// Whether this cell is one of the 12 pentagons at its resolution.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_pentagon(&self) -> bool {
        self.cell.is_pentagon()
    }

    /// Return the child whose center coincides with this cell's center.
    ///
    /// Parameters
    /// ----------
    /// resolution : int
    ///     Target resolution (``0``-``15``); must not be coarser than this
    ///     cell's.
    ///
    /// Returns
    /// -------
    /// H3Cell
    ///     The center child at ``resolution``.
    ///
    /// Raises
    /// ------
    /// GeometryError
    ///     If ``resolution`` is out of range or coarser than the cell's.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
    /// >>> cell.center_child(resolution=8).token
    /// '8828308281fffff'
    fn center_child(&self, resolution: &Bound<'_, PyAny>) -> PyResult<Self> {
        let resolution = parse_h3_resolution(resolution)?;
        self.cell
            .center_child(resolution)
            .map(|cell| Self { cell })
            .ok_or_else(|| GeometryError::new_err("children resolution must be >= cell resolution"))
    }

    /// Return the child at ``position`` in this cell's ordered descendants.
    ///
    /// The inverse of ``child_position``:
    /// ``cell.child_at(position, resolution)`` recovers the cell that
    /// reported ``position`` at this cell's resolution.
    ///
    /// Parameters
    /// ----------
    /// position : int
    ///     Zero-based position among this cell's descendants at
    ///     ``resolution`` (``0`` to ``children_count(resolution) - 1``).
    /// resolution : int
    ///     Target resolution (``0``-``15``); must not be coarser than this
    ///     cell's.
    ///
    /// Returns
    /// -------
    /// H3Cell
    ///     The descendant cell at ``position``.
    ///
    /// Raises
    /// ------
    /// GeometryError
    ///     If ``resolution`` is out of range or coarser than the cell's.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
    /// >>> cell.child_at(0, 8).token
    /// '8828308281fffff'
    fn child_at(
        &self,
        position: &Bound<'_, PyAny>,
        resolution: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let position = py_i64_required("child position", position)?;
        let resolution = parse_h3_resolution(resolution)?;
        if resolution < self.cell.resolution() {
            return Err(GeometryError::new_err(
                "children resolution must be >= cell resolution",
            ));
        }
        u64::try_from(position)
            .ok()
            .and_then(|position| self.cell.child_at(position, resolution))
            .map(|cell| Self { cell })
            .ok_or_else(|| {
                GeometryError::new_err(format!(
                    "child position must be between 0 and children_count - 1, got {position}"
                ))
            })
    }

    /// The cell's topological vertices, with canonical shared identity.
    ///
    /// Adjacent cells return the *same* vertex objects for their shared
    /// corners (equal ids), so vertices deduplicate across a coverage —
    /// unlike `polygon`, which yields per-cell coordinate copies.
    ///
    /// Returns
    /// -------
    /// H3VertexArray
    ///     Five vertices for a pentagon, six for a hexagon.
    #[getter]
    fn vertices(&self) -> PyH3VertexArray {
        let ids: Vec<u64> = self.cell.vertexes().map(u64::from).collect();
        PyH3VertexArray::from_trusted_ids(ids)
    }

    /// Return the directed edge from this cell to a neighboring cell.
    ///
    /// Parameters
    /// ----------
    /// destination : H3Cell, int, or str
    ///     The neighboring cell the edge points into.
    ///
    /// Returns
    /// -------
    /// H3Edge
    ///
    /// Raises
    /// ------
    /// GeometryError
    ///     If ``destination`` is not a neighbor of this cell.
    /// ParseError
    ///     If ``destination`` is not a valid H3 cell.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> cell = gm.H3Cell(13.4, 52.5, resolution=7)
    /// >>> edge = cell.edge(cell.neighbors[0])
    /// >>> (edge.origin == cell, edge.destination == cell.neighbors[0])
    /// (True, True)
    fn edge(&self, destination: &Bound<'_, PyAny>) -> PyResult<PyH3Edge> {
        let destination = h3_cell_index(destination)?;
        self.cell
            .edge(destination)
            .map(|edge| PyH3Edge { edge })
            .ok_or_else(|| {
                GeometryError::new_err(format!(
                    "destination {destination} is not a neighbor of cell {}",
                    self.cell
                ))
            })
    }

    /// The directed edges leaving this cell (6, or 5 on a pentagon).
    ///
    /// Returns
    /// -------
    /// H3EdgeArray
    #[getter]
    fn edges(&self) -> PyH3EdgeArray {
        let ids: Vec<u64> = self.cell.edges().map(u64::from).collect();
        PyH3EdgeArray::from_trusted_ids(ids)
    }

    /// Return cells within k grid steps (filled disk).
    ///
    /// Parameters
    /// ----------
    /// k : int
    ///     Grid radius in steps (``>= 0``); ``k=0`` is this cell alone.
    ///
    /// Returns
    /// -------
    /// CellArray of H3Cell
    ///
    /// Raises
    /// ------
    /// GeometryError
    ///     If ``k`` is negative or too large.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
    /// >>> len(cell.grid_disk(1))
    /// 7
    fn grid_disk(&self, k: &Bound<'_, PyAny>) -> PyResult<PyCellArray> {
        let k = parse_h3_grid_k(k)?;
        let ids: Vec<u64> = try_h3_grid_disk_cells(self.cell, k)
            .map_err(super::super::cell_limit_err)?
            .into_iter()
            .map(u64::from)
            .collect();
        Ok(PyCellArray::from_trusted_ids(GridKind::H3Cell, ids))
    }

    /// Return cells exactly ``k`` grid steps away (hollow ring).
    ///
    /// Parameters
    /// ----------
    /// k : int
    ///     Grid radius in steps (``>= 0``); ``k=0`` is this cell alone.
    ///
    /// Returns
    /// -------
    /// CellArray of H3Cell
    ///
    /// Raises
    /// ------
    /// GeometryError
    ///     If ``k`` is negative or too large.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
    /// >>> len(cell.grid_ring(1))
    /// 6
    fn grid_ring(&self, k: &Bound<'_, PyAny>) -> PyResult<PyCellArray> {
        let k = parse_h3_grid_k(k)?;
        let ids: Vec<u64> = try_h3_grid_ring_cells(self.cell, k)
            .map_err(super::super::cell_limit_err)?
            .into_iter()
            .map(u64::from)
            .collect();
        Ok(PyCellArray::from_trusted_ids(GridKind::H3Cell, ids))
    }

    /// Test whether ``other`` is an edge-adjacent neighbor of this cell.
    ///
    /// Parameters
    /// ----------
    /// other : H3Cell, int, or str
    ///     The candidate neighbor; must share this cell's resolution.
    ///
    /// Returns
    /// -------
    /// bool
    ///
    /// Raises
    /// ------
    /// GeometryError
    ///     If the cells cannot be compared (different resolutions).
    /// ParseError
    ///     If ``other`` is not a valid H3 cell.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
    /// >>> cell.is_neighbor(list(cell.neighbors)[0])
    /// True
    fn is_neighbor(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        let other = h3_cell_index(other)?;
        self.cell
            .is_neighbor_with(other)
            .map_err(|error| GeometryError::new_err(error.to_string()))
    }

    /// Local ``(i, j)`` coordinates of this cell relative to ``origin``
    /// (the H3 local-IJ indexing space, for grid algebra around an anchor).
    ///
    /// Parameters
    /// ----------
    /// origin : H3Cell, int, or str
    ///     Anchor cell; must share this cell's resolution and be near it.
    ///
    /// Returns
    /// -------
    /// tuple of int
    ///     The ``(i, j)`` coordinates.
    ///
    /// Raises
    /// ------
    /// GeometryError
    ///     If the cells are too far apart for local IJ.
    /// ParseError
    ///     If ``origin`` is not a valid H3 cell.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
    /// >>> cell.local_ij(cell)
    /// (160, 88)
    fn local_ij(&self, origin: &Bound<'_, PyAny>) -> PyResult<(i32, i32)> {
        let origin = h3_cell_index(origin)?;
        let local = self
            .cell
            .to_local_ij(origin)
            .map_err(|error| GeometryError::new_err(error.to_string()))?;
        Ok((local.coord.i, local.coord.j))
    }

    /// Return the cell at local ``(i, j)`` coordinates relative to this origin —
    /// the inverse of `local_ij`.
    ///
    /// Parameters
    /// ----------
    /// i, j : int
    ///     Local IJ coordinates.
    ///
    /// Returns
    /// -------
    /// H3Cell
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
    /// >>> cell.cell_from_local_ij(160, 88).token
    /// '872830828ffffff'
    fn cell_from_local_ij(&self, i: i32, j: i32) -> PyResult<Self> {
        let cell = CellIndex::try_from(LocalIJ::new(self.cell, CoordIJ::new(i, j)))
            .map_err(|error| GeometryError::new_err(error.to_string()))?;
        Ok(Self { cell })
    }

    /// The base (resolution-0) cell number this cell descends from
    /// (0-121).
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    fn base_cell(&self) -> u8 {
        self.cell.base_cell().into()
    }

    /// This cell's position among `parent(resolution)`'s descendants at
    /// this cell's resolution, or None when resolution is finer than
    /// the cell's own.
    ///
    /// Parameters
    /// ----------
    /// resolution : int
    ///     The ancestor resolution to count from.
    ///
    /// Returns
    /// -------
    /// int or None
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
    /// >>> cell.children(resolution=8)[0].child_position(7)
    /// 0
    fn child_position(&self, resolution: &Bound<'_, PyAny>) -> PyResult<Option<u64>> {
        let resolution = parse_h3_resolution(resolution)?;
        Ok(self.cell.child_position(resolution))
    }

    /// Grid-step distance to another cell.
    ///
    /// Parameters
    /// ----------
    /// other : H3Cell, int, or str
    ///     The target cell; must share this cell's resolution.
    ///
    /// Returns
    /// -------
    /// int
    ///
    /// Raises
    /// ------
    /// GeometryError
    ///     If the cells cannot be connected (different resolutions or too far apart).
    /// ParseError
    ///     If ``other`` is not a valid H3 cell.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
    /// >>> cell.grid_distance(list(cell.neighbors)[0])
    /// 1
    fn grid_distance(&self, other: &Bound<'_, PyAny>) -> PyResult<i32> {
        self.cell
            .grid_distance(h3_cell_index(other)?)
            .map_err(|error| GeometryError::new_err(error.to_string()))
    }

    /// Grid path of cells to another cell.
    ///
    /// Parameters
    /// ----------
    /// other : H3Cell, int, or str
    ///     The target cell; must share this cell's resolution.
    ///
    /// Returns
    /// -------
    /// CellArray of H3Cell
    ///
    /// Raises
    /// ------
    /// GeometryError
    ///     If the cells cannot be connected (different resolutions or too far apart).
    /// ParseError
    ///     If ``other`` is not a valid H3 cell.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
    /// >>> nbr = list(cell.neighbors)[0]
    /// >>> len(cell.grid_path(nbr))
    /// 2
    fn grid_path(&self, other: &Bound<'_, PyAny>) -> PyResult<PyCellArray> {
        let cells = self
            .cell
            .grid_path_cells(h3_cell_index(other)?)
            .map_err(|error| GeometryError::new_err(error.to_string()))?;
        let mut ids = crate::grid::CellCollector::new("H3 grid_path");
        for cell in cells {
            let cell = cell.map_err(|error| GeometryError::new_err(error.to_string()))?;
            ids.push(u64::from(cell))
                .map_err(super::super::cell_limit_err)?;
        }
        let ids = ids.into_vec();
        Ok(PyCellArray::from_trusted_ids(GridKind::H3Cell, ids))
    }
}

fn parse_h3_depth(value: &Bound<'_, PyAny>) -> PyResult<u8> {
    parse_h3_resolution(value).map(Into::into)
}

grid_cell_common_pymethods! {
    impl PyH3Cell {
        kind: GridKind::H3Cell,
        class_name: "H3Cell",
        depth: resolution,
        depth_name: "resolution",
        parse_depth: parse_h3_depth,
        parse_cell: h3_cell_index,
        unpickle: "_unpickle_h3_cell",
        nbytes: std::mem::size_of::<u64>(),
        parent_text_signature: "($self, resolution=None)",
        children_text_signature: "($self, resolution=None)",
        neighbors_doc: "The edge-adjacent neighbor cells (six for a hexagon, five around a pentagon).",
        candidate_doc: "other : H3Cell, int, or str",
        example_parent: r"
>>> import gometry as gm
>>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
>>> cell.parent(resolution=6).token
'86283082fffffff'
",
        example_children: r"
>>> import gometry as gm
>>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
>>> len(cell.children(resolution=8))
7
",
        example_children_count: r"
>>> import gometry as gm
>>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
>>> cell.children_count(resolution=8)
7
",
        example_contains: r"
>>> import gometry as gm
>>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
>>> cell.contains(cell.children(resolution=8)[0])
True
",
        example_intersects: r"
>>> import gometry as gm
>>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0]
>>> cell.intersects(cell.parent(resolution=6))
True
",
        repr: h3,
        cell_int: |cell| u64::from(cell.cell),
    }
}

pub(super) fn try_h3_grid_disk_cells(
    cell: CellIndex,
    k: u32,
) -> std::result::Result<Vec<CellIndex>, crate::grid::CellLimitExceeded> {
    let estimate = usize::try_from(h3o::max_grid_disk_size(k)).unwrap_or(usize::MAX);
    if estimate > crate::grid::GRID_MAX_CELLS {
        return Err(crate::grid::CellLimitExceeded::new("H3 grid_disk"));
    }
    let mut cells = crate::grid::CellCollector::with_estimate("H3 grid_disk", estimate);
    for value in cell.grid_disk_fast(k) {
        let Some(value) = value else {
            cells.clear();
            cells.extend(cell.grid_disk_safe(k))?;
            return Ok(cells.into_vec());
        };
        cells.push(value)?;
    }
    Ok(cells.into_vec())
}

pub(super) fn h3_grid_ring_cells(cell: CellIndex, k: u32) -> Vec<CellIndex> {
    try_h3_grid_ring_cells(cell, k).expect("one-ring H3 neighbor expansion fits the grid limit")
}

pub(super) fn try_h3_grid_ring_cells(
    cell: CellIndex,
    k: u32,
) -> std::result::Result<Vec<CellIndex>, crate::grid::CellLimitExceeded> {
    let estimate = usize::try_from(h3o::max_grid_ring_size(k)).unwrap_or(usize::MAX);
    if estimate > crate::grid::GRID_MAX_CELLS {
        return Err(crate::grid::CellLimitExceeded::new("H3 grid_ring"));
    }
    let mut cells = crate::grid::CellCollector::with_estimate("H3 grid_ring", estimate);
    for value in cell.grid_ring_fast(k) {
        let Some(value) = value else {
            cells.clear();
            cells.extend(
                cell.grid_disk_distances_safe(k)
                    .filter_map(|(cell, distance)| (distance == k).then_some(cell)),
            )?;
            return Ok(cells.into_vec());
        };
        cells.push(value)?;
    }
    Ok(cells.into_vec())
}
