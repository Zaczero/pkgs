//! The Tile cell class and parsing helpers.
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use pyo3::exceptions::PyTypeError;
use pyo3::pymethods;
use pyo3::types::{PyAny, PyBool, PyDict, PyInt, PyTuple};

use crate::Typed;
use crate::curves::morton_interleave;
use crate::grid::cell::GridCell;
use crate::grid::tile::{TILE_MAX_ZOOM, Tile};
use crate::py::cells::cell_ops::{
    cell_boundary, cell_center, cell_children_array, cell_contains, cell_descendant_count,
    cell_hash, cell_intersects, cell_neighbors_array, cell_parent, cell_reduce, cell_richcmp,
};
use crate::py::cells::{
    Bound, GridKind, Py, PyAnyMethods as _, PyCellArray, PyDictMethods as _, PyResult,
    PyTupleMethods as _, Python, construct_tile, pyclass, pyfunction,
};
use crate::py::errors::{parse_error, tag_parse_format};

/// One XYZ web-mercator tile: the slippy-map ``z/x/y`` address.
///
/// Wraps the tile with typed accessors (``tile.zoom``/``x``/``y``,
/// ``tile.token``, ``tile.polygon``, ``tile.center``) and hierarchy
/// moves (``parent``/``children``/``neighbors``). The token is the Bing
/// quadkey (empty at ``z0``); the 64-bit id packs ``(zoom, Morton x/y)``
/// so sorted ids group spatial neighbors. Convert via ``Tile(value)``,
/// ``Tile(Point(...), zoom=...)``, ``Tile(lon=..., lat=..., zoom=...)``, or
/// ``Tile(x=..., y=..., zoom=...)``. Coordinate frames are always named.
#[pyclass(
    name = "Tile",
    module = "gometry",
    frozen,
    immutable_type,
    skip_from_py_object
)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct PyTile {
    pub(crate) cell: Tile,
}

crate::heapless!(PyTile);
/// Rebuild a pickled Tile from its packed id (internal; see
/// ``Tile.__reduce__``).
#[pyfunction]
pub(super) fn _unpickle_tile(id: u64) -> PyResult<PyTile> {
    Tile::from_id(id)
        .map(|cell| PyTile { cell })
        .ok_or_else(|| {
            parse_error(
                format!("invalid tile id {id}"),
                crate::error::ParseFormat::Tile,
            )
        })
}

/// Parse a tile from an existing Tile, a packed 64-bit id, or a quadkey.
pub(crate) fn tile_arg(cell: &Bound<'_, PyAny>) -> PyResult<Tile> {
    if let Ok(cell) = cell.cast_exact::<PyTile>() {
        return Ok(cell.get().cell);
    }
    if cell.cast_exact::<PyBool>().is_err() && cell.cast::<PyInt>().is_ok() {
        let id = cell.extract::<u64>().map_err(|_| {
            parse_error(
                "tile id must be a non-negative 64-bit integer",
                crate::error::ParseFormat::Tile,
            )
        })?;
        return Tile::from_id(id).ok_or_else(|| {
            parse_error(
                format!("invalid tile id {id}"),
                crate::error::ParseFormat::Tile,
            )
        });
    }
    let text = crate::py_text_borrow(cell, "tile must be a Tile, integer id, or string quadkey")?;
    Tile::from_quadkey(text.as_ref()).map_err(|message| {
        tag_parse_format(
            crate::py::errors::ParseError::new_err(message),
            crate::error::ParseFormat::Quadkey,
        )
    })
}

/// Shared i64 → zoom conversion (`0..=29`).
pub(crate) fn parse_tile_zoom_value(value: i64) -> PyResult<u8> {
    super::super::checked_depth(value, "tile zoom", "zoom", 0, i64::from(TILE_MAX_ZOOM))
}

/// Boundary parser for tile zoom: `0..=29` (the packed-id limit).
pub(crate) fn parse_tile_zoom(value: &Bound<'_, PyAny>) -> PyResult<u8> {
    parse_tile_zoom_value(crate::py_i64_required("zoom", value)?)
}

pub(super) fn tile_floor(min_zoom: i64) -> PyResult<u8> {
    super::super::checked_depth(
        min_zoom,
        "tile min_zoom",
        "min_zoom",
        0,
        i64::from(TILE_MAX_ZOOM),
    )
}

#[pymethods]
impl PyTile {
    /// One XYZ tile from a packed id, quadkey, lon/lat keywords, point geometry,
    /// or explicit ``x=``/``y=`` tile coordinates.
    ///
    /// Parameters
    /// ----------
    /// value : Tile, int, str, or Point, optional
    ///     A tile id/quadkey, or a point geometry when paired with ``zoom``.
    ///
    /// lon, lat : float, optional
    ///     Geographic coordinates, supplied together with ``zoom``. They are
    ///     keyword-only because two bare numbers do not select a coordinate
    ///     frame.
    ///
    /// zoom : int, optional
    ///     Zoom level (``0``-``29``); keyword-only, required for every
    ///     coordinate form.
    ///
    /// x, y : int, optional
    ///     Explicit tile column/row (keyword-only, with ``zoom``) — never
    ///     inferred from a positional pair, so lon/lat can't be misread as
    ///     tile coordinates.
    ///
    /// Returns
    /// -------
    /// Tile
    ///
    /// Raises
    /// ------
    /// ParseError
    ///     If ``value`` is not a valid tile id or quadkey.
    /// GeometryError
    ///     If ``zoom`` is out of range, or ``x``/``y`` is outside
    ///     ``[0, 2**zoom)``.
    /// InvalidGeometryError
    ///     If a scalar coordinate is non-finite or out of range.
    #[new]
    #[pyo3(
        signature = (*args, **kwargs),
        text_signature = "(value=None, /, *, lon=None, lat=None, zoom=None, x=None, y=None)"
    )]
    fn new(args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        if args.len() > 1 {
            return Err(PyTypeError::new_err(
                "two positional Tile coordinates are ambiguous; use lon=..., lat=..., zoom=... or x=..., y=..., zoom=...",
            ));
        }
        let value = args.get_item(0).ok();
        let mut lon = None;
        let mut lat = None;
        let mut zoom = None;
        let mut x = None;
        let mut y = None;
        if let Some(kwargs) = kwargs {
            for (key, argument) in kwargs.iter() {
                let key = key.extract::<String>()?;
                match key.as_str() {
                    "lon" => lon = Some(argument),
                    "lat" => lat = Some(argument),
                    "zoom" => zoom = Some(argument),
                    "x" => x = Some(argument),
                    "y" => y = Some(argument),
                    _ => {
                        return Err(PyTypeError::new_err(format!(
                            "Tile got an unexpected keyword argument {key:?}"
                        )));
                    },
                }
            }
        }
        construct_tile(
            value.as_ref(),
            lon.as_ref(),
            lat.as_ref(),
            zoom.as_ref(),
            x.as_ref(),
            y.as_ref(),
        )
    }

    /// Zoom level of this tile (``0``-``29``).
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    fn zoom(&self) -> u8 {
        self.cell.depth()
    }

    /// The packed 64-bit tile id: zoom in the top bits, Morton-interleaved
    /// ``x``/``y`` below — sorted ids group spatial neighbors.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    fn id(&self) -> u64 {
        self.cell.id()
    }

    /// The Bing quadkey (one base-4 digit per zoom; empty at ``z0``).
    ///
    /// Returns
    /// -------
    /// str
    #[getter]
    fn token(&self) -> String {
        self.cell.token()
    }

    /// Tile column (west to east).
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    const fn x(&self) -> u32 {
        self.cell.x
    }

    /// Tile row (north to south).
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    const fn y(&self) -> u32 {
        self.cell.y
    }

    /// The Morton (Z-order) index of ``x``/``y`` within this zoom.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    fn morton(&self) -> u64 {
        morton_interleave(self.cell.x, self.cell.y)
    }
}

grid_cell_common_pymethods! {
    impl PyTile {
        kind: GridKind::Tile,
        class_name: "Tile",
        depth: zoom,
        depth_name: "zoom",
        parse_depth: parse_tile_zoom,
        parse_cell: tile_arg,
        unpickle: "_unpickle_tile",
        nbytes: std::mem::size_of::<u64>(),
        parent_text_signature: "($self, zoom=None)",
        children_text_signature: "($self, zoom=None)",
        neighbors_doc: "The surrounding tiles at this zoom (8, fewer at the edges), row-major from the north-west; east-west wraps the antimeridian.",
        candidate_doc: "other : Tile, int, or str",
        example_parent: r"
>>> import gometry as gm
>>> cell = gm.tile_cover(gm.Point(-122.4194, 37.7749, crs=4326), zoom=10).cells[0]
>>> str(cell.parent())
'023010203'
",
        example_children: r"
>>> import gometry as gm
>>> cell = gm.tile_cover(gm.Point(-122.4194, 37.7749, crs=4326), zoom=10).cells[0]
>>> len(cell.children())
4
",
        example_children_count: r"
>>> import gometry as gm
>>> cell = gm.tile_cover(gm.Point(-122.4194, 37.7749, crs=4326), zoom=10).cells[0]
>>> cell.children_count()
4
",
        example_contains: r"
>>> import gometry as gm
>>> cell = gm.tile_cover(gm.Point(-122.4194, 37.7749, crs=4326), zoom=10).cells[0]
>>> cell.contains(cell.children()[0])
True
",
        example_intersects: r"
>>> import gometry as gm
>>> cell = gm.tile_cover(gm.Point(-122.4194, 37.7749, crs=4326), zoom=10).cells[0]
>>> cell.intersects(cell.parent())
True
",
        repr: tile,
        cell_int: |tile| tile.cell.id(),
    }
}
