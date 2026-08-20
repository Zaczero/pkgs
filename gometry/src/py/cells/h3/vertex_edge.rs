use pyo3::types::{PyAny, PyAnyMethods as _};
use pyo3::{Bound, Py, PyResult, Python, pyfunction};

use crate::geometry::LineSeq;
use crate::py::cells::h3::{
    DirectedEdgeIndex, LatLng, PyH3Cell, PyH3Edge, PyH3Vertex, Typed, VertexIndex, parse_h3_index,
    validate_h3_index_id,
};
use crate::{CoordSeq, Point, PyGeometry, Shape};

frozen_pymethods! {
impl PyH3Vertex {
    /// One H3 vertex from an existing `H3Vertex`, a 64-bit id, or a token.
    ///
    /// Parameters
    /// ----------
    /// value : H3Vertex, int, or str
    ///     The vertex, its 64-bit id, or its token.
    ///
    /// Returns
    /// -------
    /// H3Vertex
    ///
    /// Raises
    /// ------
    /// ParseError
    ///     If ``value`` is not a valid H3 vertex id or token.
    /// TypeError
    ///     If ``value`` is not an `H3Vertex`, int, or str.
    #[new]
    fn new(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            vertex: h3_vertex_index(value)?,
        })
    }

    /// The vertex's 64-bit H3 index.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    fn id(&self) -> u64 {
        self.vertex.into()
    }

    /// Raw vertex id payload in bytes.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    const fn nbytes(&self) -> usize {
        std::mem::size_of::<VertexIndex>()
    }

    /// ``sys.getsizeof`` support: H3 vertices are heap-free value ids.
    const fn __sizeof__(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    /// The vertex's hexadecimal token.
    ///
    /// Returns
    /// -------
    /// str
    #[getter]
    fn token(&self) -> String {
        self.vertex.to_string()
    }

    /// The vertex's location.
    ///
    /// Returns
    /// -------
    /// Point
    ///     Longitude/latitude point tagged ``OGC:CRS84``.
    #[getter]
    fn point(&self) -> Typed {
        let latlng = LatLng::from(self.vertex);
        PyGeometry::typed_wgs84(Shape::Point(Point::new_unchecked_xy(
            latlng.lng(),
            latlng.lat(),
        )))
    }

    fn __int__(&self) -> u64 {
        self.vertex.into()
    }

    fn __index__(&self) -> u64 {
        self.vertex.into()
    }

    fn __hash__(&self) -> u64 {
        self.vertex.into()
    }

    fn __richcmp__(&self, other: &Self, op: pyo3::basic::CompareOp) -> bool {
        op.matches(u64::from(self.vertex).cmp(&u64::from(other.vertex)))
    }

    fn __repr__(&self) -> String {
        format!("<H3Vertex {}>", self.vertex)
    }

    /// The vertex's token — ``print(vertex)`` reads as data.
    fn __str__(&self) -> String {
        self.vertex.to_string()
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, (u64,))> {
        Ok((
            crate::gometry_lib_module(py)?
                .getattr(pyo3::intern!(py, "_unpickle_h3_vertex"))?
                .unbind(),
            (self.vertex.into(),),
        ))
    }

    /// ``case H3Vertex(id)`` destructures the 64-bit vertex index.
    #[classattr]
    const fn __match_args__() -> (&'static str,) {
        ("id",)
    }

}
}

/// Rebuild a pickled `H3Vertex` from its 64-bit index (internal; see
/// ``H3Vertex.__reduce__``).
#[pyfunction]
pub(super) fn _unpickle_h3_vertex(id: u64) -> PyResult<PyH3Vertex> {
    Ok(PyH3Vertex {
        vertex: validate_h3_index_id::<VertexIndex>(id)?,
    })
}

frozen_pymethods! {
impl PyH3Edge {
    /// One H3 directed edge from an existing `H3Edge`, a 64-bit id, or a token.
    ///
    /// Parameters
    /// ----------
    /// value : H3Edge, int, or str
    ///     The edge, its 64-bit id, or its token.
    ///
    /// Returns
    /// -------
    /// H3Edge
    ///
    /// Raises
    /// ------
    /// ParseError
    ///     If ``value`` is not a valid H3 directed-edge id or token.
    /// TypeError
    ///     If ``value`` is not an `H3Edge`, int, or str.
    #[new]
    fn new(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            edge: h3_edge_index(value)?,
        })
    }

    /// The edge's 64-bit H3 index.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    fn id(&self) -> u64 {
        self.edge.into()
    }

    /// Raw directed-edge id payload in bytes.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    const fn nbytes(&self) -> usize {
        std::mem::size_of::<DirectedEdgeIndex>()
    }

    /// ``sys.getsizeof`` support: H3 edges are heap-free value ids.
    const fn __sizeof__(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    /// The edge's hexadecimal token.
    ///
    /// Returns
    /// -------
    /// str
    #[getter]
    fn token(&self) -> String {
        self.edge.to_string()
    }

    /// The cell this directed edge leaves.
    ///
    /// Returns
    /// -------
    /// H3Cell
    #[getter]
    fn origin(&self) -> PyH3Cell {
        PyH3Cell {
            cell: self.edge.origin(),
        }
    }

    /// The cell this directed edge enters.
    ///
    /// Returns
    /// -------
    /// H3Cell
    #[getter]
    fn destination(&self) -> PyH3Cell {
        PyH3Cell {
            cell: self.edge.destination(),
        }
    }

    /// Reverse this directed edge from ``destination`` back to ``origin``.
    ///
    /// Returns
    /// -------
    /// H3Edge
        ///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
/// >>> assert cell is not None
/// >>> neighbor = list(cell.neighbors)[0]
/// >>> assert neighbor is not None
/// >>> edge = cell.edge(neighbor)
/// >>> edge.reverse().token
    /// '137283082cffffff'
fn reverse(&self) -> Self {
        Self {
            edge: self.edge.reverse(),
        }
    }

    /// The edge's linework along the shared cell boundary.
    ///
    /// Returns
    /// -------
    /// LineString
    ///     Longitude/latitude line tagged ``OGC:CRS84``.
    #[getter]
    fn line(&self) -> Typed {
        let points: Vec<Point> = self
            .edge
            .boundary()
            .iter()
            .map(|latlng| Point::new_unchecked_xy(latlng.lng(), latlng.lat()))
            .collect();
        PyGeometry::typed_wgs84(Shape::LineString(
            LineSeq::try_new(CoordSeq::from(points)).expect("H3 edge boundary is lineal"),
        ))
    }

    /// Length of the edge in meters (spherical, like `H3Cell.area`).
    ///
    /// Returns
    /// -------
    /// float
    ///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> cell = gm.H3Cell(13.4, 52.5, resolution=7)
/// >>> neighbor = cell.neighbors[0]
/// >>> assert neighbor is not None
/// >>> edge = cell.edge(neighbor)
/// >>> 1000 < edge.length < 3000
    /// True
    #[getter]
    fn length(&self) -> f64 {
        self.edge.length_m()
    }

    fn __int__(&self) -> u64 {
        self.edge.into()
    }

    fn __index__(&self) -> u64 {
        self.edge.into()
    }

    fn __hash__(&self) -> u64 {
        self.edge.into()
    }

    fn __richcmp__(&self, other: &Self, op: pyo3::basic::CompareOp) -> bool {
        op.matches(u64::from(self.edge).cmp(&u64::from(other.edge)))
    }

    fn __repr__(&self) -> String {
        format!("<H3Edge {}>", self.edge)
    }

    /// The edge's token — ``print(edge)`` reads as data.
    fn __str__(&self) -> String {
        self.edge.to_string()
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, (u64,))> {
        Ok((
            crate::gometry_lib_module(py)?
                .getattr(pyo3::intern!(py, "_unpickle_h3_edge"))?
                .unbind(),
            (self.edge.into(),),
        ))
    }

    /// ``case H3Edge(id)`` destructures the 64-bit edge index.
    #[classattr]
    const fn __match_args__() -> (&'static str,) {
        ("id",)
    }

}
}

/// Parse a vertex from an existing `H3Vertex`, a 64-bit id, or a token.
pub(super) fn h3_vertex_index(value: &Bound<'_, PyAny>) -> PyResult<VertexIndex> {
    parse_h3_index(value, |value| {
        value
            .cast_exact::<PyH3Vertex>()
            .ok()
            .map(|vertex| vertex.get().vertex)
    })
}

/// Parse a directed edge from an existing `H3Edge`, a 64-bit id, or a token.
pub(super) fn h3_edge_index(value: &Bound<'_, PyAny>) -> PyResult<DirectedEdgeIndex> {
    parse_h3_index(value, |value| {
        value
            .cast_exact::<PyH3Edge>()
            .ok()
            .map(|edge| edge.get().edge)
    })
}

/// Rebuild a pickled `H3Edge` from its 64-bit index (internal; see
/// ``H3Edge.__reduce__``).
#[pyfunction]
pub(super) fn _unpickle_h3_edge(id: u64) -> PyResult<PyH3Edge> {
    Ok(PyH3Edge {
        edge: validate_h3_index_id::<DirectedEdgeIndex>(id)?,
    })
}
