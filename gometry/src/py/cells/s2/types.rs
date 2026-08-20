#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::grid::s2::cellid::CellId;
use crate::py::cells::pyclass;

/// One S2 cell: a level-addressed quadrilateral tile on the sphere.
///
/// Wraps the 64-bit cell id with typed accessors (``cell.level``,
/// ``cell.token``, ``cell.polygon``, ``cell.center``) and hierarchy
/// moves (``parent``/``children``/``neighbors``). Convert via
/// ``S2Cell(...)``, and back with ``int(cell)``.
#[pyclass(
    name = "S2Cell",
    module = "gometry",
    frozen,
    immutable_type,
    skip_from_py_object
)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct PyS2Cell {
    pub(crate) cell: CellId,
}

crate::heapless!(CellId, PyS2Cell);
