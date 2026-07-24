#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;

// Token enums accepted directly as Python argument types: extraction parses
// the string and raises `ValueError` with the canonical message.
crate::tokens::token_from_pyobject!(
    BufferCapStyle,
    BufferJoinStyle,
    BufferSide,
    RepairMethod,
    SimplifyMethod,
    SmoothMethod,
);

crate::tokens::token_enum! {
    /// Algorithm used to triangulate geometry vertices or polygon boundaries.
    pub enum TriangulationMethod("triangulation method", param = "method") {
        Earcut = "earcut",
        Delaunay = "delaunay",
        Constrained = "constrained",
    }
}

crate::tokens::token_enum! {
    /// Space-filling curve used for geometry-centre keys and ordering.
    pub enum SpatialCurve("spatial curve", param = "curve") {
        Hilbert = "hilbert",
        Morton = "morton",
    }
}

crate::tokens::token_enum! {
    /// Coordinate basis for linear-referencing operations.
    pub enum LineReferenceBasis("line reference basis", param = "basis") {
        Distance = "distance",
        M = "m",
    }
}

crate::tokens::token_enum! {
    /// Route model used by point-navigation operations.
    pub(crate) enum NavigationPath("navigation path", token = none, param = "path") {
        Geodesic = "geodesic",
        Rhumb = "rhumb",
    }
}

crate::tokens::token_from_pyobject!(
    TriangulationMethod,
    SpatialCurve,
    LineReferenceBasis,
    NavigationPath,
);

/// Machine-readable token vocabularies for the stub-parity gate: every
/// [`token_enum!`](crate::tokens::token_enum) surface as `(enum_name, public_alias,
/// canonical_tokens)`, where the alias is the private `_types` `Literal` name
/// (`None` for surfaces typed as plain `str`). Read straight from the
/// generated `TOKENS` tables so the stub `Literal`s cannot drift from the
/// runtime parsers; `pyo3stubs parity` cross-checks this against
/// `gometry._types` and the `token_enum!` declarations in the source tree.
#[pyfunction]
pub(crate) fn _token_vocabulary() -> Vec<(&'static str, Option<&'static str>, Vec<&'static str>)> {
    vec![
        (
            "NavigationPath",
            Some("NavigationPath"),
            NavigationPath::TOKENS.to_vec(),
        ),
        (
            "CoverageOverlapRule",
            Some("CoverageOverlapRule"),
            crate::geometry::CoverageOverlapRule::TOKENS.to_vec(),
        ),
        (
            "DistanceUnit",
            Some("DistanceUnit"),
            DistanceUnit::TOKENS.to_vec(),
        ),
        (
            "ArrowEncoding",
            Some("ArrowEncoding"),
            crate::py::arrow::ArrowEncoding::TOKENS.to_vec(),
        ),
        (
            "BufferCapStyle",
            Some("CapStyle"),
            BufferCapStyle::TOKENS.to_vec(),
        ),
        (
            "BufferJoinStyle",
            Some("JoinStyle"),
            BufferJoinStyle::TOKENS.to_vec(),
        ),
        (
            "BufferSide",
            Some("BufferSide"),
            BufferSide::TOKENS.to_vec(),
        ),
        (
            "RepairMethod",
            Some("RepairMethod"),
            RepairMethod::TOKENS.to_vec(),
        ),
        (
            "SimplifyMethod",
            Some("SimplifyMethod"),
            SimplifyMethod::TOKENS.to_vec(),
        ),
        (
            "SmoothMethod",
            Some("SmoothMethod"),
            SmoothMethod::TOKENS.to_vec(),
        ),
        (
            "TriangulationMethod",
            Some("TriangulationMethod"),
            TriangulationMethod::TOKENS.to_vec(),
        ),
        (
            "SpatialCurve",
            Some("SpatialCurve"),
            SpatialCurve::TOKENS.to_vec(),
        ),
        (
            "LineReferenceBasis",
            Some("LineReferenceBasis"),
            LineReferenceBasis::TOKENS.to_vec(),
        ),
        (
            "CellRule",
            Some("CellRule"),
            crate::py::cells::CellRule::TOKENS.to_vec(),
        ),
        (
            "CrsComparison",
            Some("CrsComparison"),
            crate::crs::CrsComparison::TOKENS.to_vec(),
        ),
        (
            "ProjDirection",
            Some("TransformDirection"),
            crate::crs::ProjDirection::TOKENS.to_vec(),
        ),
        (
            "WktVersion",
            Some("WktVersion"),
            crate::crs::WktVersion::TOKENS.to_vec(),
        ),
        (
            "WktAxisRule",
            Some("WktAxisRule"),
            crate::crs::WktAxisRule::TOKENS.to_vec(),
        ),
    ]
}
