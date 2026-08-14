//! The top-level `gometry` exception hierarchy and the single Rust→Python error
//! seam.
//!
//! Every class subclasses `ValueError` through `GeometryError` (the
//! `json.JSONDecodeError` precedent), so broad `except ValueError` handlers
//! keep working while precise handlers catch the specific class.
//! `GeometryTypeError` additionally subclasses `TypeError` (the
//! `numpy.exceptions.AxisError` dual-base pattern) — a geometry of the wrong
//! kind is both a gometry domain error and a Python type error.
//!
//! The [`From<Error>`] conversion is the one classification gate: it matches
//! every domain kind exhaustively with no wildcard arm, so adding an error
//! variant forces a deliberate exception-class decision. One-off boundary
//! failures with domain meaning but no structured variant raise the right
//! class directly (`GeometryError::new_err(...)`, `parse_error(...)`).

use pyo3::create_exception;
use pyo3::exceptions::{PyTypeError, PyUserWarning, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyType};

use crate::Crs;
use crate::error::{Error, ErrorKind};

create_exception!(
    gometry,
    GeometryError,
    PyValueError,
    "Base class for every error gometry raises about your data or parameters."
);
create_exception!(
    gometry,
    InvalidGeometryError,
    GeometryError,
    "A geometry violates a structural or numeric rule."
);
create_exception!(
    gometry,
    CRSError,
    GeometryError,
    "A CRS could not be created, identified, exported, or used."
);
create_exception!(
    gometry,
    CRSMismatchError,
    CRSError,
    "Operands carry incompatible CRS or coordinate-epoch metadata."
);
create_exception!(
    gometry,
    TransformError,
    CRSError,
    "A coordinate transform could not be built or failed to run."
);
create_exception!(
    gometry,
    ParseError,
    GeometryError,
    "Serialized, grid-cell, or point-code input is malformed."
);
create_exception!(
    gometry,
    AccuracyWarning,
    PyUserWarning,
    "A CRS transform used a lower-accuracy fallback because a required grid is unavailable."
);

/// `create_exception!` is single-base, so the dual-base class is built once at
/// module init with a plain `type(name, bases, dict)` call and cached here.
static GEOMETRY_TYPE_ERROR: PyOnceLock<Py<PyType>> = PyOnceLock::new();

/// The `GeometryTypeError` class: `(GeometryError, TypeError)` dual base.
pub(crate) fn geometry_type_error(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    GEOMETRY_TYPE_ERROR
        .get_or_try_init(py, || {
            let bases = (py.get_type::<GeometryError>(), py.get_type::<PyTypeError>());
            let dict = PyDict::new(py);
            dict.set_item(
                "__doc__",
                "An operation received a geometry of the wrong kind.",
            )?;
            dict.set_item("__module__", "gometry")?;
            let class = py
                .get_type::<PyType>()
                .call1(("GeometryTypeError", bases, dict))?;
            Ok(class.cast_into::<PyType>()?.unbind())
        })
        .map(|class| class.bind(py))
}

/// Raise `GeometryTypeError` for a one-off wrong-geometry-kind boundary
/// failure (the structured wrong-kind variants route here automatically).
pub(crate) fn geometry_type_err(message: impl Into<String>) -> PyErr {
    Python::attach(|py| match geometry_type_error(py).cloned() {
        Ok(class) => PyErr::from_type(class, (message.into(),)),
        Err(err) => err,
    })
}

fn geometry_type_err_with_expected(message: String, expected: &'static str) -> PyErr {
    with_attrs(geometry_type_err(message), &[
        ("expected", Attr::Str(expected)),
        ("got", Attr::OptionalStr(None)),
    ])
}

/// A boundary parameter error carrying its public parameter name. Token
/// parsers use this when the rejected value is textual rather than numeric.
pub(crate) fn parameter_error(message: impl Into<String>, param: &str) -> PyErr {
    with_attrs(GeometryError::new_err(message.into()), &[(
        "param",
        Attr::Str(param),
    )])
}

/// A boundary parameter error carrying an integral value. Grid depth and
/// budget validation use this rather than dropping the structured value-lane
/// attributes at the PyO3 seam.
pub(crate) fn integer_parameter_error(
    message: impl Into<String>,
    param: &str,
    value: i64,
) -> PyErr {
    with_attrs(GeometryError::new_err(message.into()), &[
        ("param", Attr::Str(param)),
        ("value", Attr::Int(value)),
    ])
}

/// A boundary parameter error carrying its public floating-point value.
pub(crate) fn float_parameter_error(message: impl Into<String>, param: &str, value: f64) -> PyErr {
    with_attrs(GeometryError::new_err(message.into()), &[
        ("param", Attr::Str(param)),
        ("value", Attr::Float(value)),
    ])
}

/// Reject a lossy serializer unless the caller explicitly acknowledges that
/// coordinate-epoch metadata has no representation in its output format.
pub(crate) fn require_epoch_drop(
    epoch: Option<f64>,
    drop_epoch: bool,
    format: &str,
) -> PyResult<()> {
    if epoch.is_some() && !drop_epoch {
        return Err(GeometryError::new_err(format!(
            "{format} cannot encode coordinate epoch metadata; pass drop_epoch=True to acknowledge the loss"
        )));
    }
    Ok(())
}

/// Construct a CRS mismatch with the same structured contract as the core
/// `FrameError` conversion.  Public boundary code must not hand-roll a bare
/// `CRSMismatchError`, because callers recover by inspecting these fields.
pub(crate) fn crs_mismatch_error(
    message: impl Into<String>,
    left: Option<&str>,
    right: Option<&str>,
    index: Option<usize>,
) -> PyErr {
    let error = with_attrs(CRSMismatchError::new_err(message.into()), &[
        ("field", Attr::Str("crs")),
        ("left", Attr::OptionalStr(left)),
        ("right", Attr::OptionalStr(right)),
    ]);
    match index {
        Some(index) => with_attrs(error, &[("index", Attr::Int(index as i64))]),
        None => error,
    }
}

/// Construct an epoch mismatch with the same structured contract as the core
/// `FrameError` conversion.
pub(crate) fn epoch_mismatch_error(
    message: impl Into<String>,
    left: Option<f64>,
    right: Option<f64>,
    index: Option<usize>,
) -> PyErr {
    let error = with_attrs(CRSMismatchError::new_err(message.into()), &[
        ("field", Attr::Str("epoch")),
        ("left", Attr::OptionalFloat(left)),
        ("right", Attr::OptionalFloat(right)),
    ]);
    match index {
        Some(index) => with_attrs(error, &[("index", Attr::Int(index as i64))]),
        None => error,
    }
}

pub(crate) use crate::error::ParseFormat;

pub(crate) fn parse_error(message: impl Into<String>, format: ParseFormat) -> PyErr {
    with_attrs(ParseError::new_err(message.into()), &[(
        "format",
        Attr::Str(format.label()),
    )])
}

/// Tag a `ParseError`'s `.format` after the fact (for `map_err` over a
/// kernel that returns a bare message).
pub(crate) fn tag_parse_format(err: PyErr, format: ParseFormat) -> PyErr {
    with_attrs(err, &[("format", Attr::Str(format.label()))])
}

enum Attr<'a> {
    Str(&'a str),
    OptionalStr(Option<&'a str>),
    Int(i64),
    Float(f64),
    OptionalFloat(Option<f64>),
    OptionalInt(Option<i64>),
}

/// Best-effort structured attributes on a fresh error: handlers read
/// `e.left`/`e.right`/`e.format` instead of parsing the message. A failed
/// setattr never masks the error itself.
fn with_attrs(err: PyErr, attrs: &[(&str, Attr<'_>)]) -> PyErr {
    Python::attach(|py| {
        let exception = err.value(py);
        for (name, attr) in attrs {
            let _ = match attr {
                Attr::Str(s) => exception.setattr(*name, *s),
                Attr::OptionalStr(optional) => exception.setattr(*name, *optional),
                Attr::Int(i) => exception.setattr(*name, *i),
                Attr::Float(f) => exception.setattr(*name, *f),
                Attr::OptionalFloat(optional) => exception.setattr(*name, *optional),
                Attr::OptionalInt(optional) => exception.setattr(*name, *optional),
            };
        }
    });
    err
}

#[expect(
    clippy::too_many_lines,
    reason = "exhaustive Error→PyErr mapping; one arm per variant"
)]
impl From<Error> for PyErr {
    fn from(error: Error) -> Self {
        use crate::FrameError as F;
        use crate::crs::CrsError as C;
        use crate::geometry::GeometryErrorKind as G;
        let message = error.to_string();
        match error.kind() {
            ErrorKind::Geometry(kind) => match kind {
                // Wrong geometry kind for the operation → GeometryTypeError
                // (a TypeError subclass — you handed the operation a geometry
                // of the wrong type).
                G::LineStringRequired => geometry_type_err_with_expected(message, "LineString"),
                G::LinealRequired | G::SidedBufferRequiresLineal => {
                    geometry_type_err_with_expected(message, "LineString or MultiLineString")
                },
                G::PointSplitterRequired => {
                    geometry_type_err_with_expected(message, "Point or MultiPoint")
                },
                G::PolygonRequired | G::CoveragePolygonalRequired => {
                    geometry_type_err_with_expected(message, "Polygon or MultiPolygon")
                },
                G::FrechetLineStringRequired => geometry_type_err_with_expected(
                    message,
                    "LineString or single-line MultiLineString",
                ),
                G::SinglePolygonRequired => geometry_type_err_with_expected(message, "Polygon"),
                // Invalid parameter values → the generic GeometryError (the
                // stdlib raise-ValueError idiom; the message names the kwarg).
                G::NonFinite { param, value }
                | G::NonNegativeFinite(param, value)
                | G::PositiveFinite(param, value)
                | G::Parameter { param, value, .. } => {
                    with_attrs(GeometryError::new_err(message), &[
                        ("param", Attr::Str(param)),
                        ("value", Attr::Float(*value)),
                    ])
                },
                G::NegativeSidedBufferDistance
                | G::InvalidRectangle
                | G::InvalidMeasureRange(..)
                | G::SubstringOrder(..)
                | G::SubstringMeasureOrder(..)
                | G::SnapGridTooFine
                | G::InvalidDe9imPattern(_)
                | G::OffsetCapacityExceeded => GeometryError::new_err(message),
                G::GeneratedOutputTooLarge {
                    operation,
                    parameter,
                    produced,
                    limit,
                } => with_attrs(GeometryError::new_err(message), &[
                    ("operation", Attr::Str(operation)),
                    ("parameter", Attr::Str(parameter)),
                    ("produced", Attr::Int(*produced as i64)),
                    ("limit", Attr::Int(*limit as i64)),
                ]),
                // In-core projection failures → TransformError; an
                // unsupported projected CRS is a CRS problem.
                G::WebMercatorLatitude(_) | G::Projection(_) => TransformError::new_err(message),
                // Structural and content rule violations → InvalidGeometryError.
                G::NonFiniteCoordinate
                | G::CoordinateLength(..)
                | G::CoordinateAxesMismatch { .. }
                | G::CoordinateRange { .. }
                | G::MalformedCsrOffsets
                | G::RingTooShort(_)
                | G::EmptyLinework
                | G::EmptySampleSource
                | G::AntimeridianSplitFailed(_)
                | G::AntimeridianPoleOrdinates
                | G::Triangulation(_)
                | G::Voronoi(_)
                | G::OrdinateDropped(_)
                | G::MissingMeasure
                | G::NonMonotonicMeasure
                | G::MeasureOverwrite
                | G::MissingZ
                | G::LineStringTooShort
                | G::UnrepairableLineString
                | G::UnrepairableMultiLineString
                | G::RepairFailed(_)
                | G::Invalid(_) => InvalidGeometryError::new_err(message),
                G::EmptyGeometrySequence { operation } | G::InvalidCoverage { operation } => {
                    with_attrs(InvalidGeometryError::new_err(message), &[(
                        "operation",
                        Attr::Str(operation),
                    )])
                },
            },
            ErrorKind::Crs(kind) => match kind {
                // Transform construction/execution → TransformError.
                C::TransformCreate { from, to, .. } | C::Transform { from, to, .. } => {
                    with_attrs(TransformError::new_err(message), &[
                        ("source", Attr::Str(from)),
                        ("target", Attr::Str(to)),
                    ])
                },
                // Every other CRS failure → CRSError.
                C::VerticalUnits { crs, .. }
                | C::MetricUnits { crs, .. }
                | C::GeodesicUnits { crs, .. } => {
                    with_attrs(CRSError::new_err(message), &[("crs", Attr::Str(crs))])
                },
                C::Create { .. }
                | C::Export { .. }
                | C::Identify { .. }
                | C::EmptyGeometry
                | C::Message(_) => CRSError::new_err(message),
            },
            // Malformed serialized input → ParseError, regardless of format
            // (`e.format` names which codec rejected it).
            ErrorKind::Io(error) => with_attrs(ParseError::new_err(message), &[
                ("format", Attr::Str(error.format_label())),
                (
                    "position",
                    Attr::OptionalInt(error.position().map(|value| value as i64)),
                ),
            ]),
            ErrorKind::Frame(kind) => match kind {
                // Operands or collection items disagree on frame metadata;
                // `e.left`/`e.right` carry the two frames for programmatic
                // recovery (reproject and retry) without message parsing.
                F::CrsMismatch { left, right, .. } => {
                    with_attrs(CRSMismatchError::new_err(message), &[
                        ("field", Attr::Str("crs")),
                        ("left", Attr::OptionalStr(left.as_ref().map(Crs::as_str))),
                        ("right", Attr::OptionalStr(right.as_ref().map(Crs::as_str))),
                    ])
                },
                F::EpochMismatch { left, right, .. } => {
                    with_attrs(CRSMismatchError::new_err(message), &[
                        ("field", Attr::Str("epoch")),
                        ("left", Attr::OptionalFloat(*left)),
                        ("right", Attr::OptionalFloat(*right)),
                    ])
                },
                F::SharedCrs {
                    index,
                    first,
                    other,
                    ..
                } => with_attrs(CRSMismatchError::new_err(message), &[
                    ("field", Attr::Str("crs")),
                    ("left", Attr::OptionalStr(first.as_ref().map(Crs::as_str))),
                    ("right", Attr::OptionalStr(other.as_ref().map(Crs::as_str))),
                    ("index", Attr::Int(*index as i64)),
                ]),
                F::SharedEpoch {
                    index,
                    first,
                    other,
                    ..
                } => with_attrs(CRSMismatchError::new_err(message), &[
                    ("field", Attr::Str("epoch")),
                    ("left", Attr::OptionalFloat(*first)),
                    ("right", Attr::OptionalFloat(*other)),
                    ("index", Attr::Int(*index as i64)),
                ]),
                // `epoch ⟹ crs` violated at a Frame::new construction boundary
                // — a CRS-domain error, like its siblings (ValueError subclass).
                // Deliberately no left/right/index (single-geometry invariant).
                F::EpochRequiresCrs | F::EpochRequiresDynamicCrs { .. } => {
                    CRSError::new_err(message)
                },
            },
        }
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("GeometryError", py.get_type::<GeometryError>())?;
    m.add(
        "InvalidGeometryError",
        py.get_type::<InvalidGeometryError>(),
    )?;
    m.add("GeometryTypeError", geometry_type_error(py)?)?;
    m.add("CRSError", py.get_type::<CRSError>())?;
    m.add("CRSMismatchError", py.get_type::<CRSMismatchError>())?;
    m.add("TransformError", py.get_type::<TransformError>())?;
    m.add("ParseError", py.get_type::<ParseError>())?;
    m.add("AccuracyWarning", py.get_type::<AccuracyWarning>())?;
    // Class-level None defaults for the structured attributes: operation-
    // raised errors set real values per instance (`with_attrs`); hand-built
    // instances read None instead of AttributeError.
    let geometry = py.get_type::<GeometryError>();
    geometry.setattr("param", py.None())?;
    geometry.setattr("value", py.None())?;
    geometry.setattr("operation", py.None())?;
    geometry.setattr("parameter", py.None())?;
    geometry.setattr("produced", py.None())?;
    geometry.setattr("limit", py.None())?;
    py.get_type::<CRSError>().setattr("crs", py.None())?;
    py.get_type::<InvalidGeometryError>()
        .setattr("operation", py.None())?;
    py.get_type::<InvalidGeometryError>()
        .setattr("parameter", py.None())?;
    py.get_type::<InvalidGeometryError>()
        .setattr("produced", py.None())?;
    py.get_type::<InvalidGeometryError>()
        .setattr("limit", py.None())?;
    py.get_type::<TransformError>()
        .setattr("source", py.None())?;
    py.get_type::<TransformError>()
        .setattr("target", py.None())?;
    py.get_type::<ParseError>().setattr("format", py.None())?;
    py.get_type::<ParseError>().setattr("position", py.None())?;
    let geometry_type = geometry_type_error(py)?;
    geometry_type.setattr("expected", py.None())?;
    geometry_type.setattr("got", py.None())?;
    let mismatch = py.get_type::<CRSMismatchError>();
    mismatch.setattr("field", py.None())?;
    mismatch.setattr("left", py.None())?;
    mismatch.setattr("right", py.None())?;
    mismatch.setattr("index", py.None())?;
    Ok(())
}
