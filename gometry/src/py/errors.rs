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

use pyo3::exceptions::{PyTypeError, PyUserWarning, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyString, PyType};
use pyo3::{create_exception, ffi};

use crate::error::{Error, ErrorKind, ResourceError};
use crate::{Crs, ValidationIssue};

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

/// Own the Python projection of a geometry validity issue.  Callers pass the
/// issue itself rather than recovering fields from its display text, so row
/// context and structural diagnostics survive every ingress path unchanged.
pub(crate) fn invalid_geometry_issue(issue: &ValidationIssue, row: Option<usize>) -> PyErr {
    let message = row
        .filter(|_| issue.code != "coordinate_axes_mismatch")
        .map_or_else(
            || issue.detail.clone(),
            |row| format!("geometry {row} is invalid: {}", issue.detail),
        );
    let error = with_attrs(InvalidGeometryError::new_err(message), &[
        (
            "row",
            Attr::OptionalInt(row.and_then(|value| i64::try_from(value).ok())),
        ),
        ("path", Attr::OptionalStr(issue.path.as_deref())),
        ("code", Attr::Str(issue.code)),
        ("detail", Attr::Str(&issue.detail)),
    ]);
    Python::attach(|py| {
        if let Some(point) = issue.location {
            let _ = error.value(py).setattr("location", (point.x, point.y));
        } else {
            let _ = error.value(py).setattr("location", py.None());
        }
    });
    error
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

enum NativeResource {
    Entropy,
    Allocation,
    Overflow,
    OverflowContext(&'static str),
}

impl NativeResource {
    fn into_py_err(self) -> PyErr {
        match self {
            Self::Entropy => Python::attach(|py| {
                // SAFETY: the interpreter is attached and the message is static.
                unsafe {
                    ffi::PyErr_SetString(
                        ffi::PyExc_OSError,
                        c"operating system entropy acquisition failed".as_ptr(),
                    );
                }
                PyErr::fetch(py)
            }),
            Self::Allocation => native_memory_error(),
            Self::Overflow => native_size_overflow_error(),
            Self::OverflowContext(message) => native_overflow_error(message),
        }
    }
}

/// Raise Python's native `MemoryError` without creating PyO3's lazy Rust error
/// box. The C helper sets the interpreter error and returns NULL; adoption is
/// deliberately delegated to PyO3's version-aware fetch path.
pub(crate) fn native_memory_error() -> PyErr {
    Python::attach(|py| {
        // SAFETY: `Python::attach` supplies the active interpreter state.
        let _ = unsafe { ffi::PyErr_NoMemory() };
        debug_assert!(PyErr::occurred(py));
        PyErr::fetch(py)
    })
}

/// Raise the fixed native overflow exception without creating a Rust error
/// object. The C setter takes the static message and PyO3 owns version-specific
/// exception adoption.
pub(crate) fn native_size_overflow_error() -> PyErr {
    Python::attach(|py| {
        // SAFETY: the active interpreter owns the public exception type and
        // the C string has static storage for the duration of the call.
        unsafe {
            ffi::PyErr_SetString(
                ffi::PyExc_OverflowError,
                c"resource size arithmetic overflowed".as_ptr(),
            );
        }
        debug_assert!(PyErr::occurred(py));
        PyErr::fetch(py)
    })
}

/// Materialize an overflow exception from a Python-owned string. The string
/// conversion is fallible and uses Python's allocator; no Rust `String` or
/// lazy PyO3 error state is introduced here.
pub(crate) fn native_overflow_error(message: &str) -> PyErr {
    Python::attach(|py| {
        let message = match PyString::from_bytes(py, message.as_bytes()) {
            Ok(message) => message,
            Err(error) => return error,
        };
        // SAFETY: `message` is a live Python string in the attached
        // interpreter, and the exception type is a public CPython singleton.
        unsafe {
            ffi::PyErr_SetObject(ffi::PyExc_OverflowError, message.as_ptr());
        }
        debug_assert!(PyErr::occurred(py));
        PyErr::fetch(py)
    })
}

fn native_resource(error: &Error) -> Option<NativeResource> {
    match error.kind() {
        ErrorKind::Resource(kind) => Some(match kind {
            ResourceError::Entropy => NativeResource::Entropy,
            ResourceError::Allocation => NativeResource::Allocation,
            ResourceError::SizeOverflow => NativeResource::Overflow,
            ResourceError::SizeOverflowContext(message) => NativeResource::OverflowContext(message),
        }),
        _ => None,
    }
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
        if let Some(resource) = native_resource(&error) {
            drop(error);
            return resource.into_py_err();
        }
        let message = match error.kind() {
            ErrorKind::Io(error) => error.display_message(),
            _ => error.to_string(),
        };
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
                | G::InvalidGeographicRectangle
                | G::InvalidMeasureRange(..)
                | G::SubstringOrder(..)
                | G::SubstringMeasureOrder(..)
                | G::SnapGridTooFine
                | G::InvalidDe9imPattern(_)
                | G::OffsetCapacityExceeded
                | G::PicklePayload(_) => GeometryError::new_err(message),
                G::UnrepresentableOutput { operation } => with_attrs(
                    GeometryError::new_err(message),
                    &[("operation", Attr::Str(operation))],
                ),
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
                G::WebMercatorLatitude(_) | G::Projection(_) | G::ProjectionDomain(_) => {
                    TransformError::new_err(message)
                },
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
                | G::ExactRatioZeroDenominator
                | G::ExactCoordinateOutOfRange
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
                ("path", Attr::OptionalStr(error.path())),
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
                F::EmbeddedCrsConflict {
                    explicit, embedded, ..
                } => with_attrs(CRSMismatchError::new_err(message), &[
                    ("field", Attr::Str("crs")),
                    ("left", Attr::OptionalStr(Some(explicit.as_str()))),
                    ("right", Attr::OptionalStr(Some(embedded.as_str()))),
                ]),
                // `epoch ⟹ crs` violated at a Frame::new construction boundary
                // — a CRS-domain error, like its siblings (ValueError subclass).
                // Deliberately no left/right/index (single-geometry invariant).
                F::EpochRequiresCrs | F::EpochRequiresDynamicCrs { .. } => {
                    CRSError::new_err(message)
                },
            },
            ErrorKind::Resource(_) => unreachable!("resource errors are handled before formatting"),
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
    for name in ["row", "path", "code", "detail", "location"] {
        py.get_type::<InvalidGeometryError>()
            .setattr(name, py.None())?;
    }
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
    py.get_type::<ParseError>().setattr("path", py.None())?;
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

#[cfg(test)]
mod tests {
    use pyo3::exceptions::{PyMemoryError, PyOverflowError};
    use pyo3::prelude::*;

    use super::{native_memory_error, native_overflow_error};
    use crate::error::{Error, ResourceError};
    use crate::test_support::fail_allocations_after;

    #[test]
    fn native_memory_error_survives_persistent_rust_allocation_rejection() {
        Python::initialize();
        Python::attach(|py| {
            let error = fail_allocations_after(0, || {
                let error = Error::from(ResourceError::Allocation);
                let error: PyErr = error.into();
                error
            });
            assert!(error.is_instance_of::<PyMemoryError>(py));
            drop(error);
        });
    }

    #[test]
    fn native_memory_error_can_attach_before_the_inspection_scope() {
        Python::initialize();
        let error = fail_allocations_after(0, native_memory_error);
        Python::attach(|py| {
            assert!(error.is_instance_of::<PyMemoryError>(py));
            drop(error);
        });
    }

    #[test]
    fn contextual_overflow_keeps_its_message() {
        Python::initialize();
        Python::attach(|py| {
            let error: PyErr = Error::from(ResourceError::SizeOverflowContext(
                "coordinate count overflow",
            ))
            .into();
            assert!(error.is_instance_of::<PyOverflowError>(py));
            assert_eq!(
                error
                    .value(py)
                    .str()
                    .expect("overflow str")
                    .to_str()
                    .unwrap(),
                "coordinate count overflow"
            );
            drop(error);

            let error: PyErr = Error::from(ResourceError::SizeOverflow).into();
            assert!(error.is_instance_of::<PyOverflowError>(py));
            assert_eq!(
                error
                    .value(py)
                    .str()
                    .expect("overflow str")
                    .to_str()
                    .unwrap(),
                "resource size arithmetic overflowed"
            );
            drop(error);

            let error = native_overflow_error("row count overflow");
            assert!(error.is_instance_of::<PyOverflowError>(py));
            assert_eq!(
                error
                    .value(py)
                    .str()
                    .expect("overflow str")
                    .to_str()
                    .unwrap(),
                "row count overflow"
            );
            drop(error);
        });
    }
}
