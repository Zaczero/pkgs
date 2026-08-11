#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::borrow::Cow;

use pyo3::exceptions::PyTypeError;
use pyo3::types::PyString;

use crate::py::support::{
    Bound, CRSError, Crs, Frame, FromPyObject, GeometryError, PyAny, PyAnyMethods as _, PyErr,
    PyGeometry, PyResult, PyStringMethods as _, coordinate_epoch_option, wgs84_crs,
};

/// Borrow ``value`` as UTF-8 text when it is a Python ``str``.
pub(crate) fn py_text_borrow<'py>(
    value: &'py Bound<'py, PyAny>,
    message: &str,
) -> PyResult<Cow<'py, str>> {
    let text = value
        .cast::<PyString>()
        .map_err(|_| PyTypeError::new_err(message.to_owned()))?;
    text.to_cow()
}

pub(crate) fn parse_crs(value: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Crs>> {
    let Some(value) = value else {
        return Ok(None);
    };
    crate::py::crs::parse_crs_inner(value, 0)
}

/// A ``crs=`` parameter whose default is gometry's WGS84 lon/lat label rather
/// than "no CRS" — the shape every parser of an inherently-WGS84 format needs
/// (`from_geojson`, `from_features`, `from_polyline`).
///
/// It exists as one type so the default cannot drift per parser. It did: the
/// `geojson` lane defaulted to [`WGS84_LONLAT`] while the polyline lane
/// defaulted to `EPSG:4326`, and because the two labels are deliberately
/// unequal as stored CRS, geometry parsed from the two formats could not be
/// combined into one `GeometryArray`.
pub(crate) enum Wgs84DefaultCrs {
    /// ``crs=`` omitted — stamp [`WGS84_LONLAT`].
    Default,
    /// ``crs=`` given, already parsed; `None` is an explicit CRS-free request.
    Resolved(Option<Crs>),
}

impl Wgs84DefaultCrs {
    pub(crate) fn into_crs(self) -> Option<Crs> {
        match self {
            Self::Default => Some(wgs84_crs()),
            Self::Resolved(crs) => crs,
        }
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for Wgs84DefaultCrs {
    type Error = PyErr;

    fn extract(value: pyo3::Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if value.is_none() {
            return Ok(Self::Resolved(None));
        }
        Ok(Self::Resolved(parse_crs(Some(&value))?))
    }
}

pub(crate) fn require_antimeridian_crs(crs: Option<&str>) -> PyResult<()> {
    // Same frame contract as `split_antimeridian`: any geographic (degree)
    // CRS or CRS-free lon/lat; a projected frame has no antimeridian.
    if crs.is_some()
        && matches!(
            crate::crs::metric_model(crs)?,
            crate::crs::MetricModel::Planar { .. }
        )
    {
        return Err(CRSError::new_err(
            "crosses_antimeridian requires a geographic CRS; use set_crs(...) or to_crs(...) to \
             attach one",
        ));
    }
    Ok(())
}

pub(crate) fn common_crs_required<'a>(
    items: impl IntoIterator<Item = &'a PyGeometry>,
    context: &str,
) -> PyResult<Option<Crs>> {
    let mut items = items.into_iter();
    let Some(first) = items.next().map(|item| item.crs_ref().cloned()) else {
        return Ok(None);
    };
    for (offset, item) in items.enumerate() {
        // Same rule as `Frame::common`: items must name one frame, and the
        // first item's label is the one carried forward.
        let agrees = match (first.as_ref(), item.crs_ref()) {
            (None, None) => true,
            (Some(left), Some(right)) => crate::crs_operationally_equal(left, right)?,
            _ => false,
        };
        if !agrees {
            let index = offset + 1;
            return Err(crate::py::errors::crs_mismatch_error(
                format!(
                    "{context} requires one shared CRS; item 0 has {}, item {index} has {}. Use to_crs(...) or pass crs=... explicitly before building the array",
                    crs_label(first.as_deref()),
                    crs_label(item.crs_str()),
                ),
                first.as_deref(),
                item.crs_str(),
                Some(index),
            ));
        }
    }
    Ok(first)
}

pub(crate) fn crs_label(crs: Option<&str>) -> String {
    crs.map_or_else(|| "None".to_owned(), |value| format!("{value:?}"))
}

/// Validate a coordinate epoch arriving from a serialized source (pickle /
/// Arrow extension metadata): finite, canonical (`-0.0 → 0.0`), and present
/// only on a dynamic CRS (the `epoch ⟹ dynamic crs` invariant). Deserialization is an
/// untrusted boundary, so a tampered or foreign payload is rejected here rather
/// than producing an incoherent in-memory geometry.
pub(crate) fn deserialized_epoch(epoch: Option<f64>, crs: Option<&str>) -> PyResult<Option<f64>> {
    let epoch = match epoch {
        None => None,
        Some(value) if value.is_finite() => Some(if value == 0.0 { 0.0 } else { value }),
        Some(_) => {
            return Err(GeometryError::new_err(
                "coordinate epoch must be a finite decimal year",
            ));
        },
    };
    let crs_owned = crs.map(Crs::from);
    guard_epoch_frame(epoch, crs_owned.as_ref())?;
    Ok(epoch)
}

/// Ask the frame owner to validate an epoch at an untrusted Python ingress
/// before internal mutations that use [`Frame::from_trusted_parts`].
pub(crate) fn guard_epoch_frame(epoch: Option<f64>, crs: Option<&Crs>) -> PyResult<()> {
    Frame::new(crs.cloned(), epoch)
        .map(|_| ())
        .map_err(PyErr::from)
}

/// Parse the shared constructor `crs=`/`epoch=` arguments into a validated
/// [`Frame`], enforcing the `epoch ⟹ dynamic crs` invariant at this Python boundary.
/// The epoch is canonicalized (`-0.0 → 0.0`) by [`coordinate_epoch_option`].
pub(crate) fn parse_crs_epoch(
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Frame> {
    let crs = parse_crs(crs)?;
    let epoch = coordinate_epoch_option("epoch", epoch)?;
    Ok(Frame::new(crs, epoch)?)
}
