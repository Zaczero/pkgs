#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::py::functions::bulk_rows::{StreamedSridRow, stream_bulk_srid};
use crate::py::wire_crs::{guard_embedded_crs_conflict, prefer_wire_alias_crs, split_ewkt_srid};
use crate::{
    PyGeometry, Typed, coordinate_epoch_option, crs_arc, guard_epoch_frame, io, parse_crs,
    parse_wkb_geometry, parse_wkb_payload_batch, parse_wkb_payload_bytes,
};

/// Parse a geometry (or array) from Well-Known Text.
///
/// Also accepts EWKT; an embedded ``SRID=...;`` prefix becomes the
/// geometry's CRS. ``SRID=0`` is PostGIS unknown/unspecified and yields a
/// CRS-free geometry; nonzero codes resolve through the canonical PROJ CRS
/// parser (invalid codes raise ``CRSError``).
///
/// Parameters
/// ----------
/// data : str or iterable of str
///     A single WKT/EWKT string, or any iterable of them for a
///     ``GeometryArray``.
///
/// crs : str or int, optional
///     CRS for SRID-less input. An embedded SRID that *contradicts* an
///     explicit ``crs`` raises rather than silently winning.
///
/// epoch : float, optional
///     Coordinate epoch (decimal year) to attach as frame metadata.
///
/// Returns
/// -------
/// Geometry or GeometryArray
///     A ``GeometryArray`` when ``data`` is an iterable, else a ``Geometry``.
///
/// Raises
/// ------
/// ParseError
///     If the WKT is malformed.
/// CRSMismatchError
///     If an embedded SRID conflicts with ``crs``.
/// CRSError
///     If ``crs`` or an embedded nonzero SRID is not recognized, or
///     ``epoch`` is set without ``crs``.
/// GeometryError
///     If ``epoch`` is not a finite decimal year.
///
/// See Also
/// --------
/// Geometry.to_wkt : Serialize a geometry to WKT.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.from_wkt('POINT (1 2)').geometry_type
/// 'Point'
/// >>> gm.from_wkt('SRID=4326;POINT (1 2)').crs
/// CRS("EPSG:4326")
#[pyfunction]
#[pyo3(signature = (data, *, crs = None, epoch = None))]
pub(crate) fn from_wkt(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let fallback = parse_crs(crs)?;
    let parsed_epoch = coordinate_epoch_option("epoch", epoch)?;
    let parse = |text: &str| -> PyResult<PyGeometry> {
        let (body, srid) = split_ewkt_srid(text)?;
        let embedded = io::crs_from_optional_srid(srid)?;
        guard_embedded_crs_conflict(embedded.as_deref(), fallback.as_deref(), "EWKT SRID")?;
        let crs = prefer_wire_alias_crs(embedded.map(crs_arc), fallback.as_ref())
            .or_else(|| fallback.clone());
        guard_epoch_frame(parsed_epoch, crs.as_ref())?;
        Ok(PyGeometry::with_epoch(
            io::parse_wkt(body)?,
            crs,
            parsed_epoch,
        ))
    };
    if let Ok(text) = data.cast::<pyo3::types::PyString>() {
        return Ok(Typed(parse(text.to_cow()?.as_ref())?)
            .into_pyobject(py)?
            .unbind());
    }
    let error = || PyTypeError::new_err("expected WKT string or iterable of WKT strings");
    let iter = data.try_iter().map_err(|_| error())?;
    let array = stream_bulk_srid(
        iter,
        fallback,
        parsed_epoch,
        "WKT import",
        "EWKT SRID",
        move |item, rows| {
            let text = item
                .cast::<pyo3::types::PyString>()
                .map_err(|_| error())?
                .to_cow()?;
            let (body, srid) = split_ewkt_srid(text.as_ref())?;
            rows.try_push(io::parse_wkt(body)?)?;
            Ok(StreamedSridRow::Present(srid))
        },
    )?;
    Ok(array.into_pyobject(py)?.unbind().into())
}

/// Parse a geometry (or array) from Well-Known Binary.
///
/// Also accepts EWKB; an embedded SRID becomes the geometry's CRS.
/// ``SRID=0`` / EWKB SRID 0 is PostGIS unknown → CRS-free; nonzero codes
/// resolve through the canonical PROJ CRS parser (invalid codes raise).
///
/// Parameters
/// ----------
/// data : bytes or sequence of bytes
///     A WKB/EWKB buffer, or an iterable of them for a ``GeometryArray``.
///
/// crs : str or int, optional
///     CRS for SRID-less input. An embedded SRID that *contradicts* an
///     explicit ``crs`` raises rather than silently winning.
///
/// epoch : float, optional
///     Coordinate epoch (decimal year) to attach as frame metadata.
///
/// Returns
/// -------
/// Geometry or GeometryArray
///     A ``GeometryArray`` when ``data`` is an iterable, else a ``Geometry``.
///
/// Raises
/// ------
/// ParseError
///     If the WKB is malformed.
/// CRSMismatchError
///     If an embedded SRID conflicts with ``crs``.
/// CRSError
///     If ``crs`` or an embedded nonzero SRID is not recognized, or
///     ``epoch`` is set without ``crs``.
/// GeometryError
///     If ``epoch`` is not a finite decimal year.
///
/// Notes
/// -----
/// The (E)WKB format does not carry a coordinate epoch and one does not
/// survive a WKB round-trip; ``epoch=`` attaches it as frame metadata (as
/// with ``GeometryArray``), and Arrow interchange preserves it on export.
///
/// See Also
/// --------
/// Geometry.to_wkb : Serialize a geometry to WKB.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> hex = '0101000000000000000000f03f0000000000000040'
/// >>> gm.from_wkb(bytes.fromhex(hex)).to_wkt()
/// 'POINT (1 2)'
#[pyfunction]
#[pyo3(signature = (data, *, crs = None, epoch = None))]
pub(crate) fn from_wkb(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let fallback = parse_crs(crs)?;
    let parsed_epoch = coordinate_epoch_option("epoch", epoch)?;
    let attach = |mut geometry: PyGeometry| -> PyResult<PyGeometry> {
        guard_embedded_crs_conflict(geometry.crs_str(), fallback.as_deref(), "EWKB SRID")?;
        let resolved = prefer_wire_alias_crs(geometry.crs_ref().cloned(), fallback.as_ref())
            .or_else(|| fallback.clone());
        geometry.set_crs_keep_epoch(resolved);
        guard_epoch_frame(parsed_epoch, geometry.crs_ref())?;
        geometry.set_epoch_keep_crs(parsed_epoch);
        Ok(geometry)
    };
    match parse_wkb_payload_bytes(data, parse_wkb_geometry) {
        Ok(geometry) => return Ok(Typed(attach(geometry)?).into_pyobject(py)?.unbind()),
        Err(err) if err.is_instance_of::<PyTypeError>(py) => {},
        Err(err) => return Err(err),
    }
    if let Ok(values) = data.try_iter() {
        let arena = io::WkbCoordArena::new();
        let array = stream_bulk_srid(
            values,
            fallback,
            parsed_epoch,
            "WKB import",
            "EWKB SRID",
            move |data, rows| {
                let parsed = parse_wkb_payload_batch(data, &arena)?;
                rows.try_push(parsed.shape)?;
                Ok(StreamedSridRow::Present(parsed.srid))
            },
        )?;
        return Ok(array.into_pyobject(py)?.unbind().into());
    }
    Err(PyTypeError::new_err(
        "expected WKB bytes or iterable of WKB bytes",
    ))
}
