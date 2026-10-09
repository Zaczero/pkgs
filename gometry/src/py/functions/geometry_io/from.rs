use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;

use crate::py::functions::bulk_rows::{StreamedSridRow, batch_bulk_srid, stream_bulk_srid};
use crate::py::wire_crs::{rebase_ewkt_parse_error, resolve_wire_crs, split_ewkt_srid};
use crate::{
    OwnedBytes, PyGeometry, Typed, coordinate_epoch_option, copy_wkb_payload, exact_backed_bytes,
    guard_epoch_frame, io, parse_crs, parse_wkb_geometry,
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
        let crs = resolve_wire_crs(embedded, fallback.as_ref(), "EWKT SRID")?;
        guard_epoch_frame(parsed_epoch, crs.as_ref())?;
        Ok(PyGeometry::new_with_crs_epoch(
            io::parse_wkt(body).map_err(|error| rebase_ewkt_parse_error(error, text, body))?,
            crs,
            parsed_epoch,
        )?)
    };
    if let Ok(text) = data.cast::<pyo3::types::PyString>() {
        let text: PyBackedStr = text.extract()?;
        let geometry = py.detach(move || parse(&text))?;
        return Ok(Typed(geometry).into_pyobject(py)?.unbind());
    }
    let error = || PyTypeError::new_err("expected WKT string or iterable of WKT strings");
    let iter = crate::py_iter_or_error(data, error)?;
    if crate::is_concrete_batch(data)? {
        let array = batch_bulk_srid(
            py,
            data,
            iter,
            move |item, _row| {
                if item.is_none() {
                    return Ok(None);
                }
                item.cast::<pyo3::types::PyString>()
                    .map_err(|_| error())?
                    .extract::<PyBackedStr>()
                    .map(Some)
            },
            fallback.clone(),
            parsed_epoch,
            "WKT import",
            "EWKT SRID",
            |text| text.map(parse_wkt_stream_row).transpose(),
        )?;
        return Ok(array.into_pyobject(py)?.unbind().into());
    }
    let array = stream_bulk_srid(
        py,
        data,
        iter,
        fallback,
        parsed_epoch,
        "WKT import",
        "EWKT SRID",
        move |item, _row| {
            item.cast::<pyo3::types::PyString>()
                .map_err(|_| error())?
                .extract::<PyBackedStr>()
        },
        parse_wkt_stream_row,
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
        let resolved =
            resolve_wire_crs(geometry.crs_ref().cloned(), fallback.as_ref(), "EWKB SRID")?;
        geometry.set_crs_keep_epoch(resolved)?;
        guard_epoch_frame(parsed_epoch, geometry.crs_ref())?;
        geometry.set_epoch_keep_crs(parsed_epoch)?;
        Ok(geometry)
    };
    if crate::is_one_byte_buffer(data)? {
        let bytes = if let Some(bytes) = exact_backed_bytes(data) {
            OwnedBytes::Backed(bytes)
        } else {
            OwnedBytes::Copied(copy_wkb_payload(data)?)
        };
        let geometry = py.detach(move || attach(parse_wkb_geometry(bytes.as_slice())?))?;
        return Ok(Typed(geometry).into_pyobject(py)?.unbind());
    }
    let values = crate::py_iter_or_error(data, || {
        PyTypeError::new_err("expected WKB bytes or iterable of WKB bytes")
    })?;
    let iter = values.clone();
    if crate::is_concrete_batch(data)? {
        let array = batch_bulk_srid(
            py,
            data,
            iter,
            move |item, _row| {
                if item.is_none() {
                    return Ok(None);
                }
                exact_backed_bytes(item).map_or_else(
                    || copy_wkb_payload(item).map(|bytes| Some(OwnedBytes::Copied(bytes))),
                    |bytes| Ok(Some(OwnedBytes::Backed(bytes))),
                )
            },
            fallback.clone(),
            parsed_epoch,
            "WKB import",
            "EWKB SRID",
            |data| {
                data.map(|data| {
                    io::parse_wkb(data.as_slice()).map(|parsed| StreamedSridRow {
                        shape: parsed.shape,
                        srid: parsed.srid,
                    })
                })
                .transpose()
            },
        )?;
        return Ok(array.into_pyobject(py)?.unbind().into());
    }
    let array = stream_bulk_srid(
        py,
        data,
        values,
        fallback,
        parsed_epoch,
        "WKB import",
        "EWKB SRID",
        move |data, _row| {
            exact_backed_bytes(data).map_or_else(
                || copy_wkb_payload(data).map(OwnedBytes::Copied),
                |bytes| Ok(OwnedBytes::Backed(bytes)),
            )
        },
        move |data| {
            let parsed = io::parse_wkb(data.as_slice())?;
            Ok(StreamedSridRow {
                shape: parsed.shape,
                srid: parsed.srid,
            })
        },
    )?;
    Ok(array.into_pyobject(py)?.unbind().into())
}

fn parse_wkt_stream_row(text: PyBackedStr) -> crate::error::Result<StreamedSridRow> {
    let source = text.as_ref();
    let (body, srid) = split_ewkt_srid(source)?;
    Ok(StreamedSridRow {
        shape: io::parse_wkt(body).map_err(|error| rebase_ewkt_parse_error(error, source, body))?,
        srid,
    })
}
