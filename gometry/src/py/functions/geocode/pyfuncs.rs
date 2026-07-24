#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::types::PyTuple;

use super::pluscode::*;
use super::shortlink::*;
use super::*;

fn geocode_array_codes(
    array: &PyGeometryArray,
    operation: impl Fn(f64, f64) -> PyResult<String>,
) -> PyResult<Vec<Option<String>>> {
    let points = array
        .storage()
        .point_rows()
        .ok_or_else(|| crate::py::errors::geometry_type_err("expected Point geometry"))?;
    let lonlat_frame = array.crs_str().is_none_or(crate::crs::is_wgs84_lonlat);
    if lonlat_frame || points.is_empty() {
        return points
            .iter()
            .enumerate()
            .map(|(row, point)| {
                if array.is_row_missing(row) {
                    return Ok(None);
                }
                crate::boundary::geographic::validate_lonlat_point(point)
                    .map_err(|err| crate::note_array_row(err, row))?;
                operation(point.x, point.y)
                    .map(Some)
                    .map_err(|err| crate::note_array_row(err, row))
            })
            .collect();
    }

    let present_rows = (0..points.len())
        .filter(|&row| !array.is_row_missing(row))
        .collect::<Vec<_>>();
    if present_rows.is_empty() {
        return Ok(vec![None; points.len()]);
    }
    let source = present_rows
        .iter()
        .map(|&row| points.get(row).xy())
        .collect::<Vec<_>>();
    let source_crs = array
        .crs_str()
        .expect("non-lonlat point array carries a CRS");
    let Shape::MultiPoint(transformed) =
        crate::crs::transform(&Shape::MultiPoint(source.into()), source_crs, "EPSG:4326")?
    else {
        unreachable!("transform preserves the MultiPoint kind")
    };
    let mut output = vec![None; points.len()];
    for (index, &row) in present_rows.iter().enumerate() {
        let point = transformed.point_at(index);
        crate::boundary::geographic::validate_lonlat_point(point)
            .map_err(|err| crate::note_array_row(err, row))?;
        output[row] =
            Some(operation(point.x, point.y).map_err(|err| crate::note_array_row(err, row))?);
    }
    Ok(output)
}

fn geocode_encode_broadcast(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    lat: Option<&Bound<'_, PyAny>>,
    validate_raw_domain: bool,
    operation: impl Fn(f64, f64) -> PyResult<String> + Copy,
) -> PyResult<Py<PyAny>> {
    use pyo3::IntoPyObjectExt;

    if let Some(geometry) = exact_geometry(value) {
        if lat.is_some() {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "lat must not be provided when the first argument is a geometry",
            ));
        }
        let shape = crate::lonlat_shape(geometry)?;
        let (lon, lat) = crate::point_xy(&shape)?;
        return operation(lon, lat)?.into_py_any(py);
    }
    if let Some(array) = exact_geometry_array(value) {
        if lat.is_some() {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "lat must not be provided when the first argument is a geometry array",
            ));
        }
        return geocode_array_codes(array, operation)?.into_py_any(py);
    }

    let lat = lat.ok_or_else(|| {
        pyo3::exceptions::PyTypeError::new_err(
            "lat is required when the first argument is a longitude",
        )
    })?;
    let mut lon = coordinate_input(py, value, "longitude")?;
    let mut lat = coordinate_input(py, lat, "latitude")?;
    let scalar = lon.scalar && lat.scalar;
    broadcast_coordinate_group(
        [(&mut lon, "longitude"), (&mut lat, "latitude")],
        "lon and lat",
    )?;
    let encoded = lon
        .values
        .iter()
        .zip(&lat.values)
        .map(|(&lon, &lat)| {
            if validate_raw_domain {
                crate::boundary::geographic::validate_lonlat_xy(lon, lat)?;
            }
            operation(lon, lat)
        })
        .collect::<PyResult<Vec<_>>>()?;
    if scalar {
        encoded
            .into_iter()
            .next()
            .expect("scalar coordinate inputs produce one code")
            .into_py_any(py)
    } else {
        encoded.into_py_any(py)
    }
}

fn code_lonlat_broadcast(
    py: Python<'_>,
    code: &Bound<'_, PyAny>,
    lon: &Bound<'_, PyAny>,
    lat: &Bound<'_, PyAny>,
    operation: impl Fn(&str, f64, f64) -> PyResult<String> + Send + Sync,
) -> PyResult<Py<PyAny>> {
    use pyo3::IntoPyObjectExt;

    match CodeInput::parse(code, "code")? {
        CodeInput::Scalar(code) => operation(
            &code,
            finite_coordinate_required("lon", lon)?,
            finite_coordinate_required("lat", lat)?,
        )?
        .into_py_any(py),
        CodeInput::Many(codes) => {
            let lon = F64Param::parse(lon, "lon", codes.len())?;
            let lat = F64Param::parse(lat, "lat", codes.len())?;
            py.detach(move || {
                codes
                    .iter()
                    .enumerate()
                    .map(|(row, code)| operation(code, lon.get(row), lat.get(row)))
                    .collect::<PyResult<Vec<_>>>()
            })?
            .into_py_any(py)
        },
    }
}

/// Plus code (Open Location Code) of a point.
///
/// Encodes WGS84 coordinates as Google's Open Location Code — the
/// offline-friendly "plus codes" used where street addresses are missing.
/// Accepts a ``Point``/`GeometryArray` (CRS-aware, reprojected to lon/lat)
/// or a bare ``lon, lat`` pair.
///
/// Parameters
/// ----------
/// value : Point, GeometryArray, or float
///     The point(s) to encode, or a bare longitude.
/// lat : float, optional
///     Latitude when ``value`` is a longitude.
/// length : int, default 10
///     Significant digits (even from 2 to 10, then 11-15); 10 is roughly a
///     14 m cell, each pair beyond divides it further.
///
/// Returns
/// -------
/// str or list of str
///     The plus code(s), e.g. ``'8FVC2222+22'``.
///
/// Raises
/// ------
/// GeometryError
///     If ``length`` is invalid.
/// InvalidGeometryError
///     If a coordinate is non-finite.
///
/// Notes
/// -----
/// Bare longitude/latitude inputs follow the canonical Open Location Code
/// convention: latitude is clipped to ``[-90, 90]`` and longitude is wrapped
/// into ``[-180, 180)`` before encoding (so ``lon=181`` or ``lat=91`` encode
/// rather than raise). Only non-finite coordinates are rejected. Geometry and
/// `GeometryArray` inputs carry real spatial data and are still validated
/// against the WGS84 lon/lat domain.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.pluscode_encode(8.628, 47.366)
/// '8FVC9J8H+C6'
#[pyfunction]
#[pyo3(signature = (value, lat = None, *, length = 10), text_signature = "(value, lat=None, *, length=10)")]
pub(crate) fn pluscode_encode(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    lat: Option<&Bound<'_, PyAny>>,
    length: i64,
) -> PyResult<Py<PyAny>> {
    let length = validate_pluscode_length(length)?;
    let encode = |lon: f64, lat: f64| Ok(olc_encode(lat, lon, length));
    // Bare coordinates follow OLC's clip/wrap convention. Geometry inputs
    // remain strict WGS84 spatial data, enforced by `lonlat_shape` and the
    // array path above.
    geocode_encode_broadcast(py, value, lat, false, encode)
}

/// Return the rectangular cell a plus code covers, as a WGS84 ``Polygon``.
///
/// Parameters
/// ----------
/// code : str or iterable of str
///     A full plus code (e.g. ``'8FVC9G8F+6X'``), or one code per row.
///
/// Returns
/// -------
/// Polygon or GeometryArray
///     The code cell(s), CRS EPSG:4326.
///
/// Raises
/// ------
/// ParseError
///     If ``code`` is not a full plus code.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.pluscode_polygon('8FVC9G8F+6X').bounds
/// (8.524875, 47.3655, 8.525, 47.36562499999999)
#[pyfunction]
pub(crate) fn pluscode_polygon(py: Python<'_>, code: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    fn polygon_shape(code: &str) -> PyResult<Shape> {
        olc_require_full(code)?;
        let area = olc_decode(&code.to_ascii_uppercase());
        Ok(Shape::Polygon(crate::box_polygon(
            area.lng_lo,
            area.lat_lo,
            area.lng_hi,
            area.lat_hi,
        )?))
    }
    let frame = Frame::new(Some("EPSG:4326".into()), None)?;
    match CodeInput::parse(code, "code")? {
        CodeInput::Scalar(code) => Ok(Typed(PyGeometry::with_frame(polygon_shape(&code)?, frame))
            .into_pyobject(py)?
            .unbind()),
        CodeInput::Many(codes) => {
            let shapes = py.detach(move || {
                codes
                    .iter()
                    .map(|code| polygon_shape(code))
                    .collect::<PyResult<Vec<_>>>()
            })?;
            Ok(PyGeometryArray::from_shapes(shapes, frame)
                .into_pyobject(py)?
                .unbind()
                .into())
        },
    }
}

/// Shorten a full plus code relative to a nearby reference point.
///
/// Removes leading digits that the reference location implies (at least
/// four when close enough); `pluscode_recover` restores them.
///
/// Parameters
/// ----------
/// code : str or iterable of str
///     A full, unpadded plus code with at least 6 digits.
/// reference, lat : float or sequence of float
///     The reference location(s).
///
/// Returns
/// -------
/// str or list of str
///     The shortened code(s) — or the original when the reference is too far.
///
/// Raises
/// ------
/// ParseError
///     If ``code`` is not a full plus code.
/// GeometryError
///     If the code is padded or has fewer than 6 digits.
/// InvalidGeometryError
///     If ``reference``/``lat`` are non-finite.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.pluscode_shorten('8FVC9G8F+6X', 8.5, 47.4)
/// '9G8F+6X'
#[pyfunction]
pub(crate) fn pluscode_shorten(
    py: Python<'_>,
    code: &Bound<'_, PyAny>,
    reference: &Bound<'_, PyAny>,
    lat: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    code_lonlat_broadcast(py, code, reference, lat, |code, lon, lat| {
        olc_shorten(code, lat, lon)
    })
}

/// Recover the nearest full plus code from a shortened one.
///
/// Parameters
/// ----------
/// code : str or iterable of str
///     A short plus code (e.g. ``'9G8F+6X'``); a full code passes through
///     normalized.
/// reference, lat : float or sequence of float
///     The reference location(s) the code is near.
///
/// Returns
/// -------
/// str or list of str
///     The full plus code(s) closest to the reference.
///
/// Raises
/// ------
/// ParseError
///     If ``code`` is neither a short nor a full plus code.
/// InvalidGeometryError
///     If ``reference``/``lat`` are non-finite.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.pluscode_recover('9G8F+6X', 8.5, 47.4)
/// '8FVC9G8F+6X'
#[pyfunction]
pub(crate) fn pluscode_recover(
    py: Python<'_>,
    code: &Bound<'_, PyAny>,
    reference: &Bound<'_, PyAny>,
    lat: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    code_lonlat_broadcast(py, code, reference, lat, |code, lon, lat| {
        olc_recover(code, lat, lon)
    })
}

/// `OpenStreetMap` shortlink code of a point.
///
/// The compact location code in ``https://osm.org/go/...`` URLs (a Morton
/// quadtile path, six bits per character). Accepts a
/// ``Point``/`GeometryArray` (CRS-aware) or a bare ``lon, lat`` pair.
///
/// Parameters
/// ----------
/// value : Point, GeometryArray, or float
///     The point(s) to encode, or a bare longitude.
/// lat : float, optional
///     Latitude when ``value`` is a longitude.
/// zoom : int, default 16
///     Map zoom the link opens at (``0`` to ``22``).
///
/// Returns
/// -------
/// str or list of str
///     The shortlink code(s), e.g. ``'0EEQjE--'``.
///
/// Raises
/// ------
/// GeometryError
///     If ``zoom`` is out of range.
/// InvalidGeometryError
///     If a coordinate is non-finite or out of the lon/lat domain.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.osm_shortlink_encode(13.365, 52.5077, zoom=17)
/// '0MbEUxVoG-'
#[pyfunction]
#[pyo3(signature = (value, lat = None, *, zoom = 16), text_signature = "(value, lat=None, *, zoom=16)")]
pub(crate) fn osm_shortlink_encode(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    lat: Option<&Bound<'_, PyAny>>,
    zoom: i64,
) -> PyResult<Py<PyAny>> {
    if !(0..=22).contains(&zoom) {
        return Err(GeometryError::new_err(format!(
            "osm_shortlink zoom must be between 0 and 22, got {zoom}"
        )));
    }
    let zoom = zoom as u8;
    geocode_encode_broadcast(py, value, lat, true, |lon, lat| {
        Ok(shortlink_encode(lon, lat, zoom))
    })
}

/// Parse an OSM shortlink code back into its location and zoom.
///
/// Accepts the modern ``~`` spelling and the legacy ``@`` one.
///
/// Parameters
/// ----------
/// code : str or iterable of str
///     The shortlink code(s) (the part after ``osm.org/go/``).
///
/// Returns
/// -------
/// tuple
///     Scalar input returns ``(lon, lat, zoom)``. Bulk input returns
///     ``(lon_array, lat_array, zoom_array)``.
///
/// Raises
/// ------
/// ParseError
///     If ``code`` contains characters outside the shortlink alphabet, or is
///     too short/long to name a real zoom level.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> lon, lat, zoom = gm.osm_shortlink_location('0MbEUxVoG-')
/// >>> round(lon, 3), round(lat, 3), zoom
/// (13.365, 52.508, 17)
#[pyfunction]
pub(crate) fn osm_shortlink_location(
    py: Python<'_>,
    code: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    match CodeInput::parse(code, "code")? {
        CodeInput::Scalar(code) => Ok(shortlink_decode(&code)?.into_pyobject(py)?.unbind().into()),
        CodeInput::Many(codes) => {
            let (lon, lat, zoom) = py.detach(move || {
                let mut lon = Vec::with_capacity(codes.len());
                let mut lat = Vec::with_capacity(codes.len());
                let mut zoom = Vec::with_capacity(codes.len());
                for code in &codes {
                    let (x, y, z) = shortlink_decode(code)?;
                    lon.push(x);
                    lat.push(y);
                    zoom.push(i64::from(z));
                }
                Ok::<_, PyErr>((lon, lat, zoom))
            })?;
            let lon = crate::py::numpy::float64_array(py, lon)?;
            let lat = crate::py::numpy::float64_array(py, lat)?;
            let zoom = crate::py::numpy::int64_array(py, zoom)?;
            Ok(PyTuple::new(py, [lon, lat, zoom])?.unbind().into())
        },
    }
}
