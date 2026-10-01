#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use crate::boundary::buffer_endian::{buffer_item_count, try_typed_buffer};
use crate::py::functions::constructors::{
    Bound, CoordSeq, Frame, GeometryError, InvalidGeometryError, PyAny, PyAnyMethods as _,
    PyGeometry, PyGeometryArray, PyResult, Python, Typed, broadcast_coordinate_group,
    broadcast_crs_coordinate_inputs, build_box_shape, build_geometry_array,
    coordinate_column_values_from_buffer, coordinate_epoch_option, coordinate_inputs_are_scalar,
    finite_coordinate_required, line_string_from_data_item, multi_line_string_from_data_item,
    multi_point_from_data_item, multi_polygon_from_data_item,
    optional_coordinate_column_values_from_buffer, parse_crs, parse_crs_epoch,
    polygon_from_data_item, pyfunction,
};
use crate::py::numpy;

/// Create a rectangular ``Polygon`` from bounds ``(minx, miny, maxx, maxy)``.
///
/// Parameters
/// ----------
/// minx, miny, maxx, maxy : float
///     Finite rectangle bounds; each minimum must not exceed its maximum,
///     except that ``minx > maxx`` is allowed with ``wrap='split'`` to wrap a
///     geographic box across the antimeridian.
///
/// crs : str or int, optional
///     CRS label (EPSG code, authority string, or WKT); attached as metadata,
///     coordinates are not transformed.
///     With a geographic CRS, horizontal sides approximate constant-latitude
///     parallels in the CRS-native angular coordinates using equal-longitude
///     chord vertices, rather than corner-to-corner geodesics. See the
///     [CRS, units & measurement guide](https://gometry.monicz.dev/guide/crs/)
///     for the native-range scope and measured fidelity of this approximation.
///
/// wrap : {'split'}, optional
///     Antimeridian handling for geographic (EPSG:4326) boxes. ``'split'``
///     lets ``minx`` exceed ``maxx`` to span the 180° meridian, returning a
///     `MultiPolygon` split at the antimeridian. The default (``None``) leaves
///     coordinates unwrapped and requires ``minx <= maxx``.
///
/// ccw : bool, default True
///     If ``True`` the ring is counter-clockwise; ``False`` makes it clockwise.
///
/// epoch : float, optional
///     Coordinate epoch (decimal year) for time-dependent frames.
///
/// Returns
/// -------
/// Polygon | MultiPolygon
///     A rectangular polygon, or a MultiPolygon when ``wrap='split'`` spans
///     the antimeridian.
///
/// Raises
/// ------
/// InvalidGeometryError
///     If a bound is non-finite or a minimum exceeds its maximum without
///     ``wrap='split'``.
/// CRSError
///     If ``epoch`` is set without ``crs``, or ``wrap='split'`` is used
///     without ``crs=4326``.
/// GeometryError
///     If ``epoch`` is not a finite decimal year.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.box(0, 0, 2, 1).to_wkt()
/// 'POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))'
/// >>> gm.box(0, 0, 2, 1, ccw=False).to_wkt()
/// 'POLYGON ((0 0, 0 1, 2 1, 2 0, 0 0))'
#[pyfunction(name = "box")]
#[pyo3(
    signature = (minx, miny, maxx, maxy, *, crs = None, wrap = None, ccw = true, epoch = None)
)]
pub(super) fn box_(
    minx: &Bound<'_, PyAny>,
    miny: &Bound<'_, PyAny>,
    maxx: &Bound<'_, PyAny>,
    maxy: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    wrap: Option<&str>,
    ccw: bool,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Typed> {
    let minx = finite_coordinate_required("minx", minx)?;
    let miny = finite_coordinate_required("miny", miny)?;
    let maxx = finite_coordinate_required("maxx", maxx)?;
    let maxy = finite_coordinate_required("maxy", maxy)?;
    let crs = parse_crs(crs)?;
    let epoch = coordinate_epoch_option("epoch", epoch)?;
    let shape =
        build_box_shape(minx, miny, maxx, maxy, wrap, crs.as_ref())?.orient_polygons(!ccw)?;
    Ok(Typed(PyGeometry::with_frame(
        shape,
        Frame::new(crs, epoch)?,
    )))
}

/// Create a ``GeometryArray`` of points from parallel coordinate columns.
///
/// Taking separate ``x`` and ``y`` (not interleaved tuples) keeps axis order
/// explicit and avoids the lon/lat-vs-lat/lon footgun. Each column accepts a
/// scalar (broadcast to every row) or a sequence of floats (one per point).
///
/// Parameters
/// ----------
/// x, y : float or sequence of float
///     X and Y ordinates. At least one must be sequence of float to set the row count;
///     scalars broadcast numpy-style.
///
/// z, m : float or sequence of float, optional
///     Z and M ordinates, broadcast like ``x``/``y``.
///
/// crs : str or int, optional
///     CRS label (EPSG code, authority string, or WKT); attached as metadata,
///     coordinates are not transformed.
///
/// epoch : float, optional
///     Coordinate epoch (decimal year) for time-dependent frames.
///
/// Returns
/// -------
/// GeometryArray
///     One Point per coordinate.
///
/// Raises
/// ------
/// InvalidGeometryError
///     If columns differ in length or are non-finite.
/// CRSError
///     If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
/// GeometryError
///     If every argument is scalar (use `Point`) or ``epoch`` is invalid.
///
/// See Also
/// --------
/// Point : Build a single point.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.points([0, 1], [0, 1]).to_wkt()
/// ['POINT (0 0)', 'POINT (1 1)']
#[pyfunction]
#[pyo3(signature = (x, y, *, z = None, m = None, crs = None, epoch = None))]
pub(super) fn points(
    py: Python<'_>,
    x: &Bound<'_, PyAny>,
    y: &Bound<'_, PyAny>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometryArray> {
    for (name, value) in [("x", x), ("y", y)] {
        crate::reject_multidimensional_coordinate_array(value, name)?;
    }
    for (name, value) in [("z", z), ("m", m)]
        .into_iter()
        .filter_map(|(name, value)| {
            value
                .filter(|value| !value.is_none())
                .map(|value| (name, value))
        })
    {
        crate::reject_multidimensional_coordinate_array(value, name)?;
    }
    // Contiguous non-scalar buffers → final Arc columns (Item 12). Scalar
    // broadcast and arbitrary iterators keep the CoordinateInput path below.
    if let Some(seq) = try_points_from_arc_columns(py, x, y, z, m)? {
        let frame = parse_crs_epoch(crs, epoch)?;
        return Ok(PyGeometryArray::packed_points(seq, frame)?);
    }
    // Sibling length pin (D11): a list/buffer column establishes the count so
    // a bare `itertools.repeat` sibling rejects instantly instead of hanging.
    let mut established = crate::coordinate_sequence_len_hint(x, "x")?;
    if established.is_none() {
        established = crate::coordinate_sequence_len_hint(y, "y")?;
    }
    if established.is_none()
        && let Some(value) = z
    {
        established = crate::coordinate_sequence_len_hint(value, "z")?;
    }
    if established.is_none()
        && let Some(value) = m
    {
        established = crate::coordinate_sequence_len_hint(value, "m")?;
    }
    let mut x = crate::coordinate_input_with_expected(py, x, "x", established, &|| {
        "x must be finite".into()
    })?;
    let established = established.or_else(|| (!x.scalar).then_some(x.values.len()));
    let mut y = crate::coordinate_input_with_expected(py, y, "y", established, &|| {
        "y must be finite".into()
    })?;
    let established = established.or_else(|| (!y.scalar).then_some(y.values.len()));
    let mut z = crate::optional_coordinate_input_with_expected(py, z, "z", established)?;
    let established =
        established.or_else(|| z.as_ref().filter(|c| !c.scalar).map(|c| c.values.len()));
    let mut m = crate::optional_coordinate_input_with_expected(py, m, "m", established)?;
    if coordinate_inputs_are_scalar(&x, &y, z.as_ref(), m.as_ref()) {
        return Err(GeometryError::new_err(
            "points expects coordinate columns; use Point(x, y) for a single point",
        ));
    }
    broadcast_crs_coordinate_inputs(&mut x, &mut y, &mut z, &mut m)?;
    let frame = parse_crs_epoch(crs, epoch)?;
    let seq = CoordSeq::from_owned_columns(
        x.values,
        y.values,
        z.map(|value| value.values),
        m.map(|value| value.values),
    )?;
    Ok(PyGeometryArray::packed_points(seq, frame)?)
}

/// Fast path for `points(x, y, …)` when every supplied column is a real f64
/// buffer (NumPy/memoryview) — builds final Arc columns without the
/// intermediate `CoordinateInput` `Vec` bounce.
///
/// Returns `None` for scalars and sequences without f64 buffers.
fn try_points_from_arc_columns(
    py: Python<'_>,
    x: &Bound<'_, PyAny>,
    y: &Bound<'_, PyAny>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<CoordSeq>> {
    numpy::reject_masked_array(x)?;
    numpy::reject_masked_array(y)?;
    if let Some(value) = z {
        numpy::reject_masked_array(value)?;
    }
    if let Some(value) = m {
        numpy::reject_masked_array(value)?;
    }
    let Some(x_buffer) = try_typed_buffer::<f64>(x)? else {
        return Ok(None);
    };
    let Some(y_buffer) = try_typed_buffer::<f64>(y)? else {
        return Ok(None);
    };
    let z_buffer = match z {
        Some(value) if !value.is_none() => {
            let Some(buffer) = try_typed_buffer::<f64>(value)? else {
                return Ok(None);
            };
            Some(buffer)
        },
        _ => None,
    };
    let m_buffer = match m {
        Some(value) if !value.is_none() => {
            let Some(buffer) = try_typed_buffer::<f64>(value)? else {
                return Ok(None);
            };
            Some(buffer)
        },
        _ => None,
    };
    let x_len = buffer_item_count(&x_buffer)?;
    let y_len = buffer_item_count(&y_buffer)?;
    if x_len != y_len {
        return Err(InvalidGeometryError::new_err(format!(
            "x and y must have the same length, got {x_len} and {y_len}",
        )));
    }
    let xs = coordinate_column_values_from_buffer(py, &x_buffer, x_len)?;
    let ys = coordinate_column_values_from_buffer(py, &y_buffer, y_len)?;
    let zs = optional_coordinate_column_values_from_buffer(py, z_buffer.as_ref(), x_len, "z")?;
    let ms = optional_coordinate_column_values_from_buffer(py, m_buffer.as_ref(), x_len, "m")?;
    Ok(Some(CoordSeq::from_owned_columns(xs, ys, zs, ms)?))
}

/// Create a ``GeometryArray`` of rectangular polygons from bound columns.
///
/// Parameters
/// ----------
/// minx, miny, maxx, maxy : float or sequence of float
///     Rectangle bounds per row. At least one must be sequence of float; scalars
///     broadcast numpy-style.
///
/// crs : str or int, optional
///     CRS applied to every box. With a geographic CRS, horizontal sides
///     approximate constant-latitude parallels in the CRS-native angular
///     coordinates using equal-longitude chord vertices. See the
///     [CRS, units & measurement guide](https://gometry.monicz.dev/guide/crs/)
///     for the native-range scope and measured fidelity of this approximation.
///
/// wrap : {'split'}, optional
///     Antimeridian handling for geographic (EPSG:4326) boxes — same as `box`.
///
/// ccw : bool, default True
///     If ``True`` each ring is counter-clockwise; ``False`` makes it clockwise.
///
/// epoch : float, optional
///     Coordinate epoch (decimal year) for time-dependent frames.
///
/// Returns
/// -------
/// GeometryArray
///     One `Polygon` or `MultiPolygon` per bound tuple.
///
/// Raises
/// ------
/// InvalidGeometryError
///     If bounds are non-finite or invalid per row.
/// CRSError
///     If ``epoch`` is set without ``crs``, or ``wrap='split'`` is used
///     without ``crs=4326``.
/// GeometryError
///     If every argument is scalar (use `box`) or ``epoch`` is invalid.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.boxes(0, 0, [1, 2], [1, 2]).to_wkt()
/// ['POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))']
#[pyfunction]
#[pyo3(signature = (minx, miny, maxx, maxy, *, crs = None, wrap = None, ccw = true, epoch = None))]
pub(super) fn boxes(
    py: Python<'_>,
    minx: &Bound<'_, PyAny>,
    miny: &Bound<'_, PyAny>,
    maxx: &Bound<'_, PyAny>,
    maxy: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    wrap: Option<&str>,
    ccw: bool,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometryArray> {
    // A known sibling length pins a bare iterator before any other column is
    // drained, so paired infinite inputs stop at the first excess item.
    let mut expected = crate::coordinate_sequence_len_hint(minx, "minx")?;
    if expected.is_none() {
        expected = crate::coordinate_sequence_len_hint(miny, "miny")?;
    }
    if expected.is_none() {
        expected = crate::coordinate_sequence_len_hint(maxx, "maxx")?;
    }
    if expected.is_none() {
        expected = crate::coordinate_sequence_len_hint(maxy, "maxy")?;
    }
    let mut minx = crate::coordinate_input_with_expected(py, minx, "minx", expected, &|| {
        "minx must be finite".into()
    })?;
    let expected = expected.or_else(|| (!minx.scalar).then_some(minx.values.len()));
    let mut miny = crate::coordinate_input_with_expected(py, miny, "miny", expected, &|| {
        "miny must be finite".into()
    })?;
    let expected = expected.or_else(|| (!miny.scalar).then_some(miny.values.len()));
    let mut maxx = crate::coordinate_input_with_expected(py, maxx, "maxx", expected, &|| {
        "maxx must be finite".into()
    })?;
    let expected = expected.or_else(|| (!maxx.scalar).then_some(maxx.values.len()));
    let mut maxy = crate::coordinate_input_with_expected(py, maxy, "maxy", expected, &|| {
        "maxy must be finite".into()
    })?;
    if coordinate_inputs_are_scalar(&minx, &miny, Some(&maxx), Some(&maxy)) {
        return Err(GeometryError::new_err(
            "boxes expects bound columns; use box(minx, miny, maxx, maxy) for a single rectangle",
        ));
    }
    broadcast_coordinate_group(
        [
            (&mut minx, "minx"),
            (&mut miny, "miny"),
            (&mut maxx, "maxx"),
            (&mut maxy, "maxy"),
        ],
        "minx, miny, maxx, and maxy",
    )?;
    let frame = parse_crs_epoch(crs, epoch)?;
    // The broadcast coordinate columns establish the exact output-row count.
    let mut rows = crate::array::StreamingShapes::with_capacity(0, minx.values.len());
    for (((minx, miny), maxx), maxy) in minx
        .values
        .iter()
        .zip(miny.values.iter())
        .zip(maxx.values.iter())
        .zip(maxy.values.iter())
    {
        let shape = build_box_shape(*minx, *miny, *maxx, *maxy, wrap, frame.crs_ref())?
            .orient_polygons(!ccw)?;
        rows.try_push(shape)?;
    }
    Ok(rows.finish(frame)?)
}

/// Create a ``GeometryArray`` of linestrings from per-line coordinate inputs.
///
/// Parameters
/// ----------
/// values : sequence
///     Each member is a raw coordinate sequence accepted by `LineString`.
///
/// crs : str or int, optional
///     CRS applied to every linestring.
///
/// epoch : float, optional
///     Coordinate epoch (decimal year) for time-dependent frames.
///
/// Returns
/// -------
/// GeometryArray
///     One `LineString` per input sequence.
///
/// Raises
/// ------
/// InvalidGeometryError
///     If a member line has fewer than two vertices or non-finite coordinates.
/// TypeError
///     If a member is an already-built geometry; use `GeometryArray(values)`.
/// CRSError
///     If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
/// GeometryError
///     If ``epoch`` is not a finite decimal year.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> lines = gm.line_strings([[(0, 0), (1, 1)], [(2, 2), (3, 3)]])
/// >>> len(lines)
/// 2
#[pyfunction]
#[pyo3(signature = (values, *, crs = None, epoch = None))]
pub(super) fn line_strings(
    values: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometryArray> {
    build_geometry_array(
        values,
        crs,
        epoch,
        crate::CoordSeqStructure::Sequence,
        line_string_from_data_item,
    )
}

/// Create a ``GeometryArray`` of polygons from per-polygon ring inputs.
///
/// Parameters
/// ----------
/// values : sequence
///     Each member is a raw shell coordinate sequence or ``[shell, *holes]``.
///
/// crs : str or int, optional
///     CRS applied to every polygon.
///
/// epoch : float, optional
///     Coordinate epoch (decimal year) for time-dependent frames.
///
/// Returns
/// -------
/// GeometryArray
///     One `Polygon` per input.
///
/// Raises
/// ------
/// InvalidGeometryError
///     If a ring has fewer than three corners or non-finite coordinates.
/// TypeError
///     If a member is an already-built geometry; use `GeometryArray(values)`.
/// CRSError
///     If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
/// GeometryError
///     If ``epoch`` is not a finite decimal year.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> polys = gm.polygons([[(0, 0), (1, 0), (1, 1), (0, 1)]])
/// >>> len(polys)
/// 1
#[pyfunction]
#[pyo3(signature = (values, *, crs = None, epoch = None))]
pub(super) fn polygons(
    values: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometryArray> {
    build_geometry_array(
        values,
        crs,
        epoch,
        crate::CoordSeqStructure::Polygon,
        polygon_from_data_item,
    )
}

/// Create a ``GeometryArray`` of multipoints from per-multipoint inputs.
///
/// Parameters
/// ----------
/// values : sequence
///     Each member is a raw coordinate sequence accepted by `MultiPoint`.
///
/// crs : str or int, optional
///     CRS applied to every multipoint.
///
/// epoch : float, optional
///     Coordinate epoch (decimal year) for time-dependent frames.
///
/// Returns
/// -------
/// GeometryArray
///     One `MultiPoint` per input.
///
/// Raises
/// ------
/// InvalidGeometryError
///     If any coordinate is non-finite or has mixed dimensionality.
/// TypeError
///     If a member is an already-built geometry; use `GeometryArray(values)`.
/// CRSError
///     If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
/// GeometryError
///     If ``epoch`` is not a finite decimal year.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.multi_points([[(0, 0), (1, 1)]]).to_wkt()
/// ['MULTIPOINT ((0 0), (1 1))']
#[pyfunction]
#[pyo3(signature = (values, *, crs = None, epoch = None))]
pub(super) fn multi_points(
    values: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometryArray> {
    build_geometry_array(
        values,
        crs,
        epoch,
        crate::CoordSeqStructure::Sequence,
        multi_point_from_data_item,
    )
}

/// Create a ``GeometryArray`` of multilinestrings from per-multiline inputs.
///
/// Parameters
/// ----------
/// values : sequence
///     Each member is raw line coordinate sequences accepted by `MultiLineString`.
///
/// crs : str or int, optional
///     CRS applied to every multilinestring.
///
/// epoch : float, optional
///     Coordinate epoch (decimal year) for time-dependent frames.
///
/// Returns
/// -------
/// GeometryArray
///     One `MultiLineString` per input.
///
/// Raises
/// ------
/// InvalidGeometryError
///     If a member line has fewer than two vertices or non-finite coordinates.
/// TypeError
///     If a member is an already-built geometry; use `GeometryArray(values)`.
/// CRSError
///     If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
/// GeometryError
///     If ``epoch`` is not a finite decimal year.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.multi_line_strings([[[(0, 0), (1, 1)]]]).to_wkt()
/// ['MULTILINESTRING ((0 0, 1 1))']
#[pyfunction]
#[pyo3(signature = (values, *, crs = None, epoch = None))]
pub(super) fn multi_line_strings(
    values: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometryArray> {
    build_geometry_array(
        values,
        crs,
        epoch,
        crate::CoordSeqStructure::MultiLineString,
        multi_line_string_from_data_item,
    )
}

/// Create a ``GeometryArray`` of multipolygons from per-multipolygon inputs.
///
/// Parameters
/// ----------
/// values : sequence
///     Each member is raw polygon coordinate sequences accepted by `MultiPolygon`.
///
/// crs : str or int, optional
///     CRS applied to every multipolygon.
///
/// epoch : float, optional
///     Coordinate epoch (decimal year) for time-dependent frames.
///
/// Returns
/// -------
/// GeometryArray
///     One `MultiPolygon` per input.
///
/// Raises
/// ------
/// InvalidGeometryError
///     If any ring has fewer than three corners or non-finite coordinates.
/// TypeError
///     If a member is an already-built geometry; use `GeometryArray(values)`.
/// CRSError
///     If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
/// GeometryError
///     If ``epoch`` is not a finite decimal year.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.multi_polygons([[[[(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]]]]).to_wkt()
/// ['MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))']
#[pyfunction]
#[pyo3(signature = (values, *, crs = None, epoch = None))]
pub(super) fn multi_polygons(
    values: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometryArray> {
    build_geometry_array(
        values,
        crs,
        epoch,
        crate::CoordSeqStructure::MultiPolygon,
        multi_polygon_from_data_item,
    )
}
