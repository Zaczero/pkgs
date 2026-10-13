use super::*;

/// Capture a coordinate column in owned native-endian storage. Contiguous and
/// simple strided buffers fill the column directly; general inputs use
/// [`float_values`]. The producer must remain quiescent until capture
/// returns; retaining a `PyBuffer` pins its allocation but does not synchronize
/// native writers.
pub(crate) fn coordinate_column_values(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
) -> PyResult<Vec<f64>> {
    reject_masked_array(value)?;
    if let Some(buffer) = try_typed_buffer::<f64>(value)? {
        let item_count = buffer_item_count(&buffer)?;
        reject_coordinate_buffer_rank(buffer.dimensions(), name)?;
        return coordinate_column_values_from_buffer(py, &buffer, item_count);
    }
    float_values(py, value, name, FloatInput::Coordinate)
}

/// Copy an already-admitted `f64` buffer into its final native-endian column.
/// Keeping the admission object across the caller's length checks avoids a
/// second `PyBuffer::get` and preserves the same contiguous/strided safety
/// paths as [`coordinate_column_values`]. `item_count` must come from
/// [`buffer_item_count`] for this buffer.
pub(crate) fn coordinate_column_values_from_buffer(
    py: Python<'_>,
    buffer: &PyBuffer<f64>,
    item_count: usize,
) -> PyResult<Vec<f64>> {
    if buffer.is_c_contiguous() {
        return buffer_to_column_f64(py, buffer, item_count);
    }
    // Strided 1-D (or flattened multi-D): fill the column via a strided walk.
    strided_buffer_to_column_f64(py, buffer, item_count)
}

/// Capture a non-contiguous buffer in an owned column.
///
/// Each element is read through [`pyo3::buffer::ReadOnlyCell`] — never as a
/// plain `&f64` / `&[f64]` over buffer memory. `ReadOnlyCell::get` is a
/// non-atomic load, and the fallback copy is an ordinary copy rather than a
/// synchronization protocol: the producer must stay quiescent until this
/// capture returns. Afterward only the owned column is retained.
fn strided_buffer_to_column_f64(
    py: Python<'_>,
    buffer: &PyBuffer<f64>,
    len: usize,
) -> PyResult<Vec<f64>> {
    use pyo3::buffer::ReadOnlyCell;

    // The strided fast path below reads through a raw pointer. When the
    // exporter does not support direct capture, take the ordinary contiguous
    // copy the suboffset case already falls back to. Neither choice
    // synchronizes a native writer; the producer remains quiescent for both.
    if !buffer_endian::exporter_supports_direct_capture(py, buffer) {
        return buffer_to_vec_f64(py, buffer, len);
    }

    // Indirect (PIL-style) buffers store pointers in the strided slots;
    // `suboffsets[n] >= 0` requires a pointer dereference before adding the
    // suboffset. The flat `buf_ptr + i*stride` arithmetic would read the
    // pointer-table bytes as f64 coordinates. Route through PyO3's safe
    // contiguous copy which honors suboffsets via `PyBuffer_GetPointer`.
    if buffer.suboffsets().is_some() {
        return buffer_to_vec_f64(py, buffer, len);
    }
    // General strides and byte order use PyO3's contiguous copy. A simple
    // native-endian column can fill its destination directly.
    if buffer.dimensions() == 1 && buffer_endian::buffer_format_is_native_endian(buffer.format()) {
        let stride = *buffer
            .strides()
            .first()
            .expect("one-dimensional buffers have one stride");
        let item = isize::try_from(std::mem::size_of::<f64>()).expect("f64 item size fits isize");
        if stride.checked_rem(item) == Some(0) {
            check_1d_byte_stride_span(len, stride)?;
            let step = stride.checked_div(item).expect("f64 item size is nonzero");
            let mut values = vec![0.0; len];
            // ReadOnlyCell is transparent over T; treating each strided slot as
            // a cell matches PyBuffer::as_slice for the contiguous case.
            let base = buffer.buf_ptr().cast::<ReadOnlyCell<f64>>();
            for (i, value) in values.iter_mut().enumerate() {
                // The admission above proved every index fits the signed offset domain.
                let index = i as isize;
                let offset = index.checked_mul(step).ok_or_else(|| {
                    pyo3::exceptions::PyOverflowError::new_err("buffer stride index overflows")
                })?;
                // SAFETY: PyBuffer is held; length and stride were validated;
                // no suboffsets are present, so each step addresses an element
                // in the declared range for CPython and NumPy exporters.
                *value = unsafe { (*base.offset(offset)).get() };
            }
            return Ok(values);
        }
    }
    buffer_to_vec_f64(py, buffer, len)
}

fn check_1d_byte_stride_span(len: usize, stride: isize) -> PyResult<()> {
    let Some(last_index) = len.checked_sub(1) else {
        return Ok(());
    };
    let index = isize::try_from(last_index)
        .map_err(|_| pyo3::exceptions::PyOverflowError::new_err("buffer stride index overflows"))?;
    index.checked_mul(stride).ok_or_else(|| {
        pyo3::exceptions::PyOverflowError::new_err("buffer stride index overflows")
    })?;
    Ok(())
}

/// Copy a `f64` buffer through PyO3's layout-aware path, then de-interleave it.
fn coordseq_from_copied_nd_buffer(
    py: Python<'_>,
    buffer: &PyBuffer<f64>,
    item_count: usize,
    point_count: usize,
    d: usize,
    shape: CoordSeqShape,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
) -> PyResult<CoordSeqBuffer> {
    let flat = buffer_to_vec_f64(py, buffer, item_count)?;
    let mut xs = Vec::with_capacity(point_count);
    let mut ys = Vec::with_capacity(point_count);
    let mut zs_col = (d >= 3).then(|| Vec::with_capacity(point_count));
    let mut ms_col = (d >= 4).then(|| Vec::with_capacity(point_count));
    // `buffer_item_count` admitted the shape product, and this copy preserves
    // its exact item count, so `chunks_exact` covers every coordinate.
    for row in flat.chunks_exact(d) {
        let mut ordinates = row.iter().copied();
        xs.push(
            ordinates
                .next()
                .expect("coordinate rows have at least two axes"),
        );
        ys.push(ordinates.next().expect("N×D rows have at least two axes"));
        if let Some(zs) = zs_col.as_mut() {
            zs.push(ordinates.next().expect("three-axis rows carry Z"));
        }
        if let Some(ms) = ms_col.as_mut() {
            ms.push(ordinates.next().expect("four-axis rows carry M"));
        }
    }
    let mut zs = zs_col;
    let mut ms = ms_col;
    if let Some(z_col) = z {
        zs = Some(float_values_exact(
            z_col.py(),
            z_col,
            "z",
            FloatInput::Coordinate,
            point_count,
            |got| {
                InvalidGeometryError::new_err(format!(
                    "z must have the same length as x/y coordinates (got length {got})"
                ))
            },
        )?);
    }
    if let Some(m_col) = m {
        ms = Some(float_values_exact(
            m_col.py(),
            m_col,
            "m",
            FloatInput::Coordinate,
            point_count,
            |got| {
                InvalidGeometryError::new_err(format!(
                    "m must have the same length as x/y coordinates (got length {got})"
                ))
            },
        )?);
    }
    Ok(CoordSeqBuffer {
        coordinates: CoordSeq::try_from_vecs(xs, ys, zs, ms)?,
        shape,
    })
}

/// Capture a typed coordinate buffer into SoA columns, retaining the regular
/// dimensions above its final ordinate axis for Rust-side row slicing.
///
/// `z`/`m` optional columns override inline ordinates and cannot combine with
/// D>2 buffers (same rule as [`crate::py::support::extract::extract_points`]).
pub(crate) fn try_coordseq_from_nd_buffer(
    value: &Bound<'_, PyAny>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
    mode: CoordSeqBufferMode,
) -> PyResult<Option<CoordSeqBuffer>> {
    reject_masked_array(value)?;
    if matches!(mode, CoordSeqBufferMode::GeometryArray(_))
        && value.cast_exact::<PyUntypedArray>().is_err()
    {
        return Ok(None);
    }
    let Some(buffer) = try_typed_buffer::<f64>(value)? else {
        return Ok(None);
    };
    let item_count = buffer_item_count(&buffer)?;
    let dimensions = buffer.dimensions();
    if !mode.accepts_rank(dimensions) {
        return Ok(None);
    }
    let Some((&ordinate_count, leading_shape)) = buffer.shape().split_last() else {
        return Ok(None);
    };
    if !(2..=4).contains(&ordinate_count) {
        return Ok(None);
    }
    let Some(shape) = CoordSeqShape::from_slice(leading_shape) else {
        return Ok(None);
    };
    let point_count = leading_shape
        .iter()
        .try_fold(1_usize, |total, &length| total.checked_mul(length))
        .ok_or(crate::error::ResourceError::SizeOverflow)
        .map_err(crate::error::Error::from)?;
    if dimensions > 2 && (z.is_some() || m.is_some()) {
        return Ok(None);
    }
    if ordinate_count > 2 && (z.is_some() || m.is_some()) {
        return Err(InvalidGeometryError::new_err(
            "inline Z/M coordinates cannot be combined with z/m arrays",
        ));
    }
    // Indirect (PIL-style) buffers need pointer indirection via suboffsets.
    // Snapshot through PyO3's safe contiguous copy rather than reading their
    // pointer tables as coordinates.
    if buffer.suboffsets().is_some() {
        return coordseq_from_copied_nd_buffer(
            value.py(),
            &buffer,
            item_count,
            point_count,
            ordinate_count,
            shape,
            z,
            m,
        )
        .map(Some);
    }
    if dimensions > 2 {
        let mut xs = Vec::with_capacity(point_count);
        let mut ys = Vec::with_capacity(point_count);
        let mut zs = (ordinate_count >= 3).then(|| Vec::with_capacity(point_count));
        let mut ms = (ordinate_count >= 4).then(|| Vec::with_capacity(point_count));
        if point_count > 0
            && buffer_endian::exporter_supports_direct_capture(value.py(), &buffer)
            && let Some(cells) = buffer.as_slice(value.py())
        {
            let swap = !buffer_endian::buffer_format_is_native_endian(buffer.format());
            for coordinate in cells.chunks_exact(ordinate_count) {
                let mut ordinates = [0.0; 4];
                for (slot, cell) in ordinates.iter_mut().zip(coordinate) {
                    let mut ordinate = cell.get();
                    if swap {
                        ordinate = buffer_endian::swap_f64_endian(ordinate);
                    }
                    *slot = ordinate;
                }
                let [
                    first_ordinate,
                    second_ordinate,
                    third_ordinate,
                    fourth_ordinate,
                ] = ordinates;
                xs.push(first_ordinate);
                ys.push(second_ordinate);
                if let Some(zs) = &mut zs {
                    zs.push(third_ordinate);
                }
                if let Some(ms) = &mut ms {
                    ms.push(fourth_ordinate);
                }
            }
        } else if point_count > 0 {
            return coordseq_from_copied_nd_buffer(
                value.py(),
                &buffer,
                item_count,
                point_count,
                ordinate_count,
                shape,
                None,
                None,
            )
            .map(Some);
        }
        return Ok(Some(CoordSeqBuffer {
            coordinates: CoordSeq::try_from_vecs(xs, ys, zs, ms)?,
            shape,
        }));
    }

    let n = point_count;
    let Some(&[row_stride, col_stride]) = buffer.strides().first_chunk::<2>() else {
        return Ok(None);
    };
    let item = isize::try_from(std::mem::size_of::<f64>()).expect("f64 item size fits isize");
    if row_stride.checked_rem(item) != Some(0) || col_stride.checked_rem(item) != Some(0) {
        return Ok(None);
    }
    let row_step = row_stride
        .checked_div(item)
        .expect("f64 item size is nonzero");
    let col_step = col_stride
        .checked_div(item)
        .expect("f64 item size is nonzero");
    // The direct stride walk and the ordinary copy both require a quiescent
    // producer; the copy path is not a native-writer synchronization protocol.
    if !buffer_endian::exporter_supports_direct_capture(value.py(), &buffer) {
        return coordseq_from_copied_nd_buffer(
            value.py(),
            &buffer,
            item_count,
            point_count,
            ordinate_count,
            shape,
            z,
            m,
        )
        .map(Some);
    }
    check_2d_byte_stride_corners(n, ordinate_count, row_stride, col_stride)?;
    let swap = !buffer_endian::buffer_format_is_native_endian(buffer.format());
    let fill_column = |col: usize| -> crate::error::Result<Vec<f64>> {
        use pyo3::buffer::ReadOnlyCell;

        let mut values = vec![0.0; n];
        // Load each element through ReadOnlyCell rather than forming a slice
        // over foreign storage. The loads are non-atomic; the producer must
        // stay quiescent for this finite capture.
        let base = buffer.buf_ptr().cast::<ReadOnlyCell<f64>>();
        let column = col as isize;
        for (i, value) in values.iter_mut().enumerate() {
            // The preceding corner admission proves the enumerated offsets fit.
            let row = i as isize;
            let offset = row
                .checked_mul(row_step)
                .and_then(|row| row.checked_add(column.checked_mul(col_step)?))
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            // SAFETY: buffer is held; shape/strides and `col < ordinate_count` were
            // validated; no suboffsets are present, so this is in the declared
            // range. The provider must remain quiescent during capture.
            let mut ordinate = unsafe { (*base.offset(offset)).get() };
            if swap {
                ordinate = buffer_endian::swap_f64_endian(ordinate);
            }
            *value = ordinate;
        }
        Ok(values)
    };

    let xs = fill_column(0).map_err(pyo3::PyErr::from)?;
    let ys = match fill_column(1) {
        Ok(values) => values,
        Err(error) => {
            drop(xs);
            return Err(pyo3::PyErr::from(error));
        },
    };
    let mut zs = match (ordinate_count >= 3).then(|| fill_column(2)).transpose() {
        Ok(values) => values,
        Err(error) => {
            drop((xs, ys));
            return Err(pyo3::PyErr::from(error));
        },
    };
    let mut ms = match (ordinate_count >= 4).then(|| fill_column(3)).transpose() {
        Ok(values) => values,
        Err(error) => {
            drop((xs, ys, zs));
            return Err(pyo3::PyErr::from(error));
        },
    };
    if let Some(z_col) = z {
        zs = Some(float_values_exact(
            z_col.py(),
            z_col,
            "z",
            FloatInput::Coordinate,
            point_count,
            |got| {
                InvalidGeometryError::new_err(format!(
                    "z must have the same length as x/y coordinates (got length {got})"
                ))
            },
        )?);
    }
    if let Some(m_col) = m {
        ms = Some(float_values_exact(
            m_col.py(),
            m_col,
            "m",
            FloatInput::Coordinate,
            point_count,
            |got| {
                InvalidGeometryError::new_err(format!(
                    "m must have the same length as x/y coordinates (got length {got})"
                ))
            },
        )?);
    }
    Ok(Some(CoordSeqBuffer {
        coordinates: CoordSeq::try_from_vecs(xs, ys, zs, ms)?,
        shape,
    }))
}

fn check_2d_byte_stride_corners(
    rows: usize,
    columns: usize,
    row_stride: isize,
    column_stride: isize,
) -> PyResult<()> {
    let (Some(last_row), Some(last_column)) = (rows.checked_sub(1), columns.checked_sub(1)) else {
        return Ok(());
    };
    let last_row = isize::try_from(last_row)
        .map_err(|_| pyo3::exceptions::PyOverflowError::new_err("buffer stride index overflows"))?;
    let last_column = isize::try_from(last_column)
        .map_err(|_| pyo3::exceptions::PyOverflowError::new_err("buffer stride index overflows"))?;
    let row_offset = last_row.checked_mul(row_stride).ok_or_else(|| {
        pyo3::exceptions::PyOverflowError::new_err("buffer stride index overflows")
    })?;
    let column_offset = last_column.checked_mul(column_stride).ok_or_else(|| {
        pyo3::exceptions::PyOverflowError::new_err("buffer stride index overflows")
    })?;
    row_offset.checked_add(column_offset).ok_or_else(|| {
        pyo3::exceptions::PyOverflowError::new_err("buffer stride index overflows")
    })?;
    Ok(())
}

/// Like [`coordinate_column_values`], but stop after `expected + 1` items when
/// the length is known from the receiver (D11: `with_coordinates`, …).
pub(crate) fn coordinate_column_values_exact(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
    expected: usize,
    on_len: impl FnOnce(usize) -> PyErr,
) -> PyResult<Vec<f64>> {
    reject_masked_array(value)?;
    if let Some(buffer) = try_typed_buffer::<f64>(value)? {
        let got = buffer_item_count(&buffer)?;
        reject_coordinate_buffer_rank(buffer.dimensions(), name)?;
        if got != expected {
            return Err(on_len(got));
        }
        return coordinate_column_values_from_buffer(py, &buffer, got);
    }
    float_values_exact(py, value, name, FloatInput::Coordinate, expected, on_len)
}

/// Optional `z`/`m` column for the columnar constructor path — the
/// [`coordinate_column_values`] sibling of [`optional_coordinates`].
pub(crate) fn optional_coordinate_column_values(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
    expected_len: usize,
    name: &str,
) -> PyResult<Option<Vec<f64>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_none() {
        return Ok(None);
    }
    let values = coordinate_column_values_exact(py, value, name, expected_len, |got| {
        InvalidGeometryError::new_err(format!(
            "{name} must have the same length as x/y coordinates (got length {got})"
        ))
    })?;
    Ok(Some(values))
}

/// Length-check and copy an already-admitted optional `f64` column.
pub(crate) fn optional_coordinate_column_values_from_buffer(
    py: Python<'_>,
    buffer: Option<&PyBuffer<f64>>,
    expected_len: usize,
    name: &str,
) -> PyResult<Option<Vec<f64>>> {
    let Some(buffer) = buffer else {
        return Ok(None);
    };
    let got = buffer_item_count(buffer)?;
    if got != expected_len {
        return Err(InvalidGeometryError::new_err(format!(
            "{name} must have the same length as x/y coordinates (got length {got})"
        )));
    }
    coordinate_column_values_from_buffer(py, buffer, got).map(Some)
}
