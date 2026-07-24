use crate::py::arrow::*;

pub(crate) fn geometries_from_pyarrow(
    py: Python<'_>,
    pa: &Bound<'_, PyModule>,
    value: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let input = arrow_geometry_input(pa, value)?;
    let value = input.value.bind(py);
    let field = input.field.as_ref().map(|field| field.bind(py));

    // m09 / R04 stream parity: zero-length chunks are never retained.
    // Total row count is authoritative — a ChunkedArray of N empty chunks has
    // len==0. We still validate each empty chunk's start offset (D18/N1) but
    // discard without retaining storage (O(1) product, not O(chunk-count)).
    // Non-empty inputs walk chunks once and keep only non-empty storages.
    if value.len()? == 0 {
        // D18/N1: every empty physical chunk retains one start offset that must
        // be non-negative — including multi-chunk all-empty ChunkedArrays.
        ensure_empty_pyarrow_value_offsets(py, pa, value, field)?;
        return empty_pyarrow_frame(py, pa, value, field, crs, epoch);
    }

    let mut storages = Vec::new();
    if let Ok(chunk_iterable) = value.getattr("chunks") {
        for chunk in chunk_iterable.try_iter()? {
            let chunk = chunk?;
            if chunk.len()? == 0 {
                // D18: discard empty chunks only after validating their start
                // offset; retention stays O(1) (no storage kept).
                ensure_empty_pyarrow_chunk_offsets(py, pa, &chunk, field)?;
                continue;
            }
            crate::try_push(&mut storages, arrow_storage_array(pa, &chunk, field)?)?;
        }
        // len>0 but every chunk empty is impossible for a well-formed ChunkedArray;
        // if it happens, fall through to the empty-frame product.
        if storages.is_empty() {
            return empty_pyarrow_frame(py, pa, value, field, crs, epoch);
        }
    } else {
        crate::try_push(&mut storages, arrow_storage_array(pa, value, field)?)?;
    }
    geometries_from_arrow_storages(py, storages, crs, epoch)
}

/// D18/N1 empty-path offset check for a len==0 pyarrow value.
///
/// Every physical empty chunk with an offset chain is validated (malformed
/// start offsets like `-1` must reject even when total length is 0). Chunk
/// storages are not retained (m09 O(1) product). Point / BinaryView encodings
/// have no offset start slot — after the first chunk classifies as offset-free
/// the remaining empty chunks need no per-chunk walk (m09 many-empty points).
fn ensure_empty_pyarrow_value_offsets(
    py: Python<'_>,
    pa: &Bound<'_, PyModule>,
    value: &Bound<'_, PyAny>,
    field: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    if let Ok(num_chunks) = value.getattr("num_chunks") {
        let n: usize = num_chunks.extract()?;
        if n == 0 {
            return Ok(());
        }
        let first = value.call_method1("chunk", (0,))?;
        let first_storage = arrow_storage_array(pa, &first, field)?;
        ensure_pyarrow_storage_offsets_monotonic(py, &first_storage)?;
        // Shared ChunkedArray type: offset-free encodings need no further walk.
        if empty_chunk_has_no_offset_chain(&first_storage) {
            return Ok(());
        }
        for index in 1..n {
            let chunk = value.call_method1("chunk", (index,))?;
            ensure_empty_pyarrow_chunk_offsets(py, pa, &chunk, field)?;
        }
        return Ok(());
    }
    ensure_empty_pyarrow_chunk_offsets(py, pa, value, field)
}

const fn empty_chunk_has_no_offset_chain(storage: &ArrowStorage) -> bool {
    match storage.encoding {
        GeometryEncoding::Point => true,
        GeometryEncoding::Wkb => matches!(storage.wkb_offset_width, WkbOffsetWidth::View),
        _ => false,
    }
}

fn ensure_empty_pyarrow_chunk_offsets(
    py: Python<'_>,
    pa: &Bound<'_, PyModule>,
    value: &Bound<'_, PyAny>,
    field: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    let storage = arrow_storage_array(pa, value, field)?;
    ensure_pyarrow_storage_offsets_monotonic(py, &storage)
}

fn empty_pyarrow_frame(
    py: Python<'_>,
    pa: &Bound<'_, PyModule>,
    value: &Bound<'_, PyAny>,
    field: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let (embedded_crs, embedded_epoch) = arrow_value_frame(pa, value, field)?;
    let crs = reconcile_arrow_crs(embedded_crs.as_deref(), crs)?;
    let epoch = reconcile_arrow_epoch(embedded_epoch, epoch)?;
    Ok(
        PyGeometryArray::mixed(Vec::new(), Frame::new(crs.map(crs_arc), epoch)?)
            .into_pyobject(py)?
            .unbind()
            .into(),
    )
}
