use super::*;

pub(in crate::py::functions::constructors) fn build_geometry_array<'py>(
    values: &Bound<'py, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
    structure: crate::CoordSeqStructure,
    item: impl Fn(GeometryArrayItem<'py>) -> PyResult<(Shape, Frame)>,
) -> PyResult<PyGeometryArray> {
    numpy::reject_masked_array(values)?;
    // Admit CRS and iterator acquisition before consulting the source's exact
    // length. The count sizes storage only; iteration remains authoritative.
    let frame = parse_crs_epoch(crs, epoch)?;
    let iter = values.try_iter()?;
    let row_capacity = crate::optional_len(values)?.unwrap_or_default();
    let mut rows = crate::array::StreamingShapes::with_capacity(0, row_capacity);
    let first_signal_checked =
        row_capacity > 0 && values.cast_exact::<::numpy::PyUntypedArray>().is_ok();
    if first_signal_checked {
        crate::check_python_signals(values.py(), 0)?;
    }

    if let Some(buffer) = crate::try_coordseq_from_nd_buffer(
        values,
        None,
        None,
        crate::CoordSeqBufferMode::GeometryArray(structure),
    )? {
        let &[row_count, ..] = buffer.shape.as_slice() else {
            unreachable!("a geometry-array buffer has an outer row dimension");
        };
        for row in 0..row_count {
            if row != 0 || !first_signal_checked {
                crate::check_python_signals(values.py(), row)?;
            }
            let (shape, _) = item(GeometryArrayItem::Buffer(buffer.row(row)?))?;
            rows.try_push(shape)?;
        }
        return Ok(rows.finish(frame)?);
    }

    let mut row_frames: Option<Vec<Frame>> = None;
    let mut row_count = 0;
    for (row, item_value) in iter.enumerate() {
        let item_value = item_value?;
        if row != 0 || !first_signal_checked {
            crate::check_python_signals(item_value.py(), row)?;
        }
        let buffer = if item_value.cast_exact::<::numpy::PyUntypedArray>().is_ok() {
            crate::try_coordseq_from_nd_buffer(
                &item_value,
                None,
                None,
                crate::CoordSeqBufferMode::Geometry(structure),
            )?
        } else {
            None
        };
        let data_item = buffer.map_or_else(
            || GeometryArrayItem::Python(item_value),
            GeometryArrayItem::Buffer,
        );
        let (shape, row_frame) = item(data_item)?;
        if let Some(frames) = &mut row_frames {
            frames.push(row_frame);
        } else if !matches!(row_frame, Frame::None) {
            let mut frames = Vec::with_capacity(row_capacity);
            frames.resize(row_count, Frame::None);
            frames.push(row_frame);
            row_frames = Some(frames);
        }
        rows.try_push(shape)?;
        row_count = row_count.wrapping_add(1);
    }
    let frame = if let Some(mut frames) = row_frames {
        metadata::resolve_frame_rows(
            &mut frames,
            crate::FrameAdoption {
                crs: frame.crs_owned(),
                epoch: frame.epoch(),
            },
            "GeometryArray",
        )?
    } else {
        frame
    };
    Ok(rows.finish(frame)?)
}

/// Build a ``GeometryCollection`` from a sequence of geometries.
pub(crate) fn build_geometry_collection(
    geometries: Option<&Bound<'_, PyAny>>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    // No members → `GEOMETRYCOLLECTION EMPTY` (Shapely-compatible
    // `GeometryCollection()`), carrying any explicit crs/epoch.
    let Some(geometries) = geometries else {
        return geometry_with_crs_epoch(empty_geometry_collection(), crs, epoch);
    };
    // Fail-fast on the first missing member: GeometryCollection cannot carry
    // None (unlike GeometryArray rows).
    // The collector retains each member, bounding this logical row counter.
    let mut row = 0_usize;
    let mut members = crate::collect_py_iter(geometries, |item| {
        let this_row = row;
        row = row.wrapping_add(1);
        if item.is_none() {
            return Err(crate::missing_geometry_items_error());
        }
        let Some(geometry) = exact_geometry(&item) else {
            return Err(PyTypeError::new_err(format!(
                "expected Geometry at element {this_row}; strings parse via from_wkt/from_wkb/from_geojson"
            )));
        };
        Ok(geometry.clone())
    })?;
    let explicit_crs = parse_crs(crs)?;
    let explicit_epoch = coordinate_epoch_option("epoch", epoch)?;
    let frame = metadata::resolve_geometry_frames(
        &mut members,
        FrameAdoption {
            crs: explicit_crs,
            epoch: explicit_epoch,
        },
        "GeometryCollection",
    )?;
    let items = members
        .into_iter()
        .map(
            |geometry| match std::sync::Arc::try_unwrap(geometry.shape) {
                Ok(data) => data.into_shape(),
                Err(shared) => shared.shape().clone(),
            },
        )
        .collect();
    Ok(PyGeometry::new_with_crs_epoch(
        Shape::geometry_collection(items)?,
        frame.crs_owned(),
        frame.epoch(),
    )?)
}
