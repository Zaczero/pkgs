use super::*;
use crate::boundary::reserve::PYTHON_SIGNAL_STRIDE;

pub(in crate::py::functions::constructors) fn build_geometry_array(
    values: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
    kind: GeometryKind,
) -> PyResult<PyGeometryArray> {
    let structure = match kind {
        GeometryKind::LineString | GeometryKind::MultiPoint => crate::CoordSeqStructure::Sequence,
        GeometryKind::Polygon => crate::CoordSeqStructure::Polygon,
        GeometryKind::MultiLineString => crate::CoordSeqStructure::MultiLineString,
        GeometryKind::MultiPolygon => crate::CoordSeqStructure::MultiPolygon,
        GeometryKind::Point | GeometryKind::GeometryCollection => {
            unreachable!("coordinate-array factories construct sequences or polygons")
        },
    };
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
        if row_count == 0 {
            return Ok(rows.finish(frame)?);
        }
        return Ok(values
            .py()
            .detach(|| -> PyResult<crate::error::Result<_>> {
                for row in 0..row_count {
                    if row.is_multiple_of(PYTHON_SIGNAL_STRIDE)
                        && (row != 0 || !first_signal_checked)
                    {
                        pyo3::Python::attach(|py| crate::check_python_signals(py, row))?;
                    }
                    if let Err(error) = buffer
                        .row(row)
                        .and_then(|buffer| data::geometry_from_buffer(buffer, kind))
                        .and_then(|shape| rows.try_push(shape))
                    {
                        return Ok(Err(error));
                    }
                }
                Ok(rows.finish(frame))
            })??);
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
        let (shape, row_frame) = if let Some(buffer) = buffer {
            (data::geometry_from_buffer(buffer, kind)?, Frame::None)
        } else {
            data::geometry_from_data_item(&item_value, kind)?
        };
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
    if row_count == 0 {
        return Ok(rows.finish(frame)?);
    }
    Ok(values.py().detach(|| -> crate::error::Result<_> {
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
        rows.finish(frame)
    })?)
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
    if members.is_empty() {
        return Ok(PyGeometry::new_with_crs_epoch(
            empty_geometry_collection(),
            explicit_crs,
            explicit_epoch,
        )?);
    }
    Ok(geometries.py().detach(|| -> crate::error::Result<_> {
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
        PyGeometry::new_with_crs_epoch(
            Shape::geometry_collection(items)?,
            frame.crs_owned(),
            frame.epoch(),
        )
    })?)
}
