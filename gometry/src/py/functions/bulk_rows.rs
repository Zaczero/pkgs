#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::py::wire_crs::{SharedRowCrs, SridFrameAdmission};
use crate::{Crs, Frame, PyGeometryArray, guard_epoch_frame};

pub(crate) enum StreamedRow {
    Present {
        crs: Option<Crs>,
        epoch: Option<f64>,
    },
    Missing,
}

/// Bulk stream row carrying a raw normalized SRID (`None` = CRS-free).
pub(crate) enum StreamedSridRow {
    Present(Option<u32>),
}

pub(crate) fn stream_bulk<'py>(
    iter: impl Iterator<Item = PyResult<Bound<'py, PyAny>>>,
    fallback: Option<Crs>,
    explicit_epoch: Option<f64>,
    crs_context: Option<&str>,
    mut parse_row: impl FnMut(
        &Bound<'py, PyAny>,
        &mut crate::array::StreamingShapes,
        usize,
    ) -> PyResult<StreamedRow>,
) -> PyResult<PyGeometryArray> {
    let mut rows = crate::array::StreamingShapes::new();
    let mut first_crs = SharedRowCrs::Unseen;
    let mut first_epoch = None;
    let mut have_present_row = false;
    let mut missing_rows: Vec<usize> = Vec::new();
    let mut row_count = 0;
    for (row, item) in iter.enumerate() {
        row_count = row + 1;
        let item = item?;
        crate::check_python_signals(item.py(), row)?;
        if item.is_none() {
            // Final-order kind-preserving placeholder (Stage 3); no later scatter.
            crate::try_push(&mut missing_rows, row)?;
            rows.try_push_missing()?;
            continue;
        }
        match parse_row(&item, &mut rows, row).map_err(|err| crate::note_array_row(err, row))? {
            StreamedRow::Present {
                crs: row_crs,
                epoch,
            } => {
                if let Some(context) = crs_context {
                    first_crs
                        .admit(row_crs, fallback.as_ref(), row, context)
                        .map_err(|err| crate::note_array_row(err, row))?;
                }
                if let Some(explicit) = explicit_epoch
                    && let Some(row_epoch) = epoch
                    && !crate::boundary::metadata::epochs_equal(row_epoch, explicit)
                {
                    return Err(crate::error::Error::from(
                        crate::boundary::metadata::FrameError::SharedEpoch {
                            context: crs_context.unwrap_or("GeometryArray").to_owned().into(),
                            index: row,
                            first: Some(explicit),
                            other: Some(row_epoch),
                        },
                    )
                    .into());
                }
                if explicit_epoch.is_none() {
                    if !have_present_row {
                        first_epoch = epoch;
                        have_present_row = true;
                    } else if first_epoch != epoch {
                        return Err(crate::error::Error::from(
                            crate::boundary::metadata::FrameError::SharedEpoch {
                                context: crs_context.unwrap_or("GeometryArray").to_owned().into(),
                                index: row,
                                first: first_epoch,
                                other: epoch,
                            },
                        )
                        .into());
                    }
                }
            },
            StreamedRow::Missing => {
                crate::try_push(&mut missing_rows, row)?;
                rows.try_push_missing()?;
            },
        }
    }
    let crs = if crs_context.is_some() {
        first_crs.into_crs(fallback)
    } else {
        fallback
    };
    let epoch = explicit_epoch.or(first_epoch);
    guard_epoch_frame(epoch, crs.as_ref())?;
    let array = rows.finish(Frame::new(crs, epoch)?);
    if let Some(mask) = crate::array::sparse_missing_mask(row_count, &missing_rows) {
        Ok(array.with_missing_mask(Some(mask)))
    } else {
        Ok(array)
    }
}

/// Bulk stream with numeric SRID admission (WKB/EWKT): one shared final Crs.
/// Missing rows insert kind-preserving placeholders in final order (no scatter).
pub(crate) fn stream_bulk_srid<'py>(
    iter: impl Iterator<Item = PyResult<Bound<'py, PyAny>>>,
    fallback: Option<Crs>,
    epoch: Option<f64>,
    context: &str,
    source: &str,
    mut parse_row: impl FnMut(
        &Bound<'py, PyAny>,
        &mut crate::array::StreamingShapes,
    ) -> PyResult<StreamedSridRow>,
) -> PyResult<PyGeometryArray> {
    let mut rows = crate::array::StreamingShapes::new();
    let mut frame = SridFrameAdmission::new(fallback, None);
    let mut missing_rows: Vec<usize> = Vec::new();
    let mut row_count = 0;
    for (row, item) in iter.enumerate() {
        row_count = row + 1;
        let item = item?;
        crate::check_python_signals(item.py(), row)?;
        if item.is_none() {
            crate::try_push(&mut missing_rows, row)?;
            rows.try_push_missing()?;
            continue;
        }
        match parse_row(&item, &mut rows).map_err(|err| crate::note_array_row(err, row))? {
            StreamedSridRow::Present(srid) => {
                frame
                    .admit_srid(srid, row, context, source)
                    .map_err(|err| crate::note_array_row(err, row))?;
            },
        }
    }
    let crs = frame.finish()?;
    guard_epoch_frame(epoch, crs.as_ref())?;
    let array = rows.finish(Frame::new(crs, epoch)?);
    if let Some(mask) = crate::array::sparse_missing_mask(row_count, &missing_rows) {
        Ok(array.with_missing_mask(Some(mask)))
    } else {
        Ok(array)
    }
}
