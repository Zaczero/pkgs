#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::py::wire_crs::{SharedRowCrs, SridFrameAdmission};
use crate::{Crs, Frame, PyGeometryArray, guard_epoch_frame};

pub(crate) enum StreamedRow {
    Present(Option<Crs>),
    Missing,
}

/// Bulk stream row carrying a raw normalized SRID (`None` = CRS-free).
pub(crate) enum StreamedSridRow {
    Present(Option<u32>),
}

pub(crate) fn stream_bulk<'py>(
    iter: impl Iterator<Item = PyResult<Bound<'py, PyAny>>>,
    fallback: Option<Crs>,
    epoch: Option<f64>,
    crs_context: Option<&str>,
    mut parse_row: impl FnMut(
        &Bound<'py, PyAny>,
        &mut crate::array::StreamingShapes,
    ) -> PyResult<StreamedRow>,
) -> PyResult<PyGeometryArray> {
    let mut rows = crate::array::StreamingShapes::new();
    let mut first_crs = SharedRowCrs::Unseen;
    let mut missing_rows: Vec<usize> = Vec::new();
    let mut row_count = 0;
    for (row, item) in iter.enumerate() {
        row_count = row + 1;
        let item = item?;
        if item.is_none() {
            // Final-order kind-preserving placeholder (Stage 3); no later scatter.
            crate::try_push(&mut missing_rows, row)?;
            rows.try_push_missing()?;
            continue;
        }
        match parse_row(&item, &mut rows).map_err(|err| crate::note_array_row(err, row))? {
            StreamedRow::Present(row_crs) => {
                if let Some(context) = crs_context {
                    first_crs.admit(row_crs, fallback.as_ref(), row, context)?;
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
