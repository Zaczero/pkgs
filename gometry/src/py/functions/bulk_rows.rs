use pyo3::prelude::*;

use crate::boundary::metadata;
use crate::boundary::metadata::FrameError;
use crate::py::wire_crs::{SharedRowCrs, SridFrameAdmission};
use crate::{Crs, Frame, PyGeometryArray, Shape, guard_epoch_frame};

const DETACHED_BATCH_ROWS: usize = 0x4000;

pub(crate) struct StreamedRow {
    pub(crate) crs: Option<Crs>,
    pub(crate) epoch: Option<f64>,
}

/// The attached half of a streamed row owns all data that detached parsing
/// needs. A sum type keeps missing rows from carrying a shape and present rows
/// from carrying absent metadata.
pub(crate) enum StreamedShape {
    Missing,
    Present {
        shape: Shape,
        crs: Option<Crs>,
        epoch: Option<f64>,
    },
}

/// Bulk stream row carrying a raw normalized SRID (`None` = CRS-free).
pub(crate) struct StreamedSridRow {
    pub(crate) shape: Shape,
    pub(crate) srid: Option<u32>,
}

struct BulkState {
    rows: crate::array::StreamingShapes,
    first_crs: SharedRowCrs,
    first_epoch: Option<f64>,
    have_present_row: bool,
    fallback: Option<Crs>,
}

/// Stream rows while keeping Python iteration attached and all native parsing,
/// frame admission, and storage growth detached. `own_row` must snapshot any
/// Python-backed payload before `parse_row` receives it.
pub(crate) fn stream_bulk_owned<'py, T, Own, Parse>(
    values: &Bound<'py, PyAny>,
    iter: impl Iterator<Item = PyResult<Bound<'py, PyAny>>>,
    fallback: Option<Crs>,
    explicit_epoch: Option<f64>,
    crs_context: Option<&str>,
    own_row: Own,
    parse_row: Parse,
) -> PyResult<PyGeometryArray>
where
    T: Send,
    Own: FnMut(&Bound<'py, PyAny>, usize) -> PyResult<T>,
    Parse: Fn(T, usize) -> crate::error::Result<StreamedShape> + Send + Sync,
{
    let py = values.py();
    let row_capacity = crate::optional_len(values)?.unwrap_or_default();
    let mut own_row = own_row;
    let mut state = BulkState {
        rows: crate::array::StreamingShapes::with_capacity(0, row_capacity),
        first_crs: SharedRowCrs::Unseen,
        first_epoch: None,
        have_present_row: false,
        fallback,
    };
    for (row, item) in iter.enumerate() {
        let item = item?;
        crate::check_python_signals(item.py(), row)?;
        if item.is_none() {
            let BulkState {
                mut rows,
                first_crs,
                first_epoch,
                have_present_row,
                fallback,
            } = state;
            let result = item
                .py()
                .detach(move || -> crate::error::Result<BulkState> {
                    rows.try_push_missing()?;
                    Ok(BulkState {
                        rows,
                        first_crs,
                        first_epoch,
                        have_present_row,
                        fallback,
                    })
                });
            state = match result {
                Ok(state) => state,
                Err(error) => return Err(crate::note_array_row(error.into(), row)),
            };
            continue;
        }

        let owned = match own_row(&item, row) {
            Ok(owned) => owned,
            Err(error) => return Err(crate::note_array_row(error, row)),
        };
        let parse_row_ref = &parse_row;
        let BulkState {
            mut rows,
            mut first_crs,
            mut first_epoch,
            mut have_present_row,
            fallback,
        } = state;
        let result = item
            .py()
            .detach(move || -> crate::error::Result<BulkState> {
                let parsed = parse_row_ref(owned, row)?;
                match parsed {
                    StreamedShape::Present {
                        shape,
                        crs: row_crs,
                        epoch,
                    } => {
                        rows.try_push(shape)?;
                        if let Some(context) = crs_context {
                            first_crs.admit(row_crs, fallback.as_ref(), row, context)?;
                        }
                        if let Some(explicit) = explicit_epoch
                            && let Some(row_epoch) = epoch
                            && !metadata::epochs_equal(row_epoch, explicit)
                        {
                            return Err(FrameError::SharedEpoch {
                                context: crs_context.unwrap_or("GeometryArray").to_owned().into(),
                                index: row,
                                first: Some(explicit),
                                other: Some(row_epoch),
                            }
                            .into());
                        }
                        if explicit_epoch.is_none() {
                            if !have_present_row {
                                first_epoch = epoch;
                                have_present_row = true;
                            } else if first_epoch != epoch {
                                return Err(FrameError::SharedEpoch {
                                    context: crs_context
                                        .unwrap_or("GeometryArray")
                                        .to_owned()
                                        .into(),
                                    index: row,
                                    first: first_epoch,
                                    other: epoch,
                                }
                                .into());
                            }
                        }
                    },
                    StreamedShape::Missing => {
                        rows.try_push_missing()?;
                    },
                }
                Ok(BulkState {
                    rows,
                    first_crs,
                    first_epoch,
                    have_present_row,
                    fallback,
                })
            });
        state = match result {
            Ok(state) => state,
            Err(error) => return Err(crate::note_array_row(error.into(), row)),
        };
    }

    let BulkState {
        rows,
        first_crs,
        first_epoch,
        fallback,
        ..
    } = state;
    let result = py.detach(move || {
        iter_detach_finalize(
            rows,
            first_crs,
            first_epoch,
            fallback,
            explicit_epoch,
            crs_context,
        )
    });
    result.map_err(PyErr::from)
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
    for (row, item) in iter.enumerate() {
        let item = item?;
        crate::check_python_signals(item.py(), row)?;
        if item.is_none() {
            rows.try_push_missing()?;
            continue;
        }
        let StreamedRow {
            crs: row_crs,
            epoch,
        } = parse_row(&item, &mut rows, row).map_err(|err| crate::note_array_row(err, row))?;
        if let Some(context) = crs_context {
            first_crs
                .admit(row_crs, fallback.as_ref(), row, context)
                .map_err(|err| crate::note_array_row(err.into(), row))?;
        }
        if let Some(explicit) = explicit_epoch
            && let Some(row_epoch) = epoch
            && !metadata::epochs_equal(row_epoch, explicit)
        {
            return Err(crate::note_array_row(
                crate::error::Error::from(FrameError::SharedEpoch {
                    context: crs_context.unwrap_or("GeometryArray").to_owned().into(),
                    index: row,
                    first: Some(explicit),
                    other: Some(row_epoch),
                })
                .into(),
                row,
            ));
        }
        if explicit_epoch.is_none() {
            if !have_present_row {
                first_epoch = epoch;
                have_present_row = true;
            } else if first_epoch != epoch {
                return Err(crate::note_array_row(
                    crate::error::Error::from(FrameError::SharedEpoch {
                        context: crs_context.unwrap_or("GeometryArray").to_owned().into(),
                        index: row,
                        first: first_epoch,
                        other: epoch,
                    })
                    .into(),
                    row,
                ));
            }
        }
    }
    py_finish_attached(
        rows,
        first_crs,
        first_epoch,
        fallback,
        explicit_epoch,
        crs_context,
    )
}

fn py_finish_attached(
    rows: crate::array::StreamingShapes,
    first_crs: SharedRowCrs,
    first_epoch: Option<f64>,
    fallback: Option<Crs>,
    explicit_epoch: Option<f64>,
    crs_context: Option<&str>,
) -> PyResult<PyGeometryArray> {
    iter_detach_finalize(
        rows,
        first_crs,
        first_epoch,
        fallback,
        explicit_epoch,
        crs_context,
    )
    .map_err(PyErr::from)
}

fn iter_detach_finalize(
    rows: crate::array::StreamingShapes,
    first_crs: SharedRowCrs,
    first_epoch: Option<f64>,
    fallback: Option<Crs>,
    explicit_epoch: Option<f64>,
    crs_context: Option<&str>,
) -> crate::error::Result<PyGeometryArray> {
    let crs = if crs_context.is_some() {
        first_crs.into_crs(fallback)
    } else {
        fallback
    };
    let epoch = explicit_epoch.or(first_epoch);
    guard_epoch_frame(epoch, crs.as_ref())?;
    let frame = Frame::new(crs, epoch)?;
    rows.finish(frame)
}

struct SridBulkState {
    rows: crate::array::StreamingShapes,
    frame: SridFrameAdmission,
}

enum BatchError {
    Row(usize, crate::error::Error),
}

struct AcquiredChunk<T> {
    rows: Vec<(usize, T)>,
    error: Option<(usize, PyErr)>,
    finished: bool,
}

fn acquire_owned_chunk<'py, T>(
    iter: &mut impl Iterator<Item = PyResult<Bound<'py, PyAny>>>,
    own_row: &mut impl FnMut(&Bound<'py, PyAny>, usize) -> PyResult<T>,
    first_row: usize,
) -> PyResult<AcquiredChunk<T>> {
    let mut rows = crate::try_vec_with_capacity(DETACHED_BATCH_ROWS)?;
    for row in first_row..first_row + DETACHED_BATCH_ROWS {
        let Some(item) = iter.next() else {
            return Ok(AcquiredChunk {
                rows,
                error: None,
                finished: true,
            });
        };
        let item = match item {
            Ok(item) => item,
            Err(error) => {
                return Ok(AcquiredChunk {
                    rows,
                    error: Some((row, error)),
                    finished: false,
                });
            },
        };
        if let Err(error) = crate::check_python_signals(item.py(), row) {
            return Ok(AcquiredChunk {
                rows,
                error: Some((row, error)),
                finished: false,
            });
        }
        let owned = match own_row(&item, row) {
            Ok(owned) => owned,
            Err(error) => {
                return Ok(AcquiredChunk {
                    rows,
                    error: Some((row, error)),
                    finished: false,
                });
            },
        };
        crate::try_push(&mut rows, (row, owned))?;
    }
    Ok(AcquiredChunk {
        rows,
        error: None,
        finished: false,
    })
}

fn process_owned_chunks<'py, T, S, Own, Parse>(
    py: Python<'py>,
    iter: impl Iterator<Item = PyResult<Bound<'py, PyAny>>>,
    mut own_row: Own,
    mut state: S,
    parse_chunk: Parse,
) -> PyResult<S>
where
    T: Send,
    S: Send,
    Own: FnMut(&Bound<'py, PyAny>, usize) -> PyResult<T>,
    Parse: Fn(S, Vec<(usize, T)>) -> std::result::Result<S, BatchError> + Send + Sync,
{
    let mut iter = iter;
    let mut first_row = 0;
    loop {
        let AcquiredChunk {
            rows,
            error,
            finished,
        } = acquire_owned_chunk(&mut iter, &mut own_row, first_row)?;
        let acquired = rows.len();
        // A failed next acquisition stays pending until this prefix has had its
        // chance to produce the earlier row error.
        if !rows.is_empty() {
            let parse = &parse_chunk;
            state = match py.detach(move || parse(state, rows)) {
                Ok(state) => state,
                Err(BatchError::Row(row, error)) => {
                    return Err(crate::note_array_row(error.into(), row));
                },
            };
        }
        if let Some((row, error)) = error {
            return Err(crate::note_array_row(error, row));
        }
        if finished {
            return Ok(state);
        }
        first_row += acquired;
    }
}

fn parse_srid_chunk<T, Parse>(
    mut state: SridBulkState,
    chunk: Vec<(usize, T)>,
    context: &'static str,
    source: &'static str,
    parse_row: &Parse,
) -> std::result::Result<SridBulkState, BatchError>
where
    Parse: Fn(T) -> crate::error::Result<Option<StreamedSridRow>>,
{
    for (row, owned) in chunk {
        let Some(parsed) = parse_row(owned).map_err(|error| BatchError::Row(row, error))? else {
            state
                .rows
                .try_push_missing()
                .map_err(|error| BatchError::Row(row, error))?;
            continue;
        };
        state
            .frame
            .admit_srid(parsed.srid, row, context, source)
            .map_err(|error| BatchError::Row(row, error))?;
        state
            .rows
            .try_push(parsed.shape)
            .map_err(|error| BatchError::Row(row, error))?;
    }
    Ok(state)
}

fn parse_geometry_chunk<T, Parse>(
    mut state: BulkState,
    chunk: Vec<(usize, T)>,
    explicit_epoch: Option<f64>,
    crs_context: Option<&str>,
    parse_row: &Parse,
) -> std::result::Result<BulkState, BatchError>
where
    Parse: Fn(T, usize) -> crate::error::Result<StreamedShape>,
{
    for (row, owned) in chunk {
        let parsed = parse_row(owned, row).map_err(|error| BatchError::Row(row, error))?;
        match parsed {
            StreamedShape::Present { shape, crs, epoch } => {
                state
                    .rows
                    .try_push(shape)
                    .map_err(|error| BatchError::Row(row, error))?;
                if let Some(context) = crs_context {
                    state
                        .first_crs
                        .admit(crs, state.fallback.as_ref(), row, context)
                        .map_err(|error| BatchError::Row(row, error))?;
                }
                if let Some(explicit) = explicit_epoch
                    && let Some(row_epoch) = epoch
                    && !metadata::epochs_equal(row_epoch, explicit)
                {
                    return Err(BatchError::Row(
                        row,
                        FrameError::SharedEpoch {
                            context: crs_context.unwrap_or("GeometryArray").to_owned().into(),
                            index: row,
                            first: Some(explicit),
                            other: Some(row_epoch),
                        }
                        .into(),
                    ));
                }
                if explicit_epoch.is_none() {
                    if !state.have_present_row {
                        state.first_epoch = epoch;
                        state.have_present_row = true;
                    } else if state.first_epoch != epoch {
                        return Err(BatchError::Row(
                            row,
                            FrameError::SharedEpoch {
                                context: crs_context.unwrap_or("GeometryArray").to_owned().into(),
                                index: row,
                                first: state.first_epoch,
                                other: epoch,
                            }
                            .into(),
                        ));
                    }
                }
            },
            StreamedShape::Missing => {
                state
                    .rows
                    .try_push_missing()
                    .map_err(|error| BatchError::Row(row, error))?;
            },
        }
    }
    Ok(state)
}

/// Acquire and parse a concrete batch in bounded detached regions.
pub(crate) fn batch_bulk_srid<'py, T, Own, Parse>(
    py: Python<'py>,
    iter: impl Iterator<Item = PyResult<Bound<'py, PyAny>>>,
    own_row: Own,
    fallback: Option<Crs>,
    epoch: Option<f64>,
    context: &'static str,
    source: &'static str,
    parse_row: Parse,
) -> PyResult<PyGeometryArray>
where
    T: Send,
    Own: FnMut(&Bound<'py, PyAny>, usize) -> PyResult<T>,
    Parse: Fn(T) -> crate::error::Result<Option<StreamedSridRow>> + Send + Sync,
{
    let state = SridBulkState {
        rows: crate::array::StreamingShapes::new(),
        frame: SridFrameAdmission::new(fallback, None),
    };
    let state = process_owned_chunks(py, iter, own_row, state, move |state, chunk| {
        parse_srid_chunk(state, chunk, context, source, &parse_row)
    })?;
    py.detach(move || item_detach_finalize(state.rows, state.frame, epoch))
        .map_err(PyErr::from)
}

/// Acquire and parse a concrete GeoJSON/polyline batch in bounded detached
/// regions. `None` rows represent missing array slots.
pub(crate) fn batch_bulk<'py, T, Own, Parse>(
    py: Python<'py>,
    iter: impl Iterator<Item = PyResult<Bound<'py, PyAny>>>,
    own_row: Own,
    fallback: Option<Crs>,
    explicit_epoch: Option<f64>,
    crs_context: Option<&str>,
    parse_row: Parse,
) -> PyResult<PyGeometryArray>
where
    T: Send,
    Own: FnMut(&Bound<'py, PyAny>, usize) -> PyResult<T>,
    Parse: Fn(T, usize) -> crate::error::Result<StreamedShape> + Send + Sync,
{
    let state = BulkState {
        rows: crate::array::StreamingShapes::new(),
        first_crs: SharedRowCrs::Unseen,
        first_epoch: None,
        have_present_row: false,
        fallback,
    };
    let state = process_owned_chunks(py, iter, own_row, state, move |state, chunk| {
        parse_geometry_chunk(state, chunk, explicit_epoch, crs_context, &parse_row)
    })?;
    py.detach(move || {
        iter_detach_finalize(
            state.rows,
            state.first_crs,
            state.first_epoch,
            state.fallback,
            explicit_epoch,
            crs_context,
        )
    })
    .map_err(PyErr::from)
}

/// Bulk WKT/WKB stream with numeric SRID admission. Python rows are traversed
/// attached, while owned payload parsing, SRID admission, and packing are
/// detached one row at a time.
pub(crate) fn stream_bulk_srid<'py, T, Own, Parse>(
    py: Python<'py>,
    iter: impl Iterator<Item = PyResult<Bound<'py, PyAny>>>,
    fallback: Option<Crs>,
    epoch: Option<f64>,
    context: &'static str,
    source: &'static str,
    own_row: Own,
    parse_row: Parse,
) -> PyResult<PyGeometryArray>
where
    T: Send,
    Own: FnMut(&Bound<'py, PyAny>, usize) -> PyResult<T>,
    Parse: Fn(T) -> crate::error::Result<StreamedSridRow> + Send + Sync,
{
    let mut own_row = own_row;
    let mut state = SridBulkState {
        rows: crate::array::StreamingShapes::new(),
        frame: SridFrameAdmission::new(fallback, None),
    };
    for (row, item) in iter.enumerate() {
        let item = item?;
        crate::check_python_signals(item.py(), row)?;
        if item.is_none() {
            let SridBulkState { mut rows, frame } = state;
            let result = item
                .py()
                .detach(move || -> crate::error::Result<SridBulkState> {
                    rows.try_push_missing()?;
                    Ok(SridBulkState { rows, frame })
                });
            state = match result {
                Ok(state) => state,
                Err(error) => return Err(crate::note_array_row(error.into(), row)),
            };
            continue;
        }

        let owned = match own_row(&item, row) {
            Ok(owned) => owned,
            Err(error) => return Err(crate::note_array_row(error, row)),
        };
        let parse_row_ref = &parse_row;
        let SridBulkState {
            mut rows,
            mut frame,
        } = state;
        let result = item
            .py()
            .detach(move || -> crate::error::Result<SridBulkState> {
                let parsed = parse_row_ref(owned)?;
                frame.admit_srid(parsed.srid, row, context, source)?;
                rows.try_push(parsed.shape)?;
                Ok(SridBulkState { rows, frame })
            });
        state = match result {
            Ok(state) => state,
            Err(error) => return Err(crate::note_array_row(error.into(), row)),
        };
    }

    let SridBulkState { rows, frame } = state;
    let result = py.detach(move || item_detach_finalize(rows, frame, epoch));
    result.map_err(PyErr::from)
}

fn item_detach_finalize(
    rows: crate::array::StreamingShapes,
    frame: SridFrameAdmission,
    epoch: Option<f64>,
) -> crate::error::Result<PyGeometryArray> {
    let crs = frame.finish()?;
    guard_epoch_frame(epoch, crs.as_ref())?;
    let frame = Frame::new(crs, epoch)?;
    rows.finish(frame)
}
