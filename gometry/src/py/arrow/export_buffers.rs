#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::IntoPyObject as _;
use pyo3::prelude::{Bound, Py, PyAny, PyResult, Python};
use pyo3::types::PyBytes;

use crate::geometry::{CoordinateAxes, Coordinates, Point};
use crate::py::arrow::mixed_axes_error;

/// Serialize a native `f64` column into a `PyBytes` Arrow buffer. On
/// little-endian hosts the `bytemuck` cast is a zero-cost reinterpretation, so
/// the only copy is the single `PyBytes` fill; on a big-endian host each value
/// is byte-swapped.
pub(crate) fn f64_column_to_pybytes<'py>(
    py: Python<'py>,
    values: &[f64],
) -> PyResult<Bound<'py, PyBytes>> {
    if cfg!(target_endian = "little") {
        let bytes: &[u8] = bytemuck::cast_slice(values);
        Ok(PyBytes::new(py, bytes))
    } else {
        PyBytes::new_with(py, size_of_val(values), |out| {
            for (value, chunk) in values.iter().zip(out.as_chunks_mut::<8>().0) {
                chunk.copy_from_slice(&value.to_le_bytes());
            }
            Ok(())
        })
    }
}

/// The four serialized coordinate-column buffers of one export.
pub(crate) type ArrowColumnBytes<'py> = (
    Bound<'py, PyBytes>,
    Bound<'py, PyBytes>,
    Option<Bound<'py, PyBytes>>,
    Option<Bound<'py, PyBytes>>,
);

/// The four exported coordinate-column buffer objects (`_Float64Buffer`
/// movers on little-endian hosts, `PyBytes` on big-endian).
pub(crate) type ArrowColumnBuffers = (Py<PyAny>, Py<PyAny>, Option<Py<PyAny>>, Option<Py<PyAny>>);

/// Serialize `(xs, ys, zs, ms)` columns into Arrow buffer bytes in one place.
pub(crate) fn columns_to_pybytes<'py>(
    py: Python<'py>,
    xs: &[f64],
    ys: &[f64],
    zs: Option<&[f64]>,
    ms: Option<&[f64]>,
) -> PyResult<ArrowColumnBytes<'py>> {
    Ok((
        f64_column_to_pybytes(py, xs)?,
        f64_column_to_pybytes(py, ys)?,
        zs.map(|values| f64_column_to_pybytes(py, values))
            .transpose()?,
        ms.map(|values| f64_column_to_pybytes(py, values))
            .transpose()?,
    ))
}

/// Accumulating struct-of-arrays coordinate columns used by the Arrow export
/// builders. Import decoding owns its separate native representation.
pub(crate) struct ArrowCoordinateBuffers {
    xs: Vec<f64>,
    ys: Vec<f64>,
    zs: Option<Vec<f64>>,
    ms: Option<Vec<f64>>,
}

impl ArrowCoordinateBuffers {
    pub(crate) fn new(axes: CoordinateAxes) -> Self {
        Self {
            xs: Vec::new(),
            ys: Vec::new(),
            zs: axes.has_z().then(Vec::new),
            ms: axes.has_m().then(Vec::new),
        }
    }

    pub(crate) fn with_capacity(
        axes: CoordinateAxes,
        coordinate_count: usize,
    ) -> crate::error::Result<Self> {
        let mut buffers = Self::new(axes);
        buffers.reserve(coordinate_count)?;
        Ok(buffers)
    }

    pub(crate) fn release(&mut self) {
        self.xs = Vec::new();
        self.ys = Vec::new();
        if let Some(values) = &mut self.zs {
            *values = Vec::new();
        }
        if let Some(values) = &mut self.ms {
            *values = Vec::new();
        }
    }

    pub(crate) fn reserve(&mut self, coordinate_count: usize) -> crate::error::Result<()> {
        if let Err(error) = crate::try_reserve_vec(&mut self.xs, coordinate_count) {
            self.release();
            return Err(error);
        }
        if let Err(error) = crate::try_reserve_vec(&mut self.ys, coordinate_count) {
            self.release();
            return Err(error);
        }
        if let Some(values) = &mut self.zs {
            if let Err(error) = crate::try_reserve_vec(values, coordinate_count) {
                self.release();
                return Err(error);
            }
        }
        if let Some(values) = &mut self.ms {
            if let Err(error) = crate::try_reserve_vec(values, coordinate_count) {
                self.release();
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn push_point(&mut self, point: Point) -> PyResult<()> {
        self.xs.push(point.x);
        self.ys.push(point.y);
        if let Some(values) = &mut self.zs {
            values.push(point.z().ok_or_else(mixed_axes_error)?);
        }
        if let Some(values) = &mut self.ms {
            values.push(point.m().ok_or_else(mixed_axes_error)?);
        }
        Ok(())
    }

    /// Append a whole coordinate sequence. When the source stores contiguous
    /// columns (`SoA` [`CoordSeq`]/[`Ring`]) and its axes match this buffer,
    /// the ordinates bulk-`memcpy` in one `extend_from_slice` per column;
    /// otherwise (scratch `[Point]`) it falls back to per-point appends.
    pub(crate) fn push_points<C: Coordinates + ?Sized>(&mut self, points: &C) -> PyResult<()> {
        self.reserve(points.coord_count())
            .map_err(pyo3::PyErr::from)?;
        let Some((xs, ys)) = points.xy_columns() else {
            for point in points.iter_coords() {
                self.push_point(point)?;
            }
            return Ok(());
        };
        self.xs.extend_from_slice(xs);
        self.ys.extend_from_slice(ys);
        self.extend_optional_column(|buffers| &mut buffers.zs, points.z_column())?;
        self.extend_optional_column(|buffers| &mut buffers.ms, points.m_column())?;
        Ok(())
    }

    /// Hand the accumulated columns to pyarrow: on little-endian hosts the
    /// gathered `Vec`s MOVE into `_Float64Buffer` `Arc`s (PEP 3118 — no
    /// second byte copy); big-endian hosts keep the byte-swapping
    /// `PyBytes` path.
    pub(crate) fn into_buffers(self, py: Python<'_>) -> PyResult<ArrowColumnBuffers> {
        if cfg!(target_endian = "little") {
            let column = |values: Vec<f64>| -> PyResult<Py<PyAny>> {
                let arc: std::sync::Arc<[f64]> = values.into();
                let window = 0..arc.len();
                Ok(crate::py::vectors::Float64Buffer::view(arc, window)?
                    .into_pyobject(py)?
                    .unbind()
                    .into_any())
            };
            return Ok((
                column(self.xs)?,
                column(self.ys)?,
                self.zs.map(column).transpose()?,
                self.ms.map(column).transpose()?,
            ));
        }
        let (xs, ys, zs, ms) = columns_to_pybytes(
            py,
            &self.xs,
            &self.ys,
            self.zs.as_deref(),
            self.ms.as_deref(),
        )?;
        Ok((
            xs.unbind().into_any(),
            ys.unbind().into_any(),
            zs.map(|bytes| bytes.unbind().into_any()),
            ms.map(|bytes| bytes.unbind().into_any()),
        ))
    }

    pub(crate) fn extend_optional_column(
        &mut self,
        select: impl Fn(&mut Self) -> &mut Option<Vec<f64>>,
        column: Option<&[f64]>,
    ) -> PyResult<()> {
        match (select(self), column) {
            (Some(buffer), Some(column)) => {
                buffer.extend_from_slice(column);
                Ok(())
            },
            (None, None) => Ok(()),
            _ => Err(mixed_axes_error()),
        }
    }
}
