#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::py::arrow::*;

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

/// Accumulating struct-of-arrays coordinate columns for Arrow export. Holding
/// native `f64` (not pre-serialized bytes) lets each geometry's [`CoordSeq`]
/// columns bulk-`memcpy` in via [`push_points`](Self::push_points), and defers
/// the single LE byte copy to [`f64_column_to_pybytes`] at the end.
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

    pub(crate) fn with_capacity(axes: CoordinateAxes, coordinate_count: usize) -> Self {
        let mut buffers = Self::new(axes);
        buffers.reserve(coordinate_count);
        buffers
    }

    /// Fallible capacity for import paths that must not abort on huge forged counts.
    pub(crate) fn try_with_capacity(
        axes: CoordinateAxes,
        coordinate_count: usize,
    ) -> PyResult<Self> {
        Ok(Self {
            xs: crate::try_vec_with_capacity(coordinate_count)?,
            ys: crate::try_vec_with_capacity(coordinate_count)?,
            zs: if axes.has_z() {
                Some(crate::try_vec_with_capacity(coordinate_count)?)
            } else {
                None
            },
            ms: if axes.has_m() {
                Some(crate::try_vec_with_capacity(coordinate_count)?)
            } else {
                None
            },
        })
    }

    pub(crate) fn reserve(&mut self, coordinate_count: usize) {
        self.xs.reserve(coordinate_count);
        self.ys.reserve(coordinate_count);
        if let Some(values) = &mut self.zs {
            values.reserve(coordinate_count);
        }
        if let Some(values) = &mut self.ms {
            values.reserve(coordinate_count);
        }
    }

    /// Append finite coordinate columns from a GeoArrow import chunk.
    pub(crate) fn append_arrow_coordinates(
        &mut self,
        coordinates: &ArrowCoordinateValues,
    ) -> PyResult<()> {
        use crate::geometry::column_all_finite;
        let xs = coordinates.x.values.as_ref();
        let ys = coordinates.y.values.as_ref();
        let zs = coordinates.z.as_ref().map(|values| values.values.as_ref());
        let ms = coordinates.m.as_ref().map(|values| values.values.as_ref());
        if !column_all_finite(xs)
            || !column_all_finite(ys)
            || zs.is_some_and(|column| !column_all_finite(column))
            || ms.is_some_and(|column| !column_all_finite(column))
        {
            return Err(arrow_content_error(
                crate::geometry::GeometryErrorKind::NonFiniteCoordinate.into(),
            ));
        }
        self.xs.extend_from_slice(xs);
        self.ys.extend_from_slice(ys);
        if let (Some(out), Some(column)) = (self.zs.as_mut(), zs) {
            out.extend_from_slice(column);
        }
        if let (Some(out), Some(column)) = (self.ms.as_mut(), ms) {
            out.extend_from_slice(column);
        }
        Ok(())
    }

    /// Finish import accumulation into a validated coordinate sequence.
    pub(crate) fn into_coord_seq(self) -> PyResult<CoordSeq> {
        Ok(CoordSeq::try_from_columns(
            self.xs.into(),
            self.ys.into(),
            self.zs.map(Into::into),
            self.ms.map(Into::into),
        )?)
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
        self.reserve(points.coord_count());
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
