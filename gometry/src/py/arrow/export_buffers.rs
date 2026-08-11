#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::py::arrow::{
    ArrowCoordinateValues, Bound, CoordSeq, CoordinateAxes, Coordinates, IntoPyObject as _, Point,
    Py, PyAny, PyBytes, PyResult, Python, arrow_content_error, geoarrow_parse_error,
    mixed_axes_error,
};

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

/// Multi-chunk Arrow import accumulator: totals are known before the fill, so
/// each ordinate column is an exact-size final `Arc` written chunk-by-chunk
/// (no `Vec` → `Arc` conversion).
///
/// Axes are fixed at construction. Every [`append_arrow_coordinates`] call must
/// supply exactly those axes (Z/M presence must match); a mismatch is a typed
/// error and does not advance the fill cursor. That makes unwritten optional
/// columns unreachable at [`into_coord_seq`] — the previous `if let (Some, Some)`
/// write pattern could leave Z/M uninit while still completing the XY fill.
pub(crate) struct ExactArrowCoordinateFill {
    xs: std::sync::Arc<[std::mem::MaybeUninit<f64>]>,
    ys: std::sync::Arc<[std::mem::MaybeUninit<f64>]>,
    zs: Option<std::sync::Arc<[std::mem::MaybeUninit<f64>]>>,
    ms: Option<std::sync::Arc<[std::mem::MaybeUninit<f64>]>>,
    pos: usize,
    capacity: usize,
    /// Declared axes at construction; append requires exact Z/M presence match.
    axes: CoordinateAxes,
}

impl ExactArrowCoordinateFill {
    pub(crate) fn with_capacity(axes: CoordinateAxes, coordinate_count: usize) -> Self {
        use std::sync::Arc;
        // Capacity is a sum of trusted chunk lengths (already validated per
        // chunk); the allocation is proportional to admitted input.
        Self {
            xs: Arc::new_uninit_slice(coordinate_count),
            ys: Arc::new_uninit_slice(coordinate_count),
            zs: axes
                .has_z()
                .then(|| Arc::new_uninit_slice(coordinate_count)),
            ms: axes
                .has_m()
                .then(|| Arc::new_uninit_slice(coordinate_count)),
            pos: 0,
            capacity: coordinate_count,
            axes,
        }
    }

    pub(crate) fn append_arrow_coordinates(
        &mut self,
        coordinates: &ArrowCoordinateValues,
    ) -> PyResult<()> {
        use std::sync::Arc;

        use crate::geometry::column_all_finite;
        let xs = coordinates.x.values.as_ref();
        let ys = coordinates.y.values.as_ref();
        let zs = coordinates.z.as_ref().map(|values| values.values.as_ref());
        let ms = coordinates.m.as_ref().map(|values| values.values.as_ref());
        // Axes mismatch is always-on and fallible: an XY chunk must never be
        // accepted into an XYZ/XYM/XYZM fill (or the reverse). Without this,
        // `pos` can reach capacity while optional columns stay uninit, and
        // `into_coord_seq` would `assume_init` them.
        let src_axes = CoordinateAxes::new(
            crate::geometry::HasZ(zs.is_some()),
            crate::geometry::HasM(ms.is_some()),
        );
        if src_axes != self.axes {
            return Err(geoarrow_parse_error(
                "Arrow coordinate chunk axes do not match the multi-chunk fill axes",
            ));
        }
        if !column_all_finite(xs)
            || !column_all_finite(ys)
            || zs.is_some_and(|column| !column_all_finite(column))
            || ms.is_some_and(|column| !column_all_finite(column))
        {
            return Err(arrow_content_error(
                crate::geometry::GeometryErrorKind::NonFiniteCoordinate.into(),
            ));
        }
        let n = xs.len();
        if ys.len() != n
            || zs.is_some_and(|column| column.len() != n)
            || ms.is_some_and(|column| column.len() != n)
        {
            return Err(geoarrow_parse_error(
                "Arrow coordinate columns have mismatched lengths",
            ));
        }
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| geoarrow_parse_error("Arrow coordinate count overflows"))?;
        if end > self.capacity {
            return Err(geoarrow_parse_error(
                "Arrow coordinate count exceeds precomputed total",
            ));
        }
        let start = self.pos;
        // SAFETY: unique Arcs; `start..end` is in-range. Axes match is enforced
        // above, so every allocated optional column has a source of equal length
        // and is fully written in this range (or no optional column exists).
        unsafe {
            let write = |dst: &mut Arc<[std::mem::MaybeUninit<f64>]>, src: &[f64]| {
                let slot = Arc::get_mut(dst).unwrap_unchecked();
                slot[start..end].write_copy_of_slice(src);
            };
            write(&mut self.xs, xs);
            write(&mut self.ys, ys);
            // Axes match ⇒ presence of src tracks presence of dst exactly.
            if let Some(dst) = self.zs.as_mut() {
                write(dst, zs.unwrap_unchecked());
            }
            if let Some(dst) = self.ms.as_mut() {
                write(dst, ms.unwrap_unchecked());
            }
        }
        self.pos = end;
        Ok(())
    }

    pub(crate) fn into_coord_seq(self) -> PyResult<CoordSeq> {
        // Fallible completeness: never `assume_init` an under-filled buffer.
        // `append_arrow_coordinates` already rejects over-fill and axes mismatch;
        // a shortfall means the multi-chunk total was wrong or a chunk was skipped.
        if self.pos != self.capacity {
            return Err(geoarrow_parse_error(
                "Arrow coordinate count does not match precomputed total",
            ));
        }
        // SAFETY: every slot `0..capacity` of every allocated column was written:
        // - XY always written by each successful append for that range;
        // - Z/M allocated iff `axes` has them, and every append requires matching
        //   axes so each optional column is written on the same ranges;
        // - `pos == capacity` means the full range was covered by successful appends.
        let (xs, ys, zs, ms) = unsafe {
            (
                self.xs.assume_init(),
                self.ys.assume_init(),
                self.zs.map(|column| column.assume_init()),
                self.ms.map(|column| column.assume_init()),
            )
        };
        Ok(CoordSeq::try_from_columns(xs, ys, zs, ms)?)
    }
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

#[cfg(test)]
mod exact_arrow_fill_tests {
    use std::sync::Arc;

    use super::*;
    use crate::geometry::{CoordinateAxes, HasM, HasZ};
    use crate::py::arrow::{ArrowCoordinateValues, ArrowOrdinateValues, ArrowValidity};

    /// Build a fill and feed raw columns without going through Arrow Python
    /// values — exercises the same axes gate as production append.
    fn try_append_raw(
        fill: &mut ExactArrowCoordinateFill,
        xs: &[f64],
        ys: &[f64],
        zs: Option<&[f64]>,
        ms: Option<&[f64]>,
    ) -> Result<(), String> {
        let src_axes = CoordinateAxes::new(HasZ(zs.is_some()), HasM(ms.is_some()));
        if src_axes != fill.axes {
            return Err("axes mismatch".into());
        }
        // Mirror production: only advance after a successful axes-matched write
        // of every allocated column.
        let n = xs.len();
        if ys.len() != n || zs.is_some_and(|c| c.len() != n) || ms.is_some_and(|c| c.len() != n) {
            return Err("length mismatch".into());
        }
        let end = fill.pos.checked_add(n).ok_or("overflow")?;
        if end > fill.capacity {
            return Err("over capacity".into());
        }
        let start = fill.pos;
        // SAFETY: unique Arc, range in capacity, axes matched ⇒ every column written.
        unsafe {
            let write = |dst: &mut Arc<[std::mem::MaybeUninit<f64>]>, src: &[f64]| {
                let slot = Arc::get_mut(dst).unwrap_unchecked();
                slot[start..end].write_copy_of_slice(src);
            };
            write(&mut fill.xs, xs);
            write(&mut fill.ys, ys);
            if let Some(dst) = fill.zs.as_mut() {
                write(dst, zs.unwrap_unchecked());
            }
            if let Some(dst) = fill.ms.as_mut() {
                write(dst, ms.unwrap_unchecked());
            }
        }
        fill.pos = end;
        Ok(())
    }

    fn ordinate(values: &[f64], field: &'static str) -> ArrowOrdinateValues {
        ArrowOrdinateValues {
            values: Arc::<[f64]>::from(values),
            base: 0,
            validity: ArrowValidity {
                bitmap: None,
                offset: 0,
            },
            field,
        }
    }

    #[test]
    fn mismatched_axes_cannot_complete_xyz_fill() {
        // PyResult error construction needs an interpreter.
        pyo3::Python::initialize();
        // XYZ fill + XY-only values must not reach assume_init on Z.
        let mut fill = ExactArrowCoordinateFill::with_capacity(CoordinateAxes::XYZ, 2);
        let err = try_append_raw(&mut fill, &[1.0, 2.0], &[3.0, 4.0], None, None)
            .expect_err("XY into XYZ must fail");
        assert!(err.contains("axes"), "{err}");
        assert_eq!(fill.pos, 0, "failed append must not advance the cursor");
        // Completing without any write is a shortfall error, not UB.
        fill.into_coord_seq()
            .expect_err("unfilled XYZ must not assume_init");
    }

    #[test]
    fn mismatched_axes_cannot_complete_xy_fill_with_z() {
        let mut fill = ExactArrowCoordinateFill::with_capacity(CoordinateAxes::XY, 1);
        let err = try_append_raw(&mut fill, &[1.0], &[2.0], Some(&[3.0]), None)
            .expect_err("XYZ into XY must fail");
        assert!(err.contains("axes"), "{err}");
        assert_eq!(fill.pos, 0);
    }

    #[test]
    fn matched_xyz_fill_finishes() {
        let mut fill = ExactArrowCoordinateFill::with_capacity(CoordinateAxes::XYZ, 2);
        try_append_raw(&mut fill, &[1.0, 2.0], &[3.0, 4.0], Some(&[5.0, 6.0]), None)
            .expect("matched axes");
        let seq = fill.into_coord_seq().expect("full fill");
        assert_eq!(seq.len(), 2);
        assert!(seq.axes().has_z());
        assert!(!seq.axes().has_m());
    }

    #[test]
    fn production_append_rejects_axes_mismatch() {
        // Drive the real `append_arrow_coordinates` entry with synthetic
        // `ArrowCoordinateValues` so the shipped axes gate is what fails.
        pyo3::Python::initialize();
        let mut fill = ExactArrowCoordinateFill::with_capacity(CoordinateAxes::XYZ, 1);
        let xy_only = ArrowCoordinateValues {
            x: ordinate(&[1.0], "x"),
            y: ordinate(&[2.0], "y"),
            z: None,
            m: None,
            value_validity: ArrowValidity {
                bitmap: None,
                offset: 0,
            },
            value_base: 0,
            full: std::cell::OnceCell::new(),
        };
        fill.append_arrow_coordinates(&xy_only)
            .expect_err("production append must reject XY into XYZ");
        assert_eq!(fill.pos, 0);
        fill.into_coord_seq().unwrap_err();
    }
}
