use std::sync::Arc;

use crate::geometry::{CoordWindow, MOrdinate, ZOrdinate, column_all_finite};
use crate::py::arrow::{
    ArrowCoordinateValues, ArrowOrdinateValues, ArrowValidity, Bound, CoordSeq, CoordinateAxes,
    EmptyKind, HasM, HasZ, Point, Polygon, PyAny, PyAnyMethods as _, PyResult, PyTypeMethods as _,
    Python, Shape, arrow_array_offset, arrow_content_error, arrow_f64_values_span,
    arrow_i32_offsets_window, arrow_i64_offsets_window, arrow_null_error, arrow_validity_window,
    coordinate_span, ensure_arrow_range, ensure_offset_terminal_within_child,
    ensure_usize_offsets_monotonic, geoarrow_parse_error, i32_offset_to_usize, i64_offset_to_usize,
    reject_inner_nulls_in_range, same_active_position, usize_offset_at,
};

/// Decode the coordinate columns for the visible coordinate run
/// `[base, base + span)` only — not the whole (possibly large parent) buffer.
///
/// Accepts GeoArrow **separated** (`Struct<x,y[,z][,m]>`) and **interleaved**
/// (`FixedSizeList<float64>[n]`) coordinate storage.
pub(crate) fn arrow_coordinate_values(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    base: usize,
    span: usize,
) -> PyResult<ArrowCoordinateValues> {
    if is_fixed_size_list_array(values)? {
        return arrow_interleaved_coordinate_values(py, values, base, span);
    }
    let xs = values.call_method1("field", ("x",))?;
    let ys = values.call_method1("field", ("y",))?;
    let field_names = arrow_struct_field_names(values)?;
    Ok(ArrowCoordinateValues {
        x: arrow_ordinate_values(py, xs, "x", base, span)?,
        y: arrow_ordinate_values(py, ys, "y", base, span)?,
        z: optional_arrow_ordinate_values(py, values, &field_names, "z", base, span)?,
        m: optional_arrow_ordinate_values(py, values, &field_names, "m", base, span)?,
        value_validity: arrow_validity_window(py, values, base, span)?,
        value_base: base,
        full: std::cell::OnceCell::new(),
    })
}

fn is_fixed_size_list_array(values: &Bound<'_, PyAny>) -> PyResult<bool> {
    let value_type = values.getattr("type")?;
    if let Ok(format) = value_type.getattr("format")
        && let Ok(format) = format.extract::<String>()
    {
        return Ok(format.starts_with("+w:"));
    }
    if let Ok(name) = value_type.get_type().name() {
        return Ok(name == "FixedSizeListType");
    }
    Ok(false)
}

/// Interleaved dimensions from FixedSizeList size + optional field name.
/// size 3 + "xym" → XYM; other size 3 → XYZ; size 2 → XY; size 4 → XYZM.
fn interleaved_axes(list_size: usize, dim_name: &str) -> PyResult<(bool, bool)> {
    match (list_size, dim_name) {
        (2, _) => Ok((false, false)),
        (3, "xym") => Ok((false, true)),
        (3, _) => Ok((true, false)), // "xyz", "item", empty → XYZ
        (4, _) => Ok((true, true)),
        _ => Err(geoarrow_parse_error(
            "geoarrow interleaved coordinates require fixed_size_list of length 2, 3, or 4",
        )),
    }
}

fn dense_ordinate(values: Vec<f64>, base: usize, field: &'static str) -> ArrowOrdinateValues {
    ArrowOrdinateValues {
        values: Arc::<[f64]>::from(values),
        base,
        validity: ArrowValidity {
            bitmap: None,
            offset: 0,
        },
        field,
    }
}

/// Deinterleave FixedSizeList coordinates into SoA ordinate columns for the
/// shared decode surface.
fn arrow_interleaved_coordinate_values(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    base: usize,
    span: usize,
) -> PyResult<ArrowCoordinateValues> {
    let value_type = values.getattr("type")?;
    let list_size: usize = value_type
        .getattr("list_size")?
        .extract::<i32>()?
        .try_into()
        .map_err(|_| {
            geoarrow_parse_error(
                "geoarrow interleaved coordinates require fixed_size_list of length 2, 3, or 4",
            )
        })?;
    let dim_name = value_type
        .getattr("value_field")
        .ok()
        .and_then(|f| f.getattr("name").ok())
        .and_then(|n| n.extract::<String>().ok())
        .unwrap_or_default();
    let (has_z, has_m) = interleaved_axes(list_size, dim_name.as_str())?;
    // Flat float64: vertex `i` is `list_size` consecutive values; array offset
    // multiplies into the flat buffer for sliced FixedSizeList parents.
    let flat = values.getattr("values")?;
    let array_offset = arrow_array_offset(values)?;
    let flat_base = array_offset
        .checked_add(base)
        .and_then(|v| v.checked_mul(list_size))
        .ok_or_else(|| geoarrow_parse_error("Arrow interleaved coordinate span overflows"))?;
    let flat_span = span
        .checked_mul(list_size)
        .ok_or_else(|| geoarrow_parse_error("Arrow interleaved coordinate span overflows"))?;
    let flat_values = arrow_f64_values_span(py, &flat, flat_base, flat_span)?;
    if flat_values.len() != flat_span {
        return Err(geoarrow_parse_error(
            "Arrow interleaved coordinate buffer is shorter than declared",
        ));
    }
    // GeoArrow forbids null coordinate *elements* under a present geometry
    // (outer FixedSizeList nulls are fine). Admit the flat child validity and
    // reject any referenced child null — slots wholly hidden by an outer null
    // are ignored (C10).
    let value_validity = arrow_validity_window(py, values, base, span)?;
    let child_validity = arrow_validity_window(py, &flat, flat_base, flat_span)?;
    for i in 0..span {
        if !value_validity.is_valid(i) {
            continue;
        }
        reject_inner_nulls_in_range(&child_validity, i * list_size, list_size)?;
    }
    let mut xs = crate::try_vec_with_capacity(span)?;
    let mut ys = crate::try_vec_with_capacity(span)?;
    let mut zs = has_z
        .then(|| crate::try_vec_with_capacity(span))
        .transpose()?;
    let mut ms = has_m
        .then(|| crate::try_vec_with_capacity(span))
        .transpose()?;
    for i in 0..span {
        let o = i * list_size;
        xs.push(flat_values[o]);
        ys.push(flat_values[o + 1]);
        if let Some(zcol) = zs.as_mut() {
            zcol.push(flat_values[o + 2]);
        }
        if let Some(mcol) = ms.as_mut() {
            // XYM: third cell is M; XYZM: fourth cell is M.
            let m_off = if has_z { o + 3 } else { o + 2 };
            mcol.push(flat_values[m_off]);
        }
    }
    // FixedSizeList validity is outer; deinterleaved columns are dense with
    // `base` absolute so ring ranges reindex via `start - base`.
    Ok(ArrowCoordinateValues {
        x: dense_ordinate(xs, base, "x"),
        y: dense_ordinate(ys, base, "y"),
        z: zs.map(|col| dense_ordinate(col, base, "z")),
        m: ms.map(|col| dense_ordinate(col, base, "m")),
        value_validity,
        value_base: base,
        full: std::cell::OnceCell::new(),
    })
}

impl ArrowCoordinateValues {
    /// Axes declared by the coordinate struct fields (schema), independent of
    /// decoded vertex presence — empty multiparts and POINT EMPTY use this.
    pub(crate) const fn axes(&self) -> CoordinateAxes {
        CoordinateAxes::new(HasZ(self.z.is_some()), HasM(self.m.is_some()))
    }

    /// GeoArrow/OGC empty-point sentinel: every *active* ordinate is `NaN`.
    /// Partial NaN or any Inf is not empty (caller rejects via finite check).
    pub(crate) fn is_empty_point_sentinel(&self, index: usize) -> bool {
        if !self.x.value(index).is_nan() || !self.y.value(index).is_nan() {
            return false;
        }
        if self
            .z
            .as_ref()
            .is_some_and(|values| !values.value(index).is_nan())
        {
            return false;
        }
        if self
            .m
            .as_ref()
            .is_some_and(|values| !values.value(index).is_nan())
        {
            return false;
        }
        true
    }

    /// Decode one point row: all-active-ordinate `NaN` → typed `POINT EMPTY`
    /// with schema axes; otherwise a finite `Point` (partial NaN/Inf error).
    pub(crate) fn point_shape(&self, index: usize, row: usize) -> PyResult<Shape> {
        self.ensure_ranges(index, 1)?;
        if !self.is_valid(index) {
            return Err(arrow_null_error(row));
        }
        if self.is_empty_point_sentinel(index) {
            return Ok(Shape::typed_empty(EmptyKind::Point, self.axes()));
        }
        Ok(Shape::Point(
            Point::new_axes(
                self.x.value(index),
                self.y.value(index),
                ZOrdinate(self.z.as_ref().map(|values| values.value(index))),
                MOrdinate(self.m.as_ref().map(|values| values.value(index))),
            )
            .map_err(arrow_content_error)?,
        ))
    }

    /// Build a `CoordSeq` for the coordinate range directly from the ordinate
    /// columns (SoA, no per-vertex `Point` stage). Each ordinate array carries
    /// its own Arrow offset, so the columns are sliced independently.
    /// Finiteness is validated only for `[start, end)` — child spans under
    /// outer-null rows are undefined (R06) and must not reject an otherwise
    /// valid present row.
    pub(crate) fn coordseq(&self, start: usize, end: usize, row: usize) -> PyResult<CoordSeq> {
        self.ensure_ranges(start, end - start)?;
        for point_index in start..end {
            if !self.is_valid(point_index) {
                return Err(arrow_null_error(row));
            }
        }
        // Present-row span only: hidden-null NaN/Inf child data is not checked.
        self.ensure_range_finite(start, end)?;
        // Full-span parent is assembled without a global finite gate so outer
        // null slots can hold undefined child payload; each view is range-checked.
        if self.full.get().is_none() {
            let built = CoordSeq::try_from_columns(
                Arc::clone(&self.x.values),
                Arc::clone(&self.y.values),
                self.z.as_ref().map(|ordinate| Arc::clone(&ordinate.values)),
                self.m.as_ref().map(|ordinate| Arc::clone(&ordinate.values)),
            )
            .map_err(arrow_content_error)?;
            let _ = self.full.set(built);
        }
        let full = self.full.get().expect("just initialized");
        Ok(full.view(CoordWindow::trusted(
            start - self.x.base..end - self.x.base,
            full.len(),
        )))
    }

    /// Finiteness for a logical coordinate window (absolute indices in the
    /// ordinate buffers). Outer-null hidden spans are never passed here.
    pub(crate) fn ensure_range_finite(&self, start: usize, end: usize) -> PyResult<()> {
        let base = self.x.base;
        let lo = start - base;
        let hi = end - base;
        let xs = &self.x.values[lo..hi];
        let ys = &self.y.values[lo..hi];
        if !column_all_finite(xs)
            || !column_all_finite(ys)
            || self
                .z
                .as_ref()
                .is_some_and(|ordinate| !column_all_finite(&ordinate.values[lo..hi]))
            || self
                .m
                .as_ref()
                .is_some_and(|ordinate| !column_all_finite(&ordinate.values[lo..hi]))
        {
            return Err(arrow_content_error(
                crate::geometry::GeometryErrorKind::NonFiniteCoordinate.into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn ensure_ranges(&self, start: usize, len: usize) -> PyResult<()> {
        self.x.ensure_range(start, len)?;
        self.y.ensure_range(start, len)?;
        if let Some(values) = &self.z {
            values.ensure_range(start, len)?;
        }
        if let Some(values) = &self.m {
            values.ensure_range(start, len)?;
        }
        Ok(())
    }

    /// Whether every vertex of the logical range is valid (no struct or
    /// ordinate nulls) — the packed import lane's all-or-fallback gate.
    pub(crate) fn all_valid(&self, start: usize, end: usize) -> bool {
        (start..end).all(|index| self.is_valid(index))
    }

    pub(crate) fn is_valid(&self, index: usize) -> bool {
        let Some(local) = index.checked_sub(self.value_base) else {
            return false;
        };
        self.value_validity.is_valid(local)
            && self.x.is_valid(index)
            && self.y.is_valid(index)
            && self.z.as_ref().is_none_or(|values| values.is_valid(index))
            && self.m.as_ref().is_none_or(|values| values.is_valid(index))
    }
}

impl ArrowOrdinateValues {
    pub(crate) fn value(&self, index: usize) -> f64 {
        self.values[index - self.base]
    }

    pub(crate) fn ensure_range(&self, start: usize, len: usize) -> PyResult<()> {
        let Some(rebased) = start.checked_sub(self.base) else {
            return Err(geoarrow_parse_error(format!(
                "Arrow {} buffer is shorter than declared array length",
                self.field
            )));
        };
        ensure_arrow_range(self.values.len(), rebased, len, self.field)
    }

    fn is_valid(&self, index: usize) -> bool {
        index
            .checked_sub(self.base)
            .is_some_and(|local| self.validity.is_valid(local))
    }
}

pub(crate) fn arrow_struct_field_names(values: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    values.getattr("type")?.getattr("names")?.extract()
}

pub(crate) fn arrow_ordinate_values(
    py: Python<'_>,
    array: Bound<'_, PyAny>,
    field: &'static str,
    base: usize,
    span: usize,
) -> PyResult<ArrowOrdinateValues> {
    Ok(ArrowOrdinateValues {
        values: arrow_f64_values_span(py, &array, base, span)?,
        base,
        validity: arrow_validity_window(py, &array, base, span)?,
        field,
    })
}

pub(crate) fn optional_arrow_ordinate_values(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    field_names: &[String],
    field: &'static str,
    base: usize,
    span: usize,
) -> PyResult<Option<ArrowOrdinateValues>> {
    if !field_names.iter().any(|name| name == field) {
        return Ok(None);
    }
    let array = values.call_method1("field", (field,))?;
    arrow_ordinate_values(py, array, field, base, span).map(Some)
}

/// One selected nesting level of an Arrow list / large_list array.
///
/// Only the `(row_count + 1)` visible offset slots are owned. Their values are
/// rebased to `child_base`, while public ranges remain absolute child indices
/// so the next nested level can project the same selected span.
pub(crate) struct ArrowListLevel {
    /// Child indices rebased to [`Self::child_base`].
    pub(crate) offsets: Vec<usize>,
    /// Logical row index represented by `offsets[0]`.
    row_base: usize,
    /// Original child index represented by a rebased offset of zero.
    child_base: usize,
    /// Child array length the offsets index into (N2 terminal bound).
    pub(crate) child_len: usize,
}

impl ArrowListLevel {
    pub(crate) fn read(py: Python<'_>, array: &Bound<'_, PyAny>) -> PyResult<Self> {
        Self::read_selected(py, array, 0, array.len()?)
    }

    /// Project `row_count` rows beginning at logical child row `row_base`.
    /// The array's own Arrow offset is added only while snapshotting the
    /// physical offset bytes; no physical-parent prefix is copied.
    pub(crate) fn read_selected(
        py: Python<'_>,
        array: &Bound<'_, PyAny>,
        row_base: usize,
        row_count: usize,
    ) -> PyResult<Self> {
        let rows = array.len()?;
        let row_end = row_base
            .checked_add(row_count)
            .ok_or_else(|| geoarrow_parse_error("Arrow list child window overflows"))?;
        if row_end > rows {
            return Err(geoarrow_parse_error(
                "Arrow list child window exceeds child array length",
            ));
        }
        let physical_start = arrow_array_offset(array)?
            .checked_add(row_base)
            .ok_or_else(|| geoarrow_parse_error("Arrow offsets window overflows the buffer"))?;
        let slots = row_count
            .checked_add(1)
            .ok_or_else(|| geoarrow_parse_error("Arrow offset count overflows"))?;
        let raw = list_offsets_as_usize_window(py, array, physical_start, slots)?;
        let child_base = *raw
            .first()
            .ok_or_else(|| geoarrow_parse_error("Arrow offsets buffer is shorter than declared"))?;
        let offsets = raw
            .into_iter()
            .map(|value| {
                value
                    .checked_sub(child_base)
                    .ok_or_else(|| geoarrow_parse_error("Arrow offsets must be ordered"))
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Self {
            offsets,
            row_base,
            child_base,
            child_len: array.getattr("values")?.len()?,
        })
    }

    /// Verify the offsets buffer covers `count` rows starting at `start` and
    /// is non-decreasing across that whole window (null slots included — m01).
    /// Terminal offset must not exceed [`Self::child_len`] (N2).
    pub(crate) fn ensure(&self, start: usize, count: usize) -> PyResult<()> {
        let window = self.local_row(start)?;
        // Validate the compact rebased representation first, then compare the
        // original terminal against the physical child length so diagnostics
        // retain Arrow's absolute offset values.
        ensure_usize_offsets_monotonic(&self.offsets, window, count, usize::MAX)?;
        ensure_offset_terminal_within_child(
            self.absolute_offset(window.checked_add(count).ok_or_else(|| {
                geoarrow_parse_error("Arrow offsets window overflows the buffer")
            })?)?,
            self.child_len,
        )
    }

    /// Child range of the `index`-th visible row, ordering-checked.
    pub(crate) fn range(&self, index: usize) -> PyResult<std::ops::Range<usize>> {
        let index = self.local_row(index)?;
        let start = self.absolute_offset(index)?;
        let end =
            self.absolute_offset(index.checked_add(1).ok_or_else(|| {
                geoarrow_parse_error("Arrow offsets window overflows the buffer")
            })?)?;
        if start > end {
            return Err(geoarrow_parse_error("Arrow offsets must be ordered"));
        }
        Ok(start..end)
    }

    /// Child position of the visible-window endpoint `position` (for slicing
    /// the coordinate span out of nested buffers).
    pub(crate) fn endpoint(&self, position: usize) -> PyResult<usize> {
        self.absolute_offset(self.local_row(position)?)
    }

    fn local_row(&self, row: usize) -> PyResult<usize> {
        row.checked_sub(self.row_base)
            .filter(|&index| index < self.offsets.len())
            .ok_or_else(|| geoarrow_parse_error("Arrow offsets buffer is shorter than declared"))
    }

    fn absolute_offset(&self, index: usize) -> PyResult<usize> {
        self.child_base
            .checked_add(usize_offset_at(&self.offsets, index)?)
            .ok_or_else(|| geoarrow_parse_error("Arrow offset exceeds usize range"))
    }
}

/// Read one list or large_list offset window as absolute `usize` child
/// indices. The caller rebases the values immediately after this snapshot.
///
/// GeoArrow SHOULD accept LargeList (`+L` / i64 offsets); convert once here so
/// every nested decoder shares one offset representation.
fn list_offsets_as_usize_window(
    py: Python<'_>,
    array: &Bound<'_, PyAny>,
    start: usize,
    count: usize,
) -> PyResult<Vec<usize>> {
    let value_type = array.getattr("type")?;
    let is_large = if let Ok(format) = value_type.getattr("format")
        && let Ok(format) = format.extract::<String>()
    {
        format == "+L"
    } else if let Ok(name) = value_type.get_type().name() {
        name == "LargeListType"
    } else {
        false
    };
    if is_large {
        let raw = arrow_i64_offsets_window(py, array, start, count)?;
        raw.into_iter().map(i64_offset_to_usize).collect()
    } else {
        let raw = arrow_i32_offsets_window(py, array, start, count)?;
        raw.into_iter().map(i32_offset_to_usize).collect()
    }
}

/// Two nesting levels of an Arrow polygon array: polygon offsets and ring
/// offsets, plus the coordinate span extraction the packed import lane needs.
pub(crate) struct ArrowPolygonLevels {
    pub(crate) polygons: ArrowListLevel,
    pub(crate) rings: ArrowListLevel,
}

impl ArrowPolygonLevels {
    pub(crate) fn read(py: Python<'_>, array: &Bound<'_, PyAny>) -> PyResult<Self> {
        let polygons = ArrowListLevel::read(py, array)?;
        let len = array.len()?;
        polygons.ensure(0, len)?;
        let rings = array.getattr("values")?;
        let ring_start = polygons.endpoint(0)?;
        let ring_end = polygons.endpoint(len)?;
        Ok(Self {
            polygons,
            rings: ArrowListLevel::read_selected(
                py,
                &rings,
                ring_start,
                ring_end
                    .checked_sub(ring_start)
                    .ok_or_else(|| geoarrow_parse_error("Arrow offsets must be ordered"))?,
            )?,
        })
    }

    /// Visible coordinate span across every ring of every polygon row.
    pub(crate) fn visible_coordinate_span(&self, len: usize) -> PyResult<(usize, usize)> {
        coordinate_span(
            self.rings.endpoint(self.polygons.endpoint(0)?)?,
            self.rings.endpoint(self.polygons.endpoint(len)?)?,
        )
    }
}

/// Ensure a ring's vertex range lies entirely within the loaded coordinate
/// span `[base, base + span)` before any ordinate indexing.
pub(crate) fn ensure_vertex_range_in_span(
    vertex_range: &std::ops::Range<usize>,
    base: usize,
    span: usize,
) -> PyResult<()> {
    let end = base
        .checked_add(span)
        .ok_or_else(|| geoarrow_parse_error("Arrow coordinate span overflows"))?;
    if vertex_range.start < base || vertex_range.end > end {
        return Err(geoarrow_parse_error(
            "Arrow ring vertex range exceeds the loaded coordinate span",
        ));
    }
    Ok(())
}

/// Whether a ring's first and last vertices differ on any **active** ordinate
/// (X/Y/Z/M) — the packed lane cannot auto-close rings the way [`Ring::closed`]
/// does on the per-row path.
///
/// Uses [`same_active_position`] — the same pack-admission / D05 pickle
/// predicate as [`crate::array::ring_seq_is_packable`]. GeoArrow requires
/// endpoint identity on all active ordinates; XY-only comparison would admit
/// Z/M-open rings into trusted packed state the unpickler rejects (F4).
///
/// Fallible: validates the vertex range against the ordinate buffers before
/// indexing so malformed nested offsets raise a clean parse error instead of
/// panicking on out-of-bounds access.
pub(crate) fn arrow_ring_needs_closure(
    coordinates: &ArrowCoordinateValues,
    vertex_range: &std::ops::Range<usize>,
) -> PyResult<bool> {
    if vertex_range.is_empty() {
        return Ok(false);
    }
    let len = vertex_range
        .end
        .checked_sub(vertex_range.start)
        .ok_or_else(|| geoarrow_parse_error("Arrow ring vertex range is inverted"))?;
    coordinates.x.ensure_range(vertex_range.start, len)?;
    coordinates.y.ensure_range(vertex_range.start, len)?;
    if let Some(z) = coordinates.z.as_ref() {
        z.ensure_range(vertex_range.start, len)?;
    }
    if let Some(m) = coordinates.m.as_ref() {
        m.ensure_range(vertex_range.start, len)?;
    }
    let first = vertex_range.start;
    let last = vertex_range.end - 1;
    // Comparison-only Points (finiteness is gated on the coordinate span).
    let first_pt = Point::new_unchecked_axes(
        coordinates.x.value(first),
        coordinates.y.value(first),
        ZOrdinate(coordinates.z.as_ref().map(|values| values.value(first))),
        MOrdinate(coordinates.m.as_ref().map(|values| values.value(first))),
    );
    let last_pt = Point::new_unchecked_axes(
        coordinates.x.value(last),
        coordinates.y.value(last),
        ZOrdinate(coordinates.z.as_ref().map(|values| values.value(last))),
        MOrdinate(coordinates.m.as_ref().map(|values| values.value(last))),
    );
    Ok(!same_active_position(first_pt, last_pt))
}

pub(crate) fn arrow_polygon_from_ring_range(
    coordinates: &ArrowCoordinateValues,
    rings: &ArrowListLevel,
    range: std::ops::Range<usize>,
    row: usize,
) -> PyResult<Shape> {
    // Outer list empty (`[[]]` / zero rings) is the GeoArrow POLYGON EMPTY form;
    // axes come from the coordinate struct schema, not from vertices.
    if range.is_empty() {
        return Ok(Shape::typed_empty(EmptyKind::Polygon, coordinates.axes()));
    }
    let mut shell_and_holes = crate::try_vec_with_capacity(range.len())?;
    for ring_index in range {
        let ring = rings.range(ring_index)?;
        // Same admission as WKT/WKB/pickle (`admit_closed_ring`): <3 corners
        // reject; XY-open with ≥3 corners silent-close; XY-closed Z/M-open
        // reject; fully closed with ≥4 vertices accept. Never use
        // `Ring::closed` here — it only XY-closes and can admit short rings
        // the other three ingresses refuse.
        let seq = coordinates.coordseq(ring.start, ring.end, row)?;
        let admitted = crate::io::admit_closed_ring(seq).map_err(|error| {
            // Preserve the GeoArrow parse surface while keeping the shared
            // policy's reason (RingTooShort / active-ordinate close / OOM).
            let reason = error.to_string();
            if reason.contains("closed on all active ordinates") {
                geoarrow_parse_error("Arrow polygon ring must be closed on all active ordinates")
            } else if let crate::error::ErrorKind::Geometry(
                crate::geometry::GeometryErrorKind::RingTooShort(n),
            ) = error.kind()
            {
                geoarrow_parse_error(format!(
                    "Arrow polygon rings require at least three coordinates, got {n}"
                ))
            } else {
                geoarrow_parse_error(format!("Arrow polygon ring: {reason}"))
            }
        })?;
        shell_and_holes.push(admitted);
    }
    let mut shell_and_holes = shell_and_holes.into_iter();
    let shell = shell_and_holes
        .next()
        .expect("range.is_empty checked above");
    Ok(Shape::Polygon(Polygon::new(
        shell,
        shell_and_holes.collect(),
    )))
}
