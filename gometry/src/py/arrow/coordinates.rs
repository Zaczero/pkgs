#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::sync::Arc;

use crate::geometry::{CoordWindow, MOrdinate, ZOrdinate, column_all_finite};
use crate::py::arrow::*;

/// Decode the coordinate struct columns for the visible coordinate run
/// `[base, base + span)` only — not the whole (possibly large parent) buffer.
pub(crate) fn arrow_coordinate_values(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    base: usize,
    span: usize,
) -> PyResult<ArrowCoordinateValues> {
    let xs = values.call_method1("field", ("x",))?;
    let ys = values.call_method1("field", ("y",))?;
    let field_names = arrow_struct_field_names(values)?;
    Ok(ArrowCoordinateValues {
        x: arrow_ordinate_values(py, xs, "x", base, span)?,
        y: arrow_ordinate_values(py, ys, "y", base, span)?,
        z: optional_arrow_ordinate_values(py, values, &field_names, "z", base, span)?,
        m: optional_arrow_ordinate_values(py, values, &field_names, "m", base, span)?,
        value_validity: arrow_validity(py, values)?,
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

    pub(crate) fn points(&self, start: usize, end: usize, row: usize) -> PyResult<Vec<Point>> {
        self.ensure_ranges(start, end - start)?;
        let count = end - start;
        // Coordinate span is visible input (16 B/vertex XY); fallible reserve.
        let mut points = crate::try_vec_with_capacity(count)?;
        for point_index in start..end {
            if !self.is_valid(point_index) {
                return Err(arrow_null_error(row));
            }
            points.push(Point::new_axes(
                self.x.value(point_index),
                self.y.value(point_index),
                ZOrdinate(self.z.as_ref().map(|values| values.value(point_index))),
                MOrdinate(self.m.as_ref().map(|values| values.value(point_index))),
            )?);
        }
        Ok(points)
    }

    /// Build a `CoordSeq` for the coordinate range directly from the ordinate
    /// columns — the `SoA` counterpart to [`points`](Self::points) that skips
    /// the per-vertex `Point` construction and the `Vec<Point>` re-gather.
    /// Each ordinate array carries its own Arrow offset, so the columns are
    /// sliced independently. Finiteness is validated only for `[start, end)` —
    /// child spans under outer-null rows are undefined (R06) and must not
    /// reject an otherwise valid present row.
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
        self.value_validity.is_valid(index)
            && self.x.validity.is_valid(index)
            && self.y.validity.is_valid(index)
            && self
                .z
                .as_ref()
                .is_none_or(|values| values.validity.is_valid(index))
            && self
                .m
                .as_ref()
                .is_none_or(|values| values.validity.is_valid(index))
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
        validity: arrow_validity(py, &array)?,
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

/// One nesting level of an Arrow list array: its offsets column plus the
/// slice offset of the visible window. Owns the per-row range extraction so
/// the offsets-ordering check and the slice arithmetic live in one place.
pub(crate) struct ArrowListLevel {
    pub(crate) offsets: Vec<i32>,
    pub(crate) offset: usize,
    /// Child array length the offsets index into (N2 terminal bound).
    pub(crate) child_len: usize,
}

impl ArrowListLevel {
    pub(crate) fn read(py: Python<'_>, array: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            offsets: arrow_i32_offsets(py, array)?,
            offset: arrow_array_offset(array)?,
            child_len: array.getattr("values")?.len()?,
        })
    }

    /// Verify the offsets buffer covers `count` rows starting at `start` and
    /// is non-decreasing across that whole window (null slots included — m01).
    /// Terminal offset must not exceed [`Self::child_len`] (N2).
    pub(crate) fn ensure(&self, start: usize, count: usize) -> PyResult<()> {
        let window = self
            .offset
            .checked_add(start)
            .ok_or_else(|| geoarrow_parse_error("Arrow offsets window overflows the buffer"))?;
        ensure_i32_offsets_monotonic(&self.offsets, window, count, self.child_len)
    }

    /// Child range of the `index`-th visible row, ordering-checked.
    pub(crate) fn range(&self, index: usize) -> PyResult<std::ops::Range<usize>> {
        let start = i32_offset_to_usize(self.offsets[self.offset + index])?;
        let end = i32_offset_to_usize(self.offsets[self.offset + index + 1])?;
        if start > end {
            return Err(geoarrow_parse_error("Arrow offsets must be ordered"));
        }
        Ok(start..end)
    }

    /// Child position of the visible-window endpoint `position` (for slicing
    /// the coordinate span out of nested buffers).
    pub(crate) fn endpoint(&self, position: usize) -> PyResult<usize> {
        offset_at(&self.offsets, self.offset + position)
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
        let rings = array.getattr("values")?;
        Ok(Self {
            polygons,
            rings: ArrowListLevel::read(py, &rings)?,
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
        let points = coordinates.points(ring.start, ring.end, row)?;
        // Ring::closed auto-closes on XY only (`same_point`). A ring already
        // closed in XY but open on Z/M would be accepted verbatim — reject so
        // GeoArrow never builds trusted state the unpickler refuses (F4/D05).
        if let (Some(&first), Some(&last)) = (points.first(), points.last())
            && same_point(first, last)
            && !same_active_position(first, last)
        {
            return Err(geoarrow_parse_error(
                "Arrow polygon ring must be closed on all active ordinates",
            ));
        }
        shell_and_holes.push(Ring::closed(points)?);
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
