use super::{
    CheckedRingWindow, CoordSeq, CoordSeqBuilder, CoordinateReplacement, CsrOffsetBuilder,
    LineColumns, PackedColumnError, PackedColumnOutput, PackedColumnResult, PackedColumns,
    PointColumns, PolygonColumns, ReplacementAxis, RingWindowError, batch_err, csr_value_window,
    storage,
};

#[expect(
    clippy::indexing_slicing,
    reason = "replacement X/Y columns are admitted for the selected output vertex count"
)]
fn push_replacement_vertex(
    builder: &mut CoordSeqBuilder,
    old: &CoordSeq,
    old_index: usize,
    replacement: &CoordinateReplacement,
    replacement_index: usize,
) {
    builder.push_xyzm(
        replacement
            .xs
            .as_ref()
            .map_or_else(|| old.xs()[old_index], |values| values[replacement_index]),
        replacement
            .ys
            .as_ref()
            .map_or_else(|| old.ys()[old_index], |values| values[replacement_index]),
        replacement_axis_value(&replacement.zs, replacement_index, old.zs(), old_index),
        replacement_axis_value(&replacement.ms, replacement_index, old.ms(), old_index),
    );
}

#[expect(
    clippy::indexing_slicing,
    reason = "replacement lengths are admitted for the selected output, and old_index belongs to a resolved packed row window"
)]
fn replacement_axis_value(
    axis: &ReplacementAxis,
    replacement_index: usize,
    old: Option<&[f64]>,
    old_index: usize,
) -> Option<f64> {
    match axis {
        ReplacementAxis::Replace(values) => Some(values[replacement_index]),
        ReplacementAxis::Carry => old.map(|column| column[old_index]),
        ReplacementAxis::Drop => None,
    }
}

fn finish_replacement_builder(
    builder: CoordSeqBuilder,
    allow_mask_placeholders: bool,
) -> PackedColumnResult<CoordSeq> {
    if allow_mask_placeholders {
        builder
            .finish_with_missing_placeholders()
            .map_err(batch_err)
    } else {
        builder.finish().map_err(batch_err)
    }
}

fn replace_point_columns(
    point_columns: &PointColumns<'_>,
    replacement: &CoordinateReplacement,
    replacement_index: &mut usize,
) -> PackedColumnResult<PackedColumnOutput> {
    let old = point_columns.coords();
    let rows = point_columns.rows();
    let capacity = rows.present_count();
    storage::admit_packed_coord_count(capacity).map_err(batch_err)?;
    let mut builder = CoordSeqBuilder::with_capacity(replacement.output_axes(old.axes()), capacity);
    let mut replace_present = |physical| {
        push_replacement_vertex(&mut builder, old, physical, replacement, *replacement_index);
        *replacement_index = (*replacement_index).wrapping_add(1);
    };
    if point_columns.missing.is_some() {
        for (_row, physical) in rows.cursor() {
            if let Some(physical) = physical {
                replace_present(physical);
            }
        }
    } else {
        for logical in 0..point_columns.len() {
            replace_present(point_columns.row_map.physical(logical));
        }
    }
    let coords = builder.finish().map_err(batch_err)?;
    Ok(PackedColumnOutput::Points(coords))
}

fn replace_line_columns<const HAS_MISSING: bool>(
    line_columns: &LineColumns<'_>,
    replacement: &CoordinateReplacement,
    replacement_index: &mut usize,
) -> PackedColumnResult<PackedColumnOutput> {
    let old = line_columns.coords();
    let capacity = line_columns.materialized_present_vertices(2)?;
    let mut builder = CoordSeqBuilder::with_capacity(replacement.output_axes(old.axes()), capacity);
    let mut offsets = CsrOffsetBuilder::with_capacity(line_columns.rows());
    for row in 0..line_columns.rows() {
        if HAS_MISSING && line_columns.is_missing(row) {
            // Missing rows carry no logical child coordinates.
        } else {
            let window = line_columns.row_window(row);
            if window.len() >= 2 {
                for old_index in window {
                    push_replacement_vertex(
                        &mut builder,
                        old,
                        old_index,
                        replacement,
                        *replacement_index,
                    );
                    *replacement_index = (*replacement_index).wrapping_add(1);
                }
            }
        }
        offsets
            .push_end(builder.len(), i32::MAX as usize)
            .map_err(batch_err)?;
    }
    let coords = finish_replacement_builder(builder, HAS_MISSING)?;
    let cap = coords.len();
    Ok(PackedColumnOutput::Lines {
        coords,
        offsets: offsets.finish(cap).map_err(batch_err)?,
    })
}

fn replace_polygon_columns<const HAS_MISSING: bool>(
    polygon_columns: &PolygonColumns<'_>,
    replacement: &CoordinateReplacement,
    replacement_index: &mut usize,
) -> PackedColumnResult<PackedColumnOutput> {
    let old = polygon_columns.coords();
    let (vertex_capacity, ring_capacity) = polygon_columns.materialized_present_sizes()?;
    let mut builder =
        CoordSeqBuilder::with_capacity(replacement.output_axes(old.axes()), vertex_capacity);
    let mut ring_builder = CsrOffsetBuilder::with_capacity(ring_capacity);
    let mut polygon_builder = CsrOffsetBuilder::with_capacity(polygon_columns.rows());
    let mut ring_count = 0_usize;
    for row in 0..polygon_columns.rows() {
        if HAS_MISSING && polygon_columns.is_missing(row) {
            polygon_builder
                .push_end(ring_count, i32::MAX as usize)
                .map_err(batch_err)?;
            continue;
        }
        let packed_row = polygon_columns.packed_row(row);
        let row_rings = packed_row
            .polygon_rings()
            .expect("packed polygon row has a ring window");
        for ring in row_rings.clone() {
            let window = packed_row
                .polygon_ring_window(ring)
                .expect("ring lies in its admitted polygon row");
            for old_index in window {
                push_replacement_vertex(
                    &mut builder,
                    old,
                    old_index,
                    replacement,
                    *replacement_index,
                );
                *replacement_index = (*replacement_index).wrapping_add(1);
            }
            ring_builder
                .push_end(builder.len(), i32::MAX as usize)
                .map_err(batch_err)?;
        }
        ring_count = ring_count.wrapping_add(row_rings.len());
        polygon_builder
            .push_end(ring_count, i32::MAX as usize)
            .map_err(batch_err)?;
    }
    let coords = finish_replacement_builder(builder, HAS_MISSING)?;
    let coord_len = coords.len();
    let ring_offsets = ring_builder
        .finish(coord_len)
        .map_err(batch_err)?
        .cast_level();
    let polygon_offsets = polygon_builder
        .finish(ring_count)
        .map_err(batch_err)?
        .cast_level();
    for row in 0..polygon_columns.rows() {
        let rings = csr_value_window(polygon_offsets.as_slice(), row..row.wrapping_add(1));
        for ring in rings {
            let window = csr_value_window(ring_offsets.as_slice(), ring..ring.wrapping_add(1));
            validate_replacement_ring(&coords, window)
                .map_err(|error| PackedColumnError::Row { row, error })?;
        }
    }
    Ok(PackedColumnOutput::Polygons {
        coords,
        ring_offsets,
        polygon_offsets,
    })
}

fn replace_packed_columns_for_mask<const HAS_MISSING: bool>(
    columns: PackedColumns<'_>,
    replacement: &CoordinateReplacement,
    replacement_index: &mut usize,
) -> PackedColumnResult<PackedColumnOutput> {
    match columns {
        PackedColumns::Points(point_columns) => {
            replace_point_columns(&point_columns, replacement, replacement_index)
        },
        PackedColumns::Lines(line_columns) => {
            replace_line_columns::<HAS_MISSING>(&line_columns, replacement, replacement_index)
        },
        PackedColumns::Polygons(polygon_columns) => {
            replace_polygon_columns::<HAS_MISSING>(&polygon_columns, replacement, replacement_index)
        },
    }
}

pub(crate) fn validate_replacement_ring(
    coords: &CoordSeq,
    window: std::ops::Range<usize>,
) -> crate::error::Result<()> {
    match CheckedRingWindow::checked(coords, window) {
        Ok(_) => Ok(()),
        Err(RingWindowError::TooShort(len)) => {
            Err(crate::geometry::GeometryErrorKind::RingTooShort(len).into())
        },
        Err(RingWindowError::Open) => Err(crate::geometry::GeometryErrorKind::Invalid(
            "polygon ring must be closed".into(),
        )
        .into()),
        Err(RingWindowError::Inverted | RingWindowError::OutOfRange) => Err(
            crate::geometry::GeometryErrorKind::Invalid("polygon ring range is invalid".into())
                .into(),
        ),
    }
}

pub(crate) fn replace_packed_columns_dispatch(
    columns: PackedColumns<'_>,
    replacement: &CoordinateReplacement,
    replacement_index: &mut usize,
) -> PackedColumnResult<PackedColumnOutput> {
    let has_missing = match &columns {
        PackedColumns::Points(columns) => columns.missing.is_some(),
        PackedColumns::Lines(columns) => columns.missing.is_some(),
        PackedColumns::Polygons(columns) => columns.missing.is_some(),
    };
    if has_missing {
        replace_packed_columns_for_mask::<true>(columns, replacement, replacement_index)
    } else {
        replace_packed_columns_for_mask::<false>(columns, replacement, replacement_index)
    }
}
