use super::*;

pub(crate) fn packed_lines_coord_len<'a>(
    offsets: &[i32],
    row_map: impl Into<RowSelectionRef<'a>>,
) -> usize {
    match row_map.into() {
        RowSelectionRef::Identity => offsets.last().map_or(0, |&end| end as usize),
        RowSelectionRef::Window { start, len } => {
            if len == 0 {
                0
            } else {
                csr_value_window(offsets, start..start.wrapping_add(len)).len()
            }
        },
        RowSelectionRef::Gather(map) => map
            .iter()
            .map(|&physical| {
                RowSelectionRef::Identity
                    .csr_window(offsets, physical)
                    .len()
            })
            .sum(),
    }
}

pub(crate) fn packed_polygons_coord_len<'a>(
    ring_offsets: &[i32],
    polygon_offsets: &[i32],
    row_map: impl Into<RowSelectionRef<'a>>,
) -> usize {
    match row_map.into() {
        RowSelectionRef::Identity => ring_offsets.last().map_or(0, |&end| end as usize),
        RowSelectionRef::Window { start, len } => {
            if len == 0 {
                0
            } else {
                csr_value_window(
                    ring_offsets,
                    csr_value_window(polygon_offsets, start..start.wrapping_add(len)),
                )
                .len()
            }
        },
        RowSelectionRef::Gather(map) => map
            .iter()
            .map(|&physical| {
                let rings = RowSelectionRef::Identity.csr_window(polygon_offsets, physical);
                csr_value_window(ring_offsets, rings).len()
            })
            .sum(),
    }
}

pub(crate) fn packed_polygons_ring_len<'a>(
    polygon_offsets: &[i32],
    row_map: impl Into<RowSelectionRef<'a>>,
) -> usize {
    match row_map.into() {
        RowSelectionRef::Identity => polygon_offsets.last().map_or(0, |&end| end as usize),
        RowSelectionRef::Window { start, len } => {
            if len == 0 {
                0
            } else {
                csr_value_window(polygon_offsets, start..start.wrapping_add(len)).len()
            }
        },
        RowSelectionRef::Gather(map) => map
            .iter()
            .map(|&physical| {
                RowSelectionRef::Identity
                    .csr_window(polygon_offsets, physical)
                    .len()
            })
            .sum(),
    }
}

pub(crate) fn admit_packed_coord_count(count: usize) -> crate::error::Result<usize> {
    if count > i32::MAX as usize {
        return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
    }
    Ok(count)
}

pub(crate) fn admit_packed_total(total: usize, additional: usize) -> crate::error::Result<usize> {
    let count = total
        .checked_add(additional)
        .ok_or(GeometryErrorKind::OffsetCapacityExceeded)?;
    admit_packed_coord_count(count)
}

pub(crate) fn materialized_packed_lines_len<'a>(
    offsets: &[i32],
    row_map: impl Into<RowSelectionRef<'a>>,
) -> crate::error::Result<usize> {
    let map = row_map.into();
    let rows = line_logical_len(offsets, map);
    let mut vertices = 0_u128;
    for row in 0..rows {
        vertices = vertices
            .checked_add(map.csr_window(offsets, row).len() as u128)
            .expect("packed line gather count fits u128");
        if vertices > i32::MAX as u128 {
            return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
        }
    }
    Ok(vertices as usize)
}

pub(crate) fn materialized_packed_polygons_len<'a>(
    ring_offsets: &[i32],
    polygon_offsets: &[i32],
    row_map: impl Into<RowSelectionRef<'a>>,
) -> crate::error::Result<(usize, usize)> {
    let map = row_map.into();
    let rows = polygon_logical_len(polygon_offsets, map);
    let mut rings = 0_u128;
    let mut vertices = 0_u128;
    for row in 0..rows {
        let row_rings = map.csr_window(polygon_offsets, row);
        rings = rings
            .checked_add(row_rings.len() as u128)
            .expect("packed polygon ring count fits u128");
        if rings > i32::MAX as u128 {
            return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
        }
        if !row_rings.is_empty() {
            let vertex_window = csr_value_window(ring_offsets, row_rings);
            vertices = vertices
                .checked_add(
                    u128::try_from(vertex_window.len())
                        .expect("packed polygon vertex count fits u128"),
                )
                .expect("packed polygon vertex count fits u128");
            if vertices > i32::MAX as u128 {
                return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
            }
        }
    }
    Ok((vertices as usize, rings as usize))
}
