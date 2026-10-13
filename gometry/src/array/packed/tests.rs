use super::*;
use crate::geometry::storage::packed_columns::{PackedColumnOutput, map_selected_points};
use crate::geometry::storage::{RowSelectionRef, ShapeRow, validity as missing};
use crate::geometry::{CoordWindow, GeometryKind, LineSeq, MColumn, SharedSlice};
use crate::heap_size::HeapSize as _;

fn prior_storage_heap_bytes(layout: &GeometryArrayStorage) -> usize {
    let owner = crate::heap_size::ARC_CONTROL_BYTES
        .saturating_add(std::mem::size_of::<GeometryArrayStorage>());
    owner.saturating_add(match layout {
        GeometryArrayStorage::Mixed(shapes) => {
            let mut memo = ShapeHeapMemo::default();
            let nested = shapes.iter().fold(0_usize, |bytes, shape| {
                bytes.saturating_add(memo.heap_bytes(shape))
            });
            shapes
                .capacity()
                .saturating_mul(std::mem::size_of::<Shape>())
                .saturating_add(nested)
        },
        GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
            let rows = storage::point_logical_len(coords, row_map.as_deref());
            crate::heap_size::ARC_CONTROL_BYTES
                .saturating_add(std::mem::size_of::<CoordSeq>())
                .saturating_add(coords.logical_heap_bytes_for_count_saturating(rows))
                .saturating_add(row_map.heap_bytes())
        },
        GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords,
            offsets,
            row_map,
        }) => {
            let map = row_map.as_deref();
            let rows = storage::line_logical_len(offsets.as_slice(), map);
            let vertices = (0..rows).fold(0_usize, |count, row| {
                count.saturating_add(map.csr_window(offsets.as_slice(), row).len())
            });
            crate::heap_size::ARC_CONTROL_BYTES
                .saturating_add(std::mem::size_of::<CoordSeq>())
                .saturating_add(coords.logical_heap_bytes_for_count_saturating(vertices))
                .saturating_add(offsets.logical_heap_bytes_for_count(rows.saturating_add(1)))
                .saturating_add(row_map.heap_bytes())
        },
        GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        }) => {
            let map = row_map.as_deref();
            let polygons = polygon_offsets.as_slice();
            let rings = ring_offsets.as_slice();
            let rows = storage::polygon_logical_len(polygons, map);
            let (ring_count, vertices) =
                (0..rows).fold((0_usize, 0_usize), |(total_rings, total_vertices), row| {
                    let row_rings = map.csr_window(polygons, row);
                    if row_rings.is_empty() {
                        (total_rings, total_vertices)
                    } else {
                        (
                            total_rings.saturating_add(row_rings.len()),
                            total_vertices
                                .saturating_add(storage::csr_value_window(rings, row_rings).len()),
                        )
                    }
                });
            crate::heap_size::ARC_CONTROL_BYTES
                .saturating_add(std::mem::size_of::<CoordSeq>())
                .saturating_add(coords.logical_heap_bytes_for_count_saturating(vertices))
                .saturating_add(
                    ring_offsets.logical_heap_bytes_for_count(ring_count.saturating_add(1)),
                )
                .saturating_add(
                    polygon_offsets.logical_heap_bytes_for_count(rows.saturating_add(1)),
                )
                .saturating_add(row_map.heap_bytes())
        },
    })
}

fn prior_selected_row_map_heap_bytes(
    row_map: RowSelectionRef<'_>,
    rows: std::ops::Range<usize>,
) -> usize {
    let Some(first_logical) = rows.clone().next() else {
        return 0;
    };
    let first_physical = storage::physical_row(row_map, first_logical);
    if rows.clone().enumerate().all(|(offset, logical)| {
        first_physical.checked_add(offset) == Some(storage::physical_row(row_map, logical))
    }) {
        0
    } else {
        rows.len().saturating_mul(std::mem::size_of::<usize>())
    }
}

fn prior_range_heap_bytes(array: &PyGeometryArray, rows: std::ops::Range<usize>) -> usize {
    let owner = crate::heap_size::ARC_CONTROL_BYTES
        .saturating_add(std::mem::size_of::<GeometryArrayStorage>());
    owner.saturating_add(match array.storage() {
        GeometryArrayStorage::Mixed(shapes) => {
            let mut memo = ShapeHeapMemo::default();
            let selected = shapes
                .get(rows.clone())
                .expect("test range is bounded by mixed storage");
            selected
                .iter()
                .zip(rows.clone())
                .filter(|(_, logical)| !array.is_row_missing(*logical))
                .fold(
                    rows.len().saturating_mul(std::mem::size_of::<Shape>()),
                    |bytes, (shape, _)| bytes.saturating_add(memo.heap_bytes(shape)),
                )
        },
        GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
            let count = if array.is_compact_nullable() {
                array.missing().map_or_else(
                    || rows.len(),
                    |missing| {
                        rows.clone()
                            .filter(|&logical| missing.is_valid(logical))
                            .count()
                    },
                )
            } else {
                rows.len()
            };
            crate::heap_size::ARC_CONTROL_BYTES
                .saturating_add(std::mem::size_of::<CoordSeq>())
                .saturating_add(coords.logical_heap_bytes_for_count_saturating(count))
                .saturating_add(prior_selected_row_map_heap_bytes(row_map.as_deref(), rows))
        },
        GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords,
            offsets,
            row_map,
        }) => {
            let map = row_map.as_deref();
            let vertices = rows
                .clone()
                .map(|logical| map.csr_window(offsets.as_slice(), logical).len())
                .fold(0_usize, usize::saturating_add);
            crate::heap_size::ARC_CONTROL_BYTES
                .saturating_add(std::mem::size_of::<CoordSeq>())
                .saturating_add(coords.logical_heap_bytes_for_count_saturating(vertices))
                .saturating_add(offsets.logical_heap_bytes_for_count(rows.len().saturating_add(1)))
                .saturating_add(prior_selected_row_map_heap_bytes(map, rows))
        },
        GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        }) => {
            let map = row_map.as_deref();
            let polygons = polygon_offsets.as_slice();
            let ring_offsets_slice = ring_offsets.as_slice();
            let ring_count = rows
                .clone()
                .map(|logical| map.csr_window(polygons, logical).len())
                .fold(0_usize, usize::saturating_add);
            let vertices = rows
                .clone()
                .map(|logical| {
                    let rings = map.csr_window(polygons, logical);
                    if rings.is_empty() {
                        0
                    } else {
                        storage::csr_value_window(ring_offsets_slice, rings).len()
                    }
                })
                .fold(0_usize, usize::saturating_add);
            crate::heap_size::ARC_CONTROL_BYTES
                .saturating_add(std::mem::size_of::<CoordSeq>())
                .saturating_add(coords.logical_heap_bytes_for_count_saturating(vertices))
                .saturating_add(
                    ring_offsets.logical_heap_bytes_for_count(ring_count.saturating_add(1)),
                )
                .saturating_add(
                    polygon_offsets.logical_heap_bytes_for_count(rows.len().saturating_add(1)),
                )
                .saturating_add(prior_selected_row_map_heap_bytes(map, rows))
        },
    })
}

fn measured_lines() -> PyGeometryArray {
    PyGeometryArray::packed_lines(
        CoordSeq::try_from_vecs(
            (0..9).map(f64::from).collect(),
            (0..9).map(|row| f64::from(row) * 2.0).collect(),
            Some((0..9).map(|row| f64::from(row) * 3.0).collect()),
            Some((0..9).map(|row| f64::from(row) * 4.0).collect()),
        )
        .unwrap(),
        CsrOffsetColumn::try_new(vec![0, 2, 2, 5, 8, 9], 9).unwrap(),
        Frame::None,
    )
    .unwrap()
}

fn measured_polygons() -> PyGeometryArray {
    PyGeometryArray::packed_polygons(
        CoordSeq::try_from_vecs(
            (0..17).map(f64::from).collect(),
            (0..17).map(|row| f64::from(row) * 2.0).collect(),
            Some((0..17).map(|row| f64::from(row) * 3.0).collect()),
            Some((0..17).map(|row| f64::from(row) * 4.0).collect()),
        )
        .unwrap(),
        CsrOffsetColumn::<RingLevel>::try_new(vec![0, 4, 8, 13, 17, 17], 17).unwrap(),
        CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 1, 1, 3, 5], 5).unwrap(),
        Frame::None,
    )
    .unwrap()
}

fn measured_points() -> PyGeometryArray {
    PyGeometryArray::packed_points(
        CoordSeq::try_from_vecs(
            (0..5).map(f64::from).collect(),
            (0..5).map(|row| f64::from(row) * 2.0).collect(),
            Some((0..5).map(|row| f64::from(row) * 3.0).collect()),
            Some((0..5).map(|row| f64::from(row) * 4.0).collect()),
        )
        .unwrap(),
        Frame::None,
    )
    .unwrap()
}

fn logical_point_xs(array: &PyGeometryArray) -> Vec<Option<f64>> {
    array
        .logical_rows()
        .map(|row| match row {
            LogicalRow::Missing { .. } => None,
            LogicalRow::Present {
                row: ShapeRow::Point(point),
                ..
            } => Some(point.x),
            LogicalRow::Present { .. } => panic!("expected point row"),
        })
        .collect()
}

mod bounds;
mod nullable;
mod publication;
mod views;
