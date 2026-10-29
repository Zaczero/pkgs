//! Contiguous coordinate and CSR materialization of selected packed rows.

use std::sync::Arc;

use super::builder::{PackedColumnBuilder, PointColumnBuilder};
use super::packed_columns::batch_err;
use crate::error::{BatchResult, Result};
use crate::geometry::storage::{
    GeometryArrayStorage, PackedStorage, RowSelection, RowSelectionRef, admit_packed_coord_count,
    materialized_packed_lines_len, materialized_packed_polygons_len,
};
use crate::geometry::{
    CoordSeq, CsrOffsetBuilder, CsrOffsetColumn, PolygonLevel, RingLevel, storage,
};

pub(crate) fn gather_point_columns(coords: &CoordSeq, map: &[usize]) -> Result<CoordSeq> {
    admit_packed_coord_count(map.len())?;
    let mut builder = PointColumnBuilder::with_capacity(coords.axes(), map.len());
    for &physical in map {
        builder.push_at(coords, physical);
    }
    builder.finish()
}

pub(crate) fn gather_line_columns(
    coords: &CoordSeq,
    offsets: &[i32],
    selection: RowSelectionRef<'_>,
) -> Result<(CoordSeq, CsrOffsetColumn)> {
    let rows = storage::line_logical_len(offsets, selection);
    let vertices = materialized_packed_lines_len(offsets, selection)?;
    let mut builder = PackedColumnBuilder::like(coords, vertices, rows);
    for row in 0..rows {
        let window = selection.csr_window(offsets, row);
        builder.push_window_admitted(coords, window)?;
    }
    let cap = builder.vertex_len();
    builder.finish(cap)
}

pub(crate) fn gather_polygon_columns(
    coords: &CoordSeq,
    ring_offsets: &[i32],
    polygon_offsets: &[i32],
    map: &[usize],
) -> Result<(
    CoordSeq,
    CsrOffsetColumn<RingLevel>,
    CsrOffsetColumn<PolygonLevel>,
)> {
    let selection = RowSelectionRef::Gather(map);
    let (vertices, rings) =
        materialized_packed_polygons_len(ring_offsets, polygon_offsets, selection)?;
    let mut builder = PackedColumnBuilder::like(coords, vertices, rings);
    let mut polygon_builder = CsrOffsetBuilder::with_capacity(map.len());
    let mut ring_index = 0_usize;
    for &physical in map {
        let rings = RowSelectionRef::Identity.csr_window(polygon_offsets, physical);
        for ring in rings.clone() {
            let window = RowSelectionRef::Identity.csr_window(ring_offsets, ring);
            builder.push_window_admitted(coords, window)?;
        }
        ring_index = ring_index.wrapping_add(rings.len());
        polygon_builder.push_end(ring_index, ring_index)?;
    }
    let cap = builder.vertex_len();
    let (out_coords, out_ring_offsets) = builder.finish(cap)?;
    let out_ring_offsets = out_ring_offsets.cast_level::<RingLevel>();
    let out_polygon_offsets = polygon_builder
        .finish(out_ring_offsets.len().saturating_sub(1))?
        .cast_level::<PolygonLevel>();
    Ok((out_coords, out_ring_offsets, out_polygon_offsets))
}

/// Copy the logical rows of a storage owner into detached packed columns.
pub(crate) fn materialized_storage(storage: &GeometryArrayStorage) -> Result<GeometryArrayStorage> {
    let packed = match storage {
        GeometryArrayStorage::Mixed(shapes) => {
            return Ok(GeometryArrayStorage::Mixed(
                shapes
                    .iter()
                    .map(|shape| shape.map_points(&Ok))
                    .collect::<Result<Vec<_>>>()?,
            ));
        },
        GeometryArrayStorage::Packed(packed) => packed,
    };
    let materialized = match packed {
        PackedStorage::Points { coords, row_map } => {
            let selection = row_map.as_deref();
            let map = (0..selection.len(coords.len()))
                .map(|row| selection.physical(row))
                .collect::<Vec<_>>();
            PackedStorage::Points {
                coords: Arc::new(gather_point_columns(coords, &map)?),
                row_map: RowSelection::Identity,
            }
        },
        PackedStorage::Lines {
            coords,
            offsets,
            row_map,
        } => {
            let (coords, offsets) =
                gather_line_columns(coords, offsets.as_slice(), row_map.as_deref())?;
            PackedStorage::Lines {
                coords: Arc::new(coords),
                offsets,
                row_map: RowSelection::Identity,
            }
        },
        PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        } => {
            let selection = row_map.as_deref();
            let map = (0..selection.len(polygon_offsets.len().saturating_sub(1)))
                .map(|row| selection.physical(row))
                .collect::<Vec<_>>();
            let (coords, ring_offsets, polygon_offsets) = gather_polygon_columns(
                coords,
                ring_offsets.as_slice(),
                polygon_offsets.as_slice(),
                &map,
            )?;
            PackedStorage::Polygons {
                coords: Arc::new(coords),
                ring_offsets,
                polygon_offsets,
                row_map: RowSelection::Identity,
            }
        },
    };
    Ok(GeometryArrayStorage::Packed(materialized))
}

/// Materialize a fancy-selected (`Gather`) packed storage into contiguous
/// `Identity` columns. `None` when the storage is not gather-selected —
/// Identity and Window resolve zero-copy and never need materialization.
pub(crate) fn materialized_gather(
    storage: &GeometryArrayStorage,
) -> BatchResult<Option<GeometryArrayStorage>> {
    match storage {
        GeometryArrayStorage::Packed(packed) => {
            Ok(materialize_gather(packed)?.map(GeometryArrayStorage::Packed))
        },
        GeometryArrayStorage::Mixed(_) => Ok(None),
    }
}

fn materialize_gather(storage: &PackedStorage) -> BatchResult<Option<PackedStorage>> {
    Ok(match storage {
        PackedStorage::Points { coords, row_map } => match row_map.as_deref() {
            RowSelectionRef::Gather(map) => Some(PackedStorage::Points {
                coords: Arc::new(gather_point_columns(coords, map).map_err(batch_err)?),
                row_map: RowSelection::Identity,
            }),
            RowSelectionRef::Identity | RowSelectionRef::Window { .. } => None,
        },
        PackedStorage::Lines {
            coords,
            offsets,
            row_map,
        } => match row_map.as_deref() {
            RowSelectionRef::Gather(map) => {
                let (out_coords, out_offsets) =
                    gather_line_columns(coords, offsets.as_slice(), RowSelectionRef::Gather(map))
                        .map_err(batch_err)?;
                Some(PackedStorage::Lines {
                    coords: Arc::new(out_coords),
                    offsets: out_offsets,
                    row_map: RowSelection::Identity,
                })
            },
            RowSelectionRef::Identity | RowSelectionRef::Window { .. } => None,
        },
        PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        } => match row_map.as_deref() {
            RowSelectionRef::Gather(map) => {
                let (out_coords, out_ring_offsets, out_polygon_offsets) = gather_polygon_columns(
                    coords,
                    ring_offsets.as_slice(),
                    polygon_offsets.as_slice(),
                    map,
                )
                .map_err(batch_err)?;
                Some(PackedStorage::Polygons {
                    coords: Arc::new(out_coords),
                    ring_offsets: out_ring_offsets,
                    polygon_offsets: out_polygon_offsets,
                    row_map: RowSelection::Identity,
                })
            },
            RowSelectionRef::Identity | RowSelectionRef::Window { .. } => None,
        },
    })
}
