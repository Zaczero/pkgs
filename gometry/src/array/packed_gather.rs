//! Gather materialization for fancy-selected packed storage: the
//! per-kind column gathers plus the shared `gathered_memo`
//! normalization used by every packed-column chokepoint.

use crate::array::packed_columns::{PackedColumnResult, batch_err};
use crate::array::{
    Arc, CoordSeq, CsrOffsetBuilder, CsrOffsetColumn, GeometryArrayStorage, PackedColumnBuilder,
    PointColumnBuilder, PolygonLevel, Result, RingLevel, RowSelection, RowSelectionRef,
};

pub(crate) fn gather_point_columns(coords: &CoordSeq, map: &[usize]) -> CoordSeq {
    let mut builder = PointColumnBuilder::like_coords(coords, map.len());
    for &physical in map {
        builder.push_at(coords, physical);
    }
    builder.finish_infallible()
}

pub(crate) fn gather_line_columns(
    coords: &CoordSeq,
    offsets: &[i32],
    map: &[usize],
) -> Result<(CoordSeq, CsrOffsetColumn)> {
    let mut builder = PackedColumnBuilder::like(coords, 0);
    for &physical in map {
        let window = offsets[physical] as usize..offsets[physical + 1] as usize;
        builder.push_window(coords, window)?;
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
    let mut builder = PackedColumnBuilder::like(coords, 0);
    let mut polygon_builder = CsrOffsetBuilder::new();
    let mut ring_index = 0_usize;
    for &physical in map {
        let ring_start = polygon_offsets[physical] as usize;
        let ring_end = polygon_offsets[physical + 1] as usize;
        for ring in ring_start..ring_end {
            let window = ring_offsets[ring] as usize..ring_offsets[ring + 1] as usize;
            builder.push_window(coords, window)?;
            ring_index += 1;
        }
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

/// Materialize a fancy-selected (`Gather`) packed storage into contiguous
/// `Identity` columns. `None` when the storage is not gather-selected —
/// Identity and Window resolve zero-copy and never need materialization.
pub(crate) fn materialized_gather(
    storage: &GeometryArrayStorage,
) -> PackedColumnResult<Option<GeometryArrayStorage>> {
    Ok(match storage {
        GeometryArrayStorage::Mixed(_) => None,
        GeometryArrayStorage::Points { coords, row_map } => match row_map.as_deref() {
            RowSelectionRef::Gather(map) => Some(GeometryArrayStorage::Points {
                coords: Arc::new(gather_point_columns(coords, map)),
                row_map: RowSelection::Identity,
            }),
            _ => None,
        },
        GeometryArrayStorage::Lines {
            coords,
            offsets,
            row_map,
        } => match row_map.as_deref() {
            RowSelectionRef::Gather(map) => {
                let (out_coords, out_offsets) =
                    gather_line_columns(coords, offsets.as_slice(), map).map_err(batch_err)?;
                Some(GeometryArrayStorage::Lines {
                    coords: Arc::new(out_coords),
                    offsets: out_offsets,
                    row_map: RowSelection::Identity,
                })
            },
            _ => None,
        },
        GeometryArrayStorage::Polygons {
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
                Some(GeometryArrayStorage::Polygons {
                    coords: Arc::new(out_coords),
                    ring_offsets: out_ring_offsets,
                    polygon_offsets: out_polygon_offsets,
                    row_map: RowSelection::Identity,
                })
            },
            _ => None,
        },
    })
}

/// Gather-normalize a packed storage through the array's shared memo: the
/// first packed op on a fancy-selected array pays the gather ONCE; every
/// later op on the same array (or a clone) reuses the contiguous columns
/// zero-copy. Identity/Window storages pass straight through. A lost
/// `get_or_init` race costs one redundant gather, never correctness.
pub(crate) fn normalized_gather_storage(
    storage: &Arc<GeometryArrayStorage>,
    memo: &std::sync::OnceLock<Arc<GeometryArrayStorage>>,
) -> PackedColumnResult<Arc<GeometryArrayStorage>> {
    if let Some(hit) = memo.get() {
        return Ok(Arc::clone(hit));
    }
    let Some(built) = materialized_gather(storage)? else {
        return Ok(Arc::clone(storage));
    };
    let built = Arc::new(built);
    Ok(Arc::clone(memo.get_or_init(|| built)))
}
