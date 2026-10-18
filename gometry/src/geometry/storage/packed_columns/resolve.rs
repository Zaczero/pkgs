use super::{
    Arc, ArrayRows, CoordSeq, CsrOffsetColumn, GeometryArrayStorage, LineColumns, PackedColumns,
    PackedStorage, PointColumns, PolygonColumns, PolygonLevel, RingLevel, RowSelectionRef,
    ValidityMask, storage,
};
use crate::error::BatchError;

pub(crate) enum PackedColumnOutput {
    Points(CoordSeq),
    Lines {
        coords: CoordSeq,
        offsets: CsrOffsetColumn,
    },
    Polygons {
        coords: CoordSeq,
        ring_offsets: CsrOffsetColumn<RingLevel>,
        polygon_offsets: CsrOffsetColumn<PolygonLevel>,
    },
}

fn resolve_columns<'a>(
    storage: &'a PackedStorage,
    missing: Option<&'a ValidityMask>,
) -> PackedColumns<'a> {
    match storage {
        PackedStorage::Points { coords, row_map } => {
            PackedColumns::Points(resolve_point_columns(coords, row_map.as_deref(), missing))
        },
        PackedStorage::Lines {
            coords,
            offsets,
            row_map,
        } => PackedColumns::Lines(resolve_line_columns(
            coords,
            offsets,
            row_map.as_deref(),
            missing,
        )),
        PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        } => PackedColumns::Polygons(resolve_polygon_columns(
            coords,
            ring_offsets,
            polygon_offsets,
            row_map.as_deref(),
            missing,
        )),
    }
}

pub(super) const fn resolve_point_columns<'a>(
    coords: &'a CoordSeq,
    row_map: RowSelectionRef<'a>,
    missing: Option<&'a ValidityMask>,
) -> PointColumns<'a> {
    PointColumns {
        coords,
        row_map,
        missing,
    }
}

pub(super) const fn resolve_line_columns<'a>(
    coords: &'a CoordSeq,
    offsets: &'a CsrOffsetColumn,
    row_map: RowSelectionRef<'a>,
    missing: Option<&'a ValidityMask>,
) -> LineColumns<'a> {
    LineColumns {
        coords,
        offsets,
        row_map,
        missing,
    }
}

pub(super) const fn resolve_polygon_columns<'a>(
    coords: &'a CoordSeq,
    ring_offsets: &'a CsrOffsetColumn<RingLevel>,
    polygon_offsets: &'a CsrOffsetColumn<PolygonLevel>,
    row_map: RowSelectionRef<'a>,
    missing: Option<&'a ValidityMask>,
) -> PolygonColumns<'a> {
    PolygonColumns {
        coords,
        ring_offsets,
        polygon_offsets,
        row_map,
        missing,
    }
}

pub(in crate::geometry::storage) fn resolve_packed_columns(
    owner: &ArrayRows,
) -> Option<PackedColumns<'_>> {
    match owner.storage() {
        GeometryArrayStorage::Packed(packed) => Some(resolve_columns(packed, owner.missing())),
        GeometryArrayStorage::Mixed(_) => None,
    }
}

pub(crate) fn assemble_packed_output(output: PackedColumnOutput) -> GeometryArrayStorage {
    match output {
        PackedColumnOutput::Points(coords) => GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(coords),
            row_map: storage::RowSelection::Identity,
        }),
        PackedColumnOutput::Lines { coords, offsets } => {
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords: Arc::new(coords),
                offsets,
                row_map: storage::RowSelection::Identity,
            })
        },
        PackedColumnOutput::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
        } => GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords: Arc::new(coords),
            ring_offsets,
            polygon_offsets,
            row_map: storage::RowSelection::Identity,
        }),
    }
}

pub(in crate::geometry::storage) const fn batch_err(error: crate::error::Error) -> BatchError {
    BatchError::Batch(error)
}

pub(in crate::geometry::storage) const fn row_err(
    row: usize,
    error: crate::error::Error,
) -> BatchError {
    BatchError::Row { row, error }
}
