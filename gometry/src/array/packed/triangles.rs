use super::{
    Arc, CoordSeq, CsrOffsetBuilder, CsrOffsetColumn, Frame, GeometryArrayStorage, PackedStorage,
    Point, PolygonLevel, PyGeometryArray, RingLevel, RowSelection, storage,
};

impl PyGeometryArray {
    /// Build a packed `Polygons` array straight from a flat triangle vertex
    /// stream — `[a, c, b, a]` per triangle (one closed ring of 4 each), as
    /// `Shape::delaunay_triangle_vertices` emits. The CSR offsets are exact
    /// arithmetic ranges (ring `t` is `4t..4t+4`, polygon `t` is ring `t`), so
    /// the whole triangulation lands in ONE coords column with no per-triangle
    /// `Polygon`/`CoordSeq` materialization or re-pack.
    pub(crate) fn packed_triangles(vertices: &[Point], frame: Frame) -> crate::error::Result<Self> {
        let len = vertices.len();
        if !len.is_multiple_of(4) {
            return Err(crate::geometry::GeometryErrorKind::message(format!(
                "packed triangle vertex stream length must be a multiple of 4, got {len}"
            )));
        }
        storage::admit_packed_coord_count(len)?;
        let count = len / 4;
        storage::admit_packed_coord_count(count)?;
        let capacity = count
            .checked_add(1)
            .expect("an admitted triangle count is below the coordinate width");
        let mut rings = Vec::with_capacity(capacity);
        let mut polygons = Vec::with_capacity(capacity);
        rings.extend((0..=count).map(|index| {
            index
                .checked_mul(4)
                .expect("an admitted triangle vertex offset fits usize")
        }));
        polygons.extend(0..=count);
        let ring_offsets = CsrOffsetColumn::<RingLevel>::try_new(rings, len)?;
        let polygon_offsets = CsrOffsetColumn::<PolygonLevel>::try_new(polygons, count)?;
        Self::from_storage(
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords: Arc::new(CoordSeq::try_from_points(vertices)?),
                ring_offsets,
                polygon_offsets,
                row_map: RowSelection::Identity,
            }),
            frame,
        )
    }

    pub(crate) fn packed_triangles_with_ring_ends(
        vertices: &[Point],
        ring_ends: &[usize],
        frame: Frame,
    ) -> crate::error::Result<Self> {
        storage::admit_packed_coord_count(vertices.len())?;
        storage::admit_packed_coord_count(ring_ends.len())?;
        let count = ring_ends.len();
        let mut rings = CsrOffsetBuilder::with_capacity(count);
        let mut polygons = CsrOffsetBuilder::with_capacity(count);
        let mut previous = 0;
        for (index, &end) in ring_ends.iter().enumerate() {
            let ring_len = end.checked_sub(previous).ok_or_else(|| {
                crate::error::Error::from(crate::geometry::GeometryErrorKind::MalformedCsrOffsets)
            })?;
            if ring_len < 4 || end > vertices.len() {
                return Err(crate::error::Error::from(
                    crate::geometry::GeometryErrorKind::MalformedCsrOffsets,
                ));
            }
            rings.push_end(end, vertices.len())?;
            polygons.push_end(index.wrapping_add(1), count)?;
            previous = end;
        }
        if previous != vertices.len() {
            return Err(crate::error::Error::from(
                crate::geometry::GeometryErrorKind::MalformedCsrOffsets,
            ));
        }
        Self::from_storage(
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords: Arc::new(CoordSeq::try_from_points(vertices)?),
                ring_offsets: rings.finish(vertices.len())?.cast_level::<RingLevel>(),
                polygon_offsets: polygons.finish(count)?.cast_level::<PolygonLevel>(),
                row_map: RowSelection::Identity,
            }),
            frame,
        )
    }
}
