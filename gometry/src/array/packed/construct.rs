use super::{
    Arc, ArrayRows, ComputedCaches, CoordSeq, CoordSeqBuilder, CsrOffsetColumn, FinishedGeometry,
    Frame, GeometryArrayStorage, PackedStorage, PolygonLevel, PyGeometryArray, RingLevel,
    RowSelection, Shape, StorageLayout, ValidityMask, prepared, storage,
};

impl PyGeometryArray {
    /// Materialize compact point validity into coordinate placeholders for an
    /// interchange format that requires one physical point per logical row.
    /// The stored compact payload remains present-only.
    pub(crate) fn materialized_compact_point_coords(
        &self,
    ) -> crate::error::Result<Option<CoordSeq>> {
        if !self.is_compact_nullable() {
            return Ok(None);
        }
        let GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) = self.storage()
        else {
            return Ok(None);
        };
        let axes = coords.axes();
        storage::admit_packed_coord_count(self.len())?;
        let mut builder = CoordSeqBuilder::with_capacity(axes, self.len());
        for row in self.row_operation().rows_with_missing(|| ()) {
            match row {
                crate::array::ArrayRow::Missing { .. } => builder.push_xyzm(
                    f64::NAN,
                    f64::NAN,
                    axes.has_z().then_some(f64::NAN),
                    axes.has_m().then_some(f64::NAN),
                ),
                crate::array::ArrayRow::Present(row) => builder.push(
                    row.row()
                        .point()
                        .expect("compact point storage yields point rows"),
                ),
            }
        }
        Ok(Some(builder.finish_with_missing_placeholders()?))
    }

    /// Attach or normalize a validity mask through the selection constructor.
    ///
    /// The current array is the source of this operation, so source-row
    /// products stay attached to their logical rows. Compact payloads are
    /// selected before publication when the new mask removes additional rows;
    /// no product owner is patched after the result exists.
    pub(crate) fn with_missing_mask(
        self,
        mask: Option<ValidityMask>,
    ) -> crate::error::Result<Self> {
        if self.missing() == mask.as_ref() {
            return Ok(self);
        }
        let rows = self.rows.remask(mask)?;
        Ok(Self::from_selected_rows(
            &self,
            rows,
            self.frame.clone(),
            0..self.len(),
        ))
    }

    /// Publish a row-aligned result from a dense present-only computation.
    /// The present-only layout is explicit, so a result that already carries
    /// a mask cannot be scattered a second time.
    pub(crate) fn scatter_present_rows(
        present: &Self,
        mask: ValidityMask,
    ) -> crate::error::Result<Self> {
        if present.missing().is_some() {
            return Err(crate::geometry::GeometryErrorKind::message(
                "present-only scatter input must be dense",
            ));
        }
        if present.len() != mask.present_count() {
            return Err(crate::geometry::GeometryErrorKind::message(
                "present-row array length must match missing mask present count",
            ));
        }
        Self::from_result_storage(
            Arc::clone(present.storage_arc()),
            present.frame.clone(),
            Some(mask),
            StorageLayout::PresentOnly,
            ComputedCaches::fresh(),
        )
    }

    pub(crate) fn from_storage_arc(
        storage: Arc<GeometryArrayStorage>,
        frame: Frame,
    ) -> crate::error::Result<Self> {
        Self::from_result_storage(
            storage,
            frame,
            None,
            StorageLayout::LogicalRows,
            ComputedCaches::fresh(),
        )
    }

    /// Canonical route for newly computed or materialized data. Geometry and
    /// aggregate caches may be carried explicitly, but prepared/frame
    /// products are always fresh.
    pub(crate) fn from_result_storage(
        storage: Arc<GeometryArrayStorage>,
        frame: Frame,
        missing: Option<ValidityMask>,
        layout: StorageLayout,
        caches: ComputedCaches,
    ) -> crate::error::Result<Self> {
        let rows = ArrayRows::admit(storage, missing, layout)?;
        Ok(Self::from_admitted_rows(rows, frame, caches))
    }

    pub(crate) fn from_finished(finished: FinishedGeometry) -> crate::error::Result<Self> {
        let (rows, frame) = finished.into_array()?;
        Ok(Self::from_admitted_rows(
            rows,
            frame,
            ComputedCaches::fresh(),
        ))
    }

    pub(crate) fn from_admitted_rows(
        rows: ArrayRows,
        frame: Frame,
        caches: ComputedCaches,
    ) -> Self {
        let len = rows.len();
        Self {
            rows,
            frame,
            bounds_cache: caches.bounds_cache,
            total_bounds_cache: caches.total_bounds_cache,
            row_products: prepared::RowProducts::fresh(len),
        }
    }

    /// Publish a source-derived selection with products selected independently
    /// from its admitted payload rows.
    pub(in crate::array) fn from_selected_rows(
        source: &Self,
        rows: ArrayRows,
        frame: Frame,
        source_rows: impl IntoIterator<Item = usize>,
    ) -> Self {
        let products = if rows.missing().is_some_and(|mask| mask.present_count() == 0) {
            prepared::RowProducts::fresh(rows.len())
        } else {
            source.selected_row_products(source_rows)
        };
        let caches = ComputedCaches::fresh();
        Self {
            rows,
            frame,
            bounds_cache: caches.bounds_cache,
            total_bounds_cache: caches.total_bounds_cache,
            row_products: products,
        }
    }

    pub(crate) fn from_storage(
        storage: GeometryArrayStorage,
        frame: Frame,
    ) -> crate::error::Result<Self> {
        Self::from_storage_arc(Arc::new(storage), frame)
    }

    pub(crate) fn retag_frame(&self, frame: Frame) -> Self {
        // Frame products carry exact dependency keys, so retaining this owner
        // preserves valid products while segregating changed-frame products.
        let mut result = self.clone();
        result.frame = frame;
        result.total_bounds_cache = Arc::new(std::sync::OnceLock::new());
        result
    }

    /// Wrap plain shapes as `Mixed` storage with an explicit array frame.
    pub(crate) fn mixed_shapes(shapes: Vec<Shape>, frame: Frame) -> crate::error::Result<Self> {
        Self::from_storage(GeometryArrayStorage::Mixed(shapes), frame)
    }

    /// Wrap a homogeneous point column as packed `Points` storage — no per-row
    /// `PyGeometry` allocation. The shared frame applies to every row.
    pub(crate) fn packed_points(seq: CoordSeq, frame: Frame) -> crate::error::Result<Self> {
        Self::packed_points_mapped(seq, frame, RowSelection::Identity)
    }

    pub(crate) fn packed_points_mapped(
        seq: CoordSeq,
        frame: Frame,
        row_map: RowSelection,
    ) -> crate::error::Result<Self> {
        Self::from_storage(
            GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: Arc::new(seq),
                row_map,
            }),
            frame,
        )
    }

    /// Build storage straight from kernel-output `Shape`s. Uniform packing can
    /// return a resource error; structural admission failures fall through to
    /// `Mixed` without publishing a partially packed result.
    pub(crate) fn storage_from_shapes(
        shapes: Vec<Shape>,
    ) -> crate::error::Result<GeometryArrayStorage> {
        // Use the first row kind to select a uniform column packer.
        match shapes.first() {
            Some(Shape::Point(_)) => {
                if let Some(column) = Self::uniform_point_column(shapes.iter())? {
                    return Ok(GeometryArrayStorage::Packed(PackedStorage::Points {
                        coords: Arc::new(column),
                        row_map: RowSelection::Identity,
                    }));
                }
            },
            Some(Shape::LineString(_)) => {
                if let Some((coords, offsets)) = Self::uniform_line_column(shapes.iter())? {
                    return Ok(GeometryArrayStorage::Packed(PackedStorage::Lines {
                        coords: Arc::new(coords),
                        offsets,
                        row_map: RowSelection::Identity,
                    }));
                }
            },
            Some(Shape::Polygon(_)) => {
                if let Some((coords, ring_offsets, polygon_offsets)) =
                    Self::uniform_polygon_column(shapes.iter())?
                {
                    return Ok(GeometryArrayStorage::Packed(PackedStorage::Polygons {
                        coords: Arc::new(coords),
                        ring_offsets,
                        polygon_offsets,
                        row_map: RowSelection::Identity,
                    }));
                }
            },
            _ => {},
        }
        Ok(GeometryArrayStorage::Mixed(shapes))
    }

    /// Build an array straight from kernel-output `Shape`s: all-`Point`
    /// uniform-axes results pack into one shared `CoordSeq` without ever
    /// materializing per-row wrappers; anything else lands in array-owned
    /// `Mixed` shape storage (no per-row `PyGeometry` / `ShapeData`).
    pub(crate) fn from_shapes(shapes: Vec<Shape>, frame: Frame) -> crate::error::Result<Self> {
        Self::from_storage(Self::storage_from_shapes(shapes)?, frame)
    }

    /// Wrap homogeneous `LineString` rows as packed `Lines` storage: one
    /// shared column set plus CSR vertex offsets.
    pub(crate) fn packed_lines(
        coords: CoordSeq,
        offsets: CsrOffsetColumn,
        frame: Frame,
    ) -> crate::error::Result<Self> {
        Self::packed_lines_mapped(coords, offsets, frame, RowSelection::Identity)
    }

    pub(crate) fn packed_lines_mapped(
        coords: CoordSeq,
        offsets: CsrOffsetColumn,
        frame: Frame,
        row_map: RowSelection,
    ) -> crate::error::Result<Self> {
        Self::from_storage(
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords: Arc::new(coords),
                offsets,
                row_map,
            }),
            frame,
        )
    }

    pub(crate) fn packed_polygons(
        coords: CoordSeq,
        ring_offsets: CsrOffsetColumn<RingLevel>,
        polygon_offsets: CsrOffsetColumn<PolygonLevel>,
        frame: Frame,
    ) -> crate::error::Result<Self> {
        Self::packed_polygons_mapped(
            coords,
            ring_offsets,
            polygon_offsets,
            frame,
            RowSelection::Identity,
        )
    }

    pub(crate) fn packed_polygons_mapped(
        coords: CoordSeq,
        ring_offsets: CsrOffsetColumn<RingLevel>,
        polygon_offsets: CsrOffsetColumn<PolygonLevel>,
        frame: Frame,
        row_map: RowSelection,
    ) -> crate::error::Result<Self> {
        Self::from_storage(
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords: Arc::new(coords),
                ring_offsets,
                polygon_offsets,
                row_map,
            }),
            frame,
        )
    }
}
