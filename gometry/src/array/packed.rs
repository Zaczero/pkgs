use std::sync::Arc;

use crate::array::{
    ArrayRows, Bounds, CoordSeq, CoordinateAxes, Crs, CsrOffsetBuilder, CsrOffsetColumn,
    ElementBounds, Frame, GeometryArrayStorage, GeometryKind, InvalidGeometryError, LogicalRow,
    LogicalRows, OriginSpec, PackedColumns, PackedStorage, Point, PointColumnBuilder, PolygonLevel,
    Py, PyAny, PyErr, PyGeometry, PyGeometryArray, PyResult, Python, RingLevel, RowSelection,
    RowSelectionRef, Shape, StorageLayout, TotalBoundsCache, ValidityMask,
    packed_per_row_self_origin_affine_columns, physical_row, point_logical_len, prepared,
    row_map_is_identity, row_ord_extremes, row_selection_from_logical_rows,
};
use crate::geometry::CoordSeqBuilder;
use crate::py::numpy::optional_float64_array;

/// Geometry-derived cache owners supplied to the computed-data constructor.
/// Prepared/frame products deliberately do not live here: every computed
/// result receives a fresh product owner.
pub(crate) struct ComputedCaches {
    bounds_cache: Arc<std::sync::OnceLock<Option<ElementBounds>>>,
    total_bounds_cache: TotalBoundsCache,
}

impl ComputedCaches {
    pub(crate) fn fresh() -> Self {
        Self {
            bounds_cache: Arc::new(std::sync::OnceLock::new()),
            total_bounds_cache: Arc::new(std::sync::OnceLock::new()),
        }
    }

    pub(crate) const fn carried(
        bounds_cache: Arc<std::sync::OnceLock<Option<ElementBounds>>>,
        total_bounds_cache: TotalBoundsCache,
    ) -> Self {
        Self {
            bounds_cache,
            total_bounds_cache,
        }
    }
}

impl PyGeometryArray {
    pub(crate) fn storage(&self) -> &GeometryArrayStorage {
        self.rows.storage().as_ref()
    }

    pub(crate) const fn storage_arc(&self) -> &Arc<GeometryArrayStorage> {
        self.rows.storage()
    }

    pub(crate) const fn missing(&self) -> Option<&ValidityMask> {
        self.rows.missing()
    }

    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn logical_row(&self, logical: usize) -> Option<LogicalRow<'_>> {
        self.rows.logical_row(logical)
    }

    pub(crate) fn logical_rows(&self) -> LogicalRows<'_> {
        self.rows.logical_rows()
    }

    pub(crate) const fn is_compact_nullable(&self) -> bool {
        self.rows.is_compact_nullable()
    }

    pub(crate) const fn point_row_layout(&self) -> crate::array::PointRowLayout {
        self.rows.point_row_layout()
    }

    /// Shared driver for the Z-extreme accessors (`min_z`/`max_z`):
    /// reads packed per-row extremes when storage exposes them, else falls back
    /// to per-shape computation. See [`Self::extreme_lane`].
    pub(crate) fn z_extreme_lane(
        &self,
        py: Python<'_>,
        project: impl Fn(f64, f64) -> f64 + Send,
        from_shape: impl Fn(&Shape) -> Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        self.extreme_lane(
            py,
            self.has_z(),
            |columns| match columns {
                PackedColumns::Points(point_columns) => point_columns.coords().zs(),
                PackedColumns::Lines(line_columns) => line_columns.coords().zs(),
                PackedColumns::Polygons(polygon_columns) => polygon_columns.coords().zs(),
            },
            row_ord_extremes,
            project,
            from_shape,
        )
    }

    /// Shared driver for the M-extreme accessors (`min_m`/`max_m`):
    /// reads packed per-row extremes when storage exposes them, else falls back
    /// to per-shape computation. See [`Self::extreme_lane`].
    pub(crate) fn m_extreme_lane(
        &self,
        py: Python<'_>,
        project: impl Fn(f64, f64) -> f64 + Send,
        from_shape: impl Fn(&Shape) -> Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        self.extreme_lane(
            py,
            self.has_m(),
            |columns| match columns {
                PackedColumns::Points(point_columns) => point_columns.coords().ms(),
                PackedColumns::Lines(line_columns) => line_columns.coords().ms(),
                PackedColumns::Polygons(polygon_columns) => polygon_columns.coords().ms(),
            },
            row_ord_extremes,
            project,
            from_shape,
        )
    }

    /// Shared per-row ordinate-extreme reduction behind the Z and M accessors.
    /// When `present` and storage is packed, `ordinate` selects the contiguous
    /// ordinate column and `row_extremes` reduces each row's window; `project`
    /// maps a row's `(low, high)` pair to the emitted value. Otherwise
    /// `from_shape` recomputes per shape. Absent ordinate stays `None` (`nan`).
    fn extreme_lane(
        &self,
        py: Python<'_>,
        present: bool,
        ordinate: impl for<'a> Fn(&'a PackedColumns<'_>) -> Option<&'a [f64]> + Send,
        row_extremes: impl Fn(&[f64], std::ops::Range<usize>) -> Option<(f64, f64)> + Send,
        project: impl Fn(f64, f64) -> f64 + Send,
        from_shape: impl Fn(&Shape) -> Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        if self.is_compact_nullable() {
            let values = crate::try_collect_results(
                self.row_operation()
                    .rows_with_missing(|| None::<f64>)
                    .map(|row| match row {
                        crate::array::ArrayRow::Missing { value, .. } => Ok(value),
                        crate::array::ArrayRow::Present(row) => row
                            .row()
                            .with_shape(&from_shape)
                            .map_err(crate::PyErr::from),
                    }),
            )?;
            return optional_float64_array(py, values);
        }
        let missing = self.missing().cloned();
        if present
            && let Some(values) = self.reduce_packed_columns_detached(py, move |columns| {
                let column = ordinate(&columns).expect("checked ordinate presence");
                Ok(columns.map_rows(|row| {
                    if missing
                        .as_ref()
                        .is_some_and(|mask| mask.is_missing(row.index()))
                    {
                        return None;
                    }
                    row.vertex_window()
                        .and_then(|window| row_extremes(column, window))
                        .map(|(low, high)| project(low, high))
                }))
            })?
        {
            return optional_float64_array(py, values);
        }
        optional_float64_array(
            py,
            crate::try_collect_results(self.storage().iter_shapes().enumerate().map(
                |(row, shape)| {
                    let shape = shape.map_err(crate::PyErr::from)?;
                    Ok((!self.is_row_missing(row))
                        .then(|| from_shape(&shape))
                        .flatten())
                },
            ))?,
        )
    }

    /// Return the published per-element bounds without initializing the cache.
    /// Cold callers must fall back to their row-local bounds computation.
    pub(crate) fn existing_element_bounds(&self) -> Option<ElementBounds> {
        self.bounds_cache.get().cloned().flatten()
    }

    /// Build and publish the full per-element bounds cache when a caller
    /// explicitly requires it. Every owned allocation is fallible, and a
    /// failed initialization leaves the cache unpublished for a later retry.
    pub(crate) fn try_element_bounds(&self) -> crate::error::Result<Option<ElementBounds>> {
        let bounds = self.bounds_cache.get_or_try_init(|| {
            let mut values = Vec::new();
            crate::boundary::try_reserve_vec(&mut values, self.len())?;
            let columnar = self.try_for_each_element_bounds(|bounds| {
                crate::boundary::try_push_vec(&mut values, bounds)
            })?;
            Ok::<_, crate::error::Error>(columnar.then(|| Arc::new(values)))
        })?;
        Ok(bounds.clone())
    }

    /// Visit each logical row's planar bounds in row order, straight off the
    /// packed columns: `None` for a missing or empty row. Mixed storage has
    /// no columnar fold; it visits nothing and returns `false`.
    pub(crate) fn try_for_each_element_bounds<E>(
        &self,
        mut visit: impl FnMut(Option<Bounds>) -> std::result::Result<(), E>,
    ) -> std::result::Result<bool, E> {
        let storage = self.storage();
        let Some(mask) = self.missing() else {
            return storage.try_for_each_element_bounds(visit);
        };
        let mut logical = 0;
        if storage.len() == mask.len() {
            // One stored window per logical row; the mask decides missing rows.
            return storage.try_for_each_element_bounds(|bounds| {
                let missing = mask.is_missing(logical);
                logical += 1;
                visit(bounds.filter(|_| !missing))
            });
        }
        // Present-only payload: the mask places each stored row.
        let columnar = storage.try_for_each_element_bounds(|bounds| {
            while mask.is_missing(logical) {
                visit(None)?;
                logical += 1;
            }
            logical += 1;
            visit(bounds)
        })?;
        if columnar {
            for _ in logical..mask.len() {
                visit(None)?;
            }
        }
        Ok(columnar)
    }

    /// Whether a scalar geometry shares this array's frame (CRS + epoch) —
    /// the precondition for element-level `__eq__` comparisons.
    pub(crate) fn frame_matches(&self, geom: &PyGeometry) -> bool {
        self.crs_ref() == geom.crs_ref() && self.epoch() == geom.epoch()
    }

    /// Whether any present row carries a Z ordinate.
    pub(crate) fn has_z(&self) -> bool {
        self.any_present_axes(CoordinateAxes::has_z)
    }

    /// Whether any present row carries an M ordinate.
    pub(crate) fn has_m(&self) -> bool {
        self.any_present_axes(CoordinateAxes::has_m)
    }

    /// Whether any present row's ordinate layout satisfies `carries`. Packed
    /// storage shares one coordinate column, so its answer is a single axes
    /// read.
    fn any_present_axes(&self, carries: fn(CoordinateAxes) -> bool) -> bool {
        if self
            .missing()
            .as_ref()
            .is_some_and(|mask| mask.present_count() == 0)
        {
            return false;
        }
        match self.storage() {
            GeometryArrayStorage::Packed(
                PackedStorage::Points { coords, .. }
                | PackedStorage::Lines { coords, .. }
                | PackedStorage::Polygons { coords, .. },
            ) => carries(coords.axes()),
            GeometryArrayStorage::Mixed(shapes) => shapes
                .iter()
                .enumerate()
                .any(|(row, shape)| !self.is_row_missing(row) && carries(shape.axes())),
        }
    }

    /// The single ordinate layout shared by every PRESENT row, or `None` when
    /// present rows disagree. Packed storage owns one layout even through row
    /// selections and missing placeholders, so its answer is O(1). Mixed
    /// missing rows are skipped (their XY placeholder must not vote). Packed
    /// all-missing arrays retain their declared layout; heterogeneous
    /// all-missing arrays are `XY`.
    pub fn uniform_axes(&self) -> crate::error::Result<Option<CoordinateAxes>> {
        if let Some(axes) = self.storage().packed_axes() {
            return Ok(Some(axes));
        }
        if self
            .missing()
            .as_ref()
            .is_some_and(|mask| mask.present_count() == 0)
        {
            return Ok(Some(CoordinateAxes::XY));
        }
        let mut axes = None;
        for (row, shape) in self.storage().iter_shapes().enumerate() {
            if self.is_row_missing(row) {
                continue;
            }
            let shape = shape?;
            let row_axes = shape.axes();
            match axes {
                None => axes = Some(row_axes),
                Some(seen) if seen != row_axes => return Ok(None),
                Some(_) => {},
            }
        }
        Ok(Some(axes.unwrap_or(CoordinateAxes::XY)))
    }

    // --- Frame accessors (#37 migration; see PyGeometry::crs_ref) -----------
    /// The array's CRS as a borrow — internal counterpart to the `crs` getter
    /// (which clones into `PyCrs`). Replaces `self.crs_ref()`.
    pub(crate) const fn crs_ref(&self) -> Option<&Crs> {
        self.frame.crs_ref()
    }

    /// The array's CRS code as `&str` — replaces `self.crs_str()`.
    pub(crate) fn crs_str(&self) -> Option<&str> {
        self.frame.crs_str()
    }

    /// Whether any row is missing (`None` mask = dense fast path).
    pub(crate) const fn has_missing(&self) -> bool {
        self.missing().is_some()
    }

    pub(crate) fn is_row_missing(&self, row: usize) -> bool {
        self.missing()
            .as_ref()
            .is_some_and(|mask| mask.is_missing(row))
    }

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
        let mut builder = CoordSeqBuilder::try_with_capacity(axes, self.len())?;
        for row in self.row_operation().rows_with_missing(|| ()) {
            match row {
                crate::array::ArrayRow::Missing { .. } => builder.push_xyzm(
                    f64::NAN,
                    f64::NAN,
                    axes.has_z().then_some(f64::NAN),
                    axes.has_m().then_some(f64::NAN),
                )?,
                crate::array::ArrayRow::Present(row) => match row.row() {
                    crate::array::ShapeRow::Point(point) => builder.push(point)?,
                    _ => {
                        unreachable!("compact point storage yields point rows")
                    },
                },
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
        let Some(mask) = mask else {
            if self.missing().is_some() {
                return Err(crate::geometry::GeometryErrorKind::message(
                    "cannot clear an existing GeometryArray missing mask",
                ));
            }
            return Ok(self);
        };
        if mask.len() != self.len() {
            return Err(crate::geometry::GeometryErrorKind::message(
                "missing mask length must match array length",
            ));
        }
        if self.missing() == Some(&mask) {
            return Ok(self);
        }
        if self.is_compact_nullable() {
            for (row, present) in mask.cursor() {
                if present.is_some() && self.is_row_missing(row) {
                    return Err(crate::geometry::GeometryErrorKind::message(
                        "a compact nullable array cannot unmask an existing row",
                    ));
                }
            }
            let storage = if matches!(
                self.storage(),
                GeometryArrayStorage::Packed(PackedStorage::Points { .. })
            ) {
                let source = self.missing().expect("compact nullable storage has a mask");
                self.selected_point_present_rows(
                    source.cursor().filter_map(|(logical, present)| {
                        present.filter(|_| mask.is_valid(logical))
                    }),
                )?
            } else {
                self.selected_storage((0..mask.len()).filter(|&logical| !mask.is_missing(logical)))?
            };
            return Self::from_selected_storage(
                &self,
                Arc::new(storage),
                self.frame.clone(),
                Some(mask),
                StorageLayout::PresentOnly,
                0..self.len(),
            );
        }
        Self::from_selected_storage(
            &self,
            Arc::clone(self.storage_arc()),
            self.frame.clone(),
            Some(mask),
            StorageLayout::LogicalRows,
            0..self.len(),
        )
    }

    /// Coordinate-free sentinel retained only to keep heterogeneous `Mixed`
    /// storage row-aligned. Logical access never exposes it.
    pub(crate) const fn missing_placeholder() -> Shape {
        Shape::Empty(GeometryKind::GeometryCollection, CoordinateAxes::XY)
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

    pub(super) fn gather_point_coords(
        coords: &CoordSeq,
        row_map: RowSelectionRef<'_>,
    ) -> crate::error::Result<CoordSeq> {
        let mut builder = PointColumnBuilder::try_with_capacity(
            coords.axes(),
            point_logical_len(coords, row_map),
        )?;
        for logical in 0..point_logical_len(coords, row_map) {
            builder.push_at(coords, physical_row(row_map, logical))?;
        }
        builder.finish()
    }

    fn compact_point_storage_from_logical(
        storage: &GeometryArrayStorage,
        mask: &ValidityMask,
    ) -> crate::error::Result<GeometryArrayStorage> {
        let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) = storage
        else {
            return Err(crate::geometry::GeometryErrorKind::message(
                "logical nullable point storage requires packed points",
            ));
        };
        if storage.len() != mask.len() {
            return Err(crate::geometry::GeometryErrorKind::message(
                "logical nullable payload length must match the missing mask",
            ));
        }
        let mut builder =
            PointColumnBuilder::try_with_capacity(coords.axes(), mask.present_count())?;
        for (logical, present) in mask.cursor() {
            if present.is_some() {
                builder.push_at(coords, physical_row(row_map.as_deref(), logical))?;
            }
        }
        Ok(GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(builder.finish()?),
            row_map: RowSelection::Identity,
        }))
    }

    fn compact_storage_from_logical(
        storage: &GeometryArrayStorage,
        mask: &ValidityMask,
    ) -> crate::error::Result<GeometryArrayStorage> {
        if storage.len() != mask.len() {
            return Err(crate::geometry::GeometryErrorKind::message(
                "logical nullable payload length must match the missing mask",
            ));
        }
        let present_rows = (0..mask.len()).filter(|&logical| !mask.is_missing(logical));
        match storage {
            GeometryArrayStorage::Packed(PackedStorage::Points { .. }) => {
                Self::compact_point_storage_from_logical(storage, mask)
            },
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => {
                let selected = GeometryArrayStorage::Packed(PackedStorage::Lines {
                    coords: Arc::clone(coords),
                    offsets: offsets.clone(),
                    row_map: row_selection_from_logical_rows(
                        row_map.as_deref(),
                        offsets.len().saturating_sub(1),
                        present_rows,
                    )?,
                });
                Self::compact_storage_from_present(&selected, mask)
            },
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => {
                let selected = GeometryArrayStorage::Packed(PackedStorage::Polygons {
                    coords: Arc::clone(coords),
                    ring_offsets: ring_offsets.clone(),
                    polygon_offsets: polygon_offsets.clone(),
                    row_map: row_selection_from_logical_rows(
                        row_map.as_deref(),
                        polygon_offsets.len().saturating_sub(1),
                        present_rows,
                    )?,
                });
                Self::compact_storage_from_present(&selected, mask)
            },
            GeometryArrayStorage::Mixed(_) => Err(crate::geometry::GeometryErrorKind::message(
                "logical nullable mixed storage cannot use packed compaction",
            )),
        }
    }

    /// Convert an explicitly present-only packed payload to the canonical
    /// nullable layout. Lines and polygons acquire one logical CSR window per
    /// mask row; points retain one physical coordinate row per present bit.
    fn compact_storage_from_present(
        storage: &GeometryArrayStorage,
        mask: &ValidityMask,
    ) -> crate::error::Result<GeometryArrayStorage> {
        if storage.len() != mask.present_count() {
            return Err(crate::geometry::GeometryErrorKind::message(
                "present-only payload length must match the mask present count",
            ));
        }
        match storage {
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                Ok(GeometryArrayStorage::Packed(PackedStorage::Points {
                    coords: if row_map.is_identity() {
                        Arc::clone(coords)
                    } else {
                        Arc::new(Self::gather_point_coords(coords, row_map.as_deref())?)
                    },
                    row_map: RowSelection::Identity,
                }))
            },
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => {
                let map = row_map.as_deref();
                if row_map.is_identity() {
                    let mut logical_offsets = CsrOffsetBuilder::with_capacity(mask.len())?;
                    let mut present_row = 0;
                    for (_, present) in mask.cursor() {
                        if present.is_some() {
                            present_row += 1;
                        }
                        logical_offsets
                            .push_end(offsets[present_row] as usize, i32::MAX as usize)?;
                    }
                    return Ok(GeometryArrayStorage::Packed(PackedStorage::Lines {
                        coords: Arc::clone(coords),
                        offsets: logical_offsets.finish(coords.len())?,
                        row_map: RowSelection::Identity,
                    }));
                }
                let mut builder = PointColumnBuilder::try_with_capacity(
                    coords.axes(),
                    crate::array::packed_lines_coord_len(offsets.as_slice(), map),
                )?;
                let mut logical_offsets = CsrOffsetBuilder::with_capacity(mask.len())?;
                let mut present_row = 0;
                for (_, present_index) in mask.cursor() {
                    if present_index.is_some() {
                        let physical = physical_row(map, present_row);
                        for vertex in offsets[physical] as usize..offsets[physical + 1] as usize {
                            builder.push_at(coords, vertex)?;
                        }
                        present_row += 1;
                    }
                    logical_offsets.push_end(builder.len(), i32::MAX as usize)?;
                }
                let coords = builder.finish()?;
                Ok(GeometryArrayStorage::Packed(PackedStorage::Lines {
                    offsets: logical_offsets.finish(coords.len())?,
                    coords: Arc::new(coords),
                    row_map: RowSelection::Identity,
                }))
            },
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => {
                let map = row_map.as_deref();
                if row_map.is_identity() {
                    let mut logical_polygons = CsrOffsetBuilder::with_capacity(mask.len())?;
                    let mut present_row = 0;
                    for (_, present) in mask.cursor() {
                        if present.is_some() {
                            present_row += 1;
                        }
                        logical_polygons
                            .push_end(polygon_offsets[present_row] as usize, i32::MAX as usize)?;
                    }
                    return Ok(GeometryArrayStorage::Packed(PackedStorage::Polygons {
                        coords: Arc::clone(coords),
                        ring_offsets: ring_offsets.clone(),
                        polygon_offsets: logical_polygons
                            .finish(ring_offsets.len().saturating_sub(1))?
                            .cast_level::<PolygonLevel>(),
                        row_map: RowSelection::Identity,
                    }));
                }
                let mut builder = PointColumnBuilder::try_with_capacity(
                    coords.axes(),
                    crate::array::packed_polygons_coord_len(
                        ring_offsets.as_slice(),
                        polygon_offsets.as_slice(),
                        map,
                    ),
                )?;
                let mut out_rings = CsrOffsetBuilder::with_capacity(mask.len())?;
                let mut out_polygons = CsrOffsetBuilder::with_capacity(mask.len())?;
                let mut present_row = 0;
                let mut rings = 0;
                for (_, present_index) in mask.cursor() {
                    if present_index.is_some() {
                        let physical = physical_row(map, present_row);
                        for ring in polygon_offsets[physical] as usize
                            ..polygon_offsets[physical + 1] as usize
                        {
                            for vertex in
                                ring_offsets[ring] as usize..ring_offsets[ring + 1] as usize
                            {
                                builder.push_at(coords, vertex)?;
                            }
                            rings += 1;
                            out_rings.push_end(builder.len(), i32::MAX as usize)?;
                        }
                        present_row += 1;
                    }
                    out_polygons.push_end(rings, i32::MAX as usize)?;
                }
                let coords = builder.finish()?;
                let coord_len = coords.len();
                Ok(GeometryArrayStorage::Packed(PackedStorage::Polygons {
                    coords: Arc::new(coords),
                    ring_offsets: out_rings.finish(coord_len)?.cast_level::<RingLevel>(),
                    polygon_offsets: out_polygons.finish(rings)?.cast_level::<PolygonLevel>(),
                    row_map: RowSelection::Identity,
                }))
            },
            GeometryArrayStorage::Mixed(_) => Err(crate::geometry::GeometryErrorKind::message(
                "present-only mixed storage requires row scattering",
            )),
        }
    }

    /// Whether a logical CSR payload already has the canonical nullable
    /// representation: missing rows own zero-width windows and the coordinate
    /// owner contains only present vertices. This is the publication shape
    /// produced by [`crate::array::StreamingShapes`]; accepting it directly
    /// avoids a second copy through `compact_storage_from_logical`.
    fn is_compact_logical_storage(storage: &GeometryArrayStorage, mask: &ValidityMask) -> bool {
        if mask.present_count() == mask.len() {
            return false;
        }
        match storage {
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) if row_map.is_identity() => {
                let ends = offsets.as_slice();
                ends.len() == mask.len().saturating_add(1)
                    && ends
                        .last()
                        .is_some_and(|&end| end >= 0 && end as usize == coords.len())
                    && mask
                        .iter()
                        .enumerate()
                        .all(|(row, missing)| !missing || ends[row] == ends[row + 1])
            },
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) if row_map.is_identity() => {
                let polygons = polygon_offsets.as_slice();
                let rings = ring_offsets.as_slice();
                polygons.len() == mask.len().saturating_add(1)
                    && rings
                        .last()
                        .is_some_and(|&end| end >= 0 && end as usize == coords.len())
                    && mask
                        .iter()
                        .enumerate()
                        .all(|(row, missing)| !missing || polygons[row] == polygons[row + 1])
            },
            _ => false,
        }
    }

    fn scatter_mixed_storage(
        storage: &GeometryArrayStorage,
        mask: &ValidityMask,
    ) -> crate::error::Result<GeometryArrayStorage> {
        let GeometryArrayStorage::Mixed(shapes) = storage else {
            return Err(crate::geometry::GeometryErrorKind::message(
                "mixed row scattering requires mixed storage",
            ));
        };
        if shapes.len() != mask.present_count() {
            return Err(crate::geometry::GeometryErrorKind::message(
                "present-only payload length must match the mask present count",
            ));
        }
        let mut rows = Vec::new();
        crate::boundary::try_reserve_vec(&mut rows, mask.len())?;
        let mut present = shapes.iter();
        for missing in mask.iter() {
            if missing {
                crate::try_push_vec(&mut rows, Self::missing_placeholder())?;
            } else {
                let shape = present.next().ok_or_else(|| {
                    crate::geometry::GeometryErrorKind::message(
                        "present-only payload ended before the missing mask",
                    )
                })?;
                crate::try_push_vec(&mut rows, shape.clone())?;
            }
        }
        Ok(GeometryArrayStorage::Mixed(rows))
    }

    fn adopt_rows(
        storage: Arc<GeometryArrayStorage>,
        mask: Option<ValidityMask>,
        layout: StorageLayout,
    ) -> crate::error::Result<ArrayRows> {
        let Some(mask) = mask else {
            return Ok(ArrayRows::new(storage, None));
        };
        match layout {
            StorageLayout::LogicalRows => {
                if storage.len() != mask.len() {
                    return Err(crate::geometry::GeometryErrorKind::message(
                        "logical nullable payload length must match the missing mask",
                    ));
                }
                match storage.as_ref() {
                    GeometryArrayStorage::Mixed(_) => Ok(ArrayRows::new(storage, Some(mask))),
                    GeometryArrayStorage::Packed(PackedStorage::Points { .. }) => {
                        let compact =
                            Self::compact_point_storage_from_logical(storage.as_ref(), &mask)?;
                        Ok(ArrayRows::CompactNullable {
                            storage: Arc::new(compact),
                            missing: mask,
                        })
                    },
                    GeometryArrayStorage::Packed(
                        PackedStorage::Lines { .. } | PackedStorage::Polygons { .. },
                    ) => {
                        if Self::is_compact_logical_storage(storage.as_ref(), &mask) {
                            Ok(ArrayRows::CompactNullable {
                                storage,
                                missing: mask,
                            })
                        } else {
                            let compact =
                                Self::compact_storage_from_logical(storage.as_ref(), &mask)?;
                            Ok(ArrayRows::CompactNullable {
                                storage: Arc::new(compact),
                                missing: mask,
                            })
                        }
                    },
                }
            },
            StorageLayout::PresentOnly => match storage.as_ref() {
                GeometryArrayStorage::Mixed(_) => {
                    let scattered = Self::scatter_mixed_storage(storage.as_ref(), &mask)?;
                    Ok(ArrayRows::new(Arc::new(scattered), Some(mask)))
                },
                GeometryArrayStorage::Packed(_) => {
                    if storage.len() != mask.present_count() {
                        return Err(crate::geometry::GeometryErrorKind::message(
                            "present-only payload length must match the missing mask present count",
                        ));
                    }
                    if matches!(
                        storage.as_ref(),
                        GeometryArrayStorage::Packed(PackedStorage::Points {
                            row_map,
                            ..
                        }) if row_map.is_identity()
                    ) {
                        Ok(ArrayRows::CompactNullable {
                            storage,
                            missing: mask,
                        })
                    } else {
                        let compact = Self::compact_storage_from_present(storage.as_ref(), &mask)?;
                        Ok(ArrayRows::CompactNullable {
                            storage: Arc::new(compact),
                            missing: mask,
                        })
                    }
                },
            },
        }
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
        let rows = Self::adopt_rows(storage, missing, layout)?;
        let len = rows.len();
        Ok(Self {
            rows,
            frame,
            bounds_cache: caches.bounds_cache,
            total_bounds_cache: caches.total_bounds_cache,
            row_products: prepared::RowProducts::fresh(len),
        })
    }

    /// Canonical route for a selection/view/gather. Product provenance is
    /// selected from source logical rows independently of the payload's row
    /// map or whether nullable points were rebased to row zero.
    pub(crate) fn from_selected_storage(
        source: &Self,
        storage: Arc<GeometryArrayStorage>,
        frame: Frame,
        missing: Option<ValidityMask>,
        layout: StorageLayout,
        source_rows: impl IntoIterator<Item = usize>,
    ) -> crate::error::Result<Self> {
        // Canonical nullable line/polygon storage already owns present-only
        // coordinates and one logical CSR window per source row. Preserve a
        // selection's row map so a contiguous window remains an Arrow
        // offset/length view instead of copying its present coordinates to
        // row zero merely to re-compact the same payload.
        let rows = if source.is_compact_nullable()
            && matches!(layout, StorageLayout::LogicalRows)
            && let Some(mask) = missing.as_ref()
            && storage.len() == mask.len()
            && matches!(
                storage.as_ref(),
                GeometryArrayStorage::Packed(
                    PackedStorage::Lines { .. } | PackedStorage::Polygons { .. }
                )
            ) {
            ArrayRows::CompactNullable {
                storage,
                missing: mask.clone(),
            }
        } else {
            Self::adopt_rows(storage, missing, layout)?
        };
        let products = if rows.missing().is_some_and(|mask| mask.present_count() == 0) {
            prepared::RowProducts::fresh(rows.len())
        } else {
            source.selected_row_products(source_rows)?
        };
        let caches = ComputedCaches::fresh();
        Ok(Self {
            rows,
            frame,
            bounds_cache: caches.bounds_cache,
            total_bounds_cache: caches.total_bounds_cache,
            row_products: products,
        })
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

    /// Build a packed `Polygons` array straight from a flat triangle vertex
    /// stream — `[a, c, b, a]` per triangle (one closed ring of 4 each), as
    /// `Shape::delaunay_triangle_vertices` emits. The CSR offsets are exact
    /// arithmetic ranges (ring `t` is `4t..4t+4`, polygon `t` is ring `t`), so
    /// the whole triangulation lands in ONE coords column with no per-triangle
    /// `Polygon`/`CoordSeq` materialization or re-pack.
    pub(crate) fn packed_triangles(vertices: &[Point], frame: Frame) -> PyResult<Self> {
        let len = vertices.len();
        if !len.is_multiple_of(4) {
            return Err(InvalidGeometryError::new_err(format!(
                "packed triangle vertex stream length must be a multiple of 4, got {len}"
            )));
        }
        let count = len / 4;
        let mut rings = Vec::new();
        let mut polygons = Vec::new();
        crate::try_reserve_vec(&mut rings, count + 1)?;
        crate::try_reserve_vec(&mut polygons, count + 1)?;
        rings.extend((0..=count).map(|index| index * 4));
        polygons.extend(0..=count);
        let ring_offsets = CsrOffsetColumn::<RingLevel>::try_new(rings, len)?;
        let polygon_offsets = CsrOffsetColumn::<PolygonLevel>::try_new(polygons, count)?;
        Ok(Self::from_storage(
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords: Arc::new(CoordSeq::try_from_points(vertices)?),
                ring_offsets,
                polygon_offsets,
                row_map: RowSelection::Identity,
            }),
            frame,
        )?)
    }

    /// Detached packed lane for an explicit six-coefficient affine matrix.
    /// CSR offsets are reused unchanged and the packed layout is preserved;
    /// origin-relative transforms use the exact pivot lane below. `Mixed`
    /// defers.
    pub(crate) fn packed_affine(
        &self,
        py: Python<'_>,
        matrix: &[f64; 6],
    ) -> PyResult<Option<Self>> {
        #[expect(clippy::float_cmp, reason = "exact identity may share storage")]
        if *matrix == [1.0, 0.0, 0.0, 1.0, 0.0, 0.0] {
            return Ok(Some(self.clone()));
        }
        let matrix = *matrix;
        let GeometryArrayStorage::Packed(packed) = self.storage() else {
            return Ok(None);
        };
        let axes = packed.axes();
        self.map_packed_points_detached(py, self.frame.clone(), axes, move |point| {
            let [a, b, d, e, xoff, yoff] = matrix;
            let (mapped_x, mapped_y) =
                crate::geometry::stable_affine_xy(a, b, d, e, xoff, yoff, point.x, point.y);
            point.with_xy(mapped_x, mapped_y)
        })
    }

    /// Exact affine helper for a self-relative origin. A fixed pivot and an
    /// element-relative pivot both stay explicit so no translation offset can
    /// overflow before the coordinate expression is evaluated.
    pub(crate) fn packed_self_origin_affine(
        &self,
        py: Python<'_>,
        spec: OriginSpec,
        matrix: impl FnOnce() -> [f64; 4],
    ) -> PyResult<Option<Self>> {
        let matrix = matrix();
        if !matrix.iter().all(|value| value.is_finite()) {
            let error: crate::error::Error =
                crate::geometry::GeometryErrorKind::NonFiniteCoordinate.into();
            return Err(PyErr::from(error));
        }
        if affine_linear_is_identity(matrix) {
            return Ok(Some(self.clone()));
        }
        self.packed_per_row_self_origin_affine(py, spec, || matrix)
    }

    /// Per-row self-origin affine for packed storage: each CSR window gets its
    /// pivot from column kernels and uses the exact origin-relative evaluator.
    /// Points are identity only for their own centroid/center; mixed storage
    /// returns `None`.
    pub(crate) fn packed_per_row_self_origin_affine(
        &self,
        py: Python<'_>,
        spec: OriginSpec,
        matrix: impl FnOnce() -> [f64; 4],
    ) -> PyResult<Option<Self>> {
        if matches!(spec, OriginSpec::Centroid | OriginSpec::Center)
            && let Some(identity) = self.packed_points_identity()
        {
            return Ok(Some(identity));
        }
        let matrix = matrix();
        self.map_packed_columns_detached(py, self.frame.clone(), move |columns| {
            packed_per_row_self_origin_affine_columns(columns, spec, matrix)
        })
    }

    /// Identity lane: every packed point row is its own result for this
    /// op (a point is its own centroid/hull/simplification/reverse...), so
    /// the whole array is — share the storage Arc.
    pub(crate) fn packed_points_identity(&self) -> Option<Self> {
        match self.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Points { row_map, .. })
                if row_map_is_identity(row_map.as_deref()) =>
            {
                Some(self.clone())
            },
            _ => None,
        }
    }

    /// Identity lane: packed `Lines` rows are single `LineString`s, and
    /// [`Shape::line_merge`] on a line string is the identity, so the whole
    /// array shares the storage Arc.
    pub(crate) fn packed_lines_identity(&self) -> Option<Self> {
        matches!(
            self.storage(),
            GeometryArrayStorage::Packed(PackedStorage::Lines { .. })
        )
        .then(|| self.clone())
    }

    /// Computed-point lane for ops whose binding already enforced the
    /// with Z/M (under ``auto``) flatten columnar.
    pub(crate) fn packed_points_xy(&self) -> crate::error::Result<Option<Self>> {
        let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) =
            self.storage()
        else {
            return Ok(None);
        };
        Ok(Some(if coords.zs().is_none() && coords.ms().is_none() {
            self.clone()
        } else {
            Self::packed_points_mapped(coords.force_2d()?, self.frame.clone(), row_map.clone())?
        }))
    }

    /// Build an array, packing into `Points`, `Lines`, or `Polygons` storage
    /// when every element is a uniform-axes primitive of that kind (so
    /// `array([points])`, line-returning ops, and polygon-returning ops
    /// auto-pack), otherwise `Mixed`.
    pub(crate) fn pack_or_mixed(
        items: Vec<PyGeometry>,
        frame: Frame,
    ) -> crate::error::Result<Self> {
        let mut shapes = Vec::new();
        for item in items {
            crate::boundary::try_push_vec(&mut shapes, item.shape.shape().clone())?;
        }
        Self::from_shapes(shapes, frame)
    }

    /// Build storage straight from kernel-output `Shape`s. Uniform packing can
    /// return a resource error; structural admission failures fall through to
    /// `Mixed` without publishing a partially packed result.
    pub(crate) fn storage_from_shapes(
        shapes: Vec<Shape>,
    ) -> crate::error::Result<GeometryArrayStorage> {
        // First-kind dispatch, exactly like `pack_or_mixed`.
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

#[expect(
    clippy::float_cmp,
    reason = "exact identity linear coefficients may share storage"
)]
fn affine_linear_is_identity(matrix: [f64; 4]) -> bool {
    matrix[0] == 1.0
        && crate::geometry::same_topological_coordinate(matrix[1], 0.0)
        && crate::geometry::same_topological_coordinate(matrix[2], 0.0)
        && matrix[3] == 1.0
}

#[cfg(test)]
mod compact_nullable_tests {
    use super::*;
    use crate::array::{ShapeRow, missing};
    use crate::geometry::{CoordWindow, MColumn, SharedSlice};
    use crate::heap_size::HeapSize as _;

    #[test]
    fn nullable_points_retain_only_present_coordinates_and_rank_scalar_rows() {
        let present = PyGeometryArray::packed_points(
            CoordSeq::try_from_vecs(vec![1.0, 3.0], vec![2.0, 4.0], None, None).unwrap(),
            Frame::None,
        )
        .unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: present_coords,
            ..
        }) = present.storage()
        else {
            unreachable!()
        };
        let present_coords = Arc::clone(present_coords);
        let mask = ValidityMask::from_vec(5, vec![true, false, true, false, true])
            .unwrap()
            .unwrap();
        let array = PyGeometryArray::scatter_present_rows(&present, mask).unwrap();
        assert_eq!(array.len(), 5);
        let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) =
            array.storage()
        else {
            panic!("nullable points remain homogeneous")
        };
        assert!(row_map.is_identity());
        assert_eq!(coords.len(), 2);
        assert!(Arc::ptr_eq(coords, &present_coords));
        assert_eq!(
            array.geometry_at(1).unwrap().shape.shape(),
            &Shape::Point(Point::new_raw_xy(1.0, 2.0))
        );
        assert_eq!(
            array.geometry_at(3).unwrap().shape.shape(),
            &Shape::Point(Point::new_raw_xy(3.0, 4.0))
        );
    }

    #[test]
    fn compact_nullable_storage_charges_present_coordinates_once() {
        let present = PyGeometryArray::packed_points(
            CoordSeq::try_from_vecs(vec![1.0, 3.0], vec![2.0, 4.0], None, None).unwrap(),
            Frame::None,
        )
        .unwrap();
        let compact = PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_vec(3, vec![false, true, false])
                .unwrap()
                .unwrap(),
        )
        .unwrap();

        assert_eq!(compact.nbytes(), present.nbytes());
        assert_eq!(
            compact.storage().logical_heap_bytes(),
            present.storage().logical_heap_bytes(),
            "logical nullable rows must not be charged as physical coordinates"
        );
    }

    #[test]
    fn compact_point_ranges_ignore_missing_logical_rows() {
        let present =
            PyGeometryArray::packed_points(CoordSeq::empty(CoordinateAxes::XY), Frame::None)
                .unwrap();
        let compact = PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_vec(3, vec![true, true, true])
                .unwrap()
                .unwrap(),
        )
        .unwrap();

        assert_eq!(
            compact.logical_heap_bytes_range(0..3),
            compact.logical_heap_bytes_range(0..0),
            "compact point ranges must not turn missing rows into coordinates"
        );
    }

    #[test]
    fn drop_missing_reuses_compact_points_without_prefix_rank_work() {
        let present = PyGeometryArray::packed_points(
            CoordSeq::try_from_vecs(
                (0..10_000).map(f64::from).collect(),
                (0..10_000).map(|row| f64::from(row * 2)).collect(),
                None,
                None,
            )
            .unwrap(),
            Frame::None,
        )
        .unwrap();
        let array = PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_vec(10_001, (0..10_001).map(|row| row == 9_999).collect())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let source_coords = match array.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) => {
                Arc::clone(coords)
            },
            _ => panic!("nullable point fixture must remain packed"),
        };

        missing::reset_count_valid_bytes();
        let dense = array.try_drop_missing().unwrap();

        assert_eq!(dense.len(), 10_000);
        assert!(!dense.has_missing());
        assert_eq!(missing::count_valid_bytes(), 0);
        assert!(matches!(
            dense.storage(),
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. })
                if Arc::ptr_eq(coords, &source_coords)
        ));
    }

    #[test]
    fn compact_bulk_publication_and_validation_use_the_cursor() {
        let present = PyGeometryArray::packed_points(
            CoordSeq::try_from_vecs(
                (0..10_000).map(f64::from).collect(),
                (0..10_000).map(|row| f64::from(row * 2)).collect(),
                None,
                None,
            )
            .unwrap(),
            Frame::None,
        )
        .unwrap();
        let array = PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_vec(10_001, (0..10_001).map(|row| row == 9_999).collect())
                .unwrap()
                .unwrap(),
        )
        .unwrap();

        missing::reset_count_valid_bytes();
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let _ = array.to_numpy(py).unwrap();
        });
        assert_eq!(missing::count_valid_bytes(), 0);

        missing::reset_count_valid_bytes();
        pyo3::Python::attach(|py| array.validate_impl(py)).unwrap();
        assert_eq!(missing::count_valid_bytes(), 0);
    }

    #[test]
    fn nullable_line_offsets_are_logical_without_missing_payload() {
        let present = PyGeometryArray::packed_lines(
            CoordSeq::try_from_vecs(vec![0.0, 1.0], vec![2.0, 3.0], None, None).unwrap(),
            CsrOffsetColumn::try_new(vec![0, 0, 2], 2).unwrap(),
            Frame::None,
        )
        .unwrap();
        let mask = ValidityMask::from_vec(4, vec![true, false, false, true])
            .unwrap()
            .unwrap();
        let array = PyGeometryArray::scatter_present_rows(&present, mask).unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords, offsets, ..
        }) = array.storage()
        else {
            panic!("nullable lines remain homogeneous")
        };
        assert_eq!(coords.len(), 2);
        assert_eq!(offsets.as_slice(), &[0, 0, 0, 2, 2]);
        assert!(matches!(
            array.logical_row(0),
            Some(LogicalRow::Missing { .. })
        ));
        assert!(matches!(
            array.logical_row(1),
            Some(LogicalRow::Present {
                row: ShapeRow::Line(_, 0, 0),
                ..
            })
        ));
        assert!(matches!(
            array.logical_row(2),
            Some(LogicalRow::Present {
                row: ShapeRow::Line(_, 0, 2),
                ..
            })
        ));
        assert!(matches!(
            array.logical_row(3),
            Some(LogicalRow::Missing { .. })
        ));
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

    #[test]
    fn point_validity_never_zips_against_present_payload_positions() {
        let present = PyGeometryArray::packed_points(
            CoordSeq::try_from_vecs(vec![10.0, 20.0], vec![1.0, 2.0], None, None).unwrap(),
            Frame::None,
        )
        .unwrap();
        for missing in [
            vec![true, false, false],
            vec![false, true, false],
            vec![false, false, true],
            vec![true, false, true, false],
        ] {
            let mask = ValidityMask::from_vec(missing.len(), missing.clone())
                .unwrap()
                .unwrap();
            let array = PyGeometryArray::scatter_present_rows(&present, mask).unwrap();
            let mut values = [10.0, 20.0].into_iter();
            let expected = missing
                .iter()
                .map(|&missing| (!missing).then(|| values.next().unwrap()))
                .collect::<Vec<_>>();
            assert_eq!(logical_point_xs(&array), expected);
        }
    }

    #[test]
    fn compact_points_preserve_every_coordinate_layout() {
        for (z, m, axes) in [
            (false, false, CoordinateAxes::XY),
            (true, false, CoordinateAxes::XYZ),
            (false, true, CoordinateAxes::XYM),
            (true, true, CoordinateAxes::XYZM),
        ] {
            let present = PyGeometryArray::packed_points(
                CoordSeq::try_from_vecs(
                    vec![1.0],
                    vec![2.0],
                    z.then(|| vec![3.0]),
                    m.then(|| vec![4.0]),
                )
                .unwrap(),
                Frame::None,
            )
            .unwrap();
            let array = PyGeometryArray::scatter_present_rows(
                &present,
                ValidityMask::from_vec(3, vec![true, false, true])
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(array.uniform_axes().unwrap(), Some(axes));
            assert_eq!(array.nbytes(), axes.byte_width(1));
            let all_missing = array.gather_logical_rows(&[0, 2]).unwrap();
            assert_eq!(all_missing.uniform_axes().unwrap(), Some(axes));
            assert_eq!(all_missing.nbytes(), 0);
        }
    }

    #[test]
    fn point_gathers_preserve_missing_order_duplicates_and_all_missing_type() {
        let present = PyGeometryArray::packed_points(
            CoordSeq::try_from_vecs(vec![10.0, 20.0], vec![1.0, 2.0], None, None).unwrap(),
            Frame::None,
        )
        .unwrap();
        let array = PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_vec(4, vec![true, false, true, false])
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let gathered = array.gather_logical_rows(&[3, 0, 1, 3]).unwrap();
        assert_eq!(logical_point_xs(&gathered), [
            Some(20.0),
            None,
            Some(10.0),
            Some(20.0)
        ]);
        let nested = gathered.gather_logical_rows(&[2, 0, 2]).unwrap();
        assert_eq!(logical_point_xs(&nested), [
            Some(10.0),
            Some(20.0),
            Some(10.0)
        ]);
        let missing = array.gather_logical_rows(&[0, 2, 0]).unwrap();
        assert!(missing.is_compact_nullable());
        assert!(
            matches!(missing.storage(), GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) if coords.is_empty())
        );
        assert_eq!(logical_point_xs(&missing), [None, None, None]);
        assert_eq!(
            missing.row_products.heap_bytes(),
            crate::heap_size::ARC_CONTROL_BYTES + std::mem::size_of::<prepared::RowProductOwner>()
        );
        assert!(matches!(
            missing.logical_row(1),
            Some(LogicalRow::Missing { .. })
        ));
        assert_eq!(
            missing.row_products.heap_bytes(),
            crate::heap_size::ARC_CONTROL_BYTES + std::mem::size_of::<prepared::RowProductOwner>()
        );
    }

    #[test]
    fn line_and_polygon_gathers_keep_nested_csr_windows_and_missing_rows() {
        let mask = ValidityMask::from_vec(5, vec![false, true, false, false, true])
            .unwrap()
            .unwrap();
        let lines = PyGeometryArray::packed_lines(
            CoordSeq::try_from_vecs(
                vec![0.0, 1.0, 10.0, 11.0, 20.0, 21.0],
                vec![0.0; 6],
                None,
                None,
            )
            .unwrap(),
            CsrOffsetColumn::try_new(vec![0, 2, 4, 6], 6).unwrap(),
            Frame::None,
        )
        .unwrap();
        let lines = PyGeometryArray::scatter_present_rows(&lines, mask.clone()).unwrap();
        let selected = lines.gather_logical_rows(&[3, 1, 0, 3]).unwrap();
        let line_first_x = |array: &PyGeometryArray| {
            array
                .logical_rows()
                .map(|row| match row {
                    LogicalRow::Missing { .. } => None,
                    LogicalRow::Present {
                        row: ShapeRow::Line(coords, start, end),
                        ..
                    } if start < end => Some(coords.xs()[start]),
                    LogicalRow::Present { .. } => Some(f64::NAN),
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(line_first_x(&selected), [
            Some(20.0),
            None,
            Some(0.0),
            Some(20.0)
        ]);
        let nested = selected.gather_logical_rows(&[3, 0, 3]).unwrap();
        assert_eq!(line_first_x(&nested), [Some(20.0), Some(20.0), Some(20.0)]);

        let polygons = PyGeometryArray::packed_polygons(
            CoordSeq::try_from_vecs(
                vec![
                    0.0, 1.0, 1.0, 0.0, 0.0, 10.0, 11.0, 11.0, 10.0, 10.0, 20.0, 21.0, 21.0, 20.0,
                    20.0,
                ],
                vec![
                    0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0,
                ],
                None,
                None,
            )
            .unwrap(),
            CsrOffsetColumn::<RingLevel>::try_new(vec![0, 5, 10, 15], 15).unwrap(),
            CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 1, 2, 3], 3).unwrap(),
            Frame::None,
        )
        .unwrap();
        let polygons = PyGeometryArray::scatter_present_rows(&polygons, mask).unwrap();
        let selected = polygons.gather_logical_rows(&[3, 1, 0, 3]).unwrap();
        let polygon_first_x = |array: &PyGeometryArray| {
            array
                .logical_rows()
                .map(|row| match row {
                    LogicalRow::Missing { .. } => None,
                    LogicalRow::Present {
                        row: ShapeRow::Rings(coords, offsets, start, end),
                        ..
                    } if start < end => Some(coords.xs()[offsets[start] as usize]),
                    LogicalRow::Present { .. } => Some(f64::NAN),
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(polygon_first_x(&selected), [
            Some(20.0),
            None,
            Some(0.0),
            Some(20.0)
        ]);
        let nested = selected.gather_logical_rows(&[3, 0, 3]).unwrap();
        assert_eq!(polygon_first_x(&nested), [
            Some(20.0),
            Some(20.0),
            Some(20.0)
        ]);
    }

    #[test]
    fn masking_dense_lines_and_polygons_removes_missing_csr_payload_windows() {
        let lines = PyGeometryArray::packed_lines(
            CoordSeq::try_from_vecs(
                vec![0.0, 1.0, 10.0, 11.0, 20.0, 21.0],
                vec![0.0; 6],
                None,
                None,
            )
            .unwrap(),
            CsrOffsetColumn::try_new(vec![0, 2, 4, 6], 6).unwrap(),
            Frame::None,
        )
        .unwrap();
        let source_line_cache = lines.row_products.frame_cache(0);
        let lines = lines
            .with_missing_mask(ValidityMask::from_vec(3, vec![false, true, false]).unwrap())
            .unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords, offsets, ..
        }) = lines.storage()
        else {
            panic!("masked lines changed storage kind");
        };
        assert_eq!(coords.len(), 4);
        assert_eq!(offsets.as_slice(), &[0, 2, 2, 4]);
        assert!(matches!(
            lines.logical_row(1),
            Some(LogicalRow::Missing { logical: 1 })
        ));
        assert!(Arc::ptr_eq(
            &source_line_cache,
            &lines.row_products.frame_cache(0),
        ));

        let polygons = PyGeometryArray::packed_polygons(
            CoordSeq::try_from_vecs(
                vec![
                    0.0, 1.0, 1.0, 0.0, 0.0, 10.0, 11.0, 11.0, 10.0, 10.0, 20.0, 21.0, 21.0, 20.0,
                    20.0,
                ],
                vec![
                    0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0,
                ],
                None,
                None,
            )
            .unwrap(),
            CsrOffsetColumn::<RingLevel>::try_new(vec![0, 5, 10, 15], 15).unwrap(),
            CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 1, 2, 3], 3).unwrap(),
            Frame::None,
        )
        .unwrap();
        let source_polygon_cache = polygons.row_products.frame_cache(0);
        let polygons = polygons
            .with_missing_mask(ValidityMask::from_vec(3, vec![false, true, false]).unwrap())
            .unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            ..
        }) = polygons.storage()
        else {
            panic!("masked polygons changed storage kind");
        };
        assert_eq!(coords.len(), 10);
        assert_eq!(ring_offsets.as_slice(), &[0, 5, 10]);
        assert_eq!(polygon_offsets.as_slice(), &[0, 1, 1, 2]);
        assert!(matches!(
            polygons.logical_row(1),
            Some(LogicalRow::Missing { logical: 1 })
        ));
        assert!(Arc::ptr_eq(
            &source_polygon_cache,
            &polygons.row_products.frame_cache(0),
        ));
    }

    #[test]
    fn nullable_layout_mismatch_is_rejected_at_result_admission() {
        let storage = Arc::new(GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(
                CoordSeq::try_from_vecs(vec![1.0, 2.0], vec![3.0, 4.0], None, None).unwrap(),
            ),
            row_map: RowSelection::Identity,
        }));
        let mask = ValidityMask::from_vec(3, vec![false, true, false])
            .unwrap()
            .unwrap();
        let error = PyGeometryArray::from_result_storage(
            storage,
            Frame::None,
            Some(mask),
            StorageLayout::LogicalRows,
            ComputedCaches::fresh(),
        )
        .unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(crate::geometry::GeometryErrorKind::Invalid(_))
        ));
    }

    #[test]
    fn already_masked_results_are_not_scattered_again() {
        let present = PyGeometryArray::packed_points(
            CoordSeq::try_from_vecs(vec![1.0], vec![2.0], None, None).unwrap(),
            Frame::None,
        )
        .unwrap();
        let source = PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_vec(2, vec![false, true])
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let result = source.scatter_present_result(source.clone()).unwrap();
        assert!(Arc::ptr_eq(result.storage_arc(), source.storage_arc()));
        assert!(result.row_products.shares_owner_with(&source.row_products));
    }

    #[test]
    fn newly_computed_data_never_reuses_source_products() {
        let source = PyGeometryArray::from_shapes(
            vec![Shape::Point(Point::new_raw_xy(1.0, 2.0))],
            Frame::None,
        )
        .unwrap();
        let source_cache = source.row_products.frame_cache(0);
        let computed = PyGeometryArray::from_shapes(
            vec![Shape::Point(Point::new_raw_xy(1.0, 2.0))],
            Frame::None,
        )
        .unwrap();
        let computed_cache = computed.row_products.frame_cache(0);
        assert!(!Arc::ptr_eq(&source_cache, &computed_cache));
        assert!(
            !computed
                .row_products
                .shares_owner_with(&source.row_products)
        );
    }

    #[test]
    fn sliced_xyzm_selection_keeps_the_independent_m_owner_and_window() {
        let m = SharedSlice::from_vec((50..60).map(f64::from).collect());
        let coords = CoordSeq::from_shared_columns_with_window(
            SharedSlice::from_vec((0..6).map(f64::from).collect()),
            SharedSlice::from_vec(vec![10.0; 6]),
            None,
            Some(MColumn::Local {
                owner: m.clone(),
                offset: 4,
            }),
            CoordWindow::from_range(2..5, 6),
        )
        .unwrap();
        let source = PyGeometryArray::packed_points(coords, Frame::None).unwrap();
        let selected = source.gather_logical_rows(&[2, 0]).unwrap();
        let values = selected
            .logical_rows()
            .map(|row| match row {
                LogicalRow::Present {
                    row: ShapeRow::Point(point),
                    ..
                } => point.m(),
                LogicalRow::Missing { .. } | LogicalRow::Present { .. } => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(values, [Some(56.0), Some(54.0)]);
        let GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) = selected.storage()
        else {
            panic!("selection changed point storage kind");
        };
        assert!(Arc::ptr_eq(
            &coords.column_arcs().m_parts().unwrap().0.owner().unwrap(),
            &m.owner().unwrap(),
        ));
    }

    #[test]
    fn nullable_polygon_distinguishes_missing_from_typed_empty() {
        let present = PyGeometryArray::packed_polygons(
            CoordSeq::try_from_vecs(
                vec![0.0, 1.0, 1.0, 0.0, 0.0],
                vec![0.0, 0.0, 1.0, 1.0, 0.0],
                None,
                None,
            )
            .unwrap(),
            CsrOffsetColumn::<RingLevel>::try_new(vec![0, 5], 5).unwrap(),
            CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 0, 1], 1).unwrap(),
            Frame::None,
        )
        .unwrap();
        let array = PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_vec(4, vec![true, false, true, false])
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords,
            polygon_offsets,
            ..
        }) = array.storage()
        else {
            panic!("nullable polygons remain homogeneous")
        };
        assert_eq!(coords.len(), 5);
        assert_eq!(polygon_offsets.as_slice(), &[0, 0, 0, 0, 1]);
        assert!(matches!(
            array.logical_row(0),
            Some(LogicalRow::Missing { .. })
        ));
        assert!(matches!(
            array.logical_row(1),
            Some(LogicalRow::Present {
                row: ShapeRow::Rings(_, _, 0, 0),
                ..
            })
        ));
        assert!(matches!(
            array.logical_row(2),
            Some(LogicalRow::Missing { .. })
        ));
        assert!(matches!(
            array.logical_row(3),
            Some(LogicalRow::Present {
                row: ShapeRow::Rings(_, _, 0, 1),
                ..
            })
        ));
    }

    #[test]
    fn nonidentity_nullable_compaction_reserves_selected_duplicate_payload() {
        let coords = CoordSeq::try_from_vecs(vec![0.0, 1.0], vec![0.0, 1.0], None, None).unwrap();
        let storage = Arc::new(GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords: Arc::new(coords),
            offsets: CsrOffsetColumn::try_new(vec![0, 2], 2).unwrap(),
            row_map: RowSelection::gather_checked_native(vec![0, 0, 0], 1).unwrap(),
        }));
        let mask = ValidityMask::from_vec(4, vec![false, false, false, true])
            .unwrap()
            .unwrap();
        let result = PyGeometryArray::from_result_storage(
            storage,
            Frame::None,
            Some(mask),
            StorageLayout::PresentOnly,
            ComputedCaches::fresh(),
        )
        .unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Lines { coords, .. }) = result.storage()
        else {
            panic!("duplicate line selection must stay packed");
        };
        assert_eq!(coords.len(), 6);
        assert!(coords.column_arcs().xs.capacity() >= 6);

        let coords = CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 1.0, 0.0, 0.0],
            vec![0.0, 0.0, 1.0, 1.0, 0.0],
            None,
            None,
        )
        .unwrap();
        let storage = Arc::new(GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords: Arc::new(coords),
            ring_offsets: CsrOffsetColumn::<RingLevel>::try_new(vec![0, 5], 5).unwrap(),
            polygon_offsets: CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 1], 1).unwrap(),
            row_map: RowSelection::gather_checked_native(vec![0, 0], 1).unwrap(),
        }));
        let mask = ValidityMask::from_vec(3, vec![false, false, true])
            .unwrap()
            .unwrap();
        let result = PyGeometryArray::from_result_storage(
            storage,
            Frame::None,
            Some(mask),
            StorageLayout::PresentOnly,
            ComputedCaches::fresh(),
        )
        .unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Polygons { coords, .. }) = result.storage()
        else {
            panic!("duplicate polygon selection must stay packed");
        };
        assert_eq!(coords.len(), 10);
        assert!(coords.column_arcs().xs.capacity() >= 10);
    }
}

#[cfg(test)]
mod element_bounds_cache_tests {
    use std::sync::Arc;

    use super::*;
    use crate::array::BoundsSeed;
    use crate::geometry::Bounds;
    use crate::heap_size::HeapSize as _;
    use crate::test_support::allocation_count;

    fn lines() -> PyGeometryArray {
        PyGeometryArray::packed_lines(
            CoordSeq::try_from_vecs(
                vec![0.0, 1.0, 10.0, 11.0],
                vec![0.0, 0.0, 10.0, 10.0],
                None,
                None,
            )
            .unwrap(),
            CsrOffsetColumn::try_new(vec![0, 2, 4], 4).unwrap(),
            Frame::None,
        )
        .unwrap()
    }

    fn single_line() -> PyGeometryArray {
        PyGeometryArray::packed_lines(
            CoordSeq::try_from_vecs(vec![0.0, 1.0], vec![0.0, 0.0], None, None).unwrap(),
            CsrOffsetColumn::try_new(vec![0, 2], 2).unwrap(),
            Frame::None,
        )
        .unwrap()
    }

    #[test]
    fn cold_row_seed_does_not_materialize_element_bounds() {
        let array = lines();
        let (seed, allocations) = allocation_count(|| array.row_bounds_seed(0));

        assert!(matches!(seed, BoundsSeed::Unset));
        assert_eq!(allocations, 0);
        assert!(array.existing_element_bounds().is_none());
    }

    #[test]
    fn warm_lookup_returns_the_published_element_bounds() {
        let array = lines();
        let published = array
            .try_element_bounds()
            .unwrap()
            .expect("packed lines publish element bounds");
        let (lookup, allocations) = allocation_count(|| {
            array
                .existing_element_bounds()
                .expect("published bounds remain available")
        });

        assert_eq!(allocations, 0);
        assert!(Arc::ptr_eq(&published, &lookup));
        assert_eq!(lookup.len(), 2);
        assert_eq!(lookup[0], Some(Bounds::new_raw(0.0, 0.0, 1.0, 0.0)));
        assert_eq!(lookup[1], Some(Bounds::new_raw(10.0, 10.0, 11.0, 10.0)));
    }

    #[test]
    fn retained_size_includes_published_element_bounds() {
        let array = lines();
        let cold = array.heap_bytes();
        array
            .try_element_bounds()
            .expect("element bounds")
            .expect("non-empty lines publish bounds");
        assert!(array.heap_bytes() > cold);
    }

    #[test]
    fn required_bounds_keep_compact_nullable_rows_logically_aligned() {
        let present = single_line();
        let array = PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_vec(2, vec![false, true])
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let bounds = array
            .try_element_bounds()
            .unwrap()
            .expect("compact lines publish element bounds");

        assert_eq!(bounds.len(), 2);
        assert_eq!(bounds[0], Some(Bounds::new_raw(0.0, 0.0, 1.0, 0.0)));
        assert_eq!(bounds[1], None);
    }
}

#[cfg(test)]
mod packed_simplify_keep_tests {
    use super::*;

    #[test]
    fn packed_dp_keep_all_row_and_collinear_drop() {
        let coords = CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 2.0, 0.0, 1.0, 2.0],
            vec![0.0, 1.0, 0.0, 0.0, 0.0, 0.0],
            None,
            None,
        )
        .unwrap();
        let offsets = CsrOffsetColumn::try_new(vec![0, 3, 6], 6).unwrap();
        let array = PyGeometryArray::packed_lines(coords, offsets, Frame::None).unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords, offsets, ..
        }) = array.storage()
        else {
            panic!("packed lines");
        };
        let out = array
            .simplify_dp_packed_lines(coords, offsets, 1e-9)
            .unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords: out_coords,
            offsets: out_offsets,
            ..
        }) = out.storage()
        else {
            panic!("packed lines remain packed");
        };
        assert_eq!(out_offsets.as_slice(), &[0, 3, 5]);
        assert_eq!(out_coords.xs(), &[0.0, 1.0, 2.0, 0.0, 2.0]);
        assert_eq!(out_coords.ys(), &[0.0, 1.0, 0.0, 0.0, 0.0]);
    }

    #[test]
    fn packed_vw_zero_keeps_every_vertex_including_zm_bits() {
        let nan = f64::NAN;
        let coords = CoordSeq::try_from_vecs(
            vec![-0.0, 1.0, 2.0],
            vec![0.0, -0.0, 0.0],
            Some(vec![nan, 1.0, nan]),
            Some(vec![0.0, nan, -0.0]),
        )
        .unwrap();
        let offsets = CsrOffsetColumn::try_new(vec![0, 3], 3).unwrap();
        let array = PyGeometryArray::packed_lines(coords, offsets, Frame::None).unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords, offsets, ..
        }) = array.storage()
        else {
            panic!("packed lines");
        };
        let out = array
            .simplify_vw_packed_lines(coords, offsets, 0.0)
            .unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords: out_coords, ..
        }) = out.storage()
        else {
            panic!("packed lines remain packed");
        };
        assert_eq!(out_coords.xs()[0].to_bits(), (-0.0_f64).to_bits());
        assert_eq!(out_coords.zs().unwrap()[0].to_bits(), nan.to_bits());
        assert_eq!(out_coords.ms().unwrap()[2].to_bits(), (-0.0_f64).to_bits());
        assert_eq!(out_coords.len(), 3);
    }

    #[test]
    fn packed_dp_zero_keeps_l_bend_and_drops_collinear() {
        let coords = CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 2.0, 0.0, 1.0, 2.0],
            vec![0.0, 1.0, 0.0, 0.0, 0.0, 0.0],
            None,
            None,
        )
        .unwrap();
        let offsets = CsrOffsetColumn::try_new(vec![0, 3, 6], 6).unwrap();
        let array = PyGeometryArray::packed_lines(coords, offsets, Frame::None).unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords, offsets, ..
        }) = array.storage()
        else {
            panic!("packed lines");
        };
        let out = array
            .simplify_dp_packed_lines(coords, offsets, 0.0)
            .unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords: out_coords,
            offsets: out_offsets,
            ..
        }) = out.storage()
        else {
            panic!("packed lines remain packed");
        };
        assert_eq!(out_offsets.as_slice(), &[0, 3, 5]);
        assert_eq!(out_coords.xs(), &[0.0, 1.0, 2.0, 0.0, 2.0]);
        assert_eq!(out_coords.ys(), &[0.0, 1.0, 0.0, 0.0, 0.0]);
    }
}
