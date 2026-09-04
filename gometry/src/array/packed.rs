#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::array::PackedStorage;
use std::sync::Arc;

use crate::array::{
    ArrayRows, CoordSeq, CoordinateAxes, Crs, CsrOffsetBuilder, CsrOffsetColumn, ElementBounds,
    Frame, GeometryArrayStorage, GeometryKind, InvalidGeometryError, LogicalRow, LogicalRows,
    OriginSpec, PackedColumns, Point, PointColumnBuilder, PolygonLevel, Py, PyAny, PyErr,
    PyGeometry, PyGeometryArray, PyResult, Python, RingLevel, RowSelection, RowSelectionRef, Shape,
    TotalBoundsCache, ValidityMask, affine_about, contiguous_physical_range,
    packed_per_row_self_origin_affine_columns, physical_row, point_logical_len, prepared,
    row_map_is_identity, row_ord_extremes, row_selection_from_logical_rows,
};
use crate::geometry::{CoordSeqBuilder, CoordWindow};

/// Build packed validity from sparse missing-row indices (`None` when no row
/// is missing) — the importers' lane for `from_wkt([s, None, ...])`.
pub(crate) fn sparse_missing_mask(len: usize, missing_rows: &[usize]) -> Option<ValidityMask> {
    ValidityMask::from_sparse(len, missing_rows)
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

    /// Shared driver for the Z-extreme accessors (`min_z`/`max_z`/`z_range`):
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

    /// Shared driver for the M-extreme accessors (`min_m`/`max_m`/`m_range`):
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
            let values: Vec<Option<f64>> = self
                .row_operation()
                .rows_with_missing(|| None::<f64>)
                .map(|row| match row {
                    crate::array::ArrayRow::Missing { value, .. } => value,
                    crate::array::ArrayRow::Present(row) => row.row().with_shape(&from_shape),
                })
                .collect();
            return crate::py::numpy::optional_float64_array(py, values);
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
            return crate::py::numpy::optional_float64_array(py, values);
        }
        crate::py::numpy::optional_float64_array(
            py,
            self.storage()
                .iter_shapes()
                .enumerate()
                .map(|(row, shape)| {
                    (!self.is_row_missing(row))
                        .then(|| from_shape(&shape))
                        .flatten()
                })
                .collect::<Vec<_>>(),
        )
    }

    pub(crate) fn cached_element_bounds(&self) -> Option<ElementBounds> {
        self.bounds_cache
            .get_or_init(|| {
                if self.is_compact_nullable() {
                    Some(
                        self.row_operation()
                            .rows_with_missing(|| ())
                            .map(|row| match row {
                                crate::array::ArrayRow::Missing { .. } => None,
                                crate::array::ArrayRow::Present(row) => row.row().quick_bounds(),
                            })
                            .collect::<Vec<_>>()
                            .into(),
                    )
                } else {
                    self.storage().per_element_bounds().map(Arc::from)
                }
            })
            .clone()
    }

    /// Whether a scalar geometry shares this array's frame (CRS + epoch) —
    /// the precondition for element-level `__eq__` comparisons.
    pub(crate) fn frame_matches(&self, geom: &PyGeometry) -> bool {
        self.crs_ref() == geom.crs_ref() && self.epoch() == geom.epoch()
    }

    /// The single ordinate layout shared by every PRESENT row, or `None` when
    /// present rows disagree. Packed storage owns one layout even through row
    /// selections and missing placeholders, so its answer is O(1). Mixed
    /// missing rows are skipped (their XY placeholder must not vote). Packed
    /// all-missing arrays retain their declared layout; heterogeneous
    /// all-missing arrays are `XY`.
    pub fn uniform_axes(&self) -> Option<CoordinateAxes> {
        if let Some(axes) = self.storage().packed_axes() {
            return Some(axes);
        }
        if self
            .missing()
            .as_ref()
            .is_some_and(|mask| mask.present_count() == 0)
        {
            return Some(CoordinateAxes::XY);
        }
        let mut axes = None;
        for (row, shape) in self.storage().iter_shapes().enumerate() {
            if self.is_row_missing(row) {
                continue;
            }
            let row_axes = shape.axes();
            match axes {
                None => axes = Some(row_axes),
                Some(seen) if seen != row_axes => return None,
                Some(_) => {},
            }
        }
        Some(axes.unwrap_or(CoordinateAxes::XY))
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
    ) -> Option<CoordSeq> {
        if !self.is_compact_nullable() {
            return None;
        }
        let GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) = self.storage() else {
            return None;
        };
        let axes = coords.axes();
        let mut builder = CoordSeqBuilder::with_capacity(axes, self.len());
        for row in self.row_operation().rows_with_missing(|| ()) {
            match row {
                crate::array::ArrayRow::Missing { .. } => builder.push_xyzm(
                    f64::NAN,
                    f64::NAN,
                    axes.has_z().then_some(f64::NAN),
                    axes.has_m().then_some(f64::NAN),
                ),
                crate::array::ArrayRow::Present(row) => match row.row() {
                    crate::array::ShapeRow::Point(point) => builder.push(point),
                    _ => {
                        unreachable!("compact point storage yields point rows")
                    },
                },
            }
        }
        Some(builder.finish_with_missing_placeholders())
    }

    /// Attach a missing mask, normalizing the all-present case back to the
    /// dense representation so downstream fast paths stay mask-free.
    ///
    /// Invalidates mask-dependent aggregates (`total_bounds_cache`): a shared
    /// clone's warm cache would otherwise report bounds over rows that are
    /// now masked out.
    pub(crate) fn with_missing_mask(mut self, mask: Option<ValidityMask>) -> Self {
        if self.rows.is_compact_nullable() {
            let mask = mask.expect("compact nullable rows cannot discard validity");
            assert_eq!(
                mask.len(),
                self.len(),
                "missing mask length must match array length"
            );
            if self.missing() == Some(&mask) {
                return self;
            }
            let shapes = mask
                .cursor()
                .filter_map(|(row, present)| {
                    present.map(|_| self.geometry_at(row).shape.shape().clone())
                })
                .collect();
            let present = Self::from_shapes(shapes, self.frame);
            return Self::compact_present_rows(&present, mask);
        }
        if let Some(mask) = mask.as_ref()
            && !matches!(self.storage(), GeometryArrayStorage::Mixed(_))
        {
            if self.len() == mask.present_count() {
                return Self::compact_present_rows(&self, mask.clone());
            }
            assert_eq!(
                mask.len(),
                self.len(),
                "missing mask length must match array length"
            );
            let present_rows: Vec<_> = mask
                .cursor()
                .filter_map(|(logical, present)| present.map(|_| logical))
                .collect();
            let present = self.gather_logical_rows_dense(&present_rows);
            return Self::compact_present_rows(&present, mask.clone());
        }
        if let Some(mask) = mask.as_ref()
            && matches!(self.storage(), GeometryArrayStorage::Mixed(_))
            && self.len() == mask.present_count()
        {
            return Self::scatter_present_rows(&self, mask.clone());
        }
        if let Some(mask) = &mask {
            assert_eq!(
                mask.len(),
                self.len(),
                "missing mask length must match array length"
            );
        }
        self.rows = self.rows.with_missing(mask);
        // Mask changes which rows contribute to array-wide aggregates.
        self.total_bounds_cache = std::sync::Arc::new(std::sync::OnceLock::new());
        self
    }

    /// The mask entries for a row subset, in gather order.
    pub(crate) fn gather_missing(&self, rows: &[usize]) -> Option<ValidityMask> {
        self.missing().and_then(|mask| {
            mask.try_gather(rows)
                .expect("validity gather allocation failed")
        })
    }

    /// The mask entries for a contiguous row range.
    pub(crate) fn gather_missing_range(
        &self,
        rows: std::ops::Range<usize>,
    ) -> Option<ValidityMask> {
        self.missing()
            .and_then(|mask| mask.window(rows.start, rows.len()))
    }

    /// Coordinate-free sentinel retained only to keep heterogeneous `Mixed`
    /// storage row-aligned. Logical access never exposes it.
    pub(crate) const fn missing_placeholder() -> Shape {
        Shape::Empty(GeometryKind::GeometryCollection, CoordinateAxes::XY)
    }

    /// Rebuild a full-length masked array from its transformed PRESENT rows:
    /// row `i` takes the next dense row when unmasked, the placeholder when
    /// masked. The inverse of `drop_missing` for mask-blind bulk transforms.
    pub(crate) fn scatter_present_rows(present: &Self, mask: ValidityMask) -> Self {
        assert_eq!(
            present.len(),
            mask.present_count(),
            "present-row array length must match missing mask present count"
        );
        let frame = present.frame.clone();
        if !matches!(present.storage(), GeometryArrayStorage::Mixed(_)) {
            return Self::compact_present_rows(present, mask);
        }
        let scattered = match present.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Points { .. } |
PackedStorage::Lines { .. } | PackedStorage::Polygons { .. }) => unreachable!(),
            GeometryArrayStorage::Mixed(_) => {
                let mut rows = present.storage().iter_shapes();
                let shapes: Vec<Shape> = mask
                    .iter()
                    .map(|missing| {
                        if missing {
                            Self::missing_placeholder()
                        } else {
                            rows.next()
                                .expect("present count checked before scatter")
                                .into_owned()
                        }
                    })
                    .collect();
                Self::from_shapes(shapes, frame)
            },
        };
        scattered.with_missing_mask(Some(mask))
    }

    fn compact_present_rows(present: &Self, mask: ValidityMask) -> Self {
        let frame = present.frame.clone();
        let compact = match present.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: if row_map.is_identity() {
                    Arc::clone(coords)
                } else {
                    Arc::new(Self::gather_point_coords(coords, row_map.as_deref()))
                },
                row_map: RowSelection::Identity,
            }),
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => {
                let map = row_map.as_deref();
                if row_map.is_identity() {
                    let mut logical_offsets = CsrOffsetBuilder::new();
                    let mut present_row = 0;
                    for (_, present) in mask.cursor() {
                        if present.is_some() {
                            present_row += 1;
                        }
                        logical_offsets
                            .push_end(offsets[present_row] as usize, i32::MAX as usize)
                            .expect("compact line offsets remain in range");
                    }
                    return Self::compact_from_storage(
                        GeometryArrayStorage::Packed(PackedStorage::Lines {
                            coords: Arc::clone(coords),
                            offsets: logical_offsets
                                .finish(coords.len())
                                .expect("compact line offsets match payload"),
                            row_map: RowSelection::Identity,
                        }),
                        mask,
                        frame,
                    );
                }
                let mut builder = PointColumnBuilder::like_coords(coords, coords.len());
                let mut logical_offsets = CsrOffsetBuilder::new();
                let mut present_row = 0;
                for (_, present_index) in mask.cursor() {
                    if present_index.is_some() {
                        let physical = physical_row(map, present_row);
                        for vertex in offsets[physical] as usize..offsets[physical + 1] as usize {
                            builder.push_at(coords, vertex);
                        }
                        present_row += 1;
                    }
                    logical_offsets
                        .push_end(builder.len(), i32::MAX as usize)
                        .expect("compact line offsets remain in range");
                }
                let coords = builder.finish_infallible();
                GeometryArrayStorage::Packed(PackedStorage::Lines {
                    offsets: logical_offsets
                        .finish(coords.len())
                        .expect("compact line offsets match payload"),
                    coords: Arc::new(coords),
                    row_map: RowSelection::Identity,
                })
            },
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => {
                let map = row_map.as_deref();
                if row_map.is_identity() {
                    let mut logical_polygons = CsrOffsetBuilder::new();
                    let mut present_row = 0;
                    for (_, present) in mask.cursor() {
                        if present.is_some() {
                            present_row += 1;
                        }
                        logical_polygons
                            .push_end(polygon_offsets[present_row] as usize, i32::MAX as usize)
                            .expect("compact polygon offsets remain in range");
                    }
                    return Self::compact_from_storage(
                        GeometryArrayStorage::Packed(PackedStorage::Polygons {
                            coords: Arc::clone(coords),
                            ring_offsets: ring_offsets.clone(),
                            polygon_offsets: logical_polygons
                                .finish(ring_offsets.len().saturating_sub(1))
                                .expect("compact polygon offsets match rings")
                                .cast_level::<PolygonLevel>(),
                            row_map: RowSelection::Identity,
                        }),
                        mask,
                        frame,
                    );
                }
                let mut builder = PointColumnBuilder::like_coords(coords, coords.len());
                let mut out_rings = CsrOffsetBuilder::new();
                let mut out_polygons = CsrOffsetBuilder::new();
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
                                builder.push_at(coords, vertex);
                            }
                            rings += 1;
                            out_rings
                                .push_end(builder.len(), i32::MAX as usize)
                                .expect("compact ring offsets remain in range");
                        }
                        present_row += 1;
                    }
                    out_polygons
                        .push_end(rings, i32::MAX as usize)
                        .expect("compact polygon offsets remain in range");
                }
                let coords = builder.finish_infallible();
                let coord_len = coords.len();
                GeometryArrayStorage::Packed(PackedStorage::Polygons {
                    coords: Arc::new(coords),
                    ring_offsets: out_rings
                        .finish(coord_len)
                        .expect("compact ring offsets match payload")
                        .cast_level::<RingLevel>(),
                    polygon_offsets: out_polygons
                        .finish(rings)
                        .expect("compact polygon offsets match rings")
                        .cast_level::<PolygonLevel>(),
                    row_map: RowSelection::Identity,
                })
            },
            GeometryArrayStorage::Mixed(_) => unreachable!(),
        };
        Self::compact_from_storage(compact, mask, frame)
    }

    pub(crate) fn compact_from_storage(
        compact: GeometryArrayStorage,
        mask: ValidityMask,
        frame: Frame,
    ) -> Self {
        let len = mask.len();
        match &compact {
            GeometryArrayStorage::Packed(PackedStorage::Points { .. }) => assert_eq!(
                compact.len(),
                mask.present_count(),
                "compact point payload must contain exactly one row per present mask bit"
            ),
            GeometryArrayStorage::Packed(PackedStorage::Lines { .. } |
PackedStorage::Polygons { .. }) => {
                assert_eq!(
                    compact.len(),
                    len,
                    "compact row-aligned payload must match the missing-mask length"
                );
            },
            GeometryArrayStorage::Mixed(_) => {
                unreachable!("compact nullable storage is built from packed storage only");
            },
        }
        Self {
            rows: ArrayRows::CompactNullable {
                storage: Arc::new(compact),
                missing: mask,
            },
            frame,
            bounds_cache: Arc::new(std::sync::OnceLock::new()),
            total_bounds_cache: Arc::new(std::sync::OnceLock::new()),
            row_products: prepared::RowProducts::fresh(len),
        }
    }

    fn gather_point_coords(coords: &CoordSeq, row_map: RowSelectionRef<'_>) -> CoordSeq {
        let mut builder =
            PointColumnBuilder::like_coords(coords, point_logical_len(coords, row_map));
        for logical in 0..point_logical_len(coords, row_map) {
            builder.push_at(coords, physical_row(row_map, logical));
        }
        builder.finish_infallible()
    }

    pub(crate) fn empty_homogeneous_like(&self) -> Self {
        match self.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) => Self::packed_points(
                PointColumnBuilder::like_coords(coords, 0).finish_infallible(),
                self.frame.clone(),
            ),
            GeometryArrayStorage::Packed(PackedStorage::Lines { coords, .. }) => Self::packed_lines(
                PointColumnBuilder::like_coords(coords, 0).finish_infallible(),
                CsrOffsetColumn::try_new(vec![0], 0).expect("empty line offsets are valid"),
                self.frame.clone(),
            ),
            GeometryArrayStorage::Packed(PackedStorage::Polygons { coords, .. }) => Self::packed_polygons(
                PointColumnBuilder::like_coords(coords, 0).finish_infallible(),
                CsrOffsetColumn::<RingLevel>::try_new(vec![0], 0)
                    .expect("empty ring offsets are valid"),
                CsrOffsetColumn::<PolygonLevel>::try_new(vec![0], 0)
                    .expect("empty polygon offsets are valid"),
                self.frame.clone(),
            ),
            GeometryArrayStorage::Mixed(_) => Self::mixed_shapes(Vec::new(), self.frame.clone()),
        }
    }

    pub(crate) fn from_storage_arc(storage: Arc<GeometryArrayStorage>, frame: Frame) -> Self {
        Self::from_result_storage(
            storage,
            frame,
            None,
            Arc::new(std::sync::OnceLock::new()),
            Arc::new(std::sync::OnceLock::new()),
        )
    }

    /// Canonical lifecycle for newly-computed geometry rows. Bounds may be
    /// carried only when the caller proves them unchanged; prepared and
    /// frame-dependent caches are always fresh because their invalidation
    /// rules differ. `total_bounds_cache` follows the same share
    /// rule as `bounds_cache` (geometry-derived and frame-dependent aggregate).
    pub(crate) fn from_result_storage(
        storage: Arc<GeometryArrayStorage>,
        frame: Frame,
        missing: Option<ValidityMask>,
        bounds_cache: Arc<std::sync::OnceLock<Option<ElementBounds>>>,
        total_bounds_cache: TotalBoundsCache,
    ) -> Self {
        let len = storage.len();
        Self {
            rows: ArrayRows::new(storage, None),
            frame,
            bounds_cache,
            total_bounds_cache,
            row_products: prepared::RowProducts::fresh(len),
        }
        .with_missing_mask(missing)
    }

    pub(crate) fn from_storage(storage: GeometryArrayStorage, frame: Frame) -> Self {
        Self::from_storage_arc(Arc::new(storage), frame)
    }

    /// Canonical selection lifecycle: geometry-derived caches remain fresh,
    /// while frame caches follow their selected source rows.
    pub(crate) fn with_selected_caches_from(
        mut self,
        source: &Self,
        rows: impl IntoIterator<Item = usize>,
    ) -> Self {
        if source.is_compact_nullable() {
            self.row_products = source.selected_row_products(rows);
            return self;
        }
        self.row_products = match self.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Points { row_map, .. } |
PackedStorage::Lines { row_map, .. } | PackedStorage::Polygons { row_map, ..
}) => {
                source.row_products.with_selection(row_map.clone())
            },
            GeometryArrayStorage::Mixed(_) => source.selected_row_products(rows),
        };
        self
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
    pub(crate) fn mixed_shapes(shapes: Vec<Shape>, frame: Frame) -> Self {
        Self::from_storage(GeometryArrayStorage::Mixed(shapes), frame)
    }

    /// Wrap a homogeneous point column as packed `Points` storage — no per-row
    /// `PyGeometry` allocation. The shared frame applies to every row.
    pub(crate) fn packed_points(seq: CoordSeq, frame: Frame) -> Self {
        Self::packed_points_mapped(seq, frame, RowSelection::Identity)
    }

    pub(crate) fn packed_points_mapped(seq: CoordSeq, frame: Frame, row_map: RowSelection) -> Self {
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
        let ring_offsets = CsrOffsetColumn::<RingLevel>::try_new(
            (0..=count).map(|index| index * 4).collect(),
            len,
        )?;
        let polygon_offsets =
            CsrOffsetColumn::<PolygonLevel>::try_new((0..=count).collect(), count)?;
        Ok(Self::from_storage(
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords: Arc::new(CoordSeq::try_from_points(vertices).map_err(PyErr::from)?),
                ring_offsets,
                polygon_offsets,
                row_map: RowSelection::Identity,
            }),
            frame,
        ))
    }

    /// Columnar affine lane for ANY packed storage (points, lines, polygons):
    /// one uniform matrix maps the shared coordinate columns directly and the
    /// CSR offsets (vertex indices, structure-only) are reused unchanged — no
    /// per-row `Shape` materialization or re-pack, and the packed layout is
    /// preserved for downstream zero-copy/Arrow export. Bit-identical to the
    /// per-shape path, which is itself just `try_map_xy` (no re-orientation),
    /// so a reflecting matrix flips winding the same way on both. `Mixed`
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
        let axes = self
            .uniform_axes()
            .expect("packed storage has uniform axes");
        self.map_packed_points_detached(py, self.frame.clone(), axes, move |point| {
            let [a, b, d, e, xoff, yoff] = matrix;
            let (mapped_x, mapped_y) =
                crate::geometry::stable_affine_xy(a, b, d, e, xoff, yoff, point.x, point.y);
            point.with_xy(mapped_x, mapped_y)
        })
    }

    /// Affine helper for a self-relative origin. A **fixed** `(x, y)` pivot
    /// folds into ONE matrix, so the whole array takes the columnar
    /// [`packed_affine`](Self::packed_affine) lane (Points/Lines/Polygons
    /// alike — the same path `affine_transform` uses). A
    /// `'centroid'`/`'center'` origin is each ELEMENT's own center: a point
    /// is its own center (identity, the array is its own result); lines and
    /// polygons take a per-row pivot column loop (see
    /// [`packed_per_row_self_origin_affine`](Self::packed_per_row_self_origin_affine)).
    /// `matrix` builds the origin-at-zero form; the pivot is re-seated here.
    pub(crate) fn packed_self_origin_affine(
        &self,
        py: Python<'_>,
        spec: OriginSpec,
        matrix: impl FnOnce() -> [f64; 6],
    ) -> PyResult<Option<Self>> {
        let matrix = matrix();
        if affine_matrix_is_identity(matrix) {
            return Ok(Some(self.clone()));
        }
        match spec {
            OriginSpec::Centroid | OriginSpec::Center => {
                self.packed_per_row_self_origin_affine(py, spec, || matrix)
            },
            OriginSpec::Fixed(pivot_x, pivot_y) => {
                let [a, b, d, e, _, _] = matrix;
                self.packed_affine(py, &affine_about(a, b, d, e, (pivot_x, pivot_y)))
            },
        }
    }

    /// Per-row self-origin affine for packed lines/polygons: each CSR window
    /// gets its own pivot from column kernels, then [`CoordSeq::try_affine`]
    /// on that window. Points are identity; mixed storage returns `None`.
    pub(crate) fn packed_per_row_self_origin_affine(
        &self,
        py: Python<'_>,
        spec: OriginSpec,
        matrix: impl FnOnce() -> [f64; 6],
    ) -> PyResult<Option<Self>> {
        if let Some(identity) = self.packed_points_identity() {
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
        matches!(self.storage(), GeometryArrayStorage::Packed(PackedStorage::Lines { .. })).then(|| self.clone())
    }

    /// Computed-point lane for ops whose binding already enforced the
    /// with Z/M (under ``auto``) flatten columnar.
    pub(crate) fn packed_points_xy(&self) -> Option<Self> {
        let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) = self.storage() else {
            return None;
        };
        Some(if coords.zs().is_none() && coords.ms().is_none() {
            self.clone()
        } else {
            Self::packed_points_mapped(coords.force_2d(), self.frame.clone(), row_map.clone())
        })
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
        Ok(Self::from_shapes(shapes, frame))
    }

    /// Build an array straight from kernel-output `Shape`s: all-`Point`
    /// uniform-axes results pack into one shared `CoordSeq` without ever
    /// materializing per-row wrappers; anything else lands in array-owned
    /// `Mixed` shape storage (no per-row `PyGeometry` / `ShapeData`).
    pub(crate) fn from_shapes(shapes: Vec<Shape>, frame: Frame) -> Self {
        // First-kind dispatch, exactly like `pack_or_mixed`.
        match shapes.first() {
            Some(Shape::Point(_)) => {
                if let Some(column) = Self::uniform_point_column(shapes.iter()) {
                    return Self::packed_points(column, frame);
                }
            },
            Some(Shape::LineString(_)) => {
                if let Some((coords, offsets)) = Self::uniform_line_column(shapes.iter()) {
                    return Self::packed_lines(coords, offsets, frame);
                }
            },
            Some(Shape::Polygon(_)) => {
                if let Some((coords, ring_offsets, polygon_offsets)) =
                    Self::uniform_polygon_column(shapes.iter())
                {
                    return Self::packed_polygons(coords, ring_offsets, polygon_offsets, frame);
                }
            },
            _ => {},
        }
        Self::mixed_shapes(shapes, frame)
    }

    /// Wrap homogeneous `LineString` rows as packed `Lines` storage: one
    /// shared column set plus CSR vertex offsets.
    pub(crate) fn packed_lines(coords: CoordSeq, offsets: CsrOffsetColumn, frame: Frame) -> Self {
        Self::packed_lines_mapped(coords, offsets, frame, RowSelection::Identity)
    }

    pub(crate) fn packed_lines_mapped(
        coords: CoordSeq,
        offsets: CsrOffsetColumn,
        frame: Frame,
        row_map: RowSelection,
    ) -> Self {
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
    ) -> Self {
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
    ) -> Self {
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

    /// Zero-copy logical row reorder/subset via `row_map` composition.
    pub(crate) fn remap_packed_points(
        &self,
        coords: Arc<CoordSeq>,
        row_map: &RowSelection,
        rows: impl Iterator<Item = usize>,
    ) -> Self {
        let row_map = row_selection_from_logical_rows(row_map.as_deref(), coords.len(), rows);
        Self::from_storage(
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }),
            self.frame.clone(),
        )
    }

    pub(crate) fn try_concat_packed_points(&self, other: &Self) -> PyResult<Option<Self>> {
        if self.is_compact_nullable() || other.is_compact_nullable() {
            return Ok(None);
        }
        let (
            GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: left,
                row_map: left_row_map,
            }),
            GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: right,
                row_map: right_row_map,
            }),
        ) = (self.storage(), other.storage())
        else {
            return Ok(None);
        };
        if left.axes() != right.axes() {
            return Ok(None);
        }
        if left_row_map.reorders() {
            return Ok(Some(self.materialize_packed_points().concat_pair(other)?));
        }
        if right_row_map.reorders() {
            return Ok(Some(self.concat_pair(&other.materialize_packed_points())?));
        }
        let Some(joined) = left.concat(right) else {
            return Ok(None);
        };
        Ok(Some(Self::packed_points(joined, self.frame.clone())))
    }

    /// Packed-point slice: zero-copy `view`/`row_map` for step-`1` contiguous
    /// windows; non-contiguous strides materialize a fresh column.
    pub(crate) fn slice_packed_points(&self, start: isize, stop: isize, step: isize) -> Self {
        if self.missing().is_some() {
            // Masked lane: materialize the row list once so the mask gathers
            // alongside the coordinates; dense arrays keep the window path.
            let mut rows = Vec::new();
            let mut i = start;
            while (step > 0 && i < stop) || (step < 0 && i > stop) {
                rows.push(i as usize);
                i += step;
            }
            return self.gather_logical_rows(&rows);
        }
        let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) = self.storage() else {
            unreachable!("slice_packed_points requires packed Points storage");
        };
        if let Some(window) = CoordSeq::contiguous_positive_slice(start, stop, step)
            && window.end <= self.len()
        {
            return self.select_packed_points(Arc::clone(coords), row_map, window);
        }
        let mut rows = Vec::new();
        let mut i = start;
        while (step > 0 && i < stop) || (step < 0 && i > stop) {
            rows.push(i as usize);
            i += step;
        }
        let map = row_map.as_deref();
        let mut builder = PointColumnBuilder::like_coords(coords, rows.len());
        for &logical in &rows {
            builder.push_at(coords, physical_row(map, logical));
        }
        Self::packed_points(builder.finish_infallible(), self.frame.clone())
    }

    /// Contiguous `take`/`filter` fast path: `view` when physical rows form a
    /// window; otherwise compose `row_map` for zero-copy scatter.
    pub(crate) fn select_packed_points(
        &self,
        coords: Arc<CoordSeq>,
        row_map: &RowSelection,
        rows: impl IntoIterator<Item = usize> + Clone,
    ) -> Self {
        if let Some(window) = contiguous_physical_range(row_map.as_deref(), rows.clone()) {
            return Self::packed_points_mapped(
                coords.view(CoordWindow::from_range(window, coords.len())),
                self.frame.clone(),
                RowSelection::Identity,
            );
        }
        self.remap_packed_points(coords, row_map, rows.into_iter())
    }
}

#[expect(
    clippy::float_cmp,
    reason = "affine identity rows are exact coefficients"
)]
fn affine_matrix_is_identity(matrix: [f64; 6]) -> bool {
    matrix[0] == 1.0
        && crate::geometry::same_topological_coordinate(matrix[1], 0.0)
        && crate::geometry::same_topological_coordinate(matrix[2], 0.0)
        && matrix[3] == 1.0
        && crate::geometry::same_topological_coordinate(matrix[4], 0.0)
        && crate::geometry::same_topological_coordinate(matrix[5], 0.0)
}

#[cfg(test)]
mod compact_nullable_tests {
    use super::*;
    use crate::array::ShapeRow;
    use crate::heap_size::HeapSize as _;

    #[test]
    fn nullable_points_retain_only_present_coordinates_and_rank_scalar_rows() {
        let present = PyGeometryArray::packed_points(
            CoordSeq::from_vecs(vec![1.0, 3.0], vec![2.0, 4.0], None, None),
            Frame::None,
        );
        let GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: present_coords,
            ..
        }) = present.storage()
        else {
            unreachable!()
        };
        let present_coords = Arc::clone(present_coords);
        let mask = ValidityMask::from_vec(5, vec![true, false, true, false, true]).unwrap();
        let array = PyGeometryArray::scatter_present_rows(&present, mask);
        assert_eq!(array.len(), 5);
        let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) = array.storage() else {
            panic!("nullable points remain homogeneous")
        };
        assert!(row_map.is_identity());
        assert_eq!(coords.len(), 2);
        assert!(Arc::ptr_eq(coords, &present_coords));
        assert_eq!(
            array.geometry_at(1).shape.shape(),
            &Shape::Point(Point::new_raw_xy(1.0, 2.0))
        );
        assert_eq!(
            array.geometry_at(3).shape.shape(),
            &Shape::Point(Point::new_raw_xy(3.0, 4.0))
        );
    }

    #[test]
    fn nullable_line_offsets_are_logical_without_missing_payload() {
        let present = PyGeometryArray::packed_lines(
            CoordSeq::from_vecs(vec![0.0, 1.0], vec![2.0, 3.0], None, None),
            CsrOffsetColumn::try_new(vec![0, 0, 2], 2).unwrap(),
            Frame::None,
        );
        let mask = ValidityMask::from_vec(4, vec![true, false, false, true]).unwrap();
        let array = PyGeometryArray::scatter_present_rows(&present, mask);
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
            CoordSeq::from_vecs(vec![10.0, 20.0], vec![1.0, 2.0], None, None),
            Frame::None,
        );
        for missing in [
            vec![true, false, false],
            vec![false, true, false],
            vec![false, false, true],
            vec![true, false, true, false],
        ] {
            let mask = ValidityMask::from_vec(missing.len(), missing.clone()).unwrap();
            let array = PyGeometryArray::scatter_present_rows(&present, mask);
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
                CoordSeq::from_vecs(
                    vec![1.0],
                    vec![2.0],
                    z.then(|| vec![3.0]),
                    m.then(|| vec![4.0]),
                ),
                Frame::None,
            );
            let array = PyGeometryArray::scatter_present_rows(
                &present,
                ValidityMask::from_vec(3, vec![true, false, true]).unwrap(),
            );
            assert_eq!(array.uniform_axes(), Some(axes));
            assert_eq!(array.nbytes(), axes.byte_width(1));
            let all_missing = array.gather_logical_rows(&[0, 2]);
            assert_eq!(all_missing.uniform_axes(), Some(axes));
            assert_eq!(all_missing.nbytes(), 0);
        }
    }

    #[test]
    fn point_gathers_preserve_missing_order_duplicates_and_all_missing_type() {
        let present = PyGeometryArray::packed_points(
            CoordSeq::from_vecs(vec![10.0, 20.0], vec![1.0, 2.0], None, None),
            Frame::None,
        );
        let array = PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_vec(4, vec![true, false, true, false]).unwrap(),
        );
        let gathered = array.gather_logical_rows(&[3, 0, 1, 3]);
        assert_eq!(logical_point_xs(&gathered), [
            Some(20.0),
            None,
            Some(10.0),
            Some(20.0)
        ]);
        let nested = gathered.gather_logical_rows(&[2, 0, 2]);
        assert_eq!(logical_point_xs(&nested), [
            Some(10.0),
            Some(20.0),
            Some(10.0)
        ]);
        let missing = array.gather_logical_rows(&[0, 2, 0]);
        assert!(missing.is_compact_nullable());
        assert!(
            matches!(missing.storage(), GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) if coords.is_empty())
        );
        assert_eq!(logical_point_xs(&missing), [None, None, None]);
        assert_eq!(missing.row_products.heap_bytes(), 0);
        assert!(matches!(
            missing.logical_row(1),
            Some(LogicalRow::Missing { .. })
        ));
        assert_eq!(missing.row_products.heap_bytes(), 0);
    }

    #[test]
    fn nullable_polygon_distinguishes_missing_from_typed_empty() {
        let present = PyGeometryArray::packed_polygons(
            CoordSeq::from_vecs(
                vec![0.0, 1.0, 1.0, 0.0, 0.0],
                vec![0.0, 0.0, 1.0, 1.0, 0.0],
                None,
                None,
            ),
            CsrOffsetColumn::<RingLevel>::try_new(vec![0, 5], 5).unwrap(),
            CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 0, 1], 1).unwrap(),
            Frame::None,
        );
        let array = PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_vec(4, vec![true, false, true, false]).unwrap(),
        );
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
}
