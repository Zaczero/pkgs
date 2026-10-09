use std::sync::Arc;

use crate::array::{
    ArrayRows, Bounds, CoordSeq, CoordinateAxes, Crs, CsrOffsetBuilder, CsrOffsetColumn,
    ElementBounds, Frame, InvalidGeometryError, LogicalRow, LogicalRows, OriginSpec, Point,
    PolygonLevel, Py, PyAny, PyGeometry, PyGeometryArray, PyResult, Python, RingLevel, Shape,
    StorageLayout, TotalBoundsCache, prepared,
};
use crate::geometry::storage::kernels::packed_per_row_self_origin_affine_columns;
use crate::geometry::storage::packed_columns::PackedColumns;
use crate::geometry::storage::{
    GeometryArrayStorage, PackedStorage, RowSelection, ValidityMask, row_map_is_identity,
    row_ord_extremes,
};
use crate::geometry::{CoordSeqBuilder, storage};
use crate::py::numpy::optional_float64_array;

/// Geometry-derived cache owners supplied to the computed-data constructor.
/// Prepared/frame products deliberately do not live here: every computed
/// result receives a fresh product owner.
pub(crate) struct ComputedCaches {
    bounds_cache: Arc<std::sync::OnceLock<ElementBounds>>,
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
        bounds_cache: Arc<std::sync::OnceLock<ElementBounds>>,
        total_bounds_cache: TotalBoundsCache,
    ) -> Self {
        Self {
            bounds_cache,
            total_bounds_cache,
        }
    }
}

impl PyGeometryArray {
    pub(crate) const fn row_owner(&self) -> &ArrayRows {
        &self.rows
    }

    pub(crate) fn storage(&self) -> &GeometryArrayStorage {
        self.rows.storage()
    }

    pub(crate) const fn storage_arc(&self) -> &Arc<GeometryArrayStorage> {
        self.rows.storage_arc()
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

    pub(crate) fn packed_columns(&self) -> Option<PackedColumns<'_>> {
        self.rows.packed_columns()
    }

    pub(crate) fn is_compact_nullable(&self) -> bool {
        self.rows.is_compact_nullable()
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
            let values = self
                .row_operation()
                .rows_with_missing(|| None::<f64>)
                .map(|row| match row {
                    crate::array::ArrayRow::Missing { value, .. } => Ok(value),
                    crate::array::ArrayRow::Present(row) => row
                        .row()
                        .with_shape(&from_shape)
                        .map_err(crate::PyErr::from),
                })
                .collect::<pyo3::PyResult<Vec<_>>>()?;
            return optional_float64_array(py, values);
        }
        if present
            && let Some(values) = self.reduce_packed_columns_detached(py, move |columns| {
                let column = ordinate(&columns).expect("checked ordinate presence");
                Ok(columns.collect_rows::<_, Vec<_>>(|row| {
                    row.and_then(|row| row.vertex_window())
                        .and_then(|window| row_extremes(column, window))
                        .map(|(low, high)| project(low, high))
                }))
            })?
        {
            return optional_float64_array(py, values);
        }
        optional_float64_array(
            py,
            self.storage()
                .iter_shapes()
                .enumerate()
                .map(|(row, shape)| {
                    let shape = shape.map_err(crate::PyErr::from)?;
                    Ok((!self.is_row_missing(row))
                        .then(|| from_shape(&shape))
                        .flatten())
                })
                .collect::<pyo3::PyResult<Vec<_>>>()?,
        )
    }

    /// Return the published per-element bounds without initializing the cache.
    /// Cold callers must fall back to their row-local bounds computation.
    pub(crate) fn existing_element_bounds(&self) -> Option<ElementBounds> {
        self.bounds_cache.get().cloned()
    }

    /// Build and publish per-element bounds for packed storage.
    pub(crate) fn element_bounds(&self) -> Option<ElementBounds> {
        if matches!(self.storage(), GeometryArrayStorage::Mixed(_)) {
            return None;
        }
        Some(Arc::clone(self.bounds_cache.get_or_init(|| {
            let mut values = Vec::with_capacity(self.len());
            let Ok(_) = self.try_for_each_element_bounds(|bounds| {
                values.push(bounds);
                Ok::<_, std::convert::Infallible>(())
            });
            Arc::new(values)
        })))
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
        if !matches!(
            storage,
            GeometryArrayStorage::Packed(PackedStorage::Points { .. })
        ) {
            // CSR keeps a window per logical row; Mixed has no columnar fold.
            return storage.try_for_each_element_bounds(|bounds| {
                let missing = mask.is_missing(logical);
                logical = logical.wrapping_add(1);
                visit(bounds.filter(|_| !missing))
            });
        }
        // Present-only payload: the mask places each stored row.
        let columnar = storage.try_for_each_element_bounds(|bounds| {
            while mask.is_missing(logical) {
                visit(None)?;
                logical = logical.wrapping_add(1);
            }
            logical = logical.wrapping_add(1);
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
    pub fn uniform_axes(&self) -> Option<CoordinateAxes> {
        let GeometryArrayStorage::Mixed(shapes) = self.storage() else {
            return self.storage().packed_axes();
        };
        if self.missing().is_some_and(|mask| mask.present_count() == 0) {
            return Some(CoordinateAxes::XY);
        }
        let mut axes = None;
        for (row, shape) in shapes.iter().enumerate() {
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
        let len = rows.len();
        Ok(Self {
            rows,
            frame,
            bounds_cache: caches.bounds_cache,
            total_bounds_cache: caches.total_bounds_cache,
            row_products: prepared::RowProducts::fresh(len),
        })
    }

    /// Publish a source-derived selection with products selected independently
    /// from its admitted payload rows.
    pub(super) fn from_selected_rows(
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
        storage::admit_packed_coord_count(len)?;
        let count = len / 4;
        storage::admit_packed_coord_count(count)?;
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&frame, &mut metric_scratch)?;
        if let crate::geometry::TopologyDomain::Geographic(geographic) = domain {
            let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "method");
            budget.add(len)?;
            let metric_access = || {
                metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic triangulation requires the operation's CRS metric",
                    )
                    .into()
                })
            };
            let source_context =
                crate::geometry::SourceArcContext::new(domain, Some(&metric_access));
            let published = crate::geometry::publish_triangle_vertex_stream_budgeted(
                vertices,
                geographic,
                source_context,
                &mut budget,
            )?;
            return Self::packed_triangles_with_ring_ends(
                &published.points,
                &published.ring_ends,
                frame,
            );
        }
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

    pub(crate) fn packed_triangles_with_ring_ends(
        vertices: &[Point],
        ring_ends: &[usize],
        frame: Frame,
    ) -> PyResult<Self> {
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
                )
                .into());
            }
            rings.push_end(end, vertices.len())?;
            polygons.push_end(index.wrapping_add(1), count)?;
            previous = end;
        }
        if previous != vertices.len() {
            return Err(crate::error::Error::from(
                crate::geometry::GeometryErrorKind::MalformedCsrOffsets,
            )
            .into());
        }
        Ok(Self::from_storage(
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords: Arc::new(CoordSeq::try_from_points(vertices)?),
                ring_offsets: rings.finish(vertices.len())?.cast_level::<RingLevel>(),
                polygon_offsets: polygons.finish(count)?.cast_level::<PolygonLevel>(),
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
        matrix: [f64; 4],
    ) -> PyResult<Option<Self>> {
        let linear = crate::geometry::AffineLinear::new(matrix)?;
        if linear.is_identity() {
            return Ok(Some(self.clone()));
        }
        if matches!(spec, OriginSpec::Centroid | OriginSpec::Center)
            && let Some(identity) = self.packed_points_identity()
        {
            return Ok(Some(identity));
        }
        self.map_packed_columns_detached(py, self.frame.clone(), move |columns| {
            packed_per_row_self_origin_affine_columns(columns, spec, &linear)
        })
    }

    /// Identity lane: every packed point row is its own result for this
    /// op (a point is its own centroid/hull/MRR/simplification/reverse...), so
    /// the whole array is — share the storage Arc.
    pub(crate) fn packed_points_identity(&self) -> Option<Self> {
        match self.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Points { row_map, .. })
                if row_map_is_identity(row_map.as_deref()) =>
            {
                Some(self.clone())
            },
            GeometryArrayStorage::Packed(
                PackedStorage::Points { .. }
                | PackedStorage::Lines { .. }
                | PackedStorage::Polygons { .. },
            )
            | GeometryArrayStorage::Mixed(_) => None,
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
        let shapes = items
            .into_iter()
            .map(|item| item.shape.shape().clone())
            .collect();
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

#[cfg(test)]
mod compact_nullable_tests {
    use super::*;
    use crate::geometry::storage::packed_columns::{PackedColumnOutput, map_selected_points};
    use crate::geometry::storage::{ShapeRow, validity as missing};
    use crate::geometry::{CoordWindow, GeometryKind, MColumn, SharedSlice};
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
        let mask = ValidityMask::from_vec(5, vec![true, false, true, false, true]).unwrap();
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
            ValidityMask::from_vec(3, vec![false, true, false]).unwrap(),
        )
        .unwrap();

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
            ValidityMask::from_vec(3, vec![true, true, true]).unwrap(),
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
            ValidityMask::from_vec(10_001, (0..10_001).map(|row| row == 9_999).collect()).unwrap(),
        )
        .unwrap();
        let source_coords = match array.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) => {
                Arc::clone(coords)
            },
            GeometryArrayStorage::Packed(
                PackedStorage::Lines { .. } | PackedStorage::Polygons { .. },
            )
            | GeometryArrayStorage::Mixed(_) => {
                panic!("nullable point fixture must remain packed")
            },
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
            ValidityMask::from_vec(10_001, (0..10_001).map(|row| row == 9_999).collect()).unwrap(),
        )
        .unwrap();

        missing::reset_count_valid_bytes();
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            drop(array.to_numpy(py).unwrap());
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
        let mask = ValidityMask::from_vec(4, vec![true, false, false, true]).unwrap();
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
                row: ShapeRow::Packed(row),
                ..
            }) if row.vertex_window() == Some(0..0)
        ));
        assert!(matches!(
            array.logical_row(2),
            Some(LogicalRow::Present {
                row: ShapeRow::Packed(row),
                ..
            }) if row.vertex_window() == Some(0..2)
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
    fn logical_nullable_point_publication_compacts_hidden_payload() {
        let storage = Arc::new(GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(
                CoordSeq::try_from_vecs(
                    vec![901.0, 10.0, 802.0, 20.0],
                    vec![91.0, 1.0, 82.0, 2.0],
                    None,
                    None,
                )
                .unwrap(),
            ),
            row_map: RowSelection::Identity,
        }));
        let source = PyGeometryArray::from_result_storage(
            storage,
            Frame::None,
            Some(ValidityMask::from_vec(4, vec![true, false, true, false]).unwrap()),
            StorageLayout::LogicalRows,
            ComputedCaches::fresh(),
        )
        .unwrap();
        assert_eq!(logical_point_xs(&source), [
            None,
            Some(10.0),
            None,
            Some(20.0)
        ]);

        let gathered = source.gather_logical_rows(&[3, 0, 1, 3]).unwrap();
        assert_eq!(logical_point_xs(&gathered), [
            Some(20.0),
            None,
            Some(10.0),
            Some(20.0)
        ]);

        let columns = source.packed_columns().expect("admitted point view");
        let PackedColumnOutput::Points(mapped) =
            map_selected_points(columns, CoordinateAxes::XY, |point| {
                Ok(Point::new_raw_xy(point.x + 3.0, point.y))
            })
            .unwrap()
        else {
            panic!("point mapping preserves packed points");
        };
        assert_eq!(mapped.xs(), &[13.0, 23.0]);
        let mapped = PyGeometryArray::from_result_storage(
            Arc::new(GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: Arc::new(mapped),
                row_map: RowSelection::Identity,
            })),
            Frame::None,
            source.missing().cloned(),
            StorageLayout::PresentOnly,
            ComputedCaches::fresh(),
        )
        .unwrap();
        assert_eq!(logical_point_xs(&mapped), [
            None,
            Some(13.0),
            None,
            Some(23.0)
        ]);
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
            let mask = ValidityMask::from_vec(missing.len(), missing.clone()).unwrap();
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
                ValidityMask::from_vec(3, vec![true, false, true]).unwrap(),
            )
            .unwrap();
            assert_eq!(array.uniform_axes(), Some(axes));
            let all_missing = array.gather_logical_rows(&[0, 2]).unwrap();
            assert_eq!(all_missing.uniform_axes(), Some(axes));
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
            ValidityMask::from_vec(4, vec![true, false, true, false]).unwrap(),
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
        let mask = ValidityMask::from_vec(5, vec![false, true, false, false, true]).unwrap();
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
                    LogicalRow::Present { row, .. } => Some(
                        row.with_shape(|shape| {
                            if let Shape::LineString(line) = shape {
                                line.xs().first().copied().unwrap_or(f64::NAN)
                            } else {
                                f64::NAN
                            }
                        })
                        .expect("selected packed line row materializes"),
                    ),
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
                    LogicalRow::Present { row, .. } => Some(
                        row.with_shape(|shape| {
                            if let Shape::Polygon(polygon) = shape {
                                polygon
                                    .shell
                                    .coords()
                                    .xs()
                                    .first()
                                    .copied()
                                    .unwrap_or(f64::NAN)
                            } else {
                                f64::NAN
                            }
                        })
                        .expect("selected packed polygon row materializes"),
                    ),
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
            .with_missing_mask(ValidityMask::from_vec(3, vec![false, true, false]))
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
            .with_missing_mask(ValidityMask::from_vec(3, vec![false, true, false]))
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
        let mask = ValidityMask::from_vec(3, vec![false, true, false]).unwrap();
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
            ValidityMask::from_vec(2, vec![false, true]).unwrap(),
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
            ValidityMask::from_vec(4, vec![true, false, true, false]).unwrap(),
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
                row: ShapeRow::Packed(row),
                ..
            }) if row.polygon_rings() == Some(0..0)
        ));
        assert!(matches!(
            array.logical_row(2),
            Some(LogicalRow::Missing { .. })
        ));
        assert!(matches!(
            array.logical_row(3),
            Some(LogicalRow::Present {
                row: ShapeRow::Packed(row),
                ..
            }) if row.polygon_rings() == Some(0..1)
        ));

        let empty = Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XY);
        let Some(LogicalRow::Present { row, .. }) = array.logical_row(1) else {
            panic!("the present zero-ring row must remain present");
        };
        assert_eq!(row.with_shape(Clone::clone).unwrap(), empty);
        assert_eq!(
            array.storage().row(1).with_shape(Clone::clone).unwrap(),
            empty
        );

        let mut shapes = array.storage().iter_shapes();
        assert_eq!(shapes.next().unwrap().unwrap().as_ref(), &empty);
        assert_eq!(shapes.next().unwrap().unwrap().as_ref(), &empty);

        assert_eq!(array.is_simple_unary_packed().unwrap().unwrap(), [
            false, true, false, true
        ]);
        assert_eq!(array.is_valid_unary_packed().unwrap().unwrap(), [
            false, true, false, true
        ]);

        let shape_at = |array: &PyGeometryArray, logical| match array
            .logical_row(logical)
            .expect("test row is in range")
        {
            LogicalRow::Present { row, .. } => row.with_shape(Clone::clone).unwrap(),
            LogicalRow::Missing { .. } => panic!("test row is present"),
        };
        let selected = array.gather_logical_rows(&[3, 1, 3]).unwrap();
        assert_eq!(shape_at(&selected, 0), shape_at(&array, 3));
        assert_eq!(shape_at(&selected, 1), empty);
        assert_eq!(shape_at(&selected, 2), shape_at(&array, 3));
    }

    #[test]
    fn nonidentity_nullable_compaction_reserves_selected_duplicate_payload() {
        let coords = CoordSeq::try_from_vecs(vec![0.0, 1.0], vec![0.0, 1.0], None, None).unwrap();
        let storage = Arc::new(GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords: Arc::new(coords),
            offsets: CsrOffsetColumn::try_new(vec![0, 2], 2).unwrap(),
            row_map: RowSelection::gather_checked_native(vec![0, 0, 0], 1).unwrap(),
        }));
        let mask = ValidityMask::from_vec(4, vec![false, false, false, true]).unwrap();
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
        let mask = ValidityMask::from_vec(3, vec![false, false, true]).unwrap();
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

    use super::*;
    use crate::array::BoundsSeed;
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
    fn mixed_element_bounds_decline_without_allocating() {
        let array = PyGeometryArray::from_shapes(
            vec![
                Shape::Point(Point::new_raw_xy(0.0, 0.0)),
                Shape::empty_polygon(),
            ],
            Frame::None,
        )
        .unwrap();
        let (bounds, allocations) = allocation_count(|| array.element_bounds());
        assert!(bounds.is_none());
        assert_eq!(allocations, 0);
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
            .element_bounds()
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
            .element_bounds()
            .expect("non-empty lines publish bounds");
        assert!(array.heap_bytes() > cold);
    }

    #[test]
    fn required_bounds_keep_compact_nullable_rows_logically_aligned() {
        let present = single_line();
        let array = PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_vec(2, vec![false, true]).unwrap(),
        )
        .unwrap();
        let bounds = array
            .element_bounds()
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

#[cfg(test)]
mod computed_triangle_publication_tests {
    use super::*;
    use crate::crs::frame::crs_arc_static;

    #[test]
    fn geographic_packed_triangles_publish_variable_ring_offsets() {
        let vertices = [
            Point::new_raw_xy(10.0, 20.0),
            Point::new_raw_xy(260.0, 40.0),
            Point::new_raw_xy(260.0, 60.0),
            Point::new_raw_xy(10.0, 20.0),
        ];
        let array =
            PyGeometryArray::packed_triangles(&vertices, Frame::Crs(crs_arc_static("EPSG:4326")))
                .unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            ..
        }) = array.storage()
        else {
            panic!("triangle output remains packed polygons");
        };
        assert_eq!(ring_offsets.as_slice(), &[0, 6]);
        assert_eq!(polygon_offsets.as_slice(), &[0, 1]);
        assert_eq!(coords.iter().collect::<Vec<_>>(), [
            Point::new_raw_xy(10.0, 20.0),
            Point::new_raw_xy(135.0, 30.0),
            Point::new_raw_xy(-100.0, 40.0),
            Point::new_raw_xy(-100.0, 60.0),
            Point::new_raw_xy(135.0, 40.0),
            Point::new_raw_xy(10.0, 20.0),
        ]);
    }
}
