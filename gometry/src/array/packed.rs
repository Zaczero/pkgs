#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::sync::Arc;

use crate::array::{
    ArrayRows, CoordSeq, CoordinateAxes, Crs, CsrOffsetBuilder, CsrOffsetColumn, ElementBounds,
    Frame, GeometryArrayStorage, InvalidGeometryError, MissingMask, OriginSpec, PackedColumns,
    Point, PointColumnBuilder, PolygonLevel, Py, PyAny, PyErr, PyGeometry, PyGeometryArray,
    PyResult, Python, RingLevel, RowSelection, RowSelectionRef, Shape, TotalBoundsCache,
    affine_about, contiguous_physical_range, fresh_prepared_cache, line_logical_len,
    packed_per_row_self_origin_affine_columns, physical_row, point_logical_len,
    polygon_logical_len, prepared, row_map_is_identity, row_ord_extremes,
    row_selection_from_logical_rows,
};
use crate::geometry::CoordWindow;

/// Build a dense bool mask from sparse missing-row indices (`None` when no
/// row is missing) — the importers' lane for `from_wkt([s, None, ...])`.
pub(crate) fn sparse_missing_mask(len: usize, missing_rows: &[usize]) -> Option<MissingMask> {
    MissingMask::from_sparse(len, missing_rows)
}

impl PyGeometryArray {
    pub(crate) fn storage(&self) -> &GeometryArrayStorage {
        self.rows.storage().as_ref()
    }

    pub(crate) const fn storage_arc(&self) -> &Arc<GeometryArrayStorage> {
        self.rows.storage()
    }

    pub(crate) const fn missing(&self) -> Option<&MissingMask> {
        self.rows.missing()
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
            .get_or_init(|| self.storage().per_element_bounds().map(Arc::from))
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
    /// missing rows are skipped (their XY placeholder must not vote); an array
    /// with no present rows is uniform `XY`.
    pub fn uniform_axes(&self) -> Option<CoordinateAxes> {
        if self
            .missing()
            .as_ref()
            .is_some_and(|mask| mask.present_count() == 0)
        {
            return Some(CoordinateAxes::XY);
        }
        if let Some(axes) = self.storage().packed_axes() {
            return Some(axes);
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

    /// Attach a missing mask, normalizing the all-present case back to the
    /// dense representation so downstream fast paths stay mask-free.
    ///
    /// Invalidates mask-dependent aggregates (`total_bounds_cache`): a shared
    /// clone's warm cache would otherwise report bounds over rows that are
    /// now masked out.
    pub(crate) fn with_missing_mask(mut self, mask: Option<MissingMask>) -> Self {
        if let Some(mask) = &mask {
            assert_eq!(
                mask.len(),
                self.storage().len(),
                "missing mask length must match array length"
            );
        }
        self.rows = self.rows.with_missing(mask);
        // Mask changes which rows contribute to array-wide aggregates.
        self.total_bounds_cache = std::sync::Arc::new(std::sync::OnceLock::new());
        self
    }

    /// The mask entries for a row subset, in gather order.
    pub(crate) fn gather_missing(&self, rows: &[usize]) -> Option<MissingMask> {
        self.missing().and_then(|mask| {
            MissingMask::from_vec(
                rows.len(),
                rows.iter().map(|&row| mask.is_missing(row)).collect(),
            )
        })
    }

    /// The mask entries for a contiguous row range.
    pub(crate) fn gather_missing_range(&self, rows: std::ops::Range<usize>) -> Option<MissingMask> {
        let len = rows.len();
        self.missing()
            .as_ref()
            .and_then(|mask| MissingMask::from_vec(len, mask.as_slice()[rows].to_vec()))
    }

    /// The dense placeholder stored at a missing slot: ``POINT (NaN NaN)``.
    /// The mask is the source of truth; the placeholder only keeps packed
    /// storage rectangular.
    pub(crate) const fn missing_placeholder() -> Shape {
        Shape::Point(crate::geometry::Point::new_unchecked_xy(f64::NAN, f64::NAN))
    }

    /// Rebuild a full-length masked array from its transformed PRESENT rows:
    /// row `i` takes the next dense row when unmasked, the placeholder when
    /// masked. The inverse of `drop_missing` for mask-blind bulk transforms.
    pub(crate) fn scatter_present_rows(present: &Self, mask: MissingMask) -> Self {
        assert_eq!(
            present.storage().len(),
            mask.present_count(),
            "present-row array length must match missing mask present count"
        );
        let frame = present.frame.clone();
        let scattered = match present.storage() {
            GeometryArrayStorage::Points { coords, row_map } => {
                Self::scatter_present_points(coords, row_map.as_deref(), mask.as_slice(), frame)
            },
            GeometryArrayStorage::Lines {
                coords,
                offsets,
                row_map,
            } => Self::scatter_present_lines(
                coords,
                offsets.as_slice(),
                row_map.as_deref(),
                mask.as_slice(),
                frame,
            ),
            GeometryArrayStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            } => Self::scatter_present_polygons(
                coords,
                ring_offsets.as_slice(),
                polygon_offsets.as_slice(),
                row_map.as_deref(),
                mask.as_slice(),
                frame,
            ),
            GeometryArrayStorage::Mixed(_) => {
                let mut rows = present.storage().iter_shapes();
                let shapes: Vec<Shape> = mask
                    .iter()
                    .map(|&missing| {
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

    fn missing_zm(axes: CoordinateAxes) -> (Option<f64>, Option<f64>) {
        (
            axes.has_z().then_some(f64::NAN),
            axes.has_m().then_some(f64::NAN),
        )
    }

    fn scatter_present_points(
        coords: &CoordSeq,
        row_map: RowSelectionRef<'_>,
        mask: &[bool],
        frame: Frame,
    ) -> Self {
        let mut builder = PointColumnBuilder::like_coords(coords, mask.len());
        let (z, m) = Self::missing_zm(coords.axes());
        let mut present_row = 0;
        for &missing in mask {
            if missing {
                builder.push_xyzm(f64::NAN, f64::NAN, z, m);
            } else {
                builder.push_at(coords, physical_row(row_map, present_row));
                present_row += 1;
            }
        }
        debug_assert_eq!(present_row, point_logical_len(coords, row_map));
        Self::packed_points(builder.finish_infallible(), frame)
    }

    fn scatter_present_lines(
        coords: &CoordSeq,
        offsets: &[i32],
        row_map: RowSelectionRef<'_>,
        mask: &[bool],
        frame: Frame,
    ) -> Self {
        let missing_vertices = mask.iter().filter(|&&missing| missing).count() * 2;
        let mut builder =
            PointColumnBuilder::like_coords(coords, coords.len().saturating_add(missing_vertices));
        let mut out_offsets = CsrOffsetBuilder::new();
        let (z, m) = Self::missing_zm(coords.axes());
        let mut present_row = 0;
        for &missing in mask {
            if missing {
                builder.push_xyzm(f64::NAN, f64::NAN, z, m);
                builder.push_xyzm(f64::NAN, f64::NAN, z, m);
            } else {
                let physical = physical_row(row_map, present_row);
                for vertex in offsets[physical] as usize..offsets[physical + 1] as usize {
                    builder.push_at(coords, vertex);
                }
                present_row += 1;
            }
            out_offsets
                .push_end(builder.len(), i32::MAX as usize)
                .expect("scattered line offsets remain monotonic and in range");
        }
        debug_assert_eq!(present_row, line_logical_len(offsets, row_map));
        let out_coords = builder.finish_infallible();
        let offsets = out_offsets
            .finish(out_coords.len())
            .expect("scattered line offsets match coordinate length");
        Self::packed_lines(out_coords, offsets, frame)
    }

    fn scatter_present_polygons(
        coords: &CoordSeq,
        ring_offsets: &[i32],
        polygon_offsets: &[i32],
        row_map: RowSelectionRef<'_>,
        mask: &[bool],
        frame: Frame,
    ) -> Self {
        let missing_count = mask.iter().filter(|&&missing| missing).count();
        let mut builder =
            PointColumnBuilder::like_coords(coords, coords.len().saturating_add(missing_count * 4));
        let mut out_ring_offsets = CsrOffsetBuilder::new();
        let mut out_polygon_offsets = CsrOffsetBuilder::new();
        let (z, m) = Self::missing_zm(coords.axes());
        let mut present_row = 0;
        let mut ring_count = 0;
        for &missing in mask {
            if missing {
                for _ in 0..4 {
                    builder.push_xyzm(f64::NAN, f64::NAN, z, m);
                }
                ring_count += 1;
                out_ring_offsets
                    .push_end(builder.len(), i32::MAX as usize)
                    .expect("scattered polygon ring offsets remain monotonic and in range");
            } else {
                let physical = physical_row(row_map, present_row);
                let rings =
                    polygon_offsets[physical] as usize..polygon_offsets[physical + 1] as usize;
                for ring in rings {
                    for vertex in ring_offsets[ring] as usize..ring_offsets[ring + 1] as usize {
                        builder.push_at(coords, vertex);
                    }
                    ring_count += 1;
                    out_ring_offsets
                        .push_end(builder.len(), i32::MAX as usize)
                        .expect("scattered polygon ring offsets remain monotonic and in range");
                }
                present_row += 1;
            }
            out_polygon_offsets
                .push_end(ring_count, i32::MAX as usize)
                .expect("scattered polygon offsets remain monotonic and in range");
        }
        debug_assert_eq!(present_row, polygon_logical_len(polygon_offsets, row_map));
        let out_coords = builder.finish_infallible();
        let ring_offsets = out_ring_offsets
            .finish(out_coords.len())
            .expect("scattered polygon ring offsets match coordinate length")
            .cast_level::<RingLevel>();
        let polygon_offsets = out_polygon_offsets
            .finish(ring_count)
            .expect("scattered polygon offsets match ring count")
            .cast_level::<PolygonLevel>();
        Self::packed_polygons(out_coords, ring_offsets, polygon_offsets, frame)
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
    /// carried only when the caller proves them unchanged; prepared,
    /// frame-dependent, and gathered caches are always fresh because their
    /// invalidation rules differ. `total_bounds_cache` follows the same share
    /// rule as `bounds_cache` (geometry-derived and frame-dependent aggregate).
    pub(crate) fn from_result_storage(
        storage: Arc<GeometryArrayStorage>,
        frame: Frame,
        missing: Option<MissingMask>,
        bounds_cache: Arc<std::sync::OnceLock<Option<ElementBounds>>>,
        total_bounds_cache: TotalBoundsCache,
    ) -> Self {
        let len = storage.len();
        Self {
            rows: ArrayRows::new(storage, missing),
            frame,
            bounds_cache,
            total_bounds_cache,
            prepared_cache: fresh_prepared_cache(),
            frame_caches: prepared::fresh_frame_caches(len),
            gathered_memo: Arc::new(std::sync::OnceLock::new()),
        }
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
        self.frame_caches = source.selected_frame_caches(rows);
        self
    }

    pub(crate) fn retag_frame(&self, frame: Frame) -> Self {
        // Mixed rows no longer carry per-row frames — shape storage is shared
        // and only the array-level frame + frame-cache sidecars refresh.
        Self::from_storage_arc(Arc::clone(self.storage_arc()), frame)
            .with_missing_mask(self.missing().cloned())
    }

    /// Wrap plain shapes as `Mixed` storage with an explicit array frame.
    pub(crate) fn mixed_shapes(shapes: Vec<Shape>, frame: Frame) -> Self {
        Self::from_storage(GeometryArrayStorage::Mixed(shapes), frame)
    }

    /// Wrap `PyGeometry` elements as `Mixed` storage with an explicit frame.
    /// Extracts shapes only (prepared state is not retained — use
    /// [`Self::from_shapes`] / [`Self::mixed_shapes`] at bulk sinks).
    pub(crate) fn mixed(items: Vec<PyGeometry>, frame: Frame) -> Self {
        Self::mixed_shapes(
            items
                .into_iter()
                .map(|item| item.shape.shape().clone())
                .collect(),
            frame,
        )
    }

    /// Wrap a homogeneous point column as packed `Points` storage — no per-row
    /// `PyGeometry` allocation. The shared frame applies to every row.
    pub(crate) fn packed_points(seq: CoordSeq, frame: Frame) -> Self {
        Self::packed_points_mapped(seq, frame, RowSelection::Identity)
    }

    pub(crate) fn packed_points_mapped(seq: CoordSeq, frame: Frame, row_map: RowSelection) -> Self {
        Self::from_storage(
            GeometryArrayStorage::Points {
                coords: Arc::new(seq),
                row_map,
            },
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
            GeometryArrayStorage::Polygons {
                coords: Arc::new(CoordSeq::try_from_points(vertices).map_err(PyErr::from)?),
                ring_offsets,
                polygon_offsets,
                row_map: RowSelection::Identity,
            },
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
        self.map_packed_coordseq_detached(py, self.frame.clone(), move |coords| {
            coords.try_affine(&matrix)
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
            GeometryArrayStorage::Points { row_map, .. }
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
        matches!(self.storage(), GeometryArrayStorage::Lines { .. }).then(|| self.clone())
    }

    /// Computed-point lane for ops whose binding already enforced the
    /// with Z/M (under ``auto``) flatten columnar.
    pub(crate) fn packed_points_xy(&self) -> Option<Self> {
        let GeometryArrayStorage::Points { coords, row_map } = self.storage() else {
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
    pub(crate) fn pack_or_mixed(items: Vec<PyGeometry>, frame: Frame) -> Self {
        Self::from_shapes(
            items
                .into_iter()
                .map(|item| item.shape.shape().clone())
                .collect(),
            frame,
        )
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
            GeometryArrayStorage::Lines {
                coords: Arc::new(coords),
                offsets,
                row_map,
            },
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
            GeometryArrayStorage::Polygons {
                coords: Arc::new(coords),
                ring_offsets,
                polygon_offsets,
                row_map,
            },
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
            GeometryArrayStorage::Points { coords, row_map },
            self.frame.clone(),
        )
    }

    pub(crate) fn try_concat_packed_points(&self, other: &Self) -> PyResult<Option<Self>> {
        let (
            GeometryArrayStorage::Points {
                coords: left,
                row_map: left_row_map,
            },
            GeometryArrayStorage::Points {
                coords: right,
                row_map: right_row_map,
            },
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
        let GeometryArrayStorage::Points { coords, row_map } = self.storage() else {
            unreachable!("slice_packed_points requires packed Points storage");
        };
        if let Some(window) = CoordSeq::contiguous_positive_slice(start, stop, step)
            && window.end <= self.storage().len()
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
                coords.view(CoordWindow::trusted(window, coords.len())),
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
