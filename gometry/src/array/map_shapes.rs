//! The `map_shapes*` combinator family: per-row `Shape` transforms over
//! any storage, mask-aware, with detached (GIL-free) variants. Child
//! module of [`super`] (`packed_ops`).

use crate::array::packed_ops::{
    Arc, FrameCacheRows, GeometryArrayStorage, LogicalRow, MissingMask, PyGeometryArray, PyResult,
    Python, Result, Shape, note_array_row, point_logical_len, prepared, resolve_physical_row,
    rows_err,
};

/// One monomorphized row walker for every per-shape map. `T` lets callers
/// collect either replacement shapes or optional replacements without an
/// intermediate enum/allocation; `E` stays native until the Python boundary.
fn try_map_shape_rows<T, E>(
    storage: &GeometryArrayStorage,
    missing: Option<&MissingMask>,
    mut transform: impl FnMut(&Shape, usize) -> std::result::Result<T, E>,
) -> std::result::Result<Vec<T>, (usize, E)> {
    let mut output =
        Vec::with_capacity(missing.map_or_else(|| storage.len(), MissingMask::present_count));
    let mut apply = |shape: &Shape, row| {
        if missing.is_some_and(|mask| mask[row]) {
            return Ok(());
        }
        match transform(shape, row) {
            Ok(value) => {
                output.push(value);
                Ok(())
            },
            Err(error) => Err((row, error)),
        }
    };
    match storage {
        GeometryArrayStorage::Mixed(shapes) => {
            for (row, shape) in shapes.iter().enumerate() {
                apply(shape, row)?;
            }
        },
        GeometryArrayStorage::Points { coords, row_map } => {
            let map = row_map.as_deref();
            for row in 0..point_logical_len(coords, map) {
                let shape =
                    Shape::Point(coords.point_at(resolve_physical_row(map, LogicalRow(row)).0));
                apply(&shape, row)?;
            }
        },
        GeometryArrayStorage::Lines { .. } | GeometryArrayStorage::Polygons { .. } => {
            for (row, shape) in storage.iter_shapes().enumerate() {
                apply(&shape, row)?;
            }
        },
    }
    Ok(output)
}

fn rebuild_shared_mixed(
    source: &PyGeometryArray,
    shapes: &[Shape],
    changed_present: Vec<Option<Shape>>,
) -> PyGeometryArray {
    let mut changed = changed_present.into_iter();
    let mut unchanged = Vec::with_capacity(shapes.len());
    let mapped: Vec<Shape> = shapes
        .iter()
        .enumerate()
        .map(|(row, shape)| {
            if source.missing().is_some_and(|mask| mask[row]) {
                unchanged.push(true);
                return shape.clone();
            }
            match changed
                .next()
                .expect("one transform result per present row")
            {
                None => {
                    unchanged.push(true);
                    shape.clone()
                },
                Some(new_shape) => {
                    unchanged.push(false);
                    new_shape
                },
            }
        })
        .collect();
    let exhausted = changed.next().is_none();
    debug_assert!(exhausted);
    let mut result = PyGeometryArray::mixed_shapes(mapped, source.frame.clone())
        .with_missing_mask(source.missing().cloned());
    result.frame_caches = FrameCacheRows::mapped_from(&source.frame_caches, &unchanged);
    result.prepared_cache = prepared::mapped_prepared_cache(&source.prepared_cache, &unchanged);
    result
}

impl PyGeometryArray {
    /// Map every element through `transform`, preserving the array's CRS and
    /// epoch. Packed `Points` storage is walked as stack `Shape::Point`s, so a
    /// per-element transform never materializes an input `PyGeometry` wrapper
    /// (only the transformed outputs are allocated); all-`Point` outputs
    /// re-pack into `Points` storage, so point-to-point transforms keep the
    /// packed representation end to end.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the one-shot callback is deliberately existential; a named generic adds no API meaning"
    )]
    pub fn map_shapes(&self, transform: impl Fn(&Shape) -> PyResult<Shape>) -> PyResult<Self> {
        self.map_shapes_indexed(|shape, _| transform(shape))
    }

    /// [`map_shapes`](Self::map_shapes) with the row index — the hook for
    /// per-element [`F64Param`] arguments (`param.get(row)`).
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the one-shot callback is deliberately existential; a named generic adds no API meaning"
    )]
    pub fn map_shapes_indexed(
        &self,
        transform: impl Fn(&Shape, usize) -> PyResult<Shape>,
    ) -> PyResult<Self> {
        let shapes = try_map_shape_rows(self.storage(), self.missing(), transform)
            .map_err(|(row, error)| note_array_row(error, row))?;
        let result = Self::from_shapes(shapes, self.frame.clone());
        Ok(if self.has_missing() {
            self.scatter_present_result(result)
        } else {
            result
        })
    }

    /// [`map_shapes`](Self::map_shapes), but for pure-Rust kernels: clone the
    /// array storage handle, run the serial loop without the GIL, and convert
    /// the first failing row's crate error at the Python boundary.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the one-shot callback is deliberately existential; a named generic adds no API meaning"
    )]
    pub fn map_shapes_detached(
        &self,
        py: Python<'_>,
        transform: impl Fn(&Shape) -> Result<Shape> + Send + Sync,
    ) -> PyResult<Self> {
        self.map_shapes_detached_indexed(py, move |shape, _| transform(shape))
    }

    /// [`map_shapes_detached`](Self::map_shapes_detached) with the row index —
    /// the GIL-released hook for per-element [`F64Param`] arguments.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the one-shot callback is deliberately existential; a named generic adds no API meaning"
    )]
    pub fn map_shapes_detached_indexed(
        &self,
        py: Python<'_>,
        transform: impl Fn(&Shape, usize) -> Result<Shape> + Send + Sync,
    ) -> PyResult<Self> {
        let storage = Arc::clone(self.storage_arc());
        let missing = self.missing().cloned();
        let shapes = py
            .detach(move || try_map_shape_rows(&storage, missing.as_ref(), transform))
            .map_err(rows_err)?;
        let result = Self::from_shapes(shapes, self.frame.clone());
        Ok(if self.has_missing() {
            self.scatter_present_result(result)
        } else {
            result
        })
    }

    /// Budgeted detached map for constructive operations whose generated work
    /// is shared by every present row, including mixed storage and missing
    /// masks.  The caller owns the operation-wide budget; nested Shape paths
    /// receive that same mutable counter.
    pub(crate) fn map_shapes_detached_indexed_budgeted(
        &self,
        py: Python<'_>,
        operation: &'static str,
        parameter: &'static str,
        mut transform: impl FnMut(&Shape, usize, &mut crate::geometry::ExpansionBudget) -> Result<Shape>
        + Send,
    ) -> PyResult<Self> {
        let storage = Arc::clone(self.storage_arc());
        let missing = self.missing().cloned();
        let shapes = py
            .detach(move || {
                let mut budget = crate::geometry::ExpansionBudget::new(operation, parameter);
                try_map_shape_rows(&storage, missing.as_ref(), |shape, row| {
                    transform(shape, row, &mut budget)
                })
            })
            .map_err(rows_err)?;
        let result = Self::from_shapes(shapes, self.frame.clone());
        Ok(if self.has_missing() {
            self.scatter_present_result(result)
        } else {
            result
        })
    }

    /// [`map_shapes`](Self::map_shapes) for transforms that cannot fail.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the one-shot callback is deliberately existential; a named generic adds no API meaning"
    )]
    pub fn map_shapes_infallible(&self, transform: impl Fn(&Shape) -> Shape) -> Self {
        self.map_shapes(|shape| Ok(transform(shape)))
            .expect("infallible shape map")
    }

    /// [`map_shapes`](Self::map_shapes) for transforms that often return
    /// the input UNCHANGED (idempotent canonicalizers re-run on clean
    /// data): `Ok(None)` reuses the row's existing `Shape` (clone of the
    /// mixed-column value), and array-side frame/prepared caches transfer
    /// for those unchanged rows so bounds/validity/point-tester slots
    /// survive the no-op.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the one-shot callback is deliberately existential; a named generic adds no API meaning"
    )]
    pub fn map_shapes_shared(
        &self,
        transform: impl Fn(&Shape) -> PyResult<Option<Shape>>,
    ) -> PyResult<Self> {
        match self.storage() {
            GeometryArrayStorage::Mixed(shapes) => {
                // Two phases like `map_shapes` (transforms first, rebuild
                // second): interleaving measured ~250 ns/row slower.
                let changed =
                    try_map_shape_rows(self.storage(), self.missing(), |shape, _| transform(shape))
                        .map_err(|(row, error)| note_array_row(error, row))?;
                Ok(rebuild_shared_mixed(self, shapes, changed))
            },
            // Packed rows re-pack either way — the plain map already
            // never materializes wrappers there (and packed-row clones are
            // Arc bumps since the CoordSeq view conversion).
            GeometryArrayStorage::Points { .. }
            | GeometryArrayStorage::Lines { .. }
            | GeometryArrayStorage::Polygons { .. } => {
                self.map_shapes(|shape| Ok(transform(shape)?.unwrap_or_else(|| shape.clone())))
            },
        }
    }

    /// [`map_shapes_shared`](Self::map_shapes_shared), detached for pure-Rust
    /// idempotent canonicalizers. Only `Shape` values cross the detached
    /// boundary; unchanged mixed rows keep their stored `Shape` and transfer
    /// array-side frame/prepared cache slots afterward.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the one-shot callback is deliberately existential; a named generic adds no API meaning"
    )]
    pub fn map_shapes_shared_detached(
        &self,
        py: Python<'_>,
        transform: impl Fn(&Shape) -> Result<Option<Shape>> + Send + Sync,
    ) -> PyResult<Self> {
        self.map_shapes_shared_detached_indexed(py, move |shape, _| transform(shape))
    }

    /// [`map_shapes_shared_detached`](Self::map_shapes_shared_detached) with
    /// the row index — the shared-row hook for per-element [`F64Param`] args.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the one-shot callback is deliberately existential; a named generic adds no API meaning"
    )]
    pub fn map_shapes_shared_detached_indexed(
        &self,
        py: Python<'_>,
        transform: impl Fn(&Shape, usize) -> Result<Option<Shape>> + Send + Sync,
    ) -> PyResult<Self> {
        match self.storage() {
            GeometryArrayStorage::Mixed(shapes) => {
                let storage = Arc::clone(self.storage_arc());
                let missing = self.missing().cloned();
                let changed = py
                    .detach(move || try_map_shape_rows(&storage, missing.as_ref(), transform))
                    .map_err(rows_err)?;
                Ok(rebuild_shared_mixed(self, shapes, changed))
            },
            GeometryArrayStorage::Points { .. }
            | GeometryArrayStorage::Lines { .. }
            | GeometryArrayStorage::Polygons { .. } => self
                .map_shapes_detached_indexed(py, |shape, row| {
                    Ok(transform(shape, row)?.unwrap_or_else(|| shape.clone()))
                }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::boundary::Frame;
    use crate::geometry::Point;

    fn mixed_with_missing() -> PyGeometryArray {
        let shapes = (0..3)
            .map(|x| Shape::Point(Point::new_unchecked_xy(f64::from(x), 0.0)))
            .collect();
        PyGeometryArray::mixed_shapes(shapes, Frame::None)
            .with_missing_mask(MissingMask::from_sparse(3, &[1]))
    }

    #[test]
    fn shared_mixed_map_preserves_unchanged_handles_and_row_caches_with_missing() {
        let source = mixed_with_missing();
        // Warm prepared slots so the shared map can transfer them.
        let warmed0 = source.geometry_at(0);
        let warmed1 = source.geometry_at(1);
        let before_cache = source.row_frame_cache(0);
        let result = source
            .map_shapes_shared(|shape| {
                Ok((shape == &Shape::Point(Point::new_unchecked_xy(2.0, 0.0)))
                    .then(|| Shape::Point(Point::new_unchecked_xy(20.0, 0.0))))
            })
            .expect("shared map");
        assert!(Arc::ptr_eq(&warmed0.shape, &result.geometry_at(0).shape));
        assert!(Arc::ptr_eq(&warmed1.shape, &result.geometry_at(1).shape));
        assert!(!Arc::ptr_eq(
            &source.geometry_at(2).shape,
            &result.geometry_at(2).shape
        ));
        assert!(Arc::ptr_eq(&before_cache, &result.row_frame_cache(0)));
        assert!(!Arc::ptr_eq(
            &source.row_frame_cache(2),
            &result.row_frame_cache(2)
        ));
        assert!(result.is_row_missing(1));
    }

    #[test]
    fn all_unchanged_map_reuses_lazy_frame_cache_rows() {
        let source = mixed_with_missing();
        let result = source
            .map_shapes_shared(|_| Ok(None))
            .expect("no-op shared map");
        for row in 0..source.storage().len() {
            assert!(Arc::ptr_eq(
                &source.row_frame_cache(row),
                &result.row_frame_cache(row)
            ));
        }
    }

    #[test]
    fn mapper_reports_original_logical_row_after_skipping_missing() {
        let source = mixed_with_missing();
        let error = try_map_shape_rows(source.storage(), source.missing(), |_, row| {
            if row == 2 { Err("boom") } else { Ok(()) }
        })
        .expect_err("row two fails");
        assert_eq!(error, (2, "boom"));
    }
}
