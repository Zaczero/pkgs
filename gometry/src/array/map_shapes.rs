//! The `map_shapes*` combinator family: per-row `Shape` transforms over
//! any storage, mask-aware, with detached (GIL-free) variants. Child
//! module of [`super`] (`packed_ops`).

use crate::array::packed_ops::{
    PyGeometryArray, PyResult, Python, Result, Shape, note_array_row, rows_err,
};
use crate::array::{Arc, ComputedCaches, StorageLayout};
use crate::geometry::storage::{GeometryArrayStorage, PackedStorage};

/// One monomorphized row walker for every per-shape map. `T` lets callers
/// collect either replacement shapes or optional replacements without an
/// intermediate enum/allocation; `E` stays native until the Python boundary.
fn try_map_shape_rows<T, E>(
    array: &PyGeometryArray,
    mut transform: impl FnMut(&Shape, usize) -> std::result::Result<T, E>,
) -> std::result::Result<Vec<T>, (usize, E)>
where
    E: From<crate::error::Error>,
{
    let mut output = Vec::with_capacity(
        array
            .missing()
            .map_or(array.len(), |mask| mask.present_count()),
    );
    for row in array.row_operation().present_rows() {
        let logical = row.logical();
        let value = row
            .row()
            .try_with_shape(|shape| transform(shape, logical))
            .map_err(|error| (logical, error))?;
        output.push(value);
    }
    Ok(output)
}

fn rebuild_shared_mixed(
    source: &PyGeometryArray,
    shapes: &[Shape],
    changed_present: Vec<Option<Shape>>,
) -> Result<PyGeometryArray> {
    let mut changed = changed_present.into_iter();
    let mut mapped = Vec::with_capacity(shapes.len());
    for (row, shape) in shapes.iter().enumerate() {
        let mapped_shape = {
            if source.missing().is_some_and(|mask| mask.is_missing(row)) {
                shape.clone()
            } else {
                changed
                    .next()
                    .expect("one transform result per present row")
                    .unwrap_or_else(|| shape.clone())
            }
        };
        mapped.push(mapped_shape);
    }
    let exhausted = changed.next().is_none();
    debug_assert!(exhausted);
    PyGeometryArray::from_result_storage(
        Arc::new(GeometryArrayStorage::Mixed(mapped)),
        source.frame.clone(),
        source.missing().cloned(),
        StorageLayout::LogicalRows,
        ComputedCaches::fresh(),
    )
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
        let shapes = try_map_shape_rows(self, transform)
            .map_err(|(row, error)| note_array_row(error, row))?;
        let result = Self::from_shapes(shapes, self.frame.clone())?;
        Ok(if self.has_missing() {
            self.scatter_present_result(result)?
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
        self.map_shapes_detached_indexed_with_storage(py, transform, false)
    }

    /// Geographic literal publishers bind their final row as a `Shape`; keep
    /// that same row owner in `Mixed` storage rather than packing it afterward.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the one-shot callback shares the existing detached row loop"
    )]
    pub(crate) fn map_shapes_detached_mixed(
        &self,
        py: Python<'_>,
        transform: impl Fn(&Shape) -> Result<Shape> + Send + Sync,
    ) -> PyResult<Self> {
        self.map_shapes_detached_indexed_with_storage(py, move |shape, _| transform(shape), true)
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "one row loop owns both established packed output and literal-preserving Mixed output"
    )]
    fn map_shapes_detached_indexed_with_storage(
        &self,
        py: Python<'_>,
        transform: impl Fn(&Shape, usize) -> Result<Shape> + Send + Sync,
        keep_mixed: bool,
    ) -> PyResult<Self> {
        let array = self.clone();
        let shapes = py
            .detach(move || try_map_shape_rows(&array, transform))
            .map_err(rows_err)?;
        let result = if keep_mixed {
            Self::mixed_shapes(shapes, self.frame.clone())?
        } else {
            Self::from_shapes(shapes, self.frame.clone())?
        };
        Ok(if self.has_missing() {
            self.scatter_present_result(result)?
        } else {
            result
        })
    }

    /// Budgeted detached map for constructive operations whose generated work
    /// is shared by every present row, including mixed storage and missing
    /// masks. Each row gets its own budget: it bounds what a parameter makes
    /// of one geometry, and the array's length is the caller's business.
    pub(crate) fn map_shapes_detached_indexed_budgeted(
        &self,
        py: Python<'_>,
        operation: &'static str,
        parameter: &'static str,
        mut transform: impl FnMut(&Shape, usize, &mut crate::geometry::ExpansionBudget) -> Result<Shape>
        + Send,
    ) -> PyResult<Self> {
        let array = self.clone();
        let shapes = py
            .detach(move || {
                try_map_shape_rows(&array, |shape, row| {
                    let mut budget = crate::geometry::ExpansionBudget::new(operation, parameter);
                    transform(shape, row, &mut budget)
                })
            })
            .map_err(rows_err)?;
        let result = Self::from_shapes(shapes, self.frame.clone())?;
        Ok(if self.has_missing() {
            self.scatter_present_result(result)?
        } else {
            result
        })
    }

    /// [`map_shapes`](Self::map_shapes) for transforms that often return
    /// the input UNCHANGED (idempotent canonicalizers re-run on clean
    /// data): `Ok(None)` reuses the row's existing `Shape` (clone of the
    /// mixed-column value). The materialized result owns fresh array-side
    /// prepared and frame-product slots.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the one-shot callback is deliberately existential; a named generic adds no API meaning"
    )]
    #[cfg(test)]
    pub fn map_shapes_shared(
        &self,
        transform: impl Fn(&Shape) -> PyResult<Option<Shape>>,
    ) -> PyResult<Self> {
        match self.storage() {
            GeometryArrayStorage::Mixed(shapes) => {
                // Two phases like `map_shapes` (transforms first, rebuild
                // second): interleaving measured ~250 ns/row slower.
                let changed = try_map_shape_rows(self, |shape, _| transform(shape))
                    .map_err(|(row, error)| note_array_row(error, row))?;
                Ok(rebuild_shared_mixed(self, shapes, changed)?)
            },
            // Packed rows re-pack either way — the plain map already
            // never materializes wrappers there (and packed-row clones are
            // Arc bumps since the CoordSeq view conversion).
            GeometryArrayStorage::Packed(
                PackedStorage::Points { .. }
                | PackedStorage::Lines { .. }
                | PackedStorage::Polygons { .. },
            ) => self.map_shapes(|shape| Ok(transform(shape)?.unwrap_or_else(|| shape.clone()))),
        }
    }

    /// [`map_shapes_shared`](Self::map_shapes_shared), detached for pure-Rust
    /// idempotent canonicalizers. Only `Shape` values cross the detached
    /// boundary; unchanged mixed rows keep their stored `Shape`, while the
    /// materialized result owns fresh array-side product slots.
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
                let array = self.clone();
                let changed = py
                    .detach(move || try_map_shape_rows(&array, transform))
                    .map_err(rows_err)?;
                Ok(rebuild_shared_mixed(self, shapes, changed)?)
            },
            GeometryArrayStorage::Packed(
                PackedStorage::Points { .. }
                | PackedStorage::Lines { .. }
                | PackedStorage::Polygons { .. },
            ) => self.map_shapes_detached_indexed(py, |shape, row| {
                Ok(transform(shape, row)?.unwrap_or_else(|| shape.clone()))
            }),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::crs::frame::Frame;
    use crate::geometry::Point;
    use crate::geometry::storage::ValidityMask;

    fn mixed_with_missing() -> PyGeometryArray {
        let shapes = (0..3)
            .map(|x| Shape::Point(Point::new_raw_xy(f64::from(x), 0.0)))
            .collect();
        PyGeometryArray::mixed_shapes(shapes, Frame::None)
            .unwrap()
            .with_missing_mask(ValidityMask::from_sparse(3, &[1]))
            .unwrap()
    }

    #[test]
    fn shared_mixed_map_keeps_values_and_starts_products_cold_with_missing() {
        let source = mixed_with_missing();
        // Warm source products so an aliasing implementation would be visible.
        let warmed0 = source.geometry_at(0).unwrap();
        let before_cache = source.row_products.frame_cache(0);
        let result = source
            .map_shapes_shared(|shape| {
                Ok((shape == &Shape::Point(Point::new_raw_xy(2.0, 0.0)))
                    .then(|| Shape::Point(Point::new_raw_xy(20.0, 0.0))))
            })
            .expect("shared map");
        assert_eq!(result.row_products.initialized_counts(), (0, 0, 0));
        assert_eq!(
            warmed0.shape.shape(),
            result.geometry_at(0).unwrap().shape.shape()
        );
        assert_eq!(
            result.geometry_at(2).unwrap().shape.shape(),
            &Shape::Point(Point::new_raw_xy(20.0, 0.0))
        );
        assert!(!Arc::ptr_eq(
            &before_cache,
            &result.row_products.frame_cache(0)
        ));
        assert!(result.is_row_missing(1));
    }

    #[test]
    fn all_unchanged_map_starts_with_fresh_product_owner() {
        let source = mixed_with_missing();
        let source_cache = source.row_products.frame_cache(0);
        let result = source
            .map_shapes_shared(|_| Ok(None))
            .expect("no-op shared map");
        assert!(!result.row_products.shares_owner_with(&source.row_products));
        assert_eq!(result.row_products.initialized_counts(), (0, 0, 0));
        assert!(!Arc::ptr_eq(
            &source_cache,
            &result.row_products.frame_cache(0)
        ));
    }

    #[test]
    fn partial_shared_map_does_not_transfer_selected_source_products() {
        let source = mixed_with_missing()
            .gather_logical_rows(&[2, 0, 2])
            .unwrap();
        let source_unchanged = source.row_products.frame_cache(0);
        let source_changed = source.row_products.frame_cache(1);
        let result = source
            .map_shapes_shared(|shape| {
                Ok((shape == &Shape::Point(Point::new_raw_xy(0.0, 0.0)))
                    .then(|| Shape::Point(Point::new_raw_xy(10.0, 0.0))))
            })
            .expect("partial shared map");

        assert!(!result.row_products.shares_owner_with(&source.row_products));
        assert_eq!(result.row_products.initialized_counts(), (0, 0, 0));
        assert!(!Arc::ptr_eq(
            &source_unchanged,
            &result.row_products.frame_cache(0)
        ));
        assert!(!Arc::ptr_eq(
            &source_unchanged,
            &result.row_products.frame_cache(2)
        ));
        assert!(!Arc::ptr_eq(
            &source_changed,
            &result.row_products.frame_cache(1)
        ));
    }

    #[test]
    fn mapper_reports_original_logical_row_after_skipping_missing() {
        let source = mixed_with_missing();
        let error = try_map_shape_rows(&source, |_, row| {
            if row == 2 {
                Err::<(), crate::error::Error>(crate::geometry::GeometryErrorKind::message("boom"))
            } else {
                Ok(())
            }
        })
        .expect_err("row two fails");
        assert_eq!(error.0, 2);
        assert!(error.1.to_string().contains("boom"));
    }
}
