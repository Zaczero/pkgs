//! Python admission, detach and publication for native packed-column kernels.

use std::sync::Arc;

use pyo3::marker::Ungil;
use pyo3::prelude::{PyErr, PyResult, Python};

use crate::array::{ComputedCaches, Frame, PyGeometryArray, StorageLayout};
use crate::error::{BatchError, BatchResult};
use crate::geometry::storage::packed_columns::{
    PackedColumnOutput, PackedColumns, PointColumns, assemble_packed_output, map_selected_points,
    map_selected_to_crs, replace_packed_columns_dispatch,
};
use crate::geometry::storage::replacement::CoordinateReplacement;
use crate::geometry::storage::{GeometryArrayStorage, PackedStorage};

impl PyGeometryArray {
    fn assemble_packed_result(
        &self,
        output: PackedColumnOutput,
        output_frame: Frame,
    ) -> crate::error::Result<Self> {
        let layout = match (&output, self.storage()) {
            (
                PackedColumnOutput::Points(_),
                GeometryArrayStorage::Packed(PackedStorage::Points { .. }),
            ) if self.is_compact_nullable() => StorageLayout::PresentOnly,
            _ => StorageLayout::LogicalRows,
        };
        Self::from_result_storage(
            Arc::new(assemble_packed_output(output)),
            output_frame,
            self.missing().cloned(),
            layout,
            ComputedCaches::fresh(),
        )
    }

    pub(crate) fn reduce_packed_columns_detached<R>(
        &self,
        py: Python<'_>,
        kernel: impl for<'a> FnOnce(PackedColumns<'a>) -> BatchResult<R> + Ungil,
    ) -> PyResult<Option<R>>
    where
        R: Ungil,
    {
        let Some(columns) = self.rows.packed_columns() else {
            return Ok(None);
        };
        let result = py.detach(move || kernel(columns)).map_err(PyErr::from)?;
        Ok(Some(result))
    }

    pub(crate) fn map_packed_columns_detached(
        &self,
        py: Python<'_>,
        output_frame: Frame,
        kernel: impl for<'a> FnOnce(PackedColumns<'a>) -> BatchResult<PackedColumnOutput> + Ungil,
    ) -> PyResult<Option<Self>> {
        let output = self
            .reduce_packed_columns_detached(py, kernel)?
            .map(|output| self.assemble_packed_result(output, output_frame))
            .transpose()?;
        Ok(output)
    }

    /// Like [`Self::map_packed_columns_detached`], but lets a packed kernel
    /// decline a row layout it cannot represent without materializing Shapes.
    /// `None` means the caller should use its general path.
    pub(crate) fn try_map_packed_columns_detached(
        &self,
        py: Python<'_>,
        output_frame: Frame,
        kernel: impl for<'a> FnOnce(PackedColumns<'a>) -> BatchResult<Option<PackedColumnOutput>>
        + Ungil,
    ) -> PyResult<Option<Self>> {
        let Some(output) = self.reduce_packed_columns_detached(py, kernel)? else {
            return Ok(None);
        };
        let Some(output) = output else {
            return Ok(None);
        };
        Ok(Some(
            self.assemble_packed_result(output, output_frame)
                .map_err(PyErr::from)?,
        ))
    }

    pub(crate) fn map_packed_points_detached(
        &self,
        py: Python<'_>,
        output_frame: Frame,
        output_axes: crate::geometry::CoordinateAxes,
        map: impl Fn(crate::geometry::Point) -> crate::error::Result<crate::geometry::Point> + Ungil,
    ) -> PyResult<Option<Self>> {
        let Some(columns) = self.rows.packed_columns() else {
            return Ok(None);
        };
        let output = py
            .detach(move || map_selected_points(columns, output_axes, map))
            .map_err(PyErr::from)?;
        Ok(Some(
            self.assemble_packed_result(output, output_frame)
                .map_err(PyErr::from)?,
        ))
    }

    pub(crate) fn map_packed_to_crs_detached(
        &self,
        py: Python<'_>,
        output_frame: Frame,
        transformer: crate::crs::TransformPlan,
    ) -> PyResult<Option<Self>> {
        let Some(columns) = self.rows.packed_columns() else {
            return Ok(None);
        };
        let output = py
            .detach(move || map_selected_to_crs(columns, &transformer))
            .map_err(PyErr::from)?;
        Ok(Some(
            self.assemble_packed_result(output, output_frame)
                .map_err(PyErr::from)?,
        ))
    }

    pub(crate) fn replace_packed_coords_detached(
        &self,
        py: Python<'_>,
        output_frame: Frame,
        replacement: CoordinateReplacement,
    ) -> PyResult<Option<Self>> {
        let Some(columns) = self.rows.packed_columns() else {
            return Ok(None);
        };
        let output = py
            .detach(move || -> BatchResult<PackedColumnOutput> {
                let mut replacement_index = 0;
                let output =
                    replace_packed_columns_dispatch(columns, &replacement, &mut replacement_index)?;
                if replacement_index != replacement.len {
                    return Err(BatchError::Batch(
                        crate::geometry::GeometryErrorKind::message(format!(
                            "coordinates must have length {}; got length {replacement_index}",
                            replacement.len
                        )),
                    ));
                }
                Ok(output)
            })
            .map_err(PyErr::from)?;
        Ok(Some(
            self.assemble_packed_result(output, output_frame)
                .map_err(PyErr::from)?,
        ))
    }

    pub(crate) fn pair_packed_columns_detached<R>(
        py: Python<'_>,
        left: &Self,
        right: &Self,
        kernel: impl for<'a, 'b> FnOnce(PackedColumns<'a>, PackedColumns<'b>) -> BatchResult<R> + Ungil,
    ) -> PyResult<Option<R>>
    where
        R: Ungil,
    {
        let Some(left_columns) = left.rows.packed_columns() else {
            return Ok(None);
        };
        let Some(right_columns) = right.rows.packed_columns() else {
            return Ok(None);
        };
        let result = py
            .detach(move || kernel(left_columns, right_columns))
            .map_err(PyErr::from)?;
        Ok(Some(result))
    }

    /// Points-only reduce: returns `None` for mixed or line/polygon packed storage.
    pub(crate) fn reduce_packed_points_detached<R>(
        &self,
        py: Python<'_>,
        kernel: impl FnOnce(PointColumns<'_>) -> BatchResult<R> + Ungil,
    ) -> PyResult<Option<R>>
    where
        R: Ungil,
    {
        let GeometryArrayStorage::Packed(PackedStorage::Points { .. }) = self.storage() else {
            return Ok(None);
        };
        let rows = self.rows.clone();
        let result = py
            .detach(move || -> BatchResult<R> {
                let columns = rows.packed_columns().expect("points");
                let PackedColumns::Points(point_columns) = columns else {
                    unreachable!("reduce_packed_points_detached requires point storage");
                };
                kernel(point_columns)
            })
            .map_err(PyErr::from)?;
        Ok(Some(result))
    }

    pub(crate) fn pair_packed_points_detached<T>(
        &self,
        py: Python<'_>,
        right: &Self,
        kernel: impl FnOnce(&[f64], &[f64], &[f64], &[f64]) -> crate::error::Result<Vec<T>> + Ungil,
    ) -> PyResult<Option<Vec<T>>>
    where
        T: Ungil,
    {
        let (
            GeometryArrayStorage::Packed(PackedStorage::Points { .. }),
            GeometryArrayStorage::Packed(PackedStorage::Points { .. }),
        ) = (self.storage(), right.storage())
        else {
            return Ok(None);
        };
        let left_rows = self.rows.clone();
        let right_rows = right.rows.clone();
        let values = py
            .detach(move || -> BatchResult<Option<Vec<T>>> {
                let left_columns = left_rows.packed_columns().expect("points");
                let right_columns = right_rows.packed_columns().expect("points");
                let PackedColumns::Points(left) = left_columns else {
                    unreachable!("pair_packed_points_detached requires point storage");
                };
                let PackedColumns::Points(right) = right_columns else {
                    unreachable!("pair_packed_points_detached requires point storage");
                };
                let (Some(left_xy), Some(right_xy)) = (left.contiguous_xy(), right.contiguous_xy())
                else {
                    return Ok(None);
                };
                kernel(left_xy.xs, left_xy.ys, right_xy.xs, right_xy.ys)
                    .map(Some)
                    .map_err(BatchError::Batch)
            })
            .map_err(PyErr::from)?;
        Ok(values)
    }

    /// O(1) packed-storage X/Y swap — narrow escape hatch, not a general scan.
    pub(crate) fn swap_xy_packed_storage(&self) -> crate::error::Result<Option<Self>> {
        Ok(match self.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => Some(
                Self::packed_points_mapped(coords.swap_xy(), self.frame.clone(), row_map.clone())?,
            ),
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => Some(Self::packed_lines_mapped(
                coords.swap_xy(),
                offsets.clone(),
                self.frame.clone(),
                row_map.clone(),
            )?),
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => Some(Self::packed_polygons_mapped(
                coords.swap_xy(),
                ring_offsets.clone(),
                polygon_offsets.clone(),
                self.frame.clone(),
                row_map.clone(),
            )?),
            GeometryArrayStorage::Mixed(_) => None,
        })
    }
}
