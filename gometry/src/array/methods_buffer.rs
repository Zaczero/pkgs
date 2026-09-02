use std::num::NonZeroU32;

use crate::array::{GeometryArrayStorage, PackedColumns, PyGeometryArray, PyResult, Python};
use crate::crs::MetricModel;
use crate::geometry::BufferSide;
use crate::{DistanceUnit, F64Param};

impl PyGeometryArray {
    /// Direct packed point buffering for the common planar scalar case.
    ///
    /// The general buffer lane owns geographic projection, per-element
    /// distances, and sided lineal semantics. Point buffers need none of that:
    /// every row is one identical circle topology, so the output can be emitted
    /// directly into polygon columns.
    pub(crate) fn buffer_points_unary_packed(
        &self,
        py: Python<'_>,
        distance: &F64Param,
        quadrant_segments: NonZeroU32,
        side: BufferSide,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Option<Self>> {
        if self.is_compact_nullable() {
            return Ok(None);
        }
        if side != BufferSide::Both
            || !matches!(self.storage(), GeometryArrayStorage::Points { .. })
        {
            return Ok(None);
        }
        let Some(distance) = distance.as_scalar() else {
            return Ok(None);
        };
        if !distance.is_finite() || distance <= 0.0 {
            return Ok(None);
        }
        let model = crate::broadcast::resolve_metric(self.crs_str(), unit, "buffer")?;
        let MetricModel::Planar { to_metre } = model else {
            return Ok(None);
        };
        let distance = distance / to_metre.get();
        let missing = self.missing().cloned();
        let missing_for_kernel = missing.clone();
        let output =
            self.try_map_packed_columns_detached(py, self.frame.clone(), move |columns| {
                match columns {
                    PackedColumns::Points(points) => crate::array::buffer_point_columns(
                        &points,
                        distance,
                        quadrant_segments,
                        missing_for_kernel.as_ref(),
                    ),
                    PackedColumns::Lines(_) | PackedColumns::Polygons(_) => Ok(None),
                }
            })?;
        Ok(output.map(|array| array.with_missing_mask(missing)))
    }
}
