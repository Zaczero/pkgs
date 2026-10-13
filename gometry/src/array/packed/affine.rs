use super::{
    GeometryArrayStorage, OriginSpec, PackedStorage, PyGeometryArray, PyResult, Python,
    packed_per_row_self_origin_affine_columns, row_map_is_identity,
};

impl PyGeometryArray {
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
}
