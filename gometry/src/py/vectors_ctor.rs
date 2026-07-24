//! `Groups` bulk constructors: CSR ingestion from index/kernel row
//! outputs (usize ids cast zero-copy to i64) plus the ragged-row
//! collectors for geometry and cell fan-outs.

use super::*;

impl Groups {
    /// Adopt an already-built int64 CSR batch — the zero-copy hand-off from
    /// kernels that stream matches directly.
    pub fn from_int64_csr(ids: Vec<usize>, offsets: Vec<usize>) -> PyResult<Self> {
        // Row ids and offsets are bounded by `isize::MAX` (the Vec length
        // bound), so they always fit in i64; the bytemuck cast reuses both
        // allocations in place. The fallback arm only fires on an
        // allocation-layout mismatch, which no supported target produces.
        fn cast_usize_vec(values: Vec<usize>) -> PyResult<Vec<i64>> {
            bytemuck::allocation::try_cast_vec(values).map_or_else(
                |(_, values)| {
                    values
                        .into_iter()
                        .map(|value| {
                            i64::try_from(value).map_err(|_| {
                                PyOverflowError::new_err("index does not fit in int64")
                            })
                        })
                        .collect()
                },
                Ok,
            )
        }
        let ids = cast_usize_vec(ids)?;
        let offsets = cast_usize_vec(offsets)?;
        // Validate before `len() - 1` so empty offsets never underflow.
        validate_csr_offsets(&offsets, ids.len())?;
        let n = offsets
            .len()
            .checked_sub(1)
            .ok_or_else(|| GeometryError::new_err("invalid CSR offsets"))?;
        Ok(Self {
            values: GroupsValues::Int64(ids.into()),
            offsets: offsets.into(),
            rows: 0..n,
        })
    }

    pub(super) fn from_geometry_csr(
        values: Arc<PyGeometryArray>,
        offsets: Arc<[i64]>,
        rows: Range<usize>,
    ) -> PyResult<Self> {
        validate_csr_offsets(&offsets, values.storage().len())?;
        Ok(Self {
            values: GroupsValues::Geometry(values),
            offsets,
            rows,
        })
    }

    /// Adopt a pre-built flat geometry column plus per-row `offsets` (one
    /// group per input row) — the shared hand-off from the per-row tessellation
    /// kernels (`delaunay_triangles`/`voronoi_polygons`/`polygonize`/…) that
    /// build the flat values and offsets in one detached pass.
    pub(crate) fn from_geometry_flat(values: PyGeometryArray, offsets: Vec<i64>) -> PyResult<Self> {
        // Validate CSR before computing the row range — empty offsets must not
        // underflow on `len() - 1`.
        validate_csr_offsets(&offsets, values.storage().len())?;
        let n = offsets
            .len()
            .checked_sub(1)
            .ok_or_else(|| GeometryError::new_err("invalid CSR offsets"))?;
        Self::from_geometry_csr(Arc::new(values), offsets.into(), 0..n)
    }

    /// Element-wise self-intersection rows as one flat geometry column.
    pub fn from_self_intersection_rows(array: &PyGeometryArray) -> PyResult<Self> {
        let mut offsets = vec![0_i64];
        let mut shapes = Vec::new();
        let geographic = crate::geometry::is_geographic_frame(&array.frame);
        for (missing, shape) in array.masked_shape_rows() {
            if missing {
                offsets.push(
                    i64::try_from(shapes.len())
                        .map_err(|_| PyOverflowError::new_err("offset does not fit in int64"))?,
                );
                continue;
            }
            for point in crate::geometry::self_intersections_in_frame(&shape, geographic) {
                shapes.push(Shape::Point(point));
            }
            offsets.push(
                i64::try_from(shapes.len())
                    .map_err(|_| PyOverflowError::new_err("offset does not fit in int64"))?,
            );
        }
        let values = Arc::new(PyGeometryArray::from_shapes(shapes, array.frame.clone()));
        let rows = offsets.len() - 1;
        Self::from_geometry_csr(values, offsets.into(), 0..rows)
    }

    pub(super) fn from_cell_csr(
        values: Arc<PyCellArray>,
        offsets: Arc<[i64]>,
        rows: Range<usize>,
    ) -> PyResult<Self> {
        validate_csr_offsets(&offsets, values.len())?;
        Ok(Self {
            values: GroupsValues::Cells(values),
            offsets,
            rows,
        })
    }

    /// One flat cell column plus per-input-cell row offsets — the ragged
    /// result of a cell array's `neighbors`/`children` (one row per input
    /// cell, in input order).
    pub(crate) fn from_cell_rows<I, R>(kind: GridKind, rows: I) -> PyResult<Self>
    where
        I: IntoIterator<Item = R>,
        R: AsRef<[u64]>,
    {
        let mut ids = Vec::new();
        let mut offsets = vec![0_i64];
        for row in rows {
            ids.extend_from_slice(row.as_ref());
            offsets.push(
                i64::try_from(ids.len()).map_err(|_| {
                    GeometryError::new_err("cell-group offset does not fit in int64")
                })?,
            );
        }
        Self::from_cell_flat(kind, ids, offsets)
    }

    pub(crate) fn from_cell_flat(
        kind: GridKind,
        ids: Vec<u64>,
        offsets: Vec<i64>,
    ) -> PyResult<Self> {
        let values = Arc::new(PyCellArray::from_trusted_ids(kind, ids));
        // Validate before `len() - 1` so empty offsets never underflow.
        validate_csr_offsets(&offsets, values.len())?;
        let n = offsets
            .len()
            .checked_sub(1)
            .ok_or_else(|| GeometryError::new_err("invalid CSR offsets"))?;
        Self::from_cell_csr(values, offsets.into(), 0..n)
    }
}
