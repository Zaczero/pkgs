//! `Groups` bulk constructors: CSR ingestion from index/kernel row
//! outputs (usize ids cast zero-copy to i64) plus the ragged-row
//! collectors for geometry and cell fan-outs.

use crate::CsrBuilder;
use crate::py::vectors::{
    Arc, GeometryError, GridKind, Groups, GroupsValues, PyCellArray, PyGeometryArray,
    PyOverflowError, PyResult, Range, Shape, validate_csr_offsets,
};

impl Groups {
    /// Adopt an already-built int64 CSR batch — the zero-copy hand-off from
    /// kernels that stream matches directly.
    pub fn from_int64_csr(ids: Vec<usize>, offsets: Vec<i64>) -> PyResult<Self> {
        // Row ids are bounded by `isize::MAX` (the Vec length bound), so they
        // always fit in i64; the bytemuck cast reuses that allocation in place.
        // The fallback arm only fires on an allocation-layout mismatch, which
        // no supported target produces.
        fn cast_usize_vec(values: Vec<usize>) -> PyResult<Vec<i64>> {
            bytemuck::allocation::try_cast_vec(values).map_or_else(
                |(_, values)| {
                    crate::try_collect_results(values.into_iter().map(|value| {
                        i64::try_from(value)
                            .map_err(|_| PyOverflowError::new_err("index does not fit in int64"))
                    }))
                },
                Ok,
            )
        }
        let ids = cast_usize_vec(ids)?;
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
        validate_csr_offsets(&offsets, values.len())?;
        Ok(Self {
            values: GroupsValues::Geometry(values),
            offsets,
            rows,
        })
    }

    /// Adopt a packed line column whose rows are already grouped by input
    /// provenance: `group_offsets` counts LineString rows in `coords`/`line_offsets`.
    pub(crate) fn from_packed_line_groups(
        frame: crate::Frame,
        coords: crate::geometry::CoordSeq,
        line_offsets: crate::geometry::CsrOffsetColumn,
        group_offsets: Vec<i64>,
    ) -> PyResult<Self> {
        let values = PyGeometryArray::packed_lines(coords, line_offsets, frame);
        validate_csr_offsets(&group_offsets, values.len())?;
        let n = group_offsets
            .len()
            .checked_sub(1)
            .ok_or_else(|| GeometryError::new_err("invalid CSR offsets"))?;
        Self::from_geometry_csr(Arc::new(values), group_offsets.into(), 0..n)
    }

    /// Adopt a fallible geometry CSR builder after its detached row walk.
    pub(crate) fn from_geometry_builder(
        frame: crate::Frame,
        builder: CsrBuilder<Shape>,
    ) -> PyResult<Self> {
        let (shapes, offsets) = builder.finish().map_err(pyo3::PyErr::from)?;
        let values = PyGeometryArray::from_shapes(shapes, frame);
        validate_csr_offsets(&offsets, values.len())?;
        let n = offsets
            .len()
            .checked_sub(1)
            .ok_or_else(|| GeometryError::new_err("invalid CSR offsets"))?;
        Self::from_geometry_csr(Arc::new(values), offsets.into(), 0..n)
    }

    pub(crate) fn polygonize_full_parts(
        full: crate::geometry::PolygonizeFull,
        frame: crate::Frame,
    ) -> crate::error::Result<(PyGeometryArray, Self, Self, Self)> {
        fn diagnostic_csr(rows: Vec<Vec<Shape>>) -> crate::error::Result<(Vec<Shape>, Vec<i64>)> {
            let mut builder = CsrBuilder::new();
            for row in rows {
                builder.push_row(row)?;
            }
            builder.finish()
        }

        let crate::geometry::PolygonizeFull { polygons, cuts, dangles, invalid_rings } = full;
        let cuts = diagnostic_csr(cuts)?;
        let dangles = diagnostic_csr(dangles)?;
        let invalid_rings = diagnostic_csr(invalid_rings)?;
        let adopt = |(shapes, offsets): (Vec<Shape>, Vec<i64>)| {
            let rows = 0..offsets.len() - 1;
            Self {
                values: GroupsValues::Geometry(Arc::new(PyGeometryArray::from_shapes(
                    shapes, frame.clone(),
                ))),
                offsets: offsets.into(),
                rows,
            }
        };
        Ok((
            PyGeometryArray::from_shapes(polygons, frame.clone()),
            adopt(cuts),
            adopt(dangles),
            adopt(invalid_rings),
        ))
    }

    /// Adopt a point CSR builder whose logical rows contain groups of four
    /// vertices per triangle, materializing the packed triangle geometry
    /// column only after CSR construction completes.
    pub(crate) fn from_point_builder(
        frame: crate::Frame,
        builder: CsrBuilder<crate::Point>,
    ) -> PyResult<Self> {
        let (vertices, offsets) = builder.finish().map_err(pyo3::PyErr::from)?;
        let values = if vertices.is_empty() {
            PyGeometryArray::from_shapes(Vec::new(), frame)
        } else {
            PyGeometryArray::packed_triangles(&vertices, frame)?
        };
        validate_csr_offsets(&offsets, values.len())?;
        let n = offsets
            .len()
            .checked_sub(1)
            .ok_or_else(|| GeometryError::new_err("invalid CSR offsets"))?;
        Self::from_geometry_csr(Arc::new(values), offsets.into(), 0..n)
    }

    /// Build coordinate-valued Groups from one fixed-width coordinate chunk per
    /// value. CSR offsets count coordinate rows, while the backing payload is
    /// flattened row-major for NumPy's two-dimensional row views.
    pub(crate) fn from_coordinate_chunks(
        dimensions: usize,
        builder: CsrBuilder<[f64; 4]>,
    ) -> PyResult<Self> {
        if !(2..=4).contains(&dimensions) {
            return Err(GeometryError::new_err(
                "coordinate dimensions must be positive",
            ));
        }
        let (chunks, offsets) = builder.finish().map_err(pyo3::PyErr::from)?;
        let mut values =
            crate::try_vec_with_capacity(chunks.len().checked_mul(dimensions).ok_or_else(
                || PyOverflowError::new_err("coordinate matrix size overflows usize"),
            )?)?;
        for chunk in chunks {
            for value in &chunk[..dimensions] {
                crate::try_push(&mut values, *value)?;
            }
        }
        validate_csr_offsets(&offsets, values.len() / dimensions)?;
        let rows = offsets
            .len()
            .checked_sub(1)
            .ok_or_else(|| GeometryError::new_err("invalid CSR offsets"))?;
        Ok(Self {
            values: GroupsValues::Coordinates {
                values: values.into(),
                dimensions,
            },
            offsets: offsets.into(),
            rows: 0..rows,
        })
    }

    /// Element-wise self-intersection rows as one flat geometry column.
    pub fn from_self_intersection_rows(array: &PyGeometryArray) -> PyResult<Self> {
        let mut builder = CsrBuilder::<Shape>::new();
        let domain = crate::geometry::topology_domain(&array.frame)?;
        for row in array.row_operation().rows_with_missing(|| ()) {
            match row {
                crate::array::ArrayRow::Missing { .. } => builder
                    .push_row(std::iter::empty())
                    .map_err(pyo3::PyErr::from)?,
                crate::array::ArrayRow::Present(row) => row
                    .row()
                    .with_shape(|shape| {
                        builder.push_row(
                            crate::geometry::self_intersections_in_frame(shape, domain)
                                .into_iter()
                                .map(Shape::Point),
                        )
                    })
                    .map_err(pyo3::PyErr::from)?,
            }
        }
        let (shapes, offsets) = builder.finish().map_err(pyo3::PyErr::from)?;
        let values = Arc::new(PyGeometryArray::from_shapes(shapes, array.frame.clone()));
        let rows = offsets
            .len()
            .checked_sub(1)
            .ok_or_else(|| GeometryError::new_err("invalid CSR offsets"))?;
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
        let mut builder = CsrBuilder::<u64>::new();
        for row in rows {
            builder
                .push_row(row.as_ref().iter().copied())
                .map_err(pyo3::PyErr::from)?;
        }
        let (ids, offsets) = builder.finish().map_err(pyo3::PyErr::from)?;
        Self::from_cell_flat(kind, ids, offsets)
    }

    pub(crate) fn from_cell_flat(
        kind: GridKind,
        ids: Vec<u64>,
        offsets: Vec<i64>,
    ) -> PyResult<Self> {
        let values = Arc::new(PyCellArray::from_ids(kind, ids));
        // Validate before `len() - 1` so empty offsets never underflow.
        validate_csr_offsets(&offsets, values.len())?;
        let n = offsets
            .len()
            .checked_sub(1)
            .ok_or_else(|| GeometryError::new_err("invalid CSR offsets"))?;
        Self::from_cell_csr(values, offsets.into(), 0..n)
    }
}
