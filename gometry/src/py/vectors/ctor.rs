//! `Groups` bulk constructors: CSR ingestion from index/kernel row
//! outputs (usize ids cast zero-copy to i64) plus the ragged-row
//! collectors for geometry and cell fan-outs.

use pyo3::Python;

use crate::CsrBuilder;
use crate::error::Result;
use crate::geometry::TopologyDomain;
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
                    values
                        .into_iter()
                        .map(|value| {
                            i64::try_from(value).map_err(|_| {
                                PyOverflowError::new_err("index does not fit in int64")
                            })
                        })
                        .collect::<pyo3::PyResult<Vec<_>>>()
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
        let values = PyGeometryArray::packed_lines(coords, line_offsets, frame)?;
        validate_csr_offsets(&group_offsets, values.len())?;
        let n = group_offsets
            .len()
            .checked_sub(1)
            .ok_or_else(|| GeometryError::new_err("invalid CSR offsets"))?;
        Self::from_geometry_csr(Arc::new(values), group_offsets.into(), 0..n)
    }

    /// Adopt a checked geometry CSR builder after its detached row walk.
    pub(crate) fn from_geometry_builder(
        frame: crate::Frame,
        builder: CsrBuilder<Shape>,
    ) -> PyResult<Self> {
        let (shapes, offsets) = builder.finish();
        let values = PyGeometryArray::from_shapes(shapes, frame)?;
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
            let mut builder = CsrBuilder::with_capacity(0, rows.len());
            for row in rows {
                builder.push_row(row)?;
            }
            Ok(builder.finish())
        }

        let crate::geometry::PolygonizeFull {
            polygons,
            cuts,
            dangles,
            invalid_rings,
        } = full;
        let cuts = diagnostic_csr(cuts)?;
        let dangles = diagnostic_csr(dangles)?;
        let invalid_rings = diagnostic_csr(invalid_rings)?;
        let adopt = |(shapes, offsets): (Vec<Shape>, Vec<i64>)| -> crate::error::Result<Self> {
            let rows = 0..offsets.len() - 1;
            Ok(Self {
                values: GroupsValues::Geometry(Arc::new(PyGeometryArray::from_shapes(
                    shapes,
                    frame.clone(),
                )?)),
                offsets: offsets.into(),
                rows,
            })
        };
        Ok((
            PyGeometryArray::from_shapes(polygons, frame.clone())?,
            adopt(cuts)?,
            adopt(dangles)?,
            adopt(invalid_rings)?,
        ))
    }

    /// Adopt a point CSR builder whose logical rows contain groups of four
    /// vertices per triangle, materializing the packed triangle geometry
    /// column only after CSR construction completes.
    pub(crate) fn from_point_builder(
        frame: crate::Frame,
        builder: CsrBuilder<crate::Point>,
    ) -> PyResult<Self> {
        let (vertices, offsets) = builder.finish();
        let values = if vertices.is_empty() {
            PyGeometryArray::from_shapes(Vec::new(), frame)?
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

    /// Element-wise self-intersection rows as one flat geometry column.
    pub fn from_self_intersection_rows(py: Python<'_>, array: &PyGeometryArray) -> PyResult<Self> {
        let domain = crate::geometry::topology_domain(&array.frame)?;
        let builder = py.detach(|| self_intersection_csr(array, domain))?;
        Self::from_geometry_builder(array.frame.clone(), builder)
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

fn self_intersection_csr(
    array: &PyGeometryArray,
    domain: TopologyDomain,
) -> Result<CsrBuilder<Shape>> {
    let mut builder = CsrBuilder::<Shape>::with_capacity(0, array.len());
    for row in array.row_operation().rows_with_missing(|| ()) {
        match row {
            crate::array::ArrayRow::Missing { .. } => builder.push_row(std::iter::empty())?,
            crate::array::ArrayRow::Present(row) => row.row().try_with_shape(|shape| {
                builder.push_row(
                    crate::geometry::self_intersections_in_frame(shape, domain)?
                        .into_iter()
                        .map(Shape::Point),
                )
            })?,
        }
    }
    Ok(builder)
}

#[cfg(test)]
mod self_intersection_csr_tests {
    use pyo3::prelude::*;

    use super::{Groups, GroupsValues, self_intersection_csr};
    use crate::array::{PyGeometryArray, ValidityMask};
    use crate::boundary::{Frame, crs_arc_static};
    use crate::geometry::Shape;
    use crate::io::parse_wkt;

    #[test]
    fn polygonize_assembly_preserves_empty_diagnostic_rows() {
        let full = crate::geometry::PolygonizeFull {
            polygons: Vec::new(),
            cuts: vec![Vec::new(), vec![line("LINESTRING (2 3, 4 5)")], Vec::new()],
            dangles: vec![Vec::new(), Vec::new(), Vec::new()],
            invalid_rings: vec![Vec::new(), Vec::new(), Vec::new()],
        };
        let (polygons, cuts, dangles, invalid) =
            Groups::polygonize_full_parts(full, Frame::None).unwrap();
        assert_eq!(polygons.len(), 0);
        assert_eq!(cuts.offsets.as_ref(), &[0, 0, 1, 1]);
        assert_eq!(dangles.offsets.as_ref(), &[0, 0, 0, 0]);
        assert_eq!(invalid.offsets.as_ref(), &[0, 0, 0, 0]);
    }

    fn line(wkt: &str) -> Shape {
        parse_wkt(wkt).expect("test WKT")
    }

    fn domain(array: &PyGeometryArray) -> crate::geometry::TopologyDomain {
        crate::geometry::topology_domain(&array.frame).expect("topology domain")
    }

    #[test]
    fn crossing_line_keeps_the_intersection_point() {
        let array = PyGeometryArray::from_shapes(
            vec![line("LINESTRING (0 0, 2 0, 1 1, 1 -1)")],
            Frame::None,
        )
        .unwrap();
        let (shapes, offsets) = self_intersection_csr(&array, domain(&array))
            .expect("CSR walk")
            .finish();
        assert_eq!(offsets, [0, 1]);
        let Shape::Point(point) = shapes[0] else {
            panic!("intersection is a point");
        };
        assert_eq!((point.x, point.y), (1.0, 0.0));
    }

    #[test]
    fn missing_and_empty_rows_keep_empty_offsets() {
        let array = PyGeometryArray::from_shapes(
            vec![
                line("LINESTRING (0 0, 2 0, 1 1, 1 -1)"),
                line("LINESTRING (0 0, 1 0)"),
                line("LINESTRING EMPTY"),
            ],
            Frame::None,
        )
        .unwrap()
        .with_missing_mask(ValidityMask::from_vec(3, vec![false, true, false]))
        .unwrap();
        let (shapes, offsets) = self_intersection_csr(&array, domain(&array))
            .expect("CSR walk")
            .finish();
        assert_eq!(offsets, [0, 1, 1, 1]);
        assert_eq!(shapes.len(), 1);
        let Shape::Point(point) = shapes[0] else {
            panic!("intersection is a point");
        };
        assert_eq!((point.x, point.y), (1.0, 0.0));
    }

    #[test]
    fn groups_reuse_the_input_frame() {
        crate::test_support::initialize_python();
        let frame = Frame::new(Some(crs_arc_static("EPSG:4326")), None).expect("frame");
        let array =
            PyGeometryArray::from_shapes(vec![line("LINESTRING (0 0, 1 0)")], frame.clone())
                .unwrap();
        let groups =
            Python::attach(|py| Groups::from_self_intersection_rows(py, &array)).expect("groups");
        let GroupsValues::Geometry(values) = &groups.values else {
            panic!("geometry groups");
        };
        assert_eq!(values.frame, frame);
        assert_eq!(groups.offsets.as_ref(), &[0, 0]);
    }
}
