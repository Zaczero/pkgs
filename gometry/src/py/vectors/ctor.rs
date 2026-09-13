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
        let values = PyGeometryArray::packed_lines(coords, line_offsets, frame)?;
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
            let mut builder = CsrBuilder::new();
            for row in rows {
                builder.push_row(row)?;
            }
            builder.finish()
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
        let (vertices, offsets) = builder.finish().map_err(pyo3::PyErr::from)?;
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

fn self_intersection_csr(
    array: &PyGeometryArray,
    domain: TopologyDomain,
) -> Result<CsrBuilder<Shape>> {
    let mut builder = CsrBuilder::<Shape>::new();
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
    use crate::error::{ErrorKind, ResourceError};
    use crate::geometry::Shape;
    use crate::io::parse_wkt;
    use crate::test_support::{allocation_count, fail_allocations_after};

    #[test]
    fn polygonize_assembly_failure_releases_all_output_categories() {
        let shape = line("LINESTRING (0 0, 2 1)");
        let Shape::LineString(coords) = &shape else {
            panic!("line fixture")
        };
        let observer = std::sync::Arc::downgrade(&coords.column_arcs().xs.owner().unwrap());
        let full = crate::geometry::PolygonizeFull {
            polygons: vec![shape.clone()],
            cuts: vec![vec![shape.clone()], vec![shape.clone()]],
            dangles: vec![vec![shape.clone()]],
            invalid_rings: vec![vec![shape]],
        };
        let result = fail_allocations_after(0, || Groups::polygonize_full_parts(full, Frame::None));
        let Err(error) = result else {
            panic!("cuts allocation must fail");
        };
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
        assert!(observer.upgrade().is_none());
    }

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

    fn kernel_allocations(array: &PyGeometryArray) -> usize {
        let domain = domain(array);
        allocation_count(|| {
            for row in array.row_operation().rows_with_missing(|| ()) {
                if let crate::array::ArrayRow::Present(row) = row {
                    row.row()
                        .with_shape(|shape| {
                            crate::geometry::self_intersections_in_frame(shape, domain).unwrap()
                        })
                        .unwrap();
                }
            }
        })
        .1
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
            .finish()
            .expect("finish");
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
        .with_missing_mask(ValidityMask::from_vec(3, vec![false, true, false]).unwrap())
        .unwrap();
        let (shapes, offsets) = self_intersection_csr(&array, domain(&array))
            .expect("CSR walk")
            .finish()
            .expect("finish");
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

    #[test]
    fn csr_allocation_failure_is_native_until_the_closure_exits() {
        let array =
            PyGeometryArray::from_shapes(vec![line("LINESTRING (0 0, 1 0)")], Frame::None).unwrap();
        let domain = domain(&array);
        let _ = kernel_allocations(&array);
        drop(self_intersection_csr(&array, domain).expect("warm walk before the fault window"));
        let kernel = kernel_allocations(&array);
        let (_, walk) = allocation_count(|| {
            self_intersection_csr(&array, domain).expect("walk succeeds without a fault window")
        });
        assert!(
            walk > kernel,
            "CSR row growth must allocate after the infallible kernel (kernel={kernel}, walk={walk})"
        );
        let Err(error) = fail_allocations_after(kernel, || self_intersection_csr(&array, domain))
        else {
            panic!("offset growth must fail under rejection");
        };
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
        crate::test_support::initialize_python();
        let pyerr: PyErr = error.into();
        Python::attach(|py| {
            let value = pyerr.value(py);
            if let Ok(notes) = value.getattr("__notes__") {
                let notes: Vec<String> = notes.extract().unwrap_or_default();
                assert!(
                    notes.iter().all(|note| !note.contains("array element")),
                    "allocation failure must not grow a row note: {notes:?}"
                );
            }
            drop(pyerr);
        });
    }
}
