use crate::Shape;
use crate::dispatch::kernels;
use crate::geometry::{CoordSeq, CoordinateAxes, CsrOffsetColumn};
use crate::py::classes::coordinate_methods::map_coordinate_sequence_callback;
use crate::py::methods::unary_constructive_methods::{
    Py, PyAny, PyGeometry, PyGeometryArray, PyResult, Python, pymethods,
};
use crate::py::vectors::Groups;

unary_shape_method!(
    build_area,
    BuildArea,
    doc_build_area,
    kernels::unary_build_area,
    kernels::unary_build_area_array,
    synthesized
);
unary_shape_method!(
    node,
    Node,
    doc_node,
    kernels::unary_node,
    kernels::unary_node_array,
    synthesized
);
unary_shape_method!(
    unique_points,
    UniquePoints,
    doc_unique_points,
    kernels::unary_unique_points
);
unary_shape_method!(
    split_antimeridian,
    SplitAntimeridian,
    doc_split_antimeridian,
    kernels::unary_split_antimeridian
);

#[pymethods]
impl PyGeometry {
    /// Consecutive vertex pairs from each line and polygon ring, in source order.
    ///
    /// Includes closing ring edges and repeated-coordinate pairs; never joins
    /// independent parts or rings. Points and empty geometries have no segments.
    /// Every segment retains its source ordinates, CRS, and epoch.
    ///
    /// Returns
    /// -------
    /// GeometryArray[LineString]
    pub fn segments(&self, py: Python<'_>) -> PyResult<PyGeometryArray> {
        let segments = py.detach(|| self.shape.segments())?;
        if segments.is_empty() {
            return Ok(PyGeometryArray::packed_lines(
                CoordSeq::empty(self.shape.axes()),
                CsrOffsetColumn::try_new(vec![0], 0)?,
                self.frame.clone(),
            )?);
        }
        Ok(PyGeometryArray::from_shapes(segments, self.frame.clone())?)
    }

    /// Map each line, ring, or point coordinate sequence independently.
    ///
    /// The callback receives a read-only ``(N, D)`` float64 array in native
    /// ordinate order (XY, XYZ, XYM, or XYZM). It may change N, but must retain
    /// D and return finite coordinates. A point receives one coordinate and
    /// may return zero or one. A multipoint receives its stored point sequence;
    /// changing its size replaces its point membership, including empty slots.
    /// Empty components are retained without calling the callback.
    ///
    /// Parts, rings, CRS, epoch, and ordinate schemas stay with their source.
    /// Empty output empties a point or line, removes a hole, or empties a
    /// polygon whose rings all become empty. A nonempty hole with an empty
    /// shell is an error. Nonempty lines need two vertices; nonempty rings
    /// use constructor admission (three open corners, or four closed
    /// vertices, closed on all active ordinates). No validity repair is run.
    ///
    /// Parameters
    /// ----------
    /// func : callable
    ///     Per-sequence transformation, returning a two-dimensional matrix.
    ///
    /// Returns
    /// -------
    /// Geometry
    pub fn map_coordinate_sequences(
        &self,
        py: Python<'_>,
        func: &pyo3::Bound<'_, PyAny>,
    ) -> PyResult<crate::Typed> {
        let mapped = self.shape.try_map_coordinate_sequences(&mut |coords| {
            map_coordinate_sequence_callback(py, coords, func)
        })?;
        Ok(self.typed_shape(mapped))
    }

    #[doc = doc_polygonize!(scalar)]
    pub fn polygonize(&self, py: Python<'_>) -> PyResult<PyGeometryArray> {
        self.polygonize_impl(py)
    }
    #[doc = doc_line_merge!(scalar)]
    #[pyo3(signature = (*, directed = false))]
    pub fn line_merge(&self, py: Python<'_>, directed: bool) -> PyResult<crate::Typed> {
        self.line_merge_impl(py, directed)
    }
    #[doc = doc_extremes!(scalar)]
    pub fn extremes(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.extremes_impl(py)
    }
    #[doc = doc_self_intersections!(scalar)]
    pub fn self_intersections(&self, py: Python<'_>) -> PyResult<PyGeometryArray> {
        self.self_intersections_impl(py)
    }
}

#[pymethods]
impl PyGeometryArray {
    /// Consecutive vertex pairs per source geometry.
    ///
    /// Each group contains that row's line and ring segments, including closing
    /// edges, without joining parts or rings. Point and empty rows have empty
    /// groups; missing rows remain missing. Ordinates, CRS, and epoch are retained.
    ///
    /// Returns
    /// -------
    /// Groups[GeometryArray[LineString] | None]
    pub fn segments(&self, py: Python<'_>) -> PyResult<Groups> {
        let builder = py.detach(|| -> PyResult<_> {
            let mut builder = crate::CsrBuilder::with_capacity(0, self.len());
            for row in self.row_operation().rows_with_missing(|| ()) {
                match row {
                    crate::array::ArrayRow::Missing { .. } => {
                        builder.push_row(std::iter::empty())?;
                    },
                    crate::array::ArrayRow::Present(row) => {
                        let segments = row
                            .row()
                            .try_with_shape(Shape::segments)
                            .map_err(|error| crate::note_array_row(error.into(), row.logical()))?;
                        builder.push_row(segments)?;
                    },
                }
            }
            Ok(builder)
        })?;
        if builder.len() == 0 {
            let (_, offsets) = builder.finish();
            return Ok(Groups::from_packed_line_groups(
                self.frame.clone(),
                CoordSeq::empty(self.uniform_axes().unwrap_or(CoordinateAxes::XY)),
                CsrOffsetColumn::try_new(vec![0], 0)?,
                offsets,
                self.missing().cloned(),
            )?);
        }
        Ok(Groups::from_geometry_builder(
            self.frame.clone(),
            builder,
            self.missing().cloned(),
        )?)
    }

    /// Map each point, line, or ring sequence independently in row order.
    ///
    /// Uses ``Geometry.map_coordinate_sequences`` admission and callback rules,
    /// preserving each row's grouping, native ordinate schema, CRS, and epoch.
    /// Missing rows remain missing and do not invoke the callback. Errors carry
    /// the source row; callback exceptions retain their original type.
    ///
    /// Parameters
    /// ----------
    /// func : callable
    ///     Receives one read-only ``(N, D)`` float64 matrix at a time and may
    ///     change N while preserving D.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    pub fn map_coordinate_sequences(
        &self,
        py: Python<'_>,
        func: &pyo3::Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        if self.len() == 0 || self.missing().is_some_and(|mask| mask.present_count() == 0) {
            return Ok(self.clone());
        }
        self.map_shapes_attached(|shape, row| {
            crate::check_python_signals(py, row)?;
            shape.try_map_coordinate_sequences(&mut |coords| {
                map_coordinate_sequence_callback(py, coords, func)
            })
        })
    }

    #[doc = doc_polygonize!(array)]
    pub fn polygonize(&self, py: Python<'_>) -> PyResult<Groups> {
        self.polygonize_impl(py)
    }
    #[doc = doc_line_merge!(array)]
    #[pyo3(signature = (*, directed = false))]
    pub fn line_merge(&self, py: Python<'_>, directed: bool) -> PyResult<Self> {
        self.line_merge_impl(py, directed)
    }
    #[doc = doc_extremes!(array)]
    pub fn extremes(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.extremes_impl(py)
    }
    #[doc = doc_self_intersections!(array)]
    pub fn self_intersections(&self, py: Python<'_>) -> PyResult<Groups> {
        self.self_intersections_impl(py)
    }
}
