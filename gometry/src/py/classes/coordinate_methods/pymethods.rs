use pyo3::exceptions::{PyIndexError, PyOverflowError, PyValueError};
use pyo3::types::PyInt;
use pyo3::{IntoPyObjectExt as _, PyErr};

use crate::geometry::storage;
use crate::geometry::storage::PackedStorage;
use crate::py::classes::coordinate_methods::{
    Bound, CoordinateAxes, CoordinateAxis, GeometryArrayStorage, GeometryError, Py, PyAny,
    PyAnyMethods as _, PyCoordinates, PyCoordinatesIter, PyDict, PyDictMethods as _, PyList, PyRef,
    PyResult, Python, RowIndexOrSlice, Shape, column_axis_to_py, coordinates, coordinates_object,
    parse_row_index_or_slice, py_bool, pymethods,
};
use crate::py::numpy::{astype_readonly, int64_array, numpy_module};
use crate::py::row::normalize_row;
use crate::py::sizeof::python_sizeof;

#[pymethods]
impl PyCoordinates {
    /// Number of vertices in this coordinate view.
    ///
    /// Returns
    /// -------
    /// int
    pub fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        self.view.len_for_python(py)
    }

    /// Logical coordinate payload in bytes (numpy's ``nbytes`` convention):
    /// the stored ``f64`` ordinate values behind this view only. Slices and
    /// array-backed views report only their logical rows. Temporary NumPy
    /// matrices from ``numpy.asarray(coords)`` and any lazy prepared-geometry
    /// or membership sidecars on the parent geometry/array are not included —
    /// those live on the owner, not on this view.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    pub fn nbytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyInt>> {
        self.view.nbytes_pyint(py)
    }

    /// ``sys.getsizeof`` support: Python's allocation base plus the logical
    /// Rust-side coordinate heap retained by this view (ordinate payload and
    /// structural row metadata for array-backed views). Shared backing buffers
    /// are estimated from the visible view, not the full parent allocation.
    /// Parent geometry/array sidecars (prepared caches, coverage membership,
    /// …) are not owned by the view and are not counted here.
    ///
    /// Raises
    /// ------
    /// OverflowError
    ///     If the reported size exceeds Python's ``Py_ssize_t`` range.
    pub fn __sizeof__(slf: &Bound<'_, Self>) -> PyResult<isize> {
        python_sizeof(
            slf.py(),
            slf.as_any(),
            slf.borrow().view.logical_heap_bytes(),
        )
    }

    /// Select vertices by integer or slice.
    ///
    /// An ``int`` returns one coordinate tuple ``(x, y[, z[, m]])``.
    /// A ``slice`` returns a ``list`` of those tuples.
    ///
    /// Returns
    /// -------
    /// tuple or list of tuple
    pub fn __getitem__(&self, py: Python<'_>, index: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if index.cast::<pyo3::types::PySlice>().is_ok() {
            let len = self.view.len_for_python(py)?;
            return match parse_row_index_or_slice(index, len, "coordinate")? {
                RowIndexOrSlice::Slice {
                    start, step, count, ..
                } => {
                    // Stream only the selected rows in one walk (no full
                    // materialization): membership in the slice's arithmetic
                    // progression is a cheap test per flat position; descending
                    // slices collect ascending and reverse at the end.
                    let capacity =
                        coordinates::checked_vector_len::<Py<PyAny>>(count, "coordinate slice")?;
                    let mut tuples = Vec::with_capacity(capacity);
                    if count > 0 {
                        use std::ops::ControlFlow;
                        let first = if step > 0 {
                            start
                        } else {
                            start.wrapping_add(step.wrapping_mul(count.wrapping_sub(1) as isize))
                        } as usize;
                        let stride = step.unsigned_abs();
                        let mut position = 0_usize;
                        // Track the next selected position in the arithmetic
                        // progression instead of testing `(position - first) % stride`
                        // per coordinate — the modulo compiled to a per-row `idiv`.
                        let mut next = first;
                        if let ControlFlow::Break(result) =
                            self.view.try_for_each_point(&mut |coord| {
                                let current = position;
                                position = position.wrapping_add(1);
                                if current != next {
                                    return ControlFlow::Continue(());
                                }
                                match self.tuple(py, coord) {
                                    Ok(tuple) => {
                                        tuples.push(tuple);
                                        if tuples.len() == count {
                                            return ControlFlow::Break(Ok(()));
                                        }
                                        next = next.wrapping_add(stride);
                                        ControlFlow::Continue(())
                                    },
                                    Err(err) => ControlFlow::Break(Err(err)),
                                }
                            })
                        {
                            result?;
                        }
                        if step < 0 {
                            tuples.reverse();
                        }
                    }
                    Ok(PyList::new(py, tuples)?.unbind().into())
                },
                RowIndexOrSlice::Index(_) => unreachable!("slice input parsed as a slice"),
            };
        }
        let index = crate::py_isize_required("coordinate", index)?;
        let row = if index < 0 {
            let len = self.view.len_for_python(py)?;
            normalize_row(index, len, "coordinate")?
        } else {
            index as usize
        };
        let point = self
            .view
            .point_at(row)
            .ok_or_else(|| PyIndexError::new_err("coordinate index out of range"))?;
        self.tuple(py, point)
    }

    /// Iterate coordinate tuples in vertex order.
    ///
    /// Returns
    /// -------
    /// iterator of tuple
    pub fn __iter__(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PyCoordinatesIter>> {
        Py::new(
            py,
            PyCoordinatesIter::new(slf.view.clone(), slf.layout, false),
        )
    }

    /// Iterate coordinate tuples in reverse vertex order.
    ///
    /// Returns
    /// -------
    /// iterator of tuple
    pub fn __reversed__(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PyCoordinatesIter>> {
        Py::new(
            py,
            PyCoordinatesIter::new(slf.view.clone(), slf.layout, true),
        )
    }

    pub fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let callable = crate::gometry_lib_module(py)?
            .getattr(pyo3::intern!(py, "_unpickle_coordinates_v1"))?
            .unbind();
        let args = (
            self.owner.clone_ref(py),
            self.layout.map(CoordinateAxes::as_str),
        )
            .into_py_any(py)?;
        Ok((callable, args))
    }

    pub fn __copy__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    #[pyo3(signature = (memo))]
    pub fn __deepcopy__(slf: &Bound<'_, Self>, memo: &Bound<'_, PyAny>) -> Py<Self> {
        let _ = memo;
        slf.clone().unbind()
    }

    /// Coordinate layout: ``'XY'``, ``'XYZ'``, ``'XYM'``, or ``'XYZM'`` — the
    /// `select`-forced layout when set, else the union of the present axes.
    ///
    /// Returns
    /// -------
    /// CoordinateAxes
    ///     The exact ordinate layout visible through this coordinate view.
    #[getter]
    pub fn coordinate_axes(&self, py: Python<'_>) -> PyResult<&'static str> {
        match self.layout {
            Some(layout) => Ok(layout.as_str()),
            None => Ok(self.view.axes_for_python(py)?.as_str()),
        }
    }

    /// Return a view of the same coordinates in a fixed ``coordinate_axes`` layout
    /// (``'XY'``/``'XYZ'``/``'XYM'``/``'XYZM'``). This layout defines the schema
    /// of coordinate tuples, nested output, matrices, dictionaries, and equality.
    /// Tuple/nested output uses ``None`` where a coordinate lacks the requested
    /// Z/M; matrices and dictionary columns use ``NaN`` there. Independent axis
    /// accessors such as ``.z`` continue to read the source view, regardless of
    /// the selected layout. Selection does not change the geometry.
    ///
    /// Parameters
    /// ----------
    /// axes : {'XY', 'XYZ', 'XYM', 'XYZM'}
    ///     The layout every tuple and column should take.
    ///
    /// Returns
    /// -------
    /// Coordinates
    ///     The same coordinates with the selected view layout.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> coords = gm.LineString([(0, 0), (1, 1)]).coords
    /// >>> coords.select('XYZ').to_nested()
    /// [(0.0, 0.0, None), (1.0, 1.0, None)]
    pub fn select(&self, py: Python<'_>, axes: CoordinateAxes) -> Self {
        Self {
            view: self.view.clone(),
            owner: self.owner.clone_ref(py),
            layout: Some(axes),
        }
    }

    /// Per-coordinate source geometry row (all ``0`` for a scalar geometry);
    /// the source row for each vertex of a ``GeometryArray``.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     A materialized, read-only int64 array in flat coordinate order, one
    ///     source-row label per vertex.
    #[getter]
    pub fn row_index(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        int64_array(py, self.view.try_row_index_i64(py)?)
    }

    /// The X ordinates as a read-only ``float64`` ``numpy.ndarray``.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One read-only value per vertex in view order, in the geometry's
    ///     native coordinate units (raw coordinate units when CRS-free). The
    ///     array shares the immutable coordinate column when one is available.
    #[getter]
    pub fn x(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        column_axis_to_py(py, slf, CoordinateAxis::X)
    }

    /// The Y ordinates as a read-only ``float64`` ``numpy.ndarray``.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One read-only value per vertex in view order, in the geometry's
    ///     native coordinate units (raw coordinate units when CRS-free). The
    ///     array shares the immutable coordinate column when one is available.
    #[getter]
    pub fn y(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        column_axis_to_py(py, slf, CoordinateAxis::Y)
    }

    /// The Z ordinates as a read-only ``float64`` ``numpy.ndarray`` of view
    /// length (``NaN`` when no coordinate carries Z).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One read-only value per vertex in view order; missing Z ordinates
    ///     are ``NaN``. Values remain in the geometry's native coordinate
    ///     units; absent columns are materialized.
    #[getter]
    pub fn z(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        column_axis_to_py(py, slf, CoordinateAxis::Z)
    }

    /// The M ordinates as a read-only ``float64`` ``numpy.ndarray`` of view
    /// length (``NaN`` when no coordinate carries M).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One read-only value per vertex in view order; missing M ordinates
    ///     are ``NaN``. M is a measure ordinate, not a CRS unit; absent columns
    ///     are materialized.
    #[getter]
    pub fn m(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        column_axis_to_py(py, slf, CoordinateAxis::M)
    }

    /// Return a dependency-free column dict — ``{'x': ndarray, 'y': ndarray, …}`` — ready
    /// for ``pandas``/``polars`` (``pd.DataFrame(coords.to_dict())``).
    /// ``z``/``m`` columns appear when carried by the view or selected layout,
    /// with ``NaN`` for rows that lack an ordinate; ``index=True`` adds the
    /// source-geometry row column.
    ///
    /// Parameters
    /// ----------
    /// index : bool, default False
    ///     Add an ``'index'`` column with each coordinate's source row.
    ///
    /// Returns
    /// -------
    /// dict
    ///     One read-only ndarray per axis (plus ``'index'`` when requested).
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.LineString([(0, 0), (1, 1)]).coords.to_dict()['x'].tolist()
    /// [0.0, 1.0]
    #[pyo3(signature = (*, index = false))]
    pub fn to_dict<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        index: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        let this = slf.borrow();
        let layout = match this.layout {
            Some(layout) => layout,
            None => this.view.axes_for_python(py)?,
        };
        drop(this);
        dict.set_item("x", column_axis_to_py(py, slf.clone(), CoordinateAxis::X)?)?;
        dict.set_item("y", column_axis_to_py(py, slf.clone(), CoordinateAxis::Y)?)?;
        if layout.has_z() {
            dict.set_item("z", column_axis_to_py(py, slf.clone(), CoordinateAxis::Z)?)?;
        }
        if layout.has_m() {
            dict.set_item("m", column_axis_to_py(py, slf.clone(), CoordinateAxis::M)?)?;
        }
        if index {
            dict.set_item(
                "index",
                int64_array(py, slf.borrow().view.try_row_index_i64(py)?)?,
            )?;
        }
        Ok(dict)
    }

    /// NumPy array protocol: export as a ``(N, dims)`` floating ndarray.
    ///
    /// Parameters
    /// ----------
    /// dtype : float dtype, optional
    ///     ``None`` or any floating dtype (native export is ``float64``;
    ///     other floating dtypes, including opposite-endian ``float64``, are
    ///     cast with ``astype``).
    /// copy : bool, optional
    ///     When ``False``, raises — coordinate export always copies.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     The ``(N, dims)`` coordinate matrix (``float64`` by default).
    ///
    /// Raises
    /// ------
    /// ValueError
    ///     If ``copy`` is ``False``.
    /// GeometryError
    ///     If ``dtype`` is not a floating dtype.
    #[pyo3(signature = (dtype=None, copy=None))]
    pub fn __array__(
        &self,
        py: Python<'_>,
        dtype: Option<&Bound<'_, PyAny>>,
        copy: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        if copy == Some(false) {
            return Err(PyValueError::new_err(
                "gometry cannot return the requested array without copying",
            ));
        }
        let array = self.to_numpy_internal(py, None, f64::NAN)?;
        let Some(dtype) = dtype else {
            return Ok(array);
        };
        if dtype.is_none() {
            return Ok(array);
        }
        let numpy = numpy_module(py)?;
        let dtype = numpy.getattr("dtype")?.call1((dtype,))?;
        let kind = dtype.getattr("kind")?.extract::<String>()?;
        let native = dtype.getattr("isnative")?.extract::<bool>()?;
        if kind == "f" && dtype.getattr("itemsize")?.extract::<usize>()? == 8 && native {
            return Ok(array);
        }
        if kind != "f" {
            return Err(GeometryError::new_err(
                "dtype must be a floating dtype or None",
            ));
        }
        astype_readonly(array.bind(py), &dtype)
    }

    /// Return the coordinates as a nested structure mirroring the geometry's topology
    /// (point → ``[x, y]``, line → list of tuples, polygon → list of rings;
    /// arrays → one entry per present geometry) — the ``__geo_interface__``-style
    /// nesting, as Python lists. A selected view layout determines each
    /// coordinate's ordinates; absent selected Z/M values are ``None``. Missing
    /// array rows are skipped. The flat columns do not preserve this shape.
    ///
    /// Returns
    /// -------
    /// list
    ///     Nested Python lists and coordinate tuples matching the topology.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.LineString([(0, 0), (1, 1)]).coords.to_nested()
    /// [(0.0, 0.0), (1.0, 1.0)]
    pub fn to_nested(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.view.source() {
            coordinates::CoordinateSource::Shape(shape) => {
                coordinates_object(py, shape, self.layout)
            },
            coordinates::CoordinateSource::Array(rows) => {
                let nested = match rows.storage() {
                    GeometryArrayStorage::Mixed(shapes) => shapes
                        .iter()
                        .enumerate()
                        .filter(|(row, _)| {
                            !rows.missing().is_some_and(|mask| mask.is_missing(*row))
                        })
                        .map(|(_, shape)| coordinates_object(py, shape, self.layout))
                        .collect::<PyResult<Vec<_>>>()?,
                    GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                        let map = row_map.as_deref();
                        if rows.is_compact_nullable() {
                            let mask = rows
                                .missing()
                                .expect("compact nullable rows carry validity");
                            let nested = mask
                                .cursor()
                                .filter_map(|(_, present)| present)
                                .map(|present| {
                                    coordinates_object(
                                        py,
                                        &Shape::Point(
                                            coords.point_at(storage::physical_row(map, present)),
                                        ),
                                        self.layout,
                                    )
                                })
                                .collect::<PyResult<Vec<_>>>()?;
                            return Ok(PyList::new(py, nested)?.unbind().into());
                        }
                        (0..storage::point_logical_len(coords, map))
                            .map(|index| {
                                coordinates_object(
                                    py,
                                    &Shape::Point(
                                        coords.point_at(storage::physical_row(map, index)),
                                    ),
                                    self.layout,
                                )
                            })
                            .collect::<PyResult<Vec<_>>>()?
                    },
                    GeometryArrayStorage::Packed(
                        PackedStorage::Lines { .. } | PackedStorage::Polygons { .. },
                    ) => rows
                        .storage()
                        .iter_shapes()
                        .enumerate()
                        .filter(|(row, _)| {
                            !rows.missing().is_some_and(|mask| mask.is_missing(*row))
                        })
                        .map(|(_, shape)| {
                            shape
                                .map_err(PyErr::from)
                                .and_then(|shape| coordinates_object(py, &shape, self.layout))
                        })
                        .collect::<PyResult<Vec<_>>>()?,
                };
                Ok(PyList::new(py, nested)?.unbind().into())
            },
        }
    }

    /// Value equality against another `Coordinates` or a sequence of coordinate
    /// tuples. Other types defer with ``NotImplemented``. Errors from an
    /// admitted iterator or item comparison propagate. Arc-identity /
    /// short-circuit paths in ``coordinates_equal`` are preserved.
    pub fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if let Ok(other_coords) = other.extract::<PyRef<Self>>() {
            return Ok(py_bool(py, self.coordinates_equal(py, &other_coords)?));
        }
        let Some(items) = crate::boundary::reserve::py_iter_or_none(other)? else {
            return Ok(py.NotImplemented());
        };
        Ok(py_bool(py, self.coordinates_equal_iter(items)?))
    }

    /// Whether a coordinate tuple appears among the vertices, using the same
    /// visible layout as iteration / ``select``.
    ///
    /// Returns
    /// -------
    /// bool
    pub fn __contains__(&self, py: Python<'_>, item: &Bound<'_, PyAny>) -> PyResult<bool> {
        use std::ops::ControlFlow;
        let found = self.view.try_for_each_point(&mut |coord| match self
            .visible_equals(py, coord, item)
        {
            Ok(true) => ControlFlow::Break(Ok(true)),
            Ok(false) => ControlFlow::Continue(()),
            Err(err) => ControlFlow::Break(Err(err)),
        });
        match found {
            ControlFlow::Break(result) => result,
            ControlFlow::Continue(()) => Ok(false),
        }
    }

    /// First index of an equal coordinate in ``[start, stop)``.
    ///
    /// Parameters
    /// ----------
    /// value : object
    ///     The coordinate value to locate.
    /// start : int, default 0
    ///     First position searched.
    /// stop : int, optional
    ///     One past the last position searched.
    ///
    /// Returns
    /// -------
    /// int
    ///     The first matching position.
    ///
    /// Raises
    /// ------
    /// ValueError
    ///     If no coordinate in the window equals ``value``.
    #[pyo3(signature = (value, start = 0, stop = None), text_signature = "($self, value, start=0, stop=None)")]
    pub fn index(
        &self,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = crate::strict_i64)] start: i64,
        #[pyo3(from_py_with = crate::strict_optional_i64)] stop: Option<i64>,
    ) -> PyResult<usize> {
        use std::ops::ControlFlow;
        let len = if start < 0 || stop.is_some_and(|bound| bound < 0) {
            Some(self.view.len_for_python(py)?)
        } else {
            None
        };
        let normalize_negative = |bound: i64| -> usize {
            let len = i64::try_from(len.expect("negative bounds admitted the exact length"))
                .expect("Python sequence length fits isize");
            usize::try_from(len.wrapping_add(bound).max(0)).unwrap_or(0)
        };
        let start = if start < 0 {
            normalize_negative(start)
        } else {
            start as usize
        };
        let stop = match stop {
            Some(bound) if bound < 0 => normalize_negative(bound),
            Some(bound) => bound as usize,
            None => usize::MAX,
        };
        let mut position = 0_usize;
        let found = self.view.try_for_each_point(&mut |coord| {
            if position >= stop {
                return ControlFlow::Break(Ok(None));
            }
            if position >= start {
                match self.visible_equals(py, coord, value) {
                    Ok(true) => return ControlFlow::Break(Ok(Some(position))),
                    Ok(false) => {},
                    Err(err) => return ControlFlow::Break(Err(err)),
                }
            }
            let Some(next) = position.checked_add(1) else {
                return ControlFlow::Break(Err(PyOverflowError::new_err(
                    "coordinate index exceeds the native index range",
                )));
            };
            position = next;
            ControlFlow::Continue(())
        });
        match found {
            ControlFlow::Break(Ok(Some(position))) => return Ok(position),
            ControlFlow::Break(Err(err)) => return Err(err),
            ControlFlow::Break(Ok(None)) | ControlFlow::Continue(()) => {},
        }
        let value = value
            .repr()
            .and_then(|repr| repr.extract::<String>())
            .unwrap_or_else(|_| "value".to_owned());
        Err(PyValueError::new_err(format!(
            "{value} is not in Coordinates"
        )))
    }

    /// Number of coordinates equal to ``value`` under the visible layout
    /// (same representation as iteration / ``select``).
    ///
    /// Parameters
    /// ----------
    /// value : object
    ///     The coordinate value to count.
    ///
    /// Returns
    /// -------
    /// int
    pub fn count(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<usize> {
        // Sequential run walk via the shared visible-equality abstraction —
        // membership / index / count cannot drift from iteration.
        let mut count = 0_usize;
        let walk = self.view.try_for_each_point(&mut |coord| match self
            .visible_equals(py, coord, value)
        {
            Ok(true) => count.checked_add(1).map_or_else(
                || {
                    std::ops::ControlFlow::Break(PyOverflowError::new_err(
                        "coordinate count exceeds the native integer range",
                    ))
                },
                |next| {
                    count = next;
                    std::ops::ControlFlow::Continue(())
                },
            ),
            Ok(false) => std::ops::ControlFlow::Continue(()),
            Err(err) => std::ops::ControlFlow::Break(err),
        });
        match walk {
            std::ops::ControlFlow::Break(err) => Err(err),
            std::ops::ControlFlow::Continue(()) => Ok(count),
        }
    }

    pub fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let measure = self.view.exact_measure(py)?;
        let axes = match self.view.source() {
            coordinates::CoordinateSource::Shape(_) => measure.shape_axes(),
            coordinates::CoordinateSource::Array(_) => measure.present_axes(),
        };
        let len = measure.count_pyint(py).str()?.extract::<String>()?;
        Ok(format!("<Coordinates {} len={len}>", axes.as_str()))
    }
}
