#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::exceptions::{PyTypeError, PyValueError};

use super::*;

#[pymethods]
impl PyCoordinates {
    // NEP 13: opt out of numpy ufunc dispatch (we have our own & | - ^ /
    // predicates)
    #[classattr]
    #[expect(non_upper_case_globals, reason = "Python dunder name")]
    const __array_ufunc__: Option<Py<PyAny>> = None;

    /// Number of vertices in this coordinate view.
    ///
    /// Returns
    /// -------
    /// int
    pub fn __len__(&self) -> usize {
        self.view.len()
    }

    /// Logical coordinate payload in bytes (numpy's ``nbytes`` convention):
    /// the stored ``f64`` ordinate values behind this view. Slices and
    /// array-backed views report only their logical rows; temporary NumPy
    /// matrices produced by ``numpy.asarray(coords)`` are not included.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    pub fn nbytes(&self) -> usize {
        self.view.nbytes()
    }

    /// ``sys.getsizeof`` support: the wrapper plus the logical Rust-side
    /// coordinate heap retained by this view. Array-backed coordinate views
    /// include logical coordinate payload and structural row metadata; shared
    /// backing buffers are reported like NumPy views, not as the full parent
    /// allocation.
    pub fn __sizeof__(&self) -> usize {
        std::mem::size_of::<Self>() + self.view.logical_heap_bytes()
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
        match parse_row_index_or_slice(index, self.view.len(), "coordinate")? {
            RowIndexOrSlice::Slice {
                start, step, count, ..
            } => {
                // Stream only the selected rows in one walk (no full
                // materialization): membership in the slice's arithmetic
                // progression is a cheap test per flat position; descending
                // slices collect ascending and reverse at the end.
                let mut tuples = Vec::with_capacity(count);
                if count > 0 {
                    let (first, last, stride) = if step > 0 {
                        (start, start + step * (count as isize - 1), step)
                    } else {
                        (start + step * (count as isize - 1), start, -step)
                    };
                    let mut error = None;
                    let mut position: isize = -1;
                    // Track the next selected position in the arithmetic
                    // progression instead of testing `(position - first) % stride`
                    // per coordinate — the modulo compiled to a per-row `idiv`.
                    let mut next = first;
                    self.view.for_each_point(|coord| {
                        position += 1;
                        if error.is_some() || position != next || position > last {
                            return;
                        }
                        next += stride;
                        match self.tuple(py, coord.point) {
                            Ok(tuple) => tuples.push(tuple),
                            Err(err) => error = Some(err),
                        }
                    });
                    if let Some(err) = error {
                        return Err(err);
                    }
                    if step < 0 {
                        tuples.reverse();
                    }
                }
                Ok(PyList::new(py, tuples)?.unbind().into())
            },
            RowIndexOrSlice::Index(index) => {
                let point = self
                    .view
                    .point_at(index)
                    .expect("index already range-checked")
                    .point;
                self.tuple(py, point)
            },
        }
    }

    /// Iterate coordinate tuples in vertex order.
    ///
    /// Returns
    /// -------
    /// iterator of tuple
    pub fn __iter__(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PyCoordinatesIter>> {
        let mut points = Vec::with_capacity(slf.view.len());
        slf.view.for_each_point(|coord| points.push(coord.point));
        Py::new(py, PyCoordinatesIter::new(points, slf.layout, false))
    }

    /// Iterate coordinate tuples in reverse vertex order.
    ///
    /// Returns
    /// -------
    /// iterator of tuple
    pub fn __reversed__(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PyCoordinatesIter>> {
        let mut points = Vec::with_capacity(slf.view.len());
        slf.view.for_each_point(|coord| points.push(coord.point));
        Py::new(py, PyCoordinatesIter::new(points, slf.layout, true))
    }

    pub fn __reduce__(&self) -> PyResult<Py<PyAny>> {
        Err(PyTypeError::new_err(
            "cannot pickle Coordinates; use numpy.asarray(coords) to materialize coordinates",
        ))
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
    #[getter]
    pub fn coordinate_axes(&self) -> &'static str {
        self.layout.unwrap_or_else(|| self.view.axes()).as_str()
    }

    /// Return a view of the same coordinates in a fixed ``coordinate_axes`` layout
    /// (``'XY'``/``'XYZ'``/``'XYM'``/``'XYZM'``): every tuple/column has that
    /// shape. Nested/tuple output (``to_nested`` / ``select`` iteration) uses
    /// ``None`` where a coordinate lacks the requested Z/M; ndarray columns
    /// (``.x``/``.y``/``.z``/``.m``) and ``numpy.asarray`` use NaN for absent
    /// axes. Makes a mixed-dimension array rectangular for iteration and
    /// ``numpy.asarray``.
    ///
    /// Parameters
    /// ----------
    /// axes : {'XY', 'XYZ', 'XYM', 'XYZM'}
    ///     The layout every tuple and column should take.
    ///
    /// Returns
    /// -------
    /// Coordinates
    ///     The same coordinates with the forced layout.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> coords = gm.LineString([(0, 0), (1, 1)]).coords
    /// >>> coords.select('XY').to_nested()
    /// [(0.0, 0.0), (1.0, 1.0)]
    pub fn select(&self, axes: CoordinateAxes) -> Self {
        Self {
            view: self.view.clone(),
            layout: Some(axes),
        }
    }

    /// Per-coordinate source geometry row (all ``0`` for a scalar geometry);
    /// the source row for each vertex of a ``GeometryArray``.
    #[getter]
    pub fn row_index(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::py::numpy::int64_array(py, self.view.row_index_i64())
    }

    /// The X ordinates as a read-only ``float64`` ``numpy.ndarray``.
    #[getter]
    pub fn x(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        column_axis_to_py(py, slf, CoordinateAxis::X)
    }

    /// The Y ordinates as a read-only ``float64`` ``numpy.ndarray``.
    #[getter]
    pub fn y(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        column_axis_to_py(py, slf, CoordinateAxis::Y)
    }

    /// The Z ordinates as a read-only ``float64`` ``numpy.ndarray`` of view
    /// length (``NaN`` when no coordinate carries Z).
    #[getter]
    pub fn z(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        column_axis_to_py(py, slf, CoordinateAxis::Z)
    }

    /// The M ordinates as a read-only ``float64`` ``numpy.ndarray`` of view
    /// length (``NaN`` when no coordinate carries M).
    #[getter]
    pub fn m(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        column_axis_to_py(py, slf, CoordinateAxis::M)
    }

    /// Return a dependency-free column dict — ``{'x': ndarray, 'y': ndarray, …}`` — ready
    /// for ``pandas``/``polars`` (``pd.DataFrame(coords.to_dict())``).
    /// ``z``/``m`` columns appear when present or `select`-forced (``NaN`` for
    /// rows that lack them); ``index=True`` adds the source-geometry row
    /// column.
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
        let layout = this.layout.unwrap_or_else(|| this.view.axes());
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
                crate::py::numpy::int64_array(py, slf.borrow().view.row_index_i64())?,
            )?;
        }
        Ok(dict)
    }

    /// NumPy array protocol: export as a ``(N, dims)`` ``float64`` ndarray.
    ///
    /// Parameters
    /// ----------
    /// dtype : float, optional
    ///     ``None`` or ``numpy.float64`` (the native layout); other floating
    ///     dtypes are cast with ``astype``.
    /// copy : bool, optional
    ///     When ``False``, raises — coordinate export always copies.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     The ``(N, dims)`` coordinate matrix.
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
        let kind = dtype.getattr("kind")?.extract::<String>()?;
        let itemsize = dtype.getattr("itemsize")?.extract::<usize>()?;
        if kind == "f" && itemsize == 8 {
            return Ok(array);
        }
        if kind != "f" {
            return Err(GeometryError::new_err("dtype must be float64 or None"));
        }
        Ok(array.bind(py).call_method1("astype", (dtype,))?.unbind())
    }

    /// Return the coordinates as a nested structure mirroring the geometry's topology
    /// (point → ``[x, y]``, line → list of tuples, polygon → list of rings;
    /// arrays → one entry per present geometry) — the ``__geo_interface__``-style
    /// nesting, as Python lists. Missing array rows are skipped. The flat
    /// columns do not preserve this shape.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.LineString([(0, 0), (1, 1)]).coords.to_nested()
    /// [(0.0, 0.0), (1.0, 1.0)]
    pub fn to_nested(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.view.source() {
            coordinates::CoordinateSource::Shape(shape) => coordinates_object(py, shape),
            coordinates::CoordinateSource::Array(storage, missing) => {
                let nested = match storage {
                    GeometryArrayStorage::Mixed(items) => items
                        .iter()
                        .enumerate()
                        .filter(|(row, _)| !missing.is_some_and(|mask| mask[*row]))
                        .map(|(_, item)| coordinates_object(py, &item.shape))
                        .collect::<PyResult<Vec<_>>>()?,
                    GeometryArrayStorage::Points { coords, row_map } => {
                        let map = row_map.as_deref();
                        (0..crate::array::point_logical_len(coords, map))
                            .filter(|&index| !missing.is_some_and(|mask| mask[index]))
                            .map(|index| {
                                coordinates_object(
                                    py,
                                    &Shape::Point(
                                        coords.point_at(crate::array::physical_row(map, index)),
                                    ),
                                )
                            })
                            .collect::<PyResult<Vec<_>>>()?
                    },
                    GeometryArrayStorage::Lines { .. } | GeometryArrayStorage::Polygons { .. } => {
                        storage
                            .iter_shapes()
                            .enumerate()
                            .filter(|(row, _)| !missing.is_some_and(|mask| mask[*row]))
                            .map(|(_, shape)| coordinates_object(py, &shape))
                            .collect::<PyResult<Vec<_>>>()?
                    },
                };
                Ok(PyList::new(py, nested)?.unbind().into())
            },
        }
    }

    /// Value equality against another `Coordinates` or a sequence of coordinate
    /// tuples. Other types defer with ``NotImplemented``.
    pub fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> Py<PyAny> {
        if let Ok(other_coords) = other.extract::<PyRef<Self>>() {
            return py_bool(py, self.coordinates_equal(&other_coords));
        }
        if other.try_iter().is_ok() {
            return match self.coordinates_equal_sequence(other) {
                Ok(equal) => py_bool(py, equal),
                Err(_) => return py.NotImplemented(),
            };
        }
        py.NotImplemented()
    }

    /// ``coord in coords`` — whether a coordinate tuple appears in the
    /// sequence.
    /// Whether a coordinate tuple appears among the vertices.
    ///
    /// Returns
    /// -------
    /// bool
    pub fn __contains__(&self, item: &Bound<'_, PyAny>) -> bool {
        parse_coordinate_member(item).is_some_and(|needle| {
            let mut found = false;
            self.view.for_each_point(|coord| {
                if !found && coord.point == needle {
                    found = true;
                }
            });
            found
        })
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
        value: &Bound<'_, PyAny>,
        start: i64,
        stop: Option<i64>,
    ) -> PyResult<usize> {
        let needle = parse_coordinate_member(value);
        let len = self.view.len();
        let clamp = |bound: i64| -> usize {
            let resolved = if bound < 0 {
                bound + i64::try_from(len).unwrap_or(i64::MAX)
            } else {
                bound
            };
            usize::try_from(resolved.max(0)).unwrap_or(0).min(len)
        };
        let start = clamp(start);
        let stop = stop.map_or(len, clamp);
        if let Some(needle) = needle {
            let mut position = 0_usize;
            let mut found = None;
            self.view.for_each_point(|coord| {
                if found.is_none() && position >= start && position < stop && coord.point == needle
                {
                    found = Some(position);
                }
                position += 1;
            });
            if let Some(position) = found {
                return Ok(position);
            }
        }
        let value = value
            .repr()
            .and_then(|repr| repr.extract::<String>())
            .unwrap_or_else(|_| "value".to_owned());
        Err(PyValueError::new_err(format!(
            "{value} is not in Coordinates"
        )))
    }

    /// Number of coordinates equal to ``value``.
    ///
    /// Parameters
    /// ----------
    /// value : object
    ///     The coordinate value to count.
    ///
    /// Returns
    /// -------
    /// int
    pub fn count(&self, value: &Bound<'_, PyAny>) -> usize {
        let Some(needle) = parse_coordinate_member(value) else {
            return 0;
        };
        let mut count = 0_usize;
        self.view.for_each_point(|coord| {
            if coord.point == needle {
                count += 1;
            }
        });
        count
    }

    pub fn __repr__(&self) -> String {
        format!(
            "<Coordinates {} len={}>",
            self.view.axes().as_str(),
            self.view.len()
        )
    }
}
