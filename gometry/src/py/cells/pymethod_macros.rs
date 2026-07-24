#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Shared `macro_rules!` helpers for grid cell/coverage PyO3 boilerplate.

macro_rules! grid_cell_common_pymethods {
    (
        impl $py_type:ident {
            kind: $kind:expr,
            class_name: $class_name:literal,
            depth: $depth:ident,
            depth_name: $depth_name:literal,
            parse_depth: $parse_depth:path,
            parse_cell: $parse_cell:path,
            unpickle: $unpickle:literal,
            nbytes: $nbytes:expr,
            parent_text_signature: $parent_text_signature:literal,
            children_text_signature: $children_text_signature:literal,
            neighbors_doc: $neighbors_doc:literal,
            candidate_doc: $candidate_doc:literal,
            example_parent: $example_parent:literal,
            example_children: $example_children:literal,
            example_children_count: $example_children_count:literal,
            example_contains: $example_contains:literal,
            example_intersects: $example_intersects:literal,
            $(match_arg: $match_arg:literal,)?
            repr: $repr:ident $(,)?
            $(cell_int: |$int_self:ident| $int_expr:expr,)?
        }
    ) => {
        #[pyo3::pymethods]
        impl $py_type {
            /// Raw scalar cell id payload in bytes.
            ///
            /// Returns
            /// -------
            /// int
            #[getter]
            const fn nbytes(&self) -> usize {
                $nbytes
            }

            /// `sys.getsizeof` support: scalar cells are heap-free value ids.
            const fn __sizeof__(&self) -> usize {
                std::mem::size_of::<Self>()
            }

            fn __hash__(&self) -> u64 {
                cell_hash(self.cell)
            }

            /// Pickle support: a cell is its id.
            fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
                cell_reduce(self.cell, py, $unpickle)
            }

            /// Cell center as a WGS84 Point (lon/lat).
            ///
            /// Returns
            /// -------
            /// Point
            #[getter]
            fn center(&self) -> Typed {
                cell_center(self.cell)
            }

            /// The cell as a filled WGS84 Polygon (lon/lat).
            ///
            /// Returns
            /// -------
            /// Polygon
            #[getter]
            fn polygon(&self) -> Typed {
                cell_boundary(self.cell)
            }

            /// Geodesic area of the cell in square meters.
            ///
            /// Returns
            /// -------
            /// float
            #[getter]
            fn area(&self) -> f64 {
                GridCell::area_m2(self.cell)
            }

            #[doc = concat!("Parent cell at a coarser ", $depth_name, ".")]
            ///
            /// Parameters
            /// ----------
            #[doc = concat!(stringify!($depth), " : int, optional")]
            #[doc = concat!("    Target ", $depth_name, "; must not be finer than this cell's.")]
            #[doc = concat!("    Defaults to one coarser than this cell's ", $depth_name, ".")]
            ///
            /// Returns
            /// -------
#[doc = concat!("", $class_name, "")]
            ///     The ancestor cell.
            ///
            /// Raises
            /// ------
            /// GeometryError
            ///     If the target depth is invalid for this cell.
            ///
            /// Examples
            /// --------
            #[doc = $example_parent]
            #[pyo3(signature = ($depth = None), text_signature = $parent_text_signature)]
            fn parent(&self, $depth: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
                cell_parent(self.cell, $depth, $parse_depth).map(|cell| Self { cell })
            }

            #[doc = concat!("Child cells at a finer ", $depth_name, ".")]
            ///
            /// Parameters
            /// ----------
            #[doc = concat!(stringify!($depth), " : int, optional")]
            #[doc = concat!("    Target ", $depth_name, "; must not be coarser than this cell's.")]
            #[doc = concat!("    Defaults to one finer than this cell's ", $depth_name, ";")]
            ///     a maximum-depth cell has no children and yields an empty
            ///     CellArray.
            ///
            /// Returns
            /// -------
#[doc = concat!("CellArray of ", $class_name, "")]
            ///     The descendant cells.
            ///
            /// Raises
            /// ------
            /// GeometryError
            ///     If the target depth is invalid for this cell.
            ///
            /// Examples
            /// --------
            #[doc = $example_children]
            #[pyo3(signature = ($depth = None), text_signature = $children_text_signature)]
            fn children(&self, $depth: Option<&Bound<'_, PyAny>>) -> PyResult<PyCellArray> {
                cell_children_array($kind, self.cell, $depth, $parse_depth)
            }

            #[doc = concat!("Number of descendant cells at a finer ", $depth_name, ", counted closed-form without materializing them.")]
            ///
            /// Parameters
            /// ----------
            #[doc = concat!(stringify!($depth), " : int, optional")]
            #[doc = concat!("    Target ", $depth_name, "; must not be coarser than this cell's.")]
            #[doc = concat!("    Defaults to one finer than this cell's ", $depth_name, ";")]
            ///     a maximum-depth cell has no children and returns ``0``.
            ///
            /// Returns
            /// -------
            /// int
            ///     The exact descendant count (H3 pentagons have slightly fewer
            ///     than hexagons).
            ///
            /// Raises
            /// ------
            /// GeometryError
            ///     If the target depth is coarser than this cell's, or invalid.
            ///
            /// Examples
            /// --------
            #[doc = $example_children_count]
            #[pyo3(signature = ($depth = None), text_signature = $children_text_signature)]
            fn children_count(&self, $depth: Option<&Bound<'_, PyAny>>) -> PyResult<u64> {
                cell_descendant_count(self.cell, $depth, $parse_depth)
            }

            #[doc = $neighbors_doc]
            ///
            /// Returns
            /// -------
#[doc = concat!("CellArray of ", $class_name, "")]
            #[getter]
            fn neighbors(&self) -> PyCellArray {
                cell_neighbors_array($kind, self.cell)
            }

            $(
                #[expect(
                    clippy::allow_attributes,
                    reason = "the scoped allow below is required by mixed const/non-const grid id APIs"
                )]
                #[allow(
                    clippy::missing_const_for_fn,
                    reason = "some grid id extractors call non-const dependency APIs"
                )]
                fn __int__(&self) -> u64 {
                    let $int_self = self;
                    $int_expr
                }

                #[expect(
                    clippy::allow_attributes,
                    reason = "the scoped allow below is required by mixed const/non-const grid id APIs"
                )]
                #[allow(
                    clippy::missing_const_for_fn,
                    reason = "some grid id extractors call non-const dependency APIs"
                )]
                fn __index__(&self) -> u64 {
                    let $int_self = self;
                    $int_expr
                }
            )?

            /// Test whether this cell contains another cell (itself, or any
            /// descendant of it in the cell hierarchy).
            ///
            /// Parameters
            /// ----------
            #[doc = $candidate_doc]
            ///     The candidate cell.
            ///
            /// Returns
            /// -------
            /// bool
            ///
            /// Raises
            /// ------
            /// ParseError
            ///     If an id or token is not a valid cell.
            /// TypeError
            ///     If ``other`` is not a valid cell object, id, or token.
            ///
            /// Examples
            /// --------
            #[doc = $example_contains]
            fn contains(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
                cell_contains(self.cell, other, $parse_cell)
            }

            /// Test whether this cell intersects another cell (one contains the
            /// other — hierarchy cells cannot partially overlap).
            ///
            /// Parameters
            /// ----------
            #[doc = $candidate_doc]
            ///     The candidate cell.
            ///
            /// Returns
            /// -------
            /// bool
            ///
            /// Raises
            /// ------
            /// ParseError
            ///     If an id or token is not a valid cell.
            /// TypeError
            ///     If ``other`` is not a valid cell object, id, or token.
            ///
            /// Examples
            /// --------
            #[doc = $example_intersects]
            fn intersects(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
                cell_intersects(self.cell, other, $parse_cell)
            }

            /// The cell's token — ``print(cell)`` reads as data.
            fn __str__(&self) -> String {
                self.cell.token()
            }

            fn __repr__(&self) -> String {
                grid_cell_common_pymethods!(@repr $repr, self)
            }

            /// Cells compare by their canonical id. Other types defer with
            /// ``NotImplemented``.
            fn __richcmp__(&self, other: &Self, op: pyo3::basic::CompareOp) -> bool {
                cell_richcmp(self.cell, other.cell, op)
            }

            /// ``case Cell(value)`` destructures the public cell identity.
            #[classattr]
            const fn __match_args__() -> (&'static str,) {
                (grid_cell_common_pymethods!(@match_arg $($match_arg)?),)
            }

            /// ``copy.copy`` returns the object itself — cells are immutable
            /// values, so a copy IS the original (like ``tuple``).
            fn __copy__(slf: &Bound<'_, Self>) -> Py<Self> {
                slf.clone().unbind()
            }

            /// ``copy.deepcopy`` returns the object itself: every field is immutable
            /// and holds no Python references, so there is nothing to copy.
            #[pyo3(signature = (memo))]
            fn __deepcopy__(slf: &Bound<'_, Self>, memo: &Bound<'_, PyAny>) -> Py<Self> {
                let _ = memo;
                slf.clone().unbind()
            }
        }
    };
    (@repr h3, $self_:ident) => {
        format!(
            "<H3Cell {} resolution={}>",
            $self_.cell,
            u8::from($self_.cell.resolution())
        )
    };
    (@repr s2, $self_:ident) => {
        format!("<S2Cell {} level={}>", $self_.cell.token(), $self_.cell.level())
    };
    (@repr geohash, $self_:ident) => {
        format!(
            "<GeohashCell {} precision={}>",
            $self_.cell.token(),
            $self_.cell.precision
        )
    };
    (@repr tile, $self_:ident) => {
        format!(
            "<Tile z={} x={} y={}>",
            $self_.cell.z, $self_.cell.x, $self_.cell.y
        )
    };
    (@match_arg $match_arg:literal) => {
        $match_arg
    };
    (@match_arg) => {
        "id"
    };
}

macro_rules! grid_coverage_common_pymethods {
    (
        impl $py_type:ident {
            this: $this:ident,
            kind: $kind:expr,
            iter: $iter_type:ident,
            cell_array: $cell_array:path,
            parse_cell: $parse_cell:path,
            parsed_key: |$parsed:ident| $parsed_key:expr,
            interior_doc: $interior_doc:literal,
            interior_cells: { $($interior_cells:tt)* },
            boundary_doc: $boundary_doc:literal,
            boundary_cells: { $($boundary_cells:tt)* },
            depth_fields: [$($depth_field:ident),+ $(,)?],
            hash_depth: ($($hash_depth:tt)*),
            cell_hash_key: |$hash_item:ident| { $($cell_hash_key:tt)* },
            explain_grid: $explain_grid:literal,
            explain_depth: { $($explain_depth:tt)* },
            explain_cells: $explain_cells:literal,
            explain_interior_len: { $($explain_interior_len:tt)* },
            explain_outer_len: { $($explain_outer_len:tt)* },
            to_polygon_doc: $to_polygon_doc:literal,
            to_polygon: { $($to_polygon:tt)* },
            reduce_unpickle: $reduce_unpickle:literal,
            reduce_args: { $($reduce_args:tt)* },
            repr: { $($repr:tt)* },
            index_error: $index_error:literal $(,)?
        }
    ) => {
        impl crate::HeapSize for $py_type {
            fn heap_bytes(&self) -> usize {
                self.retained_heap_bytes()
            }
        }

        #[pyo3::pymethods]
        impl $py_type {
            // NEP 13: opt out of numpy ufunc dispatch. A coverage is a spatial
            // object, not an array of numeric ids to broadcast over.
            #[classattr]
            #[expect(non_upper_case_globals, reason = "Python dunder name")]
            const __array_ufunc__: Option<pyo3::Py<pyo3::PyAny>> = None;

            /// ``case Coverage(cells)`` destructures the materialized cell list.
            #[classattr]
            const fn __match_args__() -> (&'static str,) {
                ("cells",)
            }

            /// The rule that materialized ``cells`` (``'center'``, ``'within'``,
            /// ``'overlap'``, or ``'bbox'``). It shapes only the visible cell set; the
            /// exact membership predicates never depend on it.
            ///
            /// Returns
            /// -------
            /// str
            ///     The ``cell_rule`` token the covering was built with.
            #[getter]
            fn cell_rule(&self) -> &str {
                self.cell_rule.token()
            }

            #[doc = $interior_doc]
            #[getter]
            fn interior_cells(&self) -> crate::py::cells::PyCellArray {
                let $this = self;
                $($interior_cells)*
            }

            #[doc = $boundary_doc]
            #[getter]
            fn boundary_cells(&self) -> crate::py::cells::PyCellArray {
                let $this = self;
                $($boundary_cells)*
            }

            /// The cells that make up the covering.
            ///
            /// Returns
            /// -------
            /// CellArray
            #[getter]
            fn cells(&self) -> crate::py::cells::PyCellArray {
                $cell_array(&self.cells)
            }

            /// Logical cell-id payload in bytes for the visible cell set.
            ///
            /// Returns
            /// -------
            /// int
            #[getter]
            #[expect(
                clippy::allow_attributes,
                reason = "the scoped allow below is required by mixed direct/wrapper coverage storage"
            )]
            #[allow(
                clippy::missing_const_for_fn,
                reason = "rectangular coverage wrappers reach cells through a non-const Deref"
            )]
            fn nbytes(&self) -> usize {
                self.cells.len() * std::mem::size_of::<u64>()
            }

            /// `sys.getsizeof` support: the wrapper plus visible cell ids,
            /// partition data, and the retained source geometry.
            fn __sizeof__(&self) -> usize {
                crate::HeapSize::total_size(self)
            }

            /// Exact, boundary-inclusive membership of candidates in the covered area.
            ///
            /// Always answers against the source geometry — never the cells — so the
            /// result is exact regardless of cell_rule.
            ///
            /// Parameters
            /// ----------
            /// geom : Geometry or GeometryArray
            ///     Candidate geometry (or array). Follows the grid input policy:
            ///     WGS84 and CRS-free lon/lat pass through, any other CRS is
            ///     reprojected.
            ///
            /// Returns
            /// -------
            /// bool or numpy.ndarray
            ///     One result per input geometry.
            fn covers(
                &self,
                geom: &pyo3::Bound<'_, pyo3::PyAny>,
            ) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                crate::py::cells::coverage_members(
                    geom,
                    |candidate| {
                        crate::py::cells::coverage_ops::coverage_member(
                            &self.geometry,
                            candidate,
                            crate::py::cells::CoveragePredicate::Covers,
                        )
                    },
                    |point| {
                        crate::py::cells::coverage_ops::coverage_member_point(
                            &self.geometry,
                            point,
                            crate::py::cells::CoveragePredicate::Covers,
                        )
                    },
                )
            }

            /// Exact, strict-interior membership of candidates in the covered area.
            ///
            /// Like covers but boundary-exclusive: a point on the source
            /// geometry's boundary is False.
            ///
            /// Parameters
            /// ----------
            /// geom : Geometry or GeometryArray
            ///     Candidate geometry (or array). Follows the grid input policy:
            ///     WGS84 and CRS-free lon/lat pass through, any other CRS is
            ///     reprojected.
            ///
            /// Returns
            /// -------
            /// bool or numpy.ndarray
            ///     One result per input geometry.
            fn contains(
                &self,
                geom: &pyo3::Bound<'_, pyo3::PyAny>,
            ) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                crate::py::cells::coverage_members(
                    geom,
                    |candidate| {
                        crate::py::cells::coverage_ops::coverage_member(
                            &self.geometry,
                            candidate,
                            crate::py::cells::CoveragePredicate::Contains,
                        )
                    },
                    |point| {
                        crate::py::cells::coverage_ops::coverage_member_point(
                            &self.geometry,
                            point,
                            crate::py::cells::CoveragePredicate::Contains,
                        )
                    },
                )
            }

            /// Exact intersection test of candidates against the covered area.
            ///
            /// For points this matches covers; for lines and polygons it is true
            /// when the candidate shares any point with the source geometry.
            ///
            /// Parameters
            /// ----------
            /// geom : Geometry or GeometryArray
            ///     Candidate geometry (or array). Follows the grid input policy:
            ///     WGS84 and CRS-free lon/lat pass through, any other CRS is
            ///     reprojected.
            ///
            /// Returns
            /// -------
            /// bool or numpy.ndarray
            ///     One result per input geometry.
            fn intersects(
                &self,
                geom: &pyo3::Bound<'_, pyo3::PyAny>,
            ) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                crate::py::cells::coverage_members(
                    geom,
                    |candidate| {
                        crate::py::cells::coverage_ops::coverage_member(
                            &self.geometry,
                            candidate,
                            crate::py::cells::CoveragePredicate::Intersects,
                        )
                    },
                    |point| {
                        crate::py::cells::coverage_ops::coverage_member_point(
                            &self.geometry,
                            point,
                            crate::py::cells::CoveragePredicate::Intersects,
                        )
                    },
                )
            }

            /// Exact, strict contains test for raw lon/lat coordinates.
            ///
            /// Answers exactly against the source geometry, independent of
            /// cell_rule.
            ///
            /// Parameters
            /// ----------
            /// x, y : float or sequence of float
            ///     Longitude and latitude in degrees.
            ///
            /// Returns
            /// -------
            /// bool or numpy.ndarray
            ///     A single bool for scalar ``x, y``, or one result per coordinate.
            fn contains_xy(
                &self,
                py: pyo3::Python<'_>,
                x: &pyo3::Bound<'_, pyo3::PyAny>,
                y: &pyo3::Bound<'_, pyo3::PyAny>,
            ) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                crate::py::cells::coverage_ops::coverage_members_xy(
                    py,
                    &self.geometry,
                    x,
                    y,
                    crate::py::cells::CoveragePredicate::Contains,
                )
            }

            /// Exact, boundary-inclusive membership test for raw lon/lat coordinates.
            ///
            /// Parameters
            /// ----------
            /// x, y : float or sequence of float
            ///     Longitude and latitude in degrees.
            ///
            /// Returns
            /// -------
            /// bool or numpy.ndarray
            ///     A single bool for scalar ``x, y``, or one result per coordinate.
            fn intersects_xy(
                &self,
                py: pyo3::Python<'_>,
                x: &pyo3::Bound<'_, pyo3::PyAny>,
                y: &pyo3::Bound<'_, pyo3::PyAny>,
            ) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                crate::py::cells::coverage_ops::coverage_members_xy(
                    py,
                    &self.geometry,
                    x,
                    y,
                    crate::py::cells::CoveragePredicate::Intersects,
                )
            }

            /// Describe the membership plan.
            ///
            /// Returns
            /// -------
            /// list of str
            ///     One line per plan step.
            fn explain(&self) -> Vec<String> {
                let $this = self;
                crate::py::cells::coverage_explain(
                    $explain_grid,
                    &$($explain_depth)*,
                    self.cell_rule,
                    self.cells.len(),
                    $explain_cells,
                    $($explain_interior_len)*,
                    $($explain_outer_len)*,
                )
            }

            #[doc = $to_polygon_doc]
            fn to_polygon(&self) -> pyo3::PyResult<crate::Typed> {
                let $this = self;
                $($to_polygon)*
            }

            /// Equal when source geometry, rule, depth fields, and visible cells match.
            fn __eq__(&self, other: &Self) -> bool {
                crate::py::cells::coverage_geometry_eq(&self.geometry, &other.geometry)
                    && self.cell_rule == other.cell_rule
                    $(
                        && self.$depth_field == other.$depth_field
                    )+
                    && self.cells.len() == other.cells.len()
                    && self
                        .cells
                        .iter()
                        .zip(other.cells.iter())
                        .all(|(left, right)| left.cell == right.cell)
            }

            /// Hash consistent with __eq__.
            fn __hash__(&self) -> u64 {
                let $this = self;
                crate::collections::python_hash(&(
                    crate::py::cells::coverage_geometry_hash(&self.geometry),
                    self.cell_rule.token(),
                    $($hash_depth)*
                    self.cells
                        .iter()
                        .map(|$hash_item| $($cell_hash_key)*)
                        .collect::<Vec<_>>(),
                ))
            }

            /// Pickle support: round-trip through the source geometry, visible
            /// cell ids, rule, depth fields, and factory max_cells budget.
            fn __reduce__(
                &self,
                py: pyo3::Python<'_>,
            ) -> pyo3::PyResult<(pyo3::Py<pyo3::PyAny>, pyo3::Py<pyo3::PyAny>)> {
                let $this = self;
                Ok((
                    crate::gometry_lib_module(py)?
                        .getattr($reduce_unpickle)?
                        .unbind(),
                    ($($reduce_args)*).into_py_any(py)?,
                ))
            }

            fn __repr__(&self) -> String {
                let $this = self;
                $($repr)*
            }

            #[expect(
                clippy::allow_attributes,
                reason = "the scoped allow below is required by mixed direct/wrapper coverage storage"
            )]
            #[allow(
                clippy::missing_const_for_fn,
                reason = "rectangular coverage wrappers reach cells through a non-const Deref"
            )]
            /// Number of visible cells in the coverage.
            ///
            /// Returns
            /// -------
            /// int
            fn __len__(&self) -> usize {
                self.cells.len()
            }

            #[expect(
                clippy::allow_attributes,
                reason = "the scoped allow below is required by mixed direct/wrapper coverage storage"
            )]
            #[allow(
                clippy::missing_const_for_fn,
                reason = "rectangular coverage wrappers reach cells through a non-const Deref"
            )]
            /// ``False`` only when the coverage has no visible cells.
            ///
            /// Returns
            /// -------
            /// bool
            fn __bool__(&self) -> bool {
                !self.cells.is_empty()
            }

            /// Select visible cells by integer or slice.
            ///
            /// An ``int`` returns one cell. A ``slice`` returns a ``CellArray``
            /// of those cells (not a sliced coverage — membership still
            /// answers against the full source geometry via the coverage).
            ///
            /// Returns
            /// -------
            /// Cell or CellArray
            fn __getitem__(
                &self,
                py: pyo3::Python<'_>,
                index: &pyo3::Bound<'_, pyo3::PyAny>,
            ) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                crate::py::cells::coverage_getitem(
                    py,
                    &self.cells,
                    index,
                    $index_error,
                    $kind,
                )
            }

            /// Iterate visible cells in coverage order.
            ///
            /// Returns
            /// -------
            /// iterator of Cell
            fn __iter__(&self) -> $iter_type {
                $iter_type::new(&self.cells, false)
            }

            /// Iterate visible cells in reverse order.
            ///
            /// Returns
            /// -------
            /// iterator of Cell
            fn __reversed__(&self) -> $iter_type {
                $iter_type::new(&self.cells, true)
            }

            /// Whether a cell is among the visible coverage cells.
            ///
            /// Returns
            /// -------
            /// bool
            fn __contains__(&self, cell: &pyo3::Bound<'_, pyo3::PyAny>) -> bool {
                // Membership is a predicate: an unparseable / wrong-type operand
                // is simply not a member (never a raise), matching CellArray /
// GeometryArray / Groups in.
                let Ok($parsed) = $parse_cell(cell) else {
                    return false;
                };
                self.cells.contains_id($parsed_key)
            }

            /// First index of an equal cell in `[start, stop)`.
            ///
            /// Parameters
            /// ----------
            /// value : object
            ///     The cell value to locate.
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
            ///     If no cell in the window equals ``value``.
            #[pyo3(signature = (value, start = 0, stop = None), text_signature = "($self, value, start=0, stop=None)")]
            fn index(
                &self,
                value: &pyo3::Bound<'_, pyo3::PyAny>,
                start: i64,
                stop: Option<i64>,
            ) -> pyo3::PyResult<usize> {
                let parsed = $parse_cell(value).ok();
                let len = self.cells.len();
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
                if let Some($parsed) = parsed
                    && start < stop
                    && let Some(row) = self.cells.logical_index($parsed_key)
                    && (start..stop).contains(&row)
                {
                    return Ok(row);
                }
                let class_name = <Self as pyo3::PyTypeInfo>::type_object(value.py()).name()?;
                let value = value
                    .repr()
                    .and_then(|repr| repr.extract::<String>())
                    .unwrap_or_else(|_| "value".to_owned());
                Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "{value} is not in {class_name}"
                )))
            }

            /// Number of cells equal to ``value``.
            ///
            /// Parameters
            /// ----------
            /// value : object
            ///     The cell value to count.
            ///
            /// Returns
            /// -------
            /// int
            fn count(&self, value: &pyo3::Bound<'_, pyo3::PyAny>) -> usize {
                let Ok($parsed) = $parse_cell(value) else {
                    return 0;
                };
                usize::from(self.cells.contains_id($parsed_key))
            }

            /// `copy.copy` returns the coverage itself — it is an immutable value.
            fn __copy__(slf: &pyo3::Bound<'_, Self>) -> pyo3::Py<Self> {
                slf.clone().unbind()
            }

            /// `copy.deepcopy` returns the coverage itself: every field is immutable.
            #[pyo3(signature = (memo))]
            fn __deepcopy__(
                slf: &pyo3::Bound<'_, Self>,
                memo: &pyo3::Bound<'_, pyo3::PyAny>,
            ) -> pyo3::Py<Self> {
                let _ = memo;
                slf.clone().unbind()
            }
        }
    };
}

macro_rules! coverage_iter_pyclass {
    (iter: $iter:ident, cell: $cell:ty, name: $name:literal) => {
        /// Lazy iterator over a coverage's cells, yielding one cell per step.
        #[pyclass(name = $name, module = "gometry", frozen)]
        pub(super) struct $iter {
            source: crate::py::cells::coverage_ops::CoverageIterCells<$cell>,
            state: crate::py::row::RowIterState,
        }

        impl $iter {
            pub(super) fn new(
                cells: &crate::py::cells::coverage_ops::CoverageCells<$cell>,
                reverse: bool,
            ) -> Self {
                Self {
                    source: crate::py::cells::coverage_ops::CoverageIterCells::new(cells),
                    state: crate::py::row::RowIterState::new(reverse),
                }
            }
        }

        row_iter_pymethods! {
            impl $iter {
                source: crate::py::cells::coverage_ops::CoverageIterCells<$cell>,
            }
        }

        #[pymethods]
        impl $iter {
            fn __reversed__(&self) -> Self {
                Self {
                    source: self.source.clone(),
                    state: crate::py::row::RowIterState::new(!self.state.is_reverse()),
                }
            }
        }
    };
}

macro_rules! grid_hierarchical_coverage_common_pymethods {
    (
        impl $py_type:ident {
            compact_doc: $compact_doc:literal,
            compact_param: $compact_param:ident,
            compact_default: $compact_default:literal,
            compact_text_signature: $compact_text_signature:literal,
            uncompact_doc: $uncompact_doc:literal,
            uncompact_param: $uncompact_param:ident,
            uncompact_text_signature: $uncompact_text_signature:literal,
            with_parents_doc: $with_parents_doc:literal,
            with_parents_param: $with_parents_param:ident,
            with_parents_default: $with_parents_default:literal,
            with_parents_text_signature: $with_parents_text_signature:literal $(,)?
        }
    ) => {
        #[pyo3::pymethods]
        impl $py_type {
            #[doc = $compact_doc]
            #[pyo3(signature = (*, $compact_param = $compact_default), text_signature = $compact_text_signature)]
            fn compact(&self, $compact_param: i64) -> pyo3::PyResult<Self> {
                crate::py::cells::coverage_ops::hierarchical_coverage_compact(
                    self,
                    $compact_param,
                )
            }

            #[doc = $uncompact_doc]
            #[pyo3(signature = ($uncompact_param), text_signature = $uncompact_text_signature)]
            fn uncompact(
                &self,
                $uncompact_param: &pyo3::Bound<'_, pyo3::PyAny>,
            ) -> pyo3::PyResult<Self> {
                crate::py::cells::coverage_ops::hierarchical_coverage_uncompact(
                    self,
                    $uncompact_param,
                )
            }

            #[doc = $with_parents_doc]
            #[pyo3(signature = (*, $with_parents_param = $with_parents_default), text_signature = $with_parents_text_signature)]
            fn with_parents(&self, $with_parents_param: i64) -> pyo3::PyResult<Self> {
                crate::py::cells::coverage_ops::hierarchical_coverage_with_parents(
                    self,
                    $with_parents_param,
                )
            }
        }
    };
}

macro_rules! grid_rect_coverage_common_pymethods {
    (
        impl $py_type:ident {
            cell: $cell_type:ty,
            kind: $kind:expr,
            kernel: $kernel:ident,
            cell_vec: $cell_vec:path,
            depth: $depth:ident,
            depth_field: $depth_field:ident,
            depth_name: $depth_name:literal,
            min_depth: $min_depth:ident,
            floor_default: $floor_default:literal,
            floor: $floor:path,
            parse_depth: $parse_depth:path,
            compact_doc: $compact_doc:literal,
            compact_text_signature: $compact_text_signature:literal,
            uncompact_doc: $uncompact_doc:literal,
            uncompact_text_signature: $uncompact_text_signature:literal,
            with_parents_doc: $with_parents_doc:literal,
            with_parents_text_signature: $with_parents_text_signature:literal,
            fine_token: |$fine_cell:ident| { $($fine_token:tt)* } $(,)?
        }
    ) => {
        #[pyo3::pymethods]
        impl $py_type {
            #[doc = $compact_doc]
            #[pyo3(signature = (*, $min_depth = $floor_default), text_signature = $compact_text_signature)]
            fn compact(&self, $min_depth: i64) -> pyo3::PyResult<Self> {
                let floor = $floor($min_depth)?;
                // Decorative with_parents ancestors must not participate in
                // compact (N7) — only the leaf frontier covers the factory set.
                let cells: Vec<$cell_type> = self.cells.iter().map(|cell| cell.cell).collect();
                let cells = crate::py::cells::coverage_ops::coverage_frontier(&cells);
                Ok(self.with_cells($cell_vec($kernel::compact_with_floor(cells, floor))))
            }

            #[doc = $uncompact_doc]
            #[pyo3(signature = ($depth), text_signature = $uncompact_text_signature)]
            fn uncompact(
                &self,
                $depth: &pyo3::Bound<'_, pyo3::PyAny>,
            ) -> pyo3::PyResult<Self> {
                let $depth = $parse_depth($depth)?;
                if let Some($fine_cell) = self
                    .cells
                    .iter()
                    .find(|cell| cell.cell.$depth_field > $depth)
                {
                    return Err(crate::py::cells::uncompact_floor_error(
                        $kind,
                        $depth_name,
                        $($fine_token)*
                    ));
                }
                // Explicit coverage transform — no cell budget re-cap.
                // Expand only the leaf frontier so decorative parents cannot
                // invent sibling branches outside the factory covering (N7).
                let cells: Vec<$cell_type> = self.cells.iter().map(|cell| cell.cell).collect();
                let cells = crate::py::cells::coverage_ops::coverage_frontier(&cells);
                Ok(self.with_cells_depth(
                    $cell_vec($kernel::uncompact_unlimited(&cells, $depth)),
                    crate::grid::cell::CellDepth::Uniform($depth),
                ))
            }

            #[doc = $with_parents_doc]
            #[pyo3(signature = (*, $min_depth = $floor_default), text_signature = $with_parents_text_signature)]
            fn with_parents(&self, $min_depth: i64) -> pyo3::PyResult<Self> {
                let floor = $floor($min_depth)?;
                let mut cells: Vec<$cell_type> = Vec::new();
                for cell in self.cells.iter() {
                    cells.push(cell.cell);
                    for depth in floor..cell.cell.$depth_field {
                        cells.push(cell.cell.parent_at(depth));
                    }
                }
                Ok(self.with_cells($cell_vec(cells)))
            }
        }
    };
}

macro_rules! rect_coverage_pyclass {
    (
        spec: $spec:ident,
        coverage: $coverage:ident,
        cell: $cell:ty,
        kernel_cell: $kernel_cell:ty,
        kind: $kind:expr,
        roots: $roots:block,
        level: |$level_cell:ident| $level_expr:expr,
        parse_depth: $parse_depth:path,
        label: $label:literal,
        class_name: $class_name:literal,
        class_doc: $class_doc:literal,
        iter: $iter:ident,
        iter_name: $iter_name:literal,
        depth_getter: $depth_getter:ident,
        depth_doc: $depth_doc:literal $(,)?
    ) => {
        struct $spec;

        impl crate::py::cells::coverage_ops::RectCoverSpec for $spec {
            type Cell = $kernel_cell;

            const KIND: crate::py::cells::GridKind = $kind;

            fn roots() -> Vec<Self::Cell> {
                $roots
            }

            fn level_of(cell: &Self::Cell) -> u8 {
                let $level_cell = cell;
                $level_expr
            }

            fn parse_depth(value: &pyo3::Bound<'_, pyo3::types::PyAny>) -> pyo3::PyResult<u8> {
                $parse_depth(value)
            }

            fn coverage_label() -> &'static str {
                $label
            }
        }

        impl crate::py::cells::coverage_ops::RectCoverageCell for $cell {
            type Cell = $kernel_cell;

            fn from_rect_cell(cell: Self::Cell) -> Self {
                Self { cell }
            }

            fn rect_cell(self) -> Self::Cell {
                self.cell
            }

            fn level(self) -> u8 {
                <$spec as crate::py::cells::coverage_ops::RectCoverSpec>::level_of(&self.cell)
            }
        }

        #[doc = $class_doc]
        #[pyclass(name = $class_name, module = "gometry", frozen, sequence, skip_from_py_object)]
        #[derive(Clone, Debug)]
        pub(super) struct $coverage(crate::py::cells::coverage_ops::RectCoverageState<$cell>);

        impl std::ops::Deref for $coverage {
            type Target = crate::py::cells::coverage_ops::RectCoverageState<$cell>;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl $coverage {
            fn with_cells(&self, cells: Vec<$cell>) -> Self {
                Self(self.0.with_cells(cells))
            }

            fn with_cells_depth(
                &self,
                cells: Vec<$cell>,
                fallback: crate::grid::cell::CellDepth,
            ) -> Self {
                Self(self.0.with_cells_depth(cells, fallback))
            }
        }

        coverage_iter_pyclass! { iter: $iter, cell: $cell, name: $iter_name }

        #[pyo3::pymethods]
        impl $coverage {
            #[doc = $depth_doc]
            #[getter]
            fn $depth_getter(&self) -> Option<u8> {
                self.depth.uniform_level()
            }
        }
    };
}

macro_rules! grid_free_functions {
    (
        @set_algebra {
            cell_set_arg: $cell_set_arg:ident,
            cell_type: $cell_type:ty,
            label: $label:literal,
            cell_doc: $cell_doc:literal,
            item_doc: $item_doc:literal,
            contract_doc: $contract_doc:literal,
            parse_cell: $parse_cell:expr,
            array: |$array_cells:ident| $array_expr:expr,
            union: $union:ident,
            intersection: $intersection:ident,
            difference: $difference:ident,
            example_union: $example_union:literal,
            example_intersection: $example_intersection:literal,
            example_difference: $example_difference:literal $(,)?
        }
    ) => {
        #[doc = concat!(
                            "Collect and normalize one or an iterable of ",
                            $label,
                            " cells (sorted, deduplicated, sibling-merged) for the set algebra."
                        )]
        pub(super) fn $cell_set_arg(
            cells: &pyo3::Bound<'_, pyo3::PyAny>,
        ) -> pyo3::PyResult<Vec<$cell_type>> {
            let items = crate::py::cells::cell_items(cells)?;
            let ids = items
                .iter()
                .map(|cell| $parse_cell(cell))
                .collect::<pyo3::PyResult<Vec<_>>>()?;
            Ok(crate::grid::cell_set::normalize(ids))
        }

        #[doc = concat!("Hierarchy-aware union of two ", $label, " cell sets.")]
        ///
        /// Returns the normalized cell union: sorted, with contained cells absorbed
        /// by their ancestors and complete sibling groups merged into parents.
        #[doc = $contract_doc]
        ///
        /// Parameters
        /// ----------
        #[doc = concat!("left, right : ", $item_doc)]
        ///     A single cell identity or a collection on either side (any
        ///     accepted mix of cell objects and identity values for this grid).
        ///
        /// Returns
        /// -------
        #[doc = concat!("CellArray of ", $cell_doc)]
        ///
        /// Raises
        /// ------
        /// ParseError
        #[doc = concat!("    If a cell input is not valid for the ", $label, " grid.")]
        ///
        /// Examples
        /// --------
        #[doc = $example_union]
        #[pyo3::pyfunction]
        pub(super) fn $union(
            left: &pyo3::Bound<'_, pyo3::PyAny>,
            right: &pyo3::Bound<'_, pyo3::PyAny>,
        ) -> pyo3::PyResult<crate::py::cells::PyCellArray> {
            let left = $cell_set_arg(left)?;
            let right = $cell_set_arg(right)?;
            let $array_cells = crate::grid::cell_set::union(left, right);
            Ok($array_expr)
        }

        #[doc = concat!("Hierarchy-aware intersection of two ", $label, " cell sets.")]
        ///
        /// A cell survives where the two sets overlap; ancestor/descendant overlap
        /// keeps the finer cell.
        #[doc = $contract_doc]
        ///
        /// Parameters
        /// ----------
        #[doc = concat!("left, right : ", $item_doc)]
        ///     The two cell sets (any mix of cell objects, ids, or tokens).
        ///
        /// Returns
        /// -------
        #[doc = concat!("CellArray of ", $cell_doc)]
        ///
        /// Raises
        /// ------
        /// ParseError
        #[doc = concat!("    If an id or token is not a valid ", $label, " cell.")]
        ///
        /// Examples
        /// --------
        #[doc = $example_intersection]
        #[pyo3::pyfunction]
        pub(super) fn $intersection(
            left: &pyo3::Bound<'_, pyo3::PyAny>,
            right: &pyo3::Bound<'_, pyo3::PyAny>,
        ) -> pyo3::PyResult<crate::py::cells::PyCellArray> {
            let left = $cell_set_arg(left)?;
            let right = $cell_set_arg(right)?;
            let $array_cells = crate::grid::cell_set::intersection(&left, &right);
            Ok($array_expr)
        }

        #[doc = concat!("Hierarchy-aware difference of two ", $label, " cell sets.")]
        ///
        /// Cells of ``left`` partially covered by ``right`` split into children
        /// until the remainder is exact; the result is normalized.
        #[doc = $contract_doc]
        ///
        /// Parameters
        /// ----------
        #[doc = concat!("left, right : ", $item_doc)]
        ///     The two cell sets (any mix of cell objects, ids, or tokens).
        ///
        /// Returns
        /// -------
        #[doc = concat!("CellArray of ", $cell_doc)]
        ///
        /// Raises
        /// ------
        /// ParseError
        #[doc = concat!("    If an id or token is not a valid ", $label, " cell.")]
        ///
        /// Examples
        /// --------
        #[doc = $example_difference]
        #[pyo3::pyfunction]
        pub(super) fn $difference(
            left: &pyo3::Bound<'_, pyo3::PyAny>,
            right: &pyo3::Bound<'_, pyo3::PyAny>,
        ) -> pyo3::PyResult<crate::py::cells::PyCellArray> {
            let left = $cell_set_arg(left)?;
            let right = $cell_set_arg(right)?;
            let $array_cells = crate::grid::cell_set::difference(&left, &right)
                .map_err(crate::py::cells::cell_limit_err)?;
            Ok($array_expr)
        }
    };

    (
        @to_polygon {
            function: $function:ident,
            argument: $argument:ident,
            label: $label:literal,
            item_doc: $item_doc:literal,
            depth_plural: $depth_plural:literal,
            parse_cell: $parse_cell:expr,
            dissolve: |$dissolve_cells:ident| $dissolve_expr:expr $(,)?
        }
    ) => {
        #[doc = concat!("Dissolve ", $label, " cells into one outline geometry.")]
        ///
        /// Shared cell edges are removed, so the result is the cells' combined region
        /// as a single geometry, not one polygon per cell. Mixed
        #[doc = concat!($depth_plural, " are allowed, so compacted sets dissolve directly.")]
        ///
        /// Parameters
        /// ----------
        #[doc = concat!(stringify!($argument), " : ", $item_doc)]
        ///     The cells to dissolve (any mix of objects, ids, or tokens).
        ///
        /// Returns
        /// -------
        /// Polygon or MultiPolygon
        ///     The dissolved region, tagged ``EPSG:4326``.
        ///
        /// Raises
        /// ------
        /// GeometryError
        ///     If ``cells`` is empty.
        /// ParseError
        #[doc = concat!("    If an id or token is not a valid ", $label, " cell.")]
        ///
        /// Examples
        /// --------
        /// Dissolve a detached cell list (when you have a coverage, use
        /// ``coverage.to_polygon()`` directly).
        #[pyo3::pyfunction]
        pub(super) fn $function(
            $argument: &pyo3::Bound<'_, pyo3::PyAny>,
        ) -> pyo3::PyResult<crate::Typed> {
            let items = crate::py::cells::cell_items($argument)?;
            let $dissolve_cells = items
                .iter()
                .map(|cell| $parse_cell(cell))
                .collect::<pyo3::PyResult<Vec<_>>>()?;
            $dissolve_expr
        }
    };


    (
        @parent {
            function: $function:ident,
            label: $label:literal,
            cell_doc: $cell_doc:literal,
            item_doc: $item_doc:literal,
            depth: $depth:ident,
            depth_name: $depth_name:literal,
            text_signature: $text_signature:literal,
            parse_cell: $parse_cell:expr,
            parse_depth: $parse_depth:path,
            array: |$array_cells:ident| $array_expr:expr $(,)?
        }
    ) => {
        /// Parent cell of every input cell — a row-aligned mapping, one output per
        /// input (duplicates preserved), for roll-up/group-by-parent workflows.
        ///
        /// Parameters
        /// ----------
        #[doc = concat!("cells : ", $item_doc)]
        ///     Cells to map (any mix of objects, ids, or tokens).
        #[doc = concat!(stringify!($depth), " : int, optional")]
        #[doc = concat!("    Target ", $depth_name, "; must not be finer than any input cell.")]
        #[doc = concat!("    Defaults to one coarser than each cell's own ", $depth_name, ".")]
        ///
        /// Returns
        /// -------
        #[doc = concat!("CellArray of ", $cell_doc)]
        ///     The parent of each input cell, in input order.
        ///
        /// Raises
        /// ------
        /// GeometryError
        #[doc = concat!(
                            "    If ",
                            $depth_name,
                            " is out of range, finer than an input cell, or a minimum-",
                            $depth_name,
                            " cell has no parent."
                        )]
        /// ParseError
        #[doc = concat!("    If an id or token is not a valid ", $label, " cell.")]
        #[pyo3::pyfunction]
        #[pyo3(signature = (cells, $depth = None), text_signature = $text_signature)]
        pub(super) fn $function(
            cells: &pyo3::Bound<'_, pyo3::PyAny>,
            $depth: Option<&pyo3::Bound<'_, pyo3::PyAny>>,
        ) -> pyo3::PyResult<crate::py::cells::PyCellArray> {
            let items = crate::py::cells::cell_items(cells)?;
            let $array_cells = items
                .iter()
                .map(|cell| {
                    crate::py::cells::cell_ops::cell_parent(
                        $parse_cell(cell)?,
                        $depth,
                        $parse_depth,
                    )
                })
                .collect::<pyo3::PyResult<Vec<_>>>()?;
            Ok($array_expr)
        }
    };
}
