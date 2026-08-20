//! Shared `macro_rules!` helpers for grid cell PyO3 boilerplate.

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
            descendant_count_doc: $descendant_count_doc:literal,
            parse_error_doc: $parse_error_doc:literal,
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

            /// Pickle support: a cell is its identity value.
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
            /// H3 and S2 emit **planar chord proxies** for the true spherical
            /// cell boundary (great-circle / geodesic edges). Prefer
            /// cell-algebra methods (``contains``, ``parent``, set ops,
            /// ``grid_disk``) for exact hierarchical work — do not treat this
            /// polygon as a densified spherical boundary. Geohash and tile
            /// cells are exact lon/lat rectangles (no chord proxy).
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
            #[doc = concat!("    ", $descendant_count_doc)]
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
            #[doc = concat!("    ", $parse_error_doc)]
            /// TypeError
            #[doc = grid_cell_common_pymethods!(@type_error $repr)]
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
            #[doc = concat!("    ", $parse_error_doc)]
            /// TypeError
            #[doc = grid_cell_common_pymethods!(@type_error $repr)]
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
    (@type_error geohash) => {
        "    If ``other`` is not a valid cell object or token."
    };
    (@type_error $repr:ident) => {
        "    If ``other`` is not a valid cell object, id, or token."
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
            parse_error_doc: $parse_error_doc:literal,
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
        #[doc = concat!("    ", $parse_error_doc)]
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
        #[doc = grid_free_functions!(@set_input_doc $cell_set_arg)]
        ///
        /// Returns
        /// -------
        #[doc = concat!("CellArray of ", $cell_doc)]
        ///
        /// Raises
        /// ------
        /// ParseError
        #[doc = concat!("    ", $parse_error_doc)]
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
        #[doc = grid_free_functions!(@set_input_doc $cell_set_arg)]
        ///
        /// Returns
        /// -------
        #[doc = concat!("CellArray of ", $cell_doc)]
        ///
        /// Raises
        /// ------
        /// ParseError
        #[doc = concat!("    ", $parse_error_doc)]
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
    (@set_input_doc geohash_cell_set_arg) => {
        "    The two cell sets (any mix of cell objects or tokens)."
    };
    (@set_input_doc $cell_set_arg:ident) => {
        "    The two cell sets (any mix of cell objects, ids, or tokens)."
    };
}
