use super::*;

fn parse_compact_floor(
    depth: Option<&Bound<'_, PyAny>>,
    label: &str,
    parameter: &str,
    min: i64,
    max: i64,
) -> PyResult<u8> {
    let value = match depth {
        Some(value) if !value.is_none() => py_i64_required(label, value)?,
        _ => min,
    };
    checked_depth(value, label, parameter, min, max)
}

pub(super) trait CellArrayGrid: GridCell {
    fn from_validated_id(id: u64) -> Self;
    fn parse_depth(value: &Bound<'_, PyAny>) -> PyResult<u8>;
    fn parse_compact_floor(depth: Option<&Bound<'_, PyAny>>) -> PyResult<u8>;
    fn compact_cells(cells: Vec<Self>, floor: u8) -> PyResult<Vec<Self>> {
        Ok(cell_set::compact_with_floor(cells, floor).into_public_vec())
    }
    fn compact_ids(ids: Vec<u64>, floor: u8) -> PyResult<Vec<u64>> {
        let cells: Vec<_> = ids.into_iter().map(Self::from_validated_id).collect();
        let cells = Self::compact_cells(cells, floor)?;
        Ok(cells.into_iter().map(Self::hash_key).collect())
    }
    fn uncompact_cells(cells: Vec<Self>, depth: u8) -> PyResult<Vec<Self>> {
        Ok(cell_set::uncompact(&cells, depth)?.into_public_vec())
    }
    fn uncompact_floor_token(cell: &Self) -> String;
}

impl CellArrayGrid for h3o::CellIndex {
    fn from_validated_id(id: u64) -> Self {
        Self::try_from(id).expect("ids validated at construction")
    }

    fn parse_depth(value: &Bound<'_, PyAny>) -> PyResult<u8> {
        super::h3::parse_h3_resolution(value).map(Into::into)
    }

    fn parse_compact_floor(depth: Option<&Bound<'_, PyAny>>) -> PyResult<u8> {
        parse_compact_floor(
            depth,
            "H3 min_resolution",
            "min_resolution",
            0,
            i64::from(H3_MAX_RESOLUTION),
        )
    }

    fn uncompact_floor_token(cell: &Self) -> String {
        cell.to_string()
    }
}

impl CellArrayGrid for CellId {
    fn from_validated_id(id: u64) -> Self {
        Self::from_raw(id).expect("ids validated at construction")
    }

    fn parse_depth(value: &Bound<'_, PyAny>) -> PyResult<u8> {
        super::s2::parse_s2_level(value)
    }

    fn parse_compact_floor(depth: Option<&Bound<'_, PyAny>>) -> PyResult<u8> {
        let value = match depth {
            Some(value) if !value.is_none() => py_i64_required("S2 min_level", value)?,
            _ => 0,
        };
        super::s2::parse_s2_min_level_value(value)
    }

    fn uncompact_floor_token(cell: &Self) -> String {
        cell.token()
    }
}

impl CellArrayGrid for Tile {
    fn from_validated_id(id: u64) -> Self {
        Self::from_id(id).expect("ids validated at construction")
    }

    fn parse_depth(value: &Bound<'_, PyAny>) -> PyResult<u8> {
        super::tiles::parse_tile_zoom(value)
    }

    fn parse_compact_floor(depth: Option<&Bound<'_, PyAny>>) -> PyResult<u8> {
        parse_compact_floor(
            depth,
            "tile min_zoom",
            "min_zoom",
            0,
            i64::from(TILE_MAX_ZOOM),
        )
    }

    fn uncompact_floor_token(cell: &Self) -> String {
        cell.quadkey()
    }
}

impl CellArrayGrid for Geohash {
    fn from_validated_id(id: u64) -> Self {
        super::grid_kind::geohash_from_identity_key(id).expect("ids validated at construction")
    }

    fn parse_depth(value: &Bound<'_, PyAny>) -> PyResult<u8> {
        super::geohash::parse_geohash_precision(value)
    }

    fn parse_compact_floor(depth: Option<&Bound<'_, PyAny>>) -> PyResult<u8> {
        parse_compact_floor(
            depth,
            "geohash min_precision",
            "min_precision",
            1,
            i64::from(GEOHASH_MAX_PRECISION),
        )
    }

    fn uncompact_floor_token(cell: &Self) -> String {
        cell.token()
    }
}

impl PyCellArray {
    fn ids_from_cells<G: GridCell>(cells: Vec<G>) -> Vec<u64> {
        cells.into_iter().map(GridCell::hash_key).collect()
    }

    pub(super) fn numpy_array_kind(
        &self,
        py: Python<'_>,
        dtype: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<CellNumpyArrayKind> {
        // Omitted dtype and explicit None both mean the grid default.
        if dtype.is_none_or(pyo3::types::PyAnyMethods::is_none) {
            return Ok(
                if self.kind() == GridKind::GeohashCell
                    || (0..self.len()).any(|row| self.is_row_missing(row))
                {
                    CellNumpyArrayKind::Objects
                } else {
                    CellNumpyArrayKind::Ids {
                        dtype: None,
                        native: true,
                    }
                },
            );
        }
        let dtype = dtype.expect("checked non-None above");
        let numpy = numpy_module(py)?;
        let dtype = numpy.getattr("dtype")?.call1((dtype,))?;
        let kind = dtype.getattr("kind")?.extract::<String>()?;
        let itemsize = dtype.getattr("itemsize")?.extract::<usize>()?;
        if kind == "O" {
            Ok(CellNumpyArrayKind::Objects)
        } else if kind == "u" && itemsize == size_of::<u64>() {
            if self.kind() == GridKind::GeohashCell {
                return Err(GeometryError::new_err(
                    "geohash CellArray has string tokens, not public uint64 ids; use dtype=object or .token",
                ));
            }
            if (0..self.len()).any(|row| self.is_row_missing(row)) {
                return Err(GeometryError::new_err(
                    "masked CellArray cannot be exported as uint64",
                ));
            }
            Ok(CellNumpyArrayKind::Ids {
                native: dtype.getattr("isnative")?.extract::<bool>()?,
                dtype: Some(dtype.unbind()),
            })
        } else {
            Err(GeometryError::new_err(
                "dtype must be uint64, object, or None",
            ))
        }
    }

    pub(super) fn object_numpy_array(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let values = self.to_cell_list(py)?;
        let array =
            numpy::PyArray1::from_owned_object_array(py, numpy::ndarray::Array1::from_vec(values));
        array.try_readwrite()?.make_nonwriteable();
        Ok(array.into_any().unbind())
    }

    fn hierarchy_predicate(
        &self,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
        intersects: bool,
    ) -> PyResult<Py<PyAny>> {
        let kind = self.kind();
        let values = if let Ok(other) = other.cast::<Self>() {
            let other = other.borrow();
            if kind != other.kind() {
                return Err(GeometryError::new_err(format!(
                    "cell types must match, got {} and {}",
                    kind.class_name(),
                    other.kind().class_name()
                )));
            }
            if self.len() != other.len() {
                return Err(GeometryError::new_err(format!(
                    "cell arrays must have equal lengths, got {} and {}",
                    self.len(),
                    other.len()
                )));
            }
            dispatch_cell_grid!(self, G, {
                let mut values = Vec::with_capacity(self.len());
                for row in 0..self.len() {
                    let value = if self.is_row_missing(row) || other.is_row_missing(row) {
                        false
                    } else {
                        let left = G::from_validated_id(self.id_at(row));
                        let right = G::from_validated_id(other.id_at(row));
                        left.contains_cell(right) || (intersects && right.contains_cell(left))
                    };
                    values.push(value);
                }
                values
            })
        } else {
            if let Some(other_kind) = grid_kind_from_value(other) {
                if kind != other_kind {
                    return Err(GeometryError::new_err(format!(
                        "cell types must match, got {} and {}",
                        kind.class_name(),
                        other_kind.class_name()
                    )));
                }
            } else if !is_raw_identity(other, kind) {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "other must be a same-grid cell, integer id, or string token",
                ));
            }
            let other_id = kind.id_from_value(other)?;
            dispatch_cell_grid!(self, G, {
                let right = G::from_validated_id(other_id);
                let mut values = Vec::with_capacity(self.len());
                for row in 0..self.len() {
                    let value = if self.is_row_missing(row) {
                        false
                    } else {
                        let left = G::from_validated_id(self.id_at(row));
                        left.contains_cell(right) || (intersects && right.contains_cell(left))
                    };
                    values.push(value);
                }
                values
            })
        };
        bool_array(py, values)
    }
}

#[pyo3::pymethods]
impl PyCellArray {
    /// Return the child cells of every cell at a finer depth, as ragged rows.
    ///
    /// Parameters
    /// ----------
    /// depth : int, optional
    ///     Target depth; must not be coarser than any input cell. Defaults to
    ///     one finer than each cell's own depth.
    ///
    /// Returns
    /// -------
    /// Groups of CellArray
    ///     One row of children per input cell, in input order. Missing input
    ///     rows yield ``None`` groups.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> children = gm.CellArray([cell]).children(8)[0]
    /// >>> assert children is not None
    /// >>> len(children)
    /// 7
    #[pyo3(signature = (depth = None, /), text_signature = "($self, depth=None, /)")]
    pub(crate) fn children(
        &self,
        py: Python<'_>,
        depth: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Groups> {
        let kind = self.kind();
        let (missing, _) = self.missing_mask_and_present_count();
        let cells = self.clone();
        let depth = dispatch_cell_grid!(self, G, {
            match cells.logical_cells::<G>().next() {
                Some(_) => {
                    let depth = depth
                        .filter(|value| !value.is_none())
                        .map(G::parse_depth)
                        .transpose()?;
                    if let Some(target) = depth
                        && cells.logical_cells::<G>().any(|cell| target < cell.depth())
                    {
                        return Err(GeometryError::new_err(format!(
                            "children {} must be >= cell {}",
                            G::DEPTH_NAME,
                            G::DEPTH_NAME
                        )));
                    }
                    depth
                },
                None => None,
            }
        });
        let (ids, offsets) = py.detach(move || {
            dispatch_cell_grid!(&cells, G, {
                let mut builder = crate::CsrBuilder::<u64>::with_capacity(0, cells.len());
                for row in 0..cells.len() {
                    if cells.is_row_missing(row) {
                        builder.push_row(std::iter::empty::<u64>())?;
                        continue;
                    }
                    let cell = G::from_validated_id(cells.id_at(row));
                    let target = depth.or_else(|| {
                        cell.depth()
                            .checked_add(1)
                            .filter(|target| *target <= G::MAX_DEPTH)
                    });
                    let Some(target) = target else {
                        builder.push_row(std::iter::empty::<u64>())?;
                        continue;
                    };
                    builder.push_row(
                        cell.children_to(target)?
                            .into_iter()
                            .map(GridCell::hash_key),
                    )?;
                }
                Ok::<_, PyErr>(builder.finish())
            })
        })?;
        Groups::from_cell_flat(kind, ids, offsets, missing)
    }

    /// Number of descendant cells each cell has at ``depth``.
    ///
    /// The columnar mirror of the scalar ``children_count`` — the count only,
    /// without materializing the children (which ``children`` does, as ragged
    /// rows). Present-row counts are integer-valued but are represented as
    /// ``float64``; they are exact only when within float64's integer precision.
    /// Missing rows are represented by ``NaN`` to preserve row alignment.
    ///
    /// Parameters
    /// ----------
    /// depth : int, optional
    ///     Target depth (resolution / level / precision / zoom). Omitted means
    ///     one step finer than each cell.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     Read-only ``float64`` ``numpy.ndarray`` of shape ``(n,)``;
    ///     missing rows are ``NaN``.
    ///
    /// Raises
    /// ------
    /// GeometryError
    ///     If ``depth`` is out of range for the grid.
    ///
    /// See Also
    /// --------
    /// children : The descendant cells themselves, as ragged rows.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> neighbor = list(cell.neighbors)[0]
    /// >>> assert neighbor is not None
    /// >>> cells = gm.CellArray([cell, neighbor])
    /// >>> cells.children_count(9).tolist()
    /// [49.0, 49.0]
    #[pyo3(signature = (depth = None, /), text_signature = "($self, depth=None, /)")]
    pub(crate) fn children_count(
        &self,
        py: Python<'_>,
        depth: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let values = dispatch_cell_grid!(self, G, {
            let mut present = self.logical_cells::<G>();
            let first = present.next();
            let target_depth = first
                .as_ref()
                .map(|_| cell_ops::parse_explicit_depth(depth, G::parse_depth))
                .transpose()?;
            let mut present = first.into_iter().chain(present);
            let mut values = Vec::with_capacity(self.len());
            for row in 0..self.len() {
                let value = if self.is_row_missing(row) {
                    f64::NAN
                } else {
                    let cell = present.next().expect("present cell count matches");
                    cell_ops::cell_descendant_count_at_depth(
                        cell,
                        target_depth.expect("a present cell admitted the depth"),
                    )? as f64
                };
                values.push(value);
            }
            Ok::<_, PyErr>(values)
        })?;
        float64_array(py, values)
    }

    /// Compact this cell set to the coarsest exact covering, returned in the
    /// grid's public identity order. Duplicates and descendants are absorbed;
    /// complete sibling groups merge recursively, stopping at ``depth``.
    /// Missing rows are skipped; this is a set operation over present cells.
    ///
    /// Parameters
    /// ----------
    /// depth : int, optional
    ///     Coarsest depth allowed.
    ///
    /// Returns
    /// -------
    /// CellArray
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> neighbor = list(cell.neighbors)[0]
    /// >>> assert neighbor is not None
    /// >>> cells = gm.CellArray([cell, neighbor])
    /// >>> len(cells.compact(5))
    /// 2
    #[pyo3(signature = (depth = None, /), text_signature = "($self, depth=None, /)")]
    pub(crate) fn compact(
        &self,
        py: Python<'_>,
        depth: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let kind = self.kind();
        let cells = self.clone();
        let compacted = dispatch_cell_grid!(self, G, {
            let ids = (0..cells.len())
                .filter(|&logical| !cells.is_row_missing(logical))
                .map(|logical| cells.id_at(logical))
                .collect();
            let floor = G::parse_compact_floor(depth)?;
            py.detach(move || G::compact_ids(ids, floor))?
        });
        Ok(Self::from_ids(kind, compacted))
    }

    /// Test whether every row hierarchically contains the paired cell.
    ///
    /// Parameters
    /// ----------
    /// other : same-grid cell, integer id, string token, or CellArray
    ///     One identity broadcast to every row, or a same-length array.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One read-only boolean per row.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> neighbor = list(cell.neighbors)[0]
    /// >>> assert neighbor is not None
    /// >>> gm.CellArray([cell, neighbor]).contains(cell).tolist()
    /// [True, False]
    fn contains(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.hierarchy_predicate(py, other, false)
    }

    /// Test whether every row hierarchically intersects the paired cell.
    ///
    /// Two cells intersect when either is an ancestor of the other.
    ///
    /// Parameters
    /// ----------
    /// other : same-grid cell, integer id, string token, or CellArray
    ///     One identity broadcast to every row, or a same-length array.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One read-only boolean per row.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> neighbor = list(cell.neighbors)[0]
    /// >>> assert neighbor is not None
    /// >>> gm.CellArray([cell, neighbor]).intersects(cell).tolist()
    /// [True, False]
    fn intersects(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.hierarchy_predicate(py, other, true)
    }

    /// The edge-adjacent cells of every cell, as ragged rows.
    ///
    /// Returns
    /// -------
    /// Groups of CellArray
    ///     One row of neighbors per input cell, in input order. Neighbor
    ///     counts vary, so the result is a Groups, not a rectangular
    ///     CellArray. Missing input rows yield ``None`` groups.
    #[getter]
    pub(crate) fn neighbors(&self, py: Python<'_>) -> PyResult<Groups> {
        let kind = self.kind();
        let (missing, _) = self.missing_mask_and_present_count();
        let cells = self.clone();
        let (ids, offsets) = py.detach(move || {
            dispatch_cell_grid!(&cells, G, {
                let mut builder = crate::CsrBuilder::<u64>::with_capacity(0, cells.len());
                for row in 0..cells.len() {
                    if cells.is_row_missing(row) {
                        builder.push_row(std::iter::empty::<u64>())?;
                        continue;
                    }
                    let cell = G::from_validated_id(cells.id_at(row));
                    builder.push_row(
                        GridCell::neighbors(cell)
                            .into_iter()
                            .map(GridCell::hash_key),
                    )?;
                }
                Ok::<_, PyErr>(builder.finish())
            })
        })?;
        Groups::from_cell_flat(kind, ids, offsets, missing)
    }

    /// Parent cell of every input cell.
    ///
    /// Parameters
    /// ----------
    /// depth : int, optional
    ///     Target depth; defaults to one coarser than each row.
    ///
    /// Returns
    /// -------
    /// CellArray
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> neighbor = list(cell.neighbors)[0]
    /// >>> assert neighbor is not None
    /// >>> cells = gm.CellArray([cell, neighbor])
    /// >>> parent = cells.parent(6)[0]
    /// >>> assert parent is not None
    /// >>> parent.token
    /// '86283082fffffff'
    #[pyo3(signature = (depth = None, /), text_signature = "($self, depth=None, /)")]
    pub(crate) fn parent(&self, depth: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let kind = self.kind();
        let ids = dispatch_cell_grid!(self, G, {
            let mut present = self.logical_cells::<G>();
            let first = present.next();
            let target_depth = first
                .as_ref()
                .map(|_| cell_ops::parse_explicit_depth(depth, G::parse_depth))
                .transpose()?;
            let mut present = first.into_iter().chain(present);
            let mut ids = Vec::with_capacity(self.len());
            for row in 0..self.len() {
                ids.push(if self.is_row_missing(row) {
                    0
                } else {
                    cell_ops::cell_parent_at_depth(
                        present.next().expect("present cell count matches"),
                        target_depth.expect("a present cell admitted the depth"),
                    )?
                    .hash_key()
                });
            }
            Ok::<_, PyErr>(ids)
        })?;
        let (missing, _) = self.missing_mask_and_present_count();
        Ok(Self {
            storage: CellStorage::new(kind, ids, missing),
            selection: RowSelection::Identity,
        })
    }

    /// Expand this cell set to a uniform depth, returned in the grid's public
    /// identity order. Expansion is bounded only by checked size arithmetic
    /// and allocator capacity; it has no fixed cell-count limit.
    /// Missing rows are skipped; this is a set operation over present cells.
    ///
    /// Parameters
    /// ----------
    /// depth : int
    ///     Target depth; no coarser than any row.
    ///
    /// Returns
    /// -------
    /// CellArray
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> len(gm.CellArray([cell]).uncompact(8))
    /// 7
    #[pyo3(signature = (depth, /), text_signature = "($self, depth, /)")]
    pub(crate) fn uncompact(&self, py: Python<'_>, depth: &Bound<'_, PyAny>) -> PyResult<Self> {
        let kind = self.kind();
        let cells = self.clone();
        let expanded = dispatch_cell_grid!(self, G, {
            let depth = G::parse_depth(depth)?;
            if let Some(cell) = cells.logical_cells::<G>().find(|cell| cell.depth() > depth) {
                return Err(uncompact_floor_error(
                    kind,
                    G::DEPTH_NAME,
                    G::uncompact_floor_token(&cell),
                ));
            }
            py.detach(move || {
                let cells = cells.map_logical_cells::<G, _>(|cell| cell);
                Ok::<_, PyErr>(Self::ids_from_cells(G::uncompact_cells(cells, depth)?))
            })?
        });
        Ok(Self::from_ids(kind, expanded))
    }
}
