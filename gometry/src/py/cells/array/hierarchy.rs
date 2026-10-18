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
    fn compact_cells(cells: Vec<Self>, floor: u8) -> Vec<Self> {
        cell_set::compact_with_floor(cells, floor).into_public_vec()
    }
    fn compact_ids(ids: Vec<u64>, floor: u8) -> Vec<u64> {
        let cells: Vec<_> = ids.into_iter().map(Self::from_validated_id).collect();
        Self::compact_cells(cells, floor)
            .into_iter()
            .map(GridCell::hash_key)
            .collect()
    }
    fn uncompact_cells(cells: Vec<Self>, depth: u8) -> crate::error::Result<Vec<Self>> {
        Ok(cell_set::uncompact(&cells, depth)?.into_public_vec())
    }
    fn uncompact_floor_token(cell: &Self) -> String;
}

enum ChildrenAdmission {
    AllMissing(Groups),
    Present(Option<ValidityMask>),
}

enum ParentAdmission {
    AllMissing(PyCellArray),
    Present,
}

fn cell_children_groups<G: CellArrayGrid>(
    cells: &PyCellArray,
    depth: Option<u8>,
    missing: Option<ValidityMask>,
) -> Result<Groups, CellArrayError> {
    if let Some(target) = depth
        && cells.logical_cells::<G>().any(|cell| target < cell.depth())
    {
        return Err(CellArrayError::Geometry(format!(
            "children {} must be >= cell {}",
            G::DEPTH_NAME,
            G::DEPTH_NAME
        )));
    }

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
    let (ids, offsets) = builder.finish();
    Ok(Groups::from_cell_flat(cells.kind(), ids, offsets, missing)?)
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
                if self.kind() == GridKind::GeohashCell || self.has_missing(py) {
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
            if self.has_missing(py) {
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
            let left = self;
            let right = &*other;
            py.detach(|| {
                dispatch_cell_grid!(left, G, {
                    let mut values = Vec::with_capacity(left.len());
                    for row in 0..left.len() {
                        let value = if left.is_row_missing(row) || right.is_row_missing(row) {
                            false
                        } else {
                            let left = G::from_validated_id(left.id_at(row));
                            let right = G::from_validated_id(right.id_at(row));
                            left.contains_cell(right) || (intersects && right.contains_cell(left))
                        };
                        values.push(value);
                    }
                    values
                })
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
            py.detach(|| {
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
        let cells = self.clone();
        let depth = depth.filter(|value| !value.is_none());
        if depth.is_none() {
            return py
                .detach(|| {
                    let (missing, _) = cells.missing_mask_and_present_count();
                    dispatch_cell_grid!(&cells, G, {
                        cell_children_groups::<G>(&cells, None, missing)
                    })
                })
                .map_err(PyErr::from);
        }

        let admission = py.detach(|| {
            dispatch_cell_grid!(&cells, G, {
                let (missing, present_count) = cells.missing_mask_and_present_count();
                if present_count == 0 {
                    Ok::<_, CellArrayError>(ChildrenAdmission::AllMissing(
                        cell_children_groups::<G>(&cells, None, missing)?,
                    ))
                } else {
                    Ok(ChildrenAdmission::Present(missing))
                }
            })
        })?;
        match admission {
            ChildrenAdmission::AllMissing(groups) => Ok(groups),
            ChildrenAdmission::Present(missing) => {
                let depth = dispatch_cell_grid!(&cells, G, {
                    G::parse_depth(depth.expect("an explicit cell depth was admitted"))?
                });
                py.detach(|| {
                    dispatch_cell_grid!(&cells, G, {
                        cell_children_groups::<G>(&cells, Some(depth), missing)
                    })
                })
                .map_err(PyErr::from)
            },
        }
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
        let depth = depth.filter(|value| !value.is_none());
        let target_depth = dispatch_cell_grid!(self, G, {
            match depth {
                Some(depth) if self.has_present(py) => Some(G::parse_depth(depth)?),
                _ => None,
            }
        });
        let values = py.detach(|| {
            dispatch_cell_grid!(self, G, {
                let mut present = self.logical_cells::<G>();
                let mut values = Vec::with_capacity(self.len());
                for row in 0..self.len() {
                    let value = if self.is_row_missing(row) {
                        f64::NAN
                    } else {
                        let cell = present.next().expect("present cell count matches");
                        cell_ops::cell_descendant_count_at_depth(cell, target_depth)
                            .map(|count| count as f64)?
                    };
                    values.push(value);
                }
                Ok::<_, CellArrayError>(values)
            })
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
        let ids = py.detach(|| {
            (0..cells.len())
                .filter(|&logical| !cells.is_row_missing(logical))
                .map(|logical| cells.id_at(logical))
                .collect::<Vec<_>>()
        });
        let floor = dispatch_cell_grid!(self, G, { G::parse_compact_floor(depth)? });
        Ok(py.detach(move || {
            dispatch_cell_grid!(&cells, G, {
                Self::from_ids(kind, G::compact_ids(ids, floor))
            })
        }))
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
        let cells = self.clone();
        py.detach(move || {
            let (missing, _) = cells.missing_mask_and_present_count();
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
                let (ids, offsets) = builder.finish();
                Groups::from_cell_flat(kind, ids, offsets, missing)
            })
        })
        .map_err(PyErr::from)
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
    pub(crate) fn parent(
        &self,
        py: Python<'_>,
        depth: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let kind = self.kind();
        let depth = depth.filter(|value| !value.is_none());
        if depth.is_none() {
            return py
                .detach(|| {
                    dispatch_cell_grid!(self, G, {
                        let mut present = self.logical_cells::<G>();
                        let mut ids = Vec::with_capacity(self.len());
                        for row in 0..self.len() {
                            ids.push(if self.is_row_missing(row) {
                                0
                            } else {
                                cell_ops::cell_parent_at_depth(
                                    present.next().expect("present cell count matches"),
                                    None,
                                )?
                                .hash_key()
                            });
                        }
                        let (missing, _) = self.missing_mask_and_present_count();
                        Ok::<_, CellArrayError>(Self::from_ids_with_mask(kind, ids, missing))
                    })
                })
                .map_err(PyErr::from);
        }

        let admission = py.detach(|| {
            dispatch_cell_grid!(self, G, {
                if self.logical_cells::<G>().next().is_some() {
                    ParentAdmission::Present
                } else {
                    let (missing, _) = self.missing_mask_and_present_count();
                    ParentAdmission::AllMissing(Self::from_ids_with_mask(
                        kind,
                        vec![0; self.len()],
                        missing,
                    ))
                }
            })
        });
        let target_depth = match admission {
            ParentAdmission::AllMissing(cells) => return Ok(cells),
            ParentAdmission::Present => dispatch_cell_grid!(self, G, {
                cell_ops::parse_explicit_depth(depth, G::parse_depth)?
            }),
        };
        py.detach(|| {
            dispatch_cell_grid!(self, G, {
                let mut present = self.logical_cells::<G>();
                let mut ids = Vec::with_capacity(self.len());
                for row in 0..self.len() {
                    ids.push(if self.is_row_missing(row) {
                        0
                    } else {
                        cell_ops::cell_parent_at_depth(
                            present.next().expect("present cell count matches"),
                            target_depth,
                        )?
                        .hash_key()
                    });
                }
                let (missing, _) = self.missing_mask_and_present_count();
                Ok::<_, CellArrayError>(Self::from_ids_with_mask(kind, ids, missing))
            })
        })
        .map_err(PyErr::from)
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
        let depth = dispatch_cell_grid!(self, G, { G::parse_depth(depth)? });
        py.detach(|| {
            dispatch_cell_grid!(self, G, {
                if let Some(cell) = self.logical_cells::<G>().find(|cell| cell.depth() > depth) {
                    return Err(CellArrayError::Geometry(uncompact_floor_message(
                        kind,
                        G::DEPTH_NAME,
                        G::uncompact_floor_token(&cell),
                    )));
                }
                let cells = self.map_logical_cells::<G, _>(|cell| cell);
                let ids = Self::ids_from_cells(G::uncompact_cells(cells, depth)?);
                Ok::<_, CellArrayError>(Self::from_ids(kind, ids))
            })
        })
        .map_err(PyErr::from)
    }
}
