use super::*;

#[pyo3::pymethods]
impl PyCellArray {
    /// Cell areas in square meters, using each grid's area model.
    ///
    /// H3 and S2 use an authalic-sphere model. Geohash and tile use the WGS 84
    /// ellipsoidal area of their lon/lat rectangle.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One ``float64`` area (m²) per cell.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> arr = gm.CellArray([gm.Tile(x=0, y=0, zoom=0)])
    /// >>> float(arr.area[0]) > 1e14
    /// True
    #[getter]
    pub(crate) fn area(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cells = self.clone();
        let values = py.detach(move || {
            dispatch_cell_grid!(&cells, G, {
                let mut values = Vec::with_capacity(cells.len());
                for row in 0..cells.len() {
                    values.push(if cells.is_row_missing(row) {
                        f64::NAN
                    } else {
                        G::from_validated_id(cells.id_at(row)).area_m2()
                    });
                }
                values
            })
        });
        float64_array(py, values)
    }

    /// Center points of every cell, as a packed WGS84 point array.
    ///
    /// Computes each center from its cell id and allocates a packed point
    /// coordinate column.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///     Dense cells yield one ``Point`` (lon/lat, ``OGC:CRS84``) per row.
    ///     Missing rows in a nullable ``CellArray`` remain ``None``.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> arr = gm.CellArray([gm.Tile(x=0, y=0, zoom=1)])
    /// >>> arr.center.to_wkt()[0]
    /// 'POINT (-90 42.5255643899033)'
    #[getter]
    pub(crate) fn center(&self, py: Python<'_>) -> PyResult<crate::PyGeometryArray> {
        use crate::grid::cell::GridCell as _;
        let cells = self.clone();
        py.detach(move || {
            let (missing, present_count) = cells.missing_mask_and_present_count();
            dispatch_cell_grid!(&cells, G, {
                let mut xs = Vec::with_capacity(present_count);
                let mut ys = Vec::with_capacity(present_count);
                for cell in cells.logical_cells::<G>() {
                    let point = cell.center_point();
                    xs.push(point.x);
                    ys.push(point.y);
                }
                let coords = CoordSeq::try_from_vecs(xs, ys, None, None)?;
                let result = crate::PyGeometryArray::packed_points(
                    coords,
                    crate::Frame::Crs(crate::wgs84_crs()),
                )?;
                match missing {
                    None => Ok(result),
                    Some(mask) => crate::PyGeometryArray::scatter_present_rows(&result, mask),
                }
            })
        })
        .map_err(PyErr::from)
    }

    /// Filled WGS84 polygon of every cell, as a geometry array.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///     Dense cells yield one polygon per row. Missing rows in a nullable
    ///     ``CellArray`` remain ``None``.
    #[getter]
    pub(crate) fn polygon(&self, py: Python<'_>) -> PyResult<crate::PyGeometryArray> {
        let cells = self.clone();
        py.detach(move || -> crate::error::Result<crate::PyGeometryArray> {
            let (missing, present_count) = cells.missing_mask_and_present_count();
            dispatch_cell_grid!(&cells, G, {
                let shapes = cells.try_map_logical_cells::<G, _, crate::error::Error>(
                    Vec::with_capacity(present_count),
                    GridCell::boundary_shape,
                )?;
                let result = crate::PyGeometryArray::from_shapes(
                    shapes,
                    crate::Frame::Crs(crate::wgs84_crs()),
                )?;
                match missing {
                    None => Ok(result),
                    Some(mask) => crate::PyGeometryArray::scatter_present_rows(&result, mask),
                }
            })
        })
        .map_err(PyErr::from)
    }

    /// Dissolve this cell set into one WGS 84 longitude/latitude outline.
    /// Missing rows are skipped; this is a set operation over present cells.
    /// A seam-crossing outline preserves support on both sides of ±180° rather
    /// than filling through the map's middle; its chart-cut edge is not part of
    /// the represented cell boundary.
    ///
    /// Returns
    /// -------
    /// Polygon or MultiPolygon
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> gm.CellArray([cell]).to_polygon().geometry_type
    /// 'Polygon'
    fn to_polygon(&self, py: Python<'_>) -> PyResult<crate::Typed> {
        let cells = self.clone();
        py.detach(move || match cells.kind() {
            GridKind::H3Cell => {
                let cells = cells.map_logical_cells::<h3o::CellIndex, _>(|cell| cell);
                super::h3::h3_dissolve(cells)
            },
            GridKind::S2Cell => {
                let cells = cells.map_logical_cells::<CellId, _>(|cell| cell);
                super::s2::s2_dissolve(&cells)
            },
            GridKind::Tile => {
                let cells = cells.map_logical_cells::<Tile, _>(|cell| cell);
                rect_cells_to_polygon(cells)
            },
            GridKind::GeohashCell => {
                let cells = cells.map_logical_cells::<Geohash, _>(|cell| cell);
                rect_cells_to_polygon(cells)
            },
        })
        .map_err(PyErr::from)
    }
}
