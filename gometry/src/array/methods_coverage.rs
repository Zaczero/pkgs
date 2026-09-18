use crate::array::{
    PyGeometry, PyGeometryArray, PyResult, Python, SimplifyMethod, Typed, geometry, pymethods,
};

#[pymethods]
impl PyGeometryArray {
    /// Test whether this polygonal coverage is valid.
    ///
    /// Parameters
    /// ----------
    /// gap_width : float, default 0.0
    ///     Also flag boundaries that face a neighbor across a gap narrower
    ///     than this (0 disables gap detection).
    ///
    /// Returns
    /// -------
    /// bool
    ///     ``True`` when no row has invalid coverage edges.
    ///
    /// Raises
    /// ------
    /// GeometryTypeError
    ///     If a row is not a `Polygon` or `MultiPolygon`.
    /// GeometryError
    ///     If ``gap_width`` is negative or non-finite.
    ///
    /// See Also
    /// --------
    /// coverage_invalid_edges : The offending linework itself.
    /// coverage_clean : Rebuild an exact coverage from a near-coverage.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> grid = gm.GeometryArray([
    /// ...     gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
    /// ...     gm.Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
    /// ... ])
    /// >>> grid.coverage_is_valid()
    /// True
    /// >>> gm.GeometryArray([
    /// ...     gm.Polygon([(0, 0), (1.1, 0), (1.1, 1), (0, 1)]),
    /// ...     gm.Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
    /// ... ]).coverage_is_valid()
    /// False
    #[pyo3(signature = (*, gap_width = 0.0))]
    pub fn coverage_is_valid(&self, py: Python<'_>, gap_width: f64) -> PyResult<bool> {
        let shapes = self.present_shapes()?;
        Ok(py.detach(move || geometry::coverage_is_valid(&shapes, gap_width))?)
    }

    /// Per-row invalid coverage boundary linework (see `coverage_invalid_edges`).
    ///
    /// Parameters
    /// ----------
    /// gap_width : float, default 0.0
    ///     Also flag boundaries that face a neighbor across a gap narrower
    ///     than this (0 disables gap detection).
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///     One `LineString`/`MultiLineString` per input row.
    ///
    /// Raises
    /// ------
    /// GeometryTypeError
    ///     If a row is not a `Polygon` or `MultiPolygon`.
    /// GeometryError
    ///     If ``gap_width`` is negative or non-finite.
    ///
    /// See Also
    /// --------
    /// coverage_is_valid : The boolean verdict.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> len(gm.GeometryArray(
    /// ...     [gm.box(0, 0, 1, 1), gm.box(0.5, 0, 1.5, 1)]
    /// ... ).coverage_invalid_edges())
    /// 2
    #[pyo3(signature = (*, gap_width = 0.0))]
    pub fn coverage_invalid_edges(&self, py: Python<'_>, gap_width: f64) -> PyResult<Self> {
        let shapes = self.present_shapes()?;
        let edges = py.detach(move || geometry::coverage_invalid_edges(&shapes, gap_width))?;
        Ok(self.scatter_present_result(Self::from_shapes(edges, self.frame.clone())?)?)
    }

    /// Simplify this valid polygonal coverage's boundaries (see `coverage_simplify`).
    ///
    /// Parameters
    /// ----------
    /// tolerance : float
    ///     Distance-scale simplification tolerance, in coordinate units;
    ///     non-negative finite.
    /// method : {'vw', 'dp'}, default 'vw'
    ///     Importance criterion: ``'vw'`` is area-based (Visvalingam-Whyatt),
    ///     ``'dp'`` is distance-based (Douglas-Peucker). For coverage
    ///     simplification, this is a local importance criterion; it does not
    ///     provide scalar Douglas-Peucker's original-span error contract.
    /// simplify_boundary : bool, default True
    ///     Also simplify exterior (unshared) boundaries; ``False`` pins them
    ///     and simplifies only the shared interfaces.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///     One simplified `Polygon`/`MultiPolygon` per input row.
    ///
    /// Raises
    /// ------
    /// GeometryTypeError
    ///     If a row is not a `Polygon` or `MultiPolygon`.
    /// GeometryError
    ///     If ``tolerance`` is negative or non-finite.
    /// InvalidGeometryError
    ///     If the rows do not form a valid coverage.
    ///
    /// See Also
    /// --------
    /// Geometry.simplify : Per-geometry simplify (not coverage-topology-preserving).
    /// coverage_is_valid : Whether the rows form a valid polygonal coverage.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> left = gm.Polygon([(0, 0), (1, 0), (1.05, 0.5), (1, 1), (0, 1)])
    /// >>> right = gm.Polygon([(1, 0), (2, 0), (2, 1), (1, 1), (1.05, 0.5)])
    /// >>> out = gm.GeometryArray([left, right]).coverage_simplify(0.5)
    /// >>> out.coverage_is_valid()
    /// True
    /// >>> [round(geometry.area, 6) for geometry in out]
    /// [1.0, 1.0]
    #[pyo3(
        signature = (tolerance, *, method = SimplifyMethod::Vw, simplify_boundary = true),
        text_signature = "($self, tolerance, *, method='vw', simplify_boundary=True)"
    )]
    pub fn coverage_simplify(
        &self,
        py: Python<'_>,
        tolerance: f64,
        method: SimplifyMethod,
        simplify_boundary: bool,
    ) -> PyResult<Self> {
        let shapes = self.present_shapes()?;
        let rows = py.detach(move || {
            geometry::coverage_simplify(&shapes, tolerance, method, simplify_boundary)
        })?;
        Ok(self.scatter_present_result(Self::from_shapes(rows, self.frame.clone())?)?)
    }

    /// Union this polygonal coverage into one geometry (see `coverage_union`).
    ///
    /// Returns
    /// -------
    /// Geometry
    ///     A single `Polygon`/`MultiPolygon` covering the merged area.
    ///
    /// Raises
    /// ------
    /// InvalidGeometryError
    ///     If the array is empty or the rows do not form a valid coverage.
    ///
    /// See Also
    /// --------
    /// union_all : General multi-geometry union (handles overlaps).
    /// coverage_is_valid : Whether the rows form a valid polygonal coverage.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> tiles = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1)])
    /// >>> merged = tiles.coverage_union()
    /// >>> (merged.geometry_type, round(merged.area, 6))
    /// ('Polygon', 2.0)
    pub fn coverage_union(&self, py: Python<'_>) -> PyResult<Typed> {
        let shapes = self.present_shapes()?;
        if shapes.is_empty() {
            return Err(crate::error::Error::from(
                crate::geometry::GeometryErrorKind::EmptyGeometrySequence {
                    operation: "coverage_union",
                },
            )
            .into());
        }
        let domain = geometry::topology_domain(&self.frame)?;
        let shape = py.detach(move || geometry::coverage_union_topo(&shapes, domain))?;
        Ok(Typed(PyGeometry::with_frame(shape, self.frame.clone())?))
    }

    /// Clean this near-coverage into an exact polygonal coverage (see `coverage_clean`).
    ///
    /// Parameters
    /// ----------
    /// grid_size : float, default 0.0
    ///     Vertex snapping grid in coordinate units; ``0`` preserves input
    ///     coordinates and disables snapping.
    /// gap_width : float, default 0.0
    ///     Merge enclosed gaps narrower than this into a neighbor (0 keeps
    ///     gaps).
    /// overlap_rule : str, default 'longest_border'
    ///     Which row keeps a region covered more than once:
    ///     ``'longest_border'``, ``'max_area'``, ``'min_area'``, ``'min_index'``.
    ///     Cleaning rebuilds faces from one exact partition. Source Z/M
    ///     ordinates are retained for rows whose emitted nodes have one
    ///     consistent source value; an unavailable or conflicting axis is
    ///     dropped for that row.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///     One cleaned `Polygon`/`MultiPolygon` per input row.
    ///
    /// Raises
    /// ------
    /// GeometryTypeError
    ///     If a row is not a `Polygon` or `MultiPolygon`.
    /// GeometryError
    ///     If ``grid_size`` or ``gap_width`` is negative or non-finite.
    /// InvalidGeometryError
    ///     If ``grid_size > 0`` and snap-repair cannot converge on a valid
    ///     grid-aligned result.
    ///
    /// See Also
    /// --------
    /// coverage_is_valid : Test whether a polygonal coverage is already valid.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> rows = gm.GeometryArray([gm.box(0, 0, 1.2, 1), gm.box(1, 0, 2, 1)])
    /// >>> rows.coverage_is_valid()
    /// False
    /// >>> cleaned = rows.coverage_clean()
    /// >>> cleaned.coverage_is_valid()
    /// True
    #[pyo3(
        signature = (*, grid_size = 0.0, gap_width = 0.0, overlap_rule = geometry::CoverageOverlapRule::LongestBorder),
        text_signature = "($self, *, grid_size=0.0, gap_width=0.0, overlap_rule='longest_border')"
    )]
    pub fn coverage_clean(
        &self,
        py: Python<'_>,
        grid_size: f64,
        gap_width: f64,
        overlap_rule: geometry::CoverageOverlapRule,
    ) -> PyResult<Self> {
        let shapes = self.present_shapes()?;
        let rows = py.detach(move || {
            geometry::coverage_clean(&shapes, grid_size, gap_width, overlap_rule)
        })?;
        Ok(self.scatter_present_result(Self::from_shapes(rows, self.frame.clone())?)?)
    }
}
