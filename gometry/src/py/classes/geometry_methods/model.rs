use std::sync::Arc;

use pyo3::IntoPyObjectExt as _;
use pyo3::exceptions::PyValueError;
use pyo3::types::PySequence;

use crate::array::RowProducts;
use crate::boundary::coordinates::measure_shape;
use crate::geometry::PreparedPlan;
use crate::geometry::types::ShapeHeapMemo;
use crate::py::row::{RowContainer, RowIndexOrSlice, RowIterState, parse_row_index_or_slice};
use crate::py::sizeof::python_sizeof;
use crate::{
    Bound, Crs, Frame, GeometryKind, HeapSize, IntoPyObject, Py, PyAny, PyAnyMethods as _,
    PyClassInitializer, PyErr, PyRef, PyResult, Python, Result, ShapeData, exact_geometry, pyclass,
    pymethods, wgs84_crs,
};
#[cfg(test)]
use crate::{Point, Shape};

/// An immutable geometry with optional CRS.
///
/// The frozen scalar value at the heart of gometry: a point, linestring,
/// polygon, multi-part, or collection, carrying its coordinate
/// dimensionality (XY/XYZ/XYM/XYZM) and an optional CRS + epoch frame.
/// Construct with the leaf classes (``Point(...)``, ``LineString(...)``) or
/// parsers (``from_wkt(...)``),
/// inspect it (``geom.bounds``, ``geom.coords``), relate it
/// (``contains(geom, other)``), measure it (``geom.area`` — geodesic meters on
/// a geographic CRS), and derive from it (``geom.buffer(10.0)``); every
/// operation returns a new geometry. Instances are one of the typed subclasses
/// (``Point``, ``LineString``, ``Polygon``, ...), so ``isinstance`` narrows.
#[pyclass(
    name = "Geometry",
    module = "gometry",
    frozen,
    immutable_type,
    subclass,
    weakref,
    skip_from_py_object
)]
#[derive(Clone, Debug)]
pub struct PyGeometry {
    // Frozen, immutable geometry. Pattern-matches use `geometry.shape.shape()`.
    pub shape: Arc<ShapeData>,
    /// One physical row-product cell owns both shape-derived and frame-derived
    /// products. Array scalar extraction clones this view, so the scalar and
    /// its source cannot initialize drifting plan/cache pairs.
    pub(crate) row_products: RowProducts,
    pub(crate) product_row: usize,
    /// CRS + coordinate epoch as one frame — the `epoch ⟹ crs` invariant lives
    /// in the type (`Frame` has no epoch-without-CRS variant), not a runtime
    /// guard.
    pub(crate) frame: Frame,
}

impl HeapSize for PyGeometry {
    fn heap_bytes(&self) -> usize {
        let shape_bytes = HeapSize::heap_bytes(&self.shape);
        let product_bytes = self.row_products.heap_bytes_at(self.product_row);
        // A scalar prepared plan borrows this exact ShapeData Arc. Its retained
        // size includes the shape, which the scalar already reports above.
        let prepared_shape_bytes = self
            .cached_prepared_plan()
            .filter(|plan| Arc::ptr_eq(plan.shape_arc(), &self.shape))
            .map_or(0, |_| shape_bytes);
        shape_bytes
            .saturating_add(product_bytes.saturating_sub(prepared_shape_bytes))
            .saturating_add(self.frame.crs_ref().map_or(0, HeapSize::heap_bytes))
    }
}

// Typed leaf classes. Each is a real Python subclass of `Geometry`, carrying no
// data of its own — the `Shape` enum in the base already distinguishes the
// kind. Their only job is to make `isinstance(g, Point)` work, host
// type-specific members (e.g. `Point.x`, `Polygon.exterior`), and let the stub
// narrow returns. Construction always routes through [`Typed`] so a returned
// geometry is the subclass matching its `Shape` variant.
// rustfmt re-indents attributes inside `macro_rules!` deeper on every
// run (non-idempotent), so this macro opts out and keeps a sane indent.
#[rustfmt::skip]
macro_rules! geometry_leaf {
    ($ident:ident, $name:literal, $doc:literal) => {
        #[doc = $doc]
        // No freelist: PyO3 0.28's freelist takes a lock for free-threaded
        // safety, and A/B release benchmarks (2k-element array iteration,
        // grid_disk cell fan-outs) show it costs 0-20% rather than saving —
        // the plain allocator wins.
        #[pyclass(
            name = $name,
            module = "gometry",
            extends = PyGeometry,
            frozen,
            immutable_type,
            skip_from_py_object
        )]
        pub(crate) struct $ident;
    };
}
geometry_leaf!(
    PyPoint,
    "Point",
    "A single point geometry.

Parameters
----------
x, y : float, optional
    Point coordinates; both omitted builds ``POINT EMPTY``.
z : float, optional
    Z ordinate (adds a 3D dimension).
m : float, optional
    M (measure) ordinate.
crs : CRS, int, str, or None, optional
    Coordinate reference system, attached as metadata (never transforms coordinates).
epoch : float or None, optional
    Coordinate epoch for a dynamic CRS; allowed only with ``crs``."
);
geometry_leaf!(
    PyMultiPoint,
    "MultiPoint",
    "A collection of points.

Parameters
----------
coordinates : sequence of points or coordinate tuples, optional
    The member points; omitted builds an empty multipoint.
x, y, z, m : sequence of float, optional
    Column form: parallel ordinate arrays, one entry per point.
crs : CRS, int, str, or None, optional
    Coordinate reference system, attached as metadata (never transforms coordinates).
epoch : float or None, optional
    Coordinate epoch for a dynamic CRS; allowed only with ``crs``."
);
geometry_leaf!(
    PyLineString,
    "LineString",
    "A single linestring (polyline).

Parameters
----------
coordinates : sequence of coordinate tuples, optional
    The vertices as ``(x, y[, z, m])`` tuples; omitted builds an empty line.
x, y, z, m : sequence of float, optional
    Column form: parallel ordinate arrays, one entry per vertex.
crs : CRS, int, str, or None, optional
    Coordinate reference system, attached as metadata (never transforms coordinates).
epoch : float or None, optional
    Coordinate epoch for a dynamic CRS; allowed only with ``crs``."
);
geometry_leaf!(
    PyMultiLineString,
    "MultiLineString",
    "A collection of linestrings.

Parameters
----------
lines : sequence of LineString or coordinate sequences, optional
    The member lines; omitted builds an empty multilinestring.
crs : CRS, int, str, or None, optional
    Coordinate reference system, attached as metadata (never transforms coordinates).
epoch : float or None, optional
    Coordinate epoch for a dynamic CRS; allowed only with ``crs``."
);
geometry_leaf!(
    PyPolygon,
    "Polygon",
    "A single polygon (an exterior ring with optional holes).

Parameters
----------
shell : sequence of coordinate tuples, optional
    Exterior ring vertices; omitted builds ``POLYGON EMPTY``.
holes : sequence of rings, optional
    Interior rings, each a coordinate sequence.
x, y, z, m : sequence of float, optional
    Column form for the exterior ring: parallel ordinate arrays.
crs : CRS, int, str, or None, optional
    Coordinate reference system, attached as metadata (never transforms coordinates).
epoch : float or None, optional
    Coordinate epoch for a dynamic CRS; allowed only with ``crs``."
);
geometry_leaf!(
    PyMultiPolygon,
    "MultiPolygon",
    "A collection of polygons.

Parameters
----------
polygons : sequence of Polygon or (shell, holes) pairs, optional
    The member polygons; omitted builds an empty multipolygon.
crs : CRS, int, str, or None, optional
    Coordinate reference system, attached as metadata (never transforms coordinates).
epoch : float or None, optional
    Coordinate epoch for a dynamic CRS; allowed only with ``crs``."
);
geometry_leaf!(
    PyGeometryCollection,
    "GeometryCollection",
    "A heterogeneous collection of geometries.

Parameters
----------
geometries : sequence of Geometry, optional
    The member geometries, of any types; omitted builds an empty collection.
crs : CRS, int, str, or None, optional
    Coordinate reference system, attached as metadata (never transforms coordinates).
epoch : float or None, optional
    Coordinate epoch for a dynamic CRS; allowed only with ``crs``."
);

/// A geometry on its way back to Python as the leaf subclass matching its
/// `Shape` variant.
///
/// Geometry-returning methods/functions return `Typed` (or `PyResult<Typed>`)
/// instead of the bare base class so that, e.g., `centroid()` yields a `Point`
/// instance and `isinstance(..., Point)` holds.
pub struct Typed(pub PyGeometry);

impl<'py> IntoPyObject<'py> for Typed {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        // Read the variant tag (cheap, Copy) before moving the geometry into the
        // initializer, then attach the matching leaf marker.
        let kind = GeometryKind::of(&self.0.shape);
        let init = PyClassInitializer::from(self.0);
        macro_rules! leaf {
            ($leaf:ident) => {
                Bound::new(py, init.add_subclass($leaf))?.into_any()
            };
        }
        Ok(match kind {
            GeometryKind::Point => leaf!(PyPoint),
            GeometryKind::MultiPoint => leaf!(PyMultiPoint),
            GeometryKind::LineString => leaf!(PyLineString),
            GeometryKind::MultiLineString => leaf!(PyMultiLineString),
            GeometryKind::Polygon => leaf!(PyPolygon),
            GeometryKind::MultiPolygon => leaf!(PyMultiPolygon),
            GeometryKind::GeometryCollection => leaf!(PyGeometryCollection),
        })
    }
}

impl From<PyGeometry> for Typed {
    fn from(geometry: PyGeometry) -> Self {
        Self(geometry)
    }
}

// --- Typed leaf members -----------------------------------------------------
// Type-specific members live on the leaf so they are only reachable on the
// right kind (e.g. `line.x` is an AttributeError, not a runtime TypeError).
// Each reads the base `Geometry` via `PyRef::as_super`; a leaf always wraps its
// own variant.

/// Lazy view over one geometry's immediate parts.
///
/// Returned by ``.parts`` on every geometry. Simple geometries expose a
/// singleton view of themselves; multipart and collection geometries expose
/// their immediate members. Scalar indexing and iteration materialize one
/// typed geometry at a time via ``part_at``; slice indexing may still build a
/// list up front.
#[pyclass(
    name = "GeometryParts",
    module = "gometry",
    frozen,
    immutable_type,
    sequence
)]
pub(crate) struct PyGeometryParts {
    geometry: PyGeometry,
}

impl PyGeometryParts {
    fn part_matches(&self, row: usize, item: &Bound<'_, PyAny>) -> bool {
        geometry_part_matches(&self.geometry, row, item)
    }
}

fn geometry_part_matches(geometry: &PyGeometry, row: usize, item: &Bound<'_, PyAny>) -> bool {
    let Some(other) = exact_geometry(item) else {
        return false;
    };
    geometry_part_matches_shape(
        &geometry.shape,
        row,
        geometry.crs_ref() == other.crs_ref() && geometry.epoch() == other.epoch(),
        other.shape.shape(),
    )
}

fn geometry_part_matches_shape(
    geometry: &crate::geometry::ShapeData,
    row: usize,
    same_frame: bool,
    other: &crate::geometry::Shape,
) -> bool {
    same_frame && geometry.part_at(row).is_some_and(|part| part == *other)
}

fn geometry_parts_contains(py: Python<'_>, geometry: &PyGeometry, item: &Bound<'_, PyAny>) -> bool {
    let Some(other) = exact_geometry(item) else {
        return false;
    };
    let same_frame = geometry.crs_ref() == other.crs_ref() && geometry.epoch() == other.epoch();
    let geometry_shape = Arc::clone(&geometry.shape);
    let other_shape = Arc::clone(&other.shape);
    py.detach(move || {
        (0..geometry_shape.part_count()).any(|row| {
            geometry_part_matches_shape(&geometry_shape, row, same_frame, other_shape.shape())
        })
    })
}

fn geometry_parts_index(
    geometry: &PyGeometry,
    value: &Bound<'_, PyAny>,
    start: i64,
    stop: Option<i64>,
) -> PyResult<usize> {
    let len = geometry.shape.part_count();
    let clamp = |bound: i64| -> usize {
        let resolved = if bound < 0 {
            bound.saturating_add(i64::try_from(len).unwrap_or(i64::MAX))
        } else {
            bound
        };
        usize::try_from(resolved.max(0)).unwrap_or(0).min(len)
    };
    let start = clamp(start);
    let stop = stop.map_or(len, clamp);
    if start < stop
        && let Some(row) = (start..stop).find(|&row| geometry_part_matches(geometry, row, value))
    {
        return Ok(row);
    }
    let value = value
        .repr()
        .and_then(|repr| repr.extract::<String>())
        .unwrap_or_else(|_| "value".to_owned());
    Err(PyValueError::new_err(format!(
        "{value} is not in GeometryParts"
    )))
}

fn geometry_parts_count(geometry: &PyGeometry, value: &Bound<'_, PyAny>) -> usize {
    (0..geometry.shape.part_count())
        .filter(|&row| geometry_part_matches(geometry, row, value))
        .count()
}

/// Index or slice into a multipart geometry's parts. Scalar access clones one
/// part via ``part_at``; slice access may materialize a list.
fn geometry_parts_getitem(
    geometry: &PyGeometry,
    py: Python<'_>,
    index: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    match parse_row_index_or_slice(
        index,
        geometry.shape.part_count(),
        <PyGeometryParts as RowContainer>::LABEL,
    )? {
        RowIndexOrSlice::Slice {
            start, step, count, ..
        } => {
            let mut parts = Vec::with_capacity(count);
            let mut row = start;
            for _ in 0..count {
                if let Some(part) = geometry.shape.part_at(row as usize) {
                    parts.push(geometry.typed_shape(part));
                }
                row = row.saturating_add(step);
            }
            Ok(parts.into_pyobject(py)?.into_any().unbind())
        },
        RowIndexOrSlice::Index(row) => {
            let part = geometry.shape.part_at(row).expect("part index in range");
            Ok(geometry
                .typed_shape(part)
                .into_pyobject(py)?
                .into_any()
                .unbind())
        },
    }
}

#[pymethods]
impl PyGeometryParts {
    // Sequences compare by value; like lists, they do not hash.
    #[expect(non_upper_case_globals, reason = "Python dunder name")]
    const __hash__: Option<Py<PyAny>> = None;

    /// Number of component parts.
    ///
    /// Returns
    /// -------
    /// int
    fn __len__(&self) -> usize {
        self.geometry.shape.part_count()
    }

    /// Logical coordinate payload retained by the source geometry.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    fn nbytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyInt>> {
        let measure = measure_shape(py, self.geometry.shape.shape())?;
        Ok(measure.bytes_pyint(py))
    }

    /// ``sys.getsizeof`` support: this lazy view plus the source geometry's
    /// logical coordinate heap. The view shares the geometry; it does not
    /// materialize individual parts.
    ///
    /// Raises
    /// ------
    /// OverflowError
    ///     If the reported size exceeds Python's ``Py_ssize_t`` range.
    fn __sizeof__(&self) -> PyResult<isize> {
        python_sizeof(std::mem::size_of::<Self>(), self.heap_bytes())
    }

    fn __repr__(&self) -> String {
        format!("<GeometryParts len={}>", self.geometry.shape.part_count())
    }

    /// Select parts by integer or slice.
    ///
    /// An ``int`` returns one component geometry. A ``slice`` returns a
    /// ``list`` of component geometries.
    ///
    /// Returns
    /// -------
    /// Geometry or list of Geometry
    fn __getitem__(&self, py: Python<'_>, index: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        geometry_parts_getitem(&self.geometry, py, index)
    }

    /// Iterate component geometries.
    ///
    /// Returns
    /// -------
    /// iterator of Geometry
    fn __iter__(&self) -> PyGeometryPartsIter {
        PyGeometryPartsIter::new(&self.geometry, false)
    }

    /// Iterate component geometries in reverse order.
    ///
    /// Returns
    /// -------
    /// iterator of Geometry
    fn __reversed__(&self) -> PyGeometryPartsIter {
        PyGeometryPartsIter::new(&self.geometry, true)
    }

    /// Whether a geometry equals one of the component parts.
    ///
    /// Returns
    /// -------
    /// bool
    fn __contains__(&self, py: Python<'_>, item: &Bound<'_, PyAny>) -> bool {
        geometry_parts_contains(py, &self.geometry, item)
    }

    /// First index of an equal part in ``[start, stop)``.
    ///
    /// Parameters
    /// ----------
    /// value : object
    ///     The geometry value to locate.
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
    ///     If no part in the window equals ``value``.
    #[pyo3(signature = (value, start = 0, stop = None), text_signature = "($self, value, start=0, stop=None)")]
    fn index(
        &self,
        value: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = crate::strict_i64)] start: i64,
        #[pyo3(from_py_with = crate::strict_optional_i64)] stop: Option<i64>,
    ) -> PyResult<usize> {
        geometry_parts_index(&self.geometry, value, start, stop)
    }

    /// Number of parts equal to ``value``.
    ///
    /// Parameters
    /// ----------
    /// value : object
    ///     The geometry value to count.
    ///
    /// Returns
    /// -------
    /// int
    fn count(&self, value: &Bound<'_, PyAny>) -> usize {
        geometry_parts_count(&self.geometry, value)
    }

    /// Value equality against another parts view or geometry sequence.
    fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let len = self.geometry.shape.part_count();
        if let Ok(parts) = other.cast::<Self>() {
            let parts = parts.get();
            let equal = len == parts.geometry.shape.part_count()
                && (0..len).all(|row| {
                    self.geometry.shape.part_at(row) == parts.geometry.shape.part_at(row)
                        && self.geometry.frame == parts.geometry.frame
                });
            return equal.into_py_any(py);
        }
        let Ok(sequence) = other.cast::<PySequence>() else {
            return Ok(py.NotImplemented());
        };
        let mut items = sequence.try_iter()?;
        for row in 0..len {
            crate::check_python_signals(py, row)?;
            let Some(item) = items.next() else {
                return false.into_py_any(py);
            };
            let item = item?;
            if !self.part_matches(row, &item) {
                return false.into_py_any(py);
            }
        }
        match items.next() {
            None => {},
            Some(Err(error)) => return Err(error),
            Some(Ok(_)) => return false.into_py_any(py),
        }
        true.into_py_any(py)
    }

    /// ``copy.copy`` returns this immutable view itself.
    fn __copy__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    /// ``copy.deepcopy`` returns this immutable view itself.
    #[pyo3(signature = (memo))]
    fn __deepcopy__(slf: &Bound<'_, Self>, memo: &Bound<'_, PyAny>) -> Py<Self> {
        let _ = memo;
        slf.clone().unbind()
    }

    /// Pickle support through the parent geometry's ``parts`` property.
    fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let getattr = py.import("builtins")?.getattr("getattr")?.unbind();
        let parent = Typed(self.geometry.clone()).into_py_any(py)?;
        Ok((getattr, (parent, "parts").into_py_any(py)?))
    }
}

/// Lazy iterator over a ``GeometryParts`` view: one typed leaf per
/// ``__next__`` via ``part_at``, without building the full part list up front.
#[pyclass(
    name = "GeometryPartsIterator",
    module = "gometry",
    frozen,
    immutable_type
)]
pub(crate) struct PyGeometryPartsIter {
    source: PyGeometryParts,
    state: RowIterState,
}

impl PyGeometryPartsIter {
    fn new(base: &PyGeometry, reverse: bool) -> Self {
        Self {
            source: PyGeometryParts {
                geometry: PyGeometry {
                    shape: Arc::clone(&base.shape),
                    row_products: base.row_products.clone(),
                    product_row: base.product_row,
                    frame: base.frame.clone(),
                },
            },
            state: RowIterState::new(reverse),
        }
    }
}

row_iter_pymethods! {
    impl PyGeometryPartsIter {
        source: PyGeometryParts,
    }
}

impl HeapSize for PyGeometryParts {
    fn heap_bytes(&self) -> usize {
        ShapeHeapMemo::default().coordinate_bytes(self.geometry.shape.shape())
    }
}

impl RowContainer for PyGeometryParts {
    const LABEL: &'static str = "GeometryParts";

    fn row_count(&self) -> usize {
        self.geometry.shape.part_count()
    }

    fn scalar_row(&self, py: Python<'_>, row: usize) -> PyResult<Py<PyAny>> {
        let part = self
            .geometry
            .shape
            .part_at(row)
            .expect("part index in range");
        self.geometry.typed_shape(part).into_py_any(py)
    }
}

#[pymethods]
impl PyGeometry {
    /// Lazy view over this geometry's top-level parts.
    ///
    /// Simple geometries expose themselves as a one-row view; multipart and
    /// collection geometries expose their members. `GeometryArray(geom.parts)`
    /// materializes them as an array.
    ///
    /// Returns
    /// -------
    /// GeometryParts
    #[getter]
    fn parts(&self) -> PyGeometryParts {
        PyGeometryParts {
            geometry: Self {
                shape: Arc::clone(&self.shape),
                row_products: self.row_products.clone(),
                product_row: self.product_row,
                frame: self.frame.clone(),
            },
        }
    }
}

#[cfg(test)]
mod frame_cache_ownership_tests {
    use super::*;
    use crate::boundary::crs_arc_static;

    #[test]
    fn scalar_retag_preserves_row_products_but_new_shapes_are_fresh() {
        let geometry =
            PyGeometry::with_frame(Shape::Point(Point::new_raw_xy(1.0, 2.0)), Frame::None);
        let clone = geometry.clone();
        assert!(Arc::ptr_eq(&geometry.frame_cache(), &clone.frame_cache()));

        let retagged = geometry.retag_frame(
            Frame::new(Some(crs_arc_static("EPSG:4326")), None).expect("valid static CRS"),
        );
        assert!(Arc::ptr_eq(&geometry.shape, &retagged.shape));
        assert!(Arc::ptr_eq(
            &geometry.frame_cache(),
            &retagged.frame_cache()
        ));
        assert!(Arc::ptr_eq(
            &geometry.prepared_plan(),
            &retagged.prepared_plan()
        ));
        assert_eq!(geometry.heap_bytes(), retagged.heap_bytes());

        let mut construction_retag = geometry.clone();
        construction_retag
            .set_crs_keep_epoch(Some(crs_arc_static("EPSG:4326")))
            .expect("static CRS has no epoch to validate");
        construction_retag
            .set_epoch_keep_crs(Some(2020.0))
            .expect("EPSG:4326 accepts a coordinate epoch");
        assert!(Arc::ptr_eq(
            &geometry.prepared_plan(),
            &construction_retag.prepared_plan()
        ));
        assert!(Arc::ptr_eq(
            &geometry.frame_cache(),
            &construction_retag.frame_cache()
        ));

        let transformed = PyGeometry::with_frame(
            Shape::Point(Point::new_raw_xy(1.0, 2.0)),
            retagged.frame.clone(),
        );
        assert!(!Arc::ptr_eq(&retagged.shape, &transformed.shape));
        assert!(!Arc::ptr_eq(
            &retagged.frame_cache(),
            &transformed.frame_cache()
        ));
        assert!(!Arc::ptr_eq(
            &retagged.prepared_plan(),
            &transformed.prepared_plan()
        ));
    }
}

/// `.parts` + the sequence protocol (`len`, indexing, iteration) for the
/// multipart and collection leaves, so `for part in multi` and `multi[i]` work
/// and each part is a typed geometry.
macro_rules! geometry_parts_methods {
    ($leaf:ty) => {
        #[pymethods]
        impl $leaf {
            /// ``case MultiPolygon([first, *rest])`` destructures the parts.
            #[classattr]
            const fn __match_args__() -> (&'static str,) {
                ("parts",)
            }

            /// The component geometries.
            #[getter]
            fn parts(slf: PyRef<'_, Self>) -> PyGeometryParts {
                let base = &**slf.as_super();
                PyGeometryParts {
                    geometry: crate::PyGeometry {
                        shape: Arc::clone(&base.shape),
                        row_products: base.row_products.clone(),
                        product_row: base.product_row,
                        frame: base.frame.clone(),
                    },
                }
            }

            /// Number of component parts.
            ///
            /// Returns
            /// -------
            /// int
            fn __len__(slf: PyRef<'_, Self>) -> usize {
                slf.as_super().shape.part_count()
            }

            /// Select parts by integer or slice.
            ///
            /// An ``int`` returns one component geometry. A ``slice`` returns a
            /// ``list`` of component geometries.
            ///
            /// Returns
            /// -------
            /// Geometry or list of Geometry
            fn __getitem__(slf: PyRef<'_, Self>, index: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
                let py = slf.py();
                geometry_parts_getitem(slf.as_super(), py, index)
            }

            /// Iterate component geometries.
            ///
            /// Returns
            /// -------
            /// iterator of Geometry
            fn __iter__(slf: PyRef<'_, Self>) -> PyGeometryPartsIter {
                PyGeometryPartsIter::new(&**slf.as_super(), false)
            }

            /// Iterate component geometries in reverse order.
            ///
            /// Returns
            /// -------
            /// iterator of Geometry
            fn __reversed__(slf: PyRef<'_, Self>) -> PyGeometryPartsIter {
                PyGeometryPartsIter::new(&**slf.as_super(), true)
            }

            /// Whether a geometry equals one of the component parts.
            fn __contains__(
                slf: PyRef<'_, Self>,
                py: Python<'_>,
                item: &Bound<'_, PyAny>,
            ) -> bool {
                let geometry = &**slf.as_super();
                geometry_parts_contains(py, geometry, item)
            }

            /// First index of an equal part in ``[start, stop)``.
            ///
            /// Parameters
            /// ----------
            /// value : object
            ///     The geometry value to locate.
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
            ///     If no part in the window equals ``value``.
            ///
            /// Examples
            /// --------
            /// >>> import gometry as gm
            /// >>> multi = gm.MultiPoint([(0, 0), (1, 1)])
            /// >>> part = multi[0]
            /// >>> multi.index(part)
            /// 0
            #[pyo3(signature = (value, start = 0, stop = None), text_signature = "($self, value, start=0, stop=None)")]
            fn index(
                slf: PyRef<'_, Self>,
                value: &Bound<'_, PyAny>,
                #[pyo3(from_py_with = crate::strict_i64)] start: i64,
                #[pyo3(from_py_with = crate::strict_optional_i64)] stop: Option<i64>,
            ) -> PyResult<usize> {
                geometry_parts_index(&**slf.as_super(), value, start, stop)
            }

            /// Number of parts equal to ``value``.
            ///
            /// Parameters
            /// ----------
            /// value : object
            ///     The geometry value to count.
            ///
            /// Returns
            /// -------
            /// int
            ///
            /// Examples
            /// --------
            /// >>> import gometry as gm
            /// >>> multi = gm.MultiPoint([(0, 0), (1, 1)])
            /// >>> multi.count(multi[0])
            /// 1
            fn count(slf: PyRef<'_, Self>, value: &Bound<'_, PyAny>) -> usize {
                geometry_parts_count(&**slf.as_super(), value)
            }
        }
    };
}
geometry_parts_methods!(PyMultiPoint);
geometry_parts_methods!(PyMultiLineString);
geometry_parts_methods!(PyMultiPolygon);
geometry_parts_methods!(PyGeometryCollection);

impl PyGeometry {
    pub(crate) const fn with_row_products(
        shape: Arc<ShapeData>,
        frame: Frame,
        row_products: RowProducts,
        product_row: usize,
    ) -> Self {
        Self {
            shape,
            row_products,
            product_row,
            frame,
        }
    }

    pub(crate) fn frame_cache(&self) -> Arc<crate::geometry::FrameDependentCaches> {
        self.row_products.frame_cache(self.product_row)
    }

    pub(crate) fn prepared_plan(&self) -> Arc<PreparedPlan> {
        let Ok(plan) = self.row_products.prepared_plan(self.product_row, || {
            Ok::<_, std::convert::Infallible>(Arc::clone(&self.shape))
        });
        plan
    }

    pub(crate) fn cached_prepared_plan(&self) -> Option<Arc<PreparedPlan>> {
        self.row_products.cached_prepared_plan(self.product_row)
    }

    // Borrowing frame accessors keep internal kernels on the canonical `Frame`
    // representation while Python-facing getters wrap or clone public values.

    /// The CRS as a borrow — the internal counterpart to the `crs` getter
    /// (which clones into a `PyCrs` for Python). Replaces `self.crs_ref()`.
    pub(crate) const fn crs_ref(&self) -> Option<&Crs> {
        self.frame.crs_ref()
    }

    /// The CRS code as `&str` — replaces `self.crs_str()`.
    pub(crate) fn crs_str(&self) -> Option<&str> {
        self.frame.crs_str()
    }

    /// New geometry with an explicit CRS and coordinate epoch (and a fresh
    /// bounds cache). Prefer [`with_shape`](Self::with_shape) when the CRS and
    /// epoch are inherited from an existing geometry.
    pub(crate) fn with_epoch(
        shape: impl crate::geometry::IntoSharedShape,
        crs: Option<Crs>,
        epoch: Option<f64>,
    ) -> crate::Result<Self> {
        Ok(Self {
            shape: shape.into_shared(),
            row_products: RowProducts::fresh(1),
            product_row: 0,
            frame: Frame::new(crs, epoch)?,
        })
    }

    /// New WGS84 lon/lat geometry with no coordinate epoch.
    pub(crate) fn wgs84(shape: impl crate::geometry::IntoSharedShape) -> Self {
        Self::with_frame(shape, Frame::Crs(wgs84_crs()))
    }

    /// [`wgs84`](Self::wgs84) wrapped as [`Typed`] for return to Python.
    pub(crate) fn typed_wgs84(shape: impl crate::geometry::IntoSharedShape) -> Typed {
        Typed(Self::wgs84(shape))
    }

    /// New geometry from an already-built [`Frame`] with fresh products — the
    /// zero-conversion path for new coordinate storage.
    pub(crate) fn with_frame(shape: impl crate::geometry::IntoSharedShape, frame: Frame) -> Self {
        Self {
            shape: shape.into_shared(),
            row_products: RowProducts::fresh(1),
            product_row: 0,
            frame,
        }
    }

    /// Re-tag immutable coordinates without invalidating their row products.
    pub(crate) fn retag_frame(&self, frame: Frame) -> Self {
        Self {
            shape: Arc::clone(&self.shape),
            row_products: self.row_products.clone(),
            product_row: self.product_row,
            frame,
        }
    }

    /// Re-tag the CRS, keeping any epoch — a construction-time mutation (the
    /// geometry is `frozen` to Python; these run before it is exposed).
    pub(crate) fn set_crs_keep_epoch(&mut self, crs: Option<Crs>) -> crate::Result<()> {
        self.frame = Frame::new(crs, self.frame.epoch())?;
        Ok(())
    }

    /// Re-tag the epoch, keeping the CRS — see [`set_crs_keep_epoch`].
    pub(crate) fn set_epoch_keep_crs(&mut self, epoch: Option<f64>) -> crate::Result<()> {
        self.frame = Frame::new(self.frame.crs_owned(), epoch)?;
        Ok(())
    }

    /// New geometry that keeps this one's CRS and epoch but swaps in `shape`.
    pub(crate) fn with_shape(&self, shape: impl crate::geometry::IntoSharedShape) -> Self {
        Self::with_frame(shape, self.frame.clone())
    }

    pub(crate) fn with_prepared_plan(&self, plan: Arc<PreparedPlan>) -> Self {
        let plan = self
            .row_products
            .install_prepared_plan(self.product_row, plan);
        Self {
            shape: Arc::clone(plan.shape_arc()),
            row_products: self.row_products.clone(),
            product_row: self.product_row,
            frame: self.frame.clone(),
        }
    }

    pub(crate) fn from_prepared_plan(plan: Arc<PreparedPlan>, frame: Frame) -> Self {
        Self {
            shape: Arc::clone(plan.shape_arc()),
            row_products: RowProducts::fresh(1),
            product_row: 0,
            frame,
        }
    }

    /// Like [`with_shape`](Self::with_shape) but wrapped as [`Typed`] for
    /// return to Python as the leaf subclass matching `shape` (e.g.
    /// `centroid` → Point).
    pub(crate) fn typed_shape(&self, shape: impl crate::geometry::IntoSharedShape) -> Typed {
        Typed(self.with_shape(shape))
    }
}
