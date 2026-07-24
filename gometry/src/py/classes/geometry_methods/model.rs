#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::types::PySequence;

use crate::geometry::FrameDependentCaches;
use crate::py::row::{
    RowContainer, RowIndexOrSlice, RowIterState, collect_slice_rows, parse_row_index_or_slice,
};
use crate::{Crs, Frame, GeometryKind, HeapSize, ShapeData, *};

/// An immutable geometry with optional CRS.
///
/// The frozen scalar value at the heart of gometry: a point, linestring,
/// polygon, multi-part, or collection, carrying its coordinate
/// dimensionality (XY/XYZ/XYM/XYZM) and an optional CRS + epoch frame.
/// Construct with the leaf classes (``Point(...)``, ``LineString(...)``) or
/// parsers (``from_wkt(...)``),
/// inspect it (``geom.bounds``, ``geom.coords``), relate it
/// (``contains(geom, other)``), measure it (``geom.area`` — meters when
/// a CRS is set), and derive from it (``geom.buffer(10.0)``); every
/// operation returns a new geometry. Instances are one of the typed subclasses
/// (``Point``, ``LineString``, ``Polygon``, ...), so ``isinstance`` narrows.
#[pyclass(
    name = "Geometry",
    module = "gometry",
    frozen,
    subclass,
    weakref,
    skip_from_py_object
)]
#[derive(Clone, Debug)]
pub struct PyGeometry {
    // Frozen, immutable geometry + its lazily-built prepared state (bounds,
    // distance working set, point tester) — shared behind one `Arc` so
    // cloning a `PyGeometry` (array element access, broadcast operands,
    // detached work) is a cheap refcount bump AND every clone shares the
    // amortized indexes. Deref chains to `Shape` for method calls;
    // pattern-matches use `geometry.shape.shape()`.
    pub shape: Arc<ShapeData>,
    /// Prepared products whose interpretation depends on `frame`. Clones of
    /// the same framed value share it; every retag creates a fresh sidecar.
    pub(crate) frame_cache: Arc<FrameDependentCaches>,
    /// CRS + coordinate epoch as one frame — the `epoch ⟹ crs` invariant lives
    /// in the type (`Frame` has no epoch-without-CRS variant), not a runtime
    /// guard.
    pub(crate) frame: Frame,
}

impl HeapSize for PyGeometry {
    fn heap_bytes(&self) -> usize {
        self.shape.shape().coordinate_bytes() + self.frame_cache.heap_bytes()
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
#[pyclass(name = "GeometryParts", module = "gometry", frozen, sequence)]
pub(crate) struct PyGeometryParts {
    geometry: PyGeometry,
}

impl PyGeometryParts {
    fn part_matches(&self, row: usize, item: &Bound<'_, PyAny>) -> bool {
        let Some(other) = exact_geometry(item) else {
            return false;
        };
        self.geometry.crs_ref() == other.crs_ref()
            && self.geometry.epoch() == other.epoch()
            && self
                .geometry
                .shape
                .part_at(row)
                .is_some_and(|part| part == *other.shape.shape())
    }
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
            start, stop, step, ..
        } => {
            let parts: Vec<Typed> = collect_slice_rows(start, stop, step)
                .into_iter()
                .filter_map(|row| geometry.shape.part_at(row))
                .map(|part| geometry.typed_shape(part))
                .collect();
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
    fn nbytes(&self) -> usize {
        self.geometry.shape.shape().coordinate_bytes()
    }

    /// ``sys.getsizeof`` support: this lazy view plus the source geometry's
    /// logical coordinate heap. The view shares the geometry; it does not
    /// materialize individual parts.
    fn __sizeof__(&self) -> usize {
        self.total_size()
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
    fn __contains__(&self, item: &Bound<'_, PyAny>) -> bool {
        (0..self.geometry.shape.part_count()).any(|row| self.part_matches(row, item))
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
    fn index(&self, value: &Bound<'_, PyAny>, start: i64, stop: Option<i64>) -> PyResult<usize> {
        let len = self.geometry.shape.part_count();
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
        if start < stop
            && let Some(row) = (start..stop).find(|&row| self.part_matches(row, value))
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
        (0..self.geometry.shape.part_count())
            .filter(|&row| self.part_matches(row, value))
            .count()
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
        if sequence.len()? != len {
            return false.into_py_any(py);
        }
        for row in 0..len {
            if !self.part_matches(row, &sequence.get_item(row)?) {
                return false.into_py_any(py);
            }
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
#[pyclass(name = "GeometryPartsIterator", module = "gometry", frozen)]
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
                    frame_cache: Arc::clone(&base.frame_cache),
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
        self.geometry.shape.shape().coordinate_bytes()
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
    /// collection geometries expose their members. Use free function ``parts`` for
    /// the materialized `GeometryArray` form.
    ///
    /// Returns
    /// -------
    /// GeometryParts
    #[getter]
    fn parts(&self) -> PyGeometryParts {
        PyGeometryParts {
            geometry: Self {
                shape: Arc::clone(&self.shape),
                frame_cache: Arc::clone(&self.frame_cache),
                frame: self.frame.clone(),
            },
        }
    }
}

#[cfg(test)]
mod frame_cache_ownership_tests {
    use super::*;

    #[test]
    fn scalar_clone_shares_but_retag_gets_fresh_frame_cache() {
        let geometry =
            PyGeometry::with_frame(Shape::Point(Point::new_unchecked_xy(1.0, 2.0)), Frame::None);
        let clone = geometry.clone();
        assert!(Arc::ptr_eq(&geometry.frame_cache, &clone.frame_cache));

        let retagged = PyGeometry::with_frame(
            Arc::clone(&geometry.shape),
            Frame::from_trusted_parts(Some(crs_arc_static("EPSG:4326")), None),
        );
        assert!(Arc::ptr_eq(&geometry.shape, &retagged.shape));
        assert!(!Arc::ptr_eq(&geometry.frame_cache, &retagged.frame_cache));
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
                        frame_cache: Arc::clone(&base.frame_cache),
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
        }
    };
}
geometry_parts_methods!(PyMultiPoint);
geometry_parts_methods!(PyMultiLineString);
geometry_parts_methods!(PyMultiPolygon);
geometry_parts_methods!(PyGeometryCollection);

impl PyGeometry {
    // Constructors take `impl Into<Arc<ShapeData>>` so a caller can pass
    // either a freshly-built `Shape` (wrapped once, fresh caches) or an
    // existing `Arc<ShapeData>` (shared with no coordinate copy AND shared
    // prepared state — e.g. `set_crs`/`with_shape` reuse the parent's
    // payload and its indexes).
    pub fn from_shape_crs(shape: impl Into<Arc<ShapeData>>, crs: Option<Crs>) -> Self {
        Self::with_epoch(shape, crs, None)
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
    pub fn with_epoch(
        shape: impl Into<Arc<ShapeData>>,
        crs: Option<Crs>,
        epoch: Option<f64>,
    ) -> Self {
        Self {
            shape: shape.into(),
            frame_cache: Arc::new(FrameDependentCaches::default()),
            frame: Frame::from_trusted_parts(crs, epoch),
        }
    }

    /// New WGS84 lon/lat geometry with no coordinate epoch.
    pub(crate) fn wgs84(shape: impl Into<Arc<ShapeData>>) -> Self {
        Self::with_epoch(shape, Some(crs_arc_static("EPSG:4326")), None)
    }

    /// New geometry from an already-built [`Frame`] — the zero-conversion path
    /// when the frame is threaded directly (avoids the `from_trusted_parts`
    /// re-match).
    pub(crate) fn with_frame(shape: impl Into<Arc<ShapeData>>, frame: Frame) -> Self {
        Self {
            shape: shape.into(),
            frame_cache: Arc::new(FrameDependentCaches::default()),
            frame,
        }
    }

    /// Re-tag the CRS, keeping any epoch — a construction-time mutation (the
    /// geometry is `frozen` to Python; these run before it is exposed).
    pub(crate) fn set_crs_keep_epoch(&mut self, crs: Option<Crs>) {
        self.frame = Frame::from_trusted_parts(crs, self.frame.epoch());
        self.frame_cache = Arc::new(FrameDependentCaches::default());
    }

    /// Re-tag the epoch, keeping the CRS — see [`set_crs_keep_epoch`].
    pub(crate) fn set_epoch_keep_crs(&mut self, epoch: Option<f64>) {
        self.frame = Frame::from_trusted_parts(self.frame.crs_owned(), epoch);
        self.frame_cache = Arc::new(FrameDependentCaches::default());
    }

    /// New geometry that keeps this one's CRS and epoch but swaps in `shape`.
    pub fn with_shape(&self, shape: impl Into<Arc<ShapeData>>) -> Self {
        Self::with_epoch(shape, self.crs_ref().cloned(), self.epoch())
    }

    /// Like [`with_shape`](Self::with_shape) but wrapped as [`Typed`] for
    /// return to Python as the leaf subclass matching `shape` (e.g.
    /// `centroid` → Point).
    pub fn typed_shape(&self, shape: impl Into<Arc<ShapeData>>) -> Typed {
        Typed(self.with_shape(shape))
    }

    /// Like [`with_epoch`](Self::with_epoch) but wrapped as [`Typed`] for
    /// return to Python (used where the CRS/epoch change, e.g. `to_crs`,
    /// `set_crs`).
    pub fn typed_with_epoch(
        shape: impl Into<Arc<ShapeData>>,
        crs: Option<Crs>,
        epoch: Option<f64>,
    ) -> Typed {
        Typed(Self::with_epoch(shape, crs, epoch))
    }
}
