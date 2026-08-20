#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Predicate broadcast dispatch: the scalar/array operand routing over the
//! unified [`Predicate`] table (`crate::predicates::engine`), plus the string/pattern
//! relate broadcasts and the XY point-coordinate fast path. The generic value
//! cores live in the parent `broadcast`.

use std::simd::cmp::SimdPartialEq as _;

use pyo3::types::PyAny;
use pyo3::{IntoPyObject as _, IntoPyObjectExt as _};

use crate::array::MissingMask;
use crate::boundary::metadata::Frame;
use crate::broadcast::{
    Arc, Bound, Bounds, CollectRows as _, CoordSeq, CoordinateInput, EmptyKind,
    GeometryArrayStorage, GeometryErrorKind, GeometryInput, Point, PredicateSpec, Py, PyErr,
    PyGeometry, PyGeometryArray, PyResult, Python, Shape, ShapeData, ShapeRow,
    broadcast_coordinate_input, broadcast2, classify_input, coordinate_input_with_expected,
    coordinate_sequence_len_hint, expected_geometry_or_array_for, mask_missing, paired_arrays,
    point_batch, py_bool, scalar_vs_shapes,
};
use crate::error::Result;
use crate::geometry::{
    CellCertificate, PointProbeUse, REDUCE_LANES, is_geographic_frame, pair_select_mask,
    point_is_geographic_pole, same_topological_coordinate, topology_coordinate_bits_simd,
    topology_split,
};
use crate::predicates::PyPreparedGeometry;
use crate::predicates::engine::{
    Predicate, geo_binary, geo_split_pair, skip_antimeridian_bounds_gate_row, topology_scalar_pair,
    try_geographic_point_membership,
};
use crate::py::errors::{GeometryError, InvalidGeometryError};
use crate::py::numpy::bool_array;

pub(crate) fn bool_array_mask_missing(
    py: Python<'_>,
    mut values: Vec<bool>,
    missing: Option<&MissingMask>,
) -> PyResult<Py<PyAny>> {
    mask_missing(&mut values, missing, false);
    bool_array(py, values)
}

/// Hoist a split-normalized scalar operand when it geographically crosses
/// ±180 (reuses the shared handle otherwise).
fn antimeridian_scalar_operand(shape: &Arc<ShapeData>, geographic: bool) -> Arc<ShapeData> {
    if geographic && shape.shape().crosses_antimeridian() {
        Arc::new(ShapeData::from(topology_split(shape.shape())))
    } else {
        Arc::clone(shape)
    }
}

/// Whether a classified broadcast operand carries a geographic frame. Lets the
/// `relate`/`relate_pattern` broadcasts derive the [`geo_binary`] gate from the
/// operand frames the generic value cores would otherwise discard.
fn input_is_geographic(value: &Bound<'_, PyAny>) -> bool {
    match classify_input(value) {
        Some(GeometryInput::One(geometry)) => is_geographic_frame(&geometry.frame),
        Some(GeometryInput::Many(array)) => is_geographic_frame(&array.frame),
        None => false,
    }
}

/// Present prepared operands as their underlying geometries to a broadcast
/// implementation. The owned Python values keep the temporary unwrapped
/// operands alive for the duration of the callback.
fn with_prepared_operands(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    operation: impl for<'a> FnOnce(
        &Bound<'a, PyAny>,
        &Bound<'a, PyAny>,
        bool,
        bool,
    ) -> PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let left_prepared = left.cast::<PyPreparedGeometry>().ok();
    let right_prepared = right.cast::<PyPreparedGeometry>().ok();
    let left_geometry = match left_prepared {
        Some(prepared) => Some(prepared.get().geometry.clone().into_pyobject(py)?),
        None => None,
    };
    let right_geometry = match right_prepared {
        Some(prepared) => Some(prepared.get().geometry.clone().into_pyobject(py)?),
        None => None,
    };
    let left = left_geometry
        .as_ref()
        .map_or(left, |geometry| geometry.as_any());
    let right = right_geometry
        .as_ref()
        .map_or(right, |geometry| geometry.as_any());
    operation(
        left,
        right,
        left_prepared.is_some(),
        right_prepared.is_some(),
    )
}

/// Point-in-container membership for one [`ShapeRow`], mirroring
/// [`point_batch_eval`] without materializing a `ShapeData` handle.
fn row_point_membership(
    row: ShapeRow<'_>,
    point: Point,
    spec: &PredicateSpec,
    container_is_left: bool,
    geographic: bool,
    // The row's already-computed crossing verdict (`geographic && crosses`),
    // shared with the caller's bounds-gate skip so the ring walk runs once.
    row_crosses: bool,
) -> bool {
    if geographic {
        let container = crate::array::PreparedRow::transient(row);
        if let Some(verdict) =
            try_geographic_point_membership(spec, container.shape(), point, container_is_left)
        {
            return verdict;
        }
        if row_crosses {
            // `topology_scalar_pair` split-normalizes the crossing container
            // internally (the pole fast path already ran above).
            let point = ShapeData::from(Shape::Point(point));
            return if container_is_left {
                topology_scalar_pair(spec, &container, &point, true)
            } else {
                topology_scalar_pair(spec, &point, &container, true)
            };
        }
        return row_point_membership_fast(&container, point, spec, container_is_left);
    }
    row_point_membership_fast_row(row, point, spec, container_is_left)
}

fn row_point_membership_fast_row(
    row: ShapeRow<'_>,
    point: Point,
    spec: &PredicateSpec,
    container_is_left: bool,
) -> bool {
    let container = crate::array::PreparedRow::transient(row);
    row_point_membership_fast(&container, point, spec, container_is_left)
}

fn row_point_membership_fast(
    container: &ShapeData,
    point: Point,
    spec: &PredicateSpec,
    container_is_left: bool,
) -> bool {
    if container_is_left {
        match spec.predicate {
            Predicate::Contains | Predicate::ContainsProperly => container.contains_point(point),
            Predicate::Covers | Predicate::CoveredBy | Predicate::Intersects => {
                container.covers_point(point)
            },
            Predicate::Disjoint => !container.covers_point(point),
            _ => spec.right_point.expect("caller gates right_point")(container.shape(), point),
        }
    } else {
        match spec.predicate {
            Predicate::Within => container.contains_point(point),
            Predicate::CoveredBy | Predicate::Intersects => container.covers_point(point),
            Predicate::Disjoint => !container.covers_point(point),
            _ => spec.left_point.expect("caller gates left_point")(point, container.shape()),
        }
    }
}

/// Broadcast one named predicate over `Geometry`/`GeometryArray` operands,
/// returning bool or ndarray.
///
/// The four operand shapes route through one
/// policy: point fast kernels when an operand actually is a point (scalar
/// `Point` or packed point storage), then the batch engine
/// ([`scalar_vs_shapes`]) which amortizes a cached or prepared fixed operand.
pub(crate) fn predicate_broadcast(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    predicate: Predicate,
) -> PyResult<Py<PyAny>> {
    with_prepared_operands(
        py,
        left,
        right,
        |left, right, left_prepared, right_prepared| {
            crate::dispatch::dispatch_predicate(
                py,
                left,
                right,
                predicate,
                left_prepared,
                right_prepared,
            )
        },
    )
}

/// One fixed `scalar` operand against every element of `array`.
///
/// `scalar_is_left` names the operand order (`scalar OP element` vs
/// `element OP scalar`). Frame compatibility is checked once at the array
/// level; evaluation runs with the GIL released.
pub(crate) fn predicate_scalar_vs_array(
    py: Python<'_>,
    spec: &PredicateSpec,
    scalar: &PyGeometry,
    array: &PyGeometryArray,
    scalar_is_left: bool,
    scalar_prepared: bool,
) -> PyResult<Py<PyAny>> {
    Frame::compatible_parts(
        scalar.crs_ref(),
        scalar.epoch(),
        array.crs_ref(),
        array.epoch(),
        spec.token,
    )?;
    let geographic = is_geographic_frame(&scalar.frame) || is_geographic_frame(&array.frame);
    let spec = *spec;
    let missing = array.missing().cloned();
    // Point fast kernels apply when an operand actually is a point: packed
    // point storage reads its column with no per-row materialization, and a
    // scalar `Point` answers through the opposite-side kernel.
    let array_has_point_kernel = if scalar_is_left {
        spec.right_point.is_some()
    } else {
        spec.left_point.is_some()
    };
    let scalar_crosses = geographic && scalar.shape.shape().crosses_antimeridian();
    if array_has_point_kernel
        && !scalar_crosses
        && let Some(points) = array.storage().point_rows()
        && !(geographic
            && points
                .iter()
                .any(|point| point_is_geographic_pole(point).is_some()))
    {
        // A packed pole probe must take the same original-shape membership
        // route as a scalar pole: longitude is a spelling there, so the batch
        // point tester may not establish a planar negative first.
        let shape = Arc::clone(&scalar.shape);
        return bool_array_mask_missing(
            py,
            py.detach(move || {
                // Cached band-indexed raycaster via `point_batch`: built once
                // per handle, each probe scans only its Y-band.
                point_batch(&spec, &shape, &points, scalar_is_left, scalar_prepared)
                    .expect("array-side point kernel checked above")
            }),
            missing.as_ref(),
        );
    }

    let scalar_has_point_kernel = if scalar_is_left {
        spec.left_point.is_some()
    } else {
        spec.right_point.is_some()
    };
    if scalar_has_point_kernel && let Shape::Point(point) = scalar.shape.shape() {
        let point = *point;
        let shapes = Arc::clone(array.storage_arc());
        let bounds = array.cached_element_bounds();
        return bool_array_mask_missing(
            py,
            py.detach(move || {
                shapes
                    .iter_rows()
                    .enumerate()
                    .map(|(index, row)| {
                        let row_crosses = skip_antimeridian_bounds_gate_row(geographic, row);
                        // A geographic pole probe is structurally like a seam
                        // probe: its Cartesian X is only a spelling, so planar
                        // row bounds cannot establish a negative before the
                        // pole-aware membership path has run.
                        let pole_probe = geographic && point_is_geographic_pole(point).is_some();
                        if !row_crosses
                            && !pole_probe
                            && let Some(row_bounds) =
                                bounds.as_ref().and_then(|cached| cached[index])
                        {
                            let point_bounds = Bounds::from_point(point);
                            let (left_bounds, right_bounds) = if scalar_is_left {
                                (point_bounds, row_bounds)
                            } else {
                                (row_bounds, point_bounds)
                            };
                            if let Some(verdict) = spec.bounds_gate(left_bounds, right_bounds) {
                                return verdict;
                            }
                        }
                        row_point_membership(
                            row,
                            point,
                            &spec,
                            !scalar_is_left,
                            geographic,
                            row_crosses,
                        )
                    })
                    .collect::<Vec<_>>()
            }),
            missing.as_ref(),
        );
    }
    let shape = Arc::clone(&scalar.shape);
    let array = array.clone();
    bool_array_mask_missing(
        py,
        py.detach(move || {
            scalar_vs_shapes(
                &spec,
                &shape,
                array.storage().iter_rows().enumerate(),
                scalar_is_left,
                scalar_prepared,
                Some(&array),
                geographic,
            )
        }),
        missing.as_ref(),
    )
}

/// Equal-length arrays, evaluated pairwise (packed point columns read
/// directly; mixed rows go through the per-pair kernel with its point arms).
#[expect(
    clippy::too_many_lines,
    reason = "cohesive kernel; splitting obscures the algorithm"
)]
pub(crate) fn predicate_pairwise_arrays(
    py: Python<'_>,
    spec: &PredicateSpec,
    left: &PyGeometryArray,
    right: &PyGeometryArray,
) -> PyResult<Py<PyAny>> {
    let (left_shapes, right_shapes) = paired_arrays(left, right, spec.token)?;
    let geographic = is_geographic_frame(&left.frame) || is_geographic_frame(&right.frame);
    let spec = *spec;
    let missing = crate::array::missing::union_pair(left.missing(), right.missing());
    // Missing rows are placeholders, not empty geometries.  Decide this
    // before any packed fast path, bounds cache, or row materialization so a
    // null pair cannot enter an exact topology kernel.
    if missing.is_some() {
        let left_array = left.clone();
        let right_array = right.clone();
        return bool_array_mask_missing(
            py,
            py.detach(move || {
                left_shapes
                    .iter_rows()
                    .zip(right_shapes.iter_rows())
                    .enumerate()
                    .map(|(index, (left, right))| {
                        if left_array.is_row_missing(index) || right_array.is_row_missing(index) {
                            return false;
                        }
                        let left = left_array.prepared_row(index, left);
                        let right = right_array.prepared_row(index, right);
                        topology_scalar_pair(&spec, &left, &right, geographic)
                    })
                    .collect::<Vec<_>>()
            }),
            missing.as_ref(),
        );
    }
    // Areal × areal CROSSES is FALSE by OGC definition (crosses needs the
    // intersection's dimension strictly below BOTH operands'; two areas cannot).
    // Answer the whole vector at once — no per-element kernel (shapely is
    // vectorized here too).
    if spec.predicate == Predicate::Crosses
        && matches!(left.storage(), GeometryArrayStorage::Polygons { .. })
        && matches!(right.storage(), GeometryArrayStorage::Polygons { .. })
    {
        return bool_array_mask_missing(py, vec![false; left.storage().len()], missing.as_ref());
    }
    // Point × point: both operands are contiguous packed point columns, so each
    // pair is a pure XY equality — no per-row `ShapeData`/`Arc` materialization
    // and no per-row bounds. Mirrors the metrics array path's point-column lane.
    // (Mapped/selected point columns lack `xy_columns` and fall through to the
    // general lane below, which is correct, just not column-direct.)
    if let (Some(_), Some(_)) = (left.storage().point_rows(), right.storage().point_rows())
        && let Some(verdicts) = left.pair_packed_points_detached(py, right, {
            let predicate = spec.predicate;
            move |lx, ly, rx, ry| {
                let mut out = vec![false; lx.len()];
                pair_select_mask(
                    lx,
                    ly,
                    rx,
                    ry,
                    &mut out,
                    |lxi, lyi, rxi, ryi| {
                        predicate.point_pair(
                            same_topological_coordinate(lxi, rxi)
                                && same_topological_coordinate(lyi, ryi),
                        )
                    },
                    |lx_chunk, ly_chunk, rx_chunk, ry_chunk| {
                        let same = topology_coordinate_bits_simd(lx_chunk)
                            .simd_eq(topology_coordinate_bits_simd(rx_chunk))
                            & topology_coordinate_bits_simd(ly_chunk)
                                .simd_eq(topology_coordinate_bits_simd(ry_chunk));
                        match predicate {
                            Predicate::Disjoint => !same,
                            Predicate::Touches | Predicate::Crosses | Predicate::Overlaps => {
                                std::simd::Mask::<i64, REDUCE_LANES>::splat(false)
                            },
                            _ => same,
                        }
                    },
                );
                Ok(out)
            }
        })?
    {
        return bool_array_mask_missing(py, verdicts, missing.as_ref());
    }
    // Point fast paths: one operand is a packed point column, so each pair is a
    // point-in-shape test through the spec's point kernel. The OTHER side's
    // cached element envelope bounds-gates every pair first — a point outside
    // the container's box settles each point-in-polygon predicate in O(1)
    // (intersects/covers/contains → false), so the disjoint pairs that dominate
    // a scattered point query skip the raycast AND the row materialization
    // (`iter_rows` stays lazy). Mirrors the pairwise fallback's gate and
    // shapely's envelope short-circuit.
    if let (Some(_), Some(points)) = (spec.right_point, right.storage().point_rows()) {
        let left_bounds = left.cached_element_bounds();
        return bool_array_mask_missing(
            py,
            py.detach(move || {
                left_shapes
                    .iter_rows()
                    .zip(points.iter())
                    .enumerate()
                    .map(|(index, (left, point))| {
                        let row_crosses = skip_antimeridian_bounds_gate_row(geographic, left);
                        // At a physical pole longitude is only a spelling:
                        // the planar envelope may not reject it before the
                        // geographic membership kernel has answered.
                        let pole_probe = geographic && point_is_geographic_pole(point).is_some();
                        if !row_crosses
                            && !pole_probe
                            && let Some(left_bounds) =
                                left_bounds.as_ref().and_then(|cached| cached[index])
                            && let Some(verdict) =
                                spec.bounds_gate(left_bounds, Bounds::from_point(point))
                        {
                            return verdict;
                        }
                        row_point_membership(left, point, &spec, true, geographic, row_crosses)
                    })
                    .collect::<Vec<_>>()
            }),
            missing.as_ref(),
        );
    }
    if let (Some(_), Some(points)) = (spec.left_point, left.storage().point_rows()) {
        let right_bounds = right.cached_element_bounds();
        return bool_array_mask_missing(
            py,
            py.detach(move || {
                points
                    .iter()
                    .zip(right_shapes.iter_rows())
                    .enumerate()
                    .map(|(index, (point, right))| {
                        let row_crosses = skip_antimeridian_bounds_gate_row(geographic, right);
                        // See the paired right-point lane above: the
                        // longitude of a physical pole cannot establish a
                        // planar negative.
                        let pole_probe = geographic && point_is_geographic_pole(point).is_some();
                        if !row_crosses
                            && !pole_probe
                            && let Some(right_bounds) =
                                right_bounds.as_ref().and_then(|cached| cached[index])
                            && let Some(verdict) =
                                spec.bounds_gate(Bounds::from_point(point), right_bounds)
                        {
                            return verdict;
                        }
                        row_point_membership(right, point, &spec, false, geographic, row_crosses)
                    })
                    .collect::<Vec<_>>()
            }),
            missing.as_ref(),
        );
    }
    // The unified predicate table owns all bounds-only short-circuits. The
    // per-row boxes are SIMD `column_minmax` straight off the packed columns,
    // so a refuted pair skips materialization + the full kernel while preserving
    // the scalar/prepared lane's single source of truth.
    if let (Some(left_bounds), Some(right_bounds)) =
        (left.cached_element_bounds(), right.cached_element_bounds())
    {
        let left_array = left.clone();
        let right_array = right.clone();
        return bool_array_mask_missing(
            py,
            py.detach(move || {
                left_shapes
                    .iter_rows()
                    .zip(right_shapes.iter_rows())
                    .enumerate()
                    .map(|(index, (left, right))| {
                        // A mixed array can carry a point row without taking
                        // either packed-point fast lane.  Preserve the same
                        // fail-open pole rule there as well.
                        let pole_probe = geographic
                            && (left.with_shape(|shape| {
                                matches!(shape, Shape::Point(point) if point_is_geographic_pole(*point).is_some())
                            }) || right.with_shape(|shape| {
                                matches!(shape, Shape::Point(point) if point_is_geographic_pole(*point).is_some())
                            }));
                        if !pole_probe
                            && !skip_antimeridian_bounds_gate_row(geographic, left)
                            && !skip_antimeridian_bounds_gate_row(geographic, right)
                            && let (Some(left_bounds), Some(right_bounds)) =
                                (left_bounds[index], right_bounds[index])
                            && let Some(verdict) = spec.bounds_gate(left_bounds, right_bounds)
                        {
                            return verdict;
                        }
                        let left = left_array.prepared_row(index, left);
                        let right = right_array.prepared_row(index, right);
                        topology_scalar_pair(&spec, &left, &right, geographic)
                    })
                    .collect::<Vec<_>>()
            }),
            missing.as_ref(),
        );
    }
    let left_array = left.clone();
    let right_array = right.clone();
    bool_array_mask_missing(
        py,
        py.detach(move || {
            left_shapes
                .iter_rows()
                .zip(right_shapes.iter_rows())
                .enumerate()
                .map(|(index, (left, right))| {
                    let left = left_array.prepared_row(index, left);
                    let right = right_array.prepared_row(index, right);
                    topology_scalar_pair(&spec, &left, &right, geographic)
                })
                .collect::<Vec<_>>()
        }),
        missing.as_ref(),
    )
}

pub(crate) fn multipoint_splitter_from_array(splitters: &PyGeometryArray) -> Result<Shape> {
    // Packed point arrays are already a uniform-axis column — clone it directly
    // rather than gathering every row and repacking through
    // `CoordSeq::from_points`.
    if let GeometryArrayStorage::Points { coords, row_map } = splitters.storage() {
        if row_map.is_identity() && !splitters.has_missing() {
            return Ok(Shape::MultiPoint(coords.as_ref().clone()));
        }
        let map = row_map.as_deref();
        let points: Vec<_> = (0..crate::array::point_logical_len(coords, map))
            .filter(|&logical| !splitters.is_row_missing(logical))
            .map(|logical| coords.point_at(crate::array::physical_row(map, logical)))
            .collect();
        return Ok(Shape::MultiPoint(CoordSeq::from_points(&points)));
    }
    let mut points = Vec::new();
    for (missing, shape) in splitters.masked_shape_rows() {
        if missing {
            continue;
        }
        match &*shape {
            Shape::Point(point) => points.push(point.to_xy()),
            Shape::MultiPoint(items) => points.extend(items.iter().map(Point::to_xy)),
            // `POINT EMPTY` is point-like but contributes no split locations.
            Shape::Empty(EmptyKind::Point, _) => {},
            _ => return Err(GeometryErrorKind::PointSplitterRequired.into()),
        }
    }
    Ok(Shape::MultiPoint(points.into()))
}

pub(crate) fn ensure_same_len(left: usize, right: usize) -> PyResult<()> {
    if left == right {
        Ok(())
    } else {
        Err(GeometryError::new_err(format!(
            "GeometryArray operands must have the same length, got {left} and {right}"
        )))
    }
}

/// `relate` matrix-string broadcast with a scalar fast path: a `(scalar,
/// scalar)` pair routes through the cached [`ShapeData::relate`] so repeated
/// calls on the same handles reuse each side's prepared point tester. Array
/// rows stream owned shapes (no persistent handle), so they keep the raw
/// [`Shape::relate`] path.
pub(crate) fn relate_string_broadcast(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    with_prepared_operands(py, left, right, |left, right, _, _| {
        use GeometryInput::One;
        if let (Some(One(left)), Some(One(right))) = (classify_input(left), classify_input(right)) {
            left.frame.compatible(&right.frame, "relate")?;
            let geographic = is_geographic_frame(&left.frame);
            return match geo_split_pair(geographic, left.shape.shape(), right.shape.shape()) {
                Some((left_shape, right_shape)) => {
                    let left = ShapeData::from(left_shape);
                    let right = ShapeData::from(right_shape);
                    ShapeData::relate(&left, &right).into_py_any(py)
                },
                None => ShapeData::relate(&left.shape, &right.shape).into_py_any(py),
            };
        }
        let geographic = input_is_geographic(left) || input_is_geographic(right);
        broadcast2(py, left, right, "relate", move |left, right| {
            Ok(geo_binary(geographic, left, right, Shape::relate))
        })
    })
}

fn invalid_de9im_pattern_err(pattern: &str) -> PyErr {
    GeometryErrorKind::invalid_de9im_pattern(format!(
        "invalid DE-9IM pattern {pattern:?}; expected 9 characters of \
         'T', 'F', '0', '1', '2', or '*'"
    ))
    .into()
}

/// ``relate_pattern`` broadcast: compile the DE-9IM pattern once per call, not
/// once per array row.
pub(crate) fn relate_pattern_broadcast(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    pattern: &str,
) -> PyResult<Py<PyAny>> {
    use GeometryInput::One;
    let canonical: Option<fn(&Shape, &Shape) -> bool> = match pattern {
        "FF*FF****" => Some(|left, right| !left.intersects(right)),
        "T*****FF*" => Some(Shape::contains),
        "T*F**F***" => Some(Shape::within),
        "T*F**FFF*" => Some(Shape::equals),
        _ => None,
    };
    with_prepared_operands(py, left, right, |left, right, _, _| {
        if let Some(kernel) = canonical {
            if let (Some(One(left)), Some(One(right))) =
                (classify_input(left), classify_input(right))
            {
                left.frame.compatible(&right.frame, "relate_pattern")?;
                let geographic = is_geographic_frame(&left.frame);
                return match geo_split_pair(geographic, left.shape.shape(), right.shape.shape()) {
                    Some((left_shape, right_shape)) => {
                        Ok(py_bool(py, kernel(&left_shape, &right_shape)))
                    },
                    None => Ok(py_bool(py, kernel(left.shape.shape(), right.shape.shape()))),
                };
            }
            let geographic = input_is_geographic(left) || input_is_geographic(right);
            return broadcast2(py, left, right, "relate_pattern", move |left, right| {
                Ok(geo_binary(geographic, left, right, kernel))
            });
        }
        let pattern_owned = pattern.to_owned();
        let compiled = crate::geometry::CompiledPattern::new(&pattern_owned)
            .ok_or_else(|| invalid_de9im_pattern_err(pattern))?;
        if let (Some(One(left)), Some(One(right))) = (classify_input(left), classify_input(right)) {
            left.frame.compatible(&right.frame, "relate_pattern")?;
            let geographic = is_geographic_frame(&left.frame);
            return match geo_split_pair(geographic, left.shape.shape(), right.shape.shape()) {
                Some((left_shape, right_shape)) => {
                    let left = ShapeData::from(left_shape);
                    let right = ShapeData::from(right_shape);
                    Ok(py_bool(py, left.relate_pattern_compiled(&right, compiled)))
                },
                None => Ok(py_bool(
                    py,
                    left.shape.relate_pattern_compiled(&right.shape, compiled),
                )),
            };
        }
        let geographic = input_is_geographic(left) || input_is_geographic(right);
        broadcast2(py, left, right, "relate_pattern", move |left, right| {
            Ok(geo_binary(geographic, left, right, |left, right| {
                Shape::relate_pattern_compiled(left, right, compiled)
            }))
        })
    })
}

/// The shared `contains_xy`/`intersects_xy` engine. `inclusive` selects the
/// boundary-inclusive `covers_point` kernel (`intersects_xy`) over the strict
/// `contains_point` one.
pub(crate) fn xy_predicate(
    py: Python<'_>,
    geometry: &Bound<'_, PyAny>,
    x: &Bound<'_, PyAny>,
    y: &Bound<'_, PyAny>,
    inclusive: bool,
) -> PyResult<Py<PyAny>> {
    if let Ok(prepared) = geometry.cast::<PyPreparedGeometry>() {
        // Preserve the prepared operand's public policy while reusing the
        // coordinate parser and scalar XY kernel.
        return xy_predicate_geometry(py, &prepared.get().geometry, x, y, inclusive, true);
    }
    let input = classify_input(geometry).ok_or_else(|| expected_geometry_or_array_for(geometry))?;
    // Streaming owner: pin bare one-shot iterators with the sibling length when
    // known so each coordinate column is drained once (not twice via independent
    // `coordinate_input` + broadcast).
    let x_hint = coordinate_sequence_len_hint(x);
    let y_hint = coordinate_sequence_len_hint(y);
    let mut x = coordinate_input_with_expected(py, x, "x", y_hint, &|| "x must be finite".into())?;
    let mut y = coordinate_input_with_expected(py, y, "y", x_hint, &|| "y must be finite".into())?;
    if x.values.len() != y.values.len() && !x.scalar && !y.scalar {
        return Err(InvalidGeometryError::new_err(format!(
            "x and y must have the same length, got {} and {}",
            x.values.len(),
            y.values.len(),
        )));
    }
    match input {
        GeometryInput::One(geometry) => {
            xy_predicate_geometry_inputs(py, geometry, x, y, inclusive, false)
        },
        GeometryInput::Many(array) => {
            let len = array.storage().len();
            broadcast_coordinate_input(&mut x, len, "GeometryArray and x")?;
            broadcast_coordinate_input(&mut y, len, "GeometryArray and y")?;
            xy_predicate_array_values(py, array, x.values, y.values, inclusive)
        },
    }
}

/// Scalar-geometry entry used by both the public free functions and prepared
/// geometry methods. Keeping coordinate parsing/broadcasting here avoids
/// manufacturing a temporary Python geometry merely to re-enter dispatch.
pub(crate) fn xy_predicate_geometry(
    py: Python<'_>,
    geometry: &PyGeometry,
    x: &Bound<'_, PyAny>,
    y: &Bound<'_, PyAny>,
    inclusive: bool,
    scalar_prepared: bool,
) -> PyResult<Py<PyAny>> {
    let x_hint = coordinate_sequence_len_hint(x);
    let y_hint = coordinate_sequence_len_hint(y);
    let x = coordinate_input_with_expected(py, x, "x", y_hint, &|| "x must be finite".into())?;
    let y = coordinate_input_with_expected(py, y, "y", x_hint, &|| "y must be finite".into())?;
    xy_predicate_geometry_inputs(py, geometry, x, y, inclusive, scalar_prepared)
}

fn xy_predicate_geometry_inputs(
    py: Python<'_>,
    geometry: &PyGeometry,
    mut x: CoordinateInput,
    mut y: CoordinateInput,
    inclusive: bool,
    scalar_prepared: bool,
) -> PyResult<Py<PyAny>> {
    if x.values.len() != y.values.len() && !x.scalar && !y.scalar {
        return Err(InvalidGeometryError::new_err(format!(
            "x and y must have the same length, got {} and {}",
            x.values.len(),
            y.values.len(),
        )));
    }
    let scalar = x.scalar && y.scalar;
    let len = x.values.len().max(y.values.len());
    broadcast_coordinate_input(&mut x, len, "x and y")?;
    broadcast_coordinate_input(&mut y, len, "x and y")?;
    xy_predicate_values(
        py,
        geometry,
        x.values,
        y.values,
        scalar,
        inclusive,
        scalar_prepared,
    )
}

fn xy_predicate_array_values(
    py: Python<'_>,
    array: &PyGeometryArray,
    x: Vec<f64>,
    y: Vec<f64>,
    inclusive: bool,
) -> PyResult<Py<PyAny>> {
    let array = array.clone();
    let geographic = is_geographic_frame(&array.frame);
    let spec = if inclusive {
        Predicate::Intersects.spec()
    } else {
        Predicate::Contains.spec()
    };
    let values = py
        .detach(move || {
            array
                .masked_storage_rows()
                .zip(x.into_iter().zip(y))
                .map(|((missing, row), (x, y))| {
                    if missing {
                        return Ok(false);
                    }
                    let point = Point::new_unchecked_xy(x, y);
                    let crosses = geographic && row.with_shape(Shape::crosses_antimeridian);
                    Ok(row_point_membership(
                        row, point, &spec, true, geographic, crosses,
                    ))
                })
                .collect_rows()
        })
        .map_err(super::rows_err)?;
    bool_array(py, values)
}

/// `contains_xy`/`intersects_xy` after the caller has parsed, broadcast, and
/// optionally domain-validated the coordinate columns.
fn xy_exact_membership(shape: &ShapeData, point: Point, inclusive: bool) -> bool {
    if inclusive {
        shape.covers_point(point)
    } else {
        shape.contains_point(point)
    }
}

fn xy_array_values(
    x: Vec<f64>,
    y: Vec<f64>,
    len: usize,
    shape: &Arc<ShapeData>,
    original: &Arc<ShapeData>,
    spec: PredicateSpec,
    geographic: bool,
    inclusive: bool,
    scalar_prepared: bool,
) -> Result<Vec<bool>, (usize, crate::error::Error)> {
    let split_transient = !std::ptr::eq(Arc::as_ptr(shape), Arc::as_ptr(original));
    let points: Vec<Point> = x
        .into_iter()
        .zip(y)
        .map(|(x, y)| Point::new_unchecked_xy(x, y))
        .collect();
    let mode = if scalar_prepared {
        PointProbeUse::AcrossCalls
    } else {
        PointProbeUse::OneShot(len)
    };
    if split_transient {
        let tester = shape.point_tester_for(mode);
        return points
            .into_iter()
            .map(|point| {
                if let Some(verdict) =
                    try_geographic_point_membership(&spec, original.shape(), point, true)
                {
                    return Ok(verdict);
                }
                Ok(match tester {
                    Some(tester) if inclusive => tester.covers_point(point),
                    Some(tester) => tester.contains_point(point),
                    None => xy_exact_membership(shape, point, inclusive),
                })
            })
            .collect_rows();
    }
    if let Some(tester) = shape.point_tester_for(mode) {
        let geographic_verdicts: Vec<Option<bool>> = points
            .iter()
            .map(|&point| {
                geographic
                    .then(|| try_geographic_point_membership(&spec, original.shape(), point, true))
                    .flatten()
            })
            .collect();
        if let Some(grid_values) = tester.cell_batch_classify(len, &points) {
            return points
                .into_iter()
                .zip(geographic_verdicts)
                .enumerate()
                .map(|(index, (point, geographic_verdict))| {
                    if let Some(verdict) = geographic_verdict {
                        return Ok(verdict);
                    }
                    if let Some(certificate) = grid_values[index] {
                        return Ok(matches!(certificate, CellCertificate::Interior));
                    }
                    Ok(if inclusive {
                        tester.covers_point(point)
                    } else {
                        tester.contains_point(point)
                    })
                })
                .collect_rows();
        }
        return points
            .into_iter()
            .zip(geographic_verdicts)
            .map(|(point, geographic_verdict)| {
                if let Some(verdict) = geographic_verdict {
                    return Ok(verdict);
                }
                Ok(if inclusive {
                    tester.covers_point(point)
                } else {
                    tester.contains_point(point)
                })
            })
            .collect_rows();
    }

    let bounds = shape.bounds();
    points
        .into_iter()
        .map(|point| {
            if geographic
                && let Some(verdict) =
                    try_geographic_point_membership(&spec, original.shape(), point, true)
            {
                return Ok(verdict);
            }
            if let Some(bounds) = bounds {
                let probe = Bounds::from_point(point);
                if !bounds.contains(probe) {
                    return Ok(false);
                }
            }
            Ok(xy_exact_membership(shape, point, inclusive))
        })
        .collect_rows()
}

pub(crate) fn xy_predicate_values(
    py: Python<'_>,
    geometry: &PyGeometry,
    x: Vec<f64>,
    y: Vec<f64>,
    scalar: bool,
    inclusive: bool,
    scalar_prepared: bool,
) -> PyResult<Py<PyAny>> {
    let len = x.len();
    let spec = if inclusive {
        Predicate::Intersects.spec()
    } else {
        Predicate::Contains.spec()
    };
    let kernel: fn(&Shape, Point) -> bool = if inclusive {
        Shape::covers_point
    } else {
        Shape::contains_point
    };
    // Geographic antimeridian-crossing containers are split-normalized once so
    // the `*_xy` coordinate fast paths agree with the full predicate surface.
    // The raw crossing shape's planar box is the false-middle band (minx>maxx),
    // which inverts both the scalar kernel and the array bounds pre-filter.
    let geographic = is_geographic_frame(&geometry.frame);
    let original = Arc::clone(&geometry.shape);
    if scalar {
        let point = Point::new(x[0], y[0])?;
        if geographic
            && let Some(verdict) =
                try_geographic_point_membership(&spec, original.shape(), point, true)
        {
            return Ok(py_bool(py, verdict));
        }
        let shape = antimeridian_scalar_operand(&geometry.shape, geographic);
        if std::ptr::eq(Arc::as_ptr(&shape), Arc::as_ptr(&original))
            && let Some(tester) = original.point_tester_for(if scalar_prepared {
                PointProbeUse::AcrossCalls
            } else {
                PointProbeUse::OneShot(1)
            })
            && let Some(verdict) =
                crate::predicates::engine::point_batch_eval(&spec, tester, point, true)
        {
            return Ok(py_bool(py, verdict));
        }
        return Ok(py_bool(py, kernel(&shape, point)));
    }
    let shape = antimeridian_scalar_operand(&geometry.shape, geographic);
    let result = py
        .detach(move || {
            xy_array_values(
                x,
                y,
                len,
                &shape,
                &original,
                spec,
                geographic,
                inclusive,
                scalar_prepared,
            )
        })
        .map_err(super::rows_err)?;
    bool_array(py, result)
}

#[cfg(test)]
mod geographic_tests {

    use pyo3::types::PyAnyMethods as _;

    use super::*;
    use crate::boundary::{Frame, crs_arc_static};
    use crate::geometry::{CoordSeq, Polygon, Ring};

    #[test]
    fn geographic_crossing_xy_agrees_with_scalar_oracle_at_seam_and_pole() {
        crate::test_support::initialize_python();
        let shape = Shape::Polygon(Polygon::new(
            Ring::from_trusted_closed(CoordSeq::from(vec![
                Point::new_unchecked_xy(170.0, 80.0),
                Point::new_unchecked_xy(-170.0, 80.0),
                Point::new_unchecked_xy(-170.0, 90.0),
                Point::new_unchecked_xy(170.0, 90.0),
            ])),
            Vec::new(),
        ));
        let frame = Frame::new(Some(crs_arc_static("EPSG:4326")), None)
            .expect("EPSG:4326 is a valid geographic frame");
        let geometry = PyGeometry::with_frame(shape, frame);

        // The first four points sit on or immediately beside both longitude
        // representations of the seam; the rest test the high-latitude cap.
        let probes = [
            (180.0, 85.0),
            (-180.0, 85.0),
            (179.999, 85.0),
            (-179.999, 85.0),
            (0.0, 90.0),
            (90.0, 90.0),
            (-90.0, 90.0),
        ];
        let x = probes.iter().map(|&(x, _)| x).collect();
        let y = probes.iter().map(|&(_, y)| y).collect();
        let spec = Predicate::Intersects.spec();
        let expected: Vec<bool> = probes
            .iter()
            .map(|&(x, y)| {
                try_geographic_point_membership(
                    &spec,
                    geometry.shape.shape(),
                    Point::new_unchecked_xy(x, y),
                    true,
                )
                .unwrap_or_else(|| {
                    topology_split(geometry.shape.shape())
                        .covers_point(Point::new_unchecked_xy(x, y))
                })
            })
            .collect();

        let actual = Python::attach(|py| {
            let result = xy_predicate_values(py, &geometry, x, y, false, true, false)
                .expect("geographic XY predicate succeeds");
            result
                .bind(py)
                .call_method0("tolist")
                .expect("predicate returns an array with tolist")
                .extract::<Vec<bool>>()
                .expect("predicate array contains booleans")
        });

        assert_eq!(
            actual, expected,
            "geographic XY result differs from scalar oracle"
        );
    }
}
