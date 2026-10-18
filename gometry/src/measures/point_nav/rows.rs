use super::adapters::geometry_point;
use super::{
    CoordSeq, CoordSeqBuilder, CoordinateAxes, GeometryArrayStorage, GeometryInput,
    LogicalRowIndex, MOrdinate, PackedStorage, Point, PointRows, PresentRowIndex, PyGeometryArray,
    PyResult, Shape, ValidityMask, ValidityRank, ZOrdinate, crs, storage, union_many,
};

pub(super) enum PointOperand<'a> {
    One(Point),
    Compact(&'a PyGeometryArray),
    Many(PointRows<'a>),
}

#[derive(Clone, Copy)]
struct PhysicalPointRow(usize);

impl PhysicalPointRow {
    fn from_present(row_map: storage::RowSelectionRef<'_>, present: PresentRowIndex) -> Self {
        Self(storage::physical_row(row_map, present.get()))
    }

    const fn get(self) -> usize {
        self.0
    }
}

pub(super) enum PreparedPointOperand<'a> {
    One(Point),
    Compact {
        array: &'a PyGeometryArray,
        rank: ValidityRank<'a>,
    },
    Many(PointRows<'a>),
}

impl<'a> PointOperand<'a> {
    pub(super) fn prepare(self) -> PreparedPointOperand<'a> {
        match self {
            Self::One(point) => PreparedPointOperand::One(point),
            Self::Compact(array) => PreparedPointOperand::Compact {
                array,
                rank: array
                    .missing()
                    .expect("compact point rows carry validity")
                    .full_rank(),
            },
            Self::Many(points) => PreparedPointOperand::Many(points),
        }
    }
}

impl PreparedPointOperand<'_> {
    pub(super) fn get(&self, row: usize) -> Point {
        match self {
            Self::One(point) => *point,
            Self::Compact { array, rank } => {
                let present = rank
                    .resolve_present(LogicalRowIndex::new(row))
                    .expect("point kernels read only present compact rows");
                let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) =
                    array.storage()
                else {
                    unreachable!("compact point rows are packed points");
                };
                coords.point_at(PhysicalPointRow::from_present(row_map.as_deref(), present).get())
            },
            Self::Many(points) => points.get(row),
        }
    }
}

pub(super) trait PointRowSource: Send {
    fn point_at(&self, row: usize) -> Point;
}

impl PointRowSource for PreparedPointOperand<'_> {
    fn point_at(&self, row: usize) -> Point {
        self.get(row)
    }
}

pub(super) fn point_from_point_nav_shape(shape: &Shape) -> Point {
    let Shape::Point(point) = shape else {
        unreachable!("point-navigation point fast paths return Point shapes");
    };
    *point
}

pub(super) fn point_nav_missing_point_with_axes(axes: CoordinateAxes) -> Point {
    Point::new_raw_axes(
        f64::NAN,
        f64::NAN,
        ZOrdinate(axes.has_z().then_some(f64::NAN)),
        MOrdinate(axes.has_m().then_some(f64::NAN)),
    )
}

pub(super) fn finish_point_nav_coords(
    builder: CoordSeqBuilder,
    missing: Option<&ValidityMask>,
) -> PyResult<CoordSeq> {
    match missing {
        Some(_) => Ok(builder.finish_with_missing_placeholders()?),
        None => Ok(builder.finish()?),
    }
}

pub(super) fn row_missing<const HAS_MISSING: bool>(
    missing: Option<&ValidityMask>,
    row: usize,
) -> bool {
    if HAS_MISSING {
        missing
            .expect("HAS_MISSING rows carry a mask")
            .is_missing(row)
    } else {
        false
    }
}

pub(super) fn uniform_axes<const HAS_MISSING: bool>(
    len: usize,
    missing: Option<&ValidityMask>,
    mut axes_at: impl FnMut(usize) -> CoordinateAxes,
) -> Option<CoordinateAxes> {
    let mut axes = None;
    for row in 0..len {
        if row_missing::<HAS_MISSING>(missing, row) {
            continue;
        }
        let row_axes = axes_at(row);
        if let Some(seen) = axes {
            if seen != row_axes {
                return None;
            }
        } else {
            axes = Some(row_axes);
        }
    }
    Some(axes.unwrap_or(CoordinateAxes::XY))
}

fn uniform_point_axes<const HAS_MISSING: bool>(
    points: &PointRows<'_>,
    len: usize,
    missing: Option<&ValidityMask>,
) -> Option<CoordinateAxes> {
    uniform_axes::<HAS_MISSING>(len, missing, |row| {
        CoordinateAxes::from_point(points.get(row))
    })
}

pub(super) fn uniform_point_axes_dispatch(
    points: &PointRows<'_>,
    len: usize,
    missing: Option<&ValidityMask>,
) -> Option<CoordinateAxes> {
    if let Some(mask) = missing {
        return uniform_point_axes::<true>(points, len, Some(mask));
    }
    uniform_point_axes::<false>(points, len, None)
}

pub(super) trait PointBinaryF64Kernel: Copy + Send + Sync + pyo3::marker::Ungil {
    fn eval_planar(self, to_metre: f64, left: Point, right: Point) -> PyResult<f64>;
    fn eval_geodesic(
        self,
        metric: &crs::GeodesicMetric,
        left: Point,
        right: Point,
    ) -> PyResult<f64>;

    /// Geodesic array path: resolve the ellipsoid once, then map rows.
    fn eval_rows_geodesic<const HAS_MISSING: bool, P>(
        self,
        metric: &crs::GeodesicMetric,
        len: usize,
        missing: Option<&ValidityMask>,
        points: P,
    ) -> PyResult<Vec<f64>>
    where
        P: FnMut(usize) -> (Point, Point),
    {
        point_binary_f64_rows::<HAS_MISSING, _>(
            self,
            len,
            missing,
            points,
            |kernel, left, right| kernel.eval_geodesic(metric, left, right),
        )
    }
}

fn point_binary_f64_rows<const HAS_MISSING: bool, K: PointBinaryF64Kernel>(
    kernel: K,
    len: usize,
    missing: Option<&ValidityMask>,
    mut points: impl FnMut(usize) -> (Point, Point),
    mut evaluate: impl FnMut(K, Point, Point) -> PyResult<f64>,
) -> PyResult<Vec<f64>> {
    (0..len)
        .map(|row| {
            if row_missing::<HAS_MISSING>(missing, row) {
                return Ok(f64::NAN);
            }
            let (left, right) = points(row);
            evaluate(kernel, left, right).map_err(|err| crate::note_array_row(err, row))
        })
        .collect()
}

pub(super) fn point_binary_f64_eval<K: PointBinaryF64Kernel>(
    kernel: K,
    model: &crs::MetricModel,
    left: Point,
    right: Point,
) -> PyResult<f64> {
    match model {
        crs::MetricModel::Geodesic { .. } => {
            let metric = crs::GeodesicMetric::from_model(model)?;
            kernel.eval_geodesic(&metric, left, right)
        },
        crs::MetricModel::Planar { to_metre } => kernel.eval_planar(to_metre.get(), left, right),
    }
}

pub(super) fn point_binary_f64_rows_dispatch<K: PointBinaryF64Kernel>(
    kernel: K,
    model: &crs::MetricModel,
    len: usize,
    missing: Option<&ValidityMask>,
    points: impl FnMut(usize) -> (Point, Point),
) -> PyResult<Vec<f64>> {
    match model {
        crs::MetricModel::Geodesic { .. } => {
            let metric = crs::GeodesicMetric::from_model(model)?;
            match missing {
                Some(mask) => {
                    kernel.eval_rows_geodesic::<true, _>(&metric, len, Some(mask), points)
                },
                None => kernel.eval_rows_geodesic::<false, _>(&metric, len, None, points),
            }
        },
        crs::MetricModel::Planar { to_metre } => match missing {
            Some(mask) => point_binary_f64_rows::<true, _>(
                kernel,
                len,
                Some(mask),
                points,
                |kernel, left, right| kernel.eval_planar(to_metre.get(), left, right),
            ),
            None => point_binary_f64_rows::<false, _>(
                kernel,
                len,
                None,
                points,
                |kernel, left, right| kernel.eval_planar(to_metre.get(), left, right),
            ),
        },
    }
}

#[derive(Clone, Copy)]
pub(super) struct DistanceRow {
    pub(super) logical: LogicalRowIndex,
    pub(super) distance: f64,
}

pub(super) fn distance_rows(
    distances: &[f64],
    len: usize,
) -> impl ExactSizeIterator<Item = DistanceRow> + '_ {
    distances
        .get(..len)
        .expect("point-between distances cover every logical row")
        .iter()
        .copied()
        .enumerate()
        .map(|(logical, distance)| DistanceRow {
            logical: LogicalRowIndex::new(logical),
            distance,
        })
}

pub(super) trait PointBinaryGeometryKernel:
    Copy + Send + Sync + pyo3::marker::Ungil
{
    fn distances(&self) -> &[f64];

    fn eval_planar(
        self,
        to_metre: f64,
        row: DistanceRow,
        left: Point,
        right: Point,
    ) -> PyResult<Shape>;

    fn eval_geodesic(
        self,
        metric: &crs::GeodesicMetric,
        row: DistanceRow,
        left: Point,
        right: Point,
    ) -> PyResult<Shape>;

    fn output_axes(self, left: Point, right: Point) -> CoordinateAxes;

    /// Geodesic array path writing lon/lat (+Z/M) into a `CoordSeq` with the
    /// ellipsoid resolved once. Default keeps per-row `eval` (rhumb).
    fn coords_rows_geodesic<const HAS_MISSING: bool, P>(
        self,
        metric: &crs::GeodesicMetric,
        len: usize,
        axes: CoordinateAxes,
        missing: Option<&ValidityMask>,
        points: P,
    ) -> PyResult<CoordSeq>
    where
        P: FnMut(usize) -> (Point, Point),
    {
        point_binary_geometry_coords::<HAS_MISSING, _>(
            self,
            len,
            axes,
            missing,
            points,
            |kernel, row, left, right| kernel.eval_geodesic(metric, row, left, right),
        )
    }
}

fn point_binary_geometry_coords<const HAS_MISSING: bool, K: PointBinaryGeometryKernel>(
    kernel: K,
    len: usize,
    axes: CoordinateAxes,
    missing: Option<&ValidityMask>,
    mut points: impl FnMut(usize) -> (Point, Point),
    mut evaluate: impl FnMut(K, DistanceRow, Point, Point) -> PyResult<Shape>,
) -> PyResult<CoordSeq> {
    let mut builder = CoordSeqBuilder::with_capacity(axes, len);
    for row in distance_rows(kernel.distances(), len) {
        let logical = row.logical.get();
        if row_missing::<HAS_MISSING>(missing, logical) {
            builder.push(point_nav_missing_point_with_axes(axes));
        } else {
            let (left, right) = points(logical);
            let shape = evaluate(kernel, row, left, right)
                .map_err(|err| crate::note_array_row(err, logical))?;
            builder.push(point_from_point_nav_shape(&shape));
        }
    }
    finish_point_nav_coords(builder, missing)
}

pub(super) fn point_binary_geometry_eval<K: PointBinaryGeometryKernel>(
    kernel: K,
    model: &crs::MetricModel,
    row: DistanceRow,
    left: Point,
    right: Point,
) -> PyResult<Shape> {
    match model {
        crs::MetricModel::Geodesic { .. } => {
            let metric = crs::GeodesicMetric::from_model(model)?;
            kernel.eval_geodesic(&metric, row, left, right)
        },
        crs::MetricModel::Planar { to_metre } => {
            kernel.eval_planar(to_metre.get(), row, left, right)
        },
    }
}

pub(super) fn point_binary_geometry_coords_dispatch<K: PointBinaryGeometryKernel>(
    kernel: K,
    model: &crs::MetricModel,
    len: usize,
    axes: CoordinateAxes,
    missing: Option<&ValidityMask>,
    points: impl FnMut(usize) -> (Point, Point),
) -> PyResult<CoordSeq> {
    match model {
        crs::MetricModel::Geodesic { .. } => {
            let metric = crs::GeodesicMetric::from_model(model)?;
            match missing {
                Some(mask) => {
                    kernel.coords_rows_geodesic::<true, _>(&metric, len, axes, Some(mask), points)
                },
                None => kernel.coords_rows_geodesic::<false, _>(&metric, len, axes, None, points),
            }
        },
        crs::MetricModel::Planar { to_metre } => match missing {
            Some(mask) => point_binary_geometry_coords::<true, _>(
                kernel,
                len,
                axes,
                Some(mask),
                points,
                |kernel, row, left, right| kernel.eval_planar(to_metre.get(), row, left, right),
            ),
            None => point_binary_geometry_coords::<false, _>(
                kernel,
                len,
                axes,
                None,
                points,
                |kernel, row, left, right| kernel.eval_planar(to_metre.get(), row, left, right),
            ),
        },
    }
}

fn uniform_point_binary_geometry_axes<const HAS_MISSING: bool, K: PointBinaryGeometryKernel>(
    kernel: K,
    len: usize,
    missing: Option<&ValidityMask>,
    mut points: impl FnMut(usize) -> (Point, Point),
) -> Option<CoordinateAxes> {
    uniform_axes::<HAS_MISSING>(len, missing, |row| {
        let (left, right) = points(row);
        kernel.output_axes(left, right)
    })
}

pub(super) fn uniform_point_binary_geometry_axes_dispatch<K: PointBinaryGeometryKernel>(
    kernel: K,
    len: usize,
    missing: Option<&ValidityMask>,
    points: impl FnMut(usize) -> (Point, Point),
) -> Option<CoordinateAxes> {
    match missing {
        Some(mask) => {
            uniform_point_binary_geometry_axes::<true, _>(kernel, len, Some(mask), points)
        },
        None => uniform_point_binary_geometry_axes::<false, _>(kernel, len, None, points),
    }
}

const fn input_missing(input: GeometryInput<'_>) -> Option<&ValidityMask> {
    match input {
        GeometryInput::One(_) => None,
        GeometryInput::Many(array) => array.missing(),
    }
}

pub(super) fn union_missing_masks<'a>(
    inputs: impl IntoIterator<Item = GeometryInput<'a>>,
) -> Option<ValidityMask> {
    union_many(inputs.into_iter().map(input_missing))
}

pub(super) fn point_operand_input(input: GeometryInput<'_>) -> Option<PointOperand<'_>> {
    match input {
        GeometryInput::One(geometry) => geometry_point(geometry).map(PointOperand::One),
        GeometryInput::Many(array) if array.is_compact_nullable() => matches!(
            array.storage(),
            GeometryArrayStorage::Packed(PackedStorage::Points { .. })
        )
        .then_some(PointOperand::Compact(array)),
        GeometryInput::Many(array) => array.storage().point_rows().map(PointOperand::Many),
    }
}
