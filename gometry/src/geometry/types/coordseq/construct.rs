use super::*;

fn columns_all_finite(xs: &[f64], ys: &[f64], zs: Option<&[f64]>, ms: Option<&[f64]>) -> bool {
    column_all_finite(xs)
        && column_all_finite(ys)
        && zs.is_none_or(column_all_finite)
        && ms.is_none_or(column_all_finite)
}

fn reserve_point_column(count: usize, work: &mut impl Work) -> Result<Vec<f64>> {
    work.charge_product_into_error(count, 1)?;
    Ok(Vec::with_capacity(count))
}

fn copy_point_column(
    points: &[Point],
    column: &mut Vec<f64>,
    ordinate: impl FnMut(Point) -> f64,
    work: &mut impl Work,
) -> Result<()> {
    work.charge_product_into_error(points.len(), 1)?;
    column.extend(points.iter().copied().map(ordinate));
    Ok(())
}

fn point_column_all_finite(values: &[f64], work: &mut impl Work) -> Result<bool> {
    work.charge_product_into_error(values.len(), 1)?;
    Ok(column_all_finite(values))
}

impl CoordSeq {
    /// Empty sequence with the given axes.
    pub fn empty(axes: CoordinateAxes) -> Self {
        // Empty owners use the null representation, while the outer options
        // preserve present-empty Z/M axes.
        Self {
            xs: SharedSlice::empty(),
            ys: SharedSlice::empty(),
            zs: axes.has_z().then(SharedSlice::empty),
            ms: axes.has_m().then(|| MColumn::Shared(SharedSlice::empty())),
            range: 0..0,
        }
    }

    /// Fallible point gather for untrusted and resource-covered ingestion.
    pub fn try_from_points(points: &[Point]) -> Result<Self> {
        Self::try_from_points_with_work(points, &mut Unmetered)
    }

    /// Construct the active columns while charging each lane's reserve, copy,
    /// and finiteness pass to the Native operation account.
    pub(crate) fn try_from_points_with_work(
        points: &[Point],
        work: &mut impl Work,
    ) -> Result<Self> {
        let Some(first) = points.first() else {
            return Ok(Self::empty(CoordinateAxes::XY));
        };
        if let Some(point) = points.iter().find(|point| point.axes != first.axes) {
            return Err(GeometryErrorKind::CoordinateAxesMismatch {
                declared: first.axes,
                got: point.axes,
            }
            .into());
        }
        ensure_coordseq_vertex_capacity(points.len())?;
        let mut xs = reserve_point_column(points.len(), work)?;
        let mut ys = reserve_point_column(points.len(), work)?;
        let mut zs = if first.axes.has_z() {
            Some(reserve_point_column(points.len(), work)?)
        } else {
            None
        };
        let mut ms = if first.axes.has_m() {
            Some(reserve_point_column(points.len(), work)?)
        } else {
            None
        };
        copy_point_column(points, &mut xs, |point| point.x, work)?;
        copy_point_column(points, &mut ys, |point| point.y, work)?;
        if let Some(zs) = &mut zs {
            copy_point_column(points, zs, |point| point.z, work)?;
        }
        if let Some(ms) = &mut ms {
            copy_point_column(points, ms, |point| point.m, work)?;
        }
        if !point_column_all_finite(&xs, work)? || !point_column_all_finite(&ys, work)? {
            return Err(GeometryErrorKind::NonFiniteCoordinate.into());
        }
        if let Some(zs) = &zs
            && !point_column_all_finite(zs, work)?
        {
            return Err(GeometryErrorKind::NonFiniteCoordinate.into());
        }
        if let Some(ms) = &ms
            && !point_column_all_finite(ms, work)?
        {
            return Err(GeometryErrorKind::NonFiniteCoordinate.into());
        }
        Self::try_from_vecs(xs, ys, zs, ms)
    }

    /// Map stored points directly into new columns, inferring axes from the
    /// first output. Callback errors precede column validation.
    pub(crate) fn try_map_points<E>(
        &self,
        mut map: impl FnMut(Point) -> Result<Point, E>,
    ) -> Result<Self, E>
    where
        E: From<crate::error::Error>,
    {
        let mut points = self.iter();
        let Some(first) = points.next() else {
            return Ok(Self::empty(self.axes()));
        };
        let first = map(first)?;
        let mut builder = CoordSeqBuilder::with_capacity(first.axes(), self.len());
        builder.push(first);
        for point in points {
            builder.try_push(map(point))?;
        }
        builder.finish().map_err(E::from)
    }

    /// Gather a slice of `XY` (planar, Z/M-less) coordinates into columns.
    pub fn try_from_xy(points: &[XY]) -> Result<Self> {
        ensure_coordseq_vertex_capacity(points.len())?;
        let mut xs = Vec::with_capacity(points.len());
        let mut ys = Vec::with_capacity(points.len());
        xs.extend(points.iter().map(|point| point.x));
        ys.extend(points.iter().map(|point| point.y));
        Self::try_from_vecs(xs, ys, None, None)
    }

    /// Assemble from owned ordinate vectors. `zs`/`ms` presence sets the axes;
    /// all present columns must match `xs` in length.
    pub fn try_from_vecs(
        xs: Vec<f64>,
        ys: Vec<f64>,
        zs: Option<Vec<f64>>,
        ms: Option<Vec<f64>>,
    ) -> Result<Self> {
        let xs = SharedSlice::from_vec(xs);
        let ys = SharedSlice::from_vec(ys);
        let zs = zs.map(SharedSlice::from_vec);
        let ms = ms.map(|values| MColumn::Shared(SharedSlice::from_vec(values)));
        Self::from_shared_columns(xs, ys, zs, ms)
    }

    pub(crate) fn from_shared_columns(
        xs: SharedSlice<f64>,
        ys: SharedSlice<f64>,
        zs: Option<SharedSlice<f64>>,
        ms: Option<MColumn>,
    ) -> Result<Self> {
        ensure_coordseq_vertex_capacity(xs.len())?;
        if ys.len() != xs.len() {
            return Err(GeometryErrorKind::CoordinateLength(xs.len(), ys.len()).into());
        }
        if let Some(zs) = &zs
            && zs.len() != xs.len()
        {
            return Err(GeometryErrorKind::CoordinateLength(xs.len(), zs.len()).into());
        }
        admit_m_column(ms.as_ref(), xs.len(), xs.len())?;
        // SAFETY: capacity and every column's required length were admitted above.
        Ok(unsafe { Self::from_shared_columns_unchecked(xs, ys, zs, ms) })
    }

    /// Infallible assembly from already-frozen, validated owners.
    ///
    /// # Safety
    ///
    /// XY/Z lengths are parallel. Shared M length equals XY. Local M covers
    /// `offset + XY length` without using remaining suffix as a logical length.
    /// Vertex capacity fits `i32`. Visible length is `0..xs.len()`.
    pub(crate) unsafe fn from_shared_columns_unchecked(
        xs: SharedSlice<f64>,
        ys: SharedSlice<f64>,
        zs: Option<SharedSlice<f64>>,
        ms: Option<MColumn>,
    ) -> Self {
        debug_assert_eq!(xs.len(), ys.len());
        debug_assert!(zs.as_ref().is_none_or(|column| column.len() == xs.len()));
        debug_assert!(admit_m_column(ms.as_ref(), xs.len(), xs.len()).is_ok());
        debug_assert!(ensure_coordseq_vertex_capacity(xs.len()).is_ok());
        let range = 0..xs.len() as u32;
        Self {
            xs,
            ys,
            zs,
            ms,
            range,
        }
    }

    /// Assemble shared coordinate columns with a local M column.
    ///
    /// The window is a checked coordinate boundary, and all inputs are
    /// validated before the resulting sequence can expose them.
    pub(crate) fn from_shared_columns_with_window(
        xs: SharedSlice<f64>,
        ys: SharedSlice<f64>,
        zs: Option<SharedSlice<f64>>,
        ms: Option<MColumn>,
        window: CoordWindow,
    ) -> Result<Self> {
        debug_assert_eq!(xs.len(), ys.len());
        debug_assert!(zs.as_ref().is_none_or(|column| column.len() == xs.len()));
        debug_assert!(window.end() <= xs.len());
        debug_assert!(ensure_coordseq_vertex_capacity(xs.len()).is_ok());
        admit_m_column(ms.as_ref(), xs.len(), window.len())?;
        Ok(Self {
            xs,
            ys,
            zs,
            ms,
            range: window.start() as u32..window.end() as u32,
        })
    }

    /// Build owned columns from already-owned `Vec`s, validating every
    /// coordinate is finite — the zero-copy columnar constructor for
    /// column-form input (`line_string(x=, y=)`, `points(...)`) that skips
    /// the per-vertex `Point` build and the `Vec<Point>` re-gather.
    /// `zs`/`ms` being `Some` sets the axes; the caller guarantees equal
    /// column lengths (checked in debug).
    pub fn from_owned_columns(
        xs: Vec<f64>,
        ys: Vec<f64>,
        zs: Option<Vec<f64>>,
        ms: Option<Vec<f64>>,
    ) -> Result<Self> {
        debug_assert_eq!(xs.len(), ys.len());
        if !columns_all_finite(&xs, &ys, zs.as_deref(), ms.as_deref()) {
            return Err(GeometryErrorKind::NonFiniteCoordinate.into());
        }
        Self::try_from_vecs(xs, ys, zs, ms)
    }
}

/// Concatenate two axis-homogeneous coordinate columns in one exact allocation
/// per ordinate lane — the trusted packed-storage concat fast path.
///
/// Each lane is reserved to exact length, filled with two `copy_from_slice`
/// writes, and frozen once into an `Arc` (no builder, no double-init zero fill,
/// no spare-capacity shrink on freeze).
#[cfg(test)]
pub(crate) fn concat_coord_columns(left: &CoordSeq, right: &CoordSeq) -> Result<CoordSeq> {
    debug_assert_eq!(left.axes(), right.axes());
    let len = left.len().saturating_add(right.len());
    ensure_coordseq_vertex_capacity(len)?;
    let xs = concat_f64_slices(left.xs(), right.xs(), len);
    let ys = concat_f64_slices(left.ys(), right.ys(), len);
    let zs = match (left.zs(), right.zs()) {
        (Some(left_zs), Some(right_zs)) => Some(concat_f64_slices(left_zs, right_zs, len)),
        (None, None) => None,
        _ => {
            debug_assert!(false, "concat_coord_columns requires matching axes");
            return Err(GeometryErrorKind::CoordinateAxesMismatch {
                declared: left.axes(),
                got: right.axes(),
            }
            .into());
        },
    };
    let ms = match (left.ms(), right.ms()) {
        (Some(left_ms), Some(right_ms)) => Some(concat_f64_slices(left_ms, right_ms, len)),
        (None, None) => None,
        _ => {
            debug_assert!(false, "concat_coord_columns requires matching axes");
            return Err(GeometryErrorKind::CoordinateAxesMismatch {
                declared: left.axes(),
                got: right.axes(),
            }
            .into());
        },
    };
    CoordSeq::try_from_vecs(xs, ys, zs, ms)
}

/// Two-slice join into mutable storage whose original capacity is retained by
/// the final shared owner.
#[cfg(test)]
fn concat_f64_slices(left: &[f64], right: &[f64], output_len: usize) -> Vec<f64> {
    let mut values = Vec::with_capacity(output_len);
    values.extend_from_slice(left);
    values.extend_from_slice(right);
    values
}

/// The two-point sequence of a witness line, keeping the ordinate columns
/// BOTH endpoints carry — the axes intersection.
///
/// A two-vertex sequence on the axes both endpoints carry. Reached through
/// [`crate::geometry::LineSeq::segment`], which is where the rule is documented.
pub(crate) fn coerced_pair(start: Point, end: Point) -> Result<CoordSeq> {
    let (start, end) = coerce_to_common_axes(start, end);
    let mut builder = CoordSeqBuilder::with_capacity(start.axes, 2);
    builder.push(start);
    builder.push(end);
    builder.finish()
}

/// Coerce a witness pair to the ordinate columns BOTH endpoints carry (the axes
/// intersection) — a 2D endpoint never gets fabricated Z/M. The single source of
/// the common-dimensionality rule for `nearest_points`/`shortest_line`, so the
/// witness `LineString` ([`crate::geometry::LineSeq::segment`]) and the point pair cannot diverge:
/// `nearest_points(left, right)` returns exactly `shortest_line`'s endpoints,
/// dimensionality included (an XYZM line vs an XY point yields XY witnesses).
pub(crate) fn coerce_to_common_axes(start: Point, end: Point) -> (Point, Point) {
    let has_z = start.axes.has_z() && end.axes.has_z();
    let has_m = start.axes.has_m() && end.axes.has_m();
    let coerce = |point: Point| {
        Point::new_raw_axes(
            point.x,
            point.y,
            ZOrdinate(has_z.then_some(point.z)),
            MOrdinate(has_m.then_some(point.m)),
        )
    };
    (coerce(start), coerce(end))
}
