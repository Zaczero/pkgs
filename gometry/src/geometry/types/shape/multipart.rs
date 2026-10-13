use super::{
    CoordIter, CoordSeq, CoordSeqBuilder, CoordinateAxes, Coordinates, GeometryErrorKind, HeapSize,
    LineSeq, MultiLineString, MultiMember, MultiPoint, MultiPolygon, Point, Polygon, Result,
    SharedSlice, SharedSliceIter,
};

impl<T: crate::HeapSize> crate::HeapSize for MultiMember<T> {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Value(value) => value.heap_bytes(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum MultipartMembers {
    Dense,
    // Contains at least one Empty; Value indices are consecutive encounter order.
    Sparse(SharedSlice<MultiMember<usize>>),
}

impl MultipartMembers {
    fn try_collect<T, O>(
        members: Vec<MultiMember<T>>,
        make_values: impl FnOnce(usize) -> O,
        mut push_value: impl FnMut(&mut O, T) -> Result<()>,
    ) -> Result<(O, Self)> {
        let present = members
            .iter()
            .filter(|member| matches!(member, MultiMember::Value(_)))
            .count();
        let mut values = make_values(present);
        let mut indices = (present < members.len()).then(|| Vec::with_capacity(members.len()));
        let mut next_value = 0_usize;
        for member in members {
            match member {
                MultiMember::Empty => {
                    if let Some(indices) = indices.as_mut() {
                        indices.push(MultiMember::Empty);
                    }
                },
                MultiMember::Value(value) => {
                    push_value(&mut values, value)?;
                    if let Some(indices) = indices.as_mut() {
                        indices.push(MultiMember::Value(next_value));
                    }
                    next_value = next_value.wrapping_add(1);
                },
            }
        }
        let layout = if let Some(indices) = indices {
            Self::Sparse(SharedSlice::from_vec(indices))
        } else if present == 0 {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "geometry payload",
            }
            .into());
        } else {
            Self::Dense
        };
        Ok((values, layout))
    }

    fn len(&self, value_len: usize) -> usize {
        match self {
            Self::Dense => value_len,
            Self::Sparse(members) => members.len(),
        }
    }

    const fn has_empty_members(&self) -> bool {
        matches!(self, Self::Sparse(_))
    }

    fn get(&self, index: usize, value_len: usize) -> Option<MultiMember<usize>> {
        match self {
            Self::Dense => (index < value_len).then_some(MultiMember::Value(index)),
            Self::Sparse(members) => members.get(index).copied(),
        }
    }

    fn iter(&self, value_len: usize) -> MultipartMemberIter<'_> {
        match self {
            Self::Dense => MultipartMemberIter::Dense(0..value_len),
            Self::Sparse(members) => MultipartMemberIter::Sparse(members.iter()),
        }
    }

    pub(super) fn heap_bytes(&self) -> usize {
        match self {
            Self::Dense => 0,
            Self::Sparse(members) => members.heap_bytes(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum MultipartMemberIter<'a> {
    Dense(std::ops::Range<usize>),
    Sparse(std::slice::Iter<'a, MultiMember<usize>>),
}

impl Iterator for MultipartMemberIter<'_> {
    type Item = MultiMember<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense(indices) => indices.next().map(MultiMember::Value),
            Self::Sparse(members) => members.next().copied(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl DoubleEndedIterator for MultipartMemberIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense(indices) => indices.next_back().map(MultiMember::Value),
            Self::Sparse(members) => members.next_back().copied(),
        }
    }
}

impl ExactSizeIterator for MultipartMemberIter<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Dense(indices) => indices.len(),
            Self::Sparse(members) => members.len(),
        }
    }
}

macro_rules! impl_multipart {
    ($name:ident, $member:ty) => {
        impl $name {
            pub(crate) fn try_new(
                axes: CoordinateAxes,
                members: Vec<MultiMember<$member>>,
            ) -> Result<Self> {
                let (values, members) = MultipartMembers::try_collect(
                    members,
                    Vec::with_capacity,
                    |values: &mut Vec<$member>, value| {
                        let got = shape_member_axes(&value);
                        if got != axes {
                            return Err(GeometryErrorKind::CoordinateAxesMismatch {
                                declared: axes,
                                got,
                            }
                            .into());
                        }
                        values.push(value);
                        Ok(())
                    },
                )?;
                Ok(Self {
                    axes,
                    values: SharedSlice::from_vec(values),
                    members,
                })
            }

            /// Replace stored values one-for-one while retaining the admitted
            /// member layout, including explicit EMPTY positions.
            pub(crate) fn with_values(&self, values: Vec<$member>) -> Self {
                debug_assert_eq!(values.len(), self.values.len());
                debug_assert!(
                    values
                        .iter()
                        .all(|value| shape_member_axes(value) == self.axes)
                );
                Self {
                    axes: self.axes,
                    values: SharedSlice::from_vec(values),
                    members: self.members.clone(),
                }
            }

            /// A dense multipart from members that are all present, declaring
            /// the axes of the first. Axis-homogeneity is a payload invariant
            /// (the DbC boundary rule [`CoordSeq::try_from_points`] follows);
            /// external members are admitted through [`Self::try_new`], which
            /// checks. Returns `None` for an empty payload — that is
            /// `Shape::Empty`, not a multipart with no members.
            pub(crate) fn from_dense(values: Vec<$member>) -> Option<Self> {
                let first = values.first()?;
                let axes = shape_member_axes(first);
                debug_assert!(
                    values.iter().all(|value| shape_member_axes(value) == axes),
                    "from_dense requires an axis-homogeneous payload",
                );
                Some(Self {
                    axes,
                    values: SharedSlice::from_vec(values),
                    members: MultipartMembers::Dense,
                })
            }

            pub(crate) const fn axes(&self) -> CoordinateAxes {
                self.axes
            }

            pub(crate) fn members(&self) -> MultipartMemberIter<'_> {
                self.members.iter(self.values.len())
            }

            /// Borrow members in logical order, resolving admitted value
            /// indices at the multipart layout owner.
            pub(crate) fn members_with_values(
                &self,
            ) -> impl Iterator<Item = MultiMember<&$member>> + '_ {
                self.members
                    .iter(self.values.len())
                    .map(|member| match member {
                        MultiMember::Empty => MultiMember::Empty,
                        MultiMember::Value(index) => MultiMember::Value(
                            self.values
                                .as_slice()
                                .get(index)
                                .expect("multipart value indices are admitted with their layout"),
                        ),
                    })
            }

            pub(crate) fn member_at(&self, index: usize) -> Option<MultiMember<usize>> {
                self.members.get(index, self.values.len())
            }

            pub(crate) const fn has_empty_members(&self) -> bool {
                self.members.has_empty_members()
            }

            pub(crate) fn try_map_values<E>(
                &self,
                axes: CoordinateAxes,
                mut map: impl FnMut(&$member) -> Result<$member, E>,
            ) -> Result<Self, E>
            where
                E: From<crate::error::Error>,
            {
                let mut values = Vec::with_capacity(self.values.len());
                for value in self.values.iter() {
                    values.push(map(value)?);
                }
                self.try_with_values(axes, values).map_err(E::from)
            }

            pub(crate) fn try_with_values(
                &self,
                axes: CoordinateAxes,
                values: Vec<$member>,
            ) -> Result<Self> {
                debug_assert_eq!(values.len(), self.values.len());
                if let Some(value) = values
                    .iter()
                    .find(|value| shape_member_axes(*value) != axes)
                {
                    return Err(GeometryErrorKind::CoordinateAxesMismatch {
                        declared: axes,
                        got: shape_member_axes(value),
                    }
                    .into());
                }
                Ok(Self {
                    axes,
                    values: SharedSlice::from_vec(values),
                    members: self.members.clone(),
                })
            }

            pub(crate) fn with_axes(&self, axes: CoordinateAxes) -> Self {
                Self {
                    axes,
                    values: self.values.clone(),
                    members: self.members.clone(),
                }
            }

            pub(crate) fn heap_bytes_saturating(&self) -> usize {
                self.values
                    .heap_bytes_with(HeapSize::heap_bytes)
                    .saturating_add(self.members.heap_bytes())
            }
        }

        impl crate::HeapSize for $name {
            fn heap_bytes(&self) -> usize {
                self.heap_bytes_saturating()
            }
        }
    };
}

impl_multipart!(MultiLineString, LineSeq);

impl_multipart!(MultiPolygon, Polygon);

impl MultiPoint {
    pub(crate) fn try_new(axes: CoordinateAxes, members: Vec<MultiMember<Point>>) -> Result<Self> {
        let (points, members) = MultipartMembers::try_collect(
            members,
            |present| CoordSeqBuilder::with_capacity(axes, present),
            |points, point| {
                if point.axes() != axes {
                    return Err(GeometryErrorKind::CoordinateAxesMismatch {
                        declared: axes,
                        got: point.axes(),
                    }
                    .into());
                }
                points.push(point);
                Ok(())
            },
        )?;
        Ok(Self {
            coords: points.finish()?,
            members,
        })
    }

    pub(crate) fn from_coordseq(coords: CoordSeq) -> Result<Self> {
        if coords.is_empty() {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "MultiPoint payload",
            }
            .into());
        }
        Ok(Self {
            coords,
            members: MultipartMembers::Dense,
        })
    }

    pub(crate) const fn axes(&self) -> CoordinateAxes {
        self.coords.axes()
    }

    pub(crate) fn members(&self) -> MultipartMemberIter<'_> {
        self.members.iter(self.coords.len())
    }

    pub(crate) fn member_at(&self, index: usize) -> Option<MultiMember<usize>> {
        self.members.get(index, self.coords.len())
    }

    pub(crate) const fn has_empty_members(&self) -> bool {
        self.members.has_empty_members()
    }

    /// Members including EMPTY ones, as `members()` yields them. The
    /// coordinate sequence this type derefs to holds only stored points.
    pub(crate) fn member_count(&self) -> usize {
        self.members.len(self.coords.len())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.coords.is_empty()
    }

    pub(crate) fn iter(&self) -> CoordIter<'_> {
        self.coords.iter()
    }

    pub(crate) fn points(&self) -> CoordIter<'_> {
        self.iter()
    }

    pub(crate) fn point_at(&self, index: usize) -> Point {
        self.coords.point_at(index)
    }

    pub(crate) fn to_vec(&self) -> Vec<Point> {
        let mut points = Vec::with_capacity(self.coord_count());
        points.extend(self.points());
        points
    }

    pub(crate) fn with_values(&self, values: &[Point]) -> Result<Self> {
        debug_assert_eq!(values.len(), self.coords.len());
        let coords = if values.is_empty() {
            CoordSeq::empty(self.axes())
        } else {
            CoordSeq::try_from_points(values)?
        };
        Ok(Self {
            coords,
            members: self.members.clone(),
        })
    }

    pub(crate) fn xs(&self) -> &[f64] {
        self.coords.xs()
    }

    pub(crate) fn ys(&self) -> &[f64] {
        self.coords.ys()
    }

    pub(crate) fn zs(&self) -> Option<&[f64]> {
        self.coords.zs()
    }

    pub(crate) fn ms(&self) -> Option<&[f64]> {
        self.coords.ms()
    }

    pub(crate) fn coordinate_bytes(&self) -> usize {
        self.coords.coordinate_bytes()
    }

    pub(crate) fn try_map_xy_with<E>(
        &self,
        op: impl FnMut(f64, f64) -> Result<(f64, f64), E>,
    ) -> Result<Self, E>
    where
        E: From<crate::error::Error>,
    {
        let coords = self.coords.try_map_xy_with(op)?;
        Ok(Self {
            coords,
            members: self.members.clone(),
        })
    }

    /// [`Self::try_map_values`] for a map whose callback cannot fail.
    pub(crate) fn map_values(&self, mut map: impl FnMut(Point) -> Point) -> Result<Self> {
        self.try_map_values(|point| Ok::<_, crate::error::Error>(map(point)))
    }

    pub(crate) fn try_map_values<E>(
        &self,
        map: impl FnMut(Point) -> Result<Point, E>,
    ) -> Result<Self, E>
    where
        E: From<crate::error::Error>,
    {
        Ok(Self {
            coords: self.coords.try_map_points(map)?,
            members: self.members.clone(),
        })
    }

    pub(crate) fn quantize(&self, precision: i32) -> Result<Self> {
        let coords = self.coords.quantize(precision)?;
        Ok(Self {
            coords,
            members: self.members.clone(),
        })
    }

    pub(crate) fn force_2d(&self) -> Result<Self> {
        let coords = self.coords.force_2d()?;
        Ok(Self {
            coords,
            members: self.members.clone(),
        })
    }

    pub(crate) fn with_axes(&self, axes: CoordinateAxes) -> Self {
        Self {
            coords: if self.coords.is_empty() {
                CoordSeq::empty(axes)
            } else {
                self.coords.clone()
            },
            members: self.members.clone(),
        }
    }

    pub(crate) fn with_coords(&self, coords: CoordSeq) -> Self {
        debug_assert_eq!(coords.len(), self.coords.len());
        Self {
            coords,
            members: self.members.clone(),
        }
    }

    pub(crate) fn heap_bytes_saturating(&self) -> usize {
        self.coords
            .heap_bytes_saturating()
            .saturating_add(self.members.heap_bytes())
    }
}

impl<'a> IntoIterator for &'a MultiPoint {
    type Item = Point;
    type IntoIter = CoordIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl crate::HeapSize for MultiPoint {
    fn heap_bytes(&self) -> usize {
        self.heap_bytes_saturating()
    }
}

macro_rules! impl_multipart_iteration {
    ($name:ident, $member:ty) => {
        impl $name {
            /// Members including EMPTY ones, as `members()` yields them. The
            /// slice this type derefs to holds only the stored values.
            pub(crate) fn member_count(&self) -> usize {
                self.members.len(self.values.len())
            }

            pub(crate) const fn is_empty(&self) -> bool {
                self.values.is_empty()
            }

            pub(crate) fn iter(&self) -> impl Iterator<Item = &$member> {
                self.values.iter()
            }
        }

        impl<'a> IntoIterator for &'a $name {
            type Item = &'a $member;
            type IntoIter = std::slice::Iter<'a, $member>;

            fn into_iter(self) -> Self::IntoIter {
                self.values.iter()
            }
        }

        impl IntoIterator for $name {
            type Item = $member;
            type IntoIter = SharedSliceIter<$member>;

            fn into_iter(self) -> Self::IntoIter {
                self.values.iter_owned()
            }
        }
    };
}

impl_multipart_iteration!(MultiLineString, LineSeq);

impl_multipart_iteration!(MultiPolygon, Polygon);

impl MultiLineString {
    pub(crate) fn as_slice(&self) -> &[LineSeq] {
        &self.values
    }
}

impl MultiPolygon {
    pub(crate) fn first(&self) -> Option<&Polygon> {
        self.iter().next()
    }

    pub(crate) fn as_slice(&self) -> &[Polygon] {
        &self.values
    }
}

impl std::ops::Deref for MultiPoint {
    type Target = CoordSeq;

    fn deref(&self) -> &Self::Target {
        &self.coords
    }
}

impl std::ops::Deref for MultiLineString {
    type Target = [LineSeq];

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl std::ops::Deref for MultiPolygon {
    type Target = [Polygon];

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl Coordinates for MultiPoint {
    fn coord_count(&self) -> usize {
        self.coords.len()
    }

    fn nth_coord(&self, index: usize) -> Point {
        self.point_at(index)
    }

    fn iter_coords(&self) -> impl DoubleEndedIterator<Item = Point> + '_ {
        self.coords.iter()
    }

    fn xy_columns(&self) -> Option<(&[f64], &[f64])> {
        self.coords.xy_columns()
    }

    fn z_column(&self) -> Option<&[f64]> {
        self.coords.z_column()
    }

    fn m_column(&self) -> Option<&[f64]> {
        self.coords.m_column()
    }
}

impl Point {
    pub(crate) const fn axes(self) -> CoordinateAxes {
        self.axes
    }
}

impl Polygon {
    pub(crate) fn axes(&self) -> CoordinateAxes {
        self.rings()
            .map(|ring| ring.axes())
            .fold(CoordinateAxes::XY, CoordinateAxes::union)
    }
}

fn shape_member_axes(value: &impl ShapeMemberAxes) -> CoordinateAxes {
    value.shape_member_axes()
}

trait ShapeMemberAxes {
    fn shape_member_axes(&self) -> CoordinateAxes;
}

impl ShapeMemberAxes for Point {
    fn shape_member_axes(&self) -> CoordinateAxes {
        self.axes()
    }
}

impl ShapeMemberAxes for LineSeq {
    fn shape_member_axes(&self) -> CoordinateAxes {
        self.axes()
    }
}

impl ShapeMemberAxes for Polygon {
    fn shape_member_axes(&self) -> CoordinateAxes {
        self.axes()
    }
}
