use super::*;

impl Shape {
    /// Admit collection children after checking the complete subtree without
    /// recursion. Empty input uses the typed empty collection representation.
    pub(crate) fn geometry_collection(parts: Vec<Self>) -> Result<Self> {
        if parts.is_empty() {
            return Ok(Self::Empty(
                GeometryKind::GeometryCollection,
                CoordinateAxes::XY,
            ));
        }
        Ok(Self::GeometryCollection(GeometryCollection::try_new(
            parts,
        )?))
    }

    /// Maximum collection depth in this shape.
    pub(crate) const fn nesting_depth(&self) -> usize {
        match self {
            Self::GeometryCollection(collection) => collection.depth(),
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Polygon(_)
            | Self::MultiPolygon(_)
            | Self::Empty(..) => 0,
        }
    }
}

/// A checked nonempty vector used by shape payloads. Empty top-level values
/// have a typed [`Shape::Empty`] variant instead.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NonEmptyVec<T>(SharedSlice<T>);

impl<T> NonEmptyVec<T> {
    pub(crate) fn try_from_vec(items: Vec<T>) -> Result<Self> {
        if items.is_empty() {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "geometry payload",
            }
            .into());
        }
        Ok(Self(SharedSlice::from_vec(items)))
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn is_shared(&self) -> bool {
        self.0.is_shared()
    }

    fn shallow_heap_bytes(&self) -> usize {
        self.0.shallow_heap_bytes()
    }
}

impl<T> std::ops::Deref for NonEmptyVec<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, T> IntoIterator for &'a NonEmptyVec<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<T: Clone> IntoIterator for NonEmptyVec<T> {
    type Item = T;
    type IntoIter = SharedSliceIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_owned()
    }
}

impl<T: crate::HeapSize> crate::HeapSize for NonEmptyVec<T> {
    fn heap_bytes(&self) -> usize {
        self.0.heap_bytes()
    }
}

/// A checked, immutable collection payload carrying its exact nesting depth
/// and topological dimension. The members stay behind this type so no
/// operation can mutate them without rebuilding the enclosing collection.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GeometryCollection {
    members: NonEmptyVec<Shape>,
    depth: NonZeroU8,
    topological_dimension: Dimension,
}

impl GeometryCollection {
    fn try_new(parts: Vec<Shape>) -> Result<Self> {
        let members = NonEmptyVec::try_from_vec(parts)?;
        Self::from_members(members, MAX_NESTING)
    }

    fn from_members(members: NonEmptyVec<Shape>, limit: usize) -> Result<Self> {
        let mut child_depth = 0;
        let mut topological_dimension = Dimension::Point;
        for member in &members {
            child_depth = child_depth.max(member.nesting_depth());
            topological_dimension = topological_dimension.max(member.topological_dimension());
        }
        let depth = child_depth.saturating_add(1);
        if depth > MAX_NESTING || depth > limit {
            return Err(collection_depth_error());
        }
        Ok(Self {
            members,
            depth: NonZeroU8::new(depth as u8).expect("collection depth is non-zero"),
            topological_dimension,
        })
    }

    /// Rebuild members while enforcing that the new collection is no deeper
    /// than the source collection. This is the path for structure-preserving
    /// transforms; arbitrary children use [`Shape::geometry_collection`].
    pub(crate) fn try_map_members<E, F>(&self, mut map: F) -> std::result::Result<Self, E>
    where
        E: From<crate::error::Error>,
        F: FnMut(&Shape) -> std::result::Result<Shape, E>,
    {
        let members = map_collection_members(self.members.iter(), &mut map)?;
        Self::from_members(
            NonEmptyVec::try_from_vec(members).map_err(E::from)?,
            self.depth.get() as usize,
        )
        .map_err(E::from)
    }

    pub(crate) fn try_map_owned_members<E, F>(self, mut map: F) -> std::result::Result<Self, E>
    where
        E: From<crate::error::Error>,
        F: FnMut(Shape) -> std::result::Result<Shape, E>,
    {
        let limit = self.depth.get() as usize;
        let members = map_collection_members(self.members.into_iter(), &mut map)?;
        Self::from_members(NonEmptyVec::try_from_vec(members).map_err(E::from)?, limit)
            .map_err(E::from)
    }

    pub(crate) fn try_rebuild_members(&self, members: Vec<Shape>) -> Result<Self> {
        Self::from_members(NonEmptyVec::try_from_vec(members)?, self.depth())
    }

    pub(crate) fn try_filter_map_owned_members<F>(self, mut map: F) -> Result<Shape>
    where
        F: FnMut(Shape) -> Result<Option<Shape>>,
    {
        let limit = self.depth();
        let mut members = Vec::new();
        for shape in self.members {
            if let Some(mapped) = map(shape)? {
                members.push(mapped);
            }
        }
        if members.is_empty() {
            return Ok(Shape::typed_empty(
                GeometryKind::GeometryCollection,
                CoordinateAxes::XY,
            ));
        }
        Ok(Shape::GeometryCollection(Self::from_members(
            NonEmptyVec::try_from_vec(members)?,
            limit,
        )?))
    }

    pub(crate) const fn depth(&self) -> usize {
        self.depth.get() as usize
    }

    pub(crate) const fn topological_dimension(&self) -> Dimension {
        self.topological_dimension
    }

    pub(crate) fn as_slice(&self) -> &[Shape] {
        &self.members
    }

    pub(crate) fn iter(&self) -> std::slice::Iter<'_, Shape> {
        self.members.iter()
    }

    pub(crate) fn len(&self) -> usize {
        self.members.len()
    }

    pub(crate) fn members_are_shared(&self) -> bool {
        self.members.is_shared()
    }

    pub(super) fn shallow_heap_bytes(&self) -> usize {
        self.members.shallow_heap_bytes()
    }
}

fn map_collection_members<I, E>(
    members: I,
    mut map: impl FnMut(I::Item) -> std::result::Result<Shape, E>,
) -> std::result::Result<Vec<Shape>, E>
where
    I: ExactSizeIterator,
{
    let mut mapped = Vec::with_capacity(members.len());
    for member in members {
        mapped.push(map(member)?);
    }
    Ok(mapped)
}

impl Deref for GeometryCollection {
    type Target = [Shape];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<'a> IntoIterator for &'a GeometryCollection {
    type Item = &'a Shape;
    type IntoIter = std::slice::Iter<'a, Shape>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for GeometryCollection {
    type Item = Shape;
    type IntoIter = SharedSliceIter<Shape>;

    fn into_iter(self) -> Self::IntoIter {
        self.members.into_iter()
    }
}

fn collection_depth_error() -> crate::error::Error {
    GeometryErrorKind::message(format!(
        "geometry collection nesting exceeds the limit of {MAX_NESTING}"
    ))
}

impl crate::HeapSize for GeometryCollection {
    fn heap_bytes(&self) -> usize {
        ShapeHeapMemo::default().collection_stats(self).heap_bytes
    }
}
