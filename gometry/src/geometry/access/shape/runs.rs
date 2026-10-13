use super::{
    ControlFlow, CoordSeq, GeometryKind, Infallible, LineSeq, MultiLineString, MultiPoint,
    MultiPolygon, Point, Polygon, Result, RunIdentity, Shape, SharedShapeOwners, SupportRunKind,
};

/// One atomic part of a shape: a point, a line, or a polygon.
///
/// Multiparts and collections are syntax over these, so an operation that
/// treats a one-member `MultiPolygon` and a bare `Polygon` differently is
/// wrong by construction. Polygon rings stay together: shell and holes are one
/// part, not three.
#[derive(Clone, Copy)]
pub(crate) enum Component<'a> {
    Point(Point),
    LineString(&'a LineSeq),
    Polygon(&'a Polygon),
}

impl Component<'_> {
    pub(crate) fn to_shape(self) -> Shape {
        match self {
            Self::Point(point) => Shape::Point(point),
            Self::LineString(line) => Shape::LineString(line.clone()),
            Self::Polygon(polygon) => Shape::Polygon(polygon.clone()),
        }
    }
}

/// One contiguous coordinate run of a shape, in flatten order.
///
/// A `MultiPoint` is a single run rather than one run per member, because
/// that is how flattening, reconstruction and the coordinate views all treat
/// it. A walk that has to tell puntal members from linework wants
/// [`Component`] instead.
#[derive(Clone, Copy)]
pub(crate) enum CoordRun<'a> {
    Point(Point),
    Seq(&'a CoordSeq),
}

/// One isolated atom or coordinate run in a metric support set.
#[derive(Clone, Copy)]
pub(crate) enum MetricSupportRun<'a> {
    Point(Point),
    MultiPoint(&'a MultiPoint),
    SegmentChain(&'a CoordSeq),
}

/// A lineal operand, empties included. Operations over lines match it
/// exhaustively, so none can route a typed empty into its kind error.
pub(crate) enum Lineal<'a> {
    LineString(&'a LineSeq),
    MultiLineString(&'a MultiLineString),
    Empty,
}

/// A polygonal operand, empties included (see [`Lineal`]).
pub(crate) enum Polygonal<'a> {
    Polygon(&'a Polygon),
    MultiPolygon(&'a MultiPolygon),
    Empty,
}

impl Shape {
    /// The shape as a lineal operand; `None` for any other kind.
    pub(crate) const fn lineal(&self) -> Option<Lineal<'_>> {
        match self {
            Self::LineString(line) => Some(Lineal::LineString(line)),
            Self::MultiLineString(lines) => Some(Lineal::MultiLineString(lines)),
            Self::Empty(GeometryKind::LineString | GeometryKind::MultiLineString, _) => {
                Some(Lineal::Empty)
            },
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::Polygon(_)
            | Self::MultiPolygon(_)
            | Self::GeometryCollection(_)
            | Self::Empty(
                GeometryKind::Point
                | GeometryKind::MultiPoint
                | GeometryKind::Polygon
                | GeometryKind::MultiPolygon
                | GeometryKind::GeometryCollection,
                _,
            ) => None,
        }
    }

    /// The shape as a polygonal operand; `None` for any other kind.
    pub(crate) const fn polygonal(&self) -> Option<Polygonal<'_>> {
        match self {
            Self::Polygon(polygon) => Some(Polygonal::Polygon(polygon)),
            Self::MultiPolygon(polygons) => Some(Polygonal::MultiPolygon(polygons)),
            Self::Empty(GeometryKind::Polygon | GeometryKind::MultiPolygon, _) => {
                Some(Polygonal::Empty)
            },
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::GeometryCollection(_)
            | Self::Empty(
                GeometryKind::Point
                | GeometryKind::MultiPoint
                | GeometryKind::LineString
                | GeometryKind::MultiLineString
                | GeometryKind::GeometryCollection,
                _,
            ) => None,
        }
    }

    /// Visit each first-occurrence atom in the metric support of the shape.
    pub(crate) fn for_each_first_support_run<B>(
        &self,
        identity: RunIdentity,
        mut visit: impl FnMut(MetricSupportRun<'_>) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        let Some(mut owners) = SharedShapeOwners::for_shape(self) else {
            return self.for_each_first_support_run_inner(&mut None, identity, &mut visit);
        };
        let mut owner_set = Some(&mut owners);
        self.for_each_first_support_run_inner(&mut owner_set, identity, &mut visit)
    }

    fn for_each_first_support_run_inner<B>(
        &self,
        owners: &mut Option<&mut SharedShapeOwners>,
        identity: RunIdentity,
        visit: &mut impl FnMut(MetricSupportRun<'_>) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        match self {
            Self::Point(point) => visit(MetricSupportRun::Point(*point))?,
            Self::MultiPoint(points) => {
                if owners.as_mut().is_none_or(|owners| {
                    owners.first_support_run(points, identity, SupportRunKind::PointSet)
                }) {
                    visit(MetricSupportRun::MultiPoint(points))?;
                }
            },
            Self::LineString(line) => {
                let coords = line.as_coords();
                if owners.as_mut().is_none_or(|owners| {
                    owners.first_support_run(coords, identity, SupportRunKind::SegmentChain)
                }) {
                    visit(MetricSupportRun::SegmentChain(coords))?;
                }
            },
            Self::MultiLineString(lines) => {
                for line in lines {
                    let coords = line.as_coords();
                    if owners.as_mut().is_none_or(|owners| {
                        owners.first_support_run(coords, identity, SupportRunKind::SegmentChain)
                    }) {
                        visit(MetricSupportRun::SegmentChain(coords))?;
                    }
                }
            },
            Self::Polygon(polygon) => {
                for ring in polygon.rings() {
                    let coords = ring.coords();
                    if owners.as_mut().is_none_or(|owners| {
                        owners.first_support_run(coords, identity, SupportRunKind::SegmentChain)
                    }) {
                        visit(MetricSupportRun::SegmentChain(coords))?;
                    }
                }
            },
            Self::MultiPolygon(polygons) => {
                for ring in polygons.iter().flat_map(Polygon::rings) {
                    let coords = ring.coords();
                    if owners.as_mut().is_none_or(|owners| {
                        owners.first_support_run(coords, identity, SupportRunKind::SegmentChain)
                    }) {
                        visit(MetricSupportRun::SegmentChain(coords))?;
                    }
                }
            },
            Self::GeometryCollection(geometries) => {
                if owners.as_mut().is_none_or(|owners| {
                    owners.first_collection(geometries.as_slice(), geometries.members_are_shared())
                }) {
                    for geometry in geometries {
                        geometry.for_each_first_support_run_inner(owners, identity, visit)?;
                    }
                }
            },
            Self::Empty(..) => {},
        }
        ControlFlow::Continue(())
    }

    /// Visit every segment-bearing coordinate chain — linestrings,
    /// multi-linestring members, and polygon rings, recursively through
    /// collections — as borrowed columns. The chain enumeration behind
    /// segment-level prepared state (facet linework).
    pub(crate) fn for_each_segment_chain(&self, mut visit: impl FnMut(&CoordSeq)) {
        let ControlFlow::Continue(()): ControlFlow<Infallible> =
            self.for_each_segment_chain_inner(&mut None, RunIdentity::XY, &mut |chain| {
                visit(chain);
                ControlFlow::Continue(())
            });
    }

    /// Visit every segment-bearing chain until `visit` returns `Err`.
    pub(crate) fn try_for_each_segment_chain<E>(
        &self,
        mut visit: impl FnMut(&CoordSeq) -> Result<(), E>,
    ) -> Result<(), E> {
        match self.for_each_segment_chain_inner(
            &mut None,
            RunIdentity::XY,
            &mut |chain| match visit(chain) {
                Ok(()) => ControlFlow::Continue(()),
                Err(error) => ControlFlow::Break(error),
            },
        ) {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(error) => Err(error),
        }
    }

    pub(crate) fn try_for_each_first_segment_chain<E>(
        &self,
        identity: RunIdentity,
        mut visit: impl FnMut(&CoordSeq) -> Result<(), E>,
    ) -> Result<(), E> {
        match self.for_each_first_segment_chain(identity, |chain| match visit(chain) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }) {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(error) => Err(error),
        }
    }

    pub(crate) fn for_each_first_segment_chain<B>(
        &self,
        identity: RunIdentity,
        mut visit: impl FnMut(&CoordSeq) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        let Some(mut owners) = SharedShapeOwners::for_shape(self) else {
            return self.for_each_segment_chain_inner(&mut None, identity, &mut visit);
        };
        self.for_each_segment_chain_inner(&mut Some(&mut owners), identity, &mut visit)
    }

    /// Visit distinct shared XY chains in first-occurrence order.
    pub(crate) fn for_each_first_xy_segment_chain(
        &self,
        visit: impl FnMut(&CoordSeq) -> ControlFlow<()>,
    ) -> ControlFlow<()> {
        self.for_each_first_segment_chain(RunIdentity::XY, visit)
    }

    fn for_each_segment_chain_inner<B>(
        &self,
        owners: &mut Option<&mut SharedShapeOwners>,
        identity: RunIdentity,
        visit: &mut impl FnMut(&CoordSeq) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => {},
            Self::LineString(points) => {
                if owners
                    .as_mut()
                    .is_none_or(|owners| owners.first_coord_seq(points.as_coords(), identity))
                {
                    visit(points.as_coords())?;
                }
            },
            Self::MultiLineString(lines) => {
                for line in lines {
                    if owners
                        .as_mut()
                        .is_none_or(|owners| owners.first_coord_seq(line.as_coords(), identity))
                    {
                        visit(line.as_coords())?;
                    }
                }
            },
            Self::Polygon(polygon) => {
                for ring in polygon.rings() {
                    if owners
                        .as_mut()
                        .is_none_or(|owners| owners.first_coord_seq(ring.coords(), identity))
                    {
                        visit(ring.coords())?;
                    }
                }
            },
            Self::MultiPolygon(polygons) => {
                for ring in polygons.iter().flat_map(Polygon::rings) {
                    if owners
                        .as_mut()
                        .is_none_or(|owners| owners.first_coord_seq(ring.coords(), identity))
                    {
                        visit(ring.coords())?;
                    }
                }
            },
            Self::GeometryCollection(geometries) => {
                if owners.as_mut().is_none_or(|owners| {
                    owners.first_collection(geometries.as_slice(), geometries.members_are_shared())
                }) {
                    for geometry in geometries {
                        geometry.for_each_segment_chain_inner(owners, identity, visit)?;
                    }
                }
            },
        }
        ControlFlow::Continue(())
    }

    /// Visit every atomic [`Component`], recursing through collections and
    /// skipping empties.
    /// Visit every [`CoordRun`] of this shape in flatten order, recursing
    /// through collections and skipping the empty variants.
    ///
    /// The visitor's return type carries the break: `Result` stops at the
    /// first error, `ControlFlow` at the first `Break`.
    pub(crate) fn for_each_coord_run<'a, R: std::ops::Try<Output = ()>>(
        &'a self,
        visit: &mut impl FnMut(CoordRun<'a>) -> R,
    ) -> R {
        self.for_each_coord_run_inner(&mut None, RunIdentity::Full, visit)
    }

    pub(super) fn for_each_coord_run_inner<'a, R: std::ops::Try<Output = ()>>(
        &'a self,
        owners: &mut Option<&mut SharedShapeOwners>,
        identity: RunIdentity,
        visit: &mut impl FnMut(CoordRun<'a>) -> R,
    ) -> R {
        match self {
            Self::Point(point) => visit(CoordRun::Point(*point))?,
            Self::MultiPoint(points) => {
                if owners
                    .as_mut()
                    .is_none_or(|owners| owners.first_coord_seq(points, identity))
                {
                    visit(CoordRun::Seq(points))?;
                }
            },
            Self::LineString(line) => {
                if owners
                    .as_mut()
                    .is_none_or(|owners| owners.first_coord_seq(line.as_coords(), identity))
                {
                    visit(CoordRun::Seq(line))?;
                }
            },
            Self::MultiLineString(lines) => {
                for line in lines {
                    if owners
                        .as_mut()
                        .is_none_or(|owners| owners.first_coord_seq(line.as_coords(), identity))
                    {
                        visit(CoordRun::Seq(line))?;
                    }
                }
            },
            Self::Polygon(polygon) => {
                for ring in polygon.rings() {
                    if owners
                        .as_mut()
                        .is_none_or(|owners| owners.first_coord_seq(ring.coords(), identity))
                    {
                        visit(CoordRun::Seq(ring.coords()))?;
                    }
                }
            },
            Self::MultiPolygon(polygons) => {
                for ring in polygons.iter().flat_map(Polygon::rings) {
                    if owners
                        .as_mut()
                        .is_none_or(|owners| owners.first_coord_seq(ring.coords(), identity))
                    {
                        visit(CoordRun::Seq(ring.coords()))?;
                    }
                }
            },
            Self::GeometryCollection(parts) => {
                if owners.as_mut().is_none_or(|owners| {
                    owners.first_collection(parts.as_slice(), parts.members_are_shared())
                }) {
                    for part in parts {
                        part.for_each_coord_run_inner(owners, identity, visit)?;
                    }
                }
            },
            Self::Empty(..) => {},
        }
        R::from_output(())
    }

    /// [`Self::try_for_each_component`] for a visitor that cannot fail.
    pub(crate) fn for_each_component<'a>(&'a self, visit: &mut impl FnMut(Component<'a>)) {
        match self.try_for_each_component(
            &mut |component| -> Result<(), std::convert::Infallible> {
                visit(component);
                Ok(())
            },
        ) {
            Ok(()) => {},
            Err(never) => match never {},
        }
    }

    pub(crate) fn try_for_each_component<'a, E>(
        &'a self,
        visit: &mut impl FnMut(Component<'a>) -> Result<(), E>,
    ) -> Result<(), E> {
        self.try_for_each_component_inner(&mut None, visit)
    }

    /// Visit components in order, traversing each shared collection owner once.
    /// Set-valued topology does not need to expand repeated immutable subtrees.
    pub(crate) fn try_for_each_first_component<'a, E>(
        &'a self,
        visit: &mut impl FnMut(Component<'a>) -> Result<(), E>,
    ) -> Result<(), E> {
        let Some(mut owners) = SharedShapeOwners::for_shape(self) else {
            return self.try_for_each_component(visit);
        };
        self.try_for_each_component_inner(&mut Some(&mut owners), visit)
    }

    fn try_for_each_component_inner<'a, E>(
        &'a self,
        owners: &mut Option<&mut SharedShapeOwners>,
        visit: &mut impl FnMut(Component<'a>) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            Self::Point(point) => visit(Component::Point(*point))?,
            Self::LineString(line) => visit(Component::LineString(line))?,
            Self::Polygon(polygon) => visit(Component::Polygon(polygon))?,
            Self::MultiPoint(points) => {
                for point in points {
                    visit(Component::Point(point))?;
                }
            },
            Self::MultiLineString(lines) => {
                for line in lines {
                    visit(Component::LineString(line))?;
                }
            },
            Self::MultiPolygon(polygons) => {
                for polygon in polygons {
                    visit(Component::Polygon(polygon))?;
                }
            },
            Self::GeometryCollection(parts) => {
                if owners.as_mut().is_none_or(|owners| {
                    owners.first_collection(parts.as_slice(), parts.members_are_shared())
                }) {
                    for part in parts {
                        part.try_for_each_component_inner(owners, visit)?;
                    }
                }
            },
            Self::Empty(..) => {},
        }
        Ok(())
    }
}

/// First vertex of the first non-empty line piece, if any.
pub(crate) fn linework_first_point(lines: &[&CoordSeq]) -> Option<Point> {
    lines.iter().find_map(|line| line.first())
}
