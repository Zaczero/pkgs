use super::*;

#[derive(Clone, Copy, Debug)]
struct ShapeCountSummary {
    coordinates: Option<usize>,
    segments: Option<usize>,
}

impl Default for ShapeCountSummary {
    fn default() -> Self {
        Self {
            coordinates: Some(0),
            segments: Some(0),
        }
    }
}

impl ShapeCountSummary {
    fn add_assign(&mut self, other: &Self) {
        self.coordinates = self
            .coordinates
            .zip(other.coordinates)
            .and_then(|(left, right)| left.checked_add(right));
        self.segments = self
            .segments
            .zip(other.segments)
            .and_then(|(left, right)| left.checked_add(right));
    }

    fn sequence(sequence: &CoordSeq, lineal: bool) -> Self {
        let coordinates = sequence.len();
        Self {
            coordinates: Some(coordinates),
            segments: Some(if lineal {
                coordinates.saturating_sub(1)
            } else {
                0
            }),
        }
    }
}

/// One operation's exact logical coordinate and segment counts.
///
/// The memo is local to the query. A cached owner summary is added for every
/// reference to that owner, so sharing avoids repeated subtree scans without
/// changing logical multiplicity. Its source lifetime keeps cached slice
/// addresses from being reused while the memo can still be queried.
#[derive(Default)]
pub(crate) struct ShapeCountQuery<'source> {
    owners: Option<HashMap<ShapeOwnerKey, ShapeCountSummary>>,
    segment_presence: Option<HashMap<ShapeOwnerKey, bool>>,
    // Cached slice addresses stay valid only while every borrowed source lives.
    _source: PhantomData<&'source Shape>,
}

impl<'source> ShapeCountQuery<'source> {
    pub(crate) fn coordinates(&mut self, shape: &'source Shape) -> crate::error::Result<usize> {
        self.coordinate_count(shape)
            .ok_or_else(|| crate::error::ResourceError::SizeOverflow.into())
    }

    pub(crate) fn segments(&mut self, shape: &'source Shape) -> crate::error::Result<usize> {
        self.shape_summary(shape)
            .segments
            .ok_or_else(|| crate::error::ResourceError::SizeOverflow.into())
    }

    pub(crate) fn coordinate_count(&mut self, shape: &'source Shape) -> Option<usize> {
        self.shape_summary(shape).coordinates
    }

    pub(crate) fn coordinates_at_most(&mut self, shape: &'source Shape, limit: usize) -> bool {
        self.coordinate_count(shape)
            .is_some_and(|count| count <= limit)
    }

    pub(crate) fn coordinates_for_shapes(
        &mut self,
        shapes: impl IntoIterator<Item = &'source Shape>,
    ) -> crate::error::Result<usize> {
        let mut total = Some(0_usize);
        for shape in shapes {
            let Some(shape_coordinates) = self.shape_summary(shape).coordinates else {
                total = None;
                break;
            };
            total = total.and_then(|left| left.checked_add(shape_coordinates));
            if total.is_none() {
                break;
            }
        }
        total.ok_or_else(|| crate::error::ResourceError::SizeOverflow.into())
    }

    pub(crate) fn polygon_coordinates(
        &mut self,
        polygon: &'source Polygon,
    ) -> crate::error::Result<usize> {
        self.polygon_summary(polygon)
            .coordinates
            .ok_or_else(|| crate::error::ResourceError::SizeOverflow.into())
    }

    pub(crate) fn has_segments(&mut self, shape: &'source Shape) -> bool {
        match shape {
            Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) => false,
            Shape::LineString(line) => line.len() > 1,
            Shape::MultiLineString(lines) => self.shared_slice_has_segments(
                ShapeOwnerKind::MultiLineValues,
                lines.values.as_slice(),
                lines.values.is_shared(),
                |_, values| values.iter().any(|line| line.len() > 1),
            ),
            Shape::Polygon(polygon) => self.polygon_has_segments(polygon),
            Shape::MultiPolygon(polygons) => self.shared_slice_has_segments(
                ShapeOwnerKind::MultiPolygonValues,
                polygons.values.as_slice(),
                polygons.values.is_shared(),
                |query, values| {
                    values
                        .iter()
                        .any(|polygon| query.polygon_has_segments(polygon))
                },
            ),
            Shape::GeometryCollection(collection) => self.shared_slice_has_segments(
                ShapeOwnerKind::Collection,
                collection.as_slice(),
                collection.members_are_shared(),
                |query, members| members.iter().any(|member| query.has_segments(member)),
            ),
        }
    }

    fn shared_slice_summary<T>(
        &mut self,
        kind: ShapeOwnerKind,
        values: &'source [T],
        shared: bool,
        summarize: impl FnOnce(&mut Self, &'source [T]) -> ShapeCountSummary,
    ) -> ShapeCountSummary {
        let key = shared.then(|| ShapeOwnerKey::for_slice(kind, values));
        if let Some(summary) = key.as_ref().and_then(|key| self.owners.as_ref()?.get(key)) {
            return *summary;
        }
        let summary = summarize(self, values);
        if let Some(key) = key {
            self.owners
                .get_or_insert_with(HashMap::new)
                .insert(key, summary);
        }
        summary
    }

    fn shared_slice_has_segments<T>(
        &mut self,
        kind: ShapeOwnerKind,
        values: &'source [T],
        shared: bool,
        contains_segments: impl FnOnce(&mut Self, &'source [T]) -> bool,
    ) -> bool {
        let key = shared.then(|| ShapeOwnerKey::for_slice(kind, values));
        if let Some(contains_segments) = key
            .as_ref()
            .and_then(|key| self.segment_presence.as_ref()?.get(key))
        {
            return *contains_segments;
        }
        let contains_segments = contains_segments(self, values);
        if let Some(key) = key {
            self.segment_presence
                .get_or_insert_with(HashMap::new)
                .insert(key, contains_segments);
        }
        contains_segments
    }

    fn polygon_has_segments(&mut self, polygon: &'source Polygon) -> bool {
        polygon.shell.len() > 1
            || self.shared_slice_has_segments(
                ShapeOwnerKind::PolygonHoles,
                polygon.holes.as_slice(),
                polygon.holes.is_shared(),
                |_, holes| holes.iter().any(|hole| hole.len() > 1),
            )
    }

    fn shape_summary(&mut self, shape: &'source Shape) -> ShapeCountSummary {
        match shape {
            Shape::Point(_) => ShapeCountSummary {
                coordinates: Some(1),
                segments: Some(0),
            },
            Shape::MultiPoint(points) => ShapeCountSummary::sequence(&points.coords, false),
            Shape::LineString(line) => ShapeCountSummary::sequence(line.as_coords(), true),
            Shape::MultiLineString(lines) => self.shared_slice_summary(
                ShapeOwnerKind::MultiLineValues,
                lines.values.as_slice(),
                lines.values.is_shared(),
                |_query, values| {
                    values
                        .iter()
                        .fold(ShapeCountSummary::default(), |mut total, line| {
                            total.add_assign(&ShapeCountSummary::sequence(line.as_coords(), true));
                            total
                        })
                },
            ),
            Shape::Polygon(polygon) => self.polygon_summary(polygon),
            Shape::MultiPolygon(polygons) => self.shared_slice_summary(
                ShapeOwnerKind::MultiPolygonValues,
                polygons.values.as_slice(),
                polygons.values.is_shared(),
                |query, values| {
                    values
                        .iter()
                        .fold(ShapeCountSummary::default(), |mut total, polygon| {
                            total.add_assign(&query.polygon_summary(polygon));
                            total
                        })
                },
            ),
            Shape::GeometryCollection(collection) => self.shared_slice_summary(
                ShapeOwnerKind::Collection,
                collection.as_slice(),
                collection.members_are_shared(),
                |query, members| {
                    members
                        .iter()
                        .fold(ShapeCountSummary::default(), |mut total, member| {
                            total.add_assign(&query.shape_summary(member));
                            total
                        })
                },
            ),
            Shape::Empty(..) => ShapeCountSummary::default(),
        }
    }

    fn polygon_summary(&mut self, polygon: &'source Polygon) -> ShapeCountSummary {
        let shell = ShapeCountSummary::sequence(polygon.shell.coords(), true);
        let holes = self.shared_slice_summary(
            ShapeOwnerKind::PolygonHoles,
            polygon.holes.as_slice(),
            polygon.holes.is_shared(),
            |_query, holes| {
                holes
                    .iter()
                    .fold(ShapeCountSummary::default(), |mut total, hole| {
                        total.add_assign(&ShapeCountSummary::sequence(hole.coords(), true));
                        total
                    })
            },
        );
        let mut total = shell;
        total.add_assign(&holes);
        total
    }
}
