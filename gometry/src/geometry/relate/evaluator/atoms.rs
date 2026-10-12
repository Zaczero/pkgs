use super::contacts::event_point_exact;
use super::*;

pub(super) fn intern_event_points(
    events: &mut [ContactEvent],
    segments: &[Segment],
    forest: &mut ExactPointForest<'_>,
    stored_points: &mut HashMap<PointKey, u32>,
    coordinates: &mut ExactCoordinates,
) -> Result<Vec<Vec<u32>>> {
    let mut segment_cuts = Vec::with_capacity(segments.len());
    segment_cuts.resize_with(segments.len(), Vec::new);
    for event in events {
        let pair = event.segments;
        for point in event.points.iter_mut().flatten() {
            let handle = if let Some(handle) = point.handle {
                handle
            } else {
                let handle = match point.kind {
                    EventPointKind::Stored(coordinate) => {
                        let key = PointKey::new(coordinate);
                        if let Some(&handle) = stored_points.get(&key) {
                            handle
                        } else {
                            let handle = forest.push_stored(coordinate)?;
                            stored_points.insert(key, handle);
                            handle
                        }
                    },
                    EventPointKind::Crossing {
                        segments: crossing_segments,
                        rounded,
                    } => {
                        let exact = event_point_exact(point, segments, coordinates)?;
                        forest.push_crossing_with_exact(crossing_segments, rounded, exact)?
                    },
                };
                point.handle = Some(handle);
                handle
            };
            for segment in pair {
                segment_cuts
                    .get_mut(segment)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "event references a missing staged segment".into(),
                        )
                    })?
                    .push(handle);
            }
        }
    }
    Ok(segment_cuts)
}

pub(super) enum AtomTable<T, const N: usize> {
    Uncut([T; N]),
    Split(Vec<T>),
}

impl<T, const N: usize> std::ops::Deref for AtomTable<T, N> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        match self {
            Self::Uncut(values) => values,
            Self::Split(values) => values,
        }
    }
}

impl<T, const N: usize> std::ops::DerefMut for AtomTable<T, N> {
    fn deref_mut(&mut self) -> &mut [T] {
        match self {
            Self::Uncut(values) => values,
            Self::Split(values) => values,
        }
    }
}

pub(super) type AtomCuts = AtomTable<AtomPoint, 2>;
pub(super) type AtomCoverage = AtomTable<Option<Coverage>, 1>;
pub(super) type OrderedEventCuts = (Vec<AtomCuts>, HashMap<(usize, u32), usize>);

pub(super) fn order_event_cuts(
    segments: &[Segment],
    segment_cuts: &mut [Vec<u32>],
    forest: &mut ExactPointForest<'_>,
    coordinates: &mut ExactCoordinates,
) -> Result<OrderedEventCuts> {
    for (segment_index, cuts) in segment_cuts.iter_mut().enumerate() {
        let segment = *segments
            .get(segment_index)
            .expect("cut list is aligned with the source segment pool");
        let mut equal = Vec::new();
        forest.points().order_along(
            segment,
            cuts,
            |handle| handle,
            |left, right| {
                equal.push((left, right));
                Ok(())
            },
        )?;
        for (left, right) in equal {
            if !forest.points().same_exact_point(left, right)? {
                return Err(GeometryErrorKind::Invalid(
                    "equal cut parameters identify different exact points".into(),
                )
                .into());
            }
            forest.union(left, right)?;
        }
    }

    let mut positions = HashMap::with_hasher(collection_hash_state()?.clone());
    let capacity = segment_cuts.iter().map(Vec::len).sum();
    positions.reserve(capacity);
    let mut boundaries = Vec::with_capacity(segments.len());
    for (segment_index, (segment, cuts)) in segments.iter().zip(segment_cuts).enumerate() {
        let mut ordered = Vec::new();
        let mut previous = None;
        for &handle in cuts.iter() {
            let root = forest.find(handle);
            if previous == Some(root) {
                continue;
            }
            previous = Some(root);
            if point_handle_equals_xy(forest, root, segment.start, coordinates)?
                || point_handle_equals_xy(forest, root, segment.end, coordinates)?
            {
                continue;
            }
            let position = ordered.len().wrapping_add(1);
            ordered.push(AtomPoint::Event(root));
            positions.insert((segment_index, root), position);
        }
        boundaries.push(if ordered.is_empty() {
            AtomTable::Uncut([
                AtomPoint::Endpoint(segment.start),
                AtomPoint::Endpoint(segment.end),
            ])
        } else {
            ordered.insert(0, AtomPoint::Endpoint(segment.start));
            ordered.push(AtomPoint::Endpoint(segment.end));
            AtomTable::Split(ordered)
        });
    }
    Ok((boundaries, positions))
}

pub(super) fn point_handle_equals_xy(
    forest: &ExactPointForest<'_>,
    handle: u32,
    point: XY,
    coordinates: &mut ExactCoordinates,
) -> Result<bool> {
    let arena = forest.points();
    if let Some(key) = arena.stored_key(handle) {
        return Ok(key == PointKey::new(point));
    }
    let exact = arena.exact(handle)?;
    let [x, y, weight] = &*exact;
    let [stored_x, stored_y, one] = coordinates.stored(point);
    Ok(compare_ratios(x, weight, stored_x, one)? == Ordering::Equal
        && compare_ratios(y, weight, stored_y, one)? == Ordering::Equal)
}

pub(super) fn same_atom_point(
    left: AtomPoint,
    right: AtomPoint,
    forest: &ExactPointForest<'_>,
    coordinates: &mut ExactCoordinates,
) -> Result<bool> {
    match (left, right) {
        (AtomPoint::Endpoint(left), AtomPoint::Endpoint(right)) => Ok(same_point(left, right)),
        (AtomPoint::Event(left), AtomPoint::Event(right)) => {
            forest.points().same_exact_point(left, right)
        },
        (AtomPoint::Endpoint(endpoint), AtomPoint::Event(handle))
        | (AtomPoint::Event(handle), AtomPoint::Endpoint(endpoint)) => {
            point_handle_equals_xy(forest, handle, endpoint, coordinates)
        },
    }
}

pub(super) fn atom_position(
    segment_index: usize,
    segment: Segment,
    handle: u32,
    boundary_count: usize,
    positions: &HashMap<(usize, u32), usize>,
    forest: &ExactPointForest<'_>,
    coordinates: &mut ExactCoordinates,
) -> Result<usize> {
    if point_handle_equals_xy(forest, handle, segment.start, coordinates)? {
        return Ok(0);
    }
    if point_handle_equals_xy(forest, handle, segment.end, coordinates)? {
        return Ok(boundary_count.wrapping_sub(1));
    }
    positions
        .get(&(segment_index, handle))
        .copied()
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("collinear cut endpoint is absent from its segment".into())
                .into()
        })
}

pub(super) fn build_atom_coverage(
    events: &[ContactEvent],
    segments: &[Segment],
    components: &[Component],
    boundaries: &[AtomCuts],
    positions: &HashMap<(usize, u32), usize>,
    forest: &mut ExactPointForest<'_>,
    coordinates: &mut ExactCoordinates,
) -> Result<Vec<AtomCoverage>> {
    let mut coverage = boundaries
        .iter()
        .map(|cuts| {
            if cuts.len() == 2 {
                AtomTable::Uncut([None])
            } else {
                AtomTable::Split(vec![None; cuts.len().saturating_sub(1)])
            }
        })
        .collect::<Vec<_>>();
    for event in events {
        let EventKind::Collinear { same_direction } = event.kind else {
            continue;
        };
        let left_component =
            usize::try_from(event.components[0]).map_err(|_| ResourceError::SizeOverflow)?;
        let right_component =
            usize::try_from(event.components[1]).map_err(|_| ResourceError::SizeOverflow)?;
        let left_side = components
            .get(left_component)
            .expect("event left component is admitted")
            .side;
        let right_side = components
            .get(right_component)
            .expect("event right component is admitted")
            .side;
        if left_side == right_side {
            continue;
        }
        let (Some(start), Some(end)) = (
            event
                .points
                .first()
                .and_then(Option::as_ref)
                .and_then(|p| p.handle),
            event
                .points
                .get(1)
                .and_then(Option::as_ref)
                .and_then(|p| p.handle),
        ) else {
            return Err(GeometryErrorKind::Invalid(
                "collinear event has no interned span endpoints".into(),
            )
            .into());
        };
        let start = forest.find(start);
        let end = forest.find(end);
        let [left_segment, right_segment] = event.segments;
        let [left_component, right_component] = event.components;
        for (segment_index, other_component) in [
            (left_segment, right_component),
            (right_segment, left_component),
        ] {
            let segment = *segments
                .get(segment_index)
                .expect("event segment index is admitted");
            let list = boundaries
                .get(segment_index)
                .expect("event segment has its boundary list");
            let first = atom_position(
                segment_index,
                segment,
                start,
                list.len(),
                positions,
                forest,
                coordinates,
            )?;
            let last = atom_position(
                segment_index,
                segment,
                end,
                list.len(),
                positions,
                forest,
                coordinates,
            )?;
            let (lo, hi) = (first.min(last), first.max(last));
            let atoms = coverage
                .get_mut(segment_index)
                .expect("coverage table is aligned with segments");
            for atom in lo..hi {
                let Some(slot) = atoms.get_mut(atom) else {
                    return Err(GeometryErrorKind::Invalid(
                        "collinear span exceeds its segment's atom list".into(),
                    )
                    .into());
                };
                if slot.is_none() {
                    *slot = Some(Coverage {
                        partner_component: other_component,
                        same_direction,
                    });
                }
            }
        }
    }
    Ok(coverage)
}

pub(super) fn cross_event_nodes(
    events: &[ContactEvent],
    components: &[Component],
    forest: &mut ExactPointForest<'_>,
) -> Result<(HashSet<PointKey>, HashSet<u32>)> {
    let mut stored = HashSet::new();
    let mut constructed = HashSet::new();
    for event in events {
        let left = components
            .get(usize::try_from(event.components[0]).map_err(|_| ResourceError::SizeOverflow)?)
            .expect("event left component is admitted")
            .side;
        let right = components
            .get(usize::try_from(event.components[1]).map_err(|_| ResourceError::SizeOverflow)?)
            .expect("event right component is admitted")
            .side;
        if left == right {
            continue;
        }
        for point in event.points.iter().flatten() {
            let Some(handle) = point.handle else {
                return Err(
                    GeometryErrorKind::Invalid("cross event node was not interned".into()).into(),
                );
            };
            let root = forest.find(handle);
            if let Some(key) = forest.points().stored_key(root) {
                stored.insert(key);
            } else {
                constructed.insert(root);
            }
        }
    }
    Ok((stored, constructed))
}

pub(super) fn is_cross_event_node(
    point: AtomPoint,
    forest: &mut ExactPointForest<'_>,
    cross_stored: &HashSet<PointKey>,
    cross_constructed: &HashSet<u32>,
) -> bool {
    match point {
        AtomPoint::Endpoint(point) => cross_stored.contains(&PointKey::new(point)),
        AtomPoint::Event(handle) => {
            let root = forest.find(handle);
            forest.points().stored_key(root).map_or_else(
                || cross_constructed.contains(&root),
                |key| cross_stored.contains(&key),
            )
        },
    }
}
