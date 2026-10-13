use super::*;

#[derive(Clone, Copy)]
pub(crate) enum RunIdentity {
    Full,
    XY,
}

/// The same CoordSeq owner can represent point support and linework support.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum SupportRunKind {
    PointSet,
    SegmentChain,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum SupportRunOwner {
    FullPointSet(VisiblePointRunKey),
    FullSegmentChain(VisiblePointRunKey),
    XyPointSet(VisibleXYKey),
    XySegmentChain(VisibleXYKey),
}

/// Query-local aliases for immutable collection and coordinate owners.
///
/// A single coordinate run cannot recur within a simple Point, MultiPoint,
/// or LineString query. Avoid a per-query owner table on those ordinary paths.
pub(crate) struct SharedShapeOwners {
    collections: HashSet<ShapeOwnerKey>,
    xy_runs: HashSet<VisibleXYKey>,
    full_runs: HashSet<VisiblePointRunKey>,
    support_runs: HashSet<SupportRunOwner>,
    collection_capacity_hint: usize,
    track_runs: bool,
    repeated_owner: bool,
}

impl SharedShapeOwners {
    pub(crate) fn for_collections() -> Self {
        Self {
            collections: HashSet::new(),
            xy_runs: HashSet::new(),
            full_runs: HashSet::new(),
            support_runs: HashSet::new(),
            collection_capacity_hint: 0,
            track_runs: false,
            repeated_owner: false,
        }
    }

    pub(crate) fn for_shape(shape: &Shape) -> Option<Self> {
        let track_runs = shape_may_repeat_coord_run(shape);
        track_runs.then(|| Self {
            collections: HashSet::new(),
            xy_runs: HashSet::new(),
            full_runs: HashSet::new(),
            support_runs: HashSet::new(),
            collection_capacity_hint: shape.nesting_depth(),
            track_runs,
            repeated_owner: false,
        })
    }

    pub(crate) fn first_collection(&mut self, members: &[Shape], shared: bool) -> bool {
        if !shared {
            return true;
        }
        if self.collections.is_empty() && self.collection_capacity_hint > 0 {
            self.collections.reserve(self.collection_capacity_hint);
            self.collection_capacity_hint = 0;
        }
        let owner = ShapeOwnerKey::new(members);
        // Unique member vectors cannot recur in an immutable Shape tree.
        // Avoid hashing the ordinary collection owner that is visited once.
        let first = self.collections.insert(owner);
        self.repeated_owner |= !first;
        first
    }

    pub(crate) fn first_coord_seq(&mut self, coords: &CoordSeq, identity: RunIdentity) -> bool {
        if !self.track_runs {
            return true;
        }
        let first = match identity {
            RunIdentity::Full => coords
                .shared_point_run_key()
                .is_none_or(|key| self.full_runs.insert(key)),
            RunIdentity::XY => coords
                .shared_xy_key()
                .is_none_or(|key| self.xy_runs.insert(key)),
        };
        self.repeated_owner |= !first;
        first
    }

    pub(crate) fn first_support_run(
        &mut self,
        coords: &CoordSeq,
        identity: RunIdentity,
        kind: SupportRunKind,
    ) -> bool {
        if !self.track_runs {
            return true;
        }
        let key = match (identity, kind) {
            (RunIdentity::Full, SupportRunKind::PointSet) => coords
                .shared_point_run_key()
                .map(SupportRunOwner::FullPointSet),
            (RunIdentity::Full, SupportRunKind::SegmentChain) => coords
                .shared_point_run_key()
                .map(SupportRunOwner::FullSegmentChain),
            (RunIdentity::XY, SupportRunKind::PointSet) => {
                coords.shared_xy_key().map(SupportRunOwner::XyPointSet)
            },
            (RunIdentity::XY, SupportRunKind::SegmentChain) => {
                coords.shared_xy_key().map(SupportRunOwner::XySegmentChain)
            },
        };
        let first = key.is_none_or(|key| self.support_runs.insert(key));
        self.repeated_owner |= !first;
        first
    }

    pub(crate) const fn saw_repeated_owner(&self) -> bool {
        self.repeated_owner
    }
}

fn shape_may_repeat_coord_run(shape: &Shape) -> bool {
    match shape {
        Shape::Point(_) | Shape::MultiPoint(_) | Shape::LineString(_) | Shape::Empty(..) => false,
        Shape::MultiLineString(lines) => lines.as_slice().len() > 1,
        Shape::Polygon(polygon) => !polygon.holes.is_empty(),
        Shape::MultiPolygon(polygons) => {
            polygons.as_slice().len() > 1
                || polygons.iter().any(|polygon| !polygon.holes.is_empty())
        },
        Shape::GeometryCollection(members) => {
            members.len() > 1 || members.iter().any(shape_may_repeat_coord_run)
        },
    }
}

fn reserve_owner_map<K: Hash + Eq, V>(owners: &mut HashMap<K, V>, capacity_hint: &mut usize) {
    if owners.is_empty() && *capacity_hint > 0 {
        owners.reserve(*capacity_hint);
        *capacity_hint = 0;
    }
}
