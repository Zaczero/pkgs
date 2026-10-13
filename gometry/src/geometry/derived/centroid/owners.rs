use super::*;

/// Arc sharing can come from an external alias; keep admitted candidates inline
/// and promote only when more distinct owners need tracking.
pub(super) struct CentroidOwnerMap<K, V, const INLINE: usize = 1> {
    inline: [Option<(K, V)>; INLINE],
    rest: Option<HashMap<K, V>>,
}

impl<K, V, const INLINE: usize> Default for CentroidOwnerMap<K, V, INLINE> {
    fn default() -> Self {
        Self {
            inline: std::array::from_fn(|_| None),
            rest: None,
        }
    }
}

impl<K, V, const INLINE: usize> CentroidOwnerMap<K, V, INLINE>
where
    K: Copy + Eq + Hash,
    V: Clone,
{
    pub(super) fn get(&self, key: &K) -> Option<&V> {
        for (owner, value) in self.inline.iter().flatten() {
            if owner == key {
                return Some(value);
            }
        }
        self.rest.as_ref().and_then(|rest| rest.get(key))
    }

    /// Insert an owner already established as absent by `get`.
    pub(super) fn insert_new(&mut self, key: &K, value: V) {
        debug_assert!(self.get(key).is_none());
        if let Some(slot) = self.inline.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some((*key, value));
            return;
        }
        let old = self
            .rest
            .get_or_insert_with(|| HashMap::with_capacity(1))
            .insert(*key, value);
        debug_assert!(old.is_none());
    }
}

#[derive(Default)]
pub(super) struct CentroidEmptyMemo<'owner> {
    shared: Option<CentroidOwnerMap<ShapeOwnerKey, ()>>,
    _owner: PhantomData<&'owner Shape>,
    #[cfg(test)]
    work: EmptyProbeWork,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct EmptyProbeWork {
    pub(super) owner_visits: usize,
    pub(super) member_slots: usize,
    pub(super) cache_hits: usize,
    pub(super) cache_entries: usize,
}

impl<'owner> CentroidEmptyMemo<'owner> {
    pub(super) fn has_any_point_root(&mut self, shape: &'owner Shape) -> bool {
        let Shape::GeometryCollection(collection) = shape else {
            return shape.any_point(|_| true);
        };
        if collection.depth() == 1 {
            return shape.any_point(|_| true);
        }
        self.collection_has_any_point(collection.as_slice())
    }

    pub(super) fn has_any_point(&mut self, shape: &'owner Shape) -> bool {
        let Shape::GeometryCollection(collection) = shape else {
            #[cfg(test)]
            {
                self.work.owner_visits = self.work.owner_visits.wrapping_add(1);
            }
            return shape.any_point(|_| true);
        };
        let shared = collection.members_are_shared();
        if shared
            && let Some(memo) = self.shared.as_ref()
            && memo
                .get(&ShapeOwnerKey::new(collection.as_slice()))
                .is_some()
        {
            #[cfg(test)]
            {
                self.work.cache_hits = self.work.cache_hits.wrapping_add(1);
            }
            return false;
        }
        #[cfg(test)]
        {
            self.work.owner_visits = self.work.owner_visits.wrapping_add(1);
        }
        let has_point = self.collection_has_any_point(collection.as_slice());
        if shared && !has_point {
            let owner = ShapeOwnerKey::new(collection.as_slice());
            self.shared
                .get_or_insert_with(CentroidOwnerMap::default)
                .insert_new(&owner, ());
            #[cfg(test)]
            {
                self.work.cache_entries = self.work.cache_entries.wrapping_add(1);
            }
        }
        has_point
    }

    pub(super) fn collection_has_any_point(&mut self, members: &'owner [Shape]) -> bool {
        for member in members {
            #[cfg(test)]
            {
                self.work.member_slots = self.work.member_slots.wrapping_add(1);
            }
            let has_point = if matches!(member, Shape::GeometryCollection(_)) {
                self.has_any_point(member)
            } else {
                #[cfg(test)]
                {
                    self.work.owner_visits = self.work.owner_visits.wrapping_add(1);
                }
                member.any_point(|_| true)
            };
            if has_point {
                return true;
            }
        }
        false
    }
}

/// Empty admission for centroid memoizes only shared collection owners. This
/// preserves the `Shape::is_empty` short-circuit while bounding all-empty DAGs.
pub(crate) fn centroid_is_empty(shape: &Shape) -> bool {
    let mut memo = CentroidEmptyMemo::default();
    !memo.has_any_point_root(shape)
}

#[cfg(test)]
pub(super) fn centroid_is_empty_with_work(shape: &Shape) -> (bool, EmptyProbeWork) {
    let mut memo = CentroidEmptyMemo::default();
    let is_empty = !memo.has_any_point_root(shape);
    (is_empty, memo.work)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CentroidDimension {
    Areal,
    Lineal,
    Puntal,
}

pub(super) struct OwnerAdmission<'owner> {
    pub(super) owners: CentroidOwnerMap<ShapeOwnerKey, (), CENTROID_ADMISSION_INLINE_OWNERS>,
    pub(super) repeated_owner: bool,
    pub(super) repeated_work: usize,
    pub(super) _owner: PhantomData<&'owner Shape>,
}

pub(super) const CENTROID_ADMISSION_INLINE_OWNERS: usize = 4;

/// Member slots and XY values admitted after the first repeated owner before
/// switching to the exact fold. This selects an algorithm; it never caps input
/// occurrences.
pub(super) const CENTROID_FAST_REPEAT_WORK_BUDGET: usize = 128;

impl OwnerAdmission<'_> {
    pub(super) fn new() -> Self {
        Self {
            owners: CentroidOwnerMap::default(),
            repeated_owner: false,
            repeated_work: 0,
            _owner: PhantomData,
        }
    }

    pub(super) fn observe(&mut self, key: &ShapeOwnerKey) -> bool {
        if self.owners.get(key).is_some() {
            self.repeated_owner = true;
            return false;
        }
        self.owners.insert_new(key, ());
        false
    }

    pub(super) const fn charge_work(&mut self, amount: usize) -> bool {
        if self.repeated_owner {
            self.repeated_work = self.repeated_work.saturating_add(amount);
        }
        self.needs_exact_fold()
    }

    pub(super) const fn needs_exact_fold(&self) -> bool {
        self.repeated_owner && self.repeated_work > CENTROID_FAST_REPEAT_WORK_BUDGET
    }
}

// Matched D4 Callgrind profiles show +3.54–5.57% instructions; this owner admission keeps shared-DAG work bounded at D8/D12.
pub(super) fn observe_collection_owner<'owner>(
    shape: &'owner Shape,
    admission: &mut Option<OwnerAdmission<'owner>>,
) -> bool {
    let Shape::GeometryCollection(collection) = shape else {
        return false;
    };
    if let Some(admission) = admission.as_ref()
        && admission.repeated_owner
    {
        return admission.needs_exact_fold();
    }
    if !collection.members_are_shared() {
        return false;
    }
    let key = ShapeOwnerKey::new(collection.as_slice());
    admission
        .get_or_insert_with(OwnerAdmission::new)
        .observe(&key)
}

pub(super) fn charge_repeated_work(
    admission: &mut Option<OwnerAdmission<'_>>,
    amount: usize,
) -> bool {
    admission
        .as_mut()
        .is_some_and(|admission| admission.charge_work(amount))
}
