use std::hash::Hash;
use std::marker::PhantomData;
use std::simd::cmp::{SimdPartialEq as _, SimdPartialOrd as _};
use std::simd::num::SimdFloat as _;
use std::simd::{Select as _, StdFloat as _};

use crate::collections::{HashMap, HashMapExt as _};
use crate::error::{ErrorKind, ResourceError, Result as CoreResult};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::exact_arith::product_sum::ExactCentroidSums as FixedExactCentroidSums;
use crate::geometry::tessellation::exact::{ExactDyadic, round_ratio, round_sqrt_ratio};
use crate::geometry::{
    Component, CoordSeq, Coordinates as _, Dimension, GeometryErrorKind, Point, Polygon,
    REDUCE_LANES, REDUCE_SIMD_MIN, ReduceSimd, Ring, Shape, ShapeOwnerKey, axis_pow2_scale,
    canonicalize_zero, centroid_ring_sums, closed_columns_winding, column_mean2, point_distance,
    simd_reduce_f64,
};

/// Arc sharing can come from an external alias; keep admitted candidates inline
/// and promote only when more distinct owners need tracking.
struct CentroidOwnerMap<K, V, const INLINE: usize = 1> {
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
    fn get(&self, key: &K) -> Option<&V> {
        for (owner, value) in self.inline.iter().flatten() {
            if owner == key {
                return Some(value);
            }
        }
        self.rest.as_ref().and_then(|rest| rest.get(key))
    }

    /// Insert an owner already established as absent by `get`.
    fn insert_new(&mut self, key: &K, value: V) {
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
struct CentroidEmptyMemo<'owner> {
    shared: Option<CentroidOwnerMap<ShapeOwnerKey, ()>>,
    _owner: PhantomData<&'owner Shape>,
    #[cfg(test)]
    work: EmptyProbeWork,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct EmptyProbeWork {
    owner_visits: usize,
    member_slots: usize,
    cache_hits: usize,
    cache_entries: usize,
}

impl<'owner> CentroidEmptyMemo<'owner> {
    fn has_any_point_root(&mut self, shape: &'owner Shape) -> bool {
        let Shape::GeometryCollection(collection) = shape else {
            return shape.any_point(|_| true);
        };
        if collection.depth() == 1 {
            return shape.any_point(|_| true);
        }
        self.collection_has_any_point(collection.as_slice())
    }

    fn has_any_point(&mut self, shape: &'owner Shape) -> bool {
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

    fn collection_has_any_point(&mut self, members: &'owner [Shape]) -> bool {
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
fn centroid_is_empty_with_work(shape: &Shape) -> (bool, EmptyProbeWork) {
    let mut memo = CentroidEmptyMemo::default();
    let is_empty = !memo.has_any_point_root(shape);
    (is_empty, memo.work)
}

fn polygon_ring_columns<'a>(
    xs: &'a [f64],
    ys: &'a [f64],
    ring_offsets: &'a [i32],
    rings: std::ops::Range<usize>,
) -> impl Iterator<Item = (bool, &'a [f64], &'a [f64])> + 'a {
    let offsets = ring_offsets
        .get(rings.start..=rings.end)
        .expect("ring offsets cover the admitted ring range");
    offsets
        .array_windows::<2>()
        .enumerate()
        .map(move |(index, &[start, end])| {
            let start = usize::try_from(start).expect("ring offsets are nonnegative");
            let end = usize::try_from(end).expect("ring offsets are nonnegative");
            let xs = xs
                .get(start..end)
                .expect("ring offsets fit the admitted x column");
            let ys = ys
                .get(start..end)
                .expect("ring offsets fit the admitted y column");
            (index != 0, xs, ys)
        })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CentroidDimension {
    Areal,
    Lineal,
    Puntal,
}

struct OwnerAdmission<'owner> {
    owners: CentroidOwnerMap<ShapeOwnerKey, (), CENTROID_ADMISSION_INLINE_OWNERS>,
    repeated_owner: bool,
    repeated_work: usize,
    _owner: PhantomData<&'owner Shape>,
}

const CENTROID_ADMISSION_INLINE_OWNERS: usize = 4;

/// Member slots and XY values admitted after the first repeated owner before
/// switching to the exact fold. This selects an algorithm; it never caps input
/// occurrences.
const CENTROID_FAST_REPEAT_WORK_BUDGET: usize = 128;

impl OwnerAdmission<'_> {
    fn new() -> Self {
        Self {
            owners: CentroidOwnerMap::default(),
            repeated_owner: false,
            repeated_work: 0,
            _owner: PhantomData,
        }
    }

    fn observe(&mut self, key: &ShapeOwnerKey) -> bool {
        if self.owners.get(key).is_some() {
            self.repeated_owner = true;
            return false;
        }
        self.owners.insert_new(key, ());
        false
    }

    const fn charge_work(&mut self, amount: usize) -> bool {
        if self.repeated_owner {
            self.repeated_work = self.repeated_work.saturating_add(amount);
        }
        self.needs_exact_fold()
    }

    const fn needs_exact_fold(&self) -> bool {
        self.repeated_owner && self.repeated_work > CENTROID_FAST_REPEAT_WORK_BUDGET
    }
}

// Matched D4 Callgrind profiles show +3.54–5.57% instructions; this owner admission keeps shared-DAG work bounded at D8/D12.
fn observe_collection_owner<'owner>(
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

fn charge_repeated_work(admission: &mut Option<OwnerAdmission<'_>>, amount: usize) -> bool {
    admission
        .as_mut()
        .is_some_and(|admission| admission.charge_work(amount))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExactAtRoot;

enum CentroidAttempt<T> {
    Ready(T),
    ExactAtRoot(ExactAtRoot),
}

fn exact_coordinate_difference(left: f64, right: f64) -> CoreResult<ExactDyadic> {
    ExactDyadic::from_f64(left).try_subtract(ExactDyadic::from_f64(right))
}

#[expect(
    clippy::float_cmp,
    reason = "exact stored XY equality defines whether a segment has zero length"
)]
fn exact_segment_length(x0: f64, y0: f64, x1: f64, y1: f64) -> CoreResult<Option<ExactDyadic>> {
    if x0 == x1 && y0 == y1 {
        return Ok(None);
    }
    let distance = point_distance(Point::new_raw_xy(x0, y0), Point::new_raw_xy(x1, y1));
    if distance > 0.0 && distance.is_finite() {
        return Ok(Some(ExactDyadic::from_f64(distance)));
    }

    // Keep an unrepresentable world length as a rounded significand plus its
    // exact exponent. Distinct finite binary64 endpoints have positive exact
    // distance even when subtraction or world-scale rounding overflows.
    let dx = exact_coordinate_difference(x1, x0)?;
    let dy = exact_coordinate_difference(y1, y0)?;
    let squared = dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)?;
    debug_assert!(!squared.is_zero());
    let high = *squared.limbs.last().expect("positive squared length");
    let preceding_limbs = squared
        .limbs
        .len()
        .checked_sub(1)
        .expect("positive squared length has at least one limb");
    let limb_bits = usize::try_from(u64::BITS).expect("u64 width fits usize");
    let high_bits = usize::try_from(high.bit_width()).expect("u64 bit width fits usize");
    let bits = preceding_limbs
        .checked_mul(limb_bits)
        .and_then(|bits| bits.checked_add(high_bits))
        .expect("binary64 squared-length width fits usize");
    let highest_bit = bits
        .checked_sub(1)
        .expect("positive squared length has at least one bit");
    let top = squared
        .exponent
        .checked_add(i64::try_from(highest_bit).expect("binary64 squared-length width fits i64"))
        .ok_or(ResourceError::SizeOverflow)?;
    let root_exponent = top.div_euclid(2);
    let mut normalized_squared = squared;
    normalized_squared.exponent = normalized_squared
        .exponent
        .checked_sub(
            root_exponent
                .checked_mul(2)
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .ok_or(ResourceError::SizeOverflow)?;
    let significand = round_sqrt_ratio(&normalized_squared, &ExactDyadic::one())?;
    debug_assert!(significand.is_finite() && (1.0..=2.0).contains(&significand));
    let mut length = ExactDyadic::from_f64(significand);
    length.exponent = length
        .exponent
        .checked_add(root_exponent)
        .ok_or(ResourceError::SizeOverflow)?;
    length.try_trim()?;
    Ok(Some(length))
}

fn accumulate_ring_centroid(
    xs: &[f64],
    ys: &[f64],
    hole: bool,
    base: &mut Option<(f64, f64)>,
    area_sum2: &mut f64,
    cg3: &mut (f64, f64),
) {
    let pairs = xs.len().saturating_sub(1);
    if pairs == 0 {
        return;
    }
    // One GLOBAL base point (the first ring vertex seen) keeps every
    // triangle small. The SIMD `centroid_ring_sums` twin of the shoelace
    // fold sums the ring's SIGNED triangle-fan contributions (cross-product
    // areas about the anchor); shells and holes receive opposite signs based
    // on their declared role and winding.
    let Some((&x, &y)) = xs.first().zip(ys.first()) else {
        return;
    };
    let anchor = *base.get_or_insert((x, y));
    let ccw = closed_columns_winding(xs, ys, pairs).is_ccw();
    let sign = if hole == ccw { 1.0 } else { -1.0 };
    let (area2, cgx, cgy) = centroid_ring_sums(xs, ys, pairs, anchor);
    *area_sum2 += sign * area2;
    cg3.0 += sign * cgx;
    cg3.1 += sign * cgy;
}

fn finish_areal_centroid(area_sum2: f64, cg3: (f64, f64)) -> Option<(f64, f64, f64)> {
    // Signed triangle-fan sums can cancel to -0.0 on a symmetric polygon; the
    // lineal/puntal paths happen to land on +0.0, so normalize here to keep the
    // +0.0 invariant uniform across centroid of every geometry type.
    // Reject non-finite so a moment overflow cannot reach `Point::new`.
    // Reject subnormal |area|: cg/area collapses toward the origin even when
    // the ring is a positive square (e.g. [0,2e-162]²) — leave None for the
    // exact-areal / lineal cascade.
    let area_abs = area_sum2.abs();
    if area_abs > 0.0 && !area_sum2.is_normal() {
        return None;
    }
    (area_abs > 0.0)
        .then(|| {
            (
                area_abs / 2.0,
                canonicalize_zero(cg3.0 / 3.0 / area_sum2),
                canonicalize_zero(cg3.1 / 3.0 / area_sum2),
            )
        })
        .filter(|(_, x, y)| x.is_finite() && y.is_finite())
}

/// Exact areal centroid over original stored coordinates.  This cold fallback
/// avoids both shared-frame cancellation and a rounded ring-orientation gate.
fn exact_areal_centroid(
    visit: impl Fn(&mut dyn FnMut(&[f64], &[f64], bool)),
) -> Option<(f64, f64, f64)> {
    let mut sums = FixedExactCentroidSums::zero();
    visit(&mut |xs, ys, hole| {
        let mut ring = FixedExactCentroidSums::zero();
        for (&[x0, x1], &[y0, y1]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
            ring.add_edge(x0, y0, x1, y1);
        }
        sums.merge_ring(&ring, hole);
    });
    sums.finish_with_area()
}

/// Column kernel for polygonal centroid (dimension-2 pass) over one polygon's
/// ring window range.
/// Use the exact original-coordinate centroid when world-space moments lose digits:
/// tiny extents (subnormal / near-subnormal area) or huge coordinates.
const AREAL_EXACT_MIN_EXTENT: f64 = 1e-8;
const AREAL_EXACT_MAX_COORD: f64 = 1e12;

fn centroid_polygon_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> Option<(f64, f64, f64)> {
    let exact = || {
        exact_areal_centroid(|visit| {
            for (hole, ring_xs, ring_ys) in
                polygon_ring_columns(xs, ys, ring_offsets, rings.clone())
            {
                visit(ring_xs, ring_ys, hole);
            }
        })
    };
    // Classical fan + extent in one ring walk (no separate pre-scan).
    let mut base = None;
    let mut area_sum2 = 0.0_f64;
    let mut cg3 = (0.0_f64, 0.0_f64);
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    for (hole, rxs, rys) in polygon_ring_columns(xs, ys, ring_offsets, rings.clone()) {
        for (&x, &y) in rxs.iter().zip(rys.iter()) {
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
        accumulate_ring_centroid(rxs, rys, hole, &mut base, &mut area_sum2, &mut cg3);
    }
    let extreme = if min_x.is_finite() {
        let extent = (max_x - min_x).max(max_y - min_y);
        let max_abs = max_x
            .abs()
            .max(min_x.abs())
            .max(max_y.abs())
            .max(min_y.abs());
        extent < AREAL_EXACT_MIN_EXTENT || max_abs > AREAL_EXACT_MAX_COORD
    } else {
        false
    };
    if extreme {
        return exact();
    }
    // Ordinary coordinates retain the single-pass triangle fan.
    finish_areal_centroid(area_sum2, cg3).or_else(exact)
}

#[expect(
    clippy::indexing_slicing,
    reason = "segment and shifted coordinates stay inside admitted equal-length coordinate columns"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the incremented segment index is bounded by the admitted coordinate columns"
)]
fn lineal_centroid_segment_contrib(xs: &[f64], ys: &[f64], segment: usize) -> (f64, f64, f64) {
    let length = point_distance(
        Point::new_raw_xy(xs[segment], ys[segment]),
        Point::new_raw_xy(xs[segment + 1], ys[segment + 1]),
    );
    (
        length,
        length.algebraic_mul(f64::midpoint(xs[segment], xs[segment + 1])),
        length.algebraic_mul(f64::midpoint(ys[segment], ys[segment + 1])),
    )
}

fn lineal_centroid_scalar_plain(
    xs: &[f64],
    ys: &[f64],
    range: std::ops::Range<usize>,
) -> (f64, f64, f64) {
    // Use the shared point-distance (hypot rescue on untrusted squares) so
    // length matches `.length` / `point_distance` at subnormal scales. The
    // length·midpoint product may still underfloor — finish rescues via online.
    if range.len() < 2 {
        return range.fold((0.0_f64, 0.0_f64, 0.0_f64), |(tl, lx, ly), segment| {
            let (length, cx, cy) = lineal_centroid_segment_contrib(xs, ys, segment);
            (tl + length, lx + cx, ly + cy)
        });
    }

    let mut total_length = 0.0_f64;
    let mut moment_x = 0.0_f64;
    let mut moment_y = 0.0_f64;
    let mut moment_x_signs = 0_u8;
    let mut moment_y_signs = 0_u8;
    for segment in range.clone() {
        let (length, cx, cy) = lineal_centroid_segment_contrib(xs, ys, segment);
        total_length += length;
        moment_x += cx;
        moment_y += cy;
        moment_x_signs |= if cx > 0.0 {
            0b01
        } else if cx < 0.0 {
            0b10
        } else {
            0
        };
        moment_y_signs |= if cy > 0.0 {
            0b01
        } else if cy < 0.0 {
            0b10
        } else {
            0
        };
    }

    if moment_x_signs == 0b11 || moment_y_signs == 0b11 {
        let mut compensated_x = NeumaierSum::default();
        let mut compensated_y = NeumaierSum::default();
        for segment in range {
            let (_, cx, cy) = lineal_centroid_segment_contrib(xs, ys, segment);
            if moment_x_signs == 0b11 {
                compensated_x.add(cx);
            }
            if moment_y_signs == 0b11 {
                compensated_y.add(cy);
            }
        }
        if moment_x_signs == 0b11 {
            moment_x = compensated_x.total();
        }
        if moment_y_signs == 0b11 {
            moment_y = compensated_y.total();
        }
    }
    (total_length, moment_x, moment_y)
}

fn lineal_centroid_scalar_algebraic(
    xs: &[f64],
    ys: &[f64],
    range: std::ops::Range<usize>,
) -> (f64, f64, f64) {
    range.fold((0.0_f64, 0.0_f64, 0.0_f64), |(tl, lx, ly), segment| {
        let (length, cx, cy) = lineal_centroid_segment_contrib(xs, ys, segment);
        (
            tl.algebraic_add(length),
            lx.algebraic_add(cx),
            ly.algebraic_add(cy),
        )
    })
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "the SIMD floating-point expressions and reducer-bounded chunk arithmetic preserve the centroid's established operation order"
)]
#[expect(
    clippy::indexing_slicing,
    reason = "simd_reduce_f64 supplies lane-aligned starts within the full shifted as_chunks slices derived from equal-length columns"
)]
fn lineal_centroid_column_sums(xs: &[f64], ys: &[f64]) -> (f64, f64, f64) {
    let segments = xs.len().saturating_sub(1);
    if segments < REDUCE_SIMD_MIN {
        return lineal_centroid_scalar_plain(xs, ys, 0..segments);
    }
    let simd_moment_sum = |accumulated: ReduceSimd, scalar: f64| {
        let zero = ReduceSimd::splat(0.0);
        let has_positive = accumulated.simd_gt(zero).any() || scalar > 0.0;
        let has_negative = accumulated.simd_lt(zero).any() || scalar < 0.0;
        if has_positive && has_negative {
            let mut sum = NeumaierSum::default();
            for lane in accumulated.to_array() {
                sum.add(lane);
            }
            sum.add(scalar);
            sum.total()
        } else {
            accumulated.reduce_sum().algebraic_add(scalar)
        }
    };
    let (x0, _) = xs[..segments].as_chunks::<REDUCE_LANES>();
    let (x1, _) = xs[1..=segments].as_chunks::<REDUCE_LANES>();
    let (y0, _) = ys[..segments].as_chunks::<REDUCE_LANES>();
    let (y1, _) = ys[1..=segments].as_chunks::<REDUCE_LANES>();
    let simd_fold = |mut acc: (ReduceSimd, ReduceSimd, ReduceSimd),
                     mut scalar: (f64, f64, f64),
                     start: usize| {
        let index = start / REDUCE_LANES;
        let dx = ReduceSimd::from_array(x1[index]) - ReduceSimd::from_array(x0[index]);
        let dy = ReduceSimd::from_array(y1[index]) - ReduceSimd::from_array(y0[index]);
        let squared = dx * dx + dy * dy;
        let length = squared.sqrt();
        let zero_delta = dx.simd_eq(ReduceSimd::splat(0.0)) & dy.simd_eq(ReduceSimd::splat(0.0));
        let underflow = squared.simd_eq(ReduceSimd::splat(0.0)) & !zero_delta;
        if length.is_finite().all() && !underflow.any() {
            let half = ReduceSimd::splat(0.5);
            let mid_x =
                (ReduceSimd::from_array(x0[index]) + ReduceSimd::from_array(x1[index])) * half;
            let mid_y =
                (ReduceSimd::from_array(y0[index]) + ReduceSimd::from_array(y1[index])) * half;
            acc.0 += length;
            acc.1 += length * mid_x;
            acc.2 += length * mid_y;
        } else {
            for segment in start..start + REDUCE_LANES {
                if segment < segments {
                    let (length, cx, cy) = lineal_centroid_segment_contrib(xs, ys, segment);
                    scalar.0 = scalar.0.algebraic_add(length);
                    scalar.1 = scalar.1.algebraic_add(cx);
                    scalar.2 = scalar.2.algebraic_add(cy);
                }
            }
        }
        (acc, scalar)
    };
    let lanes = x0.len() * REDUCE_LANES;
    simd_reduce_f64(
        segments,
        (
            ReduceSimd::splat(0.0),
            ReduceSimd::splat(0.0),
            ReduceSimd::splat(0.0),
        ),
        (0.0_f64, 0.0_f64, 0.0_f64),
        simd_fold,
        |scalar, range| {
            if range.start == 0 {
                return lineal_centroid_scalar_algebraic(xs, ys, range);
            }
            let start = segments - REDUCE_LANES;
            let dx =
                ReduceSimd::from_slice(&xs[start + 1..]) - ReduceSimd::from_slice(&xs[start..]);
            let dy =
                ReduceSimd::from_slice(&ys[start + 1..]) - ReduceSimd::from_slice(&ys[start..]);
            let lane_index = ReduceSimd::from_array(std::array::from_fn(|lane| lane as f64));
            let fresh = lane_index.simd_ge(ReduceSimd::splat(f64::from((lanes - start) as u32)));
            let squared = dx * dx + dy * dy;
            let length = fresh.select(squared.sqrt(), ReduceSimd::splat(0.0));
            let zero_delta =
                dx.simd_eq(ReduceSimd::splat(0.0)) & dy.simd_eq(ReduceSimd::splat(0.0));
            let underflow = fresh & squared.simd_eq(ReduceSimd::splat(0.0)) & !zero_delta;
            if length.is_finite().all() && !underflow.any() {
                let half = ReduceSimd::splat(0.5);
                let mid_x = (ReduceSimd::from_slice(&xs[start..])
                    + ReduceSimd::from_slice(&xs[start + 1..]))
                    * half;
                let mid_y = (ReduceSimd::from_slice(&ys[start..])
                    + ReduceSimd::from_slice(&ys[start + 1..]))
                    * half;
                let contrib_x = fresh.select(length * mid_x, ReduceSimd::splat(0.0));
                let contrib_y = fresh.select(length * mid_y, ReduceSimd::splat(0.0));
                let contrib_len = fresh.select(length, ReduceSimd::splat(0.0));
                (
                    scalar.0.algebraic_add(contrib_len.reduce_sum()),
                    scalar.1.algebraic_add(simd_moment_sum(contrib_x, 0.0)),
                    scalar.2.algebraic_add(simd_moment_sum(contrib_y, 0.0)),
                )
            } else {
                let (added_len, added_x, added_y) =
                    lineal_centroid_scalar_algebraic(xs, ys, lanes..segments);
                (
                    scalar.0.algebraic_add(added_len),
                    scalar.1.algebraic_add(added_x),
                    scalar.2.algebraic_add(added_y),
                )
            }
        },
        |(len_acc, x_acc, y_acc), scalar| {
            (
                len_acc.reduce_sum().algebraic_add(scalar.0),
                simd_moment_sum(x_acc, scalar.1),
                simd_moment_sum(y_acc, scalar.2),
            )
        },
    )
}

fn accumulate_lineal_centroid(
    xs: &[f64],
    ys: &[f64],
    total_length: &mut f64,
    line_sum: &mut (f64, f64),
) {
    if xs.len() < 2 {
        return;
    }
    let (len, sum_x, sum_y) = lineal_centroid_column_sums(xs, ys);
    *total_length += len;
    line_sum.0 += sum_x;
    line_sum.1 += sum_y;
}

fn finish_lineal_centroid(total_length: f64, line_sum: (f64, f64)) -> Option<(f64, f64)> {
    (total_length > 0.0).then(|| (line_sum.0 / total_length, line_sum.1 / total_length))
}

/// Exact fallback over these physical coordinate runs. Zero-length segments
/// contribute nothing; positive lengths retain their exponent, so no
/// world-scale frame can be moved by a zero-weight run or erase a tiny weight.
fn exact_lineal_centroid_runs(
    revisit: impl Fn(&mut dyn FnMut(&[f64], &[f64])),
) -> Option<(f64, f64)> {
    let mut sums = GrowableCentroidSums::zero();
    revisit(&mut |xs, ys| {
        debug_assert_eq!(xs.len(), ys.len());
        for ((&x0, &x1), (&y0, &y1)) in xs
            .iter()
            .zip(xs.iter().skip(1))
            .zip(ys.iter().zip(ys.iter().skip(1)))
        {
            let Some(length) = exact_segment_length(x0, y0, x1, y1)
                .expect("finite binary64 line terms fit the growable centroid bound")
            else {
                continue;
            };
            let midpoint_x = ExactDyadic::from_f64(f64::midpoint(x0, x1));
            let midpoint_y = ExactDyadic::from_f64(f64::midpoint(y0, y1));
            let term = GrowableCentroidSums {
                measure: length.clone(),
                moment_x: length
                    .try_product(&midpoint_x)
                    .expect("finite binary64 line moment fits the growable bound"),
                moment_y: length
                    .try_product(&midpoint_y)
                    .expect("finite binary64 line moment fits the growable bound"),
            };
            let previous = std::mem::replace(&mut sums, GrowableCentroidSums::zero());
            sums = previous
                .add(term)
                .expect("finite binary64 line sum fits the growable centroid bound");
        }
    });
    exact_summary_centroid(sums, CentroidDimension::Lineal)
        .expect("finite binary64 line quotient fits the growable centroid bound")
}

/// [`finish_lineal_centroid`] with a growable exact rescue. The
/// `Σ length·midpoint` fast-path can overflow, underflow, or lose cancellation
/// tails even when the finite source has a finite centroid. Replay this one
/// physical owner through exact dyadic weights and moments; zero-length runs
/// cannot affect a frame because the rescue uses no world-coordinate frame.
fn finish_lineal_centroid_rescued(
    total_length: f64,
    line_sum: (f64, f64),
    revisit: impl Fn(&mut dyn FnMut(&[f64], &[f64])),
) -> Option<(f64, f64)> {
    match finish_lineal_centroid(total_length, line_sum) {
        Some((x, y)) if x.is_finite() && y.is_finite() => {
            // A subnormal total or weighted sum may have rounded away centroid
            // bits even when the quotient is finite and nonzero. Exact
            // cancellation to the origin is also worth resolving.
            let underresolved = !total_length.is_normal()
                || (line_sum.0 != 0.0 && !line_sum.0.is_normal())
                || (line_sum.1 != 0.0 && !line_sum.1.is_normal());
            if underresolved || (total_length > 0.0 && x == 0.0 && y == 0.0) {
                return exact_lineal_centroid_runs(revisit).or(Some((x, y)));
            }
            Some((x, y))
        },
        Some(_) => exact_lineal_centroid_runs(revisit),
        None => None,
    }
}

/// Column kernel for lineal centroid (dimension-1 pass) over one coordinate
/// run.
pub(super) fn centroid_line_columns(xs: &[f64], ys: &[f64]) -> Option<(f64, f64)> {
    let mut total_length = 0.0_f64;
    let mut line_sum = (0.0_f64, 0.0_f64);
    accumulate_lineal_centroid(xs, ys, &mut total_length, &mut line_sum);
    finish_lineal_centroid_rescued(total_length, line_sum, |visit| visit(xs, ys))
}

/// Zero-area polygon fallback: length-weighted edge midpoints over all rings.
fn centroid_polygon_lineal_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> Option<(f64, f64)> {
    let mut total_length = 0.0_f64;
    let mut line_sum = (0.0_f64, 0.0_f64);
    for (_, ring_xs, ring_ys) in polygon_ring_columns(xs, ys, ring_offsets, rings.clone()) {
        accumulate_lineal_centroid(ring_xs, ring_ys, &mut total_length, &mut line_sum);
    }
    finish_lineal_centroid_rescued(total_length, line_sum, |visit| {
        for (_, ring_xs, ring_ys) in polygon_ring_columns(xs, ys, ring_offsets, rings.clone()) {
            visit(ring_xs, ring_ys);
        }
    })
}

/// Packed-line row kernel: lineal centroid, then the first-vertex puntal
/// anchor.
pub(crate) fn centroid_line_row_columns(xs: &[f64], ys: &[f64]) -> Option<(f64, f64)> {
    centroid_line_columns(xs, ys).or_else(|| xs.first().zip(ys.first()).map(|(&x, &y)| (x, y)))
}

/// Packed-polygon row kernel: areal → lineal boundary → first-shell-vertex
/// cascade.
pub(crate) fn centroid_polygon_row_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> Option<(f64, f64)> {
    centroid_polygon_columns(xs, ys, ring_offsets, rings.clone())
        .map(|(_, x, y)| (x, y))
        .or_else(|| {
            centroid_polygon_lineal_columns(xs, ys, ring_offsets, rings.clone()).or_else(|| {
                let start = usize::try_from(
                    *ring_offsets
                        .get(rings.start)
                        .expect("polygon row has an admitted ring start"),
                )
                .expect("ring offsets are nonnegative");
                xs.get(start).zip(ys.get(start)).map(|(&x, &y)| (x, y))
            })
        })
}

/// Single-polygon areal centroid (shell + holes). One triangle fan also tracks
/// extent; exact accumulation runs only when the fan rejects or the extent is
/// extreme. Ordinary mid-range polygons pay a single vertex pass (no separate
/// pre-scan).
fn areal_centroid_polygon(polygon: &Polygon) -> Option<(f64, f64, f64)> {
    let exact = || {
        exact_areal_centroid(|visit| {
            for (ring, hole) in polygon_rings(polygon) {
                let Some((xs, ys)) = ring.xy_columns() else {
                    continue;
                };
                visit(xs, ys, hole);
            }
        })
    };
    let mut base = None;
    let mut area_sum2 = 0.0_f64;
    let mut cg3 = (0.0_f64, 0.0_f64);
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    for (ring, hole) in polygon_rings(polygon) {
        let Some((xs, ys)) = ring.xy_columns() else {
            continue;
        };
        for (&x, &y) in xs.iter().zip(ys.iter()) {
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
        accumulate_ring_centroid(xs, ys, hole, &mut base, &mut area_sum2, &mut cg3);
    }
    let extreme = if min_x.is_finite() {
        let extent = (max_x - min_x).max(max_y - min_y);
        let max_abs = max_x
            .abs()
            .max(min_x.abs())
            .max(max_y.abs())
            .max(min_y.abs());
        extent < AREAL_EXACT_MIN_EXTENT || max_abs > AREAL_EXACT_MAX_COORD
    } else {
        false
    };
    // Extreme extents: prefer exact accumulation (classical moments overflow / lose digits).
    if extreme {
        return exact();
    }
    finish_areal_centroid(area_sum2, cg3).or_else(exact)
}

/// Streaming Neumaier accumulator for scalar cancellation and component merges,
/// without staging contributions.
#[derive(Default)]
struct NeumaierSum {
    sum: f64,
    correction: f64,
}

impl NeumaierSum {
    fn add(&mut self, value: f64) {
        let next = self.sum + value;
        if self.sum.abs() >= value.abs() {
            self.correction += (self.sum - next) + value;
        } else {
            self.correction += (value - next) + self.sum;
        }
        self.sum = next;
    }

    fn total(self) -> f64 {
        self.sum + self.correction
    }
}

fn neumaier_sum(values: impl IntoIterator<Item = f64>) -> f64 {
    let mut sum = NeumaierSum::default();
    for value in values {
        sum.add(value);
    }
    sum.total()
}

fn positive_weight_normalizes_to_zero(weight: f64, normalized: f64) -> bool {
    weight > 0.0 && normalized == 0.0
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WeightedCentroidMergeFallback {
    None,
    ReplayWholeSource,
    PositiveWeightNormalizationUnderflow,
}

struct WeightedCentroidMergeResult {
    centroid: Option<(f64, f64)>,
    fallback: WeightedCentroidMergeFallback,
}

/// Area/length-weighted merge of component centroids. Ordinary mid-range
/// inputs use a flat weighted sum (bit-stable). Large absolute coordinates or
/// detected cancellation scale by a power-of-two first and Neumaier-sum the
/// scaled weighted coordinates so order cannot move the result (N6).
///
/// Finite weights are normalized by their finite maximum before *both* the
/// denominator and coordinate moments. Infinite weights and nonfinite lineal
/// centers require whole-source replay before any moment is accumulated.
///
/// With `LINEAL_PARTS_ADMITTED`, the first scan owns replay admission; the fast
/// and stable moment scans can then skip their repeated validity checks.
/// Lineal parts come from positive sums of nonnegative segment lengths, so
/// after replay sentinels are removed their weights are positive and finite.
fn weighted_centroid_merge<
    const DETECT_POSITIVE_WEIGHT_UNDERFLOW: bool,
    const LINEAL_PARTS_ADMITTED: bool,
>(
    parts: &[(f64, f64, f64)],
) -> WeightedCentroidMergeResult {
    if parts.is_empty() {
        return WeightedCentroidMergeResult {
            centroid: None,
            fallback: WeightedCentroidMergeFallback::None,
        };
    }
    let mut max_weight = 0.0_f64;
    let mut max_abs_x = 0.0_f64;
    let mut max_abs_y = 0.0_f64;
    for &(w, cx, cy) in parts {
        if LINEAL_PARTS_ADMITTED {
            if lineal_part_requires_exact_replay(w, cx, cy) {
                return WeightedCentroidMergeResult {
                    centroid: None,
                    fallback: WeightedCentroidMergeFallback::ReplayWholeSource,
                };
            }
        } else {
            if !(w > 0.0 && cx.is_finite() && cy.is_finite()) {
                continue;
            }
            if !w.is_finite() {
                return WeightedCentroidMergeResult {
                    centroid: None,
                    fallback: WeightedCentroidMergeFallback::None,
                };
            }
        }
        max_weight = max_weight.max(w);
        max_abs_x = max_abs_x.max(cx.abs());
        max_abs_y = max_abs_y.max(cy.abs());
    }
    if !(max_weight > 0.0 && max_weight.is_finite()) {
        return WeightedCentroidMergeResult {
            centroid: None,
            fallback: WeightedCentroidMergeFallback::None,
        };
    }
    // Fast path: unscaled weighted sum when coordinates are mid-range.
    if max_abs_x.max(max_abs_y) <= AREAL_EXACT_MAX_COORD {
        let mut sx = 0.0_f64;
        let mut sy = 0.0_f64;
        let mut w_sum = 0.0_f64;
        for &(w, cx, cy) in parts {
            if !(LINEAL_PARTS_ADMITTED
                || (w > 0.0 && w.is_finite() && cx.is_finite() && cy.is_finite()))
            {
                continue;
            }
            let normalized = w / max_weight;
            if DETECT_POSITIVE_WEIGHT_UNDERFLOW && positive_weight_normalizes_to_zero(w, normalized)
            {
                return WeightedCentroidMergeResult {
                    centroid: None,
                    fallback: WeightedCentroidMergeFallback::PositiveWeightNormalizationUnderflow,
                };
            }
            w_sum += normalized;
            sx += normalized * cx;
            sy += normalized * cy;
        }
        let (mx, my) = (sx / w_sum, sy / w_sum);
        if mx.is_finite() && my.is_finite() {
            // Cancellation guard: if the mean is tiny vs max |c|, recompute.
            if max_abs_x.max(max_abs_y) == 0.0
                || mx.abs().max(my.abs()) >= max_abs_x.max(max_abs_y) * f64::EPSILON * 32.0
            {
                return WeightedCentroidMergeResult {
                    centroid: Some((canonicalize_zero(mx), canonicalize_zero(my))),
                    fallback: WeightedCentroidMergeFallback::None,
                };
            }
        }
    }
    // Stable path: scale each coordinate axis independently, then
    // Neumaier-sum the normalized-weight moments. A single scale chosen from
    // huge X makes a finite subnormal Y round to zero before it is merged.
    // The normalization is load-bearing: doing it only in the coordinate sums
    // still lets the denominator overflow.
    let scale_x = axis_pow2_scale(max_abs_x);
    let scale_y = axis_pow2_scale(max_abs_y);
    let inv_scale_x = 1.0 / scale_x;
    let inv_scale_y = 1.0 / scale_y;
    let mut w_sum = 0.0_f64;
    let mut x_sum = NeumaierSum::default();
    let mut y_sum = NeumaierSum::default();
    for &(w, cx, cy) in parts {
        if !(LINEAL_PARTS_ADMITTED
            || (w > 0.0 && w.is_finite() && cx.is_finite() && cy.is_finite()))
        {
            continue;
        }
        let normalized = w / max_weight;
        if DETECT_POSITIVE_WEIGHT_UNDERFLOW && positive_weight_normalizes_to_zero(w, normalized) {
            return WeightedCentroidMergeResult {
                centroid: None,
                fallback: WeightedCentroidMergeFallback::PositiveWeightNormalizationUnderflow,
            };
        }
        w_sum += normalized;
        x_sum.add(normalized * (cx * scale_x));
        y_sum.add(normalized * (cy * scale_y));
    }
    if w_sum <= 0.0 {
        return WeightedCentroidMergeResult {
            centroid: None,
            fallback: WeightedCentroidMergeFallback::None,
        };
    }
    WeightedCentroidMergeResult {
        centroid: Some((
            canonicalize_zero((x_sum.total() / w_sum) * inv_scale_x),
            canonicalize_zero((y_sum.total() / w_sum) * inv_scale_y),
        )),
        fallback: WeightedCentroidMergeFallback::None,
    }
}

#[derive(Clone)]
struct GrowableCentroidSums {
    measure: ExactDyadic,
    moment_x: ExactDyadic,
    moment_y: ExactDyadic,
}

impl GrowableCentroidSums {
    fn zero() -> Self {
        Self {
            measure: ExactDyadic::zero(),
            moment_x: ExactDyadic::zero(),
            moment_y: ExactDyadic::zero(),
        }
    }

    fn add(self, other: Self) -> CoreResult<Self> {
        Ok(Self {
            measure: self.measure.try_add(other.measure)?,
            moment_x: self.moment_x.try_add(other.moment_x)?,
            moment_y: self.moment_y.try_add(other.moment_y)?,
        })
    }

    fn scaled(self, occurrences: usize) -> CoreResult<Self> {
        if occurrences == 1 {
            return Ok(self);
        }
        let factor = ExactDyadic::try_from_u128(
            false,
            u128::try_from(occurrences).expect("a usize occurrence count fits in u128"),
            0,
        )?;
        Ok(Self {
            measure: self.measure.try_product(&factor)?,
            moment_x: self.moment_x.try_product(&factor)?,
            moment_y: self.moment_y.try_product(&factor)?,
        })
    }

    fn negate(self) -> Self {
        Self {
            measure: self.measure.neg(),
            moment_x: self.moment_x.neg(),
            moment_y: self.moment_y.neg(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum RoundedExactRatio {
    Value(f64),
    OutsideBinary64,
}

fn rounded_exact_ratio(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
) -> CoreResult<RoundedExactRatio> {
    match round_ratio(numerator, denominator) {
        Ok(value) => Ok(RoundedExactRatio::Value(value)),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::Geometry(GeometryErrorKind::ExactCoordinateOutOfRange)
            ) =>
        {
            Ok(RoundedExactRatio::OutsideBinary64)
        },
        Err(error) => Err(error),
    }
}

fn exact_product(left: f64, right: f64) -> CoreResult<ExactDyadic> {
    ExactDyadic::from_f64(left).try_product(&ExactDyadic::from_f64(right))
}

fn exact_cross(x0: f64, y0: f64, x1: f64, y1: f64) -> CoreResult<ExactDyadic> {
    exact_product(x0, y1)?.try_subtract(exact_product(x1, y0)?)
}

const fn centroid_coordinate_outside() -> f64 {
    f64::INFINITY
}

fn exact_summary_centroid(
    summary: GrowableCentroidSums,
    tier: CentroidDimension,
) -> CoreResult<Option<(f64, f64)>> {
    if summary.measure.is_zero() {
        return Ok(None);
    }
    let denominator = match tier {
        CentroidDimension::Areal => summary.measure.try_product(&ExactDyadic::from_f64(3.0))?,
        CentroidDimension::Lineal | CentroidDimension::Puntal => summary.measure,
    };
    let rounded_x = rounded_exact_ratio(&summary.moment_x, &denominator)?;
    let rounded_y = rounded_exact_ratio(&summary.moment_y, &denominator)?;
    match tier {
        CentroidDimension::Areal => {
            let (RoundedExactRatio::Value(x), RoundedExactRatio::Value(y)) = (rounded_x, rounded_y)
            else {
                return Ok(None);
            };
            Ok(Some((canonicalize_zero(x), canonicalize_zero(y))))
        },
        CentroidDimension::Lineal | CentroidDimension::Puntal => {
            let x = match rounded_x {
                RoundedExactRatio::Value(value) => value,
                RoundedExactRatio::OutsideBinary64 => centroid_coordinate_outside(),
            };
            let y = match rounded_y {
                RoundedExactRatio::Value(value) => value,
                RoundedExactRatio::OutsideBinary64 => centroid_coordinate_outside(),
            };
            let point = (x, y);
            Ok(Some(if tier == CentroidDimension::Lineal {
                (canonicalize_zero(point.0), canonicalize_zero(point.1))
            } else {
                point
            }))
        },
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CentroidWorkStats {
    owner_visits: usize,
    member_slots: usize,
    source_xy: usize,
    cache_hits: usize,
}

struct ExactOwnerFold<'owner> {
    tier: CentroidDimension,
    owners: CentroidOwnerMap<ShapeOwnerKey, GrowableCentroidSums>,
    _owner: PhantomData<&'owner Shape>,
    #[cfg(test)]
    work: CentroidWorkStats,
}

impl<'owner> ExactOwnerFold<'owner> {
    fn new(tier: CentroidDimension) -> Self {
        Self {
            tier,
            owners: CentroidOwnerMap::default(),
            _owner: PhantomData,
            #[cfg(test)]
            work: CentroidWorkStats::default(),
        }
    }

    fn shape_summary(&mut self, shape: &'owner Shape) -> CoreResult<GrowableCentroidSums> {
        let owner = if let Shape::GeometryCollection(collection) = shape
            && collection.members_are_shared()
        {
            Some(ShapeOwnerKey::new(collection.as_slice()))
        } else {
            None
        };
        if let Some(key) = owner
            && let Some(summary) = self.owners.get(&key)
        {
            #[cfg(test)]
            {
                self.work.cache_hits = self.work.cache_hits.wrapping_add(1);
            }
            return Ok(summary.clone());
        }

        #[cfg(test)]
        {
            self.work.owner_visits = self.work.owner_visits.wrapping_add(1);
        }
        let summary = match shape {
            Shape::Point(point) if self.tier == CentroidDimension::Puntal => {
                exact_point_summary(*point)
            },
            Shape::MultiPoint(points) if self.tier == CentroidDimension::Puntal => {
                let mut summary = GrowableCentroidSums::zero();
                #[cfg(test)]
                {
                    self.work.member_slots =
                        self.work.member_slots.wrapping_add(points.member_count());
                    self.work.source_xy = self.work.source_xy.wrapping_add(points.len());
                }
                for point in points {
                    summary = summary.add(exact_point_summary(point))?;
                }
                summary
            },
            Shape::LineString(line)
                if matches!(
                    self.tier,
                    CentroidDimension::Lineal | CentroidDimension::Puntal
                ) =>
            {
                self.sequence_summary(line.as_coords())?
            },
            Shape::MultiLineString(lines)
                if matches!(
                    self.tier,
                    CentroidDimension::Lineal | CentroidDimension::Puntal
                ) =>
            {
                let mut summary = GrowableCentroidSums::zero();
                #[cfg(test)]
                {
                    self.work.member_slots =
                        self.work.member_slots.wrapping_add(lines.member_count());
                }
                for line in lines {
                    summary = summary.add(self.sequence_summary(line.as_coords())?)?;
                }
                summary
            },
            Shape::Polygon(polygon) => self.polygon_summary(polygon)?,
            Shape::MultiPolygon(polygons) => {
                let mut summary = GrowableCentroidSums::zero();
                #[cfg(test)]
                {
                    self.work.member_slots =
                        self.work.member_slots.wrapping_add(polygons.member_count());
                }
                for polygon in polygons {
                    summary = summary.add(self.polygon_summary(polygon)?)?;
                }
                summary
            },
            Shape::GeometryCollection(collection) => {
                #[cfg(test)]
                {
                    self.work.member_slots = self.work.member_slots.wrapping_add(collection.len());
                }
                self.shape_children(collection.as_slice())?
            },
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Empty(..) => GrowableCentroidSums::zero(),
        };
        if let Some(key) = owner {
            self.owners.insert_new(&key, summary.clone());
        }
        Ok(summary)
    }

    fn shape_children(&mut self, children: &'owner [Shape]) -> CoreResult<GrowableCentroidSums> {
        let mut first_shared: Option<(ShapeOwnerKey, &'owner Shape, usize)> = None;
        let mut indices: Option<HashMap<ShapeOwnerKey, usize>> = None;
        let mut shared = Vec::<(&'owner Shape, usize)>::new();
        let mut summary = GrowableCentroidSums::zero();
        for child in children {
            if let Shape::GeometryCollection(collection) = child
                && collection.members_are_shared()
            {
                let key = ShapeOwnerKey::new(collection.as_slice());
                let occurrences = match &mut first_shared {
                    None => {
                        first_shared = Some((key, child, 1));
                        continue;
                    },
                    Some((first, _, count)) if *first == key => count,
                    Some(_) => match indices
                        .get_or_insert_with(|| HashMap::with_capacity(1))
                        .entry(key)
                    {
                        std::collections::hash_map::Entry::Occupied(entry) => {
                            &mut shared[*entry.get()].1
                        },
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            entry.insert(shared.len());
                            shared.push((child, 1));
                            continue;
                        },
                    },
                };
                *occurrences = occurrences
                    .checked_add(1)
                    .expect("one collection's repeated slots fit its slice length");
            } else {
                summary = summary.add(self.shape_summary(child)?)?;
            }
        }
        for (child, occurrences) in first_shared
            .into_iter()
            .map(|(_, child, occurrences)| (child, occurrences))
            .chain(shared)
        {
            let child = self.shape_summary(child)?.scaled(occurrences)?;
            summary = summary.add(child)?;
        }
        Ok(summary)
    }

    fn polygon_summary(&mut self, polygon: &Polygon) -> CoreResult<GrowableCentroidSums> {
        let mut summary = GrowableCentroidSums::zero();
        let rings = std::iter::once(&polygon.shell).chain(polygon.holes.iter());
        for (index, ring) in rings.enumerate() {
            #[cfg(test)]
            {
                self.work.member_slots = self.work.member_slots.wrapping_add(1);
            }
            let mut ring_summary = self.sequence_summary(ring.coords())?;
            if self.tier == CentroidDimension::Areal {
                if ring_summary.measure.is_zero() {
                    continue;
                }
                let hole = index != 0;
                let reverse = (ring_summary.measure.sign() == ExactSign::Negative) ^ hole;
                if reverse {
                    ring_summary = ring_summary.negate();
                }
            }
            summary = summary.add(ring_summary)?;
        }
        Ok(summary)
    }

    #[cfg_attr(
        not(test),
        expect(
            clippy::needless_pass_by_ref_mut,
            reason = "test-only traversal statistics record the visited coordinates"
        )
    )]
    fn sequence_summary(&mut self, coords: &CoordSeq) -> CoreResult<GrowableCentroidSums> {
        #[cfg(test)]
        {
            self.work.source_xy = self.work.source_xy.wrapping_add(coords.len());
        }
        let xs = coords.xs();
        let ys = coords.ys();
        debug_assert_eq!(xs.len(), ys.len());
        let mut summary = GrowableCentroidSums::zero();
        match self.tier {
            CentroidDimension::Areal => {
                for ((&x0, &x1), (&y0, &y1)) in xs
                    .iter()
                    .zip(xs.iter().skip(1))
                    .zip(ys.iter().zip(ys.iter().skip(1)))
                {
                    let cross = exact_cross(x0, y0, x1, y1)?;
                    let x_pair = ExactDyadic::from_f64(x0).try_add(ExactDyadic::from_f64(x1))?;
                    let y_pair = ExactDyadic::from_f64(y0).try_add(ExactDyadic::from_f64(y1))?;
                    summary = summary.add(GrowableCentroidSums {
                        measure: cross.clone(),
                        moment_x: cross.try_product(&x_pair)?,
                        moment_y: cross.try_product(&y_pair)?,
                    })?;
                }
            },
            CentroidDimension::Lineal => {
                for ((&x0, &x1), (&y0, &y1)) in xs
                    .iter()
                    .zip(xs.iter().skip(1))
                    .zip(ys.iter().zip(ys.iter().skip(1)))
                {
                    let Some(length) = exact_segment_length(x0, y0, x1, y1)? else {
                        continue;
                    };
                    let midpoint_x = ExactDyadic::from_f64(f64::midpoint(x0, x1));
                    let midpoint_y = ExactDyadic::from_f64(f64::midpoint(y0, y1));
                    summary = summary.add(GrowableCentroidSums {
                        measure: length.clone(),
                        moment_x: length.try_product(&midpoint_x)?,
                        moment_y: length.try_product(&midpoint_y)?,
                    })?;
                }
            },
            CentroidDimension::Puntal => {
                if let Some((&x, &y)) = xs.first().zip(ys.first()) {
                    summary = exact_xy_point_summary(x, y);
                }
            },
        }
        Ok(summary)
    }
}

#[cfg(test)]
fn exact_owner_centroid_with_work(
    shape: &Shape,
    tier: CentroidDimension,
) -> CoreResult<(Option<(f64, f64)>, CentroidWorkStats)> {
    let mut fold = ExactOwnerFold::new(tier);
    let summary = fold.shape_summary(shape)?;
    let centroid = exact_summary_centroid(summary, tier)?;
    Ok((centroid, fold.work))
}

fn exact_xy_point_summary(x: f64, y: f64) -> GrowableCentroidSums {
    GrowableCentroidSums {
        measure: ExactDyadic::one(),
        moment_x: ExactDyadic::from_f64(x),
        moment_y: ExactDyadic::from_f64(y),
    }
}

fn exact_point_summary(point: Point) -> GrowableCentroidSums {
    exact_xy_point_summary(point.x, point.y)
}

fn exact_owner_centroid(shape: &Shape, tier: CentroidDimension) -> CoreResult<Option<(f64, f64)>> {
    let mut fold = ExactOwnerFold::new(tier);
    exact_summary_centroid(fold.shape_summary(shape)?, tier)
}

fn exact_areal_centroid_shape(shape: &Shape) -> Option<(f64, f64, f64)> {
    exact_areal_centroid(|visit| {
        shape.for_each_component(&mut |component| {
            if let Component::Polygon(polygon) = component {
                for (ring, hole) in polygon_rings(polygon) {
                    if let Some((xs, ys)) = ring.xy_columns() {
                        visit(xs, ys, hole);
                    }
                }
            }
        });
    })
}

fn areal_child_weight_requires_exact_replay(area: f64) -> bool {
    // Exact zero area finishes as None; a present zero weight is positive
    // exact area that rounded to zero.
    area.is_infinite() || area == 0.0
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "Only polygon variants contribute; keep the measured non-areal fast path"
)]
fn areal_centroid_summary_direct(shape: &Shape) -> Option<(f64, f64, f64)> {
    match shape {
        Shape::Polygon(polygon) => areal_centroid_polygon(polygon),
        Shape::MultiPolygon(polygons) => {
            if polygons.len() == 1 {
                return polygons.first().and_then(areal_centroid_polygon);
            }
            let mut parts = Vec::with_capacity(polygons.len());
            let mut area = 0.0_f64;
            for polygon in polygons {
                if let Some(part) = areal_centroid_polygon(polygon) {
                    if areal_child_weight_requires_exact_replay(part.0) {
                        drop(parts);
                        return exact_areal_centroid_shape(shape);
                    }
                    if part.0 > 0.0 && part.0.is_finite() {
                        area += part.0;
                        parts.push(part);
                    }
                }
            }
            let merged = weighted_centroid_merge::<true, false>(&parts);
            if merged.fallback
                == WeightedCentroidMergeFallback::PositiveWeightNormalizationUnderflow
            {
                drop(parts);
                return exact_areal_centroid_shape(shape);
            }
            merged.centroid.map(|(x, y)| (area, x, y))
        },
        _ => None,
    }
}

/// Aggregate areal members only at GeometryCollection boundaries. Other shape
/// variants take their direct kernel and never construct owner-admission state.
fn areal_centroid_summary<'owner>(
    shape: &'owner Shape,
    admission: &mut Option<OwnerAdmission<'owner>>,
) -> CentroidAttempt<Option<(f64, f64, f64)>> {
    let repeated_work = if let Shape::GeometryCollection(collection) = shape {
        collection.depth() > 1
            || admission
                .as_ref()
                .is_some_and(|admission| admission.repeated_owner)
    } else {
        false
    };
    if repeated_work {
        areal_centroid_summary_impl::<false, true>(shape, admission)
    } else {
        areal_centroid_summary_impl::<false, false>(shape, admission)
    }
}

fn areal_centroid_summary_impl<'owner, const TRACK_OWNERS: bool, const TRACK_REPEAT_WORK: bool>(
    shape: &'owner Shape,
    admission: &mut Option<OwnerAdmission<'owner>>,
) -> CentroidAttempt<Option<(f64, f64, f64)>> {
    let Shape::GeometryCollection(collection) = shape else {
        return CentroidAttempt::Ready(areal_centroid_summary_direct(shape));
    };
    if collection.topological_dimension() < Dimension::Surface {
        return CentroidAttempt::Ready(None);
    }
    if TRACK_OWNERS && observe_collection_owner(shape, admission) {
        return CentroidAttempt::ExactAtRoot(ExactAtRoot);
    }
    let mut parts = Vec::new();
    let mut area = 0.0_f64;
    for geometry in collection {
        if TRACK_REPEAT_WORK
            && admission
                .as_ref()
                .is_some_and(|admission| admission.repeated_owner)
            && charge_repeated_work(admission, 1)
        {
            return CentroidAttempt::ExactAtRoot(ExactAtRoot);
        }
        let part = if let Shape::GeometryCollection(child) = geometry {
            let track_owners = child.members_are_shared()
                || (TRACK_OWNERS
                    && admission
                        .as_ref()
                        .is_some_and(|admission| admission.repeated_owner));
            if track_owners {
                areal_centroid_summary_impl::<true, TRACK_REPEAT_WORK>(geometry, admission)
            } else {
                areal_centroid_summary_impl::<false, TRACK_REPEAT_WORK>(geometry, admission)
            }
        } else {
            CentroidAttempt::Ready(areal_centroid_summary_direct(geometry))
        };
        let part = match part {
            CentroidAttempt::Ready(part) => part,
            CentroidAttempt::ExactAtRoot(reason) => {
                return CentroidAttempt::ExactAtRoot(reason);
            },
        };
        if let Some(part) = part {
            if areal_child_weight_requires_exact_replay(part.0) {
                return CentroidAttempt::ExactAtRoot(ExactAtRoot);
            }
            if part.0 > 0.0 && part.0.is_finite() {
                area += part.0;
                parts.push(part);
            }
        }
    }
    let merged = weighted_centroid_merge::<true, false>(&parts);
    if merged.fallback == WeightedCentroidMergeFallback::PositiveWeightNormalizationUnderflow {
        return CentroidAttempt::ExactAtRoot(ExactAtRoot);
    }
    CentroidAttempt::Ready(merged.centroid.map(|(x, y)| (area, x, y)))
}

pub(crate) fn areal_centroid(shape: &Shape) -> CoreResult<Option<(f64, f64)>> {
    if !matches!(shape, Shape::GeometryCollection(_)) {
        return Ok(areal_centroid_summary_direct(shape).map(|(_, x, y)| (x, y)));
    }
    if shape.topological_dimension() < Dimension::Surface {
        return Ok(None);
    }
    let mut admission = None;
    match areal_centroid_summary(shape, &mut admission) {
        CentroidAttempt::Ready(summary) => Ok(summary.map(|(_, x, y)| (x, y))),
        CentroidAttempt::ExactAtRoot(_) => exact_owner_centroid(shape, CentroidDimension::Areal),
    }
}

/// Per-component lineal centroid for a single coordinate run, returned with
/// its length weight for the caller's merge or whole-shape replay decision.
fn lineal_component_centroid(xs: &[f64], ys: &[f64]) -> Option<(f64, f64, f64)> {
    let mut total_length = 0.0_f64;
    let mut line_sum = (0.0_f64, 0.0_f64);
    accumulate_lineal_centroid(xs, ys, &mut total_length, &mut line_sum);
    let (cx, cy) = finish_lineal_centroid_rescued(total_length, line_sum, |visit| visit(xs, ys))?;
    (total_length > 0.0).then_some((total_length, cx, cy))
}

const fn lineal_part_requires_exact_replay(weight: f64, cx: f64, cy: f64) -> bool {
    // An infinite length remains a replay sentinel even if its rescued center failed too.
    weight.is_infinite() || !cx.is_finite() || !cy.is_finite()
}

fn lineal_fast_result_needs_exact(total_length: f64, line_sum: (f64, f64)) -> bool {
    match finish_lineal_centroid(total_length, line_sum) {
        Some((x, y)) if x.is_finite() && y.is_finite() => {
            !total_length.is_normal()
                || (line_sum.0 != 0.0 && !line_sum.0.is_normal())
                || (line_sum.1 != 0.0 && !line_sum.1.is_normal())
                || (total_length > 0.0 && x == 0.0 && y == 0.0)
        },
        Some(_) => true,
        None => false,
    }
}

fn lineal_single_run_centroid(
    shape: &Shape,
    xs: &[f64],
    ys: &[f64],
) -> CoreResult<Option<(f64, f64)>> {
    let mut total_length = 0.0_f64;
    let mut line_sum = (0.0_f64, 0.0_f64);
    accumulate_lineal_centroid(xs, ys, &mut total_length, &mut line_sum);
    if lineal_fast_result_needs_exact(total_length, line_sum) {
        exact_owner_centroid(shape, CentroidDimension::Lineal)
    } else {
        Ok(finish_lineal_centroid(total_length, line_sum))
    }
}

fn try_for_each_lineal(shape: &Shape, visit: &mut impl FnMut(&[f64], &[f64]) -> bool) -> bool {
    shape
        .try_for_each_component(&mut |component| {
            let stopped = match component {
                Component::Point(_) => false,
                Component::LineString(line) => visit(line.xs(), line.ys()),
                Component::Polygon(polygon) => polygon_rings(polygon)
                    .any(|(ring, _)| ring.xy_columns().is_some_and(|(xs, ys)| visit(xs, ys))),
            };
            if stopped { Err(()) } else { Ok(()) }
        })
        .is_err()
}

fn for_each_lineal(shape: &Shape, visit: &mut impl FnMut(&[f64], &[f64])) {
    let stopped = try_for_each_lineal(shape, &mut |xs, ys| {
        visit(xs, ys);
        false
    });
    debug_assert!(!stopped);
}

/// Traverse collections with owner admission; a non-collection subtree uses
/// its direct component kernel without checking its leaf storage owners.
fn visit_lineal_owner_runs<'owner>(
    shape: &'owner Shape,
    admission: &mut Option<OwnerAdmission<'owner>>,
    visit: &mut impl FnMut(&[f64], &[f64]) -> bool,
) -> CentroidAttempt<()> {
    let repeated_work = if let Shape::GeometryCollection(collection) = shape {
        collection.depth() > 1
            || admission
                .as_ref()
                .is_some_and(|admission| admission.repeated_owner)
    } else {
        false
    };
    if repeated_work {
        visit_lineal_owner_runs_impl::<false, true>(shape, admission, visit)
    } else {
        visit_lineal_owner_runs_impl::<false, false>(shape, admission, visit)
    }
}

fn visit_lineal_owner_runs_impl<'owner, const TRACK_OWNERS: bool, const TRACK_REPEAT_WORK: bool>(
    shape: &'owner Shape,
    admission: &mut Option<OwnerAdmission<'owner>>,
    visit: &mut impl FnMut(&[f64], &[f64]) -> bool,
) -> CentroidAttempt<()> {
    let Shape::GeometryCollection(collection) = shape else {
        let repeated = TRACK_REPEAT_WORK
            && admission
                .as_ref()
                .is_some_and(|admission| admission.repeated_owner);
        let stopped = try_for_each_lineal(shape, &mut |xs, ys| {
            (repeated && charge_repeated_work(admission, xs.len())) || visit(xs, ys)
        });
        return if stopped {
            CentroidAttempt::ExactAtRoot(ExactAtRoot)
        } else {
            CentroidAttempt::Ready(())
        };
    };
    if TRACK_OWNERS && observe_collection_owner(shape, admission) {
        return CentroidAttempt::ExactAtRoot(ExactAtRoot);
    }
    for child in collection {
        let repeated = TRACK_REPEAT_WORK
            && admission
                .as_ref()
                .is_some_and(|admission| admission.repeated_owner);
        if repeated && charge_repeated_work(admission, 1) {
            return CentroidAttempt::ExactAtRoot(ExactAtRoot);
        }
        let outcome = if let Shape::GeometryCollection(child_collection) = child {
            let track_owners = child_collection.members_are_shared()
                || (TRACK_OWNERS
                    && admission
                        .as_ref()
                        .is_some_and(|admission| admission.repeated_owner));
            if track_owners {
                visit_lineal_owner_runs_impl::<true, TRACK_REPEAT_WORK>(child, admission, visit)
            } else {
                visit_lineal_owner_runs_impl::<false, TRACK_REPEAT_WORK>(child, admission, visit)
            }
        } else {
            visit_lineal_owner_runs_impl::<TRACK_OWNERS, TRACK_REPEAT_WORK>(child, admission, visit)
        };
        if let CentroidAttempt::ExactAtRoot(reason) = outcome {
            return CentroidAttempt::ExactAtRoot(reason);
        }
    }
    CentroidAttempt::Ready(())
}

/// Dimension-1 centroid: length-weighted edge-midpoint sums over every lineal
/// run — standalone lines and polygon-ring boundaries (the zero-area fallback).
/// `None` when the total length is zero. Multi-part lines use component-local
/// centroids then a length-weighted stable merge so order and cancellation
/// cannot move the mean.
pub(crate) fn lineal_centroid(shape: &Shape) -> CoreResult<Option<(f64, f64)>> {
    match shape {
        Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) => Ok(None),
        Shape::LineString(line) => lineal_single_run_centroid(shape, line.xs(), line.ys()),
        Shape::MultiLineString(lines) if lines.len() <= 1 => {
            lines.first().map_or(Ok(None), |line| {
                lineal_single_run_centroid(shape, line.xs(), line.ys())
            })
        },
        Shape::MultiLineString(lines) => {
            let mut parts = Vec::with_capacity(lines.len());
            for line in lines {
                if let Some((w, cx, cy)) = lineal_component_centroid(line.xs(), line.ys()) {
                    if lineal_part_requires_exact_replay(w, cx, cy) {
                        drop(parts);
                        return exact_owner_centroid(shape, CentroidDimension::Lineal);
                    }
                    parts.push((w, cx, cy));
                }
            }
            let merged = weighted_centroid_merge::<true, true>(&parts);
            let fallback = merged.fallback;
            let centroid = merged.centroid;
            drop(parts);
            match fallback {
                WeightedCentroidMergeFallback::ReplayWholeSource
                | WeightedCentroidMergeFallback::PositiveWeightNormalizationUnderflow => {
                    exact_owner_centroid(shape, CentroidDimension::Lineal)
                },
                WeightedCentroidMergeFallback::None => Ok(centroid),
            }
        },
        Shape::GeometryCollection(collection)
            if collection.topological_dimension() == Dimension::Point =>
        {
            Ok(None)
        },
        Shape::Polygon(_) | Shape::MultiPolygon(_) => {
            let mut total_length = 0.0_f64;
            let mut line_sum = (0.0_f64, 0.0_f64);
            for_each_lineal(shape, &mut |xs, ys| {
                accumulate_lineal_centroid(xs, ys, &mut total_length, &mut line_sum);
            });
            if lineal_fast_result_needs_exact(total_length, line_sum) {
                exact_owner_centroid(shape, CentroidDimension::Lineal)
            } else {
                Ok(finish_lineal_centroid(total_length, line_sum))
            }
        },
        Shape::GeometryCollection(_) => {
            let mut admission = None;
            // Mixed collections: accumulate per lineal leaf, then merge.
            let mut parts = Vec::new();
            let outcome = visit_lineal_owner_runs(shape, &mut admission, &mut |xs, ys| {
                if let Some((w, cx, cy)) = lineal_component_centroid(xs, ys) {
                    if lineal_part_requires_exact_replay(w, cx, cy) {
                        return true;
                    }
                    parts.push((w, cx, cy));
                }
                false
            });
            if let CentroidAttempt::ExactAtRoot(_) = outcome {
                drop(parts);
                return exact_owner_centroid(shape, CentroidDimension::Lineal);
            }
            if parts.len() > 1 {
                let merged = weighted_centroid_merge::<true, true>(&parts);
                let fallback = merged.fallback;
                let centroid = merged.centroid;
                drop(parts);
                match fallback {
                    WeightedCentroidMergeFallback::ReplayWholeSource
                    | WeightedCentroidMergeFallback::PositiveWeightNormalizationUnderflow => {
                        exact_owner_centroid(shape, CentroidDimension::Lineal)
                    },
                    WeightedCentroidMergeFallback::None => Ok(centroid),
                }
            } else if let Some(&(weight, cx, cy)) = parts.first() {
                if lineal_part_requires_exact_replay(weight, cx, cy) {
                    drop(parts);
                    exact_owner_centroid(shape, CentroidDimension::Lineal)
                } else {
                    Ok(Some((cx, cy)))
                }
            } else {
                Ok(None)
            }
        },
    }
}

/// Equal-weight stable mean of 2-D points. Ordinary mid-range inputs keep the
/// flat `column_mean2` bit path. Large absolute coordinates or detected
/// cancellation scale first then Neumaier-sum so every permutation of
/// `[1e16, 1, -1e16]` yields exact `1/3`.
fn stable_point_mean(xs: &[f64], ys: &[f64]) -> Option<(f64, f64)> {
    let n = xs.len();
    if n == 0 {
        return None;
    }
    debug_assert_eq!(xs.len(), ys.len());
    let mut max_abs_x = 0.0_f64;
    let mut max_abs_y = 0.0_f64;
    for (&x, &y) in xs.iter().zip(ys.iter()) {
        max_abs_x = max_abs_x.max(x.abs());
        max_abs_y = max_abs_y.max(y.abs());
    }
    let max_abs = max_abs_x.max(max_abs_y);
    if max_abs <= AREAL_EXACT_MAX_COORD
        && let Some((mx, my)) = column_mean2(xs, ys)
        && mx.is_finite()
        && my.is_finite()
        && (max_abs == 0.0 || mx.abs().max(my.abs()) >= max_abs * f64::EPSILON * 32.0)
    {
        return Some((mx, my));
    }
    let scale_x = axis_pow2_scale(max_abs_x.max(1.0));
    let scale_y = axis_pow2_scale(max_abs_y.max(1.0));
    let n_f = n as f64;
    let sx = neumaier_sum(xs.iter().map(|&x| x * scale_x));
    let sy = neumaier_sum(ys.iter().map(|&y| y * scale_y));
    Some((
        canonicalize_zero((sx / n_f) / scale_x),
        canonicalize_zero((sy / n_f) / scale_y),
    ))
}

/// Dimension-0 centroid: the average of the geometry's points — actual
/// `Point`/`MultiPoint` vertices plus the first vertex of each (necessarily
/// zero-length, since we only get here when total length is zero) lineal run.
/// `None` for a fully empty geometry.
pub(crate) fn point_centroid(shape: &Shape) -> CoreResult<Option<(f64, f64)>> {
    if let Shape::Point(point) = shape {
        return Ok(Some((point.x, point.y)));
    }
    if let Shape::MultiPoint(seq) = shape {
        return Ok(stable_point_mean(seq.xs(), seq.ys()));
    }
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    if matches!(shape, Shape::GeometryCollection(_)) {
        let mut admission = None;
        match visit_fallback_points(shape, &mut admission, &mut |x, y| {
            xs.push(x);
            ys.push(y);
        }) {
            CentroidAttempt::ExactAtRoot(_) => {
                drop(xs);
                drop(ys);
                exact_owner_centroid(shape, CentroidDimension::Puntal)
            },
            CentroidAttempt::Ready(()) => Ok(stable_point_mean(&xs, &ys)),
        }
    } else {
        for_each_fallback_point(shape, &mut |x, y| {
            xs.push(x);
            ys.push(y);
        });
        Ok(stable_point_mean(&xs, &ys))
    }
}

fn polygon_rings(polygon: &Polygon) -> impl Iterator<Item = (&Ring, bool)> {
    std::iter::once((&polygon.shell, false)).chain(polygon.holes.iter().map(|hole| (hole, true)))
}

fn anchor_vertex(xs: &[f64], ys: &[f64], visit: &mut impl FnMut(f64, f64)) {
    if let Some((&x, &y)) = xs.first().zip(ys.first()) {
        visit(x, y);
    }
}

fn for_each_fallback_point(shape: &Shape, visit: &mut impl FnMut(f64, f64)) {
    shape.for_each_component(&mut |component| match component {
        Component::Point(point) => visit(point.x, point.y),
        Component::LineString(line) => anchor_vertex(line.xs(), line.ys(), visit),
        Component::Polygon(polygon) => {
            for (ring, _) in polygon_rings(polygon) {
                if let Some((xs, ys)) = ring.xy_columns() {
                    anchor_vertex(xs, ys, visit);
                }
            }
        },
    });
}

fn try_anchor_vertex(xs: &[f64], ys: &[f64], visit: &mut impl FnMut(f64, f64) -> bool) -> bool {
    xs.first()
        .zip(ys.first())
        .is_some_and(|(&x, &y)| visit(x, y))
}

fn try_for_each_fallback_point(shape: &Shape, visit: &mut impl FnMut(f64, f64) -> bool) -> bool {
    shape
        .try_for_each_component(&mut |component| {
            let stopped = match component {
                Component::Point(point) => visit(point.x, point.y),
                Component::LineString(line) => try_anchor_vertex(line.xs(), line.ys(), visit),
                Component::Polygon(polygon) => polygon_rings(polygon).any(|(ring, _)| {
                    ring.xy_columns()
                        .is_some_and(|(xs, ys)| try_anchor_vertex(xs, ys, visit))
                }),
            };
            if stopped { Err(()) } else { Ok(()) }
        })
        .is_err()
}

/// Visit point-tier occurrences at collection boundaries. Non-collection
/// children use the baseline point, line and ring traversal without owner checks.
fn visit_fallback_points<'owner>(
    shape: &'owner Shape,
    admission: &mut Option<OwnerAdmission<'owner>>,
    visit: &mut impl FnMut(f64, f64),
) -> CentroidAttempt<()> {
    visit_fallback_points_impl::<false>(shape, admission, visit)
}

fn visit_fallback_point_leaf(
    shape: &Shape,
    admission: &mut Option<OwnerAdmission<'_>>,
    visit: &mut impl FnMut(f64, f64),
) -> CentroidAttempt<()> {
    let repeated = admission
        .as_ref()
        .is_some_and(|admission| admission.repeated_owner);
    if !repeated {
        for_each_fallback_point(shape, visit);
        return CentroidAttempt::Ready(());
    }

    let stopped = try_for_each_fallback_point(shape, &mut |x, y| {
        if charge_repeated_work(admission, 1) {
            true
        } else {
            visit(x, y);
            false
        }
    });
    if stopped {
        CentroidAttempt::ExactAtRoot(ExactAtRoot)
    } else {
        CentroidAttempt::Ready(())
    }
}

fn visit_fallback_points_impl<'owner, const TRACK_OWNERS: bool>(
    shape: &'owner Shape,
    admission: &mut Option<OwnerAdmission<'owner>>,
    visit: &mut impl FnMut(f64, f64),
) -> CentroidAttempt<()> {
    let Shape::GeometryCollection(collection) = shape else {
        return visit_fallback_point_leaf(shape, admission, visit);
    };
    if TRACK_OWNERS && observe_collection_owner(shape, admission) {
        return CentroidAttempt::ExactAtRoot(ExactAtRoot);
    }
    for child in collection {
        if admission
            .as_ref()
            .is_some_and(|admission| admission.repeated_owner)
            && charge_repeated_work(admission, 1)
        {
            return CentroidAttempt::ExactAtRoot(ExactAtRoot);
        }
        let outcome = if let Shape::GeometryCollection(child_collection) = child {
            if child_collection.members_are_shared() {
                visit_fallback_points_impl::<true>(child, admission, visit)
            } else {
                visit_fallback_points_impl::<false>(child, admission, visit)
            }
        } else {
            visit_fallback_point_leaf(child, admission, visit)
        };
        if let CentroidAttempt::ExactAtRoot(reason) = outcome {
            return CentroidAttempt::ExactAtRoot(reason);
        }
    }
    CentroidAttempt::Ready(())
}

#[cfg(test)]
mod exact_lineal_centroid_tests {
    use super::exact_lineal_centroid_runs;

    #[test]
    fn exact_line_sum_recovers_subnormal_moment_products() {
        let delta = 2.0_f64.powi(-540);
        let first_x = [-0.5 * delta, 0.5 * delta];
        let first_y = [0.0, 0.0];
        let second_x = [3.5 * delta, 4.5 * delta];
        let second_y = [0.0, 0.0];

        assert_eq!((first_x[1] - first_x[0]).to_bits(), delta.to_bits());
        assert_eq!((second_x[1] - second_x[0]).to_bits(), delta.to_bits());
        assert_eq!((delta * (4.0 * delta)).to_bits(), 0.0_f64.to_bits());

        let centroid = exact_lineal_centroid_runs(|visit| {
            visit(&first_x, &first_y);
            visit(&second_x, &second_y);
        })
        .expect("positive lineal measure");
        assert_eq!(centroid.0.to_bits(), (2.0 * delta).to_bits());
        assert_eq!(centroid.1.to_bits(), 0.0_f64.to_bits());
    }
}

#[cfg(test)]
mod weighted_centroid_merge_tests {
    use super::{WeightedCentroidMergeFallback, weighted_centroid_merge};

    #[test]
    fn finite_weight_nonfinite_center_requires_whole_source_replay() {
        let parts = [(1.0, f64::INFINITY, 0.0), (3.0, 2.0, 3.0)];

        let old_checked_skip = weighted_centroid_merge::<true, false>(&parts);
        assert_eq!(old_checked_skip.centroid, Some((2.0, 3.0)));

        let admitted_lineal = weighted_centroid_merge::<true, true>(&parts);
        assert_eq!(
            admitted_lineal.fallback,
            WeightedCentroidMergeFallback::ReplayWholeSource
        );
        assert_eq!(admitted_lineal.centroid, None);
    }

    #[test]
    fn infinite_weight_precedes_nonfinite_center_admission() {
        let parts = [
            (f64::INFINITY, f64::INFINITY, f64::INFINITY),
            (3.0, 2.0, 3.0),
        ];

        let result = weighted_centroid_merge::<true, true>(&parts);

        assert_eq!(
            result.fallback,
            WeightedCentroidMergeFallback::ReplayWholeSource
        );
        assert_eq!(result.centroid, None);
    }

    #[test]
    fn positive_normalization_underflow_remains_a_separate_outcome() {
        let parts = [(1.0e308, 0.0, 0.0), (f64::from_bits(1), 4.0, 0.0)];

        let result = weighted_centroid_merge::<true, true>(&parts);

        assert_eq!(
            result.fallback,
            WeightedCentroidMergeFallback::PositiveWeightNormalizationUnderflow
        );
    }
}

#[cfg(test)]
mod owner_fold_tests {
    use super::*;
    use crate::geometry::{CoordinateAxes, LineSeq, MOrdinate, ZOrdinate};

    fn polygon(x: f64, y: f64, side: f64) -> Shape {
        let points = [
            Point::new_raw_xy(x, y),
            Point::new_raw_xy(x + side, y),
            Point::new_raw_xy(x + side, y + side),
            Point::new_raw_xy(x, y + side),
            Point::new_raw_xy(x, y),
        ];
        Shape::Polygon(Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
    }

    fn line(points: &[(f64, f64)]) -> Shape {
        let values: Vec<_> = points
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect();
        Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(&values).unwrap()).unwrap())
    }

    fn repeated(mut shape: Shape, depth: usize) -> Shape {
        for _ in 0..depth {
            shape = Shape::geometry_collection(vec![shape.clone(), shape]).unwrap();
        }
        shape
    }

    #[test]
    fn centroid_owner_map_keeps_four_entries_inline_before_promoting() {
        let (mut owners, inline_allocations) = crate::test_support::allocation_count(|| {
            let mut owners =
                CentroidOwnerMap::<u8, u8, CENTROID_ADMISSION_INLINE_OWNERS>::default();
            for key in 0..CENTROID_ADMISSION_INLINE_OWNERS as u8 {
                owners.insert_new(&key, key * 2);
            }
            owners
        });

        assert_eq!(inline_allocations, 0);
        for key in 0..CENTROID_ADMISSION_INLINE_OWNERS as u8 {
            assert_eq!(owners.get(&key), Some(&(key * 2)));
        }

        let ((), promotion_allocations) = crate::test_support::allocation_count(|| {
            let key = CENTROID_ADMISSION_INLINE_OWNERS as u8;
            owners.insert_new(&key, key * 2);
        });

        assert!(promotion_allocations > 0);
        for key in 0..=CENTROID_ADMISSION_INLINE_OWNERS as u8 {
            assert_eq!(owners.get(&key), Some(&(key * 2)));
        }
    }

    fn point_from(shape: &Shape) -> Point {
        let Shape::Point(point) = shape.centroid().unwrap() else {
            panic!("centroid returns a point");
        };
        point
    }

    #[test]
    fn exact_owner_work_reuses_shared_collection_slices() {
        let polygon = polygon(0.0, 0.0, 1.0);
        let line = line(&[(0.0, 0.0), (3.0, 4.0)]);
        let multi_point = Shape::multi_point(
            CoordinateAxes::XYZM,
            CoordSeq::try_from_points(&[
                Point::new_axes(2.0, -4.0, ZOrdinate(Some(10.0)), MOrdinate(Some(20.0))).unwrap(),
                Point::new_axes(6.0, 8.0, ZOrdinate(Some(-10.0)), MOrdinate(Some(-20.0))).unwrap(),
            ])
            .unwrap(),
        );

        for depth in [8, 12, 128] {
            let cases = [
                (
                    repeated(polygon.clone(), depth),
                    CentroidDimension::Areal,
                    (0.5, 0.5),
                    10,
                ),
                (
                    repeated(line.clone(), depth),
                    CentroidDimension::Lineal,
                    (1.5, 2.0),
                    4,
                ),
                (
                    repeated(multi_point.clone(), depth),
                    CentroidDimension::Puntal,
                    (4.0, 2.0),
                    4,
                ),
            ];
            for (shape, tier, expected, source_xy) in cases {
                let (centroid, work) = exact_owner_centroid_with_work(&shape, tier).unwrap();
                let centroid = centroid.expect("positive tier measure");
                assert_eq!(centroid, expected);
                let expected_owner_visits = depth
                    .checked_add(2)
                    .expect("test depth fits its owner count");
                let leaf_members = match tier {
                    CentroidDimension::Areal => 2,
                    CentroidDimension::Lineal => 0,
                    CentroidDimension::Puntal => 4,
                };
                let expected_member_slots = depth
                    .checked_mul(2)
                    .and_then(|slots| slots.checked_add(leaf_members))
                    .expect("test depth fits its member count");
                assert_eq!(work.owner_visits, expected_owner_visits);
                assert_eq!(work.member_slots, expected_member_slots);
                assert_eq!(work.source_xy, source_xy);
            }
        }
    }

    #[test]
    fn repeated_shared_line_dags_wait_for_the_repeat_work_budget() {
        let source = line(&[(0.0, 0.0), (2.0, 0.0)]);
        for depth in [4, 8] {
            let shape = repeated(source.clone(), depth);
            let mut admission = None;
            let outcome = visit_lineal_owner_runs(&shape, &mut admission, &mut |_, _| false);
            assert!(
                admission
                    .as_ref()
                    .is_some_and(|admission| admission.repeated_owner)
            );
            let repeated_work = admission.as_ref().unwrap().repeated_work;
            if depth == 4 {
                assert!(matches!(outcome, CentroidAttempt::Ready(())));
                assert!(repeated_work <= CENTROID_FAST_REPEAT_WORK_BUDGET);
            } else {
                assert!(matches!(outcome, CentroidAttempt::ExactAtRoot(_)));
                assert!(repeated_work > CENTROID_FAST_REPEAT_WORK_BUDGET);
            }
            assert_eq!((point_from(&shape).x, point_from(&shape).y), (1.0, 0.0));
        }
    }

    #[test]
    fn puntal_dags_replay_occurrences_until_the_repeat_work_budget() {
        let source = Shape::multi_point(
            CoordinateAxes::XY,
            CoordSeq::try_from_points(&[Point::new_raw_xy(2.0, -4.0), Point::new_raw_xy(6.0, 8.0)])
                .unwrap(),
        );

        let small = repeated(source.clone(), 2);
        let mut admission = None;
        let mut staged = 0;
        assert!(matches!(
            visit_fallback_points(&small, &mut admission, &mut |_, _| staged += 1),
            CentroidAttempt::Ready(())
        ));
        assert!(
            admission
                .as_ref()
                .is_some_and(|admission| admission.repeated_owner)
        );
        assert!(admission.as_ref().unwrap().repeated_work <= CENTROID_FAST_REPEAT_WORK_BUDGET);
        assert_eq!(staged, 8);
        assert_eq!((point_from(&small).x, point_from(&small).y), (4.0, 2.0));

        let large = repeated(source, 8);
        let mut admission = None;
        let mut staged = 0;
        assert!(matches!(
            visit_fallback_points(&large, &mut admission, &mut |_, _| staged += 1),
            CentroidAttempt::ExactAtRoot(_)
        ));
        assert!(admission.as_ref().unwrap().repeated_work > CENTROID_FAST_REPEAT_WORK_BUDGET);
        assert!(staged < 2 * (1 << 8));
        assert_eq!((point_from(&large).x, point_from(&large).y), (4.0, 2.0));
    }

    #[test]
    fn exact_at_root_precedes_nested_infinite_area_replay() {
        let shape = repeated(polygon(0.0, 0.0, 1.0e200), 12);
        let mut admission = None;
        assert!(matches!(
            areal_centroid_summary(&shape, &mut admission),
            CentroidAttempt::ExactAtRoot(_)
        ));
        let point = point_from(&shape);
        assert_eq!(point.x, 5.0e199);
        assert_eq!(point.y, 5.0e199);
    }

    #[test]
    fn external_polygon_alias_does_not_start_collection_admission() {
        let source = polygon(0.0, 0.0, 1.0);
        let external_alias = Shape::geometry_collection(vec![source.clone()]).unwrap();
        let mut admission = None;
        let CentroidAttempt::Ready(Some((area, x, y))) =
            areal_centroid_summary(&external_alias, &mut admission)
        else {
            panic!("one reachable externally aliased child remains on the fast path");
        };
        assert_eq!(area, 1.0);
        assert_eq!((x, y), (0.5, 0.5));
        assert!(admission.is_none());
        assert_eq!(
            source.centroid().unwrap(),
            Shape::Point(Point::new_raw_xy(0.5, 0.5))
        );
        drop(external_alias);
    }

    #[test]
    fn external_collection_alias_occurring_once_keeps_its_centroid() {
        let aliased_collection = Shape::geometry_collection(vec![polygon(0.0, 0.0, 1.0)]).unwrap();
        let external_collection_alias = aliased_collection.clone();
        let root = Shape::geometry_collection(vec![aliased_collection]).unwrap();
        let mut admission = None;
        let CentroidAttempt::Ready(Some((area, x, y))) =
            areal_centroid_summary(&root, &mut admission)
        else {
            panic!("one externally aliased collection occurrence keeps the fast path");
        };
        assert_eq!((area, x, y), (1.0, 0.5, 0.5));
        assert!(admission.is_some());
        assert_eq!(point_from(&root), Point::new_raw_xy(0.5, 0.5));
        drop(external_collection_alias);
    }

    #[test]
    fn exact_fold_reuses_shared_collection_owner_across_siblings() {
        let shared = Shape::geometry_collection(vec![polygon(0.0, 0.0, 1.0)]).unwrap();
        let left =
            Shape::geometry_collection(vec![shared.clone(), polygon(20.0, 0.0, 1.0)]).unwrap();
        let right = Shape::geometry_collection(vec![shared, polygon(40.0, 0.0, 1.0)]).unwrap();
        let siblings = Shape::geometry_collection(vec![left, right]).unwrap();
        let (centroid, work) =
            exact_owner_centroid_with_work(&siblings, CentroidDimension::Areal).unwrap();
        assert_eq!(centroid, Some((15.5, 0.5)));
        assert!(work.cache_hits > 0);
    }

    #[test]
    fn unshared_collection_members_construct_no_tier_admission() {
        let shape = Shape::geometry_collection(vec![
            polygon(0.0, 0.0, 1.0),
            line(&[(10.0, 0.0), (12.0, 0.0)]),
        ])
        .unwrap();
        let Shape::GeometryCollection(collection) = &shape else {
            unreachable!("fixture is a collection")
        };
        assert!(!collection.members_are_shared());

        let mut areal_admission = None;
        assert!(matches!(
            areal_centroid_summary(&shape, &mut areal_admission),
            CentroidAttempt::Ready(Some((1.0, 0.5, 0.5)))
        ));
        assert!(areal_admission.is_none());

        let mut lineal_admission = None;
        assert!(matches!(
            visit_lineal_owner_runs(&shape, &mut lineal_admission, &mut |_, _| false),
            CentroidAttempt::Ready(())
        ));
        assert!(lineal_admission.is_none());

        let mut puntal_admission = None;
        assert!(matches!(
            visit_fallback_points(&shape, &mut puntal_admission, &mut |_, _| {}),
            CentroidAttempt::Ready(())
        ));
        assert!(puntal_admission.is_none());
        assert_eq!(point_from(&shape), Point::new_raw_xy(0.5, 0.5));
    }

    #[test]
    fn centroid_empty_probe_memoizes_depth_128_empty_collection_dags() {
        let shape = repeated(Shape::empty_point(), 128);
        let (is_empty, work) = centroid_is_empty_with_work(&shape);
        assert!(is_empty);
        assert_eq!(work.member_slots, 256);
        assert_eq!(work.owner_visits, 129);
        assert_eq!(work.cache_entries, 127);
        assert_eq!(work.cache_hits, 127);
        assert_eq!(shape.centroid().unwrap(), Shape::empty_point());

        let unshared =
            Shape::geometry_collection(vec![Shape::empty_point(), Shape::empty_point()]).unwrap();
        let (is_empty, work) = centroid_is_empty_with_work(&unshared);
        assert!(is_empty);
        assert_eq!(work.member_slots, 0);
        assert_eq!(work.cache_entries, 0);
    }

    #[test]
    fn centroid_empty_probe_does_not_cache_nonempty_shared_owners() {
        let shape = repeated(Shape::Point(Point::new_raw_xy(2.0, -4.0)), 4);

        let (is_empty, work) = centroid_is_empty_with_work(&shape);

        assert!(!is_empty);
        assert_eq!(work.cache_entries, 0);
        assert_eq!(work.cache_hits, 0);
    }

    #[test]
    fn centroid_empty_probe_keeps_one_external_alias_inline() {
        let shared = Shape::geometry_collection(vec![Shape::empty_point()]).unwrap();
        let external_alias = shared.clone();
        let root = Shape::geometry_collection(vec![shared]).unwrap();

        let ((is_empty, work), probe_allocations) =
            crate::test_support::allocation_count(|| centroid_is_empty_with_work(&root));

        assert!(is_empty);
        assert_eq!(work.cache_entries, 1);
        assert_eq!(work.cache_hits, 0);
        assert_eq!(probe_allocations, 0);

        let (centroid, centroid_allocations) =
            crate::test_support::allocation_count(|| root.centroid().unwrap());
        assert_eq!(centroid, Shape::empty_point());
        assert_eq!(centroid_allocations, 0);
        drop(external_alias);
    }

    #[test]
    fn centroid_empty_probe_keeps_late_external_alias_outside_the_tree() {
        let child = Shape::geometry_collection(vec![Shape::empty_point()]).unwrap();
        let root = Shape::geometry_collection(vec![child]).unwrap();
        let Shape::GeometryCollection(root_collection) = &root else {
            unreachable!("collection fixture")
        };
        let Shape::GeometryCollection(child_collection) = &root_collection.as_slice()[0] else {
            unreachable!("nested collection fixture")
        };
        let external_alias = Shape::GeometryCollection(child_collection.clone());
        assert!(child_collection.members_are_shared());

        let ((is_empty, work), probe_allocations) =
            crate::test_support::allocation_count(|| centroid_is_empty_with_work(&root));

        assert!(is_empty);
        assert_eq!(work.member_slots, 2);
        assert_eq!(work.cache_entries, 1);
        assert_eq!(work.cache_hits, 0);
        assert_eq!(probe_allocations, 0);
        assert_eq!(root.centroid().unwrap(), Shape::empty_point());
        drop(external_alias);
    }

    #[test]
    fn unshared_polygon_centroid_does_not_allocate_owner_state() {
        let shape = polygon(0.0, 0.0, 1.0);

        let (centroid, allocations) =
            crate::test_support::allocation_count(|| shape.centroid().unwrap());

        assert_eq!(point_from(&centroid), Point::new_raw_xy(0.5, 0.5));
        assert_eq!(allocations, 0);
    }

    #[test]
    fn repeated_zero_length_line_occurrences_keep_puntal_anchors() {
        let line = line(&[(2.0, 4.0), (2.0, 4.0)]);
        let shape = Shape::geometry_collection(vec![
            line.clone(),
            line,
            Shape::Point(Point::new_raw_xy(10.0, 0.0)),
        ])
        .unwrap();

        let centroid = point_from(&shape);

        assert_eq!((centroid.x, centroid.y), (14.0 / 3.0, 8.0 / 3.0));
    }

    #[test]
    fn zero_length_far_line_does_not_move_positive_line_rescue_frame() {
        let maximum = f64::MAX;
        let anchor = 3.0_f64 * 2.0_f64.powi(970);
        let minimum = 2.0 * anchor - maximum;
        let zero = line(&[(minimum, 0.0), (minimum, 0.0)]);
        let positive = line(&[(maximum, -maximum), (maximum, maximum)]);
        for shape in [
            Shape::geometry_collection(vec![zero.clone(), positive.clone()]).unwrap(),
            Shape::geometry_collection(vec![positive, zero]).unwrap(),
        ] {
            let point = point_from(&shape);
            assert_eq!(point.x.to_bits(), maximum.to_bits());
            assert_eq!(point.y.to_bits(), 0.0_f64.to_bits());
        }
    }
}
