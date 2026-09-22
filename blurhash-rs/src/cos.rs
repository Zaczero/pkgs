use std::cell::RefCell;
use std::f32::consts::PI;
use std::mem::size_of;
use std::rc::Rc;

use arrayvec::ArrayVec;

use crate::color::{LANES, V4};

type CacheKey = (usize, usize);
type CosAxisCache = ArrayVec<(CacheKey, Rc<[f32]>), 32>;
type CosAxisSimdCache = ArrayVec<(CacheKey, Rc<[V4]>), 32>;

pub(crate) fn dimensions_fit(
    width: usize,
    height: usize,
    x_components: usize,
    y_components: usize,
) -> bool {
    allocation_fits::<f32>(height, y_components)
        && allocation_fits::<V4>(width, x_components.div_ceil(LANES))
}

fn allocation_fits<T>(len: usize, components: usize) -> bool {
    len.checked_mul(components)
        .and_then(|elements| elements.checked_mul(size_of::<T>()))
        .is_some_and(|bytes| isize::try_from(bytes).is_ok())
}

pub(crate) fn precompute_cos_axis(len: usize, components: usize) -> Vec<f32> {
    let len_f = len as f32;
    let mut out = vec![0.0_f32; len.saturating_mul(components)];
    for (p, row) in out.chunks_exact_mut(components).enumerate() {
        let p_f = p as f32;
        for (c, slot) in row.iter_mut().enumerate() {
            *slot = (PI * p_f * (c as f32) / len_f).cos();
        }
    }
    out
}

thread_local! {
    // Small caches: typical callers only ever use a handful of image sizes and
    // component counts. A linear scan over a tiny Vec has better locality than
    // a HashMap and avoids hashing overhead.
    static COS_AXIS_CACHE: RefCell<CosAxisCache> = RefCell::new(ArrayVec::new());
    static COS_AXIS_SIMD_CACHE: RefCell<CosAxisSimdCache> = RefCell::new(ArrayVec::new());
}

pub(crate) fn cos_axis_cached(len: usize, components: usize) -> Rc<[f32]> {
    let key = (len, components);
    COS_AXIS_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        for (k, v) in cache.iter() {
            if *k == key {
                return Rc::clone(v);
            }
        }

        // Simple eviction to avoid unbounded growth in pathological cases.
        if cache.is_full() {
            cache.clear();
        }

        let rc: Rc<[f32]> = precompute_cos_axis(len, components).into();
        cache.push((key, Rc::clone(&rc)));
        rc
    })
}

pub(crate) fn cos_axis_simd_cached(len: usize, components: usize) -> Rc<[V4]> {
    let key = (len, components);
    COS_AXIS_SIMD_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        for (k, v) in cache.iter() {
            if *k == key {
                return Rc::clone(v);
            }
        }

        // Simple eviction to avoid unbounded growth in pathological cases.
        if cache.is_full() {
            cache.clear();
        }

        let blocks = components.div_ceil(LANES);
        let len_f = len as f32;

        let mut out = Vec::with_capacity(len.wrapping_mul(blocks));
        for p in 0..len {
            let p_f = p as f32;
            for block in 0..blocks {
                let start = block.wrapping_mul(LANES);
                let lanes = components.wrapping_sub(start).min(LANES);
                let mut v = [0.0_f32; LANES];
                for (lane, slot) in v.iter_mut().enumerate().take(lanes) {
                    let c = start.wrapping_add(lane);
                    let cosine = (PI * p_f * (c as f32) / len_f).cos();
                    *slot = cosine;
                }
                out.push(V4::from_array(v));
            }
        }

        let rc: Rc<[V4]> = out.into();
        cache.push((key, Rc::clone(&rc)));
        rc
    })
}
